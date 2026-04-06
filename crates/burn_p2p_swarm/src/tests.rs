//! Regression and transport tests for the burn_p2p_swarm crate.

use std::collections::{BTreeMap, BTreeSet};
#[cfg(not(target_arch = "wasm32"))]
use std::io::{Read, Write};
#[cfg(not(target_arch = "wasm32"))]
use std::net::TcpListener;
#[cfg(not(target_arch = "wasm32"))]
use std::thread;
use std::time::{Duration, Instant};

use burn_p2p_core::{
    ArtifactDescriptor, ArtifactId, ArtifactKind, CapabilityCard, CapabilityCardId,
    CapabilityClass, ChunkDescriptor, ChunkId, ClientPlatform, ContentId, HeadDescriptor, HeadId,
    MetricValue, MetricsLiveEvent, MetricsLiveEventKind, NetworkId, PeerId, PeerRoleSet,
    PersistenceClass, Precision, RevisionId, StudyId, TelemetrySummary, WindowActivation, WindowId,
};
use chrono::Utc;
use futures::{executor::block_on, future};
use semver::Version;

use super::{
    ArtifactChunkPayload, ControlAnnouncement, ControlPlaneSnapshot, ExperimentControlEnvelope,
    ExperimentOverlaySet, HeadAnnouncement, LiveControlPlaneEvent, LiveSwarmEvent,
    MemoryControlPlaneShell, MemorySwarmShell, MigrationCoordinator, NativeControlPlaneShell,
    OverlayChannel, OverlayTopic, PeerObservation, PeerStore, ProtocolSet, PubsubPayload,
    RuntimeBoundary, RuntimeTransportPolicy, SwarmAddress, SwarmError, TransportKind,
};
use burn_p2p_experiment::ActivationTarget;

#[cfg(not(target_arch = "wasm32"))]
fn spawn_browser_edge_server(
    directory: Vec<burn_p2p_core::ExperimentDirectoryEntry>,
    heads: Vec<HeadDescriptor>,
    live_event: Option<MetricsLiveEvent>,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test edge");
    let address = listener.local_addr().expect("local addr");
    let directory_body = serde_json::to_string(&directory).expect("encode directory");
    let heads_body = serde_json::to_string(&heads).expect("encode heads");
    let live_event_body =
        live_event.map(|event| serde_json::to_string(&event).expect("encode live event"));

    let handle = thread::spawn(move || {
        for request_index in 0..3 {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut buffer = [0_u8; 8192];
            let size = stream.read(&mut buffer).expect("read request");
            let request = String::from_utf8_lossy(&buffer[..size]).to_string();
            let request_lower = request.to_ascii_lowercase();

            let (body, expected_path) = if request.starts_with("GET /directory ") {
                assert!(
                    request_lower.contains("x-session-id: browser-session"),
                    "directory request should include x-session-id header"
                );
                (directory_body.clone(), "/directory")
            } else if request.starts_with("GET /heads ") {
                assert!(
                    !request_lower.contains("x-session-id: browser-session"),
                    "heads request should not include x-session-id header"
                );
                (heads_body.clone(), "/heads")
            } else if request.starts_with("GET /metrics/live/latest ") {
                assert!(
                    !request_lower.contains("x-session-id: browser-session"),
                    "latest metrics request should not include x-session-id header"
                );
                (
                    live_event_body.clone().expect("latest metrics body"),
                    "/metrics/live/latest",
                )
            } else {
                panic!("unexpected browser edge request: {request}");
            };

            if request_index == 0 {
                assert!(
                    request.starts_with("GET /directory "),
                    "first request should fetch /directory before /heads, got {expected_path}"
                );
            }

            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            stream.flush().expect("flush response");
        }
    });

    (format!("http://{address}"), handle)
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_browser_edge_metrics_poll_server(
    events: Vec<MetricsLiveEvent>,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test edge");
    let address = listener.local_addr().expect("local addr");
    let handle = thread::spawn(move || {
        for event in events {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut buffer = [0_u8; 8192];
            let size = stream.read(&mut buffer).expect("read request");
            let request = String::from_utf8_lossy(&buffer[..size]).to_string();
            assert!(
                request.starts_with("GET /metrics/live/latest "),
                "unexpected browser edge polling request: {request}"
            );
            let body = serde_json::to_string(&event).expect("encode live event");
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            stream.flush().expect("flush response");
        }
    });

    (format!("http://{address}"), handle)
}

#[test]
fn experiment_topics_match_the_documented_paths() {
    let overlays = ExperimentOverlaySet::new(
        NetworkId::new("mainnet"),
        StudyId::new("study"),
        burn_p2p_core::ExperimentId::new("exp"),
    )
    .expect("overlays");

    assert_eq!(overlays.control.as_str(), "/burn-p2p/mainnet/control");
    assert_eq!(
        overlays.heads.as_str(),
        "/burn-p2p/mainnet/study/study/exp/exp/heads"
    );
    assert_eq!(
        overlays.telemetry.as_str(),
        "/burn-p2p/mainnet/study/study/exp/exp/telemetry"
    );
}

#[test]
fn swarm_address_rejects_invalid_multiaddr() {
    let error = SwarmAddress::new("definitely-not-a-multiaddr").expect_err("invalid");
    assert!(matches!(error, super::SwarmError::InvalidAddress(_)));
}

#[test]
fn protocol_set_is_derived_from_network_id() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    assert_eq!(protocols.control.as_str(), "/burn-p2p/network/v1/control");
    assert_eq!(
        protocols.chunk_fetch.as_str(),
        "/burn-p2p/network/v1/chunk-fetch"
    );
}

#[test]
fn browser_transport_policy_prefers_browser_transports() {
    let policy = RuntimeTransportPolicy::browser();
    assert_eq!(
        policy.preferred_transports,
        vec![
            TransportKind::WebTransport,
            TransportKind::WebRtc,
            TransportKind::WebSocket,
            TransportKind::RelayReservation,
        ]
    );
}

#[test]
fn native_transport_policy_prefers_quic_before_tcp_for_current_runtime() {
    let policy = RuntimeTransportPolicy::native();
    assert_eq!(
        policy.preferred_transports,
        vec![
            TransportKind::Quic,
            TransportKind::Tcp,
            TransportKind::WebSocket,
            TransportKind::RelayReservation,
        ]
    );
}

#[test]
fn peer_store_aggregates_connected_peers_and_eta() {
    let now = Utc::now();
    let network_id = NetworkId::new("network");
    let study_id = StudyId::new("study");
    let experiment_id = burn_p2p_core::ExperimentId::new("exp");
    let revision_id = RevisionId::new("rev");
    let peer_id = PeerId::new("peer");

    let capability_card = CapabilityCard {
        card_id: CapabilityCardId::new("card"),
        peer_id: peer_id.clone(),
        platform: ClientPlatform::Native,
        roles: PeerRoleSet::default_trainer(),
        preferred_backends: vec!["cuda".into()],
        recommended_classes: BTreeSet::from([CapabilityClass::TrainerGpu]),
        device_memory_bytes: Some(24 * 1024 * 1024 * 1024),
        system_memory_bytes: 64 * 1024 * 1024 * 1024,
        disk_bytes: 1024 * 1024 * 1024 * 1024,
        upload_mbps: 1000.0,
        download_mbps: 1000.0,
        persistence: PersistenceClass::Durable,
        work_units_per_second: 50.0,
        attestation_level: burn_p2p_core::AttestationLevel::Manifest,
        benchmark_hash: None,
        reported_at: now,
    };

    let telemetry = TelemetrySummary {
        network_id,
        study_id,
        experiment_id,
        revision_id,
        window_id: WindowId(3),
        active_peers: 7,
        accepted_contributions: 9,
        throughput_work_units_per_second: 40.0,
        network: burn_p2p_core::NetworkEstimate {
            connected_peers: 1,
            observed_peers: 1,
            estimated_network_size: 1.0,
            estimated_total_vram_bytes: None,
            estimated_total_flops: None,
            eta_lower_seconds: None,
            eta_upper_seconds: None,
        },
        metrics: BTreeMap::from([("estimated_flops".into(), MetricValue::Float(1250.0))]),
        captured_at: now,
    };

    let mut store = PeerStore::default();
    store.upsert(
        PeerObservation::new(peer_id.clone(), now)
            .with_capability_card(capability_card)
            .with_telemetry(telemetry),
    );

    let stats = store.stats(Some(400));

    assert_eq!(stats.connected_peers, 1);
    assert_eq!(stats.connected_peer_ids, vec![peer_id]);
    assert_eq!(stats.network_estimate.observed_peers, 1);
    assert_eq!(stats.network_estimate.estimated_network_size, 7.0);
    assert_eq!(
        stats.network_estimate.estimated_total_vram_bytes,
        Some(24_u128 * 1024 * 1024 * 1024)
    );
    assert_eq!(stats.network_estimate.estimated_total_flops, Some(1250.0));
    assert_eq!(stats.network_estimate.eta_lower_seconds, Some(8));
    assert_eq!(stats.network_estimate.eta_upper_seconds, Some(12));
}

#[test]
fn migration_plan_only_switches_changed_topics() {
    let current = ExperimentOverlaySet::new(
        NetworkId::new("network"),
        StudyId::new("study"),
        burn_p2p_core::ExperimentId::new("exp-a"),
    )
    .expect("current");
    let next = ExperimentOverlaySet::new(
        NetworkId::new("network"),
        StudyId::new("study"),
        burn_p2p_core::ExperimentId::new("exp-b"),
    )
    .expect("next");

    let plan = MigrationCoordinator::plan_overlay_transition(
        &current,
        &next,
        &ActivationTarget {
            activation: WindowActivation {
                activation_window: WindowId(11),
                grace_windows: 1,
            },
            required_client_capabilities: BTreeSet::from(["wgpu".into()]),
        },
        Some(HeadId::new("base")),
    );

    assert_eq!(plan.join_topics.len(), 5);
    assert_eq!(plan.leave_topics.len(), 5);
    assert_eq!(plan.fetch_base_head_id, Some(HeadId::new("base")));
}

#[test]
fn runtime_boundary_derives_protocols_from_genesis() {
    let runtime = RuntimeBoundary::for_platform(
        &burn_p2p_core::GenesisSpec {
            network_id: NetworkId::new("network"),
            protocol_version: Version::new(0, 1, 0),
            display_name: "network".into(),
            created_at: Utc::now(),
            metadata: BTreeMap::new(),
        },
        ClientPlatform::Browser,
        vec![SwarmAddress::new("/dns4/bootstrap.example.com/tcp/4001/ws").expect("addr")],
        vec![SwarmAddress::new("/ip4/0.0.0.0/udp/4001/quic-v1").expect("addr")],
    )
    .expect("runtime");

    assert_eq!(runtime.transport_policy, RuntimeTransportPolicy::browser());
    assert_eq!(
        runtime.protocols.control.as_str(),
        "/burn-p2p/network/v1/control"
    );
}

#[test]
fn overlay_topic_rejects_control_inside_experiment_scope() {
    let error = OverlayTopic::experiment(
        NetworkId::new("network"),
        StudyId::new("study"),
        burn_p2p_core::ExperimentId::new("exp"),
        OverlayChannel::Control,
    )
    .expect_err("invalid");

    assert!(matches!(
        error,
        super::SwarmError::InvalidOverlayChannel { .. }
    ));
}

#[test]
fn wire_types_round_trip_through_serde() {
    let now = Utc::now();
    let overlay = OverlayTopic::control(NetworkId::new("network"));
    let head = HeadDescriptor {
        head_id: HeadId::new("head"),
        study_id: StudyId::new("study"),
        experiment_id: burn_p2p_core::ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        artifact_id: ArtifactId::new("artifact"),
        parent_head_id: None,
        global_step: 42,
        created_at: now,
        metrics: BTreeMap::new(),
    };

    let payload = super::HeadAnnouncement {
        overlay,
        provider_peer_id: Some(PeerId::new("provider")),
        head,
        announced_at: now,
    };

    let bytes = serde_json::to_vec(&payload).expect("encode");
    let decoded: super::HeadAnnouncement = serde_json::from_slice(&bytes).expect("decode");

    assert_eq!(decoded, payload);
}

#[test]
fn metrics_announcements_round_trip_and_use_metrics_overlay() {
    let overlay = OverlayTopic::experiment(
        NetworkId::new("network"),
        StudyId::new("study"),
        burn_p2p_core::ExperimentId::new("exp"),
        OverlayChannel::Metrics,
    )
    .expect("metrics overlay");
    let announcement = super::MetricsAnnouncement {
        overlay: overlay.clone(),
        event: MetricsLiveEvent {
            network_id: NetworkId::new("network"),
            kind: MetricsLiveEventKind::CatchupRefresh,
            cursors: vec![burn_p2p_core::MetricsSyncCursor {
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                latest_snapshot_seq: Some(2),
                latest_ledger_segment_seq: Some(3),
                latest_head_id: Some(HeadId::new("head")),
                latest_merge_window_id: Some(ContentId::new("window")),
            }],
            generated_at: Utc::now(),
        },
    };

    let payload = PubsubPayload::Metrics(announcement.clone());
    let bytes = serde_json::to_vec(&payload).expect("encode metrics payload");
    let decoded: PubsubPayload = serde_json::from_slice(&bytes).expect("decode metrics payload");

    assert_eq!(decoded, payload);
    assert_eq!(overlay.channel, OverlayChannel::Metrics);
    assert!(overlay.as_str().ends_with("/metrics"));
}

#[test]
fn metrics_announcements_keep_a_bounded_recent_tail() {
    let overlay = OverlayTopic::experiment(
        NetworkId::new("network"),
        StudyId::new("study"),
        burn_p2p_core::ExperimentId::new("exp"),
        OverlayChannel::Metrics,
    )
    .expect("metrics overlay");
    let mut snapshot = ControlPlaneSnapshot::default();

    for index in 0..80u64 {
        super::apply_pubsub_payload(
            &mut snapshot,
            PubsubPayload::Metrics(super::MetricsAnnouncement {
                overlay: overlay.clone(),
                event: MetricsLiveEvent {
                    network_id: NetworkId::new("network"),
                    kind: MetricsLiveEventKind::CatchupRefresh,
                    cursors: vec![burn_p2p_core::MetricsSyncCursor {
                        experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                        revision_id: RevisionId::new("rev"),
                        latest_snapshot_seq: Some(index),
                        latest_ledger_segment_seq: Some(index),
                        latest_head_id: Some(HeadId::new(format!("head-{index}"))),
                        latest_merge_window_id: Some(ContentId::new(format!("window-{index}"))),
                    }],
                    generated_at: Utc::now() + chrono::Duration::milliseconds(index as i64),
                },
            }),
        );
    }

    assert_eq!(snapshot.metrics_announcements.len(), 64);
    assert_eq!(
        snapshot.metrics_announcements[0].event.cursors[0].latest_snapshot_seq,
        Some(16)
    );
    assert_eq!(
        snapshot.metrics_announcements[63].event.cursors[0].latest_snapshot_seq,
        Some(79)
    );
}

#[test]
fn request_metadata_types_are_constructible() {
    let now = Utc::now();
    let provider = PeerId::new("provider");
    let descriptor = ChunkDescriptor {
        chunk_id: ChunkId::new("chunk"),
        offset_bytes: 0,
        length_bytes: 128,
        chunk_hash: ContentId::new("hash"),
    };

    let response = super::ChunkFetchResponse {
        provider,
        artifact_id: ArtifactId::new("artifact"),
        chunk: descriptor,
        generated_at: now,
    };

    assert_eq!(response.chunk.length_bytes, 128);
}

#[test]
fn control_envelope_announcements_are_usable() {
    let now = Utc::now();
    let envelope = ExperimentControlEnvelope {
        network_id: NetworkId::new("network"),
        command: burn_p2p_experiment::ExperimentControlCommand::PauseExperiment {
            study_id: StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp"),
            reason: Some("maintenance".into()),
            target: ActivationTarget {
                activation: WindowActivation {
                    activation_window: WindowId(9),
                    grace_windows: 1,
                },
                required_client_capabilities: BTreeSet::from(["cuda".into()]),
            },
        },
    };

    let cert = envelope
        .into_signed_cert(
            burn_p2p_core::SignatureMetadata {
                signer: PeerId::new("authority"),
                key_id: "authority-key".into(),
                algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                signed_at: now,
                signature_hex: "010203".into(),
            },
            Version::new(0, 1, 0),
        )
        .expect("cert");

    let announcement = super::ControlAnnouncement {
        overlay: OverlayTopic::control(NetworkId::new("network")),
        certificate: cert,
        announced_at: now,
    };

    assert_eq!(announcement.overlay.channel, OverlayChannel::Control);
}

#[test]
fn memory_swarm_shell_listens_and_connects_over_memory_transport() {
    let mut listener = MemorySwarmShell::new();
    let mut dialer = MemorySwarmShell::new();
    let listener_peer_id = listener.local_peer_id().to_string();
    let dialer_peer_id = dialer.local_peer_id().to_string();

    listener
        .listen_on(SwarmAddress::new("/memory/0").expect("memory addr"))
        .expect("listen");

    let listen_addr = block_on(future::poll_fn(|cx| match listener.poll_event(cx) {
        std::task::Poll::Ready(LiveSwarmEvent::NewListenAddr { address }) => {
            std::task::Poll::Ready(address)
        }
        std::task::Poll::Ready(_) | std::task::Poll::Pending => std::task::Poll::Pending,
    }));

    dialer.dial(listen_addr).expect("dial");

    let mut listener_connected = false;
    let mut dialer_connected = false;
    let (listener_connected, dialer_connected) = block_on(future::poll_fn(|cx| {
        loop {
            match listener.poll_event(cx) {
                std::task::Poll::Ready(LiveSwarmEvent::ConnectionEstablished { peer_id }) => {
                    listener_connected |= peer_id == dialer_peer_id;
                }
                std::task::Poll::Ready(_) => {}
                std::task::Poll::Pending => break,
            }
        }

        loop {
            match dialer.poll_event(cx) {
                std::task::Poll::Ready(LiveSwarmEvent::ConnectionEstablished { peer_id }) => {
                    dialer_connected |= peer_id == listener_peer_id;
                }
                std::task::Poll::Ready(_) => {}
                std::task::Poll::Pending => break,
            }
        }

        if listener_connected && dialer_connected {
            std::task::Poll::Ready((listener_connected, dialer_connected))
        } else {
            std::task::Poll::Pending
        }
    }));

    assert!(listener_connected);
    assert!(dialer_connected);
    assert_eq!(listener.connected_peer_count(), 1);
    assert_eq!(dialer.connected_peer_count(), 1);
}

#[test]
fn control_plane_shell_exchanges_snapshot_requests_and_responses() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let mut listener = MemoryControlPlaneShell::new(protocols.control.clone());
    let mut dialer = MemoryControlPlaneShell::new(protocols.control);
    let listener_peer_id = listener.local_peer_id().to_string();

    let now = Utc::now();
    listener.publish_control(ControlAnnouncement {
        overlay: OverlayTopic::control(NetworkId::new("network")),
        certificate: ExperimentControlEnvelope {
            network_id: NetworkId::new("network"),
            command: burn_p2p_experiment::ExperimentControlCommand::ResumeExperiment {
                study_id: StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                target: ActivationTarget {
                    activation: WindowActivation {
                        activation_window: WindowId(2),
                        grace_windows: 1,
                    },
                    required_client_capabilities: BTreeSet::from(["cuda".into()]),
                },
            },
        }
        .into_signed_cert(
            burn_p2p_core::SignatureMetadata {
                signer: PeerId::new("authority"),
                key_id: "authority-key".into(),
                algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                signed_at: now,
                signature_hex: "aabbcc".into(),
            },
            Version::new(0, 1, 0),
        )
        .expect("control cert"),
        announced_at: now,
    });
    listener.publish_head(HeadAnnouncement {
        overlay: OverlayTopic::experiment(
            NetworkId::new("network"),
            StudyId::new("study"),
            burn_p2p_core::ExperimentId::new("exp"),
            OverlayChannel::Heads,
        )
        .expect("heads overlay"),
        provider_peer_id: Some(PeerId::new(listener_peer_id.clone())),
        head: HeadDescriptor {
            head_id: HeadId::new("head-1"),
            study_id: StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-1"),
            parent_head_id: Some(HeadId::new("genesis-head")),
            global_step: 1,
            created_at: now,
            metrics: BTreeMap::new(),
        },
        announced_at: now,
    });

    listener
        .listen_on(SwarmAddress::new("/memory/0").expect("memory addr"))
        .expect("listen");
    let listen_addr = block_on(future::poll_fn(|cx| match listener.poll_event(cx) {
        std::task::Poll::Ready(LiveControlPlaneEvent::NewListenAddr { address }) => {
            std::task::Poll::Ready(address)
        }
        std::task::Poll::Ready(_) | std::task::Poll::Pending => std::task::Poll::Pending,
    }));

    dialer.dial(listen_addr).expect("dial");

    let mut listener_connected = false;
    let mut dialer_connected = false;
    block_on(future::poll_fn(|cx| {
        loop {
            match listener.poll_event(cx) {
                std::task::Poll::Ready(LiveControlPlaneEvent::ConnectionEstablished { .. }) => {
                    listener_connected = true;
                }
                std::task::Poll::Ready(_) => {}
                std::task::Poll::Pending => break,
            }
        }

        loop {
            match dialer.poll_event(cx) {
                std::task::Poll::Ready(LiveControlPlaneEvent::ConnectionEstablished { .. }) => {
                    dialer_connected = true;
                }
                std::task::Poll::Ready(_) => {}
                std::task::Poll::Pending => break,
            }
        }

        if listener_connected && dialer_connected {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    }));

    dialer
        .request_snapshot(&listener_peer_id)
        .expect("request snapshot");

    let mut listener_saw_request = false;
    let snapshot = block_on(future::poll_fn(|cx| {
        loop {
            match listener.poll_event(cx) {
                std::task::Poll::Ready(LiveControlPlaneEvent::SnapshotRequested { peer_id }) => {
                    listener_saw_request |= !peer_id.is_empty();
                }
                std::task::Poll::Ready(_) => {}
                std::task::Poll::Pending => break,
            }
        }

        loop {
            match dialer.poll_event(cx) {
                std::task::Poll::Ready(LiveControlPlaneEvent::SnapshotReceived {
                    snapshot,
                    ..
                }) => return std::task::Poll::Ready(snapshot),
                std::task::Poll::Ready(_) => {}
                std::task::Poll::Pending => break,
            }
        }

        std::task::Poll::Pending
    }));

    assert!(listener_saw_request);
    assert_eq!(
        snapshot,
        ControlPlaneSnapshot {
            control_announcements: listener.snapshot().control_announcements.clone(),
            head_announcements: listener.snapshot().head_announcements.clone(),
            lease_announcements: listener.snapshot().lease_announcements.clone(),
            merge_announcements: listener.snapshot().merge_announcements.clone(),
            merge_window_announcements: listener.snapshot().merge_window_announcements.clone(),
            reducer_assignment_announcements: listener
                .snapshot()
                .reducer_assignment_announcements
                .clone(),
            update_announcements: listener.snapshot().update_announcements.clone(),
            aggregate_announcements: listener.snapshot().aggregate_announcements.clone(),
            reduction_certificate_announcements: listener
                .snapshot()
                .reduction_certificate_announcements
                .clone(),
            reducer_load_announcements: listener.snapshot().reducer_load_announcements.clone(),
            auth_announcements: listener.snapshot().auth_announcements.clone(),
            directory_announcements: listener.snapshot().directory_announcements.clone(),
            metrics_announcements: listener.snapshot().metrics_announcements.clone(),
        }
    );
}

#[test]
fn memory_control_plane_shell_survives_reconnect_snapshot_storms() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("storm-net")).expect("protocols");
    let mut listener = MemoryControlPlaneShell::new(protocols.control.clone());
    let listener_peer_id = listener.local_peer_id().to_string();
    let now = Utc::now();
    listener.publish_head(HeadAnnouncement {
        overlay: OverlayTopic::experiment(
            NetworkId::new("storm-net"),
            StudyId::new("study"),
            burn_p2p_core::ExperimentId::new("exp"),
            OverlayChannel::Heads,
        )
        .expect("heads overlay"),
        provider_peer_id: Some(PeerId::new(listener_peer_id.clone())),
        head: HeadDescriptor {
            head_id: HeadId::new("storm-head"),
            study_id: StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact"),
            parent_head_id: Some(HeadId::new("genesis-head")),
            global_step: 3,
            created_at: now,
            metrics: BTreeMap::new(),
        },
        announced_at: now,
    });
    listener
        .listen_on(SwarmAddress::new("/memory/0").expect("memory addr"))
        .expect("listen");
    let listen_addr = block_on(future::poll_fn(|cx| match listener.poll_event(cx) {
        std::task::Poll::Ready(LiveControlPlaneEvent::NewListenAddr { address }) => {
            std::task::Poll::Ready(address)
        }
        std::task::Poll::Ready(_) | std::task::Poll::Pending => std::task::Poll::Pending,
    }));

    for reconnect_attempt in 0..6 {
        let mut dialer = MemoryControlPlaneShell::new(protocols.control.clone());
        dialer.dial(listen_addr.clone()).expect("dial");

        let connect_deadline = Instant::now() + Duration::from_secs(5);
        let mut listener_connected = false;
        let mut dialer_connected = false;
        while !(listener_connected && dialer_connected) {
            assert!(
                Instant::now() < connect_deadline,
                "memory shells did not connect on attempt {reconnect_attempt}"
            );

            if let Some(event) = listener.wait_event(Duration::from_millis(100))
                && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
            {
                listener_connected = true;
            }

            if let Some(event) = dialer.wait_event(Duration::from_millis(100))
                && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
            {
                dialer_connected = true;
            }
        }

        dialer
            .request_snapshot(&listener_peer_id)
            .expect("request snapshot");
        let snapshot_deadline = Instant::now() + Duration::from_secs(5);
        let snapshot = loop {
            assert!(
                Instant::now() < snapshot_deadline,
                "snapshot timed out on attempt {reconnect_attempt}"
            );
            if let Some(LiveControlPlaneEvent::SnapshotRequested { .. }) =
                listener.wait_event(Duration::from_millis(50))
            {}
            if let Some(event) = dialer.wait_event(Duration::from_millis(50)) {
                match event {
                    LiveControlPlaneEvent::SnapshotReceived { snapshot, .. } => break snapshot,
                    LiveControlPlaneEvent::RequestFailure { message, .. } => {
                        panic!("snapshot request failed on attempt {reconnect_attempt}: {message}")
                    }
                    _ => {}
                }
            }
        };

        assert_eq!(snapshot.head_announcements.len(), 1);
        assert_eq!(
            snapshot.head_announcements[0].head.head_id,
            HeadId::new("storm-head")
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_edge_control_plane_client_fetches_directory_and_heads_snapshot() {
    let network_id = NetworkId::new("browser-network");
    let study_id = StudyId::new("study");
    let experiment_id = burn_p2p_core::ExperimentId::new("exp");
    let revision_id = RevisionId::new("rev");
    let now = Utc::now();
    let directory = vec![burn_p2p_core::ExperimentDirectoryEntry {
        network_id: network_id.clone(),
        study_id: study_id.clone(),
        experiment_id: experiment_id.clone(),
        workload_id: burn_p2p_core::WorkloadId::new("workload"),
        display_name: "Browser Edge Experiment".into(),
        model_schema_hash: ContentId::new("schema"),
        dataset_view_id: burn_p2p_core::DatasetViewId::new("dataset"),
        resource_requirements: burn_p2p_core::ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(1024),
            estimated_download_bytes: 1024 * 1024,
            estimated_window_seconds: 30,
        },
        visibility: burn_p2p_core::ExperimentVisibility::OptIn,
        opt_in_policy: burn_p2p_core::ExperimentOptInPolicy::Scoped,
        current_revision_id: revision_id.clone(),
        current_head_id: Some(HeadId::new("head-1")),
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::new(),
        metadata: BTreeMap::new(),
    }];
    let heads = vec![HeadDescriptor {
        head_id: HeadId::new("head-1"),
        study_id: study_id.clone(),
        experiment_id: experiment_id.clone(),
        revision_id: revision_id.clone(),
        artifact_id: ArtifactId::new("artifact"),
        parent_head_id: Some(HeadId::new("genesis-head")),
        global_step: 7,
        created_at: now,
        metrics: BTreeMap::new(),
    }];
    let live_event = MetricsLiveEvent {
        network_id: network_id.clone(),
        kind: MetricsLiveEventKind::CatchupRefresh,
        cursors: vec![burn_p2p_core::MetricsSyncCursor {
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            latest_snapshot_seq: Some(1),
            latest_ledger_segment_seq: Some(2),
            latest_head_id: Some(HeadId::new("head-1")),
            latest_merge_window_id: Some(ContentId::new("window-1")),
        }],
        generated_at: now,
    };
    let (base_url, server) =
        spawn_browser_edge_server(directory.clone(), heads.clone(), Some(live_event.clone()));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let snapshot = runtime
        .block_on(async {
            super::BrowserEdgeControlPlaneClient::new(base_url, network_id.clone())
                .fetch_snapshot(Some(&ContentId::new("browser-session")))
                .await
        })
        .expect("fetch snapshot");

    server.join().expect("join test edge");

    assert_eq!(snapshot.directory_announcements.len(), 1);
    assert_eq!(snapshot.directory_announcements[0].entries, directory);
    assert_eq!(snapshot.head_announcements.len(), 1);
    assert_eq!(snapshot.head_announcements[0].head, heads[0]);
    assert_eq!(snapshot.metrics_announcements.len(), 1);
    assert_eq!(snapshot.metrics_announcements[0].event, live_event);
    assert_eq!(
        snapshot.head_announcements[0].overlay,
        OverlayTopic::experiment(
            network_id.clone(),
            study_id.clone(),
            experiment_id.clone(),
            OverlayChannel::Heads,
        )
        .expect("heads overlay")
    );
    assert_eq!(
        snapshot.metrics_announcements[0].overlay,
        OverlayTopic::experiment(network_id, study_id, experiment_id, OverlayChannel::Metrics)
            .expect("metrics overlay")
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_edge_control_plane_client_collects_unique_metrics_events_by_polling() {
    let network_id = NetworkId::new("browser-network");
    let experiment_id = burn_p2p_core::ExperimentId::new("exp");
    let revision_id = RevisionId::new("rev");
    let first = MetricsLiveEvent {
        network_id: network_id.clone(),
        kind: MetricsLiveEventKind::CatchupRefresh,
        cursors: vec![burn_p2p_core::MetricsSyncCursor {
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            latest_snapshot_seq: Some(1),
            latest_ledger_segment_seq: Some(2),
            latest_head_id: Some(HeadId::new("head-1")),
            latest_merge_window_id: Some(ContentId::new("window-1")),
        }],
        generated_at: Utc::now(),
    };
    let second = MetricsLiveEvent {
        network_id: network_id.clone(),
        kind: MetricsLiveEventKind::CatchupRefresh,
        cursors: vec![burn_p2p_core::MetricsSyncCursor {
            experiment_id,
            revision_id,
            latest_snapshot_seq: Some(2),
            latest_ledger_segment_seq: Some(3),
            latest_head_id: Some(HeadId::new("head-2")),
            latest_merge_window_id: Some(ContentId::new("window-2")),
        }],
        generated_at: Utc::now(),
    };
    let (base_url, server) =
        spawn_browser_edge_metrics_poll_server(vec![first.clone(), second.clone(), second.clone()]);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let events = runtime
        .block_on(async {
            super::BrowserEdgeControlPlaneClient::new(base_url, network_id)
                .collect_metrics_events_by_polling(3, 0)
                .await
        })
        .expect("collect metrics events");

    server.join().expect("join test edge");
    assert_eq!(events, vec![first, second]);
}

#[test]
fn native_control_plane_shell_exchanges_snapshot_requests_and_responses_over_tcp() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let mut listener =
        NativeControlPlaneShell::new(protocols.control.clone()).expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(protocols.control).expect("native dialer");
    let listener_peer_id = listener.local_peer_id().to_string();

    let now = Utc::now();
    listener.publish_head(HeadAnnouncement {
        overlay: OverlayTopic::experiment(
            NetworkId::new("network"),
            StudyId::new("study"),
            burn_p2p_core::ExperimentId::new("exp"),
            OverlayChannel::Heads,
        )
        .expect("heads overlay"),
        provider_peer_id: Some(PeerId::new(listener_peer_id.clone())),
        head: HeadDescriptor {
            head_id: HeadId::new("head-1"),
            study_id: StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-1"),
            parent_head_id: Some(HeadId::new("genesis-head")),
            global_step: 1,
            created_at: now,
            metrics: BTreeMap::new(),
        },
        announced_at: now,
    });

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("tcp addr"))
        .expect("listen");

    let listen_addr = loop {
        match listener.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("listener did not produce a listen address"),
        }
    };

    dialer.dial(listen_addr).expect("dial");

    let mut listener_connected = false;
    let mut dialer_connected = false;
    let mut listener_events = Vec::new();
    let mut dialer_events = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !(listener_connected && dialer_connected) {
        assert!(
            Instant::now() < deadline,
            "native shells did not connect; listener events: {:?}; dialer events: {:?}",
            listener_events,
            dialer_events
        );

        if let Some(event) = listener.wait_event(Duration::from_millis(100)) {
            listener_events.push(format!("{event:?}"));
            listener_connected |=
                matches!(event, LiveControlPlaneEvent::ConnectionEstablished { .. });
        }

        if let Some(event) = dialer.wait_event(Duration::from_millis(100)) {
            dialer_events.push(format!("{event:?}"));
            dialer_connected |=
                matches!(event, LiveControlPlaneEvent::ConnectionEstablished { .. });
        }
    }

    dialer
        .request_snapshot(&listener_peer_id)
        .expect("request snapshot");

    let deadline = Instant::now() + Duration::from_secs(5);
    let snapshot = loop {
        assert!(
            Instant::now() < deadline,
            "dialer did not receive native snapshot"
        );

        if let Some(event) = listener.wait_event(Duration::from_millis(100))
            && let LiveControlPlaneEvent::SnapshotRequested { .. } = event
        {
            // keep polling until the response lands on the dialer
        }

        match dialer.wait_event(Duration::from_millis(100)) {
            Some(LiveControlPlaneEvent::SnapshotReceived { snapshot, .. }) => break snapshot,
            Some(_) => {}
            None => {}
        }
    };

    assert_eq!(snapshot.head_announcements.len(), 1);
    assert_eq!(
        snapshot.head_announcements[0].head.head_id,
        HeadId::new("head-1")
    );
}

#[test]
fn native_control_plane_shell_exchanges_snapshot_requests_and_responses_over_quic() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let mut listener =
        NativeControlPlaneShell::new(protocols.control.clone()).expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(protocols.control).expect("native dialer");
    let listener_peer_id = listener.local_peer_id().to_string();

    let now = Utc::now();
    listener.publish_head(HeadAnnouncement {
        overlay: OverlayTopic::experiment(
            NetworkId::new("network"),
            StudyId::new("study"),
            burn_p2p_core::ExperimentId::new("exp"),
            OverlayChannel::Heads,
        )
        .expect("heads overlay"),
        provider_peer_id: Some(PeerId::new(listener_peer_id.clone())),
        head: HeadDescriptor {
            head_id: HeadId::new("head-quic-1"),
            study_id: StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-quic-1"),
            parent_head_id: Some(HeadId::new("genesis-head")),
            global_step: 1,
            created_at: now,
            metrics: BTreeMap::new(),
        },
        announced_at: now,
    });

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/udp/0/quic-v1").expect("quic addr"))
        .expect("listen");

    let listen_addr = loop {
        match listener.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("listener did not produce a QUIC listen address"),
        }
    };

    dialer.dial(listen_addr).expect("dial");

    let mut listener_connected = false;
    let mut dialer_connected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !(listener_connected && dialer_connected) {
        assert!(
            Instant::now() < deadline,
            "native QUIC shells did not connect"
        );

        if let Some(event) = listener.wait_event(Duration::from_millis(100))
            && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
        {
            listener_connected = true;
        }

        if let Some(event) = dialer.wait_event(Duration::from_millis(100))
            && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
        {
            dialer_connected = true;
        }
    }

    dialer
        .request_snapshot(&listener_peer_id)
        .expect("request snapshot");

    let deadline = Instant::now() + Duration::from_secs(5);
    let snapshot = loop {
        assert!(
            Instant::now() < deadline,
            "dialer did not receive QUIC snapshot"
        );

        if let Some(event) = listener.wait_event(Duration::from_millis(100))
            && let LiveControlPlaneEvent::SnapshotRequested { .. } = event
        {}

        match dialer.wait_event(Duration::from_millis(100)) {
            Some(LiveControlPlaneEvent::SnapshotReceived { snapshot, .. }) => break snapshot,
            Some(_) => {}
            None => {}
        }
    };

    assert_eq!(snapshot.head_announcements.len(), 1);
    assert_eq!(
        snapshot.head_announcements[0].head.head_id,
        HeadId::new("head-quic-1")
    );
}

#[test]
fn native_control_plane_shell_propagates_control_announcements_over_pubsub() {
    let network_id = NetworkId::new("network");
    let protocols = ProtocolSet::for_network(&network_id).expect("protocols");
    let control_overlay = OverlayTopic::control(network_id.clone());
    let mut listener =
        NativeControlPlaneShell::new(protocols.control.clone()).expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(protocols.control).expect("native dialer");

    let now = Utc::now();
    let announcement = ControlAnnouncement {
        overlay: control_overlay.clone(),
        certificate: ExperimentControlEnvelope {
            network_id: network_id.clone(),
            command: burn_p2p_experiment::ExperimentControlCommand::ResumeExperiment {
                study_id: StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                target: ActivationTarget {
                    activation: WindowActivation {
                        activation_window: WindowId(2),
                        grace_windows: 1,
                    },
                    required_client_capabilities: BTreeSet::from(["cuda".into()]),
                },
            },
        }
        .into_signed_cert(
            burn_p2p_core::SignatureMetadata {
                signer: PeerId::new("authority"),
                key_id: "authority-key".into(),
                algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                signed_at: now,
                signature_hex: "ddeeff".into(),
            },
            Version::new(0, 1, 0),
        )
        .expect("control cert"),
        announced_at: now,
    };

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("tcp addr"))
        .expect("listen");

    let listen_addr = loop {
        match listener.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("listener did not produce a listen address"),
        }
    };

    dialer.dial(listen_addr).expect("dial");

    let mut listener_connected = false;
    let mut dialer_connected = false;
    let connect_deadline = Instant::now() + Duration::from_secs(5);
    while !(listener_connected && dialer_connected) {
        assert!(
            Instant::now() < connect_deadline,
            "native shells did not connect"
        );

        if let Some(event) = listener.wait_event(Duration::from_millis(100))
            && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
        {
            listener_connected = true;
        }

        if let Some(event) = dialer.wait_event(Duration::from_millis(100))
            && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
        {
            dialer_connected = true;
        }
    }

    listener
        .subscribe_topic(control_overlay.clone())
        .expect("listener subscribe");
    dialer
        .subscribe_topic(control_overlay.clone())
        .expect("dialer subscribe");

    let publish_deadline = Instant::now() + Duration::from_secs(10);
    let mut listener_saw_remote_subscription = false;
    let mut published = false;
    let mut received_pubsub = false;
    while !received_pubsub {
        assert!(
            Instant::now() < publish_deadline,
            "dialer did not receive control announcement over pubsub"
        );

        let pump_until = Instant::now() + Duration::from_millis(250);
        while Instant::now() < pump_until {
            if let Some(event) = listener.wait_event(Duration::from_millis(25)) {
                match event {
                    LiveControlPlaneEvent::TopicSubscribed { topic }
                        if topic == control_overlay.as_str() =>
                    {
                        listener_saw_remote_subscription = true;
                    }
                    LiveControlPlaneEvent::PubsubMessage { kind, .. } if kind == "control" => {}
                    _ => {}
                }
            }

            if let Some(event) = dialer.wait_event(Duration::from_millis(25))
                && let LiveControlPlaneEvent::PubsubMessage { kind, topic, .. } = event
                && kind == "control"
                && topic == control_overlay.as_str()
            {
                received_pubsub = true;
                break;
            }
        }

        if !published && listener_saw_remote_subscription {
            listener.publish_control(announcement.clone());
            match listener.publish_pubsub(
                control_overlay.clone(),
                PubsubPayload::Control(announcement.clone()),
            ) {
                Ok(()) => published = true,
                Err(SwarmError::Pubsub(message))
                    if message.contains("NoPeersSubscribedToTopic") => {}
                Err(error) => panic!("publish control pubsub: {error}"),
            }
        }
    }

    assert!(
        dialer
            .snapshot()
            .control_announcements
            .contains(&announcement)
    );
}

#[test]
fn native_control_plane_shell_transfers_artifact_manifests_and_chunks_over_tcp() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let mut listener =
        NativeControlPlaneShell::new(protocols.control.clone()).expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(protocols.control).expect("native dialer");

    let descriptor = ArtifactDescriptor {
        artifact_id: ArtifactId::new("artifact-1"),
        kind: ArtifactKind::ServeHead,
        head_id: Some(HeadId::new("head-1")),
        base_head_id: None,
        precision: Precision::Fp16,
        model_schema_hash: ContentId::new("schema"),
        record_format: "burn-record:bin".into(),
        bytes_len: 6,
        chunks: vec![ChunkDescriptor {
            chunk_id: ChunkId::new("chunk-1"),
            offset_bytes: 0,
            length_bytes: 6,
            chunk_hash: ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(
                b"abcdef",
            )),
        }],
        root_hash: ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(b"abcdef")),
    };
    listener.publish_artifact(
        descriptor.clone(),
        vec![ArtifactChunkPayload {
            artifact_id: descriptor.artifact_id.clone(),
            chunk: descriptor.chunks[0].clone(),
            bytes: b"abcdef".to_vec(),
            generated_at: Utc::now(),
        }],
    );

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("tcp addr"))
        .expect("listen");

    let listen_addr = loop {
        match listener.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("listener did not produce a listen address"),
        }
    };

    dialer.dial(listen_addr).expect("dial");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut listener_connected = false;
    let mut dialer_connected = false;
    while !(listener_connected && dialer_connected) {
        assert!(Instant::now() < deadline, "native shells did not connect");

        if let Some(event) = listener.wait_event(Duration::from_millis(100))
            && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
        {
            listener_connected = true;
        }

        if let Some(event) = dialer.wait_event(Duration::from_millis(100))
            && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
        {
            dialer_connected = true;
        }
    }

    dialer
        .request_artifact_manifest(
            &listener.local_peer_id().to_string(),
            descriptor.artifact_id.clone(),
        )
        .expect("request descriptor");
    let deadline = Instant::now() + Duration::from_secs(5);
    let fetched_descriptor = loop {
        assert!(Instant::now() < deadline, "fetch descriptor");
        if let Some(
            LiveControlPlaneEvent::ArtifactManifestRequested { .. }
            | LiveControlPlaneEvent::SnapshotResponseSent { .. }
            | LiveControlPlaneEvent::ConnectionEstablished { .. }
            | LiveControlPlaneEvent::Other { .. },
        ) = listener.wait_event(Duration::from_millis(50))
        {}
        if let Some(event) = dialer.wait_event(Duration::from_millis(50)) {
            match event {
                LiveControlPlaneEvent::ArtifactManifestReceived { descriptor, .. } => {
                    break descriptor.expect("descriptor");
                }
                LiveControlPlaneEvent::RequestFailure { message, .. } => {
                    panic!("fetch descriptor: {message}");
                }
                _ => {}
            }
        }
    };
    assert_eq!(fetched_descriptor, descriptor);

    dialer
        .request_artifact_chunk(
            &listener.local_peer_id().to_string(),
            descriptor.artifact_id.clone(),
            descriptor.chunks[0].chunk_id.clone(),
        )
        .expect("request chunk");
    let deadline = Instant::now() + Duration::from_secs(5);
    let chunk_payload = loop {
        assert!(Instant::now() < deadline, "fetch chunk");
        if let Some(
            LiveControlPlaneEvent::ArtifactChunkRequested { .. }
            | LiveControlPlaneEvent::SnapshotResponseSent { .. }
            | LiveControlPlaneEvent::ConnectionEstablished { .. }
            | LiveControlPlaneEvent::Other { .. },
        ) = listener.wait_event(Duration::from_millis(50))
        {}
        if let Some(event) = dialer.wait_event(Duration::from_millis(50)) {
            match event {
                LiveControlPlaneEvent::ArtifactChunkReceived { payload, .. } => {
                    break payload.expect("chunk payload");
                }
                LiveControlPlaneEvent::RequestFailure { message, .. } => {
                    panic!("fetch chunk: {message}");
                }
                _ => {}
            }
        }
    };
    assert_eq!(chunk_payload.bytes, b"abcdef");
    assert_eq!(chunk_payload.chunk, descriptor.chunks[0]);
}
