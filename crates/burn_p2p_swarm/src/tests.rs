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
    AggregateEnvelope, AggregateStats, AggregateTier, ArtifactDescriptor, ArtifactId, ArtifactKind,
    BrowserResolvedSeedBootstrap, BrowserSeedBootstrapSource, BrowserSwarmPhase,
    BrowserSwarmStatus, BrowserTransportFamily, BrowserTransportObservationSource, CapabilityCard,
    CapabilityCardId, CapabilityClass, ChunkDescriptor, ChunkId, ClientPlatform, ContentId,
    DatasetViewId, ExperimentDirectoryEntry, ExperimentId, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentVisibility, HeadDescriptor, HeadId, MetricValue,
    MetricsLiveEvent, MetricsLiveEventKind, NetworkId, PeerId, PeerRole, PeerRoleSet,
    PersistenceClass, Precision, ReductionCertificate, RevisionId, StudyId, TelemetrySummary,
    ValidationQuorumCertificate, WindowActivation, WindowId, WorkloadId,
};
use chrono::Utc;
use futures::{executor::block_on, future};
use semver::Version;

use super::{
    AggregateProposalAnnouncement, ArtifactChunkPayload, BrowserSwarmBootstrap,
    BrowserSwarmDialPlan, BrowserSwarmRuntime, ControlAnnouncement, ControlPlaneSnapshot,
    ExperimentControlEnvelope, ExperimentLifecycleAnnouncement, ExperimentOverlaySet,
    FleetScheduleAnnouncement, HeadAnnouncement, LiveControlPlaneEvent, LiveSwarmEvent,
    MemoryControlPlaneShell, MemorySwarmShell, MigrationCoordinator, NativeControlPlaneShell,
    OverlayChannel, OverlayTopic, PeerDirectoryAnnouncement, PeerObservation, PeerStore,
    PlannedBrowserSwarmRuntime, ProtocolSet, PubsubEnvelope, PubsubPayload,
    ReductionCertificateAnnouncement, RuntimeBoundary, RuntimeTransportPolicy, SwarmAddress,
    SwarmError, TransportKind, ValidationQuorumAnnouncement, WasmPendingConnect,
    browser_additional_direct_connection_budget, browser_direct_seed_retry_candidates,
    browser_peer_directory_dial_candidates, browser_should_retry_direct_handoff,
    browser_transport_family_for_seed_url, browser_wss_fallback_peers_to_disconnect,
    filter_supported_browser_seed_dial_candidates, plan_browser_seed_dials,
    preferred_connected_browser_transport, preferred_wasm_browser_request_peer_id,
    selected_browser_study_and_experiment, update_wasm_browser_status_from_snapshot,
};
use burn_p2p_experiment::{
    ActivationTarget, ExperimentLifecycleEnvelope, ExperimentLifecyclePhase,
    ExperimentLifecyclePlan, FleetScheduleEpochEnvelope,
};

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
    let policy = RuntimeTransportPolicy::browser_for_roles(&PeerRoleSet::default_trainer());
    assert_eq!(
        policy.preferred_transports,
        vec![
            TransportKind::WebRtc,
            TransportKind::WebTransport,
            TransportKind::WebSocket,
        ]
    );
    assert_eq!(policy.target_bootstrap_seed_connections, 0);
    assert!(!policy.enable_local_discovery);
    assert!(!policy.enable_relay_client);
    assert!(!policy.enable_relay_server);
    assert!(!policy.enable_hole_punching);
    assert!(!policy.enable_autonat);
    assert!(!policy.enable_rendezvous_client);
    assert!(!policy.enable_rendezvous_server);
    assert!(!policy.enable_kademlia);
}

#[test]
fn native_transport_policy_prefers_quic_before_tcp_for_current_runtime() {
    let policy = RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer());
    assert_eq!(
        policy.preferred_transports,
        vec![
            TransportKind::Quic,
            TransportKind::Tcp,
            TransportKind::WebSocket,
        ]
    );
    assert_eq!(policy.target_bootstrap_seed_connections, 0);
    assert!(!policy.enable_local_discovery);
    assert!(policy.enable_relay_client);
    assert!(!policy.enable_relay_server);
    assert!(policy.enable_hole_punching);
    assert!(policy.enable_autonat);
    assert!(policy.enable_rendezvous_client);
    assert!(!policy.enable_rendezvous_server);
    assert!(policy.enable_kademlia);
}

#[test]
fn bootstrap_transport_policy_targets_more_mesh_peers_with_connection_caps() {
    let policy = RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::new([
        burn_p2p_core::PeerRole::Bootstrap,
        burn_p2p_core::PeerRole::RelayHelper,
    ]));

    assert_eq!(policy.target_connected_peers, 8);
    assert_eq!(policy.target_bootstrap_seed_connections, 0);
    assert_eq!(policy.max_established_incoming, Some(96));
    assert_eq!(policy.max_established_total, Some(128));
    assert_eq!(policy.max_established_per_peer, Some(1));
    assert!(!policy.enable_local_discovery);
    assert!(policy.enable_relay_client);
    assert!(policy.enable_relay_server);
    assert!(policy.enable_hole_punching);
    assert!(policy.enable_autonat);
    assert!(policy.enable_rendezvous_client);
    assert!(policy.enable_rendezvous_server);
    assert!(policy.enable_kademlia);
}

#[test]
fn browser_transport_family_for_seed_url_classifies_browser_capable_multiaddrs() {
    assert_eq!(
        browser_transport_family_for_seed_url(
            "/dns4/bootstrap.example/udp/4001/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ),
        Some(BrowserTransportFamily::WebRtcDirect)
    );
    assert_eq!(
        browser_transport_family_for_seed_url(
            "/dns4/bootstrap.example/udp/443/quic-v1/webtransport/certhash/uEiBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
        ),
        Some(BrowserTransportFamily::WebTransport)
    );
    assert_eq!(
        browser_transport_family_for_seed_url("/dns4/bootstrap.example/tcp/443/wss"),
        Some(BrowserTransportFamily::WssFallback)
    );
    assert_eq!(
        browser_transport_family_for_seed_url("/dns4/bootstrap.example/udp/4001/webrtc-direct"),
        None
    );
    assert_eq!(
        browser_transport_family_for_seed_url("/dns4/bootstrap.example/udp/443/webtransport"),
        None
    );
    assert_eq!(
        browser_transport_family_for_seed_url("/dns4/bootstrap.example/tcp/4001/quic-v1"),
        None
    );
}

#[test]
fn browser_seed_dial_plan_prefers_direct_browser_transports_before_fallback() {
    let bootstrap = BrowserSwarmBootstrap {
        network_id: NetworkId::new("net-browser"),
        seed_bootstrap: BrowserResolvedSeedBootstrap {
            source: BrowserSeedBootstrapSource::Merged,
            seed_node_urls: vec![
                "/dns4/bootstrap.example/tcp/443/wss".into(),
                "/dns4/bootstrap.example/udp/443/quic-v1/webtransport/certhash/uEiBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".into(),
                "/dns4/bootstrap.example/udp/4001/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
            ],
            advertised_seed_count: 3,
            last_error: None,
        },
        transport_preference: vec![
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WebTransport,
            BrowserTransportFamily::WssFallback,
        ],
        selected_experiment: None,
        selected_revision: None,
    };

    let plan = plan_browser_seed_dials(&bootstrap);
    assert_eq!(
        plan.desired_transport,
        Some(BrowserTransportFamily::WebRtcDirect)
    );
    assert_eq!(plan.candidates.len(), 3);
    assert_eq!(
        plan.candidates[0].transport,
        BrowserTransportFamily::WebRtcDirect
    );
    assert_eq!(
        plan.candidates[1].transport,
        BrowserTransportFamily::WebTransport
    );
    assert_eq!(
        plan.candidates[2].transport,
        BrowserTransportFamily::WssFallback
    );
    assert!(plan.fallback_allowed);
}

#[test]
fn supported_browser_seed_candidates_preserve_order_after_filtering() {
    let bootstrap = BrowserSwarmBootstrap {
        network_id: NetworkId::new("net-browser"),
        seed_bootstrap: BrowserResolvedSeedBootstrap {
            source: BrowserSeedBootstrapSource::Merged,
            seed_node_urls: vec![
                "/dns4/bootstrap.example/udp/4001/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
                "/dns4/bootstrap.example/udp/443/quic-v1/webtransport/certhash/uEiBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".into(),
                "/dns4/bootstrap.example/tcp/443/wss".into(),
            ],
            advertised_seed_count: 3,
            last_error: None,
        },
        transport_preference: vec![
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WebTransport,
            BrowserTransportFamily::WssFallback,
        ],
        selected_experiment: None,
        selected_revision: None,
    };

    let plan = plan_browser_seed_dials(&bootstrap);
    let supported = filter_supported_browser_seed_dial_candidates(
        &plan,
        &[
            BrowserTransportFamily::WebTransport,
            BrowserTransportFamily::WssFallback,
        ],
    );

    assert_eq!(supported.len(), 2);
    assert_eq!(supported[0].transport, BrowserTransportFamily::WebTransport);
    assert_eq!(supported[1].transport, BrowserTransportFamily::WssFallback);
}

#[test]
fn browser_peer_directory_candidates_prefer_direct_mesh_peers_before_fallback() {
    let snapshot = ControlPlaneSnapshot {
        peer_directory_announcements: vec![
            semantic_test_peer_directory(
                "peer-wss",
                &["/dns4/peer-wss.example/tcp/443/wss"],
                None,
                Utc::now(),
            ),
            semantic_test_peer_directory(
                "peer-direct",
                &[
                    "/dns4/peer-direct.example/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
                ],
                None,
                Utc::now(),
            ),
        ],
        ..ControlPlaneSnapshot::default()
    };

    let candidates = browser_peer_directory_dial_candidates(
        &snapshot,
        None,
        &[
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WssFallback,
        ],
        &[
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WssFallback,
        ],
        &[],
        &BTreeMap::new(),
    );

    assert_eq!(candidates.len(), 2);
    assert_eq!(
        candidates[0].peer_id.as_ref(),
        Some(&PeerId::new("peer-direct"))
    );
    assert_eq!(
        candidates[0].transport,
        BrowserTransportFamily::WebRtcDirect
    );
    assert_eq!(
        candidates[1].peer_id.as_ref(),
        Some(&PeerId::new("peer-wss"))
    );
    assert_eq!(candidates[1].transport, BrowserTransportFamily::WssFallback);
}

#[test]
fn browser_peer_directory_candidates_skip_connected_and_attempted_peers() {
    let attempted = BTreeMap::from([(
        String::from(
            "/dns4/peer-attempted.example/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
        ),
        Instant::now(),
    )]);
    let snapshot = ControlPlaneSnapshot {
        peer_directory_announcements: vec![
            semantic_test_peer_directory(
                "peer-connected",
                &[
                    "/dns4/peer-connected.example/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
                ],
                None,
                Utc::now(),
            ),
            semantic_test_peer_directory(
                "peer-attempted",
                &[
                    "/dns4/peer-attempted.example/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
                ],
                None,
                Utc::now(),
            ),
            semantic_test_peer_directory(
                "peer-fresh",
                &[
                    "/dns4/peer-fresh.example/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
                ],
                None,
                Utc::now(),
            ),
        ],
        ..ControlPlaneSnapshot::default()
    };

    let candidates = browser_peer_directory_dial_candidates(
        &snapshot,
        None,
        &[BrowserTransportFamily::WebRtcDirect],
        &[BrowserTransportFamily::WebRtcDirect],
        &[PeerId::new("peer-connected")],
        &attempted,
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].peer_id.as_ref(),
        Some(&PeerId::new("peer-fresh"))
    );
    assert_eq!(
        candidates[0].seed_url,
        "/dns4/peer-fresh.example/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w"
    );
}

#[test]
fn browser_peer_directory_candidates_skip_bootstrap_host_direct_ports_not_in_signed_seeds() {
    let snapshot = ControlPlaneSnapshot {
        peer_directory_announcements: vec![
            semantic_test_peer_directory(
                "peer-edge-bad-port",
                &[
                    "/dns4/edge.dragon.aberration.technology/udp/4003/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
                ],
                None,
                Utc::now(),
            ),
            semantic_test_peer_directory(
                "peer-public-direct",
                &[
                    "/ip4/203.0.113.10/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
                ],
                None,
                Utc::now(),
            ),
        ],
        ..ControlPlaneSnapshot::default()
    };
    let bootstrap = BrowserSwarmBootstrap {
        network_id: NetworkId::new("net-browser"),
        seed_bootstrap: BrowserResolvedSeedBootstrap {
            source: BrowserSeedBootstrapSource::SiteConfigFallback,
            seed_node_urls: vec![
                "/dns4/edge.dragon.aberration.technology/udp/443/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
                "/dns4/edge.dragon.aberration.technology/tcp/443/wss".into(),
            ],
            advertised_seed_count: 1,
            last_error: None,
        },
        transport_preference: vec![
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WssFallback,
        ],
        selected_experiment: None,
        selected_revision: None,
    };

    let candidates = browser_peer_directory_dial_candidates(
        &snapshot,
        Some(&bootstrap),
        &[BrowserTransportFamily::WebRtcDirect],
        &[BrowserTransportFamily::WebRtcDirect],
        &[],
        &BTreeMap::new(),
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].peer_id.as_ref(),
        Some(&PeerId::new("peer-public-direct"))
    );
    assert_eq!(
        candidates[0].seed_url,
        "/ip4/203.0.113.10/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w"
    );
}

#[test]
fn browser_peer_directory_candidates_skip_relay_circuit_routes() {
    let snapshot = ControlPlaneSnapshot {
        peer_directory_announcements: vec![
            semantic_test_peer_directory(
                "peer-relayed",
                &[
                    "/dns4/edge.example/udp/443/webrtc-direct/certhash/uEiBIQQvRGIR6ld6a-VTmYxgsVlaOOMfJtcsf5LvtFwh7mQ/p2p/12D3KooWCkxZ42qCD3mSzPeAazTE9cCrFtidxAQKisQgMiXtVFxB/p2p-circuit/p2p/12D3KooWHy3XaDbKQqJ3aphNAENiP1LuLGqjauvkaMaqX7aguR1M",
                ],
                None,
                Utc::now(),
            ),
            semantic_test_peer_directory(
                "peer-direct",
                &[
                    "/dns4/peer-direct.example/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
                ],
                None,
                Utc::now(),
            ),
        ],
        ..ControlPlaneSnapshot::default()
    };

    let candidates = browser_peer_directory_dial_candidates(
        &snapshot,
        None,
        &[BrowserTransportFamily::WebRtcDirect],
        &[BrowserTransportFamily::WebRtcDirect],
        &[],
        &BTreeMap::new(),
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].peer_id.as_ref(),
        Some(&PeerId::new("peer-direct"))
    );
    assert_eq!(
        candidates[0].seed_url,
        "/dns4/peer-direct.example/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w"
    );
}

#[test]
fn browser_peer_directory_candidates_skip_local_direct_mesh_peers() {
    let snapshot = ControlPlaneSnapshot {
        peer_directory_announcements: vec![
            semantic_test_peer_directory(
                "peer-loopback",
                &[
                    "/ip4/127.0.0.1/udp/35292/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
                ],
                None,
                Utc::now(),
            ),
            semantic_test_peer_directory(
                "peer-private",
                &[
                    "/ip4/10.42.1.10/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
                ],
                None,
                Utc::now(),
            ),
            semantic_test_peer_directory(
                "peer-localhost",
                &[
                    "/dns4/localhost/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
                ],
                None,
                Utc::now(),
            ),
            semantic_test_peer_directory(
                "peer-public-direct",
                &[
                    "/dns4/peer-public.example/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w",
                ],
                None,
                Utc::now(),
            ),
        ],
        ..ControlPlaneSnapshot::default()
    };

    let candidates = browser_peer_directory_dial_candidates(
        &snapshot,
        None,
        &[BrowserTransportFamily::WebRtcDirect],
        &[BrowserTransportFamily::WebRtcDirect],
        &[],
        &BTreeMap::new(),
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].peer_id.as_ref(),
        Some(&PeerId::new("peer-public-direct"))
    );
    assert_eq!(
        candidates[0].seed_url,
        "/dns4/peer-public.example/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w"
    );
}

#[test]
fn preferred_connected_browser_transport_promotes_direct_peers_over_wss() {
    let connected = BTreeMap::from([
        (PeerId::new("peer-wss"), BrowserTransportFamily::WssFallback),
        (
            PeerId::new("peer-direct"),
            BrowserTransportFamily::WebRtcDirect,
        ),
    ]);

    assert_eq!(
        preferred_connected_browser_transport(&connected),
        Some(BrowserTransportFamily::WebRtcDirect)
    );
}

#[test]
fn browser_additional_direct_connection_budget_keeps_one_slot_until_direct_peer_exists() {
    let connected_peer_ids = vec![
        PeerId::new("peer-a"),
        PeerId::new("peer-b"),
        PeerId::new("peer-c"),
    ];
    let connected = BTreeMap::from([
        (PeerId::new("peer-a"), BrowserTransportFamily::WssFallback),
        (PeerId::new("peer-b"), BrowserTransportFamily::WssFallback),
        (PeerId::new("peer-c"), BrowserTransportFamily::WssFallback),
    ]);

    assert_eq!(
        browser_additional_direct_connection_budget(&connected_peer_ids, &connected),
        1
    );

    let connected_direct = BTreeMap::from([
        (PeerId::new("peer-a"), BrowserTransportFamily::WssFallback),
        (PeerId::new("peer-b"), BrowserTransportFamily::WebRtcDirect),
        (PeerId::new("peer-c"), BrowserTransportFamily::WssFallback),
    ]);
    assert_eq!(
        browser_additional_direct_connection_budget(&connected_peer_ids, &connected_direct),
        0
    );
}

#[test]
fn browser_direct_seed_retry_candidates_prefer_direct_transports_after_wss_connect() {
    let bootstrap = BrowserSwarmBootstrap {
        network_id: NetworkId::new("browser-handoff"),
        seed_bootstrap: BrowserResolvedSeedBootstrap {
            source: BrowserSeedBootstrapSource::EdgeSigned,
            seed_node_urls: vec![
                "/dns4/bootstrap.example/udp/443/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
                "/dns4/bootstrap.example/tcp/443/wss".into(),
            ],
            advertised_seed_count: 2,
            last_error: None,
        },
        transport_preference: vec![
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WssFallback,
        ],
        selected_experiment: None,
        selected_revision: None,
    };
    let connected =
        BTreeMap::from([(PeerId::new("peer-wss"), BrowserTransportFamily::WssFallback)]);
    let candidates = browser_direct_seed_retry_candidates(
        &bootstrap,
        &[
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WssFallback,
        ],
        &connected,
        &BTreeMap::new(),
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].transport,
        BrowserTransportFamily::WebRtcDirect
    );
}

#[test]
fn wasm_pending_connect_final_attempt_error_preserves_async_candidate_failures() {
    let mut pending = WasmPendingConnect {
        dial_plan: BrowserSwarmDialPlan::default(),
        candidates: Vec::new(),
        next_candidate_index: 0,
        attempt_errors: Vec::new(),
        response_tx: None,
    };
    pending.record_attempt_error(
        "/ip4/203.0.113.10/udp/443/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA: timed out".to_owned(),
    );
    pending.record_attempt_error(
        "/dns4/edge.dragon.aberration.technology/tcp/443/wss: websocket handshake failed"
            .to_owned(),
    );
    pending.record_attempt_error(
        "/ip4/203.0.113.10/udp/443/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA: timed out".to_owned(),
    );

    assert_eq!(
        pending.final_attempt_error(),
        SwarmError::Runtime(
            "browser direct swarm could not dial any supported seed candidate: /ip4/203.0.113.10/udp/443/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA: timed out | /dns4/edge.dragon.aberration.technology/tcp/443/wss: websocket handshake failed".into()
        )
    );
}

#[test]
fn browser_wss_fallback_peers_disconnect_once_direct_snapshot_path_exists() {
    let connected = BTreeMap::from([
        (PeerId::new("peer-wss"), BrowserTransportFamily::WssFallback),
        (
            PeerId::new("peer-direct"),
            BrowserTransportFamily::WebRtcDirect,
        ),
    ]);
    let snapshot = ControlPlaneSnapshot {
        directory_announcements: vec![super::ExperimentDirectoryAnnouncement {
            network_id: NetworkId::new("browser-handoff"),
            entries: Vec::new(),
            announced_at: Utc::now(),
        }],
        ..ControlPlaneSnapshot::default()
    };

    assert_eq!(
        browser_wss_fallback_peers_to_disconnect(Some(&snapshot), &connected),
        vec![PeerId::new("peer-wss")]
    );
    assert!(
        browser_wss_fallback_peers_to_disconnect(None, &connected).is_empty(),
        "the bootstrap wss peer should only be dropped after the direct swarm has actual state"
    );
}

#[test]
fn preferred_wasm_browser_request_peer_id_promotes_direct_peer_over_oldest_wss_peer() {
    let connected_peer_ids = vec![PeerId::new("peer-wss"), PeerId::new("peer-direct")];
    let connected = BTreeMap::from([
        (PeerId::new("peer-wss"), BrowserTransportFamily::WssFallback),
        (
            PeerId::new("peer-direct"),
            BrowserTransportFamily::WebRtcDirect,
        ),
    ]);

    assert_eq!(
        preferred_wasm_browser_request_peer_id(&connected_peer_ids, &connected, &[]),
        Some(PeerId::new("peer-direct"))
    );
}

#[test]
fn browser_direct_seed_retry_candidates_reappear_after_retry_cooldown() {
    let bootstrap = BrowserSwarmBootstrap {
        network_id: NetworkId::new("browser-handoff"),
        seed_bootstrap: BrowserResolvedSeedBootstrap {
            source: BrowserSeedBootstrapSource::EdgeSigned,
            seed_node_urls: vec![
                "/dns4/bootstrap.example/udp/443/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
                "/dns4/bootstrap.example/tcp/443/wss".into(),
            ],
            advertised_seed_count: 2,
            last_error: None,
        },
        transport_preference: vec![
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WssFallback,
        ],
        selected_experiment: None,
        selected_revision: None,
    };
    let connected =
        BTreeMap::from([(PeerId::new("peer-wss"), BrowserTransportFamily::WssFallback)]);
    let recent_attempts = BTreeMap::from([(
        String::from(
            "/dns4/bootstrap.example/udp/443/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ),
        Instant::now(),
    )]);
    assert!(
        browser_direct_seed_retry_candidates(
            &bootstrap,
            &[
                BrowserTransportFamily::WebRtcDirect,
                BrowserTransportFamily::WssFallback,
            ],
            &connected,
            &recent_attempts,
        )
        .is_empty(),
        "recent direct attempts should observe a cooldown"
    );

    let stale_attempts = BTreeMap::from([(
        String::from(
            "/dns4/bootstrap.example/udp/443/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ),
        Instant::now() - Duration::from_secs(10),
    )]);
    let candidates = browser_direct_seed_retry_candidates(
        &bootstrap,
        &[
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WssFallback,
        ],
        &connected,
        &stale_attempts,
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].transport,
        BrowserTransportFamily::WebRtcDirect
    );
}

#[test]
fn browser_should_retry_direct_handoff_while_only_wss_is_connected() {
    let bootstrap = BrowserSwarmBootstrap {
        network_id: NetworkId::new("browser-handoff"),
        seed_bootstrap: BrowserResolvedSeedBootstrap {
            source: BrowserSeedBootstrapSource::EdgeSigned,
            seed_node_urls: vec![
                "/dns4/bootstrap.example/udp/443/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
                "/dns4/bootstrap.example/tcp/443/wss".into(),
            ],
            advertised_seed_count: 2,
            last_error: None,
        },
        transport_preference: vec![
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WssFallback,
        ],
        selected_experiment: None,
        selected_revision: None,
    };
    let connected_peer_ids = vec![PeerId::new("peer-wss")];
    let connected =
        BTreeMap::from([(PeerId::new("peer-wss"), BrowserTransportFamily::WssFallback)]);

    assert!(browser_should_retry_direct_handoff(
        Some(&bootstrap),
        None,
        &connected_peer_ids,
        &connected,
    ));

    let connected_direct = BTreeMap::from([
        (PeerId::new("peer-wss"), BrowserTransportFamily::WssFallback),
        (
            PeerId::new("peer-direct"),
            BrowserTransportFamily::WebRtcDirect,
        ),
    ]);
    let connected_direct_peer_ids = vec![PeerId::new("peer-wss"), PeerId::new("peer-direct")];
    assert!(!browser_should_retry_direct_handoff(
        Some(&bootstrap),
        None,
        &connected_direct_peer_ids,
        &connected_direct,
    ));
}

#[test]
fn planned_browser_swarm_runtime_connect_reports_transport_selected_from_bootstrap() {
    let bootstrap = BrowserSwarmBootstrap {
        network_id: NetworkId::new("net-browser"),
        seed_bootstrap: BrowserResolvedSeedBootstrap {
            source: BrowserSeedBootstrapSource::EdgeSigned,
            seed_node_urls: vec![
                "/dns4/bootstrap.example/udp/4001/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    .into(),
            ],
            advertised_seed_count: 1,
            last_error: None,
        },
        transport_preference: vec![
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WebTransport,
        ],
        selected_experiment: None,
        selected_revision: None,
    };
    let mut runtime = PlannedBrowserSwarmRuntime::default();

    let plan = block_on(runtime.connect(bootstrap)).expect("planned connect");
    let status = runtime.status();

    assert_eq!(
        plan.desired_transport,
        Some(BrowserTransportFamily::WebRtcDirect)
    );
    assert_eq!(status.phase, BrowserSwarmPhase::TransportSelected);
    assert_eq!(
        status.transport_source,
        BrowserTransportObservationSource::Selected
    );
    assert_eq!(
        status.desired_transport,
        Some(BrowserTransportFamily::WebRtcDirect)
    );
    assert_eq!(status.connected_transport, None);
    assert_eq!(status.connected_peer_count, 0);
}

#[test]
fn browser_subscription_topics_follow_selected_experiment_after_directory_seed() {
    let network_id = NetworkId::new("browser-topics");
    let study_id = StudyId::new("study-browser");
    let experiment_id = ExperimentId::new("exp-browser");
    let revision_id = RevisionId::new("rev-browser");
    let bootstrap = BrowserSwarmBootstrap {
        network_id: network_id.clone(),
        seed_bootstrap: BrowserResolvedSeedBootstrap {
            source: BrowserSeedBootstrapSource::EdgeSigned,
            seed_node_urls: vec![
                "/dns4/bootstrap.example/udp/4001/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    .into(),
            ],
            advertised_seed_count: 1,
            last_error: None,
        },
        transport_preference: vec![BrowserTransportFamily::WebRtcDirect],
        selected_experiment: Some(experiment_id.clone()),
        selected_revision: Some(revision_id.clone()),
    };
    let directory_entry = ExperimentDirectoryEntry {
        network_id: network_id.clone(),
        study_id: study_id.clone(),
        experiment_id: experiment_id.clone(),
        workload_id: WorkloadId::new("workload-browser"),
        display_name: "browser".into(),
        model_schema_hash: ContentId::new("model"),
        dataset_view_id: DatasetViewId::new("dataset"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 0,
            estimated_window_seconds: 30,
        },
        visibility: ExperimentVisibility::OptIn,
        opt_in_policy: ExperimentOptInPolicy::Scoped,
        current_revision_id: revision_id.clone(),
        current_head_id: None,
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::new(),
        metadata: BTreeMap::new(),
    };
    let snapshot = ControlPlaneSnapshot {
        directory_announcements: vec![super::ExperimentDirectoryAnnouncement {
            network_id: network_id.clone(),
            entries: vec![directory_entry],
            announced_at: Utc::now(),
        }],
        ..ControlPlaneSnapshot::default()
    };

    let selected = selected_browser_study_and_experiment(&bootstrap, &snapshot);
    assert_eq!(selected, Some((study_id.clone(), experiment_id.clone())));

    let topics = super::desired_wasm_browser_subscription_topics(
        Some(&bootstrap),
        Some(&snapshot),
        true,
        true,
        true,
    );
    let paths = topics
        .into_iter()
        .map(|topic| topic.path)
        .collect::<Vec<_>>();
    assert!(paths.contains(&OverlayTopic::control(network_id.clone()).path));
    assert!(
        paths.contains(
            &OverlayTopic::experiment(
                network_id.clone(),
                study_id.clone(),
                experiment_id.clone(),
                OverlayChannel::Heads,
            )
            .expect("heads topic")
            .path
        )
    );
    assert!(
        paths.contains(
            &OverlayTopic::experiment(network_id, study_id, experiment_id, OverlayChannel::Metrics)
                .expect("metrics topic")
                .path
        )
    );
}

#[test]
fn browser_swarm_status_advances_to_head_synced_from_selected_snapshot() {
    let network_id = NetworkId::new("browser-status");
    let study_id = StudyId::new("study-browser");
    let experiment_id = ExperimentId::new("exp-browser");
    let revision_id = RevisionId::new("rev-browser");
    let bootstrap = BrowserSwarmBootstrap {
        network_id: network_id.clone(),
        seed_bootstrap: BrowserResolvedSeedBootstrap {
            source: BrowserSeedBootstrapSource::EdgeSigned,
            seed_node_urls: vec![
                "/dns4/bootstrap.example/udp/4001/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    .into(),
            ],
            advertised_seed_count: 1,
            last_error: None,
        },
        transport_preference: vec![BrowserTransportFamily::WebRtcDirect],
        selected_experiment: Some(experiment_id.clone()),
        selected_revision: Some(revision_id.clone()),
    };
    let snapshot = ControlPlaneSnapshot {
        directory_announcements: vec![super::ExperimentDirectoryAnnouncement {
            network_id: network_id.clone(),
            entries: vec![ExperimentDirectoryEntry {
                network_id,
                study_id: study_id.clone(),
                experiment_id: experiment_id.clone(),
                workload_id: WorkloadId::new("workload-browser"),
                display_name: "browser".into(),
                model_schema_hash: ContentId::new("model"),
                dataset_view_id: DatasetViewId::new("dataset"),
                resource_requirements: ExperimentResourceRequirements {
                    minimum_roles: BTreeSet::new(),
                    minimum_device_memory_bytes: None,
                    minimum_system_memory_bytes: None,
                    estimated_download_bytes: 0,
                    estimated_window_seconds: 30,
                },
                visibility: ExperimentVisibility::OptIn,
                opt_in_policy: ExperimentOptInPolicy::Scoped,
                current_revision_id: revision_id.clone(),
                current_head_id: None,
                allowed_roles: PeerRoleSet::default(),
                allowed_scopes: BTreeSet::new(),
                metadata: BTreeMap::new(),
            }],
            announced_at: Utc::now(),
        }],
        head_announcements: vec![HeadAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("browser-status"),
                study_id.clone(),
                experiment_id.clone(),
                OverlayChannel::Heads,
            )
            .expect("heads topic"),
            provider_peer_id: Some(PeerId::new("peer-browser")),
            head: HeadDescriptor {
                head_id: HeadId::new("head-browser"),
                study_id,
                experiment_id,
                revision_id,
                artifact_id: ArtifactId::new("artifact-browser"),
                parent_head_id: None,
                global_step: 0,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        }],
        ..ControlPlaneSnapshot::default()
    };
    let mut status = BrowserSwarmStatus {
        phase: BrowserSwarmPhase::TransportConnected,
        seed_bootstrap: bootstrap.seed_bootstrap.clone(),
        transport_source: BrowserTransportObservationSource::Connected,
        desired_transport: Some(BrowserTransportFamily::WebRtcDirect),
        connected_transport: Some(BrowserTransportFamily::WebRtcDirect),
        connected_peer_count: 1,
        connected_peer_ids: vec![PeerId::new("peer-browser")],
        ..BrowserSwarmStatus::default()
    };

    update_wasm_browser_status_from_snapshot(&mut status, Some(&snapshot), Some(&bootstrap));

    assert!(status.directory_synced);
    assert!(status.assignment_bound);
    assert!(status.head_synced);
    assert_eq!(status.phase, BrowserSwarmPhase::HeadSynced);
}

#[test]
fn planned_browser_swarm_runtime_plan_connect_matches_async_connect_state() {
    let bootstrap = BrowserSwarmBootstrap {
        network_id: NetworkId::new("net-browser"),
        seed_bootstrap: BrowserResolvedSeedBootstrap {
            source: BrowserSeedBootstrapSource::SiteConfigFallback,
            seed_node_urls: vec!["/dns4/bootstrap.example/tcp/443/wss".into()],
            advertised_seed_count: 0,
            last_error: None,
        },
        transport_preference: vec![BrowserTransportFamily::WssFallback],
        selected_experiment: None,
        selected_revision: None,
    };
    let mut runtime = PlannedBrowserSwarmRuntime::default();

    let plan = runtime.plan_connect(bootstrap);

    assert_eq!(
        plan.desired_transport,
        Some(BrowserTransportFamily::WssFallback)
    );
    assert_eq!(
        runtime
            .dial_plan_ref()
            .and_then(|plan| plan.desired_transport.clone()),
        Some(BrowserTransportFamily::WssFallback)
    );
    assert_eq!(
        runtime.status_ref().seed_bootstrap.source,
        BrowserSeedBootstrapSource::SiteConfigFallback
    );
    assert_eq!(
        runtime.status_ref().phase,
        BrowserSwarmPhase::TransportSelected
    );
}

fn semantic_test_overlay() -> OverlayTopic {
    OverlayTopic::experiment(
        NetworkId::new("semantic-net"),
        StudyId::new("study"),
        burn_p2p_core::ExperimentId::new("exp"),
        OverlayChannel::Heads,
    )
    .expect("semantic overlay")
}

fn semantic_test_aggregate(
    reducer_peer_id: &str,
    aggregate_id: &str,
    artifact_id: &str,
    providers: &[&str],
    announced_at: chrono::DateTime<Utc>,
    published_at: chrono::DateTime<Utc>,
) -> AggregateProposalAnnouncement {
    AggregateProposalAnnouncement {
        overlay: semantic_test_overlay(),
        proposal: AggregateEnvelope {
            aggregate_id: ContentId::new(aggregate_id),
            study_id: StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            window_id: WindowId(7),
            base_head_id: HeadId::new("head-base"),
            aggregate_artifact_id: ArtifactId::new(artifact_id),
            tier: AggregateTier::RootCandidate,
            reducer_peer_id: PeerId::new(reducer_peer_id),
            contributor_peers: vec![PeerId::new("trainer-b"), PeerId::new("trainer-a")],
            child_aggregate_ids: Vec::new(),
            stats: AggregateStats {
                accepted_updates: 2,
                duplicate_updates: 0,
                dropped_updates: 0,
                late_updates: 0,
                sum_sample_weight: 8.0,
                sum_quality_weight: 1.75,
                sum_weighted_delta_norm: 3.25,
                max_update_norm: 1.5,
                accepted_sample_coverage: 1.0,
            },
            providers: providers.iter().map(|peer| PeerId::new(*peer)).collect(),
            published_at,
        },
        announced_at,
    }
}

fn semantic_test_reduction_certificate(
    validator: &str,
    aggregate_id: &str,
    quorum: u16,
    issued_at: chrono::DateTime<Utc>,
) -> ReductionCertificateAnnouncement {
    ReductionCertificateAnnouncement {
        overlay: semantic_test_overlay(),
        certificate: ReductionCertificate {
            reduction_id: ContentId::new(format!("reduction-{validator}")),
            study_id: StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            window_id: WindowId(7),
            base_head_id: HeadId::new("head-base"),
            aggregate_id: ContentId::new(aggregate_id),
            promoter_peer_id: PeerId::new(validator),
            promotion_mode: burn_p2p_core::HeadPromotionMode::ValidatorQuorum,
            promotion_quorum: quorum,
            cross_checked_reducers: vec![PeerId::new("reducer-b"), PeerId::new("reducer-a")],
            issued_at,
        },
        announced_at: issued_at,
    }
}

fn semantic_test_validation_quorum(
    coordinator: &str,
    aggregate_id: &str,
    merged_head_id: &str,
    validators: &[&str],
    reduction_ids: &[&str],
    issued_at: chrono::DateTime<Utc>,
) -> ValidationQuorumAnnouncement {
    ValidationQuorumAnnouncement {
        overlay: semantic_test_overlay(),
        certificate: ValidationQuorumCertificate {
            quorum_cert_id: ContentId::new(format!("quorum-{aggregate_id}-{merged_head_id}")),
            study_id: StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            window_id: WindowId(7),
            base_head_id: HeadId::new("head-base"),
            aggregate_id: ContentId::new(aggregate_id),
            aggregate_artifact_id: ArtifactId::new("artifact-a"),
            merged_head_id: HeadId::new(merged_head_id),
            promotion_mode: burn_p2p_core::HeadPromotionMode::ValidatorQuorum,
            validator_quorum: 2,
            coordinator: PeerId::new(coordinator),
            attesting_validators: validators.iter().map(|peer| PeerId::new(*peer)).collect(),
            reduction_ids: reduction_ids.iter().map(|id| ContentId::new(*id)).collect(),
            issued_at,
        },
        announced_at: issued_at,
    }
}

fn semantic_test_peer_directory(
    peer_id: &str,
    addresses: &[&str],
    roles: Option<PeerRoleSet>,
    announced_at: chrono::DateTime<Utc>,
) -> PeerDirectoryAnnouncement {
    PeerDirectoryAnnouncement {
        network_id: NetworkId::new("semantic-net"),
        peer_id: PeerId::new(peer_id),
        addresses: addresses
            .iter()
            .map(|address| SwarmAddress::new(*address).expect("peer directory address"))
            .collect(),
        advertised_roles: roles,
        announced_at,
    }
}

#[test]
fn pubsub_message_id_ignores_timestamps_and_provider_order_for_semantic_duplicates() {
    let now = Utc::now();
    let first = PubsubEnvelope {
        topic_path: semantic_test_overlay().path.clone(),
        payload: PubsubPayload::AggregateProposal(semantic_test_aggregate(
            "reducer-a",
            "aggregate-a",
            "artifact-a",
            &["provider-b", "provider-a"],
            now,
            now,
        )),
        published_at: now,
    };
    let second = PubsubEnvelope {
        topic_path: semantic_test_overlay().path.clone(),
        payload: PubsubPayload::AggregateProposal(semantic_test_aggregate(
            "reducer-a",
            "aggregate-a",
            "artifact-a",
            &["provider-a", "provider-b"],
            now + chrono::Duration::seconds(30),
            now + chrono::Duration::seconds(45),
        )),
        published_at: now + chrono::Duration::seconds(60),
    };

    let first_id = super::pubsub_semantic_message_id(
        &serde_json::to_vec(&first).expect("encode first envelope"),
    );
    let second_id = super::pubsub_semantic_message_id(
        &serde_json::to_vec(&second).expect("encode second envelope"),
    );

    assert_eq!(first_id, second_id);
}

#[test]
fn aggregate_proposal_announcements_coalesce_semantic_duplicates_and_union_providers() {
    let now = Utc::now();
    let mut snapshot = ControlPlaneSnapshot::default();
    super::apply_pubsub_payload(
        &mut snapshot,
        PubsubPayload::AggregateProposal(semantic_test_aggregate(
            "reducer-a",
            "aggregate-a",
            "artifact-a",
            &["provider-a"],
            now,
            now,
        )),
    );
    super::apply_pubsub_payload(
        &mut snapshot,
        PubsubPayload::AggregateProposal(semantic_test_aggregate(
            "reducer-a",
            "aggregate-a",
            "artifact-a",
            &["provider-c", "provider-b"],
            now + chrono::Duration::seconds(10),
            now + chrono::Duration::seconds(20),
        )),
    );

    assert_eq!(snapshot.aggregate_proposal_announcements.len(), 1);
    assert_eq!(
        snapshot.aggregate_proposal_announcements[0]
            .proposal
            .providers
            .iter()
            .map(|peer| peer.as_str())
            .collect::<Vec<_>>(),
        vec!["provider-a", "provider-b", "provider-c"]
    );
}

#[test]
fn peer_directory_message_id_ignores_timestamps_and_address_order() {
    let now = Utc::now();
    let first = PubsubEnvelope {
        topic_path: OverlayTopic::control(NetworkId::new("semantic-net"))
            .path
            .clone(),
        payload: PubsubPayload::PeerDirectory(semantic_test_peer_directory(
            "peer-a",
            &["/ip4/127.0.0.1/tcp/4011", "/ip4/127.0.0.1/tcp/4012"],
            Some(PeerRoleSet::new([
                burn_p2p_core::PeerRole::TrainerCpu,
                burn_p2p_core::PeerRole::Validator,
            ])),
            now,
        )),
        published_at: now,
    };
    let second = PubsubEnvelope {
        topic_path: OverlayTopic::control(NetworkId::new("semantic-net"))
            .path
            .clone(),
        payload: PubsubPayload::PeerDirectory(semantic_test_peer_directory(
            "peer-a",
            &["/ip4/127.0.0.1/tcp/4012", "/ip4/127.0.0.1/tcp/4011"],
            Some(PeerRoleSet::new([
                burn_p2p_core::PeerRole::Validator,
                burn_p2p_core::PeerRole::TrainerCpu,
            ])),
            now + chrono::Duration::seconds(30),
        )),
        published_at: now + chrono::Duration::seconds(45),
    };

    let first_id = super::pubsub_semantic_message_id(
        &serde_json::to_vec(&first).expect("encode first envelope"),
    );
    let second_id = super::pubsub_semantic_message_id(
        &serde_json::to_vec(&second).expect("encode second envelope"),
    );

    assert_eq!(first_id, second_id);
}

#[test]
fn peer_directory_announcements_coalesce_by_peer_and_union_addresses() {
    let now = Utc::now();
    let mut snapshot = ControlPlaneSnapshot::default();
    super::apply_pubsub_payload(
        &mut snapshot,
        PubsubPayload::PeerDirectory(semantic_test_peer_directory(
            "peer-a",
            &["/ip4/127.0.0.1/tcp/4021"],
            Some(PeerRoleSet::new([burn_p2p_core::PeerRole::TrainerCpu])),
            now,
        )),
    );
    super::apply_pubsub_payload(
        &mut snapshot,
        PubsubPayload::PeerDirectory(semantic_test_peer_directory(
            "peer-a",
            &["/ip4/127.0.0.1/tcp/4022", "/ip4/127.0.0.1/tcp/4021"],
            Some(PeerRoleSet::new([burn_p2p_core::PeerRole::Validator])),
            now + chrono::Duration::seconds(10),
        )),
    );

    assert_eq!(snapshot.peer_directory_announcements.len(), 1);
    assert_eq!(
        snapshot.peer_directory_announcements[0]
            .addresses
            .iter()
            .map(|address| address.as_str())
            .collect::<Vec<_>>(),
        vec!["/ip4/127.0.0.1/tcp/4021", "/ip4/127.0.0.1/tcp/4022"]
    );
    assert_eq!(
        snapshot.peer_directory_announcements[0]
            .advertised_roles
            .as_ref()
            .expect("advertised roles")
            .roles,
        BTreeSet::from([
            burn_p2p_core::PeerRole::TrainerCpu,
            burn_p2p_core::PeerRole::Validator,
        ])
    );
}

#[test]
fn aggregate_proposals_with_same_semantic_key_and_conflicting_payloads_are_hard_rejected() {
    let now = Utc::now();
    let mut snapshot = ControlPlaneSnapshot::default();
    super::apply_pubsub_payload(
        &mut snapshot,
        PubsubPayload::AggregateProposal(semantic_test_aggregate(
            "reducer-a",
            "aggregate-a",
            "artifact-a",
            &["provider-a"],
            now,
            now,
        )),
    );
    super::apply_pubsub_payload(
        &mut snapshot,
        PubsubPayload::AggregateProposal(semantic_test_aggregate(
            "reducer-a",
            "aggregate-a",
            "artifact-conflict",
            &["provider-b"],
            now + chrono::Duration::seconds(5),
            now + chrono::Duration::seconds(5),
        )),
    );

    assert_eq!(snapshot.aggregate_proposal_announcements.len(), 1);
    assert_eq!(
        snapshot.aggregate_proposal_announcements[0]
            .proposal
            .aggregate_artifact_id,
        ArtifactId::new("artifact-a")
    );
}

#[test]
fn reduction_certificate_announcements_cap_at_validator_quorum() {
    let now = Utc::now();
    let mut snapshot = ControlPlaneSnapshot::default();
    for (offset, validator) in ["validator-a", "validator-b", "validator-c", "validator-d"]
        .into_iter()
        .enumerate()
    {
        super::apply_pubsub_payload(
            &mut snapshot,
            PubsubPayload::ReductionCertificate(semantic_test_reduction_certificate(
                validator,
                "aggregate-a",
                2,
                now + chrono::Duration::seconds(offset as i64),
            )),
        );
    }

    assert_eq!(snapshot.reduction_certificate_announcements.len(), 2);
    assert_eq!(
        snapshot
            .reduction_certificate_announcements
            .iter()
            .map(|announcement| announcement.certificate.promoter_peer_id.as_str())
            .collect::<Vec<_>>(),
        vec!["validator-a", "validator-b"]
    );
}

#[test]
fn validation_quorum_announcements_dedupe_semantic_replays() {
    let now = Utc::now();
    let mut snapshot = ControlPlaneSnapshot::default();
    super::apply_pubsub_payload(
        &mut snapshot,
        PubsubPayload::ValidationQuorum(semantic_test_validation_quorum(
            "validator-a",
            "aggregate-a",
            "merged-head-a",
            &["validator-b", "validator-a"],
            &["reduction-b", "reduction-a"],
            now,
        )),
    );
    super::apply_pubsub_payload(
        &mut snapshot,
        PubsubPayload::ValidationQuorum(semantic_test_validation_quorum(
            "validator-a",
            "aggregate-a",
            "merged-head-a",
            &["validator-a", "validator-b"],
            &["reduction-a", "reduction-b"],
            now + chrono::Duration::seconds(30),
        )),
    );

    assert_eq!(snapshot.validation_quorum_announcements.len(), 1);
    let certificate = &snapshot.validation_quorum_announcements[0].certificate;
    assert_eq!(
        certificate
            .attesting_validators
            .iter()
            .map(|peer| peer.as_str())
            .collect::<Vec<_>>(),
        vec!["validator-a", "validator-b"]
    );
    assert_eq!(
        certificate
            .reduction_ids
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
        vec!["reduction-a", "reduction-b"]
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
        browser_capabilities: BTreeSet::new(),
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
fn peer_store_prunes_stale_and_excess_disconnected_observations() {
    let now = Utc::now();
    let mut store = PeerStore::default();

    for offset in 0..300 {
        store.mark_connection(
            PeerId::new(format!("disconnected-{offset}")),
            false,
            now - chrono::Duration::minutes(offset as i64),
        );
    }
    store.mark_connection(PeerId::new("connected-a"), true, now);
    store.mark_connection(PeerId::new("connected-b"), true, now);

    let disconnected = store
        .observations()
        .filter(|observation| !observation.connected)
        .count();

    assert!(disconnected <= 256);
    assert!(store.get(&PeerId::new("connected-a")).is_some());
    assert!(store.get(&PeerId::new("connected-b")).is_some());
    assert!(store.get(&PeerId::new("disconnected-0")).is_some());
    assert!(store.get(&PeerId::new("disconnected-299")).is_none());
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
    let runtime = RuntimeBoundary::for_platform_and_roles(
        &burn_p2p_core::GenesisSpec {
            network_id: NetworkId::new("network"),
            protocol_version: Version::new(0, 1, 0),
            display_name: "network".into(),
            created_at: Utc::now(),
            metadata: BTreeMap::new(),
        },
        ClientPlatform::Browser,
        &PeerRoleSet::default_trainer(),
        vec![SwarmAddress::new("/dns4/bootstrap.example.com/tcp/4001/ws").expect("addr")],
        vec![SwarmAddress::new("/ip4/0.0.0.0/udp/4001/quic-v1").expect("addr")],
        Vec::new(),
        None,
    )
    .expect("runtime");

    assert_eq!(
        runtime.transport_policy,
        RuntimeTransportPolicy::browser_for_roles(&PeerRoleSet::default_trainer())
    );
    assert_eq!(
        runtime.protocols.control.as_str(),
        "/burn-p2p/network/v1/control"
    );
}

#[test]
fn native_runtime_boundary_defaults_to_browser_dialable_direct_listener() {
    let runtime = RuntimeBoundary::for_platform_and_roles(
        &burn_p2p_core::GenesisSpec {
            network_id: NetworkId::new("network"),
            protocol_version: Version::new(0, 1, 0),
            display_name: "network".into(),
            created_at: Utc::now(),
            metadata: BTreeMap::new(),
        },
        ClientPlatform::Native,
        &PeerRoleSet::default_trainer(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    )
    .expect("runtime");

    assert_eq!(
        runtime.listen_addresses[0],
        SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("tcp addr")
    );
    assert!(
        runtime
            .listen_addresses
            .iter()
            .any(|address| address.as_str().contains("/webrtc-direct")),
        "native default boundary should expose a browser-dialable direct listener"
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
        peer_window_hints: Vec::new(),
        placement_snapshot: None,
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
                peer_window_hints: Vec::new(),
                placement_snapshot: None,
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
fn control_plane_snapshot_caps_live_announcement_histories() {
    const MAX_HEADS: usize = 256;
    const MAX_LEASES: usize = 128;
    const MAX_MERGE_WINDOWS: usize = 128;
    const MAX_REDUCER_ASSIGNMENTS: usize = 128;
    const MAX_UPDATES: usize = 256;
    const MAX_REDUCER_LOADS: usize = 128;

    let overlay = semantic_test_overlay();
    let now = Utc::now();
    let mut snapshot = ControlPlaneSnapshot::default();

    for index in 0..320u64 {
        snapshot.insert_head_announcement(HeadAnnouncement {
            overlay: overlay.clone(),
            provider_peer_id: Some(PeerId::new(format!("provider-{index:03}"))),
            head: HeadDescriptor {
                study_id: StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                head_id: HeadId::new(format!("head-{index:03}")),
                artifact_id: ArtifactId::new(format!("artifact-{index:03}")),
                parent_head_id: None,
                global_step: index,
                created_at: now + chrono::Duration::seconds(index as i64),
                metrics: BTreeMap::new(),
            },
            announced_at: now + chrono::Duration::seconds(index as i64),
        });
        snapshot.insert_lease_announcement(super::LeaseAnnouncement {
            overlay: overlay.clone(),
            lease: burn_p2p_core::AssignmentLease {
                lease_id: burn_p2p_core::LeaseId::new(format!("lease-{index:03}")),
                network_id: NetworkId::new("semantic-net"),
                study_id: StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                peer_id: PeerId::new(format!("trainer-{index:03}")),
                dataset_view_id: burn_p2p_core::DatasetViewId::new("dataset"),
                window_id: WindowId(index),
                granted_at: now + chrono::Duration::seconds(index as i64),
                expires_at: now + chrono::Duration::seconds(index as i64 + 60),
                budget_work_units: 1,
                microshards: vec![burn_p2p_core::MicroShardId::new(format!(
                    "microshard-{index:03}"
                ))],
                assignment_hash: ContentId::new(format!("assignment-{index:03}")),
            },
            announced_at: now + chrono::Duration::seconds(index as i64),
        });
        snapshot.insert_merge_window_announcement(super::MergeWindowAnnouncement {
            overlay: overlay.clone(),
            merge_window: burn_p2p_core::MergeWindowState {
                merge_window_id: ContentId::new(format!("merge-window-{index:03}")),
                network_id: NetworkId::new("semantic-net"),
                study_id: StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                window_id: WindowId(index),
                base_head_id: HeadId::new("base-head"),
                policy: burn_p2p_core::MergeTopologyPolicy::default(),
                reducers: vec![PeerId::new("reducer-a")],
                validators: vec![PeerId::new("validator-a")],
                opened_at: now + chrono::Duration::seconds(index as i64),
                closes_at: now + chrono::Duration::seconds(index as i64 + 30),
            },
            announced_at: now + chrono::Duration::seconds(index as i64),
        });
        snapshot.insert_reducer_assignment_announcement(super::ReducerAssignmentAnnouncement {
            overlay: overlay.clone(),
            assignment: burn_p2p_core::ReducerAssignment {
                assignment_id: ContentId::new(format!("assignment-{index:03}")),
                window_id: WindowId(index),
                source_peer_id: PeerId::new(format!("trainer-{index:03}")),
                assigned_reducers: vec![PeerId::new("reducer-a")],
                repair_reducers: vec![PeerId::new("reducer-b")],
                upper_tier_reducers: vec![PeerId::new("reducer-root")],
                assigned_at: now + chrono::Duration::seconds(index as i64),
            },
            announced_at: now + chrono::Duration::seconds(index as i64),
        });
        snapshot.insert_update_announcement(super::UpdateEnvelopeAnnouncement {
            overlay: overlay.clone(),
            update: burn_p2p_core::UpdateAnnounce {
                peer_id: PeerId::new(format!("trainer-{index:03}")),
                study_id: StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                window_id: WindowId(index),
                base_head_id: HeadId::new("base-head"),
                lease_id: Some(burn_p2p_core::LeaseId::new(format!("lease-{index:03}"))),
                delta_artifact_id: ArtifactId::new(format!("delta-{index:03}")),
                sample_weight: 1.0,
                quality_weight: 1.0,
                norm_stats: burn_p2p_core::UpdateNormStats {
                    l2_norm: 1.0,
                    max_abs: 1.0,
                    clipped: false,
                    non_finite_tensors: 0,
                },
                feature_sketch: None,
                receipt_root: ContentId::new(format!("receipt-root-{index:03}")),
                receipt_ids: Vec::new(),
                providers: vec![PeerId::new("provider-a")],
                announced_at: now + chrono::Duration::seconds(index as i64),
            },
        });
        snapshot.insert_reducer_load_announcement(super::ReducerLoadAnnouncement {
            overlay: overlay.clone(),
            report: burn_p2p_core::ReducerLoadReport {
                peer_id: PeerId::new(format!("reducer-{index:03}")),
                window_id: WindowId(index),
                assigned_leaf_updates: 1,
                assigned_aggregate_inputs: 1,
                ingress_bytes: index as u128,
                egress_bytes: index as u128,
                duplicate_transfer_ratio: 0.0,
                overload_ratio: 0.0,
                reported_at: now + chrono::Duration::seconds(index as i64),
            },
        });
    }

    assert_eq!(snapshot.head_announcements.len(), MAX_HEADS);
    assert_eq!(snapshot.lease_announcements.len(), MAX_LEASES);
    assert_eq!(snapshot.merge_window_announcements.len(), MAX_MERGE_WINDOWS);
    assert_eq!(
        snapshot.reducer_assignment_announcements.len(),
        MAX_REDUCER_ASSIGNMENTS
    );
    assert_eq!(snapshot.update_announcements.len(), MAX_UPDATES);
    assert_eq!(snapshot.reducer_load_announcements.len(), MAX_REDUCER_LOADS);

    assert_eq!(
        snapshot
            .head_announcements
            .first()
            .map(|announcement| announcement.head.global_step),
        Some((320 - MAX_HEADS) as u64)
    );
    assert_eq!(
        snapshot
            .lease_announcements
            .first()
            .map(|announcement| announcement.lease.window_id),
        Some(WindowId((320 - MAX_LEASES) as u64))
    );
    assert_eq!(
        snapshot
            .update_announcements
            .first()
            .map(|announcement| announcement.update.window_id),
        Some(WindowId((320 - MAX_UPDATES) as u64))
    );
    assert_eq!(
        snapshot
            .reducer_load_announcements
            .first()
            .map(|announcement| announcement.report.window_id),
        Some(WindowId((320 - MAX_REDUCER_LOADS) as u64))
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
    let mut listener = MemorySwarmShell::new().expect("memory swarm listener");
    let mut dialer = MemorySwarmShell::new().expect("memory swarm dialer");
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
    let mut listener =
        MemoryControlPlaneShell::new(protocols.control.clone()).expect("memory control listener");
    let mut dialer =
        MemoryControlPlaneShell::new(protocols.control).expect("memory control dialer");
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
            lifecycle_announcements: listener.snapshot().lifecycle_announcements.clone(),
            schedule_announcements: listener.snapshot().schedule_announcements.clone(),
            head_announcements: listener.snapshot().head_announcements.clone(),
            lease_announcements: listener.snapshot().lease_announcements.clone(),
            merge_announcements: listener.snapshot().merge_announcements.clone(),
            merge_window_announcements: listener.snapshot().merge_window_announcements.clone(),
            reducer_assignment_announcements: listener
                .snapshot()
                .reducer_assignment_announcements
                .clone(),
            update_announcements: listener.snapshot().update_announcements.clone(),
            trainer_promotion_attestation_announcements: listener
                .snapshot()
                .trainer_promotion_attestation_announcements
                .clone(),
            diffusion_promotion_certificate_announcements: listener
                .snapshot()
                .diffusion_promotion_certificate_announcements
                .clone(),
            aggregate_proposal_announcements: listener
                .snapshot()
                .aggregate_proposal_announcements
                .clone(),
            reduction_certificate_announcements: listener
                .snapshot()
                .reduction_certificate_announcements
                .clone(),
            validation_quorum_announcements: listener
                .snapshot()
                .validation_quorum_announcements
                .clone(),
            reducer_load_announcements: listener.snapshot().reducer_load_announcements.clone(),
            auth_announcements: listener.snapshot().auth_announcements.clone(),
            directory_announcements: listener.snapshot().directory_announcements.clone(),
            peer_directory_announcements: listener.snapshot().peer_directory_announcements.clone(),
            metrics_announcements: listener.snapshot().metrics_announcements.clone(),
        }
    );
}

#[test]
fn memory_control_plane_shell_survives_reconnect_snapshot_storms() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("storm-net")).expect("protocols");
    let mut listener =
        MemoryControlPlaneShell::new(protocols.control.clone()).expect("memory control listener");
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
        let mut dialer =
            MemoryControlPlaneShell::new(protocols.control.clone()).expect("memory control dialer");
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
    let mut listener = NativeControlPlaneShell::new(
        protocols.control.clone(),
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(
        protocols.control,
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native dialer");
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
fn native_control_plane_shell_exchanges_snapshot_requests_and_responses_over_websocket() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let mut listener = NativeControlPlaneShell::new(
        protocols.control.clone(),
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(
        protocols.control,
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native dialer");
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
            head_id: HeadId::new("head-ws-1"),
            study_id: StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-ws-1"),
            parent_head_id: Some(HeadId::new("genesis-head")),
            global_step: 1,
            created_at: now,
            metrics: BTreeMap::new(),
        },
        announced_at: now,
    });

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0/ws").expect("ws addr"))
        .expect("listen");

    let listen_addr = loop {
        match listener.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("listener did not produce a websocket listen address"),
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
            "native shells did not connect over websocket; listener events: {:?}; dialer events: {:?}",
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
            "dialer did not receive websocket snapshot"
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
        HeadId::new("head-ws-1")
    );
}

#[test]
fn native_control_plane_shell_exchanges_snapshot_requests_and_responses_over_quic() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let mut listener = NativeControlPlaneShell::new(
        protocols.control.clone(),
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(
        protocols.control,
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native dialer");
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
fn native_control_plane_shell_exchanges_snapshot_requests_and_responses_over_webrtc_direct() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let mut listener = NativeControlPlaneShell::new(
        protocols.control.clone(),
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(
        protocols.control,
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native dialer");
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
            head_id: HeadId::new("head-webrtc-1"),
            study_id: StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-webrtc-1"),
            parent_head_id: Some(HeadId::new("genesis-head")),
            global_step: 1,
            created_at: now,
            metrics: BTreeMap::new(),
        },
        announced_at: now,
    });

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/udp/0/webrtc-direct").expect("webrtc addr"))
        .expect("listen");
    dialer
        .listen_on(
            SwarmAddress::new("/ip4/127.0.0.1/udp/0/webrtc-direct").expect("dialer webrtc addr"),
        )
        .expect("dialer listen");

    let listen_addr = loop {
        match listener.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("listener did not produce a WebRTC direct listen address"),
        }
    };

    dialer.dial(listen_addr).expect("dial");

    let mut listener_connected = false;
    let mut dialer_connected = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !(listener_connected && dialer_connected) {
        assert!(
            Instant::now() < deadline,
            "native WebRTC direct shells did not connect"
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
            "dialer did not receive WebRTC direct snapshot"
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
        HeadId::new("head-webrtc-1")
    );
}

#[test]
fn native_control_plane_shell_reserves_and_dials_relay_paths() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let relay_roles = PeerRoleSet::new([
        burn_p2p_core::PeerRole::Bootstrap,
        burn_p2p_core::PeerRole::RelayHelper,
    ]);
    let client_policy = RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer());
    let relay_policy = RuntimeTransportPolicy::native_for_roles(&relay_roles);

    let mut relay_seed =
        NativeControlPlaneShell::new(protocols.control.clone(), relay_policy).expect("relay seed");
    let mut listener =
        NativeControlPlaneShell::new(protocols.control.clone(), client_policy.clone())
            .expect("listener");
    let mut dialer =
        NativeControlPlaneShell::new(protocols.control, client_policy).expect("dialer");

    relay_seed
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("relay addr"))
        .expect("relay listen");
    let relay_seed_addr = loop {
        match relay_seed.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("relay seed did not produce a listen address"),
        }
    };

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("listener addr"))
        .expect("listener listen");
    dialer
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("dialer addr"))
        .expect("dialer listen");

    let drain_until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_until {
        let _ = listener.wait_event(Duration::from_millis(10));
        let _ = dialer.wait_event(Duration::from_millis(10));
    }

    listener
        .dial(relay_seed_addr.clone())
        .expect("dial relay seed");

    let mut relay_addr = None;
    let reservation_deadline = Instant::now() + Duration::from_secs(10);
    while relay_addr.is_none() {
        assert!(
            Instant::now() < reservation_deadline,
            "listener did not confirm a relayed reachable address"
        );

        let _ = relay_seed.wait_event(Duration::from_millis(50));
        if let Some(event) = listener.wait_event(Duration::from_millis(50)) {
            match event {
                LiveControlPlaneEvent::ReachableAddressConfirmed { address }
                | LiveControlPlaneEvent::NewListenAddr { address }
                    if address.is_relay_circuit() =>
                {
                    relay_addr = Some(address);
                }
                _ => {}
            }
        }
    }

    let relay_addr = relay_addr.expect("relay address");
    dialer.dial(relay_addr).expect("dial relayed listener");

    let connect_deadline = Instant::now() + Duration::from_secs(10);
    let mut listener_connected = false;
    let mut dialer_connected = false;
    while !(listener_connected && dialer_connected) {
        assert!(
            Instant::now() < connect_deadline,
            "relayed peers did not connect through relay reservation"
        );

        let _ = relay_seed.wait_event(Duration::from_millis(25));
        if let Some(event) = listener.wait_event(Duration::from_millis(25)) {
            listener_connected |=
                matches!(event, LiveControlPlaneEvent::ConnectionEstablished { .. });
        }
        if let Some(event) = dialer.wait_event(Duration::from_millis(25)) {
            dialer_connected |=
                matches!(event, LiveControlPlaneEvent::ConnectionEstablished { .. });
        }
    }
}

#[test]
fn native_relay_path_transfers_large_artifact_chunks() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let relay_roles = PeerRoleSet::new([
        burn_p2p_core::PeerRole::Bootstrap,
        burn_p2p_core::PeerRole::RelayHelper,
    ]);
    let client_policy = RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer());
    let relay_policy = RuntimeTransportPolicy::native_for_roles(&relay_roles);

    let mut relay_seed =
        NativeControlPlaneShell::new(protocols.control.clone(), relay_policy).expect("relay seed");
    let mut listener =
        NativeControlPlaneShell::new(protocols.control.clone(), client_policy.clone())
            .expect("listener");
    let mut dialer =
        NativeControlPlaneShell::new(protocols.control, client_policy).expect("dialer");

    let chunk_bytes = vec![7_u8; 192 * 1024];
    let chunk_hash =
        ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(&chunk_bytes));
    let descriptor = ArtifactDescriptor {
        artifact_id: ArtifactId::new(
            "12209f1f0f5d4cf894e5fedff11e0fbd6186a0d913ec5337f2b6e2e7f1cf2a665cfd",
        ),
        kind: ArtifactKind::ServeHead,
        head_id: Some(HeadId::new("head-relay-large")),
        base_head_id: None,
        precision: Precision::Fp16,
        model_schema_hash: ContentId::new("schema"),
        record_format: "burn-record:bin".into(),
        bytes_len: chunk_bytes.len() as u64,
        chunks: vec![ChunkDescriptor {
            chunk_id: ChunkId::new(
                "1220684e225e57ea78aa5cd8f6c705b0c82540dd0fef1dbcd63ea1a0dc3bb638ad2f",
            ),
            offset_bytes: 0,
            length_bytes: chunk_bytes.len() as u64,
            chunk_hash: chunk_hash.clone(),
        }],
        root_hash: chunk_hash,
    };
    listener.publish_artifact(
        descriptor.clone(),
        vec![ArtifactChunkPayload {
            artifact_id: descriptor.artifact_id.clone(),
            chunk: descriptor.chunks[0].clone(),
            bytes: chunk_bytes.clone(),
            generated_at: Utc::now(),
        }],
    );

    relay_seed
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("relay addr"))
        .expect("relay listen");
    let relay_seed_addr = loop {
        match relay_seed.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("relay seed did not produce a listen address"),
        }
    };

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("listener addr"))
        .expect("listener listen");
    dialer
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("dialer addr"))
        .expect("dialer listen");

    let drain_until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_until {
        let _ = listener.wait_event(Duration::from_millis(10));
        let _ = dialer.wait_event(Duration::from_millis(10));
    }

    listener
        .dial(relay_seed_addr.clone())
        .expect("dial relay seed");

    let mut relay_addr = None;
    let reservation_deadline = Instant::now() + Duration::from_secs(10);
    while relay_addr.is_none() {
        assert!(
            Instant::now() < reservation_deadline,
            "listener did not confirm a relayed reachable address"
        );

        let _ = relay_seed.wait_event(Duration::from_millis(50));
        if let Some(event) = listener.wait_event(Duration::from_millis(50)) {
            match event {
                LiveControlPlaneEvent::ReachableAddressConfirmed { address }
                | LiveControlPlaneEvent::NewListenAddr { address }
                    if address.is_relay_circuit() =>
                {
                    relay_addr = Some(address);
                }
                _ => {}
            }
        }
    }

    let relay_addr = relay_addr.expect("relay address");
    dialer.dial(relay_addr).expect("dial relayed listener");
    let listener_peer_id = listener.local_peer_id().to_string();

    let connect_deadline = Instant::now() + Duration::from_secs(10);
    let mut listener_connected = false;
    let mut dialer_connected = false;
    while !(listener_connected && dialer_connected) {
        assert!(
            Instant::now() < connect_deadline,
            "relayed peers did not connect through relay reservation"
        );

        let _ = relay_seed.wait_event(Duration::from_millis(25));
        if let Some(event) = listener.wait_event(Duration::from_millis(25)) {
            listener_connected |=
                matches!(event, LiveControlPlaneEvent::ConnectionEstablished { .. });
        }
        if let Some(event) = dialer.wait_event(Duration::from_millis(25)) {
            dialer_connected |=
                matches!(event, LiveControlPlaneEvent::ConnectionEstablished { .. });
        }
    }

    dialer
        .request_artifact_chunk(
            &listener_peer_id,
            descriptor.artifact_id.clone(),
            descriptor.chunks[0].chunk_id.clone(),
        )
        .expect("request chunk");
    let deadline = Instant::now() + Duration::from_secs(15);
    let chunk_payload = loop {
        assert!(Instant::now() < deadline, "fetch relayed chunk");
        let _ = relay_seed.wait_event(Duration::from_millis(10));
        if let Some(event) = listener.wait_event(Duration::from_millis(10)) {
            match event {
                LiveControlPlaneEvent::ArtifactChunkRequested { .. }
                | LiveControlPlaneEvent::ConnectionEstablished { .. }
                | LiveControlPlaneEvent::Other { .. } => {}
                LiveControlPlaneEvent::ResponseSendFailure { message, .. } => {
                    panic!("listener failed to send chunk response: {message}");
                }
                _ => {}
            }
        }
        if let Some(event) = dialer.wait_event(Duration::from_millis(10)) {
            match event {
                LiveControlPlaneEvent::ArtifactChunkReceived { payload, .. } => {
                    break payload.expect("chunk payload");
                }
                LiveControlPlaneEvent::RequestFailure { message, .. } => {
                    panic!("fetch relayed chunk: {message}");
                }
                _ => {}
            }
        }
    };

    assert_eq!(chunk_payload.bytes.len(), chunk_bytes.len());
    assert_eq!(chunk_payload.bytes, chunk_bytes);
    assert_eq!(chunk_payload.chunk, descriptor.chunks[0]);
}

#[test]
fn native_control_plane_shell_discovers_peers_via_rendezvous_seed() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let seed_roles = PeerRoleSet::new([
        burn_p2p_core::PeerRole::Bootstrap,
        burn_p2p_core::PeerRole::RelayHelper,
    ]);
    let client_policy = RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer());
    let seed_policy = RuntimeTransportPolicy::native_for_roles(&seed_roles);

    let mut seed =
        NativeControlPlaneShell::new(protocols.control.clone(), seed_policy).expect("seed");
    let mut listener =
        NativeControlPlaneShell::new(protocols.control.clone(), client_policy.clone())
            .expect("listener");
    let mut dialer =
        NativeControlPlaneShell::new(protocols.control, client_policy).expect("dialer");

    let listener_peer_id = listener.local_peer_id().to_string();
    let dialer_peer_id = dialer.local_peer_id().to_string();

    seed.listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("seed addr"))
        .expect("seed listen");
    let seed_addr = loop {
        match seed.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("seed did not produce a listen address"),
        }
    };

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("listener addr"))
        .expect("listener listen");
    dialer
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("dialer addr"))
        .expect("dialer listen");

    let drain_until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_until {
        let _ = listener.wait_event(Duration::from_millis(10));
        let _ = dialer.wait_event(Duration::from_millis(10));
    }

    listener
        .dial(seed_addr.clone())
        .expect("listener dial seed");

    let mut listener_relay_addr = None;
    let mut listener_registered = false;
    let listener_ready_deadline = Instant::now() + Duration::from_secs(12);
    while listener_relay_addr.is_none() || !listener_registered {
        assert!(
            Instant::now() < listener_ready_deadline,
            "listener did not register a rendezvous-backed relay address"
        );

        let _ = seed.wait_event(Duration::from_millis(25));
        if let Some(event) = listener.wait_event(Duration::from_millis(50)) {
            match event {
                LiveControlPlaneEvent::ReachableAddressConfirmed { address }
                | LiveControlPlaneEvent::NewListenAddr { address }
                    if address.is_relay_circuit() =>
                {
                    listener_relay_addr = Some(address);
                }
                LiveControlPlaneEvent::Other { kind }
                    if kind.starts_with("rendezvous-registered:") =>
                {
                    listener_registered = true;
                }
                _ => {}
            }
        }
    }

    let listener_relay_addr = listener_relay_addr.expect("listener relay addr");
    dialer.dial(seed_addr).expect("dialer dial seed");

    let mut discovered_listener_addr = None;
    let discovery_deadline = Instant::now() + Duration::from_secs(12);
    while discovered_listener_addr.is_none() {
        assert!(
            Instant::now() < discovery_deadline,
            "dialer did not discover listener via rendezvous seed"
        );

        let _ = seed.wait_event(Duration::from_millis(25));
        let _ = listener.wait_event(Duration::from_millis(25));
        if let Some(event) = dialer.wait_event(Duration::from_millis(50)) {
            match event {
                LiveControlPlaneEvent::PeersDiscovered { peers } => {
                    for (peer_id, address) in peers {
                        if peer_id == listener_peer_id && address.is_relay_circuit() {
                            discovered_listener_addr = Some(address);
                            break;
                        }
                    }
                }
                LiveControlPlaneEvent::Other { kind }
                    if kind.starts_with("rendezvous-discover-failed:") =>
                {
                    panic!("rendezvous discovery failed: {kind}");
                }
                _ => {}
            }
        }
    }

    let discovered_listener_addr = discovered_listener_addr.expect("discovered listener addr");
    assert_eq!(discovered_listener_addr, listener_relay_addr);
    dialer
        .dial(discovered_listener_addr)
        .expect("dial discovered listener");

    let connect_deadline = Instant::now() + Duration::from_secs(20);
    let mut listener_connected = false;
    let mut dialer_connected = false;
    while !(listener_connected && dialer_connected) {
        assert!(
            Instant::now() < connect_deadline,
            "rendezvous-discovered peers did not connect"
        );

        let _ = seed.wait_event(Duration::from_millis(25));
        if let Some(event) = listener.wait_event(Duration::from_millis(25)) {
            listener_connected |= matches!(
                event,
                LiveControlPlaneEvent::ConnectionEstablished { peer_id } if peer_id == dialer_peer_id
            );
        }
        if let Some(event) = dialer.wait_event(Duration::from_millis(25)) {
            dialer_connected |= matches!(
                event,
                LiveControlPlaneEvent::ConnectionEstablished { peer_id } if peer_id == listener_peer_id
            );
        }
        if !listener_connected {
            listener_connected = listener
                .fetch_snapshot(&dialer_peer_id, Duration::from_millis(100))
                .is_ok();
        }
        if !dialer_connected {
            dialer_connected = dialer
                .fetch_snapshot(&listener_peer_id, Duration::from_millis(100))
                .is_ok();
        }
    }
}

#[test]
fn native_control_plane_shell_discovers_peers_via_kademlia_seed() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let seed_roles = PeerRoleSet::new([
        burn_p2p_core::PeerRole::Bootstrap,
        burn_p2p_core::PeerRole::RelayHelper,
    ]);
    let mut client_policy =
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer());
    client_policy.enable_rendezvous_client = false;
    let mut seed_policy = RuntimeTransportPolicy::native_for_roles(&seed_roles);
    seed_policy.enable_rendezvous_client = false;
    seed_policy.enable_rendezvous_server = false;

    let mut seed =
        NativeControlPlaneShell::new(protocols.control.clone(), seed_policy).expect("seed");
    let mut listener =
        NativeControlPlaneShell::new(protocols.control.clone(), client_policy.clone())
            .expect("listener");
    let mut dialer =
        NativeControlPlaneShell::new(protocols.control, client_policy).expect("dialer");

    let listener_peer_id = listener.local_peer_id().to_string();

    seed.listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("seed addr"))
        .expect("seed listen");
    let seed_addr = loop {
        match seed.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("seed did not produce a listen address"),
        }
    };

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("listener addr"))
        .expect("listener listen");
    dialer
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("dialer addr"))
        .expect("dialer listen");

    listener
        .dial(seed_addr.clone())
        .expect("listener dial seed");
    dialer.dial(seed_addr).expect("dialer dial seed");

    let discovery_deadline = Instant::now() + Duration::from_secs(15);
    let mut discovered_listener = false;
    while !discovered_listener {
        assert!(
            Instant::now() < discovery_deadline,
            "dialer did not discover listener through kademlia seed"
        );

        let _ = seed.wait_event(Duration::from_millis(25));
        let _ = listener.wait_event(Duration::from_millis(25));
        if let Some(LiveControlPlaneEvent::PeersDiscovered { peers }) =
            dialer.wait_event(Duration::from_millis(50))
        {
            discovered_listener = peers
                .into_iter()
                .any(|(peer_id, _)| peer_id == listener_peer_id);
        }
    }
}

#[test]
fn native_control_plane_shell_recovers_peer_directory_records_via_kademlia_seed() {
    let network_id = NetworkId::new("network");
    let protocols = ProtocolSet::for_network(&network_id).expect("protocols");
    let seed_roles = PeerRoleSet::new([
        burn_p2p_core::PeerRole::Bootstrap,
        burn_p2p_core::PeerRole::RelayHelper,
    ]);
    let mut client_policy =
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer());
    client_policy.enable_rendezvous_client = false;
    let mut seed_policy = RuntimeTransportPolicy::native_for_roles(&seed_roles);
    seed_policy.enable_rendezvous_client = false;
    seed_policy.enable_rendezvous_server = false;

    let mut seed =
        NativeControlPlaneShell::new(protocols.control.clone(), seed_policy).expect("seed");
    let mut listener =
        NativeControlPlaneShell::new(protocols.control.clone(), client_policy.clone())
            .expect("listener");
    let mut dialer =
        NativeControlPlaneShell::new(protocols.control, client_policy).expect("dialer");

    let listener_peer_id = listener.local_peer_id().to_string();
    let listener_peer = PeerId::new(listener_peer_id.clone());

    seed.listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("seed addr"))
        .expect("seed listen");
    let seed_addr = loop {
        match seed.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("seed did not produce a listen address"),
        }
    };

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("listener addr"))
        .expect("listener listen");
    let listener_addr = loop {
        match listener.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("listener did not produce a listen address"),
        }
    };
    dialer
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("dialer addr"))
        .expect("dialer listen");

    listener
        .dial(seed_addr.clone())
        .expect("listener dial seed");
    dialer.dial(seed_addr).expect("dialer dial seed");
    listener.publish_peer_directory(PeerDirectoryAnnouncement {
        network_id,
        peer_id: listener_peer.clone(),
        addresses: vec![listener_addr.clone()],
        advertised_roles: Some(PeerRoleSet::new([burn_p2p_core::PeerRole::TrainerCpu])),
        announced_at: Utc::now(),
    });

    let discovery_deadline = Instant::now() + Duration::from_secs(15);
    let mut discovered_record = None;
    while discovered_record.is_none() {
        assert!(
            Instant::now() < discovery_deadline,
            "dialer did not recover the listener peer-directory record through kademlia"
        );

        let _ = seed.wait_event(Duration::from_millis(25));
        let _ = listener.wait_event(Duration::from_millis(25));
        if let Some(LiveControlPlaneEvent::PeerDirectoryRecordReceived { announcement }) =
            dialer.wait_event(Duration::from_millis(50))
            && announcement.peer_id.as_str() == listener_peer_id
        {
            discovered_record = Some(announcement);
        }
    }

    let announcement = discovered_record.expect("peer-directory announcement");
    assert_eq!(announcement.addresses, vec![listener_addr]);
    let roles = announcement
        .advertised_roles
        .expect("advertised roles from kademlia record");
    assert!(roles.contains(&burn_p2p_core::PeerRole::TrainerCpu));
    assert!(
        dialer
            .snapshot()
            .peer_directory_announcements
            .iter()
            .any(|entry| entry.peer_id == listener_peer),
        "dialer snapshot did not retain the recovered peer-directory record"
    );
}

#[test]
fn native_control_plane_shell_propagates_control_announcements_over_pubsub() {
    let network_id = NetworkId::new("network");
    let protocols = ProtocolSet::for_network(&network_id).expect("protocols");
    let control_overlay = OverlayTopic::control(network_id.clone());
    let mut listener = NativeControlPlaneShell::new(
        protocols.control.clone(),
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(
        protocols.control,
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native dialer");

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
fn native_control_plane_shell_propagates_lifecycle_announcements_over_pubsub() {
    let network_id = NetworkId::new("network");
    let protocols = ProtocolSet::for_network(&network_id).expect("protocols");
    let control_overlay = OverlayTopic::control(network_id.clone());
    let mut listener = NativeControlPlaneShell::new(
        protocols.control.clone(),
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(
        protocols.control,
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native dialer");

    let now = Utc::now();
    let announcement = ExperimentLifecycleAnnouncement {
        overlay: control_overlay.clone(),
        certificate: ExperimentLifecycleEnvelope {
            network_id: network_id.clone(),
            plan: ExperimentLifecyclePlan {
                study_id: StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                base_revision_id: Some(RevisionId::new("rev-a")),
                target_entry: ExperimentDirectoryEntry {
                    network_id: network_id.clone(),
                    study_id: StudyId::new("study"),
                    experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                    workload_id: WorkloadId::new("alternate"),
                    display_name: "exp rev-b".into(),
                    model_schema_hash: ContentId::new("schema"),
                    dataset_view_id: DatasetViewId::new("view"),
                    resource_requirements: ExperimentResourceRequirements {
                        minimum_roles: BTreeSet::from([PeerRole::TrainerCpu]),
                        minimum_device_memory_bytes: None,
                        minimum_system_memory_bytes: None,
                        estimated_download_bytes: 0,
                        estimated_window_seconds: 60,
                    },
                    visibility: ExperimentVisibility::Public,
                    opt_in_policy: ExperimentOptInPolicy::Open,
                    current_revision_id: RevisionId::new("rev-b"),
                    current_head_id: None,
                    allowed_roles: PeerRoleSet::default_trainer(),
                    allowed_scopes: BTreeSet::new(),
                    metadata: BTreeMap::new(),
                },
                phase: ExperimentLifecyclePhase::Activating,
                target: ActivationTarget {
                    activation: WindowActivation {
                        activation_window: WindowId(2),
                        grace_windows: 1,
                    },
                    required_client_capabilities: BTreeSet::from(["cuda".into()]),
                },
                plan_epoch: 4,
                reason: Some("roll rev-b".into()),
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
        .expect("lifecycle cert"),
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
            "dialer did not receive lifecycle announcement over pubsub"
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
                    LiveControlPlaneEvent::PubsubMessage { kind, .. } if kind == "lifecycle" => {}
                    _ => {}
                }
            }

            if let Some(event) = dialer.wait_event(Duration::from_millis(25))
                && let LiveControlPlaneEvent::PubsubMessage { kind, topic, .. } = event
                && kind == "lifecycle"
                && topic == control_overlay.as_str()
            {
                received_pubsub = true;
                break;
            }
        }

        if !published && listener_saw_remote_subscription {
            listener.publish_lifecycle(announcement.clone());
            match listener.publish_pubsub(
                control_overlay.clone(),
                PubsubPayload::Lifecycle(Box::new(announcement.clone())),
            ) {
                Ok(()) => published = true,
                Err(SwarmError::Pubsub(message))
                    if message.contains("NoPeersSubscribedToTopic") => {}
                Err(error) => panic!("publish lifecycle pubsub: {error}"),
            }
        }
    }

    assert!(
        dialer
            .snapshot()
            .lifecycle_announcements
            .contains(&announcement)
    );
}

#[test]
fn native_control_plane_shell_propagates_schedule_announcements_over_pubsub() {
    let network_id = NetworkId::new("network");
    let protocols = ProtocolSet::for_network(&network_id).expect("protocols");
    let control_overlay = OverlayTopic::control(network_id.clone());
    let mut listener = NativeControlPlaneShell::new(
        protocols.control.clone(),
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(
        protocols.control,
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native dialer");

    let now = Utc::now();
    let announcement = FleetScheduleAnnouncement {
        overlay: control_overlay.clone(),
        certificate: FleetScheduleEpochEnvelope {
            network_id: network_id.clone(),
            epoch: burn_p2p_experiment::FleetScheduleEpoch {
                target: ActivationTarget {
                    activation: WindowActivation {
                        activation_window: WindowId(3),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
                ends_before_window: Some(WindowId(6)),
                plan_epoch: 2,
                assignments: vec![burn_p2p_experiment::FleetScheduleAssignment {
                    peer_id: PeerId::new("peer-a"),
                    slot_index: 0,
                    study_id: StudyId::new("study"),
                    experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                    revision_id: RevisionId::new("rev-b"),
                    budget_scale: Some(0.8),
                    microshard_scale: Some(0.75),
                }],
                reason: Some("rebalance".into()),
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
        .expect("schedule cert"),
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
            "dialer did not receive schedule announcement over pubsub"
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
                    LiveControlPlaneEvent::PubsubMessage { kind, .. } if kind == "schedule" => {}
                    _ => {}
                }
            }

            if let Some(event) = dialer.wait_event(Duration::from_millis(25))
                && let LiveControlPlaneEvent::PubsubMessage { kind, topic, .. } = event
                && kind == "schedule"
                && topic == control_overlay.as_str()
            {
                received_pubsub = true;
                break;
            }
        }

        if !published && listener_saw_remote_subscription {
            listener.publish_schedule(announcement.clone());
            match listener.publish_pubsub(
                control_overlay.clone(),
                PubsubPayload::Schedule(Box::new(announcement.clone())),
            ) {
                Ok(()) => published = true,
                Err(SwarmError::Pubsub(message))
                    if message.contains("NoPeersSubscribedToTopic") => {}
                Err(error) => panic!("publish schedule pubsub: {error}"),
            }
        }
    }

    assert!(
        dialer
            .snapshot()
            .schedule_announcements
            .contains(&announcement)
    );
}

#[test]
fn native_control_plane_shell_transfers_artifact_manifests_and_chunks_over_tcp() {
    let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
    let mut listener = NativeControlPlaneShell::new(
        protocols.control.clone(),
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native listener");
    let mut dialer = NativeControlPlaneShell::new(
        protocols.control,
        RuntimeTransportPolicy::native_for_roles(&PeerRoleSet::default_trainer()),
    )
    .expect("native dialer");

    let descriptor = ArtifactDescriptor {
        artifact_id: ArtifactId::new(
            "122064a6e0882435ad5c0e0c1853a12674b0be1e7b2adc99dfc3a299139b5da47048",
        ),
        kind: ArtifactKind::ServeHead,
        head_id: Some(HeadId::new("head-1")),
        base_head_id: None,
        precision: Precision::Fp16,
        model_schema_hash: ContentId::new("schema"),
        record_format: "burn-record:bin".into(),
        bytes_len: 6,
        chunks: vec![ChunkDescriptor {
            chunk_id: ChunkId::new(
                "1220c7c5c1d70c5dec4416ab6158afd0b223ef40c29b1dc1f97ed9428b94d4cadb1c",
            ),
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
