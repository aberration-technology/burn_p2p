//! Test harnesses, fixtures, and mixed-fleet verification helpers for burn_p2p.
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use burn_p2p::{
    AuthProvider, BrowserRolePolicy, BrowserVisibilityPolicy, ContentId,
    ExperimentDirectoryAnnouncement, ExperimentDirectoryEntry, ExperimentDirectoryPolicyExt,
    ExperimentId, ExperimentOptInPolicy, ExperimentResourceRequirements, ExperimentScope,
    ExperimentVisibility, HeadDescriptor, HeadId, NetworkId, PeerId, PeerRoleSet, Precision,
    PrincipalClaims, PrincipalId, PrincipalSession, RevisionId, RevisionManifest, StudyId,
    WindowActivation, WindowId, WorkloadId,
};
use burn_p2p_bootstrap::{BrowserDirectorySnapshot, BrowserLeaderboardSnapshot};
use burn_p2p_browser::{
    BrowserCapabilityReport, BrowserGpuSupport, BrowserJoinStage, BrowserMetricsSyncState,
    BrowserRuntimeConfig, BrowserRuntimeRole, BrowserRuntimeState, BrowserSessionState,
    BrowserTrainingBudget, BrowserTrainingPlan, BrowserTransportKind, BrowserTransportStatus,
    BrowserValidationPlan, BrowserWorkerCommand, BrowserWorkerEvent, BrowserWorkerRuntime,
};
use burn_p2p_core::{SchemaEnvelope, SignatureAlgorithm, SignatureMetadata, SignedPayload};
use burn_p2p_swarm::{
    HeadAnnouncement, LiveControlPlaneEvent, MemoryControlPlaneShell, NativeControlPlaneShell,
    OverlayChannel, OverlayTopic, ProtocolSet, RuntimeTransportPolicy, SwarmAddress,
};

fn test_timeout(timeout: Duration) -> Duration {
    let scale = std::env::var("BURN_P2P_TEST_TIMEOUT_SCALE")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_else(|| {
            if std::env::var_os("CI").is_some() {
                3
            } else {
                1
            }
        })
        .max(1);
    timeout.checked_mul(scale).unwrap_or(timeout)
}
use chrono::Utc;
use semver::Version;

fn browser_directory_snapshot(
    network_id: &NetworkId,
    study_id: &StudyId,
    experiment_id: &ExperimentId,
    revision_id: &RevisionId,
) -> BrowserDirectorySnapshot {
    let workload_id = WorkloadId::new("browser-wgpu-demo");
    let revision = RevisionManifest {
        experiment_id: experiment_id.clone(),
        revision_id: revision_id.clone(),
        workload_id: workload_id.clone(),
        required_release_train_hash: ContentId::new("browser-train"),
        model_schema_hash: ContentId::new("browser-model"),
        checkpoint_format_hash: ContentId::new("browser-checkpoint"),
        dataset_view_id: burn_p2p::DatasetViewId::new("dataset-view"),
        training_config_hash: ContentId::new("training-config"),
        merge_topology_policy_hash: ContentId::new("merge-topology"),
        slot_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(1024),
            estimated_download_bytes: 1024 * 1024,
            estimated_window_seconds: 30,
        },
        activation_window: WindowActivation {
            activation_window: WindowId(1),
            grace_windows: 1,
        },
        lag_policy: burn_p2p::LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: true,
        browser_role_policy: BrowserRolePolicy {
            observer: true,
            verifier: true,
            trainer_wgpu: true,
            fallback: true,
        },
        max_browser_checkpoint_bytes: Some(16 * 1024 * 1024),
        max_browser_window_secs: Some(30),
        max_browser_shard_bytes: Some(8 * 1024 * 1024),
        requires_webgpu: true,
        max_browser_batch_size: Some(4),
        recommended_browser_precision: Some(Precision::Fp16),
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: "browser transport test revision".into(),
    };

    let mut entry = ExperimentDirectoryEntry {
        network_id: network_id.clone(),
        study_id: study_id.clone(),
        experiment_id: experiment_id.clone(),
        workload_id,
        display_name: "Browser Transport Demo".into(),
        model_schema_hash: ContentId::new("browser-model"),
        dataset_view_id: burn_p2p::DatasetViewId::new("dataset-view"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(1024),
            estimated_download_bytes: 1024 * 1024,
            estimated_window_seconds: 30,
        },
        visibility: ExperimentVisibility::OptIn,
        opt_in_policy: ExperimentOptInPolicy::Scoped,
        current_revision_id: revision_id.clone(),
        current_head_id: None,
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::from([
            ExperimentScope::Connect,
            ExperimentScope::Train {
                experiment_id: experiment_id.clone(),
            },
            ExperimentScope::Validate {
                experiment_id: experiment_id.clone(),
            },
        ]),
        metadata: BTreeMap::new(),
    };
    entry.apply_revision_policy(&revision);

    BrowserDirectorySnapshot {
        network_id: network_id.clone(),
        generated_at: Utc::now(),
        entries: vec![entry],
    }
}

fn signed_directory(
    snapshot: BrowserDirectorySnapshot,
) -> SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_directory_snapshot",
            Version::new(0, 1, 0),
            snapshot,
        ),
        SignatureMetadata {
            signer: PeerId::new("bootstrap-authority"),
            key_id: "bootstrap-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "feed".into(),
        },
    )
    .expect("signed directory")
}

fn signed_leaderboard(
    network_id: &NetworkId,
) -> SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_leaderboard_snapshot",
            Version::new(0, 1, 0),
            BrowserLeaderboardSnapshot {
                network_id: network_id.clone(),
                score_version: "leaderboard_score_v1".into(),
                entries: Vec::new(),
                captured_at: Utc::now(),
            },
        ),
        SignatureMetadata {
            signer: PeerId::new("bootstrap-authority"),
            key_id: "bootstrap-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "cafe".into(),
        },
    )
    .expect("signed leaderboard")
}

fn browser_session(network_id: &NetworkId, experiment_id: &ExperimentId) -> BrowserSessionState {
    BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("browser-session"),
            network_id: network_id.clone(),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("browser-principal"),
                provider: AuthProvider::Static {
                    authority: "browser-test".into(),
                },
                display_name: "Browser Principal".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Train {
                        experiment_id: experiment_id.clone(),
                    },
                    ExperimentScope::Validate {
                        experiment_id: experiment_id.clone(),
                    },
                ]),
                custom_claims: BTreeMap::new(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::minutes(30),
            },
            issued_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(30),
        }),
        certificate: None,
        trust_bundle: None,
        enrolled_at: Some(Utc::now()),
        reenrollment_required: false,
    }
}

fn browser_capability() -> BrowserCapabilityReport {
    BrowserCapabilityReport {
        navigator_gpu_exposed: true,
        worker_gpu_exposed: true,
        gpu_support: BrowserGpuSupport::Available,
        recommended_role: BrowserRuntimeRole::BrowserTrainerWgpu,
        ..BrowserCapabilityReport::default()
    }
}

fn browser_head(
    study_id: &StudyId,
    experiment_id: &ExperimentId,
    revision_id: &RevisionId,
) -> HeadDescriptor {
    HeadDescriptor {
        head_id: HeadId::new("browser-head-1"),
        study_id: study_id.clone(),
        experiment_id: experiment_id.clone(),
        revision_id: revision_id.clone(),
        artifact_id: burn_p2p::ArtifactId::new("artifact-browser-1"),
        parent_head_id: Some(HeadId::new("genesis-head")),
        global_step: 1,
        created_at: Utc::now(),
        metrics: BTreeMap::new(),
    }
}

fn trainer_runtime(network_id: &NetworkId) -> BrowserWorkerRuntime {
    BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            ..BrowserRuntimeConfig::new(
                "https://edge.example",
                network_id.clone(),
                ContentId::new("browser-train"),
                "browser-wasm",
                ContentId::new("approved-artifact-browser"),
            )
        },
        browser_capability(),
        BrowserTransportStatus {
            active: None,
            webrtc_direct_enabled: true,
            webtransport_enabled: true,
            wss_fallback_enabled: true,
            last_error: None,
        },
    )
}

fn wait_for_listen_addr_memory(listener: &mut MemoryControlPlaneShell) -> SwarmAddress {
    loop {
        match listener.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => return address,
            Some(_) => {}
            None => panic!("memory listener did not produce a listen address"),
        }
    }
}

fn wait_for_memory_connection(
    listener: &mut MemoryControlPlaneShell,
    dialer: &mut MemoryControlPlaneShell,
) {
    let deadline = Instant::now() + test_timeout(Duration::from_secs(5));
    let mut listener_connected = false;
    let mut dialer_connected = false;
    while !(listener_connected && dialer_connected) {
        assert!(Instant::now() < deadline, "memory shells did not connect");
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
}

fn fetch_memory_snapshot(
    listener: &mut MemoryControlPlaneShell,
    dialer: &mut MemoryControlPlaneShell,
    listener_peer_id: &str,
) -> burn_p2p_swarm::ControlPlaneSnapshot {
    dialer
        .request_snapshot(listener_peer_id)
        .expect("request memory snapshot");
    let deadline = Instant::now() + test_timeout(Duration::from_secs(5));
    loop {
        assert!(Instant::now() < deadline, "memory snapshot timed out");
        if let Some(event) = listener.wait_event(Duration::from_millis(100))
            && let LiveControlPlaneEvent::SnapshotRequested { .. } = event
        {}
        match dialer.wait_event(Duration::from_millis(100)) {
            Some(LiveControlPlaneEvent::SnapshotReceived { snapshot, .. }) => return snapshot,
            Some(_) => {}
            None => {}
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn fetch_native_snapshot(
    listener: &mut NativeControlPlaneShell,
    dialer: &mut NativeControlPlaneShell,
    listener_peer_id: &str,
) -> burn_p2p_swarm::ControlPlaneSnapshot {
    dialer
        .request_snapshot(listener_peer_id)
        .expect("request native snapshot");
    let deadline = Instant::now() + test_timeout(Duration::from_secs(5));
    loop {
        assert!(Instant::now() < deadline, "native snapshot timed out");
        if let Some(event) = listener.wait_event(Duration::from_millis(100))
            && let LiveControlPlaneEvent::SnapshotRequested { .. } = event
        {}
        match dialer.wait_event(Duration::from_millis(100)) {
            Some(LiveControlPlaneEvent::SnapshotReceived { snapshot, .. }) => return snapshot,
            Some(_) => {}
            None => {}
        }
    }
}

#[test]
fn browser_worker_promotes_to_trainer_only_after_live_memory_swarm_snapshot() {
    let network_id = NetworkId::new("browser-transport-memory");
    let study_id = StudyId::new("study-browser");
    let experiment_id = ExperimentId::new("exp-browser");
    let revision_id = RevisionId::new("rev-browser");
    let directory =
        browser_directory_snapshot(&network_id, &study_id, &experiment_id, &revision_id);
    let head = browser_head(&study_id, &experiment_id, &revision_id);

    let protocols = ProtocolSet::for_network(&network_id).expect("protocols");
    let mut listener =
        MemoryControlPlaneShell::new(protocols.control.clone()).expect("memory control listener");
    let mut dialer =
        MemoryControlPlaneShell::new(protocols.control).expect("memory control dialer");
    let listener_peer_id = listener.local_peer_id().to_string();
    listener.publish_directory(ExperimentDirectoryAnnouncement {
        network_id: network_id.clone(),
        entries: directory.entries.clone(),
        announced_at: Utc::now(),
    });
    listener.publish_head(HeadAnnouncement {
        overlay: OverlayTopic::experiment(
            network_id.clone(),
            study_id.clone(),
            experiment_id.clone(),
            OverlayChannel::Heads,
        )
        .expect("heads overlay"),
        provider_peer_id: Some(PeerId::new(listener_peer_id.clone())),
        head: head.clone(),
        announced_at: Utc::now(),
    });

    listener
        .listen_on(SwarmAddress::new("/memory/0").expect("memory addr"))
        .expect("listen");
    let listen_addr = wait_for_listen_addr_memory(&mut listener);
    dialer.dial(listen_addr).expect("dial");
    wait_for_memory_connection(&mut listener, &mut dialer);
    let snapshot = fetch_memory_snapshot(&mut listener, &mut dialer, &listener_peer_id);

    let session = browser_session(&network_id, &experiment_id);
    let mut runtime = trainer_runtime(&network_id);
    runtime.remember_session(session.clone());
    runtime.storage.stored_certificate_peer_id = Some(PeerId::new("browser-peer"));
    runtime.apply_directory_snapshot(&directory, Some(&session));
    assert_eq!(
        runtime.state,
        Some(BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            stage: BrowserJoinStage::HeadSync,
        })
    );

    let pre_sync = runtime.apply_command(
        BrowserWorkerCommand::Train(BrowserTrainingPlan {
            study_id: study_id.clone(),
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            workload_id: WorkloadId::new("browser-wgpu-demo"),
            budget: BrowserTrainingBudget::default(),
            lease: None,
        }),
        Some(&directory),
        Some(&session),
    );
    assert!(pre_sync.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::Error { message }
            if message.contains("active assignment")
                || message.contains("trainer runtime state")
    )));

    let heads = snapshot
        .head_announcements
        .iter()
        .map(|announcement| announcement.head.clone())
        .collect::<Vec<_>>();
    let sync_events = runtime.apply_edge_sync(
        signed_directory(directory.clone()),
        &heads,
        Some(signed_leaderboard(&network_id)),
        BrowserMetricsSyncState::default(),
        runtime.transport.clone(),
        Some(&session),
    );
    assert!(sync_events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::HeadUpdated { head_id } if head_id == &head.head_id
    )));
    assert_eq!(runtime.state, Some(BrowserRuntimeState::Trainer));
    assert_eq!(
        runtime.transport.active,
        Some(BrowserTransportKind::WebRtcDirect)
    );

    let post_sync = runtime.apply_command(
        BrowserWorkerCommand::Train(BrowserTrainingPlan {
            study_id,
            experiment_id,
            revision_id,
            workload_id: WorkloadId::new("browser-wgpu-demo"),
            budget: BrowserTrainingBudget::default(),
            lease: None,
        }),
        Some(&directory),
        Some(&session),
    );
    assert!(
        post_sync
            .iter()
            .any(|event| matches!(event, BrowserWorkerEvent::TrainingCompleted(_)))
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_worker_promotes_to_verifier_only_after_live_tcp_swarm_snapshot() {
    let network_id = NetworkId::new("browser-transport-tcp");
    let study_id = StudyId::new("study-browser");
    let experiment_id = ExperimentId::new("exp-browser");
    let revision_id = RevisionId::new("rev-browser");
    let directory =
        browser_directory_snapshot(&network_id, &study_id, &experiment_id, &revision_id);
    let head = browser_head(&study_id, &experiment_id, &revision_id);

    let protocols = ProtocolSet::for_network(&network_id).expect("protocols");
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
    listener.publish_directory(ExperimentDirectoryAnnouncement {
        network_id: network_id.clone(),
        entries: directory.entries.clone(),
        announced_at: Utc::now(),
    });
    listener.publish_head(HeadAnnouncement {
        overlay: OverlayTopic::experiment(
            network_id.clone(),
            study_id.clone(),
            experiment_id.clone(),
            OverlayChannel::Heads,
        )
        .expect("heads overlay"),
        provider_peer_id: Some(PeerId::new(listener_peer_id.clone())),
        head: head.clone(),
        announced_at: Utc::now(),
    });

    listener
        .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("tcp addr"))
        .expect("listen");
    let listen_addr = loop {
        match listener.wait_event(Duration::from_secs(2)) {
            Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
            Some(_) => {}
            None => panic!("native listener did not produce a listen address"),
        }
    };
    dialer.dial(listen_addr).expect("dial");

    let deadline = Instant::now() + test_timeout(Duration::from_secs(5));
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

    let snapshot = fetch_native_snapshot(&mut listener, &mut dialer, &listener_peer_id);

    let session = browser_session(&network_id, &experiment_id);
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserVerifier,
            ..BrowserRuntimeConfig::new(
                "https://edge.example",
                network_id.clone(),
                ContentId::new("browser-train"),
                "browser-wasm",
                ContentId::new("approved-artifact-browser"),
            )
        },
        browser_capability(),
        BrowserTransportStatus {
            active: None,
            webrtc_direct_enabled: true,
            webtransport_enabled: true,
            wss_fallback_enabled: true,
            last_error: None,
        },
    );
    runtime.remember_session(session.clone());
    runtime.storage.stored_certificate_peer_id = Some(PeerId::new("browser-peer"));
    runtime.apply_directory_snapshot(&directory, Some(&session));
    assert_eq!(
        runtime.state,
        Some(BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserVerifier,
            stage: BrowserJoinStage::HeadSync,
        })
    );

    let heads = snapshot
        .head_announcements
        .iter()
        .map(|announcement| announcement.head.clone())
        .collect::<Vec<_>>();
    runtime.apply_edge_sync(
        signed_directory(directory.clone()),
        &heads,
        Some(signed_leaderboard(&network_id)),
        BrowserMetricsSyncState::default(),
        runtime.transport.clone(),
        Some(&session),
    );
    assert_eq!(runtime.state, Some(BrowserRuntimeState::Verifier));
    assert_eq!(
        runtime.transport.active,
        Some(BrowserTransportKind::WebRtcDirect)
    );

    let verify_events = runtime.apply_command(
        BrowserWorkerCommand::Verify(BrowserValidationPlan {
            head_id: head.head_id,
            max_checkpoint_bytes: 65_536,
            sample_budget: 4,
            emit_receipt: true,
        }),
        Some(&directory),
        Some(&session),
    );
    assert!(
        verify_events
            .iter()
            .any(|event| matches!(event, BrowserWorkerEvent::ValidationCompleted(_)))
    );
}
