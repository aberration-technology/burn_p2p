use super::*;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(not(target_arch = "wasm32"))]
use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::Duration as StdDuration,
};

use burn_p2p::{
    ArtifactId, AuthProvider, BrowserRole, BrowserRolePolicy, BrowserVisibilityPolicy, ContentId,
    ExperimentDirectoryEntry, ExperimentDirectoryPolicyExt, ExperimentId, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, HeadId, NetworkId,
    PeerId, PeerRole, PeerRoleSet, PrincipalClaims, PrincipalId, PrincipalSession, RevisionId,
    StudyId, WindowActivation, WindowId, WorkloadId,
};
use burn_p2p_core::{
    BackendClass, BrowserDirectorySnapshot, BrowserEdgeMode, BrowserEdgePaths,
    BrowserLeaderboardEntry, BrowserLeaderboardIdentity, BrowserLeaderboardSnapshot,
    BrowserLoginProvider, BrowserPortalSnapshot, BrowserTransportSurface, LeaseId,
    MetricsLiveEvent, MetricsLiveEventKind, MetricsSnapshotManifest, MetricsSyncCursor,
    PeerWindowMetrics, ReenrollmentStatus, RevocationEpoch, SchemaEnvelope, SignatureAlgorithm,
    SignatureMetadata, SignedPayload, TrustBundleExport,
};
use burn_p2p_metrics::{MetricsCatchupBundle, MetricsSnapshot};
use chrono::Utc;
use semver::Version;

#[test]
fn worker_bridge_messages_round_trip_through_json() {
    let command = BrowserWorkerCommand::Train(BrowserTrainingPlan {
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        workload_id: WorkloadId::new("wgpu-demo"),
        budget: BrowserTrainingBudget::default(),
    });
    let bytes = serde_json::to_vec(&command).expect("serialize command");
    let decoded: BrowserWorkerCommand =
        serde_json::from_slice(&bytes).expect("deserialize command");
    assert_eq!(decoded, command);
}

#[test]
fn browser_runtime_defaults_to_observer_safe_policy() {
    let config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    assert_eq!(config.role, BrowserRuntimeRole::BrowserObserver);
    assert_eq!(
        config.transport.preferred,
        vec![
            BrowserTransportKind::WebTransport,
            BrowserTransportKind::WebRtcDirect,
            BrowserTransportKind::WssFallback,
        ]
    );
}

#[test]
fn browser_transport_policy_tracks_swarm_browser_transport_order() {
    let policy = BrowserTransportPolicy::from(burn_p2p::RuntimeTransportPolicy::browser());
    assert_eq!(
        policy.preferred,
        vec![
            BrowserTransportKind::WebTransport,
            BrowserTransportKind::WebRtcDirect,
            BrowserTransportKind::WssFallback,
        ]
    );
    assert_eq!(policy.observer_fallback, BrowserTransportKind::WssFallback);
    assert!(policy.allow_suspend_resume);
}

#[test]
fn browser_storage_bounds_and_dedupes_metrics_catchup_bundles() {
    let mut storage = BrowserStorageSnapshot::default();
    let now = Utc::now();
    let bundles = (0..10)
        .map(|index| MetricsCatchupBundle {
            network_id: NetworkId::new("net-browser"),
            experiment_id: ExperimentId::new(format!("exp-{}", index % 9)),
            revision_id: RevisionId::new("rev-browser"),
            snapshot: MetricsSnapshot {
                manifest: MetricsSnapshotManifest {
                    network_id: NetworkId::new("net-browser"),
                    experiment_id: ExperimentId::new(format!("exp-{}", index % 9)),
                    revision_id: RevisionId::new("rev-browser"),
                    snapshot_seq: index as u64,
                    covers_until_head_id: Some(burn_p2p::HeadId::new(format!("head-{index}"))),
                    covers_until_merge_window_id: None,
                    canonical_head_metrics_ref: ContentId::new(format!("canon-{index}")),
                    window_rollups_ref: ContentId::new(format!("window-{index}")),
                    network_rollups_ref: ContentId::new(format!("network-{index}")),
                    leaderboard_ref: None,
                    created_at: now + chrono::Duration::seconds(index as i64),
                    signatures: Vec::new(),
                },
                head_eval_reports: Vec::new(),
                peer_window_metrics: Vec::new(),
                reducer_cohort_metrics: Vec::new(),
                derived_metrics: Vec::new(),
            },
            ledger_segments: Vec::new(),
            generated_at: now + chrono::Duration::seconds(index as i64),
        })
        .chain(std::iter::once(MetricsCatchupBundle {
            network_id: NetworkId::new("net-browser"),
            experiment_id: ExperimentId::new("exp-3"),
            revision_id: RevisionId::new("rev-browser"),
            snapshot: MetricsSnapshot {
                manifest: MetricsSnapshotManifest {
                    network_id: NetworkId::new("net-browser"),
                    experiment_id: ExperimentId::new("exp-3"),
                    revision_id: RevisionId::new("rev-browser"),
                    snapshot_seq: 99,
                    covers_until_head_id: Some(burn_p2p::HeadId::new("head-99")),
                    covers_until_merge_window_id: None,
                    canonical_head_metrics_ref: ContentId::new("canon-99"),
                    window_rollups_ref: ContentId::new("window-99"),
                    network_rollups_ref: ContentId::new("network-99"),
                    leaderboard_ref: None,
                    created_at: now + chrono::Duration::seconds(99),
                    signatures: Vec::new(),
                },
                head_eval_reports: Vec::new(),
                peer_window_metrics: Vec::new(),
                reducer_cohort_metrics: Vec::new(),
                derived_metrics: Vec::new(),
            },
            ledger_segments: Vec::new(),
            generated_at: now + chrono::Duration::seconds(99),
        }))
        .collect::<Vec<_>>();

    storage.remember_metrics_catchup(bundles);

    assert_eq!(storage.metrics_catchup_bundles.len(), 8);
    assert_eq!(
        storage
            .metrics_catchup_bundles
            .iter()
            .filter(|bundle| bundle.experiment_id.as_str() == "exp-3")
            .count(),
        1
    );
    assert_eq!(
        storage
            .metrics_catchup_bundles
            .iter()
            .find(|bundle| bundle.experiment_id.as_str() == "exp-3")
            .expect("exp-3 bundle")
            .snapshot
            .manifest
            .snapshot_seq,
        99
    );
}

#[test]
fn capability_report_projects_canonical_browser_capabilities() {
    let report = BrowserCapabilityReport {
        gpu_support: BrowserGpuSupport::Available,
        ..BrowserCapabilityReport::default()
    };

    let capabilities = report.capabilities();
    assert!(capabilities.contains(&burn_p2p::BrowserCapability::DedicatedWorker));
    assert!(capabilities.contains(&burn_p2p::BrowserCapability::WebGpu));
    assert!(capabilities.contains(&burn_p2p::BrowserCapability::WssFallback));
}

#[test]
fn browser_join_policy_prefers_requested_role_and_falls_back_safely() {
    let mut entry = ExperimentDirectoryEntry {
        network_id: NetworkId::new("net-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        workload_id: WorkloadId::new("wgpu-demo"),
        display_name: "Browser Demo".into(),
        model_schema_hash: ContentId::new("model-browser"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
            minimum_device_memory_bytes: Some(1024),
            minimum_system_memory_bytes: Some(2048),
            estimated_download_bytes: 4096,
            estimated_window_seconds: 30,
        },
        visibility: ExperimentVisibility::Public,
        opt_in_policy: ExperimentOptInPolicy::Open,
        current_revision_id: RevisionId::new("rev-browser"),
        current_head_id: None,
        allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
        allowed_scopes: BTreeSet::from([ExperimentScope::Connect]),
        metadata: Default::default(),
    };
    entry.apply_revision_policy(&burn_p2p::RevisionManifest {
        experiment_id: entry.experiment_id.clone(),
        revision_id: entry.current_revision_id.clone(),
        workload_id: entry.workload_id.clone(),
        required_release_train_hash: ContentId::new("train-browser"),
        model_schema_hash: entry.model_schema_hash.clone(),
        checkpoint_format_hash: ContentId::new("checkpoint-browser"),
        dataset_view_id: entry.dataset_view_id.clone(),
        training_config_hash: ContentId::new("training-browser"),
        merge_topology_policy_hash: ContentId::new("topology-browser"),
        slot_requirements: entry.resource_requirements.clone(),
        activation_window: WindowActivation {
            activation_window: WindowId(1),
            grace_windows: 0,
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
        max_browser_checkpoint_bytes: Some(1024),
        max_browser_window_secs: Some(45),
        max_browser_shard_bytes: Some(512),
        requires_webgpu: true,
        max_browser_batch_size: Some(4),
        recommended_browser_precision: Some(burn_p2p::Precision::Fp16),
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: "browser".into(),
    });

    let snapshot = BrowserDirectorySnapshot {
        network_id: NetworkId::new("net-browser"),
        generated_at: Utc::now(),
        entries: vec![entry],
    };

    let fallback_report = BrowserCapabilityReport::default();
    let fallback_policy = recommended_browser_join_policy(
        &snapshot,
        "browser-wasm",
        &fallback_report,
        BrowserRuntimeRole::BrowserTrainerWgpu,
    )
    .expect("fallback join policy");
    assert!(
        !fallback_policy
            .eligible_roles
            .contains(&burn_p2p::BrowserRole::TrainerWgpu)
    );
    assert!(
        fallback_policy
            .eligible_roles
            .contains(&burn_p2p::BrowserRole::Observer)
    );
    assert_eq!(
        recommended_browser_runtime_state(
            &snapshot,
            "browser-wasm",
            &fallback_report,
            BrowserRuntimeRole::BrowserTrainerWgpu,
        ),
        Some(BrowserRuntimeState::Verifier)
    );

    let trainer_report = BrowserCapabilityReport {
        gpu_support: BrowserGpuSupport::Available,
        ..BrowserCapabilityReport::default()
    };
    let trainer_policy = recommended_browser_join_policy(
        &snapshot,
        "browser-wasm",
        &trainer_report,
        BrowserRuntimeRole::BrowserTrainerWgpu,
    )
    .expect("trainer join policy");
    assert!(
        trainer_policy
            .eligible_roles
            .contains(&burn_p2p::BrowserRole::TrainerWgpu)
    );
    assert_eq!(
        recommended_browser_runtime_state(
            &snapshot,
            "browser-wasm",
            &trainer_report,
            BrowserRuntimeRole::BrowserTrainerWgpu,
        ),
        Some(BrowserRuntimeState::Trainer)
    );
}

#[test]
fn browser_runtime_state_surfaces_portal_only_and_blocked_candidates() {
    let base_entry = ExperimentDirectoryEntry {
        network_id: NetworkId::new("net-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        workload_id: WorkloadId::new("browser-demo"),
        display_name: "Browser Demo".into(),
        model_schema_hash: ContentId::new("model-browser"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 1024,
            estimated_window_seconds: 15,
        },
        visibility: ExperimentVisibility::Public,
        opt_in_policy: ExperimentOptInPolicy::Open,
        current_revision_id: RevisionId::new("rev-browser"),
        current_head_id: None,
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::from([ExperimentScope::Connect]),
        metadata: Default::default(),
    };

    let mut portal_entry = base_entry.clone();
    portal_entry.apply_revision_policy(&burn_p2p::RevisionManifest {
        experiment_id: portal_entry.experiment_id.clone(),
        revision_id: portal_entry.current_revision_id.clone(),
        workload_id: portal_entry.workload_id.clone(),
        required_release_train_hash: ContentId::new("train-browser"),
        model_schema_hash: portal_entry.model_schema_hash.clone(),
        checkpoint_format_hash: ContentId::new("checkpoint-browser"),
        dataset_view_id: portal_entry.dataset_view_id.clone(),
        training_config_hash: ContentId::new("training-browser"),
        merge_topology_policy_hash: ContentId::new("topology-browser"),
        slot_requirements: portal_entry.resource_requirements.clone(),
        activation_window: WindowActivation {
            activation_window: WindowId(1),
            grace_windows: 0,
        },
        lag_policy: burn_p2p::LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: false,
        browser_role_policy: BrowserRolePolicy::default(),
        max_browser_checkpoint_bytes: None,
        max_browser_window_secs: None,
        max_browser_shard_bytes: None,
        requires_webgpu: false,
        max_browser_batch_size: None,
        recommended_browser_precision: None,
        visibility_policy: BrowserVisibilityPolicy::PortalListed,
        description: "portal only".into(),
    });

    let mut blocked_entry = base_entry;
    blocked_entry.apply_revision_policy(&burn_p2p::RevisionManifest {
        experiment_id: blocked_entry.experiment_id.clone(),
        revision_id: blocked_entry.current_revision_id.clone(),
        workload_id: blocked_entry.workload_id.clone(),
        required_release_train_hash: ContentId::new("train-browser"),
        model_schema_hash: blocked_entry.model_schema_hash.clone(),
        checkpoint_format_hash: ContentId::new("checkpoint-browser"),
        dataset_view_id: blocked_entry.dataset_view_id.clone(),
        training_config_hash: ContentId::new("training-browser"),
        merge_topology_policy_hash: ContentId::new("topology-browser"),
        slot_requirements: blocked_entry.resource_requirements.clone(),
        activation_window: WindowActivation {
            activation_window: WindowId(2),
            grace_windows: 0,
        },
        lag_policy: burn_p2p::LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: false,
        browser_role_policy: BrowserRolePolicy::default(),
        max_browser_checkpoint_bytes: None,
        max_browser_window_secs: None,
        max_browser_shard_bytes: None,
        requires_webgpu: false,
        max_browser_batch_size: None,
        recommended_browser_precision: None,
        visibility_policy: BrowserVisibilityPolicy::Hidden,
        description: "blocked".into(),
    });

    let portal_snapshot = BrowserDirectorySnapshot {
        network_id: NetworkId::new("net-browser"),
        generated_at: Utc::now(),
        entries: vec![portal_entry],
    };
    assert_eq!(
        recommended_browser_runtime_state(
            &portal_snapshot,
            "browser-wasm",
            &BrowserCapabilityReport::default(),
            BrowserRuntimeRole::BrowserObserver,
        ),
        Some(BrowserRuntimeState::PortalOnly)
    );

    let blocked_snapshot = BrowserDirectorySnapshot {
        network_id: NetworkId::new("net-browser"),
        generated_at: Utc::now(),
        entries: vec![blocked_entry],
    };
    assert_eq!(
        recommended_browser_runtime_state(
            &blocked_snapshot,
            "browser-wasm",
            &BrowserCapabilityReport::default(),
            BrowserRuntimeRole::BrowserObserver,
        ),
        Some(BrowserRuntimeState::Blocked {
            reason: "revision is not approved for target artifact browser-wasm".into(),
        })
    );
}

#[test]
fn browser_portal_ui_state_projects_browser_picker_with_scope_and_fallback_metadata() {
    let mut entry = ExperimentDirectoryEntry {
        network_id: NetworkId::new("net-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        workload_id: WorkloadId::new("browser-demo"),
        display_name: "Browser Demo".into(),
        model_schema_hash: ContentId::new("model-browser"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 1024,
            estimated_window_seconds: 15,
        },
        visibility: ExperimentVisibility::Public,
        opt_in_policy: ExperimentOptInPolicy::Open,
        current_revision_id: RevisionId::new("rev-browser"),
        current_head_id: Some(burn_p2p::HeadId::new("head-browser")),
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::from([ExperimentScope::Train {
            experiment_id: ExperimentId::new("exp-browser"),
        }]),
        metadata: Default::default(),
    };
    entry.apply_revision_policy(&burn_p2p::RevisionManifest {
        experiment_id: entry.experiment_id.clone(),
        revision_id: entry.current_revision_id.clone(),
        workload_id: entry.workload_id.clone(),
        required_release_train_hash: ContentId::new("train-browser"),
        model_schema_hash: entry.model_schema_hash.clone(),
        checkpoint_format_hash: ContentId::new("checkpoint-browser"),
        dataset_view_id: entry.dataset_view_id.clone(),
        training_config_hash: ContentId::new("training-browser"),
        merge_topology_policy_hash: ContentId::new("topology-browser"),
        slot_requirements: entry.resource_requirements.clone(),
        activation_window: WindowActivation {
            activation_window: WindowId(3),
            grace_windows: 0,
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
        max_browser_checkpoint_bytes: Some(1024),
        max_browser_window_secs: Some(45),
        max_browser_shard_bytes: Some(512),
        requires_webgpu: true,
        max_browser_batch_size: Some(4),
        recommended_browser_precision: Some(burn_p2p::Precision::Fp16),
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: "browser".into(),
    });

    let directory = BrowserDirectorySnapshot {
        network_id: NetworkId::new("net-browser"),
        generated_at: Utc::now(),
        entries: vec![entry],
    };

    let now = Utc::now();
    let session_state = BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("session-browser"),
            network_id: NetworkId::new("net-browser"),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("principal-browser"),
                provider: AuthProvider::Static {
                    authority: "lab-auth".into(),
                },
                display_name: "Browser User".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([ExperimentScope::Train {
                    experiment_id: ExperimentId::new("exp-browser"),
                }]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now,
            },
            issued_at: now,
            expires_at: now,
        }),
        ..BrowserSessionState::default()
    };

    let ui_state = browser_portal_ui_state_from_directory(
        None,
        None,
        Some(&session_state),
        &directory,
        "browser-wasm",
        &BrowserCapabilityReport::default(),
        BrowserRuntimeRole::BrowserTrainerWgpu,
    );

    let generic_picker = ui_state.experiment_picker.expect("generic picker");
    assert!(generic_picker.entries[0].allowed);

    let browser_picker = ui_state.browser_experiment_picker.expect("browser picker");
    assert!(browser_picker.entries[0].allowed);
    assert_eq!(
        browser_picker.entries[0].recommended_state,
        burn_p2p_ui::BrowserExperimentPickerState::Verifier
    );
    assert_eq!(
        browser_picker.entries[0].recommended_role,
        Some(BrowserRole::Verifier)
    );
    assert!(browser_picker.entries[0].fallback_from_preferred);

    let unauthorized_ui_state = browser_portal_ui_state_from_directory(
        None,
        None,
        None,
        &directory,
        "browser-wasm",
        &BrowserCapabilityReport::default(),
        BrowserRuntimeRole::BrowserTrainerWgpu,
    );
    let unauthorized_picker = unauthorized_ui_state
        .browser_experiment_picker
        .expect("unauthorized browser picker");
    assert!(unauthorized_picker.entries[0].allowed);
    assert_eq!(
        unauthorized_picker.entries[0].recommended_state,
        burn_p2p_ui::BrowserExperimentPickerState::Verifier
    );
    assert_eq!(
        unauthorized_picker.entries[0].recommended_role,
        Some(BrowserRole::Verifier)
    );
    assert!(unauthorized_picker.entries[0].fallback_from_preferred);
    assert!(
        unauthorized_picker.entries[0].blocked_reasons.is_empty()
            || !unauthorized_picker.entries[0]
                .blocked_reasons
                .iter()
                .any(|reason| reason.contains("session scopes"))
    );
}

#[test]
fn worker_runtime_projects_directory_state_and_transport_selection() {
    let mut entry = ExperimentDirectoryEntry {
        network_id: NetworkId::new("net-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        workload_id: WorkloadId::new("wgpu-demo"),
        display_name: "Browser Demo".into(),
        model_schema_hash: ContentId::new("model-browser"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 4096,
            estimated_window_seconds: 30,
        },
        visibility: ExperimentVisibility::Public,
        opt_in_policy: ExperimentOptInPolicy::Open,
        current_revision_id: RevisionId::new("rev-browser"),
        current_head_id: None,
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::from([ExperimentScope::Connect]),
        metadata: Default::default(),
    };
    entry.apply_revision_policy(&burn_p2p::RevisionManifest {
        experiment_id: entry.experiment_id.clone(),
        revision_id: entry.current_revision_id.clone(),
        workload_id: entry.workload_id.clone(),
        required_release_train_hash: ContentId::new("train-browser"),
        model_schema_hash: entry.model_schema_hash.clone(),
        checkpoint_format_hash: ContentId::new("checkpoint-browser"),
        dataset_view_id: entry.dataset_view_id.clone(),
        training_config_hash: ContentId::new("training-browser"),
        merge_topology_policy_hash: ContentId::new("topology-browser"),
        slot_requirements: entry.resource_requirements.clone(),
        activation_window: WindowActivation {
            activation_window: WindowId(4),
            grace_windows: 0,
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
        max_browser_checkpoint_bytes: Some(1024),
        max_browser_window_secs: Some(45),
        max_browser_shard_bytes: Some(512),
        requires_webgpu: true,
        max_browser_batch_size: Some(4),
        recommended_browser_precision: Some(burn_p2p::Precision::Fp16),
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: "browser".into(),
    });

    let directory = BrowserDirectorySnapshot {
        network_id: NetworkId::new("net-browser"),
        generated_at: Utc::now(),
        entries: vec![entry],
    };

    let config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    let transport = BrowserTransportStatus {
        active: None,
        webrtc_direct_enabled: false,
        webtransport_enabled: true,
        wss_fallback_enabled: true,
        last_error: None,
    };
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            ..config
        },
        BrowserCapabilityReport::default(),
        transport,
    );

    runtime.apply_directory_snapshot(&directory, None);
    assert_eq!(
        runtime.state,
        Some(BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserVerifier,
            stage: BrowserJoinStage::HeadSync,
        })
    );
    assert_eq!(
        runtime
            .config
            .as_ref()
            .and_then(|config| config.selected_experiment.clone()),
        Some(ExperimentId::new("exp-browser"))
    );
    assert_eq!(
        runtime
            .config
            .as_ref()
            .and_then(|config| config.selected_revision.clone()),
        Some(RevisionId::new("rev-browser"))
    );
    assert_eq!(
        runtime.transport.active,
        Some(BrowserTransportKind::WebTransport)
    );
    assert_eq!(
        runtime.storage.active_assignment,
        Some(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        })
    );

    let now = Utc::now();
    let session_state = BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("session-browser"),
            network_id: NetworkId::new("net-browser"),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("principal-browser"),
                provider: AuthProvider::Static {
                    authority: "lab-auth".into(),
                },
                display_name: "Browser User".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now,
            },
            issued_at: now,
            expires_at: now,
        }),
        ..BrowserSessionState::default()
    };

    runtime.apply_directory_snapshot(&directory, Some(&session_state));
    assert_eq!(
        runtime.state,
        Some(BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserVerifier,
            stage: BrowserJoinStage::HeadSync,
        })
    );
    assert_eq!(
        runtime.transport.active,
        Some(BrowserTransportKind::WebTransport)
    );
    assert_eq!(
        runtime
            .config
            .as_ref()
            .and_then(|config| config.selected_experiment.clone()),
        Some(ExperimentId::new("exp-browser"))
    );
    assert_eq!(
        runtime
            .config
            .as_ref()
            .and_then(|config| config.selected_revision.clone()),
        Some(RevisionId::new("rev-browser"))
    );
    assert_eq!(
        runtime.storage.active_assignment,
        Some(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        })
    );

    runtime.apply_head_snapshot(&[burn_p2p::HeadDescriptor {
        head_id: burn_p2p::HeadId::new("head-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        artifact_id: ArtifactId::new("artifact-browser"),
        parent_head_id: None,
        global_step: 7,
        created_at: Utc::now(),
        metrics: BTreeMap::new(),
    }]);
    assert_eq!(runtime.state, Some(BrowserRuntimeState::Verifier));
    assert_eq!(
        runtime.storage.last_head_id,
        Some(burn_p2p::HeadId::new("head-browser"))
    );
}

#[test]
fn worker_runtime_select_experiment_persists_assignment_and_blocks_missing_selection() {
    let mut entry = ExperimentDirectoryEntry {
        network_id: NetworkId::new("net-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        workload_id: WorkloadId::new("browser-demo"),
        display_name: "Browser Demo".into(),
        model_schema_hash: ContentId::new("model-browser"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 2048,
            estimated_window_seconds: 20,
        },
        visibility: ExperimentVisibility::Public,
        opt_in_policy: ExperimentOptInPolicy::Open,
        current_revision_id: RevisionId::new("rev-browser"),
        current_head_id: None,
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::from([ExperimentScope::Connect]),
        metadata: Default::default(),
    };
    entry.apply_revision_policy(&burn_p2p::RevisionManifest {
        experiment_id: entry.experiment_id.clone(),
        revision_id: entry.current_revision_id.clone(),
        workload_id: entry.workload_id.clone(),
        required_release_train_hash: ContentId::new("train-browser"),
        model_schema_hash: entry.model_schema_hash.clone(),
        checkpoint_format_hash: ContentId::new("checkpoint-browser"),
        dataset_view_id: entry.dataset_view_id.clone(),
        training_config_hash: ContentId::new("training-browser"),
        merge_topology_policy_hash: ContentId::new("topology-browser"),
        slot_requirements: entry.resource_requirements.clone(),
        activation_window: WindowActivation {
            activation_window: WindowId(5),
            grace_windows: 0,
        },
        lag_policy: burn_p2p::LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: true,
        browser_role_policy: BrowserRolePolicy {
            observer: true,
            verifier: true,
            trainer_wgpu: false,
            fallback: true,
        },
        max_browser_checkpoint_bytes: Some(1024),
        max_browser_window_secs: Some(30),
        max_browser_shard_bytes: Some(512),
        requires_webgpu: false,
        max_browser_batch_size: None,
        recommended_browser_precision: None,
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: "browser".into(),
    });

    let directory = BrowserDirectorySnapshot {
        network_id: NetworkId::new("net-browser"),
        generated_at: Utc::now(),
        entries: vec![entry],
    };
    let now = Utc::now();
    let session_state = BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("session-browser"),
            network_id: NetworkId::new("net-browser"),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("principal-browser"),
                provider: AuthProvider::Static {
                    authority: "lab-auth".into(),
                },
                display_name: "Browser User".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now,
            },
            issued_at: now,
            expires_at: now,
        }),
        ..BrowserSessionState::default()
    };

    let config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserObserver,
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );
    runtime.remember_session(session_state.clone());
    runtime.select_experiment(
        ExperimentId::new("exp-browser"),
        None,
        Some(&directory),
        Some(&session_state),
    );

    assert_eq!(
        runtime.state,
        Some(BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserObserver,
            stage: BrowserJoinStage::HeadSync,
        })
    );
    assert_eq!(
        runtime.storage.active_assignment,
        Some(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        })
    );
    assert_eq!(
        runtime
            .config
            .as_ref()
            .and_then(|config| config.selected_experiment.clone()),
        Some(ExperimentId::new("exp-browser"))
    );
    assert_eq!(
        runtime
            .config
            .as_ref()
            .and_then(|config| config.selected_revision.clone()),
        Some(RevisionId::new("rev-browser"))
    );

    runtime.select_experiment(
        ExperimentId::new("missing-exp"),
        Some(RevisionId::new("missing-rev")),
        Some(&directory),
        Some(&session_state),
    );
    assert_eq!(
            runtime.state,
            Some(BrowserRuntimeState::Blocked {
                reason:
                    "selected experiment missing-exp revision missing-rev is not browser-visible for this session"
                        .into(),
            })
        );
    assert_eq!(runtime.storage.active_assignment, None);
}

#[test]
fn worker_runtime_apply_command_emits_selection_and_storage_events() {
    let mut entry = ExperimentDirectoryEntry {
        network_id: NetworkId::new("net-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        workload_id: WorkloadId::new("browser-demo"),
        display_name: "Browser Demo".into(),
        model_schema_hash: ContentId::new("model-browser"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 1024,
            estimated_window_seconds: 15,
        },
        visibility: ExperimentVisibility::Public,
        opt_in_policy: ExperimentOptInPolicy::Open,
        current_revision_id: RevisionId::new("rev-browser"),
        current_head_id: None,
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::from([ExperimentScope::Connect]),
        metadata: Default::default(),
    };
    entry.apply_revision_policy(&burn_p2p::RevisionManifest {
        experiment_id: entry.experiment_id.clone(),
        revision_id: entry.current_revision_id.clone(),
        workload_id: entry.workload_id.clone(),
        required_release_train_hash: ContentId::new("train-browser"),
        model_schema_hash: entry.model_schema_hash.clone(),
        checkpoint_format_hash: ContentId::new("checkpoint-browser"),
        dataset_view_id: entry.dataset_view_id.clone(),
        training_config_hash: ContentId::new("training-browser"),
        merge_topology_policy_hash: ContentId::new("topology-browser"),
        slot_requirements: entry.resource_requirements.clone(),
        activation_window: WindowActivation {
            activation_window: WindowId(6),
            grace_windows: 0,
        },
        lag_policy: burn_p2p::LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: true,
        browser_role_policy: BrowserRolePolicy {
            observer: true,
            verifier: true,
            trainer_wgpu: false,
            fallback: false,
        },
        max_browser_checkpoint_bytes: None,
        max_browser_window_secs: None,
        max_browser_shard_bytes: None,
        requires_webgpu: false,
        max_browser_batch_size: None,
        recommended_browser_precision: None,
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: "browser".into(),
    });
    let directory = BrowserDirectorySnapshot {
        network_id: NetworkId::new("net-browser"),
        generated_at: Utc::now(),
        entries: vec![entry],
    };
    let now = Utc::now();
    let session_state = BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("session-browser"),
            network_id: NetworkId::new("net-browser"),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("principal-browser"),
                provider: AuthProvider::Static {
                    authority: "lab-auth".into(),
                },
                display_name: "Browser User".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now,
            },
            issued_at: now,
            expires_at: now,
        }),
        ..BrowserSessionState::default()
    };

    let config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserObserver,
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );

    let events = runtime.apply_command(
        BrowserWorkerCommand::SelectExperiment {
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: Some(RevisionId::new("rev-browser")),
        },
        Some(&directory),
        Some(&session_state),
    );
    assert!(events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::RuntimeStateChanged(BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserObserver,
            stage: BrowserJoinStage::HeadSync,
        })
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::DirectoryUpdated {
            network_id,
            visible_entries,
        } if network_id == &NetworkId::new("net-browser") && *visible_entries == 1
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::StorageUpdated(storage)
            if storage.active_assignment == Some(BrowserStoredAssignment {
                study_id: StudyId::new("study-browser"),
                experiment_id: ExperimentId::new("exp-browser"),
                revision_id: RevisionId::new("rev-browser"),
            })
    )));
}

#[test]
fn worker_runtime_apply_command_completes_validation_and_training_locally() {
    let config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );
    let now = Utc::now();
    let session_state = BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("session-browser"),
            network_id: NetworkId::new("net-browser"),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("principal-browser"),
                provider: AuthProvider::Static {
                    authority: "lab-auth".into(),
                },
                display_name: "Browser User".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now,
            },
            issued_at: now,
            expires_at: now,
        }),
        ..BrowserSessionState::default()
    };
    runtime.remember_session(session_state);
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });
    runtime.state = Some(BrowserRuntimeState::Verifier);

    let validation_events = runtime.apply_command(
        BrowserWorkerCommand::Verify(BrowserValidationPlan {
            head_id: burn_p2p::HeadId::new("head-browser"),
            max_checkpoint_bytes: 1024,
            sample_budget: 7,
            emit_receipt: true,
        }),
        None,
        None,
    );
    assert!(validation_events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::HeadUpdated { head_id }
            if head_id == &burn_p2p::HeadId::new("head-browser")
    )));
    assert!(validation_events.iter().any(|event| matches!(
            event,
            BrowserWorkerEvent::ValidationCompleted(BrowserValidationResult {
                head_id,
                accepted: true,
                checked_chunks: 7,
                emitted_receipt_id: Some(receipt_id),
            }) if head_id == &burn_p2p::HeadId::new("head-browser")
                && receipt_id == &burn_p2p::ContributionReceiptId::new("browser-validation-receipt-head-browser-7")
        )));

    runtime.state = Some(BrowserRuntimeState::Trainer);
    let events = runtime.apply_command(
        BrowserWorkerCommand::Train(BrowserTrainingPlan {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            workload_id: WorkloadId::new("browser-demo"),
            budget: BrowserTrainingBudget::default(),
        }),
        None,
        None,
    );
    assert!(events.iter().any(|event| matches!(
            event,
            BrowserWorkerEvent::TrainingCompleted(BrowserTrainingResult {
                artifact_id,
                receipt_id: Some(receipt_id),
                window_secs: 30,
            }) if artifact_id == &ArtifactId::new("browser-artifact-exp-browser-rev-browser-browser-demo")
                && receipt_id == &burn_p2p::ContributionReceiptId::new("browser-training-receipt-exp-browser-rev-browser")
        )));
}

#[test]
fn worker_runtime_apply_edge_sync_tracks_signed_snapshots_and_promotes_join_state() {
    let mut entry = ExperimentDirectoryEntry {
        network_id: NetworkId::new("net-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        workload_id: WorkloadId::new("browser-demo"),
        display_name: "Browser Demo".into(),
        model_schema_hash: ContentId::new("model-browser"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 1024,
            estimated_window_seconds: 15,
        },
        visibility: ExperimentVisibility::Public,
        opt_in_policy: ExperimentOptInPolicy::Open,
        current_revision_id: RevisionId::new("rev-browser"),
        current_head_id: Some(burn_p2p::HeadId::new("head-browser")),
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::from([ExperimentScope::Connect]),
        metadata: Default::default(),
    };
    entry.apply_revision_policy(&burn_p2p::RevisionManifest {
        experiment_id: entry.experiment_id.clone(),
        revision_id: entry.current_revision_id.clone(),
        workload_id: entry.workload_id.clone(),
        required_release_train_hash: ContentId::new("train-browser"),
        model_schema_hash: entry.model_schema_hash.clone(),
        checkpoint_format_hash: ContentId::new("checkpoint-browser"),
        dataset_view_id: entry.dataset_view_id.clone(),
        training_config_hash: ContentId::new("training-browser"),
        merge_topology_policy_hash: ContentId::new("topology-browser"),
        slot_requirements: entry.resource_requirements.clone(),
        activation_window: WindowActivation {
            activation_window: WindowId(6),
            grace_windows: 0,
        },
        lag_policy: burn_p2p::LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: true,
        browser_role_policy: BrowserRolePolicy {
            observer: true,
            verifier: true,
            trainer_wgpu: false,
            fallback: false,
        },
        max_browser_checkpoint_bytes: None,
        max_browser_window_secs: None,
        max_browser_shard_bytes: None,
        requires_webgpu: false,
        max_browser_batch_size: None,
        recommended_browser_precision: None,
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: "browser".into(),
    });
    let directory = BrowserDirectorySnapshot {
        network_id: NetworkId::new("net-browser"),
        generated_at: Utc::now(),
        entries: vec![entry],
    };
    let signed_directory = SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_directory_snapshot",
            Version::new(0, 1, 0),
            directory,
        ),
        SignatureMetadata {
            signer: PeerId::new("bootstrap-authority"),
            key_id: "bootstrap-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "feed".into(),
        },
    )
    .expect("signed directory");
    let signed_leaderboard = SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_leaderboard_snapshot",
            Version::new(0, 1, 0),
            BrowserLeaderboardSnapshot {
                network_id: NetworkId::new("net-browser"),
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
    .expect("signed leaderboard");
    let now = Utc::now();
    let session_state = BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("session-browser"),
            network_id: NetworkId::new("net-browser"),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("principal-browser"),
                provider: AuthProvider::Static {
                    authority: "lab-auth".into(),
                },
                display_name: "Browser User".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now,
            },
            issued_at: now,
            expires_at: now,
        }),
        ..BrowserSessionState::default()
    };
    let config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserObserver,
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );
    runtime.remember_session(session_state.clone());
    let events = runtime.apply_edge_sync(
        signed_directory,
        &[burn_p2p::HeadDescriptor {
            head_id: burn_p2p::HeadId::new("head-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            parent_head_id: None,
            global_step: 7,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        }],
        Some(signed_leaderboard),
        BrowserMetricsSyncState {
            catchup_bundles: Vec::new(),
            live_event: Some(MetricsLiveEvent {
                network_id: NetworkId::new("net-browser"),
                kind: MetricsLiveEventKind::CatchupRefresh,
                cursors: vec![MetricsSyncCursor {
                    experiment_id: ExperimentId::new("exp-browser"),
                    revision_id: RevisionId::new("rev-browser"),
                    latest_snapshot_seq: Some(2),
                    latest_ledger_segment_seq: Some(1),
                    latest_head_id: Some(burn_p2p::HeadId::new("head-browser")),
                    latest_merge_window_id: None,
                }],
                generated_at: Utc::now(),
            }),
        },
        BrowserTransportStatus::default(),
        Some(&session_state),
    );

    assert_eq!(runtime.state, Some(BrowserRuntimeState::Observer));
    assert_eq!(
        runtime.storage.last_head_id,
        Some(burn_p2p::HeadId::new("head-browser"))
    );
    assert!(runtime.storage.last_signed_directory_snapshot.is_some());
    assert!(runtime.storage.last_signed_leaderboard_snapshot.is_some());
    assert!(runtime.storage.last_metrics_live_event.is_some());
    assert!(events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::HeadUpdated { head_id }
            if head_id == &burn_p2p::HeadId::new("head-browser")
    )));
}

#[test]
fn worker_runtime_waits_for_transport_after_head_sync_until_edge_transport_is_available() {
    let config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserObserver,
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus {
            active: None,
            webrtc_direct_enabled: false,
            webtransport_enabled: false,
            wss_fallback_enabled: false,
            last_error: Some("edge transports unavailable".into()),
        },
    );
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });
    runtime.state = Some(BrowserRuntimeState::Joining {
        role: BrowserRuntimeRole::BrowserObserver,
        stage: BrowserJoinStage::HeadSync,
    });

    runtime.apply_head_snapshot(&[burn_p2p::HeadDescriptor {
        head_id: burn_p2p::HeadId::new("head-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        artifact_id: ArtifactId::new("artifact-browser"),
        parent_head_id: None,
        global_step: 7,
        created_at: Utc::now(),
        metrics: BTreeMap::new(),
    }]);

    assert_eq!(
        runtime.state,
        Some(BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserObserver,
            stage: BrowserJoinStage::TransportConnect,
        })
    );

    runtime.update_transport_status(BrowserTransportStatus {
        active: None,
        webrtc_direct_enabled: false,
        webtransport_enabled: false,
        wss_fallback_enabled: true,
        last_error: None,
    });

    assert_eq!(
        runtime.transport.active,
        Some(BrowserTransportKind::WssFallback)
    );
    assert_eq!(runtime.state, Some(BrowserRuntimeState::Observer));
}

#[test]
fn worker_runtime_flushes_and_acknowledges_receipt_outbox() {
    let config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );
    let now = Utc::now();
    runtime.remember_session(BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("session-browser"),
            network_id: NetworkId::new("net-browser"),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("principal-browser"),
                provider: AuthProvider::Static {
                    authority: "lab-auth".into(),
                },
                display_name: "Browser User".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Validate {
                        experiment_id: ExperimentId::new("exp-browser"),
                    },
                    ExperimentScope::Train {
                        experiment_id: ExperimentId::new("exp-browser"),
                    },
                ]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now,
            },
            issued_at: now,
            expires_at: now,
        }),
        certificate: Some(
            burn_p2p::NodeCertificate::new(
                Version::new(0, 1, 0),
                burn_p2p::NodeCertificateClaims {
                    network_id: NetworkId::new("net-browser"),
                    project_family_id: burn_p2p::ProjectFamilyId::new("family-browser"),
                    release_train_hash: ContentId::new("train-browser"),
                    target_artifact_hash: ContentId::new("artifact-browser"),
                    peer_id: PeerId::new("peer-browser"),
                    peer_public_key_hex: "001122".into(),
                    principal_id: PrincipalId::new("principal-browser"),
                    provider: burn_p2p::AuthProvider::Static {
                        authority: "lab-auth".into(),
                    },
                    granted_roles: burn_p2p::PeerRoleSet::default(),
                    experiment_scopes: BTreeSet::from([ExperimentScope::Connect]),
                    client_policy_hash: None,
                    not_before: now,
                    not_after: now,
                    serial: 1,
                    revocation_epoch: RevocationEpoch(0),
                },
                SignatureMetadata {
                    signer: PeerId::new("issuer-browser"),
                    key_id: "issuer-key".into(),
                    algorithm: SignatureAlgorithm::Ed25519,
                    signed_at: now,
                    signature_hex: "bead".into(),
                },
            )
            .expect("certificate"),
        ),
        ..BrowserSessionState::default()
    });
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });
    runtime.state = Some(BrowserRuntimeState::Verifier);
    let _ = runtime.apply_command(
        BrowserWorkerCommand::Verify(BrowserValidationPlan {
            head_id: burn_p2p::HeadId::new("head-browser"),
            max_checkpoint_bytes: 1024,
            sample_budget: 7,
            emit_receipt: true,
        }),
        None,
        None,
    );
    runtime.state = Some(BrowserRuntimeState::Trainer);
    let _ = runtime.apply_command(
        BrowserWorkerCommand::Train(BrowserTrainingPlan {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            workload_id: WorkloadId::new("browser-demo"),
            budget: BrowserTrainingBudget::default(),
        }),
        None,
        None,
    );

    let flush_events = runtime.apply_command(BrowserWorkerCommand::FlushReceiptOutbox, None, None);
    let flushed_receipts = flush_events
        .iter()
        .find_map(|event| match event {
            BrowserWorkerEvent::ReceiptOutboxReady {
                session_id,
                submit_path,
                receipts,
            } if session_id == &ContentId::new("session-browser")
                && submit_path == "/receipts/browser" =>
            {
                Some(receipts.clone())
            }
            _ => None,
        })
        .expect("receipt outbox ready event");
    assert_eq!(flushed_receipts.len(), 2);

    let ack_events = runtime.apply_command(
        BrowserWorkerCommand::AcknowledgeSubmittedReceipts {
            receipt_ids: flushed_receipts
                .iter()
                .map(|receipt| receipt.receipt_id.clone())
                .collect(),
        },
        None,
        None,
    );
    assert!(ack_events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::ReceiptsAcknowledged {
            receipt_ids,
            pending_receipts: 0,
        } if receipt_ids.len() == 2
    )));
    assert_eq!(runtime.storage.pending_receipts.len(), 0);
    assert_eq!(runtime.storage.submitted_receipts.len(), 2);
}

#[test]
fn worker_runtime_apply_command_rejects_training_without_trainer_state() {
    let config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserObserver,
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );
    let now = Utc::now();
    runtime.remember_session(BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("session-browser"),
            network_id: NetworkId::new("net-browser"),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("principal-browser"),
                provider: AuthProvider::Static {
                    authority: "lab-auth".into(),
                },
                display_name: "Browser User".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now,
            },
            issued_at: now,
            expires_at: now,
        }),
        ..BrowserSessionState::default()
    });

    let events = runtime.apply_command(
        BrowserWorkerCommand::Train(BrowserTrainingPlan {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            workload_id: WorkloadId::new("browser-demo"),
            budget: BrowserTrainingBudget::default(),
        }),
        None,
        None,
    );
    assert!(events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::Error { message }
            if message.contains("trainer runtime state")
    )));
}

#[test]
fn worker_runtime_suspend_and_resume_moves_into_catchup() {
    let config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserObserver,
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );

    runtime.state = Some(BrowserRuntimeState::Observer);
    runtime.refresh_transport_selection();
    assert_eq!(
        runtime.transport.active,
        Some(BrowserTransportKind::WebTransport)
    );

    runtime.suspend();
    assert_eq!(
        runtime.state,
        Some(BrowserRuntimeState::BackgroundSuspended {
            role: Some(BrowserRuntimeRole::BrowserObserver),
        })
    );
    assert_eq!(runtime.transport.active, None);

    runtime.resume();
    assert_eq!(
        runtime.state,
        Some(BrowserRuntimeState::Catchup {
            role: BrowserRuntimeRole::BrowserObserver,
        })
    );
    assert_eq!(
        runtime.transport.active,
        Some(BrowserTransportKind::WebTransport)
    );
}

#[test]
fn edge_endpoints_match_browser_edge_contract() {
    let bindings = BrowserUiBindings::new("https://edge.example");
    assert_eq!(
        bindings.endpoint_url(&bindings.paths.portal_snapshot_path),
        "https://edge.example/portal/snapshot"
    );
    assert_eq!(bindings.paths.signed_directory_path, "/directory/signed");
    assert_eq!(
        bindings.paths.signed_leaderboard_path,
        "/leaderboard/signed"
    );
    assert_eq!(bindings.paths.receipt_submit_path, "/receipts/browser");
    assert_eq!(bindings.paths.artifacts_live_path, "/artifacts/live");
    assert_eq!(
        bindings.paths.artifacts_live_latest_path,
        "/artifacts/live/latest"
    );
}

#[test]
fn edge_endpoints_do_not_duplicate_prefixed_paths() {
    let bindings = BrowserUiBindings::new("https://edge.example/browser");
    assert_eq!(
        bindings.endpoint_url("/browser/portal/snapshot"),
        "https://edge.example/browser/portal/snapshot"
    );
}

#[test]
fn enrollment_config_and_bindings_can_be_derived_from_portal_snapshot() {
    let artifact_hash = ContentId::new("artifact-browser");
    let snapshot = BrowserPortalSnapshot {
        network_id: NetworkId::new("net-browser"),
        edge_mode: BrowserEdgeMode::Peer,
        browser_mode: burn_p2p::BrowserMode::Verifier,
        social_mode: burn_p2p::SocialMode::Public,
        profile_mode: burn_p2p::ProfileMode::Public,
        transports: BrowserTransportSurface {
            webrtc_direct: true,
            webtransport_gateway: false,
            wss_fallback: true,
        },
        paths: BrowserEdgePaths::default(),
        auth_enabled: true,
        login_providers: vec![BrowserLoginProvider {
            label: "Static".into(),
            login_path: "/login/static".into(),
            callback_path: Some("/callback/static".into()),
            device_path: Some("/device".into()),
        }],
        required_release_train_hash: Some(ContentId::new("train-browser")),
        allowed_target_artifact_hashes: BTreeSet::from([artifact_hash.clone()]),
        directory: BrowserDirectorySnapshot {
            network_id: NetworkId::new("net-browser"),
            generated_at: Utc::now(),
            entries: Vec::new(),
        },
        heads: Vec::new(),
        leaderboard: BrowserLeaderboardSnapshot {
            network_id: NetworkId::new("net-browser"),
            score_version: "leaderboard_score_v1".into(),
            entries: vec![BrowserLeaderboardEntry {
                identity: BrowserLeaderboardIdentity {
                    principal_id: Some(PrincipalId::new("alice")),
                    peer_ids: BTreeSet::from([PeerId::new("peer-browser")]),
                    label: "alice".into(),
                    social_profile: None,
                },
                accepted_work_score: 1.0,
                quality_weighted_impact_score: 1.5,
                validation_service_score: 0.0,
                artifact_serving_score: 0.0,
                leaderboard_score_v1: 1.75,
                accepted_receipt_count: 1,
                last_receipt_at: Some(Utc::now()),
                badges: Vec::new(),
            }],
            captured_at: Utc::now(),
        },
        trust_bundle: Some(TrustBundleExport {
            network_id: NetworkId::new("net-browser"),
            project_family_id: burn_p2p::ProjectFamilyId::new("family-browser"),
            required_release_train_hash: ContentId::new("train-browser"),
            allowed_target_artifact_hashes: BTreeSet::from([artifact_hash.clone()]),
            minimum_revocation_epoch: RevocationEpoch(3),
            active_issuer_peer_id: PeerId::new("issuer-browser"),
            issuers: Vec::new(),
            reenrollment: None,
        }),
        captured_at: Utc::now(),
    };

    let bindings = BrowserUiBindings::from_portal_snapshot("https://edge.example", &snapshot);
    let enrollment = BrowserEnrollmentConfig::from_portal_snapshot(
        &snapshot,
        "browser-wasm",
        artifact_hash,
        BTreeSet::from([
            ExperimentScope::Connect,
            ExperimentScope::Validate {
                experiment_id: ExperimentId::new("exp-browser"),
            },
        ]),
        900,
    )
    .expect("derive enrollment config");

    assert_eq!(
        bindings.endpoint_url(&bindings.paths.portal_snapshot_path),
        "https://edge.example/portal/snapshot"
    );
    assert_eq!(enrollment.project_family_id.as_str(), "family-browser");
    assert_eq!(enrollment.release_train_hash.as_str(), "train-browser");
    assert_eq!(enrollment.login_path, "/login/static");
}

#[test]
fn provider_specific_portal_snapshot_prefers_primary_login_provider() {
    let artifact_hash = ContentId::new("artifact-browser");
    let mut snapshot = BrowserPortalSnapshot {
        network_id: NetworkId::new("net-browser"),
        edge_mode: BrowserEdgeMode::Peer,
        browser_mode: burn_p2p::BrowserMode::Verifier,
        social_mode: burn_p2p::SocialMode::Public,
        profile_mode: burn_p2p::ProfileMode::Public,
        transports: BrowserTransportSurface {
            webrtc_direct: true,
            webtransport_gateway: false,
            wss_fallback: true,
        },
        paths: BrowserEdgePaths::default(),
        auth_enabled: true,
        login_providers: vec![
            BrowserLoginProvider {
                label: "GitHub".into(),
                login_path: "/login/github".into(),
                callback_path: Some("/callback/github".into()),
                device_path: None,
            },
            BrowserLoginProvider {
                label: "Static".into(),
                login_path: "/login/static".into(),
                callback_path: Some("/callback/static".into()),
                device_path: Some("/device".into()),
            },
        ],
        required_release_train_hash: Some(ContentId::new("train-browser")),
        allowed_target_artifact_hashes: BTreeSet::from([artifact_hash.clone()]),
        directory: BrowserDirectorySnapshot {
            network_id: NetworkId::new("net-browser"),
            generated_at: Utc::now(),
            entries: Vec::new(),
        },
        heads: Vec::new(),
        leaderboard: BrowserLeaderboardSnapshot {
            network_id: NetworkId::new("net-browser"),
            score_version: "leaderboard_score_v1".into(),
            entries: Vec::new(),
            captured_at: Utc::now(),
        },
        trust_bundle: Some(TrustBundleExport {
            network_id: NetworkId::new("net-browser"),
            project_family_id: burn_p2p::ProjectFamilyId::new("family-browser"),
            required_release_train_hash: ContentId::new("train-browser"),
            allowed_target_artifact_hashes: BTreeSet::from([artifact_hash.clone()]),
            minimum_revocation_epoch: RevocationEpoch(3),
            active_issuer_peer_id: PeerId::new("issuer-browser"),
            issuers: Vec::new(),
            reenrollment: None,
        }),
        captured_at: Utc::now(),
    };
    snapshot.paths.login_path = "/login/static".into();
    snapshot.paths.callback_path = "/callback/static".into();

    let bindings = BrowserUiBindings::from_portal_snapshot("https://edge.example", &snapshot);
    let enrollment = BrowserEnrollmentConfig::from_portal_snapshot(
        &snapshot,
        "browser-wasm",
        artifact_hash,
        BTreeSet::from([ExperimentScope::Connect]),
        900,
    )
    .expect("derive enrollment config");

    assert_eq!(bindings.paths.login_path, "/login/github");
    assert_eq!(bindings.paths.callback_path, "/callback/github");
    assert_eq!(enrollment.login_path, "/login/github");
    assert_eq!(enrollment.callback_path, "/callback/github");
}

#[test]
fn session_and_storage_snapshots_capture_enrollment_and_signed_state() {
    let session = burn_p2p::PrincipalSession {
        session_id: ContentId::new("session-browser"),
        network_id: NetworkId::new("net-browser"),
        claims: burn_p2p::PrincipalClaims {
            principal_id: PrincipalId::new("alice"),
            provider: burn_p2p::AuthProvider::Static {
                authority: "demo".into(),
            },
            display_name: "alice".into(),
            org_memberships: BTreeSet::new(),
            group_memberships: BTreeSet::new(),
            granted_roles: burn_p2p::PeerRoleSet::default(),
            granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
            custom_claims: Default::default(),
            issued_at: Utc::now(),
            expires_at: Utc::now(),
        },
        issued_at: Utc::now(),
        expires_at: Utc::now(),
    };
    let certificate = burn_p2p::NodeCertificate::new(
        Version::new(0, 1, 0),
        burn_p2p::NodeCertificateClaims {
            network_id: NetworkId::new("net-browser"),
            project_family_id: burn_p2p::ProjectFamilyId::new("family-browser"),
            release_train_hash: ContentId::new("train-browser"),
            target_artifact_hash: ContentId::new("artifact-browser"),
            peer_id: PeerId::new("peer-browser"),
            peer_public_key_hex: "001122".into(),
            principal_id: PrincipalId::new("alice"),
            provider: burn_p2p::AuthProvider::Static {
                authority: "demo".into(),
            },
            granted_roles: burn_p2p::PeerRoleSet::default(),
            experiment_scopes: BTreeSet::from([ExperimentScope::Connect]),
            client_policy_hash: None,
            not_before: Utc::now(),
            not_after: Utc::now(),
            serial: 7,
            revocation_epoch: RevocationEpoch(0),
        },
        SignatureMetadata {
            signer: PeerId::new("issuer-browser"),
            key_id: "issuer-key".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "bead".into(),
        },
    )
    .expect("certificate");
    let result = BrowserEnrollmentResult {
        login: burn_p2p::LoginStart {
            login_id: ContentId::new("login-browser"),
            provider: burn_p2p::AuthProvider::Static {
                authority: "demo".into(),
            },
            state: "state-browser".into(),
            authorize_url: None,
            expires_at: Utc::now(),
        },
        session,
        certificate,
        trust_bundle: Some(TrustBundleExport {
            network_id: NetworkId::new("net-browser"),
            project_family_id: burn_p2p::ProjectFamilyId::new("family-browser"),
            required_release_train_hash: ContentId::new("train-browser"),
            allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact-browser")]),
            minimum_revocation_epoch: RevocationEpoch(5),
            active_issuer_peer_id: PeerId::new("issuer-browser"),
            issuers: Vec::new(),
            reenrollment: Some(ReenrollmentStatus {
                reason: "rotate".into(),
                rotated_at: None,
                legacy_issuer_peer_ids: BTreeSet::new(),
                login_path: "/login/static".into(),
                enroll_path: "/enroll".into(),
                trust_bundle_path: "/trust".into(),
            }),
        }),
        enrolled_at: Utc::now(),
        reenrollment_required: true,
    };
    let mut session_state = BrowserSessionState::default();
    session_state.apply_enrollment(&result);
    assert_eq!(
        session_state.principal_id().expect("principal").as_str(),
        "alice"
    );
    assert_eq!(
        session_state.peer_id().expect("peer").as_str(),
        "peer-browser"
    );
    assert!(session_state.reenrollment_required);

    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_session(session_state);
    storage.remember_assignment(BrowserStoredAssignment {
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
    });
    storage.queue_receipt(burn_p2p::ContributionReceipt {
        receipt_id: burn_p2p::ContributionReceiptId::new("receipt-browser"),
        peer_id: PeerId::new("peer-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        base_head_id: burn_p2p::HeadId::new("head-browser"),
        artifact_id: ArtifactId::new("artifact-browser"),
        accepted_at: Utc::now(),
        accepted_weight: 1.0,
        metrics: BTreeMap::from([("loss".into(), burn_p2p::MetricValue::Float(0.25))]),
        merge_cert_id: None,
    });
    storage.remember_directory_snapshot(
        SignedPayload::new(
            SchemaEnvelope::new(
                "burn_p2p.browser_directory_snapshot",
                Version::new(0, 1, 0),
                BrowserDirectorySnapshot {
                    network_id: NetworkId::new("net-browser"),
                    generated_at: Utc::now(),
                    entries: Vec::new(),
                },
            ),
            SignatureMetadata {
                signer: PeerId::new("bootstrap-authority"),
                key_id: "bootstrap-browser-edge".into(),
                algorithm: SignatureAlgorithm::Ed25519,
                signed_at: Utc::now(),
                signature_hex: "feed".into(),
            },
        )
        .expect("signed directory"),
    );
    storage.remember_leaderboard_snapshot(
        SignedPayload::new(
            SchemaEnvelope::new(
                "burn_p2p.browser_leaderboard_snapshot",
                Version::new(0, 1, 0),
                BrowserLeaderboardSnapshot {
                    network_id: NetworkId::new("net-browser"),
                    score_version: "leaderboard_score_v1".into(),
                    entries: Vec::new(),
                    captured_at: Utc::now(),
                },
            ),
            SignatureMetadata {
                signer: PeerId::new("bootstrap-authority"),
                key_id: "bootstrap-browser-edge".into(),
                algorithm: SignatureAlgorithm::Ed25519,
                signed_at: Utc::now(),
                signature_hex: "cafe".into(),
            },
        )
        .expect("signed leaderboard"),
    );
    storage.remember_metrics_catchup(vec![MetricsCatchupBundle {
        network_id: NetworkId::new("net-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        snapshot: MetricsSnapshot {
            manifest: MetricsSnapshotManifest {
                network_id: NetworkId::new("net-browser"),
                experiment_id: ExperimentId::new("exp-browser"),
                revision_id: RevisionId::new("rev-browser"),
                snapshot_seq: 2,
                covers_until_head_id: Some(burn_p2p::HeadId::new("head-browser")),
                covers_until_merge_window_id: None,
                canonical_head_metrics_ref: ContentId::new("metrics-canonical"),
                window_rollups_ref: ContentId::new("metrics-window"),
                network_rollups_ref: ContentId::new("metrics-network"),
                leaderboard_ref: None,
                created_at: Utc::now(),
                signatures: Vec::new(),
            },
            head_eval_reports: Vec::new(),
            peer_window_metrics: Vec::new(),
            reducer_cohort_metrics: Vec::new(),
            derived_metrics: Vec::new(),
        },
        ledger_segments: Vec::new(),
        generated_at: Utc::now(),
    }]);
    storage.remember_metrics_live_event(MetricsLiveEvent {
        network_id: NetworkId::new("net-browser"),
        kind: MetricsLiveEventKind::CatchupRefresh,
        cursors: vec![MetricsSyncCursor {
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            latest_snapshot_seq: Some(2),
            latest_ledger_segment_seq: Some(1),
            latest_head_id: Some(burn_p2p::HeadId::new("head-browser")),
            latest_merge_window_id: None,
        }],
        generated_at: Utc::now(),
    });

    assert_eq!(
        storage
            .stored_certificate_peer_id
            .expect("stored peer")
            .as_str(),
        "peer-browser"
    );
    assert_eq!(storage.stored_receipts.len(), 1);
    assert_eq!(storage.pending_receipts.len(), 1);
    assert!(storage.last_signed_directory_snapshot.is_some());
    assert!(storage.last_signed_leaderboard_snapshot.is_some());
    assert_eq!(storage.metrics_catchup_bundles.len(), 1);
    assert_eq!(
        storage.metrics_catchup_bundles[0]
            .snapshot
            .manifest
            .experiment_id
            .as_str(),
        "exp-browser"
    );
    assert_eq!(
        storage
            .last_metrics_live_event
            .as_ref()
            .expect("metrics live event")
            .kind,
        MetricsLiveEventKind::CatchupRefresh
    );
    assert!(storage.last_metrics_sync_at.is_some());
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_metrics_sse_server(
    events: Vec<MetricsLiveEvent>,
) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("listener local addr")
    );
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept sse request");
        stream
            .set_read_timeout(Some(StdDuration::from_secs(1)))
            .expect("set read timeout");
        stream
            .set_write_timeout(Some(StdDuration::from_secs(1)))
            .expect("set write timeout");

        let mut request = Vec::new();
        loop {
            let mut chunk = [0u8; 1024];
            let read = stream.read(&mut chunk).expect("read request");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n",
            )
            .expect("write sse headers");
        stream.flush().expect("flush headers");

        for event in events {
            let frame = format!(
                "event: metrics\ndata: {}\n\n",
                serde_json::to_string(&event).expect("serialize metrics event"),
            );
            let bytes = frame.as_bytes();
            let split = bytes.len() / 2;
            stream
                .write_all(&bytes[..split])
                .expect("write sse frame first chunk");
            stream.flush().expect("flush frame first chunk");
            thread::sleep(StdDuration::from_millis(10));
            stream
                .write_all(&bytes[split..])
                .expect("write sse frame second chunk");
            stream.flush().expect("flush frame second chunk");
            thread::sleep(StdDuration::from_millis(10));
        }
    });

    (base_url, handle)
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_metrics_sse_poll_server(
    batches: Vec<Vec<MetricsLiveEvent>>,
) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("listener local addr")
    );
    let handle = thread::spawn(move || {
        for batch in batches {
            let (mut stream, _) = listener.accept().expect("accept sse request");
            stream
                .set_read_timeout(Some(StdDuration::from_secs(1)))
                .expect("set read timeout");
            stream
                .set_write_timeout(Some(StdDuration::from_secs(1)))
                .expect("set write timeout");

            let mut request = Vec::new();
            loop {
                let mut chunk = [0u8; 1024];
                let read = stream.read(&mut chunk).expect("read request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n",
                )
                .expect("write sse headers");
            stream.flush().expect("flush headers");

            for event in batch {
                let frame = format!(
                    "event: metrics\ndata: {}\n\n",
                    serde_json::to_string(&event).expect("serialize metrics event"),
                );
                stream.write_all(frame.as_bytes()).expect("write sse frame");
                stream.flush().expect("flush sse frame");
            }
        }
    });

    (base_url, handle)
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_metrics_latest_poll_server(
    events: Vec<MetricsLiveEvent>,
) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("listener local addr")
    );
    let handle = thread::spawn(move || {
        let mut latest_index = 0usize;
        while latest_index < events.len() {
            let (mut stream, _) = listener.accept().expect("accept latest request");
            stream
                .set_read_timeout(Some(StdDuration::from_secs(1)))
                .expect("set read timeout");
            stream
                .set_write_timeout(Some(StdDuration::from_secs(1)))
                .expect("set write timeout");

            let mut request = Vec::new();
            loop {
                let mut chunk = [0u8; 1024];
                let read = stream.read(&mut chunk).expect("read request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            let request_line = String::from_utf8_lossy(&request);
            let path = request_line
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/");
            let body = if path.starts_with("/metrics/catchup/") {
                serde_json::to_vec(&Vec::<MetricsCatchupBundle>::new())
                    .expect("serialize empty catchup")
            } else {
                let event = events
                    .get(latest_index)
                    .expect("latest metrics event for request");
                latest_index += 1;
                serde_json::to_vec(event).expect("serialize latest metrics event")
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write latest response");
            stream.write_all(&body).expect("write latest body");
            stream.flush().expect("flush latest response");
        }
    });

    (base_url, handle)
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_collects_multiple_metrics_live_events_from_sse_stream() {
    let events = vec![
        MetricsLiveEvent {
            network_id: NetworkId::new("net-browser"),
            kind: MetricsLiveEventKind::CatchupRefresh,
            cursors: vec![MetricsSyncCursor {
                experiment_id: ExperimentId::new("exp-browser"),
                revision_id: RevisionId::new("rev-browser"),
                latest_snapshot_seq: Some(2),
                latest_ledger_segment_seq: Some(1),
                latest_head_id: Some(burn_p2p::HeadId::new("head-browser-1")),
                latest_merge_window_id: None,
            }],
            generated_at: Utc::now(),
        },
        MetricsLiveEvent {
            network_id: NetworkId::new("net-browser"),
            kind: MetricsLiveEventKind::CatchupRefresh,
            cursors: vec![MetricsSyncCursor {
                experiment_id: ExperimentId::new("exp-browser"),
                revision_id: RevisionId::new("rev-browser"),
                latest_snapshot_seq: Some(3),
                latest_ledger_segment_seq: Some(2),
                latest_head_id: Some(burn_p2p::HeadId::new("head-browser-2")),
                latest_merge_window_id: None,
            }],
            generated_at: Utc::now(),
        },
    ];
    let (base_url, handle) = spawn_metrics_sse_server(events.clone());
    let client = BrowserPortalClient::new(
        BrowserUiBindings::new(base_url),
        BrowserEnrollmentConfig {
            network_id: NetworkId::new("net-browser"),
            project_family_id: burn_p2p::ProjectFamilyId::new("family-browser"),
            release_train_hash: ContentId::new("train-browser"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("artifact-browser"),
            login_path: "/login".into(),
            callback_path: "/callback".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::new(),
            session_ttl_secs: 900,
        },
    );

    let streamed = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .subscribe_metrics_live_events(2)
                .await
                .expect("collect live metrics events")
        });

    handle.join().expect("join sse thread");
    assert_eq!(streamed, events);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_streams_metrics_events_into_worker_runtime() {
    let events = vec![
        MetricsLiveEvent {
            network_id: NetworkId::new("net-browser"),
            kind: MetricsLiveEventKind::CatchupRefresh,
            cursors: vec![MetricsSyncCursor {
                experiment_id: ExperimentId::new("exp-browser"),
                revision_id: RevisionId::new("rev-browser"),
                latest_snapshot_seq: Some(4),
                latest_ledger_segment_seq: Some(2),
                latest_head_id: Some(burn_p2p::HeadId::new("head-browser-4")),
                latest_merge_window_id: None,
            }],
            generated_at: Utc::now(),
        },
        MetricsLiveEvent {
            network_id: NetworkId::new("net-browser"),
            kind: MetricsLiveEventKind::CatchupRefresh,
            cursors: vec![MetricsSyncCursor {
                experiment_id: ExperimentId::new("exp-browser"),
                revision_id: RevisionId::new("rev-browser"),
                latest_snapshot_seq: Some(5),
                latest_ledger_segment_seq: Some(3),
                latest_head_id: Some(burn_p2p::HeadId::new("head-browser-5")),
                latest_merge_window_id: None,
            }],
            generated_at: Utc::now(),
        },
    ];
    let (base_url, handle) = spawn_metrics_sse_server(events.clone());
    let client = BrowserPortalClient::new(
        BrowserUiBindings::new(base_url),
        BrowserEnrollmentConfig {
            network_id: NetworkId::new("net-browser"),
            project_family_id: burn_p2p::ProjectFamilyId::new("family-browser"),
            release_train_hash: ContentId::new("train-browser"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("artifact-browser"),
            login_path: "/login".into(),
            callback_path: "/callback".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::new(),
            session_ttl_secs: 900,
        },
    );
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig::new(
            "https://edge.example",
            NetworkId::new("net-browser"),
            ContentId::new("train-browser"),
            "browser-wasm",
            ContentId::new("artifact-browser"),
        ),
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });

    let worker_events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .stream_metrics_live_events_into_worker(&mut runtime, 2)
                .await
                .expect("stream live metrics events into worker")
        });

    handle.join().expect("join sse thread");
    assert!(worker_events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::MetricsUpdated(event)
            if event.cursors[0]
                .latest_head_id
                .as_ref()
                .expect("metrics head")
                .as_str()
                == "head-browser-5"
    )));
    assert!(worker_events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::HeadUpdated { head_id } if head_id.as_str() == "head-browser-4"
    )));
    assert!(worker_events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::HeadUpdated { head_id } if head_id.as_str() == "head-browser-5"
    )));
    let storage = worker_events
        .iter()
        .rev()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert_eq!(
        storage
            .last_metrics_live_event
            .as_ref()
            .expect("last metrics live event")
            .cursors[0]
            .latest_head_id
            .as_ref()
            .expect("latest head")
            .as_str(),
        "head-browser-5"
    );
    assert_eq!(
        storage
            .last_head_id
            .as_ref()
            .expect("storage head")
            .as_str(),
        "head-browser-5"
    );
}

fn sample_browser_session_state(principal_id: &str) -> BrowserSessionState {
    let now = Utc::now();
    BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new(format!("session-{principal_id}")),
            network_id: NetworkId::new("net-browser"),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new(principal_id),
                provider: AuthProvider::Static {
                    authority: "lab-auth".into(),
                },
                display_name: "Browser User".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::from([ExperimentScope::Connect]),
                custom_claims: BTreeMap::new(),
                issued_at: now,
                expires_at: now,
            },
            issued_at: now,
            expires_at: now,
        }),
        ..BrowserSessionState::default()
    }
}

fn signed_browser_directory_snapshot(
    entries: Vec<ExperimentDirectoryEntry>,
) -> SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_directory_snapshot",
            Version::new(0, 1, 0),
            BrowserDirectorySnapshot {
                network_id: NetworkId::new("net-browser"),
                generated_at: Utc::now(),
                entries,
            },
        ),
        SignatureMetadata {
            signer: PeerId::new("bootstrap-authority"),
            key_id: "bootstrap-browser-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "feed".into(),
        },
    )
    .expect("signed browser directory snapshot")
}

fn signed_browser_leaderboard_snapshot(
    entries: Vec<BrowserLeaderboardEntry>,
) -> SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_leaderboard_snapshot",
            Version::new(0, 1, 0),
            BrowserLeaderboardSnapshot {
                network_id: NetworkId::new("net-browser"),
                score_version: "leaderboard_score_v1".into(),
                entries,
                captured_at: Utc::now(),
            },
        ),
        SignatureMetadata {
            signer: PeerId::new("bootstrap-authority"),
            key_id: "bootstrap-browser-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "cafe".into(),
        },
    )
    .expect("signed browser leaderboard snapshot")
}

#[test]
fn browser_app_model_projects_trainer_focused_client_view() {
    let mut runtime = BrowserWorkerRuntime {
        config: Some(BrowserRuntimeConfig::new(
            "https://edge.example",
            NetworkId::new("net-browser"),
            ContentId::new("train-browser"),
            "browser-wasm",
            ContentId::new("artifact-browser"),
        )),
        state: Some(BrowserRuntimeState::Trainer),
        capability: Some(BrowserCapabilityReport {
            navigator_gpu_exposed: true,
            worker_gpu_exposed: true,
            gpu_support: BrowserGpuSupport::Available,
            recommended_role: BrowserRuntimeRole::BrowserTrainerWgpu,
            max_training_window_secs: 90,
            ..BrowserCapabilityReport::default()
        }),
        transport: BrowserTransportStatus {
            active: Some(BrowserTransportKind::WebTransport),
            ..BrowserTransportStatus::default()
        },
        ..BrowserWorkerRuntime::default()
    };
    runtime
        .storage
        .remember_session(sample_browser_session_state("principal-browser"));
    runtime
        .storage
        .remember_directory_snapshot(signed_browser_directory_snapshot(vec![
            ExperimentDirectoryEntry {
                network_id: NetworkId::new("net-browser"),
                study_id: StudyId::new("study-browser"),
                experiment_id: ExperimentId::new("exp-browser"),
                workload_id: WorkloadId::new("wgpu-demo"),
                display_name: "Browser Demo".into(),
                model_schema_hash: ContentId::new("model-browser"),
                dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
                resource_requirements: ExperimentResourceRequirements {
                    minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
                    minimum_device_memory_bytes: Some(1024),
                    minimum_system_memory_bytes: Some(2048),
                    estimated_download_bytes: 4096,
                    estimated_window_seconds: 30,
                },
                visibility: ExperimentVisibility::Public,
                opt_in_policy: ExperimentOptInPolicy::Open,
                current_revision_id: RevisionId::new("rev-browser"),
                current_head_id: Some(HeadId::new("head-browser")),
                allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                allowed_scopes: BTreeSet::from([ExperimentScope::Connect]),
                metadata: Default::default(),
            },
        ]));
    runtime
        .storage
        .remember_leaderboard_snapshot(signed_browser_leaderboard_snapshot(vec![
            BrowserLeaderboardEntry {
                identity: BrowserLeaderboardIdentity {
                    principal_id: Some(PrincipalId::new("principal-browser")),
                    peer_ids: BTreeSet::from([PeerId::new("peer-browser")]),
                    label: "principal-browser".into(),
                    social_profile: None,
                },
                accepted_work_score: 2.0,
                quality_weighted_impact_score: 2.5,
                validation_service_score: 1.0,
                artifact_serving_score: 0.5,
                leaderboard_score_v1: 3.0,
                accepted_receipt_count: 2,
                last_receipt_at: Some(Utc::now()),
                badges: Vec::new(),
            },
        ]));
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });
    runtime.storage.remember_head(HeadId::new("head-browser"));
    runtime
        .storage
        .remember_chunk_artifact(ArtifactId::new("artifact-browser"));
    runtime
        .storage
        .remember_microshard(burn_p2p::MicroShardId::new("micro-browser"));
    runtime
        .storage
        .queue_receipt(burn_p2p::ContributionReceipt {
            receipt_id: burn_p2p::ContributionReceiptId::new("receipt-browser"),
            peer_id: PeerId::new("peer-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            base_head_id: HeadId::new("head-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            accepted_at: Utc::now(),
            accepted_weight: 1.0,
            metrics: BTreeMap::new(),
            merge_cert_id: None,
        });
    runtime
        .storage
        .remember_metrics_live_event(MetricsLiveEvent {
            network_id: NetworkId::new("net-browser"),
            kind: MetricsLiveEventKind::CatchupRefresh,
            cursors: Vec::new(),
            generated_at: Utc::now(),
        });
    runtime
        .storage
        .remember_metrics_catchup(vec![MetricsCatchupBundle {
            network_id: NetworkId::new("net-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            snapshot: MetricsSnapshot {
                manifest: MetricsSnapshotManifest {
                    network_id: NetworkId::new("net-browser"),
                    experiment_id: ExperimentId::new("exp-browser"),
                    revision_id: RevisionId::new("rev-browser"),
                    snapshot_seq: 4,
                    covers_until_head_id: Some(HeadId::new("head-browser")),
                    covers_until_merge_window_id: None,
                    canonical_head_metrics_ref: ContentId::new("metrics-canonical"),
                    window_rollups_ref: ContentId::new("metrics-window"),
                    network_rollups_ref: ContentId::new("metrics-network"),
                    leaderboard_ref: None,
                    created_at: Utc::now(),
                    signatures: Vec::new(),
                },
                head_eval_reports: Vec::new(),
                peer_window_metrics: vec![PeerWindowMetrics {
                    network_id: NetworkId::new("net-browser"),
                    experiment_id: ExperimentId::new("exp-browser"),
                    revision_id: RevisionId::new("rev-browser"),
                    workload_id: WorkloadId::new("wgpu-demo"),
                    dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
                    peer_id: PeerId::new("peer-browser"),
                    principal_id: Some(PrincipalId::new("principal-browser")),
                    lease_id: LeaseId::new("lease-browser"),
                    base_head_id: HeadId::new("head-browser"),
                    window_started_at: Utc::now() - chrono::Duration::seconds(10),
                    window_finished_at: Utc::now(),
                    attempted_tokens_or_samples: 200,
                    accepted_tokens_or_samples: Some(180),
                    local_train_loss_mean: Some(0.30),
                    local_train_loss_last: Some(0.30),
                    grad_or_delta_norm: Some(1.2),
                    optimizer_step_count: 10,
                    compute_time_ms: 10_000,
                    data_fetch_time_ms: 600,
                    publish_latency_ms: 250,
                    head_lag_at_start: 1,
                    head_lag_at_finish: 0,
                    backend_class: BackendClass::BrowserWgpu,
                    role: PeerRole::BrowserTrainerWgpu,
                    status: burn_p2p::PeerWindowStatus::Completed,
                    status_reason: None,
                }],
                reducer_cohort_metrics: Vec::new(),
                derived_metrics: Vec::new(),
            },
            ledger_segments: Vec::new(),
            generated_at: Utc::now(),
        }]);

    let model = BrowserAppModel::from_runtime(runtime);
    let view = model.view(&BrowserUiBindings::new("https://edge.example"));

    assert_eq!(view.default_surface, burn_p2p_ui::BrowserAppSurface::Train);
    assert_eq!(view.runtime_label, "train");
    assert!(view.capability_summary.contains("train"));
    assert!(view.session_label.contains("principal-browser"));
    assert_eq!(view.viewer.visible_experiments, 1);
    assert_eq!(view.viewer.visible_heads, 1);
    assert_eq!(view.viewer.leaderboard_entries, 1);
    assert!(!view.validation.validate_available);
    assert!(!view.validation.can_validate);
    assert!(view.training.train_available);
    assert!(view.training.can_train);
    assert_eq!(
        view.training.active_assignment.as_deref(),
        Some("exp-browser/rev-browser")
    );
    assert_eq!(
        view.selected_experiment
            .as_ref()
            .map(|selected| selected.display_name.as_str()),
        Some("Browser Demo")
    );
    assert_eq!(
        view.training.latest_head_id.as_deref(),
        Some("head-browser")
    );
    assert_eq!(view.training.cached_chunk_artifacts, 1);
    assert_eq!(view.training.cached_microshards, 1);
    assert_eq!(view.training.pending_receipts, 1);
    assert_eq!(view.training.optimizer_steps, Some(10));
    assert_eq!(view.training.accepted_samples, Some(180));
    assert_eq!(view.training.last_loss.as_deref(), Some("0.3000"));
    assert_eq!(view.training.publish_latency_ms, Some(250));
    assert_eq!(
        view.training.throughput_summary.as_deref(),
        Some("18.0 sample/s")
    );
    assert_eq!(view.network.transport, "webtransport");
    assert!(view.network.metrics_live_ready);
    assert!(!view.viewer.experiments_preview.is_empty());
    assert!(!view.viewer.leaderboard_preview.is_empty());
}

#[test]
fn browser_app_model_applies_worker_events_to_local_state() {
    let runtime = BrowserWorkerRuntime {
        config: Some(BrowserRuntimeConfig::new(
            "https://edge.example",
            NetworkId::new("net-browser"),
            ContentId::new("train-browser"),
            "browser-wasm",
            ContentId::new("artifact-browser"),
        )),
        ..BrowserWorkerRuntime::default()
    };
    let mut model = BrowserAppModel::from_runtime(runtime);

    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_session(sample_browser_session_state("principal-verifier"));
    storage.remember_directory_snapshot(signed_browser_directory_snapshot(Vec::new()));
    storage.remember_assignment(BrowserStoredAssignment {
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
    });
    storage.queue_receipt(burn_p2p::ContributionReceipt {
        receipt_id: burn_p2p::ContributionReceiptId::new("receipt-browser"),
        peer_id: PeerId::new("peer-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        base_head_id: HeadId::new("head-browser-1"),
        artifact_id: ArtifactId::new("artifact-browser"),
        accepted_at: Utc::now(),
        accepted_weight: 1.0,
        metrics: BTreeMap::new(),
        merge_cert_id: None,
    });

    model.apply_event(BrowserWorkerEvent::Ready(BrowserCapabilityReport {
        recommended_role: BrowserRuntimeRole::BrowserVerifier,
        ..BrowserCapabilityReport::default()
    }));
    model.apply_event(BrowserWorkerEvent::RuntimeStateChanged(
        BrowserRuntimeState::Verifier,
    ));
    model.apply_event(BrowserWorkerEvent::TransportChanged(
        BrowserTransportStatus {
            active: Some(BrowserTransportKind::WebRtcDirect),
            ..BrowserTransportStatus::default()
        },
    ));
    model.apply_event(BrowserWorkerEvent::SessionUpdated(Box::new(
        sample_browser_session_state("principal-verifier"),
    )));
    model.apply_event(BrowserWorkerEvent::StorageUpdated(Box::new(storage)));
    model.apply_event(BrowserWorkerEvent::DirectoryUpdated {
        network_id: NetworkId::new("net-browser"),
        visible_entries: 0,
    });
    model.apply_event(BrowserWorkerEvent::ValidationCompleted(
        BrowserValidationResult {
            head_id: HeadId::new("head-browser-2"),
            accepted: true,
            checked_chunks: 8,
            emitted_receipt_id: Some(burn_p2p::ContributionReceiptId::new("receipt-browser")),
        },
    ));
    model.apply_event(BrowserWorkerEvent::ReceiptsAcknowledged {
        receipt_ids: vec![burn_p2p::ContributionReceiptId::new("receipt-browser")],
        pending_receipts: 0,
    });
    model.apply_event(BrowserWorkerEvent::MetricsUpdated(Box::new(
        MetricsLiveEvent {
            network_id: NetworkId::new("net-browser"),
            kind: MetricsLiveEventKind::CatchupRefresh,
            cursors: Vec::new(),
            generated_at: Utc::now(),
        },
    )));
    model.apply_event(BrowserWorkerEvent::Error {
        message: "edge degraded".into(),
    });

    let view = model.view(&BrowserUiBindings::new("https://edge.example"));

    assert_eq!(
        view.default_surface,
        burn_p2p_ui::BrowserAppSurface::Validate
    );
    assert_eq!(view.runtime_label, "validate");
    assert!(view.capability_summary.contains("validate"));
    assert!(view.session_label.contains("principal-verifier"));
    assert!(!view.validation.validate_available);
    assert!(view.validation.can_validate);
    assert_eq!(
        view.validation.current_head_id.as_deref(),
        Some("head-browser-2")
    );
    assert_eq!(view.validation.pending_receipts, 0);
    assert!(view.validation.metrics_sync_at.is_some());
    assert_eq!(
        view.validation.validation_status.as_deref(),
        Some("accepted")
    );
    assert_eq!(view.validation.checked_chunks, Some(8));
    assert_eq!(
        view.validation.emitted_receipt_id.as_deref(),
        Some("receipt-browser")
    );
    assert_eq!(view.network.transport, "webrtc-direct");
    assert!(view.network.metrics_live_ready);
    assert!(view.network.last_directory_sync_at.is_some());
    assert_eq!(view.network.last_error.as_deref(), Some("edge degraded"));
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_pumps_metrics_events_into_worker_without_duplicate_replays() {
    let first_event = MetricsLiveEvent {
        network_id: NetworkId::new("net-browser"),
        kind: MetricsLiveEventKind::CatchupRefresh,
        cursors: vec![MetricsSyncCursor {
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            latest_snapshot_seq: Some(4),
            latest_ledger_segment_seq: Some(2),
            latest_head_id: Some(burn_p2p::HeadId::new("head-browser-4")),
            latest_merge_window_id: None,
        }],
        generated_at: Utc::now(),
    };
    let second_event = MetricsLiveEvent {
        network_id: NetworkId::new("net-browser"),
        kind: MetricsLiveEventKind::CatchupRefresh,
        cursors: vec![MetricsSyncCursor {
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            latest_snapshot_seq: Some(5),
            latest_ledger_segment_seq: Some(3),
            latest_head_id: Some(burn_p2p::HeadId::new("head-browser-5")),
            latest_merge_window_id: None,
        }],
        generated_at: Utc::now(),
    };
    let (base_url, handle) = spawn_metrics_sse_poll_server(vec![
        vec![first_event.clone()],
        vec![second_event.clone()],
        vec![second_event.clone()],
    ]);
    let client = BrowserPortalClient::new(
        BrowserUiBindings::new(base_url),
        BrowserEnrollmentConfig {
            network_id: NetworkId::new("net-browser"),
            project_family_id: burn_p2p::ProjectFamilyId::new("family-browser"),
            release_train_hash: ContentId::new("train-browser"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("artifact-browser"),
            login_path: "/login".into(),
            callback_path: "/callback".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::new(),
            session_ttl_secs: 900,
        },
    );
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig::new(
            "https://edge.example",
            NetworkId::new("net-browser"),
            ContentId::new("train-browser"),
            "browser-wasm",
            ContentId::new("artifact-browser"),
        ),
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });

    let worker_events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .pump_metrics_live_events_into_worker(&mut runtime, 3, 1)
                .await
                .expect("pump live metrics events into worker")
        });

    handle.join().expect("join polling sse thread");
    let metrics_updates = worker_events
        .iter()
        .filter(|event| matches!(event, BrowserWorkerEvent::MetricsUpdated(_)))
        .count();
    assert_eq!(metrics_updates, 2);
    let head_updates = worker_events
        .iter()
        .filter_map(|event| match event {
            BrowserWorkerEvent::HeadUpdated { head_id } => Some(head_id.as_str().to_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        head_updates,
        vec!["head-browser-4".to_owned(), "head-browser-5".to_owned()]
    );
    let storage = worker_events
        .iter()
        .rev()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert_eq!(
        storage
            .last_metrics_live_event
            .as_ref()
            .expect("last metrics live event")
            .cursors[0]
            .latest_head_id
            .as_ref()
            .expect("latest head")
            .as_str(),
        "head-browser-5"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_pumps_edge_metrics_sync_into_worker_without_sse() {
    let first_event = MetricsLiveEvent {
        network_id: NetworkId::new("net-browser"),
        kind: MetricsLiveEventKind::CatchupRefresh,
        cursors: vec![MetricsSyncCursor {
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            latest_snapshot_seq: Some(4),
            latest_ledger_segment_seq: Some(2),
            latest_head_id: Some(burn_p2p::HeadId::new("head-browser-4")),
            latest_merge_window_id: None,
        }],
        generated_at: Utc::now(),
    };
    let second_event = MetricsLiveEvent {
        network_id: NetworkId::new("net-browser"),
        kind: MetricsLiveEventKind::CatchupRefresh,
        cursors: vec![MetricsSyncCursor {
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            latest_snapshot_seq: Some(5),
            latest_ledger_segment_seq: Some(3),
            latest_head_id: Some(burn_p2p::HeadId::new("head-browser-5")),
            latest_merge_window_id: None,
        }],
        generated_at: Utc::now(),
    };
    let (base_url, handle) = spawn_metrics_latest_poll_server(vec![
        first_event.clone(),
        second_event.clone(),
        second_event.clone(),
    ]);
    let client = BrowserPortalClient::new(
        BrowserUiBindings::new(base_url),
        BrowserEnrollmentConfig {
            network_id: NetworkId::new("net-browser"),
            project_family_id: burn_p2p::ProjectFamilyId::new("family-browser"),
            release_train_hash: ContentId::new("train-browser"),
            target_artifact_id: "browser-wasm".into(),
            target_artifact_hash: ContentId::new("artifact-browser"),
            login_path: "/login".into(),
            callback_path: "/callback".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
            requested_scopes: BTreeSet::new(),
            session_ttl_secs: 900,
        },
    );
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig::new(
            "https://edge.example",
            NetworkId::new("net-browser"),
            ContentId::new("train-browser"),
            "browser-wasm",
            ContentId::new("artifact-browser"),
        ),
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });

    let worker_events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .pump_edge_metrics_sync_into_worker(&mut runtime, 3, 0)
                .await
                .expect("pump edge metrics sync into worker")
        });

    handle.join().expect("join latest polling thread");
    let metrics_updates = worker_events
        .iter()
        .filter(|event| matches!(event, BrowserWorkerEvent::MetricsUpdated(_)))
        .count();
    assert_eq!(metrics_updates, 2);
    let head_updates = worker_events
        .iter()
        .filter_map(|event| match event {
            BrowserWorkerEvent::HeadUpdated { head_id } => Some(head_id.as_str().to_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        head_updates,
        vec!["head-browser-4".to_owned(), "head-browser-5".to_owned()]
    );
    let storage = worker_events
        .iter()
        .rev()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert_eq!(
        storage
            .last_metrics_live_event
            .as_ref()
            .expect("last metrics live event")
            .cursors[0]
            .latest_head_id
            .as_ref()
            .expect("latest head")
            .as_str(),
        "head-browser-5"
    );
}
