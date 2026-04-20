use super::*;
use crate::app::{should_fallback_to_edge_control_sync, should_wait_for_direct_swarm_bootstrap};
use std::collections::{BTreeMap, BTreeSet};
#[cfg(not(target_arch = "wasm32"))]
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
    time::Duration as StdDuration,
};

use burn_p2p::{
    ArtifactId, AuthProvider, BrowserRole, BrowserRolePolicy, BrowserVisibilityPolicy, ContentId,
    ContributionReceipt, ContributionReceiptId, ExperimentDirectoryEntry,
    ExperimentDirectoryPolicyExt, ExperimentDirectoryProjectionBuilder, ExperimentId,
    ExperimentOptInPolicy, ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility,
    HeadId, LoginStart, NetworkId, PeerId, PeerRole, PeerRoleSet, PrincipalClaims, PrincipalId,
    PrincipalSession, RevisionId, StudyId, WindowActivation, WindowId, WorkloadId,
};
use burn_p2p_core::{
    ArtifactProfile, BackendClass, BrowserArtifactSource, BrowserDirectorySnapshot,
    BrowserEdgeMode, BrowserEdgePaths, BrowserEdgeSnapshot, BrowserLeaderboardEntry,
    BrowserLeaderboardIdentity, BrowserLeaderboardSnapshot, BrowserLoginProvider,
    BrowserResolvedSeedBootstrap, BrowserSeedAdvertisement, BrowserSeedBootstrapSource,
    BrowserSeedRecord, BrowserSeedTransportKind, BrowserSeedTransportPolicy, BrowserSwarmPhase,
    BrowserSwarmStatus, BrowserTransportFamily, BrowserTransportObservationSource,
    BrowserTransportSurface, ChunkDescriptor, DownloadDeliveryMode, DownloadTicket,
    DownloadTicketId, HeadEvalReport, LeaseId, MetricValue, MetricsLiveEvent, MetricsLiveEventKind,
    MetricsSnapshotManifest, MetricsSyncCursor, PeerWindowMetrics, Precision, PublicationTargetId,
    PublishedArtifactId, PublishedArtifactRecord, PublishedArtifactStatus, ReenrollmentStatus,
    RevocationEpoch, RunId, SchemaEnvelope, SignatureAlgorithm, SignatureMetadata, SignedPayload,
    TrustBundleExport,
};
use burn_p2p_metrics::{MetricsCatchupBundle, MetricsSnapshot, derive_network_performance_summary};
use burn_p2p_swarm::BrowserSwarmBootstrap;
use chrono::Utc;
use semver::Version;

const TEST_EDGE_WEBRTC_DIRECT_SEED: &str = "/dns4/edge.example/udp/4001/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const TEST_BOOTSTRAP_WEBRTC_DIRECT_SEED: &str = "/dns4/bootstrap.example/udp/4001/webrtc-direct/certhash/uEiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const TEST_EDGE_WEBTRANSPORT_SEED: &str = "/dns4/edge.example/udp/443/quic-v1/webtransport/certhash/uEiBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";

fn browser_test_edge_snapshot() -> BrowserEdgeSnapshot {
    BrowserEdgeSnapshot {
        network_id: NetworkId::new("net-browser"),
        edge_mode: BrowserEdgeMode::Peer,
        browser_mode: burn_p2p::BrowserMode::Trainer,
        social_mode: burn_p2p::SocialMode::Public,
        profile_mode: burn_p2p::ProfileMode::Public,
        transports: BrowserTransportSurface {
            webrtc_direct: true,
            webtransport_gateway: true,
            wss_fallback: true,
        },
        paths: BrowserEdgePaths::default(),
        auth_enabled: false,
        login_providers: Vec::new(),
        required_release_train_hash: Some(ContentId::new("train-browser")),
        allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact-browser")]),
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
        trust_bundle: None,
        captured_at: Utc::now(),
    }
}

fn signed_browser_seed_advertisement(
    network_id: &NetworkId,
) -> SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_seed_advertisement",
            Version::new(0, 1, 0),
            BrowserSeedAdvertisement {
                schema_version: u32::from(burn_p2p_core::SCHEMA_VERSION),
                network_id: network_id.clone(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::minutes(10),
                transport_policy: BrowserSeedTransportPolicy {
                    preferred: vec![
                        BrowserSeedTransportKind::WebRtcDirect,
                        BrowserSeedTransportKind::WssFallback,
                    ],
                    allow_fallback_wss: true,
                },
                seeds: vec![BrowserSeedRecord {
                    peer_id: Some(PeerId::new("seed-browser")),
                    multiaddrs: vec![
                        TEST_EDGE_WEBRTC_DIRECT_SEED.into(),
                        "/dns4/edge.example/tcp/443/wss".into(),
                    ],
                }],
            },
        ),
        SignatureMetadata {
            signer: PeerId::new("bootstrap"),
            key_id: "browser-seeds".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "deadbeef".into(),
        },
    )
    .expect("signed browser seed advertisement")
}

fn conformance_revision_manifest() -> burn_p2p::RevisionManifest {
    burn_p2p::RevisionManifest {
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        workload_id: WorkloadId::new("browser-demo"),
        required_release_train_hash: ContentId::new("train-browser"),
        model_schema_hash: ContentId::new("model-browser"),
        checkpoint_format_hash: ContentId::new("checkpoint-browser"),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
        training_config_hash: ContentId::new("training-browser"),
        merge_topology_policy_hash: ContentId::new("topology-browser"),
        slot_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(1024),
            estimated_download_bytes: 1024,
            estimated_window_seconds: 30,
        },
        activation_window: WindowActivation {
            activation_window: WindowId(1),
            grace_windows: 0,
        },
        lag_policy: burn_p2p::LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::default(),
        robustness_policy: None,
        browser_enabled: true,
        browser_role_policy: BrowserRolePolicy {
            observer: true,
            verifier: true,
            trainer_wgpu: true,
            fallback: false,
        },
        max_browser_checkpoint_bytes: Some(16 * 1024 * 1024),
        max_browser_window_secs: Some(30),
        max_browser_shard_bytes: Some(8 * 1024 * 1024),
        requires_webgpu: true,
        max_browser_batch_size: Some(4),
        recommended_browser_precision: Some(Precision::Fp16),
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: "browser conformance revision".into(),
    }
}

#[test]
fn worker_bridge_messages_round_trip_through_json() {
    let command = BrowserWorkerCommand::Train(BrowserTrainingPlan {
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        workload_id: WorkloadId::new("wgpu-demo"),
        budget: BrowserTrainingBudget::default(),
        lease: None,
    });
    let bytes = serde_json::to_vec(&command).expect("serialize command");
    let decoded: BrowserWorkerCommand =
        serde_json::from_slice(&bytes).expect("deserialize command");
    assert_eq!(decoded, command);
}

#[test]
fn browser_conformance_harness_executes_authenticated_training_and_validation() {
    let revision = conformance_revision_manifest();
    let entry = ExperimentDirectoryProjectionBuilder::from_revision(
        NetworkId::new("net-browser"),
        StudyId::new("study-browser"),
        "Browser Demo",
        &revision,
    )
    .with_visibility(ExperimentVisibility::Public)
    .with_opt_in_policy(ExperimentOptInPolicy::Open)
    .with_scope(ExperimentScope::Connect)
    .with_scope(ExperimentScope::Train {
        experiment_id: revision.experiment_id.clone(),
    })
    .with_scope(ExperimentScope::Validate {
        experiment_id: revision.experiment_id.clone(),
    })
    .build();
    let directory =
        browser_conformance_directory(NetworkId::new("net-browser"), vec![entry.clone()]);
    let session = browser_conformance_session(
        NetworkId::new("net-browser"),
        PrincipalId::new("browser-principal"),
        BTreeSet::from([
            ExperimentScope::Connect,
            ExperimentScope::Train {
                experiment_id: revision.experiment_id.clone(),
            },
            ExperimentScope::Validate {
                experiment_id: revision.experiment_id.clone(),
            },
        ]),
    );
    let mut trainer_harness = BrowserConformanceHarness::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            site_seed_node_urls: vec!["/dns4/edge.example/tcp/443/wss".into()],
            ..BrowserRuntimeConfig::new(
                "https://edge.example",
                NetworkId::new("net-browser"),
                ContentId::new("train-browser"),
                "browser-wasm",
                ContentId::new("artifact-browser"),
            )
        },
        browser_conformance_capability_for_role(BrowserRuntimeRole::BrowserTrainerWgpu),
        browser_conformance_transport(),
        directory,
        session,
    );

    let heads = [burn_p2p::HeadDescriptor {
        head_id: HeadId::new("head-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        artifact_id: ArtifactId::new("artifact-browser-head"),
        parent_head_id: None,
        global_step: 1,
        created_at: Utc::now(),
        metrics: BTreeMap::new(),
    }];
    trainer_harness.apply_heads(&heads);

    let training = trainer_harness
        .run_training(BrowserTrainingPlan {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            workload_id: WorkloadId::new("browser-demo"),
            budget: BrowserTrainingBudget::default(),
            lease: None,
        })
        .expect("training result");

    let mut verifier_harness = BrowserConformanceHarness::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserVerifier,
            site_seed_node_urls: vec!["/dns4/edge.example/tcp/443/wss".into()],
            ..BrowserRuntimeConfig::new(
                "https://edge.example",
                NetworkId::new("net-browser"),
                ContentId::new("verify-browser"),
                "browser-wasm",
                ContentId::new("artifact-browser"),
            )
        },
        browser_conformance_capability_for_role(BrowserRuntimeRole::BrowserVerifier),
        browser_conformance_transport(),
        browser_conformance_directory(NetworkId::new("net-browser"), vec![entry.clone()]),
        browser_conformance_session(
            NetworkId::new("net-browser"),
            PrincipalId::new("principal-browser"),
            BTreeSet::from([
                ExperimentScope::Train {
                    experiment_id: revision.experiment_id.clone(),
                },
                ExperimentScope::Validate {
                    experiment_id: revision.experiment_id.clone(),
                },
            ]),
        ),
    );
    verifier_harness.apply_heads(&heads);

    let validation = verifier_harness
        .run_validation(BrowserValidationPlan {
            head_id: HeadId::new("head-browser"),
            max_checkpoint_bytes: 16 * 1024 * 1024,
            sample_budget: 4,
            emit_receipt: true,
        })
        .expect("validation result");

    assert_eq!(training.window_secs, 30);
    assert!(training.receipt_id.is_some());
    assert!(validation.accepted);
    assert_eq!(validation.checked_chunks, 4);
    assert!(!trainer_harness.pending_receipts().is_empty());
    assert!(!verifier_harness.pending_receipts().is_empty());
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
            BrowserTransportKind::WebRtcDirect,
            BrowserTransportKind::WebTransport,
            BrowserTransportKind::WssFallback,
        ]
    );
}

#[test]
fn browser_app_target_maps_to_expected_runtime_roles() {
    assert_eq!(
        BrowserAppTarget::Viewer.preferred_role(),
        BrowserRuntimeRole::Viewer
    );
    assert_eq!(
        BrowserAppTarget::Observe.preferred_role(),
        BrowserRuntimeRole::BrowserObserver
    );
    assert_eq!(
        BrowserAppTarget::Validate.preferred_role(),
        BrowserRuntimeRole::BrowserVerifier
    );
    assert_eq!(
        BrowserAppTarget::Train.preferred_role(),
        BrowserRuntimeRole::BrowserTrainerWgpu
    );
    assert_eq!(
        BrowserAppTarget::Custom(BrowserRuntimeRole::BrowserFallback).preferred_role(),
        BrowserRuntimeRole::BrowserFallback
    );
}

#[test]
fn browser_app_connect_config_tracks_target_and_selection() {
    let snapshot = browser_test_edge_snapshot();
    let signed_seed_advertisement = signed_browser_seed_advertisement(&snapshot.network_id);
    let config = BrowserAppConnectConfig::new(
        "https://edge.example",
        BrowserCapabilityReport::default(),
        BrowserAppTarget::Train,
    )
    .with_selection("exp-browser", Some("rev-browser"))
    .with_seed_node_urls(vec!["/dns4/site.example/tcp/443/wss".into()])
    .with_bootstrap_material(
        Some(snapshot.clone()),
        Some(signed_seed_advertisement.clone()),
    );

    assert_eq!(config.target, BrowserAppTarget::Train);
    assert_eq!(
        config.selected_experiment(),
        Some(("exp-browser".to_owned(), Some("rev-browser".to_owned())))
    );
    assert_eq!(
        config.seed_node_urls,
        vec!["/dns4/site.example/tcp/443/wss".to_owned()]
    );
    assert_eq!(config.bootstrap_snapshot, Some(snapshot));
    assert_eq!(
        config.bootstrap_signed_seed_advertisement,
        Some(signed_seed_advertisement)
    );
}

#[test]
fn browser_app_connect_config_target_builders_choose_expected_presets() {
    let capability = BrowserCapabilityReport::default();

    assert_eq!(
        BrowserAppConnectConfig::viewer("https://edge.example", capability.clone()).target,
        BrowserAppTarget::Viewer
    );
    assert_eq!(
        BrowserAppConnectConfig::observe("https://edge.example", capability.clone()).target,
        BrowserAppTarget::Observe
    );
    assert_eq!(
        BrowserAppConnectConfig::validate("https://edge.example", capability.clone()).target,
        BrowserAppTarget::Validate
    );
    assert_eq!(
        BrowserAppConnectConfig::train("https://edge.example", capability.clone()).target,
        BrowserAppTarget::Train
    );
    assert_eq!(
        BrowserAppConnectConfig::custom(
            "https://edge.example",
            capability,
            BrowserRuntimeRole::BrowserFallback
        )
        .target,
        BrowserAppTarget::Custom(BrowserRuntimeRole::BrowserFallback)
    );
}

#[test]
fn browser_app_controller_connect_with_preloaded_bootstrap_survives_edge_refresh_failure() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let snapshot = browser_test_edge_snapshot();
    let signed_seed_advertisement = signed_browser_seed_advertisement(&snapshot.network_id);

    let controller = runtime
        .block_on(async {
            BrowserAppController::connect_with(
                BrowserAppConnectConfig::observe(
                    "http://127.0.0.1:9",
                    BrowserCapabilityReport::default(),
                )
                .with_seed_node_urls(vec!["/dns4/site.example/tcp/443/wss".into()])
                .with_bootstrap_material(Some(snapshot), Some(signed_seed_advertisement)),
            )
            .await
        })
        .expect("connect with baked bootstrap");

    let view = controller.view();
    assert_eq!(view.network.edge_base_url, "http://127.0.0.1:9");
    assert!(
        view.network
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("127.0.0.1:9"))
    );
}

#[test]
fn browser_transport_policy_tracks_swarm_browser_transport_order() {
    let policy = BrowserTransportPolicy::from(burn_p2p::RuntimeTransportPolicy::browser_for_roles(
        &PeerRoleSet::default_trainer(),
    ));
    assert_eq!(
        policy.preferred,
        vec![
            BrowserTransportKind::WebRtcDirect,
            BrowserTransportKind::WebTransport,
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
                performance_summary: None,
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
                performance_summary: None,
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
fn browser_storage_bounds_receipt_membership_history() {
    let mut storage = BrowserStorageSnapshot::default();

    for index in 0..600 {
        storage.remember_receipt(burn_p2p::ContributionReceiptId::new(format!(
            "stored-{index:03}"
        )));
    }
    assert_eq!(storage.stored_receipts.len(), 512);
    assert!(
        !storage
            .stored_receipts
            .contains(&burn_p2p::ContributionReceiptId::new("stored-000"))
    );
    assert!(
        storage
            .stored_receipts
            .contains(&burn_p2p::ContributionReceiptId::new("stored-599"))
    );

    let acknowledged = (0..600)
        .map(|index| burn_p2p::ContributionReceiptId::new(format!("submitted-{index:03}")))
        .collect::<Vec<_>>();
    storage.acknowledge_receipts(&acknowledged);
    assert_eq!(storage.submitted_receipts.len(), 512);
    assert!(
        !storage
            .submitted_receipts
            .contains(&burn_p2p::ContributionReceiptId::new("submitted-000"))
    );
    assert!(
        storage
            .submitted_receipts
            .contains(&burn_p2p::ContributionReceiptId::new("submitted-599"))
    );
}

#[test]
fn browser_storage_deserializes_legacy_pending_receipt_arrays() {
    let snapshot = serde_json::json!({
        "metadata_version": 1,
        "session": BrowserSessionState::default(),
        "cached_chunk_artifacts": [],
        "cached_head_artifact_heads": [],
        "cached_microshards": [],
        "stored_receipts": [],
        "pending_receipts": [{
            "receipt_id": "receipt-browser",
            "peer_id": "peer-browser",
            "study_id": "study-browser",
            "experiment_id": "exp-browser",
            "revision_id": "rev-browser",
            "base_head_id": "head-browser",
            "artifact_id": "artifact-browser",
            "accepted_at": Utc::now(),
            "accepted_weight": 1.0,
            "metrics": {},
            "merge_cert_id": null
        }],
        "submitted_receipts": [],
        "metrics_catchup_bundles": [],
        "last_metrics_live_event": null,
        "last_metrics_sync_at": null,
        "last_head_id": null,
        "stored_certificate_peer_id": null,
        "active_assignment": null,
        "updated_at": Utc::now()
    });

    let storage: BrowserStorageSnapshot =
        serde_json::from_value(snapshot).expect("deserialize legacy browser storage");
    assert_eq!(
        storage.pending_receipts.backend,
        BrowserReceiptOutboxBackend::Snapshot
    );
    assert_eq!(storage.pending_receipts.len(), 1);
}

#[test]
fn browser_receipt_outbox_supports_structured_durable_backend() {
    let mut storage = BrowserStorageSnapshot::default();
    storage
        .pending_receipts
        .configure_backend(BrowserReceiptOutboxBackend::IndexedDb);
    storage.queue_receipt(ContributionReceipt {
        receipt_id: ContributionReceiptId::new("receipt-browser"),
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

    let encoded = serde_json::to_value(&storage).expect("serialize storage");
    assert_eq!(encoded["pending_receipts"]["backend"], "IndexedDb");
    let receipts = encoded["pending_receipts"]["receipts"]
        .as_array()
        .expect("pending_receipts.receipts should serialize as an array");
    assert_eq!(receipts.len(), 1);

    let decoded: BrowserStorageSnapshot =
        serde_json::from_value(encoded).expect("deserialize structured storage");
    assert_eq!(
        decoded.pending_receipts.backend,
        BrowserReceiptOutboxBackend::IndexedDb
    );
    assert_eq!(decoded.pending_receipts.len(), 1);
}

#[test]
fn browser_storage_round_trips_active_training_lease() {
    let mut storage = BrowserStorageSnapshot::default();
    storage.active_training_lease = Some(sample_training_lease());

    let encoded = serde_json::to_value(&storage).expect("serialize storage");
    assert_eq!(
        encoded["active_training_lease"]["lease_id"],
        serde_json::Value::String("lease-browser".into())
    );

    let decoded: BrowserStorageSnapshot =
        serde_json::from_value(encoded).expect("deserialize storage");
    assert_eq!(decoded.active_training_lease, Some(sample_training_lease()));
}

#[test]
fn browser_storage_round_trips_artifact_replay_checkpoint() {
    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_artifact_replay_checkpoint(BrowserArtifactReplayCheckpoint {
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        run_id: RunId::new("run-browser"),
        head_id: HeadId::new("head-browser"),
        artifact_id: ArtifactId::new("artifact-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        publication_content_hash: None,
        publication_content_length: None,
        provider_peer_ids: vec![PeerId::new("peer-a"), PeerId::new("peer-b")],
        artifact_descriptor: Some(burn_p2p::ArtifactDescriptor {
            artifact_id: ArtifactId::new("artifact-browser"),
            kind: burn_p2p::ArtifactKind::BrowserDataBundle,
            head_id: Some(HeadId::new("head-browser")),
            base_head_id: Some(HeadId::new("head-parent")),
            precision: Precision::Fp32,
            model_schema_hash: ContentId::new("schema-browser"),
            record_format: "json".into(),
            bytes_len: 8,
            chunks: vec![ChunkDescriptor {
                chunk_id: burn_p2p::ChunkId::new("chunk-a"),
                offset_bytes: 0,
                length_bytes: 4,
                chunk_hash: ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(
                    b"test",
                )),
            }],
            root_hash: ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(
                b"testtest",
            )),
        }),
        completed_chunks: vec![BrowserArtifactReplayChunk {
            chunk_id: burn_p2p::ChunkId::new("chunk-a"),
            storage: BrowserArtifactReplayChunkStorage::Inline,
            persisted_bytes: 4,
            chunk_bytes: b"test".to_vec(),
        }],
        edge_download_prefix: None,
        edge_download_segments: Vec::new(),
        completed_bytes: 4,
        last_attempted_at: Utc::now(),
        attempt_count: 2,
    });

    let encoded = serde_json::to_value(&storage).expect("serialize storage");
    let decoded: BrowserStorageSnapshot =
        serde_json::from_value(encoded).expect("deserialize storage");
    let checkpoint = decoded
        .artifact_replay_checkpoint
        .expect("artifact replay checkpoint should round trip");
    assert_eq!(checkpoint.head_id.as_str(), "head-browser");
    assert_eq!(checkpoint.provider_peer_ids.len(), 2);
    assert_eq!(checkpoint.completed_bytes, 4);
    assert_eq!(checkpoint.completed_chunks.len(), 1);
    assert_eq!(checkpoint.attempt_count, 2);
}

#[test]
fn browser_storage_tracks_partial_artifact_replay_chunks() {
    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_artifact_replay_checkpoint(BrowserArtifactReplayCheckpoint {
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        run_id: RunId::new("run-browser"),
        head_id: HeadId::new("head-browser"),
        artifact_id: ArtifactId::new("artifact-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        publication_content_hash: None,
        publication_content_length: None,
        provider_peer_ids: vec![PeerId::new("peer-a")],
        artifact_descriptor: None,
        completed_chunks: Vec::new(),
        edge_download_prefix: None,
        edge_download_segments: Vec::new(),
        completed_bytes: 0,
        last_attempted_at: Utc::now(),
        attempt_count: 1,
    });

    storage.remember_artifact_replay_descriptor(burn_p2p::ArtifactDescriptor {
        artifact_id: ArtifactId::new("artifact-browser"),
        kind: burn_p2p::ArtifactKind::BrowserDataBundle,
        head_id: Some(HeadId::new("head-browser")),
        base_head_id: Some(HeadId::new("head-parent")),
        precision: Precision::Fp32,
        model_schema_hash: ContentId::new("schema-browser"),
        record_format: "json".into(),
        bytes_len: 8,
        chunks: vec![
            ChunkDescriptor {
                chunk_id: burn_p2p::ChunkId::new("chunk-a"),
                offset_bytes: 0,
                length_bytes: 4,
                chunk_hash: ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(
                    b"test",
                )),
            },
            ChunkDescriptor {
                chunk_id: burn_p2p::ChunkId::new("chunk-b"),
                offset_bytes: 4,
                length_bytes: 4,
                chunk_hash: ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(
                    b"data",
                )),
            },
        ],
        root_hash: ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(b"testdata")),
    });
    storage.remember_artifact_replay_chunk(burn_p2p::ChunkId::new("chunk-a"), b"test".to_vec());

    assert_eq!(
        storage
            .artifact_replay_descriptor()
            .expect("descriptor persisted")
            .artifact_id
            .as_str(),
        "artifact-browser"
    );
    assert_eq!(
        storage
            .artifact_replay_chunk_bytes(&burn_p2p::ChunkId::new("chunk-a"))
            .expect("chunk persisted"),
        b"test"
    );
    assert_eq!(
        storage
            .artifact_replay_checkpoint
            .as_ref()
            .expect("checkpoint")
            .completed_bytes,
        4
    );
}

#[test]
fn browser_storage_externalizes_replay_chunks_for_durable_snapshot() {
    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_artifact_replay_checkpoint(BrowserArtifactReplayCheckpoint {
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        run_id: RunId::new("run-browser"),
        head_id: HeadId::new("head-browser"),
        artifact_id: ArtifactId::new("artifact-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        publication_content_hash: None,
        publication_content_length: None,
        provider_peer_ids: vec![PeerId::new("peer-a")],
        artifact_descriptor: None,
        completed_chunks: Vec::new(),
        edge_download_prefix: None,
        edge_download_segments: Vec::new(),
        completed_bytes: 0,
        last_attempted_at: Utc::now(),
        attempt_count: 1,
    });
    storage.remember_artifact_replay_chunk(burn_p2p::ChunkId::new("chunk-a"), b"test".to_vec());

    let durable = storage.durable_replay_snapshot();
    let checkpoint = durable
        .artifact_replay_checkpoint
        .expect("durable snapshot should keep replay checkpoint");
    assert_eq!(checkpoint.completed_chunks.len(), 1);
    assert!(checkpoint.completed_chunks[0].chunk_bytes.is_empty());
    assert_eq!(
        checkpoint.completed_chunks[0].storage,
        BrowserArtifactReplayChunkStorage::IndexedDb
    );
    assert_eq!(checkpoint.completed_chunks[0].persisted_bytes, 4);
}

#[test]
fn browser_storage_externalizes_edge_replay_prefix_for_durable_snapshot() {
    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_artifact_replay_checkpoint(BrowserArtifactReplayCheckpoint {
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        run_id: RunId::new("run-browser"),
        head_id: HeadId::new("head-browser"),
        artifact_id: ArtifactId::new("artifact-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        publication_content_hash: None,
        publication_content_length: None,
        provider_peer_ids: vec![PeerId::new("peer-a")],
        artifact_descriptor: None,
        completed_chunks: Vec::new(),
        edge_download_prefix: None,
        edge_download_segments: Vec::new(),
        completed_bytes: 0,
        last_attempted_at: Utc::now(),
        attempt_count: 1,
    });
    storage.remember_artifact_replay_edge_prefix(Some(8), b"test".to_vec());
    storage.remember_artifact_replay_edge_prefix(Some(8), b"testdata".to_vec());

    let durable = storage.durable_replay_snapshot();
    let checkpoint = durable
        .artifact_replay_checkpoint
        .expect("durable snapshot should keep replay checkpoint");
    let prefix = checkpoint
        .edge_download_prefix
        .expect("durable snapshot should keep edge replay prefix metadata");
    assert!(prefix.bytes.is_empty());
    assert_eq!(prefix.storage, BrowserArtifactReplayChunkStorage::IndexedDb);
    assert_eq!(prefix.persisted_bytes, 8);
    assert_eq!(prefix.total_bytes, Some(8));
    assert_eq!(checkpoint.edge_download_segments.len(), 2);
    assert_eq!(checkpoint.edge_download_segments[0].start_offset, 0);
    assert_eq!(checkpoint.edge_download_segments[0].persisted_bytes, 4);
    assert_eq!(checkpoint.edge_download_segments[1].start_offset, 4);
    assert_eq!(checkpoint.edge_download_segments[1].persisted_bytes, 4);
}

#[test]
fn browser_storage_reconstructs_edge_replay_prefix_from_segments() {
    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_artifact_replay_checkpoint(BrowserArtifactReplayCheckpoint {
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        run_id: RunId::new("run-browser"),
        head_id: HeadId::new("head-browser"),
        artifact_id: ArtifactId::new("artifact-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        publication_content_hash: None,
        publication_content_length: None,
        provider_peer_ids: vec![PeerId::new("peer-a")],
        artifact_descriptor: None,
        completed_chunks: Vec::new(),
        edge_download_prefix: None,
        edge_download_segments: Vec::new(),
        completed_bytes: 0,
        last_attempted_at: Utc::now(),
        attempt_count: 1,
    });
    storage.remember_artifact_replay_edge_prefix(Some(8), b"test".to_vec());
    storage.remember_artifact_replay_edge_prefix(Some(8), b"testdata".to_vec());
    if let Some(checkpoint) = storage.artifact_replay_checkpoint.as_mut() {
        checkpoint.edge_download_prefix = None;
    }

    assert_eq!(
        storage
            .artifact_replay_edge_prefix_bytes()
            .expect("reconstructed prefix"),
        b"testdata".to_vec()
    );
}

#[test]
fn browser_storage_keeps_replay_state_when_assignment_binding_is_unchanged() {
    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_assignment(BrowserStoredAssignment {
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
    });
    storage.remember_head(HeadId::new("head-browser"));
    storage.remember_artifact_replay_checkpoint(BrowserArtifactReplayCheckpoint {
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        run_id: RunId::new("run-browser"),
        head_id: HeadId::new("head-browser"),
        artifact_id: ArtifactId::new("artifact-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        publication_content_hash: None,
        publication_content_length: None,
        provider_peer_ids: vec![PeerId::new("peer-a")],
        artifact_descriptor: None,
        completed_chunks: Vec::new(),
        edge_download_prefix: None,
        edge_download_segments: Vec::new(),
        completed_bytes: 0,
        last_attempted_at: Utc::now(),
        attempt_count: 1,
    });

    storage.remember_assignment(BrowserStoredAssignment {
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
    });

    assert_eq!(storage.last_head_id, Some(HeadId::new("head-browser")));
    assert!(storage.artifact_replay_checkpoint.is_some());
}

#[test]
fn browser_storage_clears_replay_state_when_assignment_binding_changes() {
    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_assignment(BrowserStoredAssignment {
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
    });
    storage.remember_head(HeadId::new("head-browser"));
    storage.remember_artifact_replay_checkpoint(BrowserArtifactReplayCheckpoint {
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        run_id: RunId::new("run-browser"),
        head_id: HeadId::new("head-browser"),
        artifact_id: ArtifactId::new("artifact-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        publication_content_hash: None,
        publication_content_length: None,
        provider_peer_ids: vec![PeerId::new("peer-a")],
        artifact_descriptor: None,
        completed_chunks: Vec::new(),
        edge_download_prefix: None,
        edge_download_segments: Vec::new(),
        completed_bytes: 0,
        last_attempted_at: Utc::now(),
        attempt_count: 1,
    });

    storage.remember_assignment(BrowserStoredAssignment {
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-next"),
    });

    assert_eq!(storage.last_head_id, None);
    assert!(storage.artifact_replay_checkpoint.is_none());
}

#[test]
fn browser_storage_clears_replay_state_when_directory_revision_advances() {
    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_assignment(BrowserStoredAssignment {
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
    });
    storage.remember_head(HeadId::new("head-browser"));
    storage.remember_artifact_replay_checkpoint(BrowserArtifactReplayCheckpoint {
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        run_id: RunId::new("run-browser"),
        head_id: HeadId::new("head-browser"),
        artifact_id: ArtifactId::new("artifact-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        publication_content_hash: Some(ContentId::new("content-browser-old")),
        publication_content_length: Some(4),
        provider_peer_ids: vec![PeerId::new("peer-a")],
        artifact_descriptor: None,
        completed_chunks: Vec::new(),
        edge_download_prefix: None,
        edge_download_segments: Vec::new(),
        completed_bytes: 0,
        last_attempted_at: Utc::now(),
        attempt_count: 1,
    });

    let next_revision = burn_p2p::RevisionManifest {
        revision_id: RevisionId::new("rev-next"),
        ..conformance_revision_manifest()
    };
    let entry = ExperimentDirectoryProjectionBuilder::from_revision(
        NetworkId::new("net-browser"),
        StudyId::new("study-browser"),
        "Browser Demo",
        &next_revision,
    )
    .with_visibility(ExperimentVisibility::Public)
    .with_opt_in_policy(ExperimentOptInPolicy::Open)
    .with_scope(ExperimentScope::Connect)
    .build();

    storage.remember_directory_snapshot(signed_browser_directory_snapshot(vec![entry]));

    assert_eq!(storage.last_head_id, None);
    assert!(storage.artifact_replay_checkpoint.is_none());
    assert_eq!(
        storage.active_assignment_rollover_revision(),
        Some(RevisionId::new("rev-next"))
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
        visibility_policy: BrowserVisibilityPolicy::AppListed,
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
        Some(BrowserRuntimeState::ViewerOnly)
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

    let ui_state = browser_app_ui_state_from_directory(
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
        burn_p2p_views::BrowserExperimentPickerState::Verifier
    );
    assert_eq!(
        browser_picker.entries[0].recommended_role,
        Some(BrowserRole::Verifier)
    );
    assert!(browser_picker.entries[0].fallback_from_preferred);

    let unauthorized_ui_state = browser_app_ui_state_from_directory(
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
        burn_p2p_views::BrowserExperimentPickerState::Verifier
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
    let transport = BrowserTransportStatus::enabled(true, true, true);
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            site_seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
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
        Some(BrowserTransportKind::WebRtcDirect)
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
        Some(BrowserTransportKind::WebRtcDirect)
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
            site_seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
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
            site_seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
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
            lease: None,
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
            site_seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
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
            site_seed_node_urls: vec!["/dns4/edge.example/tcp/443/wss".into()],
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::disabled().with_last_error("edge transports unavailable"),
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

    runtime.update_transport_status(BrowserTransportStatus::enabled(false, false, true));

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
                    auth_policy_snapshot: None,
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
            lease: None,
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
fn worker_runtime_flushes_receipt_outbox_in_bounded_batches() {
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
    runtime.remember_session(sample_browser_session_state("principal-browser"));

    for offset in 0..80 {
        runtime.storage.queue_receipt(ContributionReceipt {
            receipt_id: ContributionReceiptId::new(format!("receipt-{offset}")),
            peer_id: PeerId::new("peer-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            base_head_id: HeadId::new("head-browser"),
            artifact_id: ArtifactId::new(format!("artifact-{offset}")),
            accepted_at: Utc::now(),
            accepted_weight: 1.0,
            metrics: BTreeMap::new(),
            merge_cert_id: None,
        });
    }

    let flush_events = runtime.apply_command(BrowserWorkerCommand::FlushReceiptOutbox, None, None);
    let flushed_receipts = flush_events
        .iter()
        .find_map(|event| match event {
            BrowserWorkerEvent::ReceiptOutboxReady { receipts, .. } => Some(receipts.clone()),
            _ => None,
        })
        .expect("receipt batch");

    assert_eq!(flushed_receipts.len(), 64);
    assert_eq!(runtime.storage.pending_receipts.len(), 80);
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
            site_seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
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
            lease: None,
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
            site_seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );

    runtime.state = Some(BrowserRuntimeState::Observer);
    runtime.refresh_transport_selection();
    assert_eq!(
        runtime.transport.active,
        Some(BrowserTransportKind::WebRtcDirect)
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
        Some(BrowserTransportKind::WebRtcDirect)
    );
}

#[test]
fn edge_endpoints_match_browser_edge_contract() {
    let bindings = BrowserUiBindings::new("https://edge.example");
    assert_eq!(
        bindings.endpoint_url(&bindings.paths.app_snapshot_path),
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
fn enrollment_config_and_bindings_can_be_derived_from_edge_snapshot() {
    let artifact_hash = ContentId::new("artifact-browser");
    let snapshot = BrowserEdgeSnapshot {
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

    let bindings = BrowserUiBindings::from_edge_snapshot("https://edge.example", &snapshot);
    let enrollment = BrowserEnrollmentConfig::from_edge_snapshot(
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
        bindings.endpoint_url(&bindings.paths.app_snapshot_path),
        "https://edge.example/portal/snapshot"
    );
    assert_eq!(enrollment.project_family_id.as_str(), "family-browser");
    assert_eq!(enrollment.release_train_hash.as_str(), "train-browser");
    assert_eq!(enrollment.login_path, "/login/static");
}

#[test]
fn provider_specific_portal_snapshot_prefers_primary_login_provider() {
    let artifact_hash = ContentId::new("artifact-browser");
    let mut snapshot = BrowserEdgeSnapshot {
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

    let bindings = BrowserUiBindings::from_edge_snapshot("https://edge.example", &snapshot);
    let enrollment = BrowserEnrollmentConfig::from_edge_snapshot(
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
            auth_policy_snapshot: None,
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
                retired_issuer_peer_ids: BTreeSet::new(),
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
            performance_summary: None,
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
fn spawn_callback_capture_server() -> (String, std::thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind callback listener");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("listener local addr")
    );
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept callback request");
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
            let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let header_end = header_end + 4;
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    (name.eq_ignore_ascii_case("content-length")).then_some(value.trim())
                })
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            if request.len() >= header_end + content_length {
                break;
            }
        }

        let request_text = String::from_utf8_lossy(&request);
        let trusted_header = request_text
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                (name.eq_ignore_ascii_case("x-burn-p2p-canary-token")).then_some(
                    value.trim().to_owned(),
                )
            })
            .unwrap_or_default();

        let body = serde_json::to_vec(&PrincipalSession {
            session_id: ContentId::new("session-browser"),
            network_id: NetworkId::new("net-browser"),
            claims: PrincipalClaims {
                principal_id: PrincipalId::new("principal-browser"),
                provider: AuthProvider::GitHub,
                display_name: "browser canary".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: PeerRoleSet::default(),
                granted_scopes: BTreeSet::new(),
                custom_claims: BTreeMap::new(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::minutes(15),
            },
            issued_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(15),
        })
        .expect("serialize principal session");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("write callback headers");
        stream.write_all(&body).expect("write callback body");
        stream.flush().expect("flush callback response");

        trusted_header
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
fn http_ok_json_response<T: serde::Serialize>(value: &T) -> Vec<u8> {
    let body = serde_json::to_vec(value).expect("serialize test json response");
    let mut response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend(body);
    response
}

#[cfg(not(target_arch = "wasm32"))]
fn http_ok_bytes_response(body: &[u8]) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    response
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_artifact_sync_server(
    head_view: burn_p2p_publish::HeadArtifactView,
    download_ticket: burn_p2p_publish::DownloadTicketResponse,
    artifact_bytes: Vec<u8>,
) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind artifact listener");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("artifact listener addr")
    );
    let handle = thread::spawn(move || {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept().expect("accept artifact request");
            stream
                .set_read_timeout(Some(StdDuration::from_secs(1)))
                .expect("set artifact read timeout");
            stream
                .set_write_timeout(Some(StdDuration::from_secs(1)))
                .expect("set artifact write timeout");

            let mut request = Vec::new();
            loop {
                let mut chunk = [0u8; 1024];
                let read = stream.read(&mut chunk).expect("read artifact request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            let request = String::from_utf8_lossy(&request);
            let first_line = request.lines().next().unwrap_or_default().to_owned();
            let response = if first_line.starts_with("GET /artifacts/heads/head-browser ") {
                http_ok_json_response(&head_view)
            } else if first_line.starts_with("POST /artifacts/download-ticket ") {
                http_ok_json_response(&download_ticket)
            } else if first_line.starts_with("GET /artifacts/download/ticket-browser ") {
                http_ok_bytes_response(&artifact_bytes)
            } else {
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
            };
            stream
                .write_all(&response)
                .expect("write artifact response");
            stream.flush().expect("flush artifact response");
        }
    });

    (base_url, handle)
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_resumable_artifact_sync_server(
    head_view: burn_p2p_publish::HeadArtifactView,
    download_ticket: burn_p2p_publish::DownloadTicketResponse,
    artifact_bytes: Vec<u8>,
) -> (String, Arc<Mutex<Vec<String>>>, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind resumable artifact listener");
    let base_url = format!(
        "http://{}",
        listener
            .local_addr()
            .expect("resumable artifact listener addr")
    );
    let seen_ranges = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_ranges_handle = Arc::clone(&seen_ranges);
    let handle = thread::spawn(move || {
        let mut download_attempt = 0usize;
        for _ in 0..6 {
            let (mut stream, _) = listener
                .accept()
                .expect("accept resumable artifact request");
            stream
                .set_read_timeout(Some(StdDuration::from_secs(1)))
                .expect("set resumable artifact read timeout");
            stream
                .set_write_timeout(Some(StdDuration::from_secs(1)))
                .expect("set resumable artifact write timeout");

            let mut request = Vec::new();
            loop {
                let mut chunk = [0u8; 1024];
                let read = stream
                    .read(&mut chunk)
                    .expect("read resumable artifact request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            let request = String::from_utf8_lossy(&request);
            let first_line = request.lines().next().unwrap_or_default().to_owned();
            let range_header = request
                .lines()
                .find_map(|line| {
                    line.strip_prefix("Range: ")
                        .or_else(|| line.strip_prefix("range: "))
                })
                .map(str::to_owned);
            if let Some(range) = range_header.clone() {
                seen_ranges_handle
                    .lock()
                    .expect("seen ranges should not be poisoned")
                    .push(range);
            }

            if first_line.starts_with("GET /artifacts/heads/head-browser ") {
                let response = http_ok_json_response(&head_view);
                stream
                    .write_all(&response)
                    .expect("write resumable head view response");
                stream.flush().expect("flush resumable head view response");
                continue;
            }
            if first_line.starts_with("POST /artifacts/download-ticket ") {
                let response = http_ok_json_response(&download_ticket);
                stream
                    .write_all(&response)
                    .expect("write resumable download ticket response");
                stream
                    .flush()
                    .expect("flush resumable download ticket response");
                continue;
            }
            if first_line.starts_with("GET /artifacts/download/ticket-browser ") {
                if download_attempt == 0 {
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        artifact_bytes.len()
                    )
                    .into_bytes();
                    stream
                        .write_all(&response)
                        .expect("write resumable partial headers");
                    stream
                        .write_all(&artifact_bytes[..4])
                        .expect("write resumable partial body");
                    stream.flush().expect("flush resumable partial body");
                    download_attempt += 1;
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    continue;
                }
                assert_eq!(
                    range_header.as_deref(),
                    Some("bytes=4-"),
                    "second resumable edge download should resume from the stored prefix",
                );
                let body = &artifact_bytes[4..];
                let mut response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nContent-Range: bytes 4-{}/{}\r\nConnection: close\r\n\r\n",
                    body.len(),
                    artifact_bytes.len() - 1,
                    artifact_bytes.len()
                )
                .into_bytes();
                response.extend_from_slice(body);
                stream
                    .write_all(&response)
                    .expect("write resumable resumed response");
                stream.flush().expect("flush resumable resumed response");
                download_attempt += 1;
                continue;
            }

            stream
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .expect("write resumable artifact 404");
            stream.flush().expect("flush resumable artifact 404");
        }
    });

    (base_url, seen_ranges, handle)
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_head_artifact_view_server(
    head_view: burn_p2p_publish::HeadArtifactView,
) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind head artifact listener");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("head artifact listener addr")
    );
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept head artifact request");
        stream
            .set_read_timeout(Some(StdDuration::from_secs(1)))
            .expect("set head artifact read timeout");
        stream
            .set_write_timeout(Some(StdDuration::from_secs(1)))
            .expect("set head artifact write timeout");

        let mut request = Vec::new();
        loop {
            let mut chunk = [0u8; 1024];
            let read = stream.read(&mut chunk).expect("read head artifact request");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        let request = String::from_utf8_lossy(&request);
        let first_line = request.lines().next().unwrap_or_default().to_owned();
        let response = if first_line.starts_with("GET /artifacts/heads/head-browser ") {
            http_ok_json_response(&head_view)
        } else {
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
        };
        stream
            .write_all(&response)
            .expect("write head artifact response");
        stream.flush().expect("flush head artifact response");
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
    let client = BrowserEdgeClient::new(
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
fn browser_edge_client_trusted_callback_header_completes_provider_login() {
    let (base_url, handle) = spawn_callback_capture_server();
    let client = BrowserEdgeClient::new(
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
    )
    .with_trusted_callback("x-burn-p2p-canary-token", "trusted-token");

    let session = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .complete_provider_login(
                    &LoginStart {
                        login_id: ContentId::new("login-browser"),
                        provider: AuthProvider::GitHub,
                        state: "state-browser".into(),
                        authorize_url: None,
                        expires_at: Utc::now() + chrono::Duration::minutes(5),
                    },
                    "provider-code",
                )
                .await
                .expect("trusted callback provider login")
        });

    let trusted_header = handle.join().expect("join callback server");
    assert_eq!(trusted_header, "trusted-token");
    assert_eq!(session.claims.principal_id, PrincipalId::new("principal-browser"));
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
    let client = BrowserEdgeClient::new(
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

#[test]
fn resolve_browser_seed_bootstrap_prefers_signed_edge_and_merges_site_fallback() {
    let signed = SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_seed_advertisement",
            Version::new(0, 1, 0),
            BrowserSeedAdvertisement {
                schema_version: u32::from(burn_p2p_core::SCHEMA_VERSION),
                network_id: NetworkId::new("net-browser"),
                issued_at: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::minutes(10),
                transport_policy: BrowserSeedTransportPolicy {
                    preferred: vec![
                        BrowserSeedTransportKind::WebRtcDirect,
                        BrowserSeedTransportKind::WssFallback,
                    ],
                    allow_fallback_wss: true,
                },
                seeds: vec![BrowserSeedRecord {
                    peer_id: Some(PeerId::new("seed-browser")),
                    multiaddrs: vec![
                        TEST_EDGE_WEBRTC_DIRECT_SEED.into(),
                        "/dns4/edge.example/tcp/443/wss".into(),
                    ],
                }],
            },
        ),
        SignatureMetadata {
            signer: PeerId::new("bootstrap"),
            key_id: "browser-seeds".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "deadbeef".into(),
        },
    )
    .expect("signed browser seed advertisement");

    let resolved = resolve_browser_seed_bootstrap(
        &NetworkId::new("net-browser"),
        Some(&signed),
        &[
            "/dns4/edge.example/tcp/443/wss".into(),
            "/dns4/site.example/tcp/443/wss".into(),
        ],
        Utc::now(),
    );

    assert_eq!(resolved.source, BrowserSeedBootstrapSource::Merged);
    assert_eq!(resolved.advertised_seed_count, 2);
    assert_eq!(
        resolved.seed_node_urls,
        vec![
            TEST_EDGE_WEBRTC_DIRECT_SEED.to_owned(),
            "/dns4/edge.example/tcp/443/wss".to_owned(),
            "/dns4/site.example/tcp/443/wss".to_owned(),
        ]
    );
    assert_eq!(resolved.last_error, None);
}

#[test]
fn resolve_browser_seed_bootstrap_falls_back_to_site_config_when_edge_payload_is_invalid() {
    let signed = SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_seed_advertisement",
            Version::new(0, 1, 0),
            BrowserSeedAdvertisement {
                schema_version: u32::from(burn_p2p_core::SCHEMA_VERSION),
                network_id: NetworkId::new("wrong-network"),
                issued_at: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::minutes(10),
                transport_policy: BrowserSeedTransportPolicy {
                    preferred: vec![BrowserSeedTransportKind::WebTransport],
                    allow_fallback_wss: true,
                },
                seeds: vec![BrowserSeedRecord {
                    peer_id: None,
                    multiaddrs: vec![TEST_EDGE_WEBTRANSPORT_SEED.into()],
                }],
            },
        ),
        SignatureMetadata {
            signer: PeerId::new("bootstrap"),
            key_id: "browser-seeds".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "deadbeef".into(),
        },
    )
    .expect("signed browser seed advertisement");

    let resolved = resolve_browser_seed_bootstrap(
        &NetworkId::new("net-browser"),
        Some(&signed),
        &["/dns4/site.example/tcp/443/wss".into()],
        Utc::now(),
    );

    assert_eq!(
        resolved.source,
        BrowserSeedBootstrapSource::SiteConfigFallback
    );
    assert_eq!(
        resolved.seed_node_urls,
        vec!["/dns4/site.example/tcp/443/wss".to_owned()]
    );
    assert!(
        resolved
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("did not match expected"))
    );
}

#[test]
fn browser_swarm_status_reports_selected_transport_without_fake_connection() {
    let mut config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    config.seed_bootstrap.source = BrowserSeedBootstrapSource::EdgeSigned;
    config.seed_bootstrap.seed_node_urls = vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()];
    let runtime = BrowserWorkerRuntime::start(
        config,
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::enabled(true, true, true)
            .selecting(BrowserTransportKind::WebRtcDirect),
    );

    let status = runtime.swarm_status();
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
fn browser_swarm_status_reports_peer_artifact_ready_from_truthful_runtime_state() {
    let mut config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    config.seed_bootstrap.source = BrowserSeedBootstrapSource::Merged;
    config.seed_bootstrap.seed_node_urls = vec![
        TEST_EDGE_WEBRTC_DIRECT_SEED.into(),
        "/dns4/edge.example/tcp/443/wss".into(),
    ];
    let mut runtime = BrowserWorkerRuntime::start(
        config,
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::enabled(true, true, true).connected_via(
            BrowserTransportKind::WebRtcDirect,
            vec![PeerId::new("peer-browser-1")],
        ),
    );
    runtime.state = Some(BrowserRuntimeState::Trainer);
    runtime.storage.remember_head(HeadId::new("head-browser"));
    runtime.storage.remember_synced_head_artifact(
        HeadId::new("head-browser"),
        ArtifactId::new("artifact-browser"),
        "peer-native",
    );

    let status = runtime.swarm_status();
    assert_eq!(status.phase, BrowserSwarmPhase::ArtifactReady);
    assert_eq!(
        status.transport_source,
        BrowserTransportObservationSource::Connected
    );
    assert_eq!(
        status.connected_transport,
        Some(BrowserTransportFamily::WebRtcDirect)
    );
    assert_eq!(status.connected_peer_count, 1);
    assert_eq!(status.artifact_source, BrowserArtifactSource::PeerSwarm);
    assert!(status.head_synced);
}

#[test]
fn browser_swarm_status_treats_swarm_directory_cache_as_live_state() {
    let mut config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    config.seed_bootstrap.source = BrowserSeedBootstrapSource::Merged;
    config.seed_bootstrap.seed_node_urls = vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()];
    let mut runtime = BrowserWorkerRuntime::start(
        config,
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::enabled(true, true, true).connected_via(
            BrowserTransportKind::WebRtcDirect,
            vec![PeerId::new("peer-browser-1")],
        ),
    );
    runtime.state = Some(BrowserRuntimeState::Observer);
    runtime
        .storage
        .remember_swarm_directory_snapshot(browser_directory_snapshot(vec![
            browser_directory_entry_for_assignment("exp-browser", "rev-browser", "dataset-browser"),
        ]));

    let status = runtime.swarm_status();
    assert!(status.directory_synced);
    assert_eq!(status.phase, BrowserSwarmPhase::DirectorySynced);
    assert_eq!(
        status.connected_transport,
        Some(BrowserTransportFamily::WebRtcDirect)
    );
}

#[test]
fn worker_runtime_apply_swarm_status_updates_truthful_peer_state() {
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
            site_seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );
    runtime.state = Some(BrowserRuntimeState::Joining {
        role: BrowserRuntimeRole::BrowserObserver,
        stage: BrowserJoinStage::TransportConnect,
    });

    let events = runtime.apply_command(
        BrowserWorkerCommand::ApplySwarmStatus(Box::new(BrowserSwarmStatus {
            phase: BrowserSwarmPhase::TransportConnected,
            seed_bootstrap: BrowserResolvedSeedBootstrap {
                source: BrowserSeedBootstrapSource::EdgeSigned,
                seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
                advertised_seed_count: 1,
                last_error: None,
            },
            transport_source: BrowserTransportObservationSource::Connected,
            desired_transport: Some(BrowserTransportFamily::WebRtcDirect),
            connected_transport: Some(BrowserTransportFamily::WebRtcDirect),
            connected_peer_count: 1,
            connected_peer_ids: vec![PeerId::new("peer-browser-1")],
            directory_synced: false,
            assignment_bound: false,
            head_synced: false,
            artifact_source: BrowserArtifactSource::Unavailable,
            last_error: None,
        })),
        None,
        None,
    );

    assert_eq!(runtime.state, Some(BrowserRuntimeState::Observer));
    assert_eq!(
        runtime.transport.connected,
        Some(BrowserTransportKind::WebRtcDirect)
    );
    assert_eq!(
        runtime.transport.connected_peer_ids,
        vec![PeerId::new("peer-browser-1")]
    );
    assert!(events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::RuntimeStateChanged(BrowserRuntimeState::Observer)
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::TransportChanged(transport)
            if transport.connected == Some(BrowserTransportKind::WebRtcDirect)
                && transport.connected_peer_ids == vec![PeerId::new("peer-browser-1")]
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::SwarmStatusChanged(status)
            if status.connected_transport == Some(BrowserTransportFamily::WebRtcDirect)
                && status.connected_peer_ids == vec![PeerId::new("peer-browser-1")]
    )));
}

#[test]
fn worker_runtime_apply_swarm_directory_head_and_metrics_commands_drive_runtime_state() {
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
            site_seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );
    let session = sample_browser_session_state("principal-browser");
    runtime.remember_session(session.clone());

    let directory_events = runtime.apply_command(
        BrowserWorkerCommand::ApplySwarmDirectory(Box::new(
            signed_browser_directory_snapshot(vec![browser_directory_entry_for_assignment(
                "exp-browser",
                "rev-browser",
                "view-browser",
            )])
            .payload
            .payload,
        )),
        None,
        Some(&session),
    );
    assert!(directory_events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::DirectoryUpdated { network_id, visible_entries }
            if network_id == &NetworkId::new("net-browser") && *visible_entries == 1
    )));
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

    let head_events = runtime.apply_command(
        BrowserWorkerCommand::ApplySwarmHeads(vec![burn_p2p::HeadDescriptor {
            head_id: HeadId::new("head-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            parent_head_id: None,
            global_step: 1,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        }]),
        None,
        None,
    );
    assert!(head_events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::HeadUpdated { head_id } if head_id == &HeadId::new("head-browser")
    )));

    let metrics_events = runtime.apply_command(
        BrowserWorkerCommand::ApplySwarmMetricsSync(Box::new(BrowserMetricsSyncState {
            catchup_bundles: Vec::new(),
            live_event: Some(MetricsLiveEvent {
                network_id: NetworkId::new("net-browser"),
                kind: MetricsLiveEventKind::CatchupRefresh,
                cursors: vec![MetricsSyncCursor {
                    experiment_id: ExperimentId::new("exp-browser"),
                    revision_id: RevisionId::new("rev-browser"),
                    latest_snapshot_seq: Some(1),
                    latest_ledger_segment_seq: Some(1),
                    latest_head_id: Some(HeadId::new("head-browser")),
                    latest_merge_window_id: None,
                }],
                generated_at: Utc::now(),
            }),
        })),
        None,
        None,
    );
    assert!(
        metrics_events
            .iter()
            .any(|event| matches!(event, BrowserWorkerEvent::MetricsUpdated(_)))
    );
    assert_eq!(
        runtime.storage.last_head_id,
        Some(HeadId::new("head-browser"))
    );
}

#[test]
fn worker_runtime_promotes_head_sync_once_direct_transport_arrives() {
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
            site_seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
            ..config
        },
        BrowserCapabilityReport::default(),
        BrowserTransportStatus::default(),
    );
    runtime.state = Some(BrowserRuntimeState::Joining {
        role: BrowserRuntimeRole::BrowserTrainerWgpu,
        stage: BrowserJoinStage::HeadSync,
    });
    runtime.storage.remember_head(HeadId::new("head-browser-1"));

    let events = runtime.apply_command(
        BrowserWorkerCommand::ApplySwarmStatus(Box::new(BrowserSwarmStatus {
            phase: BrowserSwarmPhase::TransportConnected,
            seed_bootstrap: BrowserResolvedSeedBootstrap {
                source: BrowserSeedBootstrapSource::EdgeSigned,
                seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
                advertised_seed_count: 1,
                last_error: None,
            },
            transport_source: BrowserTransportObservationSource::Connected,
            desired_transport: Some(BrowserTransportFamily::WebRtcDirect),
            connected_transport: Some(BrowserTransportFamily::WebRtcDirect),
            connected_peer_count: 1,
            connected_peer_ids: vec![PeerId::new("peer-browser-1")],
            directory_synced: true,
            assignment_bound: true,
            head_synced: true,
            artifact_source: BrowserArtifactSource::Unavailable,
            last_error: None,
        })),
        None,
        None,
    );

    assert_eq!(runtime.state, Some(BrowserRuntimeState::Trainer));
    assert!(events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::RuntimeStateChanged(BrowserRuntimeState::Trainer)
    )));
}

#[test]
fn browser_runtime_config_materializes_swarm_bootstrap_contract() {
    let mut config = BrowserRuntimeConfig::new(
        "https://edge.example",
        NetworkId::new("net-browser"),
        ContentId::new("train-browser"),
        "browser-wasm",
        ContentId::new("artifact-browser"),
    );
    config.seed_bootstrap.source = BrowserSeedBootstrapSource::Merged;
    config.seed_bootstrap.seed_node_urls = vec![
        TEST_BOOTSTRAP_WEBRTC_DIRECT_SEED.into(),
        "/dns4/bootstrap.example/tcp/443/wss".into(),
    ];
    config.selected_experiment = Some(ExperimentId::new("exp-browser"));
    config.selected_revision = Some(RevisionId::new("rev-browser"));

    let bootstrap: BrowserSwarmBootstrap = config.swarm_bootstrap();
    assert_eq!(bootstrap.network_id.as_str(), "net-browser");
    assert_eq!(
        bootstrap.seed_bootstrap.source,
        BrowserSeedBootstrapSource::Merged
    );
    assert_eq!(bootstrap.seed_bootstrap.seed_node_urls.len(), 2);
    assert_eq!(
        bootstrap.transport_preference,
        vec![
            BrowserTransportFamily::WebRtcDirect,
            BrowserTransportFamily::WebTransport,
            BrowserTransportFamily::WssFallback,
        ]
    );
    assert_eq!(
        bootstrap
            .selected_experiment
            .expect("selected experiment")
            .as_str(),
        "exp-browser"
    );
    assert_eq!(
        bootstrap
            .selected_revision
            .expect("selected revision")
            .as_str(),
        "rev-browser"
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

fn sample_training_lease() -> WorkloadTrainingLease {
    WorkloadTrainingLease {
        lease_id: LeaseId::new("lease-browser"),
        window_id: WindowId(7),
        dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
        assignment_hash: ContentId::new("assignment-browser"),
        microshards: vec![
            burn_p2p::MicroShardId::new("micro-browser-a"),
            burn_p2p::MicroShardId::new("micro-browser-b"),
        ],
    }
}

fn browser_directory_entry_for_assignment(
    experiment_id: &str,
    revision_id: &str,
    dataset_view_id: &str,
) -> ExperimentDirectoryEntry {
    ExperimentDirectoryEntry {
        network_id: NetworkId::new("net-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new(experiment_id),
        workload_id: WorkloadId::new("browser-demo"),
        display_name: "Browser Demo".into(),
        model_schema_hash: ContentId::new("model-browser"),
        dataset_view_id: burn_p2p::DatasetViewId::new(dataset_view_id),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 1024,
            estimated_window_seconds: 15,
        },
        visibility: ExperimentVisibility::Public,
        opt_in_policy: ExperimentOptInPolicy::Open,
        current_revision_id: RevisionId::new(revision_id),
        current_head_id: Some(HeadId::new("head-browser")),
        allowed_roles: PeerRoleSet::default(),
        allowed_scopes: BTreeSet::from([ExperimentScope::Connect]),
        metadata: Default::default(),
    }
}

fn trainer_runtime_with_assignment(
    experiment_id: &str,
    revision_id: &str,
    dataset_view_id: &str,
) -> BrowserWorkerRuntime {
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
    runtime.remember_session(sample_browser_session_state("principal-browser"));
    runtime
        .storage
        .remember_directory_snapshot(signed_browser_directory_snapshot(vec![
            browser_directory_entry_for_assignment(experiment_id, revision_id, dataset_view_id),
        ]));
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new(experiment_id),
            revision_id: RevisionId::new(revision_id),
        });
    runtime.state = Some(BrowserRuntimeState::Trainer);
    runtime
}

fn signed_browser_directory_snapshot(
    entries: Vec<ExperimentDirectoryEntry>,
) -> SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_directory_snapshot",
            Version::new(0, 1, 0),
            browser_directory_snapshot(entries),
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

fn browser_directory_snapshot(entries: Vec<ExperimentDirectoryEntry>) -> BrowserDirectorySnapshot {
    BrowserDirectorySnapshot {
        network_id: NetworkId::new("net-browser"),
        generated_at: Utc::now(),
        entries,
    }
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
    let lease = sample_training_lease();
    runtime
        .storage
        .remember_active_training_lease(lease.clone());
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
    let peer_window_metrics = vec![PeerWindowMetrics {
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
    }];
    let performance_summary =
        derive_network_performance_summary(&peer_window_metrics, &Vec::new(), &Vec::new());
    let head_eval_reports = vec![HeadEvalReport {
        network_id: NetworkId::new("net-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        workload_id: WorkloadId::new("workload-browser"),
        head_id: HeadId::new("head-browser"),
        base_head_id: Some(HeadId::new("head-parent")),
        eval_protocol_id: ContentId::new("eval-browser"),
        evaluator_set_id: ContentId::new("eval-set-browser"),
        metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.25))]),
        sample_count: 128,
        dataset_view_id: burn_p2p::DatasetViewId::new("view-browser"),
        started_at: Utc::now() - chrono::Duration::seconds(20),
        finished_at: Utc::now() - chrono::Duration::seconds(15),
        trust_class: burn_p2p::MetricTrustClass::Canonical,
        status: burn_p2p::HeadEvalStatus::Completed,
        signature_bundle: Vec::new(),
    }];
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
                head_eval_reports,
                peer_window_metrics,
                reducer_cohort_metrics: Vec::new(),
                derived_metrics: Vec::new(),
                performance_summary,
            },
            ledger_segments: Vec::new(),
            generated_at: Utc::now(),
        }]);

    let model = BrowserAppModel::from_runtime(runtime);
    let view = model.view(&BrowserUiBindings::new("https://edge.example"));

    assert_eq!(
        view.default_surface,
        burn_p2p_views::BrowserAppSurface::Train
    );
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
        view.network
            .diffusion
            .as_ref()
            .map(|diffusion| diffusion.canonical_head_id.as_str()),
        Some("head-browser")
    );
    assert_eq!(
        view.training.active_assignment.as_deref(),
        Some("exp-browser/rev-browser")
    );
    assert_eq!(
        view.training
            .active_training_lease
            .as_ref()
            .map(|lease| lease.lease_id.as_str()),
        Some("lease-browser")
    );
    assert_eq!(
        view.training
            .active_training_lease
            .as_ref()
            .map(|lease| lease.window_id),
        Some(7)
    );
    assert_eq!(
        view.training
            .active_training_lease
            .as_ref()
            .map(|lease| lease.dataset_view_id.as_str()),
        Some("view-browser")
    );
    assert_eq!(
        view.training
            .active_training_lease
            .as_ref()
            .map(|lease| lease.assignment_hash.as_str()),
        Some("assignment-browser")
    );
    assert_eq!(
        view.training
            .active_training_lease
            .as_ref()
            .map(|lease| lease.microshard_count),
        Some(2)
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
    assert_eq!(view.training.slice_target_samples, Some(200));
    assert_eq!(view.training.slice_remaining_samples, Some(20));
    assert_eq!(
        view.training.slice_status.as_str(),
        "1 microshard cached · 20 left in slice"
    );
    assert_eq!(view.training.last_loss.as_deref(), Some("0.3000"));
    assert_eq!(view.training.publish_latency_ms, Some(250));
    assert_eq!(
        view.training.throughput_summary.as_deref(),
        Some("18.0 sample/s")
    );
    assert_eq!(view.network.transport, "dialing webtransport");
    assert!(view.network.metrics_live_ready);
    assert_eq!(
        view.network
            .performance
            .as_ref()
            .map(|performance| performance.training_throughput.as_str()),
        Some("17.6 work/s")
    );
    assert_eq!(
        view.network
            .performance
            .as_ref()
            .map(|performance| performance.idle_time.as_str()),
        Some("0 ms")
    );
    assert!(!view.viewer.experiments_preview.is_empty());
    assert!(!view.viewer.leaderboard_preview.is_empty());
    assert_eq!(model.active_training_lease(), Some(&lease));
}

#[test]
fn worker_runtime_training_with_lease_persists_active_training_lease() {
    let mut runtime = trainer_runtime_with_assignment("exp-browser", "rev-browser", "view-browser");
    let lease = sample_training_lease();

    let events = runtime.apply_command(
        BrowserWorkerCommand::Train(BrowserTrainingPlan {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            workload_id: WorkloadId::new("browser-demo"),
            budget: BrowserTrainingBudget::default(),
            lease: Some(lease.clone()),
        }),
        None,
        None,
    );

    assert_eq!(runtime.storage.active_training_lease, Some(lease.clone()));
    assert!(events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::StorageUpdated(storage)
            if storage.active_training_lease == Some(lease.clone())
    )));
    assert!(
        events
            .iter()
            .any(|event| matches!(event, BrowserWorkerEvent::TrainingCompleted(_)))
    );
}

#[test]
fn worker_runtime_training_without_lease_clears_previous_active_training_lease() {
    let mut runtime = trainer_runtime_with_assignment("exp-browser", "rev-browser", "view-browser");
    runtime
        .storage
        .remember_active_training_lease(sample_training_lease());

    let events = runtime.apply_command(
        BrowserWorkerCommand::Train(BrowserTrainingPlan {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            workload_id: WorkloadId::new("browser-demo"),
            budget: BrowserTrainingBudget::default(),
            lease: None,
        }),
        None,
        None,
    );

    assert_eq!(runtime.storage.active_training_lease, None);
    assert!(events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::StorageUpdated(storage) if storage.active_training_lease.is_none()
    )));
}

#[test]
fn worker_runtime_selecting_new_experiment_clears_active_training_lease() {
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
    runtime
        .storage
        .remember_active_training_lease(sample_training_lease());

    runtime.select_experiment(
        ExperimentId::new("exp-other"),
        Some(RevisionId::new("rev-other")),
        None,
        None,
    );

    assert_eq!(runtime.storage.active_training_lease, None);
}

#[test]
fn browser_storage_clears_active_training_lease_when_session_changes() {
    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_session(sample_browser_session_state("principal-browser"));
    storage.remember_active_training_lease(sample_training_lease());

    storage.remember_session(BrowserSessionState::default());
    assert_eq!(storage.active_training_lease, None);

    storage.remember_session(sample_browser_session_state("principal-browser"));
    storage.remember_active_training_lease(sample_training_lease());
    storage.remember_session(sample_browser_session_state("principal-other"));
    assert_eq!(storage.active_training_lease, None);
}

#[test]
fn browser_conformance_harness_exposes_persisted_active_training_lease() {
    let revision = conformance_revision_manifest();
    let entry = ExperimentDirectoryProjectionBuilder::from_revision(
        NetworkId::new("net-browser"),
        StudyId::new("study-browser"),
        "Browser Demo",
        &revision,
    )
    .with_visibility(ExperimentVisibility::Public)
    .with_opt_in_policy(ExperimentOptInPolicy::Open)
    .with_scope(ExperimentScope::Connect)
    .with_scope(ExperimentScope::Train {
        experiment_id: revision.experiment_id.clone(),
    })
    .build();
    let directory =
        browser_conformance_directory(NetworkId::new("net-browser"), vec![entry.clone()]);
    let session = browser_conformance_session(
        NetworkId::new("net-browser"),
        PrincipalId::new("browser-principal"),
        BTreeSet::from([
            ExperimentScope::Connect,
            ExperimentScope::Train {
                experiment_id: revision.experiment_id.clone(),
            },
        ]),
    );
    let mut harness = BrowserConformanceHarness::start(
        BrowserRuntimeConfig {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            site_seed_node_urls: vec!["/dns4/edge.example/tcp/443/wss".into()],
            ..BrowserRuntimeConfig::new(
                "https://edge.example",
                NetworkId::new("net-browser"),
                ContentId::new("train-browser"),
                "browser-wasm",
                ContentId::new("artifact-browser"),
            )
        },
        browser_conformance_capability_for_role(BrowserRuntimeRole::BrowserTrainerWgpu),
        browser_conformance_transport(),
        directory,
        session,
    );
    harness.apply_heads(&[burn_p2p::HeadDescriptor {
        head_id: HeadId::new("head-browser"),
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
        artifact_id: ArtifactId::new("artifact-browser-head"),
        parent_head_id: None,
        global_step: 1,
        created_at: Utc::now(),
        metrics: BTreeMap::new(),
    }]);

    let lease = sample_training_lease();
    harness
        .run_training(browser_conformance_training_plan_with_lease(
            StudyId::new("study-browser"),
            ExperimentId::new("exp-browser"),
            RevisionId::new("rev-browser"),
            WorkloadId::new("browser-demo"),
            lease.clone(),
        ))
        .expect("training result");

    assert_eq!(harness.active_training_lease(), Some(&lease));
}

#[test]
fn browser_app_controller_exposes_persisted_active_training_lease() {
    let snapshot = BrowserEdgeSnapshot {
        network_id: NetworkId::new("net-browser"),
        edge_mode: BrowserEdgeMode::Peer,
        browser_mode: burn_p2p::BrowserMode::Trainer,
        social_mode: burn_p2p::SocialMode::Public,
        profile_mode: burn_p2p::ProfileMode::Public,
        transports: BrowserTransportSurface {
            webrtc_direct: true,
            webtransport_gateway: true,
            wss_fallback: true,
        },
        paths: BrowserEdgePaths::default(),
        auth_enabled: false,
        login_providers: Vec::new(),
        required_release_train_hash: Some(ContentId::new("train-browser")),
        allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact-browser")]),
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
        trust_bundle: None,
        captured_at: Utc::now(),
    };
    let mut runtime = BrowserWorkerRuntime::default();
    runtime
        .storage
        .remember_active_training_lease(sample_training_lease());
    let controller = BrowserAppController::for_tests(
        BrowserEdgeClient::new(
            BrowserUiBindings::new("https://edge.example"),
            BrowserEnrollmentConfig::for_runtime_sync(&snapshot),
        ),
        BrowserAppModel::from_runtime(runtime),
    );

    assert_eq!(
        controller
            .active_training_lease()
            .map(|lease| lease.lease_id.as_str()),
        Some("lease-browser")
    );
}

#[test]
fn browser_app_model_surfaces_current_directory_revision_when_assignment_is_stale() {
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
            gpu_support: BrowserGpuSupport::Available,
            recommended_role: BrowserRuntimeRole::BrowserTrainerWgpu,
            ..BrowserCapabilityReport::default()
        }),
        ..BrowserWorkerRuntime::default()
    };
    let next_revision = burn_p2p::RevisionManifest {
        revision_id: RevisionId::new("rev-next"),
        ..conformance_revision_manifest()
    };
    let entry = ExperimentDirectoryProjectionBuilder::from_revision(
        NetworkId::new("net-browser"),
        StudyId::new("study-browser"),
        "Browser Demo",
        &next_revision,
    )
    .with_visibility(ExperimentVisibility::Public)
    .with_opt_in_policy(ExperimentOptInPolicy::Open)
    .with_scope(ExperimentScope::Connect)
    .with_scope(ExperimentScope::Train {
        experiment_id: next_revision.experiment_id.clone(),
    })
    .build();
    runtime
        .storage
        .remember_directory_snapshot(signed_browser_directory_snapshot(vec![entry]));
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });

    let model = BrowserAppModel::from_runtime(runtime);
    let view = model.view(&BrowserUiBindings::new("https://edge.example"));

    assert_eq!(
        view.selected_experiment
            .as_ref()
            .map(|selected| selected.revision_id.as_str()),
        Some("rev-next")
    );
    assert_eq!(view.training.slice_status, "waiting for revision rev-next");
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
    model.apply_event(BrowserWorkerEvent::SwarmStatusChanged(Box::new(
        BrowserSwarmStatus {
            phase: BrowserSwarmPhase::TransportConnected,
            seed_bootstrap: BrowserResolvedSeedBootstrap {
                source: BrowserSeedBootstrapSource::EdgeSigned,
                seed_node_urls: vec![TEST_EDGE_WEBRTC_DIRECT_SEED.into()],
                advertised_seed_count: 1,
                last_error: None,
            },
            transport_source: BrowserTransportObservationSource::Connected,
            desired_transport: Some(BrowserTransportFamily::WebRtcDirect),
            connected_transport: Some(BrowserTransportFamily::WebRtcDirect),
            connected_peer_count: 1,
            connected_peer_ids: vec![PeerId::new("peer-browser-1")],
            directory_synced: true,
            assignment_bound: true,
            head_synced: true,
            artifact_source: BrowserArtifactSource::PeerSwarm,
            last_error: None,
        },
    )));
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
        burn_p2p_views::BrowserAppSurface::Validate
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
    assert_eq!(model.runtime.swarm_status().connected_peer_count, 1);
    assert_eq!(
        model.runtime.swarm_status().transport_source,
        BrowserTransportObservationSource::Connected
    );
}

#[test]
fn browser_direct_sync_only_falls_back_to_edge_without_live_transport_or_state() {
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
    assert!(should_fallback_to_edge_control_sync(&runtime));

    runtime.transport.selected = Some(BrowserTransportKind::WebRtcDirect);
    assert!(should_fallback_to_edge_control_sync(&runtime));

    runtime
        .storage
        .remember_swarm_directory_snapshot(browser_directory_snapshot(Vec::new()));
    runtime.storage.remember_head(HeadId::new("head-browser"));
    assert!(!should_fallback_to_edge_control_sync(&runtime));

    runtime.transport.connected = Some(BrowserTransportKind::WssFallback);
    runtime.transport.connected_peer_ids = vec![PeerId::new("peer-browser-bootstrap")];
    runtime.transport.last_error = Some("direct dial timeout".into());
    assert!(!should_fallback_to_edge_control_sync(&runtime));

    runtime.transport.connected = Some(BrowserTransportKind::WebRtcDirect);
    assert!(!should_fallback_to_edge_control_sync(&runtime));
}

#[test]
fn browser_assignment_dataset_view_uses_swarm_directory_when_signed_directory_is_missing() {
    let mut storage = BrowserStorageSnapshot::default();
    storage.remember_assignment(BrowserStoredAssignment {
        study_id: StudyId::new("study-browser"),
        experiment_id: ExperimentId::new("exp-browser"),
        revision_id: RevisionId::new("rev-browser"),
    });
    storage.remember_swarm_directory_snapshot(browser_directory_snapshot(vec![
        browser_directory_entry_for_assignment("exp-browser", "rev-browser", "dataset-browser"),
    ]));

    assert_eq!(
        storage
            .active_assignment_dataset_view_id()
            .map(|dataset_view_id| dataset_view_id.as_str()),
        Some("dataset-browser")
    );
}

#[test]
fn browser_direct_sync_waits_for_swarm_bootstrap_before_edge_fallback() {
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
    runtime.state = Some(BrowserRuntimeState::Trainer);

    let waiting_status = BrowserSwarmStatus {
        phase: BrowserSwarmPhase::TransportSelected,
        desired_transport: Some(BrowserTransportFamily::WebRtcDirect),
        ..BrowserSwarmStatus::default()
    };
    assert!(should_wait_for_direct_swarm_bootstrap(
        &runtime,
        &waiting_status
    ));

    let connected_status = BrowserSwarmStatus {
        connected_transport: Some(BrowserTransportFamily::WssFallback),
        ..waiting_status.clone()
    };
    assert!(should_wait_for_direct_swarm_bootstrap(
        &runtime,
        &connected_status
    ));

    let direct_connected_status = BrowserSwarmStatus {
        connected_transport: Some(BrowserTransportFamily::WebRtcDirect),
        ..waiting_status.clone()
    };
    assert!(!should_wait_for_direct_swarm_bootstrap(
        &runtime,
        &direct_connected_status
    ));

    let failed_status = BrowserSwarmStatus {
        last_error: Some("direct dial timeout".into()),
        ..connected_status.clone()
    };
    assert!(!should_wait_for_direct_swarm_bootstrap(
        &runtime,
        &failed_status
    ));

    runtime
        .storage
        .remember_swarm_directory_snapshot(browser_directory_snapshot(Vec::new()));
    assert!(!should_wait_for_direct_swarm_bootstrap(
        &runtime,
        &waiting_status
    ));
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
    let client = BrowserEdgeClient::new(
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
fn browser_portal_client_syncs_active_head_artifact_into_worker_cache_once() {
    let published = PublishedArtifactRecord {
        published_artifact_id: PublishedArtifactId::new("published-browser"),
        artifact_alias_id: None,
        experiment_id: ExperimentId::new("exp-browser"),
        run_id: Some(RunId::new("run-browser")),
        head_id: HeadId::new("head-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        object_key: "browser/head-browser.snapshot".into(),
        content_hash: ContentId::new("content-browser"),
        content_length: 4,
        created_at: Utc::now(),
        expires_at: None,
        status: PublishedArtifactStatus::Ready,
    };
    let head_view = burn_p2p_publish::HeadArtifactView {
        head: burn_p2p::HeadDescriptor {
            head_id: HeadId::new("head-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            parent_head_id: Some(HeadId::new("head-parent")),
            global_step: 3,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        },
        run_id: RunId::new("run-browser"),
        eval_reports: Vec::new(),
        aliases: Vec::new(),
        published_artifacts: vec![published.clone()],
        available_profiles: BTreeSet::from([
            ArtifactProfile::BrowserSnapshot,
            ArtifactProfile::ManifestOnly,
        ]),
        alias_history: Vec::new(),
        provider_peer_ids: Vec::new(),
    };
    let download_ticket = burn_p2p_publish::DownloadTicketResponse {
        ticket: DownloadTicket {
            download_ticket_id: DownloadTicketId::new("ticket-browser"),
            published_artifact_id: published.published_artifact_id.clone(),
            principal_id: PrincipalId::new("principal-browser"),
            issued_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(5),
            delivery_mode: DownloadDeliveryMode::EdgeStream,
        },
        published_artifact: published,
        download_path: "/artifacts/download/ticket-browser".into(),
    };
    let (base_url, handle) =
        spawn_artifact_sync_server(head_view, download_ticket, b"demo".to_vec());
    let client = BrowserEdgeClient::new(
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
    runtime.storage.remember_head(HeadId::new("head-browser"));
    let session = sample_browser_session_state("principal-browser");

    let (first_events, second_events) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            let first = client
                .sync_active_head_artifact_into_worker(&mut runtime, Some(&session))
                .await
                .expect("first active head artifact sync");
            let second = client
                .sync_active_head_artifact_into_worker(&mut runtime, Some(&session))
                .await
                .expect("second active head artifact sync");
            (first, second)
        });

    handle.join().expect("join artifact thread");
    assert!(first_events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::StorageUpdated(storage)
            if storage
                .cached_chunk_artifacts
                .contains(&ArtifactId::new("artifact-browser"))
                && storage.last_head_artifact_transport.as_deref()
                    == Some("edge-download-ticket")
    )));
    assert!(second_events.is_empty());
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_defers_edge_fallback_while_direct_handoff_is_pending() {
    #[derive(Clone)]
    struct FailingPeerArtifactFetcher;

    impl BrowserPeerArtifactFetcher for FailingPeerArtifactFetcher {
        fn fetch(&self, _request: BrowserPeerArtifactRequest) -> BrowserPeerArtifactFetchFuture {
            Box::pin(async {
                Err(BrowserAuthClientError::ArtifactTransport(
                    "browser direct handoff is still pending".into(),
                ))
            })
        }
    }

    let published = PublishedArtifactRecord {
        published_artifact_id: PublishedArtifactId::new("published-browser"),
        artifact_alias_id: None,
        experiment_id: ExperimentId::new("exp-browser"),
        run_id: Some(RunId::new("run-browser")),
        head_id: HeadId::new("head-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        object_key: "browser/head-browser.snapshot".into(),
        content_hash: ContentId::new("content-browser"),
        content_length: 4,
        created_at: Utc::now(),
        expires_at: None,
        status: PublishedArtifactStatus::Ready,
    };
    let head_view = burn_p2p_publish::HeadArtifactView {
        head: burn_p2p::HeadDescriptor {
            head_id: HeadId::new("head-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            parent_head_id: Some(HeadId::new("head-parent")),
            global_step: 3,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        },
        run_id: RunId::new("run-browser"),
        eval_reports: Vec::new(),
        aliases: Vec::new(),
        published_artifacts: vec![published],
        available_profiles: BTreeSet::from([ArtifactProfile::BrowserSnapshot]),
        alias_history: Vec::new(),
        provider_peer_ids: Vec::new(),
    };
    let (base_url, handle) = spawn_head_artifact_view_server(head_view);
    let client = BrowserEdgeClient::new(
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
    )
    .with_peer_artifact_fetcher(FailingPeerArtifactFetcher);
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig::new(
            "https://edge.example",
            NetworkId::new("net-browser"),
            ContentId::new("train-browser"),
            "browser-wasm",
            ContentId::new("artifact-browser"),
        ),
        BrowserCapabilityReport::default(),
        BrowserTransportStatus {
            selected: Some(BrowserTransportKind::WebRtcDirect),
            connected: Some(BrowserTransportKind::WssFallback),
            connected_peer_ids: vec![PeerId::new("peer-browser-bootstrap")],
            last_error: Some("direct timeout while handoff is still pending".into()),
            ..BrowserTransportStatus::default()
        },
    );
    runtime.transport.selected = Some(BrowserTransportKind::WebRtcDirect);
    runtime.transport.active = Some(BrowserTransportKind::WebRtcDirect);
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });
    runtime.storage.remember_head(HeadId::new("head-browser"));
    let session = sample_browser_session_state("principal-browser");

    let events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .sync_active_head_artifact_into_worker(&mut runtime, Some(&session))
                .await
                .expect("pending direct handoff should defer edge artifact fallback")
        });

    handle.join().expect("join head artifact thread");
    let storage = events
        .iter()
        .rev()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert!(storage.cached_chunk_artifacts.is_empty());
    assert!(storage.cached_head_artifact_heads.is_empty());
    assert_eq!(storage.last_head_artifact_transport, None);
    assert!(storage.artifact_replay_checkpoint.is_some());
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_resumes_edge_download_after_partial_failure() {
    let published = PublishedArtifactRecord {
        published_artifact_id: PublishedArtifactId::new("published-browser"),
        artifact_alias_id: None,
        experiment_id: ExperimentId::new("exp-browser"),
        run_id: Some(RunId::new("run-browser")),
        head_id: HeadId::new("head-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        object_key: "browser/head-browser.snapshot".into(),
        content_hash: ContentId::new("content-browser"),
        content_length: 8,
        created_at: Utc::now(),
        expires_at: None,
        status: PublishedArtifactStatus::Ready,
    };
    let head_view = burn_p2p_publish::HeadArtifactView {
        head: burn_p2p::HeadDescriptor {
            head_id: HeadId::new("head-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            parent_head_id: Some(HeadId::new("head-parent")),
            global_step: 3,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        },
        run_id: RunId::new("run-browser"),
        eval_reports: Vec::new(),
        aliases: Vec::new(),
        published_artifacts: vec![published.clone()],
        available_profiles: BTreeSet::from([ArtifactProfile::BrowserSnapshot]),
        alias_history: Vec::new(),
        provider_peer_ids: Vec::new(),
    };
    let download_ticket = burn_p2p_publish::DownloadTicketResponse {
        ticket: DownloadTicket {
            download_ticket_id: DownloadTicketId::new("ticket-browser"),
            published_artifact_id: published.published_artifact_id.clone(),
            principal_id: PrincipalId::new("principal-browser"),
            issued_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(5),
            delivery_mode: DownloadDeliveryMode::EdgeStream,
        },
        published_artifact: published,
        download_path: "/artifacts/download/ticket-browser".into(),
    };
    let (base_url, seen_ranges, handle) =
        spawn_resumable_artifact_sync_server(head_view, download_ticket, b"demodata".to_vec());
    let client = BrowserEdgeClient::new(
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
    runtime.storage.remember_head(HeadId::new("head-browser"));
    let session = sample_browser_session_state("principal-browser");

    let (first_result, checkpoint_after_failure, second_events) =
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(async move {
                let first = client
                    .sync_active_head_artifact_into_worker(&mut runtime, Some(&session))
                    .await;
                let checkpoint = runtime.storage.artifact_replay_checkpoint.clone();
                let second = client
                    .sync_active_head_artifact_into_worker(&mut runtime, Some(&session))
                    .await
                    .expect("second sync should resume edge download");
                let checkpoint_cleared = runtime.storage.artifact_replay_checkpoint.clone();
                assert!(checkpoint_cleared.is_none());
                (first, checkpoint, second)
            });

    handle.join().expect("join resumable artifact thread");
    let error = first_result.expect_err("first resumable sync should fail mid-download");
    assert!(matches!(
        error,
        BrowserAuthClientError::Http(_) | BrowserAuthClientError::ArtifactTransport(_)
    ));
    let checkpoint = checkpoint_after_failure.expect("checkpoint should persist partial progress");
    let prefix = checkpoint
        .edge_download_prefix
        .expect("partial edge replay prefix should be preserved");
    assert_eq!(prefix.persisted_bytes, 4);
    assert_eq!(prefix.bytes, b"demo".to_vec());
    assert!(second_events.iter().any(|event| matches!(
        event,
        BrowserWorkerEvent::StorageUpdated(storage)
            if storage.last_head_artifact_transport.as_deref()
                == Some("edge-download-ticket")
    )));
    assert_eq!(
        seen_ranges
            .lock()
            .expect("seen ranges should not be poisoned")
            .as_slice(),
        ["bytes=4-"]
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_leaves_artifact_replay_checkpoint_on_sync_failure() {
    let published = PublishedArtifactRecord {
        published_artifact_id: PublishedArtifactId::new("published-browser"),
        artifact_alias_id: None,
        experiment_id: ExperimentId::new("exp-browser"),
        run_id: Some(RunId::new("run-browser")),
        head_id: HeadId::new("head-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        object_key: "browser/head-browser.snapshot".into(),
        content_hash: ContentId::new("content-browser"),
        content_length: 4,
        created_at: Utc::now(),
        expires_at: None,
        status: PublishedArtifactStatus::Ready,
    };
    let head_view = burn_p2p_publish::HeadArtifactView {
        head: burn_p2p::HeadDescriptor {
            head_id: HeadId::new("head-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            parent_head_id: Some(HeadId::new("head-parent")),
            global_step: 3,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        },
        run_id: RunId::new("run-browser"),
        eval_reports: Vec::new(),
        aliases: Vec::new(),
        published_artifacts: vec![published],
        available_profiles: BTreeSet::from([ArtifactProfile::BrowserSnapshot]),
        alias_history: Vec::new(),
        provider_peer_ids: vec![PeerId::new("peer-browser-provider")],
    };
    let (base_url, handle) = spawn_head_artifact_view_server(head_view);
    let client = BrowserEdgeClient::new(
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
    runtime.storage.remember_head(HeadId::new("head-browser"));
    let session = sample_browser_session_state("principal-browser");

    let (result, runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            let result = client
                .sync_active_head_artifact_into_worker(&mut runtime, Some(&session))
                .await;
            (result, runtime)
        });

    handle.join().expect("join head artifact thread");
    let error = result.expect_err("artifact sync should fail without download ticket routes");
    assert!(error.to_string().contains("artifact"));
    let checkpoint = runtime
        .storage
        .artifact_replay_checkpoint
        .expect("failed sync should persist replay checkpoint");
    assert_eq!(checkpoint.head_id.as_str(), "head-browser");
    assert_eq!(checkpoint.artifact_id.as_str(), "artifact-browser");
    assert_eq!(
        checkpoint.provider_peer_ids[0].as_str(),
        "peer-browser-provider"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_prefers_peer_native_head_artifact_fetcher_when_provider_peers_exist() {
    #[derive(Clone)]
    struct StaticPeerArtifactFetcher {
        requests: Arc<Mutex<Vec<BrowserPeerArtifactRequest>>>,
    }

    impl BrowserPeerArtifactFetcher for StaticPeerArtifactFetcher {
        fn fetch(&self, request: BrowserPeerArtifactRequest) -> BrowserPeerArtifactFetchFuture {
            self.requests
                .lock()
                .expect("peer artifact requests should not be poisoned")
                .push(request);
            Box::pin(async { Ok(b"peer-artifact".to_vec()) })
        }
    }

    let published = PublishedArtifactRecord {
        published_artifact_id: PublishedArtifactId::new("published-browser"),
        artifact_alias_id: None,
        experiment_id: ExperimentId::new("exp-browser"),
        run_id: Some(RunId::new("run-browser")),
        head_id: HeadId::new("head-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        object_key: "browser/head-browser.snapshot".into(),
        content_hash: ContentId::new("content-browser"),
        content_length: 4,
        created_at: Utc::now(),
        expires_at: None,
        status: PublishedArtifactStatus::Ready,
    };
    let head_view = burn_p2p_publish::HeadArtifactView {
        head: burn_p2p::HeadDescriptor {
            head_id: HeadId::new("head-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            parent_head_id: Some(HeadId::new("head-parent")),
            global_step: 3,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        },
        run_id: RunId::new("run-browser"),
        eval_reports: Vec::new(),
        aliases: Vec::new(),
        published_artifacts: vec![published],
        available_profiles: BTreeSet::from([ArtifactProfile::BrowserSnapshot]),
        alias_history: Vec::new(),
        provider_peer_ids: vec![PeerId::new("peer-browser-provider")],
    };
    let (base_url, handle) = spawn_head_artifact_view_server(head_view);
    let requests = Arc::new(Mutex::new(Vec::<BrowserPeerArtifactRequest>::new()));
    let client = BrowserEdgeClient::new(
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
    )
    .with_peer_artifact_fetcher(StaticPeerArtifactFetcher {
        requests: Arc::clone(&requests),
    });
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
    runtime.storage.remember_head(HeadId::new("head-browser"));
    let session = sample_browser_session_state("principal-browser");

    let events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .sync_active_head_artifact_into_worker(&mut runtime, Some(&session))
                .await
                .expect("peer-native active head artifact sync")
        });

    handle.join().expect("join head artifact thread");
    let requests = requests
        .lock()
        .expect("peer artifact requests should not be poisoned")
        .clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].artifact_id.as_str(), "artifact-browser");
    assert_eq!(
        requests[0].provider_peer_ids[0].as_str(),
        "peer-browser-provider"
    );
    let storage = events
        .iter()
        .rev()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert_eq!(
        storage.last_head_artifact_transport.as_deref(),
        Some("peer-native")
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_syncs_live_head_via_peer_transport_without_publication_record() {
    #[derive(Clone)]
    struct StaticPeerArtifactFetcher {
        requests: Arc<Mutex<Vec<BrowserPeerArtifactRequest>>>,
    }

    impl BrowserPeerArtifactFetcher for StaticPeerArtifactFetcher {
        fn fetch(&self, request: BrowserPeerArtifactRequest) -> BrowserPeerArtifactFetchFuture {
            self.requests
                .lock()
                .expect("peer artifact requests should not be poisoned")
                .push(request);
            Box::pin(async { Ok(b"peer-artifact".to_vec()) })
        }
    }

    let head_view = burn_p2p_publish::HeadArtifactView {
        head: burn_p2p::HeadDescriptor {
            head_id: HeadId::new("head-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            parent_head_id: Some(HeadId::new("head-parent")),
            global_step: 3,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        },
        run_id: RunId::new("run-browser"),
        eval_reports: Vec::new(),
        aliases: Vec::new(),
        published_artifacts: Vec::new(),
        available_profiles: BTreeSet::from([ArtifactProfile::BrowserSnapshot]),
        alias_history: Vec::new(),
        provider_peer_ids: vec![PeerId::new("peer-browser-provider")],
    };
    let (base_url, handle) = spawn_head_artifact_view_server(head_view);
    let requests = Arc::new(Mutex::new(Vec::<BrowserPeerArtifactRequest>::new()));
    let client = BrowserEdgeClient::new(
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
    )
    .with_peer_artifact_fetcher(StaticPeerArtifactFetcher {
        requests: Arc::clone(&requests),
    });
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
    runtime.storage.remember_head(HeadId::new("head-browser"));
    let session = sample_browser_session_state("principal-browser");

    let events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .sync_active_head_artifact_into_worker(&mut runtime, Some(&session))
                .await
                .expect("peer-native active head artifact sync without publication")
        });

    handle.join().expect("join head artifact thread");
    let requests = requests
        .lock()
        .expect("peer artifact requests should not be poisoned")
        .clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].artifact_id.as_str(), "artifact-browser");
    assert_eq!(
        requests[0].publication_target_id.as_str(),
        "peer-swarm-direct"
    );
    assert_eq!(
        requests[0].provider_peer_ids[0].as_str(),
        "peer-browser-provider"
    );
    let storage = events
        .iter()
        .rev()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert_eq!(
        storage.last_head_artifact_transport.as_deref(),
        Some("peer-native")
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_syncs_live_head_from_direct_snapshot_without_edge_view() {
    #[derive(Clone)]
    struct StaticPeerArtifactFetcher {
        requests: Arc<Mutex<Vec<BrowserPeerArtifactRequest>>>,
    }

    impl BrowserPeerArtifactFetcher for StaticPeerArtifactFetcher {
        fn fetch(&self, request: BrowserPeerArtifactRequest) -> BrowserPeerArtifactFetchFuture {
            self.requests
                .lock()
                .expect("peer artifact requests should not be poisoned")
                .push(request);
            Box::pin(async { Ok(b"peer-artifact".to_vec()) })
        }
    }

    let requests = Arc::new(Mutex::new(Vec::<BrowserPeerArtifactRequest>::new()));
    let client = BrowserEdgeClient::new(
        BrowserUiBindings::new("https://edge.example"),
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
    )
    .with_peer_artifact_fetcher(StaticPeerArtifactFetcher {
        requests: Arc::clone(&requests),
    });
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig::new(
            "https://edge.example",
            NetworkId::new("net-browser"),
            ContentId::new("train-browser"),
            "browser-wasm",
            ContentId::new("artifact-browser"),
        ),
        BrowserCapabilityReport::default(),
        BrowserTransportStatus {
            connected: Some(BrowserTransportKind::WebRtcDirect),
            connected_peer_ids: vec![PeerId::new("peer-browser-direct")],
            ..BrowserTransportStatus::default()
        },
    );
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });
    runtime.storage.remember_head(HeadId::new("head-browser"));
    let snapshot = burn_p2p::ControlPlaneSnapshot {
        head_announcements: vec![burn_p2p::HeadAnnouncement {
            overlay: burn_p2p::OverlayTopic::control(NetworkId::new("net-browser")),
            provider_peer_id: Some(PeerId::new("peer-browser-provider")),
            head: burn_p2p::HeadDescriptor {
                head_id: HeadId::new("head-browser"),
                study_id: StudyId::new("study-browser"),
                experiment_id: ExperimentId::new("exp-browser"),
                revision_id: RevisionId::new("rev-browser"),
                artifact_id: ArtifactId::new("artifact-browser"),
                parent_head_id: Some(HeadId::new("head-parent")),
                global_step: 3,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        }],
        ..burn_p2p::ControlPlaneSnapshot::default()
    };

    let events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .sync_active_head_artifact_from_control_snapshot_into_worker(
                    &mut runtime,
                    &snapshot,
                )
                .await
                .expect("direct snapshot head artifact sync")
        });

    let requests = requests
        .lock()
        .expect("peer artifact requests should not be poisoned")
        .clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].artifact_id.as_str(), "artifact-browser");
    assert_eq!(
        requests[0].provider_peer_ids,
        vec![PeerId::new("peer-browser-provider")]
    );
    let storage = events
        .iter()
        .rev()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert_eq!(
        storage.last_head_artifact_transport.as_deref(),
        Some("peer-native")
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_syncs_live_head_from_direct_snapshot_without_provider_hints() {
    #[derive(Clone)]
    struct StaticPeerArtifactFetcher {
        requests: Arc<Mutex<Vec<BrowserPeerArtifactRequest>>>,
    }

    impl BrowserPeerArtifactFetcher for StaticPeerArtifactFetcher {
        fn fetch(&self, request: BrowserPeerArtifactRequest) -> BrowserPeerArtifactFetchFuture {
            self.requests
                .lock()
                .expect("peer artifact requests should not be poisoned")
                .push(request);
            Box::pin(async { Ok(b"peer-artifact".to_vec()) })
        }
    }

    let requests = Arc::new(Mutex::new(Vec::<BrowserPeerArtifactRequest>::new()));
    let client = BrowserEdgeClient::new(
        BrowserUiBindings::new("https://edge.example"),
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
    )
    .with_peer_artifact_fetcher(StaticPeerArtifactFetcher {
        requests: Arc::clone(&requests),
    });
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig::new(
            "https://edge.example",
            NetworkId::new("net-browser"),
            ContentId::new("train-browser"),
            "browser-wasm",
            ContentId::new("artifact-browser"),
        ),
        BrowserCapabilityReport::default(),
        BrowserTransportStatus {
            connected: Some(BrowserTransportKind::WebRtcDirect),
            connected_peer_ids: vec![PeerId::new("peer-browser-direct")],
            ..BrowserTransportStatus::default()
        },
    );
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });
    runtime.storage.remember_head(HeadId::new("head-browser"));
    let snapshot = burn_p2p::ControlPlaneSnapshot {
        head_announcements: vec![burn_p2p::HeadAnnouncement {
            overlay: burn_p2p::OverlayTopic::control(NetworkId::new("net-browser")),
            provider_peer_id: None,
            head: burn_p2p::HeadDescriptor {
                head_id: HeadId::new("head-browser"),
                study_id: StudyId::new("study-browser"),
                experiment_id: ExperimentId::new("exp-browser"),
                revision_id: RevisionId::new("rev-browser"),
                artifact_id: ArtifactId::new("artifact-browser"),
                parent_head_id: Some(HeadId::new("head-parent")),
                global_step: 3,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        }],
        ..burn_p2p::ControlPlaneSnapshot::default()
    };

    let events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .sync_active_head_artifact_from_control_snapshot_into_worker(
                    &mut runtime,
                    &snapshot,
                )
                .await
                .expect("direct snapshot head artifact sync without provider hints")
        });

    let requests = requests
        .lock()
        .expect("peer artifact requests should not be poisoned")
        .clone();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].provider_peer_ids.is_empty());
    let storage = events
        .iter()
        .rev()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert_eq!(
        storage.last_head_artifact_transport.as_deref(),
        Some("peer-native")
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_syncs_live_head_from_direct_snapshot_without_provider_hints_over_wss() {
    #[derive(Clone)]
    struct StaticPeerArtifactFetcher {
        requests: Arc<Mutex<Vec<BrowserPeerArtifactRequest>>>,
    }

    impl BrowserPeerArtifactFetcher for StaticPeerArtifactFetcher {
        fn fetch(&self, request: BrowserPeerArtifactRequest) -> BrowserPeerArtifactFetchFuture {
            self.requests
                .lock()
                .expect("peer artifact requests should not be poisoned")
                .push(request);
            Box::pin(async { Ok(b"peer-artifact".to_vec()) })
        }
    }

    let requests = Arc::new(Mutex::new(Vec::<BrowserPeerArtifactRequest>::new()));
    let client = BrowserEdgeClient::new(
        BrowserUiBindings::new("https://edge.example"),
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
    )
    .with_peer_artifact_fetcher(StaticPeerArtifactFetcher {
        requests: Arc::clone(&requests),
    });
    let mut runtime = BrowserWorkerRuntime::start(
        BrowserRuntimeConfig::new(
            "https://edge.example",
            NetworkId::new("net-browser"),
            ContentId::new("train-browser"),
            "browser-wasm",
            ContentId::new("artifact-browser"),
        ),
        BrowserCapabilityReport::default(),
        BrowserTransportStatus {
            connected: Some(BrowserTransportKind::WssFallback),
            connected_peer_ids: vec![PeerId::new("peer-browser-bootstrap")],
            ..BrowserTransportStatus::default()
        },
    );
    runtime
        .storage
        .remember_assignment(BrowserStoredAssignment {
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
        });
    runtime.storage.remember_head(HeadId::new("head-browser"));
    let snapshot = burn_p2p::ControlPlaneSnapshot {
        head_announcements: vec![burn_p2p::HeadAnnouncement {
            overlay: burn_p2p::OverlayTopic::control(NetworkId::new("net-browser")),
            provider_peer_id: None,
            head: burn_p2p::HeadDescriptor {
                head_id: HeadId::new("head-browser"),
                study_id: StudyId::new("study-browser"),
                experiment_id: ExperimentId::new("exp-browser"),
                revision_id: RevisionId::new("rev-browser"),
                artifact_id: ArtifactId::new("artifact-browser"),
                parent_head_id: Some(HeadId::new("head-parent")),
                global_step: 3,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        }],
        ..burn_p2p::ControlPlaneSnapshot::default()
    };

    let events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .sync_active_head_artifact_from_control_snapshot_into_worker(
                    &mut runtime,
                    &snapshot,
                )
                .await
                .expect("wss direct snapshot head artifact sync without provider hints")
        });

    let requests = requests
        .lock()
        .expect("peer artifact requests should not be poisoned")
        .clone();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].provider_peer_ids.is_empty());
    let storage = events
        .iter()
        .rev()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert_eq!(
        storage.last_head_artifact_transport.as_deref(),
        Some("peer-native")
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_clears_stale_replay_state_before_syncing_superseded_revision() {
    let client = BrowserEdgeClient::new(
        BrowserUiBindings::new("https://edge.example"),
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
        .remember_directory_snapshot(signed_browser_directory_snapshot(vec![
            ExperimentDirectoryProjectionBuilder::from_revision(
                NetworkId::new("net-browser"),
                StudyId::new("study-browser"),
                "Browser Demo",
                &burn_p2p::RevisionManifest {
                    revision_id: RevisionId::new("rev-next"),
                    ..conformance_revision_manifest()
                },
            )
            .with_visibility(ExperimentVisibility::Public)
            .with_opt_in_policy(ExperimentOptInPolicy::Open)
            .with_scope(ExperimentScope::Connect)
            .build(),
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
        .remember_artifact_replay_checkpoint(BrowserArtifactReplayCheckpoint {
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            run_id: RunId::new("run-browser"),
            head_id: HeadId::new("head-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            artifact_profile: ArtifactProfile::BrowserSnapshot,
            publication_target_id: PublicationTargetId::new("browser-target"),
            publication_content_hash: Some(ContentId::new("content-browser-old")),
            publication_content_length: Some(8),
            provider_peer_ids: vec![PeerId::new("peer-browser-provider-b")],
            artifact_descriptor: None,
            completed_chunks: Vec::new(),
            edge_download_prefix: None,
            edge_download_segments: Vec::new(),
            completed_bytes: 0,
            last_attempted_at: Utc::now(),
            attempt_count: 1,
        });
    let session = sample_browser_session_state("principal-browser");

    let events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .sync_active_head_artifact_into_worker(&mut runtime, Some(&session))
                .await
                .expect("stale revision should be cleared without network fetch")
        });

    let storage = events
        .iter()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert_eq!(storage.last_head_id, None);
    assert!(storage.artifact_replay_checkpoint.is_none());
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_reuses_replay_checkpoint_provider_order_and_clears_it_on_success() {
    #[derive(Clone)]
    struct StaticPeerArtifactFetcher {
        requests: Arc<Mutex<Vec<BrowserPeerArtifactRequest>>>,
    }

    impl BrowserPeerArtifactFetcher for StaticPeerArtifactFetcher {
        fn fetch(&self, request: BrowserPeerArtifactRequest) -> BrowserPeerArtifactFetchFuture {
            self.requests
                .lock()
                .expect("peer artifact requests should not be poisoned")
                .push(request);
            Box::pin(async { Ok(b"peer-artifact".to_vec()) })
        }
    }

    let published = PublishedArtifactRecord {
        published_artifact_id: PublishedArtifactId::new("published-browser"),
        artifact_alias_id: None,
        experiment_id: ExperimentId::new("exp-browser"),
        run_id: Some(RunId::new("run-browser")),
        head_id: HeadId::new("head-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        object_key: "browser/head-browser.snapshot".into(),
        content_hash: ContentId::new("content-browser"),
        content_length: 4,
        created_at: Utc::now(),
        expires_at: None,
        status: PublishedArtifactStatus::Ready,
    };
    let head_view = burn_p2p_publish::HeadArtifactView {
        head: burn_p2p::HeadDescriptor {
            head_id: HeadId::new("head-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            parent_head_id: Some(HeadId::new("head-parent")),
            global_step: 3,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        },
        run_id: RunId::new("run-browser"),
        eval_reports: Vec::new(),
        aliases: Vec::new(),
        published_artifacts: vec![published],
        available_profiles: BTreeSet::from([ArtifactProfile::BrowserSnapshot]),
        alias_history: Vec::new(),
        provider_peer_ids: vec![
            PeerId::new("peer-browser-provider-a"),
            PeerId::new("peer-browser-provider-b"),
        ],
    };
    let (base_url, handle) = spawn_head_artifact_view_server(head_view);
    let requests = Arc::new(Mutex::new(Vec::<BrowserPeerArtifactRequest>::new()));
    let client = BrowserEdgeClient::new(
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
    )
    .with_peer_artifact_fetcher(StaticPeerArtifactFetcher {
        requests: Arc::clone(&requests),
    });
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
    runtime.storage.remember_head(HeadId::new("head-browser"));
    runtime
        .storage
        .remember_artifact_replay_checkpoint(BrowserArtifactReplayCheckpoint {
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            run_id: RunId::new("run-browser"),
            head_id: HeadId::new("head-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            artifact_profile: ArtifactProfile::BrowserSnapshot,
            publication_target_id: PublicationTargetId::new("browser-target"),
            publication_content_hash: Some(ContentId::new("content-browser")),
            publication_content_length: Some(4),
            provider_peer_ids: vec![PeerId::new("peer-browser-provider-b")],
            artifact_descriptor: None,
            completed_chunks: Vec::new(),
            edge_download_prefix: None,
            edge_download_segments: Vec::new(),
            completed_bytes: 0,
            last_attempted_at: Utc::now(),
            attempt_count: 1,
        });
    let session = sample_browser_session_state("principal-browser");

    let events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .sync_active_head_artifact_into_worker(&mut runtime, Some(&session))
                .await
                .expect("peer-native active head artifact sync")
        });

    handle.join().expect("join head artifact thread");
    let requests = requests
        .lock()
        .expect("peer artifact requests should not be poisoned")
        .clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].provider_peer_ids,
        vec![
            PeerId::new("peer-browser-provider-b"),
            PeerId::new("peer-browser-provider-a"),
        ]
    );
    let storage = events
        .iter()
        .rev()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert!(storage.artifact_replay_checkpoint.is_none());
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn browser_portal_client_does_not_reuse_checkpoint_when_publication_metadata_changes() {
    #[derive(Clone)]
    struct StaticPeerArtifactFetcher {
        requests: Arc<Mutex<Vec<BrowserPeerArtifactRequest>>>,
    }

    impl BrowserPeerArtifactFetcher for StaticPeerArtifactFetcher {
        fn fetch(&self, request: BrowserPeerArtifactRequest) -> BrowserPeerArtifactFetchFuture {
            self.requests
                .lock()
                .expect("peer artifact requests should not be poisoned")
                .push(request);
            Box::pin(async { Ok(b"peer-artifact".to_vec()) })
        }
    }

    let published = PublishedArtifactRecord {
        published_artifact_id: PublishedArtifactId::new("published-browser"),
        artifact_alias_id: None,
        experiment_id: ExperimentId::new("exp-browser"),
        run_id: Some(RunId::new("run-browser")),
        head_id: HeadId::new("head-browser"),
        artifact_profile: ArtifactProfile::BrowserSnapshot,
        publication_target_id: PublicationTargetId::new("browser-target"),
        object_key: "browser/head-browser.snapshot".into(),
        content_hash: ContentId::new("content-browser-new"),
        content_length: 4,
        created_at: Utc::now(),
        expires_at: None,
        status: PublishedArtifactStatus::Ready,
    };
    let head_view = burn_p2p_publish::HeadArtifactView {
        head: burn_p2p::HeadDescriptor {
            head_id: HeadId::new("head-browser"),
            study_id: StudyId::new("study-browser"),
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            parent_head_id: Some(HeadId::new("head-parent")),
            global_step: 3,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        },
        run_id: RunId::new("run-browser"),
        eval_reports: Vec::new(),
        aliases: Vec::new(),
        published_artifacts: vec![published],
        available_profiles: BTreeSet::from([ArtifactProfile::BrowserSnapshot]),
        alias_history: Vec::new(),
        provider_peer_ids: vec![
            PeerId::new("peer-browser-provider-a"),
            PeerId::new("peer-browser-provider-b"),
        ],
    };
    let (base_url, handle) = spawn_head_artifact_view_server(head_view);
    let requests = Arc::new(Mutex::new(Vec::<BrowserPeerArtifactRequest>::new()));
    let client = BrowserEdgeClient::new(
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
    )
    .with_peer_artifact_fetcher(StaticPeerArtifactFetcher {
        requests: Arc::clone(&requests),
    });
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
    runtime.storage.remember_head(HeadId::new("head-browser"));
    runtime
        .storage
        .remember_artifact_replay_checkpoint(BrowserArtifactReplayCheckpoint {
            experiment_id: ExperimentId::new("exp-browser"),
            revision_id: RevisionId::new("rev-browser"),
            run_id: RunId::new("run-browser"),
            head_id: HeadId::new("head-browser"),
            artifact_id: ArtifactId::new("artifact-browser"),
            artifact_profile: ArtifactProfile::BrowserSnapshot,
            publication_target_id: PublicationTargetId::new("browser-target"),
            publication_content_hash: Some(ContentId::new("content-browser-old")),
            publication_content_length: Some(8),
            provider_peer_ids: vec![PeerId::new("peer-browser-provider-b")],
            artifact_descriptor: None,
            completed_chunks: Vec::new(),
            edge_download_prefix: None,
            edge_download_segments: Vec::new(),
            completed_bytes: 0,
            last_attempted_at: Utc::now(),
            attempt_count: 1,
        });
    let session = sample_browser_session_state("principal-browser");

    let events = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            client
                .sync_active_head_artifact_into_worker(&mut runtime, Some(&session))
                .await
                .expect("peer-native active head artifact sync")
        });

    handle.join().expect("join head artifact thread");
    let requests = requests
        .lock()
        .expect("peer artifact requests should not be poisoned")
        .clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].provider_peer_ids,
        vec![
            PeerId::new("peer-browser-provider-a"),
            PeerId::new("peer-browser-provider-b"),
        ]
    );
    assert_eq!(
        requests[0].publication_content_hash,
        Some(ContentId::new("content-browser-new"))
    );
    assert_eq!(requests[0].publication_content_length, Some(4));
    let storage = events
        .iter()
        .rev()
        .find_map(|event| match event {
            BrowserWorkerEvent::StorageUpdated(storage) => Some(storage.as_ref()),
            _ => None,
        })
        .expect("storage update");
    assert_eq!(
        storage.last_head_artifact_transport.as_deref(),
        Some("peer-native")
    );
    assert!(storage.artifact_replay_checkpoint.is_none());
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
    let client = BrowserEdgeClient::new(
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
