//! Schema round-trip and default-value tests.

use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;

use super::{
    ActiveServiceSet, AdminMode, AppMode, ArtifactAlias, ArtifactAliasScope,
    ArtifactAliasSourceReason, ArtifactLiveEvent, ArtifactLiveEventKind, ArtifactProfile,
    AuthProvider, BackpressurePolicy, BadgeAward, BadgeKind, BrowserMode, ClientPlatform,
    ClientReleaseManifest, CompiledFeatureSet, ConfiguredServiceSet, DownloadDeliveryMode,
    DownloadTicket, EdgeAuthProvider, EdgeFeature, EdgeServiceManifest, EvalAggregationRule,
    EvalMetricDef, EvalProtocolManifest, EvalProtocolOptions, ExperimentDirectoryEntry,
    ExperimentOptInPolicy, ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility,
    ExportJob, ExportJobStatus, HeadEvalReport, HeadEvalStatus, IdentityVisibility, LagPolicy,
    LagState, LeaderboardEntry, LeaderboardIdentity, LeaderboardSnapshot, MergeStrategy,
    MergeTopologyPolicy, MergeWindowMissPolicy, MetricTrustClass, MetricValue,
    MetricsLedgerSegment, MetricsLiveEvent, MetricsLiveEventKind, MetricsMode,
    MetricsSnapshotManifest, MetricsSyncCursor, NetworkManifest, NodeCertificate,
    NodeCertificateClaims, PeerId, PeerRole, PeerRoleSet, PeerWindowMetrics, PeerWindowStatus,
    ProfileMode, PublicationAccessMode, PublicationMode, PublicationTarget, PublicationTargetKind,
    PublishedArtifactRecord, PublishedArtifactStatus, ReducerCohortMetrics, ReducerCohortStatus,
    RejectionReason, RobustnessPolicy, RobustnessPreset, SchemaEnvelope, SocialMode, SocialProfile,
    SupportedWorkload, UpdateFeatureSketch, WindowActivation, WindowId,
};
use crate::{
    RevocationEpoch,
    codec::{CanonicalSchema, deterministic_cbor, from_cbor_slice},
    id::{
        ArtifactAliasId, ContentId, DatasetViewId, DownloadTicketId, ExperimentId, ExportJobId,
        HeadId, LeaseId, NetworkId, PrincipalId, PublicationTargetId, PublishedArtifactId,
        RevisionId, RunId, WorkloadId,
    },
};

#[test]
fn identical_payloads_produce_the_same_content_id() {
    let first = ("study", 7_u64, true);
    let second = ("study", 7_u64, true);

    let first_id = ContentId::derive(&first).expect("id");
    let second_id = ContentId::derive(&second).expect("id");

    assert_eq!(first_id, second_id);
}

#[test]
fn schema_envelope_round_trips_through_cbor() {
    let envelope = SchemaEnvelope::new(
        "burn_p2p.test",
        semver::Version::new(0, 1, 0),
        MetricValue::Float(1.25),
    );

    let bytes = envelope.to_cbor_vec().expect("encode");
    let decoded: SchemaEnvelope<MetricValue> = from_cbor_slice(&bytes).expect("decode");

    assert_eq!(decoded, envelope);
}

#[test]
fn activation_respects_window_boundaries() {
    let activation = WindowActivation {
        activation_window: WindowId(12),
        grace_windows: 2,
    };

    assert!(!activation.becomes_active_at(WindowId(11)));
    assert!(activation.becomes_active_at(WindowId(12)));
    assert!(activation.becomes_active_at(WindowId(13)));
}

#[test]
fn signed_payload_derives_a_payload_id() {
    let payload = SchemaEnvelope::new("burn_p2p.test", semver::Version::new(0, 1, 0), "ok");
    let signature = super::SignatureMetadata {
        signer: crate::id::PeerId::new("peer-a"),
        key_id: "key-1".into(),
        algorithm: super::SignatureAlgorithm::Ed25519,
        signed_at: Utc::now(),
        signature_hex: "deadbeef".into(),
    };

    let signed = super::SignedPayload::new(payload.clone(), signature).expect("sign");

    assert_eq!(signed.payload_id, payload.content_id().expect("hash"));
}

#[test]
fn node_certificate_round_trips_and_exposes_claims() {
    let claims = NodeCertificateClaims {
        network_id: crate::id::NetworkId::new("network-a"),
        project_family_id: crate::id::ProjectFamilyId::new("family-a"),
        release_train_hash: ContentId::new("train-a"),
        target_artifact_hash: ContentId::new("artifact-native-a"),
        peer_id: crate::id::PeerId::new("peer-a"),
        peer_public_key_hex: "001122".into(),
        principal_id: PrincipalId::new("principal-a"),
        provider: AuthProvider::GitHub,
        granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
        experiment_scopes: BTreeSet::from([
            ExperimentScope::Connect,
            ExperimentScope::Train {
                experiment_id: crate::id::ExperimentId::new("exp-a"),
            },
        ]),
        client_policy_hash: Some(ContentId::new("policy-a")),
        auth_policy_snapshot: None,
        not_before: Utc::now(),
        not_after: Utc::now(),
        serial: 7,
        revocation_epoch: RevocationEpoch(3),
    };
    let signature = super::SignatureMetadata {
        signer: crate::id::PeerId::new("authority-a"),
        key_id: "key-1".into(),
        algorithm: super::SignatureAlgorithm::Ed25519,
        signed_at: Utc::now(),
        signature_hex: "deadbeef".into(),
    };
    let certificate =
        NodeCertificate::new(semver::Version::new(0, 1, 0), claims.clone(), signature)
            .expect("certificate");

    let bytes = certificate.to_cbor_vec().expect("encode");
    let decoded: NodeCertificate = from_cbor_slice(&bytes).expect("decode");

    assert_eq!(decoded.claims(), &claims);
    assert_eq!(decoded.claims().revocation_epoch, RevocationEpoch(3));
}

#[test]
fn release_and_network_manifests_round_trip() {
    let workload = SupportedWorkload {
        workload_id: crate::id::WorkloadId::new("lm_125m"),
        workload_name: "LM 125M".into(),
        model_program_hash: ContentId::new("model-program"),
        checkpoint_format_hash: ContentId::new("checkpoint-format"),
        supported_revision_family: ContentId::new("revision-family"),
        resource_class: "gpu-small".into(),
    };
    let release = ClientReleaseManifest {
        project_family_id: crate::id::ProjectFamilyId::new("family-a"),
        release_train_hash: ContentId::new("train-a"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("artifact-native-a"),
        target_platform: ClientPlatform::Native,
        app_semver: semver::Version::new(0, 2, 0),
        git_commit: "deadbeef".into(),
        cargo_lock_hash: ContentId::new("cargo-lock"),
        burn_version_string: "0.21.0-pre.3".into(),
        enabled_features_hash: ContentId::new("features"),
        protocol_major: 1,
        supported_workloads: vec![workload],
        built_at: Utc::now(),
    };
    let network = NetworkManifest {
        network_id: crate::id::NetworkId::new("network-a"),
        project_family_id: crate::id::ProjectFamilyId::new("family-a"),
        protocol_major: 1,
        required_release_train_hash: ContentId::new("train-a"),
        allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact-native-a")]),
        authority_public_keys: vec!["001122".into()],
        bootstrap_addrs: vec!["/ip4/127.0.0.1/tcp/4101".into()],
        auth_policy_hash: ContentId::new("auth-policy"),
        created_at: Utc::now(),
        description: "test network".into(),
    };

    let release_bytes = release.to_cbor_vec().expect("encode release");
    let network_bytes = network.to_cbor_vec().expect("encode network");

    let decoded_release: ClientReleaseManifest =
        from_cbor_slice(&release_bytes).expect("decode release");
    let decoded_network: NetworkManifest = from_cbor_slice(&network_bytes).expect("decode network");

    assert_eq!(decoded_release, release);
    assert_eq!(decoded_network, network);
}

#[test]
fn experiment_scope_filters_experiment_specific_scopes() {
    let experiment_id = crate::id::ExperimentId::new("exp-1");

    assert!(
        ExperimentScope::Train {
            experiment_id: experiment_id.clone(),
        }
        .applies_to_experiment(&experiment_id)
    );
    assert!(ExperimentScope::Discover.allows_directory_discovery());
    assert!(
        !ExperimentScope::Archive {
            experiment_id: crate::id::ExperimentId::new("exp-2"),
        }
        .applies_to_experiment(&experiment_id)
    );
}

#[test]
fn experiment_directory_entry_round_trips() {
    let entry = ExperimentDirectoryEntry {
        network_id: crate::id::NetworkId::new("network-a"),
        study_id: crate::id::StudyId::new("study-a"),
        experiment_id: crate::id::ExperimentId::new("exp-a"),
        workload_id: crate::id::WorkloadId::new("lm_125m"),
        display_name: "Example".into(),
        model_schema_hash: ContentId::new("model-a"),
        dataset_view_id: crate::id::DatasetViewId::new("view-a"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
            minimum_device_memory_bytes: Some(1024),
            minimum_system_memory_bytes: Some(4096),
            estimated_download_bytes: 8192,
            estimated_window_seconds: 30,
        },
        visibility: ExperimentVisibility::OptIn,
        opt_in_policy: ExperimentOptInPolicy::Scoped,
        current_revision_id: crate::id::RevisionId::new("rev-a"),
        current_head_id: Some(crate::id::HeadId::new("head-a")),
        allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu, PeerRole::Evaluator]),
        allowed_scopes: BTreeSet::from([
            ExperimentScope::Discover,
            ExperimentScope::Train {
                experiment_id: crate::id::ExperimentId::new("exp-a"),
            },
        ]),
        metadata: BTreeMap::from([("family".into(), "demo".into())]),
    };

    let bytes = entry.to_cbor_vec().expect("encode");
    let decoded: ExperimentDirectoryEntry = from_cbor_slice(&bytes).expect("decode");
    assert_eq!(decoded, entry);
}

#[test]
fn leaderboard_snapshot_round_trips() {
    let now = Utc::now();
    let snapshot = LeaderboardSnapshot {
        network_id: crate::NetworkId::new("net-social"),
        score_version: "leaderboard_score_v1".into(),
        entries: vec![LeaderboardEntry {
            identity: LeaderboardIdentity {
                principal_id: Some(PrincipalId::new("alice")),
                peer_ids: BTreeSet::from([PeerId::new("peer-1"), PeerId::new("peer-2")]),
                label: "alice".into(),
                social_profile: Some(SocialProfile {
                    principal_id: PrincipalId::new("alice"),
                    display_name: Some("Alice".into()),
                    avatar_url: Some("https://example.invalid/alice.png".into()),
                    profile_url: None,
                    org_slug: Some("openai".into()),
                    team_slug: Some("research".into()),
                    visibility: IdentityVisibility::PublicProfile,
                }),
            },
            accepted_work_score: 5.0,
            quality_weighted_impact_score: 3.5,
            validation_service_score: 1.0,
            artifact_serving_score: 0.0,
            leaderboard_score_v1: 7.75,
            accepted_receipt_count: 2,
            last_receipt_at: Some(now),
            badges: vec![BadgeAward {
                kind: BadgeKind::HelpedPromoteCanonicalHead,
                label: "Helped Promote Canonical Head".into(),
                awarded_at: Some(now),
                detail: Some("Accepted work landed in a promoted merge.".into()),
            }],
        }],
        captured_at: now,
    };

    let encoded = snapshot.to_cbor_vec().expect("encode leaderboard");
    let decoded: LeaderboardSnapshot = from_cbor_slice(&encoded).expect("decode leaderboard");

    assert_eq!(decoded, snapshot);
    assert_eq!(
        decoded.entries[0]
            .contribution_rollup()
            .leaderboard_score_v1,
        7.75
    );
}

#[test]
fn robustness_policy_presets_round_trip() {
    let minimal = RobustnessPolicy::minimal();
    let balanced = RobustnessPolicy::balanced();
    let strict = RobustnessPolicy::strict();
    let research = RobustnessPolicy::research();

    assert_eq!(minimal.preset, RobustnessPreset::Minimal);
    assert_eq!(balanced.preset, RobustnessPreset::Balanced);
    assert_eq!(strict.preset, RobustnessPreset::Strict);
    assert_eq!(research.preset, RobustnessPreset::Research);
    assert!(strict.clipping_policy.global_norm_cap < balanced.clipping_policy.global_norm_cap);
    assert!(research.assumed_byzantine_fraction >= balanced.assumed_byzantine_fraction);

    let bytes = strict.to_cbor_vec().expect("encode policy");
    let decoded: RobustnessPolicy = from_cbor_slice(&bytes).expect("decode policy");
    assert_eq!(decoded, strict);
}

#[test]
fn update_feature_sketch_round_trips() {
    let sketch = UpdateFeatureSketch {
        artifact_size_bytes: 4096,
        global_norm: 1.25,
        per_layer_norms: BTreeMap::from([("encoder.0".into(), 0.8), ("head".into(), 0.45)]),
        random_projection: vec![0.1, -0.2, 0.4],
        sign_projection: vec![1, -1, 1],
        top_k_indices: vec![3, 8, 13],
        cosine_to_reference: Some(0.88),
        sign_agreement_fraction: Some(0.71),
        canary_loss_delta: Some(0.01),
        historical_deviation_score: Some(0.2),
        neighbor_distance: Some(1.4),
        staleness_windows: 1,
        receive_delay_ms: 250,
        non_finite_tensor_count: 0,
    };

    let bytes = deterministic_cbor(&sketch).expect("encode sketch");
    let decoded: UpdateFeatureSketch = from_cbor_slice(&bytes).expect("decode sketch");

    assert_eq!(decoded, sketch);
}

#[test]
fn rejection_reason_is_orderable_for_metrics_rollups() {
    let mut reasons = BTreeMap::new();
    reasons.insert(RejectionReason::Replay, 2_u32);
    reasons.insert(RejectionReason::CanaryRegression, 1_u32);

    assert_eq!(reasons.get(&RejectionReason::Replay), Some(&2));
}

#[test]
fn metrics_schema_round_trips() {
    let eval_protocol = EvalProtocolManifest::new(
        "canonical-validation",
        DatasetViewId::new("view-a"),
        "validation",
        vec![EvalMetricDef {
            metric_key: "loss".into(),
            display_name: "Loss".into(),
            unit: None,
            higher_is_better: false,
        }],
        EvalProtocolOptions::new(EvalAggregationRule::Mean, 512, 7, "v1"),
    )
    .expect("eval protocol");

    let peer_window = PeerWindowMetrics {
        network_id: NetworkId::new("network-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
        workload_id: WorkloadId::new("workload-a"),
        dataset_view_id: DatasetViewId::new("view-a"),
        peer_id: PeerId::new("peer-a"),
        principal_id: Some(PrincipalId::new("principal-a")),
        lease_id: LeaseId::new("lease-a"),
        base_head_id: HeadId::new("head-a"),
        window_started_at: Utc::now(),
        window_finished_at: Utc::now(),
        attempted_tokens_or_samples: 256,
        accepted_tokens_or_samples: Some(240),
        local_train_loss_mean: Some(0.42),
        local_train_loss_last: Some(0.39),
        grad_or_delta_norm: Some(1.2),
        optimizer_step_count: 8,
        compute_time_ms: 1500,
        data_fetch_time_ms: 200,
        publish_latency_ms: 50,
        head_lag_at_start: 1,
        head_lag_at_finish: 2,
        backend_class: super::BackendClass::BrowserWgpu,
        role: PeerRole::BrowserTrainerWgpu,
        status: PeerWindowStatus::Completed,
        status_reason: None,
    };

    let reducer_metrics = ReducerCohortMetrics {
        network_id: NetworkId::new("network-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
        workload_id: WorkloadId::new("workload-a"),
        dataset_view_id: DatasetViewId::new("view-a"),
        merge_window_id: ContentId::new("window-a"),
        reducer_group_id: ContentId::new("reducer-group-a"),
        captured_at: Utc::now(),
        base_head_id: HeadId::new("head-a"),
        candidate_head_id: Some(HeadId::new("candidate-a")),
        received_updates: 12,
        accepted_updates: 10,
        rejected_updates: 2,
        sum_weight: 9.5,
        accepted_tokens_or_samples: 2048,
        staleness_mean: 1.4,
        staleness_max: 3.0,
        window_close_delay_ms: 120,
        cohort_duration_ms: 5000,
        aggregate_norm: 2.5,
        reducer_load: 0.75,
        ingress_bytes: 4096,
        egress_bytes: 1024,
        replica_agreement: Some(0.98),
        late_arrival_count: Some(1),
        missing_peer_count: Some(0),
        rejection_reasons: BTreeMap::from([("stale_base_head".into(), 2)]),
        status: ReducerCohortStatus::Closed,
    };

    let report = HeadEvalReport {
        network_id: NetworkId::new("network-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
        workload_id: WorkloadId::new("workload-a"),
        head_id: HeadId::new("head-b"),
        base_head_id: Some(HeadId::new("head-a")),
        eval_protocol_id: eval_protocol.eval_protocol_id.clone(),
        evaluator_set_id: ContentId::new("eval-set-a"),
        metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.31))]),
        sample_count: 512,
        dataset_view_id: DatasetViewId::new("view-a"),
        started_at: Utc::now(),
        finished_at: Utc::now(),
        trust_class: MetricTrustClass::Canonical,
        status: HeadEvalStatus::Completed,
        signature_bundle: Vec::new(),
    };

    let snapshot = MetricsSnapshotManifest {
        network_id: NetworkId::new("network-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
        snapshot_seq: 3,
        covers_until_head_id: Some(HeadId::new("head-b")),
        covers_until_merge_window_id: Some(ContentId::new("window-a")),
        canonical_head_metrics_ref: ContentId::new("canonical-ref"),
        window_rollups_ref: ContentId::new("window-rollups-ref"),
        network_rollups_ref: ContentId::new("network-rollups-ref"),
        leaderboard_ref: Some(ContentId::new("leaderboard-ref")),
        created_at: Utc::now(),
        signatures: Vec::new(),
    };
    let segment = MetricsLedgerSegment {
        network_id: NetworkId::new("network-a"),
        experiment_id: ExperimentId::new("exp-a"),
        revision_id: RevisionId::new("rev-a"),
        segment_seq: 4,
        from_head_id: Some(HeadId::new("head-a")),
        to_head_id: Some(HeadId::new("head-b")),
        from_window_id: Some(ContentId::new("window-a")),
        to_window_id: Some(ContentId::new("window-b")),
        entries_ref: ContentId::new("entries-ref"),
        hash: ContentId::new("segment-hash"),
        prev_hash: Some(ContentId::new("segment-prev")),
    };
    let live_event = MetricsLiveEvent {
        network_id: NetworkId::new("network-a"),
        kind: MetricsLiveEventKind::CatchupRefresh,
        cursors: vec![MetricsSyncCursor {
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            latest_snapshot_seq: Some(3),
            latest_ledger_segment_seq: Some(4),
            latest_head_id: Some(HeadId::new("head-b")),
            latest_merge_window_id: Some(ContentId::new("window-b")),
        }],
        generated_at: Utc::now(),
    };

    let protocol_bytes = deterministic_cbor(&eval_protocol).expect("encode protocol");
    let peer_bytes = deterministic_cbor(&peer_window).expect("encode peer");
    let reducer_bytes = deterministic_cbor(&reducer_metrics).expect("encode reducer");
    let report_bytes = deterministic_cbor(&report).expect("encode report");
    let snapshot_bytes = deterministic_cbor(&snapshot).expect("encode snapshot");
    let segment_bytes = deterministic_cbor(&segment).expect("encode segment");
    let live_event_bytes = deterministic_cbor(&live_event).expect("encode live event");

    assert_eq!(
        from_cbor_slice::<EvalProtocolManifest>(&protocol_bytes).expect("decode protocol"),
        eval_protocol
    );
    assert_eq!(
        from_cbor_slice::<PeerWindowMetrics>(&peer_bytes).expect("decode peer"),
        peer_window
    );
    assert_eq!(
        from_cbor_slice::<ReducerCohortMetrics>(&reducer_bytes).expect("decode reducer"),
        reducer_metrics
    );
    assert_eq!(
        from_cbor_slice::<HeadEvalReport>(&report_bytes).expect("decode report"),
        report
    );
    assert_eq!(
        from_cbor_slice::<MetricsSnapshotManifest>(&snapshot_bytes).expect("decode snapshot"),
        snapshot
    );
    assert_eq!(
        from_cbor_slice::<MetricsLedgerSegment>(&segment_bytes).expect("decode segment"),
        segment
    );
    assert_eq!(
        from_cbor_slice::<MetricsLiveEvent>(&live_event_bytes).expect("decode live event"),
        live_event
    );
}

#[test]
fn publication_schema_round_trips() {
    let target = PublicationTarget {
        publication_target_id: PublicationTargetId::new("target-local"),
        label: "local mirror".into(),
        kind: PublicationTargetKind::LocalFilesystem,
        publication_mode: PublicationMode::LazyOnDemand,
        access_mode: PublicationAccessMode::Authenticated,
        allow_public_reads: false,
        supports_signed_urls: false,
        edge_proxy_required: true,
        max_artifact_size_bytes: Some(128 * 1024 * 1024),
        retention_ttl_secs: Some(3600),
        allowed_artifact_profiles: BTreeSet::from([
            ArtifactProfile::FullTrainingCheckpoint,
            ArtifactProfile::ServeCheckpoint,
            ArtifactProfile::ManifestOnly,
        ]),
        eager_alias_names: BTreeSet::from(["latest/serve".into()]),
        local_root: Some("/tmp/publish".into()),
        bucket: None,
        endpoint: None,
        region: None,
        access_key_id: None,
        secret_access_key: None,
        session_token: None,
        path_prefix: Some("exp".into()),
        multipart_threshold_bytes: None,
        server_side_encryption: None,
        signed_url_ttl_secs: None,
    };
    let alias = ArtifactAlias {
        artifact_alias_id: ArtifactAliasId::new("alias-latest-serve"),
        experiment_id: ExperimentId::new("exp-a"),
        run_id: Some(RunId::new("run-a")),
        scope: ArtifactAliasScope::Run,
        alias_name: "latest/serve".into(),
        head_id: HeadId::new("head-b"),
        artifact_profile: ArtifactProfile::ServeCheckpoint,
        resolved_at: Utc::now(),
        source_reason: ArtifactAliasSourceReason::LatestCanonicalHead,
    };
    let published = PublishedArtifactRecord {
        published_artifact_id: PublishedArtifactId::new("published-a"),
        artifact_alias_id: Some(alias.artifact_alias_id.clone()),
        experiment_id: ExperimentId::new("exp-a"),
        run_id: Some(RunId::new("run-a")),
        head_id: HeadId::new("head-b"),
        artifact_profile: ArtifactProfile::ServeCheckpoint,
        publication_target_id: target.publication_target_id.clone(),
        object_key: "exp/exp-a/run/run-a/latest-serve.bin".into(),
        content_hash: ContentId::new("hash-a"),
        content_length: 4096,
        created_at: Utc::now(),
        expires_at: None,
        status: PublishedArtifactStatus::Ready,
    };
    let job = ExportJob {
        export_job_id: ExportJobId::new("job-a"),
        requested_by_principal_id: Some(PrincipalId::new("principal-a")),
        experiment_id: ExperimentId::new("exp-a"),
        run_id: Some(RunId::new("run-a")),
        head_id: HeadId::new("head-b"),
        artifact_profile: ArtifactProfile::ServeCheckpoint,
        publication_target_id: target.publication_target_id.clone(),
        status: ExportJobStatus::Ready,
        queued_at: Utc::now(),
        started_at: Some(Utc::now()),
        finished_at: Some(Utc::now()),
        failure_reason: None,
    };
    let ticket = DownloadTicket {
        download_ticket_id: DownloadTicketId::new("ticket-a"),
        published_artifact_id: published.published_artifact_id.clone(),
        principal_id: PrincipalId::new("principal-a"),
        issued_at: Utc::now(),
        expires_at: Utc::now(),
        delivery_mode: DownloadDeliveryMode::EdgeStream,
    };
    let event = ArtifactLiveEvent {
        event_id: ContentId::new("event-a"),
        kind: ArtifactLiveEventKind::ExportJobReady,
        experiment_id: Some(ExperimentId::new("exp-a")),
        run_id: Some(RunId::new("run-a")),
        head_id: Some(HeadId::new("head-b")),
        artifact_profile: Some(ArtifactProfile::ServeCheckpoint),
        publication_target_id: Some(PublicationTargetId::new("target-local")),
        alias_name: Some("latest/serve".into()),
        export_job_id: Some(ExportJobId::new("job-a")),
        published_artifact_id: Some(PublishedArtifactId::new("published-a")),
        detail: Some("ready".into()),
        generated_at: Utc::now(),
    };

    let target_bytes = deterministic_cbor(&target).expect("encode target");
    let alias_bytes = deterministic_cbor(&alias).expect("encode alias");
    let published_bytes = deterministic_cbor(&published).expect("encode published");
    let job_bytes = deterministic_cbor(&job).expect("encode job");
    let ticket_bytes = deterministic_cbor(&ticket).expect("encode ticket");
    let event_bytes = deterministic_cbor(&event).expect("encode event");

    assert_eq!(
        from_cbor_slice::<PublicationTarget>(&target_bytes).expect("decode target"),
        target
    );
    assert_eq!(
        from_cbor_slice::<ArtifactAlias>(&alias_bytes).expect("decode alias"),
        alias
    );
    assert_eq!(
        from_cbor_slice::<PublishedArtifactRecord>(&published_bytes).expect("decode published"),
        published
    );
    assert_eq!(
        from_cbor_slice::<ExportJob>(&job_bytes).expect("decode job"),
        job
    );
    assert_eq!(
        from_cbor_slice::<DownloadTicket>(&ticket_bytes).expect("decode ticket"),
        ticket
    );
    assert_eq!(
        from_cbor_slice::<ArtifactLiveEvent>(&event_bytes).expect("decode event"),
        event
    );
}

#[test]
fn edge_service_manifest_round_trips() {
    let manifest = EdgeServiceManifest {
        edge_id: PeerId::new("edge-1"),
        network_id: crate::NetworkId::new("network-a"),
        app_mode: AppMode::Interactive,
        browser_mode: BrowserMode::Verifier,
        available_auth_providers: BTreeSet::from([
            EdgeAuthProvider::Static,
            EdgeAuthProvider::Oidc,
        ]),
        social_mode: SocialMode::Private,
        profile_mode: ProfileMode::Private,
        admin_mode: AdminMode::Rbac,
        metrics_mode: MetricsMode::OpenMetrics,
        compiled_feature_set: CompiledFeatureSet {
            features: BTreeSet::from([
                EdgeFeature::AdminHttp,
                EdgeFeature::App,
                EdgeFeature::AuthOidc,
            ]),
        },
        configured_service_set: ConfiguredServiceSet {
            features: BTreeSet::from([
                EdgeFeature::AdminHttp,
                EdgeFeature::App,
                EdgeFeature::AuthOidc,
            ]),
        },
        active_feature_set: ActiveServiceSet {
            features: BTreeSet::from([
                EdgeFeature::AdminHttp,
                EdgeFeature::App,
                EdgeFeature::AuthOidc,
            ]),
        },
        generated_at: Utc::now(),
    };

    let encoded = manifest
        .to_cbor_vec()
        .expect("encode edge service manifest");
    let decoded: EdgeServiceManifest =
        from_cbor_slice(&encoded).expect("decode edge service manifest");
    assert_eq!(decoded, manifest);
}

#[test]
fn lag_and_backpressure_policies_round_trip() {
    let payload = (
        LagState::CatchupRequired,
        LagPolicy {
            max_head_lag_before_catchup: 2,
            max_head_lag_before_block: 5,
            max_head_lag_before_full_rebase: 11,
            max_window_skew_before_lease_revoke: 3,
        },
        MergeWindowMissPolicy::RebaseRequired,
        BackpressurePolicy {
            max_in_flight_chunk_requests_per_peer: 12,
            max_in_flight_updates_per_peer: 6,
            reducer_queue_depth_limit: 96,
            upload_concurrency_limit: 3,
            browser_transfer_budget_bytes: 8 * 1024 * 1024,
            app_feed_sample_rate: 2,
        },
    );

    let encoded = deterministic_cbor(&payload).expect("encode policy payload");
    let decoded: (
        LagState,
        LagPolicy,
        MergeWindowMissPolicy,
        BackpressurePolicy,
    ) = from_cbor_slice(&encoded).expect("decode policy payload");

    assert_eq!(decoded.0, LagState::CatchupRequired);
    assert_eq!(decoded.1.max_head_lag_before_full_rebase, 11);
    assert_eq!(decoded.2, MergeWindowMissPolicy::RebaseRequired);
    assert_eq!(decoded.3.reducer_queue_depth_limit, 96);
}

#[test]
fn merge_topology_policy_defaults_to_replicated_rendezvous_dag() {
    let policy = MergeTopologyPolicy::default();

    assert_eq!(policy.strategy, MergeStrategy::ReplicatedRendezvousDag);
    assert_eq!(policy.reducer_replication, 2);
    assert_eq!(policy.target_leaf_cohort, 16);
    assert_eq!(policy.upper_fanin, 4);
    assert!(policy.promotion_policy.apply_single_root_ema);
}
