use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_bootstrap::{BootstrapDiagnostics, BootstrapPreset, BootstrapService};
use burn_p2p_core::{
    AggregateEnvelope, AggregateStats, AggregateTier, ArtifactId, AssignmentLease, AuthProvider,
    BrowserRole, ContributionReceipt, ContributionReceiptId, ExperimentDirectoryEntry,
    ExperimentId, ExperimentOptInPolicy, ExperimentResourceRequirements, ExperimentScope,
    ExperimentVisibility, HeadDescriptor, HeadId, MergeCertId, MergeCertificate, MergePolicy,
    MergeStrategy, MergeTopologyPolicy, MergeWindowState, NetworkEstimate, NetworkId, PeerId,
    PeerRole, PeerRoleSet, ReducerLoadReport, ReductionCertificate, RevisionId, RevocationEpoch,
    StudyId, WindowActivation, WindowId,
};
use burn_p2p_security::PeerTrustLevel;
use burn_p2p_swarm::{AlertNotice, AlertSeverity, OverlayChannel, OverlayTopic};
use chrono::{Duration, Utc};

use crate::{
    AggregateDagView, AuthPortalView, AuthorityActionRecord, BrowserExperimentPickerCard,
    BrowserExperimentPickerState, BrowserExperimentPickerView, CheckpointDagEdgeKind,
    CheckpointDagView, CheckpointDownload, ContributionIdentityPanel, CostPerformancePoint,
    EmaFlowView, ExperimentMigrationView, ExperimentPickerView, ExperimentVariantView,
    GitHubProfileLink, HeadPromotionTimelineEntry, LoginProviderView, MergeQueueEntry,
    MergeQueueStatus, MergeTopologyDashboardView, MergeWindowView, MetricPoint,
    OperatorConsoleView, OverlayStatusView, ParticipantPortalView, ParticipantProfile,
    ReducerUtilizationView, ShardAssignmentHeatmap, StudyBoardView, TrustBadgeView, UiChannel,
    UiEventEnvelope, UiPayload,
};

#[test]
fn participant_portal_sorts_receipts_and_tracks_latest() {
    let now = Utc::now();
    let older = ContributionReceipt {
        receipt_id: ContributionReceiptId::new("older"),
        peer_id: PeerId::new("peer"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        base_head_id: HeadId::new("base"),
        artifact_id: ArtifactId::new("artifact-1"),
        accepted_at: now,
        accepted_weight: 1.0,
        metrics: BTreeMap::new(),
        merge_cert_id: None,
    };
    let newer = ContributionReceipt {
        receipt_id: ContributionReceiptId::new("newer"),
        peer_id: PeerId::new("peer"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        base_head_id: HeadId::new("base"),
        artifact_id: ArtifactId::new("artifact-2"),
        accepted_at: now + Duration::seconds(5),
        accepted_weight: 2.0,
        metrics: BTreeMap::new(),
        merge_cert_id: None,
    };

    let view = ParticipantPortalView::new(
        ParticipantProfile {
            peer_id: PeerId::new("peer"),
            display_name: Some("peer".into()),
            github: Some(GitHubProfileLink {
                login: "peer".into(),
                profile_url: "https://github.com/peer".into(),
                linked_at: now,
            }),
        },
        None,
        None,
        vec![older.clone(), newer.clone()],
        Some(ArtifactId::new("published")),
        vec![CheckpointDownload {
            head_id: HeadId::new("head"),
            artifact_id: ArtifactId::new("artifact"),
            label: "latest".into(),
            download_path: "/downloads/latest".into(),
        }],
    );

    assert_eq!(view.latest_accepted_receipt, Some(newer));
    assert_eq!(view.accepted_receipts[1], older);
}

#[test]
fn checkpoint_dag_builds_lineage_and_merge_edges() {
    let now = Utc::now();
    let base = HeadDescriptor {
        head_id: HeadId::new("base"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        artifact_id: ArtifactId::new("artifact-base"),
        parent_head_id: None,
        global_step: 1,
        created_at: now,
        metrics: BTreeMap::new(),
    };
    let child = HeadDescriptor {
        head_id: HeadId::new("child"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        artifact_id: ArtifactId::new("artifact-child"),
        parent_head_id: Some(HeadId::new("base")),
        global_step: 2,
        created_at: now + Duration::seconds(1),
        metrics: BTreeMap::new(),
    };
    let merge = MergeCertificate {
        merge_cert_id: MergeCertId::new("merge"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        base_head_id: HeadId::new("base"),
        merged_head_id: HeadId::new("child"),
        merged_artifact_id: ArtifactId::new("artifact-child"),
        policy: MergePolicy::Ema,
        issued_at: now + Duration::seconds(2),
        validator: PeerId::new("validator"),
        contribution_receipts: vec![],
    };

    let dag =
        CheckpointDagView::from_heads_and_merges(vec![child.clone(), base.clone()], vec![merge]);

    assert_eq!(dag.latest_head_id, Some(child.head_id));
    assert_eq!(dag.nodes.len(), 2);
    assert!(
        dag.edges
            .iter()
            .any(|edge| edge.kind == CheckpointDagEdgeKind::Lineage)
    );
    assert!(
        dag.edges
            .iter()
            .any(|edge| edge.kind == CheckpointDagEdgeKind::CertifiedMerge)
    );
}

#[test]
fn ema_flow_orders_merge_certificates_by_time() {
    let now = Utc::now();
    let older = MergeCertificate {
        merge_cert_id: MergeCertId::new("older"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        base_head_id: HeadId::new("base-1"),
        merged_head_id: HeadId::new("merged-1"),
        merged_artifact_id: ArtifactId::new("artifact-1"),
        policy: MergePolicy::Ema,
        issued_at: now,
        validator: PeerId::new("validator"),
        contribution_receipts: vec![],
    };
    let newer = MergeCertificate {
        merge_cert_id: MergeCertId::new("newer"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        base_head_id: HeadId::new("base-2"),
        merged_head_id: HeadId::new("merged-2"),
        merged_artifact_id: ArtifactId::new("artifact-2"),
        policy: MergePolicy::Ema,
        issued_at: now + Duration::seconds(1),
        validator: PeerId::new("validator"),
        contribution_receipts: vec![],
    };

    let flow = EmaFlowView::from_merge_certificates(vec![newer.clone(), older.clone()]);

    assert_eq!(flow.steps[0].merge_cert_id, older.merge_cert_id);
    assert_eq!(flow.steps[1].merge_cert_id, newer.merge_cert_id);
}

#[test]
fn heatmap_aggregates_microshards_by_peer() {
    let now = Utc::now();
    let leases = vec![
        AssignmentLease {
            lease_id: burn_p2p_core::LeaseId::new("lease-1"),
            network_id: NetworkId::new("network"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            peer_id: PeerId::new("peer"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view"),
            window_id: WindowId(1),
            granted_at: now,
            expires_at: now + Duration::minutes(5),
            budget_work_units: 100,
            microshards: vec![burn_p2p_core::MicroShardId::new("micro-a")],
            assignment_hash: burn_p2p_core::ContentId::new("hash-1"),
        },
        AssignmentLease {
            lease_id: burn_p2p_core::LeaseId::new("lease-2"),
            network_id: NetworkId::new("network"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            peer_id: PeerId::new("peer"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view"),
            window_id: WindowId(2),
            granted_at: now + Duration::seconds(1),
            expires_at: now + Duration::minutes(5),
            budget_work_units: 80,
            microshards: vec![burn_p2p_core::MicroShardId::new("micro-a")],
            assignment_hash: burn_p2p_core::ContentId::new("hash-2"),
        },
    ];

    let heatmap = ShardAssignmentHeatmap::from_leases(leases);

    assert_eq!(heatmap.cells.len(), 1);
    assert_eq!(heatmap.cells[0].lease_count, 2);
    assert_eq!(heatmap.cells[0].total_budget_work_units, 180);
    assert_eq!(heatmap.cells[0].latest_window_id, WindowId(2));
}

#[test]
fn operator_console_and_study_board_are_framework_neutral_contracts() {
    let now = Utc::now();
    let diagnostics = BootstrapDiagnostics {
        network_id: NetworkId::new("network"),
        preset: BootstrapPreset::AllInOne,
        services: BTreeSet::from([BootstrapService::Authority]),
        roles: PeerRoleSet::default_trainer(),
        swarm: burn_p2p_swarm::SwarmStats {
            connected_peers: 3,
            connected_peer_ids: vec![PeerId::new("a"), PeerId::new("b"), PeerId::new("c")],
            observed_peers: vec![PeerId::new("a"), PeerId::new("b"), PeerId::new("c")],
            network_estimate: NetworkEstimate {
                connected_peers: 3,
                observed_peers: 3,
                estimated_network_size: 4.0,
                estimated_total_vram_bytes: Some(1),
                estimated_total_flops: Some(2.0),
                eta_lower_seconds: Some(10),
                eta_upper_seconds: Some(20),
            },
        },
        pinned_heads: BTreeSet::from([HeadId::new("head")]),
        pinned_artifacts: BTreeSet::from([ArtifactId::new("artifact")]),
        accepted_receipts: 5,
        certified_merges: 2,
        in_flight_transfers: Vec::new(),
        admitted_peers: BTreeSet::from([PeerId::new("a"), PeerId::new("b")]),
        peer_diagnostics: vec![burn_p2p_bootstrap::BootstrapPeerDiagnostic {
            peer_id: PeerId::new("a"),
            connected: true,
            observed_at: Some(now),
            trust_level: Some(PeerTrustLevel::PolicyCompliant),
            rejection_reason: None,
            reputation_score: Some(1.25),
            reputation_decision: Some(burn_p2p_security::ReputationDecision::Allow),
            quarantined: false,
            banned: false,
        }],
        rejected_peers: BTreeMap::from([(
            PeerId::new("rejected"),
            "ScopeNotAuthorized(Train { experiment_id: ExperimentId(\"exp\") })".into(),
        )]),
        quarantined_peers: BTreeSet::new(),
        banned_peers: BTreeSet::new(),
        minimum_revocation_epoch: Some(RevocationEpoch(3)),
        last_error: Some("peer rejected".into()),
        node_state: burn_p2p_bootstrap::NodeRuntimeState::IdleReady,
        slot_states: vec![burn_p2p_bootstrap::SlotRuntimeState::Unassigned],
        captured_at: now,
    };

    let operator = OperatorConsoleView::new(
        diagnostics,
        vec![OverlayStatusView {
            overlay: OverlayTopic::experiment(
                NetworkId::new("network"),
                StudyId::new("study"),
                ExperimentId::new("exp"),
                OverlayChannel::Telemetry,
            )
            .expect("overlay"),
            active_peers: 3,
            connected_peers: 3,
            last_window_id: Some(WindowId(7)),
        }],
        vec![MergeQueueEntry {
            base_head_id: HeadId::new("base"),
            candidate_receipt_ids: vec![ContributionReceiptId::new("receipt")],
            status: MergeQueueStatus::Pending,
            merged_head_id: None,
            merged_artifact_id: None,
            merge_cert_id: None,
        }],
        vec![AuthorityActionRecord {
            action: "pause".into(),
            actor_peer_id: Some(PeerId::new("authority")),
            happened_at: now,
        }],
        vec![AlertNotice {
            overlay: OverlayTopic::control(NetworkId::new("network")),
            peer_id: None,
            severity: AlertSeverity::Warn,
            code: "relay-degraded".into(),
            message: "relay latency elevated".into(),
            emitted_at: now,
        }],
    );

    let board = StudyBoardView::new(
        NetworkId::new("network"),
        StudyId::new("study"),
        vec![ExperimentVariantView {
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            metrics: vec![MetricPoint {
                window_id: WindowId(7),
                metrics: BTreeMap::new(),
                captured_at: now,
            }],
            accepted_work: 12,
            cost_performance: vec![CostPerformancePoint {
                label: "quality".into(),
                cost: 5.0,
                value: 0.9,
            }],
        }],
        vec![ExperimentMigrationView {
            experiment_id: ExperimentId::new("exp"),
            from_revision_id: RevisionId::new("rev-a"),
            to_revision_id: RevisionId::new("rev-b"),
            activation: WindowActivation {
                activation_window: WindowId(8),
                grace_windows: 1,
            },
            note: Some("warm patch".into()),
        }],
    );

    let event = UiEventEnvelope {
        channel: UiChannel::Operator,
        sequence: 1,
        emitted_at: now,
        payload: UiPayload::Operator(Box::new(operator)),
    };

    assert!(matches!(event.payload, UiPayload::Operator(_)));
    assert_eq!(board.variants.len(), 1);
    assert_eq!(board.migrations.len(), 1);
}

#[test]
fn experiment_picker_marks_scope_eligible_entries() {
    let entry = ExperimentDirectoryEntry {
        network_id: NetworkId::new("net"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        workload_id: burn_p2p_core::WorkloadId::new("demo-workload"),
        display_name: "Demo".into(),
        model_schema_hash: burn_p2p_core::ContentId::new("model"),
        dataset_view_id: burn_p2p_core::DatasetViewId::new("view"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
            minimum_device_memory_bytes: Some(1024),
            minimum_system_memory_bytes: Some(4096),
            estimated_download_bytes: 4096,
            estimated_window_seconds: 60,
        },
        visibility: ExperimentVisibility::OptIn,
        opt_in_policy: ExperimentOptInPolicy::Scoped,
        current_revision_id: RevisionId::new("rev"),
        current_head_id: Some(HeadId::new("head")),
        allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
        allowed_scopes: BTreeSet::from([ExperimentScope::Train {
            experiment_id: ExperimentId::new("exp"),
        }]),
        metadata: BTreeMap::new(),
    };

    let picker = ExperimentPickerView::from_directory(
        NetworkId::new("net"),
        vec![entry],
        &[ExperimentScope::Train {
            experiment_id: ExperimentId::new("exp"),
        }],
    );

    assert_eq!(picker.entries.len(), 1);
    assert!(picker.entries[0].allowed);
}

#[test]
fn browser_experiment_picker_sorts_and_serializes_browser_state() {
    let picker = BrowserExperimentPickerView::new(
        NetworkId::new("net"),
        vec![
            BrowserExperimentPickerCard {
                experiment_id: ExperimentId::new("exp-b"),
                study_id: StudyId::new("study"),
                display_name: "Zulu".into(),
                current_revision_id: RevisionId::new("rev-b"),
                current_head_id: None,
                estimated_download_bytes: 2048,
                estimated_window_seconds: 30,
                allowed: false,
                recommended_state: BrowserExperimentPickerState::Blocked,
                recommended_role: None,
                fallback_from_preferred: false,
                eligible_roles: Vec::new(),
                blocked_reasons: vec!["missing scope".into()],
            },
            BrowserExperimentPickerCard {
                experiment_id: ExperimentId::new("exp-a"),
                study_id: StudyId::new("study"),
                display_name: "Alpha".into(),
                current_revision_id: RevisionId::new("rev-a"),
                current_head_id: Some(HeadId::new("head-a")),
                estimated_download_bytes: 1024,
                estimated_window_seconds: 15,
                allowed: true,
                recommended_state: BrowserExperimentPickerState::Verifier,
                recommended_role: Some(BrowserRole::Verifier),
                fallback_from_preferred: true,
                eligible_roles: vec![BrowserRole::Observer, BrowserRole::Verifier],
                blocked_reasons: vec!["revision requires WebGPU support".into()],
            },
        ],
    );

    assert_eq!(picker.entries[0].display_name, "Alpha");
    let bytes = serde_json::to_vec(&picker).expect("serialize browser picker");
    let decoded: BrowserExperimentPickerView =
        serde_json::from_slice(&bytes).expect("deserialize browser picker");
    assert_eq!(
        decoded.entries[0].recommended_role,
        Some(BrowserRole::Verifier)
    );
    assert!(decoded.entries[0].fallback_from_preferred);
}

#[test]
fn auth_portal_view_serializes_provider_and_trust_badges() {
    let view = AuthPortalView {
        network_id: NetworkId::new("net"),
        providers: vec![LoginProviderView {
            provider: AuthProvider::Static {
                authority: "lab-auth".into(),
            },
            label: "Static".into(),
            login_path: "/login/static".into(),
            callback_path: Some("/callback/static".into()),
            device_path: Some("/device".into()),
        }],
        active_session: Some(ContributionIdentityPanel {
            principal_id: "alice".into(),
            provider_label: "lab-auth".into(),
            trust_badges: vec![TrustBadgeView {
                level: PeerTrustLevel::PolicyCompliant,
                label: "policy compliant".into(),
            }],
            scoped_experiments: vec![ExperimentId::new("exp")],
        }),
    };

    let bytes = serde_json::to_vec(&view).expect("serialize auth portal");
    let decoded: AuthPortalView = serde_json::from_slice(&bytes).expect("deserialize auth portal");
    assert_eq!(decoded.providers.len(), 1);
    assert_eq!(
        decoded.active_session.expect("session").trust_badges.len(),
        1
    );
}

#[test]
fn merge_topology_views_build_window_load_and_aggregate_dag() {
    let now = Utc::now();
    let merge_window = MergeWindowState {
        merge_window_id: burn_p2p_core::ContentId::new("window-a"),
        network_id: NetworkId::new("network"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        window_id: WindowId(7),
        base_head_id: HeadId::new("head-0"),
        policy: MergeTopologyPolicy {
            strategy: MergeStrategy::ReplicatedRendezvousDag,
            ..MergeTopologyPolicy::default()
        },
        reducers: vec![PeerId::new("reducer-a"), PeerId::new("reducer-b")],
        validators: vec![PeerId::new("validator-a")],
        opened_at: now,
        closes_at: now + Duration::seconds(30),
    };
    let reducer = ReducerLoadReport {
        peer_id: PeerId::new("reducer-a"),
        window_id: WindowId(7),
        assigned_leaf_updates: 8,
        assigned_aggregate_inputs: 2,
        ingress_bytes: 1024,
        egress_bytes: 256,
        duplicate_transfer_ratio: 0.25,
        overload_ratio: 0.75,
        reported_at: now,
    };
    let aggregates = vec![
        AggregateEnvelope {
            aggregate_id: burn_p2p_core::ContentId::new("leaf-a"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            window_id: WindowId(7),
            base_head_id: HeadId::new("head-0"),
            aggregate_artifact_id: ArtifactId::new("agg-art-a"),
            tier: AggregateTier::Leaf,
            reducer_peer_id: PeerId::new("reducer-a"),
            contributor_peers: vec![PeerId::new("peer-a")],
            child_aggregate_ids: Vec::new(),
            stats: AggregateStats {
                accepted_updates: 1,
                duplicate_updates: 0,
                dropped_updates: 0,
                late_updates: 0,
                sum_sample_weight: 1.0,
                sum_quality_weight: 1.0,
                sum_weighted_delta_norm: 1.0,
                max_update_norm: 1.0,
                accepted_sample_coverage: 1.0,
            },
            providers: vec![PeerId::new("reducer-a")],
            published_at: now,
        },
        AggregateEnvelope {
            aggregate_id: burn_p2p_core::ContentId::new("root-a"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            window_id: WindowId(7),
            base_head_id: HeadId::new("head-0"),
            aggregate_artifact_id: ArtifactId::new("agg-art-root"),
            tier: AggregateTier::RootCandidate,
            reducer_peer_id: PeerId::new("validator-a"),
            contributor_peers: vec![PeerId::new("peer-a")],
            child_aggregate_ids: vec![burn_p2p_core::ContentId::new("leaf-a")],
            stats: AggregateStats {
                accepted_updates: 1,
                duplicate_updates: 0,
                dropped_updates: 0,
                late_updates: 0,
                sum_sample_weight: 1.0,
                sum_quality_weight: 1.0,
                sum_weighted_delta_norm: 1.0,
                max_update_norm: 1.0,
                accepted_sample_coverage: 1.0,
            },
            providers: vec![PeerId::new("validator-a")],
            published_at: now,
        },
    ];
    let certificate = ReductionCertificate {
        reduction_id: burn_p2p_core::ContentId::new("cert-a"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        window_id: WindowId(7),
        base_head_id: HeadId::new("head-0"),
        aggregate_id: burn_p2p_core::ContentId::new("root-a"),
        validator: PeerId::new("validator-a"),
        validator_quorum: 2,
        cross_checked_reducers: vec![PeerId::new("reducer-a")],
        issued_at: now,
    };

    let dashboard = MergeTopologyDashboardView {
        merge_window: MergeWindowView::from_state(&merge_window),
        reducers: vec![ReducerUtilizationView::from(reducer)],
        aggregate_dag: AggregateDagView::from_aggregates(aggregates),
        promotion_timeline: vec![HeadPromotionTimelineEntry::from(certificate)],
    };

    assert_eq!(dashboard.merge_window.reducer_count, 2);
    assert_eq!(dashboard.aggregate_dag.nodes.len(), 2);
    assert_eq!(dashboard.aggregate_dag.edges.len(), 1);
    assert_eq!(dashboard.promotion_timeline.len(), 1);
}
