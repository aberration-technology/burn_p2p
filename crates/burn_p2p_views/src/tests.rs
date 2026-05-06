use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    AggregateEnvelope, AggregateStats, AggregateTier, AggregationStrategy, ArtifactId,
    AssignmentLease, AuthProvider, BrowserRole, CanaryEvalReport, CohortFilterStrategy,
    CohortRobustnessReport, ContributionReceipt, ContributionReceiptId, ExperimentDirectoryEntry,
    ExperimentId, ExperimentOptInPolicy, ExperimentResourceRequirements, ExperimentScope,
    ExperimentVisibility, HeadDescriptor, HeadId, MergeCertId, MergeCertificate, MergePolicy,
    MergeStrategy, MergeTopologyPolicy, MergeWindowState, NetworkEstimate, NetworkId, PeerId,
    PeerRole, PeerRoleSet, ReducerLoadReport, ReductionCertificate, RevisionId, RevocationEpoch,
    RobustnessAlert, RobustnessAlertSeverity, RobustnessPolicy, StudyId, TrustScore,
    WindowActivation, WindowId,
};
use burn_p2p_security::{PeerTrustLevel, ReputationDecision};
use burn_p2p_swarm::{AlertNotice, AlertSeverity, OverlayChannel, OverlayTopic};
use burn_p2p_workload::{
    WorkloadExecutionStage, WorkloadTrainingBudget, WorkloadTrainingProgress,
    WorkloadTrainingResult, WorkloadValidationProgress, WorkloadValidationResult,
};
use chrono::{Duration, Utc};

use crate::{
    AggregateDagView, AuthAppView, AuthorityActionRecord, BrowserAppClientView,
    BrowserAppDiffusionView, BrowserAppExperimentSummary, BrowserAppLeaderboardPreview,
    BrowserAppLiveView, BrowserAppMetricPreview, BrowserAppNetworkView, BrowserAppPerformanceView,
    BrowserAppRouteLink, BrowserAppShellView, BrowserAppStaticBootstrap, BrowserAppSummaryCard,
    BrowserAppSurface, BrowserAppSurfaceTab, BrowserAppTrainingLeaseView, BrowserAppTrainingView,
    BrowserAppValidationView, BrowserAppViewerView, BrowserExperimentPickerCard,
    BrowserExperimentPickerState, BrowserExperimentPickerView, CheckpointDagEdgeKind,
    CheckpointDagView, CheckpointDownload, ContributionIdentityPanel, CostPerformancePoint,
    DirectoryEntryDraftView, DirectoryMutationResultView, EmaFlowView, ExperimentDirectoryListView,
    ExperimentMigrationView, ExperimentPickerView, ExperimentVariantView, GitHubProfileLink,
    HeadPromotionTimelineEntry, LoginProviderView, MergeQueueEntry, MergeQueueStatus,
    MergeTopologyDashboardView, MergeWindowView, MetricPoint, NodeAppDiffusionView,
    OperatorConsoleView, OperatorDiagnosticsView, OperatorPeerDiagnosticView, OverlayStatusView,
    ParticipantAppView, ParticipantProfile, ReducerUtilizationView, RobustnessPanelView,
    RolloutPreviewView, ShardAssignmentHeatmap, StudyBoardView, TrainingBudgetSummaryView,
    TrainingProgressSummaryView, TrainingResultSummaryView, TrustBadgeView, UiChannel,
    UiEventEnvelope, UiPayload, ValidationProgressSummaryView, ValidationResultSummaryView,
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

    let view = ParticipantAppView::new(
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
        promoter_peer_id: PeerId::new("validator"),
        promotion_mode: burn_p2p_core::HeadPromotionMode::ValidatorQuorum,
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
        promoter_peer_id: PeerId::new("validator"),
        promotion_mode: burn_p2p_core::HeadPromotionMode::ValidatorQuorum,
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
        promoter_peer_id: PeerId::new("validator"),
        promotion_mode: burn_p2p_core::HeadPromotionMode::ValidatorQuorum,
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
    let diagnostics = OperatorDiagnosticsView {
        network_id: NetworkId::new("network"),
        preset_label: "AllInOne".into(),
        active_services: vec!["Authority".into()],
        roles: PeerRoleSet::default_trainer(),
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
        pinned_heads: BTreeSet::from([HeadId::new("head")]),
        pinned_artifacts: BTreeSet::from([ArtifactId::new("artifact")]),
        accepted_receipts: 5,
        certified_merges: 2,
        in_flight_transfers: Vec::new(),
        admitted_peers: BTreeSet::from([PeerId::new("a"), PeerId::new("b")]),
        peer_diagnostics: vec![OperatorPeerDiagnosticView {
            peer_id: PeerId::new("a"),
            connected: true,
            observed_at: Some(now),
            trust_level: Some(PeerTrustLevel::PolicyCompliant),
            rejection_reason: None,
            reputation_score: Some(1.25),
            reputation_decision: Some(ReputationDecision::Allow),
            quarantined: false,
            banned: false,
        }],
        rejected_peers: BTreeMap::from([(
            PeerId::new("rejected"),
            "ScopeNotAuthorized(Train { experiment_id: ExperimentId(\"exp\") })".into(),
        )]),
        quarantined_peers: BTreeSet::new(),
        banned_peers: BTreeSet::new(),
        robustness_summary: Some(crate::OperatorRobustnessSummaryView {
            rejected_updates: 2,
            mean_trust_score: 0.8,
            quarantined_peer_count: 0,
            ban_recommended_peer_count: 0,
            canary_regression_count: 1,
            alert_count: 1,
        }),
        robustness_panel: None,
        minimum_revocation_epoch: Some(RevocationEpoch(3)),
        last_error: Some("peer rejected".into()),
        node_state_label: "IdleReady".into(),
        slot_state_labels: vec!["Unassigned".into()],
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
fn training_and_validation_summary_views_follow_shared_execution_payloads() {
    let budget = WorkloadTrainingBudget::default();
    let training_progress = WorkloadTrainingProgress {
        stage: WorkloadExecutionStage::Executing,
        completed_units: 8,
        total_units: Some(16),
        detail: Some("running local micro-epoch".into()),
    };
    let validation_progress = WorkloadValidationProgress {
        stage: WorkloadExecutionStage::Publishing,
        completed_units: 4,
        total_units: Some(4),
        detail: Some("writing receipt".into()),
    };
    let training_result = WorkloadTrainingResult {
        artifact_id: ArtifactId::new("artifact"),
        receipt_id: Some(ContributionReceiptId::new("receipt")),
        window_secs: 12,
        completed_batches: 0,
        completed_examples: 0,
        completed_tokens: 0,
        artifact_published: false,
    };
    let validation_result = WorkloadValidationResult {
        head_id: HeadId::new("head"),
        accepted: true,
        checked_chunks: 5,
        emitted_receipt_id: Some(ContributionReceiptId::new("validation-receipt")),
    };

    let budget_view = TrainingBudgetSummaryView::from(&budget);
    let training_progress_view = TrainingProgressSummaryView::from(&training_progress);
    let validation_progress_view = ValidationProgressSummaryView::from(&validation_progress);
    let training_result_view = TrainingResultSummaryView::from(&training_result);
    let validation_result_view = ValidationResultSummaryView::from(&validation_result);

    assert_eq!(budget_view.execution_mode, "webgpu-required");
    assert_eq!(training_progress_view.stage, "executing");
    assert_eq!(validation_progress_view.stage, "publishing");
    assert_eq!(training_result_view.artifact_id, "artifact");
    assert_eq!(validation_result_view.head_id, "head");
}

#[test]
fn browser_app_shell_view_serializes_lowercase_surface_keys() {
    let shell = BrowserAppShellView {
        active_surface: BrowserAppSurface::Train,
        surface_tabs: vec![
            BrowserAppSurfaceTab {
                surface: BrowserAppSurface::Viewer,
                label: "overview".into(),
                detail: "leaderboard, heads, and experiment state".into(),
            },
            BrowserAppSurfaceTab {
                surface: BrowserAppSurface::Train,
                label: "train".into(),
                detail: "throughput, active slice, and checkpoint export".into(),
            },
        ],
        summary_cards: vec![BrowserAppSummaryCard {
            label: "accepted receipts".into(),
            live_key: Some("accepted_receipts".into()),
            value: "14".into(),
        }],
        routes: vec![BrowserAppRouteLink {
            label: "browser app live json".into(),
            path: "/portal/browser-app/view".into(),
        }],
        refresh_interval_ms: 15_000,
    };

    let json = serde_json::to_string(&shell).expect("serialize shell");

    assert!(json.contains("\"active_surface\":\"train\""));
    assert!(json.contains("\"surface\":\"viewer\""));
    assert!(json.contains("\"refresh_interval_ms\":15000"));
    assert_eq!(
        BrowserAppSurface::from_key("network"),
        BrowserAppSurface::Network
    );
    assert_eq!(
        BrowserAppSurface::from_key("unknown"),
        BrowserAppSurface::Viewer
    );
}

#[test]
fn browser_app_live_view_serializes_flat_live_contract() {
    let live = BrowserAppLiveView {
        captured_at: "2026-04-04T18:00:00Z".into(),
        browser_mode: "Trainer".into(),
        node_state: "Training window".into(),
        network_note: "5 direct connection(s), 12 observed peers, and an estimated 64 peers across the wider network.".into(),
        eta: "120s-180s".into(),
        last_error: "none".into(),
        trust_revocation: "7".into(),
        connected_peers: 5,
        observed_peers: 12,
        estimated_network_size: 64,
        accepted_receipts: 14,
        certified_merges: 3,
        heads_count: 2,
        leaderboard_count: 1,
        in_flight_transfers: 2,
    };

    let json = serde_json::to_string(&live).expect("serialize live view");

    assert!(json.contains("\"captured_at\":\"2026-04-04T18:00:00Z\""));
    assert!(json.contains("\"browser_mode\":\"Trainer\""));
    assert!(json.contains("\"heads_count\":2"));
    assert!(json.contains("\"leaderboard_count\":1"));
}

#[test]
fn browser_app_static_bootstrap_and_client_view_serialize_for_cdn_use() {
    let diffusion: NodeAppDiffusionView = BrowserAppDiffusionView {
        canonical_head_id: "head-8".into(),
        captured_at: "2026-04-04T18:00:00Z".into(),
        peer_adoption: "4 / 5 peers".into(),
        recent_window_adoption: "6 / 8 windows".into(),
        fragmentation: "2 visible head(s)".into(),
        timeline: "4 point(s) over 8.0s".into(),
    };
    let bootstrap = BrowserAppStaticBootstrap {
        app_name: "burn_p2p browser app".into(),
        asset_base_url: "https://cdn.example/burn-p2p".into(),
        module_entry_path: "/assets/browser-app.js".into(),
        stylesheet_path: Some("/assets/browser-app.css".into()),
        default_edge_url: Some("https://bootstrap.example".into()),
        default_surface: BrowserAppSurface::Viewer,
        refresh_interval_ms: 12_000,
    };
    let view = BrowserAppClientView {
        network_id: "mainnet".into(),
        default_surface: BrowserAppSurface::Train,
        runtime_label: "train".into(),
        runtime_detail: "active assignment".into(),
        capability_summary: "webgpu available".into(),
        session_label: "authenticated".into(),
        selected_experiment: Some(BrowserAppExperimentSummary {
            display_name: "Main experiment".into(),
            experiment_id: "exp-auth".into(),
            revision_id: "rev-auth".into(),
            workload_id: "lm-small".into(),
            current_head_id: Some("head-1".into()),
            validate_available: true,
            train_available: true,
            availability: "view / validate / train · ready".into(),
        }),
        viewer: BrowserAppViewerView {
            visible_experiments: 3,
            visible_heads: 2,
            leaderboard_entries: 10,
            signed_directory_ready: true,
            signed_leaderboard_ready: true,
            experiments_preview: vec![BrowserAppExperimentSummary {
                display_name: "Main experiment".into(),
                experiment_id: "exp-auth".into(),
                revision_id: "rev-auth".into(),
                workload_id: "lm-small".into(),
                current_head_id: Some("head-1".into()),
                validate_available: true,
                train_available: true,
                availability: "view / validate / train · ready".into(),
            }],
            leaderboard_preview: vec![BrowserAppLeaderboardPreview {
                label: "alice".into(),
                score: "42.00".into(),
                receipts: 12,
                is_local: true,
            }],
        },
        validation: BrowserAppValidationView {
            validate_available: true,
            can_validate: true,
            current_head_id: Some("head-1".into()),
            metrics_sync_at: Some("2026-04-04T18:00:00Z".into()),
            pending_receipts: 1,
            validation_status: Some("accepted".into()),
            checked_chunks: Some(8),
            emitted_receipt_id: Some("receipt-1".into()),
            evaluation_summary: Some("complete · 128 sample(s) · val_loss 0.2400".into()),
            metric_preview: vec![BrowserAppMetricPreview {
                label: "accuracy".into(),
                value: "0.9200".into(),
            }],
        },
        training: BrowserAppTrainingView {
            train_available: true,
            can_train: true,
            active_assignment: Some("exp-auth/rev-auth".into()),
            active_training_lease: Some(BrowserAppTrainingLeaseView {
                lease_id: "lease-1".into(),
                window_id: 7,
                dataset_view_id: "view-auth".into(),
                assignment_hash: "assign-auth".into(),
                microshard_count: 12,
            }),
            slice_status: "12 microshards cached · 128 left in slice".into(),
            latest_head_id: Some("head-1".into()),
            active_head_artifact_ready: true,
            active_head_artifact_source: Some("p2p".into()),
            active_head_artifact_error: None,
            cached_chunk_artifacts: 4,
            cached_microshards: 12,
            pending_receipts: 1,
            max_window_secs: Some(30),
            last_window_secs: Some(30),
            optimizer_steps: Some(48),
            accepted_samples: Some(2048),
            slice_target_samples: Some(2176),
            slice_remaining_samples: Some(128),
            last_loss: Some("0.2400".into()),
            publish_latency_ms: Some(320),
            throughput_summary: Some("68.3 sample/s".into()),
            last_artifact_id: Some("artifact-1".into()),
            last_receipt_id: Some("receipt-1".into()),
        },
        network: BrowserAppNetworkView {
            edge_base_url: "https://bootstrap.example".into(),
            transport: "webtransport".into(),
            node_state: "observe".into(),
            direct_peers: 5,
            observed_peers: 18,
            estimated_network_size: 256,
            accepted_receipts: 42,
            certified_merges: 8,
            in_flight_transfers: 2,
            network_note: "5 direct peers; browser clients stay scoped to edge visibility instead of full-network fanout.".into(),
            swarm_status: BrowserSwarmStatus::default(),
            metrics_live_ready: true,
            last_directory_sync_at: Some("2026-04-04T18:00:00Z".into()),
            last_error: None,
            performance: Some(BrowserAppPerformanceView {
                scope_summary: "5 peer(s) · 9 train window(s) · 4 eval report(s)".into(),
                captured_at: "2026-04-04T18:00:00Z".into(),
                training_throughput: "84.2 work/s".into(),
                validation_throughput: "512 sample/s".into(),
                wait_time: "820 ms".into(),
                idle_time: "6.2s".into(),
            }),
            diffusion: Some(diffusion),
        },
    };

    let bootstrap_json = serde_json::to_string(&bootstrap).expect("serialize static bootstrap");
    let view_json = serde_json::to_string(&view).expect("serialize client view");

    assert!(bootstrap_json.contains("\"module_entry_path\":\"/assets/browser-app.js\""));
    assert!(bootstrap_json.contains("\"default_surface\":\"viewer\""));
    assert!(bootstrap_json.contains("\"refresh_interval_ms\":12000"));
    assert!(view_json.contains("\"default_surface\":\"train\""));
    assert!(view_json.contains("\"runtime_label\":\"train\""));
    assert!(view_json.contains("\"metrics_live_ready\":true"));
    assert!(view_json.contains("\"estimated_network_size\":256"));
    assert!(view_json.contains("\"training_throughput\":\"84.2 work/s\""));
    assert!(view_json.contains("\"idle_time\":\"6.2s\""));
    assert!(view_json.contains("\"peer_adoption\":\"4 / 5 peers\""));
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
        training_protocol: Default::default(),
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
    let view = AuthAppView {
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
    let decoded: AuthAppView = serde_json::from_slice(&bytes).expect("deserialize auth portal");
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
        promoter_peer_id: PeerId::new("validator-a"),
        promotion_mode: burn_p2p_core::HeadPromotionMode::ValidatorQuorum,
        promotion_quorum: 2,
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

#[test]
fn robustness_panel_view_surfaces_rejections_quarantine_and_canary_failures() {
    let now = Utc::now();
    let cohort = CohortRobustnessReport {
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        window_id: WindowId(9),
        cohort_filter_strategy: CohortFilterStrategy::SimilarityAware,
        aggregation_strategy: AggregationStrategy::ClippedWeightedMean,
        total_updates: 4,
        accepted_updates: 2,
        rejected_updates: 2,
        downweighted_updates: 1,
        effective_weight_sum: 1.5,
        mean_screen_score: 2.0,
        rejection_reasons: BTreeMap::from([
            (burn_p2p_core::RejectionReason::Replay, 1),
            (burn_p2p_core::RejectionReason::SimilarityOutlier, 1),
        ]),
        alerts: vec![RobustnessAlert {
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            window_id: Some(WindowId(9)),
            peer_id: Some(PeerId::new("peer-b")),
            reason: burn_p2p_core::RejectionReason::Replay,
            severity: RobustnessAlertSeverity::Warn,
            message: "replay rejected".into(),
            emitted_at: now,
        }],
    };
    let trust_scores = vec![
        TrustScore {
            peer_id: PeerId::new("peer-a"),
            score: 0.5,
            reducer_eligible: true,
            validator_eligible: true,
            quarantined: false,
            ban_recommended: false,
            updated_at: now,
        },
        TrustScore {
            peer_id: PeerId::new("peer-b"),
            score: -2.0,
            reducer_eligible: false,
            validator_eligible: false,
            quarantined: true,
            ban_recommended: true,
            updated_at: now,
        },
    ];
    let canary_reports = vec![CanaryEvalReport {
        experiment_id: ExperimentId::new("exp"),
        revision_id: RevisionId::new("rev"),
        eval_protocol_id: burn_p2p_core::ContentId::new("canary"),
        candidate_head_id: HeadId::new("head-2"),
        base_head_id: Some(HeadId::new("head-1")),
        accepted: false,
        metric_deltas: BTreeMap::from([("loss".into(), 0.12)]),
        regression_margin: 0.12,
        detected_backdoor_trigger: false,
        evaluator_quorum: 2,
        evaluated_at: now,
    }];

    let panel = RobustnessPanelView::from_reports(
        RobustnessPolicy::strict(),
        &cohort,
        &trust_scores,
        &canary_reports,
    );

    assert_eq!(panel.rejection_reasons.len(), 2);
    assert_eq!(panel.quarantined_peers.len(), 1);
    assert!(panel.quarantined_peers[0].ban_recommended);
    assert_eq!(panel.canary_regressions.len(), 1);
    assert_eq!(panel.policy.preset, burn_p2p_core::RobustnessPreset::Strict);
}

#[test]
fn operator_rollout_views_serialize_directory_and_draft_state() {
    let entry = ExperimentDirectoryEntry {
        network_id: NetworkId::new("net"),
        study_id: StudyId::new("study"),
        experiment_id: ExperimentId::new("exp"),
        workload_id: burn_p2p_core::WorkloadId::new("wgpu-demo"),
        display_name: "Operator Experiment".into(),
        model_schema_hash: burn_p2p_core::ContentId::new("schema"),
        dataset_view_id: burn_p2p_core::DatasetViewId::new("dataset"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
            minimum_device_memory_bytes: Some(1024),
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 2048,
            estimated_window_seconds: 60,
        },
        visibility: ExperimentVisibility::AuthorityAssigned,
        opt_in_policy: ExperimentOptInPolicy::AuthorityAssigned,
        current_revision_id: RevisionId::new("rev"),
        current_head_id: None,
        allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
        allowed_scopes: BTreeSet::from([ExperimentScope::Train {
            experiment_id: ExperimentId::new("exp"),
        }]),
        training_protocol: Default::default(),
        metadata: BTreeMap::from([("profile_json".into(), "{\"kind\":\"language\"}".into())]),
    };

    let list = ExperimentDirectoryListView::from_entries(
        "/directory",
        "/directory/signed",
        Some("exp".into()),
        Some("rev".into()),
        std::slice::from_ref(&entry),
    );
    let draft = DirectoryEntryDraftView::from_entry(&entry);
    let preview = RolloutPreviewView {
        summary_label: "1 entry pending rollout".into(),
        submit_path: "/admin".into(),
        requires_session: true,
        entries: list.entries.clone(),
    };
    let status = DirectoryMutationResultView {
        status_label: "rollout applied".into(),
        directory_entries: 1,
        trusted_issuers: 2,
        reenrollment_required: false,
    };

    let encoded = serde_json::to_value((&list, &draft, &preview, &status)).expect("encode views");
    assert_eq!(encoded[0]["entries"][0]["experiment_id"], "exp");
    assert!(
        encoded[1]["metadata_json"]
            .as_str()
            .unwrap_or_default()
            .contains("profile_json")
    );
    assert_eq!(encoded[2]["entries"][0]["revision_id"], "rev");
    assert_eq!(encoded[3]["status_label"], "rollout applied");
}
use burn_p2p_core::BrowserSwarmStatus;
