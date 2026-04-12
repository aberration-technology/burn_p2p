use crate::{
    AppArtifactAliasHistoryRow, AppArtifactRow, AppArtifactRunSummaryRow, AppArtifactRunView,
    AppHeadArtifactView, AppHeadEvalSummaryRow, AppHeadRow, AppOperatorAuditFacetSummaryView,
    AppOperatorAuditPageView, AppOperatorAuditRow, AppOperatorAuditSummaryView,
    AppOperatorControlReplayPageView, AppOperatorControlReplayRow,
    AppOperatorControlReplaySummaryView, AppOperatorFacetBucketView, AppOperatorReplayPageView,
    AppOperatorReplaySnapshotDetailView, AppOperatorReplaySnapshotRow, AppOperatorRetentionView,
    AppPublishedArtifactRow, AuthSessionCard, ContributionReceiptSummaryPanel,
    ExperimentRevisionSelector, LifecycleAssignmentStatusCard, RuntimeCapabilityCard,
    TrainingResultPanel, TransportHealthPanel, assert_browser_client_selected_experiment,
    assert_experiment_picker_contains_allowed_revision, assert_lifecycle_assignment_matches,
    assert_participant_has_receipts, assert_training_result_complete,
    assert_transport_health_ready, render_artifact_run_summaries_html,
    render_artifact_run_view_html, render_browser_app_static_html, render_dashboard_html,
    render_head_artifact_view_html, render_operator_audit_html,
    render_operator_control_replay_html, render_operator_replay_html,
    render_operator_replay_snapshot_html, render_operator_retention_html,
};
use burn_p2p_core::{
    ArtifactId, ContributionReceipt, ContributionReceiptId, ExperimentId, ExperimentScope, HeadId,
    NetworkId, PeerId, RevisionId, StudyId,
};
use burn_p2p_security::PeerTrustLevel;
use burn_p2p_views::{
    BrowserAppClientView, BrowserAppNetworkView, BrowserAppStaticBootstrap, BrowserAppSurface,
    BrowserAppTrainingView, BrowserAppValidationView, BrowserAppViewerView,
    ContributionIdentityPanel, ExperimentPickerView, LifecycleAssignmentStatusView,
    ParticipantAppView, ParticipantProfile, RuntimeCapabilitySummaryView,
    TrainingResultSummaryView, TrustBadgeView,
};
use chrono::Utc;
use dioxus::{prelude::*, ssr::render_element};
use std::collections::BTreeMap;

#[test]
fn dashboard_html_mentions_bootstrap_routes() {
    let html = render_dashboard_html("mainnet");
    assert!(html.contains("/portal/snapshot"));
    assert!(html.contains("/diagnostics/bundle"));
    assert!(html.contains("/operator/audit"));
    assert!(html.contains("/operator/replay"));
    assert!(html.contains("/operator/control"));
    assert!(html.contains("/operator/retention/view"));
}

#[test]
fn operator_audit_page_renders_filters_facets_and_rows() {
    let html = render_operator_audit_html(&AppOperatorAuditPageView {
        summary: AppOperatorAuditSummaryView {
            backend: "file".into(),
            record_count: 3,
            kind_counts: vec!["lifecycle-plan: 1".into(), "schedule-epoch: 2".into()],
            distinct_study_count: 1,
            distinct_experiment_count: 2,
            distinct_revision_count: 2,
            distinct_peer_count: 1,
            distinct_head_count: 1,
            earliest_captured_at: Some("2026-04-12T00:00:00Z".into()),
            latest_captured_at: Some("2026-04-12T00:05:00Z".into()),
        },
        facets: AppOperatorAuditFacetSummaryView {
            limit: 8,
            kinds: vec![AppOperatorFacetBucketView {
                value: "schedule-epoch".into(),
                count: 2,
            }],
            studies: vec![AppOperatorFacetBucketView {
                value: "study-a".into(),
                count: 3,
            }],
            experiments: vec![AppOperatorFacetBucketView {
                value: "exp-b".into(),
                count: 2,
            }],
            revisions: vec![AppOperatorFacetBucketView {
                value: "rev-b".into(),
                count: 2,
            }],
            peers: vec![AppOperatorFacetBucketView {
                value: "trainer-a".into(),
                count: 2,
            }],
            heads: vec![AppOperatorFacetBucketView {
                value: "head-b".into(),
                count: 1,
            }],
        },
        rows: vec![AppOperatorAuditRow {
            record_id: "audit:1".into(),
            kind: "lifecycle-plan".into(),
            study_id: Some("study-a".into()),
            experiment_id: Some("exp-b".into()),
            revision_id: Some("rev-b".into()),
            peer_id: Some("trainer-a".into()),
            head_id: Some("head-b".into()),
            captured_at: "2026-04-12T00:05:00Z".into(),
            summary: "activation_window=7 · target=exp-b".into(),
            control_path: Some("/operator/control?study_id=study-a&experiment_id=exp-b&revision_id=rev-b&peer_id=trainer-a".into()),
            replay_path: Some("/operator/replay?study_id=study-a&experiment_id=exp-b&revision_id=rev-b&head_id=head-b".into()),
        }],
        filter_tags: vec!["kind=lifecycle-plan".into(), "experiment_id=exp-b".into()],
        offset: 0,
        limit: 50,
        total: 3,
        json_page_path: "/operator/audit/page?kind=lifecycle-plan".into(),
        json_summary_path: "/operator/audit/summary?kind=lifecycle-plan".into(),
        json_facets_path: "/operator/audit/facets?kind=lifecycle-plan&limit=8".into(),
        clear_filters_path: Some("/operator/audit".into()),
        prev_page_path: None,
        next_page_path: Some("/operator/audit?kind=lifecycle-plan&offset=50&limit=50".into()),
    });

    assert!(html.contains("Operator audit history"));
    assert!(html.contains("kind=lifecycle-plan"));
    assert!(html.contains("schedule-epoch (2)"));
    assert!(html.contains("trainer-a"));
    assert!(html.contains("/operator/audit/facets?kind=lifecycle-plan"));
    assert!(html.contains("/operator/control?study_id=study-a"));
    assert!(html.contains("experiment_id=exp-b"));
    assert!(html.contains("revision_id=rev-b"));
    assert!(html.contains("peer_id=trainer-a"));
    assert!(html.contains("/operator/replay?study_id=study-a"));
    assert!(html.contains("head_id=head-b"));
    assert!(html.contains(">Reset query<"));
}

#[test]
fn operator_control_replay_page_renders_filters_and_rows() {
    let html = render_operator_control_replay_html(&AppOperatorControlReplayPageView {
        summary: AppOperatorControlReplaySummaryView {
            backend: "file".into(),
            record_count: 2,
            kind_counts: vec!["schedule-epoch: 1".into(), "lifecycle-plan: 1".into()],
            distinct_network_count: 1,
            distinct_study_count: 1,
            distinct_experiment_count: 2,
            distinct_revision_count: 2,
            distinct_peer_count: 1,
            distinct_window_count: 2,
            earliest_captured_at: Some("2026-04-12T00:00:00Z".into()),
            latest_captured_at: Some("2026-04-12T00:05:00Z".into()),
        },
        rows: vec![AppOperatorControlReplayRow {
            record_id: "cert:trainer-a:0".into(),
            kind: "schedule-epoch".into(),
            network_id: "mainnet".into(),
            study_id: "study".into(),
            experiment_id: "exp-a".into(),
            revision_id: "rev-a".into(),
            source_experiment_id: None,
            source_revision_id: None,
            peer_id: Some("trainer-a".into()),
            window_id: 8,
            ends_before_window: Some(12),
            slot_index: Some(0),
            plan_epoch: 4,
            captured_at: "2026-04-12T00:05:00Z".into(),
            summary: "activation_window=8 · slot_index=0".into(),
            audit_path: "/operator/audit?study_id=study&experiment_id=exp-a&revision_id=rev-a&peer_id=trainer-a".into(),
            replay_path: "/operator/replay?study_id=study&experiment_id=exp-a&revision_id=rev-a".into(),
        }],
        filter_tags: vec!["kind=schedule-epoch".into(), "peer_id=trainer-a".into()],
        offset: 0,
        limit: 50,
        total: 2,
        json_page_path: "/operator/control/page?kind=schedule-epoch".into(),
        json_summary_path: "/operator/control/summary?kind=schedule-epoch".into(),
        clear_filters_path: Some("/operator/control".into()),
        prev_page_path: None,
        next_page_path: Some("/operator/control?kind=schedule-epoch&offset=50&limit=50".into()),
    });

    assert!(html.contains("Lifecycle and schedule history"));
    assert!(html.contains("kind=schedule-epoch"));
    assert!(html.contains("trainer-a"));
    assert!(html.contains("/operator/control/page?kind=schedule-epoch"));
    assert!(html.contains("epoch 4"));
    assert!(html.contains("/operator/audit?study_id=study"));
    assert!(html.contains("experiment_id=exp-a"));
    assert!(html.contains("revision_id=rev-a"));
    assert!(html.contains("peer_id=trainer-a"));
    assert!(html.contains("/operator/replay?study_id=study"));
    assert!(html.contains(">Reset query<"));
}

#[test]
fn operator_replay_page_renders_filters_and_rows() {
    let html = render_operator_replay_html(&AppOperatorReplayPageView {
        rows: vec![AppOperatorReplaySnapshotRow {
            captured_at: "2026-04-12T00:05:00Z".into(),
            study_ids: vec!["study-a".into()],
            experiment_ids: vec!["exp-b".into()],
            revision_ids: vec!["rev-b".into()],
            head_ids: vec!["head-b".into()],
            receipt_count: 8,
            head_count: 2,
            merge_count: 1,
            lifecycle_plan_count: 1,
            schedule_epoch_count: 1,
            peer_window_metric_count: 4,
            reducer_cohort_metric_count: 2,
            head_eval_report_count: 1,
            summary: "study-a exp-b rev-b head-b receipt_count=8".into(),
            snapshot_view_path: "/operator/replay/snapshot/view?captured_at=2026-04-12T00:05:00Z"
                .into(),
            json_snapshot_path: "/operator/replay/snapshot?captured_at=2026-04-12T00:05:00Z".into(),
            audit_scope_path: Some("/operator/audit?study_id=study-a&experiment_id=exp-b&revision_id=rev-b&head_id=head-b".into()),
            control_scope_path: Some("/operator/control?study_id=study-a&experiment_id=exp-b&revision_id=rev-b".into()),
        }],
        filter_tags: vec!["experiment_id=exp-b".into()],
        offset: 0,
        limit: 25,
        total: 1,
        visible_receipt_count: 8,
        visible_lifecycle_plan_count: 1,
        visible_schedule_epoch_count: 1,
        json_page_path: "/operator/replay/page?experiment_id=exp-b".into(),
        clear_filters_path: Some("/operator/replay".into()),
        prev_page_path: None,
        next_page_path: None,
    });

    assert!(html.contains("Retained replay snapshots"));
    assert!(html.contains("experiment_id=exp-b"));
    assert!(html.contains("receipts 8"));
    assert!(html.contains("/operator/replay/snapshot/view?captured_at=2026-04-12T00:05:00Z"));
    assert!(html.contains("/operator/audit?study_id=study-a"));
    assert!(html.contains("experiment_id=exp-b"));
    assert!(html.contains("revision_id=rev-b"));
    assert!(html.contains("head_id=head-b"));
    assert!(html.contains("/operator/control?study_id=study-a"));
    assert!(html.contains(">Reset query<"));
}

#[test]
fn operator_replay_snapshot_and_retention_pages_render_details() {
    let replay_html = render_operator_replay_snapshot_html(&AppOperatorReplaySnapshotDetailView {
        captured_at: "2026-04-12T00:05:00Z".into(),
        study_ids: vec!["study-a".into()],
        experiment_ids: vec!["exp-b".into()],
        revision_ids: vec!["rev-b".into()],
        head_ids: vec!["head-b".into()],
        receipt_count: 8,
        head_count: 2,
        merge_count: 1,
        lifecycle_plan_count: 1,
        schedule_epoch_count: 1,
        peer_window_metric_count: 4,
        reducer_cohort_metric_count: 2,
        head_eval_report_count: 1,
        eval_protocol_manifest_count: 1,
        replay_page_path: "/operator/replay".into(),
        json_snapshot_path: "/operator/replay/snapshot?captured_at=2026-04-12T00:05:00Z".into(),
        audit_scope_path: Some(
            "/operator/audit?study_id=study-a&experiment_id=exp-b&revision_id=rev-b&head_id=head-b"
                .into(),
        ),
        control_scope_path: Some(
            "/operator/control?study_id=study-a&experiment_id=exp-b&revision_id=rev-b".into(),
        ),
    });
    assert!(replay_html.contains("Retained replay snapshot"));
    assert!(replay_html.contains("View JSON snapshot"));
    assert!(replay_html.contains("peer window metrics"));
    assert!(replay_html.contains("Audit scope"));
    assert!(replay_html.contains("Control scope"));

    let retention_html = render_operator_retention_html(&AppOperatorRetentionView {
        backend: "postgres".into(),
        snapshot_retention_limit: 128,
        audit_retention_limit: 2048,
        persisted_snapshot_count: 32,
        persisted_audit_record_count: 512,
        latest_snapshot_at: Some("2026-04-12T00:05:00Z".into()),
        max_peer_window_entries_per_revision: 256,
        max_reducer_cohort_entries_per_revision: 128,
        max_head_eval_reports_per_revision: 192,
        max_metric_revisions_per_experiment: 8,
        max_peer_window_detail_windows: 64,
        json_summary_path: "/operator/retention".into(),
    });
    assert!(retention_html.contains("Retention policy"));
    assert!(retention_html.contains("backend postgres"));
    assert!(retention_html.contains("View JSON summary"));
}

#[test]
fn static_browser_app_shell_is_cdn_oriented_and_not_live_route_bound() {
    let html = render_browser_app_static_html(&BrowserAppStaticBootstrap {
        app_name: "burn_p2p".into(),
        asset_base_url: "https://cdn.example/burn-p2p".into(),
        module_entry_path: "assets/browser-app.js".into(),
        stylesheet_path: Some("assets/browser-app.css".into()),
        default_edge_url: Some("https://edge.example".into()),
        default_surface: BrowserAppSurface::Viewer,
        refresh_interval_ms: 15_000,
    });

    assert!(html.contains("data-browser-app=\"static\""));
    assert!(html.contains("id=\"browser-app-static-bootstrap\""));
    assert!(html.contains("https://cdn.example/burn-p2p/assets/browser-app.js"));
    assert!(html.contains("https://cdn.example/burn-p2p/assets/browser-app.css"));
    assert!(html.contains("data-default-edge-url=\"https://edge.example\""));
    assert!(html.contains("\"module_entry_path\":\"assets/browser-app.js\""));
    assert!(html.contains("\"refresh_interval_ms\":15000"));
    assert!(!html.contains("portal-live-json"));
    assert!(!html.contains("portal-shell-json"));
    assert!(!html.contains("data-view-path="));
    assert!(!html.contains("/portal/browser-app/view"));
}

#[test]
fn artifact_run_and_head_pages_render_history_details() {
    let summaries_html = render_artifact_run_summaries_html(
        "exp-auth",
        &[AppArtifactRunSummaryRow {
            experiment_id: "exp-auth".into(),
            run_id: "run-auth".into(),
            latest_head_id: "head-auth-1".into(),
            alias_count: 2,
            alias_history_count: 4,
            published_artifact_count: 2,
            run_view_path: "/portal/artifacts/runs/exp-auth/run-auth".into(),
            json_view_path: "/artifacts/runs/exp-auth/run-auth".into(),
        }],
        "/portal",
    );
    assert!(summaries_html.contains("Artifact runs for exp-auth"));
    assert!(summaries_html.contains("/portal/artifacts/runs/exp-auth/run-auth"));
    assert!(summaries_html.contains("/artifacts/runs/exp-auth/run-auth"));

    let run_html = render_artifact_run_view_html(&AppArtifactRunView {
        experiment_id: "exp-auth".into(),
        run_id: "run-auth".into(),
        latest_head_id: Some("head-auth-1".into()),
        heads: vec![AppHeadRow {
            experiment_id: "exp-auth".into(),
            revision_id: "rev-auth".into(),
            head_id: "head-auth-1".into(),
            global_step: 12,
            created_at: "2026-04-03T12:00:00Z".into(),
        }],
        aliases: vec![AppArtifactRow {
            alias_name: "best_val/main/serve".into(),
            scope: "Run".into(),
            artifact_profile: "ServeCheckpoint".into(),
            experiment_id: "exp-auth".into(),
            run_id: Some("run-auth".into()),
            head_id: "head-auth-1".into(),
            publication_target_id: "local-default".into(),
            artifact_alias_id: Some("alias-auth-2".into()),
            status: "Ready".into(),
            last_published_at: Some("2026-04-03T12:01:00Z".into()),
            history_count: 2,
            previous_head_id: Some("head-auth-0".into()),
            head_view_path: "/portal/artifacts/heads/head-auth-1".into(),
            run_view_path: Some("/portal/artifacts/runs/exp-auth/run-auth".into()),
            export_path: "/artifacts/export".into(),
            download_ticket_path: "/artifacts/download-ticket".into(),
        }],
        alias_history: vec![AppArtifactAliasHistoryRow {
            alias_name: "best_val/main/serve".into(),
            scope: "Run".into(),
            artifact_profile: "ServeCheckpoint".into(),
            head_id: "head-auth-0".into(),
            resolved_at: "2026-04-03T11:30:00Z".into(),
            source_reason: "BestValidation { eval_protocol_id: ContentId(\"main\") }".into(),
        }],
        eval_reports: vec![AppHeadEvalSummaryRow {
            head_id: "head-auth-1".into(),
            eval_protocol_id: "main".into(),
            status: "Completed".into(),
            dataset_view_id: "view-auth".into(),
            sample_count: 128,
            metric_summary: "validation_loss=0.2500".into(),
            finished_at: "2026-04-03T12:00:00Z".into(),
        }],
        publications: vec![AppPublishedArtifactRow {
            head_id: "head-auth-1".into(),
            artifact_profile: "ServeCheckpoint".into(),
            publication_target_id: "local-default".into(),
            status: "Ready".into(),
            object_key: "exp-auth/run-auth/latest-serve".into(),
            content_length: 4096,
            created_at: "2026-04-03T12:01:00Z".into(),
            expires_at: None,
        }],
        app_path: "/portal".into(),
        json_view_path: "/artifacts/runs/exp-auth/run-auth".into(),
    });
    assert!(run_html.contains("Artifact run run-auth"));
    assert!(run_html.contains("Alias transition history"));
    assert!(run_html.contains("validation_loss=0.2500"));
    assert!(run_html.contains("exp-auth/run-auth/latest-serve"));

    let head_html = render_head_artifact_view_html(&AppHeadArtifactView {
        head: AppHeadRow {
            experiment_id: "exp-auth".into(),
            revision_id: "rev-auth".into(),
            head_id: "head-auth-1".into(),
            global_step: 12,
            created_at: "2026-04-03T12:00:00Z".into(),
        },
        experiment_id: "exp-auth".into(),
        run_id: "run-auth".into(),
        aliases: vec![AppArtifactRow {
            alias_name: "latest/serve".into(),
            scope: "Run".into(),
            artifact_profile: "ServeCheckpoint".into(),
            experiment_id: "exp-auth".into(),
            run_id: Some("run-auth".into()),
            head_id: "head-auth-1".into(),
            publication_target_id: "local-default".into(),
            artifact_alias_id: Some("alias-auth-1".into()),
            status: "Ready".into(),
            last_published_at: Some("2026-04-03T12:00:00Z".into()),
            history_count: 3,
            previous_head_id: Some("head-auth-0".into()),
            head_view_path: "/portal/artifacts/heads/head-auth-1".into(),
            run_view_path: Some("/portal/artifacts/runs/exp-auth/run-auth".into()),
            export_path: "/artifacts/export".into(),
            download_ticket_path: "/artifacts/download-ticket".into(),
        }],
        alias_history: vec![AppArtifactAliasHistoryRow {
            alias_name: "latest/serve".into(),
            scope: "Run".into(),
            artifact_profile: "ServeCheckpoint".into(),
            head_id: "head-auth-0".into(),
            resolved_at: "2026-04-03T11:00:00Z".into(),
            source_reason: "LatestCanonicalHead".into(),
        }],
        eval_reports: vec![AppHeadEvalSummaryRow {
            head_id: "head-auth-1".into(),
            eval_protocol_id: "main".into(),
            status: "Completed".into(),
            dataset_view_id: "view-auth".into(),
            sample_count: 128,
            metric_summary: "validation_loss=0.2500".into(),
            finished_at: "2026-04-03T12:00:00Z".into(),
        }],
        publications: vec![AppPublishedArtifactRow {
            head_id: "head-auth-1".into(),
            artifact_profile: "ServeCheckpoint".into(),
            publication_target_id: "local-default".into(),
            status: "Ready".into(),
            object_key: "exp-auth/run-auth/latest-serve".into(),
            content_length: 4096,
            created_at: "2026-04-03T12:01:00Z".into(),
            expires_at: None,
        }],
        available_profiles: vec!["ServeCheckpoint".into(), "BrowserSnapshot".into()],
        app_path: "/portal".into(),
        run_view_path: "/portal/artifacts/runs/exp-auth/run-auth".into(),
        json_view_path: "/artifacts/heads/head-auth-1".into(),
    });
    assert!(head_html.contains("Artifact head head-auth-1"));
    assert!(head_html.contains("Available profiles"));
    assert!(head_html.contains("BrowserSnapshot"));
    assert!(head_html.contains("/portal/artifacts/runs/exp-auth/run-auth"));
}

#[test]
fn reusable_widgets_render_typed_non_portal_cards() {
    let auth_html = render_element(rsx! {
        AuthSessionCard {
            session: Some(ContributionIdentityPanel {
                principal_id: "principal-auth".into(),
                provider_label: "GitHub".into(),
                trust_badges: vec![TrustBadgeView {
                    level: PeerTrustLevel::Reputable,
                    label: "reputable".into(),
                }],
                scoped_experiments: vec![ExperimentId::new("exp-auth")],
            })
        }
    });
    assert!(auth_html.contains("principal-auth"));
    assert!(auth_html.contains("GitHub"));

    let capability_html = render_element(rsx! {
        RuntimeCapabilityCard {
            summary: RuntimeCapabilitySummaryView {
                preferred_role: "trainer".into(),
                backend_summary: "webgpu".into(),
                can_train: true,
            }
        }
    });
    assert!(capability_html.contains("trainer"));
    assert!(capability_html.contains("webgpu"));

    let training_html = render_element(rsx! {
        TrainingResultPanel {
            result: Some(TrainingResultSummaryView {
                artifact_id: "artifact-auth".into(),
                receipt_id: Some("receipt-auth".into()),
                window_secs: 30,
            })
        }
    });
    assert!(training_html.contains("artifact-auth"));
    assert!(training_html.contains("receipt-auth"));

    let network_html = render_element(rsx! {
        TransportHealthPanel {
            network: BrowserAppNetworkView {
                edge_base_url: "https://edge.example".into(),
                transport: "wss-fallback".into(),
                node_state: "observer".into(),
                direct_peers: 2,
                observed_peers: 5,
                estimated_network_size: 7,
                accepted_receipts: 3,
                certified_merges: 1,
                in_flight_transfers: 0,
                network_note: "healthy".into(),
                metrics_live_ready: true,
                last_directory_sync_at: Some("2026-04-11T00:00:00Z".into()),
                last_error: None,
                performance: None,
                diffusion: None,
            }
        }
    });
    assert!(network_html.contains("wss-fallback"));
    assert!(network_html.contains("https://edge.example"));

    let picker = ExperimentPickerView::from_directory(
        NetworkId::new("net-auth"),
        vec![burn_p2p_core::ExperimentDirectoryEntry {
            network_id: NetworkId::new("net-auth"),
            study_id: StudyId::new("study-auth"),
            experiment_id: ExperimentId::new("exp-auth"),
            workload_id: burn_p2p_core::WorkloadId::new("wgpu-demo"),
            display_name: "Auth Experiment".into(),
            model_schema_hash: burn_p2p_core::ContentId::new("schema-auth"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("dataset-auth"),
            resource_requirements: burn_p2p_core::ExperimentResourceRequirements {
                minimum_roles: Default::default(),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: None,
                estimated_download_bytes: 1024,
                estimated_window_seconds: 45,
            },
            visibility: burn_p2p_core::ExperimentVisibility::Public,
            opt_in_policy: burn_p2p_core::ExperimentOptInPolicy::Open,
            current_revision_id: RevisionId::new("rev-auth"),
            current_head_id: Some(HeadId::new("head-auth")),
            allowed_roles: Default::default(),
            allowed_scopes: std::collections::BTreeSet::from([ExperimentScope::Train {
                experiment_id: ExperimentId::new("exp-auth"),
            }]),
            metadata: BTreeMap::new(),
        }],
        &[ExperimentScope::Train {
            experiment_id: ExperimentId::new("exp-auth"),
        }],
    );
    let picker_html = render_element(rsx! {
        ExperimentRevisionSelector {
            picker: picker,
            selected_experiment_id: Some("exp-auth".into()),
            selected_revision_id: Some("rev-auth".into()),
            select_base_path: Some("/portal/select".into()),
        }
    });
    assert!(picker_html.contains("Auth Experiment"));
    assert!(picker_html.contains("/portal/select?experiment_id=exp-auth"));
    assert!(picker_html.contains("revision_id=rev-auth"));

    let receipt_html = render_element(rsx! {
        ContributionReceiptSummaryPanel {
            participant: ParticipantAppView {
                profile: ParticipantProfile {
                    peer_id: PeerId::new("peer-auth"),
                    display_name: Some("auth peer".into()),
                    github: None,
                },
                current_assignment: None,
                local_telemetry: None,
                accepted_receipts: vec![ContributionReceipt {
                    receipt_id: ContributionReceiptId::new("receipt-auth"),
                    peer_id: PeerId::new("peer-auth"),
                    study_id: StudyId::new("study-auth"),
                    experiment_id: ExperimentId::new("exp-auth"),
                    revision_id: RevisionId::new("rev-auth"),
                    base_head_id: HeadId::new("head-auth"),
                    artifact_id: ArtifactId::new("artifact-auth"),
                    accepted_at: Utc::now(),
                    accepted_weight: 2.5,
                    metrics: BTreeMap::new(),
                    merge_cert_id: None,
                }],
                latest_accepted_receipt: None,
                latest_published_artifact_id: None,
                checkpoint_downloads: Vec::new(),
            }
        }
    });
    assert!(receipt_html.contains("1 accepted"));
    assert!(receipt_html.contains("receipt-auth"));

    let lifecycle_html = render_element(rsx! {
        LifecycleAssignmentStatusCard {
            status: LifecycleAssignmentStatusView {
                experiment_label: "exp-auth".into(),
                revision_label: "rev-auth".into(),
                lifecycle_phase: "active".into(),
                assignment_status: "assigned".into(),
            }
        }
    });
    assert!(lifecycle_html.contains("exp-auth"));
    assert!(lifecycle_html.contains("assigned"));
}

#[test]
fn snapshot_assertions_cover_non_portal_downstream_smoke_checks() {
    let browser_view = BrowserAppClientView {
        network_id: "net-auth".into(),
        default_surface: BrowserAppSurface::Train,
        runtime_label: "trainer".into(),
        runtime_detail: "ready".into(),
        capability_summary: "webgpu".into(),
        session_label: "principal-auth".into(),
        selected_experiment: Some(burn_p2p_views::BrowserAppExperimentSummary {
            display_name: "Auth Experiment".into(),
            experiment_id: "exp-auth".into(),
            revision_id: "rev-auth".into(),
            workload_id: "wgpu-demo".into(),
            current_head_id: Some("head-auth".into()),
            validate_available: true,
            train_available: true,
            availability: "available".into(),
        }),
        viewer: BrowserAppViewerView {
            visible_experiments: 1,
            visible_heads: 1,
            leaderboard_entries: 0,
            signed_directory_ready: true,
            signed_leaderboard_ready: false,
            experiments_preview: Vec::new(),
            leaderboard_preview: Vec::new(),
        },
        validation: BrowserAppValidationView {
            validate_available: true,
            can_validate: true,
            current_head_id: Some("head-auth".into()),
            metrics_sync_at: None,
            pending_receipts: 0,
            validation_status: Some("accepted".into()),
            checked_chunks: Some(4),
            emitted_receipt_id: None,
            evaluation_summary: None,
            metric_preview: Vec::new(),
        },
        training: BrowserAppTrainingView {
            train_available: true,
            can_train: true,
            active_assignment: Some("exp-auth/rev-auth".into()),
            slice_status: "active".into(),
            latest_head_id: Some("head-auth".into()),
            cached_chunk_artifacts: 1,
            cached_microshards: 1,
            pending_receipts: 1,
            max_window_secs: Some(30),
            last_window_secs: Some(30),
            optimizer_steps: Some(4),
            accepted_samples: Some(128),
            slice_target_samples: Some(256),
            slice_remaining_samples: Some(128),
            last_loss: Some("0.12".into()),
            publish_latency_ms: Some(120),
            throughput_summary: Some("4.2/s".into()),
            last_artifact_id: Some("artifact-auth".into()),
            last_receipt_id: Some("receipt-auth".into()),
        },
        network: BrowserAppNetworkView {
            edge_base_url: "https://edge.example".into(),
            transport: "wss-fallback".into(),
            node_state: "trainer".into(),
            direct_peers: 1,
            observed_peers: 2,
            estimated_network_size: 2,
            accepted_receipts: 1,
            certified_merges: 1,
            in_flight_transfers: 0,
            network_note: "healthy".into(),
            metrics_live_ready: true,
            last_directory_sync_at: Some("2026-04-11T00:00:00Z".into()),
            last_error: None,
            performance: None,
            diffusion: None,
        },
    };
    assert_browser_client_selected_experiment(&browser_view, "exp-auth", "rev-auth")
        .expect("selected experiment assertion");
    assert_transport_health_ready(&browser_view.network).expect("transport health assertion");

    let picker = ExperimentPickerView::from_directory(
        NetworkId::new("net-auth"),
        vec![burn_p2p_core::ExperimentDirectoryEntry {
            network_id: NetworkId::new("net-auth"),
            study_id: StudyId::new("study-auth"),
            experiment_id: ExperimentId::new("exp-auth"),
            workload_id: burn_p2p_core::WorkloadId::new("wgpu-demo"),
            display_name: "Auth Experiment".into(),
            model_schema_hash: burn_p2p_core::ContentId::new("schema-auth"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("dataset-auth"),
            resource_requirements: burn_p2p_core::ExperimentResourceRequirements {
                minimum_roles: Default::default(),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: None,
                estimated_download_bytes: 1024,
                estimated_window_seconds: 45,
            },
            visibility: burn_p2p_core::ExperimentVisibility::Public,
            opt_in_policy: burn_p2p_core::ExperimentOptInPolicy::Open,
            current_revision_id: RevisionId::new("rev-auth"),
            current_head_id: Some(HeadId::new("head-auth")),
            allowed_roles: Default::default(),
            allowed_scopes: std::collections::BTreeSet::from([ExperimentScope::Train {
                experiment_id: ExperimentId::new("exp-auth"),
            }]),
            metadata: BTreeMap::new(),
        }],
        &[ExperimentScope::Train {
            experiment_id: ExperimentId::new("exp-auth"),
        }],
    );
    assert_experiment_picker_contains_allowed_revision(&picker, "exp-auth", "rev-auth")
        .expect("picker assertion");

    assert_training_result_complete(&TrainingResultSummaryView {
        artifact_id: "artifact-auth".into(),
        receipt_id: Some("receipt-auth".into()),
        window_secs: 30,
    })
    .expect("training result assertion");

    assert_participant_has_receipts(
        &ParticipantAppView {
            profile: ParticipantProfile {
                peer_id: PeerId::new("peer-auth"),
                display_name: Some("auth peer".into()),
                github: None,
            },
            current_assignment: None,
            local_telemetry: None,
            accepted_receipts: vec![ContributionReceipt {
                receipt_id: ContributionReceiptId::new("receipt-auth"),
                peer_id: PeerId::new("peer-auth"),
                study_id: StudyId::new("study-auth"),
                experiment_id: ExperimentId::new("exp-auth"),
                revision_id: RevisionId::new("rev-auth"),
                base_head_id: HeadId::new("head-auth"),
                artifact_id: ArtifactId::new("artifact-auth"),
                accepted_at: Utc::now(),
                accepted_weight: 1.0,
                metrics: BTreeMap::new(),
                merge_cert_id: None,
            }],
            latest_accepted_receipt: None,
            latest_published_artifact_id: None,
            checkpoint_downloads: Vec::new(),
        },
        1,
    )
    .expect("participant receipt assertion");

    assert_lifecycle_assignment_matches(
        &LifecycleAssignmentStatusView {
            experiment_label: "exp-auth".into(),
            revision_label: "rev-auth".into(),
            lifecycle_phase: "active".into(),
            assignment_status: "assigned".into(),
        },
        "exp-auth",
        "rev-auth",
    )
    .expect("lifecycle assertion");
}
