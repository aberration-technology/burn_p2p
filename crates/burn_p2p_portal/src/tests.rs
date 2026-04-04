use crate::{
    PortalArtifactAliasHistoryRow, PortalArtifactRow, PortalArtifactRunSummaryRow,
    PortalArtifactRunView, PortalDiagnosticsView, PortalExperimentRow, PortalHeadArtifactView,
    PortalHeadEvalSummaryRow, PortalHeadRow, PortalLeaderboardRow, PortalLoginProvider,
    PortalMetricRow, PortalMetricsPanel, PortalPaths, PortalPublishedArtifactRow,
    PortalSnapshotView, PortalTransportSurface, PortalTrustView,
    render_artifact_run_summaries_html, render_artifact_run_view_html, render_browser_portal_html,
    render_dashboard_html, render_head_artifact_view_html,
};

#[test]
fn dashboard_html_mentions_bootstrap_routes() {
    let html = render_dashboard_html("mainnet");
    assert!(html.contains("/portal"));
    assert!(html.contains("/diagnostics/bundle"));
}

#[test]
fn browser_portal_html_renders_snapshot_content() {
    let html = render_browser_portal_html(&PortalSnapshotView {
        network_id: "mainnet".into(),
        auth_enabled: true,
        edge_mode: "Trainer".into(),
        browser_mode: "Trainer".into(),
        social_enabled: true,
        profile_enabled: true,
        login_providers: vec![PortalLoginProvider {
            label: "GitHub".into(),
            login_path: "/login/github".into(),
            callback_path: Some("/callback/github".into()),
            device_path: None,
        }],
        transports: PortalTransportSurface {
            webrtc_direct: true,
            webtransport_gateway: false,
            wss_fallback: true,
        },
        paths: PortalPaths {
            portal_snapshot_path: "/portal/snapshot".into(),
            signed_directory_path: "/directory/signed".into(),
            signed_leaderboard_path: "/leaderboard/signed".into(),
            artifacts_aliases_path: "/artifacts/aliases".into(),
            artifacts_export_path: "/artifacts/export".into(),
            artifacts_download_ticket_path: "/artifacts/download-ticket".into(),
            trust_bundle_path: "/trust".into(),
        },
        diagnostics: PortalDiagnosticsView {
            connected_peers: 5,
            admitted_peers: 4,
            rejected_peers: 1,
            quarantined_peers: 0,
            accepted_receipts: 14,
            certified_merges: 3,
            active_services: vec!["portal".into(), "browser-edge".into(), "social".into()],
        },
        trust: PortalTrustView {
            required_release_train_hash: Some("train-1".into()),
            approved_target_artifact_count: 2,
            active_issuer_peer_id: Some("issuer-1".into()),
            minimum_revocation_epoch: Some(7),
            reenrollment_required: false,
        },
        experiments: vec![PortalExperimentRow {
            display_name: "Auth Demo".into(),
            experiment_id: "exp-auth".into(),
            revision_id: "rev-auth".into(),
            has_head: true,
            estimated_window_seconds: 45,
        }],
        heads: vec![PortalHeadRow {
            experiment_id: "exp-auth".into(),
            revision_id: "rev-auth".into(),
            head_id: "head-auth-1".into(),
            global_step: 12,
            created_at: "2026-04-03T12:00:00Z".into(),
        }],
        leaderboard: vec![PortalLeaderboardRow {
            principal_label: "alice".into(),
            leaderboard_score_v1: 3.5,
            accepted_receipt_count: 2,
        }],
        artifact_rows: vec![PortalArtifactRow {
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
        metrics_panels: vec![PortalMetricsPanel {
            panel_id: "quality".into(),
            title: "Canonical quality".into(),
            description: "Validator-backed head quality signals.".into(),
            rows: vec![PortalMetricRow {
                label: "validation_loss".into(),
                value: "0.2500".into(),
                scope: "head".into(),
                trust: "canonical".into(),
                key: "head-auth-1".into(),
                protocol: Some("runtime-validation-default".into()),
                operator_hint: Some(
                    "Compare this head against recent branch candidates before widening rollout."
                        .into(),
                ),
                detail_path: Some("/metrics/heads/head-auth-1".into()),
                freshness: "2026-04-03T12:00:00Z".into(),
            }],
        }],
    });
    assert!(html.contains("burn_p2p browser portal"));
    assert!(html.contains("/login/github"));
    assert!(html.contains("alice"));
    assert!(html.contains("Current heads"));
    assert!(html.contains("Join checklist"));
    assert!(html.contains("head-auth-1"));
    assert!(html.contains("Canonical quality"));
    assert!(html.contains("validation_loss"));
    assert!(html.contains("Operator hint:"));
    assert!(html.contains("/metrics/heads/head-auth-1"));
    assert!(html.contains("Artifacts"));
    assert!(html.contains("Prepare export"));
    assert!(html.contains("/portal/artifacts/heads/head-auth-1"));
    assert!(html.contains("/portal/artifacts/runs/exp-auth/run-auth"));
    assert!(html.contains("History: 3 resolutions, previous head head-auth-0"));
    assert!(html.contains("mobile-card-list"));
    assert!(html.contains("@media (max-width: 720px)"));
}

#[test]
fn browser_portal_html_hides_disabled_auth_and_social_flows() {
    let html = render_browser_portal_html(&PortalSnapshotView {
        network_id: "mainnet".into(),
        auth_enabled: false,
        edge_mode: "Minimal".into(),
        browser_mode: "Disabled".into(),
        social_enabled: false,
        profile_enabled: false,
        login_providers: Vec::new(),
        transports: PortalTransportSurface {
            webrtc_direct: false,
            webtransport_gateway: false,
            wss_fallback: false,
        },
        paths: PortalPaths {
            portal_snapshot_path: "/portal/snapshot".into(),
            signed_directory_path: "/directory/signed".into(),
            signed_leaderboard_path: "/leaderboard/signed".into(),
            artifacts_aliases_path: "/artifacts/aliases".into(),
            artifacts_export_path: "/artifacts/export".into(),
            artifacts_download_ticket_path: "/artifacts/download-ticket".into(),
            trust_bundle_path: "/trust".into(),
        },
        diagnostics: PortalDiagnosticsView {
            connected_peers: 1,
            admitted_peers: 1,
            rejected_peers: 0,
            quarantined_peers: 0,
            accepted_receipts: 0,
            certified_merges: 0,
            active_services: vec!["portal".into()],
        },
        trust: PortalTrustView {
            required_release_train_hash: None,
            approved_target_artifact_count: 1,
            active_issuer_peer_id: None,
            minimum_revocation_epoch: None,
            reenrollment_required: true,
        },
        experiments: vec![PortalExperimentRow {
            display_name: "Native Only".into(),
            experiment_id: "exp-native".into(),
            revision_id: "rev-native".into(),
            has_head: false,
            estimated_window_seconds: 60,
        }],
        heads: Vec::new(),
        leaderboard: Vec::new(),
        artifact_rows: Vec::new(),
        metrics_panels: Vec::new(),
    });
    assert!(html.contains("Join currently requires pre-provisioned credentials"));
    assert!(html.contains("Browser peer join is not currently available"));
    assert!(html.contains("Social features are disabled for this deployment."));
    assert!(!html.contains("No login providers configured."));
    assert!(!html.contains("<h2>Leaderboard</h2>"));
    assert!(!html.contains("/leaderboard/signed"));
    assert!(!html.contains("No accepted receipts yet."));
    assert!(html.contains("Use the native client path"));
}

#[test]
fn artifact_run_and_head_pages_render_history_details() {
    let summaries_html = render_artifact_run_summaries_html(
        "exp-auth",
        &[PortalArtifactRunSummaryRow {
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

    let run_html = render_artifact_run_view_html(&PortalArtifactRunView {
        experiment_id: "exp-auth".into(),
        run_id: "run-auth".into(),
        latest_head_id: Some("head-auth-1".into()),
        heads: vec![PortalHeadRow {
            experiment_id: "exp-auth".into(),
            revision_id: "rev-auth".into(),
            head_id: "head-auth-1".into(),
            global_step: 12,
            created_at: "2026-04-03T12:00:00Z".into(),
        }],
        aliases: vec![PortalArtifactRow {
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
        alias_history: vec![PortalArtifactAliasHistoryRow {
            alias_name: "best_val/main/serve".into(),
            scope: "Run".into(),
            artifact_profile: "ServeCheckpoint".into(),
            head_id: "head-auth-0".into(),
            resolved_at: "2026-04-03T11:30:00Z".into(),
            source_reason: "BestValidation { eval_protocol_id: ContentId(\"main\") }".into(),
        }],
        eval_reports: vec![PortalHeadEvalSummaryRow {
            head_id: "head-auth-1".into(),
            eval_protocol_id: "main".into(),
            status: "Completed".into(),
            dataset_view_id: "view-auth".into(),
            sample_count: 128,
            metric_summary: "validation_loss=0.2500".into(),
            finished_at: "2026-04-03T12:00:00Z".into(),
        }],
        publications: vec![PortalPublishedArtifactRow {
            head_id: "head-auth-1".into(),
            artifact_profile: "ServeCheckpoint".into(),
            publication_target_id: "local-default".into(),
            status: "Ready".into(),
            object_key: "exp-auth/run-auth/latest-serve".into(),
            content_length: 4096,
            created_at: "2026-04-03T12:01:00Z".into(),
            expires_at: None,
        }],
        portal_path: "/portal".into(),
        json_view_path: "/artifacts/runs/exp-auth/run-auth".into(),
    });
    assert!(run_html.contains("Artifact run run-auth"));
    assert!(run_html.contains("Alias transition history"));
    assert!(run_html.contains("validation_loss=0.2500"));
    assert!(run_html.contains("exp-auth/run-auth/latest-serve"));

    let head_html = render_head_artifact_view_html(&PortalHeadArtifactView {
        head: PortalHeadRow {
            experiment_id: "exp-auth".into(),
            revision_id: "rev-auth".into(),
            head_id: "head-auth-1".into(),
            global_step: 12,
            created_at: "2026-04-03T12:00:00Z".into(),
        },
        experiment_id: "exp-auth".into(),
        run_id: "run-auth".into(),
        aliases: vec![PortalArtifactRow {
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
        alias_history: vec![PortalArtifactAliasHistoryRow {
            alias_name: "latest/serve".into(),
            scope: "Run".into(),
            artifact_profile: "ServeCheckpoint".into(),
            head_id: "head-auth-0".into(),
            resolved_at: "2026-04-03T11:00:00Z".into(),
            source_reason: "LatestCanonicalHead".into(),
        }],
        eval_reports: vec![PortalHeadEvalSummaryRow {
            head_id: "head-auth-1".into(),
            eval_protocol_id: "main".into(),
            status: "Completed".into(),
            dataset_view_id: "view-auth".into(),
            sample_count: 128,
            metric_summary: "validation_loss=0.2500".into(),
            finished_at: "2026-04-03T12:00:00Z".into(),
        }],
        publications: vec![PortalPublishedArtifactRow {
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
        portal_path: "/portal".into(),
        run_view_path: "/portal/artifacts/runs/exp-auth/run-auth".into(),
        json_view_path: "/artifacts/heads/head-auth-1".into(),
    });
    assert!(head_html.contains("Artifact head head-auth-1"));
    assert!(head_html.contains("Available profiles"));
    assert!(head_html.contains("BrowserSnapshot"));
    assert!(head_html.contains("/portal/artifacts/runs/exp-auth/run-auth"));
}
