use crate::{
    PortalArtifactAliasHistoryRow, PortalArtifactRow, PortalArtifactRunSummaryRow,
    PortalArtifactRunView, PortalHeadArtifactView, PortalHeadEvalSummaryRow, PortalHeadRow,
    PortalPublishedArtifactRow, render_artifact_run_summaries_html, render_artifact_run_view_html,
    render_browser_app_static_html, render_dashboard_html, render_head_artifact_view_html,
};
use burn_p2p_ui::{BrowserAppStaticBootstrap, BrowserAppSurface};

#[test]
fn dashboard_html_mentions_bootstrap_routes() {
    let html = render_dashboard_html("mainnet");
    assert!(html.contains("/portal/snapshot"));
    assert!(html.contains("/diagnostics/bundle"));
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
