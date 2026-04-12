mod browser_app;
mod operator_app;

use crate::{
    AppArtifactRunSummaryRow, AppArtifactRunView, AppHeadArtifactView,
    AppOperatorControlReplayPageView,
};

/// Renders the operator-facing bootstrap dashboard HTML.
pub fn render_dashboard_html(network_id: &str) -> String {
    operator_app::render_dashboard_html(network_id)
}

/// Returns the shared browser-app stylesheet shipped with the static wasm shell.
pub fn browser_app_stylesheet() -> &'static str {
    browser_app::browser_app_stylesheet()
}

/// Renders a static browser-app shell for CDN-served wasm clients.
pub fn render_browser_app_static_html(
    bootstrap: &burn_p2p_views::BrowserAppStaticBootstrap,
) -> String {
    browser_app::render_browser_app_static_html(bootstrap)
}

/// Renders a static browser-app shell without requiring callers to pass
/// `burn_p2p_views` types across crate boundaries.
pub fn render_browser_app_static_html_with_config(
    app_name: &str,
    asset_base_url: &str,
    module_entry_path: &str,
    stylesheet_path: Option<&str>,
    default_edge_url: Option<&str>,
    default_surface_key: &str,
    refresh_interval_ms: u64,
) -> String {
    browser_app::render_browser_app_static_html_with_config(
        app_name,
        asset_base_url,
        module_entry_path,
        stylesheet_path,
        default_edge_url,
        default_surface_key,
        refresh_interval_ms,
    )
}

/// Renders a run-summary page for one experiment's artifact history.
pub fn render_artifact_run_summaries_html(
    experiment_id: &str,
    rows: &[AppArtifactRunSummaryRow],
    app_path: &str,
) -> String {
    operator_app::render_artifact_run_summaries_html(experiment_id, rows, app_path)
}

/// Renders one run-scoped artifact history/detail page.
pub fn render_artifact_run_view_html(view: &AppArtifactRunView) -> String {
    operator_app::render_artifact_run_view_html(view)
}

/// Renders one head-scoped artifact detail page.
pub fn render_head_artifact_view_html(view: &AppHeadArtifactView) -> String {
    operator_app::render_head_artifact_view_html(view)
}

/// Renders one operator-facing lifecycle and schedule replay page.
pub fn render_operator_control_replay_html(view: &AppOperatorControlReplayPageView) -> String {
    operator_app::render_operator_control_replay_html(view)
}
