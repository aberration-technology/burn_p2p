mod browser_app;
mod operator_app;

use crate::models::{PortalArtifactRunSummaryRow, PortalArtifactRunView, PortalHeadArtifactView};

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

/// Renders a run-summary page for one experiment's artifact history.
pub fn render_artifact_run_summaries_html(
    experiment_id: &str,
    rows: &[PortalArtifactRunSummaryRow],
    portal_path: &str,
) -> String {
    operator_app::render_artifact_run_summaries_html(experiment_id, rows, portal_path)
}

/// Renders one run-scoped artifact history/detail page.
pub fn render_artifact_run_view_html(view: &PortalArtifactRunView) -> String {
    operator_app::render_artifact_run_view_html(view)
}

/// Renders one head-scoped artifact detail page.
pub fn render_head_artifact_view_html(view: &PortalHeadArtifactView) -> String {
    operator_app::render_head_artifact_view_html(view)
}
