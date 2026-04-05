//! Reference browser/native app assets and operator rendering for burn_p2p.
//!
//! This crate intentionally stays lightweight. It owns:
//! - a static browser-app shell contract for CDN-served wasm clients
//! - one shared dioxus component tree reused by web and native hosts
//! - artifact and dashboard pages used by the bootstrap edge
//! - typed snapshot models used by test and capture tooling
//!
//! Native desktop hosting is available behind `desktop-client`.
//! Browser wasm hosting is available behind `web-client`.
#![forbid(unsafe_code)]

#[cfg(any(
    all(target_arch = "wasm32", feature = "web-client"),
    all(not(target_arch = "wasm32"), feature = "desktop-client")
))]
mod app;
mod models;
#[cfg(all(not(target_arch = "wasm32"), feature = "desktop-client"))]
mod native_client;
mod render;
#[cfg(all(target_arch = "wasm32", feature = "web-client"))]
mod web_client;

pub use models::{
    PortalArtifactAliasHistoryRow, PortalArtifactRow, PortalArtifactRunSummaryRow,
    PortalArtifactRunView, PortalDiagnosticsView, PortalExperimentRow, PortalHeadArtifactView,
    PortalHeadEvalSummaryRow, PortalHeadRow, PortalLeaderboardRow, PortalLoginProvider,
    PortalMetricRow, PortalMetricsPanel, PortalPaths, PortalPeerStatusRow,
    PortalPublishedArtifactRow, PortalRuntimeStateCard, PortalServiceStatusRow, PortalSnapshotView,
    PortalTransportSurface, PortalTrustView,
};
#[cfg(all(not(target_arch = "wasm32"), feature = "desktop-client"))]
pub use native_client::{NodeAppHostConfig, NodeAppHostSource, launch_node_app};
pub use render::{
    browser_app_stylesheet, render_artifact_run_summaries_html, render_artifact_run_view_html,
    render_browser_app_static_html, render_dashboard_html, render_head_artifact_view_html,
};

#[cfg(test)]
mod tests;
