//! Browser-app shell assets and operator artifact/dashboard rendering for burn_p2p.
//!
//! This crate intentionally stays lightweight. It owns:
//! - a static browser-app shell contract for CDN-served wasm clients
//! - artifact and dashboard pages used by the bootstrap edge
//! - typed snapshot models used by test and capture tooling
//!
//! The static-browser direction is the browser architecture. This crate does not own
//! a server-rendered browser portal.
#![forbid(unsafe_code)]

mod models;
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
pub use render::{
    browser_app_stylesheet, render_artifact_run_summaries_html, render_artifact_run_view_html,
    render_browser_app_static_html, render_dashboard_html, render_head_artifact_view_html,
};

#[cfg(test)]
mod tests;
