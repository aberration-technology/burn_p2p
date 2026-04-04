//! Reference portal rendering and view composition for burn_p2p edge deployments.
//!
//! This crate intentionally stays lightweight. It owns the reference, server-rendered
//! portal surface and the typed snapshot models that `burn_p2p_bootstrap` maps into.
//! It is useful for diagnostics, deployment validation, and a baseline browser-facing
//! product surface, but it is not the core runtime or trust model.
#![forbid(unsafe_code)]

mod models;
mod render;

pub use models::{
    PortalArtifactAliasHistoryRow, PortalArtifactRow, PortalArtifactRunSummaryRow,
    PortalArtifactRunView, PortalDiagnosticsView, PortalExperimentRow, PortalHeadArtifactView,
    PortalHeadEvalSummaryRow, PortalHeadRow, PortalLeaderboardRow, PortalLoginProvider,
    PortalMetricRow, PortalMetricsPanel, PortalPaths, PortalPublishedArtifactRow,
    PortalSnapshotView, PortalTransportSurface, PortalTrustView,
};
pub use render::{
    render_artifact_run_summaries_html, render_artifact_run_view_html, render_browser_portal_html,
    render_dashboard_html, render_head_artifact_view_html,
};

#[cfg(test)]
mod tests;
