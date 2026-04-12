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
mod assertions;
mod models;
#[cfg(all(not(target_arch = "wasm32"), feature = "desktop-client"))]
mod native_client;
mod render;
#[cfg(all(target_arch = "wasm32", feature = "web-client"))]
mod web_client;
mod widgets;

pub use assertions::{
    assert_browser_client_selected_experiment, assert_experiment_picker_contains_allowed_revision,
    assert_lifecycle_assignment_matches, assert_participant_has_receipts,
    assert_training_result_complete, assert_transport_health_ready,
};
pub use models::{
    AppArtifactAliasHistoryRow, AppArtifactRow, AppArtifactRunSummaryRow, AppArtifactRunView,
    AppDiagnosticsView, AppExperimentRow, AppHeadArtifactView, AppHeadEvalSummaryRow, AppHeadRow,
    AppLeaderboardRow, AppLoginProvider, AppMetricRow, AppMetricsPanel,
    AppOperatorAuditFacetSummaryView, AppOperatorAuditPageView, AppOperatorAuditRow,
    AppOperatorAuditSummaryView, AppOperatorControlReplayPageView, AppOperatorControlReplayRow,
    AppOperatorControlReplaySummaryView, AppOperatorFacetBucketView, AppOperatorReplayPageView,
    AppOperatorReplaySnapshotRow, AppPaths, AppPeerStatusRow, AppPublishedArtifactRow,
    AppRuntimeStateCard, AppServiceStatusRow, AppSnapshotView, AppTransportSurface, AppTrustView,
};
#[cfg(all(not(target_arch = "wasm32"), feature = "desktop-client"))]
pub use native_client::{NodeAppHostConfig, NodeAppHostSource, launch_node_app};
pub use render::{
    browser_app_stylesheet, render_artifact_run_summaries_html, render_artifact_run_view_html,
    render_browser_app_static_html, render_browser_app_static_html_with_config,
    render_dashboard_html, render_head_artifact_view_html, render_operator_audit_html,
    render_operator_control_replay_html, render_operator_replay_html,
};
pub use widgets::{
    AuthSessionCard, ContributionReceiptSummaryPanel, ExperimentRevisionSelector,
    LifecycleAssignmentStatusCard, RuntimeCapabilityCard, TrainingResultPanel,
    TransportHealthPanel,
};

#[cfg(test)]
mod tests;
