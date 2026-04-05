//! Shared UI view models and typed contracts for burn_p2p surfaces.
//!
//! This crate is intentionally presentation-oriented. It holds serializable view
//! models that map runtime, bootstrap, browser, and social state into frontend-
//! friendly payloads without owning product rendering or business logic.
#![forbid(unsafe_code)]

mod auth;
mod browser_app;
mod dashboard;
mod robustness;

pub use auth::{
    AuthPortalView, BrowserExperimentPickerCard, BrowserExperimentPickerState,
    BrowserExperimentPickerView, CheckpointDownload, ContributionIdentityPanel,
    ExperimentPickerCard, ExperimentPickerView, GitHubProfileLink, LoginProviderView,
    ParticipantPortalView, ParticipantProfile, TrustBadgeView,
};
pub use browser_app::{
    BrowserAppClientView, BrowserAppExperimentSummary, BrowserAppFocusPanel,
    BrowserAppLeaderboardPreview, BrowserAppLiveView, BrowserAppNetworkView, BrowserAppRouteLink,
    BrowserAppShellView, BrowserAppStaticBootstrap, BrowserAppSummaryCard, BrowserAppSurface,
    BrowserAppSurfaceTab, BrowserAppTrainingView, BrowserAppValidationView, BrowserAppViewerView,
};
pub use dashboard::{
    AggregateDagEdge, AggregateDagNode, AggregateDagView, AuthorityActionRecord, CheckpointDagEdge,
    CheckpointDagEdgeKind, CheckpointDagNode, CheckpointDagView, CostPerformancePoint, EmaFlowStep,
    EmaFlowView, ExperimentMigrationView, ExperimentVariantView, HeadPromotionTimelineEntry,
    MergeQueueEntry, MergeQueueStatus, MergeTopologyDashboardView, MergeWindowView, MetricPoint,
    OperatorConsoleView, OperatorDiagnosticsView, OperatorPeerDiagnosticView,
    OperatorRobustnessSummaryView, OperatorTransferView, OverlayStatusView, ReducerUtilizationView,
    ShardAssignmentCell, ShardAssignmentHeatmap, StudyBoardView, UiChannel, UiEventEnvelope,
    UiPayload,
};
pub use robustness::{
    CanaryRegressionView, QuarantinedPeerView, RobustnessPanelView, RobustnessReasonCountView,
    TrustScorePointView,
};

#[cfg(test)]
mod tests;
