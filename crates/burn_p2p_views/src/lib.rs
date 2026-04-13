//! Shared view models and typed frontend contracts for burn_p2p surfaces.
//!
//! This crate is intentionally presentation-oriented. It holds serializable view
//! models that map runtime, bootstrap, browser, and social state into frontend-
//! friendly payloads without owning rendering or product logic.
#![forbid(unsafe_code)]

mod auth;
mod browser_app;
mod dashboard;
mod operator_rollout;
mod robustness;
mod training;

pub use auth::{
    AuthAppView, BrowserExperimentPickerCard, BrowserExperimentPickerState,
    BrowserExperimentPickerView, CheckpointDownload, ContributionIdentityPanel,
    ExperimentPickerCard, ExperimentPickerView, GitHubProfileLink, LoginProviderView,
    ParticipantAppView, ParticipantProfile, TrustBadgeView,
};
pub use browser_app::{
    BrowserAppClientView, BrowserAppDiffusionView, BrowserAppExperimentSummary,
    BrowserAppFocusPanel, BrowserAppLeaderboardPreview, BrowserAppLiveView,
    BrowserAppMetricPreview, BrowserAppNetworkView, BrowserAppPerformanceView, BrowserAppRouteLink,
    BrowserAppShellView, BrowserAppStaticBootstrap, BrowserAppSummaryCard, BrowserAppSurface,
    BrowserAppSurfaceTab, BrowserAppTrainingLeaseView, BrowserAppTrainingView,
    BrowserAppValidationView, BrowserAppViewerView, NodeAppClientView, NodeAppDiffusionView,
    NodeAppExperimentSummary, NodeAppLeaderboardPreview, NodeAppMetricPreview, NodeAppNetworkView,
    NodeAppPerformanceView, NodeAppShellView, NodeAppStaticBootstrap, NodeAppSurface,
    NodeAppTrainingLeaseView, NodeAppTrainingView, NodeAppValidationView, NodeAppViewerView,
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
pub use operator_rollout::{
    AdminSessionSummaryView, DirectoryEntryDraftView, DirectoryEntryMetadataView,
    DirectoryMutationResultView, ExperimentDirectoryEntryView, ExperimentDirectoryListView,
    RolloutPreviewView,
};
pub use robustness::{
    CanaryRegressionView, QuarantinedPeerView, RobustnessPanelView, RobustnessReasonCountView,
    TrustScorePointView,
};
pub use training::{
    LifecycleAssignmentStatusView, ParticipationTrustSummaryView, RuntimeCapabilitySummaryView,
    TrainingBudgetSummaryView, TrainingProgressSummaryView, TrainingResultSummaryView,
    ValidationProgressSummaryView, ValidationResultSummaryView,
};

#[cfg(test)]
mod tests;
