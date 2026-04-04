//! Shared UI view models and typed contracts for burn_p2p surfaces.
//!
//! This crate is intentionally presentation-oriented. It holds serializable view
//! models that map runtime, bootstrap, browser, and social state into frontend-
//! friendly payloads without owning product rendering or business logic.
#![forbid(unsafe_code)]

mod auth;
mod dashboard;

pub use auth::{
    AuthPortalView, BrowserExperimentPickerCard, BrowserExperimentPickerState,
    BrowserExperimentPickerView, CheckpointDownload, ContributionIdentityPanel,
    ExperimentPickerCard, ExperimentPickerView, GitHubProfileLink, LoginProviderView,
    ParticipantPortalView, ParticipantProfile, TrustBadgeView,
};
pub use dashboard::{
    AggregateDagEdge, AggregateDagNode, AggregateDagView, AuthorityActionRecord, CheckpointDagEdge,
    CheckpointDagEdgeKind, CheckpointDagNode, CheckpointDagView, CostPerformancePoint, EmaFlowStep,
    EmaFlowView, ExperimentMigrationView, ExperimentVariantView, HeadPromotionTimelineEntry,
    MergeQueueEntry, MergeQueueStatus, MergeTopologyDashboardView, MergeWindowView, MetricPoint,
    OperatorConsoleView, OperatorDiagnosticsView, OperatorPeerDiagnosticView, OperatorTransferView,
    OverlayStatusView, ReducerUtilizationView, ShardAssignmentCell, ShardAssignmentHeatmap,
    StudyBoardView, UiChannel, UiEventEnvelope, UiPayload,
};

#[cfg(test)]
mod tests;
