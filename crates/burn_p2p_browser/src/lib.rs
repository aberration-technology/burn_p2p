//! Browser runtime, client, and worker support for burn_p2p.
#![forbid(unsafe_code)]

/// Browser-app state model for static wasm clients.
pub mod app;
/// Authentication and session helpers.
pub mod auth;
/// Browser UI and worker bridge types.
pub mod bridge;
/// Capability detection and reporting helpers.
pub mod capability;
/// Public conformance helpers for downstream browser integrations.
pub mod conformance;
/// Durable browser storage helpers.
pub mod durability;
/// Shared browser session-runtime helpers.
pub mod participant;
/// Runtime state and execution helpers.
pub mod runtime;
/// Static browser-shell site configuration helpers.
pub mod site_config;
/// Persistence and cache helpers.
pub mod storage;
/// Public APIs for training.
pub mod training;
/// Transport state and sync helpers.
pub mod transport;
/// Portal and browser-facing view bindings.
pub mod ui_bindings;
/// Public APIs for validation.
pub mod validation;
/// Public APIs for worker.
pub mod worker;

pub use app::{BrowserAppConnectConfig, BrowserAppController, BrowserAppModel, BrowserAppTarget};
pub use auth::{
    BrowserAuthClientError, BrowserEdgeClient, BrowserEnrollmentConfig, BrowserEnrollmentResult,
    BrowserLogoutResponse, BrowserPeerArtifactFetchFuture, BrowserPeerArtifactFetcher,
    BrowserPeerArtifactRequest, BrowserPeerEnrollmentRequest, BrowserSessionState,
    BrowserWorkerIdentity,
};
pub use bridge::{BrowserWorkerCommand, BrowserWorkerEvent};
pub use burn_p2p::WorkloadTrainingLease;
pub use burn_p2p::{
    EdgeAuthClient, EdgeAuthClientError, EdgeEnrollmentConfig, EdgeEnrollmentResult,
    EdgeLogoutResponse, EdgePeerEnrollmentRequest, EdgePeerIdentity, EdgeSessionState,
};
pub use burn_p2p_core::{
    BrowserArtifactSource, BrowserDirectorySnapshot, BrowserEdgeMode, BrowserEdgePaths,
    BrowserEdgeSnapshot, BrowserLeaderboardEntry, BrowserLeaderboardIdentity,
    BrowserLeaderboardSnapshot, BrowserLoginProvider, BrowserReceiptSubmissionResponse,
    BrowserResolvedSeedBootstrap, BrowserSeedBootstrapSource, BrowserSwarmPhase,
    BrowserSwarmStatus, BrowserTransportFamily, BrowserTransportObservationSource,
    BrowserTransportSurface, ReenrollmentStatus, TrustBundleExport,
};
pub use capability::{BrowserCapabilityReport, BrowserGpuSupport, BrowserWorkerSupport};
pub use conformance::{
    BrowserConformanceHarness, browser_conformance_capability_for_role,
    browser_conformance_directory, browser_conformance_session,
    browser_conformance_training_plan_with_lease, browser_conformance_transport,
};
pub use participant::{
    BrowserSessionRuntimeConfig, BrowserSessionRuntimeError, BrowserSessionRuntimeHandle,
    BrowserSessionTrainingOutcome,
};
pub use runtime::{
    BrowserJoinStage, BrowserRuntimeConfig, BrowserRuntimeRole, BrowserRuntimeState,
};
pub use site_config::BrowserSiteBootstrapConfig;
pub use storage::{
    BrowserArtifactReplayBytePrefix, BrowserArtifactReplayByteSegment,
    BrowserArtifactReplayCheckpoint, BrowserArtifactReplayChunk, BrowserArtifactReplayChunkStorage,
    BrowserReceiptOutbox, BrowserReceiptOutboxBackend, BrowserStorageSnapshot,
    BrowserStoredAssignment,
};
pub use training::{
    BrowserTrainingBudget, BrowserTrainingPlan, BrowserTrainingProgress, BrowserTrainingResult,
};
pub use transport::{
    BrowserTransportKind, BrowserTransportPolicy, BrowserTransportStatus, browser_transport_family,
    browser_transport_kind, browser_transport_label_from_family,
    browser_transport_label_from_swarm_status, resolve_browser_seed_bootstrap,
};
pub use ui_bindings::{
    BrowserAppUiState, BrowserEdgeEndpoints, BrowserExperimentCandidate, BrowserUiBindings,
    browser_app_ui_state_from_directory, browser_experiment_candidate_for_selection,
    browser_experiment_candidates_for_scopes, browser_experiment_candidates_from_directory,
    browser_experiment_picker_view_from_directory, browser_join_policies_from_directory,
    recommended_browser_candidate_for_scopes, recommended_browser_join_policy,
    recommended_browser_join_policy_for_scopes, recommended_browser_runtime_state,
    recommended_browser_runtime_state_for_scopes,
};
pub use validation::{BrowserValidationPlan, BrowserValidationProgress, BrowserValidationResult};
pub use worker::{BrowserMetricsSyncState, BrowserWorkerRuntime};

#[cfg(test)]
mod tests;
