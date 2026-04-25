//! Public facade for the `burn_p2p` runtime.
//!
//! This crate is the downstream entry point for native Burn applications. It exposes:
//!
//! - the project-family and workload integration traits
//! - node configuration and builder APIs
//! - stable identifiers, manifests, and runtime handles re-exported from the core crates
//!
//! The intended forward path is:
//!
//! 1. define one or more workloads through [`P2pWorkload`]
//! 2. group them under a [`P2pProjectFamily`]
//! 3. build a node with [`NodeBuilder`]
//! 4. bind the node to a network, workload, storage root, and auth/enrollment flow
//!
#![forbid(unsafe_code)]

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
};

use burn_p2p_core::time::{Duration, Instant};
use chrono::{DateTime, Utc};
use libp2p_identity::Keypair;
use semver::Version;
use serde::{Deserialize, Serialize};

mod app_view;
mod browser_join;
mod candidate;
mod candidate_screening;
mod config;
mod diloco;
mod edge_auth;
mod handles;
mod metrics_runtime;
mod node;
mod project_family;
mod promotion;
mod runtime_support;
mod training;
mod validation;

use handles::dedupe_peer_ids;

pub use app_view::{NodeAppSelection, build_node_app_view};
pub use browser_join::{BrowserJoinPolicy, browser_join_policy_for_entry};
pub use burn_p2p_checkpoint::{
    AggregateArtifactBytes, AggregateArtifactInput, AggregateArtifactRecord, ArtifactBuildSpec,
    CheckpointCatalog, CheckpointError, CheckpointLineage, ChunkingScheme, EmaFlow,
    FsArtifactStore, GarbageCollectionReport, MergeCandidate, MergePlan, SyncPlan, SyncRequest,
    materialize_aggregate_artifact_bytes,
};
pub use burn_p2p_core::{
    ActiveServiceSet, AdminMode, AggregateEnvelope, AggregateStats, AggregateTier, AppMode,
    ArtifactDescriptor, ArtifactId, ArtifactKind, ArtifactTargetKind, AssignmentLease,
    AuthProvider, BackpressurePolicy, BadgeAward, BadgeKind, BaseCheckpointId, BrowserCapability,
    BrowserEdgeSnapshot, BrowserLoginProvider, BrowserMode, BrowserRole, BrowserRolePolicy,
    BrowserVisibilityPolicy, CanaryEvalReport, CapabilityCard, CapabilityEstimate, ChunkId,
    ClientPlatform, ClientReleaseManifest, ClientReleaseManifestBuilder, CohortRobustnessReport,
    CompiledFeatureSet, ConfiguredServiceSet, ContentId, ContributionReceipt,
    ContributionReceiptId, ContributionRollup, DatasetId, DatasetManifest, DatasetView,
    DatasetViewId, DiLoCoAggregationPolicy, DiLoCoPolicy, DiLoCoRejoinPolicy, DiLoCoRequest,
    DiLoCoResponse, DiLoCoRoundFinalize, DiLoCoRoundHeartbeat, DiLoCoRoundOffer,
    DiLoCoStateSnapshot, DiLoCoTopologyMode, DiLoCoTopologyPolicy, DiffusionPromotionCertificate,
    DiffusionSteadyStatePolicy, EdgeAuthProvider, EdgeFeature, EdgeServiceManifest,
    EvalAggregationRule, EvalMetricDef, EvalProtocolManifest, EvalProtocolOptions,
    ExperimentDirectoryEntry, ExperimentId, ExperimentManifest, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, FlattenedTensorPack,
    GenesisSpec, GradientCodec, GroupId, HeadDescriptor, HeadEvalReport, HeadEvalStatus, HeadId,
    HeadPromotionMode, HeadPromotionPolicy, IdentityVisibility, LagPolicy, LagState,
    LeaderboardEntry, LeaderboardIdentity, LeaderboardSnapshot, LeaseId, MergeCertId,
    MergeCertificate, MergePolicy, MergeStrategy, MergeTopologyPolicy, MergeWindowMissPolicy,
    MergeWindowState, MetricScope, MetricTrustClass, MetricValue, MetricsLedgerSegment,
    MetricsMode, MetricsSnapshotManifest, MicroShardId, NetworkEstimate, NetworkId,
    NetworkManifest, NetworkManifestBuilder, NodeCertId, NodeCertificate, NodeCertificateClaims,
    OuterOptimizerPolicy, PeerAuthEnvelope, PeerId, PeerRole, PeerRoleSet, PeerWindowMetrics,
    PeerWindowPlacementHint, PeerWindowStatus, Precision, PrincipalId, ProfileMode,
    ProjectFamilyId, PseudoGradientChunk, PseudoGradientManifest, PseudoGradientManifestInput,
    ReducerAssignment, ReducerCohortMetrics, ReducerCohortStatus, ReducerLoadReport,
    ReductionCertificate, RejectionReason, ReleaseTrainManifest, RevisionId, RevisionManifest,
    RevocationEpoch, RobustnessAlert, RobustnessDecision, RobustnessPolicy, RobustnessPreset,
    RoundCursor, RoundId, RoundPhase, SignMajorityTieBreak, SignatureAlgorithm, SignatureMetadata,
    SignedPayload, SocialMode, SocialProfile, StateBlob, StudyId, SupportedWorkload,
    SupportedWorkloadBuilder, TargetArtifactManifest, TelemetrySummary,
    TrainerPromotionAttestation, TrainingProtocol, TrustScore, UpdateAnnounce, UpdateFeatureSketch,
    UpdateNormStats, ValidationQuorumCertificate, WindowActivation, WindowId, WorkloadId,
};
pub use burn_p2p_dataloader::{
    BurnDataLoaderAdapter, CachedMicroShard, CachedMicroShardLoader, DataReceiptBuilder,
    DataloaderError, DatasetRegistration, DatasetSizing, LeaseCache, LeasePlanner,
    LeasePlannerConfig, LeaseSelection, MicroShardPlan, MicroShardPlanner, MicroShardPlannerConfig,
    PlannedLease, ShardAwareSampler, ShardCache, ShardCostModel, ShardFetchEntry,
    ShardFetchManifest, UpstreamAdapter,
};
pub use burn_p2p_experiment::{
    ExperimentControlCommand, ExperimentDirectory, ExperimentDirectoryAccess,
    ExperimentDirectoryPolicyExt, ExperimentDirectoryProjectionBuilder, ExperimentLifecyclePhase,
    ExperimentLifecyclePlan, ExperimentLifecyclePlanBuilder, ExperimentSpec,
    FleetScheduleAssignment, FleetScheduleEpoch, FleetScheduleEpochBuilder, PatchClass,
    PatchSupport, PatchValue, RevisionCompatibility, RevisionSpec, RuntimePatch, StudySpec,
};
pub use burn_p2p_limits::{
    CapabilityCalibrator, CapabilityProbe, CapabilityResourceProbe, LimitPolicy, LimitProfile,
    LimitsError, LocalBackend, ObservedThroughputUpdate, WorkBudget,
    browser_probe_from_capabilities, corrected_work_units_per_second, native_probe_from_estimate,
    probe_from_profile,
};
pub use burn_p2p_security::{
    AdmissionDecision, AdmissionPolicy, AuditFinding, AuthError, CallbackPayload,
    ChallengeResponse, ClientManifest, DataAuditReport, IdentityConnector, LoginRequest,
    LoginStart, MergeEvidence, MergeEvidenceDecision, MergeEvidenceRequirement,
    NodeCertificateAuthority, NodeEnrollmentRequest, PeerAdmissionReport, PeerTrustLevel,
    PrincipalClaims, PrincipalSession, ReleaseManifest, ReleasePolicy, ReputationDecision,
    ReputationEngine, ReputationObservation, ReputationPolicy, ReputationState, SecurityError,
    StaticIdentityConnector, StaticPrincipalRecord, TrustedIssuer, UpdateAuditReport,
    ValidatorPolicy, create_peer_auth_envelope,
};
pub use burn_p2p_swarm::{
    AggregateProposalAnnouncement, AlertNotice, AlertSeverity, ArtifactChunkPayload,
    ArtifactProviderRecord, ArtifactSyncRequest, ArtifactSyncResponse, BrowserArtifactChunkRequest,
    BrowserArtifactManifestRequest, BrowserSeedDialCandidate, BrowserSwarmBootstrap,
    BrowserSwarmDialPlan, BrowserSwarmRuntime, ChunkFetchRequest, ChunkFetchResponse,
    ControlAnnouncement, ControlPlaneRequest, ControlPlaneResponse, ControlPlaneShell,
    ControlPlaneSnapshot, DiffusionPromotionCertificateAnnouncement,
    ExperimentDirectoryAnnouncement, ExperimentLifecycleAnnouncement, ExperimentOverlaySet,
    FleetScheduleAnnouncement, HeadAnnouncement, LeaseAnnouncement, LiveControlPlaneEvent,
    LiveSwarmEvent, MemoryControlPlaneShell, MemorySwarmShell, MergeAnnouncement,
    MergeWindowAnnouncement, MetricsAnnouncement, MicroShardFetchRequest, MicroShardFetchResponse,
    MicroShardProviderRecord, MigrationCoordinator, MigrationPlan, NativeControlPlaneShell,
    OverlayChannel, OverlayTopic, PeerAuthAnnouncement, PeerDirectoryAnnouncement, PeerObservation,
    PeerStore, PlannedBrowserSwarmRuntime, ProtocolId, ProtocolSet, ProviderPointer, PubsubPayload,
    ReducerAssignmentAnnouncement, ReducerLoadAnnouncement, ReductionCertificateAnnouncement,
    RuntimeBoundary, RuntimeEnvironment, RuntimeTransportPolicy, SwarmAddress, SwarmError,
    SwarmStats, TelemetryAnnouncement, TrainerPromotionAttestationAnnouncement, TransportKind,
    UpdateEnvelopeAnnouncement, ValidationQuorumAnnouncement,
    browser_transport_family_for_seed_url, native_browser_webrtc_direct_runtime_supported,
    native_browser_webtransport_gateway_runtime_supported, plan_browser_seed_dials,
};
pub use burn_p2p_workload::{
    ContinuousTrainerPolicy, DiLoCoInnerLoopReport, DiLoCoWorkload, DirectoryMetadataAttachment,
    EvalSplit, GeneratedWorkloadInputDescriptor, GeneratedWorkloadInputProvider, LeaseDataPipeline,
    LeaseDataPipelineDescriptor, LeaseDataPipelineKind, MergeModelCandidate, MetricReport,
    P2pWorkload, PatchOutcome, ReducerOutcome, TrainError, TrainerCanonicalReconcileStrategy,
    TrainingWindowOutcome, TrainingWindowTiming, ValidationCoordinationState,
    ValidationDriveOutcome, ValidationOutcome, WindowCtx, WindowReport, WorkloadExecutionStage,
    WorkloadInputSource, WorkloadTrainingBudget, WorkloadTrainingLease, WorkloadTrainingPlan,
    WorkloadTrainingProgress, WorkloadTrainingResult, WorkloadValidationPlan,
    WorkloadValidationProgress, WorkloadValidationResult, find_matching_directory_entry,
    find_matching_directory_entry_with_predicate, local_upstream_root,
    local_upstream_root_for_pipeline, standard_contribution_weight,
};
pub use config::{
    ArtifactTransferPhase, ArtifactTransferState, AuthConfig, ClientReenrollmentStatus,
    ControlHandle, DatasetConfig, IdentityConfig, MetricsRetentionBudget, MetricsRetentionConfig,
    MetricsRetentionPreset, NodeConfig, NodeRuntimeState, NodeTelemetrySnapshot, RuntimeStatus,
    SlotAssignmentState, SlotRuntimeState, StorageConfig, TelemetryHandle, TrustBundleState,
};
pub use diloco::{
    DiLoCoPeerContribution, DiLoCoReferenceCheckpoint, DiLoCoReferenceCoordinator,
    DiLoCoReferencePeer, DiLoCoReferenceRoundOutcome, DiLoCoRoundOutcome, EncodedPseudoGradient,
    aggregate_pseudo_gradients, average_pseudo_gradients, decode_pseudo_gradient,
    encode_pseudo_gradient,
};
pub use edge_auth::{
    EdgeAuthClient, EdgeAuthClientError, EdgeEnrollmentConfig, EdgeEnrollmentResult,
    EdgeLogoutResponse, EdgePeerEnrollmentRequest, EdgePeerIdentity, EdgeSessionState,
};
pub use handles::{
    CheckpointSyncHandle, ExperimentHandle, ExperimentOverlayTopics, MainnetHandle, RoleSet,
};
pub(crate) use node::TrainingPrefetchTask;
#[cfg(test)]
pub(crate) use node::prioritized_artifact_source_peers;
pub use node::{Node, NodeBuilder, RunningNode};
pub use project_family::{P2pProjectFamily, SelectedWorkloadProject, SingleWorkloadProjectFamily};
use runtime_support::{
    LagAssessment, assess_head_lag, cached_connected_snapshots, connected_peer_ids,
    effective_experiment_lifecycle_plan, effective_fleet_schedule_epoch, effective_limit_profile,
    experiment_has_lifecycle_plan, inferred_next_window_id, latest_head_from_snapshot,
    latest_merge_window_from_connected_snapshots, latest_merge_window_from_snapshot,
    latest_reducer_assignment_from_snapshot, load_head_state, load_known_peers,
    load_latest_merge_certificate, load_primary_slot_assignment, lock_telemetry_state,
    matches_experiment_head, merge_control_plane_snapshot, metric_quality,
    open_runtime_merge_window, persist_auth_state, persist_control_plane_state, persist_head_state,
    persist_in_flight_transfer_states, persist_json, persist_limit_profile,
    persist_primary_slot_assignment, persist_runtime_binding_state, persist_runtime_security_state,
    persist_window_id, prioritized_experiment_snapshot_peer_ids, remove_artifact_transfer_state,
    resolve_canonical_head, resolve_identity, restore_auth_config, restore_control_plane_state,
    restore_in_flight_transfer_states, restore_runtime_binding_config,
    restore_runtime_security_state, run_control_plane, runtime_assign_reducers,
    runtime_limit_policy, runtime_merge_topology_policy, runtime_robustness_policy,
    runtime_topology_peers, runtime_validator_peers, runtime_validators,
    set_effective_limit_profile, snapshots_with_local_control_plane,
    update_announces_from_connected_snapshots, update_feature_sketch_from_metrics,
    update_norm_stats, verify_snapshot_admission,
};
pub use training::{ContinuousTrainer, TrainingProtocolStepOutcome};

/// Public APIs for checkpoint.
pub mod checkpoint {
    pub use crate::{
        ArtifactBuildSpec, CheckpointCatalog, CheckpointError, CheckpointLineage,
        CheckpointSyncHandle, ChunkingScheme, EmaFlow, FsArtifactStore, GarbageCollectionReport,
        MergeCandidate, MergePlan, SyncPlan, SyncRequest,
    };
}

/// Public APIs for dataloader.
pub mod dataloader {
    pub use crate::{
        BurnDataLoaderAdapter, CachedMicroShard, CachedMicroShardLoader, DataReceiptBuilder,
        DataloaderError, DatasetRegistration, DatasetSizing, LeaseCache, LeasePlanner,
        LeasePlannerConfig, LeaseSelection, MicroShardPlan, MicroShardPlanner,
        MicroShardPlannerConfig, PlannedLease, ShardAwareSampler, ShardCache, ShardCostModel,
        ShardFetchEntry, ShardFetchManifest, UpstreamAdapter,
    };
}

/// Public APIs for experiment.
pub mod experiment {
    pub use crate::{
        ExperimentControlCommand, ExperimentDirectoryProjectionBuilder, ExperimentHandle,
        ExperimentLifecyclePlanBuilder, ExperimentOverlayTopics, ExperimentSpec,
        FleetScheduleEpoch, FleetScheduleEpochBuilder, PatchClass, PatchOutcome, PatchSupport,
        PatchValue, RevisionCompatibility, RevisionSpec, RuntimePatch, StudySpec,
    };
}

/// Public APIs for limits.
pub mod limits {
    pub use crate::{
        CapabilityCalibrator, CapabilityProbe, LimitPolicy, LimitProfile, LimitsError,
        LocalBackend, WorkBudget,
    };
}

/// Public APIs for security.
pub mod security {
    pub use crate::{
        AdmissionDecision, AuditFinding, ChallengeResponse, ClientManifest, DataAuditReport,
        MergeEvidence, MergeEvidenceDecision, MergeEvidenceRequirement, ReleaseManifest,
        ReleasePolicy, ReputationDecision, ReputationEngine, ReputationObservation,
        ReputationPolicy, ReputationState, SecurityError, UpdateAuditReport, ValidatorPolicy,
    };
}

#[cfg(feature = "burn")]
/// Burn-specific runtime integration helpers.
pub mod burn;

/// Public APIs for prelude.
pub mod prelude {
    #[cfg(feature = "burn")]
    pub use crate::burn;
    pub use crate::{
        AggregateArtifactBytes, AggregateArtifactInput, AggregateArtifactRecord, ArtifactBuildSpec,
        BurnDataLoaderAdapter, CachedMicroShard, CachedMicroShardLoader, CapabilityCalibrator,
        CapabilityProbe, CheckpointCatalog, CheckpointError, CheckpointLineage,
        CheckpointSyncHandle, ChunkingScheme, ControlHandle, DataReceiptBuilder, DataloaderError,
        DatasetConfig, DatasetRegistration, DatasetSizing, EmaFlow, EvalSplit,
        ExperimentControlCommand, ExperimentHandle, ExperimentOverlayTopics, ExperimentSpec,
        FleetScheduleEpoch, FleetScheduleEpochBuilder, FsArtifactStore, GarbageCollectionReport,
        IdentityConfig, LeaseCache, LeasePlanner, LeasePlannerConfig, LeaseSelection, LimitPolicy,
        LimitProfile, LimitsError, LocalBackend, MainnetHandle, MergeCandidate, MergePlan,
        MetricReport, MicroShardPlan, MicroShardPlanner, MicroShardPlannerConfig, Node,
        NodeBuilder, NodeConfig, NodeRuntimeState, NodeTelemetrySnapshot, P2pProjectFamily,
        P2pWorkload, PatchClass, PatchOutcome, PatchSupport, PatchValue, PlannedLease,
        ReducerOutcome, ReleaseManifest, ReleasePolicy, ReputationDecision, ReputationEngine,
        ReputationObservation, ReputationPolicy, ReputationState, RevisionCompatibility,
        RevisionSpec, RoleSet, RunningNode, RuntimePatch, RuntimeStatus, SecurityError,
        SelectedWorkloadProject, ShardAwareSampler, ShardCache, ShardCostModel, ShardFetchEntry,
        ShardFetchManifest, SingleWorkloadProjectFamily, SlotAssignmentState, SlotRuntimeState,
        StorageConfig, StudySpec, SyncPlan, SyncRequest, TelemetryHandle, TrainError,
        TrainingWindowOutcome, UpdateAuditReport, UpstreamAdapter, ValidationOutcome,
        ValidatorPolicy, WindowCtx, WindowReport, WorkBudget, checkpoint, dataloader, experiment,
        limits, materialize_aggregate_artifact_bytes, security,
    };
    pub use burn_p2p_core::{
        ArtifactDescriptor, ArtifactId, ArtifactKind, AssignmentLease, CapabilityCard,
        CapabilityEstimate, ChunkId, ClientReleaseManifest, ContentId, ContributionReceipt,
        ContributionReceiptId, DatasetId, DatasetManifest, DatasetView, DatasetViewId,
        ExperimentId, ExperimentManifest, GenesisSpec, HeadDescriptor, HeadId, LeaseId,
        MergeCertId, MergeCertificate, MergePolicy, MetricValue, MicroShardId, NetworkEstimate,
        NetworkId, NetworkManifest, PeerId, PeerRole, PeerRoleSet, Precision, ProjectFamilyId,
        RevisionId, RevisionManifest, StudyId, SupportedWorkload, TelemetrySummary,
        WindowActivation, WindowId, WorkloadId,
    };
}

/// Defines the slot state alias.
pub type SlotState = SlotRuntimeState;
/// Defines the catchup policy alias.
pub type CatchupPolicy = LagPolicy;

#[cfg(test)]
mod tests;
