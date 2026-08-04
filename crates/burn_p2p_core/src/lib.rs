//! Core schema, identifier, and wire-model types shared across the `burn_p2p` workspace.
//!
//! This crate intentionally stays lightweight so higher-level runtime, browser, and bootstrap
//! crates can depend on a single canonical set of IDs, manifests, and transport payloads.
#![forbid(unsafe_code)]

/// Canonical encoding and content-addressing helpers.
pub mod codec;
/// Shared diagnostic helpers used to trim noisy runtime surfaces.
pub mod diagnostics;
/// Typed identifiers used across the workspace.
pub mod id;
/// Serializable protocol schemas and manifest types.
pub mod schema;
/// Cross-target time helpers for native and wasm runtimes.
pub mod time;

pub use codec::{CanonicalSchema, SchemaError, deterministic_cbor, from_cbor_slice};
pub use diagnostics::{
    is_benign_operator_runtime_error, operator_visible_last_error,
    operator_visible_last_error_with_active_transport,
};
pub use id::{
    ArtifactAliasId, ArtifactId, CapabilityCardId, ChunkId, ContentId, ContributionReceiptId,
    ControlCertId, DatasetId, DatasetViewId, DownloadTicketId, ExperimentId, ExportJobId, HeadId,
    LeaseId, MergeCertId, MicroShardId, NetworkId, NodeCertId, PeerId, PrincipalId,
    ProjectFamilyId, PublicationTargetId, PublishedArtifactId, RevisionId, RunId, StudyId,
    WorkloadId,
};
pub use schema::{
    ActiveServiceSet, AdminMode, AggregateEnvelope, AggregateStats, AggregateTier,
    AggregationPolicy, AggregationStrategy, AppMode, ArtifactAlias, ArtifactAliasScope,
    ArtifactAliasSourceReason, ArtifactDescriptor, ArtifactKind, ArtifactLiveEvent,
    ArtifactLiveEventKind, ArtifactProfile, ArtifactTargetKind, AssignmentLease, AttestationLevel,
    AuthPolicySnapshot, AuthProvider, AuthorityEpochManifest, AuthorityEvidenceCategory,
    AuthorityEvidenceRecord, BackendClass, BackpressurePolicy, BadgeAward, BadgeKind,
    BaseCheckpointId, BrowserArtifactRouteKind, BrowserArtifactSource,
    BrowserArtifactSyncDiagnostics, BrowserCapability, BrowserDirectorySnapshot, BrowserEdgeMode,
    BrowserEdgePaths, BrowserEdgeSnapshot, BrowserLeaderboardEntry, BrowserLeaderboardIdentity,
    BrowserLeaderboardSnapshot, BrowserLoginProvider, BrowserMode,
    BrowserReceiptSubmissionResponse, BrowserResolvedSeedBootstrap, BrowserRole, BrowserRolePolicy,
    BrowserSeedAdvertisement, BrowserSeedBootstrapSource, BrowserSeedRecord,
    BrowserSeedTransportKind, BrowserSeedTransportPolicy, BrowserSwarmPhase, BrowserSwarmStatus,
    BrowserTransportFamily, BrowserTransportObservationSource, BrowserTransportSurface,
    BrowserVisibilityPolicy, COMPACT_UPDATE_PAYLOAD_VERSION, CanaryEvalReport,
    CanaryMetricDirection, CanaryMetricGate, CanaryMetricGateFailure, CanaryMetricGateResult,
    CapabilityCard, CapabilityClass, CapabilityEstimate, ChunkDescriptor, ClientPlatform,
    ClientReleaseManifest, ClientReleaseManifestBuilder, ClippingPolicy, CohortFilterPolicy,
    CohortFilterStrategy, CohortRobustnessReport, CompactScalarEncoding, CompactScalarVector,
    CompactUpdateBody, CompactUpdatePayload, CompiledFeatureSet, ConfiguredServiceSet,
    ContributionReceipt, ContributionRollup, ControlCertificate, DataReceipt, DatasetManifest,
    DatasetView, DiLoCoAggregateReady, DiLoCoAggregateSlice, DiLoCoAggregationPolicy,
    DiLoCoGradientSlice, DiLoCoPolicy, DiLoCoRejoinPolicy, DiLoCoRequest, DiLoCoResponse,
    DiLoCoRoundFinalize, DiLoCoRoundHeartbeat, DiLoCoRoundOffer, DiLoCoStateBundle,
    DiLoCoStateSnapshot, DiLoCoTopologyMode, DiLoCoTopologyPolicy, DiffusionPromotionCertificate,
    DiffusionSteadyStatePolicy, DownloadDeliveryMode, DownloadTicket, EdgeAuthProvider,
    EdgeFeature, EdgeServiceManifest, EscalationPolicy, EvalAggregationRule, EvalMetricDef,
    EvalProtocolManifest, EvalProtocolOptions, ExperimentDirectoryEntry, ExperimentManifest,
    ExperimentOptInPolicy, ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility,
    ExportJob, ExportJobStatus, FlattenedTensorPack, FleetPlacementPeer, FleetPlacementSnapshot,
    GenesisMaterialization, GenesisSpec, GradientCodec, GroupId, HardRejectPolicy, HeadDescriptor,
    HeadEvalReport, HeadEvalStatus, HeadEvaluationBinding, HeadPromotionMode, HeadPromotionPolicy,
    IdentityVisibility, LagPolicy, LagState, LeaderboardEntry, LeaderboardIdentity,
    LeaderboardSnapshot, LocalOptimizerStatePolicy, MODEL_GENESIS_SIGNATURE_KEY_ID,
    MergeCertificate, MergePolicy, MergeStrategy, MergeTopologyPolicy, MergeWindowMissPolicy,
    MergeWindowState, MetricScope, MetricTrustClass, MetricValue, MetricsLedgerSegment,
    MetricsLiveEvent, MetricsLiveEventKind, MetricsMode, MetricsSnapshotManifest,
    MetricsSyncCursor, MicroShard, ModelGenesisManifest, NetworkCompatibilityError,
    NetworkEstimate, NetworkManifest, NetworkManifestBuilder, NodeCertificate,
    NodeCertificateClaims, OuterOptimizerPolicy, PARAMETER_SUBSET_CATALOG_VERSION, Page,
    PageRequest, ParameterSubsetCatalog, ParameterSubsetEntry, PeerAuthEnvelope, PeerRole,
    PeerRoleSet, PeerWindowMetrics, PeerWindowPlacementHint, PeerWindowStatus, PersistenceClass,
    Precision, ProfileMode, PseudoGradientChunk, PseudoGradientManifest,
    PseudoGradientManifestInput, PublicationAccessMode, PublicationMode, PublicationTarget,
    PublicationTargetKind, PublishedArtifactRecord, PublishedArtifactStatus, QuarantinePolicy,
    REVISION_CONTRACT_SIGNATURE_KEY_ID, RecurrentStatePolicy, ReducerAssignment,
    ReducerCohortMetrics, ReducerCohortStatus, ReducerLoadReport, ReductionCertificate,
    ReductionCertificateError, ReenrollmentStatus, RejectionReason, ReleaseTrainManifest,
    ReputationPolicy, RequestFailureCounter, RequestFailureKind, RequestFailureOperation,
    RequestFailureReason, RevisionContractAuthorityPayload, RevisionContractBundle,
    RevisionManifest, RevocationEpoch, RobustnessAlert, RobustnessAlertSeverity,
    RobustnessDecision, RobustnessPolicy, RobustnessPreset, RoundCursor, RoundId, RoundPhase,
    SCHEMA_VERSION, SchedulerStatePolicy, SchemaEnvelope, ScreeningPolicy, SeededFitnessGeneration,
    SeededFitnessReplayPolicy, SignMajorityTieBreak, SignatureAlgorithm, SignatureMetadata,
    SignedModelGenesisManifest, SignedPayload, SocialMode, SocialProfile, StateBlob,
    SupportedWorkload, SupportedWorkloadBuilder, TRAINING_CONTRACT_VERSION, TargetArtifactManifest,
    TelemetrySummary, TrainerPromotionAttestation, TrainingContractError, TrainingContractManifest,
    TrainingProtocol, TrainingProtocolValidationError, TrustBundleExport, TrustScore,
    TrustedIssuerStatus, UpdateAnnounce, UpdateCodec, UpdateFeatureSketch, UpdateNormStats,
    UpdateReplayStats, ValidatedUpdateEvidence, ValidationQuorumCertificate,
    ValidationQuorumCertificateError, ValidatorCanaryPolicy, ValidatorSetManifest,
    ValidatorSetMember, WindowActivation, WindowId, WorkDisposition, WorkloadUpdateEnvelope,
};
