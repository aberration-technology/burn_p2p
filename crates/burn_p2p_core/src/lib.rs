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
pub use diagnostics::{is_benign_operator_runtime_error, operator_visible_last_error};
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
    BrowserArtifactSource, BrowserCapability, BrowserDirectorySnapshot, BrowserEdgeMode,
    BrowserEdgePaths, BrowserEdgeSnapshot, BrowserLeaderboardEntry, BrowserLeaderboardIdentity,
    BrowserLeaderboardSnapshot, BrowserLoginProvider, BrowserMode,
    BrowserReceiptSubmissionResponse, BrowserResolvedSeedBootstrap, BrowserRole, BrowserRolePolicy,
    BrowserSeedAdvertisement, BrowserSeedBootstrapSource, BrowserSeedRecord,
    BrowserSeedTransportKind, BrowserSeedTransportPolicy, BrowserSwarmPhase, BrowserSwarmStatus,
    BrowserTransportFamily, BrowserTransportObservationSource, BrowserTransportSurface,
    BrowserVisibilityPolicy, CanaryEvalReport, CapabilityCard, CapabilityClass, CapabilityEstimate,
    ChunkDescriptor, ClientPlatform, ClientReleaseManifest, ClientReleaseManifestBuilder,
    ClippingPolicy, CohortFilterPolicy, CohortFilterStrategy, CohortRobustnessReport,
    CompiledFeatureSet, ConfiguredServiceSet, ContributionReceipt, ContributionRollup,
    ControlCertificate, DataReceipt, DatasetManifest, DatasetView, DiffusionPromotionCertificate,
    DiffusionSteadyStatePolicy, DownloadDeliveryMode, DownloadTicket, EdgeAuthProvider,
    EdgeFeature, EdgeServiceManifest, EscalationPolicy, EvalAggregationRule, EvalMetricDef,
    EvalProtocolManifest, EvalProtocolOptions, ExperimentDirectoryEntry, ExperimentManifest,
    ExperimentOptInPolicy, ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility,
    ExportJob, ExportJobStatus, FleetPlacementPeer, FleetPlacementSnapshot, GenesisSpec,
    HardRejectPolicy, HeadDescriptor, HeadEvalReport, HeadEvalStatus, HeadPromotionMode,
    HeadPromotionPolicy, IdentityVisibility, LagPolicy, LagState, LeaderboardEntry,
    LeaderboardIdentity, LeaderboardSnapshot, MergeCertificate, MergePolicy, MergeStrategy,
    MergeTopologyPolicy, MergeWindowMissPolicy, MergeWindowState, MetricScope, MetricTrustClass,
    MetricValue, MetricsLedgerSegment, MetricsLiveEvent, MetricsLiveEventKind, MetricsMode,
    MetricsSnapshotManifest, MetricsSyncCursor, MicroShard, NetworkEstimate, NetworkManifest,
    NetworkManifestBuilder, NodeCertificate, NodeCertificateClaims, Page, PageRequest,
    PeerAuthEnvelope, PeerRole, PeerRoleSet, PeerWindowMetrics, PeerWindowPlacementHint,
    PeerWindowStatus, PersistenceClass, Precision, ProfileMode, PublicationAccessMode,
    PublicationMode, PublicationTarget, PublicationTargetKind, PublishedArtifactRecord,
    PublishedArtifactStatus, QuarantinePolicy, ReducerAssignment, ReducerCohortMetrics,
    ReducerCohortStatus, ReducerLoadReport, ReductionCertificate, ReenrollmentStatus,
    RejectionReason, ReleaseTrainManifest, ReputationPolicy, RevisionManifest, RevocationEpoch,
    RobustnessAlert, RobustnessAlertSeverity, RobustnessDecision, RobustnessPolicy,
    RobustnessPreset, SCHEMA_VERSION, SchemaEnvelope, ScreeningPolicy, SignatureAlgorithm,
    SignatureMetadata, SignedPayload, SocialMode, SocialProfile, SupportedWorkload,
    SupportedWorkloadBuilder, TargetArtifactManifest, TelemetrySummary,
    TrainerPromotionAttestation, TrustBundleExport, TrustScore, TrustedIssuerStatus,
    UpdateAnnounce, UpdateFeatureSketch, UpdateNormStats, ValidationQuorumCertificate,
    ValidatorCanaryPolicy, ValidatorSetManifest, ValidatorSetMember, WindowActivation, WindowId,
    WorkDisposition,
};
