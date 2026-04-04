//! Core schema, identifier, and wire-model types shared across the `burn_p2p` workspace.
//!
//! This crate intentionally stays lightweight so higher-level runtime, browser, and bootstrap
//! crates can depend on a single canonical set of IDs, manifests, and transport payloads.
#![forbid(unsafe_code)]

/// Canonical encoding and content-addressing helpers.
pub mod codec;
/// Typed identifiers used across the workspace.
pub mod id;
/// Serializable protocol schemas and manifest types.
pub mod schema;

pub use codec::{CanonicalSchema, SchemaError, deterministic_cbor, from_cbor_slice};
pub use id::{
    ArtifactAliasId, ArtifactId, CapabilityCardId, ChunkId, ContentId, ContributionReceiptId,
    ControlCertId, DatasetId, DatasetViewId, DownloadTicketId, ExperimentId, ExportJobId, HeadId,
    LeaseId, MergeCertId, MicroShardId, NetworkId, NodeCertId, PeerId, PrincipalId,
    ProjectFamilyId, PublicationTargetId, PublishedArtifactId, RevisionId, RunId, StudyId,
    WorkloadId,
};
pub use schema::{
    ActiveServiceSet, AdminMode, AggregateEnvelope, AggregateStats, AggregateTier, ArtifactAlias,
    ArtifactAliasScope, ArtifactAliasSourceReason, ArtifactDescriptor, ArtifactKind,
    ArtifactLiveEvent, ArtifactLiveEventKind, ArtifactProfile, ArtifactTargetKind, AssignmentLease,
    AttestationLevel, AuthProvider, BackendClass, BackpressurePolicy, BadgeAward, BadgeKind,
    BrowserCapability, BrowserMode, BrowserRole, BrowserRolePolicy, BrowserVisibilityPolicy,
    CapabilityCard, CapabilityClass, CapabilityEstimate, ChunkDescriptor, ClientPlatform,
    ClientReleaseManifest, CompiledFeatureSet, ConfiguredServiceSet, ContributionReceipt,
    ContributionRollup, ControlCertificate, DataReceipt, DatasetManifest, DatasetView,
    DownloadDeliveryMode, DownloadTicket, EdgeAuthProvider, EdgeFeature, EdgeServiceManifest,
    EvalAggregationRule, EvalMetricDef, EvalProtocolManifest, EvalProtocolOptions,
    ExperimentDirectoryEntry, ExperimentManifest, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, ExportJob,
    ExportJobStatus, GenesisSpec, HeadDescriptor, HeadEvalReport, HeadEvalStatus,
    HeadPromotionPolicy, IdentityVisibility, LagPolicy, LagState, LeaderboardEntry,
    LeaderboardIdentity, LeaderboardSnapshot, MergeCertificate, MergePolicy, MergeStrategy,
    MergeTopologyPolicy, MergeWindowMissPolicy, MergeWindowState, MetricScope, MetricTrustClass,
    MetricValue, MetricsLedgerSegment, MetricsLiveEvent, MetricsLiveEventKind, MetricsMode,
    MetricsSnapshotManifest, MetricsSyncCursor, MicroShard, NetworkEstimate, NetworkManifest,
    NodeCertificate, NodeCertificateClaims, PeerAuthEnvelope, PeerRole, PeerRoleSet,
    PeerWindowMetrics, PeerWindowStatus, PersistenceClass, PortalMode, Precision, ProfileMode,
    PublicationAccessMode, PublicationMode, PublicationTarget, PublicationTargetKind,
    PublishedArtifactRecord, PublishedArtifactStatus, ReducerAssignment, ReducerCohortMetrics,
    ReducerCohortStatus, ReducerLoadReport, ReductionCertificate, ReleaseTrainManifest,
    RevisionManifest, RevocationEpoch, SCHEMA_VERSION, SchemaEnvelope, SignatureAlgorithm,
    SignatureMetadata, SignedPayload, SocialMode, SocialProfile, SupportedWorkload,
    TargetArtifactManifest, TelemetrySummary, UpdateAnnounce, UpdateNormStats, WindowActivation,
    WindowId, WorkDisposition,
};
