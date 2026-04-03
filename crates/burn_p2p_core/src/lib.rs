#![forbid(unsafe_code)]

pub mod codec;
pub mod id;
pub mod schema;

pub use codec::{CanonicalSchema, SchemaError, deterministic_cbor, from_cbor_slice};
pub use id::{
    ArtifactId, CapabilityCardId, ChunkId, ContentId, ContributionReceiptId, ControlCertId,
    DatasetId, DatasetViewId, ExperimentId, HeadId, LeaseId, MergeCertId, MicroShardId, NetworkId,
    NodeCertId, PeerId, PrincipalId, ProjectFamilyId, RevisionId, StudyId, WorkloadId,
};
pub use schema::{
    ActiveServiceSet, AdminMode, AggregateEnvelope, AggregateStats, AggregateTier,
    ArtifactDescriptor, ArtifactKind, ArtifactTargetKind, AssignmentLease, AttestationLevel,
    AuthProvider, BackpressurePolicy, BadgeAward, BadgeKind, BrowserCapability, BrowserMode,
    BrowserRole, BrowserRolePolicy, BrowserVisibilityPolicy, CapabilityCard, CapabilityClass,
    CapabilityEstimate, ChunkDescriptor, ClientPlatform, ClientReleaseManifest, CompiledFeatureSet,
    ConfiguredServiceSet, ContributionReceipt, ContributionRollup, ControlCertificate, DataReceipt,
    DatasetManifest, DatasetView, EdgeAuthProvider, EdgeFeature, EdgeServiceManifest,
    ExperimentDirectoryEntry, ExperimentManifest, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, GenesisSpec,
    HeadDescriptor, HeadPromotionPolicy, IdentityVisibility, LagPolicy, LagState, LeaderboardEntry,
    LeaderboardIdentity, LeaderboardSnapshot, MergeCertificate, MergePolicy, MergeStrategy,
    MergeTopologyPolicy, MergeWindowMissPolicy, MergeWindowState, MetricValue, MetricsMode,
    MicroShard, NetworkEstimate, NetworkManifest, NodeCertificate, NodeCertificateClaims,
    PeerAuthEnvelope, PeerRole, PeerRoleSet, PersistenceClass, PortalMode, Precision, ProfileMode,
    ReducerAssignment, ReducerLoadReport, ReductionCertificate, ReleaseTrainManifest,
    RevisionManifest, RevocationEpoch, SCHEMA_VERSION, SchemaEnvelope, SignatureAlgorithm,
    SignatureMetadata, SignedPayload, SocialMode, SocialProfile, SupportedWorkload,
    TargetArtifactManifest, TelemetrySummary, UpdateAnnounce, UpdateNormStats, WindowActivation,
    WindowId, WorkDisposition,
};
