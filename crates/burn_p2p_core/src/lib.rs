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
    AggregateEnvelope, AggregateStats, AggregateTier, ArtifactDescriptor, ArtifactKind,
    AssignmentLease, AttestationLevel, AuthProvider, CapabilityCard, CapabilityClass,
    CapabilityEstimate, ChunkDescriptor, ClientPlatform, ClientReleaseManifest,
    ContributionReceipt, ControlCertificate, DataReceipt, DatasetManifest, DatasetView,
    ExperimentDirectoryEntry, ExperimentManifest, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, GenesisSpec,
    HeadDescriptor, HeadPromotionPolicy, MergeCertificate, MergePolicy, MergeStrategy,
    MergeTopologyPolicy, MergeWindowState, MetricValue, MicroShard, NetworkEstimate,
    NetworkManifest, NodeCertificate, NodeCertificateClaims, PeerAuthEnvelope, PeerRole,
    PeerRoleSet, PersistenceClass, Precision, ReducerAssignment, ReducerLoadReport,
    ReductionCertificate, RevisionManifest, RevocationEpoch, SCHEMA_VERSION, SchemaEnvelope,
    SignatureAlgorithm, SignatureMetadata, SignedPayload, SupportedWorkload, TelemetrySummary,
    UpdateAnnounce, UpdateNormStats, WindowActivation, WindowId, WorkDisposition,
};
