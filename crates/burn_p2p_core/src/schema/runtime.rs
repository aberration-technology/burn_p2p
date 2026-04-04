use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a node certificate claims.
pub struct NodeCertificateClaims {
    /// The network ID.
    pub network_id: NetworkId,
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The release train hash.
    pub release_train_hash: ContentId,
    /// The target artifact hash.
    pub target_artifact_hash: ContentId,
    /// The peer ID.
    pub peer_id: PeerId,
    /// The peer public key hex.
    pub peer_public_key_hex: String,
    /// The principal ID.
    pub principal_id: PrincipalId,
    /// The provider.
    pub provider: AuthProvider,
    /// The granted roles.
    pub granted_roles: PeerRoleSet,
    /// The experiment scopes.
    pub experiment_scopes: BTreeSet<ExperimentScope>,
    /// The client policy hash.
    pub client_policy_hash: Option<ContentId>,
    /// The not before.
    pub not_before: DateTime<Utc>,
    /// The not after.
    pub not_after: DateTime<Utc>,
    /// The serial.
    pub serial: u64,
    /// The revocation epoch.
    pub revocation_epoch: RevocationEpoch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Carries the node certificate data.
pub struct NodeCertificate {
    /// The node cert ID.
    pub node_cert_id: NodeCertId,
    /// The body.
    pub body: SignedPayload<SchemaEnvelope<NodeCertificateClaims>>,
}

impl NodeCertificate {
    /// Creates a new value.
    pub fn new(
        protocol_version: Version,
        claims: NodeCertificateClaims,
        signature: SignatureMetadata,
    ) -> Result<Self, SchemaError> {
        let body = SignedPayload::new(
            SchemaEnvelope::new("burn_p2p.node_certificate", protocol_version, claims),
            signature,
        )?;

        Ok(Self {
            node_cert_id: body.payload_id.clone().into(),
            body,
        })
    }

    /// Performs the claims operation.
    pub fn claims(&self) -> &NodeCertificateClaims {
        &self.body.payload.payload
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Wraps the peer auth with transport metadata.
pub struct PeerAuthEnvelope {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The certificate.
    pub certificate: NodeCertificate,
    /// The client manifest ID.
    pub client_manifest_id: Option<ContentId>,
    /// The requested scopes.
    pub requested_scopes: BTreeSet<ExperimentScope>,
    /// The nonce hash.
    pub nonce_hash: ContentId,
    /// The challenge signature hex.
    pub challenge_signature_hex: String,
    /// The presented at.
    pub presented_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported experiment visibility values.
pub enum ExperimentVisibility {
    /// Exposes the public setting.
    Public,
    /// Uses the opt in variant.
    OptIn,
    /// Uses the invite only variant.
    InviteOnly,
    /// Uses the authority assigned variant.
    AuthorityAssigned,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates experiment opt in policies.
pub enum ExperimentOptInPolicy {
    /// Uses the open variant.
    Open,
    /// Uses the scoped variant.
    Scoped,
    /// Uses the invite only variant.
    InviteOnly,
    /// Uses the authority assigned variant.
    AuthorityAssigned,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an experiment resource requirements.
pub struct ExperimentResourceRequirements {
    /// The minimum roles.
    pub minimum_roles: BTreeSet<PeerRole>,
    /// The minimum device memory bytes.
    pub minimum_device_memory_bytes: Option<u64>,
    /// The minimum system memory bytes.
    pub minimum_system_memory_bytes: Option<u64>,
    /// The estimated download bytes.
    pub estimated_download_bytes: u64,
    /// The estimated window seconds.
    pub estimated_window_seconds: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an experiment directory entry.
pub struct ExperimentDirectoryEntry {
    /// The network ID.
    pub network_id: NetworkId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The workload ID.
    pub workload_id: WorkloadId,
    /// The display name.
    pub display_name: String,
    /// The model schema hash.
    pub model_schema_hash: ContentId,
    /// The dataset view ID.
    pub dataset_view_id: DatasetViewId,
    /// The resource requirements.
    pub resource_requirements: ExperimentResourceRequirements,
    /// The visibility.
    pub visibility: ExperimentVisibility,
    /// The opt in policy.
    pub opt_in_policy: ExperimentOptInPolicy,
    /// The current revision ID.
    pub current_revision_id: RevisionId,
    /// The current head ID.
    pub current_head_id: Option<HeadId>,
    /// The allowed roles.
    pub allowed_roles: PeerRoleSet,
    /// The allowed scopes.
    pub allowed_scopes: BTreeSet<ExperimentScope>,
    /// The metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported merge strategies.
pub enum MergeStrategy {
    /// Uses the global broadcast baseline variant.
    GlobalBroadcastBaseline,
    /// Uses the central reducer baseline variant.
    CentralReducerBaseline,
    /// Uses the random peer gossip variant.
    RandomPeerGossip,
    /// Uses the k regular gossip variant.
    KRegularGossip,
    /// Uses the fixed tree reduce variant.
    FixedTreeReduce,
    /// Uses the rotating rendezvous tree variant.
    RotatingRendezvousTree,
    /// Uses the replicated rendezvous DAG variant.
    ReplicatedRendezvousDag,
    /// Uses the local gossip plus periodic global variant.
    LocalGossipPlusPeriodicGlobal,
    /// Uses the microcohort reduce plus validator promotion variant.
    MicrocohortReducePlusValidatorPromotion,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures the head promotion policy.
pub struct HeadPromotionPolicy {
    /// The validator quorum.
    pub validator_quorum: u16,
    /// The apply single root EMA.
    pub apply_single_root_ema: bool,
    /// The allow late rollover.
    pub allow_late_rollover: bool,
    /// The promote serve head.
    pub promote_serve_head: bool,
}

impl Default for HeadPromotionPolicy {
    fn default() -> Self {
        Self {
            validator_quorum: 2,
            apply_single_root_ema: true,
            allow_late_rollover: true,
            promote_serve_head: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures the merge topology policy.
pub struct MergeTopologyPolicy {
    /// The strategy.
    pub strategy: MergeStrategy,
    /// The reducer replication.
    pub reducer_replication: u8,
    /// The target leaf cohort.
    pub target_leaf_cohort: u16,
    /// The upper fanin.
    pub upper_fanin: u8,
    /// The window duration secs.
    pub window_duration_secs: u32,
    /// The publish jitter ms.
    pub publish_jitter_ms: u32,
    /// The staleness windows.
    pub staleness_windows: u16,
    /// The promotion policy.
    pub promotion_policy: HeadPromotionPolicy,
}

impl Default for MergeTopologyPolicy {
    fn default() -> Self {
        Self {
            strategy: MergeStrategy::ReplicatedRendezvousDag,
            reducer_replication: 2,
            target_leaf_cohort: 16,
            upper_fanin: 4,
            window_duration_secs: 300,
            publish_jitter_ms: 750,
            staleness_windows: 2,
            promotion_policy: HeadPromotionPolicy::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Summarizes update norm statistics.
pub struct UpdateNormStats {
    /// The l2 norm.
    pub l2_norm: f64,
    /// The max abs.
    pub max_abs: f64,
    /// The clipped.
    pub clipped: bool,
    /// The non finite tensors.
    pub non_finite_tensors: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents an update announce.
pub struct UpdateAnnounce {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The window ID.
    pub window_id: WindowId,
    /// The base head ID.
    pub base_head_id: HeadId,
    /// The delta artifact ID.
    pub delta_artifact_id: ArtifactId,
    /// The sample weight.
    pub sample_weight: f64,
    /// The quality weight.
    pub quality_weight: f64,
    /// The norm stats.
    pub norm_stats: UpdateNormStats,
    /// The receipt root.
    pub receipt_root: ContentId,
    /// The receipt IDs.
    pub receipt_ids: Vec<ContributionReceiptId>,
    /// The providers.
    pub providers: Vec<PeerId>,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a reducer assignment.
pub struct ReducerAssignment {
    /// The assignment ID.
    pub assignment_id: ContentId,
    /// The window ID.
    pub window_id: WindowId,
    /// The source peer ID.
    pub source_peer_id: PeerId,
    /// The assigned reducers.
    pub assigned_reducers: Vec<PeerId>,
    /// The repair reducers.
    pub repair_reducers: Vec<PeerId>,
    /// The upper tier reducers.
    pub upper_tier_reducers: Vec<PeerId>,
    /// The assigned at.
    pub assigned_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported aggregate tier values.
pub enum AggregateTier {
    /// Uses the leaf variant.
    Leaf,
    /// Uses the upper variant.
    Upper,
    /// Uses the root candidate variant.
    RootCandidate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Summarizes aggregate statistics.
pub struct AggregateStats {
    /// The accepted updates.
    pub accepted_updates: u32,
    /// The duplicate updates.
    pub duplicate_updates: u32,
    /// The dropped updates.
    pub dropped_updates: u32,
    /// The late updates.
    pub late_updates: u32,
    /// The sum sample weight.
    pub sum_sample_weight: f64,
    /// The sum quality weight.
    pub sum_quality_weight: f64,
    /// The sum weighted delta norm.
    pub sum_weighted_delta_norm: f64,
    /// The max update norm.
    pub max_update_norm: f64,
    /// The accepted sample coverage.
    pub accepted_sample_coverage: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Wraps the aggregate with transport metadata.
pub struct AggregateEnvelope {
    /// The aggregate ID.
    pub aggregate_id: ContentId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The window ID.
    pub window_id: WindowId,
    /// The base head ID.
    pub base_head_id: HeadId,
    /// The aggregate artifact ID.
    pub aggregate_artifact_id: ArtifactId,
    /// The tier.
    pub tier: AggregateTier,
    /// The reducer peer ID.
    pub reducer_peer_id: PeerId,
    /// The contributor peers.
    pub contributor_peers: Vec<PeerId>,
    /// The child aggregate IDs.
    pub child_aggregate_ids: Vec<ContentId>,
    /// The stats.
    pub stats: AggregateStats,
    /// The providers.
    pub providers: Vec<PeerId>,
    /// The published at.
    pub published_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Carries the reduction certificate data.
pub struct ReductionCertificate {
    /// The reduction ID.
    pub reduction_id: ContentId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The window ID.
    pub window_id: WindowId,
    /// The base head ID.
    pub base_head_id: HeadId,
    /// The aggregate ID.
    pub aggregate_id: ContentId,
    /// The validator.
    pub validator: PeerId,
    /// The validator quorum.
    pub validator_quorum: u16,
    /// The cross checked reducers.
    pub cross_checked_reducers: Vec<PeerId>,
    /// The issued at.
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Captures merge window state.
pub struct MergeWindowState {
    /// The merge window ID.
    pub merge_window_id: ContentId,
    /// The network ID.
    pub network_id: NetworkId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The window ID.
    pub window_id: WindowId,
    /// The base head ID.
    pub base_head_id: HeadId,
    /// The policy.
    pub policy: MergeTopologyPolicy,
    /// The reducers.
    pub reducers: Vec<PeerId>,
    /// The validators.
    pub validators: Vec<PeerId>,
    /// The opened at.
    pub opened_at: DateTime<Utc>,
    /// The closes at.
    pub closes_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Reports reducer load details.
pub struct ReducerLoadReport {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The window ID.
    pub window_id: WindowId,
    /// The assigned leaf updates.
    pub assigned_leaf_updates: u32,
    /// The assigned aggregate inputs.
    pub assigned_aggregate_inputs: u32,
    /// The ingress bytes.
    pub ingress_bytes: u128,
    /// The egress bytes.
    pub egress_bytes: u128,
    /// The duplicate transfer ratio.
    pub duplicate_transfer_ratio: f64,
    /// The overload ratio.
    pub overload_ratio: f64,
    /// The reported at.
    pub reported_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a micro shard.
pub struct MicroShard {
    /// The microshard ID.
    pub microshard_id: MicroShardId,
    /// The dataset view ID.
    pub dataset_view_id: DatasetViewId,
    /// The ordinal.
    pub ordinal: u32,
    /// The estimated examples.
    pub estimated_examples: u64,
    /// The estimated tokens.
    pub estimated_tokens: u64,
    /// The estimated bytes.
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an assignment lease.
pub struct AssignmentLease {
    /// The lease ID.
    pub lease_id: LeaseId,
    /// The network ID.
    pub network_id: NetworkId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The peer ID.
    pub peer_id: PeerId,
    /// The dataset view ID.
    pub dataset_view_id: DatasetViewId,
    /// The window ID.
    pub window_id: WindowId,
    /// The granted at.
    pub granted_at: DateTime<Utc>,
    /// The expires at.
    pub expires_at: DateTime<Utc>,
    /// The budget work units.
    pub budget_work_units: u64,
    /// The microshards.
    pub microshards: Vec<MicroShardId>,
    /// The assignment hash.
    pub assignment_hash: ContentId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported work disposition values.
pub enum WorkDisposition {
    /// Uses the accepted variant.
    Accepted,
    /// Uses the rejected variant.
    Rejected,
    /// Uses the partial variant.
    Partial,
    /// Uses the expired variant.
    Expired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Records the data.
pub struct DataReceipt {
    /// The receipt ID.
    pub receipt_id: ContentId,
    /// The lease ID.
    pub lease_id: LeaseId,
    /// The peer ID.
    pub peer_id: PeerId,
    /// The completed at.
    pub completed_at: DateTime<Utc>,
    /// The microshards.
    pub microshards: Vec<MicroShardId>,
    /// The examples processed.
    pub examples_processed: u64,
    /// The tokens processed.
    pub tokens_processed: u64,
    /// The disposition.
    pub disposition: WorkDisposition,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported precision values.
pub enum Precision {
    /// Uses the fp32 variant.
    Fp32,
    /// Uses the fp16 variant.
    Fp16,
    /// Uses the bf16 variant.
    Bf16,
    /// Uses the int8 variant.
    Int8,
    /// Uses the custom variant.
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported artifact kinds.
pub enum ArtifactKind {
    /// Uses the full head kind.
    FullHead,
    /// Uses the serve head kind.
    ServeHead,
    /// Uses the delta pack kind.
    DeltaPack,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the chunk.
pub struct ChunkDescriptor {
    /// The chunk ID.
    pub chunk_id: ChunkId,
    /// The offset bytes.
    pub offset_bytes: u64,
    /// The length bytes.
    pub length_bytes: u64,
    /// The chunk hash.
    pub chunk_hash: ContentId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the artifact.
pub struct ArtifactDescriptor {
    /// The artifact ID.
    pub artifact_id: ArtifactId,
    /// The kind.
    pub kind: ArtifactKind,
    /// The head ID.
    pub head_id: Option<HeadId>,
    /// The base head ID.
    pub base_head_id: Option<HeadId>,
    /// The precision.
    pub precision: Precision,
    /// The model schema hash.
    pub model_schema_hash: ContentId,
    /// The record format.
    pub record_format: String,
    /// The bytes len.
    pub bytes_len: u64,
    /// The chunks.
    pub chunks: Vec<ChunkDescriptor>,
    /// The root hash.
    pub root_hash: ContentId,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
/// Enumerates the supported metric value values.
pub enum MetricValue {
    /// Uses the integer variant.
    Integer(i64),
    /// Uses the float variant.
    Float(f64),
    /// Uses the bool variant.
    Bool(bool),
    /// Uses the text variant.
    Text(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Describes the head.
pub struct HeadDescriptor {
    /// The head ID.
    pub head_id: HeadId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The artifact ID.
    pub artifact_id: ArtifactId,
    /// The parent head ID.
    pub parent_head_id: Option<HeadId>,
    /// The global step.
    pub global_step: u64,
    /// The created at.
    pub created_at: DateTime<Utc>,
    /// The metrics.
    pub metrics: BTreeMap<String, MetricValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates merge policies.
pub enum MergePolicy {
    /// Uses the weighted mean variant.
    WeightedMean,
    /// Uses the norm clipped weighted mean variant.
    NormClippedWeightedMean,
    /// Uses the trimmed mean variant.
    TrimmedMean,
    /// Uses the EMA variant.
    Ema,
    /// Uses the quality weighted EMA variant.
    QualityWeightedEma,
    /// Uses the custom variant.
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Records the contribution.
pub struct ContributionReceipt {
    /// The receipt ID.
    pub receipt_id: ContributionReceiptId,
    /// The peer ID.
    pub peer_id: PeerId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The base head ID.
    pub base_head_id: HeadId,
    /// The artifact ID.
    pub artifact_id: ArtifactId,
    /// The accepted at.
    pub accepted_at: DateTime<Utc>,
    /// The accepted weight.
    pub accepted_weight: f64,
    /// The metrics.
    pub metrics: BTreeMap<String, MetricValue>,
    /// The merge cert ID.
    pub merge_cert_id: Option<MergeCertId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Carries the merge certificate data.
pub struct MergeCertificate {
    /// The merge cert ID.
    pub merge_cert_id: MergeCertId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The base head ID.
    pub base_head_id: HeadId,
    /// The merged head ID.
    pub merged_head_id: HeadId,
    /// The merged artifact ID.
    pub merged_artifact_id: ArtifactId,
    /// The policy.
    pub policy: MergePolicy,
    /// The issued at.
    pub issued_at: DateTime<Utc>,
    /// The validator.
    pub validator: PeerId,
    /// The contribution receipts.
    pub contribution_receipts: Vec<ContributionReceiptId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Carries the control certificate data.
pub struct ControlCertificate<T> {
    /// The control cert ID.
    pub control_cert_id: ControlCertId,
    /// The network ID.
    pub network_id: NetworkId,
    /// The activation.
    pub activation: WindowActivation,
    /// The required client capabilities.
    pub required_client_capabilities: BTreeSet<String>,
    /// The body.
    pub body: SignedPayload<SchemaEnvelope<T>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a network estimate.
pub struct NetworkEstimate {
    /// The connected peers.
    pub connected_peers: u32,
    /// The observed peers.
    pub observed_peers: u64,
    /// The estimated network size.
    pub estimated_network_size: f64,
    /// The estimated total vram bytes.
    pub estimated_total_vram_bytes: Option<u128>,
    /// The estimated total FLOPs.
    pub estimated_total_flops: Option<f64>,
    /// The eta lower seconds.
    pub eta_lower_seconds: Option<u64>,
    /// The eta upper seconds.
    pub eta_upper_seconds: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Summarizes the telemetry.
pub struct TelemetrySummary {
    /// The network ID.
    pub network_id: NetworkId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The window ID.
    pub window_id: WindowId,
    /// The active peers.
    pub active_peers: u32,
    /// The accepted contributions.
    pub accepted_contributions: u64,
    /// The throughput work units per second.
    pub throughput_work_units_per_second: f64,
    /// The network.
    pub network: NetworkEstimate,
    /// The metrics.
    pub metrics: BTreeMap<String, MetricValue>,
    /// The captured at.
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a genesis spec.
pub struct GenesisSpec {
    /// The network ID.
    pub network_id: NetworkId,
    /// The protocol version.
    pub protocol_version: Version,
    /// The display name.
    pub display_name: String,
    /// The created at.
    pub created_at: DateTime<Utc>,
    /// The metadata.
    pub metadata: BTreeMap<String, String>,
}
