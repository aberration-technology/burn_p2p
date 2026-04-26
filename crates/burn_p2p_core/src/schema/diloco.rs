use std::fmt;

use super::*;
use crate::codec::{CanonicalSchema, SchemaError};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
/// Identifies one DiLoCo synchronization round.
pub struct RoundId(u64);

impl RoundId {
    /// Creates a new value.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric round index.
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Returns the next round identifier.
    pub fn next(&self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for RoundId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
/// Identifies one DiLoCo aggregation group.
pub struct GroupId(String);

impl GroupId {
    /// Creates a new value from an existing string.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Derives a deterministic group identifier from canonical content.
    pub fn derive<T>(value: &T) -> Result<Self, SchemaError>
    where
        T: serde::Serialize + ?Sized,
    {
        Ok(Self(ContentId::derive(value)?.into_inner()))
    }

    /// Returns the string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
/// Identifies the base checkpoint used to seed one DiLoCo round.
pub struct BaseCheckpointId(String);

impl BaseCheckpointId {
    /// Creates a new value from an existing string.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Derives a deterministic base-checkpoint identifier from canonical content.
    pub fn derive<T>(value: &T) -> Result<Self, SchemaError>
    where
        T: serde::Serialize + ?Sized,
    {
        Ok(Self(ContentId::derive(value)?.into_inner()))
    }

    /// Returns the string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BaseCheckpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<HeadId> for BaseCheckpointId {
    fn from(value: HeadId) -> Self {
        Self(value.as_str().to_owned())
    }
}

impl From<BaseCheckpointId> for ContentId {
    fn from(value: BaseCheckpointId) -> Self {
        ContentId::new(value.0)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Tracks the current phase of one DiLoCo synchronization round.
pub enum RoundPhase {
    /// Synchronizing the starting checkpoint and outer optimizer state.
    SyncBase,
    /// Running local inner-loop optimization steps.
    InnerTrain,
    /// Building the pseudo-gradient payload from the local inner-loop result.
    BuildPseudoGradient,
    /// Matching peers into one aggregation cohort.
    Matchmake,
    /// Aggregating peer pseudo-gradients.
    Aggregate,
    /// Applying the aggregated outer-loop update.
    OuterApply,
    /// Publishing a cold-path checkpoint when the round interval requires it.
    MaybeCheckpoint,
    /// Completed the round and is ready to advance.
    #[default]
    Completed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Locates one peer inside the DiLoCo round state machine.
pub struct RoundCursor {
    /// The current round identifier.
    pub round_id: RoundId,
    /// The assigned aggregation group, when matchmaking has completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<GroupId>,
    /// The checkpoint or head used as the base for this round.
    pub base_checkpoint_id: BaseCheckpointId,
    /// The active phase of the round.
    pub phase: RoundPhase,
    /// The configured inner-loop step budget for the round.
    pub num_inner_steps: u32,
}

impl RoundCursor {
    /// Creates a new cursor at the start of one round.
    pub fn new(base_checkpoint_id: BaseCheckpointId, num_inner_steps: u32) -> Self {
        Self {
            round_id: RoundId::new(0),
            group_id: None,
            base_checkpoint_id,
            phase: RoundPhase::SyncBase,
            num_inner_steps,
        }
    }

    /// Advances the cursor to one new round.
    pub fn advance(&self, base_checkpoint_id: BaseCheckpointId) -> Self {
        Self {
            round_id: self.round_id.next(),
            group_id: None,
            base_checkpoint_id,
            phase: RoundPhase::SyncBase,
            num_inner_steps: self.num_inner_steps,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Selects the encoded representation used for pseudo-gradient transport.
pub enum GradientCodec {
    /// Sends pseudo-gradients as native `f32` payloads.
    Fp32,
    /// Sends pseudo-gradients as IEEE-754 half precision payloads.
    #[default]
    Fp16,
    /// Sends blockwise int8 payloads with one scale factor per block.
    BlockwiseInt8 {
        /// The number of values quantized under one shared scale.
        block_size: u32,
    },
    /// Sends QSGD-style stochastic buckets.
    Qsgd {
        /// Number of quantization bits.
        bits: u8,
        /// Whether stochastic rounding is enabled.
        stochastic: bool,
    },
    /// Sends one low-rank factorization per pseudo-gradient tensor pack.
    LowRank {
        /// The retained factorization rank.
        rank: u16,
    },
    /// Sends only the update sign with optional error-feedback state.
    SignSgd {
        /// Whether error-feedback residuals are preserved across rounds.
        error_feedback: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures how one peer rejoins after missing DiLoCo rounds.
pub struct DiLoCoRejoinPolicy {
    /// Maximum round lag tolerated before requiring a full checkpoint resync.
    pub max_fast_forward_round_lag: u32,
    /// Whether the peer must observe the exact base-checkpoint hash before rejoining.
    pub require_checkpoint_hash_match: bool,
}

impl Default for DiLoCoRejoinPolicy {
    fn default() -> Self {
        Self {
            max_fast_forward_round_lag: 4,
            require_checkpoint_hash_match: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Selects the outer optimizer used when applying aggregated pseudo-gradients.
pub enum OuterOptimizerPolicy {
    /// Uses SGD with optional momentum and Nesterov acceleration.
    Sgd {
        /// Scalar learning rate applied to the aggregated pseudo-gradient in millionths.
        learning_rate_micros: u64,
        /// Optional momentum coefficient in millionths.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        momentum_micros: Option<u64>,
        /// Whether Nesterov-style lookahead is enabled.
        #[serde(default)]
        nesterov: bool,
        /// Optional weight decay factor in millionths.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        weight_decay_micros: Option<u64>,
    },
}

impl OuterOptimizerPolicy {
    /// Returns the learning rate as an `f64`.
    pub fn learning_rate(&self) -> f64 {
        match self {
            Self::Sgd {
                learning_rate_micros,
                ..
            } => *learning_rate_micros as f64 / 1_000_000.0,
        }
    }

    /// Returns the momentum coefficient as an `f64`, when configured.
    pub fn momentum(&self) -> Option<f64> {
        match self {
            Self::Sgd {
                momentum_micros, ..
            } => momentum_micros.map(|value| value as f64 / 1_000_000.0),
        }
    }

    /// Returns the weight decay coefficient as an `f64`, when configured.
    pub fn weight_decay(&self) -> Option<f64> {
        match self {
            Self::Sgd {
                weight_decay_micros,
                ..
            } => weight_decay_micros.map(|value| value as f64 / 1_000_000.0),
        }
    }
}

impl Default for OuterOptimizerPolicy {
    fn default() -> Self {
        Self::Sgd {
            learning_rate_micros: 1_000_000,
            momentum_micros: None,
            nesterov: false,
            weight_decay_micros: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Selects how peer pseudo-gradients are combined after transport decoding.
pub enum DiLoCoAggregationPolicy {
    /// Averages all compatible peer pseudo-gradients with equal weight.
    #[default]
    Mean,
    /// Uses a coordinate-wise median for Byzantine/outlier resistance.
    CoordinateMedian,
    /// Aggregates ternary update signs by coordinate-wise majority.
    SignMajority {
        /// Minimum required agreement in millionths before emitting a non-zero sign.
        #[serde(default)]
        minimum_agreement_micros: u64,
        /// Tie-breaking behavior when positive and negative sign counts match.
        #[serde(default)]
        tie_break: SignMajorityTieBreak,
    },
}

impl DiLoCoAggregationPolicy {
    /// Returns the minimum agreement threshold as a clamped fraction.
    pub fn minimum_agreement(&self) -> f32 {
        match self {
            Self::SignMajority {
                minimum_agreement_micros,
                ..
            } => (*minimum_agreement_micros as f32 / 1_000_000.0).clamp(0.0, 1.0),
            Self::Mean | Self::CoordinateMedian => 0.0,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Selects how signSGD resolves balanced positive/negative votes.
pub enum SignMajorityTieBreak {
    /// Emits zero when signs are exactly tied.
    #[default]
    Zero,
    /// Emits a positive sign when signs are exactly tied.
    Positive,
    /// Emits a negative sign when signs are exactly tied.
    Negative,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Selects the DiLoCo peer matching topology.
pub enum DiLoCoTopologyMode {
    /// Deterministically forms rendezvous cohorts from connected compatible peers.
    #[default]
    DeterministicRendezvous,
    /// Uses bounded gossip neighborhoods for partial-connectivity overlays.
    GossipNeighborhood,
    /// Allows relay-assisted cohort formation when direct paths are sparse.
    RelayAssisted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures how DiLoCo cohorts should be formed across different network topologies.
pub struct DiLoCoTopologyPolicy {
    /// The desired cohort formation mode.
    #[serde(default)]
    pub mode: DiLoCoTopologyMode,
    /// Maximum number of peers considered for one local cohort candidate set.
    #[serde(default = "default_diloco_topology_fanout")]
    pub fanout: u16,
    /// Whether lower-latency peers should be preferred when equivalent candidates are available.
    #[serde(default = "default_true")]
    pub prefer_low_latency: bool,
    /// Whether relay-assisted paths are allowed for DiLoCo synchronization traffic.
    #[serde(default = "default_true")]
    pub allow_relay: bool,
}

impl Default for DiLoCoTopologyPolicy {
    fn default() -> Self {
        Self {
            mode: DiLoCoTopologyMode::DeterministicRendezvous,
            fanout: default_diloco_topology_fanout(),
            prefer_low_latency: default_true(),
            allow_relay: default_true(),
        }
    }
}

const fn default_diloco_topology_fanout() -> u16 {
    8
}

const fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures the DiLoCo inner/outer synchronization policy for one revision.
pub struct DiLoCoPolicy {
    /// Number of local inner-loop steps executed before each outer synchronization.
    pub num_inner_steps: u32,
    /// Target number of peers matched into one aggregation cohort.
    pub target_group_size: u16,
    /// Minimum viable cohort size before falling back to partial participation.
    pub minimum_group_size: u16,
    /// Time budget for group formation.
    pub matchmaking_timeout_ms: u32,
    /// Time budget for pseudo-gradient aggregation and broadcast.
    pub aggregation_timeout_ms: u32,
    /// Number of completed rounds between cold-path checkpoint publications.
    pub checkpoint_interval_rounds: u32,
    /// Transport codec used for pseudo-gradient exchange.
    #[serde(default)]
    pub codec: GradientCodec,
    /// Aggregation semantics applied after pseudo-gradient transport decoding.
    #[serde(default)]
    pub aggregation_policy: DiLoCoAggregationPolicy,
    /// Cohort formation policy for partially connected or relay-assisted networks.
    #[serde(default)]
    pub topology_policy: DiLoCoTopologyPolicy,
    /// Rejoin policy for stale or late peers.
    #[serde(default)]
    pub rejoin_policy: DiLoCoRejoinPolicy,
    /// Outer optimizer semantics applied after aggregation.
    #[serde(default)]
    pub outer_optimizer_policy: OuterOptimizerPolicy,
    /// Whether remote DiLoCo state and pseudo-gradient manifests must carry
    /// signatures from authenticated peers before they can participate.
    #[serde(default)]
    pub require_signed_peer_payloads: bool,
}

impl Default for DiLoCoPolicy {
    fn default() -> Self {
        Self {
            num_inner_steps: 32,
            target_group_size: 4,
            minimum_group_size: 1,
            matchmaking_timeout_ms: 5_000,
            aggregation_timeout_ms: 15_000,
            checkpoint_interval_rounds: 8,
            codec: GradientCodec::Fp16,
            aggregation_policy: DiLoCoAggregationPolicy::default(),
            topology_policy: DiLoCoTopologyPolicy::default(),
            rejoin_policy: DiLoCoRejoinPolicy::default(),
            outer_optimizer_policy: OuterOptimizerPolicy::default(),
            require_signed_peer_payloads: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Selects the training protocol executed inside one experiment revision.
pub enum TrainingProtocol {
    /// Uses the existing artifact-window local training flow.
    #[default]
    ArtifactWindows,
    /// Uses DiLoCo inner/outer optimization rounds with pseudo-gradient exchange.
    DiLoCo(DiLoCoPolicy),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Stores one opaque optimizer or workload state snapshot.
pub struct StateBlob {
    /// Canonical content identifier for the stored bytes.
    pub content_id: ContentId,
    /// Human-readable encoding label.
    pub encoding: String,
    /// Serialized bytes.
    pub bytes: Vec<u8>,
}

impl StateBlob {
    /// Creates a new blob from raw bytes.
    pub fn try_new(
        encoding: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) -> Result<Self, SchemaError> {
        let encoding = encoding.into();
        let bytes = bytes.into();
        let content_id = ContentId::derive(&(encoding.as_str(), bytes.as_slice()))?;
        Ok(Self {
            content_id,
            encoding,
            bytes,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Flattened deterministic tensor pack used by the first DiLoCo transport layer.
pub struct FlattenedTensorPack {
    /// Model schema hash used to verify compatibility.
    pub model_schema_hash: ContentId,
    /// Deterministic layout hash for the flattened parameter order.
    pub layout_hash: ContentId,
    /// Flattened parameter values in deterministic order.
    pub values: Vec<f32>,
}

impl FlattenedTensorPack {
    /// Creates a new value.
    pub fn new(model_schema_hash: ContentId, layout_hash: ContentId, values: Vec<f32>) -> Self {
        Self {
            model_schema_hash,
            layout_hash,
            values,
        }
    }

    /// Returns the number of flattened parameters.
    pub fn parameter_count(&self) -> usize {
        self.values.len()
    }

    /// Returns whether two packs can be combined safely.
    pub fn is_compatible_with(&self, other: &Self) -> bool {
        self.model_schema_hash == other.model_schema_hash
            && self.layout_hash == other.layout_hash
            && self.values.len() == other.values.len()
    }

    /// Computes the L2 norm of the flattened values.
    pub fn l2_norm(&self) -> f64 {
        self.values
            .iter()
            .map(|value| f64::from(*value) * f64::from(*value))
            .sum::<f64>()
            .sqrt()
    }

    /// Returns the maximum absolute value.
    pub fn max_abs(&self) -> f32 {
        self.values
            .iter()
            .fold(0.0_f32, |current, value| current.max(value.abs()))
    }

    /// Computes the canonical checksum for the flattened pack.
    pub fn checksum(&self) -> Result<ContentId, SchemaError> {
        self.content_id()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes one encoded pseudo-gradient transfer.
pub struct PseudoGradientManifest {
    /// Stable content identifier for the encoded manifest.
    pub manifest_id: ContentId,
    /// Experiment scope for the transfer.
    pub experiment_id: ExperimentId,
    /// Revision scope for the transfer.
    pub revision_id: RevisionId,
    /// Originating peer.
    pub peer_id: PeerId,
    /// Round cursor associated with the transfer.
    pub round_cursor: RoundCursor,
    /// Codec used to encode the payload bytes.
    pub codec: GradientCodec,
    /// Model schema hash for the flattened pack.
    pub model_schema_hash: ContentId,
    /// Flattened layout hash.
    pub layout_hash: ContentId,
    /// Total parameter count represented by the payload.
    pub parameter_count: u64,
    /// Canonical checksum of the decoded pseudo-gradient pack.
    pub checksum: ContentId,
    /// Total number of encoded chunks.
    pub chunk_count: u32,
    /// Total encoded byte length across all chunks.
    pub total_encoded_bytes: u64,
    /// Signatures attesting to this manifest.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signature_bundle: Vec<SignatureMetadata>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
/// Input fields used to create one encoded pseudo-gradient manifest.
pub struct PseudoGradientManifestInput<'a> {
    /// Experiment scope for the transfer.
    pub experiment_id: ExperimentId,
    /// Revision scope for the transfer.
    pub revision_id: RevisionId,
    /// Originating peer.
    pub peer_id: PeerId,
    /// Round cursor associated with the transfer.
    pub round_cursor: RoundCursor,
    /// Codec used to encode the payload bytes.
    pub codec: GradientCodec,
    /// Decoded transport pack used for manifest checksum derivation.
    pub pack: &'a FlattenedTensorPack,
    /// Total number of encoded chunks.
    pub chunk_count: u32,
    /// Total encoded byte length across all chunks.
    pub total_encoded_bytes: u64,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
}

impl PseudoGradientManifest {
    /// Creates a manifest for one encoded pseudo-gradient payload.
    pub fn try_new(input: PseudoGradientManifestInput<'_>) -> Result<Self, SchemaError> {
        let checksum = input.pack.checksum()?;
        let manifest_id = ContentId::derive(&(
            input.experiment_id.as_str(),
            input.revision_id.as_str(),
            input.peer_id.as_str(),
            input.round_cursor.round_id.as_u64(),
            input.round_cursor.base_checkpoint_id.as_str(),
            &input.codec,
            input.pack.layout_hash.as_str(),
            input.pack.values.len(),
            checksum.as_str(),
            input.chunk_count,
            input.total_encoded_bytes,
        ))?;

        Ok(Self {
            manifest_id,
            experiment_id: input.experiment_id,
            revision_id: input.revision_id,
            peer_id: input.peer_id,
            round_cursor: input.round_cursor,
            codec: input.codec,
            model_schema_hash: input.pack.model_schema_hash.clone(),
            layout_hash: input.pack.layout_hash.clone(),
            parameter_count: input.pack.parameter_count() as u64,
            checksum,
            chunk_count: input.chunk_count,
            total_encoded_bytes: input.total_encoded_bytes,
            signature_bundle: Vec::new(),
            created_at: input.created_at,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Carries one encoded pseudo-gradient chunk.
pub struct PseudoGradientChunk {
    /// Manifest identifier that owns the chunk.
    pub manifest_id: ContentId,
    /// Zero-based chunk index.
    pub chunk_index: u32,
    /// Raw encoded bytes.
    pub bytes: Vec<u8>,
    /// Timestamp when the chunk was produced.
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Captures the durable DiLoCo round and optimizer state used for rejoin/bootstrap.
pub struct DiLoCoStateSnapshot {
    /// Experiment scope for the snapshot.
    pub experiment_id: ExperimentId,
    /// Revision scope for the snapshot.
    pub revision_id: RevisionId,
    /// Training protocol pinned for the snapshot.
    pub training_protocol: TrainingProtocol,
    /// Current round cursor.
    pub round_cursor: RoundCursor,
    /// Latest published cold-path checkpoint head, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_head_id: Option<HeadId>,
    /// Latest pseudo-gradient manifest published for the current round, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_gradient_manifest_id: Option<ContentId>,
    /// Checksum of the current local parameter state, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_parameter_checksum: Option<ContentId>,
    /// Serialized outer optimizer state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outer_optimizer_state: Option<StateBlob>,
    /// Signatures attesting to this state snapshot.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signature_bundle: Vec<SignatureMetadata>,
    /// Snapshot timestamp.
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Announces intent to participate in one DiLoCo aggregation round.
pub struct DiLoCoRoundOffer {
    /// Experiment scope for the offer.
    pub experiment_id: ExperimentId,
    /// Revision scope for the offer.
    pub revision_id: RevisionId,
    /// Peer advertising the offer.
    pub peer_id: PeerId,
    /// Cursor being offered.
    pub round_cursor: RoundCursor,
    /// Requested cohort size.
    pub target_group_size: u16,
    /// Offer timestamp.
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Emits liveness updates while one peer is waiting inside the DiLoCo control loop.
pub struct DiLoCoRoundHeartbeat {
    /// Experiment scope for the heartbeat.
    pub experiment_id: ExperimentId,
    /// Revision scope for the heartbeat.
    pub revision_id: RevisionId,
    /// Reporting peer.
    pub peer_id: PeerId,
    /// Cursor being observed.
    pub round_cursor: RoundCursor,
    /// Number of participants the peer has observed for the current group.
    pub observed_participants: u16,
    /// Heartbeat timestamp.
    pub emitted_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Finalizes one DiLoCo round after aggregation completes.
pub struct DiLoCoRoundFinalize {
    /// Experiment scope for the finalization.
    pub experiment_id: ExperimentId,
    /// Revision scope for the finalization.
    pub revision_id: RevisionId,
    /// Finalizing peer.
    pub peer_id: PeerId,
    /// Cursor being finalized.
    pub round_cursor: RoundCursor,
    /// Final participant count used for the aggregate.
    pub participant_count: u16,
    /// Checksum of the aggregated pseudo-gradient when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate_checksum: Option<ContentId>,
    /// Finalization timestamp.
    pub finalized_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Namespaces the control-plane request/response surface for DiLoCo-specific traffic.
pub enum DiLoCoRequest {
    /// Offer participation in one round.
    RoundOffer(Box<DiLoCoRoundOffer>),
    /// Send one heartbeat for an in-flight round.
    RoundHeartbeat(Box<DiLoCoRoundHeartbeat>),
    /// Finalize one completed round.
    RoundFinalize(Box<DiLoCoRoundFinalize>),
    /// Request the latest durable state snapshot.
    StateSnapshot {
        /// Experiment scope.
        experiment_id: ExperimentId,
        /// Revision scope.
        revision_id: RevisionId,
    },
    /// Request the latest outer optimizer state blob.
    OuterOptimizerState {
        /// Experiment scope.
        experiment_id: ExperimentId,
        /// Revision scope.
        revision_id: RevisionId,
    },
    /// Request one pseudo-gradient manifest by content identifier.
    GradientManifest {
        /// Manifest identifier.
        manifest_id: ContentId,
    },
    /// Request the current deterministic parameter pack for rejoin/bootstrap.
    CurrentParameters {
        /// Experiment scope.
        experiment_id: ExperimentId,
        /// Revision scope.
        revision_id: RevisionId,
    },
    /// Request one pseudo-gradient chunk.
    GradientChunk {
        /// Manifest identifier.
        manifest_id: ContentId,
        /// Chunk index.
        chunk_index: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Carries one DiLoCo control-plane response.
pub enum DiLoCoResponse {
    /// Returns an acknowledgement for one control message.
    Ack {
        /// Whether the request was accepted.
        accepted: bool,
        /// Cursor echoed back by the responder, when available.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<RoundCursor>,
        /// Human-readable status message.
        message: String,
    },
    /// Returns one durable DiLoCo state snapshot.
    StateSnapshot(Option<DiLoCoStateSnapshot>),
    /// Returns one serialized outer optimizer state blob.
    OuterOptimizerState(Option<StateBlob>),
    /// Returns one pseudo-gradient manifest.
    GradientManifest(Option<PseudoGradientManifest>),
    /// Returns one current deterministic parameter pack.
    CurrentParameters(Option<FlattenedTensorPack>),
    /// Returns one pseudo-gradient chunk.
    GradientChunk(Option<PseudoGradientChunk>),
    /// Returns one unconfigured/unavailable message.
    Unavailable {
        /// Human-readable reason.
        message: String,
    },
}
