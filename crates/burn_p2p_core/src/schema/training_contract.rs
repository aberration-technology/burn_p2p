use super::*;

/// Current version of the workload training-contract schema.
pub const TRAINING_CONTRACT_VERSION: u16 = 1;
/// Current version of compact update payloads.
pub const COMPACT_UPDATE_PAYLOAD_VERSION: u16 = 1;
/// Current version of canonical mutable-parameter subset catalogs.
pub const PARAMETER_SUBSET_CATALOG_VERSION: u16 = 1;
/// Domain-separated key identifier for complete revision contract signatures.
pub const REVISION_CONTRACT_SIGNATURE_KEY_ID: &str = "burn-p2p-revision-contract-v1";
/// Domain-separated key identifier for model genesis signatures.
pub const MODEL_GENESIS_SIGNATURE_KEY_ID: &str = "burn-p2p-model-genesis-v1";

/// Declares how local optimizer state behaves between distributed windows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalOptimizerStatePolicy {
    /// Every window starts from the optimizer's declared initial state.
    ResetPerWindow,
    /// State remains local to one peer and is invalidated when the base head changes.
    PeerLocalUntilReconcile,
    /// State remains local to one peer and persists across model-head reconciliation.
    ///
    /// The state is revision-scoped and may be restored by the same peer, but
    /// is not transferred when another peer's runtime state is adopted.
    PeerLocalPersistent,
    /// State is serialized as a canonical artifact and follows the model head.
    CanonicalArtifact,
    /// The workload has no reverse-mode optimizer state.
    StatelessForwardOnly,
    /// A workload-defined policy identified by a stable name.
    Custom(String),
}

/// Declares how the learning-rate or equivalent scheduler cursor advances.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerStatePolicy {
    /// The scheduler is indexed by the canonical model step.
    CanonicalGlobalStep,
    /// The scheduler is indexed by accepted tokens or sample-equivalent units.
    CanonicalAcceptedWork,
    /// The scheduler restarts for every local window.
    ResetPerWindow,
    /// The scheduler cursor remains peer-local and persists across reconciliation.
    PeerLocalPersistent,
    /// A workload-defined policy identified by a stable name.
    Custom(String),
}

/// Declares how recurrent or streaming workload state is owned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecurrentStatePolicy {
    /// No recurrent state crosses batch or window boundaries.
    Ephemeral,
    /// State is local to a run, revision, head, lease, and logical stream.
    LeaseScoped,
    /// State is a canonical artifact and may be transferred between peers.
    CanonicalArtifact,
    /// A workload-defined policy identified by a stable name.
    Custom(String),
}

/// Validator-owned replay policy for seeded-fitness observations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeededFitnessReplayPolicy {
    /// Deterministically selected antithetic pairs checked per generation.
    pub pairs_per_generation: u32,
    /// Absolute comparison tolerance in millionths.
    pub absolute_tolerance_micros: u32,
    /// Relative comparison tolerance in parts per million.
    pub relative_tolerance_ppm: u32,
}

impl Default for SeededFitnessReplayPolicy {
    fn default() -> Self {
        Self {
            pairs_per_generation: 1,
            absolute_tolerance_micros: 1_000,
            relative_tolerance_ppm: 1_000,
        }
    }
}

impl SeededFitnessReplayPolicy {
    /// Absolute comparison tolerance as an `f64`.
    pub fn absolute_tolerance(&self) -> f64 {
        f64::from(self.absolute_tolerance_micros) / 1_000_000.0
    }

    /// Relative comparison tolerance as an `f64`.
    pub fn relative_tolerance(&self) -> f64 {
        f64::from(self.relative_tolerance_ppm) / 1_000_000.0
    }
}

/// Architecture-neutral encoding used for one peer update.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum UpdateCodec {
    /// A complete model artifact.
    FullModel,
    /// A dense parameter delta from the declared base head.
    DenseDelta,
    /// Absolute values for one canonical subset of the model parameters.
    ///
    /// This is useful when the remaining parameters are immutable or can be
    /// reconstructed locally from the revision contract.
    MutableSubsetParameters {
        /// Canonical path, shape, and ordering contract for transmitted values.
        parameter_catalog_hash: ContentId,
        /// Number of scalar parameters covered by the catalog.
        parameter_count: u64,
        /// Scalar wire representation.
        encoding: CompactScalarEncoding,
    },
    /// A sparse delta for one dynamically routed context generation.
    ///
    /// Unlike `MutableSubsetParameters`, the exact parameter catalog is bound
    /// by each payload because different context masks may activate different
    /// coordinates under one stable family contract.
    ContextSparseDelta {
        /// Stable identity of the deterministic context-mask family.
        context_family_hash: ContentId,
        /// Hard upper bound on transmitted scalar deltas.
        max_parameter_count: u64,
        /// Scalar wire representation.
        encoding: CompactScalarEncoding,
    },
    /// A block-quantized dense delta.
    QuantizedBlock {
        /// Number of quantization bits per value.
        bits: u8,
        /// Number of values sharing one quantization scale.
        block_size: u32,
        /// Whether residual error is fed into the next local update.
        error_feedback: bool,
    },
    /// A deterministic seeded low-rank update.
    SeededLowRank {
        /// Maximum rank per encoded matrix.
        rank: u32,
        /// Seed used to regenerate fixed factors.
        seed: u64,
    },
    /// A low-dimensional update in a deterministic parameter subspace.
    SubspaceLatent {
        /// Number of transmitted latent coefficients.
        dimensions: u32,
        /// Seed used to regenerate the shared subspace.
        seed: u64,
    },
    /// A PowerSGD-style low-rank compressor.
    PowerSgd {
        /// Maximum rank per encoded matrix.
        rank: u32,
        /// Whether residual error is fed into the next local update.
        error_feedback: bool,
    },
    /// Seeded perturbations with scalar fitness observations.
    SeededFitness {
        /// Number of evaluated perturbations represented by the payload.
        population: u32,
        /// Rank of each regenerated perturbation.
        rank: u32,
        /// Seed used to regenerate the population.
        seed: u64,
        /// Validator-owned bounded replay policy.
        #[serde(default)]
        replay: SeededFitnessReplayPolicy,
    },
    /// A workload-defined codec identified by a stable name and config hash.
    Custom {
        /// Stable codec name.
        name: String,
        /// Canonical hash of the codec configuration.
        config_hash: ContentId,
    },
}

impl UpdateCodec {
    /// Returns whether the codec requires a declared base head.
    pub fn requires_base_head(&self) -> bool {
        !matches!(self, Self::FullModel)
    }

    /// Returns whether candidate promotion requires independent workload replay.
    pub fn requires_independent_replay(&self) -> bool {
        matches!(self, Self::SeededFitness { .. })
    }
}

/// Scalar wire encoding used by compact update payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactScalarEncoding {
    /// IEEE-754 single precision.
    Fp32,
    /// Symmetric signed 8-bit quantization with one payload-wide scale.
    SymmetricInt8,
    /// Symmetric signed 16-bit quantization with one payload-wide scale.
    SymmetricInt16,
}

/// Architecture-neutral identity of one bounded routed-context generation.
///
/// Slots may be reused, so `slot` is never sufficient on its own. The
/// generation and dynamic parameter catalog are part of every signed update.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateRoutingContext {
    pub context_family_hash: ContentId,
    pub slot: u32,
    pub generation: u64,
    pub parameter_catalog_hash: ContentId,
}

impl UpdateRoutingContext {
    pub fn validate(&self) -> Result<(), TrainingContractError> {
        if self.context_family_hash.as_str().is_empty() {
            return Err(TrainingContractError::InvalidUpdatePayload(
                "context family hash must not be empty".into(),
            ));
        }
        if self.parameter_catalog_hash.as_str().is_empty() {
            return Err(TrainingContractError::InvalidUpdatePayload(
                "context parameter catalog hash must not be empty".into(),
            ));
        }
        Ok(())
    }
}

/// One canonically ordered tensor in a parameter subset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParameterSubsetEntry {
    /// Stable module parameter path.
    pub path: String,
    /// Tensor shape in row-major logical order.
    pub shape: Vec<u64>,
}

impl ParameterSubsetEntry {
    /// Returns the number of scalar values in this tensor.
    pub fn parameter_count(&self) -> Result<u64, TrainingContractError> {
        if self.path.trim().is_empty() {
            return Err(TrainingContractError::InvalidParameterSubsetCatalog(
                "parameter paths must not be empty".into(),
            ));
        }
        if self.shape.is_empty() || self.shape.contains(&0) {
            return Err(TrainingContractError::InvalidParameterSubsetCatalog(
                format!(
                    "parameter {} must have a non-empty positive shape",
                    self.path
                ),
            ));
        }
        self.shape.iter().try_fold(1_u64, |count, dimension| {
            count.checked_mul(*dimension).ok_or_else(|| {
                TrainingContractError::InvalidParameterSubsetCatalog(format!(
                    "parameter {} shape overflows u64",
                    self.path
                ))
            })
        })
    }
}

/// Canonical path, shape, and ordering contract for a model parameter subset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParameterSubsetCatalog {
    /// Catalog schema version.
    pub version: u16,
    /// Full model tensor schema this subset belongs to.
    pub model_schema_hash: ContentId,
    /// Strictly path-sorted tensor entries. Values are flattened in this order.
    pub entries: Vec<ParameterSubsetEntry>,
}

impl ParameterSubsetCatalog {
    /// Creates a versioned catalog.
    pub fn new(model_schema_hash: ContentId, entries: Vec<ParameterSubsetEntry>) -> Self {
        Self {
            version: PARAMETER_SUBSET_CATALOG_VERSION,
            model_schema_hash,
            entries,
        }
    }

    /// Validates canonical ordering and shape bounds.
    pub fn validate(&self) -> Result<(), TrainingContractError> {
        if self.version != PARAMETER_SUBSET_CATALOG_VERSION {
            return Err(TrainingContractError::InvalidParameterSubsetCatalog(
                format!(
                    "unsupported parameter subset catalog version {}",
                    self.version
                ),
            ));
        }
        if self.model_schema_hash.as_str().is_empty() {
            return Err(TrainingContractError::InvalidParameterSubsetCatalog(
                "model schema hash must not be empty".into(),
            ));
        }
        if self.entries.is_empty() {
            return Err(TrainingContractError::InvalidParameterSubsetCatalog(
                "parameter subset catalog must contain at least one tensor".into(),
            ));
        }
        let mut previous_path: Option<&str> = None;
        for entry in &self.entries {
            entry.parameter_count()?;
            if previous_path.is_some_and(|previous| previous >= entry.path.as_str()) {
                return Err(TrainingContractError::InvalidParameterSubsetCatalog(
                    "parameter subset entries must be strictly path sorted and unique".into(),
                ));
            }
            previous_path = Some(entry.path.as_str());
        }
        self.parameter_count()?;
        Ok(())
    }

    /// Returns the total number of scalar values covered by this catalog.
    pub fn parameter_count(&self) -> Result<u64, TrainingContractError> {
        self.entries.iter().try_fold(0_u64, |count, entry| {
            count.checked_add(entry.parameter_count()?).ok_or_else(|| {
                TrainingContractError::InvalidParameterSubsetCatalog(
                    "parameter subset scalar count overflows u64".into(),
                )
            })
        })
    }

    /// Returns the canonical content identity of this catalog.
    pub fn catalog_id(&self) -> Result<ContentId, SchemaError> {
        self.content_id()
    }
}

impl CompactScalarEncoding {
    fn bytes_per_value(self) -> usize {
        match self {
            Self::Fp32 => 4,
            Self::SymmetricInt8 => 1,
            Self::SymmetricInt16 => 2,
        }
    }
}

/// A bounded, self-describing scalar vector for compact update transport.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactScalarVector {
    /// Scalar encoding.
    pub encoding: CompactScalarEncoding,
    /// Quantization scale. Must be one for `fp32`.
    pub scale: f32,
    /// Number of decoded scalars.
    pub value_count: u32,
    /// Packed scalar bytes.
    #[serde(with = "crate::codec::compact_bytes")]
    pub bytes: Vec<u8>,
}

impl CompactScalarVector {
    /// Encodes finite values using the requested representation.
    pub fn encode(
        values: &[f32],
        encoding: CompactScalarEncoding,
    ) -> Result<Self, TrainingContractError> {
        if values.len() > u32::MAX as usize {
            return Err(TrainingContractError::InvalidUpdatePayload(
                "compact scalar vector exceeds u32::MAX values".into(),
            ));
        }
        if values.iter().any(|value| !value.is_finite()) {
            return Err(TrainingContractError::InvalidUpdatePayload(
                "compact scalar vectors require finite values".into(),
            ));
        }

        let max_abs = values
            .iter()
            .fold(0.0_f32, |current, value| current.max(value.abs()));
        let (scale, bytes) = match encoding {
            CompactScalarEncoding::Fp32 => {
                let mut bytes = Vec::with_capacity(values.len() * 4);
                for value in values {
                    bytes.extend_from_slice(&value.to_le_bytes());
                }
                (1.0, bytes)
            }
            CompactScalarEncoding::SymmetricInt8 => {
                let scale = if max_abs == 0.0 { 1.0 } else { max_abs / 127.0 };
                let bytes = values
                    .iter()
                    .map(|value| (value / scale).round().clamp(-127.0, 127.0) as i8 as u8)
                    .collect();
                (scale, bytes)
            }
            CompactScalarEncoding::SymmetricInt16 => {
                let scale = if max_abs == 0.0 {
                    1.0
                } else {
                    max_abs / 32_767.0
                };
                let mut bytes = Vec::with_capacity(values.len() * 2);
                for value in values {
                    let quantized = (value / scale).round().clamp(-32_767.0, 32_767.0) as i16;
                    bytes.extend_from_slice(&quantized.to_le_bytes());
                }
                (scale, bytes)
            }
        };
        let vector = Self {
            encoding,
            scale,
            value_count: values.len() as u32,
            bytes,
        };
        vector.validate()?;
        Ok(vector)
    }

    /// Validates bounds and encoding invariants without allocating decoded values.
    pub fn validate(&self) -> Result<(), TrainingContractError> {
        if !self.scale.is_finite() || self.scale <= 0.0 {
            return Err(TrainingContractError::InvalidUpdatePayload(
                "compact scalar scale must be finite and positive".into(),
            ));
        }
        if self.encoding == CompactScalarEncoding::Fp32 && self.scale != 1.0 {
            return Err(TrainingContractError::InvalidUpdatePayload(
                "fp32 compact scalar scale must equal one".into(),
            ));
        }
        let expected = (self.value_count as usize)
            .checked_mul(self.encoding.bytes_per_value())
            .ok_or_else(|| {
                TrainingContractError::InvalidUpdatePayload(
                    "compact scalar byte length overflow".into(),
                )
            })?;
        if self.bytes.len() != expected {
            return Err(TrainingContractError::InvalidUpdatePayload(format!(
                "compact scalar payload has {} bytes, expected {expected}",
                self.bytes.len()
            )));
        }
        Ok(())
    }

    /// Decodes the scalar vector after validating all structural invariants.
    pub fn decode(&self) -> Result<Vec<f32>, TrainingContractError> {
        self.validate()?;
        let values = match self.encoding {
            CompactScalarEncoding::Fp32 => self
                .bytes
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect::<Vec<_>>(),
            CompactScalarEncoding::SymmetricInt8 => self
                .bytes
                .iter()
                .map(|value| f32::from(*value as i8) * self.scale)
                .collect(),
            CompactScalarEncoding::SymmetricInt16 => self
                .bytes
                .chunks_exact(2)
                .map(|chunk| f32::from(i16::from_le_bytes([chunk[0], chunk[1]])) * self.scale)
                .collect(),
        };
        if values.iter().any(|value| !value.is_finite()) {
            return Err(TrainingContractError::InvalidUpdatePayload(
                "decoded compact scalar vector contains non-finite values".into(),
            ));
        }
        Ok(values)
    }
}

/// Fitness observations for one deterministic zeroth-order generation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SeededFitnessGeneration {
    /// Monotonic optimizer generation.
    pub generation: u64,
    /// Digest of the exact input batch and objective inputs used for evaluation.
    pub batch_digest: ContentId,
    /// Content digests of exact records in batch order.
    #[serde(default)]
    pub record_digests: Vec<ContentId>,
    /// Whether recurrent state was reset before evaluating this batch.
    #[serde(default)]
    pub reset_stream_state: bool,
    /// Fitness values in `[pair0+, pair0-, pair1+, pair1-, ...]` order.
    pub fitness: CompactScalarVector,
}

/// Architecture-neutral compact update body.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CompactUpdateBody {
    /// Ordered values for one canonical mutable-parameter subset.
    MutableSubsetParameters {
        /// Parameter values flattened according to the payload's catalog.
        values: CompactScalarVector,
    },
    /// Canonically ordered deltas for one routed context generation.
    ContextSparseDelta {
        /// Slot, generation, family, and dynamic catalog identity.
        context: UpdateRoutingContext,
        /// Parameter deltas flattened according to the context catalog.
        deltas: CompactScalarVector,
    },
    /// Coefficients in a deterministic linear parameter subspace.
    SubspaceLatent {
        /// Number of subspace dimensions.
        dimensions: u32,
        /// Shared subspace seed.
        seed: u64,
        /// Transmitted update coefficients.
        coefficients: CompactScalarVector,
    },
    /// Forward-only observations for deterministic seeded perturbations.
    SeededFitness {
        /// Number of perturbations per generation, including antithetic signs.
        population: u32,
        /// Rank of each regenerated matrix perturbation.
        rank: u32,
        /// Shared population seed.
        seed: u64,
        /// Hash of the exact perturbation generator and parameter catalog.
        perturbation_generator_hash: ContentId,
        /// Hash of fitness normalization and parameter-update semantics.
        optimizer_update_hash: ContentId,
        /// Sequential generations evaluated against the declared base head.
        generations: Vec<SeededFitnessGeneration>,
    },
}

/// Compact, content-addressed update payload carried by a `DeltaPack` artifact.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompactUpdatePayload {
    /// Compact payload schema version.
    pub version: u16,
    /// Training contract that defines decode and update semantics.
    pub training_contract_id: ContentId,
    /// Model tensor schema.
    pub model_schema_hash: ContentId,
    /// Canonical parameter path, shape, and dtype catalog hash.
    pub parameter_catalog_hash: ContentId,
    /// Number of model parameters covered by the update.
    pub parameter_count: u64,
    /// Compact update representation.
    pub body: CompactUpdateBody,
}

impl CompactUpdatePayload {
    /// Validates the payload against the exact codec declared by its contract.
    pub fn validate_against_codec(&self, codec: &UpdateCodec) -> Result<(), TrainingContractError> {
        if self.version != COMPACT_UPDATE_PAYLOAD_VERSION {
            return Err(TrainingContractError::InvalidUpdatePayload(format!(
                "unsupported compact update payload version {}",
                self.version
            )));
        }
        if self.parameter_count == 0 {
            return Err(TrainingContractError::InvalidUpdatePayload(
                "compact update parameter_count must be positive".into(),
            ));
        }
        match (&self.body, codec) {
            (
                CompactUpdateBody::MutableSubsetParameters { values },
                UpdateCodec::MutableSubsetParameters {
                    parameter_catalog_hash,
                    parameter_count,
                    encoding,
                },
            ) if &self.parameter_catalog_hash == parameter_catalog_hash
                && &self.parameter_count == parameter_count
                && values.encoding == *encoding =>
            {
                values.validate()?;
                if u64::from(values.value_count) != self.parameter_count {
                    return Err(TrainingContractError::InvalidUpdatePayload(
                        "mutable-subset scalar count must equal parameter_count".into(),
                    ));
                }
            }
            (
                CompactUpdateBody::ContextSparseDelta { context, deltas },
                UpdateCodec::ContextSparseDelta {
                    context_family_hash,
                    max_parameter_count,
                    encoding,
                },
            ) if &context.context_family_hash == context_family_hash
                && context.parameter_catalog_hash == self.parameter_catalog_hash
                && self.parameter_count <= *max_parameter_count
                && deltas.encoding == *encoding =>
            {
                context.validate()?;
                deltas.validate()?;
                if self.parameter_count == 0
                    || u64::from(deltas.value_count) != self.parameter_count
                {
                    return Err(TrainingContractError::InvalidUpdatePayload(
                        "context-sparse scalar count must equal positive parameter_count".into(),
                    ));
                }
            }
            (
                CompactUpdateBody::SubspaceLatent {
                    dimensions,
                    seed,
                    coefficients,
                },
                UpdateCodec::SubspaceLatent {
                    dimensions: expected_dimensions,
                    seed: expected_seed,
                },
            ) if dimensions == expected_dimensions && seed == expected_seed => {
                coefficients.validate()?;
                if coefficients.value_count != *dimensions {
                    return Err(TrainingContractError::InvalidUpdatePayload(
                        "subspace coefficient count must equal dimensions".into(),
                    ));
                }
            }
            (
                CompactUpdateBody::SeededFitness {
                    population,
                    rank,
                    seed,
                    generations,
                    ..
                },
                UpdateCodec::SeededFitness {
                    population: expected_population,
                    rank: expected_rank,
                    seed: expected_seed,
                    ..
                },
            ) if population == expected_population
                && rank == expected_rank
                && seed == expected_seed =>
            {
                if generations.is_empty() {
                    return Err(TrainingContractError::InvalidUpdatePayload(
                        "seeded-fitness payload requires at least one generation".into(),
                    ));
                }
                let mut previous_generation = None;
                for generation in generations {
                    if generation.record_digests.is_empty() {
                        return Err(TrainingContractError::InvalidUpdatePayload(
                            "seeded-fitness replay requires exact record digests".into(),
                        ));
                    }
                    if generation.record_digests.len() > 4_096 {
                        return Err(TrainingContractError::InvalidUpdatePayload(
                            "seeded-fitness generation exceeds 4096 records".into(),
                        ));
                    }
                    generation.fitness.validate()?;
                    if generation.fitness.value_count != *population {
                        return Err(TrainingContractError::InvalidUpdatePayload(
                            "seeded-fitness scalar count must equal population".into(),
                        ));
                    }
                    if previous_generation
                        .is_some_and(|previous| generation.generation != previous + 1)
                    {
                        return Err(TrainingContractError::InvalidUpdatePayload(
                            "seeded-fitness generations must be contiguous".into(),
                        ));
                    }
                    previous_generation = Some(generation.generation);
                }
            }
            _ => return Err(TrainingContractError::UpdateContractMismatch),
        }
        Ok(())
    }

    /// Returns the canonical content identifier for this exact payload.
    pub fn payload_id(&self) -> Result<ContentId, SchemaError> {
        self.content_id()
    }
}

/// Complete hardware-neutral contract for executing one workload revision.
///
/// Backend, device name, memory size, and peer-local batch calibration are
/// deliberately absent. They are execution capabilities, not semantic
/// revision identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingContractManifest {
    /// Schema version for this contract.
    pub version: u16,
    /// Workload implementation selected by the revision.
    pub workload_id: WorkloadId,
    /// Hash of the model program and architecture configuration.
    pub model_program_hash: ContentId,
    /// Hash of the materialized model tensor schema.
    pub model_schema_hash: ContentId,
    /// Hash of the checkpoint format.
    pub checkpoint_format_hash: ContentId,
    /// Dataset view consumed by the revision.
    pub dataset_view_id: DatasetViewId,
    /// Hash of tokenizer or equivalent input vocabulary semantics.
    pub tokenizer_hash: ContentId,
    /// Hash of preprocessing, serialization, and batching semantics.
    pub preprocessing_hash: ContentId,
    /// Hash of all training objectives and regularizers.
    pub objective_hash: ContentId,
    /// Hash of optimizer configuration and parameter grouping.
    pub optimizer_hash: ContentId,
    /// Hash of scheduler configuration.
    pub scheduler_hash: ContentId,
    /// Local optimizer-state policy.
    pub optimizer_state_policy: LocalOptimizerStatePolicy,
    /// Scheduler cursor policy.
    pub scheduler_state_policy: SchedulerStatePolicy,
    /// Recurrent-state ownership policy.
    pub recurrent_state_policy: RecurrentStatePolicy,
    /// Update encoding emitted by peers.
    pub update_codec: UpdateCodec,
    /// Hash of aggregation and canonical outer-optimizer semantics.
    pub aggregation_hash: ContentId,
    /// Hash of validation, replay, and numerical tolerance semantics.
    pub validation_hash: ContentId,
    /// Hash of the initialization algorithm and seed contract.
    pub initialization_hash: ContentId,
    /// Additional stable workload-neutral extension hashes.
    #[serde(default)]
    pub extensions: BTreeMap<String, ContentId>,
}

impl TrainingContractManifest {
    /// Validates structural invariants before the contract is accepted.
    pub fn validate(&self) -> Result<(), TrainingContractError> {
        if self.version != TRAINING_CONTRACT_VERSION {
            return Err(TrainingContractError::UnsupportedVersion {
                found: self.version,
                supported: TRAINING_CONTRACT_VERSION,
            });
        }
        if let UpdateCodec::QuantizedBlock {
            bits, block_size, ..
        } = self.update_codec
        {
            if !(2..=16).contains(&bits) {
                return Err(TrainingContractError::InvalidUpdateCodec(
                    "quantized block bits must be in 2..=16".into(),
                ));
            }
            if block_size == 0 {
                return Err(TrainingContractError::InvalidUpdateCodec(
                    "quantized block size must be greater than zero".into(),
                ));
            }
        }
        match self.update_codec {
            UpdateCodec::MutableSubsetParameters {
                parameter_count: 0, ..
            } => {
                return Err(TrainingContractError::InvalidUpdateCodec(
                    "mutable-subset parameter count must be greater than zero".into(),
                ));
            }
            UpdateCodec::ContextSparseDelta {
                ref context_family_hash,
                max_parameter_count,
                ..
            } if context_family_hash.as_str().is_empty() || max_parameter_count == 0 => {
                return Err(TrainingContractError::InvalidUpdateCodec(
                    "context-sparse codec requires a family hash and positive parameter bound"
                        .into(),
                ));
            }
            UpdateCodec::SeededLowRank { rank, .. }
            | UpdateCodec::PowerSgd { rank, .. }
            | UpdateCodec::SeededFitness { rank, .. }
                if rank == 0 =>
            {
                return Err(TrainingContractError::InvalidUpdateCodec(
                    "low-rank codec rank must be greater than zero".into(),
                ));
            }
            UpdateCodec::SubspaceLatent { dimensions: 0, .. } => {
                return Err(TrainingContractError::InvalidUpdateCodec(
                    "subspace dimensions must be greater than zero".into(),
                ));
            }
            UpdateCodec::SeededFitness {
                population,
                ref replay,
                ..
            } => {
                if population == 0 || !population.is_multiple_of(2) {
                    return Err(TrainingContractError::InvalidUpdateCodec(
                        "seeded fitness population must be positive and even".into(),
                    ));
                }
                if replay.pairs_per_generation == 0 || replay.pairs_per_generation > population / 2
                {
                    return Err(TrainingContractError::InvalidUpdateCodec(
                        "seeded fitness replay pairs must be in 1..=population/2".into(),
                    ));
                }
                if replay.absolute_tolerance_micros == 0 && replay.relative_tolerance_ppm == 0 {
                    return Err(TrainingContractError::InvalidUpdateCodec(
                        "seeded fitness replay requires a non-zero numerical tolerance".into(),
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Returns the canonical semantic identity of the training contract.
    pub fn contract_id(&self) -> Result<ContentId, SchemaError> {
        self.content_id()
    }
}

/// Declares how a logical full model head is materialized from its artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum GenesisMaterialization {
    /// The artifact stores every tensor value needed by the model head.
    #[default]
    FullArtifact,
    /// Immutable tensors are regenerated and the artifact stores mutable state.
    DeterministicReconstruction {
        /// Stable generator implementation identifier required on every peer.
        generator_id: String,
        /// Hash of the complete deterministic reconstruction contract.
        reconstruction_contract_hash: ContentId,
        /// Catalog hash for tensors reconstructed rather than transmitted.
        immutable_parameter_catalog_hash: ContentId,
        /// Number of regenerated immutable scalar parameters.
        immutable_parameter_count: u64,
        /// Catalog hash for transmitted mutable tensors.
        mutable_parameter_catalog_hash: ContentId,
        /// Number of transmitted mutable scalar parameters.
        mutable_parameter_count: u64,
    },
}

/// Authority-controlled declaration of the unique model genesis artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelGenesisManifest {
    /// Experiment receiving this genesis.
    pub experiment_id: ExperimentId,
    /// Revision receiving this genesis.
    pub revision_id: RevisionId,
    /// Workload expected to load the artifact.
    pub workload_id: WorkloadId,
    /// Canonical training contract bound to the artifact.
    pub training_contract_id: ContentId,
    /// Full model artifact descriptor.
    pub artifact: ArtifactDescriptor,
    /// Digest of canonical tensor names, shapes, dtypes, and values.
    pub tensor_digest: ContentId,
    /// Stable initialization algorithm name.
    pub initialization_algorithm: String,
    /// Optional deterministic seed when genesis can be reproduced locally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initialization_seed: Option<u64>,
    /// Whether the artifact is complete bytes or a deterministic reconstruction bundle.
    #[serde(default)]
    pub materialization: GenesisMaterialization,
    /// Authority epoch that approved this genesis.
    pub authority_epoch: u64,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
}

impl ModelGenesisManifest {
    /// Validates that this is a complete, base-less model artifact.
    pub fn validate(&self) -> Result<(), TrainingContractError> {
        if self.artifact.kind != ArtifactKind::FullHead {
            return Err(TrainingContractError::GenesisNotFullHead);
        }
        if self.artifact.base_head_id.is_some() {
            return Err(TrainingContractError::GenesisHasBaseHead);
        }
        if self.artifact.model_schema_hash.as_str().is_empty()
            || self.artifact.head_id.is_none()
            || self.tensor_digest.as_str().is_empty()
            || self.initialization_algorithm.trim().is_empty()
        {
            return Err(TrainingContractError::IncompleteGenesis);
        }
        if let GenesisMaterialization::DeterministicReconstruction {
            generator_id,
            reconstruction_contract_hash,
            immutable_parameter_catalog_hash,
            immutable_parameter_count,
            mutable_parameter_catalog_hash,
            mutable_parameter_count,
        } = &self.materialization
            && (generator_id.trim().is_empty()
                || reconstruction_contract_hash.as_str().is_empty()
                || immutable_parameter_catalog_hash.as_str().is_empty()
                || *immutable_parameter_count == 0
                || mutable_parameter_catalog_hash.as_str().is_empty()
                || *mutable_parameter_count == 0)
        {
            return Err(TrainingContractError::IncompleteGenesis);
        }
        Ok(())
    }
}

/// Signed wire form used for model genesis distribution.
pub type SignedModelGenesisManifest = SignedPayload<SchemaEnvelope<ModelGenesisManifest>>;

/// Canonical authority payload binding revision policy, semantic training, and genesis.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RevisionContractAuthorityPayload {
    /// Existing revision and capability policy.
    pub revision: RevisionManifest,
    /// Canonical semantic identity for `training`.
    pub training_contract_id: ContentId,
    /// Complete hardware-neutral training contract.
    pub training: TrainingContractManifest,
    /// Canonical content identifier of the separately signed genesis payload.
    pub genesis_payload_id: ContentId,
}

/// Binds one legacy revision manifest to its complete semantic contract and
/// signed model genesis.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RevisionContractBundle {
    /// Existing revision and capability policy.
    pub revision: RevisionManifest,
    /// Canonical semantic identity for `training`.
    pub training_contract_id: ContentId,
    /// Complete hardware-neutral training contract.
    pub training: TrainingContractManifest,
    /// Authority-signed model genesis.
    pub genesis: SignedModelGenesisManifest,
    /// Authority signature over the complete revision/training/genesis binding.
    pub contract_signature: SignatureMetadata,
}

impl RevisionContractBundle {
    /// Returns the canonical payload covered by `contract_signature`.
    pub fn authority_payload(&self) -> RevisionContractAuthorityPayload {
        RevisionContractAuthorityPayload {
            revision: self.revision.clone(),
            training_contract_id: self.training_contract_id.clone(),
            training: self.training.clone(),
            genesis_payload_id: self.genesis.payload_id.clone(),
        }
    }

    /// Validates cross-object identities and structural invariants.
    pub fn validate(&self) -> Result<(), TrainingContractError> {
        self.training.validate()?;
        self.genesis.payload.payload.validate()?;

        let computed_contract_id = self.training.contract_id()?;
        if computed_contract_id != self.training_contract_id {
            return Err(TrainingContractError::ContractIdMismatch {
                declared: self.training_contract_id.clone(),
                computed: computed_contract_id,
            });
        }
        let computed_payload_id = self.genesis.payload.content_id()?;
        if computed_payload_id != self.genesis.payload_id {
            return Err(TrainingContractError::GenesisPayloadIdMismatch);
        }
        let genesis = &self.genesis.payload.payload;
        if self.genesis.payload.schema != "burn-p2p-model-genesis-v1"
            || genesis.experiment_id != self.revision.experiment_id
            || genesis.revision_id != self.revision.revision_id
            || genesis.workload_id != self.revision.workload_id
            || genesis.training_contract_id != self.training_contract_id
        {
            return Err(TrainingContractError::GenesisRevisionMismatch);
        }
        if self.training.workload_id != self.revision.workload_id
            || self.training.model_schema_hash != self.revision.model_schema_hash
            || self.training.checkpoint_format_hash != self.revision.checkpoint_format_hash
            || self.training.dataset_view_id != self.revision.dataset_view_id
            || self.revision.training_config_hash != self.training_contract_id
        {
            return Err(TrainingContractError::RevisionContractMismatch);
        }
        if genesis.artifact.model_schema_hash != self.training.model_schema_hash {
            return Err(TrainingContractError::GenesisModelSchemaMismatch);
        }
        if self.genesis.signature.key_id != MODEL_GENESIS_SIGNATURE_KEY_ID
            || self.genesis.signature.signature_hex.trim().is_empty()
        {
            return Err(TrainingContractError::MissingGenesisSignature);
        }
        if self.contract_signature.key_id != REVISION_CONTRACT_SIGNATURE_KEY_ID
            || self.contract_signature.signature_hex.trim().is_empty()
            || self.contract_signature.signer != self.genesis.signature.signer
        {
            return Err(TrainingContractError::MissingRevisionContractSignature);
        }
        Ok(())
    }
}

/// Host-produced update payload with untrusted claimed statistics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkloadUpdateEnvelope {
    /// Training contract used to produce the update.
    pub training_contract_id: ContentId,
    /// Revision used to produce the update.
    pub revision_id: RevisionId,
    /// Canonical base head.
    pub base_head_id: HeadId,
    /// Window that authorized the update.
    pub window_id: WindowId,
    /// Lease that authorized the update.
    pub lease_id: LeaseId,
    /// Encoding of the payload.
    pub codec: UpdateCodec,
    /// Context generation targeted by a context-routed update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_context: Option<UpdateRoutingContext>,
    /// Materialized payload artifact.
    pub artifact: ArtifactDescriptor,
    /// Optional peer-computed hash of decoded canonical update tensors.
    ///
    /// Forward-only compact peers may omit this to avoid materializing the
    /// full update. Validators always compute their own reconstruction digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decoded_tensor_digest: Option<ContentId>,
    /// Peer-claimed statistics. These are telemetry only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_norm_stats: Option<UpdateNormStats>,
    /// Peer-claimed feature sketch. This is telemetry only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_feature_sketch: Option<UpdateFeatureSketch>,
}

impl WorkloadUpdateEnvelope {
    /// Returns whether two envelopes identify the same contract-bound payload.
    ///
    /// Peer-claimed norms and feature sketches are deliberately excluded:
    /// they are untrusted telemetry, may be rounded by transports, and are
    /// independently recomputed by validators.
    pub fn same_contract_bound_payload(&self, other: &Self) -> bool {
        self.training_contract_id == other.training_contract_id
            && self.revision_id == other.revision_id
            && self.base_head_id == other.base_head_id
            && self.window_id == other.window_id
            && self.lease_id == other.lease_id
            && self.codec == other.codec
            && self.routing_context == other.routing_context
            && self.artifact == other.artifact
            && self.decoded_tensor_digest == other.decoded_tensor_digest
    }

    /// Validates identities that can be checked without decoding the payload.
    pub fn validate_against(
        &self,
        contract_id: &ContentId,
        contract: &TrainingContractManifest,
    ) -> Result<(), TrainingContractError> {
        if &self.training_contract_id != contract_id || self.codec != contract.update_codec {
            return Err(TrainingContractError::UpdateContractMismatch);
        }
        match (&self.codec, &self.routing_context) {
            (
                UpdateCodec::ContextSparseDelta {
                    context_family_hash,
                    ..
                },
                Some(context),
            ) if &context.context_family_hash == context_family_hash => context.validate()?,
            (UpdateCodec::ContextSparseDelta { .. }, _) => {
                return Err(TrainingContractError::UpdateContractMismatch);
            }
            (_, None) => {}
            (_, Some(_)) => return Err(TrainingContractError::UpdateContractMismatch),
        }
        if self.codec.requires_base_head()
            && self.artifact.base_head_id.as_ref() != Some(&self.base_head_id)
        {
            return Err(TrainingContractError::UpdateBaseHeadMismatch);
        }
        if self.artifact.model_schema_hash != contract.model_schema_hash {
            return Err(TrainingContractError::UpdateModelSchemaMismatch);
        }
        Ok(())
    }
}

/// Validator-computed bounded replay statistics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateReplayStats {
    /// Number of generations whose batch contract was reconstructed.
    pub generations_checked: u32,
    /// Number of antithetic pairs recomputed by the validator.
    pub pairs_checked: u32,
    /// Total antithetic pairs represented by the update.
    pub total_pairs: u32,
    /// Largest absolute fitness disagreement observed.
    pub max_absolute_error: f64,
    /// Largest relative fitness disagreement observed.
    pub max_relative_error: f64,
}

/// Validator-computed evidence. It is never populated from peer claims.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ValidatedUpdateEvidence {
    /// Hash of the envelope that was validated.
    pub update_envelope_id: ContentId,
    /// Validator-computed norm statistics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub norm_stats: Option<UpdateNormStats>,
    /// Validator-computed feature sketch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature_sketch: Option<UpdateFeatureSketch>,
    /// Whether deterministic decode/reconstruction succeeded.
    pub reconstruction_verified: bool,
    /// Whether bounded workload replay succeeded.
    pub replay_verified: bool,
    /// Validator-owned replay details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_stats: Option<UpdateReplayStats>,
    /// Validator identity.
    pub validator_peer_id: PeerId,
    /// Validation timestamp.
    pub validated_at: DateTime<Utc>,
}

/// Structural training-contract failures.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TrainingContractError {
    /// Schema version is unsupported.
    #[error("unsupported training contract version {found}; supported version is {supported}")]
    UnsupportedVersion { found: u16, supported: u16 },
    /// Update codec configuration is invalid.
    #[error("invalid update codec: {0}")]
    InvalidUpdateCodec(String),
    /// Compact update payload is malformed or violates declared bounds.
    #[error("invalid compact update payload: {0}")]
    InvalidUpdatePayload(String),
    /// Canonical parameter subset catalog is malformed.
    #[error("invalid parameter subset catalog: {0}")]
    InvalidParameterSubsetCatalog(String),
    /// Canonical schema encoding failed.
    #[error("canonical schema error: {0}")]
    Schema(String),
    /// Contract ID does not match its canonical content.
    #[error("training contract id mismatch: declared {declared}, computed {computed}")]
    ContractIdMismatch {
        declared: ContentId,
        computed: ContentId,
    },
    /// Signed genesis payload ID is invalid.
    #[error("signed genesis payload id does not match its canonical content")]
    GenesisPayloadIdMismatch,
    /// Genesis artifact is not a complete model head.
    #[error("model genesis artifact must be a full head")]
    GenesisNotFullHead,
    /// Genesis artifact unexpectedly names a base head.
    #[error("model genesis artifact must not name a base head")]
    GenesisHasBaseHead,
    /// Genesis is structurally incomplete.
    #[error("model genesis manifest is incomplete")]
    IncompleteGenesis,
    /// Genesis identities do not match the revision.
    #[error("model genesis identities do not match the revision contract")]
    GenesisRevisionMismatch,
    /// Revision fields do not match the complete training contract.
    #[error("revision manifest does not match its complete training contract")]
    RevisionContractMismatch,
    /// Genesis model schema is incompatible.
    #[error("model genesis schema does not match the training contract")]
    GenesisModelSchemaMismatch,
    /// Signed genesis lacks signature metadata.
    #[error("model genesis is not authority signed")]
    MissingGenesisSignature,
    /// Revision/training/genesis binding lacks an authority signature.
    #[error("revision contract is not authority signed")]
    MissingRevisionContractSignature,
    /// Update envelope is bound to a different contract or codec.
    #[error("update envelope does not match the training contract")]
    UpdateContractMismatch,
    /// Update base-head binding is invalid.
    #[error("update envelope base head does not match its artifact")]
    UpdateBaseHeadMismatch,
    /// Update model schema is incompatible.
    #[error("update model schema does not match the training contract")]
    UpdateModelSchemaMismatch,
}

impl From<SchemaError> for TrainingContractError {
    fn from(value: SchemaError) -> Self {
        Self::Schema(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use semver::Version;

    use super::*;

    fn content(value: &str) -> ContentId {
        ContentId::new(value)
    }

    fn contract() -> TrainingContractManifest {
        TrainingContractManifest {
            version: TRAINING_CONTRACT_VERSION,
            workload_id: WorkloadId::new("test-workload"),
            model_program_hash: content("model-program"),
            model_schema_hash: content("model-schema"),
            checkpoint_format_hash: content("checkpoint-format"),
            dataset_view_id: DatasetViewId::new("dataset-view"),
            tokenizer_hash: content("tokenizer"),
            preprocessing_hash: content("preprocessing"),
            objective_hash: content("objective"),
            optimizer_hash: content("optimizer"),
            scheduler_hash: content("scheduler"),
            optimizer_state_policy: LocalOptimizerStatePolicy::ResetPerWindow,
            scheduler_state_policy: SchedulerStatePolicy::CanonicalGlobalStep,
            recurrent_state_policy: RecurrentStatePolicy::LeaseScoped,
            update_codec: UpdateCodec::SeededLowRank { rank: 8, seed: 7 },
            aggregation_hash: content("aggregation"),
            validation_hash: content("validation"),
            initialization_hash: content("initialization"),
            extensions: BTreeMap::new(),
        }
    }

    #[test]
    fn peer_local_persistent_state_policies_have_stable_wire_names() {
        assert_eq!(
            serde_json::to_value(LocalOptimizerStatePolicy::PeerLocalPersistent)
                .expect("optimizer policy"),
            serde_json::json!("peer_local_persistent")
        );
        assert_eq!(
            serde_json::to_value(SchedulerStatePolicy::PeerLocalPersistent)
                .expect("scheduler policy"),
            serde_json::json!("peer_local_persistent")
        );
    }

    fn artifact() -> ArtifactDescriptor {
        ArtifactDescriptor {
            artifact_id: ArtifactId::new("genesis-artifact"),
            kind: ArtifactKind::FullHead,
            head_id: Some(HeadId::new("genesis-head")),
            base_head_id: None,
            precision: Precision::Fp32,
            model_schema_hash: content("model-schema"),
            record_format: "test".into(),
            bytes_len: 4,
            chunks: Vec::new(),
            root_hash: content("artifact-root"),
        }
    }

    fn revision(training_contract_id: ContentId) -> RevisionManifest {
        RevisionManifest {
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            workload_id: WorkloadId::new("test-workload"),
            required_release_train_hash: content("release"),
            model_schema_hash: content("model-schema"),
            checkpoint_format_hash: content("checkpoint-format"),
            dataset_view_id: DatasetViewId::new("dataset-view"),
            training_config_hash: training_contract_id,
            merge_topology_policy_hash: content("merge"),
            training_protocol: TrainingProtocol::default(),
            slot_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::new(),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: None,
                estimated_download_bytes: 0,
                estimated_window_seconds: 1,
            },
            activation_window: WindowActivation {
                activation_window: WindowId(0),
                grace_windows: 0,
            },
            lag_policy: LagPolicy::default(),
            merge_window_miss_policy: MergeWindowMissPolicy::default(),
            robustness_policy: None,
            browser_enabled: true,
            browser_role_policy: BrowserRolePolicy::default(),
            max_browser_checkpoint_bytes: None,
            max_browser_window_secs: None,
            max_browser_shard_bytes: None,
            requires_webgpu: false,
            max_browser_batch_size: None,
            recommended_browser_precision: None,
            visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
            description: "test".into(),
        }
    }

    fn bundle() -> RevisionContractBundle {
        let training = contract();
        let training_contract_id = training.contract_id().expect("contract id");
        let genesis = ModelGenesisManifest {
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            workload_id: WorkloadId::new("test-workload"),
            training_contract_id: training_contract_id.clone(),
            artifact: artifact(),
            tensor_digest: content("tensor-digest"),
            initialization_algorithm: "deterministic-test".into(),
            initialization_seed: Some(11),
            materialization: GenesisMaterialization::FullArtifact,
            authority_epoch: 1,
            created_at: Utc::now(),
        };
        let envelope =
            SchemaEnvelope::new("burn-p2p-model-genesis-v1", Version::new(0, 21, 0), genesis);
        let signed = SignedPayload::new(
            envelope,
            SignatureMetadata {
                signer: PeerId::new("authority"),
                key_id: MODEL_GENESIS_SIGNATURE_KEY_ID.into(),
                algorithm: SignatureAlgorithm::Ed25519,
                signed_at: Utc::now(),
                signature_hex: "00".into(),
            },
        )
        .expect("signed genesis");
        RevisionContractBundle {
            revision: revision(training_contract_id.clone()),
            training_contract_id,
            training,
            genesis: signed,
            contract_signature: SignatureMetadata {
                signer: PeerId::new("authority"),
                key_id: REVISION_CONTRACT_SIGNATURE_KEY_ID.into(),
                algorithm: SignatureAlgorithm::Ed25519,
                signed_at: Utc::now(),
                signature_hex: "00".into(),
            },
        }
    }

    #[test]
    fn hardware_does_not_change_training_contract_identity() {
        let contract = contract();
        let cuda_id = contract.contract_id().expect("cuda contract");
        let webgpu_id = contract.contract_id().expect("webgpu contract");
        assert_eq!(cuda_id, webgpu_id);
    }

    #[test]
    fn mutable_subset_catalog_and_payload_bind_order_shape_and_encoding() {
        let catalog = ParameterSubsetCatalog::new(
            content("model-schema"),
            vec![
                ParameterSubsetEntry {
                    path: "adapter.a".into(),
                    shape: vec![2, 3],
                },
                ParameterSubsetEntry {
                    path: "adapter.b".into(),
                    shape: vec![3, 2],
                },
            ],
        );
        catalog.validate().expect("catalog");
        let catalog_id = catalog.catalog_id().expect("catalog id");
        let values =
            CompactScalarVector::encode(&[0.25; 12], CompactScalarEncoding::SymmetricInt16)
                .expect("values");
        let codec = UpdateCodec::MutableSubsetParameters {
            parameter_catalog_hash: catalog_id.clone(),
            parameter_count: 12,
            encoding: CompactScalarEncoding::SymmetricInt16,
        };
        let payload = CompactUpdatePayload {
            version: COMPACT_UPDATE_PAYLOAD_VERSION,
            training_contract_id: content("contract"),
            model_schema_hash: content("model-schema"),
            parameter_catalog_hash: catalog_id,
            parameter_count: 12,
            body: CompactUpdateBody::MutableSubsetParameters { values },
        };

        payload.validate_against_codec(&codec).expect("payload");

        let wrong_catalog = UpdateCodec::MutableSubsetParameters {
            parameter_catalog_hash: content("different"),
            parameter_count: 12,
            encoding: CompactScalarEncoding::SymmetricInt16,
        };
        assert_eq!(
            payload
                .validate_against_codec(&wrong_catalog)
                .expect_err("catalog identity must match"),
            TrainingContractError::UpdateContractMismatch
        );
    }

    #[test]
    fn reconstructible_genesis_requires_complete_materialization_contract() {
        let mut bundle = bundle();
        bundle.genesis.payload.payload.materialization =
            GenesisMaterialization::DeterministicReconstruction {
                generator_id: String::new(),
                reconstruction_contract_hash: content("reconstruction"),
                immutable_parameter_catalog_hash: content("immutable"),
                immutable_parameter_count: 10,
                mutable_parameter_catalog_hash: content("mutable"),
                mutable_parameter_count: 2,
            };

        assert_eq!(
            bundle
                .genesis
                .payload
                .payload
                .validate()
                .expect_err("generator id is required"),
            TrainingContractError::IncompleteGenesis
        );
    }

    #[test]
    fn complete_revision_bundle_validates() {
        bundle().validate().expect("valid revision bundle");
    }

    #[test]
    fn revision_bundle_rejects_peer_local_genesis_substitution() {
        let mut bundle = bundle();
        bundle.genesis.payload.payload.artifact.root_hash = content("other-root");
        let error = bundle.validate().expect_err("mutated signed payload");
        assert_eq!(error, TrainingContractError::GenesisPayloadIdMismatch);
    }

    #[test]
    fn low_rank_update_is_bound_to_contract_and_base_head() {
        let contract = contract();
        let contract_id = contract.contract_id().expect("contract id");
        let mut update_artifact = artifact();
        update_artifact.kind = ArtifactKind::DeltaPack;
        update_artifact.base_head_id = Some(HeadId::new("base"));
        let envelope = WorkloadUpdateEnvelope {
            training_contract_id: contract_id.clone(),
            revision_id: RevisionId::new("revision"),
            base_head_id: HeadId::new("base"),
            window_id: WindowId(3),
            lease_id: LeaseId::new("lease"),
            codec: contract.update_codec.clone(),
            routing_context: None,
            artifact: update_artifact,
            decoded_tensor_digest: Some(content("decoded")),
            claimed_norm_stats: None,
            claimed_feature_sketch: None,
        };
        envelope
            .validate_against(&contract_id, &contract)
            .expect("matching update");

        let mut telemetry_variant = envelope.clone();
        telemetry_variant.claimed_norm_stats = Some(UpdateNormStats {
            l2_norm: 1.0 + f64::EPSILON,
            max_abs: 0.25,
            clipped: false,
            non_finite_tensors: 0,
        });
        assert!(envelope.same_contract_bound_payload(&telemetry_variant));

        telemetry_variant.decoded_tensor_digest = Some(content("different-decoded"));
        assert!(!envelope.same_contract_bound_payload(&telemetry_variant));
    }

    #[test]
    fn context_update_binds_generation_across_cbor_transport() {
        let mut contract = contract();
        contract.update_codec = UpdateCodec::ContextSparseDelta {
            context_family_hash: content("context-family"),
            max_parameter_count: 16,
            encoding: CompactScalarEncoding::SymmetricInt16,
        };
        let contract_id = contract.contract_id().expect("contract id");
        let mut update_artifact = artifact();
        update_artifact.kind = ArtifactKind::DeltaPack;
        update_artifact.base_head_id = Some(HeadId::new("base"));
        let context = UpdateRoutingContext {
            context_family_hash: content("context-family"),
            slot: 3,
            generation: 7,
            parameter_catalog_hash: content("context-catalog"),
        };
        let envelope = WorkloadUpdateEnvelope {
            training_contract_id: contract_id.clone(),
            revision_id: RevisionId::new("revision"),
            base_head_id: HeadId::new("base"),
            window_id: WindowId(3),
            lease_id: LeaseId::new("lease"),
            codec: contract.update_codec.clone(),
            routing_context: Some(context),
            artifact: update_artifact,
            decoded_tensor_digest: None,
            claimed_norm_stats: None,
            claimed_feature_sketch: None,
        };
        envelope
            .validate_against(&contract_id, &contract)
            .expect("matching context update");
        let bytes = crate::deterministic_cbor(&envelope).expect("encode context envelope");
        let decoded: WorkloadUpdateEnvelope =
            crate::from_cbor_slice(&bytes).expect("decode context envelope");
        assert_eq!(decoded, envelope);

        let mut stale = envelope.clone();
        stale.routing_context.as_mut().expect("context").generation = 6;
        assert!(!envelope.same_contract_bound_payload(&stale));

        let mut missing = envelope;
        missing.routing_context = None;
        assert_eq!(
            missing
                .validate_against(&contract_id, &contract)
                .expect_err("context codec requires identity"),
            TrainingContractError::UpdateContractMismatch
        );
    }

    #[test]
    fn compact_scalar_encodings_round_trip_with_bounded_error() {
        let values = [-2.0, -0.25, 0.0, 0.5, 3.0];
        for (encoding, tolerance) in [
            (CompactScalarEncoding::Fp32, 0.0),
            (CompactScalarEncoding::SymmetricInt16, 1.0e-4),
            (CompactScalarEncoding::SymmetricInt8, 0.025),
        ] {
            let encoded = CompactScalarVector::encode(&values, encoding).expect("encode");
            let decoded = encoded.decode().expect("decode");
            assert_eq!(decoded.len(), values.len());
            for (actual, expected) in decoded.iter().zip(values) {
                assert!(
                    (actual - expected).abs() <= tolerance,
                    "{encoding:?} decoded {actual}, expected {expected}"
                );
            }
        }
    }

    #[test]
    fn compact_scalar_vector_rejects_non_finite_and_truncated_payloads() {
        assert!(
            CompactScalarVector::encode(&[f32::NAN], CompactScalarEncoding::SymmetricInt8).is_err()
        );
        let mut encoded =
            CompactScalarVector::encode(&[1.0, -1.0], CompactScalarEncoding::SymmetricInt16)
                .expect("encode");
        encoded.bytes.pop();
        assert!(encoded.decode().is_err());
    }

    #[test]
    fn seeded_fitness_payload_is_contract_bound_and_canonical() {
        let codec = UpdateCodec::SeededFitness {
            population: 4,
            rank: 2,
            seed: 17,
            replay: SeededFitnessReplayPolicy::default(),
        };
        let payload = CompactUpdatePayload {
            version: COMPACT_UPDATE_PAYLOAD_VERSION,
            training_contract_id: content("training-contract"),
            model_schema_hash: content("model-schema"),
            parameter_catalog_hash: content("parameter-catalog"),
            parameter_count: 1_024,
            body: CompactUpdateBody::SeededFitness {
                population: 4,
                rank: 2,
                seed: 17,
                perturbation_generator_hash: content("generator"),
                optimizer_update_hash: content("update"),
                generations: vec![
                    SeededFitnessGeneration {
                        generation: 8,
                        batch_digest: content("batch-8"),
                        record_digests: vec![content("record-8")],
                        reset_stream_state: true,
                        fitness: CompactScalarVector::encode(
                            &[1.0, 2.0, 3.0, 4.0],
                            CompactScalarEncoding::SymmetricInt16,
                        )
                        .expect("fitness"),
                    },
                    SeededFitnessGeneration {
                        generation: 9,
                        batch_digest: content("batch-9"),
                        record_digests: vec![content("record-9")],
                        reset_stream_state: false,
                        fitness: CompactScalarVector::encode(
                            &[4.0, 3.0, 2.0, 1.0],
                            CompactScalarEncoding::SymmetricInt16,
                        )
                        .expect("fitness"),
                    },
                ],
            },
        };
        payload
            .validate_against_codec(&codec)
            .expect("matching payload");
        let payload_id = payload.payload_id().expect("payload id");
        let bytes = crate::deterministic_cbor(&payload).expect("encode payload");
        let decoded: CompactUpdatePayload = crate::from_cbor_slice(&bytes).expect("decode payload");
        assert_eq!(decoded.payload_id().expect("decoded id"), payload_id);

        let wrong_codec = UpdateCodec::SeededFitness {
            population: 8,
            rank: 2,
            seed: 17,
            replay: SeededFitnessReplayPolicy::default(),
        };
        assert_eq!(
            payload
                .validate_against_codec(&wrong_codec)
                .expect_err("population mismatch"),
            TrainingContractError::UpdateContractMismatch
        );
    }

    #[test]
    fn seeded_fitness_payload_rejects_non_contiguous_generations() {
        let codec = UpdateCodec::SeededFitness {
            population: 2,
            rank: 1,
            seed: 5,
            replay: SeededFitnessReplayPolicy::default(),
        };
        let fitness =
            CompactScalarVector::encode(&[0.0, 1.0], CompactScalarEncoding::Fp32).expect("fitness");
        let payload = CompactUpdatePayload {
            version: COMPACT_UPDATE_PAYLOAD_VERSION,
            training_contract_id: content("training-contract"),
            model_schema_hash: content("model-schema"),
            parameter_catalog_hash: content("parameter-catalog"),
            parameter_count: 4,
            body: CompactUpdateBody::SeededFitness {
                population: 2,
                rank: 1,
                seed: 5,
                perturbation_generator_hash: content("generator"),
                optimizer_update_hash: content("update"),
                generations: vec![
                    SeededFitnessGeneration {
                        generation: 1,
                        batch_digest: content("batch-1"),
                        record_digests: vec![content("record-1")],
                        reset_stream_state: true,
                        fitness: fitness.clone(),
                    },
                    SeededFitnessGeneration {
                        generation: 3,
                        batch_digest: content("batch-3"),
                        record_digests: vec![content("record-3")],
                        reset_stream_state: false,
                        fitness,
                    },
                ],
            },
        };
        assert!(payload.validate_against_codec(&codec).is_err());
    }
}
