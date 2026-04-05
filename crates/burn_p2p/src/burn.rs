use std::collections::BTreeMap;

use burn::{module::Module, prelude::Backend};

use crate::{
    ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard, ChunkingScheme,
    ClientReleaseManifest, ContentId, EvalSplit, FsArtifactStore, HeadId, MergeModelCandidate,
    MergePolicy, MetricReport, MetricValue, NodeBuilder, P2pWorkload, PatchOutcome, PatchSupport,
    Precision, RuntimePatch, SingleWorkloadProjectFamily, SupportedWorkload, TrainError, WindowCtx,
    WindowReport,
};

pub use burn::module::Module as BurnModule;
pub use burn::prelude::Backend as BurnBackend;
pub use burn_p2p_engine::{
    BurnArtifactBytes, BurnArtifactFile, BurnCheckpointer, BurnEvaluator, BurnLearner,
    BurnLearningCheckpointer, BurnMergeCandidate, BurnModuleInventory, BurnModuleParameter,
    BurnModuleTarget, BurnRecordBytesFormat, BurnRecordFileFormat, BurnRecordPrecision,
    BurnStoreFormat, BurnTensorKind, EngineError, apply_root_ema_modules, encode_record_bytes,
    encode_store_bytes, inspect_module, load_record_bytes, load_record_file, load_store_bytes,
    load_store_file, materialize_record_bytes_artifact, materialize_record_file_artifact,
    materialize_store_bytes_artifact, materialize_store_file_artifact, merge_weighted_mean_modules,
    module_schema_hash, save_record_file, save_store_file,
};

#[derive(Clone, Debug)]
/// Represents a record bytes runtime artifact options.
pub struct RecordBytesRuntimeArtifactOptions {
    /// The artifact kind.
    pub artifact_kind: ArtifactKind,
    /// The head ID.
    pub head_id: HeadId,
    /// The base head ID.
    pub base_head_id: Option<HeadId>,
    /// The format.
    pub format: BurnRecordBytesFormat,
    /// The precision.
    pub precision: BurnRecordPrecision,
    /// The chunking.
    pub chunking: ChunkingScheme,
}

#[derive(Clone, Debug)]
/// Represents a store bytes runtime artifact options.
pub struct StoreBytesRuntimeArtifactOptions {
    /// The artifact kind.
    pub artifact_kind: ArtifactKind,
    /// The head ID.
    pub head_id: HeadId,
    /// The base head ID.
    pub base_head_id: Option<HeadId>,
    /// The format.
    pub format: BurnStoreFormat,
    /// The declared precision.
    pub declared_precision: Precision,
    /// The chunking.
    pub chunking: ChunkingScheme,
}

/// Performs the load record bytes runtime artifact operation.
pub fn load_record_bytes_runtime_artifact<B, M>(
    module: M,
    descriptor: &ArtifactDescriptor,
    store: &FsArtifactStore,
    format: BurnRecordBytesFormat,
    precision: BurnRecordPrecision,
    device: &B::Device,
) -> Result<M, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    let bytes = store.materialize_artifact_bytes(descriptor)?;
    load_record_bytes::<B, M>(module, bytes, format, precision, device)
}

/// Performs the materialize record bytes runtime artifact operation.
pub fn materialize_record_bytes_runtime_artifact<B, M>(
    module: M,
    store: &FsArtifactStore,
    options: RecordBytesRuntimeArtifactOptions,
) -> Result<ArtifactDescriptor, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    let artifact = materialize_record_bytes_artifact::<B, M>(
        module,
        options.format,
        options.precision,
        options.artifact_kind,
        Some(options.head_id),
        options.base_head_id,
        options.chunking,
    )?;
    store.store_prebuilt_artifact_bytes(&artifact.descriptor, &artifact.bytes)?;
    Ok(artifact.descriptor)
}

/// Performs the load store bytes runtime artifact operation.
pub fn load_store_bytes_runtime_artifact<B, M>(
    mut module: M,
    descriptor: &ArtifactDescriptor,
    store: &FsArtifactStore,
    format: BurnStoreFormat,
) -> Result<M, EngineError>
where
    B: Backend,
    M: BurnModuleTarget<B>,
{
    let bytes = store.materialize_artifact_bytes(descriptor)?;
    load_store_bytes::<B, M>(&mut module, bytes, format)?;
    Ok(module)
}

/// Performs the materialize store bytes runtime artifact operation.
pub fn materialize_store_bytes_runtime_artifact<B, M>(
    module: &M,
    store: &FsArtifactStore,
    options: StoreBytesRuntimeArtifactOptions,
) -> Result<ArtifactDescriptor, EngineError>
where
    B: Backend,
    M: BurnModuleTarget<B>,
{
    let artifact = materialize_store_bytes_artifact::<B, M>(
        module,
        options.format,
        options.declared_precision,
        options.artifact_kind,
        Some(options.head_id),
        options.base_head_id,
        options.chunking,
    )?;
    store.store_prebuilt_artifact_bytes(&artifact.descriptor, &artifact.bytes)?;
    Ok(artifact.descriptor)
}

#[derive(Clone, Debug)]
/// Configures the checkpoint format used by [`BurnWorkloadAdapter`].
pub enum BurnArtifactConfig {
    /// Uses store bytes helpers such as burnpack or safetensors.
    StoreBytes {
        /// The store format.
        format: BurnStoreFormat,
        /// The declared precision stored in the descriptor.
        declared_precision: Precision,
        /// The chunking policy for artifact publication.
        chunking: ChunkingScheme,
    },
    /// Uses record bytes helpers such as named mpk or raw bin.
    RecordBytes {
        /// The record bytes format.
        format: BurnRecordBytesFormat,
        /// The recorder precision.
        precision: BurnRecordPrecision,
        /// The chunking policy for artifact publication.
        chunking: ChunkingScheme,
    },
}

impl BurnArtifactConfig {
    /// Returns a burnpack-backed artifact config with fp32 descriptor precision.
    pub fn burnpack(chunking: ChunkingScheme) -> Self {
        Self::StoreBytes {
            format: BurnStoreFormat::Burnpack,
            declared_precision: Precision::Fp32,
            chunking,
        }
    }

    /// Returns a safetensors-backed artifact config.
    pub fn safetensors(declared_precision: Precision, chunking: ChunkingScheme) -> Self {
        Self::StoreBytes {
            format: BurnStoreFormat::Safetensors,
            declared_precision,
            chunking,
        }
    }

    /// Returns a named-mpk record-bytes artifact config.
    pub fn named_mpk(precision: BurnRecordPrecision, chunking: ChunkingScheme) -> Self {
        Self::RecordBytes {
            format: BurnRecordBytesFormat::NamedMpk,
            precision,
            chunking,
        }
    }

    /// Returns a bin record-bytes artifact config.
    pub fn bin_bytes(precision: BurnRecordPrecision, chunking: ChunkingScheme) -> Self {
        Self::RecordBytes {
            format: BurnRecordBytesFormat::Bin,
            precision,
            chunking,
        }
    }
}

#[derive(Clone, Copy, Debug)]
/// Configures default merge behavior for [`BurnWorkloadAdapter`].
pub enum BurnMergeConfig {
    /// Defers merge behavior to the surrounding runtime.
    Disabled,
    /// Uses weighted-mean merge with the current `MergePolicy` quality weighting.
    WeightedMean,
    /// Uses weighted-mean merge followed by root ema.
    WeightedMeanWithRootEma {
        /// The ema decay applied after merge selection.
        decay: f64,
    },
}

#[derive(Clone, Debug)]
/// Configures a [`BurnWorkloadAdapter`].
pub struct BurnWorkloadConfig {
    /// The public workload descriptor advertised to the network.
    pub supported_workload: SupportedWorkload,
    /// The checkpoint serialization strategy used for model artifacts.
    pub artifact: BurnArtifactConfig,
    /// The merge behavior exposed to validators.
    pub merge: BurnMergeConfig,
}

impl BurnWorkloadConfig {
    /// Creates a new config with weighted-mean merge enabled.
    pub fn new(supported_workload: SupportedWorkload, artifact: BurnArtifactConfig) -> Self {
        Self {
            supported_workload,
            artifact,
            merge: BurnMergeConfig::WeightedMean,
        }
    }

    /// Disables default merge behavior.
    pub fn with_merge_disabled(mut self) -> Self {
        self.merge = BurnMergeConfig::Disabled;
        self
    }

    /// Enables root ema after weighted-mean merge.
    pub fn with_root_ema(mut self, decay: f64) -> Self {
        self.merge = BurnMergeConfig::WeightedMeanWithRootEma { decay };
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Selects the native node target preset for a Burn workload.
pub enum BurnNodeTarget {
    /// Trainer-oriented native node.
    Trainer,
    /// Authority / validator / archive native node.
    Validator,
    /// Custom native role set.
    Custom(crate::PeerRoleSet),
}

impl BurnNodeTarget {
    /// Returns the role set applied for the target preset.
    pub fn roles(&self) -> crate::PeerRoleSet {
        match self {
            Self::Trainer => crate::RoleSet::default_trainer(),
            Self::Validator => crate::PeerRoleSet::new([
                crate::PeerRole::Authority,
                crate::PeerRole::Validator,
                crate::PeerRole::Archive,
            ]),
            Self::Custom(roles) => roles.clone(),
        }
    }
}

/// Burn-oriented workload seam with defaults for checkpoint io and merge behavior.
///
/// Use this trait when the training logic already lives in a Burn-native project
/// and only the p2p wiring should be added. The adapter keeps `train_window`,
/// dataset hooks, and evaluation logic local to the workload while handling
/// model schema hashing, checkpoint materialization, and merge defaults.
pub trait BurnWorkload {
    /// The burn backend.
    type Backend: Backend;
    /// The burn model/module type.
    type Model: BurnModuleTarget<Self::Backend> + Clone;
    /// The batch type consumed by `train_window`.
    type Batch;
    /// Per-window stats surfaced in receipts and telemetry.
    type WindowStats;

    /// Initializes the model for the provided device.
    fn init_model(&self, device: &<Self::Backend as Backend>::Device) -> Self::Model;

    /// Reports runtime capability for the local device.
    fn benchmark(
        &self,
        model: &Self::Model,
        device: &<Self::Backend as Backend>::Device,
    ) -> crate::CapabilityEstimate;

    /// Runs one training window over the leased batches.
    fn train_window(
        &self,
        ctx: &mut WindowCtx<<Self::Backend as Backend>::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError>;

    /// Evaluates the current model on the requested split.
    fn evaluate(&self, model: &Self::Model, split: EvalSplit) -> MetricReport;

    /// Returns the runtime device used by the workload.
    fn runtime_device(&self) -> <Self::Backend as Backend>::Device;

    /// Returns the dataset registration used to plan microshards.
    fn dataset_registration(&self) -> anyhow::Result<crate::DatasetRegistration>;

    /// Builds the microshard plan for the registered dataset.
    fn microshard_plan(
        &self,
        registration: &crate::DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan>;

    /// Loads training batches from cached microshards.
    fn load_batches(
        &self,
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>>;

    /// Returns receipt metrics for a completed training window.
    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue>;

    /// Returns the contribution weight used for receipt scoring.
    fn contribution_weight(&self, _report: &WindowReport<Self::WindowStats>) -> f64 {
        1.0
    }

    /// Applies an optional runtime patch.
    fn apply_patch(&mut self, _patch: &RuntimePatch) -> PatchOutcome {
        PatchOutcome::Rejected("workload does not support runtime patches".into())
    }

    /// Declares which patch classes are accepted.
    fn supported_patch_classes(&self) -> PatchSupport {
        PatchSupport {
            hot: false,
            warm: false,
            cold: false,
        }
    }
}

#[derive(Clone, Debug)]
/// Adapter from [`BurnWorkload`] to [`crate::P2pWorkload`].
pub struct BurnWorkloadAdapter<W> {
    workload: W,
    supported_workload: SupportedWorkload,
    model_schema_hash: ContentId,
    artifact: BurnArtifactConfig,
    merge: BurnMergeConfig,
}

impl<W> BurnWorkloadAdapter<W>
where
    W: BurnWorkload,
{
    /// Creates a new adapter and derives the model schema hash from the initial module.
    pub fn try_new(workload: W, config: BurnWorkloadConfig) -> Result<Self, EngineError> {
        let device = workload.runtime_device();
        let model = workload.init_model(&device);
        let model_schema_hash = module_schema_hash::<W::Backend, _>(&model)?;

        Ok(Self {
            workload,
            supported_workload: config.supported_workload,
            model_schema_hash,
            artifact: config.artifact,
            merge: config.merge,
        })
    }

    /// Returns the wrapped workload.
    pub fn workload(&self) -> &W {
        &self.workload
    }

    /// Returns the wrapped workload mutably.
    pub fn workload_mut(&mut self) -> &mut W {
        &mut self.workload
    }

    /// Consumes the adapter and returns the wrapped workload.
    pub fn into_inner(self) -> W {
        self.workload
    }
}

/// Extension trait for converting a burn-native workload into a p2p workload.
pub trait IntoBurnWorkloadAdapter: BurnWorkload + Sized {
    /// Wraps the workload with default checkpoint and merge behavior.
    fn into_p2p_workload(
        self,
        config: BurnWorkloadConfig,
    ) -> Result<BurnWorkloadAdapter<Self>, EngineError> {
        BurnWorkloadAdapter::try_new(self, config)
    }
}

impl<W> IntoBurnWorkloadAdapter for W where W: BurnWorkload {}

/// Builds a native node builder from a Burn-native workload and target preset.
pub fn native<W>(
    target: BurnNodeTarget,
    release_manifest: ClientReleaseManifest,
    workload: W,
    config: BurnWorkloadConfig,
) -> anyhow::Result<NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<W>>>>
where
    W: BurnWorkload + Clone,
{
    Ok(node(release_manifest, workload, config)?.with_roles(target.roles()))
}

/// Builds a single-workload node builder from a Burn-native workload.
pub fn node<W>(
    release_manifest: ClientReleaseManifest,
    workload: W,
    config: BurnWorkloadConfig,
) -> anyhow::Result<NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<W>>>>
where
    W: BurnWorkload + Clone,
{
    let workload = workload
        .into_p2p_workload(config)
        .map_err(anyhow::Error::from)?;
    let family = SingleWorkloadProjectFamily::new(release_manifest, workload)?;
    Ok(NodeBuilder::new(family))
}

/// Builds a trainer-flavored node builder from a Burn-native workload.
pub fn trainer<W>(
    release_manifest: ClientReleaseManifest,
    workload: W,
    config: BurnWorkloadConfig,
) -> anyhow::Result<NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<W>>>>
where
    W: BurnWorkload + Clone,
{
    native(BurnNodeTarget::Trainer, release_manifest, workload, config)
}

/// Builds an authority/validator/archive-flavored node builder from a Burn-native workload.
pub fn validator<W>(
    release_manifest: ClientReleaseManifest,
    workload: W,
    config: BurnWorkloadConfig,
) -> anyhow::Result<NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<W>>>>
where
    W: BurnWorkload + Clone,
{
    native(
        BurnNodeTarget::Validator,
        release_manifest,
        workload,
        config,
    )
}

impl<W> P2pWorkload for BurnWorkloadAdapter<W>
where
    W: BurnWorkload,
{
    type Device = <W::Backend as Backend>::Device;
    type Model = W::Model;
    type Batch = W::Batch;
    type WindowStats = W::WindowStats;

    fn init_model(&self, device: &Self::Device) -> Self::Model {
        self.workload.init_model(device)
    }

    fn benchmark(&self, model: &Self::Model, device: &Self::Device) -> crate::CapabilityEstimate {
        self.workload.benchmark(model, device)
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<Self::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        self.workload.train_window(ctx)
    }

    fn evaluate(&self, model: &Self::Model, split: EvalSplit) -> MetricReport {
        self.workload.evaluate(model, split)
    }

    fn apply_patch(&mut self, patch: &RuntimePatch) -> PatchOutcome {
        self.workload.apply_patch(patch)
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        self.workload.supported_patch_classes()
    }

    fn runtime_device(&self) -> Self::Device {
        self.workload.runtime_device()
    }

    fn dataset_registration(&self) -> anyhow::Result<crate::DatasetRegistration> {
        self.workload.dataset_registration()
    }

    fn microshard_plan(
        &self,
        registration: &crate::DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        self.workload.microshard_plan(registration)
    }

    fn load_batches(
        &self,
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        self.workload.load_batches(lease, cached_microshards)
    }

    fn load_model_artifact(
        &self,
        model: Self::Model,
        descriptor: &ArtifactDescriptor,
        store: &FsArtifactStore,
        device: &Self::Device,
    ) -> anyhow::Result<Self::Model> {
        Ok(match &self.artifact {
            BurnArtifactConfig::StoreBytes { format, .. } => {
                load_store_bytes_runtime_artifact::<W::Backend, _>(
                    model, descriptor, store, *format,
                )?
            }
            BurnArtifactConfig::RecordBytes {
                format, precision, ..
            } => load_record_bytes_runtime_artifact::<W::Backend, _>(
                model, descriptor, store, *format, *precision, device,
            )?,
        })
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: ArtifactKind,
        head_id: HeadId,
        base_head_id: Option<HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor> {
        Ok(match &self.artifact {
            BurnArtifactConfig::StoreBytes {
                format,
                declared_precision,
                chunking,
            } => materialize_store_bytes_runtime_artifact::<W::Backend, _>(
                model,
                store,
                StoreBytesRuntimeArtifactOptions {
                    artifact_kind,
                    head_id,
                    base_head_id,
                    format: *format,
                    declared_precision: declared_precision.clone(),
                    chunking: *chunking,
                },
            )?,
            BurnArtifactConfig::RecordBytes {
                format,
                precision,
                chunking,
            } => materialize_record_bytes_runtime_artifact::<W::Backend, _>(
                model.clone(),
                store,
                RecordBytesRuntimeArtifactOptions {
                    artifact_kind,
                    head_id,
                    base_head_id,
                    format: *format,
                    precision: *precision,
                    chunking: *chunking,
                },
            )?,
        })
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        self.workload.contribution_metrics(report)
    }

    fn contribution_weight(&self, report: &WindowReport<Self::WindowStats>) -> f64 {
        self.workload.contribution_weight(report)
    }

    fn merge_candidate_models(
        &self,
        base_model: &Self::Model,
        candidates: &[MergeModelCandidate<'_, Self::Model>],
        policy: MergePolicy,
    ) -> anyhow::Result<Option<Self::Model>> {
        match self.merge {
            BurnMergeConfig::Disabled => Ok(None),
            BurnMergeConfig::WeightedMean | BurnMergeConfig::WeightedMeanWithRootEma { .. } => {
                let weighted = candidates
                    .iter()
                    .map(|candidate| {
                        let quality = match policy {
                            MergePolicy::WeightedMean => 1.0,
                            MergePolicy::NormClippedWeightedMean => {
                                candidate.quality_weight.clamp(0.0, 1.0)
                            }
                            MergePolicy::TrimmedMean => candidate.quality_weight,
                            MergePolicy::Ema | MergePolicy::QualityWeightedEma => {
                                candidate.quality_weight
                            }
                            MergePolicy::Custom(_) => candidate.quality_weight,
                        };
                        BurnMergeCandidate {
                            module: candidate.model,
                            weight: candidate.sample_weight * quality,
                        }
                    })
                    .collect::<Vec<_>>();

                Ok(merge_weighted_mean_modules::<W::Backend, _>(
                    base_model, &weighted,
                )?)
            }
        }
    }

    fn apply_single_root_ema(
        &self,
        base_model: &Self::Model,
        merged_model: Self::Model,
        _policy: MergePolicy,
    ) -> anyhow::Result<Self::Model> {
        Ok(match self.merge {
            BurnMergeConfig::WeightedMeanWithRootEma { decay } => {
                apply_root_ema_modules::<W::Backend, _>(base_model, &merged_model, decay)?
            }
            BurnMergeConfig::Disabled | BurnMergeConfig::WeightedMean => merged_model,
        })
    }

    fn supported_workload(&self) -> SupportedWorkload {
        self.supported_workload.clone()
    }

    fn model_schema_hash(&self) -> ContentId {
        self.model_schema_hash.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn::{
        backend::NdArray,
        module::Module,
        nn::{Linear, LinearConfig},
        tensor::backend::Backend,
    };
    use chrono::Utc;

    type TestBackend = NdArray<f32>;

    #[derive(Module, Debug)]
    struct TinyModel<B: Backend> {
        linear: Linear<B>,
    }

    impl<B: Backend> TinyModel<B> {
        fn new(device: &B::Device) -> Self {
            Self {
                linear: LinearConfig::new(4, 2).init(device),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct TinyBurnWorkload;

    impl BurnWorkload for TinyBurnWorkload {
        type Backend = TestBackend;
        type Model = TinyModel<TestBackend>;
        type Batch = f32;
        type WindowStats = BTreeMap<String, MetricValue>;

        fn init_model(&self, device: &<Self::Backend as Backend>::Device) -> Self::Model {
            TinyModel::new(device)
        }

        fn benchmark(
            &self,
            _model: &Self::Model,
            _device: &<Self::Backend as Backend>::Device,
        ) -> crate::CapabilityEstimate {
            crate::CapabilityEstimate {
                preferred_backends: vec!["ndarray".into()],
                work_units_per_second: 16.0,
                target_window_seconds: 1,
            }
        }

        fn train_window(
            &self,
            _ctx: &mut WindowCtx<<Self::Backend as Backend>::Device, Self::Model, Self::Batch>,
        ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
            Ok(WindowReport {
                contribution: None,
                stats: BTreeMap::from([("loss".into(), MetricValue::Float(0.0))]),
                completed_at: Utc::now(),
            })
        }

        fn evaluate(&self, _model: &Self::Model, _split: EvalSplit) -> MetricReport {
            MetricReport {
                metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.0))]),
                captured_at: Utc::now(),
            }
        }

        fn runtime_device(&self) -> <Self::Backend as Backend>::Device {
            <TestBackend as Backend>::Device::default()
        }

        fn dataset_registration(&self) -> anyhow::Result<crate::DatasetRegistration> {
            Ok(crate::DatasetRegistration {
                manifest: crate::DatasetManifest {
                    dataset_id: crate::DatasetId::new("tiny-dataset"),
                    source_uri: ".".into(),
                    format: "microshards".into(),
                    manifest_hash: ContentId::new("tiny-manifest"),
                    metadata: BTreeMap::new(),
                },
                view: crate::DatasetView {
                    dataset_view_id: crate::DatasetViewId::new("tiny-view"),
                    dataset_id: crate::DatasetId::new("tiny-dataset"),
                    preprocessing_hash: ContentId::new("tiny-preprocess"),
                    tokenizer_hash: None,
                    manifest_hash: ContentId::new("tiny-manifest"),
                    metadata: BTreeMap::new(),
                },
                upstream: crate::UpstreamAdapter::Local { root: ".".into() },
            })
        }

        fn microshard_plan(
            &self,
            registration: &crate::DatasetRegistration,
        ) -> anyhow::Result<crate::MicroShardPlan> {
            Ok(
                crate::MicroShardPlanner::new(crate::MicroShardPlannerConfig {
                    target_microshard_bytes: 16,
                    min_microshards: 1,
                    max_microshards: 1,
                })?
                .plan(
                    &registration.view,
                    crate::DatasetSizing {
                        total_examples: 1,
                        total_tokens: 1,
                        total_bytes: 16,
                    },
                )?,
            )
        }

        fn load_batches(
            &self,
            _lease: &AssignmentLease,
            _cached_microshards: &[CachedMicroShard],
        ) -> anyhow::Result<Vec<Self::Batch>> {
            Ok(vec![1.0])
        }

        fn contribution_metrics(
            &self,
            report: &WindowReport<Self::WindowStats>,
        ) -> BTreeMap<String, MetricValue> {
            report.stats.clone()
        }
    }

    #[test]
    fn burn_workload_adapter_derives_schema_and_manifest() {
        let supported_workload = SupportedWorkload {
            workload_id: crate::WorkloadId::new("tiny-workload"),
            workload_name: "Tiny Burn Workload".into(),
            model_program_hash: ContentId::new("tiny-program"),
            checkpoint_format_hash: ContentId::new("tiny-burnpack"),
            supported_revision_family: ContentId::new("tiny-family"),
            resource_class: "cpu".into(),
        };
        let config = BurnWorkloadConfig::new(
            supported_workload.clone(),
            BurnArtifactConfig::burnpack(ChunkingScheme::new(64).expect("chunking")),
        )
        .with_root_ema(0.99);
        let adapter = TinyBurnWorkload
            .into_p2p_workload(config)
            .expect("adapter should build");

        assert_eq!(adapter.supported_workload(), supported_workload);
        assert_eq!(
            adapter.checkpoint_format_hash(),
            ContentId::new("tiny-burnpack")
        );
        assert!(!adapter.model_schema_hash().as_str().is_empty());
    }

    #[test]
    fn trainer_builder_wraps_single_burn_workload_with_trainer_roles() {
        let supported_workload = SupportedWorkload {
            workload_id: crate::WorkloadId::new("tiny-workload"),
            workload_name: "Tiny Burn Workload".into(),
            model_program_hash: ContentId::new("tiny-program"),
            checkpoint_format_hash: ContentId::new("tiny-burnpack"),
            supported_revision_family: ContentId::new("tiny-family"),
            resource_class: "cpu".into(),
        };
        let release_manifest = ClientReleaseManifest {
            project_family_id: crate::ProjectFamilyId::new("tiny-family"),
            release_train_hash: ContentId::new("tiny-train"),
            target_artifact_id: "native-linux-x86_64".into(),
            target_artifact_hash: ContentId::new("tiny-artifact"),
            target_platform: crate::ClientPlatform::Native,
            app_semver: semver::Version::new(0, 1, 0),
            git_commit: "local".into(),
            cargo_lock_hash: ContentId::new("tiny-lock"),
            burn_version_string: "0.21.0-pre.2".into(),
            enabled_features_hash: ContentId::new("tiny-features"),
            protocol_major: 0,
            supported_workloads: vec![supported_workload.clone()],
            built_at: Utc::now(),
        };
        let network_manifest = crate::NetworkManifest {
            network_id: crate::NetworkId::new("tiny-network"),
            project_family_id: release_manifest.project_family_id.clone(),
            protocol_major: release_manifest.protocol_major,
            required_release_train_hash: release_manifest.release_train_hash.clone(),
            allowed_target_artifact_hashes: std::collections::BTreeSet::from([release_manifest
                .target_artifact_hash
                .clone()]),
            authority_public_keys: Vec::new(),
            bootstrap_addrs: Vec::new(),
            auth_policy_hash: ContentId::new("tiny-auth-policy"),
            created_at: Utc::now(),
            description: "tiny network".into(),
        };
        let builder = trainer(
            release_manifest,
            TinyBurnWorkload,
            BurnWorkloadConfig::new(
                supported_workload.clone(),
                BurnArtifactConfig::burnpack(ChunkingScheme::new(64).expect("chunking")),
            ),
        )
        .expect("trainer builder")
        .with_network(network_manifest)
        .expect("network binding");
        let prepared = builder.prepare().expect("prepared node");

        assert_eq!(prepared.mainnet().roles, crate::RoleSet::default_trainer());
        assert_eq!(
            prepared.config().selected_workload_id,
            Some(supported_workload.workload_id)
        );
    }

    #[test]
    fn validator_builder_wraps_single_burn_workload_with_validator_roles() {
        let supported_workload = SupportedWorkload {
            workload_id: crate::WorkloadId::new("tiny-workload"),
            workload_name: "Tiny Burn Workload".into(),
            model_program_hash: ContentId::new("tiny-program"),
            checkpoint_format_hash: ContentId::new("tiny-burnpack"),
            supported_revision_family: ContentId::new("tiny-family"),
            resource_class: "cpu".into(),
        };
        let release_manifest = ClientReleaseManifest {
            project_family_id: crate::ProjectFamilyId::new("tiny-family"),
            release_train_hash: ContentId::new("tiny-train"),
            target_artifact_id: "native-linux-x86_64".into(),
            target_artifact_hash: ContentId::new("tiny-artifact"),
            target_platform: crate::ClientPlatform::Native,
            app_semver: semver::Version::new(0, 1, 0),
            git_commit: "local".into(),
            cargo_lock_hash: ContentId::new("tiny-lock"),
            burn_version_string: "0.21.0-pre.2".into(),
            enabled_features_hash: ContentId::new("tiny-features"),
            protocol_major: 0,
            supported_workloads: vec![supported_workload.clone()],
            built_at: Utc::now(),
        };
        let network_manifest = crate::NetworkManifest {
            network_id: crate::NetworkId::new("tiny-network"),
            project_family_id: release_manifest.project_family_id.clone(),
            protocol_major: release_manifest.protocol_major,
            required_release_train_hash: release_manifest.release_train_hash.clone(),
            allowed_target_artifact_hashes: std::collections::BTreeSet::from([release_manifest
                .target_artifact_hash
                .clone()]),
            authority_public_keys: Vec::new(),
            bootstrap_addrs: Vec::new(),
            auth_policy_hash: ContentId::new("tiny-auth-policy"),
            created_at: Utc::now(),
            description: "tiny network".into(),
        };
        let builder = validator(
            release_manifest,
            TinyBurnWorkload,
            BurnWorkloadConfig::new(
                supported_workload.clone(),
                BurnArtifactConfig::burnpack(ChunkingScheme::new(64).expect("chunking")),
            ),
        )
        .expect("validator builder")
        .with_network(network_manifest)
        .expect("network binding");
        let prepared = builder.prepare().expect("prepared node");

        assert_eq!(
            prepared.mainnet().roles,
            crate::PeerRoleSet::new([
                crate::PeerRole::Authority,
                crate::PeerRole::Validator,
                crate::PeerRole::Archive
            ])
        );
        assert_eq!(
            prepared.config().selected_workload_id,
            Some(supported_workload.workload_id)
        );
    }

    #[test]
    fn native_builder_accepts_explicit_target_preset() {
        let supported_workload = SupportedWorkload {
            workload_id: crate::WorkloadId::new("tiny-workload"),
            workload_name: "Tiny Burn Workload".into(),
            model_program_hash: ContentId::new("tiny-program"),
            checkpoint_format_hash: ContentId::new("tiny-burnpack"),
            supported_revision_family: ContentId::new("tiny-family"),
            resource_class: "cpu".into(),
        };
        let release_manifest = ClientReleaseManifest {
            project_family_id: crate::ProjectFamilyId::new("tiny-family"),
            release_train_hash: ContentId::new("tiny-train"),
            target_artifact_id: "native-linux-x86_64".into(),
            target_artifact_hash: ContentId::new("tiny-artifact"),
            target_platform: crate::ClientPlatform::Native,
            app_semver: semver::Version::new(0, 1, 0),
            git_commit: "local".into(),
            cargo_lock_hash: ContentId::new("tiny-lock"),
            burn_version_string: "0.21.0-pre.2".into(),
            enabled_features_hash: ContentId::new("tiny-features"),
            protocol_major: 0,
            supported_workloads: vec![supported_workload.clone()],
            built_at: Utc::now(),
        };

        let builder = native(
            BurnNodeTarget::Trainer,
            release_manifest,
            TinyBurnWorkload,
            BurnWorkloadConfig::new(
                supported_workload,
                BurnArtifactConfig::burnpack(ChunkingScheme::new(64).expect("chunking")),
            ),
        )
        .expect("native builder");

        assert_eq!(builder.config().selected_workload_id, None);
    }
}
