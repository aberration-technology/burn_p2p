use std::collections::BTreeMap;

use burn::{
    module::{AutodiffModule, Module},
    optim::{Optimizer, lr_scheduler::LrScheduler},
    prelude::Backend,
    tensor::backend::AutodiffBackend,
    train::{InferenceStep, LearningComponentsMarker, LearningComponentsTypes, TrainStep},
};
use chrono::Utc;

use crate::{
    ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard, ChunkingScheme,
    ClientReleaseManifest, ContentId, EvalSplit, FsArtifactStore, HeadId, MergeModelCandidate,
    MergePolicy, MetricReport, MetricValue, NodeBuilder, P2pWorkload, PatchOutcome, PatchSupport,
    Precision, RuntimePatch, SingleWorkloadProjectFamily, SupportedWorkload, TrainError,
    TrainerCanonicalReconcileStrategy, WindowCtx, WindowReport,
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

/// Type alias for the burn learning components used by [`BurnLearnerWorkload`].
pub type BurnLearningComponents<W> = LearningComponentsMarker<
    <W as BurnLearnerWorkload>::Backend,
    <W as BurnLearnerWorkload>::Scheduler,
    <W as BurnLearnerWorkload>::Model,
    <W as BurnLearnerWorkload>::Optimizer,
>;

/// Type alias for the burn learner used by [`BurnLearnerWorkload`].
pub type BurnWorkloadLearner<W> = BurnLearner<BurnLearningComponents<W>>;

/// Type alias for the burn backend used by a concrete learner.
pub type BurnLearnerBackend<LC> = <LC as LearningComponentsTypes>::Backend;

/// Type alias for the burn device used by a concrete learner.
pub type BurnLearnerDevice<LC> = <<LC as LearningComponentsTypes>::Backend as Backend>::Device;

/// Type alias for the training model used by a concrete learner.
pub type BurnLearnerModel<LC> = <LC as LearningComponentsTypes>::TrainingModel;

/// Type alias for the inference model used by a concrete learner.
pub type BurnLearnerEvalModel<LC> = <LC as LearningComponentsTypes>::InferenceModel;

/// Type alias for the training batch input used by a concrete learner.
pub type BurnLearnerBatch<LC> =
    <<LC as LearningComponentsTypes>::TrainingModel as TrainStep>::Input;

/// Type alias for the training step output used by a concrete learner.
pub type BurnLearnerOutput<LC> =
    <<LC as LearningComponentsTypes>::TrainingModel as TrainStep>::Output;

/// Type alias for burn's supervised training dataloader.
pub type BurnTrainLoader<LC> = burn::train::TrainLoader<LC>;

/// Type alias for burn's upstream validation dataloader.
pub type BurnValidationLoader<LC> = burn::train::ValidLoader<LC>;

/// Type alias for the training batch input used by [`BurnLearnerWorkload`].
pub type BurnTrainBatch<W> = <<W as BurnLearnerWorkload>::Model as TrainStep>::Input;

/// Type alias for the per-step training output used by [`BurnLearnerWorkload`].
pub type BurnTrainOutput<W> = <<W as BurnLearnerWorkload>::Model as TrainStep>::Output;

/// Type alias for the inference model derived from [`BurnLearnerWorkload`].
pub type BurnEvalModel<W> = <W as BurnLearnerWorkload>::EvalModel;

type LearnerBenchmarkFn<LC> = dyn Fn(&BurnLearnerModel<LC>, &BurnLearnerDevice<LC>) -> crate::CapabilityEstimate
    + Send
    + Sync;
type LearnerEvaluateFn<LC> =
    dyn Fn(&BurnLearnerEvalModel<LC>, EvalSplit) -> MetricReport + Send + Sync;
type LearnerBatchLoaderFn<LC> = dyn Fn(
        &AssignmentLease,
        &[CachedMicroShard],
        &BurnLearnerDevice<LC>,
    ) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
    + Send
    + Sync;
type LearnerStepMetricFn<LC> = dyn Fn(usize, &BurnLearnerOutput<LC>, &mut BTreeMap<String, MetricValue>) -> Result<(), TrainError>
    + Send
    + Sync;
type LearnerWindowMetricFn<LC> = dyn Fn(&BurnLearner<LC>, &mut BTreeMap<String, MetricValue>) -> Result<(), TrainError>
    + Send
    + Sync;

pub(crate) fn extend_window_metrics_with_cached_microshard_counts(
    metrics: &mut BTreeMap<String, MetricValue>,
    cached_microshards: &[CachedMicroShard],
) {
    let examples_processed = cached_microshards
        .iter()
        .map(|cached| cached.microshard.estimated_examples)
        .sum::<u64>();
    let tokens_processed = cached_microshards
        .iter()
        .map(|cached| cached.microshard.estimated_tokens)
        .sum::<u64>();

    if examples_processed > 0 {
        metrics.insert(
            "examples_processed".into(),
            MetricValue::Integer(examples_processed as i64),
        );
    }
    if tokens_processed > 0 {
        metrics.insert(
            "tokens_processed".into(),
            MetricValue::Integer(tokens_processed as i64),
        );
    }
    if !cached_microshards.is_empty() {
        metrics.insert(
            "microshard_count".into(),
            MetricValue::Integer(cached_microshards.len() as i64),
        );
    }
}

/// Type alias for a lease/micro-epoch data pipeline backing one burn learner.
pub type BurnLearnerDataPipeline<LC> =
    crate::LeaseDataPipeline<BurnLearnerDevice<LC>, BurnLearnerBatch<LC>>;

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
    /// Default post-merge root-ema decay used by [`Self::standard`].
    pub const fn standard_root_ema_decay() -> f64 {
        0.35
    }

    /// Creates a new config with weighted-mean merge enabled.
    pub fn new(supported_workload: SupportedWorkload, artifact: BurnArtifactConfig) -> Self {
        Self {
            supported_workload,
            artifact,
            merge: BurnMergeConfig::WeightedMean,
        }
    }

    /// Creates the default burn config used by the learner-first happy path.
    ///
    /// Uses burnpack artifacts with the default chunking scheme and applies one
    /// root-ema smoothing step after weighted-mean merge.
    pub fn standard(supported_workload: SupportedWorkload) -> Self {
        Self::new(
            supported_workload,
            BurnArtifactConfig::burnpack(ChunkingScheme::default()),
        )
        .with_root_ema(Self::standard_root_ema_decay())
    }

    /// Disables default merge behavior.
    pub fn with_merge_disabled(mut self) -> Self {
        self.merge = BurnMergeConfig::Disabled;
        self
    }

    /// Forces plain weighted-mean merge without post-merge root ema.
    pub fn with_plain_weighted_mean(mut self) -> Self {
        self.merge = BurnMergeConfig::WeightedMean;
        self
    }

    /// Enables root ema after weighted-mean merge.
    pub fn with_root_ema(mut self, decay: f64) -> Self {
        self.merge = BurnMergeConfig::WeightedMeanWithRootEma { decay };
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Selects the node target preset for a Burn workload.
pub enum BurnTarget {
    /// Trainer-oriented native node.
    Trainer,
    /// Reducer-oriented native node.
    Reducer,
    /// Authority / validator / archive native node.
    Validator,
    /// Custom native role set.
    Custom(crate::PeerRoleSet),
}

impl BurnTarget {
    /// Returns the role set applied for the target preset.
    pub fn roles(&self) -> crate::PeerRoleSet {
        match self {
            Self::Trainer => crate::RoleSet::default_trainer(),
            Self::Reducer => crate::PeerRoleSet::new([crate::PeerRole::Reducer]),
            Self::Validator => crate::PeerRoleSet::new([
                crate::PeerRole::Authority,
                crate::PeerRole::Validator,
                crate::PeerRole::Archive,
            ]),
            Self::Custom(roles) => roles.clone(),
        }
    }

    /// Returns whether the target can issue local training windows.
    pub fn requires_training_hooks(&self) -> bool {
        let roles = self.roles();
        roles.contains(&crate::PeerRole::TrainerCpu)
            || roles.contains(&crate::PeerRole::TrainerGpu)
            || roles.contains(&crate::PeerRole::BrowserTrainer)
            || roles.contains(&crate::PeerRole::BrowserTrainerWgpu)
    }
}

mod dataset;
mod learner;
pub use dataset::{BurnShardedDataset, BurnShardedDatasetConfig};
pub use learner::{BurnLearnerProject, BurnLearnerProjectBuilder, from_learner, from_loaders};

/// Advanced learner-backed burn integration seam.
///
/// Prefer [`from_learner`] for the common path that starts from a burn
/// `Learner::new(...)`.
///
/// Use this trait when the same hooks should live on a reusable project type.
/// The default adapter rebuilds a `Learner` from the current model head, runs
/// one burn-native window, records a small metric map, and reuses the existing
/// artifact + merge helpers.
pub trait BurnLearnerWorkload {
    /// Burn autodiff backend used for local training.
    type Backend: AutodiffBackend;
    /// Burn training model.
    type Model: BurnModuleTarget<Self::Backend>
        + TrainStep
        + AutodiffModule<Self::Backend, InnerModule = Self::EvalModel>
        + Clone
        + core::fmt::Display
        + 'static;
    /// Burn inference model produced by `valid()`.
    type EvalModel: BurnModuleTarget<<Self::Backend as AutodiffBackend>::InnerBackend>
        + InferenceStep
        + Clone
        + 'static;
    /// Burn optimizer used by the learner.
    type Optimizer: Optimizer<Self::Model, Self::Backend> + Clone + 'static;
    /// Burn scheduler used by the learner.
    type Scheduler: LrScheduler + Clone + 'static;

    /// Creates the initial learner for the provided device.
    fn init_learner(
        &self,
        device: &<Self::Backend as Backend>::Device,
    ) -> BurnWorkloadLearner<Self>;

    /// Restores a learner from the current p2p model head.
    fn restore_learner(
        &self,
        model: Self::Model,
        device: &<Self::Backend as Backend>::Device,
    ) -> BurnWorkloadLearner<Self> {
        let mut learner = self.init_learner(device);
        learner.load_model(model.into_record());
        learner
    }

    /// Initializes the starting model from the learner.
    fn init_model(&self, device: &<Self::Backend as Backend>::Device) -> Self::Model {
        self.init_learner(device).model()
    }

    /// Reports runtime capability for the local device.
    fn benchmark(
        &self,
        model: &Self::Model,
        device: &<Self::Backend as Backend>::Device,
    ) -> crate::CapabilityEstimate;

    /// Evaluates the current model on the requested split.
    fn evaluate(&self, model: &BurnEvalModel<Self>, split: EvalSplit) -> MetricReport;

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
    ) -> anyhow::Result<Vec<BurnTrainBatch<Self>>>;

    /// Runs before a training window starts.
    fn before_window(
        &self,
        _learner: &mut BurnWorkloadLearner<Self>,
        _lease: &AssignmentLease,
    ) -> Result<(), TrainError> {
        Ok(())
    }

    /// Records step-local metrics after one learner step.
    fn after_train_step(
        &self,
        _step_index: usize,
        _output: &BurnTrainOutput<Self>,
        _metrics: &mut BTreeMap<String, MetricValue>,
    ) -> Result<(), TrainError> {
        Ok(())
    }

    /// Finalizes the metric map before the window report is emitted.
    fn after_window(
        &self,
        _learner: &BurnWorkloadLearner<Self>,
        _metrics: &mut BTreeMap<String, MetricValue>,
    ) -> Result<(), TrainError> {
        Ok(())
    }

    /// Returns receipt metrics for a completed training window.
    fn contribution_metrics(
        &self,
        report: &WindowReport<BTreeMap<String, MetricValue>>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }

    /// Returns the contribution weight used for receipt scoring.
    fn contribution_weight(&self, report: &WindowReport<BTreeMap<String, MetricValue>>) -> f64 {
        burn_p2p_workload::standard_contribution_weight(&report.stats).unwrap_or(1.0)
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

impl<W> BurnWorkload for W
where
    W: BurnLearnerWorkload,
{
    type Backend = W::Backend;
    type Model = W::Model;
    type Batch = BurnTrainBatch<W>;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, device: &<Self::Backend as Backend>::Device) -> Self::Model {
        BurnLearnerWorkload::init_model(self, device)
    }

    fn benchmark(
        &self,
        model: &Self::Model,
        device: &<Self::Backend as Backend>::Device,
    ) -> crate::CapabilityEstimate {
        BurnLearnerWorkload::benchmark(self, model, device)
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<<Self::Backend as Backend>::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        let batch_count = ctx.batches.len() as i64;
        let mut learner = self.restore_learner(ctx.model.clone(), &ctx.device);
        self.before_window(&mut learner, &ctx.lease)?;

        let mut metrics =
            BTreeMap::from([("batch_count".into(), MetricValue::Integer(batch_count))]);
        extend_window_metrics_with_cached_microshard_counts(&mut metrics, &ctx.cached_microshards);

        for (step_index, batch) in ctx.batches.drain(..).enumerate() {
            learner.lr_step();
            let output = learner.train_step(batch);
            let lr = learner.lr_current();
            learner.optimizer_step(output.grads);

            metrics.insert(
                "train_steps".into(),
                MetricValue::Integer((step_index + 1) as i64),
            );
            metrics.insert("learning_rate".into(), MetricValue::Float(lr));

            self.after_train_step(step_index, &output.item, &mut metrics)?;
        }

        self.after_window(&learner, &mut metrics)?;
        ctx.model = learner.model();

        Ok(WindowReport {
            contribution: None,
            stats: metrics,
            completed_at: Utc::now(),
        })
    }

    fn evaluate(&self, model: &Self::Model, split: EvalSplit) -> MetricReport {
        BurnLearnerWorkload::evaluate(self, &model.valid(), split)
    }

    fn runtime_device(&self) -> <Self::Backend as Backend>::Device {
        BurnLearnerWorkload::runtime_device(self)
    }

    fn dataset_registration(&self) -> anyhow::Result<crate::DatasetRegistration> {
        BurnLearnerWorkload::dataset_registration(self)
    }

    fn microshard_plan(
        &self,
        registration: &crate::DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        BurnLearnerWorkload::microshard_plan(self, registration)
    }

    fn load_batches(
        &self,
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        BurnLearnerWorkload::load_batches(self, lease, cached_microshards)
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        BurnLearnerWorkload::contribution_metrics(self, report)
    }

    fn contribution_weight(&self, report: &WindowReport<Self::WindowStats>) -> f64 {
        BurnLearnerWorkload::contribution_weight(self, report)
    }

    fn apply_patch(&mut self, patch: &RuntimePatch) -> PatchOutcome {
        BurnLearnerWorkload::apply_patch(self, patch)
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        BurnLearnerWorkload::supported_patch_classes(self)
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
    fn contribution_weight(&self, report: &WindowReport<Self::WindowStats>) -> f64 {
        let metrics = self.contribution_metrics(report);
        burn_p2p_workload::standard_contribution_weight(&metrics).unwrap_or(1.0)
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

/// Builds a node builder from a Burn-native workload and target preset.
pub fn connect<W>(
    target: BurnTarget,
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
    connect(BurnTarget::Trainer, release_manifest, workload, config)
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
    connect(BurnTarget::Validator, release_manifest, workload, config)
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

    fn reconcile_canonical_model(
        &self,
        local_model: &Self::Model,
        canonical_model: Self::Model,
        strategy: TrainerCanonicalReconcileStrategy,
    ) -> anyhow::Result<Self::Model> {
        Ok(match strategy {
            TrainerCanonicalReconcileStrategy::Replace => canonical_model,
            TrainerCanonicalReconcileStrategy::RootEma { canonical_weight } => {
                apply_root_ema_modules::<W::Backend, _>(
                    local_model,
                    &canonical_model,
                    canonical_weight,
                )?
            }
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
mod tests;
