use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc};

use burn::{
    optim::GradientsAccumulator,
    record::{BinBytesRecorder, FullPrecisionSettings, Recorder},
};
use chrono::Utc;

use super::*;

const PERSISTENT_INNER_STATE_ENCODING: &str = "burn-learner-inner-state:bin-full-v1";
const PERSISTENT_INNER_STATE_MAGIC: &[u8; 8] = b"BLISv001";

#[derive(Clone, Debug)]
struct BurnLocalDatasetConfig {
    dataset_name: String,
    sizing: crate::DatasetSizing,
}

impl Default for BurnLocalDatasetConfig {
    fn default() -> Self {
        Self {
            dataset_name: "burn-local-dataset".into(),
            sizing: crate::DatasetSizing {
                total_examples: 1,
                total_tokens: 0,
                total_bytes: 1,
            },
        }
    }
}

struct PersistentLearnerComponents<LC>
where
    LC: LearningComponentsTypes + 'static,
{
    optimizer: <LC as LearningComponentsTypes>::Optimizer,
    scheduler: <LC as LearningComponentsTypes>::LrScheduler,
    gradient_accumulation_steps: usize,
}

impl<LC> Clone for PersistentLearnerComponents<LC>
where
    LC: LearningComponentsTypes + 'static,
{
    fn clone(&self) -> Self {
        Self {
            optimizer: self.optimizer.clone(),
            scheduler: self.scheduler.clone(),
            gradient_accumulation_steps: self.gradient_accumulation_steps,
        }
    }
}

/// Learner-first workload built directly from a burn [`BurnLearner`].
pub struct BurnLearnerProject<LC>
where
    LC: LearningComponentsTypes + 'static,
{
    learner: BurnLearner<LC>,
    device: BurnLearnerDevice<LC>,
    benchmark: Arc<LearnerBenchmarkFn<LC>>,
    evaluate: Arc<LearnerEvaluateFn<LC>>,
    data_pipeline: BurnLearnerDataPipeline<LC>,
    after_train_step: Arc<LearnerStepMetricFn<LC>>,
    after_window: Arc<LearnerWindowMetricFn<LC>>,
    persistent_inner_loop: Option<PersistentLearnerComponents<LC>>,
    materialize_workload_update: Option<Arc<LearnerWorkloadUpdateMaterializationFn<LC>>>,
    apply_workload_update: Option<Arc<LearnerWorkloadUpdateFn<LC>>>,
    validate_workload_update: Option<Arc<LearnerWorkloadUpdateValidationFn<LC>>>,
    materialize_genesis: Option<Arc<LearnerGenesisMaterializationFn<LC>>>,
    load_genesis: Option<Arc<LearnerGenesisLoadFn<LC>>>,
    materialize_model_artifact: Option<Arc<LearnerModelArtifactMaterializationFn<LC>>>,
    load_model_artifact: Option<Arc<LearnerModelArtifactLoadFn<LC>>>,
}

impl<LC> Clone for BurnLearnerProject<LC>
where
    LC: LearningComponentsTypes + 'static,
{
    fn clone(&self) -> Self {
        Self {
            learner: self.learner.clone(),
            device: self.device.clone(),
            benchmark: Arc::clone(&self.benchmark),
            evaluate: Arc::clone(&self.evaluate),
            data_pipeline: self.data_pipeline.clone(),
            after_train_step: Arc::clone(&self.after_train_step),
            after_window: Arc::clone(&self.after_window),
            persistent_inner_loop: self.persistent_inner_loop.clone(),
            materialize_workload_update: self.materialize_workload_update.clone(),
            apply_workload_update: self.apply_workload_update.clone(),
            validate_workload_update: self.validate_workload_update.clone(),
            materialize_genesis: self.materialize_genesis.clone(),
            load_genesis: self.load_genesis.clone(),
            materialize_model_artifact: self.materialize_model_artifact.clone(),
            load_model_artifact: self.load_model_artifact.clone(),
        }
    }
}

impl<LC> BurnLearnerProject<LC>
where
    LC: LearningComponentsTypes + 'static,
{
    /// Returns the static lease/micro-epoch data pipeline descriptor.
    pub fn data_pipeline_descriptor(&self) -> &crate::LeaseDataPipelineDescriptor {
        self.data_pipeline.descriptor()
    }

    /// Returns the configured lease/micro-epoch pipeline kind.
    pub fn data_pipeline_kind(&self) -> crate::LeaseDataPipelineKind {
        self.data_pipeline.kind()
    }

    /// Returns the dataset registration backing the current pipeline.
    pub fn data_pipeline_registration(&self) -> anyhow::Result<crate::DatasetRegistration> {
        self.data_pipeline.dataset_registration()
    }

    /// Returns the local upstream root when the current pipeline is backed by
    /// a `Local` dataset registration.
    pub fn local_upstream_root(&self) -> anyhow::Result<Option<PathBuf>> {
        crate::local_upstream_root_for_pipeline(&self.data_pipeline)
    }
}

/// Builder for [`BurnLearnerProject`].
pub struct BurnLearnerProjectBuilder<LC>
where
    LC: LearningComponentsTypes + 'static,
{
    learner: BurnLearner<LC>,
    device: BurnLearnerDevice<LC>,
    benchmark: Arc<LearnerBenchmarkFn<LC>>,
    evaluate: Option<Arc<LearnerEvaluateFn<LC>>>,
    data_pipeline: Option<BurnLearnerDataPipeline<LC>>,
    train_loader: Option<BurnTrainLoader<LC>>,
    validation_loader: Option<BurnValidationLoader<LC>>,
    local_dataset: BurnLocalDatasetConfig,
    after_train_step: Arc<LearnerStepMetricFn<LC>>,
    after_window: Arc<LearnerWindowMetricFn<LC>>,
    persistent_inner_loop: Option<PersistentLearnerComponents<LC>>,
    materialize_workload_update: Option<Arc<LearnerWorkloadUpdateMaterializationFn<LC>>>,
    apply_workload_update: Option<Arc<LearnerWorkloadUpdateFn<LC>>>,
    validate_workload_update: Option<Arc<LearnerWorkloadUpdateValidationFn<LC>>>,
    materialize_genesis: Option<Arc<LearnerGenesisMaterializationFn<LC>>>,
    load_genesis: Option<Arc<LearnerGenesisLoadFn<LC>>>,
    materialize_model_artifact: Option<Arc<LearnerModelArtifactMaterializationFn<LC>>>,
    load_model_artifact: Option<Arc<LearnerModelArtifactLoadFn<LC>>>,
}

/// Starts the recommended burn integration path from an existing [`BurnLearner`].
///
/// Use this when the project already has a burn `Learner::new(model, optimizer, scheduler)`.
/// Add eval + dataset + batch hooks on the returned builder, then call
/// `.trainer(...)`, `.validator(...)`, or `.connect(...)`.
///
/// `burn_p2p` will clone the learner, restore the current p2p head into the
/// learner model, run one window with `lr_step()`, `train_step(...)`, and
/// `optimizer_step(...)`, then publish the updated model artifact.
pub fn from_learner<LC>(
    learner: BurnLearner<LC>,
    device: BurnLearnerDevice<LC>,
) -> BurnLearnerProjectBuilder<LC>
where
    LC: LearningComponentsTypes + 'static,
    BurnLearnerModel<LC>: BurnModuleTarget<BurnLearnerBackend<LC>>
        + TrainStep
        + AutodiffModule<BurnLearnerBackend<LC>, InnerModule = BurnLearnerEvalModel<LC>>
        + Clone
        + core::fmt::Display
        + 'static,
    BurnLearnerEvalModel<LC>: BurnModuleTarget<<BurnLearnerBackend<LC> as AutodiffBackend>::InnerBackend>
        + InferenceStep
        + Clone
        + 'static,
{
    BurnLearnerProjectBuilder::new(learner, device)
}

/// Starts a learner-backed project with persistent DiLoCo optimizer state.
///
/// Unlike [`from_learner`], this constructor retains explicit optimizer and
/// scheduler templates. Their Burn records are serialized into the peer-local
/// DiLoCo state blob after every inner loop and restored before the next one.
pub fn from_stateful_components<B, LR, M, O>(
    model: M,
    optimizer: O,
    scheduler: LR,
    gradient_accumulation_steps: usize,
    device: B::Device,
) -> BurnLearnerProjectBuilder<LearningComponentsMarker<B, LR, M, O>>
where
    B: AutodiffBackend + 'static,
    LR: LrScheduler + 'static,
    M: BurnModuleTarget<B> + TrainStep + AutodiffModule<B> + Clone + core::fmt::Display + 'static,
    M::InnerModule: BurnModuleTarget<B::InnerBackend> + InferenceStep + Clone + 'static,
    M::Input: Clone,
    O: Optimizer<M, B> + Clone + 'static,
{
    let learner = BurnLearner::new(model, optimizer.clone(), scheduler.clone());
    BurnLearnerProjectBuilder::new(learner, device).with_persistent_inner_loop(
        optimizer,
        scheduler,
        gradient_accumulation_steps,
    )
}

/// Starts the higher-level loader-based integration path from a burn learner
/// plus train/validation dataloaders.
///
/// Prefer this when the project already has a clean `Learner + train loader +
/// validation loader` seam.
///
/// loader naming here is intentionally generic:
///
/// - train loader: batches used for local window training
/// - validation loader: batches used for local model evaluation
///
/// self-supervised workloads fit naturally if they already expose train/validation
/// batch loaders. paradigms that do not naturally use dataloaders, such as
/// some rl flows, should usually use [`BurnLearnerWorkload`] or
/// [`BurnWorkload`] instead.
pub fn from_loaders<LC>(
    learner: BurnLearner<LC>,
    device: BurnLearnerDevice<LC>,
    train_loader: BurnTrainLoader<LC>,
    validation_loader: BurnValidationLoader<LC>,
) -> BurnLearnerProjectBuilder<LC>
where
    LC: LearningComponentsTypes + 'static,
    BurnLearnerModel<LC>: BurnModuleTarget<BurnLearnerBackend<LC>>
        + TrainStep
        + AutodiffModule<BurnLearnerBackend<LC>, InnerModule = BurnLearnerEvalModel<LC>>
        + Clone
        + core::fmt::Display
        + 'static,
    BurnLearnerEvalModel<LC>: BurnModuleTarget<<BurnLearnerBackend<LC> as AutodiffBackend>::InnerBackend>
        + InferenceStep
        + Clone
        + 'static,
{
    let mut builder = from_learner(learner, device);
    builder.train_loader = Some(train_loader);
    builder.validation_loader = Some(validation_loader);
    builder
}

/// Starts the loader-based integration path with persistent DiLoCo state.
#[allow(clippy::too_many_arguments)]
pub fn from_stateful_loaders<B, LR, M, O>(
    model: M,
    optimizer: O,
    scheduler: LR,
    gradient_accumulation_steps: usize,
    device: B::Device,
    train_loader: BurnTrainLoader<LearningComponentsMarker<B, LR, M, O>>,
    validation_loader: BurnValidationLoader<LearningComponentsMarker<B, LR, M, O>>,
) -> BurnLearnerProjectBuilder<LearningComponentsMarker<B, LR, M, O>>
where
    B: AutodiffBackend + 'static,
    LR: LrScheduler + 'static,
    M: BurnModuleTarget<B> + TrainStep + AutodiffModule<B> + Clone + core::fmt::Display + 'static,
    M::InnerModule: BurnModuleTarget<B::InnerBackend> + InferenceStep + Clone + 'static,
    M::Input: Clone,
    O: Optimizer<M, B> + Clone + 'static,
{
    let mut builder = from_stateful_components(
        model,
        optimizer,
        scheduler,
        gradient_accumulation_steps,
        device,
    );
    builder.train_loader = Some(train_loader);
    builder.validation_loader = Some(validation_loader);
    builder
}

impl<LC> BurnLearnerProjectBuilder<LC>
where
    LC: LearningComponentsTypes + 'static,
    BurnLearnerModel<LC>: BurnModuleTarget<BurnLearnerBackend<LC>>
        + TrainStep
        + AutodiffModule<BurnLearnerBackend<LC>, InnerModule = BurnLearnerEvalModel<LC>>
        + Clone
        + core::fmt::Display
        + 'static,
    BurnLearnerEvalModel<LC>: BurnModuleTarget<<BurnLearnerBackend<LC> as AutodiffBackend>::InnerBackend>
        + InferenceStep
        + Clone
        + 'static,
{
    /// Creates a new builder from an existing learner.
    pub fn new(learner: BurnLearner<LC>, device: BurnLearnerDevice<LC>) -> Self {
        Self {
            learner,
            device,
            benchmark: Arc::new(default_learner_benchmark::<LC>),
            evaluate: None,
            data_pipeline: None,
            train_loader: None,
            validation_loader: None,
            local_dataset: BurnLocalDatasetConfig::default(),
            after_train_step: Arc::new(default_learner_step_metrics::<LC>),
            after_window: Arc::new(default_learner_window_metrics::<LC>),
            persistent_inner_loop: None,
            materialize_workload_update: None,
            apply_workload_update: None,
            validate_workload_update: None,
            materialize_genesis: None,
            load_genesis: None,
            materialize_model_artifact: None,
            load_model_artifact: None,
        }
    }

    fn with_persistent_inner_loop(
        mut self,
        optimizer: <LC as LearningComponentsTypes>::Optimizer,
        scheduler: <LC as LearningComponentsTypes>::LrScheduler,
        gradient_accumulation_steps: usize,
    ) -> Self {
        self.persistent_inner_loop = Some(PersistentLearnerComponents {
            optimizer,
            scheduler,
            gradient_accumulation_steps,
        });
        self
    }

    /// Overrides capability estimation.
    pub fn with_benchmark(
        mut self,
        benchmark: impl Fn(&BurnLearnerModel<LC>, &BurnLearnerDevice<LC>) -> crate::CapabilityEstimate
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.benchmark = Arc::new(benchmark);
        self
    }

    /// Sets evaluation over the learner's inference model.
    pub fn with_evaluate(
        mut self,
        evaluate: impl Fn(&BurnLearnerEvalModel<LC>, EvalSplit) -> MetricReport + Send + Sync + 'static,
    ) -> Self {
        self.evaluate = Some(Arc::new(evaluate));
        self
    }

    /// Sets the validation dataloader used by the default evaluation path.
    ///
    /// If no custom `.with_evaluate(...)` hook is provided, `burn_p2p` will run
    /// the inference model over the loader and emit generic evaluation counts.
    pub fn with_validation_loader(mut self, validation_loader: BurnValidationLoader<LC>) -> Self {
        self.validation_loader = Some(validation_loader);
        self
    }

    /// Sets a shard-backed training dataset that the runtime can fetch lease by
    /// lease.
    ///
    /// Use this when native and wasm/browser trainers should share the same
    /// p2p-compatible shard layout. The runtime will still fetch only the
    /// assigned microshards for each lease.
    pub fn with_sharded_dataset<Record, Ba>(
        mut self,
        dataset: BurnShardedDataset<Record>,
        batcher: Ba,
        batch_size: usize,
    ) -> Self
    where
        Record: serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
        Ba: burn::data::dataloader::batcher::Batcher<
                BurnLearnerBackend<LC>,
                Record,
                BurnLearnerBatch<LC>,
            > + Clone
            + Send
            + Sync
            + 'static,
    {
        let registration = dataset.registration().clone();
        let microshard_plan = dataset.microshard_plan().clone();
        let load_dataset = dataset.clone();
        self.data_pipeline = Some(crate::LeaseDataPipeline::new(
            crate::LeaseDataPipelineDescriptor::new(
                "burn-sharded-dataset",
                crate::LeaseDataPipelineKind::ShardedStatic,
            )
            .with_metadata_entry("format", "burn-sharded-dataset"),
            move || Ok(registration.clone()),
            move |_registration| Ok(microshard_plan.clone()),
            move |_lease, cached_microshards, device| {
                load_dataset.load_batches(cached_microshards, batcher.clone(), batch_size, device)
            },
        ));
        self
    }

    /// Sets a complete lease/micro-epoch data pipeline in one value.
    pub fn with_data_pipeline(mut self, data_pipeline: BurnLearnerDataPipeline<LC>) -> Self {
        self.data_pipeline = Some(data_pipeline);
        self
    }

    /// Overrides per-step metric extraction.
    pub fn with_step_metrics(
        mut self,
        after_train_step: impl Fn(
            usize,
            &BurnLearnerOutput<LC>,
            &mut BTreeMap<String, MetricValue>,
        ) -> Result<(), TrainError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.after_train_step = Arc::new(after_train_step);
        self
    }

    /// Overrides end-of-window metric extraction.
    pub fn with_window_metrics(
        mut self,
        after_window: impl Fn(
            &BurnLearner<LC>,
            &mut BTreeMap<String, MetricValue>,
        ) -> Result<(), TrainError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.after_window = Arc::new(after_window);
        self
    }

    /// Installs a workload-specific decoder for contract-bound compact updates.
    pub fn with_workload_update_applier(
        mut self,
        apply: impl Fn(
            BurnLearnerModel<LC>,
            &ArtifactDescriptor,
            &WorkloadUpdateEnvelope,
            &TrainingContractManifest,
            &FsArtifactStore,
            &BurnLearnerDevice<LC>,
        ) -> anyhow::Result<BurnLearnerModel<LC>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.apply_workload_update = Some(Arc::new(apply));
        self
    }

    /// Installs a workload-specific compact-update materializer for trained windows.
    pub fn with_workload_update_materializer(
        mut self,
        materialize: impl for<'a> Fn(
            WorkloadUpdateMaterializationContext<'a, BurnLearnerDevice<LC>, BurnLearnerModel<LC>>,
        ) -> anyhow::Result<Option<MaterializedWorkloadUpdate>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.materialize_workload_update = Some(Arc::new(materialize));
        self
    }

    /// Installs workload-specific independent validation for typed updates.
    pub fn with_workload_update_validator(
        mut self,
        validate: impl Fn(
            BurnLearnerModel<LC>,
            WorkloadUpdateValidationContext<'_, BurnLearnerDevice<LC>>,
        ) -> anyhow::Result<ValidatedWorkloadUpdate<BurnLearnerModel<LC>>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.validate_workload_update = Some(Arc::new(validate));
        self
    }

    /// Installs workload-specific deterministic genesis materialization.
    pub fn with_genesis_materializer(
        mut self,
        materialize: impl for<'a> Fn(
            GenesisArtifactMaterializationContext<'a, BurnLearnerModel<LC>>,
        ) -> anyhow::Result<Option<ArtifactDescriptor>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.materialize_genesis = Some(Arc::new(materialize));
        self
    }

    /// Installs workload-specific deterministic genesis reconstruction.
    pub fn with_genesis_loader(
        mut self,
        load: impl Fn(
            BurnLearnerModel<LC>,
            GenesisArtifactLoadContext<'_, BurnLearnerDevice<LC>>,
        ) -> anyhow::Result<Option<BurnLearnerModel<LC>>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.load_genesis = Some(Arc::new(load));
        self
    }

    /// Installs workload-specific canonical model artifact materialization.
    pub fn with_model_artifact_materializer(
        mut self,
        materialize: impl Fn(
            &BurnLearnerModel<LC>,
            ArtifactKind,
            &HeadId,
            Option<&HeadId>,
            &FsArtifactStore,
            &ContentId,
        ) -> anyhow::Result<Option<ArtifactDescriptor>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.materialize_model_artifact = Some(Arc::new(materialize));
        self
    }

    /// Installs workload-specific canonical model artifact loading.
    pub fn with_model_artifact_loader(
        mut self,
        load: impl Fn(
            &BurnLearnerModel<LC>,
            &ArtifactDescriptor,
            &FsArtifactStore,
            &BurnLearnerDevice<LC>,
            &ContentId,
        ) -> anyhow::Result<Option<BurnLearnerModel<LC>>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.load_model_artifact = Some(Arc::new(load));
        self
    }

    /// Finalizes the learner-backed workload.
    pub fn build(self) -> anyhow::Result<BurnLearnerProject<LC>> {
        self.build_with_training_hooks(true)
    }

    fn build_with_training_hooks(
        self,
        require_training_hooks: bool,
    ) -> anyhow::Result<BurnLearnerProject<LC>> {
        let Self {
            learner,
            device,
            benchmark,
            evaluate,
            data_pipeline,
            train_loader,
            validation_loader,
            local_dataset,
            after_train_step,
            after_window,
            persistent_inner_loop,
            materialize_workload_update,
            apply_workload_update,
            validate_workload_update,
            materialize_genesis,
            load_genesis,
            materialize_model_artifact,
            load_model_artifact,
        } = self;
        anyhow::ensure!(
            persistent_inner_loop
                .as_ref()
                .is_none_or(|state| state.gradient_accumulation_steps > 0),
            "persistent burn inner-loop gradient_accumulation_steps must be greater than zero"
        );
        let local_data_pipeline = if data_pipeline.is_none() {
            if let Some(train_loader) = train_loader.as_ref() {
                Some(local_dataset_bundle::<LC>(
                    &local_dataset,
                    crate::LeaseDataPipelineKind::IndexedDataset,
                    loader_batch_source::<LC>(train_loader.clone()),
                )?)
            } else {
                None
            }
        } else {
            None
        };
        let passive_data_pipeline = if !require_training_hooks
            && data_pipeline.is_none()
            && local_data_pipeline.is_none()
        {
            Some(passive_dataset_bundle::<LC>(&local_dataset)?)
        } else {
            None
        };
        let resolved_data_pipeline = data_pipeline
            .or(local_data_pipeline)
            .or(passive_data_pipeline);

        Ok(BurnLearnerProject {
            learner,
            device,
            benchmark,
            evaluate: evaluate
                .or_else(|| {
                    validation_loader
                        .as_ref()
                        .map(|validation_loader| loader_evaluate_fn::<LC>(validation_loader.clone()))
                })
                .unwrap_or_else(|| Arc::new(default_learner_evaluate::<LC>)),
            data_pipeline: resolved_data_pipeline
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "missing burn learner training data; use from_loaders(...), with_sharded_dataset(...), or with_data_pipeline(...)"
                    )
                })?,
            after_train_step,
            after_window,
            persistent_inner_loop,
            materialize_workload_update,
            apply_workload_update,
            validate_workload_update,
            materialize_genesis,
            load_genesis,
            materialize_model_artifact,
            load_model_artifact,
        })
    }

    /// Finalizes the learner-backed workload and wraps it in a node builder.
    pub fn node(
        self,
        release_manifest: ClientReleaseManifest,
        supported_workload: SupportedWorkload,
    ) -> anyhow::Result<
        NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<BurnLearnerProject<LC>>>>,
    > {
        self.node_with_config(
            release_manifest,
            BurnWorkloadConfig::standard(supported_workload),
        )
    }

    /// Finalizes the learner-backed workload with an explicit workload config.
    pub fn node_with_config(
        self,
        release_manifest: ClientReleaseManifest,
        config: BurnWorkloadConfig,
    ) -> anyhow::Result<
        NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<BurnLearnerProject<LC>>>>,
    > {
        node(release_manifest, self.build()?, config)
    }

    /// Finalizes the learner-backed workload and applies the requested target preset.
    pub fn connect(
        self,
        target: BurnTarget,
        release_manifest: ClientReleaseManifest,
        supported_workload: SupportedWorkload,
    ) -> anyhow::Result<
        NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<BurnLearnerProject<LC>>>>,
    > {
        self.connect_with_config(
            target,
            release_manifest,
            BurnWorkloadConfig::standard(supported_workload),
        )
    }

    /// Finalizes the learner-backed workload and applies the requested target preset.
    pub fn connect_with_config(
        self,
        target: BurnTarget,
        release_manifest: ClientReleaseManifest,
        config: BurnWorkloadConfig,
    ) -> anyhow::Result<
        NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<BurnLearnerProject<LC>>>>,
    > {
        let require_training_hooks = target.requires_training_hooks();
        connect(
            target,
            release_manifest,
            self.build_with_training_hooks(require_training_hooks)?,
            config,
        )
    }

    /// Finalizes the learner-backed workload as a trainer node builder.
    pub fn trainer(
        self,
        release_manifest: ClientReleaseManifest,
        supported_workload: SupportedWorkload,
    ) -> anyhow::Result<
        NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<BurnLearnerProject<LC>>>>,
    > {
        self.connect(BurnTarget::Trainer, release_manifest, supported_workload)
    }

    /// Finalizes the learner-backed workload as a trainer node builder.
    pub fn trainer_with_config(
        self,
        release_manifest: ClientReleaseManifest,
        config: BurnWorkloadConfig,
    ) -> anyhow::Result<
        NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<BurnLearnerProject<LC>>>>,
    > {
        self.connect_with_config(BurnTarget::Trainer, release_manifest, config)
    }

    /// Finalizes the learner-backed workload as an authority / validator / archive node builder.
    pub fn validator(
        self,
        release_manifest: ClientReleaseManifest,
        supported_workload: SupportedWorkload,
    ) -> anyhow::Result<
        NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<BurnLearnerProject<LC>>>>,
    > {
        self.connect(BurnTarget::Validator, release_manifest, supported_workload)
    }

    /// Finalizes the learner-backed workload as an authority / validator / archive node builder.
    pub fn validator_with_config(
        self,
        release_manifest: ClientReleaseManifest,
        config: BurnWorkloadConfig,
    ) -> anyhow::Result<
        NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<BurnLearnerProject<LC>>>>,
    > {
        self.connect_with_config(BurnTarget::Validator, release_manifest, config)
    }
}

fn default_learner_benchmark<LC>(
    model: &BurnLearnerModel<LC>,
    _device: &BurnLearnerDevice<LC>,
) -> crate::CapabilityEstimate
where
    LC: LearningComponentsTypes + 'static,
    BurnLearnerModel<LC>: BurnModuleTarget<BurnLearnerBackend<LC>>,
{
    let inventory = inspect_module::<BurnLearnerBackend<LC>, _>(model);
    crate::CapabilityEstimate {
        preferred_backends: vec!["burn".into()],
        work_units_per_second: inventory.total_scalar_parameters.max(1) as f64,
        target_window_seconds: 1,
    }
}

fn default_learner_evaluate<LC>(model: &BurnLearnerEvalModel<LC>, _split: EvalSplit) -> MetricReport
where
    LC: LearningComponentsTypes + 'static,
    BurnLearnerEvalModel<LC>:
        BurnModuleTarget<<BurnLearnerBackend<LC> as AutodiffBackend>::InnerBackend>,
{
    let inventory =
        inspect_module::<<BurnLearnerBackend<LC> as AutodiffBackend>::InnerBackend, _>(model);
    MetricReport {
        metrics: BTreeMap::from([
            (
                "parameter_count".into(),
                MetricValue::Integer(inventory.total_scalar_parameters as i64),
            ),
            (
                "parameter_tensor_count".into(),
                MetricValue::Integer(inventory.parameter_count as i64),
            ),
        ]),
        captured_at: Utc::now(),
    }
}

fn default_loader_evaluate<LC>(
    model: &BurnLearnerEvalModel<LC>,
    split: EvalSplit,
    validation_loader: BurnValidationLoader<LC>,
) -> MetricReport
where
    LC: LearningComponentsTypes + 'static,
    BurnLearnerEvalModel<LC>: BurnModuleTarget<<BurnLearnerBackend<LC> as AutodiffBackend>::InnerBackend>
        + InferenceStep
        + Clone
        + 'static,
{
    let mut report = default_learner_evaluate::<LC>(model, split);
    let device = model
        .devices()
        .into_iter()
        .next()
        .expect("burn evaluation model should expose at least one device");
    let validation_loader = validation_loader.to_device(&device);
    let evaluation_items = validation_loader.num_items() as i64;
    let mut evaluation_batches = 0_i64;
    let iterator = validation_loader.iter();
    for item in iterator {
        let _ = model.step(item);
        evaluation_batches += 1;
    }
    report.metrics.insert(
        "evaluation_items".into(),
        MetricValue::Integer(evaluation_items),
    );
    report.metrics.insert(
        "evaluation_batches".into(),
        MetricValue::Integer(evaluation_batches),
    );
    report
}

fn loader_evaluate_fn<LC>(validation_loader: BurnValidationLoader<LC>) -> Arc<LearnerEvaluateFn<LC>>
where
    LC: LearningComponentsTypes + 'static,
    BurnLearnerEvalModel<LC>: BurnModuleTarget<<BurnLearnerBackend<LC> as AutodiffBackend>::InnerBackend>
        + InferenceStep
        + Clone
        + 'static,
{
    Arc::new(
        move |model: &BurnLearnerEvalModel<LC>, split: EvalSplit| -> MetricReport {
            default_loader_evaluate::<LC>(model, split, validation_loader.clone())
        },
    )
}

fn default_learner_step_metrics<LC>(
    _step_index: usize,
    _output: &BurnLearnerOutput<LC>,
    _metrics: &mut BTreeMap<String, MetricValue>,
) -> Result<(), TrainError>
where
    LC: LearningComponentsTypes + 'static,
{
    Ok(())
}

fn default_learner_window_metrics<LC>(
    learner: &BurnLearner<LC>,
    metrics: &mut BTreeMap<String, MetricValue>,
) -> Result<(), TrainError>
where
    LC: LearningComponentsTypes + 'static,
    BurnLearnerModel<LC>: BurnModuleTarget<BurnLearnerBackend<LC>>,
{
    let inventory = inspect_module::<BurnLearnerBackend<LC>, _>(&learner.model());
    metrics.insert(
        "parameter_count".into(),
        MetricValue::Integer(inventory.total_scalar_parameters as i64),
    );
    metrics.insert(
        "parameter_tensor_count".into(),
        MetricValue::Integer(inventory.parameter_count as i64),
    );
    Ok(())
}

fn local_dataset_bundle<LC>(
    config: &BurnLocalDatasetConfig,
    pipeline_kind: crate::LeaseDataPipelineKind,
    load_batches: Arc<LearnerBatchLoaderFn<LC>>,
) -> anyhow::Result<BurnLearnerDataPipeline<LC>>
where
    LC: LearningComponentsTypes + 'static,
{
    let dataset_id = crate::DatasetId::derive(&("burn-p2p-local", &config.dataset_name))?;
    let dataset_view_id =
        crate::DatasetViewId::derive(&(dataset_id.as_str(), &config.dataset_name, "view"))?;
    let manifest_hash = ContentId::derive(&(
        "burn-p2p-local-manifest",
        dataset_id.as_str(),
        &config.sizing,
    ))?;
    let preprocessing_hash =
        ContentId::derive(&("burn-p2p-local-preprocess", dataset_view_id.as_str()))?;
    let root_hash =
        ContentId::derive(&("burn-p2p-local-root", &config.dataset_name, &config.sizing))?;
    let root = std::env::temp_dir()
        .join("burn_p2p")
        .join("local-dataset")
        .join(root_hash.as_str());
    fs::create_dir_all(&root)?;

    let registration = crate::DatasetRegistration {
        manifest: crate::DatasetManifest {
            dataset_id: dataset_id.clone(),
            source_uri: format!("runtime-local://{}", config.dataset_name),
            format: "runtime-local".into(),
            manifest_hash: manifest_hash.clone(),
            metadata: BTreeMap::from([("dataset_name".into(), config.dataset_name.clone())]),
        },
        view: crate::DatasetView {
            dataset_view_id: dataset_view_id.clone(),
            dataset_id,
            preprocessing_hash,
            tokenizer_hash: None,
            manifest_hash,
            metadata: BTreeMap::from([("dataset_kind".into(), "runtime-local".into())]),
        },
        upstream: crate::UpstreamAdapter::Local {
            root: root.display().to_string(),
        },
    };
    let plan = crate::MicroShardPlanner::new(crate::MicroShardPlannerConfig {
        target_microshard_bytes: config.sizing.total_bytes.max(1),
        min_microshards: 1,
        max_microshards: 1,
    })?
    .plan(&registration.view, config.sizing.clone())?;
    let fetch_manifest =
        crate::ShardFetchManifest::from_microshards(&plan.dataset_view, &plan.microshards, |_| {
            vec![0]
        });
    fs::write(
        root.join("fetch-manifest.json"),
        serde_json::to_vec_pretty(&fetch_manifest)?,
    )?;
    for entry in &fetch_manifest.entries {
        let bytes = vec![0_u8; entry.bytes_len.max(1) as usize];
        fs::write(root.join(PathBuf::from(&entry.locator)), bytes)?;
    }

    Ok(crate::LeaseDataPipeline::new(
        crate::LeaseDataPipelineDescriptor::new(config.dataset_name.clone(), pipeline_kind)
            .with_metadata_entry("source_uri", registration.manifest.source_uri.clone())
            .with_metadata_entry("format", registration.manifest.format.clone()),
        move || Ok(registration.clone()),
        move |_registration| Ok(plan.clone()),
        move |lease, cached_microshards, device| load_batches(lease, cached_microshards, device),
    ))
}

fn passive_dataset_bundle<LC>(
    config: &BurnLocalDatasetConfig,
) -> anyhow::Result<BurnLearnerDataPipeline<LC>>
where
    LC: LearningComponentsTypes + 'static,
{
    local_dataset_bundle::<LC>(
        config,
        crate::LeaseDataPipelineKind::Custom,
        Arc::new(
            |_lease: &AssignmentLease,
             _cached_microshards: &[CachedMicroShard],
             _device: &BurnLearnerDevice<LC>| {
                anyhow::bail!("training batches are unavailable for this non-training node target")
            },
        ),
    )
}

fn loader_batch_source<LC>(train_loader: BurnTrainLoader<LC>) -> Arc<LearnerBatchLoaderFn<LC>>
where
    LC: LearningComponentsTypes + 'static,
{
    Arc::new(
        move |_lease: &AssignmentLease,
              _cached_microshards: &[CachedMicroShard],
              device: &BurnLearnerDevice<LC>| {
            let train_loader = train_loader.to_device(device);
            let mut batches = Vec::new();
            for batch in train_loader.iter() {
                batches.push(batch);
            }
            Ok(batches)
        },
    )
}

fn encode_persistent_inner_state<LC>(
    optimizer: &<LC as LearningComponentsTypes>::Optimizer,
    scheduler: &<LC as LearningComponentsTypes>::LrScheduler,
    microsteps_completed: u64,
) -> Result<StateBlob, TrainError>
where
    LC: LearningComponentsTypes + 'static,
{
    let recorder = BinBytesRecorder::<FullPrecisionSettings>::default();
    let optimizer_bytes = recorder
        .record(optimizer.to_record(), ())
        .map_err(|error| TrainError::new(format!("failed to encode optimizer state: {error}")))?;
    let scheduler_bytes = recorder
        .record(scheduler.to_record::<BurnLearnerBackend<LC>>(), ())
        .map_err(|error| TrainError::new(format!("failed to encode scheduler state: {error}")))?;
    let optimizer_len = u64::try_from(optimizer_bytes.len())
        .map_err(|_| TrainError::new("optimizer state length exceeds u64"))?;
    let scheduler_len = u64::try_from(scheduler_bytes.len())
        .map_err(|_| TrainError::new("scheduler state length exceeds u64"))?;
    let mut bytes = Vec::with_capacity(
        PERSISTENT_INNER_STATE_MAGIC.len()
            + size_of::<u64>() * 3
            + optimizer_bytes.len()
            + scheduler_bytes.len(),
    );
    bytes.extend_from_slice(PERSISTENT_INNER_STATE_MAGIC);
    bytes.extend_from_slice(&microsteps_completed.to_le_bytes());
    bytes.extend_from_slice(&optimizer_len.to_le_bytes());
    bytes.extend_from_slice(&scheduler_len.to_le_bytes());
    bytes.extend_from_slice(&optimizer_bytes);
    bytes.extend_from_slice(&scheduler_bytes);
    StateBlob::try_new(PERSISTENT_INNER_STATE_ENCODING, bytes)
        .map_err(|error| TrainError::new(format!("failed to bind optimizer state: {error}")))
}

fn take_u64(bytes: &mut &[u8], label: &str) -> Result<u64, TrainError> {
    let encoded = bytes
        .get(..size_of::<u64>())
        .ok_or_else(|| TrainError::new(format!("persistent inner state is missing {label}")))?;
    *bytes = &bytes[size_of::<u64>()..];
    Ok(u64::from_le_bytes(
        encoded
            .try_into()
            .expect("a checked u64 state field has eight bytes"),
    ))
}

fn take_state_bytes<'a>(
    bytes: &mut &'a [u8],
    len: u64,
    label: &str,
) -> Result<&'a [u8], TrainError> {
    let len = usize::try_from(len)
        .map_err(|_| TrainError::new(format!("{label} state length exceeds usize")))?;
    let selected = bytes
        .get(..len)
        .ok_or_else(|| TrainError::new(format!("persistent inner state truncates {label}")))?;
    *bytes = &bytes[len..];
    Ok(selected)
}

fn decode_persistent_inner_state<LC>(
    templates: &PersistentLearnerComponents<LC>,
    state: &StateBlob,
    device: &BurnLearnerDevice<LC>,
) -> Result<
    (
        <LC as LearningComponentsTypes>::Optimizer,
        <LC as LearningComponentsTypes>::LrScheduler,
        u64,
    ),
    TrainError,
>
where
    LC: LearningComponentsTypes + 'static,
{
    if state.encoding != PERSISTENT_INNER_STATE_ENCODING {
        return Err(TrainError::new(format!(
            "unsupported persistent inner state encoding {}",
            state.encoding
        )));
    }
    let mut bytes = state.bytes.as_slice();
    let magic = bytes
        .get(..PERSISTENT_INNER_STATE_MAGIC.len())
        .ok_or_else(|| TrainError::new("persistent inner state is missing its header"))?;
    if magic != PERSISTENT_INNER_STATE_MAGIC {
        return Err(TrainError::new(
            "persistent inner state magic does not match",
        ));
    }
    bytes = &bytes[PERSISTENT_INNER_STATE_MAGIC.len()..];
    let microsteps_completed = take_u64(&mut bytes, "microstep count")?;
    let optimizer_len = take_u64(&mut bytes, "optimizer length")?;
    let scheduler_len = take_u64(&mut bytes, "scheduler length")?;
    let optimizer_bytes = take_state_bytes(&mut bytes, optimizer_len, "optimizer")?;
    let scheduler_bytes = take_state_bytes(&mut bytes, scheduler_len, "scheduler")?;
    if !bytes.is_empty() {
        return Err(TrainError::new(
            "persistent inner state contains trailing bytes",
        ));
    }

    let recorder = BinBytesRecorder::<FullPrecisionSettings>::default();
    let optimizer_record = recorder
        .load(optimizer_bytes.to_vec(), device)
        .map_err(|error| TrainError::new(format!("failed to decode optimizer state: {error}")))?;
    let scheduler_record = recorder
        .load(scheduler_bytes.to_vec(), device)
        .map_err(|error| TrainError::new(format!("failed to decode scheduler state: {error}")))?;
    Ok((
        templates.optimizer.clone().load_record(optimizer_record),
        templates
            .scheduler
            .clone()
            .load_record::<BurnLearnerBackend<LC>>(scheduler_record),
        microsteps_completed,
    ))
}

impl<LC> BurnWorkload for BurnLearnerProject<LC>
where
    LC: LearningComponentsTypes + 'static,
    BurnLearnerModel<LC>: BurnModuleTarget<BurnLearnerBackend<LC>>
        + TrainStep
        + AutodiffModule<BurnLearnerBackend<LC>, InnerModule = BurnLearnerEvalModel<LC>>
        + Clone
        + core::fmt::Display
        + 'static,
    BurnLearnerEvalModel<LC>: BurnModuleTarget<<BurnLearnerBackend<LC> as AutodiffBackend>::InnerBackend>
        + InferenceStep
        + Clone
        + 'static,
{
    type Backend = BurnLearnerBackend<LC>;
    type Model = BurnLearnerModel<LC>;
    type Batch = BurnLearnerBatch<LC>;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, device: &BurnLearnerDevice<LC>) -> Self::Model {
        let mut learner = self.learner.clone();
        learner.fork(device);
        learner.model()
    }

    fn benchmark(
        &self,
        model: &Self::Model,
        device: &BurnLearnerDevice<LC>,
    ) -> crate::CapabilityEstimate {
        (self.benchmark)(model, device)
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<BurnLearnerDevice<LC>, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        let batch_count = ctx.batches.len() as i64;
        let mut learner = self.learner.clone();
        learner.fork(&ctx.device);
        learner.load_model(ctx.model.clone().into_record());

        let mut metrics =
            BTreeMap::from([("batch_count".into(), MetricValue::Integer(batch_count))]);
        super::extend_window_metrics_with_cached_microshard_counts(
            &mut metrics,
            &ctx.cached_microshards,
        );

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
            (self.after_train_step)(step_index, &output.item, &mut metrics)?;
        }

        (self.after_window)(&learner, &mut metrics)?;
        ctx.model = learner.model();

        Ok(WindowReport {
            contribution: None,
            stats: metrics,
            completed_at: Utc::now(),
        })
    }

    fn evaluate(&self, model: &Self::Model, split: EvalSplit) -> MetricReport {
        (self.evaluate)(&model.valid(), split)
    }

    fn runtime_device(&self) -> BurnLearnerDevice<LC> {
        self.device.clone()
    }

    fn dataset_registration(&self) -> anyhow::Result<crate::DatasetRegistration> {
        self.data_pipeline.dataset_registration()
    }

    fn microshard_plan(
        &self,
        registration: &crate::DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        self.data_pipeline.microshard_plan(registration)
    }

    fn load_batches(
        &self,
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        self.data_pipeline
            .load_batches(lease, cached_microshards, &self.device)
    }

    fn run_persistent_inner_steps(
        &self,
        model: &Self::Model,
        batches: &[Self::Batch],
        num_inner_steps: u32,
        inner_optimizer_state: Option<&StateBlob>,
    ) -> Option<Result<BurnPersistentInnerLoopResult<Self::Model>, TrainError>>
    where
        Self::Batch: Clone,
    {
        let templates = self.persistent_inner_loop.as_ref()?;
        Some((|| {
            if num_inner_steps > 0 && batches.is_empty() {
                return Err(TrainError::new(
                    "persistent Burn inner loop requires at least one batch",
                ));
            }
            let state_restored = inner_optimizer_state.is_some();
            let (mut optimizer, mut scheduler, microstep_offset) = match inner_optimizer_state {
                Some(state) => decode_persistent_inner_state::<LC>(templates, state, &self.device)?,
                None => (templates.optimizer.clone(), templates.scheduler.clone(), 0),
            };
            let selected_batches = batches
                .iter()
                .cloned()
                .cycle()
                .take(num_inner_steps as usize)
                .collect::<Vec<_>>();
            let mut model = model.clone();
            let mut accumulator = GradientsAccumulator::new();
            let mut accumulated = 0_usize;
            let mut optimizer_steps = 0_usize;
            let mut last_lr = 0.0;
            let mut metrics = BTreeMap::from([
                (
                    "batch_count".into(),
                    MetricValue::Integer(selected_batches.len() as i64),
                ),
                (
                    "gradient_accumulation_steps".into(),
                    MetricValue::Integer(templates.gradient_accumulation_steps as i64),
                ),
                (
                    "diloco_inner_state_restored".into(),
                    MetricValue::Bool(state_restored),
                ),
                (
                    "diloco_inner_microstep_offset_start".into(),
                    MetricValue::Integer(microstep_offset.min(i64::MAX as u64) as i64),
                ),
            ]);

            for (step_index, batch) in selected_batches.into_iter().enumerate() {
                last_lr = scheduler.step();
                let output = model.step(batch);
                accumulator.accumulate(&model, output.grads);
                accumulated += 1;
                if accumulated == templates.gradient_accumulation_steps {
                    model = optimizer.step(last_lr, model, accumulator.grads());
                    accumulated = 0;
                    optimizer_steps += 1;
                }

                metrics.insert(
                    "train_steps".into(),
                    MetricValue::Integer((step_index + 1) as i64),
                );
                metrics.insert("learning_rate".into(), MetricValue::Float(last_lr));
                (self.after_train_step)(step_index, &output.item, &mut metrics)?;
            }

            let flushed_partial_accumulation = accumulated > 0;
            if flushed_partial_accumulation {
                model = optimizer.step(last_lr, model, accumulator.grads());
                optimizer_steps += 1;
            }
            let microsteps_completed = microstep_offset.saturating_add(u64::from(num_inner_steps));
            metrics.insert(
                "optimizer_steps".into(),
                MetricValue::Integer(optimizer_steps as i64),
            );
            metrics.insert(
                "flushed_partial_gradient_accumulation".into(),
                MetricValue::Bool(flushed_partial_accumulation),
            );
            metrics.insert(
                "diloco_inner_microstep_offset_end".into(),
                MetricValue::Integer(microsteps_completed.min(i64::MAX as u64) as i64),
            );

            let mut reporting_learner = self.learner.clone();
            reporting_learner.fork(&self.device);
            reporting_learner.load_model(model.clone().into_record());
            (self.after_window)(&reporting_learner, &mut metrics)?;

            Ok(BurnPersistentInnerLoopResult {
                model,
                inner_optimizer_state: encode_persistent_inner_state::<LC>(
                    &optimizer,
                    &scheduler,
                    microsteps_completed,
                )?,
                steps_completed: num_inner_steps,
                metrics,
            })
        })())
    }

    fn materialize_deterministic_genesis(
        &self,
        context: GenesisArtifactMaterializationContext<'_, Self::Model>,
    ) -> anyhow::Result<Option<ArtifactDescriptor>> {
        match self.materialize_genesis.as_ref() {
            Some(materialize) => materialize(context),
            None => Ok(None),
        }
    }

    fn load_deterministic_genesis(
        &self,
        model: Self::Model,
        context: GenesisArtifactLoadContext<'_, BurnLearnerDevice<LC>>,
    ) -> anyhow::Result<Option<Self::Model>> {
        match self.load_genesis.as_ref() {
            Some(load) => load(model, context),
            None => Ok(None),
        }
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: ArtifactKind,
        head_id: &HeadId,
        base_head_id: Option<&HeadId>,
        store: &FsArtifactStore,
        model_schema_hash: &ContentId,
    ) -> anyhow::Result<Option<ArtifactDescriptor>> {
        match self.materialize_model_artifact.as_ref() {
            Some(materialize) => materialize(
                model,
                artifact_kind,
                head_id,
                base_head_id,
                store,
                model_schema_hash,
            ),
            None => Ok(None),
        }
    }

    fn load_model_artifact(
        &self,
        model: &Self::Model,
        descriptor: &ArtifactDescriptor,
        store: &FsArtifactStore,
        device: &BurnLearnerDevice<LC>,
        model_schema_hash: &ContentId,
    ) -> anyhow::Result<Option<Self::Model>> {
        match self.load_model_artifact.as_ref() {
            Some(load) => load(model, descriptor, store, device, model_schema_hash),
            None => Ok(None),
        }
    }

    fn apply_workload_update(
        &self,
        base_model: Self::Model,
        descriptor: &ArtifactDescriptor,
        update: &WorkloadUpdateEnvelope,
        contract: &TrainingContractManifest,
        store: &FsArtifactStore,
        device: &BurnLearnerDevice<LC>,
    ) -> anyhow::Result<Self::Model> {
        self.apply_workload_update.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "burn learner project does not support typed update artifact {}",
                descriptor.artifact_id.as_str()
            )
        })?(base_model, descriptor, update, contract, store, device)
    }

    fn materialize_workload_update(
        &self,
        context: WorkloadUpdateMaterializationContext<'_, BurnLearnerDevice<LC>, Self::Model>,
    ) -> anyhow::Result<Option<MaterializedWorkloadUpdate>> {
        match self.materialize_workload_update.as_ref() {
            Some(materialize) => materialize(context),
            None => Ok(None),
        }
    }

    fn validate_and_apply_workload_update(
        &self,
        base_model: Self::Model,
        context: WorkloadUpdateValidationContext<'_, BurnLearnerDevice<LC>>,
    ) -> anyhow::Result<ValidatedWorkloadUpdate<Self::Model>> {
        if let Some(validate) = self.validate_workload_update.as_ref() {
            return validate(base_model, context);
        }
        let WorkloadUpdateValidationContext {
            descriptor,
            update,
            contract,
            store,
            device,
            replay,
        } = context;
        let model =
            self.apply_workload_update(base_model, descriptor, update, contract, store, device)?;
        Ok(ValidatedWorkloadUpdate {
            model,
            evidence: crate::ValidatedUpdateEvidence {
                update_envelope_id: ContentId::derive(update)?,
                norm_stats: None,
                feature_sketch: None,
                reconstruction_verified: true,
                replay_verified: !contract.update_codec.requires_independent_replay(),
                replay_stats: None,
                validator_peer_id: replay.validator_peer_id.clone(),
                validated_at: Utc::now(),
            },
        })
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }
}
