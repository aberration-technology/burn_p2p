use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc};

use chrono::Utc;

use super::*;

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
        }
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
        after_window: impl Fn(&BurnLearner<LC>, &mut BTreeMap<String, MetricValue>) -> Result<(), TrainError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.after_window = Arc::new(after_window);
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
        } = self;
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
        work_units_per_second: inventory.parameter_count.max(1) as f64,
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
        metrics: BTreeMap::from([(
            "parameter_count".into(),
            MetricValue::Integer(inventory.parameter_count as i64),
        )]),
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

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }
}
