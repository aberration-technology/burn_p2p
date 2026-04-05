use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc};

use chrono::Utc;

use super::*;

#[derive(Clone, Debug)]
/// Configures the local dataset fallback used by
/// [`BurnLearnerProjectBuilderAdvancedExt::with_batches`],
/// [`BurnLearnerProjectBuilderAdvancedExt::with_assignment_batches`], and
/// [`BurnLearnerProjectBuilder::with_train_loader`].
pub struct BurnLocalDatasetConfig {
    /// Label used to derive stable dataset and view ids.
    pub dataset_name: String,
    /// Approximate dataset sizing used for planning.
    pub sizing: crate::DatasetSizing,
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
    dataset_registration: Arc<LearnerDatasetRegistrationFn>,
    microshard_plan: Arc<LearnerMicroshardPlanFn>,
    load_batches: Arc<LearnerBatchLoaderFn<LC>>,
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
            dataset_registration: Arc::clone(&self.dataset_registration),
            microshard_plan: Arc::clone(&self.microshard_plan),
            load_batches: Arc::clone(&self.load_batches),
            after_train_step: Arc::clone(&self.after_train_step),
            after_window: Arc::clone(&self.after_window),
        }
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
    dataset_registration: Option<Arc<LearnerDatasetRegistrationFn>>,
    microshard_plan: Option<Arc<LearnerMicroshardPlanFn>>,
    load_batches: Option<Arc<LearnerBatchLoaderFn<LC>>>,
    train_loader: Option<BurnTrainLoader<LC>>,
    valid_loader: Option<BurnValidLoader<LC>>,
    local_batches: Option<Arc<LearnerBatchFn<LC>>>,
    assignment_batches: Option<Arc<LearnerAssignmentBatchFn<LC>>>,
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
/// plus train/eval dataloaders.
///
/// Prefer this when the project already has a clean `Learner + train loader +
/// eval loader` seam.
///
/// loader naming here is intentionally generic:
///
/// - train loader: batches used for local window training
/// - eval loader: batches used for local model evaluation
///
/// self-supervised workloads fit naturally if they already expose train/eval
/// batch loaders. paradigms that do not naturally use dataloaders, such as
/// some rl flows, should usually use [`BurnLearnerWorkload`] or
/// [`BurnWorkload`] instead.
pub fn from_loaders<LC>(
    learner: BurnLearner<LC>,
    device: BurnLearnerDevice<LC>,
    train_loader: BurnTrainLoader<LC>,
    eval_loader: BurnEvalLoader<LC>,
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
    from_learner(learner, device).with_loaders(train_loader, eval_loader)
}

/// Advanced burn builder hooks for projects that need custom local batch or
/// shard plumbing beyond the common loader-based path.
pub trait BurnLearnerProjectBuilderAdvancedExt<LC>: Sized
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
    /// Sets dataset registration.
    fn with_dataset_registration<F>(self, dataset_registration: F) -> Self
    where
        F: Fn() -> anyhow::Result<crate::DatasetRegistration> + Send + Sync + 'static;

    /// Sets a fixed dataset registration and microshard plan.
    fn with_dataset(
        self,
        registration: crate::DatasetRegistration,
        microshard_plan: crate::MicroShardPlan,
    ) -> Self;

    /// Overrides the local dataset metadata used by `with_batches(...)`,
    /// `with_assignment_batches(...)`, and `with_train_loader(...)`.
    fn with_local_dataset(self, config: BurnLocalDatasetConfig) -> Self;

    /// Sets microshard planning.
    fn with_microshard_plan<F>(self, microshard_plan: F) -> Self
    where
        F: Fn(&crate::DatasetRegistration) -> anyhow::Result<crate::MicroShardPlan>
            + Send
            + Sync
            + 'static;

    /// Sets batch loading from cached microshards.
    fn with_load_batches<F>(self, load_batches: F) -> Self
    where
        F: Fn(
                &AssignmentLease,
                &[CachedMicroShard],
                &BurnLearnerDevice<LC>,
            ) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
            + Send
            + Sync
            + 'static;

    /// Sets a simple batch source for local learner-owned datasets.
    fn with_batches<F>(self, batches: F) -> Self
    where
        F: Fn(&BurnLearnerDevice<LC>) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
            + Send
            + Sync
            + 'static;

    /// Sets a lease-aware batch source without explicit shard plumbing.
    fn with_assignment_batches<F>(self, batches: F) -> Self
    where
        F: Fn(
                &AssignmentLease,
                &BurnLearnerDevice<LC>,
            ) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
            + Send
            + Sync
            + 'static;
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
            dataset_registration: None,
            microshard_plan: None,
            load_batches: None,
            train_loader: None,
            valid_loader: None,
            local_batches: None,
            assignment_batches: None,
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

    /// Sets the train dataloader used for p2p windows.
    ///
    /// `burn_p2p` will iterate the loader once per window and use the emitted
    /// batches as the local contribution for that lease.
    pub fn with_train_loader(mut self, train_loader: BurnTrainLoader<LC>) -> Self {
        self.train_loader = Some(train_loader);
        self
    }

    /// Sets the evaluation dataloader used by the default evaluation path.
    ///
    /// If no custom `.with_evaluate(...)` hook is provided, `burn_p2p` will run
    /// the inference model over the loader and emit generic evaluation counts.
    pub fn with_eval_loader(mut self, eval_loader: BurnEvalLoader<LC>) -> Self {
        self.valid_loader = Some(eval_loader);
        self
    }

    /// Sets both train and eval dataloaders.
    pub fn with_loaders(
        mut self,
        train_loader: BurnTrainLoader<LC>,
        eval_loader: BurnEvalLoader<LC>,
    ) -> Self {
        self.train_loader = Some(train_loader);
        self.valid_loader = Some(eval_loader);
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
        self.dataset_registration = Some(Arc::new(move || Ok(registration.clone())));
        self.microshard_plan = Some(Arc::new(move |_registration| Ok(microshard_plan.clone())));
        self.load_batches = Some(Arc::new(move |_lease, cached_microshards, device| {
            load_dataset.load_batches(cached_microshards, batcher.clone(), batch_size, device)
        }));
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

    /// Finalizes the learner-backed workload.
    pub fn build(self) -> anyhow::Result<BurnLearnerProject<LC>> {
        self.build_with_training_hooks(true)
    }

    fn build_with_training_hooks(
        self,
        require_training_hooks: bool,
    ) -> anyhow::Result<BurnLearnerProject<LC>> {
        let local_dataset_bundle = if self.load_batches.is_none() {
            if let Some(batches) = self.local_batches.as_ref() {
                Some(local_dataset_bundle::<LC>(
                    &self.local_dataset,
                    Arc::new({
                        let batches = batches.clone();
                        move |_lease: &AssignmentLease,
                              _cached_microshards: &[CachedMicroShard],
                              device: &BurnLearnerDevice<LC>| {
                            batches(device)
                        }
                    }),
                )?)
            } else if let Some(train_loader) = self.train_loader.as_ref() {
                Some(local_dataset_bundle::<LC>(
                    &self.local_dataset,
                    loader_batch_source::<LC>(train_loader.clone()),
                )?)
            } else {
                self.assignment_batches
                    .as_ref()
                    .map(|batches| {
                        local_dataset_bundle::<LC>(
                            &self.local_dataset,
                            Arc::new({
                                let batches = batches.clone();
                                move |lease: &AssignmentLease,
                                      _cached_microshards: &[CachedMicroShard],
                                      device: &BurnLearnerDevice<LC>| { batches(lease, device) }
                            }),
                        )
                    })
                    .transpose()?
            }
        } else {
            None
        };
        let passive_dataset_bundle = if !require_training_hooks
            && self.dataset_registration.is_none()
            && self.microshard_plan.is_none()
            && self.load_batches.is_none()
            && local_dataset_bundle.is_none()
        {
            Some(passive_dataset_bundle::<LC>(&self.local_dataset)?)
        } else {
            None
        };

        Ok(BurnLearnerProject {
            learner: self.learner,
            device: self.device,
            benchmark: self.benchmark,
            evaluate: self
                .evaluate
                .or_else(|| {
                    self.valid_loader.as_ref().map(|valid_loader| {
                        loader_evaluate_fn::<LC>(valid_loader.clone())
                    })
                })
                .unwrap_or_else(|| Arc::new(default_learner_evaluate::<LC>)),
            dataset_registration: self
                .dataset_registration
                .or_else(|| {
                    local_dataset_bundle.as_ref().map(|(registration, _, _)| {
                        Arc::new({
                            let registration = registration.clone();
                            move || Ok(registration.clone())
                        }) as Arc<LearnerDatasetRegistrationFn>
                    })
                })
                .or_else(|| {
                    passive_dataset_bundle.as_ref().map(|(registration, _, _)| {
                        Arc::new({
                            let registration = registration.clone();
                            move || Ok(registration.clone())
                        }) as Arc<LearnerDatasetRegistrationFn>
                    })
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "missing burn learner training data; use from_loaders(...), with_train_loader(...), with_sharded_dataset(...), or the advanced dataset hooks"
                    )
                })?,
            microshard_plan: self
                .microshard_plan
                .or_else(|| {
                    local_dataset_bundle.as_ref().map(|(_, plan, _)| {
                        let plan = plan.clone();
                        let plan_fn: Arc<LearnerMicroshardPlanFn> = Arc::new(
                            move |_registration: &crate::DatasetRegistration| Ok(plan.clone()),
                        );
                        plan_fn
                    })
                })
                .or_else(|| {
                    passive_dataset_bundle.as_ref().map(|(_, plan, _)| {
                        let plan = plan.clone();
                        let plan_fn: Arc<LearnerMicroshardPlanFn> = Arc::new(
                            move |_registration: &crate::DatasetRegistration| Ok(plan.clone()),
                        );
                        plan_fn
                    })
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "missing burn learner training data; use from_loaders(...), with_train_loader(...), with_sharded_dataset(...), or the advanced dataset hooks"
                    )
                })?,
            load_batches: self
                .load_batches
                .or_else(|| {
                    local_dataset_bundle
                        .as_ref()
                        .map(|(_, _, load_batches)| load_batches.clone())
                })
                .or_else(|| {
                    passive_dataset_bundle
                        .as_ref()
                        .map(|(_, _, load_batches)| load_batches.clone())
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "missing burn learner batch loader; use from_loaders(...), with_train_loader(...), with_sharded_dataset(...), or the advanced dataset hooks"
                    )
                })?,
            after_train_step: self.after_train_step,
            after_window: self.after_window,
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

impl<LC> BurnLearnerProjectBuilderAdvancedExt<LC> for BurnLearnerProjectBuilder<LC>
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
    fn with_dataset_registration<F>(mut self, dataset_registration: F) -> Self
    where
        F: Fn() -> anyhow::Result<crate::DatasetRegistration> + Send + Sync + 'static,
    {
        self.dataset_registration = Some(Arc::new(dataset_registration));
        self
    }

    fn with_dataset(
        mut self,
        registration: crate::DatasetRegistration,
        microshard_plan: crate::MicroShardPlan,
    ) -> Self {
        self.dataset_registration = Some(Arc::new(move || Ok(registration.clone())));
        self.microshard_plan = Some(Arc::new(move |_registration| Ok(microshard_plan.clone())));
        self
    }

    fn with_local_dataset(mut self, config: BurnLocalDatasetConfig) -> Self {
        self.local_dataset = config;
        self
    }

    fn with_microshard_plan<F>(mut self, microshard_plan: F) -> Self
    where
        F: Fn(&crate::DatasetRegistration) -> anyhow::Result<crate::MicroShardPlan>
            + Send
            + Sync
            + 'static,
    {
        self.microshard_plan = Some(Arc::new(microshard_plan));
        self
    }

    fn with_load_batches<F>(mut self, load_batches: F) -> Self
    where
        F: Fn(
                &AssignmentLease,
                &[CachedMicroShard],
                &BurnLearnerDevice<LC>,
            ) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
            + Send
            + Sync
            + 'static,
    {
        self.load_batches = Some(Arc::new(load_batches));
        self
    }

    fn with_batches<F>(mut self, batches: F) -> Self
    where
        F: Fn(&BurnLearnerDevice<LC>) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
            + Send
            + Sync
            + 'static,
    {
        self.local_batches = Some(Arc::new(batches));
        self
    }

    fn with_assignment_batches<F>(mut self, batches: F) -> Self
    where
        F: Fn(
                &AssignmentLease,
                &BurnLearnerDevice<LC>,
            ) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
            + Send
            + Sync
            + 'static,
    {
        self.assignment_batches = Some(Arc::new(batches));
        self
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
    valid_loader: BurnValidLoader<LC>,
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
    let valid_loader = valid_loader.to_device(&device);
    let evaluation_items = valid_loader.num_items() as i64;
    let mut evaluation_batches = 0_i64;
    let iterator = valid_loader.iter();
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

fn loader_evaluate_fn<LC>(valid_loader: BurnValidLoader<LC>) -> Arc<LearnerEvaluateFn<LC>>
where
    LC: LearningComponentsTypes + 'static,
    BurnLearnerEvalModel<LC>: BurnModuleTarget<<BurnLearnerBackend<LC> as AutodiffBackend>::InnerBackend>
        + InferenceStep
        + Clone
        + 'static,
{
    Arc::new(
        move |model: &BurnLearnerEvalModel<LC>, split: EvalSplit| -> MetricReport {
            default_loader_evaluate::<LC>(model, split, valid_loader.clone())
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
    load_batches: Arc<LearnerBatchLoaderFn<LC>>,
) -> anyhow::Result<(
    crate::DatasetRegistration,
    crate::MicroShardPlan,
    Arc<LearnerBatchLoaderFn<LC>>,
)>
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

    Ok((registration, plan, load_batches))
}

fn passive_dataset_bundle<LC>(
    config: &BurnLocalDatasetConfig,
) -> anyhow::Result<(
    crate::DatasetRegistration,
    crate::MicroShardPlan,
    Arc<LearnerBatchLoaderFn<LC>>,
)>
where
    LC: LearningComponentsTypes + 'static,
{
    local_dataset_bundle::<LC>(
        config,
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
        (self.dataset_registration)()
    }

    fn microshard_plan(
        &self,
        registration: &crate::DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        (self.microshard_plan)(registration)
    }

    fn load_batches(
        &self,
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        (self.load_batches)(lease, cached_microshards, &self.device)
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }
}
