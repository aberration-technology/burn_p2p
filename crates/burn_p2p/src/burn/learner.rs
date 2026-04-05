use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc};

use chrono::Utc;

use super::*;

#[derive(Clone, Debug)]
/// Configures the local dataset fallback used by [`BurnLearnerProjectBuilder::with_batches`].
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

#[deprecated(note = "use BurnLocalDatasetConfig")]
#[doc(hidden)]
pub type BurnSyntheticDatasetConfig = BurnLocalDatasetConfig;

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

    /// Sets dataset registration.
    pub fn with_dataset_registration(
        mut self,
        dataset_registration: impl Fn() -> anyhow::Result<crate::DatasetRegistration>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.dataset_registration = Some(Arc::new(dataset_registration));
        self
    }

    /// Sets a fixed dataset registration and microshard plan.
    pub fn with_dataset(
        mut self,
        registration: crate::DatasetRegistration,
        microshard_plan: crate::MicroShardPlan,
    ) -> Self {
        self.dataset_registration = Some(Arc::new(move || Ok(registration.clone())));
        self.microshard_plan = Some(Arc::new(move |_registration| Ok(microshard_plan.clone())));
        self
    }

    /// Overrides the local dataset metadata used by [`Self::with_batches`]
    /// and [`Self::with_assignment_batches`].
    pub fn with_local_dataset(mut self, config: BurnLocalDatasetConfig) -> Self {
        self.local_dataset = config;
        self
    }

    #[deprecated(note = "use with_local_dataset")]
    #[doc(hidden)]
    pub fn with_synthetic_dataset(self, config: BurnLocalDatasetConfig) -> Self {
        self.with_local_dataset(config)
    }

    /// Sets microshard planning.
    pub fn with_microshard_plan(
        mut self,
        microshard_plan: impl Fn(&crate::DatasetRegistration) -> anyhow::Result<crate::MicroShardPlan>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.microshard_plan = Some(Arc::new(microshard_plan));
        self
    }

    /// Sets batch loading from cached microshards.
    pub fn with_load_batches(
        mut self,
        load_batches: impl Fn(
            &AssignmentLease,
            &[CachedMicroShard],
            &BurnLearnerDevice<LC>,
        ) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.load_batches = Some(Arc::new(load_batches));
        self
    }

    /// Sets a simple batch source for local learner-owned datasets.
    ///
    /// The runtime will generate local dataset registration, microshard planning,
    /// fetch manifest, and placeholder shard bytes automatically.
    ///
    /// Each window still runs through the real burn learner loop. The closure
    /// only supplies the training batches for the local device.
    pub fn with_batches(
        mut self,
        batches: impl Fn(&BurnLearnerDevice<LC>) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.local_batches = Some(Arc::new(batches));
        self
    }

    /// Sets a lease-aware batch source without requiring explicit dataset or
    /// shard plumbing.
    ///
    /// Use this when local batch selection depends on the assignment lease, but
    /// the runtime should still generate the default local dataset metadata.
    pub fn with_assignment_batches(
        mut self,
        batches: impl Fn(
            &AssignmentLease,
            &BurnLearnerDevice<LC>,
        ) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.assignment_batches = Some(Arc::new(batches));
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

        Ok(BurnLearnerProject {
            learner: self.learner,
            device: self.device,
            benchmark: self.benchmark,
            evaluate: self
                .evaluate
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
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "missing burn learner dataset hooks; use with_dataset(...) + with_load_batches(...), with_assignment_batches(...), or with_batches(...)"
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
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "missing burn learner dataset hooks; use with_dataset(...) + with_load_batches(...), with_assignment_batches(...), or with_batches(...)"
                    )
                })?,
            load_batches: self
                .load_batches
                .or_else(|| {
                    local_dataset_bundle
                        .as_ref()
                        .map(|(_, _, load_batches)| load_batches.clone())
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "missing burn learner batch loader; use with_load_batches(...), with_assignment_batches(...), or with_batches(...)"
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
        connect(target, release_manifest, self.build()?, config)
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

    #[deprecated(note = "use trainer")]
    /// Backward-compatible alias for the older default-config trainer helper.
    #[doc(hidden)]
    pub fn trainer_for(
        self,
        release_manifest: ClientReleaseManifest,
        supported_workload: SupportedWorkload,
    ) -> anyhow::Result<
        NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<BurnLearnerProject<LC>>>>,
    > {
        self.trainer(release_manifest, supported_workload)
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

    #[deprecated(note = "use validator")]
    /// Backward-compatible alias for the older default-config validator helper.
    #[doc(hidden)]
    pub fn validator_for(
        self,
        release_manifest: ClientReleaseManifest,
        supported_workload: SupportedWorkload,
    ) -> anyhow::Result<
        NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<BurnLearnerProject<LC>>>>,
    > {
        self.validator(release_manifest, supported_workload)
    }

    #[deprecated(note = "use connect")]
    /// Backward-compatible alias for the older default-config target helper.
    #[doc(hidden)]
    pub fn connect_for(
        self,
        target: BurnTarget,
        release_manifest: ClientReleaseManifest,
        supported_workload: SupportedWorkload,
    ) -> anyhow::Result<
        NodeBuilder<SingleWorkloadProjectFamily<BurnWorkloadAdapter<BurnLearnerProject<LC>>>>,
    > {
        self.connect(target, release_manifest, supported_workload)
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
