use std::{collections::BTreeMap, path::PathBuf};

use anyhow::Context;
use burn_p2p_checkpoint::{ArtifactBuildSpec, ChunkingScheme, FsArtifactStore};
use burn_p2p_core::{
    ArtifactDescriptor, ArtifactKind, AssignmentLease, CapabilityEstimate, ContentId, DatasetId,
    DatasetManifest, DatasetView, DatasetViewId, HeadId, MergePolicy, MetricValue, Precision,
    SupportedWorkload,
};
use burn_p2p_dataloader::{
    CachedMicroShard, DatasetRegistration, DatasetSizing, MicroShardPlan, MicroShardPlanner,
    MicroShardPlannerConfig, UpstreamAdapter,
};
use burn_p2p_experiment::{PatchSupport, RuntimePatch};
use burn_p2p_workload::{
    EvalSplit, LeaseDataPipeline, LeaseDataPipelineDescriptor, LeaseDataPipelineKind,
    MergeModelCandidate, MetricReport, P2pWorkload, PatchOutcome, TrainError,
    TrainerCanonicalReconcileStrategy, WindowCtx, WindowReport, local_upstream_root_for_pipeline,
    standard_contribution_weight,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod worker;

use worker::{PythonMergeCandidateRef, PythonWorkerClient};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Configures how the Rust runtime launches the Python worker process.
pub struct PythonTorchRuntimeConfig {
    /// Python executable used to launch the runtime worker.
    pub python_executable: PathBuf,
    /// Additional module roots appended to `PYTHONPATH`.
    pub module_search_roots: Vec<PathBuf>,
    /// Python workload factory in `module:attr` form.
    pub workload_factory: String,
    /// JSON config passed directly to the Python workload.
    pub workload_config: Value,
    /// Additional environment variables for the worker process.
    pub env: BTreeMap<String, String>,
}

impl PythonTorchRuntimeConfig {
    /// Creates a new config for the provided Python workload factory.
    pub fn new(
        python_executable: impl Into<PathBuf>,
        workload_factory: impl Into<String>,
        workload_config: Value,
    ) -> Self {
        Self {
            python_executable: python_executable.into(),
            module_search_roots: Vec::new(),
            workload_factory: workload_factory.into(),
            workload_config,
            env: BTreeMap::new(),
        }
    }

    /// Adds one extra Python import root.
    pub fn with_module_search_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.module_search_roots.push(root.into());
        self
    }

    /// Adds one environment override for the worker process.
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Declares the shard-backed dataset view exposed to the p2p runtime.
pub struct PythonTorchDatasetConfig {
    /// Root containing `fetch-manifest.json` and shard files.
    pub root: PathBuf,
    /// Stable dataset id.
    pub dataset_id: DatasetId,
    /// Stable dataset view id.
    pub dataset_view_id: DatasetViewId,
    /// Source URI surfaced in dataset metadata.
    pub source_uri: String,
    /// Dataset format tag.
    pub format: String,
    /// Dataset manifest hash.
    pub manifest_hash: ContentId,
    /// Preprocessing hash for the view.
    pub preprocessing_hash: ContentId,
    /// Optional tokenizer hash.
    pub tokenizer_hash: Option<ContentId>,
    /// Dataset sizing used to plan microshards.
    pub sizing: DatasetSizing,
    /// Planner config used to derive microshard ids.
    pub planner: MicroShardPlannerConfig,
    /// Number of cached microshards grouped into one Python batch ref.
    pub microshards_per_batch: usize,
    /// Arbitrary dataset metadata propagated into the registration.
    pub metadata: BTreeMap<String, String>,
}

impl PythonTorchDatasetConfig {
    /// Returns a local-upstream dataset registration.
    pub fn registration(&self) -> DatasetRegistration {
        DatasetRegistration {
            manifest: DatasetManifest {
                dataset_id: self.dataset_id.clone(),
                source_uri: self.source_uri.clone(),
                format: self.format.clone(),
                manifest_hash: self.manifest_hash.clone(),
                metadata: self.metadata.clone(),
            },
            view: DatasetView {
                dataset_view_id: self.dataset_view_id.clone(),
                dataset_id: self.dataset_id.clone(),
                preprocessing_hash: self.preprocessing_hash.clone(),
                tokenizer_hash: self.tokenizer_hash.clone(),
                manifest_hash: self.manifest_hash.clone(),
                metadata: self.metadata.clone(),
            },
            upstream: UpstreamAdapter::Local {
                root: self.root.display().to_string(),
            },
        }
    }

    /// Plans the microshards for this dataset view.
    pub fn plan(&self) -> anyhow::Result<MicroShardPlan> {
        let registration = self.registration();
        Ok(MicroShardPlanner::new(self.planner.clone())?
            .plan(&registration.view, self.sizing.clone())?)
    }
}

#[derive(Clone, Debug)]
/// Static workload identity and artifact settings for one Python/Torch runtime.
pub struct PythonTorchWorkloadConfig {
    /// Python worker launch config.
    pub runtime: PythonTorchRuntimeConfig,
    /// Dataset/shard config.
    pub dataset: PythonTorchDatasetConfig,
    /// Workload identity published into the release manifest.
    pub supported_workload: SupportedWorkload,
    /// Stable model schema hash.
    pub model_schema_hash: ContentId,
    /// Artifact record format tag.
    pub artifact_record_format: String,
    /// Descriptor precision published with model artifacts.
    pub artifact_precision: Precision,
    /// Chunking policy for stored model artifacts.
    pub artifact_chunking: ChunkingScheme,
    /// Patch support advertised by the workload.
    pub patch_support: PatchSupport,
}

impl PythonTorchWorkloadConfig {
    /// Creates a new config with a safetensors-backed artifact default.
    pub fn new(
        runtime: PythonTorchRuntimeConfig,
        dataset: PythonTorchDatasetConfig,
        supported_workload: SupportedWorkload,
        model_schema_hash: ContentId,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            runtime,
            dataset,
            supported_workload,
            model_schema_hash,
            artifact_record_format: "python-torch-safetensors".to_owned(),
            artifact_precision: Precision::Fp32,
            artifact_chunking: ChunkingScheme::new(256 * 1024)?,
            patch_support: PatchSupport {
                hot: false,
                warm: false,
                cold: false,
            },
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
/// One lease-loaded micro-epoch batch passed to the Python worker.
pub enum PythonBatchRef {
    /// Cached shard group materialized by the Rust shard cache.
    CachedMicroshardGroup {
        /// Paths of the cached microshards that should be consumed together.
        shard_paths: Vec<PathBuf>,
        /// Stable microshard ids included in this batch group.
        microshard_ids: Vec<String>,
        /// Ordinals of the grouped microshards.
        ordinals: Vec<u32>,
        /// Total byte size represented by the group.
        bytes_len: u64,
    },
    /// Lease-scoped micro-epoch descriptor rebuilt inside Python.
    MicroEpoch {
        /// Lease id driving this micro-epoch.
        lease_id: String,
        /// Stable microshard ids included in the lease.
        microshard_ids: Vec<String>,
        /// Ordinals of the microshards included in the lease.
        ordinals: Vec<u32>,
        /// Total byte size represented by the cached microshards, when known.
        bytes_len: u64,
        /// High-level pipeline kind associated with this lease.
        pipeline_kind: LeaseDataPipelineKind,
        /// Opaque workload-specific payload consumed by Python.
        payload: Value,
    },
}

impl PythonBatchRef {
    /// Builds a shard-backed batch ref.
    pub fn cached_microshard_group(group: &[CachedMicroShard]) -> Self {
        Self::CachedMicroshardGroup {
            shard_paths: group.iter().map(|entry| entry.path.clone()).collect(),
            microshard_ids: group
                .iter()
                .map(|entry| entry.microshard.microshard_id.as_str().to_owned())
                .collect(),
            ordinals: group.iter().map(|entry| entry.microshard.ordinal).collect(),
            bytes_len: group.iter().map(|entry| entry.bytes_len).sum(),
        }
    }

    /// Builds a generic micro-epoch descriptor for Python-side dataloader reconstruction.
    pub fn micro_epoch(
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
        pipeline_kind: LeaseDataPipelineKind,
        payload: Value,
    ) -> Self {
        Self::MicroEpoch {
            lease_id: lease.lease_id.as_str().to_owned(),
            microshard_ids: lease
                .microshards
                .iter()
                .map(|microshard_id| microshard_id.as_str().to_owned())
                .collect(),
            ordinals: cached_microshards
                .iter()
                .map(|entry| entry.microshard.ordinal)
                .collect(),
            bytes_len: cached_microshards.iter().map(|entry| entry.bytes_len).sum(),
            pipeline_kind,
            payload,
        }
    }
}

#[derive(Debug)]
/// Worker-owned model/optimizer state referenced by one opaque handle id.
pub struct PythonModelHandle {
    id: String,
    client: PythonWorkerClient,
}

impl PythonModelHandle {
    fn new(id: String, client: PythonWorkerClient) -> Self {
        Self { id, client }
    }

    fn id(&self) -> &str {
        &self.id
    }
}

impl Drop for PythonModelHandle {
    fn drop(&mut self) {
        self.client.release_model(&self.id);
    }
}

#[derive(Clone, Debug)]
/// Python/Torch-backed workload bridge implemented on top of the generic p2p runtime.
pub struct PythonTorchProject {
    client: PythonWorkerClient,
    config: PythonTorchWorkloadConfig,
    data_pipeline: LeaseDataPipeline<String, PythonBatchRef>,
    workload_name: String,
    runtime_device: String,
    capability: CapabilityEstimate,
}

impl PythonTorchProject {
    /// Spawns the backing Python worker and probes its runtime capability.
    pub fn new(config: PythonTorchWorkloadConfig) -> anyhow::Result<Self> {
        let data_pipeline = Self::sharded_data_pipeline(&config.dataset);
        Self::new_with_data_pipeline(config, data_pipeline)
    }

    /// Spawns the backing Python worker with an explicit lease data pipeline.
    pub fn new_with_data_pipeline(
        config: PythonTorchWorkloadConfig,
        data_pipeline: LeaseDataPipeline<String, PythonBatchRef>,
    ) -> anyhow::Result<Self> {
        let client = PythonWorkerClient::spawn(&config.runtime)?;
        let hello = client.hello()?;
        if hello.protocol_version != 1 {
            anyhow::bail!(
                "python worker protocol mismatch: expected 1, got {}",
                hello.protocol_version
            );
        }
        let probe = client.capability_probe()?;
        Ok(Self {
            client,
            config,
            data_pipeline,
            workload_name: hello.workload_name,
            runtime_device: probe.runtime_device,
            capability: probe.capability,
        })
    }

    /// Returns the default shard-backed lease data pipeline for one dataset config.
    pub fn sharded_data_pipeline(
        dataset: &PythonTorchDatasetConfig,
    ) -> LeaseDataPipeline<String, PythonBatchRef> {
        let registration = dataset.registration();
        let microshard_plan = dataset.plan().expect("python dataset plan should resolve");
        let group_size = dataset.microshards_per_batch.max(1);
        LeaseDataPipeline::new(
            LeaseDataPipelineDescriptor::new(
                "python-sharded-dataset",
                LeaseDataPipelineKind::ShardedStatic,
            )
            .with_metadata_entry("format", dataset.format.clone()),
            move || Ok(registration.clone()),
            move |_registration| Ok(microshard_plan.clone()),
            move |_lease, cached_microshards, _device| {
                let batch_count = cached_microshards.len().div_ceil(group_size).max(1);
                let mut batches = Vec::with_capacity(batch_count);
                for group in cached_microshards.chunks(group_size) {
                    batches.push(PythonBatchRef::cached_microshard_group(group));
                }
                Ok(batches)
            },
        )
    }

    /// Builds a generic Python micro-epoch pipeline backed by workload-defined payloads.
    pub fn micro_epoch_pipeline(
        descriptor: LeaseDataPipelineDescriptor,
        dataset_registration: impl Fn() -> anyhow::Result<DatasetRegistration> + Send + Sync + 'static,
        microshard_plan: impl Fn(&DatasetRegistration) -> anyhow::Result<MicroShardPlan>
        + Send
        + Sync
        + 'static,
        payload: impl Fn(&AssignmentLease, &[CachedMicroShard]) -> anyhow::Result<Value>
        + Send
        + Sync
        + 'static,
    ) -> LeaseDataPipeline<String, PythonBatchRef> {
        let pipeline_kind = descriptor.kind;
        LeaseDataPipeline::new(
            descriptor,
            dataset_registration,
            microshard_plan,
            move |lease, cached_microshards, _device| {
                Ok(vec![PythonBatchRef::micro_epoch(
                    lease,
                    cached_microshards,
                    pipeline_kind,
                    payload(lease, cached_microshards)?,
                )])
            },
        )
    }

    /// Builds a Python pipeline for existing torch `Dataset`/`Sampler`-style data flows.
    pub fn indexed_dataset_pipeline(
        pipeline_name: impl Into<String>,
        dataset_registration: impl Fn() -> anyhow::Result<DatasetRegistration> + Send + Sync + 'static,
        microshard_plan: impl Fn(&DatasetRegistration) -> anyhow::Result<MicroShardPlan>
        + Send
        + Sync
        + 'static,
        payload: impl Fn(&AssignmentLease, &[CachedMicroShard]) -> anyhow::Result<Value>
        + Send
        + Sync
        + 'static,
    ) -> LeaseDataPipeline<String, PythonBatchRef> {
        Self::micro_epoch_pipeline(
            LeaseDataPipelineDescriptor::new(pipeline_name, LeaseDataPipelineKind::IndexedDataset),
            dataset_registration,
            microshard_plan,
            payload,
        )
    }

    /// Builds a Python pipeline for deterministic synthetic or recipe-driven data generation.
    pub fn generated_dataset_pipeline(
        pipeline_name: impl Into<String>,
        dataset_registration: impl Fn() -> anyhow::Result<DatasetRegistration> + Send + Sync + 'static,
        microshard_plan: impl Fn(&DatasetRegistration) -> anyhow::Result<MicroShardPlan>
        + Send
        + Sync
        + 'static,
        payload: impl Fn(&AssignmentLease, &[CachedMicroShard]) -> anyhow::Result<Value>
        + Send
        + Sync
        + 'static,
    ) -> LeaseDataPipeline<String, PythonBatchRef> {
        Self::micro_epoch_pipeline(
            LeaseDataPipelineDescriptor::new(
                pipeline_name,
                LeaseDataPipelineKind::GeneratedDataset,
            ),
            dataset_registration,
            microshard_plan,
            payload,
        )
    }

    /// Returns the worker-advertised capability estimate.
    pub fn probe_capability(&self) -> &CapabilityEstimate {
        &self.capability
    }

    /// Returns the resolved runtime device tag.
    pub fn runtime_device_name(&self) -> &str {
        &self.runtime_device
    }

    /// Returns the Python-side workload name advertised by the worker.
    pub fn workload_name(&self) -> &str {
        &self.workload_name
    }

    /// Returns the static lease/micro-epoch data pipeline descriptor.
    pub fn data_pipeline_descriptor(&self) -> &LeaseDataPipelineDescriptor {
        self.data_pipeline.descriptor()
    }

    /// Returns the configured lease/micro-epoch pipeline kind.
    pub fn data_pipeline_kind(&self) -> LeaseDataPipelineKind {
        self.data_pipeline.kind()
    }

    /// Returns the dataset registration backing the current pipeline.
    pub fn data_pipeline_registration(&self) -> anyhow::Result<DatasetRegistration> {
        self.data_pipeline.dataset_registration()
    }

    /// Returns the local upstream root when the current pipeline is backed by
    /// a `Local` dataset registration.
    pub fn local_upstream_root(&self) -> anyhow::Result<Option<PathBuf>> {
        local_upstream_root_for_pipeline(&self.data_pipeline)
    }

    /// Returns the configured shard root for the default sharded Python dataset
    /// config. This is configuration data only and may be unrelated to the
    /// active pipeline when `new_with_data_pipeline(...)` is used.
    pub fn configured_shard_root(&self) -> &std::path::Path {
        &self.config.dataset.root
    }
}

impl P2pWorkload for PythonTorchProject {
    type Device = String;
    type Model = PythonModelHandle;
    type Batch = PythonBatchRef;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, device: &Self::Device) -> Self::Model {
        let model_id = self
            .client
            .init_model(device)
            .expect("python worker should initialize a model");
        PythonModelHandle::new(model_id, self.client.clone())
    }

    fn benchmark(&self, _model: &Self::Model, _device: &Self::Device) -> CapabilityEstimate {
        self.capability.clone()
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<Self::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        let mut metrics = self
            .client
            .train_window(ctx.model.id(), &ctx.batches)
            .map_err(|error| TrainError::new(error.to_string()))?;
        metrics.insert(
            "batch_count".into(),
            MetricValue::Integer(ctx.batches.len() as i64),
        );
        let examples_processed = ctx
            .cached_microshards
            .iter()
            .map(|cached| cached.microshard.estimated_examples)
            .sum::<u64>();
        let tokens_processed = ctx
            .cached_microshards
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
        if !ctx.cached_microshards.is_empty() {
            metrics.insert(
                "microshard_count".into(),
                MetricValue::Integer(ctx.cached_microshards.len() as i64),
            );
        }
        Ok(WindowReport {
            contribution: None,
            stats: metrics,
            completed_at: Utc::now(),
        })
    }

    fn evaluate(&self, model: &Self::Model, split: EvalSplit) -> MetricReport {
        let metrics = self
            .client
            .evaluate(model.id(), split)
            .unwrap_or_else(|error| {
                BTreeMap::from([("python_error".into(), MetricValue::Text(error.to_string()))])
            });
        MetricReport {
            metrics,
            captured_at: Utc::now(),
        }
    }

    fn apply_patch(&mut self, patch: &RuntimePatch) -> PatchOutcome {
        self.client
            .apply_patch(patch)
            .unwrap_or_else(|error| PatchOutcome::Rejected(error.to_string()))
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        self.config.patch_support
    }

    fn runtime_device(&self) -> Self::Device {
        self.runtime_device.clone()
    }

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        self.data_pipeline.dataset_registration()
    }

    fn microshard_plan(
        &self,
        _registration: &DatasetRegistration,
    ) -> anyhow::Result<MicroShardPlan> {
        self.data_pipeline.microshard_plan(_registration)
    }

    fn load_batches(
        &self,
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        self.data_pipeline
            .load_batches(lease, cached_microshards, &self.runtime_device)
    }

    fn load_model_artifact(
        &self,
        model: Self::Model,
        descriptor: &ArtifactDescriptor,
        store: &FsArtifactStore,
        _device: &Self::Device,
    ) -> anyhow::Result<Self::Model> {
        let staged_dir = tempfile::Builder::new()
            .prefix("burn-p2p-python-load-artifact")
            .tempdir()?;
        let staged_path = staged_dir.path().join("artifact.safetensors");
        store.materialize_artifact_file(descriptor, &staged_path)?;
        self.client
            .load_model_artifact_path(model.id(), &staged_path)
            .context("load python model artifact into worker")?;
        Ok(model)
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: ArtifactKind,
        head_id: HeadId,
        base_head_id: Option<HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let staged_dir = tempfile::Builder::new()
            .prefix("burn-p2p-python-materialized-artifact")
            .tempdir()?;
        let staged_path = staged_dir.path().join("artifact.safetensors");
        self.client
            .materialize_model_artifact_path(model.id(), &staged_path)
            .context("materialize python model artifact")?;
        let mut spec = ArtifactBuildSpec::new(
            artifact_kind,
            self.config.artifact_precision.clone(),
            self.config.model_schema_hash.clone(),
            self.config.artifact_record_format.clone(),
        )
        .with_head(head_id);
        if let Some(base_head_id) = base_head_id {
            spec = spec.with_base_head(base_head_id);
        }
        store
            .store_artifact_file(&spec, &staged_path, self.config.artifact_chunking)
            .map_err(Into::into)
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }

    fn contribution_weight(&self, report: &WindowReport<Self::WindowStats>) -> f64 {
        standard_contribution_weight(&report.stats).unwrap_or(1.0)
    }

    fn reconcile_canonical_model(
        &self,
        local_model: &Self::Model,
        canonical_model: Self::Model,
        strategy: TrainerCanonicalReconcileStrategy,
    ) -> anyhow::Result<Self::Model> {
        let canonical_model = canonical_model;
        let returned_id = self.client.reconcile_canonical_model(
            local_model.id(),
            canonical_model.id(),
            strategy,
        )?;
        debug_assert_eq!(returned_id, canonical_model.id());
        Ok(canonical_model)
    }

    fn merge_candidate_models(
        &self,
        base_model: &Self::Model,
        candidates: &[MergeModelCandidate<'_, Self::Model>],
        policy: MergePolicy,
    ) -> anyhow::Result<Option<Self::Model>> {
        let candidate_refs = candidates
            .iter()
            .map(|candidate| PythonMergeCandidateRef {
                peer_id: candidate.peer_id.as_str(),
                head_id: candidate.head_id.as_str(),
                artifact_id: candidate.artifact_id.as_str(),
                model_id: candidate.model.id(),
                sample_weight: candidate.sample_weight,
                quality_weight: candidate.quality_weight,
            })
            .collect::<Vec<_>>();
        let merged =
            self.client
                .merge_candidate_models(base_model.id(), &candidate_refs, policy)?;
        Ok(merged.map(|model_id| PythonModelHandle::new(model_id, self.client.clone())))
    }

    fn apply_single_root_ema(
        &self,
        base_model: &Self::Model,
        merged_model: Self::Model,
        policy: MergePolicy,
    ) -> anyhow::Result<Self::Model> {
        let merged_model = merged_model;
        let returned_id =
            self.client
                .apply_single_root_ema(base_model.id(), merged_model.id(), policy)?;
        debug_assert_eq!(returned_id, merged_model.id());
        Ok(merged_model)
    }

    fn supported_workload(&self) -> SupportedWorkload {
        self.config.supported_workload.clone()
    }

    fn model_schema_hash(&self) -> ContentId {
        self.config.model_schema_hash.clone()
    }
}
