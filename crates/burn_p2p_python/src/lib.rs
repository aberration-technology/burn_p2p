use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, ensure};
use burn_p2p_checkpoint::{ArtifactBuildSpec, ChunkingScheme, FsArtifactStore};
use burn_p2p_core::{
    ArtifactDescriptor, ArtifactKind, AssignmentLease, CapabilityEstimate, ContentId, DatasetId,
    DatasetManifest, DatasetView, DatasetViewId, FlattenedTensorPack, HeadId, MergePolicy,
    MetricValue, Precision, StateBlob, SupportedWorkload,
};
use burn_p2p_dataloader::{
    CachedMicroShard, DatasetRegistration, DatasetSizing, MicroShardPlan, MicroShardPlanner,
    MicroShardPlannerConfig, UpstreamAdapter,
};
use burn_p2p_experiment::{PatchSupport, RuntimePatch};
use burn_p2p_workload::{
    DiLoCoInnerLoopReport, DiLoCoWorkload, EvalSplit, LeaseDataPipeline,
    LeaseDataPipelineDescriptor, LeaseDataPipelineKind, MergeModelCandidate, MetricReport,
    P2pWorkload, PatchOutcome, TrainError, TrainerCanonicalReconcileStrategy, WindowCtx,
    WindowReport, local_upstream_root_for_pipeline, standard_contribution_weight,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod worker;

use worker::{
    PythonDiLoCoInnerLoopPathRequest, PythonDiLoCoInnerLoopResponse, PythonMergeCandidateRef,
    PythonWorkerClient,
};

const PYTHON_PARAMETER_PACK_FORMAT: &str = "burn-p2p-python-flattened-parameter-pack-v1";

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
    /// Optional DiLoCo bridge configuration. Artifact-window training is unchanged when disabled.
    pub diloco: PythonDiLoCoConfig,
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
            diloco: PythonDiLoCoConfig::disabled(),
        })
    }

    /// Enables DiLoCo through the long-lived Python worker.
    pub fn with_diloco_in_process(mut self) -> Self {
        self.diloco = PythonDiLoCoConfig::in_process_worker();
        self
    }

    /// Enables DiLoCo through an external checkpoint-command job.
    pub fn with_diloco_checkpoint_command(
        mut self,
        command: PythonCheckpointCommandConfig,
    ) -> Self {
        self.diloco = PythonDiLoCoConfig::checkpoint_command(command);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Configures the Python-side DiLoCo inner-loop bridge.
pub struct PythonDiLoCoConfig {
    /// Backend used to execute local DiLoCo inner loops.
    pub backend: PythonDiLoCoBackend,
    /// Rejects inner-loop reports that complete fewer or more steps than requested.
    pub require_exact_steps: bool,
}

impl PythonDiLoCoConfig {
    /// Disables the Python DiLoCo bridge.
    pub fn disabled() -> Self {
        Self {
            backend: PythonDiLoCoBackend::Disabled,
            require_exact_steps: true,
        }
    }

    /// Runs DiLoCo inner loops inside the already-running Python worker.
    pub fn in_process_worker() -> Self {
        Self {
            backend: PythonDiLoCoBackend::InProcessWorker,
            require_exact_steps: true,
        }
    }

    /// Runs DiLoCo inner loops by invoking one checkpoint-command job per round.
    pub fn checkpoint_command(command: PythonCheckpointCommandConfig) -> Self {
        Self {
            backend: PythonDiLoCoBackend::CheckpointCommand(command),
            require_exact_steps: true,
        }
    }
}

impl Default for PythonDiLoCoConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
/// Selects how Python-backed DiLoCo inner loops are executed.
pub enum PythonDiLoCoBackend {
    /// DiLoCo hooks are not available for this Python workload.
    Disabled,
    /// Uses a protocol-v2 method on the long-lived Python worker.
    InProcessWorker,
    /// Uses an external command that consumes a JSON job manifest and emits a checkpoint result.
    CheckpointCommand(PythonCheckpointCommandConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Command-mode DiLoCo adapter for downstream trainers that expose checkpoint I/O only.
pub struct PythonCheckpointCommandConfig {
    /// Program to execute for each DiLoCo inner-loop job.
    pub program: PathBuf,
    /// Arguments passed to the program. `{job_manifest}` and `{result_manifest}` are expanded.
    pub args: Vec<String>,
    /// Extra environment variables passed to the command.
    pub env: BTreeMap<String, String>,
    /// Extra module roots appended to the command `PYTHONPATH`.
    pub module_search_roots: Vec<PathBuf>,
}

impl PythonCheckpointCommandConfig {
    /// Creates a new checkpoint-command adapter.
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            module_search_roots: Vec::new(),
        }
    }

    /// Adds one command-line argument.
    pub fn with_arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Adds one Python import root for the command process.
    pub fn with_module_search_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.module_search_roots.push(root.into());
        self
    }

    /// Adds one environment override for the command process.
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PythonParameterPackManifest {
    format: String,
    model_schema_hash: String,
    layout_hash: String,
    parameter_count: usize,
    values_f32_le: String,
}

#[derive(Clone, Debug, Serialize)]
struct PythonDiLoCoCheckpointJob<'a> {
    protocol_version: u32,
    job_id: String,
    model_schema_hash: String,
    base_parameter_pack_path: String,
    output_parameter_pack_path: String,
    result_manifest_path: String,
    batches: &'a [PythonBatchRef],
    num_inner_steps: u32,
    require_exact_steps: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inner_optimizer_state_path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PythonDiLoCoCheckpointResult {
    steps_completed: u32,
    #[serde(default)]
    metrics: BTreeMap<String, MetricValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    local_parameter_pack_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inner_optimizer_state_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inner_optimizer_state_encoding: Option<String>,
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
        if hello.protocol_version == 0 || hello.protocol_version > 2 {
            anyhow::bail!(
                "python worker protocol mismatch: expected 1 or 2, got {}",
                hello.protocol_version
            );
        }
        if matches!(&config.diloco.backend, PythonDiLoCoBackend::InProcessWorker)
            && !hello
                .capabilities
                .iter()
                .any(|capability| capability == "diloco_checkpoint_job")
        {
            anyhow::bail!(
                "python worker does not advertise the diloco_checkpoint_job capability required by in-process DiLoCo"
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

    fn read_parameter_pack_dir(path: &Path) -> anyhow::Result<FlattenedTensorPack> {
        let manifest_path = path.join("manifest.json");
        let manifest: PythonParameterPackManifest =
            serde_json::from_slice(&fs::read(&manifest_path).with_context(|| {
                format!("read parameter-pack manifest {}", manifest_path.display())
            })?)
            .with_context(|| {
                format!("decode parameter-pack manifest {}", manifest_path.display())
            })?;
        ensure!(
            manifest.format == PYTHON_PARAMETER_PACK_FORMAT,
            "unsupported Python parameter-pack format {}",
            manifest.format
        );
        let values_path = path.join(&manifest.values_f32_le);
        let value_bytes = fs::read(&values_path)
            .with_context(|| format!("read parameter-pack values {}", values_path.display()))?;
        ensure!(
            value_bytes.len() == manifest.parameter_count * std::mem::size_of::<f32>(),
            "parameter-pack byte length {} does not match parameter count {}",
            value_bytes.len(),
            manifest.parameter_count
        );
        let mut values = Vec::with_capacity(manifest.parameter_count);
        for chunk in value_bytes.chunks_exact(std::mem::size_of::<f32>()) {
            values.push(f32::from_le_bytes(
                chunk.try_into().expect("chunk has 4 bytes"),
            ));
        }
        Ok(FlattenedTensorPack::new(
            ContentId::new(manifest.model_schema_hash),
            ContentId::new(manifest.layout_hash),
            values,
        ))
    }

    fn write_parameter_pack_dir(path: &Path, pack: &FlattenedTensorPack) -> anyhow::Result<()> {
        fs::create_dir_all(path)
            .with_context(|| format!("create parameter-pack dir {}", path.display()))?;
        let values_path = path.join("values.f32le");
        let mut value_bytes = Vec::with_capacity(pack.values.len() * std::mem::size_of::<f32>());
        for value in &pack.values {
            value_bytes.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&values_path, value_bytes)
            .with_context(|| format!("write parameter-pack values {}", values_path.display()))?;
        let manifest = PythonParameterPackManifest {
            format: PYTHON_PARAMETER_PACK_FORMAT.to_owned(),
            model_schema_hash: pack.model_schema_hash.as_str().to_owned(),
            layout_hash: pack.layout_hash.as_str().to_owned(),
            parameter_count: pack.parameter_count(),
            values_f32_le: "values.f32le".into(),
        };
        fs::write(
            path.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )
        .with_context(|| format!("write parameter-pack manifest {}", path.display()))?;
        Ok(())
    }

    fn write_state_blob_file(path: &Path, state: &StateBlob) -> anyhow::Result<()> {
        fs::write(path, &state.bytes)
            .with_context(|| format!("write optimizer state blob {}", path.display()))
    }

    fn read_state_blob_file(path: &Path, encoding: impl Into<String>) -> anyhow::Result<StateBlob> {
        StateBlob::try_new(
            encoding,
            fs::read(path)
                .with_context(|| format!("read optimizer state blob {}", path.display()))?,
        )
        .map_err(anyhow::Error::from)
    }

    fn validate_diloco_steps(
        &self,
        requested: u32,
        response: &PythonDiLoCoInnerLoopResponse,
    ) -> Result<(), TrainError> {
        if self.config.diloco.require_exact_steps && response.steps_completed != requested {
            return Err(TrainError::new(format!(
                "Python DiLoCo inner loop completed {} step(s), requested {}",
                response.steps_completed, requested
            )));
        }
        Ok(())
    }

    fn run_checkpoint_command(
        &self,
        command_config: &PythonCheckpointCommandConfig,
        job_manifest_path: &Path,
        result_manifest_path: &Path,
    ) -> anyhow::Result<()> {
        let runtime_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python");
        let mut pythonpath_entries = vec![runtime_root];
        pythonpath_entries.extend(self.config.runtime.module_search_roots.iter().cloned());
        pythonpath_entries.extend(command_config.module_search_roots.iter().cloned());
        let pythonpath = worker::join_pythonpath_for_command(&pythonpath_entries)?;

        let mut command = Command::new(&command_config.program);
        for arg in &command_config.args {
            command.arg(
                arg.replace(
                    "{job_manifest}",
                    job_manifest_path.to_string_lossy().as_ref(),
                )
                .replace(
                    "{result_manifest}",
                    result_manifest_path.to_string_lossy().as_ref(),
                ),
            );
        }
        command
            .env("BURN_P2P_DILOCO_JOB_MANIFEST", job_manifest_path)
            .env("BURN_P2P_DILOCO_RESULT_MANIFEST", result_manifest_path);
        if let Some(path) = pythonpath {
            command.env("PYTHONPATH", path);
        }
        for (key, value) in &self.config.runtime.env {
            command.env(key, value);
        }
        for (key, value) in &command_config.env {
            command.env(key, value);
        }
        let status = command.status().with_context(|| {
            format!(
                "spawn Python DiLoCo checkpoint command {:?}",
                command_config.program
            )
        })?;
        ensure!(
            status.success(),
            "Python DiLoCo checkpoint command exited with {status}"
        );
        Ok(())
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

impl DiLoCoWorkload for PythonTorchProject {
    fn export_parameter_pack(&self, model: &Self::Model) -> anyhow::Result<FlattenedTensorPack> {
        let staged_dir = tempfile::Builder::new()
            .prefix("burn-p2p-python-export-pack")
            .tempdir()?;
        let pack_path = staged_dir.path().join("parameters");
        self.client
            .export_parameter_pack_path(model.id(), &pack_path, &self.config.model_schema_hash)
            .context("export Python model parameter pack")?;
        let pack = Self::read_parameter_pack_dir(&pack_path)?;
        ensure!(
            pack.model_schema_hash == self.config.model_schema_hash,
            "Python exported model schema {}, expected {}",
            pack.model_schema_hash.as_str(),
            self.config.model_schema_hash.as_str()
        );
        Ok(pack)
    }

    fn import_parameter_pack(
        &self,
        device: &Self::Device,
        pack: &FlattenedTensorPack,
    ) -> anyhow::Result<Self::Model> {
        ensure!(
            pack.model_schema_hash == self.config.model_schema_hash,
            "cannot import Python parameter pack for schema {}, expected {}",
            pack.model_schema_hash.as_str(),
            self.config.model_schema_hash.as_str()
        );
        let staged_dir = tempfile::Builder::new()
            .prefix("burn-p2p-python-import-pack")
            .tempdir()?;
        let pack_path = staged_dir.path().join("parameters");
        Self::write_parameter_pack_dir(&pack_path, pack)?;
        let model_id = self
            .client
            .import_parameter_pack_path(device, &pack_path)
            .context("import Python model parameter pack")?;
        Ok(PythonModelHandle::new(model_id, self.client.clone()))
    }

    fn run_inner_steps(
        &self,
        model: &Self::Model,
        batches: &[Self::Batch],
        num_inner_steps: u32,
        inner_optimizer_state: Option<&StateBlob>,
    ) -> Result<DiLoCoInnerLoopReport, TrainError> {
        if num_inner_steps > 0 && batches.is_empty() {
            return Err(TrainError::new(
                "Python DiLoCo inner loop requires at least one batch",
            ));
        }

        match &self.config.diloco.backend {
            PythonDiLoCoBackend::Disabled => Err(TrainError::new(
                "Python DiLoCo bridge is disabled for this workload",
            )),
            PythonDiLoCoBackend::InProcessWorker => {
                self.run_worker_inner_loop(model, batches, num_inner_steps, inner_optimizer_state)
            }
            PythonDiLoCoBackend::CheckpointCommand(command) => self.run_command_inner_loop(
                command,
                model,
                batches,
                num_inner_steps,
                inner_optimizer_state,
            ),
        }
    }
}

impl PythonTorchProject {
    fn run_worker_inner_loop(
        &self,
        model: &PythonModelHandle,
        batches: &[PythonBatchRef],
        num_inner_steps: u32,
        inner_optimizer_state: Option<&StateBlob>,
    ) -> Result<DiLoCoInnerLoopReport, TrainError> {
        let staged_dir = tempfile::Builder::new()
            .prefix("burn-p2p-python-diloco-worker")
            .tempdir()
            .map_err(|error| TrainError::new(error.to_string()))?;
        let output_pack_path = staged_dir.path().join("local-parameters");
        let inner_state_path = if let Some(state) = inner_optimizer_state {
            let path = staged_dir.path().join("inner-state-input.blob");
            Self::write_state_blob_file(&path, state)
                .map_err(|error| TrainError::new(error.to_string()))?;
            Some(path)
        } else {
            None
        };
        let response = self
            .client
            .run_inner_loop_path(PythonDiLoCoInnerLoopPathRequest {
                model_id: model.id(),
                batches,
                num_inner_steps,
                inner_optimizer_state_path: inner_state_path.as_deref(),
                output_parameter_pack_path: &output_pack_path,
                model_schema_hash: &self.config.model_schema_hash,
                require_exact_steps: self.config.diloco.require_exact_steps,
            })
            .map_err(|error| TrainError::new(error.to_string()))?;
        self.validate_diloco_steps(num_inner_steps, &response)?;
        let local_parameters = Self::read_parameter_pack_dir(&output_pack_path)
            .map_err(|error| TrainError::new(error.to_string()))?;
        let inner_optimizer_state = response
            .inner_optimizer_state_path
            .as_ref()
            .map(|path| {
                Self::read_state_blob_file(
                    path,
                    response
                        .inner_optimizer_state_encoding
                        .clone()
                        .unwrap_or_else(|| "application/octet-stream".into()),
                )
            })
            .transpose()
            .map_err(|error| TrainError::new(error.to_string()))?;
        Ok(DiLoCoInnerLoopReport {
            local_parameters,
            inner_optimizer_state,
            steps_completed: response.steps_completed,
            metrics: response.metrics,
        })
    }

    fn run_command_inner_loop(
        &self,
        command_config: &PythonCheckpointCommandConfig,
        model: &PythonModelHandle,
        batches: &[PythonBatchRef],
        num_inner_steps: u32,
        inner_optimizer_state: Option<&StateBlob>,
    ) -> Result<DiLoCoInnerLoopReport, TrainError> {
        let staged_dir = tempfile::Builder::new()
            .prefix("burn-p2p-python-diloco-command")
            .tempdir()
            .map_err(|error| TrainError::new(error.to_string()))?;
        let base_pack_path = staged_dir.path().join("base-parameters");
        let output_pack_path = staged_dir.path().join("local-parameters");
        let result_manifest_path = staged_dir.path().join("result.json");
        let job_manifest_path = staged_dir.path().join("job.json");

        self.client
            .export_parameter_pack_path(model.id(), &base_pack_path, &self.config.model_schema_hash)
            .map_err(|error| TrainError::new(error.to_string()))?;
        let inner_state_path = if let Some(state) = inner_optimizer_state {
            let path = staged_dir.path().join("inner-state-input.blob");
            Self::write_state_blob_file(&path, state)
                .map_err(|error| TrainError::new(error.to_string()))?;
            Some(path)
        } else {
            None
        };
        let job = PythonDiLoCoCheckpointJob {
            protocol_version: 1,
            job_id: format!(
                "diloco-inner-{}",
                Utc::now().timestamp_nanos_opt().unwrap_or(0)
            ),
            model_schema_hash: self.config.model_schema_hash.as_str().to_owned(),
            base_parameter_pack_path: base_pack_path.to_string_lossy().into_owned(),
            output_parameter_pack_path: output_pack_path.to_string_lossy().into_owned(),
            result_manifest_path: result_manifest_path.to_string_lossy().into_owned(),
            batches,
            num_inner_steps,
            require_exact_steps: self.config.diloco.require_exact_steps,
            inner_optimizer_state_path: inner_state_path
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned()),
        };
        fs::write(
            &job_manifest_path,
            serde_json::to_vec_pretty(&job).map_err(|error| {
                TrainError::new(format!("serialize Python DiLoCo command job: {error}"))
            })?,
        )
        .map_err(|error| TrainError::new(error.to_string()))?;

        self.run_checkpoint_command(command_config, &job_manifest_path, &result_manifest_path)
            .map_err(|error| TrainError::new(error.to_string()))?;
        let result: PythonDiLoCoCheckpointResult =
            serde_json::from_slice(&fs::read(&result_manifest_path).map_err(|error| {
                TrainError::new(format!(
                    "read Python DiLoCo command result {}: {error}",
                    result_manifest_path.display()
                ))
            })?)
            .map_err(|error| TrainError::new(format!("decode Python DiLoCo result: {error}")))?;
        let response = PythonDiLoCoInnerLoopResponse {
            steps_completed: result.steps_completed,
            metrics: result.metrics,
            inner_optimizer_state_path: result.inner_optimizer_state_path.map(PathBuf::from),
            inner_optimizer_state_encoding: result.inner_optimizer_state_encoding,
        };
        self.validate_diloco_steps(num_inner_steps, &response)?;
        let local_pack_path = result
            .local_parameter_pack_path
            .map(PathBuf::from)
            .unwrap_or(output_pack_path);
        let local_parameters = Self::read_parameter_pack_dir(&local_pack_path)
            .map_err(|error| TrainError::new(error.to_string()))?;
        let inner_optimizer_state = response
            .inner_optimizer_state_path
            .as_ref()
            .map(|path| {
                Self::read_state_blob_file(
                    path,
                    response
                        .inner_optimizer_state_encoding
                        .clone()
                        .unwrap_or_else(|| "application/octet-stream".into()),
                )
            })
            .transpose()
            .map_err(|error| TrainError::new(error.to_string()))?;

        Ok(DiLoCoInnerLoopReport {
            local_parameters,
            inner_optimizer_state,
            steps_completed: response.steps_completed,
            metrics: response.metrics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_diloco_config_defaults_to_disabled_exact_steps() {
        let config = PythonDiLoCoConfig::default();
        assert!(matches!(config.backend, PythonDiLoCoBackend::Disabled));
        assert!(config.require_exact_steps);

        let command = PythonCheckpointCommandConfig::new("python3").with_arg("trainer.py");
        let config = PythonDiLoCoConfig::checkpoint_command(command);
        assert!(matches!(
            config.backend,
            PythonDiLoCoBackend::CheckpointCommand(_)
        ));
        assert!(config.require_exact_steps);
    }

    #[test]
    fn parameter_pack_sidecar_round_trips_flattened_values() {
        let temp = tempfile::tempdir().expect("tempdir");
        let pack = FlattenedTensorPack::new(
            ContentId::new("schema-a"),
            ContentId::new("layout-a"),
            vec![0.25, -1.5, 3.0, 8.25],
        );
        PythonTorchProject::write_parameter_pack_dir(temp.path(), &pack).expect("write pack");
        let decoded = PythonTorchProject::read_parameter_pack_dir(temp.path()).expect("read pack");
        assert_eq!(decoded, pack);
    }
}
