use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::Context;
use burn_p2p::{
    ArtifactBuildSpec, ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard,
    CapabilityEstimate, ChunkingScheme, DatasetRegistration, DatasetSizing, EvalSplit,
    FsArtifactStore, GenesisSpec, HeadDescriptor, MetricReport, MetricValue, MicroShardPlanner,
    MicroShardPlannerConfig, NodeBuilder, P2pWorkload, PatchOutcome, PatchSupport, PeerRole,
    PeerRoleSet, RuntimePatch, ShardFetchManifest, StorageConfig, SwarmAddress, TrainError,
    UpstreamAdapter, WindowCtx, WindowReport,
};
use burn_p2p_core::{HeadEvalReport, PeerWindowMetrics, ReducerCohortMetrics};
use burn_p2p_metrics::{NetworkPerformanceSummary, derive_network_performance_summary};
use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Serialize};

const OPTIONAL_CANONICAL_ADVANCE_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(feature = "native-gpu-probe")]
use burn::{
    backend::{Autodiff, Cuda, NdArray, Wgpu, wgpu},
    data::dataloader::batcher::Batcher,
    module::Module,
    nn::{Linear, LinearConfig},
    optim::SgdConfig,
    tensor::{
        ElementConversion, Tensor, TensorData,
        backend::{AutodiffBackend, Backend},
    },
    train::{InferenceStep, ItemLazy, TrainOutput, TrainStep},
};
#[cfg(feature = "native-gpu-probe")]
use burn_p2p::burn::{
    BurnShardedDataset, BurnShardedDatasetConfig, BurnTrainBatch, BurnTrainOutput, inspect_module,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported synthetic workload kinds.
pub enum SyntheticWorkloadKind {
    /// Uses the original scalar placeholder runtime workload.
    #[default]
    Scalar,
    /// Uses the Burn-backed linear learner workload.
    BurnLinear,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported native backends for Burn-backed synthetic nodes.
pub enum SyntheticNativeBackend {
    /// Leaves backend resolution to the caller; the runtime falls back to CPU.
    Auto,
    /// Uses the CPU ndarray backend.
    #[default]
    NdArray,
    /// Uses the native WGPU backend.
    Wgpu,
    /// Uses the native CUDA backend.
    Cuda,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported synthetic process modes.
pub enum SyntheticProcessMode {
    /// Runs in validator mode.
    Validator,
    /// Allows trainer behavior.
    Trainer,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a synthetic experiment scope.
pub struct SyntheticExperimentScope {
    /// The network ID.
    pub network_id: String,
    /// The study ID.
    pub study_id: String,
    /// The experiment ID.
    pub experiment_id: String,
    /// The revision ID.
    pub revision_id: String,
}

impl Default for SyntheticExperimentScope {
    fn default() -> Self {
        Self {
            network_id: "testkit-native".into(),
            study_id: "synthetic-study".into(),
            experiment_id: "synthetic-exp".into(),
            revision_id: "rev-1".into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures synthetic process.
pub struct SyntheticProcessConfig {
    /// The mode.
    pub mode: SyntheticProcessMode,
    /// The synthetic workload kind to execute.
    #[serde(default)]
    pub workload_kind: SyntheticWorkloadKind,
    /// The preferred native backend when using a Burn-backed workload.
    #[serde(default)]
    pub native_backend: SyntheticNativeBackend,
    /// The scope.
    pub scope: SyntheticExperimentScope,
    /// The storage root.
    pub storage_root: PathBuf,
    /// The dataset root.
    pub dataset_root: PathBuf,
    /// The report path.
    pub report_path: PathBuf,
    /// The start sentinel.
    pub start_sentinel: Option<PathBuf>,
    /// The shutdown sentinel.
    pub shutdown_sentinel: Option<PathBuf>,
    /// The bootstrap peers.
    pub bootstrap_peers: Vec<SwarmAddress>,
    /// The listen addresses.
    pub listen_addresses: Vec<SwarmAddress>,
    /// The persist identity.
    pub persist_identity: bool,
    /// The learning rate.
    pub learning_rate: f64,
    /// The target model.
    pub target_model: f64,
    /// The startup timeout secs.
    pub startup_timeout_secs: u64,
    /// The poll interval ms.
    pub poll_interval_ms: u64,
    /// The sync timeout secs.
    pub sync_timeout_secs: u64,
    /// The merge wait timeout secs.
    pub merge_wait_timeout_secs: u64,
    /// Whether a trainer process must observe the canonical head advance after publishing.
    pub wait_for_canonical_advance: bool,
    /// Whether failure to observe a canonical advance should fail the process.
    #[serde(default = "default_canonical_advance_required")]
    pub canonical_advance_required: bool,
    /// Whether trainer processes should use the speculative continuous trainer API.
    #[serde(default)]
    pub continuous_training: bool,
    /// The window count.
    pub window_count: u32,
}

impl SyntheticProcessConfig {
    /// Performs the validator operation.
    pub fn validator(
        storage_root: impl Into<PathBuf>,
        dataset_root: impl Into<PathBuf>,
        report_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            mode: SyntheticProcessMode::Validator,
            workload_kind: SyntheticWorkloadKind::Scalar,
            native_backend: SyntheticNativeBackend::NdArray,
            scope: SyntheticExperimentScope::default(),
            storage_root: storage_root.into(),
            dataset_root: dataset_root.into(),
            report_path: report_path.into(),
            start_sentinel: None,
            shutdown_sentinel: None,
            bootstrap_peers: Vec::new(),
            listen_addresses: Vec::new(),
            persist_identity: true,
            learning_rate: 1.0,
            target_model: 100.0,
            startup_timeout_secs: 15,
            poll_interval_ms: 50,
            sync_timeout_secs: 15,
            merge_wait_timeout_secs: 15,
            wait_for_canonical_advance: true,
            canonical_advance_required: true,
            continuous_training: false,
            window_count: 1,
        }
    }

    /// Performs the trainer operation.
    pub fn trainer(
        storage_root: impl Into<PathBuf>,
        dataset_root: impl Into<PathBuf>,
        report_path: impl Into<PathBuf>,
        bootstrap_peers: Vec<SwarmAddress>,
    ) -> Self {
        Self {
            mode: SyntheticProcessMode::Trainer,
            workload_kind: SyntheticWorkloadKind::Scalar,
            native_backend: SyntheticNativeBackend::NdArray,
            scope: SyntheticExperimentScope::default(),
            storage_root: storage_root.into(),
            dataset_root: dataset_root.into(),
            report_path: report_path.into(),
            start_sentinel: None,
            shutdown_sentinel: None,
            bootstrap_peers,
            listen_addresses: Vec::new(),
            persist_identity: false,
            learning_rate: 1.0,
            target_model: 100.0,
            startup_timeout_secs: 15,
            poll_interval_ms: 50,
            sync_timeout_secs: 15,
            merge_wait_timeout_secs: 15,
            wait_for_canonical_advance: true,
            canonical_advance_required: true,
            continuous_training: false,
            window_count: 1,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Reports synthetic process details.
pub struct SyntheticProcessReport {
    /// The status.
    pub status: String,
    /// The local peer ID.
    pub local_peer_id: Option<String>,
    /// The listen addresses.
    pub listen_addresses: Vec<String>,
    /// The initialized head ID.
    pub initialized_head_id: Option<String>,
    /// The synced head ID.
    pub synced_head_id: Option<String>,
    /// The trained head ID.
    pub trained_head_id: Option<String>,
    /// The trained parent head ID.
    pub trained_parent_head_id: Option<String>,
    /// The observed canonical head ID.
    pub observed_canonical_head_id: Option<String>,
    /// The merged head ID.
    pub merged_head_id: Option<String>,
    /// The receipt count.
    pub receipt_count: usize,
    /// The merge count.
    pub merge_count: usize,
    /// When the process report was first created.
    pub started_at: Option<DateTime<Utc>>,
    /// When the runtime first became ready.
    pub runtime_ready_at: Option<DateTime<Utc>>,
    /// When the process completed or failed.
    pub completed_at: Option<DateTime<Utc>>,
    /// Per-window timing checkpoints observed by the process.
    #[serde(default)]
    pub window_timelines: Vec<SyntheticWindowTimeline>,
    /// Connected peer count from the latest runtime telemetry snapshot.
    pub connected_peer_count: usize,
    /// Matching experiment head announcements visible from the latest runtime snapshot.
    pub visible_experiment_head_count: usize,
    /// Matching experiment update announcements visible from the latest runtime snapshot.
    pub visible_experiment_update_count: usize,
    /// The error.
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// One per-window timing record captured by the synthetic process harness.
pub struct SyntheticWindowTimeline {
    /// Zero-based window index within the process.
    pub window_index: u32,
    /// When the process first synced the canonical head for the window.
    pub synced_at: Option<DateTime<Utc>>,
    /// When training finished for the window.
    pub trained_at: Option<DateTime<Utc>>,
    /// When the process observed the canonical head advance after training.
    pub canonical_advanced_at: Option<DateTime<Utc>>,
    /// Whether canonical observation timed out for the completed window.
    #[serde(default)]
    pub canonical_observation_timed_out: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Percentile-style distribution summary for one timing family.
pub struct SyntheticTimingDistributionSummary {
    /// Number of samples that contributed to the distribution.
    pub sample_count: usize,
    /// Mean latency across the observed samples.
    pub mean_ms: u64,
    /// P50 latency.
    pub p50_ms: u64,
    /// P90 latency.
    pub p90_ms: u64,
    /// Maximum latency.
    pub max_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
/// Network-dynamics summary for one synthetic soak run.
pub struct SyntheticNetworkDynamicsSummary {
    /// Certified merge rate across the soak wall time.
    pub merge_rate_per_sec: f64,
    /// Accepted receipt rate across the soak wall time.
    pub receipt_rate_per_sec: f64,
    /// Startup latency from process launch until runtime ready.
    pub peer_startup_latency_ms: SyntheticTimingDistributionSummary,
    /// Time from runtime ready until the first synced canonical head on trainers.
    pub trainer_sync_latency_ms: SyntheticTimingDistributionSummary,
    /// Time from a synced head until the trained candidate head was produced.
    pub trainer_training_latency_ms: SyntheticTimingDistributionSummary,
    /// Time from trained candidate publication until canonical-head advance.
    pub trainer_canonical_advance_latency_ms: SyntheticTimingDistributionSummary,
    /// Fraction of training elapsed time spent in active windows.
    pub training_active_ratio: f64,
    /// Fraction of training elapsed time spent idle between windows.
    pub training_idle_ratio: f64,
    /// Fraction of validation elapsed time spent in active windows.
    pub validation_active_ratio: f64,
    /// Fraction of validation elapsed time spent idle between windows.
    pub validation_idle_ratio: f64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures synthetic soak.
pub struct SyntheticSoakConfig {
    /// The root.
    pub root: PathBuf,
    /// The synthetic workload kind to execute.
    #[serde(default)]
    pub workload_kind: SyntheticWorkloadKind,
    /// The backend requested for trainer processes when using a Burn-backed workload.
    #[serde(default)]
    pub trainer_backend: SyntheticNativeBackend,
    /// The backend requested for validator processes when using a Burn-backed workload.
    #[serde(default)]
    pub validator_backend: SyntheticNativeBackend,
    /// The trainer count.
    pub trainer_count: u32,
    /// The trainer window count.
    pub trainer_window_count: u32,
    /// Whether trainers should stay alive across all windows instead of being
    /// respawned every round.
    #[serde(default)]
    pub persistent_trainers: bool,
    /// Whether trainer processes should use the speculative continuous trainer API.
    #[serde(default)]
    pub continuous_training: bool,
    /// The startup timeout secs.
    pub startup_timeout_secs: u64,
    /// The poll interval ms.
    pub poll_interval_ms: u64,
    /// The sync timeout secs.
    pub sync_timeout_secs: u64,
    /// The merge wait timeout secs.
    pub merge_wait_timeout_secs: u64,
    /// Whether each trainer must fail when it cannot directly observe canonical advancement.
    #[serde(default = "default_canonical_advance_required")]
    pub canonical_advance_required: bool,
}

impl Default for SyntheticSoakConfig {
    fn default() -> Self {
        Self {
            root: std::env::temp_dir().join(format!(
                "burn-p2p-soak-{}",
                Utc::now().timestamp_nanos_opt().unwrap_or_default()
            )),
            workload_kind: SyntheticWorkloadKind::Scalar,
            trainer_backend: SyntheticNativeBackend::NdArray,
            validator_backend: SyntheticNativeBackend::NdArray,
            trainer_count: 4,
            trainer_window_count: 3,
            persistent_trainers: false,
            continuous_training: false,
            startup_timeout_secs: 15,
            poll_interval_ms: 50,
            sync_timeout_secs: 15,
            merge_wait_timeout_secs: 15,
            canonical_advance_required: true,
        }
    }
}

fn default_canonical_advance_required() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Summarizes the synthetic soak.
pub struct SyntheticSoakSummary {
    /// The synthetic workload kind used during the soak.
    pub workload_kind: SyntheticWorkloadKind,
    /// The backend requested for trainer processes during the soak.
    pub trainer_backend: SyntheticNativeBackend,
    /// The backend requested for validator processes during the soak.
    pub validator_backend: SyntheticNativeBackend,
    /// The validator report.
    pub validator_report: SyntheticProcessReport,
    /// The trainer reports.
    pub trainer_reports: Vec<SyntheticProcessReport>,
    /// The trainer count.
    pub trainer_count: u32,
    /// The trainer window count.
    pub trainer_window_count: u32,
    /// Whether trainer processes used speculative continuous training.
    pub continuous_training: bool,
    /// The completed rounds.
    pub completed_rounds: u32,
    /// The trainer process count.
    pub trainer_process_count: usize,
    /// The total receipts.
    pub total_receipts: usize,
    /// The total merges.
    pub total_merges: usize,
    /// The elapsed millis.
    pub elapsed_millis: u128,
    /// Optional runtime-derived performance summary from persisted metric records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub performance_summary: Option<NetworkPerformanceSummary>,
    /// Optional higher-level dynamics rollup for startup, sync, and convergence timing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamics_summary: Option<SyntheticNetworkDynamicsSummary>,
    /// The root.
    pub root: PathBuf,
}

#[derive(Clone, Debug)]
struct SyntheticRuntimeProject {
    dataset_root: PathBuf,
    learning_rate: f64,
    target_model: f64,
}

impl P2pWorkload for SyntheticRuntimeProject {
    type Device = String;
    type Model = f64;
    type Batch = f64;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, _device: &String) -> Self::Model {
        0.0
    }

    fn benchmark(&self, _model: &Self::Model, _device: &String) -> CapabilityEstimate {
        CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 64.0,
            target_window_seconds: 1,
        }
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<String, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        let delta = ctx.batches.iter().copied().sum::<f64>() * self.learning_rate;
        ctx.model += delta;

        Ok(WindowReport {
            contribution: None,
            stats: BTreeMap::from([
                ("delta".into(), MetricValue::Float(delta)),
                ("model".into(), MetricValue::Float(ctx.model)),
                (
                    "loss".into(),
                    MetricValue::Float((self.target_model - ctx.model).abs()),
                ),
            ]),
            completed_at: Utc::now(),
        })
    }

    fn evaluate(&self, model: &Self::Model, _split: EvalSplit) -> MetricReport {
        MetricReport {
            metrics: BTreeMap::from([
                (
                    "loss".into(),
                    MetricValue::Float((self.target_model - *model).abs()),
                ),
                ("model".into(), MetricValue::Float(*model)),
            ]),
            captured_at: Utc::now(),
        }
    }

    fn apply_patch(&mut self, patch: &RuntimePatch) -> PatchOutcome {
        if let Some(burn_p2p::PatchValue::Float(value)) = patch.values.get("learning_rate") {
            self.learning_rate = *value;
            PatchOutcome::Applied
        } else {
            PatchOutcome::Rejected("missing learning_rate patch".into())
        }
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        PatchSupport {
            hot: true,
            warm: false,
            cold: false,
        }
    }

    fn runtime_device(&self) -> String {
        "cpu".into()
    }

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        Ok(DatasetRegistration {
            manifest: burn_p2p::DatasetManifest {
                dataset_id: burn_p2p::DatasetId::new("dataset"),
                source_uri: self.dataset_root.display().to_string(),
                format: "microshards".into(),
                manifest_hash: burn_p2p::ContentId::new("manifest"),
                metadata: BTreeMap::new(),
            },
            view: burn_p2p::DatasetView {
                dataset_view_id: burn_p2p::DatasetViewId::new("dataset-view"),
                dataset_id: burn_p2p::DatasetId::new("dataset"),
                preprocessing_hash: burn_p2p::ContentId::new("preprocess"),
                tokenizer_hash: None,
                manifest_hash: burn_p2p::ContentId::new("manifest"),
                metadata: BTreeMap::new(),
            },
            upstream: UpstreamAdapter::Local {
                root: self.dataset_root.display().to_string(),
            },
        })
    }

    fn microshard_plan(
        &self,
        registration: &DatasetRegistration,
    ) -> anyhow::Result<burn_p2p::MicroShardPlan> {
        Ok(MicroShardPlanner::new(MicroShardPlannerConfig {
            target_microshard_bytes: 10,
            min_microshards: 2,
            max_microshards: 2,
        })?
        .plan(
            &registration.view,
            DatasetSizing {
                total_examples: 2,
                total_tokens: 2,
                total_bytes: 20,
            },
        )?)
    }

    fn load_batches(
        &self,
        _lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        cached_microshards
            .iter()
            .map(|shard| {
                let bytes = fs::read(&shard.path)?;
                let text = String::from_utf8(bytes)?;
                text.trim().parse::<f64>().map_err(anyhow::Error::from)
            })
            .collect()
    }

    fn load_model_artifact(
        &self,
        _model: Self::Model,
        descriptor: &ArtifactDescriptor,
        store: &FsArtifactStore,
        _device: &String,
    ) -> anyhow::Result<Self::Model> {
        Ok(serde_json::from_slice(
            &store.materialize_artifact_bytes(descriptor)?,
        )?)
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: ArtifactKind,
        head_id: burn_p2p::HeadId,
        base_head_id: Option<burn_p2p::HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let mut spec = ArtifactBuildSpec::new(
            artifact_kind,
            burn_p2p::Precision::Fp32,
            burn_p2p::ContentId::new("synthetic-schema"),
            "synthetic-json",
        )
        .with_head(head_id);
        if let Some(base_head_id) = base_head_id {
            spec = spec.with_base_head(base_head_id);
        }
        let bytes = serde_json::to_vec(model)?;
        Ok(store.store_artifact_reader(
            &spec,
            std::io::Cursor::new(bytes),
            ChunkingScheme::new(16)?,
        )?)
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }

    fn merge_candidate_models(
        &self,
        base_model: &Self::Model,
        candidates: &[burn_p2p::MergeModelCandidate<'_, Self::Model>],
        policy: burn_p2p::MergePolicy,
    ) -> anyhow::Result<Option<Self::Model>> {
        if candidates.is_empty() {
            return Ok(None);
        }
        let (weighted_sum, total_weight) = candidates.iter().fold(
            (0.0_f64, 0.0_f64),
            |(weighted_sum, total_weight), candidate| {
                let quality = match policy {
                    burn_p2p::MergePolicy::WeightedMean => 1.0,
                    burn_p2p::MergePolicy::NormClippedWeightedMean => {
                        candidate.quality_weight.clamp(0.0, 1.0)
                    }
                    burn_p2p::MergePolicy::TrimmedMean => candidate.quality_weight,
                    burn_p2p::MergePolicy::Ema | burn_p2p::MergePolicy::QualityWeightedEma => {
                        candidate.quality_weight
                    }
                    burn_p2p::MergePolicy::Custom(_) => candidate.quality_weight,
                };
                let weight = candidate.sample_weight * quality;
                (
                    weighted_sum + (*candidate.model * weight),
                    total_weight + weight,
                )
            },
        );
        if total_weight <= f64::EPSILON {
            return Ok(Some(*base_model));
        }
        Ok(Some(weighted_sum / total_weight))
    }

    fn supported_workload(&self) -> burn_p2p::SupportedWorkload {
        burn_p2p::SupportedWorkload {
            workload_id: burn_p2p::WorkloadId::new("synthetic-runtime"),
            workload_name: "Synthetic Runtime".into(),
            model_program_hash: burn_p2p::ContentId::new("synthetic-program"),
            checkpoint_format_hash: burn_p2p::ContentId::new("synthetic-json"),
            supported_revision_family: burn_p2p::ContentId::new("synthetic-revision-family"),
            resource_class: "cpu".into(),
        }
    }

    fn model_schema_hash(&self) -> burn_p2p::ContentId {
        burn_p2p::ContentId::new("synthetic-schema")
    }
}

#[cfg(feature = "native-gpu-probe")]
type SyntheticBurnNdArrayBackend = Autodiff<NdArray<f32>>;
#[cfg(feature = "native-gpu-probe")]
type SyntheticBurnWgpuBackend = Autodiff<Wgpu>;
#[cfg(feature = "native-gpu-probe")]
type SyntheticBurnCudaBackend = Autodiff<Cuda>;
#[cfg(feature = "native-gpu-probe")]
type SyntheticBurnEvalBackend<B> = <B as AutodiffBackend>::InnerBackend;
#[cfg(feature = "native-gpu-probe")]
type SyntheticBurnOptimizer<B> = burn::optim::adaptor::OptimizerAdaptor<
    burn::optim::Sgd<<B as AutodiffBackend>::InnerBackend>,
    SyntheticBurnModel<B>,
    B,
>;

#[cfg(feature = "native-gpu-probe")]
#[derive(Module, Debug)]
struct SyntheticBurnModel<B: Backend> {
    output: Linear<B>,
}

#[cfg(feature = "native-gpu-probe")]
impl<B: Backend> SyntheticBurnModel<B> {
    fn new(device: &B::Device) -> Self {
        Self {
            output: LinearConfig::new(1, 1).init(device),
        }
    }

    fn forward(&self, inputs: Tensor<B, 2>) -> Tensor<B, 2> {
        self.output.forward(inputs)
    }
}

#[cfg(feature = "native-gpu-probe")]
#[derive(Clone, Debug)]
struct SyntheticBurnBatch<B: Backend> {
    inputs: Tensor<B, 2>,
    targets: Tensor<B, 2>,
}

#[cfg(feature = "native-gpu-probe")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct SyntheticBurnRecord {
    value: f32,
}

#[cfg(feature = "native-gpu-probe")]
#[derive(Clone, Debug, Default)]
struct SyntheticBurnBatcher;

#[cfg(feature = "native-gpu-probe")]
impl<B: Backend> Batcher<B, SyntheticBurnRecord, SyntheticBurnBatch<B>> for SyntheticBurnBatcher {
    fn batch(&self, items: Vec<SyntheticBurnRecord>, device: &B::Device) -> SyntheticBurnBatch<B> {
        let mut inputs = Vec::with_capacity(items.len());
        let mut targets = Vec::with_capacity(items.len());
        for item in items {
            let value = item.value;
            inputs.push(value);
            targets.push(value * 2.0 + 1.0);
        }
        let batch_len = targets.len();
        SyntheticBurnBatch {
            inputs: Tensor::from_data(TensorData::new(inputs, [batch_len, 1]), device),
            targets: Tensor::from_data(TensorData::new(targets, [batch_len, 1]), device),
        }
    }
}

#[cfg(feature = "native-gpu-probe")]
#[derive(Clone, Debug)]
struct SyntheticBurnMetric {
    loss: f64,
}

#[cfg(feature = "native-gpu-probe")]
impl ItemLazy for SyntheticBurnMetric {
    type ItemSync = Self;

    fn sync(self) -> Self::ItemSync {
        self
    }
}

#[cfg(feature = "native-gpu-probe")]
impl<B: Backend> InferenceStep for SyntheticBurnModel<B> {
    type Input = SyntheticBurnBatch<B>;
    type Output = SyntheticBurnMetric;

    fn step(&self, batch: Self::Input) -> Self::Output {
        let predictions = self.forward(batch.inputs);
        let loss = (predictions - batch.targets).powf_scalar(2.0).mean();
        SyntheticBurnMetric {
            loss: loss.into_scalar().elem::<f64>(),
        }
    }
}

#[cfg(feature = "native-gpu-probe")]
impl<B: AutodiffBackend> TrainStep for SyntheticBurnModel<B> {
    type Input = SyntheticBurnBatch<B>;
    type Output = SyntheticBurnMetric;

    fn step(&self, batch: Self::Input) -> TrainOutput<Self::Output> {
        let predictions = self.forward(batch.inputs);
        let loss = (predictions - batch.targets).powf_scalar(2.0).mean();
        let loss_value = loss.clone().into_scalar().elem::<f64>();
        let grads = loss.backward();
        TrainOutput::new(self, grads, SyntheticBurnMetric { loss: loss_value })
    }
}

#[cfg(feature = "native-gpu-probe")]
#[derive(Clone, Debug)]
struct SyntheticBurnRuntimeProject<B: AutodiffBackend> {
    dataset: BurnShardedDataset<SyntheticBurnRecord>,
    device: B::Device,
    learning_rate: f64,
    backend_label: &'static str,
    work_units_per_second: f64,
}

#[cfg(feature = "native-gpu-probe")]
impl<B> burn_p2p::burn::BurnLearnerWorkload for SyntheticBurnRuntimeProject<B>
where
    B: AutodiffBackend<FloatElem = f32> + 'static,
    SyntheticBurnEvalBackend<B>: Backend<FloatElem = f32>,
{
    type Backend = B;
    type Model = SyntheticBurnModel<B>;
    type EvalModel = SyntheticBurnModel<SyntheticBurnEvalBackend<B>>;
    type Optimizer = SyntheticBurnOptimizer<B>;
    type Scheduler = f64;

    fn init_learner(
        &self,
        device: &<Self::Backend as Backend>::Device,
    ) -> burn_p2p::burn::BurnWorkloadLearner<Self> {
        burn_p2p::burn::BurnLearner::new(
            SyntheticBurnModel::<B>::new(device),
            SgdConfig::new().init(),
            self.learning_rate,
        )
    }

    fn benchmark(
        &self,
        model: &Self::Model,
        _device: &<Self::Backend as Backend>::Device,
    ) -> CapabilityEstimate {
        let inventory = inspect_module::<B, _>(model);
        CapabilityEstimate {
            preferred_backends: vec![self.backend_label.into()],
            work_units_per_second: self
                .work_units_per_second
                .max(inventory.parameter_count.max(1) as f64),
            target_window_seconds: 1,
        }
    }

    fn evaluate(&self, model: &Self::EvalModel, _split: EvalSplit) -> MetricReport {
        let batch = SyntheticBurnBatcher.batch(
            vec![
                SyntheticBurnRecord { value: 0.2 },
                SyntheticBurnRecord { value: 0.4 },
                SyntheticBurnRecord { value: 0.6 },
                SyntheticBurnRecord { value: 0.8 },
            ],
            &self.device,
        );
        let metric = model.step(batch);
        MetricReport {
            metrics: BTreeMap::from([
                ("loss".into(), MetricValue::Float(metric.loss)),
                ("evaluation_items".into(), MetricValue::Integer(4)),
                ("evaluation_batches".into(), MetricValue::Integer(1)),
            ]),
            captured_at: Utc::now(),
        }
    }

    fn runtime_device(&self) -> <Self::Backend as Backend>::Device {
        self.device.clone()
    }

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        Ok(self.dataset.registration().clone())
    }

    fn microshard_plan(
        &self,
        _registration: &DatasetRegistration,
    ) -> anyhow::Result<burn_p2p::MicroShardPlan> {
        Ok(self.dataset.microshard_plan().clone())
    }

    fn load_batches(
        &self,
        _lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<BurnTrainBatch<Self>>> {
        self.dataset
            .load_batches(cached_microshards, SyntheticBurnBatcher, 8, &self.device)
    }

    fn after_train_step(
        &self,
        step_index: usize,
        output: &BurnTrainOutput<Self>,
        metrics: &mut BTreeMap<String, MetricValue>,
    ) -> Result<(), TrainError> {
        metrics.insert("loss".into(), MetricValue::Float(output.loss));
        metrics.insert(
            "batch_progress".into(),
            MetricValue::Integer((step_index + 1) as i64),
        );
        Ok(())
    }

    fn after_window(
        &self,
        learner: &burn_p2p::burn::BurnWorkloadLearner<Self>,
        metrics: &mut BTreeMap<String, MetricValue>,
    ) -> Result<(), TrainError> {
        let inventory = inspect_module::<B, _>(&learner.model());
        metrics.insert(
            "parameter_count".into(),
            MetricValue::Integer(inventory.parameter_count as i64),
        );
        Ok(())
    }
}

#[cfg(feature = "native-gpu-probe")]
fn synthetic_burn_backend_label(backend: SyntheticNativeBackend) -> &'static str {
    match backend {
        SyntheticNativeBackend::Auto => "auto",
        SyntheticNativeBackend::NdArray => "ndarray",
        SyntheticNativeBackend::Wgpu => "wgpu",
        SyntheticNativeBackend::Cuda => "cuda",
    }
}

#[cfg(feature = "native-gpu-probe")]
fn synthetic_burn_work_units_per_second(backend: SyntheticNativeBackend) -> f64 {
    match backend {
        SyntheticNativeBackend::Cuda => 192.0,
        SyntheticNativeBackend::Wgpu => 144.0,
        SyntheticNativeBackend::NdArray | SyntheticNativeBackend::Auto => 64.0,
    }
}

fn synthetic_trainer_learning_rate(
    workload_kind: SyntheticWorkloadKind,
    trainer_index: u32,
    trainer_count: u32,
) -> f64 {
    match workload_kind {
        SyntheticWorkloadKind::Scalar => {
            if trainer_index + 1 == trainer_count {
                1.0
            } else {
                ((trainer_index + 1) as f64) / ((trainer_count + 1) as f64)
            }
        }
        SyntheticWorkloadKind::BurnLinear => {
            if trainer_count <= 1 {
                0.02
            } else {
                let min = 0.01;
                let max = 0.03;
                let position = trainer_index as f64 / (trainer_count.saturating_sub(1)) as f64;
                min + (max - min) * position
            }
        }
    }
}

#[cfg(feature = "native-gpu-probe")]
fn create_synthetic_burn_dataset(
    root: &Path,
) -> anyhow::Result<BurnShardedDataset<SyntheticBurnRecord>> {
    let records = (1..=64)
        .map(|index| SyntheticBurnRecord {
            value: index as f32 / 64.0,
        })
        .collect::<Vec<_>>();
    BurnShardedDataset::write_local(
        root,
        &records,
        BurnShardedDatasetConfig::new("synthetic-burn-linear")
            .with_microshards(8)
            .with_sizing(DatasetSizing {
                total_examples: records.len() as u64,
                total_tokens: records.len() as u64,
                total_bytes: (records.len() * 64) as u64,
            }),
    )
}

#[cfg(feature = "native-gpu-probe")]
fn synthetic_burn_supported_workload() -> burn_p2p::SupportedWorkload {
    burn_p2p::SupportedWorkload {
        workload_id: burn_p2p::WorkloadId::new("synthetic-burn-linear"),
        workload_name: "Synthetic Burn Linear".into(),
        model_program_hash: burn_p2p::ContentId::new("synthetic-burn-linear-program"),
        checkpoint_format_hash: burn_p2p::ContentId::new("burn-burnpack"),
        supported_revision_family: burn_p2p::ContentId::new("synthetic-burn-linear-family"),
        resource_class: "native".into(),
    }
}

#[cfg(feature = "native-gpu-probe")]
fn synthetic_burn_release_manifest(
    supported_workload: &burn_p2p::SupportedWorkload,
) -> burn_p2p::ClientReleaseManifest {
    burn_p2p::ClientReleaseManifest {
        project_family_id: burn_p2p::ProjectFamilyId::new("synthetic-burn-linear-family"),
        release_train_hash: burn_p2p::ContentId::new("synthetic-burn-linear-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: burn_p2p::ContentId::new("synthetic-burn-linear-artifact"),
        target_platform: burn_p2p::ClientPlatform::Native,
        app_semver: Version::new(0, 1, 0),
        git_commit: "local-testkit".into(),
        cargo_lock_hash: burn_p2p::ContentId::new("synthetic-burn-linear-lock"),
        burn_version_string: "0.21.0-pre.3".into(),
        enabled_features_hash: burn_p2p::ContentId::new("synthetic-burn-linear-features"),
        protocol_major: 0,
        supported_workloads: vec![supported_workload.clone()],
        built_at: Utc::now(),
    }
}

#[cfg(feature = "native-gpu-probe")]
fn synthetic_burn_network_manifest(
    config: &SyntheticProcessConfig,
    release_manifest: &burn_p2p::ClientReleaseManifest,
) -> burn_p2p::NetworkManifest {
    burn_p2p::NetworkManifest {
        network_id: burn_p2p::NetworkId::new(config.scope.network_id.clone()),
        project_family_id: release_manifest.project_family_id.clone(),
        protocol_major: release_manifest.protocol_major,
        minimum_client_version: release_manifest.app_semver.clone(),
        required_release_train_hash: release_manifest.release_train_hash.clone(),
        allowed_target_artifact_hashes: std::collections::BTreeSet::from([release_manifest
            .target_artifact_hash
            .clone()]),
        authority_public_keys: Vec::new(),
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: burn_p2p::ContentId::new("synthetic-burn-linear-auth-policy"),
        created_at: Utc::now(),
        description: "burn_p2p synthetic burn soak".into(),
    }
}

/// Performs the create synthetic runtime dataset operation.
pub fn create_synthetic_runtime_dataset(root: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(root)?;
    let project = SyntheticRuntimeProject {
        dataset_root: root.to_path_buf(),
        learning_rate: 1.0,
        target_model: 100.0,
    };
    let registration = project.dataset_registration()?;
    let plan = project.microshard_plan(&registration)?;
    let manifest =
        ShardFetchManifest::from_microshards(&plan.dataset_view, &plan.microshards, |ordinal| {
            match ordinal {
                0 => b"4.0".to_vec(),
                _ => b"6.0".to_vec(),
            }
        });

    fs::write(
        root.join("fetch-manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    for entry in &manifest.entries {
        let bytes = match entry.ordinal {
            0 => b"4.0".to_vec(),
            _ => b"6.0".to_vec(),
        };
        fs::write(root.join(&entry.locator), bytes)?;
    }

    Ok(())
}

#[cfg(feature = "native-gpu-probe")]
fn run_synthetic_burn_process_node(
    config: &SyntheticProcessConfig,
) -> anyhow::Result<SyntheticProcessReport> {
    let backend = match config.native_backend {
        SyntheticNativeBackend::Auto => SyntheticNativeBackend::NdArray,
        other => other,
    };

    match backend {
        SyntheticNativeBackend::NdArray | SyntheticNativeBackend::Auto => {
            run_synthetic_burn_process_node_with_backend::<SyntheticBurnNdArrayBackend>(
                config,
                backend,
                <SyntheticBurnNdArrayBackend as Backend>::Device::default(),
            )
        }
        SyntheticNativeBackend::Wgpu => {
            let device = burn::backend::wgpu::WgpuDevice::default();
            wgpu::init_setup::<wgpu::graphics::AutoGraphicsApi>(&device, Default::default());
            run_synthetic_burn_process_node_with_backend::<SyntheticBurnWgpuBackend>(
                config, backend, device,
            )
        }
        SyntheticNativeBackend::Cuda => {
            run_synthetic_burn_process_node_with_backend::<SyntheticBurnCudaBackend>(
                config,
                backend,
                <SyntheticBurnCudaBackend as Backend>::Device::default(),
            )
        }
    }
}

#[cfg(feature = "native-gpu-probe")]
fn run_synthetic_burn_process_node_with_backend<B>(
    config: &SyntheticProcessConfig,
    backend: SyntheticNativeBackend,
    device: <B as Backend>::Device,
) -> anyhow::Result<SyntheticProcessReport>
where
    B: AutodiffBackend<FloatElem = f32> + 'static,
    SyntheticBurnEvalBackend<B>: Backend<FloatElem = f32>,
{
    let supported_workload = synthetic_burn_supported_workload();
    let release_manifest = synthetic_burn_release_manifest(&supported_workload);
    let network_manifest = synthetic_burn_network_manifest(config, &release_manifest);
    let workload = SyntheticBurnRuntimeProject::<B> {
        dataset: create_synthetic_burn_dataset(&config.dataset_root)?,
        device,
        learning_rate: config.learning_rate,
        backend_label: synthetic_burn_backend_label(backend),
        work_units_per_second: synthetic_burn_work_units_per_second(backend),
    };
    let builder = match config.mode {
        SyntheticProcessMode::Validator => burn_p2p::burn::validator(
            release_manifest,
            workload,
            burn_p2p::burn::BurnWorkloadConfig::standard(supported_workload),
        )?,
        SyntheticProcessMode::Trainer => burn_p2p::burn::trainer(
            release_manifest,
            workload,
            burn_p2p::burn::BurnWorkloadConfig::standard(supported_workload),
        )?,
    }
    .with_network(network_manifest)?;

    run_synthetic_process_node_with_builder(config, builder)
}

/// Performs the run synthetic process node operation.
pub fn run_synthetic_process_node(
    config: &SyntheticProcessConfig,
) -> anyhow::Result<SyntheticProcessReport> {
    match config.workload_kind {
        SyntheticWorkloadKind::Scalar => {
            if !config.dataset_root.join("fetch-manifest.json").exists() {
                create_synthetic_runtime_dataset(&config.dataset_root)?;
            }
            let roles = match config.mode {
                SyntheticProcessMode::Validator => {
                    PeerRoleSet::new([PeerRole::Authority, PeerRole::Validator, PeerRole::Archive])
                }
                SyntheticProcessMode::Trainer => PeerRoleSet::default_trainer(),
            };
            let genesis = GenesisSpec {
                network_id: burn_p2p::NetworkId::new(config.scope.network_id.clone()),
                protocol_version: Version::new(0, 1, 0),
                display_name: "burn_p2p testkit native process".into(),
                created_at: Utc::now(),
                metadata: BTreeMap::from([("mode".into(), format!("{:?}", config.mode))]),
            };
            let builder = NodeBuilder::new(SyntheticRuntimeProject {
                dataset_root: config.dataset_root.clone(),
                learning_rate: config.learning_rate,
                target_model: config.target_model,
            })
            .with_mainnet(genesis)
            .with_roles(roles);
            run_synthetic_process_node_with_builder(config, builder)
        }
        SyntheticWorkloadKind::BurnLinear => {
            #[cfg(feature = "native-gpu-probe")]
            {
                if !config.dataset_root.join("fetch-manifest.json").exists() {
                    let _ = create_synthetic_burn_dataset(&config.dataset_root)?;
                }
                run_synthetic_burn_process_node(config)
            }
            #[cfg(not(feature = "native-gpu-probe"))]
            {
                anyhow::bail!(
                    "SyntheticWorkloadKind::BurnLinear requires burn_p2p_testkit built with --features native-gpu-probe"
                )
            }
        }
    }
}

fn run_synthetic_process_node_with_builder<W>(
    config: &SyntheticProcessConfig,
    mut builder: NodeBuilder<W>,
) -> anyhow::Result<SyntheticProcessReport>
where
    W: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
    W::Model: Send + 'static,
{
    if let Some(parent) = config.report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(shutdown_sentinel) = &config.shutdown_sentinel
        && let Some(parent) = shutdown_sentinel.parent()
    {
        fs::create_dir_all(parent)?;
    }

    builder = builder
        .with_storage(StorageConfig::new(config.storage_root.clone()))
        .with_bootstrap_peers(config.bootstrap_peers.clone());
    if config.persist_identity {
        builder = builder.with_identity(burn_p2p::IdentityConfig::Persistent);
    }
    if !config.listen_addresses.is_empty() {
        builder = builder.with_listen_addresses(config.listen_addresses.clone());
    }

    let mut report = SyntheticProcessReport {
        status: "starting".into(),
        started_at: Some(Utc::now()),
        ..SyntheticProcessReport::default()
    };
    write_report(&config.report_path, &report)?;

    let mut node = match builder.spawn() {
        Ok(node) => node,
        Err(error) => {
            report.status = "failed".into();
            report.error = Some(error.to_string());
            write_report(&config.report_path, &report)?;
            return Err(error);
        }
    };
    let telemetry = node.telemetry();
    wait_for(
        Duration::from_secs(config.startup_timeout_secs),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.status == burn_p2p::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "runtime did not become ready",
    )?;

    let snapshot = telemetry.snapshot();
    let experiment = node.experiment(
        burn_p2p::StudyId::new(config.scope.study_id.clone()),
        burn_p2p::ExperimentId::new(config.scope.experiment_id.clone()),
        burn_p2p::RevisionId::new(config.scope.revision_id.clone()),
    );
    report.status = "running".into();
    report.runtime_ready_at = Some(Utc::now());
    report.local_peer_id = snapshot.local_peer_id.map(|peer_id| peer_id.to_string());
    report.listen_addresses = snapshot
        .listen_addresses
        .iter()
        .map(|address| address.as_str().to_owned())
        .collect();
    refresh_report_counts(&mut report, &config.storage_root)?;
    write_report(&config.report_path, &report)?;

    let result = match config.mode {
        SyntheticProcessMode::Validator => {
            run_validator_process(config, &experiment, &mut node, &mut report)
        }
        SyntheticProcessMode::Trainer => {
            run_trainer_process(config, &experiment, &mut node, &mut report)
        }
    };

    if result.is_ok() {
        if matches!(config.mode, SyntheticProcessMode::Trainer)
            && !config.wait_for_canonical_advance
        {
            let linger = Duration::from_secs(config.merge_wait_timeout_secs.clamp(5, 30));
            thread::sleep(linger);
        }
        let drain_grace = Duration::from_millis(config.poll_interval_ms.max(50) * 5);
        thread::sleep(drain_grace);
    }

    let shutdown_result = node.shutdown();
    let await_result = node.await_termination();
    match (result, shutdown_result, await_result) {
        (Ok(()), Ok(()), Ok(_)) => {
            report.status = "completed".into();
            report.completed_at = Some(Utc::now());
            refresh_report_counts(&mut report, &config.storage_root)?;
            write_report(&config.report_path, &report)?;
            Ok(report)
        }
        (Err(error), _, _) => {
            report.status = "failed".into();
            report.completed_at = Some(Utc::now());
            report.error = Some(error.to_string());
            refresh_report_counts(&mut report, &config.storage_root)?;
            write_report(&config.report_path, &report)?;
            Err(error)
        }
        (Ok(()), Err(error), _) => {
            report.status = "failed".into();
            report.completed_at = Some(Utc::now());
            report.error = Some(error.to_string());
            refresh_report_counts(&mut report, &config.storage_root)?;
            write_report(&config.report_path, &report)?;
            Err(error)
        }
        (Ok(()), Ok(()), Err(error)) => {
            report.status = "failed".into();
            report.completed_at = Some(Utc::now());
            report.error = Some(error.to_string());
            refresh_report_counts(&mut report, &config.storage_root)?;
            write_report(&config.report_path, &report)?;
            Err(error)
        }
    }
}

fn run_validator_process<W>(
    config: &SyntheticProcessConfig,
    experiment: &burn_p2p::ExperimentHandle,
    node: &mut burn_p2p::RunningNode<W>,
    report: &mut SyntheticProcessReport,
) -> anyhow::Result<()>
where
    W: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
    W::Model: Send + 'static,
{
    let head = node
        .restore_experiment_head(experiment)?
        .map(Ok)
        .unwrap_or_else(|| node.initialize_local_head(experiment))?;
    let synced_head = node
        .sync_experiment_head(experiment)?
        .unwrap_or_else(|| head.clone());
    report.initialized_head_id = Some(head.head_id.to_string());
    report.synced_head_id = Some(synced_head.head_id.to_string());
    refresh_runtime_view(report, experiment, node);
    refresh_report_counts(report, &config.storage_root)?;
    write_report(&config.report_path, report)?;

    let poll_interval = Duration::from_millis(config.poll_interval_ms.max(10));
    loop {
        if config
            .shutdown_sentinel
            .as_ref()
            .is_some_and(|path| path.exists())
        {
            return Ok(());
        }

        match node.validate_candidates_once(experiment) {
            Ok(Some(outcome)) => {
                report.merged_head_id = Some(outcome.merged_head.head_id.to_string());
                report.observed_canonical_head_id = report.merged_head_id.clone();
                report.error = None;
                refresh_runtime_view(report, experiment, node);
                refresh_report_counts(report, &config.storage_root)?;
                write_report(&config.report_path, report)?;
            }
            Ok(None) => {
                refresh_runtime_view(report, experiment, node);
                refresh_report_counts(report, &config.storage_root)?;
                write_report(&config.report_path, report)?;
                thread::sleep(poll_interval);
            }
            Err(error) => {
                report.error = Some(error.to_string());
                refresh_runtime_view(report, experiment, node);
                write_report(&config.report_path, report)?;
                thread::sleep(poll_interval);
            }
        }
    }
}

fn run_trainer_process<W>(
    config: &SyntheticProcessConfig,
    experiment: &burn_p2p::ExperimentHandle,
    node: &mut burn_p2p::RunningNode<W>,
    report: &mut SyntheticProcessReport,
) -> anyhow::Result<()>
where
    W: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
    W::Model: Send + 'static,
{
    if config.continuous_training {
        anyhow::ensure!(
            !config.wait_for_canonical_advance,
            "continuous training does not support strict per-window canonical wait in the synthetic harness"
        );
        return run_continuous_trainer_process(config, experiment, node, report);
    }

    let poll_interval = Duration::from_millis(config.poll_interval_ms.max(10));
    let sync_timeout = Duration::from_secs(config.sync_timeout_secs);
    let merge_timeout = Duration::from_secs(config.merge_wait_timeout_secs);
    let canonical_advance_timeout = trainer_canonical_advance_timeout(config, merge_timeout);
    let mut next_base_head = None;
    let mut previous_base_head = None;
    let mut start_barrier_pending = true;

    for window_index in 0..config.window_count.max(1) {
        report.window_timelines.push(SyntheticWindowTimeline {
            window_index,
            ..SyntheticWindowTimeline::default()
        });
        let synced_head = if let Some(head) = next_base_head.take() {
            head
        } else if let Some(base_head) = previous_base_head.as_ref() {
            let advanced_head = wait_for_inter_window_canonical_advance(
                node,
                experiment,
                base_head,
                merge_timeout,
                poll_interval,
                report,
                ReportPaths {
                    report_path: &config.report_path,
                    storage_root: &config.storage_root,
                },
            )?;
            if let Some(timeline) = report
                .window_timelines
                .iter_mut()
                .rev()
                .find(|timeline| timeline.window_index + 1 == window_index)
                && timeline.canonical_advanced_at.is_none()
            {
                timeline.canonical_advanced_at = Some(Utc::now());
            }
            advanced_head
        } else {
            wait_for_synced_head(node, experiment, sync_timeout, poll_interval)?
        };
        previous_base_head = Some(synced_head.clone());
        report.synced_head_id = Some(synced_head.head_id.to_string());
        if let Some(timeline) = report.window_timelines.last_mut() {
            timeline.synced_at = Some(Utc::now());
        }
        write_report(&config.report_path, report)?;

        if start_barrier_pending && let Some(start_sentinel) = config.start_sentinel.as_ref() {
            let barrier_timeout = Duration::from_secs(
                config
                    .merge_wait_timeout_secs
                    .max(
                        config
                            .startup_timeout_secs
                            .saturating_add(config.sync_timeout_secs),
                    )
                    .max(30),
            );
            wait_for(
                barrier_timeout,
                || start_sentinel.exists(),
                "timed out waiting for training start sentinel",
            )?;
            start_barrier_pending = false;
        }

        let outcome = train_window_once_with_retry(
            node,
            experiment,
            &synced_head,
            sync_timeout,
            poll_interval,
        )?;
        report.trained_head_id = Some(outcome.head.head_id.to_string());
        report.trained_parent_head_id = outcome
            .head
            .parent_head_id
            .as_ref()
            .map(|head_id| head_id.to_string());
        if let Some(timeline) = report.window_timelines.last_mut() {
            timeline.trained_at = Some(Utc::now());
        }
        refresh_report_counts(report, &config.storage_root)?;
        write_report(&config.report_path, report)?;

        if config.wait_for_canonical_advance {
            match wait_for_canonical_advance(
                node,
                experiment,
                CanonicalAdvanceWait {
                    base_head_id: &synced_head.head_id,
                    completed_window: &outcome,
                    timeout: canonical_advance_timeout,
                    poll_interval,
                    report,
                    paths: ReportPaths {
                        report_path: &config.report_path,
                        storage_root: &config.storage_root,
                    },
                },
            ) {
                Ok(observed_head) => {
                    report.observed_canonical_head_id = Some(observed_head.head_id.to_string());
                    if let Some(timeline) = report.window_timelines.last_mut() {
                        timeline.canonical_advanced_at = Some(Utc::now());
                    }
                    next_base_head = Some(observed_head);
                }
                Err(_error) if !config.canonical_advance_required => {
                    if let Some(timeline) = report.window_timelines.last_mut() {
                        timeline.canonical_observation_timed_out = true;
                    }
                    report.error = None;
                    refresh_report_counts(report, &config.storage_root)?;
                    write_report(&config.report_path, report)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    wait_for_shutdown_sentinel(config, poll_interval, report)
}

fn trainer_canonical_advance_timeout(
    config: &SyntheticProcessConfig,
    merge_timeout: Duration,
) -> Duration {
    if config.canonical_advance_required {
        merge_timeout
    } else {
        OPTIONAL_CANONICAL_ADVANCE_TIMEOUT
    }
}

fn wait_for_shutdown_sentinel(
    config: &SyntheticProcessConfig,
    poll_interval: Duration,
    report: &mut SyntheticProcessReport,
) -> anyhow::Result<()> {
    let Some(shutdown_sentinel) = config.shutdown_sentinel.as_ref() else {
        return Ok(());
    };

    loop {
        if shutdown_sentinel.exists() {
            return Ok(());
        }
        refresh_report_counts(report, &config.storage_root)?;
        write_report(&config.report_path, report)?;
        thread::sleep(poll_interval);
    }
}

fn run_continuous_trainer_process<W>(
    config: &SyntheticProcessConfig,
    experiment: &burn_p2p::ExperimentHandle,
    node: &mut burn_p2p::RunningNode<W>,
    report: &mut SyntheticProcessReport,
) -> anyhow::Result<()>
where
    W: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
    W::Model: Send + 'static,
{
    let poll_interval = Duration::from_millis(config.poll_interval_ms.max(10));
    let sync_timeout = Duration::from_secs(config.sync_timeout_secs);
    let merge_timeout = Duration::from_secs(config.merge_wait_timeout_secs);
    let mut start_barrier_pending = true;
    let mut last_visible_canonical =
        wait_for_synced_head(node, experiment, sync_timeout, poll_interval)?;
    report.synced_head_id = Some(last_visible_canonical.head_id.to_string());
    let mut pending_canonical_timeline = None::<usize>;
    let mut trainer = node.continuous_trainer(experiment)?;

    for window_index in 0..config.window_count.max(1) {
        report.window_timelines.push(SyntheticWindowTimeline {
            window_index,
            ..SyntheticWindowTimeline::default()
        });
        let timeline_index = report.window_timelines.len() - 1;

        if let Some(latest) = trainer.sync_canonical_head()?
            && latest.head_id != last_visible_canonical.head_id
        {
            if let Some(previous_index) = pending_canonical_timeline.take()
                && let Some(timeline) = report.window_timelines.get_mut(previous_index)
            {
                timeline.canonical_advanced_at = Some(Utc::now());
            }
            report.observed_canonical_head_id = Some(latest.head_id.to_string());
            last_visible_canonical = latest;
        }
        report.synced_head_id = Some(last_visible_canonical.head_id.to_string());
        if let Some(timeline) = report.window_timelines.get_mut(timeline_index) {
            timeline.synced_at = Some(Utc::now());
        }
        write_report(&config.report_path, report)?;

        if start_barrier_pending && let Some(start_sentinel) = config.start_sentinel.as_ref() {
            let barrier_timeout = Duration::from_secs(
                config
                    .merge_wait_timeout_secs
                    .max(
                        config
                            .startup_timeout_secs
                            .saturating_add(config.sync_timeout_secs),
                    )
                    .max(30),
            );
            wait_for(
                barrier_timeout,
                || start_sentinel.exists(),
                "timed out waiting for training start sentinel",
            )?;
            start_barrier_pending = false;
        }

        let outcome =
            train_next_window_continuous_with_retry(&mut trainer, sync_timeout, poll_interval)?;
        report.trained_head_id = Some(outcome.head.head_id.to_string());
        report.trained_parent_head_id = outcome
            .head
            .parent_head_id
            .as_ref()
            .map(|head_id| head_id.to_string());
        if let Some(timeline) = report.window_timelines.get_mut(timeline_index) {
            timeline.trained_at = Some(Utc::now());
        }
        pending_canonical_timeline = Some(timeline_index);
        refresh_report_counts(report, &config.storage_root)?;
        write_report(&config.report_path, report)?;
    }

    if let Some(pending_index) = pending_canonical_timeline.take() {
        let deadline = Instant::now() + merge_timeout;
        while Instant::now() < deadline {
            if let Some(latest) = trainer.sync_canonical_head()?
                && latest.head_id != last_visible_canonical.head_id
            {
                if let Some(timeline) = report.window_timelines.get_mut(pending_index) {
                    timeline.canonical_advanced_at = Some(Utc::now());
                }
                report.observed_canonical_head_id = Some(latest.head_id.to_string());
                break;
            }
            thread::sleep(poll_interval);
        }
        refresh_report_counts(report, &config.storage_root)?;
        write_report(&config.report_path, report)?;
    }

    wait_for_shutdown_sentinel(config, poll_interval, report)
}

fn refresh_runtime_view<W>(
    report: &mut SyntheticProcessReport,
    experiment: &burn_p2p::ExperimentHandle,
    node: &burn_p2p::RunningNode<W>,
) where
    W: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
    W::Model: Send + 'static,
{
    let snapshot = node.telemetry().snapshot();
    report.connected_peer_count = snapshot.connected_peers;
    report.visible_experiment_head_count = snapshot
        .control_plane
        .head_announcements
        .iter()
        .filter(|announcement| {
            announcement.head.study_id == experiment.study_id
                && announcement.head.experiment_id == experiment.experiment_id
                && announcement.head.revision_id == experiment.revision_id
        })
        .count();
    report.visible_experiment_update_count = snapshot
        .control_plane
        .update_announcements
        .iter()
        .filter(|announcement| {
            announcement.update.study_id == experiment.study_id
                && announcement.update.experiment_id == experiment.experiment_id
                && announcement.update.revision_id == experiment.revision_id
        })
        .count();
}

fn wait_for_synced_head<W>(
    node: &burn_p2p::RunningNode<W>,
    experiment: &burn_p2p::ExperimentHandle,
    timeout: Duration,
    poll_interval: Duration,
) -> anyhow::Result<burn_p2p::HeadDescriptor>
where
    W: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
    W::Model: Send + 'static,
{
    let mut deadline = Instant::now() + timeout;
    let mut extended_grace = false;
    let mut last_transient_error = None;
    loop {
        if Instant::now() >= deadline {
            if !extended_grace && node.telemetry().snapshot().connected_peers > 0 {
                let grace = Duration::from_millis(
                    ((timeout.as_millis() / 2).max(u128::from(poll_interval.as_millis() as u64)))
                        .min(u128::from(u64::MAX)) as u64,
                );
                deadline += grace;
                extended_grace = true;
                continue;
            }
            break;
        }

        match node.sync_experiment_head(experiment) {
            Ok(Some(head)) => return Ok(head),
            Ok(None) => {}
            Err(error) if is_transient_runtime_error(&error) => {
                last_transient_error = Some(error.to_string());
            }
            Err(error) => return Err(error),
        }
        thread::sleep(poll_interval);
    }

    if let Ok(Some(head)) = node.restore_experiment_head(experiment) {
        return Ok(head);
    }

    if let Some(message) = last_transient_error {
        Err(anyhow::anyhow!(
            "timed out waiting for canonical head sync after transient error: {message}"
        ))
    } else {
        Err(anyhow::anyhow!("timed out waiting for canonical head sync"))
    }
}

fn train_window_once_with_retry<W>(
    node: &mut burn_p2p::RunningNode<W>,
    experiment: &burn_p2p::ExperimentHandle,
    base_head: &HeadDescriptor,
    timeout: Duration,
    poll_interval: Duration,
) -> anyhow::Result<burn_p2p::TrainingWindowOutcome<BTreeMap<String, MetricValue>>>
where
    W: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
    W::Model: Send + 'static,
{
    let deadline = Instant::now() + timeout;
    let mut last_transient_error = None;
    while Instant::now() < deadline {
        match node.train_window_once_with_pinned_head(experiment, Some(base_head)) {
            Ok(outcome) => return Ok(outcome),
            Err(error) if is_transient_runtime_error(&error) => {
                last_transient_error = Some(error.to_string());
                thread::sleep(poll_interval);
            }
            Err(error) => return Err(error),
        }
    }

    if let Some(message) = last_transient_error {
        Err(anyhow::anyhow!(
            "timed out waiting for train_window_once after transient error: {message}"
        ))
    } else {
        Err(anyhow::anyhow!("timed out waiting for train_window_once"))
    }
}

fn train_next_window_continuous_with_retry<W>(
    trainer: &mut burn_p2p::ContinuousTrainer<'_, W>,
    timeout: Duration,
    poll_interval: Duration,
) -> anyhow::Result<burn_p2p::TrainingWindowOutcome<BTreeMap<String, MetricValue>>>
where
    W: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
    W::Model: Send + 'static,
{
    let deadline = Instant::now() + timeout;
    let mut last_transient_error = None;
    while Instant::now() < deadline {
        match trainer.train_next_window() {
            Ok(outcome) => return Ok(outcome),
            Err(error) if is_transient_runtime_error(&error) => {
                last_transient_error = Some(error.to_string());
                thread::sleep(poll_interval);
            }
            Err(error) => return Err(error),
        }
    }

    if let Some(message) = last_transient_error {
        Err(anyhow::anyhow!(
            "timed out waiting for continuous train_next_window after transient error: {message}"
        ))
    } else {
        Err(anyhow::anyhow!(
            "timed out waiting for continuous train_next_window"
        ))
    }
}

fn is_transient_runtime_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("request failure")
        || message.contains("Failed to dial")
        || message.contains("timed out")
        || message.contains("no connected peer provided artifact")
        || message.contains("no connected peer provided chunk")
}

struct ReportPaths<'a> {
    report_path: &'a Path,
    storage_root: &'a Path,
}

struct CanonicalAdvanceWait<'a> {
    base_head_id: &'a burn_p2p::HeadId,
    completed_window: &'a burn_p2p::TrainingWindowOutcome<BTreeMap<String, MetricValue>>,
    timeout: Duration,
    poll_interval: Duration,
    report: &'a mut SyntheticProcessReport,
    paths: ReportPaths<'a>,
}

fn wait_for_canonical_advance<W>(
    node: &burn_p2p::RunningNode<W>,
    experiment: &burn_p2p::ExperimentHandle,
    wait: CanonicalAdvanceWait<'_>,
) -> anyhow::Result<burn_p2p::HeadDescriptor>
where
    W: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
    W::Model: Send + 'static,
{
    let CanonicalAdvanceWait {
        base_head_id,
        completed_window,
        timeout,
        poll_interval,
        report,
        paths,
    } = wait;
    let timeout = test_timeout(timeout);
    let deadline = Instant::now() + timeout;
    let mut republish_attempts = 0_u32;
    while Instant::now() < deadline {
        match node.sync_experiment_head(experiment) {
            Ok(Some(head)) => {
                report.observed_canonical_head_id = Some(head.head_id.to_string());
                report.error = None;
                refresh_report_counts(report, paths.storage_root)?;
                write_report(paths.report_path, report)?;
                if &head.head_id != base_head_id {
                    return Ok(head);
                }
            }
            Ok(None) => {}
            Err(error) => {
                report.error = Some(error.to_string());
                write_report(paths.report_path, report)?;
            }
        }
        republish_attempts = republish_attempts.saturating_add(1);
        if report.receipt_count == 0 && republish_attempts.is_multiple_of(6) {
            let _ = node.republish_training_window_control_plane(
                experiment,
                completed_window.lease.window_id,
                &completed_window.contribution.base_head_id,
                &completed_window.artifact.artifact_id,
            );
        }
        thread::sleep(poll_interval);
    }

    refresh_report_counts(report, paths.storage_root)?;
    if report.receipt_count >= 1 {
        let serve_grace = timeout.max(Duration::from_secs(5));
        let serve_deadline = Instant::now() + serve_grace;
        while Instant::now() < serve_deadline {
            match node.sync_experiment_head(experiment) {
                Ok(Some(head)) => {
                    report.observed_canonical_head_id = Some(head.head_id.to_string());
                    report.error = None;
                    refresh_report_counts(report, paths.storage_root)?;
                    write_report(paths.report_path, report)?;
                    if &head.head_id != base_head_id {
                        return Ok(head);
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    report.error = Some(error.to_string());
                    write_report(paths.report_path, report)?;
                }
            }
            thread::sleep(poll_interval);
        }
    }

    Err(anyhow::anyhow!(
        "timed out waiting for canonical head to advance from {}",
        base_head_id.as_str()
    ))
}

fn wait_for_inter_window_canonical_advance<W>(
    node: &burn_p2p::RunningNode<W>,
    experiment: &burn_p2p::ExperimentHandle,
    base_head: &HeadDescriptor,
    timeout: Duration,
    poll_interval: Duration,
    report: &mut SyntheticProcessReport,
    paths: ReportPaths<'_>,
) -> anyhow::Result<burn_p2p::HeadDescriptor>
where
    W: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
    W::Model: Send + 'static,
{
    let deadline = Instant::now() + test_timeout(timeout);
    let mut last_transient_error = None;
    while Instant::now() < deadline {
        match node.sync_experiment_head(experiment) {
            Ok(Some(head)) => {
                report.observed_canonical_head_id = Some(head.head_id.to_string());
                report.error = None;
                refresh_report_counts(report, paths.storage_root)?;
                write_report(paths.report_path, report)?;
                if head.global_step > base_head.global_step && head.head_id != base_head.head_id {
                    return Ok(head);
                }
            }
            Ok(None) => {}
            Err(error) if is_transient_runtime_error(&error) => {
                last_transient_error = Some(error.to_string());
            }
            Err(error) => return Err(error),
        }
        thread::sleep(poll_interval);
    }

    refresh_report_counts(report, paths.storage_root)?;
    write_report(paths.report_path, report)?;
    if let Some(message) = last_transient_error {
        Err(anyhow::anyhow!(
            "timed out waiting for inter-window canonical head to advance from {} after transient error: {message}",
            base_head.head_id.as_str()
        ))
    } else {
        Err(anyhow::anyhow!(
            "timed out waiting for inter-window canonical head to advance from {}; refusing to train the next synthetic window on a stale base",
            base_head.head_id.as_str()
        ))
    }
}

fn wait_for(
    timeout: Duration,
    condition: impl Fn() -> bool,
    failure_message: &str,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + test_timeout(timeout);
    while Instant::now() < deadline {
        if condition() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }

    Err(anyhow::anyhow!(failure_message.to_owned()))
}

fn test_timeout(timeout: Duration) -> Duration {
    let scale = std::env::var("BURN_P2P_TEST_TIMEOUT_SCALE")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_else(|| {
            if std::env::var_os("CI").is_some() {
                3
            } else {
                1
            }
        })
        .max(1);
    timeout.checked_mul(scale).unwrap_or(timeout)
}

fn write_report(path: &Path, report: &SyntheticProcessReport) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("report path {} has no parent", path.display()))?;
    fs::create_dir_all(parent)?;
    let tmp_path = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("report"),
        std::process::id()
    ));
    fs::write(&tmp_path, serde_json::to_vec_pretty(report)?)
        .with_context(|| format!("failed to write {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| format!("failed to replace {}", path.display()))
}

fn refresh_report_counts(
    report: &mut SyntheticProcessReport,
    storage_root: &Path,
) -> anyhow::Result<()> {
    let receipts_dir = storage_root.join("receipts");
    report.receipt_count = count_json_files(&receipts_dir, |name| !name.starts_with("merge-"))?;
    report.merge_count = count_json_files(&receipts_dir, |name| name.starts_with("merge-"))?;
    Ok(())
}

fn count_json_files(dir: &Path, include: impl Fn(&str) -> bool) -> anyhow::Result<usize> {
    if !dir.exists() {
        return Ok(0);
    }

    let mut count = 0;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".json") && include(&name) {
            count += 1;
        }
    }

    Ok(count)
}

fn percentile_nearest_rank(sorted_values: &[u64], percentile: f64) -> u64 {
    debug_assert!(!sorted_values.is_empty());
    let rank = (percentile.clamp(0.0, 1.0) * sorted_values.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted_values.len() - 1);
    sorted_values[index]
}

fn summarize_timing_distribution(samples: &[u64]) -> SyntheticTimingDistributionSummary {
    if samples.is_empty() {
        return SyntheticTimingDistributionSummary::default();
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let mean_ms = (sorted.iter().copied().sum::<u64>() as f64 / sorted.len() as f64).round() as u64;
    SyntheticTimingDistributionSummary {
        sample_count: sorted.len(),
        mean_ms,
        p50_ms: percentile_nearest_rank(&sorted, 0.50),
        p90_ms: percentile_nearest_rank(&sorted, 0.90),
        max_ms: *sorted.last().unwrap_or(&0),
    }
}

fn ratio_u64(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn synthetic_soak_storage_roots(summary: &SyntheticSoakSummary) -> Vec<PathBuf> {
    let mut roots = vec![summary.root.join("validator-storage")];
    for index in 0..summary.trainer_count {
        roots.push(summary.root.join(format!("trainer-{index}-storage")));
    }
    roots
}

fn collect_runtime_metric_records(
    storage_roots: &[PathBuf],
) -> anyhow::Result<(
    Vec<PeerWindowMetrics>,
    Vec<ReducerCohortMetrics>,
    Vec<HeadEvalReport>,
)> {
    let mut peer_windows = Vec::new();
    let mut reducer_cohorts = Vec::new();
    let mut head_reports = Vec::new();

    for storage_root in storage_roots {
        let metrics_dir = storage_root.join("metrics");
        if !metrics_dir.exists() {
            continue;
        }
        for entry in fs::read_dir(&metrics_dir)
            .with_context(|| format!("failed to read {}", metrics_dir.display()))?
        {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let file_name = match path.file_name().and_then(|name| name.to_str()) {
                Some(name) => name,
                None => continue,
            };
            let bytes =
                fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
            if file_name.starts_with("peer-window-") {
                peer_windows.push(
                    serde_json::from_slice(&bytes)
                        .with_context(|| format!("failed to decode {}", path.display()))?,
                );
            } else if file_name.starts_with("reducer-cohort-") {
                reducer_cohorts.push(
                    serde_json::from_slice(&bytes)
                        .with_context(|| format!("failed to decode {}", path.display()))?,
                );
            } else if file_name.starts_with("head-eval-") {
                head_reports.push(
                    serde_json::from_slice(&bytes)
                        .with_context(|| format!("failed to decode {}", path.display()))?,
                );
            }
        }
    }

    Ok((peer_windows, reducer_cohorts, head_reports))
}

/// Derives a runtime-backed network performance summary for one synthetic soak.
pub fn derive_synthetic_soak_performance_summary(
    summary: &SyntheticSoakSummary,
) -> anyhow::Result<Option<NetworkPerformanceSummary>> {
    let storage_roots = synthetic_soak_storage_roots(summary);
    let (peer_windows, reducer_cohorts, head_reports) =
        collect_runtime_metric_records(&storage_roots)?;
    Ok(derive_network_performance_summary(
        &peer_windows,
        &reducer_cohorts,
        &head_reports,
    ))
}

/// Derives higher-level startup, sync, and convergence timing summaries for one soak.
pub fn derive_synthetic_network_dynamics_summary(
    summary: &SyntheticSoakSummary,
) -> Option<SyntheticNetworkDynamicsSummary> {
    let mut startup_latencies = Vec::new();
    let mut trainer_sync_latencies = Vec::new();
    let mut trainer_training_latencies = Vec::new();
    let mut trainer_canonical_advance_latencies = Vec::new();

    for report in std::iter::once(&summary.validator_report).chain(summary.trainer_reports.iter()) {
        if let (Some(started_at), Some(runtime_ready_at)) =
            (report.started_at, report.runtime_ready_at)
        {
            startup_latencies
                .push((runtime_ready_at - started_at).num_milliseconds().max(0) as u64);
        }
    }

    for report in &summary.trainer_reports {
        if let Some(runtime_ready_at) = report.runtime_ready_at
            && let Some(first_sync) = report
                .window_timelines
                .first()
                .and_then(|timeline| timeline.synced_at)
        {
            trainer_sync_latencies
                .push((first_sync - runtime_ready_at).num_milliseconds().max(0) as u64);
        }
        for timeline in &report.window_timelines {
            if let (Some(synced_at), Some(trained_at)) = (timeline.synced_at, timeline.trained_at) {
                trainer_training_latencies
                    .push((trained_at - synced_at).num_milliseconds().max(0) as u64);
            }
            if let (Some(trained_at), Some(canonical_advanced_at)) =
                (timeline.trained_at, timeline.canonical_advanced_at)
            {
                trainer_canonical_advance_latencies.push(
                    (canonical_advanced_at - trained_at)
                        .num_milliseconds()
                        .max(0) as u64,
                );
            }
        }
    }

    let performance = summary.performance_summary.as_ref();
    Some(SyntheticNetworkDynamicsSummary {
        merge_rate_per_sec: if summary.elapsed_millis == 0 {
            0.0
        } else {
            summary.total_merges as f64 / (summary.elapsed_millis as f64 / 1000.0)
        },
        receipt_rate_per_sec: if summary.elapsed_millis == 0 {
            0.0
        } else {
            summary.total_receipts as f64 / (summary.elapsed_millis as f64 / 1000.0)
        },
        peer_startup_latency_ms: summarize_timing_distribution(&startup_latencies),
        trainer_sync_latency_ms: summarize_timing_distribution(&trainer_sync_latencies),
        trainer_training_latency_ms: summarize_timing_distribution(&trainer_training_latencies),
        trainer_canonical_advance_latency_ms: summarize_timing_distribution(
            &trainer_canonical_advance_latencies,
        ),
        training_active_ratio: performance
            .map(|summary| {
                ratio_u64(
                    summary.training.active_window_time_ms,
                    summary.training.elapsed_time_ms,
                )
            })
            .unwrap_or(0.0),
        training_idle_ratio: performance
            .map(|summary| {
                ratio_u64(
                    summary.training.idle_time_ms,
                    summary.training.elapsed_time_ms,
                )
            })
            .unwrap_or(0.0),
        validation_active_ratio: performance
            .map(|summary| {
                ratio_u64(
                    summary.validation.active_window_time_ms,
                    summary.validation.elapsed_time_ms,
                )
            })
            .unwrap_or(0.0),
        validation_idle_ratio: performance
            .map(|summary| {
                ratio_u64(
                    summary.validation.idle_time_ms,
                    summary.validation.elapsed_time_ms,
                )
            })
            .unwrap_or(0.0),
    })
}

/// Performs the run synthetic process soak operation.
pub fn run_synthetic_process_soak(
    config: &SyntheticSoakConfig,
    node_binary: &Path,
) -> anyhow::Result<SyntheticSoakSummary> {
    if config.continuous_training {
        anyhow::ensure!(
            config.persistent_trainers,
            "synthetic continuous training requires persistent_trainers = true"
        );
    }
    let started_at = Instant::now();
    if config.root.exists() {
        fs::remove_dir_all(&config.root)?;
    }
    fs::create_dir_all(&config.root)?;
    let dataset_root = config.root.join("dataset");
    match config.workload_kind {
        SyntheticWorkloadKind::Scalar => create_synthetic_runtime_dataset(&dataset_root)?,
        SyntheticWorkloadKind::BurnLinear => {
            #[cfg(feature = "native-gpu-probe")]
            {
                let _ = create_synthetic_burn_dataset(&dataset_root)?;
            }
            #[cfg(not(feature = "native-gpu-probe"))]
            {
                anyhow::bail!(
                    "SyntheticWorkloadKind::BurnLinear requires burn_p2p_testkit built with --features native-gpu-probe"
                );
            }
        }
    }

    let validator_report_path = config.root.join("validator-report.json");
    let validator_config_path = config.root.join("validator.json");
    let validator_shutdown = config.root.join("validator.stop");
    let mut validator_config = SyntheticProcessConfig::validator(
        config.root.join("validator-storage"),
        dataset_root.clone(),
        validator_report_path.clone(),
    );
    validator_config.workload_kind = config.workload_kind;
    validator_config.native_backend = config.validator_backend;
    validator_config.shutdown_sentinel = Some(validator_shutdown.clone());
    validator_config.startup_timeout_secs = config.startup_timeout_secs;
    validator_config.poll_interval_ms = config.poll_interval_ms;
    validator_config.sync_timeout_secs = config.sync_timeout_secs;
    validator_config.merge_wait_timeout_secs = config.merge_wait_timeout_secs;
    fs::write(
        &validator_config_path,
        serde_json::to_vec_pretty(&validator_config)?,
    )?;

    let mut validator = SyntheticProcessChild::spawn(node_binary, &validator_config_path)?;
    let validator_ready = wait_for_process_report(
        &validator_report_path,
        Duration::from_secs(config.startup_timeout_secs),
        |report| report.initialized_head_id.is_some() && !report.listen_addresses.is_empty(),
    )?;
    let validator_addr = SwarmAddress::new(validator_ready.listen_addresses[0].clone())?;

    let mut trainer_reports = Vec::new();
    let round_count = config.trainer_window_count.max(1);
    let trainer_count = config.trainer_count.max(1);
    let mut completed_rounds = 0_u32;
    let mut validator_report = validator_ready;

    if config.persistent_trainers {
        let mut trainers = Vec::new();
        let start_sentinel = config.root.join("trainers.start");
        let shutdown_sentinel = config.root.join("trainers.stop");
        for index in 0..trainer_count {
            let report_path = config.root.join(format!("trainer-{index}-report.json"));
            let config_path = config.root.join(format!("trainer-{index}.json"));
            let mut trainer_config = SyntheticProcessConfig::trainer(
                config.root.join(format!("trainer-{index}-storage")),
                dataset_root.clone(),
                report_path.clone(),
                vec![validator_addr.clone()],
            );
            trainer_config.workload_kind = config.workload_kind;
            trainer_config.native_backend = config.trainer_backend;
            trainer_config.start_sentinel = Some(start_sentinel.clone());
            trainer_config.shutdown_sentinel = Some(shutdown_sentinel.clone());
            trainer_config.persist_identity = true;
            trainer_config.startup_timeout_secs = config.startup_timeout_secs;
            trainer_config.poll_interval_ms = config.poll_interval_ms;
            trainer_config.sync_timeout_secs = config.sync_timeout_secs;
            trainer_config.merge_wait_timeout_secs = config.merge_wait_timeout_secs;
            trainer_config.canonical_advance_required = config.canonical_advance_required;
            trainer_config.continuous_training = config.continuous_training;
            if config.continuous_training {
                trainer_config.wait_for_canonical_advance = false;
            }
            trainer_config.window_count = round_count;
            trainer_config.learning_rate =
                synthetic_trainer_learning_rate(config.workload_kind, index, trainer_count);
            fs::write(&config_path, serde_json::to_vec_pretty(&trainer_config)?)?;
            trainers.push((
                report_path,
                SyntheticProcessChild::spawn(node_binary, &config_path)?,
            ));
        }

        for (report_path, _) in &trainers {
            let _ = wait_for_process_report(
                report_path,
                Duration::from_secs(config.sync_timeout_secs.max(5)),
                |report| report.synced_head_id.is_some(),
            )?;
        }
        fs::write(&start_sentinel, b"start")?;

        for (report_path, _) in &trainers {
            let _ = wait_for_process_report(
                report_path,
                Duration::from_secs(config.merge_wait_timeout_secs.max(5) * 2),
                |report| trainer_finished_windows(report, round_count),
            )?;
        }

        if config.continuous_training {
            validator_report = wait_for_stable_process_report(
                &validator_report_path,
                Duration::from_secs(config.merge_wait_timeout_secs.max(5) * 2),
                Duration::from_millis(config.poll_interval_ms.max(50) * 8),
                |report| report.merge_count >= 1 && report.receipt_count >= 1,
            )?;
        } else {
            validator_report = wait_for_process_report(
                &validator_report_path,
                Duration::from_secs(config.merge_wait_timeout_secs.max(5) * 2),
                |report| validator_completed_synthetic_rounds(report, round_count),
            )?;
        }

        fs::write(&shutdown_sentinel, b"stop")?;
        for (report_path, child) in &mut trainers {
            child.wait_success()?;
            trainer_reports.push(read_process_report(report_path)?);
        }
        completed_rounds = round_count;
    } else {
        for round in 0..round_count {
            let mut trainers = Vec::new();
            let round_start_sentinel = config.root.join(format!("round-{round}.start"));
            for index in 0..trainer_count {
                let report_path = config
                    .root
                    .join(format!("trainer-{index}-round-{round}-report.json"));
                let config_path = config
                    .root
                    .join(format!("trainer-{index}-round-{round}.json"));
                let mut trainer_config = SyntheticProcessConfig::trainer(
                    config.root.join(format!("trainer-{index}-storage")),
                    dataset_root.clone(),
                    report_path.clone(),
                    vec![validator_addr.clone()],
                );
                trainer_config.workload_kind = config.workload_kind;
                trainer_config.native_backend = config.trainer_backend;
                trainer_config.start_sentinel = Some(round_start_sentinel.clone());
                // Non-persistent trainers restart each round; fresh peer IDs avoid stale provider
                // records and dead connections from previous process lifetimes.
                trainer_config.persist_identity = false;
                trainer_config.startup_timeout_secs = config.startup_timeout_secs;
                trainer_config.poll_interval_ms = config.poll_interval_ms;
                trainer_config.sync_timeout_secs = config.sync_timeout_secs;
                trainer_config.merge_wait_timeout_secs = config.merge_wait_timeout_secs;
                trainer_config.canonical_advance_required = config.canonical_advance_required;
                trainer_config.continuous_training = config.continuous_training;
                if config.continuous_training {
                    trainer_config.wait_for_canonical_advance = false;
                }
                trainer_config.window_count = 1;
                trainer_config.learning_rate =
                    synthetic_trainer_learning_rate(config.workload_kind, index, trainer_count);
                fs::write(&config_path, serde_json::to_vec_pretty(&trainer_config)?)?;
                trainers.push((
                    report_path,
                    SyntheticProcessChild::spawn(node_binary, &config_path)?,
                ));
            }

            for (report_path, _) in &trainers {
                let _ = wait_for_process_report(
                    report_path,
                    Duration::from_secs(config.sync_timeout_secs.max(5)),
                    |report| report.synced_head_id.is_some(),
                )?;
            }
            fs::write(&round_start_sentinel, b"start")?;

            for (report_path, child) in &mut trainers {
                child.wait_success()?;
                trainer_reports.push(read_process_report(report_path)?);
            }

            let expected_merges = (round + 1) as usize;
            let expected_receipts = expected_merges;
            validator_report = wait_for_process_report(
                &validator_report_path,
                Duration::from_secs(config.merge_wait_timeout_secs.max(5) * 2),
                |report| {
                    report.receipt_count >= expected_receipts
                        && validator_completed_synthetic_rounds(report, expected_merges as u32)
                },
            )?;
            completed_rounds = round + 1;
        }
    }

    fs::write(&validator_shutdown, b"stop")?;
    validator.wait_success()?;
    let validator_report = read_process_report(&validator_report_path).unwrap_or(validator_report);

    let mut summary = SyntheticSoakSummary {
        workload_kind: config.workload_kind,
        trainer_backend: config.trainer_backend,
        validator_backend: config.validator_backend,
        trainer_count: config.trainer_count,
        trainer_window_count: config.trainer_window_count,
        continuous_training: config.continuous_training,
        completed_rounds,
        trainer_process_count: trainer_reports.len(),
        total_receipts: validator_report.receipt_count,
        total_merges: validator_report.merge_count,
        elapsed_millis: started_at.elapsed().as_millis(),
        validator_report,
        trainer_reports,
        performance_summary: None,
        dynamics_summary: None,
        root: config.root.clone(),
    };
    summary.performance_summary = derive_synthetic_soak_performance_summary(&summary)?;
    summary.dynamics_summary = derive_synthetic_network_dynamics_summary(&summary);

    Ok(summary)
}

fn trainer_finished_windows(report: &SyntheticProcessReport, expected_windows: u32) -> bool {
    report
        .window_timelines
        .iter()
        .filter(|timeline| timeline.trained_at.is_some())
        .count()
        >= expected_windows as usize
}

fn validator_completed_synthetic_rounds(
    report: &SyntheticProcessReport,
    expected_rounds: u32,
) -> bool {
    let expected_rounds = expected_rounds as usize;
    if synthetic_merged_head_window(report).is_some_and(|window| window >= expected_rounds) {
        return report.merge_count > 0 && report.receipt_count > 0;
    }

    report.merge_count >= expected_rounds && report.receipt_count >= expected_rounds
}

fn synthetic_merged_head_window(report: &SyntheticProcessReport) -> Option<usize> {
    let head_id = report
        .merged_head_id
        .as_deref()
        .or(report.observed_canonical_head_id.as_deref())?;
    head_id.rsplit_once("-window-")?.1.parse().ok()
}

struct SyntheticProcessChild {
    child: Child,
}

impl SyntheticProcessChild {
    fn spawn(node_binary: &Path, config_path: &Path) -> anyhow::Result<Self> {
        let child = Command::new(node_binary)
            .arg(config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(Self { child })
    }

    fn wait_success(&mut self) -> anyhow::Result<()> {
        let status = self.child.wait()?;
        anyhow::ensure!(status.success(), "child exited with status {status}");
        Ok(())
    }
}

impl Drop for SyntheticProcessChild {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn wait_for_process_report(
    path: &Path,
    timeout: Duration,
    predicate: impl Fn(&SyntheticProcessReport) -> bool,
) -> anyhow::Result<SyntheticProcessReport> {
    let deadline = Instant::now() + timeout;
    let mut latest_report = None;
    let mut last_read_error = None;
    while Instant::now() < deadline {
        if path.exists() {
            match read_process_report(path) {
                Ok(report) if predicate(&report) => return Ok(report),
                Ok(report) => latest_report = Some(report),
                Err(error) => last_read_error = Some(error.to_string()),
            }
        }
        thread::sleep(Duration::from_millis(25));
    }

    Err(process_report_timeout_error(
        "timed out waiting for process report",
        path,
        latest_report.as_ref(),
        last_read_error.as_deref(),
    ))
}

fn wait_for_stable_process_report(
    path: &Path,
    timeout: Duration,
    quiet_period: Duration,
    predicate: impl Fn(&SyntheticProcessReport) -> bool,
) -> anyhow::Result<SyntheticProcessReport> {
    let deadline = Instant::now() + timeout;
    let mut latest_match = None::<SyntheticProcessReport>;
    let mut last_progress_at = None::<Instant>;
    let mut last_read_error = None;
    while Instant::now() < deadline {
        if path.exists() {
            match read_process_report(path) {
                Ok(report) => {
                    let progressed = latest_match.as_ref().is_none_or(|previous| {
                        previous.receipt_count != report.receipt_count
                            || previous.merge_count != report.merge_count
                            || previous.merged_head_id != report.merged_head_id
                    });
                    if progressed {
                        last_progress_at = Some(Instant::now());
                        latest_match = Some(report.clone());
                    } else if latest_match.is_none() {
                        latest_match = Some(report.clone());
                    }
                    if predicate(&report)
                        && last_progress_at
                            .is_some_and(|progress| progress.elapsed() >= quiet_period)
                    {
                        return Ok(report);
                    }
                }
                Err(error) => last_read_error = Some(error.to_string()),
            }
        }
        thread::sleep(Duration::from_millis(25));
    }

    if let Some(report) = latest_match.as_ref()
        && predicate(&report)
    {
        return Ok(report.clone());
    }

    Err(process_report_timeout_error(
        "timed out waiting for stable process report",
        path,
        latest_match.as_ref(),
        last_read_error.as_deref(),
    ))
}

fn read_process_report(path: &Path) -> anyhow::Result<SyntheticProcessReport> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn process_report_timeout_error(
    prefix: &str,
    path: &Path,
    latest_report: Option<&SyntheticProcessReport>,
    last_read_error: Option<&str>,
) -> anyhow::Error {
    let mut message = format!("{prefix} {}", path.display());
    if let Some(report) = latest_report {
        message.push_str("; last observed report: ");
        message.push_str(&format_process_report_progress(report));
    } else {
        message.push_str("; no complete report was observed");
    }
    if let Some(error) = last_read_error {
        message.push_str("; last read error: ");
        message.push_str(error);
    }
    anyhow::anyhow!(message)
}

fn format_process_report_progress(report: &SyntheticProcessReport) -> String {
    let timelines = report
        .window_timelines
        .iter()
        .map(|timeline| {
            format!(
                "#{}:sync={} train={} advanced={} timed_out={}",
                timeline.window_index,
                timeline.synced_at.is_some(),
                timeline.trained_at.is_some(),
                timeline.canonical_advanced_at.is_some(),
                timeline.canonical_observation_timed_out
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "status={} synced={} trained={} parent={} observed={} merged={} receipts={} merges={} completed={} connected_peers={} visible_heads={} visible_updates={} error={} timelines=[{}]",
        report.status,
        report.synced_head_id.as_deref().unwrap_or("-"),
        report.trained_head_id.as_deref().unwrap_or("-"),
        report.trained_parent_head_id.as_deref().unwrap_or("-"),
        report.observed_canonical_head_id.as_deref().unwrap_or("-"),
        report.merged_head_id.as_deref().unwrap_or("-"),
        report.receipt_count,
        report.merge_count,
        report.completed_at.is_some(),
        report.connected_peer_count,
        report.visible_experiment_head_count,
        report.visible_experiment_update_count,
        report.error.as_deref().unwrap_or("-"),
        timelines,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_report_timeout_summary_includes_progress_fields() {
        let report = SyntheticProcessReport {
            status: "running".into(),
            synced_head_id: Some("head-0".into()),
            trained_head_id: Some("trainer-window-2".into()),
            trained_parent_head_id: Some("head-0".into()),
            observed_canonical_head_id: Some("merged-window-1".into()),
            receipt_count: 2,
            merge_count: 1,
            connected_peer_count: 3,
            visible_experiment_head_count: 4,
            visible_experiment_update_count: 5,
            window_timelines: vec![SyntheticWindowTimeline {
                window_index: 0,
                synced_at: Some(Utc::now()),
                trained_at: Some(Utc::now()),
                canonical_advanced_at: None,
                canonical_observation_timed_out: true,
            }],
            ..SyntheticProcessReport::default()
        };

        let summary = format_process_report_progress(&report);
        assert!(summary.contains("status=running"));
        assert!(summary.contains("trained=trainer-window-2"));
        assert!(summary.contains("parent=head-0"));
        assert!(summary.contains("observed=merged-window-1"));
        assert!(summary.contains("receipts=2"));
        assert!(summary.contains("merges=1"));
        assert!(summary.contains("#0:sync=true train=true advanced=false timed_out=true"));
    }
}
