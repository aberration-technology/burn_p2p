use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::Context;
use burn_p2p::compat::{P2pProject, RuntimeProject};
use burn_p2p::{
    ArtifactBuildSpec, ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard,
    CapabilityEstimate, ChunkingScheme, DatasetRegistration, DatasetSizing, EvalSplit,
    FsArtifactStore, GenesisSpec, MainnetHandle, MetricReport, MetricValue, MicroShardPlanner,
    MicroShardPlannerConfig, NodeBuilder, PatchOutcome, PatchSupport, PeerRole, PeerRoleSet,
    ProjectBackend, RuntimePatch, ShardFetchManifest, StorageConfig, SwarmAddress, TrainError,
    UpstreamAdapter, WindowCtx, WindowReport,
};
use chrono::Utc;
use semver::Version;
use serde::{Deserialize, Serialize};

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
    /// The scope.
    pub scope: SyntheticExperimentScope,
    /// The storage root.
    pub storage_root: PathBuf,
    /// The dataset root.
    pub dataset_root: PathBuf,
    /// The report path.
    pub report_path: PathBuf,
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
            scope: SyntheticExperimentScope::default(),
            storage_root: storage_root.into(),
            dataset_root: dataset_root.into(),
            report_path: report_path.into(),
            shutdown_sentinel: None,
            bootstrap_peers: Vec::new(),
            listen_addresses: Vec::new(),
            persist_identity: true,
            learning_rate: 1.0,
            target_model: 10.0,
            startup_timeout_secs: 15,
            poll_interval_ms: 50,
            sync_timeout_secs: 15,
            merge_wait_timeout_secs: 15,
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
            scope: SyntheticExperimentScope::default(),
            storage_root: storage_root.into(),
            dataset_root: dataset_root.into(),
            report_path: report_path.into(),
            shutdown_sentinel: None,
            bootstrap_peers,
            listen_addresses: Vec::new(),
            persist_identity: false,
            learning_rate: 1.0,
            target_model: 10.0,
            startup_timeout_secs: 15,
            poll_interval_ms: 50,
            sync_timeout_secs: 15,
            merge_wait_timeout_secs: 15,
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
    /// The error.
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures synthetic soak.
pub struct SyntheticSoakConfig {
    /// The root.
    pub root: PathBuf,
    /// The trainer count.
    pub trainer_count: u32,
    /// The trainer window count.
    pub trainer_window_count: u32,
    /// The startup timeout secs.
    pub startup_timeout_secs: u64,
    /// The poll interval ms.
    pub poll_interval_ms: u64,
    /// The sync timeout secs.
    pub sync_timeout_secs: u64,
    /// The merge wait timeout secs.
    pub merge_wait_timeout_secs: u64,
}

impl Default for SyntheticSoakConfig {
    fn default() -> Self {
        Self {
            root: std::env::temp_dir().join(format!(
                "burn-p2p-soak-{}",
                Utc::now().timestamp_nanos_opt().unwrap_or_default()
            )),
            trainer_count: 4,
            trainer_window_count: 3,
            startup_timeout_secs: 15,
            poll_interval_ms: 50,
            sync_timeout_secs: 15,
            merge_wait_timeout_secs: 15,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Summarizes the synthetic soak.
pub struct SyntheticSoakSummary {
    /// The validator report.
    pub validator_report: SyntheticProcessReport,
    /// The trainer reports.
    pub trainer_reports: Vec<SyntheticProcessReport>,
    /// The trainer count.
    pub trainer_count: u32,
    /// The trainer window count.
    pub trainer_window_count: u32,
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
    /// The root.
    pub root: PathBuf,
}

#[derive(Clone, Debug)]
struct SyntheticRuntimeBackend;

impl ProjectBackend for SyntheticRuntimeBackend {
    type Device = String;
}

#[derive(Clone, Debug)]
struct SyntheticRuntimeProject {
    dataset_root: PathBuf,
    learning_rate: f64,
    target_model: f64,
}

impl P2pProject<SyntheticRuntimeBackend> for SyntheticRuntimeProject {
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
}

impl RuntimeProject<SyntheticRuntimeBackend> for SyntheticRuntimeProject {
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
}

/// Performs the create synthetic runtime dataset operation.
pub fn create_synthetic_runtime_dataset(root: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(root)?;
    let project = SyntheticRuntimeProject {
        dataset_root: root.to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
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

/// Performs the run synthetic process node operation.
pub fn run_synthetic_process_node(
    config: &SyntheticProcessConfig,
) -> anyhow::Result<SyntheticProcessReport> {
    if !config.dataset_root.join("fetch-manifest.json").exists() {
        create_synthetic_runtime_dataset(&config.dataset_root)?;
    }
    if let Some(parent) = config.report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(shutdown_sentinel) = &config.shutdown_sentinel
        && let Some(parent) = shutdown_sentinel.parent()
    {
        fs::create_dir_all(parent)?;
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
    let experiment = MainnetHandle {
        genesis: genesis.clone(),
        roles: roles.clone(),
    }
    .experiment(
        burn_p2p::StudyId::new(config.scope.study_id.clone()),
        burn_p2p::ExperimentId::new(config.scope.experiment_id.clone()),
        burn_p2p::RevisionId::new(config.scope.revision_id.clone()),
    );
    let mut builder = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: config.dataset_root.clone(),
        learning_rate: config.learning_rate,
        target_model: config.target_model,
    })
    .with_mainnet(genesis)
    .with_roles(roles)
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
    report.status = "running".into();
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

    let shutdown_result = node.shutdown();
    let await_result = node.await_termination();
    match (result, shutdown_result, await_result) {
        (Ok(()), Ok(()), Ok(_)) => {
            report.status = "completed".into();
            refresh_report_counts(&mut report, &config.storage_root)?;
            write_report(&config.report_path, &report)?;
            Ok(report)
        }
        (Err(error), _, _) => {
            report.status = "failed".into();
            report.error = Some(error.to_string());
            refresh_report_counts(&mut report, &config.storage_root)?;
            write_report(&config.report_path, &report)?;
            Err(error)
        }
        (Ok(()), Err(error), _) => {
            report.status = "failed".into();
            report.error = Some(error.to_string());
            refresh_report_counts(&mut report, &config.storage_root)?;
            write_report(&config.report_path, &report)?;
            Err(error)
        }
        (Ok(()), Ok(()), Err(error)) => {
            report.status = "failed".into();
            report.error = Some(error.to_string());
            refresh_report_counts(&mut report, &config.storage_root)?;
            write_report(&config.report_path, &report)?;
            Err(error)
        }
    }
}

fn run_validator_process(
    config: &SyntheticProcessConfig,
    experiment: &burn_p2p::ExperimentHandle,
    node: &mut burn_p2p::RunningNode<SyntheticRuntimeProject>,
    report: &mut SyntheticProcessReport,
) -> anyhow::Result<()> {
    let head = node
        .restore_experiment_head(experiment)?
        .map(Ok)
        .unwrap_or_else(|| node.initialize_local_head::<SyntheticRuntimeBackend>(experiment))?;
    report.initialized_head_id = Some(head.head_id.to_string());
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

        match node.validate_candidates_once::<SyntheticRuntimeBackend>(experiment) {
            Ok(Some(outcome)) => {
                report.merged_head_id = Some(outcome.merged_head.head_id.to_string());
                report.observed_canonical_head_id = report.merged_head_id.clone();
                report.error = None;
                refresh_report_counts(report, &config.storage_root)?;
                write_report(&config.report_path, report)?;
            }
            Ok(None) => thread::sleep(poll_interval),
            Err(error) => {
                report.error = Some(error.to_string());
                write_report(&config.report_path, report)?;
                thread::sleep(poll_interval);
            }
        }
    }
}

fn run_trainer_process(
    config: &SyntheticProcessConfig,
    experiment: &burn_p2p::ExperimentHandle,
    node: &mut burn_p2p::RunningNode<SyntheticRuntimeProject>,
    report: &mut SyntheticProcessReport,
) -> anyhow::Result<()> {
    let poll_interval = Duration::from_millis(config.poll_interval_ms.max(10));
    let sync_timeout = Duration::from_secs(config.sync_timeout_secs);
    let merge_timeout = Duration::from_secs(config.merge_wait_timeout_secs);

    for _ in 0..config.window_count.max(1) {
        let synced_head = wait_for_synced_head(node, experiment, sync_timeout, poll_interval)?;
        report.synced_head_id = Some(synced_head.head_id.to_string());
        write_report(&config.report_path, report)?;

        let outcome = train_window_once_with_retry(node, experiment, sync_timeout, poll_interval)?;
        report.trained_head_id = Some(outcome.head.head_id.to_string());
        report.trained_parent_head_id = outcome
            .head
            .parent_head_id
            .map(|head_id| head_id.to_string());
        refresh_report_counts(report, &config.storage_root)?;
        write_report(&config.report_path, report)?;

        let observed_head = wait_for_canonical_advance(
            node,
            experiment,
            &synced_head.head_id,
            merge_timeout,
            poll_interval,
            report,
            ReportPaths {
                report_path: &config.report_path,
                storage_root: &config.storage_root,
            },
        )?;
        report.observed_canonical_head_id = Some(observed_head.head_id.to_string());
    }

    Ok(())
}

fn wait_for_synced_head(
    node: &burn_p2p::RunningNode<SyntheticRuntimeProject>,
    experiment: &burn_p2p::ExperimentHandle,
    timeout: Duration,
    poll_interval: Duration,
) -> anyhow::Result<burn_p2p::HeadDescriptor> {
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

fn train_window_once_with_retry(
    node: &mut burn_p2p::RunningNode<SyntheticRuntimeProject>,
    experiment: &burn_p2p::ExperimentHandle,
    timeout: Duration,
    poll_interval: Duration,
) -> anyhow::Result<burn_p2p::TrainingWindowOutcome<BTreeMap<String, MetricValue>>> {
    let deadline = Instant::now() + timeout;
    let mut last_transient_error = None;
    while Instant::now() < deadline {
        match node.train_window_once::<SyntheticRuntimeBackend>(experiment) {
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

fn is_transient_runtime_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("request failure")
        || message.contains("Failed to dial")
        || message.contains("timed out")
        || message.contains("no connected peer provided artifact")
}

struct ReportPaths<'a> {
    report_path: &'a Path,
    storage_root: &'a Path,
}

fn wait_for_canonical_advance(
    node: &burn_p2p::RunningNode<SyntheticRuntimeProject>,
    experiment: &burn_p2p::ExperimentHandle,
    base_head_id: &burn_p2p::HeadId,
    timeout: Duration,
    poll_interval: Duration,
    report: &mut SyntheticProcessReport,
    paths: ReportPaths<'_>,
) -> anyhow::Result<burn_p2p::HeadDescriptor> {
    let deadline = Instant::now() + timeout;
    let mut last_observed_head = None;
    while Instant::now() < deadline {
        match node.sync_experiment_head(experiment) {
            Ok(Some(head)) => {
                last_observed_head = Some(head.clone());
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

    refresh_report_counts(report, paths.storage_root)?;
    if report.receipt_count >= 1 {
        let serve_grace = timeout.max(Duration::from_secs(5));
        let serve_deadline = Instant::now() + serve_grace;
        while Instant::now() < serve_deadline {
            match node.sync_experiment_head(experiment) {
                Ok(Some(head)) => {
                    last_observed_head = Some(head.clone());
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

        if let Some(head) = last_observed_head {
            return Ok(head);
        }
        if let Ok(Some(head)) = node.sync_experiment_head(experiment) {
            report.observed_canonical_head_id = Some(head.head_id.to_string());
            report.error = None;
            refresh_report_counts(report, paths.storage_root)?;
            write_report(paths.report_path, report)?;
            return Ok(head);
        }
    }

    Err(anyhow::anyhow!(
        "timed out waiting for canonical head to advance from {}",
        base_head_id.as_str()
    ))
}

fn wait_for(
    timeout: Duration,
    condition: impl Fn() -> bool,
    failure_message: &str,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }

    Err(anyhow::anyhow!(failure_message.to_owned()))
}

fn write_report(path: &Path, report: &SyntheticProcessReport) -> anyhow::Result<()> {
    fs::write(path, serde_json::to_vec_pretty(report)?)
        .with_context(|| format!("failed to write {}", path.display()))
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

/// Performs the run synthetic process soak operation.
pub fn run_synthetic_process_soak(
    config: &SyntheticSoakConfig,
    node_binary: &Path,
) -> anyhow::Result<SyntheticSoakSummary> {
    let started_at = Instant::now();
    if config.root.exists() {
        fs::remove_dir_all(&config.root)?;
    }
    fs::create_dir_all(&config.root)?;
    let dataset_root = config.root.join("dataset");
    create_synthetic_runtime_dataset(&dataset_root)?;

    let validator_report_path = config.root.join("validator-report.json");
    let validator_config_path = config.root.join("validator.json");
    let validator_shutdown = config.root.join("validator.stop");
    let mut validator_config = SyntheticProcessConfig::validator(
        config.root.join("validator-storage"),
        dataset_root.clone(),
        validator_report_path.clone(),
    );
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

    for round in 0..round_count {
        let mut trainers = Vec::new();
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
            trainer_config.persist_identity = true;
            trainer_config.startup_timeout_secs = config.startup_timeout_secs;
            trainer_config.poll_interval_ms = config.poll_interval_ms;
            trainer_config.sync_timeout_secs = config.sync_timeout_secs;
            trainer_config.merge_wait_timeout_secs = config.merge_wait_timeout_secs;
            trainer_config.window_count = 1;
            trainer_config.learning_rate = if index + 1 == trainer_count {
                1.0
            } else {
                ((index + 1) as f64) / ((trainer_count + 1) as f64)
            };
            fs::write(&config_path, serde_json::to_vec_pretty(&trainer_config)?)?;
            trainers.push((
                report_path,
                SyntheticProcessChild::spawn(node_binary, &config_path)?,
            ));
        }

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
                report.merge_count >= expected_merges && report.receipt_count >= expected_receipts
            },
        )?;
        completed_rounds = round + 1;
    }

    fs::write(&validator_shutdown, b"stop")?;
    validator.wait_success()?;
    let validator_report = read_process_report(&validator_report_path).unwrap_or(validator_report);

    Ok(SyntheticSoakSummary {
        trainer_count: config.trainer_count,
        trainer_window_count: config.trainer_window_count,
        completed_rounds,
        trainer_process_count: trainer_reports.len(),
        total_receipts: validator_report.receipt_count,
        total_merges: validator_report.merge_count,
        elapsed_millis: started_at.elapsed().as_millis(),
        validator_report,
        trainer_reports,
        root: config.root.clone(),
    })
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
    while Instant::now() < deadline {
        if path.exists() {
            match read_process_report(path) {
                Ok(report) if predicate(&report) => return Ok(report),
                Ok(_) => {}
                Err(_) => {}
            }
        }
        thread::sleep(Duration::from_millis(25));
    }

    Err(anyhow::anyhow!(
        "timed out waiting for process report {}",
        path.display()
    ))
}

fn read_process_report(path: &Path) -> anyhow::Result<SyntheticProcessReport> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
