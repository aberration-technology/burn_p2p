use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use burn::{
    backend::{Autodiff, NdArray},
    optim::AdamConfig,
    train::{InferenceStep, Learner},
};
use burn_p2p::{
    AuthConfig, BrowserRolePolicy, BrowserVisibilityPolicy, ChunkingScheme, ClientPlatform,
    ClientReleaseManifest, ContentId, ExperimentDirectoryEntry, ExperimentDirectoryPolicyExt,
    ExperimentId, ExperimentOptInPolicy, ExperimentResourceRequirements, ExperimentScope,
    ExperimentVisibility, HeadDescriptor, HeadEvalReport, HeadEvalStatus, HeadPromotionMode,
    HeadPromotionPolicy, LagPolicy, MergeCertificate, MergeStrategy, MergeTopologyPolicy,
    MetricTrustClass, MetricValue, NetworkManifest, P2pWorkload, PeerId, PeerRole, PeerRoleSet,
    PeerWindowMetrics, PeerWindowStatus, Precision, PrincipalId, ProjectFamilyId,
    ReducerCohortMetrics, RevisionId, RevisionManifest, StorageConfig, StudyId,
    SupportedWorkload, WindowActivation, WindowId, WorkloadId, DiffusionSteadyStatePolicy,
    burn::{
        BurnArtifactConfig, BurnRecordPrecision, BurnTarget, BurnWorkloadConfig, from_learner,
        from_loaders, inspect_module,
    },
};
use burn_p2p_core::{
    BackendClass, BrowserLeaderboardEntry, BrowserLeaderboardIdentity, BrowserSeedRecord,
};
use burn_p2p_metrics::{MetricsIndexerConfig, MetricsStore};
use chrono::Utc;
use semver::Version;

use crate::{
    args::{Args, MnistPromotionMode},
    correctness::export::{DemoPhaseEvent, DemoPhaseSummary, DemoPhaseTiming},
    correctness::live_browser::{
        LiveBrowserEdgeConfig, LiveBrowserProbeManifest, materialize_live_browser_dataset_bundle,
        materialize_live_browser_head_artifact_bundle, prepare_live_browser_dataset_transport,
        prepare_live_browser_head_artifact_transport, write_live_browser_edge_config,
    },
    data::{MnistDatasetConfig, PreparedMnistData, prepare_mnist_dataset},
    model::{MnistMetricItem, MnistModel},
};

type RuntimeBackend = Autodiff<NdArray<f32>>;
type EvalBackend = NdArray<f32>;

const PROJECT_FAMILY: &str = "mnist-demo-family";
const NETWORK_ID: &str = "mnist-demo-net";
const STUDY_ID: &str = "mnist-study";
const BASELINE_EXPERIMENT: &str = "mnist-baseline";
const LOW_LR_EXPERIMENT: &str = "mnist-low-lr";
const BASELINE_REVISION: &str = "rev-baseline";
const LOW_LR_REVISION: &str = "rev-low-lr";
const WORKLOAD_ID: &str = "mnist-classifier";
const HELPER_LABEL: &str = "helper";
const VALIDATOR_LABEL: &str = "validator";
const VALIDATOR_B_LABEL: &str = "validator-b";
const REDUCER_LABEL: &str = "reducer";
const VIEWER_LABEL: &str = "viewer";
const TRAINER_A1_LABEL: &str = "trainer-a1";
const TRAINER_A2_LABEL: &str = "trainer-a2";
const TRAINER_A3_LABEL: &str = "trainer-a3";
const TRAINER_B_LABEL: &str = "trainer-b";
const TRAINER_LATE_LABEL: &str = "trainer-late";
const BASELINE_LEARNING_RATE: f64 = 1.0e-3;
const LOW_LR_LEARNING_RATE: f64 = 2.0e-4;
const BOUNDED_CI_LOW_LR_LEARNING_RATE: f64 = 1.0e-4;
const TOPOLOGY_HEAD_SYNC_TIMEOUT: Duration = Duration::from_secs(45);
const FOLLOWER_HEAD_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(45);
const NODE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const DEMO_VALIDATION_ROUND_TIMEOUT: Duration = Duration::from_secs(45);
const RESTART_CANDIDATE_ARTIFACT_TIMEOUT: Duration = Duration::from_secs(90);
const BOUNDED_CI_MNIST_ENV: &str = "BURN_P2P_BOUNDED_CI_MNIST";

pub(crate) struct CoreNodeRecord {
    pub label: String,
    pub peer_id: PeerId,
    pub roles: Vec<String>,
    pub storage_root: PathBuf,
}

pub(crate) struct CoreMnistRun {
    pub promotion_mode: HeadPromotionMode,
    pub prepared_data: PreparedMnistData,
    pub network_manifest: NetworkManifest,
    pub release_manifest: ClientReleaseManifest,
    pub supported_workload: SupportedWorkload,
    pub baseline_genesis: HeadDescriptor,
    pub baseline_head: HeadDescriptor,
    pub low_lr_genesis: HeadDescriptor,
    pub low_lr_head: HeadDescriptor,
    pub baseline_outcomes: Vec<TrainingRecord>,
    pub low_lr_outcomes: Vec<TrainingRecord>,
    pub merge_certificates: Vec<MergeCertificate>,
    pub metrics_catchup: Vec<burn_p2p_metrics::MetricsCatchupBundle>,
    pub peer_window_metrics: Vec<PeerWindowMetrics>,
    pub reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    pub head_eval_reports: Vec<HeadEvalReport>,
    pub node_records: Vec<CoreNodeRecord>,
    pub bootstrap_plan: BTreeMap<String, Vec<String>>,
    pub bootstrap_seed_label: String,
    pub dedicated_reducer_label: String,
    pub validator_labels: Vec<String>,
    pub final_snapshots: BTreeMap<String, burn_p2p::NodeTelemetrySnapshot>,
    pub restarted_trainer_label: String,
    pub resilience_drills_executed: bool,
    pub trainer_restart_reconnected: bool,
    pub trainer_restart_resumed_training: bool,
    pub late_joiner_synced_checkpoint: bool,
    pub phase_timeline: DemoPhaseSummary,
    pub browser_probe_manifest: Option<LiveBrowserProbeManifest>,
    pub browser_probe_summary: Option<serde_json::Value>,
}

pub(crate) struct DynamicsSummaryInput<'a> {
    pub baseline_outcomes: &'a [TrainingRecord],
    pub low_lr_outcomes: &'a [TrainingRecord],
    pub baseline_initial_accuracy: f64,
    pub baseline_accuracy: f64,
    pub low_lr_initial_accuracy: f64,
    pub low_lr_accuracy: f64,
    pub prepared_data: &'a PreparedMnistData,
    pub performance: crate::correctness::export::MnistPerformanceSummary,
}

#[derive(Clone)]
struct DemoValidationResult {
    merged_head: HeadDescriptor,
    merge_certificate: MergeCertificate,
    evaluation: burn_p2p::MetricReport,
}

struct CoreDemoContext {
    output: PathBuf,
    storage_root: PathBuf,
    prepared_data: PreparedMnistData,
    supported_workload: SupportedWorkload,
    burn_workload_config: BurnWorkloadConfig,
    release_manifest: ClientReleaseManifest,
    network_manifest: NetworkManifest,
    low_lr_learning_rate: f64,
}

struct MnistDirectoryEntrySpec<'a> {
    experiment_id: ExperimentId,
    revision_id: RevisionId,
    current_head_id: Option<burn_p2p::HeadId>,
    display_name: &'a str,
    learning_rate: &'a str,
}

struct BrowserProbeWriteInput<'a, P> {
    output: &'a Path,
    storage_root: &'a Path,
    prepared_data: &'a PreparedMnistData,
    supported_workload: &'a SupportedWorkload,
    release_manifest: &'a ClientReleaseManifest,
    network_manifest: &'a NetworkManifest,
    promotion_mode: HeadPromotionMode,
    baseline_genesis: &'a HeadDescriptor,
    baseline_head: &'a HeadDescriptor,
    low_lr_genesis: &'a HeadDescriptor,
    low_lr_head: &'a HeadDescriptor,
    baseline_outcomes: &'a [TrainingRecord],
    low_lr_outcomes: &'a [TrainingRecord],
    merge_certificates: &'a [MergeCertificate],
    head_eval_reports: &'a [HeadEvalReport],
    browser_provider_label: &'a str,
    browser_provider: &'a burn_p2p::RunningNode<P>,
    head_artifact_provider: &'a burn_p2p::RunningNode<P>,
    browser_lease: &'a burn_p2p::AssignmentLease,
    browser_head_providers: &'a [PeerId],
    browser_nodes: &'a [(&'a str, &'a burn_p2p::RunningNode<P>)],
}

struct CollectedDemoArtifacts {
    node_records: Vec<CoreNodeRecord>,
    bootstrap_plan: BTreeMap<String, Vec<String>>,
    final_snapshots: BTreeMap<String, burn_p2p::NodeTelemetrySnapshot>,
    metrics_catchup: Vec<burn_p2p_metrics::MetricsCatchupBundle>,
    peer_window_metrics: Vec<PeerWindowMetrics>,
    reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    head_eval_reports: Vec<HeadEvalReport>,
}

pub(crate) fn run_core_demo(args: &Args) -> anyhow::Result<CoreMnistRun> {
    let output = args.output.clone();
    if output.exists() {
        std::fs::remove_dir_all(&output)
            .with_context(|| format!("failed to reset {}", output.display()))?;
    }
    std::fs::create_dir_all(&output)?;
    write_demo_phase(&output, "run-start")?;
    let dataset_root = output.join("dataset");
    let storage_root = output.join("nodes");
    std::fs::create_dir_all(&storage_root)?;

    let prepared_data = prepare_mnist_dataset(
        &dataset_root,
        &MnistDatasetConfig {
            train_examples_per_digit: args.train_examples_per_digit,
            eval_examples_per_digit: args.eval_examples_per_digit,
            microshard_count: args.microshards,
            batch_size: args.batch_size,
        },
    )?;
    let supported_workload = supported_workload()?;
    let burn_workload_config = mnist_burn_workload_config(supported_workload.clone());
    let release_manifest = release_manifest(&supported_workload);
    let network_manifest = network_manifest(&release_manifest);
    let low_lr_learning_rate = demo_low_lr_learning_rate();
    let promotion_mode = match args.promotion_mode {
        MnistPromotionMode::ValidatorQuorum => HeadPromotionMode::ValidatorQuorum,
        MnistPromotionMode::DiffusionSteadyState => HeadPromotionMode::DiffusionSteadyState,
    };

    let demo_context = CoreDemoContext {
        output,
        storage_root,
        prepared_data,
        supported_workload,
        burn_workload_config,
        release_manifest,
        network_manifest,
        low_lr_learning_rate,
    };

    if matches!(promotion_mode, HeadPromotionMode::DiffusionSteadyState) {
        return run_core_demo_diffusion(args, demo_context);
    }

    let CoreDemoContext {
        output,
        storage_root,
        prepared_data,
        supported_workload,
        burn_workload_config,
        release_manifest,
        network_manifest,
        low_lr_learning_rate,
    } = demo_context;

    let build_trainer_project = |learning_rate: f64| -> anyhow::Result<_> {
        let device = <RuntimeBackend as burn::tensor::backend::Backend>::Device::default();
        let prepared_for_eval = prepared_data.clone();
        let train_loader = prepared_data.build_train_loader(&device);
        let validation_loader = prepared_data.build_eval_loader(&device);
        Ok(from_loaders(
            Learner::new(
                MnistModel::<RuntimeBackend>::new(&device),
                AdamConfig::new().init(),
                learning_rate,
            ),
            device,
            train_loader,
            validation_loader,
        )
        .with_benchmark(|_model, _device| burn_p2p::CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 16.0,
            target_window_seconds: 1,
        })
        .with_sharded_dataset(
            prepared_data.train_dataset.clone(),
            crate::data::MnistBatcher,
            prepared_data.batch_size,
        )
        .with_evaluate(move |model, _split| evaluate_model(model, &prepared_for_eval))
        .with_step_metrics(|step_index, output, metrics| {
            accumulate_training_metrics(step_index, output, metrics);
            Ok(())
        })
        .with_window_metrics(|learner, metrics| {
            let inventory = inspect_module::<RuntimeBackend, _>(&learner.model());
            metrics.insert(
                "parameter_count".into(),
                MetricValue::Integer(inventory.parameter_count as i64),
            );
            Ok(())
        }))
    };
    let build_validator_project = |learning_rate: f64| -> anyhow::Result<_> {
        let device = <RuntimeBackend as burn::tensor::backend::Backend>::Device::default();
        let prepared_for_eval = prepared_data.clone();
        let validation_loader = prepared_data.build_eval_loader(&device);
        Ok(from_learner(
            Learner::new(
                MnistModel::<RuntimeBackend>::new(&device),
                AdamConfig::new().init(),
                learning_rate,
            ),
            device,
        )
        .with_benchmark(|_model, _device| burn_p2p::CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 16.0,
            target_window_seconds: 1,
        })
        .with_validation_loader(validation_loader)
        .with_evaluate(move |model, _split| evaluate_model(model, &prepared_for_eval)))
    };
    let build_service_project = |learning_rate: f64| -> anyhow::Result<_> {
        let device = <RuntimeBackend as burn::tensor::backend::Backend>::Device::default();
        Ok(from_learner(
            Learner::new(
                MnistModel::<RuntimeBackend>::new(&device),
                AdamConfig::new().init(),
                learning_rate,
            ),
            device,
        )
        .with_benchmark(|_model, _device| burn_p2p::CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 16.0,
            target_window_seconds: 1,
        }))
    };

    let helper = build_service_project(1.0e-3)?
        .connect_with_config(
            BurnTarget::Custom(PeerRoleSet::new([
                PeerRole::Bootstrap,
                PeerRole::RelayHelper,
            ])),
            release_manifest.clone(),
            burn_workload_config.clone(),
        )?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(HELPER_LABEL)))
        .spawn()?;
    wait_for(
        Duration::from_secs(10),
        || {
            let snapshot = helper.telemetry().snapshot();
            snapshot.status == burn_p2p::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
                && snapshot.local_peer_id.is_some()
        },
        "helper seed did not start",
    )?;
    let helper_addr = helper.telemetry().snapshot().listen_addresses[0].clone();

    let validator = build_validator_project(1.0e-3)?
        .validator_with_config(release_manifest.clone(), burn_workload_config.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(VALIDATOR_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;
    wait_for(
        Duration::from_secs(10),
        || {
            let snapshot = validator.telemetry().snapshot();
            snapshot.status == burn_p2p::RuntimeStatus::Running
                && snapshot.connected_peers >= 1
                && snapshot.local_peer_id.is_some()
        },
        "validator did not join the helper seed",
    )?;

    let reducer = build_validator_project(1.0e-3)?
        .connect_with_config(
            BurnTarget::Custom(PeerRoleSet::new([PeerRole::Reducer])),
            release_manifest.clone(),
            burn_workload_config.clone(),
        )?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(REDUCER_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;

    let validator_b = build_validator_project(1.0e-3)?
        .validator_with_config(release_manifest.clone(), burn_workload_config.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(VALIDATOR_B_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;

    wait_for(
        Duration::from_secs(15),
        || {
            let helper_snapshot = helper.telemetry().snapshot();
            let reducer_snapshot = reducer.telemetry().snapshot();
            let validator_b_snapshot = validator_b.telemetry().snapshot();
            helper_snapshot.connected_peers >= 3
                && reducer_snapshot.connected_peers >= 1
                && validator_b_snapshot.connected_peers >= 1
        },
        "seed/reducer/validator topology did not converge",
    )?;

    let viewer = build_service_project(1.0e-3)?
        .connect_with_config(
            BurnTarget::Custom(PeerRoleSet::new([
                PeerRole::Viewer,
                PeerRole::BrowserObserver,
            ])),
            release_manifest.clone(),
            burn_workload_config.clone(),
        )?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(VIEWER_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;

    let trainer_a1 = build_trainer_project(BASELINE_LEARNING_RATE)?
        .trainer_with_config(release_manifest.clone(), burn_workload_config.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(TRAINER_A1_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;
    let trainer_a2 = build_trainer_project(BASELINE_LEARNING_RATE)?
        .trainer_with_config(release_manifest.clone(), burn_workload_config.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(TRAINER_A2_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;
    let trainer_b = build_trainer_project(low_lr_learning_rate)?
        .trainer_with_config(release_manifest.clone(), burn_workload_config.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(TRAINER_B_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;

    wait_for(
        Duration::from_secs(20),
        || {
            validator.telemetry().snapshot().connected_peers >= 1
                && validator_b.telemetry().snapshot().connected_peers >= 1
                && reducer.telemetry().snapshot().connected_peers >= 1
                && trainer_a1.telemetry().snapshot().connected_peers >= 1
                && trainer_a2.telemetry().snapshot().connected_peers >= 1
                && trainer_b.telemetry().snapshot().connected_peers >= 1
                && viewer.telemetry().snapshot().connected_peers >= 1
        },
        "fleet did not converge on the helper-seeded reducer/validator network",
    )?;

    let helper = helper;
    let mut validator = validator;
    let mut validator_b = validator_b;
    let mut reducer = reducer;
    let viewer = viewer;
    let mut trainer_a1 = trainer_a1;
    let mut trainer_a2 = trainer_a2;
    let mut trainer_b = trainer_b;

    let baseline = validator.experiment(
        StudyId::new(STUDY_ID),
        ExperimentId::new(BASELINE_EXPERIMENT),
        RevisionId::new(BASELINE_REVISION),
    );
    let low_lr = validator.experiment(
        StudyId::new(STUDY_ID),
        ExperimentId::new(LOW_LR_EXPERIMENT),
        RevisionId::new(LOW_LR_REVISION),
    );

    let baseline_genesis = validator.initialize_local_head(&baseline)?;
    let low_lr_genesis = validator.initialize_local_head(&low_lr)?;
    let mut baseline_head = baseline_genesis.clone();
    let mut low_lr_head = low_lr_genesis.clone();
    let validator_peer_id = validator
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("validator missing local peer id")?;
    validator.publish_head_provider(&baseline, &baseline_head)?;
    validator.publish_head_provider(&low_lr, &low_lr_head)?;
    wait_for_artifact_from_provider(
        (VALIDATOR_LABEL, &validator),
        [(REDUCER_LABEL, &reducer), (VALIDATOR_B_LABEL, &validator_b)],
        &validator_peer_id,
        &baseline_head.artifact_id,
        demo_provider_artifact_timeout(),
        "baseline genesis head artifact was not fetchable from validator",
    )?;
    wait_for_artifact_from_provider(
        (VALIDATOR_LABEL, &validator),
        [(REDUCER_LABEL, &reducer), (VALIDATOR_B_LABEL, &validator_b)],
        &validator_peer_id,
        &low_lr_head.artifact_id,
        demo_provider_artifact_timeout(),
        "low-lr genesis head artifact was not fetchable from validator",
    )?;
    wait_for_specific_head(
        &reducer,
        &baseline,
        &baseline_head,
        Duration::from_secs(20),
        "reducer did not sync baseline genesis head",
    )?;
    wait_for_specific_head(
        &reducer,
        &low_lr,
        &low_lr_head,
        Duration::from_secs(20),
        "reducer did not sync low-lr genesis head",
    )?;
    wait_for_specific_head(
        &validator_b,
        &baseline,
        &baseline_head,
        Duration::from_secs(20),
        "validator-b did not sync baseline genesis head",
    )?;
    wait_for_specific_head(
        &validator_b,
        &low_lr,
        &low_lr_head,
        Duration::from_secs(20),
        "validator-b did not sync low-lr genesis head",
    )?;
    wait_for_specific_head(
        &trainer_a1,
        &baseline,
        &baseline_head,
        Duration::from_secs(20),
        "trainer-a1 did not sync baseline genesis head",
    )?;
    wait_for_specific_head(
        &trainer_a2,
        &baseline,
        &baseline_head,
        Duration::from_secs(20),
        "trainer-a2 did not sync baseline genesis head",
    )?;
    wait_for_specific_head(
        &trainer_b,
        &low_lr,
        &low_lr_head,
        Duration::from_secs(20),
        "trainer-b did not sync low-lr genesis head",
    )?;

    let mut baseline_outcomes = Vec::new();
    let mut low_lr_outcomes = Vec::new();
    let mut merge_certificates = Vec::<MergeCertificate>::new();
    let browser_probe_manifest = None;
    let browser_probe_summary = None;
    let restarted_trainer_label = TRAINER_A2_LABEL.to_string();
    let resilience_drills_required = require_resilience_drills();
    let mut head_eval_reports = vec![
        head_eval_report(
            &baseline_genesis,
            &prepared_data,
            supported_workload.workload_id.clone(),
        ),
        head_eval_report(
            &low_lr_genesis,
            &prepared_data,
            supported_workload.workload_id.clone(),
        ),
    ];
    let reducer_peer_id = reducer
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("reducer missing local peer id")?;
    let validator_peer_id = validator
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("validator missing local peer id")?;
    let validator_b_peer_id = validator_b
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("validator-b missing local peer id")?;
    let validator_peer_ids =
        BTreeSet::from([validator_peer_id.clone(), validator_b_peer_id.clone()]);
    let mut baseline_head_providers = vec![validator_peer_id.clone()];
    write_demo_phase(&output, "cluster-ready")?;

    for round_index in 0..args.baseline_rounds {
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-trainers-start", round_index + 1),
        )?;
        let (outcome_a1, outcome_a2) = run_parallel_training_pair(
            &mut trainer_a1,
            TRAINER_A1_LABEL,
            &mut trainer_a2,
            TRAINER_A2_LABEL,
            &baseline,
            &baseline_head,
            &prepared_data,
        )?;
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-trained", round_index + 1),
        )?;
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-artifact-prewarm-start", round_index + 1),
        )?;
        wait_for_candidate_artifacts(
            [
                (TRAINER_A1_LABEL, &trainer_a1, &outcome_a1),
                (TRAINER_A2_LABEL, &trainer_a2, &outcome_a2),
            ],
            [
                (REDUCER_LABEL, &reducer),
                (VALIDATOR_LABEL, &validator),
                (VALIDATOR_B_LABEL, &validator_b),
            ],
            &baseline,
            Duration::from_secs(45),
            "baseline trainer artifacts were not fetchable by the reducer/validator tier",
        )?;
        wait_for_candidate_control_plane(
            [
                (REDUCER_LABEL, &reducer),
                (VALIDATOR_LABEL, &validator),
                (VALIDATOR_B_LABEL, &validator_b),
            ],
            &baseline,
            [&outcome_a1, &outcome_a2],
            Duration::from_secs(45),
            "baseline candidate updates did not propagate across the reducer/validator tier",
        )?;
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-candidates-ready", round_index + 1),
        )?;
        baseline_outcomes.push(outcome_a1);
        baseline_outcomes.push(outcome_a2);
        let validation = run_reducer_validation_round(
            &mut reducer,
            &mut validator,
            &mut validator_b,
            &baseline,
            &reducer_peer_id,
            &validator_peer_ids,
        )?;
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-validation-complete", round_index + 1),
        )?;
        baseline_head = validation.merged_head.clone();
        baseline_head_providers = vec![
            validation.merge_certificate.promoter_peer_id.clone(),
            reducer_peer_id.clone(),
            validator_peer_id.clone(),
            validator_b_peer_id.clone(),
        ];
        if require_full_topology_convergence() {
            sync_topology_heads(
                &[
                    (REDUCER_LABEL, &reducer),
                    (VALIDATOR_LABEL, &validator),
                    (VALIDATOR_B_LABEL, &validator_b),
                    (TRAINER_A1_LABEL, &trainer_a1),
                    (TRAINER_A2_LABEL, &trainer_a2),
                    (VIEWER_LABEL, &viewer),
                ],
                &baseline_head_providers,
                &baseline,
                &baseline_head,
                TOPOLOGY_HEAD_SYNC_TIMEOUT,
                "baseline topology did not converge on the promoted head",
            )?;
        } else {
            sync_topology_heads(
                &[
                    (REDUCER_LABEL, &reducer),
                    (VALIDATOR_LABEL, &validator),
                    (VALIDATOR_B_LABEL, &validator_b),
                ],
                &baseline_head_providers,
                &baseline,
                &baseline_head,
                TOPOLOGY_HEAD_SYNC_TIMEOUT,
                "baseline reducer/validator tier did not converge on the promoted head",
            )?;
            let _ = wait_for_specific_head(
                &trainer_a1,
                &baseline,
                &baseline_head,
                Duration::from_secs(15),
                "trainer-a1 did not observe promoted baseline head on time",
            );
            let _ = wait_for_specific_head(
                &trainer_a2,
                &baseline,
                &baseline_head,
                Duration::from_secs(15),
                "trainer-a2 did not observe promoted baseline head on time",
            );
            wait_for_head_artifacts(
                &[(TRAINER_A1_LABEL, &trainer_a1), (TRAINER_A2_LABEL, &trainer_a2)],
                &baseline_head_providers,
                &baseline_head.artifact_id,
                Duration::from_secs(20),
                "baseline trainers could not prewarm the promoted baseline head artifact",
            )?;
            let _ = wait_for_specific_head(
                &viewer,
                &baseline,
                &baseline_head,
                Duration::from_secs(15),
                "viewer did not observe promoted baseline head on time",
            );
        }
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-head-artifact-ready", round_index + 1),
        )?;
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-topology-synced", round_index + 1),
        )?;
        merge_certificates.push(validation.merge_certificate.clone());
        head_eval_reports.push(head_eval_report_from_validation(
            &validation,
            &prepared_data,
            supported_workload.workload_id.clone(),
        ));
    }

    write_demo_phase(&output, "baseline-rounds-complete")?;

    let trainer_restart_reconnected;
    let trainer_restart_resumed_training;
    let late_joiner_synced_checkpoint;
    let resilience_drills_executed;
    let late_joiner;
    if resilience_drills_required {
        resilience_drills_executed = true;
        write_demo_phase(&output, "restart-trainer-shutdown-start")?;
        shutdown_node(TRAINER_A2_LABEL, trainer_a2)?;
        write_demo_phase(&output, "restart-trainer-shutdown-complete")?;
        let restarted_trainer_a2 = build_trainer_project(BASELINE_LEARNING_RATE)?
            .trainer_with_config(release_manifest.clone(), burn_workload_config.clone())?
            .with_network(network_manifest.clone())?
            .with_storage(StorageConfig::new(storage_root.join(TRAINER_A2_LABEL)))
            .with_bootstrap_peer(helper_addr.clone())
            .spawn()?;
        trainer_a2 = restarted_trainer_a2;
        wait_for(
            Duration::from_secs(15),
            || trainer_a2.telemetry().snapshot().connected_peers >= 1,
            "restarted trainer-a2 did not reconnect",
        )?;
        write_demo_phase(&output, "restart-trainer-reconnected")?;
        let _ = wait_for_synced_head(
            &trainer_a2,
            &baseline,
            FOLLOWER_HEAD_DISCOVERY_TIMEOUT,
            "restarted trainer-a2 did not resync baseline head",
        )?;
        wait_for_head_artifacts(
            &[(TRAINER_A2_LABEL, &trainer_a2)],
            &baseline_head_providers,
            &baseline_head.artifact_id,
            Duration::from_secs(20),
            "restarted trainer-a2 could not prewarm the promoted baseline head artifact",
        )?;
        trainer_restart_reconnected = true;
        let restarted_outcome = run_training_round(
            &mut trainer_a2,
            TRAINER_A2_LABEL,
            &baseline,
            Some(&baseline_head),
            &prepared_data,
        )?;
        write_demo_phase(&output, "restart-round-trained")?;
        wait_for_candidate_artifacts(
            [(TRAINER_A2_LABEL, &trainer_a2, &restarted_outcome)],
            [
                (REDUCER_LABEL, &reducer),
                (VALIDATOR_LABEL, &validator),
                (VALIDATOR_B_LABEL, &validator_b),
            ],
            &baseline,
            RESTART_CANDIDATE_ARTIFACT_TIMEOUT,
            "restarted trainer-a2 artifact was not fetchable by the reducer/validator tier",
        )?;
        wait_for_candidate_control_plane(
            [
                (REDUCER_LABEL, &reducer),
                (VALIDATOR_LABEL, &validator),
                (VALIDATOR_B_LABEL, &validator_b),
            ],
            &baseline,
            [&restarted_outcome],
            Duration::from_secs(45),
            "restart candidate update did not propagate across the reducer/validator tier",
        )?;
        write_demo_phase(&output, "restart-round-candidates-ready")?;
        baseline_outcomes.push(restarted_outcome);
        let validation = run_reducer_validation_round(
            &mut reducer,
            &mut validator,
            &mut validator_b,
            &baseline,
            &reducer_peer_id,
            &validator_peer_ids,
        )?;
        write_demo_phase(&output, "restart-round-validation-complete")?;
        baseline_head = validation.merged_head.clone();
        baseline_head_providers = vec![
            validation.merge_certificate.promoter_peer_id.clone(),
            reducer_peer_id.clone(),
            validator_peer_id.clone(),
            validator_b_peer_id.clone(),
        ];
        if require_full_topology_convergence() {
            sync_topology_heads(
                &[
                    (REDUCER_LABEL, &reducer),
                    (VALIDATOR_LABEL, &validator),
                    (VALIDATOR_B_LABEL, &validator_b),
                    (TRAINER_A1_LABEL, &trainer_a1),
                    (TRAINER_A2_LABEL, &trainer_a2),
                    (VIEWER_LABEL, &viewer),
                ],
                &baseline_head_providers,
                &baseline,
                &baseline_head,
                TOPOLOGY_HEAD_SYNC_TIMEOUT,
                "restart validation did not converge on the promoted baseline head",
            )?;
        } else {
            sync_topology_heads(
                &[
                    (REDUCER_LABEL, &reducer),
                    (VALIDATOR_LABEL, &validator),
                    (VALIDATOR_B_LABEL, &validator_b),
                ],
                &baseline_head_providers,
                &baseline,
                &baseline_head,
                TOPOLOGY_HEAD_SYNC_TIMEOUT,
                "restart reducer/validator tier did not converge on the promoted baseline head",
            )?;
            let _ = wait_for_specific_head(
                &trainer_a1,
                &baseline,
                &baseline_head,
                Duration::from_secs(15),
                "trainer-a1 did not observe restart-promoted baseline head on time",
            );
            let _ = wait_for_specific_head(
                &trainer_a2,
                &baseline,
                &baseline_head,
                Duration::from_secs(15),
                "trainer-a2 did not observe restart-promoted baseline head on time",
            );
            wait_for_head_artifacts(
                &[(TRAINER_A1_LABEL, &trainer_a1), (TRAINER_A2_LABEL, &trainer_a2)],
                &baseline_head_providers,
                &baseline_head.artifact_id,
                Duration::from_secs(20),
                "baseline trainers could not prewarm the restart-promoted baseline head artifact",
            )?;
            let _ = wait_for_specific_head(
                &viewer,
                &baseline,
                &baseline_head,
                Duration::from_secs(15),
                "viewer did not observe restart-promoted baseline head on time",
            );
        }
        write_demo_phase(&output, "restart-round-topology-synced")?;
        merge_certificates.push(validation.merge_certificate.clone());
        head_eval_reports.push(head_eval_report_from_validation(
            &validation,
            &prepared_data,
            supported_workload.workload_id.clone(),
        ));
        trainer_restart_resumed_training = true;

        let late_joiner_node = build_trainer_project(BASELINE_LEARNING_RATE)?
            .trainer_with_config(release_manifest.clone(), burn_workload_config.clone())?
            .with_network(network_manifest.clone())?
            .with_storage(StorageConfig::new(storage_root.join(TRAINER_LATE_LABEL)))
            .with_bootstrap_peer(helper_addr.clone())
            .spawn()?;
        wait_for(
            Duration::from_secs(15),
            || late_joiner_node.telemetry().snapshot().connected_peers >= 1,
            "late joiner did not connect",
        )?;
        wait_for_head_artifacts(
            &[(TRAINER_LATE_LABEL, &late_joiner_node)],
            &baseline_head_providers,
            &baseline_head.artifact_id,
            Duration::from_secs(20),
            "late joiner could not prewarm the promoted baseline head artifact",
        )?;
        let synced_head = wait_for_specific_head(
            &late_joiner_node,
            &baseline,
            &baseline_head,
            FOLLOWER_HEAD_DISCOVERY_TIMEOUT,
            "late joiner did not discover baseline head",
        )?;
        let late_joiner_store = late_joiner_node
            .artifact_store()
            .context("late joiner missing artifact store")?;
        late_joiner_synced_checkpoint = late_joiner_store.has_manifest(&synced_head.artifact_id);
        write_demo_phase(&output, "late-joiner-synced")?;
        late_joiner = Some(late_joiner_node);
    } else {
        resilience_drills_executed = false;
        trainer_restart_reconnected = false;
        trainer_restart_resumed_training = false;
        late_joiner_synced_checkpoint = false;
        late_joiner = None;
        write_demo_phase(&output, "resilience-drills-skipped")?;
    }

    for round_index in 0..args.low_lr_rounds {
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-trainer-start", round_index + 1),
        )?;
        let low_lr_outcome = run_training_round(
            &mut trainer_b,
            TRAINER_B_LABEL,
            &low_lr,
            Some(&low_lr_head),
            &prepared_data,
        )?;
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-trained", round_index + 1),
        )?;
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-artifact-prewarm-start", round_index + 1),
        )?;
        wait_for_candidate_artifacts(
            [(TRAINER_B_LABEL, &trainer_b, &low_lr_outcome)],
            [
                (REDUCER_LABEL, &reducer),
                (VALIDATOR_LABEL, &validator),
                (VALIDATOR_B_LABEL, &validator_b),
            ],
            &low_lr,
            Duration::from_secs(45),
            "low-lr trainer artifact was not fetchable by the reducer/validator tier",
        )?;
        wait_for_candidate_control_plane(
            [
                (REDUCER_LABEL, &reducer),
                (VALIDATOR_LABEL, &validator),
                (VALIDATOR_B_LABEL, &validator_b),
            ],
            &low_lr,
            [&low_lr_outcome],
            Duration::from_secs(45),
            "low-lr candidate update did not propagate across the reducer/validator tier",
        )?;
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-candidates-ready", round_index + 1),
        )?;
        low_lr_outcomes.push(low_lr_outcome);
        let validation = run_reducer_validation_round(
            &mut reducer,
            &mut validator,
            &mut validator_b,
            &low_lr,
            &reducer_peer_id,
            &validator_peer_ids,
        )?;
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-validation-complete", round_index + 1),
        )?;
        low_lr_head = validation.merged_head.clone();
        let low_lr_head_providers = vec![
            validation.merge_certificate.promoter_peer_id.clone(),
            reducer_peer_id.clone(),
            validator_peer_id.clone(),
            validator_b_peer_id.clone(),
        ];
        if require_full_topology_convergence() {
            sync_topology_heads(
                &[
                    (REDUCER_LABEL, &reducer),
                    (VALIDATOR_LABEL, &validator),
                    (VALIDATOR_B_LABEL, &validator_b),
                    (TRAINER_B_LABEL, &trainer_b),
                    (VIEWER_LABEL, &viewer),
                ],
                &low_lr_head_providers,
                &low_lr,
                &low_lr_head,
                TOPOLOGY_HEAD_SYNC_TIMEOUT,
                "low-lr topology did not converge on the promoted head",
            )?;
        } else {
            sync_topology_heads(
                &[
                    (REDUCER_LABEL, &reducer),
                    (VALIDATOR_LABEL, &validator),
                    (VALIDATOR_B_LABEL, &validator_b),
                ],
                &low_lr_head_providers,
                &low_lr,
                &low_lr_head,
                TOPOLOGY_HEAD_SYNC_TIMEOUT,
                "low-lr reducer/validator tier did not converge on the promoted head",
            )?;
            let _ = wait_for_specific_head(
                &trainer_b,
                &low_lr,
                &low_lr_head,
                Duration::from_secs(15),
                "trainer-b did not observe promoted low-lr head on time",
            );
            wait_for_head_artifacts(
                &[(TRAINER_B_LABEL, &trainer_b)],
                &low_lr_head_providers,
                &low_lr_head.artifact_id,
                Duration::from_secs(20),
                "low-lr trainer could not prewarm the promoted low-lr head artifact",
            )?;
            let _ = wait_for_specific_head(
                &viewer,
                &low_lr,
                &low_lr_head,
                Duration::from_secs(15),
                "viewer did not observe promoted low-lr head on time",
            );
        }
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-topology-synced", round_index + 1),
        )?;
        merge_certificates.push(validation.merge_certificate.clone());
        head_eval_reports.push(head_eval_report_from_validation(
            &validation,
            &prepared_data,
            supported_workload.workload_id.clone(),
        ));
    }
    write_demo_phase(&output, "low-lr-rounds-complete")?;

    let browser_nodes = [
        (HELPER_LABEL, &helper),
        (VALIDATOR_LABEL, &validator),
        (VALIDATOR_B_LABEL, &validator_b),
        (REDUCER_LABEL, &reducer),
        (VIEWER_LABEL, &viewer),
        (TRAINER_A1_LABEL, &trainer_a1),
        (TRAINER_A2_LABEL, &trainer_a2),
        (TRAINER_B_LABEL, &trainer_b),
    ];

    if args.live_browser_probe {
        let browser_source = baseline_outcomes
            .last()
            .context("mnist demo did not produce a baseline trainer lease for live browser probe")?
            .clone();
        let (browser_provider_label, browser_provider) =
            resolve_browser_provider(browser_source.label, &browser_nodes)?;
        write_browser_probe_bundle(BrowserProbeWriteInput {
            output: &output,
            storage_root: &storage_root,
            prepared_data: &prepared_data,
            supported_workload: &supported_workload,
            release_manifest: &release_manifest,
            network_manifest: &network_manifest,
            promotion_mode: promotion_mode.clone(),
            baseline_genesis: &baseline_genesis,
            baseline_head: &baseline_head,
            low_lr_genesis: &low_lr_genesis,
            low_lr_head: &low_lr_head,
            baseline_outcomes: &baseline_outcomes,
            low_lr_outcomes: &low_lr_outcomes,
            merge_certificates: &merge_certificates,
            head_eval_reports: &head_eval_reports,
            browser_provider_label,
            browser_provider,
            head_artifact_provider: &validator,
            browser_lease: &browser_source.training.lease,
            browser_head_providers: &baseline_head_providers,
            browser_nodes: &browser_nodes,
        })?;
    }

    let mut final_nodes = browser_nodes
        .iter()
        .map(|(label, node)| (*label, *node))
        .collect::<Vec<_>>();
    if let Some(late_joiner) = late_joiner.as_ref() {
        final_nodes.push((TRAINER_LATE_LABEL, late_joiner));
    }
    let CollectedDemoArtifacts {
        node_records,
        bootstrap_plan,
        final_snapshots,
        metrics_catchup,
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
    } = collect_demo_artifacts(&output, &storage_root, &final_nodes, HELPER_LABEL)?;

    write_demo_phase(&output, "shutdown-start")?;
    let mut shutdown_nodes = Vec::new();
    if let Some(late_joiner) = late_joiner {
        shutdown_nodes.push((TRAINER_LATE_LABEL, late_joiner));
    }
    shutdown_nodes.extend([
        (TRAINER_B_LABEL, trainer_b),
        (TRAINER_A2_LABEL, trainer_a2),
        (TRAINER_A1_LABEL, trainer_a1),
        (VIEWER_LABEL, viewer),
        (VALIDATOR_B_LABEL, validator_b),
        (REDUCER_LABEL, reducer),
        (VALIDATOR_LABEL, validator),
        (HELPER_LABEL, helper),
    ]);
    shutdown_nodes_in_order(shutdown_nodes)?;
    write_demo_phase(&output, "shutdown-complete")?;
    let phase_timeline = read_demo_phase_summary(&output)?;

    Ok(CoreMnistRun {
        promotion_mode,
        prepared_data,
        network_manifest,
        release_manifest,
        supported_workload,
        baseline_genesis,
        baseline_head,
        low_lr_genesis,
        low_lr_head,
        baseline_outcomes,
        low_lr_outcomes,
        merge_certificates,
        metrics_catchup,
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
        node_records,
        bootstrap_plan,
        bootstrap_seed_label: HELPER_LABEL.into(),
        dedicated_reducer_label: REDUCER_LABEL.into(),
        validator_labels: vec![VALIDATOR_LABEL.into(), VALIDATOR_B_LABEL.into()],
        final_snapshots,
        restarted_trainer_label,
        resilience_drills_executed,
        trainer_restart_reconnected,
        trainer_restart_resumed_training,
        late_joiner_synced_checkpoint,
        phase_timeline,
        browser_probe_manifest,
        browser_probe_summary,
    })
}

fn run_core_demo_diffusion(args: &Args, context: CoreDemoContext) -> anyhow::Result<CoreMnistRun> {
    let CoreDemoContext {
        output,
        storage_root,
        prepared_data,
        supported_workload,
        burn_workload_config,
        release_manifest,
        network_manifest,
        low_lr_learning_rate,
    } = context;
    let diffusion_auth = AuthConfig::new().with_experiment_directory(
        mnist_runtime_directory_entries(
            &network_manifest,
            &supported_workload,
            &prepared_data,
            HeadPromotionMode::DiffusionSteadyState,
        ),
    );
    let build_trainer_project = |learning_rate: f64| -> anyhow::Result<_> {
        let device = <RuntimeBackend as burn::tensor::backend::Backend>::Device::default();
        let prepared_for_eval = prepared_data.clone();
        let train_loader = prepared_data.build_train_loader(&device);
        let validation_loader = prepared_data.build_eval_loader(&device);
        Ok(from_loaders(
            Learner::new(
                MnistModel::<RuntimeBackend>::new(&device),
                AdamConfig::new().init(),
                learning_rate,
            ),
            device,
            train_loader,
            validation_loader,
        )
        .with_benchmark(|_model, _device| burn_p2p::CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 16.0,
            target_window_seconds: 1,
        })
        .with_sharded_dataset(
            prepared_data.train_dataset.clone(),
            crate::data::MnistBatcher,
            prepared_data.batch_size,
        )
        .with_evaluate(move |model, _split| evaluate_model(model, &prepared_for_eval))
        .with_step_metrics(|step_index, output, metrics| {
            accumulate_training_metrics(step_index, output, metrics);
            Ok(())
        })
        .with_window_metrics(|learner, metrics| {
            let inventory = inspect_module::<RuntimeBackend, _>(&learner.model());
            metrics.insert(
                "parameter_count".into(),
                MetricValue::Integer(inventory.parameter_count as i64),
            );
            Ok(())
        }))
    };
    let build_service_project = |learning_rate: f64| -> anyhow::Result<_> {
        let device = <RuntimeBackend as burn::tensor::backend::Backend>::Device::default();
        Ok(from_learner(
            Learner::new(
                MnistModel::<RuntimeBackend>::new(&device),
                AdamConfig::new().init(),
                learning_rate,
            ),
            device,
        )
        .with_benchmark(|_model, _device| burn_p2p::CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 16.0,
            target_window_seconds: 1,
        }))
    };

    let helper = build_service_project(BASELINE_LEARNING_RATE)?
        .connect_with_config(
            BurnTarget::Custom(PeerRoleSet::new([
                PeerRole::Bootstrap,
                PeerRole::RelayHelper,
            ])),
            release_manifest.clone(),
            burn_workload_config.clone(),
        )?
        .with_network(network_manifest.clone())?
        .with_auth(diffusion_auth.clone())
        .with_storage(StorageConfig::new(storage_root.join(HELPER_LABEL)))
        .spawn()?;
    wait_for(
        Duration::from_secs(10),
        || {
            let snapshot = helper.telemetry().snapshot();
            snapshot.status == burn_p2p::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
                && snapshot.local_peer_id.is_some()
        },
        "helper seed did not start",
    )?;
    let helper_addr = helper.telemetry().snapshot().listen_addresses[0].clone();

    let viewer = build_service_project(BASELINE_LEARNING_RATE)?
        .connect_with_config(
            BurnTarget::Custom(PeerRoleSet::new([
                PeerRole::Viewer,
                PeerRole::BrowserObserver,
            ])),
            release_manifest.clone(),
            burn_workload_config.clone(),
        )?
        .with_network(network_manifest.clone())?
        .with_auth(diffusion_auth.clone())
        .with_storage(StorageConfig::new(storage_root.join(VIEWER_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;
    let trainer_a1 = build_trainer_project(BASELINE_LEARNING_RATE)?
        .trainer_with_config(release_manifest.clone(), burn_workload_config.clone())?
        .with_network(network_manifest.clone())?
        .with_auth(diffusion_auth.clone())
        .with_storage(StorageConfig::new(storage_root.join(TRAINER_A1_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;
    let trainer_a2 = build_trainer_project(BASELINE_LEARNING_RATE)?
        .trainer_with_config(release_manifest.clone(), burn_workload_config.clone())?
        .with_network(network_manifest.clone())?
        .with_auth(diffusion_auth.clone())
        .with_storage(StorageConfig::new(storage_root.join(TRAINER_A2_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;
    let trainer_a3 = build_trainer_project(BASELINE_LEARNING_RATE)?
        .trainer_with_config(release_manifest.clone(), burn_workload_config.clone())?
        .with_network(network_manifest.clone())?
        .with_auth(diffusion_auth.clone())
        .with_storage(StorageConfig::new(storage_root.join(TRAINER_A3_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;
    let trainer_b = build_trainer_project(low_lr_learning_rate)?
        .trainer_with_config(release_manifest.clone(), burn_workload_config.clone())?
        .with_network(network_manifest.clone())?
        .with_auth(diffusion_auth.clone())
        .with_storage(StorageConfig::new(storage_root.join(TRAINER_B_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;

    wait_for(
        Duration::from_secs(20),
        || {
            helper.telemetry().snapshot().connected_peers >= 5
                && viewer.telemetry().snapshot().connected_peers >= 1
                && trainer_a1.telemetry().snapshot().connected_peers >= 1
                && trainer_a2.telemetry().snapshot().connected_peers >= 1
                && trainer_a3.telemetry().snapshot().connected_peers >= 1
                && trainer_b.telemetry().snapshot().connected_peers >= 1
        },
        "fleet did not converge on the helper-seeded trainer-only diffusion network",
    )?;

    let helper = helper;
    let viewer = viewer;
    let mut trainer_a1 = trainer_a1;
    let mut trainer_a2 = trainer_a2;
    let mut trainer_a3 = trainer_a3;
    let mut trainer_b = trainer_b;

    let baseline = helper.experiment(
        StudyId::new(STUDY_ID),
        ExperimentId::new(BASELINE_EXPERIMENT),
        RevisionId::new(BASELINE_REVISION),
    );
    let low_lr = helper.experiment(
        StudyId::new(STUDY_ID),
        ExperimentId::new(LOW_LR_EXPERIMENT),
        RevisionId::new(LOW_LR_REVISION),
    );

    let baseline_genesis = trainer_a1.initialize_local_head(&baseline)?;
    let low_lr_genesis = trainer_b.initialize_local_head(&low_lr)?;
    let mut baseline_head = baseline_genesis.clone();
    let mut low_lr_head = low_lr_genesis.clone();
    let trainer_a1_peer_id = trainer_a1
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("trainer-a1 missing local peer id")?;
    let trainer_a2_peer_id = trainer_a2
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("trainer-a2 missing local peer id")?;
    let trainer_a3_peer_id = trainer_a3
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("trainer-a3 missing local peer id")?;
    trainer_a1.publish_head_provider(&baseline, &baseline_head)?;
    trainer_b.publish_head_provider(&low_lr, &low_lr_head)?;
    wait_for_artifact_from_provider(
        (TRAINER_A1_LABEL, &trainer_a1),
        [(TRAINER_A2_LABEL, &trainer_a2), (TRAINER_A3_LABEL, &trainer_a3)],
        &trainer_a1_peer_id,
        &baseline_head.artifact_id,
        demo_provider_artifact_timeout(),
        "baseline genesis head artifact was not fetchable across the diffusion trainer cohort",
    )?;
    wait_for_specific_head(
        &trainer_a2,
        &baseline,
        &baseline_head,
        Duration::from_secs(20),
        "trainer-a2 did not sync baseline genesis head",
    )?;
    wait_for_specific_head(
        &trainer_a3,
        &baseline,
        &baseline_head,
        Duration::from_secs(20),
        "trainer-a3 did not sync baseline genesis head",
    )?;

    let mut baseline_outcomes = Vec::new();
    let mut low_lr_outcomes = Vec::new();
    let mut merge_certificates = Vec::<MergeCertificate>::new();
    let browser_probe_manifest = None;
    let browser_probe_summary = None;
    let restarted_trainer_label = TRAINER_A2_LABEL.to_string();
    let resilience_drills_required = require_resilience_drills();
    let mut head_eval_reports = vec![
        head_eval_report(
            &baseline_genesis,
            &prepared_data,
            supported_workload.workload_id.clone(),
        ),
        head_eval_report(
            &low_lr_genesis,
            &prepared_data,
            supported_workload.workload_id.clone(),
        ),
    ];
    let mut baseline_head_providers = vec![
        trainer_a1_peer_id.clone(),
        trainer_a3_peer_id.clone(),
    ];
    write_demo_phase(&output, "cluster-ready")?;

    for round_index in 0..args.baseline_rounds {
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-trainers-start", round_index + 1),
        )?;
        let (outcome_a1, outcome_a2) = run_parallel_training_pair(
            &mut trainer_a1,
            TRAINER_A1_LABEL,
            &mut trainer_a2,
            TRAINER_A2_LABEL,
            &baseline,
            &baseline_head,
            &prepared_data,
        )?;
        let outcome_a3 = run_training_round(
            &mut trainer_a3,
            TRAINER_A3_LABEL,
            &baseline,
            Some(&baseline_head),
            &prepared_data,
        )?;
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-trained", round_index + 1),
        )?;
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-artifact-prewarm-start", round_index + 1),
        )?;
        wait_for_candidate_artifacts(
            [
                (TRAINER_A1_LABEL, &trainer_a1, &outcome_a1),
                (TRAINER_A2_LABEL, &trainer_a2, &outcome_a2),
                (TRAINER_A3_LABEL, &trainer_a3, &outcome_a3),
            ],
            [
                (TRAINER_A1_LABEL, &trainer_a1),
                (TRAINER_A2_LABEL, &trainer_a2),
                (TRAINER_A3_LABEL, &trainer_a3),
            ],
            &baseline,
            Duration::from_secs(45),
            "baseline trainer artifacts were not fetchable across the diffusion promotion cohort",
        )?;
        wait_for_candidate_control_plane(
            [
                (TRAINER_A1_LABEL, &trainer_a1),
                (TRAINER_A2_LABEL, &trainer_a2),
                (TRAINER_A3_LABEL, &trainer_a3),
            ],
            &baseline,
            [&outcome_a1, &outcome_a2, &outcome_a3],
            Duration::from_secs(45),
            "baseline candidate updates did not propagate across the diffusion promotion cohort",
        )?;
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-candidates-ready", round_index + 1),
        )?;
        baseline_outcomes.push(outcome_a1.clone());
        baseline_outcomes.push(outcome_a2.clone());
        baseline_outcomes.push(outcome_a3.clone());
        let validation = run_diffusion_promotion_round_triple(
            &mut trainer_a1,
            &mut trainer_a2,
            &mut trainer_a3,
            &baseline,
            &baseline_head,
            [&outcome_a1, &outcome_a2, &outcome_a3],
        )?;
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-validation-complete", round_index + 1),
        )?;
        baseline_head = validation.merged_head.clone();
        sync_topology_heads(
            &[
                (TRAINER_A1_LABEL, &trainer_a1),
                (TRAINER_A2_LABEL, &trainer_a2),
                (TRAINER_A3_LABEL, &trainer_a3),
            ],
            std::slice::from_ref(&validation.merge_certificate.promoter_peer_id),
            &baseline,
            &baseline_head,
            TOPOLOGY_HEAD_SYNC_TIMEOUT,
            "baseline diffusion trainer cohort did not converge on the promoted head",
        )?;
        republish_head_from_nodes(
            [
                (TRAINER_A1_LABEL, &mut trainer_a1),
                (TRAINER_A2_LABEL, &mut trainer_a2),
                (TRAINER_A3_LABEL, &mut trainer_a3),
            ],
            &baseline,
            &baseline_head,
        )?;
        baseline_head_providers = vec![
            trainer_a1_peer_id.clone(),
            trainer_a2_peer_id.clone(),
            trainer_a3_peer_id.clone(),
        ];
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-head-artifact-ready", round_index + 1),
        )?;
        write_demo_phase(
            &output,
            &format!("baseline-round-{}-topology-synced", round_index + 1),
        )?;
        merge_certificates.push(validation.merge_certificate.clone());
        head_eval_reports.push(head_eval_report_from_validation(
            &validation,
            &prepared_data,
            supported_workload.workload_id.clone(),
        ));
    }

    write_demo_phase(&output, "baseline-rounds-complete")?;

    let trainer_restart_reconnected;
    let trainer_restart_resumed_training;
    let late_joiner_synced_checkpoint;
    let resilience_drills_executed;
    let late_joiner;
    if resilience_drills_required {
        resilience_drills_executed = true;
        write_demo_phase(&output, "restart-trainer-shutdown-start")?;
        shutdown_node(TRAINER_A2_LABEL, trainer_a2)?;
        write_demo_phase(&output, "restart-trainer-shutdown-complete")?;
        let restarted_trainer_a2 = build_trainer_project(BASELINE_LEARNING_RATE)?
            .trainer_with_config(release_manifest.clone(), burn_workload_config.clone())?
            .with_network(network_manifest.clone())?
            .with_auth(diffusion_auth.clone())
            .with_storage(StorageConfig::new(storage_root.join(TRAINER_A2_LABEL)))
            .with_bootstrap_peer(helper_addr.clone())
            .spawn()?;
        trainer_a2 = restarted_trainer_a2;
        wait_for(
            Duration::from_secs(15),
            || trainer_a2.telemetry().snapshot().connected_peers >= 1,
            "restarted trainer-a2 did not reconnect",
        )?;
        write_demo_phase(&output, "restart-trainer-reconnected")?;
        let _ = wait_for_synced_head(
            &trainer_a2,
            &baseline,
            FOLLOWER_HEAD_DISCOVERY_TIMEOUT,
            "restarted trainer-a2 did not resync baseline head",
        )?;
        trainer_restart_reconnected = true;
        let (restarted_a1_outcome, restarted_a2_outcome) = run_parallel_training_pair(
            &mut trainer_a1,
            TRAINER_A1_LABEL,
            &mut trainer_a2,
            TRAINER_A2_LABEL,
            &baseline,
            &baseline_head,
            &prepared_data,
        )?;
        let restarted_a3_outcome = run_training_round(
            &mut trainer_a3,
            TRAINER_A3_LABEL,
            &baseline,
            Some(&baseline_head),
            &prepared_data,
        )?;
        write_demo_phase(&output, "restart-round-trained")?;
        wait_for_candidate_artifacts(
            [
                (TRAINER_A1_LABEL, &trainer_a1, &restarted_a1_outcome),
                (TRAINER_A2_LABEL, &trainer_a2, &restarted_a2_outcome),
                (TRAINER_A3_LABEL, &trainer_a3, &restarted_a3_outcome),
            ],
            [
                (TRAINER_A1_LABEL, &trainer_a1),
                (TRAINER_A2_LABEL, &trainer_a2),
                (TRAINER_A3_LABEL, &trainer_a3),
            ],
            &baseline,
            RESTART_CANDIDATE_ARTIFACT_TIMEOUT,
            "restart trainer artifacts were not fetchable across the diffusion promotion cohort",
        )?;
        wait_for_candidate_control_plane(
            [
                (TRAINER_A1_LABEL, &trainer_a1),
                (TRAINER_A2_LABEL, &trainer_a2),
                (TRAINER_A3_LABEL, &trainer_a3),
            ],
            &baseline,
            [
                &restarted_a1_outcome,
                &restarted_a2_outcome,
                &restarted_a3_outcome,
            ],
            Duration::from_secs(45),
            "restart candidate updates did not propagate across the diffusion promotion cohort",
        )?;
        write_demo_phase(&output, "restart-round-candidates-ready")?;
        baseline_outcomes.push(restarted_a1_outcome.clone());
        baseline_outcomes.push(restarted_a2_outcome.clone());
        baseline_outcomes.push(restarted_a3_outcome.clone());
        let validation = run_diffusion_promotion_round_triple(
            &mut trainer_a1,
            &mut trainer_a2,
            &mut trainer_a3,
            &baseline,
            &baseline_head,
            [
                &restarted_a1_outcome,
                &restarted_a2_outcome,
                &restarted_a3_outcome,
            ],
        )?;
        write_demo_phase(&output, "restart-round-validation-complete")?;
        baseline_head = validation.merged_head.clone();
        sync_topology_heads(
            &[
                (TRAINER_A1_LABEL, &trainer_a1),
                (TRAINER_A2_LABEL, &trainer_a2),
                (TRAINER_A3_LABEL, &trainer_a3),
            ],
            std::slice::from_ref(&validation.merge_certificate.promoter_peer_id),
            &baseline,
            &baseline_head,
            TOPOLOGY_HEAD_SYNC_TIMEOUT,
            "restart diffusion trainer cohort did not converge on the promoted baseline head",
        )?;
        republish_head_from_nodes(
            [
                (TRAINER_A1_LABEL, &mut trainer_a1),
                (TRAINER_A2_LABEL, &mut trainer_a2),
                (TRAINER_A3_LABEL, &mut trainer_a3),
            ],
            &baseline,
            &baseline_head,
        )?;
        baseline_head_providers = vec![
            trainer_a1_peer_id.clone(),
            trainer_a2
                .telemetry()
                .snapshot()
                .local_peer_id
                .context("restarted trainer-a2 missing local peer id")?,
            trainer_a3_peer_id.clone(),
        ];
        write_demo_phase(&output, "restart-round-topology-synced")?;
        merge_certificates.push(validation.merge_certificate.clone());
        head_eval_reports.push(head_eval_report_from_validation(
            &validation,
            &prepared_data,
            supported_workload.workload_id.clone(),
        ));
        trainer_restart_resumed_training = true;

        let late_joiner_node = build_trainer_project(BASELINE_LEARNING_RATE)?
            .trainer_with_config(release_manifest.clone(), burn_workload_config.clone())?
            .with_network(network_manifest.clone())?
            .with_auth(diffusion_auth.clone())
            .with_storage(StorageConfig::new(storage_root.join(TRAINER_LATE_LABEL)))
            .with_bootstrap_peer(helper_addr.clone())
            .spawn()?;
        wait_for(
            Duration::from_secs(15),
            || late_joiner_node.telemetry().snapshot().connected_peers >= 1,
            "late joiner did not connect",
        )?;
        wait_for_head_artifacts(
            &[(TRAINER_LATE_LABEL, &late_joiner_node)],
            &baseline_head_providers,
            &baseline_head.artifact_id,
            Duration::from_secs(20),
            "late joiner could not prewarm the promoted baseline head artifact",
        )?;
        let synced_head = wait_for_specific_head(
            &late_joiner_node,
            &baseline,
            &baseline_head,
            FOLLOWER_HEAD_DISCOVERY_TIMEOUT,
            "late joiner did not discover baseline head",
        )?;
        let late_joiner_store = late_joiner_node
            .artifact_store()
            .context("late joiner missing artifact store")?;
        late_joiner_synced_checkpoint = late_joiner_store.has_manifest(&synced_head.artifact_id);
        write_demo_phase(&output, "late-joiner-synced")?;
        late_joiner = Some(late_joiner_node);
    } else {
        resilience_drills_executed = false;
        trainer_restart_reconnected = false;
        trainer_restart_resumed_training = false;
        late_joiner_synced_checkpoint = false;
        late_joiner = None;
        write_demo_phase(&output, "resilience-drills-skipped")?;
    }

    for round_index in 0..args.low_lr_rounds {
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-trainer-start", round_index + 1),
        )?;
        let low_lr_outcome = run_training_round(
            &mut trainer_b,
            TRAINER_B_LABEL,
            &low_lr,
            Some(&low_lr_head),
            &prepared_data,
        )?;
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-trained", round_index + 1),
        )?;
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-artifact-prewarm-start", round_index + 1),
        )?;
        wait_for_candidate_artifacts(
            [(TRAINER_B_LABEL, &trainer_b, &low_lr_outcome)],
            [(TRAINER_B_LABEL, &trainer_b)],
            &low_lr,
            Duration::from_secs(45),
            "low-lr trainer artifact was not fetchable for the diffusion promoter",
        )?;
        wait_for_candidate_control_plane(
            [(TRAINER_B_LABEL, &trainer_b)],
            &low_lr,
            [&low_lr_outcome],
            Duration::from_secs(45),
            "low-lr candidate update did not publish at the diffusion promoter",
        )?;
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-candidates-ready", round_index + 1),
        )?;
        low_lr_outcomes.push(low_lr_outcome.clone());
        let validation = run_diffusion_promotion_round_single(
            &mut trainer_b,
            &low_lr,
            &low_lr_head,
            &low_lr_outcome,
        )?;
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-validation-complete", round_index + 1),
        )?;
        low_lr_head = validation.merged_head.clone();
        republish_head_from_nodes(
            [(TRAINER_B_LABEL, &mut trainer_b)],
            &low_lr,
            &low_lr_head,
        )?;
        write_demo_phase(
            &output,
            &format!("low-lr-round-{}-topology-synced", round_index + 1),
        )?;
        merge_certificates.push(validation.merge_certificate.clone());
        head_eval_reports.push(head_eval_report_from_validation(
            &validation,
            &prepared_data,
            supported_workload.workload_id.clone(),
        ));
    }
    write_demo_phase(&output, "low-lr-rounds-complete")?;

    let mut browser_nodes = vec![
        (HELPER_LABEL, &helper),
        (VIEWER_LABEL, &viewer),
        (TRAINER_A1_LABEL, &trainer_a1),
        (TRAINER_A2_LABEL, &trainer_a2),
        (TRAINER_A3_LABEL, &trainer_a3),
        (TRAINER_B_LABEL, &trainer_b),
    ];
    if let Some(late_joiner) = late_joiner.as_ref() {
        browser_nodes.push((TRAINER_LATE_LABEL, late_joiner));
    }

    if args.live_browser_probe {
        let browser_source = baseline_outcomes
            .last()
            .context("mnist demo did not produce a baseline trainer lease for live browser probe")?
            .clone();
        let (browser_provider_label, browser_provider) =
            resolve_browser_provider(browser_source.label, &browser_nodes)?;
        write_browser_probe_bundle(BrowserProbeWriteInput {
            output: &output,
            storage_root: &storage_root,
            prepared_data: &prepared_data,
            supported_workload: &supported_workload,
            release_manifest: &release_manifest,
            network_manifest: &network_manifest,
            promotion_mode: HeadPromotionMode::DiffusionSteadyState,
            baseline_genesis: &baseline_genesis,
            baseline_head: &baseline_head,
            low_lr_genesis: &low_lr_genesis,
            low_lr_head: &low_lr_head,
            baseline_outcomes: &baseline_outcomes,
            low_lr_outcomes: &low_lr_outcomes,
            merge_certificates: &merge_certificates,
            head_eval_reports: &head_eval_reports,
            browser_provider_label,
            browser_provider,
            head_artifact_provider: browser_provider,
            browser_lease: &browser_source.training.lease,
            browser_head_providers: &baseline_head_providers,
            browser_nodes: &browser_nodes,
        })?;
    }

    let CollectedDemoArtifacts {
        node_records,
        bootstrap_plan,
        final_snapshots,
        metrics_catchup,
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
    } = collect_demo_artifacts(&output, &storage_root, &browser_nodes, HELPER_LABEL)?;

    write_demo_phase(&output, "shutdown-start")?;
    let mut shutdown_nodes = Vec::new();
    if let Some(late_joiner) = late_joiner {
        shutdown_nodes.push((TRAINER_LATE_LABEL, late_joiner));
    }
    shutdown_nodes.extend([
        (TRAINER_B_LABEL, trainer_b),
        (TRAINER_A3_LABEL, trainer_a3),
        (TRAINER_A2_LABEL, trainer_a2),
        (TRAINER_A1_LABEL, trainer_a1),
        (VIEWER_LABEL, viewer),
        (HELPER_LABEL, helper),
    ]);
    shutdown_nodes_in_order(shutdown_nodes)?;
    write_demo_phase(&output, "shutdown-complete")?;
    let phase_timeline = read_demo_phase_summary(&output)?;

    Ok(CoreMnistRun {
        promotion_mode: HeadPromotionMode::DiffusionSteadyState,
        prepared_data,
        network_manifest,
        release_manifest,
        supported_workload,
        baseline_genesis,
        baseline_head,
        low_lr_genesis,
        low_lr_head,
        baseline_outcomes,
        low_lr_outcomes,
        merge_certificates,
        metrics_catchup,
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
        node_records,
        bootstrap_plan,
        bootstrap_seed_label: HELPER_LABEL.into(),
        dedicated_reducer_label: String::new(),
        validator_labels: Vec::new(),
        final_snapshots,
        restarted_trainer_label,
        resilience_drills_executed,
        trainer_restart_reconnected,
        trainer_restart_resumed_training,
        late_joiner_synced_checkpoint,
        phase_timeline,
        browser_probe_manifest,
        browser_probe_summary,
    })
}

#[derive(Clone)]
pub(crate) struct TrainingRecord {
    pub label: &'static str,
    pub training: burn_p2p::TrainingWindowOutcome<BTreeMap<String, MetricValue>>,
}

fn run_training_round<P>(
    trainer: &mut burn_p2p::RunningNode<P>,
    label: &'static str,
    experiment: &burn_p2p::ExperimentHandle,
    pinned_head: Option<&HeadDescriptor>,
    _prepared_data: &PreparedMnistData,
) -> anyhow::Result<TrainingRecord>
where
    P: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
{
    let training = trainer.train_window_once_with_pinned_head(experiment, pinned_head)?;
    trainer.publish_head_provider(experiment, &training.head)?;
    trainer.publish_artifact_from_store(&training.artifact.artifact_id)?;
    trainer.publish_artifact_from_store(&training.head.artifact_id)?;
    wait_for_local_training_publication(
        trainer,
        label,
        experiment,
        &training,
        Duration::from_secs(10),
    )?;
    trainer.republish_training_window_control_plane(
        experiment,
        training.lease.window_id,
        &training.contribution.base_head_id,
        &training.artifact.artifact_id,
    )?;
    Ok(TrainingRecord { label, training })
}

fn run_parallel_training_pair<P>(
    trainer_a: &mut burn_p2p::RunningNode<P>,
    label_a: &'static str,
    trainer_b: &mut burn_p2p::RunningNode<P>,
    label_b: &'static str,
    experiment: &burn_p2p::ExperimentHandle,
    pinned_head: &HeadDescriptor,
    prepared_data: &PreparedMnistData,
) -> anyhow::Result<(TrainingRecord, TrainingRecord)>
where
    P: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>> + Send,
{
    thread::scope(|scope| {
        // Keep both trainers on the same base-head view before either one can
        // publish its completed window and shift the merge lineage.
        let paired_start = Arc::new(Barrier::new(2));
        let training_a_start = Arc::clone(&paired_start);
        let training_b_start = Arc::clone(&paired_start);
        let training_a = scope.spawn(move || {
            training_a_start.wait();
            run_training_round(
                trainer_a,
                label_a,
                experiment,
                Some(pinned_head),
                prepared_data,
            )
        });
        let training_b = scope.spawn(move || {
            training_b_start.wait();
            run_training_round(
                trainer_b,
                label_b,
                experiment,
                Some(pinned_head),
                prepared_data,
            )
        });
        let outcome_a = training_a
            .join()
            .map_err(|_| anyhow::anyhow!("parallel training thread {label_a} panicked"))??;
        let outcome_b = training_b
            .join()
            .map_err(|_| anyhow::anyhow!("parallel training thread {label_b} panicked"))??;
        Ok((outcome_a, outcome_b))
    })
}

fn wait_for_local_training_publication<P>(
    trainer: &burn_p2p::RunningNode<P>,
    label: &str,
    experiment: &burn_p2p::ExperimentHandle,
    training: &burn_p2p::TrainingWindowOutcome<P::WindowStats>,
    timeout: Duration,
) -> anyhow::Result<()>
where
    P: P2pWorkload,
{
    let deadline = Instant::now() + timeout;
    let overlay = experiment.overlay_set()?.heads;

    while Instant::now() < deadline {
        let snapshot = trainer.telemetry().snapshot();
        let update_visible =
            snapshot
                .control_plane
                .update_announcements
                .iter()
                .any(|announcement| {
                    announcement.overlay == overlay
                        && announcement.update.study_id == experiment.study_id
                        && announcement.update.experiment_id == experiment.experiment_id
                        && announcement.update.revision_id == experiment.revision_id
                        && announcement.update.peer_id == training.contribution.peer_id
                        && announcement.update.window_id == training.lease.window_id
                        && announcement.update.base_head_id == training.contribution.base_head_id
                        && announcement.update.delta_artifact_id == training.artifact.artifact_id
                });
        let head_visible = snapshot
            .control_plane
            .head_announcements
            .iter()
            .any(|announcement| {
                announcement.overlay == overlay
                    && announcement.head.study_id == experiment.study_id
                    && announcement.head.experiment_id == experiment.experiment_id
                    && announcement.head.revision_id == experiment.revision_id
                    && announcement.head.head_id == training.head.head_id
            });
        let merge_window_visible = snapshot
            .control_plane
            .merge_window_announcements
            .iter()
            .any(|announcement| {
                announcement.overlay == overlay
                    && announcement.merge_window.study_id == experiment.study_id
                    && announcement.merge_window.experiment_id == experiment.experiment_id
                    && announcement.merge_window.revision_id == experiment.revision_id
                    && announcement.merge_window.window_id == training.lease.window_id
                    && announcement.merge_window.base_head_id == training.contribution.base_head_id
            });

        if update_visible && head_visible && merge_window_visible {
            return Ok(());
        }

        thread::sleep(Duration::from_millis(50));
    }

    anyhow::bail!(
        "{label} did not publish local training control-plane state for window {}",
        training.lease.window_id.0,
    )
}

fn wait_for_candidate_artifacts<P, const PROVIDER_COUNT: usize, const NODE_COUNT: usize>(
    providers: [(&str, &burn_p2p::RunningNode<P>, &TrainingRecord); PROVIDER_COUNT],
    consumers: [(&str, &burn_p2p::RunningNode<P>); NODE_COUNT],
    experiment: &burn_p2p::ExperimentHandle,
    timeout: Duration,
    failure_message: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !consumers.is_empty(),
        "candidate artifact wait requires at least one consumer"
    );
    let tier_timeout = demo_candidate_artifact_timeout(timeout);
    let mut ordered_consumers = consumers.iter().copied().collect::<Vec<_>>();
    ordered_consumers.sort_by_key(|(label, _)| candidate_prewarm_consumer_priority(label));

    for (label, provider, outcome) in providers {
        let mut artifact_ids = vec![outcome.training.artifact.artifact_id.clone()];
        if outcome.training.head.artifact_id != outcome.training.artifact.artifact_id {
            artifact_ids.push(outcome.training.head.artifact_id.clone());
        }
        for artifact_id in artifact_ids {
            let staged_providers = vec![(
                label,
                provider,
                outcome.training.contribution.peer_id.clone(),
            )];
            wait_for_artifact_from_topology(
                &staged_providers,
                &ordered_consumers,
                &artifact_id,
                tier_timeout,
                failure_message,
            )?;
        }
        for (_, consumer) in &consumers {
            consumer.publish_head_provider(experiment, &outcome.training.head)?;
        }
    }
    Ok(())
}

fn candidate_prewarm_consumer_priority(label: &str) -> u8 {
    match label {
        VALIDATOR_LABEL => 0,
        VALIDATOR_B_LABEL => 1,
        REDUCER_LABEL => 2,
        _ => 3,
    }
}

fn wait_for_candidate_control_plane<P, const NODE_COUNT: usize, const OUTCOME_COUNT: usize>(
    observers: [(&str, &burn_p2p::RunningNode<P>); NODE_COUNT],
    experiment: &burn_p2p::ExperimentHandle,
    outcomes: [&TrainingRecord; OUTCOME_COUNT],
    timeout: Duration,
    failure_message: &str,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let expected_peer_ids = outcomes
        .iter()
        .map(|outcome| outcome.training.contribution.peer_id.clone())
        .collect::<BTreeSet<_>>();
    let expected_head_ids = outcomes
        .iter()
        .map(|outcome| outcome.training.head.head_id.clone())
        .collect::<BTreeSet<_>>();
    let expected_base_head_id = outcomes
        .first()
        .map(|outcome| outcome.training.contribution.base_head_id.clone())
        .context("candidate control-plane wait requires at least one outcome")?;
    let overlay = experiment.overlay_set()?.heads;
    let mut last_missing = BTreeMap::<String, Vec<String>>::new();
    let mut last_sync_errors = BTreeMap::<String, Vec<String>>::new();

    while Instant::now() < deadline {
        let mut all_ready = true;
        last_missing.clear();
        last_sync_errors.clear();

        for (label, observer) in &observers {
            for outcome in &outcomes {
                if let Err(error) = observer.seed_training_candidate(experiment, &outcome.training)
                {
                    last_sync_errors
                        .entry((*label).to_string())
                        .or_default()
                        .push(format!(
                            "candidate:{}:{error}",
                            outcome.training.contribution.peer_id.as_str()
                        ));
                }
            }
            let snapshot = observer.telemetry().snapshot();
            let observed_update_peers = snapshot
                .control_plane
                .update_announcements
                .iter()
                .filter(|announcement| {
                    announcement.overlay == overlay
                        && announcement.update.study_id == experiment.study_id
                        && announcement.update.experiment_id == experiment.experiment_id
                        && announcement.update.revision_id == experiment.revision_id
                        && announcement.update.base_head_id == expected_base_head_id
                })
                .map(|announcement| announcement.update.peer_id.clone())
                .collect::<BTreeSet<_>>();
            let observed_head_ids = snapshot
                .control_plane
                .head_announcements
                .iter()
                .filter(|announcement| {
                    announcement.overlay == overlay
                        && announcement.head.study_id == experiment.study_id
                        && announcement.head.experiment_id == experiment.experiment_id
                        && announcement.head.revision_id == experiment.revision_id
                        && announcement.head.parent_head_id.as_ref() == Some(&expected_base_head_id)
                })
                .map(|announcement| announcement.head.head_id.clone())
                .collect::<BTreeSet<_>>();
            let merge_window_visible = snapshot
                .control_plane
                .merge_window_announcements
                .iter()
                .any(|announcement| {
                    announcement.overlay == overlay
                        && announcement.merge_window.study_id == experiment.study_id
                        && announcement.merge_window.experiment_id == experiment.experiment_id
                        && announcement.merge_window.revision_id == experiment.revision_id
                        && announcement.merge_window.base_head_id == expected_base_head_id
                });

            let mut missing = expected_peer_ids
                .difference(&observed_update_peers)
                .map(|peer_id| format!("update:{}", peer_id.as_str()))
                .collect::<Vec<_>>();
            missing.extend(
                expected_head_ids
                    .difference(&observed_head_ids)
                    .map(|head_id| format!("head:{}", head_id.as_str())),
            );
            if !merge_window_visible {
                missing.push("merge-window".into());
            }
            if !missing.is_empty() {
                all_ready = false;
                last_missing.insert((*label).to_string(), missing);
            }
        }

        if all_ready {
            return Ok(());
        }

        thread::sleep(Duration::from_millis(100));
    }

    let mut details = last_missing
        .into_iter()
        .map(|(label, missing)| format!("{label} missing {}", missing.join(",")))
        .collect::<Vec<_>>();
    details.extend(
        last_sync_errors
            .into_iter()
            .map(|(label, errors)| format!("{label} sync {}", errors.join(","))),
    );
    anyhow::bail!("{failure_message}: {}", details.join("; "))
}

fn wait_for_head_artifacts<P>(
    consumers: &[(&str, &burn_p2p::RunningNode<P>)],
    provider_peer_ids: &[PeerId],
    artifact_id: &burn_p2p::ArtifactId,
    timeout: Duration,
    failure_message: &str,
) -> anyhow::Result<()> {
    wait_for_artifact_from_fixed_providers(
        consumers,
        provider_peer_ids,
        artifact_id,
        timeout,
        failure_message,
    )
}

fn wait_for_artifact_from_topology<P>(
    providers: &[(&str, &burn_p2p::RunningNode<P>, PeerId)],
    consumers: &[(&str, &burn_p2p::RunningNode<P>)],
    artifact_id: &burn_p2p::ArtifactId,
    timeout: Duration,
    failure_message: &str,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_error = None::<String>;
    let fixed_provider_peer_ids = providers
        .iter()
        .map(|(_, _, peer_id)| peer_id.clone())
        .collect::<Vec<_>>();
    let mut staged_provider_peer_ids = fixed_provider_peer_ids.clone();
    while Instant::now() < deadline {
        let mut provider_peer_ids = staged_provider_peer_ids.clone();
        let mut republish_failed = false;
        for (label, provider, peer_id) in providers {
            match provider.publish_artifact_from_store(artifact_id) {
                Ok(_) => {
                    push_unique_peer_id(&mut provider_peer_ids, peer_id.clone());
                    push_unique_peer_id(&mut staged_provider_peer_ids, peer_id.clone());
                }
                Err(error) => {
                    republish_failed = true;
                    last_error = Some(format!(
                        "{label}: could not republish {}: {error}",
                        artifact_id.as_str(),
                    ));
                    break;
                }
            }
        }
        if republish_failed {
            thread::sleep(Duration::from_millis(100));
            continue;
        }

        let mut all_ready = true;
        for (label, consumer) in consumers {
            if record_artifact_provider(
                label,
                consumer,
                artifact_id,
                &mut staged_provider_peer_ids,
                &mut last_error,
            ) {
                provider_peer_ids = staged_provider_peer_ids.clone();
                continue;
            }

            let Some(attempt_timeout) = artifact_sync_attempt_timeout_before(deadline) else {
                all_ready = false;
                break;
            };

            match consumer.wait_for_artifact_from_peers(
                &provider_peer_ids,
                artifact_id,
                attempt_timeout,
            ) {
                Ok(_) => {}
                Err(error) => {
                    all_ready = false;
                    last_error = Some(format!("{label}: {error}"));
                    continue;
                }
            }
            if !record_artifact_provider(
                label,
                consumer,
                artifact_id,
                &mut staged_provider_peer_ids,
                &mut last_error,
            ) {
                all_ready = false;
                continue;
            }
            provider_peer_ids = staged_provider_peer_ids.clone();
        }

        if all_ready {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }

    if let Some(error) = last_error {
        anyhow::bail!("{failure_message}: {error}");
    }
    anyhow::bail!(failure_message.to_owned())
}

fn push_unique_peer_id(provider_peer_ids: &mut Vec<PeerId>, peer_id: PeerId) {
    if !provider_peer_ids.contains(&peer_id) {
        provider_peer_ids.push(peer_id);
    }
}

fn hosted_ci_mode() -> bool {
    std::env::var_os("CI").is_some() || std::env::var_os("GITHUB_ACTIONS").is_some()
}

fn bounded_ci_mnist_mode() -> bool {
    std::env::var_os(BOUNDED_CI_MNIST_ENV).is_some()
}

fn demo_low_lr_learning_rate() -> f64 {
    if bounded_ci_mnist_mode() {
        BOUNDED_CI_LOW_LR_LEARNING_RATE
    } else {
        LOW_LR_LEARNING_RATE
    }
}

fn format_learning_rate(value: f64) -> String {
    format!("{value:.1e}")
}

pub(crate) fn promotion_mode_slug(mode: &HeadPromotionMode) -> &'static str {
    match mode {
        HeadPromotionMode::ValidatorQuorum => "validator-quorum",
        HeadPromotionMode::ReducerAuthority => "reducer-authority",
        HeadPromotionMode::DiffusionSteadyState => "diffusion-steady-state",
    }
}

fn demo_artifact_sync_attempt_timeout() -> Duration {
    if bounded_ci_mnist_mode() {
        Duration::from_secs(45)
    } else if hosted_ci_mode() {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(5)
    }
}

fn cap_artifact_sync_attempt_timeout(
    remaining: Duration,
    max_attempt_timeout: Duration,
) -> Option<Duration> {
    if remaining.is_zero() {
        None
    } else {
        Some(remaining.min(max_attempt_timeout))
    }
}

fn artifact_sync_attempt_timeout_before(deadline: Instant) -> Option<Duration> {
    cap_artifact_sync_attempt_timeout(
        deadline.saturating_duration_since(Instant::now()),
        demo_artifact_sync_attempt_timeout(),
    )
}

fn demo_candidate_artifact_timeout(timeout: Duration) -> Duration {
    if bounded_ci_mnist_mode() {
        timeout.max(Duration::from_secs(180))
    } else if hosted_ci_mode() {
        timeout.max(Duration::from_secs(120))
    } else {
        timeout
    }
}

fn demo_provider_artifact_timeout() -> Duration {
    if bounded_ci_mnist_mode() {
        Duration::from_secs(75)
    } else if hosted_ci_mode() {
        Duration::from_secs(45)
    } else {
        Duration::from_secs(20)
    }
}

fn demo_validation_round_timeout() -> Duration {
    if hosted_ci_mode() {
        Duration::from_secs(75)
    } else {
        DEMO_VALIDATION_ROUND_TIMEOUT
    }
}

fn require_full_topology_convergence() -> bool {
    !bounded_ci_mnist_mode()
}

fn require_resilience_drills() -> bool {
    !bounded_ci_mnist_mode()
}

fn wait_for_artifact_from_fixed_providers<P>(
    consumers: &[(&str, &burn_p2p::RunningNode<P>)],
    provider_peer_ids: &[PeerId],
    artifact_id: &burn_p2p::ArtifactId,
    timeout: Duration,
    failure_message: &str,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_error = None::<String>;
    let mut staged_provider_peer_ids = provider_peer_ids.to_vec();

    while Instant::now() < deadline {
        let mut all_ready = true;
        for (label, consumer) in consumers {
            if record_artifact_provider(
                label,
                consumer,
                artifact_id,
                &mut staged_provider_peer_ids,
                &mut last_error,
            ) {
                continue;
            }

            let Some(attempt_timeout) = artifact_sync_attempt_timeout_before(deadline) else {
                all_ready = false;
                break;
            };

            match consumer.wait_for_artifact_from_peers(
                &staged_provider_peer_ids,
                artifact_id,
                attempt_timeout,
            ) {
                Ok(_) => {}
                Err(error) => {
                    all_ready = false;
                    last_error = Some(format!("{label}: {error}"));
                    continue;
                }
            }

            if !record_artifact_provider(
                label,
                consumer,
                artifact_id,
                &mut staged_provider_peer_ids,
                &mut last_error,
            ) {
                all_ready = false;
                continue;
            }
        }

        if all_ready {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }

    if let Some(error) = last_error {
        anyhow::bail!("{failure_message}: {error}");
    }
    anyhow::bail!(failure_message.to_owned())
}

fn record_artifact_provider<P>(
    label: &str,
    node: &burn_p2p::RunningNode<P>,
    artifact_id: &burn_p2p::ArtifactId,
    provider_peer_ids: &mut Vec<PeerId>,
    last_error: &mut Option<String>,
) -> bool {
    let Some(store) = node.artifact_store() else {
        return false;
    };
    match store.has_complete_artifact(artifact_id) {
        Ok(true) => {}
        Ok(false) => return false,
        Err(error) => {
            *last_error = Some(format!(
                "{label}: could not inspect {} completeness: {error}",
                artifact_id.as_str(),
            ));
            return false;
        }
    }

    let Some(local_peer_id) = node.telemetry().snapshot().local_peer_id else {
        *last_error = Some(format!(
            "{label}: local peer id unavailable while staging {}",
            artifact_id.as_str(),
        ));
        return false;
    };

    match node.publish_artifact_from_store(artifact_id) {
        Ok(_) => {
            push_unique_peer_id(provider_peer_ids, local_peer_id);
            true
        }
        Err(error) => {
            *last_error = Some(format!(
                "{label}: could not republish {}: {error}",
                artifact_id.as_str(),
            ));
            false
        }
    }
}

fn write_demo_phase(output_root: &Path, phase: &str) -> anyhow::Result<()> {
    let now = Utc::now();
    let events_path = output_root.join("phase-events.json");
    let mut events = read_demo_phase_events(output_root)?;
    let elapsed_ms_since_start = events
        .first()
        .map(|first| {
            (now - first.started_at)
                .num_milliseconds()
                .try_into()
                .unwrap_or_default()
        })
        .unwrap_or(0);
    events.push(DemoPhaseEvent {
        phase: phase.into(),
        started_at: now,
        elapsed_ms_since_start,
    });
    let summary = build_demo_phase_summary(&events);

    eprintln!("mnist demo phase: {phase} (+{}ms)", elapsed_ms_since_start);
    fs::write(&events_path, serde_json::to_vec_pretty(&events)?)
        .with_context(|| format!("failed to write {}", events_path.display()))?;
    fs::write(
        output_root.join("phase-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )
    .with_context(|| {
        format!(
            "failed to write {}",
            output_root.join("phase-summary.json").display()
        )
    })?;
    fs::write(
        output_root.join("phase.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "phase": phase,
            "updated_at": now,
            "elapsed_ms_since_start": elapsed_ms_since_start,
        }))?,
    )
    .with_context(|| {
        format!(
            "failed to write {}",
            output_root.join("phase.json").display()
        )
    })
}

fn read_demo_phase_events(output_root: &Path) -> anyhow::Result<Vec<DemoPhaseEvent>> {
    let events_path = output_root.join("phase-events.json");
    if !events_path.exists() {
        return Ok(Vec::new());
    }

    let bytes = fs::read(&events_path)
        .with_context(|| format!("failed to read {}", events_path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to decode {}", events_path.display()))
}

fn build_demo_phase_summary(events: &[DemoPhaseEvent]) -> DemoPhaseSummary {
    let phases = events
        .iter()
        .enumerate()
        .map(|(index, event)| DemoPhaseTiming {
            phase: event.phase.clone(),
            started_at: event.started_at,
            ended_at: events.get(index + 1).map(|next| next.started_at),
            duration_ms: events.get(index + 1).and_then(|next| {
                (next.started_at - event.started_at)
                    .num_milliseconds()
                    .try_into()
                    .ok()
            }),
        })
        .collect::<Vec<_>>();
    let total_elapsed_ms = events
        .last()
        .map(|event| event.elapsed_ms_since_start)
        .unwrap_or_default();

    DemoPhaseSummary {
        total_elapsed_ms,
        events: events.to_vec(),
        phases,
    }
}

fn read_demo_phase_summary(output_root: &Path) -> anyhow::Result<DemoPhaseSummary> {
    let events = read_demo_phase_events(output_root)?;
    Ok(build_demo_phase_summary(&events))
}

fn evaluate_model(
    model: &MnistModel<EvalBackend>,
    prepared: &PreparedMnistData,
) -> burn_p2p::MetricReport {
    let loader = prepared.build_eval_loader(&Default::default());
    let mut sample_count = 0usize;
    let mut batch_count = 0usize;
    let mut loss_sum = 0.0;
    let mut accuracy_sum = 0.0;
    let mut zero_accuracy_sum = 0.0;
    let mut zero_weight = 0usize;

    for batch in loader.iter() {
        let metrics = model.step(batch);
        batch_count += 1;
        sample_count += metrics.sample_count;
        loss_sum += metrics.loss * metrics.sample_count as f64;
        accuracy_sum += metrics.accuracy * metrics.sample_count as f64;
        let zero_examples = metrics.digit_zero_examples;
        zero_accuracy_sum += metrics.digit_zero_accuracy * zero_examples as f64;
        zero_weight += zero_examples;
    }

    burn_p2p::MetricReport {
        metrics: BTreeMap::from([
            (
                "accuracy".into(),
                MetricValue::Float(if sample_count == 0 {
                    0.0
                } else {
                    accuracy_sum / sample_count as f64
                }),
            ),
            (
                "digit_zero_accuracy".into(),
                MetricValue::Float(if zero_weight == 0 {
                    0.0
                } else {
                    zero_accuracy_sum / zero_weight as f64
                }),
            ),
            (
                "loss".into(),
                MetricValue::Float(if sample_count == 0 {
                    0.0
                } else {
                    loss_sum / sample_count as f64
                }),
            ),
            (
                "evaluation_batches".into(),
                MetricValue::Integer(batch_count as i64),
            ),
            (
                "evaluation_items".into(),
                MetricValue::Integer(sample_count as i64),
            ),
        ]),
        captured_at: Utc::now(),
    }
}

fn accumulate_training_metrics(
    step_index: usize,
    output: &MnistMetricItem,
    metrics: &mut BTreeMap<String, MetricValue>,
) {
    let step = step_index + 1;
    let loss_sum = metric_float(metrics, "loss_sum") + output.loss;
    let accuracy_sum = metric_float(metrics, "accuracy_sum") + output.accuracy;
    let zero_accuracy_sum =
        metric_float(metrics, "digit_zero_accuracy_sum") + output.digit_zero_accuracy;
    metrics.insert("loss_sum".into(), MetricValue::Float(loss_sum));
    metrics.insert("accuracy_sum".into(), MetricValue::Float(accuracy_sum));
    metrics.insert(
        "digit_zero_accuracy_sum".into(),
        MetricValue::Float(zero_accuracy_sum),
    );
    metrics.insert("train_loss_last".into(), MetricValue::Float(output.loss));
    metrics.insert(
        "train_loss_mean".into(),
        MetricValue::Float(loss_sum / step as f64),
    );
    metrics.insert(
        "train_accuracy_last".into(),
        MetricValue::Float(output.accuracy),
    );
    metrics.insert(
        "train_accuracy_mean".into(),
        MetricValue::Float(accuracy_sum / step as f64),
    );
    metrics.insert(
        "digit_zero_accuracy_last".into(),
        MetricValue::Float(output.digit_zero_accuracy),
    );
    metrics.insert(
        "digit_zero_accuracy_mean".into(),
        MetricValue::Float(zero_accuracy_sum / step as f64),
    );
    metrics.insert(
        "samples_last".into(),
        MetricValue::Integer(output.sample_count as i64),
    );
}

pub(crate) fn supported_workload() -> anyhow::Result<SupportedWorkload> {
    Ok(SupportedWorkload {
        workload_id: WorkloadId::new(WORKLOAD_ID),
        workload_name: "MNIST classifier".into(),
        model_program_hash: ContentId::new("mnist-demo-mlp-v1"),
        checkpoint_format_hash: ContentId::new("mnist-demo-named-mpk"),
        supported_revision_family: ContentId::new("mnist-demo-revision-family"),
        resource_class: "cpu".into(),
    })
}

fn mnist_burn_workload_config(supported_workload: SupportedWorkload) -> BurnWorkloadConfig {
    BurnWorkloadConfig::new(
        supported_workload,
        BurnArtifactConfig::named_mpk(BurnRecordPrecision::Full, ChunkingScheme::default()),
    )
    .with_root_ema(BurnWorkloadConfig::standard_root_ema_decay())
}

pub(crate) fn release_manifest(supported_workload: &SupportedWorkload) -> ClientReleaseManifest {
    ClientReleaseManifest {
        project_family_id: ProjectFamilyId::new(PROJECT_FAMILY),
        release_train_hash: ContentId::new("mnist-demo-release-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("mnist-demo-native-target"),
        target_platform: ClientPlatform::Native,
        app_semver: Version::new(0, 1, 0),
        git_commit: "local-mnist-demo".into(),
        cargo_lock_hash: ContentId::new("mnist-demo-cargo-lock"),
        burn_version_string: "0.21.0-pre.3".into(),
        enabled_features_hash: ContentId::new("burn,portal"),
        protocol_major: 0,
        supported_workloads: vec![supported_workload.clone()],
        built_at: Utc::now(),
    }
}

pub(crate) fn network_manifest(release_manifest: &ClientReleaseManifest) -> NetworkManifest {
    NetworkManifest {
        network_id: burn_p2p::NetworkId::new(NETWORK_ID),
        project_family_id: release_manifest.project_family_id.clone(),
        protocol_major: release_manifest.protocol_major,
        required_release_train_hash: release_manifest.release_train_hash.clone(),
        allowed_target_artifact_hashes: BTreeSet::from([release_manifest
            .target_artifact_hash
            .clone()]),
        authority_public_keys: vec!["mnist-authority".into()],
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: ContentId::new("mnist-demo-auth-policy"),
        created_at: Utc::now(),
        description: "single-machine mnist sanity net".into(),
    }
}

fn mnist_diffusion_merge_topology(
    target_leaf_cohort: u16,
    allow_solo_promotion: bool,
) -> MergeTopologyPolicy {
    MergeTopologyPolicy {
        strategy: MergeStrategy::KRegularGossip,
        target_leaf_cohort,
        promotion_policy: HeadPromotionPolicy {
            mode: HeadPromotionMode::DiffusionSteadyState,
            validator_quorum: 1,
            diffusion: Some(DiffusionSteadyStatePolicy {
                allow_solo_promotion,
                ..DiffusionSteadyStatePolicy::default()
            }),
            ..HeadPromotionPolicy::default()
        },
        ..MergeTopologyPolicy::default()
    }
}

fn mnist_directory_entry(
    network_manifest: &NetworkManifest,
    supported_workload: &SupportedWorkload,
    prepared_data: &PreparedMnistData,
    promotion_mode: &HeadPromotionMode,
    spec: MnistDirectoryEntrySpec<'_>,
) -> ExperimentDirectoryEntry {
    let MnistDirectoryEntrySpec {
        experiment_id,
        revision_id,
        current_head_id,
        display_name,
        learning_rate,
    } = spec;
    let mut entry = ExperimentDirectoryEntry {
        network_id: network_manifest.network_id.clone(),
        study_id: StudyId::new(STUDY_ID),
        experiment_id: experiment_id.clone(),
        workload_id: supported_workload.workload_id.clone(),
        display_name: display_name.into(),
        model_schema_hash: ContentId::new("mnist-demo-schema"),
        dataset_view_id: prepared_data.registration.view.dataset_view_id.clone(),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(512 * 1024 * 1024),
            estimated_download_bytes: prepared_data.microshard_plan.sizing.total_bytes,
            estimated_window_seconds: 1,
        },
        visibility: ExperimentVisibility::Public,
        opt_in_policy: ExperimentOptInPolicy::Open,
        current_revision_id: revision_id.clone(),
        current_head_id,
        allowed_roles: PeerRoleSet::new([
            PeerRole::TrainerGpu,
            PeerRole::Validator,
            PeerRole::Viewer,
            PeerRole::BrowserObserver,
            PeerRole::BrowserVerifier,
            PeerRole::BrowserTrainerWgpu,
        ]),
        allowed_scopes: BTreeSet::from([
            ExperimentScope::Connect,
            ExperimentScope::Discover,
            ExperimentScope::Train {
                experiment_id: experiment_id.clone(),
            },
            ExperimentScope::Validate {
                experiment_id: experiment_id.clone(),
            },
        ]),
        metadata: BTreeMap::from([("learning_rate".into(), learning_rate.into())]),
    };
    entry.apply_revision_policy(&RevisionManifest {
        experiment_id: experiment_id.clone(),
        revision_id,
        workload_id: supported_workload.workload_id.clone(),
        required_release_train_hash: network_manifest.required_release_train_hash.clone(),
        model_schema_hash: ContentId::new("mnist-demo-schema"),
        checkpoint_format_hash: supported_workload.checkpoint_format_hash.clone(),
        dataset_view_id: prepared_data.registration.view.dataset_view_id.clone(),
        training_config_hash: ContentId::new(format!("mnist-demo-training-{learning_rate}")),
        merge_topology_policy_hash: ContentId::new("mnist-demo-topology"),
        slot_requirements: entry.resource_requirements.clone(),
        activation_window: WindowActivation {
            activation_window: WindowId(1),
            grace_windows: 0,
        },
        lag_policy: LagPolicy::default(),
        merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: true,
        browser_role_policy: BrowserRolePolicy {
            observer: true,
            verifier: true,
            trainer_wgpu: true,
            fallback: true,
        },
        max_browser_checkpoint_bytes: Some(16 * 1024 * 1024),
        max_browser_window_secs: Some(30),
        max_browser_shard_bytes: Some(8 * 1024 * 1024),
        requires_webgpu: false,
        max_browser_batch_size: Some(prepared_data.batch_size as u32),
        recommended_browser_precision: Some(Precision::Fp16),
        visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
        description: display_name.into(),
    });
    if matches!(promotion_mode, HeadPromotionMode::DiffusionSteadyState) {
        let target_leaf_cohort = if experiment_id.as_str() == BASELINE_EXPERIMENT {
            3
        } else {
            1
        };
        let allow_solo_promotion = experiment_id.as_str() != BASELINE_EXPERIMENT;
        entry.metadata.insert(
            "burn_p2p.revision.merge_topology.policy_json".into(),
            serde_json::to_string(&mnist_diffusion_merge_topology(
                target_leaf_cohort,
                allow_solo_promotion,
            ))
            .expect("mnist diffusion merge topology json"),
        );
    }
    entry
}

fn mnist_runtime_directory_entries(
    network_manifest: &NetworkManifest,
    supported_workload: &SupportedWorkload,
    prepared_data: &PreparedMnistData,
    promotion_mode: HeadPromotionMode,
) -> Vec<ExperimentDirectoryEntry> {
    vec![
        mnist_directory_entry(
            network_manifest,
            supported_workload,
            prepared_data,
            &promotion_mode,
            MnistDirectoryEntrySpec {
                experiment_id: ExperimentId::new(BASELINE_EXPERIMENT),
                revision_id: RevisionId::new(BASELINE_REVISION),
                current_head_id: None,
                display_name: "MNIST baseline",
                learning_rate: &format_learning_rate(BASELINE_LEARNING_RATE),
            },
        ),
        mnist_directory_entry(
            network_manifest,
            supported_workload,
            prepared_data,
            &promotion_mode,
            MnistDirectoryEntrySpec {
                experiment_id: ExperimentId::new(LOW_LR_EXPERIMENT),
                revision_id: RevisionId::new(LOW_LR_REVISION),
                current_head_id: None,
                display_name: "MNIST lower learning rate",
                learning_rate: &format_learning_rate(demo_low_lr_learning_rate()),
            },
        ),
    ]
}

pub(crate) fn experiment_directory_entries(
    network_manifest: &NetworkManifest,
    supported_workload: &SupportedWorkload,
    prepared_data: &PreparedMnistData,
    promotion_mode: HeadPromotionMode,
    heads: [&HeadDescriptor; 2],
) -> Vec<ExperimentDirectoryEntry> {
    vec![
        mnist_directory_entry(
            network_manifest,
            supported_workload,
            prepared_data,
            &promotion_mode,
            MnistDirectoryEntrySpec {
                experiment_id: heads[0].experiment_id.clone(),
                revision_id: heads[0].revision_id.clone(),
                current_head_id: Some(heads[0].head_id.clone()),
                display_name: "MNIST baseline",
                learning_rate: &format_learning_rate(BASELINE_LEARNING_RATE),
            },
        ),
        mnist_directory_entry(
            network_manifest,
            supported_workload,
            prepared_data,
            &promotion_mode,
            MnistDirectoryEntrySpec {
                experiment_id: heads[1].experiment_id.clone(),
                revision_id: heads[1].revision_id.clone(),
                current_head_id: Some(heads[1].head_id.clone()),
                display_name: "MNIST lower learning rate",
                learning_rate: &format_learning_rate(demo_low_lr_learning_rate()),
            },
        ),
    ]
}

pub(crate) fn leaderboard_entries(
    nodes: &[(&'static str, PeerId)],
    baseline_outcomes: &[TrainingRecord],
    low_lr_outcomes: &[TrainingRecord],
    merge_certificates: &[MergeCertificate],
) -> Vec<BrowserLeaderboardEntry> {
    let mut accepted_counts = BTreeMap::<PeerId, u64>::new();
    let mut accepted_weight = BTreeMap::<PeerId, f64>::new();
    for outcome in baseline_outcomes.iter().chain(low_lr_outcomes.iter()) {
        *accepted_counts
            .entry(outcome.training.contribution.peer_id.clone())
            .or_default() += 1;
        *accepted_weight
            .entry(outcome.training.contribution.peer_id.clone())
            .or_default() += outcome.training.contribution.accepted_weight;
    }

    let mut validation_counts = BTreeMap::<PeerId, u64>::new();
    for certificate in merge_certificates {
        *validation_counts
            .entry(certificate.promoter_peer_id.clone())
            .or_default() += 1;
    }

    nodes
        .iter()
        .map(|(label, peer_id)| {
            let accepted_receipt_count = accepted_counts.get(peer_id).copied().unwrap_or_default();
            let work_score = accepted_weight.get(peer_id).copied().unwrap_or_default();
            let validation_service =
                validation_counts.get(peer_id).copied().unwrap_or_default() as f64;
            BrowserLeaderboardEntry {
                identity: BrowserLeaderboardIdentity {
                    principal_id: Some(PrincipalId::new(format!("principal-{label}"))),
                    peer_ids: BTreeSet::from([peer_id.clone()]),
                    label: (*label).into(),
                    social_profile: None,
                },
                accepted_work_score: work_score,
                quality_weighted_impact_score: work_score,
                validation_service_score: validation_service,
                artifact_serving_score: if matches!(
                    *label,
                    VALIDATOR_LABEL | VALIDATOR_B_LABEL | REDUCER_LABEL | TRAINER_LATE_LABEL
                ) {
                    1.0
                } else {
                    0.0
                },
                leaderboard_score_v1: work_score + validation_service,
                accepted_receipt_count,
                last_receipt_at: Some(Utc::now()),
                badges: Vec::new(),
            }
        })
        .collect()
}

pub(crate) fn node_peer_id(node_records: &[CoreNodeRecord], label: &str) -> anyhow::Result<PeerId> {
    node_records
        .iter()
        .find(|record| record.label == label)
        .map(|record| record.peer_id.clone())
        .with_context(|| format!("mnist demo missing node record for {label}"))
}

pub(crate) fn leaderboard_node_pairs(
    node_records: &[CoreNodeRecord],
) -> anyhow::Result<Vec<(&'static str, PeerId)>> {
    Ok([
        TRAINER_A1_LABEL,
        TRAINER_A2_LABEL,
        TRAINER_A3_LABEL,
        TRAINER_B_LABEL,
        VALIDATOR_LABEL,
        VALIDATOR_B_LABEL,
        REDUCER_LABEL,
        VIEWER_LABEL,
        HELPER_LABEL,
        TRAINER_LATE_LABEL,
    ]
    .into_iter()
    .filter_map(|label| {
        node_records
            .iter()
            .find(|record| record.label == label)
            .map(|record| (label, record.peer_id.clone()))
    })
    .collect())
}

fn run_reducer_validation_round<P>(
    reducer: &mut burn_p2p::RunningNode<P>,
    validator: &mut burn_p2p::RunningNode<P>,
    validator_b: &mut burn_p2p::RunningNode<P>,
    experiment: &burn_p2p::ExperimentHandle,
    reducer_peer_id: &PeerId,
    validator_peer_ids: &BTreeSet<PeerId>,
) -> anyhow::Result<DemoValidationResult>
where
    P: P2pWorkload + Send,
    P::Model: Send + 'static,
{
    let reduced = reducer
        .reduce_candidates_once(experiment)?
        .context("dedicated reducer did not publish an aggregate proposal")?;
    anyhow::ensure!(
        reduced.aggregate.reducer_peer_id == *reducer_peer_id,
        "aggregate proposal came from {} instead of the dedicated reducer {}",
        reduced.aggregate.reducer_peer_id,
        reducer_peer_id,
    );
    reducer.publish_artifact_from_store(&reduced.aggregate.aggregate_artifact_id)?;
    wait_for_artifact_from_provider(
        (REDUCER_LABEL, reducer),
        [
            (VALIDATOR_LABEL, validator),
            (VALIDATOR_B_LABEL, validator_b),
        ],
        reducer_peer_id,
        &reduced.aggregate.aggregate_artifact_id,
        demo_validation_round_timeout(),
        "validators could not fetch the dedicated reducer aggregate artifact",
    )?;

    wait_for(
        Duration::from_secs(10),
        || {
            let expected_aggregate_id = &reduced.aggregate.aggregate_id;
            let expected_artifact_id = &reduced.aggregate.aggregate_artifact_id;
            [
                validator.telemetry().snapshot(),
                validator_b.telemetry().snapshot(),
            ]
            .into_iter()
            .all(|snapshot| {
                snapshot
                    .control_plane
                    .aggregate_proposal_announcements
                    .iter()
                    .any(|announcement| {
                        announcement.proposal.aggregate_id == *expected_aggregate_id
                            && announcement.proposal.aggregate_artifact_id == *expected_artifact_id
                            && announcement.proposal.reducer_peer_id == *reducer_peer_id
                    })
            })
        },
        "validators did not observe the dedicated reducer aggregate proposal",
    )?;

    let aggregate_id = reduced.aggregate.aggregate_id.clone();
    let merged_head_id = reduced.merged_head.head_id.clone();
    let validator_peer_id = validator
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("validator missing local peer id")?;
    let validator_b_peer_id = validator_b
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("validator-b missing local peer id")?;
    let promotion_deadline = Instant::now() + DEMO_VALIDATION_ROUND_TIMEOUT;
    let (validator_outcome, validator_b_outcome, validator_attempts, validator_b_attempts) =
        run_parallel_demo_validation_drivers(
            validator,
            validator_b,
            experiment,
            &aggregate_id,
            &merged_head_id,
            promotion_deadline,
        )?;
    let coordination = observe_demo_validation_coordination(
        [validator, validator_b],
        experiment,
        &aggregate_id,
        &merged_head_id,
    )?;
    let mut promoted: Option<burn_p2p::ValidationOutcome> = None;
    promoted = record_promoted_validation_outcome(promoted, validator_outcome)?;
    promoted = record_promoted_validation_outcome(promoted, validator_b_outcome)?;
    log_demo_validation_coordination("driver-final", &aggregate_id, &coordination);

    let validation = if let Some(promoted) = promoted {
        DemoValidationResult {
            merged_head: promoted.merged_head,
            merge_certificate: promoted.merge_certificate,
            evaluation: promoted.evaluation,
        }
    } else {
        let merge_certificate = coordination.merge_certificate.clone().with_context(|| {
            format!(
                "no validator returned the promoted merge result and no merge certificate was observed; observed_attesters={:?}; quorum_visible={}; merge_visible={}; validator_attempts={validator_attempts}; validator_b_attempts={validator_b_attempts}",
                coordination
                    .attesters
                    .iter()
                    .map(|peer_id| peer_id.as_str().to_owned())
                    .collect::<Vec<_>>(),
                coordination.quorum_visible,
                coordination.merge_visible,
            )
        })?;
        DemoValidationResult {
            merged_head: reduced.merged_head.clone(),
            merge_certificate,
            evaluation: reduced.evaluation.clone(),
        }
    };
    anyhow::ensure!(
        validator_peer_ids.contains(&validation.merge_certificate.promoter_peer_id),
        "merge promotion came from non-validator peer {}",
        validation.merge_certificate.promoter_peer_id,
    );
    if validation.merge_certificate.promoter_peer_id == validator_peer_id {
        validator.publish_head_provider(experiment, &validation.merged_head)?;
        wait_for_artifact_from_provider(
            (VALIDATOR_LABEL, validator),
            [(REDUCER_LABEL, reducer), (VALIDATOR_B_LABEL, validator_b)],
            &validator_peer_id,
            &validation.merged_head.artifact_id,
            demo_provider_artifact_timeout(),
            "reducer tier could not fetch the promoted merged head from validator",
        )?;
    } else {
        validator_b.publish_head_provider(experiment, &validation.merged_head)?;
        wait_for_artifact_from_provider(
            (VALIDATOR_B_LABEL, validator_b),
            [(REDUCER_LABEL, reducer), (VALIDATOR_LABEL, validator)],
            &validator_b_peer_id,
            &validation.merged_head.artifact_id,
            demo_provider_artifact_timeout(),
            "reducer tier could not fetch the promoted merged head from validator-b",
        )?;
    }
    reducer.publish_head_provider(experiment, &validation.merged_head)?;
    validator.publish_head_provider(experiment, &validation.merged_head)?;
    validator_b.publish_head_provider(experiment, &validation.merged_head)?;
    Ok(validation)
}

fn run_diffusion_promotion_round_triple<P>(
    trainer_a: &mut burn_p2p::RunningNode<P>,
    trainer_b: &mut burn_p2p::RunningNode<P>,
    trainer_c: &mut burn_p2p::RunningNode<P>,
    experiment: &burn_p2p::ExperimentHandle,
    base_head: &HeadDescriptor,
    outcomes: [&TrainingRecord; 3],
) -> anyhow::Result<DemoValidationResult>
where
    P: P2pWorkload,
{
    let target_window_id = outcomes[0].training.lease.window_id;
    let local_head_ids = outcomes
        .iter()
        .map(|outcome| outcome.training.head.head_id.clone())
        .collect::<BTreeSet<_>>();
    let deadline = Instant::now() + demo_validation_round_timeout();
    let mut promoted_head = None;

    while Instant::now() < deadline {
        trainer_a.advance_diffusion_steady_state(
            experiment,
            Some(target_window_id),
            Some(&base_head.head_id),
        )?;
        trainer_b.advance_diffusion_steady_state(
            experiment,
            Some(target_window_id),
            Some(&base_head.head_id),
        )?;
        trainer_c.advance_diffusion_steady_state(
            experiment,
            Some(target_window_id),
            Some(&base_head.head_id),
        )?;

        let trainer_a_head = trainer_a.sync_experiment_head(experiment)?;
        let trainer_b_head = trainer_b.sync_experiment_head(experiment)?;
        let trainer_c_head = trainer_c.sync_experiment_head(experiment)?;
        if let (Some(trainer_a_head), Some(trainer_b_head), Some(trainer_c_head)) =
            (trainer_a_head, trainer_b_head, trainer_c_head)
            && trainer_a_head.head_id == trainer_b_head.head_id
            && trainer_a_head.head_id == trainer_c_head.head_id
            && trainer_a_head.head_id != base_head.head_id
            && !local_head_ids.contains(&trainer_a_head.head_id)
        {
            promoted_head = Some(trainer_a_head);
            break;
        }

        thread::sleep(Duration::from_millis(25));
    }

    let cohort_diagnostics = [
        diffusion_snapshot_summary(TRAINER_A1_LABEL, trainer_a, experiment, target_window_id, &base_head.head_id),
        diffusion_snapshot_summary(TRAINER_A2_LABEL, trainer_b, experiment, target_window_id, &base_head.head_id),
        diffusion_snapshot_summary(TRAINER_A3_LABEL, trainer_c, experiment, target_window_id, &base_head.head_id),
    ]
    .join(" | ");
    let promoted_head = promoted_head.with_context(|| {
        format!(
            "diffusion promotion cohort did not converge on one promoted head for {} within {:?}; base_head={}; local_heads={:?}; cohort={cohort_diagnostics}",
            experiment.experiment_id.as_str(),
            demo_validation_round_timeout(),
            base_head.head_id.as_str(),
            local_head_ids,
        )
    })?;
    let merge_certificate = wait_for_diffusion_merge_certificate(
        [trainer_a, trainer_b, trainer_c],
        experiment,
        target_window_id,
        &base_head.head_id,
        &promoted_head.head_id,
        demo_validation_round_timeout(),
    )?;
    let trainer_a_peer_id = trainer_a
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("trainer-a missing local peer id")?;
    let trainer_b_peer_id = trainer_b
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("trainer-b missing local peer id")?;
    let trainer_c_peer_id = trainer_c
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("trainer-c missing local peer id")?;
    if merge_certificate.promoter_peer_id == trainer_a_peer_id {
        trainer_a.publish_head_provider(experiment, &promoted_head)?;
    } else if merge_certificate.promoter_peer_id == trainer_b_peer_id {
        trainer_b.publish_head_provider(experiment, &promoted_head)?;
    } else if merge_certificate.promoter_peer_id == trainer_c_peer_id {
        trainer_c.publish_head_provider(experiment, &promoted_head)?;
    }
    Ok(DemoValidationResult {
        merged_head: promoted_head.clone(),
        merge_certificate,
        evaluation: burn_p2p::MetricReport {
            metrics: promoted_head.metrics.clone(),
            captured_at: Utc::now(),
        },
    })
}

fn run_diffusion_promotion_round_single<P>(
    trainer: &mut burn_p2p::RunningNode<P>,
    experiment: &burn_p2p::ExperimentHandle,
    base_head: &HeadDescriptor,
    outcome: &TrainingRecord,
) -> anyhow::Result<DemoValidationResult>
where
    P: P2pWorkload,
{
    let target_window_id = outcome.training.lease.window_id;
    let deadline = Instant::now() + demo_validation_round_timeout();
    let mut promoted_head = None;

    while Instant::now() < deadline {
        trainer.advance_diffusion_steady_state(
            experiment,
            Some(target_window_id),
            Some(&base_head.head_id),
        )?;

        let trainer_head = trainer.sync_experiment_head(experiment)?;
        if let Some(trainer_head) = trainer_head
            && trainer_head.head_id != base_head.head_id
            && trainer_head.head_id != outcome.training.head.head_id
        {
            promoted_head = Some(trainer_head);
            break;
        }

        thread::sleep(Duration::from_millis(25));
    }

    let promoter_diagnostics = diffusion_snapshot_summary(
        TRAINER_B_LABEL,
        trainer,
        experiment,
        target_window_id,
        &base_head.head_id,
    );
    let promoted_head = promoted_head.with_context(|| {
        format!(
            "single-trainer diffusion promotion did not settle for {} within {:?}; base_head={}; local_head={}; snapshot={promoter_diagnostics}",
            experiment.experiment_id.as_str(),
            demo_validation_round_timeout(),
            base_head.head_id.as_str(),
            outcome.training.head.head_id.as_str(),
        )
    })?;
    let merge_certificate = wait_for_diffusion_merge_certificate(
        [trainer],
        experiment,
        target_window_id,
        &base_head.head_id,
        &promoted_head.head_id,
        demo_validation_round_timeout(),
    )?;
    trainer.publish_head_provider(experiment, &promoted_head)?;
    Ok(DemoValidationResult {
        merged_head: promoted_head.clone(),
        merge_certificate,
        evaluation: burn_p2p::MetricReport {
            metrics: promoted_head.metrics.clone(),
            captured_at: Utc::now(),
        },
    })
}

fn wait_for_diffusion_merge_certificate<P, const N: usize>(
    nodes: [&burn_p2p::RunningNode<P>; N],
    experiment: &burn_p2p::ExperimentHandle,
    target_window_id: WindowId,
    base_head_id: &burn_p2p::HeadId,
    merged_head_id: &burn_p2p::HeadId,
    timeout: Duration,
) -> anyhow::Result<MergeCertificate> {
    let overlay = experiment.overlay_set()?.heads;
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        for node in &nodes {
            let snapshot = node.telemetry().snapshot();
            for announcement in &snapshot.control_plane.merge_announcements {
                if announcement.overlay == overlay
                    && announcement.certificate.study_id == experiment.study_id
                    && announcement.certificate.experiment_id == experiment.experiment_id
                    && announcement.certificate.revision_id == experiment.revision_id
                    && announcement.certificate.base_head_id == *base_head_id
                    && announcement.certificate.merged_head_id == *merged_head_id
                    && announcement.certificate.promotion_mode
                        == HeadPromotionMode::DiffusionSteadyState
                {
                    return Ok(announcement.certificate.clone());
                }
            }
        }
        thread::sleep(Duration::from_millis(25));
    }

    anyhow::bail!(
        "diffusion merge certificate for {} window {} was not observed within {:?}",
        experiment.experiment_id.as_str(),
        target_window_id.0,
        timeout,
    )
}

fn diffusion_snapshot_summary<P>(
    label: &str,
    node: &burn_p2p::RunningNode<P>,
    experiment: &burn_p2p::ExperimentHandle,
    target_window_id: WindowId,
    base_head_id: &burn_p2p::HeadId,
) -> String
where
    P: P2pWorkload,
{
    let snapshot = node.telemetry().snapshot();
    let current_head = node
        .sync_experiment_head(experiment)
        .ok()
        .flatten()
        .map(|head| head.head_id.as_str().to_owned())
        .unwrap_or_else(|| "none".into());
    let updates = snapshot
        .control_plane
        .update_announcements
        .iter()
        .filter(|announcement| {
            announcement.update.study_id == experiment.study_id
                && announcement.update.experiment_id == experiment.experiment_id
                && announcement.update.revision_id == experiment.revision_id
                && announcement.update.window_id == target_window_id
                && announcement.update.base_head_id == *base_head_id
        })
        .map(|announcement| announcement.update.peer_id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let candidate_heads = snapshot
        .control_plane
        .head_announcements
        .iter()
        .filter(|announcement| {
            announcement.head.study_id == experiment.study_id
                && announcement.head.experiment_id == experiment.experiment_id
                && announcement.head.revision_id == experiment.revision_id
                && announcement.head.parent_head_id.as_ref() == Some(base_head_id)
        })
        .map(|announcement| {
            format!(
                "{}@{}",
                announcement.head.head_id.as_str(),
                announcement
                    .provider_peer_id
                    .as_ref()
                    .map(|peer_id| peer_id.as_str())
                    .unwrap_or("unknown")
            )
        })
        .collect::<BTreeSet<_>>();
    let attestations = snapshot
        .control_plane
        .trainer_promotion_attestation_announcements
        .iter()
        .filter(|announcement| {
            announcement.attestation.study_id == experiment.study_id
                && announcement.attestation.experiment_id == experiment.experiment_id
                && announcement.attestation.revision_id == experiment.revision_id
                && announcement.attestation.window_id == target_window_id
                && announcement.attestation.base_head_id == *base_head_id
        })
        .map(|announcement| {
            format!(
                "{}=>{}",
                announcement.attestation.attester_peer_id.as_str(),
                announcement.attestation.merged_head_id.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    let diffusion_certificates = snapshot
        .control_plane
        .diffusion_promotion_certificate_announcements
        .iter()
        .filter(|announcement| {
            announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.window_id == target_window_id
                && announcement.certificate.base_head_id == *base_head_id
        })
        .map(|announcement| {
            format!(
                "{}:{}@{}",
                announcement.certificate.merged_head_id.as_str(),
                announcement.certificate.attester_count,
                announcement.certificate.promoter_peer_id.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    let merge_certificates = snapshot
        .control_plane
        .merge_announcements
        .iter()
        .filter(|announcement| {
            announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.base_head_id == *base_head_id
                && matches!(
                    announcement.certificate.promotion_mode,
                    HeadPromotionMode::DiffusionSteadyState
                )
        })
        .map(|announcement| {
            format!(
                "{}@{}",
                announcement.certificate.merged_head_id.as_str(),
                announcement.certificate.promoter_peer_id.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    format!(
        "{label}[connected={},current_head={},updates={:?},candidate_heads={:?},attestations={:?},diffusion_certs={:?},merge_certs={:?}]",
        snapshot.connected_peers,
        current_head,
        updates,
        candidate_heads,
        attestations,
        diffusion_certificates,
        merge_certificates,
    )
}

fn record_promoted_validation_outcome(
    promoted: Option<burn_p2p::ValidationOutcome>,
    outcome: Option<burn_p2p::ValidationOutcome>,
) -> anyhow::Result<Option<burn_p2p::ValidationOutcome>> {
    let Some(outcome) = outcome else {
        return Ok(promoted);
    };
    anyhow::ensure!(
        outcome.merge_certificate.merged_head_id == outcome.merged_head.head_id
            && outcome.merge_certificate.merged_artifact_id == outcome.merged_head.artifact_id,
        "validator promoted merge certificate/head mismatch for {}",
        outcome.merged_head.head_id.as_str(),
    );
    if let Some(existing) = &promoted {
        anyhow::ensure!(
            existing.merged_head.head_id == outcome.merged_head.head_id,
            "validators promoted different merged heads for the same aggregate proposal",
        );
        anyhow::ensure!(
            existing.merged_head.artifact_id == outcome.merged_head.artifact_id,
            "validators promoted different merged artifacts for the same aggregate proposal",
        );
        Ok(promoted)
    } else {
        Ok(Some(outcome))
    }
}

fn run_parallel_demo_validation_drivers<P>(
    validator: &mut burn_p2p::RunningNode<P>,
    validator_b: &mut burn_p2p::RunningNode<P>,
    experiment: &burn_p2p::ExperimentHandle,
    aggregate_id: &ContentId,
    merged_head_id: &burn_p2p::HeadId,
    deadline: Instant,
) -> anyhow::Result<(
    Option<burn_p2p::ValidationOutcome>,
    Option<burn_p2p::ValidationOutcome>,
    usize,
    usize,
)>
where
    P: P2pWorkload + Send,
    P::Model: Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    thread::scope(|scope| {
        let validator_stop = Arc::clone(&stop);
        let validator_run = scope.spawn(|| {
            run_demo_validation_driver(
                VALIDATOR_LABEL,
                validator,
                experiment,
                aggregate_id,
                merged_head_id,
                deadline,
                validator_stop,
            )
        });
        let validator_b_stop = Arc::clone(&stop);
        let validator_b_run = scope.spawn(|| {
            run_demo_validation_driver(
                VALIDATOR_B_LABEL,
                validator_b,
                experiment,
                aggregate_id,
                merged_head_id,
                deadline,
                validator_b_stop,
            )
        });

        let (validator_attempts, validator_outcome) = validator_run.join().map_err(|_| {
            anyhow::anyhow!("parallel validation thread {VALIDATOR_LABEL} panicked")
        })??;
        let (validator_b_attempts, validator_b_outcome) =
            validator_b_run.join().map_err(|_| {
                anyhow::anyhow!("parallel validation thread {VALIDATOR_B_LABEL} panicked")
            })??;
        Ok((
            validator_outcome,
            validator_b_outcome,
            validator_attempts,
            validator_b_attempts,
        ))
    })
}

fn run_demo_validation_driver<P>(
    label: &str,
    node: &mut burn_p2p::RunningNode<P>,
    experiment: &burn_p2p::ExperimentHandle,
    aggregate_id: &ContentId,
    merged_head_id: &burn_p2p::HeadId,
    deadline: Instant,
    stop: Arc<AtomicBool>,
) -> anyhow::Result<(usize, Option<burn_p2p::ValidationOutcome>)>
where
    P: P2pWorkload,
    P::Model: Send + 'static,
{
    let mut attempts = 0usize;
    let mut promoted = None;

    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        promoted = run_demo_validation_attempt(
            label,
            node,
            experiment,
            aggregate_id,
            &mut attempts,
            promoted,
        )?;
        let coordination =
            observe_demo_validation_coordination([node], experiment, aggregate_id, merged_head_id)?;
        if promoted.is_some()
            || coordination.quorum_visible
            || coordination.merge_certificate.is_some()
        {
            stop.store(true, Ordering::Relaxed);
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    Ok((attempts, promoted))
}

#[derive(Default)]
struct DemoValidationCoordination {
    attesters: BTreeSet<PeerId>,
    quorum_visible: bool,
    merge_visible: bool,
    merge_certificate: Option<MergeCertificate>,
}

fn observe_demo_validation_coordination<P, const N: usize>(
    nodes: [&burn_p2p::RunningNode<P>; N],
    experiment: &burn_p2p::ExperimentHandle,
    aggregate_id: &ContentId,
    merged_head_id: &burn_p2p::HeadId,
) -> anyhow::Result<DemoValidationCoordination> {
    let overlay = experiment.overlay_set()?.heads;
    let mut attesters = BTreeSet::new();
    let mut quorum_visible = false;
    let mut merge_visible = false;
    let mut merge_certificate = None;
    for node in nodes {
        let snapshot = node.telemetry().snapshot();
        for announcement in &snapshot.control_plane.reduction_certificate_announcements {
            if announcement.overlay == overlay
                && announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.aggregate_id == *aggregate_id
            {
                attesters.insert(announcement.certificate.promoter_peer_id.clone());
            }
        }
        quorum_visible |= snapshot
            .control_plane
            .validation_quorum_announcements
            .iter()
            .any(|announcement| {
                announcement.overlay == overlay
                    && announcement.certificate.study_id == experiment.study_id
                    && announcement.certificate.experiment_id == experiment.experiment_id
                    && announcement.certificate.revision_id == experiment.revision_id
                    && announcement.certificate.aggregate_id == *aggregate_id
                    && announcement.certificate.merged_head_id == *merged_head_id
            });
        for announcement in &snapshot.control_plane.merge_announcements {
            if announcement.overlay == overlay
                && announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.merged_head_id == *merged_head_id
            {
                merge_visible = true;
                if merge_certificate.is_none() {
                    merge_certificate = Some(announcement.certificate.clone());
                }
            }
        }
    }
    Ok(DemoValidationCoordination {
        attesters,
        quorum_visible,
        merge_visible,
        merge_certificate,
    })
}

fn run_demo_validation_attempt<P>(
    label: &str,
    node: &mut burn_p2p::RunningNode<P>,
    experiment: &burn_p2p::ExperimentHandle,
    aggregate_id: &ContentId,
    attempts: &mut usize,
    promoted: Option<burn_p2p::ValidationOutcome>,
) -> anyhow::Result<Option<burn_p2p::ValidationOutcome>>
where
    P: P2pWorkload,
    P::Model: Send + 'static,
{
    *attempts += 1;
    let started_at = Instant::now();
    eprintln!(
        "mnist reducer validation: {label} attempt {} for aggregate {}",
        *attempts,
        aggregate_id.as_str(),
    );
    let promoted =
        record_promoted_validation_outcome(promoted, node.validate_candidates_once(experiment)?)?;
    eprintln!(
        "mnist reducer validation: {label} attempt {} finished in {:?} promoted={}",
        *attempts,
        started_at.elapsed(),
        promoted.is_some(),
    );
    Ok(promoted)
}

fn log_demo_validation_coordination(
    stage: &str,
    aggregate_id: &ContentId,
    coordination: &DemoValidationCoordination,
) {
    eprintln!(
        "mnist reducer validation: stage={stage} aggregate={} attesters={:?} quorum_visible={} merge_visible={} merge_validator={}",
        aggregate_id.as_str(),
        coordination
            .attesters
            .iter()
            .map(|peer_id| peer_id.as_str().to_owned())
            .collect::<Vec<_>>(),
        coordination.quorum_visible,
        coordination.merge_visible,
        coordination
            .merge_certificate
            .as_ref()
            .map(|certificate| certificate.promoter_peer_id.as_str().to_owned())
            .unwrap_or_else(|| "none".into()),
    );
}

fn wait_for_artifact_from_provider<P, const N: usize>(
    provider: (&str, &burn_p2p::RunningNode<P>),
    consumers: [(&str, &burn_p2p::RunningNode<P>); N],
    provider_peer_id: &PeerId,
    artifact_id: &burn_p2p::ArtifactId,
    timeout: Duration,
    failure_message: &str,
) -> anyhow::Result<()> {
    let mut ignored_provider_peer_ids = Vec::new();
    let mut last_error = None::<String>;
    let _ = record_artifact_provider(
        provider.0,
        provider.1,
        artifact_id,
        &mut ignored_provider_peer_ids,
        &mut last_error,
    );
    wait_for_artifact_from_fixed_providers(
        &consumers,
        std::slice::from_ref(provider_peer_id),
        artifact_id,
        timeout,
        failure_message,
    )
}

fn head_eval_report(
    head: &HeadDescriptor,
    prepared_data: &PreparedMnistData,
    workload_id: WorkloadId,
) -> HeadEvalReport {
    HeadEvalReport {
        network_id: burn_p2p::NetworkId::new(NETWORK_ID),
        experiment_id: head.experiment_id.clone(),
        revision_id: head.revision_id.clone(),
        workload_id,
        head_id: head.head_id.clone(),
        base_head_id: head.parent_head_id.clone(),
        eval_protocol_id: ContentId::new("mnist-eval"),
        evaluator_set_id: ContentId::new("mnist-evaluator-set"),
        metric_values: head.metrics.clone(),
        sample_count: prepared_data.eval_records.len() as u64,
        dataset_view_id: prepared_data.registration.view.dataset_view_id.clone(),
        started_at: head.created_at,
        finished_at: head.created_at,
        trust_class: MetricTrustClass::Canonical,
        status: HeadEvalStatus::Completed,
        signature_bundle: Vec::new(),
    }
}

fn head_eval_report_from_validation(
    validation: &DemoValidationResult,
    prepared_data: &PreparedMnistData,
    workload_id: WorkloadId,
) -> HeadEvalReport {
    HeadEvalReport {
        network_id: burn_p2p::NetworkId::new(NETWORK_ID),
        experiment_id: validation.merged_head.experiment_id.clone(),
        revision_id: validation.merged_head.revision_id.clone(),
        workload_id,
        head_id: validation.merged_head.head_id.clone(),
        base_head_id: validation.merged_head.parent_head_id.clone(),
        eval_protocol_id: ContentId::new("mnist-eval"),
        evaluator_set_id: ContentId::new("mnist-evaluator-set"),
        metric_values: validation.evaluation.metrics.clone(),
        sample_count: prepared_data.eval_records.len() as u64,
        dataset_view_id: prepared_data.registration.view.dataset_view_id.clone(),
        started_at: validation.merged_head.created_at,
        finished_at: validation.evaluation.captured_at,
        trust_class: MetricTrustClass::Canonical,
        status: HeadEvalStatus::Completed,
        signature_bundle: Vec::new(),
    }
}

fn peer_window_metrics_from_training(
    outcome: &TrainingRecord,
    prepared_data: &PreparedMnistData,
    network_manifest: &NetworkManifest,
) -> PeerWindowMetrics {
    let total_window_ms = (outcome.training.timing.completed_at
        - outcome.training.timing.window_started_at)
        .num_milliseconds()
        .max(0) as u64;
    let compute_time_ms =
        total_window_ms.saturating_sub(outcome.training.timing.data_fetch_time_ms);
    PeerWindowMetrics {
        network_id: network_manifest.network_id.clone(),
        experiment_id: outcome.training.head.experiment_id.clone(),
        revision_id: outcome.training.head.revision_id.clone(),
        workload_id: WorkloadId::new(WORKLOAD_ID),
        dataset_view_id: prepared_data.registration.view.dataset_view_id.clone(),
        peer_id: outcome.training.contribution.peer_id.clone(),
        principal_id: Some(PrincipalId::new(format!("principal-{}", outcome.label))),
        lease_id: outcome.training.lease.lease_id.clone(),
        base_head_id: outcome.training.contribution.base_head_id.clone(),
        window_started_at: outcome.training.lease.granted_at,
        window_finished_at: outcome.training.report.completed_at,
        attempted_tokens_or_samples: prepared_data.examples_for_lease(&outcome.training.lease)
            as u64,
        accepted_tokens_or_samples: Some(
            prepared_data.examples_for_lease(&outcome.training.lease) as u64
        ),
        local_train_loss_mean: Some(metric_float(
            &outcome.training.report.stats,
            "train_loss_mean",
        )),
        local_train_loss_last: Some(metric_float(
            &outcome.training.report.stats,
            "train_loss_last",
        )),
        grad_or_delta_norm: None,
        optimizer_step_count: metric_float(&outcome.training.report.stats, "train_steps") as u64,
        compute_time_ms,
        data_fetch_time_ms: outcome.training.timing.data_fetch_time_ms,
        publish_latency_ms: outcome.training.timing.publish_latency_ms,
        head_lag_at_start: 0,
        head_lag_at_finish: 0,
        backend_class: BackendClass::Ndarray,
        role: PeerRole::TrainerCpu,
        status: PeerWindowStatus::Completed,
        status_reason: None,
    }
}

pub(crate) fn trainer_lease_summary(
    outcome: &TrainingRecord,
    prepared_data: &PreparedMnistData,
) -> crate::correctness::export::TrainerLeaseSummary {
    crate::correctness::export::TrainerLeaseSummary {
        node_label: outcome.label.into(),
        peer_id: outcome.training.contribution.peer_id.clone(),
        experiment_id: outcome.training.head.experiment_id.as_str().into(),
        lease_id: outcome.training.lease.lease_id.clone(),
        base_head_id: outcome.training.contribution.base_head_id.clone(),
        accepted_head_id: outcome.training.head.head_id.clone(),
        microshards: outcome.training.lease.microshards.clone(),
        examples: prepared_data.examples_for_lease(&outcome.training.lease),
        window_duration_ms: (outcome.training.timing.completed_at
            - outcome.training.timing.window_started_at)
            .num_milliseconds()
            .max(0) as u64,
        data_fetch_time_ms: outcome.training.timing.data_fetch_time_ms,
        publish_latency_ms: outcome.training.timing.publish_latency_ms,
        metrics: outcome
            .training
            .report
            .stats
            .iter()
            .map(|(key, value)| (key.clone(), metric_json(value)))
            .collect(),
    }
}

pub(crate) fn dynamics_summary(
    input: DynamicsSummaryInput<'_>,
) -> crate::correctness::export::MnistDynamicsSummary {
    let windows = input
        .baseline_outcomes
        .iter()
        .chain(input.low_lr_outcomes.iter())
        .collect::<Vec<_>>();
    let training_window_count = windows.len();
    let total_window_duration_ms = windows
        .iter()
        .map(|outcome| {
            (outcome.training.timing.completed_at - outcome.training.timing.window_started_at)
                .num_milliseconds()
                .max(0) as u64
        })
        .sum::<u64>();
    let total_data_fetch_time_ms = windows
        .iter()
        .map(|outcome| outcome.training.timing.data_fetch_time_ms)
        .sum::<u64>();
    let total_publish_latency_ms = windows
        .iter()
        .map(|outcome| outcome.training.timing.publish_latency_ms)
        .sum::<u64>();
    let total_examples = windows
        .iter()
        .map(|outcome| {
            input
                .prepared_data
                .examples_for_lease(&outcome.training.lease)
        })
        .sum::<usize>();

    crate::correctness::export::MnistDynamicsSummary {
        training_window_count,
        mean_window_duration_ms: if training_window_count == 0 {
            0
        } else {
            total_window_duration_ms / training_window_count as u64
        },
        max_window_duration_ms: windows
            .iter()
            .map(|outcome| {
                (outcome.training.timing.completed_at - outcome.training.timing.window_started_at)
                    .num_milliseconds()
                    .max(0) as u64
            })
            .max()
            .unwrap_or_default(),
        mean_data_fetch_time_ms: if training_window_count == 0 {
            0
        } else {
            total_data_fetch_time_ms / training_window_count as u64
        },
        mean_publish_latency_ms: if training_window_count == 0 {
            0
        } else {
            total_publish_latency_ms / training_window_count as u64
        },
        mean_examples_per_window: if training_window_count == 0 {
            0.0
        } else {
            total_examples as f64 / training_window_count as f64
        },
        global_training_throughput_work_units_per_sec: input
            .performance
            .overall
            .training
            .throughput_work_units_per_sec,
        global_validation_throughput_samples_per_sec: input
            .performance
            .overall
            .head_evaluation
            .throughput_samples_per_sec,
        global_wait_time_ms: input.performance.overall.training.wait_time_ms
            + input.performance.overall.validation.wait_time_ms,
        global_idle_time_ms: input.performance.overall.training.idle_time_ms
            + input.performance.overall.validation.idle_time_ms,
        baseline_accuracy_gain: input.baseline_accuracy - input.baseline_initial_accuracy,
        low_lr_accuracy_gain: input.low_lr_accuracy - input.low_lr_initial_accuracy,
        performance: input.performance,
    }
}

pub(crate) fn node_record<P>(
    label: &str,
    node: &burn_p2p::RunningNode<P>,
    storage_root: &Path,
) -> CoreNodeRecord {
    let snapshot = node.telemetry().snapshot();
    CoreNodeRecord {
        label: label.into(),
        peer_id: snapshot.local_peer_id.expect("node peer id should exist"),
        roles: snapshot
            .configured_roles
            .roles
            .iter()
            .map(|role| format!("{role:?}"))
            .collect(),
        storage_root: storage_root.to_path_buf(),
    }
}

fn collect_node_records<P>(
    nodes: &[(&str, &burn_p2p::RunningNode<P>)],
    storage_root: &Path,
) -> Vec<CoreNodeRecord> {
    nodes.iter()
        .map(|(label, node)| node_record(label, *node, &storage_root.join(label)))
        .collect()
}

fn collect_final_snapshots<P>(
    nodes: &[(&str, &burn_p2p::RunningNode<P>)],
) -> BTreeMap<String, burn_p2p::NodeTelemetrySnapshot> {
    nodes.iter()
        .map(|(label, node)| ((*label).to_string(), node.telemetry().snapshot()))
        .collect()
}

fn browser_seed_records_from_nodes<P>(
    nodes: &[(&str, &burn_p2p::RunningNode<P>)],
) -> Vec<BrowserSeedRecord> {
    nodes.iter()
        .filter_map(|(_, node)| {
            let snapshot = node.telemetry().snapshot();
            let multiaddrs = snapshot
                .listen_addresses
                .iter()
                .map(|address| address.as_str().to_owned())
                .filter(|address| browser_dialable_seed_multiaddr(address))
                .collect::<Vec<_>>();
            if multiaddrs.is_empty() {
                None
            } else {
                Some(BrowserSeedRecord {
                    peer_id: snapshot.local_peer_id,
                    multiaddrs,
                })
            }
        })
        .collect()
}

fn browser_dialable_seed_multiaddr(multiaddr: &str) -> bool {
    let segments = multiaddr
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    (segments.contains(&"webrtc-direct") && segments.contains(&"certhash"))
        || (segments.contains(&"webtransport")
            && segments.contains(&"quic-v1")
            && segments.contains(&"certhash"))
}

fn bootstrap_plan_for_labels(
    bootstrap_seed_label: &str,
    labels: &[&str],
) -> BTreeMap<String, Vec<String>> {
    labels
        .iter()
        .map(|label| {
            (
                (*label).to_string(),
                if *label == bootstrap_seed_label {
                    Vec::new()
                } else {
                    vec![bootstrap_seed_label.to_string()]
                },
            )
        })
        .collect()
}

fn metric_storages_for_labels(storage_root: &Path, labels: &[&str]) -> Vec<StorageConfig> {
    labels
        .iter()
        .map(|label| StorageConfig::new(storage_root.join(label)))
        .collect()
}

fn resolve_browser_provider<'a, P>(
    label: &str,
    nodes: &'a [(&'a str, &'a burn_p2p::RunningNode<P>)],
) -> anyhow::Result<(&'a str, &'a burn_p2p::RunningNode<P>)> {
    nodes.iter()
        .copied()
        .find(|(node_label, _)| *node_label == label)
        .with_context(|| format!("unsupported browser dataset source trainer {label}"))
}

fn write_browser_probe_bundle<P>(input: BrowserProbeWriteInput<'_, P>) -> anyhow::Result<()>
where
    P: P2pWorkload,
{
    write_demo_phase(input.output, "browser-handoff-start")?;
    write_demo_phase(
        input.output,
        &format!(
            "browser-handoff-source-{}",
            input.browser_provider_label
        ),
    )?;
    let browser_dataset = prepare_live_browser_dataset_transport(
        input.browser_provider,
        input.prepared_data,
        input.browser_lease,
    )?;
    let browser_head_artifact = prepare_live_browser_head_artifact_transport(
        input.head_artifact_provider,
        input.baseline_head,
        input.browser_head_providers.to_vec(),
    )?;
    write_demo_phase(input.output, "browser-handoff-bundle-ready")?;
    let browser_node_records = collect_node_records(input.browser_nodes, input.storage_root);
    write_demo_phase(input.output, "browser-handoff-node-records-ready")?;
    let browser_peer_window_metrics = input
        .baseline_outcomes
        .iter()
        .chain(input.low_lr_outcomes.iter())
        .map(|outcome| {
            peer_window_metrics_from_training(outcome, input.prepared_data, input.network_manifest)
        })
        .collect::<Vec<_>>();
    let browser_metrics_catchup = build_metrics_catchup(
        &input.output.join("browser-metrics-store"),
        &browser_peer_window_metrics,
        &[],
        input.head_eval_reports,
    )?;
    write_demo_phase(input.output, "browser-handoff-metrics-ready")?;
    let browser_seed_records = browser_seed_records_from_nodes(input.browser_nodes);
    ensure!(
        !browser_seed_records.is_empty(),
        "live browser probe requires at least one browser-dialable direct seed record"
    );
    let directory_entries = experiment_directory_entries(
        input.network_manifest,
        input.supported_workload,
        input.prepared_data,
        input.promotion_mode.clone(),
        [input.baseline_head, input.low_lr_head],
    );
    let leaderboard = leaderboard_entries(
        &leaderboard_node_pairs(&browser_node_records)?,
        input.baseline_outcomes,
        input.low_lr_outcomes,
        input.merge_certificates,
    );
    let browser_edge_config = LiveBrowserEdgeConfig {
        network_manifest: input.network_manifest.clone(),
        release_manifest: input.release_manifest.clone(),
        workload_id: input.supported_workload.workload_id.clone(),
        principal_id: PrincipalId::new("mnist-browser-trainer"),
        directory_entries,
        heads: vec![
            input.baseline_genesis.clone(),
            input.baseline_head.clone(),
            input.low_lr_genesis.clone(),
            input.low_lr_head.clone(),
        ],
        leaderboard_entries: leaderboard,
        metrics_catchup: browser_metrics_catchup,
        selected_head_id: input.baseline_head.head_id.clone(),
        selected_experiment_id: input.baseline_head.experiment_id.clone(),
        selected_revision_id: input.baseline_head.revision_id.clone(),
        active_lease_id: input.browser_lease.lease_id.clone(),
        leased_microshards: input.browser_lease.microshards.clone(),
        browser_seed_records,
        browser_dataset,
        browser_head_artifact,
    };
    write_demo_phase(input.output, "browser-handoff-edge-config-ready")?;
    write_live_browser_edge_config(input.output, &browser_edge_config)?;
    let peer_dataset_root =
        materialize_live_browser_dataset_bundle(input.output, &browser_edge_config.browser_dataset.bundle)?;
    let peer_head_artifact_root = materialize_live_browser_head_artifact_bundle(
        input.output,
        &browser_edge_config.browser_head_artifact,
    )?;
    write_demo_phase(input.output, "browser-handoff-peer-dataset-ready")?;
    fs::write(
        input.output.join("browser-peer-dataset-root.txt"),
        peer_dataset_root.display().to_string(),
    )
    .with_context(|| {
        format!(
            "failed to write {}",
            input.output.join("browser-peer-dataset-root.txt").display()
        )
    })?;
    fs::write(
        input.output.join("browser-peer-head-artifact-root.txt"),
        peer_head_artifact_root.display().to_string(),
    )
    .with_context(|| {
        format!(
            "failed to write {}",
            input.output
                .join("browser-peer-head-artifact-root.txt")
                .display()
        )
    })?;
    write_demo_phase(input.output, "browser-handoff-config-written")
}

fn collect_demo_artifacts<P>(
    output: &Path,
    storage_root: &Path,
    nodes: &[(&str, &burn_p2p::RunningNode<P>)],
    bootstrap_seed_label: &str,
) -> anyhow::Result<CollectedDemoArtifacts> {
    let labels = nodes.iter().map(|(label, _)| *label).collect::<Vec<_>>();
    write_demo_phase(output, "final-snapshots-start")?;
    let node_records = collect_node_records(nodes, storage_root);
    let bootstrap_plan = bootstrap_plan_for_labels(bootstrap_seed_label, &labels);
    let final_snapshots = collect_final_snapshots(nodes);
    let metric_storages = metric_storages_for_labels(storage_root, &labels);
    let peer_window_metrics =
        load_metric_artifacts_from_storages::<PeerWindowMetrics>(&metric_storages, "peer-window-")?;
    let reducer_cohort_metrics = load_metric_artifacts_from_storages::<ReducerCohortMetrics>(
        &metric_storages,
        "reducer-cohort-",
    )?;
    let head_eval_reports =
        load_metric_artifacts_from_storages::<HeadEvalReport>(&metric_storages, "head-eval-")?;
    let metrics_catchup = build_metrics_catchup(
        &output.join("metrics-store"),
        &peer_window_metrics,
        &reducer_cohort_metrics,
        &head_eval_reports,
    )?;

    Ok(CollectedDemoArtifacts {
        node_records,
        bootstrap_plan,
        final_snapshots,
        metrics_catchup,
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
    })
}

fn shutdown_nodes_in_order<P>(nodes: Vec<(&str, burn_p2p::RunningNode<P>)>) -> anyhow::Result<()> {
    for (label, node) in nodes {
        shutdown_node(label, node)?;
    }
    Ok(())
}

fn build_metrics_catchup(
    metrics_root: &Path,
    peer_window_metrics: &[PeerWindowMetrics],
    reducer_cohort_metrics: &[ReducerCohortMetrics],
    head_eval_reports: &[HeadEvalReport],
) -> anyhow::Result<Vec<burn_p2p_metrics::MetricsCatchupBundle>> {
    let mut metrics_store = MetricsStore::open(metrics_root, MetricsIndexerConfig::default())?;
    metrics_store.sync_from_records(
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
    )?;
    Ok(metrics_store.load_catchup_bundles()?)
}

fn load_metric_artifacts_from_storages<T: serde::de::DeserializeOwned>(
    storages: &[StorageConfig],
    prefix: &str,
) -> anyhow::Result<Vec<T>> {
    let mut paths = Vec::new();
    for storage in storages {
        if !storage.metrics_dir().exists() {
            continue;
        }
        for entry in fs::read_dir(storage.metrics_dir())
            .with_context(|| format!("failed to read {}", storage.metrics_dir().display()))?
        {
            let path = entry?.path();
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if file_name.starts_with(prefix)
                && path.extension().and_then(|extension| extension.to_str()) == Some("json")
            {
                paths.push(path);
            }
        }
    }
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            serde_json::from_slice(
                &fs::read(&path).with_context(|| {
                    format!("failed to read metrics artifact {}", path.display())
                })?,
            )
            .with_context(|| format!("failed to decode metrics artifact {}", path.display()))
        })
        .collect()
}

fn sync_topology_heads<P>(
    nodes: &[(&str, &burn_p2p::RunningNode<P>)],
    provider_peer_ids: &[PeerId],
    experiment: &burn_p2p::ExperimentHandle,
    expected_head: &HeadDescriptor,
    timeout: Duration,
    failure_context: &str,
) -> anyhow::Result<()> {
    if !provider_peer_ids.is_empty() {
        wait_for_head_artifacts(
            nodes,
            provider_peer_ids,
            &expected_head.artifact_id,
            timeout,
            &format!(
                "{failure_context}: promoted head artifact {} was not prewarmed",
                expected_head.artifact_id.as_str()
            ),
        )?;
    }

    let deadline = Instant::now() + timeout;
    let mut synced_labels = BTreeSet::new();
    let mut last_errors = BTreeMap::<String, String>::new();

    while Instant::now() < deadline {
        for (label, node) in nodes {
            if synced_labels.contains(*label) {
                continue;
            }
            match wait_for_specific_head(
                node,
                experiment,
                expected_head,
                Duration::from_millis(250),
                &format!(
                    "{failure_context}: {label} did not sync {}",
                    expected_head.head_id.as_str()
                ),
            ) {
                Ok(_) => {
                    synced_labels.insert((*label).to_string());
                    last_errors.remove(*label);
                }
                Err(error) => {
                    last_errors.insert((*label).to_string(), error.to_string());
                }
            }
        }

        if synced_labels.len() == nodes.len() {
            return Ok(());
        }

        thread::sleep(Duration::from_millis(50));
    }

    let unsynced = nodes
        .iter()
        .map(|(label, _)| *label)
        .filter(|label| !synced_labels.contains(*label))
        .collect::<Vec<_>>();
    let error_suffix = if last_errors.is_empty() {
        String::new()
    } else {
        format!(
            ": {}",
            last_errors
                .into_iter()
                .map(|(label, error)| format!("{label}: {error}"))
                .collect::<Vec<_>>()
                .join("; ")
        )
    };
    anyhow::bail!(
        "{failure_context}: nodes did not converge on {} within {:?}: {}{}",
        expected_head.head_id.as_str(),
        timeout,
        unsynced.join(", "),
        error_suffix,
    )
}

fn republish_head_from_nodes<P, const N: usize>(
    nodes: [(&str, &mut burn_p2p::RunningNode<P>); N],
    experiment: &burn_p2p::ExperimentHandle,
    head: &HeadDescriptor,
) -> anyhow::Result<()>
where
    P: P2pWorkload,
{
    for (label, node) in nodes {
        node.publish_artifact_from_store(&head.artifact_id)
            .with_context(|| {
                format!(
                    "{label} could not republish promoted head artifact {}",
                    head.artifact_id.as_str()
                )
            })?;
        node.publish_head_provider(experiment, head).with_context(|| {
            format!(
                "{label} could not republish promoted head {}",
                head.head_id.as_str()
            )
        })?;
    }
    Ok(())
}

pub(crate) fn metric_float(metrics: &BTreeMap<String, MetricValue>, key: &str) -> f64 {
    match metrics.get(key) {
        Some(MetricValue::Float(value)) => *value,
        Some(MetricValue::Integer(value)) => *value as f64,
        Some(MetricValue::Bool(true)) => 1.0,
        Some(MetricValue::Bool(false)) => 0.0,
        _ => 0.0,
    }
}

pub(crate) fn metric_json(value: &MetricValue) -> serde_json::Value {
    match value {
        MetricValue::Integer(value) => serde_json::json!(value),
        MetricValue::Float(value) => serde_json::json!(value),
        MetricValue::Bool(value) => serde_json::json!(value),
        MetricValue::Text(value) => serde_json::json!(value),
    }
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
        thread::sleep(Duration::from_millis(25));
    }
    anyhow::bail!(failure_message.to_owned())
}

fn wait_for_synced_head<P>(
    node: &burn_p2p::RunningNode<P>,
    experiment: &burn_p2p::ExperimentHandle,
    timeout: Duration,
    failure_message: &str,
) -> anyhow::Result<HeadDescriptor> {
    node.wait_for_experiment_head(experiment, timeout)
        .with_context(|| failure_message.to_owned())
}

fn wait_for_specific_head<P>(
    node: &burn_p2p::RunningNode<P>,
    experiment: &burn_p2p::ExperimentHandle,
    expected_head: &HeadDescriptor,
    timeout: Duration,
    failure_message: &str,
) -> anyhow::Result<HeadDescriptor> {
    node.wait_for_known_head(experiment, expected_head, timeout)
        .with_context(|| failure_message.to_owned())
}

fn shutdown_node<P>(label: &str, node: burn_p2p::RunningNode<P>) -> anyhow::Result<()> {
    eprintln!("mnist demo shutdown: {label}");
    node.shutdown()?;
    let _ = node
        .await_termination_timeout(NODE_SHUTDOWN_TIMEOUT)
        .with_context(|| format!("{label} did not terminate cleanly"))?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::cap_artifact_sync_attempt_timeout;
    use std::time::Duration;

    #[test]
    fn artifact_sync_attempt_timeout_caps_to_remaining_budget() {
        assert_eq!(
            cap_artifact_sync_attempt_timeout(Duration::from_secs(90), Duration::from_secs(45)),
            Some(Duration::from_secs(45))
        );
        assert_eq!(
            cap_artifact_sync_attempt_timeout(Duration::from_secs(12), Duration::from_secs(45)),
            Some(Duration::from_secs(12))
        );
    }

    #[test]
    fn artifact_sync_attempt_timeout_returns_none_when_budget_exhausted() {
        assert_eq!(
            cap_artifact_sync_attempt_timeout(Duration::ZERO, Duration::from_secs(45)),
            None
        );
    }
}
