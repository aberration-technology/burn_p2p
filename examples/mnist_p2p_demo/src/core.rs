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

use anyhow::Context;
use burn::{
    backend::{Autodiff, NdArray},
    optim::AdamConfig,
    train::{InferenceStep, Learner},
};
use burn_p2p::{
    BrowserRolePolicy, BrowserVisibilityPolicy, ClientPlatform, ClientReleaseManifest, ContentId,
    ExperimentDirectoryEntry, ExperimentDirectoryPolicyExt, ExperimentId, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, HeadDescriptor,
    HeadEvalReport, HeadEvalStatus, LagPolicy, MergeCertificate, MetricTrustClass,
    MetricValue, NetworkManifest, P2pWorkload, PeerId, PeerRole, PeerRoleSet, PeerWindowMetrics,
    PeerWindowStatus, Precision, PrincipalId, ProjectFamilyId, ReducerCohortMetrics,
    RevisionId, RevisionManifest, StorageConfig, StudyId, SupportedWorkload, WindowActivation,
    WindowId, WorkloadId,
    burn::{BurnTarget, from_learner, from_loaders, inspect_module},
};
use burn_p2p_core::{BackendClass, BrowserLeaderboardEntry, BrowserLeaderboardIdentity};
use burn_p2p_metrics::{MetricsIndexerConfig, MetricsStore};
use chrono::Utc;
use semver::Version;

use crate::{
    args::Args,
    correctness::live_browser::{
        LiveBrowserEdgeConfig, LiveBrowserProbeManifest,
        materialize_live_browser_dataset_bundle, materialize_live_browser_head_artifact_bundle,
        prepare_live_browser_dataset_transport, prepare_live_browser_head_artifact_transport,
        write_live_browser_edge_config,
    },
    correctness::export::{DemoPhaseEvent, DemoPhaseSummary, DemoPhaseTiming},
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
const TRAINER_B_LABEL: &str = "trainer-b";
const TRAINER_LATE_LABEL: &str = "trainer-late";
const TOPOLOGY_HEAD_SYNC_TIMEOUT: Duration = Duration::from_secs(45);
const FOLLOWER_HEAD_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(45);
const NODE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const DEMO_VALIDATION_ROUND_TIMEOUT: Duration = Duration::from_secs(45);
const DEMO_ARTIFACT_SYNC_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const RESTART_CANDIDATE_ARTIFACT_TIMEOUT: Duration = Duration::from_secs(90);

pub(crate) struct CoreNodeRecord {
    pub label: String,
    pub peer_id: PeerId,
    pub roles: Vec<String>,
    pub storage_root: PathBuf,
}

pub(crate) struct CoreMnistRun {
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
    pub trainer_restart_reconnected: bool,
    pub trainer_restart_resumed_training: bool,
    pub late_joiner_synced_checkpoint: bool,
    pub phase_timeline: DemoPhaseSummary,
    pub browser_probe_manifest: Option<LiveBrowserProbeManifest>,
    pub browser_probe_summary: Option<serde_json::Value>,
}

#[derive(Clone)]
struct DemoValidationResult {
    merged_head: HeadDescriptor,
    merge_certificate: MergeCertificate,
    evaluation: burn_p2p::MetricReport,
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
    let release_manifest = release_manifest(&supported_workload);
    let network_manifest = network_manifest(&release_manifest);

    let build_trainer_project = |learning_rate: f64| -> anyhow::Result<_> {
        let device = <RuntimeBackend as burn::tensor::backend::Backend>::Device::default();
        let prepared_for_eval = prepared_data.clone();
        let train_loader = prepared_data.build_train_loader(&device);
        let eval_loader = prepared_data.build_eval_loader(&device);
        Ok(from_loaders(
            Learner::new(
                MnistModel::<RuntimeBackend>::new(&device),
                AdamConfig::new().init(),
                learning_rate,
            ),
            device,
            train_loader,
            eval_loader,
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
        let eval_loader = prepared_data.build_eval_loader(&device);
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
        .with_eval_loader(eval_loader)
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
        .connect(
            BurnTarget::Custom(PeerRoleSet::new([
                PeerRole::Bootstrap,
                PeerRole::RelayHelper,
            ])),
            release_manifest.clone(),
            supported_workload.clone(),
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
        .validator(release_manifest.clone(), supported_workload.clone())?
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
        .connect(
            BurnTarget::Custom(PeerRoleSet::new([PeerRole::Reducer])),
            release_manifest.clone(),
            supported_workload.clone(),
        )?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(REDUCER_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;

    let validator_b = build_validator_project(1.0e-3)?
        .validator(release_manifest.clone(), supported_workload.clone())?
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
        .connect(
            BurnTarget::Custom(PeerRoleSet::new([
                PeerRole::Viewer,
                PeerRole::BrowserObserver,
            ])),
            release_manifest.clone(),
            supported_workload.clone(),
        )?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(VIEWER_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;

    let trainer_a1 = build_trainer_project(1.0e-3)?
        .trainer(release_manifest.clone(), supported_workload.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(TRAINER_A1_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;
    let trainer_a2 = build_trainer_project(1.0e-3)?
        .trainer(release_manifest.clone(), supported_workload.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(TRAINER_A2_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;
    let trainer_b = build_trainer_project(2.0e-4)?
        .trainer(release_manifest.clone(), supported_workload.clone())?
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
                &&
                trainer_a1.telemetry().snapshot().connected_peers >= 1
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
        [
            (REDUCER_LABEL, &reducer),
            (VALIDATOR_B_LABEL, &validator_b),
            (TRAINER_A1_LABEL, &trainer_a1),
            (TRAINER_A2_LABEL, &trainer_a2),
        ],
        &validator_peer_id,
        &baseline_head.artifact_id,
        Duration::from_secs(20),
        "baseline genesis head artifact was not fetchable from validator",
    )?;
    wait_for_artifact_from_provider(
        [
            (REDUCER_LABEL, &reducer),
            (VALIDATOR_B_LABEL, &validator_b),
            (TRAINER_B_LABEL, &trainer_b),
        ],
        &validator_peer_id,
        &low_lr_head.artifact_id,
        Duration::from_secs(20),
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
    let validator_peer_ids = BTreeSet::from([
        validator_peer_id.clone(),
        validator_b_peer_id.clone(),
    ]);
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
                (REDUCER_LABEL, &reducer),
                (VALIDATOR_LABEL, &validator),
                (VALIDATOR_B_LABEL, &validator_b),
            ],
            &baseline,
            [&outcome_a1, &outcome_a2],
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
            validation.merge_certificate.validator.clone(),
            reducer_peer_id.clone(),
            validator_peer_id.clone(),
            validator_b_peer_id.clone(),
        ];
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

    write_demo_phase(&output, "restart-trainer-shutdown-start")?;
    shutdown_node(TRAINER_A2_LABEL, trainer_a2)?;
    write_demo_phase(&output, "restart-trainer-shutdown-complete")?;
    let mut trainer_a2 = build_trainer_project(1.0e-3)?
        .trainer(release_manifest.clone(), supported_workload.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(TRAINER_A2_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;
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
    let trainer_restart_reconnected = true;
    let restarted_outcome = run_training_round(
        &mut trainer_a2,
        TRAINER_A2_LABEL,
        &baseline,
        Some(&baseline_head),
        &prepared_data,
    )?;
    write_demo_phase(&output, "restart-round-trained")?;
    wait_for_candidate_artifacts(
        [
            (REDUCER_LABEL, &reducer),
            (VALIDATOR_LABEL, &validator),
            (VALIDATOR_B_LABEL, &validator_b),
        ],
        &baseline,
        [&restarted_outcome],
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
        validation.merge_certificate.validator.clone(),
        reducer_peer_id.clone(),
        validator_peer_id.clone(),
        validator_b_peer_id.clone(),
    ];
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
    write_demo_phase(&output, "restart-round-topology-synced")?;
    merge_certificates.push(validation.merge_certificate.clone());
    head_eval_reports.push(head_eval_report_from_validation(
        &validation,
        &prepared_data,
        supported_workload.workload_id.clone(),
    ));
    let trainer_restart_resumed_training = true;

    let late_joiner = build_trainer_project(1.0e-3)?
        .trainer(release_manifest.clone(), supported_workload.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join(TRAINER_LATE_LABEL)))
        .with_bootstrap_peer(helper_addr.clone())
        .spawn()?;
    wait_for(
        Duration::from_secs(15),
        || late_joiner.telemetry().snapshot().connected_peers >= 1,
        "late joiner did not connect",
    )?;
    wait_for_head_artifacts(
        &[(TRAINER_LATE_LABEL, &late_joiner)],
        &baseline_head_providers,
        &baseline_head.artifact_id,
        Duration::from_secs(20),
        "late joiner could not prewarm the promoted baseline head artifact",
    )?;
    let synced_head = wait_for_specific_head(
        &late_joiner,
        &baseline,
        &baseline_head,
        FOLLOWER_HEAD_DISCOVERY_TIMEOUT,
        "late joiner did not discover baseline head",
    )?;
    let late_joiner_store = late_joiner
        .artifact_store()
        .context("late joiner missing artifact store")?;
    let late_joiner_synced_checkpoint = late_joiner_store.has_manifest(&synced_head.artifact_id);
    write_demo_phase(&output, "late-joiner-synced")?;

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
        wait_for_candidate_artifacts(
            [
                (REDUCER_LABEL, &reducer),
                (VALIDATOR_LABEL, &validator),
                (VALIDATOR_B_LABEL, &validator_b),
            ],
            &low_lr,
            [&low_lr_outcome],
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
            validation.merge_certificate.validator.clone(),
            reducer_peer_id.clone(),
            validator_peer_id.clone(),
            validator_b_peer_id.clone(),
        ];
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

    if args.live_browser_probe {
        write_demo_phase(&output, "browser-handoff-start")?;
        let browser_source = baseline_outcomes
            .last()
            .context("mnist demo did not produce a baseline trainer lease for live browser probe")?
            .clone();
        let (browser_provider_label, browser_provider) = match browser_source.label {
            TRAINER_A1_LABEL => (TRAINER_A1_LABEL, &trainer_a1),
            TRAINER_A2_LABEL => (TRAINER_A2_LABEL, &trainer_a2),
            other => anyhow::bail!("unsupported browser dataset source trainer {other}"),
        };
        write_demo_phase(
            &output,
            &format!("browser-handoff-source-{browser_provider_label}"),
        )?;
        let browser_lease = browser_source.training.lease.clone();
        let browser_dataset = prepare_live_browser_dataset_transport(
            browser_provider,
            &prepared_data,
            &browser_lease,
        )?;
        let browser_head_artifact = prepare_live_browser_head_artifact_transport(
            &validator,
            &baseline_head,
            baseline_head_providers.clone(),
        )?;
        write_demo_phase(&output, "browser-handoff-bundle-ready")?;
        let browser_node_records = collect_node_records(
            &helper,
            &validator,
            &validator_b,
            &reducer,
            &viewer,
            &trainer_a1,
            &trainer_a2,
            &trainer_b,
            None,
            &storage_root,
        );
        write_demo_phase(&output, "browser-handoff-node-records-ready")?;
        let browser_peer_window_metrics = baseline_outcomes
            .iter()
            .chain(low_lr_outcomes.iter())
            .map(|outcome| {
                peer_window_metrics_from_training(outcome, &prepared_data, &network_manifest)
            })
            .collect::<Vec<_>>();
        let browser_metrics_catchup = build_metrics_catchup(
            &output.join("browser-metrics-store"),
            &browser_peer_window_metrics,
            &[],
            &head_eval_reports,
        )?;
        write_demo_phase(&output, "browser-handoff-metrics-ready")?;
        let directory_entries = experiment_directory_entries(
            &network_manifest,
            &supported_workload,
            &prepared_data,
            [&baseline_head, &low_lr_head],
        );
        let leaderboard = leaderboard_entries(
            &leaderboard_node_pairs(&browser_node_records)?,
            &baseline_outcomes,
            &low_lr_outcomes,
            &merge_certificates,
        );
        let peer_dataset_root =
            materialize_live_browser_dataset_bundle(&output, &browser_dataset.bundle)?;
        let peer_head_artifact_root =
            materialize_live_browser_head_artifact_bundle(&output, &browser_head_artifact)?;
        write_demo_phase(&output, "browser-handoff-peer-dataset-ready")?;
        let browser_edge_config = LiveBrowserEdgeConfig {
            network_manifest: network_manifest.clone(),
            release_manifest: release_manifest.clone(),
            workload_id: supported_workload.workload_id.clone(),
            directory_entries,
            heads: vec![
                baseline_genesis.clone(),
                baseline_head.clone(),
                low_lr_genesis.clone(),
                low_lr_head.clone(),
            ],
            leaderboard_entries: leaderboard,
            metrics_catchup: browser_metrics_catchup,
            selected_head_id: baseline_head.head_id.clone(),
            selected_experiment_id: baseline_head.experiment_id.clone(),
            selected_revision_id: baseline_head.revision_id.clone(),
            active_lease_id: browser_lease.lease_id.clone(),
            leased_microshards: browser_lease.microshards.clone(),
            browser_dataset,
            browser_head_artifact,
        };
        write_demo_phase(&output, "browser-handoff-edge-config-ready")?;
        write_live_browser_edge_config(&output, &browser_edge_config)?;
        fs::write(
            output.join("browser-peer-dataset-root.txt"),
            peer_dataset_root.display().to_string(),
        )
        .with_context(|| {
            format!(
                "failed to write {}",
                output.join("browser-peer-dataset-root.txt").display()
            )
        })?;
        fs::write(
            output.join("browser-peer-head-artifact-root.txt"),
            peer_head_artifact_root.display().to_string(),
        )
        .with_context(|| {
            format!(
                "failed to write {}",
                output.join("browser-peer-head-artifact-root.txt").display()
            )
        })?;
        write_demo_phase(&output, "browser-handoff-config-written")?;
    }

    let node_records = collect_node_records(
        &helper,
        &validator,
        &validator_b,
        &reducer,
        &viewer,
        &trainer_a1,
        &trainer_a2,
        &trainer_b,
        Some(node_record(
            TRAINER_LATE_LABEL,
            &late_joiner,
            &storage_root.join(TRAINER_LATE_LABEL),
        )),
        &storage_root,
    );
    let bootstrap_plan = BTreeMap::from([
        (HELPER_LABEL.to_string(), Vec::new()),
        (VALIDATOR_LABEL.to_string(), vec![HELPER_LABEL.to_string()]),
        (VALIDATOR_B_LABEL.to_string(), vec![HELPER_LABEL.to_string()]),
        (REDUCER_LABEL.to_string(), vec![HELPER_LABEL.to_string()]),
        (VIEWER_LABEL.to_string(), vec![HELPER_LABEL.to_string()]),
        (TRAINER_A1_LABEL.to_string(), vec![HELPER_LABEL.to_string()]),
        (TRAINER_A2_LABEL.to_string(), vec![HELPER_LABEL.to_string()]),
        (TRAINER_B_LABEL.to_string(), vec![HELPER_LABEL.to_string()]),
        (TRAINER_LATE_LABEL.to_string(), vec![HELPER_LABEL.to_string()]),
    ]);
    write_demo_phase(&output, "final-snapshots-start")?;
    let final_snapshots = BTreeMap::from([
        (HELPER_LABEL.to_string(), helper.telemetry().snapshot()),
        (VALIDATOR_LABEL.to_string(), validator.telemetry().snapshot()),
        (VALIDATOR_B_LABEL.to_string(), validator_b.telemetry().snapshot()),
        (REDUCER_LABEL.to_string(), reducer.telemetry().snapshot()),
        (VIEWER_LABEL.to_string(), viewer.telemetry().snapshot()),
        (TRAINER_A1_LABEL.to_string(), trainer_a1.telemetry().snapshot()),
        (TRAINER_A2_LABEL.to_string(), trainer_a2.telemetry().snapshot()),
        (TRAINER_B_LABEL.to_string(), trainer_b.telemetry().snapshot()),
        (TRAINER_LATE_LABEL.to_string(), late_joiner.telemetry().snapshot()),
    ]);
    let metric_storages = [
        StorageConfig::new(storage_root.join(HELPER_LABEL)),
        StorageConfig::new(storage_root.join(VALIDATOR_LABEL)),
        StorageConfig::new(storage_root.join(VALIDATOR_B_LABEL)),
        StorageConfig::new(storage_root.join(REDUCER_LABEL)),
        StorageConfig::new(storage_root.join(VIEWER_LABEL)),
        StorageConfig::new(storage_root.join(TRAINER_A1_LABEL)),
        StorageConfig::new(storage_root.join(TRAINER_A2_LABEL)),
        StorageConfig::new(storage_root.join(TRAINER_B_LABEL)),
        StorageConfig::new(storage_root.join(TRAINER_LATE_LABEL)),
    ];
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

    write_demo_phase(&output, "shutdown-start")?;
    shutdown_node(TRAINER_LATE_LABEL, late_joiner)?;
    shutdown_node(TRAINER_B_LABEL, trainer_b)?;
    shutdown_node(TRAINER_A2_LABEL, trainer_a2)?;
    shutdown_node(TRAINER_A1_LABEL, trainer_a1)?;
    shutdown_node(VIEWER_LABEL, viewer)?;
    shutdown_node(VALIDATOR_B_LABEL, validator_b)?;
    shutdown_node(REDUCER_LABEL, reducer)?;
    shutdown_node(VALIDATOR_LABEL, validator)?;
    shutdown_node(HELPER_LABEL, helper)?;
    write_demo_phase(&output, "shutdown-complete")?;
    let phase_timeline = read_demo_phase_summary(&output)?;

    Ok(CoreMnistRun {
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
    Ok(TrainingRecord {
        label,
        training,
    })
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
            run_training_round(trainer_a, label_a, experiment, Some(pinned_head), prepared_data)
        });
        let training_b = scope.spawn(move || {
            training_b_start.wait();
            run_training_round(trainer_b, label_b, experiment, Some(pinned_head), prepared_data)
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
        let update_visible = snapshot.control_plane.update_announcements.iter().any(|announcement| {
            announcement.overlay == overlay
                && announcement.update.study_id == experiment.study_id
                && announcement.update.experiment_id == experiment.experiment_id
                && announcement.update.revision_id == experiment.revision_id
                && announcement.update.peer_id == training.contribution.peer_id
                && announcement.update.window_id == training.lease.window_id
                && announcement.update.base_head_id == training.contribution.base_head_id
                && announcement.update.delta_artifact_id == training.artifact.artifact_id
        });
        let head_visible = snapshot.control_plane.head_announcements.iter().any(|announcement| {
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

fn wait_for_candidate_artifacts<P, const NODE_COUNT: usize, const N: usize>(
    consumers: [(&str, &burn_p2p::RunningNode<P>); NODE_COUNT],
    experiment: &burn_p2p::ExperimentHandle,
    outcomes: [&TrainingRecord; N],
    timeout: Duration,
    failure_message: &str,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_error = None::<String>;
    while Instant::now() < deadline {
        let mut all_ready = true;
        for (label, consumer) in &consumers {
            for outcome in outcomes {
                match consumer.wait_for_artifact_from_peers(
                    std::slice::from_ref(&outcome.training.contribution.peer_id),
                    &outcome.training.head.artifact_id,
                    DEMO_ARTIFACT_SYNC_ATTEMPT_TIMEOUT,
                ) {
                    Ok(_) => {
                        consumer.publish_head_provider(experiment, &outcome.training.head)?;
                    }
                    Err(error) => {
                        all_ready = false;
                        last_error = Some(format!("{label}: {error}"));
                        break;
                    }
                }
            }
            if !all_ready {
                break;
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
                if let Err(error) = observer.seed_training_candidate(experiment, &outcome.training) {
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
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    while Instant::now() < deadline {
        let mut all_ready = true;
        for (label, consumer) in consumers {
            if let Err(error) = consumer.wait_for_artifact_from_peers(
                provider_peer_ids,
                artifact_id,
                DEMO_ARTIFACT_SYNC_ATTEMPT_TIMEOUT,
            )
            {
                all_ready = false;
                last_error = Some(format!("{label}: {error}"));
                break;
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

    eprintln!(
        "mnist demo phase: {phase} (+{}ms)",
        elapsed_ms_since_start
    );
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
    .with_context(|| format!("failed to write {}", output_root.join("phase.json").display()))
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
        checkpoint_format_hash: ContentId::new("mnist-demo-burnpack"),
        supported_revision_family: ContentId::new("mnist-demo-revision-family"),
        resource_class: "cpu".into(),
    })
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
        authority_public_keys: vec!["mnist-validator".into()],
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: ContentId::new("mnist-demo-auth-policy"),
        created_at: Utc::now(),
        description: "single-machine mnist sanity net".into(),
    }
}

pub(crate) fn experiment_directory_entries(
    network_manifest: &NetworkManifest,
    supported_workload: &SupportedWorkload,
    prepared_data: &PreparedMnistData,
    heads: [&HeadDescriptor; 2],
) -> Vec<ExperimentDirectoryEntry> {
    let make_entry = |head: &HeadDescriptor, display_name: &str, learning_rate: &str| {
        let mut entry = ExperimentDirectoryEntry {
            network_id: network_manifest.network_id.clone(),
            study_id: StudyId::new(STUDY_ID),
            experiment_id: head.experiment_id.clone(),
            workload_id: supported_workload.workload_id.clone(),
            display_name: display_name.into(),
            model_schema_hash: ContentId::new("mnist-demo-schema"),
            dataset_view_id: prepared_data.registration.view.dataset_view_id.clone(),
            resource_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::from([PeerRole::TrainerCpu]),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: Some(512 * 1024 * 1024),
                estimated_download_bytes: prepared_data.microshard_plan.sizing.total_bytes,
                estimated_window_seconds: 1,
            },
            visibility: ExperimentVisibility::Public,
            opt_in_policy: ExperimentOptInPolicy::Open,
            current_revision_id: head.revision_id.clone(),
            current_head_id: Some(head.head_id.clone()),
            allowed_roles: PeerRoleSet::new([
                PeerRole::TrainerCpu,
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
                    experiment_id: head.experiment_id.clone(),
                },
                ExperimentScope::Validate {
                    experiment_id: head.experiment_id.clone(),
                },
            ]),
            metadata: BTreeMap::from([("learning_rate".into(), learning_rate.into())]),
        };
        entry.apply_revision_policy(&RevisionManifest {
            experiment_id: head.experiment_id.clone(),
            revision_id: head.revision_id.clone(),
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
        entry
    };

    vec![
        make_entry(heads[0], "MNIST baseline", "1.0e-3"),
        make_entry(heads[1], "MNIST lower learning rate", "2.0e-4"),
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
            .entry(certificate.validator.clone())
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

pub(crate) fn node_peer_id(
    node_records: &[CoreNodeRecord],
    label: &str,
) -> anyhow::Result<PeerId> {
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
        [
            (VALIDATOR_LABEL, validator),
            (VALIDATOR_B_LABEL, validator_b),
        ],
        reducer_peer_id,
        &reduced.aggregate.aggregate_artifact_id,
        Duration::from_secs(20),
        "validators could not fetch the dedicated reducer aggregate artifact",
    )?;

    wait_for(
        Duration::from_secs(10),
        || {
            let expected_aggregate_id = &reduced.aggregate.aggregate_id;
            let expected_artifact_id = &reduced.aggregate.aggregate_artifact_id;
            [validator.telemetry().snapshot(), validator_b.telemetry().snapshot()]
                .into_iter()
                .all(|snapshot| {
                    snapshot
                        .control_plane
                        .aggregate_proposal_announcements
                        .iter()
                        .any(|announcement| {
                            announcement.proposal.aggregate_id == *expected_aggregate_id
                                && announcement.proposal.aggregate_artifact_id
                                    == *expected_artifact_id
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
    let (
        validator_outcome,
        validator_b_outcome,
        validator_attempts,
        validator_b_attempts,
    ) = run_parallel_demo_validation_drivers(
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
        validator_peer_ids.contains(&validation.merge_certificate.validator),
        "merge promotion came from non-validator peer {}",
            validation.merge_certificate.validator,
    );
    if validation.merge_certificate.validator == validator_peer_id {
        validator.publish_head_provider(experiment, &validation.merged_head)?;
        wait_for_artifact_from_provider(
            [
                (REDUCER_LABEL, reducer),
                (VALIDATOR_B_LABEL, validator_b),
            ],
            &validator_peer_id,
            &validation.merged_head.artifact_id,
            Duration::from_secs(20),
            "reducer tier could not fetch the promoted merged head from validator",
        )?;
    } else {
        validator_b.publish_head_provider(experiment, &validation.merged_head)?;
        wait_for_artifact_from_provider(
            [
                (REDUCER_LABEL, reducer),
                (VALIDATOR_LABEL, validator),
            ],
            &validator_b_peer_id,
            &validation.merged_head.artifact_id,
            Duration::from_secs(20),
            "reducer tier could not fetch the promoted merged head from validator-b",
        )?;
    }
    reducer.publish_head_provider(experiment, &validation.merged_head)?;
    validator.publish_head_provider(experiment, &validation.merged_head)?;
    validator_b.publish_head_provider(experiment, &validation.merged_head)?;
    Ok(validation)
}

fn record_promoted_validation_outcome(
    promoted: Option<burn_p2p::ValidationOutcome>,
    outcome: Option<burn_p2p::ValidationOutcome>,
) -> anyhow::Result<Option<burn_p2p::ValidationOutcome>> {
    let Some(outcome) = outcome else {
        return Ok(promoted);
    };
    if let Some(existing) = &promoted {
        anyhow::ensure!(
            existing.merged_head.head_id == outcome.merged_head.head_id,
            "validators promoted different merged heads for the same aggregate proposal",
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

        let (validator_attempts, validator_outcome) = validator_run
            .join()
            .map_err(|_| anyhow::anyhow!("parallel validation thread {VALIDATOR_LABEL} panicked"))??;
        let (validator_b_attempts, validator_b_outcome) = validator_b_run
            .join()
            .map_err(|_| anyhow::anyhow!("parallel validation thread {VALIDATOR_B_LABEL} panicked"))??;
        Ok((validator_outcome, validator_b_outcome, validator_attempts, validator_b_attempts))
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
        promoted =
            run_demo_validation_attempt(label, node, experiment, aggregate_id, &mut attempts, promoted)?;
        let coordination = observe_demo_validation_coordination(
            [node],
            experiment,
            aggregate_id,
            merged_head_id,
        )?;
        if promoted.is_some() || coordination.quorum_visible || coordination.merge_certificate.is_some() {
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
                attesters.insert(announcement.certificate.validator.clone());
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
    let promoted = record_promoted_validation_outcome(
        promoted,
        node.validate_candidates_once(experiment)?,
    )?;
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
            .map(|certificate| certificate.validator.as_str().to_owned())
            .unwrap_or_else(|| "none".into()),
    );
}

fn wait_for_artifact_from_provider<P, const N: usize>(
    consumers: [(&str, &burn_p2p::RunningNode<P>); N],
    provider_peer_id: &PeerId,
    artifact_id: &burn_p2p::ArtifactId,
    timeout: Duration,
    failure_message: &str,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    while Instant::now() < deadline {
        let mut all_ready = true;
        for (label, consumer) in &consumers {
            if let Err(error) = consumer.wait_for_artifact_from_peers(
                std::slice::from_ref(provider_peer_id),
                artifact_id,
                DEMO_ARTIFACT_SYNC_ATTEMPT_TIMEOUT,
            ) {
                all_ready = false;
                last_error = Some(format!("{label}: {error}"));
                break;
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
        evaluator_set_id: ContentId::new("validator-set"),
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
        evaluator_set_id: ContentId::new("validator-set"),
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
    baseline_outcomes: &[TrainingRecord],
    low_lr_outcomes: &[TrainingRecord],
    baseline_initial_accuracy: f64,
    baseline_accuracy: f64,
    low_lr_initial_accuracy: f64,
    low_lr_accuracy: f64,
    prepared_data: &PreparedMnistData,
    performance: crate::correctness::export::MnistPerformanceSummary,
) -> crate::correctness::export::MnistDynamicsSummary {
    let windows = baseline_outcomes
        .iter()
        .chain(low_lr_outcomes.iter())
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
        .map(|outcome| prepared_data.examples_for_lease(&outcome.training.lease))
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
        global_training_throughput_work_units_per_sec: performance
            .overall
            .training
            .throughput_work_units_per_sec,
        global_validation_throughput_samples_per_sec: performance
            .overall
            .head_evaluation
            .throughput_samples_per_sec,
        global_wait_time_ms: performance.overall.training.wait_time_ms
            + performance.overall.validation.wait_time_ms,
        global_idle_time_ms: performance.overall.training.idle_time_ms
            + performance.overall.validation.idle_time_ms,
        baseline_accuracy_gain: baseline_accuracy - baseline_initial_accuracy,
        low_lr_accuracy_gain: low_lr_accuracy - low_lr_initial_accuracy,
        performance,
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

fn collect_node_records<PH, PV, PVB, PR, PVW, PTA1, PTA2, PTB>(
    helper: &burn_p2p::RunningNode<PH>,
    validator: &burn_p2p::RunningNode<PV>,
    validator_b: &burn_p2p::RunningNode<PVB>,
    reducer: &burn_p2p::RunningNode<PR>,
    viewer: &burn_p2p::RunningNode<PVW>,
    trainer_a1: &burn_p2p::RunningNode<PTA1>,
    trainer_a2: &burn_p2p::RunningNode<PTA2>,
    trainer_b: &burn_p2p::RunningNode<PTB>,
    late_joiner_record: Option<CoreNodeRecord>,
    storage_root: &Path,
) -> Vec<CoreNodeRecord> {
    let mut records = vec![
        node_record(HELPER_LABEL, helper, &storage_root.join(HELPER_LABEL)),
        node_record(VALIDATOR_LABEL, validator, &storage_root.join(VALIDATOR_LABEL)),
        node_record(
            VALIDATOR_B_LABEL,
            validator_b,
            &storage_root.join(VALIDATOR_B_LABEL),
        ),
        node_record(REDUCER_LABEL, reducer, &storage_root.join(REDUCER_LABEL)),
        node_record(VIEWER_LABEL, viewer, &storage_root.join(VIEWER_LABEL)),
        node_record(
            TRAINER_A1_LABEL,
            trainer_a1,
            &storage_root.join(TRAINER_A1_LABEL),
        ),
        node_record(
            TRAINER_A2_LABEL,
            trainer_a2,
            &storage_root.join(TRAINER_A2_LABEL),
        ),
        node_record(TRAINER_B_LABEL, trainer_b, &storage_root.join(TRAINER_B_LABEL)),
    ];
    if let Some(late_joiner_record) = late_joiner_record {
        records.push(late_joiner_record);
    }
    records
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
            serde_json::from_slice(&fs::read(&path).with_context(|| {
                format!("failed to read metrics artifact {}", path.display())
            })?)
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
