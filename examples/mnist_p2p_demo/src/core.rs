use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
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
    HeadEvalReport, HeadEvalStatus, LagPolicy, MergeCertificate, MetricTrustClass, MetricValue,
    NetworkManifest, P2pWorkload, PeerId, PeerRole, PeerRoleSet, PeerWindowMetrics,
    PeerWindowStatus, Precision, PrincipalId, ProjectFamilyId, RevisionId, RevisionManifest,
    StorageConfig, StudyId, SupportedWorkload, WindowActivation, WindowId, WorkloadId,
    burn::{BurnTarget, from_learner, from_loaders, inspect_module},
};
use burn_p2p_core::{BackendClass, BrowserLeaderboardEntry, BrowserLeaderboardIdentity};
use burn_p2p_metrics::{MetricsIndexerConfig, MetricsStore};
use chrono::Utc;
use semver::Version;

use crate::{
    args::Args,
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
    pub node_records: Vec<CoreNodeRecord>,
    pub restarted_trainer_label: String,
    pub trainer_restart_reconnected: bool,
    pub trainer_restart_resumed_training: bool,
    pub late_joiner_synced_checkpoint: bool,
}

pub(crate) fn run_core_demo(args: &Args) -> anyhow::Result<CoreMnistRun> {
    let output = args.output.clone();
    if output.exists() {
        std::fs::remove_dir_all(&output)
            .with_context(|| format!("failed to reset {}", output.display()))?;
    }
    std::fs::create_dir_all(&output)?;
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

    let validator = build_validator_project(1.0e-3)?
        .validator(release_manifest.clone(), supported_workload.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join("validator")))
        .spawn()?;
    wait_for(
        Duration::from_secs(10),
        || {
            let snapshot = validator.telemetry().snapshot();
            snapshot.status == burn_p2p::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
                && snapshot.local_peer_id.is_some()
        },
        "validator did not start",
    )?;
    let validator_addr = validator.telemetry().snapshot().listen_addresses[0].clone();

    let helper = build_service_project(1.0e-3)?
        .connect(
            BurnTarget::Custom(PeerRoleSet::new([
                PeerRole::Bootstrap,
                PeerRole::RelayHelper,
                PeerRole::Archive,
            ])),
            release_manifest.clone(),
            supported_workload.clone(),
        )?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join("helper")))
        .with_bootstrap_peer(validator_addr.clone())
        .spawn()?;
    wait_for(
        Duration::from_secs(10),
        || {
            let snapshot = helper.telemetry().snapshot();
            snapshot.status == burn_p2p::RuntimeStatus::Running
                && snapshot.connected_peers >= 1
                && snapshot.local_peer_id.is_some()
        },
        "helper node did not start",
    )?;

    let viewer = build_service_project(1.0e-3)?
        .connect(
            BurnTarget::Custom(PeerRoleSet::new([
                PeerRole::PortalViewer,
                PeerRole::BrowserObserver,
            ])),
            release_manifest.clone(),
            supported_workload.clone(),
        )?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join("viewer")))
        .with_bootstrap_peer(validator_addr.clone())
        .spawn()?;

    let trainer_a1 = build_trainer_project(1.0e-3)?
        .trainer(release_manifest.clone(), supported_workload.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join("trainer-a1")))
        .with_bootstrap_peer(validator_addr.clone())
        .spawn()?;
    let trainer_a2 = build_trainer_project(1.0e-3)?
        .trainer(release_manifest.clone(), supported_workload.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join("trainer-a2")))
        .with_bootstrap_peer(validator_addr.clone())
        .spawn()?;
    let trainer_b = build_trainer_project(2.0e-4)?
        .trainer(release_manifest.clone(), supported_workload.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join("trainer-b")))
        .with_bootstrap_peer(validator_addr.clone())
        .spawn()?;

    wait_for(
        Duration::from_secs(20),
        || {
            trainer_a1.telemetry().snapshot().connected_peers >= 1
                && trainer_a2.telemetry().snapshot().connected_peers >= 1
                && trainer_b.telemetry().snapshot().connected_peers >= 1
                && viewer.telemetry().snapshot().connected_peers >= 1
        },
        "fleet did not converge on the helper/validator network",
    )?;

    let helper = helper;
    let mut validator = validator;
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

    let mut baseline_outcomes = Vec::new();
    let mut low_lr_outcomes = Vec::new();
    let mut merge_certificates = Vec::<MergeCertificate>::new();
    let restarted_trainer_label = "trainer-a2".to_string();
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

    for _ in 0..args.baseline_rounds {
        baseline_outcomes.push(run_training_round(
            &mut trainer_a1,
            "trainer-a1",
            &baseline,
            &prepared_data,
        )?);
        baseline_outcomes.push(run_training_round(
            &mut trainer_a2,
            "trainer-a2",
            &baseline,
            &prepared_data,
        )?);
        if let Some(validation) = validator.validate_candidates_once(&baseline)? {
            baseline_head = validation.merged_head.clone();
            merge_certificates.push(validation.merge_certificate.clone());
            head_eval_reports.push(head_eval_report_from_validation(
                &validation,
                &prepared_data,
                supported_workload.workload_id.clone(),
            ));
        }
    }

    shutdown_node(trainer_a2)?;
    let mut trainer_a2 = build_trainer_project(1.0e-3)?
        .trainer(release_manifest.clone(), supported_workload.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join("trainer-a2")))
        .with_bootstrap_peer(validator_addr.clone())
        .spawn()?;
    wait_for(
        Duration::from_secs(15),
        || trainer_a2.telemetry().snapshot().connected_peers >= 1,
        "restarted trainer-a2 did not reconnect",
    )?;
    let _ = wait_for_synced_head(
        &trainer_a2,
        &baseline,
        Duration::from_secs(20),
        "restarted trainer-a2 did not resync baseline head",
    )?;
    let trainer_restart_reconnected = true;
    baseline_outcomes.push(run_training_round(
        &mut trainer_a2,
        "trainer-a2",
        &baseline,
        &prepared_data,
    )?);
    if let Some(validation) = validator.validate_candidates_once(&baseline)? {
        baseline_head = validation.merged_head.clone();
        merge_certificates.push(validation.merge_certificate.clone());
        head_eval_reports.push(head_eval_report_from_validation(
            &validation,
            &prepared_data,
            supported_workload.workload_id.clone(),
        ));
    }
    let trainer_restart_resumed_training = true;

    let late_joiner = build_trainer_project(1.0e-3)?
        .trainer(release_manifest.clone(), supported_workload.clone())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(storage_root.join("trainer-late")))
        .with_bootstrap_peer(validator_addr.clone())
        .spawn()?;
    wait_for(
        Duration::from_secs(15),
        || late_joiner.telemetry().snapshot().connected_peers >= 1,
        "late joiner did not connect",
    )?;
    let synced_head = wait_for_synced_head(
        &late_joiner,
        &baseline,
        Duration::from_secs(20),
        "late joiner did not discover baseline head",
    )?;
    let late_joiner_store = late_joiner
        .artifact_store()
        .context("late joiner missing artifact store")?;
    let late_joiner_synced_checkpoint = late_joiner_store.has_manifest(&synced_head.artifact_id);

    for _ in 0..args.low_lr_rounds {
        low_lr_outcomes.push(run_training_round(
            &mut trainer_b,
            "trainer-b",
            &low_lr,
            &prepared_data,
        )?);
        if let Some(validation) = validator.validate_candidates_once(&low_lr)? {
            low_lr_head = validation.merged_head.clone();
            merge_certificates.push(validation.merge_certificate.clone());
            head_eval_reports.push(head_eval_report_from_validation(
                &validation,
                &prepared_data,
                supported_workload.workload_id.clone(),
            ));
        }
    }

    let _ = wait_for_synced_head(
        &viewer,
        &baseline,
        Duration::from_secs(20),
        "viewer did not discover baseline head",
    )?;
    let _ = wait_for_synced_head(
        &viewer,
        &low_lr,
        Duration::from_secs(20),
        "viewer did not discover low-lr head",
    )?;

    let peer_window_metrics = baseline_outcomes
        .iter()
        .chain(low_lr_outcomes.iter())
        .map(|outcome| {
            peer_window_metrics_from_training(outcome, &prepared_data, &network_manifest)
        })
        .collect::<Vec<_>>();
    let metrics_root = output.join("metrics-store");
    let mut metrics_store = MetricsStore::open(&metrics_root, MetricsIndexerConfig::default())?;
    metrics_store.sync_from_records(&peer_window_metrics, &[], &head_eval_reports)?;
    let metrics_catchup = metrics_store.load_catchup_bundles()?;
    let node_records = vec![
        node_record("helper", &helper, &storage_root.join("helper")),
        node_record("validator", &validator, &storage_root.join("validator")),
        node_record("viewer", &viewer, &storage_root.join("viewer")),
        node_record("trainer-a1", &trainer_a1, &storage_root.join("trainer-a1")),
        node_record("trainer-a2", &trainer_a2, &storage_root.join("trainer-a2")),
        node_record("trainer-b", &trainer_b, &storage_root.join("trainer-b")),
        node_record(
            "trainer-late",
            &late_joiner,
            &storage_root.join("trainer-late"),
        ),
    ];

    shutdown_node(late_joiner)?;
    shutdown_node(trainer_b)?;
    shutdown_node(trainer_a2)?;
    shutdown_node(trainer_a1)?;
    shutdown_node(viewer)?;
    shutdown_node(validator)?;
    shutdown_node(helper)?;

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
        node_records,
        restarted_trainer_label,
        trainer_restart_reconnected,
        trainer_restart_resumed_training,
        late_joiner_synced_checkpoint,
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
    _prepared_data: &PreparedMnistData,
) -> anyhow::Result<TrainingRecord>
where
    P: P2pWorkload<WindowStats = BTreeMap<String, MetricValue>>,
{
    Ok(TrainingRecord {
        label,
        training: trainer.train_window_once(experiment)?,
    })
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
        burn_version_string: "0.21.0-pre.2".into(),
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
                PeerRole::PortalViewer,
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
    nodes: [(&'static str, PeerId); 7],
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
                artifact_serving_score: if *label == "validator" || *label == "trainer-late" {
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
    validation: &burn_p2p::ValidationOutcome,
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
        baseline_accuracy_gain: baseline_accuracy - baseline_initial_accuracy,
        low_lr_accuracy_gain: low_lr_accuracy - low_lr_initial_accuracy,
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
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    while Instant::now() < deadline {
        match node.sync_experiment_head(experiment) {
            Ok(Some(head)) => return Ok(head),
            Ok(None) => {}
            Err(error) => last_error = Some(error.to_string()),
        }
        thread::sleep(Duration::from_millis(50));
    }
    if let Some(error) = last_error {
        anyhow::bail!("{failure_message}: {error}");
    }
    anyhow::bail!(failure_message.to_owned())
}

fn shutdown_node<P>(node: burn_p2p::RunningNode<P>) -> anyhow::Result<()> {
    node.shutdown()?;
    let _ = node.await_termination()?;
    Ok(())
}
