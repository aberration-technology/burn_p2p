use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, bail};
use burn_p2p::{
    AuthConfig, ClientPlatform, ClientReleaseManifest, ContentId, DiLoCoPolicy,
    ExperimentOptInPolicy, ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility,
    GenesisSpec, MainnetHandle, MicroShardPlannerConfig, NetworkManifest, OuterOptimizerPolicy,
    PeerRole, PeerRoleSet, ProjectFamilyId, RevisionId, RoleSet, ShardFetchManifest,
    SingleWorkloadProjectFamily, StorageConfig, SupportedWorkload, TrainingProtocol, WorkloadId,
};
use burn_p2p_python::{
    PythonCheckpointCommandConfig, PythonTorchDatasetConfig, PythonTorchProject,
    PythonTorchRuntimeConfig, PythonTorchWorkloadConfig, PythonStateDictFilterConfig,
};
use chrono::Utc;
use semver::Version;
use serde_json::json;

const SAMPLE_COUNT: u64 = 512;
const SHARD_COUNT: u32 = 8;
const DEFAULT_ROOT: &str = ".burn_p2p-python-mnist";
const DEFAULT_PYTHON: &str = "python3";
const DEFAULT_DILOCO_PEER_COUNT: usize = 2;

#[derive(Debug)]
struct DemoArgs {
    root: PathBuf,
    python_executable: PathBuf,
    protocol: DemoProtocol,
    peer_count: usize,
    state_dict_includes: Vec<String>,
    state_dict_excludes: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DemoProtocol {
    ArtifactWindows,
    DiLoCo,
}

impl DemoArgs {
    fn parse() -> anyhow::Result<Self> {
        let mut root = PathBuf::from(DEFAULT_ROOT);
        let mut python_executable = PathBuf::from(DEFAULT_PYTHON);
        let mut protocol = DemoProtocol::ArtifactWindows;
        let mut peer_count = DEFAULT_DILOCO_PEER_COUNT;
        let mut state_dict_includes = Vec::new();
        let mut state_dict_excludes = Vec::new();
        let mut args = env::args_os().skip(1);

        while let Some(arg) = args.next() {
            match arg.to_str() {
                Some("--help") | Some("-h") => {
                    print_help();
                    std::process::exit(0);
                }
                Some("--root") => {
                    root = args.next().context("--root requires a path")?.into();
                }
                Some("--python") => {
                    python_executable = args.next().context("--python requires a path")?.into();
                }
                Some("--protocol") => {
                    protocol = parse_protocol(
                        args.next()
                            .context("--protocol requires artifact-windows or diloco")?
                            .to_str()
                            .context("--protocol must be valid UTF-8")?,
                    )?;
                }
                Some("--peer-count") => {
                    peer_count = parse_peer_count(
                        args.next()
                            .context("--peer-count requires a positive integer")?
                            .to_str()
                            .context("--peer-count must be valid UTF-8")?,
                    )?;
                }
                Some("--state-dict-include") => {
                    state_dict_includes.push(parse_glob_arg(
                        args.next()
                            .context("--state-dict-include requires a glob")?
                            .to_str()
                            .context("--state-dict-include must be valid UTF-8")?,
                    )?);
                }
                Some("--state-dict-exclude") => {
                    state_dict_excludes.push(parse_glob_arg(
                        args.next()
                            .context("--state-dict-exclude requires a glob")?
                            .to_str()
                            .context("--state-dict-exclude must be valid UTF-8")?,
                    )?);
                }
                Some(value) if value.starts_with("--root=") => {
                    root = PathBuf::from(&value["--root=".len()..]);
                }
                Some(value) if value.starts_with("--python=") => {
                    python_executable = PathBuf::from(&value["--python=".len()..]);
                }
                Some(value) if value.starts_with("--protocol=") => {
                    protocol = parse_protocol(&value["--protocol=".len()..])?;
                }
                Some(value) if value.starts_with("--peer-count=") => {
                    peer_count = parse_peer_count(&value["--peer-count=".len()..])?;
                }
                Some(value) if value.starts_with("--state-dict-include=") => {
                    state_dict_includes.push(parse_glob_arg(
                        &value["--state-dict-include=".len()..],
                    )?);
                }
                Some(value) if value.starts_with("--state-dict-exclude=") => {
                    state_dict_excludes.push(parse_glob_arg(
                        &value["--state-dict-exclude=".len()..],
                    )?);
                }
                _ => bail!("unknown argument {arg:?}; use --help for usage"),
            }
        }

        Ok(Self {
            root,
            python_executable,
            protocol,
            peer_count,
            state_dict_includes,
            state_dict_excludes,
        })
    }
}

fn parse_protocol(value: &str) -> anyhow::Result<DemoProtocol> {
    match value {
        "artifact-windows" => Ok(DemoProtocol::ArtifactWindows),
        "diloco" => Ok(DemoProtocol::DiLoCo),
        other => bail!("unsupported protocol {other:?}; expected artifact-windows or diloco"),
    }
}

fn parse_peer_count(value: &str) -> anyhow::Result<usize> {
    let peer_count = value
        .parse::<usize>()
        .with_context(|| format!("invalid --peer-count value {value:?}"))?;
    if peer_count == 0 {
        bail!("--peer-count must be at least 1");
    }
    Ok(peer_count)
}

fn parse_glob_arg(value: &str) -> anyhow::Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("state_dict glob must not be empty");
    }
    Ok(value.to_owned())
}

fn print_help() {
    println!(
        "Python/Torch MNIST burn_p2p demo\n\n\
         Usage: torch_mnist_p2p_demo [--root PATH] [--python PATH] [--protocol artifact-windows|diloco] [--peer-count N] [--state-dict-include GLOB] [--state-dict-exclude GLOB]\n\n\
         Options:\n  \
           --root PATH    Output/cache root [default: {DEFAULT_ROOT}]\n  \
           --python PATH  Python executable for dataset prep and workers [default: {DEFAULT_PYTHON}]\n  \
           --protocol     Training protocol to run [default: artifact-windows]\n  \
           --peer-count   DiLoCo trainer participants [default: {DEFAULT_DILOCO_PEER_COUNT}]\n  \
           --state-dict-include GLOB  Include trainable state_dict keys for DiLoCo parameter packs\n  \
           --state-dict-exclude GLOB  Exclude frozen state_dict keys; may be repeated"
    );
}

fn main() -> anyhow::Result<()> {
    let args = DemoArgs::parse()?;
    let root = args.root;
    let dataset_root = root.join("dataset");
    let mnist_root = root.join("mnist");
    let python_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python");

    prepare_demo_dataset(
        &dataset_root,
        &mnist_root,
        &python_root,
        &args.python_executable,
    )?;

    let dataset_config = build_dataset_config(&dataset_root)?;
    write_fetch_manifest(&dataset_config)?;

    let project_config = build_project_config(
        dataset_config.clone(),
        &python_root,
        &mnist_root,
        &args.python_executable,
        args.protocol,
        &args.state_dict_includes,
        &args.state_dict_excludes,
    )?;
    match args.protocol {
        DemoProtocol::ArtifactWindows => {
            run_artifact_windows_demo(root, dataset_config, project_config)
        }
        DemoProtocol::DiLoCo => {
            run_diloco_demo(root, dataset_config, project_config, args.peer_count)
        }
    }
}

fn run_artifact_windows_demo(
    root: PathBuf,
    dataset_config: PythonTorchDatasetConfig,
    project_config: PythonTorchWorkloadConfig,
) -> anyhow::Result<()> {
    let validator_storage = root.join("validator");
    let trainer_storage = root.join("trainer");
    let _ = fs::remove_dir_all(&validator_storage);
    let _ = fs::remove_dir_all(&trainer_storage);

    let validator_project = PythonTorchProject::new_with_data_pipeline(
        project_config.clone(),
        build_micro_epoch_pipeline(&dataset_config),
    )?;
    println!(
        "python capability device={} preferred_backends={:?} work_units_per_second={:.2}",
        validator_project.runtime_device_name(),
        validator_project.probe_capability().preferred_backends,
        validator_project.probe_capability().work_units_per_second
    );
    let trainer_project = PythonTorchProject::new_with_data_pipeline(
        project_config,
        build_micro_epoch_pipeline(&dataset_config),
    )?;

    let genesis = GenesisSpec {
        network_id: burn_p2p::NetworkId::new("torch-mnist-python-demo"),
        protocol_version: Version::new(0, 1, 0),
        display_name: "Torch MNIST Python Demo".into(),
        created_at: Utc::now(),
        metadata: BTreeMap::from([("runtime".into(), "python-torch".into())]),
    };
    let network_manifest = NetworkManifest {
        network_id: genesis.network_id.clone(),
        project_family_id: ProjectFamilyId::new("torch-mnist-python-family"),
        protocol_major: 0,
        minimum_client_version: Version::new(0, 0, 0),
        required_release_train_hash: ContentId::new("torch-mnist-python-train"),
        allowed_target_artifact_hashes: BTreeSet::from([ContentId::new(
            "torch-mnist-python-artifact",
        )]),
        authority_public_keys: vec!["torch-mnist-validator".into()],
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: ContentId::new("auth-policy"),
        created_at: genesis.created_at,
        description: genesis.display_name.clone(),
    };
    let experiment = MainnetHandle {
        genesis: genesis.clone(),
        roles: RoleSet::default_trainer(),
    }
    .experiment(
        burn_p2p::StudyId::new("torch-mnist-study"),
        burn_p2p::ExperimentId::new("torch-mnist-exp"),
        RevisionId::new("rev-1"),
    );

    let validator_family = SingleWorkloadProjectFamily::new(release_manifest(), validator_project)?;
    let validator = burn_p2p::NodeBuilder::new(validator_family)
        .with_network(network_manifest.clone())?
        .with_roles(PeerRoleSet::new([
            PeerRole::Authority,
            PeerRole::Validator,
            PeerRole::Archive,
        ]))
        .with_storage(StorageConfig::new(&validator_storage))
        .spawn()?;
    wait_for(
        Duration::from_secs(10),
        || {
            let snapshot = validator.telemetry().snapshot();
            snapshot.status == burn_p2p::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "validator runtime did not start",
    )?;
    let validator_addr = validator.telemetry().snapshot().listen_addresses[0].clone();

    let mut validator = validator;
    let genesis_head = validator.initialize_local_head(&experiment)?;
    println!("validator published genesis head {}", genesis_head.head_id);

    let trainer_family = SingleWorkloadProjectFamily::new(release_manifest(), trainer_project)?;
    let trainer = burn_p2p::NodeBuilder::new(trainer_family)
        .with_network(network_manifest)?
        .with_roles(RoleSet::default_trainer())
        .with_storage(StorageConfig::new(&trainer_storage))
        .with_bootstrap_peer(validator_addr)
        .spawn()?;
    wait_for(
        Duration::from_secs(10),
        || trainer.telemetry().snapshot().connected_peers >= 1,
        "trainer did not connect to validator",
    )?;

    let mut trainer = trainer;
    let mut trainer_session = trainer.continuous_trainer(&experiment)?;
    let first_window = trainer_session.train_next_window()?;
    println!(
        "trainer published candidate {} loss={:?}",
        first_window.head.head_id,
        first_window.report.stats.get("loss")
    );
    let first_validation =
        wait_for_validation(&mut validator, &experiment, Duration::from_secs(10))?;
    println!(
        "validator certified merged head {} from {}",
        first_validation.merged_head.head_id, first_validation.source_peer_id
    );
    let first_canonical = wait_for_canonical_step(
        &mut trainer_session,
        first_validation.merged_head.global_step,
        Duration::from_secs(10),
    )?;
    println!(
        "trainer observed canonical head {}",
        first_canonical.head_id
    );

    let second_window = trainer_session.train_next_window()?;
    println!(
        "trainer published second candidate {} accuracy={:?}",
        second_window.head.head_id,
        second_window.report.stats.get("train_accuracy")
    );
    let second_validation =
        wait_for_validation(&mut validator, &experiment, Duration::from_secs(10))?;
    println!(
        "validator certified second merged head {} from {}",
        second_validation.merged_head.head_id, second_validation.source_peer_id
    );

    trainer.shutdown()?;
    let _ = trainer.await_termination()?;
    validator.shutdown()?;
    let _ = validator.await_termination()?;
    Ok(())
}

fn run_diloco_demo(
    root: PathBuf,
    dataset_config: PythonTorchDatasetConfig,
    project_config: PythonTorchWorkloadConfig,
    peer_count: usize,
) -> anyhow::Result<()> {
    let peer_count_u16 =
        u16::try_from(peer_count).context("--peer-count exceeds supported DiLoCo group size")?;
    let seed_storage = root.join("diloco-seed");
    let _ = fs::remove_dir_all(&seed_storage);
    for peer_index in 1..peer_count {
        let _ = fs::remove_dir_all(root.join(format!("diloco-peer-{peer_index}")));
    }

    let policy = DiLoCoPolicy {
        num_inner_steps: 2,
        target_group_size: peer_count_u16,
        minimum_group_size: peer_count_u16,
        matchmaking_timeout_ms: 10_000,
        aggregation_timeout_ms: 15_000,
        checkpoint_interval_rounds: 1,
        outer_optimizer_policy: OuterOptimizerPolicy::Sgd {
            learning_rate_micros: 1_000_000,
            momentum_micros: None,
            nesterov: false,
            weight_decay_micros: None,
        },
        ..DiLoCoPolicy::default()
    };

    let seed_project = PythonTorchProject::new_with_data_pipeline(
        project_config.clone(),
        build_micro_epoch_pipeline(&dataset_config),
    )?;
    let sanity =
        seed_project.sanity_check_training_protocol(&TrainingProtocol::DiLoCo(policy.clone()))?;
    println!(
        "python capability device={} preferred_backends={:?} work_units_per_second={:.2}",
        seed_project.runtime_device_name(),
        seed_project.probe_capability().preferred_backends,
        seed_project.probe_capability().work_units_per_second
    );
    println!(
        "DiLoCo parameter pack selected {} tensors ({} parameters)",
        sanity.parameter_pack_plan.included_keys.len(),
        sanity.parameter_pack_plan.parameter_count
    );

    let genesis = GenesisSpec {
        network_id: burn_p2p::NetworkId::new("torch-mnist-python-demo-diloco"),
        protocol_version: Version::new(0, 1, 0),
        display_name: "Torch MNIST Python DiLoCo Demo".into(),
        created_at: Utc::now(),
        metadata: BTreeMap::from([("runtime".into(), "python-torch-diloco".into())]),
    };
    let network_manifest = NetworkManifest {
        network_id: genesis.network_id.clone(),
        project_family_id: ProjectFamilyId::new("torch-mnist-python-family"),
        protocol_major: 0,
        minimum_client_version: Version::new(0, 0, 0),
        required_release_train_hash: ContentId::new("torch-mnist-python-train"),
        allowed_target_artifact_hashes: BTreeSet::from([ContentId::new(
            "torch-mnist-python-artifact",
        )]),
        authority_public_keys: vec!["torch-mnist-diloco".into()],
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: ContentId::new("auth-policy"),
        created_at: genesis.created_at,
        description: genesis.display_name.clone(),
    };
    let experiment = MainnetHandle {
        genesis: genesis.clone(),
        roles: RoleSet::default_trainer(),
    }
    .experiment(
        burn_p2p::StudyId::new("torch-mnist-study"),
        burn_p2p::ExperimentId::new("torch-mnist-exp"),
        RevisionId::new("rev-diloco"),
    );
    let auth = AuthConfig::new().with_experiment_directory([diloco_directory_entry(
        &experiment,
        &policy,
    )]);

    let seed_family = SingleWorkloadProjectFamily::new(release_manifest(), seed_project)?;
    let mut seed = burn_p2p::NodeBuilder::new(seed_family)
        .with_network(network_manifest.clone())?
        .with_roles(RoleSet::default_trainer())
        .with_storage(StorageConfig::new(&seed_storage))
        .with_auth(auth.clone())
        .spawn()?;
    wait_for(
        Duration::from_secs(10),
        || {
            let snapshot = seed.telemetry().snapshot();
            snapshot.status == burn_p2p::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "DiLoCo seed runtime did not start",
    )?;
    let genesis_head = seed.initialize_local_head(&experiment)?;
    println!("DiLoCo seed published genesis head {}", genesis_head.head_id);
    let seed_addr = seed.telemetry().snapshot().listen_addresses[0].clone();

    let mut peers = Vec::new();
    for peer_index in 1..peer_count {
        let peer_project = PythonTorchProject::new_with_data_pipeline(
            project_config.clone(),
            build_micro_epoch_pipeline(&dataset_config),
        )?;
        let peer_family = SingleWorkloadProjectFamily::new(release_manifest(), peer_project)?;
        let peer = burn_p2p::NodeBuilder::new(peer_family)
            .with_network(network_manifest.clone())?
            .with_roles(RoleSet::default_trainer())
            .with_storage(StorageConfig::new(
                root.join(format!("diloco-peer-{peer_index}")),
            ))
            .with_auth(auth.clone())
            .with_bootstrap_peer(seed_addr.clone())
            .spawn()?;
        wait_for(
            Duration::from_secs(10),
            || peer.telemetry().snapshot().connected_peers >= 1,
            "DiLoCo peer did not connect to seed",
        )?;
        wait_for(
            Duration::from_secs(10),
            || peer.sync_experiment_head(&experiment).unwrap_or(None).is_some(),
            "DiLoCo peer did not sync genesis head",
        )?;
        peers.push(peer);
    }
    if peer_count > 1 {
        wait_for(
            Duration::from_secs(10),
            || seed.telemetry().snapshot().connected_peers >= peer_count - 1,
            "DiLoCo seed did not connect to every peer",
        )?;
    }

    let mut peer_threads = Vec::new();
    for mut peer in peers {
        let experiment_for_peer = experiment.clone();
        peer_threads.push(thread::spawn(move || {
            peer.diloco_round_once(&experiment_for_peer)
                .map(|outcome| (peer, outcome))
                .map_err(|error| format!("{error:#}"))
        }));
    }
    let seed_outcome = seed.diloco_round_once(&experiment)?;
    let mut peer_results = Vec::new();
    for peer_thread in peer_threads {
        peer_results.push(
            peer_thread
                .join()
                .map_err(|_| anyhow::anyhow!("DiLoCo peer thread panicked"))?
                .map_err(|error| anyhow::anyhow!(error))?,
        );
    }
    println!(
        "DiLoCo round {} group={} participants={} seed_contributions={} checkpoint={:?}",
        seed_outcome.completed_round.round_id,
        seed_outcome.group_id,
        peer_count,
        seed_outcome.contributions.len(),
        seed_outcome
            .published_checkpoint
            .as_ref()
            .map(|head| head.head_id.as_str().to_owned())
    );

    for (peer, peer_outcome) in peer_results {
        println!(
            "DiLoCo peer round {} contributions={}",
            peer_outcome.completed_round.round_id,
            peer_outcome.contributions.len()
        );
        peer.shutdown()?;
        let _ = peer.await_termination()?;
    }
    seed.shutdown()?;
    let _ = seed.await_termination()?;
    Ok(())
}

fn build_project_config(
    dataset: PythonTorchDatasetConfig,
    python_root: &Path,
    mnist_root: &Path,
    python_executable: &Path,
    protocol: DemoProtocol,
    state_dict_includes: &[String],
    state_dict_excludes: &[String],
) -> anyhow::Result<PythonTorchWorkloadConfig> {
    let workload = workload_manifest();
    let workload_config = json!({
        "mnist_root": mnist_root,
        "hidden_size": 64,
        "learning_rate": 0.05,
        "train_batch_size": 32,
        "eval_batch_size": 64,
        "eval_limit": 256,
        "preferred_device": "auto",
    });
    let runtime = PythonTorchRuntimeConfig::new(
        python_executable,
        "torch_mnist_p2p_demo.runtime:TorchMnistWorkload",
        workload_config.clone(),
    )
    .with_module_search_root(python_root);
    let mut config = PythonTorchWorkloadConfig::new(
        runtime,
        dataset,
        workload,
        ContentId::new("python-torch-mnist-schema-v1"),
    )?;
    if protocol == DemoProtocol::DiLoCo {
        let mut filter = PythonStateDictFilterConfig::all();
        for pattern in state_dict_includes {
            filter = filter.with_include_glob(pattern.clone());
        }
        for pattern in state_dict_excludes {
            filter = filter.with_exclude_glob(pattern.clone());
        }
        filter.validate()?;
        config = config.with_state_dict_filter(filter);

        let command = PythonCheckpointCommandConfig::new(python_executable)
            .with_arg("-m")
            .with_arg("torch_mnist_p2p_demo.runtime")
            .with_arg("diloco-command-job")
            .with_arg("--config-json")
            .with_arg(serde_json::to_string(&workload_config)?)
            .with_module_search_root(python_root);
        config = config.with_diloco_checkpoint_command(command);
    }
    Ok(config)
}

fn build_dataset_config(dataset_root: &Path) -> anyhow::Result<PythonTorchDatasetConfig> {
    let total_bytes = dataset_bytes(dataset_root)?;
    Ok(PythonTorchDatasetConfig {
        root: dataset_root.to_path_buf(),
        dataset_id: burn_p2p::DatasetId::new("torch-mnist"),
        dataset_view_id: burn_p2p::DatasetViewId::new("torch-mnist-view"),
        source_uri: dataset_root.display().to_string(),
        format: "python-torch-mnist-safetensors".into(),
        manifest_hash: ContentId::new("torch-mnist-shards-v1"),
        preprocessing_hash: ContentId::new("torch-mnist-preprocess-v1"),
        tokenizer_hash: None,
        sizing: burn_p2p::DatasetSizing {
            total_examples: SAMPLE_COUNT,
            total_tokens: SAMPLE_COUNT,
            total_bytes,
        },
        planner: MicroShardPlannerConfig {
            target_microshard_bytes: (total_bytes / SHARD_COUNT as u64).max(1),
            min_microshards: SHARD_COUNT,
            max_microshards: SHARD_COUNT,
        },
        microshards_per_batch: 1,
        metadata: BTreeMap::from([
            ("dataset".into(), "mnist".into()),
            ("runtime".into(), "python-torch".into()),
        ]),
    })
}

fn diloco_directory_entry(
    experiment: &burn_p2p::ExperimentHandle,
    policy: &DiLoCoPolicy,
) -> burn_p2p::ExperimentDirectoryEntry {
    burn_p2p::ExperimentDirectoryEntry {
        network_id: experiment.network_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        workload_id: WorkloadId::new("python-torch-mnist"),
        display_name: "Python Torch MNIST DiLoCo".into(),
        model_schema_hash: ContentId::new("python-torch-mnist-schema-v1"),
        dataset_view_id: burn_p2p::DatasetViewId::new("torch-mnist-view"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 1024,
            estimated_window_seconds: 5,
        },
        visibility: ExperimentVisibility::Public,
        opt_in_policy: ExperimentOptInPolicy::Open,
        current_revision_id: experiment.revision_id.clone(),
        current_head_id: None,
        allowed_roles: RoleSet::default_trainer(),
        allowed_scopes: BTreeSet::from([ExperimentScope::Train {
            experiment_id: experiment.experiment_id.clone(),
        }]),
        training_protocol: TrainingProtocol::DiLoCo(policy.clone()),
        metadata: BTreeMap::new(),
    }
}

fn write_fetch_manifest(dataset: &PythonTorchDatasetConfig) -> anyhow::Result<()> {
    let plan = dataset.plan()?;
    if plan.microshards.len() != SHARD_COUNT as usize {
        bail!(
            "expected {} microshards, planned {}",
            SHARD_COUNT,
            plan.microshards.len()
        );
    }
    let manifest =
        ShardFetchManifest::from_microshards(&plan.dataset_view, &plan.microshards, |ordinal| {
            fs::read(dataset.root.join(format!("{ordinal:05}.bin"))).unwrap_or_default()
        });
    fs::write(
        dataset.root.join("fetch-manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(())
}

fn build_micro_epoch_pipeline(
    dataset: &PythonTorchDatasetConfig,
) -> burn_p2p::LeaseDataPipeline<String, burn_p2p_python::PythonBatchRef> {
    let registration = dataset.registration();
    let plan = dataset.plan().expect("dataset plan");
    let pipeline_dataset = dataset.clone();
    PythonTorchProject::indexed_dataset_pipeline(
        "torch-mnist-indexed-micro-epochs",
        move || Ok(registration.clone()),
        move |_registration| Ok(plan.clone()),
        move |_lease, cached_microshards| {
            Ok(json!({
                "dataset_root": pipeline_dataset.root,
                "shard_paths": cached_microshards.iter().map(|entry| entry.path.clone()).collect::<Vec<_>>(),
                "microshards_per_batch": pipeline_dataset.microshards_per_batch,
            }))
        },
    )
}

fn prepare_demo_dataset(
    dataset_root: &Path,
    mnist_root: &Path,
    python_root: &Path,
    python_executable: &Path,
) -> anyhow::Result<()> {
    if dataset_root.exists() {
        fs::remove_dir_all(dataset_root)?;
    }
    fs::create_dir_all(dataset_root)?;
    fs::create_dir_all(mnist_root)?;

    let pythonpath = joined_pythonpath([python_root.to_path_buf()])?;
    let status = Command::new(python_executable)
        .arg("-m")
        .arg("torch_mnist_p2p_demo.runtime")
        .arg("prepare-dataset")
        .arg("--dataset-root")
        .arg(dataset_root)
        .arg("--mnist-root")
        .arg(mnist_root)
        .arg("--sample-count")
        .arg(SAMPLE_COUNT.to_string())
        .arg("--shard-count")
        .arg(SHARD_COUNT.to_string())
        .env("PYTHONPATH", pythonpath)
        .status()
        .context("run python MNIST shard preparation")?;
    if !status.success() {
        bail!("python MNIST shard preparation failed with {status}");
    }
    Ok(())
}

fn dataset_bytes(dataset_root: &Path) -> anyhow::Result<u64> {
    let mut total = 0_u64;
    for ordinal in 0..SHARD_COUNT {
        let path = dataset_root.join(format!("{ordinal:05}.bin"));
        total += fs::metadata(&path)
            .with_context(|| format!("stat dataset shard {}", path.display()))?
            .len();
    }
    Ok(total)
}

fn joined_pythonpath(entries: impl IntoIterator<Item = PathBuf>) -> anyhow::Result<OsString> {
    let mut all_entries = entries.into_iter().collect::<Vec<_>>();
    if let Some(existing) = std::env::var_os("PYTHONPATH") {
        all_entries.extend(std::env::split_paths(&existing));
    }
    std::env::join_paths(all_entries).context("join python path for example worker")
}

fn workload_manifest() -> SupportedWorkload {
    SupportedWorkload {
        workload_id: WorkloadId::new("python-torch-mnist"),
        workload_name: "Python Torch MNIST".into(),
        model_program_hash: ContentId::new("python-torch-mnist-program-v1"),
        checkpoint_format_hash: ContentId::new("python-torch-safetensors-v1"),
        supported_revision_family: ContentId::new("python-torch-mnist-revision-family"),
        resource_class: "python-torch-cpu".into(),
    }
}

fn release_manifest() -> ClientReleaseManifest {
    ClientReleaseManifest {
        project_family_id: ProjectFamilyId::new("torch-mnist-python-family"),
        release_train_hash: ContentId::new("torch-mnist-python-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("torch-mnist-python-artifact"),
        target_platform: ClientPlatform::Native,
        app_semver: Version::new(0, 1, 0),
        git_commit: "local-demo".into(),
        cargo_lock_hash: ContentId::new("cargo-lock"),
        burn_version_string: "python-torch".into(),
        enabled_features_hash: ContentId::new("python-runtime"),
        protocol_major: 0,
        supported_workloads: vec![workload_manifest()],
        built_at: Utc::now(),
    }
}

fn wait_for(
    timeout: Duration,
    mut condition: impl FnMut() -> bool,
    failure_message: &str,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(25));
    }
    bail!(failure_message.to_owned())
}

fn wait_for_validation<P>(
    validator: &mut burn_p2p::RunningNode<P>,
    experiment: &burn_p2p::ExperimentHandle,
    timeout: Duration,
) -> anyhow::Result<burn_p2p::ValidationOutcome>
where
    P: burn_p2p::P2pWorkload,
    P::Model: Send + 'static,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(outcome) = validator.validate_candidates_once(experiment)? {
            return Ok(outcome);
        }
        thread::sleep(Duration::from_millis(100));
    }
    bail!("validator did not certify a visible python trainer update before timeout")
}

fn wait_for_canonical_step<'a, P>(
    trainer: &mut burn_p2p::ContinuousTrainer<'a, P>,
    expected_global_step: u64,
    timeout: Duration,
) -> anyhow::Result<burn_p2p::HeadDescriptor>
where
    P: burn_p2p::P2pWorkload,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let _ = trainer.sync_canonical_head()?;
        if let Some(head) = trainer.canonical_head()
            && head.global_step >= expected_global_step
        {
            return Ok(head.clone());
        }
        thread::sleep(Duration::from_millis(100));
    }
    bail!(
        "trainer did not observe canonical head step {} before timeout",
        expected_global_step
    )
}
