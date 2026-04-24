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
    ClientPlatform, ClientReleaseManifest, ContentId, GenesisSpec, MainnetHandle,
    MicroShardPlannerConfig, NetworkManifest, PeerRole, PeerRoleSet, ProjectFamilyId, RevisionId,
    RoleSet, ShardFetchManifest, SingleWorkloadProjectFamily, StorageConfig, SupportedWorkload,
    WorkloadId,
};
use burn_p2p_python::{
    PythonTorchDatasetConfig, PythonTorchProject, PythonTorchRuntimeConfig,
    PythonTorchWorkloadConfig,
};
use chrono::Utc;
use semver::Version;
use serde_json::json;

const SAMPLE_COUNT: u64 = 512;
const SHARD_COUNT: u32 = 8;
const DEFAULT_ROOT: &str = ".burn_p2p-python-mnist";
const DEFAULT_PYTHON: &str = "python3";

#[derive(Debug)]
struct DemoArgs {
    root: PathBuf,
    python_executable: PathBuf,
}

impl DemoArgs {
    fn parse() -> anyhow::Result<Self> {
        let mut root = PathBuf::from(DEFAULT_ROOT);
        let mut python_executable = PathBuf::from(DEFAULT_PYTHON);
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
                Some(value) if value.starts_with("--root=") => {
                    root = PathBuf::from(&value["--root=".len()..]);
                }
                Some(value) if value.starts_with("--python=") => {
                    python_executable = PathBuf::from(&value["--python=".len()..]);
                }
                _ => bail!("unknown argument {arg:?}; use --help for usage"),
            }
        }

        Ok(Self {
            root,
            python_executable,
        })
    }
}

fn print_help() {
    println!(
        "Python/Torch MNIST burn_p2p demo\n\n\
         Usage: torch_mnist_p2p_demo [--root PATH] [--python PATH]\n\n\
         Options:\n  \
           --root PATH    Output/cache root [default: {DEFAULT_ROOT}]\n  \
           --python PATH  Python executable for dataset prep and workers [default: {DEFAULT_PYTHON}]"
    );
}

fn main() -> anyhow::Result<()> {
    let args = DemoArgs::parse()?;
    let root = args.root;
    let dataset_root = root.join("dataset");
    let mnist_root = root.join("mnist");
    let validator_storage = root.join("validator");
    let trainer_storage = root.join("trainer");
    let python_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python");

    let _ = fs::remove_dir_all(&validator_storage);
    let _ = fs::remove_dir_all(&trainer_storage);
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
    )?;
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

fn build_project_config(
    dataset: PythonTorchDatasetConfig,
    python_root: &Path,
    mnist_root: &Path,
    python_executable: &Path,
) -> anyhow::Result<PythonTorchWorkloadConfig> {
    let workload = workload_manifest();
    let runtime = PythonTorchRuntimeConfig::new(
        python_executable,
        "torch_mnist_p2p_demo.runtime:TorchMnistWorkload",
        json!({
            "mnist_root": mnist_root,
            "hidden_size": 64,
            "learning_rate": 0.05,
            "train_batch_size": 32,
            "eval_batch_size": 64,
            "eval_limit": 256,
            "preferred_device": "auto",
        }),
    )
    .with_module_search_root(python_root);
    PythonTorchWorkloadConfig::new(
        runtime,
        dataset,
        workload,
        ContentId::new("python-torch-mnist-schema-v1"),
    )
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
