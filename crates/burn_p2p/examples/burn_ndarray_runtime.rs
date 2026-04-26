//! burn-native learner integration for the burn_p2p runtime.
use std::{
    collections::{BTreeMap, BTreeSet},
    thread,
    time::{Duration, Instant},
};

use burn::{
    backend::{Autodiff, NdArray},
    data::{
        dataloader::{DataLoaderBuilder, batcher::Batcher},
        dataset::InMemDataset,
    },
    module::Module,
    nn::{Linear, LinearConfig},
    optim::SgdConfig,
    tensor::{
        ElementConversion, Tensor, TensorData,
        backend::{AutodiffBackend, Backend},
    },
    train::{InferenceStep, ItemLazy, Learner, TrainOutput, TrainStep},
};
use burn_p2p::burn::{from_loaders, inspect_module};
use burn_p2p::{
    ClientReleaseManifest, ContentId, GenesisSpec, MetricValue, NetworkManifest, RoleSet,
    StorageConfig, SupportedWorkload, WorkloadId,
};
use chrono::Utc;
use semver::Version;

type RuntimeBackend = Autodiff<NdArray<f32>>;

#[derive(Module, Debug)]
struct TinyModel<B: Backend> {
    linear: Linear<B>,
}

impl<B: Backend> TinyModel<B> {
    fn new(device: &B::Device) -> Self {
        Self {
            linear: LinearConfig::new(4, 2).init(device),
        }
    }

    fn forward(&self, inputs: Tensor<B, 2>) -> Tensor<B, 2> {
        self.linear.forward(inputs)
    }
}

#[derive(Clone, Debug)]
struct TinyBatch<B: Backend> {
    inputs: Tensor<B, 2>,
    targets: Tensor<B, 2>,
}

#[derive(Clone, Debug)]
struct TinyItem {
    value: f32,
}

#[derive(Clone, Debug, Default)]
struct TinyBatcher;

impl<B: Backend> Batcher<B, TinyItem, TinyBatch<B>> for TinyBatcher {
    fn batch(&self, items: Vec<TinyItem>, device: &B::Device) -> TinyBatch<B> {
        let mut inputs = Vec::with_capacity(items.len() * 4);
        let mut targets = Vec::with_capacity(items.len() * 2);
        for item in items {
            inputs.extend([item.value, item.value + 1.0, item.value * 0.5, 1.0]);
            targets.extend([item.value * 0.3, item.value * 0.7]);
        }
        let batch_len = targets.len() / 2;
        TinyBatch {
            inputs: Tensor::from_data(TensorData::new(inputs, [batch_len, 4]), device),
            targets: Tensor::from_data(TensorData::new(targets, [batch_len, 2]), device),
        }
    }
}

#[derive(Clone, Debug)]
struct TinyMetricItem {
    loss: f64,
}

impl ItemLazy for TinyMetricItem {
    type ItemSync = Self;

    fn sync(self) -> Self::ItemSync {
        self
    }
}

impl<B: Backend> InferenceStep for TinyModel<B> {
    type Input = TinyBatch<B>;
    type Output = TinyMetricItem;

    fn step(&self, batch: Self::Input) -> Self::Output {
        let predictions = self.forward(batch.inputs);
        let loss = (predictions - batch.targets).powf_scalar(2.0).mean();
        TinyMetricItem {
            loss: loss.into_scalar().elem::<f64>(),
        }
    }
}

impl<B: AutodiffBackend> TrainStep for TinyModel<B> {
    type Input = TinyBatch<B>;
    type Output = TinyMetricItem;

    fn step(&self, batch: Self::Input) -> TrainOutput<Self::Output> {
        let predictions = self.forward(batch.inputs);
        let loss = (predictions - batch.targets).powf_scalar(2.0).mean();
        let loss_value = loss.clone().into_scalar().elem::<f64>();
        let grads = loss.backward();

        TrainOutput::new(self, grads, TinyMetricItem { loss: loss_value })
    }
}

fn tiny_workload_manifest() -> SupportedWorkload {
    SupportedWorkload {
        workload_id: WorkloadId::new("burn-ndarray-linear"),
        workload_name: "Burn NdArray Linear".into(),
        model_program_hash: ContentId::new("burn-ndarray-program"),
        checkpoint_format_hash: ContentId::new("burn-ndarray-burnpack"),
        supported_revision_family: ContentId::new("burn-ndarray-revision-family"),
        resource_class: "cpu".into(),
    }
}

fn tiny_release_manifest() -> ClientReleaseManifest {
    ClientReleaseManifest {
        project_family_id: burn_p2p::ProjectFamilyId::new("burn-ndarray-family"),
        release_train_hash: ContentId::new("burn-ndarray-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("burn-ndarray-artifact-native"),
        target_platform: burn_p2p::ClientPlatform::Native,
        app_semver: Version::new(0, 2, 0),
        git_commit: "local-demo".into(),
        cargo_lock_hash: ContentId::new("cargo-lock"),
        burn_version_string: "0.21.0-pre.3".into(),
        enabled_features_hash: ContentId::new("burn-runtime"),
        protocol_major: 0,
        supported_workloads: vec![tiny_workload_manifest()],
        built_at: Utc::now(),
    }
}

fn main() -> anyhow::Result<()> {
    let genesis = GenesisSpec {
        network_id: burn_p2p::NetworkId::new("burn-ndarray-demo"),
        protocol_version: Version::new(0, 1, 0),
        display_name: "Burn NdArray Demo".into(),
        created_at: Utc::now(),
        metadata: BTreeMap::from([("purpose".into(), "burn learner example".into())]),
    };
    let network_manifest = NetworkManifest {
        network_id: genesis.network_id.clone(),
        project_family_id: burn_p2p::ProjectFamilyId::new("burn-ndarray-family"),
        protocol_major: 0,
        minimum_client_version: Version::new(0, 0, 0),
        required_release_train_hash: ContentId::new("burn-ndarray-train"),
        allowed_target_artifact_hashes: BTreeSet::from([ContentId::new(
            "burn-ndarray-artifact-native",
        )]),
        authority_public_keys: vec!["burn-validator".into()],
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: ContentId::new("auth-policy"),
        created_at: genesis.created_at,
        description: genesis.display_name.clone(),
    };
    let experiment = burn_p2p::MainnetHandle {
        genesis: genesis.clone(),
        roles: RoleSet::default_trainer(),
    }
    .experiment(
        burn_p2p::StudyId::new("burn-study"),
        burn_p2p::ExperimentId::new("burn-exp"),
        burn_p2p::RevisionId::new("rev-1"),
    );

    let make_workload = || -> anyhow::Result<_> {
        let device = <RuntimeBackend as Backend>::Device::default();
        let train_loader =
            DataLoaderBuilder::new(TinyBatcher)
                .batch_size(2)
                .build(InMemDataset::new(vec![
                    TinyItem { value: 4.0 },
                    TinyItem { value: 6.0 },
                ]));
        let eval_loader = DataLoaderBuilder::new(TinyBatcher)
            .batch_size(1)
            .build(InMemDataset::new(vec![TinyItem { value: 5.0 }]));

        Ok(from_loaders(
            Learner::new(
                TinyModel::<RuntimeBackend>::new(&device),
                SgdConfig::new().init(),
                0.05,
            ),
            device,
            train_loader,
            eval_loader,
        )
        .with_step_metrics(|_step_index, output, metrics| {
            metrics.insert("loss".into(), MetricValue::Float(output.loss));
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

    let validator = make_workload()?
        .validator(tiny_release_manifest(), tiny_workload_manifest())?
        .with_network(network_manifest.clone())?
        .with_storage(StorageConfig::new(".burn_p2p-burn-demo/validator"))
        .spawn()?;
    wait_for(
        Duration::from_secs(5),
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
    println!(
        "validator published burn genesis head {}",
        genesis_head.head_id.as_str()
    );

    let trainer = make_workload()?
        .trainer(tiny_release_manifest(), tiny_workload_manifest())?
        .with_network(network_manifest)?
        .with_storage(StorageConfig::new(".burn_p2p-burn-demo/trainer"))
        .with_bootstrap_peer(validator_addr)
        .spawn()?;
    wait_for(
        Duration::from_secs(5),
        || trainer.telemetry().snapshot().connected_peers >= 1,
        "trainer did not connect to validator",
    )?;

    let mut trainer = trainer;
    let training = trainer.train_window_once(&experiment)?;
    println!(
        "trainer published burn candidate head {} with metrics {:?}",
        training.head.head_id.as_str(),
        training.report.stats
    );

    let validated = validator
        .validate_candidates_once(&experiment)?
        .expect("validator should certify the trainer update");
    println!(
        "validator certified burn head {} from {}",
        validated.merged_head.head_id.as_str(),
        validated.source_peer_id.as_str()
    );

    trainer.shutdown()?;
    let _ = trainer.await_termination()?;
    validator.shutdown()?;
    let _ = validator.await_termination()?;
    Ok(())
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
