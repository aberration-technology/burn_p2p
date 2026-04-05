use std::{
    fs,
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use super::advanced::BurnLearnerProjectBuilderAdvancedExt;
use super::*;
use burn::{
    backend::{Autodiff, NdArray},
    data::{
        dataloader::{DataLoaderBuilder, batcher::Batcher},
        dataset::InMemDataset,
    },
    module::Module,
    nn::{Linear, LinearConfig},
    optim::{SgdConfig, adaptor::OptimizerAdaptor},
    tensor::{
        ElementConversion, Tensor, TensorData,
        backend::{AutodiffBackend, Backend},
    },
    train::{InferenceStep, ItemLazy, TrainOutput, TrainStep},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tempfile::tempdir;

type TestBackend = NdArray<f32>;

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
}

#[derive(Clone, Debug)]
struct TinyBurnWorkload;

impl BurnWorkload for TinyBurnWorkload {
    type Backend = TestBackend;
    type Model = TinyModel<TestBackend>;
    type Batch = f32;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, device: &<Self::Backend as Backend>::Device) -> Self::Model {
        TinyModel::new(device)
    }

    fn benchmark(
        &self,
        _model: &Self::Model,
        _device: &<Self::Backend as Backend>::Device,
    ) -> crate::CapabilityEstimate {
        crate::CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 16.0,
            target_window_seconds: 1,
        }
    }

    fn train_window(
        &self,
        _ctx: &mut WindowCtx<<Self::Backend as Backend>::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        Ok(WindowReport {
            contribution: None,
            stats: BTreeMap::from([("loss".into(), MetricValue::Float(0.0))]),
            completed_at: Utc::now(),
        })
    }

    fn evaluate(&self, _model: &Self::Model, _split: EvalSplit) -> MetricReport {
        MetricReport {
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.0))]),
            captured_at: Utc::now(),
        }
    }

    fn runtime_device(&self) -> <Self::Backend as Backend>::Device {
        <TestBackend as Backend>::Device::default()
    }

    fn dataset_registration(&self) -> anyhow::Result<crate::DatasetRegistration> {
        Ok(crate::DatasetRegistration {
            manifest: crate::DatasetManifest {
                dataset_id: crate::DatasetId::new("tiny-dataset"),
                source_uri: ".".into(),
                format: "microshards".into(),
                manifest_hash: ContentId::new("tiny-manifest"),
                metadata: BTreeMap::new(),
            },
            view: crate::DatasetView {
                dataset_view_id: crate::DatasetViewId::new("tiny-view"),
                dataset_id: crate::DatasetId::new("tiny-dataset"),
                preprocessing_hash: ContentId::new("tiny-preprocess"),
                tokenizer_hash: None,
                manifest_hash: ContentId::new("tiny-manifest"),
                metadata: BTreeMap::new(),
            },
            upstream: crate::UpstreamAdapter::Local { root: ".".into() },
        })
    }

    fn microshard_plan(
        &self,
        registration: &crate::DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        Ok(
            crate::MicroShardPlanner::new(crate::MicroShardPlannerConfig {
                target_microshard_bytes: 16,
                min_microshards: 1,
                max_microshards: 1,
            })?
            .plan(
                &registration.view,
                crate::DatasetSizing {
                    total_examples: 1,
                    total_tokens: 1,
                    total_bytes: 16,
                },
            )?,
        )
    }

    fn load_batches(
        &self,
        _lease: &AssignmentLease,
        _cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        Ok(vec![1.0])
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }
}

type LearnerBackend = Autodiff<NdArray<f32>>;
type LearnerEvalBackend = NdArray<f32>;
type TinyLearnerOptimizer = OptimizerAdaptor<
    burn::optim::Sgd<<LearnerBackend as AutodiffBackend>::InnerBackend>,
    TinyLearnerModel<LearnerBackend>,
    LearnerBackend,
>;
type TinyLearnerComponents = LearningComponentsMarker<
    LearnerBackend,
    f64,
    TinyLearnerModel<LearnerBackend>,
    TinyLearnerOptimizer,
>;

#[derive(Module, Debug)]
struct TinyLearnerModel<B: Backend> {
    linear: Linear<B>,
}

impl<B: Backend> TinyLearnerModel<B> {
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
struct TinyLearnerBatch<B: Backend> {
    inputs: Tensor<B, 2>,
    targets: Tensor<B, 2>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct TinyLearnerItem {
    value: f32,
}

#[derive(Clone, Debug, Default)]
struct TinyLearnerBatcher;

impl<B: Backend> Batcher<B, TinyLearnerItem, TinyLearnerBatch<B>> for TinyLearnerBatcher {
    fn batch(&self, items: Vec<TinyLearnerItem>, device: &B::Device) -> TinyLearnerBatch<B> {
        let mut inputs = Vec::with_capacity(items.len() * 4);
        let mut targets = Vec::with_capacity(items.len() * 2);
        for item in items {
            inputs.extend([item.value, item.value + 1.0, item.value * 0.5, 1.0]);
            targets.extend([item.value * 0.3, item.value * 0.7]);
        }
        let batch_len = targets.len() / 2;
        TinyLearnerBatch {
            inputs: Tensor::from_data(TensorData::new(inputs, [batch_len, 4]), device),
            targets: Tensor::from_data(TensorData::new(targets, [batch_len, 2]), device),
        }
    }
}

#[derive(Clone, Debug)]
struct TinyLearnerMetric {
    loss: f64,
}

impl ItemLazy for TinyLearnerMetric {
    type ItemSync = Self;

    fn sync(self) -> Self::ItemSync {
        self
    }
}

impl<B: Backend> InferenceStep for TinyLearnerModel<B> {
    type Input = TinyLearnerBatch<B>;
    type Output = TinyLearnerMetric;

    fn step(&self, batch: Self::Input) -> Self::Output {
        let predictions = self.forward(batch.inputs);
        let loss = (predictions - batch.targets).powf_scalar(2.0).mean();
        TinyLearnerMetric {
            loss: loss.into_scalar().elem::<f64>(),
        }
    }
}

impl<B: AutodiffBackend> TrainStep for TinyLearnerModel<B> {
    type Input = TinyLearnerBatch<B>;
    type Output = TinyLearnerMetric;

    fn step(&self, batch: Self::Input) -> TrainOutput<Self::Output> {
        let predictions = self.forward(batch.inputs);
        let loss = (predictions - batch.targets).powf_scalar(2.0).mean();
        let loss_value = loss.clone().into_scalar().elem::<f64>();
        let grads = loss.backward();
        TrainOutput::new(self, grads, TinyLearnerMetric { loss: loss_value })
    }
}

#[derive(Clone, Debug)]
struct TinyLearnerWorkload;

impl TinyLearnerWorkload {
    fn batch<B: Backend>(device: &B::Device, value: f32) -> TinyLearnerBatch<B> {
        TinyLearnerBatch {
            inputs: Tensor::from_data([[value, value + 1.0, value * 0.5, 1.0]], device),
            targets: Tensor::from_data([[value * 0.3, value * 0.7]], device),
        }
    }
}

fn tiny_train_loader() -> BurnTrainLoader<TinyLearnerComponents> {
    DataLoaderBuilder::new(TinyLearnerBatcher)
        .batch_size(2)
        .build(InMemDataset::new(vec![
            TinyLearnerItem { value: 1.0 },
            TinyLearnerItem { value: 2.0 },
        ]))
}

fn tiny_eval_loader() -> BurnEvalLoader<TinyLearnerComponents> {
    DataLoaderBuilder::new(TinyLearnerBatcher)
        .batch_size(1)
        .build(InMemDataset::new(vec![TinyLearnerItem { value: 2.0 }]))
}

fn tiny_sharded_dataset(
    root: &Path,
    microshards: u32,
) -> anyhow::Result<BurnShardedDataset<TinyLearnerItem>> {
    let records = vec![
        TinyLearnerItem { value: 1.0 },
        TinyLearnerItem { value: 2.0 },
        TinyLearnerItem { value: 3.0 },
        TinyLearnerItem { value: 4.0 },
    ];
    BurnShardedDataset::write_local(
        root,
        &records,
        BurnShardedDatasetConfig::new("tiny-sharded-dataset")
            .with_microshards(microshards)
            .with_sizing(crate::DatasetSizing {
                total_examples: records.len() as u64,
                total_tokens: records.len() as u64,
                total_bytes: 64,
            }),
    )
}

fn tiny_assignment_lease() -> AssignmentLease {
    AssignmentLease {
        lease_id: crate::LeaseId::new("lease-1"),
        network_id: crate::NetworkId::new("net-1"),
        study_id: crate::StudyId::new("study-1"),
        experiment_id: crate::ExperimentId::new("exp-1"),
        revision_id: crate::RevisionId::new("rev-1"),
        peer_id: crate::PeerId::new("peer-1"),
        dataset_view_id: crate::DatasetViewId::new("view-1"),
        window_id: crate::WindowId(1),
        granted_at: Utc::now(),
        expires_at: Utc::now(),
        budget_work_units: 1,
        microshards: vec![crate::MicroShardId::new("shard-1")],
        assignment_hash: ContentId::new("assign-1"),
    }
}

impl BurnLearnerWorkload for TinyLearnerWorkload {
    type Backend = LearnerBackend;
    type Model = TinyLearnerModel<LearnerBackend>;
    type EvalModel = TinyLearnerModel<LearnerEvalBackend>;
    type Optimizer = TinyLearnerOptimizer;
    type Scheduler = f64;

    fn init_learner(
        &self,
        device: &<Self::Backend as Backend>::Device,
    ) -> BurnWorkloadLearner<Self> {
        BurnLearner::new(
            TinyLearnerModel::<LearnerBackend>::new(device),
            SgdConfig::new().init(),
            0.05,
        )
    }

    fn benchmark(
        &self,
        _model: &Self::Model,
        _device: &<Self::Backend as Backend>::Device,
    ) -> crate::CapabilityEstimate {
        crate::CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 8.0,
            target_window_seconds: 1,
        }
    }

    fn evaluate(&self, model: &Self::EvalModel, _split: EvalSplit) -> MetricReport {
        let device = <LearnerEvalBackend as Backend>::Device::default();
        let loss = model
            .step(Self::batch::<LearnerEvalBackend>(&device, 2.0))
            .loss;
        MetricReport {
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(loss))]),
            captured_at: Utc::now(),
        }
    }

    fn runtime_device(&self) -> <Self::Backend as Backend>::Device {
        <LearnerBackend as Backend>::Device::default()
    }

    fn dataset_registration(&self) -> anyhow::Result<crate::DatasetRegistration> {
        TinyBurnWorkload.dataset_registration()
    }

    fn microshard_plan(
        &self,
        registration: &crate::DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        TinyBurnWorkload.microshard_plan(registration)
    }

    fn load_batches(
        &self,
        _lease: &AssignmentLease,
        _cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<BurnTrainBatch<Self>>> {
        let device = BurnLearnerWorkload::runtime_device(self);
        Ok(vec![
            Self::batch::<LearnerBackend>(&device, 1.0),
            Self::batch::<LearnerBackend>(&device, 2.0),
        ])
    }

    fn after_train_step(
        &self,
        _step_index: usize,
        output: &BurnTrainOutput<Self>,
        metrics: &mut BTreeMap<String, MetricValue>,
    ) -> Result<(), TrainError> {
        metrics.insert("loss".into(), MetricValue::Float(output.loss));
        Ok(())
    }
}

#[test]
fn burn_workload_adapter_derives_schema_and_manifest() {
    let supported_workload = SupportedWorkload {
        workload_id: crate::WorkloadId::new("tiny-workload"),
        workload_name: "Tiny Burn Workload".into(),
        model_program_hash: ContentId::new("tiny-program"),
        checkpoint_format_hash: ContentId::new("tiny-burnpack"),
        supported_revision_family: ContentId::new("tiny-family"),
        resource_class: "cpu".into(),
    };
    let config = BurnWorkloadConfig::new(
        supported_workload.clone(),
        BurnArtifactConfig::burnpack(ChunkingScheme::new(64).expect("chunking")),
    )
    .with_root_ema(0.99);
    let adapter = TinyBurnWorkload
        .into_p2p_workload(config)
        .expect("adapter should build");

    assert_eq!(adapter.supported_workload(), supported_workload);
    assert_eq!(
        adapter.checkpoint_format_hash(),
        ContentId::new("tiny-burnpack")
    );
    assert!(!adapter.model_schema_hash().as_str().is_empty());
}

#[test]
fn trainer_builder_wraps_single_burn_workload_with_trainer_roles() {
    let supported_workload = SupportedWorkload {
        workload_id: crate::WorkloadId::new("tiny-workload"),
        workload_name: "Tiny Burn Workload".into(),
        model_program_hash: ContentId::new("tiny-program"),
        checkpoint_format_hash: ContentId::new("tiny-burnpack"),
        supported_revision_family: ContentId::new("tiny-family"),
        resource_class: "cpu".into(),
    };
    let release_manifest = ClientReleaseManifest {
        project_family_id: crate::ProjectFamilyId::new("tiny-family"),
        release_train_hash: ContentId::new("tiny-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("tiny-artifact"),
        target_platform: crate::ClientPlatform::Native,
        app_semver: semver::Version::new(0, 1, 0),
        git_commit: "local".into(),
        cargo_lock_hash: ContentId::new("tiny-lock"),
        burn_version_string: "0.21.0-pre.2".into(),
        enabled_features_hash: ContentId::new("tiny-features"),
        protocol_major: 0,
        supported_workloads: vec![supported_workload.clone()],
        built_at: Utc::now(),
    };
    let network_manifest = crate::NetworkManifest {
        network_id: crate::NetworkId::new("tiny-network"),
        project_family_id: release_manifest.project_family_id.clone(),
        protocol_major: release_manifest.protocol_major,
        required_release_train_hash: release_manifest.release_train_hash.clone(),
        allowed_target_artifact_hashes: std::collections::BTreeSet::from([release_manifest
            .target_artifact_hash
            .clone()]),
        authority_public_keys: Vec::new(),
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: ContentId::new("tiny-auth-policy"),
        created_at: Utc::now(),
        description: "tiny network".into(),
    };
    let builder = trainer(
        release_manifest,
        TinyBurnWorkload,
        BurnWorkloadConfig::new(
            supported_workload.clone(),
            BurnArtifactConfig::burnpack(ChunkingScheme::new(64).expect("chunking")),
        ),
    )
    .expect("trainer builder")
    .with_network(network_manifest)
    .expect("network binding");
    let prepared = builder.prepare().expect("prepared node");

    assert_eq!(prepared.mainnet().roles, crate::RoleSet::default_trainer());
    assert_eq!(
        prepared.config().selected_workload_id,
        Some(supported_workload.workload_id)
    );
}

#[test]
fn validator_builder_wraps_single_burn_workload_with_validator_roles() {
    let supported_workload = SupportedWorkload {
        workload_id: crate::WorkloadId::new("tiny-workload"),
        workload_name: "Tiny Burn Workload".into(),
        model_program_hash: ContentId::new("tiny-program"),
        checkpoint_format_hash: ContentId::new("tiny-burnpack"),
        supported_revision_family: ContentId::new("tiny-family"),
        resource_class: "cpu".into(),
    };
    let release_manifest = ClientReleaseManifest {
        project_family_id: crate::ProjectFamilyId::new("tiny-family"),
        release_train_hash: ContentId::new("tiny-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("tiny-artifact"),
        target_platform: crate::ClientPlatform::Native,
        app_semver: semver::Version::new(0, 1, 0),
        git_commit: "local".into(),
        cargo_lock_hash: ContentId::new("tiny-lock"),
        burn_version_string: "0.21.0-pre.2".into(),
        enabled_features_hash: ContentId::new("tiny-features"),
        protocol_major: 0,
        supported_workloads: vec![supported_workload.clone()],
        built_at: Utc::now(),
    };
    let network_manifest = crate::NetworkManifest {
        network_id: crate::NetworkId::new("tiny-network"),
        project_family_id: release_manifest.project_family_id.clone(),
        protocol_major: release_manifest.protocol_major,
        required_release_train_hash: release_manifest.release_train_hash.clone(),
        allowed_target_artifact_hashes: std::collections::BTreeSet::from([release_manifest
            .target_artifact_hash
            .clone()]),
        authority_public_keys: Vec::new(),
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: ContentId::new("tiny-auth-policy"),
        created_at: Utc::now(),
        description: "tiny network".into(),
    };
    let builder = validator(
        release_manifest,
        TinyBurnWorkload,
        BurnWorkloadConfig::new(
            supported_workload.clone(),
            BurnArtifactConfig::burnpack(ChunkingScheme::new(64).expect("chunking")),
        ),
    )
    .expect("validator builder")
    .with_network(network_manifest)
    .expect("network binding");
    let prepared = builder.prepare().expect("prepared node");

    assert_eq!(
        prepared.mainnet().roles,
        crate::PeerRoleSet::new([
            crate::PeerRole::Authority,
            crate::PeerRole::Validator,
            crate::PeerRole::Archive
        ])
    );
    assert_eq!(
        prepared.config().selected_workload_id,
        Some(supported_workload.workload_id)
    );
}

#[test]
fn connect_builder_accepts_explicit_target_preset() {
    let supported_workload = SupportedWorkload {
        workload_id: crate::WorkloadId::new("tiny-workload"),
        workload_name: "Tiny Burn Workload".into(),
        model_program_hash: ContentId::new("tiny-program"),
        checkpoint_format_hash: ContentId::new("tiny-burnpack"),
        supported_revision_family: ContentId::new("tiny-family"),
        resource_class: "cpu".into(),
    };
    let release_manifest = ClientReleaseManifest {
        project_family_id: crate::ProjectFamilyId::new("tiny-family"),
        release_train_hash: ContentId::new("tiny-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("tiny-artifact"),
        target_platform: crate::ClientPlatform::Native,
        app_semver: semver::Version::new(0, 1, 0),
        git_commit: "local".into(),
        cargo_lock_hash: ContentId::new("tiny-lock"),
        burn_version_string: "0.21.0-pre.2".into(),
        enabled_features_hash: ContentId::new("tiny-features"),
        protocol_major: 0,
        supported_workloads: vec![supported_workload.clone()],
        built_at: Utc::now(),
    };

    let builder = connect(
        BurnTarget::Trainer,
        release_manifest,
        TinyBurnWorkload,
        BurnWorkloadConfig::new(
            supported_workload,
            BurnArtifactConfig::burnpack(ChunkingScheme::new(64).expect("chunking")),
        ),
    )
    .expect("native builder");

    assert_eq!(builder.config().selected_workload_id, None);
}

#[test]
fn learner_workload_runs_default_burn_window() {
    let device = <LearnerBackend as Backend>::Device::default();
    let mut ctx = WindowCtx {
        device,
        model: BurnLearnerWorkload::init_model(&TinyLearnerWorkload, &device),
        lease: AssignmentLease {
            lease_id: crate::LeaseId::new("lease-1"),
            network_id: crate::NetworkId::new("net-1"),
            study_id: crate::StudyId::new("study-1"),
            experiment_id: crate::ExperimentId::new("exp-1"),
            revision_id: crate::RevisionId::new("rev-1"),
            peer_id: crate::PeerId::new("peer-1"),
            dataset_view_id: crate::DatasetViewId::new("view-1"),
            window_id: crate::WindowId(1),
            granted_at: Utc::now(),
            expires_at: Utc::now(),
            budget_work_units: 1,
            microshards: vec![crate::MicroShardId::new("shard-1")],
            assignment_hash: ContentId::new("assign-1"),
        },
        batches: vec![
            TinyLearnerWorkload::batch::<LearnerBackend>(&device, 1.0),
            TinyLearnerWorkload::batch::<LearnerBackend>(&device, 2.0),
        ],
    };

    let report = TinyLearnerWorkload
        .train_window(&mut ctx)
        .expect("learner-backed burn window");

    assert_eq!(
        report.stats.get("train_steps"),
        Some(&MetricValue::Integer(2))
    );
    assert!(matches!(
        report.stats.get("learning_rate"),
        Some(MetricValue::Float(_))
    ));
    assert!(matches!(
        report.stats.get("loss"),
        Some(MetricValue::Float(_))
    ));
}

#[test]
fn from_loaders_builder_wraps_single_burn_workload_with_trainer_roles() {
    let supported_workload = SupportedWorkload {
        workload_id: crate::WorkloadId::new("tiny-workload"),
        workload_name: "Tiny Burn Workload".into(),
        model_program_hash: ContentId::new("tiny-program"),
        checkpoint_format_hash: ContentId::new("tiny-burnpack"),
        supported_revision_family: ContentId::new("tiny-family"),
        resource_class: "cpu".into(),
    };
    let release_manifest = ClientReleaseManifest {
        project_family_id: crate::ProjectFamilyId::new("tiny-family"),
        release_train_hash: ContentId::new("tiny-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("tiny-artifact"),
        target_platform: crate::ClientPlatform::Native,
        app_semver: semver::Version::new(0, 1, 0),
        git_commit: "local".into(),
        cargo_lock_hash: ContentId::new("tiny-lock"),
        burn_version_string: "0.21.0-pre.2".into(),
        enabled_features_hash: ContentId::new("tiny-features"),
        protocol_major: 0,
        supported_workloads: vec![supported_workload.clone()],
        built_at: Utc::now(),
    };
    let network_manifest = crate::NetworkManifest {
        network_id: crate::NetworkId::new("tiny-network"),
        project_family_id: release_manifest.project_family_id.clone(),
        protocol_major: release_manifest.protocol_major,
        required_release_train_hash: release_manifest.release_train_hash.clone(),
        allowed_target_artifact_hashes: std::collections::BTreeSet::from([release_manifest
            .target_artifact_hash
            .clone()]),
        authority_public_keys: Vec::new(),
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: ContentId::new("tiny-auth-policy"),
        created_at: Utc::now(),
        description: "tiny network".into(),
    };
    let device = <LearnerBackend as Backend>::Device::default();
    let builder = from_loaders(
        BurnLearner::new(
            TinyLearnerModel::<LearnerBackend>::new(&device),
            SgdConfig::new().init(),
            0.05,
        ),
        device,
        tiny_train_loader(),
        tiny_eval_loader(),
    )
    .trainer(release_manifest, supported_workload.clone())
    .expect("trainer builder")
    .with_network(network_manifest)
    .expect("network binding");
    let prepared = builder.prepare().expect("prepared node");

    assert_eq!(prepared.mainnet().roles, crate::RoleSet::default_trainer());
    assert_eq!(
        prepared.config().selected_workload_id,
        Some(supported_workload.workload_id)
    );
}

#[test]
fn from_learner_validator_builder_does_not_require_training_dataset_hooks() {
    let supported_workload = SupportedWorkload {
        workload_id: crate::WorkloadId::new("tiny-workload"),
        workload_name: "Tiny Burn Workload".into(),
        model_program_hash: ContentId::new("tiny-program"),
        checkpoint_format_hash: ContentId::new("tiny-burnpack"),
        supported_revision_family: ContentId::new("tiny-family"),
        resource_class: "cpu".into(),
    };
    let release_manifest = ClientReleaseManifest {
        project_family_id: crate::ProjectFamilyId::new("tiny-family"),
        release_train_hash: ContentId::new("tiny-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("tiny-artifact"),
        target_platform: crate::ClientPlatform::Native,
        app_semver: semver::Version::new(0, 1, 0),
        git_commit: "local".into(),
        cargo_lock_hash: ContentId::new("tiny-lock"),
        burn_version_string: "0.21.0-pre.2".into(),
        enabled_features_hash: ContentId::new("tiny-features"),
        protocol_major: 0,
        supported_workloads: vec![supported_workload.clone()],
        built_at: Utc::now(),
    };
    let network_manifest = crate::NetworkManifest {
        network_id: crate::NetworkId::new("tiny-network"),
        project_family_id: release_manifest.project_family_id.clone(),
        protocol_major: release_manifest.protocol_major,
        required_release_train_hash: release_manifest.release_train_hash.clone(),
        allowed_target_artifact_hashes: std::collections::BTreeSet::from([release_manifest
            .target_artifact_hash
            .clone()]),
        authority_public_keys: Vec::new(),
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: ContentId::new("tiny-auth-policy"),
        created_at: Utc::now(),
        description: "tiny network".into(),
    };
    let device = <LearnerBackend as Backend>::Device::default();
    let builder = from_learner(
        BurnLearner::new(
            TinyLearnerModel::<LearnerBackend>::new(&device),
            SgdConfig::new().init(),
            0.05,
        ),
        device,
    )
    .with_eval_loader(tiny_eval_loader())
    .validator(release_manifest, supported_workload.clone())
    .expect("validator builder")
    .with_network(network_manifest)
    .expect("network binding");
    let prepared = builder.prepare().expect("prepared node");

    assert_eq!(
        prepared.mainnet().roles,
        crate::PeerRoleSet::new([
            crate::PeerRole::Authority,
            crate::PeerRole::Validator,
            crate::PeerRole::Archive,
        ])
    );
    assert_eq!(
        prepared.config().selected_workload_id,
        Some(supported_workload.workload_id)
    );
}

#[test]
fn from_learner_custom_non_training_builder_does_not_require_training_dataset_hooks() {
    let supported_workload = SupportedWorkload {
        workload_id: crate::WorkloadId::new("tiny-workload"),
        workload_name: "Tiny Burn Workload".into(),
        model_program_hash: ContentId::new("tiny-program"),
        checkpoint_format_hash: ContentId::new("tiny-burnpack"),
        supported_revision_family: ContentId::new("tiny-family"),
        resource_class: "cpu".into(),
    };
    let release_manifest = ClientReleaseManifest {
        project_family_id: crate::ProjectFamilyId::new("tiny-family"),
        release_train_hash: ContentId::new("tiny-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("tiny-artifact"),
        target_platform: crate::ClientPlatform::Native,
        app_semver: semver::Version::new(0, 1, 0),
        git_commit: "local".into(),
        cargo_lock_hash: ContentId::new("tiny-lock"),
        burn_version_string: "0.21.0-pre.2".into(),
        enabled_features_hash: ContentId::new("tiny-features"),
        protocol_major: 0,
        supported_workloads: vec![supported_workload.clone()],
        built_at: Utc::now(),
    };
    let network_manifest = crate::NetworkManifest {
        network_id: crate::NetworkId::new("tiny-network"),
        project_family_id: release_manifest.project_family_id.clone(),
        protocol_major: release_manifest.protocol_major,
        required_release_train_hash: release_manifest.release_train_hash.clone(),
        allowed_target_artifact_hashes: std::collections::BTreeSet::from([release_manifest
            .target_artifact_hash
            .clone()]),
        authority_public_keys: Vec::new(),
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: ContentId::new("tiny-auth-policy"),
        created_at: Utc::now(),
        description: "tiny network".into(),
    };
    let device = <LearnerBackend as Backend>::Device::default();
    let roles = crate::PeerRoleSet::new([crate::PeerRole::PortalViewer]);
    let builder = from_learner(
        BurnLearner::new(
            TinyLearnerModel::<LearnerBackend>::new(&device),
            SgdConfig::new().init(),
            0.05,
        ),
        device,
    )
    .connect(
        BurnTarget::Custom(roles.clone()),
        release_manifest,
        supported_workload.clone(),
    )
    .expect("custom builder")
    .with_network(network_manifest)
    .expect("network binding");
    let prepared = builder.prepare().expect("prepared node");

    assert_eq!(prepared.mainnet().roles, roles);
    assert_eq!(
        prepared.config().selected_workload_id,
        Some(supported_workload.workload_id)
    );
}

#[test]
fn from_learner_trainer_builder_still_requires_training_dataset_hooks() {
    let supported_workload = SupportedWorkload {
        workload_id: crate::WorkloadId::new("tiny-workload"),
        workload_name: "Tiny Burn Workload".into(),
        model_program_hash: ContentId::new("tiny-program"),
        checkpoint_format_hash: ContentId::new("tiny-burnpack"),
        supported_revision_family: ContentId::new("tiny-family"),
        resource_class: "cpu".into(),
    };
    let release_manifest = ClientReleaseManifest {
        project_family_id: crate::ProjectFamilyId::new("tiny-family"),
        release_train_hash: ContentId::new("tiny-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("tiny-artifact"),
        target_platform: crate::ClientPlatform::Native,
        app_semver: semver::Version::new(0, 1, 0),
        git_commit: "local".into(),
        cargo_lock_hash: ContentId::new("tiny-lock"),
        burn_version_string: "0.21.0-pre.2".into(),
        enabled_features_hash: ContentId::new("tiny-features"),
        protocol_major: 0,
        supported_workloads: vec![supported_workload],
        built_at: Utc::now(),
    };
    let device = <LearnerBackend as Backend>::Device::default();
    let error = from_learner(
        BurnLearner::new(
            TinyLearnerModel::<LearnerBackend>::new(&device),
            SgdConfig::new().init(),
            0.05,
        ),
        device,
    )
    .trainer(
        release_manifest,
        SupportedWorkload {
            workload_id: crate::WorkloadId::new("tiny-workload"),
            workload_name: "Tiny Burn Workload".into(),
            model_program_hash: ContentId::new("tiny-program"),
            checkpoint_format_hash: ContentId::new("tiny-burnpack"),
            supported_revision_family: ContentId::new("tiny-family"),
            resource_class: "cpu".into(),
        },
    )
    .err()
    .expect("trainer should require dataset hooks");

    assert!(
        error
            .to_string()
            .contains("missing burn learner training data")
    );
}

#[test]
fn from_learner_builder_accepts_train_loader_directly() {
    let device = <LearnerBackend as Backend>::Device::default();
    let project = from_learner(
        BurnLearner::new(
            TinyLearnerModel::<LearnerBackend>::new(&device),
            SgdConfig::new().init(),
            0.05,
        ),
        device,
    )
    .with_train_loader(tiny_train_loader())
    .build()
    .expect("learner project");

    let batches = project
        .load_batches(&tiny_assignment_lease(), &[])
        .expect("train loader batches");

    assert_eq!(batches.len(), 1);
    assert_eq!(
        project
            .dataset_registration()
            .expect("local registration")
            .manifest
            .format,
        "runtime-local"
    );
}

#[test]
fn from_loaders_builder_defaults_evaluate_and_local_dataset_hooks() {
    let device = <LearnerBackend as Backend>::Device::default();
    let project = from_loaders(
        BurnLearner::new(
            TinyLearnerModel::<LearnerBackend>::new(&device),
            SgdConfig::new().init(),
            0.05,
        ),
        device,
        tiny_train_loader(),
        tiny_eval_loader(),
    )
    .build()
    .expect("learner project");

    let registration = project.dataset_registration().expect("local registration");
    let plan = project
        .microshard_plan(&registration)
        .expect("local microshard plan");
    let evaluation = project.evaluate(&project.init_model(&device), EvalSplit::Validation);

    assert_eq!(registration.manifest.format, "runtime-local");
    assert_eq!(plan.microshards.len(), 1);
    assert!(matches!(
        evaluation.metrics.get("evaluation_items"),
        Some(MetricValue::Integer(1))
    ));
    assert!(matches!(
        evaluation.metrics.get("evaluation_batches"),
        Some(MetricValue::Integer(1))
    ));
    assert!(matches!(
        evaluation.metrics.get("parameter_count"),
        Some(MetricValue::Integer(value)) if *value > 0
    ));
}

#[test]
fn from_loaders_builder_supports_sharded_dataset_training_hooks() {
    let dataset_root = tempdir().expect("dataset root");
    let cache_root = tempdir().expect("cache root");
    let dataset = tiny_sharded_dataset(dataset_root.path(), 2).expect("sharded dataset");
    let lease = crate::LeasePlanner::default()
        .plan_lease(
            crate::NetworkId::new("net-1"),
            crate::StudyId::new("study-1"),
            crate::ExperimentId::new("exp-1"),
            crate::RevisionId::new("rev-1"),
            &dataset.microshard_plan().dataset_view,
            crate::PeerId::new("peer-1"),
            crate::WindowId(1),
            Utc::now(),
            1,
            &dataset.microshard_plan().microshards[..1],
        )
        .expect("lease")
        .lease;
    let cached = crate::ShardCache::new(cache_root.path())
        .fetch_lease_microshards(dataset.registration(), dataset.microshard_plan(), &lease)
        .expect("cached shards");
    let device = <LearnerBackend as Backend>::Device::default();
    let project = from_loaders(
        BurnLearner::new(
            TinyLearnerModel::<LearnerBackend>::new(&device),
            SgdConfig::new().init(),
            0.05,
        ),
        device,
        tiny_train_loader(),
        tiny_eval_loader(),
    )
    .with_sharded_dataset(dataset.clone(), TinyLearnerBatcher, 2)
    .build()
    .expect("learner project");

    let registration = project.dataset_registration().expect("registration");
    let plan = project
        .microshard_plan(&registration)
        .expect("microshard plan");
    let loaded_records = dataset.load_records(&cached).expect("loaded records");
    let batches = project
        .load_batches(&lease, &cached)
        .expect("loaded batches");

    assert_eq!(registration, dataset.registration().clone());
    assert_eq!(plan, dataset.microshard_plan().clone());
    assert_eq!(loaded_records.len(), dataset.examples_for_lease(&lease));
    assert_eq!(batches.len(), 1);
}

#[test]
fn sharded_dataset_http_upstream_fetches_only_assigned_shards() {
    let upstream_dir = tempdir().expect("upstream dir");
    let cache_dir = tempdir().expect("cache dir");
    let dataset = tiny_sharded_dataset(upstream_dir.path(), 3).expect("sharded dataset");
    let selected_microshard = dataset.microshard_plan().microshards[1].clone();
    let manifest: crate::ShardFetchManifest = serde_json::from_slice(
        &fs::read(upstream_dir.path().join("fetch-manifest.json")).expect("manifest bytes"),
    )
    .expect("manifest");
    let selected_locator = manifest
        .entry_for_microshard(&selected_microshard.microshard_id)
        .expect("selected locator")
        .locator
        .clone();

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    let stop = Arc::new(AtomicBool::new(false));
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let root = upstream_dir.path().to_path_buf();
    let stop_for_thread = Arc::clone(&stop);
    let requests_for_thread = Arc::clone(&requests);
    let server = thread::spawn(move || {
        while !stop_for_thread.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
                    let mut line = String::new();
                    reader.read_line(&mut line).expect("request line");
                    let path = line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("/")
                        .trim_start_matches('/')
                        .to_owned();
                    requests_for_thread
                        .lock()
                        .expect("requests")
                        .push(path.clone());
                    loop {
                        let mut header = String::new();
                        reader.read_line(&mut header).expect("header");
                        if header == "\r\n" || header.is_empty() {
                            break;
                        }
                    }
                    let body = fs::read(root.join(&path)).expect("served file");
                    stream
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .expect("write head");
                    stream.write_all(&body).expect("write body");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("server error: {error}"),
            }
        }
    });

    let http_dataset = dataset.clone().with_http_upstream(format!("http://{addr}"));
    let lease = crate::LeasePlanner::default()
        .plan_lease(
            crate::NetworkId::new("net-1"),
            crate::StudyId::new("study-1"),
            crate::ExperimentId::new("exp-1"),
            crate::RevisionId::new("rev-1"),
            &http_dataset.microshard_plan().dataset_view,
            crate::PeerId::new("peer-1"),
            crate::WindowId(3),
            Utc::now(),
            1,
            std::slice::from_ref(&selected_microshard),
        )
        .expect("lease")
        .lease;
    let cache = crate::ShardCache::new(cache_dir.path());

    cache
        .fetch_lease_microshards(
            http_dataset.registration(),
            http_dataset.microshard_plan(),
            &lease,
        )
        .expect("first fetch");
    cache
        .fetch_lease_microshards(
            http_dataset.registration(),
            http_dataset.microshard_plan(),
            &lease,
        )
        .expect("second fetch");

    stop.store(true, Ordering::Relaxed);
    let _ = server.join();

    let requests = requests.lock().expect("requests").clone();
    assert_eq!(
        requests,
        vec!["fetch-manifest.json".to_string(), selected_locator.clone()]
    );
    assert!(
        !requests
            .iter()
            .any(|path| path.ends_with("00000.bin") && path != &selected_locator)
    );
    assert!(
        !requests
            .iter()
            .any(|path| path.ends_with("00002.bin") && path != &selected_locator)
    );
}

#[test]
fn from_learner_builder_supports_assignment_aware_batches_with_local_dataset_defaults() {
    let device = <LearnerBackend as Backend>::Device::default();
    let project = from_learner(
        BurnLearner::new(
            TinyLearnerModel::<LearnerBackend>::new(&device),
            SgdConfig::new().init(),
            0.05,
        ),
        device,
    )
    .with_assignment_batches(|lease, device| {
        let value = if lease.window_id.0 == 1 { 1.0 } else { 2.0 };
        Ok(vec![TinyLearnerWorkload::batch::<LearnerBackend>(
            device, value,
        )])
    })
    .build()
    .expect("learner project");

    let batches = project
        .load_batches(&tiny_assignment_lease(), &[])
        .expect("local assignment-aware batches");

    assert_eq!(batches.len(), 1);
    assert_eq!(
        project
            .dataset_registration()
            .expect("local registration")
            .manifest
            .format,
        "runtime-local"
    );
}
