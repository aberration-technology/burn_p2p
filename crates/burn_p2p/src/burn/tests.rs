use super::*;
use burn::{
    backend::{Autodiff, NdArray},
    module::Module,
    nn::{Linear, LinearConfig},
    optim::SgdConfig,
    tensor::{
        ElementConversion, Tensor,
        backend::{AutodiffBackend, Backend},
    },
    train::{InferenceStep, ItemLazy, TrainOutput, TrainStep},
};
use chrono::Utc;

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
    type Optimizer = BurnSgdOptimizer<Self::Backend, Self::Model>;
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
fn from_learner_builder_wraps_single_burn_workload_with_trainer_roles() {
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
    .with_batches(|device| {
        Ok(vec![
            TinyLearnerWorkload::batch::<LearnerBackend>(device, 1.0),
            TinyLearnerWorkload::batch::<LearnerBackend>(device, 2.0),
        ])
    })
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
fn from_learner_builder_defaults_evaluate_and_local_dataset_hooks() {
    let device = <LearnerBackend as Backend>::Device::default();
    let project = from_learner(
        BurnLearner::new(
            TinyLearnerModel::<LearnerBackend>::new(&device),
            SgdConfig::new().init(),
            0.05,
        ),
        device,
    )
    .with_batches(|device| {
        Ok(vec![
            TinyLearnerWorkload::batch::<LearnerBackend>(device, 1.0),
            TinyLearnerWorkload::batch::<LearnerBackend>(device, 2.0),
        ])
    })
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
        evaluation.metrics.get("parameter_count"),
        Some(MetricValue::Integer(value)) if *value > 0
    ));
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
