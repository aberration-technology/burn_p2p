//! Public facade for the burn_p2p runtime, configuration, and compatibility surface.
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use burn::{
    backend::NdArray,
    module::{Module, ModuleMapper, Param},
    nn::{Linear, LinearConfig},
    tensor::{Tensor, backend::Backend},
};
use burn_p2p::{
    AssignmentLease, CachedMicroShard, CapabilityEstimate, ClientReleaseManifest, ContentId,
    DatasetRegistration, DatasetSizing, EvalSplit, FsArtifactStore, GenesisSpec, MergePolicy,
    MetricReport, MetricValue, NetworkManifest, NodeBuilder, P2pWorkload, PatchOutcome,
    PatchSupport, ProjectBackend, ProjectFamilyId, RoleSet, RuntimePatch, ShardFetchManifest,
    SingleWorkloadProjectFamily, StorageConfig, SupportedWorkload, TrainError, WindowCtx,
    WindowReport, WorkloadId,
    compat::{P2pProject, RuntimeProject},
};
use chrono::Utc;
use semver::Version;

type RuntimeBackend = NdArray<f32>;

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
struct TinyProject {
    dataset_root: PathBuf,
    ema_decay: f64,
}

#[derive(Debug)]
struct AddScalarMapper {
    delta: f32,
}

impl<B: Backend> ModuleMapper<B> for AddScalarMapper {
    fn map_float<const D: usize>(&mut self, param: Param<Tensor<B, D>>) -> Param<Tensor<B, D>> {
        param.map(|tensor| tensor + self.delta)
    }
}

fn shift_model<B: Backend>(model: TinyModel<B>, delta: f32) -> TinyModel<B> {
    let mut mapper = AddScalarMapper { delta };
    model.map(&mut mapper)
}

impl ProjectBackend for TinyProject {
    type Device = <RuntimeBackend as Backend>::Device;
}

impl P2pProject<TinyProject> for TinyProject {
    type Model = TinyModel<RuntimeBackend>;
    type Batch = f32;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, device: &<TinyProject as ProjectBackend>::Device) -> Self::Model {
        TinyModel::<RuntimeBackend>::new(device)
    }

    fn benchmark(
        &self,
        _model: &Self::Model,
        _device: &<TinyProject as ProjectBackend>::Device,
    ) -> CapabilityEstimate {
        CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 32.0,
            target_window_seconds: 1,
        }
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<<TinyProject as ProjectBackend>::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        let batch_sum = ctx.batches.iter().copied().sum::<f32>() as f64;
        ctx.model = shift_model(ctx.model.clone(), (batch_sum as f32) / 100.0);
        let parameter_count =
            burn_p2p::burn::inspect_module::<RuntimeBackend, _>(&ctx.model).parameter_count as i64;

        Ok(WindowReport {
            contribution: None,
            stats: BTreeMap::from([
                ("loss".into(), MetricValue::Float(0.0)),
                ("batch_sum".into(), MetricValue::Float(batch_sum)),
                (
                    "parameter_count".into(),
                    MetricValue::Integer(parameter_count),
                ),
            ]),
            completed_at: Utc::now(),
        })
    }

    fn evaluate(&self, model: &Self::Model, _split: EvalSplit) -> MetricReport {
        let inventory = burn_p2p::burn::inspect_module::<RuntimeBackend, _>(model);
        MetricReport {
            metrics: BTreeMap::from([
                ("loss".into(), MetricValue::Float(0.0)),
                (
                    "parameter_count".into(),
                    MetricValue::Integer(inventory.parameter_count as i64),
                ),
            ]),
            captured_at: Utc::now(),
        }
    }

    fn apply_patch(&mut self, _patch: &RuntimePatch) -> PatchOutcome {
        PatchOutcome::Rejected("no runtime patches supported".into())
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        PatchSupport {
            hot: false,
            warm: false,
            cold: false,
        }
    }
}

impl RuntimeProject<TinyProject> for TinyProject {
    fn runtime_device(&self) -> <TinyProject as ProjectBackend>::Device {
        <RuntimeBackend as Backend>::Device::default()
    }

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        Ok(DatasetRegistration {
            manifest: burn_p2p::DatasetManifest {
                dataset_id: burn_p2p::DatasetId::new("burn-ndarray-dataset"),
                source_uri: self.dataset_root.display().to_string(),
                format: "microshards".into(),
                manifest_hash: burn_p2p::ContentId::new("burn-ndarray-manifest"),
                metadata: BTreeMap::new(),
            },
            view: burn_p2p::DatasetView {
                dataset_view_id: burn_p2p::DatasetViewId::new("burn-ndarray-view"),
                dataset_id: burn_p2p::DatasetId::new("burn-ndarray-dataset"),
                preprocessing_hash: burn_p2p::ContentId::new("burn-ndarray-preprocess"),
                tokenizer_hash: None,
                manifest_hash: burn_p2p::ContentId::new("burn-ndarray-manifest"),
                metadata: BTreeMap::new(),
            },
            upstream: burn_p2p::UpstreamAdapter::Local {
                root: self.dataset_root.display().to_string(),
            },
        })
    }

    fn microshard_plan(
        &self,
        registration: &DatasetRegistration,
    ) -> anyhow::Result<burn_p2p::MicroShardPlan> {
        Ok(
            burn_p2p::MicroShardPlanner::new(burn_p2p::MicroShardPlannerConfig {
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
            )?,
        )
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
                text.trim().parse::<f32>().map_err(anyhow::Error::from)
            })
            .collect()
    }

    fn load_model_artifact(
        &self,
        model: Self::Model,
        descriptor: &burn_p2p::ArtifactDescriptor,
        store: &FsArtifactStore,
        device: &<TinyProject as ProjectBackend>::Device,
    ) -> anyhow::Result<Self::Model> {
        let _ = device;
        Ok(burn_p2p::burn::load_store_bytes_runtime_artifact::<
            RuntimeBackend,
            _,
        >(
            model,
            descriptor,
            store,
            burn_p2p::burn::BurnStoreFormat::Burnpack,
        )?)
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: burn_p2p::ArtifactKind,
        head_id: burn_p2p::HeadId,
        base_head_id: Option<burn_p2p::HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<burn_p2p::ArtifactDescriptor> {
        Ok(burn_p2p::burn::materialize_store_bytes_runtime_artifact::<
            RuntimeBackend,
            _,
        >(
            model,
            store,
            burn_p2p::burn::StoreBytesRuntimeArtifactOptions {
                artifact_kind,
                head_id,
                base_head_id,
                format: burn_p2p::burn::BurnStoreFormat::Burnpack,
                declared_precision: burn_p2p::Precision::Fp32,
                chunking: burn_p2p::ChunkingScheme::new(64)?,
            },
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
        policy: MergePolicy,
    ) -> anyhow::Result<Option<Self::Model>> {
        let weighted = candidates
            .iter()
            .map(|candidate| {
                let quality = match policy {
                    MergePolicy::WeightedMean => 1.0,
                    MergePolicy::NormClippedWeightedMean => {
                        candidate.quality_weight.clamp(0.0, 1.0)
                    }
                    MergePolicy::TrimmedMean => candidate.quality_weight,
                    MergePolicy::Ema | MergePolicy::QualityWeightedEma => candidate.quality_weight,
                    MergePolicy::Custom(_) => candidate.quality_weight,
                };
                burn_p2p::burn::BurnMergeCandidate {
                    module: candidate.model,
                    weight: candidate.sample_weight * quality,
                }
            })
            .collect::<Vec<_>>();

        Ok(burn_p2p::burn::merge_weighted_mean_modules::<
            RuntimeBackend,
            _,
        >(base_model, &weighted)?)
    }

    fn apply_single_root_ema(
        &self,
        base_model: &Self::Model,
        merged_model: Self::Model,
        _policy: MergePolicy,
    ) -> anyhow::Result<Self::Model> {
        Ok(burn_p2p::burn::apply_root_ema_modules::<RuntimeBackend, _>(
            base_model,
            &merged_model,
            self.ema_decay,
        )?)
    }
}

impl P2pWorkload<TinyProject> for TinyProject {
    fn supported_workload(&self) -> SupportedWorkload {
        tiny_workload_manifest()
    }

    fn model_schema_hash(&self) -> ContentId {
        ContentId::new("burn-ndarray-schema")
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
        project_family_id: ProjectFamilyId::new("burn-ndarray-family"),
        release_train_hash: ContentId::new("burn-ndarray-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("burn-ndarray-artifact-native"),
        target_platform: burn_p2p::ClientPlatform::Native,
        app_semver: Version::new(0, 2, 0),
        git_commit: "local-demo".into(),
        cargo_lock_hash: ContentId::new("cargo-lock"),
        burn_version_string: "0.21.0-pre.2".into(),
        enabled_features_hash: ContentId::new("burn-runtime"),
        protocol_major: 0,
        supported_workloads: vec![tiny_workload_manifest()],
        built_at: Utc::now(),
    }
}

fn main() -> anyhow::Result<()> {
    let data_root = PathBuf::from(".burn_p2p-burn-demo-data");
    create_demo_dataset(&data_root)?;

    let genesis = GenesisSpec {
        network_id: burn_p2p::NetworkId::new("burn-ndarray-demo"),
        protocol_version: Version::new(0, 1, 0),
        display_name: "Burn NdArray Demo".into(),
        created_at: Utc::now(),
        metadata: BTreeMap::from([("purpose".into(), "burn runtime example".into())]),
    };
    let network_manifest = NetworkManifest {
        network_id: genesis.network_id.clone(),
        project_family_id: ProjectFamilyId::new("burn-ndarray-family"),
        protocol_major: 0,
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

    let validator_family = SingleWorkloadProjectFamily::<TinyProject, _>::new(
        tiny_release_manifest(),
        TinyProject {
            dataset_root: data_root.clone(),
            ema_decay: 0.75,
        },
    )?;
    let validator = NodeBuilder::new(validator_family)
        .with_network(network_manifest.clone())?
        .with_roles(burn_p2p::PeerRoleSet::new([
            burn_p2p::PeerRole::Authority,
            burn_p2p::PeerRole::Validator,
            burn_p2p::PeerRole::Archive,
        ]))
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
    let genesis_head = validator.initialize_local_head::<TinyProject>(&experiment)?;
    println!(
        "validator published Burn genesis head {}",
        genesis_head.head_id.as_str()
    );

    let trainer_family = SingleWorkloadProjectFamily::<TinyProject, _>::new(
        tiny_release_manifest(),
        TinyProject {
            dataset_root: data_root,
            ema_decay: 0.75,
        },
    )?;
    let trainer = NodeBuilder::new(trainer_family)
        .with_network(network_manifest)?
        .with_roles(RoleSet::default_trainer())
        .with_storage(StorageConfig::new(".burn_p2p-burn-demo/trainer"))
        .with_bootstrap_peer(validator_addr)
        .spawn()?;
    wait_for(
        Duration::from_secs(5),
        || trainer.telemetry().snapshot().connected_peers >= 1,
        "trainer did not connect to validator",
    )?;

    let mut trainer = trainer;
    let training = trainer.train_window_once::<TinyProject>(&experiment)?;
    println!(
        "trainer published Burn candidate head {} with metrics {:?}",
        training.head.head_id.as_str(),
        training.report.stats
    );

    let validated = validator
        .validate_candidates_once::<TinyProject>(&experiment)?
        .expect("validator should certify the trainer update");
    println!(
        "validator certified Burn head {} from {}",
        validated.merged_head.head_id.as_str(),
        validated.source_peer_id.as_str()
    );

    trainer.shutdown()?;
    let _ = trainer.await_termination()?;
    validator.shutdown()?;
    let _ = validator.await_termination()?;
    Ok(())
}

fn create_demo_dataset(root: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(root)?;
    let project = TinyProject {
        dataset_root: root.to_path_buf(),
        ema_decay: 0.75,
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
