//! Public facade for the burn_p2p runtime, configuration, and workload surface.
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use burn_p2p::{
    ArtifactBuildSpec, ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard,
    CapabilityEstimate, ChunkingScheme, ClientReleaseManifest, ContentId, DatasetRegistration,
    DatasetSizing, EvalSplit, FsArtifactStore, GenesisSpec, MetricReport, MetricValue,
    MicroShardPlannerConfig, NodeBuilder, P2pWorkload, PatchOutcome, PatchSupport, ProjectFamilyId,
    RoleSet, RuntimePatch, ShardFetchManifest, SingleWorkloadProjectFamily, StorageConfig,
    SupportedWorkload, TrainError, UpstreamAdapter, WindowCtx, WindowReport, WorkloadId,
};
use chrono::Utc;
use semver::Version;

#[derive(Clone, Debug)]
struct SyntheticProject {
    dataset_root: PathBuf,
    learning_rate: f64,
    target_model: f64,
}

impl P2pWorkload for SyntheticProject {
    type Device = String;
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
        Ok(burn_p2p::MicroShardPlanner::new(MicroShardPlannerConfig {
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

    fn supported_workload(&self) -> SupportedWorkload {
        synthetic_workload_manifest()
    }

    fn model_schema_hash(&self) -> ContentId {
        ContentId::new("synthetic-schema")
    }
}

fn synthetic_workload_manifest() -> SupportedWorkload {
    SupportedWorkload {
        workload_id: WorkloadId::new("synthetic-regression"),
        workload_name: "Synthetic Regression".into(),
        model_program_hash: ContentId::new("synthetic-program"),
        checkpoint_format_hash: ContentId::new("synthetic-json"),
        supported_revision_family: ContentId::new("synthetic-revision-family"),
        resource_class: "cpu".into(),
    }
}

fn synthetic_release_manifest() -> ClientReleaseManifest {
    ClientReleaseManifest {
        project_family_id: ProjectFamilyId::new("synthetic-family"),
        release_train_hash: ContentId::new("synthetic-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("synthetic-artifact-native"),
        target_platform: burn_p2p::ClientPlatform::Native,
        app_semver: Version::new(0, 2, 0),
        git_commit: "local-demo".into(),
        cargo_lock_hash: ContentId::new("cargo-lock"),
        burn_version_string: "0.21.0-pre.3".into(),
        enabled_features_hash: ContentId::new("burn-runtime"),
        protocol_major: 1,
        supported_workloads: vec![synthetic_workload_manifest()],
        built_at: Utc::now(),
    }
}

fn main() -> anyhow::Result<()> {
    let data_root = PathBuf::from(".burn_p2p-demo-data");
    create_demo_dataset(&data_root)?;

    let genesis = GenesisSpec {
        network_id: burn_p2p::NetworkId::new("local-demo"),
        protocol_version: Version::new(0, 1, 0),
        display_name: "Local demo network".into(),
        created_at: Utc::now(),
        metadata: BTreeMap::from([("purpose".into(), "synthetic runtime example".into())]),
    };
    let network_manifest = burn_p2p::NetworkManifest {
        network_id: genesis.network_id.clone(),
        project_family_id: ProjectFamilyId::new("synthetic-family"),
        protocol_major: 0,
        required_release_train_hash: ContentId::new("synthetic-train"),
        allowed_target_artifact_hashes: BTreeSet::from([ContentId::new(
            "synthetic-artifact-native",
        )]),
        authority_public_keys: vec!["local-validator".into()],
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
        burn_p2p::StudyId::new("synthetic-study"),
        burn_p2p::ExperimentId::new("synthetic-exp"),
        burn_p2p::RevisionId::new("rev-1"),
    );

    let validator_family = SingleWorkloadProjectFamily::new(
        synthetic_release_manifest(),
        SyntheticProject {
            dataset_root: data_root.clone(),
            learning_rate: 1.0,
            target_model: 10.0,
        },
    )?;
    let validator = NodeBuilder::new(validator_family)
        .with_network(network_manifest.clone())?
        .with_roles(burn_p2p::PeerRoleSet::new([
            burn_p2p::PeerRole::Authority,
            burn_p2p::PeerRole::Validator,
            burn_p2p::PeerRole::Archive,
        ]))
        .with_storage(StorageConfig::new(".burn_p2p-demo/validator"))
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
        "validator published genesis head {}",
        genesis_head.head_id.as_str()
    );

    let trainer_family = SingleWorkloadProjectFamily::new(
        synthetic_release_manifest(),
        SyntheticProject {
            dataset_root: data_root,
            learning_rate: 1.0,
            target_model: 10.0,
        },
    )?;
    let trainer = NodeBuilder::new(trainer_family)
        .with_network(network_manifest)?
        .with_roles(RoleSet::default_trainer())
        .with_storage(StorageConfig::new(".burn_p2p-demo/trainer"))
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
        "trainer published candidate head {} with loss {:?}",
        training.head.head_id.as_str(),
        training.report.stats.get("loss")
    );

    let validated = validator
        .validate_candidates_once(&experiment)?
        .expect("validator should certify the trainer update");
    println!(
        "validator certified head {} from {}",
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
    let project = SyntheticProject {
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
