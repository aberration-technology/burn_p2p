//! Bootstrap, edge, and operator-facing services for burn_p2p deployments.
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use burn_p2p::{
    ArtifactBuildSpec, ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard,
    CapabilityEstimate, ChunkingScheme, ClientReleaseManifest, ContentId, DatasetRegistration,
    DatasetSizing, EvalSplit, FsArtifactStore, MetricReport, MetricValue, MicroShardPlanner,
    MicroShardPlannerConfig, P2pWorkload, PatchOutcome, PatchSupport, ProjectFamilyId,
    RuntimePatch, ShardFetchManifest, SingleWorkloadProjectFamily, StorageConfig,
    SupportedWorkload, TrainError, UpstreamAdapter, WindowCtx, WindowReport, WorkloadId,
};
use burn_p2p_bootstrap::{
    ActiveExperiment, AdminApiPlan, ArchivePlan, AuthorityPlan, BootstrapEmbeddedDaemonConfig,
    BootstrapPreset, BootstrapSpec,
};
use burn_p2p_core::ClientPlatform;
use burn_p2p_security::{MergeEvidenceRequirement, ReleasePolicy, ValidatorPolicy};
use chrono::Utc;
use semver::{Version, VersionReq};
use tempfile::tempdir;

#[derive(Clone, Debug)]
struct DemoProject {
    dataset_root: PathBuf,
    learning_rate: f64,
    target_model: f64,
}

impl P2pWorkload for DemoProject {
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
        Ok(MicroShardPlanner::new(MicroShardPlannerConfig {
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
            burn_p2p::ContentId::new("demo-schema"),
            "synthetic-json",
        )
        .with_head(head_id);
        if let Some(base_head_id) = base_head_id {
            spec = spec.with_base_head(base_head_id);
        }
        Ok(store.store_artifact_reader(
            &spec,
            std::io::Cursor::new(serde_json::to_vec(model)?),
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
        demo_workload_manifest()
    }

    fn model_schema_hash(&self) -> ContentId {
        ContentId::new("demo-schema")
    }
}

fn demo_workload_manifest() -> SupportedWorkload {
    SupportedWorkload {
        workload_id: WorkloadId::new("embedded-demo-regression"),
        workload_name: "Embedded Demo Regression".into(),
        model_program_hash: ContentId::new("demo-program"),
        checkpoint_format_hash: ContentId::new("synthetic-json"),
        supported_revision_family: ContentId::new("demo-revision-family"),
        resource_class: "cpu".into(),
    }
}

fn demo_release_manifest() -> ClientReleaseManifest {
    ClientReleaseManifest {
        project_family_id: ProjectFamilyId::new("embedded-demo-family"),
        release_train_hash: ContentId::new("embedded-demo-train"),
        target_artifact_id: "native-linux-x86_64".into(),
        target_artifact_hash: ContentId::new("embedded-demo-artifact-native"),
        target_platform: ClientPlatform::Native,
        app_semver: Version::new(0, 2, 0),
        git_commit: "local-demo".into(),
        cargo_lock_hash: ContentId::new("cargo-lock"),
        burn_version_string: "0.21.0-pre.3".into(),
        enabled_features_hash: ContentId::new("bootstrap-example"),
        protocol_major: 0,
        supported_workloads: vec![demo_workload_manifest()],
        built_at: Utc::now(),
    }
}

fn main() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let dataset_root = temp.path().join("dataset");
    let storage_root = temp.path().join("bootstrap");
    create_demo_dataset(&dataset_root)?;

    let genesis = burn_p2p::GenesisSpec {
        network_id: burn_p2p::NetworkId::new("bootstrap-demo"),
        protocol_version: Version::new(0, 1, 0),
        display_name: "Embedded bootstrap demo".into(),
        created_at: Utc::now(),
        metadata: BTreeMap::new(),
    };
    let plan = BootstrapSpec {
        preset: BootstrapPreset::AuthorityValidator,
        genesis: genesis.clone(),
        platform: ClientPlatform::Native,
        bootstrap_addresses: Vec::new(),
        listen_addresses: Vec::new(),
        authority: Some(AuthorityPlan {
            release_policy: ReleasePolicy::new(
                Version::new(0, 1, 0),
                vec![VersionReq::parse("^0.1")?],
            )?,
            validator_policy: ValidatorPolicy {
                release_policy: ReleasePolicy::new(
                    Version::new(0, 1, 0),
                    vec![VersionReq::parse("^0.1")?],
                )?,
                evidence_requirement: MergeEvidenceRequirement::default(),
            },
            validator_set_manifest: None,
            authority_epoch_manifest: None,
        }),
        archive: ArchivePlan::default(),
        admin_api: AdminApiPlan::default(),
    }
    .plan()?;

    let daemon_family = SingleWorkloadProjectFamily::new(
        demo_release_manifest(),
        DemoProject {
            dataset_root,
            learning_rate: 1.0,
            target_model: 10.0,
        },
    )?;
    let daemon = plan.spawn_embedded_daemon(
        daemon_family,
        BootstrapEmbeddedDaemonConfig {
            node: burn_p2p::NodeConfig {
                identity: burn_p2p::IdentityConfig::Persistent,
                storage: Some(StorageConfig::new(storage_root)),
                dataset: None,
                auth: None,
                network_manifest: None,
                client_release_manifest: Some(demo_release_manifest()),
                selected_workload_id: Some(demo_workload_manifest().workload_id),
                metrics_retention: burn_p2p::MetricsRetentionConfig::default(),
                bootstrap_peers: Vec::new(),
                listen_addresses: Vec::new(),
                external_addresses: Vec::new(),
            },
            active_experiment: ActiveExperiment {
                study_id: burn_p2p::StudyId::new("study"),
                experiment_id: burn_p2p::ExperimentId::new("demo-exp"),
                revision_id: burn_p2p::RevisionId::new("rev-1"),
            },
            initialize_head_on_start: true,
            restore_head_on_start: true,
            validation_interval_millis: 200,
            reducer_interval_millis: None,
            training_interval_millis: Some(200),
        },
    )?;

    wait_for(
        Duration::from_secs(5),
        || daemon.telemetry().snapshot().status == burn_p2p::RuntimeStatus::Running,
        "daemon runtime did not start",
    )?;
    let diagnostics = daemon.diagnostics(Some(64));
    println!(
        "embedded daemon services: {:?}, connected peers: {}",
        diagnostics.services, diagnostics.swarm.connected_peers
    );
    daemon.shutdown()?;
    daemon.await_termination()?;
    Ok(())
}

fn create_demo_dataset(root: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(root)?;
    let registration = DemoProject {
        dataset_root: root.to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    }
    .dataset_registration()?;
    let plan = DemoProject {
        dataset_root: root.to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    }
    .microshard_plan(&registration)?;
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
