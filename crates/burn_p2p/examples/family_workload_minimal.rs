//! Minimal family/workload-oriented integration example for a downstream Burn app.
//!
//! The forward-facing concepts are the workload, the project family, and the node builder.
//! The legacy runtime traits still exist under `burn_p2p::compat` and are implemented below
//! only because the current workload adapter layer still depends on them internally.

use std::{collections::BTreeMap, path::PathBuf};

use burn_p2p::{
    AssignmentLease, CachedMicroShard, CapabilityEstimate, ClientReleaseManifest, ContentId,
    DatasetManifest, DatasetRegistration, DatasetSizing, DatasetView, EvalSplit, FsArtifactStore,
    GenesisSpec, MetricReport, MetricValue, NodeBuilder, P2pWorkload, PatchOutcome, PatchSupport,
    PeerRole, Precision, ProjectBackend, ProjectFamilyId, RevisionId, RuntimePatch,
    SingleWorkloadProjectFamily, StorageConfig, SupportedWorkload, TrainError, UpstreamAdapter,
    WindowCtx, WindowReport, WorkloadId,
    compat::{P2pProject, RuntimeProject},
};
use chrono::Utc;
use semver::Version;

#[derive(Clone, Debug)]
struct ExampleBackend;

impl ProjectBackend for ExampleBackend {
    type Device = String;
}

#[derive(Clone, Debug)]
struct ExampleWorkload {
    dataset_root: PathBuf,
}

impl P2pProject<ExampleBackend> for ExampleWorkload {
    type Model = f32;
    type Batch = f32;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, _device: &String) -> Self::Model {
        0.0
    }

    fn benchmark(&self, _model: &Self::Model, _device: &String) -> CapabilityEstimate {
        CapabilityEstimate {
            preferred_backends: vec!["cpu".into()],
            work_units_per_second: 16.0,
            target_window_seconds: 1,
        }
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<String, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        let delta = ctx.batches.iter().copied().sum::<f32>();
        ctx.model += delta;
        Ok(WindowReport {
            contribution: None,
            stats: BTreeMap::from([
                ("delta".into(), MetricValue::Float(delta as f64)),
                ("model".into(), MetricValue::Float(ctx.model as f64)),
            ]),
            completed_at: Utc::now(),
        })
    }

    fn evaluate(&self, model: &Self::Model, _split: EvalSplit) -> MetricReport {
        MetricReport {
            metrics: BTreeMap::from([("model".into(), MetricValue::Float(*model as f64))]),
            captured_at: Utc::now(),
        }
    }

    fn apply_patch(&mut self, _patch: &RuntimePatch) -> PatchOutcome {
        PatchOutcome::Rejected("example workload does not support runtime patches".into())
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        PatchSupport {
            hot: false,
            warm: false,
            cold: false,
        }
    }
}

impl RuntimeProject<ExampleBackend> for ExampleWorkload {
    fn runtime_device(&self) -> String {
        "cpu".into()
    }

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        Ok(DatasetRegistration {
            manifest: DatasetManifest {
                dataset_id: burn_p2p::DatasetId::new("minimal-dataset"),
                source_uri: self.dataset_root.display().to_string(),
                format: "microshards".into(),
                manifest_hash: ContentId::new("minimal-manifest"),
                metadata: BTreeMap::new(),
            },
            view: DatasetView {
                dataset_view_id: burn_p2p::DatasetViewId::new("minimal-view"),
                dataset_id: burn_p2p::DatasetId::new("minimal-dataset"),
                preprocessing_hash: ContentId::new("minimal-preprocess"),
                tokenizer_hash: None,
                manifest_hash: ContentId::new("minimal-manifest"),
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
        Ok(
            burn_p2p::MicroShardPlanner::new(burn_p2p::MicroShardPlannerConfig {
                target_microshard_bytes: 1024,
                min_microshards: 1,
                max_microshards: 4,
            })?
            .plan(
                &registration.view,
                DatasetSizing {
                    total_examples: 128,
                    total_tokens: 2048,
                    total_bytes: 4096,
                },
            )?,
        )
    }

    fn load_batches(
        &self,
        _lease: &AssignmentLease,
        _cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        Ok(vec![1.0, 2.0, 3.0])
    }

    fn load_model_artifact(
        &self,
        model: Self::Model,
        _descriptor: &burn_p2p::ArtifactDescriptor,
        _store: &FsArtifactStore,
        _device: &String,
    ) -> anyhow::Result<Self::Model> {
        Ok(model)
    }

    fn materialize_model_artifact(
        &self,
        _model: &Self::Model,
        _artifact_kind: burn_p2p::ArtifactKind,
        head_id: burn_p2p::HeadId,
        _base_head_id: Option<burn_p2p::HeadId>,
        _store: &FsArtifactStore,
    ) -> anyhow::Result<burn_p2p::ArtifactDescriptor> {
        Ok(burn_p2p::ArtifactDescriptor {
            artifact_id: burn_p2p::ArtifactId::new("minimal-artifact"),
            kind: burn_p2p::ArtifactKind::ServeHead,
            head_id: Some(head_id),
            base_head_id: None,
            precision: Precision::Fp32,
            model_schema_hash: ContentId::new("minimal-model-schema"),
            record_format: "noop".into(),
            bytes_len: 0,
            chunks: Vec::new(),
            root_hash: ContentId::new("minimal-root"),
        })
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }
}

impl P2pWorkload<ExampleBackend> for ExampleWorkload {
    fn supported_workload(&self) -> SupportedWorkload {
        SupportedWorkload {
            workload_id: WorkloadId::new("minimal-workload"),
            workload_name: "Minimal Example".into(),
            model_program_hash: ContentId::new("minimal-model-program"),
            checkpoint_format_hash: ContentId::new("minimal-checkpoint"),
            supported_revision_family: ContentId::new("minimal-revision-family"),
            resource_class: "cpu-dev".into(),
        }
    }

    fn model_schema_hash(&self) -> ContentId {
        ContentId::new("minimal-model-schema")
    }
}

fn main() -> anyhow::Result<()> {
    let workload = ExampleWorkload {
        dataset_root: std::env::temp_dir().join("burn-p2p-minimal-example"),
    };
    let supported_workload = workload.supported_workload();
    let family = SingleWorkloadProjectFamily::<ExampleBackend, _>::new(
        ClientReleaseManifest {
            project_family_id: ProjectFamilyId::new("minimal-family"),
            release_train_hash: ContentId::new("release-train-1"),
            target_artifact_id: "native-linux-x86_64".into(),
            target_artifact_hash: ContentId::new("target-native-linux"),
            target_platform: burn_p2p::ClientPlatform::Native,
            app_semver: Version::new(0, 1, 0),
            git_commit: "example".into(),
            cargo_lock_hash: ContentId::new("cargo-lock"),
            burn_version_string: "example".into(),
            enabled_features_hash: ContentId::new("features"),
            protocol_major: 1,
            supported_workloads: vec![supported_workload.clone()],
            built_at: Utc::now(),
        },
        workload,
    )?;

    let _selected = burn_p2p::SelectedWorkloadProject::new(
        family.clone(),
        supported_workload.workload_id.clone(),
    )?;

    let _builder = NodeBuilder::new(family)
        .with_mainnet(GenesisSpec {
            network_id: burn_p2p::NetworkId::new("minimal-network"),
            protocol_version: Version::new(0, 1, 0),
            display_name: "Minimal Example Network".into(),
            created_at: Utc::now(),
            metadata: BTreeMap::from([("profile".into(), "dev".into())]),
        })
        .with_roles(burn_p2p::RoleSet::new([PeerRole::TrainerCpu]))
        .with_storage(StorageConfig::new(
            std::env::temp_dir().join("burn-p2p-minimal-node"),
        ));

    println!(
        "configured family={} workload={} revision={}",
        ProjectFamilyId::new("minimal-family").as_str(),
        supported_workload.workload_id.as_str(),
        RevisionId::new("revision-example").as_str()
    );
    Ok(())
}
