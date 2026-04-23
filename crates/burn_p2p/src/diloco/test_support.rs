use std::{collections::BTreeMap, io::Cursor};

use burn_p2p_workload::P2pWorkload;
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactBuildSpec, CapabilityEstimate, ChunkingScheme, ContentId, DatasetId, DatasetManifest,
    DatasetRegistration, DatasetView, DatasetViewId, DiLoCoInnerLoopReport, DiLoCoWorkload,
    EvalSplit, FlattenedTensorPack, FsArtifactStore, HeadId, MetricReport, MetricValue,
    OuterOptimizerPolicy, PatchOutcome, PatchSupport, Precision, StateBlob, SupportedWorkload,
    TrainError, UpstreamAdapter, WindowCtx, WindowReport, WorkloadId,
};

#[derive(Clone, Debug)]
pub(super) struct ScalarDiLoCoTestWorkload {
    pub(super) inner_lr: f32,
    work_units_per_second: f64,
    workload_id: &'static str,
    workload_name: &'static str,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MomentumState {
    velocity: f32,
}

impl ScalarDiLoCoTestWorkload {
    pub(super) fn reference(inner_lr: f32) -> Self {
        Self {
            inner_lr,
            work_units_per_second: 16.0,
            workload_id: "scalar-diloco",
            workload_name: "Scalar DiLoCo",
        }
    }

    pub(super) fn network(inner_lr: f32) -> Self {
        Self {
            inner_lr,
            work_units_per_second: 32.0,
            workload_id: "scalar-diloco-network",
            workload_name: "Scalar DiLoCo Network",
        }
    }
}

impl P2pWorkload for ScalarDiLoCoTestWorkload {
    type Device = ();
    type Model = f32;
    type Batch = f32;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, _device: &Self::Device) -> Self::Model {
        0.0
    }

    fn benchmark(&self, _model: &Self::Model, _device: &Self::Device) -> CapabilityEstimate {
        CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: self.work_units_per_second,
            target_window_seconds: 1,
        }
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<Self::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        let delta = ctx.batches.iter().copied().sum::<f32>() * self.inner_lr;
        ctx.model += delta;
        Ok(WindowReport {
            contribution: None,
            stats: BTreeMap::from([("delta".into(), MetricValue::Float(delta as f64))]),
            completed_at: chrono::Utc::now(),
        })
    }

    fn evaluate(&self, model: &Self::Model, _split: EvalSplit) -> MetricReport {
        MetricReport {
            metrics: BTreeMap::from([("model".into(), MetricValue::Float(*model as f64))]),
            captured_at: chrono::Utc::now(),
        }
    }

    fn apply_patch(&mut self, _patch: &crate::RuntimePatch) -> PatchOutcome {
        PatchOutcome::Rejected("unsupported".into())
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        PatchSupport::default()
    }

    fn runtime_device(&self) -> Self::Device {}

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        Ok(DatasetRegistration {
            manifest: DatasetManifest {
                dataset_id: DatasetId::new("scalar-dataset"),
                source_uri: "memory://scalar-dataset".into(),
                format: "synthetic".into(),
                manifest_hash: ContentId::new("scalar-manifest"),
                metadata: BTreeMap::new(),
            },
            view: DatasetView {
                dataset_view_id: DatasetViewId::new("scalar-view"),
                dataset_id: DatasetId::new("scalar-dataset"),
                preprocessing_hash: ContentId::new("scalar-preprocess"),
                tokenizer_hash: None,
                manifest_hash: ContentId::new("scalar-manifest"),
                metadata: BTreeMap::new(),
            },
            upstream: UpstreamAdapter::Local {
                root: "/tmp".into(),
            },
        })
    }

    fn microshard_plan(
        &self,
        _registration: &DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        unimplemented!("DiLoCo tests inject batches directly")
    }

    fn load_batches(
        &self,
        _lease: &crate::AssignmentLease,
        _cached_microshards: &[crate::CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        unimplemented!("DiLoCo tests inject batches directly")
    }

    fn load_model_artifact(
        &self,
        _model: Self::Model,
        descriptor: &crate::ArtifactDescriptor,
        store: &FsArtifactStore,
        _device: &Self::Device,
    ) -> anyhow::Result<Self::Model> {
        Ok(serde_json::from_slice(
            &store.materialize_artifact_bytes(descriptor)?,
        )?)
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: crate::ArtifactKind,
        head_id: HeadId,
        base_head_id: Option<HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<crate::ArtifactDescriptor> {
        let bytes = serde_json::to_vec(model)?;
        let mut spec = ArtifactBuildSpec::new(
            artifact_kind,
            Precision::Fp32,
            ContentId::new("scalar-schema"),
            "scalar-json",
        )
        .with_head(head_id);
        if let Some(base_head_id) = base_head_id {
            spec = spec.with_base_head(base_head_id);
        }
        Ok(store.store_artifact_reader(&spec, Cursor::new(bytes), ChunkingScheme::new(8)?)?)
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }

    fn supported_workload(&self) -> SupportedWorkload {
        SupportedWorkload {
            workload_id: WorkloadId::new(self.workload_id),
            workload_name: self.workload_name.into(),
            model_program_hash: ContentId::new("scalar-program"),
            checkpoint_format_hash: ContentId::new("scalar-format"),
            supported_revision_family: ContentId::new("scalar-family"),
            resource_class: "cpu".into(),
        }
    }

    fn model_schema_hash(&self) -> ContentId {
        ContentId::new("scalar-schema")
    }
}

impl DiLoCoWorkload for ScalarDiLoCoTestWorkload {
    fn export_parameter_pack(&self, model: &Self::Model) -> anyhow::Result<FlattenedTensorPack> {
        Ok(FlattenedTensorPack::new(
            self.model_schema_hash(),
            ContentId::new("scalar-layout"),
            vec![*model],
        ))
    }

    fn import_parameter_pack(
        &self,
        _device: &Self::Device,
        pack: &FlattenedTensorPack,
    ) -> anyhow::Result<Self::Model> {
        Ok(*pack.values.first().unwrap_or(&0.0))
    }

    fn run_inner_steps(
        &self,
        model: &Self::Model,
        batches: &[Self::Batch],
        num_inner_steps: u32,
        inner_optimizer_state: Option<&StateBlob>,
    ) -> Result<DiLoCoInnerLoopReport, TrainError> {
        let mut local = *model;
        for batch in batches
            .iter()
            .copied()
            .cycle()
            .take(num_inner_steps as usize)
        {
            local += batch * self.inner_lr;
        }
        Ok(DiLoCoInnerLoopReport {
            local_parameters: self
                .export_parameter_pack(&local)
                .map_err(|error| TrainError::new(error.to_string()))?,
            inner_optimizer_state: inner_optimizer_state.cloned(),
            steps_completed: num_inner_steps,
            metrics: BTreeMap::from([("local_model".into(), MetricValue::Float(local as f64))]),
        })
    }

    fn initialize_outer_optimizer_state(
        &self,
        _model: &Self::Model,
        _policy: &OuterOptimizerPolicy,
    ) -> anyhow::Result<StateBlob> {
        StateBlob::try_new(
            "application/json",
            serde_json::to_vec(&MomentumState { velocity: 0.0 })?,
        )
        .map_err(anyhow::Error::from)
    }

    fn apply_aggregated_outer_update(
        &self,
        base: &FlattenedTensorPack,
        aggregate: &FlattenedTensorPack,
        outer_optimizer_state: &StateBlob,
        policy: &OuterOptimizerPolicy,
    ) -> anyhow::Result<(FlattenedTensorPack, StateBlob)> {
        let mut state: MomentumState = serde_json::from_slice(&outer_optimizer_state.bytes)?;
        let momentum = policy.momentum().unwrap_or(0.0) as f32;
        let grad = aggregate.values[0];
        state.velocity = momentum * state.velocity + grad;
        let step = match policy {
            OuterOptimizerPolicy::Sgd { nesterov: true, .. } => momentum * state.velocity + grad,
            _ => state.velocity,
        };
        let next = base.values[0] - ((policy.learning_rate() as f32) * step);
        Ok((
            FlattenedTensorPack::new(
                base.model_schema_hash.clone(),
                base.layout_hash.clone(),
                vec![next],
            ),
            StateBlob::try_new("application/json", serde_json::to_vec(&state)?)?,
        ))
    }
}
