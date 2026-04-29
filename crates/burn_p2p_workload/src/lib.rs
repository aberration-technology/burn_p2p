//! Backend-facing workload and data-pipeline seam for `burn_p2p`.
#![forbid(unsafe_code)]

mod backend;
mod data_pipeline;
mod diloco;
mod directory_metadata;
mod execution;

use std::{collections::BTreeMap, path::PathBuf};

use burn_p2p_checkpoint::FsArtifactStore;
use burn_p2p_core::{
    ArtifactDescriptor, ArtifactKind, AssignmentLease, CapabilityEstimate, ContentId, HeadId,
    MergePolicy, MetricValue, RevisionManifest, SupportedWorkload,
};
use burn_p2p_dataloader::{CachedMicroShard, DatasetRegistration, MicroShardPlan, UpstreamAdapter};
use burn_p2p_experiment::{PatchSupport, RuntimePatch};

pub use backend::{
    ContinuousTrainerPolicy, EvalSplit, MergeModelCandidate, MetricReport, PatchOutcome,
    ReducerOutcome, TrainError, TrainerCanonicalReconcileStrategy, TrainingWindowOutcome,
    TrainingWindowTiming, ValidationCoordinationState, ValidationDriveOutcome, ValidationOutcome,
    WindowCtx, WindowReport,
};
pub use data_pipeline::{
    GeneratedWorkloadInputDescriptor, GeneratedWorkloadInputProvider, LeaseDataPipeline,
    LeaseDataPipelineDescriptor, LeaseDataPipelineKind, WorkloadInputSource,
};
pub use diloco::{DiLoCoInnerLoopReport, DiLoCoWorkload};
pub use directory_metadata::{
    DirectoryMetadataAttachment, find_matching_directory_entry,
    find_matching_directory_entry_with_predicate,
};
pub use execution::{
    WorkloadExecutionStage, WorkloadTrainingArtifact, WorkloadTrainingArtifactChunk,
    WorkloadTrainingBudget, WorkloadTrainingContribution, WorkloadTrainingLease,
    WorkloadTrainingPlan, WorkloadTrainingProgress, WorkloadTrainingResult, WorkloadValidationPlan,
    WorkloadValidationProgress, WorkloadValidationResult,
};

/// Returns the local filesystem root for one dataset registration when the
/// dataset is backed by a local upstream.
pub fn local_upstream_root(registration: &DatasetRegistration) -> Option<PathBuf> {
    match &registration.upstream {
        UpstreamAdapter::Local { root } => Some(PathBuf::from(root)),
        _ => None,
    }
}

/// Resolves the local filesystem root for one lease-data pipeline when the
/// underlying dataset registration is backed by a local upstream.
pub fn local_upstream_root_for_pipeline<D, B>(
    pipeline: &LeaseDataPipeline<D, B>,
) -> anyhow::Result<Option<PathBuf>> {
    Ok(local_upstream_root(&pipeline.dataset_registration()?))
}

fn numeric_metric(metrics: &BTreeMap<String, MetricValue>, key: &str) -> Option<f64> {
    match metrics.get(key) {
        Some(MetricValue::Integer(value)) => Some(*value as f64),
        Some(MetricValue::Float(value)) => Some(*value),
        Some(MetricValue::Bool(_)) | Some(MetricValue::Text(_)) | None => None,
    }
}

/// Infers a standard contribution weight from commonly emitted processed-work metrics.
///
/// The current preference order is:
///
/// - `tokens_processed`
/// - `accepted_tokens_or_samples`
/// - `examples_processed`
/// - `sample_count`
/// - `samples`
/// - `batch_count`
pub fn standard_contribution_weight(metrics: &BTreeMap<String, MetricValue>) -> Option<f64> {
    for key in [
        "tokens_processed",
        "accepted_tokens_or_samples",
        "examples_processed",
        "sample_count",
        "samples",
        "batch_count",
    ] {
        let Some(value) = numeric_metric(metrics, key) else {
            continue;
        };
        if value.is_finite() && value > 0.0 {
            return Some(value);
        }
    }

    None
}

/// Defines one executable workload inside a project family.
pub trait P2pWorkload {
    /// Defines the device alias.
    type Device;
    /// Defines the model alias.
    type Model;
    /// Defines the batch alias.
    type Batch;
    /// Defines the window stats alias.
    type WindowStats;

    /// Initializes a model instance for the provided backend device.
    fn init_model(&self, device: &Self::Device) -> Self::Model;

    /// Benchmarks the workload and reports the runtime capability estimate.
    fn benchmark(&self, model: &Self::Model, device: &Self::Device) -> CapabilityEstimate;

    /// Runs one training window for the leased batches.
    fn train_window(
        &self,
        ctx: &mut WindowCtx<Self::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError>;

    /// Evaluates the model on the requested dataset split.
    fn evaluate(&self, model: &Self::Model, split: EvalSplit) -> MetricReport;

    /// Applies a runtime patch to the workload implementation.
    fn apply_patch(&mut self, patch: &RuntimePatch) -> PatchOutcome;

    /// Returns the patch classes accepted by the workload implementation.
    fn supported_patch_classes(&self) -> PatchSupport;

    /// Returns the runtime device used by the workload.
    fn runtime_device(&self) -> Self::Device;

    /// Returns the dataset registration used to plan microshards.
    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration>;

    /// Builds the microshard plan for the registered dataset.
    fn microshard_plan(&self, registration: &DatasetRegistration)
    -> anyhow::Result<MicroShardPlan>;

    /// Loads training batches for the lease from cached microshards.
    fn load_batches(
        &self,
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>>;

    /// Loads a model artifact from the artifact store into the runtime model representation.
    fn load_model_artifact(
        &self,
        model: Self::Model,
        descriptor: &ArtifactDescriptor,
        store: &FsArtifactStore,
        device: &Self::Device,
    ) -> anyhow::Result<Self::Model>;

    /// Materializes a model artifact into the checkpoint store.
    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: ArtifactKind,
        head_id: HeadId,
        base_head_id: Option<HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor>;

    /// Returns receipt metrics for a completed training window.
    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue>;

    /// Returns the contribution weight used for receipt scoring.
    fn contribution_weight(&self, _report: &WindowReport<Self::WindowStats>) -> f64 {
        1.0
    }

    /// Reconciles a speculative local trainer model with a newly visible canonical model.
    fn reconcile_canonical_model(
        &self,
        _local_model: &Self::Model,
        canonical_model: Self::Model,
        _strategy: TrainerCanonicalReconcileStrategy,
    ) -> anyhow::Result<Self::Model> {
        Ok(canonical_model)
    }

    /// Optionally merges candidate models into one merged model.
    fn merge_candidate_models(
        &self,
        _base_model: &Self::Model,
        _candidates: &[MergeModelCandidate<'_, Self::Model>],
        _policy: MergePolicy,
    ) -> anyhow::Result<Option<Self::Model>> {
        Ok(None)
    }

    /// Optionally applies single-root EMA after merge selection.
    fn apply_single_root_ema(
        &self,
        _base_model: &Self::Model,
        merged_model: Self::Model,
        _policy: MergePolicy,
    ) -> anyhow::Result<Self::Model> {
        Ok(merged_model)
    }

    /// Returns the supported workload manifest.
    fn supported_workload(&self) -> SupportedWorkload;

    /// Returns the stable model schema hash.
    fn model_schema_hash(&self) -> ContentId;

    /// Returns the workload id.
    fn workload_id(&self) -> burn_p2p_core::WorkloadId {
        self.supported_workload().workload_id
    }

    /// Switches the live runtime to one other compiled workload when supported.
    ///
    /// The default implementation only succeeds when the requested workload is
    /// already active. Family-style runtimes can override this to hot-swap the
    /// selected workload during an experiment lifecycle transition.
    fn switch_runtime_workload(
        &mut self,
        workload_id: &burn_p2p_core::WorkloadId,
    ) -> anyhow::Result<()> {
        if self.workload_id() != *workload_id {
            anyhow::bail!(
                "runtime workload switch to {} is unsupported for {}",
                workload_id.as_str(),
                self.workload_id().as_str(),
            );
        }

        Ok(())
    }

    /// Returns the checkpoint format hash.
    fn checkpoint_format_hash(&self) -> ContentId {
        self.supported_workload().checkpoint_format_hash
    }

    /// Verifies that the runtime workload matches one revision manifest.
    fn verify_revision(&self, revision: &RevisionManifest) -> anyhow::Result<()> {
        let workload = self.supported_workload();

        if revision.workload_id != workload.workload_id {
            anyhow::bail!(
                "revision {} targets workload {}, but this workload is {}",
                revision.revision_id.as_str(),
                revision.workload_id.as_str(),
                workload.workload_id.as_str(),
            );
        }

        let model_schema_hash = self.model_schema_hash();
        if revision.model_schema_hash != model_schema_hash {
            anyhow::bail!(
                "revision {} requires model schema {}, but workload {} exposes {}",
                revision.revision_id.as_str(),
                revision.model_schema_hash.as_str(),
                workload.workload_id.as_str(),
                model_schema_hash.as_str(),
            );
        }

        if revision.checkpoint_format_hash != workload.checkpoint_format_hash {
            anyhow::bail!(
                "revision {} requires checkpoint format {}, but workload {} exposes {}",
                revision.revision_id.as_str(),
                revision.checkpoint_format_hash.as_str(),
                workload.workload_id.as_str(),
                workload.checkpoint_format_hash.as_str(),
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use burn_p2p_core::MetricValue;

    use crate::standard_contribution_weight;

    #[test]
    fn standard_contribution_weight_prefers_tokens_when_present() {
        let metrics = BTreeMap::from([
            ("examples_processed".into(), MetricValue::Integer(64)),
            ("tokens_processed".into(), MetricValue::Integer(2048)),
        ]);

        assert_eq!(standard_contribution_weight(&metrics), Some(2048.0));
    }

    #[test]
    fn standard_contribution_weight_falls_back_to_batch_count() {
        let metrics = BTreeMap::from([("batch_count".into(), MetricValue::Integer(8))]);

        assert_eq!(standard_contribution_weight(&metrics), Some(8.0));
    }
}
