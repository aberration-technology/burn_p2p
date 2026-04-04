//! Legacy compatibility traits for pre-family `burn_p2p` integrations.
//!
//! New downstream integrations should prefer [`crate::P2pWorkload`] together with
//! [`crate::P2pProjectFamily`] and [`crate::NodeBuilder`]. These traits remain available so
//! existing embeddings can keep compiling while they migrate onto the family/workload model.

use std::collections::BTreeMap;

use crate::{
    ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard, CapabilityEstimate,
    DatasetRegistration, EvalSplit, FsArtifactStore, HeadId, MergeModelCandidate, MergePolicy,
    MetricReport, MetricValue, PatchOutcome, PatchSupport, ProjectBackend, RuntimePatch, WindowCtx,
    WindowReport,
};

/// Legacy compatibility trait for the pre-family runtime API.
///
/// New downstream integrations should implement [`crate::P2pWorkload`] and expose workloads
/// through [`crate::P2pProjectFamily`] instead of depending on this trait directly.
pub trait P2pProject<B: ProjectBackend> {
    /// Defines the model alias.
    type Model;
    /// Defines the batch alias.
    type Batch;
    /// Defines the window stats alias.
    type WindowStats;

    /// Initializes a model instance for the provided backend device.
    fn init_model(&self, device: &B::Device) -> Self::Model;
    /// Benchmarks the workload and reports the runtime capability estimate.
    fn benchmark(&self, model: &Self::Model, device: &B::Device) -> CapabilityEstimate;
    /// Runs one training window for the leased batches.
    fn train_window(
        &self,
        ctx: &mut WindowCtx<B::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, crate::TrainError>;
    /// Evaluates the model on the requested dataset split.
    fn evaluate(&self, model: &Self::Model, split: EvalSplit) -> MetricReport;
    /// Applies a runtime patch to the workload implementation.
    fn apply_patch(&mut self, patch: &RuntimePatch) -> PatchOutcome;
    /// Returns the patch classes accepted by the workload implementation.
    fn supported_patch_classes(&self) -> PatchSupport;
}

/// Deprecated alias retained for compatibility with older examples and downstream code.
#[deprecated(note = "Use burn_p2p::compat::P2pProject or the family/workload API instead.")]
pub use P2pProject as Project;

/// Legacy compatibility trait for the pre-family runtime API.
///
/// New downstream integrations should implement [`crate::P2pWorkload`] and use
/// [`crate::SingleWorkloadProjectFamily`] or a custom [`crate::P2pProjectFamily`]
/// implementation as the public integration surface.
pub trait RuntimeProject<B: ProjectBackend>: P2pProject<B> {
    /// Returns the runtime device used by the workload.
    fn runtime_device(&self) -> B::Device;

    /// Returns the dataset registration used to plan microshards.
    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration>;

    /// Builds the microshard plan for the registered dataset.
    fn microshard_plan(
        &self,
        registration: &DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan>;

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
        device: &B::Device,
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
}
