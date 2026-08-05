//! Backend-facing workload and data-pipeline seam for `burn_p2p`.
#![forbid(unsafe_code)]

mod backend;
mod compact_update;
mod data_pipeline;
mod diloco;
mod directory_metadata;
mod execution;
mod window_executor;

use std::{collections::BTreeMap, path::PathBuf};

use burn_p2p_checkpoint::FsArtifactStore;
use burn_p2p_core::{
    ArtifactDescriptor, ArtifactKind, AssignmentLease, CapabilityEstimate, ContentId,
    GenesisMaterialization, HeadId, LeaseId, MergePolicy, MetricValue, PeerId, RevisionId,
    RevisionManifest, SupportedWorkload, TrainingContractManifest, ValidatedUpdateEvidence,
    WindowId, WorkloadUpdateEnvelope,
};
use burn_p2p_dataloader::{CachedMicroShard, DatasetRegistration, MicroShardPlan, UpstreamAdapter};
use burn_p2p_experiment::{PatchSupport, RuntimePatch};

pub use backend::{
    ContinuousTrainerPolicy, EvalSplit, MergeModelCandidate, MetricReport, PatchOutcome,
    ReducerOutcome, TrainError, TrainerCanonicalReconcileStrategy, TrainingWindowOutcome,
    TrainingWindowTiming, ValidationCoordinationState, ValidationDriveOutcome, ValidationOutcome,
    WindowCtx, WindowReport,
};
pub use compact_update::{
    CompactUpdateReconstructor, ContextSparseDeltaReconstructor, MAX_COMPACT_UPDATE_BYTES,
    MutableSubsetParameterReconstructor, SeededSubspaceReconstructor, ValidatedCompactUpdate,
    average_context_sparse_deltas, average_mutable_subset_parameters, average_subspace_updates,
    decode_compact_update, decode_context_sparse_update, encode_compact_update,
    reconstructed_update_norm_stats,
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
pub use window_executor::{
    WindowExecutorLifecycle, WindowExecutorSession, WindowExecutorSessionError,
    WorkloadExecutionObserver, WorkloadWindowExecutor,
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

/// Exact validator-owned inputs for replaying one typed workload update.
pub struct WorkloadUpdateReplayContext<'a> {
    /// Authenticated assignment lease that authorized the contribution.
    pub lease: &'a AssignmentLease,
    /// Content-verified shards selected by that lease.
    pub cached_microshards: &'a [CachedMicroShard],
    /// Validator producing the evidence.
    pub validator_peer_id: &'a PeerId,
}

/// Contract-bound inputs for independently validating one typed workload update.
pub struct WorkloadUpdateValidationContext<'a, D> {
    /// Content-addressed descriptor for the typed update artifact.
    pub descriptor: &'a ArtifactDescriptor,
    /// Typed update envelope announced by the contributing peer.
    pub update: &'a WorkloadUpdateEnvelope,
    /// Authority-signed training contract governing reconstruction and replay.
    pub contract: &'a TrainingContractManifest,
    /// Artifact store containing the content-verified update payload.
    pub store: &'a FsArtifactStore,
    /// Backend device used for deterministic reconstruction and replay.
    pub device: &'a D,
    /// Validator-owned lease, data, and identity inputs.
    pub replay: WorkloadUpdateReplayContext<'a>,
}

/// Inputs for materializing a deterministic-reconstruction genesis artifact.
pub struct GenesisArtifactMaterializationContext<'a, M> {
    /// Locally initialized model whose mutable state must be serialized.
    pub model: &'a M,
    /// Canonical genesis head identity.
    pub head_id: HeadId,
    /// Authority-bound training contract identity.
    pub training_contract_id: &'a ContentId,
    /// Authority-bound training semantics.
    pub contract: &'a TrainingContractManifest,
    /// Deterministic reconstruction contract.
    pub materialization: &'a GenesisMaterialization,
    /// Destination artifact store.
    pub store: &'a FsArtifactStore,
}

/// Inputs for loading a deterministic-reconstruction genesis artifact.
pub struct GenesisArtifactLoadContext<'a, D> {
    /// Authority-bound genesis artifact.
    pub descriptor: &'a ArtifactDescriptor,
    /// Authority-bound training contract identity.
    pub training_contract_id: &'a ContentId,
    /// Authority-bound training semantics.
    pub contract: &'a TrainingContractManifest,
    /// Deterministic reconstruction contract.
    pub materialization: &'a GenesisMaterialization,
    /// Source artifact store.
    pub store: &'a FsArtifactStore,
    /// Runtime device receiving the reconstructed model.
    pub device: &'a D,
}

/// Candidate model plus validator-owned reconstruction and replay evidence.
pub struct ValidatedWorkloadUpdate<M> {
    /// Model reconstructed from the canonical base and typed update.
    pub model: M,
    /// Evidence computed locally by the validator.
    pub evidence: ValidatedUpdateEvidence,
}

/// Inputs for materializing one contract-bound update after a local training window.
pub struct WorkloadUpdateMaterializationContext<'a, D, M> {
    /// Canonical model head used at the beginning of the window.
    pub base_model: &'a M,
    /// Locally trained model at the end of the window.
    pub trained_model: &'a M,
    /// Content identity of the governing training contract.
    pub training_contract_id: &'a ContentId,
    /// Governing training contract.
    pub contract: &'a TrainingContractManifest,
    /// Revision producing the update.
    pub revision_id: &'a RevisionId,
    /// Canonical base head identity.
    pub base_head_id: &'a HeadId,
    /// Logical candidate head identity.
    pub candidate_head_id: &'a HeadId,
    /// Window that authorized the update.
    pub window_id: WindowId,
    /// Lease that authorized the update.
    pub lease_id: &'a LeaseId,
    /// Content-addressed artifact store.
    pub store: &'a FsArtifactStore,
    /// Runtime backend device.
    pub device: &'a D,
}

/// One materialized compact artifact plus its contract-bound wire envelope.
pub struct MaterializedWorkloadUpdate {
    /// Compact update artifact descriptor.
    pub artifact: ArtifactDescriptor,
    /// Metadata required for validator reconstruction.
    pub envelope: WorkloadUpdateEnvelope,
}

/// Defines one executable workload inside a project family.
pub trait P2pWorkload {
    /// Defines the device alias.
    type Device;
    /// Defines the model alias.
    type Model: Clone;
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

    /// Loads genesis state, regenerating deterministic immutable parameters when declared.
    fn load_genesis_artifact(
        &self,
        model: Self::Model,
        context: GenesisArtifactLoadContext<'_, Self::Device>,
    ) -> anyhow::Result<Self::Model> {
        match context.materialization {
            GenesisMaterialization::FullArtifact => {
                self.load_model_artifact(model, context.descriptor, context.store, context.device)
            }
            GenesisMaterialization::DeterministicReconstruction { .. } => anyhow::bail!(
                "workload {} does not implement deterministic genesis reconstruction",
                self.workload_id().as_str(),
            ),
        }
    }

    /// Computes the canonical tensor digest for a decoded model.
    ///
    /// Workloads participating in authority-signed revisions must implement
    /// this hook. The default fails closed rather than trusting an artifact
    /// envelope without checking the decoded weights.
    fn model_tensor_digest(&self, _model: &Self::Model) -> anyhow::Result<ContentId> {
        anyhow::bail!(
            "workload {} does not implement canonical model tensor digests",
            self.workload_id().as_str(),
        )
    }

    /// Materializes a compact update when the training contract does not use full-model payloads.
    ///
    /// The default fails closed by returning no update. Runtimes require a
    /// concrete result for every non-`FullModel` contract.
    fn materialize_workload_update(
        &self,
        _context: WorkloadUpdateMaterializationContext<'_, Self::Device, Self::Model>,
    ) -> anyhow::Result<Option<MaterializedWorkloadUpdate>> {
        Ok(None)
    }

    /// Reconstructs one contract-bound typed update from its canonical base model.
    ///
    /// The runtime calls this only after validating the envelope against the
    /// authority-signed training contract and matching its artifact descriptor.
    fn apply_workload_update(
        &self,
        _base_model: Self::Model,
        descriptor: &ArtifactDescriptor,
        _update: &WorkloadUpdateEnvelope,
        _contract: &TrainingContractManifest,
        _store: &FsArtifactStore,
        _device: &Self::Device,
    ) -> anyhow::Result<Self::Model> {
        anyhow::bail!(
            "workload {} does not support typed update artifact {}",
            self.workload_id().as_str(),
            descriptor.artifact_id.as_str(),
        )
    }

    /// Validates and applies one typed update using authenticated replay inputs.
    ///
    /// Workloads with replay-sensitive codecs should override this method.
    /// The default reconstructs the model but deliberately leaves replay
    /// unverified, causing replay-required codecs to fail candidate admission.
    fn validate_and_apply_workload_update(
        &self,
        base_model: Self::Model,
        context: WorkloadUpdateValidationContext<'_, Self::Device>,
    ) -> anyhow::Result<ValidatedWorkloadUpdate<Self::Model>> {
        let WorkloadUpdateValidationContext {
            descriptor,
            update,
            contract,
            store,
            device,
            replay,
        } = context;
        let model =
            self.apply_workload_update(base_model, descriptor, update, contract, store, device)?;
        Ok(ValidatedWorkloadUpdate {
            model,
            evidence: ValidatedUpdateEvidence {
                update_envelope_id: ContentId::derive(update)?,
                norm_stats: None,
                feature_sketch: None,
                reconstruction_verified: true,
                replay_verified: !contract.update_codec.requires_independent_replay(),
                replay_stats: None,
                validator_peer_id: replay.validator_peer_id.clone(),
                validated_at: chrono::Utc::now(),
            },
        })
    }

    /// Materializes a model artifact into the checkpoint store.
    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: ArtifactKind,
        head_id: HeadId,
        base_head_id: Option<HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor>;

    /// Materializes genesis state according to its reconstruction contract.
    fn materialize_genesis_artifact(
        &self,
        context: GenesisArtifactMaterializationContext<'_, Self::Model>,
    ) -> anyhow::Result<ArtifactDescriptor> {
        match context.materialization {
            GenesisMaterialization::FullArtifact => self.materialize_model_artifact(
                context.model,
                ArtifactKind::FullHead,
                context.head_id,
                None,
                context.store,
            ),
            GenesisMaterialization::DeterministicReconstruction { .. } => anyhow::bail!(
                "workload {} does not implement deterministic genesis materialization",
                self.workload_id().as_str(),
            ),
        }
    }

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
