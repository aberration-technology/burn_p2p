use super::*;
use crate::candidate_screening::{
    build_validation_canary_report_against_baseline_with_policy,
    build_validation_canary_report_with_policy,
};
use std::path::PathBuf;

mod discovery;
mod model;
#[cfg(test)]
mod tests;

pub(crate) use discovery::collect_validation_candidate_heads;
pub(crate) use model::{
    fallback_best_candidate_index, load_validation_base_model, load_validation_candidate_model,
    select_reducer_authority_head, select_validation_head,
};

pub(crate) struct ValidationCandidate<M> {
    pub peer_id: PeerId,
    pub head: HeadDescriptor,
    pub update: UpdateAnnounce,
    pub evaluation: MetricReport,
    pub canary_report: Option<CanaryEvalReport>,
    pub sample_weight: f64,
    pub quality_weight: f64,
    pub model: M,
    pub update_evidence: Option<ValidatedUpdateEvidence>,
}

#[derive(Clone, Copy)]
pub(crate) struct ValidationCandidateView<'a, M> {
    pub peer_id: &'a PeerId,
    pub head: &'a HeadDescriptor,
    pub update: &'a UpdateAnnounce,
    pub evaluation: &'a MetricReport,
    pub canary_report: Option<&'a CanaryEvalReport>,
    pub sample_weight: f64,
    pub quality_weight: f64,
    pub model: &'a M,
    pub update_evidence: Option<&'a ValidatedUpdateEvidence>,
}

impl<'a, M> From<&'a ValidationCandidate<M>> for ValidationCandidateView<'a, M> {
    fn from(candidate: &'a ValidationCandidate<M>) -> Self {
        Self {
            peer_id: &candidate.peer_id,
            head: &candidate.head,
            update: &candidate.update,
            evaluation: &candidate.evaluation,
            canary_report: candidate.canary_report.as_ref(),
            sample_weight: candidate.sample_weight,
            quality_weight: candidate.quality_weight,
            model: &candidate.model,
            update_evidence: candidate.update_evidence.as_ref(),
        }
    }
}

pub(crate) struct ValidationCandidateLoadArgs<'a, D> {
    pub experiment: &'a ExperimentHandle,
    pub store: &'a FsArtifactStore,
    pub device: &'a D,
    pub current_head: &'a Option<(PeerId, HeadDescriptor)>,
    pub revision_contract: Option<&'a RevisionContractBundle>,
    pub baseline_metrics: Option<&'a BTreeMap<String, MetricValue>>,
    pub canary_policy: &'a ValidatorCanaryPolicy,
    pub evaluate_candidates: bool,
    pub replay_snapshots: &'a [(PeerId, ControlPlaneSnapshot)],
    pub dataset_cache_dir: PathBuf,
    pub validator_peer_id: &'a PeerId,
}

pub(crate) struct ValidationCandidateHead {
    pub origin_peer_id: PeerId,
    pub provider_peer_ids: Vec<PeerId>,
    pub head: HeadDescriptor,
    pub update: UpdateAnnounce,
    pub workload_update: Option<WorkloadUpdateEnvelope>,
}

/// Matches the immutable identity of an update while ignoring relay-local
/// metadata such as providers and announcement time.
pub(crate) fn same_update_identity(left: &UpdateAnnounce, right: &UpdateAnnounce) -> bool {
    left.peer_id == right.peer_id
        && left.study_id == right.study_id
        && left.experiment_id == right.experiment_id
        && left.revision_id == right.revision_id
        && left.window_id == right.window_id
        && left.base_head_id == right.base_head_id
        && left.lease_id == right.lease_id
        && left.delta_artifact_id == right.delta_artifact_id
}
