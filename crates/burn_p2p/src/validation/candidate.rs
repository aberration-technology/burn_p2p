use super::*;

mod discovery;
mod model;
#[cfg(test)]
mod tests;

pub(super) use discovery::collect_validation_candidate_heads;
pub(super) use model::{
    fallback_best_candidate_index, load_validation_base_model, load_validation_candidate_model,
    select_reducer_authority_head, select_validation_head,
};

pub(super) struct ValidationCandidate<M> {
    pub peer_id: PeerId,
    pub head: HeadDescriptor,
    pub update: UpdateAnnounce,
    pub evaluation: MetricReport,
    pub canary_report: Option<CanaryEvalReport>,
    pub sample_weight: f64,
    pub quality_weight: f64,
    pub model: M,
}

#[derive(Clone, Copy)]
pub(super) struct ValidationCandidateView<'a, M> {
    pub peer_id: &'a PeerId,
    pub head: &'a HeadDescriptor,
    pub update: &'a UpdateAnnounce,
    pub evaluation: &'a MetricReport,
    pub canary_report: Option<&'a CanaryEvalReport>,
    pub sample_weight: f64,
    pub quality_weight: f64,
    pub model: &'a M,
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
        }
    }
}

pub(super) struct ValidationCandidateLoadArgs<'a, D> {
    pub experiment: &'a ExperimentHandle,
    pub store: &'a FsArtifactStore,
    pub device: &'a D,
    pub current_head: &'a Option<(PeerId, HeadDescriptor)>,
    pub canary_threshold: f64,
    pub evaluate_candidates: bool,
}

pub(super) struct ValidationCandidateHead {
    pub origin_peer_id: PeerId,
    pub provider_peer_ids: Vec<PeerId>,
    pub head: HeadDescriptor,
    pub update: UpdateAnnounce,
}
