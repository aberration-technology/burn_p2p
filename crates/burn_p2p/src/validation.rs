use super::*;
use crate::metrics_runtime::{
    ValidationMetricBuildArgs, build_head_eval_report, build_metrics_announcement,
    build_peer_window_placement_hint, build_reducer_cohort_metrics,
    build_validation_peer_window_metrics, persist_eval_protocol_manifest, persist_head_eval_report,
    persist_peer_window_metrics, persist_reducer_cohort_metrics,
};
use crate::runtime_support::LagAssessment;
use crate::runtime_support::{
    active_experiment_directory_entry, load_json, runtime_training_peers,
    snapshots_with_local_control_plane,
};
use burn_p2p_core::MetricsLiveEventKind;
use burn_p2p_core::QuarantinePolicy;
use burn_p2p_core::RobustnessAlertSeverity;

mod candidate;
mod coordination;
mod execution;
mod output;
mod robustness;
#[cfg(test)]
mod tests;

use candidate::{
    ValidationCandidate, ValidationCandidateHead, ValidationCandidateLoadArgs,
    ValidationCandidateView, collect_validation_candidate_heads, fallback_best_candidate_index,
    load_validation_base_model, load_validation_candidate_model, select_validation_head,
};
use output::{
    LocalAggregateMaterialization, ValidationExecution, aggregate_stats_from_updates,
    build_aggregate_record, build_reduction_certificate, build_validation_aggregate,
    build_validation_contribution, build_validation_merge_certificate,
    build_validation_quorum_certificate, build_validation_reducer_load,
};
use robustness::{
    CandidateRobustnessOutcome, PersistedRobustnessState, ValidationRobustnessExecution,
    append_canary_escalation_alert, append_quarantine_escalation_alerts,
    append_replica_disagreement_alert, build_validation_canary_report,
    evaluate_candidate_robustness, observed_replica_agreement, persist_validation_robustness_state,
};
#[cfg(test)]
use robustness::{PeerRobustnessState, project_robustness_state};

const VALIDATION_QUORUM_WAIT: Duration = Duration::from_secs(5);
const REDUCER_PROPOSAL_WAIT: Duration = Duration::from_secs(2);
const VALIDATION_PREPARE_REMOTE_SNAPSHOT_TIMEOUT: Duration = Duration::from_millis(750);
const VALIDATION_ARTIFACT_SYNC_TIMEOUT: Duration = Duration::from_secs(15);
const VALIDATION_PROMOTION_GRACE: Duration = Duration::from_millis(300);
const VALIDATION_COORDINATION_POLL_INTERVAL: Duration = Duration::from_millis(25);

struct ValidationPreparedState {
    assignment: SlotAssignmentState,
    storage: StorageConfig,
    store: FsArtifactStore,
    local_peer_id: PeerId,
    snapshots: Vec<(PeerId, ControlPlaneSnapshot)>,
    visible_snapshots: Vec<(PeerId, ControlPlaneSnapshot)>,
    current_head: Option<(PeerId, HeadDescriptor)>,
    base_head_id: HeadId,
    dataset_view_id: DatasetViewId,
    merge_window: MergeWindowState,
    updates: Vec<UpdateAnnounce>,
    expected_training_peer_count: usize,
    first_update_announced_at: Option<DateTime<Utc>>,
    metrics_retention: MetricsRetentionBudget,
    robustness_policy: RobustnessPolicy,
    robustness_state: PersistedRobustnessState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ValidationCandidateCacheKey {
    study_id: StudyId,
    experiment_id: ExperimentId,
    revision_id: RevisionId,
    window_id: WindowId,
    base_head_id: HeadId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ValidationExecutionDraftKey {
    accepted_candidates: Vec<(PeerId, ArtifactId, u64, u64)>,
}

#[derive(Clone)]
struct ValidationExecutionDraft {
    key: ValidationExecutionDraftKey,
    source_peer_id: PeerId,
    merged_head: HeadDescriptor,
    evaluation: MetricReport,
    aggregate_record: AggregateArtifactRecord,
    local_aggregate_materialization: LocalAggregateMaterialization,
}

pub(crate) struct ValidationCandidateCache<M> {
    key: ValidationCandidateCacheKey,
    base_model: Option<M>,
    candidates: Vec<ValidationCandidate<M>>,
    draft: Option<ValidationExecutionDraft>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggregateResolutionMode {
    RequireLocalReduction,
    PreferRemoteReducerProposal,
}

struct ResolvedAggregateProposal {
    aggregate: AggregateEnvelope,
    local_aggregate_materialization: Option<LocalAggregateMaterialization>,
}

struct ValidationSnapshotObservation {
    canonical_snapshots: Vec<(PeerId, ControlPlaneSnapshot)>,
    current_head: Option<(PeerId, HeadDescriptor)>,
    observed_merge_window: Option<MergeWindowState>,
    updates: Vec<UpdateAnnounce>,
    candidate_head_count: usize,
    base_head_id: HeadId,
}

fn effective_validator_quorum(merge_window: &MergeWindowState) -> usize {
    usize::from(merge_window.policy.promotion_policy.validator_quorum.max(1))
        .min(merge_window.validators.len().max(1))
}

fn validation_candidate_cache_key(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
) -> ValidationCandidateCacheKey {
    ValidationCandidateCacheKey {
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: prepared.merge_window.window_id,
        base_head_id: prepared.base_head_id.clone(),
    }
}

fn effective_canary_minimum_evaluator_quorum(prepared: &ValidationPreparedState) -> u16 {
    prepared
        .robustness_policy
        .validator_canary_policy
        .minimum_evaluator_quorum
        .min(effective_validator_quorum(&prepared.merge_window) as u16)
}

fn validation_execution_draft_key<M>(
    candidate_models: &[ValidationCandidateView<'_, M>],
) -> ValidationExecutionDraftKey {
    ValidationExecutionDraftKey {
        accepted_candidates: candidate_models
            .iter()
            .map(|candidate| {
                (
                    candidate.peer_id.clone(),
                    candidate.head.artifact_id.clone(),
                    candidate.sample_weight.to_bits(),
                    candidate.quality_weight.to_bits(),
                )
            })
            .collect(),
    }
}

fn canary_blocks_promotion(
    prepared: &ValidationPreparedState,
    canary_report: &CanaryEvalReport,
) -> bool {
    let policy = &prepared.robustness_policy.validator_canary_policy;
    !canary_report.accepted
        || canary_report.detected_backdoor_trigger
        || canary_report.regression_margin > policy.maximum_regression_delta
        || canary_report.evaluator_quorum < effective_canary_minimum_evaluator_quorum(prepared)
}

fn should_wait_for_candidate_settle(
    prepared: &ValidationPreparedState,
    candidate_head_count: usize,
    now: DateTime<Utc>,
    resolution_mode: AggregateResolutionMode,
) -> bool {
    if !matches!(
        resolution_mode,
        AggregateResolutionMode::RequireLocalReduction
    ) {
        return false;
    }
    if candidate_head_count == 0 {
        return false;
    }

    let expected_peer_count = prepared.expected_training_peer_count.max(1);
    let target_cohort = usize::from(prepared.merge_window.policy.target_leaf_cohort.max(1));
    let expected_candidate_count = expected_peer_count.min(target_cohort);
    if expected_candidate_count <= 1 {
        return false;
    }
    if candidate_head_count >= expected_candidate_count {
        return false;
    }
    if candidate_head_count != 1 {
        return false;
    }
    if prepared.updates.len() <= candidate_head_count {
        return false;
    }

    let settle_ms = i64::from(
        prepared
            .merge_window
            .policy
            .publish_jitter_ms
            .div_ceil(3)
            .clamp(25, 250),
    );
    let settle_anchor = prepared
        .first_update_announced_at
        .unwrap_or(prepared.merge_window.opened_at);
    now < settle_anchor + chrono::Duration::milliseconds(settle_ms)
}
