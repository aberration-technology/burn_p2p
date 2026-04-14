use super::*;
use crate::metrics_runtime::{
    TrainingMetricBuildArgs, build_metrics_announcement, build_peer_window_placement_hint,
    build_training_peer_window_metrics, persist_peer_window_metrics,
};
use crate::runtime_support::{
    LagAssessment, active_experiment_directory_entry, local_training_adaptation_factor,
    local_training_schedule_hint, runtime_training_assignment_peers,
    snapshots_with_local_control_plane,
};
use burn_p2p_core::{MetricsLiveEventKind, MicroShard};
use std::collections::BTreeSet;

mod continuous;
mod execution;
mod planning;
#[cfg(test)]
mod tests;

pub(crate) use planning::load_model_for_head;
use planning::{
    adaptive_microshard_cap, ensure_training_placement_roles, fair_share_budget_work_units,
    fair_share_microshard_cap, load_runtime_model, merge_connected_lease_announcements,
    plan_prefetch_lease_for_window, preferred_microshards_for_peer, prefetched_lease_is_reusable,
    runtime_blocked_reason, scheduled_microshard_cap, supports_background_shard_prefetch,
    training_placement_budget_work_units, unleased_microshards_for_window,
    wait_for_prefetch_completion,
};

struct TrainingPreparedState {
    experiment: ExperimentHandle,
    assignment: SlotAssignmentState,
    storage: StorageConfig,
    store: FsArtifactStore,
    local_peer_id: PeerId,
    current_head: Option<(PeerId, HeadDescriptor)>,
    network_id: NetworkId,
    telemetry_snapshot: NodeTelemetrySnapshot,
    mainnet_roles: PeerRoleSet,
    metrics_retention: MetricsRetentionBudget,
    node_config: NodeConfig,
    robustness_policy: RobustnessPolicy,
}

struct PlannedTrainingWindow {
    calibrator: CapabilityCalibrator,
    limit_profile: LimitProfile,
    registration: DatasetRegistration,
    microshard_plan: MicroShardPlan,
    lease: PlannedLease,
    window_id: WindowId,
    base_head_id: HeadId,
    merge_window: MergeWindowState,
    reducer_assignment: Option<ReducerAssignment>,
}

struct TrainingExecution<T, M> {
    lease: AssignmentLease,
    window_id: WindowId,
    base_head_id: HeadId,
    merge_window: MergeWindowState,
    reducer_assignment: Option<ReducerAssignment>,
    limit_profile: LimitProfile,
    model: M,
    head: HeadDescriptor,
    artifact: ArtifactDescriptor,
    contribution: ContributionReceipt,
    report: WindowReport<T>,
    window_started_at: DateTime<Utc>,
    data_fetch_time_ms: u64,
}

/// Stateful continuous-training helper that keeps a warm in-memory model and
/// reconciles newly visible canonical heads between windows.
///
/// This is the opinionated long-running trainer path: it lets a trainer keep
/// publishing local windows without synchronously waiting for validator
/// promotion after every window. While the trainer is ahead of the last visible
/// canonical head it advances on its own local speculative chain; once a newer
/// canonical head appears, the session rebases onto that canonical head using
/// the configured reconcile strategy before the next window starts.
pub struct ContinuousTrainer<'a, P>
where
    P: P2pWorkload,
{
    node: &'a mut RunningNode<P>,
    experiment: ExperimentHandle,
    policy: ContinuousTrainerPolicy,
    canonical_head: Option<HeadDescriptor>,
    training_head: Option<HeadDescriptor>,
    warm_model: Option<P::Model>,
}

pub(in crate::training) fn poll_diffusion_steady_state_opportunistically<P>(
    node: &mut RunningNode<P>,
    experiment: &ExperimentHandle,
) where
    P: P2pWorkload,
{
    // Diffusion settlement is a best-effort background pass. Trainers must be
    // able to keep moving even if one local polling attempt fails.
    if let Err(error) = node.advance_diffusion_steady_state(experiment, None, None) {
        drop(error);
    }
}

pub(in crate::training) fn kick_diffusion_steady_state_after_local_publish<P>(
    node: &mut RunningNode<P>,
    experiment: &ExperimentHandle,
    window_id: WindowId,
    base_head_id: &HeadId,
    head_id: &HeadId,
    artifact_id: &ArtifactId,
) where
    P: P2pWorkload,
{
    const LOCAL_DIFFUSION_PUBLISH_SYNC_TIMEOUT: Duration = Duration::from_millis(250);
    const LOCAL_DIFFUSION_PUBLISH_SYNC_POLL_INTERVAL: Duration = Duration::from_millis(10);

    let deadline = Instant::now() + LOCAL_DIFFUSION_PUBLISH_SYNC_TIMEOUT;
    while Instant::now() < deadline {
        let snapshot = node.telemetry().snapshot();
        let Some(local_peer_id) = snapshot.local_peer_id.as_ref() else {
            break;
        };
        let local_publish_visible = snapshot
            .control_plane
            .merge_window_announcements
            .iter()
            .any(|announcement| {
                announcement.merge_window.study_id == experiment.study_id
                    && announcement.merge_window.experiment_id == experiment.experiment_id
                    && announcement.merge_window.revision_id == experiment.revision_id
                    && announcement.merge_window.window_id == window_id
                    && announcement.merge_window.base_head_id == *base_head_id
            })
            && snapshot
                .control_plane
                .update_announcements
                .iter()
                .any(|announcement| {
                    announcement.update.study_id == experiment.study_id
                        && announcement.update.experiment_id == experiment.experiment_id
                        && announcement.update.revision_id == experiment.revision_id
                        && announcement.update.window_id == window_id
                        && announcement.update.base_head_id == *base_head_id
                        && announcement.update.peer_id == *local_peer_id
                        && announcement.update.delta_artifact_id == *artifact_id
                })
            && snapshot
                .control_plane
                .head_announcements
                .iter()
                .any(|announcement| {
                    announcement.head.study_id == experiment.study_id
                        && announcement.head.experiment_id == experiment.experiment_id
                        && announcement.head.revision_id == experiment.revision_id
                        && announcement.head.head_id == *head_id
                        && announcement.head.artifact_id == *artifact_id
                        && announcement.provider_peer_id.as_ref() == Some(local_peer_id)
                });
        if local_publish_visible {
            break;
        }
        std::thread::sleep(LOCAL_DIFFUSION_PUBLISH_SYNC_POLL_INTERVAL);
    }

    poll_diffusion_steady_state_opportunistically(node, experiment);
}

struct PrefetchLeasePlanArgs<'a> {
    network_id: &'a NetworkId,
    experiment: &'a ExperimentHandle,
    dataset_view: &'a DatasetView,
    local_peer_id: &'a PeerId,
    assignment_peers: &'a [PeerId],
    microshards: &'a [MicroShard],
    window_id: WindowId,
    budget_work_units: u64,
    adaptation_factor: f64,
}

fn observed_elapsed_seconds(started_at: DateTime<Utc>, finished_at: DateTime<Utc>) -> u64 {
    let elapsed_ms = finished_at
        .signed_duration_since(started_at)
        .num_milliseconds()
        .max(1) as u64;
    elapsed_ms.saturating_add(999) / 1000
}
