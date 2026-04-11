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

use planning::{
    adaptive_microshard_cap, ensure_training_placement_roles, fair_share_budget_work_units,
    fair_share_microshard_cap, load_model_for_head, load_runtime_model,
    merge_connected_lease_announcements, plan_prefetch_lease_for_window,
    preferred_microshards_for_peer, prefetched_lease_is_reusable, runtime_blocked_reason,
    scheduled_microshard_cap, supports_background_shard_prefetch,
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
    reducer_assignment: ReducerAssignment,
}

struct TrainingExecution<T, M> {
    lease: AssignmentLease,
    window_id: WindowId,
    base_head_id: HeadId,
    merge_window: MergeWindowState,
    reducer_assignment: ReducerAssignment,
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
