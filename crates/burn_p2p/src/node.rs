use crate::config::default_node_runtime_state;

use super::*;

mod artifacts;
mod builder;
mod experiments;
mod lifecycle;
mod selected;

#[cfg(test)]
pub(crate) use artifacts::bounded_deadline;
pub(crate) use artifacts::{artifact_sync_attempt_timeout, ci_scaled_timeout};
use artifacts::{
    fair_request_timeout as artifact_fair_request_timeout, is_transient_artifact_sync_error,
};
#[cfg(test)]
pub(crate) use experiments::prioritize_connected_provider_peers;

#[cfg(test)]
pub(crate) use artifacts::prioritized_artifact_source_peers;
pub use builder::{Node, NodeBuilder};
pub use lifecycle::RunningNode;
pub(crate) use lifecycle::TrainingPrefetchTask;

pub(crate) fn fair_request_timeout(
    deadline: Instant,
    request_timeout: Duration,
    candidate_count: usize,
) -> Option<Duration> {
    artifact_fair_request_timeout(deadline, request_timeout, candidate_count)
}

fn slot_assignment_from_state(slot_state: &SlotRuntimeState) -> Option<SlotAssignmentState> {
    match slot_state {
        SlotRuntimeState::Unassigned => None,
        SlotRuntimeState::Assigned(assignment)
        | SlotRuntimeState::MaterializingBase(assignment)
        | SlotRuntimeState::FetchingShards(assignment)
        | SlotRuntimeState::Training(assignment)
        | SlotRuntimeState::Publishing(assignment)
        | SlotRuntimeState::CoolingDown(assignment)
        | SlotRuntimeState::Migrating(assignment) => Some(assignment.clone()),
        SlotRuntimeState::Blocked { assignment, .. } => assignment.clone(),
    }
}
