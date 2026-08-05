use std::collections::BTreeMap;

use burn_ecs::{
    bevy_ecs,
    prelude::{Component, Resource, TrainingRunId},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Component, Deserialize, Serialize, PartialEq, Eq)]
pub struct P2pTrainingTelemetryState {
    pub run_id: Option<TrainingRunId>,
    pub experiment_id: Option<String>,
    pub revision_id: Option<String>,
    pub latest_window_id: Option<u64>,
    pub latest_head_id: Option<String>,
    pub latest_artifact_id: Option<String>,
    pub canonical_head_id: Option<String>,
    pub training_head_id: Option<String>,
    pub published_windows: u64,
    pub reconciliations: u64,
    pub participation: Option<burn_ecs::PipelineParticipation>,
    pub compute: Option<burn_ecs::PipelineComputeClass>,
    pub capability_transitions: u64,
}

#[derive(Debug, Clone)]
pub struct P2pContinuousTrainerState<M> {
    pub canonical_head_id: Option<String>,
    pub training_head_id: Option<String>,
    pub warm_model: Option<M>,
    pub speculative_lead_steps: u64,
}

impl<M> Default for P2pContinuousTrainerState<M> {
    fn default() -> Self {
        Self {
            canonical_head_id: None,
            training_head_id: None,
            warm_model: None,
            speculative_lead_steps: 0,
        }
    }
}

impl<M: Send + Sync + 'static> burn_ecs::prelude::Component for P2pContinuousTrainerState<M> {
    const STORAGE_TYPE: bevy_ecs::component::StorageType = bevy_ecs::component::StorageType::Table;

    type Mutability = bevy_ecs::component::Mutable;
}

#[derive(Debug, Clone, Component, Deserialize, Serialize, PartialEq, Eq)]
pub struct P2pWindowMetadata {
    pub experiment_id: String,
    pub revision_id: String,
    pub base_head_id: Option<String>,
    pub canonical_head_id: Option<String>,
    pub training_head_id: Option<String>,
}

#[derive(Debug, Clone, Default, Resource)]
pub struct PendingP2pWindowMetadata {
    pub by_run_window_id: BTreeMap<(TrainingRunId, u64), P2pWindowMetadata>,
}

#[derive(Debug, Clone, Default, Component, Deserialize, Serialize, PartialEq, Eq)]
pub struct P2pWindowContextUpdates {
    pub contexts: Vec<crate::UpdateRoutingContext>,
}

#[derive(Debug, Clone, Default, Resource)]
pub struct PendingP2pWindowContextUpdates {
    pub by_run_window_id: BTreeMap<(TrainingRunId, u64), Vec<crate::UpdateRoutingContext>>,
}

pub const MAX_PENDING_CONTEXT_WINDOWS: usize = 1_024;
pub const MAX_CONTEXT_IDENTITIES_PER_WINDOW: usize = 256;

impl PendingP2pWindowContextUpdates {
    pub fn record(
        &mut self,
        run_id: TrainingRunId,
        window_id: u64,
        context: crate::UpdateRoutingContext,
    ) {
        let key = (run_id, window_id);
        let contexts = self.by_run_window_id.entry(key).or_default();
        if !contexts.contains(&context) {
            if contexts.len() >= MAX_CONTEXT_IDENTITIES_PER_WINDOW {
                contexts.remove(0);
            }
            contexts.push(context);
        }
        while self.by_run_window_id.len() > MAX_PENDING_CONTEXT_WINDOWS {
            let Some(evicted) = self.by_run_window_id.keys().next().cloned() else {
                break;
            };
            self.by_run_window_id.remove(&evicted);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(generation: u64) -> crate::UpdateRoutingContext {
        crate::UpdateRoutingContext {
            context_family_hash: crate::ContentId::new("family"),
            slot: 1,
            generation,
            parameter_catalog_hash: crate::ContentId::new(format!("catalog-{generation}")),
        }
    }

    #[test]
    fn pending_context_metadata_is_deduplicated_and_bounded() {
        let mut pending = PendingP2pWindowContextUpdates::default();
        for generation in 0..260 {
            pending.record("run".into(), 7, context(generation));
        }
        pending.record("run".into(), 7, context(259));
        let contexts = pending
            .by_run_window_id
            .get(&(TrainingRunId::from("run"), 7))
            .expect("pending context window");
        assert_eq!(contexts.len(), MAX_CONTEXT_IDENTITIES_PER_WINDOW);
        assert_eq!(contexts.first().expect("first retained").generation, 4);
        assert_eq!(contexts.last().expect("last retained").generation, 259);

        for window in 0..=MAX_PENDING_CONTEXT_WINDOWS as u64 {
            pending.record("other".into(), window, context(window));
        }
        assert_eq!(pending.by_run_window_id.len(), MAX_PENDING_CONTEXT_WINDOWS);
    }
}
