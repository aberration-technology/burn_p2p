use std::collections::BTreeMap;

use burn_ecs::{
    bevy_ecs,
    prelude::{Component, Resource},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Resource, Deserialize, Serialize, PartialEq, Eq)]
pub struct P2pTrainingTelemetryState {
    pub run_id: Option<String>,
    pub experiment_id: Option<String>,
    pub revision_id: Option<String>,
    pub latest_window_id: Option<u64>,
    pub latest_head_id: Option<String>,
    pub latest_artifact_id: Option<String>,
    pub canonical_head_id: Option<String>,
    pub training_head_id: Option<String>,
    pub published_windows: u64,
    pub reconciliations: u64,
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

impl<M: Send + Sync + 'static> burn_ecs::prelude::Resource for P2pContinuousTrainerState<M> {}

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
    pub by_window_id: BTreeMap<u64, P2pWindowMetadata>,
}
