use serde::{Deserialize, Serialize};

use burn_ecs::TrainingRunId;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct P2pWindowStarted {
    pub run_id: TrainingRunId,
    pub experiment_id: String,
    pub revision_id: String,
    pub window_id: u64,
    pub base_head_id: Option<String>,
    pub canonical_head_id: Option<String>,
    pub training_head_id: Option<String>,
}

impl burn_ecs::prelude::Message for P2pWindowStarted {}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct P2pWindowFinished {
    pub run_id: TrainingRunId,
    pub experiment_id: String,
    pub revision_id: String,
    pub window_id: u64,
    pub head_id: String,
    pub artifact_id: String,
    pub data_fetch_time_ms: u64,
    pub publish_latency_ms: u64,
    pub metrics: Vec<(String, f64)>,
}

impl burn_ecs::prelude::Message for P2pWindowFinished {}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
/// Reports one accepted context-bound update for a run/window.
pub struct P2pContextUpdateObserved {
    pub run_id: TrainingRunId,
    pub window_id: u64,
    pub context: crate::UpdateRoutingContext,
}

impl burn_ecs::prelude::Message for P2pContextUpdateObserved {}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct P2pCanonicalReconcileEvent {
    pub run_id: TrainingRunId,
    pub experiment_id: String,
    pub revision_id: String,
    pub previous_training_head_id: Option<String>,
    pub canonical_head_id: String,
    pub strategy: String,
}

impl burn_ecs::prelude::Message for P2pCanonicalReconcileEvent {}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
/// Reports the latest effective capability selected by P2P negotiation.
pub struct P2pCapabilityAssessment {
    pub run_id: TrainingRunId,
    pub participation: burn_ecs::PipelineParticipation,
    pub compute: burn_ecs::PipelineComputeClass,
    pub supported_participation: std::collections::BTreeSet<burn_ecs::PipelineParticipation>,
    pub reason: String,
}

impl burn_ecs::prelude::Message for P2pCapabilityAssessment {}
