use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct P2pWindowStarted {
    pub run_id: String,
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
    pub run_id: String,
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
pub struct P2pCanonicalReconcileEvent {
    pub run_id: String,
    pub experiment_id: String,
    pub revision_id: String,
    pub previous_training_head_id: Option<String>,
    pub canonical_head_id: String,
    pub strategy: String,
}

impl burn_ecs::prelude::Message for P2pCanonicalReconcileEvent {}
