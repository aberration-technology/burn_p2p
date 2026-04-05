use std::collections::BTreeMap;

use burn_p2p::{BrowserPortalSnapshot, HeadId, LeaseId, MicroShardId, PeerId};
use burn_p2p_metrics::MetricsCatchupBundle;
use burn_p2p_views::BrowserAppSurface;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaptureInteraction {
    pub action: String,
    #[serde(default)]
    pub selector: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub wait_for_text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaptureViewport {
    pub width: u32,
    pub height: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserScenarioExport {
    pub slug: String,
    pub title: String,
    pub description: String,
    pub default_surface: BrowserAppSurface,
    pub snapshot: BrowserPortalSnapshot,
    pub metrics_catchup: Vec<MetricsCatchupBundle>,
    #[serde(default)]
    pub runtime_states: Vec<String>,
    #[serde(default)]
    pub interactions: Vec<CaptureInteraction>,
    #[serde(default)]
    pub viewport: Option<CaptureViewport>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrainerLeaseSummary {
    pub node_label: String,
    pub peer_id: PeerId,
    pub experiment_id: String,
    pub lease_id: LeaseId,
    pub base_head_id: HeadId,
    pub accepted_head_id: HeadId,
    pub microshards: Vec<MicroShardId>,
    pub examples: usize,
    pub window_duration_ms: u64,
    pub data_fetch_time_ms: u64,
    pub publish_latency_ms: u64,
    pub metrics: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExperimentRunSummary {
    pub experiment_id: String,
    pub revision_id: String,
    pub display_name: String,
    pub trainer_labels: Vec<String>,
    pub initial_head_id: HeadId,
    pub final_head_id: HeadId,
    pub initial_accuracy: f64,
    pub final_accuracy: f64,
    pub final_loss: f64,
    pub digit_zero_accuracy: f64,
    pub merge_count: usize,
    pub accepted_receipt_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeSummary {
    pub label: String,
    pub peer_id: PeerId,
    pub roles: Vec<String>,
    pub storage_root: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserRoleExerciseSummary {
    pub role: String,
    pub runtime_state: String,
    pub active_assignment: bool,
    pub active_head_id: Option<HeadId>,
    pub transport: Option<String>,
    pub command_completed: bool,
    pub emitted_receipt_id: Option<String>,
    pub transport_recovered: bool,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserDatasetAccessSummary {
    pub upstream_mode: String,
    pub browser_http_base_url: String,
    pub fetch_manifest_requested: bool,
    pub leased_microshards: Vec<MicroShardId>,
    pub requested_paths: Vec<String>,
    pub fetched_only_leased_shards: bool,
    pub shards_distributed_over_p2p: bool,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResilienceDrillSummary {
    pub restarted_trainer_label: String,
    pub trainer_restart_reconnected: bool,
    pub trainer_restart_resumed_training: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MnistDynamicsSummary {
    pub training_window_count: usize,
    pub mean_window_duration_ms: u64,
    pub max_window_duration_ms: u64,
    pub mean_data_fetch_time_ms: u64,
    pub mean_publish_latency_ms: u64,
    pub mean_examples_per_window: f64,
    pub baseline_accuracy_gain: f64,
    pub low_lr_accuracy_gain: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DemoAssessmentSummary {
    pub live_native_training: bool,
    pub live_browser_training: bool,
    pub browser_runtime_roles_exercised: bool,
    pub browser_dataset_transport: String,
    pub browser_shards_distributed_over_p2p: bool,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoreMnistSummary {
    pub generated_at: DateTime<Utc>,
    pub network_id: String,
    pub dataset_root: String,
    pub experiments: Vec<ExperimentRunSummary>,
    pub nodes: Vec<NodeSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MnistCorrectnessSummary {
    pub generated_at: DateTime<Utc>,
    pub baseline_outperformed_low_lr: bool,
    pub late_joiner_synced_checkpoint: bool,
    pub shard_assignments_are_distinct: bool,
    pub trainer_leases: Vec<TrainerLeaseSummary>,
    pub browser_roles: Vec<BrowserRoleExerciseSummary>,
    pub browser_dataset_access: BrowserDatasetAccessSummary,
    pub resilience: ResilienceDrillSummary,
    pub dynamics: MnistDynamicsSummary,
    pub assessment: DemoAssessmentSummary,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MnistRunExport {
    pub summary: CoreMnistSummary,
    pub correctness: MnistCorrectnessSummary,
    pub browser_scenarios: Vec<BrowserScenarioExport>,
}
