use std::collections::BTreeMap;

use burn_p2p::{
    BrowserEdgeSnapshot, ExperimentId, HeadId, LeaseId, MicroShardId, PeerId, RevisionId,
};
use burn_p2p_metrics::{MetricsCatchupBundle, NetworkPerformanceSummary};
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
    pub snapshot: BrowserEdgeSnapshot,
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
    #[serde(default)]
    pub active_experiment_id: Option<ExperimentId>,
    #[serde(default)]
    pub active_revision_id: Option<RevisionId>,
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
    pub executed: bool,
    pub restarted_trainer_label: String,
    pub trainer_restart_reconnected: bool,
    pub trainer_restart_resumed_training: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MnistPerformanceSummary {
    pub overall: NetworkPerformanceSummary,
    pub per_experiment: Vec<NetworkPerformanceSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserDeviceLimitSummary {
    pub wasm_backend: Option<String>,
    pub gpu_adapter_available: bool,
    pub gpu_reported_available: bool,
    pub navigator_gpu_exposed: bool,
    pub worker_gpu_exposed: bool,
    pub capability_recommended_role: Option<String>,
    pub capability_max_training_window_secs: Option<u64>,
    pub capability_max_checkpoint_bytes: Option<u64>,
    pub capability_max_shard_bytes: Option<u64>,
    pub training_budget_requires_webgpu: bool,
    pub training_budget_max_window_secs: Option<u64>,
    pub training_budget_max_checkpoint_bytes: Option<u64>,
    pub training_budget_max_shard_bytes: Option<u64>,
    pub training_budget_max_batch_size: Option<u32>,
    pub training_budget_precision: Option<String>,
    pub webgpu_backend_confirmed: bool,
    pub budget_within_capability_limits: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeNodeDeviceLimitSummary {
    pub label: String,
    pub backend_preference: Option<String>,
    pub preferred_backends: Vec<String>,
    pub configured_roles: Vec<String>,
    pub recommended_roles: Vec<String>,
    pub target_window_seconds: Option<u64>,
    pub budget_work_units: Option<u64>,
    pub device_memory_bytes: Option<u64>,
    pub system_memory_bytes: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeviceLimitExerciseSummary {
    pub native_limit_profiles_present: bool,
    pub native_trainer_backend_visible: bool,
    pub native_validator_backend_visible: bool,
    pub native_trainer_backends: Vec<String>,
    pub native_validator_backends: Vec<String>,
    pub native_nodes: Vec<NativeNodeDeviceLimitSummary>,
    pub browser: BrowserDeviceLimitSummary,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DemoPhaseEvent {
    pub phase: String,
    pub started_at: DateTime<Utc>,
    pub elapsed_ms_since_start: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DemoPhaseTiming {
    pub phase: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DemoPhaseSummary {
    pub total_elapsed_ms: u64,
    pub events: Vec<DemoPhaseEvent>,
    pub phases: Vec<DemoPhaseTiming>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MnistDynamicsSummary {
    pub training_window_count: usize,
    pub mean_window_duration_ms: u64,
    pub max_window_duration_ms: u64,
    pub mean_data_fetch_time_ms: u64,
    pub mean_publish_latency_ms: u64,
    pub mean_examples_per_window: f64,
    pub global_training_throughput_work_units_per_sec: f64,
    pub global_validation_throughput_samples_per_sec: f64,
    pub global_wait_time_ms: u64,
    pub global_idle_time_ms: u64,
    pub baseline_accuracy_gain: f64,
    pub low_lr_accuracy_gain: f64,
    pub performance: MnistPerformanceSummary,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DemoAssessmentSummary {
    pub live_native_training: bool,
    pub live_browser_training: bool,
    pub browser_runtime_roles_exercised: bool,
    pub split_topology_roles_exercised: bool,
    pub browser_dataset_transport: String,
    pub browser_shards_distributed_over_p2p: bool,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopologyExerciseSummary {
    pub bootstrap_seed_label: String,
    pub dedicated_reducer_label: String,
    pub validator_labels: Vec<String>,
    pub all_non_seed_nodes_bootstrap_via_seed: bool,
    pub mesh_fanout_beyond_seed_observed: bool,
    pub dedicated_reducer_participated: bool,
    pub aggregate_proposals_only_from_dedicated_reducer: bool,
    pub reducer_load_only_from_dedicated_reducer: bool,
    pub reducer_load_publishers_within_reducer_validation_tier: bool,
    pub reduction_attestations_only_from_validators: bool,
    pub merge_certificates_only_from_validators: bool,
    pub validators_observed_validation_quorum: bool,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserExecutionSummary {
    pub live_browser_training: bool,
    pub browser_latency_emulated: bool,
    pub slower_profile_increased_total_time: bool,
    pub same_network_context: bool,
    pub same_experiment_context: bool,
    pub same_revision_context: bool,
    pub same_head_context: bool,
    pub same_lease_context: bool,
    pub same_leased_microshards: bool,
    pub session_enrolled: bool,
    pub receipt_submission_accepted: bool,
    pub runtime_state: Option<String>,
    pub transport: Option<String>,
    pub head_artifact_transport: Option<String>,
    pub active_assignment: bool,
    pub emitted_receipt_id: Option<String>,
    pub accepted_receipt_ids: Vec<String>,
    pub trainer_runtime_and_wasm_training_coherent: bool,
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
    pub baseline_accuracy_delta_vs_low_lr: f64,
    pub baseline_accuracy_tolerance_vs_low_lr: f64,
    pub baseline_loss_advantage_vs_low_lr: f64,
    pub late_joiner_synced_checkpoint: bool,
    pub shard_assignments_are_distinct: bool,
    pub phase_timeline: DemoPhaseSummary,
    pub device_limits: DeviceLimitExerciseSummary,
    pub topology: TopologyExerciseSummary,
    pub trainer_leases: Vec<TrainerLeaseSummary>,
    pub browser_roles: Vec<BrowserRoleExerciseSummary>,
    pub browser_dataset_access: BrowserDatasetAccessSummary,
    pub browser_execution: BrowserExecutionSummary,
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
