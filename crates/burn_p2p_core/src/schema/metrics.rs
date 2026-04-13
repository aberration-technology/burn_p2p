use super::*;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Labels the trust class attached to a surfaced metric series or report.
pub enum MetricTrustClass {
    /// Validator- or evaluator-backed canonical metric material.
    Canonical,
    /// Read-model output derived from canonical reports and accepted runtime data.
    Derived,
    /// Candidate or cohort metrics that are useful before promotion but not yet authoritative.
    Provisional,
    /// Peer-local self-reported metrics for one lease or window.
    Local,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Labels the scope at which a metric is intended to be interpreted.
pub enum MetricScope {
    /// A single peer and work window.
    Peer,
    /// A reducer cohort or merge window.
    Cohort,
    /// A head-scoped evaluation or summary.
    Head,
    /// A network-wide progress, dynamics, or operational rollup.
    Network,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Captures the execution backend class that produced a peer-window metric record.
pub enum BackendClass {
    /// Read-only viewer or observer path.
    Viewer,
    /// Native CPU execution.
    Cpu,
    /// NdArray CPU execution.
    Ndarray,
    /// Native WGPU execution.
    Wgpu,
    /// Native CUDA execution.
    Cuda,
    /// Native ROCm execution.
    Rocm,
    /// Native Metal execution.
    Metal,
    /// Browser WebGPU execution.
    BrowserWgpu,
    /// Browser fallback execution without a hardware-backed training path.
    BrowserFallback,
    /// Deployment-specific or future backend class.
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Defines how evaluation metrics should be aggregated inside one protocol.
pub enum EvalAggregationRule {
    /// Arithmetic mean across the sampled items.
    Mean,
    /// Weighted mean across the sampled items.
    WeightedMean,
    /// Median across the sampled items.
    Median,
    /// Sum across the sampled items.
    Sum,
    /// Minimum across the sampled items.
    Min,
    /// Maximum across the sampled items.
    Max,
    /// Last observed value.
    Last,
    /// Deployment-specific aggregation behavior.
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Describes one metric emitted by a fixed evaluation protocol.
pub struct EvalMetricDef {
    /// Stable key used in metric maps and query payloads.
    pub metric_key: String,
    /// Human-readable label for dashboards.
    pub display_name: String,
    /// Optional unit label for the metric.
    pub unit: Option<String>,
    /// Whether higher values are better for this metric.
    pub higher_is_better: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures sampling, aggregation, and versioning for one evaluation protocol.
pub struct EvalProtocolOptions {
    /// Aggregation rule used to combine per-sample values.
    pub aggregation_rule: EvalAggregationRule,
    /// Maximum number of samples evaluated by the protocol.
    pub sample_budget: u64,
    /// Deterministic sampler seed for reproducible runs.
    pub deterministic_sampler_seed: u64,
    /// Human-readable version string for the protocol series.
    pub version: String,
}

impl EvalProtocolOptions {
    /// Creates evaluation protocol options.
    pub fn new(
        aggregation_rule: EvalAggregationRule,
        sample_budget: u64,
        deterministic_sampler_seed: u64,
        version: impl Into<String>,
    ) -> Self {
        Self {
            aggregation_rule,
            sample_budget,
            deterministic_sampler_seed,
            version: version.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Defines a deterministic evaluation protocol used to compare heads over time.
pub struct EvalProtocolManifest {
    /// Stable identifier for the protocol definition.
    pub eval_protocol_id: ContentId,
    /// Human-readable protocol name.
    pub name: String,
    /// Dataset view evaluated by the protocol.
    pub dataset_view_id: DatasetViewId,
    /// Split identifier within the dataset view.
    pub split_id: String,
    /// Metrics emitted by the protocol.
    pub metric_defs: Vec<EvalMetricDef>,
    /// Aggregation rule used to combine per-sample values.
    pub aggregation_rule: EvalAggregationRule,
    /// Maximum number of samples evaluated by the protocol.
    pub sample_budget: u64,
    /// Deterministic sampler seed for reproducible runs.
    pub deterministic_sampler_seed: u64,
    /// Human-readable version string for the protocol series.
    pub version: String,
}

impl EvalProtocolManifest {
    /// Creates a new evaluation protocol manifest and derives a stable identifier.
    pub fn new(
        name: impl Into<String>,
        dataset_view_id: DatasetViewId,
        split_id: impl Into<String>,
        metric_defs: Vec<EvalMetricDef>,
        options: EvalProtocolOptions,
    ) -> Result<Self, SchemaError> {
        let name = name.into();
        let split_id = split_id.into();
        let eval_protocol_id = ContentId::derive(&(
            &name,
            dataset_view_id.as_str(),
            &split_id,
            &metric_defs,
            &options.aggregation_rule,
            options.sample_budget,
            options.deterministic_sampler_seed,
            &options.version,
        ))?;
        Ok(Self {
            eval_protocol_id,
            name,
            dataset_view_id,
            split_id,
            metric_defs,
            aggregation_rule: options.aggregation_rule,
            sample_budget: options.sample_budget,
            deterministic_sampler_seed: options.deterministic_sampler_seed,
            version: options.version,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Captures the status of one peer-window metric emission.
pub enum PeerWindowStatus {
    /// The peer completed the planned work window.
    Completed,
    /// The peer aborted the window before publish.
    Aborted,
    /// The peer published work but it was rejected downstream.
    Rejected,
    /// The peer failed before producing a publishable delta.
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Peer-local training, validation, or aborted-window metrics scoped to one lease and base head.
pub struct PeerWindowMetrics {
    /// Network identifier for the metric record.
    pub network_id: NetworkId,
    /// Experiment identifier for the metric record.
    pub experiment_id: ExperimentId,
    /// Revision identifier for the metric record.
    pub revision_id: RevisionId,
    /// Workload identifier for the metric record.
    pub workload_id: WorkloadId,
    /// Dataset view identifier for the metric record.
    pub dataset_view_id: DatasetViewId,
    /// Peer that produced the metrics.
    pub peer_id: PeerId,
    /// Principal attached to the peer session, when known.
    pub principal_id: Option<PrincipalId>,
    /// Lease associated with the training or validation window.
    pub lease_id: LeaseId,
    /// Base head used when the window started.
    pub base_head_id: HeadId,
    /// Window start timestamp.
    pub window_started_at: DateTime<Utc>,
    /// Window finish timestamp.
    pub window_finished_at: DateTime<Utc>,
    /// Number of attempted tokens or samples.
    pub attempted_tokens_or_samples: u64,
    /// Number of accepted tokens or samples when known.
    pub accepted_tokens_or_samples: Option<u64>,
    /// Mean local training loss over the window, when available.
    pub local_train_loss_mean: Option<f64>,
    /// Final local training loss observed in the window, when available.
    pub local_train_loss_last: Option<f64>,
    /// Norm of the gradient or published delta, when available.
    pub grad_or_delta_norm: Option<f64>,
    /// Optimizer steps executed in the window.
    pub optimizer_step_count: u64,
    /// Compute time spent in the window.
    pub compute_time_ms: u64,
    /// Time spent fetching data or shards.
    pub data_fetch_time_ms: u64,
    /// Time spent publishing the resulting update.
    pub publish_latency_ms: u64,
    /// Head lag in steps when the window started.
    pub head_lag_at_start: u64,
    /// Head lag in steps when the window finished.
    pub head_lag_at_finish: u64,
    /// Backend class used by the peer for this window.
    pub backend_class: BackendClass,
    /// Runtime role used while executing the window.
    pub role: PeerRole,
    /// Outcome of the peer window.
    pub status: PeerWindowStatus,
    /// Optional reason attached to an aborted or rejected window.
    pub status_reason: Option<String>,
}

impl PeerWindowMetrics {
    /// Returns the fixed scope label for peer-window metrics.
    pub fn scope(&self) -> MetricScope {
        MetricScope::Peer
    }

    /// Returns the fixed trust class for peer-window metrics.
    pub fn trust_class(&self) -> MetricTrustClass {
        MetricTrustClass::Local
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Lightweight placement hint derived from a recent peer-window metric.
pub struct PeerWindowPlacementHint {
    /// Peer that produced the source window.
    pub peer_id: PeerId,
    /// Runtime role used while executing the source window.
    pub role: PeerRole,
    /// Backend class used by the peer for the source window.
    pub backend_class: BackendClass,
    /// Outcome of the source window.
    pub status: PeerWindowStatus,
    /// Accepted work units surfaced by the source window.
    pub accepted_tokens_or_samples: u64,
    /// End-to-end elapsed time for the source window.
    pub window_elapsed_ms: u64,
    /// Head lag observed when the source window finished.
    pub head_lag_at_finish: u64,
    /// Completion timestamp for the source window.
    pub window_finished_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Summarizes one trainer candidate inside a long-horizon fleet placement snapshot.
pub struct FleetPlacementPeer {
    /// Peer covered by the ranked placement record.
    pub peer_id: PeerId,
    /// Runtime role most recently observed for the peer.
    pub role: PeerRole,
    /// Backend class most recently observed for the peer.
    pub backend_class: BackendClass,
    /// Count of recent placement windows retained in the summary.
    pub recent_window_count: usize,
    /// Count of completed windows retained in the summary.
    pub completed_window_count: usize,
    /// Number of consecutive recent failed or non-completed windows.
    pub recent_failure_streak: usize,
    /// Latest observed placement status.
    pub latest_status: PeerWindowStatus,
    /// Latest observed placement timestamp.
    pub latest_finished_at: DateTime<Utc>,
    /// Decayed health score across retained windows.
    pub weighted_health: f64,
    /// Decayed head-lag score across retained windows.
    pub weighted_head_lag: f64,
    /// Decayed throughput score across retained windows.
    pub weighted_throughput: f64,
    /// Current trust score visible to the planner.
    pub trust_score: f64,
    /// Planner-recommended budget scale for future lease sizing.
    #[serde(default = "default_fleet_placement_scale")]
    pub recommended_budget_scale: f64,
    /// Planner-recommended microshard scale for future lease sizing.
    #[serde(default = "default_fleet_placement_scale")]
    pub recommended_microshard_scale: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Signed long-horizon trainer placement snapshot emitted on the metrics plane.
pub struct FleetPlacementSnapshot {
    /// Network covered by the placement snapshot.
    pub network_id: NetworkId,
    /// Planner peer that emitted the snapshot.
    pub planner_peer_id: PeerId,
    /// Monotonic generation time for the snapshot.
    pub generated_at: DateTime<Utc>,
    /// Freshness budget advertised by the planner.
    pub freshness_secs: u32,
    /// How many hint windows the planner retained when ranking peers.
    pub retained_hint_windows: usize,
    /// Selected trainers ordered from highest to lowest planner preference.
    pub selected_peer_ids: Vec<PeerId>,
    /// Ranked candidate summaries used to derive the selection.
    pub ranked_candidates: Vec<FleetPlacementPeer>,
    /// Free-form planner version label for audit and replay.
    pub planner_version: String,
    /// Signatures or attestations backing the placement decision.
    #[serde(default)]
    pub signature_bundle: Vec<SignatureMetadata>,
}

fn default_fleet_placement_scale() -> f64 {
    1.0
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Captures the status of one reducer cohort or merge window.
pub enum ReducerCohortStatus {
    /// The cohort is still open and may receive additional updates.
    Open,
    /// The cohort closed cleanly but has not been promoted.
    Closed,
    /// Reducer replicas disagreed beyond the configured threshold.
    Inconsistent,
    /// The cohort produced the promoted aggregate.
    Promoted,
    /// The cohort was superseded by a newer or better candidate.
    Superseded,
    /// The cohort was rejected.
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Reducer-side metrics that describe one merge window or candidate aggregate cohort.
pub struct ReducerCohortMetrics {
    /// Network identifier for the metric record.
    pub network_id: NetworkId,
    /// Experiment identifier for the metric record.
    pub experiment_id: ExperimentId,
    /// Revision identifier for the metric record.
    pub revision_id: RevisionId,
    /// Workload identifier for the metric record.
    pub workload_id: WorkloadId,
    /// Dataset view identifier for the metric record.
    pub dataset_view_id: DatasetViewId,
    /// Merge window identifier.
    pub merge_window_id: ContentId,
    /// Reducer group identifier.
    pub reducer_group_id: ContentId,
    /// Timestamp when the cohort metrics were emitted.
    pub captured_at: DateTime<Utc>,
    /// Base head for the cohort.
    pub base_head_id: HeadId,
    /// Candidate head produced by the cohort, when available.
    pub candidate_head_id: Option<HeadId>,
    /// Number of updates received by the reducer cohort.
    pub received_updates: u64,
    /// Number of updates accepted into the aggregate.
    pub accepted_updates: u64,
    /// Number of updates rejected from the aggregate.
    pub rejected_updates: u64,
    /// Total accepted weight across the cohort.
    pub sum_weight: f64,
    /// Number of accepted tokens or samples represented by the cohort.
    pub accepted_tokens_or_samples: u64,
    /// Mean staleness observed across accepted updates.
    pub staleness_mean: f64,
    /// Maximum staleness observed across accepted updates.
    pub staleness_max: f64,
    /// Delay between scheduled and actual cohort close time.
    pub window_close_delay_ms: u64,
    /// Total cohort duration.
    pub cohort_duration_ms: u64,
    /// Norm of the resulting aggregate update.
    pub aggregate_norm: f64,
    /// Relative reducer load associated with the cohort.
    pub reducer_load: f64,
    /// Estimated ingress bytes consumed while reducing the cohort.
    #[serde(default)]
    pub ingress_bytes: u128,
    /// Estimated egress bytes emitted while publishing the cohort result.
    #[serde(default)]
    pub egress_bytes: u128,
    /// Agreement score between replicated reducers, when present.
    pub replica_agreement: Option<f64>,
    /// Count of updates that arrived after the cohort's accepted close boundary.
    pub late_arrival_count: Option<u64>,
    /// Count of peers expected but missing from the cohort.
    pub missing_peer_count: Option<u64>,
    /// Rejection counts broken down by reason.
    #[serde(default)]
    pub rejection_reasons: BTreeMap<String, u64>,
    /// Outcome of the reducer cohort.
    pub status: ReducerCohortStatus,
}

impl ReducerCohortMetrics {
    /// Returns the fixed scope label for reducer cohort metrics.
    pub fn scope(&self) -> MetricScope {
        MetricScope::Cohort
    }

    /// Returns the fixed trust class for reducer cohort metrics.
    pub fn trust_class(&self) -> MetricTrustClass {
        MetricTrustClass::Provisional
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Captures the outcome of one validator or evaluator head report.
pub enum HeadEvalStatus {
    /// The evaluation completed successfully.
    Completed,
    /// The evaluation was intentionally skipped because promotion did not require model inference.
    Skipped,
    /// The evaluation only produced partial or incomplete data.
    Partial,
    /// The evaluation failed.
    Failed,
    /// The evaluation was superseded by a newer report.
    Superseded,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Authoritative or candidate head evaluation material tied to a fixed evaluation protocol.
pub struct HeadEvalReport {
    /// Network identifier for the report.
    pub network_id: NetworkId,
    /// Experiment identifier for the report.
    pub experiment_id: ExperimentId,
    /// Revision identifier for the report.
    pub revision_id: RevisionId,
    /// Workload identifier for the report.
    pub workload_id: WorkloadId,
    /// Evaluated head identifier.
    pub head_id: HeadId,
    /// Base head identifier, when the evaluated head was derived from another head.
    pub base_head_id: Option<HeadId>,
    /// Evaluation protocol used to produce the report.
    pub eval_protocol_id: ContentId,
    /// Evaluator set identifier for the committee or evaluator group.
    pub evaluator_set_id: ContentId,
    /// Metric values produced by the evaluation protocol.
    pub metric_values: BTreeMap<String, MetricValue>,
    /// Number of samples evaluated by the report.
    pub sample_count: u64,
    /// Dataset view evaluated by the report.
    pub dataset_view_id: DatasetViewId,
    /// Evaluation start timestamp.
    pub started_at: DateTime<Utc>,
    /// Evaluation finish timestamp.
    pub finished_at: DateTime<Utc>,
    /// Trust class attached to the report.
    pub trust_class: MetricTrustClass,
    /// Status of the evaluation report.
    pub status: HeadEvalStatus,
    /// Signatures or attestations backing the report.
    pub signature_bundle: Vec<SignatureMetadata>,
}

impl HeadEvalReport {
    /// Returns the fixed scope label for head evaluation reports.
    pub fn scope(&self) -> MetricScope {
        MetricScope::Head
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Snapshot manifest that identifies one point-in-time export of the metrics read model.
pub struct MetricsSnapshotManifest {
    /// Network identifier covered by the snapshot.
    pub network_id: NetworkId,
    /// Experiment identifier covered by the snapshot.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the snapshot.
    pub revision_id: RevisionId,
    /// Monotonic snapshot sequence number.
    pub snapshot_seq: u64,
    /// Canonical head watermark covered by the snapshot, when present.
    pub covers_until_head_id: Option<HeadId>,
    /// Merge-window watermark covered by the snapshot, when present.
    pub covers_until_merge_window_id: Option<ContentId>,
    /// Content reference for canonical head metrics included by the snapshot.
    pub canonical_head_metrics_ref: ContentId,
    /// Content reference for window or cohort rollups included by the snapshot.
    pub window_rollups_ref: ContentId,
    /// Content reference for network-wide rollups included by the snapshot.
    pub network_rollups_ref: ContentId,
    /// Content reference for an attached leaderboard snapshot, when present.
    pub leaderboard_ref: Option<ContentId>,
    /// Snapshot creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Signatures backing the snapshot manifest.
    pub signatures: Vec<SignatureMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Append-only ledger segment that allows peers to catch up on metrics after a snapshot.
pub struct MetricsLedgerSegment {
    /// Network identifier covered by the segment.
    pub network_id: NetworkId,
    /// Experiment identifier covered by the segment.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the segment.
    pub revision_id: RevisionId,
    /// Monotonic segment sequence number.
    pub segment_seq: u64,
    /// Starting head watermark covered by the segment, when present.
    pub from_head_id: Option<HeadId>,
    /// Ending head watermark covered by the segment, when present.
    pub to_head_id: Option<HeadId>,
    /// Starting merge-window watermark covered by the segment, when present.
    pub from_window_id: Option<ContentId>,
    /// Ending merge-window watermark covered by the segment, when present.
    pub to_window_id: Option<ContentId>,
    /// Content reference for the segment entries.
    pub entries_ref: ContentId,
    /// Hash of the segment payload.
    pub hash: ContentId,
    /// Hash of the previous segment, when present.
    pub prev_hash: Option<ContentId>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Labels the type of update carried by a live metrics event.
pub enum MetricsLiveEventKind {
    /// Snapshot or head-eval material changed and clients should refresh head-scoped views.
    SnapshotRefresh,
    /// Ledger growth changed and clients should refresh append-only progress views.
    LedgerAppend,
    /// Both snapshot and ledger state may have changed and clients should refresh catchup cursors.
    CatchupRefresh,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Summarizes the current metrics watermark for one experiment revision.
pub struct MetricsSyncCursor {
    /// Experiment identifier covered by the cursor.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the cursor.
    pub revision_id: RevisionId,
    /// Latest snapshot sequence available for the revision, when present.
    pub latest_snapshot_seq: Option<u64>,
    /// Latest ledger segment sequence available for the revision, when present.
    pub latest_ledger_segment_seq: Option<u64>,
    /// Latest certified or evaluated head covered by the metrics plane, when present.
    pub latest_head_id: Option<HeadId>,
    /// Latest merge-window watermark covered by the metrics plane, when present.
    pub latest_merge_window_id: Option<ContentId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Lightweight live event emitted by metrics publishers and edge services.
pub struct MetricsLiveEvent {
    /// Network identifier covered by the event.
    pub network_id: NetworkId,
    /// Type of refresh signaled by the event.
    pub kind: MetricsLiveEventKind,
    /// Per-revision metrics watermarks carried by the event.
    pub cursors: Vec<MetricsSyncCursor>,
    /// Timestamp when the event was generated.
    pub generated_at: DateTime<Utc>,
}
