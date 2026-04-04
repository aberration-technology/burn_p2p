//! Optional metrics indexer and read-model helpers for `burn_p2p`.
//!
//! This crate is intentionally non-consensus-critical. It ingests peer-window,
//! reducer-cohort, and head-evaluation records and materializes derived views,
//! snapshot manifests, and append-only ledger segments for dashboards and peer
//! catchup flows.
#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use burn_p2p_core::{
    ContentId, DatasetViewId, ExperimentId, HeadEvalReport, HeadEvalStatus, HeadId, LeaseId,
    MetricScope, MetricTrustClass, MetricsLedgerSegment, MetricsLiveEvent, MetricsLiveEventKind,
    MetricsSnapshotManifest, MetricsSyncCursor, NetworkId, PeerId, PeerWindowMetrics,
    PeerWindowStatus, ReducerCohortMetrics, RevisionId, SignatureMetadata, WorkloadId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Appends one raw metric record to the metrics ledger.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MetricEnvelope {
    /// Peer-window metric material.
    PeerWindow(PeerWindowMetrics),
    /// Reducer cohort metric material.
    ReducerCohort(ReducerCohortMetrics),
    /// Canonical or candidate head evaluation material.
    HeadEval(HeadEvalReport),
}

impl MetricEnvelope {
    fn network_id(&self) -> &NetworkId {
        match self {
            Self::PeerWindow(metrics) => &metrics.network_id,
            Self::ReducerCohort(metrics) => &metrics.network_id,
            Self::HeadEval(report) => &report.network_id,
        }
    }

    fn experiment_id(&self) -> &ExperimentId {
        match self {
            Self::PeerWindow(metrics) => &metrics.experiment_id,
            Self::ReducerCohort(metrics) => &metrics.experiment_id,
            Self::HeadEval(report) => &report.experiment_id,
        }
    }

    fn revision_id(&self) -> &RevisionId {
        match self {
            Self::PeerWindow(metrics) => &metrics.revision_id,
            Self::ReducerCohort(metrics) => &metrics.revision_id,
            Self::HeadEval(report) => &report.revision_id,
        }
    }

    fn base_head_id(&self) -> Option<&HeadId> {
        match self {
            Self::PeerWindow(metrics) => Some(&metrics.base_head_id),
            Self::ReducerCohort(metrics) => Some(&metrics.base_head_id),
            Self::HeadEval(report) => report.base_head_id.as_ref(),
        }
    }

    fn merge_window_id(&self) -> Option<&ContentId> {
        match self {
            Self::ReducerCohort(metrics) => Some(&metrics.merge_window_id),
            Self::PeerWindow(_) | Self::HeadEval(_) => None,
        }
    }

    fn revision_key(&self) -> (ExperimentId, RevisionId) {
        (self.experiment_id().clone(), self.revision_id().clone())
    }

    fn retention_kind(&self) -> RetentionKind {
        match self {
            Self::PeerWindow(_) => RetentionKind::PeerWindow,
            Self::ReducerCohort(_) => RetentionKind::ReducerCohort,
            Self::HeadEval(_) => RetentionKind::HeadEval,
        }
    }

    fn captured_at(&self) -> DateTime<Utc> {
        match self {
            Self::PeerWindow(metrics) => metrics.window_finished_at,
            Self::ReducerCohort(metrics) => metrics.captured_at,
            Self::HeadEval(report) => report.finished_at,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RetentionKind {
    PeerWindow,
    ReducerCohort,
    HeadEval,
}

/// Enumerates the derived metrics produced by the read-model service.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DerivedMetricKind {
    /// Accepted tokens or samples per second.
    AcceptedTokensPerSec,
    /// Accepted updates per hour.
    AcceptedUpdatesPerHour,
    /// Approximate epoch progress over accepted work.
    EffectiveEpochProgress,
    /// Approximate unique-sample coverage across accepted work.
    UniqueSampleCoverageEstimate,
    /// Time between canonical head evaluations.
    CanonicalHeadCadence,
    /// Fraction of accepted work that exceeded the stale-work threshold.
    StaleWorkFraction,
    /// Mean head lag across peer-window metrics.
    MeanHeadLag,
    /// Maximum head lag across peer-window metrics.
    MaxHeadLag,
    /// Mean agreement across replicated reducer cohorts.
    ReducerReplicaAgreement,
    /// Merge-window close skew.
    MergeWindowSkew,
    /// Number of distinct candidate branches alive for one base head.
    CandidateBranchFactor,
    /// P50 head adoption lag.
    HeadAdoptionLagP50,
    /// P90 head adoption lag.
    HeadAdoptionLagP90,
    /// Acceptance ratio across attempted and accepted work.
    AcceptanceRatio,
    /// Rejection ratio grouped by reason.
    RejectionRatioByReason,
    /// Bytes transferred per accepted token or sample.
    BytesPerAcceptedToken,
    /// Validation reports per hour.
    ValidationServiceRate,
}

/// One derived metric point materialized by the metrics indexer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DerivedMetricPoint {
    /// Derived metric family.
    pub metric: DerivedMetricKind,
    /// Scope used to interpret the metric.
    pub scope: MetricScope,
    /// Trust class attached to the metric.
    pub trust: MetricTrustClass,
    /// Network identifier for the metric point.
    pub network_id: NetworkId,
    /// Experiment identifier for the metric point.
    pub experiment_id: ExperimentId,
    /// Revision identifier for the metric point.
    pub revision_id: RevisionId,
    /// Workload identifier for the metric point.
    pub workload_id: WorkloadId,
    /// Dataset view identifier for the metric point.
    pub dataset_view_id: DatasetViewId,
    /// Canonical head identifier, when present.
    pub canonical_head_id: Option<HeadId>,
    /// Base head identifier, when present.
    pub base_head_id: Option<HeadId>,
    /// Candidate head identifier, when present.
    pub candidate_head_id: Option<HeadId>,
    /// Merge-window identifier, when present.
    pub merge_window_id: Option<ContentId>,
    /// Reducer-group identifier, when present.
    pub reducer_group_id: Option<ContentId>,
    /// Peer identifier, when present.
    pub peer_id: Option<PeerId>,
    /// Lease identifier, when present.
    pub lease_id: Option<LeaseId>,
    /// Optional label or dimension for the point.
    pub series_label: Option<String>,
    /// Metric value.
    pub value: f64,
    /// Timestamp attached to the materialized point.
    pub captured_at: DateTime<Utc>,
}

/// Peer-window distribution summary grouped by experiment revision and base head.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PeerWindowDistributionSummary {
    /// Network identifier covered by the distribution.
    pub network_id: NetworkId,
    /// Experiment identifier covered by the distribution.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the distribution.
    pub revision_id: RevisionId,
    /// Workload identifier covered by the distribution.
    pub workload_id: WorkloadId,
    /// Dataset view identifier covered by the distribution.
    pub dataset_view_id: DatasetViewId,
    /// Base head used by the summarized peer windows.
    pub base_head_id: HeadId,
    /// Number of peer windows included in the summary.
    pub sample_count: usize,
    /// Number of completed peer windows in the summary.
    pub completed_windows: usize,
    /// Number of peer windows that contributed accepted work.
    pub accepted_windows: usize,
    /// Number of rejected peer windows in the summary.
    pub rejected_windows: usize,
    /// Number of aborted peer windows in the summary.
    pub aborted_windows: usize,
    /// Number of failed peer windows in the summary.
    pub failed_windows: usize,
    /// Total attempted tokens or samples across the peer windows.
    pub attempted_tokens_or_samples: u64,
    /// Total accepted tokens or samples across the peer windows.
    pub accepted_tokens_or_samples: u64,
    /// Acceptance ratio across attempted and accepted work.
    pub acceptance_ratio: f64,
    /// P10 local-train-loss mean when reported.
    pub local_train_loss_mean_p10: Option<f64>,
    /// P50 local-train-loss mean when reported.
    pub local_train_loss_mean_p50: Option<f64>,
    /// P90 local-train-loss mean when reported.
    pub local_train_loss_mean_p90: Option<f64>,
    /// P10 compute time across peer windows.
    pub compute_time_ms_p10: f64,
    /// P50 compute time across peer windows.
    pub compute_time_ms_p50: f64,
    /// P90 compute time across peer windows.
    pub compute_time_ms_p90: f64,
    /// P10 data-fetch latency across peer windows.
    pub data_fetch_time_ms_p10: f64,
    /// P50 data-fetch latency across peer windows.
    pub data_fetch_time_ms_p50: f64,
    /// P90 data-fetch latency across peer windows.
    pub data_fetch_time_ms_p90: f64,
    /// P10 publish latency across peer windows.
    pub publish_latency_ms_p10: f64,
    /// P50 publish latency across peer windows.
    pub publish_latency_ms_p50: f64,
    /// P90 publish latency across peer windows.
    pub publish_latency_ms_p90: f64,
    /// P10 finish-time head lag across peer windows.
    pub head_lag_at_finish_p10: f64,
    /// P50 finish-time head lag across peer windows.
    pub head_lag_at_finish_p50: f64,
    /// P90 finish-time head lag across peer windows.
    pub head_lag_at_finish_p90: f64,
    /// Count of peer windows grouped by runtime role.
    pub role_counts: BTreeMap<String, usize>,
    /// Count of peer windows grouped by backend class.
    pub backend_counts: BTreeMap<String, usize>,
    /// Latest capture timestamp for the summarized peer windows.
    pub captured_at: DateTime<Utc>,
}

/// Drilldown view for one peer-window distribution group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PeerWindowDistributionDetail {
    /// Summary row for the selected experiment, revision, and base head.
    pub summary: PeerWindowDistributionSummary,
    /// Raw peer-window records contributing to that summary.
    pub windows: Vec<PeerWindowMetrics>,
}

/// Configures the in-memory metrics indexer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsIndexerConfig {
    /// Head-lag threshold in steps used to classify stale work.
    pub stale_head_lag_threshold_steps: u64,
    /// Maximum number of raw entries included in one ledger segment.
    pub ledger_segment_entry_limit: usize,
    /// Maximum retained peer-window envelopes per experiment revision.
    pub max_peer_window_entries_per_revision: usize,
    /// Maximum retained reducer cohort envelopes per experiment revision.
    pub max_reducer_cohort_entries_per_revision: usize,
    /// Maximum retained head-eval envelopes per experiment revision.
    pub max_head_eval_reports_per_revision: usize,
    /// Maximum recent revisions with raw metrics detail retained per experiment.
    pub max_revisions_per_experiment: usize,
    /// Maximum raw peer windows returned by one detail drilldown.
    pub max_peer_window_detail_windows: usize,
}

impl Default for MetricsIndexerConfig {
    fn default() -> Self {
        Self {
            stale_head_lag_threshold_steps: 4,
            ledger_segment_entry_limit: 128,
            max_peer_window_entries_per_revision: 256,
            max_reducer_cohort_entries_per_revision: 128,
            max_head_eval_reports_per_revision: 192,
            max_revisions_per_experiment: 8,
            max_peer_window_detail_windows: 64,
        }
    }
}

/// Materialized snapshot exported by the metrics indexer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Snapshot manifest for the export.
    pub manifest: MetricsSnapshotManifest,
    /// Canonical and candidate head reports included by the snapshot.
    pub head_eval_reports: Vec<HeadEvalReport>,
    /// Peer-window metrics included by the snapshot.
    pub peer_window_metrics: Vec<PeerWindowMetrics>,
    /// Reducer cohort metrics included by the snapshot.
    pub reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    /// Derived metric rollups included by the snapshot.
    pub derived_metrics: Vec<DerivedMetricPoint>,
}

/// Catchup bundle that seeds a peer or dashboard from one point-in-time metrics snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricsCatchupBundle {
    /// Network identifier covered by the bundle.
    pub network_id: NetworkId,
    /// Experiment identifier covered by the bundle.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the bundle.
    pub revision_id: RevisionId,
    /// Latest snapshot material for the revision.
    pub snapshot: MetricsSnapshot,
    /// Ledger segments after or alongside the snapshot.
    pub ledger_segments: Vec<MetricsLedgerSegment>,
    /// Time when the bundle was generated.
    pub generated_at: DateTime<Utc>,
}

/// Errors raised by the durable metrics store.
#[derive(Debug, Error)]
pub enum MetricsStoreError {
    /// Filesystem access failed.
    #[error("metrics store I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization or deserialization failed.
    #[error("metrics store JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// Content-addressing or schema derivation failed.
    #[error("metrics store schema failed: {0}")]
    Schema(#[from] burn_p2p_core::SchemaError),
}

/// Durable file-backed metrics store used by read-model services.
#[derive(Clone, Debug)]
pub struct MetricsStore {
    root_dir: PathBuf,
    indexer: MetricsIndexer,
}

/// In-memory metrics read model for one or more experiment revisions.
#[derive(Clone, Debug, Default)]
pub struct MetricsIndexer {
    config: MetricsIndexerConfig,
    entries: Vec<MetricEnvelope>,
}

impl MetricsIndexer {
    /// Creates a new metrics indexer.
    pub fn new(config: MetricsIndexerConfig) -> Self {
        Self {
            config,
            entries: Vec::new(),
        }
    }

    /// Consumes the indexer and returns the retained raw envelopes.
    pub fn into_entries(self) -> Vec<MetricEnvelope> {
        self.entries
    }

    /// Ingests a peer-window metric envelope.
    pub fn ingest_peer_window_metrics(&mut self, metrics: PeerWindowMetrics) {
        self.entries.push(MetricEnvelope::PeerWindow(metrics));
        self.enforce_retention();
    }

    /// Ingests a reducer cohort metric envelope.
    pub fn ingest_reducer_cohort_metrics(&mut self, metrics: ReducerCohortMetrics) {
        self.entries.push(MetricEnvelope::ReducerCohort(metrics));
        self.enforce_retention();
    }

    /// Ingests a head evaluation report envelope.
    pub fn ingest_head_eval_report(&mut self, report: HeadEvalReport) {
        self.entries.push(MetricEnvelope::HeadEval(report));
        self.enforce_retention();
    }

    fn enforce_retention(&mut self) {
        if self.entries.is_empty() {
            return;
        }

        let mut grouped = BTreeMap::<
            (ExperimentId, RevisionId, RetentionKind),
            Vec<(usize, DateTime<Utc>)>,
        >::new();
        for (index, entry) in self.entries.iter().enumerate() {
            let (experiment_id, revision_id) = entry.revision_key();
            grouped
                .entry((experiment_id, revision_id, entry.retention_kind()))
                .or_default()
                .push((index, entry.captured_at()));
        }

        let mut keep = vec![false; self.entries.len()];
        for ((_, _, kind), mut indices) in grouped {
            indices.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
            let limit = match kind {
                RetentionKind::PeerWindow => self.config.max_peer_window_entries_per_revision,
                RetentionKind::ReducerCohort => self.config.max_reducer_cohort_entries_per_revision,
                RetentionKind::HeadEval => self.config.max_head_eval_reports_per_revision,
            };
            for (index, _) in indices.into_iter().take(limit) {
                keep[index] = true;
            }
        }

        let retained_entries = self
            .entries
            .drain(..)
            .enumerate()
            .filter_map(|(index, entry)| keep[index].then_some(entry))
            .collect::<Vec<_>>();

        let mut revision_latest = BTreeMap::<(ExperimentId, RevisionId), DateTime<Utc>>::new();
        for entry in &retained_entries {
            revision_latest
                .entry(entry.revision_key())
                .and_modify(|existing| {
                    if entry.captured_at() > *existing {
                        *existing = entry.captured_at();
                    }
                })
                .or_insert(entry.captured_at());
        }

        let mut revisions_by_experiment =
            BTreeMap::<ExperimentId, Vec<(RevisionId, DateTime<Utc>)>>::new();
        for ((experiment_id, revision_id), captured_at) in revision_latest {
            revisions_by_experiment
                .entry(experiment_id)
                .or_default()
                .push((revision_id, captured_at));
        }

        let mut retained_revision_keys = BTreeSet::<(ExperimentId, RevisionId)>::new();
        for (experiment_id, mut revisions) in revisions_by_experiment {
            revisions
                .sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
            for (revision_id, _) in revisions
                .into_iter()
                .take(self.config.max_revisions_per_experiment.max(1))
            {
                retained_revision_keys.insert((experiment_id.clone(), revision_id));
            }
        }

        self.entries = retained_entries
            .into_iter()
            .filter(|entry| retained_revision_keys.contains(&entry.revision_key()))
            .collect();
    }

    /// Returns the latest canonical head evaluation report for an experiment revision.
    pub fn latest_canonical_head_report(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
    ) -> Option<HeadEvalReport> {
        self.head_eval_reports(experiment_id, revision_id)
            .into_iter()
            .filter(|report| {
                report.status == HeadEvalStatus::Completed
                    && report.trust_class == MetricTrustClass::Canonical
            })
            .max_by_key(|report| report.finished_at)
    }

    /// Materializes derived metrics for one experiment revision.
    pub fn derive_metrics(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
    ) -> Vec<DerivedMetricPoint> {
        let peer_windows = self.peer_window_metrics(experiment_id, revision_id);
        let reducer_cohorts = self.reducer_cohort_metrics(experiment_id, revision_id);
        let head_reports = self.head_eval_reports(experiment_id, revision_id);

        let context = metric_context(&peer_windows, &reducer_cohorts, &head_reports);
        let Some(context) = context else {
            return Vec::new();
        };

        let mut points = Vec::new();
        let accepted_tokens: u64 = peer_windows
            .iter()
            .filter_map(|metrics| metrics.accepted_tokens_or_samples)
            .sum();
        let attempted_tokens: u64 = peer_windows
            .iter()
            .map(|metrics| metrics.attempted_tokens_or_samples)
            .sum();
        let accepted_updates = peer_windows
            .iter()
            .filter(|metrics| metrics.status == PeerWindowStatus::Completed)
            .count() as u64;

        if let Some(elapsed_seconds) = elapsed_seconds_from_peer_windows(&peer_windows)
            && elapsed_seconds > 0.0
        {
            points.push(context.point(
                DerivedMetricKind::AcceptedTokensPerSec,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                accepted_tokens as f64 / elapsed_seconds,
            ));
            points.push(context.point(
                DerivedMetricKind::AcceptedUpdatesPerHour,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                accepted_updates as f64 * 3600.0 / elapsed_seconds,
            ));
        }

        if attempted_tokens > 0 {
            let ratio = accepted_tokens as f64 / attempted_tokens as f64;
            points.push(context.point(
                DerivedMetricKind::AcceptanceRatio,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                ratio,
            ));
            points.push(context.point(
                DerivedMetricKind::EffectiveEpochProgress,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                ratio,
            ));
            points.push(context.point(
                DerivedMetricKind::UniqueSampleCoverageEstimate,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                ratio.min(1.0),
            ));
        }

        if !peer_windows.is_empty() {
            let head_lags = peer_windows
                .iter()
                .map(|metrics| metrics.head_lag_at_finish as f64)
                .collect::<Vec<_>>();
            let mean_head_lag = head_lags.iter().sum::<f64>() / head_lags.len() as f64;
            let max_head_lag = head_lags.into_iter().fold(0.0_f64, f64::max);
            let stale_work = peer_windows
                .iter()
                .filter(|metrics| {
                    metrics.accepted_tokens_or_samples.unwrap_or(0) > 0
                        && metrics.head_lag_at_finish > self.config.stale_head_lag_threshold_steps
                })
                .count() as f64
                / peer_windows
                    .iter()
                    .filter(|metrics| metrics.accepted_tokens_or_samples.unwrap_or(0) > 0)
                    .count()
                    .max(1) as f64;
            points.push(context.point(
                DerivedMetricKind::MeanHeadLag,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                mean_head_lag,
            ));
            points.push(context.point(
                DerivedMetricKind::MaxHeadLag,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                max_head_lag,
            ));
            points.push(context.point(
                DerivedMetricKind::StaleWorkFraction,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                stale_work,
            ));
        }

        if !reducer_cohorts.is_empty() {
            let replica_values = reducer_cohorts
                .iter()
                .filter_map(|metrics| metrics.replica_agreement)
                .collect::<Vec<_>>();
            if !replica_values.is_empty() {
                let mean_agreement =
                    replica_values.iter().sum::<f64>() / replica_values.len() as f64;
                points.push(context.point(
                    DerivedMetricKind::ReducerReplicaAgreement,
                    MetricScope::Network,
                    MetricTrustClass::Derived,
                    None,
                    mean_agreement,
                ));
            }

            let merge_window_skew = reducer_cohorts
                .iter()
                .map(|metrics| metrics.window_close_delay_ms as f64)
                .fold(0.0_f64, f64::max);
            points.push(context.point(
                DerivedMetricKind::MergeWindowSkew,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                merge_window_skew,
            ));

            let candidate_branch_factor = reducer_cohorts
                .iter()
                .fold(
                    BTreeMap::<HeadId, BTreeSet<HeadId>>::new(),
                    |mut acc, metrics| {
                        if let Some(candidate_head_id) = metrics.candidate_head_id.as_ref() {
                            acc.entry(metrics.base_head_id.clone())
                                .or_default()
                                .insert(candidate_head_id.clone());
                        }
                        acc
                    },
                )
                .into_values()
                .map(|candidates| candidates.len() as f64)
                .fold(0.0_f64, f64::max);
            points.push(context.point(
                DerivedMetricKind::CandidateBranchFactor,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                candidate_branch_factor,
            ));

            let total_reducer_bytes = reducer_cohorts.iter().fold(0_u128, |sum, metrics| {
                sum + metrics.ingress_bytes + metrics.egress_bytes
            });
            let total_reducer_tokens = reducer_cohorts
                .iter()
                .map(|metrics| metrics.accepted_tokens_or_samples)
                .sum::<u64>();
            if total_reducer_bytes > 0 && total_reducer_tokens > 0 {
                points.push(context.point(
                    DerivedMetricKind::BytesPerAcceptedToken,
                    MetricScope::Network,
                    MetricTrustClass::Derived,
                    None,
                    total_reducer_bytes as f64 / total_reducer_tokens as f64,
                ));
            }

            let total_rejected: u64 = reducer_cohorts
                .iter()
                .map(|metrics| metrics.rejected_updates)
                .sum();
            if total_rejected > 0 {
                let mut by_reason = BTreeMap::<String, u64>::new();
                for metrics in &reducer_cohorts {
                    for (reason, count) in &metrics.rejection_reasons {
                        *by_reason.entry(reason.clone()).or_default() += *count;
                    }
                }
                for (reason, count) in by_reason {
                    points.push(context.point(
                        DerivedMetricKind::RejectionRatioByReason,
                        MetricScope::Network,
                        MetricTrustClass::Derived,
                        Some(reason),
                        count as f64 / total_rejected as f64,
                    ));
                }
            }
        }

        let mut canonical_reports = head_reports
            .iter()
            .filter(|report| {
                report.status == HeadEvalStatus::Completed
                    && report.trust_class == MetricTrustClass::Canonical
            })
            .collect::<Vec<_>>();
        canonical_reports.sort_by_key(|report| report.finished_at);
        let canonical_finish_by_head = canonical_reports.iter().fold(
            BTreeMap::<HeadId, DateTime<Utc>>::new(),
            |mut acc, report| {
                acc.entry(report.head_id.clone())
                    .and_modify(|captured_at| {
                        if report.finished_at > *captured_at {
                            *captured_at = report.finished_at;
                        }
                    })
                    .or_insert(report.finished_at);
                acc
            },
        );
        let mut adoption_lags_ms = peer_windows
            .iter()
            .filter_map(|metrics| {
                canonical_finish_by_head
                    .get(&metrics.base_head_id)
                    .map(|certified_at| {
                        (metrics.window_started_at - *certified_at)
                            .num_milliseconds()
                            .max(0) as f64
                    })
            })
            .collect::<Vec<_>>();
        if !adoption_lags_ms.is_empty() {
            adoption_lags_ms.sort_by(f64::total_cmp);
            points.push(context.point(
                DerivedMetricKind::HeadAdoptionLagP50,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                percentile_nearest_rank(&adoption_lags_ms, 0.50),
            ));
            points.push(context.point(
                DerivedMetricKind::HeadAdoptionLagP90,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                percentile_nearest_rank(&adoption_lags_ms, 0.90),
            ));
        }
        if canonical_reports.len() >= 2 {
            let earliest = canonical_reports
                .first()
                .expect("canonical reports")
                .finished_at;
            let latest = canonical_reports
                .last()
                .expect("canonical reports")
                .finished_at;
            let elapsed = (latest - earliest).num_milliseconds().max(1) as f64 / 1000.0;
            let cadence = elapsed / (canonical_reports.len() - 1) as f64;
            points.push(context.point(
                DerivedMetricKind::CanonicalHeadCadence,
                MetricScope::Head,
                MetricTrustClass::Derived,
                None,
                cadence,
            ));
        }
        if let Some(elapsed_seconds) = elapsed_seconds_from_head_reports(&head_reports)
            && elapsed_seconds > 0.0
        {
            let completed_reports = head_reports
                .iter()
                .filter(|report| report.status == HeadEvalStatus::Completed)
                .count() as f64;
            points.push(context.point(
                DerivedMetricKind::ValidationServiceRate,
                MetricScope::Network,
                MetricTrustClass::Derived,
                None,
                completed_reports * 3600.0 / elapsed_seconds,
            ));
        }

        points
    }

    /// Derives peer-window distribution summaries for one experiment revision.
    pub fn derive_peer_window_distributions(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
    ) -> Vec<PeerWindowDistributionSummary> {
        derive_peer_window_distribution_summaries(
            &self.peer_window_metrics(experiment_id, revision_id),
        )
    }

    /// Exports one metrics snapshot for the selected experiment revision.
    pub fn export_snapshot(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        snapshot_seq: u64,
        leaderboard_ref: Option<ContentId>,
        signatures: Vec<SignatureMetadata>,
    ) -> Result<MetricsSnapshot, burn_p2p_core::SchemaError> {
        let peer_window_metrics = self.peer_window_metrics(experiment_id, revision_id);
        let reducer_cohort_metrics = self.reducer_cohort_metrics(experiment_id, revision_id);
        let head_eval_reports = self.head_eval_reports(experiment_id, revision_id);
        let derived_metrics = self.derive_metrics(experiment_id, revision_id);

        let network_id = peer_window_metrics
            .first()
            .map(|metrics| metrics.network_id.clone())
            .or_else(|| {
                reducer_cohort_metrics
                    .first()
                    .map(|metrics| metrics.network_id.clone())
            })
            .or_else(|| {
                head_eval_reports
                    .first()
                    .map(|report| report.network_id.clone())
            })
            .expect("snapshot export requires at least one metric entry");

        let canonical_head_metrics_ref = ContentId::derive(&head_eval_reports)?;
        let window_rollups_ref =
            ContentId::derive(&(peer_window_metrics.clone(), reducer_cohort_metrics.clone()))?;
        let network_rollups_ref = ContentId::derive(&derived_metrics)?;
        let covers_until_head_id = head_eval_reports
            .iter()
            .max_by_key(|report| report.finished_at)
            .map(|report| report.head_id.clone());
        let covers_until_merge_window_id = reducer_cohort_metrics
            .iter()
            .max_by_key(|metrics| metrics.captured_at)
            .map(|metrics| metrics.merge_window_id.clone());
        let manifest = MetricsSnapshotManifest {
            network_id,
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            snapshot_seq,
            covers_until_head_id,
            covers_until_merge_window_id,
            canonical_head_metrics_ref,
            window_rollups_ref,
            network_rollups_ref,
            leaderboard_ref,
            created_at: Utc::now(),
            signatures,
        };

        Ok(MetricsSnapshot {
            manifest,
            head_eval_reports,
            peer_window_metrics,
            reducer_cohort_metrics,
            derived_metrics,
        })
    }

    /// Exports append-only ledger segments for one experiment revision.
    pub fn export_ledger_segments(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
    ) -> Result<Vec<MetricsLedgerSegment>, burn_p2p_core::SchemaError> {
        let filtered = self
            .entries
            .iter()
            .filter(|entry| {
                entry.experiment_id() == experiment_id && entry.revision_id() == revision_id
            })
            .cloned()
            .collect::<Vec<_>>();
        if filtered.is_empty() {
            return Ok(Vec::new());
        }

        let mut segments = Vec::new();
        let mut prev_hash = None;
        for (index, chunk) in filtered
            .chunks(self.config.ledger_segment_entry_limit.max(1))
            .enumerate()
        {
            let entries_ref = ContentId::derive(&chunk)?;
            let segment_hash = ContentId::derive(&(
                experiment_id.as_str(),
                revision_id.as_str(),
                index as u64,
                &entries_ref,
                &prev_hash,
            ))?;
            let segment = MetricsLedgerSegment {
                network_id: chunk
                    .first()
                    .expect("chunk should not be empty")
                    .network_id()
                    .clone(),
                experiment_id: experiment_id.clone(),
                revision_id: revision_id.clone(),
                segment_seq: index as u64,
                from_head_id: chunk
                    .first()
                    .and_then(MetricEnvelope::base_head_id)
                    .cloned(),
                to_head_id: chunk.last().and_then(MetricEnvelope::base_head_id).cloned(),
                from_window_id: chunk
                    .first()
                    .and_then(MetricEnvelope::merge_window_id)
                    .cloned(),
                to_window_id: chunk
                    .last()
                    .and_then(MetricEnvelope::merge_window_id)
                    .cloned(),
                entries_ref,
                hash: segment_hash.clone(),
                prev_hash: prev_hash.clone(),
            };
            prev_hash = Some(segment_hash);
            segments.push(segment);
        }
        Ok(segments)
    }

    fn peer_window_metrics(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
    ) -> Vec<PeerWindowMetrics> {
        self.entries
            .iter()
            .filter_map(|entry| match entry {
                MetricEnvelope::PeerWindow(metrics)
                    if &metrics.experiment_id == experiment_id
                        && &metrics.revision_id == revision_id =>
                {
                    Some(metrics.clone())
                }
                _ => None,
            })
            .collect()
    }

    fn reducer_cohort_metrics(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
    ) -> Vec<ReducerCohortMetrics> {
        self.entries
            .iter()
            .filter_map(|entry| match entry {
                MetricEnvelope::ReducerCohort(metrics)
                    if &metrics.experiment_id == experiment_id
                        && &metrics.revision_id == revision_id =>
                {
                    Some(metrics.clone())
                }
                _ => None,
            })
            .collect()
    }

    fn head_eval_reports(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
    ) -> Vec<HeadEvalReport> {
        self.entries
            .iter()
            .filter_map(|entry| match entry {
                MetricEnvelope::HeadEval(report)
                    if &report.experiment_id == experiment_id
                        && &report.revision_id == revision_id =>
                {
                    Some(report.clone())
                }
                _ => None,
            })
            .collect()
    }
}

impl MetricsStore {
    /// Opens or creates a durable metrics store rooted at the provided path.
    pub fn open(
        root_dir: impl AsRef<Path>,
        config: MetricsIndexerConfig,
    ) -> Result<Self, MetricsStoreError> {
        let root_dir = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(root_dir.join("snapshots"))?;
        fs::create_dir_all(root_dir.join("ledger"))?;

        let mut indexer = MetricsIndexer::new(config);
        let entries_path = root_dir.join("entries.jsonl");
        let mut loaded_entries = 0usize;
        if entries_path.exists() {
            let reader = BufReader::new(fs::File::open(&entries_path)?);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                loaded_entries += 1;
                match serde_json::from_str::<MetricEnvelope>(&line)? {
                    MetricEnvelope::PeerWindow(metrics) => {
                        indexer.ingest_peer_window_metrics(metrics)
                    }
                    MetricEnvelope::ReducerCohort(metrics) => {
                        indexer.ingest_reducer_cohort_metrics(metrics)
                    }
                    MetricEnvelope::HeadEval(report) => indexer.ingest_head_eval_report(report),
                }
            }
        }

        let store = Self { root_dir, indexer };
        if loaded_entries != store.indexer.entries.len() {
            store.persist_entries_file()?;
            store.materialize_views()?;
        }

        Ok(store)
    }

    /// Replaces the raw metric envelopes stored on disk and in memory.
    pub fn replace_entries(
        &mut self,
        entries: impl IntoIterator<Item = MetricEnvelope>,
    ) -> Result<(), MetricsStoreError> {
        let entries = entries.into_iter().collect::<Vec<_>>();
        self.indexer = MetricsIndexer::new(self.indexer.config.clone());
        for entry in &entries {
            match entry {
                MetricEnvelope::PeerWindow(metrics) => {
                    self.indexer.ingest_peer_window_metrics(metrics.clone())
                }
                MetricEnvelope::ReducerCohort(metrics) => {
                    self.indexer.ingest_reducer_cohort_metrics(metrics.clone())
                }
                MetricEnvelope::HeadEval(report) => {
                    self.indexer.ingest_head_eval_report(report.clone())
                }
            }
        }

        self.persist_entries_file()?;
        Ok(())
    }

    /// Rebuilds the durable store from metric records held by the caller.
    pub fn sync_from_records(
        &mut self,
        peer_window_metrics: &[PeerWindowMetrics],
        reducer_cohort_metrics: &[ReducerCohortMetrics],
        head_eval_reports: &[HeadEvalReport],
    ) -> Result<(), MetricsStoreError> {
        let entries = peer_window_metrics
            .iter()
            .cloned()
            .map(MetricEnvelope::PeerWindow)
            .chain(
                reducer_cohort_metrics
                    .iter()
                    .cloned()
                    .map(MetricEnvelope::ReducerCohort),
            )
            .chain(
                head_eval_reports
                    .iter()
                    .cloned()
                    .map(MetricEnvelope::HeadEval),
            )
            .collect::<Vec<_>>();
        self.replace_entries(entries)?;
        self.materialize_views()?;
        Ok(())
    }

    /// Materializes per-revision snapshots and ledger caches to disk.
    pub fn materialize_views(&self) -> Result<(), MetricsStoreError> {
        let snapshot_dir = self.root_dir.join("snapshots");
        let ledger_dir = self.root_dir.join("ledger");
        fs::create_dir_all(&snapshot_dir)?;
        fs::create_dir_all(&ledger_dir)?;

        let revisions = self
            .indexer
            .entries
            .iter()
            .map(|entry| (entry.experiment_id().clone(), entry.revision_id().clone()))
            .collect::<BTreeSet<_>>();

        let mut live_snapshot_paths = BTreeSet::new();
        let mut live_ledger_paths = BTreeSet::new();

        for (experiment_id, revision_id) in revisions {
            let snapshot_seq = self
                .indexer
                .entries
                .iter()
                .filter(|entry| {
                    entry.experiment_id() == &experiment_id && entry.revision_id() == &revision_id
                })
                .count() as u64;
            let snapshot = self.indexer.export_snapshot(
                &experiment_id,
                &revision_id,
                snapshot_seq,
                None,
                Vec::new(),
            )?;
            let ledger = self
                .indexer
                .export_ledger_segments(&experiment_id, &revision_id)?;

            let snapshot_path = self.snapshot_path(&experiment_id, &revision_id);
            let ledger_path = self.ledger_path(&experiment_id, &revision_id);
            serde_json::to_writer_pretty(
                BufWriter::new(fs::File::create(&snapshot_path)?),
                &snapshot,
            )?;
            serde_json::to_writer_pretty(BufWriter::new(fs::File::create(&ledger_path)?), &ledger)?;
            live_snapshot_paths.insert(snapshot_path);
            live_ledger_paths.insert(ledger_path);
        }

        prune_unknown_files(&snapshot_dir, &live_snapshot_paths)?;
        prune_unknown_files(&ledger_dir, &live_ledger_paths)?;
        Ok(())
    }

    /// Loads materialized metrics snapshots from disk.
    pub fn load_snapshots(&self) -> Result<Vec<MetricsSnapshot>, MetricsStoreError> {
        let snapshot_dir = self.root_dir.join("snapshots");
        let mut paths = read_json_paths(&snapshot_dir)?;
        if paths.is_empty() && !self.indexer.entries.is_empty() {
            self.materialize_views()?;
            paths = read_json_paths(&snapshot_dir)?;
        }
        paths.sort();
        let mut snapshots = paths
            .into_iter()
            .map(|path| {
                serde_json::from_reader::<_, MetricsSnapshot>(BufReader::new(fs::File::open(path)?))
                    .map_err(MetricsStoreError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        snapshots.sort_by(|left, right| {
            left.manifest
                .experiment_id
                .cmp(&right.manifest.experiment_id)
                .then_with(|| left.manifest.revision_id.cmp(&right.manifest.revision_id))
                .then_with(|| left.manifest.snapshot_seq.cmp(&right.manifest.snapshot_seq))
        });
        Ok(snapshots)
    }

    /// Loads materialized metrics snapshots for one experiment.
    pub fn load_snapshots_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<MetricsSnapshot>, MetricsStoreError> {
        Ok(self
            .load_snapshots()?
            .into_iter()
            .filter(|snapshot| &snapshot.manifest.experiment_id == experiment_id)
            .collect())
    }

    /// Loads materialized metrics ledger segments from disk.
    pub fn load_ledger_segments(&self) -> Result<Vec<MetricsLedgerSegment>, MetricsStoreError> {
        let ledger_dir = self.root_dir.join("ledger");
        let mut paths = read_json_paths(&ledger_dir)?;
        if paths.is_empty() && !self.indexer.entries.is_empty() {
            self.materialize_views()?;
            paths = read_json_paths(&ledger_dir)?;
        }
        paths.sort();
        let mut segments = Vec::new();
        for path in paths {
            segments.extend(serde_json::from_reader::<_, Vec<MetricsLedgerSegment>>(
                BufReader::new(fs::File::open(path)?),
            )?);
        }
        segments.sort_by(|left, right| {
            left.experiment_id
                .cmp(&right.experiment_id)
                .then_with(|| left.revision_id.cmp(&right.revision_id))
                .then_with(|| left.segment_seq.cmp(&right.segment_seq))
        });
        Ok(segments)
    }

    /// Loads materialized catchup bundles across all persisted experiment revisions.
    pub fn load_catchup_bundles(&self) -> Result<Vec<MetricsCatchupBundle>, MetricsStoreError> {
        Ok(build_catchup_bundles(
            self.load_snapshots()?,
            self.load_ledger_segments()?,
        ))
    }

    /// Loads materialized catchup bundles for one experiment.
    pub fn load_catchup_bundles_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<MetricsCatchupBundle>, MetricsStoreError> {
        Ok(self
            .load_catchup_bundles()?
            .into_iter()
            .filter(|bundle| &bundle.experiment_id == experiment_id)
            .collect())
    }

    /// Loads the current live metrics refresh event derived from persisted views.
    pub fn load_live_event(
        &self,
        kind: MetricsLiveEventKind,
    ) -> Result<Option<MetricsLiveEvent>, MetricsStoreError> {
        Ok(build_live_event(
            kind,
            &self.load_snapshots()?,
            &self.load_ledger_segments()?,
        ))
    }

    /// Loads head evaluation reports for one head from the durable store.
    pub fn load_head_eval_reports(
        &self,
        head_id: &HeadId,
    ) -> Result<Vec<HeadEvalReport>, MetricsStoreError> {
        Ok(self
            .indexer
            .entries
            .iter()
            .filter_map(|entry| match entry {
                MetricEnvelope::HeadEval(report) if &report.head_id == head_id => {
                    Some(report.clone())
                }
                _ => None,
            })
            .collect())
    }

    /// Loads the raw metric envelopes held by the durable store.
    pub fn load_entries(&self) -> Vec<MetricEnvelope> {
        self.indexer.entries.clone()
    }

    /// Loads peer-window distribution summaries across all persisted experiment revisions.
    pub fn load_peer_window_distributions(
        &self,
    ) -> Result<Vec<PeerWindowDistributionSummary>, MetricsStoreError> {
        Ok(derive_peer_window_distribution_summaries(
            &self
                .indexer
                .entries
                .iter()
                .filter_map(|entry| match entry {
                    MetricEnvelope::PeerWindow(metrics) => Some(metrics.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))
    }

    /// Loads peer-window distribution summaries for one experiment.
    pub fn load_peer_window_distributions_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<PeerWindowDistributionSummary>, MetricsStoreError> {
        Ok(self
            .load_peer_window_distributions()?
            .into_iter()
            .filter(|summary| &summary.experiment_id == experiment_id)
            .collect())
    }

    /// Loads one peer-window distribution drilldown for an experiment revision and base head.
    pub fn load_peer_window_distribution_detail(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        base_head_id: &HeadId,
    ) -> Result<Option<PeerWindowDistributionDetail>, MetricsStoreError> {
        Ok(derive_peer_window_distribution_detail_with_limit(
            &self
                .indexer
                .entries
                .iter()
                .filter_map(|entry| match entry {
                    MetricEnvelope::PeerWindow(metrics) => Some(metrics.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            experiment_id,
            revision_id,
            base_head_id,
            self.indexer.config.max_peer_window_detail_windows,
        ))
    }

    fn snapshot_path(&self, experiment_id: &ExperimentId, revision_id: &RevisionId) -> PathBuf {
        self.root_dir.join("snapshots").join(format!(
            "{}--{}.json",
            experiment_id.as_str(),
            revision_id.as_str()
        ))
    }

    fn persist_entries_file(&self) -> Result<(), MetricsStoreError> {
        let entries_path = self.root_dir.join("entries.jsonl");
        let mut writer = BufWriter::new(fs::File::create(entries_path)?);
        for entry in &self.indexer.entries {
            serde_json::to_writer(&mut writer, entry)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        Ok(())
    }

    fn ledger_path(&self, experiment_id: &ExperimentId, revision_id: &RevisionId) -> PathBuf {
        self.root_dir.join("ledger").join(format!(
            "{}--{}.json",
            experiment_id.as_str(),
            revision_id.as_str()
        ))
    }
}

fn prune_unknown_files(
    dir: &Path,
    live_paths: &BTreeSet<PathBuf>,
) -> Result<(), MetricsStoreError> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        if !live_paths.contains(&path) {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

fn read_json_paths(dir: &Path) -> Result<Vec<PathBuf>, MetricsStoreError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    Ok(paths)
}

/// Builds per-revision catchup bundles from materialized snapshots and ledger segments.
pub fn build_catchup_bundles(
    snapshots: Vec<MetricsSnapshot>,
    ledger_segments: Vec<MetricsLedgerSegment>,
) -> Vec<MetricsCatchupBundle> {
    snapshots
        .into_iter()
        .map(|snapshot| {
            let experiment_id = snapshot.manifest.experiment_id.clone();
            let revision_id = snapshot.manifest.revision_id.clone();
            let network_id = snapshot.manifest.network_id.clone();
            let segments = ledger_segments
                .iter()
                .filter(|segment| {
                    segment.experiment_id == experiment_id && segment.revision_id == revision_id
                })
                .cloned()
                .collect();
            MetricsCatchupBundle {
                network_id,
                experiment_id,
                revision_id,
                generated_at: snapshot.manifest.created_at,
                snapshot,
                ledger_segments: segments,
            }
        })
        .collect()
}

/// Builds one live metrics refresh event from the currently materialized views.
pub fn build_live_event(
    kind: MetricsLiveEventKind,
    snapshots: &[MetricsSnapshot],
    ledger_segments: &[MetricsLedgerSegment],
) -> Option<MetricsLiveEvent> {
    let network_id = snapshots
        .first()
        .map(|snapshot| snapshot.manifest.network_id.clone())
        .or_else(|| {
            ledger_segments
                .first()
                .map(|segment| segment.network_id.clone())
        })?;

    let mut cursors = snapshots
        .iter()
        .map(|snapshot| {
            let latest_ledger_segment_seq = ledger_segments
                .iter()
                .filter(|segment| {
                    segment.experiment_id == snapshot.manifest.experiment_id
                        && segment.revision_id == snapshot.manifest.revision_id
                })
                .map(|segment| segment.segment_seq)
                .max();
            MetricsSyncCursor {
                experiment_id: snapshot.manifest.experiment_id.clone(),
                revision_id: snapshot.manifest.revision_id.clone(),
                latest_snapshot_seq: Some(snapshot.manifest.snapshot_seq),
                latest_ledger_segment_seq,
                latest_head_id: snapshot.manifest.covers_until_head_id.clone(),
                latest_merge_window_id: snapshot.manifest.covers_until_merge_window_id.clone(),
            }
        })
        .collect::<Vec<_>>();
    cursors.sort_by(|left, right| {
        left.experiment_id
            .cmp(&right.experiment_id)
            .then_with(|| left.revision_id.cmp(&right.revision_id))
    });

    Some(MetricsLiveEvent {
        network_id,
        kind,
        cursors,
        generated_at: Utc::now(),
    })
}

/// Derives peer-window distribution summaries across one or more experiment revisions.
pub fn derive_peer_window_distribution_summaries(
    peer_windows: &[PeerWindowMetrics],
) -> Vec<PeerWindowDistributionSummary> {
    let mut grouped = peer_windows.iter().fold(
        BTreeMap::<
            (ExperimentId, RevisionId, WorkloadId, DatasetViewId, HeadId),
            Vec<PeerWindowMetrics>,
        >::new(),
        |mut acc, metrics| {
            acc.entry((
                metrics.experiment_id.clone(),
                metrics.revision_id.clone(),
                metrics.workload_id.clone(),
                metrics.dataset_view_id.clone(),
                metrics.base_head_id.clone(),
            ))
            .or_default()
            .push(metrics.clone());
            acc
        },
    );

    let mut summaries = grouped
        .values_mut()
        .filter_map(|metrics| summarize_peer_window_distribution(metrics))
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| {
        right
            .captured_at
            .cmp(&left.captured_at)
            .then_with(|| left.experiment_id.cmp(&right.experiment_id))
            .then_with(|| left.revision_id.cmp(&right.revision_id))
            .then_with(|| left.base_head_id.cmp(&right.base_head_id))
    });
    summaries
}

/// Derives one peer-window distribution drilldown for an experiment revision and base head.
pub fn derive_peer_window_distribution_detail(
    peer_windows: &[PeerWindowMetrics],
    experiment_id: &ExperimentId,
    revision_id: &RevisionId,
    base_head_id: &HeadId,
) -> Option<PeerWindowDistributionDetail> {
    derive_peer_window_distribution_detail_with_limit(
        peer_windows,
        experiment_id,
        revision_id,
        base_head_id,
        usize::MAX,
    )
}

/// Derives one peer-window distribution drilldown with an explicit raw-window cap.
pub fn derive_peer_window_distribution_detail_with_limit(
    peer_windows: &[PeerWindowMetrics],
    experiment_id: &ExperimentId,
    revision_id: &RevisionId,
    base_head_id: &HeadId,
    max_windows: usize,
) -> Option<PeerWindowDistributionDetail> {
    let mut filtered = peer_windows
        .iter()
        .filter(|metrics| {
            &metrics.experiment_id == experiment_id
                && &metrics.revision_id == revision_id
                && &metrics.base_head_id == base_head_id
        })
        .cloned()
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        return None;
    }
    filtered.sort_by(|left, right| {
        right
            .window_finished_at
            .cmp(&left.window_finished_at)
            .then_with(|| left.peer_id.cmp(&right.peer_id))
    });
    let summary = summarize_peer_window_distribution(&filtered)?;
    filtered.truncate(max_windows);
    Some(PeerWindowDistributionDetail {
        summary,
        windows: filtered,
    })
}

#[derive(Clone)]
struct MetricPointContext {
    network_id: NetworkId,
    experiment_id: ExperimentId,
    revision_id: RevisionId,
    workload_id: WorkloadId,
    dataset_view_id: DatasetViewId,
    canonical_head_id: Option<HeadId>,
    captured_at: DateTime<Utc>,
}

impl MetricPointContext {
    fn point(
        &self,
        metric: DerivedMetricKind,
        scope: MetricScope,
        trust: MetricTrustClass,
        series_label: Option<String>,
        value: f64,
    ) -> DerivedMetricPoint {
        DerivedMetricPoint {
            metric,
            scope,
            trust,
            network_id: self.network_id.clone(),
            experiment_id: self.experiment_id.clone(),
            revision_id: self.revision_id.clone(),
            workload_id: self.workload_id.clone(),
            dataset_view_id: self.dataset_view_id.clone(),
            canonical_head_id: self.canonical_head_id.clone(),
            base_head_id: None,
            candidate_head_id: None,
            merge_window_id: None,
            reducer_group_id: None,
            peer_id: None,
            lease_id: None,
            series_label,
            value,
            captured_at: self.captured_at,
        }
    }
}

fn metric_context(
    peer_windows: &[PeerWindowMetrics],
    reducer_cohorts: &[ReducerCohortMetrics],
    head_reports: &[HeadEvalReport],
) -> Option<MetricPointContext> {
    if let Some(metrics) = peer_windows.first() {
        let canonical_head_id = head_reports
            .iter()
            .filter(|report| report.status == HeadEvalStatus::Completed)
            .max_by_key(|report| report.finished_at)
            .map(|report| report.head_id.clone());
        return Some(MetricPointContext {
            network_id: metrics.network_id.clone(),
            experiment_id: metrics.experiment_id.clone(),
            revision_id: metrics.revision_id.clone(),
            workload_id: metrics.workload_id.clone(),
            dataset_view_id: metrics.dataset_view_id.clone(),
            canonical_head_id,
            captured_at: metrics.window_finished_at,
        });
    }
    if let Some(metrics) = reducer_cohorts.first() {
        let canonical_head_id = head_reports
            .iter()
            .filter(|report| report.status == HeadEvalStatus::Completed)
            .max_by_key(|report| report.finished_at)
            .map(|report| report.head_id.clone());
        return Some(MetricPointContext {
            network_id: metrics.network_id.clone(),
            experiment_id: metrics.experiment_id.clone(),
            revision_id: metrics.revision_id.clone(),
            workload_id: metrics.workload_id.clone(),
            dataset_view_id: metrics.dataset_view_id.clone(),
            canonical_head_id,
            captured_at: metrics.captured_at,
        });
    }
    head_reports.first().map(|report| MetricPointContext {
        network_id: report.network_id.clone(),
        experiment_id: report.experiment_id.clone(),
        revision_id: report.revision_id.clone(),
        workload_id: report.workload_id.clone(),
        dataset_view_id: report.dataset_view_id.clone(),
        canonical_head_id: head_reports
            .iter()
            .filter(|candidate| {
                candidate.status == HeadEvalStatus::Completed
                    && candidate.trust_class == MetricTrustClass::Canonical
            })
            .max_by_key(|candidate| candidate.finished_at)
            .map(|candidate| candidate.head_id.clone()),
        captured_at: report.finished_at,
    })
}

fn elapsed_seconds_from_peer_windows(metrics: &[PeerWindowMetrics]) -> Option<f64> {
    let earliest = metrics
        .iter()
        .map(|metrics| metrics.window_started_at)
        .min()?;
    let latest = metrics
        .iter()
        .map(|metrics| metrics.window_finished_at)
        .max()?;
    Some((latest - earliest).num_milliseconds().max(1) as f64 / 1000.0)
}

fn elapsed_seconds_from_head_reports(reports: &[HeadEvalReport]) -> Option<f64> {
    let earliest = reports.iter().map(|report| report.started_at).min()?;
    let latest = reports.iter().map(|report| report.finished_at).max()?;
    Some((latest - earliest).num_milliseconds().max(1) as f64 / 1000.0)
}

fn percentile_nearest_rank(sorted_values: &[f64], percentile: f64) -> f64 {
    debug_assert!(!sorted_values.is_empty());
    let rank = (percentile.clamp(0.0, 1.0) * sorted_values.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted_values.len() - 1);
    sorted_values[index]
}

fn summarize_peer_window_distribution(
    peer_windows: &[PeerWindowMetrics],
) -> Option<PeerWindowDistributionSummary> {
    let first = peer_windows.first()?;

    let completed_windows = peer_windows
        .iter()
        .filter(|metrics| metrics.status == PeerWindowStatus::Completed)
        .count();
    let accepted_windows = peer_windows
        .iter()
        .filter(|metrics| metrics.accepted_tokens_or_samples.unwrap_or(0) > 0)
        .count();
    let rejected_windows = peer_windows
        .iter()
        .filter(|metrics| metrics.status == PeerWindowStatus::Rejected)
        .count();
    let aborted_windows = peer_windows
        .iter()
        .filter(|metrics| metrics.status == PeerWindowStatus::Aborted)
        .count();
    let failed_windows = peer_windows
        .iter()
        .filter(|metrics| metrics.status == PeerWindowStatus::Failed)
        .count();
    let attempted_tokens_or_samples = peer_windows
        .iter()
        .map(|metrics| metrics.attempted_tokens_or_samples)
        .sum::<u64>();
    let accepted_tokens_or_samples = peer_windows
        .iter()
        .filter_map(|metrics| metrics.accepted_tokens_or_samples)
        .sum::<u64>();
    let acceptance_ratio = if attempted_tokens_or_samples > 0 {
        accepted_tokens_or_samples as f64 / attempted_tokens_or_samples as f64
    } else {
        0.0
    };
    let role_counts = peer_windows
        .iter()
        .fold(BTreeMap::new(), |mut acc, metrics| {
            *acc.entry(snake_case_debug_label(&metrics.role))
                .or_insert(0) += 1;
            acc
        });
    let backend_counts = peer_windows
        .iter()
        .fold(BTreeMap::new(), |mut acc, metrics| {
            *acc.entry(snake_case_debug_label(&metrics.backend_class))
                .or_insert(0) += 1;
            acc
        });

    Some(PeerWindowDistributionSummary {
        network_id: first.network_id.clone(),
        experiment_id: first.experiment_id.clone(),
        revision_id: first.revision_id.clone(),
        workload_id: first.workload_id.clone(),
        dataset_view_id: first.dataset_view_id.clone(),
        base_head_id: first.base_head_id.clone(),
        sample_count: peer_windows.len(),
        completed_windows,
        accepted_windows,
        rejected_windows,
        aborted_windows,
        failed_windows,
        attempted_tokens_or_samples,
        accepted_tokens_or_samples,
        acceptance_ratio,
        local_train_loss_mean_p10: percentile_option(
            peer_windows
                .iter()
                .filter_map(|metrics| metrics.local_train_loss_mean)
                .collect(),
            0.10,
        ),
        local_train_loss_mean_p50: percentile_option(
            peer_windows
                .iter()
                .filter_map(|metrics| metrics.local_train_loss_mean)
                .collect(),
            0.50,
        ),
        local_train_loss_mean_p90: percentile_option(
            peer_windows
                .iter()
                .filter_map(|metrics| metrics.local_train_loss_mean)
                .collect(),
            0.90,
        ),
        compute_time_ms_p10: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.compute_time_ms as f64)
                .collect(),
            0.10,
        ),
        compute_time_ms_p50: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.compute_time_ms as f64)
                .collect(),
            0.50,
        ),
        compute_time_ms_p90: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.compute_time_ms as f64)
                .collect(),
            0.90,
        ),
        data_fetch_time_ms_p10: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.data_fetch_time_ms as f64)
                .collect(),
            0.10,
        ),
        data_fetch_time_ms_p50: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.data_fetch_time_ms as f64)
                .collect(),
            0.50,
        ),
        data_fetch_time_ms_p90: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.data_fetch_time_ms as f64)
                .collect(),
            0.90,
        ),
        publish_latency_ms_p10: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.publish_latency_ms as f64)
                .collect(),
            0.10,
        ),
        publish_latency_ms_p50: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.publish_latency_ms as f64)
                .collect(),
            0.50,
        ),
        publish_latency_ms_p90: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.publish_latency_ms as f64)
                .collect(),
            0.90,
        ),
        head_lag_at_finish_p10: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.head_lag_at_finish as f64)
                .collect(),
            0.10,
        ),
        head_lag_at_finish_p50: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.head_lag_at_finish as f64)
                .collect(),
            0.50,
        ),
        head_lag_at_finish_p90: percentile_value(
            peer_windows
                .iter()
                .map(|metrics| metrics.head_lag_at_finish as f64)
                .collect(),
            0.90,
        ),
        role_counts,
        backend_counts,
        captured_at: peer_windows
            .iter()
            .map(|metrics| metrics.window_finished_at)
            .max()
            .unwrap_or(first.window_finished_at),
    })
}

fn percentile_value(mut values: Vec<f64>, percentile: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    percentile_nearest_rank(&values, percentile)
}

fn percentile_option(values: Vec<f64>, percentile: f64) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(percentile_value(values, percentile))
    }
}

fn snake_case_debug_label(value: &impl std::fmt::Debug) -> String {
    let debug = format!("{value:?}");
    let mut out = String::with_capacity(debug.len() + 4);
    for (index, ch) in debug.chars().enumerate() {
        if ch.is_ascii_uppercase() && index > 0 {
            out.push('_');
        }
        out.push(ch.to_ascii_lowercase());
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_core::{
        BackendClass, ContentId, DatasetViewId, ExperimentId, HeadEvalReport, HeadId, LeaseId,
        MetricValue, MetricsLiveEventKind, NetworkId, PeerRole, PeerWindowMetrics,
        PeerWindowStatus, ReducerCohortMetrics, ReducerCohortStatus, RevisionId, WorkloadId,
    };
    use chrono::{Duration, Utc};
    use tempfile::tempdir;

    use super::{
        DerivedMetricKind, MetricEnvelope, MetricsIndexer, MetricsIndexerConfig, MetricsStore,
        derive_peer_window_distribution_detail, derive_peer_window_distribution_detail_with_limit,
        derive_peer_window_distribution_summaries,
    };

    #[test]
    fn metrics_indexer_derives_rollups_and_exports_snapshot_and_segments() {
        let mut indexer = MetricsIndexer::new(MetricsIndexerConfig {
            stale_head_lag_threshold_steps: 2,
            ledger_segment_entry_limit: 2,
            ..MetricsIndexerConfig::default()
        });
        let started_at = Utc::now();
        indexer.ingest_peer_window_metrics(PeerWindowMetrics {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            dataset_view_id: DatasetViewId::new("view-a"),
            peer_id: burn_p2p_core::PeerId::new("peer-a"),
            principal_id: None,
            lease_id: LeaseId::new("lease-a"),
            base_head_id: HeadId::new("head-a"),
            window_started_at: started_at,
            window_finished_at: started_at + Duration::seconds(10),
            attempted_tokens_or_samples: 200,
            accepted_tokens_or_samples: Some(180),
            local_train_loss_mean: Some(0.4),
            local_train_loss_last: Some(0.3),
            grad_or_delta_norm: Some(1.1),
            optimizer_step_count: 10,
            compute_time_ms: 8_000,
            data_fetch_time_ms: 1_000,
            publish_latency_ms: 100,
            head_lag_at_start: 1,
            head_lag_at_finish: 1,
            backend_class: BackendClass::BrowserWgpu,
            role: PeerRole::BrowserTrainerWgpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        });
        indexer.ingest_peer_window_metrics(PeerWindowMetrics {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            dataset_view_id: DatasetViewId::new("view-a"),
            peer_id: burn_p2p_core::PeerId::new("peer-b"),
            principal_id: None,
            lease_id: LeaseId::new("lease-b"),
            base_head_id: HeadId::new("head-a"),
            window_started_at: started_at + Duration::seconds(1),
            window_finished_at: started_at + Duration::seconds(11),
            attempted_tokens_or_samples: 300,
            accepted_tokens_or_samples: Some(250),
            local_train_loss_mean: Some(0.5),
            local_train_loss_last: Some(0.45),
            grad_or_delta_norm: Some(1.4),
            optimizer_step_count: 12,
            compute_time_ms: 9_000,
            data_fetch_time_ms: 900,
            publish_latency_ms: 110,
            head_lag_at_start: 2,
            head_lag_at_finish: 4,
            backend_class: BackendClass::Cpu,
            role: PeerRole::TrainerCpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        });
        indexer.ingest_reducer_cohort_metrics(ReducerCohortMetrics {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            dataset_view_id: DatasetViewId::new("view-a"),
            merge_window_id: burn_p2p_core::ContentId::new("window-a"),
            reducer_group_id: burn_p2p_core::ContentId::new("reducers-a"),
            captured_at: started_at + Duration::seconds(12),
            base_head_id: HeadId::new("head-a"),
            candidate_head_id: Some(HeadId::new("head-b")),
            received_updates: 4,
            accepted_updates: 3,
            rejected_updates: 1,
            sum_weight: 2.5,
            accepted_tokens_or_samples: 430,
            staleness_mean: 1.5,
            staleness_max: 4.0,
            window_close_delay_ms: 250,
            cohort_duration_ms: 5_000,
            aggregate_norm: 1.2,
            reducer_load: 0.7,
            ingress_bytes: 3_072,
            egress_bytes: 1_024,
            replica_agreement: Some(0.95),
            late_arrival_count: Some(1),
            missing_peer_count: Some(0),
            rejection_reasons: BTreeMap::from([("stale_base_head".into(), 1)]),
            status: ReducerCohortStatus::Closed,
        });
        indexer.ingest_head_eval_report(burn_p2p_core::HeadEvalReport {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            head_id: HeadId::new("head-b"),
            base_head_id: Some(HeadId::new("head-a")),
            eval_protocol_id: burn_p2p_core::ContentId::new("eval-a"),
            evaluator_set_id: burn_p2p_core::ContentId::new("eval-set-a"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.25))]),
            sample_count: 512,
            dataset_view_id: DatasetViewId::new("view-a"),
            started_at: started_at + Duration::seconds(12),
            finished_at: started_at + Duration::seconds(14),
            trust_class: burn_p2p_core::MetricTrustClass::Canonical,
            status: burn_p2p_core::HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        });

        let derived =
            indexer.derive_metrics(&ExperimentId::new("exp-a"), &RevisionId::new("rev-a"));
        assert!(
            derived
                .iter()
                .any(|point| point.metric == DerivedMetricKind::AcceptedTokensPerSec)
        );
        assert!(
            derived
                .iter()
                .any(|point| point.metric == DerivedMetricKind::MeanHeadLag)
        );
        assert!(
            derived
                .iter()
                .any(|point| point.metric == DerivedMetricKind::ReducerReplicaAgreement)
        );
        assert!(
            derived
                .iter()
                .any(|point| point.metric == DerivedMetricKind::BytesPerAcceptedToken)
        );
        assert!(
            derived
                .iter()
                .any(|point| point.metric == DerivedMetricKind::RejectionRatioByReason)
        );

        let snapshot = indexer
            .export_snapshot(
                &ExperimentId::new("exp-a"),
                &RevisionId::new("rev-a"),
                3,
                None,
                Vec::new(),
            )
            .expect("snapshot");
        assert_eq!(snapshot.manifest.snapshot_seq, 3);
        assert_eq!(
            snapshot.manifest.covers_until_head_id,
            Some(HeadId::new("head-b"))
        );
        assert_eq!(snapshot.peer_window_metrics.len(), 2);

        let segments = indexer
            .export_ledger_segments(&ExperimentId::new("exp-a"), &RevisionId::new("rev-a"))
            .expect("segments");
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].segment_seq, 0);
        assert_eq!(segments[1].prev_hash, Some(segments[0].hash.clone()));
    }

    #[test]
    fn peer_window_distributions_group_quantiles_by_base_head() {
        let started_at = Utc::now();
        let summaries = derive_peer_window_distribution_summaries(&[
            PeerWindowMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                peer_id: burn_p2p_core::PeerId::new("peer-a"),
                principal_id: None,
                lease_id: LeaseId::new("lease-a"),
                base_head_id: HeadId::new("head-a"),
                window_started_at: started_at,
                window_finished_at: started_at + Duration::seconds(10),
                attempted_tokens_or_samples: 100,
                accepted_tokens_or_samples: Some(80),
                local_train_loss_mean: Some(0.6),
                local_train_loss_last: Some(0.55),
                grad_or_delta_norm: Some(1.0),
                optimizer_step_count: 10,
                compute_time_ms: 1_000,
                data_fetch_time_ms: 100,
                publish_latency_ms: 10,
                head_lag_at_start: 0,
                head_lag_at_finish: 1,
                backend_class: BackendClass::Cpu,
                role: PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            },
            PeerWindowMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                peer_id: burn_p2p_core::PeerId::new("peer-b"),
                principal_id: None,
                lease_id: LeaseId::new("lease-b"),
                base_head_id: HeadId::new("head-a"),
                window_started_at: started_at + Duration::seconds(1),
                window_finished_at: started_at + Duration::seconds(11),
                attempted_tokens_or_samples: 120,
                accepted_tokens_or_samples: Some(0),
                local_train_loss_mean: Some(0.9),
                local_train_loss_last: Some(0.8),
                grad_or_delta_norm: Some(1.2),
                optimizer_step_count: 11,
                compute_time_ms: 1_400,
                data_fetch_time_ms: 300,
                publish_latency_ms: 30,
                head_lag_at_start: 2,
                head_lag_at_finish: 5,
                backend_class: BackendClass::BrowserWgpu,
                role: PeerRole::BrowserVerifier,
                status: PeerWindowStatus::Rejected,
                status_reason: Some("lag".into()),
            },
            PeerWindowMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                peer_id: burn_p2p_core::PeerId::new("peer-c"),
                principal_id: None,
                lease_id: LeaseId::new("lease-c"),
                base_head_id: HeadId::new("head-b"),
                window_started_at: started_at,
                window_finished_at: started_at + Duration::seconds(12),
                attempted_tokens_or_samples: 90,
                accepted_tokens_or_samples: Some(70),
                local_train_loss_mean: Some(0.4),
                local_train_loss_last: Some(0.35),
                grad_or_delta_norm: Some(0.9),
                optimizer_step_count: 8,
                compute_time_ms: 900,
                data_fetch_time_ms: 90,
                publish_latency_ms: 9,
                head_lag_at_start: 0,
                head_lag_at_finish: 0,
                backend_class: BackendClass::Cpu,
                role: PeerRole::Evaluator,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            },
        ]);

        assert_eq!(summaries.len(), 2);
        let head_a = summaries
            .iter()
            .find(|summary| summary.base_head_id.as_str() == "head-a")
            .expect("head-a summary");
        assert_eq!(head_a.sample_count, 2);
        assert_eq!(head_a.completed_windows, 1);
        assert_eq!(head_a.rejected_windows, 1);
        assert_eq!(head_a.accepted_windows, 1);
        assert_eq!(head_a.attempted_tokens_or_samples, 220);
        assert_eq!(head_a.accepted_tokens_or_samples, 80);
        assert!(head_a.acceptance_ratio > 0.36 && head_a.acceptance_ratio < 0.37);
        assert_eq!(head_a.local_train_loss_mean_p10, Some(0.6));
        assert_eq!(head_a.local_train_loss_mean_p50, Some(0.6));
        assert_eq!(head_a.local_train_loss_mean_p90, Some(0.9));
        assert_eq!(head_a.compute_time_ms_p90, 1_400.0);
        assert_eq!(head_a.role_counts.get("trainercpu"), None);
        assert_eq!(head_a.role_counts.get("trainer_cpu"), Some(&1));
        assert_eq!(head_a.role_counts.get("browser_verifier"), Some(&1));
        assert_eq!(head_a.backend_counts.get("cpu"), Some(&1));
        assert_eq!(head_a.backend_counts.get("browserwgpu"), None);
        assert_eq!(head_a.backend_counts.get("browser_wgpu"), Some(&1));
    }

    #[test]
    fn peer_window_distribution_detail_filters_one_revision_and_base_head() {
        let started_at = Utc::now();
        let detail = derive_peer_window_distribution_detail(
            &[
                PeerWindowMetrics {
                    network_id: NetworkId::new("network-a"),
                    experiment_id: ExperimentId::new("exp-a"),
                    revision_id: RevisionId::new("rev-a"),
                    workload_id: WorkloadId::new("workload-a"),
                    dataset_view_id: DatasetViewId::new("view-a"),
                    peer_id: burn_p2p_core::PeerId::new("peer-a"),
                    principal_id: None,
                    lease_id: LeaseId::new("lease-a"),
                    base_head_id: HeadId::new("head-a"),
                    window_started_at: started_at,
                    window_finished_at: started_at + Duration::seconds(10),
                    attempted_tokens_or_samples: 100,
                    accepted_tokens_or_samples: Some(80),
                    local_train_loss_mean: Some(0.6),
                    local_train_loss_last: Some(0.55),
                    grad_or_delta_norm: Some(1.0),
                    optimizer_step_count: 10,
                    compute_time_ms: 1_000,
                    data_fetch_time_ms: 100,
                    publish_latency_ms: 10,
                    head_lag_at_start: 0,
                    head_lag_at_finish: 1,
                    backend_class: BackendClass::Cpu,
                    role: PeerRole::TrainerCpu,
                    status: PeerWindowStatus::Completed,
                    status_reason: None,
                },
                PeerWindowMetrics {
                    network_id: NetworkId::new("network-a"),
                    experiment_id: ExperimentId::new("exp-a"),
                    revision_id: RevisionId::new("rev-a"),
                    workload_id: WorkloadId::new("workload-a"),
                    dataset_view_id: DatasetViewId::new("view-a"),
                    peer_id: burn_p2p_core::PeerId::new("peer-b"),
                    principal_id: None,
                    lease_id: LeaseId::new("lease-b"),
                    base_head_id: HeadId::new("head-a"),
                    window_started_at: started_at + Duration::seconds(1),
                    window_finished_at: started_at + Duration::seconds(11),
                    attempted_tokens_or_samples: 120,
                    accepted_tokens_or_samples: Some(0),
                    local_train_loss_mean: Some(0.9),
                    local_train_loss_last: Some(0.8),
                    grad_or_delta_norm: Some(1.2),
                    optimizer_step_count: 11,
                    compute_time_ms: 1_400,
                    data_fetch_time_ms: 300,
                    publish_latency_ms: 30,
                    head_lag_at_start: 2,
                    head_lag_at_finish: 5,
                    backend_class: BackendClass::BrowserWgpu,
                    role: PeerRole::BrowserVerifier,
                    status: PeerWindowStatus::Rejected,
                    status_reason: Some("lag".into()),
                },
                PeerWindowMetrics {
                    network_id: NetworkId::new("network-a"),
                    experiment_id: ExperimentId::new("exp-a"),
                    revision_id: RevisionId::new("rev-b"),
                    workload_id: WorkloadId::new("workload-a"),
                    dataset_view_id: DatasetViewId::new("view-a"),
                    peer_id: burn_p2p_core::PeerId::new("peer-c"),
                    principal_id: None,
                    lease_id: LeaseId::new("lease-c"),
                    base_head_id: HeadId::new("head-a"),
                    window_started_at: started_at + Duration::seconds(2),
                    window_finished_at: started_at + Duration::seconds(12),
                    attempted_tokens_or_samples: 90,
                    accepted_tokens_or_samples: Some(70),
                    local_train_loss_mean: Some(0.4),
                    local_train_loss_last: Some(0.35),
                    grad_or_delta_norm: Some(0.9),
                    optimizer_step_count: 8,
                    compute_time_ms: 900,
                    data_fetch_time_ms: 90,
                    publish_latency_ms: 9,
                    head_lag_at_start: 0,
                    head_lag_at_finish: 0,
                    backend_class: BackendClass::Cpu,
                    role: PeerRole::Evaluator,
                    status: PeerWindowStatus::Completed,
                    status_reason: None,
                },
            ],
            &ExperimentId::new("exp-a"),
            &RevisionId::new("rev-a"),
            &HeadId::new("head-a"),
        )
        .expect("detail");

        assert_eq!(detail.summary.revision_id.as_str(), "rev-a");
        assert_eq!(detail.summary.base_head_id.as_str(), "head-a");
        assert_eq!(detail.summary.sample_count, 2);
        assert_eq!(detail.windows.len(), 2);
        assert_eq!(detail.windows[0].peer_id.as_str(), "peer-b");
        assert_eq!(detail.windows[1].peer_id.as_str(), "peer-a");
    }

    #[test]
    fn metrics_store_persists_and_recovers_materialized_views() {
        let mut indexer = MetricsIndexer::new(MetricsIndexerConfig {
            stale_head_lag_threshold_steps: 2,
            ledger_segment_entry_limit: 2,
            ..MetricsIndexerConfig::default()
        });
        let started_at = Utc::now();
        indexer.ingest_peer_window_metrics(PeerWindowMetrics {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            dataset_view_id: DatasetViewId::new("view-a"),
            peer_id: burn_p2p_core::PeerId::new("peer-a"),
            principal_id: None,
            lease_id: LeaseId::new("lease-a"),
            base_head_id: HeadId::new("head-a"),
            window_started_at: started_at,
            window_finished_at: started_at + Duration::seconds(10),
            attempted_tokens_or_samples: 200,
            accepted_tokens_or_samples: Some(180),
            local_train_loss_mean: Some(0.4),
            local_train_loss_last: Some(0.3),
            grad_or_delta_norm: Some(1.1),
            optimizer_step_count: 10,
            compute_time_ms: 8_000,
            data_fetch_time_ms: 1_000,
            publish_latency_ms: 100,
            head_lag_at_start: 1,
            head_lag_at_finish: 1,
            backend_class: BackendClass::BrowserWgpu,
            role: PeerRole::BrowserTrainerWgpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        });
        indexer.ingest_head_eval_report(burn_p2p_core::HeadEvalReport {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            head_id: HeadId::new("head-b"),
            base_head_id: Some(HeadId::new("head-a")),
            eval_protocol_id: burn_p2p_core::ContentId::new("eval-a"),
            evaluator_set_id: burn_p2p_core::ContentId::new("eval-set-a"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.25))]),
            sample_count: 512,
            dataset_view_id: DatasetViewId::new("view-a"),
            started_at: started_at + Duration::seconds(12),
            finished_at: started_at + Duration::seconds(14),
            trust_class: burn_p2p_core::MetricTrustClass::Canonical,
            status: burn_p2p_core::HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        });

        let temp = tempdir().expect("temp dir");
        let mut store = MetricsStore::open(temp.path(), MetricsIndexerConfig::default())
            .expect("open metrics store");
        store
            .replace_entries(indexer.entries.clone())
            .expect("persist entries");
        store.materialize_views().expect("materialize views");

        let reopened = MetricsStore::open(temp.path(), MetricsIndexerConfig::default())
            .expect("reopen metrics store");
        let snapshots = reopened.load_snapshots().expect("load snapshots");
        let segments = reopened
            .load_ledger_segments()
            .expect("load ledger segments");
        let reports = reopened
            .load_head_eval_reports(&HeadId::new("head-b"))
            .expect("load head eval reports");
        let catchup = reopened
            .load_catchup_bundles()
            .expect("load catchup bundles");
        let live_event = reopened
            .load_live_event(MetricsLiveEventKind::CatchupRefresh)
            .expect("load live event")
            .expect("live event");

        assert_eq!(snapshots.len(), 1);
        assert_eq!(
            snapshots[0].manifest.experiment_id,
            ExperimentId::new("exp-a")
        );
        assert!(!segments.is_empty());
        assert_eq!(reports.len(), 1);
        assert_eq!(catchup.len(), 1);
        assert_eq!(
            catchup[0].snapshot.manifest.revision_id,
            RevisionId::new("rev-a")
        );
        assert_eq!(live_event.cursors.len(), 1);
        assert_eq!(live_event.kind, MetricsLiveEventKind::CatchupRefresh);
    }

    #[test]
    fn metrics_indexer_prunes_raw_history_and_caps_detail_payloads() {
        let started_at = Utc::now();
        let mut indexer = MetricsIndexer::new(MetricsIndexerConfig {
            max_peer_window_entries_per_revision: 2,
            max_reducer_cohort_entries_per_revision: 1,
            max_head_eval_reports_per_revision: 1,
            max_revisions_per_experiment: 2,
            max_peer_window_detail_windows: 1,
            ..MetricsIndexerConfig::default()
        });

        for offset in 0..3 {
            indexer.ingest_peer_window_metrics(PeerWindowMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                peer_id: burn_p2p_core::PeerId::new(format!("peer-{offset}")),
                principal_id: None,
                lease_id: LeaseId::new(format!("lease-{offset}")),
                base_head_id: HeadId::new("head-a"),
                window_started_at: started_at + Duration::seconds(offset),
                window_finished_at: started_at + Duration::seconds(offset + 1),
                attempted_tokens_or_samples: 100,
                accepted_tokens_or_samples: Some(100),
                local_train_loss_mean: Some(0.5 + offset as f64),
                local_train_loss_last: Some(0.5 + offset as f64),
                grad_or_delta_norm: Some(1.0),
                optimizer_step_count: 1,
                compute_time_ms: 100,
                data_fetch_time_ms: 10,
                publish_latency_ms: 5,
                head_lag_at_start: 0,
                head_lag_at_finish: offset as u64,
                backend_class: BackendClass::Cpu,
                role: PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            });
        }
        for offset in 0..2 {
            indexer.ingest_reducer_cohort_metrics(ReducerCohortMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                merge_window_id: ContentId::new(format!("merge-{offset}")),
                reducer_group_id: ContentId::new("reducer-a"),
                captured_at: started_at + Duration::seconds(20 + offset),
                base_head_id: HeadId::new("head-a"),
                candidate_head_id: Some(HeadId::new(format!("candidate-{offset}"))),
                received_updates: 1,
                accepted_updates: 1,
                rejected_updates: 0,
                sum_weight: 1.0,
                accepted_tokens_or_samples: 100,
                staleness_mean: 0.0,
                staleness_max: 0.0,
                window_close_delay_ms: 1,
                cohort_duration_ms: 1,
                aggregate_norm: 1.0,
                reducer_load: 0.1,
                ingress_bytes: 10,
                egress_bytes: 10,
                replica_agreement: Some(1.0),
                late_arrival_count: Some(0),
                missing_peer_count: Some(0),
                rejection_reasons: BTreeMap::new(),
                status: burn_p2p_core::ReducerCohortStatus::Closed,
            });
        }
        for offset in 0..2 {
            indexer.ingest_head_eval_report(HeadEvalReport {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                head_id: HeadId::new(format!("head-{offset}")),
                base_head_id: Some(HeadId::new("head-a")),
                eval_protocol_id: ContentId::new("eval-a"),
                evaluator_set_id: ContentId::new("eval-set-a"),
                metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.2))]),
                sample_count: 32,
                dataset_view_id: DatasetViewId::new("view-a"),
                started_at: started_at + Duration::seconds(30 + offset),
                finished_at: started_at + Duration::seconds(31 + offset),
                trust_class: burn_p2p_core::MetricTrustClass::Canonical,
                status: burn_p2p_core::HeadEvalStatus::Completed,
                signature_bundle: Vec::new(),
            });
        }

        let entries = indexer.into_entries();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, MetricEnvelope::PeerWindow(_)))
                .count(),
            2
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, MetricEnvelope::ReducerCohort(_)))
                .count(),
            1
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, MetricEnvelope::HeadEval(_)))
                .count(),
            1
        );

        let peer_windows = entries
            .iter()
            .filter_map(|entry| match entry {
                MetricEnvelope::PeerWindow(metrics) => Some(metrics.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let detail = derive_peer_window_distribution_detail_with_limit(
            &peer_windows,
            &ExperimentId::new("exp-a"),
            &RevisionId::new("rev-a"),
            &HeadId::new("head-a"),
            1,
        )
        .expect("detail");
        assert_eq!(detail.summary.sample_count, 2);
        assert_eq!(detail.windows.len(), 1);
        assert_eq!(detail.windows[0].peer_id.as_str(), "peer-2");
    }

    #[test]
    fn metrics_indexer_prunes_older_revisions_per_experiment() {
        let started_at = Utc::now();
        let mut indexer = MetricsIndexer::new(MetricsIndexerConfig {
            max_peer_window_entries_per_revision: 2,
            max_revisions_per_experiment: 2,
            ..MetricsIndexerConfig::default()
        });

        for (offset, revision) in ["rev-a", "rev-b", "rev-c"].into_iter().enumerate() {
            indexer.ingest_peer_window_metrics(PeerWindowMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new(revision),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                peer_id: burn_p2p_core::PeerId::new(format!("peer-{offset}")),
                principal_id: None,
                lease_id: LeaseId::new(format!("lease-{offset}")),
                base_head_id: HeadId::new(format!("head-{offset}")),
                window_started_at: started_at + Duration::seconds(offset as i64),
                window_finished_at: started_at + Duration::seconds(offset as i64 + 1),
                attempted_tokens_or_samples: 100,
                accepted_tokens_or_samples: Some(100),
                local_train_loss_mean: Some(0.5),
                local_train_loss_last: Some(0.5),
                grad_or_delta_norm: Some(1.0),
                optimizer_step_count: 1,
                compute_time_ms: 100,
                data_fetch_time_ms: 10,
                publish_latency_ms: 5,
                head_lag_at_start: 0,
                head_lag_at_finish: 0,
                backend_class: BackendClass::Cpu,
                role: PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            });
        }

        let retained_revisions = indexer
            .into_entries()
            .into_iter()
            .map(|entry| entry.revision_id().clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            retained_revisions,
            BTreeSet::from([RevisionId::new("rev-b"), RevisionId::new("rev-c")])
        );
    }
}
