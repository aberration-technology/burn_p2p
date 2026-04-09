use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    ContentId, DatasetViewId, ExperimentId, HeadEvalReport, HeadEvalStatus, HeadId, MetricScope,
    MetricTrustClass, MetricsLedgerSegment, MetricsSnapshotManifest, NetworkId, PeerRole,
    PeerWindowMetrics, PeerWindowStatus, ReducerCohortMetrics, RevisionId, SignatureMetadata,
    WorkloadId,
};
use chrono::{DateTime, Duration, Utc};

use crate::{
    DerivedMetricKind, DerivedMetricPoint, HeadEvaluationPerformanceSummary, MetricEnvelope,
    MetricsIndexerConfig, MetricsSnapshot, NetworkPerformanceSummary, PeerPerformanceSummary,
    PeerWindowDistributionSummary, ReducerPerformanceSummary, WorkClassPerformanceSummary,
    derive_peer_window_distribution_summaries,
};

/// In-memory metrics read model for one or more experiment revisions.
#[derive(Clone, Debug, Default)]
pub struct MetricsIndexer {
    pub(crate) config: MetricsIndexerConfig,
    pub(crate) entries: Vec<MetricEnvelope>,
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
        crate::retention::enforce_retention(&mut self.entries, &self.config);
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
        let performance_summary = derive_network_performance_summary(
            &peer_window_metrics,
            &reducer_cohort_metrics,
            &head_eval_reports,
        );

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
        let network_rollups_ref =
            ContentId::derive(&(derived_metrics.clone(), performance_summary.clone()))?;
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
            performance_summary,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkClass {
    Training,
    Validation,
}

/// Derives a reusable network performance rollup from raw runtime metrics.
pub fn derive_network_performance_summary(
    peer_windows: &[PeerWindowMetrics],
    reducer_cohorts: &[ReducerCohortMetrics],
    head_reports: &[HeadEvalReport],
) -> Option<NetworkPerformanceSummary> {
    let network_id = common_network_id(peer_windows, reducer_cohorts, head_reports)?;
    let captured_at = latest_captured_at(peer_windows, reducer_cohorts, head_reports)?;
    let experiment_id = common_experiment_id(peer_windows, reducer_cohorts, head_reports);
    let revision_id = common_revision_id(peer_windows, reducer_cohorts, head_reports);
    let workload_id = common_workload_id(peer_windows, reducer_cohorts, head_reports);
    let dataset_view_id = common_dataset_view_id(peer_windows, reducer_cohorts, head_reports);
    let peers = derive_peer_performance_summaries(peer_windows);
    let training_windows = peer_windows
        .iter()
        .filter(|metrics| classify_work_class(&metrics.role) == Some(WorkClass::Training))
        .cloned()
        .collect::<Vec<_>>();
    let validation_windows = peer_windows
        .iter()
        .filter(|metrics| classify_work_class(&metrics.role) == Some(WorkClass::Validation))
        .cloned()
        .collect::<Vec<_>>();

    Some(NetworkPerformanceSummary {
        network_id,
        experiment_id,
        revision_id,
        workload_id,
        dataset_view_id,
        captured_at,
        total_peer_count: peers.len() as u64,
        training: summarize_work_class(&training_windows),
        validation: summarize_work_class(&validation_windows),
        head_evaluation: summarize_head_evaluations(head_reports),
        reducer: summarize_reducer_cohorts(reducer_cohorts),
        peers,
    })
}

fn derive_peer_performance_summaries(
    peer_windows: &[PeerWindowMetrics],
) -> Vec<PeerPerformanceSummary> {
    let mut per_peer = BTreeMap::<String, Vec<PeerWindowMetrics>>::new();
    for metrics in peer_windows {
        per_peer
            .entry(metrics.peer_id.as_str().to_string())
            .or_default()
            .push(metrics.clone());
    }

    per_peer
        .into_values()
        .filter_map(|mut windows| {
            windows.sort_by_key(|metrics| metrics.window_started_at);
            let latest = windows
                .iter()
                .max_by_key(|metrics| metrics.window_finished_at)?;
            let attempted_work_units = windows
                .iter()
                .map(|metrics| metrics.attempted_tokens_or_samples)
                .sum::<u64>();
            let accepted_work_units = windows
                .iter()
                .filter_map(|metrics| metrics.accepted_tokens_or_samples)
                .sum::<u64>();
            let active_window_time_ms = windows.iter().map(peer_window_active_time_ms).sum::<u64>();
            let end_to_end_window_time_ms = windows
                .iter()
                .map(peer_window_end_to_end_time_ms)
                .sum::<u64>();
            let compute_time_ms = windows
                .iter()
                .map(|metrics| metrics.compute_time_ms)
                .sum::<u64>();
            let data_fetch_time_ms = windows
                .iter()
                .map(|metrics| metrics.data_fetch_time_ms)
                .sum::<u64>();
            let publish_latency_ms = windows
                .iter()
                .map(|metrics| metrics.publish_latency_ms)
                .sum::<u64>();
            let wait_time_ms = windows.iter().map(peer_window_wait_time_ms).sum::<u64>();
            let idle_time_ms = peer_idle_time_ms(&windows);
            let elapsed_time_ms = elapsed_time_ms_from_peer_windows(&windows).unwrap_or_default();
            let throughput_work_units_per_sec =
                throughput_per_second(accepted_work_units, elapsed_time_ms);

            Some(PeerPerformanceSummary {
                peer_id: latest.peer_id.clone(),
                principal_id: latest.principal_id.clone(),
                role: latest.role.clone(),
                backend_class: latest.backend_class.clone(),
                window_count: windows.len() as u64,
                attempted_work_units,
                accepted_work_units,
                elapsed_time_ms,
                active_window_time_ms,
                end_to_end_window_time_ms,
                compute_time_ms,
                data_fetch_time_ms,
                publish_latency_ms,
                wait_time_ms,
                idle_time_ms,
                throughput_work_units_per_sec,
            })
        })
        .collect()
}

fn summarize_work_class(peer_windows: &[PeerWindowMetrics]) -> WorkClassPerformanceSummary {
    if peer_windows.is_empty() {
        return WorkClassPerformanceSummary {
            peer_count: 0,
            window_count: 0,
            attempted_work_units: 0,
            accepted_work_units: 0,
            elapsed_time_ms: 0,
            active_window_time_ms: 0,
            end_to_end_window_time_ms: 0,
            mean_end_to_end_window_time_ms: 0,
            max_end_to_end_window_time_ms: 0,
            compute_time_ms: 0,
            data_fetch_time_ms: 0,
            publish_latency_ms: 0,
            wait_time_ms: 0,
            idle_time_ms: 0,
            throughput_work_units_per_sec: 0.0,
        };
    }

    let peer_summaries = derive_peer_performance_summaries(peer_windows);
    let window_count = peer_windows.len() as u64;
    let attempted_work_units = peer_windows
        .iter()
        .map(|metrics| metrics.attempted_tokens_or_samples)
        .sum::<u64>();
    let accepted_work_units = peer_windows
        .iter()
        .filter_map(|metrics| metrics.accepted_tokens_or_samples)
        .sum::<u64>();
    let elapsed_time_ms = elapsed_time_ms_from_peer_windows(peer_windows).unwrap_or_default();
    let active_window_time_ms = peer_windows
        .iter()
        .map(peer_window_active_time_ms)
        .sum::<u64>();
    let end_to_end_window_time_ms = peer_windows
        .iter()
        .map(peer_window_end_to_end_time_ms)
        .sum::<u64>();
    let max_end_to_end_window_time_ms = peer_windows
        .iter()
        .map(peer_window_end_to_end_time_ms)
        .max()
        .unwrap_or_default();
    let compute_time_ms = peer_windows
        .iter()
        .map(|metrics| metrics.compute_time_ms)
        .sum::<u64>();
    let data_fetch_time_ms = peer_windows
        .iter()
        .map(|metrics| metrics.data_fetch_time_ms)
        .sum::<u64>();
    let publish_latency_ms = peer_windows
        .iter()
        .map(|metrics| metrics.publish_latency_ms)
        .sum::<u64>();
    let wait_time_ms = peer_windows
        .iter()
        .map(peer_window_wait_time_ms)
        .sum::<u64>();
    let idle_time_ms = peer_summaries
        .iter()
        .map(|summary| summary.idle_time_ms)
        .sum::<u64>();

    WorkClassPerformanceSummary {
        peer_count: peer_summaries.len() as u64,
        window_count,
        attempted_work_units,
        accepted_work_units,
        elapsed_time_ms,
        active_window_time_ms,
        end_to_end_window_time_ms,
        mean_end_to_end_window_time_ms: end_to_end_window_time_ms / window_count.max(1),
        max_end_to_end_window_time_ms,
        compute_time_ms,
        data_fetch_time_ms,
        publish_latency_ms,
        wait_time_ms,
        idle_time_ms,
        throughput_work_units_per_sec: throughput_per_second(accepted_work_units, elapsed_time_ms),
    }
}

fn summarize_head_evaluations(reports: &[HeadEvalReport]) -> HeadEvaluationPerformanceSummary {
    if reports.is_empty() {
        return HeadEvaluationPerformanceSummary {
            report_count: 0,
            sample_count: 0,
            elapsed_time_ms: 0,
            total_eval_time_ms: 0,
            mean_eval_time_ms: 0,
            max_eval_time_ms: 0,
            throughput_samples_per_sec: 0.0,
        };
    }

    let report_count = reports.len() as u64;
    let sample_count = reports
        .iter()
        .map(|report| report.sample_count)
        .sum::<u64>();
    let eval_durations = reports
        .iter()
        .map(head_eval_duration_ms)
        .collect::<Vec<_>>();
    let total_eval_time_ms = eval_durations.iter().sum::<u64>();
    let max_eval_time_ms = eval_durations.iter().copied().max().unwrap_or_default();
    let elapsed_time_ms = elapsed_time_ms_from_head_reports_ms(reports).unwrap_or_default();

    HeadEvaluationPerformanceSummary {
        report_count,
        sample_count,
        elapsed_time_ms,
        total_eval_time_ms,
        mean_eval_time_ms: total_eval_time_ms / report_count.max(1),
        max_eval_time_ms,
        throughput_samples_per_sec: throughput_per_second(sample_count, elapsed_time_ms),
    }
}

fn summarize_reducer_cohorts(
    reducer_cohorts: &[ReducerCohortMetrics],
) -> ReducerPerformanceSummary {
    if reducer_cohorts.is_empty() {
        return ReducerPerformanceSummary {
            cohort_count: 0,
            unique_cohort_count: 0,
            accepted_updates: 0,
            rejected_updates: 0,
            accepted_work_units: 0,
            mean_cohort_duration_ms: 0,
            max_cohort_duration_ms: 0,
            mean_window_close_delay_ms: 0,
            max_window_close_delay_ms: 0,
            ingress_bytes: 0,
            egress_bytes: 0,
        };
    }

    let mut unique =
        BTreeMap::<(ContentId, ContentId, Option<HeadId>, HeadId), ReducerCohortMetrics>::new();
    for metrics in reducer_cohorts {
        let key = (
            metrics.merge_window_id.clone(),
            metrics.reducer_group_id.clone(),
            metrics.candidate_head_id.clone(),
            metrics.base_head_id.clone(),
        );
        match unique.get(&key) {
            Some(existing) if existing.captured_at >= metrics.captured_at => {}
            _ => {
                unique.insert(key, metrics.clone());
            }
        }
    }
    let unique = unique.into_values().collect::<Vec<_>>();
    let unique_cohort_count = unique.len() as u64;
    let accepted_updates = unique
        .iter()
        .map(|metrics| metrics.accepted_updates)
        .sum::<u64>();
    let rejected_updates = unique
        .iter()
        .map(|metrics| metrics.rejected_updates)
        .sum::<u64>();
    let accepted_work_units = unique
        .iter()
        .map(|metrics| metrics.accepted_tokens_or_samples)
        .sum::<u64>();
    let total_cohort_duration_ms = unique
        .iter()
        .map(|metrics| metrics.cohort_duration_ms)
        .sum::<u64>();
    let max_cohort_duration_ms = unique
        .iter()
        .map(|metrics| metrics.cohort_duration_ms)
        .max()
        .unwrap_or_default();
    let total_window_close_delay_ms = unique
        .iter()
        .map(|metrics| metrics.window_close_delay_ms)
        .sum::<u64>();
    let max_window_close_delay_ms = unique
        .iter()
        .map(|metrics| metrics.window_close_delay_ms)
        .max()
        .unwrap_or_default();
    let ingress_bytes = unique
        .iter()
        .map(|metrics| metrics.ingress_bytes)
        .sum::<u128>();
    let egress_bytes = unique
        .iter()
        .map(|metrics| metrics.egress_bytes)
        .sum::<u128>();

    ReducerPerformanceSummary {
        cohort_count: reducer_cohorts.len() as u64,
        unique_cohort_count,
        accepted_updates,
        rejected_updates,
        accepted_work_units,
        mean_cohort_duration_ms: total_cohort_duration_ms / unique_cohort_count.max(1),
        max_cohort_duration_ms,
        mean_window_close_delay_ms: total_window_close_delay_ms / unique_cohort_count.max(1),
        max_window_close_delay_ms,
        ingress_bytes,
        egress_bytes,
    }
}

fn classify_work_class(role: &PeerRole) -> Option<WorkClass> {
    match role {
        PeerRole::TrainerGpu
        | PeerRole::TrainerCpu
        | PeerRole::BrowserTrainerWgpu
        | PeerRole::BrowserFallback
        | PeerRole::BrowserTrainer => Some(WorkClass::Training),
        PeerRole::Validator | PeerRole::Evaluator | PeerRole::BrowserVerifier => {
            Some(WorkClass::Validation)
        }
        PeerRole::Bootstrap
        | PeerRole::Authority
        | PeerRole::Archive
        | PeerRole::Reducer
        | PeerRole::Viewer
        | PeerRole::BrowserObserver
        | PeerRole::RelayHelper => None,
    }
}

fn common_network_id(
    peer_windows: &[PeerWindowMetrics],
    reducer_cohorts: &[ReducerCohortMetrics],
    head_reports: &[HeadEvalReport],
) -> Option<NetworkId> {
    let first = peer_windows
        .first()
        .map(|metrics| metrics.network_id.clone())
        .or_else(|| {
            reducer_cohorts
                .first()
                .map(|metrics| metrics.network_id.clone())
        })
        .or_else(|| head_reports.first().map(|report| report.network_id.clone()))?;
    let all_match = peer_windows
        .iter()
        .all(|metrics| metrics.network_id == first)
        && reducer_cohorts
            .iter()
            .all(|metrics| metrics.network_id == first)
        && head_reports.iter().all(|report| report.network_id == first);
    all_match.then_some(first)
}

fn common_experiment_id(
    peer_windows: &[PeerWindowMetrics],
    reducer_cohorts: &[ReducerCohortMetrics],
    head_reports: &[HeadEvalReport],
) -> Option<ExperimentId> {
    common_optional_id(
        peer_windows
            .iter()
            .map(|metrics| metrics.experiment_id.clone()),
        reducer_cohorts
            .iter()
            .map(|metrics| metrics.experiment_id.clone()),
        head_reports
            .iter()
            .map(|report| report.experiment_id.clone()),
    )
}

fn common_revision_id(
    peer_windows: &[PeerWindowMetrics],
    reducer_cohorts: &[ReducerCohortMetrics],
    head_reports: &[HeadEvalReport],
) -> Option<RevisionId> {
    common_optional_id(
        peer_windows
            .iter()
            .map(|metrics| metrics.revision_id.clone()),
        reducer_cohorts
            .iter()
            .map(|metrics| metrics.revision_id.clone()),
        head_reports.iter().map(|report| report.revision_id.clone()),
    )
}

fn common_workload_id(
    peer_windows: &[PeerWindowMetrics],
    reducer_cohorts: &[ReducerCohortMetrics],
    head_reports: &[HeadEvalReport],
) -> Option<WorkloadId> {
    common_optional_id(
        peer_windows
            .iter()
            .map(|metrics| metrics.workload_id.clone()),
        reducer_cohorts
            .iter()
            .map(|metrics| metrics.workload_id.clone()),
        head_reports.iter().map(|report| report.workload_id.clone()),
    )
}

fn common_dataset_view_id(
    peer_windows: &[PeerWindowMetrics],
    reducer_cohorts: &[ReducerCohortMetrics],
    head_reports: &[HeadEvalReport],
) -> Option<DatasetViewId> {
    common_optional_id(
        peer_windows
            .iter()
            .map(|metrics| metrics.dataset_view_id.clone()),
        reducer_cohorts
            .iter()
            .map(|metrics| metrics.dataset_view_id.clone()),
        head_reports
            .iter()
            .map(|report| report.dataset_view_id.clone()),
    )
}

fn common_optional_id<T, I, J, K>(first: I, second: J, third: K) -> Option<T>
where
    T: Clone + Ord,
    I: IntoIterator<Item = T>,
    J: IntoIterator<Item = T>,
    K: IntoIterator<Item = T>,
{
    let mut values = first
        .into_iter()
        .chain(second)
        .chain(third)
        .collect::<BTreeSet<_>>();
    if values.len() == 1 {
        values.pop_first()
    } else {
        None
    }
}

fn latest_captured_at(
    peer_windows: &[PeerWindowMetrics],
    reducer_cohorts: &[ReducerCohortMetrics],
    head_reports: &[HeadEvalReport],
) -> Option<DateTime<Utc>> {
    let latest_peer = peer_windows
        .iter()
        .map(peer_window_end_to_end_finished_at)
        .max();
    let latest_reducer = reducer_cohorts
        .iter()
        .map(|metrics| metrics.captured_at)
        .max();
    let latest_eval = head_reports.iter().map(|report| report.finished_at).max();
    latest_peer
        .into_iter()
        .chain(latest_reducer)
        .chain(latest_eval)
        .max()
}

fn peer_window_active_time_ms(metrics: &PeerWindowMetrics) -> u64 {
    (metrics.window_finished_at - metrics.window_started_at)
        .num_milliseconds()
        .max(0) as u64
}

fn peer_window_end_to_end_time_ms(metrics: &PeerWindowMetrics) -> u64 {
    peer_window_active_time_ms(metrics).saturating_add(metrics.publish_latency_ms)
}

fn peer_window_wait_time_ms(metrics: &PeerWindowMetrics) -> u64 {
    peer_window_active_time_ms(metrics).saturating_sub(
        metrics
            .compute_time_ms
            .saturating_add(metrics.data_fetch_time_ms),
    )
}

fn peer_window_end_to_end_finished_at(metrics: &PeerWindowMetrics) -> DateTime<Utc> {
    metrics.window_finished_at + Duration::milliseconds(metrics.publish_latency_ms as i64)
}

fn peer_idle_time_ms(peer_windows: &[PeerWindowMetrics]) -> u64 {
    let mut sorted = peer_windows.to_vec();
    sorted.sort_by_key(|metrics| metrics.window_started_at);
    sorted
        .windows(2)
        .map(|window| {
            let previous_end = peer_window_end_to_end_finished_at(&window[0]);
            let next_start = window[1].window_started_at;
            if next_start > previous_end {
                (next_start - previous_end).num_milliseconds().max(0) as u64
            } else {
                0
            }
        })
        .sum()
}

fn elapsed_time_ms_from_peer_windows(metrics: &[PeerWindowMetrics]) -> Option<u64> {
    let earliest = metrics
        .iter()
        .map(|metrics| metrics.window_started_at)
        .min()?;
    let latest = metrics
        .iter()
        .map(peer_window_end_to_end_finished_at)
        .max()?;
    Some((latest - earliest).num_milliseconds().max(1) as u64)
}

fn elapsed_time_ms_from_head_reports_ms(reports: &[HeadEvalReport]) -> Option<u64> {
    let earliest = reports.iter().map(|report| report.started_at).min()?;
    let latest = reports.iter().map(|report| report.finished_at).max()?;
    Some((latest - earliest).num_milliseconds().max(1) as u64)
}

fn head_eval_duration_ms(report: &HeadEvalReport) -> u64 {
    (report.finished_at - report.started_at)
        .num_milliseconds()
        .max(0) as u64
}

fn throughput_per_second(units: u64, elapsed_time_ms: u64) -> f64 {
    if units == 0 || elapsed_time_ms == 0 {
        0.0
    } else {
        units as f64 / (elapsed_time_ms as f64 / 1000.0)
    }
}
