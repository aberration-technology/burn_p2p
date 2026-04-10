use std::collections::BTreeMap;

use burn_p2p_core::{
    DatasetViewId, ExperimentId, HeadEvalReport, HeadEvalStatus, HeadId, MetricTrustClass,
    NetworkId, PeerId, PeerWindowMetrics, PeerWindowStatus, RevisionId, WorkloadId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One cumulative checkpoint-adoption sample for a canonical head.
pub struct CanonicalHeadAdoptionPoint {
    /// Timestamp when the sampled peer-window batch started.
    pub observed_at: DateTime<Utc>,
    /// Milliseconds since the canonical head was certified.
    pub elapsed_since_certified_ms: u64,
    /// Number of peer windows seen so far in the head's visibility interval.
    pub cumulative_window_count: usize,
    /// Number of peer windows seen so far that started from the canonical head.
    pub cumulative_adopted_window_count: usize,
    /// Share of peer windows seen so far that adopted the canonical head.
    pub adoption_coverage: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Cumulative adoption curve for one canonical head inside its visibility interval.
pub struct CanonicalHeadAdoptionCurve {
    /// Network identifier covered by the curve.
    pub network_id: NetworkId,
    /// Experiment identifier covered by the curve.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the curve.
    pub revision_id: RevisionId,
    /// Workload identifier covered by the curve.
    pub workload_id: WorkloadId,
    /// Dataset view identifier covered by the curve.
    pub dataset_view_id: DatasetViewId,
    /// Canonical head whose diffusion is being measured.
    pub canonical_head_id: HeadId,
    /// Parent head for the canonical head, when known.
    pub parent_head_id: Option<HeadId>,
    /// Timestamp when the canonical head was certified.
    pub certified_at: DateTime<Utc>,
    /// Timestamp when the next canonical head was certified, when known.
    pub next_canonical_certified_at: Option<DateTime<Utc>>,
    /// Number of peer windows in the measured interval.
    pub total_window_count: usize,
    /// Number of peer windows in the measured interval that adopted the canonical head.
    pub adopted_window_count: usize,
    /// Final cumulative adoption coverage for the interval.
    pub final_adoption_coverage: f64,
    /// Cumulative adoption samples over time.
    pub points: Vec<CanonicalHeadAdoptionPoint>,
    /// Latest timestamp covered by the curve.
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One visible-head bucket inside a latest-head population histogram.
pub struct VisibleHeadPopulationBucket {
    /// Base head currently visible in the peer population bucket.
    pub base_head_id: HeadId,
    /// Number of peers whose latest recent window used this base head.
    pub peer_count: usize,
    /// Share of visible peers whose latest recent window used this base head.
    pub peer_share: f64,
    /// Number of recent peer windows that used this base head.
    pub recent_window_count: usize,
    /// Share of recent peer windows that used this base head.
    pub recent_window_share: f64,
    /// Whether the bucket corresponds to the latest canonical head.
    pub is_latest_canonical: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Histogram of the latest visible peer population after a canonical promotion.
pub struct VisibleHeadPopulationHistogram {
    /// Network identifier covered by the histogram.
    pub network_id: NetworkId,
    /// Experiment identifier covered by the histogram.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the histogram.
    pub revision_id: RevisionId,
    /// Workload identifier covered by the histogram.
    pub workload_id: WorkloadId,
    /// Dataset view identifier covered by the histogram.
    pub dataset_view_id: DatasetViewId,
    /// Latest canonical head anchoring the histogram.
    pub canonical_head_id: HeadId,
    /// Parent head for the latest canonical head, when known.
    pub parent_head_id: Option<HeadId>,
    /// Timestamp when the latest canonical head was certified.
    pub certified_at: DateTime<Utc>,
    /// Number of peers with a visible recent window after certification.
    pub total_visible_peer_count: usize,
    /// Number of peer windows observed after certification.
    pub total_recent_window_count: usize,
    /// Population buckets grouped by visible base head.
    pub buckets: Vec<VisibleHeadPopulationBucket>,
    /// Latest timestamp covered by the histogram.
    pub captured_at: DateTime<Utc>,
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

/// Derives canonical-head adoption curves across one or more experiment revisions.
pub fn derive_canonical_head_adoption_curves(
    peer_windows: &[PeerWindowMetrics],
    head_reports: &[HeadEvalReport],
) -> Vec<CanonicalHeadAdoptionCurve> {
    let mut curves = Vec::new();
    let grouped_windows = peer_windows.iter().fold(
        BTreeMap::<(ExperimentId, RevisionId), Vec<PeerWindowMetrics>>::new(),
        |mut acc, metrics| {
            acc.entry((metrics.experiment_id.clone(), metrics.revision_id.clone()))
                .or_default()
                .push(metrics.clone());
            acc
        },
    );
    let grouped_reports = head_reports.iter().fold(
        BTreeMap::<(ExperimentId, RevisionId), Vec<HeadEvalReport>>::new(),
        |mut acc, report| {
            acc.entry((report.experiment_id.clone(), report.revision_id.clone()))
                .or_default()
                .push(report.clone());
            acc
        },
    );

    for (revision_key, reports) in grouped_reports {
        let Some(canonical_reports) = canonical_head_anchors(&reports) else {
            continue;
        };
        let revision_windows = grouped_windows
            .get(&revision_key)
            .cloned()
            .unwrap_or_default();
        for (index, report) in canonical_reports.iter().enumerate() {
            let next_certified_at = canonical_reports
                .get(index + 1)
                .map(|next| next.finished_at);
            let mut interval_windows = revision_windows
                .iter()
                .filter(|metrics| {
                    metrics.window_started_at >= report.finished_at
                        && next_certified_at
                            .map(|next| metrics.window_started_at < next)
                            .unwrap_or(true)
                })
                .cloned()
                .collect::<Vec<_>>();
            if interval_windows.is_empty() {
                continue;
            }
            interval_windows.sort_by(|left, right| {
                left.window_started_at
                    .cmp(&right.window_started_at)
                    .then_with(|| left.peer_id.cmp(&right.peer_id))
            });

            let grouped_points = interval_windows.iter().fold(
                BTreeMap::<DateTime<Utc>, Vec<PeerWindowMetrics>>::new(),
                |mut acc, metrics| {
                    acc.entry(metrics.window_started_at)
                        .or_default()
                        .push(metrics.clone());
                    acc
                },
            );
            let mut cumulative_window_count = 0usize;
            let mut cumulative_adopted_window_count = 0usize;
            let mut points = Vec::new();
            for (observed_at, windows_at_ts) in grouped_points {
                cumulative_window_count += windows_at_ts.len();
                cumulative_adopted_window_count += windows_at_ts
                    .iter()
                    .filter(|metrics| metrics.base_head_id == report.head_id)
                    .count();
                points.push(CanonicalHeadAdoptionPoint {
                    observed_at,
                    elapsed_since_certified_ms: (observed_at - report.finished_at)
                        .num_milliseconds()
                        .max(0) as u64,
                    cumulative_window_count,
                    cumulative_adopted_window_count,
                    adoption_coverage: cumulative_adopted_window_count as f64
                        / cumulative_window_count as f64,
                });
            }
            let adopted_window_count = interval_windows
                .iter()
                .filter(|metrics| metrics.base_head_id == report.head_id)
                .count();
            curves.push(CanonicalHeadAdoptionCurve {
                network_id: report.network_id.clone(),
                experiment_id: report.experiment_id.clone(),
                revision_id: report.revision_id.clone(),
                workload_id: report.workload_id.clone(),
                dataset_view_id: report.dataset_view_id.clone(),
                canonical_head_id: report.head_id.clone(),
                parent_head_id: report.base_head_id.clone(),
                certified_at: report.finished_at,
                next_canonical_certified_at: next_certified_at,
                total_window_count: interval_windows.len(),
                adopted_window_count,
                final_adoption_coverage: adopted_window_count as f64
                    / interval_windows.len() as f64,
                captured_at: interval_windows
                    .iter()
                    .map(|metrics| metrics.window_finished_at)
                    .max()
                    .unwrap_or(report.finished_at),
                points,
            });
        }
    }

    curves.sort_by(|left, right| {
        right
            .certified_at
            .cmp(&left.certified_at)
            .then_with(|| left.experiment_id.cmp(&right.experiment_id))
            .then_with(|| left.revision_id.cmp(&right.revision_id))
            .then_with(|| left.canonical_head_id.cmp(&right.canonical_head_id))
    });
    curves
}

/// Derives latest-canonical visible-head population histograms across experiment revisions.
pub fn derive_latest_canonical_head_population_histograms(
    peer_windows: &[PeerWindowMetrics],
    head_reports: &[HeadEvalReport],
) -> Vec<VisibleHeadPopulationHistogram> {
    let grouped_windows = peer_windows.iter().fold(
        BTreeMap::<(ExperimentId, RevisionId), Vec<PeerWindowMetrics>>::new(),
        |mut acc, metrics| {
            acc.entry((metrics.experiment_id.clone(), metrics.revision_id.clone()))
                .or_default()
                .push(metrics.clone());
            acc
        },
    );
    let grouped_reports = head_reports.iter().fold(
        BTreeMap::<(ExperimentId, RevisionId), Vec<HeadEvalReport>>::new(),
        |mut acc, report| {
            acc.entry((report.experiment_id.clone(), report.revision_id.clone()))
                .or_default()
                .push(report.clone());
            acc
        },
    );

    let mut histograms = Vec::new();
    for (revision_key, reports) in grouped_reports {
        let Some(canonical_reports) = canonical_head_anchors(&reports) else {
            continue;
        };
        let Some(latest_canonical) = canonical_reports.last() else {
            continue;
        };
        let mut recent_windows = grouped_windows
            .get(&revision_key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|metrics| metrics.window_started_at >= latest_canonical.finished_at)
            .collect::<Vec<_>>();
        if recent_windows.is_empty() {
            continue;
        }
        recent_windows.sort_by(|left, right| {
            right
                .window_started_at
                .cmp(&left.window_started_at)
                .then_with(|| right.window_finished_at.cmp(&left.window_finished_at))
                .then_with(|| left.peer_id.cmp(&right.peer_id))
        });

        let latest_by_peer = recent_windows.iter().fold(
            BTreeMap::<PeerId, PeerWindowMetrics>::new(),
            |mut acc, metrics| {
                acc.entry(metrics.peer_id.clone())
                    .and_modify(|current| {
                        if metrics.window_started_at > current.window_started_at
                            || (metrics.window_started_at == current.window_started_at
                                && metrics.window_finished_at > current.window_finished_at)
                        {
                            *current = metrics.clone();
                        }
                    })
                    .or_insert_with(|| metrics.clone());
                acc
            },
        );
        let latest_peer_count_by_head = latest_by_peer.into_values().fold(
            BTreeMap::<HeadId, usize>::new(),
            |mut acc, metrics| {
                *acc.entry(metrics.base_head_id.clone()).or_default() += 1;
                acc
            },
        );
        let recent_window_count_by_head =
            recent_windows
                .iter()
                .fold(BTreeMap::<HeadId, usize>::new(), |mut acc, metrics| {
                    *acc.entry(metrics.base_head_id.clone()).or_default() += 1;
                    acc
                });
        let total_visible_peer_count = latest_peer_count_by_head.values().sum::<usize>();
        let total_recent_window_count = recent_windows.len();
        let mut buckets = latest_peer_count_by_head
            .into_iter()
            .map(|(base_head_id, peer_count)| {
                let recent_window_count = recent_window_count_by_head
                    .get(&base_head_id)
                    .copied()
                    .unwrap_or_default();
                VisibleHeadPopulationBucket {
                    base_head_id: base_head_id.clone(),
                    peer_count,
                    peer_share: peer_count as f64 / total_visible_peer_count.max(1) as f64,
                    recent_window_count,
                    recent_window_share: recent_window_count as f64
                        / total_recent_window_count.max(1) as f64,
                    is_latest_canonical: base_head_id == latest_canonical.head_id,
                }
            })
            .collect::<Vec<_>>();
        buckets.sort_by(|left, right| {
            right
                .peer_count
                .cmp(&left.peer_count)
                .then_with(|| right.recent_window_count.cmp(&left.recent_window_count))
                .then_with(|| left.base_head_id.cmp(&right.base_head_id))
        });
        histograms.push(VisibleHeadPopulationHistogram {
            network_id: latest_canonical.network_id.clone(),
            experiment_id: latest_canonical.experiment_id.clone(),
            revision_id: latest_canonical.revision_id.clone(),
            workload_id: latest_canonical.workload_id.clone(),
            dataset_view_id: latest_canonical.dataset_view_id.clone(),
            canonical_head_id: latest_canonical.head_id.clone(),
            parent_head_id: latest_canonical.base_head_id.clone(),
            certified_at: latest_canonical.finished_at,
            total_visible_peer_count,
            total_recent_window_count,
            captured_at: recent_windows
                .iter()
                .map(|metrics| metrics.window_finished_at)
                .max()
                .unwrap_or(latest_canonical.finished_at),
            buckets,
        });
    }

    histograms.sort_by(|left, right| {
        right
            .certified_at
            .cmp(&left.certified_at)
            .then_with(|| left.experiment_id.cmp(&right.experiment_id))
            .then_with(|| left.revision_id.cmp(&right.revision_id))
    });
    histograms
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

fn canonical_head_anchors(head_reports: &[HeadEvalReport]) -> Option<Vec<HeadEvalReport>> {
    let mut by_head = head_reports
        .iter()
        .filter(|report| {
            report.status == HeadEvalStatus::Completed
                && report.trust_class == MetricTrustClass::Canonical
        })
        .fold(
            BTreeMap::<HeadId, HeadEvalReport>::new(),
            |mut acc, report| {
                acc.entry(report.head_id.clone())
                    .and_modify(|current| {
                        if report.finished_at < current.finished_at {
                            *current = report.clone();
                        }
                    })
                    .or_insert_with(|| report.clone());
                acc
            },
        )
        .into_values()
        .collect::<Vec<_>>();
    if by_head.is_empty() {
        return None;
    }
    by_head.sort_by_key(|report| report.finished_at);
    Some(by_head)
}

fn percentile_nearest_rank(sorted_values: &[f64], percentile: f64) -> f64 {
    debug_assert!(!sorted_values.is_empty());
    let rank = (percentile.clamp(0.0, 1.0) * sorted_values.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted_values.len() - 1);
    sorted_values[index]
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
