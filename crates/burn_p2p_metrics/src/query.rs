use std::collections::BTreeMap;

use burn_p2p_core::{
    DatasetViewId, ExperimentId, HeadId, NetworkId, PeerWindowMetrics, PeerWindowStatus,
    RevisionId, WorkloadId,
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
