use std::{fs, path::PathBuf};

use burn_p2p::{ContributionReceipt, HeadDescriptor, MetricsRetentionBudget, StorageConfig};
use burn_p2p_core::{
    EvalProtocolManifest, ExperimentId, HeadEvalReport, MergeCertificate, PeerWindowMetrics,
    ReducerCohortMetrics, RevisionId,
};
#[cfg(feature = "metrics-indexer")]
use burn_p2p_metrics::{MetricEnvelope, MetricsIndexer, MetricsIndexerConfig, MetricsStore};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Captures one persisted evaluation protocol manifest scoped to a revision.
pub struct StoredEvalProtocolManifestRecord {
    /// Experiment the protocol belongs to.
    pub experiment_id: ExperimentId,
    /// Revision the protocol belongs to.
    pub revision_id: RevisionId,
    /// Timestamp when the protocol record was written.
    pub captured_at: DateTime<Utc>,
    /// Evaluation protocol manifest itself.
    pub manifest: EvalProtocolManifest,
}

pub(crate) struct OperatorHistory {
    pub(crate) receipts: Vec<ContributionReceipt>,
    pub(crate) merges: Vec<MergeCertificate>,
    pub(crate) heads: Vec<HeadDescriptor>,
    pub(crate) peer_window_metrics: Vec<PeerWindowMetrics>,
    pub(crate) reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    pub(crate) head_eval_reports: Vec<HeadEvalReport>,
    pub(crate) eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
}

pub(crate) fn load_operator_history(
    storage: Option<&StorageConfig>,
    metrics_retention: MetricsRetentionBudget,
) -> anyhow::Result<OperatorHistory> {
    #[cfg(not(feature = "metrics-indexer"))]
    let _ = metrics_retention;

    let Some(storage) = storage else {
        return Ok(OperatorHistory {
            receipts: Vec::new(),
            merges: Vec::new(),
            heads: Vec::new(),
            peer_window_metrics: Vec::new(),
            reducer_cohort_metrics: Vec::new(),
            head_eval_reports: Vec::new(),
            eval_protocol_manifests: Vec::new(),
        });
    };
    let receipts_dir = storage.receipts_dir();
    let mut receipt_paths = Vec::<PathBuf>::new();
    let mut merge_paths = Vec::<PathBuf>::new();
    if receipts_dir.exists() {
        for entry in fs::read_dir(&receipts_dir).map_err(|error| {
            anyhow::anyhow!("failed to read {}: {error}", receipts_dir.display())
        })? {
            let path = entry
                .map_err(|error| anyhow::anyhow!("failed to read receipt entry: {error}"))?
                .path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("merge-"))
            {
                merge_paths.push(path);
            } else {
                receipt_paths.push(path);
            }
        }
    }

    let heads_dir = storage.heads_dir();
    let mut head_paths = Vec::<PathBuf>::new();
    if heads_dir.exists() {
        for entry in fs::read_dir(&heads_dir)
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", heads_dir.display()))?
        {
            let path = entry
                .map_err(|error| anyhow::anyhow!("failed to read head entry: {error}"))?
                .path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
                head_paths.push(path);
            }
        }
    }

    let metrics_dir = storage.metrics_dir();
    let mut peer_window_paths = Vec::<PathBuf>::new();
    let mut reducer_cohort_paths = Vec::<PathBuf>::new();
    let mut head_eval_paths = Vec::<PathBuf>::new();
    let mut eval_protocol_paths = Vec::<PathBuf>::new();
    if metrics_dir.exists() {
        for entry in fs::read_dir(&metrics_dir)
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", metrics_dir.display()))?
        {
            let path = entry
                .map_err(|error| anyhow::anyhow!("failed to read metrics entry: {error}"))?
                .path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            match path.file_name().and_then(|name| name.to_str()) {
                Some(name) if name.starts_with("peer-window-") => peer_window_paths.push(path),
                Some(name) if name.starts_with("reducer-cohort-") => {
                    reducer_cohort_paths.push(path);
                }
                Some(name) if name.starts_with("head-eval-") => head_eval_paths.push(path),
                Some(name) if name.starts_with("eval-protocol-") => eval_protocol_paths.push(path),
                _ => {}
            }
        }
    }

    receipt_paths.sort();
    merge_paths.sort();
    head_paths.sort();
    peer_window_paths.sort();
    reducer_cohort_paths.sort();
    head_eval_paths.sort();
    eval_protocol_paths.sort();

    let mut receipts = receipt_paths
        .into_iter()
        .map(load_json_file::<ContributionReceipt>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    receipts.sort_by_key(|receipt| receipt.accepted_at);

    let mut merges = merge_paths
        .into_iter()
        .map(load_json_file::<MergeCertificate>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    merges.sort_by_key(|merge| merge.issued_at);

    let mut heads = head_paths
        .into_iter()
        .map(load_json_file::<HeadDescriptor>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    heads.sort_by_key(|head| head.created_at);

    #[cfg(feature = "metrics-indexer")]
    if storage.metrics_indexer_dir().exists() {
        let store = MetricsStore::open(
            storage.metrics_indexer_dir(),
            crate::metrics::metrics_indexer_config_for_budget(metrics_retention),
        )
        .map_err(anyhow::Error::from)?;
        let entries = store.load_entries();
        if !entries.is_empty() {
            return Ok(operator_history_with_metric_entries(
                receipts,
                merges,
                heads,
                entries,
                load_eval_protocol_manifests(eval_protocol_paths)?,
            ));
        }
    }

    let mut peer_window_metrics = peer_window_paths
        .into_iter()
        .map(load_json_file::<PeerWindowMetrics>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    peer_window_metrics.sort_by_key(|metrics| metrics.window_finished_at);

    let mut reducer_cohort_metrics = reducer_cohort_paths
        .into_iter()
        .map(load_json_file::<ReducerCohortMetrics>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    reducer_cohort_metrics.sort_by_key(|metrics| metrics.captured_at);

    let mut head_eval_reports = head_eval_paths
        .into_iter()
        .map(load_json_file::<HeadEvalReport>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    head_eval_reports.sort_by_key(|report| report.finished_at);
    let mut eval_protocol_manifests = load_eval_protocol_manifests(eval_protocol_paths)?;
    eval_protocol_manifests.sort_by(|left, right| {
        left.experiment_id
            .cmp(&right.experiment_id)
            .then_with(|| left.revision_id.cmp(&right.revision_id))
            .then_with(|| {
                left.manifest
                    .eval_protocol_id
                    .cmp(&right.manifest.eval_protocol_id)
            })
    });

    #[cfg(feature = "metrics-indexer")]
    let (peer_window_metrics, reducer_cohort_metrics, head_eval_reports) = bounded_metric_history(
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
        crate::metrics::metrics_indexer_config_for_budget(metrics_retention),
    );

    Ok(OperatorHistory {
        receipts,
        merges,
        heads,
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
        eval_protocol_manifests,
    })
}

fn load_json_file<T: for<'de> Deserialize<'de>>(path: PathBuf) -> anyhow::Result<T> {
    let bytes = fs::read(&path)
        .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| anyhow::anyhow!("failed to decode {}: {error}", path.display()))
}

fn load_eval_protocol_manifests(
    paths: Vec<PathBuf>,
) -> anyhow::Result<Vec<StoredEvalProtocolManifestRecord>> {
    paths
        .into_iter()
        .map(load_json_file::<StoredEvalProtocolManifestRecord>)
        .collect()
}

#[cfg(feature = "metrics-indexer")]
fn operator_history_with_metric_entries(
    receipts: Vec<ContributionReceipt>,
    merges: Vec<MergeCertificate>,
    heads: Vec<HeadDescriptor>,
    entries: Vec<MetricEnvelope>,
    eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
) -> OperatorHistory {
    let mut peer_window_metrics = Vec::new();
    let mut reducer_cohort_metrics = Vec::new();
    let mut head_eval_reports = Vec::new();

    for entry in entries {
        match entry {
            MetricEnvelope::PeerWindow(metrics) => peer_window_metrics.push(metrics),
            MetricEnvelope::ReducerCohort(metrics) => reducer_cohort_metrics.push(metrics),
            MetricEnvelope::HeadEval(report) => head_eval_reports.push(report),
        }
    }
    peer_window_metrics.sort_by_key(|metrics| metrics.window_finished_at);
    reducer_cohort_metrics.sort_by_key(|metrics| metrics.captured_at);
    head_eval_reports.sort_by_key(|report| report.finished_at);

    OperatorHistory {
        receipts,
        merges,
        heads,
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
        eval_protocol_manifests,
    }
}

#[cfg(feature = "metrics-indexer")]
fn bounded_metric_history(
    peer_window_metrics: Vec<PeerWindowMetrics>,
    reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    head_eval_reports: Vec<HeadEvalReport>,
    config: MetricsIndexerConfig,
) -> (
    Vec<PeerWindowMetrics>,
    Vec<ReducerCohortMetrics>,
    Vec<HeadEvalReport>,
) {
    let mut indexer = MetricsIndexer::new(config);
    for metrics in peer_window_metrics {
        indexer.ingest_peer_window_metrics(metrics);
    }
    for metrics in reducer_cohort_metrics {
        indexer.ingest_reducer_cohort_metrics(metrics);
    }
    for report in head_eval_reports {
        indexer.ingest_head_eval_report(report);
    }

    let mut peer_window_metrics = Vec::new();
    let mut reducer_cohort_metrics = Vec::new();
    let mut head_eval_reports = Vec::new();
    for entry in indexer.into_entries() {
        match entry {
            MetricEnvelope::PeerWindow(metrics) => peer_window_metrics.push(metrics),
            MetricEnvelope::ReducerCohort(metrics) => reducer_cohort_metrics.push(metrics),
            MetricEnvelope::HeadEval(report) => head_eval_reports.push(report),
        }
    }
    peer_window_metrics.sort_by_key(|metrics| metrics.window_finished_at);
    reducer_cohort_metrics.sort_by_key(|metrics| metrics.captured_at);
    head_eval_reports.sort_by_key(|report| report.finished_at);
    (
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
    )
}
