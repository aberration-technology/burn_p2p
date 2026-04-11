use std::{collections::BTreeMap, fs, path::PathBuf};

use burn_p2p::{ContributionReceipt, HeadDescriptor, MetricsRetentionBudget, StorageConfig};
use burn_p2p_core::{
    EvalProtocolManifest, ExperimentId, HeadEvalReport, MergeCertificate, Page, PageRequest,
    PeerWindowMetrics, ReducerCohortMetrics, RevisionId,
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
    pub(crate) totals: OperatorHistoryTotals,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OperatorHistoryTotals {
    pub(crate) receipt_count: usize,
    pub(crate) merge_count: usize,
    pub(crate) head_count: usize,
    pub(crate) eval_protocol_manifest_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OperatorHistoryMemoryBudget {
    pub(crate) max_receipts_in_memory: usize,
    pub(crate) max_merges_in_memory: usize,
    pub(crate) max_heads_in_memory: usize,
    pub(crate) max_eval_protocol_manifests_in_memory: usize,
}

impl Default for OperatorHistoryMemoryBudget {
    fn default() -> Self {
        Self {
            max_receipts_in_memory: 256,
            max_merges_in_memory: 256,
            max_heads_in_memory: 256,
            max_eval_protocol_manifests_in_memory: 64,
        }
    }
}

pub(crate) fn load_operator_history(
    storage: Option<&StorageConfig>,
    metrics_retention: MetricsRetentionBudget,
    memory_budget: OperatorHistoryMemoryBudget,
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
            totals: OperatorHistoryTotals::default(),
        });
    };
    let mut receipts = load_all_contribution_receipts(storage)?;
    receipts.sort_by_key(|receipt| receipt.accepted_at);
    let receipt_count = receipts.len();
    receipts = retain_recent(receipts, memory_budget.max_receipts_in_memory, |receipt| {
        receipt.accepted_at
    });

    let mut merges = load_all_merge_certificates(storage)?;
    merges.sort_by_key(|merge| merge.issued_at);
    let merge_count = merges.len();
    merges = retain_recent(merges, memory_budget.max_merges_in_memory, |merge| {
        merge.issued_at
    });

    let mut heads = load_all_head_descriptors(storage)?;
    heads.sort_by_key(|head| head.created_at);
    let head_count = heads.len();
    heads = retain_recent(heads, memory_budget.max_heads_in_memory, |head| {
        head.created_at
    });

    let mut eval_protocol_manifests = load_all_eval_protocol_manifests(storage)?;
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
    let eval_protocol_manifest_count = eval_protocol_manifests.len();
    eval_protocol_manifests = retain_recent(
        eval_protocol_manifests,
        memory_budget.max_eval_protocol_manifests_in_memory,
        |record| record.captured_at,
    );

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
                eval_protocol_manifests,
                OperatorHistoryTotals {
                    receipt_count,
                    merge_count,
                    head_count,
                    eval_protocol_manifest_count,
                },
            ));
        }
    }

    let peer_window_metrics = retain_recent_per_group(
        load_peer_window_metrics(storage)?,
        metrics_retention.max_peer_window_entries_per_revision,
        |metrics| (metrics.experiment_id.clone(), metrics.revision_id.clone()),
        |metrics| metrics.window_finished_at,
    );

    let reducer_cohort_metrics = retain_recent_per_group(
        load_reducer_cohort_metrics(storage)?,
        metrics_retention.max_reducer_cohort_entries_per_revision,
        |metrics| (metrics.experiment_id.clone(), metrics.revision_id.clone()),
        |metrics| metrics.captured_at,
    );

    let head_eval_reports = retain_recent_per_group(
        load_all_head_eval_reports(storage)?,
        metrics_retention.max_head_eval_reports_per_revision,
        |report| (report.experiment_id.clone(), report.revision_id.clone()),
        |report| report.finished_at,
    );

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
        totals: OperatorHistoryTotals {
            receipt_count,
            merge_count,
            head_count,
            eval_protocol_manifest_count,
        },
    })
}

fn load_json_file<T: for<'de> Deserialize<'de>>(path: PathBuf) -> anyhow::Result<T> {
    let bytes = fs::read(&path)
        .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| anyhow::anyhow!("failed to decode {}: {error}", path.display()))
}

#[cfg(feature = "metrics-indexer")]
fn operator_history_with_metric_entries(
    receipts: Vec<ContributionReceipt>,
    merges: Vec<MergeCertificate>,
    heads: Vec<HeadDescriptor>,
    entries: Vec<MetricEnvelope>,
    eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
    totals: OperatorHistoryTotals,
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
        totals,
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

pub(crate) fn load_all_contribution_receipts(
    storage: &StorageConfig,
) -> anyhow::Result<Vec<ContributionReceipt>> {
    read_storage_json_entries(
        storage.receipts_dir(),
        |name| name.ends_with(".json") && !name.starts_with("merge-"),
        load_json_file::<ContributionReceipt>,
    )
}

pub(crate) fn load_paged_contribution_receipts<F>(
    storage: &StorageConfig,
    page: PageRequest,
    matches: F,
) -> anyhow::Result<Page<ContributionReceipt>>
where
    F: Fn(&ContributionReceipt) -> bool,
{
    read_storage_json_entries_page(
        storage.receipts_dir(),
        |name| name.ends_with(".json") && !name.starts_with("merge-"),
        load_json_file::<ContributionReceipt>,
        matches,
        page,
    )
}

pub(crate) fn load_all_merge_certificates(
    storage: &StorageConfig,
) -> anyhow::Result<Vec<MergeCertificate>> {
    read_storage_json_entries(
        storage.receipts_dir(),
        |name| name.ends_with(".json") && name.starts_with("merge-"),
        load_json_file::<MergeCertificate>,
    )
}

pub(crate) fn load_paged_merge_certificates<F>(
    storage: &StorageConfig,
    page: PageRequest,
    matches: F,
) -> anyhow::Result<Page<MergeCertificate>>
where
    F: Fn(&MergeCertificate) -> bool,
{
    read_storage_json_entries_page(
        storage.receipts_dir(),
        |name| name.ends_with(".json") && name.starts_with("merge-"),
        load_json_file::<MergeCertificate>,
        matches,
        page,
    )
}

pub(crate) fn load_all_head_descriptors(
    storage: &StorageConfig,
) -> anyhow::Result<Vec<HeadDescriptor>> {
    read_storage_json_entries(
        storage.heads_dir(),
        |name| name.ends_with(".json"),
        load_json_file::<HeadDescriptor>,
    )
}

pub(crate) fn load_paged_head_descriptors<F>(
    storage: &StorageConfig,
    page: PageRequest,
    matches: F,
) -> anyhow::Result<Page<HeadDescriptor>>
where
    F: Fn(&HeadDescriptor) -> bool,
{
    read_storage_json_entries_page(
        storage.heads_dir(),
        |name| name.ends_with(".json"),
        load_json_file::<HeadDescriptor>,
        matches,
        page,
    )
}

pub(crate) fn load_all_head_eval_reports(
    storage: &StorageConfig,
) -> anyhow::Result<Vec<HeadEvalReport>> {
    read_storage_json_entries(
        storage.metrics_dir(),
        |name| name.starts_with("head-eval-") && name.ends_with(".json"),
        load_json_file::<HeadEvalReport>,
    )
}

pub(crate) fn load_all_eval_protocol_manifests(
    storage: &StorageConfig,
) -> anyhow::Result<Vec<StoredEvalProtocolManifestRecord>> {
    read_storage_json_entries(
        storage.metrics_dir(),
        |name| name.starts_with("eval-protocol-") && name.ends_with(".json"),
        load_json_file::<StoredEvalProtocolManifestRecord>,
    )
}

pub(crate) fn load_peer_window_metrics(
    storage: &StorageConfig,
) -> anyhow::Result<Vec<PeerWindowMetrics>> {
    read_storage_json_entries(
        storage.metrics_dir(),
        |name| name.starts_with("peer-window-") && name.ends_with(".json"),
        load_json_file::<PeerWindowMetrics>,
    )
}

pub(crate) fn load_reducer_cohort_metrics(
    storage: &StorageConfig,
) -> anyhow::Result<Vec<ReducerCohortMetrics>> {
    read_storage_json_entries(
        storage.metrics_dir(),
        |name| name.starts_with("reducer-cohort-") && name.ends_with(".json"),
        load_json_file::<ReducerCohortMetrics>,
    )
}

fn read_storage_json_entries<T, F, L>(
    dir: PathBuf,
    predicate: F,
    loader: L,
) -> anyhow::Result<Vec<T>>
where
    F: Fn(&str) -> bool,
    L: Fn(PathBuf) -> anyhow::Result<T>,
{
    let mut paths = Vec::<PathBuf>::new();
    if dir.exists() {
        for entry in fs::read_dir(&dir)
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", dir.display()))?
        {
            let path = entry
                .map_err(|error| {
                    anyhow::anyhow!("failed to read {} entry: {error}", dir.display())
                })?
                .path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if predicate(name) {
                paths.push(path);
            }
        }
    }
    paths.sort();
    paths.into_iter().map(loader).collect()
}

fn read_storage_json_entries_page<T, F, L, M>(
    dir: PathBuf,
    predicate: F,
    loader: L,
    matches: M,
    page: PageRequest,
) -> anyhow::Result<Page<T>>
where
    F: Fn(&str) -> bool,
    L: Fn(PathBuf) -> anyhow::Result<T>,
    M: Fn(&T) -> bool,
{
    let page = page.normalized();
    let mut paths = Vec::<PathBuf>::new();
    if dir.exists() {
        for entry in fs::read_dir(&dir)
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", dir.display()))?
        {
            let path = entry
                .map_err(|error| {
                    anyhow::anyhow!("failed to read {} entry: {error}", dir.display())
                })?
                .path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if predicate(name) {
                paths.push(path);
            }
        }
    }
    paths.sort();

    let mut items = Vec::new();
    let mut total = 0usize;
    for path in paths {
        let value = loader(path)?;
        if !matches(&value) {
            continue;
        }
        if total >= page.offset && items.len() < page.limit {
            items.push(value);
        }
        total += 1;
    }

    Ok(Page::new(items, page, total))
}

fn retain_recent<T, F>(mut values: Vec<T>, limit: usize, key: F) -> Vec<T>
where
    F: Fn(&T) -> DateTime<Utc>,
{
    if values.len() <= limit.max(1) {
        return values;
    }

    values.sort_by_key(|right| std::cmp::Reverse(key(right)));
    values.truncate(limit.max(1));
    values.sort_by_key(|value| key(value));
    values
}

fn retain_recent_per_group<T, K, G, F>(
    mut values: Vec<T>,
    limit_per_group: usize,
    group: G,
    key: F,
) -> Vec<T>
where
    K: Ord,
    G: Fn(&T) -> K,
    F: Fn(&T) -> DateTime<Utc>,
{
    let limit_per_group = limit_per_group.max(1);
    values.sort_by_key(|value| (group(value), key(value)));

    let mut retained = Vec::with_capacity(values.len().min(limit_per_group));
    let mut counts = BTreeMap::<K, usize>::new();
    for value in values.into_iter().rev() {
        let group_key = group(&value);
        let count = counts.entry(group_key).or_default();
        if *count >= limit_per_group {
            continue;
        }
        *count += 1;
        retained.push(value);
    }

    retained.sort_by_key(|value| key(value));
    retained
}
