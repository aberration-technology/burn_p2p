use std::{collections::BTreeMap, path::PathBuf};

use crate::state::{
    HeadQuery, OperatorAuditKind, OperatorAuditQuery, OperatorAuditRecord, OperatorAuditSummary,
    OperatorReplayQuery, OperatorReplaySnapshot, OperatorReplaySnapshotSummary,
    OperatorRetentionSummary, ReceiptQuery,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p::FsArtifactStore;
use burn_p2p::{ContributionReceipt, HeadDescriptor, MetricsRetentionBudget, StorageConfig};
use burn_p2p_core::{
    BrowserLeaderboardSnapshot, ExperimentId, HeadEvalReport, HeadId, MergeCertificate, NetworkId,
    Page, PageRequest, PeerId, PeerWindowMetrics, PrincipalId, PublicationTarget,
    ReducerCohortMetrics, RevisionId, StudyId,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p_publish::PublicationStore;
use chrono::{DateTime, Utc};
use postgres::{NoTls, types::ToSql};
use redis::Commands;
use serde::{Deserialize, Serialize};

use crate::{StoredEvalProtocolManifestRecord, history, state::page_from_filtered};

pub(crate) trait OperatorStore {
    fn receipts(&self, query: &ReceiptQuery) -> anyhow::Result<Vec<ContributionReceipt>>;
    fn receipts_page(
        &self,
        query: &ReceiptQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<ContributionReceipt>>;
    fn heads(&self, query: &HeadQuery) -> anyhow::Result<Vec<HeadDescriptor>>;
    fn heads_page(
        &self,
        query: &HeadQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<HeadDescriptor>>;
    fn merges(&self) -> anyhow::Result<Vec<MergeCertificate>>;
    fn merges_page(&self, page: PageRequest) -> anyhow::Result<Page<MergeCertificate>>;
    fn audit_page(
        &self,
        query: &OperatorAuditQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorAuditRecord>>;
    fn audit_summary(&self, query: &OperatorAuditQuery) -> anyhow::Result<OperatorAuditSummary>;
    fn replay_snapshot(
        &self,
        captured_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<Option<OperatorReplaySnapshot>>;
    fn replay_page(
        &self,
        query: &OperatorReplayQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorReplaySnapshotSummary>>;
    fn retention_summary(&self) -> anyhow::Result<OperatorRetentionSummary>;
    fn head_eval_reports(&self, head_id: &HeadId) -> anyhow::Result<Vec<HeadEvalReport>>;
    #[cfg(feature = "artifact-publish")]
    fn eval_protocol_manifests(&self) -> anyhow::Result<Vec<StoredEvalProtocolManifestRecord>>;
    fn leaderboard_snapshot(
        &self,
        network_id: &NetworkId,
        peer_principals: &BTreeMap<PeerId, PrincipalId>,
        captured_at: DateTime<Utc>,
    ) -> anyhow::Result<BrowserLeaderboardSnapshot>;

    #[cfg(feature = "artifact-publish")]
    fn artifact_store(&self) -> anyhow::Result<Option<FsArtifactStore>>;

    #[cfg(feature = "artifact-publish")]
    fn publication_store(&self) -> anyhow::Result<Option<PublicationStore>>;
}

#[derive(Clone, Debug)]
pub(crate) struct FileOperatorStoreConfig {
    pub(crate) history_root: Option<PathBuf>,
    pub(crate) metrics_store_root: Option<PathBuf>,
    pub(crate) metrics_retention: MetricsRetentionBudget,
    pub(crate) publication_store_root: Option<PathBuf>,
    pub(crate) publication_targets: Vec<PublicationTarget>,
    pub(crate) artifact_store_root: Option<PathBuf>,
    pub(crate) operator_state_backend: Option<OperatorStateBackendConfig>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct FileOperatorStorePreview {
    pub(crate) receipts: Vec<ContributionReceipt>,
    pub(crate) heads: Vec<HeadDescriptor>,
    pub(crate) merges: Vec<MergeCertificate>,
    pub(crate) peer_window_metrics: Vec<PeerWindowMetrics>,
    pub(crate) reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    pub(crate) head_eval_reports: Vec<HeadEvalReport>,
    pub(crate) eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperatorStateBackendConfig {
    Redis {
        url: String,
        snapshot_key: String,
    },
    Postgres {
        url: String,
        table_name: String,
        snapshot_key: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OperatorStateSnapshot {
    captured_at: DateTime<Utc>,
    preview: FileOperatorStorePreview,
}

#[derive(Clone, Debug)]
struct PostgresOperatorStateRow {
    captured_at: DateTime<Utc>,
    preview: FileOperatorStorePreview,
}

fn postgres_operator_audit_table_name(snapshot_table_name: &str) -> String {
    format!("{snapshot_table_name}_audit")
}

fn operator_snapshot_retention_limit(retention: MetricsRetentionBudget) -> usize {
    (retention.max_metric_revisions_per_experiment.max(1) * 32).max(128)
}

fn operator_audit_retention_limit(retention: MetricsRetentionBudget) -> usize {
    let per_revision = retention.max_peer_window_entries_per_revision
        + retention.max_reducer_cohort_entries_per_revision
        + retention.max_head_eval_reports_per_revision
        + 256;
    (retention.max_metric_revisions_per_experiment.max(1) * per_revision).max(2_048)
}

fn replay_snapshot_from_preview(
    captured_at: DateTime<Utc>,
    preview: FileOperatorStorePreview,
) -> OperatorReplaySnapshot {
    OperatorReplaySnapshot {
        captured_at,
        receipts: preview.receipts,
        heads: preview.heads,
        merges: preview.merges,
        peer_window_metrics: preview.peer_window_metrics,
        reducer_cohort_metrics: preview.reducer_cohort_metrics,
        head_eval_reports: preview.head_eval_reports,
        eval_protocol_manifests: preview.eval_protocol_manifests,
    }
}

fn preview_scope_ids(
    preview: &FileOperatorStorePreview,
) -> (
    Vec<StudyId>,
    Vec<ExperimentId>,
    Vec<RevisionId>,
    Vec<HeadId>,
) {
    let mut study_ids = BTreeMap::<StudyId, ()>::new();
    let mut experiment_ids = BTreeMap::<ExperimentId, ()>::new();
    let mut revision_ids = BTreeMap::<RevisionId, ()>::new();
    let mut head_ids = BTreeMap::<HeadId, ()>::new();

    for receipt in &preview.receipts {
        study_ids.insert(receipt.study_id.clone(), ());
        experiment_ids.insert(receipt.experiment_id.clone(), ());
        revision_ids.insert(receipt.revision_id.clone(), ());
        head_ids.insert(receipt.base_head_id.clone(), ());
    }
    for head in &preview.heads {
        study_ids.insert(head.study_id.clone(), ());
        experiment_ids.insert(head.experiment_id.clone(), ());
        revision_ids.insert(head.revision_id.clone(), ());
        head_ids.insert(head.head_id.clone(), ());
        if let Some(parent) = head.parent_head_id.as_ref() {
            head_ids.insert(parent.clone(), ());
        }
    }
    for merge in &preview.merges {
        study_ids.insert(merge.study_id.clone(), ());
        experiment_ids.insert(merge.experiment_id.clone(), ());
        revision_ids.insert(merge.revision_id.clone(), ());
        head_ids.insert(merge.base_head_id.clone(), ());
        head_ids.insert(merge.merged_head_id.clone(), ());
    }
    for metrics in &preview.peer_window_metrics {
        experiment_ids.insert(metrics.experiment_id.clone(), ());
        revision_ids.insert(metrics.revision_id.clone(), ());
        head_ids.insert(metrics.base_head_id.clone(), ());
    }
    for metrics in &preview.reducer_cohort_metrics {
        experiment_ids.insert(metrics.experiment_id.clone(), ());
        revision_ids.insert(metrics.revision_id.clone(), ());
        head_ids.insert(metrics.base_head_id.clone(), ());
        if let Some(candidate_head_id) = metrics.candidate_head_id.as_ref() {
            head_ids.insert(candidate_head_id.clone(), ());
        }
    }
    for report in &preview.head_eval_reports {
        experiment_ids.insert(report.experiment_id.clone(), ());
        revision_ids.insert(report.revision_id.clone(), ());
        head_ids.insert(report.head_id.clone(), ());
        if let Some(base_head_id) = report.base_head_id.as_ref() {
            head_ids.insert(base_head_id.clone(), ());
        }
    }

    (
        study_ids.into_keys().collect(),
        experiment_ids.into_keys().collect(),
        revision_ids.into_keys().collect(),
        head_ids.into_keys().collect(),
    )
}

fn build_replay_snapshot_summary(
    preview: &FileOperatorStorePreview,
    captured_at: DateTime<Utc>,
) -> OperatorReplaySnapshotSummary {
    let (study_ids, experiment_ids, revision_ids, head_ids) = preview_scope_ids(preview);
    let summary = BTreeMap::from([
        ("kind".into(), "operator_snapshot".into()),
        ("heads".into(), preview.heads.len().to_string()),
        ("receipts".into(), preview.receipts.len().to_string()),
        ("merges".into(), preview.merges.len().to_string()),
        (
            "peer_window_metrics".into(),
            preview.peer_window_metrics.len().to_string(),
        ),
        (
            "reducer_cohort_metrics".into(),
            preview.reducer_cohort_metrics.len().to_string(),
        ),
        (
            "head_eval_reports".into(),
            preview.head_eval_reports.len().to_string(),
        ),
    ]);

    OperatorReplaySnapshotSummary {
        captured_at,
        study_ids,
        experiment_ids,
        revision_ids,
        head_ids,
        receipt_count: preview.receipts.len(),
        head_count: preview.heads.len(),
        merge_count: preview.merges.len(),
        peer_window_metric_count: preview.peer_window_metrics.len(),
        reducer_cohort_metric_count: preview.reducer_cohort_metrics.len(),
        head_eval_report_count: preview.head_eval_reports.len(),
        summary,
    }
}

fn build_audit_records_from_preview(
    preview: &FileOperatorStorePreview,
    snapshot_captured_at: Option<DateTime<Utc>>,
) -> Vec<OperatorAuditRecord> {
    let mut records = Vec::new();

    for receipt in &preview.receipts {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::Receipt,
            record_id: receipt.receipt_id.as_str().to_owned(),
            study_id: Some(receipt.study_id.clone()),
            experiment_id: Some(receipt.experiment_id.clone()),
            revision_id: Some(receipt.revision_id.clone()),
            peer_id: Some(receipt.peer_id.clone()),
            head_id: Some(receipt.base_head_id.clone()),
            captured_at: receipt.accepted_at,
            summary: BTreeMap::from([
                ("kind".into(), "receipt".into()),
                (
                    "accepted_weight".into(),
                    format!("{:.6}", receipt.accepted_weight),
                ),
                (
                    "artifact_id".into(),
                    receipt.artifact_id.as_str().to_owned(),
                ),
            ]),
        });
    }

    for head in &preview.heads {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::Head,
            record_id: head.head_id.as_str().to_owned(),
            study_id: Some(head.study_id.clone()),
            experiment_id: Some(head.experiment_id.clone()),
            revision_id: Some(head.revision_id.clone()),
            peer_id: None,
            head_id: Some(head.head_id.clone()),
            captured_at: head.created_at,
            summary: BTreeMap::from([
                ("kind".into(), "head".into()),
                ("artifact_id".into(), head.artifact_id.as_str().to_owned()),
                ("global_step".into(), head.global_step.to_string()),
            ]),
        });
    }

    for merge in &preview.merges {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::Merge,
            record_id: merge.merge_cert_id.as_str().to_owned(),
            study_id: Some(merge.study_id.clone()),
            experiment_id: Some(merge.experiment_id.clone()),
            revision_id: Some(merge.revision_id.clone()),
            peer_id: Some(merge.validator.clone()),
            head_id: Some(merge.merged_head_id.clone()),
            captured_at: merge.issued_at,
            summary: BTreeMap::from([
                ("kind".into(), "merge".into()),
                (
                    "merged_artifact_id".into(),
                    merge.merged_artifact_id.as_str().to_owned(),
                ),
                ("policy".into(), format!("{:?}", merge.policy)),
            ]),
        });
    }

    for metrics in &preview.peer_window_metrics {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::PeerWindowMetric,
            record_id: metrics.lease_id.as_str().to_owned(),
            study_id: None,
            experiment_id: Some(metrics.experiment_id.clone()),
            revision_id: Some(metrics.revision_id.clone()),
            peer_id: Some(metrics.peer_id.clone()),
            head_id: Some(metrics.base_head_id.clone()),
            captured_at: metrics.window_finished_at,
            summary: BTreeMap::from([
                ("kind".into(), "peer_window_metric".into()),
                ("status".into(), format!("{:?}", metrics.status)),
                (
                    "accepted_tokens_or_samples".into(),
                    metrics
                        .accepted_tokens_or_samples
                        .unwrap_or_default()
                        .to_string(),
                ),
                (
                    "head_lag_at_finish".into(),
                    metrics.head_lag_at_finish.to_string(),
                ),
            ]),
        });
    }

    for metrics in &preview.reducer_cohort_metrics {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::ReducerCohortMetric,
            record_id: metrics.merge_window_id.as_str().to_owned(),
            study_id: None,
            experiment_id: Some(metrics.experiment_id.clone()),
            revision_id: Some(metrics.revision_id.clone()),
            peer_id: None,
            head_id: metrics.candidate_head_id.clone(),
            captured_at: metrics.captured_at,
            summary: BTreeMap::from([
                ("kind".into(), "reducer_cohort_metric".into()),
                ("status".into(), format!("{:?}", metrics.status)),
                (
                    "accepted_updates".into(),
                    metrics.accepted_updates.to_string(),
                ),
                (
                    "accepted_tokens_or_samples".into(),
                    metrics.accepted_tokens_or_samples.to_string(),
                ),
            ]),
        });
    }

    for report in &preview.head_eval_reports {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::HeadEvalReport,
            record_id: report.eval_protocol_id.as_str().to_owned(),
            study_id: None,
            experiment_id: Some(report.experiment_id.clone()),
            revision_id: Some(report.revision_id.clone()),
            peer_id: None,
            head_id: Some(report.head_id.clone()),
            captured_at: report.finished_at,
            summary: BTreeMap::from([
                ("kind".into(), "head_eval_report".into()),
                ("status".into(), format!("{:?}", report.status)),
                ("sample_count".into(), report.sample_count.to_string()),
            ]),
        });
    }

    if let Some(captured_at) = snapshot_captured_at {
        let replay_summary = build_replay_snapshot_summary(preview, captured_at);
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::Snapshot,
            record_id: format!("snapshot:{}", captured_at.timestamp_millis()),
            study_id: None,
            experiment_id: None,
            revision_id: None,
            peer_id: None,
            head_id: None,
            captured_at,
            summary: replay_summary.summary,
        });
    }

    records
}

#[derive(Clone, Debug)]
pub(crate) struct FileOperatorStore {
    history_root: Option<PathBuf>,
    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    metrics_store_root: Option<PathBuf>,
    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    metrics_retention: MetricsRetentionBudget,
    #[cfg_attr(not(feature = "artifact-publish"), allow(dead_code))]
    publication_store_root: Option<PathBuf>,
    #[cfg_attr(not(feature = "artifact-publish"), allow(dead_code))]
    publication_targets: Vec<PublicationTarget>,
    #[cfg_attr(not(feature = "artifact-publish"), allow(dead_code))]
    artifact_store_root: Option<PathBuf>,
    operator_state_backend: Option<OperatorStateBackendConfig>,
    receipt_preview: Vec<ContributionReceipt>,
    head_preview: Vec<HeadDescriptor>,
    merge_preview: Vec<MergeCertificate>,
    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    peer_window_preview: Vec<PeerWindowMetrics>,
    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    reducer_cohort_preview: Vec<ReducerCohortMetrics>,
    head_eval_preview: Vec<HeadEvalReport>,
    #[cfg_attr(not(feature = "artifact-publish"), allow(dead_code))]
    eval_protocol_preview: Vec<StoredEvalProtocolManifestRecord>,
}

impl FileOperatorStore {
    pub(crate) fn new(config: FileOperatorStoreConfig, preview: FileOperatorStorePreview) -> Self {
        Self {
            history_root: config.history_root,
            metrics_store_root: config.metrics_store_root,
            metrics_retention: config.metrics_retention,
            publication_store_root: config.publication_store_root,
            publication_targets: config.publication_targets,
            artifact_store_root: config.artifact_store_root,
            operator_state_backend: config.operator_state_backend,
            receipt_preview: preview.receipts,
            head_preview: preview.heads,
            merge_preview: preview.merges,
            peer_window_preview: preview.peer_window_metrics,
            reducer_cohort_preview: preview.reducer_cohort_metrics,
            head_eval_preview: preview.head_eval_reports,
            eval_protocol_preview: preview.eval_protocol_manifests,
        }
    }

    fn storage_config(&self) -> Option<StorageConfig> {
        self.history_root.clone().map(StorageConfig::new)
    }

    fn redis_connection(url: &str) -> anyhow::Result<redis::Connection> {
        Ok(redis::Client::open(url)?.get_connection()?)
    }

    fn remote_snapshot(&self) -> anyhow::Result<Option<OperatorStateSnapshot>> {
        let Some(backend) = self.operator_state_backend.as_ref() else {
            return Ok(None);
        };

        match backend {
            OperatorStateBackendConfig::Redis { url, snapshot_key } => {
                let mut connection = Self::redis_connection(url)?;
                let payload = connection.get::<_, Option<String>>(snapshot_key)?;
                payload
                    .map(|encoded| {
                        serde_json::from_str::<OperatorStateSnapshot>(&encoded)
                            .map_err(anyhow::Error::from)
                    })
                    .transpose()
            }
            OperatorStateBackendConfig::Postgres {
                url,
                table_name,
                snapshot_key,
            } => load_postgres_operator_state_snapshot(url, table_name, snapshot_key),
        }
    }

    fn remote_preview(&self) -> anyhow::Result<Option<FileOperatorStorePreview>> {
        self.remote_snapshot()
            .map(|snapshot| snapshot.map(|snapshot| snapshot.preview))
    }

    pub(crate) fn all_head_eval_reports(&self) -> anyhow::Result<Vec<HeadEvalReport>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(preview.head_eval_reports);
        }

        #[cfg(feature = "metrics-indexer")]
        if let Some(root) = self.metrics_store_root.as_ref() {
            let store = burn_p2p_metrics::MetricsStore::open(
                root,
                crate::metrics::metrics_indexer_config_for_budget(self.metrics_retention),
            )
            .map_err(anyhow::Error::from)?;
            let mut reports = store
                .load_entries()
                .into_iter()
                .filter_map(|entry| match entry {
                    burn_p2p_metrics::MetricEnvelope::HeadEval(report) => Some(report),
                    _ => None,
                })
                .collect::<Vec<_>>();
            reports.sort_by_key(|report| report.finished_at);
            return Ok(reports);
        }

        self.storage_config()
            .map(|storage| history::load_all_head_eval_reports(&storage))
            .transpose()
            .map(|loaded| loaded.unwrap_or_else(|| self.head_eval_preview.clone()))
    }

    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    pub(crate) fn all_peer_window_metrics(&self) -> anyhow::Result<Vec<PeerWindowMetrics>> {
        if let Some(preview) = self.remote_preview()? {
            let mut metrics = preview.peer_window_metrics;
            metrics.sort_by_key(|metrics| metrics.window_finished_at);
            return Ok(metrics);
        }

        #[cfg(feature = "metrics-indexer")]
        if let Some(root) = self.metrics_store_root.as_ref() {
            let store = burn_p2p_metrics::MetricsStore::open(
                root,
                crate::metrics::metrics_indexer_config_for_budget(self.metrics_retention),
            )
            .map_err(anyhow::Error::from)?;
            let mut metrics = store
                .load_entries()
                .into_iter()
                .filter_map(|entry| match entry {
                    burn_p2p_metrics::MetricEnvelope::PeerWindow(metrics) => Some(metrics),
                    _ => None,
                })
                .collect::<Vec<_>>();
            metrics.sort_by_key(|metrics| metrics.window_finished_at);
            return Ok(metrics);
        }

        self.storage_config()
            .map(|storage| history::load_peer_window_metrics(&storage))
            .transpose()
            .map(|loaded| {
                let mut metrics = loaded.unwrap_or_else(|| self.peer_window_preview.clone());
                metrics.sort_by_key(|metrics| metrics.window_finished_at);
                metrics
            })
    }

    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    pub(crate) fn all_reducer_cohort_metrics(&self) -> anyhow::Result<Vec<ReducerCohortMetrics>> {
        if let Some(preview) = self.remote_preview()? {
            let mut metrics = preview.reducer_cohort_metrics;
            metrics.sort_by_key(|metrics| metrics.captured_at);
            return Ok(metrics);
        }

        #[cfg(feature = "metrics-indexer")]
        if let Some(root) = self.metrics_store_root.as_ref() {
            let store = burn_p2p_metrics::MetricsStore::open(
                root,
                crate::metrics::metrics_indexer_config_for_budget(self.metrics_retention),
            )
            .map_err(anyhow::Error::from)?;
            let mut metrics = store
                .load_entries()
                .into_iter()
                .filter_map(|entry| match entry {
                    burn_p2p_metrics::MetricEnvelope::ReducerCohort(metrics) => Some(metrics),
                    _ => None,
                })
                .collect::<Vec<_>>();
            metrics.sort_by_key(|metrics| metrics.captured_at);
            return Ok(metrics);
        }

        self.storage_config()
            .map(|storage| history::load_reducer_cohort_metrics(&storage))
            .transpose()
            .map(|loaded| {
                let mut metrics = loaded.unwrap_or_else(|| self.reducer_cohort_preview.clone());
                metrics.sort_by_key(|metrics| metrics.captured_at);
                metrics
            })
    }

    fn build_audit_records(&self) -> anyhow::Result<Vec<OperatorAuditRecord>> {
        let preview = FileOperatorStorePreview {
            receipts: self.receipts(&ReceiptQuery::default())?,
            heads: self.heads(&HeadQuery::default())?,
            merges: self.merges()?,
            peer_window_metrics: self.all_peer_window_metrics()?,
            reducer_cohort_metrics: self.all_reducer_cohort_metrics()?,
            head_eval_reports: self.all_head_eval_reports()?,
            #[cfg(feature = "artifact-publish")]
            eval_protocol_manifests: self.eval_protocol_manifests()?,
            #[cfg(not(feature = "artifact-publish"))]
            eval_protocol_manifests: self.eval_protocol_preview.clone(),
        };
        let snapshot_captured_at = self.remote_snapshot()?.map(|snapshot| snapshot.captured_at);
        let mut records = build_audit_records_from_preview(&preview, snapshot_captured_at);

        records.sort_by(|left, right| {
            right
                .captured_at
                .cmp(&left.captured_at)
                .then_with(|| left.record_id.cmp(&right.record_id))
        });
        Ok(records)
    }
}

fn ensure_valid_postgres_identifier(identifier: &str) -> anyhow::Result<&str> {
    if identifier.is_empty()
        || !identifier
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        anyhow::bail!("invalid postgres identifier `{identifier}`");
    }
    Ok(identifier)
}

fn postgres_connection(url: &str) -> anyhow::Result<postgres::Client> {
    Ok(postgres::Client::connect(url, NoTls)?)
}

fn ensure_postgres_operator_state_table(
    client: &mut postgres::Client,
    table_name: &str,
) -> anyhow::Result<()> {
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    client.batch_execute(&format!(
        r#"
CREATE TABLE IF NOT EXISTS {table_name} (
    id BIGSERIAL PRIMARY KEY,
    snapshot_key TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    preview_json JSONB NOT NULL,
    summary_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    summary_text TEXT NOT NULL DEFAULT '',
    study_ids TEXT[] NOT NULL DEFAULT '{{}}',
    experiment_ids TEXT[] NOT NULL DEFAULT '{{}}',
    revision_ids TEXT[] NOT NULL DEFAULT '{{}}',
    head_ids TEXT[] NOT NULL DEFAULT '{{}}',
    receipt_count BIGINT NOT NULL DEFAULT 0,
    head_count BIGINT NOT NULL DEFAULT 0,
    merge_count BIGINT NOT NULL DEFAULT 0,
    peer_window_metric_count BIGINT NOT NULL DEFAULT 0,
    reducer_cohort_metric_count BIGINT NOT NULL DEFAULT 0,
    head_eval_report_count BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS {table_name}_snapshot_lookup
    ON {table_name} (snapshot_key, captured_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS {table_name}_captured_lookup
    ON {table_name} (snapshot_key, captured_at DESC);
CREATE INDEX IF NOT EXISTS {table_name}_study_ids_gin
    ON {table_name} USING GIN (study_ids);
CREATE INDEX IF NOT EXISTS {table_name}_experiment_ids_gin
    ON {table_name} USING GIN (experiment_ids);
CREATE INDEX IF NOT EXISTS {table_name}_revision_ids_gin
    ON {table_name} USING GIN (revision_ids);
CREATE INDEX IF NOT EXISTS {table_name}_head_ids_gin
    ON {table_name} USING GIN (head_ids);
"#
    ))?;
    Ok(())
}

fn ensure_postgres_operator_audit_table(
    client: &mut postgres::Client,
    table_name: &str,
) -> anyhow::Result<()> {
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    client.batch_execute(&format!(
        r#"
CREATE TABLE IF NOT EXISTS {table_name} (
    id BIGSERIAL PRIMARY KEY,
    snapshot_key TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    record_id TEXT NOT NULL,
    study_id TEXT NULL,
    experiment_id TEXT NULL,
    revision_id TEXT NULL,
    peer_id TEXT NULL,
    head_id TEXT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    summary_json JSONB NOT NULL,
    summary_text TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_snapshot_dedupe
    ON {table_name} (snapshot_key, dedupe_key);
CREATE INDEX IF NOT EXISTS {table_name}_snapshot_lookup
    ON {table_name} (snapshot_key, captured_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS {table_name}_kind_lookup
    ON {table_name} (snapshot_key, kind, captured_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS {table_name}_revision_lookup
    ON {table_name} (snapshot_key, experiment_id, revision_id, captured_at DESC, id DESC);
"#
    ))?;
    Ok(())
}

fn operator_audit_record_dedupe_key(record: &OperatorAuditRecord) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        record.kind.as_slug(),
        record.record_id,
        record.captured_at.timestamp_millis(),
        record
            .study_id
            .as_ref()
            .map(StudyId::as_str)
            .unwrap_or_default(),
        record
            .experiment_id
            .as_ref()
            .map(ExperimentId::as_str)
            .unwrap_or_default(),
        record
            .revision_id
            .as_ref()
            .map(RevisionId::as_str)
            .unwrap_or_default(),
        record
            .peer_id
            .as_ref()
            .map(PeerId::as_str)
            .unwrap_or_default(),
    )
}

fn operator_audit_record_summary_text(record: &OperatorAuditRecord) -> String {
    let mut text = record.record_id.clone();
    for (key, value) in &record.summary {
        text.push(' ');
        text.push_str(key);
        text.push('=');
        text.push_str(value);
    }
    text
}

fn replay_snapshot_summary_text(summary: &OperatorReplaySnapshotSummary) -> String {
    let mut text = summary
        .study_ids
        .iter()
        .map(StudyId::as_str)
        .chain(summary.experiment_ids.iter().map(ExperimentId::as_str))
        .chain(summary.revision_ids.iter().map(RevisionId::as_str))
        .chain(summary.head_ids.iter().map(HeadId::as_str))
        .collect::<Vec<_>>()
        .join(" ");
    for (key, value) in &summary.summary {
        text.push(' ');
        text.push_str(key);
        text.push('=');
        text.push_str(value);
    }
    text.trim().to_owned()
}

fn operator_backend_label(backend: Option<&OperatorStateBackendConfig>) -> String {
    match backend {
        Some(OperatorStateBackendConfig::Redis { .. }) => "redis".into(),
        Some(OperatorStateBackendConfig::Postgres { .. }) => "postgres".into(),
        None => "file".into(),
    }
}

fn build_operator_audit_summary(
    backend: String,
    records: impl IntoIterator<Item = OperatorAuditRecord>,
) -> OperatorAuditSummary {
    let mut record_count = 0usize;
    let mut counts_by_kind = BTreeMap::<String, usize>::new();
    let mut studies = BTreeMap::<StudyId, ()>::new();
    let mut experiments = BTreeMap::<ExperimentId, ()>::new();
    let mut revisions = BTreeMap::<RevisionId, ()>::new();
    let mut peers = BTreeMap::<PeerId, ()>::new();
    let mut heads = BTreeMap::<HeadId, ()>::new();
    let mut earliest_captured_at: Option<chrono::DateTime<chrono::Utc>> = None;
    let mut latest_captured_at: Option<chrono::DateTime<chrono::Utc>> = None;

    for record in records {
        record_count += 1;
        *counts_by_kind
            .entry(record.kind.as_slug().to_owned())
            .or_default() += 1;
        if let Some(study_id) = record.study_id {
            studies.insert(study_id, ());
        }
        if let Some(experiment_id) = record.experiment_id {
            experiments.insert(experiment_id, ());
        }
        if let Some(revision_id) = record.revision_id {
            revisions.insert(revision_id, ());
        }
        if let Some(peer_id) = record.peer_id {
            peers.insert(peer_id, ());
        }
        if let Some(head_id) = record.head_id {
            heads.insert(head_id, ());
        }
        earliest_captured_at = Some(match earliest_captured_at {
            Some(current) => current.min(record.captured_at),
            None => record.captured_at,
        });
        latest_captured_at = Some(match latest_captured_at {
            Some(current) => current.max(record.captured_at),
            None => record.captured_at,
        });
    }

    OperatorAuditSummary {
        backend,
        record_count,
        counts_by_kind,
        distinct_study_count: studies.len(),
        distinct_experiment_count: experiments.len(),
        distinct_revision_count: revisions.len(),
        distinct_peer_count: peers.len(),
        distinct_head_count: heads.len(),
        earliest_captured_at,
        latest_captured_at,
    }
}

fn insert_postgres_operator_audit_records(
    client: &mut postgres::Client,
    table_name: &str,
    snapshot_key: &str,
    records: &[OperatorAuditRecord],
) -> anyhow::Result<()> {
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    for record in records {
        let dedupe_key = operator_audit_record_dedupe_key(record);
        let kind = record.kind.as_slug().to_owned();
        let study_id = record
            .study_id
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let experiment_id = record
            .experiment_id
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let revision_id = record
            .revision_id
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let peer_id = record
            .peer_id
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let head_id = record
            .head_id
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let summary_json = serde_json::to_string(&record.summary)?;
        let summary_text = operator_audit_record_summary_text(record);
        client.execute(
            &format!(
                "INSERT INTO {table_name} \
                 (snapshot_key, dedupe_key, kind, record_id, study_id, experiment_id, revision_id, peer_id, head_id, captured_at, summary_json, summary_text) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12) \
                 ON CONFLICT (snapshot_key, dedupe_key) DO NOTHING"
            ),
            &[
                &snapshot_key,
                &dedupe_key,
                &kind,
                &record.record_id,
                &study_id,
                &experiment_id,
                &revision_id,
                &peer_id,
                &head_id,
                &record.captured_at,
                &summary_json,
                &summary_text,
            ],
        )?;
    }
    Ok(())
}

fn prune_postgres_snapshot_rows(
    client: &mut postgres::Client,
    table_name: &str,
    snapshot_key: &str,
    max_rows: usize,
) -> anyhow::Result<()> {
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    client.execute(
        &format!(
            "DELETE FROM {table_name} WHERE id IN (\
                SELECT id FROM {table_name} \
                WHERE snapshot_key = $1 \
                ORDER BY captured_at DESC, id DESC \
                OFFSET $2\
            )"
        ),
        &[&snapshot_key, &i64::try_from(max_rows).unwrap_or(i64::MAX)],
    )?;
    Ok(())
}

fn prune_postgres_audit_rows(
    client: &mut postgres::Client,
    table_name: &str,
    snapshot_key: &str,
    max_rows: usize,
) -> anyhow::Result<()> {
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    client.execute(
        &format!(
            "DELETE FROM {table_name} WHERE id IN (\
                SELECT id FROM {table_name} \
                WHERE snapshot_key = $1 \
                ORDER BY captured_at DESC, id DESC \
                OFFSET $2\
            )"
        ),
        &[&snapshot_key, &i64::try_from(max_rows).unwrap_or(i64::MAX)],
    )?;
    Ok(())
}

fn load_postgres_operator_audit_page(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
    query: &OperatorAuditQuery,
    page: PageRequest,
) -> anyhow::Result<Page<OperatorAuditRecord>> {
    let page = page.normalized();
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    ensure_postgres_operator_audit_table(&mut client, table_name)?;

    let mut params: Vec<Box<dyn ToSql + Sync>> = vec![Box::new(snapshot_key.to_owned())];
    let mut clauses = vec!["snapshot_key = $1".to_owned()];
    if let Some(kind) = query.kind.as_ref() {
        params.push(Box::new(kind.as_slug().to_owned()));
        clauses.push(format!("kind = ${}", params.len()));
    }
    if let Some(study_id) = query.study_id.as_ref() {
        params.push(Box::new(study_id.as_str().to_owned()));
        clauses.push(format!("study_id = ${}", params.len()));
    }
    if let Some(experiment_id) = query.experiment_id.as_ref() {
        params.push(Box::new(experiment_id.as_str().to_owned()));
        clauses.push(format!("experiment_id = ${}", params.len()));
    }
    if let Some(revision_id) = query.revision_id.as_ref() {
        params.push(Box::new(revision_id.as_str().to_owned()));
        clauses.push(format!("revision_id = ${}", params.len()));
    }
    if let Some(peer_id) = query.peer_id.as_ref() {
        params.push(Box::new(peer_id.as_str().to_owned()));
        clauses.push(format!("peer_id = ${}", params.len()));
    }
    if let Some(head_id) = query.head_id.as_ref() {
        params.push(Box::new(head_id.as_str().to_owned()));
        clauses.push(format!("head_id = ${}", params.len()));
    }
    if let Some(since) = query.since.as_ref() {
        params.push(Box::new(since.to_owned()));
        clauses.push(format!("captured_at >= ${}", params.len()));
    }
    if let Some(until) = query.until.as_ref() {
        params.push(Box::new(until.to_owned()));
        clauses.push(format!("captured_at <= ${}", params.len()));
    }
    if let Some(text) = query
        .text
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let pattern = format!("%{}%", text.to_ascii_lowercase());
        params.push(Box::new(pattern.clone()));
        let record_idx = params.len();
        params.push(Box::new(pattern));
        let summary_idx = params.len();
        clauses.push(format!(
            "(LOWER(record_id) LIKE ${record_idx} OR LOWER(summary_text) LIKE ${summary_idx})"
        ));
    }
    let where_sql = clauses.join(" AND ");
    let refs = params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    let count = client
        .query_one(
            &format!("SELECT COUNT(*) FROM {table_name} WHERE {where_sql}"),
            &refs,
        )?
        .get::<_, i64>(0)
        .max(0) as usize;

    let mut page_params = params;
    page_params.push(Box::new(i64::try_from(page.offset).unwrap_or(i64::MAX)));
    let offset_idx = page_params.len();
    page_params.push(Box::new(i64::try_from(page.limit).unwrap_or(i64::MAX)));
    let limit_idx = page_params.len();
    let page_refs = page_params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    let rows = client.query(
        &format!(
            "SELECT kind, record_id, study_id, experiment_id, revision_id, peer_id, head_id, captured_at, summary_json::text \
             FROM {table_name} \
             WHERE {where_sql} \
             ORDER BY captured_at DESC, id DESC \
             OFFSET ${offset_idx} LIMIT ${limit_idx}"
        ),
        &page_refs,
    )?;

    let items = rows
        .into_iter()
        .map(|row| {
            let kind = OperatorAuditKind::from_slug(&row.get::<_, String>(0))
                .ok_or_else(|| anyhow::anyhow!("unknown operator audit kind stored in postgres"))?;
            let summary_json = row.get::<_, String>(8);
            Ok(OperatorAuditRecord {
                kind,
                record_id: row.get(1),
                study_id: row.get::<_, Option<String>>(2).map(StudyId::new),
                experiment_id: row.get::<_, Option<String>>(3).map(ExperimentId::new),
                revision_id: row.get::<_, Option<String>>(4).map(RevisionId::new),
                peer_id: row.get::<_, Option<String>>(5).map(PeerId::new),
                head_id: row.get::<_, Option<String>>(6).map(HeadId::new),
                captured_at: row.get(7),
                summary: serde_json::from_str(&summary_json)?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(Page::new(items, page, count))
}

fn load_postgres_operator_audit_summary(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
    query: &OperatorAuditQuery,
) -> anyhow::Result<OperatorAuditSummary> {
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    ensure_postgres_operator_audit_table(&mut client, table_name)?;

    let mut params: Vec<Box<dyn ToSql + Sync>> = vec![Box::new(snapshot_key.to_owned())];
    let mut clauses = vec!["snapshot_key = $1".to_owned()];
    if let Some(kind) = query.kind.as_ref() {
        params.push(Box::new(kind.as_slug().to_owned()));
        clauses.push(format!("kind = ${}", params.len()));
    }
    if let Some(study_id) = query.study_id.as_ref() {
        params.push(Box::new(study_id.as_str().to_owned()));
        clauses.push(format!("study_id = ${}", params.len()));
    }
    if let Some(experiment_id) = query.experiment_id.as_ref() {
        params.push(Box::new(experiment_id.as_str().to_owned()));
        clauses.push(format!("experiment_id = ${}", params.len()));
    }
    if let Some(revision_id) = query.revision_id.as_ref() {
        params.push(Box::new(revision_id.as_str().to_owned()));
        clauses.push(format!("revision_id = ${}", params.len()));
    }
    if let Some(peer_id) = query.peer_id.as_ref() {
        params.push(Box::new(peer_id.as_str().to_owned()));
        clauses.push(format!("peer_id = ${}", params.len()));
    }
    if let Some(head_id) = query.head_id.as_ref() {
        params.push(Box::new(head_id.as_str().to_owned()));
        clauses.push(format!("head_id = ${}", params.len()));
    }
    if let Some(since) = query.since.as_ref() {
        params.push(Box::new(*since));
        clauses.push(format!("captured_at >= ${}", params.len()));
    }
    if let Some(until) = query.until.as_ref() {
        params.push(Box::new(*until));
        clauses.push(format!("captured_at <= ${}", params.len()));
    }
    if let Some(text) = query
        .text
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let pattern = format!("%{}%", text.to_ascii_lowercase());
        params.push(Box::new(pattern.clone()));
        let record_idx = params.len();
        params.push(Box::new(pattern));
        let summary_idx = params.len();
        clauses.push(format!(
            "(LOWER(record_id) LIKE ${record_idx} OR LOWER(summary_text) LIKE ${summary_idx})"
        ));
    }
    let where_sql = clauses.join(" AND ");
    let refs = params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    let summary_row = client.query_one(
        &format!(
            "SELECT COUNT(*), \
                    COUNT(DISTINCT study_id), \
                    COUNT(DISTINCT experiment_id), \
                    COUNT(DISTINCT revision_id), \
                    COUNT(DISTINCT peer_id), \
                    COUNT(DISTINCT head_id), \
                    MIN(captured_at), \
                    MAX(captured_at) \
             FROM {table_name} WHERE {where_sql}"
        ),
        &refs,
    )?;
    let kind_rows = client.query(
        &format!(
            "SELECT kind, COUNT(*) \
             FROM {table_name} \
             WHERE {where_sql} \
             GROUP BY kind \
             ORDER BY kind ASC"
        ),
        &refs,
    )?;
    let counts_by_kind = kind_rows
        .into_iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, i64>(1).max(0) as usize,
            )
        })
        .collect();

    Ok(OperatorAuditSummary {
        backend: "postgres".into(),
        record_count: summary_row.get::<_, i64>(0).max(0) as usize,
        counts_by_kind,
        distinct_study_count: summary_row.get::<_, i64>(1).max(0) as usize,
        distinct_experiment_count: summary_row.get::<_, i64>(2).max(0) as usize,
        distinct_revision_count: summary_row.get::<_, i64>(3).max(0) as usize,
        distinct_peer_count: summary_row.get::<_, i64>(4).max(0) as usize,
        distinct_head_count: summary_row.get::<_, i64>(5).max(0) as usize,
        earliest_captured_at: summary_row.get(6),
        latest_captured_at: summary_row.get(7),
    })
}

fn load_postgres_operator_replay_page(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
    query: &OperatorReplayQuery,
    page: PageRequest,
) -> anyhow::Result<Page<OperatorReplaySnapshotSummary>> {
    let page = page.normalized();
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    ensure_postgres_operator_state_table(&mut client, table_name)?;

    let mut params: Vec<Box<dyn ToSql + Sync>> = vec![Box::new(snapshot_key.to_owned())];
    let mut clauses = vec!["snapshot_key = $1".to_owned()];
    if let Some(study_id) = query.study_id.as_ref() {
        params.push(Box::new(study_id.as_str().to_owned()));
        clauses.push(format!("${} = ANY(study_ids)", params.len()));
    }
    if let Some(experiment_id) = query.experiment_id.as_ref() {
        params.push(Box::new(experiment_id.as_str().to_owned()));
        clauses.push(format!("${} = ANY(experiment_ids)", params.len()));
    }
    if let Some(revision_id) = query.revision_id.as_ref() {
        params.push(Box::new(revision_id.as_str().to_owned()));
        clauses.push(format!("${} = ANY(revision_ids)", params.len()));
    }
    if let Some(head_id) = query.head_id.as_ref() {
        params.push(Box::new(head_id.as_str().to_owned()));
        clauses.push(format!("${} = ANY(head_ids)", params.len()));
    }
    if let Some(since) = query.since.as_ref() {
        params.push(Box::new(*since));
        clauses.push(format!("captured_at >= ${}", params.len()));
    }
    if let Some(until) = query.until.as_ref() {
        params.push(Box::new(*until));
        clauses.push(format!("captured_at <= ${}", params.len()));
    }
    if let Some(text) = query
        .text
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        params.push(Box::new(format!("%{}%", text.to_ascii_lowercase())));
        clauses.push(format!("LOWER(summary_text) LIKE ${}", params.len()));
    }
    let where_sql = clauses.join(" AND ");
    let refs = params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    let count = client
        .query_one(
            &format!("SELECT COUNT(*) FROM {table_name} WHERE {where_sql}"),
            &refs,
        )?
        .get::<_, i64>(0)
        .max(0) as usize;

    let mut page_params = params;
    page_params.push(Box::new(i64::try_from(page.offset).unwrap_or(i64::MAX)));
    let offset_idx = page_params.len();
    page_params.push(Box::new(i64::try_from(page.limit).unwrap_or(i64::MAX)));
    let limit_idx = page_params.len();
    let page_refs = page_params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    let rows = client.query(
        &format!(
            "SELECT captured_at, study_ids, experiment_ids, revision_ids, head_ids, \
                    receipt_count, head_count, merge_count, peer_window_metric_count, \
                    reducer_cohort_metric_count, head_eval_report_count, summary_json::text \
             FROM {table_name} \
             WHERE {where_sql} \
             ORDER BY captured_at DESC, id DESC \
             OFFSET ${offset_idx} LIMIT ${limit_idx}"
        ),
        &page_refs,
    )?;

    let items = rows
        .into_iter()
        .map(|row| {
            Ok(OperatorReplaySnapshotSummary {
                captured_at: row.get(0),
                study_ids: row
                    .get::<_, Vec<String>>(1)
                    .into_iter()
                    .map(StudyId::new)
                    .collect(),
                experiment_ids: row
                    .get::<_, Vec<String>>(2)
                    .into_iter()
                    .map(ExperimentId::new)
                    .collect(),
                revision_ids: row
                    .get::<_, Vec<String>>(3)
                    .into_iter()
                    .map(RevisionId::new)
                    .collect(),
                head_ids: row
                    .get::<_, Vec<String>>(4)
                    .into_iter()
                    .map(HeadId::new)
                    .collect(),
                receipt_count: row.get::<_, i64>(5).max(0) as usize,
                head_count: row.get::<_, i64>(6).max(0) as usize,
                merge_count: row.get::<_, i64>(7).max(0) as usize,
                peer_window_metric_count: row.get::<_, i64>(8).max(0) as usize,
                reducer_cohort_metric_count: row.get::<_, i64>(9).max(0) as usize,
                head_eval_report_count: row.get::<_, i64>(10).max(0) as usize,
                summary: serde_json::from_str(&row.get::<_, String>(11))?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(Page::new(items, page, count))
}

fn load_postgres_operator_retention_summary(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
    metrics_retention: MetricsRetentionBudget,
) -> anyhow::Result<OperatorRetentionSummary> {
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    let audit_table_name = postgres_operator_audit_table_name(table_name);
    ensure_postgres_operator_state_table(&mut client, table_name)?;
    ensure_postgres_operator_audit_table(&mut client, &audit_table_name)?;

    let snapshot_row = client.query_one(
        &format!("SELECT COUNT(*), MAX(captured_at) FROM {table_name} WHERE snapshot_key = $1"),
        &[&snapshot_key],
    )?;
    let audit_row = client.query_one(
        &format!("SELECT COUNT(*) FROM {audit_table_name} WHERE snapshot_key = $1"),
        &[&snapshot_key],
    )?;

    Ok(OperatorRetentionSummary {
        backend: "postgres".into(),
        metrics_retention,
        snapshot_retention_limit: operator_snapshot_retention_limit(metrics_retention),
        audit_retention_limit: operator_audit_retention_limit(metrics_retention),
        persisted_snapshot_count: snapshot_row.get::<_, i64>(0).max(0) as usize,
        persisted_audit_record_count: audit_row.get::<_, i64>(0).max(0) as usize,
        latest_snapshot_at: snapshot_row.get(1),
    })
}

fn load_postgres_operator_state_snapshot(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
) -> anyhow::Result<Option<OperatorStateSnapshot>> {
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    ensure_postgres_operator_state_table(&mut client, table_name)?;
    let row = client.query_opt(
        &format!(
            "SELECT captured_at, preview_json::text FROM {table_name} WHERE snapshot_key = $1 ORDER BY captured_at DESC, id DESC LIMIT 1"
        ),
        &[&snapshot_key],
    )?;
    row.map(|row| {
        let captured_at = row.get::<_, DateTime<Utc>>(0);
        let preview_json = row.get::<_, String>(1);
        let preview = serde_json::from_str::<FileOperatorStorePreview>(&preview_json)?;
        Ok(OperatorStateSnapshot {
            captured_at,
            preview,
        })
    })
    .transpose()
}

fn load_postgres_operator_state_rows(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
) -> anyhow::Result<Vec<PostgresOperatorStateRow>> {
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    ensure_postgres_operator_state_table(&mut client, table_name)?;
    let rows = client.query(
        &format!(
            "SELECT captured_at, preview_json::text FROM {table_name} WHERE snapshot_key = $1 ORDER BY captured_at DESC, id DESC"
        ),
        &[&snapshot_key],
    )?;
    rows.into_iter()
        .map(|row| {
            let captured_at = row.get::<_, DateTime<Utc>>(0);
            let preview_json = row.get::<_, String>(1);
            let preview = serde_json::from_str::<FileOperatorStorePreview>(&preview_json)?;
            Ok(PostgresOperatorStateRow {
                captured_at,
                preview,
            })
        })
        .collect()
}

pub(crate) fn persist_operator_state_snapshot(
    backend: Option<&OperatorStateBackendConfig>,
    metrics_retention: MetricsRetentionBudget,
    preview: &FileOperatorStorePreview,
) -> anyhow::Result<()> {
    let Some(backend) = backend else {
        return Ok(());
    };

    match backend {
        OperatorStateBackendConfig::Redis { url, snapshot_key } => {
            let mut connection = FileOperatorStore::redis_connection(url)?;
            let snapshot = OperatorStateSnapshot {
                captured_at: Utc::now(),
                preview: preview.clone(),
            };
            let payload = serde_json::to_string(&snapshot)?;
            connection.set::<_, _, ()>(snapshot_key, payload)?;
            Ok(())
        }
        OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        } => {
            let snapshot = OperatorStateSnapshot {
                captured_at: Utc::now(),
                preview: preview.clone(),
            };
            let mut client = postgres_connection(url)?;
            let table_name = ensure_valid_postgres_identifier(table_name)?;
            let audit_table_name = postgres_operator_audit_table_name(table_name);
            ensure_postgres_operator_state_table(&mut client, table_name)?;
            ensure_postgres_operator_audit_table(&mut client, &audit_table_name)?;
            let preview_json = serde_json::to_string(&snapshot.preview)?;
            let replay_summary =
                build_replay_snapshot_summary(&snapshot.preview, snapshot.captured_at);
            let summary_json = serde_json::to_string(&replay_summary.summary)?;
            let summary_text = replay_snapshot_summary_text(&replay_summary);
            let study_ids = replay_summary
                .study_ids
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>();
            let experiment_ids = replay_summary
                .experiment_ids
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>();
            let revision_ids = replay_summary
                .revision_ids
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>();
            let head_ids = replay_summary
                .head_ids
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>();
            client.execute(
                &format!(
                    "INSERT INTO {table_name} \
                     (snapshot_key, captured_at, preview_json, summary_json, summary_text, study_ids, experiment_ids, revision_ids, head_ids, receipt_count, head_count, merge_count, peer_window_metric_count, reducer_cohort_metric_count, head_eval_report_count) \
                     VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"
                ),
                &[
                    &snapshot_key,
                    &snapshot.captured_at,
                    &preview_json,
                    &summary_json,
                    &summary_text,
                    &study_ids,
                    &experiment_ids,
                    &revision_ids,
                    &head_ids,
                    &i64::try_from(replay_summary.receipt_count).unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.head_count).unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.merge_count).unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.peer_window_metric_count).unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.reducer_cohort_metric_count)
                        .unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.head_eval_report_count).unwrap_or(i64::MAX),
                ],
            )?;
            let audit_records =
                build_audit_records_from_preview(&snapshot.preview, Some(snapshot.captured_at));
            insert_postgres_operator_audit_records(
                &mut client,
                &audit_table_name,
                snapshot_key,
                &audit_records,
            )?;
            prune_postgres_snapshot_rows(
                &mut client,
                table_name,
                snapshot_key,
                operator_snapshot_retention_limit(metrics_retention),
            )?;
            prune_postgres_audit_rows(
                &mut client,
                &audit_table_name,
                snapshot_key,
                operator_audit_retention_limit(metrics_retention),
            )?;
            Ok(())
        }
    }
}

impl OperatorStore for FileOperatorStore {
    fn receipts(&self, query: &ReceiptQuery) -> anyhow::Result<Vec<ContributionReceipt>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(preview
                .receipts
                .into_iter()
                .filter(|receipt| query.matches(receipt))
                .collect());
        }

        Ok(self
            .storage_config()
            .map(|storage| history::load_all_contribution_receipts(&storage))
            .transpose()?
            .unwrap_or_else(|| self.receipt_preview.clone())
            .into_iter()
            .filter(|receipt| query.matches(receipt))
            .collect())
    }

    fn receipts_page(
        &self,
        query: &ReceiptQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<ContributionReceipt>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(page_from_filtered(preview.receipts, page, |receipt| {
                query.matches(receipt)
            }));
        }
        if let Some(storage) = self.storage_config() {
            return history::load_paged_contribution_receipts(&storage, page, |receipt| {
                query.matches(receipt)
            });
        }
        Ok(page_from_filtered(
            self.receipt_preview.iter().cloned(),
            page,
            |receipt| query.matches(receipt),
        ))
    }

    fn heads(&self, query: &HeadQuery) -> anyhow::Result<Vec<HeadDescriptor>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(preview
                .heads
                .into_iter()
                .filter(|head| query.matches(head))
                .collect());
        }

        Ok(self
            .storage_config()
            .map(|storage| history::load_all_head_descriptors(&storage))
            .transpose()?
            .unwrap_or_else(|| self.head_preview.clone())
            .into_iter()
            .filter(|head| query.matches(head))
            .collect())
    }

    fn heads_page(
        &self,
        query: &HeadQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<HeadDescriptor>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(page_from_filtered(preview.heads, page, |head| {
                query.matches(head)
            }));
        }
        if let Some(storage) = self.storage_config() {
            return history::load_paged_head_descriptors(&storage, page, |head| {
                query.matches(head)
            });
        }
        Ok(page_from_filtered(
            self.head_preview.iter().cloned(),
            page,
            |head| query.matches(head),
        ))
    }

    fn merges(&self) -> anyhow::Result<Vec<MergeCertificate>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(preview.merges);
        }

        self.storage_config()
            .map(|storage| history::load_all_merge_certificates(&storage))
            .transpose()
            .map(|loaded| loaded.unwrap_or_else(|| self.merge_preview.clone()))
    }

    fn merges_page(&self, page: PageRequest) -> anyhow::Result<Page<MergeCertificate>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(page_from_filtered(preview.merges, page, |_| true));
        }
        if let Some(storage) = self.storage_config() {
            return history::load_paged_merge_certificates(&storage, page, |_| true);
        }
        Ok(page_from_filtered(
            self.merge_preview.iter().cloned(),
            page,
            |_| true,
        ))
    }

    fn audit_page(
        &self,
        query: &OperatorAuditQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorAuditRecord>> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            return load_postgres_operator_audit_page(
                url,
                &postgres_operator_audit_table_name(table_name),
                snapshot_key,
                query,
                page,
            );
        }
        let records = self.build_audit_records()?;
        Ok(page_from_filtered(records, page, |record| {
            query.matches(record)
        }))
    }

    fn audit_summary(&self, query: &OperatorAuditQuery) -> anyhow::Result<OperatorAuditSummary> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            return load_postgres_operator_audit_summary(
                url,
                &postgres_operator_audit_table_name(table_name),
                snapshot_key,
                query,
            );
        }
        let records = self
            .build_audit_records()?
            .into_iter()
            .filter(|record| query.matches(record))
            .collect::<Vec<_>>();
        Ok(build_operator_audit_summary(
            operator_backend_label(self.operator_state_backend.as_ref()),
            records,
        ))
    }

    fn replay_snapshot(
        &self,
        captured_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<Option<OperatorReplaySnapshot>> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            let rows = load_postgres_operator_state_rows(url, table_name, snapshot_key)?;
            let selected = match captured_at {
                Some(at) => rows.into_iter().find(|row| row.captured_at <= at),
                None => rows.into_iter().next(),
            };
            return Ok(
                selected.map(|row| replay_snapshot_from_preview(row.captured_at, row.preview))
            );
        }

        if let Some(snapshot) = self.remote_snapshot()? {
            if captured_at.is_none_or(|at| snapshot.captured_at <= at) {
                return Ok(Some(replay_snapshot_from_preview(
                    snapshot.captured_at,
                    snapshot.preview,
                )));
            }
            return Ok(None);
        }

        Ok(Some(replay_snapshot_from_preview(
            Utc::now(),
            FileOperatorStorePreview {
                receipts: self.receipt_preview.clone(),
                heads: self.head_preview.clone(),
                merges: self.merge_preview.clone(),
                peer_window_metrics: self.peer_window_preview.clone(),
                reducer_cohort_metrics: self.reducer_cohort_preview.clone(),
                head_eval_reports: self.head_eval_preview.clone(),
                eval_protocol_manifests: self.eval_protocol_preview.clone(),
            },
        )))
    }

    fn replay_page(
        &self,
        query: &OperatorReplayQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorReplaySnapshotSummary>> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            return load_postgres_operator_replay_page(url, table_name, snapshot_key, query, page);
        }

        let page = page.normalized();
        let summaries = if let Some(snapshot) = self.remote_snapshot()? {
            vec![build_replay_snapshot_summary(
                &snapshot.preview,
                snapshot.captured_at,
            )]
        } else {
            vec![build_replay_snapshot_summary(
                &FileOperatorStorePreview {
                    receipts: self.receipt_preview.clone(),
                    heads: self.head_preview.clone(),
                    merges: self.merge_preview.clone(),
                    peer_window_metrics: self.peer_window_preview.clone(),
                    reducer_cohort_metrics: self.reducer_cohort_preview.clone(),
                    head_eval_reports: self.head_eval_preview.clone(),
                    eval_protocol_manifests: self.eval_protocol_preview.clone(),
                },
                Utc::now(),
            )]
        };
        Ok(page_from_filtered(summaries, page, |summary| {
            query.matches(summary)
        }))
    }

    fn retention_summary(&self) -> anyhow::Result<OperatorRetentionSummary> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            return load_postgres_operator_retention_summary(
                url,
                table_name,
                snapshot_key,
                self.metrics_retention,
            );
        }

        let latest_snapshot_at = self.remote_snapshot()?.map(|snapshot| snapshot.captured_at);
        Ok(OperatorRetentionSummary {
            backend: operator_backend_label(self.operator_state_backend.as_ref()),
            metrics_retention: self.metrics_retention,
            snapshot_retention_limit: operator_snapshot_retention_limit(self.metrics_retention),
            audit_retention_limit: operator_audit_retention_limit(self.metrics_retention),
            persisted_snapshot_count: usize::from(latest_snapshot_at.is_some()),
            persisted_audit_record_count: self.build_audit_records()?.len(),
            latest_snapshot_at,
        })
    }

    fn head_eval_reports(&self, head_id: &HeadId) -> anyhow::Result<Vec<HeadEvalReport>> {
        Ok(self
            .all_head_eval_reports()?
            .into_iter()
            .filter(|report| &report.head_id == head_id)
            .collect())
    }

    #[cfg(feature = "artifact-publish")]
    fn eval_protocol_manifests(&self) -> anyhow::Result<Vec<StoredEvalProtocolManifestRecord>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(preview.eval_protocol_manifests);
        }

        self.storage_config()
            .map(|storage| history::load_all_eval_protocol_manifests(&storage))
            .transpose()
            .map(|loaded| loaded.unwrap_or_else(|| self.eval_protocol_preview.clone()))
    }

    fn leaderboard_snapshot(
        &self,
        network_id: &NetworkId,
        peer_principals: &BTreeMap<PeerId, PrincipalId>,
        captured_at: DateTime<Utc>,
    ) -> anyhow::Result<BrowserLeaderboardSnapshot> {
        Ok(crate::social_services::leaderboard_snapshot(
            network_id,
            &self.receipts(&ReceiptQuery::default())?,
            &self.merges()?,
            peer_principals,
            captured_at,
        ))
    }

    #[cfg(feature = "artifact-publish")]
    fn artifact_store(&self) -> anyhow::Result<Option<FsArtifactStore>> {
        self.artifact_store_root
            .as_ref()
            .map(|root| {
                let store = FsArtifactStore::new(root.clone());
                store.ensure_layout().map_err(anyhow::Error::from)?;
                Ok(store)
            })
            .transpose()
    }

    #[cfg(feature = "artifact-publish")]
    fn publication_store(&self) -> anyhow::Result<Option<PublicationStore>> {
        use burn_p2p_core::EvalProtocolManifest;

        self.publication_store_root
            .as_ref()
            .map(|root| {
                let mut store = PublicationStore::open(root)?;
                store.configure_targets(self.publication_targets.clone())?;
                let heads = self.heads(&HeadQuery::default())?;
                let head_eval_reports = self.all_head_eval_reports()?;
                let protocols = self
                    .eval_protocol_manifests()?
                    .into_iter()
                    .map(|record| record.manifest)
                    .collect::<Vec<EvalProtocolManifest>>();
                if let Some(artifact_store) = self.artifact_store()? {
                    store.sync_aliases_and_eager_publications(
                        &artifact_store,
                        &heads,
                        &head_eval_reports,
                        &protocols,
                    )?;
                } else {
                    store.sync_aliases(&heads, &head_eval_reports, &protocols)?;
                }
                Ok(store)
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        net::TcpListener,
        process::{Child, Command, Stdio},
        thread,
        time::Duration as StdDuration,
    };

    use super::*;
    use crate::state::HeadQuery;
    use burn_p2p::{HeadDescriptor, MetricValue, PeerRole, PeerWindowStatus};
    use burn_p2p_core::{
        ArtifactId, ContentId, DatasetViewId, ExperimentId, HeadEvalStatus, MetricTrustClass,
        NetworkId, RevisionId, StudyId, WorkloadId,
    };
    use tempfile::{TempDir, tempdir};

    struct RedisTestServer {
        url: String,
        port: u16,
        child: Option<Child>,
        state: TempDir,
    }

    impl RedisTestServer {
        fn spawn() -> Option<Self> {
            let status = Command::new("redis-server")
                .arg("--version")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .ok()?;
            if !status.success() {
                return None;
            }

            let state = tempdir().expect("redis temp dir");
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind redis test port");
            let port = listener.local_addr().expect("redis local addr").port();
            drop(listener);

            let mut server = Self {
                url: format!("redis://127.0.0.1:{port}/0"),
                port,
                child: None,
                state,
            };
            server.start();
            Some(server)
        }

        fn start(&mut self) {
            assert!(self.child.is_none(), "redis test server already started");
            let child = Command::new("redis-server")
                .arg("--save")
                .arg("")
                .arg("--appendonly")
                .arg("no")
                .arg("--bind")
                .arg("127.0.0.1")
                .arg("--port")
                .arg(self.port.to_string())
                .arg("--dir")
                .arg(self.state.path())
                .arg("--dbfilename")
                .arg("dump.rdb")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn redis-server");

            let deadline = std::time::Instant::now() + StdDuration::from_secs(5);
            loop {
                if let Ok(client) = redis::Client::open(self.url.as_str())
                    && let Ok(mut connection) = client.get_connection()
                    && redis::cmd("PING")
                        .query::<String>(&mut connection)
                        .map(|pong| pong == "PONG")
                        .unwrap_or(false)
                {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "redis test server did not become ready at {}",
                    self.url
                );
                thread::sleep(StdDuration::from_millis(25));
            }
            self.child = Some(child);
        }

        fn stop(&mut self) {
            if let Some(mut child) = self.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    impl Drop for RedisTestServer {
        fn drop(&mut self) {
            self.stop();
        }
    }

    fn sample_operator_preview(now: DateTime<Utc>) -> FileOperatorStorePreview {
        let head = HeadDescriptor {
            head_id: burn_p2p_core::HeadId::new("head-1"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-1"),
            parent_head_id: Some(burn_p2p_core::HeadId::new("base")),
            global_step: 42,
            created_at: now,
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.125))]),
        };
        let receipt = ContributionReceipt {
            receipt_id: burn_p2p::ContributionReceiptId::new("receipt-1"),
            peer_id: PeerId::new("trainer-a"),
            study_id: head.study_id.clone(),
            experiment_id: head.experiment_id.clone(),
            revision_id: head.revision_id.clone(),
            base_head_id: burn_p2p_core::HeadId::new("base"),
            artifact_id: ArtifactId::new("artifact-input"),
            accepted_at: now - chrono::Duration::seconds(4),
            accepted_weight: 1.5,
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.2))]),
            merge_cert_id: None,
        };
        let merge = MergeCertificate {
            merge_cert_id: burn_p2p::MergeCertId::new("merge-1"),
            study_id: head.study_id.clone(),
            experiment_id: head.experiment_id.clone(),
            revision_id: head.revision_id.clone(),
            base_head_id: burn_p2p_core::HeadId::new("base"),
            merged_head_id: head.head_id.clone(),
            merged_artifact_id: head.artifact_id.clone(),
            policy: burn_p2p::MergePolicy::WeightedMean,
            issued_at: now - chrono::Duration::seconds(3),
            validator: PeerId::new("validator-a"),
            contribution_receipts: vec![receipt.receipt_id.clone()],
        };
        let peer_window_metrics = PeerWindowMetrics {
            network_id: NetworkId::new("mainnet"),
            experiment_id: head.experiment_id.clone(),
            revision_id: head.revision_id.clone(),
            workload_id: WorkloadId::new("workload"),
            dataset_view_id: DatasetViewId::new("validation"),
            peer_id: PeerId::new("trainer-a"),
            principal_id: None,
            lease_id: burn_p2p::LeaseId::new("lease-a"),
            base_head_id: burn_p2p_core::HeadId::new("base"),
            window_started_at: now - chrono::Duration::seconds(5),
            window_finished_at: now - chrono::Duration::seconds(1),
            attempted_tokens_or_samples: 128,
            accepted_tokens_or_samples: Some(120),
            local_train_loss_mean: Some(0.25),
            local_train_loss_last: Some(0.2),
            grad_or_delta_norm: Some(0.9),
            optimizer_step_count: 8,
            compute_time_ms: 1_200,
            data_fetch_time_ms: 200,
            publish_latency_ms: 80,
            head_lag_at_start: 0,
            head_lag_at_finish: 1,
            backend_class: burn_p2p_core::BackendClass::Ndarray,
            role: PeerRole::TrainerCpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        };
        let reducer_cohort_metrics = ReducerCohortMetrics {
            network_id: NetworkId::new("mainnet"),
            experiment_id: head.experiment_id.clone(),
            revision_id: head.revision_id.clone(),
            workload_id: WorkloadId::new("workload"),
            dataset_view_id: DatasetViewId::new("validation"),
            merge_window_id: ContentId::new("merge-window-a"),
            reducer_group_id: ContentId::new("reducers-a"),
            captured_at: now - chrono::Duration::milliseconds(500),
            base_head_id: burn_p2p_core::HeadId::new("base"),
            candidate_head_id: Some(head.head_id.clone()),
            received_updates: 2,
            accepted_updates: 2,
            rejected_updates: 0,
            sum_weight: 2.0,
            accepted_tokens_or_samples: 128,
            staleness_mean: 0.1,
            staleness_max: 0.5,
            window_close_delay_ms: 40,
            cohort_duration_ms: 500,
            aggregate_norm: 0.7,
            reducer_load: 0.25,
            ingress_bytes: 1024,
            egress_bytes: 512,
            replica_agreement: Some(1.0),
            late_arrival_count: Some(0),
            missing_peer_count: Some(0),
            rejection_reasons: BTreeMap::new(),
            status: burn_p2p_core::ReducerCohortStatus::Closed,
        };
        let report = HeadEvalReport {
            network_id: NetworkId::new("mainnet"),
            experiment_id: head.experiment_id.clone(),
            revision_id: head.revision_id.clone(),
            workload_id: WorkloadId::new("workload"),
            head_id: head.head_id.clone(),
            base_head_id: head.parent_head_id.clone(),
            eval_protocol_id: ContentId::new("protocol"),
            evaluator_set_id: ContentId::new("validators"),
            metric_values: BTreeMap::from([("accuracy".into(), MetricValue::Float(0.91))]),
            sample_count: 128,
            dataset_view_id: DatasetViewId::new("validation"),
            started_at: now - chrono::Duration::seconds(2),
            finished_at: now - chrono::Duration::seconds(1),
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        };
        FileOperatorStorePreview {
            receipts: vec![receipt],
            heads: vec![head],
            merges: vec![merge],
            peer_window_metrics: vec![peer_window_metrics],
            reducer_cohort_metrics: vec![reducer_cohort_metrics],
            head_eval_reports: vec![report],
            eval_protocol_manifests: Vec::new(),
        }
    }

    #[test]
    fn build_audit_records_from_preview_flattens_replayable_snapshot_rows() {
        let now = Utc::now();
        let preview = sample_operator_preview(now);
        let records = build_audit_records_from_preview(&preview, Some(now));

        assert_eq!(records.len(), 7);
        assert!(
            records
                .iter()
                .any(|record| record.kind == OperatorAuditKind::Receipt
                    && record.record_id == "receipt-1"
                    && record.summary.get("artifact_id") == Some(&"artifact-input".to_string()))
        );
        assert!(
            records
                .iter()
                .any(|record| record.kind == OperatorAuditKind::PeerWindowMetric
                    && record.summary.get("status") == Some(&"Completed".to_string()))
        );
        let snapshot = records
            .iter()
            .find(|record| record.kind == OperatorAuditKind::Snapshot)
            .expect("snapshot record");
        assert_eq!(snapshot.summary.get("heads"), Some(&"1".to_string()));
        assert_eq!(
            snapshot.summary.get("peer_window_metrics"),
            Some(&"1".to_string())
        );
    }

    #[test]
    fn file_operator_store_audit_page_filters_kind_text_and_time() {
        let now = Utc::now();
        let preview = sample_operator_preview(now);
        let store = FileOperatorStore::new(
            FileOperatorStoreConfig {
                history_root: None,
                metrics_store_root: None,
                metrics_retention: MetricsRetentionBudget::default(),
                publication_store_root: None,
                publication_targets: Vec::new(),
                artifact_store_root: None,
                operator_state_backend: None,
            },
            preview,
        );

        let page = store
            .audit_page(
                &OperatorAuditQuery {
                    kind: Some(OperatorAuditKind::PeerWindowMetric),
                    since: Some(now - chrono::Duration::seconds(2)),
                    until: Some(now),
                    text: Some("completed".into()),
                    ..OperatorAuditQuery::default()
                },
                PageRequest::default(),
            )
            .expect("audit page");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].kind, OperatorAuditKind::PeerWindowMetric);
        assert_eq!(
            page.items[0].summary.get("status"),
            Some(&"Completed".to_string())
        );
    }

    #[test]
    fn file_operator_store_audit_summary_counts_kinds_and_scopes() {
        let now = Utc::now();
        let preview = sample_operator_preview(now);
        let store = FileOperatorStore::new(
            FileOperatorStoreConfig {
                history_root: None,
                metrics_store_root: None,
                metrics_retention: MetricsRetentionBudget::default(),
                publication_store_root: None,
                publication_targets: Vec::new(),
                artifact_store_root: None,
                operator_state_backend: None,
            },
            preview,
        );

        let summary = store
            .audit_summary(&OperatorAuditQuery {
                since: Some(now - chrono::Duration::seconds(10)),
                until: Some(now),
                ..OperatorAuditQuery::default()
            })
            .expect("audit summary");

        assert_eq!(summary.backend, "file");
        assert_eq!(summary.record_count, 6);
        assert_eq!(summary.counts_by_kind.get("receipt"), Some(&1));
        assert_eq!(summary.counts_by_kind.get("snapshot"), None);
        assert_eq!(summary.distinct_study_count, 1);
        assert_eq!(summary.distinct_experiment_count, 1);
        assert_eq!(summary.distinct_revision_count, 1);
        assert_eq!(summary.distinct_head_count, 2);
        assert!(summary.earliest_captured_at.is_some());
        assert!(summary.latest_captured_at.is_some());
    }

    #[test]
    fn operator_retention_limits_scale_with_metrics_budget() {
        let default_budget = MetricsRetentionBudget::default();
        let expanded_budget = MetricsRetentionBudget {
            max_metric_revisions_per_experiment: default_budget.max_metric_revisions_per_experiment
                * 4,
            ..default_budget
        };

        assert!(
            operator_snapshot_retention_limit(expanded_budget)
                > operator_snapshot_retention_limit(default_budget)
        );
        assert!(
            operator_audit_retention_limit(expanded_budget)
                > operator_audit_retention_limit(default_budget)
        );
    }

    #[test]
    fn redis_operator_snapshot_shares_heads_metrics_and_head_eval_reports_across_edges() {
        let Some(redis) = RedisTestServer::spawn() else {
            eprintln!(
                "skipping redis-backed operator-store test because redis-server is unavailable"
            );
            return;
        };

        let now = Utc::now();
        let head = HeadDescriptor {
            head_id: burn_p2p_core::HeadId::new("head-1"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-1"),
            parent_head_id: Some(burn_p2p_core::HeadId::new("base")),
            global_step: 42,
            created_at: now,
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.125))]),
        };
        let report = HeadEvalReport {
            network_id: NetworkId::new("mainnet"),
            experiment_id: head.experiment_id.clone(),
            revision_id: head.revision_id.clone(),
            workload_id: WorkloadId::new("workload"),
            head_id: head.head_id.clone(),
            base_head_id: head.parent_head_id.clone(),
            eval_protocol_id: ContentId::new("protocol"),
            evaluator_set_id: ContentId::new("evalset"),
            metric_values: BTreeMap::from([("accuracy".into(), MetricValue::Float(0.91))]),
            sample_count: 128,
            dataset_view_id: DatasetViewId::new("validation"),
            started_at: now,
            finished_at: now,
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        };
        let peer_window_metrics = PeerWindowMetrics {
            network_id: NetworkId::new("mainnet"),
            experiment_id: head.experiment_id.clone(),
            revision_id: head.revision_id.clone(),
            workload_id: WorkloadId::new("workload"),
            dataset_view_id: DatasetViewId::new("validation"),
            peer_id: PeerId::new("trainer-a"),
            principal_id: None,
            lease_id: burn_p2p::LeaseId::new("lease-a"),
            base_head_id: burn_p2p_core::HeadId::new("base"),
            window_started_at: now - chrono::Duration::seconds(5),
            window_finished_at: now - chrono::Duration::seconds(1),
            attempted_tokens_or_samples: 128,
            accepted_tokens_or_samples: Some(128),
            local_train_loss_mean: Some(0.25),
            local_train_loss_last: Some(0.2),
            grad_or_delta_norm: Some(0.9),
            optimizer_step_count: 8,
            compute_time_ms: 1_200,
            data_fetch_time_ms: 200,
            publish_latency_ms: 80,
            head_lag_at_start: 0,
            head_lag_at_finish: 1,
            backend_class: burn_p2p_core::BackendClass::Ndarray,
            role: PeerRole::TrainerCpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        };
        let reducer_cohort_metrics = ReducerCohortMetrics {
            network_id: NetworkId::new("mainnet"),
            experiment_id: head.experiment_id.clone(),
            revision_id: head.revision_id.clone(),
            workload_id: WorkloadId::new("workload"),
            dataset_view_id: DatasetViewId::new("validation"),
            merge_window_id: ContentId::new("merge-window-a"),
            reducer_group_id: ContentId::new("reducers-a"),
            captured_at: now,
            base_head_id: burn_p2p_core::HeadId::new("base"),
            candidate_head_id: Some(head.head_id.clone()),
            received_updates: 2,
            accepted_updates: 2,
            rejected_updates: 0,
            sum_weight: 2.0,
            accepted_tokens_or_samples: 128,
            staleness_mean: 0.1,
            staleness_max: 0.5,
            window_close_delay_ms: 40,
            cohort_duration_ms: 500,
            aggregate_norm: 0.7,
            reducer_load: 0.25,
            ingress_bytes: 1024,
            egress_bytes: 512,
            replica_agreement: Some(1.0),
            late_arrival_count: Some(0),
            missing_peer_count: Some(0),
            rejection_reasons: BTreeMap::new(),
            status: burn_p2p_core::ReducerCohortStatus::Closed,
        };
        let preview = FileOperatorStorePreview {
            heads: vec![head.clone()],
            peer_window_metrics: vec![peer_window_metrics.clone()],
            reducer_cohort_metrics: vec![reducer_cohort_metrics.clone()],
            head_eval_reports: vec![report.clone()],
            ..FileOperatorStorePreview::default()
        };
        let backend = OperatorStateBackendConfig::Redis {
            url: redis.url.clone(),
            snapshot_key: "burn-p2p:test:operator-state:snapshot".into(),
        };

        persist_operator_state_snapshot(
            Some(&backend),
            MetricsRetentionBudget::default(),
            &preview,
        )
        .expect("persist redis-backed operator snapshot");

        let reader = FileOperatorStore::new(
            FileOperatorStoreConfig {
                history_root: None,
                metrics_store_root: None,
                metrics_retention: MetricsRetentionBudget::default(),
                publication_store_root: None,
                publication_targets: Vec::new(),
                artifact_store_root: None,
                operator_state_backend: Some(backend),
            },
            FileOperatorStorePreview::default(),
        );

        assert_eq!(
            reader
                .heads(&HeadQuery {
                    head_id: Some(head.head_id.clone()),
                    ..HeadQuery::default()
                })
                .expect("load heads from redis snapshot"),
            vec![head.clone()]
        );
        assert_eq!(
            reader
                .head_eval_reports(&head.head_id)
                .expect("load head eval reports from redis snapshot"),
            vec![report]
        );
        assert_eq!(
            reader
                .all_peer_window_metrics()
                .expect("load peer window metrics from redis snapshot"),
            vec![peer_window_metrics]
        );
        assert_eq!(
            reader
                .all_reducer_cohort_metrics()
                .expect("load reducer cohort metrics from redis snapshot"),
            vec![reducer_cohort_metrics]
        );
        let replay = reader
            .replay_snapshot(None)
            .expect("load replay snapshot from redis snapshot")
            .expect("replay snapshot");
        assert_eq!(replay.heads, vec![head]);
    }

    #[cfg(feature = "metrics-indexer")]
    #[test]
    fn redis_operator_snapshot_drives_metrics_exports_across_edges() {
        let Some(redis) = RedisTestServer::spawn() else {
            eprintln!(
                "skipping redis-backed metrics export test because redis-server is unavailable"
            );
            return;
        };

        let now = Utc::now();
        let backend = OperatorStateBackendConfig::Redis {
            url: redis.url.clone(),
            snapshot_key: "burn-p2p:test:operator-state:metrics-snapshot".into(),
        };
        let peer_window_metrics = PeerWindowMetrics {
            network_id: NetworkId::new("mainnet"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            workload_id: WorkloadId::new("workload"),
            dataset_view_id: DatasetViewId::new("validation"),
            peer_id: PeerId::new("trainer-a"),
            principal_id: None,
            lease_id: burn_p2p::LeaseId::new("lease-a"),
            base_head_id: burn_p2p_core::HeadId::new("base"),
            window_started_at: now - chrono::Duration::seconds(5),
            window_finished_at: now - chrono::Duration::seconds(1),
            attempted_tokens_or_samples: 128,
            accepted_tokens_or_samples: Some(128),
            local_train_loss_mean: Some(0.25),
            local_train_loss_last: Some(0.2),
            grad_or_delta_norm: Some(0.9),
            optimizer_step_count: 8,
            compute_time_ms: 1_200,
            data_fetch_time_ms: 200,
            publish_latency_ms: 80,
            head_lag_at_start: 0,
            head_lag_at_finish: 1,
            backend_class: burn_p2p_core::BackendClass::Ndarray,
            role: PeerRole::TrainerCpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        };
        let reducer_cohort_metrics = ReducerCohortMetrics {
            network_id: NetworkId::new("mainnet"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            workload_id: WorkloadId::new("workload"),
            dataset_view_id: DatasetViewId::new("validation"),
            merge_window_id: ContentId::new("merge-window-a"),
            reducer_group_id: ContentId::new("reducers-a"),
            captured_at: now,
            base_head_id: burn_p2p_core::HeadId::new("base"),
            candidate_head_id: Some(burn_p2p_core::HeadId::new("candidate")),
            received_updates: 2,
            accepted_updates: 2,
            rejected_updates: 0,
            sum_weight: 2.0,
            accepted_tokens_or_samples: 128,
            staleness_mean: 0.1,
            staleness_max: 0.5,
            window_close_delay_ms: 40,
            cohort_duration_ms: 500,
            aggregate_norm: 0.7,
            reducer_load: 0.25,
            ingress_bytes: 1024,
            egress_bytes: 512,
            replica_agreement: Some(1.0),
            late_arrival_count: Some(0),
            missing_peer_count: Some(0),
            rejection_reasons: BTreeMap::new(),
            status: burn_p2p_core::ReducerCohortStatus::Closed,
        };
        let report = HeadEvalReport {
            network_id: NetworkId::new("mainnet"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            workload_id: WorkloadId::new("workload"),
            head_id: burn_p2p_core::HeadId::new("candidate"),
            base_head_id: Some(burn_p2p_core::HeadId::new("base")),
            eval_protocol_id: ContentId::new("protocol"),
            evaluator_set_id: ContentId::new("validators"),
            metric_values: BTreeMap::from([("accuracy".into(), MetricValue::Float(0.91))]),
            sample_count: 128,
            dataset_view_id: DatasetViewId::new("validation"),
            started_at: now - chrono::Duration::seconds(2),
            finished_at: now - chrono::Duration::seconds(1),
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        };

        persist_operator_state_snapshot(
            Some(&backend),
            MetricsRetentionBudget::default(),
            &FileOperatorStorePreview {
                peer_window_metrics: vec![peer_window_metrics.clone()],
                reducer_cohort_metrics: vec![reducer_cohort_metrics.clone()],
                head_eval_reports: vec![report],
                ..FileOperatorStorePreview::default()
            },
        )
        .expect("persist redis-backed operator metrics snapshot");

        let state = crate::BootstrapAdminState {
            operator_state_backend: Some(backend),
            metrics_retention: MetricsRetentionBudget::default(),
            ..crate::BootstrapAdminState::default()
        };

        let snapshots = state
            .export_metrics_snapshots()
            .expect("export metrics snapshots from redis-backed state");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].peer_window_metrics, vec![peer_window_metrics]);
        assert_eq!(
            snapshots[0].reducer_cohort_metrics,
            vec![reducer_cohort_metrics]
        );
    }
}
