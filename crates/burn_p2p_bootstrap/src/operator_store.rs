use std::{collections::BTreeMap, path::PathBuf};

#[cfg(feature = "artifact-publish")]
use crate::state::{HeadQuery, ReceiptQuery};
#[cfg(not(feature = "artifact-publish"))]
use crate::state::{HeadQuery, ReceiptQuery};
#[cfg(feature = "artifact-publish")]
use burn_p2p::FsArtifactStore;
use burn_p2p::{ContributionReceipt, HeadDescriptor, MetricsRetentionBudget, StorageConfig};
use burn_p2p_core::{
    BrowserLeaderboardSnapshot, HeadEvalReport, HeadId, MergeCertificate, NetworkId, Page,
    PageRequest, PeerId, PeerWindowMetrics, PrincipalId, PublicationTarget, ReducerCohortMetrics,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p_publish::PublicationStore;
use chrono::{DateTime, Utc};
use postgres::NoTls;
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
    preview_json JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS {table_name}_snapshot_lookup
    ON {table_name} (snapshot_key, captured_at DESC, id DESC);
"#
    ))?;
    Ok(())
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

pub(crate) fn persist_operator_state_snapshot(
    backend: Option<&OperatorStateBackendConfig>,
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
            ensure_postgres_operator_state_table(&mut client, table_name)?;
            let preview_json = serde_json::to_string(&snapshot.preview)?;
            client.execute(
                &format!(
                    "INSERT INTO {table_name} (snapshot_key, captured_at, preview_json) VALUES ($1, $2, $3::jsonb)"
                ),
                &[&snapshot_key, &snapshot.captured_at, &preview_json],
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
            return Ok(page_from_filtered(
                preview.receipts.into_iter(),
                page,
                |receipt| query.matches(receipt),
            ));
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
            return Ok(page_from_filtered(
                preview.heads.into_iter(),
                page,
                |head| query.matches(head),
            ));
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
            return Ok(page_from_filtered(preview.merges.into_iter(), page, |_| {
                true
            }));
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

        persist_operator_state_snapshot(Some(&backend), &preview)
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
