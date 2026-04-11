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
    PageRequest, PeerId, PrincipalId, PublicationTarget,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p_publish::PublicationStore;
use chrono::{DateTime, Utc};
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
pub(crate) struct FileOperatorStorePreview {
    pub(crate) receipts: Vec<ContributionReceipt>,
    pub(crate) heads: Vec<HeadDescriptor>,
    pub(crate) merges: Vec<MergeCertificate>,
    pub(crate) head_eval_reports: Vec<HeadEvalReport>,
    pub(crate) eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperatorStateBackendConfig {
    Redis { url: String, snapshot_key: String },
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
            let payload = serde_json::to_string(&OperatorStateSnapshot {
                captured_at: Utc::now(),
                preview: preview.clone(),
            })?;
            connection.set::<_, _, ()>(snapshot_key, payload)?;
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
    use burn_p2p::{HeadDescriptor, MetricValue};
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
    fn redis_operator_snapshot_shares_heads_and_head_eval_reports_across_edges() {
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
        let preview = FileOperatorStorePreview {
            heads: vec![head.clone()],
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
    }
}
