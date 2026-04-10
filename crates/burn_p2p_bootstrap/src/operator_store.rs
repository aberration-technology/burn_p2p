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
}

#[derive(Clone, Debug, Default)]
pub(crate) struct FileOperatorStorePreview {
    pub(crate) receipts: Vec<ContributionReceipt>,
    pub(crate) heads: Vec<HeadDescriptor>,
    pub(crate) merges: Vec<MergeCertificate>,
    pub(crate) head_eval_reports: Vec<HeadEvalReport>,
    pub(crate) eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
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

    pub(crate) fn all_head_eval_reports(&self) -> anyhow::Result<Vec<HeadEvalReport>> {
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

impl OperatorStore for FileOperatorStore {
    fn receipts(&self, query: &ReceiptQuery) -> anyhow::Result<Vec<ContributionReceipt>> {
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
        self.storage_config()
            .map(|storage| history::load_all_merge_certificates(&storage))
            .transpose()
            .map(|loaded| loaded.unwrap_or_else(|| self.merge_preview.clone()))
    }

    fn merges_page(&self, page: PageRequest) -> anyhow::Result<Page<MergeCertificate>> {
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
