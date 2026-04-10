use std::collections::BTreeSet;

use burn_p2p::{
    ArtifactId, ContributionReceipt, ContributionReceiptId, ExperimentId, HeadId, MicroShardId,
    PeerId, RevisionId, StudyId,
};
use burn_p2p_core::{MetricsLiveEvent, SchemaEnvelope, SignedPayload};
use burn_p2p_metrics::MetricsCatchupBundle;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{BrowserDirectorySnapshot, BrowserLeaderboardSnapshot, BrowserSessionState};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser stored assignment.
pub struct BrowserStoredAssignment {
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Declares how the browser receipt outbox is persisted.
pub enum BrowserReceiptOutboxBackend {
    /// Uses the snapshot-backed variant.
    #[default]
    Snapshot,
    /// Uses the local-storage-backed durable variant.
    LocalStorage,
    /// Uses the indexeddb-backed durable variant.
    IndexedDb,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
/// Holds the browser receipt submission outbox.
pub struct BrowserReceiptOutbox {
    /// The configured persistence backend.
    pub backend: BrowserReceiptOutboxBackend,
    receipts: Vec<ContributionReceipt>,
}

impl Default for BrowserReceiptOutbox {
    fn default() -> Self {
        Self {
            backend: BrowserReceiptOutboxBackend::Snapshot,
            receipts: Vec::new(),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BrowserReceiptOutboxWire {
    Legacy(Vec<ContributionReceipt>),
    Structured {
        #[serde(default)]
        backend: BrowserReceiptOutboxBackend,
        #[serde(default)]
        receipts: Vec<ContributionReceipt>,
    },
}

impl<'de> Deserialize<'de> for BrowserReceiptOutbox {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match BrowserReceiptOutboxWire::deserialize(deserializer)? {
            BrowserReceiptOutboxWire::Legacy(receipts) => Ok(Self {
                backend: BrowserReceiptOutboxBackend::Snapshot,
                receipts,
            }),
            BrowserReceiptOutboxWire::Structured { backend, receipts } => {
                Ok(Self { backend, receipts })
            }
        }
    }
}

impl BrowserReceiptOutbox {
    /// Configures the preferred outbox backend.
    pub fn configure_backend(&mut self, backend: BrowserReceiptOutboxBackend) {
        self.backend = backend;
    }

    /// Returns whether the backend is durable across browser reloads.
    pub fn is_durable(&self) -> bool {
        matches!(
            self.backend,
            BrowserReceiptOutboxBackend::LocalStorage | BrowserReceiptOutboxBackend::IndexedDb
        )
    }

    /// Returns the number of receipts currently queued.
    pub fn len(&self) -> usize {
        self.receipts.len()
    }

    /// Returns whether the outbox is empty.
    pub fn is_empty(&self) -> bool {
        self.receipts.is_empty()
    }

    /// Returns an iterator over queued receipts.
    pub fn iter(&self) -> impl Iterator<Item = &ContributionReceipt> {
        self.receipts.iter()
    }

    /// Queues one new receipt.
    pub fn enqueue(&mut self, receipt: ContributionReceipt) {
        self.receipts.push(receipt);
    }

    /// Returns whether the outbox already contains the given receipt.
    pub fn contains(&self, receipt_id: &ContributionReceiptId) -> bool {
        self.receipts
            .iter()
            .any(|pending| &pending.receipt_id == receipt_id)
    }

    /// Returns one bounded submission batch.
    pub fn submission_batch(&self, limit: usize) -> Vec<ContributionReceipt> {
        self.receipts.iter().take(limit).cloned().collect()
    }

    /// Acknowledges receipt IDs and returns the remaining queue length.
    pub fn acknowledge(&mut self, receipt_ids: &BTreeSet<ContributionReceiptId>) -> usize {
        if receipt_ids.is_empty() {
            return self.receipts.len();
        }
        self.receipts
            .retain(|receipt| !receipt_ids.contains(&receipt.receipt_id));
        self.receipts.len()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Captures a snapshot of browser storage.
pub struct BrowserStorageSnapshot {
    /// The metadata version.
    pub metadata_version: u16,
    /// The session.
    pub session: BrowserSessionState,
    /// The cached chunk artifacts.
    pub cached_chunk_artifacts: BTreeSet<ArtifactId>,
    /// Heads whose active artifact has already been synced into the browser cache.
    #[serde(default)]
    pub cached_head_artifact_heads: BTreeSet<HeadId>,
    #[serde(default)]
    /// Transport that most recently delivered the active head artifact into the browser cache.
    pub last_head_artifact_transport: Option<String>,
    /// The cached microshards.
    pub cached_microshards: BTreeSet<MicroShardId>,
    /// The stored receipts.
    pub stored_receipts: BTreeSet<ContributionReceiptId>,
    /// The pending receipt outbox.
    pub pending_receipts: BrowserReceiptOutbox,
    /// The submitted receipts.
    pub submitted_receipts: BTreeSet<ContributionReceiptId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    stored_receipt_order: Vec<ContributionReceiptId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    submitted_receipt_order: Vec<ContributionReceiptId>,
    /// The last directory sync at.
    pub last_directory_sync_at: Option<DateTime<Utc>>,
    /// The last signed directory snapshot.
    pub last_signed_directory_snapshot:
        Option<SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>>>,
    /// The last signed leaderboard snapshot.
    pub last_signed_leaderboard_snapshot:
        Option<SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>>>,
    /// Cached metrics catchup bundles keyed by experiment revision.
    #[serde(default)]
    pub metrics_catchup_bundles: Vec<MetricsCatchupBundle>,
    /// The latest live metrics event fetched by the browser client.
    pub last_metrics_live_event: Option<MetricsLiveEvent>,
    /// The last metrics sync timestamp.
    pub last_metrics_sync_at: Option<DateTime<Utc>>,
    /// The last head ID.
    pub last_head_id: Option<HeadId>,
    /// The stored certificate peer ID.
    pub stored_certificate_peer_id: Option<PeerId>,
    /// The active assignment.
    pub active_assignment: Option<BrowserStoredAssignment>,
    /// The updated at.
    pub updated_at: DateTime<Utc>,
}

impl Default for BrowserStorageSnapshot {
    fn default() -> Self {
        Self {
            metadata_version: 1,
            session: BrowserSessionState::default(),
            cached_chunk_artifacts: BTreeSet::new(),
            cached_head_artifact_heads: BTreeSet::new(),
            last_head_artifact_transport: None,
            cached_microshards: BTreeSet::new(),
            stored_receipts: BTreeSet::new(),
            pending_receipts: BrowserReceiptOutbox::default(),
            submitted_receipts: BTreeSet::new(),
            stored_receipt_order: Vec::new(),
            submitted_receipt_order: Vec::new(),
            last_directory_sync_at: None,
            last_signed_directory_snapshot: None,
            last_signed_leaderboard_snapshot: None,
            metrics_catchup_bundles: Vec::new(),
            last_metrics_live_event: None,
            last_metrics_sync_at: None,
            last_head_id: None,
            stored_certificate_peer_id: None,
            active_assignment: None,
            updated_at: Utc::now(),
        }
    }
}

impl BrowserStorageSnapshot {
    const MAX_METRICS_CATCHUP_BUNDLES: usize = 8;
    const MAX_STORED_RECEIPT_IDS: usize = 512;
    const MAX_SUBMITTED_RECEIPT_IDS: usize = 512;

    /// Performs the remember session operation.
    pub fn remember_session(&mut self, session: BrowserSessionState) {
        self.stored_certificate_peer_id = session.peer_id().cloned();
        self.session = session;
        self.updated_at = Utc::now();
    }

    /// Performs the remember directory snapshot operation.
    pub fn remember_directory_snapshot(
        &mut self,
        snapshot: SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>>,
    ) {
        self.last_directory_sync_at = Some(Utc::now());
        self.last_signed_directory_snapshot = Some(snapshot);
        self.updated_at = Utc::now();
    }

    /// Performs the remember leaderboard snapshot operation.
    pub fn remember_leaderboard_snapshot(
        &mut self,
        snapshot: SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>>,
    ) {
        self.last_signed_leaderboard_snapshot = Some(snapshot);
        self.updated_at = Utc::now();
    }

    /// Performs the remember metrics catchup operation.
    pub fn remember_metrics_catchup(&mut self, bundles: Vec<MetricsCatchupBundle>) {
        let mut bundles_by_revision =
            std::collections::BTreeMap::<(ExperimentId, RevisionId), MetricsCatchupBundle>::new();
        for bundle in bundles {
            let key = (bundle.experiment_id.clone(), bundle.revision_id.clone());
            match bundles_by_revision.get(&key) {
                Some(existing) if existing.generated_at >= bundle.generated_at => {}
                _ => {
                    bundles_by_revision.insert(key, bundle);
                }
            }
        }

        let mut retained = bundles_by_revision.into_values().collect::<Vec<_>>();
        retained.sort_by(|left, right| {
            right
                .generated_at
                .cmp(&left.generated_at)
                .then_with(|| right.experiment_id.cmp(&left.experiment_id))
                .then_with(|| right.revision_id.cmp(&left.revision_id))
        });
        retained.truncate(Self::MAX_METRICS_CATCHUP_BUNDLES);
        retained.sort_by(|left, right| {
            left.experiment_id
                .cmp(&right.experiment_id)
                .then_with(|| left.revision_id.cmp(&right.revision_id))
        });

        self.metrics_catchup_bundles = retained;
        self.last_metrics_sync_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    /// Performs the remember metrics live event operation.
    pub fn remember_metrics_live_event(&mut self, event: MetricsLiveEvent) {
        self.last_metrics_live_event = Some(event);
        self.last_metrics_sync_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    /// Performs the remember assignment operation.
    pub fn remember_assignment(&mut self, assignment: BrowserStoredAssignment) {
        self.active_assignment = Some(assignment);
        self.updated_at = Utc::now();
    }

    /// Performs the clear assignment operation.
    pub fn clear_assignment(&mut self) {
        self.active_assignment = None;
        self.updated_at = Utc::now();
    }

    /// Performs the remember head operation.
    pub fn remember_head(&mut self, head_id: HeadId) {
        self.last_head_id = Some(head_id);
        self.updated_at = Utc::now();
    }

    /// Performs the remember chunk artifact operation.
    pub fn remember_chunk_artifact(&mut self, artifact_id: ArtifactId) {
        self.cached_chunk_artifacts.insert(artifact_id);
        self.updated_at = Utc::now();
    }

    /// Performs the remember synced head artifact operation.
    pub fn remember_synced_head_artifact(
        &mut self,
        head_id: HeadId,
        artifact_id: ArtifactId,
        transport: impl Into<String>,
    ) {
        self.cached_head_artifact_heads.insert(head_id);
        self.cached_chunk_artifacts.insert(artifact_id);
        self.last_head_artifact_transport = Some(transport.into());
        self.updated_at = Utc::now();
    }

    /// Performs the remember microshard operation.
    pub fn remember_microshard(&mut self, microshard_id: MicroShardId) {
        self.cached_microshards.insert(microshard_id);
        self.updated_at = Utc::now();
    }

    /// Performs the remember receipt operation.
    pub fn remember_receipt(&mut self, receipt_id: ContributionReceiptId) {
        remember_recent_receipt_id(
            &mut self.stored_receipts,
            &mut self.stored_receipt_order,
            receipt_id,
            Self::MAX_STORED_RECEIPT_IDS,
        );
        self.updated_at = Utc::now();
    }

    /// Performs the queue receipt operation.
    pub fn queue_receipt(&mut self, receipt: ContributionReceipt) {
        clamp_recent_receipt_ids(
            &mut self.stored_receipts,
            &mut self.stored_receipt_order,
            Self::MAX_STORED_RECEIPT_IDS,
        );
        clamp_recent_receipt_ids(
            &mut self.submitted_receipts,
            &mut self.submitted_receipt_order,
            Self::MAX_SUBMITTED_RECEIPT_IDS,
        );
        if self.submitted_receipts.contains(&receipt.receipt_id)
            || self.pending_receipts.contains(&receipt.receipt_id)
        {
            return;
        }
        remember_recent_receipt_id(
            &mut self.stored_receipts,
            &mut self.stored_receipt_order,
            receipt.receipt_id.clone(),
            Self::MAX_STORED_RECEIPT_IDS,
        );
        self.pending_receipts.enqueue(receipt);
        self.updated_at = Utc::now();
    }

    /// Performs the acknowledge receipts operation.
    pub fn acknowledge_receipts(&mut self, receipt_ids: &[ContributionReceiptId]) -> usize {
        let acknowledged = receipt_ids.iter().cloned().collect::<BTreeSet<_>>();
        if acknowledged.is_empty() {
            return self.pending_receipts.len();
        }
        let pending_receipts = self.pending_receipts.acknowledge(&acknowledged);
        for receipt_id in acknowledged {
            remember_recent_receipt_id(
                &mut self.submitted_receipts,
                &mut self.submitted_receipt_order,
                receipt_id,
                Self::MAX_SUBMITTED_RECEIPT_IDS,
            );
        }
        self.updated_at = Utc::now();
        pending_receipts
    }

    /// Returns one bounded receipt submission batch.
    pub fn receipt_submission_batch(&self, limit: usize) -> Vec<ContributionReceipt> {
        self.pending_receipts.submission_batch(limit)
    }

    /// Performs the clear cached data operation.
    pub fn clear_cached_data(&mut self) {
        self.cached_chunk_artifacts.clear();
        self.cached_head_artifact_heads.clear();
        self.last_head_artifact_transport = None;
        self.cached_microshards.clear();
        self.updated_at = Utc::now();
    }
}

fn remember_recent_receipt_id(
    ids: &mut BTreeSet<ContributionReceiptId>,
    order: &mut Vec<ContributionReceiptId>,
    receipt_id: ContributionReceiptId,
    max_entries: usize,
) {
    ids.insert(receipt_id.clone());
    if let Some(position) = order.iter().position(|candidate| candidate == &receipt_id) {
        order.remove(position);
    }
    order.push(receipt_id);
    clamp_recent_receipt_ids(ids, order, max_entries);
}

fn clamp_recent_receipt_ids(
    ids: &mut BTreeSet<ContributionReceiptId>,
    order: &mut Vec<ContributionReceiptId>,
    max_entries: usize,
) {
    if ids.len() <= max_entries && order.len() <= max_entries {
        return;
    }

    let mut retained = Vec::<ContributionReceiptId>::new();
    let mut seen = BTreeSet::<ContributionReceiptId>::new();
    for receipt_id in order.iter().rev() {
        if ids.contains(receipt_id) && seen.insert(receipt_id.clone()) {
            retained.push(receipt_id.clone());
            if retained.len() == max_entries {
                break;
            }
        }
    }
    if retained.len() < max_entries {
        for receipt_id in ids.iter().rev() {
            if seen.insert(receipt_id.clone()) {
                retained.push(receipt_id.clone());
                if retained.len() == max_entries {
                    break;
                }
            }
        }
    }
    retained.reverse();
    *ids = retained.iter().cloned().collect();
    *order = retained;
}
