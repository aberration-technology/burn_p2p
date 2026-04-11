use std::collections::BTreeSet;

use burn_p2p::{
    ArtifactDescriptor, ArtifactId, ChunkId, ContributionReceipt, ContributionReceiptId,
    ExperimentId, HeadId, MicroShardId, PeerId, RevisionId, StudyId,
};
use burn_p2p_core::{
    ArtifactProfile, MetricsLiveEvent, PublicationTargetId, RunId, SchemaEnvelope, SignedPayload,
};
use burn_p2p_metrics::MetricsCatchupBundle;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{BrowserDirectorySnapshot, BrowserLeaderboardSnapshot, BrowserSessionState};

mod base64_bytes {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        STANDARD
            .decode(encoded.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

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
/// Declares how one replay chunk payload is durably stored.
pub enum BrowserArtifactReplayChunkStorage {
    /// The chunk bytes are stored inline in the browser storage snapshot.
    #[default]
    Inline,
    /// The chunk bytes are stored in the indexeddb replay chunk store.
    IndexedDb,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One persisted chunk used to resume browser artifact replay across reloads.
pub struct BrowserArtifactReplayChunk {
    /// Persisted chunk identifier.
    pub chunk_id: ChunkId,
    /// Durable storage backend used for the chunk payload.
    #[serde(default)]
    pub storage: BrowserArtifactReplayChunkStorage,
    /// Durable chunk length retained across reloads.
    #[serde(default)]
    pub persisted_bytes: u64,
    /// Persisted verified chunk payload.
    #[serde(default, with = "base64_bytes", skip_serializing_if = "Vec::is_empty")]
    pub chunk_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// One durably persisted contiguous byte prefix for range-based artifact replay.
pub struct BrowserArtifactReplayBytePrefix {
    /// Durable storage backend used for the prefix payload.
    #[serde(default)]
    pub storage: BrowserArtifactReplayChunkStorage,
    /// Number of verified bytes retained across reloads.
    #[serde(default)]
    pub persisted_bytes: u64,
    /// Full artifact length when the edge download path exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    /// Persisted contiguous prefix bytes already downloaded from the edge.
    #[serde(default, with = "base64_bytes", skip_serializing_if = "Vec::is_empty")]
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Durable checkpoint describing an in-progress or failed active-head artifact replay.
pub struct BrowserArtifactReplayCheckpoint {
    /// Experiment covered by the checkpoint.
    pub experiment_id: ExperimentId,
    /// Revision covered by the checkpoint.
    pub revision_id: RevisionId,
    /// Run covered by the checkpoint.
    pub run_id: RunId,
    /// Head whose artifact is being replayed.
    pub head_id: HeadId,
    /// Artifact being replayed into the browser cache.
    pub artifact_id: ArtifactId,
    /// Publication profile used for the replay attempt.
    pub artifact_profile: ArtifactProfile,
    /// Publication target used for the replay attempt.
    pub publication_target_id: PublicationTargetId,
    /// Provider peers last attempted for the replay.
    #[serde(default)]
    pub provider_peer_ids: Vec<PeerId>,
    /// Persisted descriptor, once the browser has resolved it from the swarm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_descriptor: Option<ArtifactDescriptor>,
    /// Persisted verified chunks already downloaded for the artifact.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completed_chunks: Vec<BrowserArtifactReplayChunk>,
    /// Persisted contiguous byte prefix already downloaded through the edge range path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_download_prefix: Option<BrowserArtifactReplayBytePrefix>,
    /// Aggregate completed replay bytes retained in the checkpoint.
    #[serde(default)]
    pub completed_bytes: u64,
    /// Last replay attempt timestamp.
    pub last_attempted_at: DateTime<Utc>,
    /// Number of replay attempts retained for the checkpoint.
    pub attempt_count: u32,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Durable checkpoint for resuming active-head artifact replay after offline failure.
    pub artifact_replay_checkpoint: Option<BrowserArtifactReplayCheckpoint>,
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
            metadata_version: 3,
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
            artifact_replay_checkpoint: None,
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

    /// Performs the remember artifact replay checkpoint operation.
    pub fn remember_artifact_replay_checkpoint(
        &mut self,
        checkpoint: BrowserArtifactReplayCheckpoint,
    ) {
        self.artifact_replay_checkpoint = Some(checkpoint);
        self.updated_at = Utc::now();
    }

    /// Persists the resolved descriptor for the active replay checkpoint.
    pub fn remember_artifact_replay_descriptor(&mut self, descriptor: ArtifactDescriptor) {
        if let Some(checkpoint) = self.artifact_replay_checkpoint.as_mut() {
            checkpoint.completed_chunks.retain(|chunk| {
                descriptor
                    .chunks
                    .iter()
                    .any(|descriptor_chunk| descriptor_chunk.chunk_id == chunk.chunk_id)
            });
            checkpoint.completed_bytes = checkpoint
                .completed_chunks
                .iter()
                .map(browser_artifact_replay_chunk_len)
                .sum();
            checkpoint.edge_download_prefix = None;
            checkpoint.artifact_descriptor = Some(descriptor);
            checkpoint.last_attempted_at = Utc::now();
            self.updated_at = Utc::now();
        }
    }

    /// Persists one verified chunk for the active replay checkpoint.
    pub fn remember_artifact_replay_chunk(&mut self, chunk_id: ChunkId, chunk_bytes: Vec<u8>) {
        if let Some(checkpoint) = self.artifact_replay_checkpoint.as_mut() {
            if let Some(existing) = checkpoint
                .completed_chunks
                .iter_mut()
                .find(|chunk| chunk.chunk_id == chunk_id)
            {
                existing.storage = BrowserArtifactReplayChunkStorage::Inline;
                existing.persisted_bytes = chunk_bytes.len() as u64;
                existing.chunk_bytes = chunk_bytes;
            } else {
                checkpoint
                    .completed_chunks
                    .push(BrowserArtifactReplayChunk {
                        chunk_id,
                        storage: BrowserArtifactReplayChunkStorage::Inline,
                        persisted_bytes: chunk_bytes.len() as u64,
                        chunk_bytes,
                    });
            }
            checkpoint.completed_bytes = checkpoint
                .completed_chunks
                .iter()
                .map(browser_artifact_replay_chunk_len)
                .sum();
            checkpoint.last_attempted_at = Utc::now();
            self.updated_at = Utc::now();
        }
    }

    /// Persists the contiguous range-downloaded edge prefix for the active replay checkpoint.
    pub fn remember_artifact_replay_edge_prefix(
        &mut self,
        total_bytes: Option<u64>,
        bytes: Vec<u8>,
    ) {
        if let Some(checkpoint) = self.artifact_replay_checkpoint.as_mut() {
            checkpoint.edge_download_prefix = Some(BrowserArtifactReplayBytePrefix {
                storage: BrowserArtifactReplayChunkStorage::Inline,
                persisted_bytes: bytes.len() as u64,
                total_bytes,
                bytes,
            });
            checkpoint.completed_bytes = checkpoint
                .artifact_descriptor
                .as_ref()
                .map(|_| {
                    checkpoint
                        .completed_chunks
                        .iter()
                        .map(browser_artifact_replay_chunk_len)
                        .sum()
                })
                .unwrap_or_else(|| {
                    checkpoint
                        .edge_download_prefix
                        .as_ref()
                        .map(browser_artifact_replay_prefix_len)
                        .unwrap_or(0)
                });
            checkpoint.last_attempted_at = Utc::now();
            self.updated_at = Utc::now();
        }
    }

    /// Returns the stored replay checkpoint descriptor when present.
    pub fn artifact_replay_descriptor(&self) -> Option<&ArtifactDescriptor> {
        self.artifact_replay_checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.artifact_descriptor.as_ref())
    }

    /// Returns one stored replay chunk payload when present.
    pub fn artifact_replay_chunk_bytes(&self, chunk_id: &ChunkId) -> Option<&[u8]> {
        self.artifact_replay_checkpoint
            .as_ref()
            .and_then(|checkpoint| {
                checkpoint
                    .completed_chunks
                    .iter()
                    .find(|chunk| &chunk.chunk_id == chunk_id)
            })
            .and_then(|chunk| {
                (!chunk.chunk_bytes.is_empty()).then_some(chunk.chunk_bytes.as_slice())
            })
    }

    /// Returns the stored edge replay prefix bytes when present.
    pub fn artifact_replay_edge_prefix_bytes(&self) -> Option<&[u8]> {
        self.artifact_replay_checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.edge_download_prefix.as_ref())
            .and_then(|prefix| (!prefix.bytes.is_empty()).then_some(prefix.bytes.as_slice()))
    }

    /// Returns a clone suitable for durable persistence with large replay chunk payloads
    /// externalized to IndexedDB metadata records.
    pub fn durable_replay_snapshot(&self) -> Self {
        let mut snapshot = self.clone();
        if let Some(checkpoint) = snapshot.artifact_replay_checkpoint.as_mut() {
            for chunk in &mut checkpoint.completed_chunks {
                if !chunk.chunk_bytes.is_empty() {
                    chunk.persisted_bytes = chunk.chunk_bytes.len() as u64;
                    chunk.storage = BrowserArtifactReplayChunkStorage::IndexedDb;
                    chunk.chunk_bytes.clear();
                }
            }
            if let Some(prefix) = checkpoint.edge_download_prefix.as_mut()
                && !prefix.bytes.is_empty()
            {
                prefix.persisted_bytes = prefix.bytes.len() as u64;
                prefix.storage = BrowserArtifactReplayChunkStorage::IndexedDb;
                prefix.bytes.clear();
            }
        }
        snapshot
    }

    /// Performs the clear artifact replay checkpoint operation.
    pub fn clear_artifact_replay_checkpoint(&mut self) {
        self.artifact_replay_checkpoint = None;
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
        self.clear_artifact_replay_checkpoint();
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
        self.artifact_replay_checkpoint = None;
        self.cached_microshards.clear();
        self.updated_at = Utc::now();
    }
}

fn browser_artifact_replay_chunk_len(chunk: &BrowserArtifactReplayChunk) -> u64 {
    chunk.persisted_bytes.max(chunk.chunk_bytes.len() as u64)
}

fn browser_artifact_replay_prefix_len(prefix: &BrowserArtifactReplayBytePrefix) -> u64 {
    prefix.persisted_bytes.max(prefix.bytes.len() as u64)
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
