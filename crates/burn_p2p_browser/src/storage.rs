use std::collections::BTreeSet;

use burn_p2p::{
    ArtifactDescriptor, ArtifactId, ChunkId, ContributionReceipt, ContributionReceiptId,
    ExperimentDirectoryEntry, ExperimentId, HeadDescriptor, HeadId, MicroShardId, PeerId,
    RevisionId, StudyId, WorkloadTrainingLease,
};
use burn_p2p_core::{
    ArtifactProfile, BrowserArtifactRouteKind, BrowserArtifactSyncDiagnostics, MetricsLiveEvent,
    PublicationTargetId, RunId, SchemaEnvelope, SignedPayload,
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

impl BrowserStoredAssignment {
    fn matches_binding(&self, other: &Self) -> bool {
        self.study_id == other.study_id
            && self.experiment_id == other.experiment_id
            && self.revision_id == other.revision_id
    }
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
/// One persisted range segment used to resume edge-based artifact replay across reloads.
pub struct BrowserArtifactReplayByteSegment {
    /// Inclusive starting byte offset of the persisted segment.
    pub start_offset: u64,
    /// Durable storage backend used for the segment payload.
    #[serde(default)]
    pub storage: BrowserArtifactReplayChunkStorage,
    /// Number of verified bytes retained for the segment.
    #[serde(default)]
    pub persisted_bytes: u64,
    /// Full artifact length when the edge download path exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    /// Persisted contiguous bytes for this segment.
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
    /// Content hash exposed by the publication when the checkpoint was recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_content_hash: Option<burn_p2p::ContentId>,
    /// Content length exposed by the publication when the checkpoint was recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_content_length: Option<u64>,
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
    /// Persisted edge download segments retained for long-offline resumable replay.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edge_download_segments: Vec<BrowserArtifactReplayByteSegment>,
    /// Aggregate completed replay bytes retained in the checkpoint.
    #[serde(default)]
    pub completed_bytes: u64,
    /// Last replay attempt timestamp.
    pub last_attempted_at: DateTime<Utc>,
    /// Number of replay attempts retained for the checkpoint.
    pub attempt_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// In-memory active-head artifact payload retained after replay checkpoint cleanup.
struct BrowserActiveHeadArtifactCache {
    /// Head whose artifact bytes are retained.
    head_id: HeadId,
    /// Complete artifact descriptor.
    descriptor: ArtifactDescriptor,
    /// Verified chunk payloads for peer-swarm artifact replay.
    completed_chunks: Vec<BrowserArtifactReplayChunk>,
    /// Complete edge prefix payload when the artifact came from edge range download.
    edge_download_prefix: Option<BrowserArtifactReplayBytePrefix>,
    /// Edge range segments retained when they reconstruct the complete prefix.
    edge_download_segments: Vec<BrowserArtifactReplayByteSegment>,
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
    /// Most recent active-head artifact sync diagnostic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_head_artifact_sync: Option<BrowserArtifactSyncDiagnostics>,
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
    /// The last usable unsigned directory snapshot learned from browser bootstrap or the peer
    /// swarm. This remains distinct from the signed edge snapshot so the runtime can degrade
    /// truthfully when direct peer state stays healthy but edge control sync is stale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_swarm_directory_snapshot: Option<BrowserDirectorySnapshot>,
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
    /// The last active head descriptor learned from edge or swarm control state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_head_descriptor: Option<HeadDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Durable checkpoint for resuming active-head artifact replay after offline failure.
    pub artifact_replay_checkpoint: Option<BrowserArtifactReplayCheckpoint>,
    /// Complete active-head artifact payload retained for same-page browser training.
    #[serde(skip)]
    active_head_artifact_cache: Option<BrowserActiveHeadArtifactCache>,
    /// The stored certificate peer ID.
    pub stored_certificate_peer_id: Option<PeerId>,
    /// The active assignment.
    pub active_assignment: Option<BrowserStoredAssignment>,
    /// The active training lease accepted for the current assignment.
    #[serde(default)]
    pub active_training_lease: Option<WorkloadTrainingLease>,
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
            last_head_artifact_sync: None,
            cached_microshards: BTreeSet::new(),
            stored_receipts: BTreeSet::new(),
            pending_receipts: BrowserReceiptOutbox::default(),
            submitted_receipts: BTreeSet::new(),
            stored_receipt_order: Vec::new(),
            submitted_receipt_order: Vec::new(),
            last_directory_sync_at: None,
            last_signed_directory_snapshot: None,
            last_swarm_directory_snapshot: None,
            last_signed_leaderboard_snapshot: None,
            metrics_catchup_bundles: Vec::new(),
            last_metrics_live_event: None,
            last_metrics_sync_at: None,
            last_head_id: None,
            last_head_descriptor: None,
            artifact_replay_checkpoint: None,
            active_head_artifact_cache: None,
            stored_certificate_peer_id: None,
            active_assignment: None,
            active_training_lease: None,
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
        let session_changed = self.session.session_id() != session.session_id()
            || self.session.principal_id() != session.principal_id();
        if session_changed {
            self.active_training_lease = None;
        }
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
        self.invalidate_stale_assignment_replay_state();
        self.updated_at = Utc::now();
    }

    /// Performs the remember swarm directory snapshot operation.
    pub fn remember_swarm_directory_snapshot(&mut self, snapshot: BrowserDirectorySnapshot) {
        self.last_directory_sync_at = Some(Utc::now());
        self.last_swarm_directory_snapshot = Some(snapshot);
        self.invalidate_stale_assignment_replay_state();
        self.updated_at = Utc::now();
    }

    /// Returns the best available directory snapshot for local browser operation.
    pub fn directory_snapshot(&self) -> Option<&BrowserDirectorySnapshot> {
        self.last_signed_directory_snapshot
            .as_ref()
            .map(|snapshot| &snapshot.payload.payload)
            .or(self.last_swarm_directory_snapshot.as_ref())
    }

    fn directory_entry_for_assignment(
        &self,
        assignment: &BrowserStoredAssignment,
    ) -> Option<&ExperimentDirectoryEntry> {
        self.directory_snapshot().and_then(|snapshot| {
            snapshot.entries.iter().find(|entry| {
                entry.study_id == assignment.study_id
                    && entry.experiment_id == assignment.experiment_id
            })
        })
    }

    /// Returns the canonical current head for the active assignment when the directory has one.
    pub fn active_assignment_current_head_id(&self) -> Option<&HeadId> {
        let assignment = self.active_assignment.as_ref()?;
        self.directory_entry_for_assignment(assignment)
            .filter(|entry| entry.current_revision_id == assignment.revision_id)
            .and_then(|entry| entry.current_head_id.as_ref())
    }

    /// Returns the current directory revision when the active assignment is still visible.
    pub fn active_assignment_directory_revision(&self) -> Option<RevisionId> {
        let assignment = self.active_assignment.as_ref()?;
        self.directory_entry_for_assignment(assignment)
            .map(|entry| entry.current_revision_id.clone())
    }

    /// Returns the current directory revision when the active assignment has been superseded.
    pub fn active_assignment_rollover_revision(&self) -> Option<RevisionId> {
        let assignment = self.active_assignment.as_ref()?;
        let entry = self.directory_entry_for_assignment(assignment)?;
        (entry.current_revision_id != assignment.revision_id)
            .then(|| entry.current_revision_id.clone())
    }

    /// Clears replay/head state when the signed directory shows that the active assignment has
    /// rolled forward to a newer revision.
    pub fn invalidate_stale_assignment_replay_state(&mut self) -> bool {
        if self.active_assignment_rollover_revision().is_none() {
            return false;
        }
        if self.artifact_replay_checkpoint.is_none()
            && self.last_head_id.is_none()
            && self.last_head_descriptor.is_none()
            && self.last_head_artifact_transport.is_none()
            && self.last_head_artifact_sync.is_none()
            && self.active_training_lease.is_none()
        {
            return false;
        }
        self.artifact_replay_checkpoint = None;
        self.active_head_artifact_cache = None;
        self.last_head_id = None;
        self.last_head_descriptor = None;
        self.last_head_artifact_transport = None;
        self.last_head_artifact_sync = None;
        self.active_training_lease = None;
        self.updated_at = Utc::now();
        true
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
        let assignment_changed = self
            .active_assignment
            .as_ref()
            .is_some_and(|existing| !existing.matches_binding(&assignment));
        if assignment_changed {
            self.clear_artifact_replay_checkpoint();
            self.last_head_id = None;
            self.last_head_descriptor = None;
            self.last_head_artifact_sync = None;
            self.active_training_lease = None;
        }
        self.active_assignment = Some(assignment);
        self.updated_at = Utc::now();
    }

    /// Performs the clear assignment operation.
    pub fn clear_assignment(&mut self) {
        self.active_assignment = None;
        self.active_training_lease = None;
        self.updated_at = Utc::now();
    }

    /// Returns the dataset view ID for the active assignment when it is present in the latest
    /// usable directory snapshot.
    pub fn active_assignment_dataset_view_id(&self) -> Option<&burn_p2p::DatasetViewId> {
        let assignment = self.active_assignment.as_ref()?;
        self.directory_entry_for_assignment(assignment)
            .map(|entry| &entry.dataset_view_id)
    }

    /// Persists the active training lease accepted by the browser worker.
    pub fn remember_active_training_lease(&mut self, lease: WorkloadTrainingLease) {
        self.active_training_lease = Some(lease);
        self.updated_at = Utc::now();
    }

    /// Clears the persisted active training lease.
    pub fn clear_active_training_lease(&mut self) {
        if self.active_training_lease.is_some() {
            self.active_training_lease = None;
            self.updated_at = Utc::now();
        }
    }

    /// Performs the remember head operation.
    pub fn remember_head(&mut self, head_id: HeadId) {
        if self.last_head_id.as_ref() != Some(&head_id) {
            self.last_head_artifact_sync = None;
            self.last_head_descriptor = None;
        }
        self.last_head_id = Some(head_id);
        self.updated_at = Utc::now();
    }

    /// Performs the remember head descriptor operation.
    pub fn remember_head_descriptor(&mut self, head: HeadDescriptor) {
        let head_id = head.head_id.clone();
        if self.last_head_id.as_ref() != Some(&head_id) {
            self.last_head_artifact_sync = None;
        }
        self.last_head_id = Some(head_id);
        self.last_head_descriptor = Some(head);
        self.updated_at = Utc::now();
    }

    /// Records the route selected for the active-head artifact sync attempt.
    pub fn remember_active_head_artifact_sync_attempt(
        &mut self,
        head_id: HeadId,
        artifact_id: ArtifactId,
        provider_peer_ids: Vec<PeerId>,
        route_label: impl Into<String>,
        route_kind: BrowserArtifactRouteKind,
    ) {
        self.last_head_artifact_sync = Some(BrowserArtifactSyncDiagnostics {
            head_id: Some(head_id),
            artifact_id: Some(artifact_id),
            provider_peer_ids,
            selected_peer_id: None,
            route_label: Some(route_label.into()),
            route_kind,
            completed_bytes: None,
            duration_ms: None,
            last_error: None,
        });
        self.updated_at = Utc::now();
    }

    /// Records the latest active-head artifact sync failure.
    pub fn remember_active_head_artifact_sync_failure(
        &mut self,
        route_label: impl Into<String>,
        route_kind: BrowserArtifactRouteKind,
        duration_ms: u64,
        error: impl Into<String>,
    ) {
        let completed_bytes = self
            .artifact_replay_checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.completed_bytes);
        let diagnostic = self
            .last_head_artifact_sync
            .get_or_insert_with(Default::default);
        diagnostic.route_label = Some(route_label.into());
        diagnostic.route_kind = route_kind;
        diagnostic.completed_bytes = completed_bytes;
        diagnostic.duration_ms = Some(duration_ms);
        diagnostic.last_error = Some(error.into());
        self.updated_at = Utc::now();
    }

    /// Records the latest active-head artifact sync success.
    pub fn remember_active_head_artifact_sync_success(
        &mut self,
        route_label: impl Into<String>,
        route_kind: BrowserArtifactRouteKind,
        duration_ms: u64,
        completed_bytes: u64,
    ) {
        let diagnostic = self
            .last_head_artifact_sync
            .get_or_insert_with(Default::default);
        diagnostic.route_label = Some(route_label.into());
        diagnostic.route_kind = route_kind;
        diagnostic.completed_bytes = Some(completed_bytes);
        diagnostic.duration_ms = Some(duration_ms);
        diagnostic.last_error = None;
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
            checkpoint.edge_download_segments.clear();
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
            let previous_prefix_len = checkpoint
                .edge_download_prefix
                .as_ref()
                .map(browser_artifact_replay_prefix_len)
                .or_else(|| {
                    replay_edge_prefix_bytes_from_segments(&checkpoint.edge_download_segments)
                        .map(|prefix| prefix.len() as u64)
                })
                .unwrap_or(0);
            if bytes.len() as u64 > previous_prefix_len {
                checkpoint
                    .edge_download_segments
                    .push(BrowserArtifactReplayByteSegment {
                        start_offset: previous_prefix_len,
                        storage: BrowserArtifactReplayChunkStorage::Inline,
                        persisted_bytes: (bytes.len() as u64).saturating_sub(previous_prefix_len),
                        total_bytes,
                        bytes: bytes[previous_prefix_len as usize..].to_vec(),
                    });
                checkpoint
                    .edge_download_segments
                    .sort_by_key(|segment| segment.start_offset);
            }
            checkpoint.edge_download_prefix = Some(BrowserArtifactReplayBytePrefix {
                storage: BrowserArtifactReplayChunkStorage::Inline,
                persisted_bytes: bytes.len() as u64,
                total_bytes,
                bytes,
            });
            let chunk_completed_bytes: u64 = checkpoint
                .completed_chunks
                .iter()
                .map(browser_artifact_replay_chunk_len)
                .sum();
            let edge_completed_bytes = checkpoint
                .edge_download_prefix
                .as_ref()
                .map(browser_artifact_replay_prefix_len)
                .unwrap_or(0);
            checkpoint.completed_bytes = chunk_completed_bytes.max(edge_completed_bytes);
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
    pub fn artifact_replay_edge_prefix_bytes(&self) -> Option<Vec<u8>> {
        self.artifact_replay_checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.edge_download_prefix.as_ref())
            .and_then(|prefix| (!prefix.bytes.is_empty()).then_some(prefix.bytes.clone()))
            .or_else(|| {
                self.artifact_replay_checkpoint
                    .as_ref()
                    .and_then(|checkpoint| {
                        replay_edge_prefix_bytes_from_segments(&checkpoint.edge_download_segments)
                    })
            })
    }

    /// Returns the active head artifact bytes when the replay cache still has a
    /// complete descriptor and byte payload in memory.
    pub fn active_head_artifact_bytes(&self) -> Option<(HeadId, ArtifactDescriptor, Vec<u8>)> {
        if let Some(cache) = self.active_head_artifact_cache.as_ref()
            && let Some(bytes) = active_head_artifact_cache_bytes(cache)
            && self.cached_head_artifact_heads.contains(&cache.head_id)
        {
            return Some((cache.head_id.clone(), cache.descriptor.clone(), bytes));
        }

        let descriptor = self.artifact_replay_descriptor()?.clone();
        let head_id = descriptor
            .head_id
            .clone()
            .or_else(|| self.last_head_id.clone())?;
        if !self.cached_head_artifact_heads.contains(&head_id) {
            return None;
        }

        if let Some(bytes) = self.artifact_replay_edge_prefix_bytes()
            && bytes.len() as u64 == descriptor.bytes_len
        {
            return Some((head_id, descriptor, bytes));
        }

        let mut chunks = descriptor.chunks.clone();
        chunks.sort_by_key(|chunk| chunk.offset_bytes);
        let capacity = usize::try_from(descriptor.bytes_len).ok()?;
        let mut bytes = Vec::with_capacity(capacity);
        for chunk in &chunks {
            let chunk_bytes = self.artifact_replay_chunk_bytes(&chunk.chunk_id)?;
            if chunk_bytes.len() as u64 != chunk.length_bytes {
                return None;
            }
            bytes.extend_from_slice(chunk_bytes);
        }
        (bytes.len() as u64 == descriptor.bytes_len).then_some((head_id, descriptor, bytes))
    }

    /// Returns whether the active head artifact has complete bytes available locally.
    pub fn active_head_artifact_ready(&self) -> bool {
        let Some(head_id) = self.last_head_id.as_ref() else {
            return false;
        };
        if !self.cached_head_artifact_heads.contains(head_id) {
            return false;
        }
        if let Some(cache) = self.active_head_artifact_cache.as_ref()
            && &cache.head_id == head_id
        {
            return active_head_artifact_cache_complete(cache);
        }
        let Some(descriptor) = self.artifact_replay_descriptor() else {
            return false;
        };
        if let Some(bytes) = self.artifact_replay_edge_prefix_bytes() {
            return bytes.len() as u64 == descriptor.bytes_len;
        }
        descriptor.chunks.iter().all(|chunk| {
            self.artifact_replay_chunk_bytes(&chunk.chunk_id)
                .is_some_and(|bytes| bytes.len() as u64 == chunk.length_bytes)
        })
    }

    /// Returns whether the latest active head artifact finished syncing into browser storage.
    ///
    /// This is intentionally weaker than [`Self::active_head_artifact_ready`]. The ready
    /// predicate requires bytes to be available in the current storage instance for training,
    /// while browser UI snapshots may cross a worker boundary where the in-memory byte cache is
    /// not serialized.
    pub fn active_head_artifact_synced(&self) -> bool {
        if self.active_head_artifact_ready() {
            return true;
        }

        let Some(head_id) = self.last_head_id.as_ref() else {
            return false;
        };
        if !self.cached_head_artifact_heads.contains(head_id) {
            return false;
        }

        let Some(diagnostic) = self.last_head_artifact_sync.as_ref() else {
            return false;
        };
        if diagnostic.head_id.as_ref() != Some(head_id) {
            return false;
        }
        if diagnostic.last_error.is_some() {
            return false;
        }
        diagnostic.completed_bytes.is_some_and(|bytes| bytes > 0)
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
            for segment in &mut checkpoint.edge_download_segments {
                if !segment.bytes.is_empty() {
                    segment.persisted_bytes = segment.bytes.len() as u64;
                    segment.storage = BrowserArtifactReplayChunkStorage::IndexedDb;
                    segment.bytes.clear();
                }
            }
        }
        snapshot
    }

    /// Performs the clear artifact replay checkpoint operation.
    pub fn clear_artifact_replay_checkpoint(&mut self) {
        self.artifact_replay_checkpoint = None;
        self.active_head_artifact_cache = None;
        self.last_head_artifact_sync = None;
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
        let transport = transport.into();
        let completed_bytes = self
            .active_head_artifact_bytes()
            .map(|(_, _, bytes)| bytes.len() as u64)
            .or_else(|| {
                self.artifact_replay_checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.completed_bytes)
            })
            .filter(|completed_bytes| *completed_bytes > 0);
        self.remember_active_head_artifact_cache(&head_id, &artifact_id);
        self.artifact_replay_checkpoint = None;
        self.cached_head_artifact_heads.insert(head_id);
        self.cached_chunk_artifacts.insert(artifact_id);
        self.last_head_artifact_transport = Some(transport.clone());
        if let Some(diagnostic) = self.last_head_artifact_sync.as_mut() {
            diagnostic.route_label = Some(transport.clone());
            diagnostic.route_kind = if transport.starts_with("edge-") {
                BrowserArtifactRouteKind::EdgeHttp
            } else {
                BrowserArtifactRouteKind::PeerSwarm
            };
            if let Some(completed_bytes) = completed_bytes.or(diagnostic.completed_bytes) {
                diagnostic.completed_bytes = Some(completed_bytes);
            }
            diagnostic.duration_ms.get_or_insert(0);
            diagnostic.last_error = None;
        }
        self.updated_at = Utc::now();
    }

    fn remember_active_head_artifact_cache(&mut self, head_id: &HeadId, artifact_id: &ArtifactId) {
        let Some(checkpoint) = self.artifact_replay_checkpoint.as_ref() else {
            return;
        };
        if &checkpoint.head_id != head_id || &checkpoint.artifact_id != artifact_id {
            return;
        }
        let Some(descriptor) = checkpoint.artifact_descriptor.clone() else {
            return;
        };
        self.active_head_artifact_cache = Some(BrowserActiveHeadArtifactCache {
            head_id: head_id.clone(),
            descriptor,
            completed_chunks: checkpoint.completed_chunks.clone(),
            edge_download_prefix: checkpoint.edge_download_prefix.clone(),
            edge_download_segments: checkpoint.edge_download_segments.clone(),
        });
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
        self.last_head_artifact_sync = None;
        self.artifact_replay_checkpoint = None;
        self.active_head_artifact_cache = None;
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

fn browser_artifact_replay_segment_len(segment: &BrowserArtifactReplayByteSegment) -> u64 {
    segment.persisted_bytes.max(segment.bytes.len() as u64)
}

fn active_head_artifact_cache_bytes(cache: &BrowserActiveHeadArtifactCache) -> Option<Vec<u8>> {
    if let Some(prefix) = cache.edge_download_prefix.as_ref()
        && !prefix.bytes.is_empty()
        && prefix.bytes.len() as u64 == cache.descriptor.bytes_len
    {
        return Some(prefix.bytes.clone());
    }
    if let Some(bytes) = replay_edge_prefix_bytes_from_segments(&cache.edge_download_segments)
        && bytes.len() as u64 == cache.descriptor.bytes_len
    {
        return Some(bytes);
    }

    let mut chunks = cache.descriptor.chunks.clone();
    chunks.sort_by_key(|chunk| chunk.offset_bytes);
    let capacity = usize::try_from(cache.descriptor.bytes_len).ok()?;
    let mut bytes = Vec::with_capacity(capacity);
    for chunk in &chunks {
        let chunk_bytes = cache
            .completed_chunks
            .iter()
            .find(|candidate| candidate.chunk_id == chunk.chunk_id)
            .and_then(|candidate| {
                (!candidate.chunk_bytes.is_empty()).then_some(candidate.chunk_bytes.as_slice())
            })?;
        if chunk_bytes.len() as u64 != chunk.length_bytes {
            return None;
        }
        bytes.extend_from_slice(chunk_bytes);
    }
    (bytes.len() as u64 == cache.descriptor.bytes_len).then_some(bytes)
}

fn active_head_artifact_cache_complete(cache: &BrowserActiveHeadArtifactCache) -> bool {
    if cache
        .edge_download_prefix
        .as_ref()
        .is_some_and(|prefix| prefix.bytes.len() as u64 == cache.descriptor.bytes_len)
    {
        return true;
    }
    if replay_edge_segments_cover(&cache.edge_download_segments, cache.descriptor.bytes_len) {
        return true;
    }

    cache.descriptor.chunks.iter().all(|chunk| {
        cache.completed_chunks.iter().any(|candidate| {
            candidate.chunk_id == chunk.chunk_id
                && candidate.chunk_bytes.len() as u64 == chunk.length_bytes
        })
    })
}

fn replay_edge_prefix_bytes_from_segments(
    segments: &[BrowserArtifactReplayByteSegment],
) -> Option<Vec<u8>> {
    if segments.is_empty() {
        return None;
    }

    let mut sorted = segments.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|segment| segment.start_offset);
    let mut prefix = Vec::new();
    let mut expected_offset = 0u64;
    for segment in sorted {
        if segment.start_offset > expected_offset {
            break;
        }
        let segment_len = browser_artifact_replay_segment_len(segment);
        if segment_len == 0 {
            continue;
        }
        if segment.bytes.is_empty() {
            return None;
        }
        let overlap = expected_offset.saturating_sub(segment.start_offset) as usize;
        if overlap >= segment.bytes.len() {
            expected_offset = segment.start_offset.saturating_add(segment_len);
            continue;
        }
        prefix.extend_from_slice(&segment.bytes[overlap..]);
        expected_offset = segment.start_offset.saturating_add(segment_len);
    }
    (!prefix.is_empty()).then_some(prefix)
}

fn replay_edge_segments_cover(
    segments: &[BrowserArtifactReplayByteSegment],
    total_bytes: u64,
) -> bool {
    if segments.is_empty() {
        return false;
    }

    let mut sorted = segments.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|segment| segment.start_offset);
    let mut expected_offset = 0u64;
    for segment in sorted {
        if segment.start_offset > expected_offset {
            return false;
        }
        let segment_len = browser_artifact_replay_segment_len(segment);
        if segment_len == 0 || segment.bytes.is_empty() {
            return false;
        }
        expected_offset = expected_offset.max(segment.start_offset.saturating_add(segment_len));
        if expected_offset >= total_bytes {
            return true;
        }
    }
    expected_offset >= total_bytes
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
