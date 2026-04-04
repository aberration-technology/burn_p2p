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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Captures a snapshot of browser storage.
pub struct BrowserStorageSnapshot {
    /// The metadata version.
    pub metadata_version: u16,
    /// The session.
    pub session: BrowserSessionState,
    /// The cached chunk artifacts.
    pub cached_chunk_artifacts: BTreeSet<ArtifactId>,
    /// The cached microshards.
    pub cached_microshards: BTreeSet<MicroShardId>,
    /// The stored receipts.
    pub stored_receipts: BTreeSet<ContributionReceiptId>,
    /// The pending receipts.
    pub pending_receipts: Vec<ContributionReceipt>,
    /// The submitted receipts.
    pub submitted_receipts: BTreeSet<ContributionReceiptId>,
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
            cached_microshards: BTreeSet::new(),
            stored_receipts: BTreeSet::new(),
            pending_receipts: Vec::new(),
            submitted_receipts: BTreeSet::new(),
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

    /// Performs the remember microshard operation.
    pub fn remember_microshard(&mut self, microshard_id: MicroShardId) {
        self.cached_microshards.insert(microshard_id);
        self.updated_at = Utc::now();
    }

    /// Performs the remember receipt operation.
    pub fn remember_receipt(&mut self, receipt_id: ContributionReceiptId) {
        self.stored_receipts.insert(receipt_id);
        self.updated_at = Utc::now();
    }

    /// Performs the queue receipt operation.
    pub fn queue_receipt(&mut self, receipt: ContributionReceipt) {
        if self.submitted_receipts.contains(&receipt.receipt_id)
            || self
                .pending_receipts
                .iter()
                .any(|pending| pending.receipt_id == receipt.receipt_id)
        {
            return;
        }
        self.stored_receipts.insert(receipt.receipt_id.clone());
        self.pending_receipts.push(receipt);
        self.updated_at = Utc::now();
    }

    /// Performs the acknowledge receipts operation.
    pub fn acknowledge_receipts(&mut self, receipt_ids: &[ContributionReceiptId]) -> usize {
        let acknowledged = receipt_ids.iter().cloned().collect::<BTreeSet<_>>();
        if acknowledged.is_empty() {
            return self.pending_receipts.len();
        }
        self.pending_receipts
            .retain(|receipt| !acknowledged.contains(&receipt.receipt_id));
        self.submitted_receipts.extend(acknowledged);
        self.updated_at = Utc::now();
        self.pending_receipts.len()
    }

    /// Performs the clear cached data operation.
    pub fn clear_cached_data(&mut self) {
        self.cached_chunk_artifacts.clear();
        self.cached_microshards.clear();
        self.updated_at = Utc::now();
    }
}
