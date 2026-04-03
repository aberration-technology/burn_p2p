use std::collections::BTreeSet;

use burn_p2p::{
    ArtifactId, ContributionReceipt, ContributionReceiptId, ExperimentId, HeadId, MicroShardId,
    PeerId, RevisionId, StudyId,
};
use burn_p2p_core::{SchemaEnvelope, SignedPayload};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{BrowserDirectorySnapshot, BrowserLeaderboardSnapshot, BrowserSessionState};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserStoredAssignment {
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrowserStorageSnapshot {
    pub metadata_version: u16,
    pub session: BrowserSessionState,
    pub cached_chunk_artifacts: BTreeSet<ArtifactId>,
    pub cached_microshards: BTreeSet<MicroShardId>,
    pub stored_receipts: BTreeSet<ContributionReceiptId>,
    pub pending_receipts: Vec<ContributionReceipt>,
    pub submitted_receipts: BTreeSet<ContributionReceiptId>,
    pub last_directory_sync_at: Option<DateTime<Utc>>,
    pub last_signed_directory_snapshot:
        Option<SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>>>,
    pub last_signed_leaderboard_snapshot:
        Option<SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>>>,
    pub last_head_id: Option<HeadId>,
    pub stored_certificate_peer_id: Option<PeerId>,
    pub active_assignment: Option<BrowserStoredAssignment>,
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
            last_head_id: None,
            stored_certificate_peer_id: None,
            active_assignment: None,
            updated_at: Utc::now(),
        }
    }
}

impl BrowserStorageSnapshot {
    pub fn remember_session(&mut self, session: BrowserSessionState) {
        self.stored_certificate_peer_id = session.peer_id().cloned();
        self.session = session;
        self.updated_at = Utc::now();
    }

    pub fn remember_directory_snapshot(
        &mut self,
        snapshot: SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>>,
    ) {
        self.last_directory_sync_at = Some(Utc::now());
        self.last_signed_directory_snapshot = Some(snapshot);
        self.updated_at = Utc::now();
    }

    pub fn remember_leaderboard_snapshot(
        &mut self,
        snapshot: SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>>,
    ) {
        self.last_signed_leaderboard_snapshot = Some(snapshot);
        self.updated_at = Utc::now();
    }

    pub fn remember_assignment(&mut self, assignment: BrowserStoredAssignment) {
        self.active_assignment = Some(assignment);
        self.updated_at = Utc::now();
    }

    pub fn clear_assignment(&mut self) {
        self.active_assignment = None;
        self.updated_at = Utc::now();
    }

    pub fn remember_head(&mut self, head_id: HeadId) {
        self.last_head_id = Some(head_id);
        self.updated_at = Utc::now();
    }

    pub fn remember_chunk_artifact(&mut self, artifact_id: ArtifactId) {
        self.cached_chunk_artifacts.insert(artifact_id);
        self.updated_at = Utc::now();
    }

    pub fn remember_microshard(&mut self, microshard_id: MicroShardId) {
        self.cached_microshards.insert(microshard_id);
        self.updated_at = Utc::now();
    }

    pub fn remember_receipt(&mut self, receipt_id: ContributionReceiptId) {
        self.stored_receipts.insert(receipt_id);
        self.updated_at = Utc::now();
    }

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

    pub fn clear_cached_data(&mut self) {
        self.cached_chunk_artifacts.clear();
        self.cached_microshards.clear();
        self.updated_at = Utc::now();
    }
}
