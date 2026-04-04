use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use crate::{StoredEvalProtocolManifestRecord, deploy::BootstrapPlan};
use burn_p2p::{
    ArtifactTransferState, ContributionReceipt, ContributionReceiptId, HeadDescriptor,
    MetricsRetentionBudget, NodeRuntimeState, NodeTelemetrySnapshot, ReducerLoadAnnouncement,
    RevocationEpoch, SlotRuntimeState,
};
use burn_p2p_core::{
    ExperimentId, HeadEvalReport, HeadId, MergeCertificate, NetworkId, PeerId, PeerWindowMetrics,
    ReducerCohortMetrics, RevisionId, StudyId, TrustBundleExport,
};
use burn_p2p_security::{
    PeerAdmissionReport, PeerTrustLevel, ReputationDecision, ReputationEngine, ReputationState,
};
use burn_p2p_swarm::{PeerStore, SwarmStats};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a receipt query.
pub struct ReceiptQuery {
    /// The study ID.
    pub study_id: Option<StudyId>,
    /// The experiment ID.
    pub experiment_id: Option<ExperimentId>,
    /// The revision ID.
    pub revision_id: Option<RevisionId>,
    /// The peer ID.
    pub peer_id: Option<PeerId>,
}

impl ReceiptQuery {
    /// Performs the matches operation.
    pub fn matches(&self, receipt: &ContributionReceipt) -> bool {
        self.study_id
            .as_ref()
            .is_none_or(|study_id| &receipt.study_id == study_id)
            && self
                .experiment_id
                .as_ref()
                .is_none_or(|experiment_id| &receipt.experiment_id == experiment_id)
            && self
                .revision_id
                .as_ref()
                .is_none_or(|revision_id| &receipt.revision_id == revision_id)
            && self
                .peer_id
                .as_ref()
                .is_none_or(|peer_id| &receipt.peer_id == peer_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a head query.
pub struct HeadQuery {
    /// The study ID.
    pub study_id: Option<StudyId>,
    /// The experiment ID.
    pub experiment_id: Option<ExperimentId>,
    /// The revision ID.
    pub revision_id: Option<RevisionId>,
    /// The head ID.
    pub head_id: Option<HeadId>,
}

impl HeadQuery {
    /// Performs the matches operation.
    pub fn matches(&self, head: &HeadDescriptor) -> bool {
        self.study_id
            .as_ref()
            .is_none_or(|study_id| &head.study_id == study_id)
            && self
                .experiment_id
                .as_ref()
                .is_none_or(|experiment_id| &head.experiment_id == experiment_id)
            && self
                .revision_id
                .as_ref()
                .is_none_or(|revision_id| &head.revision_id == revision_id)
            && self
                .head_id
                .as_ref()
                .is_none_or(|head_id| &head.head_id == head_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a reducer load query.
pub struct ReducerLoadQuery {
    /// The study ID.
    pub study_id: Option<StudyId>,
    /// The experiment ID.
    pub experiment_id: Option<ExperimentId>,
    /// The peer ID.
    pub peer_id: Option<PeerId>,
}

impl ReducerLoadQuery {
    /// Performs the matches operation.
    pub fn matches(&self, announcement: &ReducerLoadAnnouncement) -> bool {
        self.study_id
            .as_ref()
            .is_none_or(|study_id| announcement.overlay.study_id.as_ref() == Some(study_id))
            && self.experiment_id.as_ref().is_none_or(|experiment_id| {
                announcement.overlay.experiment_id.as_ref() == Some(experiment_id)
            })
            && self
                .peer_id
                .as_ref()
                .is_none_or(|peer_id| &announcement.report.peer_id == peer_id)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
/// Captures bootstrap admin state.
pub struct BootstrapAdminState {
    /// The head descriptors.
    pub head_descriptors: Vec<HeadDescriptor>,
    /// The peer store.
    pub peer_store: PeerStore,
    /// The contribution receipts.
    pub contribution_receipts: Vec<ContributionReceipt>,
    /// The merge certificates.
    pub merge_certificates: Vec<MergeCertificate>,
    /// The peer-window metrics loaded from runtime storage.
    pub peer_window_metrics: Vec<PeerWindowMetrics>,
    /// The reducer cohort metrics loaded from runtime storage.
    pub reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    /// The head evaluation reports loaded from runtime storage.
    pub head_eval_reports: Vec<HeadEvalReport>,
    /// The scoped evaluation protocol manifests loaded from runtime storage.
    pub eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
    /// The runtime storage root used to materialize canonical artifacts.
    pub artifact_store_root: Option<PathBuf>,
    /// The durable publication store root, when artifact publication is enabled.
    pub publication_store_root: Option<PathBuf>,
    /// Explicit artifact publication targets configured by the operator.
    pub publication_targets: Vec<burn_p2p_core::PublicationTarget>,
    /// The durable metrics store root, when the metrics indexer is enabled.
    pub metrics_store_root: Option<PathBuf>,
    /// The effective metrics retention budget used for raw-history tails and drilldowns.
    pub metrics_retention: MetricsRetentionBudget,
    /// The reducer load announcements.
    pub reducer_load_announcements: Vec<ReducerLoadAnnouncement>,
    /// The in flight transfers.
    pub in_flight_transfers: Vec<ArtifactTransferState>,
    /// The admitted peers.
    pub admitted_peers: BTreeSet<PeerId>,
    /// The peer admission reports.
    pub peer_admission_reports: BTreeMap<PeerId, PeerAdmissionReport>,
    /// The rejected peers.
    pub rejected_peers: BTreeMap<PeerId, String>,
    /// The peer reputation.
    pub peer_reputation: BTreeMap<PeerId, ReputationState>,
    /// The quarantined peers.
    pub quarantined_peers: BTreeSet<PeerId>,
    /// The banned peers.
    pub banned_peers: BTreeSet<PeerId>,
    /// The minimum revocation epoch.
    pub minimum_revocation_epoch: Option<RevocationEpoch>,
    /// The trust bundle.
    pub trust_bundle: Option<TrustBundleExport>,
    /// The last error.
    pub last_error: Option<String>,
    /// The node state.
    pub node_state: NodeRuntimeState,
    /// The slot states.
    pub slot_states: Vec<SlotRuntimeState>,
    /// The runtime snapshot.
    pub runtime_snapshot: Option<NodeTelemetrySnapshot>,
}

impl BootstrapAdminState {
    /// Performs the ingest contribution receipts operation.
    pub fn ingest_contribution_receipts(
        &mut self,
        receipts: impl IntoIterator<Item = ContributionReceipt>,
    ) -> Vec<ContributionReceiptId> {
        let mut known_receipt_ids = self
            .contribution_receipts
            .iter()
            .map(|receipt| receipt.receipt_id.clone())
            .collect::<BTreeSet<_>>();
        let mut accepted_receipt_ids = Vec::new();

        for receipt in receipts {
            if !known_receipt_ids.insert(receipt.receipt_id.clone()) {
                continue;
            }
            accepted_receipt_ids.push(receipt.receipt_id.clone());
            self.contribution_receipts.push(receipt);
        }

        self.contribution_receipts
            .sort_by_key(|receipt| receipt.accepted_at);
        accepted_receipt_ids
    }

    /// Performs the peer diagnostics operation.
    fn peer_diagnostics(&self) -> Vec<BootstrapPeerDiagnostic> {
        let peer_ids = self
            .peer_store
            .observed_peer_ids()
            .into_iter()
            .chain(self.peer_admission_reports.keys().cloned())
            .chain(self.rejected_peers.keys().cloned())
            .chain(self.peer_reputation.keys().cloned())
            .chain(self.quarantined_peers.iter().cloned())
            .chain(self.banned_peers.iter().cloned())
            .collect::<BTreeSet<_>>();
        let reputation_engine = ReputationEngine::default();

        peer_ids
            .into_iter()
            .map(|peer_id| {
                let observation = self.peer_store.get(&peer_id);
                let admission_report = self.peer_admission_reports.get(&peer_id);
                let reputation_state = self.peer_reputation.get(&peer_id);
                BootstrapPeerDiagnostic {
                    peer_id: peer_id.clone(),
                    connected: observation.map(|entry| entry.connected).unwrap_or(false),
                    observed_at: observation.map(|entry| entry.observed_at),
                    trust_level: admission_report.map(|report| report.trust_level.clone()),
                    rejection_reason: self.rejected_peers.get(&peer_id).cloned(),
                    reputation_score: reputation_state.map(|state| state.score),
                    reputation_decision: reputation_state
                        .map(|state| reputation_engine.decision(state.score)),
                    quarantined: self.quarantined_peers.contains(&peer_id),
                    banned: self.banned_peers.contains(&peer_id),
                }
            })
            .collect()
    }

    /// Exports the receipts.
    pub fn export_receipts(&self, query: &ReceiptQuery) -> Vec<ContributionReceipt> {
        self.contribution_receipts
            .iter()
            .filter(|receipt| query.matches(receipt))
            .cloned()
            .collect()
    }

    /// Exports the heads.
    pub fn export_heads(&self, query: &HeadQuery) -> Vec<HeadDescriptor> {
        self.head_descriptors
            .iter()
            .filter(|head| query.matches(head))
            .cloned()
            .collect()
    }

    /// Exports the reducer load.
    pub fn export_reducer_load(&self, query: &ReducerLoadQuery) -> Vec<ReducerLoadAnnouncement> {
        self.reducer_load_announcements
            .iter()
            .filter(|announcement| query.matches(announcement))
            .cloned()
            .collect()
    }

    /// Exports the head evaluation reports for one head.
    pub fn export_head_eval_reports(&self, head_id: &HeadId) -> Vec<HeadEvalReport> {
        self.head_eval_reports
            .iter()
            .filter(|report| &report.head_id == head_id)
            .cloned()
            .collect()
    }

    /// Performs the diagnostics operation.
    pub fn diagnostics(
        &self,
        plan: &BootstrapPlan,
        captured_at: DateTime<Utc>,
        remaining_work_units: Option<u64>,
    ) -> BootstrapDiagnostics {
        BootstrapDiagnostics {
            network_id: plan.genesis.network_id.clone(),
            preset: plan.preset.clone(),
            services: plan.services.clone(),
            roles: plan.roles.clone(),
            swarm: self.peer_store.stats(remaining_work_units),
            pinned_heads: plan.archive.pinned_heads.clone(),
            pinned_artifacts: plan.archive.pinned_artifacts.clone(),
            accepted_receipts: self.contribution_receipts.len() as u64,
            certified_merges: self.merge_certificates.len() as u64,
            in_flight_transfers: self.in_flight_transfers.clone(),
            admitted_peers: self.admitted_peers.clone(),
            peer_diagnostics: self.peer_diagnostics(),
            rejected_peers: self.rejected_peers.clone(),
            quarantined_peers: self.quarantined_peers.clone(),
            banned_peers: self.banned_peers.clone(),
            minimum_revocation_epoch: self.minimum_revocation_epoch,
            last_error: self.last_error.clone(),
            node_state: self.node_state.clone(),
            slot_states: self.slot_states.clone(),
            captured_at,
        }
    }

    /// Performs the diagnostics bundle operation.
    pub fn diagnostics_bundle(
        &self,
        plan: &BootstrapPlan,
        captured_at: DateTime<Utc>,
        remaining_work_units: Option<u64>,
    ) -> BootstrapDiagnosticsBundle {
        BootstrapDiagnosticsBundle {
            plan: plan.clone(),
            diagnostics: self.diagnostics(plan, captured_at, remaining_work_units),
            runtime_snapshot: self.runtime_snapshot.clone(),
            heads: self.head_descriptors.clone(),
            contribution_receipts: self.contribution_receipts.clone(),
            merge_certificates: self.merge_certificates.clone(),
            reducer_load_announcements: self.reducer_load_announcements.clone(),
            trust_bundle: self.trust_bundle.clone(),
            captured_at,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a bootstrap peer diagnostic.
pub struct BootstrapPeerDiagnostic {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The connected.
    pub connected: bool,
    /// The observed at.
    pub observed_at: Option<DateTime<Utc>>,
    /// The trust level.
    pub trust_level: Option<PeerTrustLevel>,
    /// The rejection reason.
    pub rejection_reason: Option<String>,
    /// The reputation score.
    pub reputation_score: Option<f64>,
    /// The reputation decision.
    pub reputation_decision: Option<ReputationDecision>,
    /// The quarantined.
    pub quarantined: bool,
    /// The banned.
    pub banned: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a bootstrap diagnostics.
pub struct BootstrapDiagnostics {
    /// The network ID.
    pub network_id: NetworkId,
    /// The preset.
    pub preset: crate::BootstrapPreset,
    /// The services.
    pub services: BTreeSet<crate::BootstrapService>,
    /// The roles.
    pub roles: burn_p2p_core::PeerRoleSet,
    /// The swarm.
    pub swarm: SwarmStats,
    /// The pinned heads.
    pub pinned_heads: BTreeSet<HeadId>,
    /// The pinned artifacts.
    pub pinned_artifacts: BTreeSet<burn_p2p_core::ArtifactId>,
    /// The accepted receipts.
    pub accepted_receipts: u64,
    /// The certified merges.
    pub certified_merges: u64,
    /// The in flight transfers.
    pub in_flight_transfers: Vec<ArtifactTransferState>,
    /// The admitted peers.
    pub admitted_peers: BTreeSet<PeerId>,
    /// The peer diagnostics.
    pub peer_diagnostics: Vec<BootstrapPeerDiagnostic>,
    /// The rejected peers.
    pub rejected_peers: BTreeMap<PeerId, String>,
    /// The quarantined peers.
    pub quarantined_peers: BTreeSet<PeerId>,
    /// The banned peers.
    pub banned_peers: BTreeSet<PeerId>,
    /// The minimum revocation epoch.
    pub minimum_revocation_epoch: Option<burn_p2p::RevocationEpoch>,
    /// The last error.
    pub last_error: Option<String>,
    /// The node state.
    pub node_state: NodeRuntimeState,
    /// The slot states.
    pub slot_states: Vec<SlotRuntimeState>,
    /// The captured at.
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a bootstrap diagnostics bundle.
pub struct BootstrapDiagnosticsBundle {
    /// The plan.
    pub plan: BootstrapPlan,
    /// The diagnostics.
    pub diagnostics: BootstrapDiagnostics,
    /// The runtime snapshot.
    pub runtime_snapshot: Option<NodeTelemetrySnapshot>,
    /// The heads.
    pub heads: Vec<HeadDescriptor>,
    /// The contribution receipts.
    pub contribution_receipts: Vec<ContributionReceipt>,
    /// The merge certificates.
    pub merge_certificates: Vec<MergeCertificate>,
    /// The reducer load announcements.
    pub reducer_load_announcements: Vec<ReducerLoadAnnouncement>,
    /// The trust bundle.
    pub trust_bundle: Option<TrustBundleExport>,
    /// The captured at.
    pub captured_at: DateTime<Utc>,
}

/// Performs the render openmetrics operation.
pub fn render_openmetrics(diagnostics: &BootstrapDiagnostics) -> String {
    let mut lines = Vec::new();
    lines.push("# TYPE burn_p2p_connected_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_connected_peers {}",
        diagnostics.swarm.connected_peers
    ));
    lines.push("# TYPE burn_p2p_observed_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_observed_peers {}",
        diagnostics.swarm.observed_peers.len()
    ));
    lines.push("# TYPE burn_p2p_estimated_network_size gauge".to_owned());
    lines.push(format!(
        "burn_p2p_estimated_network_size {}",
        diagnostics.swarm.network_estimate.estimated_network_size
    ));
    lines.push("# TYPE burn_p2p_accepted_receipts counter".to_owned());
    lines.push(format!(
        "burn_p2p_accepted_receipts {}",
        diagnostics.accepted_receipts
    ));
    lines.push("# TYPE burn_p2p_certified_merges counter".to_owned());
    lines.push(format!(
        "burn_p2p_certified_merges {}",
        diagnostics.certified_merges
    ));
    lines.push("# TYPE burn_p2p_in_flight_transfers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_in_flight_transfers {}",
        diagnostics.in_flight_transfers.len()
    ));
    lines.push("# TYPE burn_p2p_admitted_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_admitted_peers {}",
        diagnostics.admitted_peers.len()
    ));
    lines.push("# TYPE burn_p2p_rejected_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_rejected_peers {}",
        diagnostics.rejected_peers.len()
    ));
    lines.push("# TYPE burn_p2p_quarantined_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_quarantined_peers {}",
        diagnostics.quarantined_peers.len()
    ));
    lines.push("# TYPE burn_p2p_banned_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_banned_peers {}",
        diagnostics.banned_peers.len()
    ));

    if let Some(eta_lower_seconds) = diagnostics.swarm.network_estimate.eta_lower_seconds {
        lines.push("# TYPE burn_p2p_eta_lower_seconds gauge".to_owned());
        lines.push(format!("burn_p2p_eta_lower_seconds {eta_lower_seconds}"));
    }
    if let Some(eta_upper_seconds) = diagnostics.swarm.network_estimate.eta_upper_seconds {
        lines.push("# TYPE burn_p2p_eta_upper_seconds gauge".to_owned());
        lines.push(format!("burn_p2p_eta_upper_seconds {eta_upper_seconds}"));
    }
    if let Some(estimated_total_flops) = diagnostics.swarm.network_estimate.estimated_total_flops {
        lines.push("# TYPE burn_p2p_estimated_total_flops gauge".to_owned());
        lines.push(format!(
            "burn_p2p_estimated_total_flops {}",
            estimated_total_flops
        ));
    }
    if let Some(minimum_revocation_epoch) = diagnostics.minimum_revocation_epoch {
        lines.push("# TYPE burn_p2p_minimum_revocation_epoch gauge".to_owned());
        lines.push(format!(
            "burn_p2p_minimum_revocation_epoch {}",
            minimum_revocation_epoch.0
        ));
    }
    if let Some(estimated_total_vram_bytes) = diagnostics
        .swarm
        .network_estimate
        .estimated_total_vram_bytes
    {
        lines.push("# TYPE burn_p2p_estimated_total_vram_bytes gauge".to_owned());
        lines.push(format!(
            "burn_p2p_estimated_total_vram_bytes {}",
            estimated_total_vram_bytes
        ));
    }

    lines.join("\n") + "\n"
}
