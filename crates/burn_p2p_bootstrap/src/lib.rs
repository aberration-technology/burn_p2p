#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use burn_p2p::{
    ArtifactTransferState, ContentId, ControlHandle, ExperimentHandle, HeadDescriptor,
    IdentityConfig, LiveControlPlaneEvent, NodeBuilder, NodeConfig, NodeTelemetrySnapshot,
    ProjectBackend, ProjectFamilyId, ReducerLoadAnnouncement, RevocationEpoch, RunningNode,
    RuntimeProject, StorageConfig, TelemetryHandle, TrustedIssuer,
};
use burn_p2p_core::{
    ArtifactId, ContributionReceipt, ControlCertificate, ExperimentId, GenesisSpec, HeadId,
    MergeCertificate, NetworkId, PeerId, PeerRole, PeerRoleSet, RevisionId, SignatureMetadata,
    StudyId,
};
use burn_p2p_experiment::{ExperimentControlCommand, ExperimentControlEnvelope};
use burn_p2p_security::{
    PeerAdmissionReport, PeerTrustLevel, ReleasePolicy, ReputationDecision, ReputationEngine,
    ReputationState, ValidatorPolicy,
};
use burn_p2p_swarm::{PeerStore, RuntimeBoundary, SwarmAddress, SwarmError, SwarmStats};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use burn_p2p::{NodeRuntimeState, SlotRuntimeState};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BootstrapPreset {
    BootstrapOnly,
    BootstrapArchive,
    AuthorityValidator,
    AllInOne,
}

impl BootstrapPreset {
    pub fn services(&self) -> BTreeSet<BootstrapService> {
        match self {
            Self::BootstrapOnly => BTreeSet::from([
                BootstrapService::Relay,
                BootstrapService::Rendezvous,
                BootstrapService::Kademlia,
                BootstrapService::AdminApi,
                BootstrapService::TelemetryExport,
            ]),
            Self::BootstrapArchive => BTreeSet::from([
                BootstrapService::Relay,
                BootstrapService::Rendezvous,
                BootstrapService::Kademlia,
                BootstrapService::Archive,
                BootstrapService::AdminApi,
                BootstrapService::TelemetryExport,
            ]),
            Self::AuthorityValidator => BTreeSet::from([
                BootstrapService::Authority,
                BootstrapService::Validator,
                BootstrapService::ExperimentController,
                BootstrapService::AdminApi,
                BootstrapService::TelemetryExport,
            ]),
            Self::AllInOne => BTreeSet::from([
                BootstrapService::Relay,
                BootstrapService::Rendezvous,
                BootstrapService::Kademlia,
                BootstrapService::Authority,
                BootstrapService::Validator,
                BootstrapService::Archive,
                BootstrapService::AdminApi,
                BootstrapService::ExperimentController,
                BootstrapService::TelemetryExport,
            ]),
        }
    }

    pub fn roles(&self) -> PeerRoleSet {
        match self {
            Self::BootstrapOnly => PeerRoleSet::new([PeerRole::Bootstrap, PeerRole::RelayHelper]),
            Self::BootstrapArchive => PeerRoleSet::new([
                PeerRole::Bootstrap,
                PeerRole::Archive,
                PeerRole::RelayHelper,
            ]),
            Self::AuthorityValidator => {
                PeerRoleSet::new([PeerRole::Authority, PeerRole::Validator])
            }
            Self::AllInOne => PeerRoleSet::new([
                PeerRole::Bootstrap,
                PeerRole::Authority,
                PeerRole::Validator,
                PeerRole::Archive,
                PeerRole::RelayHelper,
            ]),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BootstrapService {
    Relay,
    Rendezvous,
    Kademlia,
    Authority,
    Validator,
    Archive,
    AdminApi,
    ExperimentController,
    TelemetryExport,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AuthorityPlan {
    pub release_policy: ReleasePolicy,
    pub validator_policy: ValidatorPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchivePlan {
    pub pinned_heads: BTreeSet<HeadId>,
    pub pinned_artifacts: BTreeSet<ArtifactId>,
    pub retain_contribution_receipts: bool,
}

impl Default for ArchivePlan {
    fn default() -> Self {
        Self {
            pinned_heads: BTreeSet::new(),
            pinned_artifacts: BTreeSet::new(),
            retain_contribution_receipts: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminApiPlan {
    pub supported_actions: BTreeSet<AdminCapability>,
    pub diagnostics_enabled: bool,
    pub receipt_exports_enabled: bool,
}

impl Default for AdminApiPlan {
    fn default() -> Self {
        Self {
            supported_actions: BTreeSet::from([
                AdminCapability::Control,
                AdminCapability::BanPeer,
                AdminCapability::ExportDiagnostics,
                AdminCapability::ExportDiagnosticsBundle,
                AdminCapability::ExportHeads,
                AdminCapability::ExportReceipts,
                AdminCapability::ExportReducerLoad,
                AdminCapability::ExportTrustBundle,
                AdminCapability::RolloutAuthPolicy,
                AdminCapability::RetireTrustedIssuers,
                AdminCapability::RotateAuthorityMaterial,
            ]),
            diagnostics_enabled: true,
            receipt_exports_enabled: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AdminCapability {
    Control,
    BanPeer,
    ExportDiagnostics,
    ExportDiagnosticsBundle,
    ExportHeads,
    ExportReceipts,
    ExportReducerLoad,
    ExportTrustBundle,
    RolloutAuthPolicy,
    RetireTrustedIssuers,
    RotateAuthorityMaterial,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryExportPlan {
    pub openmetrics_enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BootstrapSpec {
    pub preset: BootstrapPreset,
    pub genesis: GenesisSpec,
    pub platform: burn_p2p_core::ClientPlatform,
    pub bootstrap_addresses: Vec<SwarmAddress>,
    pub listen_addresses: Vec<SwarmAddress>,
    pub authority: Option<AuthorityPlan>,
    pub archive: ArchivePlan,
    pub admin_api: AdminApiPlan,
}

impl BootstrapSpec {
    pub fn plan(self) -> Result<BootstrapPlan, BootstrapError> {
        let services = self.preset.services();

        if services.contains(&BootstrapService::Authority) && self.authority.is_none() {
            return Err(BootstrapError::MissingAuthorityPolicy);
        }

        Ok(BootstrapPlan {
            preset: self.preset.clone(),
            roles: self.preset.roles(),
            runtime: RuntimeBoundary::for_platform(
                &self.genesis,
                self.platform,
                self.bootstrap_addresses,
                self.listen_addresses,
            )?,
            telemetry: TelemetryExportPlan {
                openmetrics_enabled: true,
            },
            services,
            genesis: self.genesis,
            authority: self.authority,
            archive: self.archive,
            admin_api: self.admin_api,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BootstrapPlan {
    pub preset: BootstrapPreset,
    pub genesis: GenesisSpec,
    pub services: BTreeSet<BootstrapService>,
    pub roles: PeerRoleSet,
    pub runtime: RuntimeBoundary,
    pub authority: Option<AuthorityPlan>,
    pub archive: ArchivePlan,
    pub admin_api: AdminApiPlan,
    pub telemetry: TelemetryExportPlan,
}

impl BootstrapPlan {
    pub fn network_id(&self) -> &NetworkId {
        &self.genesis.network_id
    }

    pub fn supports_service(&self, service: &BootstrapService) -> bool {
        self.services.contains(service)
    }

    pub fn supports_admin_action(&self, action: &AdminAction) -> bool {
        self.admin_api
            .supported_actions
            .contains(&action.capability())
    }

    pub fn issue_control_certificate(
        &self,
        command: ExperimentControlCommand,
        signer: SignatureMetadata,
    ) -> Result<ControlCertificate<ExperimentControlEnvelope>, BootstrapError> {
        if !self.supports_service(&BootstrapService::Authority) {
            return Err(BootstrapError::AuthorityServiceRequired);
        }

        Ok(ExperimentControlEnvelope {
            network_id: self.genesis.network_id.clone(),
            command,
        }
        .into_signed_cert(signer, self.genesis.protocol_version.clone())?)
    }

    pub fn execute_admin_action(
        &self,
        action: AdminAction,
        state: &mut BootstrapAdminState,
        signer: Option<SignatureMetadata>,
        captured_at: DateTime<Utc>,
        remaining_work_units: Option<u64>,
    ) -> Result<AdminResult, BootstrapError> {
        if !self.supports_admin_action(&action) {
            return Err(BootstrapError::UnsupportedAdminAction(action.capability()));
        }

        match action {
            AdminAction::Control(command) => {
                let signer = signer.ok_or(BootstrapError::MissingSigner)?;
                apply_control_command_to_admin_state(state, &command);
                let certificate = self.issue_control_certificate(command, signer)?;
                Ok(AdminResult::Control(certificate))
            }
            AdminAction::BanPeer { peer_id, reason } => {
                state.quarantined_peers.insert(peer_id.clone());
                state.banned_peers.insert(peer_id.clone());
                Ok(AdminResult::PeerBanned { peer_id, reason })
            }
            AdminAction::ExportDiagnostics => Ok(AdminResult::Diagnostics(state.diagnostics(
                self,
                captured_at,
                remaining_work_units,
            ))),
            AdminAction::ExportDiagnosticsBundle => Ok(AdminResult::DiagnosticsBundle(Box::new(
                state.diagnostics_bundle(self, captured_at, remaining_work_units),
            ))),
            AdminAction::ExportHeads(query) => Ok(AdminResult::Heads(state.export_heads(&query))),
            AdminAction::ExportReceipts(query) => {
                Ok(AdminResult::Receipts(state.export_receipts(&query)))
            }
            AdminAction::ExportReducerLoad(query) => {
                Ok(AdminResult::ReducerLoad(state.export_reducer_load(&query)))
            }
            AdminAction::ExportTrustBundle => {
                Ok(AdminResult::TrustBundle(state.trust_bundle.clone()))
            }
            AdminAction::RolloutAuthPolicy(rollout) => {
                if let Some(minimum_revocation_epoch) = rollout.minimum_revocation_epoch {
                    state.minimum_revocation_epoch = Some(
                        state
                            .minimum_revocation_epoch
                            .map(|epoch| epoch.max(minimum_revocation_epoch))
                            .unwrap_or(minimum_revocation_epoch),
                    );
                }
                let trusted_issuers = if let Some(trusted_issuers) = rollout.trusted_issuers {
                    if let Some(trust_bundle) = state.trust_bundle.as_mut() {
                        let active_issuer_peer_id = trust_bundle.active_issuer_peer_id.clone();
                        trust_bundle.issuers = trusted_issuers
                            .into_iter()
                            .map(|issuer| TrustedIssuerStatus {
                                active_for_new_certificates: issuer.issuer_peer_id
                                    == active_issuer_peer_id,
                                accepted_for_admission: true,
                                issuer_peer_id: issuer.issuer_peer_id,
                                issuer_public_key_hex: issuer.issuer_public_key_hex,
                            })
                            .collect();
                        trust_bundle.issuers.len()
                    } else {
                        trusted_issuers.len()
                    }
                } else {
                    state
                        .trust_bundle
                        .as_ref()
                        .map(|trust_bundle| trust_bundle.issuers.len())
                        .unwrap_or_default()
                };
                let reenrollment_required = if let Some(reenrollment) = rollout.reenrollment {
                    if let Some(trust_bundle) = state.trust_bundle.as_mut() {
                        trust_bundle.reenrollment = Some(reenrollment);
                    }
                    true
                } else {
                    state
                        .trust_bundle
                        .as_ref()
                        .and_then(|trust_bundle| trust_bundle.reenrollment.as_ref())
                        .is_some()
                };
                if let Some(minimum_revocation_epoch) = state.minimum_revocation_epoch
                    && let Some(trust_bundle) = state.trust_bundle.as_mut()
                {
                    trust_bundle.minimum_revocation_epoch = minimum_revocation_epoch;
                }
                Ok(AdminResult::AuthPolicyRolledOut {
                    minimum_revocation_epoch: rollout.minimum_revocation_epoch,
                    directory_entries: rollout.directory_entries.as_ref().map_or(0, Vec::len),
                    trusted_issuers,
                    reenrollment_required,
                })
            }
            AdminAction::RetireTrustedIssuers { issuer_peer_ids } => {
                let Some(trust_bundle) = state.trust_bundle.as_mut() else {
                    return Ok(AdminResult::TrustedIssuersRetired {
                        retired_issuers: 0,
                        remaining_issuers: 0,
                        reenrollment_required: false,
                    });
                };
                let active_issuer_peer_id = trust_bundle.active_issuer_peer_id.clone();
                let previous_len = trust_bundle.issuers.len();
                trust_bundle.issuers.retain(|issuer| {
                    issuer.issuer_peer_id == active_issuer_peer_id
                        || !issuer_peer_ids.contains(&issuer.issuer_peer_id)
                });
                if let Some(reenrollment) = trust_bundle.reenrollment.as_mut() {
                    reenrollment
                        .legacy_issuer_peer_ids
                        .retain(|issuer_peer_id| !issuer_peer_ids.contains(issuer_peer_id));
                    if reenrollment.legacy_issuer_peer_ids.is_empty() {
                        trust_bundle.reenrollment = None;
                    }
                }
                Ok(AdminResult::TrustedIssuersRetired {
                    retired_issuers: previous_len.saturating_sub(trust_bundle.issuers.len()),
                    remaining_issuers: trust_bundle.issuers.len(),
                    reenrollment_required: trust_bundle.reenrollment.is_some(),
                })
            }
            AdminAction::RotateAuthorityMaterial {
                issuer_key_id,
                retain_previous_issuer: _,
                require_reenrollment,
                reenrollment_reason: _,
            } => Ok(AdminResult::AuthorityMaterialRotated {
                issuer_key_id: issuer_key_id.unwrap_or_else(|| "bootstrap-auth".into()),
                issuer_peer_id: PeerId::new("authority-rotated"),
                issuer_public_key_hex: String::new(),
                trusted_issuers: state
                    .trust_bundle
                    .as_ref()
                    .map(|trust_bundle| trust_bundle.issuers.len())
                    .unwrap_or(0),
                reenrollment_required: require_reenrollment
                    || state
                        .trust_bundle
                        .as_ref()
                        .and_then(|trust_bundle| trust_bundle.reenrollment.as_ref())
                        .is_some(),
                rotated_at: captured_at,
            }),
        }
    }
}

fn apply_control_command_to_admin_state(
    state: &mut BootstrapAdminState,
    command: &ExperimentControlCommand,
) {
    match command {
        ExperimentControlCommand::RevokePeer {
            peer_id,
            minimum_revocation_epoch,
            ..
        } => {
            state.quarantined_peers.insert(peer_id.clone());
            state.banned_peers.insert(peer_id.clone());
            state.minimum_revocation_epoch = Some(
                state
                    .minimum_revocation_epoch
                    .map(|epoch| epoch.max(*minimum_revocation_epoch))
                    .unwrap_or(*minimum_revocation_epoch),
            );
        }
        ExperimentControlCommand::QuarantinePeer { peer_id, .. } => {
            state.quarantined_peers.insert(peer_id.clone());
        }
        ExperimentControlCommand::PardonPeer { peer_id, .. } => {
            state.quarantined_peers.remove(peer_id);
            state.banned_peers.remove(peer_id);
        }
        _ => {}
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptQuery {
    pub study_id: Option<StudyId>,
    pub experiment_id: Option<ExperimentId>,
    pub revision_id: Option<RevisionId>,
    pub peer_id: Option<PeerId>,
}

impl ReceiptQuery {
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
pub struct HeadQuery {
    pub study_id: Option<StudyId>,
    pub experiment_id: Option<ExperimentId>,
    pub revision_id: Option<RevisionId>,
    pub head_id: Option<HeadId>,
}

impl HeadQuery {
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
pub struct ReducerLoadQuery {
    pub study_id: Option<StudyId>,
    pub experiment_id: Option<ExperimentId>,
    pub peer_id: Option<PeerId>,
}

impl ReducerLoadQuery {
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AuthPolicyRollout {
    pub minimum_revocation_epoch: Option<RevocationEpoch>,
    pub directory_entries: Option<Vec<burn_p2p::ExperimentDirectoryEntry>>,
    pub trusted_issuers: Option<Vec<TrustedIssuer>>,
    pub reenrollment: Option<ReenrollmentStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedIssuerStatus {
    pub issuer_peer_id: PeerId,
    pub issuer_public_key_hex: String,
    pub active_for_new_certificates: bool,
    pub accepted_for_admission: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReenrollmentStatus {
    pub reason: String,
    pub rotated_at: Option<DateTime<Utc>>,
    pub legacy_issuer_peer_ids: BTreeSet<PeerId>,
    pub login_path: String,
    pub enroll_path: String,
    pub trust_bundle_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustBundleExport {
    pub network_id: NetworkId,
    pub project_family_id: ProjectFamilyId,
    pub required_client_release_hash: ContentId,
    pub minimum_revocation_epoch: RevocationEpoch,
    pub active_issuer_peer_id: PeerId,
    pub issuers: Vec<TrustedIssuerStatus>,
    pub reenrollment: Option<ReenrollmentStatus>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AdminAction {
    Control(ExperimentControlCommand),
    BanPeer {
        peer_id: PeerId,
        reason: String,
    },
    ExportDiagnostics,
    ExportDiagnosticsBundle,
    ExportHeads(HeadQuery),
    ExportReceipts(ReceiptQuery),
    ExportReducerLoad(ReducerLoadQuery),
    ExportTrustBundle,
    RolloutAuthPolicy(AuthPolicyRollout),
    RetireTrustedIssuers {
        issuer_peer_ids: BTreeSet<PeerId>,
    },
    RotateAuthorityMaterial {
        issuer_key_id: Option<String>,
        retain_previous_issuer: bool,
        require_reenrollment: bool,
        reenrollment_reason: Option<String>,
    },
}

impl AdminAction {
    pub fn capability(&self) -> AdminCapability {
        match self {
            Self::Control(_) => AdminCapability::Control,
            Self::BanPeer { .. } => AdminCapability::BanPeer,
            Self::ExportDiagnostics => AdminCapability::ExportDiagnostics,
            Self::ExportDiagnosticsBundle => AdminCapability::ExportDiagnosticsBundle,
            Self::ExportHeads(_) => AdminCapability::ExportHeads,
            Self::ExportReceipts(_) => AdminCapability::ExportReceipts,
            Self::ExportReducerLoad(_) => AdminCapability::ExportReducerLoad,
            Self::ExportTrustBundle => AdminCapability::ExportTrustBundle,
            Self::RolloutAuthPolicy(_) => AdminCapability::RolloutAuthPolicy,
            Self::RetireTrustedIssuers { .. } => AdminCapability::RetireTrustedIssuers,
            Self::RotateAuthorityMaterial { .. } => AdminCapability::RotateAuthorityMaterial,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AdminResult {
    Control(ControlCertificate<ExperimentControlEnvelope>),
    PeerBanned {
        peer_id: PeerId,
        reason: String,
    },
    Diagnostics(BootstrapDiagnostics),
    DiagnosticsBundle(Box<BootstrapDiagnosticsBundle>),
    Heads(Vec<HeadDescriptor>),
    Receipts(Vec<ContributionReceipt>),
    ReducerLoad(Vec<ReducerLoadAnnouncement>),
    TrustBundle(Option<TrustBundleExport>),
    AuthPolicyRolledOut {
        minimum_revocation_epoch: Option<RevocationEpoch>,
        directory_entries: usize,
        trusted_issuers: usize,
        reenrollment_required: bool,
    },
    TrustedIssuersRetired {
        retired_issuers: usize,
        remaining_issuers: usize,
        reenrollment_required: bool,
    },
    AuthorityMaterialRotated {
        issuer_key_id: String,
        issuer_peer_id: PeerId,
        issuer_public_key_hex: String,
        trusted_issuers: usize,
        reenrollment_required: bool,
        rotated_at: DateTime<Utc>,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BootstrapAdminState {
    pub head_descriptors: Vec<HeadDescriptor>,
    pub peer_store: PeerStore,
    pub contribution_receipts: Vec<ContributionReceipt>,
    pub merge_certificates: Vec<MergeCertificate>,
    pub reducer_load_announcements: Vec<ReducerLoadAnnouncement>,
    pub in_flight_transfers: Vec<ArtifactTransferState>,
    pub admitted_peers: BTreeSet<PeerId>,
    pub peer_admission_reports: BTreeMap<PeerId, PeerAdmissionReport>,
    pub rejected_peers: BTreeMap<PeerId, String>,
    pub peer_reputation: BTreeMap<PeerId, ReputationState>,
    pub quarantined_peers: BTreeSet<PeerId>,
    pub banned_peers: BTreeSet<PeerId>,
    pub minimum_revocation_epoch: Option<RevocationEpoch>,
    pub trust_bundle: Option<TrustBundleExport>,
    pub last_error: Option<String>,
    pub node_state: NodeRuntimeState,
    pub slot_states: Vec<SlotRuntimeState>,
    pub runtime_snapshot: Option<NodeTelemetrySnapshot>,
}

impl BootstrapAdminState {
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

    pub fn export_receipts(&self, query: &ReceiptQuery) -> Vec<ContributionReceipt> {
        self.contribution_receipts
            .iter()
            .filter(|receipt| query.matches(receipt))
            .cloned()
            .collect()
    }

    pub fn export_heads(&self, query: &HeadQuery) -> Vec<HeadDescriptor> {
        self.head_descriptors
            .iter()
            .filter(|head| query.matches(head))
            .cloned()
            .collect()
    }

    pub fn export_reducer_load(&self, query: &ReducerLoadQuery) -> Vec<ReducerLoadAnnouncement> {
        self.reducer_load_announcements
            .iter()
            .filter(|announcement| query.matches(announcement))
            .cloned()
            .collect()
    }

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
pub struct BootstrapPeerDiagnostic {
    pub peer_id: PeerId,
    pub connected: bool,
    pub observed_at: Option<DateTime<Utc>>,
    pub trust_level: Option<PeerTrustLevel>,
    pub rejection_reason: Option<String>,
    pub reputation_score: Option<f64>,
    pub reputation_decision: Option<ReputationDecision>,
    pub quarantined: bool,
    pub banned: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BootstrapDiagnostics {
    pub network_id: NetworkId,
    pub preset: BootstrapPreset,
    pub services: BTreeSet<BootstrapService>,
    pub roles: PeerRoleSet,
    pub swarm: SwarmStats,
    pub pinned_heads: BTreeSet<HeadId>,
    pub pinned_artifacts: BTreeSet<ArtifactId>,
    pub accepted_receipts: u64,
    pub certified_merges: u64,
    pub in_flight_transfers: Vec<ArtifactTransferState>,
    pub admitted_peers: BTreeSet<PeerId>,
    pub peer_diagnostics: Vec<BootstrapPeerDiagnostic>,
    pub rejected_peers: BTreeMap<PeerId, String>,
    pub quarantined_peers: BTreeSet<PeerId>,
    pub banned_peers: BTreeSet<PeerId>,
    pub minimum_revocation_epoch: Option<burn_p2p::RevocationEpoch>,
    pub last_error: Option<String>,
    pub node_state: NodeRuntimeState,
    pub slot_states: Vec<SlotRuntimeState>,
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BootstrapDiagnosticsBundle {
    pub plan: BootstrapPlan,
    pub diagnostics: BootstrapDiagnostics,
    pub runtime_snapshot: Option<NodeTelemetrySnapshot>,
    pub heads: Vec<HeadDescriptor>,
    pub contribution_receipts: Vec<ContributionReceipt>,
    pub merge_certificates: Vec<MergeCertificate>,
    pub reducer_load_announcements: Vec<ReducerLoadAnnouncement>,
    pub trust_bundle: Option<TrustBundleExport>,
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveExperiment {
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
}

impl ActiveExperiment {
    pub fn handle(&self, network: &burn_p2p::MainnetHandle) -> ExperimentHandle {
        network.experiment(
            self.study_id.clone(),
            self.experiment_id.clone(),
            self.revision_id.clone(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapEmbeddedDaemonConfig {
    pub node: NodeConfig,
    pub active_experiment: ActiveExperiment,
    pub initialize_head_on_start: bool,
    pub restore_head_on_start: bool,
    pub validation_interval_millis: u64,
    pub training_interval_millis: Option<u64>,
}

impl Default for BootstrapEmbeddedDaemonConfig {
    fn default() -> Self {
        Self {
            node: NodeConfig {
                identity: IdentityConfig::Persistent,
                storage: Some(StorageConfig::new(".burn_p2p-bootstrap")),
                dataset: None,
                auth: None,
                network_manifest: None,
                client_release_manifest: None,
                selected_workload_id: None,
                bootstrap_peers: Vec::new(),
                listen_addresses: Vec::new(),
            },
            active_experiment: ActiveExperiment {
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("experiment"),
                revision_id: RevisionId::new("revision"),
            },
            initialize_head_on_start: true,
            restore_head_on_start: true,
            validation_interval_millis: 250,
            training_interval_millis: None,
        }
    }
}

pub struct BootstrapEmbeddedDaemon {
    plan: BootstrapPlan,
    admin_state: Arc<Mutex<BootstrapAdminState>>,
    telemetry: TelemetryHandle,
    control: ControlHandle,
    shutdown_requested: Arc<AtomicBool>,
    worker: Option<JoinHandle<anyhow::Result<()>>>,
}

impl std::fmt::Debug for BootstrapEmbeddedDaemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BootstrapEmbeddedDaemon")
            .field("network_id", &self.plan.network_id())
            .finish_non_exhaustive()
    }
}

impl BootstrapEmbeddedDaemon {
    pub fn plan(&self) -> &BootstrapPlan {
        &self.plan
    }

    pub fn telemetry(&self) -> TelemetryHandle {
        self.telemetry.clone()
    }

    pub fn control_handle(&self) -> ControlHandle {
        self.control.clone()
    }

    pub fn admin_state(&self) -> Arc<Mutex<BootstrapAdminState>> {
        Arc::clone(&self.admin_state)
    }

    pub fn diagnostics(&self, remaining_work_units: Option<u64>) -> BootstrapDiagnostics {
        self.admin_state
            .lock()
            .expect("bootstrap embedded state should not be poisoned")
            .diagnostics(&self.plan, Utc::now(), remaining_work_units)
    }

    pub fn diagnostics_bundle(
        &self,
        remaining_work_units: Option<u64>,
    ) -> BootstrapDiagnosticsBundle {
        self.admin_state
            .lock()
            .expect("bootstrap embedded state should not be poisoned")
            .diagnostics_bundle(&self.plan, Utc::now(), remaining_work_units)
    }

    pub fn shutdown(&self) -> anyhow::Result<()> {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        let _ = self.control.shutdown();
        Ok(())
    }

    pub fn await_termination(self) -> anyhow::Result<()> {
        let BootstrapEmbeddedDaemon {
            plan: _plan,
            admin_state,
            telemetry,
            control,
            shutdown_requested,
            worker,
        } = self;
        drop(control);
        drop(telemetry);
        drop(admin_state);
        drop(shutdown_requested);

        if let Some(worker) = worker {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("bootstrap embedded daemon panicked"))??;
        }
        Ok(())
    }
}

impl BootstrapPlan {
    pub fn spawn_embedded_daemon<P, B>(
        &self,
        project: P,
        config: BootstrapEmbeddedDaemonConfig,
    ) -> anyhow::Result<BootstrapEmbeddedDaemon>
    where
        P: RuntimeProject<B> + Send + 'static,
        B: ProjectBackend + 'static,
    {
        let mut builder = NodeBuilder::new(project)
            .with_mainnet(self.genesis.clone())
            .with_roles(self.roles.clone());
        builder = apply_node_config(builder, &config.node);
        let running = builder.spawn()?;
        let telemetry = running.telemetry();
        let control = running.control_handle();
        let admin_state = Arc::new(Mutex::new(BootstrapAdminState::default()));
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let plan = self.clone();
        let admin_state_thread = Arc::clone(&admin_state);
        let shutdown_requested_thread = Arc::clone(&shutdown_requested);

        let worker = thread::Builder::new()
            .name("burn-p2p-bootstrap-embedded".into())
            .spawn(move || {
                embedded_daemon_loop::<P, B>(
                    plan,
                    config,
                    running,
                    admin_state_thread,
                    shutdown_requested_thread,
                )
            })
            .map_err(|error| anyhow::anyhow!("failed to spawn bootstrap worker: {error}"))?;

        Ok(BootstrapEmbeddedDaemon {
            plan: self.clone(),
            admin_state,
            telemetry,
            control,
            shutdown_requested,
            worker: Some(worker),
        })
    }
}

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

pub fn render_dashboard_html(network_id: &NetworkId) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>burn_p2p bootstrap {network_id}</title>
  <style>
    :root {{
      --bg: #f6f5ef;
      --panel: #fffdf7;
      --ink: #1d241f;
      --accent: #0d6b4d;
      --muted: #5f665f;
      --line: #d6d2c4;
      --danger: #8b2e24;
      font-family: "IBM Plex Sans", "Avenir Next", sans-serif;
    }}
    body {{ margin: 0; background: linear-gradient(180deg, #ede8da, #f8f6ef); color: var(--ink); }}
    main {{ max-width: 1040px; margin: 0 auto; padding: 24px; }}
    h1 {{ margin: 0 0 8px; font-size: 2rem; }}
    .grid {{ display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }}
    .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 14px; padding: 16px; box-shadow: 0 8px 24px rgba(29,36,31,0.06); }}
    .metric {{ font-size: 1.6rem; font-weight: 700; color: var(--accent); }}
    .muted {{ color: var(--muted); }}
    pre {{ white-space: pre-wrap; word-break: break-word; font-size: 0.85rem; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid var(--line); font-size: 0.9rem; }}
    .danger {{ color: var(--danger); }}
  </style>
</head>
<body>
  <main>
    <h1>burn_p2p bootstrap</h1>
    <p class="muted">Network <strong>{network_id}</strong>. Live diagnostics stream over <code>/events</code>; bundle export lives at <code>/diagnostics/bundle</code>; operator history is available from <code>/heads</code>, <code>/receipts</code>, and <code>/reducers/load</code>; trust rollout and re-enrollment status are exposed via <code>/trust</code> and <code>/reenrollment</code>.</p>
    <section class="grid">
      <article class="panel"><div class="muted">Connected peers</div><div id="connected" class="metric">0</div></article>
      <article class="panel"><div class="muted">Observed peers</div><div id="observed" class="metric">0</div></article>
      <article class="panel"><div class="muted">Admitted peers</div><div id="admitted" class="metric">0</div></article>
      <article class="panel"><div class="muted">Rejected peers</div><div id="rejected" class="metric">0</div></article>
      <article class="panel"><div class="muted">Accepted receipts</div><div id="receipts" class="metric">0</div></article>
      <article class="panel"><div class="muted">Certified merges</div><div id="merges" class="metric">0</div></article>
    </section>
    <section class="panel" style="margin-top:16px;">
      <h2>Services</h2>
      <div id="services" class="muted">loading...</div>
    </section>
    <section class="panel" style="margin-top:16px;">
      <h2>Peers / policy</h2>
      <table>
        <tbody>
          <tr><th>In-flight transfers</th><td id="transfers">0</td></tr>
          <tr><th>Quarantined peers</th><td id="quarantined">0</td></tr>
          <tr><th>Banned peers</th><td id="banned">0</td></tr>
          <tr><th>Min revocation epoch</th><td id="revocation">n/a</td></tr>
          <tr><th>ETA range</th><td id="eta">n/a</td></tr>
          <tr><th>Last error</th><td id="error">none</td></tr>
        </tbody>
      </table>
    </section>
    <section class="panel" style="margin-top:16px;">
      <h2>Raw diagnostics</h2>
      <pre id="raw">waiting for snapshot...</pre>
    </section>
  </main>
  <script>
    const raw = document.getElementById("raw");
    const update = (payload) => {{
      document.getElementById("connected").textContent = payload.swarm.connected_peers;
      document.getElementById("observed").textContent = payload.swarm.observed_peers.length;
      document.getElementById("admitted").textContent = payload.admitted_peers.length;
      document.getElementById("rejected").textContent = Object.keys(payload.rejected_peers).length;
      document.getElementById("receipts").textContent = payload.accepted_receipts;
      document.getElementById("merges").textContent = payload.certified_merges;
      document.getElementById("services").textContent = payload.services.join(", ");
      document.getElementById("transfers").textContent = payload.in_flight_transfers.length;
      document.getElementById("quarantined").textContent = payload.quarantined_peers.length;
      document.getElementById("banned").textContent = payload.banned_peers.length;
      document.getElementById("revocation").textContent = payload.minimum_revocation_epoch == null ? "n/a" : payload.minimum_revocation_epoch;
      const lower = payload.swarm.network_estimate.eta_lower_seconds;
      const upper = payload.swarm.network_estimate.eta_upper_seconds;
      document.getElementById("eta").textContent = lower == null && upper == null ? "n/a" : `${{lower ?? "?"}}s - ${{upper ?? "?"}}s`;
      document.getElementById("error").textContent = payload.last_error ?? "none";
      raw.textContent = JSON.stringify(payload, null, 2);
    }};

    fetch("/status").then((response) => response.json()).then(update).catch((error) => {{
      raw.textContent = `failed to load /status: ${{error}}`;
    }});

    const events = new EventSource("/events");
    events.onmessage = (event) => {{
      update(JSON.parse(event.data));
    }};
    events.onerror = () => {{
      document.body.classList.add("danger");
    }};
  </script>
</body>
</html>"#
    )
}

fn apply_node_config<P>(builder: NodeBuilder<P>, config: &NodeConfig) -> NodeBuilder<P> {
    let builder = builder.with_identity(config.identity.clone());
    let builder = match config.storage.clone() {
        Some(storage) => builder.with_storage(storage),
        None => builder,
    };
    let builder = match config.dataset.clone() {
        Some(dataset) => builder.with_dataset(dataset),
        None => builder,
    };
    builder
        .with_bootstrap_peers(config.bootstrap_peers.clone())
        .with_listen_addresses(config.listen_addresses.clone())
}

fn embedded_daemon_loop<P, B>(
    plan: BootstrapPlan,
    config: BootstrapEmbeddedDaemonConfig,
    mut running: RunningNode<P>,
    admin_state: Arc<Mutex<BootstrapAdminState>>,
    shutdown_requested: Arc<AtomicBool>,
) -> anyhow::Result<()>
where
    P: RuntimeProject<B>,
    B: ProjectBackend,
{
    let experiment = config.active_experiment.handle(running.mainnet());
    let validation_interval = Duration::from_millis(config.validation_interval_millis.max(25));
    let training_interval = config
        .training_interval_millis
        .map(|millis| Duration::from_millis(millis.max(25)));
    let mut last_validation = Instant::now()
        .checked_sub(validation_interval)
        .unwrap_or_else(Instant::now);
    let mut last_training = training_interval.map(|interval| {
        Instant::now()
            .checked_sub(interval)
            .unwrap_or_else(Instant::now)
    });

    wait_for_runtime_ready(&running.telemetry(), Duration::from_secs(5))?;

    if config.restore_head_on_start && running.restore_experiment_head(&experiment)?.is_none() {
        if config.initialize_head_on_start {
            let _ = running.initialize_local_head::<B>(&experiment)?;
        }
    } else if config.initialize_head_on_start
        && load_operator_history(running.config().storage.as_ref())?
            .0
            .is_empty()
    {
        let _ = running.initialize_local_head::<B>(&experiment)?;
    }

    loop {
        if shutdown_requested.load(Ordering::SeqCst) {
            break;
        }

        if let Some(interval) = training_interval
            && last_training
                .as_ref()
                .is_some_and(|instant| instant.elapsed() >= interval)
        {
            let _ = running.train_window_once::<B>(&experiment)?;
            last_training = Some(Instant::now());
        }

        if plan.supports_service(&BootstrapService::Validator)
            && last_validation.elapsed() >= validation_interval
        {
            let _ = running.validate_candidates_once::<B>(&experiment)?;
            last_validation = Instant::now();
        }

        {
            let snapshot = running.telemetry().snapshot();
            let mut state = admin_state
                .lock()
                .expect("bootstrap embedded state should not be poisoned");
            refresh_admin_state_from_runtime(
                &mut state,
                &snapshot,
                running.config().storage.as_ref(),
            )?;
        }

        thread::sleep(Duration::from_millis(50));
    }

    running.shutdown()?;
    let _ = running.await_termination()?;
    Ok(())
}

fn refresh_admin_state_from_runtime(
    state: &mut BootstrapAdminState,
    telemetry: &NodeTelemetrySnapshot,
    storage: Option<&StorageConfig>,
) -> anyhow::Result<()> {
    let observed_at = telemetry.updated_at;
    state.node_state = telemetry.node_state.clone();
    state.slot_states = telemetry.slot_states.clone();
    state.in_flight_transfers = telemetry.in_flight_transfers.values().cloned().collect();
    state.admitted_peers = telemetry.admitted_peers.keys().cloned().collect();
    state.peer_admission_reports = telemetry.admitted_peers.clone();
    state.rejected_peers = telemetry.rejected_peers.clone();
    state.peer_reputation = telemetry.peer_reputation.clone();
    state.minimum_revocation_epoch = telemetry.minimum_revocation_epoch;
    state.last_error = telemetry.last_error.clone();
    state.runtime_snapshot = Some(telemetry.clone());
    state.reducer_load_announcements = telemetry.control_plane.reducer_load_announcements.clone();
    if let Some(local_peer_id) = telemetry.local_peer_id.clone() {
        state
            .peer_store
            .mark_connection(local_peer_id, true, observed_at);
    }

    if let Some(snapshot_peer_id) = telemetry.last_snapshot_peer_id.clone() {
        state
            .peer_store
            .mark_connection(snapshot_peer_id, true, observed_at);
    }

    for event in &telemetry.recent_events {
        if let Some(peer_id) = observed_peer_id_from_event(event) {
            state.peer_store.mark_connection(peer_id, true, observed_at);
        }
    }

    if let Some(storage) = storage {
        let (receipts, merges, heads) = load_operator_history(Some(storage))?;
        state.head_descriptors = heads;
        state.contribution_receipts = receipts;
        state.merge_certificates = merges;
    }

    Ok(())
}

fn observed_peer_id_from_event(event: &LiveControlPlaneEvent) -> Option<PeerId> {
    match event {
        LiveControlPlaneEvent::ConnectionEstablished { peer_id }
        | LiveControlPlaneEvent::SnapshotRequested { peer_id }
        | LiveControlPlaneEvent::SnapshotReceived { peer_id, .. }
        | LiveControlPlaneEvent::ArtifactManifestRequested { peer_id, .. }
        | LiveControlPlaneEvent::ArtifactManifestReceived { peer_id, .. }
        | LiveControlPlaneEvent::ArtifactChunkRequested { peer_id, .. }
        | LiveControlPlaneEvent::ArtifactChunkReceived { peer_id, .. }
        | LiveControlPlaneEvent::SnapshotResponseSent { peer_id }
        | LiveControlPlaneEvent::PubsubMessage { peer_id, .. }
        | LiveControlPlaneEvent::PeerIdentified { peer_id, .. }
        | LiveControlPlaneEvent::RequestFailure { peer_id, .. }
        | LiveControlPlaneEvent::InboundFailure { peer_id, .. }
        | LiveControlPlaneEvent::ResponseSendFailure { peer_id, .. } => {
            Some(PeerId::new(peer_id.clone()))
        }
        LiveControlPlaneEvent::PeersDiscovered { peers }
        | LiveControlPlaneEvent::PeersExpired { peers } => peers
            .first()
            .map(|(peer_id, _)| PeerId::new(peer_id.clone())),
        LiveControlPlaneEvent::OutgoingConnectionError {
            peer_id: Some(peer_id),
            ..
        } => Some(PeerId::new(peer_id.clone())),
        LiveControlPlaneEvent::NewListenAddr { .. }
        | LiveControlPlaneEvent::TopicSubscribed { .. }
        | LiveControlPlaneEvent::IncomingConnectionError { .. }
        | LiveControlPlaneEvent::OutgoingConnectionError { peer_id: None, .. }
        | LiveControlPlaneEvent::Other { .. } => None,
    }
}

fn load_operator_history(
    storage: Option<&StorageConfig>,
) -> anyhow::Result<(
    Vec<ContributionReceipt>,
    Vec<MergeCertificate>,
    Vec<HeadDescriptor>,
)> {
    let Some(storage) = storage else {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    };
    let receipts_dir = storage.receipts_dir();
    let mut receipt_paths = Vec::<PathBuf>::new();
    let mut merge_paths = Vec::<PathBuf>::new();
    if receipts_dir.exists() {
        for entry in fs::read_dir(&receipts_dir).map_err(|error| {
            anyhow::anyhow!("failed to read {}: {error}", receipts_dir.display())
        })? {
            let path = entry
                .map_err(|error| anyhow::anyhow!("failed to read receipt entry: {error}"))?
                .path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("merge-"))
            {
                merge_paths.push(path);
            } else {
                receipt_paths.push(path);
            }
        }
    }

    let heads_dir = storage.heads_dir();
    let mut head_paths = Vec::<PathBuf>::new();
    if heads_dir.exists() {
        for entry in fs::read_dir(&heads_dir)
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", heads_dir.display()))?
        {
            let path = entry
                .map_err(|error| anyhow::anyhow!("failed to read head entry: {error}"))?
                .path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
                head_paths.push(path);
            }
        }
    }

    receipt_paths.sort();
    merge_paths.sort();
    head_paths.sort();

    let mut receipts = receipt_paths
        .into_iter()
        .map(load_json_file::<ContributionReceipt>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    receipts.sort_by_key(|receipt| receipt.accepted_at);

    let mut merges = merge_paths
        .into_iter()
        .map(load_json_file::<MergeCertificate>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    merges.sort_by_key(|merge| merge.issued_at);

    let mut heads = head_paths
        .into_iter()
        .map(load_json_file::<HeadDescriptor>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    heads.sort_by_key(|head| head.created_at);

    Ok((receipts, merges, heads))
}

fn load_json_file<T: for<'de> Deserialize<'de>>(path: PathBuf) -> anyhow::Result<T> {
    let bytes = fs::read(&path)
        .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| anyhow::anyhow!("failed to decode {}: {error}", path.display()))
}

fn wait_for_runtime_ready(telemetry: &TelemetryHandle, timeout: Duration) -> anyhow::Result<()> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        let snapshot = telemetry.snapshot();
        if snapshot.local_peer_id.is_some() && !snapshot.listen_addresses.is_empty() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }

    Err(anyhow::anyhow!(
        "runtime did not become ready within {}ms",
        timeout.as_millis()
    ))
}

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("swarm configuration error: {0}")]
    Swarm(#[from] SwarmError),
    #[error("schema error: {0}")]
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("authority-capable presets require authority and validator policy")]
    MissingAuthorityPolicy,
    #[error("this bootstrap plan does not include authority service")]
    AuthorityServiceRequired,
    #[error("admin action `{0:?}` is not enabled for this bootstrap plan")]
    UnsupportedAdminAction(AdminCapability),
    #[error("this admin action requires a signer")]
    MissingSigner,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        path::{Path, PathBuf},
        thread,
        time::{Duration, Instant},
    };

    use burn_p2p::{
        ArtifactBuildSpec, ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard,
        CapabilityEstimate, ChunkingScheme, ControlPlaneSnapshot, DatasetConfig,
        DatasetRegistration, DatasetSizing, EvalSplit, FsArtifactStore, HeadDescriptor,
        LiveControlPlaneEvent, MetricReport, MetricValue, NodeBuilder, NodeConfig,
        NodeTelemetrySnapshot, P2pProject, PatchOutcome, PatchSupport, ProjectBackend,
        ReducerLoadAnnouncement, RuntimePatch, RuntimeProject, RuntimeStatus, ShardFetchManifest,
        SlotAssignmentState, StorageConfig, TrainError, WindowCtx, WindowReport,
    };
    use burn_p2p_core::{
        ArtifactId, CapabilityCard, CapabilityCardId, CapabilityClass, ClientPlatform, ContentId,
        ContributionReceipt, ExperimentId, HeadId, MergeCertificate, PeerId, PersistenceClass,
        PrincipalId, RevisionId, StudyId, TelemetrySummary, WindowActivation, WindowId,
    };
    use burn_p2p_experiment::{ActivationTarget, ExperimentControlCommand};
    use burn_p2p_security::{
        MergeEvidenceRequirement, PeerAdmissionReport, PeerTrustLevel, ReleasePolicy,
        ReputationDecision, ReputationEngine, ReputationState, ValidatorPolicy,
    };
    use burn_p2p_swarm::PeerObservation;
    use chrono::Utc;
    use semver::{Version, VersionReq};
    use tempfile::TempDir;

    use super::{
        ActiveExperiment, AdminAction, AdminResult, BootstrapAdminState,
        BootstrapEmbeddedDaemonConfig, BootstrapPlan, BootstrapPreset, BootstrapService,
        BootstrapSpec, HeadQuery, ReceiptQuery, ReducerLoadQuery, render_dashboard_html,
        render_openmetrics,
    };

    fn genesis() -> burn_p2p_core::GenesisSpec {
        burn_p2p_core::GenesisSpec {
            network_id: burn_p2p_core::NetworkId::new("mainnet"),
            protocol_version: Version::new(0, 1, 0),
            display_name: "Mainnet".into(),
            created_at: Utc::now(),
            metadata: BTreeMap::new(),
        }
    }

    fn authority_plan() -> super::AuthorityPlan {
        let release_policy = ReleasePolicy::new(
            Version::new(0, 1, 0),
            vec![VersionReq::parse("^0.1").expect("version req")],
        )
        .expect("release policy");
        let validator_policy = ValidatorPolicy {
            release_policy: release_policy.clone(),
            evidence_requirement: MergeEvidenceRequirement::default(),
        };

        super::AuthorityPlan {
            release_policy,
            validator_policy,
        }
    }

    fn plan(preset: BootstrapPreset) -> BootstrapPlan {
        BootstrapSpec {
            preset,
            genesis: genesis(),
            platform: ClientPlatform::Native,
            bootstrap_addresses: vec![
                burn_p2p_swarm::SwarmAddress::new("/dns4/bootstrap.example.com/tcp/4001/ws")
                    .expect("addr"),
            ],
            listen_addresses: vec![
                burn_p2p_swarm::SwarmAddress::new("/ip4/0.0.0.0/udp/4001/quic-v1").expect("addr"),
            ],
            authority: Some(authority_plan()),
            archive: super::ArchivePlan {
                pinned_heads: BTreeSet::from([HeadId::new("head")]),
                pinned_artifacts: BTreeSet::from([ArtifactId::new("artifact")]),
                retain_contribution_receipts: true,
            },
            admin_api: super::AdminApiPlan::default(),
        }
        .plan()
        .expect("plan")
    }

    #[derive(Clone, Debug)]
    struct SyntheticBackend;

    impl ProjectBackend for SyntheticBackend {
        type Device = String;
    }

    #[derive(Clone, Debug)]
    struct SyntheticProject {
        dataset_root: PathBuf,
        learning_rate: f64,
        target_model: f64,
    }

    impl P2pProject<SyntheticBackend> for SyntheticProject {
        type Model = f64;
        type Batch = f64;
        type WindowStats = BTreeMap<String, MetricValue>;

        fn init_model(&self, _device: &String) -> Self::Model {
            0.0
        }

        fn benchmark(&self, _model: &Self::Model, _device: &String) -> CapabilityEstimate {
            CapabilityEstimate {
                preferred_backends: vec!["ndarray".into()],
                work_units_per_second: 64.0,
                target_window_seconds: 1,
            }
        }

        fn train_window(
            &self,
            ctx: &mut WindowCtx<String, Self::Model, Self::Batch>,
        ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
            let delta = ctx.batches.iter().copied().sum::<f64>() * self.learning_rate;
            ctx.model += delta;

            Ok(WindowReport {
                contribution: None,
                stats: BTreeMap::from([
                    ("delta".into(), MetricValue::Float(delta)),
                    ("model".into(), MetricValue::Float(ctx.model)),
                    (
                        "loss".into(),
                        MetricValue::Float((self.target_model - ctx.model).abs()),
                    ),
                ]),
                completed_at: Utc::now(),
            })
        }

        fn evaluate(&self, model: &Self::Model, _split: EvalSplit) -> MetricReport {
            MetricReport {
                metrics: BTreeMap::from([
                    (
                        "loss".into(),
                        MetricValue::Float((self.target_model - *model).abs()),
                    ),
                    ("model".into(), MetricValue::Float(*model)),
                ]),
                captured_at: Utc::now(),
            }
        }

        fn apply_patch(&mut self, patch: &RuntimePatch) -> PatchOutcome {
            if let Some(burn_p2p::PatchValue::Float(value)) = patch.values.get("learning_rate") {
                self.learning_rate = *value;
                PatchOutcome::Applied
            } else {
                PatchOutcome::Rejected("missing learning_rate".into())
            }
        }

        fn supported_patch_classes(&self) -> PatchSupport {
            PatchSupport {
                hot: true,
                warm: false,
                cold: false,
            }
        }
    }

    impl RuntimeProject<SyntheticBackend> for SyntheticProject {
        fn runtime_device(&self) -> String {
            "cpu".into()
        }

        fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
            Ok(DatasetRegistration {
                manifest: burn_p2p::DatasetManifest {
                    dataset_id: burn_p2p::DatasetId::new("bootstrap-dataset"),
                    source_uri: self.dataset_root.display().to_string(),
                    format: "microshards".into(),
                    manifest_hash: ContentId::new("bootstrap-manifest"),
                    metadata: BTreeMap::new(),
                },
                view: burn_p2p::DatasetView {
                    dataset_view_id: burn_p2p::DatasetViewId::new("bootstrap-dataset-view"),
                    dataset_id: burn_p2p::DatasetId::new("bootstrap-dataset"),
                    preprocessing_hash: ContentId::new("bootstrap-preprocess"),
                    tokenizer_hash: None,
                    manifest_hash: ContentId::new("bootstrap-manifest"),
                    metadata: BTreeMap::new(),
                },
                upstream: burn_p2p::UpstreamAdapter::Local {
                    root: self.dataset_root.display().to_string(),
                },
            })
        }

        fn microshard_plan(
            &self,
            registration: &DatasetRegistration,
        ) -> anyhow::Result<burn_p2p::MicroShardPlan> {
            Ok(
                burn_p2p::MicroShardPlanner::new(burn_p2p::MicroShardPlannerConfig {
                    target_microshard_bytes: 10,
                    min_microshards: 2,
                    max_microshards: 2,
                })?
                .plan(
                    &registration.view,
                    DatasetSizing {
                        total_examples: 2,
                        total_tokens: 2,
                        total_bytes: 20,
                    },
                )?,
            )
        }

        fn load_batches(
            &self,
            _lease: &AssignmentLease,
            cached_microshards: &[CachedMicroShard],
        ) -> anyhow::Result<Vec<Self::Batch>> {
            cached_microshards
                .iter()
                .map(|shard| {
                    let bytes = fs::read(&shard.path)?;
                    let text = String::from_utf8(bytes)?;
                    text.trim().parse::<f64>().map_err(anyhow::Error::from)
                })
                .collect()
        }

        fn load_model_artifact(
            &self,
            _model: Self::Model,
            descriptor: &ArtifactDescriptor,
            store: &FsArtifactStore,
            _device: &String,
        ) -> anyhow::Result<Self::Model> {
            Ok(serde_json::from_slice(
                &store.materialize_artifact_bytes(descriptor)?,
            )?)
        }

        fn materialize_model_artifact(
            &self,
            model: &Self::Model,
            artifact_kind: ArtifactKind,
            head_id: burn_p2p::HeadId,
            base_head_id: Option<burn_p2p::HeadId>,
            store: &FsArtifactStore,
        ) -> anyhow::Result<ArtifactDescriptor> {
            let mut spec = ArtifactBuildSpec::new(
                artifact_kind,
                burn_p2p::Precision::Fp32,
                ContentId::new("bootstrap-synthetic-schema"),
                "synthetic-json",
            )
            .with_head(head_id);
            if let Some(base_head_id) = base_head_id {
                spec = spec.with_base_head(base_head_id);
            }
            let bytes = serde_json::to_vec(model)?;
            Ok(store.store_artifact_reader(
                &spec,
                std::io::Cursor::new(bytes),
                ChunkingScheme::new(16)?,
            )?)
        }

        fn contribution_metrics(
            &self,
            report: &WindowReport<Self::WindowStats>,
        ) -> BTreeMap<String, MetricValue> {
            report.stats.clone()
        }

        fn merge_candidate_models(
            &self,
            base_model: &Self::Model,
            candidates: &[burn_p2p::MergeModelCandidate<'_, Self::Model>],
            policy: burn_p2p::MergePolicy,
        ) -> anyhow::Result<Option<Self::Model>> {
            if candidates.is_empty() {
                return Ok(None);
            }
            let (weighted_sum, total_weight) = candidates.iter().fold(
                (0.0_f64, 0.0_f64),
                |(weighted_sum, total_weight), candidate| {
                    let quality = match policy {
                        burn_p2p::MergePolicy::WeightedMean => 1.0,
                        burn_p2p::MergePolicy::NormClippedWeightedMean => {
                            candidate.quality_weight.clamp(0.0, 1.0)
                        }
                        burn_p2p::MergePolicy::TrimmedMean => candidate.quality_weight,
                        burn_p2p::MergePolicy::Ema | burn_p2p::MergePolicy::QualityWeightedEma => {
                            candidate.quality_weight
                        }
                        burn_p2p::MergePolicy::Custom(_) => candidate.quality_weight,
                    };
                    let weight = candidate.sample_weight * quality;
                    (
                        weighted_sum + (*candidate.model * weight),
                        total_weight + weight,
                    )
                },
            );
            if total_weight <= f64::EPSILON {
                return Ok(Some(*base_model));
            }
            Ok(Some(weighted_sum / total_weight))
        }
    }

    fn create_dataset(root: &Path) {
        fs::create_dir_all(root).expect("create dataset root");
        let project = SyntheticProject {
            dataset_root: root.to_path_buf(),
            learning_rate: 1.0,
            target_model: 10.0,
        };
        let registration = project
            .dataset_registration()
            .expect("dataset registration");
        let plan = project
            .microshard_plan(&registration)
            .expect("microshard plan");
        let manifest = ShardFetchManifest::from_microshards(
            &plan.dataset_view,
            &plan.microshards,
            |ordinal| match ordinal {
                0 => b"3.5".to_vec(),
                _ => b"6.5".to_vec(),
            },
        );
        fs::write(
            root.join("fetch-manifest.json"),
            serde_json::to_vec_pretty(&manifest).expect("manifest"),
        )
        .expect("write manifest");
        for entry in &manifest.entries {
            let bytes = match entry.ordinal {
                0 => b"3.5".to_vec(),
                _ => b"6.5".to_vec(),
            };
            fs::write(root.join(&entry.locator), bytes).expect("write shard");
        }
    }

    fn wait_for(timeout: Duration, predicate: impl Fn() -> bool, message: &str) {
        let started = Instant::now();
        while started.elapsed() < timeout {
            if predicate() {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("{message}");
    }

    #[test]
    fn preset_derives_expected_services_and_roles() {
        let plan = plan(BootstrapPreset::AllInOne);

        assert!(plan.supports_service(&BootstrapService::Relay));
        assert!(plan.supports_service(&BootstrapService::Authority));
        assert!(plan.supports_service(&BootstrapService::Archive));
        assert!(plan.roles.contains(&burn_p2p_core::PeerRole::Bootstrap));
        assert!(plan.roles.contains(&burn_p2p_core::PeerRole::Authority));
    }

    #[test]
    fn authority_presets_require_authority_policy() {
        let error = BootstrapSpec {
            preset: BootstrapPreset::AuthorityValidator,
            genesis: genesis(),
            platform: ClientPlatform::Native,
            bootstrap_addresses: vec![],
            listen_addresses: vec![],
            authority: None,
            archive: super::ArchivePlan::default(),
            admin_api: super::AdminApiPlan::default(),
        }
        .plan()
        .expect_err("missing policy");

        assert!(matches!(
            error,
            super::BootstrapError::MissingAuthorityPolicy
        ));
    }

    #[test]
    fn control_action_issues_signed_control_certificate() {
        let plan = plan(BootstrapPreset::AuthorityValidator);
        let mut state = BootstrapAdminState::default();

        let result = plan
            .execute_admin_action(
                AdminAction::Control(ExperimentControlCommand::PauseExperiment {
                    study_id: StudyId::new("study"),
                    experiment_id: ExperimentId::new("exp"),
                    reason: Some("maintenance".into()),
                    target: ActivationTarget {
                        activation: WindowActivation {
                            activation_window: WindowId(5),
                            grace_windows: 1,
                        },
                        required_client_capabilities: BTreeSet::from(["cuda".into()]),
                    },
                }),
                &mut state,
                Some(burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: Utc::now(),
                    signature_hex: "abcd".into(),
                }),
                Utc::now(),
                None,
            )
            .expect("result");

        match result {
            AdminResult::Control(cert) => {
                assert_eq!(cert.network_id.as_str(), "mainnet");
                assert_eq!(cert.activation.activation_window, WindowId(5));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn diagnostics_export_composes_peer_store_and_archive_state() {
        let plan = plan(BootstrapPreset::BootstrapArchive);
        let now = Utc::now();
        let mut state = BootstrapAdminState::default();
        let peer_id = PeerId::new("peer");

        state.peer_store.upsert(
            PeerObservation::new(peer_id.clone(), now)
                .with_capability_card(CapabilityCard {
                    card_id: CapabilityCardId::new("card"),
                    peer_id: peer_id.clone(),
                    platform: ClientPlatform::Native,
                    roles: burn_p2p_core::PeerRoleSet::default_trainer(),
                    preferred_backends: vec!["cuda".into()],
                    recommended_classes: BTreeSet::from([CapabilityClass::TrainerGpu]),
                    device_memory_bytes: Some(8 * 1024 * 1024 * 1024),
                    system_memory_bytes: 32 * 1024 * 1024 * 1024,
                    disk_bytes: 256 * 1024 * 1024 * 1024,
                    upload_mbps: 500.0,
                    download_mbps: 500.0,
                    persistence: PersistenceClass::Durable,
                    work_units_per_second: 20.0,
                    attestation_level: burn_p2p_core::AttestationLevel::Manifest,
                    benchmark_hash: None,
                    reported_at: now,
                })
                .with_telemetry(TelemetrySummary {
                    network_id: burn_p2p_core::NetworkId::new("mainnet"),
                    study_id: StudyId::new("study"),
                    experiment_id: ExperimentId::new("exp"),
                    revision_id: RevisionId::new("rev"),
                    window_id: WindowId(3),
                    active_peers: 1,
                    accepted_contributions: 2,
                    throughput_work_units_per_second: 18.0,
                    network: burn_p2p_core::NetworkEstimate {
                        connected_peers: 1,
                        observed_peers: 1,
                        estimated_network_size: 1.0,
                        estimated_total_vram_bytes: None,
                        estimated_total_flops: None,
                        eta_lower_seconds: None,
                        eta_upper_seconds: None,
                    },
                    metrics: BTreeMap::from([("estimated_flops".into(), MetricValue::Float(42.0))]),
                    captured_at: now,
                }),
        );
        state.quarantined_peers.insert(peer_id.clone());
        state.banned_peers.insert(PeerId::new("banned"));

        let result = plan
            .execute_admin_action(
                AdminAction::ExportDiagnostics,
                &mut state,
                None,
                now,
                Some(360),
            )
            .expect("diagnostics");

        match result {
            AdminResult::Diagnostics(diagnostics) => {
                assert_eq!(diagnostics.swarm.connected_peers, 1);
                assert_eq!(diagnostics.pinned_heads.len(), 1);
                assert!(diagnostics.quarantined_peers.contains(&peer_id));
                assert!(diagnostics.admitted_peers.is_empty());
                assert!(diagnostics.rejected_peers.is_empty());
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn diagnostics_export_includes_peer_trust_and_rejection_details() {
        let plan = plan(BootstrapPreset::BootstrapArchive);
        let now = Utc::now();
        let admitted_peer = PeerId::new("admitted");
        let rejected_peer = PeerId::new("rejected");
        let mut state = BootstrapAdminState::default();

        state
            .peer_store
            .mark_connection(admitted_peer.clone(), true, now);
        state
            .peer_store
            .mark_connection(rejected_peer.clone(), false, now);
        state.admitted_peers.insert(admitted_peer.clone());
        state.peer_admission_reports.insert(
            admitted_peer.clone(),
            PeerAdmissionReport {
                peer_id: admitted_peer.clone(),
                principal_id: PrincipalId::new("principal-admitted"),
                requested_scopes: BTreeSet::new(),
                trust_level: PeerTrustLevel::PolicyCompliant,
                issuer_peer_id: PeerId::new("issuer"),
                findings: Vec::new(),
                verified_at: now,
            },
        );
        state
            .rejected_peers
            .insert(rejected_peer.clone(), "missing auth envelope".into());

        let mut admitted_reputation = ReputationState::new(admitted_peer.clone(), now);
        admitted_reputation.score = 1.5;
        state
            .peer_reputation
            .insert(admitted_peer.clone(), admitted_reputation);
        let mut rejected_reputation = ReputationState::new(rejected_peer.clone(), now);
        rejected_reputation.score = ReputationEngine::default().policy.ban_threshold - 1.0;
        state
            .peer_reputation
            .insert(rejected_peer.clone(), rejected_reputation);

        let diagnostics = state.diagnostics(&plan, now, None);
        let admitted = diagnostics
            .peer_diagnostics
            .iter()
            .find(|entry| entry.peer_id == admitted_peer)
            .expect("admitted peer diagnostic");
        assert!(admitted.connected);
        assert_eq!(admitted.trust_level, Some(PeerTrustLevel::PolicyCompliant));
        assert_eq!(
            admitted.reputation_decision,
            Some(ReputationDecision::Allow)
        );
        assert_eq!(admitted.reputation_score, Some(1.5));

        let rejected = diagnostics
            .peer_diagnostics
            .iter()
            .find(|entry| entry.peer_id == rejected_peer)
            .expect("rejected peer diagnostic");
        assert!(!rejected.connected);
        assert_eq!(
            rejected.rejection_reason.as_deref(),
            Some("missing auth envelope")
        );
        assert_eq!(rejected.reputation_decision, Some(ReputationDecision::Ban));
    }

    #[test]
    fn diagnostics_bundle_export_includes_runtime_snapshot_and_persisted_state() {
        let plan = plan(BootstrapPreset::AuthorityValidator);
        let now = Utc::now();
        let peer_id = PeerId::new("peer");
        let receipt = ContributionReceipt {
            receipt_id: burn_p2p_core::ContributionReceiptId::new("receipt"),
            peer_id: peer_id.clone(),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base"),
            artifact_id: ArtifactId::new("artifact"),
            accepted_at: now,
            accepted_weight: 1.0,
            metrics: BTreeMap::new(),
            merge_cert_id: Some(burn_p2p_core::MergeCertId::new("merge-cert")),
        };
        let merge = MergeCertificate {
            merge_cert_id: burn_p2p_core::MergeCertId::new("merge-cert"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base"),
            merged_head_id: HeadId::new("head"),
            merged_artifact_id: ArtifactId::new("artifact"),
            policy: burn_p2p_core::MergePolicy::WeightedMean,
            issued_at: now,
            validator: PeerId::new("validator"),
            contribution_receipts: vec![burn_p2p_core::ContributionReceiptId::new("receipt")],
        };
        let head = HeadDescriptor {
            head_id: HeadId::new("head"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-head"),
            parent_head_id: Some(HeadId::new("base")),
            global_step: 4,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let reducer_load = ReducerLoadAnnouncement {
            overlay: burn_p2p::OverlayTopic::experiment(
                burn_p2p_core::NetworkId::new("mainnet"),
                StudyId::new("study"),
                ExperimentId::new("exp"),
                burn_p2p::OverlayChannel::Telemetry,
            )
            .expect("telemetry overlay"),
            report: burn_p2p_core::ReducerLoadReport {
                peer_id: peer_id.clone(),
                window_id: WindowId(7),
                assigned_leaf_updates: 2,
                assigned_aggregate_inputs: 1,
                ingress_bytes: 64,
                egress_bytes: 32,
                duplicate_transfer_ratio: 0.0,
                overload_ratio: 0.0,
                reported_at: now,
            },
        };
        let runtime_snapshot = NodeTelemetrySnapshot {
            status: RuntimeStatus::Running,
            node_state: super::NodeRuntimeState::IdleReady,
            slot_states: vec![super::SlotRuntimeState::Assigned(SlotAssignmentState::new(
                StudyId::new("study"),
                ExperimentId::new("exp"),
                RevisionId::new("rev"),
            ))],
            network_id: Some(burn_p2p_core::NetworkId::new("mainnet")),
            local_peer_id: Some(peer_id.clone()),
            configured_roles: burn_p2p_core::PeerRoleSet::new([burn_p2p_core::PeerRole::Validator]),
            connected_peers: 1,
            observed_peer_ids: BTreeSet::from([peer_id.clone()]),
            known_peer_addresses: BTreeSet::new(),
            runtime_boundary: None,
            listen_addresses: Vec::new(),
            control_plane: ControlPlaneSnapshot {
                reducer_load_announcements: vec![reducer_load.clone()],
                ..ControlPlaneSnapshot::default()
            },
            recent_events: vec![LiveControlPlaneEvent::ConnectionEstablished {
                peer_id: peer_id.to_string(),
            }],
            last_snapshot_peer_id: Some(peer_id.clone()),
            last_snapshot: Some(ControlPlaneSnapshot::default()),
            admitted_peers: BTreeMap::new(),
            rejected_peers: BTreeMap::new(),
            peer_reputation: BTreeMap::new(),
            minimum_revocation_epoch: None,
            trust_bundle: None,
            in_flight_transfers: BTreeMap::new(),
            effective_limit_profile: None,
            last_error: Some("peer rejected".into()),
            started_at: now,
            updated_at: now,
        };
        let mut state = BootstrapAdminState {
            head_descriptors: vec![head.clone()],
            contribution_receipts: vec![receipt.clone()],
            merge_certificates: vec![merge.clone()],
            reducer_load_announcements: vec![reducer_load.clone()],
            runtime_snapshot: Some(runtime_snapshot.clone()),
            ..BootstrapAdminState::default()
        };

        let result = plan
            .execute_admin_action(
                AdminAction::ExportDiagnosticsBundle,
                &mut state,
                None,
                now,
                Some(42),
            )
            .expect("bundle");

        match result {
            AdminResult::DiagnosticsBundle(bundle) => {
                assert_eq!(
                    bundle.plan.network_id(),
                    &burn_p2p_core::NetworkId::new("mainnet")
                );
                assert_eq!(bundle.diagnostics.accepted_receipts, 1);
                assert_eq!(bundle.heads, vec![head]);
                assert_eq!(bundle.contribution_receipts, vec![receipt]);
                assert_eq!(bundle.merge_certificates, vec![merge]);
                assert_eq!(bundle.reducer_load_announcements, vec![reducer_load]);
                assert_eq!(bundle.runtime_snapshot, Some(runtime_snapshot));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn head_and_reducer_load_exports_filter_on_scope() {
        let plan = plan(BootstrapPreset::AuthorityValidator);
        let now = Utc::now();
        let head = HeadDescriptor {
            head_id: HeadId::new("head-1"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-head"),
            parent_head_id: Some(HeadId::new("base")),
            global_step: 9,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let reducer_load = ReducerLoadAnnouncement {
            overlay: burn_p2p::OverlayTopic::experiment(
                burn_p2p_core::NetworkId::new("mainnet"),
                StudyId::new("study"),
                ExperimentId::new("exp"),
                burn_p2p::OverlayChannel::Telemetry,
            )
            .expect("telemetry overlay"),
            report: burn_p2p_core::ReducerLoadReport {
                peer_id: PeerId::new("peer"),
                window_id: WindowId(9),
                assigned_leaf_updates: 3,
                assigned_aggregate_inputs: 1,
                ingress_bytes: 128,
                egress_bytes: 64,
                duplicate_transfer_ratio: 0.25,
                overload_ratio: 0.1,
                reported_at: now,
            },
        };
        let mut state = BootstrapAdminState {
            head_descriptors: vec![head.clone()],
            reducer_load_announcements: vec![reducer_load.clone()],
            ..BootstrapAdminState::default()
        };

        let heads = plan
            .execute_admin_action(
                AdminAction::ExportHeads(HeadQuery {
                    study_id: Some(StudyId::new("study")),
                    experiment_id: Some(ExperimentId::new("exp")),
                    revision_id: Some(RevisionId::new("rev")),
                    head_id: None,
                }),
                &mut state,
                None,
                now,
                None,
            )
            .expect("heads");
        match heads {
            AdminResult::Heads(exported) => assert_eq!(exported, vec![head]),
            other => panic!("unexpected result: {other:?}"),
        }

        let reducer_loads = plan
            .execute_admin_action(
                AdminAction::ExportReducerLoad(ReducerLoadQuery {
                    study_id: Some(StudyId::new("study")),
                    experiment_id: Some(ExperimentId::new("exp")),
                    peer_id: Some(PeerId::new("peer")),
                }),
                &mut state,
                None,
                now,
                None,
            )
            .expect("reducer load");
        match reducer_loads {
            AdminResult::ReducerLoad(exported) => assert_eq!(exported, vec![reducer_load]),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn control_actions_update_local_admin_policy_state() {
        let plan = plan(BootstrapPreset::AuthorityValidator);
        let now = Utc::now();
        let peer_id = PeerId::new("peer-revoked");
        let mut state = BootstrapAdminState::default();

        let revoke = plan
            .execute_admin_action(
                AdminAction::Control(ExperimentControlCommand::RevokePeer {
                    peer_id: peer_id.clone(),
                    minimum_revocation_epoch: burn_p2p::RevocationEpoch(7),
                    reason: "key compromised".into(),
                }),
                &mut state,
                Some(burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: now,
                    signature_hex: "abcd".into(),
                }),
                now,
                None,
            )
            .expect("revoke result");
        assert!(matches!(revoke, AdminResult::Control(_)));
        assert!(state.quarantined_peers.contains(&peer_id));
        assert!(state.banned_peers.contains(&peer_id));
        assert_eq!(
            state.minimum_revocation_epoch,
            Some(burn_p2p::RevocationEpoch(7))
        );

        let quarantine_peer = PeerId::new("peer-quarantined");
        let quarantine = plan
            .execute_admin_action(
                AdminAction::Control(ExperimentControlCommand::QuarantinePeer {
                    peer_id: quarantine_peer.clone(),
                    reason: "audit pending".into(),
                    target: ActivationTarget {
                        activation: WindowActivation {
                            activation_window: WindowId(3),
                            grace_windows: 0,
                        },
                        required_client_capabilities: BTreeSet::new(),
                    },
                }),
                &mut state,
                Some(burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: now,
                    signature_hex: "beef".into(),
                }),
                now,
                None,
            )
            .expect("quarantine result");
        assert!(matches!(quarantine, AdminResult::Control(_)));
        assert!(state.quarantined_peers.contains(&quarantine_peer));
        assert!(!state.banned_peers.contains(&quarantine_peer));

        let pardon = plan
            .execute_admin_action(
                AdminAction::Control(ExperimentControlCommand::PardonPeer {
                    peer_id: quarantine_peer.clone(),
                    target: ActivationTarget {
                        activation: WindowActivation {
                            activation_window: WindowId(4),
                            grace_windows: 0,
                        },
                        required_client_capabilities: BTreeSet::new(),
                    },
                }),
                &mut state,
                Some(burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: now,
                    signature_hex: "cafe".into(),
                }),
                now,
                None,
            )
            .expect("pardon result");
        assert!(matches!(pardon, AdminResult::Control(_)));
        assert!(!state.quarantined_peers.contains(&quarantine_peer));
        assert!(!state.banned_peers.contains(&quarantine_peer));
    }

    #[test]
    fn receipt_export_filters_on_scope() {
        let plan = plan(BootstrapPreset::BootstrapOnly);
        let now = Utc::now();
        let receipt = ContributionReceipt {
            receipt_id: burn_p2p_core::ContributionReceiptId::new("receipt"),
            peer_id: PeerId::new("peer"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base"),
            artifact_id: ArtifactId::new("artifact"),
            accepted_at: now,
            accepted_weight: 1.0,
            metrics: BTreeMap::new(),
            merge_cert_id: None,
        };

        let mut state = BootstrapAdminState {
            contribution_receipts: vec![receipt.clone()],
            ..BootstrapAdminState::default()
        };

        let result = plan
            .execute_admin_action(
                AdminAction::ExportReceipts(ReceiptQuery {
                    study_id: Some(StudyId::new("study")),
                    experiment_id: Some(ExperimentId::new("exp")),
                    revision_id: None,
                    peer_id: None,
                }),
                &mut state,
                None,
                now,
                None,
            )
            .expect("receipts");

        match result {
            AdminResult::Receipts(receipts) => assert_eq!(receipts, vec![receipt]),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn openmetrics_export_contains_core_operator_metrics() {
        let diagnostics = BootstrapAdminState::default().diagnostics(
            &plan(BootstrapPreset::BootstrapOnly),
            Utc::now(),
            Some(120),
        );

        let metrics = render_openmetrics(&diagnostics);

        assert!(metrics.contains("burn_p2p_connected_peers"));
        assert!(metrics.contains("burn_p2p_observed_peers"));
        assert!(metrics.contains("burn_p2p_accepted_receipts"));
        assert!(metrics.contains("burn_p2p_admitted_peers"));
        assert!(metrics.contains("burn_p2p_rejected_peers"));
    }

    #[test]
    fn dashboard_html_references_status_and_event_endpoints() {
        let html = render_dashboard_html(&burn_p2p_core::NetworkId::new("mainnet"));

        assert!(html.contains("/status"));
        assert!(html.contains("/events"));
        assert!(html.contains("burn_p2p bootstrap"));
    }

    #[test]
    fn embedded_daemon_initializes_and_certifies_trainer_updates() {
        let dataset_dir = TempDir::new().expect("dataset tempdir");
        create_dataset(dataset_dir.path());
        let validator_storage = TempDir::new().expect("validator tempdir");
        let trainer_storage = TempDir::new().expect("trainer tempdir");
        let active = ActiveExperiment {
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
        };
        let daemon = plan(BootstrapPreset::AuthorityValidator)
            .spawn_embedded_daemon::<_, SyntheticBackend>(
                SyntheticProject {
                    dataset_root: dataset_dir.path().to_path_buf(),
                    learning_rate: 0.1,
                    target_model: 10.0,
                },
                BootstrapEmbeddedDaemonConfig {
                    node: NodeConfig {
                        identity: burn_p2p::IdentityConfig::Persistent,
                        storage: Some(StorageConfig::new(validator_storage.path())),
                        dataset: Some(DatasetConfig::new(burn_p2p::UpstreamAdapter::Local {
                            root: dataset_dir.path().display().to_string(),
                        })),
                        auth: None,
                        network_manifest: None,
                        client_release_manifest: None,
                        selected_workload_id: None,
                        bootstrap_peers: Vec::new(),
                        listen_addresses: vec![
                            burn_p2p_swarm::SwarmAddress::new("/ip4/127.0.0.1/tcp/0")
                                .expect("listen address"),
                        ],
                    },
                    active_experiment: active.clone(),
                    initialize_head_on_start: true,
                    restore_head_on_start: true,
                    validation_interval_millis: 100,
                    training_interval_millis: None,
                },
            )
            .expect("spawn embedded daemon");
        let daemon_telemetry = daemon.telemetry();

        wait_for(
            Duration::from_secs(5),
            || !daemon_telemetry.snapshot().listen_addresses.is_empty(),
            "embedded daemon did not start listening",
        );
        wait_for(
            Duration::from_secs(5),
            || {
                !daemon_telemetry
                    .snapshot()
                    .control_plane
                    .head_announcements
                    .is_empty()
            },
            "embedded daemon did not publish an initial head",
        );
        let bootstrap_addr = daemon_telemetry.snapshot().listen_addresses[0].clone();

        let mut trainer = NodeBuilder::new(SyntheticProject {
            dataset_root: dataset_dir.path().to_path_buf(),
            learning_rate: 1.0,
            target_model: 10.0,
        })
        .with_mainnet(genesis())
        .with_storage(StorageConfig::new(trainer_storage.path()))
        .with_dataset(DatasetConfig::new(burn_p2p::UpstreamAdapter::Local {
            root: dataset_dir.path().display().to_string(),
        }))
        .with_bootstrap_peer(bootstrap_addr)
        .spawn()
        .expect("trainer spawn");
        let experiment = trainer.experiment(
            active.study_id.clone(),
            active.experiment_id.clone(),
            active.revision_id.clone(),
        );

        wait_for(
            Duration::from_secs(5),
            || daemon_telemetry.snapshot().connected_peers >= 1,
            "embedded daemon did not connect to trainer",
        );

        let outcome = trainer
            .train_window_once::<SyntheticBackend>(&experiment)
            .expect("trainer window");
        assert!(matches!(
            outcome.report.stats.get("loss"),
            Some(MetricValue::Float(_))
        ));

        wait_for(
            Duration::from_secs(5),
            || {
                let state = daemon.admin_state();
                let state = state
                    .lock()
                    .expect("embedded daemon state should not be poisoned");
                !state.contribution_receipts.is_empty() && !state.merge_certificates.is_empty()
            },
            "embedded daemon did not certify trainer update",
        );

        let diagnostics = daemon.diagnostics(None);
        assert_eq!(diagnostics.accepted_receipts, 1);
        assert_eq!(diagnostics.certified_merges, 1);

        trainer.shutdown().expect("trainer shutdown");
        let _ = trainer.await_termination().expect("trainer termination");
        daemon.shutdown().expect("daemon shutdown");
        daemon.await_termination().expect("daemon termination");
    }
}
