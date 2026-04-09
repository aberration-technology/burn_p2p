use std::collections::BTreeSet;

use crate::{
    AdminCapability, BootstrapAdminState, BootstrapDiagnostics, BootstrapDiagnosticsBundle,
    BootstrapError, BootstrapPlan, HeadQuery, ReceiptQuery, ReducerLoadQuery,
};
use burn_p2p::{ContributionReceipt, HeadDescriptor, ReducerLoadAnnouncement, TrustedIssuer};
use burn_p2p_core::{
    ControlCertificate, PeerId, ReenrollmentStatus, RevocationEpoch, SignatureMetadata,
    TrustBundleExport, TrustedIssuerStatus,
};
use burn_p2p_experiment::{ExperimentControlCommand, ExperimentControlEnvelope};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents an auth policy rollout.
pub struct AuthPolicyRollout {
    /// The minimum revocation epoch.
    pub minimum_revocation_epoch: Option<RevocationEpoch>,
    /// The directory entries.
    pub directory_entries: Option<Vec<burn_p2p::ExperimentDirectoryEntry>>,
    /// The trusted issuers.
    pub trusted_issuers: Option<Vec<TrustedIssuer>>,
    /// The reenrollment.
    pub reenrollment: Option<ReenrollmentStatus>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Enumerates the supported admin action values.
pub enum AdminAction {
    /// Uses the control variant.
    Control(ExperimentControlCommand),
    /// Uses the ban peer variant.
    BanPeer {
        /// The peer ID.
        peer_id: PeerId,
        /// The reason.
        reason: String,
    },
    /// Uses the export diagnostics variant.
    ExportDiagnostics,
    /// Uses the export diagnostics bundle variant.
    ExportDiagnosticsBundle,
    /// Uses the export heads variant.
    ExportHeads(HeadQuery),
    /// Uses the export receipts variant.
    ExportReceipts(ReceiptQuery),
    /// Uses the export reducer load variant.
    ExportReducerLoad(ReducerLoadQuery),
    /// Uses the export trust bundle variant.
    ExportTrustBundle,
    /// Uses the rollout auth policy variant.
    RolloutAuthPolicy(AuthPolicyRollout),
    /// Uses the retire trusted issuers variant.
    RetireTrustedIssuers {
        /// The issuer peer IDs.
        issuer_peer_ids: BTreeSet<PeerId>,
    },
    /// Uses the rotate authority material variant.
    RotateAuthorityMaterial {
        /// The issuer key ID.
        issuer_key_id: Option<String>,
        /// The retain previous issuer.
        retain_previous_issuer: bool,
        /// The require reenrollment.
        require_reenrollment: bool,
        /// The reenrollment reason.
        reenrollment_reason: Option<String>,
    },
}

impl AdminAction {
    /// Performs the capability operation.
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
/// Enumerates the supported admin result values.
pub enum AdminResult {
    /// Uses the control variant.
    Control(Box<ControlCertificate<ExperimentControlEnvelope>>),
    /// Uses the peer banned variant.
    PeerBanned {
        /// The peer ID.
        peer_id: PeerId,
        /// The reason.
        reason: String,
    },
    /// Uses the diagnostics variant.
    Diagnostics(Box<BootstrapDiagnostics>),
    /// Uses the diagnostics bundle variant.
    DiagnosticsBundle(Box<BootstrapDiagnosticsBundle>),
    /// Uses the heads variant.
    Heads(Vec<HeadDescriptor>),
    /// Uses the receipts variant.
    Receipts(Vec<ContributionReceipt>),
    /// Uses the reducer load variant.
    ReducerLoad(Vec<ReducerLoadAnnouncement>),
    /// Uses the trust bundle variant.
    TrustBundle(Option<TrustBundleExport>),
    /// Uses the auth policy rolled out variant.
    AuthPolicyRolledOut {
        /// The minimum revocation epoch.
        minimum_revocation_epoch: Option<RevocationEpoch>,
        /// The directory entries.
        directory_entries: usize,
        /// The trusted issuers.
        trusted_issuers: usize,
        /// The reenrollment required.
        reenrollment_required: bool,
    },
    /// Uses the trusted issuers retired variant.
    TrustedIssuersRetired {
        /// The retired issuers.
        retired_issuers: usize,
        /// The remaining issuers.
        remaining_issuers: usize,
        /// The reenrollment required.
        reenrollment_required: bool,
    },
    /// Uses the authority material rotated variant.
    AuthorityMaterialRotated {
        /// The issuer key ID.
        issuer_key_id: String,
        /// The issuer peer ID.
        issuer_peer_id: PeerId,
        /// The issuer public key hex.
        issuer_public_key_hex: String,
        /// The trusted issuers.
        trusted_issuers: usize,
        /// The reenrollment required.
        reenrollment_required: bool,
        /// The rotated at.
        rotated_at: DateTime<Utc>,
    },
}

impl BootstrapPlan {
    /// Returns whether the value supports admin action.
    pub fn supports_admin_action(&self, action: &AdminAction) -> bool {
        self.admin_api
            .supported_actions
            .contains(&action.capability())
    }

    /// Issues the control certificate.
    pub fn issue_control_certificate(
        &self,
        command: ExperimentControlCommand,
        signer: SignatureMetadata,
    ) -> Result<ControlCertificate<ExperimentControlEnvelope>, BootstrapError> {
        if !self.supports_service(&crate::BootstrapService::Authority) {
            return Err(BootstrapError::AuthorityServiceRequired);
        }

        Ok(ExperimentControlEnvelope {
            network_id: self.genesis.network_id.clone(),
            command,
        }
        .into_signed_cert(signer, self.genesis.protocol_version.clone())?)
    }

    /// Performs the execute admin action operation.
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
                Ok(AdminResult::Control(Box::new(certificate)))
            }
            AdminAction::BanPeer { peer_id, reason } => {
                state.quarantined_peers.insert(peer_id.clone());
                state.banned_peers.insert(peer_id.clone());
                Ok(AdminResult::PeerBanned { peer_id, reason })
            }
            AdminAction::ExportDiagnostics => Ok(AdminResult::Diagnostics(Box::new(
                state.diagnostics(self, captured_at, remaining_work_units),
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
                        .retired_issuer_peer_ids
                        .retain(|issuer_peer_id| !issuer_peer_ids.contains(issuer_peer_id));
                    if reenrollment.retired_issuer_peer_ids.is_empty() {
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
