//! Bootstrap, edge, and operator-facing services for `burn_p2p` deployments.
//!
//! This crate is the main composition root for deployment-facing features. It layers
//! optional services such as:
//!
//! - admin and diagnostics HTTP routes
//! - portal rendering and browser-edge snapshots
//! - optional auth providers and enrollment flows
//! - optional social snapshots and profile surfaces
//! - embedded-runtime daemon wiring
//!
//! Core runtime correctness still lives in `burn_p2p` and the lower-level crates. This
//! crate focuses on deployment assembly, capability advertisement, and operator-facing
//! workflows.
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

use burn_p2p::compat::RuntimeProject;
use burn_p2p::{
    ArtifactTransferState, ContentId, ControlHandle, ExperimentHandle, HeadDescriptor,
    IdentityConfig, LeaderboardEntry, LeaderboardIdentity, LeaderboardSnapshot,
    LiveControlPlaneEvent, MetricsRetentionBudget, MetricsRetentionConfig, NodeBuilder, NodeConfig,
    NodeTelemetrySnapshot, ProjectBackend, ProjectFamilyId, ReducerLoadAnnouncement,
    RevocationEpoch, RunningNode, StorageConfig, TelemetryHandle, TrustedIssuer,
};
#[cfg(feature = "metrics-indexer")]
use burn_p2p_core::MetricsLedgerSegment;
#[cfg(any(test, feature = "portal"))]
use burn_p2p_core::PrincipalId;
use burn_p2p_core::{
    ArtifactId, BrowserMode, ContributionReceipt, ContributionReceiptId, ControlCertificate,
    EvalProtocolManifest, ExperimentDirectoryEntry, ExperimentId, ExportJob, GenesisSpec,
    HeadEvalReport, HeadId, MergeCertificate, MetricsLiveEvent, MetricsLiveEventKind, NetworkId,
    PeerId, PeerRole, PeerRoleSet, PeerWindowMetrics, ProfileMode, ReducerCohortMetrics,
    RevisionId, SignatureMetadata, SocialMode, StudyId,
};
use burn_p2p_experiment::{ExperimentControlCommand, ExperimentControlEnvelope};
#[cfg(feature = "metrics-indexer")]
use burn_p2p_metrics::{
    DerivedMetricKind, MetricEnvelope, MetricsCatchupBundle, MetricsIndexer, MetricsIndexerConfig,
    MetricsSnapshot, MetricsStore, PeerWindowDistributionDetail, PeerWindowDistributionSummary,
    build_catchup_bundles, build_live_event, derive_peer_window_distribution_detail_with_limit,
    derive_peer_window_distribution_summaries,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p_publish::{
    ArtifactBackfillRequest, ArtifactBackfillResult, ArtifactPruneRequest, ArtifactPruneResult,
    DEFAULT_PUBLICATION_TARGET_ID, DownloadTicketRequest, PublicationStore,
};
use burn_p2p_security::{
    PeerAdmissionReport, PeerTrustLevel, ReleasePolicy, ReputationDecision, ReputationEngine,
    ReputationState, ValidatorPolicy,
};
use burn_p2p_swarm::{PeerStore, RuntimeBoundary, SwarmAddress, SwarmError, SwarmStats};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use burn_p2p::{NodeRuntimeState, SlotRuntimeState};
#[cfg(feature = "artifact-publish")]
pub use burn_p2p_publish::{
    ArtifactAliasStatus, ArtifactRunSummary, ArtifactRunView, DownloadArtifact,
    DownloadTicketResponse, ExportRequest, HeadArtifactView,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported bootstrap preset values.
pub enum BootstrapPreset {
    /// Uses the bootstrap only variant.
    BootstrapOnly,
    /// Uses the bootstrap archive variant.
    BootstrapArchive,
    /// Uses the authority validator variant.
    AuthorityValidator,
    /// Uses the all in one variant.
    AllInOne,
}

impl BootstrapPreset {
    /// Performs the services operation.
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

    /// Performs the roles operation.
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
/// Enumerates the supported bootstrap service values.
pub enum BootstrapService {
    /// Uses the relay variant.
    Relay,
    /// Uses the rendezvous variant.
    Rendezvous,
    /// Uses the kademlia variant.
    Kademlia,
    /// Uses the authority variant.
    Authority,
    /// Uses the validator variant.
    Validator,
    /// Uses the archive variant.
    Archive,
    /// Uses the admin API variant.
    AdminApi,
    /// Uses the experiment controller variant.
    ExperimentController,
    /// Uses the telemetry export variant.
    TelemetryExport,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents an authority plan.
pub struct AuthorityPlan {
    /// The release policy.
    pub release_policy: ReleasePolicy,
    /// The validator policy.
    pub validator_policy: ValidatorPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an archive plan.
pub struct ArchivePlan {
    /// The pinned heads.
    pub pinned_heads: BTreeSet<HeadId>,
    /// The pinned artifacts.
    pub pinned_artifacts: BTreeSet<ArtifactId>,
    /// The retain contribution receipts.
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
/// Represents an admin API plan.
pub struct AdminApiPlan {
    /// The supported actions.
    pub supported_actions: BTreeSet<AdminCapability>,
    /// The diagnostics enabled.
    pub diagnostics_enabled: bool,
    /// The receipt exports enabled.
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
/// Enumerates the supported admin capability values.
pub enum AdminCapability {
    /// Uses the control variant.
    Control,
    /// Uses the ban peer variant.
    BanPeer,
    /// Uses the export diagnostics variant.
    ExportDiagnostics,
    /// Uses the export diagnostics bundle variant.
    ExportDiagnosticsBundle,
    /// Uses the export heads variant.
    ExportHeads,
    /// Uses the export receipts variant.
    ExportReceipts,
    /// Uses the export reducer load variant.
    ExportReducerLoad,
    /// Uses the export trust bundle variant.
    ExportTrustBundle,
    /// Uses the rollout auth policy variant.
    RolloutAuthPolicy,
    /// Uses the retire trusted issuers variant.
    RetireTrustedIssuers,
    /// Uses the rotate authority material variant.
    RotateAuthorityMaterial,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a telemetry export plan.
pub struct TelemetryExportPlan {
    /// The openmetrics enabled.
    pub openmetrics_enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a bootstrap spec.
pub struct BootstrapSpec {
    /// The preset.
    pub preset: BootstrapPreset,
    /// The genesis.
    pub genesis: GenesisSpec,
    /// The platform.
    pub platform: burn_p2p_core::ClientPlatform,
    /// The bootstrap addresses.
    pub bootstrap_addresses: Vec<SwarmAddress>,
    /// The listen addresses.
    pub listen_addresses: Vec<SwarmAddress>,
    /// The authority.
    pub authority: Option<AuthorityPlan>,
    /// The archive.
    pub archive: ArchivePlan,
    /// The admin API.
    pub admin_api: AdminApiPlan,
}

impl BootstrapSpec {
    /// Performs the plan operation.
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
/// Represents a bootstrap plan.
pub struct BootstrapPlan {
    /// The preset.
    pub preset: BootstrapPreset,
    /// The genesis.
    pub genesis: GenesisSpec,
    /// The services.
    pub services: BTreeSet<BootstrapService>,
    /// The roles.
    pub roles: PeerRoleSet,
    /// The runtime.
    pub runtime: RuntimeBoundary,
    /// The authority.
    pub authority: Option<AuthorityPlan>,
    /// The archive.
    pub archive: ArchivePlan,
    /// The admin API.
    pub admin_api: AdminApiPlan,
    /// The telemetry.
    pub telemetry: TelemetryExportPlan,
}

impl BootstrapPlan {
    /// Performs the network ID operation.
    pub fn network_id(&self) -> &NetworkId {
        &self.genesis.network_id
    }

    /// Returns whether the value supports service.
    pub fn supports_service(&self, service: &BootstrapService) -> bool {
        self.services.contains(service)
    }

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
        if !self.supports_service(&BootstrapService::Authority) {
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a trusted issuer status.
pub struct TrustedIssuerStatus {
    /// The issuer peer ID.
    pub issuer_peer_id: PeerId,
    /// The issuer public key hex.
    pub issuer_public_key_hex: String,
    /// The active for new certificates.
    pub active_for_new_certificates: bool,
    /// The accepted for admission.
    pub accepted_for_admission: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a reenrollment status.
pub struct ReenrollmentStatus {
    /// The reason.
    pub reason: String,
    /// The rotated at.
    pub rotated_at: Option<DateTime<Utc>>,
    /// The legacy issuer peer IDs.
    pub legacy_issuer_peer_ids: BTreeSet<PeerId>,
    /// The login path.
    pub login_path: String,
    /// The enroll path.
    pub enroll_path: String,
    /// The trust bundle path.
    pub trust_bundle_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a trust bundle export.
pub struct TrustBundleExport {
    /// The network ID.
    pub network_id: NetworkId,
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The required release train hash.
    pub required_release_train_hash: ContentId,
    #[serde(default)]
    /// The allowed target artifact hashes.
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
    /// The minimum revocation epoch.
    pub minimum_revocation_epoch: RevocationEpoch,
    /// The active issuer peer ID.
    pub active_issuer_peer_id: PeerId,
    /// The issuers.
    pub issuers: Vec<TrustedIssuerStatus>,
    /// The reenrollment.
    pub reenrollment: Option<ReenrollmentStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported browser edge modes.
pub enum BrowserEdgeMode {
    /// Runs in minimal mode.
    Minimal,
    /// Runs in peer mode.
    Peer,
    /// Runs in full mode.
    Full,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser edge paths.
pub struct BrowserEdgePaths {
    /// The capabilities path.
    pub capabilities_path: String,
    /// The portal snapshot path.
    pub portal_snapshot_path: String,
    /// The directory path.
    pub directory_path: String,
    /// The heads path.
    pub heads_path: String,
    /// The signed directory path.
    pub signed_directory_path: String,
    /// The leaderboard path.
    pub leaderboard_path: String,
    /// The signed leaderboard path.
    pub signed_leaderboard_path: String,
    /// The receipt submit path.
    pub receipt_submit_path: String,
    /// The login path.
    pub login_path: String,
    /// The callback path.
    pub callback_path: String,
    /// The enroll path.
    pub enroll_path: String,
    /// The event stream path.
    pub event_stream_path: String,
    /// The metrics path.
    pub metrics_path: String,
    /// The metrics snapshot path.
    pub metrics_snapshot_path: String,
    /// The metrics ledger path prefix.
    pub metrics_ledger_path: String,
    /// The live metrics stream path.
    pub metrics_live_path: String,
    /// The latest live metrics frame path.
    pub metrics_live_latest_path: String,
    /// The metrics catchup path prefix.
    pub metrics_catchup_path: String,
    /// The candidate-focused metrics path prefix.
    pub metrics_candidates_path: String,
    /// The disagreement-focused metrics path prefix.
    pub metrics_disagreements_path: String,
    /// The peer-window distribution metrics path prefix.
    pub metrics_peer_windows_path: String,
    /// The head-scoped metrics path prefix.
    pub metrics_heads_path: String,
    /// The experiment-scoped metrics path prefix.
    pub metrics_experiments_path: String,
    /// The artifact alias path prefix.
    pub artifacts_aliases_path: String,
    /// The artifact live event stream path.
    pub artifacts_live_path: String,
    /// The latest artifact live event path.
    pub artifacts_live_latest_path: String,
    /// The run-scoped artifact history path prefix.
    pub artifacts_runs_path: String,
    /// The head-scoped artifact detail path prefix.
    pub artifacts_heads_path: String,
    /// The artifact export path.
    pub artifacts_export_path: String,
    /// The artifact job path prefix.
    pub artifacts_export_jobs_path: String,
    /// The artifact download-ticket path.
    pub artifacts_download_ticket_path: String,
    /// The artifact download path prefix.
    pub artifacts_download_path: String,
    /// The trust bundle path.
    pub trust_bundle_path: String,
    /// The reenrollment path.
    pub reenrollment_path: String,
}

impl Default for BrowserEdgePaths {
    fn default() -> Self {
        Self {
            capabilities_path: "/capabilities".into(),
            portal_snapshot_path: "/portal/snapshot".into(),
            directory_path: "/directory".into(),
            heads_path: "/heads".into(),
            signed_directory_path: "/directory/signed".into(),
            leaderboard_path: "/leaderboard".into(),
            signed_leaderboard_path: "/leaderboard/signed".into(),
            receipt_submit_path: "/receipts/browser".into(),
            login_path: "/login/static".into(),
            callback_path: "/callback/static".into(),
            enroll_path: "/enroll".into(),
            event_stream_path: "/events".into(),
            metrics_path: "/metrics".into(),
            metrics_snapshot_path: "/metrics/snapshot".into(),
            metrics_ledger_path: "/metrics/ledger".into(),
            metrics_live_path: "/metrics/live".into(),
            metrics_live_latest_path: "/metrics/live/latest".into(),
            metrics_catchup_path: "/metrics/catchup".into(),
            metrics_candidates_path: "/metrics/candidates".into(),
            metrics_disagreements_path: "/metrics/disagreements".into(),
            metrics_peer_windows_path: "/metrics/peer-windows".into(),
            metrics_heads_path: "/metrics/heads".into(),
            metrics_experiments_path: "/metrics/experiments".into(),
            artifacts_aliases_path: "/artifacts/aliases".into(),
            artifacts_live_path: "/artifacts/live".into(),
            artifacts_live_latest_path: "/artifacts/live/latest".into(),
            artifacts_runs_path: "/artifacts/runs".into(),
            artifacts_heads_path: "/artifacts/heads".into(),
            artifacts_export_path: "/artifacts/export".into(),
            artifacts_export_jobs_path: "/artifacts/export".into(),
            artifacts_download_ticket_path: "/artifacts/download-ticket".into(),
            artifacts_download_path: "/artifacts/download".into(),
            trust_bundle_path: "/trust".into(),
            reenrollment_path: "/reenrollment".into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser transport surface.
pub struct BrowserTransportSurface {
    /// The webrtc direct.
    pub webrtc_direct: bool,
    /// The WebTransport gateway.
    pub webtransport_gateway: bool,
    /// The WSS fallback.
    pub wss_fallback: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser login provider.
pub struct BrowserLoginProvider {
    /// The label.
    pub label: String,
    /// The login path.
    pub login_path: String,
    /// The callback path.
    pub callback_path: Option<String>,
    /// The device path.
    pub device_path: Option<String>,
}

/// Defines the browser leaderboard identity alias.
pub type BrowserLeaderboardIdentity = LeaderboardIdentity;
/// Defines the browser leaderboard entry alias.
pub type BrowserLeaderboardEntry = LeaderboardEntry;
/// Defines the browser leaderboard snapshot alias.
pub type BrowserLeaderboardSnapshot = LeaderboardSnapshot;

#[cfg(feature = "social")]
mod social_services {
    use std::collections::BTreeMap;

    use burn_p2p_core::{
        ContributionReceipt, LeaderboardSnapshot, MergeCertificate, NetworkId, PeerId, PrincipalId,
    };
    use burn_p2p_social::{
        LeaderboardComputationInput, LeaderboardService, ReceiptLeaderboardService,
    };
    use chrono::{DateTime, Utc};

    pub(super) fn leaderboard_snapshot(
        network_id: &NetworkId,
        receipts: &[ContributionReceipt],
        merge_certificates: &[MergeCertificate],
        peer_principals: &BTreeMap<PeerId, PrincipalId>,
        captured_at: DateTime<Utc>,
    ) -> LeaderboardSnapshot {
        ReceiptLeaderboardService::default().snapshot(&LeaderboardComputationInput {
            network_id,
            receipts,
            merge_certificates,
            peer_principals,
            captured_at,
        })
    }
}

#[cfg(not(feature = "social"))]
mod social_services {
    use std::collections::BTreeMap;

    use burn_p2p_core::{
        ContributionReceipt, LeaderboardSnapshot, MergeCertificate, NetworkId, PeerId, PrincipalId,
    };
    use chrono::{DateTime, Utc};

    pub(super) fn leaderboard_snapshot(
        network_id: &NetworkId,
        _receipts: &[ContributionReceipt],
        _merge_certificates: &[MergeCertificate],
        _peer_principals: &BTreeMap<PeerId, PrincipalId>,
        captured_at: DateTime<Utc>,
    ) -> LeaderboardSnapshot {
        LeaderboardSnapshot {
            network_id: network_id.clone(),
            score_version: "leaderboard_score_v1".into(),
            entries: Vec::new(),
            captured_at,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Captures a snapshot of browser directory.
pub struct BrowserDirectorySnapshot {
    /// The network ID.
    pub network_id: NetworkId,
    /// The generated at.
    pub generated_at: DateTime<Utc>,
    /// The entries.
    pub entries: Vec<ExperimentDirectoryEntry>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Captures a snapshot of browser portal.
pub struct BrowserPortalSnapshot {
    /// The network ID.
    pub network_id: NetworkId,
    /// The edge mode.
    pub edge_mode: BrowserEdgeMode,
    /// The browser mode.
    pub browser_mode: BrowserMode,
    /// The social mode.
    pub social_mode: SocialMode,
    /// The profile mode.
    pub profile_mode: ProfileMode,
    /// The transports.
    pub transports: BrowserTransportSurface,
    /// The paths.
    pub paths: BrowserEdgePaths,
    /// The auth enabled.
    pub auth_enabled: bool,
    /// The login providers.
    pub login_providers: Vec<BrowserLoginProvider>,
    /// The required release train hash.
    pub required_release_train_hash: Option<ContentId>,
    /// The allowed target artifact hashes.
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
    /// The diagnostics.
    pub diagnostics: BootstrapDiagnostics,
    /// The directory.
    pub directory: BrowserDirectorySnapshot,
    /// The heads.
    pub heads: Vec<HeadDescriptor>,
    /// The leaderboard.
    pub leaderboard: BrowserLeaderboardSnapshot,
    /// The trust bundle.
    pub trust_bundle: Option<TrustBundleExport>,
    /// The captured at.
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser receipt submission response.
pub struct BrowserReceiptSubmissionResponse {
    /// The accepted receipt IDs.
    pub accepted_receipt_ids: Vec<ContributionReceiptId>,
    /// The pending receipt count.
    pub pending_receipt_count: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures browser portal snapshot.
pub struct BrowserPortalSnapshotConfig {
    /// The captured at.
    pub captured_at: DateTime<Utc>,
    /// The remaining work units.
    pub remaining_work_units: Option<u64>,
    /// The directory.
    pub directory: BrowserDirectorySnapshot,
    /// The edge mode.
    pub edge_mode: BrowserEdgeMode,
    /// The browser mode.
    pub browser_mode: BrowserMode,
    /// The social mode.
    pub social_mode: SocialMode,
    /// The profile mode.
    pub profile_mode: ProfileMode,
    /// The transports.
    pub transports: BrowserTransportSurface,
    /// The auth enabled.
    pub auth_enabled: bool,
    /// The login providers.
    pub login_providers: Vec<BrowserLoginProvider>,
    /// The required release train hash.
    pub required_release_train_hash: Option<ContentId>,
    /// The allowed target artifact hashes.
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
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
    Control(ControlCertificate<ExperimentControlEnvelope>),
    /// Uses the peer banned variant.
    PeerBanned {
        /// The peer ID.
        peer_id: PeerId,
        /// The reason.
        reason: String,
    },
    /// Uses the diagnostics variant.
    Diagnostics(BootstrapDiagnostics),
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

    /// Performs the leaderboard snapshot operation.
    pub fn leaderboard_snapshot(
        &self,
        plan: &BootstrapPlan,
        captured_at: DateTime<Utc>,
    ) -> BrowserLeaderboardSnapshot {
        let peer_principals = self
            .peer_admission_reports
            .iter()
            .map(|(peer_id, report)| (peer_id.clone(), report.principal_id.clone()))
            .collect::<BTreeMap<_, _>>();
        social_services::leaderboard_snapshot(
            &plan.genesis.network_id,
            &self.contribution_receipts,
            &self.merge_certificates,
            &peer_principals,
            captured_at,
        )
    }

    /// Performs the browser portal snapshot operation.
    pub fn browser_portal_snapshot(
        &self,
        plan: &BootstrapPlan,
        config: BrowserPortalSnapshotConfig,
    ) -> BrowserPortalSnapshot {
        BrowserPortalSnapshot {
            network_id: plan.genesis.network_id.clone(),
            edge_mode: config.edge_mode,
            browser_mode: config.browser_mode,
            social_mode: config.social_mode,
            profile_mode: config.profile_mode,
            transports: config.transports,
            paths: BrowserEdgePaths::default(),
            auth_enabled: config.auth_enabled,
            login_providers: config.login_providers,
            required_release_train_hash: config.required_release_train_hash,
            allowed_target_artifact_hashes: config.allowed_target_artifact_hashes,
            diagnostics: self.diagnostics(plan, config.captured_at, config.remaining_work_units),
            directory: config.directory,
            heads: self.head_descriptors.clone(),
            leaderboard: self.leaderboard_snapshot(plan, config.captured_at),
            trust_bundle: self.trust_bundle.clone(),
            captured_at: config.captured_at,
        }
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

    #[cfg(feature = "artifact-publish")]
    /// Exports artifact alias statuses across all loaded experiments.
    pub fn export_artifact_alias_statuses(&self) -> anyhow::Result<Vec<ArtifactAliasStatus>> {
        Ok(self
            .publication_store()?
            .map(|store| store.alias_statuses())
            .unwrap_or_default())
    }

    #[cfg(feature = "artifact-publish")]
    /// Exports artifact alias statuses for one experiment.
    pub fn export_artifact_alias_statuses_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<ArtifactAliasStatus>> {
        Ok(self
            .publication_store()?
            .map(|store| store.alias_statuses_for_experiment(experiment_id))
            .unwrap_or_default())
    }

    #[cfg(feature = "artifact-publish")]
    /// Exports run-scoped artifact summaries for one experiment.
    pub fn export_artifact_run_summaries(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<ArtifactRunSummary>> {
        Ok(self
            .publication_store()?
            .map(|store| store.run_summaries(&self.head_descriptors, experiment_id))
            .transpose()?
            .unwrap_or_default())
    }

    #[cfg(feature = "artifact-publish")]
    /// Exports one run-scoped artifact detail view.
    pub fn export_artifact_run_view(
        &self,
        experiment_id: &ExperimentId,
        run_id: &burn_p2p_core::RunId,
    ) -> anyhow::Result<Option<ArtifactRunView>> {
        let Some(store) = self.publication_store()? else {
            return Ok(None);
        };
        Ok(Some(store.run_view(
            &self.head_descriptors,
            &self.head_eval_reports,
            experiment_id,
            run_id,
        )?))
    }

    #[cfg(feature = "artifact-publish")]
    /// Exports one head-scoped artifact detail view.
    pub fn export_head_artifact_view(
        &self,
        head_id: &HeadId,
    ) -> anyhow::Result<Option<HeadArtifactView>> {
        let Some(store) = self.publication_store()? else {
            return Ok(None);
        };
        Ok(Some(store.head_view(
            &self.head_descriptors,
            &self.head_eval_reports,
            head_id,
        )?))
    }

    #[cfg(feature = "artifact-publish")]
    /// Queues or deduplicates one export job.
    pub fn request_artifact_export(&self, request: ExportRequest) -> anyhow::Result<ExportJob> {
        let Some(artifact_store) = self.artifact_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        let Some(mut store) = self.publication_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        Ok(store.request_export(
            &artifact_store,
            &self.head_descriptors,
            &self.head_eval_reports,
            request,
        )?)
    }

    #[cfg(feature = "artifact-publish")]
    /// Backfills matching aliases into publication targets.
    pub fn backfill_artifact_aliases(
        &self,
        request: ArtifactBackfillRequest,
    ) -> anyhow::Result<ArtifactBackfillResult> {
        let Some(artifact_store) = self.artifact_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        let Some(mut store) = self.publication_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        Ok(store.backfill_aliases(
            &artifact_store,
            &self.head_descriptors,
            &self.head_eval_reports,
            &self
                .eval_protocol_manifests
                .iter()
                .map(|record| record.manifest.clone())
                .collect::<Vec<_>>(),
            request,
        )?)
    }

    #[cfg(feature = "artifact-publish")]
    /// Prunes matching publication state without touching canonical CAS state.
    pub fn prune_artifact_publications(
        &self,
        request: ArtifactPruneRequest,
    ) -> anyhow::Result<ArtifactPruneResult> {
        let Some(mut store) = self.publication_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        Ok(store.prune_matching(request)?)
    }

    #[cfg(feature = "artifact-publish")]
    /// Returns one export job by identifier.
    pub fn export_artifact_job(
        &self,
        export_job_id: &burn_p2p_core::ExportJobId,
    ) -> anyhow::Result<Option<burn_p2p_core::ExportJob>> {
        Ok(self.publication_store()?.and_then(|store| {
            store
                .export_jobs()
                .iter()
                .find(|job| &job.export_job_id == export_job_id)
                .cloned()
        }))
    }

    #[cfg(feature = "artifact-publish")]
    /// Exports publication records across the current store.
    pub fn export_published_artifacts(
        &self,
    ) -> anyhow::Result<Vec<burn_p2p_core::PublishedArtifactRecord>> {
        Ok(self
            .publication_store()?
            .map(|store| store.published_artifacts().to_vec())
            .unwrap_or_default())
    }

    #[cfg(feature = "artifact-publish")]
    /// Exports known publication jobs across the current store.
    pub fn export_artifact_jobs(&self) -> anyhow::Result<Vec<burn_p2p_core::ExportJob>> {
        Ok(self
            .publication_store()?
            .map(|store| store.export_jobs().to_vec())
            .unwrap_or_default())
    }

    #[cfg(feature = "artifact-publish")]
    /// Exports recent live artifact publication events.
    pub fn export_artifact_live_events(
        &self,
    ) -> anyhow::Result<Vec<burn_p2p_core::ArtifactLiveEvent>> {
        Ok(self
            .publication_store()?
            .map(|store| store.live_events().to_vec())
            .unwrap_or_default())
    }

    #[cfg(feature = "artifact-publish")]
    /// Exports the latest live artifact publication event, when one exists.
    pub fn export_latest_artifact_live_event(
        &self,
    ) -> anyhow::Result<Option<burn_p2p_core::ArtifactLiveEvent>> {
        Ok(self
            .publication_store()?
            .and_then(|store| store.latest_live_event()))
    }

    #[cfg(feature = "artifact-publish")]
    /// Issues one short-lived download ticket, exporting on demand when needed.
    pub fn request_artifact_download_ticket(
        &self,
        request: DownloadTicketRequest,
    ) -> anyhow::Result<DownloadTicketResponse> {
        let Some(artifact_store) = self.artifact_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        let Some(mut store) = self.publication_store()? else {
            anyhow::bail!("artifact publication is disabled for this bootstrap");
        };
        Ok(store.issue_download_ticket(
            &artifact_store,
            &self.head_descriptors,
            &self.head_eval_reports,
            request,
        )?)
    }

    #[cfg(feature = "artifact-publish")]
    /// Resolves a short-lived download ticket to a streamable artifact payload.
    pub fn resolve_artifact_download(
        &self,
        ticket_id: &burn_p2p_core::DownloadTicketId,
    ) -> anyhow::Result<Option<DownloadArtifact>> {
        let Some(mut store) = self.publication_store()? else {
            return Ok(None);
        };
        Ok(Some(store.resolve_download(ticket_id)?))
    }

    #[cfg(feature = "artifact-publish")]
    /// Forces alias synchronization and expired-record pruning.
    pub fn refresh_publication_views(&self) -> anyhow::Result<()> {
        self.sync_publication_store()
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

    #[cfg(feature = "metrics-indexer")]
    /// Exports grouped metrics snapshots across all loaded experiment revisions.
    pub fn export_metrics_snapshots(&self) -> anyhow::Result<Vec<MetricsSnapshot>> {
        if let Some(store) = self.metrics_store()? {
            let snapshots = store.load_snapshots()?;
            if !snapshots.is_empty() || self.metric_envelopes().is_empty() {
                return Ok(snapshots);
            }
        }
        self.metrics_indexers()?
            .into_iter()
            .map(|((experiment_id, revision_id), indexer)| {
                let snapshot_seq = self
                    .peer_window_metrics
                    .iter()
                    .filter(|metrics| {
                        metrics.experiment_id == experiment_id && metrics.revision_id == revision_id
                    })
                    .count()
                    + self
                        .reducer_cohort_metrics
                        .iter()
                        .filter(|metrics| {
                            metrics.experiment_id == experiment_id
                                && metrics.revision_id == revision_id
                        })
                        .count()
                    + self
                        .head_eval_reports
                        .iter()
                        .filter(|report| {
                            report.experiment_id == experiment_id
                                && report.revision_id == revision_id
                        })
                        .count();
                indexer
                    .export_snapshot(
                        &experiment_id,
                        &revision_id,
                        snapshot_seq as u64,
                        None,
                        Vec::new(),
                    )
                    .map_err(anyhow::Error::from)
            })
            .collect()
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports grouped metrics snapshots for one experiment.
    pub fn export_metrics_snapshots_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<MetricsSnapshot>> {
        if let Some(store) = self.metrics_store()? {
            let snapshots = store.load_snapshots_for_experiment(experiment_id)?;
            if !snapshots.is_empty() || self.metric_envelopes().is_empty() {
                return Ok(snapshots);
            }
        }
        Ok(self
            .export_metrics_snapshots()?
            .into_iter()
            .filter(|snapshot| &snapshot.manifest.experiment_id == experiment_id)
            .collect())
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports append-only metrics ledger segments across all loaded experiment revisions.
    pub fn export_metrics_ledger_segments(&self) -> anyhow::Result<Vec<MetricsLedgerSegment>> {
        if let Some(store) = self.metrics_store()? {
            let segments = store.load_ledger_segments()?;
            if !segments.is_empty() || self.metric_envelopes().is_empty() {
                return Ok(segments);
            }
        }
        let mut segments = Vec::new();
        for ((experiment_id, revision_id), indexer) in self.metrics_indexers()? {
            segments.extend(indexer.export_ledger_segments(&experiment_id, &revision_id)?);
        }
        Ok(segments)
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports current per-revision metrics catchup bundles.
    pub fn export_metrics_catchup_bundles(&self) -> anyhow::Result<Vec<MetricsCatchupBundle>> {
        if let Some(store) = self.metrics_store()? {
            let bundles = store.load_catchup_bundles()?;
            if !bundles.is_empty() || self.metric_envelopes().is_empty() {
                return Ok(bundles);
            }
        }
        Ok(build_catchup_bundles(
            self.export_metrics_snapshots()?,
            self.export_metrics_ledger_segments()?,
        ))
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports current metrics catchup bundles for one experiment.
    pub fn export_metrics_catchup_bundles_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<MetricsCatchupBundle>> {
        if let Some(store) = self.metrics_store()? {
            let bundles = store.load_catchup_bundles_for_experiment(experiment_id)?;
            if !bundles.is_empty() || self.metric_envelopes().is_empty() {
                return Ok(bundles);
            }
        }
        Ok(self
            .export_metrics_catchup_bundles()?
            .into_iter()
            .filter(|bundle| &bundle.experiment_id == experiment_id)
            .collect())
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports peer-window distribution summaries across all loaded experiment revisions.
    pub fn export_metrics_peer_window_distributions(
        &self,
    ) -> anyhow::Result<Vec<PeerWindowDistributionSummary>> {
        if let Some(store) = self.metrics_store()? {
            let summaries = store.load_peer_window_distributions()?;
            if !summaries.is_empty() || self.metric_envelopes().is_empty() {
                return Ok(summaries);
            }
        }
        Ok(derive_peer_window_distribution_summaries(
            &self
                .export_metrics_snapshots()?
                .into_iter()
                .flat_map(|snapshot| snapshot.peer_window_metrics.into_iter())
                .collect::<Vec<_>>(),
        ))
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports peer-window distribution summaries for one experiment.
    pub fn export_metrics_peer_window_distributions_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<PeerWindowDistributionSummary>> {
        Ok(self
            .export_metrics_peer_window_distributions()?
            .into_iter()
            .filter(|summary| &summary.experiment_id == experiment_id)
            .collect())
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports one peer-window distribution drilldown for an experiment revision and base head.
    pub fn export_metrics_peer_window_distribution_detail(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        base_head_id: &HeadId,
    ) -> anyhow::Result<Option<PeerWindowDistributionDetail>> {
        if let Some(store) = self.metrics_store()? {
            let detail = store.load_peer_window_distribution_detail(
                experiment_id,
                revision_id,
                base_head_id,
            )?;
            if detail.is_some() || self.metric_envelopes().is_empty() {
                return Ok(detail);
            }
        }
        Ok(derive_peer_window_distribution_detail_with_limit(
            &self.peer_window_metrics,
            experiment_id,
            revision_id,
            base_head_id,
            self.metrics_retention.max_peer_window_detail_windows,
        ))
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports candidate-focused reducer cohorts across all loaded experiment revisions.
    pub fn export_metrics_candidates(&self) -> anyhow::Result<Vec<ReducerCohortMetrics>> {
        let mut cohorts = self
            .export_metrics_snapshots()?
            .into_iter()
            .flat_map(|snapshot| snapshot.reducer_cohort_metrics.into_iter())
            .filter(reducer_cohort_is_candidate_view)
            .collect::<Vec<_>>();
        cohorts.sort_by_key(|metrics| std::cmp::Reverse(metrics.captured_at));
        Ok(cohorts)
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports candidate-focused reducer cohorts for one experiment.
    pub fn export_metrics_candidates_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<ReducerCohortMetrics>> {
        Ok(self
            .export_metrics_candidates()?
            .into_iter()
            .filter(|metrics| &metrics.experiment_id == experiment_id)
            .collect())
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports reducer cohorts that currently indicate disagreement or inconsistency.
    pub fn export_metrics_disagreements(&self) -> anyhow::Result<Vec<ReducerCohortMetrics>> {
        let mut cohorts = self
            .export_metrics_snapshots()?
            .into_iter()
            .flat_map(|snapshot| snapshot.reducer_cohort_metrics.into_iter())
            .filter(reducer_cohort_is_disagreement)
            .collect::<Vec<_>>();
        cohorts.sort_by_key(|metrics| std::cmp::Reverse(metrics.captured_at));
        Ok(cohorts)
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports disagreement-focused reducer cohorts for one experiment.
    pub fn export_metrics_disagreements_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<ReducerCohortMetrics>> {
        Ok(self
            .export_metrics_disagreements()?
            .into_iter()
            .filter(|metrics| &metrics.experiment_id == experiment_id)
            .collect())
    }

    #[cfg(feature = "metrics-indexer")]
    /// Exports the current live metrics refresh event.
    pub fn export_metrics_live_event(
        &self,
        network_id: &NetworkId,
    ) -> anyhow::Result<MetricsLiveEvent> {
        if let Some(event) = self.runtime_snapshot.as_ref().and_then(|snapshot| {
            snapshot
                .control_plane
                .metrics_announcements
                .iter()
                .max_by_key(|announcement| announcement.event.generated_at)
                .map(|announcement| announcement.event.clone())
        }) {
            return Ok(event);
        }

        if let Some(store) = self.metrics_store()?
            && let Some(event) = store.load_live_event(MetricsLiveEventKind::CatchupRefresh)?
        {
            return Ok(event);
        }

        Ok(build_live_event(
            MetricsLiveEventKind::CatchupRefresh,
            &self.export_metrics_snapshots()?,
            &self.export_metrics_ledger_segments()?,
        )
        .unwrap_or(MetricsLiveEvent {
            network_id: network_id.clone(),
            kind: MetricsLiveEventKind::CatchupRefresh,
            cursors: Vec::new(),
            generated_at: Utc::now(),
        }))
    }

    #[cfg(feature = "metrics-indexer")]
    fn metrics_indexers(
        &self,
    ) -> anyhow::Result<BTreeMap<(ExperimentId, RevisionId), MetricsIndexer>> {
        let mut indexers = BTreeMap::<(ExperimentId, RevisionId), MetricsIndexer>::new();

        for metrics in &self.peer_window_metrics {
            indexers
                .entry((metrics.experiment_id.clone(), metrics.revision_id.clone()))
                .or_insert_with(|| {
                    MetricsIndexer::new(metrics_indexer_config_for_budget(self.metrics_retention))
                })
                .ingest_peer_window_metrics(metrics.clone());
        }
        for metrics in &self.reducer_cohort_metrics {
            indexers
                .entry((metrics.experiment_id.clone(), metrics.revision_id.clone()))
                .or_insert_with(|| {
                    MetricsIndexer::new(metrics_indexer_config_for_budget(self.metrics_retention))
                })
                .ingest_reducer_cohort_metrics(metrics.clone());
        }
        for report in &self.head_eval_reports {
            indexers
                .entry((report.experiment_id.clone(), report.revision_id.clone()))
                .or_insert_with(|| {
                    MetricsIndexer::new(metrics_indexer_config_for_budget(self.metrics_retention))
                })
                .ingest_head_eval_report(report.clone());
        }

        Ok(indexers)
    }

    #[cfg(feature = "metrics-indexer")]
    fn metric_envelopes(&self) -> Vec<MetricEnvelope> {
        self.peer_window_metrics
            .iter()
            .cloned()
            .map(MetricEnvelope::PeerWindow)
            .chain(
                self.reducer_cohort_metrics
                    .iter()
                    .cloned()
                    .map(MetricEnvelope::ReducerCohort),
            )
            .chain(
                self.head_eval_reports
                    .iter()
                    .cloned()
                    .map(MetricEnvelope::HeadEval),
            )
            .collect()
    }

    #[cfg(feature = "metrics-indexer")]
    fn metrics_store(&self) -> anyhow::Result<Option<MetricsStore>> {
        self.metrics_store_root
            .as_ref()
            .map(|root| {
                MetricsStore::open(
                    root,
                    metrics_indexer_config_for_budget(self.metrics_retention),
                )
                .map_err(anyhow::Error::from)
            })
            .transpose()
    }

    #[cfg(feature = "metrics-indexer")]
    fn sync_metrics_store(&self) -> anyhow::Result<()> {
        let Some(root) = self.metrics_store_root.as_ref() else {
            return Ok(());
        };
        let mut store = MetricsStore::open(
            root,
            metrics_indexer_config_for_budget(self.metrics_retention),
        )?;
        store.replace_entries(self.metric_envelopes())?;
        store.materialize_views()?;
        Ok(())
    }

    #[cfg(feature = "artifact-publish")]
    fn artifact_store(&self) -> anyhow::Result<Option<burn_p2p::FsArtifactStore>> {
        self.artifact_store_root
            .as_ref()
            .map(|root| {
                let store = burn_p2p::FsArtifactStore::new(root.clone());
                store.ensure_layout().map_err(anyhow::Error::from)?;
                Ok(store)
            })
            .transpose()
    }

    #[cfg(feature = "artifact-publish")]
    fn publication_store(&self) -> anyhow::Result<Option<PublicationStore>> {
        self.publication_store_root
            .as_ref()
            .map(|root| {
                let mut store = PublicationStore::open(root)?;
                store.configure_targets(self.publication_targets.clone())?;
                let protocols = self
                    .eval_protocol_manifests
                    .iter()
                    .map(|record| record.manifest.clone())
                    .collect::<Vec<_>>();
                if let Some(artifact_store) = self.artifact_store()? {
                    store.sync_aliases_and_eager_publications(
                        &artifact_store,
                        &self.head_descriptors,
                        &self.head_eval_reports,
                        &protocols,
                    )?;
                } else {
                    store.sync_aliases(
                        &self.head_descriptors,
                        &self.head_eval_reports,
                        &protocols,
                    )?;
                }
                Ok(store)
            })
            .transpose()
    }

    #[cfg(feature = "artifact-publish")]
    fn sync_publication_store(&self) -> anyhow::Result<()> {
        let Some(root) = self.publication_store_root.as_ref() else {
            return Ok(());
        };
        let mut store = PublicationStore::open(root)?;
        store.configure_targets(self.publication_targets.clone())?;
        let protocols = self
            .eval_protocol_manifests
            .iter()
            .map(|record| record.manifest.clone())
            .collect::<Vec<_>>();
        if let Some(artifact_store) = self.artifact_store()? {
            store.sync_aliases_and_eager_publications(
                &artifact_store,
                &self.head_descriptors,
                &self.head_eval_reports,
                &protocols,
            )?;
        } else {
            store.sync_aliases(&self.head_descriptors, &self.head_eval_reports, &protocols)?;
        }
        store.prune_expired_records()?;
        Ok(())
    }
}

#[cfg(feature = "metrics-indexer")]
fn metrics_indexer_config_for_budget(budget: MetricsRetentionBudget) -> MetricsIndexerConfig {
    MetricsIndexerConfig {
        max_peer_window_entries_per_revision: budget.max_peer_window_entries_per_revision,
        max_reducer_cohort_entries_per_revision: budget.max_reducer_cohort_entries_per_revision,
        max_head_eval_reports_per_revision: budget.max_head_eval_reports_per_revision,
        max_revisions_per_experiment: budget.max_metric_revisions_per_experiment,
        max_peer_window_detail_windows: budget.max_peer_window_detail_windows,
        ..MetricsIndexerConfig::default()
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
    pub preset: BootstrapPreset,
    /// The services.
    pub services: BTreeSet<BootstrapService>,
    /// The roles.
    pub roles: PeerRoleSet,
    /// The swarm.
    pub swarm: SwarmStats,
    /// The pinned heads.
    pub pinned_heads: BTreeSet<HeadId>,
    /// The pinned artifacts.
    pub pinned_artifacts: BTreeSet<ArtifactId>,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an active experiment.
pub struct ActiveExperiment {
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
}

impl ActiveExperiment {
    /// Performs the handle operation.
    pub fn handle(&self, network: &burn_p2p::MainnetHandle) -> ExperimentHandle {
        network.experiment(
            self.study_id.clone(),
            self.experiment_id.clone(),
            self.revision_id.clone(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures bootstrap embedded daemon.
pub struct BootstrapEmbeddedDaemonConfig {
    /// The node.
    pub node: NodeConfig,
    /// The active experiment.
    pub active_experiment: ActiveExperiment,
    /// The initialize head on start.
    pub initialize_head_on_start: bool,
    /// The restore head on start.
    pub restore_head_on_start: bool,
    /// The validation interval millis.
    pub validation_interval_millis: u64,
    /// The training interval millis.
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
                metrics_retention: MetricsRetentionConfig::default(),
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

/// Represents a bootstrap embedded daemon.
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
    /// Performs the plan operation.
    pub fn plan(&self) -> &BootstrapPlan {
        &self.plan
    }

    /// Performs the telemetry operation.
    pub fn telemetry(&self) -> TelemetryHandle {
        self.telemetry.clone()
    }

    /// Performs the control handle operation.
    pub fn control_handle(&self) -> ControlHandle {
        self.control.clone()
    }

    /// Performs the admin state operation.
    pub fn admin_state(&self) -> Arc<Mutex<BootstrapAdminState>> {
        Arc::clone(&self.admin_state)
    }

    /// Performs the diagnostics operation.
    pub fn diagnostics(&self, remaining_work_units: Option<u64>) -> BootstrapDiagnostics {
        self.admin_state
            .lock()
            .expect("bootstrap embedded state should not be poisoned")
            .diagnostics(&self.plan, Utc::now(), remaining_work_units)
    }

    /// Performs the diagnostics bundle operation.
    pub fn diagnostics_bundle(
        &self,
        remaining_work_units: Option<u64>,
    ) -> BootstrapDiagnosticsBundle {
        self.admin_state
            .lock()
            .expect("bootstrap embedded state should not be poisoned")
            .diagnostics_bundle(&self.plan, Utc::now(), remaining_work_units)
    }

    /// Performs the shutdown operation.
    pub fn shutdown(&self) -> anyhow::Result<()> {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        let _ = self.control.shutdown();
        Ok(())
    }

    /// Performs the await termination operation.
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
    /// Performs the spawn embedded daemon operation.
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

#[cfg(feature = "portal")]
fn portal_snapshot_view(snapshot: &BrowserPortalSnapshot) -> burn_p2p_portal::PortalSnapshotView {
    portal_snapshot_view_with_metrics_and_artifacts(snapshot, Vec::new(), Vec::new())
}

#[cfg(feature = "portal")]
fn portal_snapshot_view_with_metrics(
    snapshot: &BrowserPortalSnapshot,
    metrics_panels: Vec<burn_p2p_portal::PortalMetricsPanel>,
) -> burn_p2p_portal::PortalSnapshotView {
    portal_snapshot_view_with_metrics_and_artifacts(snapshot, metrics_panels, Vec::new())
}

#[cfg(feature = "portal")]
fn portal_snapshot_view_with_metrics_and_artifacts(
    snapshot: &BrowserPortalSnapshot,
    metrics_panels: Vec<burn_p2p_portal::PortalMetricsPanel>,
    artifact_rows: Vec<burn_p2p_portal::PortalArtifactRow>,
) -> burn_p2p_portal::PortalSnapshotView {
    burn_p2p_portal::PortalSnapshotView {
        network_id: snapshot.network_id.as_str().to_owned(),
        auth_enabled: snapshot.auth_enabled,
        edge_mode: format!("{:?}", snapshot.edge_mode),
        browser_mode: format!("{:?}", snapshot.browser_mode),
        social_enabled: snapshot.social_mode != SocialMode::Disabled,
        profile_enabled: snapshot.profile_mode != ProfileMode::Disabled,
        login_providers: snapshot
            .login_providers
            .iter()
            .map(|provider| burn_p2p_portal::PortalLoginProvider {
                label: provider.label.clone(),
                login_path: provider.login_path.clone(),
                callback_path: provider.callback_path.clone(),
                device_path: provider.device_path.clone(),
            })
            .collect(),
        transports: burn_p2p_portal::PortalTransportSurface {
            webrtc_direct: snapshot.transports.webrtc_direct,
            webtransport_gateway: snapshot.transports.webtransport_gateway,
            wss_fallback: snapshot.transports.wss_fallback,
        },
        paths: burn_p2p_portal::PortalPaths {
            portal_snapshot_path: snapshot.paths.portal_snapshot_path.clone(),
            signed_directory_path: snapshot.paths.signed_directory_path.clone(),
            signed_leaderboard_path: snapshot.paths.signed_leaderboard_path.clone(),
            artifacts_aliases_path: snapshot.paths.artifacts_aliases_path.clone(),
            artifacts_export_path: snapshot.paths.artifacts_export_path.clone(),
            artifacts_download_ticket_path: snapshot.paths.artifacts_download_ticket_path.clone(),
            trust_bundle_path: snapshot.paths.trust_bundle_path.clone(),
        },
        diagnostics: burn_p2p_portal::PortalDiagnosticsView {
            connected_peers: snapshot.diagnostics.swarm.connected_peers as usize,
            admitted_peers: snapshot.diagnostics.admitted_peers.len(),
            rejected_peers: snapshot.diagnostics.rejected_peers.len(),
            quarantined_peers: snapshot.diagnostics.quarantined_peers.len(),
            accepted_receipts: snapshot.diagnostics.accepted_receipts,
            certified_merges: snapshot.diagnostics.certified_merges,
            active_services: snapshot
                .diagnostics
                .services
                .iter()
                .map(|service| format!("{service:?}"))
                .collect(),
        },
        trust: burn_p2p_portal::PortalTrustView {
            required_release_train_hash: snapshot
                .required_release_train_hash
                .as_ref()
                .map(ContentId::as_str)
                .map(ToOwned::to_owned),
            approved_target_artifact_count: snapshot.allowed_target_artifact_hashes.len(),
            active_issuer_peer_id: snapshot
                .trust_bundle
                .as_ref()
                .map(|bundle| bundle.active_issuer_peer_id.as_str().to_owned()),
            minimum_revocation_epoch: snapshot
                .trust_bundle
                .as_ref()
                .map(|bundle| bundle.minimum_revocation_epoch.0),
            reenrollment_required: snapshot
                .trust_bundle
                .as_ref()
                .and_then(|bundle| bundle.reenrollment.as_ref())
                .is_some(),
        },
        experiments: snapshot
            .directory
            .entries
            .iter()
            .map(|entry| burn_p2p_portal::PortalExperimentRow {
                display_name: entry.display_name.clone(),
                experiment_id: entry.experiment_id.as_str().to_owned(),
                revision_id: entry.current_revision_id.as_str().to_owned(),
                has_head: entry.current_head_id.is_some(),
                estimated_window_seconds: entry.resource_requirements.estimated_window_seconds,
            })
            .collect(),
        heads: snapshot
            .heads
            .iter()
            .map(|head| burn_p2p_portal::PortalHeadRow {
                experiment_id: head.experiment_id.as_str().to_owned(),
                revision_id: head.revision_id.as_str().to_owned(),
                head_id: head.head_id.as_str().to_owned(),
                global_step: head.global_step,
                created_at: head.created_at.to_rfc3339(),
            })
            .collect(),
        leaderboard: snapshot
            .leaderboard
            .entries
            .iter()
            .map(|entry| burn_p2p_portal::PortalLeaderboardRow {
                principal_label: entry
                    .identity
                    .principal_id
                    .as_ref()
                    .map(PrincipalId::as_str)
                    .unwrap_or("anonymous")
                    .to_owned(),
                leaderboard_score_v1: entry.leaderboard_score_v1,
                accepted_receipt_count: entry.accepted_receipt_count as usize,
            })
            .collect(),
        artifact_rows,
        metrics_panels,
    }
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_artifact_rows(
    snapshot: &BrowserPortalSnapshot,
    artifact_aliases: &[ArtifactAliasStatus],
) -> Vec<burn_p2p_portal::PortalArtifactRow> {
    artifact_aliases
        .iter()
        .map(|row| {
            let status = row
                .published_artifact
                .as_ref()
                .map(|record| format!("{:?}", record.status))
                .or_else(|| {
                    row.latest_job
                        .as_ref()
                        .map(|job| format!("{:?}", job.status))
                })
                .unwrap_or_else(|| "NotPublished".into());
            let publication_target_id = row
                .published_artifact
                .as_ref()
                .map(|record| record.publication_target_id.as_str().to_owned())
                .or_else(|| {
                    row.latest_job
                        .as_ref()
                        .map(|job| job.publication_target_id.as_str().to_owned())
                })
                .unwrap_or_else(|| DEFAULT_PUBLICATION_TARGET_ID.to_owned());
            burn_p2p_portal::PortalArtifactRow {
                alias_name: row.alias.alias_name.clone(),
                scope: format!("{:?}", row.alias.scope),
                artifact_profile: format!("{:?}", row.alias.artifact_profile),
                experiment_id: row.alias.experiment_id.as_str().to_owned(),
                run_id: row
                    .alias
                    .run_id
                    .as_ref()
                    .map(|run_id| run_id.as_str().to_owned()),
                head_id: row.alias.head_id.as_str().to_owned(),
                publication_target_id,
                artifact_alias_id: Some(row.alias.artifact_alias_id.as_str().to_owned()),
                status,
                last_published_at: row
                    .published_artifact
                    .as_ref()
                    .map(|record| record.created_at.to_rfc3339()),
                history_count: row.history_count,
                previous_head_id: row
                    .previous_head_id
                    .as_ref()
                    .map(|head_id| head_id.as_str().to_owned()),
                head_view_path: format!("/portal/artifacts/heads/{}", row.alias.head_id.as_str()),
                run_view_path: row.alias.run_id.as_ref().map(|run_id| {
                    format!(
                        "/portal/artifacts/runs/{}/{}",
                        row.alias.experiment_id.as_str(),
                        run_id.as_str()
                    )
                }),
                export_path: snapshot.paths.artifacts_export_path.clone(),
                download_ticket_path: snapshot.paths.artifacts_download_ticket_path.clone(),
            }
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn format_artifact_metric_value(value: &burn_p2p_core::MetricValue) -> String {
    match value {
        burn_p2p_core::MetricValue::Integer(value) => value.to_string(),
        burn_p2p_core::MetricValue::Float(value) => format!("{value:.4}"),
        burn_p2p_core::MetricValue::Bool(value) => value.to_string(),
        burn_p2p_core::MetricValue::Text(value) => value.clone(),
    }
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_alias_history_rows(
    alias_history: &[burn_p2p_core::ArtifactAlias],
) -> Vec<burn_p2p_portal::PortalArtifactAliasHistoryRow> {
    alias_history
        .iter()
        .map(|alias| burn_p2p_portal::PortalArtifactAliasHistoryRow {
            alias_name: alias.alias_name.clone(),
            scope: format!("{:?}", alias.scope),
            artifact_profile: format!("{:?}", alias.artifact_profile),
            head_id: alias.head_id.as_str().to_owned(),
            resolved_at: alias.resolved_at.to_rfc3339(),
            source_reason: format!("{:?}", alias.source_reason),
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_published_artifact_rows(
    publications: &[burn_p2p_core::PublishedArtifactRecord],
) -> Vec<burn_p2p_portal::PortalPublishedArtifactRow> {
    publications
        .iter()
        .map(|record| burn_p2p_portal::PortalPublishedArtifactRow {
            head_id: record.head_id.as_str().to_owned(),
            artifact_profile: format!("{:?}", record.artifact_profile),
            publication_target_id: record.publication_target_id.as_str().to_owned(),
            status: format!("{:?}", record.status),
            object_key: record.object_key.clone(),
            content_length: record.content_length,
            created_at: record.created_at.to_rfc3339(),
            expires_at: record.expires_at.map(|timestamp| timestamp.to_rfc3339()),
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_eval_summary_rows(
    eval_reports: &[HeadEvalReport],
) -> Vec<burn_p2p_portal::PortalHeadEvalSummaryRow> {
    eval_reports
        .iter()
        .map(|report| burn_p2p_portal::PortalHeadEvalSummaryRow {
            head_id: report.head_id.as_str().to_owned(),
            eval_protocol_id: report.eval_protocol_id.as_str().to_owned(),
            status: format!("{:?}", report.status),
            dataset_view_id: report.dataset_view_id.as_str().to_owned(),
            sample_count: report.sample_count,
            metric_summary: if report.metric_values.is_empty() {
                "n/a".into()
            } else {
                report
                    .metric_values
                    .iter()
                    .map(|(label, value)| {
                        format!("{label}={}", format_artifact_metric_value(value))
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            },
            finished_at: report.finished_at.to_rfc3339(),
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_artifact_run_summary_rows(
    summaries: &[ArtifactRunSummary],
) -> Vec<burn_p2p_portal::PortalArtifactRunSummaryRow> {
    summaries
        .iter()
        .map(|summary| burn_p2p_portal::PortalArtifactRunSummaryRow {
            experiment_id: summary.experiment_id.as_str().to_owned(),
            run_id: summary.run_id.as_str().to_owned(),
            latest_head_id: summary.latest_head_id.as_str().to_owned(),
            alias_count: summary.alias_count,
            alias_history_count: summary.alias_history_count,
            published_artifact_count: summary.published_artifact_count,
            run_view_path: format!(
                "/portal/artifacts/runs/{}/{}",
                summary.experiment_id.as_str(),
                summary.run_id.as_str()
            ),
            json_view_path: format!(
                "/artifacts/runs/{}/{}",
                summary.experiment_id.as_str(),
                summary.run_id.as_str()
            ),
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_artifact_run_view(view: &ArtifactRunView) -> burn_p2p_portal::PortalArtifactRunView {
    let alias_rows = view
        .aliases
        .iter()
        .map(|row| {
            let status = row
                .published_artifact
                .as_ref()
                .map(|record| format!("{:?}", record.status))
                .or_else(|| {
                    row.latest_job
                        .as_ref()
                        .map(|job| format!("{:?}", job.status))
                })
                .unwrap_or_else(|| "NotPublished".into());
            let publication_target_id = row
                .published_artifact
                .as_ref()
                .map(|record| record.publication_target_id.as_str().to_owned())
                .or_else(|| {
                    row.latest_job
                        .as_ref()
                        .map(|job| job.publication_target_id.as_str().to_owned())
                })
                .unwrap_or_else(|| DEFAULT_PUBLICATION_TARGET_ID.to_owned());
            burn_p2p_portal::PortalArtifactRow {
                alias_name: row.alias.alias_name.clone(),
                scope: format!("{:?}", row.alias.scope),
                artifact_profile: format!("{:?}", row.alias.artifact_profile),
                experiment_id: row.alias.experiment_id.as_str().to_owned(),
                run_id: row
                    .alias
                    .run_id
                    .as_ref()
                    .map(|run_id| run_id.as_str().to_owned()),
                head_id: row.alias.head_id.as_str().to_owned(),
                publication_target_id,
                artifact_alias_id: Some(row.alias.artifact_alias_id.as_str().to_owned()),
                status,
                last_published_at: row
                    .published_artifact
                    .as_ref()
                    .map(|record| record.created_at.to_rfc3339()),
                history_count: row.history_count,
                previous_head_id: row
                    .previous_head_id
                    .as_ref()
                    .map(|head_id| head_id.as_str().to_owned()),
                head_view_path: format!("/portal/artifacts/heads/{}", row.alias.head_id.as_str()),
                run_view_path: row.alias.run_id.as_ref().map(|run_id| {
                    format!(
                        "/portal/artifacts/runs/{}/{}",
                        row.alias.experiment_id.as_str(),
                        run_id.as_str()
                    )
                }),
                export_path: "/artifacts/export".into(),
                download_ticket_path: "/artifacts/download-ticket".into(),
            }
        })
        .collect();
    burn_p2p_portal::PortalArtifactRunView {
        experiment_id: view.experiment_id.as_str().to_owned(),
        run_id: view.run_id.as_str().to_owned(),
        latest_head_id: view
            .heads
            .iter()
            .max_by_key(|head| head.created_at)
            .map(|head| head.head_id.as_str().to_owned()),
        heads: view
            .heads
            .iter()
            .map(|head| burn_p2p_portal::PortalHeadRow {
                experiment_id: head.experiment_id.as_str().to_owned(),
                revision_id: head.revision_id.as_str().to_owned(),
                head_id: head.head_id.as_str().to_owned(),
                global_step: head.global_step,
                created_at: head.created_at.to_rfc3339(),
            })
            .collect(),
        aliases: alias_rows,
        alias_history: portal_alias_history_rows(&view.alias_history),
        eval_reports: portal_eval_summary_rows(&view.eval_reports),
        publications: portal_published_artifact_rows(&view.published_artifacts),
        portal_path: "/portal".into(),
        json_view_path: format!(
            "/artifacts/runs/{}/{}",
            view.experiment_id.as_str(),
            view.run_id.as_str()
        ),
    }
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
fn portal_head_artifact_view(view: &HeadArtifactView) -> burn_p2p_portal::PortalHeadArtifactView {
    let artifact_rows = view
        .aliases
        .iter()
        .map(|alias| burn_p2p_portal::PortalArtifactRow {
            alias_name: alias.alias_name.clone(),
            scope: format!("{:?}", alias.scope),
            artifact_profile: format!("{:?}", alias.artifact_profile),
            experiment_id: alias.experiment_id.as_str().to_owned(),
            run_id: alias
                .run_id
                .as_ref()
                .map(|run_id| run_id.as_str().to_owned()),
            head_id: alias.head_id.as_str().to_owned(),
            publication_target_id: DEFAULT_PUBLICATION_TARGET_ID.to_owned(),
            artifact_alias_id: Some(alias.artifact_alias_id.as_str().to_owned()),
            status: "Resolved".into(),
            last_published_at: None,
            history_count: 0,
            previous_head_id: None,
            head_view_path: format!("/portal/artifacts/heads/{}", alias.head_id.as_str()),
            run_view_path: alias.run_id.as_ref().map(|run_id| {
                format!(
                    "/portal/artifacts/runs/{}/{}",
                    alias.experiment_id.as_str(),
                    run_id.as_str()
                )
            }),
            export_path: "/artifacts/export".into(),
            download_ticket_path: "/artifacts/download-ticket".into(),
        })
        .collect();
    burn_p2p_portal::PortalHeadArtifactView {
        head: burn_p2p_portal::PortalHeadRow {
            experiment_id: view.head.experiment_id.as_str().to_owned(),
            revision_id: view.head.revision_id.as_str().to_owned(),
            head_id: view.head.head_id.as_str().to_owned(),
            global_step: view.head.global_step,
            created_at: view.head.created_at.to_rfc3339(),
        },
        experiment_id: view.head.experiment_id.as_str().to_owned(),
        run_id: view.run_id.as_str().to_owned(),
        aliases: artifact_rows,
        alias_history: portal_alias_history_rows(&view.alias_history),
        eval_reports: portal_eval_summary_rows(&view.eval_reports),
        publications: portal_published_artifact_rows(&view.published_artifacts),
        available_profiles: view
            .available_profiles
            .iter()
            .map(|profile| format!("{profile:?}"))
            .collect(),
        portal_path: "/portal".into(),
        run_view_path: format!(
            "/portal/artifacts/runs/{}/{}",
            view.head.experiment_id.as_str(),
            view.run_id.as_str()
        ),
        json_view_path: format!("/artifacts/heads/{}", view.head.head_id.as_str()),
    }
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_quality(
    snapshot: &MetricsSnapshot,
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    let Some(report) = snapshot
        .head_eval_reports
        .iter()
        .filter(|report| report.status == burn_p2p_core::HeadEvalStatus::Completed)
        .max_by_key(|report| report.finished_at)
    else {
        return Vec::new();
    };

    report
        .metric_values
        .iter()
        .map(|(label, value)| burn_p2p_portal::PortalMetricRow {
            label: format!(
                "{} {}",
                report.experiment_id.as_str(),
                label.replace('_', " ")
            ),
            value: format_metric_value(value),
            scope: "head".into(),
            trust: format!("{:?}", report.trust_class).to_ascii_lowercase(),
            key: report.head_id.as_str().to_owned(),
            protocol: Some(report.eval_protocol_id.as_str().to_owned()),
            operator_hint: None,
            detail_path: None,
            freshness: report.finished_at.to_rfc3339(),
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_derived_kinds(
    snapshots: &[MetricsSnapshot],
    kinds: &[DerivedMetricKind],
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    let mut rows = Vec::new();
    for snapshot in snapshots {
        for kind in kinds {
            if let Some(point) = snapshot
                .derived_metrics
                .iter()
                .find(|point| &point.metric == kind)
            {
                rows.push(burn_p2p_portal::PortalMetricRow {
                    label: format!(
                        "{} {}",
                        point.experiment_id.as_str(),
                        derived_metric_label(kind)
                    ),
                    value: format!("{:.4}", point.value),
                    scope: format!("{:?}", point.scope).to_ascii_lowercase(),
                    trust: format!("{:?}", point.trust).to_ascii_lowercase(),
                    key: point
                        .canonical_head_id
                        .as_ref()
                        .or(point.candidate_head_id.as_ref())
                        .or(point.base_head_id.as_ref())
                        .map(|id| id.as_str().to_owned())
                        .unwrap_or_else(|| point.experiment_id.as_str().to_owned()),
                    protocol: None,
                    operator_hint: None,
                    detail_path: None,
                    freshness: point.captured_at.to_rfc3339(),
                });
            }
        }
    }
    rows
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_peer_window_distributions(
    summaries: &[PeerWindowDistributionSummary],
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    summaries
        .iter()
        .map(|summary| burn_p2p_portal::PortalMetricRow {
            label: format!(
                "{} peer windows @ {}",
                summary.experiment_id.as_str(),
                summary.base_head_id.as_str()
            ),
            value: format!(
                "loss p10/p50/p90 {} · compute ms {:.0}/{:.0}/{:.0} · fetch ms {:.0}/{:.0}/{:.0} · publish ms {:.0}/{:.0}/{:.0} · lag {:.0}/{:.0}/{:.0} · accepted {}/{} ({:.2}) · roles {} · backends {}",
                format_optional_triplet(
                    summary.local_train_loss_mean_p10,
                    summary.local_train_loss_mean_p50,
                    summary.local_train_loss_mean_p90,
                ),
                summary.compute_time_ms_p10,
                summary.compute_time_ms_p50,
                summary.compute_time_ms_p90,
                summary.data_fetch_time_ms_p10,
                summary.data_fetch_time_ms_p50,
                summary.data_fetch_time_ms_p90,
                summary.publish_latency_ms_p10,
                summary.publish_latency_ms_p50,
                summary.publish_latency_ms_p90,
                summary.head_lag_at_finish_p10,
                summary.head_lag_at_finish_p50,
                summary.head_lag_at_finish_p90,
                summary.accepted_windows,
                summary.sample_count,
                summary.acceptance_ratio,
                format_breakdown(&summary.role_counts),
                format_breakdown(&summary.backend_counts),
            ),
            scope: "peer".into(),
            trust: "derived".into(),
            key: summary.base_head_id.as_str().to_owned(),
            protocol: None,
            operator_hint: peer_window_distribution_operator_hint(summary),
            detail_path: Some(format!(
                "/metrics/peer-windows/{}/{}/{}",
                summary.experiment_id.as_str(),
                summary.revision_id.as_str(),
                summary.base_head_id.as_str(),
            )),
            freshness: summary.captured_at.to_rfc3339(),
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_health(
    snapshot: &BrowserPortalSnapshot,
    metrics_snapshots: &[MetricsSnapshot],
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    let mut rows = vec![
        burn_p2p_portal::PortalMetricRow {
            label: "Connected peers".into(),
            value: snapshot.diagnostics.swarm.connected_peers.to_string(),
            scope: "network".into(),
            trust: "derived".into(),
            key: snapshot.network_id.as_str().to_owned(),
            protocol: None,
            operator_hint: None,
            detail_path: None,
            freshness: snapshot.diagnostics.captured_at.to_rfc3339(),
        },
        burn_p2p_portal::PortalMetricRow {
            label: "Accepted receipts".into(),
            value: snapshot.diagnostics.accepted_receipts.to_string(),
            scope: "network".into(),
            trust: "derived".into(),
            key: snapshot.network_id.as_str().to_owned(),
            protocol: None,
            operator_hint: None,
            detail_path: None,
            freshness: snapshot.diagnostics.captured_at.to_rfc3339(),
        },
        burn_p2p_portal::PortalMetricRow {
            label: "Certified merges".into(),
            value: snapshot.diagnostics.certified_merges.to_string(),
            scope: "network".into(),
            trust: "derived".into(),
            key: snapshot.network_id.as_str().to_owned(),
            protocol: None,
            operator_hint: None,
            detail_path: None,
            freshness: snapshot.diagnostics.captured_at.to_rfc3339(),
        },
    ];
    rows.extend(portal_metric_rows_for_derived_kinds(
        metrics_snapshots,
        &[
            DerivedMetricKind::ValidationServiceRate,
            DerivedMetricKind::BytesPerAcceptedToken,
        ],
    ));
    rows
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_candidates(
    snapshot: &MetricsSnapshot,
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    let eval_by_head = snapshot
        .head_eval_reports
        .iter()
        .map(|report| (report.head_id.clone(), report))
        .collect::<BTreeMap<_, _>>();
    let mut cohorts = snapshot.reducer_cohort_metrics.iter().collect::<Vec<_>>();
    cohorts.sort_by_key(|metrics| std::cmp::Reverse(metrics.captured_at));

    cohorts
        .into_iter()
        .filter(|metrics| reducer_cohort_is_candidate_view(metrics))
        .map(|metrics| {
            let eval_report = metrics
                .candidate_head_id
                .as_ref()
                .and_then(|head_id| eval_by_head.get(head_id))
                .copied();
            let subject_key = metrics
                .candidate_head_id
                .as_ref()
                .map(HeadId::as_str)
                .unwrap_or_else(|| metrics.merge_window_id.as_str());
            let eval_suffix = eval_report
                .map(|report| format!(" · eval {:?}", report.status).to_ascii_lowercase())
                .unwrap_or_else(|| " · eval pending".into());
            let agreement_suffix = metrics
                .replica_agreement
                .map(|value| format!(" · agreement {:.2}", value))
                .unwrap_or_default();
            let late_suffix = metrics
                .late_arrival_count
                .map(|count| format!(" · late {}", count))
                .unwrap_or_default();
            let missing_suffix = metrics
                .missing_peer_count
                .map(|count| format!(" · missing {}", count))
                .unwrap_or_default();
            burn_p2p_portal::PortalMetricRow {
                label: metrics
                    .candidate_head_id
                    .as_ref()
                    .map(|head_id| {
                        format!(
                            "{} candidate {}",
                            snapshot.manifest.experiment_id.as_str(),
                            head_id.as_str()
                        )
                    })
                    .unwrap_or_else(|| {
                        format!(
                            "{} cohort {}",
                            snapshot.manifest.experiment_id.as_str(),
                            metrics.merge_window_id.as_str()
                        )
                    }),
                value: format!(
                    "status {} · base {} · accepted {}/{} · rejected {} · tokens {} · stale {:.2}/{:.2} · load {:.2}{}{}{}{}",
                    format!("{:?}", metrics.status).to_ascii_lowercase(),
                    metrics.base_head_id.as_str(),
                    metrics.accepted_updates,
                    metrics.received_updates,
                    metrics.rejected_updates,
                    metrics.accepted_tokens_or_samples,
                    metrics.staleness_mean,
                    metrics.staleness_max,
                    metrics.reducer_load,
                    agreement_suffix,
                    late_suffix,
                    missing_suffix,
                    eval_suffix,
                ),
                scope: "cohort".into(),
                trust: format!("{:?}", metrics.trust_class()).to_ascii_lowercase(),
                key: subject_key.to_owned(),
                protocol: eval_report.map(|report| report.eval_protocol_id.as_str().to_owned()),
                operator_hint: Some(candidate_operator_hint(metrics)),
                detail_path: Some(format!(
                    "/metrics/candidates/{}",
                    snapshot.manifest.experiment_id.as_str()
                )),
                freshness: metrics.captured_at.to_rfc3339(),
            }
        })
        .collect()
}

#[cfg(feature = "metrics-indexer")]
fn reducer_cohort_is_candidate_view(metrics: &ReducerCohortMetrics) -> bool {
    metrics.candidate_head_id.is_some()
        || metrics.status != burn_p2p_core::ReducerCohortStatus::Closed
}

#[cfg(feature = "metrics-indexer")]
fn reducer_cohort_is_disagreement(metrics: &ReducerCohortMetrics) -> bool {
    metrics.status == burn_p2p_core::ReducerCohortStatus::Inconsistent
        || metrics
            .replica_agreement
            .map(|agreement| agreement < 0.999)
            .unwrap_or(false)
        || metrics.late_arrival_count.unwrap_or(0) > 0
        || metrics.missing_peer_count.unwrap_or(0) > 0
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn candidate_operator_hint(metrics: &ReducerCohortMetrics) -> String {
    match metrics.status {
        burn_p2p_core::ReducerCohortStatus::Open => {
            "Window still open; watch late arrivals before judging the branch.".into()
        }
        burn_p2p_core::ReducerCohortStatus::Closed => {
            "Closed cohort; wait for validator evaluation or supersession before treating it as final.".into()
        }
        burn_p2p_core::ReducerCohortStatus::Inconsistent => {
            "Hold promotion and inspect reducer disagreement before trusting this candidate.".into()
        }
        burn_p2p_core::ReducerCohortStatus::Promoted => {
            "Candidate already won promotion; compare adoption lag against peers still training on the base head.".into()
        }
        burn_p2p_core::ReducerCohortStatus::Superseded => {
            "Superseded branch; compare against the newer winning candidate before retrying the same merge path.".into()
        }
        burn_p2p_core::ReducerCohortStatus::Rejected => {
            let primary_reason = metrics
                .rejection_reasons
                .iter()
                .max_by_key(|(_, count)| *count)
                .map(|(reason, count)| format!("{reason} ({count})"))
                .unwrap_or_else(|| "unspecified".into());
            format!("Rejected branch; inspect the dominant rejection reason: {primary_reason}.")
        }
    }
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn disagreement_operator_hint(metrics: &ReducerCohortMetrics) -> String {
    if metrics
        .replica_agreement
        .map(|value| value < 0.999)
        .unwrap_or(false)
    {
        "Reducer replicas diverged; compare reducer inputs before trusting any single aggregate."
            .into()
    } else if metrics.missing_peer_count.unwrap_or(0) > 0 {
        "Expected peers were missing; check reducer reachability and peer liveness before promotion.".into()
    } else if metrics.late_arrival_count.unwrap_or(0) > 0 {
        "Late arrivals exceeded the clean-close window; inspect network lag or increase the close delay.".into()
    } else {
        "Reducer disagreement needs operator review before these numbers are treated as settled truth.".into()
    }
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn peer_window_distribution_operator_hint(
    summary: &PeerWindowDistributionSummary,
) -> Option<String> {
    if summary.sample_count < 3 {
        Some(
            "Small peer-window sample; treat the spread as directional rather than settled.".into(),
        )
    } else if summary.head_lag_at_finish_p90 > 4.0 {
        Some(
            "Peers were finishing far behind the frontier; compare this row against adoption lag and branch disagreement before widening rollout."
                .into(),
        )
    } else if summary.acceptance_ratio < 0.5 {
        Some(
            "Less than half of attempted work was accepted; inspect rejection reasons and reducer disagreement before trusting the apparent loss spread."
                .into(),
        )
    } else {
        None
    }
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metric_rows_for_disagreements(
    snapshot: &MetricsSnapshot,
) -> Vec<burn_p2p_portal::PortalMetricRow> {
    let mut cohorts = snapshot
        .reducer_cohort_metrics
        .iter()
        .filter(|metrics| reducer_cohort_is_disagreement(metrics))
        .collect::<Vec<_>>();
    cohorts.sort_by_key(|metrics| std::cmp::Reverse(metrics.captured_at));

    cohorts
        .into_iter()
        .map(|metrics| burn_p2p_portal::PortalMetricRow {
            label: format!(
                "{} reducer {}",
                snapshot.manifest.experiment_id.as_str(),
                metrics.reducer_group_id.as_str()
            ),
            value: format!(
                "status {} · candidate {} · agreement {} · late {} · missing {} · rejected {} · bytes {}/{}",
                format!("{:?}", metrics.status).to_ascii_lowercase(),
                metrics
                    .candidate_head_id
                    .as_ref()
                    .map(HeadId::as_str)
                    .unwrap_or("-"),
                metrics
                    .replica_agreement
                    .map(|value| format!("{value:.2}"))
                    .unwrap_or_else(|| "n/a".into()),
                metrics.late_arrival_count.unwrap_or(0),
                metrics.missing_peer_count.unwrap_or(0),
                metrics.rejected_updates,
                metrics.ingress_bytes,
                metrics.egress_bytes,
            ),
            scope: "cohort".into(),
            trust: format!("{:?}", metrics.trust_class()).to_ascii_lowercase(),
            key: metrics.merge_window_id.as_str().to_owned(),
            protocol: None,
            operator_hint: Some(disagreement_operator_hint(metrics)),
            detail_path: Some(format!(
                "/metrics/disagreements/{}",
                snapshot.manifest.experiment_id.as_str()
            )),
            freshness: metrics.captured_at.to_rfc3339(),
        })
        .collect()
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn portal_metrics_panels(
    snapshot: &BrowserPortalSnapshot,
    metrics_snapshots: &[MetricsSnapshot],
) -> Vec<burn_p2p_portal::PortalMetricsPanel> {
    let peer_window_distributions = derive_peer_window_distribution_summaries(
        &metrics_snapshots
            .iter()
            .flat_map(|snapshot| snapshot.peer_window_metrics.iter().cloned())
            .collect::<Vec<_>>(),
    );
    vec![
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "quality".into(),
            title: "Canonical quality".into(),
            description:
                "Validator-backed head metrics. These are the comparable quality signals for decentralized training."
                    .into(),
            rows: metrics_snapshots
                .iter()
                .flat_map(portal_metric_rows_for_quality)
                .collect(),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "progress".into(),
            title: "Global progress".into(),
            description:
                "Accepted work and effective progress. These summarize network output rather than pretending there is one global train loss line."
                    .into(),
            rows: portal_metric_rows_for_derived_kinds(
                metrics_snapshots,
                &[
                    DerivedMetricKind::AcceptedTokensPerSec,
                    DerivedMetricKind::AcceptedUpdatesPerHour,
                    DerivedMetricKind::EffectiveEpochProgress,
                    DerivedMetricKind::UniqueSampleCoverageEstimate,
                    DerivedMetricKind::AcceptanceRatio,
                ],
            ),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "dynamics".into(),
            title: "Decentralized dynamics".into(),
            description:
                "Lag, staleness, reducer agreement, and branch signals for the asynchronous training regime."
                    .into(),
            rows: portal_metric_rows_for_derived_kinds(
                metrics_snapshots,
                &[
                    DerivedMetricKind::CanonicalHeadCadence,
                    DerivedMetricKind::StaleWorkFraction,
                    DerivedMetricKind::MeanHeadLag,
                    DerivedMetricKind::MaxHeadLag,
                    DerivedMetricKind::HeadAdoptionLagP50,
                    DerivedMetricKind::HeadAdoptionLagP90,
                    DerivedMetricKind::ReducerReplicaAgreement,
                    DerivedMetricKind::MergeWindowSkew,
                    DerivedMetricKind::CandidateBranchFactor,
                ],
            ),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "peer-windows".into(),
            title: "Peer-window distributions".into(),
            description:
                "Local train-loss, step-time, fetch-latency, publish-latency, and lag spreads grouped by base head instead of being flattened into a fake global train-loss line."
                    .into(),
            rows: portal_metric_rows_for_peer_window_distributions(&peer_window_distributions),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "branches".into(),
            title: "Branch candidates".into(),
            description:
                "Provisional reducer cohorts and candidate heads. These rows keep branch provenance, reducer disagreement, and candidate status visible instead of flattening them into one global number."
                    .into(),
            rows: metrics_snapshots
                .iter()
                .flat_map(portal_metric_rows_for_candidates)
                .collect(),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "disagreement".into(),
            title: "Reducer disagreement".into(),
            description:
                "Reducer replicas, late arrivals, and missing-peer signals that need operator attention before they get mistaken for settled global truth."
                    .into(),
            rows: metrics_snapshots
                .iter()
                .flat_map(portal_metric_rows_for_disagreements)
                .collect(),
        },
        burn_p2p_portal::PortalMetricsPanel {
            panel_id: "health".into(),
            title: "Network health".into(),
            description:
                "Operational state for the current edge, augmented with validation service rate from the metrics plane."
                    .into(),
            rows: portal_metric_rows_for_health(snapshot, metrics_snapshots),
        },
    ]
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn derived_metric_label(kind: &DerivedMetricKind) -> &'static str {
    match kind {
        DerivedMetricKind::AcceptedTokensPerSec => "accepted tokens per sec",
        DerivedMetricKind::AcceptedUpdatesPerHour => "accepted updates per hour",
        DerivedMetricKind::EffectiveEpochProgress => "effective epoch progress",
        DerivedMetricKind::UniqueSampleCoverageEstimate => "unique sample coverage",
        DerivedMetricKind::CanonicalHeadCadence => "canonical head cadence",
        DerivedMetricKind::StaleWorkFraction => "stale work fraction",
        DerivedMetricKind::MeanHeadLag => "mean head lag",
        DerivedMetricKind::MaxHeadLag => "max head lag",
        DerivedMetricKind::ReducerReplicaAgreement => "reducer replica agreement",
        DerivedMetricKind::MergeWindowSkew => "merge window skew",
        DerivedMetricKind::CandidateBranchFactor => "candidate branch factor",
        DerivedMetricKind::HeadAdoptionLagP50 => "head adoption lag p50",
        DerivedMetricKind::HeadAdoptionLagP90 => "head adoption lag p90",
        DerivedMetricKind::AcceptanceRatio => "acceptance ratio",
        DerivedMetricKind::RejectionRatioByReason => "rejection ratio by reason",
        DerivedMetricKind::BytesPerAcceptedToken => "bytes per accepted token",
        DerivedMetricKind::ValidationServiceRate => "validation service rate",
    }
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn format_optional_triplet(p10: Option<f64>, p50: Option<f64>, p90: Option<f64>) -> String {
    match (p10, p50, p90) {
        (Some(p10), Some(p50), Some(p90)) => format!("{p10:.4}/{p50:.4}/{p90:.4}"),
        _ => "n/a".into(),
    }
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn format_breakdown(values: &BTreeMap<String, usize>) -> String {
    if values.is_empty() {
        return "n/a".into();
    }
    values
        .iter()
        .map(|(label, count)| format!("{label}:{count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
fn format_metric_value(value: &burn_p2p_core::MetricValue) -> String {
    match value {
        burn_p2p_core::MetricValue::Integer(value) => value.to_string(),
        burn_p2p_core::MetricValue::Float(value) => format!("{value:.4}"),
        burn_p2p_core::MetricValue::Bool(value) => value.to_string(),
        burn_p2p_core::MetricValue::Text(value) => value.clone(),
    }
}

#[cfg(feature = "portal")]
/// Performs the render dashboard html operation.
pub fn render_dashboard_html(network_id: &NetworkId) -> String {
    burn_p2p_portal::render_dashboard_html(network_id.as_str())
}

#[cfg(not(feature = "portal"))]
pub fn render_dashboard_html(network_id: &NetworkId) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><body><main><h1>burn_p2p bootstrap</h1><p>Network <strong>{}</strong>. Portal support was not compiled into this build.</p></main></body></html>",
        network_id.as_str()
    )
}

#[cfg(feature = "portal")]
/// Performs the render browser portal html operation.
pub fn render_browser_portal_html(snapshot: &BrowserPortalSnapshot) -> String {
    burn_p2p_portal::render_browser_portal_html(&portal_snapshot_view(snapshot))
}

#[cfg(all(feature = "portal", feature = "metrics-indexer"))]
/// Performs the render browser portal html operation with metrics panels.
pub fn render_browser_portal_html_with_metrics(
    snapshot: &BrowserPortalSnapshot,
    metrics_snapshots: &[MetricsSnapshot],
) -> String {
    burn_p2p_portal::render_browser_portal_html(&portal_snapshot_view_with_metrics(
        snapshot,
        portal_metrics_panels(snapshot, metrics_snapshots),
    ))
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
/// Performs the render browser portal html operation with artifact publication rows.
pub fn render_browser_portal_html_with_artifacts(
    snapshot: &BrowserPortalSnapshot,
    artifact_aliases: &[ArtifactAliasStatus],
) -> String {
    burn_p2p_portal::render_browser_portal_html(&portal_snapshot_view_with_metrics_and_artifacts(
        snapshot,
        Vec::new(),
        portal_artifact_rows(snapshot, artifact_aliases),
    ))
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
/// Performs the render artifact run summaries html operation.
pub fn render_artifact_run_summaries_html(
    experiment_id: &ExperimentId,
    summaries: &[ArtifactRunSummary],
) -> String {
    burn_p2p_portal::render_artifact_run_summaries_html(
        experiment_id.as_str(),
        &portal_artifact_run_summary_rows(summaries),
        "/portal",
    )
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
/// Performs the render artifact run view html operation.
pub fn render_artifact_run_view_html(view: &ArtifactRunView) -> String {
    burn_p2p_portal::render_artifact_run_view_html(&portal_artifact_run_view(view))
}

#[cfg(all(feature = "portal", feature = "artifact-publish"))]
/// Performs the render head artifact view html operation.
pub fn render_head_artifact_view_html(view: &HeadArtifactView) -> String {
    burn_p2p_portal::render_head_artifact_view_html(&portal_head_artifact_view(view))
}

#[cfg(any(not(feature = "portal"), not(feature = "artifact-publish")))]
pub fn render_browser_portal_html_with_artifacts<T>(
    snapshot: &BrowserPortalSnapshot,
    _artifact_aliases: &[T],
) -> String {
    render_browser_portal_html(snapshot)
}

#[cfg(any(not(feature = "portal"), not(feature = "artifact-publish")))]
pub fn render_artifact_run_summaries_html<T>(
    _experiment_id: &ExperimentId,
    _summaries: &[T],
) -> String {
    "<!doctype html><html lang=\"en\"><body><main><h1>Artifact portal unavailable</h1></main></body></html>".into()
}

#[cfg(any(not(feature = "portal"), not(feature = "artifact-publish")))]
pub fn render_artifact_run_view_html<T>(_view: &T) -> String {
    "<!doctype html><html lang=\"en\"><body><main><h1>Artifact portal unavailable</h1></main></body></html>".into()
}

#[cfg(any(not(feature = "portal"), not(feature = "artifact-publish")))]
pub fn render_head_artifact_view_html<T>(_view: &T) -> String {
    "<!doctype html><html lang=\"en\"><body><main><h1>Artifact portal unavailable</h1></main></body></html>".into()
}

#[cfg(all(
    feature = "portal",
    feature = "metrics-indexer",
    feature = "artifact-publish"
))]
/// Performs the render browser portal html operation with both metrics panels and artifact rows.
pub fn render_browser_portal_html_with_metrics_and_artifacts(
    snapshot: &BrowserPortalSnapshot,
    metrics_snapshots: &[MetricsSnapshot],
    artifact_aliases: &[ArtifactAliasStatus],
) -> String {
    burn_p2p_portal::render_browser_portal_html(&portal_snapshot_view_with_metrics_and_artifacts(
        snapshot,
        portal_metrics_panels(snapshot, metrics_snapshots),
        portal_artifact_rows(snapshot, artifact_aliases),
    ))
}

#[cfg(any(
    not(feature = "portal"),
    not(feature = "metrics-indexer"),
    not(feature = "artifact-publish")
))]
pub fn render_browser_portal_html_with_metrics_and_artifacts<M, T>(
    snapshot: &BrowserPortalSnapshot,
    _metrics_snapshots: &[M],
    _artifact_aliases: &[T],
) -> String {
    render_browser_portal_html(snapshot)
}

#[cfg(any(not(feature = "portal"), not(feature = "metrics-indexer")))]
pub fn render_browser_portal_html_with_metrics<T>(
    snapshot: &BrowserPortalSnapshot,
    _metrics_snapshots: &[T],
) -> String {
    render_browser_portal_html(snapshot)
}

#[cfg(not(feature = "portal"))]
pub fn render_browser_portal_html(snapshot: &BrowserPortalSnapshot) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><body><main><h1>burn_p2p browser portal</h1><p>Network <strong>{}</strong>. Portal support was not compiled into this build.</p></main></body></html>",
        snapshot.network_id.as_str()
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
        && load_operator_history(
            running.config().storage.as_ref(),
            running
                .config()
                .metrics_retention
                .resolve_for_roles(&running.mainnet().roles),
        )?
        .receipts
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
            refresh_admin_state_from_runtime(&mut state, &snapshot, running.config())?;
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
    config: &NodeConfig,
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

    state.metrics_retention = config
        .metrics_retention
        .resolve_for_roles(&telemetry.configured_roles);
    state.artifact_store_root = config.storage.as_ref().map(|storage| storage.root.clone());
    state.publication_store_root = config.storage.as_ref().map(StorageConfig::publication_dir);
    state.metrics_store_root = config
        .storage
        .as_ref()
        .map(StorageConfig::metrics_indexer_dir);

    if let Some(storage) = config.storage.as_ref() {
        let history = load_operator_history(Some(storage), state.metrics_retention)?;
        state.head_descriptors = history.heads;
        state.contribution_receipts = history.receipts;
        state.merge_certificates = history.merges;
        state.peer_window_metrics = history.peer_window_metrics;
        state.reducer_cohort_metrics = history.reducer_cohort_metrics;
        state.head_eval_reports = history.head_eval_reports;
        state.eval_protocol_manifests = history.eval_protocol_manifests;
        #[cfg(feature = "metrics-indexer")]
        state.sync_metrics_store()?;
        #[cfg(feature = "artifact-publish")]
        if let Err(error) = state.sync_publication_store() {
            state.last_error = Some(format!("artifact publication sync failed: {error}"));
        }
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Captures one persisted evaluation protocol manifest scoped to a revision.
pub struct StoredEvalProtocolManifestRecord {
    /// Experiment the protocol belongs to.
    pub experiment_id: ExperimentId,
    /// Revision the protocol belongs to.
    pub revision_id: RevisionId,
    /// Timestamp when the protocol record was written.
    pub captured_at: DateTime<Utc>,
    /// Evaluation protocol manifest itself.
    pub manifest: EvalProtocolManifest,
}

struct OperatorHistory {
    receipts: Vec<ContributionReceipt>,
    merges: Vec<MergeCertificate>,
    heads: Vec<HeadDescriptor>,
    peer_window_metrics: Vec<PeerWindowMetrics>,
    reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    head_eval_reports: Vec<HeadEvalReport>,
    eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
}

fn load_operator_history(
    storage: Option<&StorageConfig>,
    metrics_retention: MetricsRetentionBudget,
) -> anyhow::Result<OperatorHistory> {
    let Some(storage) = storage else {
        return Ok(OperatorHistory {
            receipts: Vec::new(),
            merges: Vec::new(),
            heads: Vec::new(),
            peer_window_metrics: Vec::new(),
            reducer_cohort_metrics: Vec::new(),
            head_eval_reports: Vec::new(),
            eval_protocol_manifests: Vec::new(),
        });
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

    let metrics_dir = storage.metrics_dir();
    let mut peer_window_paths = Vec::<PathBuf>::new();
    let mut reducer_cohort_paths = Vec::<PathBuf>::new();
    let mut head_eval_paths = Vec::<PathBuf>::new();
    let mut eval_protocol_paths = Vec::<PathBuf>::new();
    if metrics_dir.exists() {
        for entry in fs::read_dir(&metrics_dir)
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", metrics_dir.display()))?
        {
            let path = entry
                .map_err(|error| anyhow::anyhow!("failed to read metrics entry: {error}"))?
                .path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            match path.file_name().and_then(|name| name.to_str()) {
                Some(name) if name.starts_with("peer-window-") => peer_window_paths.push(path),
                Some(name) if name.starts_with("reducer-cohort-") => {
                    reducer_cohort_paths.push(path);
                }
                Some(name) if name.starts_with("head-eval-") => head_eval_paths.push(path),
                Some(name) if name.starts_with("eval-protocol-") => eval_protocol_paths.push(path),
                _ => {}
            }
        }
    }

    receipt_paths.sort();
    merge_paths.sort();
    head_paths.sort();
    peer_window_paths.sort();
    reducer_cohort_paths.sort();
    head_eval_paths.sort();
    eval_protocol_paths.sort();

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

    #[cfg(feature = "metrics-indexer")]
    if storage.metrics_indexer_dir().exists() {
        let store = MetricsStore::open(
            storage.metrics_indexer_dir(),
            metrics_indexer_config_for_budget(metrics_retention),
        )
        .map_err(anyhow::Error::from)?;
        let entries = store.load_entries();
        if !entries.is_empty() {
            return Ok(operator_history_with_metric_entries(
                receipts,
                merges,
                heads,
                entries,
                load_eval_protocol_manifests(eval_protocol_paths)?,
            ));
        }
    }

    let mut peer_window_metrics = peer_window_paths
        .into_iter()
        .map(load_json_file::<PeerWindowMetrics>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    peer_window_metrics.sort_by_key(|metrics| metrics.window_finished_at);

    let mut reducer_cohort_metrics = reducer_cohort_paths
        .into_iter()
        .map(load_json_file::<ReducerCohortMetrics>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    reducer_cohort_metrics.sort_by_key(|metrics| metrics.captured_at);

    let mut head_eval_reports = head_eval_paths
        .into_iter()
        .map(load_json_file::<HeadEvalReport>)
        .collect::<anyhow::Result<Vec<_>>>()?;
    head_eval_reports.sort_by_key(|report| report.finished_at);
    let mut eval_protocol_manifests = load_eval_protocol_manifests(eval_protocol_paths)?;
    eval_protocol_manifests.sort_by(|left, right| {
        left.experiment_id
            .cmp(&right.experiment_id)
            .then_with(|| left.revision_id.cmp(&right.revision_id))
            .then_with(|| {
                left.manifest
                    .eval_protocol_id
                    .cmp(&right.manifest.eval_protocol_id)
            })
    });

    #[cfg(feature = "metrics-indexer")]
    let (peer_window_metrics, reducer_cohort_metrics, head_eval_reports) = bounded_metric_history(
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
        metrics_indexer_config_for_budget(metrics_retention),
    );

    Ok(OperatorHistory {
        receipts,
        merges,
        heads,
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
        eval_protocol_manifests,
    })
}

fn load_json_file<T: for<'de> Deserialize<'de>>(path: PathBuf) -> anyhow::Result<T> {
    let bytes = fs::read(&path)
        .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| anyhow::anyhow!("failed to decode {}: {error}", path.display()))
}

fn load_eval_protocol_manifests(
    paths: Vec<PathBuf>,
) -> anyhow::Result<Vec<StoredEvalProtocolManifestRecord>> {
    paths
        .into_iter()
        .map(load_json_file::<StoredEvalProtocolManifestRecord>)
        .collect()
}

#[cfg(feature = "metrics-indexer")]
fn operator_history_with_metric_entries(
    receipts: Vec<ContributionReceipt>,
    merges: Vec<MergeCertificate>,
    heads: Vec<HeadDescriptor>,
    entries: Vec<MetricEnvelope>,
    eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
) -> OperatorHistory {
    let mut peer_window_metrics = Vec::new();
    let mut reducer_cohort_metrics = Vec::new();
    let mut head_eval_reports = Vec::new();

    for entry in entries {
        match entry {
            MetricEnvelope::PeerWindow(metrics) => peer_window_metrics.push(metrics),
            MetricEnvelope::ReducerCohort(metrics) => reducer_cohort_metrics.push(metrics),
            MetricEnvelope::HeadEval(report) => head_eval_reports.push(report),
        }
    }
    peer_window_metrics.sort_by_key(|metrics| metrics.window_finished_at);
    reducer_cohort_metrics.sort_by_key(|metrics| metrics.captured_at);
    head_eval_reports.sort_by_key(|report| report.finished_at);

    OperatorHistory {
        receipts,
        merges,
        heads,
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
        eval_protocol_manifests,
    }
}

#[cfg(feature = "metrics-indexer")]
fn bounded_metric_history(
    peer_window_metrics: Vec<PeerWindowMetrics>,
    reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    head_eval_reports: Vec<HeadEvalReport>,
    config: MetricsIndexerConfig,
) -> (
    Vec<PeerWindowMetrics>,
    Vec<ReducerCohortMetrics>,
    Vec<HeadEvalReport>,
) {
    let mut indexer = MetricsIndexer::new(config);
    for metrics in peer_window_metrics {
        indexer.ingest_peer_window_metrics(metrics);
    }
    for metrics in reducer_cohort_metrics {
        indexer.ingest_reducer_cohort_metrics(metrics);
    }
    for report in head_eval_reports {
        indexer.ingest_head_eval_report(report);
    }

    let mut peer_window_metrics = Vec::new();
    let mut reducer_cohort_metrics = Vec::new();
    let mut head_eval_reports = Vec::new();
    for entry in indexer.into_entries() {
        match entry {
            MetricEnvelope::PeerWindow(metrics) => peer_window_metrics.push(metrics),
            MetricEnvelope::ReducerCohort(metrics) => reducer_cohort_metrics.push(metrics),
            MetricEnvelope::HeadEval(report) => head_eval_reports.push(report),
        }
    }
    peer_window_metrics.sort_by_key(|metrics| metrics.window_finished_at);
    reducer_cohort_metrics.sort_by_key(|metrics| metrics.captured_at);
    head_eval_reports.sort_by_key(|report| report.finished_at);
    (
        peer_window_metrics,
        reducer_cohort_metrics,
        head_eval_reports,
    )
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
/// Enumerates the supported bootstrap error values.
pub enum BootstrapError {
    #[error("swarm configuration error: {0}")]
    /// Uses the swarm variant.
    Swarm(#[from] SwarmError),
    #[error("schema error: {0}")]
    /// Uses the schema variant.
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("authority-capable presets require authority and validator policy")]
    /// Uses the missing authority policy variant.
    MissingAuthorityPolicy,
    #[error("this bootstrap plan does not include authority service")]
    /// Uses the authority service required variant.
    AuthorityServiceRequired,
    #[error("admin action `{0:?}` is not enabled for this bootstrap plan")]
    /// Uses the unsupported admin action variant.
    UnsupportedAdminAction(AdminCapability),
    #[error("this admin action requires a signer")]
    /// Uses the missing signer variant.
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

    use burn_p2p::compat::{P2pProject, RuntimeProject};
    use burn_p2p::{
        ArtifactBuildSpec, ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard,
        CapabilityEstimate, ChunkingScheme, ControlPlaneSnapshot, DatasetConfig,
        DatasetRegistration, DatasetSizing, DatasetViewId, EvalSplit, ExperimentDirectoryEntry,
        ExperimentOptInPolicy, ExperimentResourceRequirements, ExperimentScope,
        ExperimentVisibility, FsArtifactStore, HeadDescriptor, LeaderboardEntry,
        LeaderboardIdentity, LeaderboardSnapshot, LiveControlPlaneEvent, MetricReport, MetricValue,
        NodeBuilder, NodeConfig, NodeTelemetrySnapshot, PatchOutcome, PatchSupport, PeerRoleSet,
        ProjectBackend, ReducerLoadAnnouncement, RuntimePatch, RuntimeStatus, ShardFetchManifest,
        SlotAssignmentState, StorageConfig, TrainError, WindowCtx, WindowReport, WorkloadId,
    };
    use burn_p2p_core::{
        ArtifactId, BadgeKind, BrowserMode, CapabilityCard, CapabilityCardId, CapabilityClass,
        ClientPlatform, ContentId, ContributionReceipt, ExperimentId, HeadId, MergeCertificate,
        PeerId, PersistenceClass, PrincipalId, ProfileMode, RevisionId, SocialMode, StudyId,
        TelemetrySummary, WindowActivation, WindowId,
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
        BootstrapSpec, BrowserDirectorySnapshot, BrowserEdgeMode, BrowserEdgePaths,
        BrowserLoginProvider, BrowserPortalSnapshot, BrowserTransportSurface, HeadQuery,
        ReceiptQuery, ReducerLoadQuery, render_browser_portal_html, render_dashboard_html,
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
            lag_state: burn_p2p::LagState::SlightlyBehind,
            head_lag_steps: 1,
            lag_policy: burn_p2p::LagPolicy::default(),
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
        assert!(html.contains("/portal"));
        assert!(html.contains("burn_p2p bootstrap"));
    }

    #[test]
    fn browser_portal_html_renders_snapshot_content() {
        let snapshot = BrowserPortalSnapshot {
            network_id: burn_p2p_core::NetworkId::new("mainnet"),
            edge_mode: BrowserEdgeMode::Peer,
            browser_mode: BrowserMode::Verifier,
            social_mode: SocialMode::Public,
            profile_mode: ProfileMode::Public,
            transports: BrowserTransportSurface {
                webrtc_direct: true,
                webtransport_gateway: false,
                wss_fallback: true,
            },
            paths: BrowserEdgePaths::default(),
            auth_enabled: true,
            login_providers: vec![BrowserLoginProvider {
                label: "Static".into(),
                login_path: "/login/static".into(),
                callback_path: Some("/callback/static".into()),
                device_path: Some("/device".into()),
            }],
            required_release_train_hash: Some(ContentId::new("train-browser")),
            allowed_target_artifact_hashes: BTreeSet::new(),
            diagnostics: BootstrapAdminState::default().diagnostics(
                &plan(BootstrapPreset::BootstrapOnly),
                Utc::now(),
                Some(10),
            ),
            directory: BrowserDirectorySnapshot {
                network_id: burn_p2p_core::NetworkId::new("mainnet"),
                generated_at: Utc::now(),
                entries: vec![ExperimentDirectoryEntry {
                    network_id: burn_p2p_core::NetworkId::new("mainnet"),
                    study_id: StudyId::new("study"),
                    experiment_id: ExperimentId::new("exp"),
                    workload_id: WorkloadId::new("demo"),
                    display_name: "Demo".into(),
                    model_schema_hash: ContentId::new("model"),
                    dataset_view_id: DatasetViewId::new("view"),
                    resource_requirements: ExperimentResourceRequirements {
                        minimum_roles: BTreeSet::new(),
                        minimum_device_memory_bytes: None,
                        minimum_system_memory_bytes: None,
                        estimated_download_bytes: 1024,
                        estimated_window_seconds: 30,
                    },
                    visibility: ExperimentVisibility::Public,
                    opt_in_policy: ExperimentOptInPolicy::Open,
                    current_revision_id: RevisionId::new("rev"),
                    current_head_id: None,
                    allowed_roles: PeerRoleSet::default(),
                    allowed_scopes: BTreeSet::from([ExperimentScope::Connect]),
                    metadata: BTreeMap::new(),
                }],
            },
            heads: Vec::new(),
            leaderboard: LeaderboardSnapshot {
                network_id: burn_p2p_core::NetworkId::new("mainnet"),
                score_version: "leaderboard_score_v1".into(),
                captured_at: Utc::now(),
                entries: vec![LeaderboardEntry {
                    identity: LeaderboardIdentity {
                        principal_id: Some(PrincipalId::new("alice")),
                        peer_ids: BTreeSet::new(),
                        label: "Alice".into(),
                        social_profile: None,
                    },
                    accepted_receipt_count: 1,
                    accepted_work_score: 1.0,
                    quality_weighted_impact_score: 0.5,
                    validation_service_score: 0.0,
                    artifact_serving_score: 0.0,
                    leaderboard_score_v1: 1.5,
                    last_receipt_at: None,
                    badges: Vec::new(),
                }],
            },
            trust_bundle: None,
            captured_at: Utc::now(),
        };

        let html = render_browser_portal_html(&snapshot);
        assert!(html.contains("burn_p2p browser portal"));
        assert!(html.contains("/login/static"));
        assert!(html.contains("Demo"));
        assert!(html.contains("alice"));
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
                        metrics_retention: burn_p2p::MetricsRetentionConfig::default(),
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

    #[cfg(feature = "social")]
    #[test]
    fn leaderboard_snapshot_aggregates_receipts_by_principal() {
        let peer_one = PeerId::new("peer-1");
        let peer_two = PeerId::new("peer-2");
        let principal_id = PrincipalId::new("alice");
        let report = |peer_id: &PeerId| PeerAdmissionReport {
            peer_id: peer_id.clone(),
            principal_id: principal_id.clone(),
            requested_scopes: BTreeSet::new(),
            trust_level: PeerTrustLevel::ScopeAuthorized,
            issuer_peer_id: PeerId::new("authority-1"),
            findings: Vec::new(),
            verified_at: Utc::now(),
        };
        let receipt =
            |peer_id: &PeerId, suffix: &str, accepted_weight: f64, loss: f64| ContributionReceipt {
                receipt_id: burn_p2p_core::ContributionReceiptId::new(format!("receipt-{suffix}")),
                peer_id: peer_id.clone(),
                study_id: StudyId::new("study-1"),
                experiment_id: ExperimentId::new("exp-1"),
                revision_id: RevisionId::new("rev-1"),
                base_head_id: HeadId::new("head-0"),
                artifact_id: ArtifactId::new(format!("artifact-{suffix}")),
                accepted_at: Utc::now(),
                accepted_weight,
                metrics: BTreeMap::from([("loss".into(), MetricValue::Float(loss))]),
                merge_cert_id: None,
            };

        let first_receipt = receipt(&peer_one, "a", 3.0, 1.0);
        let second_receipt = receipt(&peer_two, "b", 2.0, 0.5);
        let state = BootstrapAdminState {
            contribution_receipts: vec![first_receipt.clone(), second_receipt],
            merge_certificates: vec![MergeCertificate {
                merge_cert_id: burn_p2p_core::MergeCertId::new("merge-1"),
                study_id: StudyId::new("study-1"),
                experiment_id: ExperimentId::new("exp-1"),
                revision_id: RevisionId::new("rev-1"),
                base_head_id: HeadId::new("head-0"),
                merged_head_id: HeadId::new("head-1"),
                merged_artifact_id: ArtifactId::new("artifact-merged"),
                policy: burn_p2p::MergePolicy::WeightedMean,
                issued_at: Utc::now(),
                validator: PeerId::new("validator-1"),
                contribution_receipts: vec![first_receipt.receipt_id.clone()],
            }],
            peer_admission_reports: BTreeMap::from([
                (peer_one.clone(), report(&peer_one)),
                (peer_two.clone(), report(&peer_two)),
            ]),
            ..BootstrapAdminState::default()
        };

        let leaderboard =
            state.leaderboard_snapshot(&plan(BootstrapPreset::BootstrapOnly), Utc::now());
        assert_eq!(leaderboard.entries.len(), 1);
        let entry = &leaderboard.entries[0];
        assert_eq!(entry.identity.principal_id, Some(principal_id));
        assert_eq!(
            entry.identity.peer_ids,
            BTreeSet::from([peer_one, peer_two])
        );
        assert_eq!(entry.accepted_receipt_count, 2);
        assert!((entry.accepted_work_score - 4.0).abs() < f64::EPSILON);
        assert!(entry.leaderboard_score_v1 >= entry.accepted_work_score);
        assert!(
            entry
                .badges
                .iter()
                .any(|badge| badge.kind == BadgeKind::FirstAcceptedUpdate)
        );
        assert!(
            entry
                .badges
                .iter()
                .any(|badge| badge.kind == BadgeKind::HelpedPromoteCanonicalHead)
        );
    }

    #[cfg(not(feature = "social"))]
    #[test]
    fn leaderboard_snapshot_is_empty_when_social_service_is_not_compiled() {
        let state = BootstrapAdminState::default();
        let leaderboard =
            state.leaderboard_snapshot(&plan(BootstrapPreset::BootstrapOnly), Utc::now());
        assert!(leaderboard.entries.is_empty());
        assert_eq!(leaderboard.score_version, "leaderboard_score_v1");
    }

    #[cfg(feature = "metrics-indexer")]
    #[test]
    fn load_operator_history_recovers_metrics_from_durable_store() {
        let tempdir = TempDir::new().expect("tempdir");
        let storage = StorageConfig::new(tempdir.path());
        fs::create_dir_all(storage.metrics_indexer_dir()).expect("create metrics indexer dir");

        let peer_window_metrics = burn_p2p::PeerWindowMetrics {
            network_id: burn_p2p_core::NetworkId::new("network-metrics"),
            experiment_id: ExperimentId::new("exp-metrics"),
            revision_id: RevisionId::new("rev-metrics"),
            workload_id: WorkloadId::new("workload-metrics"),
            dataset_view_id: DatasetViewId::new("dataset-view-metrics"),
            peer_id: PeerId::new("peer-metrics"),
            principal_id: None,
            lease_id: burn_p2p::LeaseId::new("lease-metrics"),
            base_head_id: HeadId::new("head-base"),
            window_started_at: Utc::now() - chrono::Duration::seconds(5),
            window_finished_at: Utc::now() - chrono::Duration::seconds(2),
            attempted_tokens_or_samples: 128,
            accepted_tokens_or_samples: Some(128),
            local_train_loss_mean: Some(0.25),
            local_train_loss_last: Some(0.2),
            grad_or_delta_norm: Some(1.0),
            optimizer_step_count: 8,
            compute_time_ms: 1_000,
            data_fetch_time_ms: 250,
            publish_latency_ms: 50,
            head_lag_at_start: 0,
            head_lag_at_finish: 1,
            backend_class: burn_p2p_core::BackendClass::Ndarray,
            role: burn_p2p::PeerRole::TrainerCpu,
            status: burn_p2p_core::PeerWindowStatus::Completed,
            status_reason: None,
        };
        let reducer_cohort_metrics = burn_p2p::ReducerCohortMetrics {
            network_id: burn_p2p_core::NetworkId::new("network-metrics"),
            experiment_id: ExperimentId::new("exp-metrics"),
            revision_id: RevisionId::new("rev-metrics"),
            workload_id: WorkloadId::new("workload-metrics"),
            dataset_view_id: DatasetViewId::new("dataset-view-metrics"),
            merge_window_id: ContentId::new("merge-window-metrics"),
            reducer_group_id: ContentId::new("reducer-group-metrics"),
            captured_at: Utc::now() - chrono::Duration::seconds(1),
            base_head_id: HeadId::new("head-base"),
            candidate_head_id: Some(HeadId::new("head-candidate")),
            received_updates: 1,
            accepted_updates: 1,
            rejected_updates: 0,
            sum_weight: 1.0,
            accepted_tokens_or_samples: 128,
            staleness_mean: 0.5,
            staleness_max: 1.0,
            window_close_delay_ms: 20,
            cohort_duration_ms: 500,
            aggregate_norm: 0.8,
            reducer_load: 0.25,
            ingress_bytes: 1_024,
            egress_bytes: 512,
            replica_agreement: Some(1.0),
            late_arrival_count: Some(0),
            missing_peer_count: Some(0),
            rejection_reasons: BTreeMap::new(),
            status: burn_p2p_core::ReducerCohortStatus::Closed,
        };
        let head_eval_report = burn_p2p::HeadEvalReport {
            network_id: burn_p2p_core::NetworkId::new("network-metrics"),
            experiment_id: ExperimentId::new("exp-metrics"),
            revision_id: RevisionId::new("rev-metrics"),
            workload_id: WorkloadId::new("workload-metrics"),
            head_id: HeadId::new("head-candidate"),
            base_head_id: Some(HeadId::new("head-base")),
            eval_protocol_id: ContentId::new("eval-metrics"),
            evaluator_set_id: ContentId::new("validator-set-metrics"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.2))]),
            sample_count: 128,
            dataset_view_id: DatasetViewId::new("dataset-view-metrics"),
            started_at: Utc::now() - chrono::Duration::seconds(2),
            finished_at: Utc::now() - chrono::Duration::seconds(1),
            trust_class: burn_p2p_core::MetricTrustClass::Canonical,
            status: burn_p2p_core::HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        };

        let mut store = burn_p2p_metrics::MetricsStore::open(
            storage.metrics_indexer_dir(),
            burn_p2p_metrics::MetricsIndexerConfig::default(),
        )
        .expect("open metrics store");
        store
            .sync_from_records(
                std::slice::from_ref(&peer_window_metrics),
                std::slice::from_ref(&reducer_cohort_metrics),
                std::slice::from_ref(&head_eval_report),
            )
            .expect("persist metrics records");

        let history = super::load_operator_history(
            Some(&storage),
            burn_p2p::MetricsRetentionBudget::operator(),
        )
        .expect("load operator history");
        assert_eq!(history.peer_window_metrics, vec![peer_window_metrics]);
        assert_eq!(history.reducer_cohort_metrics, vec![reducer_cohort_metrics]);
        assert_eq!(history.head_eval_reports, vec![head_eval_report]);
    }
}
