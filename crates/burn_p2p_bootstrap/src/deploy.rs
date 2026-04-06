use std::collections::BTreeSet;

use crate::BootstrapError;
use burn_p2p::ValidatorPolicy;
use burn_p2p_core::{ArtifactId, GenesisSpec, HeadId, NetworkId, PeerRole, PeerRoleSet};
use burn_p2p_security::ReleasePolicy;
use burn_p2p_swarm::{RuntimeBoundary, SwarmAddress};
use serde::{Deserialize, Serialize};

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
                BootstrapService::CoherenceSeed,
                BootstrapService::AdminApi,
                BootstrapService::TelemetryExport,
            ]),
            Self::BootstrapArchive => BTreeSet::from([
                BootstrapService::CoherenceSeed,
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
                BootstrapService::CoherenceSeed,
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
    /// Uses the coherence seed variant.
    ///
    /// This is a cheap swarm/control-plane seed that helps peers discover one another
    /// and recover mesh connectivity. It is not a libp2p relay/rendezvous/kademlia node.
    CoherenceSeed,
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
            runtime: RuntimeBoundary::for_platform_and_roles(
                &self.genesis,
                self.platform,
                &self.preset.roles(),
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
}
