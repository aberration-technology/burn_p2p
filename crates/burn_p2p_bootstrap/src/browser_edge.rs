use std::collections::BTreeSet;

use crate::{
    deploy::BootstrapPlan,
    state::{BootstrapAdminState, BootstrapDiagnostics},
};
use burn_p2p::{ContentId, HeadDescriptor};
use burn_p2p_core::{
    BrowserDirectorySnapshot, BrowserEdgeMode, BrowserEdgePaths, BrowserLeaderboardSnapshot,
    BrowserLoginProvider, BrowserMode, BrowserTransportSurface, NetworkId, ProfileMode, SocialMode,
    TrustBundleExport,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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

impl BootstrapAdminState {
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
            .collect();
        crate::social_services::leaderboard_snapshot(
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
}
