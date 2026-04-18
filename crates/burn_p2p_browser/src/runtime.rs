use burn_p2p::{
    BrowserJoinPolicy, BrowserRole, ContentId, ExperimentId, NetworkId, PeerRole, PeerRoleSet,
    RevisionId, RuntimeTransportPolicy,
};
use burn_p2p_swarm::BrowserSwarmBootstrap;
use serde::{Deserialize, Serialize};

use crate::{BrowserResolvedSeedBootstrap, BrowserTransportPolicy};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported browser runtime roles.
pub enum BrowserRuntimeRole {
    /// Uses the portal viewer variant.
    Viewer,
    /// Uses the browser observer variant.
    BrowserObserver,
    /// Uses the browser verifier variant.
    BrowserVerifier,
    /// Uses the browser trainer wgpu variant.
    BrowserTrainerWgpu,
    /// Uses the browser fallback variant.
    BrowserFallback,
}

impl BrowserRuntimeRole {
    /// Returns the browser role view.
    pub fn as_browser_role(&self) -> BrowserRole {
        match self {
            Self::Viewer => BrowserRole::Viewer,
            Self::BrowserObserver => BrowserRole::Observer,
            Self::BrowserVerifier => BrowserRole::Verifier,
            Self::BrowserTrainerWgpu => BrowserRole::TrainerWgpu,
            Self::BrowserFallback => BrowserRole::Fallback,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported browser join stage values.
pub enum BrowserJoinStage {
    /// Uses the authenticating variant.
    Authenticating,
    /// Uses the enrolling variant.
    Enrolling,
    /// Uses the directory sync variant.
    DirectorySync,
    /// Uses the head sync variant.
    HeadSync,
    /// Uses the transport connect variant.
    TransportConnect,
}

impl BrowserJoinStage {
    /// Returns a stable short label for the join stage.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Authenticating => "joining-authenticating",
            Self::Enrolling => "joining-enrolling",
            Self::DirectorySync => "joining-directory-sync",
            Self::HeadSync => "joining-head-sync",
            Self::TransportConnect => "joining-transport-connect",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported browser runtime states.
pub enum BrowserRuntimeState {
    /// Uses the portal only variant.
    ViewerOnly,
    /// Uses the joining variant.
    Joining {
        /// The role.
        role: BrowserRuntimeRole,
        /// The stage.
        stage: BrowserJoinStage,
    },
    /// Allows observer behavior.
    Observer,
    /// Allows verifier behavior.
    Verifier,
    /// Allows trainer behavior.
    Trainer,
    /// Uses the background suspended variant.
    BackgroundSuspended {
        /// The role.
        role: Option<BrowserRuntimeRole>,
    },
    /// Uses the catchup variant.
    Catchup {
        /// The role.
        role: BrowserRuntimeRole,
    },
    /// Uses the blocked variant.
    Blocked {
        /// The reason.
        reason: String,
    },
}

impl BrowserRuntimeState {
    /// Performs the joining operation.
    pub fn joining(role: BrowserRuntimeRole, stage: BrowserJoinStage) -> Self {
        Self::Joining { role, stage }
    }

    /// Performs the blocked operation.
    pub fn blocked(reason: impl Into<String>) -> Self {
        Self::Blocked {
            reason: reason.into(),
        }
    }

    /// Performs the active role operation.
    pub fn active_role(&self) -> Option<BrowserRuntimeRole> {
        match self {
            Self::ViewerOnly => Some(BrowserRuntimeRole::Viewer),
            Self::Joining { role, .. } => Some(role.clone()),
            Self::Observer => Some(BrowserRuntimeRole::BrowserObserver),
            Self::Verifier => Some(BrowserRuntimeRole::BrowserVerifier),
            Self::Trainer => Some(BrowserRuntimeRole::BrowserTrainerWgpu),
            Self::BackgroundSuspended { role } => role.clone(),
            Self::Catchup { role } => Some(role.clone()),
            Self::Blocked { .. } => None,
        }
    }

    /// Returns whether the value requires peer transport.
    pub fn requires_peer_transport(&self) -> bool {
        matches!(
            self,
            Self::Joining { .. }
                | Self::Observer
                | Self::Verifier
                | Self::Trainer
                | Self::Catchup { .. }
        )
    }

    /// Creates a value from the join policy.
    pub fn from_join_policy(
        policy: &BrowserJoinPolicy,
        preferred_role: BrowserRuntimeRole,
    ) -> Self {
        match policy.recommended_role(preferred_role.as_browser_role()) {
            Some(BrowserRole::Viewer) => Self::ViewerOnly,
            Some(BrowserRole::Observer | BrowserRole::Fallback) => Self::Observer,
            Some(BrowserRole::Verifier) => Self::Verifier,
            Some(BrowserRole::TrainerWgpu) => Self::Trainer,
            None => Self::blocked(
                policy
                    .blocked_reasons
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "browser peer is not eligible for this revision".into()),
            ),
        }
    }

    /// Returns a stable short label for the runtime state.
    pub fn label(&self) -> String {
        match self {
            Self::ViewerOnly => "viewer-only".into(),
            Self::Joining { stage, .. } => stage.label().into(),
            Self::Observer => "observer".into(),
            Self::Verifier => "verifier".into(),
            Self::Trainer => "trainer".into(),
            Self::BackgroundSuspended { .. } => "background-suspended".into(),
            Self::Catchup { .. } => "catchup".into(),
            Self::Blocked { reason } => format!("blocked:{reason}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures browser runtime.
pub struct BrowserRuntimeConfig {
    /// The edge base URL.
    pub edge_base_url: String,
    /// The network ID.
    pub network_id: NetworkId,
    /// The release train hash.
    pub release_train_hash: ContentId,
    /// The target artifact ID.
    pub target_artifact_id: String,
    /// The target artifact hash.
    pub target_artifact_hash: ContentId,
    /// The receipt submit path.
    pub receipt_submit_path: String,
    /// The role.
    pub role: BrowserRuntimeRole,
    /// The transport.
    pub transport: BrowserTransportPolicy,
    /// Seed bootstrap material resolved for the current browser session.
    #[serde(default)]
    pub seed_bootstrap: BrowserResolvedSeedBootstrap,
    /// Site-config fallback seeds embedded into the browser artifact.
    #[serde(default)]
    pub site_seed_node_urls: Vec<String>,
    /// The selected experiment.
    pub selected_experiment: Option<ExperimentId>,
    /// The selected revision.
    pub selected_revision: Option<RevisionId>,
}

impl BrowserRuntimeConfig {
    /// Creates a new value.
    pub fn new(
        edge_base_url: impl Into<String>,
        network_id: NetworkId,
        release_train_hash: ContentId,
        target_artifact_id: impl Into<String>,
        target_artifact_hash: ContentId,
    ) -> Self {
        Self {
            edge_base_url: edge_base_url.into(),
            network_id,
            release_train_hash,
            target_artifact_id: target_artifact_id.into(),
            target_artifact_hash,
            receipt_submit_path: "/receipts/browser".into(),
            role: BrowserRuntimeRole::BrowserObserver,
            transport: BrowserTransportPolicy::from(RuntimeTransportPolicy::browser_for_roles(
                &PeerRoleSet::new([PeerRole::BrowserObserver]),
            )),
            seed_bootstrap: BrowserResolvedSeedBootstrap::default(),
            site_seed_node_urls: Vec::new(),
            selected_experiment: None,
            selected_revision: None,
        }
    }

    /// Builds the shared browser swarm bootstrap contract from the current runtime config.
    pub fn swarm_bootstrap(&self) -> BrowserSwarmBootstrap {
        let seed_bootstrap = if matches!(
            self.seed_bootstrap.source,
            burn_p2p_core::BrowserSeedBootstrapSource::Unavailable
        ) && !self.site_seed_node_urls.is_empty()
        {
            BrowserResolvedSeedBootstrap {
                source: burn_p2p_core::BrowserSeedBootstrapSource::SiteConfigFallback,
                seed_node_urls: self.site_seed_node_urls.clone(),
                advertised_seed_count: 0,
                last_error: self.seed_bootstrap.last_error.clone(),
            }
        } else {
            self.seed_bootstrap.clone()
        };
        BrowserSwarmBootstrap {
            network_id: self.network_id.clone(),
            seed_bootstrap,
            transport_preference: self
                .transport
                .preferred
                .iter()
                .map(crate::transport::browser_transport_family)
                .collect(),
            selected_experiment: self.selected_experiment.clone(),
            selected_revision: self.selected_revision.clone(),
        }
    }
}
