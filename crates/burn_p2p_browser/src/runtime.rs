use burn_p2p::{
    BrowserJoinPolicy, BrowserRole, ContentId, ExperimentId, NetworkId, RevisionId,
    RuntimeTransportPolicy,
};
use serde::{Deserialize, Serialize};

use crate::BrowserTransportPolicy;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrowserRuntimeRole {
    PortalViewer,
    BrowserObserver,
    BrowserVerifier,
    BrowserTrainerWgpu,
    BrowserFallback,
}

impl BrowserRuntimeRole {
    pub fn as_browser_role(&self) -> BrowserRole {
        match self {
            Self::PortalViewer => BrowserRole::PortalViewer,
            Self::BrowserObserver => BrowserRole::Observer,
            Self::BrowserVerifier => BrowserRole::Verifier,
            Self::BrowserTrainerWgpu => BrowserRole::TrainerWgpu,
            Self::BrowserFallback => BrowserRole::Fallback,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrowserJoinStage {
    Authenticating,
    Enrolling,
    DirectorySync,
    HeadSync,
    TransportConnect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrowserRuntimeState {
    PortalOnly,
    Joining {
        role: BrowserRuntimeRole,
        stage: BrowserJoinStage,
    },
    Observer,
    Verifier,
    Trainer,
    BackgroundSuspended {
        role: Option<BrowserRuntimeRole>,
    },
    Catchup {
        role: BrowserRuntimeRole,
    },
    Blocked {
        reason: String,
    },
}

impl BrowserRuntimeState {
    pub fn joining(role: BrowserRuntimeRole, stage: BrowserJoinStage) -> Self {
        Self::Joining { role, stage }
    }

    pub fn blocked(reason: impl Into<String>) -> Self {
        Self::Blocked {
            reason: reason.into(),
        }
    }

    pub fn active_role(&self) -> Option<BrowserRuntimeRole> {
        match self {
            Self::PortalOnly => Some(BrowserRuntimeRole::PortalViewer),
            Self::Joining { role, .. } => Some(role.clone()),
            Self::Observer => Some(BrowserRuntimeRole::BrowserObserver),
            Self::Verifier => Some(BrowserRuntimeRole::BrowserVerifier),
            Self::Trainer => Some(BrowserRuntimeRole::BrowserTrainerWgpu),
            Self::BackgroundSuspended { role } => role.clone(),
            Self::Catchup { role } => Some(role.clone()),
            Self::Blocked { .. } => None,
        }
    }

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

    pub fn from_join_policy(
        policy: &BrowserJoinPolicy,
        preferred_role: BrowserRuntimeRole,
    ) -> Self {
        match policy.recommended_role(preferred_role.as_browser_role()) {
            Some(BrowserRole::PortalViewer) => Self::PortalOnly,
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserRuntimeConfig {
    pub edge_base_url: String,
    pub network_id: NetworkId,
    pub release_train_hash: ContentId,
    pub target_artifact_id: String,
    pub target_artifact_hash: ContentId,
    pub receipt_submit_path: String,
    pub role: BrowserRuntimeRole,
    pub transport: BrowserTransportPolicy,
    pub selected_experiment: Option<ExperimentId>,
    pub selected_revision: Option<RevisionId>,
}

impl BrowserRuntimeConfig {
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
            transport: BrowserTransportPolicy::from(RuntimeTransportPolicy::browser()),
            selected_experiment: None,
            selected_revision: None,
        }
    }
}
