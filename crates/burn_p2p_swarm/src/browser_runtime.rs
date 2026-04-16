use async_trait::async_trait;
use burn_p2p_core::{
    ArtifactId, BrowserResolvedSeedBootstrap, BrowserSwarmPhase, BrowserSwarmStatus,
    BrowserTransportFamily, ChunkId, ExperimentId, NetworkId, PeerId, RevisionId,
};
use serde::{Deserialize, Serialize};

use crate::SwarmError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Bootstrap contract for one browser swarm runtime connection attempt.
pub struct BrowserSwarmBootstrap {
    /// Network the browser intends to join.
    pub network_id: NetworkId,
    /// Resolved browser seed bootstrap material.
    pub seed_bootstrap: BrowserResolvedSeedBootstrap,
    /// Desired direct browser transport order.
    pub transport_preference: Vec<BrowserTransportFamily>,
    /// Selected experiment, when already known at bootstrap time.
    #[serde(default)]
    pub selected_experiment: Option<ExperimentId>,
    /// Selected revision, when already known at bootstrap time.
    #[serde(default)]
    pub selected_revision: Option<RevisionId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One browser seed dial candidate derived from bootstrap material.
pub struct BrowserSeedDialCandidate {
    /// Browser-capable multiaddr used as the dial target.
    pub seed_url: String,
    /// Browser transport family implied by the multiaddr.
    pub transport: BrowserTransportFamily,
    /// Optional peer id attached to the seed record.
    #[serde(default)]
    pub peer_id: Option<PeerId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Ordered browser seed dial plan derived from signed bootstrap material.
pub struct BrowserSwarmDialPlan {
    /// Preferred direct transport family currently targeted by the browser.
    #[serde(default)]
    pub desired_transport: Option<BrowserTransportFamily>,
    /// Ordered dial candidates after transport-family planning and de-duplication.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<BrowserSeedDialCandidate>,
    /// Whether websocket fallback remains allowed after direct transports.
    #[serde(default)]
    pub fallback_allowed: bool,
    /// Planning warnings surfaced while deriving candidates.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Requests an artifact manifest from one browser swarm provider set.
pub struct BrowserArtifactManifestRequest {
    /// Target artifact id.
    pub artifact_id: ArtifactId,
    /// Candidate provider peer ids already known to the browser runtime.
    #[serde(default)]
    pub provider_peer_ids: Vec<PeerId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Requests an artifact chunk from one browser swarm provider set.
pub struct BrowserArtifactChunkRequest {
    /// Target artifact id.
    pub artifact_id: ArtifactId,
    /// Target chunk id.
    pub chunk_id: ChunkId,
    /// Candidate provider peer ids already known to the browser runtime.
    #[serde(default)]
    pub provider_peer_ids: Vec<PeerId>,
}

#[async_trait(?Send)]
/// Browser swarm runtime boundary implemented by direct browser transport backends.
pub trait BrowserSwarmRuntime {
    /// Applies one bootstrap contract and prepares the runtime to dial seeds directly.
    async fn connect(
        &mut self,
        bootstrap: BrowserSwarmBootstrap,
    ) -> Result<BrowserSwarmDialPlan, SwarmError>;

    /// Disconnects the current direct browser transport session.
    async fn disconnect(&mut self) -> Result<(), SwarmError>;

    /// Returns the current truthful browser swarm status.
    fn status(&self) -> BrowserSwarmStatus;

    /// Returns the most recent browser seed dial plan, when one exists.
    fn dial_plan(&self) -> Option<BrowserSwarmDialPlan>;

    /// Subscribes the browser runtime to directory assignment propagation.
    async fn subscribe_directory(&mut self) -> Result<(), SwarmError>;

    /// Subscribes the browser runtime to head propagation.
    async fn subscribe_heads(&mut self) -> Result<(), SwarmError>;

    /// Subscribes the browser runtime to metrics propagation.
    async fn subscribe_metrics(&mut self) -> Result<(), SwarmError>;

    /// Fetches one artifact manifest through the browser swarm path.
    async fn fetch_artifact_manifest(
        &mut self,
        request: BrowserArtifactManifestRequest,
    ) -> Result<burn_p2p_core::ArtifactDescriptor, SwarmError>;

    /// Fetches one artifact chunk through the browser swarm path.
    async fn fetch_artifact_chunk(
        &mut self,
        request: BrowserArtifactChunkRequest,
    ) -> Result<Vec<u8>, SwarmError>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Planning-only browser swarm runtime used until a real wasm transport backend exists.
pub struct PlannedBrowserSwarmRuntime {
    status: BrowserSwarmStatus,
    dial_plan: Option<BrowserSwarmDialPlan>,
}

impl PlannedBrowserSwarmRuntime {
    /// Applies one synchronous planning-only bootstrap connect and updates runtime status.
    pub fn plan_connect(&mut self, bootstrap: BrowserSwarmBootstrap) -> BrowserSwarmDialPlan {
        let dial_plan = plan_browser_seed_dials(&bootstrap);
        let desired_transport = dial_plan.desired_transport.clone();
        let last_error = dial_plan.warnings.first().cloned();
        let phase = if desired_transport.is_some() {
            BrowserSwarmPhase::TransportSelected
        } else if !bootstrap.seed_bootstrap.seed_node_urls.is_empty() {
            BrowserSwarmPhase::SeedResolved
        } else {
            BrowserSwarmPhase::Bootstrap
        };
        self.status = BrowserSwarmStatus {
            phase,
            seed_bootstrap: bootstrap.seed_bootstrap,
            transport_source: burn_p2p_core::BrowserTransportObservationSource::Selected,
            desired_transport,
            connected_transport: None,
            connected_peer_count: 0,
            connected_peer_ids: Vec::new(),
            directory_synced: false,
            assignment_bound: false,
            head_synced: false,
            artifact_source: burn_p2p_core::BrowserArtifactSource::Unavailable,
            last_error,
        };
        self.dial_plan = Some(dial_plan.clone());
        dial_plan
    }

    /// Clears any planning-only bootstrap state.
    pub fn plan_disconnect(&mut self) {
        self.status = BrowserSwarmStatus::default();
        self.dial_plan = None;
    }

    /// Applies one externally observed browser swarm status snapshot.
    pub fn observe_status(&mut self, status: BrowserSwarmStatus) {
        self.status = status;
    }

    /// Returns the current browser swarm status without needing the trait object path.
    pub fn status_ref(&self) -> &BrowserSwarmStatus {
        &self.status
    }

    /// Returns the most recent dial plan without cloning.
    pub fn dial_plan_ref(&self) -> Option<&BrowserSwarmDialPlan> {
        self.dial_plan.as_ref()
    }
}

#[async_trait(?Send)]
impl BrowserSwarmRuntime for PlannedBrowserSwarmRuntime {
    async fn connect(
        &mut self,
        bootstrap: BrowserSwarmBootstrap,
    ) -> Result<BrowserSwarmDialPlan, SwarmError> {
        Ok(self.plan_connect(bootstrap))
    }

    async fn disconnect(&mut self) -> Result<(), SwarmError> {
        self.plan_disconnect();
        Ok(())
    }

    fn status(&self) -> BrowserSwarmStatus {
        self.status.clone()
    }

    fn dial_plan(&self) -> Option<BrowserSwarmDialPlan> {
        self.dial_plan.clone()
    }

    async fn subscribe_directory(&mut self) -> Result<(), SwarmError> {
        Err(SwarmError::Runtime(
            "direct browser swarm directory subscription is not implemented".into(),
        ))
    }

    async fn subscribe_heads(&mut self) -> Result<(), SwarmError> {
        Err(SwarmError::Runtime(
            "direct browser swarm head subscription is not implemented".into(),
        ))
    }

    async fn subscribe_metrics(&mut self) -> Result<(), SwarmError> {
        Err(SwarmError::Runtime(
            "direct browser swarm metrics subscription is not implemented".into(),
        ))
    }

    async fn fetch_artifact_manifest(
        &mut self,
        _request: BrowserArtifactManifestRequest,
    ) -> Result<burn_p2p_core::ArtifactDescriptor, SwarmError> {
        Err(SwarmError::Runtime(
            "direct browser swarm artifact manifest fetch is not implemented".into(),
        ))
    }

    async fn fetch_artifact_chunk(
        &mut self,
        _request: BrowserArtifactChunkRequest,
    ) -> Result<Vec<u8>, SwarmError> {
        Err(SwarmError::Runtime(
            "direct browser swarm artifact chunk fetch is not implemented".into(),
        ))
    }
}

/// Derives one deterministic browser seed dial plan from bootstrap material.
pub fn plan_browser_seed_dials(bootstrap: &BrowserSwarmBootstrap) -> BrowserSwarmDialPlan {
    let mut warnings = Vec::new();
    let mut candidates = Vec::new();
    let mut desired_transport = None;

    for transport in &bootstrap.transport_preference {
        let mut any_for_transport = false;
        for seed_url in &bootstrap.seed_bootstrap.seed_node_urls {
            match browser_transport_family_for_seed_url(seed_url) {
                Some(family) if &family == transport => {
                    any_for_transport = true;
                    if !candidates
                        .iter()
                        .any(|candidate: &BrowserSeedDialCandidate| {
                            candidate.transport == family && candidate.seed_url == *seed_url
                        })
                    {
                        candidates.push(BrowserSeedDialCandidate {
                            seed_url: seed_url.clone(),
                            transport: family,
                            peer_id: None,
                        });
                    }
                }
                Some(_) => {}
                None => warnings.push(format!(
                    "seed `{seed_url}` did not map to a browser-capable transport family"
                )),
            }
        }
        if any_for_transport && desired_transport.is_none() {
            desired_transport = Some(transport.clone());
        }
    }

    BrowserSwarmDialPlan {
        desired_transport,
        candidates,
        fallback_allowed: bootstrap
            .transport_preference
            .contains(&BrowserTransportFamily::WssFallback),
        warnings: dedupe_warnings(warnings),
    }
}

/// Returns the browser-capable transport family implied by one seed multiaddr.
pub fn browser_transport_family_for_seed_url(seed_url: &str) -> Option<BrowserTransportFamily> {
    let segments = seed_url
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.contains(&"webrtc-direct") {
        return Some(BrowserTransportFamily::WebRtcDirect);
    }
    if segments.contains(&"webtransport") {
        return Some(BrowserTransportFamily::WebTransport);
    }
    if segments.contains(&"wss") || segments.contains(&"ws") {
        return Some(BrowserTransportFamily::WssFallback);
    }
    None
}

fn dedupe_warnings(warnings: Vec<String>) -> Vec<String> {
    let mut deduped = Vec::new();
    for warning in warnings {
        if !deduped.contains(&warning) {
            deduped.push(warning);
        }
    }
    deduped
}
