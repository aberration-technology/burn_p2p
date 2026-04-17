use async_trait::async_trait;
use burn_p2p_core::{
    ArtifactId, BrowserResolvedSeedBootstrap, BrowserSwarmPhase, BrowserSwarmStatus,
    BrowserTransportFamily, ChunkId, ExperimentId, NetworkId, PeerId, RevisionId, StudyId,
};
use serde::{Deserialize, Serialize};

#[cfg(target_arch = "wasm32")]
use burn_p2p_core::BrowserTransportObservationSource;

#[cfg(target_arch = "wasm32")]
use crate::runtime_helpers::stream_protocol;
#[cfg(target_arch = "wasm32")]
use crate::{
    ControlPlaneRequest, ControlPlaneResponse, ProtocolSet, PubsubEnvelope, apply_pubsub_payload,
    pubsub_semantic_message_id,
};
use crate::{ControlPlaneSnapshot, SwarmError};
use crate::{OverlayChannel, OverlayTopic};

use std::collections::{BTreeMap, BTreeSet};
#[cfg(target_arch = "wasm32")]
use std::{cell::RefCell, rc::Rc};

#[cfg(target_arch = "wasm32")]
use futures::{
    FutureExt, StreamExt,
    channel::{mpsc, oneshot},
};
#[cfg(target_arch = "wasm32")]
use gloo_timers::future::TimeoutFuture;
#[cfg(target_arch = "wasm32")]
use libp2p::Transport;
#[cfg(target_arch = "wasm32")]
use libp2p::{
    Multiaddr, SwarmBuilder,
    core::upgrade::Version,
    gossipsub,
    swarm::{NetworkBehaviour, SwarmEvent},
};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

const WASM_BROWSER_TARGET_CONNECTED_PEERS: usize = 3;

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One browser swarm update delivered from the direct swarm runtime.
pub enum BrowserSwarmUpdate {
    /// One fresh control-plane snapshot observed through the direct swarm.
    Snapshot(Box<ControlPlaneSnapshot>),
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

    /// Drains any queued browser swarm updates accumulated since the last read.
    fn drain_updates(&mut self) -> Vec<BrowserSwarmUpdate>;

    /// Fetches one control-plane snapshot through the browser swarm path.
    async fn fetch_snapshot(&mut self) -> Result<ControlPlaneSnapshot, SwarmError>;

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

    fn drain_updates(&mut self) -> Vec<BrowserSwarmUpdate> {
        Vec::new()
    }

    async fn fetch_snapshot(&mut self) -> Result<ControlPlaneSnapshot, SwarmError> {
        Err(SwarmError::Runtime(
            "direct browser swarm control snapshot fetch is not implemented".into(),
        ))
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

#[cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]
pub(crate) fn filter_supported_browser_seed_dial_candidates(
    dial_plan: &BrowserSwarmDialPlan,
    supported_transports: &[BrowserTransportFamily],
) -> Vec<BrowserSeedDialCandidate> {
    dial_plan
        .candidates
        .iter()
        .filter(|candidate| supported_transports.contains(&candidate.transport))
        .cloned()
        .collect()
}

#[cfg(target_arch = "wasm32")]
#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p_swarm::derive_prelude")]
struct WasmBrowserSwarmBehaviour {
    ping: libp2p::ping::Behaviour,
    gossipsub: gossipsub::Behaviour,
    request_response:
        libp2p_request_response::cbor::Behaviour<ControlPlaneRequest, ControlPlaneResponse>,
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Debug, Default)]
struct WasmBrowserSwarmSharedState {
    status: BrowserSwarmStatus,
    dial_plan: Option<BrowserSwarmDialPlan>,
    updates: Vec<BrowserSwarmUpdate>,
}

#[cfg(target_arch = "wasm32")]
struct WasmPendingConnect {
    dial_plan: BrowserSwarmDialPlan,
    candidates: Vec<BrowserSeedDialCandidate>,
    next_candidate_index: usize,
    response_tx: Option<oneshot::Sender<Result<BrowserSwarmDialPlan, SwarmError>>>,
}

#[cfg(target_arch = "wasm32")]
impl WasmPendingConnect {
    fn next_candidate(&mut self) -> Option<BrowserSeedDialCandidate> {
        let candidate = self.candidates.get(self.next_candidate_index).cloned();
        if candidate.is_some() {
            self.next_candidate_index += 1;
        }
        candidate
    }
}

#[cfg(target_arch = "wasm32")]
enum WasmBrowserSwarmCommand {
    Connect {
        bootstrap: BrowserSwarmBootstrap,
        response_tx: oneshot::Sender<Result<BrowserSwarmDialPlan, SwarmError>>,
    },
    Disconnect {
        response_tx: oneshot::Sender<Result<(), SwarmError>>,
    },
    SubscribeDirectory {
        response_tx: oneshot::Sender<Result<(), SwarmError>>,
    },
    SubscribeHeads {
        response_tx: oneshot::Sender<Result<(), SwarmError>>,
    },
    SubscribeMetrics {
        response_tx: oneshot::Sender<Result<(), SwarmError>>,
    },
    FetchSnapshot {
        response_tx: oneshot::Sender<Result<ControlPlaneSnapshot, SwarmError>>,
    },
    FetchArtifactManifest {
        request: BrowserArtifactManifestRequest,
        response_tx: oneshot::Sender<Result<burn_p2p_core::ArtifactDescriptor, SwarmError>>,
    },
    FetchArtifactChunk {
        request: BrowserArtifactChunkRequest,
        response_tx: oneshot::Sender<Result<Vec<u8>, SwarmError>>,
    },
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
/// Real wasm browser swarm runtime backed by browser-capable libp2p transports.
pub struct WasmBrowserSwarmRuntime {
    shared: Rc<RefCell<WasmBrowserSwarmSharedState>>,
    command_tx: mpsc::UnboundedSender<WasmBrowserSwarmCommand>,
}

#[cfg(target_arch = "wasm32")]
impl std::fmt::Debug for WasmBrowserSwarmRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmBrowserSwarmRuntime")
            .field("status", &self.status())
            .field("dial_plan", &self.dial_plan())
            .finish()
    }
}

#[cfg(target_arch = "wasm32")]
impl WasmBrowserSwarmRuntime {
    pub fn new(network_id: NetworkId) -> Self {
        let shared = Rc::new(RefCell::new(WasmBrowserSwarmSharedState::default()));
        let (command_tx, command_rx) = mpsc::unbounded();
        spawn_local(run_wasm_browser_swarm_task(
            shared.clone(),
            command_rx,
            network_id,
        ));
        Self { shared, command_tx }
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
impl BrowserSwarmRuntime for WasmBrowserSwarmRuntime {
    async fn connect(
        &mut self,
        bootstrap: BrowserSwarmBootstrap,
    ) -> Result<BrowserSwarmDialPlan, SwarmError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .unbounded_send(WasmBrowserSwarmCommand::Connect {
                bootstrap,
                response_tx,
            })
            .map_err(|_| {
                SwarmError::Runtime("browser direct swarm command channel closed".into())
            })?;

        let response = response_rx.fuse();
        let timeout = TimeoutFuture::new(15_000).fuse();
        futures::pin_mut!(response, timeout);
        match futures::future::select(response, timeout).await {
            futures::future::Either::Left((result, _)) => result.map_err(|_| {
                SwarmError::Runtime("browser direct swarm connect response was dropped".into())
            })?,
            futures::future::Either::Right((_, _)) => Err(SwarmError::Runtime(
                "timed out waiting for browser direct swarm connection".into(),
            )),
        }
    }

    async fn disconnect(&mut self) -> Result<(), SwarmError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .unbounded_send(WasmBrowserSwarmCommand::Disconnect { response_tx })
            .map_err(|_| {
                SwarmError::Runtime("browser direct swarm command channel closed".into())
            })?;

        let response = response_rx.fuse();
        let timeout = TimeoutFuture::new(5_000).fuse();
        futures::pin_mut!(response, timeout);
        match futures::future::select(response, timeout).await {
            futures::future::Either::Left((result, _)) => result.map_err(|_| {
                SwarmError::Runtime("browser direct swarm disconnect response was dropped".into())
            })?,
            futures::future::Either::Right((_, _)) => Err(SwarmError::Runtime(
                "timed out waiting for browser direct swarm disconnect".into(),
            )),
        }
    }

    fn status(&self) -> BrowserSwarmStatus {
        self.shared.borrow().status.clone()
    }

    fn dial_plan(&self) -> Option<BrowserSwarmDialPlan> {
        self.shared.borrow().dial_plan.clone()
    }

    fn drain_updates(&mut self) -> Vec<BrowserSwarmUpdate> {
        std::mem::take(&mut self.shared.borrow_mut().updates)
    }

    async fn fetch_snapshot(&mut self) -> Result<ControlPlaneSnapshot, SwarmError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .unbounded_send(WasmBrowserSwarmCommand::FetchSnapshot { response_tx })
            .map_err(|_| {
                SwarmError::Runtime("browser direct swarm command channel closed".into())
            })?;

        let response = response_rx.fuse();
        let timeout = TimeoutFuture::new(10_000).fuse();
        futures::pin_mut!(response, timeout);
        match futures::future::select(response, timeout).await {
            futures::future::Either::Left((result, _)) => result.map_err(|_| {
                SwarmError::Runtime("browser direct swarm snapshot response was dropped".into())
            })?,
            futures::future::Either::Right((_, _)) => Err(SwarmError::Runtime(
                "timed out waiting for browser direct swarm snapshot".into(),
            )),
        }
    }

    async fn subscribe_directory(&mut self) -> Result<(), SwarmError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .unbounded_send(WasmBrowserSwarmCommand::SubscribeDirectory { response_tx })
            .map_err(|_| {
                SwarmError::Runtime("browser direct swarm command channel closed".into())
            })?;
        let response = response_rx.fuse();
        let timeout = TimeoutFuture::new(5_000).fuse();
        futures::pin_mut!(response, timeout);
        match futures::future::select(response, timeout).await {
            futures::future::Either::Left((result, _)) => result.map_err(|_| {
                SwarmError::Runtime(
                    "browser direct swarm directory subscription response was dropped".into(),
                )
            })?,
            futures::future::Either::Right((_, _)) => Err(SwarmError::Runtime(
                "timed out waiting for browser direct swarm directory subscription".into(),
            )),
        }
    }

    async fn subscribe_heads(&mut self) -> Result<(), SwarmError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .unbounded_send(WasmBrowserSwarmCommand::SubscribeHeads { response_tx })
            .map_err(|_| {
                SwarmError::Runtime("browser direct swarm command channel closed".into())
            })?;
        let response = response_rx.fuse();
        let timeout = TimeoutFuture::new(5_000).fuse();
        futures::pin_mut!(response, timeout);
        match futures::future::select(response, timeout).await {
            futures::future::Either::Left((result, _)) => result.map_err(|_| {
                SwarmError::Runtime(
                    "browser direct swarm head subscription response was dropped".into(),
                )
            })?,
            futures::future::Either::Right((_, _)) => Err(SwarmError::Runtime(
                "timed out waiting for browser direct swarm head subscription".into(),
            )),
        }
    }

    async fn subscribe_metrics(&mut self) -> Result<(), SwarmError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .unbounded_send(WasmBrowserSwarmCommand::SubscribeMetrics { response_tx })
            .map_err(|_| {
                SwarmError::Runtime("browser direct swarm command channel closed".into())
            })?;
        let response = response_rx.fuse();
        let timeout = TimeoutFuture::new(5_000).fuse();
        futures::pin_mut!(response, timeout);
        match futures::future::select(response, timeout).await {
            futures::future::Either::Left((result, _)) => result.map_err(|_| {
                SwarmError::Runtime(
                    "browser direct swarm metrics subscription response was dropped".into(),
                )
            })?,
            futures::future::Either::Right((_, _)) => Err(SwarmError::Runtime(
                "timed out waiting for browser direct swarm metrics subscription".into(),
            )),
        }
    }

    async fn fetch_artifact_manifest(
        &mut self,
        request: BrowserArtifactManifestRequest,
    ) -> Result<burn_p2p_core::ArtifactDescriptor, SwarmError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .unbounded_send(WasmBrowserSwarmCommand::FetchArtifactManifest {
                request,
                response_tx,
            })
            .map_err(|_| {
                SwarmError::Runtime("browser direct swarm command channel closed".into())
            })?;

        let response = response_rx.fuse();
        let timeout = TimeoutFuture::new(15_000).fuse();
        futures::pin_mut!(response, timeout);
        match futures::future::select(response, timeout).await {
            futures::future::Either::Left((result, _)) => result.map_err(|_| {
                SwarmError::Runtime(
                    "browser direct swarm artifact manifest response was dropped".into(),
                )
            })?,
            futures::future::Either::Right((_, _)) => Err(SwarmError::Runtime(
                "timed out waiting for browser direct swarm artifact manifest".into(),
            )),
        }
    }

    async fn fetch_artifact_chunk(
        &mut self,
        request: BrowserArtifactChunkRequest,
    ) -> Result<Vec<u8>, SwarmError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .unbounded_send(WasmBrowserSwarmCommand::FetchArtifactChunk {
                request,
                response_tx,
            })
            .map_err(|_| {
                SwarmError::Runtime("browser direct swarm command channel closed".into())
            })?;

        let response = response_rx.fuse();
        let timeout = TimeoutFuture::new(15_000).fuse();
        futures::pin_mut!(response, timeout);
        match futures::future::select(response, timeout).await {
            futures::future::Either::Left((result, _)) => result.map_err(|_| {
                SwarmError::Runtime(
                    "browser direct swarm artifact chunk response was dropped".into(),
                )
            })?,
            futures::future::Either::Right((_, _)) => Err(SwarmError::Runtime(
                "timed out waiting for browser direct swarm artifact chunk".into(),
            )),
        }
    }
}

#[cfg(target_arch = "wasm32")]
async fn run_wasm_browser_swarm_task(
    shared: Rc<RefCell<WasmBrowserSwarmSharedState>>,
    mut command_rx: mpsc::UnboundedReceiver<WasmBrowserSwarmCommand>,
    network_id: NetworkId,
) {
    let local_key = libp2p::identity::Keypair::generate_ed25519();
    let mut swarm = match build_wasm_browser_swarm(local_key, &network_id) {
        Ok(swarm) => swarm,
        Err(error) => {
            shared.borrow_mut().status.last_error = Some(error.to_string());
            while let Some(command) = command_rx.next().await {
                match command {
                    WasmBrowserSwarmCommand::Connect { response_tx, .. } => {
                        let _ = response_tx.send(Err(SwarmError::Runtime(error.to_string())));
                    }
                    WasmBrowserSwarmCommand::Disconnect { response_tx } => {
                        let _ = response_tx.send(Err(SwarmError::Runtime(error.to_string())));
                    }
                    WasmBrowserSwarmCommand::SubscribeDirectory { response_tx }
                    | WasmBrowserSwarmCommand::SubscribeHeads { response_tx }
                    | WasmBrowserSwarmCommand::SubscribeMetrics { response_tx } => {
                        let _ = response_tx.send(Err(SwarmError::Runtime(error.to_string())));
                    }
                    WasmBrowserSwarmCommand::FetchSnapshot { response_tx } => {
                        let _ = response_tx.send(Err(SwarmError::Runtime(error.to_string())));
                    }
                    WasmBrowserSwarmCommand::FetchArtifactManifest { response_tx, .. } => {
                        let _ = response_tx.send(Err(SwarmError::Runtime(error.to_string())));
                    }
                    WasmBrowserSwarmCommand::FetchArtifactChunk { response_tx, .. } => {
                        let _ = response_tx.send(Err(SwarmError::Runtime(error.to_string())));
                    }
                }
            }
            return;
        }
    };

    let mut pending_connect: Option<WasmPendingConnect> = None;
    let mut active_candidate: Option<BrowserSeedDialCandidate> = None;
    let mut current_bootstrap: Option<BrowserSwarmBootstrap> = None;
    let mut current_snapshot: Option<ControlPlaneSnapshot> = None;
    let mut connected_peer_transports = BTreeMap::<PeerId, BrowserTransportFamily>::new();
    let mut directory_subscribed = false;
    let mut heads_subscribed = false;
    let mut metrics_subscribed = false;
    let mut subscribed_topic_paths = BTreeSet::<String>::new();
    let mut attempted_mesh_candidate_urls = BTreeSet::<String>::new();
    let mut pending_subscription_snapshot_request_id: Option<String> = None;
    let mut pending_snapshot_requests =
        BTreeMap::<String, oneshot::Sender<Result<ControlPlaneSnapshot, SwarmError>>>::new();
    let mut pending_manifest_requests = BTreeMap::<
        String,
        oneshot::Sender<Result<burn_p2p_core::ArtifactDescriptor, SwarmError>>,
    >::new();
    let mut pending_chunk_requests =
        BTreeMap::<String, oneshot::Sender<Result<Vec<u8>, SwarmError>>>::new();

    loop {
        let next_command = command_rx.next().fuse();
        let next_swarm_event = swarm.select_next_some().fuse();
        futures::pin_mut!(next_command, next_swarm_event);

        match futures::future::select(next_command, next_swarm_event).await {
            futures::future::Either::Left((command, _)) => {
                let Some(command) = command else {
                    break;
                };
                match command {
                    WasmBrowserSwarmCommand::Connect {
                        bootstrap,
                        response_tx,
                    } => {
                        if bootstrap.network_id != network_id {
                            let _ = response_tx.send(Err(SwarmError::Runtime(format!(
                                "browser direct swarm runtime was created for {} but connect requested {}",
                                network_id.as_str(),
                                bootstrap.network_id.as_str()
                            ))));
                            continue;
                        }
                        for peer_id in swarm.connected_peers().cloned().collect::<Vec<_>>() {
                            let _ = swarm.disconnect_peer_id(peer_id);
                        }
                        fail_pending_wasm_snapshot_requests(
                            &mut pending_snapshot_requests,
                            "browser direct swarm reconnect discarded pending snapshot request",
                        );
                        fail_pending_wasm_manifest_requests(
                            &mut pending_manifest_requests,
                            "browser direct swarm reconnect discarded pending artifact manifest request",
                        );
                        fail_pending_wasm_chunk_requests(
                            &mut pending_chunk_requests,
                            "browser direct swarm reconnect discarded pending artifact chunk request",
                        );
                        current_bootstrap = Some(bootstrap.clone());
                        current_snapshot = None;
                        connected_peer_transports.clear();
                        subscribed_topic_paths.clear();
                        attempted_mesh_candidate_urls.clear();
                        let dial_plan = plan_browser_seed_dials(&bootstrap);
                        let supported = wasm_supported_browser_transport_families();
                        let candidates =
                            filter_supported_browser_seed_dial_candidates(&dial_plan, &supported);
                        let last_error = if candidates.is_empty() {
                            Some(
                                "no supported browser-capable libp2p seed candidates were available"
                                    .to_owned(),
                            )
                        } else {
                            None
                        };
                        {
                            let mut state = shared.borrow_mut();
                            state.status = BrowserSwarmStatus {
                                phase: if dial_plan.desired_transport.is_some() {
                                    BrowserSwarmPhase::TransportSelected
                                } else if !bootstrap.seed_bootstrap.seed_node_urls.is_empty() {
                                    BrowserSwarmPhase::SeedResolved
                                } else {
                                    BrowserSwarmPhase::Bootstrap
                                },
                                seed_bootstrap: bootstrap.seed_bootstrap,
                                transport_source:
                                    burn_p2p_core::BrowserTransportObservationSource::Selected,
                                desired_transport: dial_plan.desired_transport.clone(),
                                connected_transport: None,
                                connected_peer_count: 0,
                                connected_peer_ids: Vec::new(),
                                directory_synced: false,
                                assignment_bound: false,
                                head_synced: false,
                                artifact_source: burn_p2p_core::BrowserArtifactSource::Unavailable,
                                last_error: last_error
                                    .clone()
                                    .or_else(|| dial_plan.warnings.first().cloned()),
                            };
                            state.dial_plan = Some(dial_plan.clone());
                        }
                        active_candidate = None;
                        pending_connect = Some(WasmPendingConnect {
                            dial_plan: dial_plan.clone(),
                            candidates,
                            next_candidate_index: 0,
                            response_tx: None,
                        });
                        if let Err(error) = dial_next_wasm_browser_seed_candidate(
                            &mut swarm,
                            &shared,
                            pending_connect.as_mut().expect("pending connect"),
                            &mut active_candidate,
                        ) {
                            if let Some(mut pending) = pending_connect.take()
                                && let Some(response_tx) = pending.response_tx.take()
                            {
                                let _ =
                                    response_tx.send(Err(SwarmError::Runtime(error.to_string())));
                            }
                            shared.borrow_mut().status.last_error = Some(error.to_string());
                        } else {
                            let _ = response_tx.send(Ok(dial_plan));
                        }
                    }
                    WasmBrowserSwarmCommand::Disconnect { response_tx } => {
                        active_candidate = None;
                        pending_connect = None;
                        current_bootstrap = None;
                        current_snapshot = None;
                        connected_peer_transports.clear();
                        pending_subscription_snapshot_request_id = None;
                        directory_subscribed = false;
                        heads_subscribed = false;
                        metrics_subscribed = false;
                        subscribed_topic_paths.clear();
                        attempted_mesh_candidate_urls.clear();
                        fail_pending_wasm_snapshot_requests(
                            &mut pending_snapshot_requests,
                            "browser direct swarm disconnected before snapshot request completed",
                        );
                        fail_pending_wasm_manifest_requests(
                            &mut pending_manifest_requests,
                            "browser direct swarm disconnected before artifact manifest request completed",
                        );
                        fail_pending_wasm_chunk_requests(
                            &mut pending_chunk_requests,
                            "browser direct swarm disconnected before artifact chunk request completed",
                        );
                        for peer_id in swarm.connected_peers().cloned().collect::<Vec<_>>() {
                            let _ = swarm.disconnect_peer_id(peer_id);
                        }
                        {
                            let mut state = shared.borrow_mut();
                            state.status = BrowserSwarmStatus::default();
                            state.dial_plan = None;
                        }
                        let _ = response_tx.send(Ok(()));
                    }
                    WasmBrowserSwarmCommand::SubscribeDirectory { response_tx } => {
                        directory_subscribed = true;
                        let result = ensure_wasm_browser_topic_subscriptions(
                            &mut swarm,
                            current_bootstrap.as_ref(),
                            current_snapshot.as_ref(),
                            directory_subscribed,
                            heads_subscribed,
                            metrics_subscribed,
                            &mut subscribed_topic_paths,
                        );
                        let _ = response_tx.send(result);
                        try_issue_wasm_subscription_snapshot_request(
                            &mut swarm,
                            &shared,
                            current_snapshot.as_ref(),
                            directory_subscribed,
                            heads_subscribed,
                            metrics_subscribed,
                            &mut pending_subscription_snapshot_request_id,
                        );
                    }
                    WasmBrowserSwarmCommand::SubscribeHeads { response_tx } => {
                        heads_subscribed = true;
                        let result = ensure_wasm_browser_topic_subscriptions(
                            &mut swarm,
                            current_bootstrap.as_ref(),
                            current_snapshot.as_ref(),
                            directory_subscribed,
                            heads_subscribed,
                            metrics_subscribed,
                            &mut subscribed_topic_paths,
                        );
                        let _ = response_tx.send(result);
                        try_issue_wasm_subscription_snapshot_request(
                            &mut swarm,
                            &shared,
                            current_snapshot.as_ref(),
                            directory_subscribed,
                            heads_subscribed,
                            metrics_subscribed,
                            &mut pending_subscription_snapshot_request_id,
                        );
                    }
                    WasmBrowserSwarmCommand::SubscribeMetrics { response_tx } => {
                        metrics_subscribed = true;
                        let result = ensure_wasm_browser_topic_subscriptions(
                            &mut swarm,
                            current_bootstrap.as_ref(),
                            current_snapshot.as_ref(),
                            directory_subscribed,
                            heads_subscribed,
                            metrics_subscribed,
                            &mut subscribed_topic_paths,
                        );
                        let _ = response_tx.send(result);
                        try_issue_wasm_subscription_snapshot_request(
                            &mut swarm,
                            &shared,
                            current_snapshot.as_ref(),
                            directory_subscribed,
                            heads_subscribed,
                            metrics_subscribed,
                            &mut pending_subscription_snapshot_request_id,
                        );
                    }
                    WasmBrowserSwarmCommand::FetchSnapshot { response_tx } => {
                        match resolve_wasm_browser_request_peer_id(
                            &shared.borrow().status.connected_peer_ids,
                            &[],
                        ) {
                            Ok(peer_id) => {
                                let request_id = swarm
                                    .behaviour_mut()
                                    .request_response
                                    .send_request(&peer_id, ControlPlaneRequest::Snapshot)
                                    .to_string();
                                pending_snapshot_requests.insert(request_id, response_tx);
                            }
                            Err(error) => {
                                let _ = response_tx.send(Err(error));
                            }
                        }
                    }
                    WasmBrowserSwarmCommand::FetchArtifactManifest {
                        request,
                        response_tx,
                    } => {
                        match resolve_wasm_browser_request_peer_id(
                            &shared.borrow().status.connected_peer_ids,
                            &request.provider_peer_ids,
                        ) {
                            Ok(peer_id) => {
                                let request_id = swarm
                                    .behaviour_mut()
                                    .request_response
                                    .send_request(
                                        &peer_id,
                                        ControlPlaneRequest::ArtifactManifest {
                                            artifact_id: request.artifact_id,
                                        },
                                    )
                                    .to_string();
                                pending_manifest_requests.insert(request_id, response_tx);
                            }
                            Err(error) => {
                                let _ = response_tx.send(Err(error));
                            }
                        }
                    }
                    WasmBrowserSwarmCommand::FetchArtifactChunk {
                        request,
                        response_tx,
                    } => {
                        match resolve_wasm_browser_request_peer_id(
                            &shared.borrow().status.connected_peer_ids,
                            &request.provider_peer_ids,
                        ) {
                            Ok(peer_id) => {
                                let request_id = swarm
                                    .behaviour_mut()
                                    .request_response
                                    .send_request(
                                        &peer_id,
                                        ControlPlaneRequest::ArtifactChunk {
                                            artifact_id: request.artifact_id,
                                            chunk_id: request.chunk_id,
                                        },
                                    )
                                    .to_string();
                                pending_chunk_requests.insert(request_id, response_tx);
                            }
                            Err(error) => {
                                let _ = response_tx.send(Err(error));
                            }
                        }
                    }
                }
            }
            futures::future::Either::Right((event, _)) => match event {
                SwarmEvent::ConnectionEstablished {
                    peer_id, endpoint, ..
                } => {
                    let observed_peer_id = PeerId::new(peer_id.to_string());
                    let connected_transport =
                        browser_transport_family_for_connected_point(&endpoint).or_else(|| {
                            active_candidate
                                .as_ref()
                                .map(|candidate| candidate.transport.clone())
                                .or_else(|| {
                                    shared
                                        .borrow()
                                        .dial_plan
                                        .as_ref()
                                        .and_then(|plan| plan.desired_transport.clone())
                                })
                        });
                    if let Some(transport) = connected_transport.clone() {
                        connected_peer_transports.insert(observed_peer_id.clone(), transport);
                    }
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    {
                        let mut state = shared.borrow_mut();
                        state.status.phase = BrowserSwarmPhase::TransportConnected;
                        state.status.transport_source =
                            BrowserTransportObservationSource::Connected;
                        state.status.connected_transport =
                            preferred_connected_browser_transport(&connected_peer_transports)
                                .or(connected_transport);
                        if !state.status.connected_peer_ids.contains(&observed_peer_id) {
                            state.status.connected_peer_ids.push(observed_peer_id);
                        }
                        state.status.connected_peer_count = state.status.connected_peer_ids.len();
                        state.status.last_error = None;
                    }
                    if let Some(mut pending) = pending_connect.take()
                        && let Some(response_tx) = pending.response_tx.take()
                    {
                        let _ = response_tx.send(Ok(pending.dial_plan));
                    }
                    let _ = ensure_wasm_browser_topic_subscriptions(
                        &mut swarm,
                        current_bootstrap.as_ref(),
                        current_snapshot.as_ref(),
                        directory_subscribed,
                        heads_subscribed,
                        metrics_subscribed,
                        &mut subscribed_topic_paths,
                    );
                    try_issue_wasm_subscription_snapshot_request(
                        &mut swarm,
                        &shared,
                        current_snapshot.as_ref(),
                        directory_subscribed,
                        heads_subscribed,
                        metrics_subscribed,
                        &mut pending_subscription_snapshot_request_id,
                    );
                    maybe_dial_wasm_browser_direct_seed_peers(
                        &mut swarm,
                        &shared,
                        current_bootstrap.as_ref(),
                        &connected_peer_transports,
                        &mut attempted_mesh_candidate_urls,
                    );
                    maybe_dial_wasm_browser_mesh_peers(
                        &mut swarm,
                        &shared,
                        current_bootstrap.as_ref(),
                        current_snapshot.as_ref(),
                        &connected_peer_transports,
                        &mut attempted_mesh_candidate_urls,
                    );
                    disconnect_wasm_browser_peers(
                        &mut swarm,
                        browser_wss_fallback_peers_to_disconnect(
                            current_snapshot.as_ref(),
                            &connected_peer_transports,
                        ),
                    );
                }
                SwarmEvent::OutgoingConnectionError { error, .. } => {
                    let error_message = error.to_string();
                    shared.borrow_mut().status.last_error = Some(error_message.clone());
                    if let Some(pending) = pending_connect.as_mut() {
                        if let Err(next_error) = dial_next_wasm_browser_seed_candidate(
                            &mut swarm,
                            &shared,
                            pending,
                            &mut active_candidate,
                        ) {
                            if let Some(mut pending) = pending_connect.take()
                                && let Some(response_tx) = pending.response_tx.take()
                            {
                                let _ = response_tx
                                    .send(Err(SwarmError::Runtime(next_error.to_string())));
                            }
                            shared.borrow_mut().status.last_error = Some(next_error.to_string());
                        }
                    }
                }
                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    swarm
                        .behaviour_mut()
                        .gossipsub
                        .remove_explicit_peer(&peer_id);
                    let desired_transport = shared
                        .borrow()
                        .dial_plan
                        .as_ref()
                        .and_then(|plan| plan.desired_transport.clone());
                    let closed_peer_id = PeerId::new(peer_id.to_string());
                    connected_peer_transports.remove(&closed_peer_id);
                    let connected_transport =
                        preferred_connected_browser_transport(&connected_peer_transports);
                    let mut state = shared.borrow_mut();
                    state
                        .status
                        .connected_peer_ids
                        .retain(|existing| existing != &closed_peer_id);
                    let has_connected_peers = !state.status.connected_peer_ids.is_empty();
                    state.status.connected_peer_count = state.status.connected_peer_ids.len();
                    state.status.connected_transport = connected_transport;
                    if has_connected_peers {
                        update_wasm_browser_status_from_snapshot(
                            &mut state.status,
                            current_snapshot.as_ref(),
                            current_bootstrap.as_ref(),
                        );
                    } else {
                        state.status.phase = if desired_transport.is_some() {
                            BrowserSwarmPhase::TransportSelected
                        } else if !state.status.seed_bootstrap.seed_node_urls.is_empty() {
                            BrowserSwarmPhase::SeedResolved
                        } else {
                            BrowserSwarmPhase::Bootstrap
                        };
                        state.status.transport_source = BrowserTransportObservationSource::Selected;
                        state.status.connected_transport = None;
                        update_wasm_browser_status_from_snapshot(
                            &mut state.status,
                            None,
                            current_bootstrap.as_ref(),
                        );
                    }
                    drop(state);
                    pending_subscription_snapshot_request_id = None;
                    if has_connected_peers {
                        maybe_dial_wasm_browser_direct_seed_peers(
                            &mut swarm,
                            &shared,
                            current_bootstrap.as_ref(),
                            &connected_peer_transports,
                            &mut attempted_mesh_candidate_urls,
                        );
                        maybe_dial_wasm_browser_mesh_peers(
                            &mut swarm,
                            &shared,
                            current_bootstrap.as_ref(),
                            current_snapshot.as_ref(),
                            &connected_peer_transports,
                            &mut attempted_mesh_candidate_urls,
                        );
                        disconnect_wasm_browser_peers(
                            &mut swarm,
                            browser_wss_fallback_peers_to_disconnect(
                                current_snapshot.as_ref(),
                                &connected_peer_transports,
                            ),
                        );
                    } else {
                        fail_pending_wasm_snapshot_requests(
                            &mut pending_snapshot_requests,
                            "browser direct swarm disconnected before snapshot request completed",
                        );
                        fail_pending_wasm_manifest_requests(
                            &mut pending_manifest_requests,
                            "browser direct swarm disconnected before artifact manifest request completed",
                        );
                        fail_pending_wasm_chunk_requests(
                            &mut pending_chunk_requests,
                            "browser direct swarm disconnected before artifact chunk request completed",
                        );
                        attempted_mesh_candidate_urls.clear();
                        maybe_dial_wasm_browser_mesh_peers(
                            &mut swarm,
                            &shared,
                            current_bootstrap.as_ref(),
                            current_snapshot.as_ref(),
                            &connected_peer_transports,
                            &mut attempted_mesh_candidate_urls,
                        );
                        reconnect_wasm_browser_seed_candidates(
                            &mut swarm,
                            &shared,
                            &mut pending_connect,
                            &mut active_candidate,
                        );
                    }
                }
                SwarmEvent::Behaviour(WasmBrowserSwarmBehaviourEvent::Gossipsub(event)) => {
                    match event {
                        gossipsub::Event::Message { message, .. } => {
                            match serde_json::from_slice::<PubsubEnvelope>(&message.data) {
                                Ok(envelope) => {
                                    let mut next_snapshot =
                                        current_snapshot.clone().unwrap_or_default();
                                    let previous_snapshot = next_snapshot.clone();
                                    apply_pubsub_payload(&mut next_snapshot, envelope.payload);
                                    if next_snapshot != previous_snapshot {
                                        update_wasm_browser_status_and_snapshot(
                                            &shared,
                                            &next_snapshot,
                                            current_bootstrap.as_ref(),
                                        );
                                        let _ = ensure_wasm_browser_topic_subscriptions(
                                            &mut swarm,
                                            current_bootstrap.as_ref(),
                                            Some(&next_snapshot),
                                            directory_subscribed,
                                            heads_subscribed,
                                            metrics_subscribed,
                                            &mut subscribed_topic_paths,
                                        );
                                        shared.borrow_mut().updates.push(
                                            BrowserSwarmUpdate::Snapshot(Box::new(
                                                next_snapshot.clone(),
                                            )),
                                        );
                                        current_snapshot = Some(next_snapshot);
                                        maybe_dial_wasm_browser_direct_seed_peers(
                                            &mut swarm,
                                            &shared,
                                            current_bootstrap.as_ref(),
                                            &connected_peer_transports,
                                            &mut attempted_mesh_candidate_urls,
                                        );
                                        maybe_dial_wasm_browser_mesh_peers(
                                            &mut swarm,
                                            &shared,
                                            current_bootstrap.as_ref(),
                                            current_snapshot.as_ref(),
                                            &connected_peer_transports,
                                            &mut attempted_mesh_candidate_urls,
                                        );
                                    }
                                }
                                Err(error) => {
                                    shared.borrow_mut().status.last_error = Some(format!(
                                        "browser direct swarm pubsub decode failed: {error}"
                                    ));
                                }
                            }
                        }
                        gossipsub::Event::Subscribed { peer_id, .. } => {
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        }
                        gossipsub::Event::Unsubscribed { peer_id, .. } => {
                            swarm
                                .behaviour_mut()
                                .gossipsub
                                .remove_explicit_peer(&peer_id);
                        }
                        _ => {}
                    }
                }
                SwarmEvent::Behaviour(WasmBrowserSwarmBehaviourEvent::RequestResponse(event)) => {
                    match event {
                        libp2p_request_response::Event::Message { peer, message, .. } => {
                            let peer_id = PeerId::new(peer.to_string());
                            match message {
                                libp2p_request_response::Message::Response {
                                    request_id,
                                    response,
                                } => {
                                    let request_id = request_id.to_string();
                                    match response {
                                        ControlPlaneResponse::Snapshot(snapshot) => {
                                            if pending_subscription_snapshot_request_id
                                                .as_ref()
                                                .is_some_and(|pending_id| pending_id == &request_id)
                                            {
                                                pending_subscription_snapshot_request_id = None;
                                                if current_snapshot
                                                    .as_ref()
                                                    .is_none_or(|previous| previous != &snapshot)
                                                {
                                                    update_wasm_browser_status_and_snapshot(
                                                        &shared,
                                                        &snapshot,
                                                        current_bootstrap.as_ref(),
                                                    );
                                                    let _ = ensure_wasm_browser_topic_subscriptions(
                                                        &mut swarm,
                                                        current_bootstrap.as_ref(),
                                                        Some(&snapshot),
                                                        directory_subscribed,
                                                        heads_subscribed,
                                                        metrics_subscribed,
                                                        &mut subscribed_topic_paths,
                                                    );
                                                    shared.borrow_mut().updates.push(
                                                        BrowserSwarmUpdate::Snapshot(Box::new(
                                                            snapshot.clone(),
                                                        )),
                                                    );
                                                    current_snapshot = Some(snapshot);
                                                    maybe_dial_wasm_browser_direct_seed_peers(
                                                        &mut swarm,
                                                        &shared,
                                                        current_bootstrap.as_ref(),
                                                        &connected_peer_transports,
                                                        &mut attempted_mesh_candidate_urls,
                                                    );
                                                    maybe_dial_wasm_browser_mesh_peers(
                                                        &mut swarm,
                                                        &shared,
                                                        current_bootstrap.as_ref(),
                                                        current_snapshot.as_ref(),
                                                        &connected_peer_transports,
                                                        &mut attempted_mesh_candidate_urls,
                                                    );
                                                }
                                            } else if let Some(response_tx) =
                                                pending_snapshot_requests.remove(&request_id)
                                            {
                                                update_wasm_browser_status_and_snapshot(
                                                    &shared,
                                                    &snapshot,
                                                    current_bootstrap.as_ref(),
                                                );
                                                let _ = ensure_wasm_browser_topic_subscriptions(
                                                    &mut swarm,
                                                    current_bootstrap.as_ref(),
                                                    Some(&snapshot),
                                                    directory_subscribed,
                                                    heads_subscribed,
                                                    metrics_subscribed,
                                                    &mut subscribed_topic_paths,
                                                );
                                                current_snapshot = Some(snapshot.clone());
                                                maybe_dial_wasm_browser_direct_seed_peers(
                                                    &mut swarm,
                                                    &shared,
                                                    current_bootstrap.as_ref(),
                                                    &connected_peer_transports,
                                                    &mut attempted_mesh_candidate_urls,
                                                );
                                                maybe_dial_wasm_browser_mesh_peers(
                                                    &mut swarm,
                                                    &shared,
                                                    current_bootstrap.as_ref(),
                                                    current_snapshot.as_ref(),
                                                    &connected_peer_transports,
                                                    &mut attempted_mesh_candidate_urls,
                                                );
                                                let _ = response_tx.send(Ok(snapshot));
                                            }
                                        }
                                        ControlPlaneResponse::ArtifactManifest(descriptor) => {
                                            if let Some(response_tx) =
                                                pending_manifest_requests.remove(&request_id)
                                            {
                                                let _ = response_tx.send(descriptor.ok_or_else(
                                                    || {
                                                        SwarmError::Request(format!(
                                                            "peer {} did not advertise the requested artifact manifest",
                                                            peer_id.as_str()
                                                        ))
                                                    },
                                                ));
                                            }
                                        }
                                        ControlPlaneResponse::ArtifactChunk(payload) => {
                                            if let Some(response_tx) =
                                                pending_chunk_requests.remove(&request_id)
                                            {
                                                if payload.is_some() {
                                                    let mut state = shared.borrow_mut();
                                                    state.status.artifact_source =
                                                        burn_p2p_core::BrowserArtifactSource::PeerSwarm;
                                                    state.status.phase =
                                                        BrowserSwarmPhase::ArtifactReady;
                                                }
                                                let _ = response_tx.send(payload
                                                    .map(|payload| payload.bytes)
                                                    .ok_or_else(|| {
                                                        SwarmError::Request(format!(
                                                            "peer {} did not advertise the requested artifact chunk",
                                                            peer_id.as_str()
                                                        ))
                                                    }));
                                            }
                                        }
                                    }
                                }
                                libp2p_request_response::Message::Request { .. } => {}
                            }
                        }
                        libp2p_request_response::Event::OutboundFailure {
                            request_id,
                            error,
                            ..
                        } => {
                            let request_id = request_id.to_string();
                            let error_message = error.to_string();
                            if pending_subscription_snapshot_request_id
                                .as_ref()
                                .is_some_and(|pending_id| pending_id == &request_id)
                            {
                                pending_subscription_snapshot_request_id = None;
                            }
                            if let Some(response_tx) = pending_snapshot_requests.remove(&request_id)
                            {
                                let _ = response_tx
                                    .send(Err(SwarmError::Request(error_message.clone())));
                            }
                            if let Some(response_tx) = pending_manifest_requests.remove(&request_id)
                            {
                                let _ = response_tx
                                    .send(Err(SwarmError::Request(error_message.clone())));
                            }
                            if let Some(response_tx) = pending_chunk_requests.remove(&request_id) {
                                let _ = response_tx
                                    .send(Err(SwarmError::Request(error_message.clone())));
                            }
                            shared.borrow_mut().status.last_error = Some(error_message);
                        }
                        libp2p_request_response::Event::InboundFailure { .. }
                        | libp2p_request_response::Event::ResponseSent { .. } => {}
                    }
                }
                SwarmEvent::Behaviour(_) => {}
                _ => {}
            },
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn dial_next_wasm_browser_seed_candidate(
    swarm: &mut libp2p::Swarm<WasmBrowserSwarmBehaviour>,
    shared: &Rc<RefCell<WasmBrowserSwarmSharedState>>,
    pending: &mut WasmPendingConnect,
    active_candidate: &mut Option<BrowserSeedDialCandidate>,
) -> Result<(), SwarmError> {
    while let Some(candidate) = pending.next_candidate() {
        let address = candidate
            .seed_url
            .parse::<Multiaddr>()
            .map_err(|error| SwarmError::InvalidAddress(error.to_string()))?;
        match swarm.dial(address) {
            Ok(()) => {
                *active_candidate = Some(candidate.clone());
                let mut state = shared.borrow_mut();
                state.status.phase = BrowserSwarmPhase::TransportSelected;
                state.status.transport_source =
                    burn_p2p_core::BrowserTransportObservationSource::Selected;
                state.status.desired_transport = Some(candidate.transport);
                state.status.connected_transport = None;
                state.status.connected_peer_ids.clear();
                state.status.connected_peer_count = 0;
                state.status.last_error = None;
                return Ok(());
            }
            Err(error) => {
                shared.borrow_mut().status.last_error = Some(error.to_string());
            }
        }
    }

    Err(SwarmError::Runtime(
        "browser direct swarm could not dial any supported seed candidate".into(),
    ))
}

#[cfg(target_arch = "wasm32")]
fn wasm_supported_browser_transport_families() -> Vec<BrowserTransportFamily> {
    vec![
        BrowserTransportFamily::WebRtcDirect,
        BrowserTransportFamily::WebTransport,
        BrowserTransportFamily::WssFallback,
    ]
}

#[cfg(target_arch = "wasm32")]
fn build_wasm_browser_swarm(
    keypair: libp2p::identity::Keypair,
    network_id: &NetworkId,
) -> Result<libp2p::Swarm<WasmBrowserSwarmBehaviour>, SwarmError> {
    let control_protocol = stream_protocol(&ProtocolSet::for_network(network_id)?.control)?;
    let behaviour_keypair = keypair.clone();
    let gossip_config = gossipsub::ConfigBuilder::default()
        .validation_mode(gossipsub::ValidationMode::Strict)
        .message_id_fn(|message| {
            gossipsub::MessageId::from(pubsub_semantic_message_id(&message.data))
        })
        .build()
        .map_err(|error| SwarmError::Runtime(error.to_string()))?;
    SwarmBuilder::with_existing_identity(keypair)
        .with_wasm_bindgen()
        .with_other_transport(|key| build_wasm_webrtc_transport(key))
        .map_err(|error| SwarmError::Runtime(error.to_string()))?
        .with_other_transport(|key| build_wasm_webtransport_transport(key))
        .map_err(|error| SwarmError::Runtime(error.to_string()))?
        .with_other_transport(|key| build_wasm_websocket_transport(key))
        .map_err(|error| SwarmError::Runtime(error.to_string()))?
        .with_behaviour(|_| WasmBrowserSwarmBehaviour {
            ping: libp2p::ping::Behaviour::default(),
            gossipsub: gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(behaviour_keypair.clone()),
                gossip_config.clone(),
            )
            .expect("wasm gossipsub behaviour"),
            request_response: libp2p_request_response::cbor::Behaviour::new(
                [(
                    control_protocol,
                    libp2p_request_response::ProtocolSupport::Outbound,
                )],
                libp2p_request_response::Config::default(),
            ),
        })
        .map_err(|error| SwarmError::Runtime(error.to_string()))
        .map(|builder| builder.build())
}

#[cfg(target_arch = "wasm32")]
fn resolve_wasm_browser_request_peer_id(
    connected_peer_ids: &[PeerId],
    requested_provider_peer_ids: &[PeerId],
) -> Result<libp2p::identity::PeerId, SwarmError> {
    requested_provider_peer_ids
        .iter()
        .find(|peer_id| connected_peer_ids.contains(peer_id))
        .or_else(|| connected_peer_ids.first())
        .ok_or_else(|| {
            SwarmError::Runtime(
                "browser direct swarm has no connected peer to service the request".into(),
            )
        })
        .and_then(|peer_id| {
            peer_id
                .as_str()
                .parse::<libp2p::identity::PeerId>()
                .map_err(|_| SwarmError::InvalidPeerId(peer_id.as_str().to_owned()))
        })
}

#[cfg(target_arch = "wasm32")]
fn try_issue_wasm_subscription_snapshot_request(
    swarm: &mut libp2p::Swarm<WasmBrowserSwarmBehaviour>,
    shared: &Rc<RefCell<WasmBrowserSwarmSharedState>>,
    current_snapshot: Option<&ControlPlaneSnapshot>,
    directory_subscribed: bool,
    heads_subscribed: bool,
    metrics_subscribed: bool,
    pending_subscription_snapshot_request_id: &mut Option<String>,
) {
    if pending_subscription_snapshot_request_id.is_some() {
        return;
    }
    if !(directory_subscribed || heads_subscribed || metrics_subscribed) {
        return;
    }
    if current_snapshot.is_some() {
        return;
    }
    let Ok(peer_id) =
        resolve_wasm_browser_request_peer_id(&shared.borrow().status.connected_peer_ids, &[])
    else {
        return;
    };
    let request_id = swarm
        .behaviour_mut()
        .request_response
        .send_request(&peer_id, ControlPlaneRequest::Snapshot)
        .to_string();
    *pending_subscription_snapshot_request_id = Some(request_id);
}

#[cfg(target_arch = "wasm32")]
fn maybe_dial_wasm_browser_direct_seed_peers(
    swarm: &mut libp2p::Swarm<WasmBrowserSwarmBehaviour>,
    shared: &Rc<RefCell<WasmBrowserSwarmSharedState>>,
    bootstrap: Option<&BrowserSwarmBootstrap>,
    connected_peer_transports: &BTreeMap<PeerId, BrowserTransportFamily>,
    attempted_mesh_candidate_urls: &mut BTreeSet<String>,
) {
    let Some(bootstrap) = bootstrap else {
        return;
    };
    let connected_peer_ids = shared.borrow().status.connected_peer_ids.clone();
    if connected_peer_ids.is_empty() {
        return;
    }
    let supported_transports = wasm_supported_browser_transport_families();
    let dial_budget =
        browser_additional_direct_connection_budget(&connected_peer_ids, connected_peer_transports);
    for candidate in browser_direct_seed_retry_candidates(
        bootstrap,
        &supported_transports,
        connected_peer_transports,
        attempted_mesh_candidate_urls,
    )
    .into_iter()
    .take(dial_budget)
    {
        let Ok(address) = candidate.seed_url.parse::<Multiaddr>() else {
            continue;
        };
        attempted_mesh_candidate_urls.insert(candidate.seed_url.clone());
        if let Err(error) = swarm.dial(address) {
            shared.borrow_mut().status.last_error = Some(error.to_string());
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn maybe_dial_wasm_browser_mesh_peers(
    swarm: &mut libp2p::Swarm<WasmBrowserSwarmBehaviour>,
    shared: &Rc<RefCell<WasmBrowserSwarmSharedState>>,
    bootstrap: Option<&BrowserSwarmBootstrap>,
    current_snapshot: Option<&ControlPlaneSnapshot>,
    connected_peer_transports: &BTreeMap<PeerId, BrowserTransportFamily>,
    attempted_mesh_candidate_urls: &mut BTreeSet<String>,
) {
    let Some(bootstrap) = bootstrap else {
        return;
    };
    let Some(snapshot) = current_snapshot else {
        return;
    };
    let connected_peer_ids = shared.borrow().status.connected_peer_ids.clone();
    let available_slots =
        browser_additional_direct_connection_budget(&connected_peer_ids, connected_peer_transports);
    if available_slots == 0 {
        return;
    }
    let supported_transports = wasm_supported_browser_transport_families();
    for candidate in browser_peer_directory_dial_candidates(
        snapshot,
        &bootstrap.transport_preference,
        &supported_transports,
        &connected_peer_ids,
        attempted_mesh_candidate_urls,
    )
    .into_iter()
    .take(available_slots)
    {
        let Ok(address) = candidate.seed_url.parse::<Multiaddr>() else {
            continue;
        };
        attempted_mesh_candidate_urls.insert(candidate.seed_url.clone());
        if let Err(error) = swarm.dial(address) {
            shared.borrow_mut().status.last_error = Some(error.to_string());
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn ensure_wasm_browser_topic_subscriptions(
    swarm: &mut libp2p::Swarm<WasmBrowserSwarmBehaviour>,
    bootstrap: Option<&BrowserSwarmBootstrap>,
    current_snapshot: Option<&ControlPlaneSnapshot>,
    directory_subscribed: bool,
    heads_subscribed: bool,
    metrics_subscribed: bool,
    subscribed_topic_paths: &mut BTreeSet<String>,
) -> Result<(), SwarmError> {
    for topic in desired_wasm_browser_subscription_topics(
        bootstrap,
        current_snapshot,
        directory_subscribed,
        heads_subscribed,
        metrics_subscribed,
    ) {
        if subscribed_topic_paths.insert(topic.path.clone()) {
            swarm
                .behaviour_mut()
                .gossipsub
                .subscribe(&gossipsub::IdentTopic::new(topic.path))
                .map_err(|error| SwarmError::Pubsub(error.to_string()))?;
        }
    }
    Ok(())
}

#[cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]
pub(crate) fn selected_browser_study_and_experiment(
    bootstrap: &BrowserSwarmBootstrap,
    snapshot: &ControlPlaneSnapshot,
) -> Option<(StudyId, ExperimentId)> {
    let announcement = snapshot
        .directory_announcements
        .iter()
        .rev()
        .find(|announcement| announcement.network_id == bootstrap.network_id)?;
    if let Some(experiment_id) = bootstrap.selected_experiment.as_ref() {
        let entry = announcement.entries.iter().find(|entry| {
            &entry.experiment_id == experiment_id
                && bootstrap
                    .selected_revision
                    .as_ref()
                    .is_none_or(|revision_id| &entry.current_revision_id == revision_id)
        })?;
        return Some((entry.study_id.clone(), entry.experiment_id.clone()));
    }
    announcement
        .entries
        .first()
        .map(|entry| (entry.study_id.clone(), entry.experiment_id.clone()))
}

#[cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]
pub(crate) fn desired_wasm_browser_subscription_topics(
    bootstrap: Option<&BrowserSwarmBootstrap>,
    current_snapshot: Option<&ControlPlaneSnapshot>,
    directory_subscribed: bool,
    heads_subscribed: bool,
    metrics_subscribed: bool,
) -> Vec<OverlayTopic> {
    if !(directory_subscribed || heads_subscribed || metrics_subscribed) {
        return Vec::new();
    }
    let Some(bootstrap) = bootstrap else {
        return Vec::new();
    };
    let mut topics = vec![OverlayTopic::control(bootstrap.network_id.clone())];
    if (heads_subscribed || metrics_subscribed)
        && let Some(snapshot) = current_snapshot
        && let Some((study_id, experiment_id)) =
            selected_browser_study_and_experiment(bootstrap, snapshot)
    {
        if heads_subscribed
            && let Ok(topic) = OverlayTopic::experiment(
                bootstrap.network_id.clone(),
                study_id.clone(),
                experiment_id.clone(),
                OverlayChannel::Heads,
            )
        {
            topics.push(topic);
        }
        if metrics_subscribed
            && let Ok(topic) = OverlayTopic::experiment(
                bootstrap.network_id.clone(),
                study_id,
                experiment_id,
                OverlayChannel::Metrics,
            )
        {
            topics.push(topic);
        }
    }
    let mut seen = BTreeSet::new();
    topics
        .into_iter()
        .filter(|topic| seen.insert(topic.path.clone()))
        .collect()
}

#[cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]
pub(crate) fn browser_peer_directory_dial_candidates(
    snapshot: &ControlPlaneSnapshot,
    transport_preference: &[BrowserTransportFamily],
    supported_transports: &[BrowserTransportFamily],
    connected_peer_ids: &[PeerId],
    attempted_candidate_urls: &BTreeSet<String>,
) -> Vec<BrowserSeedDialCandidate> {
    let mut candidates = Vec::new();
    for transport in transport_preference {
        if !supported_transports.contains(transport) {
            continue;
        }
        for announcement in snapshot.peer_directory_announcements.iter().rev() {
            if connected_peer_ids.contains(&announcement.peer_id) {
                continue;
            }
            for address in &announcement.addresses {
                let seed_url = address.as_str();
                if attempted_candidate_urls.contains(seed_url) {
                    continue;
                }
                let Some(family) = browser_transport_family_for_seed_url(seed_url) else {
                    continue;
                };
                if &family != transport {
                    continue;
                }
                if !candidates
                    .iter()
                    .any(|candidate: &BrowserSeedDialCandidate| {
                        candidate.seed_url == seed_url
                            || candidate.peer_id.as_ref() == Some(&announcement.peer_id)
                    })
                {
                    candidates.push(BrowserSeedDialCandidate {
                        seed_url: seed_url.to_owned(),
                        transport: family,
                        peer_id: Some(announcement.peer_id.clone()),
                    });
                }
            }
        }
    }
    candidates
}

#[cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]
pub(crate) fn preferred_connected_browser_transport(
    connected_peer_transports: &BTreeMap<PeerId, BrowserTransportFamily>,
) -> Option<BrowserTransportFamily> {
    [
        BrowserTransportFamily::WebRtcDirect,
        BrowserTransportFamily::WebTransport,
        BrowserTransportFamily::WssFallback,
    ]
    .into_iter()
    .find(|transport| {
        connected_peer_transports
            .values()
            .any(|family| family == transport)
    })
}

#[cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]
pub(crate) fn browser_wss_fallback_peers_to_disconnect(
    current_snapshot: Option<&ControlPlaneSnapshot>,
    connected_peer_transports: &BTreeMap<PeerId, BrowserTransportFamily>,
) -> Vec<PeerId> {
    let has_direct_peer = browser_has_direct_connected_peer(connected_peer_transports);
    if current_snapshot.is_none() || !has_direct_peer {
        return Vec::new();
    }
    connected_peer_transports
        .iter()
        .filter_map(|(peer_id, family)| {
            (*family == BrowserTransportFamily::WssFallback).then(|| peer_id.clone())
        })
        .collect()
}

#[cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]
pub(crate) fn browser_has_direct_connected_peer(
    connected_peer_transports: &BTreeMap<PeerId, BrowserTransportFamily>,
) -> bool {
    connected_peer_transports.values().any(|family| {
        matches!(
            family,
            BrowserTransportFamily::WebRtcDirect | BrowserTransportFamily::WebTransport
        )
    })
}

#[cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]
pub(crate) fn browser_additional_direct_connection_budget(
    connected_peer_ids: &[PeerId],
    connected_peer_transports: &BTreeMap<PeerId, BrowserTransportFamily>,
) -> usize {
    let available_slots =
        WASM_BROWSER_TARGET_CONNECTED_PEERS.saturating_sub(connected_peer_ids.len());
    if browser_has_direct_connected_peer(connected_peer_transports) {
        available_slots
    } else {
        available_slots.max(1)
    }
}

#[cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]
pub(crate) fn browser_direct_seed_retry_candidates(
    bootstrap: &BrowserSwarmBootstrap,
    supported_transports: &[BrowserTransportFamily],
    connected_peer_transports: &BTreeMap<PeerId, BrowserTransportFamily>,
    attempted_candidate_urls: &BTreeSet<String>,
) -> Vec<BrowserSeedDialCandidate> {
    if browser_has_direct_connected_peer(connected_peer_transports) {
        return Vec::new();
    }
    filter_supported_browser_seed_dial_candidates(
        &plan_browser_seed_dials(bootstrap),
        supported_transports,
    )
    .into_iter()
    .filter(|candidate| {
        candidate.transport != BrowserTransportFamily::WssFallback
            && !attempted_candidate_urls.contains(&candidate.seed_url)
    })
    .collect()
}

#[cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]
pub(crate) fn update_wasm_browser_status_from_snapshot(
    status: &mut BrowserSwarmStatus,
    snapshot: Option<&ControlPlaneSnapshot>,
    bootstrap: Option<&BrowserSwarmBootstrap>,
) {
    let Some(snapshot) = snapshot else {
        status.directory_synced = false;
        status.assignment_bound = false;
        status.head_synced = false;
        status.artifact_source = burn_p2p_core::BrowserArtifactSource::Unavailable;
        if status.connected_transport.is_some() {
            status.phase = BrowserSwarmPhase::TransportConnected;
        }
        return;
    };
    let Some(bootstrap) = bootstrap else {
        status.directory_synced = false;
        status.assignment_bound = false;
        status.head_synced = false;
        if status.connected_transport.is_some() {
            status.phase = BrowserSwarmPhase::TransportConnected;
        }
        return;
    };
    let directory_synced = snapshot
        .directory_announcements
        .iter()
        .rev()
        .any(|announcement| announcement.network_id == bootstrap.network_id);
    let assignment = selected_browser_study_and_experiment(bootstrap, snapshot);
    let head_synced = assignment
        .as_ref()
        .is_some_and(|(study_id, experiment_id)| {
            snapshot.head_announcements.iter().any(|announcement| {
                announcement.head.study_id == *study_id
                    && announcement.head.experiment_id == *experiment_id
            })
        });
    status.directory_synced = directory_synced;
    status.assignment_bound = assignment.is_some();
    status.head_synced = head_synced;
    if matches!(
        status.artifact_source,
        burn_p2p_core::BrowserArtifactSource::PeerSwarm
    ) {
        status.phase = BrowserSwarmPhase::ArtifactReady;
    } else if head_synced {
        status.phase = BrowserSwarmPhase::HeadSynced;
    } else if directory_synced {
        status.phase = BrowserSwarmPhase::DirectorySynced;
    } else if status.connected_transport.is_some() {
        status.phase = BrowserSwarmPhase::TransportConnected;
    }
}

#[cfg(target_arch = "wasm32")]
fn update_wasm_browser_status_and_snapshot(
    shared: &Rc<RefCell<WasmBrowserSwarmSharedState>>,
    snapshot: &ControlPlaneSnapshot,
    bootstrap: Option<&BrowserSwarmBootstrap>,
) {
    let mut state = shared.borrow_mut();
    update_wasm_browser_status_from_snapshot(&mut state.status, Some(snapshot), bootstrap);
}

#[cfg(target_arch = "wasm32")]
fn browser_transport_family_for_connected_point(
    endpoint: &libp2p::core::ConnectedPoint,
) -> Option<BrowserTransportFamily> {
    browser_transport_family_for_seed_url(&endpoint.get_remote_address().to_string())
}

#[cfg(target_arch = "wasm32")]
fn disconnect_wasm_browser_peers(
    swarm: &mut libp2p::Swarm<WasmBrowserSwarmBehaviour>,
    peer_ids: impl IntoIterator<Item = PeerId>,
) {
    for peer_id in peer_ids {
        if let Ok(libp2p_peer_id) = peer_id.as_str().parse::<libp2p::identity::PeerId>() {
            let _ = swarm.disconnect_peer_id(libp2p_peer_id);
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn reconnect_wasm_browser_seed_candidates(
    swarm: &mut libp2p::Swarm<WasmBrowserSwarmBehaviour>,
    shared: &Rc<RefCell<WasmBrowserSwarmSharedState>>,
    pending_connect: &mut Option<WasmPendingConnect>,
    active_candidate: &mut Option<BrowserSeedDialCandidate>,
) {
    if pending_connect.is_some() {
        return;
    }
    let Some(dial_plan) = shared.borrow().dial_plan.clone() else {
        return;
    };
    let supported = wasm_supported_browser_transport_families();
    let candidates = filter_supported_browser_seed_dial_candidates(&dial_plan, &supported);
    if candidates.is_empty() {
        return;
    }
    *pending_connect = Some(WasmPendingConnect {
        dial_plan,
        candidates,
        next_candidate_index: 0,
        response_tx: None,
    });
    if let Some(pending) = pending_connect.as_mut()
        && let Err(error) =
            dial_next_wasm_browser_seed_candidate(swarm, shared, pending, active_candidate)
    {
        shared.borrow_mut().status.last_error = Some(error.to_string());
        *pending_connect = None;
    }
}

#[cfg(target_arch = "wasm32")]
fn fail_pending_wasm_snapshot_requests(
    pending: &mut BTreeMap<String, oneshot::Sender<Result<ControlPlaneSnapshot, SwarmError>>>,
    message: &str,
) {
    for (_, response_tx) in std::mem::take(pending) {
        let _ = response_tx.send(Err(SwarmError::Runtime(message.to_owned())));
    }
}

#[cfg(target_arch = "wasm32")]
fn fail_pending_wasm_manifest_requests(
    pending: &mut BTreeMap<
        String,
        oneshot::Sender<Result<burn_p2p_core::ArtifactDescriptor, SwarmError>>,
    >,
    message: &str,
) {
    for (_, response_tx) in std::mem::take(pending) {
        let _ = response_tx.send(Err(SwarmError::Runtime(message.to_owned())));
    }
}

#[cfg(target_arch = "wasm32")]
fn fail_pending_wasm_chunk_requests(
    pending: &mut BTreeMap<String, oneshot::Sender<Result<Vec<u8>, SwarmError>>>,
    message: &str,
) {
    for (_, response_tx) in std::mem::take(pending) {
        let _ = response_tx.send(Err(SwarmError::Runtime(message.to_owned())));
    }
}

#[cfg(target_arch = "wasm32")]
fn build_wasm_websocket_transport(
    keypair: &libp2p::identity::Keypair,
) -> Result<
    libp2p::core::transport::Boxed<(
        libp2p::identity::PeerId,
        libp2p::core::muxing::StreamMuxerBox,
    )>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    Ok(libp2p::websocket_websys::Transport::default()
        .upgrade(Version::V1)
        .authenticate(libp2p::noise::Config::new(keypair)?)
        .multiplex(libp2p::yamux::Config::default())
        .boxed())
}

#[cfg(target_arch = "wasm32")]
fn build_wasm_webrtc_transport(
    keypair: &libp2p::identity::Keypair,
) -> Result<
    libp2p::core::transport::Boxed<(
        libp2p::identity::PeerId,
        libp2p::core::muxing::StreamMuxerBox,
    )>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    Ok(libp2p::webrtc_websys::Transport::new(libp2p::webrtc_websys::Config::new(keypair)).boxed())
}

#[cfg(target_arch = "wasm32")]
fn build_wasm_webtransport_transport(
    keypair: &libp2p::identity::Keypair,
) -> Result<
    libp2p::core::transport::Boxed<(
        libp2p::identity::PeerId,
        libp2p::core::muxing::StreamMuxerBox,
    )>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    Ok(
        libp2p::webtransport_websys::Transport::new(libp2p::webtransport_websys::Config::new(
            keypair,
        ))
        .boxed(),
    )
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
        if !segments.contains(&"certhash") {
            return None;
        }
        return Some(BrowserTransportFamily::WebRtcDirect);
    }
    if segments.contains(&"webtransport") {
        if !segments.contains(&"quic-v1") || !segments.contains(&"certhash") {
            return None;
        }
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
