#![forbid(unsafe_code)]

mod runtime_helpers;
mod stats;
mod types;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use burn_p2p_core::{
    AggregateEnvelope, ArtifactDescriptor, ArtifactId, AssignmentLease, ChunkDescriptor, ChunkId,
    ContentId, ControlCertificate, DatasetViewId, ExperimentDirectoryEntry, HeadDescriptor, HeadId,
    MergeCertificate, MergeWindowState, MicroShardId, NetworkId, PeerAuthEnvelope, PeerId,
    ReducerAssignment, ReducerLoadReport, ReductionCertificate, TelemetrySummary, UpdateAnnounce,
};
use burn_p2p_experiment::ExperimentControlEnvelope;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use libp2p::{Multiaddr, StreamProtocol};
use libp2p_core::{Transport, transport::MemoryTransport, upgrade};
use libp2p_identity::{Keypair, PeerId as Libp2pPeerId};
use libp2p_plaintext as plaintext;
use libp2p_request_response::{self as request_response, ProtocolSupport};
use libp2p_swarm::{Config as Libp2pSwarmConfig, NetworkBehaviour, Swarm, SwarmEvent, dummy};
use libp2p_yamux as yamux;
use serde::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
use libp2p::{SwarmBuilder, gossipsub, identify};
#[cfg(not(target_arch = "wasm32"))]
use libp2p_mdns as mdns;
#[cfg(not(target_arch = "wasm32"))]
use tokio::{
    runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime},
    time::timeout,
};

use runtime_helpers::{
    materialize_listen_addr, other_control_name, other_name, other_native_control_name,
    plaintext_config,
};
pub use stats::{
    MigrationCoordinator, MigrationPlan, PeerObservation, PeerStore, SwarmError, SwarmStats,
};
pub use types::{
    ExperimentOverlaySet, OverlayChannel, OverlayTopic, ProtocolId, ProtocolSet, RuntimeBoundary,
    RuntimeEnvironment, RuntimeTransportPolicy, SwarmAddress, TransportKind,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserEdgeControlPlanePaths {
    pub directory_path: String,
    pub heads_path: String,
}

impl Default for BrowserEdgeControlPlanePaths {
    fn default() -> Self {
        Self {
            directory_path: "/directory".into(),
            heads_path: "/heads".into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BrowserEdgeControlPlaneClient {
    http: reqwest::Client,
    base_url: String,
    network_id: NetworkId,
    paths: BrowserEdgeControlPlanePaths,
}

impl BrowserEdgeControlPlaneClient {
    pub fn new(base_url: impl Into<String>, network_id: NetworkId) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            network_id,
            paths: BrowserEdgeControlPlanePaths::default(),
        }
    }

    pub fn with_paths(mut self, paths: BrowserEdgeControlPlanePaths) -> Self {
        self.paths = paths;
        self
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        session_id: Option<&ContentId>,
    ) -> Result<T, SwarmError> {
        let request = session_id.map_or_else(
            || self.http.get(format!("{}{}", self.base_url, path)),
            |session_id| {
                self.http
                    .get(format!("{}{}", self.base_url, path))
                    .header("x-session-id", session_id.as_str())
            },
        );
        request
            .send()
            .await
            .map_err(|error| SwarmError::Request(error.to_string()))?
            .error_for_status()
            .map_err(|error| SwarmError::Request(error.to_string()))?
            .json::<T>()
            .await
            .map_err(|error| SwarmError::Request(error.to_string()))
    }

    pub async fn fetch_directory(
        &self,
        session_id: Option<&ContentId>,
    ) -> Result<Vec<ExperimentDirectoryEntry>, SwarmError> {
        self.get_json(&self.paths.directory_path, session_id).await
    }

    pub async fn fetch_heads(&self) -> Result<Vec<HeadDescriptor>, SwarmError> {
        self.get_json::<Vec<HeadDescriptor>>(&self.paths.heads_path, None)
            .await
    }

    pub async fn fetch_snapshot(
        &self,
        session_id: Option<&ContentId>,
    ) -> Result<ControlPlaneSnapshot, SwarmError> {
        let generated_at = Utc::now();
        let entries = self.fetch_directory(session_id).await?;
        let heads = self.fetch_heads().await?;

        let mut snapshot = ControlPlaneSnapshot {
            directory_announcements: vec![ExperimentDirectoryAnnouncement {
                network_id: self.network_id.clone(),
                entries,
                announced_at: generated_at,
            }],
            ..Default::default()
        };

        for head in heads {
            let overlay = OverlayTopic::experiment(
                self.network_id.clone(),
                head.study_id.clone(),
                head.experiment_id.clone(),
                OverlayChannel::Heads,
            )?;
            snapshot.head_announcements.push(HeadAnnouncement {
                overlay,
                provider_peer_id: None,
                head,
                announced_at: generated_at,
            });
        }

        Ok(snapshot)
    }
}

pub struct MemorySwarmShell {
    local_peer_id: Libp2pPeerId,
    swarm: Swarm<dummy::Behaviour>,
}

impl MemorySwarmShell {
    pub fn new() -> Self {
        Self::with_keypair(Keypair::generate_ed25519())
    }

    pub fn with_keypair(keypair: Keypair) -> Self {
        let local_peer_id = keypair.public().to_peer_id();
        let transport = MemoryTransport::default()
            .upgrade(upgrade::Version::V1)
            .authenticate(plaintext::Config::new(&keypair))
            .multiplex(yamux::Config::default())
            .boxed();
        let swarm = Swarm::new(
            transport,
            dummy::Behaviour,
            local_peer_id,
            Libp2pSwarmConfig::without_executor(),
        );

        Self {
            local_peer_id,
            swarm,
        }
    }

    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        &self.local_peer_id
    }

    pub fn listen_on(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        let address: Multiaddr = address
            .as_str()
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(address.as_str().to_owned()))?;
        self.swarm
            .listen_on(address)
            .map(|_| ())
            .map_err(|error| SwarmError::Listen(error.to_string()))
    }

    pub fn dial(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        let address: Multiaddr = address
            .as_str()
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(address.as_str().to_owned()))?;
        self.swarm
            .dial(address)
            .map_err(|error| SwarmError::Dial(error.to_string()))
    }

    pub fn connected_peer_count(&self) -> usize {
        self.swarm.network_info().num_peers()
    }

    pub fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<LiveSwarmEvent> {
        match Pin::new(&mut self.swarm).poll_next(cx) {
            Poll::Ready(Some(event)) => Poll::Ready(LiveSwarmEvent::from(event)),
            Poll::Ready(None) => Poll::Ready(LiveSwarmEvent::Other {
                kind: "stream-closed".into(),
            }),
            Poll::Pending => Poll::Pending,
        }
    }

    pub fn wait_event(&mut self, timeout: Duration) -> Option<LiveSwarmEvent> {
        let deadline = Instant::now() + timeout;
        let waker = futures::task::noop_waker_ref();

        loop {
            let mut cx = Context::from_waker(waker);
            match self.poll_event(&mut cx) {
                Poll::Ready(event) => return Some(event),
                Poll::Pending if Instant::now() >= deadline => return None,
                Poll::Pending => std::thread::sleep(Duration::from_millis(10)),
            }
        }
    }
}

impl Default for MemorySwarmShell {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LiveSwarmEvent {
    NewListenAddr {
        address: SwarmAddress,
    },
    ConnectionEstablished {
        peer_id: String,
    },
    OutgoingConnectionError {
        peer_id: Option<String>,
        message: String,
    },
    IncomingConnectionError {
        message: String,
    },
    Other {
        kind: String,
    },
}

impl From<SwarmEvent<Infallible>> for LiveSwarmEvent {
    fn from(event: SwarmEvent<Infallible>) -> Self {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => Self::NewListenAddr {
                address: SwarmAddress(address.to_string()),
            },
            SwarmEvent::ConnectionEstablished { peer_id, .. } => Self::ConnectionEstablished {
                peer_id: peer_id.to_string(),
            },
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                Self::OutgoingConnectionError {
                    peer_id: peer_id.map(|peer_id| peer_id.to_string()),
                    message: error.to_string(),
                }
            }
            SwarmEvent::IncomingConnectionError { error, .. } => Self::IncomingConnectionError {
                message: error.to_string(),
            },
            SwarmEvent::Behaviour(void) => match void {},
            other => Self::Other {
                kind: other_name(&other).to_owned(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ControlAnnouncement {
    pub overlay: OverlayTopic,
    pub certificate: ControlCertificate<ExperimentControlEnvelope>,
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HeadAnnouncement {
    pub overlay: OverlayTopic,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_peer_id: Option<PeerId>,
    pub head: HeadDescriptor,
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseAnnouncement {
    pub overlay: OverlayTopic,
    pub lease: AssignmentLease,
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TelemetryAnnouncement {
    pub overlay: OverlayTopic,
    pub telemetry: TelemetrySummary,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warn,
    Error,
    Critical,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertNotice {
    pub overlay: OverlayTopic,
    pub peer_id: Option<PeerId>,
    pub severity: AlertSeverity,
    pub code: String,
    pub message: String,
    pub emitted_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderPointer {
    pub peer_id: PeerId,
    pub addresses: Vec<SwarmAddress>,
    pub advertised_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactProviderRecord {
    pub artifact_id: ArtifactId,
    pub head_id: Option<HeadId>,
    pub providers: Vec<ProviderPointer>,
    pub advertised_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MicroShardProviderRecord {
    pub dataset_view_id: DatasetViewId,
    pub microshard_id: MicroShardId,
    pub providers: Vec<ProviderPointer>,
    pub advertised_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactSyncRequest {
    pub requester: PeerId,
    pub artifact_id: ArtifactId,
    pub wanted_chunks: BTreeSet<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactSyncResponse {
    pub provider: PeerId,
    pub descriptor: ArtifactDescriptor,
    pub available_chunks: BTreeSet<ChunkId>,
    pub direct_streams_preferred: bool,
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkFetchRequest {
    pub requester: PeerId,
    pub artifact_id: ArtifactId,
    pub chunk_id: ChunkId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkFetchResponse {
    pub provider: PeerId,
    pub artifact_id: ArtifactId,
    pub chunk: ChunkDescriptor,
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MicroShardFetchRequest {
    pub requester: PeerId,
    pub dataset_view_id: DatasetViewId,
    pub microshard_id: MicroShardId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MicroShardFetchResponse {
    pub provider: PeerId,
    pub dataset_view_id: DatasetViewId,
    pub microshard_id: MicroShardId,
    pub estimated_bytes: u64,
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MergeAnnouncement {
    pub overlay: OverlayTopic,
    pub certificate: MergeCertificate,
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeWindowAnnouncement {
    pub overlay: OverlayTopic,
    pub merge_window: MergeWindowState,
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReducerAssignmentAnnouncement {
    pub overlay: OverlayTopic,
    pub assignment: ReducerAssignment,
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateEnvelopeAnnouncement {
    pub overlay: OverlayTopic,
    pub update: UpdateAnnounce,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregateAnnouncement {
    pub overlay: OverlayTopic,
    pub aggregate: AggregateEnvelope,
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReductionCertificateAnnouncement {
    pub overlay: OverlayTopic,
    pub certificate: ReductionCertificate,
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReducerLoadAnnouncement {
    pub overlay: OverlayTopic,
    pub report: ReducerLoadReport,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PeerAuthAnnouncement {
    pub peer_id: PeerId,
    pub envelope: PeerAuthEnvelope,
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperimentDirectoryAnnouncement {
    pub network_id: NetworkId,
    pub entries: Vec<ExperimentDirectoryEntry>,
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ControlPlaneSnapshot {
    pub control_announcements: Vec<ControlAnnouncement>,
    pub head_announcements: Vec<HeadAnnouncement>,
    pub lease_announcements: Vec<LeaseAnnouncement>,
    pub merge_announcements: Vec<MergeAnnouncement>,
    pub merge_window_announcements: Vec<MergeWindowAnnouncement>,
    pub reducer_assignment_announcements: Vec<ReducerAssignmentAnnouncement>,
    pub update_announcements: Vec<UpdateEnvelopeAnnouncement>,
    pub aggregate_announcements: Vec<AggregateAnnouncement>,
    pub reduction_certificate_announcements: Vec<ReductionCertificateAnnouncement>,
    pub reducer_load_announcements: Vec<ReducerLoadAnnouncement>,
    pub auth_announcements: Vec<PeerAuthAnnouncement>,
    pub directory_announcements: Vec<ExperimentDirectoryAnnouncement>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactChunkPayload {
    pub artifact_id: ArtifactId,
    pub chunk: ChunkDescriptor,
    pub bytes: Vec<u8>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlPlaneRequest {
    Snapshot,
    ArtifactManifest {
        artifact_id: ArtifactId,
    },
    ArtifactChunk {
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ControlPlaneResponse {
    Snapshot(ControlPlaneSnapshot),
    ArtifactManifest(Option<ArtifactDescriptor>),
    ArtifactChunk(Option<ArtifactChunkPayload>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PubsubPayload {
    Control(ControlAnnouncement),
    Head(HeadAnnouncement),
    Lease(LeaseAnnouncement),
    Merge(MergeAnnouncement),
    MergeWindow(MergeWindowAnnouncement),
    ReducerAssignment(ReducerAssignmentAnnouncement),
    Update(UpdateEnvelopeAnnouncement),
    Aggregate(AggregateAnnouncement),
    ReductionCertificate(ReductionCertificateAnnouncement),
    ReducerLoad(ReducerLoadAnnouncement),
    Auth(PeerAuthAnnouncement),
    Directory(ExperimentDirectoryAnnouncement),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PubsubEnvelope {
    pub topic_path: String,
    pub payload: PubsubPayload,
    pub published_at: DateTime<Utc>,
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "NativeControlPlaneBehaviourEvent")]
#[cfg(not(target_arch = "wasm32"))]
struct NativeControlPlaneBehaviour {
    request_response: request_response::json::Behaviour<ControlPlaneRequest, ControlPlaneResponse>,
    gossipsub: gossipsub::Behaviour,
    identify: identify::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

#[cfg(not(target_arch = "wasm32"))]
enum NativeControlPlaneBehaviourEvent {
    RequestResponse(Box<request_response::Event<ControlPlaneRequest, ControlPlaneResponse>>),
    Gossipsub(Box<gossipsub::Event>),
    Identify(Box<identify::Event>),
    Mdns(mdns::Event),
}

#[cfg(not(target_arch = "wasm32"))]
impl From<request_response::Event<ControlPlaneRequest, ControlPlaneResponse>>
    for NativeControlPlaneBehaviourEvent
{
    fn from(value: request_response::Event<ControlPlaneRequest, ControlPlaneResponse>) -> Self {
        Self::RequestResponse(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<gossipsub::Event> for NativeControlPlaneBehaviourEvent {
    fn from(value: gossipsub::Event) -> Self {
        Self::Gossipsub(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<identify::Event> for NativeControlPlaneBehaviourEvent {
    fn from(value: identify::Event) -> Self {
        Self::Identify(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<mdns::Event> for NativeControlPlaneBehaviourEvent {
    fn from(value: mdns::Event) -> Self {
        Self::Mdns(value)
    }
}

fn push_unique<T: PartialEq>(values: &mut Vec<T>, value: T) {
    if !values.contains(&value) {
        values.push(value);
    }
}

fn apply_pubsub_payload(snapshot: &mut ControlPlaneSnapshot, payload: PubsubPayload) {
    match payload {
        PubsubPayload::Control(announcement) => {
            push_unique(&mut snapshot.control_announcements, announcement);
        }
        PubsubPayload::Head(announcement) => {
            push_unique(&mut snapshot.head_announcements, announcement);
        }
        PubsubPayload::Lease(announcement) => {
            push_unique(&mut snapshot.lease_announcements, announcement);
        }
        PubsubPayload::Merge(announcement) => {
            push_unique(&mut snapshot.merge_announcements, announcement);
        }
        PubsubPayload::MergeWindow(announcement) => {
            push_unique(&mut snapshot.merge_window_announcements, announcement);
        }
        PubsubPayload::ReducerAssignment(announcement) => {
            push_unique(&mut snapshot.reducer_assignment_announcements, announcement);
        }
        PubsubPayload::Update(announcement) => {
            push_unique(&mut snapshot.update_announcements, announcement);
        }
        PubsubPayload::Aggregate(announcement) => {
            push_unique(&mut snapshot.aggregate_announcements, announcement);
        }
        PubsubPayload::ReductionCertificate(announcement) => {
            push_unique(
                &mut snapshot.reduction_certificate_announcements,
                announcement,
            );
        }
        PubsubPayload::ReducerLoad(announcement) => {
            push_unique(&mut snapshot.reducer_load_announcements, announcement);
        }
        PubsubPayload::Auth(announcement) => {
            push_unique(&mut snapshot.auth_announcements, announcement);
        }
        PubsubPayload::Directory(announcement) => {
            push_unique(&mut snapshot.directory_announcements, announcement);
        }
    }
}

fn pubsub_payload_kind(payload: &PubsubPayload) -> &'static str {
    match payload {
        PubsubPayload::Control(_) => "control",
        PubsubPayload::Head(_) => "head",
        PubsubPayload::Lease(_) => "lease",
        PubsubPayload::Merge(_) => "merge",
        PubsubPayload::MergeWindow(_) => "merge-window",
        PubsubPayload::ReducerAssignment(_) => "reducer-assignment",
        PubsubPayload::Update(_) => "update",
        PubsubPayload::Aggregate(_) => "aggregate",
        PubsubPayload::ReductionCertificate(_) => "reduction-certificate",
        PubsubPayload::ReducerLoad(_) => "reducer-load",
        PubsubPayload::Auth(_) => "auth",
        PubsubPayload::Directory(_) => "directory",
    }
}

pub struct MemoryControlPlaneShell {
    local_peer_id: Libp2pPeerId,
    swarm: Swarm<request_response::json::Behaviour<ControlPlaneRequest, ControlPlaneResponse>>,
    snapshot: ControlPlaneSnapshot,
    artifacts: BTreeMap<ArtifactId, ArtifactDescriptor>,
    chunks: BTreeMap<(ArtifactId, ChunkId), ArtifactChunkPayload>,
    subscribed_topics: BTreeSet<String>,
}

impl MemoryControlPlaneShell {
    pub fn new(control_protocol: ProtocolId) -> Self {
        Self::with_keypair(control_protocol, Keypair::generate_ed25519())
    }

    pub fn with_keypair(control_protocol: ProtocolId, keypair: Keypair) -> Self {
        let local_peer_id = keypair.public().to_peer_id();
        let transport = MemoryTransport::default()
            .upgrade(upgrade::Version::V1)
            .authenticate(plaintext::Config::new(&keypair))
            .multiplex(yamux::Config::default())
            .boxed();
        let protocol =
            StreamProtocol::try_from_owned(control_protocol.as_str().to_owned()).expect("valid");
        let behaviour = request_response::json::Behaviour::new(
            [(protocol, ProtocolSupport::Full)],
            request_response::Config::default(),
        );
        let swarm = Swarm::new(
            transport,
            behaviour,
            local_peer_id,
            Libp2pSwarmConfig::without_executor(),
        );

        Self {
            local_peer_id,
            swarm,
            snapshot: ControlPlaneSnapshot::default(),
            artifacts: BTreeMap::new(),
            chunks: BTreeMap::new(),
            subscribed_topics: BTreeSet::new(),
        }
    }

    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        &self.local_peer_id
    }

    pub fn listen_on(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        let address: Multiaddr = address
            .as_str()
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(address.as_str().to_owned()))?;
        self.swarm
            .listen_on(address)
            .map(|_| ())
            .map_err(|error| SwarmError::Listen(error.to_string()))
    }

    pub fn dial(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        let address: Multiaddr = address
            .as_str()
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(address.as_str().to_owned()))?;
        self.swarm
            .dial(address)
            .map_err(|error| SwarmError::Dial(error.to_string()))
    }

    pub fn connected_peer_count(&self) -> usize {
        self.swarm.network_info().num_peers()
    }

    pub fn publish_control(&mut self, announcement: ControlAnnouncement) {
        push_unique(&mut self.snapshot.control_announcements, announcement);
    }

    pub fn publish_head(&mut self, announcement: HeadAnnouncement) {
        push_unique(&mut self.snapshot.head_announcements, announcement);
    }

    pub fn publish_lease(&mut self, announcement: LeaseAnnouncement) {
        push_unique(&mut self.snapshot.lease_announcements, announcement);
    }

    pub fn publish_merge(&mut self, announcement: MergeAnnouncement) {
        push_unique(&mut self.snapshot.merge_announcements, announcement);
    }

    pub fn publish_merge_window(&mut self, announcement: MergeWindowAnnouncement) {
        push_unique(&mut self.snapshot.merge_window_announcements, announcement);
    }

    pub fn publish_reducer_assignment(&mut self, announcement: ReducerAssignmentAnnouncement) {
        push_unique(
            &mut self.snapshot.reducer_assignment_announcements,
            announcement,
        );
    }

    pub fn publish_update(&mut self, announcement: UpdateEnvelopeAnnouncement) {
        push_unique(&mut self.snapshot.update_announcements, announcement);
    }

    pub fn publish_aggregate(&mut self, announcement: AggregateAnnouncement) {
        push_unique(&mut self.snapshot.aggregate_announcements, announcement);
    }

    pub fn publish_reduction_certificate(
        &mut self,
        announcement: ReductionCertificateAnnouncement,
    ) {
        push_unique(
            &mut self.snapshot.reduction_certificate_announcements,
            announcement,
        );
    }

    pub fn publish_reducer_load(&mut self, announcement: ReducerLoadAnnouncement) {
        push_unique(&mut self.snapshot.reducer_load_announcements, announcement);
    }

    pub fn publish_auth(&mut self, announcement: PeerAuthAnnouncement) {
        push_unique(&mut self.snapshot.auth_announcements, announcement);
    }

    pub fn publish_directory(&mut self, announcement: ExperimentDirectoryAnnouncement) {
        push_unique(&mut self.snapshot.directory_announcements, announcement);
    }

    pub fn snapshot(&self) -> &ControlPlaneSnapshot {
        &self.snapshot
    }

    pub fn subscribe_topic(&mut self, topic: OverlayTopic) -> Result<(), SwarmError> {
        self.subscribed_topics.insert(topic.path);
        Ok(())
    }

    pub fn publish_pubsub(
        &mut self,
        _topic: OverlayTopic,
        _payload: PubsubPayload,
    ) -> Result<(), SwarmError> {
        Ok(())
    }

    pub fn publish_artifact(
        &mut self,
        descriptor: ArtifactDescriptor,
        chunks: Vec<ArtifactChunkPayload>,
    ) {
        let artifact_id = descriptor.artifact_id.clone();
        self.artifacts.insert(artifact_id.clone(), descriptor);
        for chunk in chunks {
            self.chunks
                .insert((artifact_id.clone(), chunk.chunk.chunk_id.clone()), chunk);
        }
    }

    pub fn request_snapshot(&mut self, peer_id: &str) -> Result<(), SwarmError> {
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        self.swarm
            .behaviour_mut()
            .send_request(&peer_id, ControlPlaneRequest::Snapshot);
        Ok(())
    }

    pub fn request_artifact_manifest(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
    ) -> Result<(), SwarmError> {
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        self.swarm.behaviour_mut().send_request(
            &peer_id,
            ControlPlaneRequest::ArtifactManifest { artifact_id },
        );
        Ok(())
    }

    pub fn request_artifact_chunk(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
    ) -> Result<(), SwarmError> {
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        self.swarm.behaviour_mut().send_request(
            &peer_id,
            ControlPlaneRequest::ArtifactChunk {
                artifact_id,
                chunk_id,
            },
        );
        Ok(())
    }

    pub fn fetch_snapshot(
        &mut self,
        peer_id: &str,
        timeout: Duration,
    ) -> Result<ControlPlaneSnapshot, SwarmError> {
        self.request_snapshot(peer_id)?;

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.wait_event(Duration::from_millis(50)) {
                Some(LiveControlPlaneEvent::SnapshotReceived { snapshot, .. }) => {
                    return Ok(snapshot);
                }
                Some(LiveControlPlaneEvent::RequestFailure { message, .. }) => {
                    return Err(SwarmError::Request(message));
                }
                Some(_) | None => {}
            }
        }

        Err(SwarmError::TimedOut("snapshot"))
    }

    pub fn fetch_artifact_manifest(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        timeout: Duration,
    ) -> Result<Option<ArtifactDescriptor>, SwarmError> {
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        self.swarm.behaviour_mut().send_request(
            &peer_id,
            ControlPlaneRequest::ArtifactManifest { artifact_id },
        );

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.wait_event(Duration::from_millis(50)) {
                Some(LiveControlPlaneEvent::ArtifactManifestReceived { descriptor, .. }) => {
                    return Ok(descriptor);
                }
                Some(LiveControlPlaneEvent::RequestFailure { message, .. }) => {
                    return Err(SwarmError::Request(message));
                }
                Some(_) | None => {}
            }
        }

        Err(SwarmError::TimedOut("artifact-manifest"))
    }

    pub fn fetch_artifact_chunk(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
        timeout: Duration,
    ) -> Result<Option<ArtifactChunkPayload>, SwarmError> {
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        self.swarm.behaviour_mut().send_request(
            &peer_id,
            ControlPlaneRequest::ArtifactChunk {
                artifact_id,
                chunk_id,
            },
        );

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.wait_event(Duration::from_millis(50)) {
                Some(LiveControlPlaneEvent::ArtifactChunkReceived { payload, .. }) => {
                    return Ok(payload);
                }
                Some(LiveControlPlaneEvent::RequestFailure { message, .. }) => {
                    return Err(SwarmError::Request(message));
                }
                Some(_) | None => {}
            }
        }

        Err(SwarmError::TimedOut("artifact-chunk"))
    }

    pub fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<LiveControlPlaneEvent> {
        match Pin::new(&mut self.swarm).poll_next(cx) {
            Poll::Ready(Some(SwarmEvent::Behaviour(event))) => match event {
                request_response::Event::Message { peer, message, .. } => match message {
                    request_response::Message::Request {
                        request, channel, ..
                    } => match request {
                        ControlPlaneRequest::Snapshot => {
                            let response = ControlPlaneResponse::Snapshot(self.snapshot.clone());
                            match self.swarm.behaviour_mut().send_response(channel, response) {
                                Ok(()) => Poll::Ready(LiveControlPlaneEvent::SnapshotRequested {
                                    peer_id: peer.to_string(),
                                }),
                                Err(_) => Poll::Ready(LiveControlPlaneEvent::ResponseSendFailure {
                                    peer_id: peer.to_string(),
                                    message: "snapshot response channel closed".into(),
                                }),
                            }
                        }
                        ControlPlaneRequest::ArtifactManifest { artifact_id } => {
                            let response = ControlPlaneResponse::ArtifactManifest(
                                self.artifacts.get(&artifact_id).cloned(),
                            );
                            match self.swarm.behaviour_mut().send_response(channel, response) {
                                Ok(()) => {
                                    Poll::Ready(LiveControlPlaneEvent::ArtifactManifestRequested {
                                        peer_id: peer.to_string(),
                                        artifact_id,
                                    })
                                }
                                Err(_) => Poll::Ready(LiveControlPlaneEvent::ResponseSendFailure {
                                    peer_id: peer.to_string(),
                                    message: "artifact manifest response channel closed".into(),
                                }),
                            }
                        }
                        ControlPlaneRequest::ArtifactChunk {
                            artifact_id,
                            chunk_id,
                        } => {
                            let response = ControlPlaneResponse::ArtifactChunk(
                                self.chunks
                                    .get(&(artifact_id.clone(), chunk_id.clone()))
                                    .cloned(),
                            );
                            match self.swarm.behaviour_mut().send_response(channel, response) {
                                Ok(()) => {
                                    Poll::Ready(LiveControlPlaneEvent::ArtifactChunkRequested {
                                        peer_id: peer.to_string(),
                                        artifact_id,
                                        chunk_id,
                                    })
                                }
                                Err(_) => Poll::Ready(LiveControlPlaneEvent::ResponseSendFailure {
                                    peer_id: peer.to_string(),
                                    message: "artifact chunk response channel closed".into(),
                                }),
                            }
                        }
                    },
                    request_response::Message::Response { response, .. } => match response {
                        ControlPlaneResponse::Snapshot(snapshot) => {
                            Poll::Ready(LiveControlPlaneEvent::SnapshotReceived {
                                peer_id: peer.to_string(),
                                snapshot,
                            })
                        }
                        ControlPlaneResponse::ArtifactManifest(descriptor) => {
                            Poll::Ready(LiveControlPlaneEvent::ArtifactManifestReceived {
                                peer_id: peer.to_string(),
                                descriptor,
                            })
                        }
                        ControlPlaneResponse::ArtifactChunk(payload) => {
                            Poll::Ready(LiveControlPlaneEvent::ArtifactChunkReceived {
                                peer_id: peer.to_string(),
                                payload,
                            })
                        }
                    },
                },
                request_response::Event::OutboundFailure { peer, error, .. } => {
                    Poll::Ready(LiveControlPlaneEvent::RequestFailure {
                        peer_id: peer.to_string(),
                        message: error.to_string(),
                    })
                }
                request_response::Event::InboundFailure { peer, error, .. } => {
                    Poll::Ready(LiveControlPlaneEvent::InboundFailure {
                        peer_id: peer.to_string(),
                        message: error.to_string(),
                    })
                }
                request_response::Event::ResponseSent { peer, .. } => {
                    Poll::Ready(LiveControlPlaneEvent::SnapshotResponseSent {
                        peer_id: peer.to_string(),
                    })
                }
            },
            Poll::Ready(Some(event)) => Poll::Ready(LiveControlPlaneEvent::from(event)),
            Poll::Ready(None) => Poll::Ready(LiveControlPlaneEvent::Other {
                kind: "stream-closed".into(),
            }),
            Poll::Pending => Poll::Pending,
        }
    }

    pub fn wait_event(&mut self, timeout: Duration) -> Option<LiveControlPlaneEvent> {
        let deadline = Instant::now() + timeout;
        let waker = futures::task::noop_waker_ref();

        loop {
            let mut cx = Context::from_waker(waker);
            match self.poll_event(&mut cx) {
                Poll::Ready(event) => return Some(event),
                Poll::Pending if Instant::now() >= deadline => return None,
                Poll::Pending => std::thread::sleep(Duration::from_millis(10)),
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LiveControlPlaneEvent {
    NewListenAddr {
        address: SwarmAddress,
    },
    ConnectionEstablished {
        peer_id: String,
    },
    SnapshotRequested {
        peer_id: String,
    },
    SnapshotReceived {
        peer_id: String,
        snapshot: ControlPlaneSnapshot,
    },
    ArtifactManifestRequested {
        peer_id: String,
        artifact_id: ArtifactId,
    },
    ArtifactManifestReceived {
        peer_id: String,
        descriptor: Option<ArtifactDescriptor>,
    },
    ArtifactChunkRequested {
        peer_id: String,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
    },
    ArtifactChunkReceived {
        peer_id: String,
        payload: Option<ArtifactChunkPayload>,
    },
    SnapshotResponseSent {
        peer_id: String,
    },
    TopicSubscribed {
        topic: String,
    },
    PubsubMessage {
        peer_id: String,
        topic: String,
        kind: String,
    },
    PeersDiscovered {
        peers: Vec<(String, SwarmAddress)>,
    },
    PeersExpired {
        peers: Vec<(String, SwarmAddress)>,
    },
    PeerIdentified {
        peer_id: String,
        listen_addresses: Vec<SwarmAddress>,
        protocols: Vec<String>,
    },
    RequestFailure {
        peer_id: String,
        message: String,
    },
    InboundFailure {
        peer_id: String,
        message: String,
    },
    ResponseSendFailure {
        peer_id: String,
        message: String,
    },
    OutgoingConnectionError {
        peer_id: Option<String>,
        message: String,
    },
    IncomingConnectionError {
        message: String,
    },
    Other {
        kind: String,
    },
}

impl From<SwarmEvent<request_response::Event<ControlPlaneRequest, ControlPlaneResponse>>>
    for LiveControlPlaneEvent
{
    fn from(
        event: SwarmEvent<request_response::Event<ControlPlaneRequest, ControlPlaneResponse>>,
    ) -> Self {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => Self::NewListenAddr {
                address: SwarmAddress(address.to_string()),
            },
            SwarmEvent::ConnectionEstablished { peer_id, .. } => Self::ConnectionEstablished {
                peer_id: peer_id.to_string(),
            },
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                Self::OutgoingConnectionError {
                    peer_id: peer_id.map(|peer_id| peer_id.to_string()),
                    message: error.to_string(),
                }
            }
            SwarmEvent::IncomingConnectionError { error, .. } => Self::IncomingConnectionError {
                message: error.to_string(),
            },
            other => Self::Other {
                kind: other_control_name(&other).to_owned(),
            },
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub struct NativeControlPlaneShell {
    runtime: TokioRuntime,
    local_peer_id: Libp2pPeerId,
    swarm: Swarm<NativeControlPlaneBehaviour>,
    snapshot: ControlPlaneSnapshot,
    artifacts: BTreeMap<ArtifactId, ArtifactDescriptor>,
    chunks: BTreeMap<(ArtifactId, ChunkId), ArtifactChunkPayload>,
    subscribed_topics: BTreeSet<String>,
    pending_events: VecDeque<LiveControlPlaneEvent>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeControlPlaneShell {
    pub fn new(control_protocol: ProtocolId) -> Result<Self, SwarmError> {
        Self::with_keypair(control_protocol, Keypair::generate_ed25519())
    }

    pub fn with_keypair(
        control_protocol: ProtocolId,
        keypair: Keypair,
    ) -> Result<Self, SwarmError> {
        let runtime = TokioRuntimeBuilder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|error| SwarmError::Runtime(error.to_string()))?;
        let local_peer_id = keypair.public().to_peer_id();
        let protocol =
            StreamProtocol::try_from_owned(control_protocol.as_str().to_owned()).expect("valid");
        let gossip_config = gossipsub::ConfigBuilder::default()
            .validation_mode(gossipsub::ValidationMode::Permissive)
            .build()
            .map_err(|error| SwarmError::Runtime(error.to_string()))?;
        let identify_config = identify::Config::new(
            format!("{}/identify/1.0.0", control_protocol.as_str()),
            keypair.public(),
        );
        #[cfg(not(target_arch = "wasm32"))]
        let mdns_behaviour = {
            let _guard = runtime.enter();
            mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)
                .map_err(|error| SwarmError::Runtime(error.to_string()))?
        };
        let swarm = {
            let _guard = runtime.enter();
            SwarmBuilder::with_existing_identity(keypair)
                .with_tokio()
                .with_tcp(
                    libp2p::tcp::Config::default(),
                    plaintext_config,
                    yamux::Config::default,
                )
                .map_err(|error| SwarmError::Runtime(error.to_string()))?
                .with_quic()
                .with_behaviour(move |key| NativeControlPlaneBehaviour {
                    request_response: request_response::json::Behaviour::new(
                        [(protocol, ProtocolSupport::Full)],
                        request_response::Config::default(),
                    ),
                    gossipsub: gossipsub::Behaviour::new(
                        gossipsub::MessageAuthenticity::Signed(key.clone()),
                        gossip_config.clone(),
                    )
                    .expect("gossipsub behaviour should be valid"),
                    identify: identify::Behaviour::new(identify_config.clone()),
                    #[cfg(not(target_arch = "wasm32"))]
                    mdns: mdns_behaviour,
                })
                .map_err(|error| SwarmError::Runtime(error.to_string()))?
                .build()
        };

        Ok(Self {
            runtime,
            local_peer_id,
            swarm,
            snapshot: ControlPlaneSnapshot::default(),
            artifacts: BTreeMap::new(),
            chunks: BTreeMap::new(),
            subscribed_topics: BTreeSet::new(),
            pending_events: VecDeque::new(),
        })
    }

    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        &self.local_peer_id
    }

    pub fn listen_on(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        let requested: Multiaddr = address
            .as_str()
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(address.as_str().to_owned()))?;
        let concrete = materialize_listen_addr(&requested)
            .map_err(|error| SwarmError::Listen(error.to_string()))?;
        {
            let _guard = self.runtime.enter();
            self.swarm
                .listen_on(concrete.clone())
                .map_err(|error| SwarmError::Listen(error.to_string()))?;
        }

        let ready_event = self.runtime.block_on(async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < deadline {
                if let Ok(SwarmEvent::NewListenAddr { address, .. }) =
                    timeout(Duration::from_millis(100), self.swarm.select_next_some()).await
                {
                    return Some(LiveControlPlaneEvent::NewListenAddr {
                        address: SwarmAddress(address.to_string()),
                    });
                }
            }
            None
        });

        match ready_event {
            Some(event) => self.pending_events.push_back(event),
            None => return Err(SwarmError::TimedOut("listen-ready")),
        }
        Ok(())
    }

    pub fn dial(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        let address: Multiaddr = address
            .as_str()
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(address.as_str().to_owned()))?;
        let _guard = self.runtime.enter();
        self.swarm
            .dial(address)
            .map_err(|error| SwarmError::Dial(error.to_string()))
    }

    pub fn connected_peer_count(&self) -> usize {
        self.swarm.network_info().num_peers()
    }

    pub fn publish_control(&mut self, announcement: ControlAnnouncement) {
        push_unique(&mut self.snapshot.control_announcements, announcement);
    }

    pub fn publish_head(&mut self, announcement: HeadAnnouncement) {
        push_unique(&mut self.snapshot.head_announcements, announcement);
    }

    pub fn publish_lease(&mut self, announcement: LeaseAnnouncement) {
        push_unique(&mut self.snapshot.lease_announcements, announcement);
    }

    pub fn publish_merge(&mut self, announcement: MergeAnnouncement) {
        push_unique(&mut self.snapshot.merge_announcements, announcement);
    }

    pub fn publish_merge_window(&mut self, announcement: MergeWindowAnnouncement) {
        push_unique(&mut self.snapshot.merge_window_announcements, announcement);
    }

    pub fn publish_reducer_assignment(&mut self, announcement: ReducerAssignmentAnnouncement) {
        push_unique(
            &mut self.snapshot.reducer_assignment_announcements,
            announcement,
        );
    }

    pub fn publish_update(&mut self, announcement: UpdateEnvelopeAnnouncement) {
        push_unique(&mut self.snapshot.update_announcements, announcement);
    }

    pub fn publish_aggregate(&mut self, announcement: AggregateAnnouncement) {
        push_unique(&mut self.snapshot.aggregate_announcements, announcement);
    }

    pub fn publish_reduction_certificate(
        &mut self,
        announcement: ReductionCertificateAnnouncement,
    ) {
        push_unique(
            &mut self.snapshot.reduction_certificate_announcements,
            announcement,
        );
    }

    pub fn publish_reducer_load(&mut self, announcement: ReducerLoadAnnouncement) {
        push_unique(&mut self.snapshot.reducer_load_announcements, announcement);
    }

    pub fn publish_auth(&mut self, announcement: PeerAuthAnnouncement) {
        push_unique(&mut self.snapshot.auth_announcements, announcement);
    }

    pub fn publish_directory(&mut self, announcement: ExperimentDirectoryAnnouncement) {
        push_unique(&mut self.snapshot.directory_announcements, announcement);
    }

    pub fn snapshot(&self) -> &ControlPlaneSnapshot {
        &self.snapshot
    }

    pub fn subscribe_topic(&mut self, topic: OverlayTopic) -> Result<(), SwarmError> {
        if self.subscribed_topics.insert(topic.path.clone()) {
            let topic = gossipsub::IdentTopic::new(topic.path);
            self.swarm
                .behaviour_mut()
                .gossipsub
                .subscribe(&topic)
                .map_err(|error| SwarmError::Pubsub(error.to_string()))?;
        }
        Ok(())
    }

    pub fn publish_pubsub(
        &mut self,
        topic: OverlayTopic,
        payload: PubsubPayload,
    ) -> Result<(), SwarmError> {
        self.subscribe_topic(topic.clone())?;
        let envelope = PubsubEnvelope {
            topic_path: topic.path.clone(),
            payload,
            published_at: Utc::now(),
        };
        self.swarm
            .behaviour_mut()
            .gossipsub
            .publish(
                gossipsub::IdentTopic::new(topic.path),
                serde_json::to_vec(&envelope)
                    .map_err(|error| SwarmError::Pubsub(error.to_string()))?,
            )
            .map_err(|error| SwarmError::Pubsub(error.to_string()))?;
        Ok(())
    }

    pub fn publish_artifact(
        &mut self,
        descriptor: ArtifactDescriptor,
        chunks: Vec<ArtifactChunkPayload>,
    ) {
        let artifact_id = descriptor.artifact_id.clone();
        self.artifacts.insert(artifact_id.clone(), descriptor);
        for chunk in chunks {
            self.chunks
                .insert((artifact_id.clone(), chunk.chunk.chunk_id.clone()), chunk);
        }
    }

    pub fn request_snapshot(&mut self, peer_id: &str) -> Result<(), SwarmError> {
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        self.swarm
            .behaviour_mut()
            .request_response
            .send_request(&peer_id, ControlPlaneRequest::Snapshot);
        Ok(())
    }

    pub fn request_artifact_manifest(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
    ) -> Result<(), SwarmError> {
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        self.swarm.behaviour_mut().request_response.send_request(
            &peer_id,
            ControlPlaneRequest::ArtifactManifest { artifact_id },
        );
        Ok(())
    }

    pub fn request_artifact_chunk(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
    ) -> Result<(), SwarmError> {
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        self.swarm.behaviour_mut().request_response.send_request(
            &peer_id,
            ControlPlaneRequest::ArtifactChunk {
                artifact_id,
                chunk_id,
            },
        );
        Ok(())
    }

    pub fn fetch_snapshot(
        &mut self,
        peer_id: &str,
        timeout: Duration,
    ) -> Result<ControlPlaneSnapshot, SwarmError> {
        self.request_snapshot(peer_id)?;

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.wait_event(Duration::from_millis(50)) {
                Some(LiveControlPlaneEvent::SnapshotReceived { snapshot, .. }) => {
                    return Ok(snapshot);
                }
                Some(LiveControlPlaneEvent::RequestFailure { message, .. }) => {
                    return Err(SwarmError::Request(message));
                }
                Some(_) | None => {}
            }
        }

        Err(SwarmError::TimedOut("snapshot"))
    }

    pub fn fetch_artifact_manifest(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        timeout: Duration,
    ) -> Result<Option<ArtifactDescriptor>, SwarmError> {
        self.request_artifact_manifest(peer_id, artifact_id)?;

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.wait_event(Duration::from_millis(50)) {
                Some(LiveControlPlaneEvent::ArtifactManifestReceived { descriptor, .. }) => {
                    return Ok(descriptor);
                }
                Some(LiveControlPlaneEvent::RequestFailure { message, .. }) => {
                    return Err(SwarmError::Request(message));
                }
                Some(_) | None => {}
            }
        }

        Err(SwarmError::TimedOut("artifact-manifest"))
    }

    pub fn fetch_artifact_chunk(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
        timeout: Duration,
    ) -> Result<Option<ArtifactChunkPayload>, SwarmError> {
        self.request_artifact_chunk(peer_id, artifact_id, chunk_id)?;

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.wait_event(Duration::from_millis(50)) {
                Some(LiveControlPlaneEvent::ArtifactChunkReceived { payload, .. }) => {
                    return Ok(payload);
                }
                Some(LiveControlPlaneEvent::RequestFailure { message, .. }) => {
                    return Err(SwarmError::Request(message));
                }
                Some(_) | None => {}
            }
        }

        Err(SwarmError::TimedOut("artifact-chunk"))
    }

    pub fn wait_event(&mut self, duration: Duration) -> Option<LiveControlPlaneEvent> {
        if let Some(event) = self.pending_events.pop_front() {
            return Some(event);
        }

        self.runtime.block_on(async {
            timeout(duration, self.swarm.select_next_some())
                .await
                .ok()
                .map(|event| match event {
                    SwarmEvent::Behaviour(event) => match event {
                        NativeControlPlaneBehaviourEvent::RequestResponse(event) => match *event {
                            request_response::Event::Message { peer, message, .. } => match message
                            {
                                request_response::Message::Request {
                                    request, channel, ..
                                } => match request {
                                    ControlPlaneRequest::Snapshot => {
                                        let response =
                                            ControlPlaneResponse::Snapshot(self.snapshot.clone());
                                        match self
                                            .swarm
                                            .behaviour_mut()
                                            .request_response
                                            .send_response(channel, response)
                                        {
                                            Ok(()) => LiveControlPlaneEvent::SnapshotRequested {
                                                peer_id: peer.to_string(),
                                            },
                                            Err(_) => LiveControlPlaneEvent::ResponseSendFailure {
                                                peer_id: peer.to_string(),
                                                message: "snapshot response channel closed".into(),
                                            },
                                        }
                                    }
                                    ControlPlaneRequest::ArtifactManifest { artifact_id } => {
                                        let response = ControlPlaneResponse::ArtifactManifest(
                                            self.artifacts.get(&artifact_id).cloned(),
                                        );
                                        match self
                                            .swarm
                                            .behaviour_mut()
                                            .request_response
                                            .send_response(channel, response)
                                        {
                                            Ok(()) => {
                                                LiveControlPlaneEvent::ArtifactManifestRequested {
                                                    peer_id: peer.to_string(),
                                                    artifact_id,
                                                }
                                            }
                                            Err(_) => LiveControlPlaneEvent::ResponseSendFailure {
                                                peer_id: peer.to_string(),
                                                message:
                                                    "artifact manifest response channel closed"
                                                        .into(),
                                            },
                                        }
                                    }
                                    ControlPlaneRequest::ArtifactChunk {
                                        artifact_id,
                                        chunk_id,
                                    } => {
                                        let response = ControlPlaneResponse::ArtifactChunk(
                                            self.chunks
                                                .get(&(artifact_id.clone(), chunk_id.clone()))
                                                .cloned(),
                                        );
                                        match self
                                            .swarm
                                            .behaviour_mut()
                                            .request_response
                                            .send_response(channel, response)
                                        {
                                            Ok(()) => {
                                                LiveControlPlaneEvent::ArtifactChunkRequested {
                                                    peer_id: peer.to_string(),
                                                    artifact_id,
                                                    chunk_id,
                                                }
                                            }
                                            Err(_) => LiveControlPlaneEvent::ResponseSendFailure {
                                                peer_id: peer.to_string(),
                                                message: "artifact chunk response channel closed"
                                                    .into(),
                                            },
                                        }
                                    }
                                },
                                request_response::Message::Response { response, .. } => {
                                    match response {
                                        ControlPlaneResponse::Snapshot(snapshot) => {
                                            LiveControlPlaneEvent::SnapshotReceived {
                                                peer_id: peer.to_string(),
                                                snapshot,
                                            }
                                        }
                                        ControlPlaneResponse::ArtifactManifest(descriptor) => {
                                            LiveControlPlaneEvent::ArtifactManifestReceived {
                                                peer_id: peer.to_string(),
                                                descriptor,
                                            }
                                        }
                                        ControlPlaneResponse::ArtifactChunk(payload) => {
                                            LiveControlPlaneEvent::ArtifactChunkReceived {
                                                peer_id: peer.to_string(),
                                                payload,
                                            }
                                        }
                                    }
                                }
                            },
                            request_response::Event::OutboundFailure { peer, error, .. } => {
                                LiveControlPlaneEvent::RequestFailure {
                                    peer_id: peer.to_string(),
                                    message: error.to_string(),
                                }
                            }
                            request_response::Event::InboundFailure { peer, error, .. } => {
                                LiveControlPlaneEvent::InboundFailure {
                                    peer_id: peer.to_string(),
                                    message: error.to_string(),
                                }
                            }
                            request_response::Event::ResponseSent { peer, .. } => {
                                LiveControlPlaneEvent::SnapshotResponseSent {
                                    peer_id: peer.to_string(),
                                }
                            }
                        },
                        NativeControlPlaneBehaviourEvent::Gossipsub(event) => match *event {
                            gossipsub::Event::Message {
                                propagation_source,
                                message,
                                ..
                            } => match serde_json::from_slice::<PubsubEnvelope>(&message.data) {
                                Ok(envelope) => {
                                    let kind = pubsub_payload_kind(&envelope.payload).to_owned();
                                    let topic = envelope.topic_path.clone();
                                    apply_pubsub_payload(&mut self.snapshot, envelope.payload);
                                    LiveControlPlaneEvent::PubsubMessage {
                                        peer_id: propagation_source.to_string(),
                                        topic,
                                        kind,
                                    }
                                }
                                Err(error) => LiveControlPlaneEvent::Other {
                                    kind: format!("pubsub-decode-error:{error}"),
                                },
                            },
                            gossipsub::Event::Subscribed { peer_id, topic } => {
                                self.swarm
                                    .behaviour_mut()
                                    .gossipsub
                                    .add_explicit_peer(&peer_id);
                                LiveControlPlaneEvent::TopicSubscribed {
                                    topic: topic.to_string(),
                                }
                            }
                            gossipsub::Event::Unsubscribed { peer_id, .. } => {
                                self.swarm
                                    .behaviour_mut()
                                    .gossipsub
                                    .remove_explicit_peer(&peer_id);
                                LiveControlPlaneEvent::Other {
                                    kind: "pubsub-unsubscribed".into(),
                                }
                            }
                            other => LiveControlPlaneEvent::Other {
                                kind: format!("gossipsub:{other:?}"),
                            },
                        },
                        NativeControlPlaneBehaviourEvent::Identify(event) => match *event {
                            identify::Event::Received { peer_id, info, .. } => {
                                self.swarm
                                    .behaviour_mut()
                                    .gossipsub
                                    .add_explicit_peer(&peer_id);
                                LiveControlPlaneEvent::PeerIdentified {
                                    peer_id: peer_id.to_string(),
                                    listen_addresses: info
                                        .listen_addrs
                                        .into_iter()
                                        .map(|address| SwarmAddress(address.to_string()))
                                        .collect(),
                                    protocols: info
                                        .protocols
                                        .into_iter()
                                        .map(|protocol| protocol.to_string())
                                        .collect(),
                                }
                            }
                            identify::Event::Pushed { peer_id, .. } => {
                                LiveControlPlaneEvent::Other {
                                    kind: format!("identify-pushed:{peer_id}"),
                                }
                            }
                            identify::Event::Sent { peer_id, .. } => LiveControlPlaneEvent::Other {
                                kind: format!("identify-sent:{peer_id}"),
                            },
                            identify::Event::Error { peer_id, error, .. } => {
                                LiveControlPlaneEvent::Other {
                                    kind: format!("identify-error:{peer_id}:{error}"),
                                }
                            }
                        },
                        #[cfg(not(target_arch = "wasm32"))]
                        NativeControlPlaneBehaviourEvent::Mdns(event) => match event {
                            mdns::Event::Discovered(peers) => {
                                let mut discovered = Vec::new();
                                for (peer_id, address) in peers {
                                    self.swarm
                                        .behaviour_mut()
                                        .gossipsub
                                        .add_explicit_peer(&peer_id);
                                    let _ = self.swarm.dial(address.clone());
                                    discovered.push((
                                        peer_id.to_string(),
                                        SwarmAddress(address.to_string()),
                                    ));
                                }
                                LiveControlPlaneEvent::PeersDiscovered { peers: discovered }
                            }
                            mdns::Event::Expired(peers) => {
                                let mut expired = Vec::new();
                                for (peer_id, address) in peers {
                                    self.swarm
                                        .behaviour_mut()
                                        .gossipsub
                                        .remove_explicit_peer(&peer_id);
                                    expired.push((
                                        peer_id.to_string(),
                                        SwarmAddress(address.to_string()),
                                    ));
                                }
                                LiveControlPlaneEvent::PeersExpired { peers: expired }
                            }
                        },
                    },
                    SwarmEvent::NewListenAddr { address, .. } => {
                        LiveControlPlaneEvent::NewListenAddr {
                            address: SwarmAddress(address.to_string()),
                        }
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        LiveControlPlaneEvent::ConnectionEstablished {
                            peer_id: peer_id.to_string(),
                        }
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        LiveControlPlaneEvent::OutgoingConnectionError {
                            peer_id: peer_id.map(|peer_id| peer_id.to_string()),
                            message: error.to_string(),
                        }
                    }
                    SwarmEvent::IncomingConnectionError { error, .. } => {
                        LiveControlPlaneEvent::IncomingConnectionError {
                            message: error.to_string(),
                        }
                    }
                    other => LiveControlPlaneEvent::Other {
                        kind: other_native_control_name(&other).to_owned(),
                    },
                })
        })
    }
}

#[cfg(target_arch = "wasm32")]
pub struct NativeControlPlaneShell {
    inner: MemoryControlPlaneShell,
}

#[cfg(target_arch = "wasm32")]
impl NativeControlPlaneShell {
    pub fn new(control_protocol: ProtocolId) -> Result<Self, SwarmError> {
        Self::with_keypair(control_protocol, Keypair::generate_ed25519())
    }

    pub fn with_keypair(
        control_protocol: ProtocolId,
        keypair: Keypair,
    ) -> Result<Self, SwarmError> {
        Ok(Self {
            inner: MemoryControlPlaneShell::with_keypair(control_protocol, keypair),
        })
    }

    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        self.inner.local_peer_id()
    }

    pub fn listen_on(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        self.inner.listen_on(address)
    }

    pub fn dial(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        self.inner.dial(address)
    }

    pub fn connected_peer_count(&self) -> usize {
        self.inner.connected_peer_count()
    }

    pub fn publish_control(&mut self, announcement: ControlAnnouncement) {
        self.inner.publish_control(announcement);
    }

    pub fn publish_head(&mut self, announcement: HeadAnnouncement) {
        self.inner.publish_head(announcement);
    }

    pub fn publish_lease(&mut self, announcement: LeaseAnnouncement) {
        self.inner.publish_lease(announcement);
    }

    pub fn publish_merge(&mut self, announcement: MergeAnnouncement) {
        self.inner.publish_merge(announcement);
    }

    pub fn publish_merge_window(&mut self, announcement: MergeWindowAnnouncement) {
        self.inner.publish_merge_window(announcement);
    }

    pub fn publish_reducer_assignment(&mut self, announcement: ReducerAssignmentAnnouncement) {
        self.inner.publish_reducer_assignment(announcement);
    }

    pub fn publish_update(&mut self, announcement: UpdateEnvelopeAnnouncement) {
        self.inner.publish_update(announcement);
    }

    pub fn publish_aggregate(&mut self, announcement: AggregateAnnouncement) {
        self.inner.publish_aggregate(announcement);
    }

    pub fn publish_reduction_certificate(
        &mut self,
        announcement: ReductionCertificateAnnouncement,
    ) {
        self.inner.publish_reduction_certificate(announcement);
    }

    pub fn publish_reducer_load(&mut self, announcement: ReducerLoadAnnouncement) {
        self.inner.publish_reducer_load(announcement);
    }

    pub fn publish_auth(&mut self, announcement: PeerAuthAnnouncement) {
        self.inner.publish_auth(announcement);
    }

    pub fn publish_directory(&mut self, announcement: ExperimentDirectoryAnnouncement) {
        self.inner.publish_directory(announcement);
    }

    pub fn snapshot(&self) -> &ControlPlaneSnapshot {
        self.inner.snapshot()
    }

    pub fn subscribe_topic(&mut self, topic: OverlayTopic) -> Result<(), SwarmError> {
        self.inner.subscribe_topic(topic)
    }

    pub fn publish_pubsub(
        &mut self,
        topic: OverlayTopic,
        payload: PubsubPayload,
    ) -> Result<(), SwarmError> {
        self.inner.publish_pubsub(topic, payload)
    }

    pub fn publish_artifact(
        &mut self,
        descriptor: ArtifactDescriptor,
        chunks: Vec<ArtifactChunkPayload>,
    ) {
        self.inner.publish_artifact(descriptor, chunks);
    }

    pub fn request_snapshot(&mut self, peer_id: &str) -> Result<(), SwarmError> {
        self.inner.request_snapshot(peer_id)
    }

    pub fn fetch_snapshot(
        &mut self,
        peer_id: &str,
        timeout: Duration,
    ) -> Result<ControlPlaneSnapshot, SwarmError> {
        self.inner.fetch_snapshot(peer_id, timeout)
    }

    pub fn fetch_artifact_manifest(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        timeout: Duration,
    ) -> Result<Option<ArtifactDescriptor>, SwarmError> {
        self.inner
            .fetch_artifact_manifest(peer_id, artifact_id, timeout)
    }

    pub fn request_artifact_manifest(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
    ) -> Result<(), SwarmError> {
        self.inner.request_artifact_manifest(peer_id, artifact_id)
    }

    pub fn fetch_artifact_chunk(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
        timeout: Duration,
    ) -> Result<Option<ArtifactChunkPayload>, SwarmError> {
        self.inner
            .fetch_artifact_chunk(peer_id, artifact_id, chunk_id, timeout)
    }

    pub fn request_artifact_chunk(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
    ) -> Result<(), SwarmError> {
        self.inner
            .request_artifact_chunk(peer_id, artifact_id, chunk_id)
    }

    pub fn wait_event(&mut self, timeout: Duration) -> Option<LiveControlPlaneEvent> {
        self.inner.wait_event(timeout)
    }
}

pub enum ControlPlaneShell {
    Memory(Box<MemoryControlPlaneShell>),
    Native(Box<NativeControlPlaneShell>),
}

impl ControlPlaneShell {
    pub fn new(
        control_protocol: ProtocolId,
        keypair: Keypair,
        addresses: impl IntoIterator<Item = SwarmAddress>,
    ) -> Result<Self, SwarmError> {
        let addresses = addresses.into_iter().collect::<Vec<_>>();
        if addresses.iter().all(SwarmAddress::is_memory) {
            Ok(Self::Memory(Box::new(
                MemoryControlPlaneShell::with_keypair(control_protocol, keypair),
            )))
        } else {
            Ok(Self::Native(Box::new(
                NativeControlPlaneShell::with_keypair(control_protocol, keypair)?,
            )))
        }
    }

    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        match self {
            Self::Memory(shell) => shell.local_peer_id(),
            Self::Native(shell) => shell.local_peer_id(),
        }
    }

    pub fn listen_on(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        match self {
            Self::Memory(shell) => shell.listen_on(address),
            Self::Native(shell) => shell.listen_on(address),
        }
    }

    pub fn dial(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        match self {
            Self::Memory(shell) => shell.dial(address),
            Self::Native(shell) => shell.dial(address),
        }
    }

    pub fn connected_peer_count(&self) -> usize {
        match self {
            Self::Memory(shell) => shell.connected_peer_count(),
            Self::Native(shell) => shell.connected_peer_count(),
        }
    }

    pub fn publish_control(&mut self, announcement: ControlAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_control(announcement),
            Self::Native(shell) => shell.publish_control(announcement),
        }
    }

    pub fn publish_head(&mut self, announcement: HeadAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_head(announcement),
            Self::Native(shell) => shell.publish_head(announcement),
        }
    }

    pub fn publish_lease(&mut self, announcement: LeaseAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_lease(announcement),
            Self::Native(shell) => shell.publish_lease(announcement),
        }
    }

    pub fn publish_merge(&mut self, announcement: MergeAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_merge(announcement),
            Self::Native(shell) => shell.publish_merge(announcement),
        }
    }

    pub fn publish_merge_window(&mut self, announcement: MergeWindowAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_merge_window(announcement),
            Self::Native(shell) => shell.publish_merge_window(announcement),
        }
    }

    pub fn publish_reducer_assignment(&mut self, announcement: ReducerAssignmentAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_reducer_assignment(announcement),
            Self::Native(shell) => shell.publish_reducer_assignment(announcement),
        }
    }

    pub fn publish_update(&mut self, announcement: UpdateEnvelopeAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_update(announcement),
            Self::Native(shell) => shell.publish_update(announcement),
        }
    }

    pub fn publish_aggregate(&mut self, announcement: AggregateAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_aggregate(announcement),
            Self::Native(shell) => shell.publish_aggregate(announcement),
        }
    }

    pub fn publish_reduction_certificate(
        &mut self,
        announcement: ReductionCertificateAnnouncement,
    ) {
        match self {
            Self::Memory(shell) => shell.publish_reduction_certificate(announcement),
            Self::Native(shell) => shell.publish_reduction_certificate(announcement),
        }
    }

    pub fn publish_reducer_load(&mut self, announcement: ReducerLoadAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_reducer_load(announcement),
            Self::Native(shell) => shell.publish_reducer_load(announcement),
        }
    }

    pub fn publish_auth(&mut self, announcement: PeerAuthAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_auth(announcement),
            Self::Native(shell) => shell.publish_auth(announcement),
        }
    }

    pub fn publish_directory(&mut self, announcement: ExperimentDirectoryAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_directory(announcement),
            Self::Native(shell) => shell.publish_directory(announcement),
        }
    }

    pub fn snapshot(&self) -> &ControlPlaneSnapshot {
        match self {
            Self::Memory(shell) => shell.snapshot(),
            Self::Native(shell) => shell.snapshot(),
        }
    }

    pub fn subscribe_topic(&mut self, topic: OverlayTopic) -> Result<(), SwarmError> {
        match self {
            Self::Memory(shell) => shell.subscribe_topic(topic),
            Self::Native(shell) => shell.subscribe_topic(topic),
        }
    }

    pub fn publish_pubsub(
        &mut self,
        topic: OverlayTopic,
        payload: PubsubPayload,
    ) -> Result<(), SwarmError> {
        match self {
            Self::Memory(shell) => shell.publish_pubsub(topic, payload),
            Self::Native(shell) => shell.publish_pubsub(topic, payload),
        }
    }

    pub fn publish_artifact(
        &mut self,
        descriptor: ArtifactDescriptor,
        chunks: Vec<ArtifactChunkPayload>,
    ) {
        match self {
            Self::Memory(shell) => shell.publish_artifact(descriptor, chunks),
            Self::Native(shell) => shell.publish_artifact(descriptor, chunks),
        }
    }

    pub fn request_snapshot(&mut self, peer_id: &str) -> Result<(), SwarmError> {
        match self {
            Self::Memory(shell) => shell.request_snapshot(peer_id),
            Self::Native(shell) => shell.request_snapshot(peer_id),
        }
    }

    pub fn fetch_snapshot(
        &mut self,
        peer_id: &str,
        timeout: Duration,
    ) -> Result<ControlPlaneSnapshot, SwarmError> {
        match self {
            Self::Memory(shell) => shell.fetch_snapshot(peer_id, timeout),
            Self::Native(shell) => shell.fetch_snapshot(peer_id, timeout),
        }
    }

    pub fn fetch_artifact_manifest(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        timeout: Duration,
    ) -> Result<Option<ArtifactDescriptor>, SwarmError> {
        match self {
            Self::Memory(shell) => shell.fetch_artifact_manifest(peer_id, artifact_id, timeout),
            Self::Native(shell) => shell.fetch_artifact_manifest(peer_id, artifact_id, timeout),
        }
    }

    pub fn fetch_artifact_chunk(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
        timeout: Duration,
    ) -> Result<Option<ArtifactChunkPayload>, SwarmError> {
        match self {
            Self::Memory(shell) => {
                shell.fetch_artifact_chunk(peer_id, artifact_id, chunk_id, timeout)
            }
            Self::Native(shell) => {
                shell.fetch_artifact_chunk(peer_id, artifact_id, chunk_id, timeout)
            }
        }
    }

    pub fn wait_event(&mut self, timeout: Duration) -> Option<LiveControlPlaneEvent> {
        match self {
            Self::Memory(shell) => shell.wait_event(timeout),
            Self::Native(shell) => shell.wait_event(timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    #[cfg(not(target_arch = "wasm32"))]
    use std::io::{Read, Write};
    #[cfg(not(target_arch = "wasm32"))]
    use std::net::TcpListener;
    #[cfg(not(target_arch = "wasm32"))]
    use std::thread;
    use std::time::{Duration, Instant};

    use burn_p2p_core::{
        ArtifactDescriptor, ArtifactId, ArtifactKind, CapabilityCard, CapabilityCardId,
        CapabilityClass, ChunkDescriptor, ChunkId, ClientPlatform, ContentId, HeadDescriptor,
        HeadId, MetricValue, NetworkId, PeerId, PeerRoleSet, PersistenceClass, Precision,
        RevisionId, StudyId, TelemetrySummary, WindowActivation, WindowId,
    };
    use chrono::Utc;
    use futures::{executor::block_on, future};
    use semver::Version;

    use super::{
        ArtifactChunkPayload, ControlAnnouncement, ControlPlaneSnapshot, ExperimentControlEnvelope,
        ExperimentOverlaySet, HeadAnnouncement, LiveControlPlaneEvent, LiveSwarmEvent,
        MemoryControlPlaneShell, MemorySwarmShell, MigrationCoordinator, NativeControlPlaneShell,
        OverlayChannel, OverlayTopic, PeerObservation, PeerStore, ProtocolSet, PubsubPayload,
        RuntimeBoundary, RuntimeTransportPolicy, SwarmAddress, SwarmError, TransportKind,
    };
    use burn_p2p_experiment::ActivationTarget;

    #[cfg(not(target_arch = "wasm32"))]
    fn spawn_browser_edge_server(
        directory: Vec<burn_p2p_core::ExperimentDirectoryEntry>,
        heads: Vec<HeadDescriptor>,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test edge");
        let address = listener.local_addr().expect("local addr");
        let directory_body = serde_json::to_string(&directory).expect("encode directory");
        let heads_body = serde_json::to_string(&heads).expect("encode heads");

        let handle = thread::spawn(move || {
            for request_index in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept request");
                let mut buffer = [0_u8; 8192];
                let size = stream.read(&mut buffer).expect("read request");
                let request = String::from_utf8_lossy(&buffer[..size]).to_string();
                let request_lower = request.to_ascii_lowercase();

                let (body, expected_path) = if request.starts_with("GET /directory ") {
                    assert!(
                        request_lower.contains("x-session-id: browser-session"),
                        "directory request should include x-session-id header"
                    );
                    (directory_body.clone(), "/directory")
                } else if request.starts_with("GET /heads ") {
                    assert!(
                        !request_lower.contains("x-session-id: browser-session"),
                        "heads request should not include x-session-id header"
                    );
                    (heads_body.clone(), "/heads")
                } else {
                    panic!("unexpected browser edge request: {request}");
                };

                if request_index == 0 {
                    assert!(
                        request.starts_with("GET /directory "),
                        "first request should fetch /directory before /heads, got {expected_path}"
                    );
                }

                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write response");
                stream.flush().expect("flush response");
            }
        });

        (format!("http://{address}"), handle)
    }

    #[test]
    fn experiment_topics_match_the_documented_paths() {
        let overlays = ExperimentOverlaySet::new(
            NetworkId::new("mainnet"),
            StudyId::new("study"),
            burn_p2p_core::ExperimentId::new("exp"),
        )
        .expect("overlays");

        assert_eq!(overlays.control.as_str(), "/burn-p2p/mainnet/control");
        assert_eq!(
            overlays.heads.as_str(),
            "/burn-p2p/mainnet/study/study/exp/exp/heads"
        );
        assert_eq!(
            overlays.telemetry.as_str(),
            "/burn-p2p/mainnet/study/study/exp/exp/telemetry"
        );
    }

    #[test]
    fn swarm_address_rejects_invalid_multiaddr() {
        let error = SwarmAddress::new("definitely-not-a-multiaddr").expect_err("invalid");
        assert!(matches!(error, super::SwarmError::InvalidAddress(_)));
    }

    #[test]
    fn protocol_set_is_derived_from_network_id() {
        let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
        assert_eq!(protocols.control.as_str(), "/burn-p2p/network/v1/control");
        assert_eq!(
            protocols.chunk_fetch.as_str(),
            "/burn-p2p/network/v1/chunk-fetch"
        );
    }

    #[test]
    fn browser_transport_policy_prefers_browser_transports() {
        let policy = RuntimeTransportPolicy::browser();
        assert_eq!(
            policy.preferred_transports,
            vec![
                TransportKind::WebTransport,
                TransportKind::WebRtc,
                TransportKind::WebSocket,
                TransportKind::RelayReservation,
            ]
        );
    }

    #[test]
    fn native_transport_policy_prefers_tcp_before_quic_for_current_runtime() {
        let policy = RuntimeTransportPolicy::native();
        assert_eq!(
            policy.preferred_transports,
            vec![
                TransportKind::Tcp,
                TransportKind::Quic,
                TransportKind::WebSocket,
                TransportKind::RelayReservation,
            ]
        );
    }

    #[test]
    fn peer_store_aggregates_connected_peers_and_eta() {
        let now = Utc::now();
        let network_id = NetworkId::new("network");
        let study_id = StudyId::new("study");
        let experiment_id = burn_p2p_core::ExperimentId::new("exp");
        let revision_id = RevisionId::new("rev");
        let peer_id = PeerId::new("peer");

        let capability_card = CapabilityCard {
            card_id: CapabilityCardId::new("card"),
            peer_id: peer_id.clone(),
            platform: ClientPlatform::Native,
            roles: PeerRoleSet::default_trainer(),
            preferred_backends: vec!["cuda".into()],
            recommended_classes: BTreeSet::from([CapabilityClass::TrainerGpu]),
            device_memory_bytes: Some(24 * 1024 * 1024 * 1024),
            system_memory_bytes: 64 * 1024 * 1024 * 1024,
            disk_bytes: 1024 * 1024 * 1024 * 1024,
            upload_mbps: 1000.0,
            download_mbps: 1000.0,
            persistence: PersistenceClass::Durable,
            work_units_per_second: 50.0,
            attestation_level: burn_p2p_core::AttestationLevel::Manifest,
            benchmark_hash: None,
            reported_at: now,
        };

        let telemetry = TelemetrySummary {
            network_id,
            study_id,
            experiment_id,
            revision_id,
            window_id: WindowId(3),
            active_peers: 7,
            accepted_contributions: 9,
            throughput_work_units_per_second: 40.0,
            network: burn_p2p_core::NetworkEstimate {
                connected_peers: 1,
                observed_peers: 1,
                estimated_network_size: 1.0,
                estimated_total_vram_bytes: None,
                estimated_total_flops: None,
                eta_lower_seconds: None,
                eta_upper_seconds: None,
            },
            metrics: BTreeMap::from([("estimated_flops".into(), MetricValue::Float(1250.0))]),
            captured_at: now,
        };

        let mut store = PeerStore::default();
        store.upsert(
            PeerObservation::new(peer_id.clone(), now)
                .with_capability_card(capability_card)
                .with_telemetry(telemetry),
        );

        let stats = store.stats(Some(400));

        assert_eq!(stats.connected_peers, 1);
        assert_eq!(stats.connected_peer_ids, vec![peer_id]);
        assert_eq!(stats.network_estimate.observed_peers, 1);
        assert_eq!(stats.network_estimate.estimated_network_size, 7.0);
        assert_eq!(
            stats.network_estimate.estimated_total_vram_bytes,
            Some(24_u128 * 1024 * 1024 * 1024)
        );
        assert_eq!(stats.network_estimate.estimated_total_flops, Some(1250.0));
        assert_eq!(stats.network_estimate.eta_lower_seconds, Some(8));
        assert_eq!(stats.network_estimate.eta_upper_seconds, Some(12));
    }

    #[test]
    fn migration_plan_only_switches_changed_topics() {
        let current = ExperimentOverlaySet::new(
            NetworkId::new("network"),
            StudyId::new("study"),
            burn_p2p_core::ExperimentId::new("exp-a"),
        )
        .expect("current");
        let next = ExperimentOverlaySet::new(
            NetworkId::new("network"),
            StudyId::new("study"),
            burn_p2p_core::ExperimentId::new("exp-b"),
        )
        .expect("next");

        let plan = MigrationCoordinator::plan_overlay_transition(
            &current,
            &next,
            &ActivationTarget {
                activation: WindowActivation {
                    activation_window: WindowId(11),
                    grace_windows: 1,
                },
                required_client_capabilities: BTreeSet::from(["wgpu".into()]),
            },
            Some(HeadId::new("base")),
        );

        assert_eq!(plan.join_topics.len(), 4);
        assert_eq!(plan.leave_topics.len(), 4);
        assert_eq!(plan.fetch_base_head_id, Some(HeadId::new("base")));
    }

    #[test]
    fn runtime_boundary_derives_protocols_from_genesis() {
        let runtime = RuntimeBoundary::for_platform(
            &burn_p2p_core::GenesisSpec {
                network_id: NetworkId::new("network"),
                protocol_version: Version::new(0, 1, 0),
                display_name: "network".into(),
                created_at: Utc::now(),
                metadata: BTreeMap::new(),
            },
            ClientPlatform::Browser,
            vec![SwarmAddress::new("/dns4/bootstrap.example.com/tcp/4001/ws").expect("addr")],
            vec![SwarmAddress::new("/ip4/0.0.0.0/udp/4001/quic-v1").expect("addr")],
        )
        .expect("runtime");

        assert_eq!(runtime.transport_policy, RuntimeTransportPolicy::browser());
        assert_eq!(
            runtime.protocols.control.as_str(),
            "/burn-p2p/network/v1/control"
        );
    }

    #[test]
    fn overlay_topic_rejects_control_inside_experiment_scope() {
        let error = OverlayTopic::experiment(
            NetworkId::new("network"),
            StudyId::new("study"),
            burn_p2p_core::ExperimentId::new("exp"),
            OverlayChannel::Control,
        )
        .expect_err("invalid");

        assert!(matches!(
            error,
            super::SwarmError::InvalidOverlayChannel { .. }
        ));
    }

    #[test]
    fn wire_types_round_trip_through_serde() {
        let now = Utc::now();
        let overlay = OverlayTopic::control(NetworkId::new("network"));
        let head = HeadDescriptor {
            head_id: HeadId::new("head"),
            study_id: StudyId::new("study"),
            experiment_id: burn_p2p_core::ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact"),
            parent_head_id: None,
            global_step: 42,
            created_at: now,
            metrics: BTreeMap::new(),
        };

        let payload = super::HeadAnnouncement {
            overlay,
            provider_peer_id: Some(PeerId::new("provider")),
            head,
            announced_at: now,
        };

        let bytes = serde_json::to_vec(&payload).expect("encode");
        let decoded: super::HeadAnnouncement = serde_json::from_slice(&bytes).expect("decode");

        assert_eq!(decoded, payload);
    }

    #[test]
    fn request_metadata_types_are_constructible() {
        let now = Utc::now();
        let provider = PeerId::new("provider");
        let descriptor = ChunkDescriptor {
            chunk_id: ChunkId::new("chunk"),
            offset_bytes: 0,
            length_bytes: 128,
            chunk_hash: ContentId::new("hash"),
        };

        let response = super::ChunkFetchResponse {
            provider,
            artifact_id: ArtifactId::new("artifact"),
            chunk: descriptor,
            generated_at: now,
        };

        assert_eq!(response.chunk.length_bytes, 128);
    }

    #[test]
    fn control_envelope_announcements_are_usable() {
        let now = Utc::now();
        let envelope = ExperimentControlEnvelope {
            network_id: NetworkId::new("network"),
            command: burn_p2p_experiment::ExperimentControlCommand::PauseExperiment {
                study_id: StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                reason: Some("maintenance".into()),
                target: ActivationTarget {
                    activation: WindowActivation {
                        activation_window: WindowId(9),
                        grace_windows: 1,
                    },
                    required_client_capabilities: BTreeSet::from(["cuda".into()]),
                },
            },
        };

        let cert = envelope
            .into_signed_cert(
                burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: now,
                    signature_hex: "010203".into(),
                },
                Version::new(0, 1, 0),
            )
            .expect("cert");

        let announcement = super::ControlAnnouncement {
            overlay: OverlayTopic::control(NetworkId::new("network")),
            certificate: cert,
            announced_at: now,
        };

        assert_eq!(announcement.overlay.channel, OverlayChannel::Control);
    }

    #[test]
    fn memory_swarm_shell_listens_and_connects_over_memory_transport() {
        let mut listener = MemorySwarmShell::new();
        let mut dialer = MemorySwarmShell::new();
        let listener_peer_id = listener.local_peer_id().to_string();
        let dialer_peer_id = dialer.local_peer_id().to_string();

        listener
            .listen_on(SwarmAddress::new("/memory/0").expect("memory addr"))
            .expect("listen");

        let listen_addr = block_on(future::poll_fn(|cx| match listener.poll_event(cx) {
            std::task::Poll::Ready(LiveSwarmEvent::NewListenAddr { address }) => {
                std::task::Poll::Ready(address)
            }
            std::task::Poll::Ready(_) | std::task::Poll::Pending => std::task::Poll::Pending,
        }));

        dialer.dial(listen_addr).expect("dial");

        let mut listener_connected = false;
        let mut dialer_connected = false;
        let (listener_connected, dialer_connected) = block_on(future::poll_fn(|cx| {
            loop {
                match listener.poll_event(cx) {
                    std::task::Poll::Ready(LiveSwarmEvent::ConnectionEstablished { peer_id }) => {
                        listener_connected |= peer_id == dialer_peer_id;
                    }
                    std::task::Poll::Ready(_) => {}
                    std::task::Poll::Pending => break,
                }
            }

            loop {
                match dialer.poll_event(cx) {
                    std::task::Poll::Ready(LiveSwarmEvent::ConnectionEstablished { peer_id }) => {
                        dialer_connected |= peer_id == listener_peer_id;
                    }
                    std::task::Poll::Ready(_) => {}
                    std::task::Poll::Pending => break,
                }
            }

            if listener_connected && dialer_connected {
                std::task::Poll::Ready((listener_connected, dialer_connected))
            } else {
                std::task::Poll::Pending
            }
        }));

        assert!(listener_connected);
        assert!(dialer_connected);
        assert_eq!(listener.connected_peer_count(), 1);
        assert_eq!(dialer.connected_peer_count(), 1);
    }

    #[test]
    fn control_plane_shell_exchanges_snapshot_requests_and_responses() {
        let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
        let mut listener = MemoryControlPlaneShell::new(protocols.control.clone());
        let mut dialer = MemoryControlPlaneShell::new(protocols.control);
        let listener_peer_id = listener.local_peer_id().to_string();

        let now = Utc::now();
        listener.publish_control(ControlAnnouncement {
            overlay: OverlayTopic::control(NetworkId::new("network")),
            certificate: ExperimentControlEnvelope {
                network_id: NetworkId::new("network"),
                command: burn_p2p_experiment::ExperimentControlCommand::ResumeExperiment {
                    study_id: StudyId::new("study"),
                    experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                    target: ActivationTarget {
                        activation: WindowActivation {
                            activation_window: WindowId(2),
                            grace_windows: 1,
                        },
                        required_client_capabilities: BTreeSet::from(["cuda".into()]),
                    },
                },
            }
            .into_signed_cert(
                burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: now,
                    signature_hex: "aabbcc".into(),
                },
                Version::new(0, 1, 0),
            )
            .expect("control cert"),
            announced_at: now,
        });
        listener.publish_head(HeadAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("network"),
                StudyId::new("study"),
                burn_p2p_core::ExperimentId::new("exp"),
                OverlayChannel::Heads,
            )
            .expect("heads overlay"),
            provider_peer_id: Some(PeerId::new(listener_peer_id.clone())),
            head: HeadDescriptor {
                head_id: HeadId::new("head-1"),
                study_id: StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                artifact_id: ArtifactId::new("artifact-1"),
                parent_head_id: Some(HeadId::new("genesis-head")),
                global_step: 1,
                created_at: now,
                metrics: BTreeMap::new(),
            },
            announced_at: now,
        });

        listener
            .listen_on(SwarmAddress::new("/memory/0").expect("memory addr"))
            .expect("listen");
        let listen_addr = block_on(future::poll_fn(|cx| match listener.poll_event(cx) {
            std::task::Poll::Ready(LiveControlPlaneEvent::NewListenAddr { address }) => {
                std::task::Poll::Ready(address)
            }
            std::task::Poll::Ready(_) | std::task::Poll::Pending => std::task::Poll::Pending,
        }));

        dialer.dial(listen_addr).expect("dial");

        let mut listener_connected = false;
        let mut dialer_connected = false;
        block_on(future::poll_fn(|cx| {
            loop {
                match listener.poll_event(cx) {
                    std::task::Poll::Ready(LiveControlPlaneEvent::ConnectionEstablished {
                        ..
                    }) => {
                        listener_connected = true;
                    }
                    std::task::Poll::Ready(_) => {}
                    std::task::Poll::Pending => break,
                }
            }

            loop {
                match dialer.poll_event(cx) {
                    std::task::Poll::Ready(LiveControlPlaneEvent::ConnectionEstablished {
                        ..
                    }) => {
                        dialer_connected = true;
                    }
                    std::task::Poll::Ready(_) => {}
                    std::task::Poll::Pending => break,
                }
            }

            if listener_connected && dialer_connected {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        }));

        dialer
            .request_snapshot(&listener_peer_id)
            .expect("request snapshot");

        let mut listener_saw_request = false;
        let snapshot = block_on(future::poll_fn(|cx| {
            loop {
                match listener.poll_event(cx) {
                    std::task::Poll::Ready(LiveControlPlaneEvent::SnapshotRequested {
                        peer_id,
                    }) => {
                        listener_saw_request |= !peer_id.is_empty();
                    }
                    std::task::Poll::Ready(_) => {}
                    std::task::Poll::Pending => break,
                }
            }

            loop {
                match dialer.poll_event(cx) {
                    std::task::Poll::Ready(LiveControlPlaneEvent::SnapshotReceived {
                        snapshot,
                        ..
                    }) => return std::task::Poll::Ready(snapshot),
                    std::task::Poll::Ready(_) => {}
                    std::task::Poll::Pending => break,
                }
            }

            std::task::Poll::Pending
        }));

        assert!(listener_saw_request);
        assert_eq!(
            snapshot,
            ControlPlaneSnapshot {
                control_announcements: listener.snapshot().control_announcements.clone(),
                head_announcements: listener.snapshot().head_announcements.clone(),
                lease_announcements: listener.snapshot().lease_announcements.clone(),
                merge_announcements: listener.snapshot().merge_announcements.clone(),
                merge_window_announcements: listener.snapshot().merge_window_announcements.clone(),
                reducer_assignment_announcements: listener
                    .snapshot()
                    .reducer_assignment_announcements
                    .clone(),
                update_announcements: listener.snapshot().update_announcements.clone(),
                aggregate_announcements: listener.snapshot().aggregate_announcements.clone(),
                reduction_certificate_announcements: listener
                    .snapshot()
                    .reduction_certificate_announcements
                    .clone(),
                reducer_load_announcements: listener.snapshot().reducer_load_announcements.clone(),
                auth_announcements: listener.snapshot().auth_announcements.clone(),
                directory_announcements: listener.snapshot().directory_announcements.clone(),
            }
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn browser_edge_control_plane_client_fetches_directory_and_heads_snapshot() {
        let network_id = NetworkId::new("browser-network");
        let study_id = StudyId::new("study");
        let experiment_id = burn_p2p_core::ExperimentId::new("exp");
        let revision_id = RevisionId::new("rev");
        let now = Utc::now();
        let directory = vec![burn_p2p_core::ExperimentDirectoryEntry {
            network_id: network_id.clone(),
            study_id: study_id.clone(),
            experiment_id: experiment_id.clone(),
            workload_id: burn_p2p_core::WorkloadId::new("workload"),
            display_name: "Browser Edge Experiment".into(),
            model_schema_hash: ContentId::new("schema"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("dataset"),
            resource_requirements: burn_p2p_core::ExperimentResourceRequirements {
                minimum_roles: BTreeSet::new(),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: Some(1024),
                estimated_download_bytes: 1024 * 1024,
                estimated_window_seconds: 30,
            },
            visibility: burn_p2p_core::ExperimentVisibility::OptIn,
            opt_in_policy: burn_p2p_core::ExperimentOptInPolicy::Scoped,
            current_revision_id: revision_id.clone(),
            current_head_id: Some(HeadId::new("head-1")),
            allowed_roles: PeerRoleSet::default(),
            allowed_scopes: BTreeSet::new(),
            metadata: BTreeMap::new(),
        }];
        let heads = vec![HeadDescriptor {
            head_id: HeadId::new("head-1"),
            study_id: study_id.clone(),
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            artifact_id: ArtifactId::new("artifact"),
            parent_head_id: Some(HeadId::new("genesis-head")),
            global_step: 7,
            created_at: now,
            metrics: BTreeMap::new(),
        }];
        let (base_url, server) = spawn_browser_edge_server(directory.clone(), heads.clone());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        let snapshot = runtime
            .block_on(async {
                super::BrowserEdgeControlPlaneClient::new(base_url, network_id.clone())
                    .fetch_snapshot(Some(&ContentId::new("browser-session")))
                    .await
            })
            .expect("fetch snapshot");

        server.join().expect("join test edge");

        assert_eq!(snapshot.directory_announcements.len(), 1);
        assert_eq!(snapshot.directory_announcements[0].entries, directory);
        assert_eq!(snapshot.head_announcements.len(), 1);
        assert_eq!(snapshot.head_announcements[0].head, heads[0]);
        assert_eq!(
            snapshot.head_announcements[0].overlay,
            OverlayTopic::experiment(network_id, study_id, experiment_id, OverlayChannel::Heads)
                .expect("heads overlay")
        );
    }

    #[test]
    fn native_control_plane_shell_exchanges_snapshot_requests_and_responses_over_tcp() {
        let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
        let mut listener =
            NativeControlPlaneShell::new(protocols.control.clone()).expect("native listener");
        let mut dialer = NativeControlPlaneShell::new(protocols.control).expect("native dialer");
        let listener_peer_id = listener.local_peer_id().to_string();

        let now = Utc::now();
        listener.publish_head(HeadAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("network"),
                StudyId::new("study"),
                burn_p2p_core::ExperimentId::new("exp"),
                OverlayChannel::Heads,
            )
            .expect("heads overlay"),
            provider_peer_id: Some(PeerId::new(listener_peer_id.clone())),
            head: HeadDescriptor {
                head_id: HeadId::new("head-1"),
                study_id: StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                artifact_id: ArtifactId::new("artifact-1"),
                parent_head_id: Some(HeadId::new("genesis-head")),
                global_step: 1,
                created_at: now,
                metrics: BTreeMap::new(),
            },
            announced_at: now,
        });

        listener
            .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("tcp addr"))
            .expect("listen");

        let listen_addr = loop {
            match listener.wait_event(Duration::from_secs(2)) {
                Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
                Some(_) => {}
                None => panic!("listener did not produce a listen address"),
            }
        };

        dialer.dial(listen_addr).expect("dial");

        let mut listener_connected = false;
        let mut dialer_connected = false;
        let mut listener_events = Vec::new();
        let mut dialer_events = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !(listener_connected && dialer_connected) {
            assert!(
                Instant::now() < deadline,
                "native shells did not connect; listener events: {:?}; dialer events: {:?}",
                listener_events,
                dialer_events
            );

            if let Some(event) = listener.wait_event(Duration::from_millis(100)) {
                listener_events.push(format!("{event:?}"));
                listener_connected |=
                    matches!(event, LiveControlPlaneEvent::ConnectionEstablished { .. });
            }

            if let Some(event) = dialer.wait_event(Duration::from_millis(100)) {
                dialer_events.push(format!("{event:?}"));
                dialer_connected |=
                    matches!(event, LiveControlPlaneEvent::ConnectionEstablished { .. });
            }
        }

        dialer
            .request_snapshot(&listener_peer_id)
            .expect("request snapshot");

        let deadline = Instant::now() + Duration::from_secs(5);
        let snapshot = loop {
            assert!(
                Instant::now() < deadline,
                "dialer did not receive native snapshot"
            );

            if let Some(event) = listener.wait_event(Duration::from_millis(100))
                && let LiveControlPlaneEvent::SnapshotRequested { .. } = event
            {
                // keep polling until the response lands on the dialer
            }

            match dialer.wait_event(Duration::from_millis(100)) {
                Some(LiveControlPlaneEvent::SnapshotReceived { snapshot, .. }) => break snapshot,
                Some(_) => {}
                None => {}
            }
        };

        assert_eq!(snapshot.head_announcements.len(), 1);
        assert_eq!(
            snapshot.head_announcements[0].head.head_id,
            HeadId::new("head-1")
        );
    }

    #[test]
    fn native_control_plane_shell_exchanges_snapshot_requests_and_responses_over_quic() {
        let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
        let mut listener =
            NativeControlPlaneShell::new(protocols.control.clone()).expect("native listener");
        let mut dialer = NativeControlPlaneShell::new(protocols.control).expect("native dialer");
        let listener_peer_id = listener.local_peer_id().to_string();

        let now = Utc::now();
        listener.publish_head(HeadAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("network"),
                StudyId::new("study"),
                burn_p2p_core::ExperimentId::new("exp"),
                OverlayChannel::Heads,
            )
            .expect("heads overlay"),
            provider_peer_id: Some(PeerId::new(listener_peer_id.clone())),
            head: HeadDescriptor {
                head_id: HeadId::new("head-quic-1"),
                study_id: StudyId::new("study"),
                experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                artifact_id: ArtifactId::new("artifact-quic-1"),
                parent_head_id: Some(HeadId::new("genesis-head")),
                global_step: 1,
                created_at: now,
                metrics: BTreeMap::new(),
            },
            announced_at: now,
        });

        listener
            .listen_on(SwarmAddress::new("/ip4/127.0.0.1/udp/0/quic-v1").expect("quic addr"))
            .expect("listen");

        let listen_addr = loop {
            match listener.wait_event(Duration::from_secs(2)) {
                Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
                Some(_) => {}
                None => panic!("listener did not produce a QUIC listen address"),
            }
        };

        dialer.dial(listen_addr).expect("dial");

        let mut listener_connected = false;
        let mut dialer_connected = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while !(listener_connected && dialer_connected) {
            assert!(
                Instant::now() < deadline,
                "native QUIC shells did not connect"
            );

            if let Some(event) = listener.wait_event(Duration::from_millis(100))
                && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
            {
                listener_connected = true;
            }

            if let Some(event) = dialer.wait_event(Duration::from_millis(100))
                && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
            {
                dialer_connected = true;
            }
        }

        dialer
            .request_snapshot(&listener_peer_id)
            .expect("request snapshot");

        let deadline = Instant::now() + Duration::from_secs(5);
        let snapshot = loop {
            assert!(
                Instant::now() < deadline,
                "dialer did not receive QUIC snapshot"
            );

            if let Some(event) = listener.wait_event(Duration::from_millis(100))
                && let LiveControlPlaneEvent::SnapshotRequested { .. } = event
            {}

            match dialer.wait_event(Duration::from_millis(100)) {
                Some(LiveControlPlaneEvent::SnapshotReceived { snapshot, .. }) => break snapshot,
                Some(_) => {}
                None => {}
            }
        };

        assert_eq!(snapshot.head_announcements.len(), 1);
        assert_eq!(
            snapshot.head_announcements[0].head.head_id,
            HeadId::new("head-quic-1")
        );
    }

    #[test]
    fn native_control_plane_shell_propagates_control_announcements_over_pubsub() {
        let network_id = NetworkId::new("network");
        let protocols = ProtocolSet::for_network(&network_id).expect("protocols");
        let control_overlay = OverlayTopic::control(network_id.clone());
        let mut listener =
            NativeControlPlaneShell::new(protocols.control.clone()).expect("native listener");
        let mut dialer = NativeControlPlaneShell::new(protocols.control).expect("native dialer");

        let now = Utc::now();
        let announcement = ControlAnnouncement {
            overlay: control_overlay.clone(),
            certificate: ExperimentControlEnvelope {
                network_id: network_id.clone(),
                command: burn_p2p_experiment::ExperimentControlCommand::ResumeExperiment {
                    study_id: StudyId::new("study"),
                    experiment_id: burn_p2p_core::ExperimentId::new("exp"),
                    target: ActivationTarget {
                        activation: WindowActivation {
                            activation_window: WindowId(2),
                            grace_windows: 1,
                        },
                        required_client_capabilities: BTreeSet::from(["cuda".into()]),
                    },
                },
            }
            .into_signed_cert(
                burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: now,
                    signature_hex: "ddeeff".into(),
                },
                Version::new(0, 1, 0),
            )
            .expect("control cert"),
            announced_at: now,
        };

        listener
            .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("tcp addr"))
            .expect("listen");

        let listen_addr = loop {
            match listener.wait_event(Duration::from_secs(2)) {
                Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
                Some(_) => {}
                None => panic!("listener did not produce a listen address"),
            }
        };

        dialer.dial(listen_addr).expect("dial");

        let mut listener_connected = false;
        let mut dialer_connected = false;
        let connect_deadline = Instant::now() + Duration::from_secs(5);
        while !(listener_connected && dialer_connected) {
            assert!(
                Instant::now() < connect_deadline,
                "native shells did not connect"
            );

            if let Some(event) = listener.wait_event(Duration::from_millis(100))
                && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
            {
                listener_connected = true;
            }

            if let Some(event) = dialer.wait_event(Duration::from_millis(100))
                && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
            {
                dialer_connected = true;
            }
        }

        listener
            .subscribe_topic(control_overlay.clone())
            .expect("listener subscribe");
        dialer
            .subscribe_topic(control_overlay.clone())
            .expect("dialer subscribe");

        let publish_deadline = Instant::now() + Duration::from_secs(10);
        let mut listener_saw_remote_subscription = false;
        let mut published = false;
        let mut received_pubsub = false;
        while !received_pubsub {
            assert!(
                Instant::now() < publish_deadline,
                "dialer did not receive control announcement over pubsub"
            );

            let pump_until = Instant::now() + Duration::from_millis(250);
            while Instant::now() < pump_until {
                if let Some(event) = listener.wait_event(Duration::from_millis(25)) {
                    match event {
                        LiveControlPlaneEvent::TopicSubscribed { topic }
                            if topic == control_overlay.as_str() =>
                        {
                            listener_saw_remote_subscription = true;
                        }
                        LiveControlPlaneEvent::PubsubMessage { kind, .. } if kind == "control" => {}
                        _ => {}
                    }
                }

                if let Some(event) = dialer.wait_event(Duration::from_millis(25))
                    && let LiveControlPlaneEvent::PubsubMessage { kind, topic, .. } = event
                    && kind == "control"
                    && topic == control_overlay.as_str()
                {
                    received_pubsub = true;
                    break;
                }
            }

            if !published && listener_saw_remote_subscription {
                listener.publish_control(announcement.clone());
                match listener.publish_pubsub(
                    control_overlay.clone(),
                    PubsubPayload::Control(announcement.clone()),
                ) {
                    Ok(()) => published = true,
                    Err(SwarmError::Pubsub(message))
                        if message.contains("NoPeersSubscribedToTopic") => {}
                    Err(error) => panic!("publish control pubsub: {error}"),
                }
            }
        }

        assert!(
            dialer
                .snapshot()
                .control_announcements
                .contains(&announcement)
        );
    }

    #[test]
    fn native_control_plane_shell_transfers_artifact_manifests_and_chunks_over_tcp() {
        let protocols = ProtocolSet::for_network(&NetworkId::new("network")).expect("protocols");
        let mut listener =
            NativeControlPlaneShell::new(protocols.control.clone()).expect("native listener");
        let mut dialer = NativeControlPlaneShell::new(protocols.control).expect("native dialer");

        let descriptor = ArtifactDescriptor {
            artifact_id: ArtifactId::new("artifact-1"),
            kind: ArtifactKind::ServeHead,
            head_id: Some(HeadId::new("head-1")),
            base_head_id: None,
            precision: Precision::Fp16,
            model_schema_hash: ContentId::new("schema"),
            record_format: "burn-record:bin".into(),
            bytes_len: 6,
            chunks: vec![ChunkDescriptor {
                chunk_id: ChunkId::new("chunk-1"),
                offset_bytes: 0,
                length_bytes: 6,
                chunk_hash: ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(
                    b"abcdef",
                )),
            }],
            root_hash: ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(b"abcdef")),
        };
        listener.publish_artifact(
            descriptor.clone(),
            vec![ArtifactChunkPayload {
                artifact_id: descriptor.artifact_id.clone(),
                chunk: descriptor.chunks[0].clone(),
                bytes: b"abcdef".to_vec(),
                generated_at: Utc::now(),
            }],
        );

        listener
            .listen_on(SwarmAddress::new("/ip4/127.0.0.1/tcp/0").expect("tcp addr"))
            .expect("listen");

        let listen_addr = loop {
            match listener.wait_event(Duration::from_secs(2)) {
                Some(LiveControlPlaneEvent::NewListenAddr { address }) => break address,
                Some(_) => {}
                None => panic!("listener did not produce a listen address"),
            }
        };

        dialer.dial(listen_addr).expect("dial");
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut listener_connected = false;
        let mut dialer_connected = false;
        while !(listener_connected && dialer_connected) {
            assert!(Instant::now() < deadline, "native shells did not connect");

            if let Some(event) = listener.wait_event(Duration::from_millis(100))
                && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
            {
                listener_connected = true;
            }

            if let Some(event) = dialer.wait_event(Duration::from_millis(100))
                && let LiveControlPlaneEvent::ConnectionEstablished { .. } = event
            {
                dialer_connected = true;
            }
        }

        dialer
            .request_artifact_manifest(
                &listener.local_peer_id().to_string(),
                descriptor.artifact_id.clone(),
            )
            .expect("request descriptor");
        let deadline = Instant::now() + Duration::from_secs(5);
        let fetched_descriptor = loop {
            assert!(Instant::now() < deadline, "fetch descriptor");
            if let Some(
                LiveControlPlaneEvent::ArtifactManifestRequested { .. }
                | LiveControlPlaneEvent::SnapshotResponseSent { .. }
                | LiveControlPlaneEvent::ConnectionEstablished { .. }
                | LiveControlPlaneEvent::Other { .. },
            ) = listener.wait_event(Duration::from_millis(50))
            {}
            if let Some(event) = dialer.wait_event(Duration::from_millis(50)) {
                match event {
                    LiveControlPlaneEvent::ArtifactManifestReceived { descriptor, .. } => {
                        break descriptor.expect("descriptor");
                    }
                    LiveControlPlaneEvent::RequestFailure { message, .. } => {
                        panic!("fetch descriptor: {message}");
                    }
                    _ => {}
                }
            }
        };
        assert_eq!(fetched_descriptor, descriptor);

        dialer
            .request_artifact_chunk(
                &listener.local_peer_id().to_string(),
                descriptor.artifact_id.clone(),
                descriptor.chunks[0].chunk_id.clone(),
            )
            .expect("request chunk");
        let deadline = Instant::now() + Duration::from_secs(5);
        let chunk_payload = loop {
            assert!(Instant::now() < deadline, "fetch chunk");
            if let Some(
                LiveControlPlaneEvent::ArtifactChunkRequested { .. }
                | LiveControlPlaneEvent::SnapshotResponseSent { .. }
                | LiveControlPlaneEvent::ConnectionEstablished { .. }
                | LiveControlPlaneEvent::Other { .. },
            ) = listener.wait_event(Duration::from_millis(50))
            {}
            if let Some(event) = dialer.wait_event(Duration::from_millis(50)) {
                match event {
                    LiveControlPlaneEvent::ArtifactChunkReceived { payload, .. } => {
                        break payload.expect("chunk payload");
                    }
                    LiveControlPlaneEvent::RequestFailure { message, .. } => {
                        panic!("fetch chunk: {message}");
                    }
                    _ => {}
                }
            }
        };
        assert_eq!(chunk_payload.bytes, b"abcdef");
        assert_eq!(chunk_payload.chunk, descriptor.chunks[0]);
    }
}
