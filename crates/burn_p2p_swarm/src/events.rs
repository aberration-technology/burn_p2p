use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates high-level memory/native swarm shell events.
pub enum LiveSwarmEvent {
    /// Uses the new listen addr variant.
    NewListenAddr {
        /// The address.
        address: SwarmAddress,
    },
    /// Uses the connection established variant.
    ConnectionEstablished {
        /// The peer ID.
        peer_id: String,
    },
    /// Uses the outgoing connection error variant.
    OutgoingConnectionError {
        /// The peer ID.
        peer_id: Option<String>,
        /// The message.
        message: String,
    },
    /// Uses the incoming connection error variant.
    IncomingConnectionError {
        /// The message.
        message: String,
    },
    /// Uses the other variant.
    Other {
        /// The kind.
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
/// Represents a control announcement.
pub struct ControlAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The certificate.
    pub certificate: ControlCertificate<ExperimentControlEnvelope>,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a head announcement.
pub struct HeadAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The provider peer ID.
    pub provider_peer_id: Option<PeerId>,
    /// The head.
    pub head: HeadDescriptor,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a lease announcement.
pub struct LeaseAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The lease.
    pub lease: AssignmentLease,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a telemetry announcement.
pub struct TelemetryAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The telemetry.
    pub telemetry: TelemetrySummary,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported alert severity values.
pub enum AlertSeverity {
    /// Uses the info variant.
    Info,
    /// Uses the warn variant.
    Warn,
    /// Uses the error variant.
    Error,
    /// Uses the critical variant.
    Critical,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an alert notice.
pub struct AlertNotice {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The peer ID.
    pub peer_id: Option<PeerId>,
    /// The severity.
    pub severity: AlertSeverity,
    /// The code.
    pub code: String,
    /// The message.
    pub message: String,
    /// The emitted at.
    pub emitted_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a provider pointer.
pub struct ProviderPointer {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The addresses.
    pub addresses: Vec<SwarmAddress>,
    /// The advertised at.
    pub advertised_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an artifact provider record.
pub struct ArtifactProviderRecord {
    /// The artifact ID.
    pub artifact_id: ArtifactId,
    /// The head ID.
    pub head_id: Option<HeadId>,
    /// The providers.
    pub providers: Vec<ProviderPointer>,
    /// The advertised at.
    pub advertised_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a micro shard provider record.
pub struct MicroShardProviderRecord {
    /// The dataset view ID.
    pub dataset_view_id: DatasetViewId,
    /// The microshard ID.
    pub microshard_id: MicroShardId,
    /// The providers.
    pub providers: Vec<ProviderPointer>,
    /// The advertised at.
    pub advertised_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an artifact sync request.
pub struct ArtifactSyncRequest {
    /// The requester.
    pub requester: PeerId,
    /// The artifact ID.
    pub artifact_id: ArtifactId,
    /// The wanted chunks.
    pub wanted_chunks: BTreeSet<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an artifact sync response.
pub struct ArtifactSyncResponse {
    /// The provider.
    pub provider: PeerId,
    /// The descriptor.
    pub descriptor: ArtifactDescriptor,
    /// The available chunks.
    pub available_chunks: BTreeSet<ChunkId>,
    /// The direct streams preferred.
    pub direct_streams_preferred: bool,
    /// The generated at.
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a chunk fetch request.
pub struct ChunkFetchRequest {
    /// The requester.
    pub requester: PeerId,
    /// The artifact ID.
    pub artifact_id: ArtifactId,
    /// The chunk ID.
    pub chunk_id: ChunkId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a chunk fetch response.
pub struct ChunkFetchResponse {
    /// The provider.
    pub provider: PeerId,
    /// The artifact ID.
    pub artifact_id: ArtifactId,
    /// The chunk.
    pub chunk: ChunkDescriptor,
    /// The generated at.
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a micro shard fetch request.
pub struct MicroShardFetchRequest {
    /// The requester.
    pub requester: PeerId,
    /// The dataset view ID.
    pub dataset_view_id: DatasetViewId,
    /// The microshard ID.
    pub microshard_id: MicroShardId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a micro shard fetch response.
pub struct MicroShardFetchResponse {
    /// The provider.
    pub provider: PeerId,
    /// The dataset view ID.
    pub dataset_view_id: DatasetViewId,
    /// The microshard ID.
    pub microshard_id: MicroShardId,
    /// The estimated bytes.
    pub estimated_bytes: u64,
    /// The generated at.
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a merge announcement.
pub struct MergeAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The certificate.
    pub certificate: MergeCertificate,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a merge window announcement.
pub struct MergeWindowAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The merge window.
    pub merge_window: MergeWindowState,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a reducer assignment announcement.
pub struct ReducerAssignmentAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The assignment.
    pub assignment: ReducerAssignment,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents an update envelope announcement.
pub struct UpdateEnvelopeAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The update.
    pub update: UpdateAnnounce,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents an aggregate announcement.
pub struct AggregateAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The aggregate.
    pub aggregate: AggregateEnvelope,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a reduction certificate announcement.
pub struct ReductionCertificateAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The certificate.
    pub certificate: ReductionCertificate,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a reducer load announcement.
pub struct ReducerLoadAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The report.
    pub report: ReducerLoadReport,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a peer auth announcement.
pub struct PeerAuthAnnouncement {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The envelope.
    pub envelope: PeerAuthEnvelope,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an experiment directory announcement.
pub struct ExperimentDirectoryAnnouncement {
    /// The network ID.
    pub network_id: NetworkId,
    /// The entries.
    pub entries: Vec<ExperimentDirectoryEntry>,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a metrics update announcement shared over an experiment metrics topic.
pub struct MetricsAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The live event.
    pub event: MetricsLiveEvent,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
/// Captures a snapshot of control plane.
pub struct ControlPlaneSnapshot {
    /// The control announcements.
    pub control_announcements: Vec<ControlAnnouncement>,
    /// The head announcements.
    pub head_announcements: Vec<HeadAnnouncement>,
    /// The lease announcements.
    pub lease_announcements: Vec<LeaseAnnouncement>,
    /// The merge announcements.
    pub merge_announcements: Vec<MergeAnnouncement>,
    /// The merge window announcements.
    pub merge_window_announcements: Vec<MergeWindowAnnouncement>,
    /// The reducer assignment announcements.
    pub reducer_assignment_announcements: Vec<ReducerAssignmentAnnouncement>,
    /// The update announcements.
    pub update_announcements: Vec<UpdateEnvelopeAnnouncement>,
    /// The aggregate announcements.
    pub aggregate_announcements: Vec<AggregateAnnouncement>,
    /// The reduction certificate announcements.
    pub reduction_certificate_announcements: Vec<ReductionCertificateAnnouncement>,
    /// The reducer load announcements.
    pub reducer_load_announcements: Vec<ReducerLoadAnnouncement>,
    /// The auth announcements.
    pub auth_announcements: Vec<PeerAuthAnnouncement>,
    /// The directory announcements.
    pub directory_announcements: Vec<ExperimentDirectoryAnnouncement>,
    /// The metrics announcements.
    pub metrics_announcements: Vec<MetricsAnnouncement>,
}

impl ControlPlaneSnapshot {
    /// Trims live metrics announcements to the bounded recent tail kept in memory.
    pub fn clamp_metrics_announcements(&mut self) {
        cap_metrics_announcements(&mut self.metrics_announcements);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an artifact chunk payload.
pub struct ArtifactChunkPayload {
    /// The artifact ID.
    pub artifact_id: ArtifactId,
    /// The chunk.
    pub chunk: ChunkDescriptor,
    /// The bytes.
    pub bytes: Vec<u8>,
    /// The generated at.
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported control plane request values.
pub enum ControlPlaneRequest {
    /// Uses the snapshot variant.
    Snapshot,
    /// Uses the artifact manifest variant.
    ArtifactManifest {
        /// The artifact ID.
        artifact_id: ArtifactId,
    },
    /// Uses the artifact chunk variant.
    ArtifactChunk {
        /// The artifact ID.
        artifact_id: ArtifactId,
        /// The chunk ID.
        chunk_id: ChunkId,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Enumerates the supported control plane response values.
pub enum ControlPlaneResponse {
    /// Uses the snapshot variant.
    Snapshot(ControlPlaneSnapshot),
    /// Uses the artifact manifest variant.
    ArtifactManifest(Option<ArtifactDescriptor>),
    /// Uses the artifact chunk variant.
    ArtifactChunk(Option<ArtifactChunkPayload>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Enumerates the supported pubsub payload values.
pub enum PubsubPayload {
    /// Uses the control variant.
    Control(ControlAnnouncement),
    /// Uses the head variant.
    Head(HeadAnnouncement),
    /// Uses the lease variant.
    Lease(LeaseAnnouncement),
    /// Uses the merge variant.
    Merge(MergeAnnouncement),
    /// Uses the merge window variant.
    MergeWindow(MergeWindowAnnouncement),
    /// Uses the reducer assignment variant.
    ReducerAssignment(ReducerAssignmentAnnouncement),
    /// Uses the update variant.
    Update(UpdateEnvelopeAnnouncement),
    /// Uses the aggregate variant.
    Aggregate(AggregateAnnouncement),
    /// Uses the reduction certificate variant.
    ReductionCertificate(ReductionCertificateAnnouncement),
    /// Uses the reducer load variant.
    ReducerLoad(ReducerLoadAnnouncement),
    /// Uses the auth variant.
    Auth(PeerAuthAnnouncement),
    /// Uses the directory variant.
    Directory(ExperimentDirectoryAnnouncement),
    /// Uses the metrics variant.
    Metrics(MetricsAnnouncement),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Wraps the pubsub with transport metadata.
pub struct PubsubEnvelope {
    /// The topic path.
    pub topic_path: String,
    /// The payload.
    pub payload: PubsubPayload,
    /// The published at.
    pub published_at: DateTime<Utc>,
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "NativeControlPlaneBehaviourEvent")]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeControlPlaneBehaviour {
    pub(crate) request_response:
        request_response::json::Behaviour<ControlPlaneRequest, ControlPlaneResponse>,
    pub(crate) gossipsub: gossipsub::Behaviour,
    pub(crate) identify: identify::Behaviour,
    pub(crate) mdns: mdns::tokio::Behaviour,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) enum NativeControlPlaneBehaviourEvent {
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
    /// Performs the from operation.
    fn from(value: identify::Event) -> Self {
        Self::Identify(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<mdns::Event> for NativeControlPlaneBehaviourEvent {
    /// Performs the from operation.
    fn from(value: mdns::Event) -> Self {
        Self::Mdns(value)
    }
}

pub(crate) fn push_unique<T: PartialEq>(values: &mut Vec<T>, value: T) {
    if !values.contains(&value) {
        values.push(value);
    }
}

pub(crate) fn cap_tail<T>(values: &mut Vec<T>, max_len: usize) {
    if values.len() > max_len {
        let overflow = values.len() - max_len;
        values.drain(..overflow);
    }
}

const MAX_METRICS_ANNOUNCEMENTS: usize = 64;

pub(crate) fn cap_metrics_announcements(values: &mut Vec<MetricsAnnouncement>) {
    cap_tail(values, MAX_METRICS_ANNOUNCEMENTS);
}

pub(crate) fn push_metrics_announcement(
    values: &mut Vec<MetricsAnnouncement>,
    value: MetricsAnnouncement,
) {
    push_unique(values, value);
    cap_metrics_announcements(values);
}

pub(crate) fn apply_pubsub_payload(snapshot: &mut ControlPlaneSnapshot, payload: PubsubPayload) {
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
        PubsubPayload::Metrics(announcement) => {
            push_metrics_announcement(&mut snapshot.metrics_announcements, announcement);
        }
    }
}

pub(crate) fn pubsub_payload_kind(payload: &PubsubPayload) -> &'static str {
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
        PubsubPayload::Metrics(_) => "metrics",
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Enumerates high-level control-plane request, pubsub, and transport events.
pub enum LiveControlPlaneEvent {
    /// Uses the new listen addr variant.
    NewListenAddr {
        /// The address.
        address: SwarmAddress,
    },
    /// Uses the connection established variant.
    ConnectionEstablished {
        /// The peer ID.
        peer_id: String,
    },
    /// Uses the snapshot requested variant.
    SnapshotRequested {
        /// The peer ID.
        peer_id: String,
    },
    /// Uses the snapshot received variant.
    SnapshotReceived {
        /// The peer ID.
        peer_id: String,
        /// The snapshot.
        snapshot: ControlPlaneSnapshot,
    },
    /// Uses the artifact manifest requested variant.
    ArtifactManifestRequested {
        /// The peer ID.
        peer_id: String,
        /// The artifact ID.
        artifact_id: ArtifactId,
    },
    /// Uses the artifact manifest received variant.
    ArtifactManifestReceived {
        /// The peer ID.
        peer_id: String,
        /// The descriptor.
        descriptor: Option<ArtifactDescriptor>,
    },
    /// Uses the artifact chunk requested variant.
    ArtifactChunkRequested {
        /// The peer ID.
        peer_id: String,
        /// The artifact ID.
        artifact_id: ArtifactId,
        /// The chunk ID.
        chunk_id: ChunkId,
    },
    /// Uses the artifact chunk received variant.
    ArtifactChunkReceived {
        /// The peer ID.
        peer_id: String,
        /// The payload.
        payload: Option<ArtifactChunkPayload>,
    },
    /// Uses the snapshot response sent variant.
    SnapshotResponseSent {
        /// The peer ID.
        peer_id: String,
    },
    /// Uses the topic subscribed variant.
    TopicSubscribed {
        /// The topic.
        topic: String,
    },
    /// Uses the pubsub message variant.
    PubsubMessage {
        /// The peer ID.
        peer_id: String,
        /// The topic.
        topic: String,
        /// The kind.
        kind: String,
    },
    /// Uses the peers discovered variant.
    PeersDiscovered {
        /// The peers.
        peers: Vec<(String, SwarmAddress)>,
    },
    /// Uses the peers expired variant.
    PeersExpired {
        /// The peers.
        peers: Vec<(String, SwarmAddress)>,
    },
    /// Uses the peer identified variant.
    PeerIdentified {
        /// The peer ID.
        peer_id: String,
        /// The listen addresses.
        listen_addresses: Vec<SwarmAddress>,
        /// The protocols.
        protocols: Vec<String>,
    },
    /// Uses the request failure variant.
    RequestFailure {
        /// The peer ID.
        peer_id: String,
        /// The message.
        message: String,
    },
    /// Uses the inbound failure variant.
    InboundFailure {
        /// The peer ID.
        peer_id: String,
        /// The message.
        message: String,
    },
    /// Uses the response send failure variant.
    ResponseSendFailure {
        /// The peer ID.
        peer_id: String,
        /// The message.
        message: String,
    },
    /// Uses the outgoing connection error variant.
    OutgoingConnectionError {
        /// The peer ID.
        peer_id: Option<String>,
        /// The message.
        message: String,
    },
    /// Uses the incoming connection error variant.
    IncomingConnectionError {
        /// The message.
        message: String,
    },
    /// Uses the other variant.
    Other {
        /// The kind.
        kind: String,
    },
}

impl From<SwarmEvent<request_response::Event<ControlPlaneRequest, ControlPlaneResponse>>>
    for LiveControlPlaneEvent
{
    /// Performs the from operation.
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
