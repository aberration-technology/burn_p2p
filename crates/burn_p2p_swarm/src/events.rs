use super::*;
use burn_p2p_core::FleetPlacementSnapshot;

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
    /// Uses the connection closed variant.
    ConnectionClosed {
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
            SwarmEvent::ConnectionClosed { peer_id, .. } => Self::ConnectionClosed {
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
/// Represents an experiment lifecycle announcement.
pub struct ExperimentLifecycleAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The certificate.
    pub certificate: ControlCertificate<ExperimentLifecycleEnvelope>,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a fleet schedule epoch announcement.
pub struct FleetScheduleAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The certificate.
    pub certificate: ControlCertificate<FleetScheduleEpochEnvelope>,
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
/// Represents an aggregate proposal announcement.
pub struct AggregateProposalAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The proposed aggregate.
    pub proposal: AggregateEnvelope,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a compact validator quorum announcement.
pub struct ValidationQuorumAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The certificate.
    pub certificate: ValidationQuorumCertificate,
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
/// Represents a peer directory announcement shared on the main control topic.
pub struct PeerDirectoryAnnouncement {
    /// The network ID.
    pub network_id: NetworkId,
    /// The peer ID being advertised.
    pub peer_id: PeerId,
    /// The listen addresses currently advertised for that peer.
    pub addresses: Vec<SwarmAddress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The roles currently advertised for that peer.
    pub advertised_roles: Option<PeerRoleSet>,
    /// The announced at.
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a metrics update announcement shared over an experiment metrics topic.
pub struct MetricsAnnouncement {
    /// The overlay.
    pub overlay: OverlayTopic,
    /// The live event.
    pub event: MetricsLiveEvent,
    #[serde(default)]
    /// Recent peer-window hints that can inform placement decisions.
    pub peer_window_hints: Vec<PeerWindowPlacementHint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Signed long-horizon fleet placement snapshot emitted alongside the live event.
    pub placement_snapshot: Option<FleetPlacementSnapshot>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
/// Captures a snapshot of control plane.
pub struct ControlPlaneSnapshot {
    /// The control announcements.
    pub control_announcements: Vec<ControlAnnouncement>,
    /// The lifecycle announcements.
    pub lifecycle_announcements: Vec<ExperimentLifecycleAnnouncement>,
    /// The fleet schedule announcements.
    pub schedule_announcements: Vec<FleetScheduleAnnouncement>,
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
    /// The aggregate proposal announcements.
    pub aggregate_proposal_announcements: Vec<AggregateProposalAnnouncement>,
    /// The reduction certificate announcements.
    pub reduction_certificate_announcements: Vec<ReductionCertificateAnnouncement>,
    /// The validation quorum announcements.
    pub validation_quorum_announcements: Vec<ValidationQuorumAnnouncement>,
    /// The reducer load announcements.
    pub reducer_load_announcements: Vec<ReducerLoadAnnouncement>,
    /// The auth announcements.
    pub auth_announcements: Vec<PeerAuthAnnouncement>,
    /// The directory announcements.
    pub directory_announcements: Vec<ExperimentDirectoryAnnouncement>,
    /// The peer directory announcements.
    pub peer_directory_announcements: Vec<PeerDirectoryAnnouncement>,
    /// The metrics announcements.
    pub metrics_announcements: Vec<MetricsAnnouncement>,
}

impl ControlPlaneSnapshot {
    /// Trims live metrics announcements to the bounded recent tail kept in memory.
    pub fn clamp_metrics_announcements(&mut self) {
        cap_metrics_announcements(&mut self.metrics_announcements);
    }

    /// Trims bounded control-plane announcement histories kept in memory.
    pub fn clamp_announcement_histories(&mut self) {
        cap_control_announcements(&mut self.control_announcements);
        cap_lifecycle_announcements(&mut self.lifecycle_announcements);
        cap_schedule_announcements(&mut self.schedule_announcements);
        cap_head_announcements(&mut self.head_announcements);
        cap_lease_announcements(&mut self.lease_announcements);
        cap_merge_window_announcements(&mut self.merge_window_announcements);
        cap_reducer_assignment_announcements(&mut self.reducer_assignment_announcements);
        cap_update_announcements(&mut self.update_announcements);
        cap_reducer_load_announcements(&mut self.reducer_load_announcements);
        cap_auth_announcements(&mut self.auth_announcements);
        cap_directory_announcements(&mut self.directory_announcements);
        cap_peer_directory_announcements(&mut self.peer_directory_announcements);
        cap_aggregate_proposal_announcements(&mut self.aggregate_proposal_announcements);
        cap_reduction_certificate_announcements(&mut self.reduction_certificate_announcements);
        cap_validation_quorum_announcements(&mut self.validation_quorum_announcements);
        cap_merge_announcements(&mut self.merge_announcements);
        self.clamp_metrics_announcements();
    }

    pub(crate) fn insert_control_announcement(&mut self, announcement: ControlAnnouncement) {
        push_unique(&mut self.control_announcements, announcement);
        cap_control_announcements(&mut self.control_announcements);
    }

    pub(crate) fn insert_lifecycle_announcement(
        &mut self,
        announcement: ExperimentLifecycleAnnouncement,
    ) {
        push_unique(&mut self.lifecycle_announcements, announcement);
        cap_lifecycle_announcements(&mut self.lifecycle_announcements);
    }

    pub(crate) fn insert_schedule_announcement(&mut self, announcement: FleetScheduleAnnouncement) {
        push_unique(&mut self.schedule_announcements, announcement);
        cap_schedule_announcements(&mut self.schedule_announcements);
    }

    pub(crate) fn insert_head_announcement(&mut self, announcement: HeadAnnouncement) {
        push_unique(&mut self.head_announcements, announcement);
        cap_head_announcements(&mut self.head_announcements);
    }

    pub(crate) fn insert_lease_announcement(&mut self, announcement: LeaseAnnouncement) {
        push_unique(&mut self.lease_announcements, announcement);
        cap_lease_announcements(&mut self.lease_announcements);
    }

    pub(crate) fn insert_merge_window_announcement(
        &mut self,
        announcement: MergeWindowAnnouncement,
    ) {
        push_unique(&mut self.merge_window_announcements, announcement);
        cap_merge_window_announcements(&mut self.merge_window_announcements);
    }

    pub(crate) fn insert_reducer_assignment_announcement(
        &mut self,
        announcement: ReducerAssignmentAnnouncement,
    ) {
        push_unique(&mut self.reducer_assignment_announcements, announcement);
        cap_reducer_assignment_announcements(&mut self.reducer_assignment_announcements);
    }

    pub(crate) fn insert_update_announcement(&mut self, announcement: UpdateEnvelopeAnnouncement) {
        push_unique(&mut self.update_announcements, announcement);
        cap_update_announcements(&mut self.update_announcements);
    }

    pub(crate) fn insert_reducer_load_announcement(
        &mut self,
        announcement: ReducerLoadAnnouncement,
    ) {
        push_unique(&mut self.reducer_load_announcements, announcement);
        cap_reducer_load_announcements(&mut self.reducer_load_announcements);
    }

    pub(crate) fn insert_auth_announcement(&mut self, announcement: PeerAuthAnnouncement) {
        push_unique(&mut self.auth_announcements, announcement);
        cap_auth_announcements(&mut self.auth_announcements);
    }

    pub(crate) fn insert_directory_announcement(
        &mut self,
        announcement: ExperimentDirectoryAnnouncement,
    ) {
        push_unique(&mut self.directory_announcements, announcement);
        cap_directory_announcements(&mut self.directory_announcements);
    }

    pub(crate) fn insert_peer_directory_announcement(
        &mut self,
        mut announcement: PeerDirectoryAnnouncement,
    ) {
        canonicalize_peer_directory_announcement(&mut announcement);
        match self
            .peer_directory_announcements
            .iter()
            .position(|existing| {
                existing.network_id == announcement.network_id
                    && existing.peer_id == announcement.peer_id
            }) {
            Some(position) => coalesce_peer_directory_announcements(
                &mut self.peer_directory_announcements[position],
                announcement,
            ),
            None => self.peer_directory_announcements.push(announcement),
        }
        cap_peer_directory_announcements(&mut self.peer_directory_announcements);
    }

    pub(crate) fn insert_metrics_announcement(&mut self, announcement: MetricsAnnouncement) {
        push_metrics_announcement(&mut self.metrics_announcements, announcement);
    }

    pub fn merge_from_semantic(&mut self, remote: &ControlPlaneSnapshot) {
        for announcement in &remote.control_announcements {
            self.insert_control_announcement(announcement.clone());
        }
        for announcement in &remote.lifecycle_announcements {
            self.insert_lifecycle_announcement(announcement.clone());
        }
        for announcement in &remote.schedule_announcements {
            self.insert_schedule_announcement(announcement.clone());
        }
        for announcement in &remote.head_announcements {
            self.insert_head_announcement(announcement.clone());
        }
        for announcement in &remote.lease_announcements {
            self.insert_lease_announcement(announcement.clone());
        }
        let mut index = ControlPlaneHotIndex::default();
        rebuild_hot_index(self, &mut index);
        for announcement in &remote.merge_announcements {
            insert_merge_announcement_with_index(self, &mut index, announcement.clone());
        }
        for announcement in &remote.merge_window_announcements {
            self.insert_merge_window_announcement(announcement.clone());
        }
        for announcement in &remote.reducer_assignment_announcements {
            self.insert_reducer_assignment_announcement(announcement.clone());
        }
        for announcement in &remote.update_announcements {
            self.insert_update_announcement(announcement.clone());
        }
        for announcement in &remote.aggregate_proposal_announcements {
            insert_aggregate_proposal_announcement_with_index(
                self,
                &mut index,
                announcement.clone(),
            );
        }
        for announcement in &remote.reduction_certificate_announcements {
            insert_reduction_certificate_announcement_with_index(
                self,
                &mut index,
                announcement.clone(),
            );
        }
        for announcement in &remote.validation_quorum_announcements {
            insert_validation_quorum_announcement_with_index(
                self,
                &mut index,
                announcement.clone(),
            );
        }
        for announcement in &remote.reducer_load_announcements {
            self.insert_reducer_load_announcement(announcement.clone());
        }
        for announcement in &remote.auth_announcements {
            self.insert_auth_announcement(announcement.clone());
        }
        for announcement in &remote.directory_announcements {
            self.insert_directory_announcement(announcement.clone());
        }
        for announcement in &remote.peer_directory_announcements {
            self.insert_peer_directory_announcement(announcement.clone());
        }
        for announcement in &remote.metrics_announcements {
            self.insert_metrics_announcement(announcement.clone());
        }
        self.clamp_announcement_histories();
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
    /// Uses the lifecycle variant.
    Lifecycle(Box<ExperimentLifecycleAnnouncement>),
    /// Uses the schedule variant.
    Schedule(Box<FleetScheduleAnnouncement>),
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
    /// Uses the aggregate proposal variant.
    AggregateProposal(AggregateProposalAnnouncement),
    /// Uses the reduction certificate variant.
    ReductionCertificate(ReductionCertificateAnnouncement),
    /// Uses the validation quorum variant.
    ValidationQuorum(ValidationQuorumAnnouncement),
    /// Uses the reducer load variant.
    ReducerLoad(ReducerLoadAnnouncement),
    /// Uses the auth variant.
    Auth(Box<PeerAuthAnnouncement>),
    /// Uses the directory variant.
    Directory(ExperimentDirectoryAnnouncement),
    /// Uses the peer directory variant.
    PeerDirectory(PeerDirectoryAnnouncement),
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
        request_response::cbor::Behaviour<ControlPlaneRequest, ControlPlaneResponse>,
    pub(crate) gossipsub: gossipsub::Behaviour,
    pub(crate) identify: identify::Behaviour,
    pub(crate) kademlia:
        libp2p::swarm::behaviour::toggle::Toggle<kad::Behaviour<kad::store::MemoryStore>>,
    pub(crate) rendezvous_client:
        libp2p::swarm::behaviour::toggle::Toggle<rendezvous::client::Behaviour>,
    pub(crate) rendezvous_server:
        libp2p::swarm::behaviour::toggle::Toggle<rendezvous::server::Behaviour>,
    pub(crate) relay_client: relay::client::Behaviour,
    pub(crate) relay_server: libp2p::swarm::behaviour::toggle::Toggle<relay::Behaviour>,
    pub(crate) dcutr: libp2p::swarm::behaviour::toggle::Toggle<dcutr::Behaviour>,
    pub(crate) autonat: libp2p::swarm::behaviour::toggle::Toggle<autonat::Behaviour>,
    pub(crate) ping: ping::Behaviour,
    pub(crate) connection_limits: connection_limits::Behaviour,
    pub(crate) mdns: libp2p::swarm::behaviour::toggle::Toggle<mdns::tokio::Behaviour>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) enum NativeControlPlaneBehaviourEvent {
    RequestResponse(Box<request_response::Event<ControlPlaneRequest, ControlPlaneResponse>>),
    Gossipsub(Box<gossipsub::Event>),
    Identify(Box<identify::Event>),
    Kademlia(Box<kad::Event>),
    RendezvousClient(Box<rendezvous::client::Event>),
    RendezvousServer(Box<rendezvous::server::Event>),
    RelayClient(Box<relay::client::Event>),
    RelayServer(Box<relay::Event>),
    Dcutr(Box<dcutr::Event>),
    Autonat(Box<autonat::Event>),
    Ping(Box<ping::Event>),
    Mdns(mdns::Event),
}

#[cfg(not(target_arch = "wasm32"))]
impl From<Infallible> for NativeControlPlaneBehaviourEvent {
    fn from(value: Infallible) -> Self {
        match value {}
    }
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
impl From<kad::Event> for NativeControlPlaneBehaviourEvent {
    fn from(value: kad::Event) -> Self {
        Self::Kademlia(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<rendezvous::client::Event> for NativeControlPlaneBehaviourEvent {
    fn from(value: rendezvous::client::Event) -> Self {
        Self::RendezvousClient(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<rendezvous::server::Event> for NativeControlPlaneBehaviourEvent {
    fn from(value: rendezvous::server::Event) -> Self {
        Self::RendezvousServer(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<relay::client::Event> for NativeControlPlaneBehaviourEvent {
    fn from(value: relay::client::Event) -> Self {
        Self::RelayClient(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<relay::Event> for NativeControlPlaneBehaviourEvent {
    fn from(value: relay::Event) -> Self {
        Self::RelayServer(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<dcutr::Event> for NativeControlPlaneBehaviourEvent {
    fn from(value: dcutr::Event) -> Self {
        Self::Dcutr(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<autonat::Event> for NativeControlPlaneBehaviourEvent {
    fn from(value: autonat::Event) -> Self {
        Self::Autonat(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<ping::Event> for NativeControlPlaneBehaviourEvent {
    fn from(value: ping::Event) -> Self {
        Self::Ping(Box::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<mdns::Event> for NativeControlPlaneBehaviourEvent {
    /// Performs the from operation.
    fn from(value: mdns::Event) -> Self {
        Self::Mdns(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct AggregateProposalAnnouncementKey {
    overlay_path: String,
    aggregate_id: ContentId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReductionCertificateAnnouncementKey {
    overlay_path: String,
    aggregate_id: ContentId,
    promoter_peer_id: PeerId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ValidationQuorumAnnouncementKey {
    overlay_path: String,
    aggregate_id: ContentId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MergeAnnouncementKey {
    overlay_path: String,
    merged_head_id: HeadId,
    promoter_peer_id: PeerId,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ControlPlaneHotIndex {
    aggregate_proposal_announcements: BTreeMap<AggregateProposalAnnouncementKey, usize>,
    reduction_certificate_announcements: BTreeMap<ReductionCertificateAnnouncementKey, usize>,
    validation_quorum_announcements: BTreeMap<ValidationQuorumAnnouncementKey, usize>,
    merge_announcements: BTreeMap<MergeAnnouncementKey, usize>,
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
const MAX_CONTROL_ANNOUNCEMENTS: usize = 64;
const MAX_LIFECYCLE_ANNOUNCEMENTS: usize = 64;
const MAX_SCHEDULE_ANNOUNCEMENTS: usize = 64;
const MAX_HEAD_ANNOUNCEMENTS: usize = 256;
const MAX_LEASE_ANNOUNCEMENTS: usize = 128;
const MAX_MERGE_WINDOW_ANNOUNCEMENTS: usize = 128;
const MAX_REDUCER_ASSIGNMENT_ANNOUNCEMENTS: usize = 128;
const MAX_UPDATE_ANNOUNCEMENTS: usize = 256;
const MAX_REDUCER_LOAD_ANNOUNCEMENTS: usize = 128;
const MAX_AUTH_ANNOUNCEMENTS: usize = 128;
const MAX_DIRECTORY_ANNOUNCEMENTS: usize = 16;
const MAX_PEER_DIRECTORY_ANNOUNCEMENTS: usize = 256;
const MAX_AGGREGATE_PROPOSAL_ANNOUNCEMENTS: usize = 256;
const MAX_REDUCTION_CERTIFICATE_ANNOUNCEMENTS: usize = 256;
const MAX_VALIDATION_QUORUM_ANNOUNCEMENTS: usize = 128;
const MAX_MERGE_ANNOUNCEMENTS: usize = 256;
const MAX_DISTINCT_WINDOWS_PER_OVERLAY: usize = 8;

pub(crate) fn cap_metrics_announcements(values: &mut Vec<MetricsAnnouncement>) {
    cap_tail(values, MAX_METRICS_ANNOUNCEMENTS);
}

fn cap_control_announcements(values: &mut Vec<ControlAnnouncement>) {
    values.sort_by_key(|announcement| announcement.announced_at);
    cap_tail(values, MAX_CONTROL_ANNOUNCEMENTS);
}

fn cap_lifecycle_announcements(values: &mut Vec<ExperimentLifecycleAnnouncement>) {
    values.sort_by(|left, right| {
        left.certificate
            .activation
            .activation_window
            .cmp(&right.certificate.activation.activation_window)
            .then(
                left.certificate
                    .body
                    .payload
                    .payload
                    .plan
                    .plan_epoch
                    .cmp(&right.certificate.body.payload.payload.plan.plan_epoch),
            )
            .then(left.announced_at.cmp(&right.announced_at))
    });
    cap_tail(values, MAX_LIFECYCLE_ANNOUNCEMENTS);
}

fn cap_schedule_announcements(values: &mut Vec<FleetScheduleAnnouncement>) {
    values.sort_by(|left, right| {
        left.certificate
            .activation
            .activation_window
            .cmp(&right.certificate.activation.activation_window)
            .then(
                left.certificate
                    .body
                    .payload
                    .payload
                    .epoch
                    .plan_epoch
                    .cmp(&right.certificate.body.payload.payload.epoch.plan_epoch),
            )
            .then(left.announced_at.cmp(&right.announced_at))
    });
    cap_tail(values, MAX_SCHEDULE_ANNOUNCEMENTS);
}

fn cap_head_announcements(values: &mut Vec<HeadAnnouncement>) {
    values.sort_by(|left, right| {
        left.head
            .global_step
            .cmp(&right.head.global_step)
            .then(left.announced_at.cmp(&right.announced_at))
    });
    cap_tail(values, MAX_HEAD_ANNOUNCEMENTS);
}

fn cap_lease_announcements(values: &mut Vec<LeaseAnnouncement>) {
    values.sort_by(|left, right| {
        left.lease
            .window_id
            .cmp(&right.lease.window_id)
            .then(left.announced_at.cmp(&right.announced_at))
    });
    cap_tail(values, MAX_LEASE_ANNOUNCEMENTS);
}

fn cap_merge_window_announcements(values: &mut Vec<MergeWindowAnnouncement>) {
    values.sort_by(|left, right| {
        left.merge_window
            .window_id
            .cmp(&right.merge_window.window_id)
            .then(left.announced_at.cmp(&right.announced_at))
    });
    cap_tail(values, MAX_MERGE_WINDOW_ANNOUNCEMENTS);
}

fn cap_reducer_assignment_announcements(values: &mut Vec<ReducerAssignmentAnnouncement>) {
    values.sort_by(|left, right| {
        left.assignment
            .window_id
            .cmp(&right.assignment.window_id)
            .then(left.announced_at.cmp(&right.announced_at))
    });
    cap_tail(values, MAX_REDUCER_ASSIGNMENT_ANNOUNCEMENTS);
}

fn cap_update_announcements(values: &mut Vec<UpdateEnvelopeAnnouncement>) {
    values.sort_by(|left, right| {
        left.update
            .window_id
            .cmp(&right.update.window_id)
            .then(left.update.announced_at.cmp(&right.update.announced_at))
    });
    cap_tail(values, MAX_UPDATE_ANNOUNCEMENTS);
}

fn cap_reducer_load_announcements(values: &mut Vec<ReducerLoadAnnouncement>) {
    values.sort_by(|left, right| {
        left.report
            .reported_at
            .cmp(&right.report.reported_at)
            .then(left.report.peer_id.cmp(&right.report.peer_id))
    });
    cap_tail(values, MAX_REDUCER_LOAD_ANNOUNCEMENTS);
}

fn cap_auth_announcements(values: &mut Vec<PeerAuthAnnouncement>) {
    values.sort_by_key(|announcement| announcement.announced_at);
    cap_tail(values, MAX_AUTH_ANNOUNCEMENTS);
}

fn sort_and_dedup<T: Ord>(values: &mut Vec<T>) {
    values.sort();
    values.dedup();
}

fn merge_peer_role_sets(existing: &mut Option<PeerRoleSet>, incoming: Option<PeerRoleSet>) {
    match (existing.as_mut(), incoming) {
        (Some(existing), Some(incoming)) => existing.roles.extend(incoming.roles),
        (None, Some(incoming)) => *existing = Some(incoming),
        _ => {}
    }
}

fn canonicalize_peer_directory_announcement(announcement: &mut PeerDirectoryAnnouncement) {
    announcement.addresses.sort_by(|left, right| {
        left.is_relay_circuit()
            .cmp(&right.is_relay_circuit())
            .then_with(|| left.cmp(right))
    });
    announcement.addresses.dedup();
}

fn canonicalize_aggregate_proposal_announcement(announcement: &mut AggregateProposalAnnouncement) {
    sort_and_dedup(&mut announcement.proposal.contributor_peers);
    sort_and_dedup(&mut announcement.proposal.child_aggregate_ids);
    sort_and_dedup(&mut announcement.proposal.providers);
}

fn canonicalize_reduction_certificate_announcement(
    announcement: &mut ReductionCertificateAnnouncement,
) {
    sort_and_dedup(&mut announcement.certificate.cross_checked_reducers);
}

fn canonicalize_validation_quorum_announcement(announcement: &mut ValidationQuorumAnnouncement) {
    sort_and_dedup(&mut announcement.certificate.attesting_validators);
    sort_and_dedup(&mut announcement.certificate.reduction_ids);
}

fn canonicalize_merge_announcement(announcement: &mut MergeAnnouncement) {
    sort_and_dedup(&mut announcement.certificate.contribution_receipts);
}

fn coalesce_peer_directory_announcements(
    existing: &mut PeerDirectoryAnnouncement,
    mut incoming: PeerDirectoryAnnouncement,
) {
    canonicalize_peer_directory_announcement(&mut incoming);
    existing.announced_at = existing.announced_at.max(incoming.announced_at);
    existing.addresses.extend(incoming.addresses);
    merge_peer_role_sets(&mut existing.advertised_roles, incoming.advertised_roles);
    canonicalize_peer_directory_announcement(existing);
}

fn aggregate_proposal_announcement_key(
    announcement: &AggregateProposalAnnouncement,
) -> AggregateProposalAnnouncementKey {
    AggregateProposalAnnouncementKey {
        overlay_path: announcement.overlay.path.clone(),
        aggregate_id: announcement.proposal.aggregate_id.clone(),
    }
}

fn reduction_certificate_announcement_key(
    announcement: &ReductionCertificateAnnouncement,
) -> ReductionCertificateAnnouncementKey {
    ReductionCertificateAnnouncementKey {
        overlay_path: announcement.overlay.path.clone(),
        aggregate_id: announcement.certificate.aggregate_id.clone(),
        promoter_peer_id: announcement.certificate.promoter_peer_id.clone(),
    }
}

fn validation_quorum_announcement_key(
    announcement: &ValidationQuorumAnnouncement,
) -> ValidationQuorumAnnouncementKey {
    ValidationQuorumAnnouncementKey {
        overlay_path: announcement.overlay.path.clone(),
        aggregate_id: announcement.certificate.aggregate_id.clone(),
    }
}

fn merge_announcement_key(announcement: &MergeAnnouncement) -> MergeAnnouncementKey {
    MergeAnnouncementKey {
        overlay_path: announcement.overlay.path.clone(),
        merged_head_id: announcement.certificate.merged_head_id.clone(),
        promoter_peer_id: announcement.certificate.promoter_peer_id.clone(),
    }
}

fn aggregate_proposal_semantic_fingerprint(announcement: &AggregateProposalAnnouncement) -> String {
    let mut contributor_peers = announcement.proposal.contributor_peers.clone();
    let mut child_aggregate_ids = announcement.proposal.child_aggregate_ids.clone();
    sort_and_dedup(&mut contributor_peers);
    sort_and_dedup(&mut child_aggregate_ids);
    let stats_fingerprint = format!(
        "{}:{}:{}:{}:{}",
        announcement.proposal.stats.late_updates,
        announcement.proposal.stats.sum_sample_weight.to_bits(),
        announcement.proposal.stats.sum_quality_weight.to_bits(),
        announcement
            .proposal
            .stats
            .sum_weighted_delta_norm
            .to_bits(),
        announcement.proposal.stats.max_update_norm.to_bits()
    );
    format!(
        "{}|{}|{}|{}|{}|{}|{:?}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        announcement.overlay.path,
        announcement.proposal.aggregate_id,
        announcement.proposal.study_id,
        announcement.proposal.experiment_id,
        announcement.proposal.revision_id,
        announcement.proposal.window_id.0,
        announcement.proposal.tier,
        announcement.proposal.base_head_id,
        announcement.proposal.aggregate_artifact_id,
        announcement.proposal.reducer_peer_id,
        contributor_peers
            .iter()
            .map(|peer| peer.as_str())
            .collect::<Vec<_>>()
            .join(","),
        child_aggregate_ids
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>()
            .join(","),
        announcement.proposal.stats.accepted_updates,
        announcement.proposal.stats.duplicate_updates,
        announcement.proposal.stats.dropped_updates,
        stats_fingerprint,
    )
}

fn reduction_certificate_semantic_fingerprint(
    announcement: &ReductionCertificateAnnouncement,
) -> String {
    let mut reducers = announcement.certificate.cross_checked_reducers.clone();
    sort_and_dedup(&mut reducers);
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        announcement.overlay.path,
        announcement.certificate.study_id,
        announcement.certificate.experiment_id,
        announcement.certificate.revision_id,
        announcement.certificate.window_id.0,
        announcement.certificate.base_head_id,
        announcement.certificate.aggregate_id,
        reducers
            .iter()
            .map(|peer| peer.as_str())
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn validation_quorum_semantic_fingerprint(announcement: &ValidationQuorumAnnouncement) -> String {
    let mut validators = announcement.certificate.attesting_validators.clone();
    let mut reduction_ids = announcement.certificate.reduction_ids.clone();
    sort_and_dedup(&mut validators);
    sort_and_dedup(&mut reduction_ids);
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        announcement.overlay.path,
        announcement.certificate.study_id,
        announcement.certificate.experiment_id,
        announcement.certificate.revision_id,
        announcement.certificate.window_id.0,
        announcement.certificate.base_head_id,
        announcement.certificate.aggregate_id,
        announcement.certificate.aggregate_artifact_id,
        announcement.certificate.merged_head_id,
        validators
            .iter()
            .map(|peer| peer.as_str())
            .collect::<Vec<_>>()
            .join(","),
        reduction_ids
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>()
            .join(","),
    )
}

fn merge_announcement_semantic_fingerprint(announcement: &MergeAnnouncement) -> String {
    let mut receipts = announcement.certificate.contribution_receipts.clone();
    sort_and_dedup(&mut receipts);
    format!(
        "{}|{}|{}|{}|{}|{}|{:?}|{}",
        announcement.overlay.path,
        announcement.certificate.study_id,
        announcement.certificate.experiment_id,
        announcement.certificate.revision_id,
        announcement.certificate.base_head_id,
        announcement.certificate.merged_head_id,
        announcement.certificate.policy,
        receipts
            .iter()
            .map(|receipt| receipt.as_str())
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn peer_directory_semantic_fingerprint(announcement: &PeerDirectoryAnnouncement) -> String {
    let mut addresses = announcement.addresses.clone();
    sort_and_dedup(&mut addresses);
    let roles = announcement
        .advertised_roles
        .as_ref()
        .map(|roles| {
            roles
                .roles
                .iter()
                .map(|role| format!("{role:?}"))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    format!(
        "{}|{}|{}|{}",
        announcement.network_id,
        announcement.peer_id,
        addresses
            .iter()
            .map(|address| address.as_str())
            .collect::<Vec<_>>()
            .join(","),
        roles,
    )
}

fn hash_message_id(bytes: &[u8]) -> String {
    ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(bytes)).to_string()
}

pub(crate) fn pubsub_semantic_message_id(bytes: &[u8]) -> String {
    let semantic_bytes = match serde_json::from_slice::<PubsubEnvelope>(bytes) {
        Ok(envelope) => match &envelope.payload {
            PubsubPayload::AggregateProposal(announcement) => {
                aggregate_proposal_semantic_fingerprint(announcement).into_bytes()
            }
            PubsubPayload::ReductionCertificate(announcement) => {
                reduction_certificate_semantic_fingerprint(announcement).into_bytes()
            }
            PubsubPayload::ValidationQuorum(announcement) => {
                validation_quorum_semantic_fingerprint(announcement).into_bytes()
            }
            PubsubPayload::Merge(announcement) => {
                merge_announcement_semantic_fingerprint(announcement).into_bytes()
            }
            PubsubPayload::PeerDirectory(announcement) => {
                peer_directory_semantic_fingerprint(announcement).into_bytes()
            }
            _ => burn_p2p_core::deterministic_cbor(&envelope.payload)
                .unwrap_or_else(|_| serde_json::to_vec(&envelope.payload).unwrap_or_default()),
        },
        Err(_) => bytes.to_vec(),
    };
    hash_message_id(&semantic_bytes)
}

fn coalesce_aggregate_proposal_announcements(
    existing: &mut AggregateProposalAnnouncement,
    mut incoming: AggregateProposalAnnouncement,
) {
    canonicalize_aggregate_proposal_announcement(&mut incoming);
    existing.announced_at = existing.announced_at.max(incoming.announced_at);
    existing.proposal.published_at = existing
        .proposal
        .published_at
        .max(incoming.proposal.published_at);
    existing
        .proposal
        .providers
        .extend(incoming.proposal.providers);
    existing
        .proposal
        .contributor_peers
        .extend(incoming.proposal.contributor_peers);
    existing
        .proposal
        .child_aggregate_ids
        .extend(incoming.proposal.child_aggregate_ids);
    canonicalize_aggregate_proposal_announcement(existing);
}

fn aggregate_validator_count(
    values: &[ReductionCertificateAnnouncement],
    overlay_path: &str,
    aggregate_id: &ContentId,
) -> usize {
    values
        .iter()
        .filter(|announcement| {
            announcement.overlay.path == overlay_path
                && announcement.certificate.aggregate_id == *aggregate_id
        })
        .map(|announcement| announcement.certificate.promoter_peer_id.clone())
        .collect::<BTreeSet<_>>()
        .len()
}

fn latest_windows_by_overlay<T, FOverlay, FWindow>(
    values: &[T],
    overlay_of: FOverlay,
    window_of: FWindow,
) -> BTreeMap<String, BTreeSet<u64>>
where
    FOverlay: Fn(&T) -> &str,
    FWindow: Fn(&T) -> u64,
{
    let mut overlay_windows = BTreeMap::<String, BTreeSet<u64>>::new();
    for value in values {
        overlay_windows
            .entry(overlay_of(value).to_owned())
            .or_default()
            .insert(window_of(value));
    }
    overlay_windows
        .into_iter()
        .map(|(overlay, windows)| {
            let retained = windows
                .into_iter()
                .rev()
                .take(MAX_DISTINCT_WINDOWS_PER_OVERLAY)
                .collect::<BTreeSet<_>>();
            (overlay, retained)
        })
        .collect()
}

fn cap_aggregate_proposal_announcements(values: &mut Vec<AggregateProposalAnnouncement>) {
    let retained_windows = latest_windows_by_overlay(
        values,
        |announcement| announcement.overlay.path.as_str(),
        |announcement| announcement.proposal.window_id.0,
    );
    values.retain(|announcement| {
        retained_windows
            .get(&announcement.overlay.path)
            .is_some_and(|windows| windows.contains(&announcement.proposal.window_id.0))
    });
    values.sort_by(|left, right| {
        left.proposal
            .window_id
            .cmp(&right.proposal.window_id)
            .then(left.announced_at.cmp(&right.announced_at))
    });
    cap_tail(values, MAX_AGGREGATE_PROPOSAL_ANNOUNCEMENTS);
}

fn cap_directory_announcements(values: &mut Vec<ExperimentDirectoryAnnouncement>) {
    values.sort_by_key(|announcement| announcement.announced_at);
    cap_tail(values, MAX_DIRECTORY_ANNOUNCEMENTS);
}

fn cap_peer_directory_announcements(values: &mut Vec<PeerDirectoryAnnouncement>) {
    for announcement in values.iter_mut() {
        canonicalize_peer_directory_announcement(announcement);
    }
    values.sort_by(|left, right| {
        left.announced_at
            .cmp(&right.announced_at)
            .then(left.peer_id.cmp(&right.peer_id))
    });
    cap_tail(values, MAX_PEER_DIRECTORY_ANNOUNCEMENTS);
}

fn cap_reduction_certificate_announcements(values: &mut Vec<ReductionCertificateAnnouncement>) {
    let retained_windows = latest_windows_by_overlay(
        values,
        |announcement| announcement.overlay.path.as_str(),
        |announcement| announcement.certificate.window_id.0,
    );
    values.retain(|announcement| {
        retained_windows
            .get(&announcement.overlay.path)
            .is_some_and(|windows| windows.contains(&announcement.certificate.window_id.0))
    });
    values.sort_by(|left, right| {
        left.certificate
            .window_id
            .cmp(&right.certificate.window_id)
            .then(left.certificate.issued_at.cmp(&right.certificate.issued_at))
            .then(left.announced_at.cmp(&right.announced_at))
    });
    cap_tail(values, MAX_REDUCTION_CERTIFICATE_ANNOUNCEMENTS);
}

fn cap_validation_quorum_announcements(values: &mut Vec<ValidationQuorumAnnouncement>) {
    let retained_windows = latest_windows_by_overlay(
        values,
        |announcement| announcement.overlay.path.as_str(),
        |announcement| announcement.certificate.window_id.0,
    );
    values.retain(|announcement| {
        retained_windows
            .get(&announcement.overlay.path)
            .is_some_and(|windows| windows.contains(&announcement.certificate.window_id.0))
    });
    values.sort_by(|left, right| {
        left.certificate
            .window_id
            .cmp(&right.certificate.window_id)
            .then(left.certificate.issued_at.cmp(&right.certificate.issued_at))
            .then(left.announced_at.cmp(&right.announced_at))
    });
    cap_tail(values, MAX_VALIDATION_QUORUM_ANNOUNCEMENTS);
}

fn cap_merge_announcements(values: &mut Vec<MergeAnnouncement>) {
    values.sort_by(|left, right| {
        left.certificate
            .issued_at
            .cmp(&right.certificate.issued_at)
            .then(left.announced_at.cmp(&right.announced_at))
    });
    cap_tail(values, MAX_MERGE_ANNOUNCEMENTS);
}

pub(crate) fn rebuild_hot_index(snapshot: &ControlPlaneSnapshot, index: &mut ControlPlaneHotIndex) {
    index.aggregate_proposal_announcements = snapshot
        .aggregate_proposal_announcements
        .iter()
        .enumerate()
        .map(|(position, announcement)| {
            (aggregate_proposal_announcement_key(announcement), position)
        })
        .collect();
    index.reduction_certificate_announcements = snapshot
        .reduction_certificate_announcements
        .iter()
        .enumerate()
        .map(|(position, announcement)| {
            (
                reduction_certificate_announcement_key(announcement),
                position,
            )
        })
        .collect();
    index.validation_quorum_announcements = snapshot
        .validation_quorum_announcements
        .iter()
        .enumerate()
        .map(|(position, announcement)| {
            (validation_quorum_announcement_key(announcement), position)
        })
        .collect();
    index.merge_announcements = snapshot
        .merge_announcements
        .iter()
        .enumerate()
        .map(|(position, announcement)| (merge_announcement_key(announcement), position))
        .collect();
}

pub(crate) fn insert_aggregate_proposal_announcement_with_index(
    snapshot: &mut ControlPlaneSnapshot,
    index: &mut ControlPlaneHotIndex,
    mut announcement: AggregateProposalAnnouncement,
) {
    canonicalize_aggregate_proposal_announcement(&mut announcement);
    let key = aggregate_proposal_announcement_key(&announcement);
    let fingerprint = aggregate_proposal_semantic_fingerprint(&announcement);
    match index.aggregate_proposal_announcements.get(&key).copied() {
        Some(position) => {
            if aggregate_proposal_semantic_fingerprint(
                &snapshot.aggregate_proposal_announcements[position],
            ) == fingerprint
            {
                coalesce_aggregate_proposal_announcements(
                    &mut snapshot.aggregate_proposal_announcements[position],
                    announcement,
                );
            }
        }
        None => snapshot.aggregate_proposal_announcements.push(announcement),
    }
    cap_aggregate_proposal_announcements(&mut snapshot.aggregate_proposal_announcements);
    rebuild_hot_index(snapshot, index);
}

pub(crate) fn insert_reduction_certificate_announcement_with_index(
    snapshot: &mut ControlPlaneSnapshot,
    index: &mut ControlPlaneHotIndex,
    mut announcement: ReductionCertificateAnnouncement,
) {
    canonicalize_reduction_certificate_announcement(&mut announcement);
    let key = reduction_certificate_announcement_key(&announcement);
    let fingerprint = reduction_certificate_semantic_fingerprint(&announcement);
    match index.reduction_certificate_announcements.get(&key).copied() {
        Some(position) => {
            if reduction_certificate_semantic_fingerprint(
                &snapshot.reduction_certificate_announcements[position],
            ) == fingerprint
            {
                snapshot.reduction_certificate_announcements[position].announced_at = snapshot
                    .reduction_certificate_announcements[position]
                    .announced_at
                    .max(announcement.announced_at);
                snapshot.reduction_certificate_announcements[position]
                    .certificate
                    .issued_at = snapshot.reduction_certificate_announcements[position]
                    .certificate
                    .issued_at
                    .max(announcement.certificate.issued_at);
            }
        }
        None => {
            let promoter_count = aggregate_validator_count(
                &snapshot.reduction_certificate_announcements,
                &announcement.overlay.path,
                &announcement.certificate.aggregate_id,
            );
            if promoter_count < usize::from(announcement.certificate.promotion_quorum.max(1)) {
                snapshot
                    .reduction_certificate_announcements
                    .push(announcement);
            }
        }
    }
    cap_reduction_certificate_announcements(&mut snapshot.reduction_certificate_announcements);
    rebuild_hot_index(snapshot, index);
}

pub(crate) fn insert_validation_quorum_announcement_with_index(
    snapshot: &mut ControlPlaneSnapshot,
    index: &mut ControlPlaneHotIndex,
    mut announcement: ValidationQuorumAnnouncement,
) {
    canonicalize_validation_quorum_announcement(&mut announcement);
    let key = validation_quorum_announcement_key(&announcement);
    let fingerprint = validation_quorum_semantic_fingerprint(&announcement);
    match index.validation_quorum_announcements.get(&key).copied() {
        Some(position) => {
            if validation_quorum_semantic_fingerprint(
                &snapshot.validation_quorum_announcements[position],
            ) == fingerprint
            {
                snapshot.validation_quorum_announcements[position].announced_at = snapshot
                    .validation_quorum_announcements[position]
                    .announced_at
                    .max(announcement.announced_at);
                snapshot.validation_quorum_announcements[position]
                    .certificate
                    .issued_at = snapshot.validation_quorum_announcements[position]
                    .certificate
                    .issued_at
                    .max(announcement.certificate.issued_at);
            }
        }
        None => snapshot.validation_quorum_announcements.push(announcement),
    }
    cap_validation_quorum_announcements(&mut snapshot.validation_quorum_announcements);
    rebuild_hot_index(snapshot, index);
}

pub(crate) fn insert_merge_announcement_with_index(
    snapshot: &mut ControlPlaneSnapshot,
    index: &mut ControlPlaneHotIndex,
    mut announcement: MergeAnnouncement,
) {
    canonicalize_merge_announcement(&mut announcement);
    let key = merge_announcement_key(&announcement);
    let fingerprint = merge_announcement_semantic_fingerprint(&announcement);
    match index.merge_announcements.get(&key).copied() {
        Some(position) => {
            if merge_announcement_semantic_fingerprint(&snapshot.merge_announcements[position])
                == fingerprint
            {
                snapshot.merge_announcements[position].announced_at = snapshot.merge_announcements
                    [position]
                    .announced_at
                    .max(announcement.announced_at);
                snapshot.merge_announcements[position].certificate.issued_at = snapshot
                    .merge_announcements[position]
                    .certificate
                    .issued_at
                    .max(announcement.certificate.issued_at);
            }
        }
        None => snapshot.merge_announcements.push(announcement),
    }
    cap_merge_announcements(&mut snapshot.merge_announcements);
    rebuild_hot_index(snapshot, index);
}

pub(crate) fn push_metrics_announcement(
    values: &mut Vec<MetricsAnnouncement>,
    value: MetricsAnnouncement,
) {
    push_unique(values, value);
    cap_metrics_announcements(values);
}

pub(crate) fn apply_pubsub_payload_with_index(
    snapshot: &mut ControlPlaneSnapshot,
    index: &mut ControlPlaneHotIndex,
    payload: PubsubPayload,
) {
    match payload {
        PubsubPayload::Control(announcement) => snapshot.insert_control_announcement(announcement),
        PubsubPayload::Lifecycle(announcement) => {
            snapshot.insert_lifecycle_announcement(*announcement)
        }
        PubsubPayload::Schedule(announcement) => {
            snapshot.insert_schedule_announcement(*announcement)
        }
        PubsubPayload::Head(announcement) => snapshot.insert_head_announcement(announcement),
        PubsubPayload::Lease(announcement) => snapshot.insert_lease_announcement(announcement),
        PubsubPayload::Merge(announcement) => {
            insert_merge_announcement_with_index(snapshot, index, announcement);
        }
        PubsubPayload::MergeWindow(announcement) => {
            snapshot.insert_merge_window_announcement(announcement);
        }
        PubsubPayload::ReducerAssignment(announcement) => {
            snapshot.insert_reducer_assignment_announcement(announcement);
        }
        PubsubPayload::Update(announcement) => snapshot.insert_update_announcement(announcement),
        PubsubPayload::AggregateProposal(announcement) => {
            insert_aggregate_proposal_announcement_with_index(snapshot, index, announcement);
        }
        PubsubPayload::ReductionCertificate(announcement) => {
            insert_reduction_certificate_announcement_with_index(snapshot, index, announcement);
        }
        PubsubPayload::ValidationQuorum(announcement) => {
            insert_validation_quorum_announcement_with_index(snapshot, index, announcement);
        }
        PubsubPayload::ReducerLoad(announcement) => {
            snapshot.insert_reducer_load_announcement(announcement);
        }
        PubsubPayload::Auth(announcement) => snapshot.insert_auth_announcement(*announcement),
        PubsubPayload::Directory(announcement) => {
            snapshot.insert_directory_announcement(announcement);
        }
        PubsubPayload::PeerDirectory(announcement) => {
            snapshot.insert_peer_directory_announcement(announcement);
        }
        PubsubPayload::Metrics(announcement) => snapshot.insert_metrics_announcement(announcement),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn apply_pubsub_payload(snapshot: &mut ControlPlaneSnapshot, payload: PubsubPayload) {
    let mut index = ControlPlaneHotIndex::default();
    rebuild_hot_index(snapshot, &mut index);
    apply_pubsub_payload_with_index(snapshot, &mut index, payload);
}

pub(crate) fn pubsub_payload_kind(payload: &PubsubPayload) -> &'static str {
    match payload {
        PubsubPayload::Control(_) => "control",
        PubsubPayload::Lifecycle(_) => "lifecycle",
        PubsubPayload::Schedule(_) => "schedule",
        PubsubPayload::Head(_) => "head",
        PubsubPayload::Lease(_) => "lease",
        PubsubPayload::Merge(_) => "merge",
        PubsubPayload::MergeWindow(_) => "merge-window",
        PubsubPayload::ReducerAssignment(_) => "reducer-assignment",
        PubsubPayload::Update(_) => "update",
        PubsubPayload::AggregateProposal(_) => "aggregate-proposal",
        PubsubPayload::ReductionCertificate(_) => "reduction-certificate",
        PubsubPayload::ValidationQuorum(_) => "validation-quorum",
        PubsubPayload::ReducerLoad(_) => "reducer-load",
        PubsubPayload::Auth(_) => "auth",
        PubsubPayload::Directory(_) => "directory",
        PubsubPayload::PeerDirectory(_) => "peer-directory",
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
    /// Uses the reachable address confirmed variant.
    ReachableAddressConfirmed {
        /// The address.
        address: SwarmAddress,
    },
    /// Uses the reachable address expired variant.
    ReachableAddressExpired {
        /// The address.
        address: SwarmAddress,
    },
    /// Uses the connection established variant.
    ConnectionEstablished {
        /// The peer ID.
        peer_id: String,
    },
    /// Uses the connection closed variant.
    ConnectionClosed {
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
        /// The outbound request ID.
        request_id: String,
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
        /// The outbound request ID.
        request_id: String,
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
        /// The outbound request ID.
        request_id: String,
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
    /// Uses the peer-directory record received variant.
    PeerDirectoryRecordReceived {
        /// The announcement recovered from the DHT.
        announcement: PeerDirectoryAnnouncement,
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
    /// Uses the relay reservation accepted variant.
    RelayReservationAccepted {
        /// The relay peer ID.
        relay_peer_id: String,
    },
    /// Uses the direct connection upgrade succeeded variant.
    DirectConnectionUpgradeSucceeded {
        /// The peer ID.
        peer_id: String,
    },
    /// Uses the direct connection upgrade failed variant.
    DirectConnectionUpgradeFailed {
        /// The peer ID.
        peer_id: String,
        /// The message.
        message: String,
    },
    /// Uses the request failure variant.
    RequestFailure {
        /// The peer ID.
        peer_id: String,
        /// The outbound request ID, when the failure originated from a request-response exchange.
        request_id: Option<String>,
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
