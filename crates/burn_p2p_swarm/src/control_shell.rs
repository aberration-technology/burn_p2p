use super::*;

pub enum ControlPlaneShell {
    /// Uses the memory variant.
    Memory(Box<MemoryControlPlaneShell>),
    /// Uses the native variant.
    Native(Box<NativeControlPlaneShell>),
}

impl ControlPlaneShell {
    /// Creates a new value.
    pub fn new(
        control_protocol: ProtocolId,
        keypair: Keypair,
        addresses: impl IntoIterator<Item = SwarmAddress>,
        transport_policy: RuntimeTransportPolicy,
    ) -> Result<Self, SwarmError> {
        let addresses = addresses.into_iter().collect::<Vec<_>>();
        if addresses.iter().all(SwarmAddress::is_memory) {
            Ok(Self::Memory(Box::new(
                MemoryControlPlaneShell::with_keypair(control_protocol, keypair)?,
            )))
        } else {
            Ok(Self::Native(Box::new(
                NativeControlPlaneShell::with_keypair(control_protocol, keypair, transport_policy)?,
            )))
        }
    }

    /// Performs the local peer ID operation.
    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        match self {
            Self::Memory(shell) => shell.local_peer_id(),
            Self::Native(shell) => shell.local_peer_id(),
        }
    }

    /// Performs the listen on operation.
    pub fn listen_on(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        match self {
            Self::Memory(shell) => shell.listen_on(address),
            Self::Native(shell) => shell.listen_on(address),
        }
    }

    /// Performs the dial operation.
    pub fn dial(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        match self {
            Self::Memory(shell) => shell.dial(address),
            Self::Native(shell) => shell.dial(address),
        }
    }

    /// Disconnects one peer from the local swarm.
    pub fn disconnect_peer(&mut self, peer_id: &str) -> Result<(), SwarmError> {
        match self {
            Self::Memory(shell) => shell.disconnect_peer(peer_id),
            Self::Native(shell) => shell.disconnect_peer(peer_id),
        }
    }

    /// Performs the connected peer count operation.
    pub fn connected_peer_count(&self) -> usize {
        match self {
            Self::Memory(shell) => shell.connected_peer_count(),
            Self::Native(shell) => shell.connected_peer_count(),
        }
    }

    /// Performs the publish control operation.
    pub fn publish_control(&mut self, announcement: ControlAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_control(announcement),
            Self::Native(shell) => shell.publish_control(announcement),
        }
    }

    /// Performs the publish lifecycle operation.
    pub fn publish_lifecycle(&mut self, announcement: ExperimentLifecycleAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_lifecycle(announcement),
            Self::Native(shell) => shell.publish_lifecycle(announcement),
        }
    }

    /// Performs the publish head operation.
    pub fn publish_head(&mut self, announcement: HeadAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_head(announcement),
            Self::Native(shell) => shell.publish_head(announcement),
        }
    }

    /// Performs the publish lease operation.
    pub fn publish_lease(&mut self, announcement: LeaseAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_lease(announcement),
            Self::Native(shell) => shell.publish_lease(announcement),
        }
    }

    /// Performs the publish merge operation.
    pub fn publish_merge(&mut self, announcement: MergeAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_merge(announcement),
            Self::Native(shell) => shell.publish_merge(announcement),
        }
    }

    /// Performs the publish merge window operation.
    pub fn publish_merge_window(&mut self, announcement: MergeWindowAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_merge_window(announcement),
            Self::Native(shell) => shell.publish_merge_window(announcement),
        }
    }

    /// Performs the publish reducer assignment operation.
    pub fn publish_reducer_assignment(&mut self, announcement: ReducerAssignmentAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_reducer_assignment(announcement),
            Self::Native(shell) => shell.publish_reducer_assignment(announcement),
        }
    }

    /// Performs the publish update operation.
    pub fn publish_update(&mut self, announcement: UpdateEnvelopeAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_update(announcement),
            Self::Native(shell) => shell.publish_update(announcement),
        }
    }

    /// Performs the publish aggregate proposal operation.
    pub fn publish_aggregate_proposal(&mut self, announcement: AggregateProposalAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_aggregate_proposal(announcement),
            Self::Native(shell) => shell.publish_aggregate_proposal(announcement),
        }
    }

    /// Performs the publish reduction certificate operation.
    pub fn publish_reduction_certificate(
        &mut self,
        announcement: ReductionCertificateAnnouncement,
    ) {
        match self {
            Self::Memory(shell) => shell.publish_reduction_certificate(announcement),
            Self::Native(shell) => shell.publish_reduction_certificate(announcement),
        }
    }

    /// Performs the publish validation quorum operation.
    pub fn publish_validation_quorum(&mut self, announcement: ValidationQuorumAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_validation_quorum(announcement),
            Self::Native(shell) => shell.publish_validation_quorum(announcement),
        }
    }

    /// Performs the publish reducer load operation.
    pub fn publish_reducer_load(&mut self, announcement: ReducerLoadAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_reducer_load(announcement),
            Self::Native(shell) => shell.publish_reducer_load(announcement),
        }
    }

    /// Performs the publish auth operation.
    pub fn publish_auth(&mut self, announcement: PeerAuthAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_auth(announcement),
            Self::Native(shell) => shell.publish_auth(announcement),
        }
    }

    /// Performs the publish directory operation.
    pub fn publish_directory(&mut self, announcement: ExperimentDirectoryAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_directory(announcement),
            Self::Native(shell) => shell.publish_directory(announcement),
        }
    }

    /// Performs the publish peer directory operation.
    pub fn publish_peer_directory(&mut self, announcement: PeerDirectoryAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_peer_directory(announcement),
            Self::Native(shell) => shell.publish_peer_directory(announcement),
        }
    }

    /// Performs the publish metrics operation.
    pub fn publish_metrics(&mut self, announcement: MetricsAnnouncement) {
        match self {
            Self::Memory(shell) => shell.publish_metrics(announcement),
            Self::Native(shell) => shell.publish_metrics(announcement),
        }
    }

    /// Performs the snapshot operation.
    pub fn snapshot(&self) -> &ControlPlaneSnapshot {
        match self {
            Self::Memory(shell) => shell.snapshot(),
            Self::Native(shell) => shell.snapshot(),
        }
    }

    /// Performs the subscribe topic operation.
    pub fn subscribe_topic(&mut self, topic: OverlayTopic) -> Result<(), SwarmError> {
        match self {
            Self::Memory(shell) => shell.subscribe_topic(topic),
            Self::Native(shell) => shell.subscribe_topic(topic),
        }
    }

    /// Performs the publish pubsub operation.
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

    /// Performs the publish artifact operation.
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

    /// Performs the request snapshot operation.
    pub fn request_snapshot(&mut self, peer_id: &str) -> Result<(), SwarmError> {
        match self {
            Self::Memory(shell) => shell.request_snapshot(peer_id),
            Self::Native(shell) => shell.request_snapshot(peer_id),
        }
    }

    pub fn request_snapshot_id(&mut self, peer_id: &str) -> Result<String, SwarmError> {
        match self {
            Self::Memory(shell) => shell.request_snapshot_id(peer_id),
            Self::Native(shell) => shell.request_snapshot_id(peer_id),
        }
    }

    pub fn request_artifact_manifest_id(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
    ) -> Result<String, SwarmError> {
        match self {
            Self::Memory(shell) => shell.request_artifact_manifest_id(peer_id, artifact_id),
            Self::Native(shell) => shell.request_artifact_manifest_id(peer_id, artifact_id),
        }
    }

    pub fn request_artifact_chunk_id(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
    ) -> Result<String, SwarmError> {
        match self {
            Self::Memory(shell) => shell.request_artifact_chunk_id(peer_id, artifact_id, chunk_id),
            Self::Native(shell) => shell.request_artifact_chunk_id(peer_id, artifact_id, chunk_id),
        }
    }

    /// Fetches the snapshot.
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

    /// Fetches the artifact manifest.
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

    /// Fetches the artifact chunk.
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

    /// Performs the wait event operation.
    pub fn wait_event(&mut self, timeout: Duration) -> Option<LiveControlPlaneEvent> {
        match self {
            Self::Memory(shell) => shell.wait_event(timeout),
            Self::Native(shell) => shell.wait_event(timeout),
        }
    }
}
