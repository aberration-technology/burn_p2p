use super::*;
use crate::diloco_store::DiLoCoControlStore;

#[cfg(not(target_arch = "wasm32"))]
pub struct MemoryControlPlaneShell {
    local_peer_id: Libp2pPeerId,
    swarm: Swarm<request_response::cbor::Behaviour<ControlPlaneRequest, ControlPlaneResponse>>,
    snapshot: ControlPlaneSnapshot,
    hot_index: ControlPlaneHotIndex,
    artifacts: BTreeMap<ArtifactId, ArtifactDescriptor>,
    chunks: BTreeMap<(ArtifactId, ChunkId), ArtifactChunkPayload>,
    diloco: DiLoCoControlStore,
    completed_diloco_responses: BTreeMap<String, (String, DiLoCoResponse)>,
    subscribed_topics: BTreeSet<String>,
    pending_events: VecDeque<LiveControlPlaneEvent>,
}

#[cfg(not(target_arch = "wasm32"))]
impl MemoryControlPlaneShell {
    /// Creates a new value.
    pub fn new(control_protocol: ProtocolId) -> Result<Self, SwarmError> {
        Self::with_keypair(control_protocol, Keypair::generate_ed25519())
    }

    /// Returns a copy configured with the keypair.
    pub fn with_keypair(
        control_protocol: ProtocolId,
        keypair: Keypair,
    ) -> Result<Self, SwarmError> {
        let local_peer_id = keypair.public().to_peer_id();
        let transport = MemoryTransport::default()
            .upgrade(upgrade::Version::V1)
            .authenticate(tls_config(&keypair)?)
            .multiplex(yamux::Config::default())
            .boxed();
        let protocol = stream_protocol(&control_protocol)?;
        let behaviour = request_response::cbor::Behaviour::new(
            [(protocol, ProtocolSupport::Full)],
            request_response::Config::default(),
        );
        let swarm = Swarm::new(
            transport,
            behaviour,
            local_peer_id,
            Libp2pSwarmConfig::without_executor(),
        );

        Ok(Self {
            local_peer_id,
            swarm,
            snapshot: ControlPlaneSnapshot::default(),
            hot_index: ControlPlaneHotIndex::default(),
            artifacts: BTreeMap::new(),
            chunks: BTreeMap::new(),
            diloco: DiLoCoControlStore::default(),
            completed_diloco_responses: BTreeMap::new(),
            subscribed_topics: BTreeSet::new(),
            pending_events: VecDeque::new(),
        })
    }

    /// Performs the local peer ID operation.
    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        &self.local_peer_id
    }

    /// Performs the listen on operation.
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

    /// Performs the dial operation.
    pub fn dial(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        let address: Multiaddr = address
            .as_str()
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(address.as_str().to_owned()))?;
        self.swarm
            .dial(address)
            .map_err(|error| SwarmError::Dial(error.to_string()))
    }

    /// Registers one externally reachable address with the in-memory control plane.
    pub fn add_external_address(&mut self, _address: SwarmAddress) -> Result<(), SwarmError> {
        Ok(())
    }

    /// Disconnects one peer from the local swarm.
    pub fn disconnect_peer(&mut self, peer_id: &str) -> Result<(), SwarmError> {
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        self.swarm
            .disconnect_peer_id(peer_id)
            .map_err(|_| SwarmError::Request("failed to disconnect peer".into()))
    }

    /// Performs the connected peer count operation.
    pub fn connected_peer_count(&self) -> usize {
        self.swarm.network_info().num_peers()
    }

    /// Returns the currently connected peer IDs.
    pub fn connected_peer_ids(&self) -> Vec<PeerId> {
        self.swarm
            .connected_peers()
            .map(|peer_id| PeerId::new(peer_id.to_string()))
            .collect()
    }

    /// Performs the publish control operation.
    pub fn publish_control(&mut self, announcement: ControlAnnouncement) {
        self.snapshot.insert_control_announcement(announcement);
    }

    /// Performs the publish lifecycle operation.
    pub fn publish_lifecycle(&mut self, announcement: ExperimentLifecycleAnnouncement) {
        self.snapshot.insert_lifecycle_announcement(announcement);
    }

    /// Performs the publish schedule operation.
    pub fn publish_schedule(&mut self, announcement: FleetScheduleAnnouncement) {
        self.snapshot.insert_schedule_announcement(announcement);
    }

    /// Performs the publish head operation.
    pub fn publish_head(&mut self, announcement: HeadAnnouncement) {
        self.snapshot.insert_head_announcement(announcement);
    }

    /// Performs the publish lease operation.
    pub fn publish_lease(&mut self, announcement: LeaseAnnouncement) {
        self.snapshot.insert_lease_announcement(announcement);
    }

    /// Performs the publish merge operation.
    pub fn publish_merge(&mut self, announcement: MergeAnnouncement) {
        insert_merge_announcement_with_index(&mut self.snapshot, &mut self.hot_index, announcement);
    }

    /// Performs the publish merge window operation.
    pub fn publish_merge_window(&mut self, announcement: MergeWindowAnnouncement) {
        self.snapshot.insert_merge_window_announcement(announcement);
    }

    /// Performs the publish reducer assignment operation.
    pub fn publish_reducer_assignment(&mut self, announcement: ReducerAssignmentAnnouncement) {
        self.snapshot
            .insert_reducer_assignment_announcement(announcement);
    }

    /// Performs the publish update operation.
    pub fn publish_update(&mut self, announcement: UpdateEnvelopeAnnouncement) {
        self.snapshot.insert_update_announcement(announcement);
    }

    /// Performs the publish trainer promotion attestation operation.
    pub fn publish_trainer_promotion_attestation(
        &mut self,
        announcement: TrainerPromotionAttestationAnnouncement,
    ) {
        insert_trainer_promotion_attestation_announcement_with_index(
            &mut self.snapshot,
            &mut self.hot_index,
            announcement,
        );
    }

    /// Performs the publish diffusion promotion certificate operation.
    pub fn publish_diffusion_promotion_certificate(
        &mut self,
        announcement: DiffusionPromotionCertificateAnnouncement,
    ) {
        insert_diffusion_promotion_certificate_announcement_with_index(
            &mut self.snapshot,
            &mut self.hot_index,
            announcement,
        );
    }

    /// Performs the publish aggregate operation.
    pub fn publish_aggregate_proposal(&mut self, announcement: AggregateProposalAnnouncement) {
        insert_aggregate_proposal_announcement_with_index(
            &mut self.snapshot,
            &mut self.hot_index,
            announcement,
        );
    }

    /// Performs the publish reduction certificate operation.
    pub fn publish_reduction_certificate(
        &mut self,
        announcement: ReductionCertificateAnnouncement,
    ) {
        insert_reduction_certificate_announcement_with_index(
            &mut self.snapshot,
            &mut self.hot_index,
            announcement,
        );
    }

    /// Performs the publish validation quorum operation.
    pub fn publish_validation_quorum(&mut self, announcement: ValidationQuorumAnnouncement) {
        insert_validation_quorum_announcement_with_index(
            &mut self.snapshot,
            &mut self.hot_index,
            announcement,
        );
    }

    /// Performs the publish reducer load operation.
    pub fn publish_reducer_load(&mut self, announcement: ReducerLoadAnnouncement) {
        self.snapshot.insert_reducer_load_announcement(announcement);
    }

    /// Performs the publish auth operation.
    pub fn publish_auth(&mut self, announcement: PeerAuthAnnouncement) {
        self.snapshot.insert_auth_announcement(announcement);
    }

    /// Performs the publish directory operation.
    pub fn publish_directory(&mut self, announcement: ExperimentDirectoryAnnouncement) {
        self.snapshot.insert_directory_announcement(announcement);
    }

    /// Performs the publish peer directory operation.
    pub fn publish_peer_directory(&mut self, announcement: PeerDirectoryAnnouncement) {
        self.snapshot
            .insert_peer_directory_announcement(announcement);
    }

    /// Performs the publish metrics operation.
    pub fn publish_metrics(&mut self, announcement: MetricsAnnouncement) {
        self.snapshot.insert_metrics_announcement(announcement);
    }

    /// Performs the snapshot operation.
    pub fn snapshot(&self) -> &ControlPlaneSnapshot {
        &self.snapshot
    }

    /// Performs the subscribe topic operation.
    pub fn subscribe_topic(&mut self, topic: OverlayTopic) -> Result<(), SwarmError> {
        self.subscribed_topics.insert(topic.path);
        Ok(())
    }

    /// Performs the publish pubsub operation.
    pub fn publish_pubsub(
        &mut self,
        _topic: OverlayTopic,
        _payload: PubsubPayload,
    ) -> Result<(), SwarmError> {
        Ok(())
    }

    /// Performs the publish artifact operation.
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

    /// Publishes the local DiLoCo state snapshot, outer optimizer state, and current parameters.
    pub fn publish_diloco_state(
        &mut self,
        snapshot: DiLoCoStateSnapshot,
        outer_optimizer_state: Option<StateBlob>,
        current_parameters: Option<FlattenedTensorPack>,
    ) {
        self.diloco
            .publish_state(snapshot, outer_optimizer_state, current_parameters);
    }

    /// Publishes one encoded pseudo-gradient manifest and chunk set.
    pub fn publish_diloco_gradient(
        &mut self,
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
    ) {
        self.diloco.publish_gradient(manifest, chunks);
    }

    pub(crate) fn request_diloco_id(
        &mut self,
        peer_id: &str,
        request: DiLoCoRequest,
    ) -> Result<String, SwarmError> {
        self.settle_request_response();
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        Ok(self
            .swarm
            .behaviour_mut()
            .send_request(&peer_id, ControlPlaneRequest::DiLoCo(Box::new(request)))
            .to_string())
    }

    pub fn fetch_diloco(
        &mut self,
        peer_id: &str,
        request: DiLoCoRequest,
        timeout: Duration,
    ) -> Result<DiLoCoResponse, SwarmError> {
        self.settle_request_response();
        let request_id = self.request_diloco_id(peer_id, request)?;
        let mut deferred_events = std::mem::take(&mut self.pending_events);

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some((response_peer_id, response)) =
                self.completed_diloco_responses.remove(&request_id)
            {
                self.pending_events.extend(deferred_events);
                if response_peer_id != peer_id {
                    return Err(SwarmError::Request(format!(
                        "DiLoCo response peer mismatch: expected {}, got {}",
                        peer_id, response_peer_id
                    )));
                }
                return Ok(response);
            }
            if let Some(event) = self.wait_live_event(Duration::from_millis(50)) {
                match event {
                    LiveControlPlaneEvent::RequestFailure {
                        request_id: Some(failure_id),
                        message,
                        ..
                    } if failure_id == request_id => {
                        self.pending_events.extend(deferred_events);
                        return Err(SwarmError::Request(message));
                    }
                    other => deferred_events.push_back(other),
                }
            }
        }

        self.pending_events.extend(deferred_events);
        Err(SwarmError::TimedOut("diloco"))
    }

    /// Performs the request snapshot operation.
    pub fn request_snapshot(&mut self, peer_id: &str) -> Result<(), SwarmError> {
        self.request_snapshot_id(peer_id).map(|_| ())
    }

    pub(crate) fn request_snapshot_id(&mut self, peer_id: &str) -> Result<String, SwarmError> {
        self.settle_request_response();
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        Ok(self
            .swarm
            .behaviour_mut()
            .send_request(&peer_id, ControlPlaneRequest::Snapshot)
            .to_string())
    }

    /// Performs the request artifact manifest operation.
    pub fn request_artifact_manifest(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
    ) -> Result<(), SwarmError> {
        self.request_artifact_manifest_id(peer_id, artifact_id)
            .map(|_| ())
    }

    pub(crate) fn request_artifact_manifest_id(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
    ) -> Result<String, SwarmError> {
        self.settle_request_response();
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        Ok(self
            .swarm
            .behaviour_mut()
            .send_request(
                &peer_id,
                ControlPlaneRequest::ArtifactManifest { artifact_id },
            )
            .to_string())
    }

    /// Performs the request artifact chunk operation.
    pub fn request_artifact_chunk(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
    ) -> Result<(), SwarmError> {
        self.request_artifact_chunk_id(peer_id, artifact_id, chunk_id)
            .map(|_| ())
    }

    pub(crate) fn request_artifact_chunk_id(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
    ) -> Result<String, SwarmError> {
        self.settle_request_response();
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        Ok(self
            .swarm
            .behaviour_mut()
            .send_request(
                &peer_id,
                ControlPlaneRequest::ArtifactChunk {
                    artifact_id,
                    chunk_id,
                },
            )
            .to_string())
    }

    /// Fetches the snapshot.
    pub fn fetch_snapshot(
        &mut self,
        peer_id: &str,
        timeout: Duration,
    ) -> Result<ControlPlaneSnapshot, SwarmError> {
        self.settle_request_response();
        let request_id = self.request_snapshot_id(peer_id)?;
        let mut deferred_events = std::mem::take(&mut self.pending_events);

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(event) = self.wait_live_event(Duration::from_millis(50)) {
                match event {
                    LiveControlPlaneEvent::SnapshotReceived {
                        request_id: response_id,
                        snapshot,
                        ..
                    } if response_id == request_id => {
                        self.settle_request_response();
                        self.pending_events.extend(deferred_events);
                        return Ok(snapshot);
                    }
                    LiveControlPlaneEvent::RequestFailure {
                        request_id: Some(failure_id),
                        message,
                        ..
                    } if failure_id == request_id => {
                        self.pending_events.extend(deferred_events);
                        return Err(SwarmError::Request(message));
                    }
                    other => deferred_events.push_back(other),
                }
            }
        }

        self.pending_events.extend(deferred_events);
        Err(SwarmError::TimedOut("snapshot"))
    }

    /// Fetches the artifact manifest.
    pub fn fetch_artifact_manifest(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        timeout: Duration,
    ) -> Result<Option<ArtifactDescriptor>, SwarmError> {
        self.settle_request_response();
        let request_id = self.request_artifact_manifest_id(peer_id, artifact_id)?;
        let mut deferred_events = std::mem::take(&mut self.pending_events);

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(event) = self.wait_live_event(Duration::from_millis(50)) {
                match event {
                    LiveControlPlaneEvent::ArtifactManifestReceived {
                        request_id: response_id,
                        descriptor,
                        ..
                    } if response_id == request_id => {
                        self.settle_request_response();
                        self.pending_events.extend(deferred_events);
                        return Ok(descriptor);
                    }
                    LiveControlPlaneEvent::RequestFailure {
                        request_id: Some(failure_id),
                        message,
                        ..
                    } if failure_id == request_id => {
                        self.pending_events.extend(deferred_events);
                        return Err(SwarmError::Request(message));
                    }
                    other => deferred_events.push_back(other),
                }
            }
        }

        self.pending_events.extend(deferred_events);
        Err(SwarmError::TimedOut("artifact-manifest"))
    }

    /// Fetches the artifact chunk.
    pub fn fetch_artifact_chunk(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
        timeout: Duration,
    ) -> Result<Option<ArtifactChunkPayload>, SwarmError> {
        self.settle_request_response();
        let request_id = self.request_artifact_chunk_id(peer_id, artifact_id, chunk_id)?;
        let mut deferred_events = std::mem::take(&mut self.pending_events);

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(event) = self.wait_live_event(Duration::from_millis(50)) {
                match event {
                    LiveControlPlaneEvent::ArtifactChunkReceived {
                        request_id: response_id,
                        payload,
                        ..
                    } if response_id == request_id => {
                        self.settle_request_response();
                        self.pending_events.extend(deferred_events);
                        return Ok(payload);
                    }
                    LiveControlPlaneEvent::RequestFailure {
                        request_id: Some(failure_id),
                        message,
                        ..
                    } if failure_id == request_id => {
                        self.pending_events.extend(deferred_events);
                        return Err(SwarmError::Request(message));
                    }
                    other => deferred_events.push_back(other),
                }
            }
        }

        self.pending_events.extend(deferred_events);
        Err(SwarmError::TimedOut("artifact-chunk"))
    }

    /// Performs the poll event operation.
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
                        ControlPlaneRequest::DiLoCo(request) => {
                            let response = ControlPlaneResponse::DiLoCo(Box::new(
                                self.diloco.respond(*request),
                            ));
                            match self.swarm.behaviour_mut().send_response(channel, response) {
                                Ok(()) => Poll::Ready(LiveControlPlaneEvent::Other {
                                    kind: format!("responded to DiLoCo request from {}", peer),
                                }),
                                Err(_) => Poll::Ready(LiveControlPlaneEvent::ResponseSendFailure {
                                    peer_id: peer.to_string(),
                                    message: "DiLoCo response channel closed".into(),
                                }),
                            }
                        }
                    },
                    request_response::Message::Response {
                        request_id,
                        response,
                    } => match response {
                        ControlPlaneResponse::Snapshot(snapshot) => {
                            Poll::Ready(LiveControlPlaneEvent::SnapshotReceived {
                                peer_id: peer.to_string(),
                                request_id: request_id.to_string(),
                                snapshot,
                            })
                        }
                        ControlPlaneResponse::ArtifactManifest(descriptor) => {
                            Poll::Ready(LiveControlPlaneEvent::ArtifactManifestReceived {
                                peer_id: peer.to_string(),
                                request_id: request_id.to_string(),
                                descriptor,
                            })
                        }
                        ControlPlaneResponse::ArtifactChunk(payload) => {
                            Poll::Ready(LiveControlPlaneEvent::ArtifactChunkReceived {
                                peer_id: peer.to_string(),
                                request_id: request_id.to_string(),
                                payload,
                            })
                        }
                        ControlPlaneResponse::DiLoCo(response) => {
                            self.completed_diloco_responses
                                .insert(request_id.to_string(), (peer.to_string(), *response));
                            Poll::Ready(LiveControlPlaneEvent::Other {
                                kind: format!(
                                    "received DiLoCo response {} from {}",
                                    request_id, peer
                                ),
                            })
                        }
                    },
                },
                request_response::Event::OutboundFailure {
                    peer,
                    request_id,
                    error,
                    ..
                } => Poll::Ready(LiveControlPlaneEvent::RequestFailure {
                    peer_id: peer.to_string(),
                    request_id: Some(request_id.to_string()),
                    message: error.to_string(),
                }),
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

    fn wait_live_event(&mut self, timeout: Duration) -> Option<LiveControlPlaneEvent> {
        let deadline = Instant::now() + timeout;
        let waker = futures::task::noop_waker_ref();

        loop {
            let mut cx = Context::from_waker(waker);
            match self.poll_event(&mut cx) {
                Poll::Ready(event) => return Some(event),
                Poll::Pending if Instant::now() >= deadline => break,
                Poll::Pending => std::thread::sleep(Duration::from_millis(10)),
            }
        }

        None
    }

    fn settle_request_response(&mut self) {
        let deadline = Instant::now() + Duration::from_millis(250);
        while Instant::now() < deadline {
            match self.wait_live_event(Duration::from_millis(5)) {
                Some(event) => self.pending_events.push_back(event),
                None => break,
            }
        }
    }

    /// Performs the wait event operation.
    pub fn wait_event(&mut self, timeout: Duration) -> Option<LiveControlPlaneEvent> {
        self.wait_live_event(timeout)
            .or_else(|| self.pending_events.pop_front())
    }
}

#[cfg(target_arch = "wasm32")]
pub struct MemoryControlPlaneShell {
    local_peer_id: Libp2pPeerId,
    snapshot: ControlPlaneSnapshot,
    hot_index: ControlPlaneHotIndex,
    artifacts: BTreeMap<ArtifactId, ArtifactDescriptor>,
    chunks: BTreeMap<(ArtifactId, ChunkId), ArtifactChunkPayload>,
    subscribed_topics: BTreeSet<String>,
}

#[cfg(target_arch = "wasm32")]
impl MemoryControlPlaneShell {
    /// Creates a new value.
    pub fn new(control_protocol: ProtocolId) -> Result<Self, SwarmError> {
        Self::with_keypair(control_protocol, Keypair::generate_ed25519())
    }

    /// Returns a copy configured with the keypair.
    pub fn with_keypair(
        _control_protocol: ProtocolId,
        keypair: Keypair,
    ) -> Result<Self, SwarmError> {
        Ok(Self {
            local_peer_id: keypair.public().to_peer_id(),
            snapshot: ControlPlaneSnapshot::default(),
            hot_index: ControlPlaneHotIndex::default(),
            artifacts: BTreeMap::new(),
            chunks: BTreeMap::new(),
            subscribed_topics: BTreeSet::new(),
        })
    }

    /// Performs the local peer ID operation.
    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        &self.local_peer_id
    }

    /// Performs the listen on operation.
    pub fn listen_on(&mut self, _address: SwarmAddress) -> Result<(), SwarmError> {
        Err(SwarmError::Runtime(
            "memory control-plane transport is unavailable on wasm targets".into(),
        ))
    }

    /// Performs the dial operation.
    pub fn dial(&mut self, _address: SwarmAddress) -> Result<(), SwarmError> {
        Err(SwarmError::Runtime(
            "memory control-plane transport is unavailable on wasm targets".into(),
        ))
    }

    /// Registers one externally reachable address with the wasm memory stub.
    pub fn add_external_address(&mut self, _address: SwarmAddress) -> Result<(), SwarmError> {
        Ok(())
    }

    pub fn disconnect_peer(&mut self, _peer_id: &str) -> Result<(), SwarmError> {
        Ok(())
    }

    /// Performs the connected peer count operation.
    pub fn connected_peer_count(&self) -> usize {
        0
    }

    /// Returns the currently connected peer IDs.
    pub fn connected_peer_ids(&self) -> Vec<PeerId> {
        Vec::new()
    }

    /// Performs the publish control operation.
    pub fn publish_control(&mut self, announcement: ControlAnnouncement) {
        self.snapshot.insert_control_announcement(announcement);
    }

    /// Performs the publish lifecycle operation.
    pub fn publish_lifecycle(&mut self, announcement: ExperimentLifecycleAnnouncement) {
        self.snapshot.insert_lifecycle_announcement(announcement);
    }

    /// Performs the publish schedule operation.
    pub fn publish_schedule(&mut self, announcement: FleetScheduleAnnouncement) {
        self.snapshot.insert_schedule_announcement(announcement);
    }

    /// Performs the publish head operation.
    pub fn publish_head(&mut self, announcement: HeadAnnouncement) {
        self.snapshot.insert_head_announcement(announcement);
    }

    /// Performs the publish lease operation.
    pub fn publish_lease(&mut self, announcement: LeaseAnnouncement) {
        self.snapshot.insert_lease_announcement(announcement);
    }

    /// Performs the publish merge operation.
    pub fn publish_merge(&mut self, announcement: MergeAnnouncement) {
        insert_merge_announcement_with_index(&mut self.snapshot, &mut self.hot_index, announcement);
    }

    /// Performs the publish merge window operation.
    pub fn publish_merge_window(&mut self, announcement: MergeWindowAnnouncement) {
        self.snapshot.insert_merge_window_announcement(announcement);
    }

    /// Performs the publish reducer assignment operation.
    pub fn publish_reducer_assignment(&mut self, announcement: ReducerAssignmentAnnouncement) {
        self.snapshot
            .insert_reducer_assignment_announcement(announcement);
    }

    /// Performs the publish update operation.
    pub fn publish_update(&mut self, announcement: UpdateEnvelopeAnnouncement) {
        self.snapshot.insert_update_announcement(announcement);
    }

    /// Performs the publish trainer promotion attestation operation.
    pub fn publish_trainer_promotion_attestation(
        &mut self,
        announcement: TrainerPromotionAttestationAnnouncement,
    ) {
        insert_trainer_promotion_attestation_announcement_with_index(
            &mut self.snapshot,
            &mut self.hot_index,
            announcement,
        );
    }

    /// Performs the publish diffusion promotion certificate operation.
    pub fn publish_diffusion_promotion_certificate(
        &mut self,
        announcement: DiffusionPromotionCertificateAnnouncement,
    ) {
        insert_diffusion_promotion_certificate_announcement_with_index(
            &mut self.snapshot,
            &mut self.hot_index,
            announcement,
        );
    }

    /// Performs the publish aggregate operation.
    pub fn publish_aggregate_proposal(&mut self, announcement: AggregateProposalAnnouncement) {
        insert_aggregate_proposal_announcement_with_index(
            &mut self.snapshot,
            &mut self.hot_index,
            announcement,
        );
    }

    /// Performs the publish reduction certificate operation.
    pub fn publish_reduction_certificate(
        &mut self,
        announcement: ReductionCertificateAnnouncement,
    ) {
        insert_reduction_certificate_announcement_with_index(
            &mut self.snapshot,
            &mut self.hot_index,
            announcement,
        );
    }

    /// Performs the publish validation quorum operation.
    pub fn publish_validation_quorum(&mut self, announcement: ValidationQuorumAnnouncement) {
        insert_validation_quorum_announcement_with_index(
            &mut self.snapshot,
            &mut self.hot_index,
            announcement,
        );
    }

    /// Performs the publish reducer load operation.
    pub fn publish_reducer_load(&mut self, announcement: ReducerLoadAnnouncement) {
        self.snapshot.insert_reducer_load_announcement(announcement);
    }

    /// Performs the publish auth operation.
    pub fn publish_auth(&mut self, announcement: PeerAuthAnnouncement) {
        self.snapshot.insert_auth_announcement(announcement);
    }

    /// Performs the publish directory operation.
    pub fn publish_directory(&mut self, announcement: ExperimentDirectoryAnnouncement) {
        self.snapshot.insert_directory_announcement(announcement);
    }

    /// Performs the publish peer directory operation.
    pub fn publish_peer_directory(&mut self, announcement: PeerDirectoryAnnouncement) {
        self.snapshot
            .insert_peer_directory_announcement(announcement);
    }

    /// Performs the publish metrics operation.
    pub fn publish_metrics(&mut self, announcement: MetricsAnnouncement) {
        self.snapshot.insert_metrics_announcement(announcement);
    }

    /// Performs the snapshot operation.
    pub fn snapshot(&self) -> &ControlPlaneSnapshot {
        &self.snapshot
    }

    /// Performs the subscribe topic operation.
    pub fn subscribe_topic(&mut self, topic: OverlayTopic) -> Result<(), SwarmError> {
        self.subscribed_topics.insert(topic.path);
        Ok(())
    }

    /// Performs the publish pubsub operation.
    pub fn publish_pubsub(
        &mut self,
        _topic: OverlayTopic,
        _payload: PubsubPayload,
    ) -> Result<(), SwarmError> {
        Ok(())
    }

    /// Performs the publish artifact operation.
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

    /// Publishes the local DiLoCo state snapshot, outer optimizer state, and current parameters.
    pub fn publish_diloco_state(
        &mut self,
        snapshot: DiLoCoStateSnapshot,
        outer_optimizer_state: Option<StateBlob>,
        current_parameters: Option<FlattenedTensorPack>,
    ) {
        self.diloco
            .publish_state(snapshot, outer_optimizer_state, current_parameters);
    }

    /// Publishes one encoded pseudo-gradient manifest and chunk set.
    pub fn publish_diloco_gradient(
        &mut self,
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
    ) {
        self.diloco.publish_gradient(manifest, chunks);
    }

    pub(crate) fn request_diloco_id(
        &mut self,
        peer_id: &str,
        request: DiLoCoRequest,
    ) -> Result<String, SwarmError> {
        self.settle_request_response();
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        Ok(self
            .swarm
            .behaviour_mut()
            .send_request(&peer_id, ControlPlaneRequest::DiLoCo(Box::new(request)))
            .to_string())
    }

    pub fn fetch_diloco(
        &mut self,
        peer_id: &str,
        request: DiLoCoRequest,
        timeout: Duration,
    ) -> Result<DiLoCoResponse, SwarmError> {
        self.settle_request_response();
        let request_id = self.request_diloco_id(peer_id, request)?;
        let mut deferred_events = std::mem::take(&mut self.pending_events);

        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some((response_peer_id, response)) =
                self.completed_diloco_responses.remove(&request_id)
            {
                self.pending_events.extend(deferred_events);
                if response_peer_id != peer_id {
                    return Err(SwarmError::Request(format!(
                        "DiLoCo response peer mismatch: expected {}, got {}",
                        peer_id, response_peer_id
                    )));
                }
                return Ok(response);
            }
            if let Some(event) = self.wait_live_event(Duration::from_millis(50)) {
                match event {
                    LiveControlPlaneEvent::RequestFailure {
                        request_id: Some(failure_id),
                        message,
                        ..
                    } if failure_id == request_id => {
                        self.pending_events.extend(deferred_events);
                        return Err(SwarmError::Request(message));
                    }
                    other => deferred_events.push_back(other),
                }
            }
        }

        self.pending_events.extend(deferred_events);
        Err(SwarmError::TimedOut("diloco"))
    }

    /// Performs the request snapshot operation.
    pub fn request_snapshot(&mut self, _peer_id: &str) -> Result<(), SwarmError> {
        Err(SwarmError::Runtime(
            "memory control-plane transport is unavailable on wasm targets".into(),
        ))
    }

    pub(crate) fn request_snapshot_id(&mut self, peer_id: &str) -> Result<String, SwarmError> {
        self.request_snapshot(peer_id)?;
        Ok("memory-wasm-snapshot-request".into())
    }

    /// Performs the request artifact manifest operation.
    pub fn request_artifact_manifest(
        &mut self,
        _peer_id: &str,
        _artifact_id: ArtifactId,
    ) -> Result<(), SwarmError> {
        Err(SwarmError::Runtime(
            "memory control-plane transport is unavailable on wasm targets".into(),
        ))
    }

    pub(crate) fn request_artifact_manifest_id(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
    ) -> Result<String, SwarmError> {
        self.request_artifact_manifest(peer_id, artifact_id)?;
        Ok("memory-wasm-artifact-manifest-request".into())
    }

    /// Performs the request artifact chunk operation.
    pub fn request_artifact_chunk(
        &mut self,
        _peer_id: &str,
        _artifact_id: ArtifactId,
        _chunk_id: ChunkId,
    ) -> Result<(), SwarmError> {
        Err(SwarmError::Runtime(
            "memory control-plane transport is unavailable on wasm targets".into(),
        ))
    }

    pub(crate) fn request_artifact_chunk_id(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
    ) -> Result<String, SwarmError> {
        self.request_artifact_chunk(peer_id, artifact_id, chunk_id)?;
        Ok("memory-wasm-artifact-chunk-request".into())
    }

    /// Fetches the snapshot.
    pub fn fetch_snapshot(
        &mut self,
        _peer_id: &str,
        _timeout: Duration,
    ) -> Result<ControlPlaneSnapshot, SwarmError> {
        Err(SwarmError::Runtime(
            "memory control-plane transport is unavailable on wasm targets".into(),
        ))
    }

    /// Fetches the artifact manifest.
    pub fn fetch_artifact_manifest(
        &mut self,
        _peer_id: &str,
        _artifact_id: ArtifactId,
        _timeout: Duration,
    ) -> Result<Option<ArtifactDescriptor>, SwarmError> {
        Err(SwarmError::Runtime(
            "memory control-plane transport is unavailable on wasm targets".into(),
        ))
    }

    /// Fetches the artifact chunk.
    pub fn fetch_artifact_chunk(
        &mut self,
        _peer_id: &str,
        _artifact_id: ArtifactId,
        _chunk_id: ChunkId,
        _timeout: Duration,
    ) -> Result<Option<ArtifactChunkPayload>, SwarmError> {
        Err(SwarmError::Runtime(
            "memory control-plane transport is unavailable on wasm targets".into(),
        ))
    }

    /// Performs the poll event operation.
    pub fn poll_event(&mut self, _cx: &mut Context<'_>) -> Poll<LiveControlPlaneEvent> {
        Poll::Pending
    }

    /// Performs the wait event operation.
    pub fn wait_event(&mut self, _timeout: Duration) -> Option<LiveControlPlaneEvent> {
        None
    }
}
