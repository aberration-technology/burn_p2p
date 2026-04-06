use super::*;

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
    /// Creates a new value.
    pub fn new(control_protocol: ProtocolId) -> Result<Self, SwarmError> {
        Self::with_keypair(control_protocol, Keypair::generate_ed25519())
    }

    /// Returns a copy configured with the keypair.
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
                    libp2p::tls::Config::new,
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

    /// Performs the local peer ID operation.
    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        &self.local_peer_id
    }

    /// Performs the listen on operation.
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

    /// Performs the dial operation.
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

    /// Performs the connected peer count operation.
    pub fn connected_peer_count(&self) -> usize {
        self.swarm.network_info().num_peers()
    }

    /// Performs the publish control operation.
    pub fn publish_control(&mut self, announcement: ControlAnnouncement) {
        push_unique(&mut self.snapshot.control_announcements, announcement);
    }

    /// Performs the publish head operation.
    pub fn publish_head(&mut self, announcement: HeadAnnouncement) {
        push_unique(&mut self.snapshot.head_announcements, announcement);
    }

    /// Performs the publish lease operation.
    pub fn publish_lease(&mut self, announcement: LeaseAnnouncement) {
        push_unique(&mut self.snapshot.lease_announcements, announcement);
    }

    /// Performs the publish merge operation.
    pub fn publish_merge(&mut self, announcement: MergeAnnouncement) {
        push_unique(&mut self.snapshot.merge_announcements, announcement);
    }

    /// Performs the publish merge window operation.
    pub fn publish_merge_window(&mut self, announcement: MergeWindowAnnouncement) {
        push_unique(&mut self.snapshot.merge_window_announcements, announcement);
    }

    /// Performs the publish reducer assignment operation.
    pub fn publish_reducer_assignment(&mut self, announcement: ReducerAssignmentAnnouncement) {
        push_unique(
            &mut self.snapshot.reducer_assignment_announcements,
            announcement,
        );
    }

    /// Performs the publish update operation.
    pub fn publish_update(&mut self, announcement: UpdateEnvelopeAnnouncement) {
        push_unique(&mut self.snapshot.update_announcements, announcement);
    }

    /// Performs the publish aggregate operation.
    pub fn publish_aggregate(&mut self, announcement: AggregateAnnouncement) {
        push_unique(&mut self.snapshot.aggregate_announcements, announcement);
    }

    /// Performs the publish reduction certificate operation.
    pub fn publish_reduction_certificate(
        &mut self,
        announcement: ReductionCertificateAnnouncement,
    ) {
        push_unique(
            &mut self.snapshot.reduction_certificate_announcements,
            announcement,
        );
    }

    /// Performs the publish reducer load operation.
    pub fn publish_reducer_load(&mut self, announcement: ReducerLoadAnnouncement) {
        push_unique(&mut self.snapshot.reducer_load_announcements, announcement);
    }

    /// Performs the publish auth operation.
    pub fn publish_auth(&mut self, announcement: PeerAuthAnnouncement) {
        push_unique(&mut self.snapshot.auth_announcements, announcement);
    }

    /// Performs the publish directory operation.
    pub fn publish_directory(&mut self, announcement: ExperimentDirectoryAnnouncement) {
        push_unique(&mut self.snapshot.directory_announcements, announcement);
    }

    /// Performs the publish metrics operation.
    pub fn publish_metrics(&mut self, announcement: MetricsAnnouncement) {
        push_metrics_announcement(&mut self.snapshot.metrics_announcements, announcement);
    }

    /// Performs the snapshot operation.
    pub fn snapshot(&self) -> &ControlPlaneSnapshot {
        &self.snapshot
    }

    /// Performs the subscribe topic operation.
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

    /// Performs the publish pubsub operation.
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

    /// Performs the request snapshot operation.
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

    /// Performs the request artifact manifest operation.
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

    /// Performs the request artifact chunk operation.
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

    /// Fetches the snapshot.
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

    /// Fetches the artifact manifest.
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

    /// Fetches the artifact chunk.
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

    /// Performs the wait event operation.
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

    /// Performs the publish lease operation.
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

    pub fn publish_metrics(&mut self, announcement: MetricsAnnouncement) {
        self.inner.publish_metrics(announcement);
    }

    pub fn snapshot(&self) -> &ControlPlaneSnapshot {
        self.inner.snapshot()
    }

    /// Performs the subscribe topic operation.
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

    /// Performs the publish artifact operation.
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
