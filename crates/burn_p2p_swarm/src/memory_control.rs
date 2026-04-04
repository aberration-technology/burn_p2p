use super::*;

pub struct MemoryControlPlaneShell {
    local_peer_id: Libp2pPeerId,
    swarm: Swarm<request_response::json::Behaviour<ControlPlaneRequest, ControlPlaneResponse>>,
    snapshot: ControlPlaneSnapshot,
    artifacts: BTreeMap<ArtifactId, ArtifactDescriptor>,
    chunks: BTreeMap<(ArtifactId, ChunkId), ArtifactChunkPayload>,
    subscribed_topics: BTreeSet<String>,
}

impl MemoryControlPlaneShell {
    /// Creates a new value.
    pub fn new(control_protocol: ProtocolId) -> Self {
        Self::with_keypair(control_protocol, Keypair::generate_ed25519())
    }

    /// Returns a copy configured with the keypair.
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

    /// Performs the request snapshot operation.
    pub fn request_snapshot(&mut self, peer_id: &str) -> Result<(), SwarmError> {
        let peer_id = peer_id
            .parse::<Libp2pPeerId>()
            .map_err(|_| SwarmError::InvalidPeerId(peer_id.to_owned()))?;
        self.swarm
            .behaviour_mut()
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
        self.swarm.behaviour_mut().send_request(
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
        self.swarm.behaviour_mut().send_request(
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

    /// Fetches the artifact chunk.
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

    /// Performs the wait event operation.
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
