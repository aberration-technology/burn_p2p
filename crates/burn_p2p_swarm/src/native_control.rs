use super::*;
#[cfg(not(target_arch = "wasm32"))]
use libp2p_webrtc::tokio::{Certificate as WebRtcCertificate, Transport as WebRtcTransport};
#[cfg(not(target_arch = "wasm32"))]
use rand::thread_rng;

#[cfg(not(target_arch = "wasm32"))]
pub struct NativeControlPlaneShell {
    runtime: TokioRuntime,
    local_peer_id: Libp2pPeerId,
    transport_policy: RuntimeTransportPolicy,
    swarm: Swarm<NativeControlPlaneBehaviour>,
    snapshot: ControlPlaneSnapshot,
    hot_index: ControlPlaneHotIndex,
    artifacts: BTreeMap<ArtifactId, ArtifactDescriptor>,
    chunks: BTreeMap<(ArtifactId, ChunkId), ArtifactChunkPayload>,
    relay_reservation_requests: BTreeSet<SwarmAddress>,
    next_kademlia_refresh_at: Instant,
    kademlia_walk_round: u64,
    peer_directory_record_lookups: BTreeMap<Libp2pPeerId, Instant>,
    rendezvous_namespace: Option<rendezvous::Namespace>,
    rendezvous_known_servers: BTreeSet<Libp2pPeerId>,
    rendezvous_discovery_cookies: BTreeMap<Libp2pPeerId, rendezvous::Cookie>,
    next_rendezvous_refresh_at: Instant,
    subscribed_topics: BTreeSet<String>,
    pending_events: VecDeque<LiveControlPlaneEvent>,
}

#[cfg(not(target_arch = "wasm32"))]
const RENDEZVOUS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(not(target_arch = "wasm32"))]
const KADEMLIA_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(not(target_arch = "wasm32"))]
const KADEMLIA_REFRESH_DEBOUNCE: Duration = Duration::from_secs(2);
#[cfg(not(target_arch = "wasm32"))]
const PEER_DIRECTORY_RECORD_LOOKUP_DEBOUNCE: Duration = Duration::from_secs(30);
#[cfg(not(target_arch = "wasm32"))]
const PEER_DIRECTORY_RECORD_TTL: Duration = Duration::from_secs(90);

#[cfg(not(target_arch = "wasm32"))]
fn build_native_webrtc_transport(
    keypair: &Keypair,
) -> Result<WebRtcTransport, Box<dyn std::error::Error + Send + Sync>> {
    let certificate = WebRtcCertificate::generate(&mut thread_rng())?;
    Ok(WebRtcTransport::new(keypair.clone(), certificate))
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeControlPlaneShell {
    /// Creates a new value.
    pub fn new(
        control_protocol: ProtocolId,
        transport_policy: RuntimeTransportPolicy,
    ) -> Result<Self, SwarmError> {
        Self::with_keypair(
            control_protocol,
            Keypair::generate_ed25519(),
            transport_policy,
        )
    }

    /// Returns a copy configured with the keypair.
    pub fn with_keypair(
        control_protocol: ProtocolId,
        keypair: Keypair,
        transport_policy: RuntimeTransportPolicy,
    ) -> Result<Self, SwarmError> {
        let runtime = TokioRuntimeBuilder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|error| SwarmError::Runtime(error.to_string()))?;
        let behaviour_keypair = keypair.clone();
        let local_peer_id = keypair.public().to_peer_id();
        let protocol = stream_protocol(&control_protocol)?;
        let kademlia_protocol = if transport_policy.enable_kademlia {
            Some(kademlia_protocol_for_control_protocol(&control_protocol)?)
        } else {
            None
        };
        let rendezvous_namespace = if transport_policy.enable_rendezvous_client
            || transport_policy.enable_rendezvous_server
        {
            Some(rendezvous_namespace_for_control_protocol(
                &control_protocol,
            )?)
        } else {
            None
        };
        let gossip_config = gossipsub::ConfigBuilder::default()
            // Control-plane pubsub always signs messages, so require the full
            // libp2p gossipsub envelope instead of accepting unsigned or partial
            // metadata from permissive peers.
            .validation_mode(gossipsub::ValidationMode::Strict)
            .message_id_fn(|message| {
                gossipsub::MessageId::from(pubsub_semantic_message_id(&message.data))
            })
            .build()
            .map_err(|error| SwarmError::Runtime(error.to_string()))?;
        let identify_config = identify::Config::new(
            format!("{}/identify/1.0.0", control_protocol.as_str()),
            keypair.public(),
        );
        let gossipsub_behaviour = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(behaviour_keypair),
            gossip_config.clone(),
        )
        .map_err(|error| SwarmError::Runtime(error.to_string()))?;
        let kademlia_behaviour = if let Some(protocol_name) = kademlia_protocol {
            let mut config = kad::Config::new(protocol_name);
            config.set_periodic_bootstrap_interval(Some(Duration::from_secs(60)));
            let mut behaviour = kad::Behaviour::with_config(
                local_peer_id,
                kad::store::MemoryStore::new(local_peer_id),
                config,
            );
            if transport_policy.enable_relay_server {
                behaviour.set_mode(Some(kad::Mode::Server));
            }
            Some(behaviour)
        } else {
            None
        };
        let rendezvous_client_behaviour = if transport_policy.enable_rendezvous_client {
            Some(rendezvous::client::Behaviour::new(keypair.clone()))
        } else {
            None
        };
        let rendezvous_server_behaviour = if transport_policy.enable_rendezvous_server {
            Some(rendezvous::server::Behaviour::new(
                rendezvous::server::Config::default(),
            ))
        } else {
            None
        };
        let relay_server_behaviour = if transport_policy.enable_relay_server {
            let mut config = relay::Config::default();
            if let Some(max_incoming) = transport_policy.max_established_incoming {
                config.max_reservations = max_incoming as usize;
            }
            if let Some(max_total) = transport_policy.max_established_total {
                config.max_circuits = (max_total as usize).max(1) / 4;
            }
            Some(relay::Behaviour::new(local_peer_id, config))
        } else {
            None
        };
        let dcutr_behaviour = if transport_policy.enable_hole_punching {
            Some(dcutr::Behaviour::new(local_peer_id))
        } else {
            None
        };
        let autonat_behaviour = if transport_policy.enable_autonat {
            let config = autonat::Config {
                boot_delay: Duration::from_secs(2),
                retry_interval: Duration::from_secs(10),
                refresh_interval: Duration::from_secs(60),
                ..autonat::Config::default()
            };
            Some(autonat::Behaviour::new(local_peer_id, config))
        } else {
            None
        };
        #[cfg(not(target_arch = "wasm32"))]
        let mdns_behaviour = if transport_policy.enable_local_discovery {
            let _guard = runtime.enter();
            Some(
                mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?,
            )
        } else {
            None
        };
        let swarm = runtime.block_on(async move {
            Ok::<_, SwarmError>(
                SwarmBuilder::with_existing_identity(keypair)
                    .with_tokio()
                    .with_tcp(
                        libp2p::tcp::Config::default(),
                        tls_config,
                        yamux::Config::default,
                    )
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .with_quic()
                    .with_other_transport(|key| build_native_webrtc_transport(key))
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .with_dns()
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .with_websocket(tls_config, yamux::Config::default)
                    .await
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .with_relay_client(tls_config, yamux::Config::default)
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .with_behaviour(move |_, relay_client| NativeControlPlaneBehaviour {
                        request_response: request_response::cbor::Behaviour::new(
                            [(protocol, ProtocolSupport::Full)],
                            request_response::Config::default(),
                        ),
                        gossipsub: gossipsub_behaviour,
                        identify: identify::Behaviour::new(identify_config.clone()),
                        kademlia: kademlia_behaviour.into(),
                        rendezvous_client: rendezvous_client_behaviour.into(),
                        rendezvous_server: rendezvous_server_behaviour.into(),
                        relay_client,
                        relay_server: relay_server_behaviour.into(),
                        dcutr: dcutr_behaviour.into(),
                        autonat: autonat_behaviour.into(),
                        ping: ping::Behaviour::default(),
                        connection_limits: connection_limits::Behaviour::new(
                            connection_limits::ConnectionLimits::default()
                                .with_max_established_incoming(
                                    transport_policy.max_established_incoming,
                                )
                                .with_max_established(transport_policy.max_established_total)
                                .with_max_established_per_peer(
                                    transport_policy.max_established_per_peer,
                                ),
                        ),
                        #[cfg(not(target_arch = "wasm32"))]
                        mdns: mdns_behaviour.into(),
                    })
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .build(),
            )
        })?;

        Ok(Self {
            runtime,
            local_peer_id,
            transport_policy,
            swarm,
            snapshot: ControlPlaneSnapshot::default(),
            hot_index: ControlPlaneHotIndex::default(),
            artifacts: BTreeMap::new(),
            chunks: BTreeMap::new(),
            relay_reservation_requests: BTreeSet::new(),
            next_kademlia_refresh_at: Instant::now(),
            kademlia_walk_round: 0,
            peer_directory_record_lookups: BTreeMap::new(),
            rendezvous_namespace,
            rendezvous_known_servers: BTreeSet::new(),
            rendezvous_discovery_cookies: BTreeMap::new(),
            next_rendezvous_refresh_at: Instant::now(),
            subscribed_topics: BTreeSet::new(),
            pending_events: VecDeque::new(),
        })
    }

    /// Performs the local peer ID operation.
    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        &self.local_peer_id
    }

    fn maybe_request_relay_reservation(
        swarm: &mut Swarm<NativeControlPlaneBehaviour>,
        transport_policy: &RuntimeTransportPolicy,
        relay_reservation_requests: &mut BTreeSet<SwarmAddress>,
        pending_events: &mut VecDeque<LiveControlPlaneEvent>,
        relay_peer_id: &Libp2pPeerId,
        listen_addresses: &[Multiaddr],
    ) {
        if !transport_policy.enable_relay_client
            || transport_policy.enable_relay_server
            || !relay_reservation_requests.is_empty()
        {
            return;
        }

        let Some(relay_listen_addr) =
            relay_reservation_listen_addr(relay_peer_id, listen_addresses)
        else {
            return;
        };
        let relay_listen_addr = SwarmAddress(relay_listen_addr.to_string());
        if !relay_reservation_requests.insert(relay_listen_addr.clone()) {
            return;
        }

        let relay_multiaddr: Multiaddr = relay_listen_addr
            .as_str()
            .parse()
            .expect("relay reservation address should remain valid");
        let result = swarm.listen_on(relay_multiaddr);
        if let Err(error) = result {
            relay_reservation_requests.remove(&relay_listen_addr);
            pending_events.push_back(LiveControlPlaneEvent::Other {
                kind: format!("relay-reservation-listen-error:{error}"),
            });
        }
    }

    fn refresh_rendezvous_server(
        swarm: &mut Swarm<NativeControlPlaneBehaviour>,
        transport_policy: &RuntimeTransportPolicy,
        namespace: Option<&rendezvous::Namespace>,
        rendezvous_discovery_cookies: &mut BTreeMap<Libp2pPeerId, rendezvous::Cookie>,
        pending_events: &mut VecDeque<LiveControlPlaneEvent>,
        rendezvous_peer_id: Libp2pPeerId,
    ) {
        if !transport_policy.enable_rendezvous_client {
            return;
        }
        let Some(namespace) = namespace.cloned() else {
            return;
        };
        let Some(rendezvous_client) = swarm.behaviour_mut().rendezvous_client.as_mut() else {
            return;
        };

        if let Err(error) = rendezvous_client.register(namespace.clone(), rendezvous_peer_id, None)
        {
            match error {
                rendezvous::client::RegisterError::NoExternalAddresses => {}
                other => pending_events.push_back(LiveControlPlaneEvent::Other {
                    kind: format!("rendezvous-register-error:{other}"),
                }),
            }
        }

        let cookie = rendezvous_discovery_cookies
            .get(&rendezvous_peer_id)
            .cloned();
        rendezvous_client.discover(Some(namespace), cookie, Some(128), rendezvous_peer_id);
    }

    fn note_kademlia_addresses(
        swarm: &mut Swarm<NativeControlPlaneBehaviour>,
        peer_id: &Libp2pPeerId,
        addresses: impl IntoIterator<Item = Multiaddr>,
    ) {
        let Some(kademlia) = swarm.behaviour_mut().kademlia.as_mut() else {
            return;
        };
        for address in addresses {
            kademlia.add_address(peer_id, address);
        }
    }

    fn publish_peer_directory_record(
        swarm: &mut Swarm<NativeControlPlaneBehaviour>,
        announcement: &PeerDirectoryAnnouncement,
        pending_events: &mut VecDeque<LiveControlPlaneEvent>,
    ) {
        let local_peer_id = *swarm.local_peer_id();
        let Some(kademlia) = swarm.behaviour_mut().kademlia.as_mut() else {
            return;
        };
        let value = match serde_json::to_vec(announcement) {
            Ok(value) => value,
            Err(error) => {
                pending_events.push_back(LiveControlPlaneEvent::Other {
                    kind: format!("kademlia-peer-directory-encode-error:{error}"),
                });
                return;
            }
        };
        let mut record = kad::Record::new(
            peer_directory_record_key_for_peer(announcement.peer_id.as_str()),
            value,
        );
        record.publisher = Some(local_peer_id);
        record.expires = Some(Instant::now() + PEER_DIRECTORY_RECORD_TTL);
        if let Err(error) = kademlia.put_record(record, kad::Quorum::One) {
            pending_events.push_back(LiveControlPlaneEvent::Other {
                kind: format!("kademlia-peer-directory-put-error:{error}"),
            });
        }
    }

    fn maybe_request_peer_directory_record(
        swarm: &mut Swarm<NativeControlPlaneBehaviour>,
        local_peer_id: &Libp2pPeerId,
        peer_directory_record_lookups: &mut BTreeMap<Libp2pPeerId, Instant>,
        peer_id: &Libp2pPeerId,
    ) {
        if peer_id == local_peer_id {
            return;
        }
        let now = Instant::now();
        if peer_directory_record_lookups
            .get(peer_id)
            .is_some_and(|last| *last + PEER_DIRECTORY_RECORD_LOOKUP_DEBOUNCE > now)
        {
            return;
        }
        let Some(kademlia) = swarm.behaviour_mut().kademlia.as_mut() else {
            return;
        };
        kademlia.get_record(peer_directory_record_key_for_peer(&peer_id.to_string()));
        peer_directory_record_lookups.insert(*peer_id, now);
    }

    fn refresh_kademlia_discovery(
        swarm: &mut Swarm<NativeControlPlaneBehaviour>,
        transport_policy: &RuntimeTransportPolicy,
        local_peer_id: Libp2pPeerId,
        kademlia_walk_round: &mut u64,
        pending_events: &mut VecDeque<LiveControlPlaneEvent>,
    ) {
        if !transport_policy.enable_kademlia {
            return;
        }
        let Some(kademlia) = swarm.behaviour_mut().kademlia.as_mut() else {
            return;
        };
        if let Err(error) = kademlia.bootstrap()
            && !matches!(error, kad::NoKnownPeers())
        {
            pending_events.push_back(LiveControlPlaneEvent::Other {
                kind: format!("kademlia-bootstrap-error:{error}"),
            });
        }
        kademlia.get_closest_peers(local_peer_id);
        kademlia.get_closest_peers(
            format!(
                "burn-p2p-discovery-walk:{local_peer_id}:{round}",
                round = *kademlia_walk_round
            )
            .into_bytes(),
        );
        *kademlia_walk_round = kademlia_walk_round.wrapping_add(1);
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

    /// Registers one externally reachable address with the native swarm.
    pub fn add_external_address(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        let address: Multiaddr = address
            .as_str()
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(address.as_str().to_owned()))?;
        self.swarm.add_external_address(address);
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
            .insert_peer_directory_announcement(announcement.clone());
        Self::publish_peer_directory_record(
            &mut self.swarm,
            &announcement,
            &mut self.pending_events,
        );
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
            .request_response
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
            .request_response
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
            .request_response
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

    fn maybe_refresh_background_discovery(&mut self) {
        if self.transport_policy.enable_kademlia && Instant::now() >= self.next_kademlia_refresh_at
        {
            Self::refresh_kademlia_discovery(
                &mut self.swarm,
                &self.transport_policy,
                self.local_peer_id,
                &mut self.kademlia_walk_round,
                &mut self.pending_events,
            );
            self.next_kademlia_refresh_at = Instant::now() + KADEMLIA_REFRESH_INTERVAL;
        }

        if self.transport_policy.enable_rendezvous_client
            && Instant::now() >= self.next_rendezvous_refresh_at
        {
            let known_servers = self
                .rendezvous_known_servers
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            for rendezvous_peer_id in known_servers {
                Self::refresh_rendezvous_server(
                    &mut self.swarm,
                    &self.transport_policy,
                    self.rendezvous_namespace.as_ref(),
                    &mut self.rendezvous_discovery_cookies,
                    &mut self.pending_events,
                    rendezvous_peer_id,
                );
            }
            self.next_rendezvous_refresh_at = Instant::now() + RENDEZVOUS_REFRESH_INTERVAL;
        }
    }

    fn wait_live_event(&mut self, duration: Duration) -> Option<LiveControlPlaneEvent> {
        self.maybe_refresh_background_discovery();
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
                                request_response::Message::Response {
                                    request_id,
                                    response,
                                } => match response {
                                    ControlPlaneResponse::Snapshot(snapshot) => {
                                        LiveControlPlaneEvent::SnapshotReceived {
                                            peer_id: peer.to_string(),
                                            request_id: request_id.to_string(),
                                            snapshot,
                                        }
                                    }
                                    ControlPlaneResponse::ArtifactManifest(descriptor) => {
                                        LiveControlPlaneEvent::ArtifactManifestReceived {
                                            peer_id: peer.to_string(),
                                            request_id: request_id.to_string(),
                                            descriptor,
                                        }
                                    }
                                    ControlPlaneResponse::ArtifactChunk(payload) => {
                                        LiveControlPlaneEvent::ArtifactChunkReceived {
                                            peer_id: peer.to_string(),
                                            request_id: request_id.to_string(),
                                            payload,
                                        }
                                    }
                                },
                            },
                            request_response::Event::OutboundFailure {
                                peer,
                                request_id,
                                error,
                                ..
                            } => LiveControlPlaneEvent::RequestFailure {
                                peer_id: peer.to_string(),
                                request_id: Some(request_id.to_string()),
                                message: error.to_string(),
                            },
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
                                    apply_pubsub_payload_with_index(
                                        &mut self.snapshot,
                                        &mut self.hot_index,
                                        envelope.payload,
                                    );
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
                                let listen_addrs = info.listen_addrs;
                                let observed_addr = info.observed_addr;
                                let protocols = info
                                    .protocols
                                    .into_iter()
                                    .map(|protocol| protocol.to_string())
                                    .collect::<Vec<_>>();
                                let relay_hop_supported = protocol_supports_relay_hop(&protocols);
                                let rendezvous_supported =
                                    protocol_supports_rendezvous(&protocols);
                                self.swarm
                                    .behaviour_mut()
                                    .gossipsub
                                    .add_explicit_peer(&peer_id);
                                self.swarm.add_external_address(observed_addr);
                                Self::note_kademlia_addresses(
                                    &mut self.swarm,
                                    &peer_id,
                                    listen_addrs.iter().cloned(),
                                );
                                if self.transport_policy.enable_kademlia {
                                    let scheduled =
                                        Instant::now() + KADEMLIA_REFRESH_DEBOUNCE;
                                    if self.next_kademlia_refresh_at > scheduled {
                                        self.next_kademlia_refresh_at = scheduled;
                                    }
                                }
                                if rendezvous_supported {
                                    self.rendezvous_known_servers.insert(peer_id);
                                    Self::refresh_rendezvous_server(
                                        &mut self.swarm,
                                        &self.transport_policy,
                                        self.rendezvous_namespace.as_ref(),
                                        &mut self.rendezvous_discovery_cookies,
                                        &mut self.pending_events,
                                        peer_id,
                                    );
                                } else {
                                    self.rendezvous_known_servers.remove(&peer_id);
                                    self.rendezvous_discovery_cookies.remove(&peer_id);
                                }
                                if relay_hop_supported {
                                    if self.transport_policy.enable_autonat
                                        && let Some(address) = listen_addrs.first().cloned()
                                        && let Some(autonat) =
                                            self.swarm.behaviour_mut().autonat.as_mut()
                                    {
                                        autonat.add_server(peer_id, Some(address));
                                    }
                                    Self::maybe_request_relay_reservation(
                                        &mut self.swarm,
                                        &self.transport_policy,
                                        &mut self.relay_reservation_requests,
                                        &mut self.pending_events,
                                        &peer_id,
                                        &listen_addrs,
                                    );
                                }
                                LiveControlPlaneEvent::PeerIdentified {
                                    peer_id: peer_id.to_string(),
                                    listen_addresses: listen_addrs
                                        .into_iter()
                                        .map(|address| SwarmAddress(address.to_string()))
                                        .collect(),
                                    protocols,
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
                        NativeControlPlaneBehaviourEvent::Kademlia(event) => match *event {
                            kad::Event::RoutingUpdated {
                                peer, addresses, ..
                            } => {
                                Self::maybe_request_peer_directory_record(
                                    &mut self.swarm,
                                    &self.local_peer_id,
                                    &mut self.peer_directory_record_lookups,
                                    &peer,
                                );
                                LiveControlPlaneEvent::PeersDiscovered {
                                    peers: addresses
                                        .into_vec()
                                        .into_iter()
                                        .map(|address| {
                                            (peer.to_string(), SwarmAddress(address.to_string()))
                                        })
                                        .collect(),
                                }
                            }
                            kad::Event::OutboundQueryProgressed { result, .. } => match result {
                                kad::QueryResult::GetClosestPeers(Ok(ok)) => {
                                    let mut discovered = BTreeSet::new();
                                    for peer in ok.peers {
                                        Self::maybe_request_peer_directory_record(
                                            &mut self.swarm,
                                            &self.local_peer_id,
                                            &mut self.peer_directory_record_lookups,
                                            &peer.peer_id,
                                        );
                                        for address in peer.addrs {
                                            discovered.insert((
                                                peer.peer_id.to_string(),
                                                SwarmAddress(address.to_string()),
                                            ));
                                        }
                                    }
                                    LiveControlPlaneEvent::PeersDiscovered {
                                        peers: discovered.into_iter().collect(),
                                    }
                                }
                                kad::QueryResult::GetClosestPeers(Err(error)) => {
                                    let mut discovered = BTreeSet::new();
                                    match error {
                                        kad::GetClosestPeersError::Timeout { peers, .. } => {
                                            for peer in peers {
                                                Self::maybe_request_peer_directory_record(
                                                    &mut self.swarm,
                                                    &self.local_peer_id,
                                                    &mut self.peer_directory_record_lookups,
                                                    &peer.peer_id,
                                                );
                                                for address in peer.addrs {
                                                    discovered.insert((
                                                        peer.peer_id.to_string(),
                                                        SwarmAddress(address.to_string()),
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                    if discovered.is_empty() {
                                        LiveControlPlaneEvent::Other {
                                            kind: "kademlia-get-closest-timeout".into(),
                                        }
                                    } else {
                                        LiveControlPlaneEvent::PeersDiscovered {
                                            peers: discovered.into_iter().collect(),
                                        }
                                    }
                                }
                                kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(
                                    peer_record,
                                ))) => {
                                    let record_key = peer_record.record.key.clone();
                                    match serde_json::from_slice::<PeerDirectoryAnnouncement>(
                                        &peer_record.record.value,
                                    ) {
                                        Ok(announcement)
                                            if peer_directory_record_key_for_peer(
                                                announcement.peer_id.as_str(),
                                            ) == record_key =>
                                        {
                                            if let Ok(peer_id) = announcement
                                                .peer_id
                                                .as_str()
                                                .parse::<Libp2pPeerId>()
                                            {
                                                let addresses = announcement
                                                    .addresses
                                                    .iter()
                                                    .filter_map(|address| {
                                                        address.as_str().parse::<Multiaddr>().ok()
                                                    })
                                                    .collect::<Vec<_>>();
                                                Self::note_kademlia_addresses(
                                                    &mut self.swarm,
                                                    &peer_id,
                                                    addresses,
                                                );
                                            }
                                            self.snapshot.insert_peer_directory_announcement(
                                                announcement.clone(),
                                            );
                                            LiveControlPlaneEvent::PeerDirectoryRecordReceived {
                                                announcement,
                                            }
                                        }
                                        Ok(announcement) => LiveControlPlaneEvent::Other {
                                            kind: format!(
                                                "kademlia-peer-directory-key-mismatch:{}",
                                                announcement.peer_id.as_str()
                                            ),
                                        },
                                        Err(error) => LiveControlPlaneEvent::Other {
                                            kind: format!(
                                                "kademlia-peer-directory-decode-error:{error}"
                                            ),
                                        },
                                    }
                                }
                                kad::QueryResult::GetRecord(Ok(
                                    kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. },
                                )) => LiveControlPlaneEvent::Other {
                                    kind: "kademlia-peer-directory-record-finished".into(),
                                },
                                kad::QueryResult::GetRecord(Err(error)) => {
                                    LiveControlPlaneEvent::Other {
                                        kind: format!("kademlia-peer-directory-record:{error:?}"),
                                    }
                                }
                                other => LiveControlPlaneEvent::Other {
                                    kind: format!("kademlia:{other:?}"),
                                },
                            },
                            other => LiveControlPlaneEvent::Other {
                                kind: format!("kademlia:{other:?}"),
                            },
                        },
                        NativeControlPlaneBehaviourEvent::RendezvousClient(event) => match *event {
                            rendezvous::client::Event::Discovered {
                                rendezvous_node,
                                registrations,
                                cookie,
                            } => {
                                let mut discovered = BTreeSet::new();
                                self.rendezvous_discovery_cookies
                                    .insert(rendezvous_node, cookie);
                                for registration in registrations {
                                    let peer_id = registration.record.peer_id();
                                    if peer_id == self.local_peer_id {
                                        continue;
                                    }
                                    Self::maybe_request_peer_directory_record(
                                        &mut self.swarm,
                                        &self.local_peer_id,
                                        &mut self.peer_directory_record_lookups,
                                        &peer_id,
                                    );
                                    Self::note_kademlia_addresses(
                                        &mut self.swarm,
                                        &peer_id,
                                        registration.record.addresses().iter().cloned(),
                                    );
                                    let peer_id = peer_id.to_string();
                                    for address in registration.record.addresses() {
                                        discovered.insert((
                                            peer_id.clone(),
                                            SwarmAddress(address.to_string()),
                                        ));
                                    }
                                }
                                LiveControlPlaneEvent::PeersDiscovered {
                                    peers: discovered.into_iter().collect(),
                                }
                            }
                            rendezvous::client::Event::Registered {
                                rendezvous_node,
                                namespace,
                                ttl,
                            } => LiveControlPlaneEvent::Other {
                                kind: format!(
                                    "rendezvous-registered:{rendezvous_node}:{namespace}:{ttl}"
                                ),
                            },
                            rendezvous::client::Event::DiscoverFailed {
                                rendezvous_node,
                                namespace,
                                error,
                            } => LiveControlPlaneEvent::Other {
                                kind: format!(
                                    "rendezvous-discover-failed:{rendezvous_node}:{}:{error:?}",
                                    namespace
                                        .map(|namespace| namespace.to_string())
                                        .unwrap_or_else(|| "all".into())
                                ),
                            },
                            rendezvous::client::Event::RegisterFailed {
                                rendezvous_node,
                                namespace,
                                error,
                            } => LiveControlPlaneEvent::Other {
                                kind: format!(
                                    "rendezvous-register-failed:{rendezvous_node}:{namespace}:{error:?}"
                                ),
                            },
                            rendezvous::client::Event::Expired { peer } => {
                                LiveControlPlaneEvent::Other {
                                    kind: format!("rendezvous-expired:{peer}"),
                                }
                            }
                        },
                        NativeControlPlaneBehaviourEvent::RendezvousServer(event) => {
                            LiveControlPlaneEvent::Other {
                                kind: format!("rendezvous-server:{event:?}"),
                            }
                        }
                        NativeControlPlaneBehaviourEvent::RelayClient(event) => match *event {
                            relay::client::Event::ReservationReqAccepted {
                                relay_peer_id, ..
                            } => LiveControlPlaneEvent::RelayReservationAccepted {
                                relay_peer_id: relay_peer_id.to_string(),
                            },
                            other => LiveControlPlaneEvent::Other {
                                kind: format!("relay-client:{other:?}"),
                            },
                        },
                        NativeControlPlaneBehaviourEvent::RelayServer(event) => {
                            LiveControlPlaneEvent::Other {
                                kind: format!("relay-server:{event:?}"),
                            }
                        }
                        NativeControlPlaneBehaviourEvent::Dcutr(event) => match event.result {
                            Ok(_) => LiveControlPlaneEvent::DirectConnectionUpgradeSucceeded {
                                peer_id: event.remote_peer_id.to_string(),
                            },
                            Err(error) => LiveControlPlaneEvent::DirectConnectionUpgradeFailed {
                                peer_id: event.remote_peer_id.to_string(),
                                message: error.to_string(),
                            },
                        },
                        NativeControlPlaneBehaviourEvent::Autonat(event) => match *event {
                            autonat::Event::StatusChanged { old, new } => {
                                if let autonat::NatStatus::Public(previous) = old {
                                    self.pending_events.push_back(
                                        LiveControlPlaneEvent::ReachableAddressExpired {
                                            address: SwarmAddress(previous.to_string()),
                                        },
                                    );
                                }
                                match new {
                                    autonat::NatStatus::Public(address) => {
                                        self.swarm.add_external_address(address.clone());
                                        LiveControlPlaneEvent::ReachableAddressConfirmed {
                                            address: SwarmAddress(address.to_string()),
                                        }
                                    }
                                    autonat::NatStatus::Private | autonat::NatStatus::Unknown => {
                                        LiveControlPlaneEvent::Other {
                                            kind: format!("autonat:{new:?}"),
                                        }
                                    }
                                }
                            }
                            other => LiveControlPlaneEvent::Other {
                                kind: format!("autonat:{other:?}"),
                            },
                        },
                        NativeControlPlaneBehaviourEvent::Ping(event) => {
                            LiveControlPlaneEvent::Other {
                                kind: format!("ping:{event:?}"),
                            }
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        NativeControlPlaneBehaviourEvent::Mdns(event) => match event {
                            mdns::Event::Discovered(peers) => {
                                let mut discovered = Vec::new();
                                for (peer_id, address) in peers {
                                    self.swarm
                                        .behaviour_mut()
                                        .gossipsub
                                        .add_explicit_peer(&peer_id);
                                    Self::note_kademlia_addresses(
                                        &mut self.swarm,
                                        &peer_id,
                                        std::iter::once(address.clone()),
                                    );
                                    let _ = self.swarm.dial(address.clone());
                                    discovered.push((
                                        peer_id.to_string(),
                                        SwarmAddress(address.to_string()),
                                    ));
                                }
                                if self.transport_policy.enable_kademlia {
                                    let scheduled =
                                        Instant::now() + KADEMLIA_REFRESH_DEBOUNCE;
                                    if self.next_kademlia_refresh_at > scheduled {
                                        self.next_kademlia_refresh_at = scheduled;
                                    }
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
                        if address
                            .iter()
                            .any(|protocol| matches!(protocol, libp2p::multiaddr::Protocol::P2pCircuit))
                        {
                            self.swarm.add_external_address(address.clone());
                        }
                        LiveControlPlaneEvent::NewListenAddr {
                            address: SwarmAddress(address.to_string()),
                        }
                    }
                    SwarmEvent::ExternalAddrConfirmed { address } => {
                        let address = SwarmAddress(address.to_string());
                        if address.is_relay_circuit() {
                            self.relay_reservation_requests.insert(address.clone());
                        }
                        if self.transport_policy.enable_kademlia {
                            let scheduled = Instant::now() + KADEMLIA_REFRESH_DEBOUNCE;
                            if self.next_kademlia_refresh_at > scheduled {
                                self.next_kademlia_refresh_at = scheduled;
                            }
                        }
                        let known_servers =
                            self.rendezvous_known_servers.iter().cloned().collect::<Vec<_>>();
                        for rendezvous_peer_id in known_servers {
                            Self::refresh_rendezvous_server(
                                &mut self.swarm,
                                &self.transport_policy,
                                self.rendezvous_namespace.as_ref(),
                                &mut self.rendezvous_discovery_cookies,
                                &mut self.pending_events,
                                rendezvous_peer_id,
                            );
                        }
                        LiveControlPlaneEvent::ReachableAddressConfirmed { address }
                    }
                    SwarmEvent::ExternalAddrExpired { address } => {
                        let address = SwarmAddress(address.to_string());
                        if address.is_relay_circuit() {
                            self.relay_reservation_requests.remove(&address);
                        }
                        if self.transport_policy.enable_kademlia {
                            let scheduled = Instant::now() + KADEMLIA_REFRESH_DEBOUNCE;
                            if self.next_kademlia_refresh_at > scheduled {
                                self.next_kademlia_refresh_at = scheduled;
                            }
                        }
                        if self.transport_policy.enable_rendezvous_client
                            && self.next_rendezvous_refresh_at > Instant::now()
                        {
                            self.next_rendezvous_refresh_at = Instant::now();
                        }
                        LiveControlPlaneEvent::ReachableAddressExpired { address }
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        LiveControlPlaneEvent::ConnectionEstablished {
                            peer_id: peer_id.to_string(),
                        }
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        if self.transport_policy.enable_kademlia {
                            let scheduled = Instant::now() + KADEMLIA_REFRESH_DEBOUNCE;
                            if self.next_kademlia_refresh_at > scheduled {
                                self.next_kademlia_refresh_at = scheduled;
                            }
                        }
                        if self.transport_policy.enable_rendezvous_client
                            && self.next_rendezvous_refresh_at > Instant::now()
                        {
                            self.next_rendezvous_refresh_at = Instant::now();
                        }
                        LiveControlPlaneEvent::ConnectionClosed {
                            peer_id: peer_id.to_string(),
                        }
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        if self.transport_policy.enable_kademlia {
                            let scheduled = Instant::now() + KADEMLIA_REFRESH_DEBOUNCE;
                            if self.next_kademlia_refresh_at > scheduled {
                                self.next_kademlia_refresh_at = scheduled;
                            }
                        }
                        if self.transport_policy.enable_rendezvous_client
                            && self.next_rendezvous_refresh_at > Instant::now()
                        {
                            self.next_rendezvous_refresh_at = Instant::now();
                        }
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
    pub fn wait_event(&mut self, duration: Duration) -> Option<LiveControlPlaneEvent> {
        self.wait_live_event(duration)
            .or_else(|| self.pending_events.pop_front())
    }
}

#[cfg(target_arch = "wasm32")]
pub struct NativeControlPlaneShell {
    inner: MemoryControlPlaneShell,
}

#[cfg(target_arch = "wasm32")]
impl NativeControlPlaneShell {
    pub fn new(
        control_protocol: ProtocolId,
        transport_policy: RuntimeTransportPolicy,
    ) -> Result<Self, SwarmError> {
        Self::with_keypair(
            control_protocol,
            Keypair::generate_ed25519(),
            transport_policy,
        )
    }

    pub fn with_keypair(
        control_protocol: ProtocolId,
        keypair: Keypair,
        transport_policy: RuntimeTransportPolicy,
    ) -> Result<Self, SwarmError> {
        Ok(Self {
            inner: {
                let _ = transport_policy;
                MemoryControlPlaneShell::with_keypair(control_protocol, keypair)?
            },
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

    pub fn add_external_address(&mut self, address: SwarmAddress) -> Result<(), SwarmError> {
        self.inner.add_external_address(address)
    }

    pub fn disconnect_peer(&mut self, peer_id: &str) -> Result<(), SwarmError> {
        self.inner.disconnect_peer(peer_id)
    }

    pub fn connected_peer_count(&self) -> usize {
        self.inner.connected_peer_count()
    }

    pub fn publish_control(&mut self, announcement: ControlAnnouncement) {
        self.inner.publish_control(announcement);
    }

    /// Performs the publish lifecycle operation.
    pub fn publish_lifecycle(&mut self, announcement: ExperimentLifecycleAnnouncement) {
        self.inner.publish_lifecycle(announcement);
    }

    /// Performs the publish schedule operation.
    pub fn publish_schedule(&mut self, announcement: FleetScheduleAnnouncement) {
        self.inner.publish_schedule(announcement);
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

    pub fn publish_trainer_promotion_attestation(
        &mut self,
        announcement: TrainerPromotionAttestationAnnouncement,
    ) {
        self.inner
            .publish_trainer_promotion_attestation(announcement);
    }

    pub fn publish_diffusion_promotion_certificate(
        &mut self,
        announcement: DiffusionPromotionCertificateAnnouncement,
    ) {
        self.inner
            .publish_diffusion_promotion_certificate(announcement);
    }

    pub fn publish_aggregate_proposal(&mut self, announcement: AggregateProposalAnnouncement) {
        self.inner.publish_aggregate_proposal(announcement);
    }

    pub fn publish_reduction_certificate(
        &mut self,
        announcement: ReductionCertificateAnnouncement,
    ) {
        self.inner.publish_reduction_certificate(announcement);
    }

    pub fn publish_validation_quorum(&mut self, announcement: ValidationQuorumAnnouncement) {
        self.inner.publish_validation_quorum(announcement);
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

    pub fn publish_peer_directory(&mut self, announcement: PeerDirectoryAnnouncement) {
        self.inner.publish_peer_directory(announcement);
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

    pub(crate) fn request_snapshot_id(&mut self, peer_id: &str) -> Result<String, SwarmError> {
        self.inner.request_snapshot(peer_id)?;
        Ok("native-wasm-snapshot-request".into())
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

    pub(crate) fn request_artifact_manifest_id(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
    ) -> Result<String, SwarmError> {
        self.inner.request_artifact_manifest(peer_id, artifact_id)?;
        Ok("native-wasm-artifact-manifest-request".into())
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

    pub(crate) fn request_artifact_chunk_id(
        &mut self,
        peer_id: &str,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
    ) -> Result<String, SwarmError> {
        self.inner
            .request_artifact_chunk(peer_id, artifact_id, chunk_id)?;
        Ok("native-wasm-artifact-chunk-request".into())
    }

    pub fn wait_event(&mut self, timeout: Duration) -> Option<LiveControlPlaneEvent> {
        self.inner.wait_event(timeout)
    }
}
