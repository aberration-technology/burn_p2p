use super::*;
#[cfg(not(target_arch = "wasm32"))]
use crate::diloco_store::DiLoCoControlStore;
#[cfg(not(target_arch = "wasm32"))]
use burn_p2p_webrtc::tokio::{Certificate as WebRtcCertificate, Transport as WebRtcTransport};
#[cfg(not(target_arch = "wasm32"))]
use rand::thread_rng;
#[cfg(all(not(target_arch = "wasm32"), unix))]
use std::os::unix::fs::PermissionsExt;
#[cfg(not(target_arch = "wasm32"))]
use std::{fs, path::Path};

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
    diloco: DiLoCoControlStore,
    completed_diloco_responses: BTreeMap<String, (String, DiLoCoResponse)>,
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
    established_connections:
        BTreeMap<Libp2pPeerId, BTreeMap<libp2p_swarm::ConnectionId, EstablishedConnectionRoute>>,
    connection_reconciliation_deadlines: BTreeMap<Libp2pPeerId, Instant>,
    pending_outbound_requests: BTreeMap<String, Libp2pPeerId>,
    pending_inbound_responses: BTreeMap<String, Libp2pPeerId>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EstablishedConnectionRoute {
    relayed: bool,
    dialer: bool,
}

#[cfg(not(target_arch = "wasm32"))]
const RENDEZVOUS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(not(target_arch = "wasm32"))]
const RENDEZVOUS_REFRESH_DEBOUNCE: Duration = Duration::from_secs(2);
#[cfg(not(target_arch = "wasm32"))]
const KADEMLIA_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(not(target_arch = "wasm32"))]
const KADEMLIA_REFRESH_DEBOUNCE: Duration = Duration::from_secs(2);
#[cfg(not(target_arch = "wasm32"))]
const PEER_DIRECTORY_RECORD_LOOKUP_DEBOUNCE: Duration = Duration::from_secs(30);
#[cfg(not(target_arch = "wasm32"))]
const PEER_DIRECTORY_RECORD_TTL: Duration = Duration::from_secs(90);
#[cfg(not(target_arch = "wasm32"))]
const FETCH_SIDECAR_AGENT_VERSION: &str = "burn-p2p/fetch-sidecar/1";
#[cfg(not(target_arch = "wasm32"))]
const ROUTE_RECONCILIATION_GRACE: Duration = Duration::from_secs(2);
#[cfg(not(target_arch = "wasm32"))]
const ROUTE_RECONCILIATION_RETRY: Duration = Duration::from_millis(250);
#[cfg(not(target_arch = "wasm32"))]
fn control_yamux_config() -> yamux::Config {
    // Keep libp2p-yamux on its current auto-tuned implementation. Calling the
    // deprecated per-stream window setters converts this config to the legacy
    // Yamux implementation and causes severe multi-stream head-of-line stalls.
    yamux::Config::default()
}

#[cfg(not(target_arch = "wasm32"))]
fn excess_connection_ids(
    connections: &BTreeMap<libp2p_swarm::ConnectionId, EstablishedConnectionRoute>,
    maximum: usize,
    prefer_dialer: bool,
) -> Vec<libp2p_swarm::ConnectionId> {
    let mut ranked = connections
        .iter()
        .map(|(connection_id, route)| {
            (route.relayed, route.dialer != prefer_dialer, *connection_id)
        })
        .collect::<Vec<_>>();
    ranked.sort();
    ranked
        .into_iter()
        .skip(maximum)
        .map(|(_, _, connection_id)| connection_id)
        .collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn connection_limit_with_reconciliation_slack(maximum: Option<u32>) -> Option<u32> {
    maximum.map(|maximum| maximum.saturating_add(1))
}

#[cfg(not(target_arch = "wasm32"))]
fn observed_address_is_reachable(route: Option<&EstablishedConnectionRoute>) -> bool {
    route.is_some_and(|route| !route.dialer)
}

#[cfg(not(target_arch = "wasm32"))]
fn peer_has_pending_control_exchange(
    peer_id: &Libp2pPeerId,
    pending_outbound_requests: &BTreeMap<String, Libp2pPeerId>,
    pending_inbound_responses: &BTreeMap<String, Libp2pPeerId>,
) -> bool {
    pending_outbound_requests
        .values()
        .chain(pending_inbound_responses.values())
        .any(|pending_peer_id| pending_peer_id == peer_id)
}

#[cfg(not(target_arch = "wasm32"))]
fn is_low_value_maintenance_event(event: &LiveControlPlaneEvent) -> bool {
    match event {
        LiveControlPlaneEvent::PeersDiscovered { peers }
        | LiveControlPlaneEvent::PeersExpired { peers } => peers.is_empty(),
        LiveControlPlaneEvent::Other { kind } => {
            kind.starts_with("ping:")
                || kind.starts_with("identify-pushed:")
                || kind.starts_with("identify-sent:")
                || kind.starts_with("kademlia:InboundRequest")
                || kind.starts_with("kademlia:Bootstrap(")
                || kind == "kademlia-peer-directory-record-finished"
                || kind.starts_with("autonat:OutboundProbe")
        }
        _ => false,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn build_native_webrtc_transport(
    keypair: &Keypair,
    certificate_pem_path: Option<&Path>,
) -> Result<WebRtcTransport, Box<dyn std::error::Error + Send + Sync>> {
    let certificate = load_or_create_native_webrtc_certificate(certificate_pem_path)?;
    Ok(WebRtcTransport::new(keypair.clone(), certificate))
}

#[cfg(not(target_arch = "wasm32"))]
fn build_native_browser_websocket_transport(
    keypair: &Keypair,
) -> Result<
    libp2p::core::transport::Boxed<(
        libp2p::identity::PeerId,
        libp2p::core::muxing::StreamMuxerBox,
    )>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    Ok(
        libp2p::websocket::Config::new(libp2p::tcp::tokio::Transport::new(
            libp2p::tcp::Config::default(),
        ))
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(libp2p::noise::Config::new(keypair)?)
        .multiplex(control_yamux_config())
        .boxed(),
    )
}

#[cfg(not(target_arch = "wasm32"))]
fn load_or_create_native_webrtc_certificate(
    certificate_pem_path: Option<&Path>,
) -> Result<WebRtcCertificate, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(path) = certificate_pem_path
        && let Ok(pem) = fs::read_to_string(path)
    {
        match WebRtcCertificate::from_pem(&pem) {
            Ok(certificate) => return Ok(certificate),
            Err(error) => {
                eprintln!(
                    "failed to load persisted WebRTC certificate from {}: {error}; regenerating",
                    path.display()
                );
            }
        }
    }

    let certificate = WebRtcCertificate::generate(&mut thread_rng())?;
    if let Some(path) = certificate_pem_path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, certificate.serialize_pem())?;
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(certificate)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod certificate_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn persisted_webrtc_certificate_keeps_the_same_fingerprint() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("webrtc-certificate.pem");

        let first =
            load_or_create_native_webrtc_certificate(Some(&path)).expect("first certificate");
        let first_fingerprint = first.fingerprint();
        let first_pem = fs::read_to_string(&path).expect("persisted pem");

        let second =
            load_or_create_native_webrtc_certificate(Some(&path)).expect("second certificate");

        assert_eq!(second.fingerprint(), first_fingerprint);
        assert_eq!(fs::read_to_string(&path).expect("persisted pem"), first_pem);
    }

    #[test]
    fn corrupt_persisted_webrtc_certificate_is_regenerated() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("webrtc-certificate.pem");
        fs::write(&path, "not a certificate").expect("write corrupt pem");

        let certificate =
            load_or_create_native_webrtc_certificate(Some(&path)).expect("regenerated certificate");
        let persisted = fs::read_to_string(&path).expect("persisted regenerated pem");

        assert_eq!(
            WebRtcCertificate::from_pem(&persisted)
                .expect("regenerated certificate should parse")
                .fingerprint(),
            certificate.fingerprint()
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeControlPlaneShell {
    /// Creates a new value.
    pub fn new(
        control_protocol: ProtocolId,
        transport_policy: RuntimeTransportPolicy,
    ) -> Result<Self, SwarmError> {
        Self::with_keypair_and_webrtc_certificate_path(
            control_protocol,
            Keypair::generate_ed25519(),
            transport_policy,
            None,
        )
    }

    /// Returns a copy configured with the keypair.
    pub fn with_keypair(
        control_protocol: ProtocolId,
        keypair: Keypair,
        transport_policy: RuntimeTransportPolicy,
    ) -> Result<Self, SwarmError> {
        Self::with_keypair_and_webrtc_certificate_path(
            control_protocol,
            keypair,
            transport_policy,
            None,
        )
    }

    /// Returns a copy configured with the keypair and an optional persisted WebRTC certificate.
    pub fn with_keypair_and_webrtc_certificate_path(
        control_protocol: ProtocolId,
        keypair: Keypair,
        transport_policy: RuntimeTransportPolicy,
        webrtc_certificate_pem_path: Option<std::path::PathBuf>,
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
        let identify_agent_version = if transport_policy.advertise_for_discovery {
            format!("burn-p2p/native-control/{}", env!("CARGO_PKG_VERSION"))
        } else {
            FETCH_SIDECAR_AGENT_VERSION.to_owned()
        };
        let identify_config = identify::Config::new(
            format!("{}/identify/1.0.0", control_protocol.as_str()),
            keypair.public(),
        )
        .with_agent_version(identify_agent_version);
        let idle_connection_timeout =
            Duration::from_millis(transport_policy.idle_connection_timeout_ms.max(1));
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
            Some(relay::Behaviour::new(
                local_peer_id,
                relay_config_for_transport_policy(&transport_policy),
            ))
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
                        control_yamux_config,
                    )
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .with_quic()
                    .with_other_transport(|key| {
                        build_native_webrtc_transport(key, webrtc_certificate_pem_path.as_deref())
                    })
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .with_other_transport(build_native_browser_websocket_transport)
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .with_dns()
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .with_relay_client(tls_config, control_yamux_config)
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .with_behaviour(move |_, relay_client| NativeControlPlaneBehaviour {
                        request_response: request_response::cbor::Behaviour::new(
                            [(protocol, ProtocolSupport::Full)],
                            request_response::Config::default()
                                .with_request_timeout(CONTROL_REQUEST_RESPONSE_TIMEOUT),
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
                                    connection_limit_with_reconciliation_slack(
                                        transport_policy.max_established_per_peer,
                                    ),
                                ),
                        ),
                        #[cfg(not(target_arch = "wasm32"))]
                        mdns: mdns_behaviour.into(),
                    })
                    .map_err(|error| SwarmError::Runtime(error.to_string()))?
                    .with_swarm_config(|config| {
                        config.with_idle_connection_timeout(idle_connection_timeout)
                    })
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
            diloco: DiLoCoControlStore::default(),
            completed_diloco_responses: BTreeMap::new(),
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
            established_connections: BTreeMap::new(),
            connection_reconciliation_deadlines: BTreeMap::new(),
            pending_outbound_requests: BTreeMap::new(),
            pending_inbound_responses: BTreeMap::new(),
        })
    }

    /// Performs the local peer ID operation.
    pub fn local_peer_id(&self) -> &Libp2pPeerId {
        &self.local_peer_id
    }

    #[cfg(test)]
    pub(crate) fn established_connection_count(&self, peer_id: &str) -> usize {
        peer_id
            .parse::<Libp2pPeerId>()
            .ok()
            .and_then(|peer_id| self.established_connections.get(&peer_id))
            .map(BTreeMap::len)
            .unwrap_or_default()
    }

    fn send_control_request(
        &mut self,
        peer_id: Libp2pPeerId,
        request: ControlPlaneRequest,
    ) -> String {
        let request_id = self
            .swarm
            .behaviour_mut()
            .request_response
            .send_request(&peer_id, request)
            .to_string();
        self.pending_outbound_requests
            .insert(request_id.clone(), peer_id);
        request_id
    }

    fn reconcile_due_connections(&mut self) {
        let now = Instant::now();
        let due_peer_ids = self
            .connection_reconciliation_deadlines
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(peer_id, _)| *peer_id)
            .collect::<Vec<_>>();

        for peer_id in due_peer_ids {
            if peer_has_pending_control_exchange(
                &peer_id,
                &self.pending_outbound_requests,
                &self.pending_inbound_responses,
            ) {
                self.connection_reconciliation_deadlines
                    .insert(peer_id, now + ROUTE_RECONCILIATION_RETRY);
                continue;
            }

            self.connection_reconciliation_deadlines.remove(&peer_id);
            let prefer_dialer = self.local_peer_id < peer_id;
            let maximum = self
                .transport_policy
                .max_established_per_peer
                .map(|maximum| maximum as usize)
                .unwrap_or(usize::MAX);
            let excess = self
                .established_connections
                .get(&peer_id)
                .map(|connections| excess_connection_ids(connections, maximum, prefer_dialer))
                .unwrap_or_default();
            for connection_id in &excess {
                self.swarm.close_connection(*connection_id);
            }
            if !excess.is_empty() {
                self.pending_events
                    .push_back(LiveControlPlaneEvent::Other {
                        kind: format!(
                            "connection-reconciled:{peer_id}:prefer_dialer={prefer_dialer}:closed={excess:?}"
                        ),
                    });
            }
        }
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

    /// Merges a remote snapshot into the local control-plane state.
    pub fn merge_snapshot(&mut self, snapshot: &ControlPlaneSnapshot) {
        self.snapshot.merge_from_semantic(snapshot);
        rebuild_hot_index(&self.snapshot, &mut self.hot_index);
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

    /// Publishes one reduced pseudo-gradient and its exact cohort commitment.
    pub fn publish_diloco_aggregate(
        &mut self,
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
        participant_peer_ids: Vec<PeerId>,
        contribution_manifest_ids: Vec<ContentId>,
    ) {
        self.diloco.publish_aggregate(
            manifest,
            chunks,
            participant_peer_ids,
            contribution_manifest_ids,
        );
    }

    pub fn diloco_aggregate_ready(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        reducer_peer_id: &PeerId,
        round_cursor: &RoundCursor,
    ) -> Option<DiLoCoAggregateReady> {
        self.diloco
            .aggregate_ready(experiment_id, revision_id, reducer_peer_id, round_cursor)
    }

    pub(crate) fn request_diloco_id(
        &mut self,
        peer_id: &str,
        request: DiLoCoRequest,
    ) -> Result<String, SwarmError> {
        self.settle_request_response();
        self.start_diloco_request(peer_id, request)
    }

    /// Starts a DiLoCo request without blocking the swarm event loop.
    pub fn start_diloco_request(
        &mut self,
        peer_id: &str,
        request: DiLoCoRequest,
    ) -> Result<String, SwarmError> {
        let peer_id = parse_remote_peer_id(&self.local_peer_id, peer_id)?;
        let request_kind = diloco_request_kind(&request);
        let request_id =
            self.send_control_request(peer_id, ControlPlaneRequest::DiLoCo(Box::new(request)));
        if std::env::var_os("BURN_P2P_DILOCO_TRACE").is_some() {
            let routes = self
                .established_connections
                .get(&peer_id)
                .map(|connections| {
                    connections
                        .iter()
                        .map(|(connection_id, route)| {
                            format!(
                                "{connection_id:?}:relayed={}:dialer={}",
                                route.relayed, route.dialer
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            eprintln!(
                "[burn_p2p swarm] diloco-request peer={peer_id} kind={request_kind} request={request_id} routes={routes:?}"
            );
        }
        Ok(request_id)
    }

    /// Takes a completed DiLoCo response produced while polling swarm events.
    pub fn take_diloco_response(&mut self, request_id: &str) -> Option<(String, DiLoCoResponse)> {
        self.completed_diloco_responses.remove(request_id)
    }

    pub(crate) fn discard_completed_diloco_responses(&mut self) -> usize {
        let discarded = self.completed_diloco_responses.len();
        self.completed_diloco_responses.clear();
        discarded
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
            if let Some(event) =
                self.wait_live_event_with_discovery(Duration::from_millis(50), false)
            {
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
        let peer_id = parse_remote_peer_id(&self.local_peer_id, peer_id)?;
        Ok(self.send_control_request(peer_id, ControlPlaneRequest::Snapshot))
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
        let peer_id = parse_remote_peer_id(&self.local_peer_id, peer_id)?;
        Ok(self.send_control_request(
            peer_id,
            ControlPlaneRequest::ArtifactManifest { artifact_id },
        ))
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
        let peer_id = parse_remote_peer_id(&self.local_peer_id, peer_id)?;
        Ok(self.send_control_request(
            peer_id,
            ControlPlaneRequest::ArtifactChunk {
                artifact_id,
                chunk_id,
            },
        ))
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
            if let Some(event) =
                self.wait_live_event_with_discovery(Duration::from_millis(50), false)
            {
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
            if let Some(event) =
                self.wait_live_event_with_discovery(Duration::from_millis(50), false)
            {
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
            if let Some(event) =
                self.wait_live_event_with_discovery(Duration::from_millis(50), false)
            {
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

    fn wait_live_event_with_discovery(
        &mut self,
        duration: Duration,
        refresh_discovery: bool,
    ) -> Option<LiveControlPlaneEvent> {
        self.reconcile_due_connections();
        if refresh_discovery {
            self.maybe_refresh_background_discovery();
        }
        let event = self.runtime.block_on(async {
            timeout(duration, self.swarm.select_next_some())
                .await
                .ok()
                .map(|event| match event {
                    SwarmEvent::Behaviour(event) => match event {
                        NativeControlPlaneBehaviourEvent::RequestResponse(event) => match *event {
                            request_response::Event::Message { peer, message, .. } => match message
                            {
                                request_response::Message::Request {
                                    request_id,
                                    request,
                                    channel,
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
                                            Ok(()) => {
                                                self.pending_inbound_responses
                                                    .insert(request_id.to_string(), peer);
                                                LiveControlPlaneEvent::SnapshotRequested {
                                                    peer_id: peer.to_string(),
                                                }
                                            }
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
                                                self.pending_inbound_responses
                                                    .insert(request_id.to_string(), peer);
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
                                                self.pending_inbound_responses
                                                    .insert(request_id.to_string(), peer);
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
                                    ControlPlaneRequest::DiLoCo(request) => {
                                        let request_kind = diloco_request_kind(&request);
                                        let response = ControlPlaneResponse::DiLoCo(Box::new(
                                            self.diloco.respond(*request),
                                        ));
                                        match self
                                            .swarm
                                            .behaviour_mut()
                                            .request_response
                                            .send_response(channel, response)
                                        {
                                            Ok(()) => {
                                                self.pending_inbound_responses
                                                    .insert(request_id.to_string(), peer);
                                                LiveControlPlaneEvent::Other {
                                                    kind: format!(
                                                        "responded to DiLoCo {request_kind} request from {peer}"
                                                    ),
                                                }
                                            }
                                            Err(_) => LiveControlPlaneEvent::ResponseSendFailure {
                                                peer_id: peer.to_string(),
                                                message: "DiLoCo response channel closed".into(),
                                            },
                                        }
                                    }
                                },
                                request_response::Message::Response {
                                    request_id,
                                    response,
                                } => {
                                    self.pending_outbound_requests
                                        .remove(&request_id.to_string());
                                    match response {
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
                                    ControlPlaneResponse::DiLoCo(response) => {
                                        let response_kind = diloco_response_kind(&response);
                                        self.completed_diloco_responses.insert(
                                            request_id.to_string(),
                                            (peer.to_string(), *response),
                                        );
                                        LiveControlPlaneEvent::Other {
                                            kind: format!(
                                                "received DiLoCo {response_kind} response {request_id} from {peer}"
                                            ),
                                        }
                                    }
                                }
                                }
                            },
                            request_response::Event::OutboundFailure {
                                peer,
                                request_id,
                                error,
                                ..
                            } => {
                                self.pending_outbound_requests
                                    .remove(&request_id.to_string());
                                LiveControlPlaneEvent::RequestFailure {
                                    peer_id: peer.to_string(),
                                    request_id: Some(request_id.to_string()),
                                    kind: None,
                                    message: error.to_string(),
                                }
                            }
                            request_response::Event::InboundFailure {
                                peer,
                                request_id,
                                error,
                                ..
                            } => {
                                self.pending_inbound_responses
                                    .remove(&request_id.to_string());
                                LiveControlPlaneEvent::InboundFailure {
                                    peer_id: peer.to_string(),
                                    message: error.to_string(),
                                }
                            }
                            request_response::Event::ResponseSent {
                                peer, request_id, ..
                            } => {
                                self.pending_inbound_responses
                                    .remove(&request_id.to_string());
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
                            identify::Event::Received {
                                peer_id,
                                connection_id,
                                info,
                            } => {
                                if info.agent_version == FETCH_SIDECAR_AGENT_VERSION {
                                    LiveControlPlaneEvent::Other {
                                        kind: format!("ephemeral-fetch-sidecar-identified:{peer_id}"),
                                    }
                                } else {
                                    let observed_addr = info.observed_addr;
                                    let listen_addrs = info.listen_addrs;
                                    let protocols = info
                                        .protocols
                                        .into_iter()
                                        .map(|protocol| protocol.to_string())
                                        .collect::<Vec<_>>();
                                    let relay_hop_supported =
                                        protocol_supports_relay_hop(&protocols);
                                    let rendezvous_supported =
                                        protocol_supports_rendezvous(&protocols);
                                    self.swarm
                                        .behaviour_mut()
                                        .gossipsub
                                        .add_explicit_peer(&peer_id);
                                    // A remote peer's observation is a reachable local
                                    // address only when that peer dialed this exact
                                    // connection. On outbound TCP connections it is an
                                    // ephemeral source port and must not be advertised.
                                    let route = self
                                        .established_connections
                                        .get(&peer_id)
                                        .and_then(|connections| connections.get(&connection_id));
                                    if observed_address_is_reachable(route) {
                                        self.swarm.add_external_address(observed_addr);
                                    }
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
                        if self.transport_policy.enable_rendezvous_client {
                            let scheduled = Instant::now() + RENDEZVOUS_REFRESH_DEBOUNCE;
                            if self.next_rendezvous_refresh_at > scheduled {
                                self.next_rendezvous_refresh_at = scheduled;
                            }
                        }
                        LiveControlPlaneEvent::ReachableAddressExpired { address }
                    }
                    SwarmEvent::ListenerClosed {
                        listener_id,
                        addresses,
                        reason,
                    } => LiveControlPlaneEvent::Other {
                        kind: format!(
                            "listener-closed:{listener_id:?}:addresses={addresses:?}:reason={reason:?}"
                        ),
                    },
                    SwarmEvent::ListenerError { listener_id, error } => {
                        LiveControlPlaneEvent::Other {
                            kind: format!("listener-error:{listener_id:?}:{error}"),
                        }
                    }
                    SwarmEvent::ConnectionEstablished {
                        peer_id,
                        connection_id,
                        endpoint,
                        num_established,
                        ..
                    } => {
                        let relayed = endpoint.is_relayed();
                        let dialer = endpoint.is_dialer();
                        // Both endpoints must retain the same physical route. The lower peer ID
                        // prefers its dialed route; the higher peer ID prefers that route as a
                        // listener. Local connection IDs cannot provide this symmetry.
                        let prefer_dialer = self.local_peer_id < peer_id;
                        let maximum = self
                            .transport_policy
                            .max_established_per_peer
                            .map(|maximum| maximum as usize)
                            .unwrap_or(usize::MAX);
                        let (tracked, excess) = {
                            let connections =
                                self.established_connections.entry(peer_id).or_default();
                            connections.insert(connection_id, EstablishedConnectionRoute {
                                relayed,
                                dialer,
                            });
                            (
                                connections.len(),
                                excess_connection_ids(connections, maximum, prefer_dialer),
                            )
                        };
                        if !excess.is_empty() {
                            self.connection_reconciliation_deadlines
                                .insert(peer_id, Instant::now() + ROUTE_RECONCILIATION_GRACE);
                        }
                        self.pending_events
                            .push_back(LiveControlPlaneEvent::Other {
                                kind: format!(
                                    "connection-established-detail:{peer_id}:connection={connection_id:?}:relayed={relayed}:dialer={dialer}:prefer_dialer={prefer_dialer}:reported={num_established}:tracked={}:reconcile={excess:?}",
                                    tracked
                                ),
                            });
                        LiveControlPlaneEvent::ConnectionEstablished {
                            peer_id: peer_id.to_string(),
                        }
                    }
                    SwarmEvent::ConnectionClosed {
                        peer_id,
                        connection_id,
                        num_established,
                        cause,
                        ..
                    } => {
                        let remove_peer_connections =
                            if let Some(connections) =
                                self.established_connections.get_mut(&peer_id)
                            {
                                connections.remove(&connection_id);
                                connections.is_empty()
                            } else {
                                false
                            };
                        if remove_peer_connections {
                            self.established_connections.remove(&peer_id);
                        }
                        let maximum = self
                            .transport_policy
                            .max_established_per_peer
                            .map(|maximum| maximum as usize)
                            .unwrap_or(usize::MAX);
                        if self
                            .established_connections
                            .get(&peer_id)
                            .is_none_or(|connections| connections.len() <= maximum)
                        {
                            self.connection_reconciliation_deadlines.remove(&peer_id);
                        }
                        if self.transport_policy.enable_kademlia {
                            let scheduled = Instant::now() + KADEMLIA_REFRESH_DEBOUNCE;
                            if self.next_kademlia_refresh_at > scheduled {
                                self.next_kademlia_refresh_at = scheduled;
                            }
                        }
                        if self.transport_policy.enable_rendezvous_client {
                            let scheduled = Instant::now() + RENDEZVOUS_REFRESH_DEBOUNCE;
                            if self.next_rendezvous_refresh_at > scheduled {
                                self.next_rendezvous_refresh_at = scheduled;
                            }
                        }
                        self.pending_events
                            .push_back(LiveControlPlaneEvent::Other {
                                kind: format!(
                                    "connection-closed-detail:{peer_id}:remaining={num_established}:cause={cause:?}"
                                ),
                            });
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
        });
        self.reconcile_due_connections();
        event
    }

    fn settle_request_response(&mut self) {
        let deadline = Instant::now() + Duration::from_millis(250);
        while Instant::now() < deadline {
            match self.wait_live_event_with_discovery(Duration::from_millis(5), false) {
                Some(event) if !is_low_value_maintenance_event(&event) => {
                    self.pending_events.push_back(event);
                }
                Some(_) => {}
                None => break,
            }
        }
    }

    fn wait_actionable_event(
        &mut self,
        duration: Duration,
        refresh_discovery: bool,
    ) -> Option<LiveControlPlaneEvent> {
        while let Some(event) = self.pending_events.pop_front() {
            if !is_low_value_maintenance_event(&event) {
                return Some(event);
            }
        }

        let deadline = Instant::now() + duration;
        let mut refresh_discovery = refresh_discovery;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let event = self.wait_live_event_with_discovery(remaining, refresh_discovery)?;
            refresh_discovery = false;
            if !is_low_value_maintenance_event(&event) {
                return Some(event);
            }
        }
    }

    /// Performs the wait event operation.
    pub fn wait_event(&mut self, duration: Duration) -> Option<LiveControlPlaneEvent> {
        self.wait_actionable_event(duration, true)
    }

    /// Waits for an event without scheduling background discovery work.
    pub fn wait_priority_event(&mut self, duration: Duration) -> Option<LiveControlPlaneEvent> {
        self.wait_actionable_event(duration, false)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod native_event_priority_tests {
    use super::*;

    #[test]
    fn connection_pruning_prefers_direct_then_symmetric_direction() {
        let direct_listener = libp2p_swarm::ConnectionId::new_unchecked(1);
        let relay_listener = libp2p_swarm::ConnectionId::new_unchecked(2);
        let direct_dialer = libp2p_swarm::ConnectionId::new_unchecked(3);
        let connections = BTreeMap::from([
            (
                direct_listener,
                EstablishedConnectionRoute {
                    relayed: false,
                    dialer: false,
                },
            ),
            (
                relay_listener,
                EstablishedConnectionRoute {
                    relayed: true,
                    dialer: false,
                },
            ),
            (
                direct_dialer,
                EstablishedConnectionRoute {
                    relayed: false,
                    dialer: true,
                },
            ),
        ]);

        assert_eq!(
            excess_connection_ids(&connections, 1, true),
            vec![direct_listener, relay_listener]
        );
        assert_eq!(
            excess_connection_ids(&connections, 1, false),
            vec![direct_dialer, relay_listener]
        );
        assert_eq!(
            excess_connection_ids(&connections, 2, true),
            vec![relay_listener]
        );
    }

    #[test]
    fn connection_limit_allows_one_route_for_reconciliation() {
        assert_eq!(connection_limit_with_reconciliation_slack(Some(1)), Some(2));
        assert_eq!(
            connection_limit_with_reconciliation_slack(Some(u32::MAX)),
            Some(u32::MAX)
        );
        assert_eq!(connection_limit_with_reconciliation_slack(None), None);
    }

    #[test]
    fn observed_addresses_are_only_reachable_on_inbound_connections() {
        assert!(observed_address_is_reachable(Some(
            &EstablishedConnectionRoute {
                relayed: false,
                dialer: false,
            }
        )));
        assert!(!observed_address_is_reachable(Some(
            &EstablishedConnectionRoute {
                relayed: false,
                dialer: true,
            }
        )));
        assert!(!observed_address_is_reachable(None));
    }

    #[test]
    fn connection_reconciliation_waits_for_control_exchanges() {
        let peer_id = Libp2pPeerId::random();
        let other_peer_id = Libp2pPeerId::random();
        let mut outbound = BTreeMap::new();
        let mut inbound = BTreeMap::new();

        assert!(!peer_has_pending_control_exchange(
            &peer_id, &outbound, &inbound
        ));
        outbound.insert("outbound".into(), peer_id);
        assert!(peer_has_pending_control_exchange(
            &peer_id, &outbound, &inbound
        ));
        outbound.insert("other".into(), other_peer_id);
        outbound.remove("outbound");
        inbound.insert("inbound".into(), peer_id);
        assert!(peer_has_pending_control_exchange(
            &peer_id, &outbound, &inbound
        ));
    }

    #[test]
    fn filters_only_non_actionable_native_maintenance_events() {
        for kind in [
            "ping:Event { peer: 12D3KooW }",
            "identify-pushed:12D3KooW",
            "identify-sent:12D3KooW",
            "kademlia:InboundRequest { request: FindNode }",
            "kademlia:Bootstrap(Ok(()))",
            "kademlia-peer-directory-record-finished",
            "autonat:OutboundProbe(Error(NoServer))",
        ] {
            assert!(is_low_value_maintenance_event(
                &LiveControlPlaneEvent::Other { kind: kind.into() }
            ));
        }

        for kind in [
            "identify-error:12D3KooW:protocol",
            "kademlia-peer-directory-decode-error:invalid",
            "rendezvous-registered:12D3KooW:dragon:30",
            "rendezvous-discover-failed:12D3KooW:dragon:timeout",
        ] {
            assert!(!is_low_value_maintenance_event(
                &LiveControlPlaneEvent::Other { kind: kind.into() }
            ));
        }

        assert!(is_low_value_maintenance_event(
            &LiveControlPlaneEvent::PeersDiscovered { peers: Vec::new() }
        ));
        assert!(!is_low_value_maintenance_event(
            &LiveControlPlaneEvent::ConnectionEstablished {
                peer_id: "peer-a".into(),
            }
        ));
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn relay_config_for_transport_policy(transport_policy: &RuntimeTransportPolicy) -> relay::Config {
    let mut config = relay::Config::default();
    if let Some(max_incoming) = transport_policy.max_established_incoming {
        config.max_reservations = max_incoming as usize;
    }
    if let Some(max_total) = transport_policy.max_established_total {
        config.max_circuits = (max_total as usize).max(config.max_circuits);
    }
    if let Some(max_circuits) = transport_policy.max_relay_circuits {
        config.max_circuits = (max_circuits as usize).max(1);
    }
    config.max_circuit_bytes = transport_policy.max_relay_circuit_bytes.max(128 * 1024);
    config
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
        Self::with_keypair_and_webrtc_certificate_path(
            control_protocol,
            Keypair::generate_ed25519(),
            transport_policy,
            None,
        )
    }

    pub fn with_keypair(
        control_protocol: ProtocolId,
        keypair: Keypair,
        transport_policy: RuntimeTransportPolicy,
    ) -> Result<Self, SwarmError> {
        Self::with_keypair_and_webrtc_certificate_path(
            control_protocol,
            keypair,
            transport_policy,
            None,
        )
    }

    pub fn with_keypair_and_webrtc_certificate_path(
        control_protocol: ProtocolId,
        keypair: Keypair,
        transport_policy: RuntimeTransportPolicy,
        webrtc_certificate_pem_path: Option<std::path::PathBuf>,
    ) -> Result<Self, SwarmError> {
        Ok(Self {
            inner: {
                let _ = transport_policy;
                let _ = webrtc_certificate_pem_path;
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

    pub fn connected_peer_ids(&self) -> Vec<PeerId> {
        self.inner.connected_peer_ids()
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

    pub fn merge_snapshot(&mut self, snapshot: &ControlPlaneSnapshot) {
        self.inner.merge_snapshot(snapshot);
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

    pub fn publish_diloco_state(
        &mut self,
        snapshot: DiLoCoStateSnapshot,
        outer_optimizer_state: Option<StateBlob>,
        current_parameters: Option<FlattenedTensorPack>,
    ) {
        self.inner
            .publish_diloco_state(snapshot, outer_optimizer_state, current_parameters);
    }

    pub fn publish_diloco_gradient(
        &mut self,
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
    ) {
        self.inner.publish_diloco_gradient(manifest, chunks);
    }

    pub fn publish_diloco_aggregate(
        &mut self,
        manifest: PseudoGradientManifest,
        chunks: Vec<PseudoGradientChunk>,
        participant_peer_ids: Vec<PeerId>,
        contribution_manifest_ids: Vec<ContentId>,
    ) {
        self.inner.publish_diloco_aggregate(
            manifest,
            chunks,
            participant_peer_ids,
            contribution_manifest_ids,
        );
    }

    pub fn diloco_aggregate_ready(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        reducer_peer_id: &PeerId,
        round_cursor: &RoundCursor,
    ) -> Option<DiLoCoAggregateReady> {
        self.inner
            .diloco_aggregate_ready(experiment_id, revision_id, reducer_peer_id, round_cursor)
    }

    pub fn fetch_diloco(
        &mut self,
        peer_id: &str,
        request: DiLoCoRequest,
        timeout: Duration,
    ) -> Result<DiLoCoResponse, SwarmError> {
        self.inner.fetch_diloco(peer_id, request, timeout)
    }

    pub fn start_diloco_request(
        &mut self,
        peer_id: &str,
        request: DiLoCoRequest,
    ) -> Result<String, SwarmError> {
        self.inner.start_diloco_request(peer_id, request)
    }

    pub fn take_diloco_response(&mut self, request_id: &str) -> Option<(String, DiLoCoResponse)> {
        self.inner.take_diloco_response(request_id)
    }

    pub(crate) fn discard_completed_diloco_responses(&mut self) -> usize {
        self.inner.discard_completed_diloco_responses()
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

    pub fn wait_priority_event(&mut self, timeout: Duration) -> Option<LiveControlPlaneEvent> {
        self.inner.wait_priority_event(timeout)
    }
}
