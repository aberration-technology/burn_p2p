use burn_p2p_core::{ClientPlatform, GenesisSpec, NetworkId, PeerRole, PeerRoleSet};
use libp2p::{Multiaddr, StreamProtocol};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::stats::SwarmError;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported overlay channel values.
pub enum OverlayChannel {
    /// Uses the control variant.
    Control,
    /// Uses the heads variant.
    Heads,
    /// Uses the leases variant.
    Leases,
    /// Uses the metrics variant.
    Metrics,
    /// Uses the telemetry variant.
    Telemetry,
    /// Uses the alerts variant.
    Alerts,
}

impl OverlayChannel {
    /// Performs the path segment operation.
    pub fn path_segment(&self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Heads => "heads",
            Self::Leases => "leases",
            Self::Metrics => "metrics",
            Self::Telemetry => "telemetry",
            Self::Alerts => "alerts",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Represents an overlay topic.
pub struct OverlayTopic {
    /// The network ID.
    pub network_id: NetworkId,
    /// The study ID.
    pub study_id: Option<burn_p2p_core::StudyId>,
    /// The experiment ID.
    pub experiment_id: Option<burn_p2p_core::ExperimentId>,
    /// The channel.
    pub channel: OverlayChannel,
    /// The path.
    pub path: String,
}

impl OverlayTopic {
    /// Performs the control operation.
    pub fn control(network_id: NetworkId) -> Self {
        let path = format!("/burn-p2p/{}/control", network_id.as_str());
        Self {
            network_id,
            study_id: None,
            experiment_id: None,
            channel: OverlayChannel::Control,
            path,
        }
    }

    /// Performs the experiment operation.
    pub fn experiment(
        network_id: NetworkId,
        study_id: burn_p2p_core::StudyId,
        experiment_id: burn_p2p_core::ExperimentId,
        channel: OverlayChannel,
    ) -> Result<Self, SwarmError> {
        if channel == OverlayChannel::Control {
            return Err(SwarmError::InvalidOverlayChannel {
                reason: "control is mainnet-scoped and cannot be derived from an experiment",
            });
        }

        let path = format!(
            "/burn-p2p/{}/study/{}/exp/{}/{}",
            network_id.as_str(),
            study_id.as_str(),
            experiment_id.as_str(),
            channel.path_segment()
        );

        Ok(Self {
            network_id,
            study_id: Some(study_id),
            experiment_id: Some(experiment_id),
            channel,
            path,
        })
    }

    /// Returns the str view.
    pub fn as_str(&self) -> &str {
        &self.path
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an experiment overlay set.
pub struct ExperimentOverlaySet {
    /// The control.
    pub control: OverlayTopic,
    /// The heads.
    pub heads: OverlayTopic,
    /// The leases.
    pub leases: OverlayTopic,
    /// The metrics.
    pub metrics: OverlayTopic,
    /// The telemetry.
    pub telemetry: OverlayTopic,
    /// The alerts.
    pub alerts: OverlayTopic,
}

impl ExperimentOverlaySet {
    /// Creates a new value.
    pub fn new(
        network_id: NetworkId,
        study_id: burn_p2p_core::StudyId,
        experiment_id: burn_p2p_core::ExperimentId,
    ) -> Result<Self, SwarmError> {
        Ok(Self {
            control: OverlayTopic::control(network_id.clone()),
            heads: OverlayTopic::experiment(
                network_id.clone(),
                study_id.clone(),
                experiment_id.clone(),
                OverlayChannel::Heads,
            )?,
            leases: OverlayTopic::experiment(
                network_id.clone(),
                study_id.clone(),
                experiment_id.clone(),
                OverlayChannel::Leases,
            )?,
            metrics: OverlayTopic::experiment(
                network_id.clone(),
                study_id.clone(),
                experiment_id.clone(),
                OverlayChannel::Metrics,
            )?,
            telemetry: OverlayTopic::experiment(
                network_id.clone(),
                study_id.clone(),
                experiment_id.clone(),
                OverlayChannel::Telemetry,
            )?,
            alerts: OverlayTopic::experiment(
                network_id,
                study_id,
                experiment_id,
                OverlayChannel::Alerts,
            )?,
        })
    }

    /// Performs the experiment topics operation.
    pub fn experiment_topics(&self) -> [OverlayTopic; 5] {
        [
            self.heads.clone(),
            self.leases.clone(),
            self.metrics.clone(),
            self.telemetry.clone(),
            self.alerts.clone(),
        ]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Represents a swarm address.
pub struct SwarmAddress(pub(crate) String);

impl SwarmAddress {
    /// Creates a new value.
    pub fn new(value: impl Into<String>) -> Result<Self, SwarmError> {
        let value = value.into();
        let _: Multiaddr = value
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(value.clone()))?;
        Ok(Self(value))
    }

    /// Returns the str view.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether the value is memory.
    pub fn is_memory(&self) -> bool {
        self.0.starts_with("/memory/")
    }

    /// Returns whether the value is a relayed circuit address.
    pub fn is_relay_circuit(&self) -> bool {
        self.0.contains("/p2p-circuit")
    }
}

impl TryFrom<&str> for SwarmAddress {
    type Error = SwarmError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<String> for SwarmAddress {
    type Error = SwarmError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Identifies the protocol.
pub struct ProtocolId(String);

impl ProtocolId {
    /// Creates a new value.
    pub fn new(value: impl Into<String>) -> Result<Self, SwarmError> {
        let value = value.into();
        let _ = StreamProtocol::try_from_owned(value.clone())
            .map_err(|_| SwarmError::InvalidProtocolId(value.clone()))?;
        Ok(Self(value))
    }

    /// Returns the str view.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a protocol set.
pub struct ProtocolSet {
    /// The control.
    pub control: ProtocolId,
    /// The artifact sync.
    pub artifact_sync: ProtocolId,
    /// The chunk fetch.
    pub chunk_fetch: ProtocolId,
    /// The microshard fetch.
    pub microshard_fetch: ProtocolId,
    /// The telemetry snapshot.
    pub telemetry_snapshot: ProtocolId,
}

impl ProtocolSet {
    /// Performs the for network operation.
    pub fn for_network(network_id: &NetworkId) -> Result<Self, SwarmError> {
        let base = format!("/burn-p2p/{}/v1", network_id.as_str());

        Ok(Self {
            control: ProtocolId::new(format!("{base}/control"))?,
            artifact_sync: ProtocolId::new(format!("{base}/artifact-sync"))?,
            chunk_fetch: ProtocolId::new(format!("{base}/chunk-fetch"))?,
            microshard_fetch: ProtocolId::new(format!("{base}/microshard-fetch"))?,
            telemetry_snapshot: ProtocolId::new(format!("{base}/telemetry-snapshot"))?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported runtime environment values.
pub enum RuntimeEnvironment {
    /// Uses the native variant.
    Native,
    /// Uses the browser variant.
    Browser,
}

impl From<ClientPlatform> for RuntimeEnvironment {
    fn from(value: ClientPlatform) -> Self {
        match value {
            ClientPlatform::Native => Self::Native,
            ClientPlatform::Browser => Self::Browser,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported transport kinds.
pub enum TransportKind {
    /// Uses the tcp kind.
    Tcp,
    /// Uses the quic kind.
    Quic,
    /// Uses the web socket kind.
    WebSocket,
    /// Uses the web transport kind.
    WebTransport,
    /// Uses the web rtc kind.
    WebRtc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures the runtime transport policy.
pub struct RuntimeTransportPolicy {
    /// The environment.
    pub environment: RuntimeEnvironment,
    /// The preferred transports.
    pub preferred_transports: Vec<TransportKind>,
    /// The target number of direct peers to maintain before the runtime stops proactive dialing.
    pub target_connected_peers: usize,
    #[serde(default)]
    /// The number of bootstrap/coherence-seed connections to keep once a healthy non-bootstrap
    /// mesh has formed. Most peers can drop to zero and reconnect only when rediscovery is needed.
    pub target_bootstrap_seed_connections: usize,
    /// The supports direct streams.
    pub supports_direct_streams: bool,
    /// The maximum number of established inbound connections accepted by the runtime.
    pub max_established_incoming: Option<u32>,
    /// The maximum number of established connections accepted by the runtime.
    pub max_established_total: Option<u32>,
    /// The maximum number of established connections accepted from one peer.
    pub max_established_per_peer: Option<u32>,
    #[serde(default)]
    /// Enables local multicast discovery for development-oriented native swarms.
    pub enable_local_discovery: bool,
    #[serde(default)]
    /// Enables the relay client transport and relay fallback dialing.
    pub enable_relay_client: bool,
    #[serde(default)]
    /// Enables serving relay reservations for other peers.
    pub enable_relay_server: bool,
    #[serde(default)]
    /// Enables direct connection upgrade attempts after relayed rendezvous.
    pub enable_hole_punching: bool,
    #[serde(default)]
    /// Enables autonat reachability probing for native peers.
    pub enable_autonat: bool,
    #[serde(default)]
    /// Enables rendezvous-based registration and discovery against reachable seed peers.
    pub enable_rendezvous_client: bool,
    #[serde(default)]
    /// Enables serving rendezvous registrations and discovery for other peers.
    pub enable_rendezvous_server: bool,
    #[serde(default)]
    /// Enables Kademlia-backed native peer discovery and routing-table learning.
    pub enable_kademlia: bool,
    /// The export openmetrics.
    pub export_openmetrics: bool,
}

impl RuntimeTransportPolicy {
    /// Performs the native operation.
    pub fn native_for_roles(roles: &PeerRoleSet) -> Self {
        let bootstrap =
            roles.contains(&PeerRole::Bootstrap) || roles.contains(&PeerRole::RelayHelper);
        let operator = roles.contains(&PeerRole::Authority) || roles.contains(&PeerRole::Archive);
        let validator = roles.contains(&PeerRole::Validator);

        Self {
            environment: RuntimeEnvironment::Native,
            preferred_transports: vec![
                TransportKind::Quic,
                TransportKind::Tcp,
                TransportKind::WebSocket,
            ],
            target_connected_peers: if bootstrap {
                8
            } else if operator || validator {
                6
            } else {
                4
            },
            target_bootstrap_seed_connections: 0,
            supports_direct_streams: true,
            max_established_incoming: if bootstrap {
                Some(96)
            } else if operator || validator {
                Some(48)
            } else {
                Some(24)
            },
            max_established_total: if bootstrap {
                Some(128)
            } else if operator || validator {
                Some(64)
            } else {
                Some(32)
            },
            max_established_per_peer: Some(1),
            enable_local_discovery: false,
            enable_relay_client: true,
            enable_relay_server: bootstrap,
            enable_hole_punching: true,
            enable_autonat: true,
            enable_rendezvous_client: true,
            enable_rendezvous_server: bootstrap,
            enable_kademlia: true,
            export_openmetrics: true,
        }
    }

    /// Performs the browser operation.
    pub fn browser_for_roles(_roles: &PeerRoleSet) -> Self {
        Self {
            environment: RuntimeEnvironment::Browser,
            preferred_transports: vec![
                TransportKind::WebRtc,
                TransportKind::WebTransport,
                TransportKind::WebSocket,
            ],
            target_connected_peers: 3,
            target_bootstrap_seed_connections: 0,
            supports_direct_streams: true,
            max_established_incoming: None,
            max_established_total: None,
            max_established_per_peer: Some(1),
            enable_local_discovery: false,
            enable_relay_client: false,
            enable_relay_server: false,
            enable_hole_punching: false,
            enable_autonat: false,
            enable_rendezvous_client: false,
            enable_rendezvous_server: false,
            enable_kademlia: false,
            export_openmetrics: true,
        }
    }

    /// Performs the for platform and roles operation.
    pub fn for_platform_and_roles(platform: ClientPlatform, roles: &PeerRoleSet) -> Self {
        match platform {
            ClientPlatform::Native => Self::native_for_roles(roles),
            ClientPlatform::Browser => Self::browser_for_roles(roles),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a runtime boundary.
pub struct RuntimeBoundary {
    /// The environment.
    pub environment: RuntimeEnvironment,
    /// The transport policy.
    pub transport_policy: RuntimeTransportPolicy,
    /// The bootstrap addresses.
    pub bootstrap_addresses: Vec<SwarmAddress>,
    /// The listen addresses.
    pub listen_addresses: Vec<SwarmAddress>,
    /// Explicit externally reachable addresses that should be advertised by the runtime.
    pub external_addresses: Vec<SwarmAddress>,
    /// Optional persisted native WebRTC certificate PEM path used to keep certhashes stable.
    pub webrtc_certificate_pem_path: Option<PathBuf>,
    /// The protocols.
    pub protocols: ProtocolSet,
    /// The control overlay.
    pub control_overlay: OverlayTopic,
}

impl RuntimeBoundary {
    /// Performs the for platform operation.
    pub fn for_platform_and_roles(
        genesis: &GenesisSpec,
        platform: ClientPlatform,
        roles: &PeerRoleSet,
        bootstrap_addresses: Vec<SwarmAddress>,
        listen_addresses: Vec<SwarmAddress>,
        external_addresses: Vec<SwarmAddress>,
        webrtc_certificate_pem_path: Option<PathBuf>,
    ) -> Result<Self, SwarmError> {
        let listen_addresses = if listen_addresses.is_empty() {
            default_listen_addresses_for_platform_and_roles(&platform)?
        } else {
            listen_addresses
        };
        Ok(Self {
            environment: platform.clone().into(),
            transport_policy: RuntimeTransportPolicy::for_platform_and_roles(platform, roles),
            bootstrap_addresses,
            listen_addresses,
            external_addresses,
            webrtc_certificate_pem_path,
            protocols: ProtocolSet::for_network(&genesis.network_id)?,
            control_overlay: OverlayTopic::control(genesis.network_id.clone()),
        })
    }
}

fn default_listen_addresses_for_platform_and_roles(
    platform: &ClientPlatform,
) -> Result<Vec<SwarmAddress>, SwarmError> {
    match platform {
        ClientPlatform::Native => {
            let mut listen_addresses = vec![SwarmAddress::new("/ip4/127.0.0.1/tcp/0")?];
            if crate::native_browser_webrtc_direct_runtime_supported() {
                listen_addresses.push(SwarmAddress::new("/ip4/127.0.0.1/udp/0/webrtc-direct")?);
            }
            Ok(listen_addresses)
        }
        ClientPlatform::Browser => Ok(Vec::new()),
    }
}
