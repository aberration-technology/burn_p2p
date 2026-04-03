use burn_p2p_core::{ClientPlatform, GenesisSpec, NetworkId};
use libp2p::{Multiaddr, StreamProtocol};
use serde::{Deserialize, Serialize};

use crate::stats::SwarmError;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum OverlayChannel {
    Control,
    Heads,
    Leases,
    Telemetry,
    Alerts,
}

impl OverlayChannel {
    pub fn path_segment(&self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Heads => "heads",
            Self::Leases => "leases",
            Self::Telemetry => "telemetry",
            Self::Alerts => "alerts",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OverlayTopic {
    pub network_id: NetworkId,
    pub study_id: Option<burn_p2p_core::StudyId>,
    pub experiment_id: Option<burn_p2p_core::ExperimentId>,
    pub channel: OverlayChannel,
    pub path: String,
}

impl OverlayTopic {
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

    pub fn as_str(&self) -> &str {
        &self.path
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperimentOverlaySet {
    pub control: OverlayTopic,
    pub heads: OverlayTopic,
    pub leases: OverlayTopic,
    pub telemetry: OverlayTopic,
    pub alerts: OverlayTopic,
}

impl ExperimentOverlaySet {
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

    pub fn experiment_topics(&self) -> [OverlayTopic; 4] {
        [
            self.heads.clone(),
            self.leases.clone(),
            self.telemetry.clone(),
            self.alerts.clone(),
        ]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SwarmAddress(pub(crate) String);

impl SwarmAddress {
    pub fn new(value: impl Into<String>) -> Result<Self, SwarmError> {
        let value = value.into();
        let _: Multiaddr = value
            .parse()
            .map_err(|_| SwarmError::InvalidAddress(value.clone()))?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_memory(&self) -> bool {
        self.0.starts_with("/memory/")
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
pub struct ProtocolId(String);

impl ProtocolId {
    pub fn new(value: impl Into<String>) -> Result<Self, SwarmError> {
        let value = value.into();
        let _ = StreamProtocol::try_from_owned(value.clone())
            .map_err(|_| SwarmError::InvalidProtocolId(value.clone()))?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolSet {
    pub control: ProtocolId,
    pub artifact_sync: ProtocolId,
    pub chunk_fetch: ProtocolId,
    pub microshard_fetch: ProtocolId,
    pub telemetry_snapshot: ProtocolId,
}

impl ProtocolSet {
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
pub enum RuntimeEnvironment {
    Native,
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
pub enum TransportKind {
    Tcp,
    Quic,
    WebSocket,
    WebTransport,
    WebRtc,
    RelayReservation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTransportPolicy {
    pub environment: RuntimeEnvironment,
    pub preferred_transports: Vec<TransportKind>,
    pub supports_direct_streams: bool,
    pub require_relay_reservation_for_private_nodes: bool,
    pub export_openmetrics: bool,
}

impl RuntimeTransportPolicy {
    pub fn native() -> Self {
        Self {
            environment: RuntimeEnvironment::Native,
            preferred_transports: vec![
                TransportKind::Tcp,
                TransportKind::Quic,
                TransportKind::WebSocket,
                TransportKind::RelayReservation,
            ],
            supports_direct_streams: true,
            require_relay_reservation_for_private_nodes: true,
            export_openmetrics: true,
        }
    }

    pub fn browser() -> Self {
        Self {
            environment: RuntimeEnvironment::Browser,
            preferred_transports: vec![
                TransportKind::WebTransport,
                TransportKind::WebRtc,
                TransportKind::WebSocket,
                TransportKind::RelayReservation,
            ],
            supports_direct_streams: true,
            require_relay_reservation_for_private_nodes: true,
            export_openmetrics: true,
        }
    }

    pub fn for_platform(platform: ClientPlatform) -> Self {
        match platform {
            ClientPlatform::Native => Self::native(),
            ClientPlatform::Browser => Self::browser(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeBoundary {
    pub environment: RuntimeEnvironment,
    pub transport_policy: RuntimeTransportPolicy,
    pub bootstrap_addresses: Vec<SwarmAddress>,
    pub listen_addresses: Vec<SwarmAddress>,
    pub protocols: ProtocolSet,
    pub control_overlay: OverlayTopic,
}

impl RuntimeBoundary {
    pub fn for_platform(
        genesis: &GenesisSpec,
        platform: ClientPlatform,
        bootstrap_addresses: Vec<SwarmAddress>,
        listen_addresses: Vec<SwarmAddress>,
    ) -> Result<Self, SwarmError> {
        Ok(Self {
            environment: platform.clone().into(),
            transport_policy: RuntimeTransportPolicy::for_platform(platform),
            bootstrap_addresses,
            listen_addresses,
            protocols: ProtocolSet::for_network(&genesis.network_id)?,
            control_overlay: OverlayTopic::control(genesis.network_id.clone()),
        })
    }
}
