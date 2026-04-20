use std::convert::Infallible;

#[cfg(not(target_arch = "wasm32"))]
use std::net::{TcpListener, UdpSocket};

#[cfg(not(target_arch = "wasm32"))]
use libp2p::multiaddr::Protocol as MultiaddrProtocol;
use libp2p::{Multiaddr, StreamProtocol};
#[cfg(not(target_arch = "wasm32"))]
use libp2p_identity::{Keypair, PeerId as Libp2pPeerId};
#[cfg(not(target_arch = "wasm32"))]
use libp2p_kad as kad;
#[cfg(not(target_arch = "wasm32"))]
use libp2p_rendezvous::Namespace;
use libp2p_request_response as request_response;
use libp2p_swarm::SwarmEvent;

use crate::{ControlPlaneRequest, ControlPlaneResponse, ProtocolId, SwarmError};

#[cfg(not(target_arch = "wasm32"))]
use crate::NativeControlPlaneBehaviourEvent;

pub(crate) fn other_name(event: &SwarmEvent<Infallible>) -> &'static str {
    match event {
        SwarmEvent::Behaviour(_) => "behaviour",
        SwarmEvent::ConnectionEstablished { .. } => "connection-established",
        SwarmEvent::ConnectionClosed { .. } => "connection-closed",
        SwarmEvent::IncomingConnection { .. } => "incoming-connection",
        SwarmEvent::IncomingConnectionError { .. } => "incoming-connection-error",
        SwarmEvent::OutgoingConnectionError { .. } => "outgoing-connection-error",
        SwarmEvent::NewListenAddr { .. } => "new-listen-addr",
        SwarmEvent::ExpiredListenAddr { .. } => "expired-listen-addr",
        SwarmEvent::ListenerClosed { .. } => "listener-closed",
        SwarmEvent::ListenerError { .. } => "listener-error",
        SwarmEvent::Dialing { .. } => "dialing",
        SwarmEvent::NewExternalAddrCandidate { .. } => "new-external-addr-candidate",
        SwarmEvent::ExternalAddrConfirmed { .. } => "external-addr-confirmed",
        SwarmEvent::ExternalAddrExpired { .. } => "external-addr-expired",
        SwarmEvent::NewExternalAddrOfPeer { .. } => "new-external-addr-of-peer",
        _ => "other",
    }
}

pub(crate) fn other_control_name(
    event: &SwarmEvent<request_response::Event<ControlPlaneRequest, ControlPlaneResponse>>,
) -> &'static str {
    match event {
        SwarmEvent::Behaviour(_) => "behaviour",
        SwarmEvent::ConnectionEstablished { .. } => "connection-established",
        SwarmEvent::ConnectionClosed { .. } => "connection-closed",
        SwarmEvent::IncomingConnection { .. } => "incoming-connection",
        SwarmEvent::IncomingConnectionError { .. } => "incoming-connection-error",
        SwarmEvent::OutgoingConnectionError { .. } => "outgoing-connection-error",
        SwarmEvent::NewListenAddr { .. } => "new-listen-addr",
        SwarmEvent::ExpiredListenAddr { .. } => "expired-listen-addr",
        SwarmEvent::ListenerClosed { .. } => "listener-closed",
        SwarmEvent::ListenerError { .. } => "listener-error",
        SwarmEvent::Dialing { .. } => "dialing",
        SwarmEvent::NewExternalAddrCandidate { .. } => "new-external-addr-candidate",
        SwarmEvent::ExternalAddrConfirmed { .. } => "external-addr-confirmed",
        SwarmEvent::ExternalAddrExpired { .. } => "external-addr-expired",
        SwarmEvent::NewExternalAddrOfPeer { .. } => "new-external-addr-of-peer",
        _ => "other",
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn other_native_control_name(
    event: &SwarmEvent<NativeControlPlaneBehaviourEvent>,
) -> &'static str {
    match event {
        SwarmEvent::Behaviour(_) => "behaviour",
        SwarmEvent::ConnectionEstablished { .. } => "connection-established",
        SwarmEvent::ConnectionClosed { .. } => "connection-closed",
        SwarmEvent::IncomingConnection { .. } => "incoming-connection",
        SwarmEvent::IncomingConnectionError { .. } => "incoming-connection-error",
        SwarmEvent::OutgoingConnectionError { .. } => "outgoing-connection-error",
        SwarmEvent::NewListenAddr { .. } => "new-listen-addr",
        SwarmEvent::ExpiredListenAddr { .. } => "expired-listen-addr",
        SwarmEvent::ListenerClosed { .. } => "listener-closed",
        SwarmEvent::ListenerError { .. } => "listener-error",
        SwarmEvent::Dialing { .. } => "dialing",
        SwarmEvent::NewExternalAddrCandidate { .. } => "new-external-addr-candidate",
        SwarmEvent::ExternalAddrConfirmed { .. } => "external-addr-confirmed",
        SwarmEvent::ExternalAddrExpired { .. } => "external-addr-expired",
        SwarmEvent::NewExternalAddrOfPeer { .. } => "new-external-addr-of-peer",
        _ => "other",
    }
}

#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
pub(crate) fn other_native_control_name<T>(_event: &SwarmEvent<T>) -> &'static str {
    "other"
}

pub(crate) fn stream_protocol(protocol: &ProtocolId) -> Result<StreamProtocol, SwarmError> {
    StreamProtocol::try_from_owned(protocol.as_str().to_owned())
        .map_err(|_| SwarmError::InvalidProtocolId(protocol.as_str().to_owned()))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn tls_config(keypair: &Keypair) -> Result<libp2p::tls::Config, SwarmError> {
    libp2p::tls::Config::new(keypair)
        .map_err(|error| SwarmError::Runtime(format!("failed to build tls config: {error}")))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn protocol_supports_relay_hop(protocols: &[String]) -> bool {
    protocols
        .iter()
        .any(|protocol| protocol.as_str() == libp2p::relay::HOP_PROTOCOL_NAME.as_ref())
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn protocol_supports_rendezvous(protocols: &[String]) -> bool {
    protocols
        .iter()
        .any(|protocol| protocol.as_str() == "/rendezvous/1.0.0")
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn kademlia_protocol_for_control_protocol(
    protocol: &ProtocolId,
) -> Result<StreamProtocol, SwarmError> {
    let mut parts = protocol
        .as_str()
        .split('/')
        .filter(|segment| !segment.is_empty());
    let (Some("burn-p2p"), Some(network_id), Some("v1"), Some("control")) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(SwarmError::InvalidProtocolId(protocol.as_str().to_owned()));
    };
    if parts.next().is_some() {
        return Err(SwarmError::InvalidProtocolId(protocol.as_str().to_owned()));
    }

    StreamProtocol::try_from_owned(format!("/burn-p2p/{network_id}/v1/kad"))
        .map_err(|_| SwarmError::InvalidProtocolId(protocol.as_str().to_owned()))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn rendezvous_namespace_for_control_protocol(
    protocol: &ProtocolId,
) -> Result<Namespace, SwarmError> {
    let mut parts = protocol
        .as_str()
        .split('/')
        .filter(|segment| !segment.is_empty());
    let (Some("burn-p2p"), Some(network_id), Some("v1"), Some("control")) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(SwarmError::InvalidProtocolId(protocol.as_str().to_owned()));
    };
    if parts.next().is_some() {
        return Err(SwarmError::InvalidProtocolId(protocol.as_str().to_owned()));
    }

    Namespace::new(format!("burn-p2p:{network_id}"))
        .map_err(|_| SwarmError::InvalidProtocolId(protocol.as_str().to_owned()))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn relay_reservation_listen_addr(
    relay_peer_id: &Libp2pPeerId,
    listen_addresses: &[Multiaddr],
) -> Option<Multiaddr> {
    listen_addresses
        .iter()
        .find(|address| {
            !address
                .iter()
                .any(|protocol| matches!(protocol, MultiaddrProtocol::P2pCircuit))
        })
        .map(|address| {
            let mut relay_address = address.clone();
            if !relay_address
                .iter()
                .any(|protocol| matches!(protocol, MultiaddrProtocol::P2p(_)))
            {
                relay_address.push(MultiaddrProtocol::P2p(*relay_peer_id));
            }
            relay_address.push(MultiaddrProtocol::P2pCircuit);
            relay_address
        })
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn peer_directory_record_key_for_peer(peer_id: &str) -> kad::RecordKey {
    kad::RecordKey::new(&format!("burn-p2p-peer-directory:{peer_id}"))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn materialize_listen_addr(address: &Multiaddr) -> Result<Multiaddr, std::io::Error> {
    let protocols: Vec<_> = address.iter().collect();
    match protocols.as_slice() {
        [MultiaddrProtocol::Ip4(ip), MultiaddrProtocol::Tcp(0)] => {
            let listener = TcpListener::bind((*ip, 0))?;
            let port = listener.local_addr()?.port();
            drop(listener);
            let mut concrete = Multiaddr::empty();
            concrete.push(MultiaddrProtocol::Ip4(*ip));
            concrete.push(MultiaddrProtocol::Tcp(port));
            Ok(concrete)
        }
        [MultiaddrProtocol::Ip6(ip), MultiaddrProtocol::Tcp(0)] => {
            let listener = TcpListener::bind((*ip, 0))?;
            let port = listener.local_addr()?.port();
            drop(listener);
            let mut concrete = Multiaddr::empty();
            concrete.push(MultiaddrProtocol::Ip6(*ip));
            concrete.push(MultiaddrProtocol::Tcp(port));
            Ok(concrete)
        }
        [
            MultiaddrProtocol::Ip4(ip),
            MultiaddrProtocol::Udp(0),
            MultiaddrProtocol::QuicV1,
        ] => {
            let socket = UdpSocket::bind((*ip, 0))?;
            let port = socket.local_addr()?.port();
            drop(socket);
            let mut concrete = Multiaddr::empty();
            concrete.push(MultiaddrProtocol::Ip4(*ip));
            concrete.push(MultiaddrProtocol::Udp(port));
            concrete.push(MultiaddrProtocol::QuicV1);
            Ok(concrete)
        }
        [
            MultiaddrProtocol::Ip6(ip),
            MultiaddrProtocol::Udp(0),
            MultiaddrProtocol::QuicV1,
        ] => {
            let socket = UdpSocket::bind((*ip, 0))?;
            let port = socket.local_addr()?.port();
            drop(socket);
            let mut concrete = Multiaddr::empty();
            concrete.push(MultiaddrProtocol::Ip6(*ip));
            concrete.push(MultiaddrProtocol::Udp(port));
            concrete.push(MultiaddrProtocol::QuicV1);
            Ok(concrete)
        }
        [
            MultiaddrProtocol::Ip4(ip),
            MultiaddrProtocol::Udp(0),
            MultiaddrProtocol::WebRTCDirect,
        ] => {
            let socket = UdpSocket::bind((*ip, 0))?;
            let port = socket.local_addr()?.port();
            drop(socket);
            let mut concrete = Multiaddr::empty();
            concrete.push(MultiaddrProtocol::Ip4(*ip));
            concrete.push(MultiaddrProtocol::Udp(port));
            concrete.push(MultiaddrProtocol::WebRTCDirect);
            Ok(concrete)
        }
        [
            MultiaddrProtocol::Ip6(ip),
            MultiaddrProtocol::Udp(0),
            MultiaddrProtocol::WebRTCDirect,
        ] => {
            let socket = UdpSocket::bind((*ip, 0))?;
            let port = socket.local_addr()?.port();
            drop(socket);
            let mut concrete = Multiaddr::empty();
            concrete.push(MultiaddrProtocol::Ip6(*ip));
            concrete.push(MultiaddrProtocol::Udp(port));
            concrete.push(MultiaddrProtocol::WebRTCDirect);
            Ok(concrete)
        }
        _ => Ok(address.clone()),
    }
}

#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
pub(crate) fn materialize_listen_addr(address: &Multiaddr) -> Result<Multiaddr, std::io::Error> {
    Ok(address.clone())
}
