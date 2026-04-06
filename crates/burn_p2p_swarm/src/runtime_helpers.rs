use std::convert::Infallible;

#[cfg(not(target_arch = "wasm32"))]
use std::net::{TcpListener, UdpSocket};

use libp2p::{Multiaddr, StreamProtocol, multiaddr::Protocol as MultiaddrProtocol};
#[cfg(not(target_arch = "wasm32"))]
use libp2p_identity::Keypair;
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
        _ => Ok(address.clone()),
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn materialize_listen_addr(address: &Multiaddr) -> Result<Multiaddr, std::io::Error> {
    Ok(address.clone())
}
