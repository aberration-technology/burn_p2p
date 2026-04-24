use std::{
    fmt,
    future::Future,
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
};

use futures::future::FutureExt;
use libp2p_core::{
    multiaddr::{Multiaddr, Protocol},
    muxing::StreamMuxerBox,
    transport::{Boxed, DialOpts, ListenerId, Transport as _, TransportError, TransportEvent},
};
use libp2p_identity::{Keypair, PeerId};
use wasm_bindgen::JsValue;

use super::{Connection, Error, upgrade};

fn console_debug(message: impl AsRef<str>) {
    web_sys::console::debug_1(&JsValue::from_str(message.as_ref()));
}

/// Config for the [`Transport`].
#[derive(Clone)]
pub struct Config {
    keypair: Keypair,
    ice_servers: Vec<String>,
}

/// A WebTransport [`Transport`](libp2p_core::Transport) that works with `web-sys`.
pub struct Transport {
    config: Config,
}

const DEFAULT_ICE_SERVERS: &[&str] = &[
    "stun:stun.cloudflare.com:3478",
    "stun:stun.l.google.com:19302",
];

impl Config {
    /// Constructs a new configuration for the [`Transport`].
    pub fn new(keypair: &Keypair) -> Self {
        Config {
            keypair: keypair.to_owned(),
            ice_servers: DEFAULT_ICE_SERVERS
                .iter()
                .map(|server| (*server).to_owned())
                .collect(),
        }
    }

    /// Returns a copy configured with explicit browser ICE servers.
    pub fn with_ice_servers<I, S>(mut self, ice_servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.ice_servers = ice_servers.into_iter().map(Into::into).collect();
        self
    }

    /// Returns the configured browser ICE server URLs.
    pub fn ice_servers(&self) -> &[String] {
        &self.ice_servers
    }
}

impl Transport {
    /// Constructs a new `Transport` with the given [`Config`].
    pub fn new(config: Config) -> Transport {
        Transport { config }
    }

    /// Wraps `Transport` in [`Boxed`] and makes it ready to be consumed by
    /// SwarmBuilder.
    pub fn boxed(self) -> Boxed<(PeerId, StreamMuxerBox)> {
        self.map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .boxed()
    }
}

impl libp2p_core::Transport for Transport {
    type Output = (PeerId, Connection);
    type Error = Error;
    type ListenerUpgrade = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;
    type Dial = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn listen_on(
        &mut self,
        _id: ListenerId,
        addr: Multiaddr,
    ) -> Result<(), TransportError<Self::Error>> {
        Err(TransportError::MultiaddrNotSupported(addr))
    }

    fn remove_listener(&mut self, _id: ListenerId) -> bool {
        false
    }

    fn dial(
        &mut self,
        addr: Multiaddr,
        dial_opts: DialOpts,
    ) -> Result<Self::Dial, TransportError<Self::Error>> {
        if dial_opts.role.is_listener() {
            return Err(TransportError::MultiaddrNotSupported(addr));
        }

        if maybe_local_firefox() {
            return Err(TransportError::Other(
                "Firefox does not support WebRTC over localhost or 127.0.0.1"
                    .to_string()
                    .into(),
            ));
        }

        let (target, server_fingerprint) = parse_webrtc_dial_target(&addr)
            .ok_or_else(|| TransportError::MultiaddrNotSupported(addr.clone()))?;

        if target.port == 0 || target.is_unspecified_ip() {
            return Err(TransportError::MultiaddrNotSupported(addr));
        }
        let config = self.config.clone();
        console_debug(format!(
            "libp2p webrtc-direct: dialing browser seed multiaddr={} target={} fingerprint={} ice_servers={}",
            addr,
            target,
            server_fingerprint.to_sdp_format(),
            config.ice_servers.len(),
        ));

        Ok(async move {
            let (peer_id, connection) = upgrade::outbound(
                target,
                server_fingerprint,
                config.keypair.clone(),
                config.ice_servers,
            )
            .await?;

            Ok((peer_id, connection))
        }
        .boxed())
    }

    fn poll(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<TransportEvent<Self::ListenerUpgrade, Self::Error>> {
        Poll::Pending
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WebRtcDialTarget {
    address: WebRtcDialAddress,
    port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum WebRtcDialAddress {
    Ip(IpAddr),
    Dns(String),
}

impl WebRtcDialTarget {
    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn candidate_address(&self) -> String {
        match &self.address {
            WebRtcDialAddress::Ip(ip) => ip.to_string(),
            WebRtcDialAddress::Dns(host) => host.clone(),
        }
    }

    pub(crate) fn connection_address(&self) -> (&'static str, String) {
        match &self.address {
            WebRtcDialAddress::Ip(IpAddr::V4(ip)) => ("IP4", ip.to_string()),
            WebRtcDialAddress::Ip(IpAddr::V6(ip)) => ("IP6", ip.to_string()),
            WebRtcDialAddress::Dns(_) => ("IP4", "0.0.0.0".to_owned()),
        }
    }

    pub(crate) fn socket_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.address {
            WebRtcDialAddress::Ip(ip) => Some(std::net::SocketAddr::new(*ip, self.port)),
            WebRtcDialAddress::Dns(_) => None,
        }
    }

    fn is_unspecified_ip(&self) -> bool {
        matches!(self.address, WebRtcDialAddress::Ip(ip) if ip.is_unspecified())
    }
}

impl fmt::Display for WebRtcDialTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.candidate_address(), self.port)
    }
}

pub(crate) fn parse_webrtc_dial_target(
    addr: &Multiaddr,
) -> Option<(WebRtcDialTarget, libp2p_webrtc_utils::Fingerprint)> {
    let mut iter = addr.iter();

    let address = match iter.next()? {
        Protocol::Ip4(ip) => WebRtcDialAddress::Ip(IpAddr::from(ip)),
        Protocol::Ip6(ip) => WebRtcDialAddress::Ip(IpAddr::from(ip)),
        Protocol::Dns(host)
        | Protocol::Dns4(host)
        | Protocol::Dns6(host)
        | Protocol::Dnsaddr(host) => {
            let host = host.trim();
            if host.is_empty() {
                return None;
            }
            WebRtcDialAddress::Dns(host.to_owned())
        }
        _ => return None,
    };

    let port = iter.next()?;
    let webrtc = iter.next()?;
    let certhash = iter.next()?;

    let (port, fingerprint) = match (port, webrtc, certhash) {
        (Protocol::Udp(port), Protocol::WebRTCDirect, Protocol::Certhash(cert_hash)) => {
            let fingerprint = libp2p_webrtc_utils::Fingerprint::try_from_multihash(cert_hash)?;
            (port, fingerprint)
        }
        _ => return None,
    };

    match iter.next() {
        Some(Protocol::P2p(_)) | None => {}
        Some(_) => return None,
    }

    Some((WebRtcDialTarget { address, port }, fingerprint))
}

/// Checks if local Firefox.
///
/// See: `<https://bugzilla.mozilla.org/show_bug.cgi?id=1659672>` for more details
fn maybe_local_firefox() -> bool {
    let window = &web_sys::window().expect("window should be available");
    let ua = match window.navigator().user_agent() {
        Ok(agent) => agent.to_lowercase(),
        Err(_) => return false,
    };

    let hostname = match window
        .document()
        .expect("should be valid document")
        .location()
    {
        Some(location) => match location.hostname() {
            Ok(hostname) => hostname,
            Err(_) => return false,
        },
        None => return false,
    };

    // check if web_sys::Navigator::user_agent() matches any of the following:
    // - firefox
    // - seamonkey
    // - iceape
    // AND hostname is either localhost or  "127.0.0.1"
    (ua.contains("firefox") || ua.contains("seamonkey") || ua.contains("iceape"))
        && (hostname == "localhost" || hostname == "127.0.0.1" || hostname == "[::1]")
}

#[cfg(test)]
mod tests {
    use super::{Config, DEFAULT_ICE_SERVERS, parse_webrtc_dial_target};

    #[test]
    fn default_config_includes_public_stun_servers() {
        let keypair =
            libp2p_identity::Keypair::ed25519_from_bytes([7_u8; 32]).expect("test keypair");
        let config = Config::new(&keypair);

        assert_eq!(config.ice_servers(), DEFAULT_ICE_SERVERS);
    }

    #[test]
    fn parses_dns_webrtc_direct_dial_targets() {
        let addr = "/dns4/edge.dragon.aberration.technology/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w"
            .parse()
            .expect("multiaddr");

        let (target, _fingerprint) = parse_webrtc_dial_target(&addr).expect("dial target");

        assert_eq!(
            target.candidate_address(),
            "edge.dragon.aberration.technology"
        );
        assert_eq!(target.connection_address(), ("IP4", "0.0.0.0".to_owned()));
        assert_eq!(target.port(), 443);
        assert_eq!(target.to_string(), "edge.dragon.aberration.technology:443");
    }
}
