// Copyright 2022 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use std::io;

use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, channel::oneshot, future::Either};
use futures_timer::Delay;
use libp2p_core::{
    UpgradeInfo,
    upgrade::{InboundConnectionUpgrade, OutboundConnectionUpgrade},
};
use libp2p_identity as identity;
use libp2p_identity::PeerId;
use libp2p_webrtc_utils::Fingerprint;
use webrtc::{
    api::{APIBuilder, setting_engine::SettingEngine},
    data::data_channel::DataChannel,
    data_channel::data_channel_init::RTCDataChannelInit,
    dtls_transport::dtls_role::DTLSRole,
    ice::{network_type::NetworkType, udp_mux::UDPMux, udp_network::UDPNetwork},
    peer_connection::{RTCPeerConnection, configuration::RTCConfiguration},
};

use crate::tokio::{Connection, error::Error, sdp, sdp::random_ufrag, stream::Stream};

/// Creates a new outbound WebRTC connection.
pub(crate) async fn outbound(
    addr: SocketAddr,
    config: RTCConfiguration,
    udp_mux: Arc<dyn UDPMux + Send + Sync>,
    client_fingerprint: Fingerprint,
    server_fingerprint: Fingerprint,
    id_keys: identity::Keypair,
) -> Result<(PeerId, Connection), Error> {
    tracing::debug!(address=%addr, "new outbound connection to address");

    let (peer_connection, ufrag) = new_outbound_connection(addr, config, udp_mux).await?;
    let handshake_channel = create_negotiated_handshake_data_channel(&peer_connection).await?;

    let offer = peer_connection.create_offer(None).await?;
    tracing::debug!(offer=%offer.sdp, "created SDP offer for outbound connection");
    peer_connection.set_local_description(offer).await?;

    let answer = sdp::answer(addr, server_fingerprint, &ufrag);
    tracing::debug!(?answer, "calculated SDP answer for outbound connection");
    peer_connection.set_remote_description(answer).await?; // This will start the gathering of ICE candidates.

    let data_channel = wait_for_negotiated_handshake_data_channel(handshake_channel).await?;
    let peer_id = noise_outbound(
        id_keys,
        data_channel,
        server_fingerprint,
        client_fingerprint,
    )
    .await?;

    Ok((peer_id, Connection::new(peer_connection).await))
}

/// Creates a new inbound WebRTC connection.
pub(crate) async fn inbound(
    addr: SocketAddr,
    config: RTCConfiguration,
    udp_mux: Arc<dyn UDPMux + Send + Sync>,
    server_fingerprint: Fingerprint,
    remote_ufrag: String,
    id_keys: identity::Keypair,
) -> Result<(PeerId, Connection), Error> {
    tracing::debug!(address=%addr, ufrag=%remote_ufrag, "new inbound connection from address");

    let peer_connection = new_inbound_connection(addr, config, udp_mux, &remote_ufrag).await?;
    let handshake_channel = create_negotiated_handshake_data_channel(&peer_connection).await?;

    let offer = sdp::offer(addr, &remote_ufrag);
    tracing::debug!(?offer, "calculated SDP offer for inbound connection");
    peer_connection.set_remote_description(offer).await?;

    let answer = peer_connection.create_answer(None).await?;
    tracing::debug!(?answer, "created SDP answer for inbound connection");
    peer_connection.set_local_description(answer).await?; // This will start the gathering of ICE candidates.

    let data_channel = wait_for_negotiated_handshake_data_channel(handshake_channel).await?;
    let client_fingerprint = get_remote_fingerprint(&peer_connection).await;
    let peer_id = noise_inbound(
        id_keys,
        data_channel,
        client_fingerprint,
        server_fingerprint,
    )
    .await?;

    Ok((peer_id, Connection::new(peer_connection).await))
}

async fn noise_inbound<T>(
    id_keys: identity::Keypair,
    stream: T,
    client_fingerprint: Fingerprint,
    server_fingerprint: Fingerprint,
) -> Result<PeerId, libp2p_noise::Error>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let noise = libp2p_noise::Config::new(&id_keys)
        .expect("identity keypair should create a valid Noise config")
        .with_prologue(noise_prologue(client_fingerprint, server_fingerprint));
    let info = noise
        .protocol_info()
        .next()
        .expect("Noise config should expose protocol info");

    // WebRTC direct reverses the Noise roles: the direct listener sends first.
    let (peer_id, mut channel) = noise.upgrade_outbound(stream, info).await?;
    close_noise_handshake_channel(&mut channel).await?;

    Ok(peer_id)
}

async fn noise_outbound<T>(
    id_keys: identity::Keypair,
    stream: T,
    server_fingerprint: Fingerprint,
    client_fingerprint: Fingerprint,
) -> Result<PeerId, libp2p_noise::Error>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let noise = libp2p_noise::Config::new(&id_keys)
        .expect("identity keypair should create a valid Noise config")
        .with_prologue(noise_prologue(client_fingerprint, server_fingerprint));
    let info = noise
        .protocol_info()
        .next()
        .expect("Noise config should expose protocol info");

    // WebRTC direct reverses the Noise roles: the browser/outbound side receives first.
    let (peer_id, mut channel) = noise.upgrade_inbound(stream, info).await?;
    close_noise_handshake_channel(&mut channel).await?;

    Ok(peer_id)
}

async fn close_noise_handshake_channel<T>(
    channel: &mut libp2p_noise::Output<T>,
) -> Result<(), libp2p_noise::Error>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    match channel.close().await {
        Ok(()) => Ok(()),
        Err(error) if is_benign_noise_close_error(&error) => {
            tracing::debug!(
                %error,
                "ignoring benign WebRTC Noise handshake channel close race"
            );
            Ok(())
        }
        Err(error) => Err(libp2p_noise::Error::Io(error)),
    }
}

fn is_benign_noise_close_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
}

fn noise_prologue(client_fingerprint: Fingerprint, server_fingerprint: Fingerprint) -> Vec<u8> {
    let client = client_fingerprint.to_multihash().to_bytes();
    let server = server_fingerprint.to_multihash().to_bytes();
    const PREFIX: &[u8] = b"libp2p-webrtc-noise:";

    let mut out = Vec::with_capacity(PREFIX.len() + client.len() + server.len());
    out.extend_from_slice(PREFIX);
    out.extend_from_slice(&client);
    out.extend_from_slice(&server);
    out
}

async fn new_outbound_connection(
    addr: SocketAddr,
    config: RTCConfiguration,
    udp_mux: Arc<dyn UDPMux + Send + Sync>,
) -> Result<(RTCPeerConnection, String), Error> {
    let ufrag = random_ufrag();
    let se = setting_engine(udp_mux, &ufrag, addr);

    let connection = APIBuilder::new()
        .with_setting_engine(se)
        .build()
        .new_peer_connection(config)
        .await?;

    Ok((connection, ufrag))
}

async fn new_inbound_connection(
    addr: SocketAddr,
    config: RTCConfiguration,
    udp_mux: Arc<dyn UDPMux + Send + Sync>,
    ufrag: &str,
) -> Result<RTCPeerConnection, Error> {
    let mut se = setting_engine(udp_mux, ufrag, addr);
    {
        se.set_lite(true);
        se.disable_certificate_fingerprint_verification(true);
        // Act as a DTLS server (one which waits for a connection).
        //
        // NOTE: removing this seems to break DTLS setup (both sides send `ClientHello` messages,
        // but none end up responding).
        se.set_answering_dtls_role(DTLSRole::Server)?;
    }

    let connection = APIBuilder::new()
        .with_setting_engine(se)
        .build()
        .new_peer_connection(config)
        .await?;

    Ok(connection)
}

fn setting_engine(
    udp_mux: Arc<dyn UDPMux + Send + Sync>,
    ufrag: &str,
    addr: SocketAddr,
) -> SettingEngine {
    let mut se = SettingEngine::default();

    // Set both ICE user and password to our fingerprint because that's what the client is
    // expecting..
    se.set_ice_credentials(ufrag.to_owned(), ufrag.to_owned());

    se.set_udp_network(UDPNetwork::Muxed(udp_mux.clone()));

    // Allow detaching data channels.
    se.detach_data_channels();

    // Set the desired network type.
    //
    // NOTE: if not set, a [`webrtc_ice::agent::Agent`] might pick a wrong local candidate
    // (e.g. IPv6 `[::1]` while dialing an IPv4 `10.11.12.13`).
    let network_type = match addr {
        SocketAddr::V4(_) => NetworkType::Udp4,
        SocketAddr::V6(_) => NetworkType::Udp6,
    };
    se.set_network_types(vec![network_type]);

    // Select only the first address of the local candidates.
    // See https://github.com/libp2p/rust-libp2p/pull/5448#discussion_r2017418520.
    // TODO: remove when https://github.com/webrtc-rs/webrtc/issues/662 get's addressed.
    se.set_ip_filter(Box::new({
        let once = AtomicBool::new(true);
        move |_ip| {
            if once.load(Ordering::Relaxed) {
                once.store(false, Ordering::Relaxed);
                return true;
            }
            false
        }
    }));

    se
}

/// Returns the SHA-256 fingerprint of the remote.
async fn get_remote_fingerprint(conn: &RTCPeerConnection) -> Fingerprint {
    let cert_bytes = conn.sctp().transport().get_remote_certificate().await;

    Fingerprint::from_certificate(&cert_bytes)
}

async fn create_negotiated_handshake_data_channel(
    conn: &RTCPeerConnection,
) -> Result<oneshot::Receiver<Arc<DataChannel>>, Error> {
    // The negotiated id=0 data channel must exist before SDP starts ICE/DTLS.
    // Firefox can reach SCTP immediately after the answer is applied and fails
    // if the native listener creates its matching channel after that point.
    let data_channel = conn
        .create_data_channel(
            "",
            Some(RTCDataChannelInit {
                negotiated: Some(0), // 0 is reserved for the Noise substream
                ..RTCDataChannelInit::default()
            }),
        )
        .await?;

    let (tx, rx) = oneshot::channel::<Arc<DataChannel>>();
    crate::tokio::connection::register_data_channel_open_handler(data_channel, tx).await;
    Ok(rx)
}

async fn wait_for_negotiated_handshake_data_channel(
    rx: oneshot::Receiver<Arc<DataChannel>>,
) -> Result<Stream, Error> {
    let channel = match futures::future::select(rx, Delay::new(Duration::from_secs(10))).await {
        Either::Left((Ok(channel), _)) => channel,
        Either::Left((Err(_), _)) => {
            return Err(Error::Internal("failed to open data channel".to_owned()));
        }
        Either::Right(((), _)) => {
            return Err(Error::Internal(
                "data channel opening took longer than 10 seconds (see logs)".into(),
            ));
        }
    };

    let (substream, drop_listener) = Stream::new(channel);
    drop(drop_listener); // Don't care about cancelled substreams during initial handshake.

    Ok(substream)
}

#[cfg(test)]
mod tests {
    use super::is_benign_noise_close_error;
    use std::io;

    #[test]
    fn benign_noise_close_errors_cover_datachannel_shutdown_races() {
        for kind in [
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::NotConnected,
            io::ErrorKind::UnexpectedEof,
        ] {
            assert!(
                is_benign_noise_close_error(&io::Error::from(kind)),
                "{kind:?} should be tolerated after Noise peer authentication"
            );
        }
    }

    #[test]
    fn non_shutdown_noise_close_errors_still_fail() {
        assert!(!is_benign_noise_close_error(&io::Error::from(
            io::ErrorKind::PermissionDenied
        )));
        assert!(!is_benign_noise_close_error(&io::Error::from(
            io::ErrorKind::InvalidData
        )));
    }
}
