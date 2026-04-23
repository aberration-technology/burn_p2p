use std::{cell::RefCell, io, net::SocketAddr, rc::Rc};

use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, channel::oneshot};
use libp2p_core::{UpgradeInfo, upgrade::InboundConnectionUpgrade};

use libp2p_identity::{Keypair, PeerId};
use libp2p_webrtc_utils::Fingerprint;
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use web_sys::{Event, RtcDataChannel, RtcDataChannelEvent, RtcDataChannelState};

use super::Error;
use crate::{Connection, Stream, connection::RtcPeerConnection, error::AuthenticationError, sdp};

fn console_debug(message: impl AsRef<str>) {
    web_sys::console::debug_1(&JsValue::from_str(message.as_ref()));
}

fn data_channel_state_label(state: RtcDataChannelState) -> &'static str {
    match state {
        RtcDataChannelState::Connecting => "connecting",
        RtcDataChannelState::Open => "open",
        RtcDataChannelState::Closing => "closing",
        RtcDataChannelState::Closed => "closed",
        RtcDataChannelState::__Invalid => "invalid",
        _ => "unknown",
    }
}

fn send_handshake_open_result(
    sender: &Rc<RefCell<Option<oneshot::Sender<Result<(), String>>>>>,
    result: Result<(), String>,
) {
    if let Some(sender) = sender.borrow_mut().take() {
        let _ = sender.send(result);
    }
}

async fn wait_for_handshake_data_channel_open(channel: &RtcDataChannel) -> Result<(), Error> {
    match channel.ready_state() {
        RtcDataChannelState::Open => return Ok(()),
        RtcDataChannelState::Closing | RtcDataChannelState::Closed => {
            return Err(Error::Js(format!(
                "handshake datachannel closed before open: ready_state={}",
                data_channel_state_label(channel.ready_state())
            )));
        }
        RtcDataChannelState::Connecting | RtcDataChannelState::__Invalid => {}
        _ => {}
    }

    let (sender, receiver) = oneshot::channel::<Result<(), String>>();
    let sender = Rc::new(RefCell::new(Some(sender)));

    let on_open = Closure::<dyn FnMut(RtcDataChannelEvent)>::new({
        let sender = sender.clone();
        move |_: RtcDataChannelEvent| {
            console_debug("libp2p webrtc-direct datachannel: open before-noise");
            send_handshake_open_result(&sender, Ok(()));
        }
    });
    channel.set_onopen(Some(on_open.as_ref().unchecked_ref()));

    let on_close = Closure::<dyn FnMut(Event)>::new({
        let channel = channel.clone();
        let sender = sender.clone();
        move |_: Event| {
            let message = format!(
                "handshake datachannel closed before open: ready_state={}",
                data_channel_state_label(channel.ready_state())
            );
            console_debug(format!("libp2p webrtc-direct datachannel: {message}"));
            send_handshake_open_result(&sender, Err(message));
        }
    });
    channel.set_onclose(Some(on_close.as_ref().unchecked_ref()));

    let on_error = Closure::<dyn FnMut(Event)>::new({
        let channel = channel.clone();
        let sender = sender.clone();
        move |_: Event| {
            let message = format!(
                "handshake datachannel error before open: ready_state={}",
                data_channel_state_label(channel.ready_state())
            );
            console_debug(format!("libp2p webrtc-direct datachannel: {message}"));
            send_handshake_open_result(&sender, Err(message));
        }
    });
    channel.set_onerror(Some(on_error.as_ref().unchecked_ref()));

    let outcome = receiver
        .await
        .map_err(|_| Error::Js("handshake datachannel open waiter was canceled".to_owned()));

    channel.set_onopen(None);
    channel.set_onclose(None);
    channel.set_onerror(None);

    match outcome {
        Ok(result) => result.map_err(Error::Js),
        Err(error) => Err(error),
    }
}

/// Upgrades an outbound WebRTC connection by creating the data channel
/// and conducting a Noise handshake
pub(crate) async fn outbound(
    sock_addr: SocketAddr,
    remote_fingerprint: Fingerprint,
    id_keys: Keypair,
) -> Result<(PeerId, Connection), Error> {
    let fut = SendWrapper::new(outbound_inner(sock_addr, remote_fingerprint, id_keys));
    fut.await
}

/// Inner outbound function that is wrapped in [SendWrapper]
async fn outbound_inner(
    sock_addr: SocketAddr,
    remote_fingerprint: Fingerprint,
    id_keys: Keypair,
) -> Result<(PeerId, Connection), Error> {
    console_debug(format!(
        "libp2p webrtc-direct: starting outbound upgrade addr={} remote_fingerprint={}",
        sock_addr,
        remote_fingerprint.to_sdp_format(),
    ));
    let rtc_peer_connection = RtcPeerConnection::new(remote_fingerprint.algorithm()).await?;
    rtc_peer_connection.log_state("created");

    // Must create the data channel before creating the offer so it is included in SDP.
    let data_channel = rtc_peer_connection.new_handshake_data_channel();
    console_debug("libp2p webrtc-direct: created negotiated handshake datachannel id=0");

    let ufrag = libp2p_webrtc_utils::sdp::random_ufrag();

    let offer = rtc_peer_connection.create_offer().await?;
    console_debug(format!(
        "libp2p webrtc-direct: created browser offer bytes={}",
        offer.len()
    ));
    let munged_offer = sdp::offer(&offer, &ufrag);
    rtc_peer_connection
        .set_local_description(munged_offer)
        .await?;

    let answer = sdp::answer(sock_addr, remote_fingerprint, &ufrag, &offer);
    rtc_peer_connection.set_remote_description(answer).await?;

    let local_fingerprint = rtc_peer_connection.local_fingerprint()?;
    rtc_peer_connection.log_state("before-datachannel-open");
    wait_for_handshake_data_channel_open(&data_channel).await?;
    let (channel, listener) = Stream::new(data_channel);
    drop(listener);
    rtc_peer_connection.log_state("before-noise");

    tracing::trace!(?local_fingerprint);
    tracing::trace!(?remote_fingerprint);

    console_debug("libp2p webrtc-direct: starting Noise handshake over WebRTC datachannel");
    let peer_id = noise_outbound(id_keys, channel, remote_fingerprint, local_fingerprint)
        .await
        .map_err(AuthenticationError)?;
    console_debug(format!(
        "libp2p webrtc-direct: completed Noise handshake peer={}",
        peer_id
    ));

    tracing::debug!(peer=%peer_id, "Remote peer identified");

    Ok((peer_id, Connection::new(rtc_peer_connection)))
}

async fn noise_outbound<T>(
    id_keys: Keypair,
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

    // WebRTC direct reverses the Noise roles: the direct listener sends first.
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
            console_debug(format!(
                "libp2p webrtc-direct: ignoring benign Noise channel close race: {error}"
            ));
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
