use std::net::SocketAddr;

use libp2p_identity::{Keypair, PeerId};
use libp2p_webrtc_utils::{noise, Fingerprint};
use send_wrapper::SendWrapper;
use wasm_bindgen::JsValue;

use super::Error;
use crate::{connection::RtcPeerConnection, error::AuthenticationError, sdp, Connection};

fn console_debug(message: impl AsRef<str>) {
    web_sys::console::debug_1(&JsValue::from_str(message.as_ref()));
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

    // Create stream for Noise handshake
    // Must create data channel before Offer is created for it to be included in the SDP
    let (channel, listener) = rtc_peer_connection.new_handshake_stream();
    drop(listener);
    console_debug("libp2p webrtc-direct: created negotiated handshake datachannel id=0");

    let ufrag = libp2p_webrtc_utils::sdp::random_ufrag();

    let offer = rtc_peer_connection.create_offer().await?;
    console_debug(format!(
        "libp2p webrtc-direct: created browser offer bytes={}",
        offer.len()
    ));
    let munged_offer = sdp::offer(offer, &ufrag);
    rtc_peer_connection
        .set_local_description(munged_offer)
        .await?;

    let answer = sdp::answer(sock_addr, remote_fingerprint, &ufrag);
    rtc_peer_connection.set_remote_description(answer).await?;

    let local_fingerprint = rtc_peer_connection.local_fingerprint()?;
    rtc_peer_connection.log_state("before-noise");

    tracing::trace!(?local_fingerprint);
    tracing::trace!(?remote_fingerprint);

    console_debug("libp2p webrtc-direct: starting Noise handshake over WebRTC datachannel");
    let peer_id = noise::outbound(id_keys, channel, remote_fingerprint, local_fingerprint)
        .await
        .map_err(AuthenticationError)?;
    console_debug(format!(
        "libp2p webrtc-direct: completed Noise handshake peer={}",
        peer_id
    ));

    tracing::debug!(peer=%peer_id, "Remote peer identified");

    Ok((peer_id, Connection::new(rtc_peer_connection)))
}
