use libp2p_webrtc_utils::Fingerprint;
use web_sys::{RtcSdpType, RtcSessionDescriptionInit};

use crate::transport::WebRtcDialTarget;

/// Creates the SDP answer used by the client.
pub(crate) fn answer(
    target: &WebRtcDialTarget,
    server_fingerprint: Fingerprint,
    client_ufrag: &str,
    browser_offer: &str,
) -> RtcSessionDescriptionInit {
    let answer_obj = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
    let answer = render_answer(target, server_fingerprint, client_ufrag);
    answer_obj.set_sdp(&answer_for_browser_offer(answer, browser_offer));
    answer_obj
}

fn render_answer(
    target: &WebRtcDialTarget,
    server_fingerprint: Fingerprint,
    client_ufrag: &str,
) -> String {
    if let Some(addr) = target.socket_addr() {
        return libp2p_webrtc_utils::sdp::answer(addr, server_fingerprint, client_ufrag);
    }

    let (connection_address_type, connection_address) = target.connection_address();
    let candidate_address = target.candidate_address();
    format!(
        "v=0\r\n\
         o=- 0 0 IN {connection_address_type} {connection_address}\r\n\
         s=-\r\n\
         t=0 0\r\n\
         a=ice-lite\r\n\
         m=application {target_port} UDP/DTLS/SCTP webrtc-datachannel\r\n\
         c=IN {connection_address_type} {connection_address}\r\n\
         a=mid:0\r\n\
         a=ice-options:ice2\r\n\
         a=ice-ufrag:{client_ufrag}\r\n\
         a=ice-pwd:{client_ufrag}\r\n\
         a=fingerprint:{fingerprint_algorithm} {fingerprint_value}\r\n\
         a=setup:passive\r\n\
         a=sctp-port:5000\r\n\
         a=max-message-size:16384\r\n\
         a=candidate:1467250027 1 UDP 1467250027 {candidate_address} {target_port} typ host\r\n\
         a=end-of-candidates\r\n",
        target_port = target.port(),
        fingerprint_algorithm = server_fingerprint.algorithm(),
        fingerprint_value = server_fingerprint.to_sdp_format(),
    )
}

fn answer_for_browser_offer(answer: String, browser_offer: &str) -> String {
    let mut lines = sdp_lines(&answer);

    for prefix in ["a=group:", "a=msid-semantic:"] {
        if let Some(line) = offer_line_with_prefix(browser_offer, prefix) {
            ensure_session_line_before_media(&mut lines, prefix, line);
        }
    }

    if let Some(line) = offer_line_with_prefix(browser_offer, "a=ice-options:") {
        replace_or_insert_media_line(&mut lines, "a=ice-options:", line, "a=mid:");
    }

    if offer_has_line(browser_offer, "a=sendrecv") {
        ensure_media_line_after(&mut lines, "a=sendrecv", "c=");
    }

    render_sdp_lines(lines)
}

fn sdp_lines(sdp: &str) -> Vec<String> {
    sdp.lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn render_sdp_lines(lines: Vec<String>) -> String {
    let mut sdp = lines.join("\r\n");
    sdp.push_str("\r\n");
    sdp
}

fn offer_line_with_prefix(offer: &str, prefix: &str) -> Option<String> {
    offer
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with(prefix))
        .map(ToOwned::to_owned)
}

fn offer_has_line(offer: &str, expected: &str) -> bool {
    offer.lines().map(str::trim).any(|line| line == expected)
}

fn ensure_session_line_before_media(lines: &mut Vec<String>, prefix: &str, line: String) {
    if lines.iter().any(|existing| existing.starts_with(prefix)) {
        return;
    }
    let insertion_index = lines
        .iter()
        .position(|existing| existing.starts_with("m="))
        .unwrap_or(lines.len());
    lines.insert(insertion_index, line);
}

fn replace_or_insert_media_line(
    lines: &mut Vec<String>,
    prefix: &str,
    line: String,
    insert_after_prefix: &str,
) {
    if let Some(existing) = lines
        .iter_mut()
        .find(|existing| existing.starts_with(prefix))
    {
        *existing = line;
        return;
    }
    let insertion_index = lines
        .iter()
        .position(|existing| existing.starts_with(insert_after_prefix))
        .map(|index| index + 1)
        .unwrap_or(lines.len());
    lines.insert(insertion_index, line);
}

fn ensure_media_line_after(lines: &mut Vec<String>, line: &str, insert_after_prefix: &str) {
    if lines.iter().any(|existing| existing == line) {
        return;
    }
    let insertion_index = lines
        .iter()
        .position(|existing| existing.starts_with(insert_after_prefix))
        .map(|index| index + 1)
        .unwrap_or(lines.len());
    lines.insert(insertion_index, line.to_owned());
}

/// Creates the munged SDP offer from the Browser's given SDP offer
///
/// Certificate verification is disabled which is why we hardcode a dummy fingerprint here.
pub(crate) fn offer(offer: &str, client_ufrag: &str) -> RtcSessionDescriptionInit {
    // find line and replace a=ice-ufrag: with "\r\na=ice-ufrag:{client_ufrag}\r\n"
    // find line and replace a=ice-pwd: with "\r\na=ice-ufrag:{client_ufrag}\r\n"

    let mut munged_sdp_offer = String::new();

    for line in offer.lines().map(str::trim_end) {
        if line.starts_with("a=ice-ufrag:") {
            munged_sdp_offer.push_str(&format!("a=ice-ufrag:{client_ufrag}\r\n"));
            continue;
        }

        if line.starts_with("a=ice-pwd:") {
            munged_sdp_offer.push_str(&format!("a=ice-pwd:{client_ufrag}\r\n"));
            continue;
        }

        if !line.is_empty() {
            munged_sdp_offer.push_str(&format!("{}\r\n", line));
            continue;
        }
    }

    // remove any double \r\n
    let munged_sdp_offer = munged_sdp_offer.replace("\r\n\r\n", "\r\n");

    tracing::trace!(offer=%munged_sdp_offer, "Created SDP offer");

    let offer_obj = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
    offer_obj.set_sdp(&munged_sdp_offer);

    offer_obj
}

#[cfg(test)]
mod tests {
    use super::{answer_for_browser_offer, render_answer};
    use crate::transport::parse_webrtc_dial_target;
    use libp2p_webrtc_utils::Fingerprint;

    const BASE_ANSWER: &str = "v=0\r\no=- 0 0 IN IP4 1.2.3.4\r\ns=-\r\nt=0 0\r\na=ice-lite\r\nm=application 443 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 1.2.3.4\r\na=mid:0\r\na=ice-options:ice2\r\na=ice-ufrag:ufrag\r\na=ice-pwd:ufrag\r\na=fingerprint:sha-256 AA:BB\r\na=setup:passive\r\na=sctp-port:5000\r\na=max-message-size:16384\r\na=candidate:1467250027 1 UDP 1467250027 1.2.3.4 443 typ host\r\na=end-of-candidates\r\n";

    #[test]
    fn answer_mirrors_firefox_session_attributes() {
        let firefox_offer = "v=0\r\no=mozilla...THIS_IS_SDPARTA-99.0 1 0 IN IP4 0.0.0.0\r\ns=-\r\nt=0 0\r\na=fingerprint:sha-256 AA:BB\r\na=group:BUNDLE 0\r\na=ice-options:trickle\r\na=msid-semantic:WMS *\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nc=IN IP4 0.0.0.0\r\na=sendrecv\r\na=ice-pwd:pwd\r\na=ice-ufrag:ufrag\r\na=mid:0\r\na=setup:actpass\r\na=sctp-port:5000\r\na=max-message-size:1073741823\r\n";

        let answer = answer_for_browser_offer(BASE_ANSWER.to_owned(), firefox_offer);

        assert!(answer.contains("a=group:BUNDLE 0\r\n"));
        assert!(answer.contains("a=msid-semantic:WMS *\r\n"));
        assert!(answer.contains("a=sendrecv\r\n"));
        assert!(answer.contains("a=ice-options:trickle\r\n"));
        assert!(!answer.contains("a=ice-options:ice2\r\n"));
        assert!(answer.ends_with("\r\n"));
    }

    #[test]
    fn answer_remains_stable_for_minimal_browser_offer() {
        let answer = answer_for_browser_offer(BASE_ANSWER.to_owned(), "v=0\r\nt=0 0\r\n");

        assert!(!answer.contains("a=group:"));
        assert!(!answer.contains("a=sendrecv\r\n"));
        assert!(answer.contains("a=ice-options:ice2\r\n"));
    }

    #[test]
    fn answer_renders_dns_candidate_with_neutral_connection_address() {
        let addr = "/dns4/edge.dragon.aberration.technology/udp/443/webrtc-direct/certhash/uEiDikp5KVUgkLta1EjUN-IKbHk-dUBg8VzKgf5nXxLK46w"
            .parse()
            .expect("multiaddr");
        let (target, _fingerprint) = parse_webrtc_dial_target(&addr).expect("dial target");

        let answer = render_answer(
            &target,
            Fingerprint::raw([0xAA; 32]),
            "libp2p+webrtc+v1/test",
        );

        assert!(answer.contains("c=IN IP4 0.0.0.0\r\n"));
        assert!(answer.contains(
            "a=candidate:1467250027 1 UDP 1467250027 edge.dragon.aberration.technology 443 typ host\r\n"
        ));
    }
}
