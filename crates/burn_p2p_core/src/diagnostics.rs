/// Returns whether one runtime error should be hidden from public/operator-facing
/// status surfaces because it is expected transient swarm noise.
pub fn is_benign_operator_runtime_error(message: &str) -> bool {
    let normalized = message.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return true;
    }

    normalized.contains("nopeerssubscribedtotopic")
        || (targets_non_public_peer_address(&normalized)
            && normalized.contains("connection refused"))
}

fn targets_non_public_peer_address(message: &str) -> bool {
    message.contains("/ip4/127.")
        || message.contains("/ip4/10.")
        || message.contains("/ip4/192.168.")
        || message.contains("/ip6/::1")
        || message.contains("localhost")
        || (16..=31).any(|segment| message.contains(&format!("/ip4/172.{segment}.")))
}

/// Filters one runtime error for public/operator-facing status surfaces.
pub fn operator_visible_last_error(last_error: Option<&str>) -> Option<String> {
    let message = last_error?.trim();
    if message.is_empty() || is_benign_operator_runtime_error(message) {
        None
    } else {
        Some(message.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suppresses_pubsub_no_peer_noise() {
        assert!(
            operator_visible_last_error(Some("pubsub error: NoPeersSubscribedToTopic")).is_none()
        );
    }

    #[test]
    fn suppresses_loopback_connection_refused_noise() {
        assert!(operator_visible_last_error(Some(
            "Failed to negotiate transport protocol(s): [(/ip4/127.0.0.1/tcp/4001: Connection refused)]"
        ))
        .is_none());
        assert!(
            operator_visible_last_error(Some("dial /ip6/::1/tcp/4001 failed: Connection refused"))
                .is_none()
        );
    }

    #[test]
    fn suppresses_private_address_connection_refused_noise() {
        assert!(operator_visible_last_error(Some(
            "Failed to negotiate transport protocol(s): [(/ip4/10.42.1.10/tcp/45368/p2p/12D3KooWPeer: Connection refused (os error 111))]"
        ))
        .is_none());
        assert!(
            operator_visible_last_error(Some(
                "dial /ip4/172.20.8.4/tcp/4001 failed: Connection refused"
            ))
            .is_none()
        );
    }

    #[test]
    fn preserves_real_operator_errors() {
        assert_eq!(
            operator_visible_last_error(Some("artifact publication sync failed: EOF")),
            Some("artifact publication sync failed: EOF".into())
        );
    }
}
