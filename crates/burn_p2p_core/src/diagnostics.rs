/// Returns whether one runtime error should be hidden from public/operator-facing
/// status surfaces because it is expected transient swarm noise.
pub fn is_benign_operator_runtime_error(message: &str) -> bool {
    let normalized = message.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return true;
    }

    normalized.contains("nopeerssubscribedtotopic")
        || ((normalized.contains("/ip4/127.0.0.1")
            || normalized.contains("/ip6/::1")
            || normalized.contains("localhost"))
            && normalized.contains("connection refused"))
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
    fn preserves_real_operator_errors() {
        assert_eq!(
            operator_visible_last_error(Some("artifact publication sync failed: EOF")),
            Some("artifact publication sync failed: EOF".into())
        );
    }
}
