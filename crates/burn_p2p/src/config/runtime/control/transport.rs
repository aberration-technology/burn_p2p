use super::*;

pub(super) fn split_fetch_timeout(
    snapshot: &NodeTelemetrySnapshot,
    peer_id: &PeerId,
    timeout: Duration,
) -> (Duration, Duration) {
    if crate::runtime_support::connected_peer_ids(snapshot).contains(peer_id) {
        return (timeout, Duration::ZERO);
    }
    if timeout <= Duration::from_millis(750) {
        return (timeout, Duration::ZERO);
    }
    let runtime_timeout = timeout.min(Duration::from_millis(750));
    (runtime_timeout, timeout.saturating_sub(runtime_timeout))
}

pub(super) fn sidecar_peer_addresses(
    snapshot: &NodeTelemetrySnapshot,
    peer_id: &PeerId,
) -> Vec<SwarmAddress> {
    let mut addresses = snapshot
        .control_plane
        .peer_directory_announcements
        .iter()
        .filter(|announcement| announcement.peer_id == *peer_id)
        .flat_map(|announcement| announcement.addresses.iter().cloned())
        .collect::<Vec<_>>();

    addresses.extend(
        snapshot
            .known_peer_addresses
            .iter()
            .filter(|address| address.as_str().contains(peer_id.as_str()))
            .cloned(),
    );

    addresses.sort_by(|left, right| {
        left.is_relay_circuit()
            .cmp(&right.is_relay_circuit())
            .then_with(|| left.as_str().cmp(right.as_str()))
    });
    addresses.dedup();
    addresses
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_snapshot() -> NodeTelemetrySnapshot {
        NodeTelemetrySnapshot::starting(
            &MainnetHandle {
                genesis: GenesisSpec {
                    network_id: NetworkId::new("transport-timeout-test"),
                    protocol_version: semver::Version::new(1, 0, 0),
                    display_name: String::from("transport-timeout-test"),
                    created_at: Utc::now(),
                    metadata: BTreeMap::new(),
                },
                roles: PeerRoleSet::default_trainer(),
            },
            &NodeConfig::default(),
        )
    }

    #[test]
    fn split_fetch_timeout_prefers_connected_runtime_path() {
        let peer_id = PeerId::new("12D3KooWConnectedFetchTimeout1111111111111111111111");
        let mut snapshot = test_snapshot();
        snapshot.observed_peer_ids.insert(peer_id.clone());

        let (runtime_timeout, fallback_timeout) =
            split_fetch_timeout(&snapshot, &peer_id, Duration::from_secs(10));

        assert_eq!(runtime_timeout, Duration::from_secs(10));
        assert_eq!(fallback_timeout, Duration::ZERO);
    }

    #[test]
    fn split_fetch_timeout_reserves_sidecar_for_disconnected_peer() {
        let peer_id = PeerId::new("12D3KooWDisconnectedFetchTimeout11111111111111111111");
        let snapshot = test_snapshot();

        let (runtime_timeout, fallback_timeout) =
            split_fetch_timeout(&snapshot, &peer_id, Duration::from_secs(10));

        assert_eq!(runtime_timeout, Duration::from_millis(750));
        assert_eq!(fallback_timeout, Duration::from_millis(9250));
    }
}
