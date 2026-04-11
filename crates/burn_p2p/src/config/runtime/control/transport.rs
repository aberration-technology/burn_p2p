use super::*;

pub(super) fn split_fetch_timeout(timeout: Duration) -> (Duration, Duration) {
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
