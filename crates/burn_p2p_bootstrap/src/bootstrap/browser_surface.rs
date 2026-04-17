use super::*;
use std::net::IpAddr;

pub(super) fn browser_edge_mode(plan: &BootstrapPlan) -> BrowserEdgeMode {
    if plan.supports_service(&burn_p2p_bootstrap::BootstrapService::CoherenceSeed) {
        if plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Validator)
            || plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Archive)
            || plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Authority)
        {
            BrowserEdgeMode::Full
        } else {
            BrowserEdgeMode::Peer
        }
    } else {
        BrowserEdgeMode::Minimal
    }
}

pub(super) fn browser_transport_surface(
    plan: &BootstrapPlan,
    config: &BootstrapDaemonConfig,
    runtime_snapshot: Option<&burn_p2p::NodeTelemetrySnapshot>,
) -> BrowserTransportSurface {
    if !browser_join_enabled(config) {
        return BrowserTransportSurface {
            webrtc_direct: false,
            webtransport_gateway: false,
            wss_fallback: false,
        };
    }
    let edge_mode = browser_edge_mode(plan);
    BrowserTransportSurface {
        webrtc_direct: native_browser_webrtc_direct_supported()
            && browser_runtime_seed_addresses(runtime_snapshot, "webrtc-direct")
                .next()
                .is_some()
            && matches!(edge_mode, BrowserEdgeMode::Peer | BrowserEdgeMode::Full),
        webtransport_gateway: native_browser_webtransport_supported()
            && browser_runtime_seed_addresses(runtime_snapshot, "webtransport")
                .next()
                .is_some()
            && matches!(edge_mode, BrowserEdgeMode::Full),
        wss_fallback: native_browser_wss_supported()
            && matches!(edge_mode, BrowserEdgeMode::Peer | BrowserEdgeMode::Full),
    }
}

fn native_browser_webrtc_direct_supported() -> bool {
    burn_p2p_swarm::native_browser_webrtc_direct_runtime_supported()
}

fn native_browser_webtransport_supported() -> bool {
    burn_p2p_swarm::native_browser_webtransport_gateway_runtime_supported()
}

fn native_browser_wss_supported() -> bool {
    true
}

fn browser_seed_transport_policy(
    plan: &BootstrapPlan,
    config: &BootstrapDaemonConfig,
    runtime_snapshot: Option<&burn_p2p::NodeTelemetrySnapshot>,
) -> burn_p2p_core::BrowserSeedTransportPolicy {
    let surface = browser_transport_surface(plan, config, runtime_snapshot);
    let mut preferred = Vec::new();
    if surface.webrtc_direct {
        preferred.push(burn_p2p_core::BrowserSeedTransportKind::WebRtcDirect);
    }
    if surface.webtransport_gateway {
        preferred.push(burn_p2p_core::BrowserSeedTransportKind::WebTransport);
    }
    if surface.wss_fallback {
        preferred.push(burn_p2p_core::BrowserSeedTransportKind::WssFallback);
    }
    if preferred.is_empty() {
        if surface.webrtc_direct {
            preferred.push(burn_p2p_core::BrowserSeedTransportKind::WebRtcDirect);
        }
        if surface.webtransport_gateway {
            preferred.push(burn_p2p_core::BrowserSeedTransportKind::WebTransport);
        }
        if surface.wss_fallback {
            preferred.push(burn_p2p_core::BrowserSeedTransportKind::WssFallback);
        }
    }
    burn_p2p_core::BrowserSeedTransportPolicy {
        preferred,
        allow_fallback_wss: true,
    }
}

pub(super) fn current_browser_seed_advertisement(
    plan: &BootstrapPlan,
    config: &BootstrapDaemonConfig,
    request: &HttpRequest,
    runtime_snapshot: Option<&burn_p2p::NodeTelemetrySnapshot>,
) -> Option<burn_p2p_core::BrowserSeedAdvertisement> {
    if !browser_join_enabled(config) {
        return None;
    }
    let issued_at = Utc::now();
    let surface = browser_transport_surface(plan, config, runtime_snapshot);
    let multiaddrs = request_public_browser_seed_host(request)
        .map(|host| browser_seed_multiaddrs_for_host(&host, plan, &surface, runtime_snapshot))
        .filter(|multiaddrs| !multiaddrs.is_empty())
        .unwrap_or_else(|| {
            plan.runtime
                .bootstrap_addresses
                .iter()
                .filter(|address| !address.is_memory())
                .map(|address| address.as_str().to_owned())
                .collect::<Vec<_>>()
        });
    let seeds = if multiaddrs.is_empty() {
        Vec::new()
    } else {
        vec![burn_p2p_core::BrowserSeedRecord {
            peer_id: None,
            multiaddrs,
        }]
    };
    Some(burn_p2p_core::BrowserSeedAdvertisement {
        schema_version: u32::from(burn_p2p_core::SCHEMA_VERSION),
        network_id: plan.network_id().clone(),
        issued_at,
        expires_at: issued_at + chrono::Duration::minutes(15),
        transport_policy: browser_seed_transport_policy(plan, config, runtime_snapshot),
        seeds,
    })
}

fn request_public_browser_seed_host(request: &HttpRequest) -> Option<String> {
    let host = request
        .headers
        .get("x-forwarded-host")
        .or_else(|| request.headers.get("host"))?
        .trim();
    if host.is_empty() {
        return None;
    }
    if let Some(rest) = host.strip_prefix('[') {
        let end = rest.find(']')?;
        return Some(rest[..end].to_owned());
    }
    if let Some((hostname, port)) = host.rsplit_once(':')
        && port.chars().all(|value| value.is_ascii_digit())
    {
        return Some(hostname.to_owned());
    }
    Some(host.to_owned())
}

fn browser_seed_multiaddrs_for_host(
    host: &str,
    _plan: &BootstrapPlan,
    surface: &BrowserTransportSurface,
    runtime_snapshot: Option<&burn_p2p::NodeTelemetrySnapshot>,
) -> Vec<String> {
    let host_prefix = browser_seed_host_prefix(host);
    let mut multiaddrs = Vec::new();
    if surface.webrtc_direct {
        multiaddrs.extend(
            browser_runtime_seed_addresses(runtime_snapshot, "webrtc-direct")
                .filter_map(|address| rewrite_browser_seed_host(address.as_str(), &host_prefix)),
        );
    }
    if surface.webtransport_gateway {
        multiaddrs.extend(
            browser_runtime_seed_addresses(runtime_snapshot, "webtransport")
                .filter_map(|address| rewrite_browser_seed_host(address.as_str(), &host_prefix)),
        );
    }
    if surface.wss_fallback {
        multiaddrs.push(format!("{host_prefix}/tcp/443/wss"));
    }
    dedupe_browser_seed_multiaddrs(multiaddrs)
}

fn dedupe_browser_seed_multiaddrs(multiaddrs: Vec<String>) -> Vec<String> {
    let mut deduped = Vec::new();
    for multiaddr in multiaddrs {
        if !deduped.contains(&multiaddr) {
            deduped.push(multiaddr);
        }
    }
    deduped
}

fn browser_seed_host_prefix(host: &str) -> String {
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => format!("/ip4/{host}"),
        Ok(IpAddr::V6(_)) => format!("/ip6/{host}"),
        Err(_) => format!("/dns4/{host}"),
    }
}

fn browser_runtime_seed_addresses<'a>(
    runtime_snapshot: Option<&'a burn_p2p::NodeTelemetrySnapshot>,
    transport_segment: &str,
) -> impl Iterator<Item = &'a burn_p2p::SwarmAddress> {
    runtime_snapshot
        .into_iter()
        .flat_map(|snapshot| snapshot.listen_addresses.iter())
        .filter(move |address| {
            browser_runtime_seed_address_matches(address.as_str(), transport_segment)
        })
}

fn browser_runtime_seed_address_matches(address: &str, transport_segment: &str) -> bool {
    let segments = address
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let browser_transport = segments
        .windows(3)
        .any(|window| window[0] == "udp" && window[2] == transport_segment);
    if !browser_transport {
        return false;
    }
    if matches!(transport_segment, "webrtc-direct" | "webtransport") {
        return segments.contains(&"certhash");
    }
    true
}

fn rewrite_browser_seed_host(address: &str, host_prefix: &str) -> Option<String> {
    let segments = address
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() < 3 {
        return None;
    }
    match segments[0] {
        "ip4" | "ip6" | "dns4" | "dns6" => {}
        _ => return None,
    }
    Some(format!("{host_prefix}/{}", segments[2..].join("/")))
}

#[allow(dead_code)]
fn browser_seed_udp_port(
    listen_addresses: &[burn_p2p_swarm::SwarmAddress],
    transport_segment: &str,
) -> Option<u16> {
    listen_addresses.iter().find_map(|address| {
        let segments = address
            .as_str()
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        segments
            .windows(3)
            .find(|window| window[0] == "udp" && window[2] == transport_segment)
            .and_then(|window| window[1].parse::<u16>().ok())
    })
}

pub(super) fn browser_login_providers(
    auth_state: Option<&Arc<AuthPortalState>>,
) -> Vec<BrowserLoginProvider> {
    auth_state
        .map(|auth| auth.login_providers.clone())
        .unwrap_or_default()
}

pub(super) fn current_browser_directory_snapshot(
    plan: &BootstrapPlan,
    auth_state: Option<&Arc<AuthPortalState>>,
    request: &HttpRequest,
) -> Result<BrowserDirectorySnapshot, Box<dyn std::error::Error>> {
    let entries = auth_state
        .map(|auth| auth_directory_entries(auth, request))
        .transpose()?
        .unwrap_or_default();
    Ok(BrowserDirectorySnapshot {
        network_id: plan.network_id().clone(),
        generated_at: Utc::now(),
        entries,
    })
}

pub(super) fn current_browser_leaderboard(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> burn_p2p_bootstrap::BrowserLeaderboardSnapshot {
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .leaderboard_snapshot(plan, Utc::now())
}
