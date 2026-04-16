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
        webrtc_direct: matches!(edge_mode, BrowserEdgeMode::Peer | BrowserEdgeMode::Full),
        webtransport_gateway: matches!(edge_mode, BrowserEdgeMode::Full),
        wss_fallback: true,
    }
}

fn browser_seed_transport_policy(
    plan: &BootstrapPlan,
    config: &BootstrapDaemonConfig,
) -> burn_p2p_core::BrowserSeedTransportPolicy {
    let surface = browser_transport_surface(plan, config);
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
) -> Option<burn_p2p_core::BrowserSeedAdvertisement> {
    if !browser_join_enabled(config) {
        return None;
    }
    let issued_at = Utc::now();
    let surface = browser_transport_surface(plan, config);
    let multiaddrs = request_public_browser_seed_host(request)
        .map(|host| browser_seed_multiaddrs_for_host(&host, &surface))
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
        transport_policy: browser_seed_transport_policy(plan, config),
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

fn browser_seed_multiaddrs_for_host(host: &str, surface: &BrowserTransportSurface) -> Vec<String> {
    let host_prefix = match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(_)) => format!("/ip4/{host}"),
        Ok(IpAddr::V6(_)) => format!("/ip6/{host}"),
        Err(_) => format!("/dns4/{host}"),
    };
    let mut multiaddrs = Vec::new();
    if surface.webrtc_direct {
        multiaddrs.push(format!("{host_prefix}/udp/4001/webrtc-direct"));
    }
    if surface.webtransport_gateway {
        multiaddrs.push(format!("{host_prefix}/udp/443/webtransport"));
    }
    if surface.wss_fallback {
        multiaddrs.push(format!("{host_prefix}/tcp/443/wss"));
    }
    multiaddrs
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
