use super::*;

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
) -> Option<burn_p2p_core::BrowserSeedAdvertisement> {
    if !browser_join_enabled(config) {
        return None;
    }
    let issued_at = Utc::now();
    let multiaddrs = plan
        .runtime
        .bootstrap_addresses
        .iter()
        .filter(|address| !address.is_memory())
        .map(|address| address.as_str().to_owned())
        .collect::<Vec<_>>();
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
