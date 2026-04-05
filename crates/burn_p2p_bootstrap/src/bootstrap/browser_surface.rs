use super::*;

pub(super) fn browser_edge_mode(plan: &BootstrapPlan) -> BrowserEdgeMode {
    if plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Relay)
        || plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Kademlia)
    {
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
