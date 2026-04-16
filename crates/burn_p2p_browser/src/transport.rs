use burn_p2p::{PeerId, RuntimeTransportPolicy, TransportKind};
use burn_p2p_core::{
    BrowserResolvedSeedBootstrap, BrowserSeedAdvertisement, BrowserSeedBootstrapSource,
    BrowserTransportFamily, BrowserTransportObservationSource, NetworkId, SCHEMA_VERSION,
    SchemaEnvelope, SignedPayload,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported browser transport kinds.
pub enum BrowserTransportKind {
    /// Uses the web rtc direct kind.
    WebRtcDirect,
    /// Uses the web transport kind.
    WebTransport,
    /// Uses the WSS fallback kind.
    WssFallback,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures the browser transport policy.
pub struct BrowserTransportPolicy {
    /// The preferred.
    pub preferred: Vec<BrowserTransportKind>,
    /// The observer fallback.
    pub observer_fallback: BrowserTransportKind,
    /// The allow suspend resume.
    pub allow_suspend_resume: bool,
}

impl Default for BrowserTransportPolicy {
    fn default() -> Self {
        Self {
            preferred: vec![
                BrowserTransportKind::WebRtcDirect,
                BrowserTransportKind::WebTransport,
                BrowserTransportKind::WssFallback,
            ],
            observer_fallback: BrowserTransportKind::WssFallback,
            allow_suspend_resume: true,
        }
    }
}

impl From<RuntimeTransportPolicy> for BrowserTransportPolicy {
    fn from(policy: RuntimeTransportPolicy) -> Self {
        let mut preferred = Vec::new();
        for transport in policy.preferred_transports {
            let mapped = match transport {
                TransportKind::WebRtc => Some(BrowserTransportKind::WebRtcDirect),
                TransportKind::WebTransport => Some(BrowserTransportKind::WebTransport),
                TransportKind::WebSocket => Some(BrowserTransportKind::WssFallback),
                _ => None,
            };
            if let Some(mapped) = mapped
                && !preferred.contains(&mapped)
            {
                preferred.push(mapped);
            }
        }
        if preferred.is_empty() {
            return Self::default();
        }
        let observer_fallback = preferred
            .iter()
            .find(|kind| matches!(kind, BrowserTransportKind::WssFallback))
            .cloned()
            .unwrap_or_else(|| {
                preferred
                    .last()
                    .cloned()
                    .unwrap_or(BrowserTransportKind::WssFallback)
            });

        Self {
            preferred,
            observer_fallback,
            allow_suspend_resume: policy.supports_direct_streams,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser transport status.
pub struct BrowserTransportStatus {
    /// The active.
    pub active: Option<BrowserTransportKind>,
    /// The selected transport family for the current edge-mediated runtime.
    #[serde(default)]
    pub selected: Option<BrowserTransportKind>,
    /// The actually connected direct browser transport family, when one exists.
    #[serde(default)]
    pub connected: Option<BrowserTransportKind>,
    /// Real peer ids connected through one direct browser transport, when present.
    #[serde(default)]
    pub connected_peer_ids: Vec<PeerId>,
    /// The webrtc direct enabled.
    pub webrtc_direct_enabled: bool,
    /// The WebTransport enabled.
    pub webtransport_enabled: bool,
    /// The WSS fallback enabled.
    pub wss_fallback_enabled: bool,
    /// The last error.
    pub last_error: Option<String>,
}

impl Default for BrowserTransportStatus {
    fn default() -> Self {
        Self {
            active: None,
            selected: None,
            connected: None,
            connected_peer_ids: Vec::new(),
            webrtc_direct_enabled: true,
            webtransport_enabled: true,
            wss_fallback_enabled: true,
            last_error: None,
        }
    }
}

impl BrowserTransportStatus {
    /// Returns the truthful source of the current transport state.
    pub fn truth_source(&self) -> BrowserTransportObservationSource {
        if self.connected.is_some() {
            BrowserTransportObservationSource::Connected
        } else if self.selected.is_some() {
            BrowserTransportObservationSource::Selected
        } else {
            BrowserTransportObservationSource::Unavailable
        }
    }

    /// Returns the desired direct browser transport family, when selected.
    pub fn desired_family(&self) -> Option<BrowserTransportFamily> {
        self.selected.as_ref().map(browser_transport_family)
    }

    /// Returns the connected direct browser transport family, when present.
    pub fn connected_family(&self) -> Option<BrowserTransportFamily> {
        self.connected.as_ref().map(browser_transport_family)
    }

    /// Performs the supports operation.
    pub fn supports(&self, kind: &BrowserTransportKind) -> bool {
        match kind {
            BrowserTransportKind::WebRtcDirect => self.webrtc_direct_enabled,
            BrowserTransportKind::WebTransport => self.webtransport_enabled,
            BrowserTransportKind::WssFallback => self.wss_fallback_enabled,
        }
    }

    /// Performs the recommended transport operation.
    pub fn recommended_transport(
        &self,
        policy: &BrowserTransportPolicy,
        requires_peer_transport: bool,
    ) -> Option<BrowserTransportKind> {
        if !requires_peer_transport {
            return None;
        }

        policy
            .preferred
            .iter()
            .find(|kind| self.supports(kind))
            .cloned()
            .or_else(|| {
                self.supports(&policy.observer_fallback)
                    .then(|| policy.observer_fallback.clone())
            })
    }

    /// Records the currently selected browser transport family.
    pub fn set_selected_transport(&mut self, selected: Option<BrowserTransportKind>) {
        self.selected = selected.clone();
        if self.connected.is_none() {
            self.active = selected;
        }
    }

    /// Clears any direct browser transport connection details.
    pub fn clear_connected_transport(&mut self) {
        self.connected = None;
        self.connected_peer_ids.clear();
        self.active = self.selected.clone();
    }
}

impl BrowserTransportKind {
    /// Returns a short label for the transport family.
    pub fn label(&self) -> &'static str {
        match self {
            Self::WebRtcDirect => "webrtc-direct",
            Self::WebTransport => "webtransport",
            Self::WssFallback => "wss-fallback",
        }
    }
}

/// Maps the browser-local transport kind to the shared diagnostics family.
pub fn browser_transport_family(kind: &BrowserTransportKind) -> BrowserTransportFamily {
    match kind {
        BrowserTransportKind::WebRtcDirect => BrowserTransportFamily::WebRtcDirect,
        BrowserTransportKind::WebTransport => BrowserTransportFamily::WebTransport,
        BrowserTransportKind::WssFallback => BrowserTransportFamily::WssFallback,
    }
}

/// Maps the shared browser transport family back to the browser-local kind.
pub fn browser_transport_kind(family: &BrowserTransportFamily) -> BrowserTransportKind {
    match family {
        BrowserTransportFamily::WebRtcDirect => BrowserTransportKind::WebRtcDirect,
        BrowserTransportFamily::WebTransport => BrowserTransportKind::WebTransport,
        BrowserTransportFamily::WssFallback => BrowserTransportKind::WssFallback,
    }
}

fn transport_from_advertisement_label(label: &str) -> Option<BrowserTransportKind> {
    match label {
        "webrtc-direct" => Some(BrowserTransportKind::WebRtcDirect),
        "webtransport" => Some(BrowserTransportKind::WebTransport),
        "wss-fallback" => Some(BrowserTransportKind::WssFallback),
        _ => None,
    }
}

fn dedupe_seed_node_urls(urls: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut deduped = Vec::new();
    for url in urls {
        if !url.is_empty() && !deduped.contains(&url) {
            deduped.push(url);
        }
    }
    deduped
}

fn validate_seed_advertisement(
    expected_network_id: &NetworkId,
    signed: &SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>>,
    now: DateTime<Utc>,
) -> Result<Vec<String>, String> {
    let payload = &signed.payload.payload;
    if payload.schema_version != u32::from(SCHEMA_VERSION) {
        return Err(format!(
            "unsupported browser seed advertisement schema version {}",
            payload.schema_version
        ));
    }
    if &payload.network_id != expected_network_id {
        return Err(format!(
            "browser seed advertisement network {} did not match expected {}",
            payload.network_id.as_str(),
            expected_network_id.as_str()
        ));
    }
    if payload.issued_at > now + Duration::minutes(5) {
        return Err("browser seed advertisement was issued too far in the future".into());
    }
    if payload.expires_at <= now {
        return Err("browser seed advertisement expired".into());
    }
    let advertised = dedupe_seed_node_urls(
        payload
            .seeds
            .iter()
            .flat_map(|seed| seed.multiaddrs.iter().cloned()),
    );
    if advertised.is_empty() {
        return Err("browser seed advertisement contained no usable multiaddrs".into());
    }
    if payload
        .transport_policy
        .preferred
        .iter()
        .filter_map(|kind| {
            transport_from_advertisement_label(match kind {
                burn_p2p_core::BrowserSeedTransportKind::WebRtcDirect => "webrtc-direct",
                burn_p2p_core::BrowserSeedTransportKind::WebTransport => "webtransport",
                burn_p2p_core::BrowserSeedTransportKind::WssFallback => "wss-fallback",
            })
        })
        .next()
        .is_none()
        && !payload.transport_policy.allow_fallback_wss
    {
        return Err("browser seed advertisement had no usable transport families".into());
    }
    Ok(advertised)
}

/// Resolves the current browser seed bootstrap material from signed edge state
/// and optional site-config fallback addresses.
pub fn resolve_browser_seed_bootstrap(
    expected_network_id: &NetworkId,
    signed_advertisement: Option<&SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>>>,
    site_seed_node_urls: &[String],
    now: DateTime<Utc>,
) -> BrowserResolvedSeedBootstrap {
    let site_seed_node_urls = dedupe_seed_node_urls(site_seed_node_urls.iter().cloned());
    let Some(signed) = signed_advertisement else {
        return if site_seed_node_urls.is_empty() {
            BrowserResolvedSeedBootstrap::default()
        } else {
            BrowserResolvedSeedBootstrap {
                source: BrowserSeedBootstrapSource::SiteConfigFallback,
                seed_node_urls: site_seed_node_urls,
                advertised_seed_count: 0,
                last_error: None,
            }
        };
    };

    match validate_seed_advertisement(expected_network_id, signed, now) {
        Ok(advertised) => {
            let advertised_seed_count = advertised.len();
            if site_seed_node_urls.is_empty() {
                BrowserResolvedSeedBootstrap {
                    source: BrowserSeedBootstrapSource::EdgeSigned,
                    seed_node_urls: advertised,
                    advertised_seed_count,
                    last_error: None,
                }
            } else {
                let mut merged = advertised;
                for seed in site_seed_node_urls {
                    if !merged.contains(&seed) {
                        merged.push(seed);
                    }
                }
                BrowserResolvedSeedBootstrap {
                    source: BrowserSeedBootstrapSource::Merged,
                    seed_node_urls: merged,
                    advertised_seed_count,
                    last_error: None,
                }
            }
        }
        Err(error) => {
            if site_seed_node_urls.is_empty() {
                BrowserResolvedSeedBootstrap {
                    source: BrowserSeedBootstrapSource::Unavailable,
                    seed_node_urls: Vec::new(),
                    advertised_seed_count: 0,
                    last_error: Some(error),
                }
            } else {
                BrowserResolvedSeedBootstrap {
                    source: BrowserSeedBootstrapSource::SiteConfigFallback,
                    seed_node_urls: site_seed_node_urls,
                    advertised_seed_count: 0,
                    last_error: Some(error),
                }
            }
        }
    }
}
