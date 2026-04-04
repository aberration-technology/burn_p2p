use burn_p2p::{RuntimeTransportPolicy, TransportKind};
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
            webrtc_direct_enabled: true,
            webtransport_enabled: true,
            wss_fallback_enabled: true,
            last_error: None,
        }
    }
}

impl BrowserTransportStatus {
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
}
