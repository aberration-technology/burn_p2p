use serde::{Deserialize, Serialize};

use crate::id::PeerId;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Describes where the browser got its current seed list.
pub enum BrowserSeedBootstrapSource {
    /// No usable browser seed bootstrap material was available.
    #[default]
    Unavailable,
    /// Seeds came from the signed edge advertisement only.
    EdgeSigned,
    /// Seeds came from site-config fallback only.
    SiteConfigFallback,
    /// Seeds came from a merge of edge advertisement and site config.
    Merged,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Captures the resolved browser seed bootstrap material.
pub struct BrowserResolvedSeedBootstrap {
    /// The source used for the current seed list.
    pub source: BrowserSeedBootstrapSource,
    /// The effective seed node URLs after merge and validation.
    pub seed_node_urls: Vec<String>,
    /// The number of seeds advertised by the edge before merge.
    pub advertised_seed_count: usize,
    /// Last validation or merge error, when one exists.
    #[serde(default)]
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates browser-capable transport families.
pub enum BrowserTransportFamily {
    /// Uses the web rtc direct family.
    WebRtcDirect,
    /// Uses the web transport family.
    WebTransport,
    /// Uses the WSS fallback family.
    WssFallback,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Declares how truthful the current browser transport state is.
pub enum BrowserTransportObservationSource {
    /// No browser transport has been selected yet.
    #[default]
    Unavailable,
    /// A browser transport was selected, but no direct connection is confirmed.
    Selected,
    /// A browser transport is connected and backed by real peer state.
    Connected,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Declares where the active head artifact came from.
pub enum BrowserArtifactSource {
    /// No artifact source has been established yet.
    #[default]
    Unavailable,
    /// The active head artifact was delivered over peer transport.
    PeerSwarm,
    /// The active head artifact was delivered over edge HTTP fallback.
    EdgeHttp,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// High-level browser swarm lifecycle phases used by diagnostics and UI.
pub enum BrowserSwarmPhase {
    /// Browser auth/bootstrap has not resolved usable seed material yet.
    #[default]
    Bootstrap,
    /// Seed material has been resolved but no transport is planned yet.
    SeedResolved,
    /// A direct transport family is planned but not connected.
    TransportSelected,
    /// A direct browser transport is connected.
    TransportConnected,
    /// Directory state is available for the active browser session.
    DirectorySynced,
    /// A head is available for the active assignment.
    HeadSynced,
    /// The active head artifact is available locally.
    ArtifactReady,
    /// Runtime progress is blocked.
    Blocked,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Structured browser swarm/runtime status derived from truthful browser state.
pub struct BrowserSwarmStatus {
    /// Current browser swarm/runtime phase.
    pub phase: BrowserSwarmPhase,
    /// Effective seed bootstrap material currently in use.
    pub seed_bootstrap: BrowserResolvedSeedBootstrap,
    /// Whether the current transport state is selected-only or truly connected.
    pub transport_source: BrowserTransportObservationSource,
    /// Desired direct browser transport family, when one is selected.
    #[serde(default)]
    pub desired_transport: Option<BrowserTransportFamily>,
    /// Connected direct browser transport family, when one exists.
    #[serde(default)]
    pub connected_transport: Option<BrowserTransportFamily>,
    /// Real connected peer count for the direct browser transport.
    #[serde(default)]
    pub connected_peer_count: usize,
    /// Real connected peer ids for the direct browser transport.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connected_peer_ids: Vec<PeerId>,
    /// Whether directory state is available locally.
    #[serde(default)]
    pub directory_synced: bool,
    /// Whether one active assignment is bound locally.
    #[serde(default)]
    pub assignment_bound: bool,
    /// Whether one head is available locally.
    #[serde(default)]
    pub head_synced: bool,
    /// Where the active head artifact came from.
    pub artifact_source: BrowserArtifactSource,
    /// Last surfaced transport/runtime error, when available.
    #[serde(default)]
    pub last_error: Option<String>,
}
