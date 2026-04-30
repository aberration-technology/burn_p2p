use serde::{Deserialize, Serialize};

use crate::id::{ArtifactId, HeadId, PeerId};

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
/// Classifies the route used by the most recent active-head artifact sync attempt.
pub enum BrowserArtifactRouteKind {
    /// No active-head artifact sync route has been selected yet.
    #[default]
    Unknown,
    /// Artifact bytes were requested over the browser peer swarm.
    PeerSwarm,
    /// Artifact bytes were requested through a relayed circuit path.
    RelayCircuit,
    /// Artifact bytes were requested through the edge HTTP download-ticket fallback.
    EdgeHttp,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Diagnostic record for the most recent active-head artifact sync attempt.
pub struct BrowserArtifactSyncDiagnostics {
    /// Head whose artifact is being synchronized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_id: Option<HeadId>,
    /// Artifact currently being synchronized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<ArtifactId>,
    /// Provider peers advertised for the active head.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_peer_ids: Vec<PeerId>,
    /// Peer selected for the last request when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_peer_id: Option<PeerId>,
    /// Human-readable route label, such as peer-native-swarm or edge-download-ticket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_label: Option<String>,
    /// Route family used or inferred for the last sync attempt.
    #[serde(default)]
    pub route_kind: BrowserArtifactRouteKind,
    /// Verified bytes available after the latest attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_bytes: Option<u64>,
    /// Duration of the latest attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Latest route or transfer error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
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
    /// Most recent active-head artifact sync diagnostic, when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_sync: Option<BrowserArtifactSyncDiagnostics>,
    /// Last surfaced transport/runtime error, when available.
    #[serde(default)]
    pub last_error: Option<String>,
}
