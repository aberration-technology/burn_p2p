use serde::{Deserialize, Serialize};

fn default_browser_app_refresh_interval_ms() -> u64 {
    15_000
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// Canonical browser-app workspace identifiers.
pub enum BrowserAppSurface {
    /// Viewer-first workspace.
    Viewer,
    /// Validation-focused workspace.
    Validate,
    /// Training-focused workspace.
    Train,
    /// Network and service-health workspace.
    Network,
}

impl BrowserAppSurface {
    /// Returns the lowercase surface key used in DOM bindings and routes.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Validate => "validate",
            Self::Train => "train",
            Self::Network => "network",
        }
    }

    /// Parses a lowercase surface key and falls back to `viewer`.
    pub fn from_key(value: &str) -> Self {
        match value {
            "validate" => Self::Validate,
            "train" => Self::Train,
            "network" => Self::Network,
            _ => Self::Viewer,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One summary metric card shown in the browser app shell.
pub struct BrowserAppSummaryCard {
    /// Human-readable label.
    pub label: String,
    /// Optional live-update key.
    #[serde(default)]
    pub live_key: Option<String>,
    /// Current rendered value.
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One workspace tab shown in the browser app shell.
pub struct BrowserAppSurfaceTab {
    /// Canonical surface id.
    pub surface: BrowserAppSurface,
    /// Human-readable short label.
    pub label: String,
    /// Supporting copy shown under the label.
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One route shortcut in the advanced browser app section.
pub struct BrowserAppRouteLink {
    /// Human-readable route label.
    pub label: String,
    /// Path exposed by the edge.
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One spotlight or focus panel shown at the top of a workspace.
pub struct BrowserAppFocusPanel {
    /// Small uppercase eyebrow label.
    pub eyebrow: String,
    /// Main panel title.
    pub title: String,
    /// Primary spotlight value.
    pub value: String,
    /// Main supporting detail.
    pub detail: String,
    /// Additional operator note.
    pub note: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Typed shell bootstrap payload for the browser app.
pub struct BrowserAppShellView {
    /// Active surface when the page first loads.
    pub active_surface: BrowserAppSurface,
    /// Visible workspace tabs.
    pub surface_tabs: Vec<BrowserAppSurfaceTab>,
    /// Top-level summary cards.
    pub summary_cards: Vec<BrowserAppSummaryCard>,
    /// Advanced route shortcuts.
    pub routes: Vec<BrowserAppRouteLink>,
    /// Poll interval in milliseconds for live refresh.
    pub refresh_interval_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Typed live payload for the browser app's incremental refresh loop.
pub struct BrowserAppLiveView {
    /// Timestamp of the current live snapshot.
    pub captured_at: String,
    /// Human-readable browser operating mode.
    pub browser_mode: String,
    /// Human-readable node state.
    pub node_state: String,
    /// Summary note describing browser-visible network scope.
    pub network_note: String,
    /// Formatted ETA range.
    pub eta: String,
    /// Last error label, or `none`.
    pub last_error: String,
    /// Minimum trust revocation epoch, or `n/a`.
    pub trust_revocation: String,
    /// Number of direct peers connected to the current edge.
    pub connected_peers: usize,
    /// Number of observed peers visible from the current edge.
    pub observed_peers: usize,
    /// Estimated wider network size.
    pub estimated_network_size: usize,
    /// Accepted receipts visible from the edge.
    pub accepted_receipts: usize,
    /// Certified merges visible from the edge.
    pub certified_merges: usize,
    /// Number of heads visible in the current snapshot.
    pub heads_count: usize,
    /// Number of leaderboard entries visible in the current snapshot.
    pub leaderboard_count: usize,
    /// Number of in-flight transfers.
    pub in_flight_transfers: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Static bootstrap configuration for a CDN-served browser app shell.
pub struct BrowserAppStaticBootstrap {
    /// Human-readable app title.
    pub app_name: String,
    /// Asset base URL for the static bundle.
    pub asset_base_url: String,
    /// JavaScript module entry path, relative or absolute.
    pub module_entry_path: String,
    /// Optional stylesheet path, relative or absolute.
    #[serde(default)]
    pub stylesheet_path: Option<String>,
    /// Optional default edge URL shown to the app before the user selects one.
    #[serde(default)]
    pub default_edge_url: Option<String>,
    /// Default surface when the app first loads.
    pub default_surface: BrowserAppSurface,
    /// Refresh interval in milliseconds for the wasm client loop.
    #[serde(default = "default_browser_app_refresh_interval_ms")]
    pub refresh_interval_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One experiment preview card shown in the browser app.
pub struct BrowserAppExperimentSummary {
    /// Human-readable experiment name.
    pub display_name: String,
    /// Stable experiment identifier.
    pub experiment_id: String,
    /// Stable revision identifier.
    pub revision_id: String,
    /// Workload identifier surfaced by the directory.
    pub workload_id: String,
    /// Latest visible head for the experiment, when known.
    #[serde(default)]
    pub current_head_id: Option<String>,
    /// Whether validation can be enabled for the current experiment.
    #[serde(default)]
    pub validate_available: bool,
    /// Whether training can be enabled for the current experiment.
    #[serde(default)]
    pub train_available: bool,
    /// Short availability summary for the browser peer.
    pub availability: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One leaderboard preview row shown in the browser app.
pub struct BrowserAppLeaderboardPreview {
    /// Participant label.
    pub label: String,
    /// Short score label.
    pub score: String,
    /// Accepted receipt count label.
    pub receipts: usize,
    /// Whether the entry belongs to the local authenticated principal.
    pub is_local: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Viewer-first summary derived entirely on the browser client.
pub struct BrowserAppViewerView {
    /// Number of browser-visible experiments.
    pub visible_experiments: usize,
    /// Number of entries with a visible current head.
    pub visible_heads: usize,
    /// Number of leaderboard entries available to the client.
    pub leaderboard_entries: usize,
    /// Whether the client has a signed directory snapshot.
    pub signed_directory_ready: bool,
    /// Whether the client has a signed leaderboard snapshot.
    pub signed_leaderboard_ready: bool,
    /// Preview cards for browser-visible experiments.
    #[serde(default)]
    pub experiments_preview: Vec<BrowserAppExperimentSummary>,
    /// Preview rows for the visible leaderboard.
    #[serde(default)]
    pub leaderboard_preview: Vec<BrowserAppLeaderboardPreview>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Validation-focused summary derived entirely on the browser client.
pub struct BrowserAppValidationView {
    /// Whether validation is available for the selected experiment.
    pub validate_available: bool,
    /// Whether the current runtime state allows validation work.
    pub can_validate: bool,
    /// The head currently being reviewed or caught up to, when known.
    #[serde(default)]
    pub current_head_id: Option<String>,
    /// Last metrics sync timestamp, when available.
    #[serde(default)]
    pub metrics_sync_at: Option<String>,
    /// Pending receipts waiting to be flushed.
    pub pending_receipts: usize,
    /// Last completed validation outcome, when available.
    #[serde(default)]
    pub validation_status: Option<String>,
    /// Last observed checked-chunk count, when available.
    #[serde(default)]
    pub checked_chunks: Option<usize>,
    /// Last emitted receipt ID, when available.
    #[serde(default)]
    pub emitted_receipt_id: Option<String>,
    /// Short summary of the latest evaluation material, when available.
    #[serde(default)]
    pub evaluation_summary: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Training-focused summary derived entirely on the browser client.
pub struct BrowserAppTrainingView {
    /// Whether training is available for the selected experiment.
    pub train_available: bool,
    /// Whether the current runtime state allows training work.
    pub can_train: bool,
    /// Human-readable active assignment summary, when available.
    #[serde(default)]
    pub active_assignment: Option<String>,
    /// The freshest visible head ID from the current browser runtime state.
    #[serde(default)]
    pub latest_head_id: Option<String>,
    /// Count of cached chunk artifacts.
    pub cached_chunk_artifacts: usize,
    /// Count of cached microshards.
    pub cached_microshards: usize,
    /// Pending receipts waiting to be flushed.
    pub pending_receipts: usize,
    /// The max window duration recommended for this browser, when known.
    #[serde(default)]
    pub max_window_secs: Option<u64>,
    /// Latest training-window duration, when available.
    #[serde(default)]
    pub last_window_secs: Option<u64>,
    /// Latest optimizer-step count observed for the active assignment.
    #[serde(default)]
    pub optimizer_steps: Option<u64>,
    /// Latest accepted sample or token count observed for the active assignment.
    #[serde(default)]
    pub accepted_samples: Option<u64>,
    /// Latest local training loss, when available.
    #[serde(default)]
    pub last_loss: Option<String>,
    /// Latest publish latency for the active assignment, when available.
    #[serde(default)]
    pub publish_latency_ms: Option<u64>,
    /// Human-readable throughput summary, when available.
    #[serde(default)]
    pub throughput_summary: Option<String>,
    /// Last emitted training artifact, when available.
    #[serde(default)]
    pub last_artifact_id: Option<String>,
    /// Last emitted training receipt, when available.
    #[serde(default)]
    pub last_receipt_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Network-focused summary derived entirely on the browser client.
pub struct BrowserAppNetworkView {
    /// Edge base URL currently configured by the client.
    pub edge_base_url: String,
    /// Human-readable active or fallback transport label.
    pub transport: String,
    /// Human-readable node state.
    pub node_state: String,
    /// Direct peers currently connected through the active bootstrap edge.
    pub direct_peers: usize,
    /// Observed peers currently visible from the active bootstrap edge.
    pub observed_peers: usize,
    /// Estimated wider network size surfaced by the active bootstrap edge.
    pub estimated_network_size: usize,
    /// Accepted receipts visible from the current bootstrap edge.
    pub accepted_receipts: usize,
    /// Certified merges visible from the current bootstrap edge.
    pub certified_merges: usize,
    /// In-flight transfers currently surfaced by the current bootstrap edge.
    pub in_flight_transfers: usize,
    /// Human-readable note explaining browser-visible network scope.
    pub network_note: String,
    /// Whether metrics catchup or live state has been observed locally.
    pub metrics_live_ready: bool,
    /// Last directory sync timestamp, when available.
    #[serde(default)]
    pub last_directory_sync_at: Option<String>,
    /// Latest surfaced local error, when available.
    #[serde(default)]
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Static-browser-app view state derived from local browser runtime and worker state.
pub struct BrowserAppClientView {
    /// Human-readable network identifier.
    pub network_id: String,
    /// Recommended default surface based on the current runtime state.
    pub default_surface: BrowserAppSurface,
    /// Human-readable runtime state label.
    pub runtime_label: String,
    /// Human-readable runtime detail.
    pub runtime_detail: String,
    /// Human-readable capability summary.
    pub capability_summary: String,
    /// Human-readable session summary.
    pub session_label: String,
    /// Selected or currently relevant experiment.
    #[serde(default)]
    pub selected_experiment: Option<BrowserAppExperimentSummary>,
    /// Viewer-first summary.
    pub viewer: BrowserAppViewerView,
    /// Validation-focused summary.
    pub validation: BrowserAppValidationView,
    /// Training-focused summary.
    pub training: BrowserAppTrainingView,
    /// Network-focused summary.
    pub network: BrowserAppNetworkView,
}

/// Platform-agnostic alias for the shared app workspace identifiers.
pub type NodeAppSurface = BrowserAppSurface;

/// Platform-agnostic alias for one app shell bootstrap payload.
pub type NodeAppShellView = BrowserAppShellView;

/// Platform-agnostic alias for one static app bootstrap payload.
pub type NodeAppStaticBootstrap = BrowserAppStaticBootstrap;

/// Platform-agnostic alias for one shared client view.
pub type NodeAppClientView = BrowserAppClientView;

/// Platform-agnostic alias for one experiment summary row.
pub type NodeAppExperimentSummary = BrowserAppExperimentSummary;

/// Platform-agnostic alias for one leaderboard preview row.
pub type NodeAppLeaderboardPreview = BrowserAppLeaderboardPreview;

/// Platform-agnostic alias for one viewer summary.
pub type NodeAppViewerView = BrowserAppViewerView;

/// Platform-agnostic alias for one validation summary.
pub type NodeAppValidationView = BrowserAppValidationView;

/// Platform-agnostic alias for one training summary.
pub type NodeAppTrainingView = BrowserAppTrainingView;

/// Platform-agnostic alias for one network summary.
pub type NodeAppNetworkView = BrowserAppNetworkView;
