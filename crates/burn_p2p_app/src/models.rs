use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents the browser-facing transport classes exposed by an edge.
pub struct AppTransportSurface {
    /// Whether direct WebRTC peer communication is available.
    pub webrtc_direct: bool,
    /// Whether the edge exposes a WebTransport gateway.
    pub webtransport_gateway: bool,
    /// Whether the edge exposes a secure WebSocket fallback.
    pub wss_fallback: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one login provider advertised by the edge.
pub struct AppLoginProvider {
    /// Human-readable provider name.
    pub label: String,
    /// Path that begins the login flow.
    pub login_path: String,
    /// Optional callback path used for browser redirects.
    pub callback_path: Option<String>,
    /// Optional device-flow path for non-browser auth flows.
    pub device_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Collects the signed and unsigned app-related paths exposed by the edge.
pub struct AppPaths {
    /// Path for the live app snapshot.
    pub app_snapshot_path: String,
    /// Path for the signed experiment directory snapshot.
    pub signed_directory_path: String,
    /// Path for the signed leaderboard snapshot.
    pub signed_leaderboard_path: String,
    /// Path for experiment-scoped artifact aliases.
    pub artifacts_aliases_path: String,
    /// Path for creating or deduplicating artifact exports.
    pub artifacts_export_path: String,
    /// Path for issuing artifact download tickets.
    pub artifacts_download_ticket_path: String,
    /// Path for the active trust bundle snapshot.
    pub trust_bundle_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one experiment row in the reference app.
pub struct AppExperimentRow {
    /// Human-readable experiment name.
    pub display_name: String,
    /// Stable experiment identifier.
    pub experiment_id: String,
    /// Current revision identifier.
    pub revision_id: String,
    /// Whether the edge currently exposes a certified head for this revision.
    pub has_head: bool,
    /// Estimated training or verification window duration in seconds.
    pub estimated_window_seconds: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents one leaderboard row in the reference app.
pub struct AppLeaderboardRow {
    /// Principal or display label shown in the board.
    pub principal_label: String,
    /// Current v1 leaderboard score.
    pub leaderboard_score_v1: f64,
    /// Number of accepted receipts contributing to the score.
    pub accepted_receipt_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents one currently visible certified head in the portal.
pub struct AppHeadRow {
    /// Experiment identifier.
    pub experiment_id: String,
    /// Revision identifier.
    pub revision_id: String,
    /// Head identifier.
    pub head_id: String,
    /// Global training step of the head.
    pub global_step: u64,
    /// RFC3339 timestamp for head creation.
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Summarizes the live operational posture shown in the portal.
pub struct AppDiagnosticsView {
    /// Number of currently connected peers.
    pub connected_peers: usize,
    /// Number of observed peers surfaced by the edge.
    pub observed_peers: usize,
    /// Estimated wider network size beyond the directly visible peer set.
    pub estimated_network_size: usize,
    /// Number of currently admitted peers.
    pub admitted_peers: usize,
    /// Number of rejected peers visible to the edge.
    pub rejected_peers: usize,
    /// Number of quarantined peers visible to the edge.
    pub quarantined_peers: usize,
    /// Number of banned peers visible to the edge.
    pub banned_peers: usize,
    /// Number of in-flight transfer operations currently visible to the edge.
    pub in_flight_transfers: usize,
    /// Total accepted receipt count currently surfaced by the edge.
    pub accepted_receipts: u64,
    /// Total certified merge count currently surfaced by the edge.
    pub certified_merges: u64,
    /// Lower ETA bound for current remaining work, when available.
    #[serde(default)]
    pub eta_lower_seconds: Option<u64>,
    /// Upper ETA bound for current remaining work, when available.
    #[serde(default)]
    pub eta_upper_seconds: Option<u64>,
    /// Human-readable node runtime state.
    pub node_state: String,
    /// Last surfaced error, when one exists.
    #[serde(default)]
    pub last_error: Option<String>,
    /// Human-readable list of active edge services.
    pub active_services: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Summarizes trust and release posture for the current edge.
pub struct AppTrustView {
    /// Required release-train hash, if one is currently published.
    pub required_release_train_hash: Option<String>,
    /// Number of approved target artifacts in the active release train.
    pub approved_target_artifact_count: usize,
    /// Active issuing peer identifier, when available.
    pub active_issuer_peer_id: Option<String>,
    /// Minimum required revocation epoch, if one is currently enforced.
    pub minimum_revocation_epoch: Option<u64>,
    /// Whether peers must re-enroll before continuing to participate.
    pub reenrollment_required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One browser/runtime state card rendered in the reference app.
pub struct AppRuntimeStateCard {
    /// Short label for the runtime card.
    pub label: String,
    /// Human-readable state name.
    pub state: String,
    /// Optional role or mode label.
    #[serde(default)]
    pub role: Option<String>,
    /// Short detail text shown below the state.
    pub detail: String,
    /// Optional progress percentage when the state has a bounded phase.
    #[serde(default)]
    pub progress_percent: Option<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One service-health row rendered in the reference app.
pub struct AppServiceStatusRow {
    /// Service label.
    pub service: String,
    /// Human-readable service state.
    pub status: String,
    /// Short explanatory detail shown beside the status.
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One peer-status row rendered in the reference app.
pub struct AppPeerStatusRow {
    /// Stable peer label shown to operators.
    pub peer_label: String,
    /// Human-readable role label.
    pub role: String,
    /// Human-readable platform label.
    pub platform: String,
    /// Human-readable peer state.
    pub status: String,
    /// Short lag summary label.
    #[serde(default)]
    pub lag_label: Option<String>,
    /// Optional note shown for the peer.
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One labeled metrics row rendered in the reference app.
pub struct AppMetricRow {
    /// Human-readable label for the metric.
    pub label: String,
    /// Rendered metric value.
    pub value: String,
    /// Scope label attached to the metric.
    pub scope: String,
    /// Trust label attached to the metric.
    pub trust: String,
    /// Head, branch, or network key associated with the metric.
    pub key: String,
    /// Evaluation protocol or derivation label, when applicable.
    pub protocol: Option<String>,
    /// Optional operator hint shown alongside the metric row.
    #[serde(default)]
    pub operator_hint: Option<String>,
    /// Optional detail path for drilldowns from the current app edge.
    #[serde(default)]
    pub detail_path: Option<String>,
    /// RFC3339 freshness timestamp.
    pub freshness: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One metrics panel in the reference app.
pub struct AppMetricsPanel {
    /// Stable panel identifier.
    pub panel_id: String,
    /// Human-readable panel title.
    pub title: String,
    /// Short explanation of what the panel means.
    pub description: String,
    /// Rows rendered within the panel.
    pub rows: Vec<AppMetricRow>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One downloadable or exportable artifact alias row rendered in the portal.
pub struct AppArtifactRow {
    /// Alias label shown to users.
    pub alias_name: String,
    /// Human-readable alias scope.
    pub scope: String,
    /// Artifact profile label.
    pub artifact_profile: String,
    /// Experiment identifier the alias belongs to.
    pub experiment_id: String,
    /// Run identifier the alias belongs to, when available.
    pub run_id: Option<String>,
    /// Head currently resolved by the alias.
    pub head_id: String,
    /// Publication target used for the export.
    pub publication_target_id: String,
    /// Alias identifier used for on-demand export and download requests.
    pub artifact_alias_id: Option<String>,
    /// Current publication status shown in the portal.
    pub status: String,
    /// Last ready-publication timestamp, when available.
    pub last_published_at: Option<String>,
    /// Number of recorded alias resolutions for this artifact row.
    pub history_count: usize,
    /// Previously resolved head for this alias, when one exists.
    pub previous_head_id: Option<String>,
    /// Head detail path for the current alias.
    pub head_view_path: String,
    /// Run history/detail path for the current alias, when available.
    pub run_view_path: Option<String>,
    /// Export endpoint path used by the app action.
    pub export_path: String,
    /// Download-ticket endpoint path used by the app action.
    pub download_ticket_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One run-scoped summary row rendered in portal artifact history views.
pub struct AppArtifactRunSummaryRow {
    /// Experiment identifier covered by the row.
    pub experiment_id: String,
    /// Run identifier covered by the row.
    pub run_id: String,
    /// Latest head currently visible for the run.
    pub latest_head_id: String,
    /// Number of current aliases in the run.
    pub alias_count: usize,
    /// Number of historical alias resolutions recorded for the run.
    pub alias_history_count: usize,
    /// Number of published artifacts recorded for the run.
    pub published_artifact_count: usize,
    /// HTML detail path for the run view.
    pub run_view_path: String,
    /// JSON API path for the underlying run payload.
    pub json_view_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One alias-history row rendered in dedicated artifact run/head pages.
pub struct AppArtifactAliasHistoryRow {
    /// Alias label that changed.
    pub alias_name: String,
    /// Alias scope label.
    pub scope: String,
    /// Artifact profile label.
    pub artifact_profile: String,
    /// Head chosen by the alias at this point in history.
    pub head_id: String,
    /// When the alias resolved to the head.
    pub resolved_at: String,
    /// Human-readable source reason.
    pub source_reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One published-artifact row rendered in dedicated artifact run/head pages.
pub struct AppPublishedArtifactRow {
    /// Head attached to the publication.
    pub head_id: String,
    /// Artifact profile label.
    pub artifact_profile: String,
    /// Publication target label.
    pub publication_target_id: String,
    /// Human-readable publication status.
    pub status: String,
    /// Object key or mirror-relative location.
    pub object_key: String,
    /// Published content length in bytes.
    pub content_length: u64,
    /// Publication creation timestamp.
    pub created_at: String,
    /// Expiration timestamp, when one exists.
    pub expires_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One evaluation-report row rendered in dedicated artifact run/head pages.
pub struct AppHeadEvalSummaryRow {
    /// Head evaluated by the report.
    pub head_id: String,
    /// Evaluation protocol label.
    pub eval_protocol_id: String,
    /// Human-readable report status.
    pub status: String,
    /// Dataset view label.
    pub dataset_view_id: String,
    /// Sample count used by the report.
    pub sample_count: u64,
    /// Compact metric summary string.
    pub metric_summary: String,
    /// Evaluation completion timestamp.
    pub finished_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Dedicated run-scoped artifact history view rendered as HTML by the app.
pub struct AppArtifactRunView {
    /// Experiment covered by the page.
    pub experiment_id: String,
    /// Run covered by the page.
    pub run_id: String,
    /// Latest head currently visible for the run.
    pub latest_head_id: Option<String>,
    /// Current heads in the run.
    pub heads: Vec<AppHeadRow>,
    /// Current aliases in the run.
    pub aliases: Vec<AppArtifactRow>,
    /// Historical alias resolutions for the run.
    pub alias_history: Vec<AppArtifactAliasHistoryRow>,
    /// Evaluation reports attached to run heads.
    pub eval_reports: Vec<AppHeadEvalSummaryRow>,
    /// Published artifacts attached to the run.
    pub publications: Vec<AppPublishedArtifactRow>,
    /// Back link to the main app page.
    pub app_path: String,
    /// JSON API path for the run detail payload.
    pub json_view_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Dedicated head-scoped artifact detail view rendered as HTML by the app.
pub struct AppHeadArtifactView {
    /// Head currently being described.
    pub head: AppHeadRow,
    /// Experiment the head belongs to.
    pub experiment_id: String,
    /// Run derived for the head.
    pub run_id: String,
    /// Current aliases that resolve to the head.
    pub aliases: Vec<AppArtifactRow>,
    /// Historical alias resolutions for the head's run context.
    pub alias_history: Vec<AppArtifactAliasHistoryRow>,
    /// Evaluation reports attached to the head.
    pub eval_reports: Vec<AppHeadEvalSummaryRow>,
    /// Publication records attached to the head.
    pub publications: Vec<AppPublishedArtifactRow>,
    /// Available artifact profiles currently supported for the head.
    pub available_profiles: Vec<String>,
    /// Back link to the main app page.
    pub app_path: String,
    /// HTML run-detail path for the head's run.
    pub run_view_path: String,
    /// JSON API path for the head detail payload.
    pub json_view_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents the full reference app snapshot consumed by the renderer.
pub struct AppSnapshotView {
    /// Network identifier.
    pub network_id: String,
    /// RFC3339 timestamp when the app snapshot was captured.
    pub captured_at: String,
    /// Whether interactive auth is enabled for this edge.
    pub auth_enabled: bool,
    /// Human-readable edge mode label.
    pub edge_mode: String,
    /// Human-readable browser participation mode.
    pub browser_mode: String,
    /// Whether social surfaces are enabled for this edge.
    pub social_enabled: bool,
    /// Whether profile surfaces are enabled for this edge.
    pub profile_enabled: bool,
    /// Login providers currently advertised by the edge.
    pub login_providers: Vec<AppLoginProvider>,
    /// Transport classes currently available to browser peers.
    pub transports: AppTransportSurface,
    /// Snapshot and bundle paths surfaced by the edge.
    pub paths: AppPaths,
    /// Live diagnostic posture for the edge.
    pub diagnostics: AppDiagnosticsView,
    /// Trust and release posture for the edge.
    pub trust: AppTrustView,
    /// Browser/runtime state cards currently visible from the edge.
    #[serde(default)]
    pub runtime_states: Vec<AppRuntimeStateCard>,
    /// Service-health rows currently visible from the edge.
    #[serde(default)]
    pub service_statuses: Vec<AppServiceStatusRow>,
    /// Per-peer status rows currently visible from the edge.
    #[serde(default)]
    pub peer_statuses: Vec<AppPeerStatusRow>,
    /// Browser-visible experiment rows.
    pub experiments: Vec<AppExperimentRow>,
    /// Currently visible certified heads.
    pub heads: Vec<AppHeadRow>,
    /// Public leaderboard entries, when enabled.
    pub leaderboard: Vec<AppLeaderboardRow>,
    /// Downloadable/exportable artifact aliases currently visible from the edge.
    pub artifact_rows: Vec<AppArtifactRow>,
    /// Metrics panels rendered from the optional metrics indexer.
    pub metrics_panels: Vec<AppMetricsPanel>,
}
