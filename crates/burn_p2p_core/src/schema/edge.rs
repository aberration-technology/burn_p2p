use super::*;

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
/// Represents a revocation epoch.
pub struct RevocationEpoch(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported auth providers.
pub enum AuthProvider {
    /// Uses GitHub integration.
    GitHub,
    /// Uses OIDC integration.
    Oidc {
        /// The issuer.
        issuer: String,
    },
    /// Uses OAuth integration.
    OAuth {
        /// The provider.
        provider: String,
    },
    /// Uses an external integration.
    External {
        /// The authority.
        authority: String,
    },
    /// Uses static provisioning.
    Static {
        /// The authority.
        authority: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported edge auth providers.
pub enum EdgeAuthProvider {
    /// Uses static provisioning.
    Static,
    /// Uses GitHub integration.
    GitHub,
    /// Uses OIDC integration.
    Oidc,
    /// Uses OAuth integration.
    OAuth,
    /// Uses an external integration.
    External,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported app modes.
pub enum AppMode {
    /// Disables this capability.
    Disabled,
    /// Exposes read-only access.
    Readonly,
    /// Exposes interactive access.
    Interactive,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported browser modes.
pub enum BrowserMode {
    /// Disables this capability.
    Disabled,
    /// Allows observer behavior.
    Observer,
    /// Allows verifier behavior.
    Verifier,
    /// Allows trainer behavior.
    Trainer,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported social modes.
pub enum SocialMode {
    /// Disables this capability.
    Disabled,
    /// Exposes the private setting.
    Private,
    /// Exposes the public setting.
    Public,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported profile modes.
pub enum ProfileMode {
    /// Disables this capability.
    Disabled,
    /// Exposes the private setting.
    Private,
    /// Exposes the public setting.
    Public,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported admin modes.
pub enum AdminMode {
    /// Disables this capability.
    Disabled,
    /// Runs in token mode.
    Token,
    /// Runs in RBAC mode.
    Rbac,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported metrics modes.
pub enum MetricsMode {
    /// Disables this capability.
    Disabled,
    /// Runs in open metrics mode.
    OpenMetrics,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported edge feature values.
pub enum EdgeFeature {
    /// Uses the admin HTTP variant.
    AdminHttp,
    /// Uses the metrics variant.
    Metrics,
    /// Uses the app variant.
    App,
    /// Uses the browser edge variant.
    BrowserEdge,
    /// Uses the RBAC variant.
    Rbac,
    /// Uses the auth static variant.
    AuthStatic,
    /// Uses the auth git hub variant.
    AuthGitHub,
    /// Uses the auth OIDC variant.
    AuthOidc,
    /// Uses the auth o auth variant.
    AuthOAuth,
    /// Uses the auth external variant.
    AuthExternal,
    /// Uses the social variant.
    Social,
    /// Uses the profiles variant.
    Profiles,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a compiled feature set.
pub struct CompiledFeatureSet {
    /// The features.
    pub features: BTreeSet<EdgeFeature>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a configured service set.
pub struct ConfiguredServiceSet {
    /// The features.
    pub features: BTreeSet<EdgeFeature>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an active service set.
pub struct ActiveServiceSet {
    /// The features.
    pub features: BTreeSet<EdgeFeature>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Signed capability manifest for one bootstrap or edge deployment.
///
/// This is intentionally separate from [`NetworkManifest`]. Network manifests
/// describe trust-domain invariants and compatibility, while edge service
/// manifests advertise optional deployment surfaces such as app mode,
/// browser ingress, auth providers, social/profile visibility, and the active
/// compiled feature set for one edge.
pub struct EdgeServiceManifest {
    /// The edge ID.
    pub edge_id: PeerId,
    /// The network ID.
    pub network_id: NetworkId,
    /// The app mode.
    pub app_mode: AppMode,
    /// The browser mode.
    pub browser_mode: BrowserMode,
    /// The available auth providers.
    pub available_auth_providers: BTreeSet<EdgeAuthProvider>,
    /// The social mode.
    pub social_mode: SocialMode,
    /// The profile mode.
    pub profile_mode: ProfileMode,
    /// The admin mode.
    pub admin_mode: AdminMode,
    /// The metrics mode.
    pub metrics_mode: MetricsMode,
    /// The compiled feature set.
    pub compiled_feature_set: CompiledFeatureSet,
    /// The configured service set.
    pub configured_service_set: ConfiguredServiceSet,
    /// The active feature set.
    pub active_feature_set: ActiveServiceSet,
    /// The generated at.
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a trusted issuer status.
pub struct TrustedIssuerStatus {
    /// The issuer peer ID.
    pub issuer_peer_id: PeerId,
    /// The issuer public key hex.
    pub issuer_public_key_hex: String,
    /// The active for new certificates.
    pub active_for_new_certificates: bool,
    /// The accepted for admission.
    pub accepted_for_admission: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a reenrollment status.
pub struct ReenrollmentStatus {
    /// The reason.
    pub reason: String,
    /// The rotated at.
    pub rotated_at: Option<DateTime<Utc>>,
    /// The retired issuer peer IDs.
    pub retired_issuer_peer_ids: BTreeSet<PeerId>,
    /// The login path.
    pub login_path: String,
    /// The enroll path.
    pub enroll_path: String,
    /// The trust bundle path.
    pub trust_bundle_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a trust bundle export.
pub struct TrustBundleExport {
    /// The network ID.
    pub network_id: NetworkId,
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The required release train hash.
    pub required_release_train_hash: ContentId,
    #[serde(default)]
    /// The allowed target artifact hashes.
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
    /// The minimum revocation epoch.
    pub minimum_revocation_epoch: RevocationEpoch,
    /// The active issuer peer ID.
    pub active_issuer_peer_id: PeerId,
    /// The issuers.
    pub issuers: Vec<TrustedIssuerStatus>,
    /// The reenrollment.
    pub reenrollment: Option<ReenrollmentStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported browser edge modes.
pub enum BrowserEdgeMode {
    /// Runs in minimal mode.
    Minimal,
    /// Runs in peer mode.
    Peer,
    /// Runs in full mode.
    Full,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents browser-edge path bindings.
pub struct BrowserEdgePaths {
    /// The capabilities path.
    pub capabilities_path: String,
    /// The app snapshot path.
    pub app_snapshot_path: String,
    /// The directory path.
    pub directory_path: String,
    /// The heads path.
    pub heads_path: String,
    /// The signed directory path.
    pub signed_directory_path: String,
    /// The leaderboard path.
    pub leaderboard_path: String,
    /// The signed leaderboard path.
    pub signed_leaderboard_path: String,
    /// The receipt submit path.
    pub receipt_submit_path: String,
    /// The login path.
    pub login_path: String,
    /// The callback path.
    pub callback_path: String,
    /// The enroll path.
    pub enroll_path: String,
    /// The event stream path.
    pub event_stream_path: String,
    /// The metrics path.
    pub metrics_path: String,
    /// The metrics snapshot path.
    pub metrics_snapshot_path: String,
    /// The metrics ledger path prefix.
    pub metrics_ledger_path: String,
    /// The live metrics stream path.
    pub metrics_live_path: String,
    /// The latest live metrics frame path.
    pub metrics_live_latest_path: String,
    /// The metrics catchup path prefix.
    pub metrics_catchup_path: String,
    /// The candidate-focused metrics path prefix.
    pub metrics_candidates_path: String,
    /// The disagreement-focused metrics path prefix.
    pub metrics_disagreements_path: String,
    /// The peer-window distribution metrics path prefix.
    pub metrics_peer_windows_path: String,
    /// The head-scoped metrics path prefix.
    pub metrics_heads_path: String,
    /// The experiment-scoped metrics path prefix.
    pub metrics_experiments_path: String,
    /// The artifact alias path prefix.
    pub artifacts_aliases_path: String,
    /// The artifact live event stream path.
    pub artifacts_live_path: String,
    /// The latest artifact live event path.
    pub artifacts_live_latest_path: String,
    /// The run-scoped artifact history path prefix.
    pub artifacts_runs_path: String,
    /// The head-scoped artifact detail path prefix.
    pub artifacts_heads_path: String,
    /// The artifact export path.
    pub artifacts_export_path: String,
    /// The artifact job path prefix.
    pub artifacts_export_jobs_path: String,
    /// The artifact download-ticket path.
    pub artifacts_download_ticket_path: String,
    /// The artifact download path prefix.
    pub artifacts_download_path: String,
    /// The trust bundle path.
    pub trust_bundle_path: String,
    /// The reenrollment path.
    pub reenrollment_path: String,
}

impl Default for BrowserEdgePaths {
    fn default() -> Self {
        Self {
            capabilities_path: "/capabilities".into(),
            app_snapshot_path: "/portal/snapshot".into(),
            directory_path: "/directory".into(),
            heads_path: "/heads".into(),
            signed_directory_path: "/directory/signed".into(),
            leaderboard_path: "/leaderboard".into(),
            signed_leaderboard_path: "/leaderboard/signed".into(),
            receipt_submit_path: "/receipts/browser".into(),
            login_path: "/login/static".into(),
            callback_path: "/callback/static".into(),
            enroll_path: "/enroll".into(),
            event_stream_path: "/events".into(),
            metrics_path: "/metrics".into(),
            metrics_snapshot_path: "/metrics/snapshot".into(),
            metrics_ledger_path: "/metrics/ledger".into(),
            metrics_live_path: "/metrics/live".into(),
            metrics_live_latest_path: "/metrics/live/latest".into(),
            metrics_catchup_path: "/metrics/catchup".into(),
            metrics_candidates_path: "/metrics/candidates".into(),
            metrics_disagreements_path: "/metrics/disagreements".into(),
            metrics_peer_windows_path: "/metrics/peer-windows".into(),
            metrics_heads_path: "/metrics/heads".into(),
            metrics_experiments_path: "/metrics/experiments".into(),
            artifacts_aliases_path: "/artifacts/aliases".into(),
            artifacts_live_path: "/artifacts/live".into(),
            artifacts_live_latest_path: "/artifacts/live/latest".into(),
            artifacts_runs_path: "/artifacts/runs".into(),
            artifacts_heads_path: "/artifacts/heads".into(),
            artifacts_export_path: "/artifacts/export".into(),
            artifacts_export_jobs_path: "/artifacts/export".into(),
            artifacts_download_ticket_path: "/artifacts/download-ticket".into(),
            artifacts_download_path: "/artifacts/download".into(),
            trust_bundle_path: "/trust".into(),
            reenrollment_path: "/reenrollment".into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser transport surface.
pub struct BrowserTransportSurface {
    /// The webrtc direct.
    pub webrtc_direct: bool,
    /// The WebTransport gateway.
    pub webtransport_gateway: bool,
    /// The WSS fallback.
    pub wss_fallback: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser login provider.
pub struct BrowserLoginProvider {
    /// The label.
    pub label: String,
    /// The login path.
    pub login_path: String,
    /// The callback path.
    pub callback_path: Option<String>,
    /// The device path.
    pub device_path: Option<String>,
}

/// Defines the browser leaderboard identity alias.
pub type BrowserLeaderboardIdentity = LeaderboardIdentity;
/// Defines the browser leaderboard entry alias.
pub type BrowserLeaderboardEntry = LeaderboardEntry;
/// Defines the browser leaderboard snapshot alias.
pub type BrowserLeaderboardSnapshot = LeaderboardSnapshot;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Captures a snapshot of browser directory.
pub struct BrowserDirectorySnapshot {
    /// The network ID.
    pub network_id: NetworkId,
    /// The generated at.
    pub generated_at: DateTime<Utc>,
    /// The entries.
    pub entries: Vec<ExperimentDirectoryEntry>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Captures a browser-facing app snapshot.
///
/// This stays intentionally lean enough for browser clients and shared edge
/// consumers. Bootstrap-specific diagnostics stay in deployment-facing view
/// models rather than this transport contract.
pub struct BrowserEdgeSnapshot {
    /// The network ID.
    pub network_id: NetworkId,
    /// The edge mode.
    pub edge_mode: BrowserEdgeMode,
    /// The browser mode.
    pub browser_mode: BrowserMode,
    /// The social mode.
    pub social_mode: SocialMode,
    /// The profile mode.
    pub profile_mode: ProfileMode,
    /// The transports.
    pub transports: BrowserTransportSurface,
    /// The paths.
    pub paths: BrowserEdgePaths,
    /// The auth enabled.
    pub auth_enabled: bool,
    /// The login providers.
    pub login_providers: Vec<BrowserLoginProvider>,
    /// The required release train hash.
    pub required_release_train_hash: Option<ContentId>,
    /// The allowed target artifact hashes.
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
    /// The directory.
    pub directory: BrowserDirectorySnapshot,
    /// The heads.
    pub heads: Vec<HeadDescriptor>,
    /// The leaderboard.
    pub leaderboard: BrowserLeaderboardSnapshot,
    /// The trust bundle.
    pub trust_bundle: Option<TrustBundleExport>,
    /// The captured at.
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser receipt submission response.
pub struct BrowserReceiptSubmissionResponse {
    /// The accepted receipt IDs.
    pub accepted_receipt_ids: Vec<ContributionReceiptId>,
    /// The pending receipt count.
    pub pending_receipt_count: usize,
}
