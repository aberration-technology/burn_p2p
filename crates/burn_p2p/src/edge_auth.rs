use std::collections::BTreeSet;

use burn_p2p_core::{
    BrowserEdgeSnapshot, BrowserLoginProvider, ClientReleaseManifest, NetworkCompatibilityError,
    TrustBundleExport,
};
use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    CallbackPayload, ContentId, ExperimentScope, LoginRequest, LoginStart, NetworkId,
    NodeCertificate, PeerId, PrincipalId, PrincipalSession, ProjectFamilyId,
};

fn default_enrollment_app_semver() -> Version {
    Version::new(0, 0, 0)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures one edge-backed enrollment and auth session flow.
pub struct EdgeEnrollmentConfig {
    /// The network identifier expected by the edge.
    pub network_id: NetworkId,
    /// The project family identifier expected by the edge.
    pub project_family_id: ProjectFamilyId,
    /// The protocol major expected by the edge.
    pub protocol_major: u16,
    /// The client app version presented during enrollment.
    pub app_semver: Version,
    /// The required release train hash.
    pub release_train_hash: ContentId,
    /// Human-readable local target artifact identifier.
    pub target_artifact_id: String,
    /// Target artifact hash approved by the edge.
    pub target_artifact_hash: ContentId,
    /// Login-start path for browser or interactive flows.
    pub login_path: String,
    /// Optional device-flow login-start path for headless flows.
    pub device_path: Option<String>,
    /// Callback completion path.
    pub callback_path: String,
    /// Optional trusted callback header for non-interactive callback completion.
    #[serde(default)]
    pub trusted_callback_header: Option<String>,
    /// Optional trusted callback token for non-interactive callback completion.
    #[serde(default)]
    pub trusted_callback_token: Option<String>,
    /// Enrollment path that issues node certificates.
    pub enroll_path: String,
    /// Trust-bundle fetch path.
    pub trust_bundle_path: String,
    /// Requested scopes for the session and certificate.
    pub requested_scopes: BTreeSet<ExperimentScope>,
    /// Requested session TTL in seconds.
    pub session_ttl_secs: i64,
}

impl EdgeEnrollmentConfig {
    /// Creates a config from the bootstrap edge snapshot and a concrete client
    /// release manifest.
    pub fn from_edge_snapshot_for_release(
        snapshot: &BrowserEdgeSnapshot,
        release_manifest: &ClientReleaseManifest,
        requested_scopes: BTreeSet<ExperimentScope>,
        session_ttl_secs: i64,
    ) -> Result<Self, EdgeAuthClientError> {
        release_manifest.validate_for_edge_snapshot(snapshot)?;
        Self::from_edge_snapshot_with_app_version(
            snapshot,
            release_manifest.target_artifact_id.clone(),
            release_manifest.target_artifact_hash.clone(),
            release_manifest.app_semver.clone(),
            requested_scopes,
            session_ttl_secs,
        )
    }

    /// Creates a config from the bootstrap edge snapshot, selected target
    /// artifact, and actual client app version.
    pub fn from_edge_snapshot_with_app_version(
        snapshot: &BrowserEdgeSnapshot,
        target_artifact_id: impl Into<String>,
        target_artifact_hash: ContentId,
        app_semver: Version,
        requested_scopes: BTreeSet<ExperimentScope>,
        session_ttl_secs: i64,
    ) -> Result<Self, EdgeAuthClientError> {
        let trust_bundle = snapshot
            .trust_bundle
            .as_ref()
            .ok_or(EdgeAuthClientError::MissingTrustBundle)?;
        let provider = snapshot
            .login_providers
            .first()
            .ok_or(EdgeAuthClientError::MissingLoginProvider)?;

        if trust_bundle.network_id != snapshot.network_id {
            return Err(NetworkCompatibilityError::EdgeNetworkMismatch {
                snapshot: snapshot.network_id.clone(),
                trust_bundle: trust_bundle.network_id.clone(),
            }
            .into());
        }
        if trust_bundle.protocol_major != snapshot.protocol_major {
            return Err(NetworkCompatibilityError::EdgeProtocolMajorMismatch {
                snapshot: snapshot.protocol_major,
                trust_bundle: trust_bundle.protocol_major,
            }
            .into());
        }
        if app_semver < snapshot.minimum_client_version {
            return Err(NetworkCompatibilityError::ClientVersionTooOld {
                context: "edge snapshot".into(),
                minimum: snapshot.minimum_client_version.clone(),
                found: app_semver,
            }
            .into());
        }
        if app_semver < trust_bundle.minimum_client_version {
            return Err(NetworkCompatibilityError::ClientVersionTooOld {
                context: "trust bundle".into(),
                minimum: trust_bundle.minimum_client_version.clone(),
                found: app_semver,
            }
            .into());
        }

        if !snapshot.allowed_target_artifact_hashes.is_empty()
            && !snapshot
                .allowed_target_artifact_hashes
                .contains(&target_artifact_hash)
        {
            return Err(EdgeAuthClientError::UnapprovedTargetArtifact(
                target_artifact_hash,
            ));
        }

        Ok(Self {
            network_id: snapshot.network_id.clone(),
            project_family_id: trust_bundle.project_family_id.clone(),
            protocol_major: snapshot.protocol_major,
            app_semver,
            release_train_hash: snapshot
                .required_release_train_hash
                .clone()
                .unwrap_or_else(|| trust_bundle.required_release_train_hash.clone()),
            target_artifact_id: target_artifact_id.into(),
            target_artifact_hash,
            login_path: provider.login_path.clone(),
            device_path: provider.device_path.clone(),
            callback_path: provider.callback_path.clone().unwrap_or_default(),
            trusted_callback_header: None,
            trusted_callback_token: None,
            enroll_path: snapshot.paths.enroll_path.clone(),
            trust_bundle_path: snapshot.paths.trust_bundle_path.clone(),
            requested_scopes,
            session_ttl_secs,
        })
    }

    /// Creates a config from the bootstrap edge snapshot and the selected
    /// target artifact. Prefer `from_edge_snapshot_for_release` for production
    /// enrollment so the request reports the concrete client version.
    pub fn from_edge_snapshot(
        snapshot: &BrowserEdgeSnapshot,
        target_artifact_id: impl Into<String>,
        target_artifact_hash: ContentId,
        requested_scopes: BTreeSet<ExperimentScope>,
        session_ttl_secs: i64,
    ) -> Result<Self, EdgeAuthClientError> {
        Self::from_edge_snapshot_with_app_version(
            snapshot,
            target_artifact_id,
            target_artifact_hash,
            default_enrollment_app_semver(),
            requested_scopes,
            session_ttl_secs,
        )
    }

    /// Returns the first provider advertised by the edge snapshot.
    pub fn selected_provider<'a>(
        &'a self,
        providers: &'a [BrowserLoginProvider],
    ) -> Option<&'a BrowserLoginProvider> {
        providers.iter().find(|provider| {
            provider.login_path == self.login_path
                && provider.callback_path.as_deref().unwrap_or_default() == self.callback_path
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Captures persisted edge-backed session state.
pub struct EdgeSessionState {
    /// The active authenticated session, if any.
    pub session: Option<PrincipalSession>,
    /// The issued node certificate, if any.
    pub certificate: Option<NodeCertificate>,
    /// The latest trust bundle fetched from the edge, if any.
    pub trust_bundle: Option<TrustBundleExport>,
    /// Timestamp when enrollment last completed.
    pub enrolled_at: Option<DateTime<Utc>>,
    /// Whether reenrollment is currently required by the trust bundle.
    pub reenrollment_required: bool,
}

impl EdgeSessionState {
    /// Returns the active session identifier.
    pub fn session_id(&self) -> Option<&ContentId> {
        self.session.as_ref().map(|session| &session.session_id)
    }

    /// Returns the current principal identifier.
    pub fn principal_id(&self) -> Option<&PrincipalId> {
        self.session
            .as_ref()
            .map(|session| &session.claims.principal_id)
    }

    /// Returns the current peer identifier.
    pub fn peer_id(&self) -> Option<&PeerId> {
        self.certificate
            .as_ref()
            .map(|certificate| &certificate.claims().peer_id)
    }

    /// Applies one fresh enrollment result to the persisted session state.
    pub fn apply_enrollment(&mut self, result: &EdgeEnrollmentResult) {
        self.session = Some(result.session.clone());
        self.certificate = Some(result.certificate.clone());
        self.trust_bundle = result.trust_bundle.clone();
        self.enrolled_at = Some(result.enrolled_at);
        self.reenrollment_required = result.reenrollment_required;
    }

    /// Recomputes whether reenrollment is required from the current trust
    /// bundle.
    pub fn refresh_reenrollment_requirement(&mut self) {
        self.reenrollment_required = self
            .trust_bundle
            .as_ref()
            .map(|bundle| bundle.reenrollment.is_some())
            .unwrap_or(false);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Identity material used when exchanging one authenticated session for a node
/// certificate.
pub struct EdgePeerIdentity {
    /// The peer identifier.
    pub peer_id: PeerId,
    /// Hex-encoded peer public key.
    pub peer_public_key_hex: String,
    /// Local certificate serial or monotonic identity serial.
    pub serial: u64,
    /// Optional client policy hash used in certificate admission.
    pub client_policy_hash: Option<ContentId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enrollment request sent to the bootstrap edge.
pub struct EdgePeerEnrollmentRequest {
    /// Authenticated session identifier.
    pub session_id: ContentId,
    /// Client app version presented during enrollment.
    #[serde(default = "default_enrollment_app_semver")]
    pub app_semver: Version,
    /// Release train hash expected by the edge.
    pub release_train_hash: ContentId,
    /// Target artifact hash expected by the edge.
    pub target_artifact_hash: ContentId,
    /// Protocol major expected by the edge.
    #[serde(default)]
    pub protocol_major: u16,
    /// Peer identifier requesting a certificate.
    pub peer_id: PeerId,
    /// Hex-encoded peer public key.
    pub peer_public_key_hex: String,
    /// Requested scopes to bind into the certificate.
    pub requested_scopes: BTreeSet<ExperimentScope>,
    /// Optional client policy hash.
    pub client_policy_hash: Option<ContentId>,
    /// Local identity serial.
    pub serial: u64,
    /// Requested TTL in seconds.
    pub ttl_seconds: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Result of one complete login + enrollment flow.
pub struct EdgeEnrollmentResult {
    /// Login start record returned by the edge.
    pub login: LoginStart,
    /// Authenticated principal session.
    pub session: PrincipalSession,
    /// Issued node certificate.
    pub certificate: NodeCertificate,
    /// Trust bundle fetched after enrollment, when available.
    pub trust_bundle: Option<TrustBundleExport>,
    /// Enrollment completion timestamp.
    pub enrolled_at: DateTime<Utc>,
    /// Whether reenrollment is already required.
    pub reenrollment_required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Logout response returned by the edge.
pub struct EdgeLogoutResponse {
    /// Whether the session was accepted for logout.
    pub logged_out: bool,
}

#[derive(Debug, thiserror::Error)]
/// Error returned by the shared edge auth client.
pub enum EdgeAuthClientError {
    #[error("http client error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("json decode error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("edge snapshot is missing a trust bundle")]
    MissingTrustBundle,
    #[error("edge snapshot did not expose a login provider")]
    MissingLoginProvider,
    #[error("edge enrollment config is missing a callback path")]
    MissingCallbackPath,
    #[error("edge enrollment config is missing a device-flow path")]
    MissingDevicePath,
    #[error("target artifact {0} is not approved by the edge")]
    UnapprovedTargetArtifact(ContentId),
    #[error("network compatibility check failed: {0}")]
    NetworkCompatibility(#[from] NetworkCompatibilityError),
    #[error("edge snapshot mismatch: {0}")]
    EdgeSnapshotMismatch(&'static str),
}

#[derive(Clone, Debug)]
/// Shared auth and certificate client for bootstrap edge endpoints.
///
/// Native peers, desktop surfaces, and browser workers can all use the same
/// edge contract:
/// - begin login
/// - complete login
/// - enroll a peer identity
/// - refresh or revoke the session
///
/// ```no_run
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use burn_p2p::{ContentId, EdgeAuthClient, EdgeEnrollmentConfig};
/// use std::collections::BTreeSet;
///
/// let client = EdgeAuthClient::new(
///     "https://edge.example",
///     EdgeEnrollmentConfig {
///         network_id: burn_p2p::NetworkId::new("demo"),
///         project_family_id: burn_p2p::ProjectFamilyId::new("family"),
///         protocol_major: 0,
///         app_semver: semver::Version::new(0, 1, 0),
///         release_train_hash: ContentId::new("release-train"),
///         target_artifact_id: "native-peer".into(),
///         target_artifact_hash: ContentId::new("artifact"),
///         login_path: "/login/oidc".into(),
///         device_path: Some("/device/oidc".into()),
///         callback_path: "/callback/oidc".into(),
///         trusted_callback_header: None,
///         trusted_callback_token: None,
///         enroll_path: "/enroll".into(),
///         trust_bundle_path: "/trust".into(),
///         requested_scopes: BTreeSet::new(),
///         session_ttl_secs: 3600,
///     },
/// );
///
/// let login = client.begin_device_login(Some("alice".into())).await?;
/// let _authorize_url = login.authorize_url.clone();
/// # Ok(())
/// # }
/// ```
pub struct EdgeAuthClient {
    http: reqwest::Client,
    edge_base_url: String,
    enrollment: EdgeEnrollmentConfig,
}

impl EdgeAuthClient {
    /// Creates a portal client pinned to one bootstrap edge base URL and one
    /// enrollment config.
    pub fn new(edge_base_url: impl Into<String>, enrollment: EdgeEnrollmentConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            edge_base_url: edge_base_url.into().trim_end_matches('/').to_owned(),
            enrollment,
        }
    }

    /// Returns the configured edge base URL.
    pub fn edge_base_url(&self) -> &str {
        &self.edge_base_url
    }

    /// Returns the configured enrollment contract.
    pub fn enrollment(&self) -> &EdgeEnrollmentConfig {
        &self.enrollment
    }

    /// Returns a cloned client that attaches one trusted callback header
    /// during callback completion.
    pub fn with_trusted_callback(
        mut self,
        header: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        self.enrollment.trusted_callback_header = Some(header.into());
        self.enrollment.trusted_callback_token = Some(token.into());
        self
    }

    /// Builds the enrollment request for one authenticated session and one
    /// local peer identity.
    pub fn build_enrollment_request(
        &self,
        session: &PrincipalSession,
        identity: &EdgePeerIdentity,
    ) -> EdgePeerEnrollmentRequest {
        EdgePeerEnrollmentRequest {
            session_id: session.session_id.clone(),
            app_semver: self.enrollment.app_semver.clone(),
            release_train_hash: self.enrollment.release_train_hash.clone(),
            target_artifact_hash: self.enrollment.target_artifact_hash.clone(),
            protocol_major: self.enrollment.protocol_major,
            peer_id: identity.peer_id.clone(),
            peer_public_key_hex: identity.peer_public_key_hex.clone(),
            requested_scopes: self.enrollment.requested_scopes.clone(),
            client_policy_hash: identity.client_policy_hash.clone(),
            serial: identity.serial,
            ttl_seconds: self.enrollment.session_ttl_secs,
        }
    }

    /// Fetches the current trust bundle from the edge.
    pub async fn fetch_trust_bundle(&self) -> Result<TrustBundleExport, EdgeAuthClientError> {
        self.get_json(&self.enrollment.trust_bundle_path).await
    }

    /// Begins the default login flow.
    pub async fn begin_login(
        &self,
        principal_hint: Option<String>,
    ) -> Result<LoginStart, EdgeAuthClientError> {
        self.post_json(
            &self.enrollment.login_path,
            &LoginRequest {
                network_id: self.enrollment.network_id.clone(),
                principal_hint,
                requested_scopes: self.enrollment.requested_scopes.clone(),
            },
        )
        .await
    }

    /// Begins the device flow when the selected provider exposes one.
    pub async fn begin_device_login(
        &self,
        principal_hint: Option<String>,
    ) -> Result<LoginStart, EdgeAuthClientError> {
        let path = self
            .enrollment
            .device_path
            .as_ref()
            .ok_or(EdgeAuthClientError::MissingDevicePath)?;
        self.post_json(
            path,
            &LoginRequest {
                network_id: self.enrollment.network_id.clone(),
                principal_hint,
                requested_scopes: self.enrollment.requested_scopes.clone(),
            },
        )
        .await
    }

    /// Completes the static principal flow.
    pub async fn complete_static_login(
        &self,
        login: &LoginStart,
        principal_id: PrincipalId,
    ) -> Result<PrincipalSession, EdgeAuthClientError> {
        self.complete_login(login, Some(principal_id), None).await
    }

    /// Completes the provider callback flow with a provider code captured by a
    /// browser, desktop shell, or external callback handler.
    pub async fn complete_provider_login(
        &self,
        login: &LoginStart,
        provider_code: impl Into<String>,
    ) -> Result<PrincipalSession, EdgeAuthClientError> {
        self.complete_login(login, None, Some(provider_code.into()))
            .await
    }

    async fn complete_login(
        &self,
        login: &LoginStart,
        principal_id: Option<PrincipalId>,
        provider_code: Option<String>,
    ) -> Result<PrincipalSession, EdgeAuthClientError> {
        if self.enrollment.callback_path.is_empty() {
            return Err(EdgeAuthClientError::MissingCallbackPath);
        }

        let payload = CallbackPayload {
            login_id: login.login_id.clone(),
            state: login.state.clone(),
            principal_id,
            provider_code,
        };

        if let (Some(header), Some(token)) = (
            self.enrollment.trusted_callback_header.as_deref(),
            self.enrollment.trusted_callback_token.as_deref(),
        ) {
            return self
                .post_json_with_header(&self.enrollment.callback_path, &payload, header, token)
                .await;
        }

        self.post_json(&self.enrollment.callback_path, &payload)
            .await
    }

    /// Exchanges an authenticated session for a node certificate.
    pub async fn enroll(
        &self,
        request: &EdgePeerEnrollmentRequest,
    ) -> Result<NodeCertificate, EdgeAuthClientError> {
        self.post_json(&self.enrollment.enroll_path, request).await
    }

    /// Convenience helper for static or test identities.
    pub async fn enroll_static_principal(
        &self,
        principal_hint: Option<String>,
        principal_id: PrincipalId,
        identity: &EdgePeerIdentity,
    ) -> Result<EdgeEnrollmentResult, EdgeAuthClientError> {
        let login = self.begin_login(principal_hint).await?;
        let session = self.complete_static_login(&login, principal_id).await?;
        let certificate = self
            .enroll(&self.build_enrollment_request(&session, identity))
            .await?;
        let trust_bundle = self.fetch_trust_bundle().await.ok();
        let reenrollment_required = trust_bundle
            .as_ref()
            .map(|bundle| bundle.reenrollment.is_some())
            .unwrap_or(false);

        Ok(EdgeEnrollmentResult {
            login,
            session,
            certificate,
            trust_bundle,
            enrolled_at: Utc::now(),
            reenrollment_required,
        })
    }

    /// Refreshes one existing session by ID.
    pub async fn refresh_session(
        &self,
        session_id: &ContentId,
    ) -> Result<PrincipalSession, EdgeAuthClientError> {
        self.post_json(
            "/refresh",
            &serde_json::json!({
                "session_id": session_id,
            }),
        )
        .await
    }

    /// Logs out one existing session by ID.
    pub async fn logout_session(
        &self,
        session_id: &ContentId,
    ) -> Result<EdgeLogoutResponse, EdgeAuthClientError> {
        self.post_json(
            "/logout",
            &serde_json::json!({
                "session_id": session_id,
            }),
        )
        .await
    }

    fn endpoint_url(&self, path: &str) -> String {
        if path.starts_with("http://") || path.starts_with("https://") {
            return path.to_owned();
        }

        if let Ok(url) = reqwest::Url::parse(&self.edge_base_url)
            && path.starts_with('/')
        {
            let base_path = url.path().trim_end_matches('/');
            if !base_path.is_empty() && base_path != "/" && path.starts_with(base_path) {
                let mut origin =
                    format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default());
                if let Some(port) = url.port() {
                    origin.push(':');
                    origin.push_str(&port.to_string());
                }
                return format!("{origin}{path}");
            }
        }

        format!("{}{}", self.edge_base_url, path)
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, EdgeAuthClientError> {
        let response = self
            .http
            .get(self.endpoint_url(path))
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json().await?)
    }

    async fn post_json<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> Result<T, EdgeAuthClientError> {
        let response = self
            .http
            .post(self.endpoint_url(path))
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json().await?)
    }

    async fn post_json_with_header<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl Serialize,
        header: &str,
        value: &str,
    ) -> Result<T, EdgeAuthClientError> {
        let response = self
            .http
            .post(self.endpoint_url(path))
            .header(header, value)
            .json(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_enrollment_config_captures_device_flow_when_available() {
        let snapshot = BrowserEdgeSnapshot {
            network_id: NetworkId::new("demo"),
            protocol_major: 0,
            minimum_client_version: semver::Version::new(0, 0, 0),
            edge_mode: burn_p2p_core::BrowserEdgeMode::Peer,
            browser_mode: crate::BrowserMode::Observer,
            social_mode: crate::SocialMode::Disabled,
            profile_mode: crate::ProfileMode::Disabled,
            transports: burn_p2p_core::BrowserTransportSurface {
                webrtc_direct: false,
                webtransport_gateway: true,
                wss_fallback: true,
            },
            paths: burn_p2p_core::BrowserEdgePaths::default(),
            auth_enabled: true,
            login_providers: vec![BrowserLoginProvider {
                label: "OIDC".into(),
                login_path: "/login/oidc".into(),
                callback_path: Some("/callback/oidc".into()),
                device_path: Some("/device/oidc".into()),
            }],
            required_release_train_hash: Some(ContentId::new("release")),
            allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact")]),
            directory: burn_p2p_core::BrowserDirectorySnapshot {
                network_id: NetworkId::new("demo"),
                generated_at: Utc::now(),
                entries: Vec::new(),
            },
            heads: Vec::new(),
            leaderboard: burn_p2p_core::BrowserLeaderboardSnapshot {
                network_id: NetworkId::new("demo"),
                score_version: "v1".into(),
                entries: Vec::new(),
                captured_at: Utc::now(),
            },
            trust_bundle: Some(TrustBundleExport {
                network_id: NetworkId::new("demo"),
                project_family_id: ProjectFamilyId::new("family"),
                protocol_major: 0,
                minimum_client_version: semver::Version::new(0, 0, 0),
                required_release_train_hash: ContentId::new("release"),
                allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact")]),
                active_issuer_peer_id: PeerId::new("issuer"),
                issuers: Vec::new(),
                minimum_revocation_epoch: crate::RevocationEpoch(0),
                reenrollment: None,
            }),
            captured_at: Utc::now(),
        };

        let config = EdgeEnrollmentConfig::from_edge_snapshot(
            &snapshot,
            "native-peer",
            ContentId::new("artifact"),
            BTreeSet::new(),
            3600,
        )
        .expect("portal enrollment config");

        assert_eq!(config.device_path.as_deref(), Some("/device/oidc"));
        assert_eq!(config.login_path, "/login/oidc");
        assert_eq!(config.callback_path, "/callback/oidc");
        assert_eq!(config.trusted_callback_header, None);
        assert_eq!(config.trusted_callback_token, None);
    }
}
