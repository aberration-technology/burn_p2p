use std::collections::BTreeSet;

use burn_p2p::{
    CallbackPayload, ContentId, ContributionReceipt, ExperimentScope, LoginRequest, LoginStart,
    NetworkId, NodeCertificate, PeerId, PrincipalId, PrincipalSession, ProjectFamilyId,
};
use burn_p2p_bootstrap::{
    BrowserDirectorySnapshot, BrowserLeaderboardSnapshot, BrowserPortalSnapshot,
    BrowserReceiptSubmissionResponse, TrustBundleExport,
};
use burn_p2p_core::{SchemaEnvelope, SignedPayload};
use burn_p2p_swarm::{BrowserEdgeControlPlaneClient, BrowserEdgeControlPlanePaths};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    BrowserTransportStatus, BrowserUiBindings, BrowserWorkerCommand, BrowserWorkerEvent,
    BrowserWorkerRuntime,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserEnrollmentConfig {
    pub network_id: NetworkId,
    pub project_family_id: ProjectFamilyId,
    pub release_train_hash: ContentId,
    pub target_artifact_id: String,
    pub target_artifact_hash: ContentId,
    pub login_path: String,
    pub callback_path: String,
    pub enroll_path: String,
    pub trust_bundle_path: String,
    pub requested_scopes: BTreeSet<ExperimentScope>,
    pub session_ttl_secs: i64,
}

impl BrowserEnrollmentConfig {
    pub fn from_portal_snapshot(
        snapshot: &BrowserPortalSnapshot,
        target_artifact_id: impl Into<String>,
        target_artifact_hash: ContentId,
        requested_scopes: BTreeSet<ExperimentScope>,
        session_ttl_secs: i64,
    ) -> Result<Self, BrowserAuthClientError> {
        let trust_bundle = snapshot
            .trust_bundle
            .as_ref()
            .ok_or(BrowserAuthClientError::MissingTrustBundle)?;
        let provider = snapshot
            .login_providers
            .first()
            .ok_or(BrowserAuthClientError::MissingLoginProvider)?;

        if !snapshot.allowed_target_artifact_hashes.is_empty()
            && !snapshot
                .allowed_target_artifact_hashes
                .contains(&target_artifact_hash)
        {
            return Err(BrowserAuthClientError::UnapprovedTargetArtifact(
                target_artifact_hash,
            ));
        }

        Ok(Self {
            network_id: snapshot.network_id.clone(),
            project_family_id: trust_bundle.project_family_id.clone(),
            release_train_hash: snapshot
                .required_release_train_hash
                .clone()
                .unwrap_or_else(|| trust_bundle.required_release_train_hash.clone()),
            target_artifact_id: target_artifact_id.into(),
            target_artifact_hash,
            login_path: provider.login_path.clone(),
            callback_path: provider.callback_path.clone().unwrap_or_default(),
            enroll_path: snapshot.paths.enroll_path.clone(),
            trust_bundle_path: snapshot.paths.trust_bundle_path.clone(),
            requested_scopes,
            session_ttl_secs,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSessionState {
    pub session: Option<PrincipalSession>,
    pub certificate: Option<NodeCertificate>,
    pub trust_bundle: Option<TrustBundleExport>,
    pub enrolled_at: Option<DateTime<Utc>>,
    pub reenrollment_required: bool,
}

impl BrowserSessionState {
    pub fn session_id(&self) -> Option<&ContentId> {
        self.session.as_ref().map(|session| &session.session_id)
    }

    pub fn principal_id(&self) -> Option<&PrincipalId> {
        self.session
            .as_ref()
            .map(|session| &session.claims.principal_id)
    }

    pub fn peer_id(&self) -> Option<&PeerId> {
        self.certificate
            .as_ref()
            .map(|certificate| &certificate.claims().peer_id)
    }

    pub fn apply_enrollment(&mut self, result: &BrowserEnrollmentResult) {
        self.session = Some(result.session.clone());
        self.certificate = Some(result.certificate.clone());
        self.trust_bundle = result.trust_bundle.clone();
        self.enrolled_at = Some(result.enrolled_at);
        self.reenrollment_required = result.reenrollment_required;
    }

    pub fn refresh_reenrollment_requirement(&mut self) {
        self.reenrollment_required = self
            .trust_bundle
            .as_ref()
            .map(|bundle| bundle.reenrollment.is_some())
            .unwrap_or(false);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserPeerEnrollmentRequest {
    pub session_id: ContentId,
    pub release_train_hash: ContentId,
    pub target_artifact_hash: ContentId,
    pub peer_id: PeerId,
    pub peer_public_key_hex: String,
    pub requested_scopes: BTreeSet<ExperimentScope>,
    pub client_policy_hash: Option<ContentId>,
    pub serial: u64,
    pub ttl_seconds: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserWorkerIdentity {
    pub peer_id: PeerId,
    pub peer_public_key_hex: String,
    pub serial: u64,
    pub client_policy_hash: Option<ContentId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrowserEnrollmentResult {
    pub login: LoginStart,
    pub session: PrincipalSession,
    pub certificate: NodeCertificate,
    pub trust_bundle: Option<TrustBundleExport>,
    pub enrolled_at: DateTime<Utc>,
    pub reenrollment_required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserLogoutResponse {
    pub logged_out: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum BrowserAuthClientError {
    #[error("http client error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("portal snapshot is missing a trust bundle")]
    MissingTrustBundle,
    #[error("portal snapshot did not expose a login provider")]
    MissingLoginProvider,
    #[error("browser enrollment config is missing a callback path")]
    MissingCallbackPath,
    #[error("target artifact {0} is not approved by the browser edge")]
    UnapprovedTargetArtifact(ContentId),
    #[error("browser edge snapshot mismatch: {0}")]
    EdgeSnapshotMismatch(&'static str),
    #[error("swarm edge sync failed: {0}")]
    Swarm(String),
}

#[derive(Clone, Debug)]
pub struct BrowserPortalClient {
    http: reqwest::Client,
    bindings: BrowserUiBindings,
    enrollment: BrowserEnrollmentConfig,
}

impl BrowserPortalClient {
    pub fn new(bindings: BrowserUiBindings, enrollment: BrowserEnrollmentConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            bindings,
            enrollment,
        }
    }

    pub fn bindings(&self) -> &BrowserUiBindings {
        &self.bindings
    }

    pub fn enrollment(&self) -> &BrowserEnrollmentConfig {
        &self.enrollment
    }

    pub fn build_enrollment_request(
        &self,
        session: &PrincipalSession,
        identity: &BrowserWorkerIdentity,
    ) -> BrowserPeerEnrollmentRequest {
        BrowserPeerEnrollmentRequest {
            session_id: session.session_id.clone(),
            release_train_hash: self.enrollment.release_train_hash.clone(),
            target_artifact_hash: self.enrollment.target_artifact_hash.clone(),
            peer_id: identity.peer_id.clone(),
            peer_public_key_hex: identity.peer_public_key_hex.clone(),
            requested_scopes: self.enrollment.requested_scopes.clone(),
            client_policy_hash: identity.client_policy_hash.clone(),
            serial: identity.serial,
            ttl_seconds: self.enrollment.session_ttl_secs,
        }
    }

    pub async fn fetch_portal_snapshot(
        &self,
    ) -> Result<BrowserPortalSnapshot, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.portal_snapshot_path, None)
            .await
    }

    pub async fn fetch_directory(
        &self,
        session_id: Option<&ContentId>,
    ) -> Result<Vec<burn_p2p::ExperimentDirectoryEntry>, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.directory_path, session_id)
            .await
    }

    pub async fn fetch_signed_directory(
        &self,
        session_id: Option<&ContentId>,
    ) -> Result<SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>>, BrowserAuthClientError>
    {
        self.get_json(&self.bindings.paths.signed_directory_path, session_id)
            .await
    }

    pub async fn fetch_leaderboard(
        &self,
    ) -> Result<BrowserLeaderboardSnapshot, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.leaderboard_path, None)
            .await
    }

    pub async fn fetch_signed_leaderboard(
        &self,
    ) -> Result<SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>>, BrowserAuthClientError>
    {
        self.get_json(&self.bindings.paths.signed_leaderboard_path, None)
            .await
    }

    pub async fn submit_receipts(
        &self,
        session_id: &ContentId,
        receipts: &[ContributionReceipt],
    ) -> Result<BrowserReceiptSubmissionResponse, BrowserAuthClientError> {
        self.post_json_with_session(
            &self.bindings.paths.receipt_submit_path,
            receipts,
            session_id,
        )
        .await
    }

    pub async fn sync_worker_runtime(
        &self,
        runtime: &mut BrowserWorkerRuntime,
        session: Option<&BrowserSessionState>,
        include_leaderboard: bool,
    ) -> Result<Vec<BrowserWorkerEvent>, BrowserAuthClientError> {
        let session_id = session.and_then(BrowserSessionState::session_id);
        let portal_snapshot = self.fetch_portal_snapshot().await?;
        let edge_snapshot = BrowserEdgeControlPlaneClient::new(
            self.bindings.edge_base_url.clone(),
            portal_snapshot.network_id.clone(),
        )
        .with_paths(BrowserEdgeControlPlanePaths {
            directory_path: self.bindings.paths.directory_path.clone(),
            heads_path: self.bindings.paths.heads_path.clone(),
        })
        .fetch_snapshot(session_id)
        .await
        .map_err(|error| BrowserAuthClientError::Swarm(error.to_string()))?;
        let signed_directory = self.fetch_signed_directory(session_id).await?;
        let signed_leaderboard = if include_leaderboard {
            Some(self.fetch_signed_leaderboard().await?)
        } else {
            None
        };
        let edge_directory = edge_snapshot
            .directory_announcements
            .last()
            .map(|announcement| announcement.entries.as_slice())
            .unwrap_or(&[]);
        if edge_directory != signed_directory.payload.payload.entries.as_slice() {
            return Err(BrowserAuthClientError::EdgeSnapshotMismatch(
                "directory entries did not match the signed directory snapshot",
            ));
        }
        let heads = edge_snapshot
            .head_announcements
            .iter()
            .map(|announcement| announcement.head.clone())
            .collect::<Vec<_>>();
        let transport = BrowserTransportStatus {
            active: None,
            webrtc_direct_enabled: portal_snapshot.transports.webrtc_direct,
            webtransport_enabled: portal_snapshot.transports.webtransport_gateway,
            wss_fallback_enabled: portal_snapshot.transports.wss_fallback,
            last_error: runtime.transport.last_error.clone(),
        };
        Ok(runtime.apply_edge_sync(
            signed_directory,
            &heads,
            signed_leaderboard,
            transport,
            session,
        ))
    }

    pub async fn flush_worker_receipts(
        &self,
        runtime: &mut BrowserWorkerRuntime,
    ) -> Result<Vec<BrowserWorkerEvent>, BrowserAuthClientError> {
        let mut events =
            runtime.apply_command(BrowserWorkerCommand::FlushReceiptOutbox, None, None);
        let mut acknowledged_ids = Vec::new();

        for event in &events {
            if let BrowserWorkerEvent::ReceiptOutboxReady {
                session_id,
                receipts,
                ..
            } = event
            {
                if receipts.is_empty() {
                    continue;
                }
                let submission = self.submit_receipts(session_id, receipts).await?;
                acknowledged_ids.extend(submission.accepted_receipt_ids);
            }
        }

        if !acknowledged_ids.is_empty() {
            events.extend(runtime.apply_command(
                BrowserWorkerCommand::AcknowledgeSubmittedReceipts {
                    receipt_ids: acknowledged_ids,
                },
                None,
                None,
            ));
        }

        Ok(events)
    }

    pub async fn refresh_session(
        &self,
        session_id: &ContentId,
    ) -> Result<PrincipalSession, BrowserAuthClientError> {
        self.post_json(
            "/refresh",
            &serde_json::json!({
                "session_id": session_id,
            }),
        )
        .await
    }

    pub async fn logout_session(
        &self,
        session_id: &ContentId,
    ) -> Result<BrowserLogoutResponse, BrowserAuthClientError> {
        self.post_json(
            "/logout",
            &serde_json::json!({
                "session_id": session_id,
            }),
        )
        .await
    }

    pub async fn fetch_trust_bundle(&self) -> Result<TrustBundleExport, BrowserAuthClientError> {
        self.get_json(&self.enrollment.trust_bundle_path, None)
            .await
    }

    pub async fn begin_login(
        &self,
        principal_hint: Option<String>,
    ) -> Result<LoginStart, BrowserAuthClientError> {
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

    pub async fn complete_static_login(
        &self,
        login: &LoginStart,
        principal_id: PrincipalId,
    ) -> Result<PrincipalSession, BrowserAuthClientError> {
        self.complete_login(login, Some(principal_id), None).await
    }

    pub async fn complete_provider_login(
        &self,
        login: &LoginStart,
        provider_code: impl Into<String>,
    ) -> Result<PrincipalSession, BrowserAuthClientError> {
        self.complete_login(login, None, Some(provider_code.into()))
            .await
    }

    async fn complete_login(
        &self,
        login: &LoginStart,
        principal_id: Option<PrincipalId>,
        provider_code: Option<String>,
    ) -> Result<PrincipalSession, BrowserAuthClientError> {
        if self.enrollment.callback_path.is_empty() {
            return Err(BrowserAuthClientError::MissingCallbackPath);
        }

        self.post_json(
            &self.enrollment.callback_path,
            &CallbackPayload {
                login_id: login.login_id.clone(),
                state: login.state.clone(),
                principal_id,
                provider_code,
            },
        )
        .await
    }

    pub async fn enroll(
        &self,
        request: &BrowserPeerEnrollmentRequest,
    ) -> Result<NodeCertificate, BrowserAuthClientError> {
        self.post_json(&self.enrollment.enroll_path, request).await
    }

    pub async fn enroll_static_principal(
        &self,
        principal_hint: Option<String>,
        principal_id: PrincipalId,
        identity: &BrowserWorkerIdentity,
    ) -> Result<BrowserEnrollmentResult, BrowserAuthClientError> {
        let login = self.begin_login(principal_hint).await?;
        let session = self.complete_static_login(&login, principal_id).await?;
        let certificate = self
            .enroll(&self.build_enrollment_request(&session, identity))
            .await?;
        let trust_bundle = self.fetch_trust_bundle().await.ok();
        let enrolled_at = Utc::now();
        let reenrollment_required = trust_bundle
            .as_ref()
            .map(|bundle| bundle.reenrollment.is_some())
            .unwrap_or(false);

        Ok(BrowserEnrollmentResult {
            login,
            session,
            certificate,
            trust_bundle,
            enrolled_at,
            reenrollment_required,
        })
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        session_id: Option<&ContentId>,
    ) -> Result<T, BrowserAuthClientError> {
        let mut request = self.http.get(self.bindings.endpoint_url(path));
        if let Some(session_id) = session_id {
            request = request.header("x-session-id", session_id.as_str());
        }
        Ok(request.send().await?.error_for_status()?.json().await?)
    }

    async fn post_json<Req, Res>(
        &self,
        path: &str,
        body: &Req,
    ) -> Result<Res, BrowserAuthClientError>
    where
        Req: Serialize + ?Sized,
        Res: DeserializeOwned,
    {
        Ok(self
            .http
            .post(self.bindings.endpoint_url(path))
            .json(body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn post_json_with_session<Req, Res>(
        &self,
        path: &str,
        body: &Req,
        session_id: &ContentId,
    ) -> Result<Res, BrowserAuthClientError>
    where
        Req: Serialize + ?Sized,
        Res: DeserializeOwned,
    {
        Ok(self
            .http
            .post(self.bindings.endpoint_url(path))
            .header("x-session-id", session_id.as_str())
            .json(body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}
