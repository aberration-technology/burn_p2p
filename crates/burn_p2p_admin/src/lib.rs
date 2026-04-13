//! Transport-safe admin and operator client surface for `burn_p2p` edges.
#![forbid(unsafe_code)]

use burn_p2p::{ExperimentDirectoryEntry, TrustedIssuer};
use burn_p2p_core::{
    BrowserDirectorySnapshot, BrowserEdgeSnapshot, ContentId, ReenrollmentStatus, RevocationEpoch,
    SchemaEnvelope, SignedPayload,
};
use burn_p2p_experiment::{
    ExperimentControlCommand, ExperimentControlEnvelope, ExperimentLifecycleEnvelope,
    ExperimentLifecyclePlan, FleetScheduleEpoch, FleetScheduleEpochEnvelope,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Represents one auth-policy rollout request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AuthPolicyRollout {
    /// Updated minimum revocation epoch, when provided.
    pub minimum_revocation_epoch: Option<RevocationEpoch>,
    /// Replacement directory entries, when provided.
    pub directory_entries: Option<Vec<ExperimentDirectoryEntry>>,
    /// Replacement trusted issuers, when provided.
    pub trusted_issuers: Option<Vec<TrustedIssuer>>,
    /// Re-enrollment configuration, when provided.
    pub reenrollment: Option<ReenrollmentStatus>,
}

/// Downstream-facing admin action subset accepted by `/admin`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AdminAction {
    /// Issues a signed control command.
    Control(ExperimentControlCommand),
    /// Issues a signed lifecycle plan.
    Lifecycle(Box<ExperimentLifecyclePlan>),
    /// Issues a signed schedule epoch.
    Schedule(Box<FleetScheduleEpoch>),
    /// Rolls out directory and trust state through the auth policy endpoint.
    RolloutAuthPolicy(AuthPolicyRollout),
}

/// Downstream-facing admin result subset returned by `/admin`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AdminResult {
    /// Signed control certificate.
    Control(Box<burn_p2p_core::ControlCertificate<ExperimentControlEnvelope>>),
    /// Signed lifecycle certificate.
    Lifecycle(Box<burn_p2p_core::ControlCertificate<ExperimentLifecycleEnvelope>>),
    /// Signed schedule certificate.
    Schedule(Box<burn_p2p_core::ControlCertificate<FleetScheduleEpochEnvelope>>),
    /// Result summary for auth-policy rollouts.
    AuthPolicyRolledOut {
        /// Effective minimum revocation epoch after the rollout.
        minimum_revocation_epoch: Option<RevocationEpoch>,
        /// Number of directory entries now visible on the edge.
        directory_entries: usize,
        /// Number of trusted issuers now retained on the edge.
        trusted_issuers: usize,
        /// Whether the rollout requires client re-enrollment.
        reenrollment_required: bool,
    },
}

/// Configuration for one admin client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminClientConfig {
    base_url: String,
    session_id: Option<ContentId>,
    admin_token: Option<String>,
}

impl AdminClientConfig {
    /// Creates one client configuration from the edge base URL.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            session_id: None,
            admin_token: None,
        }
    }

    /// Returns the configured edge base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns the configured session identifier, when present.
    pub fn session_id(&self) -> Option<&ContentId> {
        self.session_id.as_ref()
    }

    /// Returns the configured admin token, when present.
    pub fn admin_token(&self) -> Option<&str> {
        self.admin_token.as_deref()
    }

    /// Adds an authenticated session ID.
    pub fn with_session_id(mut self, session_id: ContentId) -> Self {
        self.session_id = Some(session_id);
        self
    }

    /// Adds an admin token.
    pub fn with_admin_token(mut self, admin_token: impl Into<String>) -> Self {
        self.admin_token = Some(admin_token.into());
        self
    }
}

/// Errors returned by the admin client.
#[derive(Debug, Error)]
pub enum AdminClientError {
    #[error("failed to encode request body: {0}")]
    Encode(String),
    #[error("failed to send request: {0}")]
    Transport(String),
    #[error("request failed with status {status}: {body}")]
    HttpStatus {
        /// HTTP status code.
        status: u16,
        /// Response body text, when available.
        body: String,
    },
    #[error("failed to decode response body: {0}")]
    Decode(String),
}

/// Public admin/operator client that works on native and wasm targets.
#[derive(Clone)]
pub struct AdminClient {
    config: AdminClientConfig,
    #[cfg(not(target_arch = "wasm32"))]
    http: reqwest::Client,
}

impl std::fmt::Debug for AdminClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminClient")
            .field("config", &self.config)
            .finish()
    }
}

impl AdminClient {
    /// Creates one admin client from a typed configuration.
    pub fn new(config: AdminClientConfig) -> Self {
        Self {
            config,
            #[cfg(not(target_arch = "wasm32"))]
            http: reqwest::Client::new(),
        }
    }

    /// Returns the configuration used by this client.
    pub fn config(&self) -> &AdminClientConfig {
        &self.config
    }

    /// Fetches the browser-edge snapshot from `/state`.
    pub async fn fetch_edge_snapshot(&self) -> Result<BrowserEdgeSnapshot, AdminClientError> {
        self.get_json("/state").await
    }

    /// Fetches the visible experiment directory entries from `/directory`.
    pub async fn fetch_directory_entries(
        &self,
    ) -> Result<Vec<ExperimentDirectoryEntry>, AdminClientError> {
        self.get_json("/directory").await
    }

    /// Fetches the signed experiment directory snapshot from `/directory/signed`.
    pub async fn fetch_signed_directory(
        &self,
    ) -> Result<SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>>, AdminClientError> {
        self.get_json("/directory/signed").await
    }

    /// Posts a typed admin action to `/admin`.
    pub async fn post_action(&self, action: &AdminAction) -> Result<AdminResult, AdminClientError> {
        self.post_json("/admin", action).await
    }

    /// Rolls out a directory replacement through the auth-policy action.
    pub async fn rollout_directory_entries(
        &self,
        directory_entries: Vec<ExperimentDirectoryEntry>,
    ) -> Result<AdminResult, AdminClientError> {
        self.post_action(&AdminAction::RolloutAuthPolicy(AuthPolicyRollout {
            minimum_revocation_epoch: None,
            directory_entries: Some(directory_entries),
            trusted_issuers: None,
            reenrollment: None,
        }))
        .await
    }

    /// Inserts or replaces one directory entry within an in-memory vector.
    pub fn upsert_directory_entry(
        entries: &mut Vec<ExperimentDirectoryEntry>,
        replacement: ExperimentDirectoryEntry,
    ) {
        if let Some(entry) = entries.iter_mut().find(|entry| {
            entry.study_id == replacement.study_id
                && entry.experiment_id == replacement.experiment_id
        }) {
            *entry = replacement;
        } else {
            entries.push(replacement);
        }
    }

    async fn get_json<T>(&self, path: &str) -> Result<T, AdminClientError>
    where
        T: serde::de::DeserializeOwned,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut request = self.http.get(self.join_path(path));
            if let Some(session_id) = self.config.session_id() {
                request = request.header("x-session-id", session_id.as_str());
            }
            if let Some(admin_token) = self.config.admin_token() {
                request = request.header("x-admin-token", admin_token);
            }
            let response = request
                .send()
                .await
                .map_err(|error| AdminClientError::Transport(error.to_string()))?;
            decode_native_response(response).await
        }
        #[cfg(target_arch = "wasm32")]
        {
            let request =
                self.request_with_headers(gloo_net::http::Request::get(&self.join_path(path)));
            let response = request
                .send()
                .await
                .map_err(|error| AdminClientError::Transport(error.to_string()))?;
            decode_wasm_response(response).await
        }
    }

    async fn post_json<B, T>(&self, path: &str, body: &B) -> Result<T, AdminClientError>
    where
        B: Serialize,
        T: serde::de::DeserializeOwned,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut request = self.http.post(self.join_path(path));
            if let Some(session_id) = self.config.session_id() {
                request = request.header("x-session-id", session_id.as_str());
            }
            if let Some(admin_token) = self.config.admin_token() {
                request = request.header("x-admin-token", admin_token);
            }
            let response = request
                .json(body)
                .send()
                .await
                .map_err(|error| AdminClientError::Transport(error.to_string()))?;
            decode_native_response(response).await
        }
        #[cfg(target_arch = "wasm32")]
        {
            let request = self
                .request_with_headers(gloo_net::http::Request::post(&self.join_path(path)))
                .json(body)
                .map_err(|error| AdminClientError::Encode(error.to_string()))?;
            let response = request
                .send()
                .await
                .map_err(|error| AdminClientError::Transport(error.to_string()))?;
            decode_wasm_response(response).await
        }
    }

    fn join_path(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            path.trim_start_matches('/'),
        )
    }

    #[cfg(target_arch = "wasm32")]
    fn request_with_headers(
        &self,
        mut request: gloo_net::http::RequestBuilder,
    ) -> gloo_net::http::RequestBuilder {
        if let Some(session_id) = self.config.session_id() {
            request = request.header("x-session-id", session_id.as_str());
        }
        if let Some(admin_token) = self.config.admin_token() {
            request = request.header("x-admin-token", admin_token);
        }
        request
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn decode_native_response<T>(response: reqwest::Response) -> Result<T, AdminClientError>
where
    T: serde::de::DeserializeOwned,
{
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_else(|_| String::new());
        return Err(AdminClientError::HttpStatus {
            status: status.as_u16(),
            body: body.trim().to_owned(),
        });
    }
    response
        .json::<T>()
        .await
        .map_err(|error| AdminClientError::Decode(error.to_string()))
}

#[cfg(target_arch = "wasm32")]
async fn decode_wasm_response<T>(response: gloo_net::http::Response) -> Result<T, AdminClientError>
where
    T: serde::de::DeserializeOwned,
{
    let status = response.status();
    if !(200..300).contains(&status) {
        let body = response.text().await.unwrap_or_else(|_| String::new());
        return Err(AdminClientError::HttpStatus {
            status,
            body: body.trim().to_owned(),
        });
    }
    response
        .json::<T>()
        .await
        .map_err(|error| AdminClientError::Decode(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use burn_p2p::ExperimentScope;
    use burn_p2p_core::{
        ContentId, DatasetViewId, ExperimentId, ExperimentOptInPolicy,
        ExperimentResourceRequirements, ExperimentVisibility, NetworkId, PeerRoleSet, RevisionId,
        StudyId, WorkloadId,
    };

    use super::{AdminAction, AdminClient, AdminClientConfig, AuthPolicyRollout};

    fn sample_entry(experiment_id: &str, revision_id: &str) -> burn_p2p::ExperimentDirectoryEntry {
        burn_p2p::ExperimentDirectoryEntry {
            network_id: NetworkId::new("network"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new(experiment_id),
            workload_id: WorkloadId::new("workload"),
            display_name: "sample".into(),
            model_schema_hash: ContentId::new("schema"),
            dataset_view_id: DatasetViewId::new("dataset-view"),
            resource_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::new(),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: None,
                estimated_download_bytes: 0,
                estimated_window_seconds: 30,
            },
            visibility: ExperimentVisibility::Public,
            opt_in_policy: ExperimentOptInPolicy::Open,
            current_revision_id: RevisionId::new(revision_id),
            current_head_id: None,
            allowed_roles: PeerRoleSet::default_trainer(),
            allowed_scopes: BTreeSet::from([ExperimentScope::Discover]),
            metadata: Default::default(),
        }
    }

    #[test]
    fn upsert_directory_entry_replaces_existing_experiment() {
        let mut entries = vec![sample_entry("exp-a", "rev-a")];
        AdminClient::upsert_directory_entry(&mut entries, sample_entry("exp-a", "rev-b"));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].current_revision_id.as_str(), "rev-b");
    }

    #[test]
    fn upsert_directory_entry_appends_new_experiment() {
        let mut entries = vec![sample_entry("exp-a", "rev-a")];
        AdminClient::upsert_directory_entry(&mut entries, sample_entry("exp-b", "rev-b"));
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn rollout_auth_policy_serializes_with_bootstrap_variant_name() {
        let payload = serde_json::to_value(AdminAction::RolloutAuthPolicy(AuthPolicyRollout {
            minimum_revocation_epoch: None,
            directory_entries: Some(vec![sample_entry("exp-a", "rev-a")]),
            trusted_issuers: None,
            reenrollment: None,
        }))
        .expect("serialize admin action");
        assert!(payload.get("RolloutAuthPolicy").is_some());
    }

    #[test]
    fn config_can_capture_session_and_token() {
        let config = AdminClientConfig::new("https://edge.example")
            .with_session_id(ContentId::new("session-id"))
            .with_admin_token("token");
        assert_eq!(config.base_url(), "https://edge.example");
        assert_eq!(
            config.session_id().map(ContentId::as_str),
            Some("session-id")
        );
        assert_eq!(config.admin_token(), Some("token"));
    }
}
