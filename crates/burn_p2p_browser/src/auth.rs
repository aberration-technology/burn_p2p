use std::collections::BTreeSet;

use burn_p2p::{
    CallbackPayload, ContentId, ContributionReceipt, ExperimentId, ExperimentScope, HeadId,
    LoginRequest, LoginStart, NetworkId, NodeCertificate, PeerId, PrincipalId, PrincipalSession,
    ProjectFamilyId, RevisionId,
};
use burn_p2p_bootstrap::{
    BrowserDirectorySnapshot, BrowserLeaderboardSnapshot, BrowserPortalSnapshot,
    BrowserReceiptSubmissionResponse, TrustBundleExport,
};
use burn_p2p_core::{ArtifactLiveEvent, MetricsLiveEvent, SchemaEnvelope, SignedPayload};
use burn_p2p_metrics::{
    MetricsCatchupBundle, MetricsSnapshot, PeerWindowDistributionDetail,
    PeerWindowDistributionSummary,
};
use burn_p2p_publish::{
    ArtifactAliasStatus, ArtifactRunSummary, ArtifactRunView, DownloadTicketRequest,
    DownloadTicketResponse, ExportRequest, HeadArtifactView,
};
use burn_p2p_swarm::{BrowserEdgeControlPlaneClient, BrowserEdgeControlPlanePaths};
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    BrowserMetricsSyncState, BrowserTransportStatus, BrowserUiBindings, BrowserWorkerCommand,
    BrowserWorkerEvent, BrowserWorkerRuntime,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures browser enrollment.
pub struct BrowserEnrollmentConfig {
    /// The network ID.
    pub network_id: NetworkId,
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The release train hash.
    pub release_train_hash: ContentId,
    /// The target artifact ID.
    pub target_artifact_id: String,
    /// The target artifact hash.
    pub target_artifact_hash: ContentId,
    /// The login path.
    pub login_path: String,
    /// The callback path.
    pub callback_path: String,
    /// The enroll path.
    pub enroll_path: String,
    /// The trust bundle path.
    pub trust_bundle_path: String,
    /// The requested scopes.
    pub requested_scopes: BTreeSet<ExperimentScope>,
    /// The session TTL secs.
    pub session_ttl_secs: i64,
}

impl BrowserEnrollmentConfig {
    /// Creates a value from the portal snapshot.
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
/// Captures browser session state.
pub struct BrowserSessionState {
    /// The session.
    pub session: Option<PrincipalSession>,
    /// The certificate.
    pub certificate: Option<NodeCertificate>,
    /// The trust bundle.
    pub trust_bundle: Option<TrustBundleExport>,
    /// The enrolled at.
    pub enrolled_at: Option<DateTime<Utc>>,
    /// The reenrollment required.
    pub reenrollment_required: bool,
}

impl BrowserSessionState {
    /// Performs the session ID operation.
    pub fn session_id(&self) -> Option<&ContentId> {
        self.session.as_ref().map(|session| &session.session_id)
    }

    /// Performs the principal ID operation.
    pub fn principal_id(&self) -> Option<&PrincipalId> {
        self.session
            .as_ref()
            .map(|session| &session.claims.principal_id)
    }

    /// Performs the peer ID operation.
    pub fn peer_id(&self) -> Option<&PeerId> {
        self.certificate
            .as_ref()
            .map(|certificate| &certificate.claims().peer_id)
    }

    /// Performs the apply enrollment operation.
    pub fn apply_enrollment(&mut self, result: &BrowserEnrollmentResult) {
        self.session = Some(result.session.clone());
        self.certificate = Some(result.certificate.clone());
        self.trust_bundle = result.trust_bundle.clone();
        self.enrolled_at = Some(result.enrolled_at);
        self.reenrollment_required = result.reenrollment_required;
    }

    /// Performs the refresh reenrollment requirement operation.
    pub fn refresh_reenrollment_requirement(&mut self) {
        self.reenrollment_required = self
            .trust_bundle
            .as_ref()
            .map(|bundle| bundle.reenrollment.is_some())
            .unwrap_or(false);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser peer enrollment request.
pub struct BrowserPeerEnrollmentRequest {
    /// The session ID.
    pub session_id: ContentId,
    /// The release train hash.
    pub release_train_hash: ContentId,
    /// The target artifact hash.
    pub target_artifact_hash: ContentId,
    /// The peer ID.
    pub peer_id: PeerId,
    /// The peer public key hex.
    pub peer_public_key_hex: String,
    /// The requested scopes.
    pub requested_scopes: BTreeSet<ExperimentScope>,
    /// The client policy hash.
    pub client_policy_hash: Option<ContentId>,
    /// The serial.
    pub serial: u64,
    /// The TTL seconds.
    pub ttl_seconds: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser worker identity.
pub struct BrowserWorkerIdentity {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The peer public key hex.
    pub peer_public_key_hex: String,
    /// The serial.
    pub serial: u64,
    /// The client policy hash.
    pub client_policy_hash: Option<ContentId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a browser enrollment result.
pub struct BrowserEnrollmentResult {
    /// The login.
    pub login: LoginStart,
    /// The session.
    pub session: PrincipalSession,
    /// The certificate.
    pub certificate: NodeCertificate,
    /// The trust bundle.
    pub trust_bundle: Option<TrustBundleExport>,
    /// The enrolled at.
    pub enrolled_at: DateTime<Utc>,
    /// The reenrollment required.
    pub reenrollment_required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser logout response.
pub struct BrowserLogoutResponse {
    /// The logged out.
    pub logged_out: bool,
}

#[derive(Debug, thiserror::Error)]
/// Enumerates the supported browser auth client error values.
pub enum BrowserAuthClientError {
    #[error("http client error: {0}")]
    /// Uses the HTTP variant.
    Http(#[from] reqwest::Error),
    #[error("json decode error: {0}")]
    /// Uses the JSON variant.
    Json(#[from] serde_json::Error),
    #[error("UTF-8 decode error: {0}")]
    /// Uses the UTF-8 variant.
    Utf8(#[from] std::str::Utf8Error),
    #[error("portal snapshot is missing a trust bundle")]
    /// Uses the missing trust bundle variant.
    MissingTrustBundle,
    #[error("portal snapshot did not expose a login provider")]
    /// Uses the missing login provider variant.
    MissingLoginProvider,
    #[error("browser enrollment config is missing a callback path")]
    /// Uses the missing callback path variant.
    MissingCallbackPath,
    #[error("target artifact {0} is not approved by the browser edge")]
    /// Uses the unapproved target artifact variant.
    UnapprovedTargetArtifact(ContentId),
    #[error("browser edge snapshot mismatch: {0}")]
    /// Uses the edge snapshot mismatch variant.
    EdgeSnapshotMismatch(&'static str),
    #[error("swarm edge sync failed: {0}")]
    /// Uses the swarm variant.
    Swarm(String),
    #[error("metrics live stream closed before producing an event")]
    /// Uses the metrics stream closed variant.
    MetricsStreamClosed,
}

#[derive(Clone, Debug)]
/// Represents a browser portal client.
pub struct BrowserPortalClient {
    http: reqwest::Client,
    bindings: BrowserUiBindings,
    enrollment: BrowserEnrollmentConfig,
}

impl BrowserPortalClient {
    /// Creates a new value.
    pub fn new(bindings: BrowserUiBindings, enrollment: BrowserEnrollmentConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            bindings,
            enrollment,
        }
    }

    /// Performs the bindings operation.
    pub fn bindings(&self) -> &BrowserUiBindings {
        &self.bindings
    }

    /// Performs the enrollment operation.
    pub fn enrollment(&self) -> &BrowserEnrollmentConfig {
        &self.enrollment
    }

    fn edge_control_plane_client(&self) -> BrowserEdgeControlPlaneClient {
        BrowserEdgeControlPlaneClient::new(
            self.bindings.edge_base_url.clone(),
            self.enrollment.network_id.clone(),
        )
        .with_paths(BrowserEdgeControlPlanePaths {
            directory_path: self.bindings.paths.directory_path.clone(),
            heads_path: self.bindings.paths.heads_path.clone(),
            metrics_live_latest_path: self.bindings.paths.metrics_live_latest_path.clone(),
        })
    }

    /// Builds the enrollment request.
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

    /// Fetches the portal snapshot.
    pub async fn fetch_portal_snapshot(
        &self,
    ) -> Result<BrowserPortalSnapshot, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.portal_snapshot_path, None)
            .await
    }

    /// Fetches the directory.
    pub async fn fetch_directory(
        &self,
        session_id: Option<&ContentId>,
    ) -> Result<Vec<burn_p2p::ExperimentDirectoryEntry>, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.directory_path, session_id)
            .await
    }

    /// Fetches the signed directory.
    pub async fn fetch_signed_directory(
        &self,
        session_id: Option<&ContentId>,
    ) -> Result<SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>>, BrowserAuthClientError>
    {
        self.get_json(&self.bindings.paths.signed_directory_path, session_id)
            .await
    }

    /// Fetches the leaderboard.
    pub async fn fetch_leaderboard(
        &self,
    ) -> Result<BrowserLeaderboardSnapshot, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.leaderboard_path, None)
            .await
    }

    /// Fetches the signed leaderboard.
    pub async fn fetch_signed_leaderboard(
        &self,
    ) -> Result<SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>>, BrowserAuthClientError>
    {
        self.get_json(&self.bindings.paths.signed_leaderboard_path, None)
            .await
    }

    /// Fetches grouped metrics snapshots across visible experiment revisions.
    pub async fn fetch_metrics_snapshots(
        &self,
    ) -> Result<Vec<MetricsSnapshot>, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.metrics_snapshot_path, None)
            .await
    }

    /// Fetches grouped metrics snapshots for one experiment.
    pub async fn fetch_metrics_snapshots_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<MetricsSnapshot>, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.metrics_experiments_path,
            experiment_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Fetches append-only metrics ledger segments.
    pub async fn fetch_metrics_ledger(
        &self,
    ) -> Result<Vec<burn_p2p::MetricsLedgerSegment>, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.metrics_ledger_path, None)
            .await
    }

    /// Fetches explicit metrics catchup bundles across visible experiment revisions.
    pub async fn fetch_metrics_catchup(
        &self,
    ) -> Result<Vec<MetricsCatchupBundle>, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.metrics_catchup_path, None)
            .await
    }

    /// Fetches explicit metrics catchup bundles for one experiment.
    pub async fn fetch_metrics_catchup_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<MetricsCatchupBundle>, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.metrics_catchup_path,
            experiment_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Fetches candidate-focused reducer cohorts across visible experiment revisions.
    pub async fn fetch_metrics_candidates(
        &self,
    ) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.metrics_candidates_path, None)
            .await
    }

    /// Fetches candidate-focused reducer cohorts for one experiment.
    pub async fn fetch_metrics_candidates_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.metrics_candidates_path,
            experiment_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Fetches reducer cohorts that currently represent disagreement or
    /// inconsistency signals across visible experiment revisions.
    pub async fn fetch_metrics_disagreements(
        &self,
    ) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.metrics_disagreements_path, None)
            .await
    }

    /// Fetches disagreement-focused reducer cohorts for one experiment.
    pub async fn fetch_metrics_disagreements_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.metrics_disagreements_path,
            experiment_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Fetches peer-window distribution summaries across visible experiment revisions.
    pub async fn fetch_metrics_peer_windows(
        &self,
    ) -> Result<Vec<PeerWindowDistributionSummary>, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.metrics_peer_windows_path, None)
            .await
    }

    /// Fetches peer-window distribution summaries for one experiment.
    pub async fn fetch_metrics_peer_windows_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<PeerWindowDistributionSummary>, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.metrics_peer_windows_path,
            experiment_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Fetches one peer-window distribution drilldown for an experiment revision and base head.
    pub async fn fetch_metrics_peer_window_detail(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        base_head_id: &burn_p2p::HeadId,
    ) -> Result<PeerWindowDistributionDetail, BrowserAuthClientError> {
        let path = format!(
            "{}/{}/{}/{}",
            self.bindings.paths.metrics_peer_windows_path,
            experiment_id.as_str(),
            revision_id.as_str(),
            base_head_id.as_str(),
        );
        self.get_json(&path, None).await
    }

    /// Fetches the latest shared live metrics event frame.
    pub async fn fetch_latest_metrics_live_event(
        &self,
    ) -> Result<MetricsLiveEvent, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.metrics_live_latest_path, None)
            .await
    }

    /// Reads the first event from the live metrics SSE stream.
    pub async fn subscribe_once_metrics_live_event(
        &self,
    ) -> Result<MetricsLiveEvent, BrowserAuthClientError> {
        self.subscribe_metrics_live_events(1)
            .await?
            .into_iter()
            .next()
            .ok_or(BrowserAuthClientError::MetricsStreamClosed)
    }

    /// Reads up to `limit` events from the live metrics SSE stream without
    /// waiting for the server to close the connection.
    ///
    /// On wasm builds this falls back to the latest live-event frame route
    /// because `reqwest`'s browser response type does not expose incremental
    /// chunk reads in the same way as the native client.
    pub async fn subscribe_metrics_live_events(
        &self,
        limit: usize,
    ) -> Result<Vec<MetricsLiveEvent>, BrowserAuthClientError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        #[cfg(target_arch = "wasm32")]
        {
            return Ok(vec![self.fetch_latest_metrics_live_event().await?]);
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let response = self
                .http
                .get(
                    self.bindings
                        .endpoint_url(&self.bindings.paths.metrics_live_path),
                )
                .send()
                .await?
                .error_for_status()?;
            let mut response = response;
            let mut buffer = Vec::<u8>::new();
            let mut events = Vec::<MetricsLiveEvent>::new();

            while events.len() < limit {
                let Some(chunk) = response.chunk().await? else {
                    break;
                };
                buffer.extend_from_slice(&chunk);
                while let Some((frame_end, delimiter_len)) = next_sse_frame_boundary(&buffer) {
                    let frame = buffer[..frame_end].to_vec();
                    buffer.drain(..frame_end + delimiter_len);
                    if let Some(payload) = extract_sse_json_payload(&frame) {
                        events.push(serde_json::from_str(&payload)?);
                        if events.len() >= limit {
                            break;
                        }
                    }
                }
            }

            if events.is_empty() {
                return Err(BrowserAuthClientError::MetricsStreamClosed);
            }
            Ok(events)
        }
    }

    /// Subscribes to live metrics events and applies them to the worker
    /// runtime, emitting the resulting worker events.
    pub async fn stream_metrics_live_events_into_worker(
        &self,
        runtime: &mut BrowserWorkerRuntime,
        limit: usize,
    ) -> Result<Vec<BrowserWorkerEvent>, BrowserAuthClientError> {
        let mut worker_events = Vec::new();
        for event in self.subscribe_metrics_live_events(limit).await? {
            worker_events.extend(runtime.apply_command(
                BrowserWorkerCommand::ApplyMetricsLiveEvent(Box::new(event)),
                None,
                None,
            ));
        }
        Ok(worker_events)
    }

    /// Polls live metrics multiple times and applies only newly observed
    /// events to the worker runtime.
    ///
    /// This is intended for longer-lived browser clients that want to keep a
    /// worker synchronized across repeated SSE or latest-frame fetches without
    /// replaying duplicate state transitions.
    pub async fn pump_metrics_live_events_into_worker(
        &self,
        runtime: &mut BrowserWorkerRuntime,
        rounds: usize,
        limit_per_round: usize,
    ) -> Result<Vec<BrowserWorkerEvent>, BrowserAuthClientError> {
        if rounds == 0 || limit_per_round == 0 {
            return Ok(Vec::new());
        }

        let mut worker_events = Vec::new();
        for _ in 0..rounds {
            let events = match self
                .stream_metrics_live_events_into_worker(runtime, limit_per_round)
                .await
            {
                Ok(events) => events,
                Err(BrowserAuthClientError::MetricsStreamClosed) => break,
                Err(error) => return Err(error),
            };
            if events.is_empty() {
                break;
            }
            worker_events.extend(events);
        }
        Ok(worker_events)
    }

    /// Polls the browser-edge control plane for metrics updates and applies
    /// them to the worker runtime without requiring the SSE route.
    ///
    /// The first round seeds catchup bundles for the active or selected
    /// experiment when available. Later rounds only apply newly observed live
    /// events.
    pub async fn pump_edge_metrics_sync_into_worker(
        &self,
        runtime: &mut BrowserWorkerRuntime,
        rounds: usize,
        poll_interval_ms: u64,
    ) -> Result<Vec<BrowserWorkerEvent>, BrowserAuthClientError> {
        if rounds == 0 {
            return Ok(Vec::new());
        }

        let experiment_id = runtime
            .storage
            .active_assignment
            .as_ref()
            .map(|assignment| assignment.experiment_id.clone())
            .or_else(|| {
                runtime
                    .config
                    .as_ref()
                    .and_then(|config| config.selected_experiment.clone())
            });
        let edge_client = self.edge_control_plane_client();
        let catchup_bundles = if let Some(experiment_id) = experiment_id.as_ref() {
            self.fetch_metrics_catchup_for_experiment(experiment_id)
                .await?
        } else {
            self.get_optional_json(&self.bindings.paths.metrics_catchup_path, None)
                .await?
                .unwrap_or_default()
        };
        let live_events = edge_client
            .collect_metrics_events_by_polling(rounds, poll_interval_ms)
            .await
            .map_err(|error| BrowserAuthClientError::Swarm(error.to_string()))?;
        let mut worker_events = Vec::new();

        if live_events.is_empty() {
            return Ok(runtime.apply_metrics_sync_state(BrowserMetricsSyncState {
                catchup_bundles,
                live_event: None,
            }));
        }

        let mut catchup_bundles = Some(catchup_bundles);
        for event in live_events {
            let events = runtime.apply_metrics_sync_state(BrowserMetricsSyncState {
                catchup_bundles: catchup_bundles.take().unwrap_or_default(),
                live_event: Some(event),
            });
            if !events.is_empty() {
                worker_events.extend(events);
            }
        }

        Ok(worker_events)
    }

    /// Fetches head-scoped evaluation reports.
    pub async fn fetch_head_eval_reports(
        &self,
        head_id: &HeadId,
    ) -> Result<Vec<burn_p2p::HeadEvalReport>, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.metrics_heads_path,
            head_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Fetches artifact alias statuses across visible experiments.
    pub async fn fetch_artifact_aliases(
        &self,
    ) -> Result<Vec<ArtifactAliasStatus>, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.artifacts_aliases_path, None)
            .await
    }

    /// Fetches the latest artifact-publication live event frame.
    pub async fn fetch_latest_artifact_live_event(
        &self,
    ) -> Result<ArtifactLiveEvent, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.artifacts_live_latest_path, None)
            .await
    }

    /// Reads the first event from the artifact-publication SSE stream.
    pub async fn subscribe_once_artifact_live_event(
        &self,
    ) -> Result<ArtifactLiveEvent, BrowserAuthClientError> {
        self.subscribe_artifact_live_events(1)
            .await?
            .into_iter()
            .next()
            .ok_or(BrowserAuthClientError::MetricsStreamClosed)
    }

    /// Reads up to `limit` events from the artifact-publication SSE stream.
    pub async fn subscribe_artifact_live_events(
        &self,
        limit: usize,
    ) -> Result<Vec<ArtifactLiveEvent>, BrowserAuthClientError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        #[cfg(target_arch = "wasm32")]
        {
            return Ok(vec![self.fetch_latest_artifact_live_event().await?]);
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let response = self
                .http
                .get(
                    self.bindings
                        .endpoint_url(&self.bindings.paths.artifacts_live_path),
                )
                .send()
                .await?
                .error_for_status()?;
            let mut response = response;
            let mut buffer = Vec::<u8>::new();
            let mut events = Vec::<ArtifactLiveEvent>::new();

            while events.len() < limit {
                let Some(chunk) = response.chunk().await? else {
                    break;
                };
                buffer.extend_from_slice(&chunk);
                while let Some((frame_end, delimiter_len)) = next_sse_frame_boundary(&buffer) {
                    let frame = buffer[..frame_end].to_vec();
                    buffer.drain(..frame_end + delimiter_len);
                    if let Some(payload) = extract_sse_json_payload(&frame) {
                        events.push(serde_json::from_str(&payload)?);
                        if events.len() >= limit {
                            break;
                        }
                    }
                }
            }

            if events.is_empty() {
                return Err(BrowserAuthClientError::MetricsStreamClosed);
            }
            Ok(events)
        }
    }

    /// Fetches artifact alias statuses for one experiment.
    pub async fn fetch_artifact_aliases_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<ArtifactAliasStatus>, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.artifacts_aliases_path,
            experiment_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Fetches run-scoped artifact summaries for one experiment.
    pub async fn fetch_artifact_runs_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<ArtifactRunSummary>, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.artifacts_runs_path,
            experiment_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Fetches one run-scoped artifact publication view.
    pub async fn fetch_artifact_run_view(
        &self,
        experiment_id: &ExperimentId,
        run_id: &burn_p2p_core::RunId,
    ) -> Result<ArtifactRunView, BrowserAuthClientError> {
        let path = format!(
            "{}/{}/{}",
            self.bindings.paths.artifacts_runs_path,
            experiment_id.as_str(),
            run_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Fetches one head-scoped artifact publication view.
    pub async fn fetch_head_artifact_view(
        &self,
        head_id: &HeadId,
    ) -> Result<HeadArtifactView, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.artifacts_heads_path,
            head_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Requests export of one artifact profile.
    pub async fn request_artifact_export(
        &self,
        request: &ExportRequest,
        session_id: Option<&ContentId>,
    ) -> Result<burn_p2p_core::ExportJob, BrowserAuthClientError> {
        self.post_json_optional_session(
            &self.bindings.paths.artifacts_export_path,
            request,
            session_id,
        )
        .await
    }

    /// Fetches the current state of one artifact export job.
    pub async fn fetch_artifact_export_job(
        &self,
        export_job_id: &burn_p2p_core::ExportJobId,
    ) -> Result<burn_p2p_core::ExportJob, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.artifacts_export_jobs_path,
            export_job_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Requests a short-lived artifact download ticket.
    pub async fn request_artifact_download_ticket(
        &self,
        request: &DownloadTicketRequest,
        session_id: Option<&ContentId>,
    ) -> Result<DownloadTicketResponse, BrowserAuthClientError> {
        self.post_json_optional_session(
            &self.bindings.paths.artifacts_download_ticket_path,
            request,
            session_id,
        )
        .await
    }

    /// Builds the download URL for one previously issued ticket.
    pub fn artifact_download_url(&self, ticket_id: &burn_p2p_core::DownloadTicketId) -> String {
        self.bindings.endpoint_url(&format!(
            "{}/{}",
            self.bindings.paths.artifacts_download_path,
            ticket_id.as_str()
        ))
    }

    /// Submits the receipts.
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

    /// Synchronizes the worker runtime.
    pub async fn sync_worker_runtime(
        &self,
        runtime: &mut BrowserWorkerRuntime,
        session: Option<&BrowserSessionState>,
        include_leaderboard: bool,
    ) -> Result<Vec<BrowserWorkerEvent>, BrowserAuthClientError> {
        let session_id = session.and_then(BrowserSessionState::session_id);
        let portal_snapshot = self.fetch_portal_snapshot().await?;
        let edge_snapshot = self
            .edge_control_plane_client()
            .fetch_snapshot(session_id)
            .await
            .map_err(|error| BrowserAuthClientError::Swarm(error.to_string()))?;
        let signed_directory = self.fetch_signed_directory(session_id).await?;
        let signed_leaderboard = if include_leaderboard {
            Some(self.fetch_signed_leaderboard().await?)
        } else {
            None
        };
        let metrics_catchup = if let Some(experiment_id) = runtime
            .storage
            .active_assignment
            .as_ref()
            .map(|assignment| &assignment.experiment_id)
            .or_else(|| {
                runtime
                    .config
                    .as_ref()
                    .and_then(|config| config.selected_experiment.as_ref())
            }) {
            self.fetch_metrics_catchup_for_experiment(experiment_id)
                .await?
        } else {
            self.get_optional_json(&self.bindings.paths.metrics_catchup_path, None)
                .await?
                .unwrap_or_default()
        };
        let live_metrics_event = edge_snapshot
            .metrics_announcements
            .iter()
            .max_by_key(|announcement| announcement.event.generated_at)
            .map(|announcement| announcement.event.clone())
            .or(self
                .get_optional_json(&self.bindings.paths.metrics_live_latest_path, None)
                .await?);
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
            BrowserMetricsSyncState {
                catchup_bundles: metrics_catchup,
                live_event: live_metrics_event,
            },
            transport,
            session,
        ))
    }

    /// Performs the flush worker receipts operation.
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

    /// Performs the refresh session operation.
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

    /// Performs the logout session operation.
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

    /// Fetches the trust bundle.
    pub async fn fetch_trust_bundle(&self) -> Result<TrustBundleExport, BrowserAuthClientError> {
        self.get_json(&self.enrollment.trust_bundle_path, None)
            .await
    }

    /// Begins the login flow.
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

    /// Completes the static login flow.
    pub async fn complete_static_login(
        &self,
        login: &LoginStart,
        principal_id: PrincipalId,
    ) -> Result<PrincipalSession, BrowserAuthClientError> {
        self.complete_login(login, Some(principal_id), None).await
    }

    /// Completes the provider login flow.
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

    /// Performs the enroll operation.
    pub async fn enroll(
        &self,
        request: &BrowserPeerEnrollmentRequest,
    ) -> Result<NodeCertificate, BrowserAuthClientError> {
        self.post_json(&self.enrollment.enroll_path, request).await
    }

    /// Performs the enroll static principal operation.
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

    async fn get_optional_json<T: DeserializeOwned>(
        &self,
        path: &str,
        session_id: Option<&ContentId>,
    ) -> Result<Option<T>, BrowserAuthClientError> {
        let mut request = self.http.get(self.bindings.endpoint_url(path));
        if let Some(session_id) = session_id {
            request = request.header("x-session-id", session_id.as_str());
        }
        let response = request.send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(response.error_for_status()?.json().await?))
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

    async fn post_json_optional_session<Req, Res>(
        &self,
        path: &str,
        body: &Req,
        session_id: Option<&ContentId>,
    ) -> Result<Res, BrowserAuthClientError>
    where
        Req: Serialize + ?Sized,
        Res: DeserializeOwned,
    {
        let mut request = self.http.post(self.bindings.endpoint_url(path));
        if let Some(session_id) = session_id {
            request = request.header("x-session-id", session_id.as_str());
        }
        Ok(request
            .json(body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

fn next_sse_frame_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| (index, 4))
        .or_else(|| {
            buffer
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|index| (index, 2))
        })
}

fn extract_sse_json_payload(frame: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(frame).replace("\r\n", "\n");
    let payload = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .collect::<Vec<_>>();
    if payload.is_empty() {
        None
    } else {
        Some(payload.join("\n"))
    }
}
