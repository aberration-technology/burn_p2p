use std::{collections::BTreeSet, future::Future, pin::Pin, sync::Arc, time::Instant};

#[cfg(target_arch = "wasm32")]
use burn_p2p::ArtifactDescriptor;
#[cfg(any(test, target_arch = "wasm32"))]
use burn_p2p::ControlPlaneSnapshot;
use burn_p2p::{
    ArtifactId, CallbackPayload, ChunkId, ContentId, ContributionReceipt, ExperimentId,
    ExperimentScope, HeadDescriptor, HeadId, LoginRequest, LoginStart, NetworkId, NodeCertificate,
    PeerId, PrincipalId, PrincipalSession, ProjectFamilyId, RevisionId,
};
#[cfg(target_arch = "wasm32")]
use burn_p2p_core::ChunkDescriptor;
use burn_p2p_core::{
    ArtifactLiveEvent, ArtifactProfile, BrowserArtifactRouteKind, BrowserDirectorySnapshot,
    BrowserEdgeSnapshot, BrowserLeaderboardSnapshot, BrowserReceiptSubmissionResponse,
    BrowserSeedAdvertisement, BrowserTransportFamily, ClientReleaseManifest, MetricsLiveEvent,
    NetworkCompatibilityError, PublicationTargetId, PublishedArtifactRecord,
    PublishedArtifactStatus, RunId, SchemaEnvelope, SignedPayload, TrustBundleExport,
};
use burn_p2p_metrics::{
    CanonicalHeadAdoptionCurve, MetricsCatchupBundle, MetricsSnapshot,
    PeerWindowDistributionDetail, PeerWindowDistributionSummary, VisibleHeadPopulationHistogram,
};
use burn_p2p_publish::{
    ArtifactAliasStatus, ArtifactRunSummary, ArtifactRunView, DownloadTicketRequest,
    DownloadTicketResponse, ExportRequest, HeadArtifactView,
};
use burn_p2p_swarm::{BrowserEdgeControlPlaneClient, BrowserEdgeControlPlanePaths};
use chrono::{DateTime, Utc};
#[cfg(target_arch = "wasm32")]
use gloo_timers::future::TimeoutFuture;
use reqwest::StatusCode;
use semver::Version;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    BrowserArtifactReplayCheckpoint, BrowserMetricsSyncState, BrowserTransportKind,
    BrowserTransportStatus, BrowserUiBindings, BrowserWorkerCommand, BrowserWorkerEvent,
    BrowserWorkerRuntime, resolve_browser_seed_bootstrap,
};

fn default_enrollment_app_semver() -> Version {
    Version::new(0, 0, 0)
}

#[derive(Clone, Debug)]
struct ActiveHeadArtifactSyncPlan {
    head: HeadDescriptor,
    run_id: RunId,
    request: BrowserPeerArtifactRequest,
    edge_fallback: Option<ActiveHeadArtifactEdgeFallback>,
}

#[derive(Clone, Debug)]
struct ActiveHeadArtifactEdgeFallback {
    publication: PublishedArtifactRecord,
}

#[cfg(any(test, target_arch = "wasm32"))]
#[derive(Clone, Debug)]
struct DirectPeerArtifactRequestSeed {
    run_id: RunId,
    artifact_profile: ArtifactProfile,
    publication_target_id: PublicationTargetId,
    publication_content_hash: Option<ContentId>,
    publication_content_length: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures browser enrollment.
pub struct BrowserEnrollmentConfig {
    /// The network ID.
    pub network_id: NetworkId,
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The protocol major expected by the edge.
    pub protocol_major: u16,
    /// The client app version presented during enrollment.
    pub app_semver: Version,
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
    /// Creates a value from the app snapshot and a concrete client release
    /// manifest.
    pub fn from_edge_snapshot_for_release(
        snapshot: &BrowserEdgeSnapshot,
        release_manifest: &ClientReleaseManifest,
        requested_scopes: BTreeSet<ExperimentScope>,
        session_ttl_secs: i64,
    ) -> Result<Self, BrowserAuthClientError> {
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

    /// Creates a value from the app snapshot, selected target artifact, and
    /// actual client app version.
    pub fn from_edge_snapshot_with_app_version(
        snapshot: &BrowserEdgeSnapshot,
        target_artifact_id: impl Into<String>,
        target_artifact_hash: ContentId,
        app_semver: Version,
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
            return Err(BrowserAuthClientError::UnapprovedTargetArtifact(
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
            callback_path: provider.callback_path.clone().unwrap_or_default(),
            enroll_path: snapshot.paths.enroll_path.clone(),
            trust_bundle_path: snapshot.paths.trust_bundle_path.clone(),
            requested_scopes,
            session_ttl_secs,
        })
    }

    /// Creates a value from the app snapshot and the selected target artifact.
    /// Prefer `from_edge_snapshot_for_release` for production enrollment so the
    /// request reports the concrete client version.
    pub fn from_edge_snapshot(
        snapshot: &BrowserEdgeSnapshot,
        target_artifact_id: impl Into<String>,
        target_artifact_hash: ContentId,
        requested_scopes: BTreeSet<ExperimentScope>,
        session_ttl_secs: i64,
    ) -> Result<Self, BrowserAuthClientError> {
        Self::from_edge_snapshot_with_app_version(
            snapshot,
            target_artifact_id,
            target_artifact_hash,
            default_enrollment_app_semver(),
            requested_scopes,
            session_ttl_secs,
        )
    }

    /// Creates a minimal config for viewer-first runtime sync without assuming auth enrollment.
    pub fn for_runtime_sync(snapshot: &BrowserEdgeSnapshot) -> Self {
        let target_artifact_hash = snapshot
            .allowed_target_artifact_hashes
            .iter()
            .next()
            .cloned()
            .or_else(|| {
                snapshot
                    .trust_bundle
                    .as_ref()
                    .and_then(|bundle| bundle.allowed_target_artifact_hashes.iter().next().cloned())
            })
            .unwrap_or_else(|| ContentId::new("browser-client-artifact"));

        Self {
            network_id: snapshot.network_id.clone(),
            project_family_id: snapshot
                .trust_bundle
                .as_ref()
                .map(|bundle| bundle.project_family_id.clone())
                .unwrap_or_else(|| ProjectFamilyId::new("browser-family")),
            protocol_major: snapshot.protocol_major,
            app_semver: default_enrollment_app_semver(),
            release_train_hash: snapshot
                .required_release_train_hash
                .clone()
                .or_else(|| {
                    snapshot
                        .trust_bundle
                        .as_ref()
                        .map(|bundle| bundle.required_release_train_hash.clone())
                })
                .unwrap_or_else(|| ContentId::new("browser-client-train")),
            target_artifact_id: "browser-client".into(),
            target_artifact_hash,
            login_path: snapshot
                .login_providers
                .first()
                .map(|provider| provider.login_path.clone())
                .unwrap_or_else(|| snapshot.paths.login_path.clone()),
            callback_path: snapshot
                .login_providers
                .first()
                .and_then(|provider| provider.callback_path.clone())
                .unwrap_or_else(|| snapshot.paths.callback_path.clone()),
            enroll_path: snapshot.paths.enroll_path.clone(),
            trust_bundle_path: snapshot.paths.trust_bundle_path.clone(),
            requested_scopes: BTreeSet::new(),
            session_ttl_secs: 3600,
        }
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
    /// Client app version presented during enrollment.
    #[serde(default = "default_enrollment_app_semver")]
    pub app_semver: Version,
    /// The release train hash.
    pub release_train_hash: ContentId,
    /// The target artifact hash.
    pub target_artifact_hash: ContentId,
    /// The protocol major expected by the edge.
    #[serde(default)]
    pub protocol_major: u16,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes one browser-native peer artifact fetch request.
pub struct BrowserPeerArtifactRequest {
    /// The experiment covered by the artifact.
    pub experiment_id: ExperimentId,
    /// The revision covered by the artifact.
    pub revision_id: RevisionId,
    /// The run covered by the artifact.
    pub run_id: RunId,
    /// The head whose artifact should be fetched.
    pub head_id: HeadId,
    /// The artifact identifier to fetch.
    pub artifact_id: ArtifactId,
    /// The requested published artifact profile.
    pub artifact_profile: ArtifactProfile,
    /// The publication target backing the artifact, when known.
    pub publication_target_id: PublicationTargetId,
    /// Content hash exposed by the selected publication when known.
    pub publication_content_hash: Option<ContentId>,
    /// Content length exposed by the selected publication when known.
    pub publication_content_length: Option<u64>,
    /// Peer providers currently known to advertise the head artifact.
    pub provider_peer_ids: Vec<PeerId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
/// Describes one browser-native peer artifact chunk fetch request.
struct BrowserPeerArtifactChunkRequest {
    /// Request metadata shared with the head artifact transport.
    pub artifact: BrowserPeerArtifactRequest,
    /// The chunk identifier to fetch from the swarm provider set.
    pub chunk_id: ChunkId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
enum BrowserPeerArtifactTransportKind {
    CustomFetcher,
    PeerSwarm,
}

impl BrowserPeerArtifactTransportKind {
    fn label(&self) -> &'static str {
        match self {
            Self::CustomFetcher => "peer-native",
            Self::PeerSwarm => "peer-native-swarm",
        }
    }
}

fn elapsed_millis(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

pub(crate) fn artifact_route_kind_for_error(
    error: &BrowserAuthClientError,
) -> BrowserArtifactRouteKind {
    let message = error.to_string();
    if message.contains("/p2p-circuit") || message.contains("p2p-circuit") {
        BrowserArtifactRouteKind::RelayCircuit
    } else {
        BrowserArtifactRouteKind::PeerSwarm
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BrowserPeerArtifactTransportResult {
    bytes: Vec<u8>,
    kind: BrowserPeerArtifactTransportKind,
}

/// Future returned by browser-native peer artifact fetchers.
pub type BrowserPeerArtifactFetchFuture =
    Pin<Box<dyn Future<Output = Result<Vec<u8>, BrowserAuthClientError>> + 'static>>;

/// Pluggable browser-native artifact fetcher used ahead of edge download routes.
pub trait BrowserPeerArtifactFetcher {
    /// Fetches bytes for one active head artifact request.
    fn fetch(&self, request: BrowserPeerArtifactRequest) -> BrowserPeerArtifactFetchFuture;
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
    #[error("app snapshot is missing a trust bundle")]
    /// Uses the missing trust bundle variant.
    MissingTrustBundle,
    #[error("app snapshot did not expose a login provider")]
    /// Uses the missing login provider variant.
    MissingLoginProvider,
    #[error("browser enrollment config is missing a callback path")]
    /// Uses the missing callback path variant.
    MissingCallbackPath,
    #[error("target artifact {0} is not approved by the browser edge")]
    /// Uses the unapproved target artifact variant.
    UnapprovedTargetArtifact(ContentId),
    #[error("network compatibility check failed: {0}")]
    /// Uses the network compatibility variant.
    NetworkCompatibility(#[from] NetworkCompatibilityError),
    #[error("browser edge snapshot mismatch: {0}")]
    /// Uses the edge snapshot mismatch variant.
    EdgeSnapshotMismatch(&'static str),
    #[error("swarm edge sync failed: {0}")]
    /// Uses the swarm variant.
    Swarm(String),
    #[error("browser artifact transport failed: {0}")]
    /// Uses the artifact transport variant.
    ArtifactTransport(String),
    #[error("metrics live stream closed before producing an event")]
    /// Uses the metrics stream closed variant.
    MetricsStreamClosed,
}

impl BrowserAuthClientError {
    fn is_retryable_receipt_submission(&self) -> bool {
        match self {
            Self::Http(error) => match error.status() {
                Some(StatusCode::REQUEST_TIMEOUT)
                | Some(StatusCode::TOO_MANY_REQUESTS)
                | Some(StatusCode::BAD_GATEWAY)
                | Some(StatusCode::SERVICE_UNAVAILABLE)
                | Some(StatusCode::GATEWAY_TIMEOUT) => true,
                Some(status) => status.is_server_error(),
                None => error.is_timeout() || error.is_request(),
            },
            Self::MetricsStreamClosed => false,
            Self::Json(_)
            | Self::Utf8(_)
            | Self::MissingTrustBundle
            | Self::MissingLoginProvider
            | Self::MissingCallbackPath
            | Self::UnapprovedTargetArtifact(_)
            | Self::NetworkCompatibility(_)
            | Self::EdgeSnapshotMismatch(_)
            | Self::Swarm(_)
            | Self::ArtifactTransport(_) => false,
        }
    }
}

#[derive(Clone)]
/// Represents the browser-side auth client for one browser-edge deployment.
pub struct BrowserEdgeClient {
    http: reqwest::Client,
    bindings: BrowserUiBindings,
    enrollment: BrowserEnrollmentConfig,
    peer_artifact_fetcher: Option<Arc<dyn BrowserPeerArtifactFetcher>>,
    trusted_callback_header: Option<String>,
    trusted_callback_token: Option<String>,
}

impl std::fmt::Debug for BrowserEdgeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrowserEdgeClient")
            .field("bindings", &self.bindings)
            .field("enrollment", &self.enrollment)
            .field(
                "peer_artifact_fetcher",
                &self.peer_artifact_fetcher.as_ref().map(|_| "configured"),
            )
            .finish()
    }
}

impl BrowserEdgeClient {
    const PEER_SWARM_DIRECT_TARGET: &'static str = "peer-swarm-direct";

    fn preferred_head_artifact_publication(
        view: &HeadArtifactView,
    ) -> Option<(ArtifactProfile, &PublishedArtifactRecord)> {
        const PROFILE_PREFERENCE: [ArtifactProfile; 4] = [
            ArtifactProfile::BrowserSnapshot,
            ArtifactProfile::ManifestOnly,
            ArtifactProfile::ServeCheckpoint,
            ArtifactProfile::FullTrainingCheckpoint,
        ];

        PROFILE_PREFERENCE.into_iter().find_map(|profile| {
            view.published_artifacts
                .iter()
                .filter(|record| {
                    record.artifact_profile == profile
                        && record.status == PublishedArtifactStatus::Ready
                })
                .max_by_key(|record| record.created_at)
                .map(|record| (profile, record))
        })
    }

    fn preferred_head_artifact_profile(view: &HeadArtifactView) -> Option<ArtifactProfile> {
        const PROFILE_PREFERENCE: [ArtifactProfile; 4] = [
            ArtifactProfile::BrowserSnapshot,
            ArtifactProfile::ManifestOnly,
            ArtifactProfile::ServeCheckpoint,
            ArtifactProfile::FullTrainingCheckpoint,
        ];

        PROFILE_PREFERENCE
            .into_iter()
            .find(|profile| view.available_profiles.contains(profile))
            .or_else(|| view.available_profiles.iter().next().cloned())
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    fn direct_peer_artifact_request_seed(
        head: &HeadDescriptor,
        checkpoint: Option<&BrowserArtifactReplayCheckpoint>,
    ) -> Option<DirectPeerArtifactRequestSeed> {
        if let Some(checkpoint) = checkpoint.filter(|checkpoint| {
            checkpoint.experiment_id == head.experiment_id
                && checkpoint.revision_id == head.revision_id
                && checkpoint.head_id == head.head_id
                && checkpoint.artifact_id == head.artifact_id
        }) {
            return Some(DirectPeerArtifactRequestSeed {
                run_id: checkpoint.run_id.clone(),
                artifact_profile: checkpoint.artifact_profile.clone(),
                publication_target_id: checkpoint.publication_target_id.clone(),
                publication_content_hash: checkpoint.publication_content_hash.clone(),
                publication_content_length: checkpoint.publication_content_length,
            });
        }

        Some(DirectPeerArtifactRequestSeed {
            run_id: RunId::derive(&(head.experiment_id.as_str(), head.revision_id.as_str()))
                .ok()?,
            artifact_profile: ArtifactProfile::BrowserSnapshot,
            publication_target_id: PublicationTargetId::new(Self::PEER_SWARM_DIRECT_TARGET),
            publication_content_hash: None,
            publication_content_length: None,
        })
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    fn provider_peer_ids_from_control_snapshot(
        snapshot: &ControlPlaneSnapshot,
        head_id: &HeadId,
    ) -> Vec<PeerId> {
        let mut provider_peer_ids = Vec::new();
        for announcement in snapshot.head_announcements.iter().rev() {
            if &announcement.head.head_id != head_id {
                continue;
            }
            let Some(peer_id) = announcement.provider_peer_id.as_ref() else {
                continue;
            };
            if !provider_peer_ids.contains(peer_id) {
                provider_peer_ids.push(peer_id.clone());
            }
        }
        provider_peer_ids
    }

    fn can_attempt_peer_transport_without_provider_hints(runtime: &BrowserWorkerRuntime) -> bool {
        runtime.transport.connected.is_some()
    }

    fn should_defer_edge_artifact_fallback(runtime: &BrowserWorkerRuntime) -> bool {
        let swarm_status = runtime.swarm_status();
        let direct_selected = matches!(
            swarm_status.desired_transport,
            Some(BrowserTransportFamily::WebRtcDirect | BrowserTransportFamily::WebTransport)
        ) || matches!(
            runtime
                .transport
                .selected
                .as_ref()
                .or(runtime.transport.active.as_ref()),
            Some(BrowserTransportKind::WebRtcDirect | BrowserTransportKind::WebTransport)
        );
        let direct_connected = matches!(
            swarm_status.connected_transport,
            Some(BrowserTransportFamily::WebRtcDirect | BrowserTransportFamily::WebTransport)
        ) || matches!(
            runtime.transport.connected,
            Some(BrowserTransportKind::WebRtcDirect | BrowserTransportKind::WebTransport)
        );
        direct_selected && !direct_connected
    }

    fn build_active_head_artifact_sync_plan_from_view(
        runtime: &BrowserWorkerRuntime,
        view: &HeadArtifactView,
    ) -> Option<ActiveHeadArtifactSyncPlan> {
        let active_assignment = runtime.storage.active_assignment.as_ref()?;
        let active_head_id = runtime.storage.last_head_id.as_ref()?;
        if runtime
            .storage
            .cached_head_artifact_heads
            .contains(active_head_id)
        {
            return None;
        }
        if &view.head.head_id != active_head_id
            || view.head.study_id != active_assignment.study_id
            || view.head.experiment_id != active_assignment.experiment_id
            || view.head.revision_id != active_assignment.revision_id
        {
            return None;
        }

        let publication = Self::preferred_head_artifact_publication(view);
        let artifact_profile = publication
            .as_ref()
            .map(|(profile, _)| profile.clone())
            .or_else(|| {
                if view.provider_peer_ids.is_empty() {
                    None
                } else {
                    Self::preferred_head_artifact_profile(view)
                }
            })?;
        let request = BrowserPeerArtifactRequest {
            experiment_id: view.head.experiment_id.clone(),
            revision_id: view.head.revision_id.clone(),
            run_id: view.run_id.clone(),
            head_id: view.head.head_id.clone(),
            artifact_id: view.head.artifact_id.clone(),
            artifact_profile: artifact_profile.clone(),
            publication_target_id: publication
                .as_ref()
                .map(|(_, publication)| publication.publication_target_id.clone())
                .unwrap_or_else(|| PublicationTargetId::new(Self::PEER_SWARM_DIRECT_TARGET)),
            publication_content_hash: publication
                .as_ref()
                .map(|(_, publication)| publication.content_hash.clone()),
            publication_content_length: publication
                .as_ref()
                .map(|(_, publication)| publication.content_length),
            provider_peer_ids: view.provider_peer_ids.clone(),
        };
        Some(ActiveHeadArtifactSyncPlan {
            head: view.head.clone(),
            run_id: view.run_id.clone(),
            request,
            edge_fallback: publication.as_ref().map(|(_, publication)| {
                ActiveHeadArtifactEdgeFallback {
                    publication: (*publication).clone(),
                }
            }),
        })
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    fn build_active_head_artifact_sync_plan_from_control_snapshot(
        runtime: &BrowserWorkerRuntime,
        snapshot: &ControlPlaneSnapshot,
    ) -> Option<ActiveHeadArtifactSyncPlan> {
        let active_assignment = runtime.storage.active_assignment.as_ref()?;
        let active_head_id = runtime.storage.last_head_id.as_ref()?;
        if runtime
            .storage
            .cached_head_artifact_heads
            .contains(active_head_id)
        {
            return None;
        }
        let head = snapshot
            .head_announcements
            .iter()
            .rev()
            .map(|announcement| &announcement.head)
            .find(|head| {
                &head.head_id == active_head_id
                    && head.study_id == active_assignment.study_id
                    && head.experiment_id == active_assignment.experiment_id
                    && head.revision_id == active_assignment.revision_id
            })?
            .clone();
        let existing_checkpoint = runtime.storage.artifact_replay_checkpoint.as_ref();
        let DirectPeerArtifactRequestSeed {
            run_id,
            artifact_profile,
            publication_target_id,
            publication_content_hash,
            publication_content_length,
        } = Self::direct_peer_artifact_request_seed(&head, existing_checkpoint)?;

        Some(ActiveHeadArtifactSyncPlan {
            request: BrowserPeerArtifactRequest {
                experiment_id: head.experiment_id.clone(),
                revision_id: head.revision_id.clone(),
                run_id: run_id.clone(),
                head_id: head.head_id.clone(),
                artifact_id: head.artifact_id.clone(),
                artifact_profile,
                publication_target_id,
                publication_content_hash,
                publication_content_length,
                provider_peer_ids: Self::provider_peer_ids_from_control_snapshot(
                    snapshot,
                    &head.head_id,
                ),
            },
            head,
            run_id,
            edge_fallback: None,
        })
    }

    async fn sync_active_head_artifact_plan_into_worker(
        &self,
        runtime: &mut BrowserWorkerRuntime,
        session: Option<&BrowserSessionState>,
        plan: ActiveHeadArtifactSyncPlan,
        previous_storage: &crate::BrowserStorageSnapshot,
    ) -> Result<Vec<BrowserWorkerEvent>, BrowserAuthClientError> {
        let existing_checkpoint = runtime.storage.artifact_replay_checkpoint.clone();
        let mut replay_request = plan.request;
        Self::apply_replay_checkpoint(&mut replay_request, existing_checkpoint.as_ref());
        runtime
            .storage
            .remember_artifact_replay_checkpoint(Self::replay_checkpoint_for_request(
                &replay_request,
                existing_checkpoint.as_ref(),
            ));

        let mut transport = None;
        let mut peer_transport_error = None;
        if !replay_request.provider_peer_ids.is_empty()
            || Self::can_attempt_peer_transport_without_provider_hints(runtime)
        {
            let started_at = Instant::now();
            runtime.storage.remember_active_head_artifact_sync_attempt(
                plan.head.head_id.clone(),
                replay_request.artifact_id.clone(),
                replay_request.provider_peer_ids.clone(),
                BrowserPeerArtifactTransportKind::PeerSwarm.label(),
                BrowserArtifactRouteKind::PeerSwarm,
            );
            match self
                .try_download_artifact_via_peer_transport(&replay_request, runtime)
                .await
            {
                Ok(Some(result)) => {
                    if result.bytes.is_empty() {
                        let error = BrowserAuthClientError::ArtifactTransport(
                            "peer transport returned an empty artifact payload".into(),
                        );
                        runtime.storage.remember_active_head_artifact_sync_failure(
                            result.kind.label(),
                            BrowserArtifactRouteKind::PeerSwarm,
                            elapsed_millis(started_at),
                            error.to_string(),
                        );
                        peer_transport_error = Some(error);
                    } else {
                        runtime.storage.remember_active_head_artifact_sync_success(
                            result.kind.label(),
                            BrowserArtifactRouteKind::PeerSwarm,
                            elapsed_millis(started_at),
                            result.bytes.len() as u64,
                        );
                        transport = Some(result.kind.label().to_owned());
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    runtime.storage.remember_active_head_artifact_sync_failure(
                        BrowserPeerArtifactTransportKind::PeerSwarm.label(),
                        artifact_route_kind_for_error(&error),
                        elapsed_millis(started_at),
                        error.to_string(),
                    );
                    peer_transport_error = Some(error);
                }
            }
        }

        if transport.is_none() {
            if let Some(edge_fallback) = plan.edge_fallback.as_ref() {
                if Self::should_defer_edge_artifact_fallback(runtime) {
                    return Ok(Self::storage_update_if_changed(runtime, previous_storage));
                }
                let (Some(session), Some(principal_id)) =
                    (session, session.and_then(BrowserSessionState::principal_id))
                else {
                    return Ok(Self::storage_update_if_changed(runtime, previous_storage));
                };
                let Some(session_id) = session.session_id() else {
                    return Ok(Self::storage_update_if_changed(runtime, previous_storage));
                };
                let started_at = Instant::now();
                runtime.storage.remember_active_head_artifact_sync_attempt(
                    plan.head.head_id.clone(),
                    replay_request.artifact_id.clone(),
                    replay_request.provider_peer_ids.clone(),
                    "edge-download-ticket",
                    BrowserArtifactRouteKind::EdgeHttp,
                );
                match self
                    .request_and_download_artifact_resumable(
                        &DownloadTicketRequest {
                            principal_id: principal_id.clone(),
                            experiment_id: replay_request.experiment_id.clone(),
                            run_id: Some(plan.run_id.clone()),
                            head_id: plan.head.head_id.clone(),
                            artifact_profile: replay_request.artifact_profile.clone(),
                            publication_target_id: edge_fallback
                                .publication
                                .publication_target_id
                                .clone(),
                            artifact_alias_id: edge_fallback.publication.artifact_alias_id.clone(),
                        },
                        Some(session_id),
                        runtime,
                    )
                    .await
                {
                    Ok(_) => {
                        let completed_bytes = runtime
                            .storage
                            .artifact_replay_checkpoint
                            .as_ref()
                            .map(|checkpoint| checkpoint.completed_bytes)
                            .unwrap_or_default();
                        runtime.storage.remember_active_head_artifact_sync_success(
                            "edge-download-ticket",
                            BrowserArtifactRouteKind::EdgeHttp,
                            elapsed_millis(started_at),
                            completed_bytes,
                        );
                        transport = Some("edge-download-ticket".to_owned());
                    }
                    Err(edge_error) => {
                        runtime.storage.remember_active_head_artifact_sync_failure(
                            "edge-download-ticket",
                            BrowserArtifactRouteKind::EdgeHttp,
                            elapsed_millis(started_at),
                            edge_error.to_string(),
                        );
                        if let Some(peer_error) = peer_transport_error {
                            return Err(BrowserAuthClientError::ArtifactTransport(format!(
                                "peer transport failed: {peer_error}; edge fallback failed: {edge_error}"
                            )));
                        }
                        return Err(edge_error);
                    }
                }
            } else if let Some(peer_error) = peer_transport_error {
                return Err(peer_error);
            } else {
                return Ok(Self::storage_update_if_changed(runtime, previous_storage));
            }
        }

        runtime.storage.remember_synced_head_artifact(
            plan.head.head_id,
            plan.head.artifact_id,
            transport.unwrap_or_else(|| "unknown".into()),
        );
        Ok(Self::storage_update_if_changed(runtime, previous_storage))
    }

    /// Creates a new value.
    pub fn new(bindings: BrowserUiBindings, enrollment: BrowserEnrollmentConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            bindings,
            enrollment,
            peer_artifact_fetcher: None,
            trusted_callback_header: None,
            trusted_callback_token: None,
        }
    }

    /// Returns a cloned client that attaches one trusted callback header during callback completion.
    pub fn with_trusted_callback(
        mut self,
        header: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        self.trusted_callback_header = Some(header.into());
        self.trusted_callback_token = Some(token.into());
        self
    }

    /// Returns a copy configured with an explicit browser-native peer artifact fetcher.
    pub fn with_peer_artifact_fetcher<F>(mut self, fetcher: F) -> Self
    where
        F: BrowserPeerArtifactFetcher + 'static,
    {
        self.peer_artifact_fetcher = Some(Arc::new(fetcher));
        self
    }

    fn replay_checkpoint_matches(
        checkpoint: &BrowserArtifactReplayCheckpoint,
        request: &BrowserPeerArtifactRequest,
    ) -> bool {
        checkpoint.experiment_id == request.experiment_id
            && checkpoint.revision_id == request.revision_id
            && checkpoint.run_id == request.run_id
            && checkpoint.head_id == request.head_id
            && checkpoint.artifact_id == request.artifact_id
            && checkpoint.artifact_profile == request.artifact_profile
            && checkpoint.publication_target_id == request.publication_target_id
            && checkpoint.publication_content_hash == request.publication_content_hash
            && checkpoint.publication_content_length == request.publication_content_length
    }

    fn storage_update_if_changed(
        runtime: &BrowserWorkerRuntime,
        previous_storage: &crate::BrowserStorageSnapshot,
    ) -> Vec<BrowserWorkerEvent> {
        if runtime.storage == *previous_storage {
            Vec::new()
        } else {
            vec![BrowserWorkerEvent::StorageUpdated(Box::new(
                runtime.storage.clone(),
            ))]
        }
    }

    fn apply_replay_checkpoint(
        request: &mut BrowserPeerArtifactRequest,
        checkpoint: Option<&BrowserArtifactReplayCheckpoint>,
    ) {
        let Some(checkpoint) = checkpoint else {
            return;
        };
        if !Self::replay_checkpoint_matches(checkpoint, request) {
            return;
        }

        let mut provider_peer_ids = checkpoint.provider_peer_ids.clone();
        for peer_id in &request.provider_peer_ids {
            if !provider_peer_ids.contains(peer_id) {
                provider_peer_ids.push(peer_id.clone());
            }
        }
        request.provider_peer_ids = provider_peer_ids;
    }

    fn replay_checkpoint_for_request(
        request: &BrowserPeerArtifactRequest,
        checkpoint: Option<&BrowserArtifactReplayCheckpoint>,
    ) -> BrowserArtifactReplayCheckpoint {
        let (
            artifact_descriptor,
            completed_chunks,
            edge_download_prefix,
            edge_download_segments,
            completed_bytes,
            attempt_count,
        ) = checkpoint
            .filter(|checkpoint| Self::replay_checkpoint_matches(checkpoint, request))
            .map(|checkpoint| {
                (
                    checkpoint.artifact_descriptor.clone(),
                    checkpoint.completed_chunks.clone(),
                    checkpoint.edge_download_prefix.clone(),
                    checkpoint.edge_download_segments.clone(),
                    checkpoint.completed_bytes,
                    checkpoint.attempt_count + 1,
                )
            })
            .unwrap_or((None, Vec::new(), None, Vec::new(), 0, 1));
        BrowserArtifactReplayCheckpoint {
            experiment_id: request.experiment_id.clone(),
            revision_id: request.revision_id.clone(),
            run_id: request.run_id.clone(),
            head_id: request.head_id.clone(),
            artifact_id: request.artifact_id.clone(),
            artifact_profile: request.artifact_profile.clone(),
            publication_target_id: request.publication_target_id.clone(),
            publication_content_hash: request.publication_content_hash.clone(),
            publication_content_length: request.publication_content_length,
            provider_peer_ids: request.provider_peer_ids.clone(),
            artifact_descriptor,
            completed_chunks,
            edge_download_prefix,
            edge_download_segments,
            completed_bytes,
            last_attempted_at: Utc::now(),
            attempt_count,
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

    /// Fetches the app snapshot.
    pub async fn fetch_browser_edge_snapshot(
        &self,
    ) -> Result<BrowserEdgeSnapshot, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.app_snapshot_path, None)
            .await
    }

    /// Fetches the signed browser seed advertisement, when the edge exposes it.
    pub async fn fetch_browser_seed_advertisement(
        &self,
    ) -> Result<
        Option<SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>>>,
        BrowserAuthClientError,
    > {
        self.get_optional_json(&self.bindings.paths.browser_seed_advertisement_path, None)
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

    /// Fetches canonical-head adoption curves across visible experiment revisions.
    pub async fn fetch_metrics_head_adoption_curves(
        &self,
    ) -> Result<Vec<CanonicalHeadAdoptionCurve>, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.metrics_head_adoption_curves_path, None)
            .await
    }

    /// Fetches canonical-head adoption curves for one experiment.
    pub async fn fetch_metrics_head_adoption_curves_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<CanonicalHeadAdoptionCurve>, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.metrics_head_adoption_curves_path,
            experiment_id.as_str()
        );
        self.get_json(&path, None).await
    }

    /// Fetches latest-canonical visible-head population histograms across revisions.
    pub async fn fetch_metrics_head_populations(
        &self,
    ) -> Result<Vec<VisibleHeadPopulationHistogram>, BrowserAuthClientError> {
        self.get_json(&self.bindings.paths.metrics_head_populations_path, None)
            .await
    }

    /// Fetches latest-canonical visible-head population histograms for one experiment.
    pub async fn fetch_metrics_head_populations_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<VisibleHeadPopulationHistogram>, BrowserAuthClientError> {
        let path = format!(
            "{}/{}",
            self.bindings.paths.metrics_head_populations_path,
            experiment_id.as_str()
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

    /// Downloads artifact bytes for a previously issued ticket.
    pub async fn download_artifact_bytes(
        &self,
        ticket_id: &burn_p2p_core::DownloadTicketId,
    ) -> Result<Vec<u8>, BrowserAuthClientError> {
        self.get_bytes_absolute(&self.artifact_download_url(ticket_id))
            .await
    }

    async fn persist_replay_storage(
        runtime: &BrowserWorkerRuntime,
    ) -> Result<(), BrowserAuthClientError> {
        let Some(network_id) = runtime
            .config
            .as_ref()
            .map(|config| config.network_id.clone())
        else {
            return Ok(());
        };
        crate::durability::persist_durable_browser_storage(&network_id, &runtime.storage)
            .await
            .map_err(BrowserAuthClientError::ArtifactTransport)
    }

    fn parse_download_total_bytes_from_parts(
        status: StatusCode,
        content_range: Option<&str>,
        content_length: Option<u64>,
        completed_prefix_len: usize,
    ) -> Option<u64> {
        content_range
            .and_then(|value| value.split_once('/').map(|(_, total)| total))
            .and_then(|total| total.parse::<u64>().ok())
            .or_else(|| {
                content_length.map(|length| {
                    if status == StatusCode::PARTIAL_CONTENT {
                        length + completed_prefix_len as u64
                    } else {
                        length
                    }
                })
            })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn parse_download_total_bytes(
        response: &reqwest::Response,
        completed_prefix_len: usize,
    ) -> Option<u64> {
        Self::parse_download_total_bytes_from_parts(
            response.status(),
            response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok()),
            response.content_length(),
            completed_prefix_len,
        )
    }

    #[cfg(target_arch = "wasm32")]
    async fn download_artifact_bytes_resumable(
        &self,
        ticket_id: &burn_p2p_core::DownloadTicketId,
        runtime: &mut BrowserWorkerRuntime,
    ) -> Result<Vec<u8>, BrowserAuthClientError> {
        use js_sys::{Reflect, Uint8Array};
        use wasm_bindgen::{JsCast, JsValue};
        use wasm_bindgen_futures::JsFuture;

        let mut prefix_bytes = if let Some(bytes) =
            runtime.storage.artifact_replay_edge_prefix_bytes()
        {
            bytes
        } else if let (Some(network_id), Some(checkpoint)) = (
            runtime
                .config
                .as_ref()
                .map(|config| config.network_id.clone()),
            runtime.storage.artifact_replay_checkpoint.as_ref(),
        ) {
            crate::durability::load_durable_browser_artifact_replay_prefix(&network_id, checkpoint)
                .await
                .map_err(BrowserAuthClientError::ArtifactTransport)?
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        let Some(window) = web_sys::window() else {
            return Err(BrowserAuthClientError::ArtifactTransport(
                "browser window was unavailable for edge replay".into(),
            ));
        };
        let url = self.artifact_download_url(ticket_id);
        let request_init = web_sys::RequestInit::new();
        request_init.set_method("GET");
        let headers = web_sys::Headers::new()
            .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
        if !prefix_bytes.is_empty() {
            headers
                .append("Range", &format!("bytes={}-", prefix_bytes.len()))
                .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
        }
        request_init.set_headers(&headers);
        let request = web_sys::Request::new_with_str_and_init(&url, &request_init)
            .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
        let response_value = JsFuture::from(window.fetch_with_request(&request))
            .await
            .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
        let response = response_value
            .dyn_into::<web_sys::Response>()
            .map_err(|_| {
                BrowserAuthClientError::ArtifactTransport(
                    "browser edge fetch returned a non-response object".into(),
                )
            })?;
        let status = StatusCode::from_u16(response.status()).map_err(|_| {
            BrowserAuthClientError::ArtifactTransport(format!(
                "browser edge returned invalid HTTP status {}",
                response.status()
            ))
        })?;
        if !status.is_success() {
            return Err(BrowserAuthClientError::ArtifactTransport(format!(
                "browser edge returned HTTP {} for resumable artifact download",
                status.as_u16()
            )));
        }
        if !prefix_bytes.is_empty() && status != StatusCode::PARTIAL_CONTENT {
            prefix_bytes.clear();
        }
        let content_range = response
            .headers()
            .get("content-range")
            .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
        let content_length = response
            .headers()
            .get("content-length")
            .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?
            .and_then(|value| value.parse::<u64>().ok());
        let total_bytes = Self::parse_download_total_bytes_from_parts(
            status,
            content_range.as_deref(),
            content_length,
            prefix_bytes.len(),
        );
        let body = response.body().ok_or_else(|| {
            BrowserAuthClientError::ArtifactTransport(
                "browser edge response body was unavailable".into(),
            )
        })?;
        let reader = body
            .get_reader()
            .dyn_into::<web_sys::ReadableStreamDefaultReader>()
            .map_err(|_| {
                BrowserAuthClientError::ArtifactTransport(
                    "browser edge response body reader was unavailable".into(),
                )
            })?;

        loop {
            let read_result = JsFuture::from(reader.read())
                .await
                .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
            let done = Reflect::get(&read_result, &JsValue::from_str("done"))
                .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?
                .as_bool()
                .unwrap_or(false);
            if done {
                break;
            }
            let value = Reflect::get(&read_result, &JsValue::from_str("value"))
                .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
            let chunk = Uint8Array::new(&value).to_vec();
            if chunk.is_empty() {
                continue;
            }
            prefix_bytes.extend_from_slice(&chunk);
            runtime
                .storage
                .remember_artifact_replay_edge_prefix(total_bytes, prefix_bytes.clone());
            Self::persist_replay_storage(runtime).await?;
        }

        if let Some(total_bytes) = total_bytes
            && prefix_bytes.len() as u64 != total_bytes
        {
            return Err(BrowserAuthClientError::ArtifactTransport(format!(
                "edge replay reconstructed {} bytes but expected {}",
                prefix_bytes.len(),
                total_bytes
            )));
        }

        Ok(prefix_bytes)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn download_artifact_bytes_resumable(
        &self,
        ticket_id: &burn_p2p_core::DownloadTicketId,
        runtime: &mut BrowserWorkerRuntime,
    ) -> Result<Vec<u8>, BrowserAuthClientError> {
        let mut prefix_bytes = if let Some(bytes) =
            runtime.storage.artifact_replay_edge_prefix_bytes()
        {
            bytes
        } else if let (Some(network_id), Some(checkpoint)) = (
            runtime
                .config
                .as_ref()
                .map(|config| config.network_id.clone()),
            runtime.storage.artifact_replay_checkpoint.as_ref(),
        ) {
            crate::durability::load_durable_browser_artifact_replay_prefix(&network_id, checkpoint)
                .await
                .map_err(BrowserAuthClientError::ArtifactTransport)?
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        let url = self.artifact_download_url(ticket_id);
        let mut request = self.http.get(url);
        if !prefix_bytes.is_empty() {
            request = request.header(
                reqwest::header::RANGE,
                format!("bytes={}-", prefix_bytes.len()),
            );
        }
        let mut response = request.send().await?;
        if !response.status().is_success() {
            return Err(BrowserAuthClientError::Http(
                response.error_for_status().expect_err("non-success status"),
            ));
        }
        if !prefix_bytes.is_empty() && response.status() != StatusCode::PARTIAL_CONTENT {
            prefix_bytes.clear();
        }
        let total_bytes = Self::parse_download_total_bytes(&response, prefix_bytes.len());

        while let Some(chunk) = response.chunk().await? {
            prefix_bytes.extend_from_slice(&chunk);
            runtime
                .storage
                .remember_artifact_replay_edge_prefix(total_bytes, prefix_bytes.clone());
            Self::persist_replay_storage(runtime).await?;
        }

        if let Some(total_bytes) = total_bytes
            && prefix_bytes.len() as u64 != total_bytes
        {
            return Err(BrowserAuthClientError::ArtifactTransport(format!(
                "edge replay reconstructed {} bytes but expected {}",
                prefix_bytes.len(),
                total_bytes
            )));
        }

        Ok(prefix_bytes)
    }

    /// Requests a short-lived artifact ticket and immediately downloads the
    /// corresponding artifact bytes.
    pub async fn request_and_download_artifact(
        &self,
        request: &DownloadTicketRequest,
        session_id: Option<&ContentId>,
    ) -> Result<Vec<u8>, BrowserAuthClientError> {
        let ticket = self
            .request_artifact_download_ticket(request, session_id)
            .await?;
        self.download_artifact_bytes(&ticket.ticket.download_ticket_id)
            .await
    }

    async fn request_and_download_artifact_resumable(
        &self,
        request: &DownloadTicketRequest,
        session_id: Option<&ContentId>,
        runtime: &mut BrowserWorkerRuntime,
    ) -> Result<Vec<u8>, BrowserAuthClientError> {
        let ticket = self
            .request_artifact_download_ticket(request, session_id)
            .await?;
        self.download_artifact_bytes_resumable(&ticket.ticket.download_ticket_id, runtime)
            .await
    }

    async fn try_download_artifact_via_peer_transport(
        &self,
        request: &BrowserPeerArtifactRequest,
        runtime: &mut BrowserWorkerRuntime,
    ) -> Result<Option<BrowserPeerArtifactTransportResult>, BrowserAuthClientError> {
        if let Some(fetcher) = self.peer_artifact_fetcher.as_ref() {
            return fetcher.fetch(request.clone()).await.map(|bytes| {
                Some(BrowserPeerArtifactTransportResult {
                    bytes,
                    kind: BrowserPeerArtifactTransportKind::CustomFetcher,
                })
            });
        }
        #[cfg(target_arch = "wasm32")]
        {
            return try_download_artifact_via_browser_peer_transport(request, runtime).await;
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (request, runtime);
            Ok(None)
        }
    }

    /// Synchronizes the active head artifact into the browser worker cache.
    pub async fn sync_active_head_artifact_into_worker(
        &self,
        runtime: &mut BrowserWorkerRuntime,
        session: Option<&BrowserSessionState>,
    ) -> Result<Vec<BrowserWorkerEvent>, BrowserAuthClientError> {
        let previous_storage = runtime.storage.clone();
        runtime.storage.invalidate_stale_assignment_replay_state();
        let Some(session) = session else {
            return Ok(Self::storage_update_if_changed(runtime, &previous_storage));
        };
        if session.session_id().is_none() || session.principal_id().is_none() {
            return Ok(Self::storage_update_if_changed(runtime, &previous_storage));
        }
        let Some(active_head_id) = runtime.storage.last_head_id.as_ref() else {
            return Ok(Self::storage_update_if_changed(runtime, &previous_storage));
        };
        if runtime
            .storage
            .cached_head_artifact_heads
            .contains(active_head_id)
        {
            return Ok(Self::storage_update_if_changed(runtime, &previous_storage));
        }
        let view = self.fetch_head_artifact_view(active_head_id).await?;
        let Some(plan) = Self::build_active_head_artifact_sync_plan_from_view(runtime, &view)
        else {
            return Ok(Self::storage_update_if_changed(runtime, &previous_storage));
        };
        self.sync_active_head_artifact_plan_into_worker(
            runtime,
            Some(session),
            plan,
            &previous_storage,
        )
        .await
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) async fn sync_active_head_artifact_from_control_snapshot_into_worker(
        &self,
        runtime: &mut BrowserWorkerRuntime,
        snapshot: &ControlPlaneSnapshot,
    ) -> Result<Vec<BrowserWorkerEvent>, BrowserAuthClientError> {
        let previous_storage = runtime.storage.clone();
        runtime.storage.invalidate_stale_assignment_replay_state();
        let Some(plan) =
            Self::build_active_head_artifact_sync_plan_from_control_snapshot(runtime, snapshot)
        else {
            return Ok(Self::storage_update_if_changed(runtime, &previous_storage));
        };
        self.sync_active_head_artifact_plan_into_worker(runtime, None, plan, &previous_storage)
            .await
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
        let app_snapshot = self.fetch_browser_edge_snapshot().await?;
        let signed_seed_advertisement = self.fetch_browser_seed_advertisement().await?;
        if let Some(config) = runtime.config.as_mut() {
            config.seed_bootstrap = resolve_browser_seed_bootstrap(
                &config.network_id,
                signed_seed_advertisement.as_ref(),
                &config.site_seed_node_urls,
                app_snapshot.captured_at,
            );
        }
        let mut events = runtime.apply_command(
            BrowserWorkerCommand::ApplySwarmStatus(Box::new(
                runtime.planned_swarm_status_snapshot(),
            )),
            None,
            None,
        );
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
        let live_metrics_event = self
            .get_optional_json(&self.bindings.paths.metrics_live_latest_path, None)
            .await?;
        let heads = app_snapshot.heads.clone();
        let mut transport = BrowserTransportStatus::enabled(
            app_snapshot.transports.webrtc_direct,
            app_snapshot.transports.webtransport_gateway,
            app_snapshot.transports.wss_fallback,
        );
        transport.active = runtime.transport.active.clone();
        transport.selected = runtime.transport.selected.clone();
        transport.connected = runtime.transport.connected.clone();
        transport.connected_peer_ids = runtime.transport.connected_peer_ids.clone();
        transport.last_error = runtime.transport.last_error.clone();
        events.extend(runtime.apply_edge_sync(
            signed_directory,
            &heads,
            signed_leaderboard,
            BrowserMetricsSyncState {
                catchup_bundles: metrics_catchup,
                live_event: live_metrics_event,
            },
            transport,
            session,
        ));
        match self
            .sync_active_head_artifact_into_worker(runtime, session)
            .await
        {
            Ok(artifact_events) => events.extend(artifact_events),
            Err(error) => events.push(BrowserWorkerEvent::Error {
                message: format!("browser artifact sync failed: {error}"),
            }),
        }
        Ok(events)
    }

    /// Performs the flush worker receipts operation.
    pub async fn flush_worker_receipts(
        &self,
        runtime: &mut BrowserWorkerRuntime,
    ) -> Result<Vec<BrowserWorkerEvent>, BrowserAuthClientError> {
        let mut events =
            runtime.apply_command(BrowserWorkerCommand::FlushReceiptOutbox, None, None);
        let mut acknowledged_ids = Vec::new();
        let mut deferred_submission: Option<BrowserWorkerEvent> = None;

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
                let receipt_ids = receipts
                    .iter()
                    .map(|receipt| receipt.receipt_id.clone())
                    .collect::<Vec<_>>();
                match self.submit_receipts_with_retry(session_id, receipts).await {
                    Ok(submission) => acknowledged_ids.extend(submission.accepted_receipt_ids),
                    Err(error) if error.is_retryable_receipt_submission() => {
                        deferred_submission = Some(BrowserWorkerEvent::ReceiptSubmissionDeferred {
                            receipt_ids,
                            pending_receipts: runtime.storage.pending_receipts.len(),
                            reason: error.to_string(),
                            retryable: true,
                        });
                    }
                    Err(error) => return Err(error),
                }
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
        if let Some(event) = deferred_submission {
            events.push(event);
        }

        Ok(events)
    }

    async fn submit_receipts_with_retry(
        &self,
        session_id: &ContentId,
        receipts: &[ContributionReceipt],
    ) -> Result<BrowserReceiptSubmissionResponse, BrowserAuthClientError> {
        const MAX_ATTEMPTS: usize = 3;

        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.submit_receipts(session_id, receipts).await {
                Ok(response) => return Ok(response),
                Err(error) if attempt < MAX_ATTEMPTS && error.is_retryable_receipt_submission() => {
                    wait_receipt_retry_delay(attempt).await;
                }
                Err(error) => return Err(error),
            }
        }
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

        let payload = CallbackPayload {
            login_id: login.login_id.clone(),
            state: login.state.clone(),
            principal_id,
            provider_code,
        };
        if let (Some(header), Some(token)) = (
            self.trusted_callback_header.as_deref(),
            self.trusted_callback_token.as_deref(),
        ) {
            return self
                .post_json_with_header(&self.enrollment.callback_path, &payload, header, token)
                .await;
        }
        self.post_json(&self.enrollment.callback_path, &payload)
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

    async fn get_bytes_absolute(&self, url: &str) -> Result<Vec<u8>, BrowserAuthClientError> {
        Ok(self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?
            .to_vec())
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

    async fn post_json_with_header<Req, Res>(
        &self,
        path: &str,
        body: &Req,
        header: &str,
        value: &str,
    ) -> Result<Res, BrowserAuthClientError>
    where
        Req: Serialize + ?Sized,
        Res: DeserializeOwned,
    {
        Ok(self
            .http
            .post(self.bindings.endpoint_url(path))
            .header(header, value)
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

#[cfg(target_arch = "wasm32")]
async fn wait_receipt_retry_delay(attempt: usize) {
    const DELAYS_MS: [u32; 2] = [250, 1_000];
    let delay_ms = DELAYS_MS
        .get(attempt.saturating_sub(1))
        .copied()
        .unwrap_or(1_000);
    TimeoutFuture::new(delay_ms).await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn wait_receipt_retry_delay(_attempt: usize) {}

#[cfg(target_arch = "wasm32")]
async fn try_download_artifact_via_browser_peer_transport(
    request: &BrowserPeerArtifactRequest,
    runtime: &mut BrowserWorkerRuntime,
) -> Result<Option<BrowserPeerArtifactTransportResult>, BrowserAuthClientError> {
    if let Some(bytes) = try_download_artifact_via_browser_peer_swarm(request, runtime).await? {
        return Ok(Some(BrowserPeerArtifactTransportResult {
            bytes,
            kind: BrowserPeerArtifactTransportKind::PeerSwarm,
        }));
    }
    Ok(None)
}

#[cfg(target_arch = "wasm32")]
async fn try_download_artifact_via_browser_peer_swarm(
    request: &BrowserPeerArtifactRequest,
    runtime: &mut BrowserWorkerRuntime,
) -> Result<Option<Vec<u8>>, BrowserAuthClientError> {
    use js_sys::{Function, Promise, Reflect, Uint8Array};
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;

    async fn persist_replay_progress(
        runtime: &BrowserWorkerRuntime,
    ) -> Result<(), BrowserAuthClientError> {
        let Some(network_id) = runtime
            .config
            .as_ref()
            .map(|config| config.network_id.clone())
        else {
            return Ok(());
        };
        crate::durability::persist_durable_browser_storage(&network_id, &runtime.storage)
            .await
            .map_err(BrowserAuthClientError::ArtifactTransport)
    }

    fn checkpoint_descriptor_for_request(
        runtime: &BrowserWorkerRuntime,
        request: &BrowserPeerArtifactRequest,
    ) -> Option<ArtifactDescriptor> {
        let checkpoint = runtime.storage.artifact_replay_checkpoint.as_ref()?;
        if checkpoint.experiment_id != request.experiment_id
            || checkpoint.revision_id != request.revision_id
            || checkpoint.run_id != request.run_id
            || checkpoint.head_id != request.head_id
            || checkpoint.artifact_id != request.artifact_id
            || checkpoint.artifact_profile != request.artifact_profile
            || checkpoint.publication_target_id != request.publication_target_id
            || checkpoint.publication_content_hash != request.publication_content_hash
            || checkpoint.publication_content_length != request.publication_content_length
        {
            return None;
        }
        checkpoint.artifact_descriptor.clone()
    }

    async fn stored_checkpoint_chunk(
        runtime: &BrowserWorkerRuntime,
        chunk: &ChunkDescriptor,
    ) -> Result<Option<Vec<u8>>, BrowserAuthClientError> {
        let checkpoint = match runtime.storage.artifact_replay_checkpoint.as_ref() {
            Some(checkpoint) => checkpoint,
            None => return Ok(None),
        };
        let bytes =
            if let Some(bytes) = runtime.storage.artifact_replay_chunk_bytes(&chunk.chunk_id) {
                bytes.to_vec()
            } else {
                let Some(network_id) = runtime
                    .config
                    .as_ref()
                    .map(|config| config.network_id.clone())
                else {
                    return Ok(None);
                };
                let Some(bytes) = crate::durability::load_durable_browser_artifact_replay_chunk(
                    &network_id,
                    &checkpoint.artifact_id,
                    &chunk.chunk_id,
                )
                .await
                .map_err(BrowserAuthClientError::ArtifactTransport)?
                else {
                    return Ok(None);
                };
                bytes
            };
        if bytes.len() as u64 != chunk.length_bytes {
            return Ok(None);
        }
        let chunk_hash = ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(&bytes));
        if chunk_hash != chunk.chunk_hash {
            return Ok(None);
        }
        Ok(Some(bytes))
    }

    let Some(window) = web_sys::window() else {
        return Ok(None);
    };
    let swarm_value = Reflect::get(
        &JsValue::from(window.clone()),
        &JsValue::from_str("__burnP2PArtifactSwarm"),
    )
    .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
    if swarm_value.is_undefined() || swarm_value.is_null() {
        return Ok(None);
    }

    let descriptor = if let Some(descriptor) = checkpoint_descriptor_for_request(runtime, request) {
        descriptor
    } else {
        let fetch_manifest =
            Reflect::get(&swarm_value, &JsValue::from_str("fetchArtifactManifest"))
                .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
        let fetch_manifest = fetch_manifest.dyn_into::<Function>().map_err(|_| {
            BrowserAuthClientError::ArtifactTransport(
                "browser peer artifact swarm is missing fetchArtifactManifest".into(),
            )
        })?;
        let request_value = serde_wasm_bindgen::to_value(request)
            .map_err(|error| BrowserAuthClientError::ArtifactTransport(error.to_string()))?;
        let manifest_promise = fetch_manifest
            .call1(&swarm_value, &request_value)
            .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
        let manifest_value = JsFuture::from(Promise::from(manifest_promise))
            .await
            .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
        if manifest_value.is_undefined() || manifest_value.is_null() {
            return Ok(None);
        }
        let descriptor: burn_p2p::ArtifactDescriptor =
            serde_wasm_bindgen::from_value(manifest_value)
                .map_err(|error| BrowserAuthClientError::ArtifactTransport(error.to_string()))?;
        runtime
            .storage
            .remember_artifact_replay_descriptor(descriptor.clone());
        persist_replay_progress(runtime).await?;
        descriptor
    };
    if descriptor.artifact_id != request.artifact_id {
        return Err(BrowserAuthClientError::ArtifactTransport(format!(
            "browser peer swarm returned descriptor for unexpected artifact {}",
            descriptor.artifact_id.as_str()
        )));
    }

    let fetch_chunk = Reflect::get(&swarm_value, &JsValue::from_str("fetchArtifactChunk"))
        .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
    let fetch_chunk = fetch_chunk.dyn_into::<Function>().map_err(|_| {
        BrowserAuthClientError::ArtifactTransport(
            "browser peer artifact swarm is missing fetchArtifactChunk".into(),
        )
    })?;

    let mut chunks = descriptor.chunks.clone();
    chunks.sort_by(|left, right| {
        left.offset_bytes
            .cmp(&right.offset_bytes)
            .then_with(|| left.chunk_id.cmp(&right.chunk_id))
    });

    let mut bytes = Vec::with_capacity(descriptor.bytes_len as usize);
    for chunk in &chunks {
        if let Some(chunk_bytes) = stored_checkpoint_chunk(runtime, chunk).await? {
            bytes.extend_from_slice(&chunk_bytes);
            continue;
        }
        let chunk_request = BrowserPeerArtifactChunkRequest {
            artifact: request.clone(),
            chunk_id: chunk.chunk_id.clone(),
        };
        let chunk_request_value = serde_wasm_bindgen::to_value(&chunk_request)
            .map_err(|error| BrowserAuthClientError::ArtifactTransport(error.to_string()))?;
        let chunk_promise = fetch_chunk
            .call1(&swarm_value, &chunk_request_value)
            .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
        let chunk_value = JsFuture::from(Promise::from(chunk_promise))
            .await
            .map_err(|error| BrowserAuthClientError::ArtifactTransport(format!("{error:?}")))?;
        if chunk_value.is_undefined() || chunk_value.is_null() {
            return Ok(None);
        }
        let chunk_bytes = if chunk_value.is_instance_of::<js_sys::ArrayBuffer>()
            || chunk_value.is_instance_of::<Uint8Array>()
        {
            Uint8Array::new(&chunk_value).to_vec()
        } else if let Ok(bytes_value) = Reflect::get(&chunk_value, &JsValue::from_str("bytes")) {
            if bytes_value.is_undefined() || bytes_value.is_null() {
                return Err(BrowserAuthClientError::ArtifactTransport(format!(
                    "browser peer swarm returned an empty chunk payload for {}",
                    chunk.chunk_id.as_str()
                )));
            }
            Uint8Array::new(&bytes_value).to_vec()
        } else {
            return Err(BrowserAuthClientError::ArtifactTransport(format!(
                "browser peer swarm returned an unsupported chunk payload for {}",
                chunk.chunk_id.as_str()
            )));
        };
        if chunk_bytes.len() as u64 != chunk.length_bytes {
            return Err(BrowserAuthClientError::ArtifactTransport(format!(
                "browser peer swarm returned chunk {} with unexpected length {} (expected {})",
                chunk.chunk_id.as_str(),
                chunk_bytes.len(),
                chunk.length_bytes
            )));
        }
        let chunk_hash =
            ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(&chunk_bytes));
        if chunk_hash != chunk.chunk_hash {
            return Err(BrowserAuthClientError::ArtifactTransport(format!(
                "browser peer swarm returned chunk {} with unexpected hash {}",
                chunk.chunk_id.as_str(),
                chunk_hash.as_str()
            )));
        }
        runtime
            .storage
            .remember_artifact_replay_chunk(chunk.chunk_id.clone(), chunk_bytes.clone());
        persist_replay_progress(runtime).await?;
        bytes.extend_from_slice(&chunk_bytes);
    }

    if bytes.len() as u64 != descriptor.bytes_len {
        return Err(BrowserAuthClientError::ArtifactTransport(format!(
            "browser peer swarm reconstructed {} bytes for {} but descriptor expected {}",
            bytes.len(),
            descriptor.artifact_id.as_str(),
            descriptor.bytes_len
        )));
    }
    let root_hash = ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(&bytes));
    if root_hash != descriptor.root_hash {
        return Err(BrowserAuthClientError::ArtifactTransport(format!(
            "browser peer swarm reconstructed {} with unexpected root hash {}",
            descriptor.artifact_id.as_str(),
            root_hash.as_str()
        )));
    }

    Ok(Some(bytes))
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
async fn try_download_artifact_via_browser_peer_transport(
    _request: &BrowserPeerArtifactRequest,
    _runtime: &mut BrowserWorkerRuntime,
) -> Result<Option<BrowserPeerArtifactTransportResult>, BrowserAuthClientError> {
    Ok(None)
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
async fn try_download_artifact_via_browser_peer_swarm(
    _request: &BrowserPeerArtifactRequest,
    _runtime: &mut BrowserWorkerRuntime,
) -> Result<Option<Vec<u8>>, BrowserAuthClientError> {
    Ok(None)
}

#[cfg(not(target_arch = "wasm32"))]
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

#[cfg(not(target_arch = "wasm32"))]
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
