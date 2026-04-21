use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use burn_p2p::{
    ArtifactDescriptor, ArtifactId, BrowserEdgeSnapshot, ClientReleaseManifest, ContentId,
    ExperimentDirectoryEntry, ExperimentId, ExperimentScope, HeadDescriptor, HeadId, LeaseId,
    MicroShardId, NetworkManifest, PeerId, PeerRole, PeerRoleSet, PrincipalId, RevisionId,
    WorkloadId, WorkloadTrainingLease,
};
use burn_p2p_browser::{
    BrowserCapabilityReport, BrowserEdgeClient, BrowserEnrollmentConfig, BrowserGpuSupport,
    BrowserRuntimeRole, BrowserSessionRuntimeConfig, BrowserSessionRuntimeHandle,
    BrowserTrainingBudget, BrowserTrainingPlan, BrowserUiBindings, BrowserWorkerIdentity,
};
#[cfg(not(target_arch = "wasm32"))]
use burn_p2p_browser::{BrowserTransportKind, BrowserTransportStatus};
use burn_p2p_core::{
    BrowserLeaderboardEntry, BrowserSeedRecord, PublicationTargetId, PublishedArtifactId, RunId,
};
use burn_p2p_metrics::MetricsCatchupBundle;
#[cfg(target_arch = "wasm32")]
use gloo_net::http::Request;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[cfg(not(target_arch = "wasm32"))]
use anyhow::ensure;
#[cfg(not(target_arch = "wasm32"))]
use burn_p2p::{
    AuthProvider, BrowserLoginProvider, BrowserMode, ContributionReceipt, IdentityConnector,
    PrincipalClaims, ProjectFamilyId, RevocationEpoch, StaticIdentityConnector,
    StaticPrincipalRecord,
};
#[cfg(not(target_arch = "wasm32"))]
use burn_p2p::{NodeCertificateAuthority, NodeEnrollmentRequest, PrincipalSession, RunningNode};
#[cfg(not(target_arch = "wasm32"))]
use burn_p2p_browser::BrowserPeerEnrollmentRequest;
#[cfg(not(target_arch = "wasm32"))]
use burn_p2p_core::{
    BrowserDirectorySnapshot, BrowserEdgeMode, BrowserEdgePaths, BrowserLeaderboardSnapshot,
    BrowserReceiptSubmissionResponse, BrowserSeedAdvertisement, BrowserSeedTransportKind,
    BrowserSeedTransportPolicy, BrowserTransportSurface, DownloadDeliveryMode, DownloadTicket,
    DownloadTicketId, PublishedArtifactRecord, PublishedArtifactStatus, SchemaEnvelope,
    SignatureAlgorithm, SignatureMetadata, SignedPayload, SocialMode, TrustBundleExport,
    TrustedIssuerStatus,
};
#[cfg(not(target_arch = "wasm32"))]
use burn_p2p_publish::{DownloadTicketRequest, DownloadTicketResponse, HeadArtifactView};
#[cfg(not(target_arch = "wasm32"))]
use chrono::{Duration as ChronoDuration, Utc};
#[cfg(not(target_arch = "wasm32"))]
use libp2p_identity::Keypair;
#[cfg(not(target_arch = "wasm32"))]
use semver::Version;
#[cfg(not(target_arch = "wasm32"))]
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(not(target_arch = "wasm32"))]
const LOGIN_PATH: &str = "/login/static";
#[cfg(not(target_arch = "wasm32"))]
const CALLBACK_PATH: &str = "/callback/static";
#[cfg(not(target_arch = "wasm32"))]
const ENROLL_PATH: &str = "/enroll";
#[cfg(not(target_arch = "wasm32"))]
const RECEIPTS_PATH: &str = "/receipts/browser";
#[cfg(not(target_arch = "wasm32"))]
const PORTAL_SNAPSHOT_PATH: &str = "/portal/snapshot";
#[cfg(not(target_arch = "wasm32"))]
const DATASET_PREFIX: &str = "/dataset";
#[cfg(not(target_arch = "wasm32"))]
const DATASET_MANIFEST_PATH: &str = "/dataset/fetch-manifest.json";
#[cfg(not(target_arch = "wasm32"))]
const DIRECTORY_PATH: &str = "/directory";
#[cfg(not(target_arch = "wasm32"))]
const SIGNED_DIRECTORY_PATH: &str = "/directory/signed";
#[cfg(not(target_arch = "wasm32"))]
const HEADS_PATH: &str = "/heads";
#[cfg(not(target_arch = "wasm32"))]
const LEADERBOARD_PATH: &str = "/leaderboard";
#[cfg(not(target_arch = "wasm32"))]
const SIGNED_LEADERBOARD_PATH: &str = "/leaderboard/signed";
#[cfg(not(target_arch = "wasm32"))]
const SIGNED_BROWSER_SEEDS_PATH: &str = "/browser/seeds/signed";
#[cfg(not(target_arch = "wasm32"))]
const TRUST_PATH: &str = "/trust";
#[cfg(not(target_arch = "wasm32"))]
const METRICS_CATCHUP_PATH: &str = "/metrics/catchup";
#[cfg(not(target_arch = "wasm32"))]
const METRICS_LIVE_LATEST_PATH: &str = "/metrics/live/latest";
#[cfg(not(target_arch = "wasm32"))]
const EDGE_FIXTURE_LABEL: &str = "browser-edge-fixture";
const DEFAULT_TARGET_ARTIFACT_ID: &str = "browser-wasm";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One serialized fixture asset served by the live browser edge.
pub struct LiveBrowserFixtureAsset {
    /// HTTP content type served for this payload.
    pub content_type: String,
    /// Raw bytes returned by the live fixture route.
    pub bytes: Vec<u8>,
}

impl LiveBrowserFixtureAsset {
    /// Encodes one JSON payload as a fixture asset.
    pub fn json<T: Serialize>(value: &T) -> anyhow::Result<Self> {
        Ok(Self {
            content_type: "application/json".into(),
            bytes: serde_json::to_vec_pretty(value).context("encode live browser fixture json")?,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Manifest emitted for one live browser probe handoff.
pub struct LiveBrowserProbeManifest {
    pub edge_base_url: String,
    pub dataset_base_url: String,
    pub peer_dataset_root: Option<String>,
    pub peer_head_artifact_root: Option<String>,
    pub network_id: String,
    pub experiment_id: String,
    pub revision_id: String,
    pub selected_head_id: String,
    pub lease_id: String,
    pub leased_microshards: Vec<String>,
    pub release_train_hash: String,
    pub target_artifact_id: String,
    pub target_artifact_hash: String,
    pub workload_id: String,
    pub principal_id: String,
    pub browser_dataset_transport: String,
    pub browser_dataset_artifact_id: String,
    pub browser_dataset_provider_peer_id: String,
    pub browser_head_artifact_transport: String,
    pub browser_head_artifact_id: String,
    pub browser_head_artifact_provider_peer_ids: Vec<String>,
    pub shards_distributed_over_p2p: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Dataset payloads served through the live browser fixture.
pub struct LiveBrowserDatasetBundle {
    pub lease_id: LeaseId,
    pub leased_microshards: Vec<MicroShardId>,
    pub fetch_manifest: LiveBrowserFixtureAsset,
    #[serde(default)]
    pub assets: BTreeMap<String, LiveBrowserFixtureAsset>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One browser dataset transport summary.
pub struct LiveBrowserDatasetTransport {
    pub upstream_mode: String,
    pub provider_peer_id: PeerId,
    pub artifact_id: ArtifactId,
    pub bundle: LiveBrowserDatasetBundle,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One browser head-artifact transport summary.
pub struct LiveBrowserHeadArtifactTransport {
    pub upstream_mode: String,
    pub provider_peer_ids: Vec<PeerId>,
    pub descriptor: ArtifactDescriptor,
    pub bytes: Vec<u8>,
    pub run_id: RunId,
    pub publication_target_id: PublicationTargetId,
    pub published_artifact_id: PublishedArtifactId,
}

#[derive(Clone, Serialize, Deserialize)]
/// Serializable input for one native live browser edge fixture server.
pub struct LiveBrowserEdgeConfig {
    pub network_manifest: NetworkManifest,
    pub release_manifest: ClientReleaseManifest,
    pub workload_id: WorkloadId,
    pub principal_id: PrincipalId,
    pub directory_entries: Vec<ExperimentDirectoryEntry>,
    pub heads: Vec<HeadDescriptor>,
    pub leaderboard_entries: Vec<BrowserLeaderboardEntry>,
    pub metrics_catchup: Vec<MetricsCatchupBundle>,
    pub selected_head_id: HeadId,
    pub selected_experiment_id: ExperimentId,
    pub selected_revision_id: RevisionId,
    pub active_lease_id: LeaseId,
    pub leased_microshards: Vec<MicroShardId>,
    pub browser_seed_records: Vec<BrowserSeedRecord>,
    pub browser_dataset: LiveBrowserDatasetTransport,
    pub browser_head_artifact: LiveBrowserHeadArtifactTransport,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Generic browser worker handoff config for one live-participant drill.
pub struct BrowserLiveParticipantConfig {
    pub edge_base_url: String,
    pub network_id: String,
    pub selected_head_id: String,
    pub release_train_hash: String,
    pub target_artifact_id: String,
    pub target_artifact_hash: String,
    pub principal_id: String,
    pub training_plan: BrowserTrainingPlan,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Generic browser worker participation summary for one live-participant drill.
pub struct BrowserLiveParticipantResult {
    pub session_enrolled: bool,
    pub session_id: Option<String>,
    pub certificate_peer_id: Option<String>,
    pub runtime_state: Option<String>,
    pub transport: Option<String>,
    pub head_artifact_transport: Option<String>,
    pub active_assignment: bool,
    pub active_training_lease: Option<WorkloadTrainingLease>,
    pub emitted_receipt_id: Option<String>,
    pub accepted_receipt_ids: Vec<String>,
    pub receipt_submission_accepted: bool,
    pub capability: BrowserCapabilityReport,
    pub training_budget: BrowserTrainingBudget,
}

/// Live browser worker state captured between enrollment and receipt submission.
pub struct BrowserLiveParticipantHandle {
    runtime: BrowserSessionRuntimeHandle,
    config: BrowserLiveParticipantConfig,
    capability: BrowserCapabilityReport,
}

/// Starts one browser worker against the live edge fixture and leaves it ready to train.
pub async fn start_live_browser_participant(
    config: BrowserLiveParticipantConfig,
) -> anyhow::Result<BrowserLiveParticipantHandle> {
    let snapshot =
        fetch_json::<BrowserEdgeSnapshot>(&format!("{}/portal/snapshot", config.edge_base_url))
            .await
            .context("fetch live browser edge snapshot")?;
    let requested_scopes = BTreeSet::from([
        ExperimentScope::Connect,
        ExperimentScope::Train {
            experiment_id: config.training_plan.experiment_id.clone(),
        },
    ]);
    let bindings = BrowserUiBindings::from_edge_snapshot(&config.edge_base_url, &snapshot);
    let enrollment = BrowserEnrollmentConfig::from_edge_snapshot(
        &snapshot,
        if config.target_artifact_id.is_empty() {
            DEFAULT_TARGET_ARTIFACT_ID
        } else {
            &config.target_artifact_id
        },
        ContentId::new(config.target_artifact_hash.clone()),
        requested_scopes,
        900,
    )
    .context("build live browser enrollment config")?;
    let client = BrowserEdgeClient::new(bindings, enrollment);
    let peer_label = format!("browser-live-peer-{}", config.principal_id);
    let identity = BrowserWorkerIdentity {
        peer_id: PeerId::new(peer_label.clone()),
        peer_public_key_hex: format!("{peer_label}-public"),
        serial: 1,
        client_policy_hash: Some(ContentId::new(format!("{peer_label}-policy"))),
    };
    let enrollment_result = client
        .enroll_static_principal(
            Some(config.principal_id.clone()),
            PrincipalId::new(config.principal_id.clone()),
            &identity,
        )
        .await
        .context("enroll live browser participant")?;
    let mut session = burn_p2p_browser::BrowserSessionState::default();
    session.apply_enrollment(&enrollment_result);

    let capability = BrowserCapabilityReport {
        navigator_gpu_exposed: true,
        worker_gpu_exposed: true,
        gpu_support: BrowserGpuSupport::Available,
        recommended_role: BrowserRuntimeRole::BrowserTrainerWgpu,
        ..BrowserCapabilityReport::default()
    };
    let runtime = BrowserSessionRuntimeHandle::start(
        &snapshot,
        BrowserSessionRuntimeConfig {
            edge_base_url: config.edge_base_url.clone(),
            release_train_hash: ContentId::new(config.release_train_hash.clone()),
            target_artifact_id: if config.target_artifact_id.is_empty() {
                DEFAULT_TARGET_ARTIFACT_ID.into()
            } else {
                config.target_artifact_id.clone()
            },
            target_artifact_hash: ContentId::new(config.target_artifact_hash.clone()),
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            transport: burn_p2p_browser::BrowserTransportPolicy::from(
                burn_p2p::RuntimeTransportPolicy::browser_for_roles(&PeerRoleSet::new([
                    PeerRole::BrowserTrainerWgpu,
                ])),
            ),
            selected_experiment: Some(config.training_plan.experiment_id.clone()),
            selected_revision: Some(config.training_plan.revision_id.clone()),
            capability: capability.clone(),
            include_leaderboard: true,
        },
        session,
    )
    .await
    .context("sync live browser runtime from edge")?;
    #[cfg(not(target_arch = "wasm32"))]
    let runtime = {
        let mut runtime = runtime;
        if let Some(transport) = live_browser_fixture_connected_transport(&snapshot) {
            // This native fixture exercises browser worker state, receipt flush, and
            // dataset/head sync without spinning a real browser swarm runtime. Inject one
            // connected transport so the harness models the post-connect browser state that
            // real training requires.
            runtime.runtime.update_transport_status(transport);
        }
        runtime
    };

    Ok(BrowserLiveParticipantHandle {
        runtime,
        config,
        capability,
    })
}

#[cfg(not(target_arch = "wasm32"))]
async fn fetch_json<T: DeserializeOwned>(url: &str) -> anyhow::Result<T> {
    reqwest::get(url)
        .await
        .with_context(|| format!("request {url}"))?
        .error_for_status()
        .with_context(|| format!("validate response for {url}"))?
        .json::<T>()
        .await
        .with_context(|| format!("decode json response for {url}"))
}

#[cfg(not(target_arch = "wasm32"))]
fn live_browser_fixture_connected_transport(
    snapshot: &BrowserEdgeSnapshot,
) -> Option<BrowserTransportStatus> {
    let transport_kind = if snapshot.transports.wss_fallback {
        Some(BrowserTransportKind::WssFallback)
    } else if snapshot.transports.webrtc_direct {
        Some(BrowserTransportKind::WebRtcDirect)
    } else if snapshot.transports.webtransport_gateway {
        Some(BrowserTransportKind::WebTransport)
    } else {
        None
    }?;
    Some(
        BrowserTransportStatus::from_transport_surface(&snapshot.transports)
            .connected_via(transport_kind, vec![PeerId::new(EDGE_FIXTURE_LABEL)]),
    )
}

#[cfg(target_arch = "wasm32")]
async fn fetch_json<T: DeserializeOwned>(url: &str) -> anyhow::Result<T> {
    Request::get(url)
        .send()
        .await
        .with_context(|| format!("request {url}"))?
        .json::<T>()
        .await
        .with_context(|| format!("decode json response for {url}"))
}

/// Executes one training window and flushes receipts for the live participant harness.
pub async fn finish_live_browser_participant(
    handle: &mut BrowserLiveParticipantHandle,
) -> anyhow::Result<BrowserLiveParticipantResult> {
    let training_budget = handle.config.training_plan.budget.clone();
    let outcome = handle
        .runtime
        .run_training_plan(handle.config.training_plan.clone())
        .await
        .context("flush live browser worker receipts")?;

    Ok(BrowserLiveParticipantResult {
        session_enrolled: handle.runtime.session.session.is_some()
            && handle.runtime.session.certificate.is_some(),
        session_id: handle
            .runtime
            .session
            .session_id()
            .map(|session_id| session_id.as_str().to_owned()),
        certificate_peer_id: handle
            .runtime
            .session
            .peer_id()
            .map(|peer_id| peer_id.as_str().to_owned()),
        runtime_state: outcome.runtime_state.as_ref().map(|state| state.label()),
        transport: outcome
            .transport
            .as_ref()
            .map(|kind| kind.label().to_owned()),
        head_artifact_transport: handle
            .runtime
            .runtime
            .storage
            .last_head_artifact_transport
            .clone(),
        active_assignment: handle.runtime.runtime.storage.active_assignment.is_some(),
        active_training_lease: handle.runtime.runtime.storage.active_training_lease.clone(),
        emitted_receipt_id: outcome.emitted_receipt_id,
        receipt_submission_accepted: outcome.receipt_submission_accepted,
        accepted_receipt_ids: outcome.accepted_receipt_ids,
        capability: handle.capability.clone(),
        training_budget,
    })
}

#[cfg(not(target_arch = "wasm32"))]
struct LiveBrowserEdgeState {
    connector: StaticIdentityConnector,
    authority: NodeCertificateAuthority,
    sessions: BTreeMap<ContentId, PrincipalSession>,
    snapshot: BrowserEdgeSnapshot,
    signed_directory: SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>>,
    signed_leaderboard: SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>>,
    signed_seed_advertisement: SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>>,
    metrics_catchup: Vec<MetricsCatchupBundle>,
    accepted_receipts: Vec<ContributionReceipt>,
    browser_dataset: LiveBrowserDatasetTransport,
    browser_head_artifact: LiveBrowserHeadArtifactTransport,
    selected_head_id: HeadId,
    selected_head: HeadDescriptor,
}

#[cfg(not(target_arch = "wasm32"))]
/// One running HTTP fixture server that emulates a browser edge plus static dataset routes.
pub struct LiveBrowserEdgeServer {
    base_url: String,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<anyhow::Result<()>>>,
}

#[cfg(not(target_arch = "wasm32"))]
/// Materializes a generic head artifact transport from a node-local artifact store.
pub fn prepare_live_browser_head_artifact_transport<P>(
    provider: &RunningNode<P>,
    head: &HeadDescriptor,
    provider_peer_ids: Vec<PeerId>,
) -> anyhow::Result<LiveBrowserHeadArtifactTransport> {
    ensure!(
        !provider_peer_ids.is_empty(),
        "live browser head artifact transport requires at least one provider peer id"
    );
    let store = provider
        .artifact_store()
        .context("live browser head artifact transport requires provider storage")?;
    let descriptor = store.load_manifest(&head.artifact_id).with_context(|| {
        format!(
            "load live browser head manifest {}",
            head.artifact_id.as_str()
        )
    })?;
    let bytes = store
        .materialize_artifact_bytes(&descriptor)
        .with_context(|| {
            format!(
                "materialize live browser head artifact bytes {}",
                descriptor.artifact_id.as_str()
            )
        })?;
    ensure!(
        !bytes.is_empty(),
        "live browser head artifact transport requires non-empty artifact bytes"
    );
    Ok(LiveBrowserHeadArtifactTransport {
        upstream_mode: "peer-native-artifact-swarm".into(),
        provider_peer_ids,
        descriptor,
        bytes,
        run_id: RunId::new(format!(
            "{}-{}",
            head.experiment_id.as_str(),
            head.revision_id.as_str()
        )),
        publication_target_id: PublicationTargetId::new("browser-live-edge"),
        published_artifact_id: PublishedArtifactId::new(format!(
            "browser-head-{}",
            head.head_id.as_str()
        )),
    })
}

#[cfg(not(target_arch = "wasm32"))]
/// Materializes the dataset fixture payloads on disk for downstream browser-peer assertions.
pub fn materialize_live_browser_dataset_bundle(
    output_root: &Path,
    bundle: &LiveBrowserDatasetBundle,
) -> anyhow::Result<PathBuf> {
    let root = output_root.join("browser-peer-dataset");
    if root.exists() {
        fs::remove_dir_all(&root).with_context(|| format!("failed to reset {}", root.display()))?;
    }
    fs::create_dir_all(&root).with_context(|| format!("failed to create {}", root.display()))?;
    fs::write(
        root.join("fetch-manifest.json"),
        &bundle.fetch_manifest.bytes,
    )
    .with_context(|| {
        format!(
            "failed to write {}",
            root.join("fetch-manifest.json").display()
        )
    })?;
    for (relative_path, asset) in &bundle.assets {
        validate_fixture_relative_path(relative_path)?;
        let asset_path = root.join(relative_path);
        if let Some(parent) = asset_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(&asset_path, &asset.bytes)
            .with_context(|| format!("failed to write {}", asset_path.display()))?;
    }
    Ok(root)
}

#[cfg(not(target_arch = "wasm32"))]
/// Materializes the selected head artifact and its chunks on disk.
pub fn materialize_live_browser_head_artifact_bundle(
    output_root: &Path,
    transport: &LiveBrowserHeadArtifactTransport,
) -> anyhow::Result<PathBuf> {
    let root = output_root.join("browser-peer-head-artifact");
    if root.exists() {
        fs::remove_dir_all(&root).with_context(|| format!("failed to reset {}", root.display()))?;
    }
    fs::create_dir_all(&root).with_context(|| format!("failed to create {}", root.display()))?;
    let artifact_root = root.join(transport.descriptor.artifact_id.as_str());
    let chunk_root = artifact_root.join("chunks");
    fs::create_dir_all(&chunk_root)
        .with_context(|| format!("failed to create {}", chunk_root.display()))?;
    fs::write(
        artifact_root.join("descriptor.json"),
        serde_json::to_vec_pretty(&transport.descriptor)
            .context("encode browser head artifact descriptor")?,
    )
    .with_context(|| {
        format!(
            "failed to write {}",
            artifact_root.join("descriptor.json").display()
        )
    })?;
    for chunk in &transport.descriptor.chunks {
        let start = usize::try_from(chunk.offset_bytes)
            .context("convert head artifact chunk offset to usize")?;
        let length = usize::try_from(chunk.length_bytes)
            .context("convert head artifact chunk length to usize")?;
        let end = start
            .checked_add(length)
            .context("compute head artifact chunk end")?;
        ensure!(
            end <= transport.bytes.len(),
            "head artifact chunk {} exceeds materialized bytes",
            chunk.chunk_id.as_str()
        );
        let chunk_path = chunk_root.join(format!("{}.bin", chunk.chunk_id.as_str()));
        fs::write(&chunk_path, &transport.bytes[start..end])
            .with_context(|| format!("failed to write {}", chunk_path.display()))?;
    }
    Ok(root)
}

#[cfg(not(target_arch = "wasm32"))]
impl LiveBrowserEdgeServer {
    /// Starts the generic fixture server and returns the manifest consumed by browser probes.
    pub fn spawn(
        config: LiveBrowserEdgeConfig,
    ) -> anyhow::Result<(Self, LiveBrowserProbeManifest)> {
        let listener = TcpListener::bind("127.0.0.1:0").context("bind live browser edge")?;
        listener
            .set_nonblocking(true)
            .context("set live browser edge listener nonblocking")?;
        let addr = listener
            .local_addr()
            .context("read live browser edge address")?;
        let base_url = format!("http://{addr}");

        let authority = build_authority(&config)?;
        let snapshot = build_snapshot(&config, &authority);
        let signed_directory =
            signed_directory(snapshot.directory.clone(), authority.issuer_peer_id());
        let signed_leaderboard =
            signed_leaderboard(snapshot.leaderboard.clone(), authority.issuer_peer_id());
        let signed_seed_advertisement =
            signed_seed_advertisement(&config, authority.issuer_peer_id(), addr.port());
        let connector = build_connector(&config);
        let selected_head = config
            .heads
            .iter()
            .find(|head| head.head_id == config.selected_head_id)
            .cloned()
            .context("live browser edge missing selected head descriptor")?;
        let selected_head_id = config.selected_head_id.clone();
        let manifest = LiveBrowserProbeManifest {
            edge_base_url: base_url.clone(),
            dataset_base_url: format!("{base_url}{DATASET_PREFIX}"),
            peer_dataset_root: None,
            peer_head_artifact_root: None,
            network_id: config.network_manifest.network_id.as_str().into(),
            experiment_id: config.selected_experiment_id.as_str().into(),
            revision_id: config.selected_revision_id.as_str().into(),
            selected_head_id: config.selected_head_id.as_str().into(),
            lease_id: config.active_lease_id.as_str().into(),
            leased_microshards: config
                .leased_microshards
                .iter()
                .map(|microshard| microshard.as_str().to_owned())
                .collect(),
            release_train_hash: config.release_manifest.release_train_hash.as_str().into(),
            target_artifact_id: config.release_manifest.target_artifact_id.clone(),
            target_artifact_hash: config.release_manifest.target_artifact_hash.as_str().into(),
            workload_id: config.workload_id.as_str().into(),
            principal_id: config.principal_id.as_str().into(),
            browser_dataset_transport: config.browser_dataset.upstream_mode.clone(),
            browser_dataset_artifact_id: config.browser_dataset.artifact_id.as_str().into(),
            browser_dataset_provider_peer_id: config
                .browser_dataset
                .provider_peer_id
                .as_str()
                .into(),
            browser_head_artifact_transport: config.browser_head_artifact.upstream_mode.clone(),
            browser_head_artifact_id: config
                .browser_head_artifact
                .descriptor
                .artifact_id
                .as_str()
                .into(),
            browser_head_artifact_provider_peer_ids: config
                .browser_head_artifact
                .provider_peer_ids
                .iter()
                .map(|peer_id| peer_id.as_str().to_owned())
                .collect(),
            shards_distributed_over_p2p: true,
        };

        let state = Arc::new(Mutex::new(LiveBrowserEdgeState {
            connector,
            authority,
            sessions: BTreeMap::new(),
            snapshot,
            signed_directory,
            signed_leaderboard,
            signed_seed_advertisement,
            metrics_catchup: config.metrics_catchup,
            accepted_receipts: Vec::new(),
            browser_dataset: config.browser_dataset,
            browser_head_artifact: config.browser_head_artifact,
            selected_head_id,
            selected_head,
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);
        let state_for_thread = Arc::clone(&state);
        let thread =
            thread::spawn(move || run_http_server(listener, stop_for_thread, state_for_thread));

        Ok((
            Self {
                base_url,
                stop,
                thread: Some(thread),
            },
            manifest,
        ))
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for LiveBrowserEdgeServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Waits for one downstream browser probe result file to appear.
pub fn wait_for_live_browser_probe(
    output_root: &Path,
    timeout: Duration,
) -> anyhow::Result<serde_json::Value> {
    let result_path = output_root.join("browser-probe-result.json");
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if result_path.exists() {
            let bytes = fs::read(&result_path)
                .with_context(|| format!("failed to read {}", result_path.display()))?;
            return serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to decode {}", result_path.display()));
        }
        thread::sleep(Duration::from_millis(100));
    }
    anyhow::bail!(
        "timed out waiting for live browser probe result at {}",
        result_path.display()
    );
}

#[cfg(not(target_arch = "wasm32"))]
/// Writes the emitted live-browser manifest into the requested output root.
pub fn write_live_browser_manifest(
    output_root: &Path,
    manifest: &LiveBrowserProbeManifest,
) -> anyhow::Result<PathBuf> {
    let path = output_root.join("browser-live.json");
    fs::write(&path, serde_json::to_vec_pretty(manifest)?)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

#[cfg(not(target_arch = "wasm32"))]
/// Writes the live-browser fixture config into the requested output root.
pub fn write_live_browser_edge_config(
    output_root: &Path,
    config: &LiveBrowserEdgeConfig,
) -> anyhow::Result<PathBuf> {
    let path = output_root.join("browser-live-edge-config.json");
    fs::write(&path, serde_json::to_vec_pretty(config)?)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

#[cfg(not(target_arch = "wasm32"))]
/// Reads one serialized live-browser fixture config.
pub fn read_live_browser_edge_config(path: &Path) -> anyhow::Result<LiveBrowserEdgeConfig> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed to decode {}", path.display()))
}

#[cfg(not(target_arch = "wasm32"))]
fn validate_fixture_relative_path(relative_path: &str) -> anyhow::Result<()> {
    ensure!(
        !relative_path.is_empty(),
        "live browser fixture asset path must not be empty"
    );
    ensure!(
        Path::new(relative_path)
            .components()
            .all(|component| matches!(component, Component::Normal(_))),
        "live browser fixture asset path {relative_path} must stay within the dataset root"
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn build_authority(config: &LiveBrowserEdgeConfig) -> anyhow::Result<NodeCertificateAuthority> {
    NodeCertificateAuthority::new(
        config.network_manifest.network_id.clone(),
        config.release_manifest.project_family_id.clone(),
        config.release_manifest.release_train_hash.clone(),
        Version::new(0, 1, 0),
        Keypair::generate_ed25519(),
        EDGE_FIXTURE_LABEL,
    )
    .context("build live browser certificate authority")
}

#[cfg(not(target_arch = "wasm32"))]
fn build_connector(config: &LiveBrowserEdgeConfig) -> StaticIdentityConnector {
    let claims = PrincipalClaims {
        principal_id: config.principal_id.clone(),
        provider: AuthProvider::Static {
            authority: EDGE_FIXTURE_LABEL.into(),
        },
        display_name: "browser trainer".into(),
        org_memberships: BTreeSet::new(),
        group_memberships: BTreeSet::new(),
        granted_roles: PeerRoleSet::new([PeerRole::BrowserTrainerWgpu, PeerRole::BrowserObserver]),
        granted_scopes: BTreeSet::from([
            ExperimentScope::Connect,
            ExperimentScope::Train {
                experiment_id: config.selected_experiment_id.clone(),
            },
            ExperimentScope::Validate {
                experiment_id: config.selected_experiment_id.clone(),
            },
        ]),
        custom_claims: BTreeMap::new(),
        issued_at: Utc::now(),
        expires_at: Utc::now() + ChronoDuration::minutes(30),
    };

    StaticIdentityConnector::new(
        EDGE_FIXTURE_LABEL,
        ChronoDuration::minutes(30),
        BTreeMap::from([(
            config.principal_id.clone(),
            StaticPrincipalRecord {
                claims,
                allowed_networks: BTreeSet::from([config.network_manifest.network_id.clone()]),
            },
        )]),
    )
}

#[cfg(not(target_arch = "wasm32"))]
fn build_snapshot(
    config: &LiveBrowserEdgeConfig,
    authority: &NodeCertificateAuthority,
) -> BrowserEdgeSnapshot {
    let transports = live_browser_transport_surface(config);
    BrowserEdgeSnapshot {
        network_id: config.network_manifest.network_id.clone(),
        edge_mode: BrowserEdgeMode::Full,
        browser_mode: BrowserMode::Trainer,
        social_mode: SocialMode::Public,
        profile_mode: burn_p2p::ProfileMode::Public,
        transports,
        paths: BrowserEdgePaths {
            login_path: LOGIN_PATH.into(),
            callback_path: CALLBACK_PATH.into(),
            enroll_path: ENROLL_PATH.into(),
            receipt_submit_path: RECEIPTS_PATH.into(),
            ..BrowserEdgePaths::default()
        },
        auth_enabled: true,
        login_providers: vec![BrowserLoginProvider {
            label: "static".into(),
            login_path: LOGIN_PATH.into(),
            callback_path: Some(CALLBACK_PATH.into()),
            device_path: None,
        }],
        required_release_train_hash: Some(config.release_manifest.release_train_hash.clone()),
        allowed_target_artifact_hashes: BTreeSet::from([config
            .release_manifest
            .target_artifact_hash
            .clone()]),
        directory: BrowserDirectorySnapshot {
            network_id: config.network_manifest.network_id.clone(),
            generated_at: Utc::now(),
            entries: config.directory_entries.clone(),
        },
        heads: config.heads.clone(),
        leaderboard: BrowserLeaderboardSnapshot {
            network_id: config.network_manifest.network_id.clone(),
            score_version: "leaderboard_score_v1".into(),
            entries: config.leaderboard_entries.clone(),
            captured_at: Utc::now(),
        },
        trust_bundle: Some(TrustBundleExport {
            network_id: config.network_manifest.network_id.clone(),
            project_family_id: config.release_manifest.project_family_id.clone(),
            required_release_train_hash: config.release_manifest.release_train_hash.clone(),
            allowed_target_artifact_hashes: BTreeSet::from([config
                .release_manifest
                .target_artifact_hash
                .clone()]),
            minimum_revocation_epoch: RevocationEpoch(0),
            active_issuer_peer_id: authority.issuer_peer_id(),
            issuers: vec![TrustedIssuerStatus {
                issuer_peer_id: authority.issuer_peer_id(),
                issuer_public_key_hex: authority.issuer_public_key_hex().into(),
                active_for_new_certificates: true,
                accepted_for_admission: true,
            }],
            reenrollment: None,
        }),
        captured_at: Utc::now(),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn signed_directory(
    snapshot: BrowserDirectorySnapshot,
    signer: PeerId,
) -> SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_directory_snapshot",
            Version::new(0, 1, 0),
            snapshot,
        ),
        SignatureMetadata {
            signer,
            key_id: EDGE_FIXTURE_LABEL.into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: EDGE_FIXTURE_LABEL.into(),
        },
    )
    .expect("live browser signed directory should be serializable")
}

#[cfg(not(target_arch = "wasm32"))]
fn signed_leaderboard(
    snapshot: BrowserLeaderboardSnapshot,
    signer: PeerId,
) -> SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>> {
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_leaderboard_snapshot",
            Version::new(0, 1, 0),
            snapshot,
        ),
        SignatureMetadata {
            signer,
            key_id: EDGE_FIXTURE_LABEL.into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: EDGE_FIXTURE_LABEL.into(),
        },
    )
    .expect("live browser signed leaderboard should be serializable")
}

#[cfg(not(target_arch = "wasm32"))]
fn signed_seed_advertisement(
    config: &LiveBrowserEdgeConfig,
    signer: PeerId,
    port: u16,
) -> SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>> {
    let mut seeds = config.browser_seed_records.clone();
    seeds.push(BrowserSeedRecord {
        peer_id: Some(PeerId::new("browser-edge-fixture-seed")),
        multiaddrs: vec![format!("/ip4/127.0.0.1/tcp/{port}/wss")],
    });
    let surface = live_browser_transport_surface(config);
    SignedPayload::new(
        SchemaEnvelope::new(
            "burn_p2p.browser_seed_advertisement",
            Version::new(0, 1, 0),
            BrowserSeedAdvertisement {
                schema_version: u32::from(burn_p2p_core::SCHEMA_VERSION),
                network_id: config.network_manifest.network_id.clone(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + ChronoDuration::minutes(10),
                transport_policy: live_browser_seed_transport_policy(&surface),
                seeds,
            },
        ),
        SignatureMetadata {
            signer,
            key_id: EDGE_FIXTURE_LABEL.into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: EDGE_FIXTURE_LABEL.into(),
        },
    )
    .expect("live browser signed seed advertisement should be serializable")
}

#[cfg(not(target_arch = "wasm32"))]
fn live_browser_transport_surface(config: &LiveBrowserEdgeConfig) -> BrowserTransportSurface {
    let mut surface = BrowserTransportSurface {
        webrtc_direct: false,
        webtransport_gateway: false,
        wss_fallback: true,
    };
    for seed in &config.browser_seed_records {
        for multiaddr in &seed.multiaddrs {
            match live_browser_seed_transport_kind(multiaddr) {
                Some(BrowserSeedTransportKind::WebRtcDirect) => {
                    surface.webrtc_direct = true;
                }
                Some(BrowserSeedTransportKind::WebTransport) => {
                    surface.webtransport_gateway = true;
                }
                Some(BrowserSeedTransportKind::WssFallback) => {
                    surface.wss_fallback = true;
                }
                None => {}
            }
        }
    }
    surface
}

#[cfg(not(target_arch = "wasm32"))]
fn live_browser_seed_transport_policy(
    surface: &BrowserTransportSurface,
) -> BrowserSeedTransportPolicy {
    let mut preferred = Vec::new();
    if surface.webrtc_direct {
        preferred.push(BrowserSeedTransportKind::WebRtcDirect);
    }
    if surface.webtransport_gateway {
        preferred.push(BrowserSeedTransportKind::WebTransport);
    }
    if surface.wss_fallback {
        preferred.push(BrowserSeedTransportKind::WssFallback);
    }
    BrowserSeedTransportPolicy {
        preferred,
        allow_fallback_wss: surface.wss_fallback,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn live_browser_seed_transport_kind(multiaddr: &str) -> Option<BrowserSeedTransportKind> {
    let segments = multiaddr
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.contains(&"webrtc-direct") && segments.contains(&"certhash") {
        return Some(BrowserSeedTransportKind::WebRtcDirect);
    }
    if segments.contains(&"webtransport")
        && segments.contains(&"quic-v1")
        && segments.contains(&"certhash")
    {
        return Some(BrowserSeedTransportKind::WebTransport);
    }
    if segments.contains(&"wss") || segments.contains(&"ws") {
        return Some(BrowserSeedTransportKind::WssFallback);
    }
    None
}

#[cfg(not(target_arch = "wasm32"))]
fn published_head_artifact_record(state: &LiveBrowserEdgeState) -> PublishedArtifactRecord {
    PublishedArtifactRecord {
        published_artifact_id: state.browser_head_artifact.published_artifact_id.clone(),
        artifact_alias_id: None,
        experiment_id: state.selected_head.experiment_id.clone(),
        run_id: Some(state.browser_head_artifact.run_id.clone()),
        head_id: state.selected_head.head_id.clone(),
        artifact_profile: burn_p2p_core::ArtifactProfile::BrowserSnapshot,
        publication_target_id: state.browser_head_artifact.publication_target_id.clone(),
        object_key: format!(
            "browser-heads/{}",
            state.browser_head_artifact.descriptor.artifact_id.as_str()
        ),
        content_hash: state.browser_head_artifact.descriptor.root_hash.clone(),
        content_length: state.browser_head_artifact.descriptor.bytes_len,
        created_at: Utc::now(),
        expires_at: None,
        status: PublishedArtifactStatus::Ready,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn live_head_artifact_view(state: &LiveBrowserEdgeState) -> HeadArtifactView {
    HeadArtifactView {
        head: state.selected_head.clone(),
        run_id: state.browser_head_artifact.run_id.clone(),
        eval_reports: Vec::new(),
        aliases: Vec::new(),
        published_artifacts: vec![published_head_artifact_record(state)],
        available_profiles: BTreeSet::from([
            burn_p2p_core::ArtifactProfile::BrowserSnapshot,
            burn_p2p_core::ArtifactProfile::ManifestOnly,
        ]),
        alias_history: Vec::new(),
        provider_peer_ids: state.browser_head_artifact.provider_peer_ids.clone(),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn live_head_artifact_download_ticket(
    state: &LiveBrowserEdgeState,
    principal_id: PrincipalId,
) -> DownloadTicketResponse {
    let ticket_id = DownloadTicketId::new(format!(
        "browser-ticket-{}",
        state.selected_head_id.as_str()
    ));
    DownloadTicketResponse {
        ticket: DownloadTicket {
            download_ticket_id: ticket_id.clone(),
            published_artifact_id: state.browser_head_artifact.published_artifact_id.clone(),
            principal_id,
            issued_at: Utc::now(),
            expires_at: Utc::now() + ChronoDuration::minutes(5),
            delivery_mode: DownloadDeliveryMode::EdgeStream,
        },
        published_artifact: published_head_artifact_record(state),
        download_path: format!("/artifacts/download/{}", ticket_id.as_str()),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn run_http_server(
    listener: TcpListener,
    stop: Arc<AtomicBool>,
    state: Arc<Mutex<LiveBrowserEdgeState>>,
) -> anyhow::Result<()> {
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                if let Err(error) = handle_connection(stream, &state) {
                    eprintln!("live browser edge request failed: {error:#}");
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error).context("accept live browser edge request"),
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn handle_connection(
    mut stream: TcpStream,
    state: &Arc<Mutex<LiveBrowserEdgeState>>,
) -> anyhow::Result<()> {
    let request = read_request(&stream)?;
    let path = request.path.split('?').next().unwrap_or(&request.path);
    match (request.method.as_str(), path) {
        ("OPTIONS", _) => write_preflight_response(&mut stream),
        ("GET", PORTAL_SNAPSHOT_PATH) => {
            let snapshot = state.lock().expect("live browser state").snapshot.clone();
            write_json_response(&mut stream, 200, &snapshot)
        }
        ("GET", DATASET_MANIFEST_PATH) => {
            let asset = state
                .lock()
                .expect("live browser state")
                .browser_dataset
                .bundle
                .fetch_manifest
                .clone();
            write_binary_response(&mut stream, 200, &asset.content_type, &asset.bytes)
        }
        ("GET", path) if path.starts_with(&format!("{DATASET_PREFIX}/")) => {
            let relative_path = path
                .strip_prefix(&format!("{DATASET_PREFIX}/"))
                .unwrap_or_default();
            let asset = state
                .lock()
                .expect("live browser state")
                .browser_dataset
                .bundle
                .assets
                .get(relative_path)
                .cloned();
            match asset {
                Some(asset) => {
                    write_binary_response(&mut stream, 200, &asset.content_type, &asset.bytes)
                }
                None => write_empty_response(&mut stream, 404),
            }
        }
        ("GET", DIRECTORY_PATH) => {
            let directory = state
                .lock()
                .expect("live browser state")
                .snapshot
                .directory
                .entries
                .clone();
            write_json_response(&mut stream, 200, &directory)
        }
        ("GET", SIGNED_DIRECTORY_PATH) => {
            let signed = state
                .lock()
                .expect("live browser state")
                .signed_directory
                .clone();
            write_json_response(&mut stream, 200, &signed)
        }
        ("GET", SIGNED_BROWSER_SEEDS_PATH) => {
            let signed = state
                .lock()
                .expect("live browser state")
                .signed_seed_advertisement
                .clone();
            write_json_response(&mut stream, 200, &signed)
        }
        ("GET", HEADS_PATH) => {
            let heads = state
                .lock()
                .expect("live browser state")
                .snapshot
                .heads
                .clone();
            write_json_response(&mut stream, 200, &heads)
        }
        ("GET", path) if path.starts_with("/artifacts/heads/") => {
            let head_id = path.strip_prefix("/artifacts/heads/").unwrap_or_default();
            let state = state.lock().expect("live browser state");
            if head_id != state.selected_head_id.as_str() {
                return write_empty_response(&mut stream, 404);
            }
            write_json_response(&mut stream, 200, &live_head_artifact_view(&state))
        }
        ("GET", LEADERBOARD_PATH) => {
            let leaderboard = state
                .lock()
                .expect("live browser state")
                .snapshot
                .leaderboard
                .clone();
            write_json_response(&mut stream, 200, &leaderboard)
        }
        ("GET", SIGNED_LEADERBOARD_PATH) => {
            let signed = state
                .lock()
                .expect("live browser state")
                .signed_leaderboard
                .clone();
            write_json_response(&mut stream, 200, &signed)
        }
        ("GET", TRUST_PATH) => {
            let trust_bundle = state
                .lock()
                .expect("live browser state")
                .snapshot
                .trust_bundle
                .clone()
                .context("live browser edge snapshot missing trust bundle")?;
            write_json_response(&mut stream, 200, &trust_bundle)
        }
        ("GET", METRICS_LIVE_LATEST_PATH) => write_empty_response(&mut stream, 404),
        ("GET", path) if path == METRICS_CATCHUP_PATH || path.starts_with("/metrics/catchup/") => {
            let experiment_filter = path
                .strip_prefix("/metrics/catchup/")
                .map(str::to_owned)
                .filter(|value| !value.is_empty());
            let bundles = {
                let state = state.lock().expect("live browser state");
                state
                    .metrics_catchup
                    .iter()
                    .filter(|bundle| {
                        experiment_filter
                            .as_ref()
                            .map(|experiment_id| bundle.experiment_id.as_str() == experiment_id)
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            };
            write_json_response(&mut stream, 200, &bundles)
        }
        ("POST", LOGIN_PATH) => {
            let request_body: burn_p2p::LoginRequest = parse_json_body(&request.body)?;
            let login = state
                .lock()
                .expect("live browser state")
                .connector
                .begin_login(request_body)
                .context("begin live browser login")?;
            write_json_response(&mut stream, 200, &login)
        }
        ("POST", CALLBACK_PATH) => {
            let callback: burn_p2p::CallbackPayload = parse_json_body(&request.body)?;
            let mut state = state.lock().expect("live browser state");
            let session = state
                .connector
                .complete_login(callback)
                .context("complete live browser login")?;
            state
                .sessions
                .insert(session.session_id.clone(), session.clone());
            write_json_response(&mut stream, 200, &session)
        }
        ("POST", ENROLL_PATH) => {
            let enrollment: BrowserPeerEnrollmentRequest = parse_json_body(&request.body)?;
            let state = state.lock().expect("live browser state");
            let session = state
                .sessions
                .get(&enrollment.session_id)
                .cloned()
                .with_context(|| {
                    format!(
                        "live browser edge missing session {} for enrollment",
                        enrollment.session_id.as_str()
                    )
                })?;
            let certificate = state
                .authority
                .issue_certificate(NodeEnrollmentRequest {
                    session,
                    project_family_id: state.snapshot.trust_bundle.as_ref().map_or_else(
                        || ProjectFamilyId::new("unknown"),
                        |bundle| bundle.project_family_id.clone(),
                    ),
                    release_train_hash: enrollment.release_train_hash,
                    target_artifact_hash: enrollment.target_artifact_hash,
                    peer_id: enrollment.peer_id,
                    peer_public_key_hex: enrollment.peer_public_key_hex,
                    granted_roles: PeerRoleSet::new([
                        PeerRole::BrowserTrainerWgpu,
                        PeerRole::BrowserObserver,
                    ]),
                    requested_scopes: enrollment.requested_scopes,
                    client_policy_hash: enrollment.client_policy_hash,
                    serial: enrollment.serial,
                    not_before: Utc::now(),
                    not_after: Utc::now() + ChronoDuration::minutes(30),
                    revocation_epoch: RevocationEpoch(0),
                })
                .context("issue live browser certificate")?;
            write_json_response(&mut stream, 200, &certificate)
        }
        ("POST", "/artifacts/download-ticket") => {
            let request_body: DownloadTicketRequest = parse_json_body(&request.body)?;
            let session_id = request
                .headers
                .get("x-session-id")
                .cloned()
                .context("browser artifact ticket request missing x-session-id header")?;
            let session_id = ContentId::new(session_id);
            let state = state.lock().expect("live browser state");
            let session = state.sessions.get(&session_id).with_context(|| {
                format!(
                    "live browser edge missing session {} for artifact download",
                    session_id.as_str()
                )
            })?;
            ensure!(
                request_body.head_id == state.selected_head_id,
                "live browser edge only serves the selected head artifact"
            );
            write_json_response(
                &mut stream,
                200,
                &live_head_artifact_download_ticket(&state, session.claims.principal_id.clone()),
            )
        }
        ("GET", path) if path.starts_with("/artifacts/download/") => {
            let ticket_id = path
                .strip_prefix("/artifacts/download/")
                .unwrap_or_default();
            let state = state.lock().expect("live browser state");
            let expected_ticket_id = format!("browser-ticket-{}", state.selected_head_id.as_str());
            if ticket_id != expected_ticket_id {
                return write_empty_response(&mut stream, 404);
            }
            write_binary_response(
                &mut stream,
                200,
                "application/octet-stream",
                &state.browser_head_artifact.bytes,
            )
        }
        ("POST", RECEIPTS_PATH) => {
            let session_id = request
                .headers
                .get("x-session-id")
                .cloned()
                .context("browser receipt submission missing x-session-id header")?;
            let session_id = ContentId::new(session_id);
            let receipts: Vec<ContributionReceipt> = parse_json_body(&request.body)?;
            let mut state = state.lock().expect("live browser state");
            ensure!(
                state.sessions.contains_key(&session_id),
                "live browser edge rejected receipts for unknown session {}",
                session_id.as_str()
            );
            let accepted_ids = receipts
                .iter()
                .map(|receipt| receipt.receipt_id.clone())
                .collect::<Vec<_>>();
            state.accepted_receipts.extend(receipts);
            write_json_response(
                &mut stream,
                200,
                &BrowserReceiptSubmissionResponse {
                    accepted_receipt_ids: accepted_ids,
                    pending_receipt_count: 0,
                },
            )
        }
        _ => write_empty_response(&mut stream, 404),
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct HttpRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

#[cfg(not(target_arch = "wasm32"))]
fn read_request(stream: &TcpStream) -> anyhow::Result<HttpRequest> {
    let mut reader = BufReader::new(stream.try_clone().context("clone live browser stream")?);
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .context("read live browser request line")?;
    ensure!(
        !request_line.trim().is_empty(),
        "empty live browser request"
    );
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .context("live browser request missing method")?
        .to_owned();
    let path = request_parts
        .next()
        .context("live browser request missing path")?
        .to_owned();

    let mut headers = BTreeMap::new();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .context("read live browser request header")?;
        if line == "\r\n" || line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let value = value.trim().to_owned();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse::<usize>().unwrap_or(0);
            }
            headers.insert(name.trim().to_ascii_lowercase(), value);
        }
    }

    let mut body = vec![0; content_length];
    if content_length > 0 {
        reader
            .read_exact(&mut body)
            .context("read live browser request body")?;
    }

    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn parse_json_body<T: DeserializeOwned>(body: &[u8]) -> anyhow::Result<T> {
    serde_json::from_slice(body).context("decode live browser json body")
}

#[cfg(not(target_arch = "wasm32"))]
fn write_json_response(
    stream: &mut TcpStream,
    status: u16,
    value: &impl Serialize,
) -> anyhow::Result<()> {
    let body = serde_json::to_vec(value).context("encode live browser json response")?;
    write_response(stream, status, "application/json", &body)
}

#[cfg(not(target_arch = "wasm32"))]
fn write_empty_response(stream: &mut TcpStream, status: u16) -> anyhow::Result<()> {
    write_response(stream, status, "text/plain; charset=utf-8", &[])
}

#[cfg(not(target_arch = "wasm32"))]
fn write_binary_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> anyhow::Result<()> {
    write_response(stream, status, content_type, body)
}

#[cfg(not(target_arch = "wasm32"))]
fn write_preflight_response(stream: &mut TcpStream) -> anyhow::Result<()> {
    write_response(stream, 200, "text/plain; charset=utf-8", &[])
}

#[cfg(not(target_arch = "wasm32"))]
fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> anyhow::Result<()> {
    let status_text = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Error",
    };
    stream
        .write_all(
            format!(
                "HTTP/1.1 {status} {status_text}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\naccess-control-allow-origin: *\r\naccess-control-allow-methods: GET, POST, OPTIONS\r\naccess-control-allow-headers: content-type, x-session-id\r\naccess-control-max-age: 86400\r\nconnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .context("write live browser response head")?;
    if !body.is_empty() {
        stream
            .write_all(body)
            .context("write live browser response body")?;
    }
    stream.flush().context("flush live browser response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn_p2p::{
        ArtifactKind, ChunkId, ClientPlatform, DatasetViewId, ExperimentOptInPolicy,
        ExperimentResourceRequirements, ExperimentVisibility, MetricValue, NetworkId, Precision,
        ShardFetchManifest, StudyId, SupportedWorkload,
    };
    use burn_p2p_core::ChunkDescriptor;
    use tempfile::tempdir;

    #[test]
    fn live_browser_fixture_asset_json_round_trips() {
        let asset = LiveBrowserFixtureAsset::json(&serde_json::json!({"ok": true}))
            .expect("encode fixture asset");
        let value: serde_json::Value =
            serde_json::from_slice(&asset.bytes).expect("decode fixture asset bytes");
        assert_eq!(asset.content_type, "application/json");
        assert_eq!(value["ok"], serde_json::json!(true));
    }

    #[test]
    fn live_browser_dataset_bundle_materializes_assets() {
        let temp = tempdir().expect("tempdir");
        let bundle = LiveBrowserDatasetBundle {
            lease_id: LeaseId::new("lease"),
            leased_microshards: vec![MicroShardId::new("micro-a")],
            fetch_manifest: LiveBrowserFixtureAsset::json(&ShardFetchManifest {
                dataset_view_id: DatasetViewId::new("view"),
                entries: Vec::new(),
            })
            .expect("encode manifest"),
            assets: BTreeMap::from([(
                "eval-records.json".into(),
                LiveBrowserFixtureAsset::json(&vec![serde_json::json!({"digit": 0})])
                    .expect("encode eval asset"),
            )]),
        };
        let root =
            materialize_live_browser_dataset_bundle(temp.path(), &bundle).expect("materialize");
        assert!(root.join("fetch-manifest.json").exists());
        assert!(root.join("eval-records.json").exists());
    }

    #[test]
    fn live_browser_edge_config_round_trips() {
        let temp = tempdir().expect("tempdir");
        let config = fixture_config();
        let path = write_live_browser_edge_config(temp.path(), &config).expect("write config");
        let round_trip = read_live_browser_edge_config(&path).expect("read config");
        assert_eq!(round_trip.selected_head_id, config.selected_head_id);
        assert_eq!(round_trip.principal_id, config.principal_id);
    }

    #[tokio::test]
    async fn live_browser_fixture_server_and_participant_harness_work() {
        let config = fixture_config();
        let (server, manifest) = LiveBrowserEdgeServer::spawn(config.clone()).expect("spawn");
        let snapshot = reqwest::get(format!("{}/portal/snapshot", server.base_url()))
            .await
            .expect("fetch snapshot")
            .error_for_status()
            .expect("snapshot status")
            .json::<BrowserEdgeSnapshot>()
            .await
            .expect("decode snapshot");
        assert_eq!(
            snapshot.network_id.as_str(),
            config.network_manifest.network_id.as_str()
        );
        let directory = reqwest::get(format!("{}/directory", server.base_url()))
            .await
            .expect("fetch directory")
            .error_for_status()
            .expect("directory status")
            .json::<Vec<ExperimentDirectoryEntry>>()
            .await
            .expect("decode directory");
        assert_eq!(directory.len(), 1);
        let dataset_asset =
            reqwest::get(format!("{}/dataset/eval-records.json", server.base_url()))
                .await
                .expect("fetch dataset asset")
                .error_for_status()
                .expect("dataset asset status")
                .bytes()
                .await
                .expect("read dataset asset");
        assert!(!dataset_asset.is_empty());

        let mut handle = start_live_browser_participant(BrowserLiveParticipantConfig {
            edge_base_url: manifest.edge_base_url.clone(),
            network_id: manifest.network_id.clone(),
            selected_head_id: manifest.selected_head_id.clone(),
            release_train_hash: manifest.release_train_hash.clone(),
            target_artifact_id: manifest.target_artifact_id.clone(),
            target_artifact_hash: manifest.target_artifact_hash.clone(),
            principal_id: manifest.principal_id.clone(),
            training_plan: BrowserTrainingPlan {
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new(manifest.experiment_id.clone()),
                revision_id: RevisionId::new(manifest.revision_id.clone()),
                workload_id: WorkloadId::new(manifest.workload_id.clone()),
                budget: BrowserTrainingBudget::default(),
                lease: Some(WorkloadTrainingLease {
                    lease_id: LeaseId::new(manifest.lease_id.clone()),
                    window_id: burn_p2p::WindowId(7),
                    dataset_view_id: DatasetViewId::new("view"),
                    assignment_hash: ContentId::new("assign"),
                    microshards: vec![MicroShardId::new("micro-a")],
                }),
            },
        })
        .await
        .expect("start participant");
        let result = finish_live_browser_participant(&mut handle)
            .await
            .expect("finish participant");
        assert!(result.session_enrolled);
        assert!(matches!(
            result.capability.gpu_support,
            BrowserGpuSupport::Available
        ));
        assert!(result.training_budget.requires_webgpu);
        drop(server);
    }

    fn fixture_config() -> LiveBrowserEdgeConfig {
        let network_manifest = NetworkManifest {
            network_id: NetworkId::new("network"),
            project_family_id: burn_p2p::ProjectFamilyId::new("family"),
            protocol_major: 0,
            required_release_train_hash: ContentId::new("train-hash"),
            allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact-hash")]),
            authority_public_keys: Vec::new(),
            bootstrap_addrs: vec!["/ip4/127.0.0.1/tcp/7001".into()],
            auth_policy_hash: ContentId::new("auth-policy"),
            created_at: Utc::now(),
            description: "fixture".into(),
        };
        let supported_workload = SupportedWorkload {
            workload_id: WorkloadId::new("workload"),
            workload_name: "fixture".into(),
            model_program_hash: ContentId::new("model"),
            checkpoint_format_hash: ContentId::new("checkpoint"),
            supported_revision_family: ContentId::new("revision-family"),
            resource_class: "standard".into(),
        };
        let release_manifest = ClientReleaseManifest {
            project_family_id: network_manifest.project_family_id.clone(),
            release_train_hash: network_manifest.required_release_train_hash.clone(),
            target_artifact_id: DEFAULT_TARGET_ARTIFACT_ID.into(),
            target_artifact_hash: ContentId::new("artifact-hash"),
            target_platform: ClientPlatform::Browser,
            app_semver: Version::new(0, 21, 0),
            git_commit: "fixture".into(),
            cargo_lock_hash: ContentId::new("lock"),
            burn_version_string: "0.21.0-pre.3".into(),
            enabled_features_hash: ContentId::new("features"),
            protocol_major: 0,
            supported_workloads: vec![supported_workload.clone()],
            built_at: Utc::now(),
        };
        let study_id = StudyId::new("study");
        let experiment_id = ExperimentId::new("experiment");
        let revision_id = RevisionId::new("revision");
        let head_id = HeadId::new("head");
        let directory_entry = ExperimentDirectoryEntry {
            network_id: network_manifest.network_id.clone(),
            study_id: study_id.clone(),
            experiment_id: experiment_id.clone(),
            workload_id: supported_workload.workload_id.clone(),
            display_name: "Fixture".into(),
            model_schema_hash: ContentId::new("model"),
            dataset_view_id: DatasetViewId::new("view"),
            resource_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::from([PeerRole::BrowserTrainerWgpu]),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: None,
                estimated_download_bytes: 1024,
                estimated_window_seconds: 5,
            },
            visibility: ExperimentVisibility::Public,
            opt_in_policy: ExperimentOptInPolicy::Open,
            current_revision_id: revision_id.clone(),
            current_head_id: Some(head_id.clone()),
            allowed_roles: PeerRoleSet::new([PeerRole::BrowserTrainerWgpu]),
            allowed_scopes: BTreeSet::from([
                ExperimentScope::Connect,
                ExperimentScope::Train {
                    experiment_id: experiment_id.clone(),
                },
            ]),
            metadata: BTreeMap::from([
                (
                    "burn_p2p.revision.browser.enabled".into(),
                    String::from("true"),
                ),
                (
                    "burn_p2p.revision.browser.visibility_policy".into(),
                    String::from("swarm-eligible"),
                ),
                (
                    "burn_p2p.revision.browser.role.trainer_wgpu".into(),
                    String::from("true"),
                ),
            ]),
        };
        let head = HeadDescriptor {
            head_id: head_id.clone(),
            study_id: study_id.clone(),
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            artifact_id: ArtifactId::new("artifact"),
            parent_head_id: None,
            global_step: 1,
            created_at: Utc::now(),
            metrics: BTreeMap::from([("accuracy".into(), MetricValue::Float(0.5))]),
        };
        let descriptor = ArtifactDescriptor {
            artifact_id: ArtifactId::new("artifact"),
            kind: ArtifactKind::FullHead,
            head_id: Some(head_id.clone()),
            base_head_id: None,
            precision: Precision::Fp32,
            model_schema_hash: ContentId::new("model"),
            record_format: "fixture".into(),
            bytes_len: 4,
            chunks: vec![ChunkDescriptor {
                chunk_id: ChunkId::new("chunk"),
                offset_bytes: 0,
                length_bytes: 4,
                chunk_hash: ContentId::new("chunk-hash"),
            }],
            root_hash: ContentId::new("root-hash"),
        };
        LiveBrowserEdgeConfig {
            network_manifest,
            release_manifest,
            workload_id: supported_workload.workload_id,
            principal_id: PrincipalId::new("fixture-principal"),
            directory_entries: vec![directory_entry],
            heads: vec![head.clone()],
            leaderboard_entries: Vec::new(),
            metrics_catchup: Vec::new(),
            selected_head_id: head_id,
            selected_experiment_id: experiment_id,
            selected_revision_id: revision_id,
            active_lease_id: LeaseId::new("lease"),
            leased_microshards: vec![MicroShardId::new("micro-a")],
            browser_seed_records: Vec::new(),
            browser_dataset: LiveBrowserDatasetTransport {
                upstream_mode: "fixture-dataset".into(),
                provider_peer_id: PeerId::new("peer-a"),
                artifact_id: ArtifactId::new("dataset-artifact"),
                bundle: LiveBrowserDatasetBundle {
                    lease_id: LeaseId::new("lease"),
                    leased_microshards: vec![MicroShardId::new("micro-a")],
                    fetch_manifest: LiveBrowserFixtureAsset::json(&ShardFetchManifest {
                        dataset_view_id: DatasetViewId::new("view"),
                        entries: Vec::new(),
                    })
                    .expect("encode manifest"),
                    assets: BTreeMap::from([(
                        "eval-records.json".into(),
                        LiveBrowserFixtureAsset::json(&vec![serde_json::json!({"digit": 0})])
                            .expect("encode asset"),
                    )]),
                },
            },
            browser_head_artifact: LiveBrowserHeadArtifactTransport {
                upstream_mode: "fixture-head".into(),
                provider_peer_ids: vec![PeerId::new("peer-a")],
                descriptor,
                bytes: vec![1, 2, 3, 4],
                run_id: RunId::new("run"),
                publication_target_id: PublicationTargetId::new("target"),
                published_artifact_id: PublishedArtifactId::new("published"),
            },
        }
    }
}
