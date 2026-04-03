use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use burn_p2p::{
    ActiveServiceSet, AdminMode, ArtifactBuildSpec, ArtifactDescriptor, ArtifactKind,
    AssignmentLease, BrowserMode, CachedMicroShard, CapabilityEstimate, ChunkingScheme,
    CompiledFeatureSet, ConfiguredServiceSet, ControlAnnouncement, ControlHandle, DatasetConfig,
    DatasetRegistration, DatasetSizing, EdgeAuthProvider, EdgeFeature, EdgeServiceManifest,
    EvalSplit, ExperimentDirectory, ExperimentDirectoryAnnouncement, ExperimentDirectoryEntry,
    FsArtifactStore, IdentityConnector, LoginRequest, MetricReport, MetricValue, MetricsMode,
    NodeCertificateAuthority, NodeConfig, NodeEnrollmentRequest, OverlayTopic, P2pProject,
    PatchOutcome, PatchSupport, PortalMode, PrincipalClaims, PrincipalId, PrincipalSession,
    ProfileMode, ProjectBackend, RuntimePatch, RuntimeProject, ShardFetchManifest, SocialMode,
    StaticIdentityConnector, StaticPrincipalRecord, TrainError, TrustedIssuer, WindowCtx,
    WindowReport,
};
#[cfg(feature = "auth-external")]
use burn_p2p_auth_external::ExternalProxyIdentityConnector;
#[cfg(feature = "auth-github")]
use burn_p2p_auth_github::GitHubIdentityConnector;
#[cfg(feature = "auth-oauth")]
use burn_p2p_auth_oauth::OAuthIdentityConnector;
#[cfg(feature = "auth-oidc")]
use burn_p2p_auth_oidc::OidcIdentityConnector;
use burn_p2p_bootstrap::{
    AdminAction, AdminCapability, AuthPolicyRollout, BootstrapAdminState, BootstrapEmbeddedDaemon,
    BootstrapEmbeddedDaemonConfig, BootstrapPlan, BootstrapSpec, BrowserDirectorySnapshot,
    BrowserEdgeMode, BrowserLoginProvider, BrowserPortalSnapshotConfig,
    BrowserReceiptSubmissionResponse, BrowserTransportSurface, ReceiptQuery, ReenrollmentStatus,
    TrustBundleExport, TrustedIssuerStatus, render_browser_portal_html, render_dashboard_html,
    render_openmetrics,
};
use burn_p2p_core::{
    AuthProvider, ContentId, ExperimentScope, NetworkId, PeerId, PeerRoleSet, RevocationEpoch,
    SchemaEnvelope, SignatureAlgorithm, SignatureMetadata, SignedPayload,
};
use chrono::{DateTime, Utc};
use libp2p_identity::Keypair;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BootstrapDaemonConfig {
    pub spec: BootstrapSpec,
    pub http_bind_addr: Option<String>,
    pub admin_token: Option<String>,
    #[serde(default)]
    pub allow_dev_admin_token: bool,
    #[serde(default)]
    pub optional_services: BootstrapOptionalServicesConfig,
    pub remaining_work_units: Option<u64>,
    pub admin_signer_peer_id: Option<PeerId>,
    pub embedded_runtime: Option<BootstrapEmbeddedDaemonConfig>,
    pub auth: Option<BootstrapAuthConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct BootstrapOptionalServicesConfig {
    #[serde(default = "default_true")]
    pub portal_enabled: bool,
    #[serde(default = "default_browser_mode")]
    pub browser_mode: BrowserMode,
    #[serde(default = "default_social_mode")]
    pub social_mode: SocialMode,
    #[serde(default = "default_profile_mode")]
    pub profile_mode: ProfileMode,
}

impl Default for BootstrapOptionalServicesConfig {
    fn default() -> Self {
        Self {
            portal_enabled: true,
            browser_mode: default_browser_mode(),
            social_mode: default_social_mode(),
            profile_mode: ProfileMode::Disabled,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BootstrapAuthConfig {
    pub authority_name: String,
    #[serde(default)]
    pub connector: BootstrapAuthConnectorConfig,
    pub authority_key_path: PathBuf,
    #[serde(default = "default_issuer_key_id")]
    pub issuer_key_id: String,
    pub project_family_id: burn_p2p::ProjectFamilyId,
    pub required_release_train_hash: ContentId,
    #[serde(default)]
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
    pub session_ttl_seconds: i64,
    pub minimum_revocation_epoch: u64,
    pub principals: Vec<BootstrapAuthPrincipal>,
    pub directory_entries: Vec<ExperimentDirectoryEntry>,
    #[serde(default)]
    pub trusted_issuers: Vec<TrustedIssuer>,
    #[serde(default)]
    pub reenrollment: Option<BootstrapReenrollmentConfig>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BootstrapAuthConnectorConfig {
    #[default]
    Static,
    #[serde(rename = "github")]
    GitHub {
        #[serde(default)]
        authorize_base_url: Option<String>,
        #[serde(default)]
        exchange_url: Option<String>,
        #[serde(default)]
        token_url: Option<String>,
        #[serde(default)]
        client_id: Option<String>,
        #[serde(default)]
        client_secret: Option<String>,
        #[serde(default)]
        userinfo_url: Option<String>,
        #[serde(default)]
        refresh_url: Option<String>,
        #[serde(default)]
        revoke_url: Option<String>,
    },
    Oidc {
        issuer: String,
        #[serde(default)]
        authorize_base_url: Option<String>,
        #[serde(default)]
        exchange_url: Option<String>,
        #[serde(default)]
        token_url: Option<String>,
        #[serde(default)]
        client_id: Option<String>,
        #[serde(default)]
        client_secret: Option<String>,
        #[serde(default)]
        userinfo_url: Option<String>,
        #[serde(default)]
        refresh_url: Option<String>,
        #[serde(default)]
        revoke_url: Option<String>,
    },
    #[serde(rename = "oauth")]
    OAuth {
        provider: String,
        #[serde(default)]
        authorize_base_url: Option<String>,
        #[serde(default)]
        exchange_url: Option<String>,
        #[serde(default)]
        token_url: Option<String>,
        #[serde(default)]
        client_id: Option<String>,
        #[serde(default)]
        client_secret: Option<String>,
        #[serde(default)]
        userinfo_url: Option<String>,
        #[serde(default)]
        refresh_url: Option<String>,
        #[serde(default)]
        revoke_url: Option<String>,
    },
    External {
        authority: String,
        #[serde(default = "default_external_principal_header")]
        trusted_principal_header: String,
        trusted_internal_only: bool,
    },
}

#[derive(Debug, thiserror::Error)]
enum BootstrapCompositionError {
    #[error("service `{service}` requires compiled feature `{feature}`")]
    MissingCompiledFeature {
        service: &'static str,
        feature: &'static str,
    },
    #[error("invalid optional service configuration: {0}")]
    InvalidServiceConfig(&'static str),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BootstrapReenrollmentConfig {
    pub reason: String,
    pub rotated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub legacy_issuer_peer_ids: BTreeSet<PeerId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BootstrapAuthPrincipal {
    pub principal_id: PrincipalId,
    pub display_name: String,
    pub org_memberships: BTreeSet<String>,
    pub group_memberships: BTreeSet<String>,
    pub granted_roles: PeerRoleSet,
    pub granted_scopes: BTreeSet<ExperimentScope>,
    pub allowed_networks: BTreeSet<NetworkId>,
    pub custom_claims: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BootstrapEnrollRequest {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RevocationResponse {
    pub network_id: NetworkId,
    pub minimum_revocation_epoch: RevocationEpoch,
    pub quarantined_peers: BTreeSet<PeerId>,
    pub banned_peers: BTreeSet<PeerId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SessionRequest {
    pub session_id: ContentId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LogoutResponse {
    pub logged_out: bool,
}

struct PortalIdentityConnector {
    login_providers: Vec<BrowserLoginProvider>,
    login_paths: BTreeSet<String>,
    callback_paths: BTreeSet<String>,
    trusted_principal_header: Option<String>,
    inner: Box<dyn IdentityConnector + Send + Sync>,
}

impl std::fmt::Debug for PortalIdentityConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortalIdentityConnector")
            .field("login_providers", &self.login_providers)
            .field("login_paths", &self.login_paths)
            .field("callback_paths", &self.callback_paths)
            .field("trusted_principal_header", &self.trusted_principal_header)
            .finish()
    }
}

impl PortalIdentityConnector {
    fn new(
        login_providers: Vec<BrowserLoginProvider>,
        trusted_principal_header: Option<String>,
        inner: Box<dyn IdentityConnector + Send + Sync>,
    ) -> Self {
        let login_paths = login_providers
            .iter()
            .map(|provider| provider.login_path.clone())
            .chain(
                login_providers
                    .iter()
                    .filter_map(|provider| provider.device_path.clone()),
            )
            .collect();
        let callback_paths = login_providers
            .iter()
            .filter_map(|provider| provider.callback_path.clone())
            .collect();
        Self {
            login_providers,
            login_paths,
            callback_paths,
            trusted_principal_header: trusted_principal_header
                .map(|header| header.to_ascii_lowercase()),
            inner,
        }
    }

    fn matches_login_path(&self, path: &str) -> bool {
        self.login_paths.contains(path)
    }

    fn matches_callback_path(&self, path: &str) -> bool {
        self.callback_paths.contains(path)
    }

    fn login_providers(&self) -> Vec<BrowserLoginProvider> {
        self.login_providers.clone()
    }

    fn trusted_callback_principal(&self, request: &HttpRequest) -> Option<PrincipalId> {
        self.trusted_principal_header
            .as_ref()
            .and_then(|header| request.headers.get(header))
            .map(|principal| PrincipalId::new(principal.clone()))
    }
}

impl IdentityConnector for PortalIdentityConnector {
    fn begin_login(&self, req: LoginRequest) -> Result<burn_p2p::LoginStart, burn_p2p::AuthError> {
        self.inner.begin_login(req)
    }

    fn complete_login(
        &self,
        callback: burn_p2p::CallbackPayload,
    ) -> Result<burn_p2p::PrincipalSession, burn_p2p::AuthError> {
        self.inner.complete_login(callback)
    }

    fn refresh(
        &self,
        session: &burn_p2p::PrincipalSession,
    ) -> Result<burn_p2p::PrincipalSession, burn_p2p::AuthError> {
        self.inner.refresh(session)
    }

    fn fetch_claims(
        &self,
        session: &burn_p2p::PrincipalSession,
    ) -> Result<burn_p2p::PrincipalClaims, burn_p2p::AuthError> {
        self.inner.fetch_claims(session)
    }

    fn revoke(&self, session: &burn_p2p::PrincipalSession) -> Result<(), burn_p2p::AuthError> {
        self.inner.revoke(session)
    }
}

#[derive(Debug)]
struct AuthPortalState {
    connector: PortalIdentityConnector,
    login_providers: Vec<BrowserLoginProvider>,
    authority_key_path: PathBuf,
    network_id: NetworkId,
    protocol_version: semver::Version,
    issuer_key_id: Mutex<String>,
    authority: Mutex<NodeCertificateAuthority>,
    trusted_issuers: Mutex<BTreeMap<PeerId, TrustedIssuer>>,
    sessions: Mutex<BTreeMap<ContentId, PrincipalSession>>,
    directory: Mutex<ExperimentDirectory>,
    minimum_revocation_epoch: Mutex<RevocationEpoch>,
    reenrollment: Mutex<Option<BootstrapReenrollmentConfig>>,
    project_family_id: burn_p2p::ProjectFamilyId,
    required_release_train_hash: ContentId,
    allowed_target_artifact_hashes: BTreeSet<ContentId>,
}

fn default_issuer_key_id() -> String {
    "bootstrap-auth".into()
}

fn default_true() -> bool {
    true
}

fn default_browser_mode() -> BrowserMode {
    BrowserMode::Trainer
}

fn default_social_mode() -> SocialMode {
    SocialMode::Public
}

fn default_profile_mode() -> ProfileMode {
    ProfileMode::Disabled
}

fn default_external_principal_header() -> String {
    "x-auth-principal".into()
}

#[derive(Clone, Debug)]
struct SyntheticBootstrapBackend;

impl ProjectBackend for SyntheticBootstrapBackend {
    type Device = String;
}

#[derive(Clone, Debug)]
struct SyntheticBootstrapProject {
    dataset_root: PathBuf,
    learning_rate: f64,
    target_model: f64,
}

impl P2pProject<SyntheticBootstrapBackend> for SyntheticBootstrapProject {
    type Model = f64;
    type Batch = f64;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, _device: &String) -> Self::Model {
        0.0
    }

    fn benchmark(&self, _model: &Self::Model, _device: &String) -> CapabilityEstimate {
        CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 64.0,
            target_window_seconds: 1,
        }
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<String, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        let delta = ctx.batches.iter().copied().sum::<f64>() * self.learning_rate;
        ctx.model += delta;

        Ok(WindowReport {
            contribution: None,
            stats: BTreeMap::from([
                ("delta".into(), MetricValue::Float(delta)),
                ("model".into(), MetricValue::Float(ctx.model)),
                (
                    "loss".into(),
                    MetricValue::Float((self.target_model - ctx.model).abs()),
                ),
            ]),
            completed_at: Utc::now(),
        })
    }

    fn evaluate(&self, model: &Self::Model, _split: EvalSplit) -> MetricReport {
        MetricReport {
            metrics: BTreeMap::from([
                (
                    "loss".into(),
                    MetricValue::Float((self.target_model - *model).abs()),
                ),
                ("model".into(), MetricValue::Float(*model)),
            ]),
            captured_at: Utc::now(),
        }
    }

    fn apply_patch(&mut self, patch: &RuntimePatch) -> PatchOutcome {
        if let Some(burn_p2p::PatchValue::Float(value)) = patch.values.get("learning_rate") {
            self.learning_rate = *value;
            PatchOutcome::Applied
        } else {
            PatchOutcome::Rejected("missing learning_rate patch".into())
        }
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        PatchSupport {
            hot: true,
            warm: false,
            cold: false,
        }
    }
}

impl RuntimeProject<SyntheticBootstrapBackend> for SyntheticBootstrapProject {
    fn runtime_device(&self) -> String {
        "cpu".into()
    }

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        Ok(DatasetRegistration {
            manifest: burn_p2p::DatasetManifest {
                dataset_id: burn_p2p::DatasetId::new("bootstrap-dataset"),
                source_uri: self.dataset_root.display().to_string(),
                format: "microshards".into(),
                manifest_hash: burn_p2p::ContentId::new("bootstrap-manifest"),
                metadata: BTreeMap::new(),
            },
            view: burn_p2p::DatasetView {
                dataset_view_id: burn_p2p::DatasetViewId::new("bootstrap-dataset-view"),
                dataset_id: burn_p2p::DatasetId::new("bootstrap-dataset"),
                preprocessing_hash: burn_p2p::ContentId::new("bootstrap-preprocess"),
                tokenizer_hash: None,
                manifest_hash: burn_p2p::ContentId::new("bootstrap-manifest"),
                metadata: BTreeMap::new(),
            },
            upstream: burn_p2p::UpstreamAdapter::Local {
                root: self.dataset_root.display().to_string(),
            },
        })
    }

    fn microshard_plan(
        &self,
        registration: &DatasetRegistration,
    ) -> anyhow::Result<burn_p2p::MicroShardPlan> {
        Ok(
            burn_p2p::MicroShardPlanner::new(burn_p2p::MicroShardPlannerConfig {
                target_microshard_bytes: 10,
                min_microshards: 2,
                max_microshards: 2,
            })?
            .plan(
                &registration.view,
                DatasetSizing {
                    total_examples: 2,
                    total_tokens: 2,
                    total_bytes: 20,
                },
            )?,
        )
    }

    fn load_batches(
        &self,
        _lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        cached_microshards
            .iter()
            .map(|shard| {
                let bytes = std::fs::read(&shard.path)?;
                let text = String::from_utf8(bytes)?;
                text.trim().parse::<f64>().map_err(anyhow::Error::from)
            })
            .collect()
    }

    fn load_model_artifact(
        &self,
        _model: Self::Model,
        descriptor: &ArtifactDescriptor,
        store: &FsArtifactStore,
        _device: &String,
    ) -> anyhow::Result<Self::Model> {
        Ok(serde_json::from_slice(
            &store.materialize_artifact_bytes(descriptor)?,
        )?)
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: ArtifactKind,
        head_id: burn_p2p::HeadId,
        base_head_id: Option<burn_p2p::HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let mut spec = ArtifactBuildSpec::new(
            artifact_kind,
            burn_p2p::Precision::Fp32,
            burn_p2p::ContentId::new("bootstrap-synthetic-schema"),
            "synthetic-json",
        )
        .with_head(head_id);
        if let Some(base_head_id) = base_head_id {
            spec = spec.with_base_head(base_head_id);
        }
        let bytes = serde_json::to_vec(model)?;
        Ok(store.store_artifact_reader(
            &spec,
            std::io::Cursor::new(bytes),
            ChunkingScheme::new(16)?,
        )?)
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }

    fn merge_candidate_models(
        &self,
        base_model: &Self::Model,
        candidates: &[burn_p2p::MergeModelCandidate<'_, Self::Model>],
        policy: burn_p2p::MergePolicy,
    ) -> anyhow::Result<Option<Self::Model>> {
        if candidates.is_empty() {
            return Ok(None);
        }
        let (weighted_sum, total_weight) = candidates.iter().fold(
            (0.0_f64, 0.0_f64),
            |(weighted_sum, total_weight), candidate| {
                let quality = match policy {
                    burn_p2p::MergePolicy::WeightedMean => 1.0,
                    burn_p2p::MergePolicy::NormClippedWeightedMean => {
                        candidate.quality_weight.clamp(0.0, 1.0)
                    }
                    burn_p2p::MergePolicy::TrimmedMean => candidate.quality_weight,
                    burn_p2p::MergePolicy::Ema | burn_p2p::MergePolicy::QualityWeightedEma => {
                        candidate.quality_weight
                    }
                    burn_p2p::MergePolicy::Custom(_) => candidate.quality_weight,
                };
                let weight = candidate.sample_weight * quality;
                (
                    weighted_sum + (*candidate.model * weight),
                    total_weight + weight,
                )
            },
        );
        if total_weight <= f64::EPSILON {
            return Ok(Some(*base_model));
        }
        Ok(Some(weighted_sum / total_weight))
    }
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

#[derive(Clone)]
struct HttpServerContext {
    plan: Arc<BootstrapPlan>,
    state: Arc<Mutex<BootstrapAdminState>>,
    config: Arc<Mutex<BootstrapDaemonConfig>>,
    config_path: Arc<PathBuf>,
    admin_token: Option<String>,
    allow_dev_admin_token: bool,
    remaining_work_units: Option<u64>,
    admin_signer_peer_id: PeerId,
    auth_state: Option<Arc<AuthPortalState>>,
    control_handle: Option<ControlHandle>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: burn-p2p-bootstrap <config.json>")?;
    let config: BootstrapDaemonConfig = serde_json::from_slice(&std::fs::read(&config_path)?)?;
    validate_compiled_feature_support(&config)?;
    let shared_config = Arc::new(Mutex::new(config.clone()));
    let shared_config_path = Arc::new(config_path);
    let plan = Arc::new(config.spec.clone().plan()?);
    let state = Arc::new(Mutex::new(BootstrapAdminState::default()));
    let auth_state = config
        .auth
        .as_ref()
        .map(|auth| {
            build_auth_portal(
                auth,
                plan.network_id().clone(),
                plan.genesis.protocol_version.clone(),
            )
        })
        .transpose()?
        .map(Arc::new);
    let embedded_runtime = if let Some(runtime) = config.embedded_runtime.clone() {
        let dataset_root = runtime
            .node
            .storage
            .as_ref()
            .map(|storage| storage.root.join("synthetic-dataset"))
            .unwrap_or_else(|| PathBuf::from(".burn_p2p-bootstrap-dataset"));
        ensure_synthetic_dataset(&dataset_root)?;
        let runtime = BootstrapEmbeddedDaemonConfig {
            node: NodeConfig {
                dataset: Some(DatasetConfig::new(burn_p2p::UpstreamAdapter::Local {
                    root: dataset_root.display().to_string(),
                })),
                ..runtime.node
            },
            ..runtime
        };
        let daemon = plan.spawn_embedded_daemon::<_, SyntheticBootstrapBackend>(
            SyntheticBootstrapProject {
                dataset_root,
                learning_rate: 0.1,
                target_model: 1.0,
            },
            runtime,
        )?;
        let daemon_state = daemon.admin_state();
        {
            let mut state_lock = state
                .lock()
                .expect("bootstrap daemon state should not be poisoned");
            *state_lock = daemon_state
                .lock()
                .expect("embedded daemon state should not be poisoned")
                .clone();
        }
        Some(daemon)
    } else {
        None
    };
    let state = embedded_runtime
        .as_ref()
        .map(BootstrapEmbeddedDaemon::admin_state)
        .unwrap_or(state);
    if let Some(auth) = auth_state.as_ref() {
        sync_trust_bundle(auth, &state);
    }
    let control_handle = embedded_runtime
        .as_ref()
        .map(BootstrapEmbeddedDaemon::control_handle);
    let bind_addr = config
        .http_bind_addr
        .clone()
        .unwrap_or_else(|| "127.0.0.1:8787".to_owned());
    let listener = TcpListener::bind(&bind_addr)?;

    eprintln!(
        "burn-p2p-bootstrap listening on http://{} for network {}",
        bind_addr,
        plan.network_id()
    );

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("accept error: {error}");
                continue;
            }
        };

        let context = HttpServerContext {
            plan: Arc::clone(&plan),
            state: Arc::clone(&state),
            config: Arc::clone(&shared_config),
            config_path: Arc::clone(&shared_config_path),
            admin_token: config.admin_token.clone(),
            allow_dev_admin_token: config.allow_dev_admin_token,
            remaining_work_units: config.remaining_work_units,
            auth_state: auth_state.clone(),
            control_handle: control_handle.clone(),
            admin_signer_peer_id: config
                .admin_signer_peer_id
                .clone()
                .unwrap_or_else(|| PeerId::new("bootstrap-authority")),
        };

        thread::spawn(move || {
            if let Err(error) = handle_connection(stream, context) {
                eprintln!("connection error: {error}");
            }
        });
    }

    Ok(())
}

fn handle_connection(
    mut stream: TcpStream,
    context: HttpServerContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(request) = read_request(&stream)? else {
        return Ok(());
    };
    let HttpServerContext {
        plan,
        state,
        config,
        config_path,
        admin_token,
        allow_dev_admin_token,
        remaining_work_units,
        admin_signer_peer_id,
        auth_state,
        control_handle,
    } = context;
    let current_config = config
        .lock()
        .expect("daemon config should not be poisoned")
        .clone();

    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => write_response(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            render_dashboard_html(plan.network_id()).into_bytes(),
        )?,
        ("GET", "/capabilities") | ("GET", "/.well-known/burn-p2p-capabilities") => {
            let signed = sign_browser_snapshot(
                &plan,
                &admin_signer_peer_id,
                "burn_p2p.edge_service_manifest",
                edge_service_manifest(
                    &plan,
                    &current_config,
                    auth_state.as_ref(),
                    &admin_signer_peer_id,
                ),
            )?;
            write_json(&mut stream, &signed)?;
        }
        ("GET", "/portal") => {
            if !portal_route_enabled(&current_config) {
                write_response(
                    &mut stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"portal disabled".to_vec(),
                )?;
                return Ok(());
            }
            let snapshot = current_browser_portal_snapshot(
                &plan,
                &current_config,
                &state,
                auth_state.as_ref(),
                &request,
                remaining_work_units,
            )?;
            write_response(
                &mut stream,
                "200 OK",
                "text/html; charset=utf-8",
                render_browser_portal_html(&snapshot).into_bytes(),
            )?;
        }
        ("GET", "/healthz") => write_response(
            &mut stream,
            "200 OK",
            "text/plain; charset=utf-8",
            b"ok".to_vec(),
        )?,
        ("GET", "/status") => {
            let diagnostics = current_diagnostics(&plan, &state, remaining_work_units);
            write_json(&mut stream, &diagnostics)?;
        }
        ("GET", "/diagnostics/bundle") => {
            let bundle = current_diagnostics_bundle(&plan, &state, remaining_work_units);
            write_json(&mut stream, &bundle)?;
        }
        ("GET", "/portal/snapshot") => {
            if !portal_route_enabled(&current_config) && !browser_edge_enabled(&current_config) {
                write_response(
                    &mut stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"browser portal disabled".to_vec(),
                )?;
                return Ok(());
            }
            let snapshot = current_browser_portal_snapshot(
                &plan,
                &current_config,
                &state,
                auth_state.as_ref(),
                &request,
                remaining_work_units,
            )?;
            write_json(&mut stream, &snapshot)?;
        }
        ("GET", "/leaderboard") => {
            if !social_enabled(&current_config) {
                write_response(
                    &mut stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"social disabled".to_vec(),
                )?;
                return Ok(());
            }
            let leaderboard = current_browser_leaderboard(&plan, &state);
            write_json(&mut stream, &leaderboard)?;
        }
        ("GET", "/directory/signed") => {
            if !portal_route_enabled(&current_config) && !browser_edge_enabled(&current_config) {
                write_response(
                    &mut stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"browser directory disabled".to_vec(),
                )?;
                return Ok(());
            }
            let snapshot =
                current_browser_directory_snapshot(&plan, auth_state.as_ref(), &request)?;
            let signed = sign_browser_snapshot(
                &plan,
                &admin_signer_peer_id,
                "burn_p2p.browser_directory_snapshot",
                snapshot,
            )?;
            write_json(&mut stream, &signed)?;
        }
        ("GET", "/leaderboard/signed") => {
            if !social_enabled(&current_config) {
                write_response(
                    &mut stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"social disabled".to_vec(),
                )?;
                return Ok(());
            }
            let signed = sign_browser_snapshot(
                &plan,
                &admin_signer_peer_id,
                "burn_p2p.browser_leaderboard_snapshot",
                current_browser_leaderboard(&plan, &state),
            )?;
            write_json(&mut stream, &signed)?;
        }
        ("GET", "/metrics") => {
            let diagnostics = current_diagnostics(&plan, &state, remaining_work_units);
            write_response(
                &mut stream,
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                render_openmetrics(&diagnostics).into_bytes(),
            )?;
        }
        ("GET", "/events") => write_event_stream(&mut stream, &plan, &state, remaining_work_units)?,
        ("GET", "/receipts") => {
            let query = ReceiptQuery {
                study_id: None,
                experiment_id: None,
                revision_id: None,
                peer_id: None,
            };
            let receipts = state
                .lock()
                .expect("bootstrap admin state should not be poisoned")
                .export_receipts(&query);
            write_json(&mut stream, &receipts)?;
        }
        ("POST", "/receipts/browser") => {
            if !browser_edge_enabled(&current_config) {
                write_response(
                    &mut stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"browser edge disabled".to_vec(),
                )?;
                return Ok(());
            }
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let session = request
                .headers
                .get("x-session-id")
                .and_then(|session_id| {
                    auth.sessions
                        .lock()
                        .expect("auth session state should not be poisoned")
                        .get(&ContentId::new(session_id.clone()))
                        .cloned()
                })
                .ok_or("browser receipt submission requires x-session-id")?;
            let receipts: Vec<burn_p2p::ContributionReceipt> =
                serde_json::from_slice(&request.body)?;
            if receipts
                .iter()
                .any(|receipt| !session_allows_receipt_submission(&session, receipt))
            {
                return Err(
                    "session is not authorized to submit one or more browser receipts".into(),
                );
            }
            let accepted_receipt_ids = state
                .lock()
                .expect("bootstrap admin state should not be poisoned")
                .ingest_contribution_receipts(receipts);
            write_json(
                &mut stream,
                &BrowserReceiptSubmissionResponse {
                    pending_receipt_count: 0,
                    accepted_receipt_ids,
                },
            )?;
        }
        ("GET", "/heads") => {
            let heads = state
                .lock()
                .expect("bootstrap admin state should not be poisoned")
                .export_heads(&burn_p2p_bootstrap::HeadQuery {
                    study_id: None,
                    experiment_id: None,
                    revision_id: None,
                    head_id: None,
                });
            write_json(&mut stream, &heads)?;
        }
        ("GET", "/reducers/load") => {
            let reducer_load = state
                .lock()
                .expect("bootstrap admin state should not be poisoned")
                .export_reducer_load(&burn_p2p_bootstrap::ReducerLoadQuery {
                    study_id: None,
                    experiment_id: None,
                    peer_id: None,
                });
            write_json(&mut stream, &reducer_load)?;
        }
        ("POST", path)
            if auth_state
                .as_ref()
                .is_some_and(|auth| auth.connector.matches_login_path(path)) =>
        {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let login_request: LoginRequest = serde_json::from_slice(&request.body)?;
            let login = auth.connector.begin_login(login_request)?;
            write_json(&mut stream, &login)?;
        }
        ("POST", path)
            if auth_state
                .as_ref()
                .is_some_and(|auth| auth.connector.matches_callback_path(path)) =>
        {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let mut callback: burn_p2p::CallbackPayload = serde_json::from_slice(&request.body)?;
            if callback.principal_id.is_none() {
                callback.principal_id = auth.connector.trusted_callback_principal(&request);
            }
            let session = auth.connector.complete_login(callback)?;
            auth.sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .insert(session.session_id.clone(), session.clone());
            write_json(&mut stream, &session)?;
        }
        ("POST", "/refresh") => {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let refresh: SessionRequest = serde_json::from_slice(&request.body)?;
            let session = auth
                .sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .get(&refresh.session_id)
                .cloned()
                .ok_or("unknown session id")?;
            let refreshed = auth.connector.refresh(&session)?;
            let mut sessions = auth
                .sessions
                .lock()
                .expect("auth session state should not be poisoned");
            sessions.remove(&refresh.session_id);
            sessions.insert(refreshed.session_id.clone(), refreshed.clone());
            write_json(&mut stream, &refreshed)?;
        }
        ("POST", "/logout") => {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let logout: SessionRequest = serde_json::from_slice(&request.body)?;
            let session = auth
                .sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .get(&logout.session_id)
                .cloned();
            if let Some(session) = session.as_ref() {
                auth.connector.revoke(session)?;
            }
            let logged_out = auth
                .sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .remove(&logout.session_id)
                .is_some();
            write_json(&mut stream, &LogoutResponse { logged_out })?;
        }
        ("POST", "/enroll") => {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let effective_revocation_epoch = current_revocation_epoch(auth, &state);
            let enroll: BootstrapEnrollRequest = serde_json::from_slice(&request.body)?;
            if enroll.release_train_hash != auth.required_release_train_hash {
                return Err(format!(
                    "release train {} is not permitted by this authority",
                    enroll.release_train_hash.as_str(),
                )
                .into());
            }
            if !auth.allowed_target_artifact_hashes.is_empty()
                && !auth
                    .allowed_target_artifact_hashes
                    .contains(&enroll.target_artifact_hash)
            {
                return Err(format!(
                    "target artifact {} is not permitted by this authority",
                    enroll.target_artifact_hash.as_str(),
                )
                .into());
            }
            let session = auth
                .sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .get(&enroll.session_id)
                .cloned()
                .ok_or("unknown session id")?;
            let certificate = auth
                .authority
                .lock()
                .expect("auth authority should not be poisoned")
                .issue_certificate(NodeEnrollmentRequest {
                    session,
                    project_family_id: auth.project_family_id.clone(),
                    release_train_hash: enroll.release_train_hash.clone(),
                    target_artifact_hash: enroll.target_artifact_hash.clone(),
                    peer_id: enroll.peer_id,
                    peer_public_key_hex: enroll.peer_public_key_hex,
                    granted_roles: auth
                        .sessions
                        .lock()
                        .expect("auth session state should not be poisoned")
                        .get(&enroll.session_id)
                        .map(|session| session.claims.granted_roles.clone())
                        .ok_or("unknown session id")?,
                    requested_scopes: enroll.requested_scopes,
                    client_policy_hash: enroll.client_policy_hash,
                    serial: enroll.serial,
                    not_before: Utc::now(),
                    not_after: Utc::now() + chrono::Duration::seconds(enroll.ttl_seconds.max(1)),
                    revocation_epoch: effective_revocation_epoch,
                })?;
            write_json(&mut stream, &certificate)?;
        }
        ("GET", "/revocations") => {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let response = RevocationResponse {
                network_id: plan.network_id().clone(),
                minimum_revocation_epoch: current_revocation_epoch(auth, &state),
                quarantined_peers: state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .quarantined_peers
                    .clone(),
                banned_peers: state
                    .lock()
                    .expect("bootstrap admin state should not be poisoned")
                    .banned_peers
                    .clone(),
            };
            write_json(&mut stream, &response)?;
        }
        ("GET", "/directory") => {
            let entries = auth_state
                .as_ref()
                .map(|auth| auth_directory_entries(auth, &request))
                .transpose()?
                .unwrap_or_default();
            write_json(&mut stream, &entries)?;
        }
        ("GET", "/trust") => {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let trust_bundle = current_trust_bundle(auth, &state);
            write_json(&mut stream, &trust_bundle)?;
        }
        ("GET", "/reenrollment") => {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let trust_bundle = current_trust_bundle(auth, &state);
            write_json(&mut stream, &trust_bundle.reenrollment)?;
        }
        ("POST", "/admin") => {
            let action: AdminAction = serde_json::from_slice(&request.body)?;
            if !token_matches(&request, admin_token.as_deref(), allow_dev_admin_token) {
                let Some(capabilities) = request_admin_capabilities(&request, auth_state.as_ref())
                else {
                    write_response(
                        &mut stream,
                        "401 Unauthorized",
                        "text/plain; charset=utf-8",
                        b"missing or invalid x-admin-token or x-session-id".to_vec(),
                    )?;
                    return Ok(());
                };
                if !capabilities.contains(&action.capability()) {
                    write_response(
                        &mut stream,
                        "403 Forbidden",
                        "text/plain; charset=utf-8",
                        format!(
                            "session is not authorized for admin capability {:?}",
                            action.capability()
                        )
                        .into_bytes(),
                    )?;
                    return Ok(());
                }
            }

            let result = match action.clone() {
                AdminAction::RotateAuthorityMaterial {
                    issuer_key_id,
                    retain_previous_issuer,
                    require_reenrollment,
                    reenrollment_reason,
                } => {
                    if !plan.supports_admin_action(&action) {
                        return Err(Box::new(
                            burn_p2p_bootstrap::BootstrapError::UnsupportedAdminAction(
                                action.capability(),
                            ),
                        ));
                    }
                    let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
                    let result = rotate_authority_material(
                        auth,
                        &state,
                        issuer_key_id,
                        retain_previous_issuer,
                        require_reenrollment,
                        reenrollment_reason.clone(),
                    )?;
                    let mut config_guard =
                        config.lock().expect("daemon config should not be poisoned");
                    if let Some(auth_config) = config_guard.auth.as_mut() {
                        auth_config.issuer_key_id = match &result {
                            burn_p2p_bootstrap::AdminResult::AuthorityMaterialRotated {
                                issuer_key_id,
                                ..
                            } => issuer_key_id.clone(),
                            _ => auth_config.issuer_key_id.clone(),
                        };
                        auth_config.trusted_issuers = current_trust_bundle(auth, &state)
                            .issuers
                            .iter()
                            .map(|issuer| TrustedIssuer {
                                issuer_peer_id: issuer.issuer_peer_id.clone(),
                                issuer_public_key_hex: issuer.issuer_public_key_hex.clone(),
                            })
                            .collect();
                        auth_config.reenrollment = auth
                            .reenrollment
                            .lock()
                            .expect("auth reenrollment state should not be poisoned")
                            .clone();
                    }
                    persist_daemon_config(&config_path, &config_guard)?;
                    result
                }
                AdminAction::RolloutAuthPolicy(rollout) => {
                    if !plan.supports_admin_action(&action) {
                        return Err(Box::new(
                            burn_p2p_bootstrap::BootstrapError::UnsupportedAdminAction(
                                action.capability(),
                            ),
                        ));
                    }
                    let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
                    let result = rollout_auth_policy(
                        &plan,
                        auth,
                        &state,
                        rollout.clone(),
                        control_handle.as_ref(),
                    )?;
                    let mut config_guard =
                        config.lock().expect("daemon config should not be poisoned");
                    if let Some(auth_config) = config_guard.auth.as_mut() {
                        if let Some(minimum_revocation_epoch) = rollout.minimum_revocation_epoch {
                            auth_config.minimum_revocation_epoch = auth_config
                                .minimum_revocation_epoch
                                .max(minimum_revocation_epoch.0);
                        }
                        if let Some(directory_entries) = rollout.directory_entries {
                            auth_config.directory_entries = directory_entries;
                        }
                        if let Some(trusted_issuers) = rollout.trusted_issuers {
                            auth_config.trusted_issuers = trusted_issuers;
                        }
                        if let Some(reenrollment) = rollout.reenrollment {
                            auth_config.reenrollment = Some(BootstrapReenrollmentConfig {
                                reason: reenrollment.reason,
                                rotated_at: reenrollment.rotated_at,
                                legacy_issuer_peer_ids: reenrollment.legacy_issuer_peer_ids,
                            });
                        }
                    }
                    persist_daemon_config(&config_path, &config_guard)?;
                    result
                }
                AdminAction::RetireTrustedIssuers { issuer_peer_ids } => {
                    if !plan.supports_admin_action(&action) {
                        return Err(Box::new(
                            burn_p2p_bootstrap::BootstrapError::UnsupportedAdminAction(
                                action.capability(),
                            ),
                        ));
                    }
                    let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
                    let result = retire_trusted_issuers(auth, &state, &issuer_peer_ids)?;
                    let mut config_guard =
                        config.lock().expect("daemon config should not be poisoned");
                    if let Some(auth_config) = config_guard.auth.as_mut() {
                        auth_config.trusted_issuers = current_trust_bundle(auth, &state)
                            .issuers
                            .iter()
                            .map(|issuer| TrustedIssuer {
                                issuer_peer_id: issuer.issuer_peer_id.clone(),
                                issuer_public_key_hex: issuer.issuer_public_key_hex.clone(),
                            })
                            .collect();
                        auth_config.reenrollment = auth
                            .reenrollment
                            .lock()
                            .expect("auth reenrollment state should not be poisoned")
                            .clone();
                    }
                    persist_daemon_config(&config_path, &config_guard)?;
                    result
                }
                AdminAction::ExportTrustBundle => {
                    let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
                    burn_p2p_bootstrap::AdminResult::TrustBundle(Some(current_trust_bundle(
                        auth, &state,
                    )))
                }
                _ => {
                    let signer = Some(SignatureMetadata {
                        signer: admin_signer_peer_id,
                        key_id: "bootstrap-admin".into(),
                        algorithm: SignatureAlgorithm::Ed25519,
                        signed_at: Utc::now(),
                        signature_hex: "bootstrap-local-admin".into(),
                    });
                    let result = plan.execute_admin_action(
                        action,
                        &mut state
                            .lock()
                            .expect("bootstrap admin state should not be poisoned"),
                        signer,
                        Utc::now(),
                        remaining_work_units,
                    )?;
                    publish_admin_result(&plan, control_handle.as_ref(), &result)?;
                    result
                }
            };
            write_json(&mut stream, &result)?;
        }
        _ => write_response(
            &mut stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            b"not found".to_vec(),
        )?,
    }

    Ok(())
}

fn current_diagnostics(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
    remaining_work_units: Option<u64>,
) -> burn_p2p_bootstrap::BootstrapDiagnostics {
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .diagnostics(plan, Utc::now(), remaining_work_units)
}

fn current_diagnostics_bundle(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
    remaining_work_units: Option<u64>,
) -> burn_p2p_bootstrap::BootstrapDiagnosticsBundle {
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .diagnostics_bundle(plan, Utc::now(), remaining_work_units)
}

fn compiled_feature_set() -> CompiledFeatureSet {
    let mut features = BTreeSet::new();
    if cfg!(feature = "admin-http") {
        features.insert(EdgeFeature::AdminHttp);
    }
    if cfg!(feature = "metrics") {
        features.insert(EdgeFeature::Metrics);
    }
    if cfg!(feature = "portal") {
        features.insert(EdgeFeature::Portal);
    }
    if cfg!(feature = "browser-edge") {
        features.insert(EdgeFeature::BrowserEdge);
    }
    if cfg!(feature = "rbac") {
        features.insert(EdgeFeature::Rbac);
    }
    if cfg!(feature = "auth-static") {
        features.insert(EdgeFeature::AuthStatic);
    }
    if cfg!(feature = "auth-github") {
        features.insert(EdgeFeature::AuthGitHub);
    }
    if cfg!(feature = "auth-oidc") {
        features.insert(EdgeFeature::AuthOidc);
    }
    if cfg!(feature = "auth-oauth") {
        features.insert(EdgeFeature::AuthOAuth);
    }
    if cfg!(feature = "auth-external") {
        features.insert(EdgeFeature::AuthExternal);
    }
    if cfg!(feature = "social") {
        features.insert(EdgeFeature::Social);
        features.insert(EdgeFeature::Profiles);
    }
    CompiledFeatureSet { features }
}

fn configured_auth_providers(config: &BootstrapDaemonConfig) -> BTreeSet<EdgeAuthProvider> {
    match config.auth.as_ref().map(|auth| &auth.connector) {
        Some(BootstrapAuthConnectorConfig::Static) => BTreeSet::from([EdgeAuthProvider::Static]),
        Some(BootstrapAuthConnectorConfig::GitHub { .. }) => {
            BTreeSet::from([EdgeAuthProvider::GitHub])
        }
        Some(BootstrapAuthConnectorConfig::Oidc { .. }) => BTreeSet::from([EdgeAuthProvider::Oidc]),
        Some(BootstrapAuthConnectorConfig::OAuth { .. }) => {
            BTreeSet::from([EdgeAuthProvider::OAuth])
        }
        Some(BootstrapAuthConnectorConfig::External { .. }) => {
            BTreeSet::from([EdgeAuthProvider::External])
        }
        None => BTreeSet::new(),
    }
}

fn configured_service_set(config: &BootstrapDaemonConfig) -> ConfiguredServiceSet {
    let mut features = BTreeSet::from([EdgeFeature::AdminHttp, EdgeFeature::Metrics]);
    if config.optional_services.portal_enabled {
        features.insert(EdgeFeature::Portal);
    }
    if config.optional_services.browser_mode != BrowserMode::Disabled {
        features.insert(EdgeFeature::BrowserEdge);
    }
    if config.optional_services.social_mode != SocialMode::Disabled {
        features.insert(EdgeFeature::Social);
    }
    if config.optional_services.profile_mode != ProfileMode::Disabled {
        features.insert(EdgeFeature::Profiles);
    }
    for provider in configured_auth_providers(config) {
        match provider {
            EdgeAuthProvider::Static => {
                features.insert(EdgeFeature::AuthStatic);
            }
            EdgeAuthProvider::GitHub => {
                features.insert(EdgeFeature::AuthGitHub);
            }
            EdgeAuthProvider::Oidc => {
                features.insert(EdgeFeature::AuthOidc);
            }
            EdgeAuthProvider::OAuth => {
                features.insert(EdgeFeature::AuthOAuth);
            }
            EdgeAuthProvider::External => {
                features.insert(EdgeFeature::AuthExternal);
            }
        }
    }
    if config.auth.is_some() {
        features.insert(EdgeFeature::Rbac);
    }
    ConfiguredServiceSet { features }
}

fn active_service_set(
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
) -> ActiveServiceSet {
    let compiled = compiled_feature_set();
    let configured = configured_service_set(config);
    let features = configured
        .features
        .into_iter()
        .filter(|feature| compiled.features.contains(feature))
        .filter(|feature| match feature {
            EdgeFeature::Rbac => auth_state.is_some(),
            _ => true,
        })
        .collect();
    ActiveServiceSet { features }
}

fn validate_compiled_feature_support(
    config: &BootstrapDaemonConfig,
) -> Result<(), BootstrapCompositionError> {
    validate_compiled_feature_support_with(&compiled_feature_set(), config)
}

fn validate_compiled_feature_support_with(
    compiled: &CompiledFeatureSet,
    config: &BootstrapDaemonConfig,
) -> Result<(), BootstrapCompositionError> {
    if config.optional_services.portal_enabled && !compiled.features.contains(&EdgeFeature::Portal)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "portal",
            feature: "portal",
        });
    }
    if config.optional_services.browser_mode != BrowserMode::Disabled
        && !compiled.features.contains(&EdgeFeature::BrowserEdge)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "browser edge",
            feature: "browser-edge",
        });
    }
    if config.optional_services.social_mode != SocialMode::Disabled
        && !compiled.features.contains(&EdgeFeature::Social)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "social",
            feature: "social",
        });
    }
    if config.optional_services.profile_mode != ProfileMode::Disabled
        && config.optional_services.social_mode == SocialMode::Disabled
    {
        return Err(BootstrapCompositionError::InvalidServiceConfig(
            "profile_mode requires social_mode to be enabled",
        ));
    }
    if config.optional_services.profile_mode != ProfileMode::Disabled
        && !compiled.features.contains(&EdgeFeature::Profiles)
    {
        return Err(BootstrapCompositionError::MissingCompiledFeature {
            service: "profiles",
            feature: "social",
        });
    }
    if let Some(auth) = config.auth.as_ref() {
        let required = match &auth.connector {
            BootstrapAuthConnectorConfig::Static => EdgeFeature::AuthStatic,
            BootstrapAuthConnectorConfig::GitHub { .. } => EdgeFeature::AuthGitHub,
            BootstrapAuthConnectorConfig::Oidc { .. } => EdgeFeature::AuthOidc,
            BootstrapAuthConnectorConfig::OAuth { .. } => EdgeFeature::AuthOAuth,
            BootstrapAuthConnectorConfig::External { .. } => EdgeFeature::AuthExternal,
        };
        let feature = match required {
            EdgeFeature::AuthStatic => "auth-static",
            EdgeFeature::AuthGitHub => "auth-github",
            EdgeFeature::AuthOidc => "auth-oidc",
            EdgeFeature::AuthOAuth => "auth-oauth",
            EdgeFeature::AuthExternal => "auth-external",
            _ => unreachable!("auth feature mapping should stay exhaustive"),
        };
        if !compiled.features.contains(&required) {
            return Err(BootstrapCompositionError::MissingCompiledFeature {
                service: "auth connector",
                feature,
            });
        }
        if let BootstrapAuthConnectorConfig::External {
            trusted_principal_header,
            trusted_internal_only,
            ..
        } = &auth.connector
        {
            if !trusted_internal_only {
                return Err(BootstrapCompositionError::InvalidServiceConfig(
                    "auth-external requires trusted_internal_only = true",
                ));
            }
            if trusted_principal_header.trim().is_empty() {
                return Err(BootstrapCompositionError::InvalidServiceConfig(
                    "auth-external requires a non-empty trusted_principal_header",
                ));
            }
        }
    }
    Ok(())
}

fn portal_mode(
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
) -> PortalMode {
    if !config.optional_services.portal_enabled {
        PortalMode::Disabled
    } else if auth_state.is_some() {
        PortalMode::Interactive
    } else {
        PortalMode::Readonly
    }
}

fn profile_mode(config: &BootstrapDaemonConfig) -> ProfileMode {
    if config.optional_services.social_mode == SocialMode::Disabled {
        ProfileMode::Disabled
    } else {
        config.optional_services.profile_mode.clone()
    }
}

fn admin_mode(
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
) -> AdminMode {
    if auth_state.is_some() && cfg!(feature = "rbac") {
        AdminMode::Rbac
    } else if config.allow_dev_admin_token {
        AdminMode::Token
    } else {
        AdminMode::Disabled
    }
}

fn metrics_mode() -> MetricsMode {
    if cfg!(feature = "metrics") {
        MetricsMode::OpenMetrics
    } else {
        MetricsMode::Disabled
    }
}

fn edge_service_manifest(
    plan: &BootstrapPlan,
    config: &BootstrapDaemonConfig,
    auth_state: Option<&Arc<AuthPortalState>>,
    edge_id: &PeerId,
) -> EdgeServiceManifest {
    let compiled_feature_set = compiled_feature_set();
    let configured_service_set = configured_service_set(config);
    let active_feature_set = active_service_set(config, auth_state);
    EdgeServiceManifest {
        edge_id: edge_id.clone(),
        network_id: plan.network_id().clone(),
        portal_mode: portal_mode(config, auth_state),
        browser_mode: config.optional_services.browser_mode.clone(),
        available_auth_providers: configured_auth_providers(config),
        social_mode: config.optional_services.social_mode.clone(),
        profile_mode: profile_mode(config),
        admin_mode: admin_mode(config, auth_state),
        metrics_mode: metrics_mode(),
        compiled_feature_set,
        configured_service_set,
        active_feature_set,
        generated_at: Utc::now(),
    }
}

fn portal_route_enabled(config: &BootstrapDaemonConfig) -> bool {
    config.optional_services.portal_enabled
}

fn browser_edge_enabled(config: &BootstrapDaemonConfig) -> bool {
    config.optional_services.browser_mode != BrowserMode::Disabled
}

fn social_enabled(config: &BootstrapDaemonConfig) -> bool {
    config.optional_services.social_mode != SocialMode::Disabled
}

fn browser_edge_mode(plan: &BootstrapPlan) -> BrowserEdgeMode {
    if plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Relay)
        || plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Kademlia)
    {
        if plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Validator)
            || plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Archive)
            || plan.supports_service(&burn_p2p_bootstrap::BootstrapService::Authority)
        {
            BrowserEdgeMode::Full
        } else {
            BrowserEdgeMode::Peer
        }
    } else {
        BrowserEdgeMode::Minimal
    }
}

fn browser_transport_surface(
    plan: &BootstrapPlan,
    config: &BootstrapDaemonConfig,
) -> BrowserTransportSurface {
    if !browser_edge_enabled(config) {
        return BrowserTransportSurface {
            webrtc_direct: false,
            webtransport_gateway: false,
            wss_fallback: false,
        };
    }
    let edge_mode = browser_edge_mode(plan);
    BrowserTransportSurface {
        webrtc_direct: matches!(edge_mode, BrowserEdgeMode::Peer | BrowserEdgeMode::Full),
        webtransport_gateway: matches!(edge_mode, BrowserEdgeMode::Full),
        wss_fallback: true,
    }
}

fn browser_login_providers(auth_state: Option<&Arc<AuthPortalState>>) -> Vec<BrowserLoginProvider> {
    auth_state
        .map(|auth| auth.login_providers.clone())
        .unwrap_or_default()
}

fn current_browser_directory_snapshot(
    plan: &BootstrapPlan,
    auth_state: Option<&Arc<AuthPortalState>>,
    request: &HttpRequest,
) -> Result<BrowserDirectorySnapshot, Box<dyn std::error::Error>> {
    let entries = auth_state
        .map(|auth| auth_directory_entries(auth, request))
        .transpose()?
        .unwrap_or_default();
    Ok(BrowserDirectorySnapshot {
        network_id: plan.network_id().clone(),
        generated_at: Utc::now(),
        entries,
    })
}

fn current_browser_leaderboard(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> burn_p2p_bootstrap::BrowserLeaderboardSnapshot {
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .leaderboard_snapshot(plan, Utc::now())
}

fn current_browser_portal_snapshot(
    plan: &BootstrapPlan,
    config: &BootstrapDaemonConfig,
    state: &Arc<Mutex<BootstrapAdminState>>,
    auth_state: Option<&Arc<AuthPortalState>>,
    request: &HttpRequest,
    remaining_work_units: Option<u64>,
) -> Result<burn_p2p_bootstrap::BrowserPortalSnapshot, Box<dyn std::error::Error>> {
    let directory = current_browser_directory_snapshot(plan, auth_state, request)?;
    let (required_release_train_hash, allowed_target_artifact_hashes) = auth_state
        .map(|auth| {
            (
                Some(auth.required_release_train_hash.clone()),
                auth.allowed_target_artifact_hashes.clone(),
            )
        })
        .unwrap_or_else(|| (None, BTreeSet::new()));

    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .browser_portal_snapshot(
            plan,
            BrowserPortalSnapshotConfig {
                captured_at: Utc::now(),
                remaining_work_units,
                directory,
                edge_mode: browser_edge_mode(plan),
                browser_mode: config.optional_services.browser_mode.clone(),
                social_mode: config.optional_services.social_mode.clone(),
                profile_mode: config.optional_services.profile_mode.clone(),
                transports: browser_transport_surface(plan, config),
                auth_enabled: auth_state.is_some(),
                login_providers: browser_login_providers(auth_state),
                required_release_train_hash,
                allowed_target_artifact_hashes,
            },
        ))
}

fn sign_browser_snapshot<T: serde::Serialize>(
    plan: &BootstrapPlan,
    signer: &PeerId,
    schema: &str,
    payload: T,
) -> Result<SignedPayload<SchemaEnvelope<T>>, Box<dyn std::error::Error>> {
    Ok(SignedPayload::new(
        SchemaEnvelope::new(schema, plan.genesis.protocol_version.clone(), payload),
        SignatureMetadata {
            signer: signer.clone(),
            key_id: "bootstrap-browser-edge".into(),
            algorithm: SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "bootstrap-browser-edge".into(),
        },
    )?)
}

fn current_revocation_epoch(
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> RevocationEpoch {
    let auth_minimum_revocation_epoch = *auth
        .minimum_revocation_epoch
        .lock()
        .expect("auth revocation epoch should not be poisoned");
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .minimum_revocation_epoch
        .map(|epoch| epoch.max(auth_minimum_revocation_epoch))
        .unwrap_or(auth_minimum_revocation_epoch)
}

fn current_trust_bundle(
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> TrustBundleExport {
    let authority = auth
        .authority
        .lock()
        .expect("auth authority should not be poisoned");
    let active_issuer_peer_id = authority.issuer_peer_id();
    let active_issuer = TrustedIssuer {
        issuer_peer_id: active_issuer_peer_id.clone(),
        issuer_public_key_hex: authority.issuer_public_key_hex().to_owned(),
    };
    let mut trusted_issuers = auth
        .trusted_issuers
        .lock()
        .expect("trusted issuer state should not be poisoned")
        .clone();
    trusted_issuers.insert(active_issuer_peer_id.clone(), active_issuer);
    drop(authority);

    let mut issuers = trusted_issuers
        .into_values()
        .map(|issuer| TrustedIssuerStatus {
            active_for_new_certificates: issuer.issuer_peer_id == active_issuer_peer_id,
            accepted_for_admission: true,
            issuer_peer_id: issuer.issuer_peer_id,
            issuer_public_key_hex: issuer.issuer_public_key_hex,
        })
        .collect::<Vec<_>>();
    issuers.sort_by(|left, right| left.issuer_peer_id.cmp(&right.issuer_peer_id));

    let reenrollment = auth
        .reenrollment
        .lock()
        .expect("auth reenrollment state should not be poisoned")
        .clone()
        .map(|reenrollment| ReenrollmentStatus {
            reason: reenrollment.reason,
            rotated_at: reenrollment.rotated_at,
            legacy_issuer_peer_ids: reenrollment.legacy_issuer_peer_ids,
            login_path: "/login/static".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
        });

    TrustBundleExport {
        network_id: auth.network_id.clone(),
        project_family_id: auth.project_family_id.clone(),
        required_release_train_hash: auth.required_release_train_hash.clone(),
        allowed_target_artifact_hashes: auth.allowed_target_artifact_hashes.clone(),
        minimum_revocation_epoch: current_revocation_epoch(auth, state),
        active_issuer_peer_id,
        issuers,
        reenrollment,
    }
}

fn sync_trust_bundle(
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> TrustBundleExport {
    let trust_bundle = current_trust_bundle(auth, state);
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .trust_bundle = Some(trust_bundle.clone());
    trust_bundle
}

fn persist_daemon_config(
    config_path: &Path,
    config: &BootstrapDaemonConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(config_path, serde_json::to_vec_pretty(config)?)?;
    Ok(())
}

fn rotate_authority_material(
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
    issuer_key_id: Option<String>,
    retain_previous_issuer: bool,
    require_reenrollment: bool,
    reenrollment_reason: Option<String>,
) -> Result<burn_p2p_bootstrap::AdminResult, Box<dyn std::error::Error>> {
    let issuer_key_id = issuer_key_id.unwrap_or_else(|| "bootstrap-auth".into());
    if let Some(parent) = auth.authority_key_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let previous_authority = auth
        .authority
        .lock()
        .expect("auth authority should not be poisoned");
    let previous_issuer = TrustedIssuer {
        issuer_peer_id: previous_authority.issuer_peer_id(),
        issuer_public_key_hex: previous_authority.issuer_public_key_hex().to_owned(),
    };
    drop(previous_authority);

    let keypair = Keypair::generate_ed25519();
    std::fs::write(&auth.authority_key_path, keypair.to_protobuf_encoding()?)?;
    let authority = NodeCertificateAuthority::new(
        auth.network_id.clone(),
        auth.project_family_id.clone(),
        auth.required_release_train_hash.clone(),
        auth.protocol_version.clone(),
        keypair,
        issuer_key_id.clone(),
    )?;
    let issuer_peer_id = authority.issuer_peer_id();
    let issuer_public_key_hex = authority.issuer_public_key_hex().to_owned();
    {
        let mut trusted_issuers = auth
            .trusted_issuers
            .lock()
            .expect("trusted issuer state should not be poisoned");
        if retain_previous_issuer {
            trusted_issuers.insert(
                previous_issuer.issuer_peer_id.clone(),
                previous_issuer.clone(),
            );
        } else {
            trusted_issuers.remove(&previous_issuer.issuer_peer_id);
        }
        trusted_issuers.insert(
            issuer_peer_id.clone(),
            TrustedIssuer {
                issuer_peer_id: issuer_peer_id.clone(),
                issuer_public_key_hex: issuer_public_key_hex.clone(),
            },
        );
    }
    *auth
        .issuer_key_id
        .lock()
        .expect("auth issuer key id should not be poisoned") = issuer_key_id.clone();
    if require_reenrollment {
        *auth
            .reenrollment
            .lock()
            .expect("auth reenrollment state should not be poisoned") =
            Some(BootstrapReenrollmentConfig {
                reason: reenrollment_reason.unwrap_or_else(|| {
                    "authority material rotated; clients should re-enroll".into()
                }),
                rotated_at: Some(Utc::now()),
                legacy_issuer_peer_ids: BTreeSet::from([previous_issuer.issuer_peer_id.clone()]),
            });
    } else if !retain_previous_issuer {
        *auth
            .reenrollment
            .lock()
            .expect("auth reenrollment state should not be poisoned") = None;
    }
    *auth
        .authority
        .lock()
        .expect("auth authority should not be poisoned") = authority;
    let trust_bundle = sync_trust_bundle(auth, state);

    Ok(burn_p2p_bootstrap::AdminResult::AuthorityMaterialRotated {
        issuer_key_id,
        issuer_peer_id,
        issuer_public_key_hex,
        trusted_issuers: trust_bundle.issuers.len(),
        reenrollment_required: trust_bundle.reenrollment.is_some(),
        rotated_at: Utc::now(),
    })
}

fn rollout_auth_policy(
    plan: &BootstrapPlan,
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
    rollout: AuthPolicyRollout,
    control_handle: Option<&ControlHandle>,
) -> Result<burn_p2p_bootstrap::AdminResult, Box<dyn std::error::Error>> {
    let mut effective_minimum_revocation_epoch = None;
    if let Some(minimum_revocation_epoch) = rollout.minimum_revocation_epoch {
        let mut auth_epoch = auth
            .minimum_revocation_epoch
            .lock()
            .expect("auth revocation epoch should not be poisoned");
        *auth_epoch = (*auth_epoch).max(minimum_revocation_epoch);
        effective_minimum_revocation_epoch = Some(*auth_epoch);
    }

    let directory_entries = if let Some(entries) = rollout.directory_entries {
        let announced_at = Utc::now();
        {
            let mut directory = auth
                .directory
                .lock()
                .expect("auth directory should not be poisoned");
            directory.generated_at = announced_at;
            directory.entries = entries;
        }
        if let Some(control_handle) = control_handle {
            control_handle.publish_directory(ExperimentDirectoryAnnouncement {
                network_id: plan.network_id().clone(),
                entries: auth
                    .directory
                    .lock()
                    .expect("auth directory should not be poisoned")
                    .entries
                    .clone(),
                announced_at,
            })?;
        }
        auth.directory
            .lock()
            .expect("auth directory should not be poisoned")
            .entries
            .len()
    } else {
        auth.directory
            .lock()
            .expect("auth directory should not be poisoned")
            .entries
            .len()
    };

    let trusted_issuers = if let Some(issuers) = rollout.trusted_issuers {
        let mut trusted = auth
            .trusted_issuers
            .lock()
            .expect("trusted issuer state should not be poisoned");
        *trusted = issuers
            .into_iter()
            .map(|issuer| (issuer.issuer_peer_id.clone(), issuer))
            .collect();
        let authority = auth
            .authority
            .lock()
            .expect("auth authority should not be poisoned");
        trusted.insert(
            authority.issuer_peer_id(),
            TrustedIssuer {
                issuer_peer_id: authority.issuer_peer_id(),
                issuer_public_key_hex: authority.issuer_public_key_hex().to_owned(),
            },
        );
        trusted.len()
    } else {
        auth.trusted_issuers
            .lock()
            .expect("trusted issuer state should not be poisoned")
            .len()
    };

    if let Some(reenrollment) = rollout.reenrollment {
        *auth
            .reenrollment
            .lock()
            .expect("auth reenrollment state should not be poisoned") =
            Some(BootstrapReenrollmentConfig {
                reason: reenrollment.reason,
                rotated_at: reenrollment.rotated_at,
                legacy_issuer_peer_ids: reenrollment.legacy_issuer_peer_ids,
            });
    }

    if let Some(epoch) = effective_minimum_revocation_epoch {
        state
            .lock()
            .expect("bootstrap admin state should not be poisoned")
            .minimum_revocation_epoch = Some(epoch);
    }
    let trust_bundle = sync_trust_bundle(auth, state);

    Ok(burn_p2p_bootstrap::AdminResult::AuthPolicyRolledOut {
        minimum_revocation_epoch: effective_minimum_revocation_epoch,
        directory_entries,
        trusted_issuers: trusted_issuers.max(trust_bundle.issuers.len()),
        reenrollment_required: trust_bundle.reenrollment.is_some(),
    })
}

fn retire_trusted_issuers(
    auth: &AuthPortalState,
    state: &Arc<Mutex<BootstrapAdminState>>,
    issuer_peer_ids: &BTreeSet<PeerId>,
) -> Result<burn_p2p_bootstrap::AdminResult, Box<dyn std::error::Error>> {
    let active_issuer_peer_id = auth
        .authority
        .lock()
        .expect("auth authority should not be poisoned")
        .issuer_peer_id();
    if issuer_peer_ids.contains(&active_issuer_peer_id) {
        return Err("cannot retire the active issuer".into());
    }

    let mut trusted_issuers = auth
        .trusted_issuers
        .lock()
        .expect("trusted issuer state should not be poisoned");
    let previous_len = trusted_issuers.len();
    trusted_issuers.retain(|issuer_peer_id, _| !issuer_peer_ids.contains(issuer_peer_id));
    drop(trusted_issuers);

    let mut reenrollment_state = auth
        .reenrollment
        .lock()
        .expect("auth reenrollment state should not be poisoned");
    if let Some(reenrollment) = reenrollment_state.as_mut() {
        reenrollment
            .legacy_issuer_peer_ids
            .retain(|issuer_peer_id| !issuer_peer_ids.contains(issuer_peer_id));
        if reenrollment.legacy_issuer_peer_ids.is_empty() {
            *reenrollment_state = None;
        }
    }
    drop(reenrollment_state);

    let trust_bundle = sync_trust_bundle(auth, state);
    Ok(burn_p2p_bootstrap::AdminResult::TrustedIssuersRetired {
        retired_issuers: previous_len.saturating_sub(trust_bundle.issuers.len()),
        remaining_issuers: trust_bundle.issuers.len(),
        reenrollment_required: trust_bundle.reenrollment.is_some(),
    })
}

fn publish_admin_result(
    plan: &BootstrapPlan,
    control_handle: Option<&ControlHandle>,
    result: &burn_p2p_bootstrap::AdminResult,
) -> Result<(), Box<dyn std::error::Error>> {
    if let (Some(control_handle), burn_p2p_bootstrap::AdminResult::Control(certificate)) =
        (control_handle, result)
    {
        control_handle.publish_control(ControlAnnouncement {
            overlay: OverlayTopic::control(plan.network_id().clone()),
            certificate: certificate.clone(),
            announced_at: Utc::now(),
        })?;
    }

    Ok(())
}

fn build_auth_portal(
    config: &BootstrapAuthConfig,
    network_id: NetworkId,
    protocol_version: semver::Version,
) -> Result<AuthPortalState, Box<dyn std::error::Error>> {
    let now = Utc::now();
    let provider = match &config.connector {
        BootstrapAuthConnectorConfig::Static => AuthProvider::Static {
            authority: config.authority_name.clone(),
        },
        BootstrapAuthConnectorConfig::GitHub { .. } => AuthProvider::GitHub,
        BootstrapAuthConnectorConfig::Oidc { issuer, .. } => AuthProvider::Oidc {
            issuer: issuer.clone(),
        },
        BootstrapAuthConnectorConfig::OAuth { provider, .. } => AuthProvider::OAuth {
            provider: provider.clone(),
        },
        BootstrapAuthConnectorConfig::External { authority, .. } => AuthProvider::External {
            authority: authority.clone(),
        },
    };
    let principals = config
        .principals
        .iter()
        .map(|principal| {
            Ok((
                principal.principal_id.clone(),
                StaticPrincipalRecord {
                    claims: PrincipalClaims {
                        principal_id: principal.principal_id.clone(),
                        provider: provider.clone(),
                        display_name: principal.display_name.clone(),
                        org_memberships: principal.org_memberships.clone(),
                        group_memberships: principal.group_memberships.clone(),
                        granted_roles: principal.granted_roles.clone(),
                        granted_scopes: principal.granted_scopes.clone(),
                        custom_claims: principal.custom_claims.clone(),
                        issued_at: now,
                        expires_at: now
                            + chrono::Duration::seconds(config.session_ttl_seconds.max(1)),
                    },
                    allowed_networks: principal.allowed_networks.clone(),
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;

    let session_ttl = chrono::Duration::seconds(config.session_ttl_seconds.max(1));
    let connector = match &config.connector {
        BootstrapAuthConnectorConfig::Static => PortalIdentityConnector::new(
            vec![BrowserLoginProvider {
                label: "Static".into(),
                login_path: "/login/static".into(),
                callback_path: Some("/callback/static".into()),
                device_path: None,
            }],
            None,
            Box::new(StaticIdentityConnector::new(
                config.authority_name.clone(),
                session_ttl,
                principals.clone(),
            )),
        ),
        BootstrapAuthConnectorConfig::GitHub {
            authorize_base_url,
            exchange_url,
            token_url,
            client_id,
            client_secret,
            userinfo_url,
            refresh_url,
            revoke_url,
        } => build_github_portal_connector(
            session_ttl,
            principals.clone(),
            PortalConnectorEndpoints {
                authorize_base_url: authorize_base_url.clone(),
                exchange_url: exchange_url.clone(),
                token_url: token_url.clone(),
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
                userinfo_url: userinfo_url.clone(),
                refresh_url: refresh_url.clone(),
                revoke_url: revoke_url.clone(),
            },
        )?,
        BootstrapAuthConnectorConfig::Oidc {
            issuer,
            authorize_base_url,
            exchange_url,
            token_url,
            client_id,
            client_secret,
            userinfo_url,
            refresh_url,
            revoke_url,
        } => build_oidc_portal_connector(
            issuer.clone(),
            session_ttl,
            principals.clone(),
            PortalConnectorEndpoints {
                authorize_base_url: authorize_base_url.clone(),
                exchange_url: exchange_url.clone(),
                token_url: token_url.clone(),
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
                userinfo_url: userinfo_url.clone(),
                refresh_url: refresh_url.clone(),
                revoke_url: revoke_url.clone(),
            },
        )?,
        BootstrapAuthConnectorConfig::OAuth {
            provider,
            authorize_base_url,
            exchange_url,
            token_url,
            client_id,
            client_secret,
            userinfo_url,
            refresh_url,
            revoke_url,
        } => build_oauth_portal_connector(
            provider.clone(),
            session_ttl,
            principals.clone(),
            PortalConnectorEndpoints {
                authorize_base_url: authorize_base_url.clone(),
                exchange_url: exchange_url.clone(),
                token_url: token_url.clone(),
                client_id: client_id.clone(),
                client_secret: client_secret.clone(),
                userinfo_url: userinfo_url.clone(),
                refresh_url: refresh_url.clone(),
                revoke_url: revoke_url.clone(),
            },
        )?,
        BootstrapAuthConnectorConfig::External {
            authority,
            trusted_principal_header,
            ..
        } => build_external_portal_connector(
            authority.clone(),
            trusted_principal_header.clone(),
            session_ttl,
            principals,
        )?,
    };
    let login_providers = connector.login_providers();
    let authority_keypair = load_or_create_keypair(&config.authority_key_path)?;
    let authority = NodeCertificateAuthority::new(
        network_id.clone(),
        config.project_family_id.clone(),
        config.required_release_train_hash.clone(),
        protocol_version.clone(),
        authority_keypair,
        config.issuer_key_id.clone(),
    )?;
    let active_issuer = TrustedIssuer {
        issuer_peer_id: authority.issuer_peer_id(),
        issuer_public_key_hex: authority.issuer_public_key_hex().to_owned(),
    };
    let mut trusted_issuers = config
        .trusted_issuers
        .iter()
        .cloned()
        .map(|issuer| (issuer.issuer_peer_id.clone(), issuer))
        .collect::<BTreeMap<_, _>>();
    trusted_issuers.insert(active_issuer.issuer_peer_id.clone(), active_issuer);

    Ok(AuthPortalState {
        connector,
        login_providers,
        authority_key_path: config.authority_key_path.clone(),
        network_id: network_id.clone(),
        protocol_version,
        issuer_key_id: Mutex::new(config.issuer_key_id.clone()),
        authority: Mutex::new(authority),
        trusted_issuers: Mutex::new(trusted_issuers),
        sessions: Mutex::new(BTreeMap::new()),
        directory: Mutex::new(ExperimentDirectory {
            network_id,
            generated_at: now,
            entries: config.directory_entries.clone(),
        }),
        minimum_revocation_epoch: Mutex::new(RevocationEpoch(config.minimum_revocation_epoch)),
        reenrollment: Mutex::new(config.reenrollment.clone()),
        project_family_id: config.project_family_id.clone(),
        required_release_train_hash: config.required_release_train_hash.clone(),
        allowed_target_artifact_hashes: config.allowed_target_artifact_hashes.clone(),
    })
}

#[derive(Clone, Debug, Default)]
struct PortalConnectorEndpoints {
    authorize_base_url: Option<String>,
    exchange_url: Option<String>,
    token_url: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    userinfo_url: Option<String>,
    refresh_url: Option<String>,
    revoke_url: Option<String>,
}

#[cfg(feature = "auth-github")]
fn build_github_portal_connector(
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Ok(PortalIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: "GitHub".into(),
            login_path: "/login/github".into(),
            callback_path: Some("/callback/github".into()),
            device_path: None,
        }],
        None,
        Box::new(
            GitHubIdentityConnector::new(session_ttl, principals, endpoints.authorize_base_url)
                .with_exchange_url(endpoints.exchange_url)
                .with_token_url(endpoints.token_url)
                .with_client_credentials(endpoints.client_id, endpoints.client_secret)
                .with_userinfo_url(endpoints.userinfo_url)
                .with_refresh_url(endpoints.refresh_url)
                .with_revoke_url(endpoints.revoke_url),
        ),
    ))
}

#[cfg(not(feature = "auth-github"))]
fn build_github_portal_connector(
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    _endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Err(std::io::Error::other("auth-github feature not compiled").into())
}

#[cfg(feature = "auth-oidc")]
fn build_oidc_portal_connector(
    issuer: String,
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Ok(PortalIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: "OIDC".into(),
            login_path: "/login/oidc".into(),
            callback_path: Some("/callback/oidc".into()),
            device_path: Some("/device/oidc".into()),
        }],
        None,
        Box::new(
            OidcIdentityConnector::new(
                issuer,
                session_ttl,
                principals,
                endpoints.authorize_base_url,
            )
            .with_exchange_url(endpoints.exchange_url)
            .with_token_url(endpoints.token_url)
            .with_client_credentials(endpoints.client_id, endpoints.client_secret)
            .with_userinfo_url(endpoints.userinfo_url)
            .with_refresh_url(endpoints.refresh_url)
            .with_revoke_url(endpoints.revoke_url),
        ),
    ))
}

#[cfg(not(feature = "auth-oidc"))]
fn build_oidc_portal_connector(
    _issuer: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    _endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Err(std::io::Error::other("auth-oidc feature not compiled").into())
}

#[cfg(feature = "auth-oauth")]
fn build_oauth_portal_connector(
    provider: String,
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Ok(PortalIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: "OAuth".into(),
            login_path: "/login/oauth".into(),
            callback_path: Some("/callback/oauth".into()),
            device_path: Some("/device/oauth".into()),
        }],
        None,
        Box::new(
            OAuthIdentityConnector::new(
                provider,
                session_ttl,
                principals,
                endpoints.authorize_base_url,
            )
            .with_exchange_url(endpoints.exchange_url)
            .with_token_url(endpoints.token_url)
            .with_client_credentials(endpoints.client_id, endpoints.client_secret)
            .with_userinfo_url(endpoints.userinfo_url)
            .with_refresh_url(endpoints.refresh_url)
            .with_revoke_url(endpoints.revoke_url),
        ),
    ))
}

#[cfg(not(feature = "auth-oauth"))]
fn build_oauth_portal_connector(
    _provider: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
    _endpoints: PortalConnectorEndpoints,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Err(std::io::Error::other("auth-oauth feature not compiled").into())
}

#[cfg(feature = "auth-external")]
fn build_external_portal_connector(
    authority: String,
    trusted_principal_header: String,
    session_ttl: chrono::Duration,
    principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Ok(PortalIdentityConnector::new(
        vec![BrowserLoginProvider {
            label: format!("External ({authority})"),
            login_path: "/login/external".into(),
            callback_path: Some("/callback/external".into()),
            device_path: None,
        }],
        Some(trusted_principal_header.clone()),
        Box::new(ExternalProxyIdentityConnector::new(
            authority,
            trusted_principal_header,
            session_ttl,
            principals,
        )),
    ))
}

#[cfg(not(feature = "auth-external"))]
fn build_external_portal_connector(
    _authority: String,
    _trusted_principal_header: String,
    _session_ttl: chrono::Duration,
    _principals: BTreeMap<PrincipalId, StaticPrincipalRecord>,
) -> Result<PortalIdentityConnector, Box<dyn std::error::Error>> {
    Err(std::io::Error::other("auth-external feature not compiled").into())
}

fn auth_directory_entries(
    auth: &AuthPortalState,
    request: &HttpRequest,
) -> Result<Vec<ExperimentDirectoryEntry>, Box<dyn std::error::Error>> {
    let scopes = request
        .headers
        .get("x-session-id")
        .and_then(|session_id| {
            auth.sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .get(&ContentId::new(session_id.clone()))
                .cloned()
        })
        .map(|session| session.claims.granted_scopes)
        .unwrap_or_default();

    let directory = auth
        .directory
        .lock()
        .expect("auth directory should not be poisoned");
    Ok(directory.visible_to(&scopes).into_iter().cloned().collect())
}

fn session_allows_receipt_submission(
    session: &PrincipalSession,
    receipt: &burn_p2p::ContributionReceipt,
) -> bool {
    session
        .claims
        .granted_scopes
        .iter()
        .any(|scope| match scope {
            ExperimentScope::Connect => true,
            ExperimentScope::Train { experiment_id }
            | ExperimentScope::Validate { experiment_id } => {
                experiment_id == &receipt.experiment_id
            }
            _ => false,
        })
}

fn load_or_create_keypair(path: &Path) -> Result<Keypair, Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    if path.exists() {
        let bytes = std::fs::read(path)?;
        return Ok(Keypair::from_protobuf_encoding(&bytes)?);
    }

    let keypair = Keypair::generate_ed25519();
    std::fs::write(path, keypair.to_protobuf_encoding()?)?;
    Ok(keypair)
}

fn ensure_synthetic_dataset(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(root)?;
    let project = SyntheticBootstrapProject {
        dataset_root: root.to_path_buf(),
        learning_rate: 1.0,
        target_model: 1.0,
    };
    let registration = project.dataset_registration()?;
    let plan = project.microshard_plan(&registration)?;
    let manifest = root.join("fetch-manifest.json");
    if !manifest.exists() {
        let manifest_value = ShardFetchManifest::from_microshards(
            &plan.dataset_view,
            &plan.microshards,
            |ordinal| match ordinal {
                0 => b"3.5".to_vec(),
                _ => b"6.5".to_vec(),
            },
        );
        std::fs::write(&manifest, serde_json::to_vec_pretty(&manifest_value)?)?;
        for entry in &manifest_value.entries {
            let bytes = match entry.ordinal {
                0 => b"3.5".to_vec(),
                _ => b"6.5".to_vec(),
            };
            std::fs::write(root.join(&entry.locator), bytes)?;
        }
    }
    Ok(())
}

fn token_matches(
    request: &HttpRequest,
    admin_token: Option<&str>,
    allow_dev_admin_token: bool,
) -> bool {
    if !allow_dev_admin_token {
        return false;
    }
    match admin_token {
        Some(expected) => request
            .headers
            .get("x-admin-token")
            .is_some_and(|value| value == expected),
        None => false,
    }
}

fn export_admin_capabilities() -> BTreeSet<AdminCapability> {
    BTreeSet::from([
        AdminCapability::ExportDiagnostics,
        AdminCapability::ExportDiagnosticsBundle,
        AdminCapability::ExportHeads,
        AdminCapability::ExportReceipts,
        AdminCapability::ExportReducerLoad,
        AdminCapability::ExportTrustBundle,
    ])
}

fn operator_admin_capabilities() -> BTreeSet<AdminCapability> {
    let mut capabilities = export_admin_capabilities();
    capabilities.insert(AdminCapability::Control);
    capabilities.insert(AdminCapability::BanPeer);
    capabilities
}

fn all_admin_capabilities() -> BTreeSet<AdminCapability> {
    BTreeSet::from([
        AdminCapability::Control,
        AdminCapability::BanPeer,
        AdminCapability::ExportDiagnostics,
        AdminCapability::ExportDiagnosticsBundle,
        AdminCapability::ExportHeads,
        AdminCapability::ExportReceipts,
        AdminCapability::ExportReducerLoad,
        AdminCapability::ExportTrustBundle,
        AdminCapability::RolloutAuthPolicy,
        AdminCapability::RetireTrustedIssuers,
        AdminCapability::RotateAuthorityMaterial,
    ])
}

fn parse_admin_capability_token(token: &str) -> Option<AdminCapability> {
    let normalized = token.trim().to_ascii_lowercase().replace(['-', ' '], "_");
    match normalized.as_str() {
        "control" => Some(AdminCapability::Control),
        "banpeer" | "ban_peer" => Some(AdminCapability::BanPeer),
        "exportdiagnostics" | "export_diagnostics" => Some(AdminCapability::ExportDiagnostics),
        "exportdiagnosticsbundle" | "export_diagnostics_bundle" => {
            Some(AdminCapability::ExportDiagnosticsBundle)
        }
        "exportheads" | "export_heads" => Some(AdminCapability::ExportHeads),
        "exportreceipts" | "export_receipts" => Some(AdminCapability::ExportReceipts),
        "exportreducerload" | "export_reducer_load" => Some(AdminCapability::ExportReducerLoad),
        "exporttrustbundle" | "export_trust_bundle" => Some(AdminCapability::ExportTrustBundle),
        "rolloutauthpolicy" | "rollout_auth_policy" => Some(AdminCapability::RolloutAuthPolicy),
        "retiretrustedissuers" | "retire_trusted_issuers" => {
            Some(AdminCapability::RetireTrustedIssuers)
        }
        "rotateauthoritymaterial" | "rotate_authority_material" => {
            Some(AdminCapability::RotateAuthorityMaterial)
        }
        _ => None,
    }
}

fn session_admin_capabilities(session: &PrincipalSession) -> BTreeSet<AdminCapability> {
    let mut capabilities = BTreeSet::new();
    let custom_claims = &session.claims.custom_claims;

    if session.claims.group_memberships.contains("admins") {
        capabilities.extend(all_admin_capabilities());
    } else if session.claims.group_memberships.contains("operators") {
        capabilities.extend(operator_admin_capabilities());
    }

    if let Some(role) = custom_claims
        .get("operator_role")
        .or_else(|| custom_claims.get("admin_role"))
    {
        match role.trim().to_ascii_lowercase().as_str() {
            "viewer" | "read_only" | "readonly" => {
                capabilities.extend(export_admin_capabilities());
            }
            "operator" => {
                capabilities.extend(operator_admin_capabilities());
            }
            "admin" | "authority_admin" => {
                capabilities.extend(all_admin_capabilities());
            }
            _ => {}
        }
    }

    if let Some(tokens) = custom_claims
        .get("admin_capabilities")
        .or_else(|| custom_claims.get("operator_capabilities"))
    {
        if tokens
            .split(',')
            .any(|token| matches!(token.trim().to_ascii_lowercase().as_str(), "*" | "all"))
        {
            return all_admin_capabilities();
        }
        for token in tokens.split(',') {
            if let Some(capability) = parse_admin_capability_token(token) {
                capabilities.insert(capability);
            }
        }
    }

    capabilities
}

fn request_admin_capabilities(
    request: &HttpRequest,
    auth_state: Option<&Arc<AuthPortalState>>,
) -> Option<BTreeSet<AdminCapability>> {
    let auth = auth_state?;
    let session_id = request.headers.get("x-session-id")?;
    let session = auth
        .sessions
        .lock()
        .expect("auth session state should not be poisoned")
        .get(&ContentId::new(session_id.clone()))
        .cloned()?;
    Some(session_admin_capabilities(&session))
}

fn write_event_stream(
    stream: &mut TcpStream,
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
    remaining_work_units: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n",
    )?;
    stream.flush()?;

    loop {
        let diagnostics = current_diagnostics(plan, state, remaining_work_units);
        let payload = serde_json::to_string(&diagnostics)?;
        stream.write_all(format!("data: {payload}\n\n").as_bytes())?;
        stream.flush()?;
        thread::sleep(Duration::from_secs(1));
    }
}

fn read_request(stream: &TcpStream) -> Result<Option<HttpRequest>, Box<dyn std::error::Error>> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(None);
    }

    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or("missing method")?.to_owned();
    let path = parts.next().ok_or("missing path")?.to_owned();
    let mut headers = BTreeMap::new();

    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line == "\r\n" || line.is_empty() {
            break;
        }

        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }

    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0_u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    Ok(Some(HttpRequest {
        method,
        path,
        headers,
        body,
    }))
}

fn write_json(
    stream: &mut TcpStream,
    value: &impl Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    write_response(
        stream,
        "200 OK",
        "application/json; charset=utf-8",
        serde_json::to_vec_pretty(value)?,
    )?;
    Ok(())
}

fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.write_all(
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    )?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BootstrapAuthConfig, BootstrapAuthConnectorConfig, BootstrapAuthPrincipal,
        BootstrapDaemonConfig, BootstrapOptionalServicesConfig, HttpRequest, HttpServerContext,
        auth_directory_entries, build_auth_portal, current_revocation_epoch, current_trust_bundle,
        default_issuer_key_id, handle_connection, load_or_create_keypair, persist_daemon_config,
        retire_trusted_issuers, rollout_auth_policy, rotate_authority_material,
        validate_compiled_feature_support_with,
    };
    use std::{
        collections::{BTreeMap, BTreeSet},
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration as StdDuration,
    };

    use burn_p2p::{
        BrowserMode, CallbackPayload, CompiledFeatureSet, ContributionReceipt, EdgeFeature,
        ExperimentDirectoryEntry, ExperimentDirectoryPolicyExt, ExperimentOptInPolicy,
        ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, HeadDescriptor,
        IdentityConnector, LoginRequest, MetricValue, NodeEnrollmentRequest, OverlayChannel,
        OverlayTopic, PeerId, PeerRole, PeerRoleSet, PrincipalId, ProfileMode,
        ReducerLoadAnnouncement, SocialMode, TrustedIssuer,
    };
    use burn_p2p_bootstrap::{
        AdminApiPlan, ArchivePlan, AuthPolicyRollout, BootstrapAdminState, BootstrapPreset,
        BootstrapSpec,
    };
    use burn_p2p_browser::{
        BrowserCapabilityReport, BrowserEnrollmentConfig, BrowserPortalClient,
        BrowserRuntimeConfig, BrowserRuntimeRole, BrowserRuntimeState, BrowserSessionState,
        BrowserTransportStatus, BrowserUiBindings, BrowserValidationPlan, BrowserWorkerCommand,
        BrowserWorkerEvent, BrowserWorkerIdentity, BrowserWorkerRuntime,
    };
    use burn_p2p_core::{AuthProvider, ClientPlatform, ContentId, NetworkId, RevocationEpoch};
    use burn_p2p_security::{PeerAdmissionReport, PeerTrustLevel};
    use chrono::{Duration, Utc};
    use libp2p_identity::Keypair;
    use semver::Version;
    use serde_json::Value;
    use tempfile::tempdir;

    struct IssueRequestSpec<'a> {
        method: &'a str,
        path: &'a str,
        body: Option<Value>,
        headers: &'a [(&'a str, &'a str)],
    }

    struct HttpTestServer {
        base_url: String,
        stop: Arc<AtomicBool>,
        thread: Option<thread::JoinHandle<()>>,
    }

    impl HttpTestServer {
        fn spawn(context: HttpServerContext) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
            listener
                .set_nonblocking(true)
                .expect("set nonblocking listener");
            let addr = listener.local_addr().expect("local addr");
            let stop = Arc::new(AtomicBool::new(false));
            let stop_flag = stop.clone();
            let thread = thread::spawn(move || {
                while !stop_flag.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            handle_connection(stream, context.clone()).expect("handle connection");
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(StdDuration::from_millis(10));
                        }
                        Err(error) => panic!("accept connection: {error}"),
                    }
                }
            });
            Self {
                base_url: format!("http://{addr}"),
                stop,
                thread: Some(thread),
            }
        }

        fn base_url(&self) -> &str {
            &self.base_url
        }
    }

    impl Drop for HttpTestServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            let _ = TcpStream::connect(self.base_url.trim_start_matches("http://"));
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    fn spawn_provider_json_server(
        assert_request: impl Fn(&str) + Send + 'static,
        response_status: &'static str,
        response_body: String,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind provider exchange listener");
        let addr = listener.local_addr().expect("local addr");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept provider exchange");
            stream
                .set_read_timeout(Some(StdDuration::from_secs(2)))
                .expect("set provider exchange read timeout");
            let mut buffer = [0_u8; 8192];
            let bytes_read = stream
                .read(&mut buffer)
                .expect("read provider exchange request");
            let request = String::from_utf8_lossy(&buffer[..bytes_read]).to_string();
            assert_request(&request);
            let response = format!(
                "HTTP/1.1 {response_status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write provider exchange response");
            stream.flush().expect("flush provider exchange response");
        });
        (format!("http://{addr}"), handle)
    }

    fn sample_auth_config(root: &std::path::Path) -> BootstrapAuthConfig {
        BootstrapAuthConfig {
            authority_name: "local-auth".into(),
            connector: BootstrapAuthConnectorConfig::Static,
            authority_key_path: root.join("authority.key"),
            issuer_key_id: default_issuer_key_id(),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            required_release_train_hash: ContentId::new("demo-train"),
            allowed_target_artifact_hashes: BTreeSet::from([ContentId::new(
                "demo-artifact-native",
            )]),
            session_ttl_seconds: 300,
            minimum_revocation_epoch: 1,
            principals: vec![BootstrapAuthPrincipal {
                principal_id: PrincipalId::new("alice"),
                display_name: "Alice".into(),
                org_memberships: BTreeSet::from(["burn-lab".into()]),
                group_memberships: BTreeSet::from(["trainers".into()]),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu, PeerRole::Evaluator]),
                granted_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Discover,
                    ExperimentScope::Train {
                        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                    },
                ]),
                allowed_networks: BTreeSet::from([NetworkId::new("secure-demo")]),
                custom_claims: BTreeMap::from([
                    ("team".into(), "research".into()),
                    ("operator_role".into(), "admin".into()),
                ]),
            }],
            directory_entries: vec![
                ExperimentDirectoryEntry {
                    network_id: NetworkId::new("secure-demo"),
                    study_id: burn_p2p::StudyId::new("study-auth"),
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                    workload_id: burn_p2p::WorkloadId::new("auth-demo"),
                    display_name: "Authenticated Demo".into(),
                    model_schema_hash: burn_p2p::ContentId::new("model-auth"),
                    dataset_view_id: burn_p2p::DatasetViewId::new("view-auth"),
                    resource_requirements: ExperimentResourceRequirements {
                        minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
                        minimum_device_memory_bytes: Some(1024),
                        minimum_system_memory_bytes: Some(4096),
                        estimated_download_bytes: 65_536,
                        estimated_window_seconds: 45,
                    },
                    visibility: ExperimentVisibility::OptIn,
                    opt_in_policy: ExperimentOptInPolicy::Scoped,
                    current_revision_id: burn_p2p::RevisionId::new("rev-auth"),
                    current_head_id: None,
                    allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
                    allowed_scopes: BTreeSet::from([ExperimentScope::Train {
                        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                    }]),
                    metadata: BTreeMap::from([("owner".into(), "burn-lab".into())]),
                },
                ExperimentDirectoryEntry {
                    network_id: NetworkId::new("secure-demo"),
                    study_id: burn_p2p::StudyId::new("study-auth"),
                    experiment_id: burn_p2p::ExperimentId::new("exp-hidden"),
                    workload_id: burn_p2p::WorkloadId::new("hidden-demo"),
                    display_name: "Hidden".into(),
                    model_schema_hash: burn_p2p::ContentId::new("model-hidden"),
                    dataset_view_id: burn_p2p::DatasetViewId::new("view-hidden"),
                    resource_requirements: ExperimentResourceRequirements {
                        minimum_roles: BTreeSet::from([PeerRole::Validator]),
                        minimum_device_memory_bytes: Some(2048),
                        minimum_system_memory_bytes: Some(8192),
                        estimated_download_bytes: 131_072,
                        estimated_window_seconds: 60,
                    },
                    visibility: ExperimentVisibility::InviteOnly,
                    opt_in_policy: ExperimentOptInPolicy::Scoped,
                    current_revision_id: burn_p2p::RevisionId::new("rev-hidden"),
                    current_head_id: None,
                    allowed_roles: PeerRoleSet::new([PeerRole::Validator]),
                    allowed_scopes: BTreeSet::from([ExperimentScope::Validate {
                        experiment_id: burn_p2p::ExperimentId::new("exp-hidden"),
                    }]),
                    metadata: BTreeMap::new(),
                },
            ],
            trusted_issuers: Vec::new(),
            reenrollment: None,
        }
    }

    fn sample_auth_config_with_connector(
        root: &std::path::Path,
        connector: BootstrapAuthConnectorConfig,
    ) -> BootstrapAuthConfig {
        let mut config = sample_auth_config(root);
        config.connector = connector;
        config
    }

    fn sample_spec() -> BootstrapSpec {
        BootstrapSpec {
            preset: BootstrapPreset::BootstrapOnly,
            genesis: burn_p2p::GenesisSpec {
                network_id: NetworkId::new("secure-demo"),
                protocol_version: Version::new(0, 1, 0),
                display_name: "Secure Demo".into(),
                created_at: Utc::now(),
                metadata: BTreeMap::new(),
            },
            platform: ClientPlatform::Native,
            bootstrap_addresses: Vec::new(),
            listen_addresses: Vec::new(),
            authority: None,
            archive: ArchivePlan::default(),
            admin_api: AdminApiPlan::default(),
        }
    }

    fn issue_request(context: HttpServerContext, request: IssueRequestSpec<'_>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let addr = listener.local_addr().expect("local addr");
        let server_context = context.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept connection");
            handle_connection(stream, server_context).expect("handle connection");
        });

        let mut stream = TcpStream::connect(addr).expect("connect client");
        let payload = request
            .body
            .map(|value| serde_json::to_vec(&value).expect("serialize body"))
            .unwrap_or_default();
        let mut wire = format!(
            "{} {} HTTP/1.1\r\nHost: localhost\r\n",
            request.method, request.path
        );
        if let Some(admin_token) = context.admin_token.as_deref() {
            wire.push_str(&format!("x-admin-token: {admin_token}\r\n"));
        }
        for (name, value) in request.headers {
            wire.push_str(&format!("{name}: {value}\r\n"));
        }
        if !payload.is_empty() {
            wire.push_str("Content-Type: application/json\r\n");
            wire.push_str(&format!("Content-Length: {}\r\n", payload.len()));
        }
        wire.push_str("\r\n");
        stream
            .write_all(wire.as_bytes())
            .expect("write request headers");
        if !payload.is_empty() {
            stream.write_all(&payload).expect("write request body");
        }
        stream
            .shutdown(std::net::Shutdown::Write)
            .expect("shutdown write");

        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        server.join().expect("join server");
        response
    }

    fn response_json(response: &str) -> serde_json::Value {
        let (_, body) = response
            .split_once("\r\n\r\n")
            .expect("split response headers");
        serde_json::from_str(body).expect("deserialize response body")
    }

    fn response_body(response: &str) -> &str {
        let (_, body) = response
            .split_once("\r\n\r\n")
            .expect("split response headers");
        body
    }

    #[test]
    fn browser_portal_client_round_trips_against_live_http_router() {
        let temp = tempdir().expect("temp dir");
        let auth = Arc::new(
            build_auth_portal(
                &sample_auth_config(temp.path()),
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build auth portal"),
        );
        let context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("browser-client-e2e.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(auth),
            control_handle: None,
        };
        let server = HttpTestServer::spawn(context);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let snapshot: burn_p2p_bootstrap::BrowserPortalSnapshot = reqwest::Client::new()
                .get(format!("{}/portal/snapshot", server.base_url()))
                .send()
                .await
                .expect("fetch portal snapshot")
                .error_for_status()
                .expect("portal snapshot status")
                .json()
                .await
                .expect("decode portal snapshot");
            let bindings = BrowserUiBindings::from_portal_snapshot(server.base_url(), &snapshot);
            let enrollment = BrowserEnrollmentConfig {
                network_id: NetworkId::new("secure-demo"),
                project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
                release_train_hash: ContentId::new("demo-train"),
                target_artifact_id: "native-test-client".into(),
                target_artifact_hash: ContentId::new("demo-artifact-native"),
                login_path: "/login/static".into(),
                callback_path: "/callback/static".into(),
                enroll_path: "/enroll".into(),
                trust_bundle_path: "/trust".into(),
                requested_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Train {
                        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                    },
                ]),
                session_ttl_secs: 300,
            };
            let client = BrowserPortalClient::new(bindings, enrollment);

            let login = client
                .begin_login(Some("alice".into()))
                .await
                .expect("begin login");
            let session = client
                .complete_static_login(&login, PrincipalId::new("alice"))
                .await
                .expect("complete static login");
            let trust_bundle = client.fetch_trust_bundle().await.expect("trust bundle");
            assert_eq!(trust_bundle.network_id.as_str(), "secure-demo");

            let enrolled = client
                .enroll_static_principal(
                    Some("alice".into()),
                    PrincipalId::new("alice"),
                    &BrowserWorkerIdentity {
                        peer_id: PeerId::new("browser-http-peer"),
                        peer_public_key_hex: "001122".into(),
                        serial: 7,
                        client_policy_hash: Some(ContentId::new("policy-browser-http")),
                    },
                )
                .await
                .expect("enroll static principal");
            assert_eq!(
                enrolled.certificate.claims().peer_id.as_str(),
                "browser-http-peer"
            );

            let directory = client
                .fetch_directory(Some(&session.session_id))
                .await
                .expect("fetch directory");
            assert_eq!(directory.len(), 1);
            assert_eq!(directory[0].experiment_id.as_str(), "exp-auth");

            let signed_directory = client
                .fetch_signed_directory(Some(&session.session_id))
                .await
                .expect("fetch signed directory");
            assert_eq!(
                signed_directory.payload.schema,
                "burn_p2p.browser_directory_snapshot"
            );

            let signed_leaderboard = client
                .fetch_signed_leaderboard()
                .await
                .expect("fetch signed leaderboard");
            assert_eq!(
                signed_leaderboard.payload.schema,
                "burn_p2p.browser_leaderboard_snapshot"
            );

            let receipt = ContributionReceipt {
                receipt_id: burn_p2p::ContributionReceiptId::new("receipt-browser-http"),
                peer_id: PeerId::new("browser-http-peer"),
                study_id: burn_p2p::StudyId::new("study-auth"),
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                revision_id: burn_p2p::RevisionId::new("rev-auth"),
                base_head_id: burn_p2p::HeadId::new("head-auth"),
                artifact_id: burn_p2p::ArtifactId::new("artifact-browser-http"),
                accepted_at: Utc::now(),
                accepted_weight: 2.0,
                metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.2))]),
                merge_cert_id: None,
            };
            let submission = client
                .submit_receipts(&session.session_id, &[receipt])
                .await
                .expect("submit browser receipts");
            assert_eq!(submission.accepted_receipt_ids.len(), 1);
            assert_eq!(
                submission.accepted_receipt_ids[0].as_str(),
                "receipt-browser-http"
            );

            let refreshed = client
                .refresh_session(&session.session_id)
                .await
                .expect("refresh session");
            assert_eq!(refreshed.claims.principal_id.as_str(), "alice");

            let logout = client
                .logout_session(&refreshed.session_id)
                .await
                .expect("logout session");
            assert!(logout.logged_out);
        });
    }

    #[test]
    fn browser_portal_client_syncs_worker_runtime_and_flushes_receipts_against_live_http_router() {
        let temp = tempdir().expect("temp dir");
        let mut auth_config = sample_auth_config(temp.path());
        auth_config.directory_entries[0].current_head_id = Some(burn_p2p::HeadId::new("head-auth"));
        let slot_requirements = auth_config.directory_entries[0]
            .resource_requirements
            .clone();
        auth_config.directory_entries[0].apply_revision_policy(&burn_p2p::RevisionManifest {
            experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            revision_id: burn_p2p::RevisionId::new("rev-auth"),
            workload_id: burn_p2p::WorkloadId::new("auth-demo"),
            required_release_train_hash: ContentId::new("demo-train"),
            model_schema_hash: burn_p2p::ContentId::new("model-auth"),
            checkpoint_format_hash: ContentId::new("checkpoint-auth"),
            dataset_view_id: burn_p2p::DatasetViewId::new("view-auth"),
            training_config_hash: ContentId::new("training-auth"),
            merge_topology_policy_hash: ContentId::new("topology-auth"),
            slot_requirements,
            activation_window: burn_p2p::WindowActivation {
                activation_window: burn_p2p::WindowId(1),
                grace_windows: 0,
            },
            lag_policy: burn_p2p::LagPolicy::default(),
            merge_window_miss_policy: burn_p2p::MergeWindowMissPolicy::LeaseBlocked,
            browser_enabled: true,
            browser_role_policy: burn_p2p::BrowserRolePolicy {
                observer: true,
                verifier: true,
                trainer_wgpu: false,
                fallback: true,
            },
            max_browser_checkpoint_bytes: Some(65_536),
            max_browser_window_secs: Some(45),
            max_browser_shard_bytes: Some(32_768),
            requires_webgpu: false,
            max_browser_batch_size: Some(8),
            recommended_browser_precision: None,
            visibility_policy: burn_p2p::BrowserVisibilityPolicy::SwarmEligible,
            description: "authenticated browser demo".into(),
        });
        let auth = Arc::new(
            build_auth_portal(
                &auth_config,
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build auth portal"),
        );
        let context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState {
                head_descriptors: vec![HeadDescriptor {
                    head_id: burn_p2p::HeadId::new("head-auth"),
                    study_id: burn_p2p::StudyId::new("study-auth"),
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                    revision_id: burn_p2p::RevisionId::new("rev-auth"),
                    artifact_id: burn_p2p::ArtifactId::new("artifact-head-auth"),
                    parent_head_id: None,
                    global_step: 7,
                    created_at: Utc::now(),
                    metrics: BTreeMap::new(),
                }],
                ..BootstrapAdminState::default()
            })),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("browser-runtime-sync.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(auth),
            control_handle: None,
        };
        let server = HttpTestServer::spawn(context);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let snapshot: burn_p2p_bootstrap::BrowserPortalSnapshot = reqwest::Client::new()
                .get(format!("{}/portal/snapshot", server.base_url()))
                .send()
                .await
                .expect("fetch portal snapshot")
                .error_for_status()
                .expect("portal snapshot status")
                .json()
                .await
                .expect("decode portal snapshot");
            let bindings = BrowserUiBindings::from_portal_snapshot(server.base_url(), &snapshot);
            let enrollment = BrowserEnrollmentConfig {
                network_id: NetworkId::new("secure-demo"),
                project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
                release_train_hash: ContentId::new("demo-train"),
                target_artifact_id: "browser-wasm".into(),
                target_artifact_hash: ContentId::new("demo-artifact-native"),
                login_path: "/login/static".into(),
                callback_path: "/callback/static".into(),
                enroll_path: "/enroll".into(),
                trust_bundle_path: "/trust".into(),
                requested_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Train {
                        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                    },
                ]),
                session_ttl_secs: 300,
            };
            let client = BrowserPortalClient::new(bindings, enrollment);
            let enrolled = client
                .enroll_static_principal(
                    Some("alice".into()),
                    PrincipalId::new("alice"),
                    &BrowserWorkerIdentity {
                        peer_id: PeerId::new("browser-http-peer"),
                        peer_public_key_hex: "001122".into(),
                        serial: 7,
                        client_policy_hash: Some(ContentId::new("policy-browser-http")),
                    },
                )
                .await
                .expect("enroll static principal");
            let mut session_state = BrowserSessionState::default();
            session_state.apply_enrollment(&enrolled);

            let mut worker = BrowserWorkerRuntime::start(
                BrowserRuntimeConfig {
                    role: BrowserRuntimeRole::BrowserObserver,
                    ..BrowserRuntimeConfig::new(
                        server.base_url(),
                        NetworkId::new("secure-demo"),
                        ContentId::new("demo-train"),
                        "browser-wasm",
                        ContentId::new("demo-artifact-native"),
                    )
                },
                BrowserCapabilityReport::default(),
                BrowserTransportStatus {
                    active: None,
                    webrtc_direct_enabled: false,
                    webtransport_enabled: true,
                    wss_fallback_enabled: true,
                    last_error: None,
                },
            );
            worker.remember_session(session_state.clone());

            let sync_events = client
                .sync_worker_runtime(&mut worker, Some(&session_state), true)
                .await
                .expect("sync worker runtime");
            assert_eq!(worker.state, Some(BrowserRuntimeState::Observer));
            assert_eq!(
                worker.storage.last_head_id,
                Some(burn_p2p::HeadId::new("head-auth"))
            );
            assert!(sync_events.iter().any(|event| matches!(
                event,
                BrowserWorkerEvent::HeadUpdated { head_id }
                    if head_id == &burn_p2p::HeadId::new("head-auth")
            )));

            let verify_events = worker.apply_command(
                BrowserWorkerCommand::Verify(BrowserValidationPlan {
                    head_id: burn_p2p::HeadId::new("head-auth"),
                    max_checkpoint_bytes: 65_536,
                    sample_budget: 3,
                    emit_receipt: true,
                }),
                None,
                None,
            );
            assert!(
                verify_events
                    .iter()
                    .any(|event| matches!(event, BrowserWorkerEvent::ValidationCompleted(_)))
            );

            let flush_events = client
                .flush_worker_receipts(&mut worker)
                .await
                .expect("flush worker receipts");
            assert!(flush_events.iter().any(|event| matches!(
                event,
                BrowserWorkerEvent::ReceiptsAcknowledged {
                    pending_receipts,
                    ..
                } if *pending_receipts == 0
            )));

            let signed_leaderboard = client
                .fetch_signed_leaderboard()
                .await
                .expect("fetch signed leaderboard after receipt");
            assert_eq!(
                signed_leaderboard.payload.schema,
                "burn_p2p.browser_leaderboard_snapshot"
            );
        });
    }

    #[test]
    fn browser_portal_client_completes_github_login_via_exchange_callback() {
        let (exchange_url, exchange_server) = spawn_provider_json_server(
            |request| {
                assert!(request.contains("\"provider_code\":\"github-code-123\""));
            },
            "200 OK",
            serde_json::json!({
                "principal_id": "alice",
                "provider_subject": "github-user-42",
                "access_token": "access-token-1",
                "session_handle": "session-handle-1"
            })
            .to_string(),
        );
        let (userinfo_url, userinfo_server) = spawn_provider_json_server(
            |request| {
                assert!(request.contains("\"access_token\":\"access-token-1\""));
                assert!(request.contains("\"session_handle\":\"session-handle-1\""));
            },
            "200 OK",
            serde_json::json!({
                "display_name": "Alice Browser",
                "org_memberships": ["oss"],
                "custom_claims": {
                    "avatar_url": "https://avatars.example/alice.png"
                }
            })
            .to_string(),
        );
        let temp = tempdir().expect("temp dir");
        let auth = Arc::new(
            build_auth_portal(
                &sample_auth_config_with_connector(
                    temp.path(),
                    BootstrapAuthConnectorConfig::GitHub {
                        authorize_base_url: Some(
                            "https://github.example/login/oauth/authorize".into(),
                        ),
                        exchange_url: Some(exchange_url),
                        token_url: None,
                        client_id: None,
                        client_secret: None,
                        userinfo_url: Some(userinfo_url),
                        refresh_url: None,
                        revoke_url: None,
                    },
                ),
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build github auth portal"),
        );
        let context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("github-browser-client-exchange.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(auth),
            control_handle: None,
        };
        let server = HttpTestServer::spawn(context);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let snapshot: burn_p2p_bootstrap::BrowserPortalSnapshot = reqwest::Client::new()
                .get(format!("{}/portal/snapshot", server.base_url()))
                .send()
                .await
                .expect("fetch portal snapshot")
                .error_for_status()
                .expect("portal snapshot status")
                .json()
                .await
                .expect("decode portal snapshot");
            assert_eq!(snapshot.login_providers[0].login_path, "/login/github");
            let bindings = BrowserUiBindings::from_portal_snapshot(server.base_url(), &snapshot);
            let enrollment = BrowserEnrollmentConfig {
                network_id: NetworkId::new("secure-demo"),
                project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
                release_train_hash: ContentId::new("demo-train"),
                target_artifact_id: "browser-wasm".into(),
                target_artifact_hash: ContentId::new("demo-artifact-native"),
                login_path: "/login/github".into(),
                callback_path: "/callback/github".into(),
                enroll_path: "/enroll".into(),
                trust_bundle_path: "/trust".into(),
                requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
                session_ttl_secs: 300,
            };
            let client = BrowserPortalClient::new(bindings, enrollment);
            let login = client
                .begin_login(Some("alice".into()))
                .await
                .expect("begin github login");
            let session = client
                .complete_provider_login(&login, "github-code-123")
                .await
                .expect("complete github login");
            assert_eq!(session.claims.principal_id.as_str(), "alice");
            assert_eq!(session.claims.provider, AuthProvider::GitHub);
            assert_eq!(session.claims.display_name, "Alice Browser");
            assert!(session.claims.org_memberships.contains("oss"));
            assert_eq!(
                session.claims.custom_claims.get("avatar_url"),
                Some(&"https://avatars.example/alice.png".to_owned())
            );
        });
        exchange_server
            .join()
            .expect("join provider exchange server");
        userinfo_server
            .join()
            .expect("join provider userinfo server");
    }

    #[test]
    fn browser_portal_client_completes_github_login_via_upstream_token_exchange() {
        let (token_url, token_server) = spawn_provider_json_server(
            |request| {
                assert!(request.contains("grant_type=authorization_code"));
                assert!(request.contains("code=github-upstream-code"));
                assert!(request.contains("client_id=github-client"));
                assert!(request.contains("client_secret=github-secret"));
            },
            "200 OK",
            serde_json::json!({
                "access_token": "access-token-1",
                "refresh_token": "refresh-token-1",
                "expires_in": 3600
            })
            .to_string(),
        );
        let (userinfo_url, userinfo_server) = spawn_provider_json_server(
            |request| {
                let request = request.to_ascii_lowercase();
                assert!(request.contains("authorization: bearer access-token-1"));
            },
            "200 OK",
            serde_json::json!({
                "id": 42,
                "login": "alice-gh",
                "email": "alice@example.com",
                "name": "Alice Upstream Browser",
                "organizations": ["oss"],
                "groups": ["maintainers"],
                "avatar_url": "https://avatars.example/alice-upstream.png"
            })
            .to_string(),
        );
        let temp = tempdir().expect("temp dir");
        let mut auth_config = sample_auth_config_with_connector(
            temp.path(),
            BootstrapAuthConnectorConfig::GitHub {
                authorize_base_url: Some("https://github.com/login/oauth/authorize".into()),
                exchange_url: None,
                token_url: Some(token_url),
                client_id: Some("github-client".into()),
                client_secret: Some("github-secret".into()),
                userinfo_url: Some(userinfo_url),
                refresh_url: None,
                revoke_url: None,
            },
        );
        auth_config.principals[0]
            .custom_claims
            .insert("provider_login".into(), "alice-gh".into());
        auth_config.principals[0]
            .custom_claims
            .insert("provider_email".into(), "alice@example.com".into());

        let auth = Arc::new(
            build_auth_portal(
                &auth_config,
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build github upstream auth portal"),
        );
        let context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("github-browser-client-upstream.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(auth),
            control_handle: None,
        };
        let server = HttpTestServer::spawn(context);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let snapshot: burn_p2p_bootstrap::BrowserPortalSnapshot = reqwest::Client::new()
                .get(format!("{}/portal/snapshot", server.base_url()))
                .send()
                .await
                .expect("fetch portal snapshot")
                .error_for_status()
                .expect("portal snapshot status")
                .json()
                .await
                .expect("decode portal snapshot");
            let bindings = BrowserUiBindings::from_portal_snapshot(server.base_url(), &snapshot);
            let enrollment = BrowserEnrollmentConfig {
                network_id: NetworkId::new("secure-demo"),
                project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
                release_train_hash: ContentId::new("demo-train"),
                target_artifact_id: "browser-wasm".into(),
                target_artifact_hash: ContentId::new("demo-artifact-native"),
                login_path: "/login/github".into(),
                callback_path: "/callback/github".into(),
                enroll_path: "/enroll".into(),
                trust_bundle_path: "/trust".into(),
                requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
                session_ttl_secs: 300,
            };
            let client = BrowserPortalClient::new(bindings, enrollment);
            let login = client
                .begin_login(Some("alice".into()))
                .await
                .expect("begin github upstream login");
            let session = client
                .complete_provider_login(&login, "github-upstream-code")
                .await
                .expect("complete github upstream login");
            assert_eq!(session.claims.principal_id.as_str(), "alice");
            assert_eq!(session.claims.provider, AuthProvider::GitHub);
            assert_eq!(session.claims.display_name, "Alice Upstream Browser");
            assert!(session.claims.org_memberships.contains("oss"));
            assert!(session.claims.group_memberships.contains("maintainers"));
            assert_eq!(
                session.claims.custom_claims.get("provider_login"),
                Some(&"alice-gh".to_owned())
            );
            assert_eq!(
                session.claims.custom_claims.get("provider_email"),
                Some(&"alice@example.com".to_owned())
            );
        });

        token_server.join().expect("join provider token server");
        userinfo_server
            .join()
            .expect("join provider userinfo server");
    }

    #[test]
    fn browser_portal_client_refreshes_and_logs_out_provider_session_via_live_http_router() {
        let (exchange_url, exchange_server) = spawn_provider_json_server(
            |request| {
                assert!(request.contains("\"provider_code\":\"github-code-refresh\""));
            },
            "200 OK",
            serde_json::json!({
                "principal_id": "alice",
                "display_name": "Alice GitHub",
                "org_memberships": ["oss"],
                "group_memberships": ["maintainers"],
                "custom_claims": {
                    "avatar_url": "https://avatars.example/alice.png"
                },
                "provider_subject": "github-user-42",
                "access_token": "access-token-1",
                "refresh_token": "refresh-token-1",
                "session_handle": "session-handle-1"
            })
            .to_string(),
        );
        let (refresh_url, refresh_server) = spawn_provider_json_server(
            |request| {
                assert!(request.contains("\"refresh_token\":\"refresh-token-1\""));
                assert!(request.contains("\"session_handle\":\"session-handle-1\""));
            },
            "200 OK",
            serde_json::json!({
                "display_name": "Alice Refreshed",
                "group_memberships": ["operators"],
                "custom_claims": {
                    "avatar_url": "https://avatars.example/alice-2.png"
                },
                "access_token": "access-token-2",
                "refresh_token": "refresh-token-2",
                "session_handle": "session-handle-2"
            })
            .to_string(),
        );
        let (revoke_url, revoke_server) = spawn_provider_json_server(
            |request| {
                assert!(request.contains("\"refresh_token\":\"refresh-token-2\""));
                assert!(request.contains("\"session_handle\":\"session-handle-2\""));
            },
            "200 OK",
            "{}".into(),
        );
        let temp = tempdir().expect("temp dir");
        let auth = Arc::new(
            build_auth_portal(
                &sample_auth_config_with_connector(
                    temp.path(),
                    BootstrapAuthConnectorConfig::GitHub {
                        authorize_base_url: Some(
                            "https://github.example/login/oauth/authorize".into(),
                        ),
                        exchange_url: Some(exchange_url),
                        token_url: None,
                        client_id: None,
                        client_secret: None,
                        userinfo_url: None,
                        refresh_url: Some(refresh_url),
                        revoke_url: Some(revoke_url),
                    },
                ),
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build github auth portal"),
        );
        let context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("github-browser-client-refresh.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(auth),
            control_handle: None,
        };
        let server = HttpTestServer::spawn(context);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let snapshot: burn_p2p_bootstrap::BrowserPortalSnapshot = reqwest::Client::new()
                .get(format!("{}/portal/snapshot", server.base_url()))
                .send()
                .await
                .expect("fetch portal snapshot")
                .error_for_status()
                .expect("portal snapshot status")
                .json()
                .await
                .expect("decode portal snapshot");
            let bindings = BrowserUiBindings::from_portal_snapshot(server.base_url(), &snapshot);
            let enrollment = BrowserEnrollmentConfig {
                network_id: NetworkId::new("secure-demo"),
                project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
                release_train_hash: ContentId::new("demo-train"),
                target_artifact_id: "browser-wasm".into(),
                target_artifact_hash: ContentId::new("demo-artifact-native"),
                login_path: "/login/github".into(),
                callback_path: "/callback/github".into(),
                enroll_path: "/enroll".into(),
                trust_bundle_path: "/trust".into(),
                requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
                session_ttl_secs: 300,
            };
            let client = BrowserPortalClient::new(bindings, enrollment);
            let login = client
                .begin_login(Some("alice".into()))
                .await
                .expect("begin github login");
            let session = client
                .complete_provider_login(&login, "github-code-refresh")
                .await
                .expect("complete github login");
            assert_eq!(session.claims.display_name, "Alice GitHub");
            assert_eq!(
                session.claims.custom_claims.get("avatar_url"),
                Some(&"https://avatars.example/alice.png".to_owned())
            );

            let refreshed = client
                .refresh_session(&session.session_id)
                .await
                .expect("refresh provider session");
            assert_eq!(refreshed.claims.display_name, "Alice Refreshed");
            assert!(refreshed.claims.group_memberships.contains("operators"));
            assert_eq!(
                refreshed.claims.custom_claims.get("avatar_url"),
                Some(&"https://avatars.example/alice-2.png".to_owned())
            );

            let logout = client
                .logout_session(&refreshed.session_id)
                .await
                .expect("logout provider session");
            assert!(logout.logged_out);
        });

        exchange_server.join().expect("join exchange server");
        refresh_server.join().expect("join refresh server");
        revoke_server.join().expect("join revoke server");
    }

    #[test]
    fn capabilities_endpoint_reports_compiled_and_active_services() {
        let temp = tempdir().expect("temp dir");
        let auth_config = sample_auth_config(temp.path());
        let auth = Arc::new(
            build_auth_portal(
                &auth_config,
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build auth portal"),
        );
        let context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: Some(auth_config),
            })),
            config_path: Arc::new(temp.path().join("capabilities.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(auth),
            control_handle: None,
        };

        let manifest = response_json(&issue_request(
            context,
            IssueRequestSpec {
                method: "GET",
                path: "/capabilities",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(
            manifest["payload"]["schema"],
            "burn_p2p.edge_service_manifest"
        );
        let payload = &manifest["payload"]["payload"];
        assert_eq!(payload["portal_mode"], "Interactive");
        assert_eq!(payload["browser_mode"], "Trainer");
        assert_eq!(payload["social_mode"], "Public");
        assert_eq!(payload["admin_mode"], "Rbac");
        assert_eq!(payload["metrics_mode"], "OpenMetrics");
        assert_eq!(
            payload["available_auth_providers"],
            serde_json::json!(["Static"])
        );
        let active = payload["active_feature_set"]["features"]
            .as_array()
            .expect("active features array");
        assert!(active.contains(&serde_json::json!("Portal")));
        assert!(active.contains(&serde_json::json!("BrowserEdge")));
        assert!(active.contains(&serde_json::json!("Social")));
        assert!(active.contains(&serde_json::json!("AuthStatic")));
    }

    #[test]
    fn disabled_optional_services_hide_routes_and_capabilities() {
        let temp = tempdir().expect("temp dir");
        let context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig {
                    portal_enabled: false,
                    browser_mode: BrowserMode::Disabled,
                    social_mode: SocialMode::Disabled,
                    profile_mode: ProfileMode::Disabled,
                },
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("capabilities-disabled.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: None,
            control_handle: None,
        };

        let manifest = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/.well-known/burn-p2p-capabilities",
                body: None,
                headers: &[],
            },
        ));
        let payload = &manifest["payload"]["payload"];
        assert_eq!(payload["portal_mode"], "Disabled");
        assert_eq!(payload["browser_mode"], "Disabled");
        assert_eq!(payload["social_mode"], "Disabled");
        let active = payload["active_feature_set"]["features"]
            .as_array()
            .expect("active features array");
        assert!(!active.contains(&serde_json::json!("Portal")));
        assert!(!active.contains(&serde_json::json!("BrowserEdge")));
        assert!(!active.contains(&serde_json::json!("Social")));

        let portal = issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/portal",
                body: None,
                headers: &[],
            },
        );
        assert!(portal.starts_with("HTTP/1.1 404 Not Found"));

        let leaderboard = issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/leaderboard",
                body: None,
                headers: &[],
            },
        );
        assert!(leaderboard.starts_with("HTTP/1.1 404 Not Found"));

        let browser_receipts = issue_request(
            context,
            IssueRequestSpec {
                method: "POST",
                path: "/receipts/browser",
                body: Some(serde_json::json!([])),
                headers: &[],
            },
        );
        assert!(browser_receipts.starts_with("HTTP/1.1 404 Not Found"));
    }

    #[test]
    fn portal_hides_disabled_browser_auth_and_social_flows() {
        let temp = tempdir().expect("temp dir");
        let context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig {
                    portal_enabled: true,
                    browser_mode: BrowserMode::Disabled,
                    social_mode: SocialMode::Disabled,
                    profile_mode: ProfileMode::Disabled,
                },
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("portal-hidden-flows.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: None,
            control_handle: None,
        };

        let portal_snapshot = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/portal/snapshot",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(portal_snapshot["browser_mode"], "Disabled");
        assert_eq!(portal_snapshot["social_mode"], "Disabled");
        assert_eq!(portal_snapshot["profile_mode"], "Disabled");
        assert_eq!(portal_snapshot["auth_enabled"], false);
        assert_eq!(portal_snapshot["transports"]["wss_fallback"], false);

        let portal = issue_request(
            context,
            IssueRequestSpec {
                method: "GET",
                path: "/portal",
                body: None,
                headers: &[],
            },
        );
        let portal_html = response_body(&portal);
        assert!(portal_html.contains("Join currently requires pre-provisioned credentials"));
        assert!(portal_html.contains("Browser peer join is not currently available"));
        assert!(portal_html.contains("Social features are disabled for this deployment."));
        assert!(!portal_html.contains("/login/static"));
        assert!(!portal_html.contains("<h2>Leaderboard</h2>"));
        assert!(!portal_html.contains("/leaderboard"));
    }

    #[test]
    fn startup_validation_rejects_uncompiled_optional_services() {
        let temp = tempdir().expect("temp dir");
        let config = BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig {
                portal_enabled: true,
                browser_mode: BrowserMode::Disabled,
                social_mode: SocialMode::Public,
                profile_mode: ProfileMode::Disabled,
            },
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            embedded_runtime: None,
            auth: Some(sample_auth_config(temp.path())),
        };
        let compiled = CompiledFeatureSet {
            features: BTreeSet::from([EdgeFeature::AdminHttp, EdgeFeature::Metrics]),
        };
        let error = validate_compiled_feature_support_with(&compiled, &config)
            .expect_err("portal and social should require compiled features");
        assert!(error.to_string().contains("portal"));
    }

    #[test]
    fn startup_validation_rejects_untrusted_external_auth_config() {
        let temp = tempdir().expect("temp dir");
        let mut config = BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
            embedded_runtime: None,
            auth: Some(sample_auth_config_with_connector(
                temp.path(),
                BootstrapAuthConnectorConfig::External {
                    authority: "corp-sso".into(),
                    trusted_principal_header: String::new(),
                    trusted_internal_only: false,
                },
            )),
        };
        let compiled = CompiledFeatureSet {
            features: BTreeSet::from([
                EdgeFeature::AdminHttp,
                EdgeFeature::Metrics,
                EdgeFeature::Portal,
                EdgeFeature::BrowserEdge,
                EdgeFeature::Rbac,
                EdgeFeature::AuthExternal,
                EdgeFeature::Social,
                EdgeFeature::Profiles,
            ]),
        };
        let error = validate_compiled_feature_support_with(&compiled, &config)
            .expect_err("external auth should require trusted internal mode");
        assert!(error.to_string().contains("trusted_internal_only"));

        config.auth = Some(sample_auth_config_with_connector(
            temp.path(),
            BootstrapAuthConnectorConfig::External {
                authority: "corp-sso".into(),
                trusted_principal_header: String::new(),
                trusted_internal_only: true,
            },
        ));
        let error = validate_compiled_feature_support_with(&compiled, &config)
            .expect_err("external auth should require a trusted principal header");
        assert!(error.to_string().contains("trusted_principal_header"));
    }

    #[test]
    fn auth_portal_issues_certificates_and_filters_directory_by_session_scope() {
        let temp = tempdir().expect("temp dir");
        let auth = build_auth_portal(
            &sample_auth_config(temp.path()),
            NetworkId::new("secure-demo"),
            Version::new(0, 1, 0),
        )
        .expect("build auth portal");

        let login = auth
            .connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("secure-demo"),
                principal_hint: Some("alice".into()),
                requested_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Train {
                        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                    },
                ]),
            })
            .expect("begin login");
        let session = auth
            .connector
            .complete_login(CallbackPayload {
                login_id: login.login_id,
                state: login.state,
                principal_id: Some(PrincipalId::new("alice")),
                provider_code: None,
            })
            .expect("complete login");
        auth.sessions
            .lock()
            .expect("auth session state should not be poisoned")
            .insert(session.session_id.clone(), session.clone());

        let node_keypair = Keypair::generate_ed25519();
        let peer_id = burn_p2p::PeerId::new(node_keypair.public().to_peer_id().to_string());
        let certificate = auth
            .authority
            .lock()
            .expect("auth authority should not be poisoned")
            .issue_certificate(NodeEnrollmentRequest {
                session: session.clone(),
                project_family_id: auth.project_family_id.clone(),
                release_train_hash: auth.required_release_train_hash.clone(),
                target_artifact_hash: ContentId::new("demo-artifact-native"),
                peer_id: peer_id.clone(),
                peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
                granted_roles: session.claims.granted_roles.clone(),
                requested_scopes: BTreeSet::from([ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                }]),
                client_policy_hash: Some(ContentId::new("policy-auth")),
                serial: 1,
                not_before: Utc::now() - Duration::seconds(5),
                not_after: Utc::now() + Duration::minutes(5),
                revocation_epoch: RevocationEpoch(1),
            })
            .expect("issue certificate");
        assert_eq!(certificate.claims().peer_id, peer_id);
        assert_eq!(certificate.claims().principal_id, PrincipalId::new("alice"));

        let request = HttpRequest {
            method: "GET".into(),
            path: "/directory".into(),
            headers: BTreeMap::from([(
                "x-session-id".into(),
                session.session_id.as_str().to_owned(),
            )]),
            body: Vec::new(),
        };
        let entries = auth_directory_entries(&auth, &request).expect("directory entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].experiment_id,
            burn_p2p::ExperimentId::new("exp-auth")
        );
    }

    #[test]
    fn load_or_create_keypair_is_persistent() {
        let temp = tempdir().expect("temp dir");
        let key_path = temp.path().join("authority.key");

        let first = load_or_create_keypair(&key_path).expect("first keypair");
        let second = load_or_create_keypair(&key_path).expect("second keypair");

        assert_eq!(first.public().to_peer_id(), second.public().to_peer_id());
    }

    #[test]
    fn authenticated_bootstrap_example_deserializes() {
        let config: BootstrapDaemonConfig =
            serde_json::from_str(include_str!("../../examples/authenticated-bootstrap.json"))
                .expect("deserialize auth example");
        assert!(config.auth.is_some());
        assert_eq!(
            config.spec.genesis.network_id,
            NetworkId::new("secure-demo")
        );
    }

    #[test]
    fn deployment_profile_examples_deserialize() {
        for (contents, network_id) in [
            (
                include_str!("../../examples/trusted-minimal.json"),
                "trusted-minimal-demo",
            ),
            (
                include_str!("../../examples/enterprise-sso.json"),
                "enterprise-sso-demo",
            ),
            (
                include_str!("../../examples/trusted-browser.json"),
                "trusted-browser-demo",
            ),
            (
                include_str!("../../examples/community-web.json"),
                "community-web-demo",
            ),
        ] {
            let config: BootstrapDaemonConfig =
                serde_json::from_str(contents).expect("deserialize profile example");
            assert_eq!(config.spec.genesis.network_id, NetworkId::new(network_id));
        }
    }

    #[test]
    fn http_routes_serve_status_and_static_auth_flow() {
        let temp = tempdir().expect("temp dir");
        let auth = Arc::new(
            build_auth_portal(
                &sample_auth_config(temp.path()),
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build auth portal"),
        );
        let daemon_config = Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: None,
            allow_dev_admin_token: false,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: Some(120),
            admin_signer_peer_id: Some(burn_p2p::PeerId::new("bootstrap-authority")),
            embedded_runtime: None,
            auth: Some(sample_auth_config(temp.path())),
        }));
        let config_path = Arc::new(temp.path().join("bootstrap-config.json"));
        std::fs::write(
            &*config_path,
            serde_json::to_vec_pretty(
                &*daemon_config
                    .lock()
                    .expect("daemon config should not be poisoned"),
            )
            .expect("serialize daemon config"),
        )
        .expect("write daemon config");
        let plan = Arc::new(sample_spec().plan().expect("bootstrap plan"));
        let principal_id = PrincipalId::new("alice");
        let receipt_accepted_at = Utc::now();
        let state = Arc::new(Mutex::new(BootstrapAdminState {
            head_descriptors: vec![HeadDescriptor {
                head_id: burn_p2p::HeadId::new("head-auth"),
                study_id: burn_p2p::StudyId::new("study-auth"),
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                revision_id: burn_p2p::RevisionId::new("rev-auth"),
                artifact_id: burn_p2p::ArtifactId::new("artifact-auth"),
                parent_head_id: None,
                global_step: 1,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            }],
            reducer_load_announcements: vec![ReducerLoadAnnouncement {
                overlay: OverlayTopic::experiment(
                    NetworkId::new("secure-demo"),
                    burn_p2p::StudyId::new("study-auth"),
                    burn_p2p::ExperimentId::new("exp-auth"),
                    OverlayChannel::Telemetry,
                )
                .expect("telemetry overlay"),
                report: burn_p2p::ReducerLoadReport {
                    peer_id: burn_p2p::PeerId::new("peer-auth"),
                    window_id: burn_p2p::WindowId(1),
                    assigned_leaf_updates: 1,
                    assigned_aggregate_inputs: 0,
                    ingress_bytes: 64,
                    egress_bytes: 32,
                    duplicate_transfer_ratio: 0.0,
                    overload_ratio: 0.0,
                    reported_at: Utc::now(),
                },
            }],
            contribution_receipts: vec![ContributionReceipt {
                receipt_id: burn_p2p_core::ContributionReceiptId::new("receipt-auth"),
                peer_id: burn_p2p::PeerId::new("peer-auth"),
                study_id: burn_p2p::StudyId::new("study-auth"),
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                revision_id: burn_p2p::RevisionId::new("rev-auth"),
                base_head_id: burn_p2p::HeadId::new("head-auth"),
                artifact_id: burn_p2p::ArtifactId::new("artifact-auth"),
                accepted_at: receipt_accepted_at,
                accepted_weight: 4.0,
                metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.25))]),
                merge_cert_id: None,
            }],
            peer_admission_reports: BTreeMap::from([(
                burn_p2p::PeerId::new("peer-auth"),
                PeerAdmissionReport {
                    peer_id: burn_p2p::PeerId::new("peer-auth"),
                    principal_id: principal_id.clone(),
                    requested_scopes: BTreeSet::from([
                        ExperimentScope::Connect,
                        ExperimentScope::Train {
                            experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                        },
                    ]),
                    trust_level: PeerTrustLevel::ScopeAuthorized,
                    issuer_peer_id: burn_p2p::PeerId::new("bootstrap-authority"),
                    findings: Vec::new(),
                    verified_at: Utc::now(),
                },
            )]),
            ..BootstrapAdminState::default()
        }));
        let context = HttpServerContext {
            plan: plan.clone(),
            state: state.clone(),
            config: daemon_config,
            config_path,
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: Some(120),
            admin_signer_peer_id: burn_p2p::PeerId::new("bootstrap-authority"),
            auth_state: Some(auth.clone()),
            control_handle: None,
        };

        let status = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/status",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(status["network_id"], "secure-demo");
        let bundle = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/diagnostics/bundle",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(bundle["plan"]["genesis"]["network_id"], "secure-demo");
        assert_eq!(bundle["diagnostics"]["network_id"], "secure-demo");
        assert_eq!(bundle["heads"][0]["head_id"], "head-auth");
        assert_eq!(
            bundle["reducer_load_announcements"][0]["report"]["peer_id"],
            "peer-auth"
        );
        let heads = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/heads",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(heads.as_array().expect("heads array").len(), 1);
        assert_eq!(heads[0]["head_id"], "head-auth");
        let reducer_load = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/reducers/load",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(
            reducer_load.as_array().expect("reducer load array").len(),
            1
        );
        assert_eq!(reducer_load[0]["report"]["peer_id"], "peer-auth");
        let trust = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/trust",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(trust["network_id"], "secure-demo");
        assert_eq!(trust["issuers"].as_array().expect("issuer array").len(), 1);
        assert_eq!(trust["reenrollment"], Value::Null);
        let reenrollment = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/reenrollment",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(reenrollment, Value::Null);

        let login = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/login/static",
                body: Some(serde_json::json!({
                    "network_id": "secure-demo",
                    "principal_hint": "alice",
                    "requested_scopes": ["Connect", {"Train": {"experiment_id": "exp-auth"}}],
                })),
                headers: &[],
            },
        ));
        let session = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/callback/static",
                body: Some(serde_json::json!({
                    "login_id": login["login_id"],
                    "state": login["state"],
                    "principal_id": "alice",
                })),
                headers: &[],
            },
        ));
        let session_header = session["session_id"].as_str().expect("session id string");
        let directory_headers = [("x-session-id", session_header)];
        let directory = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/directory",
                body: None,
                headers: &directory_headers,
            },
        ));
        assert_eq!(directory.as_array().expect("directory array").len(), 1);
        assert_eq!(directory[0]["experiment_id"], "exp-auth");

        let portal_snapshot = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/portal/snapshot",
                body: None,
                headers: &directory_headers,
            },
        ));
        assert_eq!(portal_snapshot["network_id"], "secure-demo");
        assert_eq!(portal_snapshot["auth_enabled"], true);
        assert_eq!(
            portal_snapshot["leaderboard"]["entries"][0]["identity"]["principal_id"],
            "alice"
        );
        assert_eq!(
            portal_snapshot["paths"]["portal_snapshot_path"],
            "/portal/snapshot"
        );
        assert_eq!(
            portal_snapshot["paths"]["receipt_submit_path"],
            "/receipts/browser"
        );
        assert_eq!(
            portal_snapshot["directory"]["entries"][0]["experiment_id"],
            "exp-auth"
        );

        let portal_response = issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/portal",
                body: None,
                headers: &directory_headers,
            },
        );
        let portal_html = response_body(&portal_response);
        assert!(portal_html.contains("burn_p2p browser portal"));
        assert!(portal_html.contains("/login/static"));
        assert!(portal_html.contains("exp-auth"));
        assert!(portal_html.contains("alice"));

        let leaderboard = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/leaderboard",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(leaderboard["score_version"], "leaderboard_score_v1");
        assert_eq!(
            leaderboard["entries"]
                .as_array()
                .expect("entries array")
                .len(),
            1
        );
        assert_eq!(
            leaderboard["entries"][0]["identity"]["principal_id"],
            "alice"
        );
        assert_eq!(leaderboard["entries"][0]["accepted_receipt_count"], 1);

        let receipt_submission = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/receipts/browser",
                body: Some(serde_json::json!([{
                    "receipt_id": "receipt-browser-submitted",
                    "peer_id": "peer-auth",
                    "study_id": "study-auth",
                    "experiment_id": "exp-auth",
                    "revision_id": "rev-auth",
                    "base_head_id": "head-auth",
                    "artifact_id": "artifact-browser",
                    "accepted_at": Utc::now(),
                    "accepted_weight": 2.0,
                    "metrics": {"loss": 0.2},
                    "merge_cert_id": null
                }])),
                headers: &directory_headers,
            },
        ));
        assert_eq!(
            receipt_submission["accepted_receipt_ids"][0],
            "receipt-browser-submitted"
        );
        assert_eq!(receipt_submission["pending_receipt_count"], 0);

        let signed_directory = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/directory/signed",
                body: None,
                headers: &directory_headers,
            },
        ));
        assert_eq!(
            signed_directory["payload"]["schema"],
            "burn_p2p.browser_directory_snapshot"
        );
        assert_eq!(
            signed_directory["payload"]["payload"]["entries"][0]["experiment_id"],
            "exp-auth"
        );
        assert_eq!(
            signed_directory["signature"]["signer"],
            "bootstrap-authority"
        );

        let signed_leaderboard = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "GET",
                path: "/leaderboard/signed",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(
            signed_leaderboard["payload"]["schema"],
            "burn_p2p.browser_leaderboard_snapshot"
        );
        assert_eq!(
            signed_leaderboard["payload"]["payload"]["entries"][0]["identity"]["principal_id"],
            "alice"
        );
        assert_eq!(
            signed_leaderboard["signature"]["signer"],
            "bootstrap-authority"
        );

        let receipts = response_json(&issue_request(
            context,
            IssueRequestSpec {
                method: "GET",
                path: "/receipts",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(receipts.as_array().expect("receipts array").len(), 2);
    }

    #[test]
    fn github_and_oidc_routes_issue_provider_specific_sessions() {
        let temp = tempdir().expect("temp dir");
        let github_auth = Arc::new(
            build_auth_portal(
                &sample_auth_config_with_connector(
                    temp.path(),
                    BootstrapAuthConnectorConfig::GitHub {
                        authorize_base_url: Some(
                            "https://github.example/login/oauth/authorize".into(),
                        ),
                        exchange_url: None,
                        token_url: None,
                        client_id: None,
                        client_secret: None,
                        userinfo_url: None,
                        refresh_url: None,
                        revoke_url: None,
                    },
                ),
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build github auth portal"),
        );
        let github_context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("github-bootstrap.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(github_auth),
            control_handle: None,
        };
        let github_login = response_json(&issue_request(
            github_context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/login/github",
                body: Some(serde_json::json!({
                    "network_id": "secure-demo",
                    "principal_hint": "alice",
                    "requested_scopes": ["Connect"],
                })),
                headers: &[],
            },
        ));
        assert_eq!(github_login["provider"], "GitHub");
        assert!(
            github_login["authorize_url"]
                .as_str()
                .expect("authorize url")
                .starts_with("https://github.example/login/oauth/authorize")
        );
        let github_session = response_json(&issue_request(
            github_context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/callback/github",
                body: Some(serde_json::json!({
                    "login_id": github_login["login_id"],
                    "state": github_login["state"],
                    "principal_id": "alice",
                })),
                headers: &[],
            },
        ));
        assert_eq!(github_session["claims"]["provider"], "GitHub");
        let github_snapshot = response_json(&issue_request(
            github_context,
            IssueRequestSpec {
                method: "GET",
                path: "/portal/snapshot",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(
            github_snapshot["login_providers"][0]["login_path"],
            "/login/github"
        );
        assert_eq!(
            github_snapshot["login_providers"][0]["callback_path"],
            "/callback/github"
        );

        let temp = tempdir().expect("temp dir");
        let oidc_auth = Arc::new(
            build_auth_portal(
                &sample_auth_config_with_connector(
                    temp.path(),
                    BootstrapAuthConnectorConfig::Oidc {
                        issuer: "https://issuer.example".into(),
                        authorize_base_url: Some("https://issuer.example/authorize".into()),
                        exchange_url: None,
                        token_url: None,
                        client_id: None,
                        client_secret: None,
                        userinfo_url: None,
                        refresh_url: None,
                        revoke_url: None,
                    },
                ),
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build oidc auth portal"),
        );
        let oidc_context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("oidc-bootstrap.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(oidc_auth),
            control_handle: None,
        };
        let oidc_login = response_json(&issue_request(
            oidc_context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/device/oidc",
                body: Some(serde_json::json!({
                    "network_id": "secure-demo",
                    "principal_hint": "alice",
                    "requested_scopes": ["Connect"],
                })),
                headers: &[],
            },
        ));
        assert_eq!(
            oidc_login["provider"],
            serde_json::json!({"Oidc": {"issuer": "https://issuer.example"}})
        );
        let oidc_session = response_json(&issue_request(
            oidc_context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/callback/oidc",
                body: Some(serde_json::json!({
                    "login_id": oidc_login["login_id"],
                    "state": oidc_login["state"],
                    "principal_id": "alice",
                })),
                headers: &[],
            },
        ));
        assert_eq!(
            oidc_session["claims"]["provider"],
            serde_json::json!({"Oidc": {"issuer": "https://issuer.example"}})
        );
        let oidc_snapshot = response_json(&issue_request(
            oidc_context,
            IssueRequestSpec {
                method: "GET",
                path: "/portal/snapshot",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(
            oidc_snapshot["login_providers"][0]["login_path"],
            "/login/oidc"
        );
        assert_eq!(
            oidc_snapshot["login_providers"][0]["device_path"],
            "/device/oidc"
        );
    }

    #[cfg(feature = "auth-external")]
    #[test]
    fn external_routes_issue_trusted_header_sessions() {
        let temp = tempdir().expect("temp dir");
        let external_auth = Arc::new(
            build_auth_portal(
                &sample_auth_config_with_connector(
                    temp.path(),
                    BootstrapAuthConnectorConfig::External {
                        authority: "corp-sso".into(),
                        trusted_principal_header: "x-corp-principal".into(),
                        trusted_internal_only: true,
                    },
                ),
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build external auth portal"),
        );
        let external_context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("external-bootstrap.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(external_auth),
            control_handle: None,
        };
        let external_login = response_json(&issue_request(
            external_context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/login/external",
                body: Some(serde_json::json!({
                    "network_id": "secure-demo",
                    "principal_hint": "alice",
                    "requested_scopes": ["Connect"],
                })),
                headers: &[],
            },
        ));
        assert_eq!(
            external_login["provider"],
            serde_json::json!({"External": {"authority": "corp-sso"}})
        );
        let external_session = response_json(&issue_request(
            external_context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/callback/external",
                body: Some(serde_json::json!({
                    "login_id": external_login["login_id"],
                    "state": external_login["state"],
                })),
                headers: &[("x-corp-principal", "alice")],
            },
        ));
        assert_eq!(
            external_session["claims"]["provider"],
            serde_json::json!({"External": {"authority": "corp-sso"}})
        );
        let external_snapshot = response_json(&issue_request(
            external_context,
            IssueRequestSpec {
                method: "GET",
                path: "/portal/snapshot",
                body: None,
                headers: &[],
            },
        ));
        assert_eq!(
            external_snapshot["login_providers"][0]["login_path"],
            "/login/external"
        );
        assert_eq!(
            external_snapshot["login_providers"][0]["callback_path"],
            "/callback/external"
        );
    }

    #[test]
    fn auth_portal_refreshes_and_logs_out_sessions() {
        let temp = tempdir().expect("temp dir");
        let auth = Arc::new(
            build_auth_portal(
                &sample_auth_config(temp.path()),
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build auth portal"),
        );
        let context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("refresh-logout.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(auth.clone()),
            control_handle: None,
        };

        let login = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/login/static",
                body: Some(serde_json::json!({
                    "network_id": "secure-demo",
                    "principal_hint": "alice",
                    "requested_scopes": ["Connect"],
                })),
                headers: &[],
            },
        ));
        let session = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/callback/static",
                body: Some(serde_json::json!({
                    "login_id": login["login_id"],
                    "state": login["state"],
                    "principal_id": "alice",
                })),
                headers: &[],
            },
        ));
        let session_id = ContentId::new(
            session["session_id"]
                .as_str()
                .expect("session id")
                .to_owned(),
        );
        assert!(
            auth.sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .contains_key(&session_id)
        );

        let refreshed = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/refresh",
                body: Some(serde_json::json!({
                    "session_id": session_id,
                })),
                headers: &[],
            },
        ));
        let refreshed_session_id = ContentId::new(
            refreshed["session_id"]
                .as_str()
                .expect("refreshed session id")
                .to_owned(),
        );
        let sessions = auth
            .sessions
            .lock()
            .expect("auth session state should not be poisoned");
        assert!(sessions.contains_key(&refreshed_session_id));
        drop(sessions);

        let logout = response_json(&issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/logout",
                body: Some(serde_json::json!({
                    "session_id": refreshed_session_id,
                })),
                headers: &[],
            },
        ));
        assert_eq!(logout["logged_out"], true);
        assert!(
            !auth
                .sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .contains_key(&ContentId::new(
                    refreshed["session_id"]
                        .as_str()
                        .expect("refreshed session id")
                        .to_owned(),
                ))
        );
    }

    #[test]
    fn admin_route_accepts_operator_session_and_rejects_unprivileged_session() {
        let temp = tempdir().expect("temp dir");
        let auth = Arc::new(
            build_auth_portal(
                &sample_auth_config(temp.path()),
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build auth portal"),
        );
        let context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("admin-rbac.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(auth.clone()),
            control_handle: None,
        };
        let login = auth
            .connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("secure-demo"),
                principal_hint: Some("alice".into()),
                requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            })
            .expect("begin login");
        let session = auth
            .connector
            .complete_login(CallbackPayload {
                login_id: login.login_id,
                state: login.state,
                principal_id: Some(PrincipalId::new("alice")),
                provider_code: None,
            })
            .expect("complete login");
        auth.sessions
            .lock()
            .expect("auth session state should not be poisoned")
            .insert(session.session_id.clone(), session.clone());

        let session_header = session.session_id.as_str().to_owned();
        let allowed = issue_request(
            context.clone(),
            IssueRequestSpec {
                method: "POST",
                path: "/admin",
                body: Some(serde_json::json!("ExportDiagnostics")),
                headers: &[("x-session-id", session_header.as_str())],
            },
        );
        assert!(allowed.starts_with("HTTP/1.1 200 OK"));

        let temp = tempdir().expect("temp dir");
        let mut unprivileged = sample_auth_config(temp.path());
        unprivileged.principals[0]
            .custom_claims
            .remove("operator_role");
        let auth = Arc::new(
            build_auth_portal(
                &unprivileged,
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build auth portal"),
        );
        let context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: None,
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("admin-rbac-unprivileged.json")),
            admin_token: None,
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: Some(auth.clone()),
            control_handle: None,
        };
        let login = auth
            .connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("secure-demo"),
                principal_hint: Some("alice".into()),
                requested_scopes: BTreeSet::from([ExperimentScope::Connect]),
            })
            .expect("begin login");
        let session = auth
            .connector
            .complete_login(CallbackPayload {
                login_id: login.login_id,
                state: login.state,
                principal_id: Some(PrincipalId::new("alice")),
                provider_code: None,
            })
            .expect("complete login");
        auth.sessions
            .lock()
            .expect("auth session state should not be poisoned")
            .insert(session.session_id.clone(), session.clone());

        let denied = issue_request(
            context,
            IssueRequestSpec {
                method: "POST",
                path: "/admin",
                body: Some(serde_json::json!("ExportDiagnostics")),
                headers: &[("x-session-id", session.session_id.as_str())],
            },
        );
        assert!(denied.starts_with("HTTP/1.1 403 Forbidden"));
        assert!(response_body(&denied).contains("ExportDiagnostics"));
    }

    #[test]
    fn admin_token_is_dev_only_and_disabled_by_default() {
        let temp = tempdir().expect("temp dir");
        let disabled_context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: Some("secret-token".into()),
                allow_dev_admin_token: false,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("admin-token-disabled.json")),
            admin_token: Some("secret-token".into()),
            allow_dev_admin_token: false,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: None,
            control_handle: None,
        };
        let disabled = issue_request(
            disabled_context,
            IssueRequestSpec {
                method: "POST",
                path: "/admin",
                body: Some(serde_json::json!("ExportDiagnostics")),
                headers: &[],
            },
        );
        assert!(disabled.starts_with("HTTP/1.1 401 Unauthorized"));

        let enabled_context = HttpServerContext {
            plan: Arc::new(sample_spec().plan().expect("bootstrap plan")),
            state: Arc::new(Mutex::new(BootstrapAdminState::default())),
            config: Arc::new(Mutex::new(BootstrapDaemonConfig {
                spec: sample_spec(),
                http_bind_addr: None,
                admin_token: Some("secret-token".into()),
                allow_dev_admin_token: true,
                optional_services: BootstrapOptionalServicesConfig::default(),
                remaining_work_units: None,
                admin_signer_peer_id: Some(PeerId::new("bootstrap-authority")),
                embedded_runtime: None,
                auth: None,
            })),
            config_path: Arc::new(temp.path().join("admin-token-enabled.json")),
            admin_token: Some("secret-token".into()),
            allow_dev_admin_token: true,
            remaining_work_units: None,
            admin_signer_peer_id: PeerId::new("bootstrap-authority"),
            auth_state: None,
            control_handle: None,
        };
        let enabled = issue_request(
            enabled_context,
            IssueRequestSpec {
                method: "POST",
                path: "/admin",
                body: Some(serde_json::json!("ExportDiagnostics")),
                headers: &[],
            },
        );
        assert!(enabled.starts_with("HTTP/1.1 200 OK"));
    }

    #[test]
    fn auth_portal_rotation_and_policy_rollout_persist_and_reissue() {
        let temp = tempdir().expect("temp dir");
        let auth_config = sample_auth_config(temp.path());
        let daemon_config = Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: Some("secret-token".into()),
            allow_dev_admin_token: true,
            optional_services: BootstrapOptionalServicesConfig::default(),
            remaining_work_units: None,
            admin_signer_peer_id: Some(burn_p2p::PeerId::new("bootstrap-authority")),
            embedded_runtime: None,
            auth: Some(auth_config.clone()),
        }));
        let config_path = Arc::new(temp.path().join("bootstrap-config.json"));
        std::fs::write(
            &*config_path,
            serde_json::to_vec_pretty(
                &*daemon_config
                    .lock()
                    .expect("daemon config should not be poisoned"),
            )
            .expect("serialize daemon config"),
        )
        .expect("write daemon config");
        let auth = Arc::new(
            build_auth_portal(
                &auth_config,
                NetworkId::new("secure-demo"),
                Version::new(0, 1, 0),
            )
            .expect("build auth portal"),
        );
        let plan = sample_spec().plan().expect("bootstrap plan");
        let state = Arc::new(Mutex::new(BootstrapAdminState::default()));

        let login = auth
            .connector
            .begin_login(LoginRequest {
                network_id: NetworkId::new("secure-demo"),
                principal_hint: Some("alice".into()),
                requested_scopes: BTreeSet::from([
                    ExperimentScope::Connect,
                    ExperimentScope::Train {
                        experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                    },
                ]),
            })
            .expect("begin login");
        let session = auth
            .connector
            .complete_login(CallbackPayload {
                login_id: login.login_id,
                state: login.state,
                principal_id: Some(PrincipalId::new("alice")),
                provider_code: None,
            })
            .expect("complete login");
        auth.sessions
            .lock()
            .expect("auth session state should not be poisoned")
            .insert(session.session_id.clone(), session.clone());

        let node_keypair = Keypair::generate_ed25519();
        let peer_id = burn_p2p::PeerId::new(node_keypair.public().to_peer_id().to_string());
        let first_cert = auth
            .authority
            .lock()
            .expect("auth authority should not be poisoned")
            .issue_certificate(NodeEnrollmentRequest {
                session: session.clone(),
                project_family_id: auth.project_family_id.clone(),
                release_train_hash: auth.required_release_train_hash.clone(),
                target_artifact_hash: ContentId::new("demo-artifact-native"),
                peer_id: peer_id.clone(),
                peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
                granted_roles: session.claims.granted_roles.clone(),
                requested_scopes: BTreeSet::from([ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                }]),
                client_policy_hash: Some(ContentId::new("policy-auth")),
                serial: 1,
                not_before: Utc::now() - Duration::seconds(5),
                not_after: Utc::now() + Duration::minutes(5),
                revocation_epoch: current_revocation_epoch(&auth, &state),
            })
            .expect("first certificate");

        let rolled_directory = vec![ExperimentDirectoryEntry {
            network_id: NetworkId::new("secure-demo"),
            study_id: burn_p2p::StudyId::new("study-auth"),
            experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            workload_id: burn_p2p::WorkloadId::new("auth-demo"),
            display_name: "Rolled Demo".into(),
            model_schema_hash: burn_p2p::ContentId::new("model-auth-rolled"),
            dataset_view_id: burn_p2p::DatasetViewId::new("view-auth-rolled"),
            resource_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
                minimum_device_memory_bytes: Some(1024),
                minimum_system_memory_bytes: Some(4096),
                estimated_download_bytes: 98_304,
                estimated_window_seconds: 30,
            },
            visibility: ExperimentVisibility::OptIn,
            opt_in_policy: ExperimentOptInPolicy::Scoped,
            current_revision_id: burn_p2p::RevisionId::new("rev-auth-rolled"),
            current_head_id: None,
            allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
            allowed_scopes: BTreeSet::from([ExperimentScope::Train {
                experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
            }]),
            metadata: BTreeMap::from([("rollout".into(), "true".into())]),
        }];
        let rollout = rollout_auth_policy(
            &plan,
            &auth,
            &state,
            AuthPolicyRollout {
                minimum_revocation_epoch: Some(RevocationEpoch(9)),
                directory_entries: Some(rolled_directory.clone()),
                trusted_issuers: None,
                reenrollment: None,
            },
            None,
        )
        .expect("rollout auth policy");
        match rollout {
            burn_p2p_bootstrap::AdminResult::AuthPolicyRolledOut {
                minimum_revocation_epoch,
                directory_entries,
                ..
            } => {
                assert_eq!(minimum_revocation_epoch, Some(RevocationEpoch(9)));
                assert_eq!(directory_entries, 1);
            }
            other => panic!("unexpected rollout result: {other:?}"),
        }
        {
            let mut config_guard = daemon_config
                .lock()
                .expect("daemon config should not be poisoned");
            if let Some(auth_config) = config_guard.auth.as_mut() {
                auth_config.minimum_revocation_epoch = auth_config.minimum_revocation_epoch.max(9);
                auth_config.directory_entries = rolled_directory.clone();
            }
            persist_daemon_config(&config_path, &config_guard).expect("persist rollout");
        }

        assert_eq!(current_revocation_epoch(&auth, &state), RevocationEpoch(9));
        let directory_request = HttpRequest {
            method: "GET".into(),
            path: "/directory".into(),
            headers: BTreeMap::from([(
                "x-session-id".into(),
                session.session_id.as_str().to_owned(),
            )]),
            body: Vec::new(),
        };
        let directory = auth_directory_entries(&auth, &directory_request).expect("directory");
        assert_eq!(directory[0].display_name, "Rolled Demo");
        assert_eq!(
            directory[0].current_revision_id,
            burn_p2p::RevisionId::new("rev-auth-rolled")
        );

        let rotate = rotate_authority_material(
            &auth,
            &state,
            Some("rotated-key".into()),
            true,
            true,
            Some("rotate for trust rollout".into()),
        )
        .expect("rotate authority material");
        match rotate {
            burn_p2p_bootstrap::AdminResult::AuthorityMaterialRotated {
                issuer_key_id,
                trusted_issuers,
                reenrollment_required,
                ..
            } => {
                assert_eq!(issuer_key_id, "rotated-key");
                assert_eq!(trusted_issuers, 2);
                assert!(reenrollment_required);
            }
            other => panic!("unexpected rotation result: {other:?}"),
        }
        {
            let mut config_guard = daemon_config
                .lock()
                .expect("daemon config should not be poisoned");
            if let Some(auth_config) = config_guard.auth.as_mut() {
                auth_config.issuer_key_id = "rotated-key".into();
                auth_config.trusted_issuers = current_trust_bundle(&auth, &state)
                    .issuers
                    .iter()
                    .map(|issuer| TrustedIssuer {
                        issuer_peer_id: issuer.issuer_peer_id.clone(),
                        issuer_public_key_hex: issuer.issuer_public_key_hex.clone(),
                    })
                    .collect();
                auth_config.reenrollment = auth
                    .reenrollment
                    .lock()
                    .expect("auth reenrollment state should not be poisoned")
                    .clone();
            }
            persist_daemon_config(&config_path, &config_guard).expect("persist rotate");
        }
        let trust_bundle = current_trust_bundle(&auth, &state);
        assert_eq!(trust_bundle.issuers.len(), 2);
        assert!(
            trust_bundle
                .reenrollment
                .as_ref()
                .is_some_and(|status| !status.legacy_issuer_peer_ids.is_empty())
        );

        let second_cert = auth
            .authority
            .lock()
            .expect("auth authority should not be poisoned")
            .issue_certificate(NodeEnrollmentRequest {
                session,
                project_family_id: auth.project_family_id.clone(),
                release_train_hash: auth.required_release_train_hash.clone(),
                target_artifact_hash: ContentId::new("demo-artifact-native"),
                peer_id,
                peer_public_key_hex: hex::encode(node_keypair.public().encode_protobuf()),
                granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu, PeerRole::Evaluator]),
                requested_scopes: BTreeSet::from([ExperimentScope::Train {
                    experiment_id: burn_p2p::ExperimentId::new("exp-auth"),
                }]),
                client_policy_hash: Some(ContentId::new("policy-auth")),
                serial: 2,
                not_before: Utc::now() - Duration::seconds(5),
                not_after: Utc::now() + Duration::minutes(5),
                revocation_epoch: current_revocation_epoch(&auth, &state),
            })
            .expect("second certificate");
        assert_ne!(
            first_cert.body.signature.signer,
            second_cert.body.signature.signer
        );
        assert_eq!(second_cert.body.signature.key_id, "rotated-key");
        assert_eq!(second_cert.claims().revocation_epoch, RevocationEpoch(9));

        let retired = retire_trusted_issuers(
            &auth,
            &state,
            &BTreeSet::from([first_cert.body.signature.signer.clone()]),
        )
        .expect("retire trusted issuer");
        match retired {
            burn_p2p_bootstrap::AdminResult::TrustedIssuersRetired {
                retired_issuers,
                remaining_issuers,
                reenrollment_required,
            } => {
                assert_eq!(retired_issuers, 1);
                assert_eq!(remaining_issuers, 1);
                assert!(!reenrollment_required);
            }
            other => panic!("unexpected retired result: {other:?}"),
        }
        {
            let mut config_guard = daemon_config
                .lock()
                .expect("daemon config should not be poisoned");
            if let Some(auth_config) = config_guard.auth.as_mut() {
                auth_config.trusted_issuers = current_trust_bundle(&auth, &state)
                    .issuers
                    .iter()
                    .map(|issuer| TrustedIssuer {
                        issuer_peer_id: issuer.issuer_peer_id.clone(),
                        issuer_public_key_hex: issuer.issuer_public_key_hex.clone(),
                    })
                    .collect();
                auth_config.reenrollment = auth
                    .reenrollment
                    .lock()
                    .expect("auth reenrollment state should not be poisoned")
                    .clone();
            }
            persist_daemon_config(&config_path, &config_guard).expect("persist retire");
        }

        let persisted: BootstrapDaemonConfig = serde_json::from_slice(
            &std::fs::read(&*config_path).expect("read persisted daemon config"),
        )
        .expect("deserialize persisted daemon config");
        assert_eq!(
            persisted
                .auth
                .as_ref()
                .expect("persisted auth config")
                .minimum_revocation_epoch,
            9
        );
        assert_eq!(
            persisted
                .auth
                .as_ref()
                .expect("persisted auth config")
                .issuer_key_id,
            "rotated-key"
        );
        assert_eq!(
            persisted
                .auth
                .as_ref()
                .expect("persisted auth config")
                .directory_entries[0]
                .current_revision_id,
            burn_p2p::RevisionId::new("rev-auth-rolled")
        );
        assert_eq!(
            persisted
                .auth
                .as_ref()
                .expect("persisted auth config")
                .trusted_issuers
                .len(),
            1
        );
        assert!(
            persisted
                .auth
                .as_ref()
                .expect("persisted auth config")
                .reenrollment
                .is_none()
        );
    }
}
