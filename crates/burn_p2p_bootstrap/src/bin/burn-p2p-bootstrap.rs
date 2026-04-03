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
    ArtifactBuildSpec, ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard,
    CapabilityEstimate, ChunkingScheme, ControlAnnouncement, ControlHandle, DatasetConfig,
    DatasetRegistration, DatasetSizing, EvalSplit, ExperimentDirectory,
    ExperimentDirectoryAnnouncement, ExperimentDirectoryEntry, FsArtifactStore, IdentityConnector,
    LoginRequest, MetricReport, MetricValue, NodeCertificateAuthority, NodeConfig,
    NodeEnrollmentRequest, OverlayTopic, P2pProject, PatchOutcome, PatchSupport, PrincipalClaims,
    PrincipalId, PrincipalSession, ProjectBackend, RuntimePatch, RuntimeProject,
    ShardFetchManifest, StaticIdentityConnector, StaticPrincipalRecord, TrainError, TrustedIssuer,
    WindowCtx, WindowReport,
};
use burn_p2p_bootstrap::{
    AdminAction, AuthPolicyRollout, BootstrapAdminState, BootstrapEmbeddedDaemon,
    BootstrapEmbeddedDaemonConfig, BootstrapPlan, BootstrapSpec, ReceiptQuery, ReenrollmentStatus,
    TrustBundleExport, TrustedIssuerStatus, render_dashboard_html, render_openmetrics,
};
use burn_p2p_core::{
    AuthProvider, ContentId, ExperimentScope, NetworkId, PeerId, PeerRoleSet, RevocationEpoch,
    SignatureAlgorithm, SignatureMetadata,
};
use chrono::{DateTime, Utc};
use libp2p_identity::Keypair;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BootstrapDaemonConfig {
    pub spec: BootstrapSpec,
    pub http_bind_addr: Option<String>,
    pub admin_token: Option<String>,
    pub remaining_work_units: Option<u64>,
    pub admin_signer_peer_id: Option<PeerId>,
    pub embedded_runtime: Option<BootstrapEmbeddedDaemonConfig>,
    pub auth: Option<BootstrapAuthConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BootstrapAuthConfig {
    pub authority_name: String,
    pub authority_key_path: PathBuf,
    #[serde(default = "default_issuer_key_id")]
    pub issuer_key_id: String,
    pub project_family_id: burn_p2p::ProjectFamilyId,
    pub required_client_release_hash: ContentId,
    pub session_ttl_seconds: i64,
    pub minimum_revocation_epoch: u64,
    pub principals: Vec<BootstrapAuthPrincipal>,
    pub directory_entries: Vec<ExperimentDirectoryEntry>,
    #[serde(default)]
    pub trusted_issuers: Vec<TrustedIssuer>,
    #[serde(default)]
    pub reenrollment: Option<BootstrapReenrollmentConfig>,
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

#[derive(Debug)]
struct AuthPortalState {
    connector: StaticIdentityConnector,
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
    required_client_release_hash: ContentId,
}

fn default_issuer_key_id() -> String {
    "bootstrap-auth".into()
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
        remaining_work_units,
        admin_signer_peer_id,
        auth_state,
        control_handle,
    } = context;

    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => write_response(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            render_dashboard_html(plan.network_id()).into_bytes(),
        )?,
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
        ("POST", "/login/static") | ("POST", "/device") => {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let login_request: LoginRequest = serde_json::from_slice(&request.body)?;
            let login = auth.connector.begin_login(login_request)?;
            write_json(&mut stream, &login)?;
        }
        ("POST", "/callback/static") => {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let callback: burn_p2p::CallbackPayload = serde_json::from_slice(&request.body)?;
            let session = auth.connector.complete_login(callback)?;
            auth.sessions
                .lock()
                .expect("auth session state should not be poisoned")
                .insert(session.session_id.clone(), session.clone());
            write_json(&mut stream, &session)?;
        }
        ("POST", "/enroll") => {
            let auth = auth_state.as_ref().ok_or("auth portal is not configured")?;
            let effective_revocation_epoch = current_revocation_epoch(auth, &state);
            let enroll: BootstrapEnrollRequest = serde_json::from_slice(&request.body)?;
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
                    client_release_hash: auth.required_client_release_hash.clone(),
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
            if !token_matches(&request, admin_token.as_deref()) {
                write_response(
                    &mut stream,
                    "401 Unauthorized",
                    "text/plain; charset=utf-8",
                    b"missing or invalid x-admin-token".to_vec(),
                )?;
                return Ok(());
            }

            let action: AdminAction = serde_json::from_slice(&request.body)?;
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
        required_client_release_hash: auth.required_client_release_hash.clone(),
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
        auth.required_client_release_hash.clone(),
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
    let principals = config
        .principals
        .iter()
        .map(|principal| {
            Ok((
                principal.principal_id.clone(),
                StaticPrincipalRecord {
                    claims: PrincipalClaims {
                        principal_id: principal.principal_id.clone(),
                        provider: AuthProvider::Static {
                            authority: config.authority_name.clone(),
                        },
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

    let connector = StaticIdentityConnector::new(
        config.authority_name.clone(),
        chrono::Duration::seconds(config.session_ttl_seconds.max(1)),
        principals,
    );
    let authority_keypair = load_or_create_keypair(&config.authority_key_path)?;
    let authority = NodeCertificateAuthority::new(
        network_id.clone(),
        config.project_family_id.clone(),
        config.required_client_release_hash.clone(),
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
        required_client_release_hash: config.required_client_release_hash.clone(),
    })
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

fn token_matches(request: &HttpRequest, admin_token: Option<&str>) -> bool {
    match admin_token {
        Some(expected) => request
            .headers
            .get("x-admin-token")
            .is_some_and(|value| value == expected),
        None => true,
    }
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
        BootstrapAuthConfig, BootstrapAuthPrincipal, BootstrapDaemonConfig, HttpRequest,
        HttpServerContext, auth_directory_entries, build_auth_portal, current_revocation_epoch,
        current_trust_bundle, default_issuer_key_id, handle_connection, load_or_create_keypair,
        persist_daemon_config, retire_trusted_issuers, rollout_auth_policy,
        rotate_authority_material,
    };
    use std::{
        collections::{BTreeMap, BTreeSet},
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{Arc, Mutex},
        thread,
    };

    use burn_p2p::{
        CallbackPayload, ExperimentDirectoryEntry, ExperimentOptInPolicy,
        ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, HeadDescriptor,
        IdentityConnector, LoginRequest, NodeEnrollmentRequest, OverlayChannel, OverlayTopic,
        PeerRole, PeerRoleSet, PrincipalId, ReducerLoadAnnouncement, TrustedIssuer,
    };
    use burn_p2p_bootstrap::{
        AdminApiPlan, ArchivePlan, AuthPolicyRollout, BootstrapAdminState, BootstrapPreset,
        BootstrapSpec,
    };
    use burn_p2p_core::{ClientPlatform, ContentId, NetworkId, RevocationEpoch};
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

    fn sample_auth_config(root: &std::path::Path) -> BootstrapAuthConfig {
        BootstrapAuthConfig {
            authority_name: "local-auth".into(),
            authority_key_path: root.join("authority.key"),
            issuer_key_id: default_issuer_key_id(),
            project_family_id: burn_p2p::ProjectFamilyId::new("demo-family"),
            required_client_release_hash: ContentId::new("demo-release"),
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
                custom_claims: BTreeMap::from([("team".into(), "research".into())]),
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
                principal_id: PrincipalId::new("alice"),
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
                client_release_hash: auth.required_client_release_hash.clone(),
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
            ..BootstrapAdminState::default()
        }));
        let context = HttpServerContext {
            plan: plan.clone(),
            state: state.clone(),
            config: daemon_config,
            config_path,
            admin_token: None,
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
            context,
            IssueRequestSpec {
                method: "GET",
                path: "/directory",
                body: None,
                headers: &directory_headers,
            },
        ));
        assert_eq!(directory.as_array().expect("directory array").len(), 1);
        assert_eq!(directory[0]["experiment_id"], "exp-auth");
    }

    #[test]
    fn auth_portal_rotation_and_policy_rollout_persist_and_reissue() {
        let temp = tempdir().expect("temp dir");
        let auth_config = sample_auth_config(temp.path());
        let daemon_config = Arc::new(Mutex::new(BootstrapDaemonConfig {
            spec: sample_spec(),
            http_bind_addr: None,
            admin_token: Some("secret-token".into()),
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
                principal_id: PrincipalId::new("alice"),
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
                client_release_hash: auth.required_client_release_hash.clone(),
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
                client_release_hash: auth.required_client_release_hash.clone(),
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
