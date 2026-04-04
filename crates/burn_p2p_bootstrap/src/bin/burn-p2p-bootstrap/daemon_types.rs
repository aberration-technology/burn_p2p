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
    #[serde(default)]
    pub artifact_publication: Option<BootstrapArtifactPublicationConfig>,
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct BootstrapArtifactPublicationConfig {
    #[serde(default)]
    pub targets: Vec<burn_p2p_core::PublicationTarget>,
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

fn default_publication_target_id() -> burn_p2p_core::PublicationTargetId {
    burn_p2p_core::PublicationTargetId::new("local-default")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ArtifactExportHttpRequest {
    pub experiment_id: burn_p2p::ExperimentId,
    #[serde(default)]
    pub run_id: Option<burn_p2p_core::RunId>,
    pub head_id: burn_p2p::HeadId,
    pub artifact_profile: burn_p2p_core::ArtifactProfile,
    #[serde(default = "default_publication_target_id")]
    pub publication_target_id: burn_p2p_core::PublicationTargetId,
    #[serde(default)]
    pub artifact_alias_id: Option<burn_p2p_core::ArtifactAliasId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ArtifactDownloadTicketHttpRequest {
    pub experiment_id: burn_p2p::ExperimentId,
    #[serde(default)]
    pub run_id: Option<burn_p2p_core::RunId>,
    pub head_id: burn_p2p::HeadId,
    pub artifact_profile: burn_p2p_core::ArtifactProfile,
    #[serde(default = "default_publication_target_id")]
    pub publication_target_id: burn_p2p_core::PublicationTargetId,
    #[serde(default)]
    pub artifact_alias_id: Option<burn_p2p_core::ArtifactAliasId>,
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
