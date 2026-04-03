use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::{
    codec::{CanonicalSchema, SchemaError},
    id::{
        ArtifactId, CapabilityCardId, ChunkId, ContentId, ContributionReceiptId, ControlCertId,
        DatasetId, DatasetViewId, ExperimentId, HeadId, LeaseId, MergeCertId, MicroShardId,
        NetworkId, NodeCertId, PeerId, PrincipalId, ProjectFamilyId, RevisionId, StudyId,
        WorkloadId,
    },
};

pub const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WindowId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowActivation {
    pub activation_window: WindowId,
    pub grace_windows: u16,
}

impl WindowActivation {
    pub fn becomes_active_at(&self, window: WindowId) -> bool {
        window >= self.activation_window
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaEnvelope<T> {
    pub schema: String,
    pub schema_version: u16,
    pub protocol_version: Version,
    pub payload: T,
}

impl<T> SchemaEnvelope<T> {
    pub fn new(schema: impl Into<String>, protocol_version: Version, payload: T) -> Self {
        Self {
            schema: schema.into(),
            schema_version: SCHEMA_VERSION,
            protocol_version,
            payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignatureAlgorithm {
    Ed25519,
    Secp256k1,
    Unknown(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignatureMetadata {
    pub signer: PeerId,
    pub key_id: String,
    pub algorithm: SignatureAlgorithm,
    pub signed_at: DateTime<Utc>,
    pub signature_hex: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedPayload<T> {
    pub payload_id: ContentId,
    pub payload: T,
    pub signature: SignatureMetadata,
}

impl<T> SignedPayload<T>
where
    T: Serialize,
{
    pub fn new(payload: T, signature: SignatureMetadata) -> Result<Self, SchemaError> {
        Ok(Self {
            payload_id: payload.content_id()?,
            payload,
            signature,
        })
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct RevocationEpoch(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AuthProvider {
    GitHub,
    Oidc { issuer: String },
    OAuth { provider: String },
    External { authority: String },
    Static { authority: String },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EdgeAuthProvider {
    Static,
    GitHub,
    Oidc,
    OAuth,
    External,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PortalMode {
    Disabled,
    Readonly,
    Interactive,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BrowserMode {
    Disabled,
    Observer,
    Verifier,
    Trainer,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SocialMode {
    Disabled,
    Private,
    Public,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ProfileMode {
    Disabled,
    Private,
    Public,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AdminMode {
    Disabled,
    Token,
    Rbac,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MetricsMode {
    Disabled,
    OpenMetrics,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EdgeFeature {
    AdminHttp,
    Metrics,
    Portal,
    BrowserEdge,
    Rbac,
    AuthStatic,
    AuthGitHub,
    AuthOidc,
    AuthOAuth,
    AuthExternal,
    Social,
    Profiles,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledFeatureSet {
    pub features: BTreeSet<EdgeFeature>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfiguredServiceSet {
    pub features: BTreeSet<EdgeFeature>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveServiceSet {
    pub features: BTreeSet<EdgeFeature>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeServiceManifest {
    pub edge_id: PeerId,
    pub network_id: NetworkId,
    pub portal_mode: PortalMode,
    pub browser_mode: BrowserMode,
    pub available_auth_providers: BTreeSet<EdgeAuthProvider>,
    pub social_mode: SocialMode,
    pub profile_mode: ProfileMode,
    pub admin_mode: AdminMode,
    pub metrics_mode: MetricsMode,
    pub compiled_feature_set: CompiledFeatureSet,
    pub configured_service_set: ConfiguredServiceSet,
    pub active_feature_set: ActiveServiceSet,
    pub generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PeerRole {
    Bootstrap,
    Authority,
    Validator,
    Archive,
    Reducer,
    TrainerGpu,
    TrainerCpu,
    Evaluator,
    PortalViewer,
    BrowserObserver,
    BrowserVerifier,
    BrowserTrainerWgpu,
    BrowserFallback,
    BrowserTrainer,
    RelayHelper,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExperimentScope {
    Connect,
    Discover,
    Train { experiment_id: ExperimentId },
    Validate { experiment_id: ExperimentId },
    Archive { experiment_id: ExperimentId },
    Admin { study_id: StudyId },
}

impl ExperimentScope {
    pub fn applies_to_experiment(&self, experiment_id: &ExperimentId) -> bool {
        matches!(
            self,
            Self::Train { experiment_id: scoped }
                | Self::Validate { experiment_id: scoped }
                | Self::Archive { experiment_id: scoped }
                if scoped == experiment_id
        )
    }

    pub fn allows_directory_discovery(&self) -> bool {
        matches!(self, Self::Connect | Self::Discover | Self::Admin { .. })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRoleSet {
    pub roles: BTreeSet<PeerRole>,
}

impl PeerRoleSet {
    pub fn new(roles: impl IntoIterator<Item = PeerRole>) -> Self {
        Self {
            roles: roles.into_iter().collect(),
        }
    }

    pub fn default_trainer() -> Self {
        Self::new([PeerRole::TrainerGpu])
    }

    pub fn contains(&self, role: &PeerRole) -> bool {
        self.roles.contains(role)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PersistenceClass {
    Ephemeral,
    Session,
    Durable,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ClientPlatform {
    Native,
    Browser,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ArtifactTargetKind {
    NativeLinuxX86_64,
    NativeMacosAarch64,
    NativeWindowsX86_64,
    BrowserWasm,
    Other(String),
}

impl ArtifactTargetKind {
    pub fn as_target_artifact_id(&self) -> &str {
        match self {
            Self::NativeLinuxX86_64 => "native-linux-x86_64",
            Self::NativeMacosAarch64 => "native-macos-aarch64",
            Self::NativeWindowsX86_64 => "native-windows-x86_64",
            Self::BrowserWasm => "browser-wasm",
            Self::Other(target) => target.as_str(),
        }
    }

    pub fn parse(target: &str) -> Self {
        match target {
            "native-linux-x86_64" => Self::NativeLinuxX86_64,
            "native-macos-aarch64" => Self::NativeMacosAarch64,
            "native-windows-x86_64" => Self::NativeWindowsX86_64,
            "browser-wasm" => Self::BrowserWasm,
            other => Self::Other(other.into()),
        }
    }

    pub fn platform(&self) -> ClientPlatform {
        match self {
            Self::BrowserWasm => ClientPlatform::Browser,
            Self::NativeLinuxX86_64
            | Self::NativeMacosAarch64
            | Self::NativeWindowsX86_64
            | Self::Other(_) => ClientPlatform::Native,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BrowserRole {
    PortalViewer,
    Observer,
    Verifier,
    TrainerWgpu,
    Fallback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BrowserCapability {
    WebGpu,
    DedicatedWorker,
    PersistentStorage,
    WebRtcDirect,
    WebTransport,
    WssFallback,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AttestationLevel {
    None,
    Manifest,
    Challenge,
    Strong,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CapabilityClass {
    TrainerGpu,
    TrainerCpu,
    Evaluator,
    Archive,
    RelayHelper,
    BrowserObserver,
    BrowserVerifier,
    BrowserTrainerWgpu,
    BrowserFallback,
    BrowserOpportunistic,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityCard {
    pub card_id: CapabilityCardId,
    pub peer_id: PeerId,
    pub platform: ClientPlatform,
    pub roles: PeerRoleSet,
    pub preferred_backends: Vec<String>,
    pub recommended_classes: BTreeSet<CapabilityClass>,
    pub device_memory_bytes: Option<u64>,
    pub system_memory_bytes: u64,
    pub disk_bytes: u64,
    pub upload_mbps: f32,
    pub download_mbps: f32,
    pub persistence: PersistenceClass,
    pub work_units_per_second: f64,
    pub attestation_level: AttestationLevel,
    pub benchmark_hash: Option<ContentId>,
    pub reported_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityEstimate {
    pub preferred_backends: Vec<String>,
    pub work_units_per_second: f64,
    pub target_window_seconds: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetManifest {
    pub dataset_id: DatasetId,
    pub source_uri: String,
    pub format: String,
    pub manifest_hash: ContentId,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetView {
    pub dataset_view_id: DatasetViewId,
    pub dataset_id: DatasetId,
    pub preprocessing_hash: ContentId,
    pub tokenizer_hash: Option<ContentId>,
    pub manifest_hash: ContentId,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportedWorkload {
    pub workload_id: WorkloadId,
    pub workload_name: String,
    pub model_program_hash: ContentId,
    pub checkpoint_format_hash: ContentId,
    pub supported_revision_family: ContentId,
    pub resource_class: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetArtifactManifest {
    pub target_artifact_id: String,
    pub target_artifact_hash: ContentId,
    pub platform: ClientPlatform,
    pub built_at: DateTime<Utc>,
}

impl TargetArtifactManifest {
    pub fn target_kind(&self) -> ArtifactTargetKind {
        ArtifactTargetKind::parse(&self.target_artifact_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseTrainManifest {
    pub project_family_id: ProjectFamilyId,
    pub release_train_hash: ContentId,
    pub app_semver: Version,
    pub git_commit: String,
    pub cargo_lock_hash: ContentId,
    pub burn_version_string: String,
    pub enabled_features_hash: ContentId,
    pub protocol_major: u16,
    pub supported_workloads: Vec<SupportedWorkload>,
    pub approved_target_artifacts: Vec<TargetArtifactManifest>,
    pub built_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientReleaseManifest {
    pub project_family_id: ProjectFamilyId,
    pub release_train_hash: ContentId,
    pub target_artifact_id: String,
    pub target_artifact_hash: ContentId,
    pub target_platform: ClientPlatform,
    pub app_semver: Version,
    pub git_commit: String,
    pub cargo_lock_hash: ContentId,
    pub burn_version_string: String,
    pub enabled_features_hash: ContentId,
    pub protocol_major: u16,
    pub supported_workloads: Vec<SupportedWorkload>,
    pub built_at: DateTime<Utc>,
}

impl ClientReleaseManifest {
    pub fn target_kind(&self) -> ArtifactTargetKind {
        ArtifactTargetKind::parse(&self.target_artifact_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkManifest {
    pub network_id: NetworkId,
    pub project_family_id: ProjectFamilyId,
    pub protocol_major: u16,
    pub required_release_train_hash: ContentId,
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
    pub authority_public_keys: Vec<String>,
    pub bootstrap_addrs: Vec<String>,
    pub auth_policy_hash: ContentId,
    pub created_at: DateTime<Utc>,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperimentManifest {
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub workload_id: WorkloadId,
    pub display_name: String,
    pub visibility: ExperimentVisibility,
    pub current_revision_id: RevisionId,
    pub allowed_roles: PeerRoleSet,
    pub join_policy: ExperimentOptInPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserRolePolicy {
    pub observer: bool,
    pub verifier: bool,
    pub trainer_wgpu: bool,
    pub fallback: bool,
}

impl Default for BrowserRolePolicy {
    fn default() -> Self {
        Self {
            observer: true,
            verifier: true,
            trainer_wgpu: false,
            fallback: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BrowserVisibilityPolicy {
    Hidden,
    PortalListed,
    AuthenticatedPortal,
    SwarmEligible,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IdentityVisibility {
    Anonymous,
    AggregateOnly,
    PublicDisplayName,
    PublicProfile,
    PublicOrg,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SocialProfile {
    pub principal_id: PrincipalId,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub profile_url: Option<String>,
    pub org_slug: Option<String>,
    pub team_slug: Option<String>,
    pub visibility: IdentityVisibility,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BadgeKind {
    FirstAcceptedUpdate,
    FirstVerifiedHead,
    SevenDayStreak,
    TopBrowserContributor,
    TopValidatorContributor,
    HelpedPromoteCanonicalHead,
    TeamContributor,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BadgeAward {
    pub kind: BadgeKind,
    pub label: String,
    pub awarded_at: Option<DateTime<Utc>>,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderboardIdentity {
    pub principal_id: Option<PrincipalId>,
    pub peer_ids: BTreeSet<PeerId>,
    pub label: String,
    #[serde(default)]
    pub social_profile: Option<SocialProfile>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContributionRollup {
    pub accepted_work_score: f64,
    pub quality_weighted_impact_score: f64,
    pub validation_service_score: f64,
    pub artifact_serving_score: f64,
    pub leaderboard_score_v1: f64,
    pub accepted_receipt_count: u64,
    pub last_receipt_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LagState {
    Current,
    SlightlyBehind,
    CatchupRequired,
    LeaseBlocked,
    RebaseRequired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LagPolicy {
    pub max_head_lag_before_catchup: u64,
    pub max_head_lag_before_block: u64,
    pub max_head_lag_before_full_rebase: u64,
    pub max_window_skew_before_lease_revoke: u64,
}

impl Default for LagPolicy {
    fn default() -> Self {
        Self {
            max_head_lag_before_catchup: 1,
            max_head_lag_before_block: 4,
            max_head_lag_before_full_rebase: 16,
            max_window_skew_before_lease_revoke: 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MergeWindowMissPolicy {
    CatchupOnly,
    #[default]
    LeaseBlocked,
    RebaseRequired,
}

impl MergeWindowMissPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CatchupOnly => "catchup-only",
            Self::LeaseBlocked => "lease-blocked",
            Self::RebaseRequired => "rebase-required",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "catchup-only" => Some(Self::CatchupOnly),
            "lease-blocked" => Some(Self::LeaseBlocked),
            "rebase-required" => Some(Self::RebaseRequired),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackpressurePolicy {
    pub max_in_flight_chunk_requests_per_peer: u16,
    pub max_in_flight_updates_per_peer: u16,
    pub reducer_queue_depth_limit: u16,
    pub upload_concurrency_limit: u16,
    pub browser_transfer_budget_bytes: u64,
    pub portal_feed_sample_rate: u16,
}

impl Default for BackpressurePolicy {
    fn default() -> Self {
        Self {
            max_in_flight_chunk_requests_per_peer: 8,
            max_in_flight_updates_per_peer: 4,
            reducer_queue_depth_limit: 64,
            upload_concurrency_limit: 4,
            browser_transfer_budget_bytes: 64 * 1024 * 1024,
            portal_feed_sample_rate: 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeaderboardEntry {
    pub identity: LeaderboardIdentity,
    pub accepted_work_score: f64,
    pub quality_weighted_impact_score: f64,
    pub validation_service_score: f64,
    pub artifact_serving_score: f64,
    pub leaderboard_score_v1: f64,
    pub accepted_receipt_count: u64,
    pub last_receipt_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub badges: Vec<BadgeAward>,
}

impl LeaderboardEntry {
    pub fn contribution_rollup(&self) -> ContributionRollup {
        ContributionRollup {
            accepted_work_score: self.accepted_work_score,
            quality_weighted_impact_score: self.quality_weighted_impact_score,
            validation_service_score: self.validation_service_score,
            artifact_serving_score: self.artifact_serving_score,
            leaderboard_score_v1: self.leaderboard_score_v1,
            accepted_receipt_count: self.accepted_receipt_count,
            last_receipt_at: self.last_receipt_at,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeaderboardSnapshot {
    pub network_id: NetworkId,
    pub score_version: String,
    pub entries: Vec<LeaderboardEntry>,
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevisionManifest {
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub workload_id: WorkloadId,
    pub required_release_train_hash: ContentId,
    pub model_schema_hash: ContentId,
    pub checkpoint_format_hash: ContentId,
    pub dataset_view_id: DatasetViewId,
    pub training_config_hash: ContentId,
    pub merge_topology_policy_hash: ContentId,
    pub slot_requirements: ExperimentResourceRequirements,
    pub activation_window: WindowActivation,
    #[serde(default)]
    pub lag_policy: LagPolicy,
    #[serde(default)]
    pub merge_window_miss_policy: MergeWindowMissPolicy,
    pub browser_enabled: bool,
    pub browser_role_policy: BrowserRolePolicy,
    pub max_browser_checkpoint_bytes: Option<u64>,
    pub max_browser_window_secs: Option<u64>,
    pub max_browser_shard_bytes: Option<u64>,
    pub requires_webgpu: bool,
    pub max_browser_batch_size: Option<u32>,
    pub recommended_browser_precision: Option<Precision>,
    pub visibility_policy: BrowserVisibilityPolicy,
    pub description: String,
}

impl RevisionManifest {
    pub fn effective_lag_policy(&self) -> LagPolicy {
        self.lag_policy.clone()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCertificateClaims {
    pub network_id: NetworkId,
    pub project_family_id: ProjectFamilyId,
    pub release_train_hash: ContentId,
    pub target_artifact_hash: ContentId,
    pub peer_id: PeerId,
    pub peer_public_key_hex: String,
    pub principal_id: PrincipalId,
    pub provider: AuthProvider,
    pub granted_roles: PeerRoleSet,
    pub experiment_scopes: BTreeSet<ExperimentScope>,
    pub client_policy_hash: Option<ContentId>,
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
    pub serial: u64,
    pub revocation_epoch: RevocationEpoch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCertificate {
    pub node_cert_id: NodeCertId,
    pub body: SignedPayload<SchemaEnvelope<NodeCertificateClaims>>,
}

impl NodeCertificate {
    pub fn new(
        protocol_version: Version,
        claims: NodeCertificateClaims,
        signature: SignatureMetadata,
    ) -> Result<Self, SchemaError> {
        let body = SignedPayload::new(
            SchemaEnvelope::new("burn_p2p.node_certificate", protocol_version, claims),
            signature,
        )?;

        Ok(Self {
            node_cert_id: body.payload_id.clone().into(),
            body,
        })
    }

    pub fn claims(&self) -> &NodeCertificateClaims {
        &self.body.payload.payload
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerAuthEnvelope {
    pub peer_id: PeerId,
    pub certificate: NodeCertificate,
    pub client_manifest_id: Option<ContentId>,
    pub requested_scopes: BTreeSet<ExperimentScope>,
    pub nonce_hash: ContentId,
    pub challenge_signature_hex: String,
    pub presented_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExperimentVisibility {
    Public,
    OptIn,
    InviteOnly,
    AuthorityAssigned,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExperimentOptInPolicy {
    Open,
    Scoped,
    InviteOnly,
    AuthorityAssigned,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperimentResourceRequirements {
    pub minimum_roles: BTreeSet<PeerRole>,
    pub minimum_device_memory_bytes: Option<u64>,
    pub minimum_system_memory_bytes: Option<u64>,
    pub estimated_download_bytes: u64,
    pub estimated_window_seconds: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperimentDirectoryEntry {
    pub network_id: NetworkId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub workload_id: WorkloadId,
    pub display_name: String,
    pub model_schema_hash: ContentId,
    pub dataset_view_id: DatasetViewId,
    pub resource_requirements: ExperimentResourceRequirements,
    pub visibility: ExperimentVisibility,
    pub opt_in_policy: ExperimentOptInPolicy,
    pub current_revision_id: RevisionId,
    pub current_head_id: Option<HeadId>,
    pub allowed_roles: PeerRoleSet,
    pub allowed_scopes: BTreeSet<ExperimentScope>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MergeStrategy {
    GlobalBroadcastBaseline,
    CentralReducerBaseline,
    RandomPeerGossip,
    KRegularGossip,
    FixedTreeReduce,
    RotatingRendezvousTree,
    ReplicatedRendezvousDag,
    LocalGossipPlusPeriodicGlobal,
    MicrocohortReducePlusValidatorPromotion,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadPromotionPolicy {
    pub validator_quorum: u16,
    pub apply_single_root_ema: bool,
    pub allow_late_rollover: bool,
    pub promote_serve_head: bool,
}

impl Default for HeadPromotionPolicy {
    fn default() -> Self {
        Self {
            validator_quorum: 2,
            apply_single_root_ema: true,
            allow_late_rollover: true,
            promote_serve_head: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeTopologyPolicy {
    pub strategy: MergeStrategy,
    pub reducer_replication: u8,
    pub target_leaf_cohort: u16,
    pub upper_fanin: u8,
    pub window_duration_secs: u32,
    pub publish_jitter_ms: u32,
    pub staleness_windows: u16,
    pub promotion_policy: HeadPromotionPolicy,
}

impl Default for MergeTopologyPolicy {
    fn default() -> Self {
        Self {
            strategy: MergeStrategy::ReplicatedRendezvousDag,
            reducer_replication: 2,
            target_leaf_cohort: 16,
            upper_fanin: 4,
            window_duration_secs: 300,
            publish_jitter_ms: 750,
            staleness_windows: 2,
            promotion_policy: HeadPromotionPolicy::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateNormStats {
    pub l2_norm: f64,
    pub max_abs: f64,
    pub clipped: bool,
    pub non_finite_tensors: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateAnnounce {
    pub peer_id: PeerId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub window_id: WindowId,
    pub base_head_id: HeadId,
    pub delta_artifact_id: ArtifactId,
    pub sample_weight: f64,
    pub quality_weight: f64,
    pub norm_stats: UpdateNormStats,
    pub receipt_root: ContentId,
    pub receipt_ids: Vec<ContributionReceiptId>,
    pub providers: Vec<PeerId>,
    pub announced_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReducerAssignment {
    pub assignment_id: ContentId,
    pub window_id: WindowId,
    pub source_peer_id: PeerId,
    pub assigned_reducers: Vec<PeerId>,
    pub repair_reducers: Vec<PeerId>,
    pub upper_tier_reducers: Vec<PeerId>,
    pub assigned_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AggregateTier {
    Leaf,
    Upper,
    RootCandidate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregateStats {
    pub accepted_updates: u32,
    pub duplicate_updates: u32,
    pub dropped_updates: u32,
    pub late_updates: u32,
    pub sum_sample_weight: f64,
    pub sum_quality_weight: f64,
    pub sum_weighted_delta_norm: f64,
    pub max_update_norm: f64,
    pub accepted_sample_coverage: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregateEnvelope {
    pub aggregate_id: ContentId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub window_id: WindowId,
    pub base_head_id: HeadId,
    pub aggregate_artifact_id: ArtifactId,
    pub tier: AggregateTier,
    pub reducer_peer_id: PeerId,
    pub contributor_peers: Vec<PeerId>,
    pub child_aggregate_ids: Vec<ContentId>,
    pub stats: AggregateStats,
    pub providers: Vec<PeerId>,
    pub published_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReductionCertificate {
    pub reduction_id: ContentId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub window_id: WindowId,
    pub base_head_id: HeadId,
    pub aggregate_id: ContentId,
    pub validator: PeerId,
    pub validator_quorum: u16,
    pub cross_checked_reducers: Vec<PeerId>,
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeWindowState {
    pub merge_window_id: ContentId,
    pub network_id: NetworkId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub window_id: WindowId,
    pub base_head_id: HeadId,
    pub policy: MergeTopologyPolicy,
    pub reducers: Vec<PeerId>,
    pub validators: Vec<PeerId>,
    pub opened_at: DateTime<Utc>,
    pub closes_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReducerLoadReport {
    pub peer_id: PeerId,
    pub window_id: WindowId,
    pub assigned_leaf_updates: u32,
    pub assigned_aggregate_inputs: u32,
    pub ingress_bytes: u128,
    pub egress_bytes: u128,
    pub duplicate_transfer_ratio: f64,
    pub overload_ratio: f64,
    pub reported_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MicroShard {
    pub microshard_id: MicroShardId,
    pub dataset_view_id: DatasetViewId,
    pub ordinal: u32,
    pub estimated_examples: u64,
    pub estimated_tokens: u64,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentLease {
    pub lease_id: LeaseId,
    pub network_id: NetworkId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub peer_id: PeerId,
    pub dataset_view_id: DatasetViewId,
    pub window_id: WindowId,
    pub granted_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub budget_work_units: u64,
    pub microshards: Vec<MicroShardId>,
    pub assignment_hash: ContentId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkDisposition {
    Accepted,
    Rejected,
    Partial,
    Expired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataReceipt {
    pub receipt_id: ContentId,
    pub lease_id: LeaseId,
    pub peer_id: PeerId,
    pub completed_at: DateTime<Utc>,
    pub microshards: Vec<MicroShardId>,
    pub examples_processed: u64,
    pub tokens_processed: u64,
    pub disposition: WorkDisposition,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Precision {
    Fp32,
    Fp16,
    Bf16,
    Int8,
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ArtifactKind {
    FullHead,
    ServeHead,
    DeltaPack,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkDescriptor {
    pub chunk_id: ChunkId,
    pub offset_bytes: u64,
    pub length_bytes: u64,
    pub chunk_hash: ContentId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactDescriptor {
    pub artifact_id: ArtifactId,
    pub kind: ArtifactKind,
    pub head_id: Option<HeadId>,
    pub base_head_id: Option<HeadId>,
    pub precision: Precision,
    pub model_schema_hash: ContentId,
    pub record_format: String,
    pub bytes_len: u64,
    pub chunks: Vec<ChunkDescriptor>,
    pub root_hash: ContentId,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetricValue {
    Integer(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HeadDescriptor {
    pub head_id: HeadId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub artifact_id: ArtifactId,
    pub parent_head_id: Option<HeadId>,
    pub global_step: u64,
    pub created_at: DateTime<Utc>,
    pub metrics: BTreeMap<String, MetricValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MergePolicy {
    WeightedMean,
    NormClippedWeightedMean,
    TrimmedMean,
    Ema,
    QualityWeightedEma,
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContributionReceipt {
    pub receipt_id: ContributionReceiptId,
    pub peer_id: PeerId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub base_head_id: HeadId,
    pub artifact_id: ArtifactId,
    pub accepted_at: DateTime<Utc>,
    pub accepted_weight: f64,
    pub metrics: BTreeMap<String, MetricValue>,
    pub merge_cert_id: Option<MergeCertId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MergeCertificate {
    pub merge_cert_id: MergeCertId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub base_head_id: HeadId,
    pub merged_head_id: HeadId,
    pub merged_artifact_id: ArtifactId,
    pub policy: MergePolicy,
    pub issued_at: DateTime<Utc>,
    pub validator: PeerId,
    pub contribution_receipts: Vec<ContributionReceiptId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlCertificate<T> {
    pub control_cert_id: ControlCertId,
    pub network_id: NetworkId,
    pub activation: WindowActivation,
    pub required_client_capabilities: BTreeSet<String>,
    pub body: SignedPayload<SchemaEnvelope<T>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NetworkEstimate {
    pub connected_peers: u32,
    pub observed_peers: u64,
    pub estimated_network_size: f64,
    pub estimated_total_vram_bytes: Option<u128>,
    pub estimated_total_flops: Option<f64>,
    pub eta_lower_seconds: Option<u64>,
    pub eta_upper_seconds: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TelemetrySummary {
    pub network_id: NetworkId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub window_id: WindowId,
    pub active_peers: u32,
    pub accepted_contributions: u64,
    pub throughput_work_units_per_second: f64,
    pub network: NetworkEstimate,
    pub metrics: BTreeMap<String, MetricValue>,
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenesisSpec {
    pub network_id: NetworkId,
    pub protocol_version: Version,
    pub display_name: String,
    pub created_at: DateTime<Utc>,
    pub metadata: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use chrono::Utc;

    use super::{
        ActiveServiceSet, AdminMode, AuthProvider, BackpressurePolicy, BadgeAward, BadgeKind,
        BrowserMode, ClientPlatform, ClientReleaseManifest, CompiledFeatureSet,
        ConfiguredServiceSet, EdgeAuthProvider, EdgeFeature, EdgeServiceManifest,
        ExperimentDirectoryEntry, ExperimentOptInPolicy, ExperimentResourceRequirements,
        ExperimentScope, ExperimentVisibility, IdentityVisibility, LagPolicy, LagState,
        LeaderboardEntry, LeaderboardIdentity, LeaderboardSnapshot, MergeStrategy,
        MergeTopologyPolicy, MergeWindowMissPolicy, MetricValue, MetricsMode, NetworkManifest,
        NodeCertificate, NodeCertificateClaims, PeerId, PeerRole, PeerRoleSet, PortalMode,
        ProfileMode, RevocationEpoch, SchemaEnvelope, SocialMode, SocialProfile, SupportedWorkload,
        WindowActivation, WindowId,
    };
    use crate::{
        codec::{CanonicalSchema, deterministic_cbor, from_cbor_slice},
        id::{ContentId, PrincipalId},
    };

    #[test]
    fn identical_payloads_produce_the_same_content_id() {
        let first = ("study", 7_u64, true);
        let second = ("study", 7_u64, true);

        let first_id = ContentId::derive(&first).expect("id");
        let second_id = ContentId::derive(&second).expect("id");

        assert_eq!(first_id, second_id);
    }

    #[test]
    fn schema_envelope_round_trips_through_cbor() {
        let envelope = SchemaEnvelope::new(
            "burn_p2p.test",
            semver::Version::new(0, 1, 0),
            MetricValue::Float(1.25),
        );

        let bytes = envelope.to_cbor_vec().expect("encode");
        let decoded: SchemaEnvelope<MetricValue> = from_cbor_slice(&bytes).expect("decode");

        assert_eq!(decoded, envelope);
    }

    #[test]
    fn activation_respects_window_boundaries() {
        let activation = WindowActivation {
            activation_window: WindowId(12),
            grace_windows: 2,
        };

        assert!(!activation.becomes_active_at(WindowId(11)));
        assert!(activation.becomes_active_at(WindowId(12)));
        assert!(activation.becomes_active_at(WindowId(13)));
    }

    #[test]
    fn signed_payload_derives_a_payload_id() {
        let payload = SchemaEnvelope::new("burn_p2p.test", semver::Version::new(0, 1, 0), "ok");
        let signature = super::SignatureMetadata {
            signer: crate::id::PeerId::new("peer-a"),
            key_id: "key-1".into(),
            algorithm: super::SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "deadbeef".into(),
        };

        let signed = super::SignedPayload::new(payload.clone(), signature).expect("sign");

        assert_eq!(signed.payload_id, payload.content_id().expect("hash"));
    }

    #[test]
    fn node_certificate_round_trips_and_exposes_claims() {
        let claims = NodeCertificateClaims {
            network_id: crate::id::NetworkId::new("network-a"),
            project_family_id: crate::id::ProjectFamilyId::new("family-a"),
            release_train_hash: ContentId::new("train-a"),
            target_artifact_hash: ContentId::new("artifact-native-a"),
            peer_id: crate::id::PeerId::new("peer-a"),
            peer_public_key_hex: "001122".into(),
            principal_id: PrincipalId::new("principal-a"),
            provider: AuthProvider::GitHub,
            granted_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
            experiment_scopes: BTreeSet::from([
                ExperimentScope::Connect,
                ExperimentScope::Train {
                    experiment_id: crate::id::ExperimentId::new("exp-a"),
                },
            ]),
            client_policy_hash: Some(ContentId::new("policy-a")),
            not_before: Utc::now(),
            not_after: Utc::now(),
            serial: 7,
            revocation_epoch: RevocationEpoch(3),
        };
        let signature = super::SignatureMetadata {
            signer: crate::id::PeerId::new("authority-a"),
            key_id: "key-1".into(),
            algorithm: super::SignatureAlgorithm::Ed25519,
            signed_at: Utc::now(),
            signature_hex: "deadbeef".into(),
        };
        let certificate =
            NodeCertificate::new(semver::Version::new(0, 1, 0), claims.clone(), signature)
                .expect("certificate");

        let bytes = certificate.to_cbor_vec().expect("encode");
        let decoded: NodeCertificate = from_cbor_slice(&bytes).expect("decode");

        assert_eq!(decoded.claims(), &claims);
        assert_eq!(decoded.claims().revocation_epoch, RevocationEpoch(3));
    }

    #[test]
    fn release_and_network_manifests_round_trip() {
        let workload = SupportedWorkload {
            workload_id: crate::id::WorkloadId::new("lm_125m"),
            workload_name: "LM 125M".into(),
            model_program_hash: ContentId::new("model-program"),
            checkpoint_format_hash: ContentId::new("checkpoint-format"),
            supported_revision_family: ContentId::new("revision-family"),
            resource_class: "gpu-small".into(),
        };
        let release = ClientReleaseManifest {
            project_family_id: crate::id::ProjectFamilyId::new("family-a"),
            release_train_hash: ContentId::new("train-a"),
            target_artifact_id: "native-linux-x86_64".into(),
            target_artifact_hash: ContentId::new("artifact-native-a"),
            target_platform: ClientPlatform::Native,
            app_semver: semver::Version::new(0, 2, 0),
            git_commit: "deadbeef".into(),
            cargo_lock_hash: ContentId::new("cargo-lock"),
            burn_version_string: "0.21.0-pre.2".into(),
            enabled_features_hash: ContentId::new("features"),
            protocol_major: 1,
            supported_workloads: vec![workload],
            built_at: Utc::now(),
        };
        let network = NetworkManifest {
            network_id: crate::id::NetworkId::new("network-a"),
            project_family_id: crate::id::ProjectFamilyId::new("family-a"),
            protocol_major: 1,
            required_release_train_hash: ContentId::new("train-a"),
            allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact-native-a")]),
            authority_public_keys: vec!["001122".into()],
            bootstrap_addrs: vec!["/ip4/127.0.0.1/tcp/4101".into()],
            auth_policy_hash: ContentId::new("auth-policy"),
            created_at: Utc::now(),
            description: "test network".into(),
        };

        let release_bytes = release.to_cbor_vec().expect("encode release");
        let network_bytes = network.to_cbor_vec().expect("encode network");

        let decoded_release: ClientReleaseManifest =
            from_cbor_slice(&release_bytes).expect("decode release");
        let decoded_network: NetworkManifest =
            from_cbor_slice(&network_bytes).expect("decode network");

        assert_eq!(decoded_release, release);
        assert_eq!(decoded_network, network);
    }

    #[test]
    fn experiment_scope_filters_experiment_specific_scopes() {
        let experiment_id = crate::id::ExperimentId::new("exp-1");

        assert!(
            ExperimentScope::Train {
                experiment_id: experiment_id.clone(),
            }
            .applies_to_experiment(&experiment_id)
        );
        assert!(ExperimentScope::Discover.allows_directory_discovery());
        assert!(
            !ExperimentScope::Archive {
                experiment_id: crate::id::ExperimentId::new("exp-2"),
            }
            .applies_to_experiment(&experiment_id)
        );
    }

    #[test]
    fn experiment_directory_entry_round_trips() {
        let entry = ExperimentDirectoryEntry {
            network_id: crate::id::NetworkId::new("network-a"),
            study_id: crate::id::StudyId::new("study-a"),
            experiment_id: crate::id::ExperimentId::new("exp-a"),
            workload_id: crate::id::WorkloadId::new("lm_125m"),
            display_name: "Example".into(),
            model_schema_hash: ContentId::new("model-a"),
            dataset_view_id: crate::id::DatasetViewId::new("view-a"),
            resource_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
                minimum_device_memory_bytes: Some(1024),
                minimum_system_memory_bytes: Some(4096),
                estimated_download_bytes: 8192,
                estimated_window_seconds: 30,
            },
            visibility: ExperimentVisibility::OptIn,
            opt_in_policy: ExperimentOptInPolicy::Scoped,
            current_revision_id: crate::id::RevisionId::new("rev-a"),
            current_head_id: Some(crate::id::HeadId::new("head-a")),
            allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu, PeerRole::Evaluator]),
            allowed_scopes: BTreeSet::from([
                ExperimentScope::Discover,
                ExperimentScope::Train {
                    experiment_id: crate::id::ExperimentId::new("exp-a"),
                },
            ]),
            metadata: BTreeMap::from([("family".into(), "demo".into())]),
        };

        let bytes = entry.to_cbor_vec().expect("encode");
        let decoded: ExperimentDirectoryEntry = from_cbor_slice(&bytes).expect("decode");
        assert_eq!(decoded, entry);
    }

    #[test]
    fn leaderboard_snapshot_round_trips() {
        let now = Utc::now();
        let snapshot = LeaderboardSnapshot {
            network_id: crate::NetworkId::new("net-social"),
            score_version: "leaderboard_score_v1".into(),
            entries: vec![LeaderboardEntry {
                identity: LeaderboardIdentity {
                    principal_id: Some(PrincipalId::new("alice")),
                    peer_ids: BTreeSet::from([PeerId::new("peer-1"), PeerId::new("peer-2")]),
                    label: "alice".into(),
                    social_profile: Some(SocialProfile {
                        principal_id: PrincipalId::new("alice"),
                        display_name: Some("Alice".into()),
                        avatar_url: Some("https://example.invalid/alice.png".into()),
                        profile_url: None,
                        org_slug: Some("openai".into()),
                        team_slug: Some("research".into()),
                        visibility: IdentityVisibility::PublicProfile,
                    }),
                },
                accepted_work_score: 5.0,
                quality_weighted_impact_score: 3.5,
                validation_service_score: 1.0,
                artifact_serving_score: 0.0,
                leaderboard_score_v1: 7.75,
                accepted_receipt_count: 2,
                last_receipt_at: Some(now),
                badges: vec![BadgeAward {
                    kind: BadgeKind::HelpedPromoteCanonicalHead,
                    label: "Helped Promote Canonical Head".into(),
                    awarded_at: Some(now),
                    detail: Some("Accepted work landed in a promoted merge.".into()),
                }],
            }],
            captured_at: now,
        };

        let encoded = snapshot.to_cbor_vec().expect("encode leaderboard");
        let decoded: LeaderboardSnapshot = from_cbor_slice(&encoded).expect("decode leaderboard");

        assert_eq!(decoded, snapshot);
        assert_eq!(
            decoded.entries[0]
                .contribution_rollup()
                .leaderboard_score_v1,
            7.75
        );
    }

    #[test]
    fn edge_service_manifest_round_trips() {
        let manifest = EdgeServiceManifest {
            edge_id: PeerId::new("edge-1"),
            network_id: crate::NetworkId::new("network-a"),
            portal_mode: PortalMode::Interactive,
            browser_mode: BrowserMode::Verifier,
            available_auth_providers: BTreeSet::from([
                EdgeAuthProvider::Static,
                EdgeAuthProvider::Oidc,
            ]),
            social_mode: SocialMode::Private,
            profile_mode: ProfileMode::Private,
            admin_mode: AdminMode::Rbac,
            metrics_mode: MetricsMode::OpenMetrics,
            compiled_feature_set: CompiledFeatureSet {
                features: BTreeSet::from([
                    EdgeFeature::AdminHttp,
                    EdgeFeature::Portal,
                    EdgeFeature::AuthOidc,
                ]),
            },
            configured_service_set: ConfiguredServiceSet {
                features: BTreeSet::from([
                    EdgeFeature::AdminHttp,
                    EdgeFeature::Portal,
                    EdgeFeature::AuthOidc,
                ]),
            },
            active_feature_set: ActiveServiceSet {
                features: BTreeSet::from([
                    EdgeFeature::AdminHttp,
                    EdgeFeature::Portal,
                    EdgeFeature::AuthOidc,
                ]),
            },
            generated_at: Utc::now(),
        };

        let encoded = manifest
            .to_cbor_vec()
            .expect("encode edge service manifest");
        let decoded: EdgeServiceManifest =
            from_cbor_slice(&encoded).expect("decode edge service manifest");
        assert_eq!(decoded, manifest);
    }

    #[test]
    fn lag_and_backpressure_policies_round_trip() {
        let payload = (
            LagState::CatchupRequired,
            LagPolicy {
                max_head_lag_before_catchup: 2,
                max_head_lag_before_block: 5,
                max_head_lag_before_full_rebase: 11,
                max_window_skew_before_lease_revoke: 3,
            },
            MergeWindowMissPolicy::RebaseRequired,
            BackpressurePolicy {
                max_in_flight_chunk_requests_per_peer: 12,
                max_in_flight_updates_per_peer: 6,
                reducer_queue_depth_limit: 96,
                upload_concurrency_limit: 3,
                browser_transfer_budget_bytes: 8 * 1024 * 1024,
                portal_feed_sample_rate: 2,
            },
        );

        let encoded = deterministic_cbor(&payload).expect("encode policy payload");
        let decoded: (
            LagState,
            LagPolicy,
            MergeWindowMissPolicy,
            BackpressurePolicy,
        ) = from_cbor_slice(&encoded).expect("decode policy payload");

        assert_eq!(decoded.0, LagState::CatchupRequired);
        assert_eq!(decoded.1.max_head_lag_before_full_rebase, 11);
        assert_eq!(decoded.2, MergeWindowMissPolicy::RebaseRequired);
        assert_eq!(decoded.3.reducer_queue_depth_limit, 96);
    }

    #[test]
    fn merge_topology_policy_defaults_to_replicated_rendezvous_dag() {
        let policy = MergeTopologyPolicy::default();

        assert_eq!(policy.strategy, MergeStrategy::ReplicatedRendezvousDag);
        assert_eq!(policy.reducer_replication, 2);
        assert_eq!(policy.target_leaf_cohort, 16);
        assert_eq!(policy.upper_fanin, 4);
        assert!(policy.promotion_policy.apply_single_root_ema);
    }
}
