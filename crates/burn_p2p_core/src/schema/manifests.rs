use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the dataset.
pub struct DatasetManifest {
    /// The dataset ID.
    pub dataset_id: DatasetId,
    /// The source uri.
    pub source_uri: String,
    /// The format.
    pub format: String,
    /// The manifest hash.
    pub manifest_hash: ContentId,
    /// The metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a dataset view.
pub struct DatasetView {
    /// The dataset view ID.
    pub dataset_view_id: DatasetViewId,
    /// The dataset ID.
    pub dataset_id: DatasetId,
    /// The preprocessing hash.
    pub preprocessing_hash: ContentId,
    /// The tokenizer hash.
    pub tokenizer_hash: Option<ContentId>,
    /// The manifest hash.
    pub manifest_hash: ContentId,
    /// The metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a supported workload.
pub struct SupportedWorkload {
    /// The workload ID.
    pub workload_id: WorkloadId,
    /// The workload name.
    pub workload_name: String,
    /// The model program hash.
    pub model_program_hash: ContentId,
    /// The checkpoint format hash.
    pub checkpoint_format_hash: ContentId,
    /// The supported revision family.
    pub supported_revision_family: ContentId,
    /// The resource class.
    pub resource_class: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the target artifact.
pub struct TargetArtifactManifest {
    /// The target artifact ID.
    pub target_artifact_id: String,
    /// The target artifact hash.
    pub target_artifact_hash: ContentId,
    /// The platform.
    pub platform: ClientPlatform,
    /// The built at.
    pub built_at: DateTime<Utc>,
}

impl TargetArtifactManifest {
    /// Performs the target kind operation.
    pub fn target_kind(&self) -> ArtifactTargetKind {
        ArtifactTargetKind::parse(&self.target_artifact_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Canonical compatibility descriptor for one release train.
///
/// A release train fixes the project family, protocol major, and bounded set of
/// approved target artifacts that may join the network together. This replaces
/// older single-release-hash gating with one strict train that can still carry
/// multiple native and browser artifact builds.
pub struct ReleaseTrainManifest {
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The release train hash.
    pub release_train_hash: ContentId,
    /// The app semver.
    pub app_semver: Version,
    /// The git commit.
    pub git_commit: String,
    /// The cargo lock hash.
    pub cargo_lock_hash: ContentId,
    /// The burn version string.
    pub burn_version_string: String,
    /// The enabled features hash.
    pub enabled_features_hash: ContentId,
    /// The protocol major.
    pub protocol_major: u16,
    /// The supported workloads.
    pub supported_workloads: Vec<SupportedWorkload>,
    /// The approved target artifacts.
    pub approved_target_artifacts: Vec<TargetArtifactManifest>,
    /// The built at.
    pub built_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the client release.
pub struct ClientReleaseManifest {
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The release train hash.
    pub release_train_hash: ContentId,
    /// The target artifact ID.
    pub target_artifact_id: String,
    /// The target artifact hash.
    pub target_artifact_hash: ContentId,
    /// The target platform.
    pub target_platform: ClientPlatform,
    /// The app semver.
    pub app_semver: Version,
    /// The git commit.
    pub git_commit: String,
    /// The cargo lock hash.
    pub cargo_lock_hash: ContentId,
    /// The burn version string.
    pub burn_version_string: String,
    /// The enabled features hash.
    pub enabled_features_hash: ContentId,
    /// The protocol major.
    pub protocol_major: u16,
    /// The supported workloads.
    pub supported_workloads: Vec<SupportedWorkload>,
    /// The built at.
    pub built_at: DateTime<Utc>,
}

impl ClientReleaseManifest {
    /// Performs the target kind operation.
    pub fn target_kind(&self) -> ArtifactTargetKind {
        ArtifactTargetKind::parse(&self.target_artifact_id)
    }
}

/// Builder for one supported-workload manifest entry.
pub struct SupportedWorkloadBuilder {
    workload_id: WorkloadId,
    workload_name: String,
    model_program_hash: ContentId,
    checkpoint_format_hash: ContentId,
    supported_revision_family: ContentId,
    resource_class: String,
}

impl SupportedWorkloadBuilder {
    /// Creates a new builder.
    pub fn new(
        workload_id: WorkloadId,
        workload_name: impl Into<String>,
        model_program_hash: ContentId,
        checkpoint_format_hash: ContentId,
    ) -> Self {
        Self {
            supported_revision_family: ContentId::new(format!(
                "{}-revision-family",
                workload_id.as_str()
            )),
            resource_class: "standard".into(),
            workload_id,
            workload_name: workload_name.into(),
            model_program_hash,
            checkpoint_format_hash,
        }
    }

    /// Overrides the supported revision-family hash.
    pub fn with_supported_revision_family(mut self, supported_revision_family: ContentId) -> Self {
        self.supported_revision_family = supported_revision_family;
        self
    }

    /// Overrides the resource class label.
    pub fn with_resource_class(mut self, resource_class: impl Into<String>) -> Self {
        self.resource_class = resource_class.into();
        self
    }

    /// Builds the final workload manifest entry.
    pub fn build(self) -> SupportedWorkload {
        SupportedWorkload {
            workload_id: self.workload_id,
            workload_name: self.workload_name,
            model_program_hash: self.model_program_hash,
            checkpoint_format_hash: self.checkpoint_format_hash,
            supported_revision_family: self.supported_revision_family,
            resource_class: self.resource_class,
        }
    }
}

/// Builder for one client release manifest.
pub struct ClientReleaseManifestBuilder {
    project_family_id: ProjectFamilyId,
    release_train_hash: ContentId,
    target_artifact_id: String,
    target_artifact_hash: ContentId,
    target_platform: ClientPlatform,
    app_semver: Version,
    git_commit: String,
    cargo_lock_hash: ContentId,
    burn_version_string: String,
    enabled_features_hash: ContentId,
    protocol_major: u16,
    supported_workloads: Vec<SupportedWorkload>,
    built_at: DateTime<Utc>,
}

impl ClientReleaseManifestBuilder {
    /// Creates a new builder with the minimum required identity fields.
    pub fn new(
        project_family_id: ProjectFamilyId,
        release_train_hash: ContentId,
        target_artifact_id: impl Into<String>,
        target_artifact_hash: ContentId,
        target_platform: ClientPlatform,
        app_semver: Version,
        protocol_major: u16,
    ) -> Self {
        Self {
            project_family_id,
            release_train_hash,
            target_artifact_id: target_artifact_id.into(),
            target_artifact_hash,
            target_platform,
            app_semver,
            git_commit: "unknown".into(),
            cargo_lock_hash: ContentId::new("unknown-cargo-lock"),
            burn_version_string: "unknown".into(),
            enabled_features_hash: ContentId::new("unknown-enabled-features"),
            protocol_major,
            supported_workloads: Vec::new(),
            built_at: Utc::now(),
        }
    }

    /// Sets the git commit string.
    pub fn with_git_commit(mut self, git_commit: impl Into<String>) -> Self {
        self.git_commit = git_commit.into();
        self
    }

    /// Sets the cargo.lock hash.
    pub fn with_cargo_lock_hash(mut self, cargo_lock_hash: ContentId) -> Self {
        self.cargo_lock_hash = cargo_lock_hash;
        self
    }

    /// Sets the burn version string.
    pub fn with_burn_version_string(mut self, burn_version_string: impl Into<String>) -> Self {
        self.burn_version_string = burn_version_string.into();
        self
    }

    /// Sets the enabled-features hash.
    pub fn with_enabled_features_hash(mut self, enabled_features_hash: ContentId) -> Self {
        self.enabled_features_hash = enabled_features_hash;
        self
    }

    /// Sets the build timestamp.
    pub fn with_built_at(mut self, built_at: DateTime<Utc>) -> Self {
        self.built_at = built_at;
        self
    }

    /// Adds one supported workload.
    pub fn with_supported_workload(mut self, supported_workload: SupportedWorkload) -> Self {
        self.supported_workloads.push(supported_workload);
        self
    }

    /// Builds the final client release manifest.
    pub fn build(self) -> ClientReleaseManifest {
        ClientReleaseManifest {
            project_family_id: self.project_family_id,
            release_train_hash: self.release_train_hash,
            target_artifact_id: self.target_artifact_id,
            target_artifact_hash: self.target_artifact_hash,
            target_platform: self.target_platform,
            app_semver: self.app_semver,
            git_commit: self.git_commit,
            cargo_lock_hash: self.cargo_lock_hash,
            burn_version_string: self.burn_version_string,
            enabled_features_hash: self.enabled_features_hash,
            protocol_major: self.protocol_major,
            supported_workloads: self.supported_workloads,
            built_at: self.built_at,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the network.
pub struct NetworkManifest {
    /// The network ID.
    pub network_id: NetworkId,
    /// The project family ID.
    pub project_family_id: ProjectFamilyId,
    /// The protocol major.
    pub protocol_major: u16,
    /// The required release train hash.
    pub required_release_train_hash: ContentId,
    /// The allowed target artifact hashes.
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
    /// The authority public keys.
    pub authority_public_keys: Vec<String>,
    /// The bootstrap addrs.
    pub bootstrap_addrs: Vec<String>,
    /// The auth policy hash.
    pub auth_policy_hash: ContentId,
    /// The created at.
    pub created_at: DateTime<Utc>,
    /// The description.
    pub description: String,
}

/// Builder for one network manifest.
pub struct NetworkManifestBuilder {
    network_id: NetworkId,
    project_family_id: ProjectFamilyId,
    protocol_major: u16,
    required_release_train_hash: ContentId,
    allowed_target_artifact_hashes: BTreeSet<ContentId>,
    authority_public_keys: Vec<String>,
    bootstrap_addrs: Vec<String>,
    auth_policy_hash: ContentId,
    created_at: DateTime<Utc>,
    description: String,
}

impl NetworkManifestBuilder {
    /// Creates a new builder.
    pub fn new(
        network_id: NetworkId,
        project_family_id: ProjectFamilyId,
        protocol_major: u16,
        required_release_train_hash: ContentId,
        auth_policy_hash: ContentId,
        created_at: DateTime<Utc>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            network_id,
            project_family_id,
            protocol_major,
            required_release_train_hash,
            allowed_target_artifact_hashes: BTreeSet::new(),
            authority_public_keys: Vec::new(),
            bootstrap_addrs: Vec::new(),
            auth_policy_hash,
            created_at,
            description: description.into(),
        }
    }

    /// Adds one allowed target artifact hash.
    pub fn with_allowed_target_artifact_hash(mut self, target_artifact_hash: ContentId) -> Self {
        self.allowed_target_artifact_hashes
            .insert(target_artifact_hash);
        self
    }

    /// Adds one authority public key.
    pub fn with_authority_public_key(mut self, authority_public_key: impl Into<String>) -> Self {
        self.authority_public_keys.push(authority_public_key.into());
        self
    }

    /// Adds one bootstrap address.
    pub fn with_bootstrap_addr(mut self, bootstrap_addr: impl Into<String>) -> Self {
        self.bootstrap_addrs.push(bootstrap_addr.into());
        self
    }

    /// Builds the final network manifest.
    pub fn build(self) -> NetworkManifest {
        NetworkManifest {
            network_id: self.network_id,
            project_family_id: self.project_family_id,
            protocol_major: self.protocol_major,
            required_release_train_hash: self.required_release_train_hash,
            allowed_target_artifact_hashes: self.allowed_target_artifact_hashes,
            authority_public_keys: self.authority_public_keys,
            bootstrap_addrs: self.bootstrap_addrs,
            auth_policy_hash: self.auth_policy_hash,
            created_at: self.created_at,
            description: self.description,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes one validator-set member for an authority epoch.
pub struct ValidatorSetMember {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The principal ID when the validator has an authenticated identity.
    pub principal_id: Option<PrincipalId>,
    /// The admitted runtime roles for this validator.
    pub roles: PeerRoleSet,
    /// The vote weight contributed by this validator.
    pub vote_weight: u16,
    /// The minimum attestation level expected from this validator.
    pub attestation_level: AttestationLevel,
    /// Freeform operator metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Declares the active validator set for one authority epoch.
pub struct ValidatorSetManifest {
    /// The network ID.
    pub network_id: NetworkId,
    /// The validator set ID.
    pub validator_set_id: ContentId,
    /// The authority epoch this validator set belongs to.
    pub authority_epoch: u64,
    /// The total vote weight required for canonical promotion.
    pub quorum_weight: u16,
    /// The validator members.
    pub members: Vec<ValidatorSetMember>,
    /// The created at timestamp.
    pub created_at: DateTime<Utc>,
    /// Freeform operator metadata.
    pub metadata: BTreeMap<String, String>,
}

impl ValidatorSetManifest {
    /// Returns the total configured voting weight.
    pub fn total_vote_weight(&self) -> u16 {
        self.members.iter().map(|member| member.vote_weight).sum()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported authority evidence categories.
pub enum AuthorityEvidenceCategory {
    /// Uses the validator misbehavior variant.
    ValidatorMisbehavior,
    /// Uses the revocation rollout variant.
    RevocationRollout,
    /// Uses the policy rollout variant.
    PolicyRollout,
    /// Uses the availability incident variant.
    AvailabilityIncident,
    /// Uses the operator note variant.
    OperatorNote,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Records one authority-plane evidence event.
pub struct AuthorityEvidenceRecord {
    /// The evidence ID.
    pub evidence_id: ContentId,
    /// The network ID.
    pub network_id: NetworkId,
    /// The authority epoch.
    pub authority_epoch: u64,
    /// The subject peer ID when the evidence is peer-scoped.
    pub subject_peer_id: Option<PeerId>,
    /// The subject principal ID when the evidence is identity-scoped.
    pub subject_principal_id: Option<PrincipalId>,
    /// The evidence category.
    pub category: AuthorityEvidenceCategory,
    /// Human-readable summary.
    pub summary: String,
    /// Supporting content-addressed references.
    pub supporting_refs: Vec<ContentId>,
    /// The recorded at timestamp.
    pub recorded_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Declares one authority epoch transition.
pub struct AuthorityEpochManifest {
    /// The network ID.
    pub network_id: NetworkId,
    /// The authority epoch number.
    pub authority_epoch: u64,
    /// The validator set ID active in this epoch.
    pub validator_set_id: ContentId,
    /// The minimum auth revocation epoch bundled with this authority epoch.
    pub minimum_revocation_epoch: RevocationEpoch,
    /// The previous authority epoch when this manifest rotates from an older set.
    pub supersedes_authority_epoch: Option<u64>,
    /// The created at timestamp.
    pub created_at: DateTime<Utc>,
    /// Freeform operator metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the experiment.
pub struct ExperimentManifest {
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The workload ID.
    pub workload_id: WorkloadId,
    /// The display name.
    pub display_name: String,
    /// The visibility.
    pub visibility: ExperimentVisibility,
    /// The current revision ID.
    pub current_revision_id: RevisionId,
    /// The allowed roles.
    pub allowed_roles: PeerRoleSet,
    /// The join policy.
    pub join_policy: ExperimentOptInPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Declares which browser roles a revision is willing to expose.
///
/// This policy is evaluated together with browser capability probes and edge
/// service configuration. It does not guarantee that a browser will become a
/// trainer or verifier; it only describes which roles the revision would allow
/// if the device and edge are eligible.
pub struct BrowserRolePolicy {
    /// The observer.
    pub observer: bool,
    /// The verifier.
    pub verifier: bool,
    /// The trainer wgpu.
    pub trainer_wgpu: bool,
    /// The fallback.
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
/// Enumerates browser visibility policies.
pub enum BrowserVisibilityPolicy {
    /// Uses the hidden variant.
    Hidden,
    /// Uses the portal listed variant.
    AppListed,
    /// Uses the authenticated portal variant.
    AuthenticatedApp,
    /// Uses the swarm eligible variant.
    SwarmEligible,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported identity visibility values.
pub enum IdentityVisibility {
    /// Uses the anonymous variant.
    Anonymous,
    /// Uses the aggregate only variant.
    AggregateOnly,
    /// Uses the public display name variant.
    PublicDisplayName,
    /// Uses the public profile variant.
    PublicProfile,
    /// Uses the public org variant.
    PublicOrg,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a social profile.
pub struct SocialProfile {
    /// The principal ID.
    pub principal_id: PrincipalId,
    /// The display name.
    pub display_name: Option<String>,
    /// The avatar URL.
    pub avatar_url: Option<String>,
    /// The profile URL.
    pub profile_url: Option<String>,
    /// The org slug.
    pub org_slug: Option<String>,
    /// The team slug.
    pub team_slug: Option<String>,
    /// The visibility.
    pub visibility: IdentityVisibility,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported badge kinds.
pub enum BadgeKind {
    /// Uses the first accepted update kind.
    FirstAcceptedUpdate,
    /// Uses the first verified head kind.
    FirstVerifiedHead,
    /// Uses the seven day streak kind.
    SevenDayStreak,
    /// Uses the top browser contributor kind.
    TopBrowserContributor,
    /// Uses the top validator contributor kind.
    TopValidatorContributor,
    /// Uses the helped promote canonical head kind.
    HelpedPromoteCanonicalHead,
    /// Uses the team contributor kind.
    TeamContributor,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a badge award.
pub struct BadgeAward {
    /// The kind.
    pub kind: BadgeKind,
    /// The label.
    pub label: String,
    /// The awarded at.
    pub awarded_at: Option<DateTime<Utc>>,
    /// The detail.
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a leaderboard identity.
pub struct LeaderboardIdentity {
    /// The principal ID.
    pub principal_id: Option<PrincipalId>,
    /// The peer IDs.
    pub peer_ids: BTreeSet<PeerId>,
    /// The label.
    pub label: String,
    #[serde(default)]
    /// The social profile.
    pub social_profile: Option<SocialProfile>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a contribution rollup.
pub struct ContributionRollup {
    /// The accepted work score.
    pub accepted_work_score: f64,
    /// The quality weighted impact score.
    pub quality_weighted_impact_score: f64,
    /// The validation service score.
    pub validation_service_score: f64,
    /// The artifact serving score.
    pub artifact_serving_score: f64,
    /// The leaderboard score v1.
    pub leaderboard_score_v1: f64,
    /// The accepted receipt count.
    pub accepted_receipt_count: u64,
    /// The last receipt at.
    pub last_receipt_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported lag states.
pub enum LagState {
    /// Uses the current variant.
    Current,
    /// Uses the slightly behind variant.
    SlightlyBehind,
    /// Uses the catchup required variant.
    CatchupRequired,
    /// Uses the lease blocked variant.
    LeaseBlocked,
    /// Uses the rebase required variant.
    RebaseRequired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures the lag policy.
pub struct LagPolicy {
    /// The max head lag before catchup.
    pub max_head_lag_before_catchup: u64,
    /// The max head lag before block.
    pub max_head_lag_before_block: u64,
    /// The max head lag before full rebase.
    pub max_head_lag_before_full_rebase: u64,
    /// The max window skew before lease revoke.
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
/// Enumerates merge window miss policies.
pub enum MergeWindowMissPolicy {
    /// Uses the catchup only variant.
    CatchupOnly,
    #[default]
    /// Uses the lease blocked variant.
    LeaseBlocked,
    /// Uses the rebase required variant.
    RebaseRequired,
}

impl MergeWindowMissPolicy {
    /// Returns the str view.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CatchupOnly => "catchup-only",
            Self::LeaseBlocked => "lease-blocked",
            Self::RebaseRequired => "rebase-required",
        }
    }

    /// Performs the parse operation.
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
/// Configures the backpressure policy.
pub struct BackpressurePolicy {
    /// The max in flight chunk requests per peer.
    pub max_in_flight_chunk_requests_per_peer: u16,
    /// The max in flight updates per peer.
    pub max_in_flight_updates_per_peer: u16,
    /// The reducer queue depth limit.
    pub reducer_queue_depth_limit: u16,
    /// The upload concurrency limit.
    pub upload_concurrency_limit: u16,
    /// The browser transfer budget bytes.
    pub browser_transfer_budget_bytes: u64,
    /// The portal feed sample rate.
    pub app_feed_sample_rate: u16,
}

impl Default for BackpressurePolicy {
    fn default() -> Self {
        Self {
            max_in_flight_chunk_requests_per_peer: 8,
            max_in_flight_updates_per_peer: 4,
            reducer_queue_depth_limit: 64,
            upload_concurrency_limit: 4,
            browser_transfer_budget_bytes: 64 * 1024 * 1024,
            app_feed_sample_rate: 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a leaderboard entry.
pub struct LeaderboardEntry {
    /// The identity.
    pub identity: LeaderboardIdentity,
    /// The accepted work score.
    pub accepted_work_score: f64,
    /// The quality weighted impact score.
    pub quality_weighted_impact_score: f64,
    /// The validation service score.
    pub validation_service_score: f64,
    /// The artifact serving score.
    pub artifact_serving_score: f64,
    /// The leaderboard score v1.
    pub leaderboard_score_v1: f64,
    /// The accepted receipt count.
    pub accepted_receipt_count: u64,
    /// The last receipt at.
    pub last_receipt_at: Option<DateTime<Utc>>,
    #[serde(default)]
    /// The badges.
    pub badges: Vec<BadgeAward>,
}

impl LeaderboardEntry {
    /// Performs the contribution rollup operation.
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
/// Captures a snapshot of leaderboard.
pub struct LeaderboardSnapshot {
    /// The network ID.
    pub network_id: NetworkId,
    /// The score version.
    pub score_version: String,
    /// The entries.
    pub entries: Vec<LeaderboardEntry>,
    /// The captured at.
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Describes the revision.
pub struct RevisionManifest {
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The workload ID.
    pub workload_id: WorkloadId,
    /// The required release train hash.
    pub required_release_train_hash: ContentId,
    /// The model schema hash.
    pub model_schema_hash: ContentId,
    /// The checkpoint format hash.
    pub checkpoint_format_hash: ContentId,
    /// The dataset view ID.
    pub dataset_view_id: DatasetViewId,
    /// The training config hash.
    pub training_config_hash: ContentId,
    /// The merge topology policy hash.
    pub merge_topology_policy_hash: ContentId,
    /// The slot requirements.
    pub slot_requirements: ExperimentResourceRequirements,
    /// The activation window.
    pub activation_window: WindowActivation,
    #[serde(default)]
    /// The lag policy.
    pub lag_policy: LagPolicy,
    #[serde(default)]
    /// The merge window miss policy.
    pub merge_window_miss_policy: MergeWindowMissPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Robustness policy carried by the revision.
    pub robustness_policy: Option<RobustnessPolicy>,
    /// The browser enabled.
    pub browser_enabled: bool,
    /// The browser role policy.
    pub browser_role_policy: BrowserRolePolicy,
    /// The max browser checkpoint bytes.
    pub max_browser_checkpoint_bytes: Option<u64>,
    /// The max browser window secs.
    pub max_browser_window_secs: Option<u64>,
    /// The max browser shard bytes.
    pub max_browser_shard_bytes: Option<u64>,
    /// The requires WebGPU.
    pub requires_webgpu: bool,
    /// The max browser batch size.
    pub max_browser_batch_size: Option<u32>,
    /// The recommended browser precision.
    pub recommended_browser_precision: Option<Precision>,
    /// The visibility policy.
    pub visibility_policy: BrowserVisibilityPolicy,
    /// The description.
    pub description: String,
}

impl RevisionManifest {
    /// Performs the effective lag policy operation.
    pub fn effective_lag_policy(&self) -> LagPolicy {
        self.lag_policy.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_builders_cover_single_family_single_workload_setup() {
        let workload = SupportedWorkloadBuilder::new(
            WorkloadId::new("dragon"),
            "Burn Dragon",
            ContentId::new("program"),
            ContentId::new("checkpoint"),
        )
        .with_supported_revision_family(ContentId::new("family"))
        .with_resource_class("gpu-medium")
        .build();

        let release = ClientReleaseManifestBuilder::new(
            ProjectFamilyId::new("dragon-family"),
            ContentId::new("release-train"),
            "native-linux-x86_64",
            ContentId::new("artifact"),
            ClientPlatform::Native,
            Version::new(0, 21, 0),
            1,
        )
        .with_built_at(Utc::now())
        .with_git_commit("deadbeef")
        .with_cargo_lock_hash(ContentId::new("lock"))
        .with_burn_version_string("0.18")
        .with_enabled_features_hash(ContentId::new("features"))
        .with_supported_workload(workload.clone())
        .build();

        let network = NetworkManifestBuilder::new(
            NetworkId::new("dragon-network"),
            ProjectFamilyId::new("dragon-family"),
            1,
            release.release_train_hash.clone(),
            ContentId::new("auth-policy"),
            Utc::now(),
            "dragon network",
        )
        .with_allowed_target_artifact_hash(release.target_artifact_hash.clone())
        .with_authority_public_key("validator-key")
        .with_bootstrap_addr("/ip4/127.0.0.1/tcp/4001")
        .build();

        assert_eq!(release.supported_workloads, vec![workload]);
        assert!(
            network
                .allowed_target_artifact_hashes
                .contains(&ContentId::new("artifact"))
        );
        assert_eq!(network.authority_public_keys, vec!["validator-key"]);
        assert_eq!(network.bootstrap_addrs, vec!["/ip4/127.0.0.1/tcp/4001"]);
    }
}
