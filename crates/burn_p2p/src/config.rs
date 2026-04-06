use super::*;

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Enumerates the supported identity config values.
pub enum IdentityConfig {
    #[default]
    /// Uses the ephemeral variant.
    Ephemeral,
    /// Uses the persistent variant.
    Persistent,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Configures storage.
pub struct StorageConfig {
    /// The root.
    pub root: PathBuf,
}

impl StorageConfig {
    /// Creates a new value.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Performs the state dir operation.
    pub fn state_dir(&self) -> PathBuf {
        self.root.join("state")
    }

    /// Performs the artifacts dir operation.
    pub fn artifacts_dir(&self) -> PathBuf {
        self.root.join("artifacts")
    }

    /// Performs the manifests dir operation.
    pub fn manifests_dir(&self) -> PathBuf {
        self.artifacts_dir().join("manifests")
    }

    /// Performs the chunks dir operation.
    pub fn chunks_dir(&self) -> PathBuf {
        self.artifacts_dir().join("chunks")
    }

    /// Performs the pins dir operation.
    pub fn pins_dir(&self) -> PathBuf {
        self.artifacts_dir().join("pins")
    }

    /// Performs the receipts dir operation.
    pub fn receipts_dir(&self) -> PathBuf {
        self.root.join("receipts")
    }

    /// Performs the leases dir operation.
    pub fn leases_dir(&self) -> PathBuf {
        self.root.join("leases")
    }

    /// Performs the auth dir operation.
    pub fn auth_dir(&self) -> PathBuf {
        self.root.join("auth")
    }

    /// Performs the transfers dir operation.
    pub fn transfers_dir(&self) -> PathBuf {
        self.state_dir().join("transfers")
    }

    /// Performs the heads dir operation.
    pub fn heads_dir(&self) -> PathBuf {
        self.root.join("heads")
    }

    /// Performs the metrics dir operation.
    pub fn metrics_dir(&self) -> PathBuf {
        self.root.join("metrics")
    }

    /// Performs the publication dir operation.
    pub fn publication_dir(&self) -> PathBuf {
        self.root.join("publication")
    }

    /// Performs the metrics indexer dir operation.
    pub fn metrics_indexer_dir(&self) -> PathBuf {
        self.metrics_dir().join("indexer")
    }

    /// Performs the dataset cache dir operation.
    pub fn dataset_cache_dir(&self) -> PathBuf {
        self.root.join("datasets")
    }

    pub(crate) fn ensure_layout(&self) -> anyhow::Result<()> {
        for path in [
            self.root.clone(),
            self.state_dir(),
            self.artifacts_dir(),
            self.manifests_dir(),
            self.chunks_dir(),
            self.pins_dir(),
            self.receipts_dir(),
            self.leases_dir(),
            self.auth_dir(),
            self.transfers_dir(),
            self.heads_dir(),
            self.metrics_dir(),
            self.metrics_indexer_dir(),
            self.publication_dir(),
            self.dataset_cache_dir(),
        ] {
            std::fs::create_dir_all(&path)
                .map_err(|error| anyhow::anyhow!("failed to create {}: {error}", path.display()))?;
        }

        Ok(())
    }

    pub(crate) fn identity_path(&self) -> PathBuf {
        self.state_dir().join("identity.key")
    }

    pub(crate) fn known_peers_path(&self) -> PathBuf {
        self.state_dir().join("known-peers.json")
    }

    pub(crate) fn primary_slot_assignment_path(&self) -> PathBuf {
        self.state_dir().join("slot-assignment-primary.json")
    }

    pub(crate) fn auth_state_path(&self) -> PathBuf {
        self.auth_dir().join("auth-state.json")
    }

    pub(crate) fn security_state_path(&self) -> PathBuf {
        self.state_dir().join("security-state.json")
    }

    pub(crate) fn runtime_binding_state_path(&self) -> PathBuf {
        self.state_dir().join("runtime-binding.json")
    }

    pub(crate) fn control_plane_state_path(&self) -> PathBuf {
        self.state_dir().join("control-plane-state.json")
    }

    pub(crate) fn scoped_transfer_path(&self, artifact_id: &ArtifactId) -> PathBuf {
        self.transfers_dir()
            .join(format!("transfer-{}.json", artifact_id.as_str()))
    }

    pub(crate) fn scoped_window_path(&self, experiment: &ExperimentHandle) -> PathBuf {
        self.state_dir().join(format!(
            "window-{}-{}-{}.json",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str()
        ))
    }

    pub(crate) fn scoped_current_head_path(&self, experiment: &ExperimentHandle) -> PathBuf {
        self.state_dir().join(format!(
            "current-head-{}-{}-{}.json",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str()
        ))
    }

    pub(crate) fn scoped_limit_profile_path(&self, experiment: &ExperimentHandle) -> PathBuf {
        self.state_dir().join(format!(
            "limit-profile-{}-{}-{}.json",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str()
        ))
    }

    pub(crate) fn scoped_receipt_path(&self, receipt_id: &ContributionReceiptId) -> PathBuf {
        self.receipts_dir()
            .join(format!("{}.json", receipt_id.as_str()))
    }

    pub(crate) fn scoped_lease_path(
        &self,
        study_id: &StudyId,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
    ) -> PathBuf {
        self.leases_dir().join(format!(
            "lease-{}-{}-{}.json",
            study_id.as_str(),
            experiment_id.as_str(),
            revision_id.as_str()
        ))
    }

    pub(crate) fn scoped_merge_cert_path(&self, merge_cert_id: &MergeCertId) -> PathBuf {
        self.receipts_dir()
            .join(format!("merge-{}.json", merge_cert_id.as_str()))
    }

    pub(crate) fn scoped_head_path(&self, head_id: &HeadId) -> PathBuf {
        self.heads_dir().join(format!("{}.json", head_id.as_str()))
    }

    pub(crate) fn scoped_peer_window_metrics_path(
        &self,
        experiment: &ExperimentHandle,
        lease_id: &LeaseId,
    ) -> PathBuf {
        self.metrics_dir().join(format!(
            "{}{}.json",
            self.scoped_peer_window_metrics_prefix(experiment),
            lease_id.as_str()
        ))
    }

    pub(crate) fn scoped_peer_window_metrics_prefix(
        &self,
        experiment: &ExperimentHandle,
    ) -> String {
        format!(
            "peer-window-{}-{}-{}-",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
        )
    }

    pub(crate) fn scoped_peer_window_metrics_experiment_prefix(
        &self,
        experiment: &ExperimentHandle,
    ) -> String {
        format!(
            "peer-window-{}-{}-",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
        )
    }

    pub(crate) fn scoped_reducer_cohort_metrics_path(
        &self,
        experiment: &ExperimentHandle,
        merge_window_id: &ContentId,
        reducer_group_id: &ContentId,
    ) -> PathBuf {
        self.metrics_dir().join(format!(
            "{}{}-{}.json",
            self.scoped_reducer_cohort_metrics_prefix(experiment),
            merge_window_id.as_str(),
            reducer_group_id.as_str()
        ))
    }

    pub(crate) fn scoped_reducer_cohort_metrics_prefix(
        &self,
        experiment: &ExperimentHandle,
    ) -> String {
        format!(
            "reducer-cohort-{}-{}-{}-",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
        )
    }

    pub(crate) fn scoped_robustness_state_path(&self, experiment: &ExperimentHandle) -> PathBuf {
        self.state_dir().join(format!(
            "robustness-{}-{}-{}.json",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str()
        ))
    }

    pub(crate) fn scoped_cohort_robustness_report_path(
        &self,
        experiment: &ExperimentHandle,
        window_id: &WindowId,
    ) -> PathBuf {
        self.metrics_dir().join(format!(
            "cohort-robustness-{}-{}-{}-{}.json",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
            window_id.0
        ))
    }

    pub(crate) fn scoped_trust_scores_path(&self, experiment: &ExperimentHandle) -> PathBuf {
        self.metrics_dir().join(format!(
            "trust-scores-{}-{}-{}.json",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str()
        ))
    }

    pub(crate) fn scoped_canary_eval_report_path(
        &self,
        experiment: &ExperimentHandle,
        head_id: &HeadId,
    ) -> PathBuf {
        self.metrics_dir().join(format!(
            "canary-report-{}-{}-{}-{}.json",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
            head_id.as_str()
        ))
    }

    pub(crate) fn scoped_reducer_cohort_metrics_experiment_prefix(
        &self,
        experiment: &ExperimentHandle,
    ) -> String {
        format!(
            "reducer-cohort-{}-{}-",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
        )
    }

    pub(crate) fn scoped_head_eval_report_path(
        &self,
        experiment: &ExperimentHandle,
        head_id: &HeadId,
        eval_protocol_id: &ContentId,
    ) -> PathBuf {
        self.metrics_dir().join(format!(
            "{}{}-{}.json",
            self.scoped_head_eval_report_prefix(experiment),
            head_id.as_str(),
            eval_protocol_id.as_str()
        ))
    }

    pub(crate) fn scoped_head_eval_report_prefix(&self, experiment: &ExperimentHandle) -> String {
        format!(
            "head-eval-{}-{}-{}-",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
        )
    }

    pub(crate) fn scoped_head_eval_report_experiment_prefix(
        &self,
        experiment: &ExperimentHandle,
    ) -> String {
        format!(
            "head-eval-{}-{}-",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
        )
    }

    pub(crate) fn scoped_eval_protocol_manifest_path(
        &self,
        experiment: &ExperimentHandle,
        eval_protocol_id: &ContentId,
    ) -> PathBuf {
        self.metrics_dir().join(format!(
            "{}{}.json",
            self.scoped_eval_protocol_manifest_prefix(experiment),
            eval_protocol_id.as_str()
        ))
    }

    pub(crate) fn scoped_eval_protocol_manifest_prefix(
        &self,
        experiment: &ExperimentHandle,
    ) -> String {
        format!(
            "eval-protocol-{}-{}-{}-",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
        )
    }

    pub(crate) fn scoped_eval_protocol_manifest_experiment_prefix(
        &self,
        experiment: &ExperimentHandle,
    ) -> String {
        format!(
            "eval-protocol-{}-{}-",
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
        )
    }
}

impl From<PathBuf> for StorageConfig {
    fn from(value: PathBuf) -> Self {
        Self::new(value)
    }
}

impl From<&str> for StorageConfig {
    /// Performs the from operation.
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for StorageConfig {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Configures dataset.
pub struct DatasetConfig {
    /// The upstream.
    pub upstream: UpstreamAdapter,
}

impl DatasetConfig {
    /// Creates a new value.
    pub fn new(upstream: UpstreamAdapter) -> Self {
        Self { upstream }
    }
}

impl From<UpstreamAdapter> for DatasetConfig {
    fn from(value: UpstreamAdapter) -> Self {
        Self::new(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Configures auth.
pub struct AuthConfig {
    /// The admission policy.
    pub admission_policy: Option<AdmissionPolicy>,
    /// The local peer auth.
    pub local_peer_auth: Option<PeerAuthEnvelope>,
    #[serde(default)]
    /// The trust bundle endpoints.
    pub trust_bundle_endpoints: Vec<String>,
    /// The experiment directory.
    pub experiment_directory: Vec<ExperimentDirectoryEntry>,
}

impl AuthConfig {
    /// Creates a new value.
    pub fn new() -> Self {
        Self {
            admission_policy: None,
            local_peer_auth: None,
            trust_bundle_endpoints: Vec::new(),
            experiment_directory: Vec::new(),
        }
    }

    /// Returns a copy configured with the admission policy.
    pub fn with_admission_policy(mut self, admission_policy: AdmissionPolicy) -> Self {
        self.admission_policy = Some(admission_policy);
        self
    }

    /// Returns a copy configured with the local peer auth.
    pub fn with_local_peer_auth(mut self, local_peer_auth: PeerAuthEnvelope) -> Self {
        self.local_peer_auth = Some(local_peer_auth);
        self
    }

    /// Returns a copy configured with the trust bundle endpoint.
    pub fn with_trust_bundle_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.trust_bundle_endpoints.push(endpoint.into());
        self
    }

    /// Returns a copy configured with the trust bundle endpoints.
    pub fn with_trust_bundle_endpoints(
        mut self,
        endpoints: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.trust_bundle_endpoints = endpoints.into_iter().map(Into::into).collect();
        self
    }

    /// Returns a copy configured with the experiment directory.
    pub fn with_experiment_directory(
        mut self,
        experiment_directory: impl IntoIterator<Item = ExperimentDirectoryEntry>,
    ) -> Self {
        self.experiment_directory = experiment_directory.into_iter().collect();
        self
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Chooses a baseline retention profile for runtime and indexer metrics history.
pub enum MetricsRetentionPreset {
    #[default]
    /// Picks a profile from the configured runtime roles.
    Auto,
    /// Uses a low-memory profile suitable for trainer-heavy peers.
    PeerLean,
    /// Uses a moderate profile suitable for validator-capable peers.
    PeerBalanced,
    /// Uses a larger profile suitable for bootstrap, archive, or operator nodes.
    Operator,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Captures the effective steady-state metrics history budget for one node.
pub struct MetricsRetentionBudget {
    /// Maximum retained peer-window records per experiment revision.
    pub max_peer_window_entries_per_revision: usize,
    /// Maximum retained reducer cohort records per experiment revision.
    pub max_reducer_cohort_entries_per_revision: usize,
    /// Maximum retained head evaluation reports per experiment revision.
    pub max_head_eval_reports_per_revision: usize,
    /// Maximum recent revisions with raw metrics detail retained per experiment.
    pub max_metric_revisions_per_experiment: usize,
    /// Maximum peer-window rows returned by a single drilldown payload.
    pub max_peer_window_detail_windows: usize,
}

impl MetricsRetentionBudget {
    /// Returns the low-memory peer budget.
    pub const fn peer_lean() -> Self {
        Self {
            max_peer_window_entries_per_revision: 48,
            max_reducer_cohort_entries_per_revision: 24,
            max_head_eval_reports_per_revision: 32,
            max_metric_revisions_per_experiment: 2,
            max_peer_window_detail_windows: 16,
        }
    }

    /// Returns the validator-capable peer budget.
    pub const fn peer_balanced() -> Self {
        Self {
            max_peer_window_entries_per_revision: 128,
            max_reducer_cohort_entries_per_revision: 64,
            max_head_eval_reports_per_revision: 96,
            max_metric_revisions_per_experiment: 4,
            max_peer_window_detail_windows: 32,
        }
    }

    /// Returns the operator-facing indexer budget.
    pub const fn operator() -> Self {
        Self {
            max_peer_window_entries_per_revision: 256,
            max_reducer_cohort_entries_per_revision: 128,
            max_head_eval_reports_per_revision: 192,
            max_metric_revisions_per_experiment: 8,
            max_peer_window_detail_windows: 64,
        }
    }

    fn normalize(self) -> Self {
        Self {
            max_peer_window_entries_per_revision: self.max_peer_window_entries_per_revision.max(1),
            max_reducer_cohort_entries_per_revision: self
                .max_reducer_cohort_entries_per_revision
                .max(1),
            max_head_eval_reports_per_revision: self.max_head_eval_reports_per_revision.max(1),
            max_metric_revisions_per_experiment: self.max_metric_revisions_per_experiment.max(1),
            max_peer_window_detail_windows: self.max_peer_window_detail_windows.max(1),
        }
    }
}

impl Default for MetricsRetentionBudget {
    fn default() -> Self {
        Self::peer_balanced()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Configures how much raw metrics history one node keeps resident and on disk.
///
/// This budget is intentionally coarse-grained. Nodes keep recent raw detail for
/// local diagnostics and drilldowns, while older history is expected to survive
/// through derived summaries, snapshots, and ledger exports rather than unlimited
/// in-memory or on-disk raw envelope growth.
pub struct MetricsRetentionConfig {
    /// Baseline retention preset.
    #[serde(default)]
    pub preset: MetricsRetentionPreset,
    /// Optional override for retained peer-window records per revision.
    #[serde(default)]
    pub max_peer_window_entries_per_revision: Option<usize>,
    /// Optional override for retained reducer cohort records per revision.
    #[serde(default)]
    pub max_reducer_cohort_entries_per_revision: Option<usize>,
    /// Optional override for retained head evaluation reports per revision.
    #[serde(default)]
    pub max_head_eval_reports_per_revision: Option<usize>,
    /// Optional override for retained recent revisions with raw metrics detail per experiment.
    #[serde(default)]
    pub max_metric_revisions_per_experiment: Option<usize>,
    /// Optional override for peer-window drilldown payload size.
    #[serde(default)]
    pub max_peer_window_detail_windows: Option<usize>,
}

impl MetricsRetentionConfig {
    /// Resolves the effective metrics retention budget for the provided role set.
    pub fn resolve_for_roles(&self, roles: &PeerRoleSet) -> MetricsRetentionBudget {
        let mut budget = match self.preset {
            MetricsRetentionPreset::Auto => {
                if roles.contains(&PeerRole::Authority) || roles.contains(&PeerRole::Archive) {
                    MetricsRetentionBudget::operator()
                } else if roles.contains(&PeerRole::Bootstrap)
                    || roles.contains(&PeerRole::RelayHelper)
                {
                    MetricsRetentionBudget::peer_lean()
                } else if roles.contains(&PeerRole::Validator) {
                    MetricsRetentionBudget::peer_balanced()
                } else {
                    MetricsRetentionBudget::peer_lean()
                }
            }
            MetricsRetentionPreset::PeerLean => MetricsRetentionBudget::peer_lean(),
            MetricsRetentionPreset::PeerBalanced => MetricsRetentionBudget::peer_balanced(),
            MetricsRetentionPreset::Operator => MetricsRetentionBudget::operator(),
        };

        if let Some(value) = self.max_peer_window_entries_per_revision {
            budget.max_peer_window_entries_per_revision = value;
        }
        if let Some(value) = self.max_reducer_cohort_entries_per_revision {
            budget.max_reducer_cohort_entries_per_revision = value;
        }
        if let Some(value) = self.max_head_eval_reports_per_revision {
            budget.max_head_eval_reports_per_revision = value;
        }
        if let Some(value) = self.max_metric_revisions_per_experiment {
            budget.max_metric_revisions_per_experiment = value;
        }
        if let Some(value) = self.max_peer_window_detail_windows {
            budget.max_peer_window_detail_windows = value;
        }

        budget.normalize()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Bundles the durable runtime configuration for one native node.
///
/// `NodeBuilder` fills this structure incrementally, then `prepare()` validates
/// that the selected network, release, workload, auth, storage, and transport
/// settings are mutually consistent before the runtime thread starts.
pub struct NodeConfig {
    /// The identity.
    pub identity: IdentityConfig,
    /// The storage.
    pub storage: Option<StorageConfig>,
    /// The dataset.
    pub dataset: Option<DatasetConfig>,
    /// The auth.
    pub auth: Option<AuthConfig>,
    /// The network manifest.
    pub network_manifest: Option<NetworkManifest>,
    /// The client release manifest.
    pub client_release_manifest: Option<ClientReleaseManifest>,
    /// The selected workload ID.
    pub selected_workload_id: Option<WorkloadId>,
    /// The raw metrics retention policy applied to local history and drilldowns.
    #[serde(default)]
    pub metrics_retention: MetricsRetentionConfig,
    /// The bootstrap peers.
    pub bootstrap_peers: Vec<SwarmAddress>,
    /// The listen addresses.
    pub listen_addresses: Vec<SwarmAddress>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Enumerates the supported runtime statuses.
pub enum RuntimeStatus {
    /// Uses the starting variant.
    Starting,
    /// Uses the running variant.
    Running,
    /// Uses the shutdown requested variant.
    ShutdownRequested,
    /// Uses the stopped variant.
    Stopped,
    /// Uses the failed variant.
    Failed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Enumerates the supported node runtime states.
pub enum NodeRuntimeState {
    #[default]
    /// Uses the starting variant.
    Starting,
    /// Uses the connecting variant.
    Connecting,
    /// Uses the admitting variant.
    Admitting,
    /// Uses the directory sync variant.
    DirectorySync,
    /// Uses the head sync variant.
    HeadSync,
    /// Uses the idle ready variant.
    IdleReady,
    /// Uses the lease pending variant.
    LeasePending,
    /// Uses the training window variant.
    TrainingWindow,
    /// Uses the publishing update variant.
    PublishingUpdate,
    /// Uses the waiting merge variant.
    WaitingMerge,
    /// Uses the passive validator variant.
    PassiveValidator,
    /// Uses the passive archive variant.
    PassiveArchive,
    /// Uses the quarantined variant.
    Quarantined,
    /// Uses the revoked variant.
    Revoked,
    /// Uses the shutting down variant.
    ShuttingDown,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Captures slot assignment state.
pub struct SlotAssignmentState {
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
}

impl SlotAssignmentState {
    /// Creates a new value.
    pub fn new(study_id: StudyId, experiment_id: ExperimentId, revision_id: RevisionId) -> Self {
        Self {
            study_id,
            experiment_id,
            revision_id,
        }
    }

    /// Creates a value from the experiment.
    pub fn from_experiment(experiment: &ExperimentHandle) -> Self {
        Self::new(
            experiment.study_id.clone(),
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Enumerates the supported slot runtime states.
pub enum SlotRuntimeState {
    #[default]
    /// Uses the unassigned variant.
    Unassigned,
    /// Uses the assigned variant.
    Assigned(SlotAssignmentState),
    /// Uses the materializing base variant.
    MaterializingBase(SlotAssignmentState),
    /// Uses the fetching shards variant.
    FetchingShards(SlotAssignmentState),
    /// Uses the training variant.
    Training(SlotAssignmentState),
    /// Uses the publishing variant.
    Publishing(SlotAssignmentState),
    /// Uses the cooling down variant.
    CoolingDown(SlotAssignmentState),
    /// Uses the migrating variant.
    Migrating(SlotAssignmentState),
    /// Uses the blocked variant.
    Blocked {
        /// The assignment.
        assignment: Option<SlotAssignmentState>,
        /// The reason.
        reason: String,
    },
}

pub(crate) fn default_node_runtime_state(roles: &PeerRoleSet) -> NodeRuntimeState {
    if roles.contains(&PeerRole::Validator) {
        NodeRuntimeState::PassiveValidator
    } else if roles.contains(&PeerRole::Archive) {
        NodeRuntimeState::PassiveArchive
    } else {
        NodeRuntimeState::IdleReady
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Enumerates the supported artifact transfer phase values.
pub enum ArtifactTransferPhase {
    /// Uses the locating provider variant.
    LocatingProvider,
    /// Uses the fetching chunks variant.
    FetchingChunks,
    /// Uses the finalizing variant.
    Finalizing,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Captures artifact transfer state.
pub struct ArtifactTransferState {
    /// The artifact ID.
    pub artifact_id: ArtifactId,
    #[serde(default)]
    /// The source peers.
    pub source_peers: Vec<PeerId>,
    #[serde(default)]
    /// The provider peer ID.
    pub provider_peer_id: Option<PeerId>,
    #[serde(default)]
    /// The descriptor.
    pub descriptor: Option<ArtifactDescriptor>,
    #[serde(default)]
    /// The completed chunks.
    pub completed_chunks: BTreeSet<ChunkId>,
    /// The phase.
    pub phase: ArtifactTransferPhase,
    /// The started at.
    pub started_at: DateTime<Utc>,
    /// The updated at.
    pub updated_at: DateTime<Utc>,
}

impl ArtifactTransferState {
    /// Creates a new value.
    pub fn new(artifact_id: ArtifactId) -> Self {
        let now = Utc::now();
        Self {
            artifact_id,
            source_peers: Vec::new(),
            provider_peer_id: None,
            descriptor: None,
            completed_chunks: BTreeSet::new(),
            phase: ArtifactTransferPhase::LocatingProvider,
            started_at: now,
            updated_at: now,
        }
    }

    pub(crate) fn set_provider(
        &mut self,
        provider_peer_id: PeerId,
        descriptor: ArtifactDescriptor,
    ) {
        self.provider_peer_id = Some(provider_peer_id);
        self.descriptor = Some(descriptor);
        self.phase = ArtifactTransferPhase::FetchingChunks;
        self.updated_at = Utc::now();
    }

    pub(crate) fn note_completed_chunk(&mut self, chunk_id: &ChunkId) -> bool {
        let changed = self.completed_chunks.insert(chunk_id.clone());
        if changed {
            self.updated_at = Utc::now();
        }
        changed
    }

    pub(crate) fn set_phase(&mut self, phase: ArtifactTransferPhase) {
        self.phase = phase;
        self.updated_at = Utc::now();
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Represents a client reenrollment status.
pub struct ClientReenrollmentStatus {
    /// The reason.
    pub reason: String,
    /// The rotated at.
    pub rotated_at: Option<DateTime<Utc>>,
    /// The previous issuer peer IDs retained for reenrollment.
    pub legacy_issuer_peer_ids: BTreeSet<PeerId>,
    /// The login path.
    pub login_path: String,
    /// The enroll path.
    pub enroll_path: String,
    /// The trust bundle path.
    pub trust_bundle_path: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Captures trust bundle state.
pub struct TrustBundleState {
    /// The source URL.
    pub source_url: String,
    /// The required release train hash.
    pub required_release_train_hash: ContentId,
    #[serde(default)]
    /// The allowed target artifact hashes.
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
    /// The active issuer peer ID.
    pub active_issuer_peer_id: PeerId,
    /// The trusted issuers.
    pub trusted_issuers: BTreeMap<PeerId, TrustedIssuer>,
    /// The minimum revocation epoch.
    pub minimum_revocation_epoch: RevocationEpoch,
    /// The reenrollment.
    pub reenrollment: Option<ClientReenrollmentStatus>,
    /// The updated at.
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
/// Captures a snapshot of node telemetry.
pub struct NodeTelemetrySnapshot {
    /// The status.
    pub status: RuntimeStatus,
    /// The node state.
    pub node_state: NodeRuntimeState,
    /// The slot states.
    pub slot_states: Vec<SlotRuntimeState>,
    /// The lag state.
    pub lag_state: LagState,
    /// The head lag steps.
    pub head_lag_steps: u64,
    /// The lag policy.
    pub lag_policy: LagPolicy,
    /// The network ID.
    pub network_id: Option<NetworkId>,
    /// The local peer ID.
    pub local_peer_id: Option<PeerId>,
    /// The configured roles.
    pub configured_roles: PeerRoleSet,
    /// The connected peers.
    pub connected_peers: usize,
    /// The observed peer IDs.
    pub observed_peer_ids: BTreeSet<PeerId>,
    /// The known peer addresses.
    pub known_peer_addresses: BTreeSet<SwarmAddress>,
    /// The runtime boundary.
    pub runtime_boundary: Option<RuntimeBoundary>,
    /// The listen addresses.
    pub listen_addresses: Vec<SwarmAddress>,
    /// The control plane.
    pub control_plane: ControlPlaneSnapshot,
    /// The recent events.
    pub recent_events: Vec<LiveControlPlaneEvent>,
    /// The last snapshot peer ID.
    pub last_snapshot_peer_id: Option<PeerId>,
    /// The last snapshot.
    pub last_snapshot: Option<ControlPlaneSnapshot>,
    #[serde(default)]
    /// The admitted peers.
    pub admitted_peers: BTreeMap<PeerId, PeerAdmissionReport>,
    #[serde(default)]
    /// The rejected peers.
    pub rejected_peers: BTreeMap<PeerId, String>,
    #[serde(default)]
    /// The peer reputation.
    pub peer_reputation: BTreeMap<PeerId, ReputationState>,
    #[serde(default)]
    /// The minimum revocation epoch.
    pub minimum_revocation_epoch: Option<RevocationEpoch>,
    #[serde(default)]
    /// The trust bundle.
    pub trust_bundle: Option<TrustBundleState>,
    #[serde(default)]
    /// The in flight transfers.
    pub in_flight_transfers: BTreeMap<ArtifactId, ArtifactTransferState>,
    #[serde(default)]
    /// The active robustness policy, when one is currently scoped to the revision.
    pub robustness_policy: Option<RobustnessPolicy>,
    #[serde(default)]
    /// The latest cohort robustness report emitted by validation.
    pub latest_cohort_robustness: Option<CohortRobustnessReport>,
    #[serde(default)]
    /// The latest trust scores tracked by the robustness pipeline.
    pub trust_scores: Vec<TrustScore>,
    #[serde(default)]
    /// Recent canary reports tracked by the robustness pipeline.
    pub canary_reports: Vec<CanaryEvalReport>,
    /// The effective limit profile.
    pub effective_limit_profile: Option<LimitProfile>,
    /// The last error.
    pub last_error: Option<String>,
    /// The started at.
    pub started_at: DateTime<Utc>,
    /// The updated at.
    pub updated_at: DateTime<Utc>,
}

impl NodeTelemetrySnapshot {
    const MAX_RECENT_EVENTS: usize = 64;

    pub(crate) fn starting(mainnet: &MainnetHandle, config: &NodeConfig) -> Self {
        let now = Utc::now();

        Self {
            status: RuntimeStatus::Starting,
            node_state: NodeRuntimeState::Starting,
            slot_states: vec![SlotRuntimeState::Unassigned],
            lag_state: LagState::Current,
            head_lag_steps: 0,
            lag_policy: LagPolicy::default(),
            network_id: Some(mainnet.genesis.network_id.clone()),
            local_peer_id: None,
            configured_roles: mainnet.roles.clone(),
            connected_peers: 0,
            observed_peer_ids: BTreeSet::new(),
            known_peer_addresses: BTreeSet::new(),
            runtime_boundary: None,
            listen_addresses: config.listen_addresses.clone(),
            control_plane: ControlPlaneSnapshot::default(),
            recent_events: Vec::new(),
            last_snapshot_peer_id: None,
            last_snapshot: None,
            admitted_peers: BTreeMap::new(),
            rejected_peers: BTreeMap::new(),
            peer_reputation: BTreeMap::new(),
            minimum_revocation_epoch: config
                .auth
                .as_ref()
                .and_then(|auth| auth.admission_policy.as_ref())
                .map(|policy| policy.minimum_revocation_epoch),
            trust_bundle: None,
            in_flight_transfers: BTreeMap::new(),
            robustness_policy: None,
            latest_cohort_robustness: None,
            trust_scores: Vec::new(),
            canary_reports: Vec::new(),
            effective_limit_profile: None,
            last_error: None,
            started_at: now,
            updated_at: now,
        }
    }

    pub(crate) fn push_event(&mut self, event: LiveControlPlaneEvent) {
        if let LiveControlPlaneEvent::ConnectionClosed { peer_id } = &event {
            self.observed_peer_ids.remove(&PeerId::new(peer_id.clone()));
        } else if let Some(peer_id) = peer_id_from_event(&event) {
            self.observed_peer_ids.insert(peer_id);
        }
        self.recent_events.push(event);
        if self.recent_events.len() > Self::MAX_RECENT_EVENTS {
            let overflow = self.recent_events.len() - Self::MAX_RECENT_EVENTS;
            self.recent_events.drain(..overflow);
        }
        self.updated_at = Utc::now();
    }

    pub(crate) fn set_error(&mut self, message: impl Into<String>) {
        self.last_error = Some(message.into());
        self.status = RuntimeStatus::Failed;
        self.updated_at = Utc::now();
    }

    pub(crate) fn set_node_state(&mut self, node_state: NodeRuntimeState) {
        self.node_state = node_state;
        self.updated_at = Utc::now();
    }

    pub(crate) fn set_primary_slot_state(&mut self, slot_state: SlotRuntimeState) {
        if self.slot_states.is_empty() {
            self.slot_states.push(slot_state);
        } else {
            self.slot_states[0] = slot_state;
        }
        self.updated_at = Utc::now();
    }

    pub(crate) fn set_lag_status(
        &mut self,
        lag_state: LagState,
        head_lag_steps: u64,
        lag_policy: LagPolicy,
    ) {
        self.lag_state = lag_state;
        self.head_lag_steps = head_lag_steps;
        self.lag_policy = lag_policy;
        self.updated_at = Utc::now();
    }

    pub(crate) fn update_transfer_state(&mut self, transfer_state: ArtifactTransferState) {
        self.in_flight_transfers
            .insert(transfer_state.artifact_id.clone(), transfer_state);
        self.updated_at = Utc::now();
    }

    pub(crate) fn clear_transfer_state(&mut self, artifact_id: &ArtifactId) {
        self.in_flight_transfers.remove(artifact_id);
        self.updated_at = Utc::now();
    }

    pub(crate) fn set_robustness_state(
        &mut self,
        policy: RobustnessPolicy,
        cohort: CohortRobustnessReport,
        trust_scores: Vec<TrustScore>,
        canary_report: Option<CanaryEvalReport>,
    ) {
        self.robustness_policy = Some(policy);
        self.latest_cohort_robustness = Some(cohort);
        self.trust_scores = trust_scores;
        if let Some(report) = canary_report {
            self.canary_reports
                .retain(|candidate| candidate.candidate_head_id != report.candidate_head_id);
            self.canary_reports.push(report);
            self.canary_reports
                .sort_by_key(|candidate| std::cmp::Reverse(candidate.evaluated_at));
            self.canary_reports.truncate(16);
        }
        self.updated_at = Utc::now();
    }
}

pub(crate) fn peer_id_from_event(event: &LiveControlPlaneEvent) -> Option<PeerId> {
    match event {
        LiveControlPlaneEvent::ConnectionEstablished { peer_id }
        | LiveControlPlaneEvent::SnapshotRequested { peer_id }
        | LiveControlPlaneEvent::SnapshotReceived { peer_id, .. }
        | LiveControlPlaneEvent::SnapshotResponseSent { peer_id }
        | LiveControlPlaneEvent::ArtifactManifestRequested { peer_id, .. }
        | LiveControlPlaneEvent::ArtifactManifestReceived { peer_id, .. }
        | LiveControlPlaneEvent::ArtifactChunkRequested { peer_id, .. }
        | LiveControlPlaneEvent::ArtifactChunkReceived { peer_id, .. }
        | LiveControlPlaneEvent::RequestFailure { peer_id, .. }
        | LiveControlPlaneEvent::InboundFailure { peer_id, .. }
        | LiveControlPlaneEvent::ResponseSendFailure { peer_id, .. }
        | LiveControlPlaneEvent::PubsubMessage { peer_id, .. }
        | LiveControlPlaneEvent::PeerIdentified { peer_id, .. } => {
            Some(PeerId::new(peer_id.clone()))
        }
        LiveControlPlaneEvent::ConnectionClosed { .. } => None,
        LiveControlPlaneEvent::PeersDiscovered { peers }
        | LiveControlPlaneEvent::PeersExpired { peers } => peers
            .first()
            .map(|(peer_id, _)| PeerId::new(peer_id.clone())),
        LiveControlPlaneEvent::NewListenAddr { .. }
        | LiveControlPlaneEvent::TopicSubscribed { .. }
        | LiveControlPlaneEvent::OutgoingConnectionError { .. }
        | LiveControlPlaneEvent::IncomingConnectionError { .. }
        | LiveControlPlaneEvent::Other { .. } => None,
    }
}

#[derive(Clone)]
/// Represents a telemetry handle.
pub struct TelemetryHandle {
    pub(crate) state: Arc<Mutex<NodeTelemetrySnapshot>>,
}

impl fmt::Debug for TelemetryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TelemetryHandle").finish_non_exhaustive()
    }
}

impl TelemetryHandle {
    /// Performs the snapshot operation.
    pub fn snapshot(&self) -> NodeTelemetrySnapshot {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[derive(Debug)]
pub(crate) enum RuntimeCommand {
    SubscribeTopic(OverlayTopic),
    PublishControl(ControlAnnouncement),
    PublishHead(HeadAnnouncement),
    PublishLease(LeaseAnnouncement),
    PublishMerge(MergeAnnouncement),
    PublishMergeWindow(MergeWindowAnnouncement),
    PublishReducerAssignment(ReducerAssignmentAnnouncement),
    PublishUpdate(UpdateEnvelopeAnnouncement),
    PublishAggregate(AggregateAnnouncement),
    PublishReductionCertificate(ReductionCertificateAnnouncement),
    PublishReducerLoad(ReducerLoadAnnouncement),
    PublishAuth(PeerAuthAnnouncement),
    PublishDirectory(ExperimentDirectoryAnnouncement),
    PublishMetrics(MetricsAnnouncement),
    PublishArtifact {
        descriptor: ArtifactDescriptor,
        chunks: Vec<ArtifactChunkPayload>,
    },
    FetchSnapshot {
        peer_id: String,
        timeout: Duration,
        reply: mpsc::Sender<Result<ControlPlaneSnapshot, String>>,
    },
    FetchArtifactManifest {
        peer_id: String,
        artifact_id: ArtifactId,
        timeout: Duration,
        reply: mpsc::Sender<Result<Option<ArtifactDescriptor>, String>>,
    },
    FetchArtifactChunk {
        peer_id: String,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
        timeout: Duration,
        reply: mpsc::Sender<Result<Option<ArtifactChunkPayload>, String>>,
    },
    DialAddress {
        address: SwarmAddress,
    },
    RequestSnapshot {
        peer_id: String,
    },
    Shutdown,
}

#[derive(Clone)]
/// Represents a control handle.
pub struct ControlHandle {
    pub(crate) tx: mpsc::Sender<RuntimeCommand>,
}

impl fmt::Debug for ControlHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlHandle").finish_non_exhaustive()
    }
}

impl ControlHandle {
    /// Performs the subscribe topic operation.
    pub fn subscribe_topic(&self, topic: OverlayTopic) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::SubscribeTopic(topic))
            .map_err(|error| anyhow::anyhow!("failed to subscribe topic: {error}"))
    }

    /// Performs the publish control operation.
    pub fn publish_control(&self, announcement: ControlAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishControl(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send control announcement: {error}"))
    }

    /// Performs the publish head operation.
    pub fn publish_head(&self, announcement: HeadAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishHead(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send head announcement: {error}"))
    }

    /// Performs the publish lease operation.
    pub fn publish_lease(&self, announcement: LeaseAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishLease(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send lease announcement: {error}"))
    }

    /// Performs the publish merge operation.
    pub fn publish_merge(&self, announcement: MergeAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishMerge(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send merge announcement: {error}"))
    }

    /// Performs the publish merge window operation.
    pub fn publish_merge_window(
        &self,
        announcement: MergeWindowAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishMergeWindow(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send merge window announcement: {error}"))
    }

    /// Performs the publish reducer assignment operation.
    pub fn publish_reducer_assignment(
        &self,
        announcement: ReducerAssignmentAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishReducerAssignment(announcement))
            .map_err(|error| {
                anyhow::anyhow!("failed to send reducer assignment announcement: {error}")
            })
    }

    /// Performs the publish update operation.
    pub fn publish_update(&self, announcement: UpdateEnvelopeAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishUpdate(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send update announcement: {error}"))
    }

    /// Performs the publish aggregate operation.
    pub fn publish_aggregate(&self, announcement: AggregateAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishAggregate(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send aggregate announcement: {error}"))
    }

    /// Performs the publish reduction certificate operation.
    pub fn publish_reduction_certificate(
        &self,
        announcement: ReductionCertificateAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishReductionCertificate(announcement))
            .map_err(|error| {
                anyhow::anyhow!("failed to send reduction certificate announcement: {error}")
            })
    }

    /// Performs the publish reducer load operation.
    pub fn publish_reducer_load(
        &self,
        announcement: ReducerLoadAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishReducerLoad(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send reducer load announcement: {error}"))
    }

    /// Performs the publish auth operation.
    pub fn publish_auth(&self, announcement: PeerAuthAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishAuth(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send auth announcement: {error}"))
    }

    /// Performs the publish directory operation.
    pub fn publish_directory(
        &self,
        announcement: ExperimentDirectoryAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishDirectory(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send directory announcement: {error}"))
    }

    /// Performs the publish metrics operation.
    pub fn publish_metrics(&self, announcement: MetricsAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishMetrics(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send metrics announcement: {error}"))
    }

    /// Performs the request snapshot operation.
    pub fn request_snapshot(&self, peer_id: impl Into<String>) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::RequestSnapshot {
                peer_id: peer_id.into(),
            })
            .map_err(|error| anyhow::anyhow!("failed to request snapshot: {error}"))
    }

    /// Requests an outbound dial to a swarm address.
    pub fn dial_address(&self, address: SwarmAddress) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::DialAddress { address })
            .map_err(|error| anyhow::anyhow!("failed to request dial: {error}"))
    }

    /// Fetches the snapshot.
    pub fn fetch_snapshot(
        &self,
        peer_id: impl Into<String>,
        timeout: Duration,
    ) -> anyhow::Result<ControlPlaneSnapshot> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::FetchSnapshot {
                peer_id: peer_id.into(),
                timeout,
                reply: reply_tx,
            })
            .map_err(|error| anyhow::anyhow!("failed to request snapshot: {error}"))?;

        reply_rx
            .recv()
            .map_err(|error| anyhow::anyhow!("snapshot reply channel closed: {error}"))?
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    /// Performs the publish artifact operation.
    pub fn publish_artifact(
        &self,
        descriptor: ArtifactDescriptor,
        chunks: Vec<ArtifactChunkPayload>,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishArtifact { descriptor, chunks })
            .map_err(|error| anyhow::anyhow!("failed to publish artifact: {error}"))
    }

    /// Fetches the artifact manifest.
    pub fn fetch_artifact_manifest(
        &self,
        peer_id: impl Into<String>,
        artifact_id: ArtifactId,
        timeout: Duration,
    ) -> anyhow::Result<Option<ArtifactDescriptor>> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::FetchArtifactManifest {
                peer_id: peer_id.into(),
                artifact_id,
                timeout,
                reply: reply_tx,
            })
            .map_err(|error| anyhow::anyhow!("failed to request artifact manifest: {error}"))?;

        reply_rx
            .recv()
            .map_err(|error| anyhow::anyhow!("artifact manifest reply channel closed: {error}"))?
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    /// Fetches the artifact chunk.
    pub fn fetch_artifact_chunk(
        &self,
        peer_id: impl Into<String>,
        artifact_id: ArtifactId,
        chunk_id: ChunkId,
        timeout: Duration,
    ) -> anyhow::Result<Option<ArtifactChunkPayload>> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(RuntimeCommand::FetchArtifactChunk {
                peer_id: peer_id.into(),
                artifact_id,
                chunk_id,
                timeout,
                reply: reply_tx,
            })
            .map_err(|error| anyhow::anyhow!("failed to request artifact chunk: {error}"))?;

        reply_rx
            .recv()
            .map_err(|error| anyhow::anyhow!("artifact chunk reply channel closed: {error}"))?
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    /// Performs the shutdown operation.
    pub fn shutdown(&self) -> anyhow::Result<()> {
        match self.tx.send(RuntimeCommand::Shutdown) {
            Ok(()) => Ok(()),
            Err(_) => Ok(()),
        }
    }
}
