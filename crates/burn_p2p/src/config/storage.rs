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

    pub(crate) fn slot_assignments_path(&self) -> PathBuf {
        self.state_dir().join("slot-assignments.json")
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
