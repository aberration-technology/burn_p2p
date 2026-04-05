use super::*;
use crate::config::{
    ClientReenrollmentStatus, RuntimeCommand, TrustBundleState, default_node_runtime_state,
    peer_id_from_event,
};

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PersistedControlPlaneState {
    pub control_announcements: Vec<ControlAnnouncement>,
    pub lease_announcements: Vec<LeaseAnnouncement>,
    pub auth_announcements: Vec<PeerAuthAnnouncement>,
    pub directory_announcements: Vec<ExperimentDirectoryAnnouncement>,
    #[serde(default)]
    pub metrics_announcements: Vec<MetricsAnnouncement>,
}

impl PersistedControlPlaneState {
    fn from_snapshot(snapshot: &ControlPlaneSnapshot) -> Self {
        let mut bounded_snapshot = ControlPlaneSnapshot {
            metrics_announcements: snapshot.metrics_announcements.clone(),
            ..ControlPlaneSnapshot::default()
        };
        bounded_snapshot.clamp_metrics_announcements();
        Self {
            control_announcements: snapshot.control_announcements.clone(),
            lease_announcements: snapshot.lease_announcements.clone(),
            auth_announcements: snapshot.auth_announcements.clone(),
            directory_announcements: snapshot.directory_announcements.clone(),
            metrics_announcements: bounded_snapshot.metrics_announcements,
        }
    }

    fn apply_to_snapshot(self, snapshot: &mut ControlPlaneSnapshot) {
        snapshot.control_announcements = self.control_announcements;
        snapshot.lease_announcements = self.lease_announcements;
        snapshot.auth_announcements = self.auth_announcements;
        snapshot.directory_announcements = self.directory_announcements;
        snapshot.metrics_announcements = self.metrics_announcements;
        snapshot.clamp_metrics_announcements();
    }

    fn apply_to_shell(self, shell: &mut ControlPlaneShell) {
        for announcement in self.control_announcements {
            shell.publish_control(announcement);
        }
        for announcement in self.lease_announcements {
            shell.publish_lease(announcement);
        }
        for announcement in self.auth_announcements {
            shell.publish_auth(announcement);
        }
        for announcement in self.directory_announcements {
            shell.publish_directory(announcement);
        }
        for announcement in self.metrics_announcements {
            shell.publish_metrics(announcement);
        }
    }

    fn is_empty(&self) -> bool {
        self.control_announcements.is_empty()
            && self.lease_announcements.is_empty()
            && self.auth_announcements.is_empty()
            && self.directory_announcements.is_empty()
            && self.metrics_announcements.is_empty()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PersistedAuthState {
    pub admission_policy: Option<AdmissionPolicy>,
    pub local_peer_auth: Option<PeerAuthEnvelope>,
    pub trust_bundle_endpoints: Vec<String>,
}

impl PersistedAuthState {
    fn from_auth_config(auth: Option<&AuthConfig>) -> Self {
        Self {
            admission_policy: auth.and_then(|auth| auth.admission_policy.clone()),
            local_peer_auth: auth.and_then(|auth| auth.local_peer_auth.clone()),
            trust_bundle_endpoints: auth
                .map(|auth| auth.trust_bundle_endpoints.clone())
                .unwrap_or_default(),
        }
    }

    fn merge_into_auth_config(self, auth: Option<AuthConfig>) -> Option<AuthConfig> {
        if self.admission_policy.is_none()
            && self.local_peer_auth.is_none()
            && self.trust_bundle_endpoints.is_empty()
        {
            return auth;
        }

        match auth {
            Some(mut auth) => {
                if auth.admission_policy.is_none() {
                    auth.admission_policy = self.admission_policy;
                }
                if auth.local_peer_auth.is_none() {
                    auth.local_peer_auth = self.local_peer_auth;
                }
                if auth.trust_bundle_endpoints.is_empty() {
                    auth.trust_bundle_endpoints = self.trust_bundle_endpoints;
                }
                Some(auth)
            }
            None => Some(AuthConfig {
                admission_policy: self.admission_policy,
                local_peer_auth: self.local_peer_auth,
                trust_bundle_endpoints: self.trust_bundle_endpoints,
                experiment_directory: Vec::new(),
            }),
        }
    }

    fn with_local_peer_auth(mut self, local_peer_auth: PeerAuthEnvelope) -> Self {
        self.local_peer_auth = Some(local_peer_auth);
        self
    }

    fn is_empty(&self) -> bool {
        self.admission_policy.is_none()
            && self.local_peer_auth.is_none()
            && self.trust_bundle_endpoints.is_empty()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PersistedRuntimeBindingState {
    pub network_manifest: Option<NetworkManifest>,
    pub client_release_manifest: Option<ClientReleaseManifest>,
    pub selected_workload_id: Option<WorkloadId>,
}

impl PersistedRuntimeBindingState {
    fn from_node_config(config: &NodeConfig) -> Self {
        Self {
            network_manifest: config.network_manifest.clone(),
            client_release_manifest: config.client_release_manifest.clone(),
            selected_workload_id: config.selected_workload_id.clone(),
        }
    }

    fn merge_into_node_config(self, config: &mut NodeConfig) {
        if config.network_manifest.is_none() {
            config.network_manifest = self.network_manifest;
        }
        if config.client_release_manifest.is_none() {
            config.client_release_manifest = self.client_release_manifest;
        }
        if config.selected_workload_id.is_none() {
            config.selected_workload_id = self.selected_workload_id;
        }
    }

    fn is_empty(&self) -> bool {
        self.network_manifest.is_none()
            && self.client_release_manifest.is_none()
            && self.selected_workload_id.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PersistedRuntimeSecurityState {
    pub node_state: NodeRuntimeState,
    pub slot_states: Vec<SlotRuntimeState>,
    pub lag_state: LagState,
    pub head_lag_steps: u64,
    pub lag_policy: LagPolicy,
    pub admitted_peers: BTreeMap<PeerId, PeerAdmissionReport>,
    pub rejected_peers: BTreeMap<PeerId, String>,
    pub peer_reputation: BTreeMap<PeerId, ReputationState>,
    #[serde(default)]
    pub robustness_policy: Option<RobustnessPolicy>,
    #[serde(default)]
    pub latest_cohort_robustness: Option<CohortRobustnessReport>,
    #[serde(default)]
    pub trust_scores: Vec<TrustScore>,
    #[serde(default)]
    pub canary_reports: Vec<CanaryEvalReport>,
    pub minimum_revocation_epoch: Option<RevocationEpoch>,
    pub trust_bundle: Option<TrustBundleState>,
}

impl PersistedRuntimeSecurityState {
    fn from_snapshot(snapshot: &NodeTelemetrySnapshot) -> Self {
        let default_state = default_node_runtime_state(&snapshot.configured_roles);
        Self {
            node_state: persisted_node_state(&snapshot.node_state, default_state),
            slot_states: persisted_slot_states(&snapshot.slot_states),
            lag_state: snapshot.lag_state,
            head_lag_steps: snapshot.head_lag_steps,
            lag_policy: snapshot.lag_policy.clone(),
            admitted_peers: snapshot.admitted_peers.clone(),
            rejected_peers: snapshot.rejected_peers.clone(),
            peer_reputation: snapshot.peer_reputation.clone(),
            robustness_policy: snapshot.robustness_policy.clone(),
            latest_cohort_robustness: snapshot.latest_cohort_robustness.clone(),
            trust_scores: snapshot.trust_scores.clone(),
            canary_reports: snapshot.canary_reports.clone(),
            minimum_revocation_epoch: snapshot.minimum_revocation_epoch,
            trust_bundle: snapshot.trust_bundle.clone(),
        }
    }

    fn apply_to_snapshot(self, snapshot: &mut NodeTelemetrySnapshot) {
        snapshot.node_state = self.node_state;
        snapshot.slot_states = self.slot_states;
        snapshot.lag_state = self.lag_state;
        snapshot.head_lag_steps = self.head_lag_steps;
        snapshot.lag_policy = self.lag_policy;
        snapshot.admitted_peers = self.admitted_peers;
        snapshot.rejected_peers = self.rejected_peers;
        snapshot.peer_reputation = self.peer_reputation;
        snapshot.robustness_policy = self.robustness_policy;
        snapshot.latest_cohort_robustness = self.latest_cohort_robustness;
        snapshot.trust_scores = self.trust_scores;
        snapshot.canary_reports = self.canary_reports;
        if snapshot.minimum_revocation_epoch.is_none() {
            snapshot.minimum_revocation_epoch = self.minimum_revocation_epoch;
        }
        if snapshot.trust_bundle.is_none() {
            snapshot.trust_bundle = self.trust_bundle;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
struct RemoteTrustedIssuerStatus {
    issuer_peer_id: PeerId,
    issuer_public_key_hex: String,
    accepted_for_admission: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
struct RemoteReenrollmentStatus {
    reason: String,
    rotated_at: Option<DateTime<Utc>>,
    legacy_issuer_peer_ids: BTreeSet<PeerId>,
    login_path: String,
    enroll_path: String,
    trust_bundle_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
struct RemoteTrustBundleExport {
    network_id: NetworkId,
    project_family_id: ProjectFamilyId,
    required_release_train_hash: ContentId,
    #[serde(default)]
    allowed_target_artifact_hashes: BTreeSet<ContentId>,
    minimum_revocation_epoch: RevocationEpoch,
    active_issuer_peer_id: PeerId,
    issuers: Vec<RemoteTrustedIssuerStatus>,
    reenrollment: Option<RemoteReenrollmentStatus>,
}

fn persisted_node_state(
    node_state: &NodeRuntimeState,
    default_state: NodeRuntimeState,
) -> NodeRuntimeState {
    match node_state {
        NodeRuntimeState::IdleReady
        | NodeRuntimeState::PassiveValidator
        | NodeRuntimeState::PassiveArchive
        | NodeRuntimeState::Quarantined
        | NodeRuntimeState::Revoked => node_state.clone(),
        _ => default_state,
    }
}

fn persisted_slot_states(slot_states: &[SlotRuntimeState]) -> Vec<SlotRuntimeState> {
    if slot_states.is_empty() {
        return vec![SlotRuntimeState::Unassigned];
    }

    slot_states
        .iter()
        .map(|slot_state| match slot_state {
            SlotRuntimeState::Unassigned
            | SlotRuntimeState::Assigned(_)
            | SlotRuntimeState::Blocked { .. } => slot_state.clone(),
            SlotRuntimeState::MaterializingBase(assignment)
            | SlotRuntimeState::FetchingShards(assignment)
            | SlotRuntimeState::Training(assignment)
            | SlotRuntimeState::Publishing(assignment)
            | SlotRuntimeState::CoolingDown(assignment)
            | SlotRuntimeState::Migrating(assignment) => {
                SlotRuntimeState::Assigned(assignment.clone())
            }
        })
        .collect()
}

pub(crate) fn persist_json<T: serde::Serialize>(path: PathBuf, value: &T) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| anyhow::anyhow!("failed to create {}: {error}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| anyhow::anyhow!("failed to encode {}: {error}", path.display()))?;
    fs::write(&path, bytes)
        .map_err(|error| anyhow::anyhow!("failed to write {}: {error}", path.display()))
}

pub(crate) fn load_json<T: DeserializeOwned>(path: PathBuf) -> anyhow::Result<Option<T>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path)
        .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", path.display()))?;
    let value = serde_json::from_slice(&bytes)
        .map_err(|error| anyhow::anyhow!("failed to decode {}: {error}", path.display()))?;
    Ok(Some(value))
}

pub(crate) fn persist_head_state(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    head: &HeadDescriptor,
) -> anyhow::Result<()> {
    persist_json(storage.scoped_current_head_path(experiment), head)
}

pub(crate) fn persist_known_peers(
    storage: &StorageConfig,
    known_peers: &BTreeSet<SwarmAddress>,
) -> anyhow::Result<()> {
    persist_json(
        storage.known_peers_path(),
        &known_peers.iter().cloned().collect::<Vec<_>>(),
    )
}

pub(crate) fn load_known_peers(storage: &StorageConfig) -> anyhow::Result<Vec<SwarmAddress>> {
    Ok(load_json(storage.known_peers_path())?.unwrap_or_default())
}

pub(crate) fn persist_primary_slot_assignment(
    storage: &StorageConfig,
    assignment: &SlotAssignmentState,
) -> anyhow::Result<()> {
    persist_json(storage.primary_slot_assignment_path(), assignment)
}

pub(crate) fn load_primary_slot_assignment(
    storage: &StorageConfig,
) -> anyhow::Result<Option<SlotAssignmentState>> {
    load_json(storage.primary_slot_assignment_path())
}

pub(crate) fn persist_auth_state(
    storage: &StorageConfig,
    auth: Option<&AuthConfig>,
) -> anyhow::Result<()> {
    let state = PersistedAuthState::from_auth_config(auth);
    let path = storage.auth_state_path();
    if state.is_empty() {
        if path.exists() {
            fs::remove_file(&path)
                .map_err(|error| anyhow::anyhow!("failed to remove {}: {error}", path.display()))?;
        }
        return Ok(());
    }
    persist_json(path, &state)
}

pub(crate) fn load_auth_state(
    storage: &StorageConfig,
) -> anyhow::Result<Option<PersistedAuthState>> {
    load_json(storage.auth_state_path())
}

pub(crate) fn persist_runtime_binding_state(
    storage: &StorageConfig,
    config: &NodeConfig,
) -> anyhow::Result<()> {
    let state = PersistedRuntimeBindingState::from_node_config(config);
    let path = storage.runtime_binding_state_path();
    if state.is_empty() {
        if path.exists() {
            fs::remove_file(&path)
                .map_err(|error| anyhow::anyhow!("failed to remove {}: {error}", path.display()))?;
        }
        return Ok(());
    }
    persist_json(path, &state)
}

pub(crate) fn load_runtime_binding_state(
    storage: &StorageConfig,
) -> anyhow::Result<Option<PersistedRuntimeBindingState>> {
    load_json(storage.runtime_binding_state_path())
}

pub(crate) fn restore_runtime_binding_config(
    storage: &StorageConfig,
    config: &mut NodeConfig,
) -> anyhow::Result<()> {
    if let Some(state) = load_runtime_binding_state(storage)? {
        state.merge_into_node_config(config);
    }
    Ok(())
}

pub(crate) fn persist_runtime_security_state(
    storage: &StorageConfig,
    snapshot: &NodeTelemetrySnapshot,
) -> anyhow::Result<()> {
    persist_json(
        storage.security_state_path(),
        &PersistedRuntimeSecurityState::from_snapshot(snapshot),
    )
}

pub(crate) fn load_runtime_security_state(
    storage: &StorageConfig,
) -> anyhow::Result<Option<PersistedRuntimeSecurityState>> {
    load_json(storage.security_state_path())
}

pub(crate) fn restore_runtime_security_state(
    storage: &StorageConfig,
    snapshot: &mut NodeTelemetrySnapshot,
) -> anyhow::Result<bool> {
    let Some(state) = load_runtime_security_state(storage)? else {
        return Ok(false);
    };
    state.apply_to_snapshot(snapshot);
    snapshot.updated_at = Utc::now();
    Ok(true)
}

pub(crate) fn restore_auth_config(
    storage: &StorageConfig,
    auth: Option<AuthConfig>,
) -> anyhow::Result<Option<AuthConfig>> {
    Ok(load_auth_state(storage)?
        .map(|state| state.merge_into_auth_config(auth.clone()))
        .unwrap_or(auth))
}

fn persist_local_peer_auth(
    storage: &StorageConfig,
    local_peer_auth: PeerAuthEnvelope,
) -> anyhow::Result<()> {
    let state = load_auth_state(storage)?
        .unwrap_or_default()
        .with_local_peer_auth(local_peer_auth);
    persist_json(storage.auth_state_path(), &state)
}

pub(crate) fn persist_control_plane_state(
    storage: &StorageConfig,
    snapshot: &ControlPlaneSnapshot,
) -> anyhow::Result<()> {
    persist_json(
        storage.control_plane_state_path(),
        &PersistedControlPlaneState::from_snapshot(snapshot),
    )?;
    persist_latest_lease_announcements(storage, snapshot)
}

pub(crate) fn load_control_plane_state(
    storage: &StorageConfig,
) -> anyhow::Result<Option<PersistedControlPlaneState>> {
    load_json(storage.control_plane_state_path())
}

pub(crate) fn persist_artifact_transfer_state(
    storage: &StorageConfig,
    transfer_state: &ArtifactTransferState,
) -> anyhow::Result<()> {
    persist_json(
        storage.scoped_transfer_path(&transfer_state.artifact_id),
        transfer_state,
    )
}

pub(crate) fn remove_artifact_transfer_state(
    storage: &StorageConfig,
    artifact_id: &ArtifactId,
) -> anyhow::Result<()> {
    let path = storage.scoped_transfer_path(artifact_id);
    if path.exists() {
        fs::remove_file(&path)
            .map_err(|error| anyhow::anyhow!("failed to remove {}: {error}", path.display()))?;
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn load_artifact_transfer_state(
    storage: &StorageConfig,
    artifact_id: &ArtifactId,
) -> anyhow::Result<Option<ArtifactTransferState>> {
    load_json(storage.scoped_transfer_path(artifact_id))
}

fn load_persisted_artifact_transfer_states(
    storage: &StorageConfig,
) -> anyhow::Result<Vec<ArtifactTransferState>> {
    let mut paths = Vec::new();
    let transfers_dir = storage.transfers_dir();
    if !transfers_dir.exists() {
        return Ok(Vec::new());
    }

    for entry in fs::read_dir(&transfers_dir)
        .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", transfers_dir.display()))?
    {
        let path = entry
            .map_err(|error| {
                anyhow::anyhow!("failed to read {}: {error}", transfers_dir.display())
            })?
            .path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    paths.sort();

    let mut transfers = Vec::new();
    for path in paths {
        if let Some(transfer_state) = load_json(path)? {
            transfers.push(transfer_state);
        }
    }
    Ok(transfers)
}

pub(crate) fn persist_in_flight_transfer_states(
    storage: &StorageConfig,
    transfers: &BTreeMap<ArtifactId, ArtifactTransferState>,
) -> anyhow::Result<()> {
    for transfer_state in load_persisted_artifact_transfer_states(storage)? {
        if !transfers.contains_key(&transfer_state.artifact_id) {
            remove_artifact_transfer_state(storage, &transfer_state.artifact_id)?;
        }
    }

    for transfer_state in transfers.values() {
        persist_artifact_transfer_state(storage, transfer_state)?;
    }

    Ok(())
}

pub(crate) fn restore_in_flight_transfer_states(
    storage: &StorageConfig,
    snapshot: &mut NodeTelemetrySnapshot,
) -> anyhow::Result<bool> {
    let mut restored = false;
    for transfer_state in load_persisted_artifact_transfer_states(storage)? {
        snapshot.update_transfer_state(transfer_state);
        restored = true;
    }
    Ok(restored)
}

pub(crate) fn restore_control_plane_state(
    storage: &StorageConfig,
    snapshot: &mut NodeTelemetrySnapshot,
) -> anyhow::Result<bool> {
    let mut restored = false;

    if let Some(persisted) = load_control_plane_state(storage)?
        && !persisted.is_empty()
    {
        persisted.apply_to_snapshot(&mut snapshot.control_plane);
        restored = true;
    }

    if merge_persisted_lease_announcements(storage, &mut snapshot.control_plane)? {
        restored = true;
    }

    if restored {
        snapshot.updated_at = Utc::now();
    }

    Ok(restored)
}

fn seed_shell_control_plane_state(
    storage: &StorageConfig,
    shell: &mut ControlPlaneShell,
) -> anyhow::Result<bool> {
    let mut restored = false;

    if let Some(persisted) = load_control_plane_state(storage)?
        && !persisted.is_empty()
    {
        persisted.apply_to_shell(shell);
        restored = true;
    }

    if seed_shell_lease_state(storage, shell)? {
        restored = true;
    }

    Ok(restored)
}

fn sync_control_plane_snapshot(
    snapshot: &mut NodeTelemetrySnapshot,
    shell: &ControlPlaneShell,
    storage: Option<&StorageConfig>,
) {
    snapshot.control_plane = shell.snapshot().clone();
    if let Some(storage) = storage
        && let Err(error) = persist_control_plane_state(storage, &snapshot.control_plane)
    {
        snapshot.last_error = Some(format!("failed to persist control plane state: {error}"));
    }
    if let Some(storage) = storage
        && let Err(error) = persist_runtime_security_state(storage, snapshot)
    {
        snapshot.last_error = Some(format!("failed to persist security state: {error}"));
    }
    snapshot.updated_at = Utc::now();
}

fn persist_lease_announcement(
    storage: &StorageConfig,
    announcement: &LeaseAnnouncement,
) -> anyhow::Result<()> {
    persist_json(
        storage.scoped_lease_path(
            &announcement.lease.study_id,
            &announcement.lease.experiment_id,
            &announcement.lease.revision_id,
        ),
        announcement,
    )
}

fn latest_lease_announcements(snapshot: &ControlPlaneSnapshot) -> Vec<LeaseAnnouncement> {
    let mut latest = BTreeMap::<(String, String, String), LeaseAnnouncement>::new();
    for announcement in &snapshot.lease_announcements {
        let key = (
            announcement.lease.study_id.as_str().to_owned(),
            announcement.lease.experiment_id.as_str().to_owned(),
            announcement.lease.revision_id.as_str().to_owned(),
        );
        match latest.get(&key) {
            Some(existing)
                if (existing.lease.window_id.0, existing.announced_at)
                    >= (announcement.lease.window_id.0, announcement.announced_at) => {}
            _ => {
                latest.insert(key, announcement.clone());
            }
        }
    }

    latest.into_values().collect()
}

fn persist_latest_lease_announcements(
    storage: &StorageConfig,
    snapshot: &ControlPlaneSnapshot,
) -> anyhow::Result<()> {
    for announcement in latest_lease_announcements(snapshot) {
        persist_lease_announcement(storage, &announcement)?;
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn load_scoped_lease_announcement(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
) -> anyhow::Result<Option<LeaseAnnouncement>> {
    load_json(storage.scoped_lease_path(
        &experiment.study_id,
        &experiment.experiment_id,
        &experiment.revision_id,
    ))
}

fn load_persisted_lease_announcements(
    storage: &StorageConfig,
) -> anyhow::Result<Vec<LeaseAnnouncement>> {
    let mut paths = Vec::new();
    let leases_dir = storage.leases_dir();
    if !leases_dir.exists() {
        return Ok(Vec::new());
    }

    for entry in fs::read_dir(&leases_dir)
        .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", leases_dir.display()))?
    {
        let path = entry
            .map_err(|error| anyhow::anyhow!("failed to read {}: {error}", leases_dir.display()))?
            .path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    paths.sort();

    let mut announcements = Vec::new();
    for path in paths {
        if let Some(announcement) = load_json(path)? {
            announcements.push(announcement);
        }
    }
    Ok(announcements)
}

fn merge_persisted_lease_announcements(
    storage: &StorageConfig,
    snapshot: &mut ControlPlaneSnapshot,
) -> anyhow::Result<bool> {
    let mut changed = false;
    for announcement in load_persisted_lease_announcements(storage)? {
        if !snapshot.lease_announcements.contains(&announcement) {
            snapshot.lease_announcements.push(announcement);
            changed = true;
        }
    }
    Ok(changed)
}

fn seed_shell_lease_state(
    storage: &StorageConfig,
    shell: &mut ControlPlaneShell,
) -> anyhow::Result<bool> {
    let mut restored = false;
    for announcement in load_persisted_lease_announcements(storage)? {
        if !shell.snapshot().lease_announcements.contains(&announcement) {
            shell.publish_lease(announcement);
            restored = true;
        }
    }
    Ok(restored)
}

pub(crate) fn load_head_state(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
) -> anyhow::Result<Option<HeadDescriptor>> {
    load_json(storage.scoped_current_head_path(experiment))
}

pub(crate) fn persist_limit_profile(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    profile: &LimitProfile,
) -> anyhow::Result<()> {
    persist_json(storage.scoped_limit_profile_path(experiment), profile)
}

pub(crate) fn load_limit_profile(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
) -> anyhow::Result<Option<LimitProfile>> {
    load_json(storage.scoped_limit_profile_path(experiment))
}

pub(crate) fn load_latest_merge_certificate(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
) -> anyhow::Result<Option<MergeCertificate>> {
    let receipts_dir = storage.receipts_dir();
    if !receipts_dir.exists() {
        return Ok(None);
    }

    let mut best: Option<MergeCertificate> = None;
    for entry in fs::read_dir(receipts_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("merge-") || !name.ends_with(".json") {
            continue;
        }

        let Some(certificate) = load_json::<MergeCertificate>(entry.path())? else {
            continue;
        };
        if !experiment.matches_merge_certificate(&certificate) {
            continue;
        }

        let replace = best
            .as_ref()
            .map(|current| certificate.issued_at >= current.issued_at)
            .unwrap_or(true);
        if replace {
            best = Some(certificate);
        }
    }

    Ok(best)
}

pub(crate) fn persist_window_id(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    window_id: WindowId,
) -> anyhow::Result<()> {
    persist_json(storage.scoped_window_path(experiment), &window_id)
}

pub(crate) fn next_window_id(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
) -> anyhow::Result<WindowId> {
    Ok(
        load_json::<WindowId>(storage.scoped_window_path(experiment))?
            .map(|window_id| WindowId(window_id.0 + 1))
            .unwrap_or(WindowId(1)),
    )
}

pub(crate) fn runtime_limit_policy(estimate: &CapabilityEstimate) -> LimitPolicy {
    LimitPolicy {
        target_window_seconds: estimate.target_window_seconds.max(1),
        browser_target_window_seconds: estimate.target_window_seconds.max(1),
        ..LimitPolicy::default()
    }
}

fn runtime_limit_profile_from_benchmark(
    peer_id: &PeerId,
    config: &NodeConfig,
    estimate: &CapabilityEstimate,
    reported_at: DateTime<Utc>,
) -> anyhow::Result<LimitProfile> {
    let persistence = if config.storage.is_some() {
        burn_p2p_core::PersistenceClass::Durable
    } else {
        burn_p2p_core::PersistenceClass::Session
    };
    let available_backends = if estimate.preferred_backends.is_empty() {
        vec![LocalBackend::Cpu]
    } else {
        estimate
            .preferred_backends
            .iter()
            .map(|backend| match backend.as_str() {
                "cuda" => LocalBackend::Cuda,
                "wgpu" => LocalBackend::Wgpu,
                "ndarray" => LocalBackend::Ndarray,
                "cpu" => LocalBackend::Cpu,
                other => LocalBackend::Custom(other.to_owned()),
            })
            .collect()
    };
    let probe = CapabilityProbe {
        peer_id: peer_id.clone(),
        platform: burn_p2p_core::ClientPlatform::Native,
        available_backends,
        device_memory_bytes: None,
        system_memory_bytes: 0,
        disk_bytes: 0,
        upload_mbps: 0.0,
        download_mbps: 0.0,
        persistence,
        attestation_level: burn_p2p_core::AttestationLevel::None,
        work_units_per_second: estimate.work_units_per_second,
        benchmark_hash: None,
    };

    CapabilityCalibrator::new(runtime_limit_policy(estimate))?
        .calibrate(probe, reported_at)
        .map_err(anyhow::Error::from)
}

pub(crate) fn effective_limit_profile(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    peer_id: &PeerId,
    config: &NodeConfig,
    estimate: &CapabilityEstimate,
    reported_at: DateTime<Utc>,
) -> anyhow::Result<LimitProfile> {
    let calibrator = CapabilityCalibrator::new(runtime_limit_policy(estimate))?;
    let profile = match load_limit_profile(storage, experiment)? {
        Some(profile)
            if profile.card.peer_id == *peer_id
                && profile.card.platform == burn_p2p_core::ClientPlatform::Native
                && profile.card.preferred_backends == estimate.preferred_backends =>
        {
            calibrator.recalibrate_profile(&profile, reported_at)?
        }
        _ => runtime_limit_profile_from_benchmark(peer_id, config, estimate, reported_at)?,
    };
    persist_limit_profile(storage, experiment, &profile)?;
    Ok(profile)
}

pub(crate) fn set_effective_limit_profile(
    telemetry: &TelemetryHandle,
    profile: Option<LimitProfile>,
) {
    let mut snapshot = telemetry
        .state
        .lock()
        .expect("telemetry state lock should not be poisoned");
    snapshot.effective_limit_profile = profile;
    snapshot.updated_at = Utc::now();
}

pub(crate) fn connected_peer_ids(snapshot: &NodeTelemetrySnapshot) -> BTreeSet<PeerId> {
    snapshot
        .observed_peer_ids
        .iter()
        .cloned()
        .chain(snapshot.recent_events.iter().filter_map(peer_id_from_event))
        .collect()
}

fn trust_score_for_peer<'a>(
    snapshot: &'a NodeTelemetrySnapshot,
    peer_id: &PeerId,
) -> Option<&'a TrustScore> {
    snapshot
        .trust_scores
        .iter()
        .find(|score| &score.peer_id == peer_id)
}

fn peer_is_reducer_eligible(snapshot: &NodeTelemetrySnapshot, peer_id: &PeerId) -> bool {
    trust_score_for_peer(snapshot, peer_id)
        .map(|score| !score.quarantined && score.reducer_eligible)
        .unwrap_or(true)
}

fn peer_is_validator_eligible(snapshot: &NodeTelemetrySnapshot, peer_id: &PeerId) -> bool {
    trust_score_for_peer(snapshot, peer_id)
        .map(|score| !score.quarantined && score.validator_eligible)
        .unwrap_or(true)
}

pub(crate) fn matches_experiment_head(
    head: &HeadDescriptor,
    experiment: &ExperimentHandle,
) -> bool {
    head.study_id == experiment.study_id
        && head.experiment_id == experiment.experiment_id
        && head.revision_id == experiment.revision_id
}

pub(crate) fn latest_head_from_snapshot(
    snapshot: ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
) -> Option<(PeerId, HeadDescriptor)> {
    snapshot
        .head_announcements
        .into_iter()
        .filter(|announcement| matches_experiment_head(&announcement.head, experiment))
        .max_by(|left, right| {
            left.head
                .global_step
                .cmp(&right.head.global_step)
                .then(left.announced_at.cmp(&right.announced_at))
        })
        .map(|announcement| {
            (
                announcement
                    .provider_peer_id
                    .unwrap_or_else(|| PeerId::new("unknown-provider")),
                announcement.head,
            )
        })
}

fn latest_remote_head(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
) -> Option<(PeerId, HeadDescriptor)> {
    let remote_merged = snapshots
        .iter()
        .filter_map(|(peer_id, snapshot)| {
            if let Some(merge) = latest_merge_from_snapshot(snapshot, experiment) {
                return snapshot
                    .head_announcements
                    .iter()
                    .find(|announcement| announcement.head.head_id == merge.merged_head_id)
                    .map(|announcement| {
                        (
                            announcement
                                .provider_peer_id
                                .clone()
                                .unwrap_or_else(|| peer_id.clone()),
                            announcement.head.clone(),
                        )
                    });
            }
            None
        })
        .max_by(|left, right| {
            left.1
                .global_step
                .cmp(&right.1.global_step)
                .then(left.1.created_at.cmp(&right.1.created_at))
        });

    remote_merged.or_else(|| {
        snapshots
            .iter()
            .filter_map(|(_, snapshot)| latest_head_from_snapshot(snapshot.clone(), experiment))
            .max_by(|left, right| {
                left.1
                    .global_step
                    .cmp(&right.1.global_step)
                    .then(left.1.created_at.cmp(&right.1.created_at))
            })
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LagAssessment {
    pub state: LagState,
    pub head_lag_steps: u64,
}

pub(crate) fn assess_head_lag(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    lag_policy: &LagPolicy,
) -> anyhow::Result<LagAssessment> {
    let local_global_step = load_head_state(storage, experiment)?
        .map(|head| head.global_step)
        .unwrap_or(0);
    let remote_global_step = latest_remote_head(snapshots, experiment)
        .map(|(_, head)| head.global_step)
        .unwrap_or(local_global_step);
    let head_lag_steps = remote_global_step.saturating_sub(local_global_step);
    let state = if head_lag_steps == 0 {
        LagState::Current
    } else if head_lag_steps <= lag_policy.max_head_lag_before_catchup {
        LagState::SlightlyBehind
    } else if head_lag_steps <= lag_policy.max_head_lag_before_block {
        LagState::CatchupRequired
    } else if head_lag_steps <= lag_policy.max_head_lag_before_full_rebase {
        LagState::LeaseBlocked
    } else {
        LagState::RebaseRequired
    };

    Ok(LagAssessment {
        state,
        head_lag_steps,
    })
}

fn latest_merge_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
) -> Option<MergeCertificate> {
    snapshot
        .merge_announcements
        .iter()
        .filter(|announcement| {
            announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
        })
        .max_by(|left, right| {
            left.certificate
                .issued_at
                .cmp(&right.certificate.issued_at)
                .then(left.announced_at.cmp(&right.announced_at))
        })
        .map(|announcement| announcement.certificate.clone())
}

fn latest_auth_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    peer_id: &PeerId,
) -> Option<PeerAuthEnvelope> {
    snapshot
        .auth_announcements
        .iter()
        .filter(|announcement| &announcement.peer_id == peer_id)
        .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
        .map(|announcement| announcement.envelope.clone())
}

pub(crate) fn verify_snapshot_admission(
    policy: &AdmissionPolicy,
    peer_id: &PeerId,
    snapshot: &ControlPlaneSnapshot,
) -> anyhow::Result<PeerAdmissionReport> {
    let envelope = latest_auth_from_snapshot(snapshot, peer_id).ok_or_else(|| {
        anyhow::anyhow!(
            "peer {} did not publish an auth envelope in its control snapshot",
            peer_id.as_str()
        )
    })?;
    Ok(policy.verify_peer_auth(&envelope, peer_id, Utc::now())?)
}

fn note_admitted_peer(snapshot: &mut NodeTelemetrySnapshot, report: PeerAdmissionReport) {
    let now = report.verified_at;
    let peer_id = report.peer_id.clone();
    snapshot.admitted_peers.insert(peer_id.clone(), report);
    snapshot.rejected_peers.remove(&peer_id);
    snapshot
        .peer_reputation
        .entry(peer_id.clone())
        .or_insert_with(|| ReputationState::new(peer_id, now));
}

fn note_rejected_peer(
    snapshot: &mut NodeTelemetrySnapshot,
    peer_id: PeerId,
    reason: String,
    warning_count: u32,
    failure_count: u32,
) {
    snapshot.admitted_peers.remove(&peer_id);
    snapshot.rejected_peers.insert(peer_id.clone(), reason);
    let now = Utc::now();
    let reputation_state = snapshot
        .peer_reputation
        .entry(peer_id.clone())
        .or_insert_with(|| ReputationState::new(peer_id, now));
    let _ = ReputationEngine::default().apply(
        reputation_state,
        ReputationObservation {
            accepted_work_units: 0,
            warning_count,
            failure_count,
            last_control_cert_id: None,
            last_merge_cert_id: None,
        },
        now,
    );
}

fn admission_rejection_reason(report: &PeerAdmissionReport) -> String {
    report
        .findings
        .first()
        .map(|finding| format!("{finding:?}"))
        .unwrap_or_else(|| "peer admission rejected".to_owned())
}

fn latest_announced_revocation_epoch(
    snapshot: &ControlPlaneSnapshot,
    network_id: &NetworkId,
) -> Option<RevocationEpoch> {
    snapshot
        .control_announcements
        .iter()
        .filter(|announcement| announcement.certificate.network_id == *network_id)
        .filter_map(
            |announcement| match &announcement.certificate.body.payload.payload.command {
                ExperimentControlCommand::RevokePeer {
                    minimum_revocation_epoch,
                    ..
                } => Some((*minimum_revocation_epoch, announcement.announced_at)),
                _ => None,
            },
        )
        .max_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)))
        .map(|(epoch, _)| epoch)
}

fn latest_peer_revocation_reason(
    snapshot: &ControlPlaneSnapshot,
    network_id: &NetworkId,
    peer_id: &PeerId,
) -> Option<String> {
    snapshot
        .control_announcements
        .iter()
        .filter(|announcement| announcement.certificate.network_id == *network_id)
        .filter_map(
            |announcement| match &announcement.certificate.body.payload.payload.command {
                ExperimentControlCommand::RevokePeer {
                    peer_id: revoked_peer_id,
                    minimum_revocation_epoch,
                    reason,
                } if revoked_peer_id == peer_id => Some((
                    *minimum_revocation_epoch,
                    announcement.announced_at,
                    reason.clone(),
                )),
                _ => None,
            },
        )
        .max_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then(left.1.cmp(&right.1))
                .then(left.2.cmp(&right.2))
        })
        .map(|(_, _, reason)| reason)
}

fn slot_assignment_from_slot_state(slot_state: &SlotRuntimeState) -> Option<SlotAssignmentState> {
    match slot_state {
        SlotRuntimeState::Unassigned => None,
        SlotRuntimeState::Assigned(assignment)
        | SlotRuntimeState::MaterializingBase(assignment)
        | SlotRuntimeState::FetchingShards(assignment)
        | SlotRuntimeState::Training(assignment)
        | SlotRuntimeState::Publishing(assignment)
        | SlotRuntimeState::CoolingDown(assignment)
        | SlotRuntimeState::Migrating(assignment) => Some(assignment.clone()),
        SlotRuntimeState::Blocked { assignment, .. } => assignment.clone(),
    }
}

fn current_primary_assignment(snapshot: &NodeTelemetrySnapshot) -> Option<SlotAssignmentState> {
    snapshot
        .slot_states
        .first()
        .and_then(slot_assignment_from_slot_state)
}

fn local_admission_rejection_reason(report: &PeerAdmissionReport) -> String {
    report
        .findings
        .first()
        .map(|finding| format!("local certificate rejected: {finding:?}"))
        .unwrap_or_else(|| "local certificate rejected".to_owned())
}

fn local_admission_error_reason(error: &(impl ToString + std::fmt::Display)) -> String {
    format!("local certificate rejected: {error}")
}

fn set_local_node_revoked(snapshot: &mut NodeTelemetrySnapshot, reason: String) {
    snapshot.node_state = NodeRuntimeState::Revoked;
    snapshot.set_primary_slot_state(SlotRuntimeState::Blocked {
        assignment: current_primary_assignment(snapshot),
        reason,
    });
}

fn restore_local_node_ready(snapshot: &mut NodeTelemetrySnapshot) {
    if snapshot.node_state == NodeRuntimeState::Revoked {
        snapshot.node_state = default_node_runtime_state(&snapshot.configured_roles);
        snapshot.set_primary_slot_state(
            current_primary_assignment(snapshot)
                .map(SlotRuntimeState::Assigned)
                .unwrap_or(SlotRuntimeState::Unassigned),
        );
    }
}

fn refresh_local_admission_state(snapshot: &mut NodeTelemetrySnapshot, auth: &Option<AuthConfig>) {
    let Some(auth) = auth else {
        return;
    };
    let Some(admission_policy) = auth.admission_policy.as_ref() else {
        return;
    };
    let Some(local_peer_auth) = auth.local_peer_auth.as_ref() else {
        return;
    };

    let peer_id = local_peer_auth.peer_id.clone();
    match admission_policy.verify_peer_auth(local_peer_auth, &peer_id, Utc::now()) {
        Ok(report) if matches!(report.decision(), AdmissionDecision::Allow) => {
            restore_local_node_ready(snapshot);
        }
        Ok(report) => {
            let reason = report
                .findings
                .iter()
                .find_map(|finding| match finding {
                    AuditFinding::RevocationEpochStale { minimum, found } => {
                        Some((*minimum, *found))
                    }
                    _ => None,
                })
                .map(|(minimum, found)| {
                    latest_peer_revocation_reason(
                        &snapshot.control_plane,
                        &admission_policy.network_id,
                        &peer_id,
                    )
                    .unwrap_or_else(|| {
                        format!(
                            "local certificate revocation epoch {} is below required {}",
                            found.0, minimum.0
                        )
                    })
                })
                .unwrap_or_else(|| local_admission_rejection_reason(&report));
            set_local_node_revoked(snapshot, reason);
        }
        Err(error) => {
            set_local_node_revoked(snapshot, local_admission_error_reason(&error));
        }
    }
}

fn reverify_admitted_peers(
    snapshot: &mut NodeTelemetrySnapshot,
    admission_policy: &AdmissionPolicy,
) {
    let peer_ids = snapshot.admitted_peers.keys().cloned().collect::<Vec<_>>();
    for peer_id in peer_ids {
        match verify_snapshot_admission(admission_policy, &peer_id, &snapshot.control_plane) {
            Ok(report) if matches!(report.decision(), AdmissionDecision::Allow) => {
                note_admitted_peer(snapshot, report);
            }
            Ok(report) => {
                note_rejected_peer(snapshot, peer_id, admission_rejection_reason(&report), 1, 0);
            }
            Err(error) => {
                note_rejected_peer(snapshot, peer_id, error.to_string(), 0, 1);
            }
        }
    }
}

fn reconcile_live_revocation_policy(
    auth: &mut Option<AuthConfig>,
    snapshot: &mut NodeTelemetrySnapshot,
    storage: Option<&StorageConfig>,
) -> bool {
    let Some(network_id) = snapshot.network_id.clone() else {
        return false;
    };
    let Some(announced_epoch) =
        latest_announced_revocation_epoch(&snapshot.control_plane, &network_id)
    else {
        refresh_local_admission_state(snapshot, auth);
        return false;
    };

    let mut changed = false;
    let mut persist_updated_auth = false;
    if snapshot
        .minimum_revocation_epoch
        .is_none_or(|current| announced_epoch > current)
    {
        snapshot.minimum_revocation_epoch = Some(announced_epoch);
        changed = true;
    }

    if let Some(admission_policy) = auth
        .as_mut()
        .and_then(|auth| auth.admission_policy.as_mut())
        && announced_epoch > admission_policy.minimum_revocation_epoch
    {
        admission_policy.minimum_revocation_epoch = announced_epoch;
        changed = true;
        persist_updated_auth = true;
    }

    if persist_updated_auth
        && let Some(storage) = storage
        && let Err(error) = persist_auth_state(storage, auth.as_ref())
    {
        snapshot.last_error = Some(format!("failed to persist updated auth state: {error}"));
    }

    if let Some(admission_policy) = auth
        .as_ref()
        .and_then(|auth| auth.admission_policy.as_ref())
    {
        reverify_admitted_peers(snapshot, admission_policy);
    }
    refresh_local_admission_state(snapshot, auth);

    changed
}

fn remote_trust_bundle_to_state(
    source_url: &str,
    bundle: RemoteTrustBundleExport,
) -> TrustBundleState {
    let trusted_issuers = bundle
        .issuers
        .into_iter()
        .filter(|issuer| issuer.accepted_for_admission)
        .map(|issuer| {
            let trusted = TrustedIssuer {
                issuer_peer_id: issuer.issuer_peer_id.clone(),
                issuer_public_key_hex: issuer.issuer_public_key_hex,
            };
            (issuer.issuer_peer_id, trusted)
        })
        .collect();

    TrustBundleState {
        source_url: source_url.to_owned(),
        required_release_train_hash: bundle.required_release_train_hash,
        allowed_target_artifact_hashes: bundle.allowed_target_artifact_hashes,
        active_issuer_peer_id: bundle.active_issuer_peer_id,
        trusted_issuers,
        minimum_revocation_epoch: bundle.minimum_revocation_epoch,
        reenrollment: bundle
            .reenrollment
            .map(|reenrollment| ClientReenrollmentStatus {
                reason: reenrollment.reason,
                rotated_at: reenrollment.rotated_at,
                legacy_issuer_peer_ids: reenrollment.legacy_issuer_peer_ids,
                login_path: reenrollment.login_path,
                enroll_path: reenrollment.enroll_path,
                trust_bundle_path: reenrollment.trust_bundle_path,
            }),
        updated_at: Utc::now(),
    }
}

fn trust_bundle_changed(current: Option<&TrustBundleState>, next: &TrustBundleState) -> bool {
    let Some(current) = current else {
        return true;
    };

    current.source_url != next.source_url
        || current.active_issuer_peer_id != next.active_issuer_peer_id
        || current.trusted_issuers != next.trusted_issuers
        || current.minimum_revocation_epoch != next.minimum_revocation_epoch
        || current.reenrollment != next.reenrollment
}

#[cfg(not(target_arch = "wasm32"))]
fn fetch_trust_bundle_export(source_url: &str) -> anyhow::Result<RemoteTrustBundleExport> {
    let response = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|error| anyhow::anyhow!("failed to build trust bundle client: {error}"))?
        .get(source_url)
        .send()
        .map_err(|error| {
            anyhow::anyhow!("failed to fetch trust bundle from {source_url}: {error}")
        })?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("trust bundle endpoint {source_url} returned {status}");
    }

    response.json().map_err(|error| {
        anyhow::anyhow!("failed to decode trust bundle from {source_url}: {error}")
    })
}

#[cfg(target_arch = "wasm32")]
fn fetch_trust_bundle_export(source_url: &str) -> anyhow::Result<RemoteTrustBundleExport> {
    futures::executor::block_on(async {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|error| anyhow::anyhow!("failed to build trust bundle client: {error}"))?;
        let response = client.get(source_url).send().await.map_err(|error| {
            anyhow::anyhow!("failed to fetch trust bundle from {source_url}: {error}")
        })?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("trust bundle endpoint {source_url} returned {status}");
        }

        response.json().await.map_err(|error| {
            anyhow::anyhow!("failed to decode trust bundle from {source_url}: {error}")
        })
    })
}

fn fetch_trust_bundle(
    source_url: &str,
    admission_policy: &AdmissionPolicy,
) -> anyhow::Result<TrustBundleState> {
    let bundle = fetch_trust_bundle_export(source_url)?;

    if bundle.network_id != admission_policy.network_id {
        anyhow::bail!(
            "trust bundle network {} does not match runtime network {}",
            bundle.network_id.as_str(),
            admission_policy.network_id.as_str(),
        );
    }
    if bundle.project_family_id != admission_policy.project_family_id {
        anyhow::bail!(
            "trust bundle family {} does not match runtime family {}",
            bundle.project_family_id.as_str(),
            admission_policy.project_family_id.as_str(),
        );
    }
    if bundle.required_release_train_hash != admission_policy.required_release_train_hash {
        anyhow::bail!(
            "trust bundle release train {} does not match runtime release train {}",
            bundle.required_release_train_hash.as_str(),
            admission_policy.required_release_train_hash.as_str(),
        );
    }

    let trust_bundle = remote_trust_bundle_to_state(source_url, bundle);
    if !trust_bundle
        .trusted_issuers
        .contains_key(&trust_bundle.active_issuer_peer_id)
    {
        anyhow::bail!(
            "trust bundle active issuer {} is not accepted for admission",
            trust_bundle.active_issuer_peer_id.as_str(),
        );
    }

    Ok(trust_bundle)
}

fn reconcile_remote_trust_bundle(
    auth: &mut Option<AuthConfig>,
    snapshot: &mut NodeTelemetrySnapshot,
    storage: Option<&StorageConfig>,
) -> bool {
    let Some(auth_config) = auth.as_mut() else {
        return false;
    };
    let Some(initial_policy) = auth_config.admission_policy.as_ref() else {
        return false;
    };
    let endpoints = auth_config
        .trust_bundle_endpoints
        .iter()
        .map(|endpoint| endpoint.trim())
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return false;
    }

    let mut last_error = None;
    for endpoint in endpoints {
        match fetch_trust_bundle(&endpoint, initial_policy) {
            Ok(trust_bundle) => {
                let bundle_changed =
                    trust_bundle_changed(snapshot.trust_bundle.as_ref(), &trust_bundle);
                let mut changed = bundle_changed;

                if trust_bundle.minimum_revocation_epoch
                    > snapshot
                        .minimum_revocation_epoch
                        .unwrap_or(RevocationEpoch(0))
                {
                    snapshot.minimum_revocation_epoch = Some(trust_bundle.minimum_revocation_epoch);
                    changed = true;
                }

                {
                    let admission_policy = auth_config
                        .admission_policy
                        .as_mut()
                        .expect("auth admission policy should exist while reconciling trust");
                    if admission_policy.trusted_issuers != trust_bundle.trusted_issuers {
                        admission_policy.trusted_issuers = trust_bundle.trusted_issuers.clone();
                        changed = true;
                    }
                    if !trust_bundle.allowed_target_artifact_hashes.is_empty()
                        && admission_policy.allowed_target_artifact_hashes
                            != trust_bundle.allowed_target_artifact_hashes
                    {
                        admission_policy.allowed_target_artifact_hashes =
                            trust_bundle.allowed_target_artifact_hashes.clone();
                        changed = true;
                    }
                    if trust_bundle.minimum_revocation_epoch
                        > admission_policy.minimum_revocation_epoch
                    {
                        admission_policy.minimum_revocation_epoch =
                            trust_bundle.minimum_revocation_epoch;
                        changed = true;
                    }
                }

                if bundle_changed {
                    snapshot.trust_bundle = Some(trust_bundle);
                    snapshot.updated_at = Utc::now();
                }

                if changed
                    && let Some(storage) = storage
                    && let Err(error) = persist_auth_state(storage, Some(auth_config))
                {
                    snapshot.last_error = Some(format!(
                        "failed to persist trust bundle auth state: {error}"
                    ));
                }

                if let Some(admission_policy) = auth_config.admission_policy.as_ref() {
                    reverify_admitted_peers(snapshot, admission_policy);
                }
                refresh_local_admission_state(snapshot, auth);
                return changed;
            }
            Err(error) => {
                last_error = Some(error);
            }
        }
    }

    if let Some(error) = last_error {
        snapshot.last_error = Some(error.to_string());
    }

    false
}

pub(crate) fn resolve_canonical_head(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
) -> anyhow::Result<Option<(PeerId, HeadDescriptor)>> {
    let mut best = load_head_state(storage, experiment)?.map(|head| (PeerId::new("local"), head));

    let remote_merged = snapshots
        .iter()
        .filter_map(|(peer_id, snapshot)| {
            if let Some(merge) = latest_merge_from_snapshot(snapshot, experiment) {
                return snapshot
                    .head_announcements
                    .iter()
                    .find(|announcement| announcement.head.head_id == merge.merged_head_id)
                    .map(|announcement| {
                        (
                            announcement
                                .provider_peer_id
                                .clone()
                                .unwrap_or_else(|| peer_id.clone()),
                            announcement.head.clone(),
                        )
                    });
            }
            None
        })
        .max_by(|left, right| {
            left.1
                .global_step
                .cmp(&right.1.global_step)
                .then(left.1.created_at.cmp(&right.1.created_at))
        });

    if let Some(remote_merged) = remote_merged {
        let replace = best
            .as_ref()
            .map(|(_, head)| remote_merged.1.global_step >= head.global_step)
            .unwrap_or(true);
        if replace {
            best = Some(remote_merged);
        }
    }

    if best.is_none() {
        best = snapshots
            .iter()
            .filter_map(|(_, snapshot)| latest_head_from_snapshot(snapshot.clone(), experiment))
            .filter(|(_, head)| head.global_step == 0 || head.parent_head_id.is_none())
            .max_by(|left, right| {
                left.1
                    .global_step
                    .cmp(&right.1.global_step)
                    .then(left.1.created_at.cmp(&right.1.created_at))
            });
    }

    Ok(best)
}

pub(crate) fn metric_quality(metrics: &BTreeMap<String, MetricValue>) -> f64 {
    if let Some(MetricValue::Float(loss)) = metrics.get("loss") {
        return *loss;
    }
    if let Some(MetricValue::Integer(loss)) = metrics.get("loss") {
        return *loss as f64;
    }
    if let Some(MetricValue::Float(score)) = metrics.get("score") {
        return -*score;
    }
    if let Some(MetricValue::Float(accuracy)) = metrics.get("accuracy") {
        return -*accuracy;
    }

    0.0
}

fn numeric_metric_values(metrics: &BTreeMap<String, MetricValue>) -> Vec<f64> {
    metrics
        .values()
        .filter_map(|value| match value {
            MetricValue::Integer(value) => Some(*value as f64),
            MetricValue::Float(value) => Some(*value),
            MetricValue::Bool(_) | MetricValue::Text(_) => None,
        })
        .collect()
}

pub(crate) fn update_feature_sketch_from_metrics(
    metrics: &BTreeMap<String, MetricValue>,
    reference_metrics: Option<&BTreeMap<String, MetricValue>>,
    sketch_dimensionality: usize,
    staleness_windows: u16,
    receive_delay_ms: u64,
    canary_loss_delta: Option<f64>,
) -> UpdateFeatureSketch {
    let values = numeric_metric_values(metrics);
    let reference = reference_metrics.map(numeric_metric_values);
    burn_p2p_security::extract_feature_sketch(
        &values,
        reference.as_deref(),
        &[burn_p2p_security::FeatureLayer::new(
            "metrics",
            0,
            values.len(),
        )],
        sketch_dimensionality.max(1),
        staleness_windows,
        receive_delay_ms,
        canary_loss_delta,
    )
}

pub(crate) fn active_experiment_directory_entry(
    config: &NodeConfig,
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
) -> Option<ExperimentDirectoryEntry> {
    config
        .auth
        .as_ref()
        .and_then(|auth| {
            auth.experiment_directory.iter().find(|entry| {
                entry.network_id == experiment.network_id
                    && entry.study_id == experiment.study_id
                    && entry.experiment_id == experiment.experiment_id
                    && entry.current_revision_id == experiment.revision_id
            })
        })
        .cloned()
        .or_else(|| {
            snapshot
                .control_plane
                .directory_announcements
                .iter()
                .filter(|announcement| announcement.network_id == experiment.network_id)
                .flat_map(|announcement| announcement.entries.iter())
                .find(|entry| {
                    entry.network_id == experiment.network_id
                        && entry.study_id == experiment.study_id
                        && entry.experiment_id == experiment.experiment_id
                        && entry.current_revision_id == experiment.revision_id
                })
                .cloned()
        })
}

pub(crate) fn runtime_robustness_policy(
    config: &NodeConfig,
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
) -> RobustnessPolicy {
    active_experiment_directory_entry(config, snapshot, experiment)
        .and_then(|entry| entry.robustness_policy())
        .unwrap_or_default()
}

pub(crate) fn update_norm_stats(metrics: &BTreeMap<String, MetricValue>) -> UpdateNormStats {
    let values = numeric_metric_values(metrics);
    let l2_norm = values.iter().map(|value| value * value).sum::<f64>().sqrt();
    let max_abs = values
        .iter()
        .map(|value| value.abs())
        .fold(0.0_f64, f64::max);
    let non_finite_tensors = values.iter().filter(|value| !value.is_finite()).count() as u32;

    UpdateNormStats {
        l2_norm,
        max_abs,
        clipped: false,
        non_finite_tensors,
    }
}

pub(crate) fn latest_merge_window_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
    base_head_id: Option<&HeadId>,
) -> Option<MergeWindowState> {
    snapshot
        .merge_window_announcements
        .iter()
        .filter(|announcement| {
            announcement.merge_window.study_id == experiment.study_id
                && announcement.merge_window.experiment_id == experiment.experiment_id
                && announcement.merge_window.revision_id == experiment.revision_id
                && base_head_id
                    .map(|head_id| &announcement.merge_window.base_head_id == head_id)
                    .unwrap_or(true)
        })
        .max_by(|left, right| {
            left.merge_window
                .window_id
                .cmp(&right.merge_window.window_id)
                .then(left.announced_at.cmp(&right.announced_at))
        })
        .map(|announcement| announcement.merge_window.clone())
}

pub(crate) fn latest_merge_window_from_connected_snapshots(
    local_snapshot: &ControlPlaneSnapshot,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
    base_head_id: Option<&HeadId>,
) -> Option<MergeWindowState> {
    let mut best = latest_merge_window_from_snapshot(local_snapshot, experiment, base_head_id);
    for (_, snapshot) in snapshots {
        let Some(candidate) = latest_merge_window_from_snapshot(snapshot, experiment, base_head_id)
        else {
            continue;
        };
        let replace = best
            .as_ref()
            .map(|current| {
                candidate.window_id > current.window_id
                    || (candidate.window_id == current.window_id
                        && candidate.opened_at > current.opened_at)
            })
            .unwrap_or(true);
        if replace {
            best = Some(candidate);
        }
    }
    best
}

pub(crate) fn latest_reducer_assignment_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    window_id: WindowId,
    source_peer_id: &PeerId,
) -> Option<ReducerAssignment> {
    snapshot
        .reducer_assignment_announcements
        .iter()
        .filter(|announcement| {
            announcement.assignment.window_id == window_id
                && &announcement.assignment.source_peer_id == source_peer_id
        })
        .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
        .map(|announcement| announcement.assignment.clone())
}

fn update_announces_for_window(
    snapshot: &ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
    window_id: WindowId,
    base_head_id: &HeadId,
) -> Vec<UpdateAnnounce> {
    let mut updates = BTreeMap::<PeerId, UpdateAnnounce>::new();
    for announcement in &snapshot.update_announcements {
        if announcement.update.study_id != experiment.study_id
            || announcement.update.experiment_id != experiment.experiment_id
            || announcement.update.revision_id != experiment.revision_id
            || announcement.update.window_id != window_id
            || &announcement.update.base_head_id != base_head_id
        {
            continue;
        }
        updates.insert(
            announcement.update.peer_id.clone(),
            announcement.update.clone(),
        );
    }

    updates.into_values().collect()
}

pub(crate) fn update_announces_from_connected_snapshots(
    local_snapshot: &ControlPlaneSnapshot,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
    window_id: WindowId,
    base_head_id: &HeadId,
) -> Vec<UpdateAnnounce> {
    let mut updates = BTreeMap::<PeerId, UpdateAnnounce>::new();
    for update in update_announces_for_window(local_snapshot, experiment, window_id, base_head_id) {
        updates.insert(update.peer_id.clone(), update);
    }
    for (_, snapshot) in snapshots {
        for update in update_announces_for_window(snapshot, experiment, window_id, base_head_id) {
            updates.insert(update.peer_id.clone(), update);
        }
    }
    updates.into_values().collect()
}

pub(crate) fn runtime_merge_topology_policy(
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
    base_head_id: Option<&HeadId>,
) -> MergeTopologyPolicy {
    latest_merge_window_from_snapshot(&snapshot.control_plane, experiment, base_head_id)
        .map(|merge_window| merge_window.policy)
        .unwrap_or_default()
}

pub(crate) fn runtime_topology_peers(
    snapshot: &NodeTelemetrySnapshot,
    local_peer_id: &PeerId,
) -> Vec<PeerId> {
    let mut peers = connected_peer_ids(snapshot)
        .into_iter()
        .filter(|peer_id| peer_is_reducer_eligible(snapshot, peer_id))
        .collect::<BTreeSet<_>>();
    if peer_is_reducer_eligible(snapshot, local_peer_id) {
        peers.insert(local_peer_id.clone());
    }
    if peers.is_empty() {
        peers.insert(local_peer_id.clone());
    }
    peers.into_iter().collect()
}

pub(crate) fn runtime_validator_peers(
    snapshot: &NodeTelemetrySnapshot,
    local_peer_id: &PeerId,
) -> Vec<PeerId> {
    let mut peers = connected_peer_ids(snapshot)
        .into_iter()
        .filter(|peer_id| peer_is_validator_eligible(snapshot, peer_id))
        .collect::<BTreeSet<_>>();
    if peer_is_validator_eligible(snapshot, local_peer_id) {
        peers.insert(local_peer_id.clone());
    }
    if peers.is_empty() {
        peers.insert(local_peer_id.clone());
    }
    peers.into_iter().collect()
}

pub(crate) fn runtime_validators(
    roles: &PeerRoleSet,
    local_peer_id: &PeerId,
    peers: &[PeerId],
    quorum: u16,
) -> Vec<PeerId> {
    let mut validators = Vec::new();
    let local_in_pool = peers.iter().any(|peer_id| peer_id == local_peer_id);
    if local_in_pool
        && (roles.contains(&PeerRole::Validator) || roles.contains(&PeerRole::Authority))
    {
        validators.push(local_peer_id.clone());
    }
    for peer_id in peers {
        if !validators.contains(peer_id) {
            validators.push(peer_id.clone());
        }
        if validators.len() >= usize::from(quorum.max(1)) {
            break;
        }
    }
    if validators.is_empty() {
        validators.push(
            peers
                .first()
                .cloned()
                .unwrap_or_else(|| local_peer_id.clone()),
        );
    }
    validators
}

pub(crate) fn open_runtime_merge_window(
    experiment: &ExperimentHandle,
    window_id: WindowId,
    base_head_id: HeadId,
    policy: MergeTopologyPolicy,
    reducers: Vec<PeerId>,
    validators: Vec<PeerId>,
) -> anyhow::Result<MergeWindowState> {
    let opened_at = Utc::now();
    let closes_at = opened_at + chrono::Duration::seconds(i64::from(policy.window_duration_secs));
    Ok(MergeWindowState {
        merge_window_id: ContentId::derive(&(
            experiment.network_id.as_str(),
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
            window_id.0,
            base_head_id.as_str(),
        ))?,
        network_id: experiment.network_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id,
        base_head_id,
        policy,
        reducers,
        validators,
        opened_at,
        closes_at,
    })
}

pub(crate) fn runtime_assign_reducers(
    merge_window: &MergeWindowState,
    source_peer_id: &PeerId,
    reducer_pool: &[PeerId],
) -> anyhow::Result<ReducerAssignment> {
    match burn_p2p_experiment::assign_reducers(merge_window, source_peer_id, reducer_pool) {
        Ok(assignment) => Ok(assignment),
        Err(_) => {
            let mut assigned_reducers = reducer_pool
                .iter()
                .take(usize::from(merge_window.policy.reducer_replication.max(1)))
                .cloned()
                .collect::<Vec<_>>();
            if assigned_reducers.is_empty() {
                assigned_reducers.push(source_peer_id.clone());
            }

            let mut repair_reducers = reducer_pool
                .iter()
                .skip(assigned_reducers.len())
                .take(usize::from(merge_window.policy.reducer_replication.max(1)))
                .cloned()
                .collect::<Vec<_>>();
            if repair_reducers.is_empty() {
                repair_reducers = assigned_reducers.clone();
            }

            let mut upper_tier_reducers = reducer_pool
                .iter()
                .filter(|peer_id| !assigned_reducers.contains(*peer_id))
                .take(usize::from(merge_window.policy.upper_fanin.max(1)))
                .cloned()
                .collect::<Vec<_>>();
            if upper_tier_reducers.is_empty() {
                upper_tier_reducers = assigned_reducers.clone();
            }

            Ok(ReducerAssignment {
                assignment_id: ContentId::derive(&(
                    merge_window.merge_window_id.as_str(),
                    source_peer_id.as_str(),
                    &assigned_reducers,
                    &repair_reducers,
                    &upper_tier_reducers,
                ))?,
                window_id: merge_window.window_id,
                source_peer_id: source_peer_id.clone(),
                assigned_reducers,
                repair_reducers,
                upper_tier_reducers,
                assigned_at: Utc::now(),
            })
        }
    }
}

pub(crate) fn resolve_identity(
    identity: &IdentityConfig,
    storage: Option<&StorageConfig>,
) -> anyhow::Result<Keypair> {
    match identity {
        IdentityConfig::Ephemeral => Ok(Keypair::generate_ed25519()),
        IdentityConfig::Persistent => {
            let storage = storage.ok_or_else(|| {
                anyhow::anyhow!("persistent identity requires a configured storage root")
            })?;
            let identity_path = storage.identity_path();
            if identity_path.exists() {
                let bytes = fs::read(&identity_path).map_err(|error| {
                    anyhow::anyhow!(
                        "failed to read persisted identity {}: {error}",
                        identity_path.display()
                    )
                })?;
                return Keypair::from_protobuf_encoding(&bytes).map_err(|error| {
                    anyhow::anyhow!(
                        "failed to decode persisted identity {}: {error}",
                        identity_path.display()
                    )
                });
            }

            let keypair = Keypair::generate_ed25519();
            let bytes = keypair
                .to_protobuf_encoding()
                .map_err(|error| anyhow::anyhow!("failed to encode persisted identity: {error}"))?;
            fs::write(&identity_path, bytes).map_err(|error| {
                anyhow::anyhow!(
                    "failed to persist identity {}: {error}",
                    identity_path.display()
                )
            })?;
            Ok(keypair)
        }
    }
}

pub(crate) fn run_control_plane(
    boundary: RuntimeBoundary,
    keypair: Keypair,
    storage: Option<StorageConfig>,
    auth: Option<AuthConfig>,
    command_rx: mpsc::Receiver<RuntimeCommand>,
    state: Arc<Mutex<NodeTelemetrySnapshot>>,
) {
    const REDIAL_INTERVAL: Duration = Duration::from_secs(1);
    const TRUST_BUNDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
    let mut auth = auth;
    let mut shell = match ControlPlaneShell::new(
        boundary.protocols.control.clone(),
        keypair,
        boundary
            .listen_addresses
            .iter()
            .chain(boundary.bootstrap_addresses.iter())
            .cloned(),
    ) {
        Ok(shell) => shell,
        Err(error) => {
            let mut snapshot = state
                .lock()
                .expect("telemetry state lock should not be poisoned");
            snapshot.set_error(error.to_string());
            return;
        }
    };
    {
        let mut snapshot = state
            .lock()
            .expect("telemetry state lock should not be poisoned");
        snapshot.local_peer_id = Some(PeerId::new(shell.local_peer_id().to_string()));
        if !matches!(
            snapshot.node_state,
            NodeRuntimeState::Quarantined | NodeRuntimeState::Revoked
        ) {
            snapshot.set_node_state(NodeRuntimeState::Connecting);
        }
    }

    if let Some(storage) = storage.as_ref()
        && let Err(error) = seed_shell_control_plane_state(storage, &mut shell)
    {
        let mut snapshot = state
            .lock()
            .expect("telemetry state lock should not be poisoned");
        snapshot.last_error = Some(format!("failed to restore control plane state: {error}"));
    }

    for address in &boundary.listen_addresses {
        if let Err(error) = shell.listen_on(address.clone()) {
            let mut snapshot = state
                .lock()
                .expect("telemetry state lock should not be poisoned");
            snapshot.set_error(error.to_string());
            return;
        }
    }

    for address in &boundary.bootstrap_addresses {
        if let Err(error) = shell.dial(address.clone()) {
            let mut snapshot = state
                .lock()
                .expect("telemetry state lock should not be poisoned");
            snapshot.push_event(LiveControlPlaneEvent::Other {
                kind: format!("bootstrap-dial-error:{error}"),
            });
        }
    }

    if let Err(error) = shell.subscribe_topic(boundary.control_overlay.clone()) {
        let mut snapshot = state
            .lock()
            .expect("telemetry state lock should not be poisoned");
        snapshot.set_error(error.to_string());
        return;
    }

    {
        let mut snapshot = state
            .lock()
            .expect("telemetry state lock should not be poisoned");
        snapshot.status = RuntimeStatus::Running;
        if let Some(storage) = storage.as_ref()
            && let Err(error) = restore_runtime_security_state(storage, &mut snapshot)
        {
            snapshot.last_error = Some(format!("failed to restore security state: {error}"));
        }
        if !matches!(
            snapshot.node_state,
            NodeRuntimeState::Quarantined | NodeRuntimeState::Revoked
        ) {
            snapshot.node_state = default_node_runtime_state(&snapshot.configured_roles);
        }
        snapshot.connected_peers = shell.connected_peer_count();
        sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
        reconcile_live_revocation_policy(&mut auth, &mut snapshot, storage.as_ref());
        let trust_bundle_changed =
            reconcile_remote_trust_bundle(&mut auth, &mut snapshot, storage.as_ref());
        if trust_bundle_changed {
            for peer_id in connected_peer_ids(&snapshot) {
                let _ = shell.request_snapshot(peer_id.as_str());
            }
        }
        if let Some(storage) = storage.as_ref() {
            let _ = persist_runtime_security_state(storage, &snapshot);
        }
    }

    if let Some(auth_config) = auth.as_ref() {
        if shell.snapshot().auth_announcements.is_empty()
            && let Some(local_peer_auth) = auth_config.local_peer_auth.clone()
        {
            shell.publish_auth(PeerAuthAnnouncement {
                peer_id: local_peer_auth.peer_id.clone(),
                envelope: local_peer_auth,
                announced_at: Utc::now(),
            });
        }
        if shell.snapshot().directory_announcements.is_empty()
            && !auth_config.experiment_directory.is_empty()
        {
            let network_id = auth_config
                .local_peer_auth
                .as_ref()
                .map(|envelope| envelope.certificate.claims().network_id.clone())
                .or_else(|| {
                    auth_config
                        .experiment_directory
                        .first()
                        .map(|entry| entry.network_id.clone())
                })
                .or_else(|| {
                    state
                        .lock()
                        .expect("telemetry state lock should not be poisoned")
                        .network_id
                        .clone()
                })
                .unwrap_or_else(|| NetworkId::new("unknown"));
            shell.publish_directory(ExperimentDirectoryAnnouncement {
                network_id,
                entries: auth_config.experiment_directory.clone(),
                announced_at: Utc::now(),
            });
        }

        let mut snapshot = state
            .lock()
            .expect("telemetry state lock should not be poisoned");
        sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
        reconcile_live_revocation_policy(&mut auth, &mut snapshot, storage.as_ref());
        let trust_bundle_changed =
            reconcile_remote_trust_bundle(&mut auth, &mut snapshot, storage.as_ref());
        if trust_bundle_changed {
            for peer_id in connected_peer_ids(&snapshot) {
                let _ = shell.request_snapshot(peer_id.as_str());
            }
        }
        if let Some(storage) = storage.as_ref() {
            let _ = persist_runtime_security_state(storage, &snapshot);
        }
    }

    let mut last_redial_at = Instant::now();
    let mut last_trust_bundle_sync_at = Instant::now()
        .checked_sub(TRUST_BUNDLE_REFRESH_INTERVAL)
        .unwrap_or_else(Instant::now);

    loop {
        let mut shutdown_requested = false;
        loop {
            match command_rx.try_recv() {
                Ok(RuntimeCommand::SubscribeTopic(topic)) => {
                    if let Err(error) = shell.subscribe_topic(topic.clone()) {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.push_event(LiveControlPlaneEvent::Other {
                            kind: format!("topic-subscribe-error:{}:{error}", topic.as_str()),
                        });
                        snapshot.last_error = Some(error.to_string());
                    }
                }
                Ok(RuntimeCommand::PublishControl(announcement)) => {
                    shell.publish_control(announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Control(announcement),
                    ) {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                    let revocation_changed = reconcile_live_revocation_policy(
                        &mut auth,
                        &mut snapshot,
                        storage.as_ref(),
                    );
                    if revocation_changed {
                        for peer_id in connected_peer_ids(&snapshot) {
                            let _ = shell.request_snapshot(peer_id.as_str());
                        }
                    }
                }
                Ok(RuntimeCommand::PublishHead(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_head(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Head(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishLease(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_lease(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Lease(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishMerge(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_merge(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Merge(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishMergeWindow(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_merge_window(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::MergeWindow(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishReducerAssignment(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_reducer_assignment(announcement.clone());
                    if let Err(error) = shell
                        .publish_pubsub(overlay, PubsubPayload::ReducerAssignment(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishUpdate(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_update(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Update(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishAggregate(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_aggregate(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Aggregate(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishReductionCertificate(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_reduction_certificate(announcement.clone());
                    if let Err(error) = shell
                        .publish_pubsub(overlay, PubsubPayload::ReductionCertificate(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishReducerLoad(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_reducer_load(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::ReducerLoad(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    snapshot.control_plane = shell.snapshot().clone();
                    snapshot.updated_at = Utc::now();
                }
                Ok(RuntimeCommand::PublishAuth(announcement)) => {
                    let local_announcement = announcement.clone();
                    shell.publish_auth(local_announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Auth(announcement),
                    ) {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                    if local_announcement.peer_id == PeerId::new(shell.local_peer_id().to_string())
                        && let Some(auth_config) = auth.as_mut()
                    {
                        auth_config.local_peer_auth = Some(local_announcement.envelope.clone());
                    }
                    reconcile_live_revocation_policy(&mut auth, &mut snapshot, storage.as_ref());
                    if local_announcement.peer_id == PeerId::new(shell.local_peer_id().to_string())
                        && let Some(storage) = storage.as_ref()
                        && let Err(error) =
                            persist_local_peer_auth(storage, local_announcement.envelope.clone())
                    {
                        snapshot.last_error =
                            Some(format!("failed to persist local peer auth: {error}"));
                    }
                }
                Ok(RuntimeCommand::PublishDirectory(announcement)) => {
                    shell.publish_directory(announcement.clone());
                    if let Err(error) = shell.publish_pubsub(
                        boundary.control_overlay.clone(),
                        PubsubPayload::Directory(announcement),
                    ) {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                    reconcile_live_revocation_policy(&mut auth, &mut snapshot, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishMetrics(announcement)) => {
                    let overlay = announcement.overlay.clone();
                    let _ = shell.subscribe_topic(overlay.clone());
                    shell.publish_metrics(announcement.clone());
                    if let Err(error) =
                        shell.publish_pubsub(overlay, PubsubPayload::Metrics(announcement))
                    {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_error = Some(error.to_string());
                    }
                    let mut snapshot = state
                        .lock()
                        .expect("telemetry state lock should not be poisoned");
                    sync_control_plane_snapshot(&mut snapshot, &shell, storage.as_ref());
                }
                Ok(RuntimeCommand::PublishArtifact { descriptor, chunks }) => {
                    shell.publish_artifact(descriptor, chunks);
                }
                Ok(RuntimeCommand::FetchSnapshot {
                    peer_id,
                    timeout,
                    reply,
                }) => {
                    let result = shell
                        .fetch_snapshot(&peer_id, timeout)
                        .map_err(|error| error.to_string());
                    if let Ok(remote_snapshot) = &result {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.last_snapshot_peer_id = Some(PeerId::new(peer_id.clone()));
                        snapshot.last_snapshot = Some(remote_snapshot.clone());
                        snapshot.updated_at = Utc::now();
                    }
                    let _ = reply.send(result);
                }
                Ok(RuntimeCommand::FetchArtifactManifest {
                    peer_id,
                    artifact_id,
                    timeout,
                    reply,
                }) => {
                    let result = shell
                        .fetch_artifact_manifest(&peer_id, artifact_id, timeout)
                        .map_err(|error| error.to_string());
                    let _ = reply.send(result);
                }
                Ok(RuntimeCommand::FetchArtifactChunk {
                    peer_id,
                    artifact_id,
                    chunk_id,
                    timeout,
                    reply,
                }) => {
                    let result = shell
                        .fetch_artifact_chunk(&peer_id, artifact_id, chunk_id, timeout)
                        .map_err(|error| error.to_string());
                    let _ = reply.send(result);
                }
                Ok(RuntimeCommand::RequestSnapshot { peer_id }) => {
                    if let Err(error) = shell.request_snapshot(&peer_id) {
                        let mut snapshot = state
                            .lock()
                            .expect("telemetry state lock should not be poisoned");
                        snapshot.push_event(LiveControlPlaneEvent::RequestFailure {
                            peer_id,
                            message: error.to_string(),
                        });
                    }
                }
                Ok(RuntimeCommand::Shutdown) => {
                    shutdown_requested = true;
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    shutdown_requested = true;
                    break;
                }
            }
        }

        if shutdown_requested {
            let mut snapshot = state
                .lock()
                .expect("telemetry state lock should not be poisoned");
            snapshot.set_node_state(NodeRuntimeState::ShuttingDown);
            snapshot.status = RuntimeStatus::Stopped;
            snapshot.updated_at = Utc::now();
            return;
        }

        if shell.connected_peer_count() == 0 && last_redial_at.elapsed() >= REDIAL_INTERVAL {
            let known_addresses = {
                let snapshot = state
                    .lock()
                    .expect("telemetry state lock should not be poisoned");
                snapshot.known_peer_addresses.clone()
            };
            for address in boundary
                .bootstrap_addresses
                .iter()
                .cloned()
                .chain(known_addresses)
            {
                let _ = shell.dial(address);
            }
            last_redial_at = Instant::now();
        }

        if last_trust_bundle_sync_at.elapsed() >= TRUST_BUNDLE_REFRESH_INTERVAL {
            let mut snapshot = state
                .lock()
                .expect("telemetry state lock should not be poisoned");
            let trust_bundle_changed =
                reconcile_remote_trust_bundle(&mut auth, &mut snapshot, storage.as_ref());
            if trust_bundle_changed {
                for peer_id in connected_peer_ids(&snapshot) {
                    let _ = shell.request_snapshot(peer_id.as_str());
                }
            }
            if let Some(storage) = storage.as_ref()
                && let Err(error) = persist_runtime_security_state(storage, &snapshot)
            {
                snapshot.last_error = Some(format!("failed to persist security state: {error}"));
            }
            last_trust_bundle_sync_at = Instant::now();
        }

        match shell.wait_event(Duration::from_millis(50)) {
            Some(event) => {
                let mut connection_request_error = None;
                if let LiveControlPlaneEvent::ConnectionEstablished { peer_id } = &event
                    && let Err(error) = shell.request_snapshot(peer_id)
                {
                    connection_request_error = Some((peer_id.clone(), error.to_string()));
                }

                let mut snapshot = state
                    .lock()
                    .expect("telemetry state lock should not be poisoned");
                snapshot.connected_peers = shell.connected_peer_count();
                snapshot.control_plane = shell.snapshot().clone();
                if matches!(
                    &event,
                    LiveControlPlaneEvent::PubsubMessage { kind, .. }
                        if kind == "control"
                            || kind == "auth"
                            || kind == "directory"
                            || kind == "lease"
                ) && let Some(storage) = storage.as_ref()
                    && let Err(error) =
                        persist_control_plane_state(storage, &snapshot.control_plane)
                {
                    snapshot.last_error =
                        Some(format!("failed to persist control plane state: {error}"));
                }
                if let Some((peer_id, message)) = connection_request_error {
                    snapshot.push_event(LiveControlPlaneEvent::RequestFailure {
                        peer_id,
                        message: message.clone(),
                    });
                    snapshot.last_error = Some(message);
                }
                match &event {
                    LiveControlPlaneEvent::NewListenAddr { address } => {
                        if !snapshot.listen_addresses.contains(address) {
                            snapshot.listen_addresses.push(address.clone());
                        }
                    }
                    LiveControlPlaneEvent::PeersDiscovered { peers } => {
                        remember_known_peer_addresses(
                            &mut snapshot,
                            storage.as_ref(),
                            peers.iter().map(|(_, address)| address.clone()),
                        );
                    }
                    LiveControlPlaneEvent::PeerIdentified {
                        listen_addresses, ..
                    } => {
                        remember_known_peer_addresses(
                            &mut snapshot,
                            storage.as_ref(),
                            listen_addresses.iter().cloned(),
                        );
                    }
                    LiveControlPlaneEvent::SnapshotReceived {
                        peer_id,
                        snapshot: remote_snapshot,
                    } => {
                        snapshot.last_snapshot_peer_id = Some(PeerId::new(peer_id.clone()));
                        snapshot.last_snapshot = Some(remote_snapshot.clone());
                        if let Some(policy) = auth
                            .as_ref()
                            .and_then(|auth| auth.admission_policy.as_ref())
                        {
                            match verify_snapshot_admission(
                                policy,
                                &PeerId::new(peer_id.clone()),
                                remote_snapshot,
                            ) {
                                Ok(report)
                                    if matches!(report.decision(), AdmissionDecision::Allow) =>
                                {
                                    note_admitted_peer(&mut snapshot, report);
                                }
                                Ok(report) => {
                                    note_rejected_peer(
                                        &mut snapshot,
                                        PeerId::new(peer_id.clone()),
                                        admission_rejection_reason(&report),
                                        1,
                                        0,
                                    );
                                    snapshot.last_error = Some(format!(
                                        "peer {} failed admission with {} findings",
                                        peer_id,
                                        report.findings.len()
                                    ));
                                }
                                Err(error) => {
                                    note_rejected_peer(
                                        &mut snapshot,
                                        PeerId::new(peer_id.clone()),
                                        error.to_string(),
                                        0,
                                        1,
                                    );
                                    snapshot.last_error =
                                        Some(format!("peer {} admission error: {error}", peer_id));
                                }
                            }
                        }
                        reconcile_live_revocation_policy(
                            &mut auth,
                            &mut snapshot,
                            storage.as_ref(),
                        );
                    }
                    LiveControlPlaneEvent::RequestFailure { message, .. }
                    | LiveControlPlaneEvent::InboundFailure { message, .. }
                    | LiveControlPlaneEvent::ResponseSendFailure { message, .. }
                    | LiveControlPlaneEvent::OutgoingConnectionError { message, .. }
                    | LiveControlPlaneEvent::IncomingConnectionError { message } => {
                        snapshot.last_error = Some(message.clone());
                    }
                    LiveControlPlaneEvent::PubsubMessage { .. }
                    | LiveControlPlaneEvent::TopicSubscribed { .. }
                    | LiveControlPlaneEvent::PeersExpired { .. } => {}
                    LiveControlPlaneEvent::Other { .. }
                    | LiveControlPlaneEvent::ConnectionEstablished { .. }
                    | LiveControlPlaneEvent::ArtifactManifestRequested { .. }
                    | LiveControlPlaneEvent::ArtifactManifestReceived { .. }
                    | LiveControlPlaneEvent::ArtifactChunkRequested { .. }
                    | LiveControlPlaneEvent::ArtifactChunkReceived { .. }
                    | LiveControlPlaneEvent::SnapshotRequested { .. }
                    | LiveControlPlaneEvent::SnapshotResponseSent { .. } => {}
                }
                if matches!(
                    &event,
                    LiveControlPlaneEvent::PubsubMessage { kind, .. }
                        if kind == "control" || kind == "auth" || kind == "directory"
                ) {
                    let revocation_changed = reconcile_live_revocation_policy(
                        &mut auth,
                        &mut snapshot,
                        storage.as_ref(),
                    );
                    if revocation_changed {
                        for peer_id in connected_peer_ids(&snapshot) {
                            let _ = shell.request_snapshot(peer_id.as_str());
                        }
                    }
                }
                snapshot.push_event(event);
                if let Some(storage) = storage.as_ref()
                    && let Err(error) = persist_runtime_security_state(storage, &snapshot)
                {
                    snapshot.last_error =
                        Some(format!("failed to persist security state: {error}"));
                }
            }
            None => thread::sleep(Duration::from_millis(10)),
        }
    }
}

pub(crate) fn remember_known_peer_addresses(
    snapshot: &mut NodeTelemetrySnapshot,
    storage: Option<&StorageConfig>,
    addresses: impl IntoIterator<Item = SwarmAddress>,
) {
    let mut changed = false;
    for address in addresses {
        if snapshot.listen_addresses.contains(&address) {
            continue;
        }
        if snapshot.known_peer_addresses.insert(address) {
            changed = true;
        }
    }

    if changed
        && let Some(storage) = storage
        && let Err(error) = persist_known_peers(storage, &snapshot.known_peer_addresses)
    {
        snapshot.last_error = Some(format!("failed to persist known peers: {error}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_snapshot(roles: impl IntoIterator<Item = PeerRole>) -> NodeTelemetrySnapshot {
        NodeTelemetrySnapshot::starting(
            &MainnetHandle {
                genesis: GenesisSpec {
                    network_id: NetworkId::new("net-test"),
                    protocol_version: Version::new(1, 0, 0),
                    display_name: String::from("test"),
                    created_at: Utc::now(),
                    metadata: BTreeMap::new(),
                },
                roles: PeerRoleSet::new(roles),
            },
            &NodeConfig::default(),
        )
    }

    fn trust_score(
        peer_id: &str,
        reducer_eligible: bool,
        validator_eligible: bool,
        quarantined: bool,
    ) -> TrustScore {
        TrustScore {
            peer_id: PeerId::new(peer_id),
            score: if quarantined { -4.0 } else { 0.5 },
            reducer_eligible,
            validator_eligible,
            quarantined,
            ban_recommended: false,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn runtime_topology_peers_exclude_quarantined_and_reducer_demoted_peers() {
        let mut snapshot = test_snapshot([PeerRole::TrainerCpu]);
        snapshot.observed_peer_ids = BTreeSet::from([
            PeerId::new("peer-good"),
            PeerId::new("peer-quarantined"),
            PeerId::new("peer-validator-only"),
        ]);
        snapshot.trust_scores = vec![
            trust_score("peer-quarantined", false, false, true),
            trust_score("peer-validator-only", false, true, false),
        ];

        let peers = runtime_topology_peers(&snapshot, &PeerId::new("local"));

        assert!(peers.contains(&PeerId::new("local")));
        assert!(peers.contains(&PeerId::new("peer-good")));
        assert!(!peers.contains(&PeerId::new("peer-quarantined")));
        assert!(!peers.contains(&PeerId::new("peer-validator-only")));
    }

    #[test]
    fn runtime_validator_peers_and_quorum_respect_validator_eligibility() {
        let mut snapshot = test_snapshot([PeerRole::Validator]);
        snapshot.observed_peer_ids = BTreeSet::from([
            PeerId::new("peer-good"),
            PeerId::new("peer-reducer-only"),
            PeerId::new("peer-quarantined"),
        ]);
        snapshot.trust_scores = vec![
            trust_score("peer-reducer-only", true, false, false),
            trust_score("peer-quarantined", false, false, true),
        ];

        let validator_peers = runtime_validator_peers(&snapshot, &PeerId::new("local"));
        let validators = runtime_validators(
            &PeerRoleSet::new([PeerRole::Validator]),
            &PeerId::new("local"),
            &validator_peers,
            2,
        );

        assert!(validator_peers.contains(&PeerId::new("local")));
        assert!(validator_peers.contains(&PeerId::new("peer-good")));
        assert!(!validator_peers.contains(&PeerId::new("peer-reducer-only")));
        assert!(!validator_peers.contains(&PeerId::new("peer-quarantined")));
        assert_eq!(
            validators,
            vec![PeerId::new("local"), PeerId::new("peer-good")]
        );
    }
}
