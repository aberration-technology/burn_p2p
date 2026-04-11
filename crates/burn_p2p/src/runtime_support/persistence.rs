use super::*;
use crate::config::{TrustBundleState, default_node_runtime_state};
use serde::de::DeserializeOwned;

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PersistedControlPlaneState {
    pub control_announcements: Vec<ControlAnnouncement>,
    #[serde(default)]
    pub lifecycle_announcements: Vec<ExperimentLifecycleAnnouncement>,
    pub lease_announcements: Vec<LeaseAnnouncement>,
    pub auth_announcements: Vec<PeerAuthAnnouncement>,
    pub directory_announcements: Vec<ExperimentDirectoryAnnouncement>,
    #[serde(default)]
    pub peer_directory_announcements: Vec<PeerDirectoryAnnouncement>,
    #[serde(default)]
    pub metrics_announcements: Vec<MetricsAnnouncement>,
}

impl PersistedControlPlaneState {
    fn from_snapshot(snapshot: &ControlPlaneSnapshot) -> Self {
        let mut bounded_snapshot = snapshot.clone();
        bounded_snapshot.clamp_announcement_histories();
        Self {
            control_announcements: bounded_snapshot.control_announcements,
            lifecycle_announcements: bounded_snapshot.lifecycle_announcements,
            lease_announcements: bounded_snapshot.lease_announcements,
            auth_announcements: bounded_snapshot.auth_announcements,
            directory_announcements: bounded_snapshot.directory_announcements,
            peer_directory_announcements: bounded_snapshot.peer_directory_announcements,
            metrics_announcements: bounded_snapshot.metrics_announcements,
        }
    }

    fn apply_to_snapshot(self, snapshot: &mut ControlPlaneSnapshot) {
        snapshot.control_announcements = self.control_announcements;
        snapshot.lifecycle_announcements = self.lifecycle_announcements;
        snapshot.lease_announcements = self.lease_announcements;
        snapshot.auth_announcements = self.auth_announcements;
        snapshot.directory_announcements = self.directory_announcements;
        snapshot.peer_directory_announcements = self.peer_directory_announcements;
        snapshot.metrics_announcements = self.metrics_announcements;
        snapshot.clamp_announcement_histories();
    }

    fn apply_to_shell(self, shell: &mut ControlPlaneShell) {
        for announcement in self.control_announcements {
            shell.publish_control(announcement);
        }
        for announcement in self.lifecycle_announcements {
            shell.publish_lifecycle(announcement);
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
        for announcement in self.peer_directory_announcements {
            shell.publish_peer_directory(announcement);
        }
        for announcement in self.metrics_announcements {
            shell.publish_metrics(announcement);
        }
    }

    fn is_empty(&self) -> bool {
        self.control_announcements.is_empty()
            && self.lifecycle_announcements.is_empty()
            && self.lease_announcements.is_empty()
            && self.auth_announcements.is_empty()
            && self.directory_announcements.is_empty()
            && self.peer_directory_announcements.is_empty()
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

pub(super) fn persist_local_peer_auth(
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

pub(super) fn seed_shell_control_plane_state(
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

pub(super) fn sync_control_plane_snapshot(
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

pub(crate) fn inferred_next_window_id(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    current_head: Option<&HeadDescriptor>,
) -> anyhow::Result<WindowId> {
    let persisted = next_window_id(storage, experiment)?;
    let inferred_from_head = current_head
        .and_then(|head| head.global_step.checked_add(1))
        .map(WindowId)
        .unwrap_or(WindowId(1));
    Ok(std::cmp::max(persisted, inferred_from_head))
}
