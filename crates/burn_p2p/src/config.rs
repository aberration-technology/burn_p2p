use super::*;

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IdentityConfig {
    #[default]
    Ephemeral,
    Persistent,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StorageConfig {
    pub root: PathBuf,
}

impl StorageConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn state_dir(&self) -> PathBuf {
        self.root.join("state")
    }

    pub fn artifacts_dir(&self) -> PathBuf {
        self.root.join("artifacts")
    }

    pub fn manifests_dir(&self) -> PathBuf {
        self.artifacts_dir().join("manifests")
    }

    pub fn chunks_dir(&self) -> PathBuf {
        self.artifacts_dir().join("chunks")
    }

    pub fn pins_dir(&self) -> PathBuf {
        self.artifacts_dir().join("pins")
    }

    pub fn receipts_dir(&self) -> PathBuf {
        self.root.join("receipts")
    }

    pub fn leases_dir(&self) -> PathBuf {
        self.root.join("leases")
    }

    pub fn auth_dir(&self) -> PathBuf {
        self.root.join("auth")
    }

    pub fn transfers_dir(&self) -> PathBuf {
        self.state_dir().join("transfers")
    }

    pub fn heads_dir(&self) -> PathBuf {
        self.root.join("heads")
    }

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
}

impl From<PathBuf> for StorageConfig {
    fn from(value: PathBuf) -> Self {
        Self::new(value)
    }
}

impl From<&str> for StorageConfig {
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
pub struct DatasetConfig {
    pub upstream: UpstreamAdapter,
}

impl DatasetConfig {
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
pub struct AuthConfig {
    pub admission_policy: Option<AdmissionPolicy>,
    pub local_peer_auth: Option<PeerAuthEnvelope>,
    #[serde(default)]
    pub trust_bundle_endpoints: Vec<String>,
    pub experiment_directory: Vec<ExperimentDirectoryEntry>,
}

impl AuthConfig {
    pub fn new() -> Self {
        Self {
            admission_policy: None,
            local_peer_auth: None,
            trust_bundle_endpoints: Vec::new(),
            experiment_directory: Vec::new(),
        }
    }

    pub fn with_admission_policy(mut self, admission_policy: AdmissionPolicy) -> Self {
        self.admission_policy = Some(admission_policy);
        self
    }

    pub fn with_local_peer_auth(mut self, local_peer_auth: PeerAuthEnvelope) -> Self {
        self.local_peer_auth = Some(local_peer_auth);
        self
    }

    pub fn with_trust_bundle_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.trust_bundle_endpoints.push(endpoint.into());
        self
    }

    pub fn with_trust_bundle_endpoints(
        mut self,
        endpoints: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.trust_bundle_endpoints = endpoints.into_iter().map(Into::into).collect();
        self
    }

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

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeConfig {
    pub identity: IdentityConfig,
    pub storage: Option<StorageConfig>,
    pub dataset: Option<DatasetConfig>,
    pub auth: Option<AuthConfig>,
    pub network_manifest: Option<NetworkManifest>,
    pub client_release_manifest: Option<ClientReleaseManifest>,
    pub selected_workload_id: Option<WorkloadId>,
    pub bootstrap_peers: Vec<SwarmAddress>,
    pub listen_addresses: Vec<SwarmAddress>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RuntimeStatus {
    Starting,
    Running,
    ShutdownRequested,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NodeRuntimeState {
    #[default]
    Starting,
    Connecting,
    Admitting,
    DirectorySync,
    HeadSync,
    IdleReady,
    LeasePending,
    TrainingWindow,
    PublishingUpdate,
    WaitingMerge,
    PassiveValidator,
    PassiveArchive,
    Quarantined,
    Revoked,
    ShuttingDown,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SlotAssignmentState {
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
}

impl SlotAssignmentState {
    pub fn new(study_id: StudyId, experiment_id: ExperimentId, revision_id: RevisionId) -> Self {
        Self {
            study_id,
            experiment_id,
            revision_id,
        }
    }

    pub fn from_experiment(experiment: &ExperimentHandle) -> Self {
        Self::new(
            experiment.study_id.clone(),
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SlotRuntimeState {
    #[default]
    Unassigned,
    Assigned(SlotAssignmentState),
    MaterializingBase(SlotAssignmentState),
    FetchingShards(SlotAssignmentState),
    Training(SlotAssignmentState),
    Publishing(SlotAssignmentState),
    CoolingDown(SlotAssignmentState),
    Migrating(SlotAssignmentState),
    Blocked {
        assignment: Option<SlotAssignmentState>,
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
pub enum ArtifactTransferPhase {
    LocatingProvider,
    FetchingChunks,
    Finalizing,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactTransferState {
    pub artifact_id: ArtifactId,
    #[serde(default)]
    pub source_peers: Vec<PeerId>,
    #[serde(default)]
    pub provider_peer_id: Option<PeerId>,
    #[serde(default)]
    pub descriptor: Option<ArtifactDescriptor>,
    #[serde(default)]
    pub completed_chunks: BTreeSet<ChunkId>,
    pub phase: ArtifactTransferPhase,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ArtifactTransferState {
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
pub struct ClientReenrollmentStatus {
    pub reason: String,
    pub rotated_at: Option<DateTime<Utc>>,
    pub legacy_issuer_peer_ids: BTreeSet<PeerId>,
    pub login_path: String,
    pub enroll_path: String,
    pub trust_bundle_path: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TrustBundleState {
    pub source_url: String,
    pub required_release_train_hash: ContentId,
    #[serde(default)]
    pub allowed_target_artifact_hashes: BTreeSet<ContentId>,
    pub active_issuer_peer_id: PeerId,
    pub trusted_issuers: BTreeMap<PeerId, TrustedIssuer>,
    pub minimum_revocation_epoch: RevocationEpoch,
    pub reenrollment: Option<ClientReenrollmentStatus>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeTelemetrySnapshot {
    pub status: RuntimeStatus,
    pub node_state: NodeRuntimeState,
    pub slot_states: Vec<SlotRuntimeState>,
    pub lag_state: LagState,
    pub head_lag_steps: u64,
    pub lag_policy: LagPolicy,
    pub network_id: Option<NetworkId>,
    pub local_peer_id: Option<PeerId>,
    pub configured_roles: PeerRoleSet,
    pub connected_peers: usize,
    pub observed_peer_ids: BTreeSet<PeerId>,
    pub known_peer_addresses: BTreeSet<SwarmAddress>,
    pub runtime_boundary: Option<RuntimeBoundary>,
    pub listen_addresses: Vec<SwarmAddress>,
    pub control_plane: ControlPlaneSnapshot,
    pub recent_events: Vec<LiveControlPlaneEvent>,
    pub last_snapshot_peer_id: Option<PeerId>,
    pub last_snapshot: Option<ControlPlaneSnapshot>,
    #[serde(default)]
    pub admitted_peers: BTreeMap<PeerId, PeerAdmissionReport>,
    #[serde(default)]
    pub rejected_peers: BTreeMap<PeerId, String>,
    #[serde(default)]
    pub peer_reputation: BTreeMap<PeerId, ReputationState>,
    #[serde(default)]
    pub minimum_revocation_epoch: Option<RevocationEpoch>,
    #[serde(default)]
    pub trust_bundle: Option<TrustBundleState>,
    #[serde(default)]
    pub in_flight_transfers: BTreeMap<ArtifactId, ArtifactTransferState>,
    pub effective_limit_profile: Option<LimitProfile>,
    pub last_error: Option<String>,
    pub started_at: DateTime<Utc>,
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
            effective_limit_profile: None,
            last_error: None,
            started_at: now,
            updated_at: now,
        }
    }

    pub(crate) fn push_event(&mut self, event: LiveControlPlaneEvent) {
        if let Some(peer_id) = peer_id_from_event(&event) {
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
pub struct TelemetryHandle {
    pub(crate) state: Arc<Mutex<NodeTelemetrySnapshot>>,
}

impl fmt::Debug for TelemetryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TelemetryHandle").finish_non_exhaustive()
    }
}

impl TelemetryHandle {
    pub fn snapshot(&self) -> NodeTelemetrySnapshot {
        self.state
            .lock()
            .expect("telemetry state lock should not be poisoned")
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
    RequestSnapshot {
        peer_id: String,
    },
    Shutdown,
}

#[derive(Clone)]
pub struct ControlHandle {
    pub(crate) tx: mpsc::Sender<RuntimeCommand>,
}

impl fmt::Debug for ControlHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlHandle").finish_non_exhaustive()
    }
}

impl ControlHandle {
    pub fn subscribe_topic(&self, topic: OverlayTopic) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::SubscribeTopic(topic))
            .map_err(|error| anyhow::anyhow!("failed to subscribe topic: {error}"))
    }

    pub fn publish_control(&self, announcement: ControlAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishControl(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send control announcement: {error}"))
    }

    pub fn publish_head(&self, announcement: HeadAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishHead(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send head announcement: {error}"))
    }

    pub fn publish_lease(&self, announcement: LeaseAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishLease(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send lease announcement: {error}"))
    }

    pub fn publish_merge(&self, announcement: MergeAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishMerge(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send merge announcement: {error}"))
    }

    pub fn publish_merge_window(
        &self,
        announcement: MergeWindowAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishMergeWindow(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send merge window announcement: {error}"))
    }

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

    pub fn publish_update(&self, announcement: UpdateEnvelopeAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishUpdate(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send update announcement: {error}"))
    }

    pub fn publish_aggregate(&self, announcement: AggregateAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishAggregate(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send aggregate announcement: {error}"))
    }

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

    pub fn publish_reducer_load(
        &self,
        announcement: ReducerLoadAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishReducerLoad(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send reducer load announcement: {error}"))
    }

    pub fn publish_auth(&self, announcement: PeerAuthAnnouncement) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishAuth(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send auth announcement: {error}"))
    }

    pub fn publish_directory(
        &self,
        announcement: ExperimentDirectoryAnnouncement,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishDirectory(announcement))
            .map_err(|error| anyhow::anyhow!("failed to send directory announcement: {error}"))
    }

    pub fn request_snapshot(&self, peer_id: impl Into<String>) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::RequestSnapshot {
                peer_id: peer_id.into(),
            })
            .map_err(|error| anyhow::anyhow!("failed to request snapshot: {error}"))
    }

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

    pub fn publish_artifact(
        &self,
        descriptor: ArtifactDescriptor,
        chunks: Vec<ArtifactChunkPayload>,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::PublishArtifact { descriptor, chunks })
            .map_err(|error| anyhow::anyhow!("failed to publish artifact: {error}"))
    }

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

    pub fn shutdown(&self) -> anyhow::Result<()> {
        match self.tx.send(RuntimeCommand::Shutdown) {
            Ok(()) => Ok(()),
            Err(_) => Ok(()),
        }
    }
}
