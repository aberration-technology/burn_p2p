use super::*;

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
    pub retired_issuer_peer_ids: BTreeSet<PeerId>,
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

fn peer_id_from_event(event: &LiveControlPlaneEvent) -> Option<PeerId> {
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
        | LiveControlPlaneEvent::PeerIdentified { peer_id, .. }
        | LiveControlPlaneEvent::DirectConnectionUpgradeSucceeded { peer_id }
        | LiveControlPlaneEvent::DirectConnectionUpgradeFailed { peer_id, .. } => {
            Some(PeerId::new(peer_id.clone()))
        }
        LiveControlPlaneEvent::RelayReservationAccepted { relay_peer_id } => {
            Some(PeerId::new(relay_peer_id.clone()))
        }
        LiveControlPlaneEvent::ConnectionClosed { .. } => None,
        LiveControlPlaneEvent::PeersDiscovered { peers }
        | LiveControlPlaneEvent::PeersExpired { peers } => peers
            .first()
            .map(|(peer_id, _)| PeerId::new(peer_id.clone())),
        LiveControlPlaneEvent::PeerDirectoryRecordReceived { announcement } => {
            Some(announcement.peer_id.clone())
        }
        LiveControlPlaneEvent::NewListenAddr { .. }
        | LiveControlPlaneEvent::ReachableAddressConfirmed { .. }
        | LiveControlPlaneEvent::ReachableAddressExpired { .. }
        | LiveControlPlaneEvent::TopicSubscribed { .. }
        | LiveControlPlaneEvent::OutgoingConnectionError { .. }
        | LiveControlPlaneEvent::IncomingConnectionError { .. }
        | LiveControlPlaneEvent::Other { .. } => None,
    }
}
