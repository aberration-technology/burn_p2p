//! Public facade for the `burn_p2p` runtime.
//!
//! This crate is the downstream entry point for native Burn applications. It exposes:
//!
//! - the project-family and workload integration traits
//! - node configuration and builder APIs
//! - stable identifiers, manifests, and runtime handles re-exported from the core crates
//!
//! The intended forward path is:
//!
//! 1. define one or more workloads through [`P2pWorkload`]
//! 2. group them under a [`P2pProjectFamily`]
//! 3. build a node with [`NodeBuilder`]
//! 4. bind the node to a network, workload, storage root, and auth/enrollment flow
//!
#![forbid(unsafe_code)]

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    fmt, fs,
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use libp2p_identity::Keypair;
use semver::Version;
use serde::{Deserialize, Serialize};

mod app_view;
mod backend;
mod browser_join;
mod config;
mod edge_auth;
mod handles;
mod metrics_runtime;
mod project_family;
mod runtime_support;
mod training;
mod validation;

use handles::dedupe_peer_ids;

pub use app_view::{NodeAppSelection, build_node_app_view};
pub use backend::{
    ContinuousTrainerPolicy, EvalSplit, MergeModelCandidate, MetricReport, PatchOutcome,
    ReducerOutcome, TrainError, TrainerCanonicalReconcileStrategy, TrainingWindowOutcome,
    ValidationCoordinationState, ValidationDriveOutcome, ValidationOutcome, WindowCtx,
    WindowReport,
};
pub use browser_join::{BrowserJoinPolicy, browser_join_policy_for_entry};
pub use burn_p2p_checkpoint::{
    AggregateArtifactBytes, AggregateArtifactInput, AggregateArtifactRecord, ArtifactBuildSpec,
    CheckpointCatalog, CheckpointError, CheckpointLineage, ChunkingScheme, EmaFlow,
    FsArtifactStore, GarbageCollectionReport, MergeCandidate, MergePlan, SyncPlan, SyncRequest,
    materialize_aggregate_artifact_bytes,
};
pub use burn_p2p_core::{
    ActiveServiceSet, AdminMode, AggregateEnvelope, AggregateStats, AggregateTier, AppMode,
    ArtifactDescriptor, ArtifactId, ArtifactKind, ArtifactTargetKind, AssignmentLease,
    AuthProvider, BackpressurePolicy, BadgeAward, BadgeKind, BrowserCapability,
    BrowserEdgeSnapshot, BrowserLoginProvider, BrowserMode, BrowserRole, BrowserRolePolicy,
    BrowserVisibilityPolicy, CanaryEvalReport, CapabilityCard, CapabilityEstimate, ChunkId,
    ClientPlatform, ClientReleaseManifest, CohortRobustnessReport, CompiledFeatureSet,
    ConfiguredServiceSet, ContentId, ContributionReceipt, ContributionReceiptId,
    ContributionRollup, DatasetId, DatasetManifest, DatasetView, DatasetViewId, EdgeAuthProvider,
    EdgeFeature, EdgeServiceManifest, EvalAggregationRule, EvalMetricDef, EvalProtocolManifest,
    ExperimentDirectoryEntry, ExperimentId, ExperimentManifest, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, GenesisSpec,
    HeadDescriptor, HeadEvalReport, HeadEvalStatus, HeadId, HeadPromotionPolicy,
    IdentityVisibility, LagPolicy, LagState, LeaderboardEntry, LeaderboardIdentity,
    LeaderboardSnapshot, LeaseId, MergeCertId, MergeCertificate, MergePolicy, MergeStrategy,
    MergeTopologyPolicy, MergeWindowMissPolicy, MergeWindowState, MetricScope, MetricTrustClass,
    MetricValue, MetricsLedgerSegment, MetricsMode, MetricsSnapshotManifest, MicroShardId,
    NetworkEstimate, NetworkId, NetworkManifest, NodeCertId, NodeCertificate,
    NodeCertificateClaims, PeerAuthEnvelope, PeerId, PeerRole, PeerRoleSet, PeerWindowMetrics,
    PeerWindowStatus, Precision, PrincipalId, ProfileMode, ProjectFamilyId, ReducerAssignment,
    ReducerCohortMetrics, ReducerCohortStatus, ReducerLoadReport, ReductionCertificate,
    RejectionReason, ReleaseTrainManifest, RevisionId, RevisionManifest, RevocationEpoch,
    RobustnessAlert, RobustnessDecision, RobustnessPolicy, RobustnessPreset, SocialMode,
    SocialProfile, StudyId, SupportedWorkload, TargetArtifactManifest, TelemetrySummary,
    TrustScore, UpdateAnnounce, UpdateFeatureSketch, UpdateNormStats, ValidationQuorumCertificate,
    WindowActivation, WindowId, WorkloadId,
};
pub use burn_p2p_dataloader::{
    BurnDataLoaderAdapter, CachedMicroShard, CachedMicroShardLoader, DataReceiptBuilder,
    DataloaderError, DatasetRegistration, DatasetSizing, LeaseCache, LeasePlanner,
    LeasePlannerConfig, LeaseSelection, MicroShardPlan, MicroShardPlanner, MicroShardPlannerConfig,
    PlannedLease, ShardAwareSampler, ShardCache, ShardCostModel, ShardFetchEntry,
    ShardFetchManifest, UpstreamAdapter,
};
pub use burn_p2p_experiment::{
    ExperimentControlCommand, ExperimentDirectory, ExperimentDirectoryAccess,
    ExperimentDirectoryPolicyExt, ExperimentSpec, PatchClass, PatchSupport, PatchValue,
    RevisionCompatibility, RevisionSpec, RuntimePatch, StudySpec,
};
pub use burn_p2p_limits::{
    CapabilityCalibrator, CapabilityProbe, LimitPolicy, LimitProfile, LimitsError, LocalBackend,
    ObservedThroughputUpdate, WorkBudget, corrected_work_units_per_second, probe_from_profile,
};
pub use burn_p2p_security::{
    AdmissionDecision, AdmissionPolicy, AuditFinding, AuthError, CallbackPayload,
    ChallengeResponse, ClientManifest, DataAuditReport, IdentityConnector, LoginRequest,
    LoginStart, MergeEvidence, MergeEvidenceDecision, MergeEvidenceRequirement,
    NodeCertificateAuthority, NodeEnrollmentRequest, PeerAdmissionReport, PeerTrustLevel,
    PrincipalClaims, PrincipalSession, ReleaseManifest, ReleasePolicy, ReputationDecision,
    ReputationEngine, ReputationObservation, ReputationPolicy, ReputationState, SecurityError,
    StaticIdentityConnector, StaticPrincipalRecord, TrustedIssuer, UpdateAuditReport,
    ValidatorPolicy, create_peer_auth_envelope,
};
pub use burn_p2p_swarm::{
    AggregateProposalAnnouncement, AlertNotice, AlertSeverity, ArtifactChunkPayload,
    ArtifactProviderRecord, ArtifactSyncRequest, ArtifactSyncResponse, ChunkFetchRequest,
    ChunkFetchResponse, ControlAnnouncement, ControlPlaneRequest, ControlPlaneResponse,
    ControlPlaneShell, ControlPlaneSnapshot, ExperimentDirectoryAnnouncement, ExperimentOverlaySet,
    HeadAnnouncement, LeaseAnnouncement, LiveControlPlaneEvent, LiveSwarmEvent,
    MemoryControlPlaneShell, MemorySwarmShell, MergeAnnouncement, MergeWindowAnnouncement,
    MetricsAnnouncement, MicroShardFetchRequest, MicroShardFetchResponse, MicroShardProviderRecord,
    MigrationCoordinator, MigrationPlan, NativeControlPlaneShell, OverlayChannel, OverlayTopic,
    PeerAuthAnnouncement, PeerDirectoryAnnouncement, PeerObservation, PeerStore, ProtocolId,
    ProtocolSet, ProviderPointer, PubsubPayload, ReducerAssignmentAnnouncement,
    ReducerLoadAnnouncement, ReductionCertificateAnnouncement, RuntimeBoundary, RuntimeEnvironment,
    RuntimeTransportPolicy, SwarmAddress, SwarmError, SwarmStats, TelemetryAnnouncement,
    TransportKind, UpdateEnvelopeAnnouncement, ValidationQuorumAnnouncement,
};
pub use config::{
    ArtifactTransferPhase, ArtifactTransferState, AuthConfig, ClientReenrollmentStatus,
    ControlHandle, DatasetConfig, IdentityConfig, MetricsRetentionBudget, MetricsRetentionConfig,
    MetricsRetentionPreset, NodeConfig, NodeRuntimeState, NodeTelemetrySnapshot, RuntimeStatus,
    SlotAssignmentState, SlotRuntimeState, StorageConfig, TelemetryHandle, TrustBundleState,
};
pub use edge_auth::{
    EdgeAuthClient, EdgeAuthClientError, EdgeEnrollmentConfig, EdgeEnrollmentResult,
    EdgeLogoutResponse, EdgePeerEnrollmentRequest, EdgePeerIdentity, EdgeSessionState,
};
pub use handles::{
    CheckpointSyncHandle, ExperimentHandle, ExperimentOverlayTopics, MainnetHandle, RoleSet,
};
pub use project_family::{
    P2pProjectFamily, P2pWorkload, SelectedWorkloadProject, SingleWorkloadProjectFamily,
};
use runtime_support::{
    assess_head_lag, cached_connected_snapshots, connected_peer_ids, effective_limit_profile,
    inferred_next_window_id, latest_head_from_snapshot,
    latest_merge_window_from_connected_snapshots, latest_merge_window_from_snapshot,
    latest_reducer_assignment_from_snapshot, load_head_state, load_known_peers,
    load_latest_merge_certificate, load_primary_slot_assignment, lock_telemetry_state,
    matches_experiment_head, merge_control_plane_snapshot, metric_quality,
    open_runtime_merge_window, persist_auth_state, persist_control_plane_state, persist_head_state,
    persist_in_flight_transfer_states, persist_json, persist_limit_profile,
    persist_primary_slot_assignment, persist_runtime_binding_state, persist_runtime_security_state,
    persist_window_id, prioritized_experiment_snapshot_peer_ids, remove_artifact_transfer_state,
    resolve_canonical_head, resolve_identity, restore_auth_config, restore_control_plane_state,
    restore_in_flight_transfer_states, restore_runtime_binding_config,
    restore_runtime_security_state, run_control_plane, runtime_assign_reducers,
    runtime_limit_policy, runtime_merge_topology_policy, runtime_robustness_policy,
    runtime_topology_peers, runtime_validator_peers, runtime_validators,
    set_effective_limit_profile, snapshots_with_local_control_plane,
    update_announces_from_connected_snapshots, update_feature_sketch_from_metrics,
    update_norm_stats, verify_snapshot_admission,
};
pub use training::ContinuousTrainer;
#[derive(Clone, Debug)]
/// Staged builder for a native `burn_p2p` node.
///
/// The builder is the canonical entry point for downstream Burn applications:
/// attach the selected workload family, runtime identity, storage layout,
/// release metadata, and enrollment configuration, then call [`Self::prepare`]
/// or [`Self::spawn`].
pub struct NodeBuilder<P> {
    project: P,
    genesis: Option<GenesisSpec>,
    roles: PeerRoleSet,
    config: NodeConfig,
}

impl<P> NodeBuilder<P> {
    /// Creates a builder around the selected project or workload family.
    pub fn new(project: P) -> Self {
        Self {
            project,
            genesis: None,
            roles: PeerRoleSet::default_trainer(),
            config: NodeConfig::default(),
        }
    }

    /// Pins the builder to a specific network genesis.
    pub fn with_mainnet(mut self, genesis: GenesisSpec) -> Self {
        self.genesis = Some(genesis);
        self
    }

    /// Sets the local peer roles advertised by the node.
    pub fn with_roles(mut self, roles: PeerRoleSet) -> Self {
        self.roles = roles;
        self
    }

    /// Sets the local identity source used for libp2p and certificate flow.
    pub fn with_identity(mut self, identity: IdentityConfig) -> Self {
        self.config.identity = identity;
        self
    }

    /// Sets the storage root and persistence layout for the node.
    pub fn with_storage(mut self, storage: impl Into<StorageConfig>) -> Self {
        self.config.storage = Some(storage.into());
        self
    }

    /// Sets the dataset registration used for shard planning and fetch.
    pub fn with_dataset(mut self, dataset: impl Into<DatasetConfig>) -> Self {
        self.config.dataset = Some(dataset.into());
        self
    }

    /// Sets the enrollment and session configuration for certificate admission.
    pub fn with_auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Sets the raw metrics retention policy used by this node.
    pub fn with_metrics_retention(mut self, metrics_retention: MetricsRetentionConfig) -> Self {
        self.config.metrics_retention = metrics_retention;
        self
    }

    /// Adds one bootstrap peer to the initial dial set.
    pub fn with_bootstrap_peer(mut self, peer: SwarmAddress) -> Self {
        self.config.bootstrap_peers.push(peer);
        self
    }

    /// Extends the initial bootstrap peer list.
    pub fn with_bootstrap_peers(mut self, peers: impl IntoIterator<Item = SwarmAddress>) -> Self {
        self.config.bootstrap_peers.extend(peers);
        self
    }

    /// Adds one local listen address for inbound swarm traffic.
    pub fn with_listen_address(mut self, address: SwarmAddress) -> Self {
        self.config.listen_addresses.push(address);
        self
    }

    /// Extends the local listen-address list.
    pub fn with_listen_addresses(
        mut self,
        addresses: impl IntoIterator<Item = SwarmAddress>,
    ) -> Self {
        self.config.listen_addresses.extend(addresses);
        self
    }

    /// Returns the current accumulated node configuration.
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// Validates config, restores persisted state, and returns a prepared node.
    ///
    /// This is useful when callers want to inspect the final prepared config or
    /// delay runtime startup until later.
    pub fn prepare(self) -> anyhow::Result<Node<P>> {
        let mut config = self.config;
        if let Some(storage) = config.storage.clone() {
            restore_runtime_binding_config(&storage, &mut config)?;
            config.auth = restore_auth_config(&storage, config.auth)?;
        }

        let genesis = self.genesis.or_else(|| {
            config
                .network_manifest
                .as_ref()
                .map(|network_manifest| GenesisSpec {
                    network_id: network_manifest.network_id.clone(),
                    protocol_version: Version::new(
                        u64::from(network_manifest.protocol_major),
                        0,
                        0,
                    ),
                    display_name: network_manifest.description.clone(),
                    created_at: network_manifest.created_at,
                    metadata: Default::default(),
                })
        });
        let genesis = genesis.ok_or_else(|| anyhow::anyhow!("missing genesis"))?;

        if let Some(network_manifest) = &config.network_manifest {
            if genesis.network_id != network_manifest.network_id {
                anyhow::bail!(
                    "genesis network {} does not match network manifest {}",
                    genesis.network_id.as_str(),
                    network_manifest.network_id.as_str(),
                );
            }

            if genesis.protocol_version.major != u64::from(network_manifest.protocol_major) {
                anyhow::bail!(
                    "genesis protocol major {} does not match network manifest {}",
                    genesis.protocol_version.major,
                    network_manifest.protocol_major,
                );
            }
        }

        if let Some(release_manifest) = &config.client_release_manifest {
            if let Some(network_manifest) = &config.network_manifest {
                if release_manifest.project_family_id != network_manifest.project_family_id {
                    anyhow::bail!(
                        "client release family {} does not match network family {}",
                        release_manifest.project_family_id.as_str(),
                        network_manifest.project_family_id.as_str(),
                    );
                }

                if release_manifest.release_train_hash
                    != network_manifest.required_release_train_hash
                {
                    anyhow::bail!(
                        "release train hash {} does not match network requirement {}",
                        release_manifest.release_train_hash.as_str(),
                        network_manifest.required_release_train_hash.as_str(),
                    );
                }

                if !network_manifest.allowed_target_artifact_hashes.is_empty()
                    && !network_manifest
                        .allowed_target_artifact_hashes
                        .contains(&release_manifest.target_artifact_hash)
                {
                    anyhow::bail!(
                        "target artifact hash {} is not allowed by network {}",
                        release_manifest.target_artifact_hash.as_str(),
                        network_manifest.network_id.as_str(),
                    );
                }

                if release_manifest.protocol_major != network_manifest.protocol_major {
                    anyhow::bail!(
                        "client release protocol major {} does not match network protocol major {}",
                        release_manifest.protocol_major,
                        network_manifest.protocol_major,
                    );
                }
            }

            if let Some(workload_id) = &config.selected_workload_id
                && !release_manifest
                    .supported_workloads
                    .iter()
                    .any(|workload| workload.workload_id == *workload_id)
            {
                anyhow::bail!(
                    "selected workload {} is not compiled into client release {}",
                    workload_id.as_str(),
                    release_manifest.target_artifact_hash.as_str(),
                );
            }
        }

        Ok(Node {
            project: self.project,
            mainnet: MainnetHandle {
                genesis,
                roles: self.roles,
            },
            config,
        })
    }

    /// Validates the builder and starts the native runtime thread immediately.
    pub fn spawn(self) -> anyhow::Result<RunningNode<P>> {
        RunningNode::spawn(self.prepare()?)
    }
}

/// Prepared but not yet running node.
///
/// A `Node` owns the selected project and validated configuration, but the
/// control plane and swarm threads have not been started yet.
pub struct Node<P> {
    project: P,
    mainnet: MainnetHandle,
    config: NodeConfig,
}

impl<P> Node<P> {
    /// Performs the mainnet operation.
    pub fn mainnet(&self) -> &MainnetHandle {
        &self.mainnet
    }

    /// Performs the checkpoint sync operation.
    pub fn checkpoint_sync(&self, target_head_id: HeadId) -> CheckpointSyncHandle {
        self.mainnet.checkpoint_sync(target_head_id)
    }

    /// Performs the experiment operation.
    pub fn experiment(
        &self,
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
    ) -> ExperimentHandle {
        self.mainnet
            .experiment(study_id, experiment_id, revision_id)
    }

    /// Consumes the value and returns the project.
    pub fn into_project(self) -> P {
        self.project
    }

    /// Performs the config operation.
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }
}

/// Handle to a live native runtime.
///
/// This wraps the prepared node together with the telemetry/control handles and
/// the background runtime thread.
pub struct RunningNode<P> {
    node: Option<Node<P>>,
    telemetry: TelemetryHandle,
    control: ControlHandle,
    training_prefetch: Option<TrainingPrefetchTask>,
    validation_cache: Option<Box<dyn Any + Send>>,
    runtime_thread: Option<JoinHandle<()>>,
}

struct TrainingPrefetchTask {
    planned_lease: PlannedLease,
    handle: Option<JoinHandle<()>>,
}

impl<P> RunningNode<P> {
    fn spawn(node: Node<P>) -> anyhow::Result<Self> {
        if let Some(storage) = node.config.storage.as_ref() {
            storage.ensure_layout()?;
            persist_auth_state(storage, node.config.auth.as_ref())?;
            persist_runtime_binding_state(storage, node.config())?;
        }

        let keypair = resolve_identity(&node.config.identity, node.config.storage.as_ref())?;

        let mut snapshot = NodeTelemetrySnapshot::starting(&node.mainnet, &node.config);
        let listen_addresses = if node.config.listen_addresses.is_empty() {
            vec![SwarmAddress::new("/ip4/127.0.0.1/tcp/0")?]
        } else {
            node.config.listen_addresses.clone()
        };
        let mut bootstrap_addresses = node.config.bootstrap_peers.clone();
        if let Some(storage) = node.config.storage.as_ref() {
            for address in load_known_peers(storage)? {
                if !bootstrap_addresses.contains(&address) {
                    bootstrap_addresses.push(address.clone());
                }
                snapshot.known_peer_addresses.insert(address);
            }
            if let Some(assignment) = load_primary_slot_assignment(storage)? {
                snapshot.set_primary_slot_state(SlotRuntimeState::Assigned(assignment));
            }
            restore_control_plane_state(storage, &mut snapshot)?;
            restore_runtime_security_state(storage, &mut snapshot)?;
            restore_in_flight_transfer_states(storage, &mut snapshot)?;
        }
        let boundary = RuntimeBoundary::for_platform_and_roles(
            &node.mainnet.genesis,
            burn_p2p_core::ClientPlatform::Native,
            &node.mainnet.roles,
            bootstrap_addresses,
            listen_addresses.clone(),
        )?;
        snapshot.runtime_boundary = Some(boundary.clone());
        snapshot.listen_addresses = Vec::new();

        let shared_state = Arc::new(Mutex::new(snapshot));
        if let Some(storage) = node.config.storage.as_ref() {
            let snapshot = shared_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            persist_runtime_security_state(storage, &snapshot)?;
        }
        let telemetry = TelemetryHandle {
            state: Arc::clone(&shared_state),
        };
        let (command_tx, command_rx) = mpsc::channel();
        let control = ControlHandle {
            tx: command_tx,
            telemetry: telemetry.clone(),
            runtime_boundary: boundary.clone(),
        };
        let state_for_thread = Arc::clone(&shared_state);
        let auth = node.config.auth.clone();
        let storage = node.config.storage.clone();

        let runtime_thread = thread::Builder::new()
            .name("burn-p2p-runtime".into())
            .spawn(move || {
                run_control_plane(
                    boundary,
                    keypair,
                    storage,
                    auth,
                    command_rx,
                    state_for_thread,
                )
            })
            .map_err(|error| anyhow::anyhow!("failed to spawn runtime thread: {error}"))?;

        Ok(Self {
            node: Some(node),
            telemetry,
            control,
            training_prefetch: None,
            validation_cache: None,
            runtime_thread: Some(runtime_thread),
        })
    }

    /// Performs the mainnet operation.
    pub fn mainnet(&self) -> &MainnetHandle {
        &self
            .node
            .as_ref()
            .expect("running node should retain prepared node")
            .mainnet
    }

    /// Performs the config operation.
    pub fn config(&self) -> &NodeConfig {
        &self
            .node
            .as_ref()
            .expect("running node should retain prepared node")
            .config
    }

    /// Performs the checkpoint sync operation.
    pub fn checkpoint_sync(&self, target_head_id: HeadId) -> CheckpointSyncHandle {
        self.mainnet().checkpoint_sync(target_head_id)
    }

    /// Performs the experiment operation.
    pub fn experiment(
        &self,
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
    ) -> ExperimentHandle {
        self.mainnet()
            .experiment(study_id, experiment_id, revision_id)
    }

    /// Performs the telemetry operation.
    pub fn telemetry(&self) -> TelemetryHandle {
        self.telemetry.clone()
    }

    fn update_runtime_state(
        &self,
        node_state: NodeRuntimeState,
        slot_state: Option<SlotRuntimeState>,
    ) {
        let mut snapshot = self
            .telemetry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let persist_security_state = node_state != NodeRuntimeState::ShuttingDown;
        snapshot.set_node_state(node_state);
        if let Some(slot_state) = slot_state {
            snapshot.set_primary_slot_state(slot_state);
        }
        if persist_security_state
            && let Some(storage) = self.config().storage.as_ref()
            && let Err(error) = persist_runtime_security_state(storage, &snapshot)
        {
            snapshot.last_error = Some(format!("failed to persist security state: {error}"));
        }
    }

    fn update_lag_status(&self, lag_state: LagState, head_lag_steps: u64, lag_policy: LagPolicy) {
        let mut snapshot = self
            .telemetry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.set_lag_status(lag_state, head_lag_steps, lag_policy);
        if let Some(storage) = self.config().storage.as_ref()
            && let Err(error) = persist_runtime_security_state(storage, &snapshot)
        {
            snapshot.last_error = Some(format!("failed to persist security state: {error}"));
        }
    }

    fn lag_policy(&self, experiment: &ExperimentHandle) -> LagPolicy {
        self.visible_experiment_entry(
            &experiment.study_id,
            &experiment.experiment_id,
            &experiment.revision_id,
        )
        .map(|entry| entry.lag_policy())
        .unwrap_or_default()
    }

    fn assess_and_record_lag(
        &self,
        storage: &StorageConfig,
        experiment: &ExperimentHandle,
        snapshots: &[(PeerId, ControlPlaneSnapshot)],
    ) -> anyhow::Result<runtime_support::LagAssessment> {
        let lag_policy = self.lag_policy(experiment);
        let assessment = assess_head_lag(storage, experiment, snapshots, &lag_policy)?;
        self.update_lag_status(assessment.state, assessment.head_lag_steps, lag_policy);
        Ok(assessment)
    }

    fn persist_transfer_snapshot(&self, snapshot: &mut NodeTelemetrySnapshot) {
        if let Some(storage) = self.config().storage.as_ref()
            && let Err(error) =
                persist_in_flight_transfer_states(storage, &snapshot.in_flight_transfers)
        {
            snapshot.last_error = Some(format!("failed to persist transfer state: {error}"));
        }
    }

    fn record_transfer_state(&self, transfer_state: ArtifactTransferState) {
        let mut snapshot = self
            .telemetry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.update_transfer_state(transfer_state);
        self.persist_transfer_snapshot(&mut snapshot);
    }

    fn clear_transfer_state(&self, artifact_id: &ArtifactId) {
        let mut snapshot = self
            .telemetry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.clear_transfer_state(artifact_id);
        self.persist_transfer_snapshot(&mut snapshot);
        if let Some(storage) = self.config().storage.as_ref()
            && let Err(error) = remove_artifact_transfer_state(storage, artifact_id)
        {
            snapshot.last_error = Some(format!("failed to clear transfer state: {error}"));
        }
    }

    fn redial_known_candidates(&self, candidates: &[PeerId]) {
        if candidates.is_empty() {
            return;
        }
        let telemetry_snapshot = self.telemetry().snapshot();
        let connected = connected_peer_ids(&telemetry_snapshot);
        let dial_targets = telemetry_snapshot
            .control_plane
            .peer_directory_announcements
            .iter()
            .filter(|announcement| candidates.contains(&announcement.peer_id))
            .filter(|announcement| !connected.contains(&announcement.peer_id))
            .flat_map(|announcement| announcement.addresses.iter().cloned())
            .filter(|address| !telemetry_snapshot.listen_addresses.contains(address))
            .collect::<BTreeSet<_>>();
        for address in dial_targets {
            let _ = self.control.dial_address(address);
        }
    }

    fn set_experiment_idle_state(
        &self,
        experiment: &ExperimentHandle,
        node_state: NodeRuntimeState,
    ) {
        self.update_runtime_state(
            node_state,
            Some(SlotRuntimeState::Assigned(
                SlotAssignmentState::from_experiment(experiment),
            )),
        );
    }

    /// Performs the control handle operation.
    pub fn control_handle(&self) -> ControlHandle {
        self.control.clone()
    }

    /// Performs the list experiments operation.
    pub fn list_experiments(&self) -> Vec<ExperimentDirectoryEntry> {
        let scopes = self
            .config()
            .auth
            .as_ref()
            .and_then(|auth| auth.local_peer_auth.as_ref())
            .map(|envelope| {
                envelope
                    .certificate
                    .claims()
                    .experiment_scopes
                    .iter()
                    .cloned()
                    .chain(envelope.requested_scopes.iter().cloned())
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();

        let mut directory = self
            .telemetry()
            .snapshot()
            .control_plane
            .directory_announcements
            .iter()
            .filter(|announcement| announcement.network_id == self.mainnet().genesis.network_id)
            .flat_map(|announcement| announcement.entries.clone())
            .collect::<Vec<_>>();

        if directory.is_empty()
            && let Some(auth) = &self.config().auth
        {
            directory = auth.experiment_directory.clone();
        }

        let directory = ExperimentDirectory {
            network_id: self.mainnet().genesis.network_id.clone(),
            generated_at: Utc::now(),
            entries: directory,
        };

        directory.visible_to(&scopes).into_iter().cloned().collect()
    }

    fn visible_experiment_entry(
        &self,
        study_id: &StudyId,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
    ) -> anyhow::Result<ExperimentDirectoryEntry> {
        self.list_experiments()
            .into_iter()
            .find(|entry| {
                entry.study_id == *study_id
                    && entry.experiment_id == *experiment_id
                    && entry.current_revision_id == *revision_id
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "experiment {} is not visible to the current node scope",
                    experiment_id.as_str()
                )
            })
    }

    fn persist_primary_assignment(&self, assignment: &SlotAssignmentState) -> anyhow::Result<()> {
        if let Some(storage) = self.config().storage.as_ref() {
            persist_primary_slot_assignment(storage, assignment)?;
        }
        Ok(())
    }

    /// Performs the select experiment operation.
    pub fn select_experiment(
        &self,
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
    ) -> anyhow::Result<ExperimentHandle> {
        let entry = self.visible_experiment_entry(&study_id, &experiment_id, &revision_id)?;
        Ok(self.experiment(
            entry.study_id,
            entry.experiment_id,
            entry.current_revision_id,
        ))
    }

    fn ensure_experiment_topics(&self, experiment: &ExperimentHandle) -> anyhow::Result<()> {
        let overlays = experiment.overlay_set()?;
        self.control.subscribe_topic(overlays.control.clone())?;
        for topic in overlays.experiment_topics() {
            self.control.subscribe_topic(topic)?;
        }
        Ok(())
    }

    fn fetch_experiment_snapshots(
        &self,
        experiment: &ExperimentHandle,
        timeout: Duration,
    ) -> anyhow::Result<Vec<(PeerId, ControlPlaneSnapshot)>> {
        let telemetry_snapshot = self.telemetry().snapshot();
        let peer_ids = prioritized_experiment_snapshot_peer_ids(&telemetry_snapshot, experiment);
        self.fetch_targeted_snapshots_with_cache(timeout, true, Some(peer_ids))
    }

    fn fetch_targeted_snapshots_with_cache(
        &self,
        timeout: Duration,
        include_cached: bool,
        target_peer_ids: Option<Vec<PeerId>>,
    ) -> anyhow::Result<Vec<(PeerId, ControlPlaneSnapshot)>> {
        const SNAPSHOT_FETCH_PARALLELISM: usize = 4;

        let admission_policy = self.effective_admission_policy();
        let telemetry_snapshot = self.telemetry().snapshot();
        let mut snapshots = if include_cached {
            cached_connected_snapshots(&telemetry_snapshot)
                .into_iter()
                .collect::<BTreeMap<_, _>>()
        } else {
            BTreeMap::new()
        };
        let mut connected_peers = connected_peer_ids(&telemetry_snapshot)
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(target_peer_ids) = target_peer_ids {
            connected_peers = dedupe_peer_ids(target_peer_ids.into_iter().chain(connected_peers));
        }
        if connected_peers.is_empty() {
            return Ok(snapshots.into_iter().collect());
        }

        let worker_count = connected_peers.len().clamp(1, SNAPSHOT_FETCH_PARALLELISM);
        let chunk_size = connected_peers.len().div_ceil(worker_count);
        let mut workers = Vec::with_capacity(worker_count);
        for chunk in connected_peers.chunks(chunk_size) {
            let control = self.control.clone();
            let admission_policy = admission_policy.clone();
            let peers = chunk.to_vec();
            workers.push(thread::spawn(
                move || -> BTreeMap<PeerId, ControlPlaneSnapshot> {
                    let mut fetched = BTreeMap::new();
                    for peer_id in peers {
                        let Ok(snapshot) = control.fetch_snapshot(peer_id.as_str(), timeout) else {
                            continue;
                        };
                        if let Some(policy) = admission_policy.as_ref() {
                            let Ok(report) = verify_snapshot_admission(policy, &peer_id, &snapshot)
                            else {
                                continue;
                            };
                            if !matches!(report.decision(), AdmissionDecision::Allow) {
                                continue;
                            }
                        }
                        fetched.insert(peer_id, snapshot);
                    }
                    fetched
                },
            ));
        }

        for worker in workers {
            let fetched = worker
                .join()
                .map_err(|_| anyhow::anyhow!("snapshot fetch worker panicked"))?;
            snapshots.extend(fetched);
        }

        Ok(snapshots.into_iter().collect())
    }

    /// Performs the artifact store operation.
    pub fn artifact_store(&self) -> Option<FsArtifactStore> {
        self.config()
            .storage
            .as_ref()
            .map(|storage| FsArtifactStore::new(storage.root.clone()))
    }

    /// Performs the publish artifact from store operation.
    pub fn publish_artifact_from_store(
        &self,
        artifact_id: &ArtifactId,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let store = self
            .artifact_store()
            .ok_or_else(|| anyhow::anyhow!("artifact publishing requires configured storage"))?;
        let descriptor = store.load_manifest(artifact_id)?;
        let chunks = descriptor
            .chunks
            .iter()
            .map(|chunk| {
                Ok(ArtifactChunkPayload {
                    artifact_id: descriptor.artifact_id.clone(),
                    chunk: chunk.clone(),
                    bytes: store.load_chunk_bytes(chunk)?,
                    generated_at: Utc::now(),
                })
            })
            .collect::<Result<Vec<_>, CheckpointError>>()?;
        self.control.publish_artifact(descriptor.clone(), chunks)?;
        Ok(descriptor)
    }

    /// Synchronizes the artifact from peer.
    pub fn sync_artifact_from_peer(
        &self,
        peer_id: &PeerId,
        artifact_id: ArtifactId,
    ) -> anyhow::Result<ArtifactDescriptor> {
        const ARTIFACT_PROVIDER_LOCATE_TIMEOUT: Duration = Duration::from_secs(20);
        const ARTIFACT_CHUNK_FETCH_TIMEOUT: Duration = Duration::from_secs(20);
        const ARTIFACT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

        self.sync_artifact_from_peer_with_timeouts(
            peer_id,
            artifact_id,
            ARTIFACT_PROVIDER_LOCATE_TIMEOUT,
            ARTIFACT_CHUNK_FETCH_TIMEOUT,
            ARTIFACT_REQUEST_TIMEOUT,
        )
    }

    /// Synchronizes the artifact from peer with bounded per-attempt timeouts.
    pub fn sync_artifact_from_peer_bounded(
        &self,
        peer_id: &PeerId,
        artifact_id: ArtifactId,
        timeout: Duration,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let budget = timeout.max(Duration::from_secs(1));
        let request_timeout = budget.min(Duration::from_secs(2));

        self.sync_artifact_from_peer_with_timeouts(
            peer_id,
            artifact_id,
            budget,
            budget,
            request_timeout,
        )
    }

    fn sync_artifact_from_peer_with_timeouts(
        &self,
        peer_id: &PeerId,
        artifact_id: ArtifactId,
        artifact_provider_locate_timeout: Duration,
        artifact_chunk_fetch_timeout: Duration,
        artifact_request_timeout: Duration,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let admission_policy = self.effective_admission_policy();
        let mut admitted_provider_snapshot = None;
        if let Some(policy) = admission_policy.as_ref() {
            let snapshot = self
                .control
                .fetch_snapshot(peer_id.as_str(), artifact_request_timeout)?;
            let report = verify_snapshot_admission(policy, peer_id, &snapshot)?;
            if !matches!(report.decision(), AdmissionDecision::Allow) {
                return Err(anyhow::anyhow!(
                    "peer {} is not admitted for artifact sync",
                    peer_id.as_str()
                ));
            }
            admitted_provider_snapshot = Some(snapshot);
        }

        let store = self
            .artifact_store()
            .ok_or_else(|| anyhow::anyhow!("artifact sync requires configured storage"))?;
        store.ensure_layout()?;

        if store.has_manifest(&artifact_id) {
            let descriptor = store.load_manifest(&artifact_id)?;
            if descriptor
                .chunks
                .iter()
                .all(|chunk| store.has_chunk(&chunk.chunk_id))
            {
                self.clear_transfer_state(&artifact_id);
                return Ok(descriptor);
            }
        }

        let telemetry_snapshot = self.telemetry().snapshot();
        let connected_peers = connected_peer_ids(&telemetry_snapshot)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut transfer_state = telemetry_snapshot
            .in_flight_transfers
            .get(&artifact_id)
            .cloned()
            .unwrap_or_else(|| ArtifactTransferState::new(artifact_id.clone()));
        let previous_source_peers = transfer_state.source_peers.clone();
        let previous_provider_peer_id = transfer_state.provider_peer_id.clone();
        transfer_state.source_peers = prioritized_artifact_source_peers(
            peer_id,
            previous_provider_peer_id.as_ref(),
            &previous_source_peers,
            &connected_peers,
        );
        if transfer_state.descriptor.is_none() && store.has_manifest(&artifact_id) {
            transfer_state.descriptor = Some(store.load_manifest(&artifact_id)?);
            transfer_state.set_phase(ArtifactTransferPhase::FetchingChunks);
        }
        if let Some(descriptor) = transfer_state.descriptor.clone() {
            for chunk in &descriptor.chunks {
                if store.has_chunk(&chunk.chunk_id) {
                    transfer_state.note_completed_chunk(&chunk.chunk_id);
                }
            }
        }
        self.record_transfer_state(transfer_state.clone());

        let provider_addresses = telemetry_snapshot
            .control_plane
            .peer_directory_announcements
            .iter()
            .filter(|announcement| transfer_state.source_peers.contains(&announcement.peer_id))
            .filter(|announcement| !connected_peers.contains(&announcement.peer_id))
            .flat_map(|announcement| announcement.addresses.iter().cloned())
            .filter(|address| !telemetry_snapshot.listen_addresses.contains(address))
            .collect::<BTreeSet<_>>();
        for address in provider_addresses {
            let _ = self.control.dial_address(address);
        }

        let auth_policy = admission_policy.as_ref();
        let mut selected_provider = transfer_state.provider_peer_id.clone();
        let mut descriptor = transfer_state.descriptor.clone();
        let mut candidate_manifest_results = BTreeMap::<PeerId, String>::new();
        if descriptor.is_none() {
            transfer_state.set_phase(ArtifactTransferPhase::LocatingProvider);
            self.record_transfer_state(transfer_state.clone());

            let deadline = Instant::now() + artifact_provider_locate_timeout;
            while Instant::now() < deadline && selected_provider.is_none() {
                self.redial_known_candidates(&transfer_state.source_peers);
                for candidate in &transfer_state.source_peers {
                    let Some(request_timeout) =
                        remaining_request_timeout(deadline, artifact_request_timeout)
                    else {
                        break;
                    };
                    if let Some(policy) = auth_policy {
                        let snapshot = if candidate == peer_id {
                            admitted_provider_snapshot.clone().unwrap_or_else(|| {
                                unreachable!("admitted provider snapshot must exist when auth policy is enabled")
                            })
                        } else {
                            match self
                                .control
                                .fetch_snapshot(candidate.as_str(), request_timeout)
                            {
                                Ok(snapshot) => snapshot,
                                Err(error) => {
                                    candidate_manifest_results.insert(
                                        candidate.clone(),
                                        format!("snapshot-error:{error}"),
                                    );
                                    continue;
                                }
                            }
                        };

                        let Ok(report) = verify_snapshot_admission(policy, candidate, &snapshot)
                        else {
                            candidate_manifest_results
                                .insert(candidate.clone(), "snapshot-admission-error".to_owned());
                            continue;
                        };
                        if !matches!(report.decision(), AdmissionDecision::Allow) {
                            candidate_manifest_results
                                .insert(candidate.clone(), "snapshot-not-admitted".to_owned());
                            continue;
                        }
                    }

                    match self.control.fetch_artifact_manifest(
                        candidate.as_str(),
                        artifact_id.clone(),
                        request_timeout,
                    ) {
                        Ok(Some(found)) => {
                            candidate_manifest_results
                                .insert(candidate.clone(), "found".to_owned());
                            selected_provider = Some(candidate.clone());
                            descriptor = Some(found.clone());
                            transfer_state.set_provider(candidate.clone(), found);
                            self.record_transfer_state(transfer_state.clone());
                            break;
                        }
                        Ok(None) => {
                            candidate_manifest_results.insert(candidate.clone(), "none".to_owned());
                            continue;
                        }
                        Err(error) => {
                            candidate_manifest_results
                                .insert(candidate.clone(), format!("error:{error}"));
                            continue;
                        }
                    }
                }

                if selected_provider.is_none() {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }

        let provider = selected_provider
            .clone()
            .or_else(|| transfer_state.source_peers.first().cloned())
            .ok_or_else(|| {
                let snapshot = self.telemetry().snapshot();
                anyhow::anyhow!(
                    "no connected peer provided artifact {}; connected_peers={:?}; source_peers={:?}; peer_directory={:?}; candidate_results={:?}",
                    artifact_id.as_str(),
                    connected_peer_ids(&snapshot)
                        .into_iter()
                        .map(|peer_id| peer_id.to_string())
                        .collect::<Vec<_>>(),
                    transfer_state
                        .source_peers
                        .iter()
                        .map(|peer_id| peer_id.to_string())
                        .collect::<Vec<_>>(),
                    snapshot
                        .control_plane
                        .peer_directory_announcements
                        .iter()
                        .map(|announcement| (
                            announcement.peer_id.to_string(),
                            announcement
                                .addresses
                                .iter()
                                .map(|address| address.as_str().to_owned())
                                .collect::<Vec<_>>()
                        ))
                        .collect::<Vec<_>>(),
                    candidate_manifest_results
                        .iter()
                        .map(|(peer_id, result)| (peer_id.to_string(), result.clone()))
                        .collect::<Vec<_>>(),
                )
            })?;
        let descriptor = descriptor.ok_or_else(|| {
                let snapshot = self.telemetry().snapshot();
                anyhow::anyhow!(
                "no connected peer provided artifact {}; connected_peers={:?}; source_peers={:?}; selected_provider={:?}; peer_directory={:?}; candidate_results={:?}",
                artifact_id.as_str(),
                connected_peer_ids(&snapshot)
                    .into_iter()
                    .map(|peer_id| peer_id.to_string())
                    .collect::<Vec<_>>(),
                transfer_state
                    .source_peers
                    .iter()
                    .map(|peer_id| peer_id.to_string())
                    .collect::<Vec<_>>(),
                selected_provider.as_ref().map(|peer_id| peer_id.to_string()),
                snapshot
                    .control_plane
                    .peer_directory_announcements
                    .iter()
                    .map(|announcement| (
                        announcement.peer_id.to_string(),
                        announcement
                            .addresses
                            .iter()
                            .map(|address| address.as_str().to_owned())
                            .collect::<Vec<_>>()
                        ))
                        .collect::<Vec<_>>(),
                candidate_manifest_results
                    .iter()
                    .map(|(peer_id, result)| (peer_id.to_string(), result.clone()))
                    .collect::<Vec<_>>(),
            )
        })?;
        let prioritized_source_peers = prioritized_artifact_source_peers(
            peer_id,
            Some(&provider),
            &transfer_state.source_peers,
            &connected_peers,
        );
        if transfer_state.provider_peer_id.as_ref() != Some(&provider)
            || transfer_state.descriptor.as_ref() != Some(&descriptor)
            || transfer_state.source_peers != prioritized_source_peers
        {
            transfer_state.set_provider(provider.clone(), descriptor.clone());
            transfer_state.source_peers = prioritized_source_peers.clone();
            self.record_transfer_state(transfer_state.clone());
        }
        let provider_candidates = prioritized_source_peers;

        for chunk in &descriptor.chunks {
            if store.has_chunk(&chunk.chunk_id) {
                if transfer_state.note_completed_chunk(&chunk.chunk_id) {
                    self.record_transfer_state(transfer_state.clone());
                }
                continue;
            }
            let chunk_deadline = Instant::now() + artifact_chunk_fetch_timeout;
            let mut stored = false;
            while Instant::now() < chunk_deadline && !stored {
                self.redial_known_candidates(&provider_candidates);
                for candidate in &provider_candidates {
                    let Some(request_timeout) =
                        remaining_request_timeout(chunk_deadline, artifact_request_timeout)
                    else {
                        break;
                    };
                    match self.control.fetch_artifact_chunk(
                        candidate.as_str(),
                        descriptor.artifact_id.clone(),
                        chunk.chunk_id.clone(),
                        request_timeout,
                    ) {
                        Ok(Some(payload)) => {
                            store.store_chunk_bytes(&payload.chunk, &payload.bytes)?;
                            transfer_state.note_completed_chunk(&payload.chunk.chunk_id);
                            self.record_transfer_state(transfer_state.clone());
                            stored = true;
                            break;
                        }
                        Ok(None) | Err(_) => continue,
                    }
                }

                if !stored {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }

            if !stored {
                let snapshot = self.telemetry().snapshot();
                return Err(anyhow::anyhow!(
                    "no connected peer provided chunk {} for artifact {}; provider_candidates={:?}; connected_peers={:?}; peer_directory={:?}",
                    chunk.chunk_id.as_str(),
                    descriptor.artifact_id.as_str(),
                    provider_candidates
                        .iter()
                        .map(|peer_id| peer_id.to_string())
                        .collect::<Vec<_>>(),
                    connected_peer_ids(&snapshot)
                        .into_iter()
                        .map(|peer_id| peer_id.to_string())
                        .collect::<Vec<_>>(),
                    snapshot
                        .control_plane
                        .peer_directory_announcements
                        .iter()
                        .map(|announcement| (
                            announcement.peer_id.to_string(),
                            announcement
                                .addresses
                                .iter()
                                .map(|address| address.as_str().to_owned())
                                .collect::<Vec<_>>()
                        ))
                        .collect::<Vec<_>>(),
                ));
            }
        }

        transfer_state.set_phase(ArtifactTransferPhase::Finalizing);
        self.record_transfer_state(transfer_state);
        store.store_manifest(&descriptor)?;
        self.publish_artifact_from_store(&descriptor.artifact_id)?;
        self.clear_transfer_state(&descriptor.artifact_id);
        Ok(descriptor)
    }

    fn effective_admission_policy(&self) -> Option<AdmissionPolicy> {
        let mut policy = self
            .config()
            .auth
            .as_ref()
            .and_then(|auth| auth.admission_policy.clone())?;
        let snapshot = self.telemetry().snapshot();
        if let Some(minimum_revocation_epoch) = snapshot.minimum_revocation_epoch
            && minimum_revocation_epoch > policy.minimum_revocation_epoch
        {
            policy.minimum_revocation_epoch = minimum_revocation_epoch;
        }
        if let Some(trust_bundle) = snapshot.trust_bundle
            && !trust_bundle.trusted_issuers.is_empty()
        {
            policy.trusted_issuers = trust_bundle.trusted_issuers;
            if trust_bundle.minimum_revocation_epoch > policy.minimum_revocation_epoch {
                policy.minimum_revocation_epoch = trust_bundle.minimum_revocation_epoch;
            }
        }
        Some(policy)
    }

    /// Performs the initialize local head operation.
    pub fn initialize_local_head(
        &mut self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<HeadDescriptor>
    where
        P: project_family::P2pWorkload,
    {
        let assignment = SlotAssignmentState::from_experiment(experiment);
        self.persist_primary_assignment(&assignment)?;
        self.update_runtime_state(
            NodeRuntimeState::HeadSync,
            Some(SlotRuntimeState::MaterializingBase(assignment.clone())),
        );
        self.ensure_experiment_topics(experiment)?;
        let storage =
            self.config().storage.as_ref().cloned().ok_or_else(|| {
                anyhow::anyhow!("initializing a head requires configured storage")
            })?;
        let store = FsArtifactStore::new(storage.root.clone());
        store.ensure_layout()?;

        let local_peer_id = self
            .telemetry()
            .snapshot()
            .local_peer_id
            .ok_or_else(|| anyhow::anyhow!("runtime does not have a local peer id yet"))?;
        let project = &mut self
            .node
            .as_mut()
            .expect("running node should retain prepared node")
            .project;
        let device = project.runtime_device();
        let model = project.init_model(&device);
        let head_id = HeadId::new(format!(
            "{}-{}-genesis",
            experiment.experiment_id.as_str(),
            local_peer_id.as_str()
        ));
        let artifact = project.materialize_model_artifact(
            &model,
            ArtifactKind::FullHead,
            head_id.clone(),
            None,
            &store,
        )?;
        let evaluation = project.evaluate(&model, EvalSplit::Validation);
        let head = HeadDescriptor {
            head_id: head_id.clone(),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            parent_head_id: None,
            global_step: 0,
            created_at: Utc::now(),
            metrics: evaluation.metrics,
        };

        persist_head_state(&storage, experiment, &head)?;
        persist_json(storage.scoped_head_path(&head.head_id), &head)?;
        store.pin_head(&head.head_id)?;
        store.pin_artifact(&artifact.artifact_id)?;
        self.update_runtime_state(
            NodeRuntimeState::PublishingUpdate,
            Some(SlotRuntimeState::Publishing(assignment)),
        );
        self.publish_artifact_from_store(&artifact.artifact_id)?;
        self.control.publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set()?.heads,
            provider_peer_id: Some(local_peer_id),
            head: head.clone(),
            announced_at: Utc::now(),
        })?;
        self.set_experiment_idle_state(experiment, NodeRuntimeState::IdleReady);

        Ok(head)
    }

    /// Synchronizes the experiment head.
    pub fn sync_experiment_head(
        &self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<Option<HeadDescriptor>> {
        const HEAD_SYNC_SNAPSHOT_TIMEOUT: Duration = Duration::from_millis(750);
        const HEAD_SYNC_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
        const HEAD_SYNC_POLL_INTERVAL: Duration = Duration::from_millis(50);

        let assignment = SlotAssignmentState::from_experiment(experiment);
        self.persist_primary_assignment(&assignment)?;
        self.update_runtime_state(
            NodeRuntimeState::HeadSync,
            Some(SlotRuntimeState::MaterializingBase(assignment)),
        );
        self.ensure_experiment_topics(experiment)?;
        let storage = match self.config().storage.as_ref() {
            Some(storage) => storage.clone(),
            None => return Ok(None),
        };
        let telemetry_snapshot = self.telemetry().snapshot();
        let cached_snapshots = cached_connected_snapshots(&telemetry_snapshot);
        let cached_canonical_snapshots = snapshots_with_local_control_plane(
            &cached_snapshots,
            telemetry_snapshot.local_peer_id.as_ref(),
            &telemetry_snapshot.control_plane,
        );
        let mut snapshots = cached_snapshots;
        let mut resolved_head =
            resolve_canonical_head(&storage, experiment, &cached_canonical_snapshots)?;

        if resolved_head.is_none() {
            snapshots = self.fetch_experiment_snapshots(experiment, HEAD_SYNC_SNAPSHOT_TIMEOUT)?;
            let canonical_snapshots = snapshots_with_local_control_plane(
                &snapshots,
                telemetry_snapshot.local_peer_id.as_ref(),
                &telemetry_snapshot.control_plane,
            );
            resolved_head = resolve_canonical_head(&storage, experiment, &canonical_snapshots)?;
        }
        let _ = self.assess_and_record_lag(&storage, experiment, &snapshots)?;
        let Some((source_peer_id, head)) = resolved_head else {
            let connected_peers = connected_peer_ids(&telemetry_snapshot);
            let provider_addresses = telemetry_snapshot
                .control_plane
                .peer_directory_announcements
                .iter()
                .filter(|announcement| !connected_peers.contains(&announcement.peer_id))
                .flat_map(|announcement| announcement.addresses.iter().cloned())
                .filter(|address| !telemetry_snapshot.listen_addresses.contains(address))
                .collect::<BTreeSet<_>>();
            for address in provider_addresses {
                let _ = self.control.dial_address(address);
            }
            return Ok(None);
        };
        let store = FsArtifactStore::new(storage.root.clone());
        if !store.has_manifest(&head.artifact_id) && head.global_step > 0 {
            let deadline = Instant::now() + HEAD_SYNC_WAIT_TIMEOUT;
            loop {
                match self.sync_artifact_from_peer_bounded(
                    &source_peer_id,
                    head.artifact_id.clone(),
                    HEAD_SYNC_WAIT_TIMEOUT,
                ) {
                    Ok(_) => break,
                    Err(error) if is_transient_artifact_sync_error(&error) => {
                        if Instant::now() >= deadline {
                            return Ok(None);
                        }
                    }
                    Err(error) => return Err(error),
                }
                std::thread::sleep(HEAD_SYNC_POLL_INTERVAL);
            }
        }
        persist_head_state(&storage, experiment, &head)?;
        self.update_lag_status(LagState::Current, 0, self.lag_policy(experiment));
        self.set_experiment_idle_state(experiment, NodeRuntimeState::IdleReady);
        Ok(Some(head))
    }

    /// Waits until the runtime can materialize a canonical experiment head.
    pub fn wait_for_experiment_head(
        &self,
        experiment: &ExperimentHandle,
        timeout: Duration,
    ) -> anyhow::Result<HeadDescriptor> {
        const HEAD_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

        let deadline = Instant::now() + timeout;
        let mut last_error = None;
        while Instant::now() < deadline {
            match self.sync_experiment_head(experiment) {
                Ok(Some(head)) => return Ok(head),
                Ok(None) => {}
                Err(error) => last_error = Some(error.to_string()),
            }
            std::thread::sleep(HEAD_WAIT_POLL_INTERVAL);
        }

        if let Some(error) = last_error {
            anyhow::bail!("timed out waiting for experiment head sync: {error}");
        }
        anyhow::bail!("timed out waiting for experiment head sync")
    }

    /// Waits until the runtime has adopted one specific known head.
    pub fn wait_for_known_head(
        &self,
        experiment: &ExperimentHandle,
        expected_head: &HeadDescriptor,
        timeout: Duration,
    ) -> anyhow::Result<HeadDescriptor> {
        const HEAD_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

        if self.adopt_known_head_if_present(experiment, expected_head)? {
            return Ok(expected_head.clone());
        }

        let deadline = Instant::now() + timeout;
        let mut last_error = None;
        while Instant::now() < deadline {
            match self.sync_experiment_head(experiment) {
                Ok(Some(head)) if head.head_id == expected_head.head_id => return Ok(head),
                Ok(Some(_)) | Ok(None) => {
                    if self.adopt_known_head_if_present(experiment, expected_head)? {
                        return Ok(expected_head.clone());
                    }
                }
                Err(error) => last_error = Some(error.to_string()),
            }
            std::thread::sleep(HEAD_WAIT_POLL_INTERVAL);
        }

        if self.adopt_known_head_if_present(experiment, expected_head)? {
            return Ok(expected_head.clone());
        }
        if let Some(error) = last_error {
            anyhow::bail!(
                "timed out waiting for known head {}: {error}",
                expected_head.head_id.as_str()
            );
        }
        anyhow::bail!(
            "timed out waiting for known head {}",
            expected_head.head_id.as_str()
        )
    }

    /// Prewarms one artifact from any currently known provider peer.
    pub fn wait_for_artifact_from_peers(
        &self,
        provider_peer_ids: &[PeerId],
        artifact_id: &ArtifactId,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        const ARTIFACT_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(100);
        const ARTIFACT_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

        let Some(store) = self.artifact_store() else {
            anyhow::bail!("artifact prewarm requires configured storage");
        };
        if store.has_manifest(artifact_id) {
            return Ok(());
        }
        anyhow::ensure!(
            !provider_peer_ids.is_empty(),
            "artifact {} does not have any provider peers to fetch from",
            artifact_id.as_str()
        );

        let deadline = Instant::now() + timeout;
        let mut last_error = None;
        while Instant::now() < deadline {
            if store.has_manifest(artifact_id) {
                return Ok(());
            }

            for provider_peer_id in provider_peer_ids {
                let Some(request_timeout) =
                    remaining_request_timeout(deadline, ARTIFACT_REQUEST_TIMEOUT)
                else {
                    break;
                };
                match self.sync_artifact_from_peer_bounded(
                    provider_peer_id,
                    artifact_id.clone(),
                    request_timeout,
                ) {
                    Ok(_) => return Ok(()),
                    Err(error) => {
                        last_error = Some(format!(
                            "could not fetch {} from {}: {error}",
                            artifact_id.as_str(),
                            provider_peer_id.as_str(),
                        ));
                    }
                }
            }

            std::thread::sleep(ARTIFACT_WAIT_POLL_INTERVAL);
        }

        if store.has_manifest(artifact_id) {
            return Ok(());
        }
        if let Some(error) = last_error {
            anyhow::bail!("{error}");
        }
        anyhow::bail!(
            "timed out waiting for artifact {} from providers",
            artifact_id.as_str()
        )
    }

    /// Adopts a known head descriptor once its artifact is available locally.
    pub fn adopt_known_head_if_present(
        &self,
        experiment: &ExperimentHandle,
        head: &HeadDescriptor,
    ) -> anyhow::Result<bool> {
        let Some(storage) = self.config().storage.as_ref().cloned() else {
            return Ok(false);
        };
        self.ensure_experiment_topics(experiment)?;
        let store = FsArtifactStore::new(storage.root.clone());
        if !store.has_manifest(&head.artifact_id) {
            return Ok(false);
        }

        persist_head_state(&storage, experiment, head)?;
        persist_json(storage.scoped_head_path(&head.head_id), head)?;
        store.pin_head(&head.head_id)?;
        store.pin_artifact(&head.artifact_id)?;
        self.update_lag_status(LagState::Current, 0, self.lag_policy(experiment));
        self.set_experiment_idle_state(experiment, NodeRuntimeState::IdleReady);
        Ok(true)
    }

    /// Publishes a locally materialized head as an available provider without
    /// updating the experiment's canonical current head.
    pub fn publish_head_provider(
        &self,
        experiment: &ExperimentHandle,
        head: &HeadDescriptor,
    ) -> anyhow::Result<()> {
        self.ensure_experiment_topics(experiment)?;
        self.publish_artifact_from_store(&head.artifact_id)?;
        let local_peer_id = self
            .telemetry()
            .snapshot()
            .local_peer_id
            .ok_or_else(|| anyhow::anyhow!("runtime does not have a local peer id yet"))?;
        self.control.publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set()?.heads,
            provider_peer_id: Some(local_peer_id),
            head: head.clone(),
            announced_at: Utc::now(),
        })?;
        Ok(())
    }

    /// Fetches a peer snapshot on demand and merges it into the local control-plane
    /// view. This is useful when the caller already knows a peer should have
    /// relevant experiment state and wants to avoid waiting for passive gossip.
    pub fn ingest_peer_snapshot(
        &self,
        peer_id: &PeerId,
        timeout: Duration,
    ) -> anyhow::Result<ControlPlaneSnapshot> {
        const SNAPSHOT_INGEST_POLL_INTERVAL: Duration = Duration::from_millis(50);
        let deadline = Instant::now() + timeout;
        let mut last_error = None;

        loop {
            let attempt_timeout = deadline.saturating_duration_since(Instant::now());
            if attempt_timeout.is_zero() {
                break;
            }

            match self.control.fetch_snapshot(
                peer_id.as_str(),
                attempt_timeout.min(Duration::from_secs(1)),
            ) {
                Ok(remote_snapshot) => {
                    for announcement in &remote_snapshot.control_announcements {
                        let _ = self.control.publish_control(announcement.clone());
                    }
                    for announcement in &remote_snapshot.head_announcements {
                        let _ = self.control.publish_head(announcement.clone());
                    }
                    for announcement in &remote_snapshot.lease_announcements {
                        let _ = self.control.publish_lease(announcement.clone());
                    }
                    for announcement in &remote_snapshot.merge_announcements {
                        let _ = self.control.publish_merge(announcement.clone());
                    }
                    for announcement in &remote_snapshot.merge_window_announcements {
                        let _ = self.control.publish_merge_window(announcement.clone());
                    }
                    for announcement in &remote_snapshot.reducer_assignment_announcements {
                        let _ = self
                            .control
                            .publish_reducer_assignment(announcement.clone());
                    }
                    for announcement in &remote_snapshot.update_announcements {
                        let _ = self.control.publish_update(announcement.clone());
                    }
                    for announcement in &remote_snapshot.aggregate_proposal_announcements {
                        let _ = self
                            .control
                            .publish_aggregate_proposal(announcement.clone());
                    }
                    for announcement in &remote_snapshot.reduction_certificate_announcements {
                        let _ = self
                            .control
                            .publish_reduction_certificate(announcement.clone());
                    }
                    for announcement in &remote_snapshot.validation_quorum_announcements {
                        let _ = self.control.publish_validation_quorum(announcement.clone());
                    }
                    for announcement in &remote_snapshot.reducer_load_announcements {
                        let _ = self.control.publish_reducer_load(announcement.clone());
                    }
                    for announcement in &remote_snapshot.auth_announcements {
                        let _ = self.control.publish_auth(announcement.clone());
                    }
                    for announcement in &remote_snapshot.directory_announcements {
                        let _ = self.control.publish_directory(announcement.clone());
                    }
                    for announcement in &remote_snapshot.metrics_announcements {
                        let _ = self.control.publish_metrics(announcement.clone());
                    }

                    let mut telemetry_snapshot = lock_telemetry_state(&self.telemetry.state);
                    merge_control_plane_snapshot(
                        &mut telemetry_snapshot.control_plane,
                        &remote_snapshot,
                    );
                    if let Some(storage) = self.config().storage.as_ref() {
                        persist_control_plane_state(storage, &telemetry_snapshot.control_plane)?;
                    }
                    telemetry_snapshot.updated_at = Utc::now();
                    return Ok(remote_snapshot);
                }
                Err(error) => {
                    last_error = Some(error);
                    let telemetry_snapshot = self.telemetry().snapshot();
                    let known_addresses = telemetry_snapshot
                        .control_plane
                        .peer_directory_announcements
                        .iter()
                        .filter(|announcement| announcement.peer_id == *peer_id)
                        .flat_map(|announcement| announcement.addresses.iter().cloned())
                        .filter(|address| !telemetry_snapshot.listen_addresses.contains(address))
                        .collect::<BTreeSet<_>>();
                    for address in known_addresses {
                        let _ = self.control.dial_address(address);
                    }
                    let _ = self.control.request_snapshot(peer_id.as_str());
                }
            }

            std::thread::sleep(SNAPSHOT_INGEST_POLL_INTERVAL);
        }

        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!(
                "timed out ingesting snapshot from peer {}",
                peer_id.as_str()
            )
        }))
    }

    /// Republishes the locally visible training control-plane announcements for one
    /// completed training window. This is useful when a live peer has already
    /// materialized a window locally and needs to nudge merge-window/update/head
    /// propagation across the mesh without recomputing the window.
    pub fn republish_training_window_control_plane(
        &self,
        experiment: &ExperimentHandle,
        window_id: WindowId,
        base_head_id: &HeadId,
        artifact_id: &ArtifactId,
    ) -> anyhow::Result<()> {
        self.ensure_experiment_topics(experiment)?;
        let overlay = experiment.overlay_set()?.heads;
        let snapshot = self.telemetry().snapshot().control_plane;

        if let Some(announcement) = snapshot
            .merge_window_announcements
            .iter()
            .find(|announcement| {
                announcement.overlay == overlay
                    && announcement.merge_window.study_id == experiment.study_id
                    && announcement.merge_window.experiment_id == experiment.experiment_id
                    && announcement.merge_window.revision_id == experiment.revision_id
                    && announcement.merge_window.window_id == window_id
                    && announcement.merge_window.base_head_id == *base_head_id
            })
            .cloned()
        {
            self.control.publish_merge_window(announcement)?;
        }

        if let Some(announcement) = snapshot
            .update_announcements
            .iter()
            .find(|announcement| {
                announcement.overlay == overlay
                    && announcement.update.study_id == experiment.study_id
                    && announcement.update.experiment_id == experiment.experiment_id
                    && announcement.update.revision_id == experiment.revision_id
                    && announcement.update.window_id == window_id
                    && announcement.update.base_head_id == *base_head_id
                    && announcement.update.delta_artifact_id == *artifact_id
            })
            .cloned()
        {
            self.control.publish_update(announcement)?;
        }

        if let Some(announcement) = snapshot
            .head_announcements
            .iter()
            .find(|announcement| {
                announcement.overlay == overlay
                    && announcement.head.study_id == experiment.study_id
                    && announcement.head.experiment_id == experiment.experiment_id
                    && announcement.head.revision_id == experiment.revision_id
                    && announcement.head.artifact_id == *artifact_id
            })
            .cloned()
        {
            self.control.publish_head(announcement)?;
        }

        Ok(())
    }

    /// Seeds locally known control-plane state for a completed training window
    /// outcome that originated on another peer. This is useful for reducers or
    /// validators that already fetched the candidate artifact and want to
    /// prewarm the corresponding merge-window, update, and head records before a
    /// validation pass.
    pub fn seed_training_candidate<T>(
        &self,
        experiment: &ExperimentHandle,
        outcome: &TrainingWindowOutcome<T>,
    ) -> anyhow::Result<()> {
        self.ensure_experiment_topics(experiment)?;
        self.publish_artifact_from_store(&outcome.artifact.artifact_id)?;
        let telemetry_snapshot = self.telemetry().snapshot();
        let local_peer_id = telemetry_snapshot
            .local_peer_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("runtime does not have a local peer id yet"))?;
        let topology_policy = runtime_merge_topology_policy(
            &telemetry_snapshot,
            experiment,
            Some(&outcome.contribution.base_head_id),
        );
        let robustness_policy =
            runtime_robustness_policy(self.config(), &telemetry_snapshot, experiment);
        let topology_peers =
            runtime_topology_peers(&telemetry_snapshot, &self.mainnet().roles, &local_peer_id);
        let validator_peers =
            runtime_validator_peers(&telemetry_snapshot, &self.mainnet().roles, &local_peer_id);
        let validators = runtime_validators(
            &self.mainnet().roles,
            &local_peer_id,
            &validator_peers,
            topology_policy.promotion_policy.validator_quorum,
        );
        let merge_window = latest_merge_window_from_snapshot(
            &telemetry_snapshot.control_plane,
            experiment,
            Some(&outcome.contribution.base_head_id),
        )
        .filter(|merge_window| merge_window.window_id == outcome.lease.window_id)
        .unwrap_or(open_runtime_merge_window(
            experiment,
            outcome.lease.window_id,
            outcome.contribution.base_head_id.clone(),
            topology_policy,
            topology_peers,
            validators,
        )?);
        self.control.publish_merge_window(MergeWindowAnnouncement {
            overlay: experiment.overlay_set()?.heads.clone(),
            merge_window,
            announced_at: Utc::now(),
        })?;
        self.control.publish_update(UpdateEnvelopeAnnouncement {
            overlay: experiment.overlay_set()?.heads.clone(),
            update: UpdateAnnounce {
                peer_id: outcome.contribution.peer_id.clone(),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: outcome.lease.window_id,
                base_head_id: outcome.contribution.base_head_id.clone(),
                lease_id: Some(outcome.lease.lease_id.clone()),
                delta_artifact_id: outcome.artifact.artifact_id.clone(),
                sample_weight: outcome.contribution.accepted_weight,
                quality_weight: (1.0 / (1.0 + metric_quality(&outcome.contribution.metrics).abs()))
                    .max(0.01),
                norm_stats: update_norm_stats(&outcome.contribution.metrics),
                feature_sketch: Some(update_feature_sketch_from_metrics(
                    &outcome.contribution.metrics,
                    Some(&outcome.head.metrics),
                    robustness_policy.screening_policy.sketch_dimensionality as usize,
                    0,
                    0,
                    None,
                )),
                receipt_root: ContentId::derive(&[outcome.contribution.receipt_id.as_str()])?,
                receipt_ids: vec![outcome.contribution.receipt_id.clone()],
                providers: vec![outcome.contribution.peer_id.clone()],
                announced_at: Utc::now(),
            },
        })?;
        self.control.publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set()?.heads,
            provider_peer_id: Some(outcome.contribution.peer_id.clone()),
            head: outcome.head.clone(),
            announced_at: Utc::now(),
        })?;
        Ok(())
    }

    /// Performs the restore experiment head operation.
    pub fn restore_experiment_head(
        &self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<Option<HeadDescriptor>> {
        let assignment = SlotAssignmentState::from_experiment(experiment);
        self.persist_primary_assignment(&assignment)?;
        self.update_runtime_state(
            NodeRuntimeState::HeadSync,
            Some(SlotRuntimeState::MaterializingBase(assignment)),
        );
        self.ensure_experiment_topics(experiment)?;
        let Some(storage) = self.config().storage.as_ref().cloned() else {
            return Ok(None);
        };
        let Some(head) = load_head_state(&storage, experiment)? else {
            return Ok(None);
        };

        self.publish_artifact_from_store(&head.artifact_id)?;
        let local_peer_id = self
            .telemetry()
            .snapshot()
            .local_peer_id
            .ok_or_else(|| anyhow::anyhow!("runtime does not have a local peer id yet"))?;
        if let Some(merge_certificate) = load_latest_merge_certificate(&storage, experiment)?
            && merge_certificate.merged_head_id == head.head_id
        {
            self.control.publish_merge(MergeAnnouncement {
                overlay: experiment.overlay_set()?.heads,
                certificate: merge_certificate,
                announced_at: Utc::now(),
            })?;
        }
        self.control.publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set()?.heads,
            provider_peer_id: Some(local_peer_id),
            head: head.clone(),
            announced_at: Utc::now(),
        })?;
        self.set_experiment_idle_state(experiment, NodeRuntimeState::IdleReady);
        Ok(Some(head))
    }

    /// Performs the shutdown operation.
    pub fn shutdown(&self) -> anyhow::Result<()> {
        self.update_runtime_state(NodeRuntimeState::ShuttingDown, None);
        self.control.shutdown()
    }

    /// Performs the await termination operation.
    pub fn await_termination(mut self) -> anyhow::Result<Node<P>> {
        if let Some(mut training_prefetch) = self.training_prefetch.take()
            && training_prefetch
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished)
        {
            if let Some(handle) = training_prefetch.handle.take() {
                let _ = handle.join();
            }
        }
        if let Some(runtime_thread) = self.runtime_thread.take() {
            runtime_thread
                .join()
                .map_err(|_| anyhow::anyhow!("runtime thread panicked"))?;
        }

        self.node
            .take()
            .ok_or_else(|| anyhow::anyhow!("running node already consumed"))
    }

    /// Performs the await termination operation with a bounded timeout.
    pub fn await_termination_timeout(mut self, timeout: Duration) -> anyhow::Result<Node<P>> {
        if let Some(mut training_prefetch) = self.training_prefetch.take()
            && training_prefetch
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished)
        {
            if let Some(handle) = training_prefetch.handle.take() {
                let _ = handle.join();
            }
        }
        if let Some(runtime_thread) = self.runtime_thread.take() {
            let deadline = Instant::now() + timeout;
            while !runtime_thread.is_finished() {
                if Instant::now() >= deadline {
                    anyhow::bail!("timed out waiting for runtime thread termination");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            runtime_thread
                .join()
                .map_err(|_| anyhow::anyhow!("runtime thread panicked"))?;
        }

        self.node
            .take()
            .ok_or_else(|| anyhow::anyhow!("running node already consumed"))
    }
}

fn slot_assignment_from_state(slot_state: &SlotRuntimeState) -> Option<SlotAssignmentState> {
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

fn is_transient_artifact_sync_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    [
        "timed out waiting for snapshot",
        "timed out waiting for artifact-manifest",
        "timed out waiting for artifact-chunk",
        "no connected peer provided artifact",
        "no connected peer provided chunk",
    ]
    .iter()
    .any(|pattern| message.contains(pattern))
}

fn remaining_request_timeout(deadline: Instant, request_timeout: Duration) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        None
    } else {
        Some(request_timeout.min(remaining))
    }
}

fn prioritized_artifact_source_peers(
    requested_peer_id: &PeerId,
    provider_peer_id: Option<&PeerId>,
    existing_source_peers: &[PeerId],
    connected_peers: &BTreeSet<PeerId>,
) -> Vec<PeerId> {
    dedupe_peer_ids(
        std::iter::once(requested_peer_id.clone())
            .chain(provider_peer_id.into_iter().cloned())
            .chain(existing_source_peers.iter().cloned())
            .chain(connected_peers.iter().cloned()),
    )
}

impl<P> RunningNode<SelectedWorkloadProject<P>>
where
    P: P2pProjectFamily,
{
    fn current_primary_assignment(&self) -> Option<SlotAssignmentState> {
        self.telemetry()
            .snapshot()
            .slot_states
            .first()
            .and_then(slot_assignment_from_state)
            .or_else(|| {
                self.config()
                    .storage
                    .as_ref()
                    .and_then(|storage| load_primary_slot_assignment(storage).ok().flatten())
            })
    }

    fn visible_current_experiment_entry(
        &self,
        study_id: &StudyId,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<ExperimentDirectoryEntry> {
        self.list_experiments()
            .into_iter()
            .rev()
            .find(|entry| entry.study_id == *study_id && entry.experiment_id == *experiment_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "experiment {} is not visible to the current node scope",
                    experiment_id.as_str()
                )
            })
    }

    fn switch_experiment_entry(
        &mut self,
        entry: ExperimentDirectoryEntry,
    ) -> anyhow::Result<ExperimentHandle> {
        let experiment = self.experiment(
            entry.study_id.clone(),
            entry.experiment_id.clone(),
            entry.current_revision_id.clone(),
        );
        let assignment = SlotAssignmentState::from_experiment(&experiment);
        let idle_state = config::default_node_runtime_state(&self.mainnet().roles);

        self.update_runtime_state(
            NodeRuntimeState::DirectorySync,
            Some(SlotRuntimeState::Migrating(assignment.clone())),
        );

        let result = (|| -> anyhow::Result<()> {
            {
                let node = self
                    .node
                    .as_mut()
                    .expect("running node should retain prepared node");
                node.project.switch_workload(entry.workload_id.clone())?;
                node.config.selected_workload_id = Some(entry.workload_id.clone());
                if let Some(storage) = node.config.storage.as_ref() {
                    persist_runtime_binding_state(storage, node.config())?;
                }
            }
            self.ensure_experiment_topics(&experiment)?;
            self.persist_primary_assignment(&assignment)?;
            Ok(())
        })();

        if let Err(error) = result {
            self.update_runtime_state(
                idle_state,
                Some(SlotRuntimeState::Blocked {
                    assignment: Some(assignment),
                    reason: error.to_string(),
                }),
            );
            return Err(error);
        }

        self.update_runtime_state(idle_state, Some(SlotRuntimeState::Assigned(assignment)));
        Ok(experiment)
    }

    fn reconcile_directory_assignment(&mut self) -> anyhow::Result<Option<ExperimentHandle>> {
        let Some(assignment) = self.current_primary_assignment() else {
            return Ok(None);
        };
        let entry =
            self.visible_current_experiment_entry(&assignment.study_id, &assignment.experiment_id)?;
        let selected_workload_id = self.config().selected_workload_id.clone();

        if entry.current_revision_id != assignment.revision_id
            || selected_workload_id.as_ref() != Some(&entry.workload_id)
        {
            return self.switch_experiment_entry(entry).map(Some);
        }

        Ok(Some(self.experiment(
            assignment.study_id,
            assignment.experiment_id,
            assignment.revision_id,
        )))
    }

    fn apply_control_announcements_for_window(
        &mut self,
        activation_window: WindowId,
    ) -> anyhow::Result<()> {
        let local_peer_id = self.telemetry().snapshot().local_peer_id;
        let mut announcements = self
            .telemetry()
            .snapshot()
            .control_plane
            .control_announcements
            .into_iter()
            .filter(|announcement| {
                announcement.certificate.network_id == self.mainnet().genesis.network_id
                    && announcement.certificate.activation.activation_window <= activation_window
            })
            .collect::<Vec<_>>();
        announcements.sort_by(|left, right| {
            left.certificate
                .activation
                .activation_window
                .cmp(&right.certificate.activation.activation_window)
                .then(left.announced_at.cmp(&right.announced_at))
        });

        for announcement in announcements {
            match &announcement.certificate.body.payload.payload.command {
                ExperimentControlCommand::PauseExperiment {
                    study_id,
                    experiment_id,
                    reason,
                    ..
                } => {
                    let Some(assignment) = self.current_primary_assignment() else {
                        continue;
                    };
                    if assignment.study_id == *study_id
                        && assignment.experiment_id == *experiment_id
                    {
                        self.update_runtime_state(
                            config::default_node_runtime_state(&self.mainnet().roles),
                            Some(SlotRuntimeState::Blocked {
                                assignment: Some(assignment),
                                reason: reason.clone().unwrap_or_else(|| {
                                    format!("experiment {} paused", experiment_id.as_str())
                                }),
                            }),
                        );
                    }
                }
                ExperimentControlCommand::ResumeExperiment {
                    study_id,
                    experiment_id,
                    ..
                } => {
                    let Some(assignment) = self.current_primary_assignment() else {
                        continue;
                    };
                    if assignment.study_id == *study_id
                        && assignment.experiment_id == *experiment_id
                    {
                        self.update_runtime_state(
                            config::default_node_runtime_state(&self.mainnet().roles),
                            Some(SlotRuntimeState::Assigned(assignment)),
                        );
                    }
                }
                ExperimentControlCommand::RevokePeer {
                    peer_id,
                    minimum_revocation_epoch,
                    reason,
                } if local_peer_id.as_ref() == Some(peer_id) => {
                    self.update_runtime_state(
                        NodeRuntimeState::Revoked,
                        Some(SlotRuntimeState::Blocked {
                            assignment: self.current_primary_assignment(),
                            reason: format!(
                                "{} (minimum revocation epoch {})",
                                reason, minimum_revocation_epoch.0
                            ),
                        }),
                    );
                }
                ExperimentControlCommand::QuarantinePeer {
                    peer_id, reason, ..
                } if local_peer_id.as_ref() == Some(peer_id) => {
                    self.update_runtime_state(
                        NodeRuntimeState::Quarantined,
                        Some(SlotRuntimeState::Blocked {
                            assignment: self.current_primary_assignment(),
                            reason: reason.clone(),
                        }),
                    );
                }
                ExperimentControlCommand::PardonPeer { peer_id, .. }
                    if local_peer_id.as_ref() == Some(peer_id) =>
                {
                    let slot_state = self
                        .current_primary_assignment()
                        .map(SlotRuntimeState::Assigned)
                        .unwrap_or(SlotRuntimeState::Unassigned);
                    self.update_runtime_state(
                        config::default_node_runtime_state(&self.mainnet().roles),
                        Some(slot_state),
                    );
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Performs the switch experiment operation.
    pub fn switch_experiment(
        &mut self,
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
    ) -> anyhow::Result<ExperimentHandle> {
        let entry = self.visible_experiment_entry(&study_id, &experiment_id, &revision_id)?;
        self.switch_experiment_entry(entry)
    }

    /// Performs the restore selected experiment operation.
    pub fn restore_selected_experiment(&mut self) -> anyhow::Result<Option<ExperimentHandle>> {
        let Some(storage) = self.config().storage.as_ref().cloned() else {
            return Ok(None);
        };
        let Some(assignment) = load_primary_slot_assignment(&storage)? else {
            return Ok(None);
        };

        self.switch_experiment(
            assignment.study_id,
            assignment.experiment_id,
            assignment.revision_id,
        )
        .map(Some)
    }

    /// Performs the active experiment operation.
    pub fn active_experiment(&self) -> Option<ExperimentHandle> {
        let assignment = self.current_primary_assignment()?;
        Some(self.experiment(
            assignment.study_id,
            assignment.experiment_id,
            assignment.revision_id,
        ))
    }

    /// Performs the follow control plane once operation.
    pub fn follow_control_plane_once(
        &mut self,
        activation_window: WindowId,
    ) -> anyhow::Result<Option<ExperimentHandle>> {
        let experiment = self.reconcile_directory_assignment()?;
        self.apply_control_announcements_for_window(activation_window)?;
        Ok(experiment.or_else(|| self.active_experiment()))
    }
}

/// Public APIs for checkpoint.
pub mod checkpoint {
    pub use crate::{
        ArtifactBuildSpec, CheckpointCatalog, CheckpointError, CheckpointLineage,
        CheckpointSyncHandle, ChunkingScheme, EmaFlow, FsArtifactStore, GarbageCollectionReport,
        MergeCandidate, MergePlan, SyncPlan, SyncRequest,
    };
}

/// Public APIs for dataloader.
pub mod dataloader {
    pub use crate::{
        BurnDataLoaderAdapter, CachedMicroShard, CachedMicroShardLoader, DataReceiptBuilder,
        DataloaderError, DatasetRegistration, DatasetSizing, LeaseCache, LeasePlanner,
        LeasePlannerConfig, LeaseSelection, MicroShardPlan, MicroShardPlanner,
        MicroShardPlannerConfig, PlannedLease, ShardAwareSampler, ShardCache, ShardCostModel,
        ShardFetchEntry, ShardFetchManifest, UpstreamAdapter,
    };
}

/// Public APIs for experiment.
pub mod experiment {
    pub use crate::{
        ExperimentControlCommand, ExperimentHandle, ExperimentOverlayTopics, ExperimentSpec,
        PatchClass, PatchOutcome, PatchSupport, PatchValue, RevisionCompatibility, RevisionSpec,
        RuntimePatch, StudySpec,
    };
}

/// Public APIs for limits.
pub mod limits {
    pub use crate::{
        CapabilityCalibrator, CapabilityProbe, LimitPolicy, LimitProfile, LimitsError,
        LocalBackend, WorkBudget,
    };
}

/// Public APIs for security.
pub mod security {
    pub use crate::{
        AdmissionDecision, AuditFinding, ChallengeResponse, ClientManifest, DataAuditReport,
        MergeEvidence, MergeEvidenceDecision, MergeEvidenceRequirement, ReleaseManifest,
        ReleasePolicy, ReputationDecision, ReputationEngine, ReputationObservation,
        ReputationPolicy, ReputationState, SecurityError, UpdateAuditReport, ValidatorPolicy,
    };
}

#[cfg(feature = "burn")]
/// Burn-specific runtime integration helpers.
pub mod burn;

/// Public APIs for prelude.
pub mod prelude {
    #[cfg(feature = "burn")]
    pub use crate::burn;
    pub use crate::{
        AggregateArtifactBytes, AggregateArtifactInput, AggregateArtifactRecord, ArtifactBuildSpec,
        BurnDataLoaderAdapter, CachedMicroShard, CachedMicroShardLoader, CapabilityCalibrator,
        CapabilityProbe, CheckpointCatalog, CheckpointError, CheckpointLineage,
        CheckpointSyncHandle, ChunkingScheme, ControlHandle, DataReceiptBuilder, DataloaderError,
        DatasetConfig, DatasetRegistration, DatasetSizing, EmaFlow, EvalSplit,
        ExperimentControlCommand, ExperimentHandle, ExperimentOverlayTopics, ExperimentSpec,
        FsArtifactStore, GarbageCollectionReport, IdentityConfig, LeaseCache, LeasePlanner,
        LeasePlannerConfig, LeaseSelection, LimitPolicy, LimitProfile, LimitsError, LocalBackend,
        MainnetHandle, MergeCandidate, MergePlan, MetricReport, MicroShardPlan, MicroShardPlanner,
        MicroShardPlannerConfig, Node, NodeBuilder, NodeConfig, NodeRuntimeState,
        NodeTelemetrySnapshot, P2pProjectFamily, P2pWorkload, PatchClass, PatchOutcome,
        PatchSupport, PatchValue, PlannedLease, ReducerOutcome, ReleaseManifest, ReleasePolicy,
        ReputationDecision, ReputationEngine, ReputationObservation, ReputationPolicy,
        ReputationState, RevisionCompatibility, RevisionSpec, RoleSet, RunningNode, RuntimePatch,
        RuntimeStatus, SecurityError, SelectedWorkloadProject, ShardAwareSampler, ShardCache,
        ShardCostModel, ShardFetchEntry, ShardFetchManifest, SingleWorkloadProjectFamily,
        SlotAssignmentState, SlotRuntimeState, StorageConfig, StudySpec, SyncPlan, SyncRequest,
        TelemetryHandle, TrainError, TrainingWindowOutcome, UpdateAuditReport, UpstreamAdapter,
        ValidationOutcome, ValidatorPolicy, WindowCtx, WindowReport, WorkBudget, checkpoint,
        dataloader, experiment, limits, materialize_aggregate_artifact_bytes, security,
    };
    pub use burn_p2p_core::{
        ArtifactDescriptor, ArtifactId, ArtifactKind, AssignmentLease, CapabilityCard,
        CapabilityEstimate, ChunkId, ClientReleaseManifest, ContentId, ContributionReceipt,
        ContributionReceiptId, DatasetId, DatasetManifest, DatasetView, DatasetViewId,
        ExperimentId, ExperimentManifest, GenesisSpec, HeadDescriptor, HeadId, LeaseId,
        MergeCertId, MergeCertificate, MergePolicy, MetricValue, MicroShardId, NetworkEstimate,
        NetworkId, NetworkManifest, PeerId, PeerRole, PeerRoleSet, Precision, ProjectFamilyId,
        RevisionId, RevisionManifest, StudyId, SupportedWorkload, TelemetrySummary,
        WindowActivation, WindowId, WorkloadId,
    };
}

/// Defines the slot state alias.
pub type SlotState = SlotRuntimeState;
/// Defines the catchup policy alias.
pub type CatchupPolicy = LagPolicy;

#[cfg(test)]
mod tests;
