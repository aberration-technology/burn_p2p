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
    EvalSplit, MergeModelCandidate, MetricReport, PatchOutcome, TrainError, TrainingWindowOutcome,
    ValidationOutcome, WindowCtx, WindowReport,
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
    latest_head_from_snapshot, latest_merge_window_from_connected_snapshots,
    latest_merge_window_from_snapshot, latest_reducer_assignment_from_snapshot, load_head_state,
    load_known_peers, load_latest_merge_certificate, load_primary_slot_assignment,
    matches_experiment_head, metric_quality, next_window_id, open_runtime_merge_window,
    persist_auth_state, persist_head_state, persist_in_flight_transfer_states, persist_json,
    persist_limit_profile, persist_primary_slot_assignment, persist_runtime_binding_state,
    persist_runtime_security_state, persist_window_id, remove_artifact_transfer_state,
    resolve_canonical_head, resolve_identity, restore_auth_config, restore_control_plane_state,
    restore_in_flight_transfer_states, restore_runtime_binding_config,
    restore_runtime_security_state, run_control_plane, runtime_assign_reducers,
    runtime_limit_policy, runtime_merge_topology_policy, runtime_robustness_policy,
    runtime_topology_peers, runtime_validator_peers, runtime_validators,
    set_effective_limit_profile, update_announces_from_connected_snapshots,
    update_feature_sketch_from_metrics, update_norm_stats, verify_snapshot_admission,
};
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
    runtime_thread: Option<JoinHandle<()>>,
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
        let control = ControlHandle { tx: command_tx };
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

    fn fetch_connected_snapshots(
        &self,
        timeout: Duration,
    ) -> anyhow::Result<Vec<(PeerId, ControlPlaneSnapshot)>> {
        let admission_policy = self.effective_admission_policy();
        let telemetry_snapshot = self.telemetry().snapshot();
        let mut snapshots = cached_connected_snapshots(&telemetry_snapshot)
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for peer_id in connected_peer_ids(&telemetry_snapshot) {
            let Ok(snapshot) = self.control.fetch_snapshot(peer_id.as_str(), timeout) else {
                continue;
            };
            if let Some(policy) = admission_policy.as_ref() {
                let Ok(report) = verify_snapshot_admission(policy, &peer_id, &snapshot) else {
                    snapshots.remove(&peer_id);
                    continue;
                };
                if !matches!(report.decision(), AdmissionDecision::Allow) {
                    snapshots.remove(&peer_id);
                    continue;
                }
            }
            snapshots.insert(peer_id, snapshot);
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
        let admission_policy = self.effective_admission_policy();
        if let Some(policy) = admission_policy.as_ref() {
            let snapshot = self
                .control
                .fetch_snapshot(peer_id.as_str(), Duration::from_secs(5))?;
            let report = verify_snapshot_admission(policy, peer_id, &snapshot)?;
            if !matches!(report.decision(), AdmissionDecision::Allow) {
                return Err(anyhow::anyhow!(
                    "peer {} is not admitted for artifact sync",
                    peer_id.as_str()
                ));
            }
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
        let mut candidate_peers = vec![peer_id.clone()];
        candidate_peers.extend(
            connected_peers
                .iter()
                .filter(|candidate| *candidate != peer_id)
                .cloned(),
        );
        let mut transfer_state = telemetry_snapshot
            .in_flight_transfers
            .get(&artifact_id)
            .cloned()
            .unwrap_or_else(|| ArtifactTransferState::new(artifact_id.clone()));
        transfer_state.source_peers = dedupe_peer_ids(
            transfer_state
                .provider_peer_id
                .iter()
                .cloned()
                .chain(transfer_state.source_peers.iter().cloned())
                .chain(candidate_peers.iter().cloned()),
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
        if descriptor.is_none() {
            transfer_state.set_phase(ArtifactTransferPhase::LocatingProvider);
            self.record_transfer_state(transfer_state.clone());

            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline && selected_provider.is_none() {
                self.redial_known_candidates(&transfer_state.source_peers);
                for candidate in &transfer_state.source_peers {
                    if let Some(policy) = auth_policy {
                        let Ok(snapshot) = self
                            .control
                            .fetch_snapshot(candidate.as_str(), Duration::from_secs(2))
                        else {
                            continue;
                        };
                        let Ok(report) = verify_snapshot_admission(policy, candidate, &snapshot)
                        else {
                            continue;
                        };
                        if !matches!(report.decision(), AdmissionDecision::Allow) {
                            continue;
                        }
                    }

                    match self.control.fetch_artifact_manifest(
                        candidate.as_str(),
                        artifact_id.clone(),
                        Duration::from_secs(2),
                    ) {
                        Ok(Some(found)) => {
                            selected_provider = Some(candidate.clone());
                            descriptor = Some(found.clone());
                            transfer_state.set_provider(candidate.clone(), found);
                            self.record_transfer_state(transfer_state.clone());
                            break;
                        }
                        Ok(None) | Err(_) => continue,
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
                    "no connected peer provided artifact {}; connected_peers={:?}; source_peers={:?}; peer_directory={:?}",
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
                )
            })?;
        let descriptor = descriptor.ok_or_else(|| {
            let snapshot = self.telemetry().snapshot();
            anyhow::anyhow!(
                "no connected peer provided artifact {}; connected_peers={:?}; source_peers={:?}; selected_provider={:?}; peer_directory={:?}",
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
            )
        })?;
        if transfer_state.provider_peer_id.as_ref() != Some(&provider)
            || transfer_state.descriptor.as_ref() != Some(&descriptor)
        {
            transfer_state.set_provider(provider.clone(), descriptor.clone());
            self.record_transfer_state(transfer_state.clone());
        }
        let provider_candidates = dedupe_peer_ids(
            std::iter::once(provider.clone())
                .chain(transfer_state.source_peers.iter().cloned())
                .chain(candidate_peers),
        );

        for chunk in &descriptor.chunks {
            if store.has_chunk(&chunk.chunk_id) {
                if transfer_state.note_completed_chunk(&chunk.chunk_id) {
                    self.record_transfer_state(transfer_state.clone());
                }
                continue;
            }
            let chunk_deadline = Instant::now() + Duration::from_secs(10);
            let mut stored = false;
            while Instant::now() < chunk_deadline && !stored {
                self.redial_known_candidates(&provider_candidates);
                for candidate in &provider_candidates {
                    match self.control.fetch_artifact_chunk(
                        candidate.as_str(),
                        descriptor.artifact_id.clone(),
                        chunk.chunk_id.clone(),
                        Duration::from_secs(2),
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
        let snapshots = self.fetch_connected_snapshots(Duration::from_secs(3))?;
        let _ = self.assess_and_record_lag(&storage, experiment, &snapshots)?;
        let Some((source_peer_id, head)) =
            resolve_canonical_head(&storage, experiment, &snapshots)?
        else {
            return Ok(None);
        };
        let store = FsArtifactStore::new(storage.root.clone());
        if !store.has_manifest(&head.artifact_id) {
            self.sync_artifact_from_peer(&source_peer_id, head.artifact_id.clone())?;
        }
        persist_head_state(&storage, experiment, &head)?;
        self.update_lag_status(LagState::Current, 0, self.lag_policy(experiment));
        self.set_experiment_idle_state(experiment, NodeRuntimeState::IdleReady);
        Ok(Some(head))
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
        if let Some(runtime_thread) = self.runtime_thread.take() {
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
        PatchSupport, PatchValue, PlannedLease, ReleaseManifest, ReleasePolicy, ReputationDecision,
        ReputationEngine, ReputationObservation, ReputationPolicy, ReputationState,
        RevisionCompatibility, RevisionSpec, RoleSet, RunningNode, RuntimePatch, RuntimeStatus,
        SecurityError, SelectedWorkloadProject, ShardAwareSampler, ShardCache, ShardCostModel,
        ShardFetchEntry, ShardFetchManifest, SingleWorkloadProjectFamily, SlotAssignmentState,
        SlotRuntimeState, StorageConfig, StudySpec, SyncPlan, SyncRequest, TelemetryHandle,
        TrainError, TrainingWindowOutcome, UpdateAuditReport, UpstreamAdapter, ValidationOutcome,
        ValidatorPolicy, WindowCtx, WindowReport, WorkBudget, checkpoint, dataloader, experiment,
        limits, materialize_aggregate_artifact_bytes, security,
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
