use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    BootstrapAdminState, BootstrapDiagnostics, BootstrapDiagnosticsBundle, BootstrapPlan,
    BootstrapService,
};
use burn_p2p::{
    ControlHandle, ExperimentHandle, IdentityConfig, LiveControlPlaneEvent, MetricsRetentionConfig,
    MetricsRetentionPreset, NodeBuilder, NodeConfig, NodeTelemetrySnapshot, P2pWorkload,
    RunningNode, StorageConfig, TelemetryHandle,
};
use burn_p2p_core::{ExperimentId, PeerId, RevisionId, StudyId};
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an active experiment.
pub struct ActiveExperiment {
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
}

impl ActiveExperiment {
    /// Performs the handle operation.
    pub fn handle(&self, network: &burn_p2p::MainnetHandle) -> ExperimentHandle {
        network.experiment(
            self.study_id.clone(),
            self.experiment_id.clone(),
            self.revision_id.clone(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures bootstrap embedded daemon.
pub struct BootstrapEmbeddedDaemonConfig {
    /// The node.
    pub node: NodeConfig,
    /// The active experiment.
    pub active_experiment: ActiveExperiment,
    /// The initialize head on start.
    pub initialize_head_on_start: bool,
    /// The restore head on start.
    pub restore_head_on_start: bool,
    /// The validation interval millis.
    pub validation_interval_millis: u64,
    /// The reducer interval millis.
    pub reducer_interval_millis: Option<u64>,
    /// The training interval millis.
    pub training_interval_millis: Option<u64>,
}

impl Default for BootstrapEmbeddedDaemonConfig {
    fn default() -> Self {
        Self {
            node: NodeConfig {
                identity: IdentityConfig::Persistent,
                storage: Some(StorageConfig::new(".burn_p2p-bootstrap")),
                dataset: None,
                auth: None,
                network_manifest: None,
                client_release_manifest: None,
                selected_workload_id: None,
                metrics_retention: MetricsRetentionConfig::default(),
                bootstrap_peers: Vec::new(),
                listen_addresses: Vec::new(),
                external_addresses: Vec::new(),
            },
            active_experiment: ActiveExperiment {
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("experiment"),
                revision_id: RevisionId::new("revision"),
            },
            initialize_head_on_start: true,
            restore_head_on_start: true,
            validation_interval_millis: 250,
            reducer_interval_millis: None,
            training_interval_millis: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Configures a cheap swarm-only bootstrap peer.
pub struct BootstrapPeerDaemonConfig {
    /// The node.
    pub node: NodeConfig,
}

impl Default for BootstrapPeerDaemonConfig {
    fn default() -> Self {
        Self {
            node: NodeConfig {
                identity: IdentityConfig::Persistent,
                storage: Some(StorageConfig::new(".burn_p2p-bootstrap-peer")),
                dataset: None,
                auth: None,
                network_manifest: None,
                client_release_manifest: None,
                selected_workload_id: None,
                metrics_retention: MetricsRetentionConfig {
                    preset: MetricsRetentionPreset::PeerLean,
                    ..MetricsRetentionConfig::default()
                },
                bootstrap_peers: Vec::new(),
                listen_addresses: Vec::new(),
                external_addresses: Vec::new(),
            },
        }
    }
}

/// Represents a swarm-only bootstrap peer daemon.
pub struct BootstrapPeerDaemon {
    plan: BootstrapPlan,
    admin_state: Arc<Mutex<BootstrapAdminState>>,
    telemetry: TelemetryHandle,
    control: ControlHandle,
    shutdown_requested: Arc<AtomicBool>,
    worker: Option<JoinHandle<anyhow::Result<()>>>,
}

impl std::fmt::Debug for BootstrapPeerDaemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BootstrapPeerDaemon")
            .field("network_id", &self.plan.network_id())
            .finish_non_exhaustive()
    }
}

impl BootstrapPeerDaemon {
    /// Performs the plan operation.
    pub fn plan(&self) -> &BootstrapPlan {
        &self.plan
    }

    /// Performs the telemetry operation.
    pub fn telemetry(&self) -> TelemetryHandle {
        self.telemetry.clone()
    }

    /// Performs the control handle operation.
    pub fn control_handle(&self) -> ControlHandle {
        self.control.clone()
    }

    /// Performs the admin state operation.
    pub fn admin_state(&self) -> Arc<Mutex<BootstrapAdminState>> {
        Arc::clone(&self.admin_state)
    }

    /// Performs the diagnostics operation.
    pub fn diagnostics(&self, remaining_work_units: Option<u64>) -> BootstrapDiagnostics {
        self.admin_state
            .lock()
            .expect("bootstrap peer state should not be poisoned")
            .diagnostics(&self.plan, Utc::now(), remaining_work_units)
    }

    /// Performs the diagnostics bundle operation.
    pub fn diagnostics_bundle(
        &self,
        remaining_work_units: Option<u64>,
    ) -> BootstrapDiagnosticsBundle {
        self.admin_state
            .lock()
            .expect("bootstrap peer state should not be poisoned")
            .diagnostics_bundle(&self.plan, Utc::now(), remaining_work_units)
    }

    /// Performs the shutdown operation.
    pub fn shutdown(&self) -> anyhow::Result<()> {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        let _ = self.control.shutdown();
        Ok(())
    }

    /// Performs the await termination operation.
    pub fn await_termination(self) -> anyhow::Result<()> {
        let BootstrapPeerDaemon {
            plan: _plan,
            admin_state,
            telemetry,
            control,
            shutdown_requested,
            worker,
        } = self;
        drop(control);
        drop(telemetry);
        drop(admin_state);
        drop(shutdown_requested);

        if let Some(worker) = worker {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("bootstrap peer daemon panicked"))??;
        }
        Ok(())
    }
}

/// Represents a bootstrap embedded daemon.
pub struct BootstrapEmbeddedDaemon {
    plan: BootstrapPlan,
    admin_state: Arc<Mutex<BootstrapAdminState>>,
    telemetry: TelemetryHandle,
    control: ControlHandle,
    shutdown_requested: Arc<AtomicBool>,
    worker: Option<JoinHandle<anyhow::Result<()>>>,
}

impl std::fmt::Debug for BootstrapEmbeddedDaemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BootstrapEmbeddedDaemon")
            .field("network_id", &self.plan.network_id())
            .finish_non_exhaustive()
    }
}

impl BootstrapEmbeddedDaemon {
    /// Performs the plan operation.
    pub fn plan(&self) -> &BootstrapPlan {
        &self.plan
    }

    /// Performs the telemetry operation.
    pub fn telemetry(&self) -> TelemetryHandle {
        self.telemetry.clone()
    }

    /// Performs the control handle operation.
    pub fn control_handle(&self) -> ControlHandle {
        self.control.clone()
    }

    /// Performs the admin state operation.
    pub fn admin_state(&self) -> Arc<Mutex<BootstrapAdminState>> {
        Arc::clone(&self.admin_state)
    }

    /// Performs the diagnostics operation.
    pub fn diagnostics(&self, remaining_work_units: Option<u64>) -> BootstrapDiagnostics {
        self.admin_state
            .lock()
            .expect("bootstrap embedded state should not be poisoned")
            .diagnostics(&self.plan, Utc::now(), remaining_work_units)
    }

    /// Performs the diagnostics bundle operation.
    pub fn diagnostics_bundle(
        &self,
        remaining_work_units: Option<u64>,
    ) -> BootstrapDiagnosticsBundle {
        self.admin_state
            .lock()
            .expect("bootstrap embedded state should not be poisoned")
            .diagnostics_bundle(&self.plan, Utc::now(), remaining_work_units)
    }

    /// Performs the shutdown operation.
    pub fn shutdown(&self) -> anyhow::Result<()> {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        let _ = self.control.shutdown();
        Ok(())
    }

    /// Performs the await termination operation.
    pub fn await_termination(self) -> anyhow::Result<()> {
        let BootstrapEmbeddedDaemon {
            plan: _plan,
            admin_state,
            telemetry,
            control,
            shutdown_requested,
            worker,
        } = self;
        drop(control);
        drop(telemetry);
        drop(admin_state);
        drop(shutdown_requested);

        if let Some(worker) = worker {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("bootstrap embedded daemon panicked"))??;
        }
        Ok(())
    }
}

impl BootstrapPlan {
    /// Performs the spawn bootstrap peer daemon operation.
    pub fn spawn_bootstrap_peer_daemon(
        &self,
        config: BootstrapPeerDaemonConfig,
    ) -> anyhow::Result<BootstrapPeerDaemon> {
        let mut builder = NodeBuilder::new(())
            .with_mainnet(self.genesis.clone())
            .with_roles(self.roles.clone());
        builder = apply_runtime_node_config(builder, self, &config.node);
        let running = builder.spawn()?;
        let telemetry = running.telemetry();
        let control = running.control_handle();
        let admin_state = Arc::new(Mutex::new(BootstrapAdminState::default()));
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let plan = self.clone();
        let admin_state_thread = Arc::clone(&admin_state);
        let shutdown_requested_thread = Arc::clone(&shutdown_requested);

        let worker = thread::Builder::new()
            .name("burn-p2p-bootstrap-peer".into())
            .spawn(move || {
                bootstrap_peer_daemon_loop(
                    plan,
                    running,
                    admin_state_thread,
                    shutdown_requested_thread,
                )
            })
            .map_err(|error| anyhow::anyhow!("failed to spawn bootstrap peer worker: {error}"))?;

        Ok(BootstrapPeerDaemon {
            plan: self.clone(),
            admin_state,
            telemetry,
            control,
            shutdown_requested,
            worker: Some(worker),
        })
    }

    /// Performs the spawn embedded daemon operation.
    pub fn spawn_embedded_daemon<P>(
        &self,
        project: P,
        config: BootstrapEmbeddedDaemonConfig,
    ) -> anyhow::Result<BootstrapEmbeddedDaemon>
    where
        P: P2pWorkload + Send + 'static,
        P::Model: Send + 'static,
    {
        let mut builder = NodeBuilder::new(project)
            .with_mainnet(self.genesis.clone())
            .with_roles(self.roles.clone());
        builder = apply_runtime_node_config(builder, self, &config.node);
        let running = builder.spawn()?;
        let telemetry = running.telemetry();
        let control = running.control_handle();
        let admin_state = Arc::new(Mutex::new(BootstrapAdminState::default()));
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let plan = self.clone();
        let admin_state_thread = Arc::clone(&admin_state);
        let shutdown_requested_thread = Arc::clone(&shutdown_requested);

        let worker = thread::Builder::new()
            .name("burn-p2p-bootstrap-embedded".into())
            .spawn(move || {
                embedded_daemon_loop::<P>(
                    plan,
                    config,
                    running,
                    admin_state_thread,
                    shutdown_requested_thread,
                )
            })
            .map_err(|error| anyhow::anyhow!("failed to spawn bootstrap worker: {error}"))?;

        Ok(BootstrapEmbeddedDaemon {
            plan: self.clone(),
            admin_state,
            telemetry,
            control,
            shutdown_requested,
            worker: Some(worker),
        })
    }
}

fn apply_runtime_node_config<P>(
    builder: NodeBuilder<P>,
    plan: &BootstrapPlan,
    config: &NodeConfig,
) -> NodeBuilder<P> {
    let builder = builder.with_identity(config.identity.clone());
    let builder = match config.storage.clone() {
        Some(storage) => builder.with_storage(storage),
        None => builder,
    };
    let builder = match config.dataset.clone() {
        Some(dataset) => builder.with_dataset(dataset),
        None => builder,
    };
    builder
        .with_metrics_retention(config.metrics_retention.clone())
        .with_bootstrap_peers(if config.bootstrap_peers.is_empty() {
            plan.runtime.bootstrap_addresses.clone()
        } else {
            config.bootstrap_peers.clone()
        })
        .with_listen_addresses(if config.listen_addresses.is_empty() {
            plan.runtime.listen_addresses.clone()
        } else {
            config.listen_addresses.clone()
        })
        .with_external_addresses(config.external_addresses.clone())
}

fn bootstrap_peer_daemon_loop(
    plan: BootstrapPlan,
    running: RunningNode<()>,
    admin_state: Arc<Mutex<BootstrapAdminState>>,
    shutdown_requested: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    wait_for_runtime_ready(&running.telemetry(), Duration::from_secs(5))?;

    loop {
        if shutdown_requested.load(Ordering::SeqCst) {
            break;
        }

        {
            let snapshot = running.telemetry().snapshot();
            let mut state = admin_state
                .lock()
                .expect("bootstrap peer state should not be poisoned");
            refresh_admin_state_from_runtime(&mut state, &snapshot, running.config())?;
        }

        thread::sleep(Duration::from_millis(50));
    }

    running.shutdown()?;
    let _ = running.await_termination()?;
    let _ = plan;
    Ok(())
}

fn embedded_daemon_loop<P>(
    plan: BootstrapPlan,
    config: BootstrapEmbeddedDaemonConfig,
    mut running: RunningNode<P>,
    admin_state: Arc<Mutex<BootstrapAdminState>>,
    shutdown_requested: Arc<AtomicBool>,
) -> anyhow::Result<()>
where
    P: P2pWorkload,
    P::Model: Send + 'static,
{
    let experiment = config.active_experiment.handle(running.mainnet());
    let validation_interval = Duration::from_millis(config.validation_interval_millis.max(25));
    let reducer_interval = config
        .reducer_interval_millis
        .map(|millis| Duration::from_millis(millis.max(25)))
        .unwrap_or(validation_interval);
    let training_interval = config
        .training_interval_millis
        .map(|millis| Duration::from_millis(millis.max(25)));
    let mut last_reduction = Instant::now()
        .checked_sub(reducer_interval)
        .unwrap_or_else(Instant::now);
    let mut last_validation = Instant::now()
        .checked_sub(validation_interval)
        .unwrap_or_else(Instant::now);
    let mut last_training = training_interval.map(|interval| {
        Instant::now()
            .checked_sub(interval)
            .unwrap_or_else(Instant::now)
    });

    wait_for_runtime_ready(&running.telemetry(), Duration::from_secs(5))?;

    if config.restore_head_on_start && running.restore_experiment_head(&experiment)?.is_none() {
        if config.initialize_head_on_start {
            let _ = running.initialize_local_head(&experiment)?;
        }
    } else if config.initialize_head_on_start
        && crate::load_operator_history(
            running.config().storage.as_ref(),
            running
                .config()
                .metrics_retention
                .resolve_for_roles(&running.mainnet().roles),
            crate::history::OperatorHistoryMemoryBudget::default(),
        )?
        .receipts
        .is_empty()
    {
        let _ = running.initialize_local_head(&experiment)?;
    }

    loop {
        if shutdown_requested.load(Ordering::SeqCst) {
            break;
        }

        if let Some(interval) = training_interval
            && last_training
                .as_ref()
                .is_some_and(|instant| instant.elapsed() >= interval)
        {
            let _ = running.train_window_once(&experiment)?;
            last_training = Some(Instant::now());
        }

        if plan.supports_service(&BootstrapService::Reducer)
            && last_reduction.elapsed() >= reducer_interval
        {
            let _ = running.reduce_candidates_once(&experiment)?;
            last_reduction = Instant::now();
        }

        if plan.supports_service(&BootstrapService::Validator)
            && last_validation.elapsed() >= validation_interval
        {
            let _ = running.validate_candidates_once(&experiment)?;
            last_validation = Instant::now();
        }

        {
            let snapshot = running.telemetry().snapshot();
            let mut state = admin_state
                .lock()
                .expect("bootstrap embedded state should not be poisoned");
            refresh_admin_state_from_runtime(&mut state, &snapshot, running.config())?;
        }

        thread::sleep(Duration::from_millis(50));
    }

    running.shutdown()?;
    let _ = running.await_termination()?;
    Ok(())
}

fn refresh_admin_state_from_runtime(
    state: &mut BootstrapAdminState,
    telemetry: &NodeTelemetrySnapshot,
    config: &NodeConfig,
) -> anyhow::Result<()> {
    let observed_at = telemetry.updated_at;
    state.node_state = telemetry.node_state.clone();
    state.slot_states = telemetry.slot_states.clone();
    state.in_flight_transfers = telemetry.in_flight_transfers.values().cloned().collect();
    state.admitted_peers = telemetry.admitted_peers.keys().cloned().collect();
    state.peer_admission_reports = telemetry.admitted_peers.clone();
    state.rejected_peers = telemetry.rejected_peers.clone();
    state.peer_reputation = telemetry.peer_reputation.clone();
    state.minimum_revocation_epoch = telemetry.minimum_revocation_epoch;
    state.last_error = telemetry.last_error.clone();
    state.runtime_snapshot = Some(telemetry.clone());
    state.reducer_load_announcements = telemetry.control_plane.reducer_load_announcements.clone();
    if let Some(local_peer_id) = telemetry.local_peer_id.clone() {
        state
            .peer_store
            .mark_connection(local_peer_id, true, observed_at);
    }

    if let Some(snapshot_peer_id) = telemetry.last_snapshot_peer_id.clone() {
        state
            .peer_store
            .mark_connection(snapshot_peer_id, true, observed_at);
    }

    for event in &telemetry.recent_events {
        if let Some(peer_id) = observed_peer_id_from_event(event) {
            state.peer_store.mark_connection(peer_id, true, observed_at);
        }
    }

    state.metrics_retention = config
        .metrics_retention
        .resolve_for_roles(&telemetry.configured_roles);
    state.history_root = config.storage.as_ref().map(|storage| storage.root.clone());
    state.artifact_store_root = config.storage.as_ref().map(|storage| storage.root.clone());
    state.publication_store_root = config.storage.as_ref().map(StorageConfig::publication_dir);
    state.metrics_store_root = config
        .storage
        .as_ref()
        .map(StorageConfig::metrics_indexer_dir);

    if let Some(storage) = config.storage.as_ref() {
        let history = crate::load_operator_history(
            Some(storage),
            state.metrics_retention,
            crate::history::OperatorHistoryMemoryBudget::default(),
        )?;
        state.head_descriptors = history.heads;
        state.contribution_receipts = history.receipts;
        state.merge_certificates = history.merges;
        state.persisted_head_count = history.totals.head_count;
        state.persisted_receipt_count = history.totals.receipt_count;
        state.persisted_merge_count = history.totals.merge_count;
        state.peer_window_metrics = history.peer_window_metrics;
        state.reducer_cohort_metrics = history.reducer_cohort_metrics;
        state.head_eval_reports = history.head_eval_reports;
        state.eval_protocol_manifests = history.eval_protocol_manifests;
        #[cfg(feature = "metrics-indexer")]
        state.sync_metrics_store()?;
        #[cfg(feature = "artifact-publish")]
        if let Err(error) = state.sync_publication_store() {
            state.last_error = Some(format!("artifact publication sync failed: {error}"));
        }
    }

    if let Err(error) = state.persist_operator_state_snapshot() {
        state.last_error = Some(format!("operator state snapshot sync failed: {error}"));
    }

    Ok(())
}

fn observed_peer_id_from_event(event: &LiveControlPlaneEvent) -> Option<PeerId> {
    match event {
        LiveControlPlaneEvent::ConnectionEstablished { peer_id }
        | LiveControlPlaneEvent::SnapshotRequested { peer_id }
        | LiveControlPlaneEvent::SnapshotReceived { peer_id, .. }
        | LiveControlPlaneEvent::ArtifactManifestRequested { peer_id, .. }
        | LiveControlPlaneEvent::ArtifactManifestReceived { peer_id, .. }
        | LiveControlPlaneEvent::ArtifactChunkRequested { peer_id, .. }
        | LiveControlPlaneEvent::ArtifactChunkReceived { peer_id, .. }
        | LiveControlPlaneEvent::SnapshotResponseSent { peer_id }
        | LiveControlPlaneEvent::PubsubMessage { peer_id, .. }
        | LiveControlPlaneEvent::PeerIdentified { peer_id, .. }
        | LiveControlPlaneEvent::RequestFailure { peer_id, .. }
        | LiveControlPlaneEvent::InboundFailure { peer_id, .. }
        | LiveControlPlaneEvent::ResponseSendFailure { peer_id, .. }
        | LiveControlPlaneEvent::DirectConnectionUpgradeSucceeded { peer_id }
        | LiveControlPlaneEvent::DirectConnectionUpgradeFailed { peer_id, .. } => {
            Some(PeerId::new(peer_id.clone()))
        }
        LiveControlPlaneEvent::PeerDirectoryRecordReceived { announcement } => {
            Some(announcement.peer_id.clone())
        }
        LiveControlPlaneEvent::RelayReservationAccepted { relay_peer_id } => {
            Some(PeerId::new(relay_peer_id.clone()))
        }
        LiveControlPlaneEvent::PeersDiscovered { peers }
        | LiveControlPlaneEvent::PeersExpired { peers } => peers
            .first()
            .map(|(peer_id, _)| PeerId::new(peer_id.clone())),
        LiveControlPlaneEvent::OutgoingConnectionError {
            peer_id: Some(peer_id),
            ..
        } => Some(PeerId::new(peer_id.clone())),
        LiveControlPlaneEvent::ConnectionClosed { .. } => None,
        LiveControlPlaneEvent::NewListenAddr { .. }
        | LiveControlPlaneEvent::ReachableAddressConfirmed { .. }
        | LiveControlPlaneEvent::ReachableAddressExpired { .. }
        | LiveControlPlaneEvent::TopicSubscribed { .. }
        | LiveControlPlaneEvent::IncomingConnectionError { .. }
        | LiveControlPlaneEvent::OutgoingConnectionError { peer_id: None, .. }
        | LiveControlPlaneEvent::Other { .. } => None,
    }
}

fn wait_for_runtime_ready(telemetry: &TelemetryHandle, timeout: Duration) -> anyhow::Result<()> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        let snapshot = telemetry.snapshot();
        if snapshot.local_peer_id.is_some() && !snapshot.listen_addresses.is_empty() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }

    Err(anyhow::anyhow!(
        "runtime did not become ready within {}ms",
        timeout.as_millis()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn_p2p::{ClientPlatform, GenesisSpec};
    use burn_p2p_core::NetworkId;
    use semver::Version;

    fn test_plan() -> BootstrapPlan {
        let genesis = GenesisSpec {
            network_id: NetworkId::new("test-net"),
            protocol_version: Version::new(1, 0, 0),
            display_name: "test".into(),
            created_at: Utc::now(),
            metadata: std::collections::BTreeMap::new(),
        };
        let roles = crate::BootstrapPreset::BootstrapOnly.roles();
        BootstrapPlan {
            preset: crate::BootstrapPreset::BootstrapOnly,
            genesis: genesis.clone(),
            services: crate::BootstrapPreset::BootstrapOnly.services(),
            roles: roles.clone(),
            runtime: burn_p2p_swarm::RuntimeBoundary::for_platform_and_roles(
                &genesis,
                ClientPlatform::Native,
                &roles,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                None,
            )
            .expect("runtime"),
            telemetry: crate::TelemetryExportPlan {
                openmetrics_enabled: true,
            },
            authority: None,
            archive: crate::ArchivePlan::default(),
            admin_api: crate::AdminApiPlan::default(),
        }
    }

    #[test]
    fn apply_runtime_node_config_preserves_external_addresses() {
        let plan = test_plan();
        let external = burn_p2p::SwarmAddress::new("/dns4/bootstrap.example/udp/4003/webrtc-direct")
            .expect("external");
        let config = NodeConfig {
            external_addresses: vec![external.clone()],
            ..NodeConfig::default()
        };

        let builder = apply_runtime_node_config(burn_p2p::NodeBuilder::new(()), &plan, &config);

        assert_eq!(builder.config().external_addresses, vec![external]);
    }
}
