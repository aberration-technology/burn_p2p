#[cfg(feature = "artifact-publish")]
use std::collections::{BTreeMap, BTreeSet};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[cfg(feature = "artifact-publish")]
use crate::artifact_mirror::mirror_peer_artifact;
use crate::{
    BootstrapAdminState, BootstrapDiagnostics, BootstrapDiagnosticsBundle, BootstrapPlan,
    BootstrapService,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p::{
    ArtifactChunkPayload, ArtifactDescriptor, ArtifactId, ControlPlaneSnapshot, FsArtifactStore,
    HeadAnnouncement,
};
use burn_p2p::{
    ControlHandle, ExperimentHandle, IdentityConfig, LiveControlPlaneEvent, MetricsRetentionConfig,
    MetricsRetentionPreset, NodeBuilder, NodeConfig, NodeTelemetrySnapshot, P2pWorkload,
    RunningNode, StorageConfig, TelemetryHandle,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p_core::HeadId;
use burn_p2p_core::{ExperimentId, PeerId, RevisionId, StudyId};
#[cfg(feature = "artifact-publish")]
use burn_p2p_publish::PeerArtifactMirrorRequest;
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
    /// Extra filesystem artifact stores that may seed the bootstrap's P2P
    /// provider mirror for the active head.
    #[serde(default)]
    pub head_artifact_mirror_source_roots: Vec<PathBuf>,
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
            head_artifact_mirror_source_roots: Vec::new(),
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
        let controlled_shutdown = shutdown_requested.load(Ordering::SeqCst);
        drop(control);
        drop(telemetry);
        drop(admin_state);
        drop(shutdown_requested);

        if let Some(worker) = worker {
            let result = worker
                .join()
                .map_err(|_| anyhow::anyhow!("bootstrap peer daemon panicked"))?;
            if let Err(error) = result {
                if controlled_shutdown && is_controlled_shutdown_runtime_error(&error) {
                    return Ok(());
                }
                return Err(error);
            }
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
        let controlled_shutdown = shutdown_requested.load(Ordering::SeqCst);
        drop(control);
        drop(telemetry);
        drop(admin_state);
        drop(shutdown_requested);

        if let Some(worker) = worker {
            let result = worker
                .join()
                .map_err(|_| anyhow::anyhow!("bootstrap embedded daemon panicked"))?;
            if let Err(error) = result {
                if controlled_shutdown && is_controlled_shutdown_runtime_error(&error) {
                    return Ok(());
                }
                return Err(error);
            }
        }
        Ok(())
    }
}

fn is_controlled_shutdown_runtime_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}");
    message.contains("sending on a closed channel") || message.contains("reply channel closed")
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
                    config,
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

#[cfg(feature = "artifact-publish")]
const BOOTSTRAP_HEAD_ARTIFACT_MIRROR_RETRY_INTERVAL: Duration = Duration::from_secs(15);
#[cfg(feature = "artifact-publish")]
const BOOTSTRAP_HEAD_ARTIFACT_MIRROR_TIMEOUT_MS: u64 = 5 * 60 * 1000;

#[cfg(feature = "artifact-publish")]
#[derive(Debug, Default)]
struct BootstrapHeadArtifactMirror {
    last_attempt_at: BTreeMap<String, Instant>,
    mirrored_head_artifacts: BTreeSet<String>,
    source_roots: Vec<PathBuf>,
}

#[cfg(feature = "artifact-publish")]
impl BootstrapHeadArtifactMirror {
    fn new(source_roots: Vec<PathBuf>) -> Self {
        Self {
            source_roots,
            ..Self::default()
        }
    }

    fn mirror_current_heads(
        &mut self,
        running: &RunningNode<()>,
        snapshot: &NodeTelemetrySnapshot,
    ) -> anyhow::Result<Option<String>> {
        let Some(local_peer_id) = snapshot.local_peer_id.as_ref() else {
            return Ok(None);
        };
        let control = running.control_handle();

        for (announcement, provider_peer_ids) in bootstrap_head_artifact_mirror_candidates(
            &snapshot.control_plane,
            local_peer_id,
            &snapshot.connected_peer_ids,
        ) {
            let key = head_artifact_mirror_key(&announcement);
            if self.mirrored_head_artifacts.contains(&key) {
                continue;
            }
            if self
                .last_attempt_at
                .get(&key)
                .is_some_and(|last| last.elapsed() < BOOTSTRAP_HEAD_ARTIFACT_MIRROR_RETRY_INTERVAL)
            {
                continue;
            }
            self.last_attempt_at.insert(key.clone(), Instant::now());

            match publish_local_head_artifact(running, &announcement, &self.source_roots) {
                Ok(Some(message)) => {
                    self.mirrored_head_artifacts.insert(key);
                    eprintln!("{message}");
                    return Ok(Some(message));
                }
                Ok(None) => {}
                Err(error) => {
                    eprintln!(
                        "bootstrap-local-head-artifact-publish-unavailable head={} artifact={}: {error:#}",
                        announcement.head.head_id.as_str(),
                        announcement.head.artifact_id.as_str()
                    );
                }
            }

            eprintln!(
                "bootstrap-head-artifact-mirror-start head={} artifact={} providers={:?}",
                announcement.head.head_id.as_str(),
                announcement.head.artifact_id.as_str(),
                provider_peer_ids
                    .iter()
                    .map(|peer_id| peer_id.as_str())
                    .collect::<Vec<_>>()
            );
            let response = mirror_peer_artifact(
                &control,
                PeerArtifactMirrorRequest {
                    artifact_id: announcement.head.artifact_id.clone(),
                    provider_peer_ids,
                    timeout_ms: Some(BOOTSTRAP_HEAD_ARTIFACT_MIRROR_TIMEOUT_MS),
                },
            )
            .map_err(|error| {
                anyhow::anyhow!(
                    "bootstrap head artifact mirror failed for head {} artifact {}: {error}",
                    announcement.head.head_id.as_str(),
                    announcement.head.artifact_id.as_str()
                )
            })?;
            let mirrored_provider = response
                .mirrored_provider_peer_id
                .or_else(|| control.local_peer_id())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "bootstrap head artifact mirror finished without a local provider peer id"
                    )
                })?;
            control.publish_head(HeadAnnouncement {
                overlay: announcement.overlay.clone(),
                provider_peer_id: Some(mirrored_provider.clone()),
                head: announcement.head.clone(),
                announced_at: Utc::now(),
            })?;
            self.mirrored_head_artifacts.insert(key);
            let message = format!(
                "bootstrap head artifact mirror complete head={} artifact={} provider={} bytes={} chunks={}",
                announcement.head.head_id.as_str(),
                response.artifact_id.as_str(),
                mirrored_provider.as_str(),
                response.bytes_len,
                response.chunk_count
            );
            eprintln!("{message}");
            return Ok(Some(message));
        }

        Ok(None)
    }
}

#[cfg(feature = "artifact-publish")]
fn publish_local_head_artifact(
    running: &RunningNode<()>,
    announcement: &HeadAnnouncement,
    source_roots: &[PathBuf],
) -> anyhow::Result<Option<String>> {
    let artifact_id = &announcement.head.artifact_id;
    let published = if local_artifact_store_has_complete_artifact(running, artifact_id)? {
        Some((
            "local".to_owned(),
            running.publish_artifact_from_store(artifact_id)?,
        ))
    } else {
        publish_artifact_from_source_roots(running.control_handle(), artifact_id, source_roots)?
    };
    let Some((source_label, descriptor)) = published else {
        return Ok(None);
    };

    let control = running.control_handle();
    let local_peer_id = control.local_peer_id().ok_or_else(|| {
        anyhow::anyhow!("bootstrap local head artifact publish finished without a local peer id")
    })?;
    control.publish_head(HeadAnnouncement {
        overlay: announcement.overlay.clone(),
        provider_peer_id: Some(local_peer_id.clone()),
        head: announcement.head.clone(),
        announced_at: Utc::now(),
    })?;

    Ok(Some(format!(
        "bootstrap local head artifact published head={} artifact={} provider={} source={} bytes={} chunks={}",
        announcement.head.head_id.as_str(),
        descriptor.artifact_id.as_str(),
        local_peer_id.as_str(),
        source_label,
        descriptor.bytes_len,
        descriptor.chunks.len()
    )))
}

#[cfg(feature = "artifact-publish")]
fn publish_artifact_from_source_roots(
    control: ControlHandle,
    artifact_id: &ArtifactId,
    source_roots: &[PathBuf],
) -> anyhow::Result<Option<(String, ArtifactDescriptor)>> {
    for root in source_roots {
        let store = FsArtifactStore::new(root.clone());
        match store.has_complete_artifact(artifact_id) {
            Ok(true) => {
                let descriptor = publish_artifact_from_fs_store(&control, &store, artifact_id)?;
                return Ok(Some((root.display().to_string(), descriptor)));
            }
            Ok(false) => {}
            Err(error) => {
                eprintln!(
                    "bootstrap-head-artifact-source-root-unavailable root={} artifact={}: {error}",
                    root.display(),
                    artifact_id.as_str()
                );
            }
        }
    }
    Ok(None)
}

#[cfg(feature = "artifact-publish")]
fn publish_artifact_from_fs_store(
    control: &ControlHandle,
    store: &FsArtifactStore,
    artifact_id: &ArtifactId,
) -> anyhow::Result<ArtifactDescriptor> {
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
        .collect::<anyhow::Result<Vec<_>>>()?;
    control.publish_artifact(descriptor.clone(), chunks)?;
    Ok(descriptor)
}

#[cfg(feature = "artifact-publish")]
fn local_artifact_store_has_complete_artifact(
    running: &RunningNode<()>,
    artifact_id: &ArtifactId,
) -> anyhow::Result<bool> {
    let Some(store) = running.artifact_store() else {
        return Ok(false);
    };
    Ok(store.has_complete_artifact(artifact_id)?)
}

#[cfg(feature = "artifact-publish")]
fn bootstrap_head_artifact_mirror_candidates(
    control_plane: &ControlPlaneSnapshot,
    local_peer_id: &PeerId,
    connected_peer_ids: &BTreeSet<PeerId>,
) -> Vec<(HeadAnnouncement, Vec<PeerId>)> {
    let current_head_ids = current_directory_head_ids(control_plane);
    let locally_provided_head_ids = control_plane
        .head_announcements
        .iter()
        .filter(|announcement| announcement.provider_peer_id.as_ref() == Some(local_peer_id))
        .map(|announcement| announcement.head.head_id.clone())
        .collect::<BTreeSet<_>>();

    let mut candidates = BTreeMap::<HeadId, (HeadAnnouncement, Vec<PeerId>)>::new();
    for announcement in &control_plane.head_announcements {
        if !current_head_ids.is_empty() && !current_head_ids.contains(&announcement.head.head_id) {
            continue;
        }
        if locally_provided_head_ids.contains(&announcement.head.head_id) {
            continue;
        }
        let Some(provider_peer_id) = announcement.provider_peer_id.as_ref() else {
            continue;
        };
        if provider_peer_id == local_peer_id {
            continue;
        }

        let entry = candidates
            .entry(announcement.head.head_id.clone())
            .or_insert_with(|| (announcement.clone(), Vec::new()));
        if announcement.announced_at > entry.0.announced_at {
            entry.0 = announcement.clone();
        }
        if !entry.1.iter().any(|peer_id| peer_id == provider_peer_id) {
            entry.1.push(provider_peer_id.clone());
        }
    }

    let mut candidates = candidates
        .into_values()
        .filter_map(|(announcement, provider_peer_ids)| {
            prioritize_head_artifact_mirror_providers(provider_peer_ids, connected_peer_ids)
                .map(|provider_peer_ids| (announcement, provider_peer_ids))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.0.head.created_at));
    candidates
}

#[cfg(feature = "artifact-publish")]
fn prioritize_head_artifact_mirror_providers(
    provider_peer_ids: Vec<PeerId>,
    connected_peer_ids: &BTreeSet<PeerId>,
) -> Option<Vec<PeerId>> {
    if provider_peer_ids.is_empty() {
        return None;
    }
    if connected_peer_ids.is_empty() {
        return Some(provider_peer_ids);
    }

    let connected = provider_peer_ids
        .iter()
        .filter(|peer_id| connected_peer_ids.contains(*peer_id))
        .cloned()
        .collect::<Vec<_>>();
    if connected.is_empty() {
        Some(provider_peer_ids)
    } else {
        Some(connected)
    }
}

#[cfg(feature = "artifact-publish")]
fn current_directory_head_ids(control_plane: &ControlPlaneSnapshot) -> BTreeSet<HeadId> {
    control_plane
        .directory_announcements
        .iter()
        .flat_map(|announcement| announcement.entries.iter())
        .filter_map(|entry| entry.current_head_id.clone())
        .collect()
}

#[cfg(feature = "artifact-publish")]
fn head_artifact_mirror_key(announcement: &HeadAnnouncement) -> String {
    format!(
        "{}:{}",
        announcement.head.head_id.as_str(),
        announcement.head.artifact_id.as_str()
    )
}

fn bootstrap_peer_daemon_loop(
    plan: BootstrapPlan,
    config: BootstrapPeerDaemonConfig,
    running: RunningNode<()>,
    admin_state: Arc<Mutex<BootstrapAdminState>>,
    shutdown_requested: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    wait_for_runtime_ready(&running.telemetry(), Duration::from_secs(5))?;
    #[cfg(not(feature = "artifact-publish"))]
    let _ = &config;
    #[cfg(feature = "artifact-publish")]
    let mut head_artifact_mirror =
        BootstrapHeadArtifactMirror::new(config.head_artifact_mirror_source_roots.clone());

    loop {
        if shutdown_requested.load(Ordering::SeqCst) {
            break;
        }

        #[cfg(feature = "artifact-publish")]
        let mut head_artifact_mirror_error = None;

        {
            let snapshot = running.telemetry().snapshot();
            #[cfg(feature = "artifact-publish")]
            {
                if let Err(error) = head_artifact_mirror.mirror_current_heads(&running, &snapshot) {
                    let error = format!("{error:#}");
                    eprintln!("bootstrap-head-artifact-mirror-error: {error}");
                    head_artifact_mirror_error = Some(error);
                }
            }
            let mut state = admin_state
                .lock()
                .expect("bootstrap peer state should not be poisoned");
            refresh_admin_state_from_runtime(&mut state, &snapshot, running.config())?;
            #[cfg(feature = "artifact-publish")]
            if let Some(error) = head_artifact_mirror_error {
                state.last_error = Some(error);
            }
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
        let external =
            burn_p2p::SwarmAddress::new("/dns4/bootstrap.example/udp/4003/webrtc-direct")
                .expect("external");
        let config = NodeConfig {
            external_addresses: vec![external.clone()],
            ..NodeConfig::default()
        };

        let builder = apply_runtime_node_config(burn_p2p::NodeBuilder::new(()), &plan, &config);

        assert_eq!(builder.config().external_addresses, vec![external]);
    }

    #[test]
    fn controlled_shutdown_runtime_filter_is_narrow() {
        let closed_channel =
            anyhow::anyhow!("failed to subscribe topic: sending on a closed channel");
        assert!(is_controlled_shutdown_runtime_error(&closed_channel));

        let closed_reply = anyhow::anyhow!("sync runtime reply channel closed");
        assert!(is_controlled_shutdown_runtime_error(&closed_reply));

        let panic = anyhow::anyhow!("runtime thread panicked");
        assert!(!is_controlled_shutdown_runtime_error(&panic));
    }

    #[cfg(feature = "artifact-publish")]
    fn mirror_test_overlay() -> burn_p2p::OverlayTopic {
        burn_p2p::OverlayTopic::experiment(
            NetworkId::new("test-net"),
            StudyId::new("study"),
            ExperimentId::new("experiment"),
            burn_p2p::OverlayChannel::Heads,
        )
        .expect("heads overlay")
    }

    #[cfg(feature = "artifact-publish")]
    fn mirror_test_head(
        head_id: &str,
        artifact_id: &str,
        created_at: chrono::DateTime<Utc>,
    ) -> burn_p2p::HeadDescriptor {
        burn_p2p::HeadDescriptor {
            head_id: HeadId::new(head_id),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            artifact_id: burn_p2p::ArtifactId::new(artifact_id),
            parent_head_id: None,
            global_step: 0,
            created_at,
            metrics: BTreeMap::new(),
        }
    }

    #[cfg(feature = "artifact-publish")]
    fn mirror_test_directory(head_id: &str) -> burn_p2p::ExperimentDirectoryAnnouncement {
        burn_p2p::ExperimentDirectoryAnnouncement {
            network_id: NetworkId::new("test-net"),
            entries: vec![burn_p2p::ExperimentDirectoryEntry {
                network_id: NetworkId::new("test-net"),
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("experiment"),
                workload_id: burn_p2p::WorkloadId::new("workload"),
                display_name: "experiment".into(),
                model_schema_hash: burn_p2p::ContentId::new("schema"),
                dataset_view_id: burn_p2p::DatasetViewId::new("dataset"),
                resource_requirements: burn_p2p::ExperimentResourceRequirements {
                    minimum_roles: BTreeSet::new(),
                    minimum_device_memory_bytes: None,
                    minimum_system_memory_bytes: None,
                    estimated_download_bytes: 0,
                    estimated_window_seconds: 0,
                },
                visibility: burn_p2p::ExperimentVisibility::Public,
                opt_in_policy: burn_p2p::ExperimentOptInPolicy::Open,
                current_revision_id: RevisionId::new("revision"),
                current_head_id: Some(HeadId::new(head_id)),
                allowed_roles: burn_p2p::PeerRoleSet::default(),
                allowed_scopes: BTreeSet::new(),
                metadata: BTreeMap::new(),
            }],
            announced_at: Utc::now(),
        }
    }

    #[cfg(feature = "artifact-publish")]
    fn mirror_test_announcement(
        head: burn_p2p::HeadDescriptor,
        provider: &str,
    ) -> HeadAnnouncement {
        HeadAnnouncement {
            overlay: mirror_test_overlay(),
            provider_peer_id: Some(PeerId::new(provider)),
            head,
            announced_at: Utc::now(),
        }
    }

    #[cfg(feature = "artifact-publish")]
    #[test]
    fn bootstrap_head_artifact_mirror_candidates_select_current_nonlocal_providers() {
        let now = Utc::now();
        let current = mirror_test_head("head-current", "artifact-current", now);
        let stale = mirror_test_head(
            "head-stale",
            "artifact-stale",
            now - chrono::Duration::seconds(30),
        );
        let snapshot = ControlPlaneSnapshot {
            directory_announcements: vec![mirror_test_directory("head-current")],
            head_announcements: vec![
                mirror_test_announcement(stale, "provider-stale"),
                mirror_test_announcement(current.clone(), "provider-a"),
                mirror_test_announcement(current.clone(), "provider-b"),
            ],
            ..ControlPlaneSnapshot::default()
        };

        let candidates = bootstrap_head_artifact_mirror_candidates(
            &snapshot,
            &PeerId::new("edge"),
            &BTreeSet::new(),
        );

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0.head.head_id, HeadId::new("head-current"));
        assert_eq!(
            candidates[0].1,
            vec![PeerId::new("provider-a"), PeerId::new("provider-b")]
        );
    }

    #[cfg(feature = "artifact-publish")]
    #[test]
    fn bootstrap_head_artifact_mirror_candidates_skip_local_provider_heads() {
        let now = Utc::now();
        let current = mirror_test_head("head-current", "artifact-current", now);
        let snapshot = ControlPlaneSnapshot {
            directory_announcements: vec![mirror_test_directory("head-current")],
            head_announcements: vec![
                mirror_test_announcement(current.clone(), "provider-a"),
                mirror_test_announcement(current, "edge"),
            ],
            ..ControlPlaneSnapshot::default()
        };

        let candidates = bootstrap_head_artifact_mirror_candidates(
            &snapshot,
            &PeerId::new("edge"),
            &BTreeSet::new(),
        );

        assert!(candidates.is_empty());
    }

    #[cfg(feature = "artifact-publish")]
    #[test]
    fn bootstrap_head_artifact_mirror_candidates_prefer_connected_providers() {
        let now = Utc::now();
        let current = mirror_test_head("head-current", "artifact-current", now);
        let snapshot = ControlPlaneSnapshot {
            directory_announcements: vec![mirror_test_directory("head-current")],
            head_announcements: vec![
                mirror_test_announcement(current.clone(), "provider-stale"),
                mirror_test_announcement(current.clone(), "provider-live"),
                mirror_test_announcement(current, "provider-later-stale"),
            ],
            ..ControlPlaneSnapshot::default()
        };

        let candidates = bootstrap_head_artifact_mirror_candidates(
            &snapshot,
            &PeerId::new("edge"),
            &BTreeSet::from([PeerId::new("provider-live")]),
        );

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0.head.head_id, HeadId::new("head-current"));
        assert_eq!(candidates[0].1, vec![PeerId::new("provider-live")]);
    }
}
