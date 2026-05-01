use burn_p2p::{
    BrowserMode, ContentId, ExperimentDirectoryEntry, ExperimentId, MetricValue, RevisionId,
};
use burn_p2p_metrics::{
    MetricsCatchupBundle, derive_canonical_head_adoption_curves,
    derive_latest_canonical_head_population_histograms,
};
#[cfg(target_arch = "wasm32")]
use burn_p2p_swarm::{
    BrowserArtifactChunkRequest, BrowserArtifactManifestRequest, BrowserSwarmRuntime,
    BrowserSwarmUpdate, ControlPlaneSnapshot, WasmBrowserSwarmRuntime,
};
use burn_p2p_views::{
    BrowserAppClientView, BrowserAppDiffusionView, BrowserAppExperimentSummary,
    BrowserAppLeaderboardPreview, BrowserAppMetricPreview, BrowserAppNetworkView,
    BrowserAppPerformanceView, BrowserAppSurface, BrowserAppTrainingLeaseView,
    BrowserAppTrainingView, BrowserAppValidationView, BrowserAppViewerView,
};
use chrono::Utc;
#[cfg(target_arch = "wasm32")]
use gloo_timers::future::TimeoutFuture;
use serde::{Deserialize, Serialize};

use crate::{
    BrowserAuthClientError, BrowserCapabilityReport, BrowserEdgeClient, BrowserEdgeSnapshot,
    BrowserEnrollmentConfig, BrowserGpuSupport, BrowserRuntimeConfig, BrowserRuntimeRole,
    BrowserRuntimeState, BrowserSeedBootstrapSource, BrowserSessionState, BrowserStorageSnapshot,
    BrowserTransportStatus, BrowserUiBindings, BrowserWorkerCommand, BrowserWorkerEvent,
    BrowserWorkerRuntime,
    durability::{
        clear_durable_receipt_outbox, load_durable_browser_storage, load_durable_receipt_outbox,
        persist_durable_browser_storage, persist_durable_receipt_outbox,
    },
    resolve_browser_seed_bootstrap,
};
#[cfg(target_arch = "wasm32")]
use crate::{
    BrowserPeerArtifactFetchFuture, BrowserPeerArtifactFetcher, BrowserPeerArtifactPayload,
    BrowserPeerArtifactRequest,
};
use burn_p2p_core::{
    BrowserArtifactSource, BrowserSeedAdvertisement, SchemaEnvelope, SignedPayload,
};
#[cfg(any(test, target_arch = "wasm32"))]
use burn_p2p_core::{BrowserSwarmStatus, BrowserTransportFamily};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Selects the browser app target preset.
pub enum BrowserAppTarget {
    /// Viewer-first browser app.
    Viewer,
    /// Observer-first browser app.
    Observe,
    /// Validation-first browser app.
    Validate,
    /// Training-first browser app.
    Train,
    /// Explicit browser runtime role.
    Custom(BrowserRuntimeRole),
}

impl BrowserAppTarget {
    /// Returns the preferred runtime role for the target preset.
    pub fn preferred_role(&self) -> BrowserRuntimeRole {
        match self {
            Self::Viewer => BrowserRuntimeRole::Viewer,
            Self::Observe => BrowserRuntimeRole::BrowserObserver,
            Self::Validate => BrowserRuntimeRole::BrowserVerifier,
            Self::Train => BrowserRuntimeRole::BrowserTrainerWgpu,
            Self::Custom(role) => role.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures browser-app connection with one target-based entry.
pub struct BrowserAppConnectConfig {
    /// The edge base URL.
    pub edge_base_url: String,
    /// Browser capability report used to choose the local runtime mode.
    pub capability: BrowserCapabilityReport,
    /// Target preset requested by the caller.
    pub target: BrowserAppTarget,
    /// Optional selected experiment id.
    pub selected_experiment_id: Option<String>,
    /// Optional selected revision id.
    pub selected_revision_id: Option<String>,
    /// Optional site-config fallback seed urls embedded into the browser artifact.
    pub seed_node_urls: Vec<String>,
    /// Optional baked browser edge snapshot for bootstrap without a live edge fetch.
    #[serde(default)]
    pub bootstrap_snapshot: Option<BrowserEdgeSnapshot>,
    /// Optional baked signed browser seed advertisement for bootstrap without a live edge fetch.
    #[serde(default)]
    pub bootstrap_signed_seed_advertisement:
        Option<SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>>>,
}

impl BrowserAppConnectConfig {
    /// Creates a new browser-app connection config.
    pub fn new(
        edge_base_url: impl Into<String>,
        capability: BrowserCapabilityReport,
        target: BrowserAppTarget,
    ) -> Self {
        Self {
            edge_base_url: edge_base_url.into(),
            capability,
            target,
            selected_experiment_id: None,
            selected_revision_id: None,
            seed_node_urls: Vec::new(),
            bootstrap_snapshot: None,
            bootstrap_signed_seed_advertisement: None,
        }
    }

    /// Creates a viewer-first browser-app connection config.
    pub fn viewer(edge_base_url: impl Into<String>, capability: BrowserCapabilityReport) -> Self {
        Self::new(edge_base_url, capability, BrowserAppTarget::Viewer)
    }

    /// Creates an observer-first browser-app connection config.
    pub fn observe(edge_base_url: impl Into<String>, capability: BrowserCapabilityReport) -> Self {
        Self::new(edge_base_url, capability, BrowserAppTarget::Observe)
    }

    /// Creates a validation-first browser-app connection config.
    pub fn validate(edge_base_url: impl Into<String>, capability: BrowserCapabilityReport) -> Self {
        Self::new(edge_base_url, capability, BrowserAppTarget::Validate)
    }

    /// Creates a training-first browser-app connection config.
    pub fn train(edge_base_url: impl Into<String>, capability: BrowserCapabilityReport) -> Self {
        Self::new(edge_base_url, capability, BrowserAppTarget::Train)
    }

    /// Creates a browser-app connection config with an explicit runtime target.
    pub fn custom(
        edge_base_url: impl Into<String>,
        capability: BrowserCapabilityReport,
        role: BrowserRuntimeRole,
    ) -> Self {
        Self::new(edge_base_url, capability, BrowserAppTarget::Custom(role))
    }

    /// Pins the initial experiment and optional revision selection.
    pub fn with_selection(
        mut self,
        experiment_id: impl Into<String>,
        revision_id: Option<impl Into<String>>,
    ) -> Self {
        self.selected_experiment_id = Some(experiment_id.into());
        self.selected_revision_id = revision_id.map(Into::into);
        self
    }

    /// Adds site-config fallback seeds to the browser connect config.
    pub fn with_seed_node_urls(mut self, seed_node_urls: Vec<String>) -> Self {
        self.seed_node_urls = seed_node_urls;
        self
    }

    /// Adds baked browser edge bootstrap state to the browser connect config.
    pub fn with_bootstrap_material(
        mut self,
        snapshot: Option<BrowserEdgeSnapshot>,
        signed_seed_advertisement: Option<SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>>>,
    ) -> Self {
        self.bootstrap_snapshot = snapshot;
        self.bootstrap_signed_seed_advertisement = signed_seed_advertisement;
        self
    }

    /// Adds a baked browser edge snapshot to the browser connect config.
    pub fn with_bootstrap_snapshot(mut self, snapshot: BrowserEdgeSnapshot) -> Self {
        self.bootstrap_snapshot = Some(snapshot);
        self
    }

    /// Adds a baked signed browser seed advertisement to the browser connect config.
    pub fn with_signed_seed_advertisement(
        mut self,
        signed_seed_advertisement: SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>>,
    ) -> Self {
        self.bootstrap_signed_seed_advertisement = Some(signed_seed_advertisement);
        self
    }

    pub fn selected_experiment(&self) -> Option<(String, Option<String>)> {
        self.selected_experiment_id
            .as_ref()
            .map(|experiment_id| (experiment_id.clone(), self.selected_revision_id.clone()))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
/// Client-owned browser app model for a static wasm UI.
pub struct BrowserAppModel {
    /// Browser worker/runtime state.
    pub runtime: BrowserWorkerRuntime,
    /// Last surfaced local error.
    pub last_error: Option<String>,
    /// Last completed validation outcome.
    pub last_validation: Option<crate::BrowserValidationResult>,
    /// Last completed training outcome.
    pub last_training: Option<crate::BrowserTrainingResult>,
}

fn browser_artifact_source_label(source: &BrowserArtifactSource) -> Option<String> {
    match source {
        BrowserArtifactSource::Unavailable => None,
        BrowserArtifactSource::PeerSwarm => Some("p2p".into()),
        BrowserArtifactSource::EdgeHttp => Some("edge-fallback".into()),
    }
}

impl BrowserAppModel {
    /// Creates a new model from the current worker runtime state.
    pub fn from_runtime(runtime: BrowserWorkerRuntime) -> Self {
        Self {
            runtime,
            last_error: None,
            last_validation: None,
            last_training: None,
        }
    }

    /// Applies one worker event to the browser-owned app model.
    pub fn apply_event(&mut self, event: BrowserWorkerEvent) {
        match event {
            BrowserWorkerEvent::Ready(capability) => {
                self.runtime.capability = Some(capability);
            }
            BrowserWorkerEvent::RuntimeStateChanged(state) => {
                self.runtime.state = Some(state);
            }
            BrowserWorkerEvent::TransportChanged(transport) => {
                self.runtime.transport = transport;
            }
            BrowserWorkerEvent::SwarmStatusChanged(status) => {
                self.runtime.observe_swarm_status(*status);
            }
            BrowserWorkerEvent::SessionUpdated(session) => {
                self.runtime.storage.remember_session(*session);
            }
            BrowserWorkerEvent::DirectoryUpdated { .. } => {
                self.runtime.storage.last_directory_sync_at = Some(Utc::now());
                self.runtime.storage.updated_at = Utc::now();
            }
            BrowserWorkerEvent::HeadUpdated { head_id } => {
                self.runtime.storage.remember_head(head_id);
            }
            BrowserWorkerEvent::ValidationCompleted(result) => {
                self.runtime.storage.remember_head(result.head_id.clone());
                self.last_validation = Some(result);
            }
            BrowserWorkerEvent::ReceiptsAcknowledged { receipt_ids, .. } => {
                self.runtime.storage.acknowledge_receipts(&receipt_ids);
            }
            BrowserWorkerEvent::ReceiptSubmissionDeferred {
                pending_receipts,
                reason,
                retryable,
                ..
            } => {
                let retry_label = if retryable {
                    "will retry"
                } else {
                    "requires operator attention"
                };
                self.last_error = Some(format!(
                    "receipt submission pending ({pending_receipts} receipt{}): {reason}; {retry_label}",
                    if pending_receipts == 1 { "" } else { "s" }
                ));
            }
            BrowserWorkerEvent::StorageUpdated(storage) => {
                self.runtime.storage = *storage;
            }
            BrowserWorkerEvent::MetricsUpdated(event) => {
                self.runtime.storage.remember_metrics_live_event(*event);
            }
            BrowserWorkerEvent::Error { message } => {
                self.last_error = Some(message);
            }
            BrowserWorkerEvent::TrainingCompleted(result) => {
                self.last_training = Some(result);
            }
            BrowserWorkerEvent::ReceiptOutboxReady { .. } => {}
        }
    }

    /// Returns the currently persisted active training lease, when available.
    pub fn active_training_lease(&self) -> Option<&crate::WorkloadTrainingLease> {
        self.runtime.storage.active_training_lease.as_ref()
    }

    /// Builds the static-browser-app client view from local browser state.
    pub fn view(&self, bindings: &BrowserUiBindings) -> BrowserAppClientView {
        let network_id = self
            .runtime
            .config
            .as_ref()
            .map(|config| config.network_id.as_str().to_owned())
            .or_else(|| {
                self.runtime
                    .storage
                    .directory_snapshot()
                    .map(|snapshot| snapshot.network_id.as_str().to_owned())
            })
            .unwrap_or_else(|| "unknown".into());
        let runtime_state = self
            .runtime
            .state
            .clone()
            .unwrap_or(BrowserRuntimeState::ViewerOnly);
        let storage = &self.runtime.storage;
        let session = &storage.session;
        let directory = storage.directory_snapshot();
        let leaderboard = storage
            .last_signed_leaderboard_snapshot
            .as_ref()
            .map(|snapshot| &snapshot.payload.payload);
        let leaderboard_entries = leaderboard
            .map(|leaderboard| leaderboard.entries.len())
            .unwrap_or(0);
        let accepted_receipts = leaderboard
            .map(|leaderboard| {
                leaderboard
                    .entries
                    .iter()
                    .map(|entry| entry.accepted_receipt_count as usize)
                    .sum::<usize>()
            })
            .unwrap_or(0);
        let visible_heads = directory
            .map(|directory| {
                directory
                    .entries
                    .iter()
                    .filter(|entry| entry.current_head_id.is_some())
                    .count()
            })
            .unwrap_or(0);
        let swarm_status = self.runtime.swarm_status();
        let direct_peers = swarm_status.connected_peer_count;
        let selected_experiment = selected_experiment_summary(
            directory,
            &self.runtime,
            &runtime_state,
            self.runtime.capability.as_ref(),
        );
        let metrics_bundle = active_metrics_bundle(&self.runtime, selected_experiment.as_ref());
        let latest_peer_window = metrics_bundle.and_then(latest_peer_window_metrics);
        let latest_eval = metrics_bundle.and_then(latest_head_eval_report);
        let validation_metric_preview = latest_eval
            .map(|report| metric_preview(&report.metric_values))
            .unwrap_or_default();
        let performance_summary = metrics_bundle.and_then(network_performance_view);
        let diffusion_summary = metrics_bundle.and_then(network_diffusion_view);
        let validate_available = selected_experiment
            .as_ref()
            .is_some_and(|experiment| experiment.validate_available);
        let train_available = selected_experiment
            .as_ref()
            .is_some_and(|experiment| experiment.train_available);
        let active_head_artifact_ready = storage.active_head_artifact_ready();
        let active_head_artifact_error = (!active_head_artifact_ready
            && storage.last_head_id.is_some())
        .then(|| {
            swarm_status
                .artifact_sync
                .as_ref()
                .and_then(|diagnostic| diagnostic.last_error.clone())
                .or_else(|| self.last_error.clone())
                .or_else(|| self.runtime.transport.last_error.clone())
        })
        .flatten();
        let active_head_artifact_source =
            browser_artifact_source_label(&swarm_status.artifact_source);

        BrowserAppClientView {
            network_id,
            default_surface: default_surface(&runtime_state),
            runtime_label: runtime_label(&runtime_state),
            runtime_detail: runtime_detail(&runtime_state, storage),
            capability_summary: capability_summary(self.runtime.capability.as_ref()),
            session_label: session_label(session),
            selected_experiment: selected_experiment.clone(),
            viewer: BrowserAppViewerView {
                visible_experiments: directory
                    .map(|directory| directory.entries.len())
                    .unwrap_or(0),
                visible_heads,
                leaderboard_entries,
                signed_directory_ready: storage.last_signed_directory_snapshot.is_some(),
                signed_leaderboard_ready: storage.last_signed_leaderboard_snapshot.is_some(),
                experiments_preview: experiment_previews(
                    directory,
                    self.runtime.capability.as_ref(),
                ),
                leaderboard_preview: leaderboard_preview(leaderboard, session),
            },
            validation: BrowserAppValidationView {
                validate_available,
                can_validate: is_validation_active(&runtime_state),
                current_head_id: storage
                    .last_head_id
                    .as_ref()
                    .map(|head_id| head_id.as_str().to_owned()),
                metrics_sync_at: storage
                    .last_metrics_sync_at
                    .as_ref()
                    .map(|timestamp| timestamp.to_rfc3339()),
                pending_receipts: storage.pending_receipts.len(),
                validation_status: self.last_validation.as_ref().map(|result| {
                    if result.accepted {
                        "accepted".to_owned()
                    } else {
                        "rejected".to_owned()
                    }
                }),
                checked_chunks: self
                    .last_validation
                    .as_ref()
                    .map(|result| result.checked_chunks),
                emitted_receipt_id: self
                    .last_validation
                    .as_ref()
                    .and_then(|result| result.emitted_receipt_id.as_ref())
                    .map(|receipt_id| receipt_id.as_str().to_owned()),
                evaluation_summary: latest_eval.map(eval_summary),
                metric_preview: validation_metric_preview,
            },
            training: BrowserAppTrainingView {
                train_available,
                can_train: is_training_active(&runtime_state),
                active_assignment: storage.active_assignment.as_ref().map(|assignment| {
                    format!(
                        "{}/{}",
                        assignment.experiment_id.as_str(),
                        assignment.revision_id.as_str()
                    )
                }),
                active_training_lease: active_training_lease_summary(storage),
                slice_status: training_slice_status(&runtime_state, storage, latest_peer_window),
                latest_head_id: storage
                    .last_head_id
                    .as_ref()
                    .map(|head_id| head_id.as_str().to_owned()),
                active_head_artifact_ready,
                active_head_artifact_source,
                active_head_artifact_error,
                cached_chunk_artifacts: storage.cached_chunk_artifacts.len(),
                cached_microshards: storage.cached_microshards.len(),
                pending_receipts: storage.pending_receipts.len(),
                max_window_secs: self
                    .runtime
                    .capability
                    .as_ref()
                    .map(|capability| capability.max_training_window_secs),
                last_window_secs: self.last_training.as_ref().map(|result| result.window_secs),
                optimizer_steps: latest_peer_window.map(|metrics| metrics.optimizer_step_count),
                accepted_samples: latest_peer_window
                    .and_then(|metrics| metrics.accepted_tokens_or_samples),
                slice_target_samples: latest_peer_window
                    .map(|metrics| metrics.attempted_tokens_or_samples),
                slice_remaining_samples: latest_peer_window.and_then(|metrics| {
                    metrics.accepted_tokens_or_samples.map(|accepted| {
                        metrics.attempted_tokens_or_samples.saturating_sub(accepted)
                    })
                }),
                last_loss: latest_peer_window
                    .and_then(|metrics| {
                        metrics
                            .local_train_loss_last
                            .or(metrics.local_train_loss_mean)
                    })
                    .map(|loss| format!("{loss:.4}")),
                publish_latency_ms: latest_peer_window.map(|metrics| metrics.publish_latency_ms),
                throughput_summary: latest_peer_window.and_then(training_throughput_summary),
                last_artifact_id: self
                    .last_training
                    .as_ref()
                    .map(|result| result.artifact_id.as_str().to_owned()),
                last_receipt_id: self
                    .last_training
                    .as_ref()
                    .and_then(|result| result.receipt_id.as_ref())
                    .map(|receipt_id| receipt_id.as_str().to_owned()),
            },
            network: BrowserAppNetworkView {
                edge_base_url: bindings.edge_base_url.clone(),
                transport: self.runtime.transport.display_label(),
                node_state: runtime_label(&runtime_state),
                direct_peers,
                observed_peers: leaderboard_entries,
                estimated_network_size: leaderboard_entries.max(direct_peers),
                accepted_receipts,
                certified_merges: visible_heads,
                in_flight_transfers: 0,
                network_note: network_note(&self.runtime),
                swarm_status,
                metrics_live_ready: storage.last_metrics_live_event.is_some()
                    || !storage.metrics_catchup_bundles.is_empty(),
                last_directory_sync_at: storage
                    .last_directory_sync_at
                    .as_ref()
                    .map(|timestamp| timestamp.to_rfc3339()),
                last_error: self
                    .last_error
                    .clone()
                    .or_else(|| self.runtime.transport.last_error.clone()),
                performance: performance_summary,
                diffusion: diffusion_summary,
            },
        }
    }
}

#[derive(Clone, Debug)]
/// Browser-owned controller for the static wasm app.
pub struct BrowserAppController {
    edge_client: BrowserEdgeClient,
    model: BrowserAppModel,
    #[cfg(target_arch = "wasm32")]
    direct_swarm_runtime: Option<WasmBrowserSwarmRuntime>,
}

#[cfg(target_arch = "wasm32")]
const DIRECT_SWARM_BOOTSTRAP_POLL_MS: u32 = 250;
#[cfg(target_arch = "wasm32")]
const DIRECT_SWARM_BOOTSTRAP_WAIT_MS: u32 = 8_000;

impl BrowserAppController {
    #[cfg(test)]
    pub(crate) fn for_tests(edge_client: BrowserEdgeClient, model: BrowserAppModel) -> Self {
        Self {
            edge_client,
            model,
            #[cfg(target_arch = "wasm32")]
            direct_swarm_runtime: None,
        }
    }

    /// Connects the browser app using the target-based connect config.
    pub async fn connect_with(
        config: BrowserAppConnectConfig,
    ) -> Result<Self, BrowserAuthClientError> {
        let BrowserAppConnectConfig {
            edge_base_url,
            capability,
            target,
            selected_experiment_id,
            selected_revision_id,
            seed_node_urls,
            bootstrap_snapshot,
            bootstrap_signed_seed_advertisement,
        } = config;
        Self::connect(
            edge_base_url,
            capability,
            target.preferred_role(),
            selected_experiment_id.map(|experiment_id| (experiment_id, selected_revision_id)),
            seed_node_urls,
            bootstrap_snapshot,
            bootstrap_signed_seed_advertisement,
        )
        .await
    }

    /// Connects a viewer-first browser app.
    pub async fn viewer(
        edge_base_url: impl Into<String>,
        capability: BrowserCapabilityReport,
    ) -> Result<Self, BrowserAuthClientError> {
        Self::connect_with(BrowserAppConnectConfig::viewer(edge_base_url, capability)).await
    }

    /// Connects an observer-first browser app.
    pub async fn observe(
        edge_base_url: impl Into<String>,
        capability: BrowserCapabilityReport,
    ) -> Result<Self, BrowserAuthClientError> {
        Self::connect_with(BrowserAppConnectConfig::observe(edge_base_url, capability)).await
    }

    /// Connects a validation-first browser app.
    pub async fn validate(
        edge_base_url: impl Into<String>,
        capability: BrowserCapabilityReport,
    ) -> Result<Self, BrowserAuthClientError> {
        Self::connect_with(BrowserAppConnectConfig::validate(edge_base_url, capability)).await
    }

    /// Connects a training-first browser app.
    pub async fn train(
        edge_base_url: impl Into<String>,
        capability: BrowserCapabilityReport,
    ) -> Result<Self, BrowserAuthClientError> {
        Self::connect_with(BrowserAppConnectConfig::train(edge_base_url, capability)).await
    }

    /// Connects the browser app to one bootstrap edge and derives initial local runtime state.
    pub async fn connect(
        edge_base_url: impl Into<String>,
        capability: BrowserCapabilityReport,
        requested_role: BrowserRuntimeRole,
        selected_experiment: Option<(String, Option<String>)>,
        site_seed_node_urls: Vec<String>,
        bootstrap_snapshot: Option<BrowserEdgeSnapshot>,
        bootstrap_signed_seed_advertisement: Option<
            SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>>,
        >,
    ) -> Result<Self, BrowserAuthClientError> {
        let edge_base_url = edge_base_url.into().trim_end_matches('/').to_owned();
        let preloaded_bootstrap = bootstrap_snapshot.is_some();
        let snapshot = match bootstrap_snapshot {
            Some(snapshot) => snapshot,
            None => fetch_edge_snapshot(&edge_base_url).await?,
        };
        let signed_seed_advertisement = match bootstrap_signed_seed_advertisement {
            Some(signed_seed_advertisement) => Some(signed_seed_advertisement),
            None if preloaded_bootstrap => {
                fetch_signed_seed_advertisement(&edge_base_url, &snapshot)
                    .await
                    .unwrap_or(None)
            }
            None => fetch_signed_seed_advertisement(&edge_base_url, &snapshot).await?,
        };
        let bindings = BrowserUiBindings::from_edge_snapshot(&edge_base_url, &snapshot);
        let edge_client = BrowserEdgeClient::new(
            bindings.clone(),
            BrowserEnrollmentConfig::for_runtime_sync(&snapshot),
        );
        let runtime = BrowserWorkerRuntime::start(
            runtime_config_from_snapshot(
                &edge_base_url,
                &snapshot,
                &capability,
                requested_role,
                selected_experiment,
                &site_seed_node_urls,
                signed_seed_advertisement.as_ref(),
            ),
            capability,
            BrowserTransportStatus::enabled(
                snapshot.transports.webrtc_direct,
                snapshot.transports.webtransport_gateway,
                snapshot.transports.wss_fallback,
            ),
        );
        let mut runtime = runtime;
        runtime.storage = load_durable_browser_storage(&snapshot.network_id)
            .await
            .map_err(BrowserAuthClientError::ArtifactTransport)?;
        runtime.storage.pending_receipts = load_durable_receipt_outbox(&snapshot.network_id)
            .await
            .map_err(BrowserAuthClientError::ArtifactTransport)?;
        #[allow(unused_mut)]
        let mut bootstrap_events = runtime.apply_command(
            BrowserWorkerCommand::ApplySwarmStatus(Box::new(
                runtime.planned_swarm_status_snapshot(),
            )),
            None,
            None,
        );
        let stored_session = runtime.storage.session.clone();
        bootstrap_events.extend(runtime.apply_command(
            BrowserWorkerCommand::ApplySwarmDirectory(Box::new(snapshot.directory.clone())),
            None,
            Some(&stored_session),
        ));
        bootstrap_events.extend(runtime.apply_command(
            BrowserWorkerCommand::ApplySwarmHeads(snapshot.heads.clone()),
            None,
            Some(&stored_session),
        ));
        #[cfg(target_arch = "wasm32")]
        let (direct_swarm_runtime, direct_swarm_events) =
            establish_direct_swarm_runtime(&mut runtime).await;
        #[cfg(target_arch = "wasm32")]
        bootstrap_events.extend(direct_swarm_events);
        #[cfg(target_arch = "wasm32")]
        let edge_client = if let Some(direct_swarm_runtime) = direct_swarm_runtime.as_ref() {
            edge_client.with_peer_artifact_fetcher(DirectSwarmPeerArtifactFetcher::new(
                direct_swarm_runtime.clone(),
            ))
        } else {
            edge_client
        };
        let mut controller = Self {
            edge_client,
            model: BrowserAppModel::from_runtime(runtime),
            #[cfg(target_arch = "wasm32")]
            direct_swarm_runtime,
        };
        for event in bootstrap_events {
            controller.model.apply_event(event);
        }
        if let Err(error) = controller.refresh().await
            && !preloaded_bootstrap
        {
            return Err(error);
        }
        Ok(controller)
    }

    /// Refreshes local runtime state from the connected bootstrap edge.
    pub async fn refresh(&mut self) -> Result<BrowserAppClientView, BrowserAuthClientError> {
        let mut runtime = self.model.runtime.clone();
        let session = runtime.storage.session.clone();
        let previous_error = self.model.last_error.clone();
        #[cfg(target_arch = "wasm32")]
        {
            let (events, hard_error) = refresh_worker_runtime_preferring_direct_swarm(
                &self.edge_client,
                &mut runtime,
                Some(&session),
                self.direct_swarm_runtime.as_mut(),
                true,
            )
            .await;
            let mut model = BrowserAppModel::from_runtime(runtime);
            model.last_error = hard_error
                .as_ref()
                .map(ToString::to_string)
                .or(previous_error);
            for event in events {
                model.apply_event(event);
            }
            self.model = model;
            let persist_result = async {
                self.persist_receipt_outbox().await?;
                self.persist_browser_storage().await
            }
            .await;
            if let Some(error) = hard_error {
                let _ = persist_result;
                return Err(error);
            }
            persist_result?;
            Ok(self.view())
        }
        #[cfg(not(target_arch = "wasm32"))]
        match self
            .edge_client
            .sync_worker_runtime(&mut runtime, Some(&session), true)
            .await
        {
            Ok(events) => {
                let mut model = BrowserAppModel::from_runtime(runtime);
                model.last_error = previous_error;
                #[cfg(target_arch = "wasm32")]
                for event in direct_swarm_events {
                    model.apply_event(event);
                }
                for event in events {
                    model.apply_event(event);
                }
                self.model = model;
                self.persist_receipt_outbox().await?;
                self.persist_browser_storage().await?;
                Ok(self.view())
            }
            Err(error) => {
                let mut model = BrowserAppModel::from_runtime(runtime);
                model.last_error = Some(error.to_string());
                #[cfg(target_arch = "wasm32")]
                for event in direct_swarm_events {
                    model.apply_event(event);
                }
                self.model = model;
                let _ = self.persist_receipt_outbox().await;
                let _ = self.persist_browser_storage().await;
                Err(error)
            }
        }
    }

    fn durable_network_id(&self) -> Result<burn_p2p_core::NetworkId, BrowserAuthClientError> {
        self.model
            .runtime
            .config
            .as_ref()
            .map(|config| config.network_id.clone())
            .or_else(|| {
                self.model
                    .runtime
                    .storage
                    .directory_snapshot()
                    .map(|snapshot| snapshot.network_id.clone())
            })
            .ok_or_else(|| {
                BrowserAuthClientError::ArtifactTransport(
                    "browser runtime is missing a network id for durable storage".into(),
                )
            })
    }

    async fn persist_receipt_outbox(&self) -> Result<(), BrowserAuthClientError> {
        let network_id = self.durable_network_id()?;
        if self.model.runtime.storage.pending_receipts.is_empty() {
            clear_durable_receipt_outbox(&network_id)
                .await
                .map_err(BrowserAuthClientError::ArtifactTransport)
        } else {
            persist_durable_receipt_outbox(
                &network_id,
                &self.model.runtime.storage.pending_receipts,
            )
            .await
            .map_err(BrowserAuthClientError::ArtifactTransport)
        }
    }

    async fn persist_browser_storage(&self) -> Result<(), BrowserAuthClientError> {
        let network_id = self.durable_network_id()?;
        persist_durable_browser_storage(&network_id, &self.model.runtime.storage)
            .await
            .map_err(BrowserAuthClientError::ArtifactTransport)
    }

    /// Returns the current browser-app client view derived from the local runtime.
    pub fn view(&self) -> BrowserAppClientView {
        self.model.view(self.edge_client.bindings())
    }

    /// Returns the currently persisted active training lease, when available.
    pub fn active_training_lease(&self) -> Option<&crate::WorkloadTrainingLease> {
        self.model.active_training_lease()
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn refresh_worker_runtime_preferring_direct_swarm(
    edge_client: &BrowserEdgeClient,
    runtime: &mut BrowserWorkerRuntime,
    session: Option<&BrowserSessionState>,
    direct_swarm_runtime: Option<&mut WasmBrowserSwarmRuntime>,
    include_leaderboard: bool,
) -> (Vec<BrowserWorkerEvent>, Option<BrowserAuthClientError>) {
    let mut events = apply_direct_swarm_status_snapshot(runtime, direct_swarm_runtime.as_deref());

    if let Some(direct_swarm_runtime) = direct_swarm_runtime {
        events.extend(ensure_direct_swarm_runtime_connected(runtime, direct_swarm_runtime).await);
        events.extend(wait_for_direct_swarm_bootstrap(runtime, direct_swarm_runtime).await);
        match sync_worker_runtime_from_direct_swarm(
            edge_client,
            runtime,
            session,
            direct_swarm_runtime,
        )
        .await
        {
            Ok(sync_events) => {
                events.extend(sync_events);
                if !should_fallback_to_edge_control_sync(runtime) {
                    return (events, None);
                }
            }
            Err(error) => {
                runtime.transport.last_error = Some(error.to_string());
                if !should_fallback_to_edge_control_sync(runtime) {
                    events.push(BrowserWorkerEvent::Error {
                        message: error.to_string(),
                    });
                    return (events, None);
                }
            }
        }
    }

    match edge_client
        .sync_worker_runtime(runtime, session, include_leaderboard)
        .await
    {
        Ok(edge_events) => {
            events.extend(edge_events);
            (events, None)
        }
        Err(error) => (events, Some(error)),
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn establish_direct_swarm_runtime(
    runtime: &mut BrowserWorkerRuntime,
) -> (Option<WasmBrowserSwarmRuntime>, Vec<BrowserWorkerEvent>) {
    let requires_peer_transport = runtime
        .state
        .as_ref()
        .is_some_and(BrowserRuntimeState::requires_peer_transport);
    let Some(config) = runtime.config.clone() else {
        return (None, Vec::new());
    };
    if !requires_peer_transport {
        return (None, Vec::new());
    }

    let mut direct_runtime = WasmBrowserSwarmRuntime::new(config.network_id.clone());
    let connect_result = direct_runtime.connect(config.swarm_bootstrap()).await;
    let mut events = runtime.apply_command(
        BrowserWorkerCommand::ApplySwarmStatus(Box::new(direct_runtime.status())),
        None,
        None,
    );
    if let Err(error) = connect_result {
        events.push(BrowserWorkerEvent::Error {
            message: format!("browser direct swarm connect failed: {error}"),
        });
    } else {
        for (label, subscribe_result) in [
            ("directory", direct_runtime.subscribe_directory().await),
            ("heads", direct_runtime.subscribe_heads().await),
            ("metrics", direct_runtime.subscribe_metrics().await),
        ] {
            if let Err(error) = subscribe_result {
                events.push(BrowserWorkerEvent::Error {
                    message: format!("browser direct swarm {label} subscription failed: {error}"),
                });
            }
        }
    }
    (Some(direct_runtime), events)
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn ensure_direct_swarm_runtime_connected(
    runtime: &mut BrowserWorkerRuntime,
    direct_runtime: &mut WasmBrowserSwarmRuntime,
) -> Vec<BrowserWorkerEvent> {
    let status = direct_runtime.status();
    if status.connected_transport.is_some() || status.last_error.is_none() {
        return Vec::new();
    }
    let Some(config) = runtime.config.clone() else {
        return Vec::new();
    };
    let mut events = Vec::new();
    match direct_runtime.connect(config.swarm_bootstrap()).await {
        Ok(_) => {
            events.extend(runtime.apply_command(
                BrowserWorkerCommand::ApplySwarmStatus(Box::new(direct_runtime.status())),
                None,
                None,
            ));
            for (label, subscribe_result) in [
                ("directory", direct_runtime.subscribe_directory().await),
                ("heads", direct_runtime.subscribe_heads().await),
                ("metrics", direct_runtime.subscribe_metrics().await),
            ] {
                if let Err(error) = subscribe_result {
                    events.push(BrowserWorkerEvent::Error {
                        message: format!(
                            "browser direct swarm {label} subscription retry failed: {error}"
                        ),
                    });
                }
            }
        }
        Err(error) => {
            events.push(BrowserWorkerEvent::Error {
                message: format!("browser direct swarm reconnect failed: {error}"),
            });
        }
    }
    events
}

#[cfg(target_arch = "wasm32")]
fn apply_direct_swarm_status_snapshot(
    runtime: &mut BrowserWorkerRuntime,
    direct_runtime: Option<&WasmBrowserSwarmRuntime>,
) -> Vec<BrowserWorkerEvent> {
    let Some(direct_runtime) = direct_runtime else {
        return Vec::new();
    };
    runtime.apply_command(
        BrowserWorkerCommand::ApplySwarmStatus(Box::new(direct_runtime.status())),
        None,
        None,
    )
}

#[cfg(any(test, target_arch = "wasm32"))]
pub(crate) fn should_wait_for_direct_swarm_bootstrap(
    runtime: &BrowserWorkerRuntime,
    swarm_status: &BrowserSwarmStatus,
) -> bool {
    let direct_handoff_pending = matches!(
        swarm_status.desired_transport,
        Some(BrowserTransportFamily::WebRtcDirect | BrowserTransportFamily::WebTransport)
    ) && matches!(
        swarm_status.connected_transport,
        None | Some(BrowserTransportFamily::WssFallback)
    );
    runtime
        .state
        .as_ref()
        .is_some_and(BrowserRuntimeState::requires_peer_transport)
        && runtime.storage.directory_snapshot().is_none()
        && runtime.storage.last_head_id.is_none()
        && direct_handoff_pending
        && swarm_status.last_error.is_none()
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn wait_for_direct_swarm_bootstrap(
    runtime: &mut BrowserWorkerRuntime,
    direct_runtime: &WasmBrowserSwarmRuntime,
) -> Vec<BrowserWorkerEvent> {
    let mut events = Vec::new();
    let polls = DIRECT_SWARM_BOOTSTRAP_WAIT_MS / DIRECT_SWARM_BOOTSTRAP_POLL_MS;
    for _ in 0..polls {
        let status = direct_runtime.status();
        if !should_wait_for_direct_swarm_bootstrap(runtime, &status) {
            break;
        }
        TimeoutFuture::new(DIRECT_SWARM_BOOTSTRAP_POLL_MS).await;
        events.extend(runtime.apply_command(
            BrowserWorkerCommand::ApplySwarmStatus(Box::new(direct_runtime.status())),
            None,
            None,
        ));
    }
    events
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
struct DirectSwarmPeerArtifactFetcher {
    runtime: WasmBrowserSwarmRuntime,
}

#[cfg(target_arch = "wasm32")]
impl DirectSwarmPeerArtifactFetcher {
    fn new(runtime: WasmBrowserSwarmRuntime) -> Self {
        Self { runtime }
    }
}

#[cfg(target_arch = "wasm32")]
impl BrowserPeerArtifactFetcher for DirectSwarmPeerArtifactFetcher {
    fn fetch(&self, request: BrowserPeerArtifactRequest) -> BrowserPeerArtifactFetchFuture {
        let mut runtime = self.runtime.clone();
        Box::pin(async move {
            let descriptor = runtime
                .fetch_artifact_manifest(BrowserArtifactManifestRequest {
                    artifact_id: request.artifact_id.clone(),
                    provider_peer_ids: request.provider_peer_ids.clone(),
                })
                .await
                .map_err(|error| BrowserAuthClientError::ArtifactTransport(error.to_string()))?;
            if descriptor.artifact_id != request.artifact_id {
                return Err(BrowserAuthClientError::ArtifactTransport(format!(
                    "browser direct swarm returned descriptor for unexpected artifact {}",
                    descriptor.artifact_id.as_str()
                )));
            }

            let mut chunks = descriptor.chunks.clone();
            chunks.sort_by(|left, right| {
                left.offset_bytes
                    .cmp(&right.offset_bytes)
                    .then_with(|| left.chunk_id.cmp(&right.chunk_id))
            });
            let mut bytes = Vec::with_capacity(descriptor.bytes_len as usize);
            for chunk in &chunks {
                let chunk_bytes = runtime
                    .fetch_artifact_chunk(BrowserArtifactChunkRequest {
                        artifact_id: request.artifact_id.clone(),
                        chunk_id: chunk.chunk_id.clone(),
                        provider_peer_ids: request.provider_peer_ids.clone(),
                    })
                    .await
                    .map_err(|error| {
                        BrowserAuthClientError::ArtifactTransport(error.to_string())
                    })?;
                if chunk_bytes.len() as u64 != chunk.length_bytes {
                    return Err(BrowserAuthClientError::ArtifactTransport(format!(
                        "browser direct swarm returned chunk {} with unexpected length {} (expected {})",
                        chunk.chunk_id.as_str(),
                        chunk_bytes.len(),
                        chunk.length_bytes
                    )));
                }
                let chunk_hash =
                    ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(&chunk_bytes));
                if chunk_hash != chunk.chunk_hash {
                    return Err(BrowserAuthClientError::ArtifactTransport(format!(
                        "browser direct swarm returned chunk {} with unexpected hash {}",
                        chunk.chunk_id.as_str(),
                        chunk_hash.as_str()
                    )));
                }
                bytes.extend_from_slice(&chunk_bytes);
            }
            if bytes.len() as u64 != descriptor.bytes_len {
                return Err(BrowserAuthClientError::ArtifactTransport(format!(
                    "browser direct swarm reconstructed {} bytes for {} but descriptor expected {}",
                    bytes.len(),
                    descriptor.artifact_id.as_str(),
                    descriptor.bytes_len
                )));
            }
            let root_hash =
                ContentId::from_multihash(burn_p2p_core::codec::multihash_sha256(&bytes));
            if root_hash != descriptor.root_hash {
                return Err(BrowserAuthClientError::ArtifactTransport(format!(
                    "browser direct swarm reconstructed {} with unexpected root hash {}",
                    descriptor.artifact_id.as_str(),
                    root_hash.as_str()
                )));
            }
            Ok(BrowserPeerArtifactPayload { descriptor, bytes })
        })
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn sync_worker_runtime_from_direct_swarm(
    edge_client: &BrowserEdgeClient,
    runtime: &mut BrowserWorkerRuntime,
    session: Option<&BrowserSessionState>,
    direct_runtime: &mut WasmBrowserSwarmRuntime,
) -> Result<Vec<BrowserWorkerEvent>, BrowserAuthClientError> {
    let mut events = runtime.apply_command(
        BrowserWorkerCommand::ApplySwarmStatus(Box::new(direct_runtime.status())),
        None,
        None,
    );
    let mut updates = direct_runtime.drain_updates();
    if updates.is_empty() && should_fetch_direct_swarm_snapshot(runtime) {
        let snapshot = direct_runtime
            .fetch_snapshot()
            .await
            .map_err(|error| BrowserAuthClientError::Swarm(error.to_string()))?;
        updates.push(BrowserSwarmUpdate::Snapshot(Box::new(snapshot)));
    }
    for update in updates {
        match update {
            BrowserSwarmUpdate::Snapshot(snapshot) => {
                let needs_artifact_sync = apply_control_snapshot_to_browser_runtime(
                    runtime,
                    session,
                    snapshot.as_ref(),
                    &mut events,
                );
                if needs_artifact_sync {
                    match edge_client
                        .sync_active_head_artifact_from_control_snapshot_into_worker(
                            runtime,
                            snapshot.as_ref(),
                        )
                        .await
                    {
                        Ok(artifact_events) => events.extend(artifact_events),
                        Err(error) => events.push(BrowserWorkerEvent::Error {
                            message: format!("browser artifact sync failed: {error}"),
                        }),
                    }
                }
            }
        }
    }
    Ok(events)
}

#[cfg(any(test, target_arch = "wasm32"))]
pub(crate) fn should_fetch_direct_swarm_snapshot(runtime: &BrowserWorkerRuntime) -> bool {
    if runtime.storage.directory_snapshot().is_none() || runtime.storage.last_head_id.is_none() {
        return true;
    }
    should_sync_active_head_artifact(runtime, runtime.storage.last_head_id.as_ref())
}

#[cfg(any(test, target_arch = "wasm32"))]
pub(crate) fn should_sync_active_head_artifact(
    runtime: &BrowserWorkerRuntime,
    previous_head_id: Option<&burn_p2p::HeadId>,
) -> bool {
    runtime
        .storage
        .last_head_id
        .as_ref()
        .is_some_and(|head_id| {
            previous_head_id != Some(head_id) || !runtime.storage.active_head_artifact_ready()
        })
}

#[cfg(any(test, target_arch = "wasm32"))]
pub(crate) fn should_fallback_to_edge_control_sync(runtime: &BrowserWorkerRuntime) -> bool {
    let swarm_status = runtime.swarm_status();
    let direct_selected = matches!(
        swarm_status.desired_transport,
        Some(BrowserTransportFamily::WebRtcDirect | BrowserTransportFamily::WebTransport)
    );
    let direct_connected = matches!(
        swarm_status.connected_transport,
        Some(BrowserTransportFamily::WebRtcDirect | BrowserTransportFamily::WebTransport)
    );
    let missing_control_state =
        runtime.storage.directory_snapshot().is_none() || runtime.storage.last_head_id.is_none();
    if direct_selected && !direct_connected {
        return missing_control_state;
    }

    if runtime.transport.connected.is_none() {
        return true;
    }

    missing_control_state
}

#[cfg(target_arch = "wasm32")]
fn apply_control_snapshot_to_browser_runtime(
    runtime: &mut BrowserWorkerRuntime,
    session: Option<&BrowserSessionState>,
    snapshot: &ControlPlaneSnapshot,
    events: &mut Vec<BrowserWorkerEvent>,
) -> bool {
    let previous_head_id = runtime.storage.last_head_id.clone();
    if let Some(directory) = browser_directory_snapshot_from_control_snapshot(snapshot, runtime) {
        events.extend(runtime.apply_command(
            BrowserWorkerCommand::ApplySwarmDirectory(Box::new(directory)),
            None,
            session,
        ));
    }
    let heads = browser_heads_from_control_snapshot(snapshot, runtime);
    if !heads.is_empty() {
        events.extend(runtime.apply_command(
            BrowserWorkerCommand::ApplySwarmHeads(heads),
            None,
            session,
        ));
    }
    if let Some(metrics) = browser_metrics_sync_from_control_snapshot(snapshot, runtime) {
        events.extend(runtime.apply_command(
            BrowserWorkerCommand::ApplySwarmMetricsSync(Box::new(metrics)),
            None,
            session,
        ));
    }
    should_sync_active_head_artifact(runtime, previous_head_id.as_ref())
}

#[cfg(target_arch = "wasm32")]
fn browser_directory_snapshot_from_control_snapshot(
    snapshot: &ControlPlaneSnapshot,
    runtime: &BrowserWorkerRuntime,
) -> Option<burn_p2p_core::BrowserDirectorySnapshot> {
    let network_id = runtime
        .config
        .as_ref()
        .map(|config| config.network_id.clone())?;
    snapshot
        .directory_announcements
        .iter()
        .rev()
        .find(|announcement| announcement.network_id == network_id)
        .map(|announcement| burn_p2p_core::BrowserDirectorySnapshot {
            network_id,
            generated_at: announcement.announced_at,
            entries: announcement.entries.clone(),
        })
}

#[cfg(target_arch = "wasm32")]
fn browser_heads_from_control_snapshot(
    snapshot: &ControlPlaneSnapshot,
    runtime: &BrowserWorkerRuntime,
) -> Vec<burn_p2p::HeadDescriptor> {
    let Some(network_id) = runtime
        .config
        .as_ref()
        .map(|config| config.network_id.clone())
    else {
        return Vec::new();
    };
    let visible_experiments = snapshot
        .directory_announcements
        .iter()
        .rev()
        .find(|announcement| announcement.network_id == network_id)
        .map(|announcement| {
            announcement
                .entries
                .iter()
                .map(|entry| {
                    (
                        entry.study_id.clone(),
                        entry.experiment_id.clone(),
                        entry.current_revision_id.clone(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut heads = snapshot
        .head_announcements
        .iter()
        .map(|announcement| announcement.head.clone())
        .filter(|head| {
            visible_experiments
                .iter()
                .any(|(study_id, experiment_id, revision_id)| {
                    &head.study_id == study_id
                        && &head.experiment_id == experiment_id
                        && &head.revision_id == revision_id
                })
        })
        .collect::<Vec<_>>();
    heads.sort_by(|left, right| {
        left.head_id
            .cmp(&right.head_id)
            .then_with(|| left.created_at.cmp(&right.created_at))
    });
    heads.dedup_by(|left, right| left.head_id == right.head_id);
    heads
}

#[cfg(target_arch = "wasm32")]
fn browser_metrics_sync_from_control_snapshot(
    snapshot: &ControlPlaneSnapshot,
    runtime: &BrowserWorkerRuntime,
) -> Option<crate::BrowserMetricsSyncState> {
    let selected_experiment = runtime
        .storage
        .active_assignment
        .as_ref()
        .map(|assignment| assignment.experiment_id.clone())
        .or_else(|| {
            runtime
                .config
                .as_ref()
                .and_then(|config| config.selected_experiment.clone())
        });
    let selected_revision = runtime
        .storage
        .active_assignment
        .as_ref()
        .map(|assignment| assignment.revision_id.clone())
        .or_else(|| {
            runtime
                .config
                .as_ref()
                .and_then(|config| config.selected_revision.clone())
        });
    let live_event = snapshot
        .metrics_announcements
        .iter()
        .filter(|announcement| {
            announcement.event.cursors.iter().any(|cursor| {
                selected_experiment
                    .as_ref()
                    .map(|experiment_id| &cursor.experiment_id == experiment_id)
                    .unwrap_or(true)
                    && selected_revision
                        .as_ref()
                        .map(|revision_id| &cursor.revision_id == revision_id)
                        .unwrap_or(true)
            })
        })
        .max_by_key(|announcement| announcement.event.generated_at)
        .map(|announcement| announcement.event.clone())?;
    Some(crate::BrowserMetricsSyncState {
        catchup_bundles: Vec::new(),
        live_event: Some(live_event),
    })
}

async fn fetch_edge_snapshot(
    edge_base_url: &str,
) -> Result<BrowserEdgeSnapshot, BrowserAuthClientError> {
    reqwest::Client::new()
        .get(BrowserUiBindings::new(edge_base_url).endpoint_url("/portal/snapshot"))
        .send()
        .await?
        .error_for_status()?
        .json::<BrowserEdgeSnapshot>()
        .await
        .map_err(Into::into)
}

async fn fetch_signed_seed_advertisement(
    edge_base_url: &str,
    snapshot: &BrowserEdgeSnapshot,
) -> Result<Option<SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>>>, BrowserAuthClientError>
{
    let bindings = BrowserUiBindings::from_edge_snapshot(edge_base_url, snapshot);
    let response = reqwest::Client::new()
        .get(bindings.endpoint_url(&bindings.paths.browser_seed_advertisement_path))
        .send()
        .await?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    response
        .error_for_status()?
        .json::<SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>>>()
        .await
        .map(Some)
        .map_err(Into::into)
}

fn runtime_config_from_snapshot(
    edge_base_url: &str,
    snapshot: &BrowserEdgeSnapshot,
    capability: &BrowserCapabilityReport,
    requested_role: BrowserRuntimeRole,
    selected_experiment: Option<(String, Option<String>)>,
    site_seed_node_urls: &[String],
    signed_seed_advertisement: Option<&SignedPayload<SchemaEnvelope<BrowserSeedAdvertisement>>>,
) -> BrowserRuntimeConfig {
    let target_artifact_hash = snapshot
        .allowed_target_artifact_hashes
        .iter()
        .next()
        .cloned()
        .or_else(|| {
            snapshot
                .trust_bundle
                .as_ref()
                .and_then(|bundle| bundle.allowed_target_artifact_hashes.iter().next().cloned())
        })
        .unwrap_or_else(|| ContentId::new("browser-client-artifact"));
    let release_train_hash = snapshot
        .required_release_train_hash
        .clone()
        .or_else(|| {
            snapshot
                .trust_bundle
                .as_ref()
                .map(|bundle| bundle.required_release_train_hash.clone())
        })
        .unwrap_or_else(|| ContentId::new("browser-client-train"));
    let mut config = BrowserRuntimeConfig::new(
        edge_base_url,
        snapshot.network_id.clone(),
        release_train_hash,
        "browser-client",
        target_artifact_hash,
    );
    config.role = preferred_runtime_role(snapshot, capability, requested_role);
    config.receipt_submit_path = snapshot.paths.receipt_submit_path.clone();
    config.site_seed_node_urls = site_seed_node_urls.to_vec();
    config.seed_bootstrap = resolve_browser_seed_bootstrap(
        &snapshot.network_id,
        signed_seed_advertisement,
        site_seed_node_urls,
        snapshot.captured_at,
    );
    if let Some((experiment_id, revision_id)) = selected_experiment {
        config.selected_experiment = Some(ExperimentId::new(&experiment_id));
        config.selected_revision = revision_id.map(|revision_id| RevisionId::new(&revision_id));
    } else if let Some(entry) = snapshot.directory.entries.first() {
        config.selected_experiment = Some(entry.experiment_id.clone());
        config.selected_revision = Some(entry.current_revision_id.clone());
    }
    config
}

fn preferred_runtime_role(
    snapshot: &BrowserEdgeSnapshot,
    capability: &BrowserCapabilityReport,
    requested_role: BrowserRuntimeRole,
) -> BrowserRuntimeRole {
    if matches!(snapshot.browser_mode, BrowserMode::Disabled) {
        return BrowserRuntimeRole::Viewer;
    }

    match requested_role {
        BrowserRuntimeRole::Viewer | BrowserRuntimeRole::BrowserObserver => {
            BrowserRuntimeRole::BrowserObserver
        }
        BrowserRuntimeRole::BrowserVerifier => match capability.recommended_role {
            BrowserRuntimeRole::BrowserTrainerWgpu | BrowserRuntimeRole::BrowserVerifier => {
                BrowserRuntimeRole::BrowserVerifier
            }
            _ => BrowserRuntimeRole::BrowserObserver,
        },
        BrowserRuntimeRole::BrowserTrainerWgpu => match capability.recommended_role {
            BrowserRuntimeRole::BrowserTrainerWgpu => BrowserRuntimeRole::BrowserTrainerWgpu,
            _ => BrowserRuntimeRole::BrowserObserver,
        },
        BrowserRuntimeRole::BrowserFallback => BrowserRuntimeRole::BrowserFallback,
    }
}

fn selected_experiment_summary(
    directory: Option<&burn_p2p_core::BrowserDirectorySnapshot>,
    runtime: &BrowserWorkerRuntime,
    runtime_state: &BrowserRuntimeState,
    capability: Option<&BrowserCapabilityReport>,
) -> Option<BrowserAppExperimentSummary> {
    let entries = &directory?.entries;
    let selected_assignment = runtime.storage.active_assignment.as_ref();
    let selected_experiment_id = runtime
        .storage
        .active_assignment
        .as_ref()
        .map(|assignment| assignment.experiment_id.as_str())
        .or_else(|| {
            runtime
                .config
                .as_ref()
                .and_then(|config| config.selected_experiment.as_ref())
                .map(|experiment_id| experiment_id.as_str())
        })?;
    let selected_revision_id = runtime
        .storage
        .active_assignment
        .as_ref()
        .map(|assignment| assignment.revision_id.as_str())
        .or_else(|| {
            runtime
                .config
                .as_ref()
                .and_then(|config| config.selected_revision.as_ref())
                .map(|revision_id| revision_id.as_str())
        });
    entries
        .iter()
        .find(|entry| {
            entry.experiment_id.as_str() == selected_experiment_id
                && selected_revision_id
                    .map(|revision_id| entry.current_revision_id.as_str() == revision_id)
                    .unwrap_or(true)
        })
        .or_else(|| {
            selected_assignment.and_then(|assignment| {
                entries.iter().find(|entry| {
                    entry.study_id == assignment.study_id
                        && entry.experiment_id == assignment.experiment_id
                })
            })
        })
        .map(|entry| experiment_summary(entry, runtime_state, capability))
}

fn experiment_previews(
    directory: Option<&burn_p2p_core::BrowserDirectorySnapshot>,
    capability: Option<&BrowserCapabilityReport>,
) -> Vec<BrowserAppExperimentSummary> {
    directory
        .map(|directory| {
            directory
                .entries
                .iter()
                .take(4)
                .map(|entry| {
                    experiment_summary(entry, &BrowserRuntimeState::ViewerOnly, capability)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn experiment_summary(
    entry: &ExperimentDirectoryEntry,
    runtime_state: &BrowserRuntimeState,
    capability: Option<&BrowserCapabilityReport>,
) -> BrowserAppExperimentSummary {
    let (validate_available, train_available) = experiment_capabilities(entry, capability);
    BrowserAppExperimentSummary {
        display_name: entry.display_name.clone(),
        experiment_id: entry.experiment_id.as_str().to_owned(),
        revision_id: entry.current_revision_id.as_str().to_owned(),
        workload_id: entry.workload_id.as_str().to_owned(),
        current_head_id: entry
            .current_head_id
            .as_ref()
            .map(|head_id| head_id.as_str().to_owned()),
        validate_available,
        train_available,
        availability: experiment_availability(
            entry,
            runtime_state,
            validate_available,
            train_available,
        ),
    }
}

fn experiment_availability(
    entry: &ExperimentDirectoryEntry,
    runtime_state: &BrowserRuntimeState,
    validate_available: bool,
    train_available: bool,
) -> String {
    let mut modes = vec!["view"];
    if validate_available {
        modes.push("validate");
    }
    if train_available {
        modes.push("train");
    }
    let mut summary = modes.join(" · ");
    if entry.current_head_id.is_none() {
        summary.push_str(" · pending head");
    } else if is_training_active(runtime_state) && train_available {
        summary.push_str(" · active");
    } else if is_validation_active(runtime_state) && validate_available {
        summary.push_str(" · validating");
    }
    summary
}

fn experiment_capabilities(
    entry: &ExperimentDirectoryEntry,
    capability: Option<&BrowserCapabilityReport>,
) -> (bool, bool) {
    let validate_available = capability.is_some()
        && (entry.allowed_roles.contains(&burn_p2p::PeerRole::Validator)
            || entry.allowed_roles.contains(&burn_p2p::PeerRole::Evaluator)
            || entry
                .allowed_roles
                .contains(&burn_p2p::PeerRole::BrowserVerifier));
    let train_available = capability
        .is_some_and(|capability| matches!(capability.gpu_support, BrowserGpuSupport::Available))
        && (entry
            .allowed_roles
            .contains(&burn_p2p::PeerRole::TrainerGpu)
            || entry
                .allowed_roles
                .contains(&burn_p2p::PeerRole::TrainerCpu)
            || entry
                .allowed_roles
                .contains(&burn_p2p::PeerRole::BrowserTrainer)
            || entry
                .allowed_roles
                .contains(&burn_p2p::PeerRole::BrowserTrainerWgpu));
    (validate_available, train_available)
}

fn leaderboard_preview(
    leaderboard: Option<&burn_p2p_core::BrowserLeaderboardSnapshot>,
    session: &BrowserSessionState,
) -> Vec<BrowserAppLeaderboardPreview> {
    let local_principal = session.principal_id().map(|principal| principal.as_str());
    leaderboard
        .map(|leaderboard| {
            leaderboard
                .entries
                .iter()
                .take(5)
                .map(|entry| BrowserAppLeaderboardPreview {
                    label: entry.identity.label.clone(),
                    score: format!("{:.2}", entry.leaderboard_score_v1),
                    receipts: entry.accepted_receipt_count as usize,
                    is_local: local_principal
                        .zip(
                            entry
                                .identity
                                .principal_id
                                .as_ref()
                                .map(|principal| principal.as_str()),
                        )
                        .map(|(left, right)| left == right)
                        .unwrap_or(false),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn active_metrics_bundle<'a>(
    runtime: &'a BrowserWorkerRuntime,
    selected_experiment: Option<&BrowserAppExperimentSummary>,
) -> Option<&'a MetricsCatchupBundle> {
    let assignment = runtime.storage.active_assignment.as_ref();
    runtime
        .storage
        .metrics_catchup_bundles
        .iter()
        .filter(|bundle| {
            assignment
                .map(|assignment| {
                    bundle.experiment_id == assignment.experiment_id
                        && bundle.revision_id == assignment.revision_id
                })
                .or_else(|| {
                    selected_experiment.map(|selected| {
                        bundle.experiment_id.as_str() == selected.experiment_id
                            && bundle.revision_id.as_str() == selected.revision_id
                    })
                })
                .unwrap_or(true)
        })
        .max_by_key(|bundle| bundle.generated_at)
}

fn latest_peer_window_metrics(
    bundle: &MetricsCatchupBundle,
) -> Option<&burn_p2p_core::PeerWindowMetrics> {
    bundle
        .snapshot
        .peer_window_metrics
        .iter()
        .max_by_key(|metrics| metrics.window_finished_at)
}

fn latest_head_eval_report(
    bundle: &MetricsCatchupBundle,
) -> Option<&burn_p2p_core::HeadEvalReport> {
    bundle
        .snapshot
        .head_eval_reports
        .iter()
        .max_by_key(|report| report.finished_at)
}

fn network_performance_view(bundle: &MetricsCatchupBundle) -> Option<BrowserAppPerformanceView> {
    let summary = bundle.snapshot.performance_summary.as_ref()?;
    let wait_time_ms = summary
        .training
        .wait_time_ms
        .saturating_add(summary.validation.wait_time_ms);
    let idle_time_ms = summary
        .training
        .idle_time_ms
        .saturating_add(summary.validation.idle_time_ms);
    Some(BrowserAppPerformanceView {
        scope_summary: format!(
            "{} peer(s) · {} train window(s) · {} eval report(s)",
            summary.total_peer_count,
            summary.training.window_count,
            summary.head_evaluation.report_count
        ),
        captured_at: summary.captured_at.to_rfc3339(),
        training_throughput: format_rate(summary.training.throughput_work_units_per_sec, "work/s"),
        validation_throughput: format_rate(
            summary.head_evaluation.throughput_samples_per_sec,
            "sample/s",
        ),
        wait_time: format_duration_ms(wait_time_ms),
        idle_time: format_duration_ms(idle_time_ms),
    })
}

fn network_diffusion_view(bundle: &MetricsCatchupBundle) -> Option<BrowserAppDiffusionView> {
    let histogram = derive_latest_canonical_head_population_histograms(
        &bundle.snapshot.peer_window_metrics,
        &bundle.snapshot.head_eval_reports,
    )
    .into_iter()
    .next()?;
    let latest_bucket = histogram
        .buckets
        .iter()
        .find(|bucket| bucket.is_latest_canonical)?;
    let curve = derive_canonical_head_adoption_curves(
        &bundle.snapshot.peer_window_metrics,
        &bundle.snapshot.head_eval_reports,
    )
    .into_iter()
    .find(|curve| curve.canonical_head_id == histogram.canonical_head_id);
    let timeline = curve
        .as_ref()
        .and_then(|curve| curve.points.last().map(|point| (curve, point)))
        .map(|(curve, point)| {
            format!(
                "{} point(s) over {}",
                curve.points.len(),
                format_duration_ms(point.elapsed_since_certified_ms)
            )
        })
        .unwrap_or_else(|| "no post-certification windows yet".into());
    Some(BrowserAppDiffusionView {
        canonical_head_id: histogram.canonical_head_id.as_str().to_owned(),
        captured_at: histogram.captured_at.to_rfc3339(),
        peer_adoption: format!(
            "{} / {} peers",
            latest_bucket.peer_count, histogram.total_visible_peer_count
        ),
        recent_window_adoption: format!(
            "{} / {} windows",
            latest_bucket.recent_window_count, histogram.total_recent_window_count
        ),
        fragmentation: format!("{} visible head(s)", histogram.buckets.len()),
        timeline,
    })
}

fn eval_summary(report: &burn_p2p_core::HeadEvalReport) -> String {
    let primary_metric = report
        .metric_values
        .iter()
        .next()
        .map(|(key, value)| format!("{key} {}", metric_value_label(value)))
        .unwrap_or_else(|| "metrics pending".to_owned());
    format!(
        "{} · {} sample(s) · {}",
        match report.status {
            burn_p2p_core::HeadEvalStatus::Completed => "complete",
            burn_p2p_core::HeadEvalStatus::Partial => "partial",
            burn_p2p_core::HeadEvalStatus::Failed => "failed",
            burn_p2p_core::HeadEvalStatus::Superseded => "superseded",
            burn_p2p_core::HeadEvalStatus::Skipped => "skipped",
        },
        report.sample_count,
        primary_metric
    )
}

fn metric_value_label(value: &MetricValue) -> String {
    match value {
        MetricValue::Integer(value) => value.to_string(),
        MetricValue::Float(value) => format!("{value:.4}"),
        MetricValue::Bool(value) => {
            if *value {
                "true".to_owned()
            } else {
                "false".to_owned()
            }
        }
        MetricValue::Text(value) => value.clone(),
    }
}

fn metric_preview(
    metrics: &std::collections::BTreeMap<String, MetricValue>,
) -> Vec<BrowserAppMetricPreview> {
    let mut preferred = ["accuracy", "loss", "train_loss_mean", "digit_zero_accuracy"]
        .into_iter()
        .filter_map(|key| {
            metrics.get(key).map(|value| BrowserAppMetricPreview {
                label: key.to_owned(),
                value: metric_value_label(value),
            })
        })
        .collect::<Vec<_>>();
    let preferred_labels = preferred
        .iter()
        .map(|preview| preview.label.clone())
        .collect::<std::collections::BTreeSet<_>>();

    let remaining = metrics
        .iter()
        .filter(|(key, _)| !preferred_labels.contains(key.as_str()))
        .take(4usize.saturating_sub(preferred.len()))
        .map(|(key, value)| BrowserAppMetricPreview {
            label: key.clone(),
            value: metric_value_label(value),
        });
    preferred.extend(remaining);
    preferred
}

fn training_throughput_summary(metrics: &burn_p2p_core::PeerWindowMetrics) -> Option<String> {
    metrics.accepted_tokens_or_samples.and_then(|accepted| {
        if metrics.compute_time_ms == 0 {
            None
        } else {
            Some(format!(
                "{:.1} sample/s",
                accepted as f64 / (metrics.compute_time_ms as f64 / 1_000.0)
            ))
        }
    })
}

fn format_rate(value: f64, unit: &str) -> String {
    if value >= 100.0 {
        format!("{value:.0} {unit}")
    } else if value >= 10.0 {
        format!("{value:.1} {unit}")
    } else {
        format!("{value:.2} {unit}")
    }
}

fn format_duration_ms(value: u64) -> String {
    if value < 1_000 {
        format!("{value} ms")
    } else if value < 60_000 {
        format!("{:.1}s", value as f64 / 1_000.0)
    } else {
        format!("{:.1}m", value as f64 / 60_000.0)
    }
}

fn default_surface(state: &BrowserRuntimeState) -> BrowserAppSurface {
    match state {
        BrowserRuntimeState::Joining { role, .. } => match role {
            BrowserRuntimeRole::BrowserVerifier => BrowserAppSurface::Validate,
            BrowserRuntimeRole::BrowserTrainerWgpu => BrowserAppSurface::Train,
            BrowserRuntimeRole::Viewer
            | BrowserRuntimeRole::BrowserObserver
            | BrowserRuntimeRole::BrowserFallback => BrowserAppSurface::Viewer,
        },
        BrowserRuntimeState::Verifier => BrowserAppSurface::Validate,
        BrowserRuntimeState::Trainer => BrowserAppSurface::Train,
        BrowserRuntimeState::Blocked { .. } => BrowserAppSurface::Network,
        BrowserRuntimeState::ViewerOnly
        | BrowserRuntimeState::Observer
        | BrowserRuntimeState::BackgroundSuspended { .. }
        | BrowserRuntimeState::Catchup { .. } => BrowserAppSurface::Viewer,
    }
}

fn runtime_label(state: &BrowserRuntimeState) -> String {
    match state {
        BrowserRuntimeState::ViewerOnly => "portal".into(),
        BrowserRuntimeState::Joining { role, .. } => format!("joining {}", role_label(role)),
        BrowserRuntimeState::Observer => "observe".into(),
        BrowserRuntimeState::Verifier => "validate".into(),
        BrowserRuntimeState::Trainer => "train".into(),
        BrowserRuntimeState::BackgroundSuspended { role } => role
            .as_ref()
            .map(|role| format!("suspended {}", role_label(role)))
            .unwrap_or_else(|| "suspended".into()),
        BrowserRuntimeState::Catchup { role } => format!("catchup {}", role_label(role)),
        BrowserRuntimeState::Blocked { .. } => "blocked".into(),
    }
}

fn runtime_detail(state: &BrowserRuntimeState, storage: &BrowserStorageSnapshot) -> String {
    match state {
        BrowserRuntimeState::ViewerOnly => "signed snapshots only".into(),
        BrowserRuntimeState::Joining { stage, role } => {
            join_stage_detail(stage, role, storage).into()
        }
        BrowserRuntimeState::Observer => "watching heads and standings".into(),
        BrowserRuntimeState::Verifier => storage
            .last_head_id
            .as_ref()
            .map(|head_id| format!("reviewing {}", head_id.as_str()))
            .unwrap_or_else(|| "waiting for a checkpoint".into()),
        BrowserRuntimeState::Trainer => training_slice_status(state, storage, None),
        BrowserRuntimeState::BackgroundSuspended { .. } => "paused".into(),
        BrowserRuntimeState::Catchup { role } => match role {
            BrowserRuntimeRole::BrowserTrainerWgpu => "catching up before training".into(),
            BrowserRuntimeRole::BrowserVerifier => "catching up before validation".into(),
            _ => "catching up".into(),
        },
        BrowserRuntimeState::Blocked { reason } => reason.clone(),
    }
}

fn join_stage_detail(
    stage: &crate::BrowserJoinStage,
    role: &BrowserRuntimeRole,
    storage: &BrowserStorageSnapshot,
) -> &'static str {
    match stage {
        crate::BrowserJoinStage::Authenticating => "checking browser auth",
        crate::BrowserJoinStage::Enrolling => "enrolling local browser identity",
        crate::BrowserJoinStage::DirectorySync => "loading visible revisions",
        crate::BrowserJoinStage::HeadSync => match role {
            BrowserRuntimeRole::BrowserTrainerWgpu | BrowserRuntimeRole::BrowserVerifier
                if storage.last_head_id.is_none() =>
            {
                "waiting for the first checkpoint"
            }
            BrowserRuntimeRole::BrowserTrainerWgpu => "syncing checkpoint before training",
            BrowserRuntimeRole::BrowserVerifier => "syncing checkpoint before validation",
            _ => "syncing visible heads",
        },
        crate::BrowserJoinStage::TransportConnect => "connecting peer transport",
    }
}

fn capability_summary(capability: Option<&BrowserCapabilityReport>) -> String {
    let Some(capability) = capability else {
        return "checking".into();
    };
    let gpu = match capability.gpu_support {
        BrowserGpuSupport::Available => "webgpu",
        BrowserGpuSupport::Unavailable(_) => "cpu",
        BrowserGpuSupport::Unknown => "unknown",
    };
    format!("{gpu} · {}", role_label(&capability.recommended_role))
}

fn session_label(session: &BrowserSessionState) -> String {
    match (session.session.as_ref(), session.reenrollment_required) {
        (Some(principal), false) => short_identity_label(principal.claims.principal_id.as_str()),
        (Some(principal), true) => {
            format!(
                "{} · reenroll",
                short_identity_label(principal.claims.principal_id.as_str())
            )
        }
        (None, true) => "reenroll".into(),
        (None, false) => "guest".into(),
    }
}

fn network_note(runtime: &BrowserWorkerRuntime) -> String {
    let swarm_status = runtime.swarm_status();
    let artifact_note = match swarm_status.artifact_source {
        burn_p2p_core::BrowserArtifactSource::PeerSwarm => "artifact via peer swarm",
        burn_p2p_core::BrowserArtifactSource::EdgeHttp => "artifact via edge fallback",
        burn_p2p_core::BrowserArtifactSource::Unavailable => "artifact pending",
    };
    if swarm_status.connected_transport.is_some() {
        if runtime.storage.last_signed_directory_snapshot.is_some() {
            return format!("swarm control live · {artifact_note}.");
        }
        if runtime.storage.directory_snapshot().is_some() {
            return format!("swarm control cached · {artifact_note}.");
        }
    }
    let Some(config) = runtime.config.as_ref() else {
        return format!("tracks one edge and its signed snapshots · {artifact_note}.");
    };
    match config.seed_bootstrap.source {
        BrowserSeedBootstrapSource::Unavailable => {
            format!("tracks one edge and its signed snapshots · {artifact_note}.")
        }
        BrowserSeedBootstrapSource::EdgeSigned => format!(
            "edge published {} browser seed addrs · {artifact_note}.",
            config.seed_bootstrap.seed_node_urls.len(),
        ),
        BrowserSeedBootstrapSource::SiteConfigFallback => format!(
            "using {} site-config seed addrs · {artifact_note}.",
            config.seed_bootstrap.seed_node_urls.len(),
        ),
        BrowserSeedBootstrapSource::Merged => format!(
            "edge + site seeds merged ({} total) · {artifact_note}.",
            config.seed_bootstrap.seed_node_urls.len(),
        ),
    }
}

fn is_validation_active(state: &BrowserRuntimeState) -> bool {
    matches!(
        state,
        BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserVerifier,
            ..
        } | BrowserRuntimeState::Verifier
            | BrowserRuntimeState::Catchup {
                role: BrowserRuntimeRole::BrowserVerifier,
            }
    )
}

fn is_training_active(state: &BrowserRuntimeState) -> bool {
    matches!(
        state,
        BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            ..
        } | BrowserRuntimeState::Trainer
            | BrowserRuntimeState::Catchup {
                role: BrowserRuntimeRole::BrowserTrainerWgpu,
            }
    )
}

fn role_label(role: &BrowserRuntimeRole) -> &'static str {
    match role {
        BrowserRuntimeRole::Viewer => "portal",
        BrowserRuntimeRole::BrowserObserver => "observe",
        BrowserRuntimeRole::BrowserVerifier => "validate",
        BrowserRuntimeRole::BrowserTrainerWgpu => "train",
        BrowserRuntimeRole::BrowserFallback => "fallback",
    }
}

fn training_slice_status(
    state: &BrowserRuntimeState,
    storage: &BrowserStorageSnapshot,
    latest_peer_window: Option<&burn_p2p_core::PeerWindowMetrics>,
) -> String {
    if storage.active_assignment.is_none() {
        return "waiting for work".into();
    }
    if let Some(revision_id) = storage.active_assignment_rollover_revision() {
        return format!("waiting for revision {}", revision_id.as_str());
    }
    if storage.last_head_id.is_none() {
        return "waiting for checkpoint sync".into();
    }
    if storage.cached_microshards.is_empty() {
        return match state {
            BrowserRuntimeState::Trainer => "slice loads when training starts".into(),
            BrowserRuntimeState::Catchup {
                role: BrowserRuntimeRole::BrowserTrainerWgpu,
            } => "waiting to load the assigned slice".into(),
            BrowserRuntimeState::Joining {
                role: BrowserRuntimeRole::BrowserTrainerWgpu,
                ..
            } => "waiting to load the assigned slice".into(),
            _ => "downloading assigned slice".into(),
        };
    }

    let microshards_ready = storage.cached_microshards.len();
    match state {
        BrowserRuntimeState::Trainer => {
            if let Some(metrics) = latest_peer_window
                && let Some(accepted) = metrics.accepted_tokens_or_samples
            {
                let remaining = metrics.attempted_tokens_or_samples.saturating_sub(accepted);
                return format!(
                    "{} microshard{} cached · {} left in slice",
                    microshards_ready,
                    plural_suffix(microshards_ready),
                    remaining
                );
            }
            format!(
                "{} microshard{} cached · training ready",
                microshards_ready,
                plural_suffix(microshards_ready)
            )
        }
        BrowserRuntimeState::Catchup {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
        } => format!(
            "{} microshard{} cached · catching up",
            microshards_ready,
            plural_suffix(microshards_ready)
        ),
        BrowserRuntimeState::Joining {
            role: BrowserRuntimeRole::BrowserTrainerWgpu,
            ..
        } => format!(
            "{} microshard{} cached · waiting to join",
            microshards_ready,
            plural_suffix(microshards_ready)
        ),
        _ => format!(
            "{} microshard{} cached",
            microshards_ready,
            plural_suffix(microshards_ready)
        ),
    }
}

fn active_training_lease_summary(
    storage: &BrowserStorageSnapshot,
) -> Option<BrowserAppTrainingLeaseView> {
    storage
        .active_training_lease
        .as_ref()
        .map(|lease| BrowserAppTrainingLeaseView {
            lease_id: lease.lease_id.as_str().to_owned(),
            window_id: lease.window_id.0,
            dataset_view_id: lease.dataset_view_id.as_str().to_owned(),
            assignment_hash: lease.assignment_hash.as_str().to_owned(),
            microshard_count: lease.microshards.len(),
        })
}

fn plural_suffix(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn short_identity_label(value: &str) -> String {
    if value.len() <= 18 {
        return value.to_owned();
    }
    format!("{}…{}", &value[..8], &value[value.len() - 6..])
}
