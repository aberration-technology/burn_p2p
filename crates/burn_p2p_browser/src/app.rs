use burn_p2p::{
    BrowserMode, ContentId, ExperimentDirectoryEntry, ExperimentId, MetricValue, RevisionId,
};
use burn_p2p_metrics::{
    MetricsCatchupBundle, derive_canonical_head_adoption_curves,
    derive_latest_canonical_head_population_histograms,
};
use burn_p2p_views::{
    BrowserAppClientView, BrowserAppDiffusionView, BrowserAppExperimentSummary,
    BrowserAppLeaderboardPreview, BrowserAppMetricPreview, BrowserAppNetworkView,
    BrowserAppPerformanceView, BrowserAppSurface, BrowserAppTrainingView, BrowserAppValidationView,
    BrowserAppViewerView,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    BrowserAuthClientError, BrowserCapabilityReport, BrowserEdgeClient, BrowserEdgeSnapshot,
    BrowserEnrollmentConfig, BrowserGpuSupport, BrowserRuntimeConfig, BrowserRuntimeRole,
    BrowserRuntimeState, BrowserSessionState, BrowserStorageSnapshot, BrowserTransportKind,
    BrowserTransportStatus, BrowserUiBindings, BrowserWorkerEvent, BrowserWorkerRuntime,
    durability::{
        clear_durable_receipt_outbox, load_durable_receipt_outbox, persist_durable_receipt_outbox,
    },
};

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
                    .last_signed_directory_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.payload.payload.network_id.as_str().to_owned())
            })
            .unwrap_or_else(|| "unknown".into());
        let runtime_state = self
            .runtime
            .state
            .clone()
            .unwrap_or(BrowserRuntimeState::ViewerOnly);
        let storage = &self.runtime.storage;
        let session = &storage.session;
        let directory = storage
            .last_signed_directory_snapshot
            .as_ref()
            .map(|snapshot| &snapshot.payload.payload);
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
        let direct_peers = usize::from(self.runtime.transport.active.is_some());
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
                slice_status: training_slice_status(&runtime_state, storage, latest_peer_window),
                latest_head_id: storage
                    .last_head_id
                    .as_ref()
                    .map(|head_id| head_id.as_str().to_owned()),
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
                transport: transport_label(&self.runtime.transport),
                node_state: runtime_label(&runtime_state),
                direct_peers,
                observed_peers: leaderboard_entries,
                estimated_network_size: leaderboard_entries.max(direct_peers),
                accepted_receipts,
                certified_merges: visible_heads,
                in_flight_transfers: 0,
                network_note: "Tracks one edge and its signed snapshots.".into(),
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
}

impl BrowserAppController {
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
        } = config;
        Self::connect(
            edge_base_url,
            capability,
            target.preferred_role(),
            selected_experiment_id.map(|experiment_id| (experiment_id, selected_revision_id)),
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
    ) -> Result<Self, BrowserAuthClientError> {
        let edge_base_url = edge_base_url.into().trim_end_matches('/').to_owned();
        let snapshot = fetch_edge_snapshot(&edge_base_url).await?;
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
            ),
            capability,
            BrowserTransportStatus {
                active: None,
                webrtc_direct_enabled: snapshot.transports.webrtc_direct,
                webtransport_enabled: snapshot.transports.webtransport_gateway,
                wss_fallback_enabled: snapshot.transports.wss_fallback,
                last_error: None,
            },
        );
        let mut runtime = runtime;
        runtime.storage.pending_receipts = load_durable_receipt_outbox(&snapshot.network_id)
            .map_err(BrowserAuthClientError::ArtifactTransport)?;
        let mut controller = Self {
            edge_client,
            model: BrowserAppModel::from_runtime(runtime),
        };
        let _ = controller.refresh().await?;
        Ok(controller)
    }

    /// Refreshes local runtime state from the connected bootstrap edge.
    pub async fn refresh(&mut self) -> Result<BrowserAppClientView, BrowserAuthClientError> {
        let mut runtime = self.model.runtime.clone();
        let session = runtime.storage.session.clone();
        let events = self
            .edge_client
            .sync_worker_runtime(&mut runtime, Some(&session), true)
            .await?;
        let previous_error = self.model.last_error.clone();
        let mut model = BrowserAppModel::from_runtime(runtime);
        model.last_error = previous_error;
        for event in events {
            model.apply_event(event);
        }
        self.model = model;
        self.persist_receipt_outbox()?;
        Ok(self.view())
    }

    fn persist_receipt_outbox(&self) -> Result<(), BrowserAuthClientError> {
        let network_id = self
            .model
            .runtime
            .config
            .as_ref()
            .map(|config| config.network_id.clone())
            .or_else(|| {
                self.model
                    .runtime
                    .storage
                    .last_signed_directory_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.payload.payload.network_id.clone())
            })
            .ok_or_else(|| {
                BrowserAuthClientError::ArtifactTransport(
                    "browser runtime is missing a network id for durable receipt outbox".into(),
                )
            })?;
        if self.model.runtime.storage.pending_receipts.is_empty() {
            clear_durable_receipt_outbox(&network_id)
                .map_err(BrowserAuthClientError::ArtifactTransport)
        } else {
            persist_durable_receipt_outbox(
                &network_id,
                &self.model.runtime.storage.pending_receipts,
            )
            .map_err(BrowserAuthClientError::ArtifactTransport)
        }
    }

    /// Returns the current browser-app client view derived from the local runtime.
    pub fn view(&self) -> BrowserAppClientView {
        self.model.view(self.edge_client.bindings())
    }
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

fn runtime_config_from_snapshot(
    edge_base_url: &str,
    snapshot: &BrowserEdgeSnapshot,
    capability: &BrowserCapabilityReport,
    requested_role: BrowserRuntimeRole,
    selected_experiment: Option<(String, Option<String>)>,
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

fn transport_label(transport: &BrowserTransportStatus) -> String {
    match transport.active.as_ref() {
        Some(BrowserTransportKind::WebRtcDirect) => "webrtc-direct".into(),
        Some(BrowserTransportKind::WebTransport) => "webtransport".into(),
        Some(BrowserTransportKind::WssFallback) => "wss-fallback".into(),
        None => "offline".into(),
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
    if storage.last_head_id.is_none() {
        return "waiting for checkpoint sync".into();
    }
    if storage.cached_microshards.is_empty() {
        return "downloading assigned slice".into();
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

fn plural_suffix(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn short_identity_label(value: &str) -> String {
    if value.len() <= 18 {
        return value.to_owned();
    }
    format!("{}…{}", &value[..8], &value[value.len() - 6..])
}
