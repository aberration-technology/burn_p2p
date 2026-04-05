use burn_p2p::{
    BrowserMode, ContentId, ExperimentDirectoryEntry, ExperimentId, MetricValue, RevisionId,
};
use burn_p2p_metrics::MetricsCatchupBundle;
use burn_p2p_ui::{
    BrowserAppClientView, BrowserAppExperimentSummary, BrowserAppLeaderboardPreview,
    BrowserAppNetworkView, BrowserAppSurface, BrowserAppTrainingView, BrowserAppValidationView,
    BrowserAppViewerView,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    BrowserAuthClientError, BrowserCapabilityReport, BrowserEnrollmentConfig, BrowserGpuSupport,
    BrowserPortalClient, BrowserPortalSnapshot, BrowserRuntimeConfig, BrowserRuntimeRole,
    BrowserRuntimeState, BrowserSessionState, BrowserStorageSnapshot, BrowserTransportKind,
    BrowserTransportStatus, BrowserUiBindings, BrowserWorkerEvent, BrowserWorkerRuntime,
};

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
            .unwrap_or(BrowserRuntimeState::PortalOnly);
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
            },
        }
    }
}

#[derive(Clone, Debug)]
/// Browser-owned controller for the static wasm app.
pub struct BrowserAppController {
    portal_client: BrowserPortalClient,
    model: BrowserAppModel,
}

impl BrowserAppController {
    /// Connects the browser app to one bootstrap edge and derives initial local runtime state.
    pub async fn connect(
        edge_base_url: impl Into<String>,
        capability: BrowserCapabilityReport,
        requested_role: BrowserRuntimeRole,
        selected_experiment: Option<(String, Option<String>)>,
    ) -> Result<Self, BrowserAuthClientError> {
        let edge_base_url = edge_base_url.into().trim_end_matches('/').to_owned();
        let snapshot = fetch_portal_snapshot(&edge_base_url).await?;
        let bindings = BrowserUiBindings::from_portal_snapshot(&edge_base_url, &snapshot);
        let portal_client = BrowserPortalClient::new(
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
        let mut controller = Self {
            portal_client,
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
            .portal_client
            .sync_worker_runtime(&mut runtime, Some(&session), true)
            .await?;
        let previous_error = self.model.last_error.clone();
        let mut model = BrowserAppModel::from_runtime(runtime);
        model.last_error = previous_error;
        for event in events {
            model.apply_event(event);
        }
        self.model = model;
        Ok(self.view())
    }

    /// Returns the current browser-app client view derived from the local runtime.
    pub fn view(&self) -> BrowserAppClientView {
        self.model.view(self.portal_client.bindings())
    }
}

async fn fetch_portal_snapshot(
    edge_base_url: &str,
) -> Result<BrowserPortalSnapshot, BrowserAuthClientError> {
    reqwest::Client::new()
        .get(BrowserUiBindings::new(edge_base_url).endpoint_url("/portal/snapshot"))
        .send()
        .await?
        .error_for_status()?
        .json::<BrowserPortalSnapshot>()
        .await
        .map_err(Into::into)
}

fn runtime_config_from_snapshot(
    edge_base_url: &str,
    snapshot: &BrowserPortalSnapshot,
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
    snapshot: &BrowserPortalSnapshot,
    capability: &BrowserCapabilityReport,
    requested_role: BrowserRuntimeRole,
) -> BrowserRuntimeRole {
    if matches!(snapshot.browser_mode, BrowserMode::Disabled) {
        return BrowserRuntimeRole::PortalViewer;
    }

    match requested_role {
        BrowserRuntimeRole::PortalViewer | BrowserRuntimeRole::BrowserObserver => {
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
                    experiment_summary(entry, &BrowserRuntimeState::PortalOnly, capability)
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

fn default_surface(state: &BrowserRuntimeState) -> BrowserAppSurface {
    match state {
        BrowserRuntimeState::Joining { role, .. } => match role {
            BrowserRuntimeRole::BrowserVerifier => BrowserAppSurface::Validate,
            BrowserRuntimeRole::BrowserTrainerWgpu => BrowserAppSurface::Train,
            BrowserRuntimeRole::PortalViewer
            | BrowserRuntimeRole::BrowserObserver
            | BrowserRuntimeRole::BrowserFallback => BrowserAppSurface::Viewer,
        },
        BrowserRuntimeState::Verifier => BrowserAppSurface::Validate,
        BrowserRuntimeState::Trainer => BrowserAppSurface::Train,
        BrowserRuntimeState::Blocked { .. } => BrowserAppSurface::Network,
        BrowserRuntimeState::PortalOnly
        | BrowserRuntimeState::Observer
        | BrowserRuntimeState::BackgroundSuspended { .. }
        | BrowserRuntimeState::Catchup { .. } => BrowserAppSurface::Viewer,
    }
}

fn runtime_label(state: &BrowserRuntimeState) -> String {
    match state {
        BrowserRuntimeState::PortalOnly => "viewer".into(),
        BrowserRuntimeState::Joining { role, .. } => format!("joining {}", role_label(role)),
        BrowserRuntimeState::Observer => "viewer".into(),
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
        BrowserRuntimeState::PortalOnly => "Heads and standings.".into(),
        BrowserRuntimeState::Joining { stage, .. } => join_stage_label(stage).into(),
        BrowserRuntimeState::Observer => "Heads and standings.".into(),
        BrowserRuntimeState::Verifier => storage
            .last_head_id
            .as_ref()
            .map(|head_id| format!("Reviewing {}", head_id.as_str()))
            .unwrap_or_else(|| "Ready to validate.".into()),
        BrowserRuntimeState::Trainer => storage
            .active_assignment
            .as_ref()
            .map(|assignment| {
                format!(
                    "{}/{}",
                    assignment.experiment_id.as_str(),
                    assignment.revision_id.as_str()
                )
            })
            .unwrap_or_else(|| "Ready to train.".into()),
        BrowserRuntimeState::BackgroundSuspended { .. } => "Paused.".into(),
        BrowserRuntimeState::Catchup { .. } => "Catching up.".into(),
        BrowserRuntimeState::Blocked { reason } => reason.clone(),
    }
}

fn join_stage_label(stage: &crate::BrowserJoinStage) -> &'static str {
    match stage {
        crate::BrowserJoinStage::Authenticating => "authenticating",
        crate::BrowserJoinStage::Enrolling => "enrolling",
        crate::BrowserJoinStage::DirectorySync => "directory sync",
        crate::BrowserJoinStage::HeadSync => "head sync",
        crate::BrowserJoinStage::TransportConnect => "transport connect",
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
        (None, false) => "viewer".into(),
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
        BrowserRuntimeRole::PortalViewer => "viewer",
        BrowserRuntimeRole::BrowserObserver => "viewer",
        BrowserRuntimeRole::BrowserVerifier => "validate",
        BrowserRuntimeRole::BrowserTrainerWgpu => "train",
        BrowserRuntimeRole::BrowserFallback => "fallback",
    }
}

fn short_identity_label(value: &str) -> String {
    if value.len() <= 18 {
        return value.to_owned();
    }
    format!("{}…{}", &value[..8], &value[value.len() - 6..])
}
