use std::collections::BTreeMap;

use burn_p2p::{
    ArtifactId, BrowserRole, ContentId, ContributionReceipt, ContributionReceiptId, ExperimentId,
    HeadDescriptor, HeadId, MetricValue, PeerId, RevisionId, StudyId,
};
use burn_p2p_core::{MetricsLiveEvent, SchemaEnvelope, SignedPayload};
use burn_p2p_metrics::MetricsCatchupBundle;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    BrowserCapabilityReport, BrowserDirectorySnapshot, BrowserLeaderboardSnapshot,
    BrowserRuntimeConfig, BrowserRuntimeState, BrowserSessionState, BrowserStorageSnapshot,
    BrowserStoredAssignment, BrowserTrainingPlan, BrowserTrainingResult, BrowserTransportStatus,
    BrowserValidationPlan, BrowserValidationResult, BrowserWorkerCommand, BrowserWorkerEvent,
    browser_experiment_candidate_for_selection, recommended_browser_candidate_for_scopes,
};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
/// Captures optional metrics state carried alongside one browser edge sync.
pub struct BrowserMetricsSyncState {
    /// Catchup bundles that seed the browser-side metrics cache.
    pub catchup_bundles: Vec<MetricsCatchupBundle>,
    /// The latest live event observed by the browser client.
    pub live_event: Option<MetricsLiveEvent>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
/// Represents a browser worker runtime.
pub struct BrowserWorkerRuntime {
    /// The config.
    pub config: Option<BrowserRuntimeConfig>,
    /// The state.
    pub state: Option<BrowserRuntimeState>,
    /// The capability.
    pub capability: Option<BrowserCapabilityReport>,
    /// The transport.
    pub transport: BrowserTransportStatus,
    /// The storage.
    pub storage: BrowserStorageSnapshot,
}

impl BrowserWorkerRuntime {
    const MAX_RECEIPT_SUBMISSION_BATCH: usize = 64;

    fn is_duplicate_metrics_event(&self, event: &MetricsLiveEvent) -> bool {
        self.storage
            .last_metrics_live_event
            .as_ref()
            .map(|previous| {
                previous.network_id == event.network_id
                    && previous.kind == event.kind
                    && previous.cursors == event.cursors
            })
            .unwrap_or(false)
    }

    fn assignment_metrics_head(&self, event: &MetricsLiveEvent) -> Option<HeadId> {
        let assignment = self.storage.active_assignment.as_ref()?;
        event
            .cursors
            .iter()
            .find(|cursor| {
                cursor.experiment_id == assignment.experiment_id
                    && cursor.revision_id == assignment.revision_id
            })
            .and_then(|cursor| cursor.latest_head_id.clone())
    }

    /// Applies one metrics-only sync update without requiring a full edge
    /// directory or head refresh.
    pub fn apply_metrics_sync_state(
        &mut self,
        metrics: BrowserMetricsSyncState,
    ) -> Vec<BrowserWorkerEvent> {
        let previous_storage = self.storage.clone();
        let mut events = Vec::new();

        if !metrics.catchup_bundles.is_empty() {
            self.storage
                .remember_metrics_catchup(metrics.catchup_bundles);
        }
        if let Some(event) = metrics.live_event {
            events.extend(self.apply_metrics_live_event(event));
        }
        if self.storage != previous_storage
            && !events
                .iter()
                .any(|event| matches!(event, BrowserWorkerEvent::StorageUpdated(_)))
        {
            events.push(BrowserWorkerEvent::StorageUpdated(Box::new(
                self.storage.clone(),
            )));
        }
        events
    }

    fn runtime_role_from_browser_role(role: BrowserRole) -> crate::BrowserRuntimeRole {
        match role {
            BrowserRole::Viewer => crate::BrowserRuntimeRole::Viewer,
            BrowserRole::Observer => crate::BrowserRuntimeRole::BrowserObserver,
            BrowserRole::Verifier => crate::BrowserRuntimeRole::BrowserVerifier,
            BrowserRole::TrainerWgpu => crate::BrowserRuntimeRole::BrowserTrainerWgpu,
            BrowserRole::Fallback => crate::BrowserRuntimeRole::BrowserFallback,
        }
    }

    fn active_state_for_role(role: crate::BrowserRuntimeRole) -> BrowserRuntimeState {
        match role {
            crate::BrowserRuntimeRole::Viewer => BrowserRuntimeState::ViewerOnly,
            crate::BrowserRuntimeRole::BrowserObserver
            | crate::BrowserRuntimeRole::BrowserFallback => BrowserRuntimeState::Observer,
            crate::BrowserRuntimeRole::BrowserVerifier => BrowserRuntimeState::Verifier,
            crate::BrowserRuntimeRole::BrowserTrainerWgpu => BrowserRuntimeState::Trainer,
        }
    }

    fn role_requires_peer_transport(role: &crate::BrowserRuntimeRole) -> bool {
        Self::active_state_for_role(role.clone()).requires_peer_transport()
    }

    fn synchronize_transport_state(&mut self) {
        let Some(state) = self.state.clone() else {
            return;
        };
        match state {
            BrowserRuntimeState::Joining {
                role,
                stage: crate::BrowserJoinStage::TransportConnect,
            } if self.transport.active.is_some() => {
                self.state = Some(Self::active_state_for_role(role));
            }
            BrowserRuntimeState::Observer
            | BrowserRuntimeState::Verifier
            | BrowserRuntimeState::Trainer
            | BrowserRuntimeState::Catchup { .. }
                if self.transport.active.is_none() =>
            {
                if let Some(role) = state
                    .active_role()
                    .filter(Self::role_requires_peer_transport)
                {
                    self.state = Some(BrowserRuntimeState::joining(
                        role,
                        crate::BrowserJoinStage::TransportConnect,
                    ));
                }
            }
            _ => {}
        }
    }

    fn current_assignment_head<'a>(
        &self,
        heads: &'a [HeadDescriptor],
    ) -> Option<&'a HeadDescriptor> {
        let assignment = self.storage.active_assignment.as_ref()?;
        heads
            .iter()
            .filter(|head| {
                head.study_id == assignment.study_id
                    && head.experiment_id == assignment.experiment_id
                    && head.revision_id == assignment.revision_id
            })
            .max_by(|left, right| {
                left.global_step
                    .cmp(&right.global_step)
                    .then_with(|| left.created_at.cmp(&right.created_at))
            })
    }

    fn receipt_peer_id(&self) -> PeerId {
        self.storage
            .stored_certificate_peer_id
            .clone()
            .unwrap_or_else(|| PeerId::new("browser-unenrolled-peer"))
    }

    fn can_run_validation(&self) -> bool {
        matches!(
            self.state,
            Some(BrowserRuntimeState::Observer)
                | Some(BrowserRuntimeState::Verifier)
                | Some(BrowserRuntimeState::Catchup { .. })
        )
    }

    fn can_run_training(&self) -> bool {
        matches!(
            self.state,
            Some(BrowserRuntimeState::Trainer)
                | Some(BrowserRuntimeState::Catchup {
                    role: crate::BrowserRuntimeRole::BrowserTrainerWgpu,
                })
        )
    }

    fn execute_validation(
        &mut self,
        plan: BrowserValidationPlan,
    ) -> Result<BrowserValidationResult, String> {
        if self.storage.session.session.is_none() {
            return Err("browser validation requires an authenticated session".into());
        }
        if !self.can_run_validation() {
            return Err("browser validation requires observer or verifier runtime state".into());
        }

        self.storage.remember_head(plan.head_id.clone());
        let receipt_id = if plan.emit_receipt {
            let assignment = self.storage.active_assignment.clone().ok_or_else(|| {
                "browser validation receipt emission requires an active assignment".to_owned()
            })?;
            let receipt_id = ContributionReceiptId::new(format!(
                "browser-validation-receipt-{}-{}",
                plan.head_id.as_str(),
                plan.sample_budget
            ));
            self.storage.queue_receipt(ContributionReceipt {
                receipt_id: receipt_id.clone(),
                peer_id: self.receipt_peer_id(),
                study_id: assignment.study_id,
                experiment_id: assignment.experiment_id,
                revision_id: assignment.revision_id,
                base_head_id: plan.head_id.clone(),
                artifact_id: ArtifactId::new(format!(
                    "browser-validation-artifact-{}",
                    plan.head_id.as_str()
                )),
                accepted_at: Utc::now(),
                accepted_weight: plan.sample_budget as f64,
                metrics: BTreeMap::from([(
                    "validated_chunks".into(),
                    MetricValue::Integer(plan.sample_budget as i64),
                )]),
                merge_cert_id: None,
            });
            Some(receipt_id)
        } else {
            None
        };

        Ok(BrowserValidationResult {
            head_id: plan.head_id,
            accepted: true,
            checked_chunks: plan.sample_budget as usize,
            emitted_receipt_id: receipt_id,
        })
    }

    fn execute_training(
        &mut self,
        plan: BrowserTrainingPlan,
    ) -> Result<BrowserTrainingResult, String> {
        if self.storage.session.session.is_none() {
            return Err("browser training requires an authenticated session".into());
        }
        if !self.can_run_training() {
            return Err("browser training requires trainer runtime state".into());
        }
        let Some(assignment) = self.storage.active_assignment.as_ref() else {
            return Err("browser training requires an active assignment".into());
        };
        if assignment.study_id != plan.study_id
            || assignment.experiment_id != plan.experiment_id
            || assignment.revision_id != plan.revision_id
        {
            return Err("browser training plan does not match the active assignment".into());
        }

        let artifact_id = ArtifactId::new(format!(
            "browser-artifact-{}-{}-{}",
            plan.experiment_id.as_str(),
            plan.revision_id.as_str(),
            plan.workload_id.as_str()
        ));
        let receipt_id = ContributionReceiptId::new(format!(
            "browser-training-receipt-{}-{}",
            plan.experiment_id.as_str(),
            plan.revision_id.as_str()
        ));
        self.storage.queue_receipt(ContributionReceipt {
            receipt_id: receipt_id.clone(),
            peer_id: self.receipt_peer_id(),
            study_id: plan.study_id.clone(),
            experiment_id: plan.experiment_id.clone(),
            revision_id: plan.revision_id.clone(),
            base_head_id: self
                .storage
                .last_head_id
                .clone()
                .unwrap_or_else(|| HeadId::new("browser-base-head")),
            artifact_id: artifact_id.clone(),
            accepted_at: Utc::now(),
            accepted_weight: plan.budget.max_window_secs as f64,
            metrics: BTreeMap::from([(
                "window_secs".into(),
                MetricValue::Integer(plan.budget.max_window_secs as i64),
            )]),
            merge_cert_id: None,
        });

        Ok(BrowserTrainingResult {
            artifact_id,
            receipt_id: Some(receipt_id),
            window_secs: plan.budget.max_window_secs,
        })
    }

    fn flush_receipt_outbox(
        &self,
    ) -> Result<(ContentId, String, Vec<ContributionReceipt>), String> {
        let session_id = self.storage.session.session_id().cloned().ok_or_else(|| {
            "browser receipt submission requires an authenticated session".to_owned()
        })?;
        let config = self.config.as_ref().ok_or_else(|| {
            "browser receipt submission requires runtime configuration".to_owned()
        })?;

        Ok((
            session_id,
            config.receipt_submit_path.clone(),
            self.storage
                .receipt_submission_batch(Self::MAX_RECEIPT_SUBMISSION_BATCH),
        ))
    }

    /// Performs the start operation.
    pub fn start(
        config: BrowserRuntimeConfig,
        capability: BrowserCapabilityReport,
        transport: BrowserTransportStatus,
    ) -> Self {
        let initial_state = BrowserRuntimeState::joining(
            config.role.clone(),
            crate::BrowserJoinStage::DirectorySync,
        );
        let mut runtime = Self {
            config: Some(config),
            state: Some(initial_state),
            capability: Some(capability),
            transport,
            storage: BrowserStorageSnapshot::default(),
        };
        runtime.refresh_transport_selection();
        runtime
    }

    /// Performs the stop operation.
    pub fn stop(&mut self) {
        self.config = None;
        self.state = None;
        self.capability = None;
        self.transport.active = None;
        self.storage.clear_assignment();
    }

    /// Performs the remember session operation.
    pub fn remember_session(&mut self, session: BrowserSessionState) {
        self.storage.remember_session(session);
    }

    /// Performs the select experiment operation.
    pub fn select_experiment(
        &mut self,
        experiment_id: ExperimentId,
        revision_id: Option<RevisionId>,
        directory: Option<&BrowserDirectorySnapshot>,
        session: Option<&BrowserSessionState>,
    ) {
        let Some(config) = self.config.as_mut() else {
            return;
        };
        config.selected_experiment = Some(experiment_id);
        config.selected_revision = revision_id;
        self.state = Some(BrowserRuntimeState::joining(
            config.role.clone(),
            crate::BrowserJoinStage::DirectorySync,
        ));
        self.storage.clear_assignment();
        if let Some(directory) = directory {
            self.apply_directory_snapshot(directory, session);
        } else {
            self.refresh_transport_selection();
        }
    }

    /// Performs the suspend operation.
    pub fn suspend(&mut self) {
        if let Some(state) = self.state.as_ref() {
            self.state = Some(BrowserRuntimeState::BackgroundSuspended {
                role: state.active_role(),
            });
            self.transport.active = None;
        }
    }

    /// Performs the resume operation.
    pub fn resume(&mut self) {
        let Some(config) = self.config.as_ref() else {
            return;
        };
        self.state = match self.state.clone() {
            Some(BrowserRuntimeState::BackgroundSuspended { role: Some(role) }) => {
                Some(BrowserRuntimeState::Catchup { role })
            }
            Some(BrowserRuntimeState::BackgroundSuspended { role: None }) => {
                Some(BrowserRuntimeState::ViewerOnly)
            }
            Some(state) => Some(state),
            None => Some(BrowserRuntimeState::joining(
                config.role.clone(),
                crate::BrowserJoinStage::DirectorySync,
            )),
        };
        self.refresh_transport_selection();
    }

    /// Performs the apply directory snapshot operation.
    pub fn apply_directory_snapshot(
        &mut self,
        directory: &BrowserDirectorySnapshot,
        session: Option<&BrowserSessionState>,
    ) {
        let (Some(config), Some(capability)) = (self.config.as_ref(), self.capability.as_ref())
        else {
            return;
        };
        let explicit_selection = config.selected_experiment.clone();
        let explicit_revision = config.selected_revision.clone();
        let scopes = session
            .and_then(|session| session.session.as_ref())
            .map(|session| {
                session
                    .claims
                    .granted_scopes
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let candidate = explicit_selection
            .as_ref()
            .and_then(|experiment_id| {
                browser_experiment_candidate_for_selection(
                    directory,
                    &config.target_artifact_id,
                    capability,
                    config.role.clone(),
                    experiment_id,
                    explicit_revision.as_ref(),
                    &scopes,
                )
            })
            .or_else(|| {
                explicit_selection.as_ref().map(|experiment_id| {
                    let revision_suffix = explicit_revision
                        .as_ref()
                        .map(|revision_id| format!(" revision {}", revision_id.as_str()))
                        .unwrap_or_default();
                    crate::BrowserExperimentCandidate {
                        policy: burn_p2p::BrowserJoinPolicy {
                            study_id: StudyId::new("unknown"),
                            experiment_id: experiment_id.clone(),
                            revision_id: explicit_revision
                                .clone()
                                .unwrap_or_else(|| RevisionId::new("unknown")),
                            workload_id: burn_p2p::WorkloadId::new("unknown"),
                            target_artifact: burn_p2p::ArtifactTargetKind::parse(
                                &config.target_artifact_id,
                            ),
                            visibility_policy: burn_p2p::BrowserVisibilityPolicy::Hidden,
                            eligible_roles: Vec::new(),
                            blocked_reasons: vec![format!(
                                "selected experiment {}{} is not browser-visible for this session",
                                experiment_id.as_str(),
                                revision_suffix
                            )],
                        },
                        recommended_role: None,
                        fallback_from_preferred: false,
                        recommended_state: BrowserRuntimeState::blocked(format!(
                            "selected experiment {}{} is not browser-visible for this session",
                            experiment_id.as_str(),
                            revision_suffix
                        )),
                    }
                })
            })
            .or_else(|| {
                recommended_browser_candidate_for_scopes(
                    directory,
                    &config.target_artifact_id,
                    capability,
                    config.role.clone(),
                    &scopes,
                )
            });
        if let Some(config) = self.config.as_mut() {
            if explicit_selection.is_none() {
                config.selected_experiment = candidate
                    .as_ref()
                    .filter(|candidate| candidate.policy.allows_peer_join())
                    .map(|candidate| candidate.policy.experiment_id.clone());
                config.selected_revision = candidate
                    .as_ref()
                    .filter(|candidate| candidate.policy.allows_peer_join())
                    .map(|candidate| candidate.policy.revision_id.clone());
            } else if config.selected_revision.is_none() {
                config.selected_revision = candidate
                    .as_ref()
                    .map(|candidate| candidate.policy.revision_id.clone());
            }
        }
        if let Some(candidate) = candidate
            .as_ref()
            .filter(|candidate| candidate.policy.allows_peer_join())
        {
            self.storage.remember_assignment(BrowserStoredAssignment {
                study_id: candidate.policy.study_id.clone(),
                experiment_id: candidate.policy.experiment_id.clone(),
                revision_id: candidate.policy.revision_id.clone(),
            });
        } else {
            self.storage.clear_assignment();
        }
        let state = candidate
            .map(|candidate| {
                if candidate.policy.allows_peer_join() {
                    let role = candidate
                        .recommended_role
                        .map(Self::runtime_role_from_browser_role)
                        .unwrap_or(crate::BrowserRuntimeRole::BrowserObserver);
                    BrowserRuntimeState::joining(role, crate::BrowserJoinStage::HeadSync)
                } else {
                    candidate.recommended_state
                }
            })
            .unwrap_or_else(|| {
                if session
                    .and_then(|session| session.session.as_ref())
                    .is_some()
                {
                    BrowserRuntimeState::blocked(
                        "no browser-visible experiments matched this session",
                    )
                } else {
                    BrowserRuntimeState::ViewerOnly
                }
            });
        self.state = Some(state);
        self.refresh_transport_selection();
    }

    /// Performs the apply head snapshot operation.
    pub fn apply_head_snapshot(&mut self, heads: &[HeadDescriptor]) -> Option<HeadId> {
        let matched_head_id = self
            .current_assignment_head(heads)
            .map(|head| head.head_id.clone());
        if let Some(head_id) = matched_head_id.clone() {
            self.storage.remember_head(head_id);
        }
        self.refresh_transport_selection();
        if matched_head_id.is_some() {
            self.state = match self.state.clone() {
                Some(BrowserRuntimeState::Joining { role, .. }) => Some(
                    if Self::role_requires_peer_transport(&role) && self.transport.active.is_none()
                    {
                        BrowserRuntimeState::joining(
                            role,
                            crate::BrowserJoinStage::TransportConnect,
                        )
                    } else {
                        Self::active_state_for_role(role)
                    },
                ),
                Some(BrowserRuntimeState::Catchup { role }) => Some(
                    if Self::role_requires_peer_transport(&role) && self.transport.active.is_none()
                    {
                        BrowserRuntimeState::joining(
                            role,
                            crate::BrowserJoinStage::TransportConnect,
                        )
                    } else {
                        Self::active_state_for_role(role)
                    },
                ),
                Some(state) => Some(state),
                None => None,
            };
        }
        matched_head_id
    }

    /// Performs the apply edge sync operation.
    pub fn apply_edge_sync(
        &mut self,
        signed_directory: SignedPayload<SchemaEnvelope<BrowserDirectorySnapshot>>,
        heads: &[HeadDescriptor],
        signed_leaderboard: Option<SignedPayload<SchemaEnvelope<BrowserLeaderboardSnapshot>>>,
        metrics: BrowserMetricsSyncState,
        transport: BrowserTransportStatus,
        session: Option<&BrowserSessionState>,
    ) -> Vec<BrowserWorkerEvent> {
        let previous_state = self.state.clone();
        let previous_transport = self.transport.clone();
        let previous_storage = self.storage.clone();
        let directory = signed_directory.payload.payload.clone();
        self.transport = transport;
        self.storage.remember_directory_snapshot(signed_directory);
        if let Some(snapshot) = signed_leaderboard {
            self.storage.remember_leaderboard_snapshot(snapshot);
        }
        if !metrics.catchup_bundles.is_empty() {
            self.storage
                .remember_metrics_catchup(metrics.catchup_bundles);
        }
        if let Some(event) = metrics.live_event {
            self.storage.remember_metrics_live_event(event);
        }
        self.apply_directory_snapshot(&directory, session);
        let active_head_id = self.apply_head_snapshot(heads);

        let mut events = Vec::new();
        if self.state != previous_state
            && let Some(state) = self.state.clone()
        {
            events.push(BrowserWorkerEvent::RuntimeStateChanged(state));
        }
        if self.transport != previous_transport {
            events.push(BrowserWorkerEvent::TransportChanged(self.transport.clone()));
        }
        if self.storage != previous_storage {
            events.push(BrowserWorkerEvent::StorageUpdated(Box::new(
                self.storage.clone(),
            )));
        }
        events.push(BrowserWorkerEvent::DirectoryUpdated {
            network_id: directory.network_id,
            visible_entries: directory.entries.len(),
        });
        if let Some(head_id) = active_head_id {
            events.push(BrowserWorkerEvent::HeadUpdated { head_id });
        }
        events
    }

    /// Applies one live metrics event to the browser runtime and storage.
    pub fn apply_metrics_live_event(&mut self, event: MetricsLiveEvent) -> Vec<BrowserWorkerEvent> {
        if self.is_duplicate_metrics_event(&event) {
            return Vec::new();
        }
        let mut events = Vec::new();
        let updated_head = self
            .assignment_metrics_head(&event)
            .filter(|head_id| self.storage.last_head_id.as_ref() != Some(head_id));
        self.storage.remember_metrics_live_event(event.clone());
        if let Some(head_id) = updated_head {
            self.storage.remember_head(head_id.clone());
            events.push(BrowserWorkerEvent::HeadUpdated { head_id });
        }
        events.push(BrowserWorkerEvent::MetricsUpdated(Box::new(event)));
        events.push(BrowserWorkerEvent::StorageUpdated(Box::new(
            self.storage.clone(),
        )));
        events
    }

    /// Performs the update transport status operation.
    pub fn update_transport_status(&mut self, transport: BrowserTransportStatus) {
        self.transport = transport;
        self.refresh_transport_selection();
    }

    /// Performs the apply command operation.
    pub fn apply_command(
        &mut self,
        command: BrowserWorkerCommand,
        directory: Option<&BrowserDirectorySnapshot>,
        session: Option<&BrowserSessionState>,
    ) -> Vec<BrowserWorkerEvent> {
        let previous_state = self.state.clone();
        let previous_transport = self.transport.clone();
        let previous_storage = self.storage.clone();
        let mut events = Vec::new();

        match command {
            BrowserWorkerCommand::Start(config) => {
                let capability = self.capability.clone().unwrap_or_default();
                let transport = self.transport.clone();
                let storage = self.storage.clone();
                *self = Self::start(config, capability, transport);
                self.storage = storage;
            }
            BrowserWorkerCommand::Stop => self.stop(),
            BrowserWorkerCommand::Suspend => self.suspend(),
            BrowserWorkerCommand::Resume => self.resume(),
            BrowserWorkerCommand::SelectExperiment {
                experiment_id,
                revision_id,
            } => self.select_experiment(experiment_id, revision_id, directory, session),
            BrowserWorkerCommand::ClearCaches => self.storage.clear_cached_data(),
            BrowserWorkerCommand::Verify(plan) => match self.execute_validation(plan) {
                Ok(result) => {
                    let head_id = result.head_id.clone();
                    events.push(BrowserWorkerEvent::HeadUpdated { head_id });
                    events.push(BrowserWorkerEvent::ValidationCompleted(result));
                }
                Err(message) => events.push(BrowserWorkerEvent::Error { message }),
            },
            BrowserWorkerCommand::Train(plan) => match self.execute_training(plan) {
                Ok(result) => events.push(BrowserWorkerEvent::TrainingCompleted(result)),
                Err(message) => events.push(BrowserWorkerEvent::Error { message }),
            },
            BrowserWorkerCommand::FlushReceiptOutbox => match self.flush_receipt_outbox() {
                Ok((session_id, submit_path, receipts)) if !receipts.is_empty() => {
                    events.push(BrowserWorkerEvent::ReceiptOutboxReady {
                        session_id,
                        submit_path,
                        receipts,
                    });
                }
                Ok(_) => {}
                Err(message) => events.push(BrowserWorkerEvent::Error { message }),
            },
            BrowserWorkerCommand::AcknowledgeSubmittedReceipts { receipt_ids } => {
                let pending_receipts = self.storage.acknowledge_receipts(&receipt_ids);
                events.push(BrowserWorkerEvent::ReceiptsAcknowledged {
                    receipt_ids,
                    pending_receipts,
                });
            }
            BrowserWorkerCommand::ApplyMetricsLiveEvent(event) => {
                return self.apply_metrics_live_event(*event);
            }
        }

        if self.state != previous_state
            && let Some(state) = self.state.clone()
        {
            events.push(BrowserWorkerEvent::RuntimeStateChanged(state));
        }
        if self.transport != previous_transport {
            events.push(BrowserWorkerEvent::TransportChanged(self.transport.clone()));
        }
        if self.storage != previous_storage {
            events.push(BrowserWorkerEvent::StorageUpdated(Box::new(
                self.storage.clone(),
            )));
        }
        if let Some(directory) = directory {
            events.push(BrowserWorkerEvent::DirectoryUpdated {
                network_id: directory.network_id.clone(),
                visible_entries: directory.entries.len(),
            });
        }

        events
    }

    /// Performs the refresh transport selection operation.
    pub fn refresh_transport_selection(&mut self) {
        let Some(config) = self.config.as_ref() else {
            self.transport.active = None;
            return;
        };
        let requires_peer_transport = self
            .state
            .as_ref()
            .is_some_and(BrowserRuntimeState::requires_peer_transport);
        self.transport.active = self
            .transport
            .recommended_transport(&config.transport, requires_peer_transport);
        self.synchronize_transport_state();
    }
}
