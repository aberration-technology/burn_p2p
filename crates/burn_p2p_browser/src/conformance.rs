use std::collections::BTreeSet;

use burn_p2p::{
    ContributionReceipt, ExperimentDirectoryEntry, ExperimentId, ExperimentScope,
    ExperimentVisibility, HeadId, NetworkId, PrincipalClaims, PrincipalId, PrincipalSession,
    RevisionId, StudyId, WorkloadId, WorkloadTrainingLease,
};
use burn_p2p_core::{BrowserDirectorySnapshot, ContentId, HeadDescriptor};
use chrono::Utc;

use crate::{
    BrowserCapabilityReport, BrowserGpuSupport, BrowserRuntimeConfig, BrowserRuntimeRole,
    BrowserSessionState, BrowserTrainingBudget, BrowserTrainingPlan, BrowserTrainingResult,
    BrowserTransportKind, BrowserTransportStatus, BrowserValidationPlan, BrowserValidationResult,
    BrowserWorkerCommand, BrowserWorkerEvent, BrowserWorkerRuntime,
};

/// Builds a conservative default transport snapshot for conformance exercises.
pub fn browser_conformance_transport() -> BrowserTransportStatus {
    BrowserTransportStatus::enabled(false, false, true).connected_via(
        BrowserTransportKind::WssFallback,
        vec![burn_p2p::PeerId::new("browser-conformance-peer")],
    )
}

/// Builds a capability report suitable for conformance exercises of one role.
pub fn browser_conformance_capability_for_role(
    role: BrowserRuntimeRole,
) -> BrowserCapabilityReport {
    if matches!(role, BrowserRuntimeRole::BrowserTrainerWgpu) {
        BrowserCapabilityReport {
            recommended_role: role,
            navigator_gpu_exposed: true,
            worker_gpu_exposed: true,
            gpu_support: BrowserGpuSupport::Available,
            persistent_storage_exposed: true,
            ..BrowserCapabilityReport::default()
        }
    } else {
        BrowserCapabilityReport {
            recommended_role: role,
            ..BrowserCapabilityReport::default()
        }
    }
}

/// Builds one browser directory snapshot suitable for conformance exercises.
pub fn browser_conformance_directory(
    network_id: NetworkId,
    entries: Vec<ExperimentDirectoryEntry>,
) -> BrowserDirectorySnapshot {
    BrowserDirectorySnapshot {
        network_id,
        generated_at: Utc::now(),
        entries,
    }
}

/// Builds one authenticated browser session suitable for conformance exercises.
pub fn browser_conformance_session(
    network_id: NetworkId,
    principal_id: PrincipalId,
    scopes: BTreeSet<ExperimentScope>,
) -> BrowserSessionState {
    BrowserSessionState {
        session: Some(PrincipalSession {
            session_id: ContentId::new("browser-conformance-session"),
            network_id,
            claims: PrincipalClaims {
                principal_id,
                provider: burn_p2p::AuthProvider::Static {
                    authority: "browser-conformance".into(),
                },
                display_name: "browser conformance".into(),
                org_memberships: BTreeSet::new(),
                group_memberships: BTreeSet::new(),
                granted_roles: burn_p2p::PeerRoleSet::default(),
                granted_scopes: scopes,
                custom_claims: Default::default(),
                issued_at: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::minutes(30),
            },
            issued_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(30),
        }),
        certificate: None,
        trust_bundle: None,
        enrolled_at: Some(Utc::now()),
        reenrollment_required: false,
    }
}

/// Builds one shared training plan carrying an exact execution lease for browser conformance
/// exercises.
pub fn browser_conformance_training_plan_with_lease(
    study_id: StudyId,
    experiment_id: ExperimentId,
    revision_id: RevisionId,
    workload_id: WorkloadId,
    lease: WorkloadTrainingLease,
) -> BrowserTrainingPlan {
    BrowserTrainingPlan {
        study_id,
        experiment_id,
        revision_id,
        workload_id,
        budget: BrowserTrainingBudget::default(),
        lease: Some(lease),
    }
}

/// Small downstream-facing wrapper around the browser worker runtime used to
/// smoke auth, directory sync, training, and validation flows.
pub struct BrowserConformanceHarness {
    /// The wrapped browser worker runtime.
    pub runtime: BrowserWorkerRuntime,
    /// The active directory snapshot used by the harness.
    pub directory: BrowserDirectorySnapshot,
    /// The active authenticated session used by the harness.
    pub session: BrowserSessionState,
}

impl BrowserConformanceHarness {
    /// Starts one browser worker harness and hydrates it with session and directory state.
    pub fn start(
        config: BrowserRuntimeConfig,
        capability: BrowserCapabilityReport,
        transport: BrowserTransportStatus,
        directory: BrowserDirectorySnapshot,
        session: BrowserSessionState,
    ) -> Self {
        let mut runtime = BrowserWorkerRuntime::start(config, capability, transport);
        runtime.remember_session(session.clone());
        runtime.apply_directory_snapshot(&directory, Some(&session));
        Self {
            runtime,
            directory,
            session,
        }
    }

    /// Replaces the current directory snapshot and reapplies it to the runtime.
    pub fn update_directory(
        &mut self,
        directory: BrowserDirectorySnapshot,
    ) -> Vec<BrowserWorkerEvent> {
        self.directory = directory;
        self.runtime
            .apply_directory_snapshot(&self.directory, Some(&self.session));
        Vec::new()
    }

    /// Applies one visible head snapshot.
    pub fn apply_heads(&mut self, heads: &[HeadDescriptor]) -> Option<HeadId> {
        self.runtime.apply_head_snapshot(heads)
    }

    /// Selects one experiment in the wrapped runtime.
    pub fn select_experiment(
        &mut self,
        experiment_id: burn_p2p::ExperimentId,
        revision_id: Option<burn_p2p::RevisionId>,
    ) -> Vec<BrowserWorkerEvent> {
        self.runtime.apply_command(
            BrowserWorkerCommand::SelectExperiment {
                experiment_id,
                revision_id,
            },
            Some(&self.directory),
            Some(&self.session),
        )
    }

    /// Executes one training plan and returns the emitted result.
    pub fn run_training(
        &mut self,
        plan: BrowserTrainingPlan,
    ) -> Result<BrowserTrainingResult, String> {
        let events = self.runtime.apply_command(
            BrowserWorkerCommand::Train(plan),
            Some(&self.directory),
            Some(&self.session),
        );
        extract_training_result(&events)
    }

    /// Executes one validation plan and returns the emitted result.
    pub fn run_validation(
        &mut self,
        plan: BrowserValidationPlan,
    ) -> Result<BrowserValidationResult, String> {
        let events = self.runtime.apply_command(
            BrowserWorkerCommand::Verify(plan),
            Some(&self.directory),
            Some(&self.session),
        );
        extract_validation_result(&events)
    }

    /// Returns the receipts currently queued in durable browser storage.
    pub fn pending_receipts(&self) -> Vec<ContributionReceipt> {
        self.runtime.storage.receipt_submission_batch(usize::MAX)
    }

    /// Returns the currently persisted active training lease, when present.
    pub fn active_training_lease(&self) -> Option<&WorkloadTrainingLease> {
        self.runtime.storage.active_training_lease.as_ref()
    }
}

fn extract_training_result(events: &[BrowserWorkerEvent]) -> Result<BrowserTrainingResult, String> {
    for event in events {
        match event {
            BrowserWorkerEvent::TrainingCompleted(result) => return Ok(result.clone()),
            BrowserWorkerEvent::Error { message } => return Err(message.clone()),
            _ => {}
        }
    }
    Err("training did not emit a completion event".into())
}

fn extract_validation_result(
    events: &[BrowserWorkerEvent],
) -> Result<BrowserValidationResult, String> {
    for event in events {
        match event {
            BrowserWorkerEvent::ValidationCompleted(result) => return Ok(result.clone()),
            BrowserWorkerEvent::Error { message } => return Err(message.clone()),
            _ => {}
        }
    }
    Err("validation did not emit a completion event".into())
}

/// Returns whether the entry is suitable for browser conformance flows.
pub fn browser_conformance_entry_is_visible(entry: &ExperimentDirectoryEntry) -> bool {
    !matches!(entry.visibility, ExperimentVisibility::InviteOnly)
}
