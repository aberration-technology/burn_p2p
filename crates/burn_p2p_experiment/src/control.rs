use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    CanonicalSchema, ContentId, ControlCertificate, DatasetViewId, ExperimentDirectoryEntry,
    ExperimentId, HeadId, MergePolicy, NetworkId, PeerId, RevisionId, RevocationEpoch,
    SchemaEnvelope, StudyId, WindowActivation, WindowId,
};
use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::spec::{
    ExperimentSpec, PatchClass, PatchSupport, RevisionSpec, RuntimePatch, StudySpec,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an activation target.
pub struct ActivationTarget {
    /// The activation.
    pub activation: WindowActivation,
    /// The required client capabilities.
    pub required_client_capabilities: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Enumerates the supported experiment control command values.
pub enum ExperimentControlCommand {
    /// Uses the create study variant.
    CreateStudy {
        /// The study.
        study: StudySpec,
    },
    /// Uses the create experiment variant.
    CreateExperiment {
        /// The experiment.
        experiment: Box<ExperimentSpec>,
        /// The revision.
        revision: Box<RevisionSpec>,
    },
    /// Uses the patch experiment variant.
    PatchExperiment {
        /// The study ID.
        study_id: StudyId,
        /// The experiment ID.
        experiment_id: ExperimentId,
        /// The revision ID.
        revision_id: RevisionId,
        /// The patch.
        patch: RuntimePatch,
        /// The target.
        target: ActivationTarget,
    },
    /// Uses the pause experiment variant.
    PauseExperiment {
        /// The study ID.
        study_id: StudyId,
        /// The experiment ID.
        experiment_id: ExperimentId,
        /// The reason.
        reason: Option<String>,
        /// The target.
        target: ActivationTarget,
    },
    /// Uses the resume experiment variant.
    ResumeExperiment {
        /// The study ID.
        study_id: StudyId,
        /// The experiment ID.
        experiment_id: ExperimentId,
        /// The target.
        target: ActivationTarget,
    },
    /// Uses the promote head variant.
    PromoteHead {
        /// The study ID.
        study_id: StudyId,
        /// The experiment ID.
        experiment_id: ExperimentId,
        /// The revision ID.
        revision_id: RevisionId,
        /// The head ID.
        head_id: HeadId,
        /// The target.
        target: ActivationTarget,
    },
    /// Uses the rollback head variant.
    RollbackHead {
        /// The study ID.
        study_id: StudyId,
        /// The experiment ID.
        experiment_id: ExperimentId,
        /// The revision ID.
        revision_id: RevisionId,
        /// The head ID.
        head_id: HeadId,
        /// The target.
        target: ActivationTarget,
    },
    /// Uses the reassign cohort variant.
    ReassignCohort {
        /// The study ID.
        study_id: StudyId,
        /// The experiment ID.
        experiment_id: ExperimentId,
        /// The cohort.
        cohort: String,
        /// The peer IDs.
        peer_ids: Vec<PeerId>,
        /// The target.
        target: ActivationTarget,
    },
    /// Uses the revoke peer variant.
    RevokePeer {
        /// The peer ID.
        peer_id: PeerId,
        /// The minimum revocation epoch.
        minimum_revocation_epoch: RevocationEpoch,
        /// The reason.
        reason: String,
    },
    /// Uses the quarantine peer variant.
    QuarantinePeer {
        /// The peer ID.
        peer_id: PeerId,
        /// The reason.
        reason: String,
        /// The target.
        target: ActivationTarget,
    },
    /// Uses the pardon peer variant.
    PardonPeer {
        /// The peer ID.
        peer_id: PeerId,
        /// The target.
        target: ActivationTarget,
    },
}

impl ExperimentControlCommand {
    /// Performs the activation operation.
    pub fn activation(&self) -> Option<&ActivationTarget> {
        match self {
            Self::CreateStudy { .. } | Self::CreateExperiment { .. } | Self::RevokePeer { .. } => {
                None
            }
            Self::PatchExperiment { target, .. }
            | Self::PauseExperiment { target, .. }
            | Self::ResumeExperiment { target, .. }
            | Self::PromoteHead { target, .. }
            | Self::RollbackHead { target, .. }
            | Self::ReassignCohort { target, .. }
            | Self::QuarantinePeer { target, .. }
            | Self::PardonPeer { target, .. } => Some(target),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Wraps the experiment control with transport metadata.
pub struct ExperimentControlEnvelope {
    /// The network ID.
    pub network_id: NetworkId,
    /// The command.
    pub command: ExperimentControlCommand,
}

impl ExperimentControlEnvelope {
    /// Consumes the value and returns the signed cert.
    pub fn into_signed_cert(
        self,
        signer: burn_p2p_core::SignatureMetadata,
        protocol_version: Version,
    ) -> Result<ControlCertificate<ExperimentControlEnvelope>, burn_p2p_core::SchemaError> {
        let activation = self
            .command
            .activation()
            .map(|target| target.activation.clone())
            .unwrap_or(WindowActivation {
                activation_window: WindowId(0),
                grace_windows: 0,
            });

        let required_client_capabilities = self
            .command
            .activation()
            .map(|target| target.required_client_capabilities.clone())
            .unwrap_or_default();

        let body = burn_p2p_core::SignedPayload::new(
            SchemaEnvelope::new("burn_p2p.control", protocol_version, self),
            signer,
        )?;

        Ok(ControlCertificate {
            control_cert_id: body.payload_id.clone().into(),
            network_id: body.payload.payload.network_id.clone(),
            activation,
            required_client_capabilities,
            body,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the lifecycle phases for an experiment revision rollout.
pub enum ExperimentLifecyclePhase {
    /// The lifecycle plan exists but is not yet scheduled for execution.
    Draft,
    /// The target revision has been announced to the network.
    Announced,
    /// The rollout has been staged for future activation.
    Staged,
    /// The network is prewarming artifacts ahead of activation.
    Prewarming,
    /// The rollout is ready but not yet active.
    Ready,
    /// The rollout is actively switching revisions at the activation window.
    Activating,
    /// The target revision is now the active revision.
    Active,
    /// The previous revision is draining while the target remains authoritative.
    Draining,
    /// The target revision is authoritative but paused.
    Paused,
    /// The lifecycle plan rolled traffic back to the target revision.
    RolledBack,
    /// The lifecycle plan has been retired from active scheduling.
    Archived,
}

impl ExperimentLifecyclePhase {
    /// Returns whether the phase is authoritative for revision selection.
    pub fn authorizes_switch(&self) -> bool {
        matches!(
            self,
            Self::Activating
                | Self::Active
                | Self::Draining
                | Self::Paused
                | Self::RolledBack
                | Self::Archived
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents an authoritative lifecycle plan for one experiment.
pub struct ExperimentLifecyclePlan {
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The base revision expected before the plan takes effect.
    pub base_revision_id: Option<RevisionId>,
    /// The target directory projection for the rollout.
    pub target_entry: ExperimentDirectoryEntry,
    /// The current lifecycle phase.
    pub phase: ExperimentLifecyclePhase,
    /// The activation target.
    pub target: ActivationTarget,
    #[serde(default)]
    /// Monotonic operator epoch for competing lifecycle plans.
    pub plan_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional operator reason for the change-over.
    pub reason: Option<String>,
}

impl ExperimentLifecyclePlan {
    /// Returns whether the plan is effective for the supplied window.
    pub fn is_effective_for_window(&self, window: WindowId) -> bool {
        self.phase.authorizes_switch() && self.target.activation.becomes_active_at(window)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Wraps a lifecycle plan with transport metadata.
pub struct ExperimentLifecycleEnvelope {
    /// The network ID.
    pub network_id: NetworkId,
    /// The lifecycle plan.
    pub plan: ExperimentLifecyclePlan,
}

impl ExperimentLifecycleEnvelope {
    /// Consumes the value and returns the signed cert.
    pub fn into_signed_cert(
        self,
        signer: burn_p2p_core::SignatureMetadata,
        protocol_version: Version,
    ) -> Result<ControlCertificate<ExperimentLifecycleEnvelope>, burn_p2p_core::SchemaError> {
        let activation = self.plan.target.activation.clone();
        let required_client_capabilities = self.plan.target.required_client_capabilities.clone();

        let body = burn_p2p_core::SignedPayload::new(
            SchemaEnvelope::new("burn_p2p.lifecycle", protocol_version, self),
            signer,
        )?;

        Ok(ControlCertificate {
            control_cert_id: body.payload_id.clone().into(),
            network_id: body.payload.payload.network_id.clone(),
            activation,
            required_client_capabilities,
            body,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Captures experiment control state.
pub struct ExperimentControlState {
    /// The network ID.
    pub network_id: NetworkId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The current revision ID.
    pub current_revision_id: RevisionId,
    /// The current dataset view ID.
    pub current_dataset_view_id: DatasetViewId,
    /// The current merge policy.
    pub current_merge_policy: MergePolicy,
    /// The paused.
    pub paused: bool,
    /// The scheduled commands.
    pub scheduled_commands: BTreeMap<WindowId, Vec<ExperimentControlCommand>>,
    /// The updated at.
    pub updated_at: DateTime<Utc>,
}

impl ExperimentControlState {
    /// Performs the stage command operation.
    pub fn stage_command(
        &mut self,
        command: ExperimentControlCommand,
        now: DateTime<Utc>,
    ) -> Result<(), StageError> {
        let target = command.activation().ok_or(StageError::MissingActivation)?;
        self.scheduled_commands
            .entry(target.activation.activation_window)
            .or_default()
            .push(command);
        self.updated_at = now;
        Ok(())
    }

    /// Returns whether the value supports patch.
    pub fn supports_patch(&self, patch_support: PatchSupport, patch_class: PatchClass) -> bool {
        patch_support.supports(patch_class)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported stage error values.
pub enum StageError {
    /// Uses the missing activation variant.
    MissingActivation,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Captures a snapshot of experiment.
pub struct ExperimentSnapshot {
    /// The study.
    pub study: StudySpec,
    /// The experiment.
    pub experiment: ExperimentSpec,
    /// The revision.
    pub revision: RevisionSpec,
    /// The control state.
    pub control_state: ExperimentControlState,
    /// The metadata.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a patch plan.
pub struct PatchPlan {
    /// The patch.
    pub patch: RuntimePatch,
    /// The target revision ID.
    pub target_revision_id: RevisionId,
    /// The target window.
    pub target_window: WindowActivation,
    /// The notes.
    pub notes: BTreeMap<String, String>,
}

impl PatchPlan {
    /// Performs the content hash operation.
    pub fn content_hash(&self) -> Result<ContentId, burn_p2p_core::SchemaError> {
        self.content_id()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_core::{DatasetViewId, MergePolicy, WindowActivation, WindowId};
    use chrono::Utc;

    use crate::{
        control::{ActivationTarget, ExperimentControlCommand, ExperimentControlState},
        spec::{
            PatchClass, PatchSupport, PatchValue, RevisionCompatibility, RevisionSpec, RuntimePatch,
        },
    };

    #[test]
    fn patch_support_checks_patch_classes() {
        let support = PatchSupport {
            hot: true,
            warm: false,
            cold: false,
        };

        assert!(support.supports(PatchClass::Hot));
        assert!(!support.supports(PatchClass::Warm));
    }

    #[test]
    fn stage_command_buckets_by_activation_window() {
        let now = Utc::now();
        let study_id = burn_p2p_core::StudyId::new("study");
        let experiment_id = burn_p2p_core::ExperimentId::new("experiment");
        let revision_id = burn_p2p_core::RevisionId::new("revision");

        let mut state = ExperimentControlState {
            network_id: burn_p2p_core::NetworkId::new("network"),
            study_id: study_id.clone(),
            experiment_id: experiment_id.clone(),
            current_revision_id: revision_id,
            current_dataset_view_id: DatasetViewId::new("view"),
            current_merge_policy: MergePolicy::WeightedMean,
            paused: false,
            scheduled_commands: BTreeMap::new(),
            updated_at: now,
        };

        let patch = RuntimePatch::new(
            PatchClass::Hot,
            "lr tweak",
            BTreeMap::from([("learning_rate".into(), PatchValue::Float(0.0002))]),
            BTreeSet::new(),
        )
        .expect("patch");

        let command = ExperimentControlCommand::PatchExperiment {
            study_id,
            experiment_id,
            revision_id: burn_p2p_core::RevisionId::new("revision-2"),
            patch,
            target: ActivationTarget {
                activation: WindowActivation {
                    activation_window: WindowId(8),
                    grace_windows: 1,
                },
                required_client_capabilities: BTreeSet::new(),
            },
        };

        state.stage_command(command, now).expect("stage");

        assert_eq!(state.scheduled_commands.len(), 1);
        assert!(state.scheduled_commands.contains_key(&WindowId(8)));
    }

    #[test]
    fn revision_support_handles_hot_warm_cold() {
        let now = Utc::now();
        let revision = RevisionSpec::new(
            burn_p2p_core::ExperimentId::new("exp"),
            None,
            burn_p2p_core::ContentId::new("project"),
            burn_p2p_core::ContentId::new("config"),
            RevisionCompatibility {
                model_schema_hash: burn_p2p_core::ContentId::new("model"),
                dataset_view_id: DatasetViewId::new("view"),
                required_client_capabilities: BTreeSet::new(),
            },
            PatchSupport {
                hot: true,
                warm: true,
                cold: false,
            },
            now,
        )
        .expect("revision");

        assert!(revision.patch_support.supports(PatchClass::Warm));
        assert!(!revision.patch_support.supports(PatchClass::Cold));
    }
}
