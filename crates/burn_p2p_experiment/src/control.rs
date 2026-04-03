use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    CanonicalSchema, ContentId, ControlCertificate, DatasetViewId, ExperimentId, HeadId,
    MergePolicy, NetworkId, PeerId, RevisionId, RevocationEpoch, SchemaEnvelope, StudyId,
    WindowActivation, WindowId,
};
use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::spec::{
    ExperimentSpec, PatchClass, PatchSupport, RevisionSpec, RuntimePatch, StudySpec,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationTarget {
    pub activation: WindowActivation,
    pub required_client_capabilities: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ExperimentControlCommand {
    CreateStudy {
        study: StudySpec,
    },
    CreateExperiment {
        experiment: Box<ExperimentSpec>,
        revision: Box<RevisionSpec>,
    },
    PatchExperiment {
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        patch: RuntimePatch,
        target: ActivationTarget,
    },
    PauseExperiment {
        study_id: StudyId,
        experiment_id: ExperimentId,
        reason: Option<String>,
        target: ActivationTarget,
    },
    ResumeExperiment {
        study_id: StudyId,
        experiment_id: ExperimentId,
        target: ActivationTarget,
    },
    PromoteHead {
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        head_id: HeadId,
        target: ActivationTarget,
    },
    RollbackHead {
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        head_id: HeadId,
        target: ActivationTarget,
    },
    ReassignCohort {
        study_id: StudyId,
        experiment_id: ExperimentId,
        cohort: String,
        peer_ids: Vec<PeerId>,
        target: ActivationTarget,
    },
    RevokePeer {
        peer_id: PeerId,
        minimum_revocation_epoch: RevocationEpoch,
        reason: String,
    },
    QuarantinePeer {
        peer_id: PeerId,
        reason: String,
        target: ActivationTarget,
    },
    PardonPeer {
        peer_id: PeerId,
        target: ActivationTarget,
    },
}

impl ExperimentControlCommand {
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
pub struct ExperimentControlEnvelope {
    pub network_id: NetworkId,
    pub command: ExperimentControlCommand,
}

impl ExperimentControlEnvelope {
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExperimentControlState {
    pub network_id: NetworkId,
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub current_revision_id: RevisionId,
    pub current_dataset_view_id: DatasetViewId,
    pub current_merge_policy: MergePolicy,
    pub paused: bool,
    pub scheduled_commands: BTreeMap<WindowId, Vec<ExperimentControlCommand>>,
    pub updated_at: DateTime<Utc>,
}

impl ExperimentControlState {
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

    pub fn supports_patch(&self, patch_support: PatchSupport, patch_class: PatchClass) -> bool {
        patch_support.supports(patch_class)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageError {
    MissingActivation,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExperimentSnapshot {
    pub study: StudySpec,
    pub experiment: ExperimentSpec,
    pub revision: RevisionSpec,
    pub control_state: ExperimentControlState,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PatchPlan {
    pub patch: RuntimePatch,
    pub target_revision_id: RevisionId,
    pub target_window: WindowActivation,
    pub notes: BTreeMap<String, String>,
}

impl PatchPlan {
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
