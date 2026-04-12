use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    ContentId, ExperimentDirectoryEntry, ExperimentId, ExperimentOptInPolicy,
    ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, NetworkId, PeerRoleSet,
    RevisionId, RevisionManifest, StudyId, WindowActivation,
};

use crate::{
    ActivationTarget, ExperimentDirectoryPolicyExt, ExperimentLifecyclePhase,
    ExperimentLifecyclePlan,
};

/// Builder for one directory projection derived from one revision and customized
/// for a downstream product's visibility and eligibility policy.
pub struct ExperimentDirectoryProjectionBuilder {
    network_id: NetworkId,
    study_id: StudyId,
    experiment_id: ExperimentId,
    workload_id: burn_p2p_core::WorkloadId,
    display_name: String,
    model_schema_hash: ContentId,
    dataset_view_id: burn_p2p_core::DatasetViewId,
    resource_requirements: ExperimentResourceRequirements,
    visibility: ExperimentVisibility,
    opt_in_policy: ExperimentOptInPolicy,
    current_revision_id: RevisionId,
    current_head_id: Option<burn_p2p_core::HeadId>,
    allowed_roles: PeerRoleSet,
    allowed_scopes: BTreeSet<ExperimentScope>,
    metadata: BTreeMap<String, String>,
}

impl ExperimentDirectoryProjectionBuilder {
    /// Creates a new projection builder from one revision manifest.
    pub fn from_revision(
        network_id: NetworkId,
        study_id: StudyId,
        display_name: impl Into<String>,
        revision: &RevisionManifest,
    ) -> Self {
        let mut entry = ExperimentDirectoryEntry {
            network_id: network_id.clone(),
            study_id: study_id.clone(),
            experiment_id: revision.experiment_id.clone(),
            workload_id: revision.workload_id.clone(),
            display_name: display_name.into(),
            model_schema_hash: revision.model_schema_hash.clone(),
            dataset_view_id: revision.dataset_view_id.clone(),
            resource_requirements: revision.slot_requirements.clone(),
            visibility: ExperimentVisibility::OptIn,
            opt_in_policy: ExperimentOptInPolicy::Scoped,
            current_revision_id: revision.revision_id.clone(),
            current_head_id: None,
            allowed_roles: PeerRoleSet::default(),
            allowed_scopes: BTreeSet::new(),
            metadata: BTreeMap::new(),
        };
        entry.apply_revision_policy(revision);

        Self {
            network_id,
            study_id,
            experiment_id: revision.experiment_id.clone(),
            workload_id: revision.workload_id.clone(),
            display_name: entry.display_name,
            model_schema_hash: entry.model_schema_hash,
            dataset_view_id: entry.dataset_view_id,
            resource_requirements: entry.resource_requirements,
            visibility: entry.visibility,
            opt_in_policy: entry.opt_in_policy,
            current_revision_id: entry.current_revision_id,
            current_head_id: entry.current_head_id,
            allowed_roles: entry.allowed_roles,
            allowed_scopes: entry.allowed_scopes,
            metadata: entry.metadata,
        }
    }

    /// Overrides the human-readable display name.
    pub fn with_display_name(mut self, display_name: impl Into<String>) -> Self {
        self.display_name = display_name.into();
        self
    }

    /// Overrides the visibility policy.
    pub fn with_visibility(mut self, visibility: ExperimentVisibility) -> Self {
        self.visibility = visibility;
        self
    }

    /// Overrides the opt-in policy.
    pub fn with_opt_in_policy(mut self, opt_in_policy: ExperimentOptInPolicy) -> Self {
        self.opt_in_policy = opt_in_policy;
        self
    }

    /// Overrides the allowed runtime roles.
    pub fn with_allowed_roles(mut self, allowed_roles: PeerRoleSet) -> Self {
        self.allowed_roles = allowed_roles;
        self
    }

    /// Adds one allowed scope.
    pub fn with_scope(mut self, scope: ExperimentScope) -> Self {
        self.allowed_scopes.insert(scope);
        self
    }

    /// Adds the common train, validate, and admin scopes for this experiment.
    pub fn with_default_participant_scopes(mut self) -> Self {
        self.allowed_scopes.extend([
            ExperimentScope::Connect,
            ExperimentScope::Train {
                experiment_id: self.experiment_id.clone(),
            },
            ExperimentScope::Validate {
                experiment_id: self.experiment_id.clone(),
            },
            ExperimentScope::Admin {
                study_id: self.study_id.clone(),
            },
        ]);
        self
    }

    /// Overrides the visible current head.
    pub fn with_current_head_id(mut self, head_id: burn_p2p_core::HeadId) -> Self {
        self.current_head_id = Some(head_id);
        self
    }

    /// Adds one freeform metadata entry.
    pub fn with_metadata_entry(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Builds the final directory projection.
    pub fn build(self) -> ExperimentDirectoryEntry {
        ExperimentDirectoryEntry {
            network_id: self.network_id,
            study_id: self.study_id,
            experiment_id: self.experiment_id,
            workload_id: self.workload_id,
            display_name: self.display_name,
            model_schema_hash: self.model_schema_hash,
            dataset_view_id: self.dataset_view_id,
            resource_requirements: self.resource_requirements,
            visibility: self.visibility,
            opt_in_policy: self.opt_in_policy,
            current_revision_id: self.current_revision_id,
            current_head_id: self.current_head_id,
            allowed_roles: self.allowed_roles,
            allowed_scopes: self.allowed_scopes,
            metadata: self.metadata,
        }
    }
}

/// Builder for one authoritative lifecycle plan.
pub struct ExperimentLifecyclePlanBuilder {
    base_revision_id: Option<RevisionId>,
    target_entry: ExperimentDirectoryEntry,
    phase: ExperimentLifecyclePhase,
    activation: WindowActivation,
    required_client_capabilities: BTreeSet<String>,
    plan_epoch: u64,
    reason: Option<String>,
}

impl ExperimentLifecyclePlanBuilder {
    /// Creates a new lifecycle builder for one target entry.
    pub fn new(target_entry: ExperimentDirectoryEntry) -> Self {
        Self {
            base_revision_id: None,
            target_entry,
            phase: ExperimentLifecyclePhase::Staged,
            activation: WindowActivation {
                activation_window: burn_p2p_core::WindowId(0),
                grace_windows: 0,
            },
            required_client_capabilities: BTreeSet::new(),
            plan_epoch: 0,
            reason: None,
        }
    }

    /// Sets the expected base revision.
    pub fn with_base_revision_id(mut self, revision_id: RevisionId) -> Self {
        self.base_revision_id = Some(revision_id);
        self
    }

    /// Sets the lifecycle phase.
    pub fn with_phase(mut self, phase: ExperimentLifecyclePhase) -> Self {
        self.phase = phase;
        self
    }

    /// Sets the activation target.
    pub fn with_activation(mut self, activation: WindowActivation) -> Self {
        self.activation = activation;
        self
    }

    /// Adds one required client capability.
    pub fn with_required_client_capability(mut self, capability: impl Into<String>) -> Self {
        self.required_client_capabilities.insert(capability.into());
        self
    }

    /// Sets the operator plan epoch.
    pub fn with_plan_epoch(mut self, plan_epoch: u64) -> Self {
        self.plan_epoch = plan_epoch;
        self
    }

    /// Sets an operator reason.
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Builds one staged lifecycle plan.
    pub fn staged_at(self, activation: WindowActivation) -> ExperimentLifecyclePlan {
        self.with_phase(ExperimentLifecyclePhase::Staged)
            .with_activation(activation)
            .build()
    }

    /// Builds one prewarming lifecycle plan.
    pub fn prewarming_at(self, activation: WindowActivation) -> ExperimentLifecyclePlan {
        self.with_phase(ExperimentLifecyclePhase::Prewarming)
            .with_activation(activation)
            .build()
    }

    /// Builds one active lifecycle plan.
    pub fn active_at(self, activation: WindowActivation) -> ExperimentLifecyclePlan {
        self.with_phase(ExperimentLifecyclePhase::Active)
            .with_activation(activation)
            .build()
    }

    /// Builds the final lifecycle plan.
    pub fn build(self) -> ExperimentLifecyclePlan {
        ExperimentLifecyclePlan {
            study_id: self.target_entry.study_id.clone(),
            experiment_id: self.target_entry.experiment_id.clone(),
            base_revision_id: self.base_revision_id,
            target_entry: self.target_entry,
            phase: self.phase,
            target: ActivationTarget {
                activation: self.activation,
                required_client_capabilities: self.required_client_capabilities,
            },
            plan_epoch: self.plan_epoch,
            reason: self.reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn_p2p_core::{
        BrowserRolePolicy, BrowserVisibilityPolicy, ContentId, LagPolicy, MergeWindowMissPolicy,
        Precision, RevisionManifest, RobustnessPolicy, WorkloadId,
    };

    fn revision_manifest() -> RevisionManifest {
        RevisionManifest {
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            workload_id: WorkloadId::new("workload"),
            required_release_train_hash: ContentId::new("release"),
            model_schema_hash: ContentId::new("schema"),
            checkpoint_format_hash: ContentId::new("checkpoint"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("dataset-view"),
            training_config_hash: ContentId::new("training"),
            merge_topology_policy_hash: ContentId::new("merge-topology"),
            slot_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::new(),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: Some(1024),
                estimated_download_bytes: 2048,
                estimated_window_seconds: 30,
            },
            activation_window: WindowActivation {
                activation_window: burn_p2p_core::WindowId(2),
                grace_windows: 1,
            },
            lag_policy: LagPolicy::default(),
            merge_window_miss_policy: MergeWindowMissPolicy::default(),
            robustness_policy: Some(RobustnessPolicy::default()),
            browser_enabled: true,
            browser_role_policy: BrowserRolePolicy {
                observer: true,
                verifier: true,
                trainer_wgpu: true,
                fallback: false,
            },
            max_browser_checkpoint_bytes: Some(1024),
            max_browser_window_secs: Some(45),
            max_browser_shard_bytes: Some(2048),
            requires_webgpu: true,
            max_browser_batch_size: Some(8),
            recommended_browser_precision: Some(Precision::Fp16),
            visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
            description: "test revision".into(),
        }
    }

    #[test]
    fn directory_projection_builder_applies_revision_policy() {
        let entry = ExperimentDirectoryProjectionBuilder::from_revision(
            NetworkId::new("network"),
            StudyId::new("study"),
            "burn dragon",
            &revision_manifest(),
        )
        .with_default_participant_scopes()
        .build();

        assert_eq!(entry.display_name, "burn dragon");
        assert_eq!(entry.current_revision_id, RevisionId::new("revision"));
        assert!(entry.browser_enabled());
        assert_eq!(entry.max_browser_window_secs(), Some(45));
        assert!(entry.allowed_scopes.contains(&ExperimentScope::Train {
            experiment_id: ExperimentId::new("experiment"),
        }));
    }

    #[test]
    fn lifecycle_plan_builder_tracks_target_entry_and_activation() {
        let entry = ExperimentDirectoryProjectionBuilder::from_revision(
            NetworkId::new("network"),
            StudyId::new("study"),
            "burn dragon",
            &revision_manifest(),
        )
        .build();

        let plan = ExperimentLifecyclePlanBuilder::new(entry.clone())
            .with_base_revision_id(RevisionId::new("base-revision"))
            .with_required_client_capability("webgpu")
            .with_plan_epoch(7)
            .with_reason("roll to synthetic revision")
            .staged_at(WindowActivation {
                activation_window: burn_p2p_core::WindowId(8),
                grace_windows: 2,
            });

        assert_eq!(plan.target_entry, entry);
        assert_eq!(
            plan.base_revision_id,
            Some(RevisionId::new("base-revision"))
        );
        assert_eq!(plan.phase, ExperimentLifecyclePhase::Staged);
        assert_eq!(plan.plan_epoch, 7);
        assert!(plan.target.required_client_capabilities.contains("webgpu"));
        assert_eq!(
            plan.target.activation.activation_window,
            burn_p2p_core::WindowId(8)
        );
    }
}
