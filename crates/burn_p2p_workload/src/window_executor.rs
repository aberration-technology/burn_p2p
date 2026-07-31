use burn_p2p_core::{
    ArtifactId, HeadDescriptor, HeadId, RevisionContractBundle, RevisionId,
    ValidatedUpdateEvidence, WorkloadUpdateEnvelope,
};
use serde::{Deserialize, Serialize};

use crate::{
    WorkloadTrainingArtifact, WorkloadTrainingContribution, WorkloadTrainingPlan,
    WorkloadTrainingProgress,
};

/// Ordered lifecycle state shared by native and browser workload hosts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowExecutorLifecycle {
    /// No revision has been prepared.
    #[default]
    Unprepared,
    /// A complete revision contract has been accepted.
    RevisionPrepared,
    /// The exact canonical base artifact has been loaded.
    BaseLoaded,
    /// Local compute has completed.
    WindowExecuted,
    /// An update payload has been materialized.
    UpdateMaterialized,
    /// A canonical head has been applied after publication or reconciliation.
    CanonicalApplied,
}

/// Typed progress subscriber used by workload executors.
///
/// Implementations can mirror progress into Bevy messages, browser UI state,
/// logs, or tests without forcing the executor to own those runtimes.
pub trait WorkloadExecutionObserver {
    /// Receives one ordered workload progress event.
    fn observe(&mut self, progress: WorkloadTrainingProgress);
}

impl WorkloadExecutionObserver for () {
    fn observe(&mut self, _progress: WorkloadTrainingProgress) {}
}

/// Shared native/WASM lifecycle contract for one architecture-specific
/// workload executor.
///
/// The trait is intentionally synchronous and host-neutral. Native runtimes
/// may call it from a worker thread, while browser runtimes may call it from an
/// async task between transport awaits. Tensor/model types remain private to
/// the implementation.
pub trait WorkloadWindowExecutor {
    /// Executor-specific error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Accepts and prepares one complete revision contract.
    fn prepare_revision(&mut self, bundle: &RevisionContractBundle) -> Result<(), Self::Error>;

    /// Loads the exact canonical model base used by the next window.
    fn load_base(
        &mut self,
        head: &HeadDescriptor,
        artifact: &WorkloadTrainingArtifact,
    ) -> Result<(), Self::Error>;

    /// Executes the assigned local training window.
    fn execute_window(
        &mut self,
        plan: &WorkloadTrainingPlan,
        observer: &mut dyn WorkloadExecutionObserver,
    ) -> Result<WorkloadTrainingContribution, Self::Error>;

    /// Encodes a completed local contribution for publication.
    fn materialize_update(
        &mut self,
        contribution: &WorkloadTrainingContribution,
    ) -> Result<WorkloadUpdateEnvelope, Self::Error>;

    /// Decodes and verifies a peer update using local workload semantics.
    ///
    /// Security-sensitive statistics in the returned evidence must be computed
    /// from the decoded payload, never copied from peer claims.
    fn verify_update(
        &mut self,
        update: &WorkloadUpdateEnvelope,
    ) -> Result<ValidatedUpdateEvidence, Self::Error>;

    /// Reconciles local runtime state with a newly accepted canonical head.
    fn apply_canonical(
        &mut self,
        head: &HeadDescriptor,
        artifact: &WorkloadTrainingArtifact,
    ) -> Result<(), Self::Error>;
}

/// Enforces the shared executor lifecycle independently of the architecture.
#[derive(Debug)]
pub struct WindowExecutorSession<E> {
    executor: E,
    lifecycle: WindowExecutorLifecycle,
    revision_id: Option<RevisionId>,
    base_head_id: Option<HeadId>,
    update_artifact_id: Option<ArtifactId>,
}

impl<E> WindowExecutorSession<E>
where
    E: WorkloadWindowExecutor,
{
    /// Creates an unprepared executor session.
    pub fn new(executor: E) -> Self {
        Self {
            executor,
            lifecycle: WindowExecutorLifecycle::Unprepared,
            revision_id: None,
            base_head_id: None,
            update_artifact_id: None,
        }
    }

    /// Current lifecycle state.
    pub fn lifecycle(&self) -> WindowExecutorLifecycle {
        self.lifecycle
    }

    /// Accesses the architecture-specific executor.
    pub fn executor(&self) -> &E {
        &self.executor
    }

    /// Accesses the architecture-specific executor mutably.
    pub fn executor_mut(&mut self) -> &mut E {
        &mut self.executor
    }

    /// Consumes the session and returns its executor.
    pub fn into_inner(self) -> E {
        self.executor
    }

    /// Prepares a complete revision.
    pub fn prepare_revision(
        &mut self,
        bundle: &RevisionContractBundle,
    ) -> Result<(), WindowExecutorSessionError<E::Error>> {
        bundle
            .validate()
            .map_err(|error| WindowExecutorSessionError::Contract(error.to_string()))?;
        self.executor
            .prepare_revision(bundle)
            .map_err(WindowExecutorSessionError::Executor)?;
        self.revision_id = Some(bundle.revision.revision_id.clone());
        self.base_head_id = None;
        self.update_artifact_id = None;
        self.lifecycle = WindowExecutorLifecycle::RevisionPrepared;
        Ok(())
    }

    /// Loads the exact canonical base.
    pub fn load_base(
        &mut self,
        head: &HeadDescriptor,
        artifact: &WorkloadTrainingArtifact,
    ) -> Result<(), WindowExecutorSessionError<E::Error>> {
        self.require(WindowExecutorLifecycle::RevisionPrepared)?;
        if self.revision_id.as_ref() != Some(&head.revision_id)
            || artifact.descriptor.artifact_id != head.artifact_id
            || artifact.descriptor.head_id.as_ref() != Some(&head.head_id)
        {
            return Err(WindowExecutorSessionError::Identity(
                "canonical head, revision, and artifact identities do not match".into(),
            ));
        }
        self.executor
            .load_base(head, artifact)
            .map_err(WindowExecutorSessionError::Executor)?;
        self.base_head_id = Some(head.head_id.clone());
        self.update_artifact_id = None;
        self.lifecycle = WindowExecutorLifecycle::BaseLoaded;
        Ok(())
    }

    /// Executes one assigned window.
    pub fn execute_window(
        &mut self,
        plan: &WorkloadTrainingPlan,
        observer: &mut dyn WorkloadExecutionObserver,
    ) -> Result<WorkloadTrainingContribution, WindowExecutorSessionError<E::Error>> {
        self.require(WindowExecutorLifecycle::BaseLoaded)?;
        if self.revision_id.as_ref() != Some(&plan.revision_id) {
            return Err(WindowExecutorSessionError::Identity(
                "training plan revision does not match the prepared revision".into(),
            ));
        }
        let contribution = self
            .executor
            .execute_window(plan, observer)
            .map_err(WindowExecutorSessionError::Executor)?;
        if contribution.base_head_id.as_ref() != self.base_head_id.as_ref() {
            return Err(WindowExecutorSessionError::Identity(
                "training contribution does not name the loaded base head".into(),
            ));
        }
        self.lifecycle = WindowExecutorLifecycle::WindowExecuted;
        Ok(contribution)
    }

    /// Materializes a typed update payload.
    pub fn materialize_update(
        &mut self,
        contribution: &WorkloadTrainingContribution,
    ) -> Result<WorkloadUpdateEnvelope, WindowExecutorSessionError<E::Error>> {
        self.require(WindowExecutorLifecycle::WindowExecuted)?;
        if contribution.base_head_id.as_ref() != self.base_head_id.as_ref() {
            return Err(WindowExecutorSessionError::Identity(
                "materialized contribution base head changed after execution".into(),
            ));
        }
        let update = self
            .executor
            .materialize_update(contribution)
            .map_err(WindowExecutorSessionError::Executor)?;
        if Some(&update.base_head_id) != self.base_head_id.as_ref()
            || update.artifact.artifact_id != contribution.artifact_id
        {
            return Err(WindowExecutorSessionError::Identity(
                "update envelope does not match its contribution".into(),
            ));
        }
        self.update_artifact_id = Some(update.artifact.artifact_id.clone());
        self.lifecycle = WindowExecutorLifecycle::UpdateMaterialized;
        Ok(update)
    }

    /// Applies a newly accepted canonical head.
    pub fn apply_canonical(
        &mut self,
        head: &HeadDescriptor,
        artifact: &WorkloadTrainingArtifact,
    ) -> Result<(), WindowExecutorSessionError<E::Error>> {
        if !matches!(
            self.lifecycle,
            WindowExecutorLifecycle::BaseLoaded
                | WindowExecutorLifecycle::WindowExecuted
                | WindowExecutorLifecycle::UpdateMaterialized
                | WindowExecutorLifecycle::CanonicalApplied
        ) {
            return Err(WindowExecutorSessionError::InvalidTransition {
                expected: WindowExecutorLifecycle::BaseLoaded,
                actual: self.lifecycle,
            });
        }
        if self.revision_id.as_ref() != Some(&head.revision_id)
            || artifact.descriptor.artifact_id != head.artifact_id
        {
            return Err(WindowExecutorSessionError::Identity(
                "canonical reconciliation artifact does not match the prepared revision".into(),
            ));
        }
        self.executor
            .apply_canonical(head, artifact)
            .map_err(WindowExecutorSessionError::Executor)?;
        self.base_head_id = Some(head.head_id.clone());
        self.update_artifact_id = None;
        self.lifecycle = WindowExecutorLifecycle::CanonicalApplied;
        Ok(())
    }

    fn require(
        &self,
        expected: WindowExecutorLifecycle,
    ) -> Result<(), WindowExecutorSessionError<E::Error>> {
        if self.lifecycle != expected {
            return Err(WindowExecutorSessionError::InvalidTransition {
                expected,
                actual: self.lifecycle,
            });
        }
        Ok(())
    }
}

/// Lifecycle wrapper error.
#[derive(Debug, thiserror::Error)]
pub enum WindowExecutorSessionError<E>
where
    E: std::error::Error + 'static,
{
    /// Complete revision contract is invalid.
    #[error("invalid revision contract: {0}")]
    Contract(String),
    /// Lifecycle call occurred out of order.
    #[error("invalid executor transition: expected {expected:?}, actual {actual:?}")]
    InvalidTransition {
        /// Required state.
        expected: WindowExecutorLifecycle,
        /// Current state.
        actual: WindowExecutorLifecycle,
    },
    /// Cross-object identity mismatch.
    #[error("executor identity mismatch: {0}")]
    Identity(String),
    /// Workload executor failed.
    #[error("workload executor failed: {0}")]
    Executor(E),
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fmt};

    use burn_p2p_core::{
        ArtifactDescriptor, ArtifactKind, BrowserRolePolicy, BrowserVisibilityPolicy, ContentId,
        DatasetViewId, ExperimentId, ExperimentResourceRequirements, GenesisMaterialization,
        LagPolicy, LeaseId, LocalOptimizerStatePolicy, MODEL_GENESIS_SIGNATURE_KEY_ID,
        MergeWindowMissPolicy, ModelGenesisManifest, PeerId, Precision,
        REVISION_CONTRACT_SIGNATURE_KEY_ID, RecurrentStatePolicy, RevisionManifest,
        SchedulerStatePolicy, SchemaEnvelope, SignatureAlgorithm, SignatureMetadata, SignedPayload,
        StudyId, TRAINING_CONTRACT_VERSION, TrainingContractManifest, TrainingProtocol,
        UpdateCodec, WindowActivation, WindowId, WorkloadId,
    };
    use chrono::Utc;
    use semver::Version;

    use super::*;

    #[derive(Debug)]
    struct FakeError;

    impl fmt::Display for FakeError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("fake error")
        }
    }

    impl std::error::Error for FakeError {}

    #[derive(Default)]
    struct FakeExecutor {
        calls: Vec<&'static str>,
    }

    impl WorkloadWindowExecutor for FakeExecutor {
        type Error = FakeError;

        fn prepare_revision(
            &mut self,
            _bundle: &RevisionContractBundle,
        ) -> Result<(), Self::Error> {
            self.calls.push("prepare");
            Ok(())
        }

        fn load_base(
            &mut self,
            _head: &HeadDescriptor,
            _artifact: &WorkloadTrainingArtifact,
        ) -> Result<(), Self::Error> {
            self.calls.push("load");
            Ok(())
        }

        fn execute_window(
            &mut self,
            plan: &WorkloadTrainingPlan,
            observer: &mut dyn WorkloadExecutionObserver,
        ) -> Result<WorkloadTrainingContribution, Self::Error> {
            self.calls.push("execute");
            observer.observe(WorkloadTrainingProgress {
                stage: crate::WorkloadExecutionStage::Completed,
                completed_units: 1,
                total_units: Some(1),
                detail: None,
            });
            Ok(WorkloadTrainingContribution {
                artifact_id: ArtifactId::new("update"),
                completed_batches: 1,
                completed_examples: 1,
                completed_tokens: 1,
                training_time_ms: 1,
                eval_time_ms: 0,
                total_time_ms: 1,
                artifact_published: false,
                base_head_id: Some(HeadId::new("base")),
                published_artifact: None,
                workload_update: None,
                metadata: BTreeMap::from([(
                    "lease".into(),
                    plan.lease
                        .as_ref()
                        .map(|lease| lease.lease_id.as_str())
                        .unwrap_or_default()
                        .into(),
                )]),
            })
        }

        fn materialize_update(
            &mut self,
            _contribution: &WorkloadTrainingContribution,
        ) -> Result<WorkloadUpdateEnvelope, Self::Error> {
            self.calls.push("materialize");
            let mut descriptor = artifact_descriptor("update", "base");
            descriptor.kind = ArtifactKind::DeltaPack;
            descriptor.base_head_id = Some(HeadId::new("base"));
            Ok(WorkloadUpdateEnvelope {
                training_contract_id: contract().contract_id().expect("contract"),
                revision_id: RevisionId::new("revision"),
                base_head_id: HeadId::new("base"),
                window_id: WindowId(1),
                lease_id: LeaseId::new("lease"),
                codec: UpdateCodec::DenseDelta,
                artifact: descriptor,
                decoded_tensor_digest: Some(ContentId::new("decoded")),
                claimed_norm_stats: None,
                claimed_feature_sketch: None,
            })
        }

        fn verify_update(
            &mut self,
            _update: &WorkloadUpdateEnvelope,
        ) -> Result<ValidatedUpdateEvidence, Self::Error> {
            unreachable!("not used by lifecycle test")
        }

        fn apply_canonical(
            &mut self,
            _head: &HeadDescriptor,
            _artifact: &WorkloadTrainingArtifact,
        ) -> Result<(), Self::Error> {
            self.calls.push("canonical");
            Ok(())
        }
    }

    #[derive(Default)]
    struct CountingObserver(usize);

    impl WorkloadExecutionObserver for CountingObserver {
        fn observe(&mut self, _progress: WorkloadTrainingProgress) {
            self.0 += 1;
        }
    }

    fn contract() -> TrainingContractManifest {
        TrainingContractManifest {
            version: TRAINING_CONTRACT_VERSION,
            workload_id: WorkloadId::new("workload"),
            model_program_hash: ContentId::new("program"),
            model_schema_hash: ContentId::new("schema"),
            checkpoint_format_hash: ContentId::new("format"),
            dataset_view_id: DatasetViewId::new("dataset"),
            tokenizer_hash: ContentId::new("tokenizer"),
            preprocessing_hash: ContentId::new("preprocess"),
            objective_hash: ContentId::new("objective"),
            optimizer_hash: ContentId::new("optimizer"),
            scheduler_hash: ContentId::new("scheduler"),
            optimizer_state_policy: LocalOptimizerStatePolicy::ResetPerWindow,
            scheduler_state_policy: SchedulerStatePolicy::CanonicalGlobalStep,
            recurrent_state_policy: RecurrentStatePolicy::LeaseScoped,
            update_codec: UpdateCodec::DenseDelta,
            aggregation_hash: ContentId::new("aggregation"),
            validation_hash: ContentId::new("validation"),
            initialization_hash: ContentId::new("initialization"),
            extensions: BTreeMap::new(),
        }
    }

    fn artifact_descriptor(artifact_id: &str, head_id: &str) -> ArtifactDescriptor {
        ArtifactDescriptor {
            artifact_id: ArtifactId::new(artifact_id),
            kind: ArtifactKind::FullHead,
            head_id: Some(HeadId::new(head_id)),
            base_head_id: None,
            precision: Precision::Fp32,
            model_schema_hash: ContentId::new("schema"),
            record_format: "test".into(),
            bytes_len: 1,
            chunks: Vec::new(),
            root_hash: ContentId::new(format!("{artifact_id}-root")),
        }
    }

    fn bundle() -> RevisionContractBundle {
        let training = contract();
        let contract_id = training.contract_id().expect("contract");
        let revision = RevisionManifest {
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            workload_id: WorkloadId::new("workload"),
            required_release_train_hash: ContentId::new("release"),
            model_schema_hash: ContentId::new("schema"),
            checkpoint_format_hash: ContentId::new("format"),
            dataset_view_id: DatasetViewId::new("dataset"),
            training_config_hash: contract_id.clone(),
            merge_topology_policy_hash: ContentId::new("merge"),
            training_protocol: TrainingProtocol::default(),
            slot_requirements: ExperimentResourceRequirements {
                minimum_roles: Default::default(),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: None,
                estimated_download_bytes: 0,
                estimated_window_seconds: 1,
            },
            activation_window: WindowActivation {
                activation_window: WindowId(0),
                grace_windows: 0,
            },
            lag_policy: LagPolicy::default(),
            merge_window_miss_policy: MergeWindowMissPolicy::default(),
            robustness_policy: None,
            browser_enabled: true,
            browser_role_policy: BrowserRolePolicy::default(),
            max_browser_checkpoint_bytes: None,
            max_browser_window_secs: None,
            max_browser_shard_bytes: None,
            requires_webgpu: false,
            max_browser_batch_size: None,
            recommended_browser_precision: None,
            visibility_policy: BrowserVisibilityPolicy::SwarmEligible,
            description: "test".into(),
        };
        let genesis = ModelGenesisManifest {
            experiment_id: revision.experiment_id.clone(),
            revision_id: revision.revision_id.clone(),
            workload_id: revision.workload_id.clone(),
            training_contract_id: contract_id.clone(),
            artifact: artifact_descriptor("genesis", "genesis"),
            tensor_digest: ContentId::new("tensor"),
            initialization_algorithm: "test".into(),
            initialization_seed: Some(1),
            materialization: GenesisMaterialization::FullArtifact,
            authority_epoch: 1,
            created_at: Utc::now(),
        };
        let genesis = SignedPayload::new(
            SchemaEnvelope::new("burn-p2p-model-genesis-v1", Version::new(0, 21, 0), genesis),
            SignatureMetadata {
                signer: PeerId::new("authority"),
                key_id: MODEL_GENESIS_SIGNATURE_KEY_ID.into(),
                algorithm: SignatureAlgorithm::Ed25519,
                signed_at: Utc::now(),
                signature_hex: "00".into(),
            },
        )
        .expect("signed");
        RevisionContractBundle {
            revision,
            training_contract_id: contract_id,
            training,
            genesis,
            contract_signature: SignatureMetadata {
                signer: PeerId::new("authority"),
                key_id: REVISION_CONTRACT_SIGNATURE_KEY_ID.into(),
                algorithm: SignatureAlgorithm::Ed25519,
                signed_at: Utc::now(),
                signature_hex: "00".into(),
            },
        }
    }

    fn base_head() -> HeadDescriptor {
        HeadDescriptor {
            head_id: HeadId::new("base"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            artifact_id: ArtifactId::new("base-artifact"),
            parent_head_id: None,
            global_step: 0,
            created_at: Utc::now(),
            metrics: BTreeMap::new(),
        }
    }

    fn base_artifact() -> WorkloadTrainingArtifact {
        WorkloadTrainingArtifact {
            descriptor: artifact_descriptor("base-artifact", "base"),
            chunks: Vec::new(),
        }
    }

    fn plan() -> WorkloadTrainingPlan {
        WorkloadTrainingPlan {
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            workload_id: WorkloadId::new("workload"),
            budget: Default::default(),
            lease: None,
            contribution: None,
        }
    }

    #[test]
    fn executor_session_enforces_shared_native_browser_ordering() {
        let mut session = WindowExecutorSession::new(FakeExecutor::default());
        assert!(matches!(
            session.execute_window(&plan(), &mut ()),
            Err(WindowExecutorSessionError::InvalidTransition { .. })
        ));

        session.prepare_revision(&bundle()).expect("prepare");
        session
            .load_base(&base_head(), &base_artifact())
            .expect("load base");
        let mut observer = CountingObserver::default();
        let contribution = session
            .execute_window(&plan(), &mut observer)
            .expect("execute");
        let update = session
            .materialize_update(&contribution)
            .expect("materialize");

        assert_eq!(observer.0, 1);
        assert_eq!(update.base_head_id, HeadId::new("base"));
        assert_eq!(
            session.executor().calls,
            ["prepare", "load", "execute", "materialize"]
        );
        assert_eq!(
            session.lifecycle(),
            WindowExecutorLifecycle::UpdateMaterialized
        );
    }
}
