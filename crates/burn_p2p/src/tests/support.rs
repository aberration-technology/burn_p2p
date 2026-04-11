pub(super) use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

pub(super) use burn_p2p_checkpoint::{ArtifactBuildSpec, ChunkingScheme};
pub(super) use burn_p2p_core::MetricsLiveEventKind;
pub(super) use burn_p2p_dataloader::{
    DatasetRegistration, DatasetSizing, MicroShardPlannerConfig, ShardFetchManifest,
};
pub(super) use chrono::Utc;
pub(super) use semver::Version;
pub(super) use tempfile::tempdir;

pub(super) use crate::{
    ArtifactDescriptor, ArtifactKind, ArtifactTransferState, AssignmentLease, CachedMicroShard,
    CapabilityEstimate, ClientReenrollmentStatus, ContributionReceipt, EvalSplit,
    ExperimentDirectoryAnnouncement, ExperimentDirectoryEntry, ExperimentDirectoryPolicyExt,
    ExperimentHandle, ExperimentResourceRequirements, FsArtifactStore, GenesisSpec,
    HeadAnnouncement, HeadDescriptor, HeadEvalReport, MainnetHandle, MergeCertificate,
    MetricReport, MetricValue, MetricsRetentionConfig, NetworkEstimate, NodeBuilder,
    NodeCertificate, NodeCertificateClaims, P2pWorkload, PeerAuthAnnouncement, PeerAuthEnvelope,
    PeerWindowMetrics, ReducerCohortMetrics, StorageConfig, SwarmAddress, TelemetrySummary,
    UpstreamAdapter, WindowCtx, WindowId, WindowReport,
};

pub(super) fn native_swarm_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) fn mainnet() -> MainnetHandle {
    MainnetHandle {
        genesis: GenesisSpec {
            network_id: crate::NetworkId::new("net-1"),
            protocol_version: Version::new(0, 1, 0),
            display_name: "testnet".into(),
            created_at: Utc::now(),
            metadata: BTreeMap::new(),
        },
        roles: crate::PeerRoleSet::default_trainer(),
    }
}

pub(super) fn experiment() -> ExperimentHandle {
    mainnet().experiment(
        crate::StudyId::new("study-1"),
        crate::ExperimentId::new("exp-1"),
        crate::RevisionId::new("rev-1"),
    )
}

#[derive(Clone, Debug)]
pub(super) struct SyntheticRuntimeProject {
    pub(super) dataset_root: PathBuf,
    pub(super) learning_rate: f64,
    pub(super) target_model: f64,
}

impl crate::P2pWorkload for SyntheticRuntimeProject {
    type Device = String;
    type Model = f64;
    type Batch = f64;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, _device: &String) -> Self::Model {
        0.0
    }

    fn benchmark(&self, _model: &Self::Model, _device: &String) -> CapabilityEstimate {
        CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 64.0,
            target_window_seconds: 1,
        }
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<String, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, crate::TrainError> {
        let delta = ctx.batches.iter().copied().sum::<f64>() * self.learning_rate;
        ctx.model += delta;

        Ok(WindowReport {
            contribution: None,
            stats: BTreeMap::from([
                ("delta".into(), MetricValue::Float(delta)),
                ("model".into(), MetricValue::Float(ctx.model)),
                (
                    "loss".into(),
                    MetricValue::Float((self.target_model - ctx.model).abs()),
                ),
            ]),
            completed_at: Utc::now(),
        })
    }

    fn evaluate(&self, model: &Self::Model, _split: EvalSplit) -> MetricReport {
        MetricReport {
            metrics: BTreeMap::from([
                (
                    "loss".into(),
                    MetricValue::Float((self.target_model - *model).abs()),
                ),
                ("model".into(), MetricValue::Float(*model)),
            ]),
            captured_at: Utc::now(),
        }
    }

    fn apply_patch(&mut self, patch: &crate::RuntimePatch) -> crate::PatchOutcome {
        if let Some(crate::PatchValue::Float(value)) = patch.values.get("learning_rate") {
            self.learning_rate = *value;
            crate::PatchOutcome::Applied
        } else {
            crate::PatchOutcome::Rejected("missing learning_rate patch".into())
        }
    }

    fn supported_patch_classes(&self) -> crate::PatchSupport {
        crate::PatchSupport {
            hot: true,
            warm: false,
            cold: false,
        }
    }

    fn runtime_device(&self) -> String {
        "cpu".into()
    }

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        Ok(DatasetRegistration {
            manifest: crate::DatasetManifest {
                dataset_id: crate::DatasetId::new("dataset"),
                source_uri: self.dataset_root.display().to_string(),
                format: "microshards".into(),
                manifest_hash: crate::ContentId::new("manifest"),
                metadata: BTreeMap::new(),
            },
            view: crate::DatasetView {
                dataset_view_id: crate::DatasetViewId::new("dataset-view"),
                dataset_id: crate::DatasetId::new("dataset"),
                preprocessing_hash: crate::ContentId::new("preprocess"),
                tokenizer_hash: None,
                manifest_hash: crate::ContentId::new("manifest"),
                metadata: BTreeMap::new(),
            },
            upstream: UpstreamAdapter::Local {
                root: self.dataset_root.display().to_string(),
            },
        })
    }

    fn microshard_plan(
        &self,
        registration: &DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        Ok(crate::MicroShardPlanner::new(MicroShardPlannerConfig {
            target_microshard_bytes: 10,
            min_microshards: 2,
            max_microshards: 2,
        })?
        .plan(
            &registration.view,
            DatasetSizing {
                total_examples: 2,
                total_tokens: 2,
                total_bytes: 20,
            },
        )?)
    }

    fn load_batches(
        &self,
        _lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        cached_microshards
            .iter()
            .map(|shard| {
                let bytes = fs::read(&shard.path)?;
                let text = String::from_utf8(bytes)?;
                text.trim().parse::<f64>().map_err(anyhow::Error::from)
            })
            .collect()
    }

    fn load_model_artifact(
        &self,
        _model: Self::Model,
        descriptor: &ArtifactDescriptor,
        store: &FsArtifactStore,
        _device: &String,
    ) -> anyhow::Result<Self::Model> {
        Ok(serde_json::from_slice(
            &store.materialize_artifact_bytes(descriptor)?,
        )?)
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: ArtifactKind,
        head_id: crate::HeadId,
        base_head_id: Option<crate::HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let bytes = serde_json::to_vec(model)?;
        let mut spec = ArtifactBuildSpec::new(
            artifact_kind,
            crate::Precision::Fp32,
            crate::ContentId::new("synthetic-schema"),
            "synthetic-json",
        )
        .with_head(head_id);
        if let Some(base_head_id) = base_head_id {
            spec = spec.with_base_head(base_head_id);
        }
        Ok(store.store_artifact_reader(
            &spec,
            std::io::Cursor::new(bytes),
            ChunkingScheme::new(16)?,
        )?)
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }

    fn reconcile_canonical_model(
        &self,
        local_model: &Self::Model,
        canonical_model: Self::Model,
        strategy: crate::TrainerCanonicalReconcileStrategy,
    ) -> anyhow::Result<Self::Model> {
        Ok(match strategy {
            crate::TrainerCanonicalReconcileStrategy::Replace => canonical_model,
            crate::TrainerCanonicalReconcileStrategy::RootEma { canonical_weight } => {
                (local_model * (1.0 - canonical_weight)) + (canonical_model * canonical_weight)
            }
        })
    }

    fn merge_candidate_models(
        &self,
        base_model: &Self::Model,
        candidates: &[crate::MergeModelCandidate<'_, Self::Model>],
        policy: crate::MergePolicy,
    ) -> anyhow::Result<Option<Self::Model>> {
        if candidates.is_empty() {
            return Ok(None);
        }
        let (weighted_sum, total_weight) = candidates.iter().fold(
            (0.0_f64, 0.0_f64),
            |(weighted_sum, total_weight), candidate| {
                let quality = match policy {
                    crate::MergePolicy::WeightedMean => 1.0,
                    crate::MergePolicy::NormClippedWeightedMean => {
                        candidate.quality_weight.clamp(0.0, 1.0)
                    }
                    crate::MergePolicy::TrimmedMean => candidate.quality_weight,
                    crate::MergePolicy::Ema | crate::MergePolicy::QualityWeightedEma => {
                        candidate.quality_weight
                    }
                    crate::MergePolicy::Custom(_) => candidate.quality_weight,
                };
                let weight = candidate.sample_weight * quality;
                (
                    weighted_sum + (*candidate.model * weight),
                    total_weight + weight,
                )
            },
        );
        if total_weight <= f64::EPSILON {
            return Ok(Some(*base_model));
        }
        Ok(Some(weighted_sum / total_weight))
    }

    fn supported_workload(&self) -> crate::SupportedWorkload {
        crate::SupportedWorkload {
            workload_id: crate::WorkloadId::new("synthetic-runtime"),
            workload_name: "Synthetic Runtime".into(),
            model_program_hash: crate::ContentId::new("synthetic-program"),
            checkpoint_format_hash: crate::ContentId::new("synthetic-json"),
            supported_revision_family: crate::ContentId::new("synthetic-revision-family"),
            resource_class: "cpu".into(),
        }
    }

    fn model_schema_hash(&self) -> crate::ContentId {
        crate::ContentId::new("synthetic-schema")
    }
}

#[derive(Clone, Debug)]
pub(super) struct SwitchingTestWorkload {
    pub(super) manifest: crate::SupportedWorkload,
    pub(super) model_schema_hash: crate::ContentId,
}

impl crate::P2pWorkload for SwitchingTestWorkload {
    type Device = String;
    type Model = ();
    type Batch = ();
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, _device: &String) -> Self::Model {}

    fn benchmark(&self, _model: &Self::Model, _device: &String) -> CapabilityEstimate {
        CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 1.0,
            target_window_seconds: 1,
        }
    }

    fn train_window(
        &self,
        _ctx: &mut WindowCtx<String, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, crate::TrainError> {
        Ok(WindowReport {
            contribution: None,
            stats: BTreeMap::new(),
            completed_at: Utc::now(),
        })
    }

    fn evaluate(&self, _model: &Self::Model, _split: EvalSplit) -> MetricReport {
        MetricReport {
            metrics: BTreeMap::new(),
            captured_at: Utc::now(),
        }
    }

    fn apply_patch(&mut self, _patch: &crate::RuntimePatch) -> crate::PatchOutcome {
        crate::PatchOutcome::Applied
    }

    fn supported_patch_classes(&self) -> crate::PatchSupport {
        crate::PatchSupport {
            hot: true,
            warm: false,
            cold: false,
        }
    }

    fn runtime_device(&self) -> String {
        "cpu".into()
    }

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        anyhow::bail!("unused in switching test")
    }

    fn microshard_plan(
        &self,
        _registration: &DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        anyhow::bail!("unused in switching test")
    }

    fn load_batches(
        &self,
        _lease: &AssignmentLease,
        _cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        Ok(Vec::new())
    }

    fn load_model_artifact(
        &self,
        model: Self::Model,
        _descriptor: &ArtifactDescriptor,
        _store: &FsArtifactStore,
        _device: &String,
    ) -> anyhow::Result<Self::Model> {
        Ok(model)
    }

    fn materialize_model_artifact(
        &self,
        _model: &Self::Model,
        _artifact_kind: ArtifactKind,
        _head_id: crate::HeadId,
        _base_head_id: Option<crate::HeadId>,
        _store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor> {
        anyhow::bail!("unused in switching test")
    }

    fn contribution_metrics(
        &self,
        _report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        BTreeMap::new()
    }

    fn supported_workload(&self) -> crate::SupportedWorkload {
        self.manifest.clone()
    }

    fn model_schema_hash(&self) -> crate::ContentId {
        self.model_schema_hash.clone()
    }
}

#[derive(Clone, Debug)]
pub(super) struct SwitchingTestFamily {
    pub(super) release_manifest: crate::ClientReleaseManifest,
    pub(super) workloads: BTreeMap<crate::WorkloadId, SwitchingTestWorkload>,
}

impl crate::P2pProjectFamily for SwitchingTestFamily {
    type Workload = SwitchingTestWorkload;

    fn project_family_id(&self) -> &crate::ProjectFamilyId {
        &self.release_manifest.project_family_id
    }

    fn client_release_manifest(&self) -> &crate::ClientReleaseManifest {
        &self.release_manifest
    }

    fn workload(&self, workload_id: &crate::WorkloadId) -> anyhow::Result<Self::Workload> {
        self.workloads
            .get(workload_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing workload {}", workload_id.as_str()))
    }
}

pub(super) fn switching_test_workload(
    workload_id: &str,
    schema_hash: &str,
    checkpoint_format_hash: &str,
) -> SwitchingTestWorkload {
    SwitchingTestWorkload {
        manifest: crate::SupportedWorkload {
            workload_id: crate::WorkloadId::new(workload_id),
            workload_name: format!("Switch {workload_id}"),
            model_program_hash: crate::ContentId::new(format!("program-{workload_id}")),
            checkpoint_format_hash: crate::ContentId::new(checkpoint_format_hash),
            supported_revision_family: crate::ContentId::new(format!(
                "revision-family-{workload_id}"
            )),
            resource_class: "cpu".into(),
        },
        model_schema_hash: crate::ContentId::new(schema_hash),
    }
}

pub(super) fn switching_test_family() -> SwitchingTestFamily {
    let compiled = switching_test_workload("compiled", "schema-a", "format-a");
    let alternate = switching_test_workload("alternate", "schema-b", "format-b");
    let protocol_major =
        u16::try_from(mainnet().genesis.protocol_version.major).expect("protocol major");

    SwitchingTestFamily {
        release_manifest: crate::ClientReleaseManifest {
            project_family_id: crate::ProjectFamilyId::new("family-switch"),
            release_train_hash: crate::ContentId::new("train-switch"),
            target_artifact_id: "native-linux-x86_64".into(),
            target_artifact_hash: crate::ContentId::new("artifact-native-switch"),
            target_platform: crate::ClientPlatform::Native,
            app_semver: Version::new(0, 2, 0),
            git_commit: "switch-release".into(),
            cargo_lock_hash: crate::ContentId::new("switch-cargo-lock"),
            burn_version_string: "0.21.0-pre.3".into(),
            enabled_features_hash: crate::ContentId::new("switch-features"),
            protocol_major,
            supported_workloads: vec![compiled.manifest.clone(), alternate.manifest.clone()],
            built_at: Utc::now(),
        },
        workloads: BTreeMap::from([
            (compiled.manifest.workload_id.clone(), compiled),
            (alternate.manifest.workload_id.clone(), alternate),
        ]),
    }
}

pub(super) fn switching_network_manifest() -> crate::NetworkManifest {
    crate::NetworkManifest {
        network_id: mainnet().genesis.network_id.clone(),
        project_family_id: crate::ProjectFamilyId::new("family-switch"),
        protocol_major: u16::try_from(mainnet().genesis.protocol_version.major)
            .expect("protocol major"),
        required_release_train_hash: crate::ContentId::new("train-switch"),
        allowed_target_artifact_hashes: BTreeSet::from([crate::ContentId::new(
            "artifact-native-switch",
        )]),
        authority_public_keys: vec!["authority-switch".into()],
        bootstrap_addrs: Vec::new(),
        auth_policy_hash: crate::ContentId::new("auth-policy-switch"),
        created_at: Utc::now(),
        description: "switch-test network".into(),
    }
}

pub(super) fn switching_directory_entry(
    experiment_id: &str,
    revision_id: &str,
    workload_id: &str,
    schema_hash: &str,
    dataset_view_id: &str,
    display_name: &str,
) -> ExperimentDirectoryEntry {
    ExperimentDirectoryEntry {
        network_id: mainnet().genesis.network_id.clone(),
        study_id: crate::StudyId::new("study-switch"),
        experiment_id: crate::ExperimentId::new(experiment_id),
        workload_id: crate::WorkloadId::new(workload_id),
        display_name: display_name.into(),
        model_schema_hash: crate::ContentId::new(schema_hash),
        dataset_view_id: crate::DatasetViewId::new(dataset_view_id),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 0,
            estimated_window_seconds: 1,
        },
        visibility: crate::ExperimentVisibility::Public,
        opt_in_policy: crate::ExperimentOptInPolicy::Open,
        current_revision_id: crate::RevisionId::new(revision_id),
        current_head_id: None,
        allowed_roles: crate::PeerRoleSet::default_trainer(),
        allowed_scopes: BTreeSet::new(),
        metadata: BTreeMap::new(),
    }
}

pub(super) fn switching_directory_entries() -> Vec<ExperimentDirectoryEntry> {
    vec![
        switching_directory_entry(
            "exp-a", "rev-a", "compiled", "schema-a", "view-a", "Switch A",
        ),
        switching_directory_entry(
            "exp-b",
            "rev-b",
            "alternate",
            "schema-b",
            "view-b",
            "Switch B",
        ),
    ]
}

pub(super) fn control_announcement(
    command: burn_p2p_experiment::ExperimentControlCommand,
) -> crate::ControlAnnouncement {
    let envelope = burn_p2p_experiment::ExperimentControlEnvelope {
        network_id: mainnet().genesis.network_id.clone(),
        command,
    };
    let certificate = envelope
        .into_signed_cert(
            burn_p2p_core::SignatureMetadata {
                signer: crate::PeerId::new("authority-switch"),
                key_id: "authority-key".into(),
                algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                signed_at: Utc::now(),
                signature_hex: "feedface".into(),
            },
            mainnet().genesis.protocol_version.clone(),
        )
        .expect("control certificate");

    crate::ControlAnnouncement {
        overlay: mainnet().control_overlay(),
        certificate,
        announced_at: Utc::now(),
    }
}

pub(super) fn lifecycle_announcement(
    plan: burn_p2p_experiment::ExperimentLifecyclePlan,
) -> crate::ExperimentLifecycleAnnouncement {
    let envelope = burn_p2p_experiment::ExperimentLifecycleEnvelope {
        network_id: mainnet().genesis.network_id.clone(),
        plan,
    };
    let certificate = envelope
        .into_signed_cert(
            burn_p2p_core::SignatureMetadata {
                signer: crate::PeerId::new("authority-switch"),
                key_id: "authority-key".into(),
                algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                signed_at: Utc::now(),
                signature_hex: "feedface".into(),
            },
            mainnet().genesis.protocol_version.clone(),
        )
        .expect("lifecycle certificate");

    crate::ExperimentLifecycleAnnouncement {
        overlay: mainnet().control_overlay(),
        certificate,
        announced_at: Utc::now(),
    }
}

pub(super) fn auth_test_admission_policy(
    authority: &crate::NodeCertificateAuthority,
) -> crate::AdmissionPolicy {
    let issuer_peer_id = authority.issuer_peer_id();
    crate::AdmissionPolicy {
        network_id: mainnet().genesis.network_id.clone(),
        project_family_id: crate::ProjectFamilyId::new("family-auth"),
        required_release_train_hash: crate::ContentId::new("train-auth"),
        allowed_target_artifact_hashes: BTreeSet::from([crate::ContentId::new(
            "artifact-native-auth",
        )]),
        trusted_issuers: BTreeMap::from([(
            issuer_peer_id.clone(),
            crate::TrustedIssuer {
                issuer_peer_id,
                issuer_public_key_hex: authority.issuer_public_key_hex().into(),
            },
        )]),
        minimum_revocation_epoch: crate::RevocationEpoch(1),
    }
}

pub(super) fn auth_scoped_directory_entry(
    experiment_id: crate::ExperimentId,
) -> ExperimentDirectoryEntry {
    ExperimentDirectoryEntry {
        network_id: mainnet().genesis.network_id.clone(),
        study_id: crate::StudyId::new("study-auth"),
        experiment_id: experiment_id.clone(),
        workload_id: crate::WorkloadId::new("auth-demo"),
        display_name: "Auth Demo".into(),
        model_schema_hash: crate::ContentId::new("model-auth"),
        dataset_view_id: crate::DatasetViewId::new("view-auth"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::from([crate::PeerRole::TrainerCpu]),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(1024),
            estimated_download_bytes: 4096,
            estimated_window_seconds: 30,
        },
        visibility: crate::ExperimentVisibility::OptIn,
        opt_in_policy: crate::ExperimentOptInPolicy::Scoped,
        current_revision_id: crate::RevisionId::new("rev-auth"),
        current_head_id: Some(crate::HeadId::new("head-auth")),
        allowed_roles: crate::PeerRoleSet::default_trainer(),
        allowed_scopes: BTreeSet::from([crate::ExperimentScope::Train { experiment_id }]),
        metadata: BTreeMap::from([("owner".into(), "lab-auth".into())]),
    }
}

pub(super) fn runtime_directory_entry(experiment: &ExperimentHandle) -> ExperimentDirectoryEntry {
    ExperimentDirectoryEntry {
        network_id: experiment.network_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        workload_id: crate::WorkloadId::new("runtime-demo"),
        display_name: "Runtime Demo".into(),
        model_schema_hash: crate::ContentId::new("model-runtime"),
        dataset_view_id: crate::DatasetViewId::new("view-runtime"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::new(),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: None,
            estimated_download_bytes: 4096,
            estimated_window_seconds: 30,
        },
        visibility: crate::ExperimentVisibility::Public,
        opt_in_policy: crate::ExperimentOptInPolicy::Open,
        current_revision_id: experiment.revision_id.clone(),
        current_head_id: None,
        allowed_roles: crate::PeerRoleSet::default_trainer(),
        allowed_scopes: BTreeSet::from([crate::ExperimentScope::Train {
            experiment_id: experiment.experiment_id.clone(),
        }]),
        metadata: BTreeMap::new(),
    }
}

pub(super) fn issue_local_test_peer_auth(
    storage: &StorageConfig,
    authority: &crate::NodeCertificateAuthority,
    peer_id: &crate::PeerId,
    experiment_id: crate::ExperimentId,
) -> PeerAuthEnvelope {
    let node_keypair =
        crate::runtime_support::resolve_identity(&crate::IdentityConfig::Persistent, Some(storage))
            .expect("resolve persistent identity");
    let derived_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(&node_keypair.public()).to_string(),
    );
    assert_eq!(&derived_peer_id, peer_id);

    let now = Utc::now();
    let requested_scopes = BTreeSet::from([crate::ExperimentScope::Train {
        experiment_id: experiment_id.clone(),
    }]);
    let session = crate::PrincipalSession {
        session_id: crate::ContentId::new("session-auth"),
        network_id: mainnet().genesis.network_id.clone(),
        claims: crate::PrincipalClaims {
            principal_id: crate::PrincipalId::new("principal-auth"),
            provider: crate::AuthProvider::Static {
                authority: "lab-auth".into(),
            },
            display_name: "Auth Tester".into(),
            org_memberships: BTreeSet::new(),
            group_memberships: BTreeSet::new(),
            granted_roles: crate::PeerRoleSet::default_trainer(),
            granted_scopes: requested_scopes.clone(),
            custom_claims: BTreeMap::new(),
            issued_at: now,
            expires_at: now + chrono::Duration::hours(1),
        },
        issued_at: now,
        expires_at: now + chrono::Duration::hours(1),
    };
    let certificate = authority
        .issue_certificate(crate::NodeEnrollmentRequest {
            session,
            project_family_id: crate::ProjectFamilyId::new("family-auth"),
            release_train_hash: crate::ContentId::new("train-auth"),
            target_artifact_hash: crate::ContentId::new("artifact-native-auth"),
            peer_id: peer_id.clone(),
            peer_public_key_hex: node_keypair
                .public()
                .encode_protobuf()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            granted_roles: crate::PeerRoleSet::default_trainer(),
            requested_scopes: requested_scopes.clone(),
            client_policy_hash: Some(crate::ContentId::new("policy-auth")),
            serial: 7,
            not_before: now,
            not_after: now + chrono::Duration::hours(1),
            revocation_epoch: crate::RevocationEpoch(2),
        })
        .expect("issue certificate");
    authority
        .create_peer_auth_envelope(
            &node_keypair,
            certificate,
            Some(crate::ContentId::new("manifest-auth")),
            requested_scopes,
            crate::ContentId::new("nonce-auth"),
            now,
        )
        .expect("create peer auth envelope")
}

#[derive(Clone, Debug, serde::Serialize)]
pub(super) struct TestTrustIssuerStatus {
    pub(super) issuer_peer_id: crate::PeerId,
    pub(super) issuer_public_key_hex: String,
    pub(super) accepted_for_admission: bool,
    pub(super) active_for_new_certificates: bool,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(super) struct TestTrustBundleExport {
    pub(super) network_id: crate::NetworkId,
    pub(super) project_family_id: crate::ProjectFamilyId,
    pub(super) required_release_train_hash: crate::ContentId,
    pub(super) allowed_target_artifact_hashes: BTreeSet<crate::ContentId>,
    pub(super) minimum_revocation_epoch: crate::RevocationEpoch,
    pub(super) active_issuer_peer_id: crate::PeerId,
    pub(super) issuers: Vec<TestTrustIssuerStatus>,
    pub(super) reenrollment: Option<ClientReenrollmentStatus>,
}

pub(super) struct TestTrustBundleServer {
    pub(super) url: String,
    pub(super) wake_addr: String,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) bundle: Arc<Mutex<String>>,
    pub(super) thread: Option<thread::JoinHandle<()>>,
}

impl TestTrustBundleServer {
    pub(super) fn start(initial_bundle: &TestTrustBundleExport) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind trust bundle server");
        listener
            .set_nonblocking(true)
            .expect("set trust bundle server nonblocking");
        let wake_addr = listener
            .local_addr()
            .expect("trust bundle server addr")
            .to_string();
        let url = format!("http://{wake_addr}/trust");
        let stop = Arc::new(AtomicBool::new(false));
        let bundle = Arc::new(Mutex::new(
            serde_json::to_string(initial_bundle).expect("encode trust bundle"),
        ));
        let stop_for_thread = Arc::clone(&stop);
        let bundle_for_thread = Arc::clone(&bundle);
        let thread = thread::spawn(move || {
            while !stop_for_thread.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        handle_trust_bundle_request(stream, &bundle_for_thread);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            url,
            wake_addr,
            stop,
            bundle,
            thread: Some(thread),
        }
    }

    pub(super) fn set_bundle(&self, bundle: &TestTrustBundleExport) {
        *self
            .bundle
            .lock()
            .expect("trust bundle response should not be poisoned") =
            serde_json::to_string(bundle).expect("encode updated trust bundle");
    }

    pub(super) fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for TestTrustBundleServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(&self.wake_addr);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(super) fn handle_trust_bundle_request(stream: TcpStream, bundle: &Arc<Mutex<String>>) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone trust bundle stream"));
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() || line == "\r\n" || line.is_empty() {
            break;
        }
    }

    let mut stream = stream;
    let response = if request_line.starts_with("GET /trust ") {
        let body = bundle
            .lock()
            .expect("trust bundle response should not be poisoned")
            .clone();
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    } else {
        let body = "{\"error\":\"not found\"}";
        format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    };
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

pub(super) fn test_trust_bundle_export(
    authority: &crate::NodeCertificateAuthority,
    mut trusted_issuers: Vec<(crate::PeerId, String, bool)>,
    minimum_revocation_epoch: crate::RevocationEpoch,
    reenrollment: Option<ClientReenrollmentStatus>,
) -> TestTrustBundleExport {
    let active_issuer_peer_id = authority.issuer_peer_id();
    if !trusted_issuers
        .iter()
        .any(|(issuer_peer_id, _, _)| *issuer_peer_id == active_issuer_peer_id)
    {
        trusted_issuers.push((
            active_issuer_peer_id.clone(),
            authority.issuer_public_key_hex().into(),
            true,
        ));
    }
    let issuers = trusted_issuers
        .into_iter()
        .map(
            |(issuer_peer_id, issuer_public_key_hex, accepted_for_admission)| {
                TestTrustIssuerStatus {
                    active_for_new_certificates: issuer_peer_id == active_issuer_peer_id,
                    accepted_for_admission,
                    issuer_peer_id,
                    issuer_public_key_hex,
                }
            },
        )
        .collect();

    TestTrustBundleExport {
        network_id: mainnet().genesis.network_id.clone(),
        project_family_id: crate::ProjectFamilyId::new("family-auth"),
        required_release_train_hash: crate::ContentId::new("train-auth"),
        allowed_target_artifact_hashes: BTreeSet::from([crate::ContentId::new(
            "artifact-native-auth",
        )]),
        minimum_revocation_epoch,
        active_issuer_peer_id,
        issuers,
        reenrollment,
    }
}

pub(super) fn wait_for(timeout: Duration, condition: impl Fn() -> bool, failure_message: &str) {
    let deadline = Instant::now() + test_timeout(timeout);
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }

    panic!("{failure_message}");
}

pub(super) fn test_timeout(timeout: Duration) -> Duration {
    let scale = std::env::var("BURN_P2P_TEST_TIMEOUT_SCALE")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_else(|| {
            if std::env::var_os("CI").is_some() {
                3
            } else {
                1
            }
        })
        .max(1);
    timeout.checked_mul(scale).unwrap_or(timeout)
}

pub(super) fn metric_float(metrics: &BTreeMap<String, MetricValue>, key: &str) -> f64 {
    match metrics.get(key) {
        Some(MetricValue::Float(value)) => *value,
        other => panic!("expected float metric for {key}, found {other:?}"),
    }
}

pub(super) fn create_runtime_dataset(root: &std::path::Path) {
    let shard_count = 2;
    let registration = SyntheticRuntimeProject {
        dataset_root: root.to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    }
    .dataset_registration()
    .expect("registration");
    let plan = SyntheticRuntimeProject {
        dataset_root: root.to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    }
    .microshard_plan(&registration)
    .expect("plan");
    let manifest =
        ShardFetchManifest::from_microshards(&plan.dataset_view, &plan.microshards, |ordinal| {
            format!("{}\n", 1 + (ordinal % shard_count)).into_bytes()
        });

    fs::write(
        root.join("fetch-manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("manifest json"),
    )
    .expect("write manifest");
    for entry in &manifest.entries {
        let bytes = format!("{}\n", 1 + (entry.ordinal % shard_count)).into_bytes();
        fs::write(root.join(&entry.locator), bytes).expect("write shard");
    }
}

pub(super) fn persist_synthetic_runtime_head(
    project: &SyntheticRuntimeProject,
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    value: f64,
    head_id: &str,
    parent_head_id: Option<crate::HeadId>,
    global_step: u64,
) -> HeadDescriptor {
    let store = FsArtifactStore::new(storage.root.clone());
    store
        .ensure_layout()
        .expect("ensure synthetic artifact store");
    let head_id = crate::HeadId::new(head_id);
    let artifact = project
        .materialize_model_artifact(
            &value,
            ArtifactKind::FullHead,
            head_id.clone(),
            parent_head_id.clone(),
            &store,
        )
        .expect("materialize synthetic runtime head");
    let head = HeadDescriptor {
        head_id: head_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: artifact.artifact_id.clone(),
        parent_head_id,
        global_step,
        created_at: Utc::now(),
        metrics: BTreeMap::from([("model".into(), MetricValue::Float(value))]),
    };
    crate::runtime_support::persist_head_state(storage, experiment, &head)
        .expect("persist synthetic runtime head state");
    crate::runtime_support::persist_json(storage.scoped_head_path(&head.head_id), &head)
        .expect("persist synthetic runtime head descriptor");
    head
}

pub(super) fn load_metric_artifacts<T: serde::de::DeserializeOwned>(
    storage: &StorageConfig,
    prefix: &str,
) -> Vec<T> {
    let mut paths = fs::read_dir(storage.metrics_dir())
        .expect("read metrics dir")
        .filter_map(|entry| {
            let path = entry.expect("metrics entry").path();
            let file_name = path.file_name()?.to_str()?.to_owned();
            if file_name.starts_with(prefix)
                && path.extension().and_then(|extension| extension.to_str()) == Some("json")
            {
                Some(path)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            serde_json::from_slice(&fs::read(&path).expect("read metrics artifact"))
                .expect("decode metrics artifact")
        })
        .collect()
}

#[derive(serde::Deserialize)]
pub(super) struct StoredEvalProtocolManifestArtifact {
    pub(super) experiment_id: crate::ExperimentId,
    pub(super) revision_id: crate::RevisionId,
    pub(super) captured_at: chrono::DateTime<Utc>,
    pub(super) manifest: crate::EvalProtocolManifest,
}

#[derive(Clone, Debug)]
pub(super) struct AdaptiveBudgetProject {
    pub(super) dataset_root: PathBuf,
    pub(super) learning_rate: f64,
    pub(super) target_model: f64,
    pub(super) benchmark_work_units_per_second: f64,
    pub(super) benchmark_target_window_seconds: u64,
    pub(super) simulated_window_seconds: i64,
    pub(super) microshard_count: u32,
    pub(super) observed_lease_budgets: Arc<Mutex<Vec<u64>>>,
}

impl crate::P2pWorkload for AdaptiveBudgetProject {
    type Device = String;
    type Model = f64;
    type Batch = f64;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, _device: &String) -> Self::Model {
        0.0
    }

    fn benchmark(&self, _model: &Self::Model, _device: &String) -> CapabilityEstimate {
        CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: self.benchmark_work_units_per_second,
            target_window_seconds: self.benchmark_target_window_seconds.max(1),
        }
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<String, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, crate::TrainError> {
        self.observed_lease_budgets
            .lock()
            .expect("budget log lock")
            .push(ctx.lease.budget_work_units);
        let delta = ctx.batches.iter().copied().sum::<f64>() * self.learning_rate;
        ctx.model += delta;

        Ok(WindowReport {
            contribution: None,
            stats: BTreeMap::from([
                ("delta".into(), MetricValue::Float(delta)),
                ("model".into(), MetricValue::Float(ctx.model)),
                (
                    "loss".into(),
                    MetricValue::Float((self.target_model - ctx.model).abs()),
                ),
            ]),
            completed_at: Utc::now()
                + chrono::Duration::seconds(self.simulated_window_seconds.max(0)),
        })
    }

    fn evaluate(&self, model: &Self::Model, _split: EvalSplit) -> MetricReport {
        MetricReport {
            metrics: BTreeMap::from([
                (
                    "loss".into(),
                    MetricValue::Float((self.target_model - *model).abs()),
                ),
                ("model".into(), MetricValue::Float(*model)),
            ]),
            captured_at: Utc::now(),
        }
    }

    fn apply_patch(&mut self, _patch: &crate::RuntimePatch) -> crate::PatchOutcome {
        crate::PatchOutcome::Rejected("unsupported".into())
    }

    fn supported_patch_classes(&self) -> crate::PatchSupport {
        crate::PatchSupport {
            hot: false,
            warm: false,
            cold: false,
        }
    }

    fn runtime_device(&self) -> String {
        "cpu".into()
    }

    fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
        Ok(DatasetRegistration {
            manifest: crate::DatasetManifest {
                dataset_id: crate::DatasetId::new("dataset"),
                source_uri: self.dataset_root.display().to_string(),
                format: "microshards".into(),
                manifest_hash: crate::ContentId::new("manifest"),
                metadata: BTreeMap::new(),
            },
            view: crate::DatasetView {
                dataset_view_id: crate::DatasetViewId::new("dataset-view"),
                dataset_id: crate::DatasetId::new("dataset"),
                preprocessing_hash: crate::ContentId::new("preprocess"),
                tokenizer_hash: None,
                manifest_hash: crate::ContentId::new("manifest"),
                metadata: BTreeMap::new(),
            },
            upstream: UpstreamAdapter::Local {
                root: self.dataset_root.display().to_string(),
            },
        })
    }

    fn microshard_plan(
        &self,
        registration: &DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        Ok(crate::MicroShardPlanner::new(MicroShardPlannerConfig {
            target_microshard_bytes: 10,
            min_microshards: self.microshard_count,
            max_microshards: self.microshard_count,
        })?
        .plan(
            &registration.view,
            DatasetSizing {
                total_examples: u64::from(self.microshard_count),
                total_tokens: u64::from(self.microshard_count),
                total_bytes: u64::from(self.microshard_count) * 10,
            },
        )?)
    }

    fn load_batches(
        &self,
        _lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        cached_microshards
            .iter()
            .map(|shard| {
                let bytes = fs::read(&shard.path)?;
                let text = String::from_utf8(bytes)?;
                text.trim().parse::<f64>().map_err(anyhow::Error::from)
            })
            .collect()
    }

    fn load_model_artifact(
        &self,
        _model: Self::Model,
        descriptor: &ArtifactDescriptor,
        store: &FsArtifactStore,
        _device: &String,
    ) -> anyhow::Result<Self::Model> {
        Ok(serde_json::from_slice(
            &store.materialize_artifact_bytes(descriptor)?,
        )?)
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: ArtifactKind,
        head_id: crate::HeadId,
        base_head_id: Option<crate::HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor> {
        let bytes = serde_json::to_vec(model)?;
        let mut spec = ArtifactBuildSpec::new(
            artifact_kind,
            crate::Precision::Fp32,
            crate::ContentId::new("adaptive-schema"),
            "adaptive-json",
        )
        .with_head(head_id);
        if let Some(base_head_id) = base_head_id {
            spec = spec.with_base_head(base_head_id);
        }
        Ok(store.store_artifact_reader(
            &spec,
            std::io::Cursor::new(bytes),
            ChunkingScheme::new(16)?,
        )?)
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }

    fn supported_workload(&self) -> crate::SupportedWorkload {
        crate::SupportedWorkload {
            workload_id: crate::WorkloadId::new("adaptive-budget"),
            workload_name: "Adaptive Budget".into(),
            model_program_hash: crate::ContentId::new("adaptive-program"),
            checkpoint_format_hash: crate::ContentId::new("adaptive-json"),
            supported_revision_family: crate::ContentId::new("adaptive-revision-family"),
            resource_class: "cpu".into(),
        }
    }

    fn model_schema_hash(&self) -> crate::ContentId {
        crate::ContentId::new("adaptive-schema")
    }
}

pub(super) fn create_adaptive_runtime_dataset(project: &AdaptiveBudgetProject) {
    let registration = project
        .dataset_registration()
        .expect("adaptive registration");
    let plan = project
        .microshard_plan(&registration)
        .expect("adaptive plan");
    let shard_count = project.microshard_count.max(1);
    let manifest =
        ShardFetchManifest::from_microshards(&plan.dataset_view, &plan.microshards, |ordinal| {
            format!("{}\n", 1 + (ordinal % shard_count)).into_bytes()
        });

    fs::write(
        project.dataset_root.join("fetch-manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("adaptive manifest json"),
    )
    .expect("write adaptive manifest");
    for entry in &manifest.entries {
        let bytes = format!("{}\n", 1 + (entry.ordinal % shard_count)).into_bytes();
        fs::write(project.dataset_root.join(&entry.locator), bytes).expect("write adaptive shard");
    }
}
