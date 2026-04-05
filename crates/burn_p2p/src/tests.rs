//! Runtime integration and regression tests for the burn_p2p facade.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use burn_p2p_checkpoint::{ArtifactBuildSpec, ChunkingScheme};
use burn_p2p_core::MetricsLiveEventKind;
use burn_p2p_dataloader::{
    DatasetRegistration, DatasetSizing, MicroShardPlannerConfig, ShardFetchManifest,
};
use chrono::Utc;
use semver::Version;
use tempfile::tempdir;

use super::{
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

fn mainnet() -> MainnetHandle {
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

fn experiment() -> ExperimentHandle {
    mainnet().experiment(
        crate::StudyId::new("study-1"),
        crate::ExperimentId::new("exp-1"),
        crate::RevisionId::new("rev-1"),
    )
}

#[derive(Clone, Debug)]
struct SyntheticRuntimeProject {
    dataset_root: PathBuf,
    learning_rate: f64,
    target_model: f64,
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
struct SwitchingTestWorkload {
    manifest: crate::SupportedWorkload,
    model_schema_hash: crate::ContentId,
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
struct SwitchingTestFamily {
    release_manifest: crate::ClientReleaseManifest,
    workloads: BTreeMap<crate::WorkloadId, SwitchingTestWorkload>,
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

fn switching_test_workload(
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

fn switching_test_family() -> SwitchingTestFamily {
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
            burn_version_string: "0.21.0-pre.2".into(),
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

fn switching_network_manifest() -> crate::NetworkManifest {
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

fn switching_directory_entry(
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

fn switching_directory_entries() -> Vec<ExperimentDirectoryEntry> {
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

fn control_announcement(
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

fn auth_test_admission_policy(
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

fn auth_scoped_directory_entry(experiment_id: crate::ExperimentId) -> ExperimentDirectoryEntry {
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

fn issue_local_test_peer_auth(
    storage: &StorageConfig,
    authority: &crate::NodeCertificateAuthority,
    peer_id: &crate::PeerId,
    experiment_id: crate::ExperimentId,
) -> PeerAuthEnvelope {
    let node_keypair =
        super::runtime_support::resolve_identity(&crate::IdentityConfig::Persistent, Some(storage))
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
struct TestTrustIssuerStatus {
    issuer_peer_id: crate::PeerId,
    issuer_public_key_hex: String,
    accepted_for_admission: bool,
    active_for_new_certificates: bool,
}

#[derive(Clone, Debug, serde::Serialize)]
struct TestTrustBundleExport {
    network_id: crate::NetworkId,
    project_family_id: crate::ProjectFamilyId,
    required_release_train_hash: crate::ContentId,
    allowed_target_artifact_hashes: BTreeSet<crate::ContentId>,
    minimum_revocation_epoch: crate::RevocationEpoch,
    active_issuer_peer_id: crate::PeerId,
    issuers: Vec<TestTrustIssuerStatus>,
    reenrollment: Option<ClientReenrollmentStatus>,
}

struct TestTrustBundleServer {
    url: String,
    wake_addr: String,
    stop: Arc<AtomicBool>,
    bundle: Arc<Mutex<String>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl TestTrustBundleServer {
    fn start(initial_bundle: &TestTrustBundleExport) -> Self {
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

    fn set_bundle(&self, bundle: &TestTrustBundleExport) {
        *self
            .bundle
            .lock()
            .expect("trust bundle response should not be poisoned") =
            serde_json::to_string(bundle).expect("encode updated trust bundle");
    }

    fn url(&self) -> &str {
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

fn handle_trust_bundle_request(stream: TcpStream, bundle: &Arc<Mutex<String>>) {
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

fn test_trust_bundle_export(
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

fn wait_for(timeout: Duration, condition: impl Fn() -> bool, failure_message: &str) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }

    panic!("{failure_message}");
}

fn metric_float(metrics: &BTreeMap<String, MetricValue>, key: &str) -> f64 {
    match metrics.get(key) {
        Some(MetricValue::Float(value)) => *value,
        other => panic!("expected float metric for {key}, found {other:?}"),
    }
}

fn create_runtime_dataset(root: &std::path::Path) {
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

fn load_metric_artifacts<T: serde::de::DeserializeOwned>(
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
struct StoredEvalProtocolManifestArtifact {
    experiment_id: crate::ExperimentId,
    revision_id: crate::RevisionId,
    captured_at: chrono::DateTime<Utc>,
    manifest: crate::EvalProtocolManifest,
}

#[derive(Clone, Debug)]
struct AdaptiveBudgetProject {
    dataset_root: PathBuf,
    learning_rate: f64,
    target_model: f64,
    benchmark_work_units_per_second: f64,
    benchmark_target_window_seconds: u64,
    simulated_window_seconds: i64,
    microshard_count: u32,
    observed_lease_budgets: Arc<Mutex<Vec<u64>>>,
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

fn create_adaptive_runtime_dataset(project: &AdaptiveBudgetProject) {
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

#[test]
fn mainnet_and_experiment_handles_build_overlay_topics() {
    let mainnet = mainnet();
    let experiment = experiment();

    assert_eq!(mainnet.control_topic(), "/burn-p2p/net-1/control");

    let topics = experiment.overlay_topics();
    assert_eq!(
        topics.heads,
        "/burn-p2p/net-1/study/study-1/exp/exp-1/heads"
    );
    assert_eq!(
        topics.telemetry,
        "/burn-p2p/net-1/study/study-1/exp/exp-1/telemetry"
    );
}

#[test]
fn experiment_handle_matches_scoped_objects() {
    let experiment = experiment();

    let receipt = ContributionReceipt {
        receipt_id: crate::ContributionReceiptId::new("receipt-1"),
        peer_id: crate::PeerId::new("peer-1"),
        study_id: crate::StudyId::new("study-1"),
        experiment_id: crate::ExperimentId::new("exp-1"),
        revision_id: crate::RevisionId::new("rev-1"),
        base_head_id: crate::HeadId::new("head-0"),
        artifact_id: crate::ArtifactId::new("artifact-1"),
        accepted_at: Utc::now(),
        accepted_weight: 1.0,
        metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.5))]),
        merge_cert_id: None,
    };

    let merge = MergeCertificate {
        merge_cert_id: crate::MergeCertId::new("merge-1"),
        study_id: crate::StudyId::new("study-1"),
        experiment_id: crate::ExperimentId::new("exp-1"),
        revision_id: crate::RevisionId::new("rev-1"),
        base_head_id: crate::HeadId::new("head-0"),
        merged_head_id: crate::HeadId::new("head-1"),
        merged_artifact_id: crate::ArtifactId::new("artifact-merged"),
        policy: crate::MergePolicy::WeightedMean,
        issued_at: Utc::now(),
        validator: crate::PeerId::new("validator-1"),
        contribution_receipts: vec![crate::ContributionReceiptId::new("receipt-1")],
    };

    let telemetry = TelemetrySummary {
        network_id: crate::NetworkId::new("net-1"),
        study_id: crate::StudyId::new("study-1"),
        experiment_id: crate::ExperimentId::new("exp-1"),
        revision_id: crate::RevisionId::new("rev-1"),
        window_id: WindowId(4),
        active_peers: 8,
        accepted_contributions: 12,
        throughput_work_units_per_second: 42.0,
        network: NetworkEstimate {
            connected_peers: 6,
            observed_peers: 9,
            estimated_network_size: 10.5,
            estimated_total_vram_bytes: None,
            estimated_total_flops: None,
            eta_lower_seconds: Some(20),
            eta_upper_seconds: Some(45),
        },
        metrics: BTreeMap::from([("loss".into(), MetricValue::Float(0.4))]),
        captured_at: Utc::now(),
    };

    assert!(experiment.matches_receipt(&receipt));
    assert!(experiment.matches_merge_certificate(&merge));
    assert!(experiment.matches_telemetry(&telemetry));
}

#[test]
fn prepare_keeps_the_old_handle_only_behavior() {
    let node = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .prepare()
        .expect("prepare");

    assert_eq!(node.mainnet().network_id().as_str(), "net-1");
    assert!(node.config().bootstrap_peers.is_empty());
}

#[test]
fn spawn_starts_a_live_runtime_and_shutdown_returns_node() {
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-facade-runtime-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let running = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(storage_root.clone()))
        .spawn()
        .expect("spawn");

    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(2),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.local_peer_id.is_some()
                && snapshot.status == crate::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "runtime did not reach running state",
    );

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.network_id, Some(crate::NetworkId::new("net-1")));
    assert!(snapshot.local_peer_id.is_some());
    assert!(storage_root.join("state").exists());
    assert!(storage_root.join("artifacts/chunks").exists());

    running.shutdown().expect("shutdown");
    let node = running.await_termination().expect("await termination");
    assert_eq!(node.mainnet().network_id().as_str(), "net-1");
}

#[test]
fn persistent_identity_survives_restart() {
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-facade-identity-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));

    let first = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(StorageConfig::new(storage_root.clone()))
        .spawn()
        .expect("first spawn");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || first_telemetry.snapshot().local_peer_id.is_some(),
        "first runtime did not publish a local peer id",
    );
    let first_peer_id = first_telemetry
        .snapshot()
        .local_peer_id
        .expect("first peer id");
    first.shutdown().expect("first shutdown");
    let _ = first.await_termination().expect("first termination");

    let second = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(StorageConfig::new(storage_root))
        .spawn()
        .expect("second spawn");
    let second_telemetry = second.telemetry();
    wait_for(
        Duration::from_secs(5),
        || second_telemetry.snapshot().local_peer_id.is_some(),
        "second runtime did not publish a local peer id",
    );
    let second_peer_id = second_telemetry
        .snapshot()
        .local_peer_id
        .expect("second peer id");
    second.shutdown().expect("second shutdown");
    let _ = second.await_termination().expect("second termination");

    assert_eq!(first_peer_id, second_peer_id);
}

#[test]
fn discovered_bootstrap_peer_survives_restart() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let experiment = experiment();

    let validator_storage = std::env::temp_dir().join(format!(
        "burn-p2p-discovery-validator-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let trainer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-discovery-trainer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));

    let validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(validator_storage.clone()))
    .spawn()
    .expect("validator spawn");
    let validator_telemetry = validator.telemetry();
    wait_for(
        Duration::from_secs(5),
        || !validator_telemetry.snapshot().listen_addresses.is_empty(),
        "validator did not start listening",
    );
    let validator_addr = validator_telemetry.snapshot().listen_addresses[0].clone();

    let mut validator = validator;
    let genesis_head = validator
        .initialize_local_head(&experiment)
        .expect("init genesis");

    let first_trainer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
    .with_storage(StorageConfig::new(trainer_storage.clone()))
    .with_bootstrap_peer(validator_addr.clone())
    .spawn()
    .expect("first trainer spawn");
    let first_trainer_telemetry = first_trainer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || first_trainer_telemetry.snapshot().connected_peers >= 1,
        "first trainer did not connect",
    );
    wait_for(
        Duration::from_secs(5),
        || {
            first_trainer_telemetry
                .snapshot()
                .known_peer_addresses
                .contains(&validator_addr)
        },
        "first trainer did not persist discovered bootstrap address",
    );

    first_trainer.shutdown().expect("first trainer shutdown");
    let _ = first_trainer
        .await_termination()
        .expect("first trainer termination");

    let restarted_trainer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
    .with_storage(StorageConfig::new(trainer_storage))
    .spawn()
    .expect("restarted trainer spawn");
    let restarted_telemetry = restarted_trainer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || restarted_telemetry.snapshot().connected_peers >= 1,
        "restarted trainer did not reconnect from persisted peer list",
    );
    assert!(
        restarted_telemetry
            .snapshot()
            .known_peer_addresses
            .contains(&validator_addr)
    );
    let synced = restarted_trainer
        .sync_experiment_head(&experiment)
        .expect("sync canonical head after restart")
        .expect("synced head after restart");
    assert_eq!(synced.head_id, genesis_head.head_id);

    restarted_trainer
        .shutdown()
        .expect("restarted trainer shutdown");
    let _ = restarted_trainer
        .await_termination()
        .expect("restarted trainer termination");
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");
}

#[test]
fn runtime_rebudgets_next_window_after_slow_observed_throughput() {
    let dataset_dir = tempdir().expect("dataset dir");
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-rebudget-slow-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let observed_budgets = Arc::new(Mutex::new(Vec::new()));
    let project = AdaptiveBudgetProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
        benchmark_work_units_per_second: 64.0,
        benchmark_target_window_seconds: 1,
        simulated_window_seconds: 4,
        microshard_count: 128,
        observed_lease_budgets: Arc::clone(&observed_budgets),
    };
    create_adaptive_runtime_dataset(&project);

    let running = NodeBuilder::new(project)
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(storage_root.clone()))
        .spawn()
        .expect("adaptive spawn");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "adaptive runtime did not start",
    );

    let mut running = running;
    let first = running
        .train_window_once(&experiment())
        .expect("first window");
    let second = running
        .train_window_once(&experiment())
        .expect("second window");

    assert_eq!(first.lease.budget_work_units, 64);
    assert!(
        second.lease.budget_work_units < first.lease.budget_work_units,
        "expected second budget {} to be smaller than first {}",
        second.lease.budget_work_units,
        first.lease.budget_work_units
    );
    let snapshot = running.telemetry().snapshot();
    assert!(
        snapshot
            .effective_limit_profile
            .as_ref()
            .map(|profile| profile.recommended_budget.budget_work_units
                < first.lease.budget_work_units)
            .unwrap_or(false)
    );
    let persisted = super::runtime_support::load_limit_profile(
        &StorageConfig::new(storage_root),
        &experiment(),
    )
    .expect("load limit profile")
    .expect("persisted profile");
    assert!(persisted.recommended_budget.budget_work_units < first.lease.budget_work_units);
    assert_eq!(
        observed_budgets.lock().expect("budget log").as_slice(),
        &[
            first.lease.budget_work_units,
            second.lease.budget_work_units
        ]
    );

    running.shutdown().expect("adaptive shutdown");
    let _ = running.await_termination().expect("adaptive termination");
}

#[test]
fn runtime_rebudgets_next_window_after_fast_observed_throughput() {
    let dataset_dir = tempdir().expect("dataset dir");
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-rebudget-fast-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let project = AdaptiveBudgetProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
        benchmark_work_units_per_second: 4.0,
        benchmark_target_window_seconds: 10,
        simulated_window_seconds: 5,
        microshard_count: 128,
        observed_lease_budgets: Arc::new(Mutex::new(Vec::new())),
    };
    create_adaptive_runtime_dataset(&project);

    let running = NodeBuilder::new(project)
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(storage_root))
        .spawn()
        .expect("adaptive spawn");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "adaptive runtime did not start",
    );

    let mut running = running;
    let first = running
        .train_window_once(&experiment())
        .expect("first window");
    let second = running
        .train_window_once(&experiment())
        .expect("second window");

    assert_eq!(first.lease.budget_work_units, 40);
    assert!(
        second.lease.budget_work_units > first.lease.budget_work_units,
        "expected second budget {} to be larger than first {}",
        second.lease.budget_work_units,
        first.lease.budget_work_units
    );

    running.shutdown().expect("adaptive shutdown");
    let _ = running.await_termination().expect("adaptive termination");
}

#[test]
fn rebudgeted_limit_profile_survives_restart() {
    let dataset_dir = tempdir().expect("dataset dir");
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-rebudget-restart-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let first_project = AdaptiveBudgetProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
        benchmark_work_units_per_second: 64.0,
        benchmark_target_window_seconds: 1,
        simulated_window_seconds: 4,
        microshard_count: 128,
        observed_lease_budgets: Arc::new(Mutex::new(Vec::new())),
    };
    create_adaptive_runtime_dataset(&first_project);

    let first = NodeBuilder::new(first_project)
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(StorageConfig::new(storage_root.clone()))
        .spawn()
        .expect("first adaptive spawn");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || first_telemetry.snapshot().local_peer_id.is_some(),
        "first adaptive runtime did not start",
    );
    let mut first = first;
    let first_window = first
        .train_window_once(&experiment())
        .expect("first training window");
    first.shutdown().expect("first adaptive shutdown");
    let _ = first
        .await_termination()
        .expect("first adaptive termination");

    let restarted_project = AdaptiveBudgetProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
        benchmark_work_units_per_second: 64.0,
        benchmark_target_window_seconds: 1,
        simulated_window_seconds: 4,
        microshard_count: 128,
        observed_lease_budgets: Arc::new(Mutex::new(Vec::new())),
    };
    let restarted = NodeBuilder::new(restarted_project)
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(StorageConfig::new(storage_root.clone()))
        .spawn()
        .expect("restarted adaptive spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || restarted_telemetry.snapshot().local_peer_id.is_some(),
        "restarted adaptive runtime did not start",
    );
    let mut restarted = restarted;
    let restarted_window = restarted
        .train_window_once(&experiment())
        .expect("restarted training window");
    assert!(
        restarted_window.lease.budget_work_units < first_window.lease.budget_work_units,
        "expected restarted budget {} to reuse rebudgeted profile from first budget {}",
        restarted_window.lease.budget_work_units,
        first_window.lease.budget_work_units
    );
    let persisted = super::runtime_support::load_limit_profile(
        &StorageConfig::new(storage_root),
        &experiment(),
    )
    .expect("load restarted profile")
    .expect("persisted restarted profile");
    assert_eq!(
        persisted.recommended_budget.budget_work_units,
        restarted
            .telemetry()
            .snapshot()
            .effective_limit_profile
            .expect("telemetry limit profile")
            .recommended_budget
            .budget_work_units
    );

    restarted.shutdown().expect("restarted adaptive shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted adaptive termination");
}

#[test]
fn active_lease_survives_restart() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-lease-restart-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let experiment = experiment();

    let first = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
    .with_storage(StorageConfig::new(storage_root.clone()))
    .spawn()
    .expect("first runtime spawn");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || first_telemetry.snapshot().local_peer_id.is_some(),
        "first runtime did not start",
    );
    let mut first = first;
    let window = first
        .train_window_once(&experiment)
        .expect("training window");
    let persisted = super::runtime_support::load_scoped_lease_announcement(
        &StorageConfig::new(storage_root.clone()),
        &experiment,
    )
    .expect("load persisted lease")
    .expect("persisted lease announcement");
    assert_eq!(persisted.lease, window.lease);
    assert!(storage_root.join("leases").exists());

    first.shutdown().expect("first runtime shutdown");
    let _ = first
        .await_termination()
        .expect("first runtime termination");

    let restarted = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
    .with_storage(StorageConfig::new(storage_root.clone()))
    .spawn()
    .expect("restarted runtime spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            restarted_telemetry
                .snapshot()
                .control_plane
                .lease_announcements
                .iter()
                .any(|announcement| announcement.lease.lease_id == window.lease.lease_id)
        },
        "restarted runtime did not restore persisted lease",
    );
    let restored = super::runtime_support::load_scoped_lease_announcement(
        &StorageConfig::new(storage_root),
        &experiment,
    )
    .expect("load restored lease")
    .expect("restored lease announcement");
    assert_eq!(restored.lease, window.lease);

    restarted.shutdown().expect("restarted runtime shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted runtime termination");
}

#[test]
fn training_window_persists_peer_window_metrics() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-training-metrics-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let storage = StorageConfig::new(storage_root);

    let running = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(storage.clone())
    .spawn()
    .expect("runtime spawn");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "runtime did not start",
    );

    let mut running = running;
    let trained = running
        .train_window_once(&experiment())
        .expect("training window");

    let metrics = load_metric_artifacts::<PeerWindowMetrics>(&storage, "peer-window-");
    assert_eq!(metrics.len(), 1);
    let peer_window = &metrics[0];
    assert_eq!(peer_window.lease_id, trained.lease.lease_id);
    assert_eq!(peer_window.base_head_id, trained.contribution.base_head_id);
    assert_eq!(
        peer_window.accepted_tokens_or_samples,
        Some(trained.lease.budget_work_units)
    );
    assert_eq!(peer_window.dataset_view_id, trained.lease.dataset_view_id);
    wait_for(
        Duration::from_secs(5),
        || {
            telemetry
                .snapshot()
                .control_plane
                .metrics_announcements
                .len()
                == 1
        },
        "runtime did not publish training metrics announcement",
    );
    let snapshot = telemetry.snapshot();
    let metrics_events = &snapshot.control_plane.metrics_announcements;
    assert_eq!(metrics_events.len(), 1);
    assert_eq!(
        metrics_events[0].event.kind,
        MetricsLiveEventKind::LedgerAppend
    );
    assert_eq!(
        metrics_events[0].event.cursors[0].experiment_id,
        experiment().experiment_id
    );
    assert_eq!(
        metrics_events[0].event.cursors[0].revision_id,
        experiment().revision_id
    );
    assert_eq!(
        metrics_events[0].event.cursors[0].latest_merge_window_id,
        Some(
            snapshot.control_plane.merge_window_announcements[0]
                .merge_window
                .merge_window_id
                .clone()
        )
    );
    assert_eq!(
        metrics_events[0].overlay,
        experiment().overlay_set().expect("overlays").metrics
    );
    assert!(
        peer_window.compute_time_ms <= peer_window.compute_time_ms + peer_window.data_fetch_time_ms
    );

    running.shutdown().expect("runtime shutdown");
    let _ = running.await_termination().expect("runtime termination");
}

#[test]
fn metrics_retention_auto_scales_with_node_roles() {
    let trainer_budget =
        MetricsRetentionConfig::default().resolve_for_roles(&crate::PeerRoleSet::default_trainer());
    let operator_budget =
        MetricsRetentionConfig::default().resolve_for_roles(&crate::PeerRoleSet::new([
            crate::PeerRole::Bootstrap,
            crate::PeerRole::Authority,
            crate::PeerRole::Validator,
        ]));

    assert!(
        trainer_budget.max_peer_window_entries_per_revision
            < operator_budget.max_peer_window_entries_per_revision
    );
    assert!(
        trainer_budget.max_reducer_cohort_entries_per_revision
            < operator_budget.max_reducer_cohort_entries_per_revision
    );
    assert!(
        trainer_budget.max_head_eval_reports_per_revision
            < operator_budget.max_head_eval_reports_per_revision
    );
    assert!(
        trainer_budget.max_metric_revisions_per_experiment
            < operator_budget.max_metric_revisions_per_experiment
    );
    assert!(
        trainer_budget.max_peer_window_detail_windows
            < operator_budget.max_peer_window_detail_windows
    );
}

#[test]
fn training_window_prunes_raw_metrics_archive_to_configured_budget() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-training-metrics-pruned-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let storage = StorageConfig::new(storage_root);

    let running = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(storage.clone())
    .with_metrics_retention(MetricsRetentionConfig {
        max_peer_window_entries_per_revision: Some(1),
        ..MetricsRetentionConfig::default()
    })
    .spawn()
    .expect("runtime spawn");

    let mut running = running;
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "runtime did not start",
    );
    running
        .train_window_once(&experiment())
        .expect("first training window");
    running
        .train_window_once(&experiment())
        .expect("second training window");

    let metrics = load_metric_artifacts::<PeerWindowMetrics>(&storage, "peer-window-");
    assert_eq!(metrics.len(), 1);

    running.shutdown().expect("runtime shutdown");
    let _ = running.await_termination().expect("runtime termination");
}

#[test]
fn training_window_prunes_old_metric_revisions_to_configured_budget() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let storage_root = std::env::temp_dir().join(format!(
        "burn-p2p-training-metrics-revisions-pruned-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let storage = StorageConfig::new(storage_root);
    let revision_a = experiment();
    let revision_b = mainnet().experiment(
        crate::StudyId::new("study-1"),
        crate::ExperimentId::new("exp-1"),
        crate::RevisionId::new("rev-2"),
    );

    let running = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(storage.clone())
    .with_metrics_retention(MetricsRetentionConfig {
        max_peer_window_entries_per_revision: Some(2),
        max_metric_revisions_per_experiment: Some(1),
        ..MetricsRetentionConfig::default()
    })
    .spawn()
    .expect("runtime spawn");

    let mut running = running;
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().local_peer_id.is_some(),
        "runtime did not start",
    );
    running
        .train_window_once(&revision_a)
        .expect("first revision training window");
    running
        .train_window_once(&revision_b)
        .expect("second revision training window");

    let metrics = load_metric_artifacts::<PeerWindowMetrics>(&storage, "peer-window-");
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].revision_id, revision_b.revision_id);

    running.shutdown().expect("runtime shutdown");
    let _ = running.await_termination().expect("runtime termination");
}

#[test]
fn validation_persists_cohort_and_head_eval_metrics() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());

    let validator_storage = StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-validation-metrics-validator-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    )));
    let trainer_storage = StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-validation-metrics-trainer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    )));

    let validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(validator_storage.clone())
    .spawn()
    .expect("validator spawn");
    let validator_telemetry = validator.telemetry();
    wait_for(
        Duration::from_secs(5),
        || !validator_telemetry.snapshot().listen_addresses.is_empty(),
        "validator did not start listening",
    );
    let validator_addr = validator_telemetry.snapshot().listen_addresses[0].clone();

    let experiment = experiment();
    let mut validator = validator;
    let genesis_head = validator
        .initialize_local_head(&experiment)
        .expect("init validator genesis head");

    let trainer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 0.5,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(trainer_storage)
    .with_bootstrap_peer(validator_addr)
    .spawn()
    .expect("trainer spawn");
    let trainer_telemetry = trainer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || trainer_telemetry.snapshot().connected_peers >= 1,
        "trainer did not connect",
    );
    wait_for(
        Duration::from_secs(5),
        || {
            trainer
                .sync_experiment_head(&experiment)
                .expect("sync trainer head")
                .is_some()
        },
        "trainer did not sync the canonical genesis head",
    );

    let mut trainer = trainer;
    let trained = trainer
        .train_window_once(&experiment)
        .expect("trainer window");

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_telemetry.snapshot();
            snapshot
                .control_plane
                .update_announcements
                .iter()
                .any(|announcement| {
                    announcement.update.delta_artifact_id == trained.artifact.artifact_id
                })
                && snapshot
                    .control_plane
                    .head_announcements
                    .iter()
                    .any(|announcement| announcement.head.head_id == trained.head.head_id)
        },
        "validator did not observe trainer artifacts",
    );

    let validated = validator
        .validate_candidates_once(&experiment)
        .expect("validate")
        .expect("validation outcome");

    let peer_windows =
        load_metric_artifacts::<PeerWindowMetrics>(&validator_storage, "peer-window-");
    let reducer_cohorts =
        load_metric_artifacts::<ReducerCohortMetrics>(&validator_storage, "reducer-cohort-");
    let head_eval_reports =
        load_metric_artifacts::<HeadEvalReport>(&validator_storage, "head-eval-");
    let eval_protocol_manifests = load_metric_artifacts::<StoredEvalProtocolManifestArtifact>(
        &validator_storage,
        "eval-protocol-",
    );

    assert_eq!(peer_windows.len(), 1);
    assert_eq!(reducer_cohorts.len(), 1);
    assert_eq!(head_eval_reports.len(), 1);
    assert_eq!(eval_protocol_manifests.len(), 1);
    assert_eq!(peer_windows[0].base_head_id, genesis_head.head_id);
    assert_eq!(
        reducer_cohorts[0].candidate_head_id,
        Some(validated.merged_head.head_id.clone())
    );
    assert_eq!(head_eval_reports[0].head_id, validated.merged_head.head_id);
    assert_eq!(
        head_eval_reports[0].base_head_id,
        validated.merged_head.parent_head_id
    );
    assert_eq!(
        eval_protocol_manifests[0].experiment_id,
        experiment.experiment_id.clone()
    );
    assert_eq!(
        eval_protocol_manifests[0].revision_id,
        experiment.revision_id.clone()
    );
    assert_eq!(
        eval_protocol_manifests[0].manifest.eval_protocol_id,
        head_eval_reports[0].eval_protocol_id
    );
    assert!(eval_protocol_manifests[0].captured_at <= Utc::now());
    wait_for(
        Duration::from_secs(5),
        || {
            validator_telemetry
                .snapshot()
                .control_plane
                .metrics_announcements
                .len()
                >= 2
        },
        "validator did not observe training and validation metrics announcements",
    );
    let metrics_events = &validator_telemetry
        .snapshot()
        .control_plane
        .metrics_announcements;
    assert_eq!(metrics_events.len(), 2);
    assert_eq!(
        metrics_events[0].event.kind,
        MetricsLiveEventKind::LedgerAppend
    );
    assert_eq!(
        metrics_events[1].event.kind,
        MetricsLiveEventKind::CatchupRefresh
    );
    assert_eq!(
        metrics_events[1].event.cursors[0].latest_head_id,
        Some(validated.merged_head.head_id.clone())
    );
    assert_eq!(
        metrics_events[1].event.cursors[0].latest_merge_window_id,
        Some(reducer_cohorts[0].merge_window_id.clone())
    );
    assert_eq!(
        metrics_events[1].overlay,
        experiment.overlay_set().expect("overlays").metrics
    );

    trainer.shutdown().expect("trainer shutdown");
    let _ = trainer.await_termination().expect("trainer termination");
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");
}

#[test]
fn control_handle_updates_local_head_announcements() {
    let running = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_listen_address(SwarmAddress::new("/memory/0").expect("listen"))
        .spawn()
        .expect("spawn");

    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(2),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "runtime did not start",
    );

    let experiment = running.experiment(
        crate::StudyId::new("study-1"),
        crate::ExperimentId::new("exp-1"),
        crate::RevisionId::new("rev-1"),
    );
    let control = running.control_handle();
    control
        .publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set().expect("overlays").heads,
            provider_peer_id: None,
            head: HeadDescriptor {
                head_id: crate::HeadId::new("head-1"),
                study_id: crate::StudyId::new("study-1"),
                experiment_id: crate::ExperimentId::new("exp-1"),
                revision_id: crate::RevisionId::new("rev-1"),
                artifact_id: crate::ArtifactId::new("artifact-1"),
                parent_head_id: Some(crate::HeadId::new("head-0")),
                global_step: 1,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        })
        .expect("publish head");

    wait_for(
        Duration::from_secs(2),
        || telemetry.snapshot().control_plane.head_announcements.len() == 1,
        "head announcement was not reflected in local telemetry",
    );

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.control_plane.head_announcements.len(), 1);
    assert!(snapshot.local_peer_id.is_some());

    running.shutdown().expect("shutdown");
    let _ = running.await_termination().expect("await termination");
}

#[test]
fn control_handle_updates_auth_and_directory_announcements() {
    let running = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_listen_address(SwarmAddress::new("/memory/0").expect("listen"))
        .spawn()
        .expect("spawn");

    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(2),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "runtime did not start",
    );

    let experiment_id = crate::ExperimentId::new("exp-auth");
    let control = running.control_handle();
    control
        .publish_auth(PeerAuthAnnouncement {
            peer_id: crate::PeerId::new("peer-auth"),
            envelope: PeerAuthEnvelope {
                peer_id: crate::PeerId::new("peer-auth"),
                certificate: NodeCertificate::new(
                    semver::Version::new(0, 1, 0),
                    NodeCertificateClaims {
                        network_id: crate::NetworkId::new("net-1"),
                        project_family_id: crate::ProjectFamilyId::new("family-auth"),
                        release_train_hash: crate::ContentId::new("train-auth"),
                        target_artifact_hash: crate::ContentId::new("artifact-native-auth"),
                        peer_id: crate::PeerId::new("peer-auth"),
                        peer_public_key_hex: "001122".into(),
                        principal_id: crate::PrincipalId::new("principal-auth"),
                        provider: crate::AuthProvider::Static {
                            authority: "lab-auth".into(),
                        },
                        granted_roles: crate::PeerRoleSet::new([crate::PeerRole::TrainerGpu]),
                        experiment_scopes: BTreeSet::from([
                            crate::ExperimentScope::Connect,
                            crate::ExperimentScope::Train {
                                experiment_id: experiment_id.clone(),
                            },
                        ]),
                        client_policy_hash: Some(crate::ContentId::new("policy-auth")),
                        not_before: Utc::now(),
                        not_after: Utc::now(),
                        serial: 5,
                        revocation_epoch: crate::RevocationEpoch(2),
                    },
                    burn_p2p_core::SignatureMetadata {
                        signer: crate::PeerId::new("authority-auth"),
                        key_id: "authority-key".into(),
                        algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                        signed_at: Utc::now(),
                        signature_hex: "deadbeef".into(),
                    },
                )
                .expect("node certificate"),
                client_manifest_id: Some(crate::ContentId::new("manifest-auth")),
                requested_scopes: BTreeSet::from([crate::ExperimentScope::Train {
                    experiment_id: experiment_id.clone(),
                }]),
                nonce_hash: crate::ContentId::new("nonce-auth"),
                challenge_signature_hex: "feedbead".into(),
                presented_at: Utc::now(),
            },
            announced_at: Utc::now(),
        })
        .expect("publish auth");
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: crate::NetworkId::new("net-1"),
            entries: vec![ExperimentDirectoryEntry {
                network_id: crate::NetworkId::new("net-1"),
                study_id: crate::StudyId::new("study-auth"),
                experiment_id: experiment_id.clone(),
                workload_id: crate::WorkloadId::new("auth-demo"),
                display_name: "Auth Demo".into(),
                model_schema_hash: crate::ContentId::new("model-auth"),
                dataset_view_id: crate::DatasetViewId::new("view-auth"),
                resource_requirements: ExperimentResourceRequirements {
                    minimum_roles: BTreeSet::from([crate::PeerRole::TrainerGpu]),
                    minimum_device_memory_bytes: Some(1024),
                    minimum_system_memory_bytes: Some(2048),
                    estimated_download_bytes: 8192,
                    estimated_window_seconds: 45,
                },
                visibility: crate::ExperimentVisibility::OptIn,
                opt_in_policy: crate::ExperimentOptInPolicy::Scoped,
                current_revision_id: crate::RevisionId::new("rev-auth"),
                current_head_id: Some(crate::HeadId::new("head-auth")),
                allowed_roles: crate::PeerRoleSet::new([crate::PeerRole::TrainerGpu]),
                allowed_scopes: BTreeSet::from([crate::ExperimentScope::Train { experiment_id }]),
                metadata: BTreeMap::from([("owner".into(), "lab-auth".into())]),
            }],
            announced_at: Utc::now(),
        })
        .expect("publish directory");

    wait_for(
        Duration::from_secs(2),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.control_plane.auth_announcements.len() == 1
                && snapshot.control_plane.directory_announcements.len() == 1
        },
        "auth or directory announcements were not reflected in local telemetry",
    );

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.control_plane.auth_announcements.len(), 1);
    assert_eq!(snapshot.control_plane.directory_announcements.len(), 1);

    running.shutdown().expect("shutdown");
    let _ = running.await_termination().expect("await termination");
}

#[test]
fn persisted_local_auth_and_policy_survive_restart_without_with_auth() {
    let storage = tempdir().expect("persisted auth storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    let authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-key",
    )
    .expect("authority");
    let admission_policy = auth_test_admission_policy(&authority);
    let experiment_id = crate::ExperimentId::new("exp-auth-restored");

    let first = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config.clone())
        .with_auth(crate::AuthConfig::new().with_admission_policy(admission_policy.clone()))
        .spawn()
        .expect("first spawn");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = first_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.local_peer_id.is_some()
        },
        "first runtime did not start",
    );
    let local_peer_id = first_telemetry
        .snapshot()
        .local_peer_id
        .expect("local peer id");
    let local_peer_auth = issue_local_test_peer_auth(
        &storage_config,
        &authority,
        &local_peer_id,
        experiment_id.clone(),
    );
    let control = first.control_handle();
    control
        .publish_auth(PeerAuthAnnouncement {
            peer_id: local_peer_id.clone(),
            envelope: local_peer_auth.clone(),
            announced_at: Utc::now(),
        })
        .expect("publish local auth");
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![auth_scoped_directory_entry(experiment_id.clone())],
            announced_at: Utc::now(),
        })
        .expect("publish scoped directory");
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = first_telemetry.snapshot();
            snapshot
                .control_plane
                .auth_announcements
                .iter()
                .any(|announcement| announcement.peer_id == local_peer_id)
                && !snapshot.control_plane.directory_announcements.is_empty()
        },
        "first runtime did not publish persisted auth state",
    );

    first.shutdown().expect("first shutdown");
    let _ = first.await_termination().expect("first termination");
    assert!(
        fs::read(storage_config.auth_state_path()).is_ok(),
        "auth state should be persisted"
    );

    let restarted = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config.clone())
        .spawn()
        .expect("restarted spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = restarted_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id == Some(local_peer_id.clone())
                && snapshot
                    .control_plane
                    .directory_announcements
                    .iter()
                    .any(|announcement| {
                        announcement
                            .entries
                            .iter()
                            .any(|entry| entry.experiment_id == experiment_id)
                    })
        },
        "restarted runtime did not restore persisted auth state",
    );

    let restored_auth = restarted
        .config()
        .auth
        .as_ref()
        .expect("restored auth config");
    assert_eq!(
        restored_auth.admission_policy.as_ref(),
        Some(&admission_policy)
    );
    assert_eq!(
        restored_auth.local_peer_auth.as_ref(),
        Some(&local_peer_auth)
    );
    let visible = restarted.list_experiments();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].experiment_id, experiment_id);

    restarted.shutdown().expect("restarted shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted termination");
}

#[test]
fn admitted_peer_security_state_survives_restart() {
    let authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-key",
    )
    .expect("authority");
    let admission_policy = auth_test_admission_policy(&authority);
    let experiment_id = crate::ExperimentId::new("exp-auth-admitted");
    let listener_storage = tempdir().expect("listener storage");
    let listener_storage_config = StorageConfig::new(listener_storage.path().to_path_buf());
    let dialer_storage = tempdir().expect("dialer storage");
    let dialer_storage_config = StorageConfig::new(dialer_storage.path().to_path_buf());
    dialer_storage_config
        .ensure_layout()
        .expect("dialer storage layout");

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(listener_storage_config.clone())
        .with_auth(crate::AuthConfig::new().with_admission_policy(admission_policy.clone()))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "listener did not start",
    );
    let listener_addr = listener_telemetry.snapshot().listen_addresses[0].clone();

    let dialer_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &super::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&dialer_storage_config),
            )
            .expect("resolve dialer identity")
            .public(),
        )
        .to_string(),
    );
    let dialer_auth = issue_local_test_peer_auth(
        &dialer_storage_config,
        &authority,
        &dialer_peer_id,
        experiment_id.clone(),
    );
    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(dialer_storage_config.clone())
        .with_bootstrap_peer(listener_addr)
        .with_auth(
            crate::AuthConfig::new()
                .with_local_peer_auth(dialer_auth)
                .with_experiment_directory(vec![auth_scoped_directory_entry(
                    experiment_id.clone(),
                )]),
        )
        .spawn()
        .expect("dialer spawn");

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.admitted_peers.contains_key(&dialer_peer_id)
                && snapshot.peer_reputation.contains_key(&dialer_peer_id)
                && snapshot.minimum_revocation_epoch == Some(crate::RevocationEpoch(1))
        },
        "listener did not persist admitted peer security state",
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");

    assert!(
        fs::read(listener_storage_config.security_state_path()).is_ok(),
        "security state should be persisted"
    );

    let restarted = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(listener_storage_config.clone())
        .spawn()
        .expect("restarted listener spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = restarted_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.admitted_peers.contains_key(&dialer_peer_id)
                && snapshot.peer_reputation.contains_key(&dialer_peer_id)
                && snapshot.minimum_revocation_epoch == Some(crate::RevocationEpoch(1))
        },
        "restarted listener did not restore admitted security state",
    );

    restarted.shutdown().expect("restarted listener shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted listener termination");
}

#[test]
fn rejected_peer_reason_survives_restart() {
    let authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-key",
    )
    .expect("authority");
    let admission_policy = auth_test_admission_policy(&authority);
    let listener_storage = tempdir().expect("listener storage");
    let listener_storage_config = StorageConfig::new(listener_storage.path().to_path_buf());
    let dialer_storage = tempdir().expect("dialer storage");
    let dialer_storage_config = StorageConfig::new(dialer_storage.path().to_path_buf());
    dialer_storage_config
        .ensure_layout()
        .expect("dialer storage layout");

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(listener_storage_config.clone())
        .with_auth(crate::AuthConfig::new().with_admission_policy(admission_policy))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "listener did not start",
    );
    let listener_addr = listener_telemetry.snapshot().listen_addresses[0].clone();

    let dialer_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &super::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&dialer_storage_config),
            )
            .expect("resolve dialer identity")
            .public(),
        )
        .to_string(),
    );
    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(dialer_storage_config)
        .with_bootstrap_peer(listener_addr)
        .spawn()
        .expect("dialer spawn");

    wait_for(
        Duration::from_secs(5),
        || {
            listener_telemetry
                .snapshot()
                .rejected_peers
                .get(&dialer_peer_id)
                .is_some_and(|reason| reason.contains("did not publish an auth envelope"))
        },
        "listener did not persist rejected peer reason",
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");

    let restarted = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(listener_storage_config)
        .spawn()
        .expect("restarted listener spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            restarted_telemetry
                .snapshot()
                .rejected_peers
                .get(&dialer_peer_id)
                .is_some_and(|reason| reason.contains("did not publish an auth envelope"))
        },
        "restarted listener did not restore rejected peer reason",
    );

    restarted.shutdown().expect("restarted listener shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted listener termination");
}

#[test]
fn live_revocation_epoch_updates_reject_stale_peer_sync() {
    let authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-key",
    )
    .expect("authority");
    let admission_policy = auth_test_admission_policy(&authority);
    let experiment_id = crate::ExperimentId::new("exp-auth-revoke");
    let listener_storage = tempdir().expect("listener storage");
    let listener_storage_config = StorageConfig::new(listener_storage.path().to_path_buf());
    let dialer_storage = tempdir().expect("dialer storage");
    let dialer_storage_config = StorageConfig::new(dialer_storage.path().to_path_buf());
    dialer_storage_config
        .ensure_layout()
        .expect("dialer storage layout");
    let payload = b"artifact-bytes-before-revocation".to_vec();

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(listener_storage_config)
        .with_auth(crate::AuthConfig::new().with_admission_policy(admission_policy))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "listener did not start",
    );
    let listener_addr = listener_telemetry.snapshot().listen_addresses[0].clone();

    let dialer_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &super::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&dialer_storage_config),
            )
            .expect("resolve dialer identity")
            .public(),
        )
        .to_string(),
    );
    let dialer_auth = issue_local_test_peer_auth(
        &dialer_storage_config,
        &authority,
        &dialer_peer_id,
        experiment_id.clone(),
    );
    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(dialer_storage_config.clone())
        .with_bootstrap_peer(listener_addr)
        .with_auth(
            crate::AuthConfig::new()
                .with_local_peer_auth(dialer_auth)
                .with_experiment_directory(vec![auth_scoped_directory_entry(
                    experiment_id.clone(),
                )]),
        )
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.connected_peers >= 1
                && snapshot
                    .control_plane
                    .auth_announcements
                    .iter()
                    .any(|announcement| announcement.peer_id == dialer_peer_id)
        },
        "dialer did not connect",
    );
    wait_for(
        Duration::from_secs(5),
        || listener_telemetry.snapshot().connected_peers >= 1,
        "listener did not observe dialer connection",
    );
    listener
        .control_handle()
        .request_snapshot(dialer_peer_id.as_str())
        .expect("request dialer snapshot");
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.admitted_peers.contains_key(&dialer_peer_id)
        },
        "listener did not admit dialer",
    );

    let dialer_store = dialer.artifact_store().expect("dialer store");
    let descriptor = dialer_store
        .store_artifact_reader(
            &ArtifactBuildSpec::new(
                crate::ArtifactKind::ServeHead,
                crate::Precision::Fp16,
                crate::ContentId::new("schema-auth-revoke"),
                "burn-record:bin",
            ),
            std::io::Cursor::new(payload.clone()),
            ChunkingScheme::new(8).expect("chunking"),
        )
        .expect("store artifact");
    dialer
        .publish_artifact_from_store(&descriptor.artifact_id)
        .expect("publish artifact");

    let synced_descriptor = listener
        .sync_artifact_from_peer(&dialer_peer_id, descriptor.artifact_id.clone())
        .expect("initial artifact sync should succeed");
    assert_eq!(synced_descriptor, descriptor);

    listener
        .control_handle()
        .publish_control(control_announcement(
            burn_p2p_experiment::ExperimentControlCommand::RevokePeer {
                peer_id: dialer_peer_id.clone(),
                minimum_revocation_epoch: crate::RevocationEpoch(3),
                reason: "rotated certs".into(),
            },
        ))
        .expect("publish revoke control");

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.minimum_revocation_epoch == Some(crate::RevocationEpoch(3))
                && !snapshot.admitted_peers.contains_key(&dialer_peer_id)
        },
        "listener did not apply live revocation epoch",
    );

    let error = listener
        .sync_artifact_from_peer(&dialer_peer_id, descriptor.artifact_id.clone())
        .expect_err("revoked peer should be rejected for artifact sync");
    assert!(
        error.to_string().contains("not admitted for artifact sync")
            || error
                .to_string()
                .contains("request failure: Failed to dial the requested peer"),
        "unexpected error after live revocation: {error}",
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}

#[test]
fn local_revocation_control_moves_node_into_revoked_state() {
    let authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-key",
    )
    .expect("authority");
    let admission_policy = auth_test_admission_policy(&authority);
    let experiment_id = crate::ExperimentId::new("exp-auth-local-revoke");
    let storage = tempdir().expect("local revoke storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    storage_config
        .ensure_layout()
        .expect("local revoke storage layout");
    let local_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &super::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&storage_config),
            )
            .expect("resolve local identity")
            .public(),
        )
        .to_string(),
    );
    let local_peer_auth = issue_local_test_peer_auth(
        &storage_config,
        &authority,
        &local_peer_id,
        experiment_id.clone(),
    );

    let running = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config)
        .with_auth(
            crate::AuthConfig::new()
                .with_admission_policy(admission_policy)
                .with_local_peer_auth(local_peer_auth)
                .with_experiment_directory(vec![auth_scoped_directory_entry(
                    experiment_id.clone(),
                )]),
        )
        .spawn()
        .expect("local revoke runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id == Some(local_peer_id.clone())
                && snapshot
                    .control_plane
                    .auth_announcements
                    .iter()
                    .any(|announcement| announcement.peer_id == local_peer_id)
        },
        "local auth runtime did not publish its auth envelope",
    );

    running
        .control_handle()
        .publish_control(control_announcement(
            burn_p2p_experiment::ExperimentControlCommand::RevokePeer {
                peer_id: local_peer_id.clone(),
                minimum_revocation_epoch: crate::RevocationEpoch(3),
                reason: "local certificate revoked".into(),
            },
        ))
        .expect("publish local revoke control");

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.node_state == crate::NodeRuntimeState::Revoked
                && snapshot.minimum_revocation_epoch == Some(crate::RevocationEpoch(3))
                && matches!(
                    snapshot.slot_states.first(),
                    Some(crate::SlotRuntimeState::Blocked { reason, .. })
                        if reason.contains("local certificate revoked")
                )
        },
        "local runtime did not move into revoked state",
    );

    running.shutdown().expect("local revoke shutdown");
    let _ = running
        .await_termination()
        .expect("local revoke termination");
}

#[test]
fn automatic_trust_bundle_rollout_updates_policy_and_revokes_legacy_local_certificate() {
    let legacy_authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-legacy",
    )
    .expect("legacy authority");
    let rotated_authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-rotated",
    )
    .expect("rotated authority");
    let storage = tempdir().expect("trust bundle storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    storage_config
        .ensure_layout()
        .expect("trust bundle storage layout");
    let experiment_id = crate::ExperimentId::new("exp-auth-trust-rollout");
    let local_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &super::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&storage_config),
            )
            .expect("resolve local trust identity")
            .public(),
        )
        .to_string(),
    );
    let local_peer_auth = issue_local_test_peer_auth(
        &storage_config,
        &legacy_authority,
        &local_peer_id,
        experiment_id.clone(),
    );
    let initial_bundle = test_trust_bundle_export(
        &legacy_authority,
        Vec::new(),
        crate::RevocationEpoch(1),
        None,
    );
    let trust_server = TestTrustBundleServer::start(&initial_bundle);

    let running = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config.clone())
        .with_auth(
            crate::AuthConfig::new()
                .with_admission_policy(auth_test_admission_policy(&legacy_authority))
                .with_local_peer_auth(local_peer_auth)
                .with_trust_bundle_endpoint(trust_server.url().to_owned())
                .with_experiment_directory(vec![auth_scoped_directory_entry(
                    experiment_id.clone(),
                )]),
        )
        .spawn()
        .expect("trust rollout runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot
                    .trust_bundle
                    .as_ref()
                    .is_some_and(|bundle| bundle.source_url == trust_server.url())
                && snapshot.trust_bundle.as_ref().is_some_and(|bundle| {
                    bundle.active_issuer_peer_id == legacy_authority.issuer_peer_id()
                        && bundle.trusted_issuers.len() == 1
                })
        },
        "runtime did not ingest initial trust bundle",
    );

    trust_server.set_bundle(&test_trust_bundle_export(
        &rotated_authority,
        vec![(
            legacy_authority.issuer_peer_id(),
            legacy_authority.issuer_public_key_hex().into(),
            true,
        )],
        crate::RevocationEpoch(2),
        Some(ClientReenrollmentStatus {
            reason: "authority rotation".into(),
            rotated_at: Some(Utc::now()),
            legacy_issuer_peer_ids: BTreeSet::from([legacy_authority.issuer_peer_id()]),
            login_path: "/login/static".into(),
            enroll_path: "/enroll".into(),
            trust_bundle_path: "/trust".into(),
        }),
    ));

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.node_state != crate::NodeRuntimeState::Revoked
                && snapshot.minimum_revocation_epoch == Some(crate::RevocationEpoch(2))
                && snapshot.trust_bundle.as_ref().is_some_and(|bundle| {
                    bundle.active_issuer_peer_id == rotated_authority.issuer_peer_id()
                        && bundle
                            .trusted_issuers
                            .contains_key(&legacy_authority.issuer_peer_id())
                        && bundle
                            .trusted_issuers
                            .contains_key(&rotated_authority.issuer_peer_id())
                        && bundle.reenrollment.as_ref().is_some_and(|reenrollment| {
                            reenrollment
                                .legacy_issuer_peer_ids
                                .contains(&legacy_authority.issuer_peer_id())
                        })
                })
        },
        "runtime did not ingest rotated trust bundle",
    );

    let persisted_auth: serde_json::Value = serde_json::from_slice(
        &fs::read(storage_config.auth_state_path()).expect("read persisted auth state"),
    )
    .expect("decode persisted auth state");
    assert_eq!(
        persisted_auth["trust_bundle_endpoints"][0],
        serde_json::Value::String(trust_server.url().to_owned())
    );
    assert_eq!(
        persisted_auth["admission_policy"]["trusted_issuers"]
            .as_object()
            .map(|issuers| issuers.len()),
        Some(2)
    );

    trust_server.set_bundle(&test_trust_bundle_export(
        &rotated_authority,
        Vec::new(),
        crate::RevocationEpoch(2),
        None,
    ));

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.node_state == crate::NodeRuntimeState::Revoked
                && snapshot.trust_bundle.as_ref().is_some_and(|bundle| {
                    bundle.active_issuer_peer_id == rotated_authority.issuer_peer_id()
                        && bundle.trusted_issuers.len() == 1
                        && !bundle
                            .trusted_issuers
                            .contains_key(&legacy_authority.issuer_peer_id())
                })
                && matches!(
                    snapshot.slot_states.first(),
                    Some(crate::SlotRuntimeState::Blocked { reason, .. })
                        if reason.contains("untrusted issuer")
                )
        },
        "runtime did not revoke legacy local certificate after issuer retirement",
    );

    running.shutdown().expect("trust rollout shutdown");
    let _ = running
        .await_termination()
        .expect("trust rollout termination");
}

#[test]
fn trust_bundle_endpoints_survive_restart_and_resume_ingestion() {
    let legacy_authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-legacy",
    )
    .expect("legacy authority");
    let rotated_authority = crate::NodeCertificateAuthority::new(
        mainnet().genesis.network_id.clone(),
        crate::ProjectFamilyId::new("family-auth"),
        crate::ContentId::new("train-auth"),
        mainnet().genesis.protocol_version.clone(),
        libp2p_identity::Keypair::generate_ed25519(),
        "authority-rotated",
    )
    .expect("rotated authority");
    let storage = tempdir().expect("trust restart storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    storage_config
        .ensure_layout()
        .expect("trust restart storage layout");
    let experiment_id = crate::ExperimentId::new("exp-auth-trust-restart");
    let local_peer_id = crate::PeerId::new(
        libp2p_identity::PeerId::from_public_key(
            &super::runtime_support::resolve_identity(
                &crate::IdentityConfig::Persistent,
                Some(&storage_config),
            )
            .expect("resolve local trust restart identity")
            .public(),
        )
        .to_string(),
    );
    let local_peer_auth = issue_local_test_peer_auth(
        &storage_config,
        &legacy_authority,
        &local_peer_id,
        experiment_id.clone(),
    );
    let trust_server = TestTrustBundleServer::start(&test_trust_bundle_export(
        &legacy_authority,
        Vec::new(),
        crate::RevocationEpoch(1),
        None,
    ));

    let first = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config.clone())
        .with_auth(
            crate::AuthConfig::new()
                .with_admission_policy(auth_test_admission_policy(&legacy_authority))
                .with_local_peer_auth(local_peer_auth)
                .with_trust_bundle_endpoint(trust_server.url().to_owned())
                .with_experiment_directory(vec![auth_scoped_directory_entry(
                    experiment_id.clone(),
                )]),
        )
        .spawn()
        .expect("first trust restart runtime");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            first_telemetry
                .snapshot()
                .trust_bundle
                .as_ref()
                .is_some_and(|bundle| {
                    bundle.source_url == trust_server.url()
                        && bundle.active_issuer_peer_id == legacy_authority.issuer_peer_id()
                })
        },
        "first runtime did not ingest initial trust bundle",
    );
    first.shutdown().expect("first trust restart shutdown");
    let _ = first
        .await_termination()
        .expect("first trust restart termination");

    trust_server.set_bundle(&test_trust_bundle_export(
        &rotated_authority,
        vec![(
            legacy_authority.issuer_peer_id(),
            legacy_authority.issuer_public_key_hex().into(),
            true,
        )],
        crate::RevocationEpoch(2),
        None,
    ));

    let restarted = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(storage_config.clone())
        .spawn()
        .expect("restarted trust runtime");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = restarted_telemetry.snapshot();
            snapshot.trust_bundle.as_ref().is_some_and(|bundle| {
                bundle.source_url == trust_server.url()
                    && bundle.active_issuer_peer_id == rotated_authority.issuer_peer_id()
                    && bundle
                        .trusted_issuers
                        .contains_key(&legacy_authority.issuer_peer_id())
                    && bundle
                        .trusted_issuers
                        .contains_key(&rotated_authority.issuer_peer_id())
            }) && restarted.config().auth.as_ref().is_some_and(|auth| {
                auth.trust_bundle_endpoints == vec![trust_server.url().to_owned()]
            })
        },
        "restarted runtime did not restore trust bundle source or resume ingestion",
    );

    restarted.shutdown().expect("restarted trust shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted trust termination");
}

#[test]
fn persisted_runtime_binding_survives_restart_without_with_network() {
    let storage = tempdir().expect("persisted runtime binding storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    let network_manifest = switching_network_manifest();

    let first = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(network_manifest.clone())
        .expect("network binding")
        .with_storage(storage_config.clone())
        .spawn()
        .expect("first switching runtime");
    let first_telemetry = first.telemetry();
    wait_for(
        Duration::from_secs(5),
        || first_telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "first switching runtime did not start",
    );
    first.shutdown().expect("first shutdown");
    let _ = first.await_termination().expect("first termination");

    assert!(
        fs::read(storage_config.runtime_binding_state_path()).is_ok(),
        "runtime binding should be persisted"
    );

    let restarted = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_storage(storage_config.clone())
        .spawn()
        .expect("restarted switching runtime");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || restarted_telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "restarted switching runtime did not start",
    );

    assert_eq!(
        restarted.config().network_manifest.as_ref(),
        Some(&network_manifest)
    );
    assert_eq!(
        restarted
            .config()
            .client_release_manifest
            .as_ref()
            .map(|manifest| manifest.release_train_hash.clone()),
        Some(crate::ContentId::new("train-switch"))
    );
    assert_eq!(
        restarted.mainnet().network_id(),
        &network_manifest.network_id
    );
    assert_eq!(
        restarted.telemetry().snapshot().network_id,
        Some(network_manifest.network_id.clone())
    );

    restarted.shutdown().expect("restarted shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted termination");
}

#[test]
fn native_running_nodes_exchange_snapshots_over_tcp() {
    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "listener runtime did not start",
    );

    let listener_snapshot = listener_telemetry.snapshot();
    let listener_peer_id = listener_snapshot
        .local_peer_id
        .clone()
        .expect("listener peer id");
    let listener_addr = listener_snapshot.listen_addresses[0].clone();

    let experiment = listener.experiment(
        crate::StudyId::new("study-1"),
        crate::ExperimentId::new("exp-1"),
        crate::RevisionId::new("rev-1"),
    );
    listener
        .control_handle()
        .publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set().expect("overlays").heads,
            provider_peer_id: Some(listener_peer_id.clone()),
            head: HeadDescriptor {
                head_id: crate::HeadId::new("head-native"),
                study_id: crate::StudyId::new("study-1"),
                experiment_id: crate::ExperimentId::new("exp-1"),
                revision_id: crate::RevisionId::new("rev-1"),
                artifact_id: crate::ArtifactId::new("artifact-native"),
                parent_head_id: Some(crate::HeadId::new("head-0")),
                global_step: 2,
                created_at: Utc::now(),
                metrics: BTreeMap::new(),
            },
            announced_at: Utc::now(),
        })
        .expect("publish head");
    wait_for(
        Duration::from_secs(5),
        || {
            listener_telemetry
                .snapshot()
                .control_plane
                .head_announcements
                .len()
                == 1
        },
        "listener head announcement was not reflected locally",
    );

    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_bootstrap_peer(listener_addr)
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.connected_peers >= 1
                && snapshot.local_peer_id.is_some()
        },
        "dialer runtime did not connect to listener",
    );

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.last_snapshot_peer_id == Some(listener_peer_id.clone())
                && snapshot
                    .last_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.head_announcements.len() == 1)
                    .unwrap_or(false)
        },
        "dialer did not receive listener snapshot automatically",
    );

    let last_snapshot = dialer_telemetry
        .snapshot()
        .last_snapshot
        .expect("last snapshot");
    assert_eq!(last_snapshot.head_announcements.len(), 1);
    assert_eq!(
        last_snapshot.head_announcements[0].head.head_id,
        crate::HeadId::new("head-native")
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}

#[test]
fn native_running_nodes_sync_artifacts_over_tcp() {
    let listener_storage = std::env::temp_dir().join(format!(
        "burn-p2p-artifact-listener-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let dialer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-artifact-dialer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let payload = b"artifact-bytes-over-native-sync".to_vec();

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(listener_storage.clone()))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "listener runtime did not start",
    );

    let listener_store = listener.artifact_store().expect("listener store");
    let descriptor = listener_store
        .store_artifact_reader(
            &ArtifactBuildSpec::new(
                crate::ArtifactKind::ServeHead,
                crate::Precision::Fp16,
                crate::ContentId::new("schema"),
                "burn-record:bin",
            ),
            std::io::Cursor::new(payload.clone()),
            ChunkingScheme::new(8).expect("chunking"),
        )
        .expect("store artifact");
    listener
        .publish_artifact_from_store(&descriptor.artifact_id)
        .expect("publish artifact");

    let listener_snapshot = listener_telemetry.snapshot();
    let listener_peer_id = listener_snapshot
        .local_peer_id
        .clone()
        .expect("listener peer id");
    let listener_addr = listener_snapshot.listen_addresses[0].clone();

    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(dialer_storage))
        .with_bootstrap_peer(listener_addr)
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.connected_peers >= 1
        },
        "dialer runtime did not connect to listener",
    );

    let synced_descriptor = dialer
        .sync_artifact_from_peer(&listener_peer_id, descriptor.artifact_id.clone())
        .expect("sync artifact");
    assert_eq!(synced_descriptor, descriptor);

    let dialer_store = dialer.artifact_store().expect("dialer store");
    let materialized = dialer_store
        .materialize_artifact_bytes(&synced_descriptor)
        .expect("materialize");
    assert_eq!(materialized, payload);

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}

#[test]
fn artifact_sync_resumes_from_persisted_transfer_state_after_restart() {
    let listener_storage = tempdir().expect("listener storage");
    let dialer_storage = tempdir().expect("dialer storage");
    let dialer_storage_config = StorageConfig::new(dialer_storage.path().to_path_buf());
    let payload = b"artifact-transfer-restart-resume".to_vec();

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(listener_storage.path().to_path_buf()))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "listener runtime did not start",
    );

    let listener_store = listener.artifact_store().expect("listener store");
    let descriptor = listener_store
        .store_artifact_reader(
            &ArtifactBuildSpec::new(
                crate::ArtifactKind::ServeHead,
                crate::Precision::Fp16,
                crate::ContentId::new("schema-transfer-resume"),
                "burn-record:bin",
            ),
            std::io::Cursor::new(payload.clone()),
            ChunkingScheme::new(8).expect("chunking"),
        )
        .expect("store artifact");
    listener
        .publish_artifact_from_store(&descriptor.artifact_id)
        .expect("publish artifact");

    let listener_snapshot = listener_telemetry.snapshot();
    let listener_peer_id = listener_snapshot
        .local_peer_id
        .clone()
        .expect("listener peer id");
    let listener_addr = listener_snapshot.listen_addresses[0].clone();

    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(dialer_storage_config.clone())
        .with_bootstrap_peer(listener_addr.clone())
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.connected_peers >= 1
        },
        "dialer runtime did not connect to listener",
    );

    let control = dialer.control_handle();
    let descriptor_from_peer = control
        .fetch_artifact_manifest(
            listener_peer_id.as_str(),
            descriptor.artifact_id.clone(),
            Duration::from_secs(2),
        )
        .expect("fetch artifact manifest")
        .expect("artifact manifest");
    assert_eq!(descriptor_from_peer, descriptor);

    let first_chunk = descriptor
        .chunks
        .first()
        .cloned()
        .expect("artifact should have at least one chunk");
    let first_payload = control
        .fetch_artifact_chunk(
            listener_peer_id.as_str(),
            descriptor.artifact_id.clone(),
            first_chunk.chunk_id.clone(),
            Duration::from_secs(2),
        )
        .expect("fetch first chunk")
        .expect("first chunk payload");
    let dialer_store = dialer.artifact_store().expect("dialer store");
    dialer_store
        .store_chunk_bytes(&first_payload.chunk, &first_payload.bytes)
        .expect("store first chunk");

    let mut transfer_state = ArtifactTransferState::new(descriptor.artifact_id.clone());
    transfer_state.source_peers = vec![listener_peer_id.clone()];
    transfer_state.set_provider(listener_peer_id.clone(), descriptor.clone());
    transfer_state.note_completed_chunk(&first_chunk.chunk_id);
    super::runtime_support::persist_artifact_transfer_state(
        &dialer_storage_config,
        &transfer_state,
    )
    .expect("persist transfer state");
    assert!(
        fs::read(dialer_storage_config.scoped_transfer_path(&descriptor.artifact_id)).is_ok(),
        "transfer state should be persisted before restart",
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");

    let restarted = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_identity(crate::IdentityConfig::Persistent)
        .with_storage(dialer_storage_config.clone())
        .with_bootstrap_peer(listener_addr)
        .spawn()
        .expect("restarted dialer spawn");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = restarted_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.connected_peers >= 1
                && snapshot
                    .in_flight_transfers
                    .get(&descriptor.artifact_id)
                    .is_some_and(|state| {
                        state.descriptor.as_ref() == Some(&descriptor)
                            && state.completed_chunks.contains(&first_chunk.chunk_id)
                    })
        },
        "restarted dialer did not restore persisted transfer state",
    );

    let synced = restarted
        .sync_artifact_from_peer(&listener_peer_id, descriptor.artifact_id.clone())
        .expect("resume artifact sync");
    assert_eq!(synced, descriptor);

    let restarted_store = restarted.artifact_store().expect("restarted store");
    let materialized = restarted_store
        .materialize_artifact_bytes(&synced)
        .expect("materialize resumed artifact");
    assert_eq!(materialized, payload);

    wait_for(
        Duration::from_secs(5),
        || {
            !restarted_telemetry
                .snapshot()
                .in_flight_transfers
                .contains_key(&descriptor.artifact_id)
        },
        "completed transfer should be cleared from telemetry",
    );
    assert!(
        super::runtime_support::load_artifact_transfer_state(
            &dialer_storage_config,
            &descriptor.artifact_id,
        )
        .expect("load cleared transfer state")
        .is_none(),
        "completed transfer state should be removed after manifest finalize",
    );

    restarted.shutdown().expect("restarted dialer shutdown");
    let _ = restarted
        .await_termination()
        .expect("restarted dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}

#[test]
fn native_artifact_sync_rejects_unadmitted_peer() {
    let listener_storage = std::env::temp_dir().join(format!(
        "burn-p2p-auth-artifact-listener-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let dialer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-auth-artifact-dialer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let payload = b"artifact-bytes-over-native-sync".to_vec();

    let listener = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(listener_storage))
        .spawn()
        .expect("listener spawn");
    let listener_telemetry = listener.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = listener_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "listener runtime did not start",
    );

    let listener_store = listener.artifact_store().expect("listener store");
    let descriptor = listener_store
        .store_artifact_reader(
            &ArtifactBuildSpec::new(
                crate::ArtifactKind::ServeHead,
                crate::Precision::Fp16,
                crate::ContentId::new("schema"),
                "burn-record:bin",
            ),
            std::io::Cursor::new(payload),
            ChunkingScheme::new(8).expect("chunking"),
        )
        .expect("store artifact");
    listener
        .publish_artifact_from_store(&descriptor.artifact_id)
        .expect("publish artifact");

    let listener_snapshot = listener_telemetry.snapshot();
    let listener_peer_id = listener_snapshot
        .local_peer_id
        .clone()
        .expect("listener peer id");
    let listener_addr = listener_snapshot.listen_addresses[0].clone();

    let dialer = NodeBuilder::new(())
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(dialer_storage))
        .with_bootstrap_peer(listener_addr)
        .with_auth(
            crate::AuthConfig::new().with_admission_policy(crate::AdmissionPolicy {
                network_id: mainnet().genesis.network_id.clone(),
                project_family_id: crate::ProjectFamilyId::new("family-auth"),
                required_release_train_hash: crate::ContentId::new("train-auth"),
                allowed_target_artifact_hashes: BTreeSet::from([crate::ContentId::new(
                    "artifact-native-auth",
                )]),
                trusted_issuers: BTreeMap::new(),
                minimum_revocation_epoch: crate::RevocationEpoch(0),
            }),
        )
        .spawn()
        .expect("dialer spawn");
    let dialer_telemetry = dialer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = dialer_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running && snapshot.connected_peers >= 1
        },
        "dialer runtime did not connect to listener",
    );

    let error = dialer
        .sync_artifact_from_peer(&listener_peer_id, descriptor.artifact_id.clone())
        .expect_err("artifact sync should reject unauthenticated peer");
    assert!(
        error
            .to_string()
            .contains("did not publish an auth envelope"),
        "unexpected error: {error}",
    );

    dialer.shutdown().expect("dialer shutdown");
    let _ = dialer.await_termination().expect("dialer termination");
    listener.shutdown().expect("listener shutdown");
    let _ = listener.await_termination().expect("listener termination");
}

#[test]
fn family_runtime_switches_experiments_and_restores_selection() {
    let storage = tempdir().expect("switching storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    let expected_assignment = crate::SlotAssignmentState::new(
        crate::StudyId::new("study-switch"),
        crate::ExperimentId::new("exp-b"),
        crate::RevisionId::new("rev-b"),
    );

    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(storage_config.clone())
        .with_auth(
            crate::AuthConfig::new().with_experiment_directory(switching_directory_entries()),
        )
        .spawn()
        .expect("spawn switching runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "switching runtime did not start",
    );

    let switched = running
        .switch_experiment(
            expected_assignment.study_id.clone(),
            expected_assignment.experiment_id.clone(),
            expected_assignment.revision_id.clone(),
        )
        .expect("switch experiment");
    assert_eq!(switched.experiment_id, expected_assignment.experiment_id);
    assert_eq!(
        running.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );
    assert_eq!(
        telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(
            expected_assignment.clone()
        ))
    );

    let persisted_assignment: crate::SlotAssignmentState = serde_json::from_slice(
        &fs::read(storage_config.primary_slot_assignment_path())
            .expect("read persisted slot assignment"),
    )
    .expect("decode persisted slot assignment");
    assert_eq!(persisted_assignment, expected_assignment);

    running.shutdown().expect("shutdown switching runtime");
    let node = running
        .await_termination()
        .expect("await switching runtime");
    assert_eq!(
        node.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );
    assert_eq!(
        node.into_project().selected_workload_id(),
        &crate::WorkloadId::new("alternate")
    );

    let mut restarted = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(storage_config.clone())
        .with_auth(
            crate::AuthConfig::new().with_experiment_directory(switching_directory_entries()),
        )
        .spawn()
        .expect("respawn switching runtime");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || restarted_telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "restarted switching runtime did not start",
    );
    assert_eq!(
        restarted_telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(
            expected_assignment.clone()
        ))
    );

    let restored = restarted
        .restore_selected_experiment()
        .expect("restore selected experiment")
        .expect("persisted experiment should restore");
    assert_eq!(restored.experiment_id, expected_assignment.experiment_id);
    assert_eq!(
        restarted.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );

    restarted.shutdown().expect("shutdown restarted runtime");
    let restarted_node = restarted
        .await_termination()
        .expect("await restarted runtime");
    assert_eq!(
        restarted_node.into_project().selected_workload_id(),
        &crate::WorkloadId::new("alternate")
    );
}

#[test]
fn family_runtime_follows_live_directory_revision_updates() {
    let storage = tempdir().expect("directory switching storage");
    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(StorageConfig::new(storage.path().to_path_buf()))
        .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![
            switching_directory_entry(
                "exp-a", "rev-a", "compiled", "schema-a", "view-a", "Switch A",
            ),
        ]))
        .spawn()
        .expect("spawn switching runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "switching runtime did not start",
    );

    let initial = running
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial experiment selection");
    assert_eq!(initial.revision_id, crate::RevisionId::new("rev-a"));
    assert_eq!(
        running.config().selected_workload_id,
        Some(crate::WorkloadId::new("compiled"))
    );

    running
        .control_handle()
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![switching_directory_entry(
                "exp-a",
                "rev-b",
                "alternate",
                "schema-b",
                "view-b",
                "Switch A Rev B",
            )],
            announced_at: Utc::now(),
        })
        .expect("publish updated directory");
    wait_for(
        Duration::from_secs(5),
        || {
            telemetry
                .snapshot()
                .control_plane
                .directory_announcements
                .len()
                >= 2
        },
        "updated directory was not reflected in telemetry",
    );

    let followed = running
        .follow_control_plane_once(crate::WindowId(2))
        .expect("follow control plane")
        .expect("active experiment");
    assert_eq!(followed.experiment_id, crate::ExperimentId::new("exp-a"));
    assert_eq!(followed.revision_id, crate::RevisionId::new("rev-b"));
    assert_eq!(
        running.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );
    assert_eq!(
        telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(
            crate::SlotAssignmentState::new(
                crate::StudyId::new("study-switch"),
                crate::ExperimentId::new("exp-a"),
                crate::RevisionId::new("rev-b"),
            )
        ))
    );

    running.shutdown().expect("shutdown switching runtime");
    let _ = running
        .await_termination()
        .expect("await switching runtime");
}

#[test]
fn family_runtime_applies_control_plane_pause_and_resume_at_window_boundaries() {
    let storage = tempdir().expect("control switching storage");
    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(StorageConfig::new(storage.path().to_path_buf()))
        .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![
            switching_directory_entry(
                "exp-a", "rev-a", "compiled", "schema-a", "view-a", "Switch A",
            ),
        ]))
        .spawn()
        .expect("spawn switching runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "switching runtime did not start",
    );

    let experiment = running
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial experiment selection");
    let assignment = crate::SlotAssignmentState::from_experiment(&experiment);

    let control = running.control_handle();
    control
        .publish_control(control_announcement(
            burn_p2p_experiment::ExperimentControlCommand::PauseExperiment {
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                reason: Some("maintenance".into()),
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(2),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
            },
        ))
        .expect("publish pause control");
    control
        .publish_control(control_announcement(
            burn_p2p_experiment::ExperimentControlCommand::ResumeExperiment {
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(3),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
            },
        ))
        .expect("publish resume control");
    wait_for(
        Duration::from_secs(5),
        || {
            telemetry
                .snapshot()
                .control_plane
                .control_announcements
                .len()
                >= 2
        },
        "control announcements were not reflected in telemetry",
    );

    running
        .follow_control_plane_once(crate::WindowId(1))
        .expect("follow control plane before pause");
    assert_eq!(
        telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(assignment.clone()))
    );

    running
        .follow_control_plane_once(crate::WindowId(2))
        .expect("follow control plane at pause");
    match telemetry
        .snapshot()
        .slot_states
        .first()
        .expect("slot state should exist")
    {
        crate::SlotRuntimeState::Blocked {
            assignment: blocked_assignment,
            reason,
        } => {
            assert_eq!(blocked_assignment.as_ref(), Some(&assignment));
            assert!(reason.contains("maintenance"));
        }
        other => panic!("expected blocked slot state, got {other:?}"),
    }

    running
        .follow_control_plane_once(crate::WindowId(3))
        .expect("follow control plane at resume");
    assert_eq!(
        telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(assignment))
    );

    running.shutdown().expect("shutdown switching runtime");
    let _ = running
        .await_termination()
        .expect("await switching runtime");
}

#[test]
fn family_runtime_restores_persisted_control_plane_state_after_restart() {
    let storage = tempdir().expect("persisted control storage");
    let storage_config = StorageConfig::new(storage.path().to_path_buf());
    let stale_directory = vec![switching_directory_entry(
        "exp-a", "rev-a", "compiled", "schema-a", "view-a", "Switch A",
    )];
    let mut running = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(storage_config.clone())
        .with_auth(crate::AuthConfig::new().with_experiment_directory(stale_directory.clone()))
        .spawn()
        .expect("spawn switching runtime");
    let telemetry = running.telemetry();
    wait_for(
        Duration::from_secs(5),
        || telemetry.snapshot().status == crate::RuntimeStatus::Running,
        "switching runtime did not start",
    );

    let experiment = running
        .switch_experiment(
            crate::StudyId::new("study-switch"),
            crate::ExperimentId::new("exp-a"),
            crate::RevisionId::new("rev-a"),
        )
        .expect("initial experiment selection");

    let control = running.control_handle();
    control
        .publish_directory(ExperimentDirectoryAnnouncement {
            network_id: mainnet().genesis.network_id.clone(),
            entries: vec![switching_directory_entry(
                "exp-a",
                "rev-b",
                "alternate",
                "schema-b",
                "view-b",
                "Switch A Rev B",
            )],
            announced_at: Utc::now(),
        })
        .expect("publish updated directory");
    control
        .publish_control(control_announcement(
            burn_p2p_experiment::ExperimentControlCommand::PauseExperiment {
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                reason: Some("maintenance".into()),
                target: burn_p2p_experiment::ActivationTarget {
                    activation: crate::WindowActivation {
                        activation_window: crate::WindowId(2),
                        grace_windows: 0,
                    },
                    required_client_capabilities: BTreeSet::new(),
                },
            },
        ))
        .expect("publish pause control");
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = telemetry.snapshot();
            snapshot.control_plane.directory_announcements.len() >= 2
                && !snapshot.control_plane.control_announcements.is_empty()
        },
        "updated control plane state was not reflected in telemetry",
    );

    running.shutdown().expect("shutdown switching runtime");
    let _ = running
        .await_termination()
        .expect("await switching runtime");

    assert!(
        fs::read(storage_config.control_plane_state_path()).is_ok(),
        "control plane state should be persisted before restart"
    );

    let mut restarted = NodeBuilder::new(switching_test_family())
        .for_workload(crate::WorkloadId::new("compiled"))
        .expect("compiled workload")
        .with_network(switching_network_manifest())
        .expect("network binding")
        .with_storage(storage_config.clone())
        .with_auth(crate::AuthConfig::new().with_experiment_directory(stale_directory))
        .spawn()
        .expect("respawn switching runtime");
    let restarted_telemetry = restarted.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = restarted_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot
                    .control_plane
                    .directory_announcements
                    .iter()
                    .any(|announcement| {
                        announcement.entries.iter().any(|entry| {
                            entry.experiment_id == crate::ExperimentId::new("exp-a")
                                && entry.current_revision_id == crate::RevisionId::new("rev-b")
                                && entry.workload_id == crate::WorkloadId::new("alternate")
                        })
                    })
                && snapshot
                    .control_plane
                    .control_announcements
                    .iter()
                    .any(|announcement| {
                        matches!(
                            announcement.certificate.body.payload.payload.command,
                            burn_p2p_experiment::ExperimentControlCommand::PauseExperiment { .. }
                        )
                    })
        },
        "persisted control plane state was not restored after restart",
    );

    let followed = restarted
        .follow_control_plane_once(crate::WindowId(2))
        .expect("follow restored control plane")
        .expect("active experiment after restore");
    assert_eq!(followed.revision_id, crate::RevisionId::new("rev-b"));
    assert_eq!(
        restarted.config().selected_workload_id,
        Some(crate::WorkloadId::new("alternate"))
    );
    assert_eq!(
        restarted_telemetry.snapshot().slot_states.first(),
        Some(&crate::SlotRuntimeState::Blocked {
            assignment: Some(crate::SlotAssignmentState::new(
                crate::StudyId::new("study-switch"),
                crate::ExperimentId::new("exp-a"),
                crate::RevisionId::new("rev-b"),
            )),
            reason: "maintenance".into(),
        })
    );

    restarted.shutdown().expect("shutdown restarted runtime");
    let _ = restarted
        .await_termination()
        .expect("await restarted runtime");
}

#[test]
fn native_runtime_training_and_validation_progresses_across_peers() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());

    let validator_storage = std::env::temp_dir().join(format!(
        "burn-p2p-validator-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let trainer_a_storage = std::env::temp_dir().join(format!(
        "burn-p2p-trainer-a-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let trainer_b_storage = std::env::temp_dir().join(format!(
        "burn-p2p-trainer-b-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let late_joiner_storage = std::env::temp_dir().join(format!(
        "burn-p2p-late-joiner-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));

    let experiment = experiment();
    let validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(validator_storage.clone()))
    .spawn()
    .expect("validator spawn");
    let validator_telemetry = validator.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && snapshot.local_peer_id.is_some()
                && !snapshot.listen_addresses.is_empty()
        },
        "validator runtime did not start",
    );
    let validator_addr = validator_telemetry.snapshot().listen_addresses[0].clone();

    let mut validator = validator;
    let genesis_head = validator
        .initialize_local_head(&experiment)
        .expect("init genesis head");
    assert_eq!(genesis_head.global_step, 0);

    let trainer_a = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(trainer_a_storage))
    .with_bootstrap_peer(validator_addr.clone())
    .spawn()
    .expect("trainer a spawn");
    let trainer_b = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 0.5,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(trainer_b_storage))
    .with_bootstrap_peer(validator_addr.clone())
    .spawn()
    .expect("trainer b spawn");

    let trainer_a_telemetry = trainer_a.telemetry();
    let trainer_b_telemetry = trainer_b.telemetry();
    wait_for(
        Duration::from_secs(5),
        || validator_telemetry.snapshot().connected_peers >= 2,
        "validator did not connect to trainers",
    );
    wait_for(
        Duration::from_secs(5),
        || trainer_a_telemetry.snapshot().connected_peers >= 1,
        "trainer a did not connect",
    );
    wait_for(
        Duration::from_secs(5),
        || trainer_b_telemetry.snapshot().connected_peers >= 1,
        "trainer b did not connect",
    );

    for trainer in [&trainer_a, &trainer_b] {
        wait_for(
            Duration::from_secs(5),
            || {
                trainer
                    .sync_experiment_head(&experiment)
                    .expect("sync trainer head")
                    .is_some()
            },
            "trainer did not sync the canonical genesis head",
        );
    }

    let mut trainer_a = trainer_a;
    let mut trainer_b = trainer_b;
    let trainer_a_window = trainer_a
        .train_window_once(&experiment)
        .expect("trainer a window");
    let trainer_b_window = trainer_b
        .train_window_once(&experiment)
        .expect("trainer b window");

    assert_eq!(
        trainer_a_window.head.parent_head_id,
        Some(genesis_head.head_id.clone())
    );
    assert_eq!(
        trainer_b_window.head.parent_head_id,
        Some(genesis_head.head_id.clone())
    );
    let trainer_a_state = trainer_a.telemetry().snapshot();
    assert_eq!(
        trainer_a_state.node_state,
        crate::NodeRuntimeState::WaitingMerge
    );
    assert_eq!(
        trainer_a_state.slot_states.first(),
        Some(&crate::SlotRuntimeState::CoolingDown(
            crate::SlotAssignmentState::from_experiment(&experiment)
        ))
    );
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_telemetry.snapshot();
            snapshot.control_plane.update_announcements.len() >= 2
                && !snapshot.control_plane.merge_window_announcements.is_empty()
                && snapshot
                    .control_plane
                    .reducer_assignment_announcements
                    .len()
                    >= 2
        },
        "validator did not observe live merge topology announcements",
    );
    let topology_snapshot = validator_telemetry.snapshot();
    assert!(
        topology_snapshot
            .control_plane
            .update_announcements
            .iter()
            .all(|announcement| announcement.update.base_head_id == genesis_head.head_id)
    );

    let validation = validator
        .validate_candidates_once(&experiment)
        .expect("validate candidates")
        .expect("validation outcome");
    let validator_state = validator.telemetry().snapshot();
    assert_eq!(
        validator_state.node_state,
        crate::NodeRuntimeState::PassiveValidator
    );
    assert_eq!(
        validator_state.slot_states.first(),
        Some(&crate::SlotRuntimeState::Assigned(
            crate::SlotAssignmentState::from_experiment(&experiment)
        ))
    );
    assert_eq!(validation.merged_head.global_step, 1);
    assert_ne!(
        validation.merged_head.head_id,
        trainer_a_window.head.head_id
    );
    assert_ne!(
        validation.merged_head.head_id,
        trainer_b_window.head.head_id
    );
    assert_eq!(
        validation.merged_head.parent_head_id,
        Some(genesis_head.head_id.clone())
    );
    let expected_model = {
        let trainer_a_model = metric_float(&trainer_a_window.report.stats, "model");
        let trainer_b_model = metric_float(&trainer_b_window.report.stats, "model");
        let trainer_a_quality =
            1.0 / (1.0 + metric_float(&trainer_a_window.report.stats, "loss").abs());
        let trainer_b_quality =
            1.0 / (1.0 + metric_float(&trainer_b_window.report.stats, "loss").abs());
        ((trainer_a_model * trainer_a_quality) + (trainer_b_model * trainer_b_quality))
            / (trainer_a_quality + trainer_b_quality)
    };
    let merged_model = metric_float(&validation.merged_head.metrics, "model");
    assert!(
        (merged_model - expected_model).abs() < 1e-9,
        "expected merged model {expected_model}, got {merged_model}"
    );
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_telemetry.snapshot();
            !snapshot.control_plane.aggregate_announcements.is_empty()
                && !snapshot
                    .control_plane
                    .reduction_certificate_announcements
                    .is_empty()
                && !snapshot.control_plane.reducer_load_announcements.is_empty()
        },
        "validator did not publish aggregate and reduction metadata",
    );
    let topology_snapshot = validator_telemetry.snapshot();
    let aggregate = topology_snapshot
        .control_plane
        .aggregate_announcements
        .last()
        .expect("aggregate announcement")
        .aggregate
        .clone();
    assert_eq!(aggregate.base_head_id, genesis_head.head_id);
    assert!(aggregate.stats.accepted_updates >= 1);
    assert_ne!(
        aggregate.aggregate_artifact_id,
        validation.merged_head.artifact_id
    );
    let validator_store = validator.artifact_store().expect("validator store");
    assert!(validator_store.has_manifest(&aggregate.aggregate_artifact_id));

    let late_joiner = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 0.25,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(late_joiner_storage))
    .with_bootstrap_peer(validator_addr)
    .spawn()
    .expect("late joiner spawn");
    let late_joiner_telemetry = late_joiner.telemetry();
    wait_for(
        Duration::from_secs(5),
        || late_joiner_telemetry.snapshot().connected_peers >= 1,
        "late joiner did not connect",
    );

    let late_joiner = late_joiner;
    let synced_head = late_joiner
        .sync_experiment_head(&experiment)
        .expect("sync experiment head")
        .expect("synced head");
    assert_eq!(synced_head.head_id, validation.merged_head.head_id);

    let mut late_joiner = late_joiner;
    let late_window = late_joiner
        .train_window_once(&experiment)
        .expect("late joiner window");
    assert_eq!(
        late_window.head.parent_head_id,
        Some(validation.merged_head.head_id.clone())
    );

    late_joiner.shutdown().expect("late joiner shutdown");
    let _ = late_joiner
        .await_termination()
        .expect("late joiner termination");
    trainer_b.shutdown().expect("trainer b shutdown");
    let _ = trainer_b
        .await_termination()
        .expect("trainer b termination");
    trainer_a.shutdown().expect("trainer a shutdown");
    let _ = trainer_a
        .await_termination()
        .expect("trainer a termination");
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");
}

#[test]
fn training_uses_revision_lag_policy_from_directory_entry() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());

    let validator_storage = std::env::temp_dir().join(format!(
        "burn-p2p-lag-validator-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let trainer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-lag-trainer-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));

    let validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(validator_storage.clone()))
    .spawn()
    .expect("validator spawn");
    let validator_telemetry = validator.telemetry();
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_telemetry.snapshot();
            snapshot.status == crate::RuntimeStatus::Running
                && !snapshot.listen_addresses.is_empty()
        },
        "validator runtime did not start",
    );
    let validator_addr = validator_telemetry.snapshot().listen_addresses[0].clone();

    let experiment = experiment();
    let mut lag_directory_entry = ExperimentDirectoryEntry {
        network_id: mainnet().genesis.network_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        workload_id: crate::WorkloadId::new("synthetic-workload"),
        display_name: "Synthetic".into(),
        model_schema_hash: crate::ContentId::new("model-synthetic"),
        dataset_view_id: crate::DatasetViewId::new("view-synthetic"),
        resource_requirements: ExperimentResourceRequirements {
            minimum_roles: BTreeSet::from([crate::PeerRole::TrainerCpu]),
            minimum_device_memory_bytes: None,
            minimum_system_memory_bytes: Some(1024),
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
    };
    lag_directory_entry.apply_revision_policy(&crate::RevisionManifest {
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        workload_id: lag_directory_entry.workload_id.clone(),
        required_release_train_hash: crate::ContentId::new("train-synthetic"),
        model_schema_hash: lag_directory_entry.model_schema_hash.clone(),
        checkpoint_format_hash: crate::ContentId::new("checkpoint-synthetic"),
        dataset_view_id: lag_directory_entry.dataset_view_id.clone(),
        training_config_hash: crate::ContentId::new("training-synthetic"),
        merge_topology_policy_hash: crate::ContentId::new("topology-synthetic"),
        slot_requirements: lag_directory_entry.resource_requirements.clone(),
        activation_window: crate::WindowActivation {
            activation_window: crate::WindowId(1),
            grace_windows: 0,
        },
        lag_policy: crate::LagPolicy {
            max_head_lag_before_catchup: 1,
            max_head_lag_before_block: 2,
            max_head_lag_before_full_rebase: 8,
            max_window_skew_before_lease_revoke: 1,
        },
        merge_window_miss_policy: crate::MergeWindowMissPolicy::LeaseBlocked,
        robustness_policy: None,
        browser_enabled: false,
        browser_role_policy: crate::BrowserRolePolicy::default(),
        max_browser_checkpoint_bytes: None,
        max_browser_window_secs: None,
        max_browser_shard_bytes: None,
        requires_webgpu: false,
        max_browser_batch_size: None,
        recommended_browser_precision: None,
        visibility_policy: crate::BrowserVisibilityPolicy::Hidden,
        description: "synthetic lag policy".into(),
    });

    let mut validator = validator;
    let genesis_head = validator
        .initialize_local_head(&experiment)
        .expect("init validator genesis head");
    let lagged_head = HeadDescriptor {
        head_id: crate::HeadId::new("validator-lagged-head"),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        artifact_id: genesis_head.artifact_id.clone(),
        parent_head_id: Some(genesis_head.head_id.clone()),
        global_step: 3,
        created_at: Utc::now(),
        metrics: genesis_head.metrics.clone(),
    };
    super::runtime_support::persist_head_state(
        &StorageConfig::new(validator_storage.clone()),
        &experiment,
        &lagged_head,
    )
    .expect("persist lagged head state");
    super::persist_json(
        StorageConfig::new(validator_storage.clone()).scoped_head_path(&lagged_head.head_id),
        &lagged_head,
    )
    .expect("persist lagged head descriptor");
    let restored_lagged_head = validator
        .restore_experiment_head(&experiment)
        .expect("restore lagged head")
        .expect("restored lagged head");
    assert_eq!(restored_lagged_head.global_step, 3);

    let trainer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(trainer_storage))
    .with_auth(crate::AuthConfig::new().with_experiment_directory(vec![lag_directory_entry]))
    .with_bootstrap_peer(validator_addr)
    .spawn()
    .expect("trainer spawn");
    let trainer_telemetry = trainer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || trainer_telemetry.snapshot().connected_peers >= 1,
        "trainer did not connect",
    );

    let mut trainer = trainer;
    let error = trainer
        .train_window_once(&experiment)
        .expect_err("training should block on excessive lag");
    assert!(
        error
            .to_string()
            .contains("training blocked: local node is 3 head steps behind")
    );

    let snapshot = trainer_telemetry.snapshot();
    assert_eq!(snapshot.lag_state, crate::LagState::LeaseBlocked);
    assert_eq!(snapshot.head_lag_steps, 3);
    assert_eq!(
        snapshot.lag_policy,
        crate::LagPolicy {
            max_head_lag_before_catchup: 1,
            max_head_lag_before_block: 2,
            max_head_lag_before_full_rebase: 8,
            max_window_skew_before_lease_revoke: 1,
        }
    );
    match snapshot
        .slot_states
        .first()
        .expect("slot state should exist")
    {
        crate::SlotRuntimeState::Blocked { assignment, reason } => {
            assert_eq!(
                assignment.as_ref(),
                Some(&crate::SlotAssignmentState::from_experiment(&experiment))
            );
            assert!(reason.contains("3 head steps behind"));
        }
        other => panic!("expected blocked slot state, got {other:?}"),
    }

    trainer.shutdown().expect("trainer shutdown");
    let _ = trainer.await_termination().expect("trainer termination");
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");
}

#[test]
fn validator_restart_restores_canonical_head_for_late_joiners() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());

    let validator_storage = std::env::temp_dir().join(format!(
        "burn-p2p-validator-restart-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let trainer_storage = std::env::temp_dir().join(format!(
        "burn-p2p-trainer-restart-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));
    let late_joiner_storage = std::env::temp_dir().join(format!(
        "burn-p2p-late-joiner-restart-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ));

    let experiment = experiment();
    let validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
    .with_storage(StorageConfig::new(validator_storage.clone()))
    .spawn()
    .expect("validator spawn");
    let validator_telemetry = validator.telemetry();
    wait_for(
        Duration::from_secs(5),
        || !validator_telemetry.snapshot().listen_addresses.is_empty(),
        "validator did not start listening",
    );
    let validator_addr = validator_telemetry.snapshot().listen_addresses[0].clone();

    let mut validator = validator;
    validator
        .initialize_local_head(&experiment)
        .expect("init genesis");

    let trainer = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(trainer_storage))
    .with_bootstrap_peer(validator_addr)
    .spawn()
    .expect("trainer spawn");
    let trainer_telemetry = trainer.telemetry();
    wait_for(
        Duration::from_secs(5),
        || trainer_telemetry.snapshot().connected_peers >= 1,
        "trainer did not connect",
    );
    let mut trainer = trainer;
    let trained = trainer
        .train_window_once(&experiment)
        .expect("trainer window");
    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_telemetry.snapshot();
            snapshot
                .control_plane
                .head_announcements
                .iter()
                .any(|announcement| announcement.head.head_id == trained.head.head_id)
                && snapshot
                    .control_plane
                    .update_announcements
                    .iter()
                    .any(|announcement| {
                        announcement.update.delta_artifact_id == trained.artifact.artifact_id
                    })
        },
        "validator did not observe trainer head/update before validation",
    );
    let validated = validator
        .validate_candidates_once(&experiment)
        .expect("validate")
        .expect("validation outcome");
    assert_ne!(validated.merged_head.head_id, trained.head.head_id);
    assert_eq!(
        validated.merged_head.parent_head_id,
        trained.head.parent_head_id
    );
    assert!(
        (metric_float(&validated.merged_head.metrics, "model")
            - metric_float(&trained.report.stats, "model"))
        .abs()
            < 1e-9
    );

    trainer.shutdown().expect("trainer shutdown");
    let _ = trainer.await_termination().expect("trainer termination");
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");

    let restarted_validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_identity(crate::IdentityConfig::Persistent)
    .with_storage(StorageConfig::new(validator_storage))
    .spawn()
    .expect("restart validator");
    let restarted_telemetry = restarted_validator.telemetry();
    wait_for(
        Duration::from_secs(5),
        || !restarted_telemetry.snapshot().listen_addresses.is_empty(),
        "restarted validator did not start listening",
    );
    let restarted_addr = restarted_telemetry.snapshot().listen_addresses[0].clone();
    let restarted_peer_id = restarted_telemetry
        .snapshot()
        .local_peer_id
        .clone()
        .expect("restarted peer id");
    assert_eq!(
        restarted_peer_id,
        validator_telemetry
            .snapshot()
            .local_peer_id
            .expect("original validator peer id")
    );

    restarted_validator
        .restore_experiment_head(&experiment)
        .expect("restore canonical head")
        .expect("restored head");

    let late_joiner = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 0.25,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(late_joiner_storage))
    .with_bootstrap_peer(restarted_addr)
    .spawn()
    .expect("late joiner spawn");
    let late_joiner_telemetry = late_joiner.telemetry();
    wait_for(
        Duration::from_secs(5),
        || late_joiner_telemetry.snapshot().connected_peers >= 1,
        "late joiner did not connect to restarted validator",
    );

    let synced = {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(head) = late_joiner
                .sync_experiment_head(&experiment)
                .expect("sync head after restart")
            {
                break head;
            }
            assert!(Instant::now() < deadline, "synced head after restart");
            thread::sleep(Duration::from_millis(25));
        }
    };
    assert_eq!(synced.head_id, validated.merged_head.head_id);

    late_joiner.shutdown().expect("late joiner shutdown");
    let _ = late_joiner
        .await_termination()
        .expect("late joiner termination");
    restarted_validator
        .shutdown()
        .expect("restarted validator shutdown");
    let _ = restarted_validator
        .await_termination()
        .expect("restarted validator termination");
}

#[test]
fn validator_can_fan_in_many_native_trainers_in_one_round() {
    let dataset_dir = tempdir().expect("dataset dir");
    create_runtime_dataset(dataset_dir.path());
    let experiment = experiment();

    let validator = NodeBuilder::new(SyntheticRuntimeProject {
        dataset_root: dataset_dir.path().to_path_buf(),
        learning_rate: 1.0,
        target_model: 10.0,
    })
    .with_mainnet(mainnet().genesis.clone())
    .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
        "burn-p2p-fanin-validator-{}",
        Utc::now().timestamp_nanos_opt().expect("nanos")
    ))))
    .spawn()
    .expect("validator spawn");
    let validator_telemetry = validator.telemetry();
    wait_for(
        Duration::from_secs(5),
        || !validator_telemetry.snapshot().listen_addresses.is_empty(),
        "validator did not start listening",
    );
    let validator_addr = validator_telemetry.snapshot().listen_addresses[0].clone();

    let mut validator = validator;
    let genesis_head = validator
        .initialize_local_head(&experiment)
        .expect("init genesis");

    let learning_rates = [0.2, 0.4, 0.6, 0.8, 1.0, 0.3];
    let mut trainers = Vec::new();
    for (index, learning_rate) in learning_rates.iter().enumerate() {
        let trainer = NodeBuilder::new(SyntheticRuntimeProject {
            dataset_root: dataset_dir.path().to_path_buf(),
            learning_rate: *learning_rate,
            target_model: 10.0,
        })
        .with_mainnet(mainnet().genesis.clone())
        .with_storage(StorageConfig::new(std::env::temp_dir().join(format!(
            "burn-p2p-fanin-trainer-{index}-{}",
            Utc::now().timestamp_nanos_opt().expect("nanos")
        ))))
        .with_bootstrap_peer(validator_addr.clone())
        .spawn()
        .expect("trainer spawn");
        trainers.push(trainer);
    }

    wait_for(
        Duration::from_secs(5),
        || validator_telemetry.snapshot().connected_peers >= learning_rates.len(),
        "validator did not connect to all trainers",
    );
    for trainer in &trainers {
        wait_for(
            Duration::from_secs(5),
            || {
                trainer
                    .sync_experiment_head(&experiment)
                    .expect("sync trainer head")
                    .is_some()
            },
            "trainer did not sync the canonical genesis head",
        );
    }

    let mut trainer_outcomes = Vec::new();
    for trainer in &mut trainers {
        let outcome = trainer
            .train_window_once(&experiment)
            .expect("trainer window");
        trainer_outcomes.push(outcome);
    }
    assert_ne!(
        trainer_outcomes[0].lease.microshards, trainer_outcomes[1].lease.microshards,
        "same-window trainers should avoid already-announced shard assignments when alternatives exist",
    );

    wait_for(
        Duration::from_secs(5),
        || {
            let snapshot = validator_telemetry.snapshot();
            snapshot.control_plane.update_announcements.len() >= learning_rates.len()
                && !snapshot.control_plane.merge_window_announcements.is_empty()
                && snapshot
                    .control_plane
                    .reducer_assignment_announcements
                    .len()
                    >= learning_rates.len()
        },
        "validator did not observe all trainer updates for the merge window",
    );
    wait_for(
        Duration::from_secs(5),
        || {
            validator_telemetry
                .snapshot()
                .control_plane
                .head_announcements
                .len()
                > learning_rates.len()
        },
        "validator did not observe all trainer head announcements for the merge window",
    );

    let validated = validator
        .validate_candidates_once(&experiment)
        .expect("validate")
        .expect("validation outcome");
    assert_eq!(
        validated.merged_head.parent_head_id,
        Some(genesis_head.head_id.clone())
    );
    assert!(
        trainer_outcomes
            .iter()
            .all(|outcome| outcome.head.head_id != validated.merged_head.head_id)
    );
    let expected_model = {
        let (weighted_sum, total_weight) = trainer_outcomes.iter().fold(
            (0.0_f64, 0.0_f64),
            |(weighted_sum, total_weight), outcome| {
                let model = metric_float(&outcome.report.stats, "model");
                let quality = 1.0 / (1.0 + metric_float(&outcome.report.stats, "loss").abs());
                (weighted_sum + (model * quality), total_weight + quality)
            },
        );
        weighted_sum / total_weight
    };
    let merged_model = metric_float(&validated.merged_head.metrics, "model");
    assert!(
        (merged_model - expected_model).abs() < 1e-9,
        "expected merged model {expected_model}, got {merged_model}"
    );

    for trainer in trainers {
        trainer.shutdown().expect("trainer shutdown");
        let _ = trainer.await_termination().expect("trainer termination");
    }
    validator.shutdown().expect("validator shutdown");
    let _ = validator
        .await_termination()
        .expect("validator termination");
}
