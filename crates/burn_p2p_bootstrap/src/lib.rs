//! Bootstrap, edge, and operator-facing services for `burn_p2p` deployments.
//!
//! This crate is the main composition root for deployment-facing features. It layers
//! optional services such as:
//!
//! - admin and diagnostics HTTP routes
//! - portal rendering and browser-edge snapshots
//! - optional auth providers and enrollment flows
//! - optional social snapshots and profile surfaces
//! - embedded-runtime daemon wiring
//!
//! Core runtime correctness still lives in `burn_p2p` and the lower-level crates. This
//! crate focuses on deployment assembly, capability advertisement, and operator-facing
//! workflows.
#![forbid(unsafe_code)]

mod admin;
mod app_render;
mod browser_edge;
mod daemon;
mod deploy;
mod history;
#[cfg(feature = "metrics-indexer")]
mod metrics;
mod operator_store;
#[cfg(feature = "artifact-publish")]
mod publication;
mod state;

#[cfg(feature = "artifact-publish")]
use burn_p2p_publish::DEFAULT_PUBLICATION_TARGET_ID;
use burn_p2p_swarm::SwarmError;
use thiserror::Error;

pub use crate::admin::{AdminAction, AdminResult, AuthPolicyRollout};
pub use crate::app_render::{
    render_artifact_run_summaries_html, render_artifact_run_view_html, render_dashboard_html,
    render_head_artifact_view_html,
};
pub use crate::browser_edge::{BrowserEdgeSnapshot, BrowserEdgeSnapshotConfig};
pub use crate::daemon::{
    ActiveExperiment, BootstrapEmbeddedDaemon, BootstrapEmbeddedDaemonConfig, BootstrapPeerDaemon,
    BootstrapPeerDaemonConfig,
};
pub use crate::deploy::{
    AdminApiPlan, AdminCapability, ArchivePlan, AuthorityPlan, BootstrapPlan, BootstrapPreset,
    BootstrapService, BootstrapSpec, TelemetryExportPlan,
};
pub use crate::history::StoredEvalProtocolManifestRecord;
#[doc(hidden)]
pub use crate::operator_store::OperatorStateBackendConfig;
pub use crate::state::{
    BootstrapAdminState, BootstrapDiagnostics, BootstrapDiagnosticsBundle, BootstrapPeerDiagnostic,
    HeadQuery, OperatorAuditFacetSummary, OperatorAuditKind, OperatorAuditQuery,
    OperatorAuditRecord, OperatorAuditSummary, OperatorFacetBucket, OperatorReplayQuery,
    OperatorReplaySnapshot, OperatorReplaySnapshotSummary, OperatorRetentionPruneResult,
    OperatorRetentionSummary, ReceiptQuery, ReducerLoadQuery, render_openmetrics,
};
pub use burn_p2p::{NodeRuntimeState, SlotRuntimeState};
pub use burn_p2p_core::{
    BrowserDirectorySnapshot, BrowserEdgeMode, BrowserEdgePaths, BrowserLeaderboardEntry,
    BrowserLeaderboardIdentity, BrowserLeaderboardSnapshot, BrowserLoginProvider,
    BrowserReceiptSubmissionResponse, BrowserTransportSurface, ReenrollmentStatus,
    TrustBundleExport, TrustedIssuerStatus,
};
#[cfg(feature = "artifact-publish")]
pub use burn_p2p_publish::{
    ArtifactAliasStatus, ArtifactRunSummary, ArtifactRunView, DownloadArtifact,
    DownloadTicketResponse, ExportRequest, HeadArtifactView,
};

pub(crate) use crate::history::load_operator_history;

#[cfg(feature = "social")]
mod social_services {
    use std::collections::BTreeMap;

    use burn_p2p_core::{
        ContributionReceipt, LeaderboardSnapshot, MergeCertificate, NetworkId, PeerId, PrincipalId,
    };
    use burn_p2p_social::{
        LeaderboardComputationInput, LeaderboardService, ReceiptLeaderboardService,
    };
    use chrono::{DateTime, Utc};

    pub(crate) fn leaderboard_snapshot(
        network_id: &NetworkId,
        receipts: &[ContributionReceipt],
        merge_certificates: &[MergeCertificate],
        peer_principals: &BTreeMap<PeerId, PrincipalId>,
        captured_at: DateTime<Utc>,
    ) -> LeaderboardSnapshot {
        ReceiptLeaderboardService::default().snapshot(&LeaderboardComputationInput {
            network_id,
            receipts,
            merge_certificates,
            peer_principals,
            captured_at,
        })
    }
}

#[cfg(not(feature = "social"))]
mod social_services {
    use std::collections::BTreeMap;

    use burn_p2p_core::{
        ContributionReceipt, LeaderboardSnapshot, MergeCertificate, NetworkId, PeerId, PrincipalId,
    };
    use chrono::{DateTime, Utc};

    pub(crate) fn leaderboard_snapshot(
        network_id: &NetworkId,
        _receipts: &[ContributionReceipt],
        _merge_certificates: &[MergeCertificate],
        _peer_principals: &BTreeMap<PeerId, PrincipalId>,
        captured_at: DateTime<Utc>,
    ) -> LeaderboardSnapshot {
        LeaderboardSnapshot {
            network_id: network_id.clone(),
            score_version: "leaderboard_score_v1".into(),
            entries: Vec::new(),
            captured_at,
        }
    }
}

#[derive(Debug, Error)]
/// Enumerates the supported bootstrap error values.
pub enum BootstrapError {
    #[error("swarm configuration error: {0}")]
    /// Uses the swarm variant.
    Swarm(#[from] SwarmError),
    #[error("schema error: {0}")]
    /// Uses the schema variant.
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("authority-capable presets require authority and validator policy")]
    /// Uses the missing authority policy variant.
    MissingAuthorityPolicy,
    #[error("this bootstrap plan does not include authority service")]
    /// Uses the authority service required variant.
    AuthorityServiceRequired,
    #[error("admin action `{0:?}` is not enabled for this bootstrap plan")]
    /// Uses the unsupported admin action variant.
    UnsupportedAdminAction(AdminCapability),
    #[error("this admin action requires a signer")]
    /// Uses the missing signer variant.
    MissingSigner,
    #[error("invalid bootstrap configuration: {0}")]
    /// Uses the invalid configuration variant.
    InvalidConfig(String),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        net::TcpListener,
        path::{Path, PathBuf},
        thread,
        time::{Duration, Instant},
    };

    #[cfg(feature = "metrics-indexer")]
    use burn_p2p::DatasetViewId;
    use burn_p2p::{
        ArtifactBuildSpec, ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard,
        CapabilityEstimate, ChunkingScheme, ControlPlaneSnapshot, DatasetConfig,
        DatasetRegistration, DatasetSizing, EvalSplit, FsArtifactStore, HeadDescriptor,
        LiveControlPlaneEvent, MetricReport, MetricValue, NodeBuilder, NodeConfig,
        NodeTelemetrySnapshot, P2pWorkload, PatchOutcome, PatchSupport, ReducerLoadAnnouncement,
        RuntimePatch, RuntimeStatus, ShardFetchManifest, SlotAssignmentState, StorageConfig,
        TrainError, WindowCtx, WindowReport, WorkloadId,
    };
    #[cfg(feature = "social")]
    use burn_p2p_core::BadgeKind;
    use burn_p2p_core::{
        ArtifactId, AttestationLevel, AuthorityEpochManifest, BackendClass, CapabilityCard,
        CapabilityCardId, CapabilityClass, ClientPlatform, ContentId, ContributionReceipt,
        ExperimentId, HeadEvalReport, HeadEvalStatus, HeadId, MergeCertificate, MetricTrustClass,
        NetworkId, PeerId, PeerWindowMetrics, PeerWindowStatus, PersistenceClass, PrincipalId,
        ReducerCohortMetrics, ReducerCohortStatus, RevisionId, StudyId, TelemetrySummary,
        ValidatorSetManifest, ValidatorSetMember, WindowActivation, WindowId,
    };
    use burn_p2p_experiment::{
        ActivationTarget, ExperimentControlCommand, ExperimentLifecyclePhase,
        ExperimentLifecyclePlan,
    };
    use burn_p2p_security::{
        MergeEvidenceRequirement, PeerAdmissionReport, PeerTrustLevel, ReleasePolicy,
        ReputationDecision, ReputationEngine, ReputationState, ValidatorPolicy,
    };
    use burn_p2p_swarm::PeerObservation;
    use chrono::Utc;
    use semver::{Version, VersionReq};
    use tempfile::TempDir;

    #[cfg(feature = "browser-edge")]
    use super::render_dashboard_html;
    use super::{
        ActiveExperiment, AdminAction, AdminResult, BootstrapAdminState,
        BootstrapEmbeddedDaemonConfig, BootstrapPeerDaemonConfig, BootstrapPlan, BootstrapPreset,
        BootstrapService, BootstrapSpec, HeadQuery, ReceiptQuery, ReducerLoadQuery,
        render_openmetrics,
    };

    fn genesis() -> burn_p2p_core::GenesisSpec {
        burn_p2p_core::GenesisSpec {
            network_id: burn_p2p_core::NetworkId::new("mainnet"),
            protocol_version: Version::new(0, 1, 0),
            display_name: "Mainnet".into(),
            created_at: Utc::now(),
            metadata: BTreeMap::new(),
        }
    }

    fn loopback_listen_address() -> burn_p2p_swarm::SwarmAddress {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let port = listener.local_addr().expect("listener addr").port();
        drop(listener);
        burn_p2p_swarm::SwarmAddress::new(format!("/ip4/127.0.0.1/tcp/{port}"))
            .expect("listen address")
    }

    fn authority_plan() -> super::AuthorityPlan {
        let release_policy = ReleasePolicy::new(
            Version::new(0, 1, 0),
            vec![VersionReq::parse("^0.1").expect("version req")],
        )
        .expect("release policy");
        let validator_policy = ValidatorPolicy {
            release_policy: release_policy.clone(),
            evidence_requirement: MergeEvidenceRequirement::default(),
        };

        super::AuthorityPlan {
            release_policy,
            validator_policy,
            validator_set_manifest: None,
            authority_epoch_manifest: None,
        }
    }

    fn plan(preset: BootstrapPreset) -> BootstrapPlan {
        BootstrapSpec {
            preset,
            genesis: genesis(),
            platform: ClientPlatform::Native,
            bootstrap_addresses: vec![
                burn_p2p_swarm::SwarmAddress::new("/dns4/bootstrap.example.com/tcp/4001/ws")
                    .expect("addr"),
            ],
            listen_addresses: vec![
                burn_p2p_swarm::SwarmAddress::new("/ip4/0.0.0.0/udp/4001/quic-v1").expect("addr"),
            ],
            authority: Some(authority_plan()),
            archive: super::ArchivePlan {
                pinned_heads: BTreeSet::from([HeadId::new("head")]),
                pinned_artifacts: BTreeSet::from([ArtifactId::new("artifact")]),
                retain_contribution_receipts: true,
            },
            admin_api: super::AdminApiPlan::default(),
        }
        .plan()
        .expect("plan")
    }

    fn validator_set_manifest() -> ValidatorSetManifest {
        ValidatorSetManifest {
            network_id: NetworkId::new("mainnet"),
            validator_set_id: ContentId::new("validator-set-mainnet"),
            authority_epoch: 7,
            quorum_weight: 2,
            members: vec![
                ValidatorSetMember {
                    peer_id: PeerId::new("validator-a"),
                    principal_id: Some(PrincipalId::new("principal-a")),
                    roles: burn_p2p_core::PeerRoleSet::new([burn_p2p_core::PeerRole::Validator]),
                    vote_weight: 1,
                    attestation_level: AttestationLevel::Challenge,
                    metadata: BTreeMap::new(),
                },
                ValidatorSetMember {
                    peer_id: PeerId::new("validator-b"),
                    principal_id: Some(PrincipalId::new("principal-b")),
                    roles: burn_p2p_core::PeerRoleSet::new([burn_p2p_core::PeerRole::Validator]),
                    vote_weight: 1,
                    attestation_level: AttestationLevel::Challenge,
                    metadata: BTreeMap::new(),
                },
            ],
            created_at: Utc::now(),
            metadata: BTreeMap::new(),
        }
    }

    #[derive(Clone, Debug)]
    struct SyntheticProject {
        dataset_root: PathBuf,
        learning_rate: f64,
        target_model: f64,
    }

    impl P2pWorkload for SyntheticProject {
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
        ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
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

        fn apply_patch(&mut self, patch: &RuntimePatch) -> PatchOutcome {
            if let Some(burn_p2p::PatchValue::Float(value)) = patch.values.get("learning_rate") {
                self.learning_rate = *value;
                PatchOutcome::Applied
            } else {
                PatchOutcome::Rejected("missing learning_rate".into())
            }
        }

        fn supported_patch_classes(&self) -> PatchSupport {
            PatchSupport {
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
                manifest: burn_p2p::DatasetManifest {
                    dataset_id: burn_p2p::DatasetId::new("bootstrap-dataset"),
                    source_uri: self.dataset_root.display().to_string(),
                    format: "microshards".into(),
                    manifest_hash: ContentId::new("bootstrap-manifest"),
                    metadata: BTreeMap::new(),
                },
                view: burn_p2p::DatasetView {
                    dataset_view_id: burn_p2p::DatasetViewId::new("bootstrap-dataset-view"),
                    dataset_id: burn_p2p::DatasetId::new("bootstrap-dataset"),
                    preprocessing_hash: ContentId::new("bootstrap-preprocess"),
                    tokenizer_hash: None,
                    manifest_hash: ContentId::new("bootstrap-manifest"),
                    metadata: BTreeMap::new(),
                },
                upstream: burn_p2p::UpstreamAdapter::Local {
                    root: self.dataset_root.display().to_string(),
                },
            })
        }

        fn microshard_plan(
            &self,
            registration: &DatasetRegistration,
        ) -> anyhow::Result<burn_p2p::MicroShardPlan> {
            Ok(
                burn_p2p::MicroShardPlanner::new(burn_p2p::MicroShardPlannerConfig {
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
                )?,
            )
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
            head_id: burn_p2p::HeadId,
            base_head_id: Option<burn_p2p::HeadId>,
            store: &FsArtifactStore,
        ) -> anyhow::Result<ArtifactDescriptor> {
            let mut spec = ArtifactBuildSpec::new(
                artifact_kind,
                burn_p2p::Precision::Fp32,
                ContentId::new("bootstrap-synthetic-schema"),
                "synthetic-json",
            )
            .with_head(head_id);
            if let Some(base_head_id) = base_head_id {
                spec = spec.with_base_head(base_head_id);
            }
            let bytes = serde_json::to_vec(model)?;
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
            candidates: &[burn_p2p::MergeModelCandidate<'_, Self::Model>],
            policy: burn_p2p::MergePolicy,
        ) -> anyhow::Result<Option<Self::Model>> {
            if candidates.is_empty() {
                return Ok(None);
            }
            let (weighted_sum, total_weight) = candidates.iter().fold(
                (0.0_f64, 0.0_f64),
                |(weighted_sum, total_weight), candidate| {
                    let quality = match policy {
                        burn_p2p::MergePolicy::WeightedMean => 1.0,
                        burn_p2p::MergePolicy::NormClippedWeightedMean => {
                            candidate.quality_weight.clamp(0.0, 1.0)
                        }
                        burn_p2p::MergePolicy::TrimmedMean => candidate.quality_weight,
                        burn_p2p::MergePolicy::Ema | burn_p2p::MergePolicy::QualityWeightedEma => {
                            candidate.quality_weight
                        }
                        burn_p2p::MergePolicy::Custom(_) => candidate.quality_weight,
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

        fn supported_workload(&self) -> burn_p2p::SupportedWorkload {
            burn_p2p::SupportedWorkload {
                workload_id: WorkloadId::new("bootstrap-synthetic"),
                workload_name: "Bootstrap Synthetic".into(),
                model_program_hash: ContentId::new("bootstrap-synthetic-program"),
                checkpoint_format_hash: ContentId::new("synthetic-json"),
                supported_revision_family: ContentId::new("bootstrap-synthetic-revision-family"),
                resource_class: "cpu".into(),
            }
        }

        fn model_schema_hash(&self) -> ContentId {
            ContentId::new("bootstrap-synthetic-schema")
        }
    }

    fn create_dataset(root: &Path) {
        fs::create_dir_all(root).expect("create dataset root");
        let project = SyntheticProject {
            dataset_root: root.to_path_buf(),
            learning_rate: 1.0,
            target_model: 10.0,
        };
        let registration = project
            .dataset_registration()
            .expect("dataset registration");
        let plan = project
            .microshard_plan(&registration)
            .expect("microshard plan");
        let manifest = ShardFetchManifest::from_microshards(
            &plan.dataset_view,
            &plan.microshards,
            |ordinal| match ordinal {
                0 => b"3.5".to_vec(),
                _ => b"6.5".to_vec(),
            },
        );
        fs::write(
            root.join("fetch-manifest.json"),
            serde_json::to_vec_pretty(&manifest).expect("manifest"),
        )
        .expect("write manifest");
        for entry in &manifest.entries {
            let bytes = match entry.ordinal {
                0 => b"3.5".to_vec(),
                _ => b"6.5".to_vec(),
            };
            fs::write(root.join(&entry.locator), bytes).expect("write shard");
        }
    }

    fn wait_for(timeout: Duration, predicate: impl Fn() -> bool, message: &str) {
        let started = Instant::now();
        while started.elapsed() < timeout {
            if predicate() {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("{message}");
    }

    #[test]
    fn preset_derives_expected_services_and_roles() {
        let plan = plan(BootstrapPreset::AllInOne);

        assert!(plan.supports_service(&BootstrapService::CoherenceSeed));
        assert!(plan.supports_service(&BootstrapService::Authority));
        assert!(plan.supports_service(&BootstrapService::Archive));
        assert!(plan.roles.contains(&burn_p2p_core::PeerRole::Bootstrap));
        assert!(plan.roles.contains(&burn_p2p_core::PeerRole::Authority));
    }

    #[test]
    fn reducer_preset_derives_expected_services_and_roles() {
        let plan = plan(BootstrapPreset::ReducerOnly);

        assert!(plan.supports_service(&BootstrapService::Reducer));
        assert!(plan.supports_service(&BootstrapService::AdminApi));
        assert!(plan.roles.contains(&burn_p2p_core::PeerRole::Reducer));
        assert!(!plan.roles.contains(&burn_p2p_core::PeerRole::Validator));
        assert!(!plan.roles.contains(&burn_p2p_core::PeerRole::Authority));
    }

    #[test]
    fn authority_presets_require_authority_policy() {
        let error = BootstrapSpec {
            preset: BootstrapPreset::AuthorityValidator,
            genesis: genesis(),
            platform: ClientPlatform::Native,
            bootstrap_addresses: vec![],
            listen_addresses: vec![],
            authority: None,
            archive: super::ArchivePlan::default(),
            admin_api: super::AdminApiPlan::default(),
        }
        .plan()
        .expect_err("missing policy");

        assert!(matches!(
            error,
            super::BootstrapError::MissingAuthorityPolicy
        ));
    }

    #[test]
    fn authority_plan_rejects_invalid_validator_set_manifest() {
        let mut invalid_set = validator_set_manifest();
        invalid_set.quorum_weight = 3;

        let error = BootstrapSpec {
            preset: BootstrapPreset::AuthorityValidator,
            genesis: genesis(),
            platform: ClientPlatform::Native,
            bootstrap_addresses: vec![],
            listen_addresses: vec![],
            authority: Some(super::AuthorityPlan {
                release_policy: authority_plan().release_policy,
                validator_policy: authority_plan().validator_policy,
                validator_set_manifest: Some(invalid_set),
                authority_epoch_manifest: None,
            }),
            archive: super::ArchivePlan::default(),
            admin_api: super::AdminApiPlan::default(),
        }
        .plan()
        .expect_err("invalid validator quorum");

        assert!(matches!(error, super::BootstrapError::InvalidConfig(_)));
    }

    #[test]
    fn authority_plan_rejects_mismatched_epoch_manifest() {
        let validator_set = validator_set_manifest();
        let error = BootstrapSpec {
            preset: BootstrapPreset::AuthorityValidator,
            genesis: genesis(),
            platform: ClientPlatform::Native,
            bootstrap_addresses: vec![],
            listen_addresses: vec![],
            authority: Some(super::AuthorityPlan {
                release_policy: authority_plan().release_policy,
                validator_policy: authority_plan().validator_policy,
                validator_set_manifest: Some(validator_set.clone()),
                authority_epoch_manifest: Some(AuthorityEpochManifest {
                    network_id: NetworkId::new("mainnet"),
                    authority_epoch: validator_set.authority_epoch,
                    validator_set_id: ContentId::new("other-validator-set"),
                    minimum_revocation_epoch: burn_p2p_core::RevocationEpoch(0),
                    supersedes_authority_epoch: Some(6),
                    created_at: Utc::now(),
                    metadata: BTreeMap::new(),
                }),
            }),
            archive: super::ArchivePlan::default(),
            admin_api: super::AdminApiPlan::default(),
        }
        .plan()
        .expect_err("mismatched validator set reference");

        assert!(matches!(error, super::BootstrapError::InvalidConfig(_)));
    }

    #[test]
    fn control_action_issues_signed_control_certificate() {
        let plan = plan(BootstrapPreset::AuthorityValidator);
        let mut state = BootstrapAdminState::default();

        let result = plan
            .execute_admin_action(
                AdminAction::Control(ExperimentControlCommand::PauseExperiment {
                    study_id: StudyId::new("study"),
                    experiment_id: ExperimentId::new("exp"),
                    reason: Some("maintenance".into()),
                    target: ActivationTarget {
                        activation: WindowActivation {
                            activation_window: WindowId(5),
                            grace_windows: 1,
                        },
                        required_client_capabilities: BTreeSet::from(["cuda".into()]),
                    },
                }),
                &mut state,
                Some(burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: Utc::now(),
                    signature_hex: "abcd".into(),
                }),
                Utc::now(),
                None,
            )
            .expect("result");

        match result {
            AdminResult::Control(cert) => {
                assert_eq!(cert.network_id.as_str(), "mainnet");
                assert_eq!(cert.activation.activation_window, WindowId(5));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn lifecycle_action_issues_signed_lifecycle_certificate() {
        let plan = plan(BootstrapPreset::AuthorityValidator);
        let mut state = BootstrapAdminState::default();

        let result = plan
            .execute_admin_action(
                AdminAction::Lifecycle(Box::new(ExperimentLifecyclePlan {
                    study_id: StudyId::new("study"),
                    experiment_id: ExperimentId::new("exp"),
                    base_revision_id: Some(RevisionId::new("rev-a")),
                    target_entry: burn_p2p::ExperimentDirectoryEntry {
                        network_id: NetworkId::new("mainnet"),
                        study_id: StudyId::new("study"),
                        experiment_id: ExperimentId::new("exp"),
                        workload_id: WorkloadId::new("compiled"),
                        display_name: "exp rev-b".into(),
                        model_schema_hash: ContentId::new("schema"),
                        dataset_view_id: burn_p2p::DatasetViewId::new("view"),
                        resource_requirements: burn_p2p::ExperimentResourceRequirements {
                            minimum_roles: BTreeSet::from([burn_p2p_core::PeerRole::TrainerCpu]),
                            minimum_device_memory_bytes: None,
                            minimum_system_memory_bytes: None,
                            estimated_download_bytes: 0,
                            estimated_window_seconds: 60,
                        },
                        visibility: burn_p2p::ExperimentVisibility::Public,
                        opt_in_policy: burn_p2p::ExperimentOptInPolicy::Open,
                        current_revision_id: RevisionId::new("rev-b"),
                        current_head_id: None,
                        allowed_roles: burn_p2p_core::PeerRoleSet::default_trainer(),
                        allowed_scopes: BTreeSet::new(),
                        metadata: BTreeMap::new(),
                    },
                    phase: ExperimentLifecyclePhase::Activating,
                    target: ActivationTarget {
                        activation: WindowActivation {
                            activation_window: WindowId(7),
                            grace_windows: 0,
                        },
                        required_client_capabilities: BTreeSet::from(["cuda".into()]),
                    },
                    plan_epoch: 3,
                    reason: Some("roll rev-b".into()),
                })),
                &mut state,
                Some(burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: Utc::now(),
                    signature_hex: "abcd".into(),
                }),
                Utc::now(),
                None,
            )
            .expect("result");

        match result {
            AdminResult::Lifecycle(cert) => {
                assert_eq!(cert.network_id.as_str(), "mainnet");
                assert_eq!(cert.activation.activation_window, WindowId(7));
                assert_eq!(cert.body.payload.payload.plan.plan_epoch, 3);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn diagnostics_export_composes_peer_store_and_archive_state() {
        let plan = plan(BootstrapPreset::BootstrapArchive);
        let now = Utc::now();
        let mut state = BootstrapAdminState::default();
        let peer_id = PeerId::new("peer");

        state.peer_store.upsert(
            PeerObservation::new(peer_id.clone(), now)
                .with_capability_card(CapabilityCard {
                    card_id: CapabilityCardId::new("card"),
                    peer_id: peer_id.clone(),
                    platform: ClientPlatform::Native,
                    roles: burn_p2p_core::PeerRoleSet::default_trainer(),
                    preferred_backends: vec!["cuda".into()],
                    browser_capabilities: BTreeSet::new(),
                    recommended_classes: BTreeSet::from([CapabilityClass::TrainerGpu]),
                    device_memory_bytes: Some(8 * 1024 * 1024 * 1024),
                    system_memory_bytes: 32 * 1024 * 1024 * 1024,
                    disk_bytes: 256 * 1024 * 1024 * 1024,
                    upload_mbps: 500.0,
                    download_mbps: 500.0,
                    persistence: PersistenceClass::Durable,
                    work_units_per_second: 20.0,
                    attestation_level: burn_p2p_core::AttestationLevel::Manifest,
                    benchmark_hash: None,
                    reported_at: now,
                })
                .with_telemetry(TelemetrySummary {
                    network_id: burn_p2p_core::NetworkId::new("mainnet"),
                    study_id: StudyId::new("study"),
                    experiment_id: ExperimentId::new("exp"),
                    revision_id: RevisionId::new("rev"),
                    window_id: WindowId(3),
                    active_peers: 1,
                    accepted_contributions: 2,
                    throughput_work_units_per_second: 18.0,
                    network: burn_p2p_core::NetworkEstimate {
                        connected_peers: 1,
                        observed_peers: 1,
                        estimated_network_size: 1.0,
                        estimated_total_vram_bytes: None,
                        estimated_total_flops: None,
                        eta_lower_seconds: None,
                        eta_upper_seconds: None,
                    },
                    metrics: BTreeMap::from([("estimated_flops".into(), MetricValue::Float(42.0))]),
                    captured_at: now,
                }),
        );
        state.quarantined_peers.insert(peer_id.clone());
        state.banned_peers.insert(PeerId::new("banned"));

        let result = plan
            .execute_admin_action(
                AdminAction::ExportDiagnostics,
                &mut state,
                None,
                now,
                Some(360),
            )
            .expect("diagnostics");

        match result {
            AdminResult::Diagnostics(diagnostics) => {
                assert_eq!(diagnostics.swarm.connected_peers, 1);
                assert_eq!(diagnostics.pinned_heads.len(), 1);
                assert!(diagnostics.quarantined_peers.contains(&peer_id));
                assert!(diagnostics.admitted_peers.is_empty());
                assert!(diagnostics.rejected_peers.is_empty());
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn diagnostics_export_includes_peer_trust_and_rejection_details() {
        let plan = plan(BootstrapPreset::BootstrapArchive);
        let now = Utc::now();
        let admitted_peer = PeerId::new("admitted");
        let rejected_peer = PeerId::new("rejected");
        let mut state = BootstrapAdminState::default();

        state
            .peer_store
            .mark_connection(admitted_peer.clone(), true, now);
        state
            .peer_store
            .mark_connection(rejected_peer.clone(), false, now);
        state.admitted_peers.insert(admitted_peer.clone());
        state.peer_admission_reports.insert(
            admitted_peer.clone(),
            PeerAdmissionReport {
                peer_id: admitted_peer.clone(),
                principal_id: PrincipalId::new("principal-admitted"),
                requested_scopes: BTreeSet::new(),
                trust_level: PeerTrustLevel::PolicyCompliant,
                issuer_peer_id: PeerId::new("issuer"),
                findings: Vec::new(),
                verified_at: now,
            },
        );
        state
            .rejected_peers
            .insert(rejected_peer.clone(), "missing auth envelope".into());

        let mut admitted_reputation = ReputationState::new(admitted_peer.clone(), now);
        admitted_reputation.score = 1.5;
        state
            .peer_reputation
            .insert(admitted_peer.clone(), admitted_reputation);
        let mut rejected_reputation = ReputationState::new(rejected_peer.clone(), now);
        rejected_reputation.score = ReputationEngine::default().policy.ban_threshold - 1.0;
        state
            .peer_reputation
            .insert(rejected_peer.clone(), rejected_reputation);

        let diagnostics = state.diagnostics(&plan, now, None);
        let admitted = diagnostics
            .peer_diagnostics
            .iter()
            .find(|entry| entry.peer_id == admitted_peer)
            .expect("admitted peer diagnostic");
        assert!(admitted.connected);
        assert_eq!(admitted.trust_level, Some(PeerTrustLevel::PolicyCompliant));
        assert_eq!(
            admitted.reputation_decision,
            Some(ReputationDecision::Allow)
        );
        assert_eq!(admitted.reputation_score, Some(1.5));

        let rejected = diagnostics
            .peer_diagnostics
            .iter()
            .find(|entry| entry.peer_id == rejected_peer)
            .expect("rejected peer diagnostic");
        assert!(!rejected.connected);
        assert_eq!(
            rejected.rejection_reason.as_deref(),
            Some("missing auth envelope")
        );
        assert_eq!(rejected.reputation_decision, Some(ReputationDecision::Ban));
    }

    #[test]
    fn diagnostics_bundle_export_includes_runtime_snapshot_and_persisted_state() {
        let plan = plan(BootstrapPreset::AuthorityValidator);
        let now = Utc::now();
        let peer_id = PeerId::new("peer");
        let receipt = ContributionReceipt {
            receipt_id: burn_p2p_core::ContributionReceiptId::new("receipt"),
            peer_id: peer_id.clone(),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base"),
            artifact_id: ArtifactId::new("artifact"),
            accepted_at: now,
            accepted_weight: 1.0,
            metrics: BTreeMap::new(),
            merge_cert_id: Some(burn_p2p_core::MergeCertId::new("merge-cert")),
        };
        let merge = MergeCertificate {
            merge_cert_id: burn_p2p_core::MergeCertId::new("merge-cert"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base"),
            merged_head_id: HeadId::new("head"),
            merged_artifact_id: ArtifactId::new("artifact"),
            policy: burn_p2p_core::MergePolicy::WeightedMean,
            issued_at: now,
            validator: PeerId::new("validator"),
            contribution_receipts: vec![burn_p2p_core::ContributionReceiptId::new("receipt")],
        };
        let head = HeadDescriptor {
            head_id: HeadId::new("head"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-head"),
            parent_head_id: Some(HeadId::new("base")),
            global_step: 4,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let reducer_load = ReducerLoadAnnouncement {
            overlay: burn_p2p::OverlayTopic::experiment(
                burn_p2p_core::NetworkId::new("mainnet"),
                StudyId::new("study"),
                ExperimentId::new("exp"),
                burn_p2p::OverlayChannel::Telemetry,
            )
            .expect("telemetry overlay"),
            report: burn_p2p_core::ReducerLoadReport {
                peer_id: peer_id.clone(),
                window_id: WindowId(7),
                assigned_leaf_updates: 2,
                assigned_aggregate_inputs: 1,
                ingress_bytes: 64,
                egress_bytes: 32,
                duplicate_transfer_ratio: 0.0,
                overload_ratio: 0.0,
                reported_at: now,
            },
        };
        let runtime_snapshot = NodeTelemetrySnapshot {
            status: RuntimeStatus::Running,
            node_state: super::NodeRuntimeState::IdleReady,
            slot_states: vec![super::SlotRuntimeState::Assigned(SlotAssignmentState::new(
                StudyId::new("study"),
                ExperimentId::new("exp"),
                RevisionId::new("rev"),
            ))],
            lag_state: burn_p2p::LagState::SlightlyBehind,
            head_lag_steps: 1,
            lag_policy: burn_p2p::LagPolicy::default(),
            network_id: Some(burn_p2p_core::NetworkId::new("mainnet")),
            local_peer_id: Some(peer_id.clone()),
            configured_roles: burn_p2p_core::PeerRoleSet::new([burn_p2p_core::PeerRole::Validator]),
            connected_peers: 1,
            observed_peer_ids: BTreeSet::from([peer_id.clone()]),
            known_peer_addresses: BTreeSet::new(),
            runtime_boundary: None,
            listen_addresses: Vec::new(),
            control_plane: ControlPlaneSnapshot {
                reducer_load_announcements: vec![reducer_load.clone()],
                ..ControlPlaneSnapshot::default()
            },
            recent_events: vec![LiveControlPlaneEvent::ConnectionEstablished {
                peer_id: peer_id.to_string(),
            }],
            last_snapshot_peer_id: Some(peer_id.clone()),
            last_snapshot: Some(ControlPlaneSnapshot::default()),
            admitted_peers: BTreeMap::new(),
            rejected_peers: BTreeMap::new(),
            peer_reputation: BTreeMap::new(),
            minimum_revocation_epoch: None,
            trust_bundle: None,
            in_flight_transfers: BTreeMap::new(),
            robustness_policy: Some(burn_p2p_core::RobustnessPolicy::balanced()),
            latest_cohort_robustness: Some(burn_p2p_core::CohortRobustnessReport {
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                window_id: WindowId(7),
                cohort_filter_strategy: burn_p2p_core::CohortFilterStrategy::SimilarityAware,
                aggregation_strategy: burn_p2p_core::AggregationStrategy::ClippedWeightedMean,
                total_updates: 3,
                accepted_updates: 2,
                rejected_updates: 1,
                downweighted_updates: 0,
                effective_weight_sum: 2.0,
                mean_screen_score: 0.4,
                rejection_reasons: BTreeMap::from([(
                    burn_p2p_core::RejectionReason::SimilarityOutlier,
                    1,
                )]),
                alerts: vec![burn_p2p_core::RobustnessAlert {
                    experiment_id: ExperimentId::new("exp"),
                    revision_id: RevisionId::new("rev"),
                    reason: burn_p2p_core::RejectionReason::SimilarityOutlier,
                    severity: burn_p2p_core::RobustnessAlertSeverity::Warn,
                    message: "cohort drift".into(),
                    peer_id: Some(peer_id.clone()),
                    window_id: Some(WindowId(7)),
                    emitted_at: now,
                }],
            }),
            trust_scores: vec![burn_p2p_core::TrustScore {
                peer_id: peer_id.clone(),
                score: 0.72,
                quarantined: true,
                reducer_eligible: false,
                validator_eligible: false,
                ban_recommended: true,
                updated_at: now,
            }],
            canary_reports: vec![burn_p2p_core::CanaryEvalReport {
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                candidate_head_id: HeadId::new("head"),
                base_head_id: Some(HeadId::new("base")),
                eval_protocol_id: burn_p2p_core::ContentId::new("canary"),
                metric_deltas: BTreeMap::from([("loss".into(), 0.19)]),
                regression_margin: 0.19,
                detected_backdoor_trigger: false,
                accepted: false,
                evaluator_quorum: 1,
                evaluated_at: now,
            }],
            effective_limit_profile: None,
            last_error: Some("peer rejected".into()),
            started_at: now,
            updated_at: now,
        };
        let mut state = BootstrapAdminState {
            head_descriptors: vec![head.clone()],
            contribution_receipts: vec![receipt.clone()],
            merge_certificates: vec![merge.clone()],
            reducer_load_announcements: vec![reducer_load.clone()],
            runtime_snapshot: Some(runtime_snapshot.clone()),
            ..BootstrapAdminState::default()
        };

        let result = plan
            .execute_admin_action(
                AdminAction::ExportDiagnosticsBundle,
                &mut state,
                None,
                now,
                Some(42),
            )
            .expect("bundle");

        match result {
            AdminResult::DiagnosticsBundle(bundle) => {
                assert_eq!(
                    bundle.plan.network_id(),
                    &burn_p2p_core::NetworkId::new("mainnet")
                );
                assert_eq!(bundle.diagnostics.accepted_receipts, 1);
                assert_eq!(bundle.heads, vec![head]);
                assert_eq!(bundle.contribution_receipts, vec![receipt]);
                assert_eq!(bundle.merge_certificates, vec![merge]);
                assert_eq!(bundle.reducer_load_announcements, vec![reducer_load]);
                assert_eq!(
                    bundle
                        .diagnostics
                        .robustness_panel
                        .as_ref()
                        .map(|panel| panel.quarantined_peers.len()),
                    Some(1)
                );
                assert_eq!(
                    bundle
                        .diagnostics
                        .robustness_panel
                        .as_ref()
                        .and_then(|panel| panel.quarantined_peers.first())
                        .map(|peer| peer.ban_recommended),
                    Some(true)
                );
                assert!(bundle.diagnostics.quarantined_peers.contains(&peer_id));
                #[cfg(feature = "metrics-indexer")]
                assert_eq!(
                    bundle.diagnostics.robustness_rollup.as_ref().map(|rollup| {
                        (
                            rollup.quarantined_peer_count,
                            rollup.ban_recommended_peer_count,
                        )
                    }),
                    Some((1, 1))
                );
                assert_eq!(bundle.runtime_snapshot, Some(runtime_snapshot));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn head_and_reducer_load_exports_filter_on_scope() {
        let plan = plan(BootstrapPreset::AuthorityValidator);
        let now = Utc::now();
        let head = HeadDescriptor {
            head_id: HeadId::new("head-1"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-head"),
            parent_head_id: Some(HeadId::new("base")),
            global_step: 9,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let reducer_load = ReducerLoadAnnouncement {
            overlay: burn_p2p::OverlayTopic::experiment(
                burn_p2p_core::NetworkId::new("mainnet"),
                StudyId::new("study"),
                ExperimentId::new("exp"),
                burn_p2p::OverlayChannel::Telemetry,
            )
            .expect("telemetry overlay"),
            report: burn_p2p_core::ReducerLoadReport {
                peer_id: PeerId::new("peer"),
                window_id: WindowId(9),
                assigned_leaf_updates: 3,
                assigned_aggregate_inputs: 1,
                ingress_bytes: 128,
                egress_bytes: 64,
                duplicate_transfer_ratio: 0.25,
                overload_ratio: 0.1,
                reported_at: now,
            },
        };
        let mut state = BootstrapAdminState {
            head_descriptors: vec![head.clone()],
            reducer_load_announcements: vec![reducer_load.clone()],
            ..BootstrapAdminState::default()
        };

        let heads = plan
            .execute_admin_action(
                AdminAction::ExportHeads(HeadQuery {
                    study_id: Some(StudyId::new("study")),
                    experiment_id: Some(ExperimentId::new("exp")),
                    revision_id: Some(RevisionId::new("rev")),
                    head_id: None,
                }),
                &mut state,
                None,
                now,
                None,
            )
            .expect("heads");
        match heads {
            AdminResult::Heads(exported) => assert_eq!(exported, vec![head]),
            other => panic!("unexpected result: {other:?}"),
        }

        let reducer_loads = plan
            .execute_admin_action(
                AdminAction::ExportReducerLoad(ReducerLoadQuery {
                    study_id: Some(StudyId::new("study")),
                    experiment_id: Some(ExperimentId::new("exp")),
                    peer_id: Some(PeerId::new("peer")),
                }),
                &mut state,
                None,
                now,
                None,
            )
            .expect("reducer load");
        match reducer_loads {
            AdminResult::ReducerLoad(exported) => assert_eq!(exported, vec![reducer_load]),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn control_actions_update_local_admin_policy_state() {
        let plan = plan(BootstrapPreset::AuthorityValidator);
        let now = Utc::now();
        let peer_id = PeerId::new("peer-revoked");
        let mut state = BootstrapAdminState::default();

        let revoke = plan
            .execute_admin_action(
                AdminAction::Control(ExperimentControlCommand::RevokePeer {
                    peer_id: peer_id.clone(),
                    minimum_revocation_epoch: burn_p2p::RevocationEpoch(7),
                    reason: "key compromised".into(),
                }),
                &mut state,
                Some(burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: now,
                    signature_hex: "abcd".into(),
                }),
                now,
                None,
            )
            .expect("revoke result");
        assert!(matches!(revoke, AdminResult::Control(_)));
        assert!(state.quarantined_peers.contains(&peer_id));
        assert!(state.banned_peers.contains(&peer_id));
        assert_eq!(
            state.minimum_revocation_epoch,
            Some(burn_p2p::RevocationEpoch(7))
        );

        let quarantine_peer = PeerId::new("peer-quarantined");
        let quarantine = plan
            .execute_admin_action(
                AdminAction::Control(ExperimentControlCommand::QuarantinePeer {
                    peer_id: quarantine_peer.clone(),
                    reason: "audit pending".into(),
                    target: ActivationTarget {
                        activation: WindowActivation {
                            activation_window: WindowId(3),
                            grace_windows: 0,
                        },
                        required_client_capabilities: BTreeSet::new(),
                    },
                }),
                &mut state,
                Some(burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: now,
                    signature_hex: "beef".into(),
                }),
                now,
                None,
            )
            .expect("quarantine result");
        assert!(matches!(quarantine, AdminResult::Control(_)));
        assert!(state.quarantined_peers.contains(&quarantine_peer));
        assert!(!state.banned_peers.contains(&quarantine_peer));

        let pardon = plan
            .execute_admin_action(
                AdminAction::Control(ExperimentControlCommand::PardonPeer {
                    peer_id: quarantine_peer.clone(),
                    target: ActivationTarget {
                        activation: WindowActivation {
                            activation_window: WindowId(4),
                            grace_windows: 0,
                        },
                        required_client_capabilities: BTreeSet::new(),
                    },
                }),
                &mut state,
                Some(burn_p2p_core::SignatureMetadata {
                    signer: PeerId::new("authority"),
                    key_id: "authority-key".into(),
                    algorithm: burn_p2p_core::SignatureAlgorithm::Ed25519,
                    signed_at: now,
                    signature_hex: "cafe".into(),
                }),
                now,
                None,
            )
            .expect("pardon result");
        assert!(matches!(pardon, AdminResult::Control(_)));
        assert!(!state.quarantined_peers.contains(&quarantine_peer));
        assert!(!state.banned_peers.contains(&quarantine_peer));
    }

    #[test]
    fn receipt_export_filters_on_scope() {
        let plan = plan(BootstrapPreset::BootstrapOnly);
        let now = Utc::now();
        let receipt = ContributionReceipt {
            receipt_id: burn_p2p_core::ContributionReceiptId::new("receipt"),
            peer_id: PeerId::new("peer"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base"),
            artifact_id: ArtifactId::new("artifact"),
            accepted_at: now,
            accepted_weight: 1.0,
            metrics: BTreeMap::new(),
            merge_cert_id: None,
        };

        let mut state = BootstrapAdminState {
            contribution_receipts: vec![receipt.clone()],
            ..BootstrapAdminState::default()
        };

        let result = plan
            .execute_admin_action(
                AdminAction::ExportReceipts(ReceiptQuery {
                    study_id: Some(StudyId::new("study")),
                    experiment_id: Some(ExperimentId::new("exp")),
                    revision_id: None,
                    peer_id: None,
                }),
                &mut state,
                None,
                now,
                None,
            )
            .expect("receipts");

        match result {
            AdminResult::Receipts(receipts) => assert_eq!(receipts, vec![receipt]),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn openmetrics_export_contains_core_operator_metrics() {
        let diagnostics = BootstrapAdminState::default().diagnostics(
            &plan(BootstrapPreset::BootstrapOnly),
            Utc::now(),
            Some(120),
        );

        let metrics = render_openmetrics(&diagnostics);

        assert!(metrics.contains("burn_p2p_connected_peers"));
        assert!(metrics.contains("burn_p2p_observed_peers"));
        assert!(metrics.contains("burn_p2p_accepted_receipts"));
        assert!(metrics.contains("burn_p2p_admitted_peers"));
        assert!(metrics.contains("burn_p2p_rejected_peers"));
    }

    #[cfg(feature = "metrics-indexer")]
    #[test]
    fn openmetrics_prefers_rollup_for_robustness_summary_metrics() {
        let mut diagnostics = BootstrapAdminState::default().diagnostics(
            &plan(BootstrapPreset::BootstrapOnly),
            Utc::now(),
            Some(120),
        );
        diagnostics.robustness_panel = Some(burn_p2p_views::RobustnessPanelView {
            preset: burn_p2p_core::RobustnessPreset::Balanced,
            policy: burn_p2p_core::RobustnessPolicy::balanced(),
            rejection_reasons: vec![burn_p2p_views::RobustnessReasonCountView {
                reason: burn_p2p_core::RejectionReason::Replay,
                count: 1,
            }],
            trust_scores: vec![burn_p2p_views::TrustScorePointView {
                peer_id: PeerId::new("peer-panel"),
                score: 0.25,
                quarantined: true,
                ban_recommended: false,
                updated_at: Utc::now(),
            }],
            quarantined_peers: vec![burn_p2p_views::QuarantinedPeerView {
                peer_id: PeerId::new("peer-panel"),
                reducer_eligible: false,
                validator_eligible: false,
                ban_recommended: false,
            }],
            canary_regressions: vec![burn_p2p_views::CanaryRegressionView {
                candidate_head_id: HeadId::new("candidate-panel"),
                regression_margin: 0.1,
                detected_backdoor_trigger: false,
                evaluated_at: Utc::now(),
            }],
            alerts: Vec::new(),
        });
        diagnostics.robustness_rollup = Some(burn_p2p_metrics::RobustnessRollup {
            network_id: burn_p2p_core::NetworkId::new("mainnet"),
            experiment_id: ExperimentId::new("exp-rollup"),
            revision_id: RevisionId::new("rev-rollup"),
            window_id: WindowId(9),
            total_updates: 10,
            rejected_updates: 7,
            rejection_ratio: 0.7,
            mean_trust_score: -1.25,
            quarantined_peer_count: 1,
            ban_recommended_peer_count: 2,
            canary_regression_count: 3,
            alert_count: 4,
            captured_at: Utc::now(),
        });

        let metrics = render_openmetrics(&diagnostics);

        assert!(metrics.contains("burn_p2p_robustness_rejected_updates 7"));
        assert!(metrics.contains("burn_p2p_robustness_mean_trust_score -1.250000"));
        assert!(metrics.contains("burn_p2p_robustness_canary_regressions 3"));
        assert!(metrics.contains("burn_p2p_robustness_ban_recommendations 2"));
        assert!(metrics.contains("burn_p2p_robustness_alerts 4"));
    }

    #[cfg(feature = "browser-edge")]
    #[test]
    fn dashboard_html_references_status_and_event_endpoints() {
        let html = render_dashboard_html(&burn_p2p_core::NetworkId::new("mainnet"));

        assert!(html.contains("/status"));
        assert!(html.contains("/events"));
        assert!(html.contains("/portal/snapshot"));
        assert!(html.contains("burn_p2p bootstrap"));
    }

    #[test]
    fn embedded_daemon_initializes_and_certifies_trainer_updates() {
        let dataset_dir = TempDir::new().expect("dataset tempdir");
        create_dataset(dataset_dir.path());
        let validator_storage = TempDir::new().expect("validator tempdir");
        let trainer_storage = TempDir::new().expect("trainer tempdir");
        let active = ActiveExperiment {
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
        };
        let bootstrap_addr = loopback_listen_address();
        let trainer_addr = loopback_listen_address();
        let daemon = plan(BootstrapPreset::AuthorityValidator)
            .spawn_embedded_daemon(
                SyntheticProject {
                    dataset_root: dataset_dir.path().to_path_buf(),
                    learning_rate: 0.1,
                    target_model: 10.0,
                },
                BootstrapEmbeddedDaemonConfig {
                    node: NodeConfig {
                        identity: burn_p2p::IdentityConfig::Persistent,
                        storage: Some(StorageConfig::new(validator_storage.path())),
                        dataset: Some(DatasetConfig::new(burn_p2p::UpstreamAdapter::Local {
                            root: dataset_dir.path().display().to_string(),
                        })),
                        auth: None,
                        network_manifest: None,
                        client_release_manifest: None,
                        selected_workload_id: None,
                        metrics_retention: burn_p2p::MetricsRetentionConfig::default(),
                        bootstrap_peers: Vec::new(),
                        listen_addresses: vec![bootstrap_addr.clone()],
                    },
                    active_experiment: active.clone(),
                    initialize_head_on_start: true,
                    restore_head_on_start: true,
                    validation_interval_millis: 100,
                    reducer_interval_millis: None,
                    training_interval_millis: None,
                },
            )
            .expect("spawn embedded daemon");
        let daemon_telemetry = daemon.telemetry();
        let mut trainer = NodeBuilder::new(SyntheticProject {
            dataset_root: dataset_dir.path().to_path_buf(),
            learning_rate: 1.0,
            target_model: 10.0,
        })
        .with_mainnet(genesis())
        .with_storage(StorageConfig::new(trainer_storage.path()))
        .with_dataset(DatasetConfig::new(burn_p2p::UpstreamAdapter::Local {
            root: dataset_dir.path().display().to_string(),
        }))
        .with_listen_address(trainer_addr)
        .with_bootstrap_peer(bootstrap_addr)
        .spawn()
        .expect("trainer spawn");
        let experiment = trainer.experiment(
            active.study_id.clone(),
            active.experiment_id.clone(),
            active.revision_id.clone(),
        );

        wait_for(
            Duration::from_secs(5),
            || daemon_telemetry.snapshot().connected_peers >= 1,
            "embedded daemon did not connect to trainer",
        );
        wait_for(
            Duration::from_secs(10),
            || {
                trainer
                    .sync_experiment_head(&experiment)
                    .expect("trainer sync daemon head")
                    .is_some()
            },
            "trainer did not sync the embedded daemon genesis head",
        );

        let outcome = trainer
            .train_window_once(&experiment)
            .expect("trainer window");
        assert!(matches!(
            outcome.report.stats.get("loss"),
            Some(MetricValue::Float(_))
        ));

        let certification_started = Instant::now();
        loop {
            let diagnostics = daemon.diagnostics(None);
            if diagnostics.accepted_receipts >= 1 && diagnostics.certified_merges >= 1 {
                break;
            }

            if certification_started.elapsed() >= Duration::from_secs(20) {
                let telemetry = daemon_telemetry.snapshot();
                let state = daemon.admin_state();
                let state = state
                    .lock()
                    .expect("embedded daemon state should not be poisoned");
                panic!(
                    "embedded daemon did not certify trainer update within 20s; \
                     diagnostics=accepted_receipts:{} certified_merges:{} \
                     state_receipts:{} state_merges:{} connected_peers:{} \
                     update_announcements:{} merge_announcements:{} last_error={:?}",
                    diagnostics.accepted_receipts,
                    diagnostics.certified_merges,
                    state.contribution_receipts.len(),
                    state.merge_certificates.len(),
                    telemetry.connected_peers,
                    telemetry.control_plane.update_announcements.len(),
                    telemetry.control_plane.merge_announcements.len(),
                    telemetry.last_error,
                );
            }

            thread::sleep(Duration::from_millis(20));
        }

        let diagnostics = daemon.diagnostics(None);
        assert_eq!(diagnostics.accepted_receipts, 1);
        assert_eq!(diagnostics.certified_merges, 1);

        trainer.shutdown().expect("trainer shutdown");
        let _ = trainer.await_termination().expect("trainer termination");
        daemon.shutdown().expect("daemon shutdown");
        daemon.await_termination().expect("daemon termination");
    }

    #[test]
    fn bootstrap_peer_daemon_runs_without_workload_state() {
        let storage = TempDir::new().expect("bootstrap peer tempdir");
        let daemon = plan(BootstrapPreset::BootstrapOnly)
            .spawn_bootstrap_peer_daemon(BootstrapPeerDaemonConfig {
                node: NodeConfig {
                    identity: burn_p2p::IdentityConfig::Persistent,
                    storage: Some(StorageConfig::new(storage.path())),
                    dataset: None,
                    auth: None,
                    network_manifest: None,
                    client_release_manifest: None,
                    selected_workload_id: None,
                    metrics_retention: burn_p2p::MetricsRetentionConfig::default(),
                    bootstrap_peers: Vec::new(),
                    listen_addresses: vec![
                        burn_p2p_swarm::SwarmAddress::new("/ip4/127.0.0.1/tcp/0")
                            .expect("listen address"),
                    ],
                },
            })
            .expect("spawn bootstrap peer daemon");
        let telemetry = daemon.telemetry();

        wait_for(
            Duration::from_secs(5),
            || {
                let snapshot = telemetry.snapshot();
                snapshot.local_peer_id.is_some() && !snapshot.listen_addresses.is_empty()
            },
            "bootstrap peer daemon did not become ready",
        );

        let snapshot = telemetry.snapshot();
        assert!(
            snapshot
                .configured_roles
                .contains(&burn_p2p::PeerRole::Bootstrap)
        );
        assert!(
            snapshot
                .configured_roles
                .contains(&burn_p2p::PeerRole::RelayHelper)
        );
        assert!(
            !snapshot
                .configured_roles
                .contains(&burn_p2p::PeerRole::Validator)
        );

        daemon.shutdown().expect("bootstrap peer daemon shutdown");
        daemon
            .await_termination()
            .expect("bootstrap peer daemon termination");
    }

    #[cfg(feature = "social")]
    #[test]
    fn leaderboard_snapshot_aggregates_receipts_by_principal() {
        let peer_one = PeerId::new("peer-1");
        let peer_two = PeerId::new("peer-2");
        let principal_id = PrincipalId::new("alice");
        let report = |peer_id: &PeerId| PeerAdmissionReport {
            peer_id: peer_id.clone(),
            principal_id: principal_id.clone(),
            requested_scopes: BTreeSet::new(),
            trust_level: PeerTrustLevel::ScopeAuthorized,
            issuer_peer_id: PeerId::new("authority-1"),
            findings: Vec::new(),
            verified_at: Utc::now(),
        };
        let receipt =
            |peer_id: &PeerId, suffix: &str, accepted_weight: f64, loss: f64| ContributionReceipt {
                receipt_id: burn_p2p_core::ContributionReceiptId::new(format!("receipt-{suffix}")),
                peer_id: peer_id.clone(),
                study_id: StudyId::new("study-1"),
                experiment_id: ExperimentId::new("exp-1"),
                revision_id: RevisionId::new("rev-1"),
                base_head_id: HeadId::new("head-0"),
                artifact_id: ArtifactId::new(format!("artifact-{suffix}")),
                accepted_at: Utc::now(),
                accepted_weight,
                metrics: BTreeMap::from([("loss".into(), MetricValue::Float(loss))]),
                merge_cert_id: None,
            };

        let first_receipt = receipt(&peer_one, "a", 3.0, 1.0);
        let second_receipt = receipt(&peer_two, "b", 2.0, 0.5);
        let state = BootstrapAdminState {
            contribution_receipts: vec![first_receipt.clone(), second_receipt],
            merge_certificates: vec![MergeCertificate {
                merge_cert_id: burn_p2p_core::MergeCertId::new("merge-1"),
                study_id: StudyId::new("study-1"),
                experiment_id: ExperimentId::new("exp-1"),
                revision_id: RevisionId::new("rev-1"),
                base_head_id: HeadId::new("head-0"),
                merged_head_id: HeadId::new("head-1"),
                merged_artifact_id: ArtifactId::new("artifact-merged"),
                policy: burn_p2p::MergePolicy::WeightedMean,
                issued_at: Utc::now(),
                validator: PeerId::new("validator-1"),
                contribution_receipts: vec![first_receipt.receipt_id.clone()],
            }],
            peer_admission_reports: BTreeMap::from([
                (peer_one.clone(), report(&peer_one)),
                (peer_two.clone(), report(&peer_two)),
            ]),
            ..BootstrapAdminState::default()
        };

        let leaderboard =
            state.leaderboard_snapshot(&plan(BootstrapPreset::BootstrapOnly), Utc::now());
        assert_eq!(leaderboard.entries.len(), 1);
        let entry = &leaderboard.entries[0];
        assert_eq!(entry.identity.principal_id, Some(principal_id));
        assert_eq!(
            entry.identity.peer_ids,
            BTreeSet::from([peer_one, peer_two])
        );
        assert_eq!(entry.accepted_receipt_count, 2);
        assert!((entry.accepted_work_score - 4.0).abs() < f64::EPSILON);
        assert!(entry.leaderboard_score_v1 >= entry.accepted_work_score);
        assert!(
            entry
                .badges
                .iter()
                .any(|badge| badge.kind == BadgeKind::FirstAcceptedUpdate)
        );
        assert!(
            entry
                .badges
                .iter()
                .any(|badge| badge.kind == BadgeKind::HelpedPromoteCanonicalHead)
        );
    }

    #[cfg(not(feature = "social"))]
    #[test]
    fn leaderboard_snapshot_is_empty_when_social_service_is_not_compiled() {
        let state = BootstrapAdminState::default();
        let leaderboard =
            state.leaderboard_snapshot(&plan(BootstrapPreset::BootstrapOnly), Utc::now());
        assert!(leaderboard.entries.is_empty());
        assert_eq!(leaderboard.score_version, "leaderboard_score_v1");
    }

    #[test]
    fn operator_history_preview_stays_bounded_while_store_backed_exports_remain_complete() {
        fn write_json<T: serde::Serialize>(path: PathBuf, value: &T) {
            fs::create_dir_all(path.parent().expect("history parent"))
                .expect("create history parent");
            fs::write(
                &path,
                serde_json::to_vec_pretty(value).expect("encode history record"),
            )
            .expect("write history record");
        }

        let tempdir = TempDir::new().expect("history tempdir");
        let storage = StorageConfig::new(tempdir.path());
        let now = Utc::now();

        for index in 0..20 {
            let receipt = ContributionReceipt {
                receipt_id: burn_p2p_core::ContributionReceiptId::new(format!(
                    "receipt-{index:03}"
                )),
                peer_id: PeerId::new(format!("peer-{}", index % 3)),
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                base_head_id: HeadId::new("base-head"),
                artifact_id: ArtifactId::new(format!("artifact-{index:03}")),
                accepted_at: now + chrono::Duration::seconds(index as i64),
                accepted_weight: 1.0 + index as f64,
                metrics: BTreeMap::from([("loss".into(), MetricValue::Float(index as f64))]),
                merge_cert_id: Some(burn_p2p_core::MergeCertId::new(format!("merge-{index:03}"))),
            };
            write_json(
                storage
                    .receipts_dir()
                    .join(format!("{}.json", receipt.receipt_id.as_str())),
                &receipt,
            );
        }

        for index in 0..12 {
            let merge = MergeCertificate {
                merge_cert_id: burn_p2p_core::MergeCertId::new(format!("merge-{index:03}")),
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                base_head_id: HeadId::new("base-head"),
                merged_head_id: HeadId::new(format!("merged-head-{index:03}")),
                merged_artifact_id: ArtifactId::new(format!("merged-artifact-{index:03}")),
                policy: burn_p2p::MergePolicy::WeightedMean,
                issued_at: now + chrono::Duration::seconds(index as i64),
                validator: PeerId::new("validator"),
                contribution_receipts: vec![burn_p2p_core::ContributionReceiptId::new(format!(
                    "receipt-{index:03}"
                ))],
            };
            write_json(
                storage
                    .receipts_dir()
                    .join(format!("merge-{}.json", merge.merge_cert_id.as_str())),
                &merge,
            );
        }

        for index in 0..14 {
            let head = HeadDescriptor {
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                head_id: HeadId::new(format!("head-{index:03}")),
                artifact_id: ArtifactId::new(format!("head-artifact-{index:03}")),
                parent_head_id: (index > 0).then(|| HeadId::new(format!("head-{:03}", index - 1))),
                global_step: index as u64,
                created_at: now + chrono::Duration::seconds(index as i64),
                metrics: BTreeMap::new(),
            };
            write_json(
                storage.heads_dir().join(format!("head-{index:03}.json")),
                &head,
            );
        }

        for index in 0..7 {
            let metrics = PeerWindowMetrics {
                network_id: burn_p2p_core::NetworkId::new("network"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                workload_id: WorkloadId::new("workload"),
                dataset_view_id: burn_p2p::DatasetViewId::new("dataset"),
                peer_id: PeerId::new(format!("trainer-{}", index % 2)),
                principal_id: None,
                lease_id: burn_p2p::LeaseId::new(format!("lease-{index:03}")),
                base_head_id: HeadId::new("base-head"),
                window_started_at: now + chrono::Duration::seconds(index as i64),
                window_finished_at: now + chrono::Duration::seconds(index as i64 + 1),
                attempted_tokens_or_samples: 64,
                accepted_tokens_or_samples: Some(64),
                local_train_loss_mean: Some(0.1),
                local_train_loss_last: Some(0.1),
                grad_or_delta_norm: Some(1.0),
                optimizer_step_count: 1,
                compute_time_ms: 10,
                data_fetch_time_ms: 5,
                publish_latency_ms: 2,
                head_lag_at_start: 0,
                head_lag_at_finish: 0,
                backend_class: BackendClass::Ndarray,
                role: burn_p2p::PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            };
            write_json(
                storage
                    .metrics_dir()
                    .join(format!("peer-window-{index:03}.json")),
                &metrics,
            );
        }

        for index in 0..6 {
            let metrics = ReducerCohortMetrics {
                network_id: burn_p2p_core::NetworkId::new("network"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                workload_id: WorkloadId::new("workload"),
                dataset_view_id: burn_p2p::DatasetViewId::new("dataset"),
                merge_window_id: ContentId::new(format!("merge-window-{index:03}")),
                reducer_group_id: ContentId::new(format!("reducers-{index:03}")),
                captured_at: now + chrono::Duration::seconds(index as i64),
                base_head_id: HeadId::new("base-head"),
                candidate_head_id: Some(HeadId::new(format!("candidate-{index:03}"))),
                received_updates: 4,
                accepted_updates: 4,
                rejected_updates: 0,
                sum_weight: 4.0,
                accepted_tokens_or_samples: 256,
                staleness_mean: 0.0,
                staleness_max: 0.0,
                window_close_delay_ms: 10,
                cohort_duration_ms: 20,
                aggregate_norm: 0.5,
                reducer_load: 0.2,
                ingress_bytes: 128,
                egress_bytes: 64,
                replica_agreement: Some(1.0),
                late_arrival_count: Some(0),
                missing_peer_count: Some(0),
                rejection_reasons: BTreeMap::new(),
                status: ReducerCohortStatus::Closed,
            };
            write_json(
                storage
                    .metrics_dir()
                    .join(format!("reducer-cohort-{index:03}.json")),
                &metrics,
            );
        }

        for index in 0..9 {
            let report = HeadEvalReport {
                network_id: burn_p2p_core::NetworkId::new("network"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                workload_id: WorkloadId::new("workload"),
                head_id: HeadId::new("head-eval-target"),
                base_head_id: Some(HeadId::new("base-head")),
                eval_protocol_id: ContentId::new(format!("eval-protocol-{index:03}")),
                evaluator_set_id: ContentId::new("validators"),
                metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.1))]),
                sample_count: 64,
                dataset_view_id: burn_p2p::DatasetViewId::new("dataset"),
                started_at: now + chrono::Duration::seconds(index as i64),
                finished_at: now + chrono::Duration::seconds(index as i64 + 1),
                trust_class: MetricTrustClass::Canonical,
                status: HeadEvalStatus::Completed,
                signature_bundle: Vec::new(),
            };
            write_json(
                storage
                    .metrics_dir()
                    .join(format!("head-eval-{index:03}.json")),
                &report,
            );
        }

        for index in 0..8 {
            let record = super::StoredEvalProtocolManifestRecord {
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                captured_at: now + chrono::Duration::seconds(index as i64),
                manifest: burn_p2p_core::EvalProtocolManifest::new(
                    format!("validation-{index:03}"),
                    burn_p2p::DatasetViewId::new("dataset"),
                    "validation",
                    vec![burn_p2p_core::EvalMetricDef {
                        metric_key: "loss".into(),
                        display_name: "Loss".into(),
                        unit: None,
                        higher_is_better: false,
                    }],
                    burn_p2p_core::EvalProtocolOptions::new(
                        burn_p2p_core::EvalAggregationRule::Mean,
                        32,
                        1,
                        "v1",
                    ),
                )
                .expect("eval protocol"),
            };
            write_json(
                storage
                    .metrics_dir()
                    .join(format!("eval-protocol-{index:03}.json")),
                &record,
            );
        }

        let metrics_budget = burn_p2p::MetricsRetentionBudget {
            max_peer_window_entries_per_revision: 3,
            max_reducer_cohort_entries_per_revision: 2,
            max_head_eval_reports_per_revision: 4,
            max_metric_revisions_per_experiment: 1,
            max_peer_window_detail_windows: 2,
        };
        let memory_budget = super::history::OperatorHistoryMemoryBudget {
            max_receipts_in_memory: 5,
            max_merges_in_memory: 4,
            max_heads_in_memory: 3,
            max_eval_protocol_manifests_in_memory: 2,
        };

        let history = super::load_operator_history(Some(&storage), metrics_budget, memory_budget)
            .expect("load bounded operator history");

        assert_eq!(history.receipts.len(), 5);
        assert_eq!(history.merges.len(), 4);
        assert_eq!(history.heads.len(), 3);
        assert_eq!(history.eval_protocol_manifests.len(), 2);
        assert_eq!(history.peer_window_metrics.len(), 3);
        assert_eq!(history.reducer_cohort_metrics.len(), 2);
        assert_eq!(history.head_eval_reports.len(), 4);
        assert_eq!(history.totals.receipt_count, 20);
        assert_eq!(history.totals.merge_count, 12);
        assert_eq!(history.totals.head_count, 14);
        assert_eq!(history.totals.eval_protocol_manifest_count, 8);

        let state = BootstrapAdminState {
            head_descriptors: history.heads.clone(),
            contribution_receipts: history.receipts.clone(),
            persisted_receipt_count: history.totals.receipt_count,
            merge_certificates: history.merges.clone(),
            persisted_merge_count: history.totals.merge_count,
            persisted_head_count: history.totals.head_count,
            peer_window_metrics: history.peer_window_metrics.clone(),
            reducer_cohort_metrics: history.reducer_cohort_metrics.clone(),
            head_eval_reports: history.head_eval_reports.clone(),
            eval_protocol_manifests: history.eval_protocol_manifests.clone(),
            history_root: Some(storage.root.clone()),
            metrics_retention: metrics_budget,
            ..BootstrapAdminState::default()
        };

        assert_eq!(
            state
                .export_receipts(&ReceiptQuery {
                    study_id: None,
                    experiment_id: None,
                    revision_id: None,
                    peer_id: None,
                })
                .len(),
            20
        );
        assert_eq!(
            state
                .export_heads(&HeadQuery {
                    study_id: None,
                    experiment_id: None,
                    revision_id: None,
                    head_id: None,
                })
                .len(),
            14
        );
        assert_eq!(
            state
                .export_head_eval_reports(&HeadId::new("head-eval-target"))
                .len(),
            9
        );

        let diagnostics = state.diagnostics(&plan(BootstrapPreset::BootstrapOnly), now, None);
        assert_eq!(diagnostics.accepted_receipts, 20);
        assert_eq!(diagnostics.certified_merges, 12);
    }

    #[test]
    fn contribution_receipt_ingest_keeps_a_bounded_preview_tail() {
        let now = Utc::now();
        let preview_limit =
            super::history::OperatorHistoryMemoryBudget::default().max_receipts_in_memory;
        let mut state = BootstrapAdminState::default();

        let receipts = (0..(preview_limit + 32))
            .map(|index| ContributionReceipt {
                receipt_id: burn_p2p_core::ContributionReceiptId::new(format!(
                    "receipt-{index:03}"
                )),
                peer_id: PeerId::new(format!("peer-{}", index % 3)),
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                base_head_id: HeadId::new("base-head"),
                artifact_id: ArtifactId::new(format!("artifact-{index:03}")),
                accepted_at: now + chrono::Duration::seconds(index as i64),
                accepted_weight: 1.0,
                metrics: BTreeMap::new(),
                merge_cert_id: None,
            })
            .collect::<Vec<_>>();

        let accepted = state.ingest_contribution_receipts(receipts);

        assert_eq!(accepted.len(), preview_limit + 32);
        assert_eq!(state.contribution_receipts.len(), preview_limit);
        assert_eq!(
            state
                .contribution_receipts
                .first()
                .map(|receipt| receipt.receipt_id.clone()),
            Some(burn_p2p_core::ContributionReceiptId::new(format!(
                "receipt-{:03}",
                32
            )))
        );
    }

    #[cfg(feature = "social")]
    #[test]
    fn leaderboard_snapshot_reads_from_durable_history_not_preview_vectors() {
        fn write_json<T: serde::Serialize>(path: PathBuf, value: &T) {
            fs::create_dir_all(path.parent().expect("history parent"))
                .expect("create history parent");
            fs::write(
                &path,
                serde_json::to_vec_pretty(value).expect("encode history record"),
            )
            .expect("write history record");
        }

        let tempdir = TempDir::new().expect("leaderboard tempdir");
        let storage = StorageConfig::new(tempdir.path());
        let now = Utc::now();
        let peer_one = PeerId::new("peer-1");
        let peer_two = PeerId::new("peer-2");
        let principal = PrincipalId::new("alice");

        let first_receipt = ContributionReceipt {
            receipt_id: burn_p2p_core::ContributionReceiptId::new("receipt-a"),
            peer_id: peer_one.clone(),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base-head"),
            artifact_id: ArtifactId::new("artifact-a"),
            accepted_at: now,
            accepted_weight: 3.0,
            metrics: BTreeMap::new(),
            merge_cert_id: Some(burn_p2p_core::MergeCertId::new("merge-a")),
        };
        let second_receipt = ContributionReceipt {
            receipt_id: burn_p2p_core::ContributionReceiptId::new("receipt-b"),
            peer_id: peer_two.clone(),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base-head"),
            artifact_id: ArtifactId::new("artifact-b"),
            accepted_at: now + chrono::Duration::seconds(1),
            accepted_weight: 2.0,
            metrics: BTreeMap::new(),
            merge_cert_id: Some(burn_p2p_core::MergeCertId::new("merge-a")),
        };
        let merge = MergeCertificate {
            merge_cert_id: burn_p2p_core::MergeCertId::new("merge-a"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base-head"),
            merged_head_id: HeadId::new("merged-head"),
            merged_artifact_id: ArtifactId::new("merged-artifact"),
            policy: burn_p2p::MergePolicy::WeightedMean,
            issued_at: now + chrono::Duration::seconds(2),
            validator: PeerId::new("validator"),
            contribution_receipts: vec![
                first_receipt.receipt_id.clone(),
                second_receipt.receipt_id.clone(),
            ],
        };

        write_json(
            storage
                .receipts_dir()
                .join(format!("{}.json", first_receipt.receipt_id.as_str())),
            &first_receipt,
        );
        write_json(
            storage
                .receipts_dir()
                .join(format!("{}.json", second_receipt.receipt_id.as_str())),
            &second_receipt,
        );
        write_json(
            storage
                .receipts_dir()
                .join(format!("merge-{}.json", merge.merge_cert_id.as_str())),
            &merge,
        );

        let report = |peer_id: &PeerId| PeerAdmissionReport {
            peer_id: peer_id.clone(),
            principal_id: principal.clone(),
            requested_scopes: BTreeSet::new(),
            trust_level: PeerTrustLevel::ScopeAuthorized,
            issuer_peer_id: PeerId::new("authority"),
            findings: Vec::new(),
            verified_at: now,
        };
        let state = BootstrapAdminState {
            history_root: Some(storage.root.clone()),
            peer_admission_reports: BTreeMap::from([
                (peer_one.clone(), report(&peer_one)),
                (peer_two.clone(), report(&peer_two)),
            ]),
            ..BootstrapAdminState::default()
        };

        let leaderboard = state.leaderboard_snapshot(
            &plan(BootstrapPreset::BootstrapOnly),
            now + chrono::Duration::seconds(5),
        );
        assert_eq!(leaderboard.entries.len(), 1);
        assert_eq!(leaderboard.entries[0].accepted_receipt_count, 2);
        assert_eq!(
            leaderboard.entries[0].identity.principal_id,
            Some(principal)
        );
    }

    #[cfg(feature = "metrics-indexer")]
    #[test]
    fn load_operator_history_recovers_metrics_from_durable_store() {
        let tempdir = TempDir::new().expect("tempdir");
        let storage = StorageConfig::new(tempdir.path());
        fs::create_dir_all(storage.metrics_indexer_dir()).expect("create metrics indexer dir");

        let peer_window_metrics = burn_p2p::PeerWindowMetrics {
            network_id: burn_p2p_core::NetworkId::new("network-metrics"),
            experiment_id: ExperimentId::new("exp-metrics"),
            revision_id: RevisionId::new("rev-metrics"),
            workload_id: WorkloadId::new("workload-metrics"),
            dataset_view_id: DatasetViewId::new("dataset-view-metrics"),
            peer_id: PeerId::new("peer-metrics"),
            principal_id: None,
            lease_id: burn_p2p::LeaseId::new("lease-metrics"),
            base_head_id: HeadId::new("head-base"),
            window_started_at: Utc::now() - chrono::Duration::seconds(5),
            window_finished_at: Utc::now() - chrono::Duration::seconds(2),
            attempted_tokens_or_samples: 128,
            accepted_tokens_or_samples: Some(128),
            local_train_loss_mean: Some(0.25),
            local_train_loss_last: Some(0.2),
            grad_or_delta_norm: Some(1.0),
            optimizer_step_count: 8,
            compute_time_ms: 1_000,
            data_fetch_time_ms: 250,
            publish_latency_ms: 50,
            head_lag_at_start: 0,
            head_lag_at_finish: 1,
            backend_class: burn_p2p_core::BackendClass::Ndarray,
            role: burn_p2p::PeerRole::TrainerCpu,
            status: burn_p2p_core::PeerWindowStatus::Completed,
            status_reason: None,
        };
        let reducer_cohort_metrics = burn_p2p::ReducerCohortMetrics {
            network_id: burn_p2p_core::NetworkId::new("network-metrics"),
            experiment_id: ExperimentId::new("exp-metrics"),
            revision_id: RevisionId::new("rev-metrics"),
            workload_id: WorkloadId::new("workload-metrics"),
            dataset_view_id: DatasetViewId::new("dataset-view-metrics"),
            merge_window_id: ContentId::new("merge-window-metrics"),
            reducer_group_id: ContentId::new("reducer-group-metrics"),
            captured_at: Utc::now() - chrono::Duration::seconds(1),
            base_head_id: HeadId::new("head-base"),
            candidate_head_id: Some(HeadId::new("head-candidate")),
            received_updates: 1,
            accepted_updates: 1,
            rejected_updates: 0,
            sum_weight: 1.0,
            accepted_tokens_or_samples: 128,
            staleness_mean: 0.5,
            staleness_max: 1.0,
            window_close_delay_ms: 20,
            cohort_duration_ms: 500,
            aggregate_norm: 0.8,
            reducer_load: 0.25,
            ingress_bytes: 1_024,
            egress_bytes: 512,
            replica_agreement: Some(1.0),
            late_arrival_count: Some(0),
            missing_peer_count: Some(0),
            rejection_reasons: BTreeMap::new(),
            status: burn_p2p_core::ReducerCohortStatus::Closed,
        };
        let head_eval_report = burn_p2p::HeadEvalReport {
            network_id: burn_p2p_core::NetworkId::new("network-metrics"),
            experiment_id: ExperimentId::new("exp-metrics"),
            revision_id: RevisionId::new("rev-metrics"),
            workload_id: WorkloadId::new("workload-metrics"),
            head_id: HeadId::new("head-candidate"),
            base_head_id: Some(HeadId::new("head-base")),
            eval_protocol_id: ContentId::new("eval-metrics"),
            evaluator_set_id: ContentId::new("validator-set-metrics"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.2))]),
            sample_count: 128,
            dataset_view_id: DatasetViewId::new("dataset-view-metrics"),
            started_at: Utc::now() - chrono::Duration::seconds(2),
            finished_at: Utc::now() - chrono::Duration::seconds(1),
            trust_class: burn_p2p_core::MetricTrustClass::Canonical,
            status: burn_p2p_core::HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        };

        let mut store = burn_p2p_metrics::MetricsStore::open(
            storage.metrics_indexer_dir(),
            burn_p2p_metrics::MetricsIndexerConfig::default(),
        )
        .expect("open metrics store");
        store
            .sync_from_records(
                std::slice::from_ref(&peer_window_metrics),
                std::slice::from_ref(&reducer_cohort_metrics),
                std::slice::from_ref(&head_eval_report),
            )
            .expect("persist metrics records");

        let history = super::load_operator_history(
            Some(&storage),
            burn_p2p::MetricsRetentionBudget::operator(),
            super::history::OperatorHistoryMemoryBudget::default(),
        )
        .expect("load operator history");
        assert_eq!(history.peer_window_metrics, vec![peer_window_metrics]);
        assert_eq!(history.reducer_cohort_metrics, vec![reducer_cohort_metrics]);
        assert_eq!(history.head_eval_reports, vec![head_eval_report]);
    }
}
