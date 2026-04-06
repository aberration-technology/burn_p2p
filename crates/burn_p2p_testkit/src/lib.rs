//! Test harnesses, fixtures, and mixed-fleet verification helpers for burn_p2p.
#![forbid(unsafe_code)]

/// Adversarial fixtures, attacks, and robustness scenario helpers.
pub mod adversarial;
/// Static browser-app wasm asset build helpers.
pub mod browser_app_assets;
/// Public APIs for merge topology.
pub mod merge_topology;
/// Public APIs for multiprocess.
pub mod multiprocess;
/// Public APIs for portal capture bundles and Playwright scenarios.
pub mod portal_capture;

use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_bootstrap::{
    AdminApiPlan, ArchivePlan, AuthorityPlan, BootstrapAdminState, BootstrapDiagnostics,
    BootstrapPlan, BootstrapPreset, BootstrapSpec,
};
use burn_p2p_checkpoint::{
    ArtifactBuildSpec, CheckpointCatalog, ChunkingScheme, EmaFlow, MergeCandidate,
    build_artifact_descriptor_from_bytes,
};
use burn_p2p_core::{
    ArtifactDescriptor, ArtifactKind, AttestationLevel, ClientPlatform, ContentId,
    ContributionReceipt, ContributionReceiptId, DatasetId, DatasetManifest, DatasetView,
    ExperimentId, GenesisSpec, HeadDescriptor, HeadId, MergeCertId, MergeCertificate, MergePolicy,
    MetricValue, NetworkId, PeerId, Precision, ProjectFamilyId, RevisionId, SignatureAlgorithm,
    SignatureMetadata, StudyId, WindowActivation, WindowId,
};
use burn_p2p_dataloader::{
    DataReceiptBuilder, DataloaderError, DatasetSizing, LeaseCache, LeasePlanner,
    LeasePlannerConfig, MicroShardPlan, MicroShardPlanner, MicroShardPlannerConfig, PlannedLease,
};
use burn_p2p_limits::{
    CapabilityCalibrator, CapabilityProbe, LimitPolicy, LimitProfile, LimitsError, LocalBackend,
};
use burn_p2p_security::{
    AuditFinding, ChallengeResponse, ClientManifest, DataAuditReport, MergeEvidence,
    MergeEvidenceDecision, MergeEvidenceRequirement, ReleasePolicy, ReputationEngine,
    ReputationObservation, ReputationPolicy, ReputationState, SecurityError, UpdateAuditReport,
    ValidatorPolicy,
};
use burn_p2p_swarm::{PeerObservation, RuntimeTransportPolicy, SwarmError, SwarmStats};
use burn_p2p_views::{
    CheckpointDagView, EmaFlowView, OperatorConsoleView, OperatorDiagnosticsView,
    OperatorPeerDiagnosticView, OperatorRobustnessSummaryView, OperatorTransferView,
    ParticipantAppView, ParticipantProfile, ShardAssignmentHeatmap, StudyBoardView,
};
use chrono::{DateTime, Duration, Utc};
use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
/// Enumerates the supported testkit error values.
pub enum TestkitError {
    #[error("bootstrap error: {0}")]
    /// Uses the bootstrap variant.
    Bootstrap(#[from] burn_p2p_bootstrap::BootstrapError),
    #[error("checkpoint error: {0}")]
    /// Uses the checkpoint variant.
    Checkpoint(#[from] burn_p2p_checkpoint::CheckpointError),
    #[error("dataloader error: {0}")]
    /// Uses the dataloader variant.
    Dataloader(#[from] DataloaderError),
    #[error("limits error: {0}")]
    /// Uses the limits variant.
    Limits(#[from] LimitsError),
    #[error("schema error: {0}")]
    /// Uses the schema variant.
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("security error: {0}")]
    /// Uses the security variant.
    Security(#[from] SecurityError),
    #[error("swarm error: {0}")]
    /// Uses the swarm variant.
    Swarm(#[from] SwarmError),
    #[error("invalid protocol requirement `{0}`")]
    /// Uses the invalid protocol requirement variant.
    InvalidProtocolRequirement(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported peer fixture modes.
pub enum PeerFixtureMode {
    /// Runs in honest native mode.
    HonestNative,
    /// Runs in honest browser mode.
    HonestBrowser,
    /// Runs in malicious mode.
    Malicious(MaliciousBehavior),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported malicious behavior values.
pub enum MaliciousBehavior {
    /// Uses the stale base head variant.
    StaleBaseHead,
    /// Uses the wrong base head variant.
    WrongBaseHead,
    /// Uses the out of lease work variant.
    OutOfLeaseWork,
    /// Uses the non finite metric variant.
    NonFiniteMetric,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser harness.
pub struct BrowserHarness {
    /// The peer IDs.
    pub peer_ids: Vec<PeerId>,
    /// The transport policy.
    pub transport_policy: RuntimeTransportPolicy,
    /// The cache namespace.
    pub cache_namespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a chaos event.
pub struct ChaosEvent {
    /// The window ID.
    pub window_id: WindowId,
    /// The fault.
    pub fault: FaultType,
    /// The peer ID.
    pub peer_id: Option<PeerId>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported fault type values.
pub enum FaultType {
    /// Uses the peer churn variant.
    PeerChurn,
    /// Uses the partition variant.
    Partition,
    /// Uses the slow peer variant.
    SlowPeer,
    /// Uses the relay loss variant.
    RelayLoss,
    /// Uses the stale head variant.
    StaleHead,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a benchmark sample.
pub struct BenchmarkSample {
    /// The name.
    pub name: String,
    /// The unit.
    pub unit: String,
    /// The value.
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a synthetic artifact scale.
pub struct SyntheticArtifactScale {
    /// The label.
    pub label: String,
    /// The bytes len.
    pub bytes_len: u64,
    /// The chunk size bytes.
    pub chunk_size_bytes: u64,
    /// The precision.
    pub precision: Precision,
    /// The record format.
    pub record_format: String,
}

impl Default for SyntheticArtifactScale {
    fn default() -> Self {
        Self {
            label: "medium".into(),
            bytes_len: 256 * 1024,
            chunk_size_bytes: 64 * 1024,
            precision: Precision::Fp16,
            record_format: "burn-record".into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a synthetic artifact generator.
pub struct SyntheticArtifactGenerator {
    /// The scale.
    pub scale: SyntheticArtifactScale,
    /// The model schema hash.
    pub model_schema_hash: ContentId,
}

impl Default for SyntheticArtifactGenerator {
    fn default() -> Self {
        Self {
            scale: SyntheticArtifactScale::default(),
            model_schema_hash: ContentId::new("synthetic-model-schema"),
        }
    }
}

impl SyntheticArtifactGenerator {
    /// Performs the bytes for label operation.
    pub fn bytes_for_label(&self, label: &str) -> Vec<u8> {
        let seed = multibyte_seed(label, &self.scale.label);
        (0..self.scale.bytes_len as usize)
            .map(|index| seed[index % seed.len()] ^ ((index & 0xff) as u8))
            .collect()
    }

    /// Builds the descriptor.
    pub fn build_descriptor(
        &self,
        kind: ArtifactKind,
        head_id: Option<HeadId>,
        base_head_id: Option<HeadId>,
        label: &str,
    ) -> Result<ArtifactDescriptor, TestkitError> {
        let spec = ArtifactBuildSpec::new(
            kind,
            self.scale.precision.clone(),
            self.model_schema_hash.clone(),
            self.scale.record_format.clone(),
        );
        let spec = if let Some(head_id) = head_id {
            spec.with_head(head_id)
        } else {
            spec
        };
        let spec = if let Some(base_head_id) = base_head_id {
            spec.with_base_head(base_head_id)
        } else {
            spec
        };

        Ok(build_artifact_descriptor_from_bytes(
            &spec,
            self.bytes_for_label(label),
            ChunkingScheme::new(self.scale.chunk_size_bytes)?,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a peer fixture.
pub struct PeerFixture {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The mode.
    pub mode: PeerFixtureMode,
    /// The limit profile.
    pub limit_profile: LimitProfile,
    /// The client manifest.
    pub client_manifest: ClientManifest,
    /// The challenge response.
    pub challenge_response: Option<ChallengeResponse>,
    /// The reputation state.
    pub reputation_state: ReputationState,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a simulation spec.
pub struct SimulationSpec {
    /// The network ID.
    pub network_id: NetworkId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The bootstrap preset.
    pub bootstrap_preset: BootstrapPreset,
    /// The peer count.
    pub peer_count: u32,
    /// The browser peer count.
    pub browser_peer_count: u32,
    /// The window count.
    pub window_count: u32,
    /// The dataset sizing.
    pub dataset_sizing: DatasetSizing,
    /// The microshard config.
    pub microshard_config: MicroShardPlannerConfig,
    /// The lease config.
    pub lease_config: LeasePlannerConfig,
    /// The limit policy.
    pub limit_policy: LimitPolicy,
    /// The artifact scale.
    pub artifact_scale: SyntheticArtifactScale,
    /// The malicious peers.
    pub malicious_peers: BTreeMap<PeerId, MaliciousBehavior>,
    /// The chaos events.
    pub chaos_events: Vec<ChaosEvent>,
}

impl Default for SimulationSpec {
    fn default() -> Self {
        Self {
            network_id: NetworkId::new("simulation"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            bootstrap_preset: BootstrapPreset::AllInOne,
            peer_count: 4,
            browser_peer_count: 1,
            window_count: 3,
            dataset_sizing: DatasetSizing {
                total_examples: 8_192,
                total_tokens: 4_194_304,
                total_bytes: 64 * 1024 * 1024,
            },
            microshard_config: MicroShardPlannerConfig::default(),
            lease_config: LeasePlannerConfig::default(),
            limit_policy: LimitPolicy::default(),
            artifact_scale: SyntheticArtifactScale::default(),
            malicious_peers: BTreeMap::new(),
            chaos_events: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a rejected update.
pub struct RejectedUpdate {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The behavior.
    pub behavior: Option<MaliciousBehavior>,
    /// The findings.
    pub findings: Vec<AuditFinding>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a simulated window.
pub struct SimulatedWindow {
    /// The window ID.
    pub window_id: WindowId,
    /// The telemetry.
    pub telemetry: Vec<burn_p2p_core::TelemetrySummary>,
    /// The accepted receipts.
    pub accepted_receipts: Vec<ContributionReceipt>,
    /// The merge certificate.
    pub merge_certificate: Option<MergeCertificate>,
    /// The data audits.
    pub data_audits: Vec<DataAuditReport>,
    /// The update audits.
    pub update_audits: Vec<UpdateAuditReport>,
    /// The rejected updates.
    pub rejected_updates: Vec<RejectedUpdate>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a simulation outcome.
pub struct SimulationOutcome {
    /// The spec.
    pub spec: SimulationSpec,
    /// The bootstrap plan.
    pub bootstrap_plan: BootstrapPlan,
    /// The browser harness.
    pub browser_harness: BrowserHarness,
    /// The peer fixtures.
    pub peer_fixtures: Vec<PeerFixture>,
    /// The dataset manifest.
    pub dataset_manifest: DatasetManifest,
    /// The dataset view.
    pub dataset_view: DatasetView,
    /// The microshard plan.
    pub microshard_plan: MicroShardPlan,
    /// The lease cache.
    pub lease_cache: LeaseCache,
    /// The checkpoint catalog.
    pub checkpoint_catalog: CheckpointCatalog,
    /// The windows.
    pub windows: Vec<SimulatedWindow>,
    /// The diagnostics.
    pub diagnostics: BootstrapDiagnostics,
    /// The operator console.
    pub operator_console: OperatorConsoleView,
    /// The participant portals.
    pub participant_portals: Vec<ParticipantAppView>,
    /// The study board.
    pub study_board: StudyBoardView,
    /// The checkpoint DAG.
    pub checkpoint_dag: CheckpointDagView,
    /// The EMA flow.
    pub ema_flow: EmaFlowView,
    /// The heatmap.
    pub heatmap: ShardAssignmentHeatmap,
    /// The benchmark samples.
    pub benchmark_samples: Vec<BenchmarkSample>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a simulation runner.
pub struct SimulationRunner {
    /// The calibrator.
    pub calibrator: CapabilityCalibrator,
    /// The microshard planner.
    pub microshard_planner: MicroShardPlanner,
    /// The lease planner.
    pub lease_planner: LeasePlanner,
    /// The validator policy.
    pub validator_policy: ValidatorPolicy,
    /// The reputation engine.
    pub reputation_engine: ReputationEngine,
    /// The artifact generator.
    pub artifact_generator: SyntheticArtifactGenerator,
}

impl Default for SimulationRunner {
    fn default() -> Self {
        let release_policy = default_release_policy().expect("default policy");
        Self {
            calibrator: CapabilityCalibrator::default(),
            microshard_planner: MicroShardPlanner::default(),
            lease_planner: LeasePlanner::default(),
            validator_policy: ValidatorPolicy {
                release_policy,
                evidence_requirement: MergeEvidenceRequirement::default(),
            },
            reputation_engine: ReputationEngine::default(),
            artifact_generator: SyntheticArtifactGenerator::default(),
        }
    }
}

impl SimulationRunner {
    /// Creates a value from the spec.
    pub fn from_spec(spec: &SimulationSpec) -> Result<Self, TestkitError> {
        let calibrator = CapabilityCalibrator::new(spec.limit_policy.clone())?;
        let microshard_planner = MicroShardPlanner::new(spec.microshard_config.clone())?;
        let lease_planner = LeasePlanner::new(spec.lease_config.clone())?;
        let release_policy = default_release_policy()?;
        let validator_policy = ValidatorPolicy {
            release_policy,
            evidence_requirement: MergeEvidenceRequirement::default(),
        };
        let reputation_engine = ReputationEngine::new(ReputationPolicy::default())?;
        let artifact_generator = SyntheticArtifactGenerator {
            scale: spec.artifact_scale.clone(),
            model_schema_hash: ContentId::new("synthetic-model-schema"),
        };

        Ok(Self {
            calibrator,
            microshard_planner,
            lease_planner,
            validator_policy,
            reputation_engine,
            artifact_generator,
        })
    }

    /// Performs the run operation.
    pub fn run(&self, spec: SimulationSpec) -> Result<SimulationOutcome, TestkitError> {
        let started_at = Utc::now();
        let bootstrap_plan = self.bootstrap_plan(&spec, started_at)?;
        let (dataset_manifest, dataset_view, microshard_plan) =
            self.dataset_registration(&spec, started_at)?;
        let mut peer_fixtures = self.peer_fixtures(&spec, &bootstrap_plan, started_at)?;
        let browser_harness = BrowserHarness {
            peer_ids: peer_fixtures
                .iter()
                .filter(|fixture| fixture.limit_profile.card.platform == ClientPlatform::Browser)
                .map(|fixture| fixture.peer_id.clone())
                .collect(),
            transport_policy: RuntimeTransportPolicy::browser(),
            cache_namespace: format!("burn-p2p-cache-{}", spec.network_id.as_str()),
        };

        let mut lease_cache = LeaseCache::default();
        let mut checkpoint_catalog = CheckpointCatalog::default();
        let mut admin_state = BootstrapAdminState::default();
        let mut alerts = Vec::new();
        let mut authority_actions = Vec::new();
        let mut windows = Vec::new();
        let mut all_leases = Vec::new();
        let mut current_base_head =
            self.insert_genesis_head(&spec, &mut checkpoint_catalog, started_at)?;
        let mut ema_flow = EmaFlow::new(0.999)?;

        for window_ordinal in 0..spec.window_count {
            let window_id = WindowId(u64::from(window_ordinal) + 1);
            let granted_at = started_at + Duration::seconds(i64::from(window_ordinal) * 60);
            let mut accepted_receipts = Vec::new();
            let mut window_telemetry = Vec::new();
            let mut data_audits = Vec::new();
            let mut update_audits = Vec::new();
            let mut rejected_updates = Vec::new();

            apply_chaos_events(
                &spec.chaos_events,
                window_id,
                &mut admin_state,
                &mut alerts,
                granted_at,
            );

            for fixture in &mut peer_fixtures {
                let planned_lease = self.lease_planner.plan_lease(
                    spec.network_id.clone(),
                    spec.study_id.clone(),
                    spec.experiment_id.clone(),
                    spec.revision_id.clone(),
                    &dataset_view,
                    fixture.peer_id.clone(),
                    window_id,
                    granted_at,
                    fixture.limit_profile.recommended_budget.budget_work_units,
                    &microshard_plan.microshards,
                )?;
                all_leases.push(planned_lease.lease.clone());
                lease_cache.insert(planned_lease.lease.clone());

                let simulated = self.simulate_peer_window(
                    &spec,
                    fixture,
                    &planned_lease,
                    current_base_head.clone(),
                    checkpoint_catalog.head(&current_base_head)?.global_step + 1,
                    granted_at,
                )?;

                admin_state.peer_store.upsert(simulated.observation.clone());
                window_telemetry.push(simulated.telemetry.clone());
                data_audits.push(simulated.data_audit.clone());

                if let Some(update_audit) = simulated.update_audit.clone() {
                    update_audits.push(update_audit.clone());
                }

                if simulated.accepted {
                    checkpoint_catalog.insert_artifact(simulated.artifact.clone())?;
                    checkpoint_catalog.insert_head(simulated.head.clone())?;
                    admin_state
                        .contribution_receipts
                        .push(simulated.contribution_receipt.clone());
                    accepted_receipts.push(simulated.contribution_receipt.clone());
                    authority_actions.push(burn_p2p_views::AuthorityActionRecord {
                        action: "accepted-window-update".into(),
                        actor_peer_id: Some(fixture.peer_id.clone()),
                        happened_at: granted_at,
                    });
                } else {
                    rejected_updates.push(RejectedUpdate {
                        peer_id: fixture.peer_id.clone(),
                        behavior: fixture.mode.malicious_behavior(),
                        findings: simulated.rejection_findings,
                    });
                    alerts.push(burn_p2p_swarm::AlertNotice {
                        overlay: burn_p2p_swarm::OverlayTopic::control(spec.network_id.clone()),
                        peer_id: Some(fixture.peer_id.clone()),
                        severity: burn_p2p_swarm::AlertSeverity::Warn,
                        code: "rejected-update".into(),
                        message: "validator rejected simulated contribution".into(),
                        emitted_at: granted_at,
                    });
                }
            }

            let merge_certificate = if accepted_receipts.is_empty() {
                None
            } else {
                let merge = checkpoint_catalog.plan_merge(MergeCandidate {
                    study_id: spec.study_id.clone(),
                    experiment_id: spec.experiment_id.clone(),
                    revision_id: spec.revision_id.clone(),
                    base_head_id: current_base_head.clone(),
                    policy: MergePolicy::QualityWeightedEma,
                    contributions: accepted_receipts.clone(),
                })?;
                let merged_head_id = HeadId::new(format!("merged-window-{}", window_id.0));
                let merged_artifact = self.artifact_generator.build_descriptor(
                    ArtifactKind::ServeHead,
                    Some(merged_head_id.clone()),
                    Some(current_base_head.clone()),
                    &format!("merged-window-{}", window_id.0),
                )?;
                let merged_head = HeadDescriptor {
                    head_id: merged_head_id.clone(),
                    study_id: spec.study_id.clone(),
                    experiment_id: spec.experiment_id.clone(),
                    revision_id: spec.revision_id.clone(),
                    artifact_id: merged_artifact.artifact_id.clone(),
                    parent_head_id: Some(current_base_head.clone()),
                    global_step: checkpoint_catalog.head(&current_base_head)?.global_step + 1,
                    created_at: granted_at + Duration::seconds(45),
                    metrics: merge
                        .aggregated_numeric_metrics
                        .iter()
                        .map(|(key, value)| (key.clone(), MetricValue::Float(*value)))
                        .collect(),
                };
                checkpoint_catalog.insert_artifact(merged_artifact.clone())?;
                checkpoint_catalog.insert_head(merged_head.clone())?;
                let merge_certificate = MergeCertificate {
                    merge_cert_id: MergeCertId::new(format!("merge-window-{}", window_id.0)),
                    study_id: spec.study_id.clone(),
                    experiment_id: spec.experiment_id.clone(),
                    revision_id: spec.revision_id.clone(),
                    base_head_id: current_base_head.clone(),
                    merged_head_id: merged_head_id.clone(),
                    merged_artifact_id: merged_artifact.artifact_id.clone(),
                    policy: merge.policy,
                    issued_at: granted_at + Duration::seconds(50),
                    validator: PeerId::new("validator"),
                    contribution_receipts: merge.contribution_receipt_ids,
                };
                current_base_head = merged_head_id.clone();
                ema_flow.include_certified_head(merged_head_id);
                admin_state
                    .merge_certificates
                    .push(merge_certificate.clone());
                authority_actions.push(burn_p2p_views::AuthorityActionRecord {
                    action: "promote-certified-head".into(),
                    actor_peer_id: Some(PeerId::new("validator")),
                    happened_at: merge_certificate.issued_at,
                });
                Some(merge_certificate)
            };

            windows.push(SimulatedWindow {
                window_id,
                telemetry: window_telemetry,
                accepted_receipts,
                merge_certificate,
                data_audits,
                update_audits,
                rejected_updates,
            });
        }

        admin_state
            .quarantined_peers
            .extend(peer_fixtures.iter().filter_map(|fixture| {
                match self
                    .reputation_engine
                    .decision(fixture.reputation_state.score)
                {
                    burn_p2p_security::ReputationDecision::Quarantine => {
                        Some(fixture.peer_id.clone())
                    }
                    _ => None,
                }
            }));
        admin_state
            .banned_peers
            .extend(peer_fixtures.iter().filter_map(|fixture| {
                match self
                    .reputation_engine
                    .decision(fixture.reputation_state.score)
                {
                    burn_p2p_security::ReputationDecision::Ban => Some(fixture.peer_id.clone()),
                    _ => None,
                }
            }));

        let diagnostics = admin_state.diagnostics(
            &bootstrap_plan,
            started_at + Duration::seconds(i64::from(spec.window_count) * 60),
            Some(
                peer_fixtures
                    .iter()
                    .map(|fixture| fixture.limit_profile.recommended_budget.budget_work_units)
                    .sum::<u64>(),
            ),
        );

        let overlays = build_overlay_statuses(&spec, &windows, admin_state.peer_store.stats(None))?;
        let merge_queue = build_merge_queue_entries(&windows);
        let operator_console = OperatorConsoleView::new(
            operator_diagnostics_view(&diagnostics),
            overlays,
            merge_queue,
            authority_actions,
            alerts,
        );
        let checkpoint_dag = CheckpointDagView::from_heads_and_merges(
            checkpoint_catalog.heads.values().cloned().collect(),
            admin_state.merge_certificates.clone(),
        );
        let ema_flow_view =
            EmaFlowView::from_merge_certificates(admin_state.merge_certificates.clone());
        let heatmap = ShardAssignmentHeatmap::from_leases(all_leases.clone());
        let study_board = build_study_board(&spec, &windows);
        let participant_portals =
            build_participant_portals(&peer_fixtures, &lease_cache, &windows, &checkpoint_catalog);
        let benchmark_samples =
            build_benchmark_samples(&spec, &windows, &microshard_plan, &heatmap, &diagnostics);

        Ok(SimulationOutcome {
            spec,
            bootstrap_plan,
            browser_harness,
            peer_fixtures,
            dataset_manifest,
            dataset_view,
            microshard_plan,
            lease_cache,
            checkpoint_catalog,
            windows,
            diagnostics,
            operator_console,
            participant_portals,
            study_board,
            checkpoint_dag,
            ema_flow: ema_flow_view,
            heatmap,
            benchmark_samples,
        })
    }

    fn bootstrap_plan(
        &self,
        spec: &SimulationSpec,
        now: DateTime<Utc>,
    ) -> Result<BootstrapPlan, TestkitError> {
        let genesis = GenesisSpec {
            network_id: spec.network_id.clone(),
            protocol_version: Version::new(0, 1, 0),
            display_name: format!("{} testnet", spec.network_id.as_str()),
            created_at: now,
            metadata: BTreeMap::new(),
        };
        let authority = AuthorityPlan {
            release_policy: self.validator_policy.release_policy.clone(),
            validator_policy: self.validator_policy.clone(),
        };

        Ok(BootstrapSpec {
            preset: spec.bootstrap_preset.clone(),
            genesis,
            platform: ClientPlatform::Native,
            bootstrap_addresses: vec![burn_p2p_swarm::SwarmAddress::new(
                "/dns4/bootstrap.example.com/tcp/4001/ws",
            )?],
            listen_addresses: vec![burn_p2p_swarm::SwarmAddress::new(
                "/ip4/0.0.0.0/udp/4001/quic-v1",
            )?],
            authority: Some(authority),
            archive: ArchivePlan {
                pinned_heads: BTreeSet::new(),
                pinned_artifacts: BTreeSet::new(),
                retain_contribution_receipts: true,
            },
            admin_api: AdminApiPlan::default(),
        }
        .plan()?)
    }

    fn dataset_registration(
        &self,
        spec: &SimulationSpec,
        now: DateTime<Utc>,
    ) -> Result<(DatasetManifest, DatasetView, MicroShardPlan), TestkitError> {
        let manifest = DatasetManifest {
            dataset_id: DatasetId::new("dataset"),
            source_uri: "hf://synthetic/simulation".into(),
            format: "parquet".into(),
            manifest_hash: ContentId::new("dataset-manifest"),
            metadata: BTreeMap::from([("created_at".into(), now.to_rfc3339())]),
        };
        let view = DatasetView {
            dataset_view_id: burn_p2p_core::DatasetViewId::new("dataset-view"),
            dataset_id: manifest.dataset_id.clone(),
            preprocessing_hash: ContentId::new("preprocessing-hash"),
            tokenizer_hash: Some(ContentId::new("tokenizer-hash")),
            manifest_hash: manifest.manifest_hash.clone(),
            metadata: BTreeMap::new(),
        };
        let microshard_plan = self
            .microshard_planner
            .plan(&view, spec.dataset_sizing.clone())?;

        Ok((manifest, view, microshard_plan))
    }

    fn peer_fixtures(
        &self,
        spec: &SimulationSpec,
        bootstrap_plan: &BootstrapPlan,
        now: DateTime<Utc>,
    ) -> Result<Vec<PeerFixture>, TestkitError> {
        let mut fixtures = Vec::new();

        for index in 0..spec.peer_count {
            let peer_id = PeerId::new(format!("peer-{index}"));
            let browser = index < spec.browser_peer_count;
            let mode = if let Some(behavior) = spec.malicious_peers.get(&peer_id) {
                PeerFixtureMode::Malicious(behavior.clone())
            } else if browser {
                PeerFixtureMode::HonestBrowser
            } else {
                PeerFixtureMode::HonestNative
            };

            let probe = CapabilityProbe {
                peer_id: peer_id.clone(),
                platform: if browser {
                    ClientPlatform::Browser
                } else {
                    ClientPlatform::Native
                },
                available_backends: if browser {
                    vec![LocalBackend::Wgpu]
                } else if index % 2 == 0 {
                    vec![LocalBackend::Cuda, LocalBackend::Wgpu]
                } else {
                    vec![LocalBackend::Cpu, LocalBackend::Ndarray]
                },
                device_memory_bytes: (!browser).then_some(8 * 1024 * 1024 * 1024),
                system_memory_bytes: 32 * 1024 * 1024 * 1024,
                disk_bytes: if browser {
                    8 * 1024 * 1024 * 1024
                } else {
                    256 * 1024 * 1024 * 1024
                },
                upload_mbps: if browser {
                    15.0
                } else {
                    100.0 + index as f32 * 10.0
                },
                download_mbps: if browser {
                    25.0
                } else {
                    200.0 + index as f32 * 10.0
                },
                persistence: if browser {
                    burn_p2p_core::PersistenceClass::Session
                } else {
                    burn_p2p_core::PersistenceClass::Durable
                },
                attestation_level: if browser {
                    AttestationLevel::Manifest
                } else {
                    AttestationLevel::Challenge
                },
                work_units_per_second: if browser {
                    18.0
                } else {
                    80.0 + f64::from(index) * 25.0
                },
                benchmark_hash: None,
            };

            let limit_profile = self.calibrator.calibrate(probe, now)?;
            let target_artifact_id = if browser {
                "browser-wasm"
            } else {
                "native-linux-x86_64"
            };
            let target_artifact_hash = if browser {
                ContentId::new("approved-artifact-browser")
            } else {
                ContentId::new("approved-artifact-native")
            };
            let client_manifest = ClientManifest::new(
                peer_id.clone(),
                ProjectFamilyId::new("synthetic-family"),
                ContentId::new("synthetic-train"),
                target_artifact_hash,
                target_artifact_id,
                Version::new(0, 1, 1),
                bootstrap_plan.genesis.protocol_version.clone(),
                ContentId::new("approved-build"),
                None,
                limit_profile
                    .card
                    .preferred_backends
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                limit_profile.card.platform.clone(),
                Some(ContentId::new(format!("binary-{index}"))),
                browser.then_some(ContentId::new(format!("wasm-{index}"))),
                now,
            )?;
            let challenge_response = if browser {
                None
            } else {
                Some(ChallengeResponse {
                    peer_id: peer_id.clone(),
                    nonce_hash: ContentId::new(format!("nonce-{index}")),
                    response_hash: ContentId::new(format!("response-{index}")),
                    answered_at: now,
                    signature: SignatureMetadata {
                        signer: peer_id.clone(),
                        key_id: format!("peer-key-{index}"),
                        algorithm: SignatureAlgorithm::Ed25519,
                        signed_at: now,
                        signature_hex: format!("{index:02x}{index:02x}"),
                    },
                })
            };

            fixtures.push(PeerFixture {
                peer_id: peer_id.clone(),
                mode,
                limit_profile,
                client_manifest,
                challenge_response,
                reputation_state: ReputationState::new(peer_id, now),
            });
        }

        Ok(fixtures)
    }

    fn insert_genesis_head(
        &self,
        spec: &SimulationSpec,
        catalog: &mut CheckpointCatalog,
        now: DateTime<Utc>,
    ) -> Result<HeadId, TestkitError> {
        let head_id = HeadId::new("genesis-head");
        let artifact = self.artifact_generator.build_descriptor(
            ArtifactKind::FullHead,
            Some(head_id.clone()),
            None,
            "genesis-head",
        )?;
        catalog.insert_artifact(artifact.clone())?;
        catalog.insert_head(HeadDescriptor {
            head_id: head_id.clone(),
            study_id: spec.study_id.clone(),
            experiment_id: spec.experiment_id.clone(),
            revision_id: spec.revision_id.clone(),
            artifact_id: artifact.artifact_id,
            parent_head_id: None,
            global_step: 0,
            created_at: now,
            metrics: BTreeMap::from([("loss".into(), MetricValue::Float(1.0))]),
        })?;
        Ok(head_id)
    }

    fn simulate_peer_window(
        &self,
        spec: &SimulationSpec,
        fixture: &mut PeerFixture,
        planned_lease: &PlannedLease,
        current_base_head: HeadId,
        global_step: u64,
        _now: DateTime<Utc>,
    ) -> Result<SimulatedPeerWindow, TestkitError> {
        let behavior = fixture.mode.malicious_behavior();
        let mut completed_at = planned_lease.lease.granted_at + Duration::seconds(30);
        if completed_at > planned_lease.lease.expires_at {
            completed_at = planned_lease.lease.expires_at;
        }
        let mut data_receipt = DataReceiptBuilder::accepted(
            planned_lease.selection.estimated_examples,
            planned_lease.selection.estimated_tokens,
        )
        .build(&planned_lease.lease, completed_at)?;

        if matches!(behavior, Some(MaliciousBehavior::OutOfLeaseWork)) {
            data_receipt
                .microshards
                .push(burn_p2p_core::MicroShardId::new("unexpected-microshard"));
        }

        let data_audit = self.validator_policy.audit_data_receipt(
            &planned_lease.lease,
            &data_receipt,
            completed_at,
        );

        let base_head_for_receipt = match behavior {
            Some(MaliciousBehavior::WrongBaseHead) => HeadId::new("wrong-base-head"),
            Some(MaliciousBehavior::StaleBaseHead) => HeadId::new("genesis-head"),
            _ => current_base_head.clone(),
        };
        let head_id = HeadId::new(format!(
            "head-{}-{}",
            fixture.peer_id.as_str(),
            planned_lease.lease.window_id.0
        ));
        let artifact = self.artifact_generator.build_descriptor(
            ArtifactKind::DeltaPack,
            Some(head_id.clone()),
            Some(base_head_for_receipt.clone()),
            &format!(
                "delta-{}-{}",
                fixture.peer_id.as_str(),
                planned_lease.lease.window_id.0
            ),
        )?;
        let mut metrics = BTreeMap::from([
            (
                "loss".into(),
                MetricValue::Float(1.0 / (planned_lease.lease.window_id.0 as f64 + 1.0)),
            ),
            (
                "throughput".into(),
                MetricValue::Float(fixture.limit_profile.card.work_units_per_second),
            ),
        ]);
        if matches!(behavior, Some(MaliciousBehavior::NonFiniteMetric)) {
            metrics.insert("loss".into(), MetricValue::Float(f64::NAN));
        }

        let contribution_receipt = ContributionReceipt {
            receipt_id: ContributionReceiptId::new(format!(
                "receipt-{}-{}",
                fixture.peer_id.as_str(),
                planned_lease.lease.window_id.0
            )),
            peer_id: fixture.peer_id.clone(),
            study_id: spec.study_id.clone(),
            experiment_id: spec.experiment_id.clone(),
            revision_id: spec.revision_id.clone(),
            base_head_id: base_head_for_receipt.clone(),
            artifact_id: artifact.artifact_id.clone(),
            accepted_at: completed_at,
            accepted_weight: 1.0,
            metrics,
            merge_cert_id: None,
        };
        let update_audit = self
            .validator_policy
            .audit_update_receipt(&contribution_receipt, completed_at);

        let evidence = MergeEvidence {
            peer_id: fixture.peer_id.clone(),
            client_manifest: fixture.client_manifest.clone(),
            capability_card: fixture.limit_profile.card.clone(),
            challenge_response: fixture.challenge_response.clone(),
            data_receipt: Some(data_receipt.clone()),
            holdout_metrics: BTreeMap::from([("holdout_loss".into(), MetricValue::Float(0.5))]),
            reputation_score: fixture.reputation_state.score,
        };
        let evidence_decision = self
            .validator_policy
            .evaluate_merge_evidence(&contribution_receipt, &evidence);
        let mut rejection_findings = data_audit.findings.clone();
        rejection_findings.extend(update_audit.findings.clone());
        if base_head_for_receipt != current_base_head {
            rejection_findings.push(AuditFinding::BaseHeadMismatch {
                expected: current_base_head.clone(),
                found: base_head_for_receipt.clone(),
            });
        }
        if let MergeEvidenceDecision::Insufficient(findings) = evidence_decision {
            rejection_findings.extend(findings);
        }

        let accepted = rejection_findings.is_empty();
        let warning_count = rejection_findings.len() as u32;
        let accepted_work_units = if accepted {
            planned_lease.lease.budget_work_units
        } else {
            0
        };
        let _decision = self.reputation_engine.apply(
            &mut fixture.reputation_state,
            ReputationObservation {
                accepted_work_units,
                warning_count,
                failure_count: u32::from(!accepted),
                last_control_cert_id: None,
                last_merge_cert_id: None,
            },
            completed_at,
        );

        let head = HeadDescriptor {
            head_id: head_id.clone(),
            study_id: spec.study_id.clone(),
            experiment_id: spec.experiment_id.clone(),
            revision_id: spec.revision_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            parent_head_id: Some(base_head_for_receipt.clone()),
            global_step,
            created_at: completed_at,
            metrics: BTreeMap::from([(
                "loss".into(),
                contribution_receipt
                    .metrics
                    .get("loss")
                    .cloned()
                    .unwrap_or(MetricValue::Float(0.0)),
            )]),
        };
        let telemetry = burn_p2p_core::TelemetrySummary {
            network_id: spec.network_id.clone(),
            study_id: spec.study_id.clone(),
            experiment_id: spec.experiment_id.clone(),
            revision_id: spec.revision_id.clone(),
            window_id: planned_lease.lease.window_id,
            active_peers: spec.peer_count,
            accepted_contributions: u64::from(accepted),
            throughput_work_units_per_second: fixture.limit_profile.card.work_units_per_second,
            network: burn_p2p_core::NetworkEstimate {
                connected_peers: spec.peer_count,
                observed_peers: u64::from(spec.peer_count),
                estimated_network_size: f64::from(spec.peer_count),
                estimated_total_vram_bytes: fixture
                    .limit_profile
                    .card
                    .device_memory_bytes
                    .map(u128::from),
                estimated_total_flops: Some(
                    fixture.limit_profile.card.work_units_per_second * 100.0,
                ),
                eta_lower_seconds: Some(30),
                eta_upper_seconds: Some(60),
            },
            metrics: BTreeMap::from([
                (
                    "estimated_flops".into(),
                    MetricValue::Float(fixture.limit_profile.card.work_units_per_second * 100.0),
                ),
                ("accepted".into(), MetricValue::Bool(accepted)),
            ]),
            captured_at: completed_at,
        };
        let observation = PeerObservation::new(fixture.peer_id.clone(), completed_at)
            .with_capability_card(fixture.limit_profile.card.clone())
            .with_telemetry(telemetry.clone());

        Ok(SimulatedPeerWindow {
            observation,
            telemetry,
            data_audit,
            update_audit: Some(update_audit),
            artifact,
            head,
            contribution_receipt,
            accepted,
            rejection_findings,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct SimulatedPeerWindow {
    observation: PeerObservation,
    telemetry: burn_p2p_core::TelemetrySummary,
    data_audit: DataAuditReport,
    update_audit: Option<UpdateAuditReport>,
    artifact: ArtifactDescriptor,
    head: HeadDescriptor,
    contribution_receipt: ContributionReceipt,
    accepted: bool,
    rejection_findings: Vec<AuditFinding>,
}

fn default_release_policy() -> Result<ReleasePolicy, TestkitError> {
    let mut release_policy = ReleasePolicy::new(
        Version::new(0, 1, 0),
        vec![
            VersionReq::parse("^0.1")
                .map_err(|_| TestkitError::InvalidProtocolRequirement("^0.1".into()))?,
        ],
    )?;
    release_policy.required_project_family_id = Some(ProjectFamilyId::new("synthetic-family"));
    release_policy.required_release_train_hash = Some(ContentId::new("synthetic-train"));
    release_policy
        .allowed_target_artifact_hashes
        .insert(ContentId::new("approved-artifact-native"));
    release_policy
        .allowed_target_artifact_hashes
        .insert(ContentId::new("approved-artifact-browser"));
    release_policy
        .approved_build_hashes
        .insert(ContentId::new("approved-build"));
    Ok(release_policy)
}

fn build_overlay_statuses(
    spec: &SimulationSpec,
    windows: &[SimulatedWindow],
    swarm: SwarmStats,
) -> Result<Vec<burn_p2p_views::OverlayStatusView>, TestkitError> {
    let overlays = burn_p2p_swarm::ExperimentOverlaySet::new(
        spec.network_id.clone(),
        spec.study_id.clone(),
        spec.experiment_id.clone(),
    )?;
    let last_window_id = windows.last().map(|window| window.window_id);

    Ok(vec![
        burn_p2p_views::OverlayStatusView {
            overlay: overlays.heads,
            active_peers: swarm.connected_peers,
            connected_peers: swarm.connected_peers,
            last_window_id,
        },
        burn_p2p_views::OverlayStatusView {
            overlay: overlays.leases,
            active_peers: swarm.connected_peers,
            connected_peers: swarm.connected_peers,
            last_window_id,
        },
        burn_p2p_views::OverlayStatusView {
            overlay: overlays.telemetry,
            active_peers: swarm.connected_peers,
            connected_peers: swarm.connected_peers,
            last_window_id,
        },
    ])
}

fn build_merge_queue_entries(windows: &[SimulatedWindow]) -> Vec<burn_p2p_views::MergeQueueEntry> {
    windows
        .iter()
        .map(|window| burn_p2p_views::MergeQueueEntry {
            base_head_id: window
                .accepted_receipts
                .first()
                .map(|receipt| receipt.base_head_id.clone())
                .unwrap_or_else(|| HeadId::new(format!("window-{}-base", window.window_id.0))),
            candidate_receipt_ids: window
                .accepted_receipts
                .iter()
                .map(|receipt| receipt.receipt_id.clone())
                .collect(),
            status: if window.merge_certificate.is_some() {
                burn_p2p_views::MergeQueueStatus::Certified
            } else if window.accepted_receipts.is_empty() {
                burn_p2p_views::MergeQueueStatus::Rejected
            } else {
                burn_p2p_views::MergeQueueStatus::Pending
            },
            merged_head_id: window
                .merge_certificate
                .as_ref()
                .map(|certificate| certificate.merged_head_id.clone()),
            merged_artifact_id: window
                .merge_certificate
                .as_ref()
                .map(|certificate| certificate.merged_artifact_id.clone()),
            merge_cert_id: window
                .merge_certificate
                .as_ref()
                .map(|certificate| certificate.merge_cert_id.clone()),
        })
        .collect()
}

fn build_study_board(spec: &SimulationSpec, windows: &[SimulatedWindow]) -> StudyBoardView {
    let variant = burn_p2p_views::ExperimentVariantView {
        study_id: spec.study_id.clone(),
        experiment_id: spec.experiment_id.clone(),
        revision_id: spec.revision_id.clone(),
        metrics: windows
            .iter()
            .map(|window| burn_p2p_views::MetricPoint {
                window_id: window.window_id,
                metrics: BTreeMap::from([
                    (
                        "accepted_contributions".into(),
                        MetricValue::Integer(window.accepted_receipts.len() as i64),
                    ),
                    (
                        "rejected_updates".into(),
                        MetricValue::Integer(window.rejected_updates.len() as i64),
                    ),
                ]),
                captured_at: window
                    .telemetry
                    .first()
                    .map(|telemetry| telemetry.captured_at)
                    .unwrap_or_else(Utc::now),
            })
            .collect(),
        accepted_work: windows
            .iter()
            .map(|window| window.accepted_receipts.len() as u64)
            .sum(),
        cost_performance: vec![burn_p2p_views::CostPerformancePoint {
            label: "accepted-receipts-per-window".into(),
            cost: windows.len() as f64,
            value: windows
                .iter()
                .map(|window| window.accepted_receipts.len() as f64)
                .sum::<f64>()
                / windows.len().max(1) as f64,
        }],
    };

    let migrations = if windows.len() > 1 {
        vec![burn_p2p_views::ExperimentMigrationView {
            experiment_id: spec.experiment_id.clone(),
            from_revision_id: spec.revision_id.clone(),
            to_revision_id: spec.revision_id.clone(),
            activation: WindowActivation {
                activation_window: WindowId(2),
                grace_windows: 1,
            },
            note: Some("simulated steady-state runtime patch boundary".into()),
        }]
    } else {
        Vec::new()
    };

    StudyBoardView::new(
        spec.network_id.clone(),
        spec.study_id.clone(),
        vec![variant],
        migrations,
    )
}

fn build_participant_portals(
    peer_fixtures: &[PeerFixture],
    lease_cache: &LeaseCache,
    windows: &[SimulatedWindow],
    checkpoint_catalog: &CheckpointCatalog,
) -> Vec<ParticipantAppView> {
    peer_fixtures
        .iter()
        .map(|fixture| {
            let current_assignment = windows
                .last()
                .and_then(|window| lease_cache.get(window.window_id, &fixture.peer_id).cloned());
            let local_telemetry = windows
                .iter()
                .rev()
                .flat_map(|window| &window.telemetry)
                .find(|telemetry| telemetry.metrics.contains_key("accepted"))
                .cloned();
            let accepted_receipts = windows
                .iter()
                .flat_map(|window| &window.accepted_receipts)
                .filter(|receipt| receipt.peer_id == fixture.peer_id)
                .cloned()
                .collect::<Vec<_>>();
            let checkpoint_downloads = checkpoint_catalog
                .heads
                .values()
                .filter(|head| head.parent_head_id.is_some())
                .map(|head| burn_p2p_views::CheckpointDownload {
                    head_id: head.head_id.clone(),
                    artifact_id: head.artifact_id.clone(),
                    label: format!("head-{}", head.global_step),
                    download_path: format!("/artifacts/{}", head.artifact_id.as_str()),
                })
                .collect::<Vec<_>>();

            ParticipantAppView::new(
                ParticipantProfile {
                    peer_id: fixture.peer_id.clone(),
                    display_name: Some(fixture.peer_id.as_str().into()),
                    github: None,
                },
                current_assignment,
                local_telemetry,
                accepted_receipts.clone(),
                accepted_receipts
                    .last()
                    .map(|receipt| receipt.artifact_id.clone()),
                checkpoint_downloads,
            )
        })
        .collect()
}

fn build_benchmark_samples(
    spec: &SimulationSpec,
    windows: &[SimulatedWindow],
    microshard_plan: &MicroShardPlan,
    heatmap: &ShardAssignmentHeatmap,
    diagnostics: &BootstrapDiagnostics,
) -> Vec<BenchmarkSample> {
    vec![
        BenchmarkSample {
            name: "cold_start_sync_time".into(),
            unit: "seconds".into(),
            value: windows.len() as f64 * 2.0,
        },
        BenchmarkSample {
            name: "delta_publish_latency".into(),
            unit: "milliseconds".into(),
            value: 125.0,
        },
        BenchmarkSample {
            name: "merge_throughput".into(),
            unit: "certified_heads_per_window".into(),
            value: windows
                .iter()
                .filter(|window| window.merge_certificate.is_some())
                .count() as f64,
        },
        BenchmarkSample {
            name: "shard_planning_latency".into(),
            unit: "microshards".into(),
            value: microshard_plan.microshards.len() as f64,
        },
        BenchmarkSample {
            name: "eta_error".into(),
            unit: "seconds".into(),
            value: diagnostics
                .swarm
                .network_estimate
                .eta_upper_seconds
                .zip(diagnostics.swarm.network_estimate.eta_lower_seconds)
                .map(|(upper, lower)| upper.saturating_sub(lower) as f64)
                .unwrap_or_default(),
        },
        BenchmarkSample {
            name: "ui_event_throughput".into(),
            unit: "views".into(),
            value: 4.0 + heatmap.cells.len() as f64,
        },
        BenchmarkSample {
            name: "accepted_receipt_count".into(),
            unit: "receipts".into(),
            value: windows
                .iter()
                .map(|window| window.accepted_receipts.len() as f64)
                .sum(),
        },
        BenchmarkSample {
            name: "configured_peer_count".into(),
            unit: "peers".into(),
            value: spec.peer_count as f64,
        },
    ]
}

fn operator_diagnostics_view(diagnostics: &BootstrapDiagnostics) -> OperatorDiagnosticsView {
    let robustness_panel = diagnostics.robustness_panel.clone();
    let robustness_summary = diagnostics
        .robustness_rollup
        .as_ref()
        .map(|rollup| OperatorRobustnessSummaryView {
            rejected_updates: u64::from(rollup.rejected_updates),
            mean_trust_score: rollup.mean_trust_score,
            quarantined_peer_count: rollup.quarantined_peer_count,
            ban_recommended_peer_count: rollup.ban_recommended_peer_count,
            canary_regression_count: rollup.canary_regression_count,
            alert_count: rollup.alert_count,
        })
        .or_else(|| {
            robustness_panel
                .as_ref()
                .map(|panel| OperatorRobustnessSummaryView {
                    rejected_updates: panel
                        .rejection_reasons
                        .iter()
                        .map(|reason| u64::from(reason.count))
                        .sum(),
                    mean_trust_score: if panel.trust_scores.is_empty() {
                        0.0
                    } else {
                        panel
                            .trust_scores
                            .iter()
                            .map(|score| score.score)
                            .sum::<f64>()
                            / panel.trust_scores.len() as f64
                    },
                    quarantined_peer_count: panel.quarantined_peers.len() as u32,
                    ban_recommended_peer_count: panel
                        .quarantined_peers
                        .iter()
                        .filter(|peer| peer.ban_recommended)
                        .count() as u32,
                    canary_regression_count: panel.canary_regressions.len() as u32,
                    alert_count: panel.alerts.len() as u32,
                })
        });

    OperatorDiagnosticsView {
        network_id: diagnostics.network_id.clone(),
        preset_label: format!("{:?}", diagnostics.preset),
        active_services: diagnostics
            .services
            .iter()
            .map(|service| format!("{service:?}"))
            .collect(),
        roles: diagnostics.roles.clone(),
        connected_peers: diagnostics.swarm.connected_peers,
        connected_peer_ids: diagnostics.swarm.connected_peer_ids.clone(),
        observed_peers: diagnostics.swarm.observed_peers.clone(),
        network_estimate: diagnostics.swarm.network_estimate.clone(),
        pinned_heads: diagnostics.pinned_heads.clone(),
        pinned_artifacts: diagnostics.pinned_artifacts.clone(),
        accepted_receipts: diagnostics.accepted_receipts as usize,
        certified_merges: diagnostics.certified_merges as usize,
        in_flight_transfers: diagnostics
            .in_flight_transfers
            .iter()
            .map(|transfer| OperatorTransferView {
                artifact_id: transfer.artifact_id.clone(),
                phase_label: format!("{:?}", transfer.phase),
                source_peers: transfer.source_peers.clone(),
                provider_peer_id: transfer.provider_peer_id.clone(),
                completed_chunk_count: transfer.completed_chunks.len(),
                total_chunk_count: transfer.descriptor.as_ref().map(|d| d.chunks.len()),
                started_at: transfer.started_at,
                updated_at: transfer.updated_at,
            })
            .collect(),
        admitted_peers: diagnostics.admitted_peers.clone(),
        peer_diagnostics: diagnostics
            .peer_diagnostics
            .iter()
            .map(|peer| OperatorPeerDiagnosticView {
                peer_id: peer.peer_id.clone(),
                connected: peer.connected,
                observed_at: peer.observed_at,
                trust_level: peer.trust_level.clone(),
                rejection_reason: peer.rejection_reason.clone(),
                reputation_score: peer.reputation_score,
                reputation_decision: peer.reputation_decision.clone(),
                quarantined: peer.quarantined,
                banned: peer.banned,
            })
            .collect(),
        rejected_peers: diagnostics.rejected_peers.clone(),
        quarantined_peers: diagnostics.quarantined_peers.clone(),
        banned_peers: diagnostics.banned_peers.clone(),
        robustness_summary,
        robustness_panel,
        minimum_revocation_epoch: diagnostics.minimum_revocation_epoch,
        last_error: diagnostics.last_error.clone(),
        node_state_label: format!("{:?}", diagnostics.node_state),
        slot_state_labels: diagnostics
            .slot_states
            .iter()
            .map(|state| format!("{state:?}"))
            .collect(),
        captured_at: diagnostics.captured_at,
    }
}

fn apply_chaos_events(
    events: &[ChaosEvent],
    window_id: WindowId,
    admin_state: &mut BootstrapAdminState,
    alerts: &mut Vec<burn_p2p_swarm::AlertNotice>,
    emitted_at: DateTime<Utc>,
) {
    for event in events.iter().filter(|event| event.window_id == window_id) {
        match event.fault {
            FaultType::PeerChurn => {
                if let Some(peer_id) = &event.peer_id {
                    admin_state
                        .peer_store
                        .mark_connection(peer_id.clone(), false, emitted_at);
                }
            }
            FaultType::Partition
            | FaultType::RelayLoss
            | FaultType::SlowPeer
            | FaultType::StaleHead => {
                alerts.push(burn_p2p_swarm::AlertNotice {
                    overlay: burn_p2p_swarm::OverlayTopic::control(NetworkId::new("simulation")),
                    peer_id: event.peer_id.clone(),
                    severity: burn_p2p_swarm::AlertSeverity::Warn,
                    code: format!("chaos::{:?}", event.fault).to_lowercase(),
                    message: "chaos event injected".into(),
                    emitted_at,
                });
            }
        }
    }
}

fn multibyte_seed(label: &str, scale_label: &str) -> Vec<u8> {
    let seed = format!("{scale_label}:{label}");
    seed.into_bytes()
}

impl PeerFixtureMode {
    fn malicious_behavior(&self) -> Option<MaliciousBehavior> {
        match self {
            Self::Malicious(behavior) => Some(behavior.clone()),
            Self::HonestNative | Self::HonestBrowser => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use burn_p2p_core::{
        BackendClass, ContentId, DatasetViewId, ExperimentId, HeadEvalReport, HeadEvalStatus,
        HeadId, LeaseId, MetricTrustClass, MetricValue, NetworkId, PeerId, PeerRole,
        PeerWindowMetrics, PeerWindowStatus, ReducerCohortMetrics, ReducerCohortStatus, RevisionId,
        StudyId, WindowActivation, WindowId, WorkloadId,
    };
    use burn_p2p_experiment::ActivationTarget;
    use burn_p2p_metrics::{DerivedMetricKind, MetricsIndexer, MetricsIndexerConfig};
    use burn_p2p_swarm::{ExperimentOverlaySet, MigrationCoordinator, OverlayChannel};
    use chrono::{Duration, Utc};

    use super::{
        ChaosEvent, FaultType, MaliciousBehavior, SimulationRunner, SimulationSpec,
        SyntheticArtifactGenerator,
    };

    #[test]
    fn synthetic_artifact_generator_is_deterministic() {
        let generator = SyntheticArtifactGenerator::default();
        let first = generator
            .build_descriptor(
                burn_p2p_core::ArtifactKind::DeltaPack,
                Some(burn_p2p_core::HeadId::new("head")),
                Some(burn_p2p_core::HeadId::new("base")),
                "seed",
            )
            .expect("descriptor");
        let second = generator
            .build_descriptor(
                burn_p2p_core::ArtifactKind::DeltaPack,
                Some(burn_p2p_core::HeadId::new("head")),
                Some(burn_p2p_core::HeadId::new("base")),
                "seed",
            )
            .expect("descriptor");

        assert_eq!(first, second);
        assert!(!first.chunks.is_empty());
    }

    #[test]
    fn simulation_runner_produces_windows_and_views() {
        let runner = SimulationRunner::default();
        let outcome = runner.run(SimulationSpec::default()).expect("simulation");

        assert_eq!(outcome.windows.len(), 3);
        assert_eq!(outcome.participant_portals.len(), 4);
        assert!(!outcome.operator_console.overlays.is_empty());
        assert!(!outcome.checkpoint_dag.nodes.is_empty());
        assert!(!outcome.heatmap.cells.is_empty());
        assert!(!outcome.benchmark_samples.is_empty());
    }

    #[test]
    fn malicious_fixtures_are_rejected_by_validator_checks() {
        let runner = SimulationRunner::default();
        let mut spec = SimulationSpec {
            peer_count: 2,
            browser_peer_count: 0,
            ..SimulationSpec::default()
        };
        spec.malicious_peers
            .insert(PeerId::new("peer-1"), MaliciousBehavior::NonFiniteMetric);

        let outcome = runner.run(spec).expect("simulation");
        let rejected = outcome
            .windows
            .iter()
            .flat_map(|window| &window.rejected_updates)
            .filter(|rejection| rejection.peer_id == PeerId::new("peer-1"))
            .collect::<Vec<_>>();

        assert!(!rejected.is_empty());
        assert!(rejected.iter().any(|rejection| {
            rejection.findings.iter().any(|finding| {
                matches!(finding, burn_p2p_security::AuditFinding::NonFiniteMetric(_))
            })
        }));
    }

    #[test]
    fn wrong_base_head_updates_are_rejected_before_catalog_insert() {
        let runner = SimulationRunner::default();
        let mut spec = SimulationSpec {
            peer_count: 2,
            browser_peer_count: 0,
            ..SimulationSpec::default()
        };
        spec.malicious_peers
            .insert(PeerId::new("peer-1"), MaliciousBehavior::WrongBaseHead);

        let outcome = runner.run(spec).expect("simulation");
        let rejected = outcome
            .windows
            .iter()
            .flat_map(|window| &window.rejected_updates)
            .filter(|rejection| rejection.peer_id == PeerId::new("peer-1"))
            .collect::<Vec<_>>();

        assert!(!rejected.is_empty());
        assert!(rejected.iter().any(|rejection| {
            rejection.findings.iter().any(|finding| {
                matches!(
                    finding,
                    burn_p2p_security::AuditFinding::BaseHeadMismatch { .. }
                )
            })
        }));
        assert!(
            outcome
                .checkpoint_catalog
                .heads
                .keys()
                .all(|head_id| head_id.as_str() != "head-peer-1-1")
        );
    }

    #[test]
    fn chaos_events_surface_alerts_and_connection_changes() {
        let runner = SimulationRunner::default();
        let spec = SimulationSpec {
            peer_count: 2,
            chaos_events: vec![
                ChaosEvent {
                    window_id: burn_p2p_core::WindowId(1),
                    fault: FaultType::PeerChurn,
                    peer_id: Some(PeerId::new("peer-0")),
                },
                ChaosEvent {
                    window_id: burn_p2p_core::WindowId(2),
                    fault: FaultType::RelayLoss,
                    peer_id: None,
                },
            ],
            ..SimulationSpec::default()
        };

        let outcome = runner.run(spec).expect("simulation");
        assert!(
            outcome
                .operator_console
                .alerts
                .iter()
                .any(|alert| alert.code.contains("relayloss"))
        );
    }

    #[test]
    fn study_board_tracks_accepted_and_rejected_work() {
        let runner = SimulationRunner::default();
        let mut spec = SimulationSpec::default();
        spec.malicious_peers
            .insert(PeerId::new("peer-3"), MaliciousBehavior::OutOfLeaseWork);

        let outcome = runner.run(spec).expect("simulation");
        let metrics = &outcome.study_board.variants[0].metrics;

        assert!(metrics.iter().any(|point| {
            matches!(
                point.metrics.get("accepted_contributions"),
                Some(MetricValue::Integer(value)) if *value >= 0
            )
        }));
        assert!(metrics.iter().any(|point| {
            matches!(
                point.metrics.get("rejected_updates"),
                Some(MetricValue::Integer(value)) if *value >= 0
            )
        }));
    }

    #[test]
    fn merge_certificates_only_cover_window_receipts_for_one_base_head() {
        let runner = SimulationRunner::default();
        let outcome = runner.run(SimulationSpec::default()).expect("simulation");

        for window in &outcome.windows {
            let certificate = window
                .merge_certificate
                .as_ref()
                .expect("merged windows should certify");
            let expected_receipts = window
                .accepted_receipts
                .iter()
                .map(|receipt| receipt.receipt_id.clone())
                .collect::<BTreeSet<_>>();
            let actual_receipts = certificate
                .contribution_receipts
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();

            assert!(!window.accepted_receipts.is_empty());
            assert_eq!(actual_receipts, expected_receipts);
            assert!(
                window
                    .accepted_receipts
                    .iter()
                    .all(|receipt| receipt.base_head_id == certificate.base_head_id)
            );

            let merged_head = outcome
                .checkpoint_catalog
                .head(&certificate.merged_head_id)
                .expect("merged head");
            assert_eq!(
                merged_head.parent_head_id.as_ref(),
                Some(&certificate.base_head_id)
            );
            assert_eq!(merged_head.artifact_id, certificate.merged_artifact_id);
        }
    }

    #[test]
    fn honest_simulation_leases_stay_in_scope_and_pass_audits() {
        let runner = SimulationRunner::default();
        let outcome = runner.run(SimulationSpec::default()).expect("simulation");
        let known_microshards = outcome
            .microshard_plan
            .microshards
            .iter()
            .map(|microshard| microshard.microshard_id.clone())
            .collect::<BTreeSet<_>>();

        for window in &outcome.windows {
            let leases = outcome
                .lease_cache
                .leases_for_window(window.window_id)
                .expect("leases for window");

            assert_eq!(leases.len(), outcome.spec.peer_count as usize);
            assert!(window.data_audits.iter().all(|audit| audit.passed()));
            assert!(window.update_audits.iter().all(|audit| audit.passed()));

            for (peer_id, lease) in leases {
                let distinct_microshards =
                    lease.microshards.iter().cloned().collect::<BTreeSet<_>>();

                assert_eq!(lease.network_id, outcome.spec.network_id);
                assert_eq!(lease.study_id, outcome.spec.study_id);
                assert_eq!(lease.experiment_id, outcome.spec.experiment_id);
                assert_eq!(lease.revision_id, outcome.spec.revision_id);
                assert_eq!(lease.dataset_view_id, outcome.dataset_view.dataset_view_id);
                assert_eq!(lease.window_id, window.window_id);
                assert!(lease.expires_at > lease.granted_at);
                assert!(lease.budget_work_units > 0);
                assert!(!lease.microshards.is_empty());
                assert!(
                    lease.microshards.len() <= outcome.spec.lease_config.max_microshards_per_lease
                );
                assert_eq!(distinct_microshards.len(), lease.microshards.len());
                assert!(
                    lease
                        .microshards
                        .iter()
                        .all(|microshard_id| known_microshards.contains(microshard_id))
                );
                assert!(
                    outcome
                        .peer_fixtures
                        .iter()
                        .any(|fixture| fixture.peer_id == *peer_id)
                );
            }
        }
    }

    #[test]
    fn migration_plans_keep_control_scoped_and_only_move_experiment_topics() {
        let current = ExperimentOverlaySet::new(
            NetworkId::new("mainnet"),
            StudyId::new("study"),
            ExperimentId::new("exp-a"),
        )
        .expect("current overlays");
        let warm_target = ActivationTarget {
            activation: WindowActivation {
                activation_window: WindowId(3),
                grace_windows: 1,
            },
            required_client_capabilities: BTreeSet::from(["warm-patch".into()]),
        };
        let no_overlay_change = MigrationCoordinator::plan_overlay_transition(
            &current,
            &current,
            &warm_target,
            Some(HeadId::new("serve-head-2")),
        );

        assert!(no_overlay_change.leave_topics.is_empty());
        assert!(no_overlay_change.join_topics.is_empty());
        assert_eq!(
            no_overlay_change.fetch_base_head_id,
            Some(HeadId::new("serve-head-2"))
        );
        assert!(MigrationCoordinator::should_activate(
            WindowId(3),
            &warm_target.activation
        ));
        assert!(!MigrationCoordinator::should_activate(
            WindowId(2),
            &warm_target.activation
        ));

        let next = ExperimentOverlaySet::new(
            NetworkId::new("mainnet"),
            StudyId::new("study"),
            ExperimentId::new("exp-b"),
        )
        .expect("next overlays");
        let cold_target = ActivationTarget {
            activation: WindowActivation {
                activation_window: WindowId(5),
                grace_windows: 2,
            },
            required_client_capabilities: BTreeSet::from([
                "cold-patch".into(),
                "tokenizer-v2".into(),
            ]),
        };
        let overlay_change = MigrationCoordinator::plan_overlay_transition(
            &current,
            &next,
            &cold_target,
            Some(HeadId::new("serve-head-5")),
        );

        assert_eq!(overlay_change.leave_topics.len(), 5);
        assert_eq!(overlay_change.join_topics.len(), 5);
        assert!(overlay_change.drain_current_window);
        assert_eq!(
            overlay_change.required_client_capabilities,
            cold_target.required_client_capabilities
        );
        assert!(
            overlay_change
                .leave_topics
                .iter()
                .all(|topic| topic.channel != OverlayChannel::Control)
        );
        assert!(
            overlay_change
                .join_topics
                .iter()
                .all(|topic| topic.channel != OverlayChannel::Control)
        );
    }

    #[test]
    fn simulation_runner_handles_many_peers_with_mixed_updates() {
        let runner = SimulationRunner::default();
        let mut spec = SimulationSpec {
            peer_count: 24,
            browser_peer_count: 6,
            window_count: 4,
            ..SimulationSpec::default()
        };
        spec.artifact_scale.bytes_len = 16 * 1024;
        spec.artifact_scale.chunk_size_bytes = 4 * 1024;
        spec.malicious_peers
            .insert(PeerId::new("peer-5"), MaliciousBehavior::OutOfLeaseWork);
        spec.malicious_peers
            .insert(PeerId::new("peer-9"), MaliciousBehavior::WrongBaseHead);
        spec.malicious_peers
            .insert(PeerId::new("peer-17"), MaliciousBehavior::NonFiniteMetric);

        let outcome = runner.run(spec).expect("simulation");
        let total_updates = outcome
            .windows
            .iter()
            .map(|window| window.accepted_receipts.len() + window.rejected_updates.len())
            .sum::<usize>();

        assert_eq!(
            total_updates,
            outcome.spec.peer_count as usize * outcome.spec.window_count as usize
        );
        assert_eq!(
            outcome.browser_harness.peer_ids.len(),
            outcome.spec.browser_peer_count as usize
        );
        assert_eq!(
            outcome
                .windows
                .iter()
                .filter(|window| window.merge_certificate.is_some())
                .count(),
            outcome.spec.window_count as usize
        );
        assert!(
            !outcome
                .windows
                .iter()
                .flat_map(|window| &window.rejected_updates)
                .collect::<Vec<_>>()
                .is_empty()
        );
        assert_eq!(
            outcome.diagnostics.swarm.connected_peers,
            outcome.spec.peer_count
        );
        assert!(
            outcome
                .windows
                .iter()
                .flat_map(|window| &window.rejected_updates)
                .any(|rejection| {
                    rejection.peer_id == PeerId::new("peer-9")
                        && rejection.findings.iter().any(|finding| {
                            matches!(
                                finding,
                                burn_p2p_security::AuditFinding::BaseHeadMismatch { .. }
                            )
                        })
                })
        );
    }

    #[test]
    fn metrics_indexer_surfaces_lag_and_desync_signals_for_branchy_windows() {
        let mut indexer = MetricsIndexer::new(MetricsIndexerConfig {
            stale_head_lag_threshold_steps: 4,
            ledger_segment_entry_limit: 8,
            ..MetricsIndexerConfig::default()
        });
        let started_at = Utc::now();
        indexer.ingest_head_eval_report(HeadEvalReport {
            network_id: NetworkId::new("network-sim"),
            experiment_id: ExperimentId::new("exp-sim"),
            revision_id: RevisionId::new("rev-sim"),
            workload_id: WorkloadId::new("workload-sim"),
            head_id: HeadId::new("head-base"),
            base_head_id: None,
            eval_protocol_id: ContentId::new("eval-sim"),
            evaluator_set_id: ContentId::new("eval-set-sim"),
            metric_values: std::collections::BTreeMap::from([(
                "loss".into(),
                MetricValue::Float(0.22),
            )]),
            sample_count: 512,
            dataset_view_id: DatasetViewId::new("view-sim"),
            started_at,
            finished_at: started_at + Duration::seconds(1),
            trust_class: MetricTrustClass::Canonical,
            status: HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        });
        indexer.ingest_peer_window_metrics(PeerWindowMetrics {
            network_id: NetworkId::new("network-sim"),
            experiment_id: ExperimentId::new("exp-sim"),
            revision_id: RevisionId::new("rev-sim"),
            workload_id: WorkloadId::new("workload-sim"),
            dataset_view_id: DatasetViewId::new("view-sim"),
            peer_id: PeerId::new("peer-a"),
            principal_id: None,
            lease_id: LeaseId::new("lease-a"),
            base_head_id: HeadId::new("head-base"),
            window_started_at: started_at + Duration::seconds(5),
            window_finished_at: started_at + Duration::seconds(15),
            attempted_tokens_or_samples: 200,
            accepted_tokens_or_samples: Some(200),
            local_train_loss_mean: Some(0.30),
            local_train_loss_last: Some(0.28),
            grad_or_delta_norm: Some(1.0),
            optimizer_step_count: 10,
            compute_time_ms: 8_000,
            data_fetch_time_ms: 600,
            publish_latency_ms: 120,
            head_lag_at_start: 1,
            head_lag_at_finish: 1,
            backend_class: BackendClass::Cpu,
            role: PeerRole::TrainerCpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        });
        indexer.ingest_peer_window_metrics(PeerWindowMetrics {
            network_id: NetworkId::new("network-sim"),
            experiment_id: ExperimentId::new("exp-sim"),
            revision_id: RevisionId::new("rev-sim"),
            workload_id: WorkloadId::new("workload-sim"),
            dataset_view_id: DatasetViewId::new("view-sim"),
            peer_id: PeerId::new("peer-b"),
            principal_id: None,
            lease_id: LeaseId::new("lease-b"),
            base_head_id: HeadId::new("head-base"),
            window_started_at: started_at + Duration::seconds(15),
            window_finished_at: started_at + Duration::seconds(24),
            attempted_tokens_or_samples: 150,
            accepted_tokens_or_samples: Some(150),
            local_train_loss_mean: Some(0.34),
            local_train_loss_last: Some(0.31),
            grad_or_delta_norm: Some(1.3),
            optimizer_step_count: 8,
            compute_time_ms: 7_500,
            data_fetch_time_ms: 700,
            publish_latency_ms: 150,
            head_lag_at_start: 5,
            head_lag_at_finish: 6,
            backend_class: BackendClass::BrowserWgpu,
            role: PeerRole::BrowserTrainerWgpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        });
        indexer.ingest_reducer_cohort_metrics(ReducerCohortMetrics {
            network_id: NetworkId::new("network-sim"),
            experiment_id: ExperimentId::new("exp-sim"),
            revision_id: RevisionId::new("rev-sim"),
            workload_id: WorkloadId::new("workload-sim"),
            dataset_view_id: DatasetViewId::new("view-sim"),
            merge_window_id: ContentId::new("window-a"),
            reducer_group_id: ContentId::new("reducers-a"),
            captured_at: started_at + Duration::seconds(26),
            base_head_id: HeadId::new("head-base"),
            candidate_head_id: Some(HeadId::new("head-candidate-a")),
            received_updates: 4,
            accepted_updates: 3,
            rejected_updates: 1,
            sum_weight: 2.4,
            accepted_tokens_or_samples: 280,
            staleness_mean: 1.8,
            staleness_max: 6.0,
            window_close_delay_ms: 220,
            cohort_duration_ms: 5_000,
            aggregate_norm: 1.2,
            reducer_load: 0.7,
            ingress_bytes: 6_144,
            egress_bytes: 2_048,
            replica_agreement: Some(0.9),
            late_arrival_count: Some(1),
            missing_peer_count: Some(0),
            rejection_reasons: std::collections::BTreeMap::from([("late".into(), 1)]),
            status: ReducerCohortStatus::Closed,
        });
        indexer.ingest_reducer_cohort_metrics(ReducerCohortMetrics {
            network_id: NetworkId::new("network-sim"),
            experiment_id: ExperimentId::new("exp-sim"),
            revision_id: RevisionId::new("rev-sim"),
            workload_id: WorkloadId::new("workload-sim"),
            dataset_view_id: DatasetViewId::new("view-sim"),
            merge_window_id: ContentId::new("window-b"),
            reducer_group_id: ContentId::new("reducers-a"),
            captured_at: started_at + Duration::seconds(27),
            base_head_id: HeadId::new("head-base"),
            candidate_head_id: Some(HeadId::new("head-candidate-b")),
            received_updates: 5,
            accepted_updates: 2,
            rejected_updates: 3,
            sum_weight: 1.9,
            accepted_tokens_or_samples: 70,
            staleness_mean: 2.4,
            staleness_max: 7.0,
            window_close_delay_ms: 640,
            cohort_duration_ms: 6_000,
            aggregate_norm: 1.5,
            reducer_load: 0.9,
            ingress_bytes: 8_192,
            egress_bytes: 4_096,
            replica_agreement: Some(0.4),
            late_arrival_count: Some(2),
            missing_peer_count: Some(1),
            rejection_reasons: std::collections::BTreeMap::from([
                ("late".into(), 2),
                ("stale".into(), 1),
            ]),
            status: ReducerCohortStatus::Inconsistent,
        });

        let derived =
            indexer.derive_metrics(&ExperimentId::new("exp-sim"), &RevisionId::new("rev-sim"));
        let metric_value = |kind: DerivedMetricKind| {
            derived
                .iter()
                .find(|point| point.metric == kind)
                .map(|point| point.value)
                .expect("derived metric point")
        };

        assert_eq!(metric_value(DerivedMetricKind::StaleWorkFraction), 0.5);
        assert_eq!(metric_value(DerivedMetricKind::MeanHeadLag), 3.5);
        assert_eq!(metric_value(DerivedMetricKind::MaxHeadLag), 6.0);
        assert_eq!(
            metric_value(DerivedMetricKind::ReducerReplicaAgreement),
            0.65
        );
        assert_eq!(metric_value(DerivedMetricKind::MergeWindowSkew), 640.0);
        assert_eq!(metric_value(DerivedMetricKind::CandidateBranchFactor), 2.0);
        assert_eq!(metric_value(DerivedMetricKind::HeadAdoptionLagP50), 4_000.0);
        assert_eq!(
            metric_value(DerivedMetricKind::HeadAdoptionLagP90),
            14_000.0
        );
        assert!(derived.iter().any(|point| {
            point.metric == DerivedMetricKind::RejectionRatioByReason
                && point.series_label.as_deref() == Some("late")
        }));
    }
}
