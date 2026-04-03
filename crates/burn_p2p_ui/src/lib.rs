#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use burn_p2p_bootstrap::BootstrapDiagnostics;
use burn_p2p_core::{
    AggregateEnvelope, AggregateTier, ArtifactId, AssignmentLease, AuthProvider, BrowserRole,
    ContributionReceipt, ContributionReceiptId, DatasetViewId, ExperimentDirectoryEntry,
    ExperimentId, ExperimentScope, HeadDescriptor, HeadId, MergeCertId, MergeCertificate,
    MergeStrategy, MergeWindowState, MetricValue, MicroShardId, NetworkId, PeerId,
    ReducerLoadReport, ReductionCertificate, RevisionId, StudyId, TelemetrySummary,
    WindowActivation, WindowId,
};
use burn_p2p_security::PeerTrustLevel;
use burn_p2p_swarm::{AlertNotice, OverlayTopic};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginProviderView {
    pub provider: AuthProvider,
    pub label: String,
    pub login_path: String,
    pub callback_path: Option<String>,
    pub device_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustBadgeView {
    pub level: PeerTrustLevel,
    pub label: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContributionIdentityPanel {
    pub principal_id: String,
    pub provider_label: String,
    pub trust_badges: Vec<TrustBadgeView>,
    pub scoped_experiments: Vec<ExperimentId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthPortalView {
    pub network_id: NetworkId,
    pub providers: Vec<LoginProviderView>,
    pub active_session: Option<ContributionIdentityPanel>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperimentPickerCard {
    pub experiment_id: ExperimentId,
    pub study_id: StudyId,
    pub display_name: String,
    pub current_revision_id: RevisionId,
    pub current_head_id: Option<HeadId>,
    pub estimated_download_bytes: u64,
    pub estimated_window_seconds: u64,
    pub allowed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperimentPickerView {
    pub network_id: NetworkId,
    pub entries: Vec<ExperimentPickerCard>,
}

impl ExperimentPickerView {
    pub fn from_directory(
        network_id: NetworkId,
        entries: Vec<ExperimentDirectoryEntry>,
        scopes: &[ExperimentScope],
    ) -> Self {
        let scope_set = scopes
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let mut cards = entries
            .into_iter()
            .map(|entry| ExperimentPickerCard {
                experiment_id: entry.experiment_id.clone(),
                study_id: entry.study_id,
                display_name: entry.display_name,
                current_revision_id: entry.current_revision_id,
                current_head_id: entry.current_head_id,
                estimated_download_bytes: entry.resource_requirements.estimated_download_bytes,
                estimated_window_seconds: entry.resource_requirements.estimated_window_seconds,
                allowed: entry
                    .allowed_scopes
                    .iter()
                    .any(|scope| scope_set.contains(scope)),
            })
            .collect::<Vec<_>>();
        cards.sort_by(|left, right| left.display_name.cmp(&right.display_name));

        Self {
            network_id,
            entries: cards,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrowserExperimentPickerState {
    PortalOnly,
    Observer,
    Verifier,
    Trainer,
    BackgroundSuspended,
    Catchup,
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserExperimentPickerCard {
    pub experiment_id: ExperimentId,
    pub study_id: StudyId,
    pub display_name: String,
    pub current_revision_id: RevisionId,
    pub current_head_id: Option<HeadId>,
    pub estimated_download_bytes: u64,
    pub estimated_window_seconds: u64,
    pub allowed: bool,
    pub recommended_state: BrowserExperimentPickerState,
    pub recommended_role: Option<BrowserRole>,
    pub fallback_from_preferred: bool,
    pub eligible_roles: Vec<BrowserRole>,
    pub blocked_reasons: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserExperimentPickerView {
    pub network_id: NetworkId,
    pub entries: Vec<BrowserExperimentPickerCard>,
}

impl BrowserExperimentPickerView {
    pub fn new(network_id: NetworkId, mut entries: Vec<BrowserExperimentPickerCard>) -> Self {
        entries.sort_by(|left, right| left.display_name.cmp(&right.display_name));
        Self {
            network_id,
            entries,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitHubProfileLink {
    pub login: String,
    pub profile_url: String,
    pub linked_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParticipantProfile {
    pub peer_id: PeerId,
    pub display_name: Option<String>,
    pub github: Option<GitHubProfileLink>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointDownload {
    pub head_id: HeadId,
    pub artifact_id: ArtifactId,
    pub label: String,
    pub download_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParticipantPortalView {
    pub profile: ParticipantProfile,
    pub current_assignment: Option<AssignmentLease>,
    pub local_telemetry: Option<TelemetrySummary>,
    pub accepted_receipts: Vec<ContributionReceipt>,
    pub latest_accepted_receipt: Option<ContributionReceipt>,
    pub latest_published_artifact_id: Option<ArtifactId>,
    pub checkpoint_downloads: Vec<CheckpointDownload>,
}

impl ParticipantPortalView {
    pub fn new(
        profile: ParticipantProfile,
        current_assignment: Option<AssignmentLease>,
        local_telemetry: Option<TelemetrySummary>,
        mut accepted_receipts: Vec<ContributionReceipt>,
        latest_published_artifact_id: Option<ArtifactId>,
        checkpoint_downloads: Vec<CheckpointDownload>,
    ) -> Self {
        accepted_receipts.sort_by_key(|receipt| std::cmp::Reverse(receipt.accepted_at));
        let latest_accepted_receipt = accepted_receipts.first().cloned();

        Self {
            profile,
            current_assignment,
            local_telemetry,
            accepted_receipts,
            latest_accepted_receipt,
            latest_published_artifact_id,
            checkpoint_downloads,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverlayStatusView {
    pub overlay: OverlayTopic,
    pub active_peers: u32,
    pub connected_peers: u32,
    pub last_window_id: Option<WindowId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeQueueStatus {
    Pending,
    Certified,
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MergeQueueEntry {
    pub base_head_id: HeadId,
    pub candidate_receipt_ids: Vec<ContributionReceiptId>,
    pub status: MergeQueueStatus,
    pub merged_head_id: Option<HeadId>,
    pub merged_artifact_id: Option<ArtifactId>,
    pub merge_cert_id: Option<MergeCertId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityActionRecord {
    pub action: String,
    pub actor_peer_id: Option<PeerId>,
    pub happened_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperatorConsoleView {
    pub diagnostics: BootstrapDiagnostics,
    pub overlays: Vec<OverlayStatusView>,
    pub merge_queue: Vec<MergeQueueEntry>,
    pub authority_actions: Vec<AuthorityActionRecord>,
    pub alerts: Vec<AlertNotice>,
}

impl OperatorConsoleView {
    pub fn new(
        diagnostics: BootstrapDiagnostics,
        overlays: Vec<OverlayStatusView>,
        merge_queue: Vec<MergeQueueEntry>,
        authority_actions: Vec<AuthorityActionRecord>,
        alerts: Vec<AlertNotice>,
    ) -> Self {
        Self {
            diagnostics,
            overlays,
            merge_queue,
            authority_actions,
            alerts,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricPoint {
    pub window_id: WindowId,
    pub metrics: BTreeMap<String, MetricValue>,
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CostPerformancePoint {
    pub label: String,
    pub cost: f64,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExperimentVariantView {
    pub study_id: StudyId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub metrics: Vec<MetricPoint>,
    pub accepted_work: u64,
    pub cost_performance: Vec<CostPerformancePoint>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperimentMigrationView {
    pub experiment_id: ExperimentId,
    pub from_revision_id: RevisionId,
    pub to_revision_id: RevisionId,
    pub activation: WindowActivation,
    pub note: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StudyBoardView {
    pub network_id: NetworkId,
    pub study_id: StudyId,
    pub variants: Vec<ExperimentVariantView>,
    pub migrations: Vec<ExperimentMigrationView>,
}

impl StudyBoardView {
    pub fn new(
        network_id: NetworkId,
        study_id: StudyId,
        variants: Vec<ExperimentVariantView>,
        migrations: Vec<ExperimentMigrationView>,
    ) -> Self {
        Self {
            network_id,
            study_id,
            variants,
            migrations,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeWindowView {
    pub merge_window_id: burn_p2p_core::ContentId,
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub base_head_id: HeadId,
    pub strategy: MergeStrategy,
    pub reducer_count: usize,
    pub validator_count: usize,
    pub opened_at: DateTime<Utc>,
    pub closes_at: DateTime<Utc>,
}

impl MergeWindowView {
    pub fn from_state(state: &MergeWindowState) -> Self {
        Self {
            merge_window_id: state.merge_window_id.clone(),
            experiment_id: state.experiment_id.clone(),
            revision_id: state.revision_id.clone(),
            base_head_id: state.base_head_id.clone(),
            strategy: state.policy.strategy.clone(),
            reducer_count: state.reducers.len(),
            validator_count: state.validators.len(),
            opened_at: state.opened_at,
            closes_at: state.closes_at,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReducerUtilizationView {
    pub peer_id: PeerId,
    pub assigned_leaf_updates: u32,
    pub assigned_aggregate_inputs: u32,
    pub ingress_bytes: u128,
    pub egress_bytes: u128,
    pub duplicate_transfer_ratio: f64,
    pub overload_ratio: f64,
}

impl From<ReducerLoadReport> for ReducerUtilizationView {
    fn from(value: ReducerLoadReport) -> Self {
        Self {
            peer_id: value.peer_id,
            assigned_leaf_updates: value.assigned_leaf_updates,
            assigned_aggregate_inputs: value.assigned_aggregate_inputs,
            ingress_bytes: value.ingress_bytes,
            egress_bytes: value.egress_bytes,
            duplicate_transfer_ratio: value.duplicate_transfer_ratio,
            overload_ratio: value.overload_ratio,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateDagNode {
    pub aggregate_id: burn_p2p_core::ContentId,
    pub reducer_peer_id: PeerId,
    pub tier: AggregateTier,
    pub artifact_id: ArtifactId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateDagEdge {
    pub from_aggregate_id: burn_p2p_core::ContentId,
    pub to_aggregate_id: burn_p2p_core::ContentId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateDagView {
    pub nodes: Vec<AggregateDagNode>,
    pub edges: Vec<AggregateDagEdge>,
}

impl AggregateDagView {
    pub fn from_aggregates(aggregates: Vec<AggregateEnvelope>) -> Self {
        let nodes = aggregates
            .iter()
            .map(|aggregate| AggregateDagNode {
                aggregate_id: aggregate.aggregate_id.clone(),
                reducer_peer_id: aggregate.reducer_peer_id.clone(),
                tier: aggregate.tier.clone(),
                artifact_id: aggregate.aggregate_artifact_id.clone(),
            })
            .collect::<Vec<_>>();
        let edges = aggregates
            .into_iter()
            .flat_map(|aggregate| {
                aggregate
                    .child_aggregate_ids
                    .into_iter()
                    .map(move |child| AggregateDagEdge {
                        from_aggregate_id: child,
                        to_aggregate_id: aggregate.aggregate_id.clone(),
                    })
            })
            .collect();

        Self { nodes, edges }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadPromotionTimelineEntry {
    pub reduction_id: burn_p2p_core::ContentId,
    pub aggregate_id: burn_p2p_core::ContentId,
    pub validator: PeerId,
    pub validator_quorum: u16,
    pub issued_at: DateTime<Utc>,
}

impl From<ReductionCertificate> for HeadPromotionTimelineEntry {
    fn from(value: ReductionCertificate) -> Self {
        Self {
            reduction_id: value.reduction_id,
            aggregate_id: value.aggregate_id,
            validator: value.validator,
            validator_quorum: value.validator_quorum,
            issued_at: value.issued_at,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MergeTopologyDashboardView {
    pub merge_window: MergeWindowView,
    pub reducers: Vec<ReducerUtilizationView>,
    pub aggregate_dag: AggregateDagView,
    pub promotion_timeline: Vec<HeadPromotionTimelineEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckpointDagEdgeKind {
    Lineage,
    CertifiedMerge,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CheckpointDagNode {
    pub head_id: HeadId,
    pub parent_head_id: Option<HeadId>,
    pub artifact_id: ArtifactId,
    pub global_step: u64,
    pub created_at: DateTime<Utc>,
    pub metrics: BTreeMap<String, MetricValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointDagEdge {
    pub from_head_id: HeadId,
    pub to_head_id: HeadId,
    pub kind: CheckpointDagEdgeKind,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CheckpointDagView {
    pub nodes: Vec<CheckpointDagNode>,
    pub edges: Vec<CheckpointDagEdge>,
    pub latest_head_id: Option<HeadId>,
}

impl CheckpointDagView {
    pub fn from_heads_and_merges(
        mut heads: Vec<HeadDescriptor>,
        merges: Vec<MergeCertificate>,
    ) -> Self {
        heads.sort_by(|left, right| {
            left.global_step
                .cmp(&right.global_step)
                .then_with(|| left.created_at.cmp(&right.created_at))
        });

        let nodes = heads
            .iter()
            .map(|head| CheckpointDagNode {
                head_id: head.head_id.clone(),
                parent_head_id: head.parent_head_id.clone(),
                artifact_id: head.artifact_id.clone(),
                global_step: head.global_step,
                created_at: head.created_at,
                metrics: head.metrics.clone(),
            })
            .collect::<Vec<_>>();

        let mut edges = heads
            .iter()
            .filter_map(|head| {
                head.parent_head_id
                    .as_ref()
                    .map(|parent_head_id| CheckpointDagEdge {
                        from_head_id: parent_head_id.clone(),
                        to_head_id: head.head_id.clone(),
                        kind: CheckpointDagEdgeKind::Lineage,
                    })
            })
            .collect::<Vec<_>>();

        edges.extend(merges.into_iter().map(|merge| CheckpointDagEdge {
            from_head_id: merge.base_head_id,
            to_head_id: merge.merged_head_id,
            kind: CheckpointDagEdgeKind::CertifiedMerge,
        }));

        let latest_head_id = heads.last().map(|head| head.head_id.clone());

        Self {
            nodes,
            edges,
            latest_head_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmaFlowStep {
    pub base_head_id: HeadId,
    pub merged_head_id: HeadId,
    pub merge_cert_id: MergeCertId,
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmaFlowView {
    pub steps: Vec<EmaFlowStep>,
}

impl EmaFlowView {
    pub fn from_merge_certificates(mut merge_certificates: Vec<MergeCertificate>) -> Self {
        merge_certificates.sort_by_key(|merge| merge.issued_at);

        let steps = merge_certificates
            .into_iter()
            .map(|merge| EmaFlowStep {
                base_head_id: merge.base_head_id,
                merged_head_id: merge.merged_head_id,
                merge_cert_id: merge.merge_cert_id,
                issued_at: merge.issued_at,
            })
            .collect();

        Self { steps }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardAssignmentCell {
    pub peer_id: PeerId,
    pub microshard_id: MicroShardId,
    pub lease_count: u32,
    pub total_budget_work_units: u64,
    pub latest_window_id: WindowId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardAssignmentHeatmap {
    pub dataset_view_id: Option<DatasetViewId>,
    pub cells: Vec<ShardAssignmentCell>,
}

impl ShardAssignmentHeatmap {
    pub fn from_leases(mut leases: Vec<AssignmentLease>) -> Self {
        leases.sort_by_key(|lease| lease.granted_at);

        let dataset_view_id = leases.first().map(|lease| lease.dataset_view_id.clone());
        let mut cells = BTreeMap::<(PeerId, MicroShardId), ShardAssignmentCell>::new();

        for lease in leases {
            for microshard_id in lease.microshards {
                let key = (lease.peer_id.clone(), microshard_id.clone());
                cells
                    .entry(key)
                    .and_modify(|cell| {
                        cell.lease_count += 1;
                        cell.total_budget_work_units += lease.budget_work_units;
                        cell.latest_window_id = lease.window_id;
                    })
                    .or_insert(ShardAssignmentCell {
                        peer_id: lease.peer_id.clone(),
                        microshard_id,
                        lease_count: 1,
                        total_budget_work_units: lease.budget_work_units,
                        latest_window_id: lease.window_id,
                    });
            }
        }

        Self {
            dataset_view_id,
            cells: cells.into_values().collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UiChannel {
    Operator,
    Participant,
    StudyBoard,
    CheckpointDag,
    EmaFlow,
    Heatmap,
    Alert,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum UiPayload {
    Operator(Box<OperatorConsoleView>),
    Participant(Box<ParticipantPortalView>),
    StudyBoard(StudyBoardView),
    CheckpointDag(CheckpointDagView),
    EmaFlow(EmaFlowView),
    Heatmap(ShardAssignmentHeatmap),
    Alert(AlertNotice),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UiEventEnvelope {
    pub channel: UiChannel,
    pub sequence: u64,
    pub emitted_at: DateTime<Utc>,
    pub payload: UiPayload,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_bootstrap::{BootstrapDiagnostics, BootstrapPreset, BootstrapService};
    use burn_p2p_core::{
        AggregateEnvelope, AggregateStats, AggregateTier, ArtifactId, AssignmentLease,
        AuthProvider, BrowserRole, ContributionReceipt, ContributionReceiptId,
        ExperimentDirectoryEntry, ExperimentId, ExperimentOptInPolicy,
        ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, HeadDescriptor,
        HeadId, MergeCertId, MergeCertificate, MergePolicy, MergeStrategy, MergeTopologyPolicy,
        MergeWindowState, NetworkEstimate, NetworkId, PeerId, PeerRole, PeerRoleSet,
        ReducerLoadReport, ReductionCertificate, RevisionId, RevocationEpoch, StudyId,
        WindowActivation, WindowId,
    };
    use burn_p2p_security::PeerTrustLevel;
    use burn_p2p_swarm::{AlertSeverity, OverlayChannel, OverlayTopic};
    use chrono::{Duration, Utc};

    use super::{
        AggregateDagView, AlertNotice, AuthPortalView, AuthorityActionRecord,
        BrowserExperimentPickerCard, BrowserExperimentPickerState, BrowserExperimentPickerView,
        CheckpointDagEdgeKind, CheckpointDagView, CheckpointDownload, ContributionIdentityPanel,
        CostPerformancePoint, EmaFlowView, ExperimentMigrationView, ExperimentPickerView,
        ExperimentVariantView, GitHubProfileLink, HeadPromotionTimelineEntry, LoginProviderView,
        MergeQueueEntry, MergeQueueStatus, MergeTopologyDashboardView, MergeWindowView,
        OperatorConsoleView, OverlayStatusView, ParticipantPortalView, ParticipantProfile,
        ReducerUtilizationView, ShardAssignmentHeatmap, StudyBoardView, TrustBadgeView, UiChannel,
        UiEventEnvelope, UiPayload,
    };

    #[test]
    fn participant_portal_sorts_receipts_and_tracks_latest() {
        let now = Utc::now();
        let older = ContributionReceipt {
            receipt_id: ContributionReceiptId::new("older"),
            peer_id: PeerId::new("peer"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base"),
            artifact_id: ArtifactId::new("artifact-1"),
            accepted_at: now,
            accepted_weight: 1.0,
            metrics: BTreeMap::new(),
            merge_cert_id: None,
        };
        let newer = ContributionReceipt {
            receipt_id: ContributionReceiptId::new("newer"),
            peer_id: PeerId::new("peer"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base"),
            artifact_id: ArtifactId::new("artifact-2"),
            accepted_at: now + Duration::seconds(5),
            accepted_weight: 2.0,
            metrics: BTreeMap::new(),
            merge_cert_id: None,
        };

        let view = ParticipantPortalView::new(
            ParticipantProfile {
                peer_id: PeerId::new("peer"),
                display_name: Some("peer".into()),
                github: Some(GitHubProfileLink {
                    login: "peer".into(),
                    profile_url: "https://github.com/peer".into(),
                    linked_at: now,
                }),
            },
            None,
            None,
            vec![older.clone(), newer.clone()],
            Some(ArtifactId::new("published")),
            vec![CheckpointDownload {
                head_id: HeadId::new("head"),
                artifact_id: ArtifactId::new("artifact"),
                label: "latest".into(),
                download_path: "/downloads/latest".into(),
            }],
        );

        assert_eq!(view.latest_accepted_receipt, Some(newer));
        assert_eq!(view.accepted_receipts[1], older);
    }

    #[test]
    fn checkpoint_dag_builds_lineage_and_merge_edges() {
        let now = Utc::now();
        let base = HeadDescriptor {
            head_id: HeadId::new("base"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-base"),
            parent_head_id: None,
            global_step: 1,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let child = HeadDescriptor {
            head_id: HeadId::new("child"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: ArtifactId::new("artifact-child"),
            parent_head_id: Some(HeadId::new("base")),
            global_step: 2,
            created_at: now + Duration::seconds(1),
            metrics: BTreeMap::new(),
        };
        let merge = MergeCertificate {
            merge_cert_id: MergeCertId::new("merge"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base"),
            merged_head_id: HeadId::new("child"),
            merged_artifact_id: ArtifactId::new("artifact-child"),
            policy: MergePolicy::Ema,
            issued_at: now + Duration::seconds(2),
            validator: PeerId::new("validator"),
            contribution_receipts: vec![],
        };

        let dag = CheckpointDagView::from_heads_and_merges(
            vec![child.clone(), base.clone()],
            vec![merge],
        );

        assert_eq!(dag.latest_head_id, Some(child.head_id));
        assert_eq!(dag.nodes.len(), 2);
        assert!(
            dag.edges
                .iter()
                .any(|edge| edge.kind == CheckpointDagEdgeKind::Lineage)
        );
        assert!(
            dag.edges
                .iter()
                .any(|edge| edge.kind == CheckpointDagEdgeKind::CertifiedMerge)
        );
    }

    #[test]
    fn ema_flow_orders_merge_certificates_by_time() {
        let now = Utc::now();
        let older = MergeCertificate {
            merge_cert_id: MergeCertId::new("older"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base-1"),
            merged_head_id: HeadId::new("merged-1"),
            merged_artifact_id: ArtifactId::new("artifact-1"),
            policy: MergePolicy::Ema,
            issued_at: now,
            validator: PeerId::new("validator"),
            contribution_receipts: vec![],
        };
        let newer = MergeCertificate {
            merge_cert_id: MergeCertId::new("newer"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            base_head_id: HeadId::new("base-2"),
            merged_head_id: HeadId::new("merged-2"),
            merged_artifact_id: ArtifactId::new("artifact-2"),
            policy: MergePolicy::Ema,
            issued_at: now + Duration::seconds(1),
            validator: PeerId::new("validator"),
            contribution_receipts: vec![],
        };

        let flow = EmaFlowView::from_merge_certificates(vec![newer.clone(), older.clone()]);

        assert_eq!(flow.steps[0].merge_cert_id, older.merge_cert_id);
        assert_eq!(flow.steps[1].merge_cert_id, newer.merge_cert_id);
    }

    #[test]
    fn heatmap_aggregates_microshards_by_peer() {
        let now = Utc::now();
        let leases = vec![
            AssignmentLease {
                lease_id: burn_p2p_core::LeaseId::new("lease-1"),
                network_id: NetworkId::new("network"),
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                peer_id: PeerId::new("peer"),
                dataset_view_id: burn_p2p_core::DatasetViewId::new("view"),
                window_id: WindowId(1),
                granted_at: now,
                expires_at: now + Duration::minutes(5),
                budget_work_units: 100,
                microshards: vec![burn_p2p_core::MicroShardId::new("micro-a")],
                assignment_hash: burn_p2p_core::ContentId::new("hash-1"),
            },
            AssignmentLease {
                lease_id: burn_p2p_core::LeaseId::new("lease-2"),
                network_id: NetworkId::new("network"),
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                peer_id: PeerId::new("peer"),
                dataset_view_id: burn_p2p_core::DatasetViewId::new("view"),
                window_id: WindowId(2),
                granted_at: now + Duration::seconds(1),
                expires_at: now + Duration::minutes(5),
                budget_work_units: 80,
                microshards: vec![burn_p2p_core::MicroShardId::new("micro-a")],
                assignment_hash: burn_p2p_core::ContentId::new("hash-2"),
            },
        ];

        let heatmap = ShardAssignmentHeatmap::from_leases(leases);

        assert_eq!(heatmap.cells.len(), 1);
        assert_eq!(heatmap.cells[0].lease_count, 2);
        assert_eq!(heatmap.cells[0].total_budget_work_units, 180);
        assert_eq!(heatmap.cells[0].latest_window_id, WindowId(2));
    }

    #[test]
    fn operator_console_and_study_board_are_framework_neutral_contracts() {
        let now = Utc::now();
        let diagnostics = BootstrapDiagnostics {
            network_id: NetworkId::new("network"),
            preset: BootstrapPreset::AllInOne,
            services: BTreeSet::from([BootstrapService::Authority]),
            roles: PeerRoleSet::default_trainer(),
            swarm: burn_p2p_swarm::SwarmStats {
                connected_peers: 3,
                connected_peer_ids: vec![PeerId::new("a"), PeerId::new("b"), PeerId::new("c")],
                observed_peers: vec![PeerId::new("a"), PeerId::new("b"), PeerId::new("c")],
                network_estimate: NetworkEstimate {
                    connected_peers: 3,
                    observed_peers: 3,
                    estimated_network_size: 4.0,
                    estimated_total_vram_bytes: Some(1),
                    estimated_total_flops: Some(2.0),
                    eta_lower_seconds: Some(10),
                    eta_upper_seconds: Some(20),
                },
            },
            pinned_heads: BTreeSet::from([HeadId::new("head")]),
            pinned_artifacts: BTreeSet::from([ArtifactId::new("artifact")]),
            accepted_receipts: 5,
            certified_merges: 2,
            in_flight_transfers: Vec::new(),
            admitted_peers: BTreeSet::from([PeerId::new("a"), PeerId::new("b")]),
            peer_diagnostics: vec![burn_p2p_bootstrap::BootstrapPeerDiagnostic {
                peer_id: PeerId::new("a"),
                connected: true,
                observed_at: Some(now),
                trust_level: Some(PeerTrustLevel::PolicyCompliant),
                rejection_reason: None,
                reputation_score: Some(1.25),
                reputation_decision: Some(burn_p2p_security::ReputationDecision::Allow),
                quarantined: false,
                banned: false,
            }],
            rejected_peers: BTreeMap::from([(
                PeerId::new("rejected"),
                "ScopeNotAuthorized(Train { experiment_id: ExperimentId(\"exp\") })".into(),
            )]),
            quarantined_peers: BTreeSet::new(),
            banned_peers: BTreeSet::new(),
            minimum_revocation_epoch: Some(RevocationEpoch(3)),
            last_error: Some("peer rejected".into()),
            node_state: burn_p2p_bootstrap::NodeRuntimeState::IdleReady,
            slot_states: vec![burn_p2p_bootstrap::SlotRuntimeState::Unassigned],
            captured_at: now,
        };

        let operator = OperatorConsoleView::new(
            diagnostics,
            vec![OverlayStatusView {
                overlay: OverlayTopic::experiment(
                    NetworkId::new("network"),
                    StudyId::new("study"),
                    ExperimentId::new("exp"),
                    OverlayChannel::Telemetry,
                )
                .expect("overlay"),
                active_peers: 3,
                connected_peers: 3,
                last_window_id: Some(WindowId(7)),
            }],
            vec![MergeQueueEntry {
                base_head_id: HeadId::new("base"),
                candidate_receipt_ids: vec![ContributionReceiptId::new("receipt")],
                status: MergeQueueStatus::Pending,
                merged_head_id: None,
                merged_artifact_id: None,
                merge_cert_id: None,
            }],
            vec![AuthorityActionRecord {
                action: "pause".into(),
                actor_peer_id: Some(PeerId::new("authority")),
                happened_at: now,
            }],
            vec![AlertNotice {
                overlay: OverlayTopic::control(NetworkId::new("network")),
                peer_id: None,
                severity: AlertSeverity::Warn,
                code: "relay-degraded".into(),
                message: "relay latency elevated".into(),
                emitted_at: now,
            }],
        );

        let board = StudyBoardView::new(
            NetworkId::new("network"),
            StudyId::new("study"),
            vec![ExperimentVariantView {
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                metrics: vec![super::MetricPoint {
                    window_id: WindowId(7),
                    metrics: BTreeMap::new(),
                    captured_at: now,
                }],
                accepted_work: 12,
                cost_performance: vec![CostPerformancePoint {
                    label: "quality".into(),
                    cost: 5.0,
                    value: 0.9,
                }],
            }],
            vec![ExperimentMigrationView {
                experiment_id: ExperimentId::new("exp"),
                from_revision_id: RevisionId::new("rev-a"),
                to_revision_id: RevisionId::new("rev-b"),
                activation: WindowActivation {
                    activation_window: WindowId(8),
                    grace_windows: 1,
                },
                note: Some("warm patch".into()),
            }],
        );

        let event = UiEventEnvelope {
            channel: UiChannel::Operator,
            sequence: 1,
            emitted_at: now,
            payload: UiPayload::Operator(Box::new(operator)),
        };

        assert!(matches!(event.payload, UiPayload::Operator(_)));
        assert_eq!(board.variants.len(), 1);
        assert_eq!(board.migrations.len(), 1);
    }

    #[test]
    fn experiment_picker_marks_scope_eligible_entries() {
        let entry = ExperimentDirectoryEntry {
            network_id: NetworkId::new("net"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            workload_id: burn_p2p_core::WorkloadId::new("demo-workload"),
            display_name: "Demo".into(),
            model_schema_hash: burn_p2p_core::ContentId::new("model"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view"),
            resource_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::from([PeerRole::TrainerGpu]),
                minimum_device_memory_bytes: Some(1024),
                minimum_system_memory_bytes: Some(4096),
                estimated_download_bytes: 4096,
                estimated_window_seconds: 60,
            },
            visibility: ExperimentVisibility::OptIn,
            opt_in_policy: ExperimentOptInPolicy::Scoped,
            current_revision_id: RevisionId::new("rev"),
            current_head_id: Some(HeadId::new("head")),
            allowed_roles: PeerRoleSet::new([PeerRole::TrainerGpu]),
            allowed_scopes: BTreeSet::from([ExperimentScope::Train {
                experiment_id: ExperimentId::new("exp"),
            }]),
            metadata: BTreeMap::new(),
        };

        let picker = ExperimentPickerView::from_directory(
            NetworkId::new("net"),
            vec![entry],
            &[ExperimentScope::Train {
                experiment_id: ExperimentId::new("exp"),
            }],
        );

        assert_eq!(picker.entries.len(), 1);
        assert!(picker.entries[0].allowed);
    }

    #[test]
    fn browser_experiment_picker_sorts_and_serializes_browser_state() {
        let picker = BrowserExperimentPickerView::new(
            NetworkId::new("net"),
            vec![
                BrowserExperimentPickerCard {
                    experiment_id: ExperimentId::new("exp-b"),
                    study_id: StudyId::new("study"),
                    display_name: "Zulu".into(),
                    current_revision_id: RevisionId::new("rev-b"),
                    current_head_id: None,
                    estimated_download_bytes: 2048,
                    estimated_window_seconds: 30,
                    allowed: false,
                    recommended_state: BrowserExperimentPickerState::Blocked,
                    recommended_role: None,
                    fallback_from_preferred: false,
                    eligible_roles: Vec::new(),
                    blocked_reasons: vec!["missing scope".into()],
                },
                BrowserExperimentPickerCard {
                    experiment_id: ExperimentId::new("exp-a"),
                    study_id: StudyId::new("study"),
                    display_name: "Alpha".into(),
                    current_revision_id: RevisionId::new("rev-a"),
                    current_head_id: Some(HeadId::new("head-a")),
                    estimated_download_bytes: 1024,
                    estimated_window_seconds: 15,
                    allowed: true,
                    recommended_state: BrowserExperimentPickerState::Verifier,
                    recommended_role: Some(BrowserRole::Verifier),
                    fallback_from_preferred: true,
                    eligible_roles: vec![BrowserRole::Observer, BrowserRole::Verifier],
                    blocked_reasons: vec!["revision requires WebGPU support".into()],
                },
            ],
        );

        assert_eq!(picker.entries[0].display_name, "Alpha");
        let bytes = serde_json::to_vec(&picker).expect("serialize browser picker");
        let decoded: BrowserExperimentPickerView =
            serde_json::from_slice(&bytes).expect("deserialize browser picker");
        assert_eq!(
            decoded.entries[0].recommended_role,
            Some(BrowserRole::Verifier)
        );
        assert!(decoded.entries[0].fallback_from_preferred);
    }

    #[test]
    fn auth_portal_view_serializes_provider_and_trust_badges() {
        let view = AuthPortalView {
            network_id: NetworkId::new("net"),
            providers: vec![LoginProviderView {
                provider: AuthProvider::Static {
                    authority: "lab-auth".into(),
                },
                label: "Static".into(),
                login_path: "/login/static".into(),
                callback_path: Some("/callback/static".into()),
                device_path: Some("/device".into()),
            }],
            active_session: Some(ContributionIdentityPanel {
                principal_id: "alice".into(),
                provider_label: "lab-auth".into(),
                trust_badges: vec![TrustBadgeView {
                    level: PeerTrustLevel::PolicyCompliant,
                    label: "policy compliant".into(),
                }],
                scoped_experiments: vec![ExperimentId::new("exp")],
            }),
        };

        let bytes = serde_json::to_vec(&view).expect("serialize auth portal");
        let decoded: AuthPortalView =
            serde_json::from_slice(&bytes).expect("deserialize auth portal");
        assert_eq!(decoded.providers.len(), 1);
        assert_eq!(
            decoded.active_session.expect("session").trust_badges.len(),
            1
        );
    }

    #[test]
    fn merge_topology_views_build_window_load_and_aggregate_dag() {
        let now = Utc::now();
        let merge_window = MergeWindowState {
            merge_window_id: burn_p2p_core::ContentId::new("window-a"),
            network_id: NetworkId::new("network"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            window_id: WindowId(7),
            base_head_id: HeadId::new("head-0"),
            policy: MergeTopologyPolicy {
                strategy: MergeStrategy::ReplicatedRendezvousDag,
                ..MergeTopologyPolicy::default()
            },
            reducers: vec![PeerId::new("reducer-a"), PeerId::new("reducer-b")],
            validators: vec![PeerId::new("validator-a")],
            opened_at: now,
            closes_at: now + Duration::seconds(30),
        };
        let reducer = ReducerLoadReport {
            peer_id: PeerId::new("reducer-a"),
            window_id: WindowId(7),
            assigned_leaf_updates: 8,
            assigned_aggregate_inputs: 2,
            ingress_bytes: 1024,
            egress_bytes: 256,
            duplicate_transfer_ratio: 0.25,
            overload_ratio: 0.75,
            reported_at: now,
        };
        let aggregates = vec![
            AggregateEnvelope {
                aggregate_id: burn_p2p_core::ContentId::new("leaf-a"),
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                window_id: WindowId(7),
                base_head_id: HeadId::new("head-0"),
                aggregate_artifact_id: ArtifactId::new("agg-art-a"),
                tier: AggregateTier::Leaf,
                reducer_peer_id: PeerId::new("reducer-a"),
                contributor_peers: vec![PeerId::new("peer-a")],
                child_aggregate_ids: Vec::new(),
                stats: AggregateStats {
                    accepted_updates: 1,
                    duplicate_updates: 0,
                    dropped_updates: 0,
                    late_updates: 0,
                    sum_sample_weight: 1.0,
                    sum_quality_weight: 1.0,
                    sum_weighted_delta_norm: 1.0,
                    max_update_norm: 1.0,
                    accepted_sample_coverage: 1.0,
                },
                providers: vec![PeerId::new("reducer-a")],
                published_at: now,
            },
            AggregateEnvelope {
                aggregate_id: burn_p2p_core::ContentId::new("root-a"),
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("exp"),
                revision_id: RevisionId::new("rev"),
                window_id: WindowId(7),
                base_head_id: HeadId::new("head-0"),
                aggregate_artifact_id: ArtifactId::new("agg-art-root"),
                tier: AggregateTier::RootCandidate,
                reducer_peer_id: PeerId::new("validator-a"),
                contributor_peers: vec![PeerId::new("peer-a")],
                child_aggregate_ids: vec![burn_p2p_core::ContentId::new("leaf-a")],
                stats: AggregateStats {
                    accepted_updates: 1,
                    duplicate_updates: 0,
                    dropped_updates: 0,
                    late_updates: 0,
                    sum_sample_weight: 1.0,
                    sum_quality_weight: 1.0,
                    sum_weighted_delta_norm: 1.0,
                    max_update_norm: 1.0,
                    accepted_sample_coverage: 1.0,
                },
                providers: vec![PeerId::new("validator-a")],
                published_at: now,
            },
        ];
        let certificate = ReductionCertificate {
            reduction_id: burn_p2p_core::ContentId::new("cert-a"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            window_id: WindowId(7),
            base_head_id: HeadId::new("head-0"),
            aggregate_id: burn_p2p_core::ContentId::new("root-a"),
            validator: PeerId::new("validator-a"),
            validator_quorum: 2,
            cross_checked_reducers: vec![PeerId::new("reducer-a")],
            issued_at: now,
        };

        let dashboard = MergeTopologyDashboardView {
            merge_window: MergeWindowView::from_state(&merge_window),
            reducers: vec![ReducerUtilizationView::from(reducer)],
            aggregate_dag: AggregateDagView::from_aggregates(aggregates),
            promotion_timeline: vec![HeadPromotionTimelineEntry::from(certificate)],
        };

        assert_eq!(dashboard.merge_window.reducer_count, 2);
        assert_eq!(dashboard.aggregate_dag.nodes.len(), 2);
        assert_eq!(dashboard.aggregate_dag.edges.len(), 1);
        assert_eq!(dashboard.promotion_timeline.len(), 1);
    }
}
