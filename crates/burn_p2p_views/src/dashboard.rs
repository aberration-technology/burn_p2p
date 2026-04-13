use std::collections::{BTreeMap, BTreeSet};

use crate::robustness::RobustnessPanelView;
use burn_p2p_core::{
    AggregateEnvelope, AggregateTier, ArtifactId, AssignmentLease, ContentId,
    ContributionReceiptId, DatasetViewId, ExperimentId, HeadDescriptor, HeadId, MergeCertId,
    MergeCertificate, MergeStrategy, MergeWindowState, MetricValue, MicroShardId, NetworkEstimate,
    NetworkId, PeerId, PeerRoleSet, ReducerLoadReport, ReductionCertificate, RevisionId,
    RevocationEpoch, StudyId, WindowActivation, WindowId,
};
use burn_p2p_security::{PeerTrustLevel, ReputationDecision};
use burn_p2p_swarm::{AlertNotice, OverlayTopic};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One in-flight artifact transfer surfaced in operator diagnostics.
pub struct OperatorTransferView {
    /// Artifact identifier being transferred.
    pub artifact_id: ArtifactId,
    /// Human-readable transfer phase label.
    pub phase_label: String,
    /// Known source peers for the transfer.
    pub source_peers: Vec<PeerId>,
    /// Selected provider peer, when known.
    pub provider_peer_id: Option<PeerId>,
    /// Number of completed chunks observed so far.
    pub completed_chunk_count: usize,
    /// Total chunk count from the descriptor, when known.
    pub total_chunk_count: Option<usize>,
    /// Transfer start timestamp.
    pub started_at: DateTime<Utc>,
    /// Most recent update timestamp.
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One peer-level diagnostic row surfaced in operator diagnostics.
pub struct OperatorPeerDiagnosticView {
    /// Peer identifier.
    pub peer_id: PeerId,
    /// Whether the peer is currently connected.
    pub connected: bool,
    /// Most recent observation timestamp.
    pub observed_at: Option<DateTime<Utc>>,
    /// Trust level assigned to the peer.
    pub trust_level: Option<PeerTrustLevel>,
    /// Rejection reason, when present.
    pub rejection_reason: Option<String>,
    /// Current reputation score, when known.
    pub reputation_score: Option<f64>,
    /// Current reputation decision, when known.
    pub reputation_decision: Option<ReputationDecision>,
    /// Whether the peer is quarantined.
    pub quarantined: bool,
    /// Whether the peer is banned.
    pub banned: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Compact robustness summary carried alongside operator diagnostics.
pub struct OperatorRobustnessSummaryView {
    /// Total rejected updates in the latest screened cohort.
    pub rejected_updates: u64,
    /// Mean trust score in the current trust snapshot.
    pub mean_trust_score: f64,
    /// Number of quarantined peers in the current trust snapshot.
    pub quarantined_peer_count: u32,
    /// Number of peers escalated to a ban recommendation.
    pub ban_recommended_peer_count: u32,
    /// Number of candidate heads flagged by canary regression.
    pub canary_regression_count: u32,
    /// Number of active robustness alerts.
    pub alert_count: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Presentation-owned operator diagnostics payload.
pub struct OperatorDiagnosticsView {
    /// Network identifier for the diagnostics snapshot.
    pub network_id: NetworkId,
    /// Active preset label, when relevant.
    pub preset_label: String,
    /// Human-readable active service labels.
    pub active_services: Vec<String>,
    /// Runtime role set.
    pub roles: PeerRoleSet,
    /// Currently connected peers.
    pub connected_peers: u32,
    /// Connected peer identifiers.
    pub connected_peer_ids: Vec<PeerId>,
    /// Observed peer identifiers.
    pub observed_peers: Vec<PeerId>,
    /// Network estimate summary.
    pub network_estimate: NetworkEstimate,
    /// Pinned head identifiers.
    pub pinned_heads: BTreeSet<HeadId>,
    /// Pinned artifact identifiers.
    pub pinned_artifacts: BTreeSet<ArtifactId>,
    /// Accepted receipt count.
    pub accepted_receipts: usize,
    /// Certified merge count.
    pub certified_merges: usize,
    /// In-flight transfer summaries.
    pub in_flight_transfers: Vec<OperatorTransferView>,
    /// Admitted peer identifiers.
    pub admitted_peers: BTreeSet<PeerId>,
    /// Peer-level diagnostics.
    pub peer_diagnostics: Vec<OperatorPeerDiagnosticView>,
    /// Rejected peer reasons keyed by peer ID.
    pub rejected_peers: BTreeMap<PeerId, String>,
    /// Quarantined peer identifiers.
    pub quarantined_peers: BTreeSet<PeerId>,
    /// Banned peer identifiers.
    pub banned_peers: BTreeSet<PeerId>,
    #[serde(default)]
    /// Compact robustness summary aligned with the latest bootstrap diagnostics rollup.
    pub robustness_summary: Option<OperatorRobustnessSummaryView>,
    #[serde(default)]
    /// Detailed robustness panel aligned with the latest bootstrap diagnostics payload.
    pub robustness_panel: Option<RobustnessPanelView>,
    /// Minimum revocation epoch, when active.
    pub minimum_revocation_epoch: Option<RevocationEpoch>,
    /// Last operator-visible runtime error.
    pub last_error: Option<String>,
    /// Human-readable node state label.
    pub node_state_label: String,
    /// Human-readable slot state labels.
    pub slot_state_labels: Vec<String>,
    /// Capture timestamp.
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Overlay health summary shown in the operator console.
pub struct OverlayStatusView {
    /// Overlay topic identifier.
    pub overlay: OverlayTopic,
    /// Number of active peers tracked for the overlay.
    pub active_peers: u32,
    /// Number of currently connected peers for the overlay.
    pub connected_peers: u32,
    /// Most recent window identifier seen on the overlay.
    pub last_window_id: Option<WindowId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Status of a merge queue entry in the operator console.
pub enum MergeQueueStatus {
    /// Candidate updates are pending validation and merge.
    Pending,
    /// The queue entry produced a certified merge.
    Certified,
    /// The queue entry was rejected.
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One merge queue row in the operator console.
pub struct MergeQueueEntry {
    /// Base head identifier for the merge window.
    pub base_head_id: HeadId,
    /// Receipt identifiers participating in the candidate set.
    pub candidate_receipt_ids: Vec<ContributionReceiptId>,
    /// Current queue status.
    pub status: MergeQueueStatus,
    /// Merged head identifier, when present.
    pub merged_head_id: Option<HeadId>,
    /// Merged artifact identifier, when present.
    pub merged_artifact_id: Option<ArtifactId>,
    /// Merge certificate identifier, when present.
    pub merge_cert_id: Option<MergeCertId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One authority or admin action recorded for the operator console.
pub struct AuthorityActionRecord {
    /// Human-readable action label.
    pub action: String,
    /// Acting peer identifier, when applicable.
    pub actor_peer_id: Option<PeerId>,
    /// Timestamp when the action occurred.
    pub happened_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Operator-facing summary of diagnostics, overlays, merge state, and alerts.
pub struct OperatorConsoleView {
    /// Current bootstrap diagnostics snapshot.
    pub diagnostics: OperatorDiagnosticsView,
    /// Overlay-level health summaries.
    pub overlays: Vec<OverlayStatusView>,
    /// Merge queue rows.
    pub merge_queue: Vec<MergeQueueEntry>,
    /// Recent authority/admin actions.
    pub authority_actions: Vec<AuthorityActionRecord>,
    /// Active alerts surfaced to operators.
    pub alerts: Vec<AlertNotice>,
}

impl OperatorConsoleView {
    /// Creates an operator console view.
    pub fn new(
        diagnostics: OperatorDiagnosticsView,
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
/// One timestamped metric point for an experiment variant.
pub struct MetricPoint {
    /// Window identifier associated with the metrics.
    pub window_id: WindowId,
    /// Named metric values.
    pub metrics: BTreeMap<String, MetricValue>,
    /// Timestamp when the point was captured.
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One cost-versus-value point for an experiment variant.
pub struct CostPerformancePoint {
    /// Human-readable label for the point.
    pub label: String,
    /// Relative or absolute cost.
    pub cost: f64,
    /// Relative or absolute value.
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Per-variant summary shown on a study board.
pub struct ExperimentVariantView {
    /// Study identifier.
    pub study_id: StudyId,
    /// Experiment identifier.
    pub experiment_id: ExperimentId,
    /// Revision identifier.
    pub revision_id: RevisionId,
    /// Time series metrics.
    pub metrics: Vec<MetricPoint>,
    /// Accepted work accumulated by the variant.
    pub accepted_work: u64,
    /// Cost-versus-value points for comparison.
    pub cost_performance: Vec<CostPerformancePoint>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One experiment migration entry on the study board.
pub struct ExperimentMigrationView {
    /// Experiment identifier.
    pub experiment_id: ExperimentId,
    /// Source revision identifier.
    pub from_revision_id: RevisionId,
    /// Target revision identifier.
    pub to_revision_id: RevisionId,
    /// Activation boundary for the migration.
    pub activation: WindowActivation,
    /// Optional operator note.
    pub note: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Study-level comparison board across variants and migrations.
pub struct StudyBoardView {
    /// Network identifier.
    pub network_id: NetworkId,
    /// Study identifier.
    pub study_id: StudyId,
    /// Variant-level summaries.
    pub variants: Vec<ExperimentVariantView>,
    /// Migration timeline entries.
    pub migrations: Vec<ExperimentMigrationView>,
}

impl StudyBoardView {
    /// Creates a study board view.
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
/// Summary of the current merge window.
pub struct MergeWindowView {
    /// Merge window identifier.
    pub merge_window_id: ContentId,
    /// Experiment identifier.
    pub experiment_id: ExperimentId,
    /// Revision identifier.
    pub revision_id: RevisionId,
    /// Base head identifier.
    pub base_head_id: HeadId,
    /// Merge strategy in effect.
    pub strategy: MergeStrategy,
    /// Number of reducers assigned to the window.
    pub reducer_count: usize,
    /// Number of validators assigned to the window.
    pub validator_count: usize,
    /// Window open timestamp.
    pub opened_at: DateTime<Utc>,
    /// Window close timestamp.
    pub closes_at: DateTime<Utc>,
}

impl MergeWindowView {
    /// Creates a merge window view from runtime state.
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
/// Per-reducer utilization and overload summary.
pub struct ReducerUtilizationView {
    /// Reducer peer identifier.
    pub peer_id: PeerId,
    /// Assigned leaf update count.
    pub assigned_leaf_updates: u32,
    /// Assigned aggregate input count.
    pub assigned_aggregate_inputs: u32,
    /// Observed ingress bytes.
    pub ingress_bytes: u128,
    /// Observed egress bytes.
    pub egress_bytes: u128,
    /// Duplicate transfer ratio.
    pub duplicate_transfer_ratio: f64,
    /// Overload ratio.
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
/// One aggregate node in the reducer DAG view.
pub struct AggregateDagNode {
    /// Aggregate identifier.
    pub aggregate_id: ContentId,
    /// Reducer peer identifier responsible for the node.
    pub reducer_peer_id: PeerId,
    /// Aggregate tier.
    pub tier: AggregateTier,
    /// Artifact identifier produced by the aggregate.
    pub artifact_id: ArtifactId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One edge between aggregate nodes in the DAG view.
pub struct AggregateDagEdge {
    /// Child aggregate identifier.
    pub from_aggregate_id: ContentId,
    /// Parent aggregate identifier.
    pub to_aggregate_id: ContentId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// DAG view over reducer aggregates.
pub struct AggregateDagView {
    /// Aggregate nodes.
    pub nodes: Vec<AggregateDagNode>,
    /// Aggregate edges.
    pub edges: Vec<AggregateDagEdge>,
}

impl AggregateDagView {
    /// Builds an aggregate DAG view from envelopes.
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
/// One promotion event in the certified head timeline.
pub struct HeadPromotionTimelineEntry {
    /// Reduction identifier.
    pub reduction_id: ContentId,
    /// Aggregate identifier that was certified.
    pub aggregate_id: ContentId,
    /// Promoter peer identifier.
    pub promoter_peer_id: PeerId,
    /// Promotion quorum used for the promotion.
    pub promotion_quorum: u16,
    /// Timestamp when the certificate was issued.
    pub issued_at: DateTime<Utc>,
}

impl From<ReductionCertificate> for HeadPromotionTimelineEntry {
    fn from(value: ReductionCertificate) -> Self {
        Self {
            reduction_id: value.reduction_id,
            aggregate_id: value.aggregate_id,
            promoter_peer_id: value.promoter_peer_id,
            promotion_quorum: value.promotion_quorum,
            issued_at: value.issued_at,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Merge-topology dashboard payload for operator visualization.
pub struct MergeTopologyDashboardView {
    /// Current merge window summary.
    pub merge_window: MergeWindowView,
    /// Reducer utilization rows.
    pub reducers: Vec<ReducerUtilizationView>,
    /// Aggregate DAG for the window.
    pub aggregate_dag: AggregateDagView,
    /// Promotion timeline entries.
    pub promotion_timeline: Vec<HeadPromotionTimelineEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Edge kind used in the checkpoint DAG.
pub enum CheckpointDagEdgeKind {
    /// Parent-child lineage edge.
    Lineage,
    /// Certified merge edge.
    CertifiedMerge,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One node in the checkpoint DAG.
pub struct CheckpointDagNode {
    /// Head identifier.
    pub head_id: HeadId,
    /// Parent head identifier, if any.
    pub parent_head_id: Option<HeadId>,
    /// Artifact identifier for the head.
    pub artifact_id: ArtifactId,
    /// Global step for the head.
    pub global_step: u64,
    /// Head creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Metrics attached to the head.
    pub metrics: BTreeMap<String, MetricValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One edge in the checkpoint DAG.
pub struct CheckpointDagEdge {
    /// Source head identifier.
    pub from_head_id: HeadId,
    /// Destination head identifier.
    pub to_head_id: HeadId,
    /// Edge kind.
    pub kind: CheckpointDagEdgeKind,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// DAG view across certified heads and merge relationships.
pub struct CheckpointDagView {
    /// Head nodes.
    pub nodes: Vec<CheckpointDagNode>,
    /// DAG edges.
    pub edges: Vec<CheckpointDagEdge>,
    /// Latest visible head identifier, if any.
    pub latest_head_id: Option<HeadId>,
}

impl CheckpointDagView {
    /// Builds a checkpoint DAG view from heads and merge certificates.
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
/// One step in an EMA merge flow.
pub struct EmaFlowStep {
    /// Base head identifier.
    pub base_head_id: HeadId,
    /// Merged head identifier.
    pub merged_head_id: HeadId,
    /// Merge certificate identifier.
    pub merge_cert_id: MergeCertId,
    /// Certificate issue timestamp.
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Timeline of certified EMA merge steps.
pub struct EmaFlowView {
    /// Merge-flow steps sorted by issue time.
    pub steps: Vec<EmaFlowStep>,
}

impl EmaFlowView {
    /// Builds an EMA flow view from merge certificates.
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
/// One cell in the shard assignment heatmap.
pub struct ShardAssignmentCell {
    /// Peer identifier.
    pub peer_id: PeerId,
    /// Microshard identifier.
    pub microshard_id: MicroShardId,
    /// Number of leases observed for the pair.
    pub lease_count: u32,
    /// Aggregate work-unit budget across the leases.
    pub total_budget_work_units: u64,
    /// Latest window identifier seen for the pair.
    pub latest_window_id: WindowId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Heatmap view over peer-to-microshard lease assignments.
pub struct ShardAssignmentHeatmap {
    /// Dataset view identifier, when leases are present.
    pub dataset_view_id: Option<DatasetViewId>,
    /// Heatmap cells.
    pub cells: Vec<ShardAssignmentCell>,
}

impl ShardAssignmentHeatmap {
    /// Builds a heatmap from lease records.
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
/// UI channel names used by event envelopes.
pub enum UiChannel {
    /// Operator console channel.
    Operator,
    /// Participant-facing portal channel.
    Participant,
    /// Study-board channel.
    StudyBoard,
    /// Checkpoint-DAG channel.
    CheckpointDag,
    /// EMA-flow channel.
    EmaFlow,
    /// Shard-heatmap channel.
    Heatmap,
    /// Alert channel.
    Alert,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// UI payload variants carried by event envelopes.
pub enum UiPayload {
    /// Operator console payload.
    Operator(Box<OperatorConsoleView>),
    /// Participant portal payload.
    Participant(Box<crate::ParticipantAppView>),
    /// Study-board payload.
    StudyBoard(StudyBoardView),
    /// Checkpoint-DAG payload.
    CheckpointDag(CheckpointDagView),
    /// EMA-flow payload.
    EmaFlow(EmaFlowView),
    /// Shard-heatmap payload.
    Heatmap(ShardAssignmentHeatmap),
    /// Alert payload.
    Alert(AlertNotice),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Envelope that adds sequencing and timestamps to UI payloads.
pub struct UiEventEnvelope {
    /// Logical UI channel.
    pub channel: UiChannel,
    /// Monotonic sequence number.
    pub sequence: u64,
    /// Emission timestamp.
    pub emitted_at: DateTime<Utc>,
    /// Wrapped payload.
    pub payload: UiPayload,
}
