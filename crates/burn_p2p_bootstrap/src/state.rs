use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use crate::{
    StoredEvalProtocolManifestRecord,
    deploy::BootstrapPlan,
    operator_store::{
        FileOperatorStore, FileOperatorStoreConfig, FileOperatorStorePreview,
        OperatorStateBackendConfig, OperatorStore, persist_operator_state_snapshot,
    },
};
use burn_p2p::{
    ArtifactTransferState, ContributionReceipt, ContributionReceiptId,
    ExperimentLifecycleAnnouncement, FleetScheduleAnnouncement, HeadAnnouncement, HeadDescriptor,
    MetricsRetentionBudget, NodeRuntimeState, NodeTelemetrySnapshot, ReducerLoadAnnouncement,
    RequestFailureCounter, RevocationEpoch, SlotRuntimeState, StorageConfig,
};
use burn_p2p_core::{
    ExperimentId, HeadEvalReport, HeadId, MergeCertificate, NetworkId, Page, PageRequest, PeerId,
    PeerWindowMetrics, ReducerCohortMetrics, RevisionId, StudyId, TrustBundleExport, WindowId,
    operator_visible_last_error,
};
#[cfg(feature = "metrics-indexer")]
use burn_p2p_metrics::{RobustnessRollup, derive_robustness_rollup};
use burn_p2p_security::{
    PeerAdmissionReport, PeerTrustLevel, ReputationDecision, ReputationEngine, ReputationState,
};
use burn_p2p_swarm::{PeerStore, SwarmStats};
use burn_p2p_views::RobustnessPanelView;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(feature = "metrics-indexer")]
type BootstrapRobustnessRollup = RobustnessRollup;
#[cfg(not(feature = "metrics-indexer"))]
type BootstrapRobustnessRollup = serde_json::Value;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a receipt query.
pub struct ReceiptQuery {
    /// The study ID.
    pub study_id: Option<StudyId>,
    /// The experiment ID.
    pub experiment_id: Option<ExperimentId>,
    /// The revision ID.
    pub revision_id: Option<RevisionId>,
    /// The peer ID.
    pub peer_id: Option<PeerId>,
}

impl ReceiptQuery {
    /// Performs the matches operation.
    pub fn matches(&self, receipt: &ContributionReceipt) -> bool {
        self.study_id
            .as_ref()
            .is_none_or(|study_id| &receipt.study_id == study_id)
            && self
                .experiment_id
                .as_ref()
                .is_none_or(|experiment_id| &receipt.experiment_id == experiment_id)
            && self
                .revision_id
                .as_ref()
                .is_none_or(|revision_id| &receipt.revision_id == revision_id)
            && self
                .peer_id
                .as_ref()
                .is_none_or(|peer_id| &receipt.peer_id == peer_id)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a head query.
pub struct HeadQuery {
    /// The study ID.
    pub study_id: Option<StudyId>,
    /// The experiment ID.
    pub experiment_id: Option<ExperimentId>,
    /// The revision ID.
    pub revision_id: Option<RevisionId>,
    /// The head ID.
    pub head_id: Option<HeadId>,
}

impl HeadQuery {
    /// Performs the matches operation.
    pub fn matches(&self, head: &HeadDescriptor) -> bool {
        self.study_id
            .as_ref()
            .is_none_or(|study_id| &head.study_id == study_id)
            && self
                .experiment_id
                .as_ref()
                .is_none_or(|experiment_id| &head.experiment_id == experiment_id)
            && self
                .revision_id
                .as_ref()
                .is_none_or(|revision_id| &head.revision_id == revision_id)
            && self
                .head_id
                .as_ref()
                .is_none_or(|head_id| &head.head_id == head_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a reducer load query.
pub struct ReducerLoadQuery {
    /// The study ID.
    pub study_id: Option<StudyId>,
    /// The experiment ID.
    pub experiment_id: Option<ExperimentId>,
    /// The peer ID.
    pub peer_id: Option<PeerId>,
}

impl ReducerLoadQuery {
    /// Performs the matches operation.
    pub fn matches(&self, announcement: &ReducerLoadAnnouncement) -> bool {
        self.study_id
            .as_ref()
            .is_none_or(|study_id| announcement.overlay.study_id.as_ref() == Some(study_id))
            && self.experiment_id.as_ref().is_none_or(|experiment_id| {
                announcement.overlay.experiment_id.as_ref() == Some(experiment_id)
            })
            && self
                .peer_id
                .as_ref()
                .is_none_or(|peer_id| &announcement.report.peer_id == peer_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the audit record kinds surfaced by the operator plane.
pub enum OperatorAuditKind {
    Receipt,
    Head,
    Merge,
    LifecyclePlan,
    ScheduleEpoch,
    PeerWindowMetric,
    ReducerCohortMetric,
    HeadEvalReport,
    Snapshot,
}

impl OperatorAuditKind {
    /// Returns the stable slug used by HTTP and external backends.
    pub const fn as_slug(&self) -> &'static str {
        match self {
            Self::Receipt => "receipt",
            Self::Head => "head",
            Self::Merge => "merge",
            Self::LifecyclePlan => "lifecycle-plan",
            Self::ScheduleEpoch => "schedule-epoch",
            Self::PeerWindowMetric => "peer-window-metric",
            Self::ReducerCohortMetric => "reducer-cohort-metric",
            Self::HeadEvalReport => "head-eval-report",
            Self::Snapshot => "snapshot",
        }
    }

    /// Parses one stable audit kind slug.
    pub fn from_slug(slug: &str) -> Option<Self> {
        match slug {
            "receipt" => Some(Self::Receipt),
            "head" => Some(Self::Head),
            "merge" => Some(Self::Merge),
            "lifecycle-plan" => Some(Self::LifecyclePlan),
            "schedule-epoch" => Some(Self::ScheduleEpoch),
            "peer-window-metric" => Some(Self::PeerWindowMetric),
            "reducer-cohort-metric" => Some(Self::ReducerCohortMetric),
            "head-eval-report" => Some(Self::HeadEvalReport),
            "snapshot" => Some(Self::Snapshot),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Query parameters used to filter operator audit and replay exports.
pub struct OperatorAuditQuery {
    /// Optional audit record kind filter.
    pub kind: Option<OperatorAuditKind>,
    /// Optional study filter.
    pub study_id: Option<StudyId>,
    /// Optional experiment filter.
    pub experiment_id: Option<ExperimentId>,
    /// Optional revision filter.
    pub revision_id: Option<RevisionId>,
    /// Optional peer filter.
    pub peer_id: Option<PeerId>,
    /// Optional head filter.
    pub head_id: Option<HeadId>,
    /// Optional lower bound for captured timestamps.
    pub since: Option<DateTime<Utc>>,
    /// Optional upper bound for captured timestamps.
    pub until: Option<DateTime<Utc>>,
    /// Optional free-text substring filter.
    pub text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One flattened operator audit record suitable for search, retention, and replay.
pub struct OperatorAuditRecord {
    /// Record kind.
    pub kind: OperatorAuditKind,
    /// Stable local record identifier.
    pub record_id: String,
    /// Optional study scope.
    pub study_id: Option<StudyId>,
    /// Optional experiment scope.
    pub experiment_id: Option<ExperimentId>,
    /// Optional revision scope.
    pub revision_id: Option<RevisionId>,
    /// Optional peer scope.
    pub peer_id: Option<PeerId>,
    /// Optional head scope.
    pub head_id: Option<HeadId>,
    /// Timestamp captured for replay ordering.
    pub captured_at: DateTime<Utc>,
    /// Flattened searchable summary map.
    pub summary: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Aggregate audit-search summary for the current operator backend.
pub struct OperatorAuditSummary {
    /// Human-readable backend label.
    pub backend: String,
    /// Total records matching the supplied query.
    pub record_count: usize,
    /// Per-kind counts for matching records keyed by the stable kind slug.
    pub counts_by_kind: BTreeMap<String, usize>,
    /// Distinct studies visible across matching audit rows.
    pub distinct_study_count: usize,
    /// Distinct experiments visible across matching audit rows.
    pub distinct_experiment_count: usize,
    /// Distinct revisions visible across matching audit rows.
    pub distinct_revision_count: usize,
    /// Distinct peers visible across matching audit rows.
    pub distinct_peer_count: usize,
    /// Distinct heads visible across matching audit rows.
    pub distinct_head_count: usize,
    /// Earliest matching capture timestamp.
    pub earliest_captured_at: Option<DateTime<Utc>>,
    /// Latest matching capture timestamp.
    pub latest_captured_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One top facet bucket surfaced by operator audit search tooling.
pub struct OperatorFacetBucket {
    /// Normalized facet value.
    pub value: String,
    /// Count of records that matched the facet value.
    pub count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Top facet values across one filtered operator audit search query.
pub struct OperatorAuditFacetSummary {
    /// Human-readable backend label.
    pub backend: String,
    /// Maximum buckets retained per facet list.
    pub limit: usize,
    /// Top record kinds.
    pub kinds: Vec<OperatorFacetBucket>,
    /// Top study identifiers.
    pub studies: Vec<OperatorFacetBucket>,
    /// Top experiment identifiers.
    pub experiments: Vec<OperatorFacetBucket>,
    /// Top revision identifiers.
    pub revisions: Vec<OperatorFacetBucket>,
    /// Top peer identifiers.
    pub peers: Vec<OperatorFacetBucket>,
    /// Top head identifiers.
    pub heads: Vec<OperatorFacetBucket>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the control-plane replay record kinds surfaced by the operator plane.
pub enum OperatorControlReplayKind {
    LifecyclePlan,
    ScheduleEpoch,
}

impl OperatorControlReplayKind {
    /// Returns the stable slug used by HTTP and external backends.
    pub const fn as_slug(&self) -> &'static str {
        match self {
            Self::LifecyclePlan => "lifecycle-plan",
            Self::ScheduleEpoch => "schedule-epoch",
        }
    }

    /// Parses one stable control replay kind slug.
    pub fn from_slug(slug: &str) -> Option<Self> {
        match slug {
            "lifecycle-plan" => Some(Self::LifecyclePlan),
            "schedule-epoch" => Some(Self::ScheduleEpoch),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Query parameters used to filter typed control-plane replay exports.
pub struct OperatorControlReplayQuery {
    /// Optional replay kind filter.
    pub kind: Option<OperatorControlReplayKind>,
    /// Optional network filter.
    pub network_id: Option<NetworkId>,
    /// Optional study filter.
    pub study_id: Option<StudyId>,
    /// Optional experiment filter.
    pub experiment_id: Option<ExperimentId>,
    /// Optional revision filter.
    pub revision_id: Option<RevisionId>,
    /// Optional peer filter.
    pub peer_id: Option<PeerId>,
    /// Optional activation-window filter.
    pub window_id: Option<WindowId>,
    /// Optional lower bound for captured timestamps.
    pub since: Option<DateTime<Utc>>,
    /// Optional upper bound for captured timestamps.
    pub until: Option<DateTime<Utc>>,
    /// Optional free-text substring filter.
    pub text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One typed control-plane replay record derived from retained operator snapshots.
pub struct OperatorControlReplayRecord {
    /// Record kind.
    pub kind: OperatorControlReplayKind,
    /// Stable local record identifier.
    pub record_id: String,
    /// Network scope.
    pub network_id: NetworkId,
    /// Study scope.
    pub study_id: StudyId,
    /// Effective experiment scope.
    pub experiment_id: ExperimentId,
    /// Effective revision scope.
    pub revision_id: RevisionId,
    /// Optional source experiment scope for cross-experiment lifecycle plans.
    pub source_experiment_id: Option<ExperimentId>,
    /// Optional source revision scope when the plan expects one before activation.
    pub source_revision_id: Option<RevisionId>,
    /// Optional peer scope carried by schedule assignments.
    pub peer_id: Option<PeerId>,
    /// Activation window for the record.
    pub window_id: WindowId,
    /// Optional exclusive upper window bound for schedule epochs.
    pub ends_before_window: Option<WindowId>,
    /// Optional authoritative slot index.
    pub slot_index: Option<usize>,
    /// Monotonic operator epoch for competing plans.
    pub plan_epoch: u64,
    /// Timestamp captured for replay ordering.
    pub captured_at: DateTime<Utc>,
    /// Flattened searchable summary map.
    pub summary: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Aggregate summary for one typed control-plane replay query.
pub struct OperatorControlReplaySummary {
    /// Human-readable backend label.
    pub backend: String,
    /// Total records matching the supplied query.
    pub record_count: usize,
    /// Per-kind counts for matching records keyed by the stable kind slug.
    pub counts_by_kind: BTreeMap<String, usize>,
    /// Distinct networks visible across matching control rows.
    pub distinct_network_count: usize,
    /// Distinct studies visible across matching control rows.
    pub distinct_study_count: usize,
    /// Distinct experiments visible across matching control rows.
    pub distinct_experiment_count: usize,
    /// Distinct revisions visible across matching control rows.
    pub distinct_revision_count: usize,
    /// Distinct peers visible across matching control rows.
    pub distinct_peer_count: usize,
    /// Distinct activation windows visible across matching control rows.
    pub distinct_window_count: usize,
    /// Earliest matching capture timestamp.
    pub earliest_captured_at: Option<DateTime<Utc>>,
    /// Latest matching capture timestamp.
    pub latest_captured_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Query parameters used to filter retained operator replay snapshots.
pub struct OperatorReplayQuery {
    /// Optional study filter.
    pub study_id: Option<StudyId>,
    /// Optional experiment filter.
    pub experiment_id: Option<ExperimentId>,
    /// Optional revision filter.
    pub revision_id: Option<RevisionId>,
    /// Optional head filter.
    pub head_id: Option<HeadId>,
    /// Optional lower bound for captured timestamps.
    pub since: Option<DateTime<Utc>>,
    /// Optional upper bound for captured timestamps.
    pub until: Option<DateTime<Utc>>,
    /// Optional free-text substring filter.
    pub text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One retained operator replay snapshot summary suitable for paging and search.
pub struct OperatorReplaySnapshotSummary {
    /// Snapshot timestamp.
    pub captured_at: DateTime<Utc>,
    /// Distinct studies retained in the snapshot.
    pub study_ids: Vec<StudyId>,
    /// Distinct experiments retained in the snapshot.
    pub experiment_ids: Vec<ExperimentId>,
    /// Distinct revisions retained in the snapshot.
    pub revision_ids: Vec<RevisionId>,
    /// Distinct heads retained in the snapshot.
    pub head_ids: Vec<HeadId>,
    /// Count of retained receipts.
    pub receipt_count: usize,
    /// Count of retained heads.
    pub head_count: usize,
    /// Count of retained merges.
    pub merge_count: usize,
    /// Count of retained lifecycle plans.
    pub lifecycle_plan_count: usize,
    /// Count of retained schedule epochs.
    pub schedule_epoch_count: usize,
    /// Count of retained peer-window metrics.
    pub peer_window_metric_count: usize,
    /// Count of retained reducer-cohort metrics.
    pub reducer_cohort_metric_count: usize,
    /// Count of retained head-eval reports.
    pub head_eval_report_count: usize,
    /// Flattened searchable summary map.
    pub summary: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Summarizes operator retention state for the active backend.
pub struct OperatorRetentionSummary {
    /// Human-readable backend label.
    pub backend: String,
    /// Configured metrics retention budget.
    pub metrics_retention: MetricsRetentionBudget,
    /// Maximum retained replay snapshots for the backend snapshot table.
    pub snapshot_retention_limit: usize,
    /// Maximum retained audit rows for the backend audit table.
    pub audit_retention_limit: usize,
    /// Persisted retained replay snapshots currently available.
    pub persisted_snapshot_count: usize,
    /// Persisted retained audit rows currently available.
    pub persisted_audit_record_count: usize,
    /// Latest retained replay snapshot timestamp.
    pub latest_snapshot_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Reports one explicit operator retention prune operation against the active backend.
pub struct OperatorRetentionPruneResult {
    /// Human-readable backend label.
    pub backend: String,
    /// Retained replay snapshots removed by the prune.
    pub pruned_snapshot_count: usize,
    /// Retained audit rows removed by the prune.
    pub pruned_audit_record_count: usize,
    /// Persisted retained replay snapshots still available after pruning.
    pub remaining_snapshot_count: usize,
    /// Persisted retained audit rows still available after pruning.
    pub remaining_audit_record_count: usize,
    /// Latest retained replay snapshot timestamp after pruning.
    pub latest_snapshot_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// One retained operator snapshot suitable for state replay/export.
pub struct OperatorReplaySnapshot {
    /// Timestamp at which the snapshot was captured.
    pub captured_at: DateTime<Utc>,
    /// Retained receipts.
    pub receipts: Vec<ContributionReceipt>,
    /// Retained head descriptors.
    pub heads: Vec<HeadDescriptor>,
    /// Retained merge certificates.
    pub merges: Vec<MergeCertificate>,
    /// Retained lifecycle announcements.
    pub lifecycle_announcements: Vec<ExperimentLifecycleAnnouncement>,
    /// Retained fleet schedule announcements.
    pub schedule_announcements: Vec<FleetScheduleAnnouncement>,
    /// Retained peer-window metrics.
    pub peer_window_metrics: Vec<PeerWindowMetrics>,
    /// Retained reducer cohort metrics.
    pub reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    /// Retained head evaluation reports.
    pub head_eval_reports: Vec<HeadEvalReport>,
    /// Retained eval protocol manifests.
    pub eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
}

impl OperatorAuditQuery {
    /// Returns whether the audit query matches the supplied record.
    pub fn matches(&self, record: &OperatorAuditRecord) -> bool {
        let text_matches = self.text.as_ref().is_none_or(|text| {
            let needle = text.trim().to_ascii_lowercase();
            if needle.is_empty() {
                return true;
            }
            record.record_id.to_ascii_lowercase().contains(&needle)
                || record.summary.iter().any(|(key, value)| {
                    key.to_ascii_lowercase().contains(&needle)
                        || value.to_ascii_lowercase().contains(&needle)
                })
        });

        self.kind.as_ref().is_none_or(|kind| &record.kind == kind)
            && self
                .study_id
                .as_ref()
                .is_none_or(|study_id| record.study_id.as_ref() == Some(study_id))
            && self
                .experiment_id
                .as_ref()
                .is_none_or(|experiment_id| record.experiment_id.as_ref() == Some(experiment_id))
            && self
                .revision_id
                .as_ref()
                .is_none_or(|revision_id| record.revision_id.as_ref() == Some(revision_id))
            && self
                .peer_id
                .as_ref()
                .is_none_or(|peer_id| record.peer_id.as_ref() == Some(peer_id))
            && self
                .head_id
                .as_ref()
                .is_none_or(|head_id| record.head_id.as_ref() == Some(head_id))
            && self
                .since
                .as_ref()
                .is_none_or(|since| &record.captured_at >= since)
            && self
                .until
                .as_ref()
                .is_none_or(|until| &record.captured_at <= until)
            && text_matches
    }
}

impl OperatorReplayQuery {
    /// Returns whether the replay query matches the supplied retained snapshot summary.
    pub fn matches(&self, snapshot: &OperatorReplaySnapshotSummary) -> bool {
        let text_matches = self.text.as_ref().is_none_or(|text| {
            let needle = text.trim().to_ascii_lowercase();
            if needle.is_empty() {
                return true;
            }
            snapshot
                .study_ids
                .iter()
                .map(StudyId::as_str)
                .chain(snapshot.experiment_ids.iter().map(ExperimentId::as_str))
                .chain(snapshot.revision_ids.iter().map(RevisionId::as_str))
                .chain(snapshot.head_ids.iter().map(HeadId::as_str))
                .any(|value| value.to_ascii_lowercase().contains(&needle))
                || snapshot.summary.iter().any(|(key, value)| {
                    key.to_ascii_lowercase().contains(&needle)
                        || value.to_ascii_lowercase().contains(&needle)
                })
        });

        self.study_id.as_ref().is_none_or(|study_id| {
            snapshot
                .study_ids
                .iter()
                .any(|candidate| candidate == study_id)
        }) && self.experiment_id.as_ref().is_none_or(|experiment_id| {
            snapshot
                .experiment_ids
                .iter()
                .any(|candidate| candidate == experiment_id)
        }) && self.revision_id.as_ref().is_none_or(|revision_id| {
            snapshot
                .revision_ids
                .iter()
                .any(|candidate| candidate == revision_id)
        }) && self.head_id.as_ref().is_none_or(|head_id| {
            snapshot
                .head_ids
                .iter()
                .any(|candidate| candidate == head_id)
        }) && self
            .since
            .as_ref()
            .is_none_or(|since| &snapshot.captured_at >= since)
            && self
                .until
                .as_ref()
                .is_none_or(|until| &snapshot.captured_at <= until)
            && text_matches
    }
}

impl OperatorControlReplayQuery {
    /// Returns whether the query matches the supplied typed control replay record.
    pub fn matches(&self, record: &OperatorControlReplayRecord) -> bool {
        let text_matches = self.text.as_ref().is_none_or(|text| {
            let needle = text.trim().to_ascii_lowercase();
            if needle.is_empty() {
                return true;
            }
            record.record_id.to_ascii_lowercase().contains(&needle)
                || record
                    .network_id
                    .as_str()
                    .to_ascii_lowercase()
                    .contains(&needle)
                || record
                    .study_id
                    .as_str()
                    .to_ascii_lowercase()
                    .contains(&needle)
                || record
                    .experiment_id
                    .as_str()
                    .to_ascii_lowercase()
                    .contains(&needle)
                || record
                    .revision_id
                    .as_str()
                    .to_ascii_lowercase()
                    .contains(&needle)
                || record
                    .source_experiment_id
                    .as_ref()
                    .is_some_and(|value| value.as_str().to_ascii_lowercase().contains(&needle))
                || record
                    .source_revision_id
                    .as_ref()
                    .is_some_and(|value| value.as_str().to_ascii_lowercase().contains(&needle))
                || record
                    .peer_id
                    .as_ref()
                    .is_some_and(|value| value.as_str().to_ascii_lowercase().contains(&needle))
                || record.summary.iter().any(|(key, value)| {
                    key.to_ascii_lowercase().contains(&needle)
                        || value.to_ascii_lowercase().contains(&needle)
                })
        });

        self.kind.as_ref().is_none_or(|kind| &record.kind == kind)
            && self
                .network_id
                .as_ref()
                .is_none_or(|network_id| &record.network_id == network_id)
            && self
                .study_id
                .as_ref()
                .is_none_or(|study_id| &record.study_id == study_id)
            && self.experiment_id.as_ref().is_none_or(|experiment_id| {
                &record.experiment_id == experiment_id
                    || record.source_experiment_id.as_ref() == Some(experiment_id)
            })
            && self.revision_id.as_ref().is_none_or(|revision_id| {
                &record.revision_id == revision_id
                    || record.source_revision_id.as_ref() == Some(revision_id)
            })
            && self
                .peer_id
                .as_ref()
                .is_none_or(|peer_id| record.peer_id.as_ref() == Some(peer_id))
            && self
                .window_id
                .as_ref()
                .is_none_or(|window_id| &record.window_id == window_id)
            && self
                .since
                .as_ref()
                .is_none_or(|since| &record.captured_at >= since)
            && self
                .until
                .as_ref()
                .is_none_or(|until| &record.captured_at <= until)
            && text_matches
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
/// Captures bootstrap admin state.
pub struct BootstrapAdminState {
    /// The head descriptors.
    pub head_descriptors: Vec<HeadDescriptor>,
    /// Directly registered live head announcements from colocated head mirrors.
    pub live_head_announcements: Vec<HeadAnnouncement>,
    /// The peer store.
    pub peer_store: PeerStore,
    /// The contribution receipts.
    pub contribution_receipts: Vec<ContributionReceipt>,
    /// Total persisted contribution receipts available from the durable history store.
    pub persisted_receipt_count: usize,
    /// The merge certificates.
    pub merge_certificates: Vec<MergeCertificate>,
    /// Total persisted merge certificates available from the durable history store.
    pub persisted_merge_count: usize,
    /// Total persisted heads available from the durable history store.
    pub persisted_head_count: usize,
    /// The peer-window metrics loaded from runtime storage.
    pub peer_window_metrics: Vec<PeerWindowMetrics>,
    /// The reducer cohort metrics loaded from runtime storage.
    pub reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    /// The head evaluation reports loaded from runtime storage.
    pub head_eval_reports: Vec<HeadEvalReport>,
    /// The scoped evaluation protocol manifests loaded from runtime storage.
    pub eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
    /// The runtime storage root used to materialize canonical artifacts.
    pub artifact_store_root: Option<PathBuf>,
    /// Optional externalized operator-state backend for multi-edge read coherence.
    #[doc(hidden)]
    pub operator_state_backend: Option<OperatorStateBackendConfig>,
    /// The durable operator history root used for on-demand receipts, heads, and merge queries.
    pub history_root: Option<PathBuf>,
    /// The durable publication store root, when artifact publication is enabled.
    pub publication_store_root: Option<PathBuf>,
    /// Explicit artifact publication targets configured by the operator.
    pub publication_targets: Vec<burn_p2p_core::PublicationTarget>,
    /// The durable metrics store root, when the metrics indexer is enabled.
    pub metrics_store_root: Option<PathBuf>,
    /// The effective metrics retention budget used for raw-history tails and drilldowns.
    pub metrics_retention: MetricsRetentionBudget,
    /// The reducer load announcements.
    pub reducer_load_announcements: Vec<ReducerLoadAnnouncement>,
    /// The in flight transfers.
    pub in_flight_transfers: Vec<ArtifactTransferState>,
    #[serde(default)]
    /// Request failure counters grouped by operation and reason.
    pub request_failures: Vec<RequestFailureCounter>,
    /// The admitted peers.
    pub admitted_peers: BTreeSet<PeerId>,
    /// The peer admission reports.
    pub peer_admission_reports: BTreeMap<PeerId, PeerAdmissionReport>,
    /// The rejected peers.
    pub rejected_peers: BTreeMap<PeerId, String>,
    /// The peer reputation.
    pub peer_reputation: BTreeMap<PeerId, ReputationState>,
    /// The quarantined peers.
    pub quarantined_peers: BTreeSet<PeerId>,
    /// The banned peers.
    pub banned_peers: BTreeSet<PeerId>,
    /// The minimum revocation epoch.
    pub minimum_revocation_epoch: Option<RevocationEpoch>,
    /// The trust bundle.
    pub trust_bundle: Option<TrustBundleExport>,
    /// The last error.
    pub last_error: Option<String>,
    /// The node state.
    pub node_state: NodeRuntimeState,
    /// The slot states.
    pub slot_states: Vec<SlotRuntimeState>,
    /// The runtime snapshot.
    pub runtime_snapshot: Option<NodeTelemetrySnapshot>,
}

fn durable_receipt_path(storage: &StorageConfig, receipt_id: &ContributionReceiptId) -> PathBuf {
    storage
        .receipts_dir()
        .join(format!("{}.json", receipt_id.as_str()))
}

fn persist_durable_receipt(path: &Path, receipt: &ContributionReceipt) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("receipt path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent)?;
    let temp_path = path.with_extension("json.tmp");
    fs::write(&temp_path, serde_json::to_vec_pretty(receipt)?)?;
    fs::rename(&temp_path, path)?;
    Ok(())
}

impl BootstrapAdminState {
    pub(crate) fn register_live_head_announcement(
        &mut self,
        announcement: HeadAnnouncement,
    ) -> Vec<PeerId> {
        self.live_head_announcements.retain(|existing| {
            existing.head.head_id != announcement.head.head_id
                || existing.provider_peer_id != announcement.provider_peer_id
        });
        self.live_head_announcements.push(announcement.clone());
        self.provider_peer_ids_for_head(&announcement.head.head_id)
    }

    pub(crate) fn visible_head_descriptors(&self) -> Vec<HeadDescriptor> {
        let mut heads = BTreeMap::<HeadId, HeadDescriptor>::new();
        for head in &self.head_descriptors {
            heads.insert(head.head_id.clone(), head.clone());
        }
        for announcement in &self.live_head_announcements {
            let candidate = announcement.head.clone();
            let replace = heads
                .get(&candidate.head_id)
                .is_none_or(|existing| candidate.created_at >= existing.created_at);
            if replace {
                heads.insert(candidate.head_id.clone(), candidate);
            }
        }
        if let Some(snapshot) = self.runtime_snapshot.as_ref() {
            for announcement in &snapshot.control_plane.head_announcements {
                let candidate = announcement.head.clone();
                let replace = heads
                    .get(&candidate.head_id)
                    .is_none_or(|existing| candidate.created_at >= existing.created_at);
                if replace {
                    heads.insert(candidate.head_id.clone(), candidate);
                }
            }
        }
        let mut visible = heads.into_values().collect::<Vec<_>>();
        visible.sort_by_key(|head| head.created_at);
        visible
    }

    #[allow(dead_code)]
    pub(crate) fn provider_peer_ids_for_head(&self, head_id: &HeadId) -> Vec<PeerId> {
        let mut peer_ids = self
            .live_head_announcements
            .iter()
            .filter(|announcement| &announcement.head.head_id == head_id)
            .filter_map(|announcement| announcement.provider_peer_id.clone())
            .collect::<Vec<_>>();
        if let Some(snapshot) = self.runtime_snapshot.as_ref() {
            peer_ids.extend(
                snapshot
                    .control_plane
                    .head_announcements
                    .iter()
                    .filter(|announcement| &announcement.head.head_id == head_id)
                    .filter_map(|announcement| announcement.provider_peer_id.clone()),
            );
        }
        peer_ids.sort();
        peer_ids.dedup();
        if let Some(local_peer_id) = self
            .runtime_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.local_peer_id.as_ref())
            && let Some(index) = peer_ids.iter().position(|peer_id| peer_id == local_peer_id)
        {
            let local_peer_id = peer_ids.remove(index);
            peer_ids.insert(0, local_peer_id);
        }
        peer_ids
    }

    fn effective_quarantined_peers(&self) -> BTreeSet<PeerId> {
        let mut quarantined = self.quarantined_peers.clone();
        if let Some(snapshot) = self.runtime_snapshot.as_ref() {
            quarantined.extend(
                snapshot
                    .trust_scores
                    .iter()
                    .filter(|score| score.quarantined)
                    .map(|score| score.peer_id.clone()),
            );
        }
        quarantined
    }

    /// Performs the ingest contribution receipts operation.
    pub fn ingest_contribution_receipts(
        &mut self,
        receipts: impl IntoIterator<Item = ContributionReceipt>,
    ) -> Vec<ContributionReceiptId> {
        self.ingest_contribution_receipts_inner(receipts, false)
            .expect("non-durable receipt ingest should not fail")
    }

    /// Ingests browser-submitted receipts and persists them before returning an acknowledgement.
    pub fn ingest_browser_contribution_receipts(
        &mut self,
        receipts: impl IntoIterator<Item = ContributionReceipt>,
    ) -> anyhow::Result<Vec<ContributionReceiptId>> {
        self.ingest_contribution_receipts_inner(receipts, true)
    }

    fn ingest_contribution_receipts_inner(
        &mut self,
        receipts: impl IntoIterator<Item = ContributionReceipt>,
        persist: bool,
    ) -> anyhow::Result<Vec<ContributionReceiptId>> {
        let mut known_receipt_ids = self
            .contribution_receipts
            .iter()
            .map(|receipt| receipt.receipt_id.clone())
            .collect::<BTreeSet<_>>();
        let mut accepted_receipt_ids = Vec::new();
        let mut accepted_receipts = Vec::new();
        let storage = self.history_root.as_ref().map(StorageConfig::new);

        for receipt in receipts {
            if !known_receipt_ids.insert(receipt.receipt_id.clone()) {
                continue;
            }
            if persist && let Some(storage) = storage.as_ref() {
                let receipt_path = durable_receipt_path(storage, &receipt.receipt_id);
                if receipt_path.try_exists()? {
                    continue;
                }
                persist_durable_receipt(&receipt_path, &receipt)?;
            }
            accepted_receipt_ids.push(receipt.receipt_id.clone());
            accepted_receipts.push(receipt);
        }

        self.contribution_receipts.extend(accepted_receipts);
        self.contribution_receipts
            .sort_by_key(|receipt| receipt.accepted_at);
        let max_receipts_in_memory =
            crate::history::OperatorHistoryMemoryBudget::default().max_receipts_in_memory;
        if self.contribution_receipts.len() > max_receipts_in_memory {
            let trimmed = self.contribution_receipts.len() - max_receipts_in_memory;
            self.contribution_receipts.drain(0..trimmed);
        }
        if persist && storage.is_some() {
            self.persisted_receipt_count = self
                .persisted_receipt_count
                .saturating_add(accepted_receipt_ids.len())
                .max(self.contribution_receipts.len());
        }
        Ok(accepted_receipt_ids)
    }

    /// Performs the peer diagnostics operation.
    fn peer_diagnostics(&self) -> Vec<BootstrapPeerDiagnostic> {
        let effective_quarantined = self.effective_quarantined_peers();
        let peer_ids = self
            .peer_store
            .observed_peer_ids()
            .into_iter()
            .chain(self.peer_admission_reports.keys().cloned())
            .chain(self.rejected_peers.keys().cloned())
            .chain(self.peer_reputation.keys().cloned())
            .chain(effective_quarantined.iter().cloned())
            .chain(self.banned_peers.iter().cloned())
            .collect::<BTreeSet<_>>();
        let reputation_engine = ReputationEngine::default();

        peer_ids
            .into_iter()
            .map(|peer_id| {
                let observation = self.peer_store.get(&peer_id);
                let admission_report = self.peer_admission_reports.get(&peer_id);
                let reputation_state = self.peer_reputation.get(&peer_id);
                BootstrapPeerDiagnostic {
                    peer_id: peer_id.clone(),
                    connected: observation.map(|entry| entry.connected).unwrap_or(false),
                    observed_at: observation.map(|entry| entry.observed_at),
                    trust_level: admission_report.map(|report| report.trust_level.clone()),
                    rejection_reason: self.rejected_peers.get(&peer_id).cloned(),
                    reputation_score: reputation_state.map(|state| state.score),
                    reputation_decision: reputation_state
                        .map(|state| reputation_engine.decision(state.score)),
                    quarantined: effective_quarantined.contains(&peer_id),
                    banned: self.banned_peers.contains(&peer_id),
                }
            })
            .collect()
    }

    /// Exports the receipts.
    pub fn export_receipts(&self, query: &ReceiptQuery) -> Vec<ContributionReceipt> {
        self.operator_store().receipts(query).unwrap_or_else(|_| {
            self.contribution_receipts
                .iter()
                .filter(|receipt| query.matches(receipt))
                .cloned()
                .collect()
        })
    }

    /// Exports one durable page of receipts without retaining the full filtered
    /// result set in memory.
    pub fn export_receipts_page(
        &self,
        query: &ReceiptQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<ContributionReceipt>> {
        self.operator_store().receipts_page(query, page)
    }

    /// Exports the heads.
    pub fn export_heads(&self, query: &HeadQuery) -> Vec<HeadDescriptor> {
        let mut heads = self.operator_store().heads(query).unwrap_or_else(|_| {
            self.head_descriptors
                .iter()
                .filter(|head| query.matches(head))
                .cloned()
                .collect()
        });
        let mut known_head_ids = heads
            .iter()
            .map(|head| head.head_id.clone())
            .collect::<BTreeSet<_>>();
        for head in self.visible_head_descriptors() {
            if query.matches(&head) && known_head_ids.insert(head.head_id.clone()) {
                heads.push(head);
            }
        }
        heads.sort_by_key(|head| head.created_at);
        heads
    }

    /// Exports one durable page of heads without retaining the full filtered
    /// result set in memory.
    pub fn export_heads_page(
        &self,
        query: &HeadQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<HeadDescriptor>> {
        self.operator_store().heads_page(query, page)
    }

    /// Exports one durable page of merge certificates.
    pub fn export_merges_page(&self, page: PageRequest) -> anyhow::Result<Page<MergeCertificate>> {
        self.operator_store().merges_page(page)
    }

    /// Exports the reducer load.
    pub fn export_reducer_load(&self, query: &ReducerLoadQuery) -> Vec<ReducerLoadAnnouncement> {
        self.reducer_load_announcements
            .iter()
            .filter(|announcement| query.matches(announcement))
            .cloned()
            .collect()
    }

    /// Exports the head evaluation reports for one head.
    pub fn export_head_eval_reports(&self, head_id: &HeadId) -> Vec<HeadEvalReport> {
        self.operator_store()
            .head_eval_reports(head_id)
            .unwrap_or_else(|_| {
                self.head_eval_reports
                    .iter()
                    .filter(|report| &report.head_id == head_id)
                    .cloned()
                    .collect()
            })
    }

    /// Exports one durable operator audit page suitable for search and replay.
    pub fn export_operator_audit_page(
        &self,
        query: &OperatorAuditQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorAuditRecord>> {
        self.operator_store().audit_page(query, page)
    }

    /// Exports one aggregate operator audit summary for search and retention tooling.
    pub fn export_operator_audit_summary(
        &self,
        query: &OperatorAuditQuery,
    ) -> anyhow::Result<OperatorAuditSummary> {
        self.operator_store().audit_summary(query)
    }

    /// Exports one top-facet summary for operator audit search tooling.
    pub fn export_operator_audit_facets(
        &self,
        query: &OperatorAuditQuery,
        limit: usize,
    ) -> anyhow::Result<OperatorAuditFacetSummary> {
        self.operator_store().audit_facets(query, limit)
    }

    /// Exports one retained operator snapshot at or before the requested timestamp.
    pub fn export_operator_replay_snapshot(
        &self,
        captured_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<Option<OperatorReplaySnapshot>> {
        self.operator_store().replay_snapshot(captured_at)
    }

    /// Exports one retained replay snapshot summary page for search and retention tooling.
    pub fn export_operator_replay_page(
        &self,
        query: &OperatorReplayQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorReplaySnapshotSummary>> {
        self.operator_store().replay_page(query, page)
    }

    /// Exports one typed lifecycle/schedule replay page derived from retained operator snapshots.
    pub fn export_operator_control_replay_page(
        &self,
        query: &OperatorControlReplayQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorControlReplayRecord>> {
        self.operator_store().control_replay_page(query, page)
    }

    /// Exports one aggregate lifecycle/schedule replay summary for operator tooling.
    pub fn export_operator_control_replay_summary(
        &self,
        query: &OperatorControlReplayQuery,
    ) -> anyhow::Result<OperatorControlReplaySummary> {
        self.operator_store().control_replay_summary(query)
    }

    /// Exports backend retention diagnostics for operator snapshots and audit rows.
    pub fn export_operator_retention_summary(&self) -> anyhow::Result<OperatorRetentionSummary> {
        self.operator_store().retention_summary()
    }

    /// Prunes retained operator history against the configured backend budgets.
    pub fn prune_operator_retention(&self) -> anyhow::Result<OperatorRetentionPruneResult> {
        self.operator_store().prune_retention()
    }

    /// Performs the diagnostics operation.
    pub fn diagnostics(
        &self,
        plan: &BootstrapPlan,
        captured_at: DateTime<Utc>,
        remaining_work_units: Option<u64>,
    ) -> BootstrapDiagnostics {
        let effective_quarantined = self.effective_quarantined_peers();
        let certified_merges = self.visible_certified_merge_count();
        BootstrapDiagnostics {
            network_id: plan.genesis.network_id.clone(),
            preset: plan.preset.clone(),
            services: plan.services.clone(),
            roles: plan.roles.clone(),
            local_peer_id: self
                .runtime_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.local_peer_id.clone()),
            swarm: self.peer_store.stats(remaining_work_units),
            pinned_heads: plan.archive.pinned_heads.clone(),
            pinned_artifacts: plan.archive.pinned_artifacts.clone(),
            accepted_receipts: self
                .persisted_receipt_count
                .max(self.contribution_receipts.len()) as u64,
            certified_merges,
            in_flight_transfers: self.in_flight_transfers.clone(),
            request_failures: self.request_failures.clone(),
            admitted_peers: self.admitted_peers.clone(),
            peer_diagnostics: self.peer_diagnostics(),
            rejected_peers: self.rejected_peers.clone(),
            quarantined_peers: effective_quarantined,
            banned_peers: self.banned_peers.clone(),
            minimum_revocation_epoch: self.minimum_revocation_epoch,
            last_error: operator_visible_last_error(self.last_error.as_deref()),
            node_state: self.node_state.clone(),
            slot_states: self.slot_states.clone(),
            robustness_panel: self.robustness_panel(),
            robustness_rollup: self.robustness_rollup(plan, captured_at),
            captured_at,
        }
    }

    fn visible_certified_merge_count(&self) -> u64 {
        let retained_merges = self
            .persisted_merge_count
            .max(self.merge_certificates.len());
        let runtime_merges = self
            .runtime_snapshot
            .as_ref()
            .map(|snapshot| {
                snapshot.control_plane.merge_announcements.len().max(
                    snapshot
                        .control_plane
                        .diffusion_promotion_certificate_announcements
                        .len(),
                )
            })
            .unwrap_or_default();
        retained_merges.max(runtime_merges) as u64
    }

    /// Performs the diagnostics bundle operation.
    pub fn diagnostics_bundle(
        &self,
        plan: &BootstrapPlan,
        captured_at: DateTime<Utc>,
        remaining_work_units: Option<u64>,
    ) -> BootstrapDiagnosticsBundle {
        BootstrapDiagnosticsBundle {
            plan: plan.clone(),
            diagnostics: self.diagnostics(plan, captured_at, remaining_work_units),
            runtime_snapshot: self.runtime_snapshot.clone(),
            heads: self.visible_head_descriptors(),
            contribution_receipts: self.contribution_receipts.clone(),
            merge_certificates: self.merge_certificates.clone(),
            reducer_load_announcements: self.reducer_load_announcements.clone(),
            trust_bundle: self.trust_bundle.clone(),
            captured_at,
        }
    }

    fn robustness_panel(&self) -> Option<RobustnessPanelView> {
        let snapshot = self.runtime_snapshot.as_ref()?;
        let policy = snapshot.robustness_policy.clone()?;
        let cohort = snapshot.latest_cohort_robustness.as_ref()?;

        Some(RobustnessPanelView::from_reports(
            policy,
            cohort,
            &snapshot.trust_scores,
            &snapshot.canary_reports,
        ))
    }

    #[cfg(feature = "metrics-indexer")]
    fn robustness_rollup(
        &self,
        plan: &BootstrapPlan,
        captured_at: DateTime<Utc>,
    ) -> Option<BootstrapRobustnessRollup> {
        let snapshot = self.runtime_snapshot.as_ref()?;
        let cohort = snapshot.latest_cohort_robustness.as_ref()?;
        Some(derive_robustness_rollup(
            plan.genesis.network_id.clone(),
            cohort,
            &snapshot.trust_scores,
            &snapshot.canary_reports,
            captured_at,
        ))
    }

    #[cfg(not(feature = "metrics-indexer"))]
    fn robustness_rollup(
        &self,
        _plan: &BootstrapPlan,
        _captured_at: DateTime<Utc>,
    ) -> Option<BootstrapRobustnessRollup> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use burn_p2p::{
        ClientPlatform, ControlPlaneSnapshot, DiffusionPromotionCertificate,
        DiffusionPromotionCertificateAnnouncement, HeadAnnouncement, LagPolicy, LagState,
        NodeRuntimeState, RuntimeStatus,
    };
    use chrono::Utc;
    use semver::Version;

    fn test_runtime_snapshot(
        now: chrono::DateTime<Utc>,
        local_peer_id: PeerId,
        control_plane: ControlPlaneSnapshot,
    ) -> NodeTelemetrySnapshot {
        NodeTelemetrySnapshot {
            status: RuntimeStatus::Running,
            node_state: NodeRuntimeState::IdleReady,
            slot_states: Vec::new(),
            lag_state: LagState::Current,
            head_lag_steps: 0,
            lag_policy: LagPolicy::default(),
            network_id: Some(NetworkId::new("demo")),
            local_peer_id: Some(local_peer_id),
            configured_roles: burn_p2p::PeerRoleSet::default_trainer(),
            connected_peers: 1,
            connected_peer_ids: BTreeSet::new(),
            observed_peer_ids: BTreeSet::new(),
            known_peer_addresses: BTreeSet::new(),
            runtime_boundary: None,
            listen_addresses: Vec::new(),
            control_plane,
            recent_events: Vec::new(),
            last_snapshot_peer_id: None,
            last_snapshot: None,
            admitted_peers: BTreeMap::new(),
            rejected_peers: BTreeMap::new(),
            peer_reputation: BTreeMap::new(),
            minimum_revocation_epoch: None,
            trust_bundle: None,
            in_flight_transfers: BTreeMap::new(),
            request_failures: Vec::new(),
            robustness_policy: None,
            latest_cohort_robustness: None,
            trust_scores: Vec::new(),
            canary_reports: Vec::new(),
            applied_control_cert_ids: BTreeSet::new(),
            effective_limit_profile: None,
            last_error: None,
            started_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn export_heads_includes_live_runtime_head_announcements() {
        let now = Utc::now();
        let runtime_head = HeadDescriptor {
            head_id: HeadId::new("runtime-head"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: burn_p2p::ArtifactId::new("artifact"),
            parent_head_id: None,
            global_step: 0,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let state = BootstrapAdminState {
            runtime_snapshot: Some(test_runtime_snapshot(
                now,
                PeerId::new("bootstrap"),
                ControlPlaneSnapshot {
                    head_announcements: vec![HeadAnnouncement {
                        overlay: burn_p2p::OverlayTopic::experiment(
                            NetworkId::new("demo"),
                            StudyId::new("study"),
                            ExperimentId::new("exp"),
                            burn_p2p::OverlayChannel::Heads,
                        )
                        .expect("heads overlay"),
                        provider_peer_id: Some(PeerId::new("mirror")),
                        head: runtime_head.clone(),
                        announced_at: now,
                    }],
                    ..ControlPlaneSnapshot::default()
                },
            )),
            ..BootstrapAdminState::default()
        };

        let heads = state.export_heads(&HeadQuery::default());
        assert_eq!(heads, vec![runtime_head]);
    }

    #[test]
    fn export_heads_includes_directly_registered_live_head_announcements() {
        let now = Utc::now();
        let runtime_head = HeadDescriptor {
            head_id: HeadId::new("runtime-head"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: burn_p2p::ArtifactId::new("artifact"),
            parent_head_id: None,
            global_step: 0,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let mut state = BootstrapAdminState::default();
        let provider_peer_ids = state.register_live_head_announcement(HeadAnnouncement {
            overlay: burn_p2p::OverlayTopic::experiment(
                NetworkId::new("demo"),
                StudyId::new("study"),
                ExperimentId::new("exp"),
                burn_p2p::OverlayChannel::Heads,
            )
            .expect("heads overlay"),
            provider_peer_id: Some(PeerId::new("mirror")),
            head: runtime_head.clone(),
            announced_at: now,
        });

        assert_eq!(provider_peer_ids, vec![PeerId::new("mirror")]);
        let heads = state.export_heads(&HeadQuery::default());
        assert_eq!(heads, vec![runtime_head]);
    }

    #[test]
    fn provider_peer_ids_for_head_prefers_local_mirrored_provider() {
        let now = Utc::now();
        let runtime_head = HeadDescriptor {
            head_id: HeadId::new("runtime-head"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
            artifact_id: burn_p2p::ArtifactId::new("artifact"),
            parent_head_id: None,
            global_step: 0,
            created_at: now,
            metrics: BTreeMap::new(),
        };
        let head_overlay = burn_p2p::OverlayTopic::experiment(
            NetworkId::new("demo"),
            StudyId::new("study"),
            ExperimentId::new("exp"),
            burn_p2p::OverlayChannel::Heads,
        )
        .expect("heads overlay");
        let mut state = BootstrapAdminState {
            runtime_snapshot: Some(test_runtime_snapshot(
                now,
                PeerId::new("edge"),
                ControlPlaneSnapshot {
                    head_announcements: vec![HeadAnnouncement {
                        overlay: head_overlay.clone(),
                        provider_peer_id: Some(PeerId::new("mirror")),
                        head: runtime_head.clone(),
                        announced_at: now,
                    }],
                    ..ControlPlaneSnapshot::default()
                },
            )),
            ..BootstrapAdminState::default()
        };
        state.register_live_head_announcement(HeadAnnouncement {
            overlay: head_overlay,
            provider_peer_id: Some(PeerId::new("edge")),
            head: runtime_head.clone(),
            announced_at: now,
        });

        assert_eq!(
            state.provider_peer_ids_for_head(&runtime_head.head_id),
            vec![PeerId::new("edge"), PeerId::new("mirror")]
        );
    }

    #[test]
    fn diagnostics_counts_runtime_diffusion_promotion_certificates() {
        let now = Utc::now();
        let overlay = burn_p2p::OverlayTopic::experiment(
            NetworkId::new("demo"),
            StudyId::new("study"),
            ExperimentId::new("exp"),
            burn_p2p::OverlayChannel::Heads,
        )
        .expect("heads overlay");
        let state = BootstrapAdminState {
            runtime_snapshot: Some(test_runtime_snapshot(
                now,
                PeerId::new("edge"),
                ControlPlaneSnapshot {
                    diffusion_promotion_certificate_announcements: vec![
                        DiffusionPromotionCertificateAnnouncement {
                            overlay,
                            certificate: DiffusionPromotionCertificate {
                                study_id: StudyId::new("study"),
                                experiment_id: ExperimentId::new("exp"),
                                revision_id: RevisionId::new("rev"),
                                window_id: WindowId(7),
                                base_head_id: HeadId::new("base"),
                                merged_head_id: HeadId::new("head"),
                                merged_artifact_id: burn_p2p::ArtifactId::new("artifact"),
                                promotion_mode: burn_p2p::HeadPromotionMode::DiffusionSteadyState,
                                attesting_trainers: vec![PeerId::new("trainer")],
                                attestation_ids: vec![burn_p2p::ContentId::new("attestation")],
                                attester_count: 1,
                                cumulative_sample_weight: 1.0,
                                settled_at: now,
                                promoter_peer_id: PeerId::new("trainer"),
                            },
                            announced_at: now,
                        },
                    ],
                    ..ControlPlaneSnapshot::default()
                },
            )),
            ..BootstrapAdminState::default()
        };
        let diagnostics = state.diagnostics(
            &crate::deploy::BootstrapSpec {
                preset: crate::BootstrapPreset::BootstrapOnly,
                genesis: burn_p2p_core::GenesisSpec {
                    network_id: NetworkId::new("demo"),
                    protocol_version: Version::new(0, 1, 0),
                    display_name: "demo".into(),
                    created_at: Utc::now(),
                    metadata: BTreeMap::new(),
                },
                platform: ClientPlatform::Native,
                bootstrap_addresses: Vec::new(),
                listen_addresses: Vec::new(),
                authority: None,
                archive: crate::ArchivePlan::default(),
                admin_api: crate::AdminApiPlan::default(),
            }
            .plan()
            .expect("plan"),
            now,
            None,
        );

        assert_eq!(diagnostics.certified_merges, 1);
    }

    #[test]
    fn diagnostics_hide_benign_runtime_noise() {
        let state = BootstrapAdminState {
            last_error: Some("pubsub error: NoPeersSubscribedToTopic".into()),
            ..BootstrapAdminState::default()
        };
        let diagnostics = state.diagnostics(
            &crate::deploy::BootstrapSpec {
                preset: crate::BootstrapPreset::BootstrapOnly,
                genesis: burn_p2p_core::GenesisSpec {
                    network_id: NetworkId::new("demo"),
                    protocol_version: Version::new(0, 1, 0),
                    display_name: "demo".into(),
                    created_at: Utc::now(),
                    metadata: BTreeMap::new(),
                },
                platform: ClientPlatform::Native,
                bootstrap_addresses: Vec::new(),
                listen_addresses: Vec::new(),
                authority: None,
                archive: crate::ArchivePlan::default(),
                admin_api: crate::AdminApiPlan::default(),
            }
            .plan()
            .expect("plan"),
            Utc::now(),
            None,
        );
        assert_eq!(diagnostics.last_error, None);
    }
}

impl BootstrapAdminState {
    #[doc(hidden)]
    pub fn configure_operator_state_backend(&mut self, backend: OperatorStateBackendConfig) {
        self.operator_state_backend = Some(backend);
    }

    pub(crate) fn operator_store_preview(&self) -> FileOperatorStorePreview {
        FileOperatorStorePreview {
            receipts: self.contribution_receipts.clone(),
            heads: self.head_descriptors.clone(),
            merges: self.merge_certificates.clone(),
            lifecycle_announcements: self
                .runtime_snapshot
                .as_ref()
                .map(|snapshot| snapshot.control_plane.lifecycle_announcements.clone())
                .unwrap_or_default(),
            schedule_announcements: self
                .runtime_snapshot
                .as_ref()
                .map(|snapshot| snapshot.control_plane.schedule_announcements.clone())
                .unwrap_or_default(),
            peer_window_metrics: self.peer_window_metrics.clone(),
            reducer_cohort_metrics: self.reducer_cohort_metrics.clone(),
            head_eval_reports: self.head_eval_reports.clone(),
            eval_protocol_manifests: self.eval_protocol_manifests.clone(),
        }
    }

    pub(crate) fn persist_operator_state_snapshot(&self) -> anyhow::Result<()> {
        persist_operator_state_snapshot(
            self.operator_state_backend.as_ref(),
            self.metrics_retention,
            &self.operator_store_preview(),
        )
    }

    pub(crate) fn operator_store(&self) -> FileOperatorStore {
        FileOperatorStore::new(
            FileOperatorStoreConfig {
                history_root: self.history_root.clone(),
                metrics_store_root: self.metrics_store_root.clone(),
                metrics_retention: self.metrics_retention,
                publication_store_root: self.publication_store_root.clone(),
                publication_targets: self.publication_targets.clone(),
                artifact_store_root: self.artifact_store_root.clone(),
                operator_state_backend: self.operator_state_backend.clone(),
            },
            self.operator_store_preview(),
        )
    }

    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    pub(crate) fn resolved_peer_window_metrics(&self) -> Vec<PeerWindowMetrics> {
        self.operator_store()
            .all_peer_window_metrics()
            .unwrap_or_else(|_| self.peer_window_metrics.clone())
    }

    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    pub(crate) fn resolved_reducer_cohort_metrics(&self) -> Vec<ReducerCohortMetrics> {
        self.operator_store()
            .all_reducer_cohort_metrics()
            .unwrap_or_else(|_| self.reducer_cohort_metrics.clone())
    }

    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    pub(crate) fn resolved_head_eval_reports(&self) -> Vec<HeadEvalReport> {
        self.operator_store()
            .all_head_eval_reports()
            .unwrap_or_else(|_| self.head_eval_reports.clone())
    }

    #[cfg(feature = "artifact-publish")]
    pub(crate) fn stored_heads(&self) -> anyhow::Result<Vec<HeadDescriptor>> {
        self.operator_store().heads(&HeadQuery::default())
    }

    #[cfg(feature = "artifact-publish")]
    pub(crate) fn stored_eval_protocol_manifests(
        &self,
    ) -> anyhow::Result<Vec<StoredEvalProtocolManifestRecord>> {
        self.operator_store().eval_protocol_manifests()
    }

    #[cfg(feature = "artifact-publish")]
    pub(crate) fn stored_head_eval_reports(&self) -> anyhow::Result<Vec<HeadEvalReport>> {
        Ok(self.resolved_head_eval_reports())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a bootstrap peer diagnostic.
pub struct BootstrapPeerDiagnostic {
    /// The peer ID.
    pub peer_id: PeerId,
    /// The connected.
    pub connected: bool,
    /// The observed at.
    pub observed_at: Option<DateTime<Utc>>,
    /// The trust level.
    pub trust_level: Option<PeerTrustLevel>,
    /// The rejection reason.
    pub rejection_reason: Option<String>,
    /// The reputation score.
    pub reputation_score: Option<f64>,
    /// The reputation decision.
    pub reputation_decision: Option<ReputationDecision>,
    /// The quarantined.
    pub quarantined: bool,
    /// The banned.
    pub banned: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a bootstrap diagnostics.
pub struct BootstrapDiagnostics {
    /// The network ID.
    pub network_id: NetworkId,
    /// The preset.
    pub preset: crate::BootstrapPreset,
    /// The services.
    pub services: BTreeSet<crate::BootstrapService>,
    /// The roles.
    pub roles: burn_p2p_core::PeerRoleSet,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The local runtime peer ID serving this bootstrap edge, when the P2P runtime has reported it.
    pub local_peer_id: Option<PeerId>,
    /// The swarm.
    pub swarm: SwarmStats,
    /// The pinned heads.
    pub pinned_heads: BTreeSet<HeadId>,
    /// The pinned artifacts.
    pub pinned_artifacts: BTreeSet<burn_p2p_core::ArtifactId>,
    /// The accepted receipts.
    pub accepted_receipts: u64,
    /// The certified merges.
    pub certified_merges: u64,
    /// The in flight transfers.
    pub in_flight_transfers: Vec<ArtifactTransferState>,
    #[serde(default)]
    /// Request failure counters grouped by operation and reason.
    pub request_failures: Vec<RequestFailureCounter>,
    /// The admitted peers.
    pub admitted_peers: BTreeSet<PeerId>,
    /// The peer diagnostics.
    pub peer_diagnostics: Vec<BootstrapPeerDiagnostic>,
    /// The rejected peers.
    pub rejected_peers: BTreeMap<PeerId, String>,
    /// The quarantined peers.
    pub quarantined_peers: BTreeSet<PeerId>,
    /// The banned peers.
    pub banned_peers: BTreeSet<PeerId>,
    /// The minimum revocation epoch.
    pub minimum_revocation_epoch: Option<burn_p2p::RevocationEpoch>,
    /// The last error.
    pub last_error: Option<String>,
    /// The node state.
    pub node_state: NodeRuntimeState,
    /// The slot states.
    pub slot_states: Vec<SlotRuntimeState>,
    #[serde(default)]
    /// The active robustness panel, when runtime validation has emitted one.
    pub robustness_panel: Option<RobustnessPanelView>,
    #[serde(default)]
    /// The compact robustness rollup, when metrics-indexer support is enabled.
    pub robustness_rollup: Option<BootstrapRobustnessRollup>,
    /// The captured at.
    pub captured_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a bootstrap diagnostics bundle.
pub struct BootstrapDiagnosticsBundle {
    /// The plan.
    pub plan: BootstrapPlan,
    /// The diagnostics.
    pub diagnostics: BootstrapDiagnostics,
    /// The runtime snapshot.
    pub runtime_snapshot: Option<NodeTelemetrySnapshot>,
    /// The heads.
    pub heads: Vec<HeadDescriptor>,
    /// The contribution receipts.
    pub contribution_receipts: Vec<ContributionReceipt>,
    /// The merge certificates.
    pub merge_certificates: Vec<MergeCertificate>,
    /// The reducer load announcements.
    pub reducer_load_announcements: Vec<ReducerLoadAnnouncement>,
    /// The trust bundle.
    pub trust_bundle: Option<TrustBundleExport>,
    /// The captured at.
    pub captured_at: DateTime<Utc>,
}

pub(crate) fn page_from_filtered<T, I, F>(values: I, page: PageRequest, matches: F) -> Page<T>
where
    I: IntoIterator<Item = T>,
    F: Fn(&T) -> bool,
{
    let page = page.normalized();
    let mut items = Vec::new();
    let mut total = 0usize;
    for value in values {
        if !matches(&value) {
            continue;
        }
        if total >= page.offset && items.len() < page.limit {
            items.push(value);
        }
        total += 1;
    }
    Page::new(items, page, total)
}

/// Performs the render openmetrics operation.
pub fn render_openmetrics(diagnostics: &BootstrapDiagnostics) -> String {
    let mut lines = Vec::new();
    lines.push("# TYPE burn_p2p_connected_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_connected_peers {}",
        diagnostics.swarm.connected_peers
    ));
    lines.push("# TYPE burn_p2p_observed_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_observed_peers {}",
        diagnostics.swarm.observed_peers.len()
    ));
    lines.push("# TYPE burn_p2p_estimated_network_size gauge".to_owned());
    lines.push(format!(
        "burn_p2p_estimated_network_size {}",
        diagnostics.swarm.network_estimate.estimated_network_size
    ));
    lines.push("# TYPE burn_p2p_accepted_receipts counter".to_owned());
    lines.push(format!(
        "burn_p2p_accepted_receipts {}",
        diagnostics.accepted_receipts
    ));
    lines.push("# TYPE burn_p2p_certified_merges counter".to_owned());
    lines.push(format!(
        "burn_p2p_certified_merges {}",
        diagnostics.certified_merges
    ));
    lines.push("# TYPE burn_p2p_in_flight_transfers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_in_flight_transfers {}",
        diagnostics.in_flight_transfers.len()
    ));
    lines.push("# TYPE burn_p2p_request_failures counter".to_owned());
    for counter in &diagnostics.request_failures {
        lines.push(format!(
            "burn_p2p_request_failures{{operation=\"{:?}\",reason=\"{:?}\"}} {}",
            counter.kind.operation, counter.kind.reason, counter.count
        ));
    }
    lines.push("# TYPE burn_p2p_admitted_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_admitted_peers {}",
        diagnostics.admitted_peers.len()
    ));
    lines.push("# TYPE burn_p2p_rejected_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_rejected_peers {}",
        diagnostics.rejected_peers.len()
    ));
    lines.push("# TYPE burn_p2p_quarantined_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_quarantined_peers {}",
        diagnostics.quarantined_peers.len()
    ));
    lines.push("# TYPE burn_p2p_banned_peers gauge".to_owned());
    lines.push(format!(
        "burn_p2p_banned_peers {}",
        diagnostics.banned_peers.len()
    ));
    if diagnostics.robustness_panel.is_some() || diagnostics.robustness_rollup.is_some() {
        let panel = diagnostics.robustness_panel.as_ref();
        #[cfg(feature = "metrics-indexer")]
        let rollup = diagnostics.robustness_rollup.as_ref();
        let rejected_updates = {
            #[cfg(feature = "metrics-indexer")]
            {
                rollup
                    .map(|rollup| u64::from(rollup.rejected_updates))
                    .or_else(|| {
                        panel.map(|panel| {
                            panel
                                .rejection_reasons
                                .iter()
                                .map(|reason| u64::from(reason.count))
                                .sum::<u64>()
                        })
                    })
                    .unwrap_or(0)
            }
            #[cfg(not(feature = "metrics-indexer"))]
            {
                panel
                    .map(|panel| {
                        panel
                            .rejection_reasons
                            .iter()
                            .map(|reason| u64::from(reason.count))
                            .sum::<u64>()
                    })
                    .unwrap_or(0)
            }
        };
        let mean_trust_score = {
            #[cfg(feature = "metrics-indexer")]
            {
                rollup
                    .map(|rollup| rollup.mean_trust_score)
                    .or_else(|| {
                        panel.map(|panel| {
                            if panel.trust_scores.is_empty() {
                                0.0
                            } else {
                                panel
                                    .trust_scores
                                    .iter()
                                    .map(|score| score.score)
                                    .sum::<f64>()
                                    / panel.trust_scores.len() as f64
                            }
                        })
                    })
                    .unwrap_or(0.0)
            }
            #[cfg(not(feature = "metrics-indexer"))]
            {
                panel
                    .map(|panel| {
                        if panel.trust_scores.is_empty() {
                            0.0
                        } else {
                            panel
                                .trust_scores
                                .iter()
                                .map(|score| score.score)
                                .sum::<f64>()
                                / panel.trust_scores.len() as f64
                        }
                    })
                    .unwrap_or(0.0)
            }
        };
        let canary_regressions = {
            #[cfg(feature = "metrics-indexer")]
            {
                rollup
                    .map(|rollup| u64::from(rollup.canary_regression_count))
                    .or_else(|| panel.map(|panel| panel.canary_regressions.len() as u64))
                    .unwrap_or(0)
            }
            #[cfg(not(feature = "metrics-indexer"))]
            {
                panel
                    .map(|panel| panel.canary_regressions.len() as u64)
                    .unwrap_or(0)
            }
        };
        let ban_recommendations = {
            #[cfg(feature = "metrics-indexer")]
            {
                rollup
                    .map(|rollup| u64::from(rollup.ban_recommended_peer_count))
                    .or_else(|| {
                        panel.map(|panel| {
                            panel
                                .quarantined_peers
                                .iter()
                                .filter(|peer| peer.ban_recommended)
                                .count() as u64
                        })
                    })
                    .unwrap_or(0)
            }
            #[cfg(not(feature = "metrics-indexer"))]
            {
                panel
                    .map(|panel| {
                        panel
                            .quarantined_peers
                            .iter()
                            .filter(|peer| peer.ban_recommended)
                            .count() as u64
                    })
                    .unwrap_or(0)
            }
        };
        let alert_count = {
            #[cfg(feature = "metrics-indexer")]
            {
                rollup
                    .map(|rollup| rollup.alert_count)
                    .or_else(|| panel.map(|panel| panel.alerts.len() as u32))
                    .unwrap_or(0)
            }
            #[cfg(not(feature = "metrics-indexer"))]
            {
                panel.map(|panel| panel.alerts.len() as u32).unwrap_or(0)
            }
        };
        lines.push("# TYPE burn_p2p_robustness_rejected_updates gauge".to_owned());
        lines.push(format!(
            "burn_p2p_robustness_rejected_updates {rejected_updates}"
        ));
        lines.push("# TYPE burn_p2p_robustness_mean_trust_score gauge".to_owned());
        lines.push(format!(
            "burn_p2p_robustness_mean_trust_score {mean_trust_score:.6}"
        ));
        lines.push("# TYPE burn_p2p_robustness_canary_regressions gauge".to_owned());
        lines.push(format!(
            "burn_p2p_robustness_canary_regressions {}",
            canary_regressions
        ));
        lines.push("# TYPE burn_p2p_robustness_ban_recommendations gauge".to_owned());
        lines.push(format!(
            "burn_p2p_robustness_ban_recommendations {}",
            ban_recommendations
        ));
        lines.push("# TYPE burn_p2p_robustness_alerts gauge".to_owned());
        lines.push(format!("burn_p2p_robustness_alerts {alert_count}"));
    }

    if let Some(eta_lower_seconds) = diagnostics.swarm.network_estimate.eta_lower_seconds {
        lines.push("# TYPE burn_p2p_eta_lower_seconds gauge".to_owned());
        lines.push(format!("burn_p2p_eta_lower_seconds {eta_lower_seconds}"));
    }
    if let Some(eta_upper_seconds) = diagnostics.swarm.network_estimate.eta_upper_seconds {
        lines.push("# TYPE burn_p2p_eta_upper_seconds gauge".to_owned());
        lines.push(format!("burn_p2p_eta_upper_seconds {eta_upper_seconds}"));
    }
    if let Some(estimated_total_flops) = diagnostics.swarm.network_estimate.estimated_total_flops {
        lines.push("# TYPE burn_p2p_estimated_total_flops gauge".to_owned());
        lines.push(format!(
            "burn_p2p_estimated_total_flops {}",
            estimated_total_flops
        ));
    }
    if let Some(minimum_revocation_epoch) = diagnostics.minimum_revocation_epoch {
        lines.push("# TYPE burn_p2p_minimum_revocation_epoch gauge".to_owned());
        lines.push(format!(
            "burn_p2p_minimum_revocation_epoch {}",
            minimum_revocation_epoch.0
        ));
    }
    if let Some(estimated_total_vram_bytes) = diagnostics
        .swarm
        .network_estimate
        .estimated_total_vram_bytes
    {
        lines.push("# TYPE burn_p2p_estimated_total_vram_bytes gauge".to_owned());
        lines.push(format!(
            "burn_p2p_estimated_total_vram_bytes {}",
            estimated_total_vram_bytes
        ));
    }

    lines.join("\n") + "\n"
}
