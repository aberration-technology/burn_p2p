use std::{collections::BTreeMap, path::PathBuf};

use crate::state::{
    HeadQuery, OperatorAuditFacetSummary, OperatorAuditKind, OperatorAuditQuery,
    OperatorAuditRecord, OperatorAuditSummary, OperatorControlReplayKind,
    OperatorControlReplayQuery, OperatorControlReplayRecord, OperatorControlReplaySummary,
    OperatorFacetBucket, OperatorReplayQuery, OperatorReplaySnapshot,
    OperatorReplaySnapshotSummary, OperatorRetentionPruneResult, OperatorRetentionSummary,
    ReceiptQuery,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p::FsArtifactStore;
use burn_p2p::{
    ContributionReceipt, ExperimentLifecycleAnnouncement, FleetScheduleAnnouncement,
    HeadDescriptor, MetricsRetentionBudget, StorageConfig,
};
use burn_p2p_core::{
    BrowserLeaderboardSnapshot, ExperimentId, HeadEvalReport, HeadId, MergeCertificate, NetworkId,
    Page, PageRequest, PeerId, PeerWindowMetrics, PrincipalId, PublicationTarget,
    ReducerCohortMetrics, RevisionId, StudyId, WindowId,
};
#[cfg(feature = "artifact-publish")]
use burn_p2p_publish::PublicationStore;
use chrono::{DateTime, Utc};
use postgres::{NoTls, types::ToSql};
use redis::Commands;
use serde::{Deserialize, Serialize};

use crate::{StoredEvalProtocolManifestRecord, history, state::page_from_filtered};

pub(crate) trait OperatorStore {
    fn receipts(&self, query: &ReceiptQuery) -> anyhow::Result<Vec<ContributionReceipt>>;
    fn receipts_page(
        &self,
        query: &ReceiptQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<ContributionReceipt>>;
    fn heads(&self, query: &HeadQuery) -> anyhow::Result<Vec<HeadDescriptor>>;
    fn heads_page(
        &self,
        query: &HeadQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<HeadDescriptor>>;
    fn merges(&self) -> anyhow::Result<Vec<MergeCertificate>>;
    fn merges_page(&self, page: PageRequest) -> anyhow::Result<Page<MergeCertificate>>;
    fn audit_page(
        &self,
        query: &OperatorAuditQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorAuditRecord>>;
    fn audit_summary(&self, query: &OperatorAuditQuery) -> anyhow::Result<OperatorAuditSummary>;
    fn audit_facets(
        &self,
        query: &OperatorAuditQuery,
        limit: usize,
    ) -> anyhow::Result<OperatorAuditFacetSummary>;
    fn replay_snapshot(
        &self,
        captured_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<Option<OperatorReplaySnapshot>>;
    fn replay_page(
        &self,
        query: &OperatorReplayQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorReplaySnapshotSummary>>;
    fn control_replay_page(
        &self,
        query: &OperatorControlReplayQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorControlReplayRecord>>;
    fn control_replay_summary(
        &self,
        query: &OperatorControlReplayQuery,
    ) -> anyhow::Result<OperatorControlReplaySummary>;
    fn retention_summary(&self) -> anyhow::Result<OperatorRetentionSummary>;
    fn prune_retention(&self) -> anyhow::Result<OperatorRetentionPruneResult>;
    fn head_eval_reports(&self, head_id: &HeadId) -> anyhow::Result<Vec<HeadEvalReport>>;
    #[cfg(feature = "artifact-publish")]
    fn eval_protocol_manifests(&self) -> anyhow::Result<Vec<StoredEvalProtocolManifestRecord>>;
    fn leaderboard_snapshot(
        &self,
        network_id: &NetworkId,
        peer_principals: &BTreeMap<PeerId, PrincipalId>,
        captured_at: DateTime<Utc>,
    ) -> anyhow::Result<BrowserLeaderboardSnapshot>;

    #[cfg(feature = "artifact-publish")]
    fn artifact_store(&self) -> anyhow::Result<Option<FsArtifactStore>>;

    #[cfg(feature = "artifact-publish")]
    fn publication_store(&self) -> anyhow::Result<Option<PublicationStore>>;
}

#[derive(Clone, Debug)]
pub(crate) struct FileOperatorStoreConfig {
    pub(crate) history_root: Option<PathBuf>,
    pub(crate) metrics_store_root: Option<PathBuf>,
    pub(crate) metrics_retention: MetricsRetentionBudget,
    pub(crate) publication_store_root: Option<PathBuf>,
    pub(crate) publication_targets: Vec<PublicationTarget>,
    pub(crate) artifact_store_root: Option<PathBuf>,
    pub(crate) operator_state_backend: Option<OperatorStateBackendConfig>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct FileOperatorStorePreview {
    pub(crate) receipts: Vec<ContributionReceipt>,
    pub(crate) heads: Vec<HeadDescriptor>,
    pub(crate) merges: Vec<MergeCertificate>,
    #[serde(default)]
    pub(crate) lifecycle_announcements: Vec<ExperimentLifecycleAnnouncement>,
    #[serde(default)]
    pub(crate) schedule_announcements: Vec<FleetScheduleAnnouncement>,
    pub(crate) peer_window_metrics: Vec<PeerWindowMetrics>,
    pub(crate) reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    pub(crate) head_eval_reports: Vec<HeadEvalReport>,
    pub(crate) eval_protocol_manifests: Vec<StoredEvalProtocolManifestRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperatorStateBackendConfig {
    Redis {
        url: String,
        snapshot_key: String,
    },
    Postgres {
        url: String,
        table_name: String,
        snapshot_key: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OperatorStateSnapshot {
    captured_at: DateTime<Utc>,
    preview: FileOperatorStorePreview,
}

#[derive(Clone, Debug)]
struct PostgresOperatorStateRow {
    captured_at: DateTime<Utc>,
    preview: FileOperatorStorePreview,
}

fn postgres_operator_audit_table_name(snapshot_table_name: &str) -> String {
    format!("{snapshot_table_name}_audit")
}

fn operator_snapshot_retention_limit(retention: MetricsRetentionBudget) -> usize {
    (retention.max_metric_revisions_per_experiment.max(1) * 32).max(128)
}

fn operator_audit_retention_limit(retention: MetricsRetentionBudget) -> usize {
    let per_revision = retention.max_peer_window_entries_per_revision
        + retention.max_reducer_cohort_entries_per_revision
        + retention.max_head_eval_reports_per_revision
        + 256;
    (retention.max_metric_revisions_per_experiment.max(1) * per_revision).max(2_048)
}

fn replay_snapshot_from_preview(
    captured_at: DateTime<Utc>,
    preview: FileOperatorStorePreview,
) -> OperatorReplaySnapshot {
    OperatorReplaySnapshot {
        captured_at,
        receipts: preview.receipts,
        heads: preview.heads,
        merges: preview.merges,
        lifecycle_announcements: preview.lifecycle_announcements,
        schedule_announcements: preview.schedule_announcements,
        peer_window_metrics: preview.peer_window_metrics,
        reducer_cohort_metrics: preview.reducer_cohort_metrics,
        head_eval_reports: preview.head_eval_reports,
        eval_protocol_manifests: preview.eval_protocol_manifests,
    }
}

fn preview_scope_ids(
    preview: &FileOperatorStorePreview,
) -> (
    Vec<StudyId>,
    Vec<ExperimentId>,
    Vec<RevisionId>,
    Vec<HeadId>,
) {
    let mut study_ids = BTreeMap::<StudyId, ()>::new();
    let mut experiment_ids = BTreeMap::<ExperimentId, ()>::new();
    let mut revision_ids = BTreeMap::<RevisionId, ()>::new();
    let mut head_ids = BTreeMap::<HeadId, ()>::new();

    for receipt in &preview.receipts {
        study_ids.insert(receipt.study_id.clone(), ());
        experiment_ids.insert(receipt.experiment_id.clone(), ());
        revision_ids.insert(receipt.revision_id.clone(), ());
        head_ids.insert(receipt.base_head_id.clone(), ());
    }
    for head in &preview.heads {
        study_ids.insert(head.study_id.clone(), ());
        experiment_ids.insert(head.experiment_id.clone(), ());
        revision_ids.insert(head.revision_id.clone(), ());
        head_ids.insert(head.head_id.clone(), ());
        if let Some(parent) = head.parent_head_id.as_ref() {
            head_ids.insert(parent.clone(), ());
        }
    }
    for merge in &preview.merges {
        study_ids.insert(merge.study_id.clone(), ());
        experiment_ids.insert(merge.experiment_id.clone(), ());
        revision_ids.insert(merge.revision_id.clone(), ());
        head_ids.insert(merge.base_head_id.clone(), ());
        head_ids.insert(merge.merged_head_id.clone(), ());
    }
    for announcement in &preview.lifecycle_announcements {
        let plan = &announcement.certificate.body.payload.payload.plan;
        study_ids.insert(plan.study_id.clone(), ());
        experiment_ids.insert(plan.experiment_id.clone(), ());
        if let Some(base_revision_id) = plan.base_revision_id.as_ref() {
            revision_ids.insert(base_revision_id.clone(), ());
        }
        revision_ids.insert(plan.target_entry.current_revision_id.clone(), ());
    }
    for announcement in &preview.schedule_announcements {
        let epoch = &announcement.certificate.body.payload.payload.epoch;
        for assignment in &epoch.assignments {
            study_ids.insert(assignment.study_id.clone(), ());
            experiment_ids.insert(assignment.experiment_id.clone(), ());
            revision_ids.insert(assignment.revision_id.clone(), ());
        }
    }
    for metrics in &preview.peer_window_metrics {
        experiment_ids.insert(metrics.experiment_id.clone(), ());
        revision_ids.insert(metrics.revision_id.clone(), ());
        head_ids.insert(metrics.base_head_id.clone(), ());
    }
    for metrics in &preview.reducer_cohort_metrics {
        experiment_ids.insert(metrics.experiment_id.clone(), ());
        revision_ids.insert(metrics.revision_id.clone(), ());
        head_ids.insert(metrics.base_head_id.clone(), ());
        if let Some(candidate_head_id) = metrics.candidate_head_id.as_ref() {
            head_ids.insert(candidate_head_id.clone(), ());
        }
    }
    for report in &preview.head_eval_reports {
        experiment_ids.insert(report.experiment_id.clone(), ());
        revision_ids.insert(report.revision_id.clone(), ());
        head_ids.insert(report.head_id.clone(), ());
        if let Some(base_head_id) = report.base_head_id.as_ref() {
            head_ids.insert(base_head_id.clone(), ());
        }
    }

    (
        study_ids.into_keys().collect(),
        experiment_ids.into_keys().collect(),
        revision_ids.into_keys().collect(),
        head_ids.into_keys().collect(),
    )
}

fn build_replay_snapshot_summary(
    preview: &FileOperatorStorePreview,
    captured_at: DateTime<Utc>,
) -> OperatorReplaySnapshotSummary {
    let (study_ids, experiment_ids, revision_ids, head_ids) = preview_scope_ids(preview);
    let summary = BTreeMap::from([
        ("kind".into(), "operator_snapshot".into()),
        ("heads".into(), preview.heads.len().to_string()),
        ("receipts".into(), preview.receipts.len().to_string()),
        ("merges".into(), preview.merges.len().to_string()),
        (
            "lifecycle_plans".into(),
            preview.lifecycle_announcements.len().to_string(),
        ),
        (
            "schedule_epochs".into(),
            preview.schedule_announcements.len().to_string(),
        ),
        (
            "peer_window_metrics".into(),
            preview.peer_window_metrics.len().to_string(),
        ),
        (
            "reducer_cohort_metrics".into(),
            preview.reducer_cohort_metrics.len().to_string(),
        ),
        (
            "head_eval_reports".into(),
            preview.head_eval_reports.len().to_string(),
        ),
    ]);

    OperatorReplaySnapshotSummary {
        captured_at,
        study_ids,
        experiment_ids,
        revision_ids,
        head_ids,
        receipt_count: preview.receipts.len(),
        head_count: preview.heads.len(),
        merge_count: preview.merges.len(),
        lifecycle_plan_count: preview.lifecycle_announcements.len(),
        schedule_epoch_count: preview.schedule_announcements.len(),
        peer_window_metric_count: preview.peer_window_metrics.len(),
        reducer_cohort_metric_count: preview.reducer_cohort_metrics.len(),
        head_eval_report_count: preview.head_eval_reports.len(),
        summary,
    }
}

fn build_control_replay_records_from_preview(
    preview: &FileOperatorStorePreview,
    _captured_at: DateTime<Utc>,
) -> Vec<OperatorControlReplayRecord> {
    let mut records = Vec::new();

    for announcement in &preview.lifecycle_announcements {
        let plan = &announcement.certificate.body.payload.payload.plan;
        records.push(OperatorControlReplayRecord {
            kind: OperatorControlReplayKind::LifecyclePlan,
            record_id: announcement.certificate.control_cert_id.as_str().to_owned(),
            network_id: announcement.certificate.network_id.clone(),
            study_id: plan.study_id.clone(),
            experiment_id: plan.target_entry.experiment_id.clone(),
            revision_id: plan.target_entry.current_revision_id.clone(),
            source_experiment_id: (plan.experiment_id != plan.target_entry.experiment_id)
                .then(|| plan.experiment_id.clone()),
            source_revision_id: plan.base_revision_id.clone(),
            peer_id: None,
            window_id: plan.target.activation.activation_window,
            ends_before_window: None,
            slot_index: None,
            plan_epoch: plan.plan_epoch,
            captured_at: announcement.announced_at,
            summary: BTreeMap::from([
                ("kind".into(), "lifecycle_plan".into()),
                ("phase".into(), format!("{:?}", plan.phase)),
                (
                    "source_experiment_id".into(),
                    plan.experiment_id.as_str().to_owned(),
                ),
                (
                    "target_experiment_id".into(),
                    plan.target_entry.experiment_id.as_str().to_owned(),
                ),
                (
                    "target_revision_id".into(),
                    plan.target_entry.current_revision_id.as_str().to_owned(),
                ),
                (
                    "activation_window".into(),
                    plan.target.activation.activation_window.0.to_string(),
                ),
                ("plan_epoch".into(), plan.plan_epoch.to_string()),
                ("reason".into(), plan.reason.clone().unwrap_or_default()),
            ]),
        });
    }

    for announcement in &preview.schedule_announcements {
        let epoch = &announcement.certificate.body.payload.payload.epoch;
        for assignment in &epoch.assignments {
            records.push(OperatorControlReplayRecord {
                kind: OperatorControlReplayKind::ScheduleEpoch,
                record_id: format!(
                    "{}:{}:{}",
                    announcement.certificate.control_cert_id.as_str(),
                    assignment.peer_id.as_str(),
                    assignment.slot_index
                ),
                network_id: announcement.certificate.network_id.clone(),
                study_id: assignment.study_id.clone(),
                experiment_id: assignment.experiment_id.clone(),
                revision_id: assignment.revision_id.clone(),
                source_experiment_id: None,
                source_revision_id: None,
                peer_id: Some(assignment.peer_id.clone()),
                window_id: epoch.target.activation.activation_window,
                ends_before_window: epoch.ends_before_window,
                slot_index: Some(assignment.slot_index),
                plan_epoch: epoch.plan_epoch,
                captured_at: announcement.announced_at,
                summary: BTreeMap::from([
                    ("kind".into(), "schedule_epoch".into()),
                    (
                        "activation_window".into(),
                        epoch.target.activation.activation_window.0.to_string(),
                    ),
                    (
                        "ends_before_window".into(),
                        epoch
                            .ends_before_window
                            .map(|window| window.0.to_string())
                            .unwrap_or_default(),
                    ),
                    ("plan_epoch".into(), epoch.plan_epoch.to_string()),
                    ("slot_index".into(), assignment.slot_index.to_string()),
                    (
                        "budget_scale".into(),
                        assignment
                            .budget_scale
                            .map(|value| format!("{value:.4}"))
                            .unwrap_or_default(),
                    ),
                    (
                        "microshard_scale".into(),
                        assignment
                            .microshard_scale
                            .map(|value| format!("{value:.4}"))
                            .unwrap_or_default(),
                    ),
                    ("reason".into(), epoch.reason.clone().unwrap_or_default()),
                ]),
            });
        }
    }

    records.sort_by(|left, right| {
        right
            .captured_at
            .cmp(&left.captured_at)
            .then_with(|| left.record_id.cmp(&right.record_id))
    });
    records
}

fn build_operator_control_replay_summary(
    backend: String,
    records: impl IntoIterator<Item = OperatorControlReplayRecord>,
) -> OperatorControlReplaySummary {
    let records = records.into_iter().collect::<Vec<_>>();
    let mut counts_by_kind = BTreeMap::<String, usize>::new();
    let mut network_ids = BTreeMap::<NetworkId, ()>::new();
    let mut study_ids = BTreeMap::<StudyId, ()>::new();
    let mut experiment_ids = BTreeMap::<ExperimentId, ()>::new();
    let mut revision_ids = BTreeMap::<RevisionId, ()>::new();
    let mut peer_ids = BTreeMap::<PeerId, ()>::new();
    let mut window_ids = BTreeMap::<WindowId, ()>::new();
    let mut earliest: Option<DateTime<Utc>> = None;
    let mut latest: Option<DateTime<Utc>> = None;

    for record in &records {
        let captured_at = record.captured_at;
        *counts_by_kind
            .entry(record.kind.as_slug().to_owned())
            .or_default() += 1;
        network_ids.insert(record.network_id.clone(), ());
        study_ids.insert(record.study_id.clone(), ());
        experiment_ids.insert(record.experiment_id.clone(), ());
        if let Some(source_experiment_id) = record.source_experiment_id.as_ref() {
            experiment_ids.insert(source_experiment_id.clone(), ());
        }
        revision_ids.insert(record.revision_id.clone(), ());
        if let Some(source_revision_id) = record.source_revision_id.as_ref() {
            revision_ids.insert(source_revision_id.clone(), ());
        }
        if let Some(peer_id) = record.peer_id.as_ref() {
            peer_ids.insert(peer_id.clone(), ());
        }
        window_ids.insert(record.window_id, ());
        earliest = Some(
            earliest
                .map(|value| value.min(captured_at))
                .unwrap_or(captured_at),
        );
        latest = Some(
            latest
                .map(|value| value.max(captured_at))
                .unwrap_or(captured_at),
        );
    }

    OperatorControlReplaySummary {
        backend,
        record_count: records.len(),
        counts_by_kind,
        distinct_network_count: network_ids.len(),
        distinct_study_count: study_ids.len(),
        distinct_experiment_count: experiment_ids.len(),
        distinct_revision_count: revision_ids.len(),
        distinct_peer_count: peer_ids.len(),
        distinct_window_count: window_ids.len(),
        earliest_captured_at: earliest,
        latest_captured_at: latest,
    }
}

fn build_audit_records_from_preview(
    preview: &FileOperatorStorePreview,
    snapshot_captured_at: Option<DateTime<Utc>>,
) -> Vec<OperatorAuditRecord> {
    let mut records = Vec::new();

    for receipt in &preview.receipts {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::Receipt,
            record_id: receipt.receipt_id.as_str().to_owned(),
            study_id: Some(receipt.study_id.clone()),
            experiment_id: Some(receipt.experiment_id.clone()),
            revision_id: Some(receipt.revision_id.clone()),
            peer_id: Some(receipt.peer_id.clone()),
            head_id: Some(receipt.base_head_id.clone()),
            captured_at: receipt.accepted_at,
            summary: BTreeMap::from([
                ("kind".into(), "receipt".into()),
                (
                    "accepted_weight".into(),
                    format!("{:.6}", receipt.accepted_weight),
                ),
                (
                    "artifact_id".into(),
                    receipt.artifact_id.as_str().to_owned(),
                ),
            ]),
        });
    }

    for head in &preview.heads {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::Head,
            record_id: head.head_id.as_str().to_owned(),
            study_id: Some(head.study_id.clone()),
            experiment_id: Some(head.experiment_id.clone()),
            revision_id: Some(head.revision_id.clone()),
            peer_id: None,
            head_id: Some(head.head_id.clone()),
            captured_at: head.created_at,
            summary: BTreeMap::from([
                ("kind".into(), "head".into()),
                ("artifact_id".into(), head.artifact_id.as_str().to_owned()),
                ("global_step".into(), head.global_step.to_string()),
            ]),
        });
    }

    for merge in &preview.merges {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::Merge,
            record_id: merge.merge_cert_id.as_str().to_owned(),
            study_id: Some(merge.study_id.clone()),
            experiment_id: Some(merge.experiment_id.clone()),
            revision_id: Some(merge.revision_id.clone()),
            peer_id: Some(merge.promoter_peer_id.clone()),
            head_id: Some(merge.merged_head_id.clone()),
            captured_at: merge.issued_at,
            summary: BTreeMap::from([
                ("kind".into(), "merge".into()),
                (
                    "merged_artifact_id".into(),
                    merge.merged_artifact_id.as_str().to_owned(),
                ),
                ("policy".into(), format!("{:?}", merge.policy)),
            ]),
        });
    }

    for announcement in &preview.lifecycle_announcements {
        let plan = &announcement.certificate.body.payload.payload.plan;
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::LifecyclePlan,
            record_id: announcement.certificate.control_cert_id.as_str().to_owned(),
            study_id: Some(plan.study_id.clone()),
            experiment_id: Some(plan.experiment_id.clone()),
            revision_id: Some(plan.target_entry.current_revision_id.clone()),
            peer_id: None,
            head_id: plan.target_entry.current_head_id.clone(),
            captured_at: announcement.announced_at,
            summary: BTreeMap::from([
                ("kind".into(), "lifecycle_plan".into()),
                ("phase".into(), format!("{:?}", plan.phase)),
                (
                    "activation_window".into(),
                    plan.target.activation.activation_window.0.to_string(),
                ),
                ("plan_epoch".into(), plan.plan_epoch.to_string()),
                (
                    "target_experiment_id".into(),
                    plan.target_entry.experiment_id.as_str().to_owned(),
                ),
                (
                    "target_revision_id".into(),
                    plan.target_entry.current_revision_id.as_str().to_owned(),
                ),
                ("reason".into(), plan.reason.clone().unwrap_or_default()),
            ]),
        });
    }

    for announcement in &preview.schedule_announcements {
        let epoch = &announcement.certificate.body.payload.payload.epoch;
        for assignment in &epoch.assignments {
            records.push(OperatorAuditRecord {
                kind: OperatorAuditKind::ScheduleEpoch,
                record_id: format!(
                    "{}:{}:{}",
                    announcement.certificate.control_cert_id.as_str(),
                    assignment.peer_id.as_str(),
                    assignment.slot_index
                ),
                study_id: Some(assignment.study_id.clone()),
                experiment_id: Some(assignment.experiment_id.clone()),
                revision_id: Some(assignment.revision_id.clone()),
                peer_id: Some(assignment.peer_id.clone()),
                head_id: None,
                captured_at: announcement.announced_at,
                summary: BTreeMap::from([
                    ("kind".into(), "schedule_epoch".into()),
                    (
                        "activation_window".into(),
                        epoch.target.activation.activation_window.0.to_string(),
                    ),
                    (
                        "ends_before_window".into(),
                        epoch
                            .ends_before_window
                            .map(|window| window.0.to_string())
                            .unwrap_or_default(),
                    ),
                    ("plan_epoch".into(), epoch.plan_epoch.to_string()),
                    ("slot_index".into(), assignment.slot_index.to_string()),
                    (
                        "budget_scale".into(),
                        assignment
                            .budget_scale
                            .map(|value| format!("{value:.4}"))
                            .unwrap_or_default(),
                    ),
                    (
                        "microshard_scale".into(),
                        assignment
                            .microshard_scale
                            .map(|value| format!("{value:.4}"))
                            .unwrap_or_default(),
                    ),
                    ("reason".into(), epoch.reason.clone().unwrap_or_default()),
                ]),
            });
        }
    }

    for metrics in &preview.peer_window_metrics {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::PeerWindowMetric,
            record_id: metrics.lease_id.as_str().to_owned(),
            study_id: None,
            experiment_id: Some(metrics.experiment_id.clone()),
            revision_id: Some(metrics.revision_id.clone()),
            peer_id: Some(metrics.peer_id.clone()),
            head_id: Some(metrics.base_head_id.clone()),
            captured_at: metrics.window_finished_at,
            summary: BTreeMap::from([
                ("kind".into(), "peer_window_metric".into()),
                ("status".into(), format!("{:?}", metrics.status)),
                (
                    "accepted_tokens_or_samples".into(),
                    metrics
                        .accepted_tokens_or_samples
                        .unwrap_or_default()
                        .to_string(),
                ),
                (
                    "head_lag_at_finish".into(),
                    metrics.head_lag_at_finish.to_string(),
                ),
            ]),
        });
    }

    for metrics in &preview.reducer_cohort_metrics {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::ReducerCohortMetric,
            record_id: metrics.merge_window_id.as_str().to_owned(),
            study_id: None,
            experiment_id: Some(metrics.experiment_id.clone()),
            revision_id: Some(metrics.revision_id.clone()),
            peer_id: None,
            head_id: metrics.candidate_head_id.clone(),
            captured_at: metrics.captured_at,
            summary: BTreeMap::from([
                ("kind".into(), "reducer_cohort_metric".into()),
                ("status".into(), format!("{:?}", metrics.status)),
                (
                    "accepted_updates".into(),
                    metrics.accepted_updates.to_string(),
                ),
                (
                    "accepted_tokens_or_samples".into(),
                    metrics.accepted_tokens_or_samples.to_string(),
                ),
            ]),
        });
    }

    for report in &preview.head_eval_reports {
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::HeadEvalReport,
            record_id: report.eval_protocol_id.as_str().to_owned(),
            study_id: None,
            experiment_id: Some(report.experiment_id.clone()),
            revision_id: Some(report.revision_id.clone()),
            peer_id: None,
            head_id: Some(report.head_id.clone()),
            captured_at: report.finished_at,
            summary: BTreeMap::from([
                ("kind".into(), "head_eval_report".into()),
                ("status".into(), format!("{:?}", report.status)),
                ("sample_count".into(), report.sample_count.to_string()),
            ]),
        });
    }

    if let Some(captured_at) = snapshot_captured_at {
        let replay_summary = build_replay_snapshot_summary(preview, captured_at);
        records.push(OperatorAuditRecord {
            kind: OperatorAuditKind::Snapshot,
            record_id: format!("snapshot:{}", captured_at.timestamp_millis()),
            study_id: None,
            experiment_id: None,
            revision_id: None,
            peer_id: None,
            head_id: None,
            captured_at,
            summary: replay_summary.summary,
        });
    }

    records
}

#[derive(Clone, Debug)]
pub(crate) struct FileOperatorStore {
    history_root: Option<PathBuf>,
    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    metrics_store_root: Option<PathBuf>,
    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    metrics_retention: MetricsRetentionBudget,
    #[cfg_attr(not(feature = "artifact-publish"), allow(dead_code))]
    publication_store_root: Option<PathBuf>,
    #[cfg_attr(not(feature = "artifact-publish"), allow(dead_code))]
    publication_targets: Vec<PublicationTarget>,
    #[cfg_attr(not(feature = "artifact-publish"), allow(dead_code))]
    artifact_store_root: Option<PathBuf>,
    operator_state_backend: Option<OperatorStateBackendConfig>,
    receipt_preview: Vec<ContributionReceipt>,
    head_preview: Vec<HeadDescriptor>,
    merge_preview: Vec<MergeCertificate>,
    lifecycle_preview: Vec<ExperimentLifecycleAnnouncement>,
    schedule_preview: Vec<FleetScheduleAnnouncement>,
    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    peer_window_preview: Vec<PeerWindowMetrics>,
    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    reducer_cohort_preview: Vec<ReducerCohortMetrics>,
    head_eval_preview: Vec<HeadEvalReport>,
    #[cfg_attr(not(feature = "artifact-publish"), allow(dead_code))]
    eval_protocol_preview: Vec<StoredEvalProtocolManifestRecord>,
}

impl FileOperatorStore {
    pub(crate) fn new(config: FileOperatorStoreConfig, preview: FileOperatorStorePreview) -> Self {
        Self {
            history_root: config.history_root,
            metrics_store_root: config.metrics_store_root,
            metrics_retention: config.metrics_retention,
            publication_store_root: config.publication_store_root,
            publication_targets: config.publication_targets,
            artifact_store_root: config.artifact_store_root,
            operator_state_backend: config.operator_state_backend,
            receipt_preview: preview.receipts,
            head_preview: preview.heads,
            merge_preview: preview.merges,
            lifecycle_preview: preview.lifecycle_announcements,
            schedule_preview: preview.schedule_announcements,
            peer_window_preview: preview.peer_window_metrics,
            reducer_cohort_preview: preview.reducer_cohort_metrics,
            head_eval_preview: preview.head_eval_reports,
            eval_protocol_preview: preview.eval_protocol_manifests,
        }
    }

    fn storage_config(&self) -> Option<StorageConfig> {
        self.history_root.clone().map(StorageConfig::new)
    }

    fn redis_connection(url: &str) -> anyhow::Result<redis::Connection> {
        Ok(redis::Client::open(url)?.get_connection()?)
    }

    fn remote_snapshot(&self) -> anyhow::Result<Option<OperatorStateSnapshot>> {
        let Some(backend) = self.operator_state_backend.as_ref() else {
            return Ok(None);
        };

        match backend {
            OperatorStateBackendConfig::Redis { url, snapshot_key } => {
                let mut connection = Self::redis_connection(url)?;
                let payload = connection.get::<_, Option<String>>(snapshot_key)?;
                payload
                    .map(|encoded| {
                        serde_json::from_str::<OperatorStateSnapshot>(&encoded)
                            .map_err(anyhow::Error::from)
                    })
                    .transpose()
            }
            OperatorStateBackendConfig::Postgres {
                url,
                table_name,
                snapshot_key,
            } => load_postgres_operator_state_snapshot(url, table_name, snapshot_key),
        }
    }

    fn remote_preview(&self) -> anyhow::Result<Option<FileOperatorStorePreview>> {
        self.remote_snapshot()
            .map(|snapshot| snapshot.map(|snapshot| snapshot.preview))
    }

    pub(crate) fn all_head_eval_reports(&self) -> anyhow::Result<Vec<HeadEvalReport>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(preview.head_eval_reports);
        }

        #[cfg(feature = "metrics-indexer")]
        if let Some(root) = self.metrics_store_root.as_ref() {
            let store = burn_p2p_metrics::MetricsStore::open(
                root,
                crate::metrics::metrics_indexer_config_for_budget(self.metrics_retention),
            )
            .map_err(anyhow::Error::from)?;
            let mut reports = store
                .load_entries()
                .into_iter()
                .filter_map(|entry| match entry {
                    burn_p2p_metrics::MetricEnvelope::HeadEval(report) => Some(report),
                    _ => None,
                })
                .collect::<Vec<_>>();
            reports.sort_by_key(|report| report.finished_at);
            return Ok(reports);
        }

        self.storage_config()
            .map(|storage| history::load_all_head_eval_reports(&storage))
            .transpose()
            .map(|loaded| loaded.unwrap_or_else(|| self.head_eval_preview.clone()))
    }

    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    pub(crate) fn all_peer_window_metrics(&self) -> anyhow::Result<Vec<PeerWindowMetrics>> {
        if let Some(preview) = self.remote_preview()? {
            let mut metrics = preview.peer_window_metrics;
            metrics.sort_by_key(|metrics| metrics.window_finished_at);
            return Ok(metrics);
        }

        #[cfg(feature = "metrics-indexer")]
        if let Some(root) = self.metrics_store_root.as_ref() {
            let store = burn_p2p_metrics::MetricsStore::open(
                root,
                crate::metrics::metrics_indexer_config_for_budget(self.metrics_retention),
            )
            .map_err(anyhow::Error::from)?;
            let mut metrics = store
                .load_entries()
                .into_iter()
                .filter_map(|entry| match entry {
                    burn_p2p_metrics::MetricEnvelope::PeerWindow(metrics) => Some(metrics),
                    _ => None,
                })
                .collect::<Vec<_>>();
            metrics.sort_by_key(|metrics| metrics.window_finished_at);
            return Ok(metrics);
        }

        self.storage_config()
            .map(|storage| history::load_peer_window_metrics(&storage))
            .transpose()
            .map(|loaded| {
                let mut metrics = loaded.unwrap_or_else(|| self.peer_window_preview.clone());
                metrics.sort_by_key(|metrics| metrics.window_finished_at);
                metrics
            })
    }

    #[cfg_attr(not(feature = "metrics-indexer"), allow(dead_code))]
    pub(crate) fn all_reducer_cohort_metrics(&self) -> anyhow::Result<Vec<ReducerCohortMetrics>> {
        if let Some(preview) = self.remote_preview()? {
            let mut metrics = preview.reducer_cohort_metrics;
            metrics.sort_by_key(|metrics| metrics.captured_at);
            return Ok(metrics);
        }

        #[cfg(feature = "metrics-indexer")]
        if let Some(root) = self.metrics_store_root.as_ref() {
            let store = burn_p2p_metrics::MetricsStore::open(
                root,
                crate::metrics::metrics_indexer_config_for_budget(self.metrics_retention),
            )
            .map_err(anyhow::Error::from)?;
            let mut metrics = store
                .load_entries()
                .into_iter()
                .filter_map(|entry| match entry {
                    burn_p2p_metrics::MetricEnvelope::ReducerCohort(metrics) => Some(metrics),
                    _ => None,
                })
                .collect::<Vec<_>>();
            metrics.sort_by_key(|metrics| metrics.captured_at);
            return Ok(metrics);
        }

        self.storage_config()
            .map(|storage| history::load_reducer_cohort_metrics(&storage))
            .transpose()
            .map(|loaded| {
                let mut metrics = loaded.unwrap_or_else(|| self.reducer_cohort_preview.clone());
                metrics.sort_by_key(|metrics| metrics.captured_at);
                metrics
            })
    }

    fn build_audit_records(&self) -> anyhow::Result<Vec<OperatorAuditRecord>> {
        let preview = FileOperatorStorePreview {
            receipts: self.receipts(&ReceiptQuery::default())?,
            heads: self.heads(&HeadQuery::default())?,
            merges: self.merges()?,
            lifecycle_announcements: self.lifecycle_preview.clone(),
            schedule_announcements: self.schedule_preview.clone(),
            peer_window_metrics: self.all_peer_window_metrics()?,
            reducer_cohort_metrics: self.all_reducer_cohort_metrics()?,
            head_eval_reports: self.all_head_eval_reports()?,
            #[cfg(feature = "artifact-publish")]
            eval_protocol_manifests: self.eval_protocol_manifests()?,
            #[cfg(not(feature = "artifact-publish"))]
            eval_protocol_manifests: self.eval_protocol_preview.clone(),
        };
        let snapshot_captured_at = self.remote_snapshot()?.map(|snapshot| snapshot.captured_at);
        let mut records = build_audit_records_from_preview(&preview, snapshot_captured_at);

        records.sort_by(|left, right| {
            right
                .captured_at
                .cmp(&left.captured_at)
                .then_with(|| left.record_id.cmp(&right.record_id))
        });
        Ok(records)
    }

    fn build_control_replay_records(&self) -> anyhow::Result<Vec<OperatorControlReplayRecord>> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            let rows = load_postgres_operator_state_rows(url, table_name, snapshot_key)?;
            let mut records = rows
                .into_iter()
                .flat_map(|row| {
                    build_control_replay_records_from_preview(&row.preview, row.captured_at)
                })
                .collect::<Vec<_>>();
            records.sort_by(|left, right| {
                right
                    .captured_at
                    .cmp(&left.captured_at)
                    .then_with(|| left.record_id.cmp(&right.record_id))
            });
            return Ok(records);
        }

        if let Some(snapshot) = self.remote_snapshot()? {
            return Ok(build_control_replay_records_from_preview(
                &snapshot.preview,
                snapshot.captured_at,
            ));
        }

        Ok(build_control_replay_records_from_preview(
            &FileOperatorStorePreview {
                receipts: Vec::new(),
                heads: Vec::new(),
                merges: Vec::new(),
                lifecycle_announcements: self.lifecycle_preview.clone(),
                schedule_announcements: self.schedule_preview.clone(),
                peer_window_metrics: Vec::new(),
                reducer_cohort_metrics: Vec::new(),
                head_eval_reports: Vec::new(),
                eval_protocol_manifests: Vec::new(),
            },
            Utc::now(),
        ))
    }
}

fn ensure_valid_postgres_identifier(identifier: &str) -> anyhow::Result<&str> {
    if identifier.is_empty()
        || !identifier
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        anyhow::bail!("invalid postgres identifier `{identifier}`");
    }
    Ok(identifier)
}

fn postgres_connection(url: &str) -> anyhow::Result<postgres::Client> {
    Ok(postgres::Client::connect(url, NoTls)?)
}

fn ensure_postgres_operator_state_table(
    client: &mut postgres::Client,
    table_name: &str,
) -> anyhow::Result<()> {
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    client.batch_execute(&format!(
        r#"
CREATE TABLE IF NOT EXISTS {table_name} (
    id BIGSERIAL PRIMARY KEY,
    snapshot_key TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    preview_json JSONB NOT NULL,
    summary_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    summary_text TEXT NOT NULL DEFAULT '',
    study_ids TEXT[] NOT NULL DEFAULT '{{}}',
    experiment_ids TEXT[] NOT NULL DEFAULT '{{}}',
    revision_ids TEXT[] NOT NULL DEFAULT '{{}}',
    head_ids TEXT[] NOT NULL DEFAULT '{{}}',
    receipt_count BIGINT NOT NULL DEFAULT 0,
    head_count BIGINT NOT NULL DEFAULT 0,
    merge_count BIGINT NOT NULL DEFAULT 0,
    lifecycle_plan_count BIGINT NOT NULL DEFAULT 0,
    schedule_epoch_count BIGINT NOT NULL DEFAULT 0,
    peer_window_metric_count BIGINT NOT NULL DEFAULT 0,
    reducer_cohort_metric_count BIGINT NOT NULL DEFAULT 0,
    head_eval_report_count BIGINT NOT NULL DEFAULT 0
);
ALTER TABLE {table_name}
    ADD COLUMN IF NOT EXISTS lifecycle_plan_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE {table_name}
    ADD COLUMN IF NOT EXISTS schedule_epoch_count BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS {table_name}_snapshot_lookup
    ON {table_name} (snapshot_key, captured_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS {table_name}_captured_lookup
    ON {table_name} (snapshot_key, captured_at DESC);
CREATE INDEX IF NOT EXISTS {table_name}_study_ids_gin
    ON {table_name} USING GIN (study_ids);
CREATE INDEX IF NOT EXISTS {table_name}_experiment_ids_gin
    ON {table_name} USING GIN (experiment_ids);
CREATE INDEX IF NOT EXISTS {table_name}_revision_ids_gin
    ON {table_name} USING GIN (revision_ids);
CREATE INDEX IF NOT EXISTS {table_name}_head_ids_gin
    ON {table_name} USING GIN (head_ids);
"#
    ))?;
    Ok(())
}

fn ensure_postgres_operator_audit_table(
    client: &mut postgres::Client,
    table_name: &str,
) -> anyhow::Result<()> {
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    client.batch_execute(&format!(
        r#"
CREATE TABLE IF NOT EXISTS {table_name} (
    id BIGSERIAL PRIMARY KEY,
    snapshot_key TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    record_id TEXT NOT NULL,
    study_id TEXT NULL,
    experiment_id TEXT NULL,
    revision_id TEXT NULL,
    peer_id TEXT NULL,
    head_id TEXT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    summary_json JSONB NOT NULL,
    summary_text TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_snapshot_dedupe
    ON {table_name} (snapshot_key, dedupe_key);
CREATE INDEX IF NOT EXISTS {table_name}_snapshot_lookup
    ON {table_name} (snapshot_key, captured_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS {table_name}_kind_lookup
    ON {table_name} (snapshot_key, kind, captured_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS {table_name}_revision_lookup
    ON {table_name} (snapshot_key, experiment_id, revision_id, captured_at DESC, id DESC);
"#
    ))?;
    Ok(())
}

fn operator_audit_record_dedupe_key(record: &OperatorAuditRecord) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        record.kind.as_slug(),
        record.record_id,
        record.captured_at.timestamp_millis(),
        record
            .study_id
            .as_ref()
            .map(StudyId::as_str)
            .unwrap_or_default(),
        record
            .experiment_id
            .as_ref()
            .map(ExperimentId::as_str)
            .unwrap_or_default(),
        record
            .revision_id
            .as_ref()
            .map(RevisionId::as_str)
            .unwrap_or_default(),
        record
            .peer_id
            .as_ref()
            .map(PeerId::as_str)
            .unwrap_or_default(),
    )
}

fn operator_audit_record_summary_text(record: &OperatorAuditRecord) -> String {
    let mut text = record.record_id.clone();
    for (key, value) in &record.summary {
        text.push(' ');
        text.push_str(key);
        text.push('=');
        text.push_str(value);
    }
    text
}

fn replay_snapshot_summary_text(summary: &OperatorReplaySnapshotSummary) -> String {
    let mut text = summary
        .study_ids
        .iter()
        .map(StudyId::as_str)
        .chain(summary.experiment_ids.iter().map(ExperimentId::as_str))
        .chain(summary.revision_ids.iter().map(RevisionId::as_str))
        .chain(summary.head_ids.iter().map(HeadId::as_str))
        .collect::<Vec<_>>()
        .join(" ");
    for (key, value) in &summary.summary {
        text.push(' ');
        text.push_str(key);
        text.push('=');
        text.push_str(value);
    }
    text.trim().to_owned()
}

fn operator_backend_label(backend: Option<&OperatorStateBackendConfig>) -> String {
    match backend {
        Some(OperatorStateBackendConfig::Redis { .. }) => "redis".into(),
        Some(OperatorStateBackendConfig::Postgres { .. }) => "postgres".into(),
        None => "file".into(),
    }
}

fn normalize_operator_facet_limit(limit: usize) -> usize {
    limit.clamp(1, 64)
}

fn top_operator_facet_buckets(
    values: impl IntoIterator<Item = String>,
    limit: usize,
) -> Vec<OperatorFacetBucket> {
    let mut counts = BTreeMap::<String, usize>::new();
    for value in values {
        if value.trim().is_empty() {
            continue;
        }
        *counts.entry(value).or_default() += 1;
    }
    let mut buckets = counts
        .into_iter()
        .map(|(value, count)| OperatorFacetBucket { value, count })
        .collect::<Vec<_>>();
    buckets.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.value.cmp(&right.value))
    });
    buckets.truncate(normalize_operator_facet_limit(limit));
    buckets
}

fn build_operator_audit_facets(
    backend: String,
    records: impl IntoIterator<Item = OperatorAuditRecord>,
    limit: usize,
) -> OperatorAuditFacetSummary {
    let normalized_limit = normalize_operator_facet_limit(limit);
    let mut kinds = Vec::new();
    let mut studies = Vec::new();
    let mut experiments = Vec::new();
    let mut revisions = Vec::new();
    let mut peers = Vec::new();
    let mut heads = Vec::new();

    for record in records {
        kinds.push(record.kind.as_slug().to_owned());
        if let Some(study_id) = record.study_id {
            studies.push(study_id.as_str().to_owned());
        }
        if let Some(experiment_id) = record.experiment_id {
            experiments.push(experiment_id.as_str().to_owned());
        }
        if let Some(revision_id) = record.revision_id {
            revisions.push(revision_id.as_str().to_owned());
        }
        if let Some(peer_id) = record.peer_id {
            peers.push(peer_id.as_str().to_owned());
        }
        if let Some(head_id) = record.head_id {
            heads.push(head_id.as_str().to_owned());
        }
    }

    OperatorAuditFacetSummary {
        backend,
        limit: normalized_limit,
        kinds: top_operator_facet_buckets(kinds, normalized_limit),
        studies: top_operator_facet_buckets(studies, normalized_limit),
        experiments: top_operator_facet_buckets(experiments, normalized_limit),
        revisions: top_operator_facet_buckets(revisions, normalized_limit),
        peers: top_operator_facet_buckets(peers, normalized_limit),
        heads: top_operator_facet_buckets(heads, normalized_limit),
    }
}

fn build_operator_audit_summary(
    backend: String,
    records: impl IntoIterator<Item = OperatorAuditRecord>,
) -> OperatorAuditSummary {
    let mut record_count = 0usize;
    let mut counts_by_kind = BTreeMap::<String, usize>::new();
    let mut studies = BTreeMap::<StudyId, ()>::new();
    let mut experiments = BTreeMap::<ExperimentId, ()>::new();
    let mut revisions = BTreeMap::<RevisionId, ()>::new();
    let mut peers = BTreeMap::<PeerId, ()>::new();
    let mut heads = BTreeMap::<HeadId, ()>::new();
    let mut earliest_captured_at: Option<chrono::DateTime<chrono::Utc>> = None;
    let mut latest_captured_at: Option<chrono::DateTime<chrono::Utc>> = None;

    for record in records {
        record_count += 1;
        *counts_by_kind
            .entry(record.kind.as_slug().to_owned())
            .or_default() += 1;
        if let Some(study_id) = record.study_id {
            studies.insert(study_id, ());
        }
        if let Some(experiment_id) = record.experiment_id {
            experiments.insert(experiment_id, ());
        }
        if let Some(revision_id) = record.revision_id {
            revisions.insert(revision_id, ());
        }
        if let Some(peer_id) = record.peer_id {
            peers.insert(peer_id, ());
        }
        if let Some(head_id) = record.head_id {
            heads.insert(head_id, ());
        }
        earliest_captured_at = Some(match earliest_captured_at {
            Some(current) => current.min(record.captured_at),
            None => record.captured_at,
        });
        latest_captured_at = Some(match latest_captured_at {
            Some(current) => current.max(record.captured_at),
            None => record.captured_at,
        });
    }

    OperatorAuditSummary {
        backend,
        record_count,
        counts_by_kind,
        distinct_study_count: studies.len(),
        distinct_experiment_count: experiments.len(),
        distinct_revision_count: revisions.len(),
        distinct_peer_count: peers.len(),
        distinct_head_count: heads.len(),
        earliest_captured_at,
        latest_captured_at,
    }
}

fn insert_postgres_operator_audit_records(
    client: &mut postgres::Client,
    table_name: &str,
    snapshot_key: &str,
    records: &[OperatorAuditRecord],
) -> anyhow::Result<()> {
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    for record in records {
        let dedupe_key = operator_audit_record_dedupe_key(record);
        let kind = record.kind.as_slug().to_owned();
        let study_id = record
            .study_id
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let experiment_id = record
            .experiment_id
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let revision_id = record
            .revision_id
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let peer_id = record
            .peer_id
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let head_id = record
            .head_id
            .as_ref()
            .map(|value| value.as_str().to_owned());
        let summary_json = serde_json::to_string(&record.summary)?;
        let summary_text = operator_audit_record_summary_text(record);
        client.execute(
            &format!(
                "INSERT INTO {table_name} \
                 (snapshot_key, dedupe_key, kind, record_id, study_id, experiment_id, revision_id, peer_id, head_id, captured_at, summary_json, summary_text) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CAST($11 AS text)::jsonb, $12) \
                 ON CONFLICT (snapshot_key, dedupe_key) DO NOTHING"
            ),
            &[
                &snapshot_key,
                &dedupe_key,
                &kind,
                &record.record_id,
                &study_id,
                &experiment_id,
                &revision_id,
                &peer_id,
                &head_id,
                &record.captured_at,
                &summary_json,
                &summary_text,
            ],
        )?;
    }
    Ok(())
}

fn prune_postgres_snapshot_rows(
    client: &mut postgres::Client,
    table_name: &str,
    snapshot_key: &str,
    max_rows: usize,
) -> anyhow::Result<usize> {
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    let deleted = client.execute(
        &format!(
            "DELETE FROM {table_name} WHERE id IN (\
                SELECT id FROM {table_name} \
                WHERE snapshot_key = $1 \
                ORDER BY captured_at DESC, id DESC \
                OFFSET $2\
            )"
        ),
        &[&snapshot_key, &i64::try_from(max_rows).unwrap_or(i64::MAX)],
    )?;
    Ok(usize::try_from(deleted).unwrap_or(usize::MAX))
}

fn prune_postgres_audit_rows(
    client: &mut postgres::Client,
    table_name: &str,
    snapshot_key: &str,
    max_rows: usize,
) -> anyhow::Result<usize> {
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    let deleted = client.execute(
        &format!(
            "DELETE FROM {table_name} WHERE id IN (\
                SELECT id FROM {table_name} \
                WHERE snapshot_key = $1 \
                ORDER BY captured_at DESC, id DESC \
                OFFSET $2\
            )"
        ),
        &[&snapshot_key, &i64::try_from(max_rows).unwrap_or(i64::MAX)],
    )?;
    Ok(usize::try_from(deleted).unwrap_or(usize::MAX))
}

fn load_postgres_operator_audit_page(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
    query: &OperatorAuditQuery,
    page: PageRequest,
) -> anyhow::Result<Page<OperatorAuditRecord>> {
    let page = page.normalized();
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    ensure_postgres_operator_audit_table(&mut client, table_name)?;

    let mut params: Vec<Box<dyn ToSql + Sync>> = vec![Box::new(snapshot_key.to_owned())];
    let mut clauses = vec!["snapshot_key = $1".to_owned()];
    if let Some(kind) = query.kind.as_ref() {
        params.push(Box::new(kind.as_slug().to_owned()));
        clauses.push(format!("kind = ${}", params.len()));
    }
    if let Some(study_id) = query.study_id.as_ref() {
        params.push(Box::new(study_id.as_str().to_owned()));
        clauses.push(format!("study_id = ${}", params.len()));
    }
    if let Some(experiment_id) = query.experiment_id.as_ref() {
        params.push(Box::new(experiment_id.as_str().to_owned()));
        clauses.push(format!("experiment_id = ${}", params.len()));
    }
    if let Some(revision_id) = query.revision_id.as_ref() {
        params.push(Box::new(revision_id.as_str().to_owned()));
        clauses.push(format!("revision_id = ${}", params.len()));
    }
    if let Some(peer_id) = query.peer_id.as_ref() {
        params.push(Box::new(peer_id.as_str().to_owned()));
        clauses.push(format!("peer_id = ${}", params.len()));
    }
    if let Some(head_id) = query.head_id.as_ref() {
        params.push(Box::new(head_id.as_str().to_owned()));
        clauses.push(format!("head_id = ${}", params.len()));
    }
    if let Some(since) = query.since.as_ref() {
        params.push(Box::new(since.to_owned()));
        clauses.push(format!("captured_at >= ${}", params.len()));
    }
    if let Some(until) = query.until.as_ref() {
        params.push(Box::new(until.to_owned()));
        clauses.push(format!("captured_at <= ${}", params.len()));
    }
    if let Some(text) = query
        .text
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let pattern = format!("%{}%", text.to_ascii_lowercase());
        params.push(Box::new(pattern.clone()));
        let record_idx = params.len();
        params.push(Box::new(pattern));
        let summary_idx = params.len();
        clauses.push(format!(
            "(LOWER(record_id) LIKE ${record_idx} OR LOWER(summary_text) LIKE ${summary_idx})"
        ));
    }
    let where_sql = clauses.join(" AND ");
    let refs = params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    let count = client
        .query_one(
            &format!("SELECT COUNT(*) FROM {table_name} WHERE {where_sql}"),
            &refs,
        )?
        .get::<_, i64>(0)
        .max(0) as usize;

    let mut page_params = params;
    page_params.push(Box::new(i64::try_from(page.offset).unwrap_or(i64::MAX)));
    let offset_idx = page_params.len();
    page_params.push(Box::new(i64::try_from(page.limit).unwrap_or(i64::MAX)));
    let limit_idx = page_params.len();
    let page_refs = page_params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    let rows = client.query(
        &format!(
            "SELECT kind, record_id, study_id, experiment_id, revision_id, peer_id, head_id, captured_at, summary_json::text \
             FROM {table_name} \
             WHERE {where_sql} \
             ORDER BY captured_at DESC, id DESC \
             OFFSET ${offset_idx} LIMIT ${limit_idx}"
        ),
        &page_refs,
    )?;

    let items = rows
        .into_iter()
        .map(|row| {
            let kind = OperatorAuditKind::from_slug(&row.get::<_, String>(0))
                .ok_or_else(|| anyhow::anyhow!("unknown operator audit kind stored in postgres"))?;
            let summary_json = row.get::<_, String>(8);
            Ok(OperatorAuditRecord {
                kind,
                record_id: row.get(1),
                study_id: row.get::<_, Option<String>>(2).map(StudyId::new),
                experiment_id: row.get::<_, Option<String>>(3).map(ExperimentId::new),
                revision_id: row.get::<_, Option<String>>(4).map(RevisionId::new),
                peer_id: row.get::<_, Option<String>>(5).map(PeerId::new),
                head_id: row.get::<_, Option<String>>(6).map(HeadId::new),
                captured_at: row.get(7),
                summary: serde_json::from_str(&summary_json)?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(Page::new(items, page, count))
}

fn load_postgres_operator_audit_summary(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
    query: &OperatorAuditQuery,
) -> anyhow::Result<OperatorAuditSummary> {
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    ensure_postgres_operator_audit_table(&mut client, table_name)?;

    let mut params: Vec<Box<dyn ToSql + Sync>> = vec![Box::new(snapshot_key.to_owned())];
    let mut clauses = vec!["snapshot_key = $1".to_owned()];
    if let Some(kind) = query.kind.as_ref() {
        params.push(Box::new(kind.as_slug().to_owned()));
        clauses.push(format!("kind = ${}", params.len()));
    }
    if let Some(study_id) = query.study_id.as_ref() {
        params.push(Box::new(study_id.as_str().to_owned()));
        clauses.push(format!("study_id = ${}", params.len()));
    }
    if let Some(experiment_id) = query.experiment_id.as_ref() {
        params.push(Box::new(experiment_id.as_str().to_owned()));
        clauses.push(format!("experiment_id = ${}", params.len()));
    }
    if let Some(revision_id) = query.revision_id.as_ref() {
        params.push(Box::new(revision_id.as_str().to_owned()));
        clauses.push(format!("revision_id = ${}", params.len()));
    }
    if let Some(peer_id) = query.peer_id.as_ref() {
        params.push(Box::new(peer_id.as_str().to_owned()));
        clauses.push(format!("peer_id = ${}", params.len()));
    }
    if let Some(head_id) = query.head_id.as_ref() {
        params.push(Box::new(head_id.as_str().to_owned()));
        clauses.push(format!("head_id = ${}", params.len()));
    }
    if let Some(since) = query.since.as_ref() {
        params.push(Box::new(*since));
        clauses.push(format!("captured_at >= ${}", params.len()));
    }
    if let Some(until) = query.until.as_ref() {
        params.push(Box::new(*until));
        clauses.push(format!("captured_at <= ${}", params.len()));
    }
    if let Some(text) = query
        .text
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let pattern = format!("%{}%", text.to_ascii_lowercase());
        params.push(Box::new(pattern.clone()));
        let record_idx = params.len();
        params.push(Box::new(pattern));
        let summary_idx = params.len();
        clauses.push(format!(
            "(LOWER(record_id) LIKE ${record_idx} OR LOWER(summary_text) LIKE ${summary_idx})"
        ));
    }
    let where_sql = clauses.join(" AND ");
    let refs = params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    let summary_row = client.query_one(
        &format!(
            "SELECT COUNT(*), \
                    COUNT(DISTINCT study_id), \
                    COUNT(DISTINCT experiment_id), \
                    COUNT(DISTINCT revision_id), \
                    COUNT(DISTINCT peer_id), \
                    COUNT(DISTINCT head_id), \
                    MIN(captured_at), \
                    MAX(captured_at) \
             FROM {table_name} WHERE {where_sql}"
        ),
        &refs,
    )?;
    let kind_rows = client.query(
        &format!(
            "SELECT kind, COUNT(*) \
             FROM {table_name} \
             WHERE {where_sql} \
             GROUP BY kind \
             ORDER BY kind ASC"
        ),
        &refs,
    )?;
    let counts_by_kind = kind_rows
        .into_iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, i64>(1).max(0) as usize,
            )
        })
        .collect();

    Ok(OperatorAuditSummary {
        backend: "postgres".into(),
        record_count: summary_row.get::<_, i64>(0).max(0) as usize,
        counts_by_kind,
        distinct_study_count: summary_row.get::<_, i64>(1).max(0) as usize,
        distinct_experiment_count: summary_row.get::<_, i64>(2).max(0) as usize,
        distinct_revision_count: summary_row.get::<_, i64>(3).max(0) as usize,
        distinct_peer_count: summary_row.get::<_, i64>(4).max(0) as usize,
        distinct_head_count: summary_row.get::<_, i64>(5).max(0) as usize,
        earliest_captured_at: summary_row.get(6),
        latest_captured_at: summary_row.get(7),
    })
}

#[derive(Clone, Debug, Default)]
struct PostgresOperatorAuditFilters {
    kind_slug: Option<String>,
    study_id: Option<String>,
    experiment_id: Option<String>,
    revision_id: Option<String>,
    peer_id: Option<String>,
    head_id: Option<String>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    text_pattern: Option<String>,
}

impl PostgresOperatorAuditFilters {
    fn from_query(query: &OperatorAuditQuery) -> Self {
        Self {
            kind_slug: query.kind.as_ref().map(|kind| kind.as_slug().to_owned()),
            study_id: query.study_id.as_ref().map(ToString::to_string),
            experiment_id: query.experiment_id.as_ref().map(ToString::to_string),
            revision_id: query.revision_id.as_ref().map(ToString::to_string),
            peer_id: query.peer_id.as_ref().map(ToString::to_string),
            head_id: query.head_id.as_ref().map(ToString::to_string),
            since: query.since,
            until: query.until,
            text_pattern: query
                .text
                .as_ref()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(|value| format!("%{}%", value.to_ascii_lowercase())),
        }
    }

    fn push_sql_filters<'a>(
        &'a self,
        params: &mut Vec<&'a (dyn ToSql + Sync)>,
        clauses: &mut Vec<String>,
    ) {
        self.push_string_filter("kind", self.kind_slug.as_ref(), params, clauses);
        self.push_string_filter("study_id", self.study_id.as_ref(), params, clauses);
        self.push_string_filter(
            "experiment_id",
            self.experiment_id.as_ref(),
            params,
            clauses,
        );
        self.push_string_filter("revision_id", self.revision_id.as_ref(), params, clauses);
        self.push_string_filter("peer_id", self.peer_id.as_ref(), params, clauses);
        self.push_string_filter("head_id", self.head_id.as_ref(), params, clauses);

        if let Some(since) = self.since.as_ref() {
            params.push(since);
            clauses.push(format!("captured_at >= ${}", params.len()));
        }
        if let Some(until) = self.until.as_ref() {
            params.push(until);
            clauses.push(format!("captured_at <= ${}", params.len()));
        }
        if let Some(text_pattern) = self.text_pattern.as_ref() {
            params.push(text_pattern);
            let record_idx = params.len();
            params.push(text_pattern);
            let summary_idx = params.len();
            clauses.push(format!(
                "(LOWER(record_id) LIKE ${record_idx} OR LOWER(summary_text) LIKE ${summary_idx})"
            ));
        }
    }

    fn push_string_filter<'a>(
        &'a self,
        column: &str,
        value: Option<&'a String>,
        params: &mut Vec<&'a (dyn ToSql + Sync)>,
        clauses: &mut Vec<String>,
    ) {
        if let Some(value) = value {
            params.push(value);
            clauses.push(format!("{column} = ${}", params.len()));
        }
    }
}

fn load_postgres_operator_facet_counts(
    client: &mut postgres::Client,
    table_name: &str,
    snapshot_key: &str,
    query: &OperatorAuditQuery,
    value_expr: &str,
    limit: usize,
) -> anyhow::Result<Vec<OperatorFacetBucket>> {
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    let filters = PostgresOperatorAuditFilters::from_query(query);
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&snapshot_key];
    let mut clauses = vec!["snapshot_key = $1".to_owned()];
    filters.push_sql_filters(&mut params, &mut clauses);

    let normalized_limit = i64::try_from(normalize_operator_facet_limit(limit)).unwrap_or(i64::MAX);
    params.push(&normalized_limit);
    let rows = client.query(
        &format!(
            "SELECT {value_expr} AS value, COUNT(*)::BIGINT AS count \
             FROM {table_name} \
             WHERE {} AND {value_expr} IS NOT NULL AND {value_expr} <> '' \
             GROUP BY value \
             ORDER BY count DESC, value ASC \
             LIMIT ${}",
            clauses.join(" AND "),
            params.len()
        ),
        &params,
    )?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            row.get::<_, Option<String>>("value")
                .map(|value| OperatorFacetBucket {
                    value,
                    count: usize::try_from(row.get::<_, i64>("count")).unwrap_or(usize::MAX),
                })
        })
        .collect())
}

fn load_postgres_operator_audit_facets(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
    query: &OperatorAuditQuery,
    limit: usize,
) -> anyhow::Result<OperatorAuditFacetSummary> {
    let mut client = postgres_connection(url)?;
    let normalized_limit = normalize_operator_facet_limit(limit);
    Ok(OperatorAuditFacetSummary {
        backend: "postgres".into(),
        limit: normalized_limit,
        kinds: load_postgres_operator_facet_counts(
            &mut client,
            table_name,
            snapshot_key,
            query,
            "kind",
            normalized_limit,
        )?,
        studies: load_postgres_operator_facet_counts(
            &mut client,
            table_name,
            snapshot_key,
            query,
            "study_id",
            normalized_limit,
        )?,
        experiments: load_postgres_operator_facet_counts(
            &mut client,
            table_name,
            snapshot_key,
            query,
            "experiment_id",
            normalized_limit,
        )?,
        revisions: load_postgres_operator_facet_counts(
            &mut client,
            table_name,
            snapshot_key,
            query,
            "revision_id",
            normalized_limit,
        )?,
        peers: load_postgres_operator_facet_counts(
            &mut client,
            table_name,
            snapshot_key,
            query,
            "peer_id",
            normalized_limit,
        )?,
        heads: load_postgres_operator_facet_counts(
            &mut client,
            table_name,
            snapshot_key,
            query,
            "head_id",
            normalized_limit,
        )?,
    })
}

fn load_postgres_operator_replay_page(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
    query: &OperatorReplayQuery,
    page: PageRequest,
) -> anyhow::Result<Page<OperatorReplaySnapshotSummary>> {
    let page = page.normalized();
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    ensure_postgres_operator_state_table(&mut client, table_name)?;

    let mut params: Vec<Box<dyn ToSql + Sync>> = vec![Box::new(snapshot_key.to_owned())];
    let mut clauses = vec!["snapshot_key = $1".to_owned()];
    if let Some(study_id) = query.study_id.as_ref() {
        params.push(Box::new(study_id.as_str().to_owned()));
        clauses.push(format!("${} = ANY(study_ids)", params.len()));
    }
    if let Some(experiment_id) = query.experiment_id.as_ref() {
        params.push(Box::new(experiment_id.as_str().to_owned()));
        clauses.push(format!("${} = ANY(experiment_ids)", params.len()));
    }
    if let Some(revision_id) = query.revision_id.as_ref() {
        params.push(Box::new(revision_id.as_str().to_owned()));
        clauses.push(format!("${} = ANY(revision_ids)", params.len()));
    }
    if let Some(head_id) = query.head_id.as_ref() {
        params.push(Box::new(head_id.as_str().to_owned()));
        clauses.push(format!("${} = ANY(head_ids)", params.len()));
    }
    if let Some(since) = query.since.as_ref() {
        params.push(Box::new(*since));
        clauses.push(format!("captured_at >= ${}", params.len()));
    }
    if let Some(until) = query.until.as_ref() {
        params.push(Box::new(*until));
        clauses.push(format!("captured_at <= ${}", params.len()));
    }
    if let Some(text) = query
        .text
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        params.push(Box::new(format!("%{}%", text.to_ascii_lowercase())));
        clauses.push(format!("LOWER(summary_text) LIKE ${}", params.len()));
    }
    let where_sql = clauses.join(" AND ");
    let refs = params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    let count = client
        .query_one(
            &format!("SELECT COUNT(*) FROM {table_name} WHERE {where_sql}"),
            &refs,
        )?
        .get::<_, i64>(0)
        .max(0) as usize;

    let mut page_params = params;
    page_params.push(Box::new(i64::try_from(page.offset).unwrap_or(i64::MAX)));
    let offset_idx = page_params.len();
    page_params.push(Box::new(i64::try_from(page.limit).unwrap_or(i64::MAX)));
    let limit_idx = page_params.len();
    let page_refs = page_params
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    let rows = client.query(
        &format!(
            "SELECT captured_at, study_ids, experiment_ids, revision_ids, head_ids, \
                    receipt_count, head_count, merge_count, lifecycle_plan_count, schedule_epoch_count, peer_window_metric_count, \
                    reducer_cohort_metric_count, head_eval_report_count, summary_json::text \
             FROM {table_name} \
             WHERE {where_sql} \
             ORDER BY captured_at DESC, id DESC \
             OFFSET ${offset_idx} LIMIT ${limit_idx}"
        ),
        &page_refs,
    )?;

    let items = rows
        .into_iter()
        .map(|row| {
            Ok(OperatorReplaySnapshotSummary {
                captured_at: row.get(0),
                study_ids: row
                    .get::<_, Vec<String>>(1)
                    .into_iter()
                    .map(StudyId::new)
                    .collect(),
                experiment_ids: row
                    .get::<_, Vec<String>>(2)
                    .into_iter()
                    .map(ExperimentId::new)
                    .collect(),
                revision_ids: row
                    .get::<_, Vec<String>>(3)
                    .into_iter()
                    .map(RevisionId::new)
                    .collect(),
                head_ids: row
                    .get::<_, Vec<String>>(4)
                    .into_iter()
                    .map(HeadId::new)
                    .collect(),
                receipt_count: row.get::<_, i64>(5).max(0) as usize,
                head_count: row.get::<_, i64>(6).max(0) as usize,
                merge_count: row.get::<_, i64>(7).max(0) as usize,
                lifecycle_plan_count: row.get::<_, i64>(8).max(0) as usize,
                schedule_epoch_count: row.get::<_, i64>(9).max(0) as usize,
                peer_window_metric_count: row.get::<_, i64>(10).max(0) as usize,
                reducer_cohort_metric_count: row.get::<_, i64>(11).max(0) as usize,
                head_eval_report_count: row.get::<_, i64>(12).max(0) as usize,
                summary: serde_json::from_str(&row.get::<_, String>(13))?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(Page::new(items, page, count))
}

fn load_postgres_operator_retention_summary(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
    metrics_retention: MetricsRetentionBudget,
) -> anyhow::Result<OperatorRetentionSummary> {
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    let audit_table_name = postgres_operator_audit_table_name(table_name);
    ensure_postgres_operator_state_table(&mut client, table_name)?;
    ensure_postgres_operator_audit_table(&mut client, &audit_table_name)?;

    let snapshot_row = client.query_one(
        &format!("SELECT COUNT(*), MAX(captured_at) FROM {table_name} WHERE snapshot_key = $1"),
        &[&snapshot_key],
    )?;
    let audit_row = client.query_one(
        &format!("SELECT COUNT(*) FROM {audit_table_name} WHERE snapshot_key = $1"),
        &[&snapshot_key],
    )?;

    Ok(OperatorRetentionSummary {
        backend: "postgres".into(),
        metrics_retention,
        snapshot_retention_limit: operator_snapshot_retention_limit(metrics_retention),
        audit_retention_limit: operator_audit_retention_limit(metrics_retention),
        persisted_snapshot_count: snapshot_row.get::<_, i64>(0).max(0) as usize,
        persisted_audit_record_count: audit_row.get::<_, i64>(0).max(0) as usize,
        latest_snapshot_at: snapshot_row.get(1),
    })
}

fn prune_postgres_operator_retention(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
    metrics_retention: MetricsRetentionBudget,
) -> anyhow::Result<OperatorRetentionPruneResult> {
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    let audit_table_name = postgres_operator_audit_table_name(table_name);
    ensure_postgres_operator_state_table(&mut client, table_name)?;
    ensure_postgres_operator_audit_table(&mut client, &audit_table_name)?;
    let pruned_snapshot_count = prune_postgres_snapshot_rows(
        &mut client,
        table_name,
        snapshot_key,
        operator_snapshot_retention_limit(metrics_retention),
    )?;
    let pruned_audit_record_count = prune_postgres_audit_rows(
        &mut client,
        &audit_table_name,
        snapshot_key,
        operator_audit_retention_limit(metrics_retention),
    )?;
    let summary =
        load_postgres_operator_retention_summary(url, table_name, snapshot_key, metrics_retention)?;
    Ok(OperatorRetentionPruneResult {
        backend: summary.backend,
        pruned_snapshot_count,
        pruned_audit_record_count,
        remaining_snapshot_count: summary.persisted_snapshot_count,
        remaining_audit_record_count: summary.persisted_audit_record_count,
        latest_snapshot_at: summary.latest_snapshot_at,
    })
}

fn load_postgres_operator_state_snapshot(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
) -> anyhow::Result<Option<OperatorStateSnapshot>> {
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    ensure_postgres_operator_state_table(&mut client, table_name)?;
    let row = client.query_opt(
        &format!(
            "SELECT captured_at, preview_json::text FROM {table_name} WHERE snapshot_key = $1 ORDER BY captured_at DESC, id DESC LIMIT 1"
        ),
        &[&snapshot_key],
    )?;
    row.map(|row| {
        let captured_at = row.get::<_, DateTime<Utc>>(0);
        let preview_json = row.get::<_, String>(1);
        let preview = serde_json::from_str::<FileOperatorStorePreview>(&preview_json)?;
        Ok(OperatorStateSnapshot {
            captured_at,
            preview,
        })
    })
    .transpose()
}

fn load_postgres_operator_state_rows(
    url: &str,
    table_name: &str,
    snapshot_key: &str,
) -> anyhow::Result<Vec<PostgresOperatorStateRow>> {
    let mut client = postgres_connection(url)?;
    let table_name = ensure_valid_postgres_identifier(table_name)?;
    ensure_postgres_operator_state_table(&mut client, table_name)?;
    let rows = client.query(
        &format!(
            "SELECT captured_at, preview_json::text FROM {table_name} WHERE snapshot_key = $1 ORDER BY captured_at DESC, id DESC"
        ),
        &[&snapshot_key],
    )?;
    rows.into_iter()
        .map(|row| {
            let captured_at = row.get::<_, DateTime<Utc>>(0);
            let preview_json = row.get::<_, String>(1);
            let preview = serde_json::from_str::<FileOperatorStorePreview>(&preview_json)?;
            Ok(PostgresOperatorStateRow {
                captured_at,
                preview,
            })
        })
        .collect()
}

pub(crate) fn persist_operator_state_snapshot(
    backend: Option<&OperatorStateBackendConfig>,
    metrics_retention: MetricsRetentionBudget,
    preview: &FileOperatorStorePreview,
) -> anyhow::Result<()> {
    let Some(backend) = backend else {
        return Ok(());
    };

    match backend {
        OperatorStateBackendConfig::Redis { url, snapshot_key } => {
            let mut connection = FileOperatorStore::redis_connection(url)?;
            let snapshot = OperatorStateSnapshot {
                captured_at: Utc::now(),
                preview: preview.clone(),
            };
            let payload = serde_json::to_string(&snapshot)?;
            connection.set::<_, _, ()>(snapshot_key, payload)?;
            Ok(())
        }
        OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        } => {
            let snapshot = OperatorStateSnapshot {
                captured_at: Utc::now(),
                preview: preview.clone(),
            };
            let mut client = postgres_connection(url)?;
            let table_name = ensure_valid_postgres_identifier(table_name)?;
            let audit_table_name = postgres_operator_audit_table_name(table_name);
            ensure_postgres_operator_state_table(&mut client, table_name)?;
            ensure_postgres_operator_audit_table(&mut client, &audit_table_name)?;
            let preview_json = serde_json::to_string(&snapshot.preview)?;
            let replay_summary =
                build_replay_snapshot_summary(&snapshot.preview, snapshot.captured_at);
            let summary_json = serde_json::to_string(&replay_summary.summary)?;
            let summary_text = replay_snapshot_summary_text(&replay_summary);
            let study_ids = replay_summary
                .study_ids
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>();
            let experiment_ids = replay_summary
                .experiment_ids
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>();
            let revision_ids = replay_summary
                .revision_ids
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>();
            let head_ids = replay_summary
                .head_ids
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect::<Vec<_>>();
            client.execute(
                &format!(
                    "INSERT INTO {table_name} \
                     (snapshot_key, captured_at, preview_json, summary_json, summary_text, study_ids, experiment_ids, revision_ids, head_ids, receipt_count, head_count, merge_count, lifecycle_plan_count, schedule_epoch_count, peer_window_metric_count, reducer_cohort_metric_count, head_eval_report_count) \
                     VALUES ($1, $2, CAST($3 AS text)::jsonb, CAST($4 AS text)::jsonb, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)"
                ),
                &[
                    &snapshot_key,
                    &snapshot.captured_at,
                    &preview_json,
                    &summary_json,
                    &summary_text,
                    &study_ids,
                    &experiment_ids,
                    &revision_ids,
                    &head_ids,
                    &i64::try_from(replay_summary.receipt_count).unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.head_count).unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.merge_count).unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.lifecycle_plan_count).unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.schedule_epoch_count).unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.peer_window_metric_count).unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.reducer_cohort_metric_count)
                        .unwrap_or(i64::MAX),
                    &i64::try_from(replay_summary.head_eval_report_count).unwrap_or(i64::MAX),
                ],
            )?;
            let audit_records =
                build_audit_records_from_preview(&snapshot.preview, Some(snapshot.captured_at));
            insert_postgres_operator_audit_records(
                &mut client,
                &audit_table_name,
                snapshot_key,
                &audit_records,
            )?;
            let _ = prune_postgres_snapshot_rows(
                &mut client,
                table_name,
                snapshot_key,
                operator_snapshot_retention_limit(metrics_retention),
            )?;
            let _ = prune_postgres_audit_rows(
                &mut client,
                &audit_table_name,
                snapshot_key,
                operator_audit_retention_limit(metrics_retention),
            )?;
            Ok(())
        }
    }
}

impl OperatorStore for FileOperatorStore {
    fn receipts(&self, query: &ReceiptQuery) -> anyhow::Result<Vec<ContributionReceipt>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(preview
                .receipts
                .into_iter()
                .filter(|receipt| query.matches(receipt))
                .collect());
        }

        Ok(self
            .storage_config()
            .map(|storage| history::load_all_contribution_receipts(&storage))
            .transpose()?
            .unwrap_or_else(|| self.receipt_preview.clone())
            .into_iter()
            .filter(|receipt| query.matches(receipt))
            .collect())
    }

    fn receipts_page(
        &self,
        query: &ReceiptQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<ContributionReceipt>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(page_from_filtered(preview.receipts, page, |receipt| {
                query.matches(receipt)
            }));
        }
        if let Some(storage) = self.storage_config() {
            return history::load_paged_contribution_receipts(&storage, page, |receipt| {
                query.matches(receipt)
            });
        }
        Ok(page_from_filtered(
            self.receipt_preview.iter().cloned(),
            page,
            |receipt| query.matches(receipt),
        ))
    }

    fn heads(&self, query: &HeadQuery) -> anyhow::Result<Vec<HeadDescriptor>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(preview
                .heads
                .into_iter()
                .filter(|head| query.matches(head))
                .collect());
        }

        Ok(self
            .storage_config()
            .map(|storage| history::load_all_head_descriptors(&storage))
            .transpose()?
            .unwrap_or_else(|| self.head_preview.clone())
            .into_iter()
            .filter(|head| query.matches(head))
            .collect())
    }

    fn heads_page(
        &self,
        query: &HeadQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<HeadDescriptor>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(page_from_filtered(preview.heads, page, |head| {
                query.matches(head)
            }));
        }
        if let Some(storage) = self.storage_config() {
            return history::load_paged_head_descriptors(&storage, page, |head| {
                query.matches(head)
            });
        }
        Ok(page_from_filtered(
            self.head_preview.iter().cloned(),
            page,
            |head| query.matches(head),
        ))
    }

    fn merges(&self) -> anyhow::Result<Vec<MergeCertificate>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(preview.merges);
        }

        self.storage_config()
            .map(|storage| history::load_all_merge_certificates(&storage))
            .transpose()
            .map(|loaded| loaded.unwrap_or_else(|| self.merge_preview.clone()))
    }

    fn merges_page(&self, page: PageRequest) -> anyhow::Result<Page<MergeCertificate>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(page_from_filtered(preview.merges, page, |_| true));
        }
        if let Some(storage) = self.storage_config() {
            return history::load_paged_merge_certificates(&storage, page, |_| true);
        }
        Ok(page_from_filtered(
            self.merge_preview.iter().cloned(),
            page,
            |_| true,
        ))
    }

    fn audit_page(
        &self,
        query: &OperatorAuditQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorAuditRecord>> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            return load_postgres_operator_audit_page(
                url,
                &postgres_operator_audit_table_name(table_name),
                snapshot_key,
                query,
                page,
            );
        }
        let records = self.build_audit_records()?;
        Ok(page_from_filtered(records, page, |record| {
            query.matches(record)
        }))
    }

    fn audit_summary(&self, query: &OperatorAuditQuery) -> anyhow::Result<OperatorAuditSummary> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            return load_postgres_operator_audit_summary(
                url,
                &postgres_operator_audit_table_name(table_name),
                snapshot_key,
                query,
            );
        }
        let records = self
            .build_audit_records()?
            .into_iter()
            .filter(|record| query.matches(record))
            .collect::<Vec<_>>();
        Ok(build_operator_audit_summary(
            operator_backend_label(self.operator_state_backend.as_ref()),
            records,
        ))
    }

    fn audit_facets(
        &self,
        query: &OperatorAuditQuery,
        limit: usize,
    ) -> anyhow::Result<OperatorAuditFacetSummary> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            return load_postgres_operator_audit_facets(
                url,
                &postgres_operator_audit_table_name(table_name),
                snapshot_key,
                query,
                limit,
            );
        }
        let records = self
            .build_audit_records()?
            .into_iter()
            .filter(|record| query.matches(record))
            .collect::<Vec<_>>();
        Ok(build_operator_audit_facets(
            operator_backend_label(self.operator_state_backend.as_ref()),
            records,
            limit,
        ))
    }

    fn replay_snapshot(
        &self,
        captured_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<Option<OperatorReplaySnapshot>> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            let rows = load_postgres_operator_state_rows(url, table_name, snapshot_key)?;
            let selected = match captured_at {
                Some(at) => rows.into_iter().find(|row| row.captured_at <= at),
                None => rows.into_iter().next(),
            };
            return Ok(
                selected.map(|row| replay_snapshot_from_preview(row.captured_at, row.preview))
            );
        }

        if let Some(snapshot) = self.remote_snapshot()? {
            if captured_at.is_none_or(|at| snapshot.captured_at <= at) {
                return Ok(Some(replay_snapshot_from_preview(
                    snapshot.captured_at,
                    snapshot.preview,
                )));
            }
            return Ok(None);
        }

        Ok(Some(replay_snapshot_from_preview(
            Utc::now(),
            FileOperatorStorePreview {
                receipts: self.receipt_preview.clone(),
                heads: self.head_preview.clone(),
                merges: self.merge_preview.clone(),
                lifecycle_announcements: self.lifecycle_preview.clone(),
                schedule_announcements: self.schedule_preview.clone(),
                peer_window_metrics: self.peer_window_preview.clone(),
                reducer_cohort_metrics: self.reducer_cohort_preview.clone(),
                head_eval_reports: self.head_eval_preview.clone(),
                eval_protocol_manifests: self.eval_protocol_preview.clone(),
            },
        )))
    }

    fn replay_page(
        &self,
        query: &OperatorReplayQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorReplaySnapshotSummary>> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            return load_postgres_operator_replay_page(url, table_name, snapshot_key, query, page);
        }

        let page = page.normalized();
        let summaries = if let Some(snapshot) = self.remote_snapshot()? {
            vec![build_replay_snapshot_summary(
                &snapshot.preview,
                snapshot.captured_at,
            )]
        } else {
            vec![build_replay_snapshot_summary(
                &FileOperatorStorePreview {
                    receipts: self.receipt_preview.clone(),
                    heads: self.head_preview.clone(),
                    merges: self.merge_preview.clone(),
                    lifecycle_announcements: self.lifecycle_preview.clone(),
                    schedule_announcements: self.schedule_preview.clone(),
                    peer_window_metrics: self.peer_window_preview.clone(),
                    reducer_cohort_metrics: self.reducer_cohort_preview.clone(),
                    head_eval_reports: self.head_eval_preview.clone(),
                    eval_protocol_manifests: self.eval_protocol_preview.clone(),
                },
                Utc::now(),
            )]
        };
        Ok(page_from_filtered(summaries, page, |summary| {
            query.matches(summary)
        }))
    }

    fn control_replay_page(
        &self,
        query: &OperatorControlReplayQuery,
        page: PageRequest,
    ) -> anyhow::Result<Page<OperatorControlReplayRecord>> {
        let records = self.build_control_replay_records()?;
        Ok(page_from_filtered(records, page, |record| {
            query.matches(record)
        }))
    }

    fn control_replay_summary(
        &self,
        query: &OperatorControlReplayQuery,
    ) -> anyhow::Result<OperatorControlReplaySummary> {
        let records = self
            .build_control_replay_records()?
            .into_iter()
            .filter(|record| query.matches(record))
            .collect::<Vec<_>>();
        Ok(build_operator_control_replay_summary(
            operator_backend_label(self.operator_state_backend.as_ref()),
            records,
        ))
    }

    fn retention_summary(&self) -> anyhow::Result<OperatorRetentionSummary> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            return load_postgres_operator_retention_summary(
                url,
                table_name,
                snapshot_key,
                self.metrics_retention,
            );
        }

        let latest_snapshot_at = self.remote_snapshot()?.map(|snapshot| snapshot.captured_at);
        Ok(OperatorRetentionSummary {
            backend: operator_backend_label(self.operator_state_backend.as_ref()),
            metrics_retention: self.metrics_retention,
            snapshot_retention_limit: operator_snapshot_retention_limit(self.metrics_retention),
            audit_retention_limit: operator_audit_retention_limit(self.metrics_retention),
            persisted_snapshot_count: usize::from(latest_snapshot_at.is_some()),
            persisted_audit_record_count: self.build_audit_records()?.len(),
            latest_snapshot_at,
        })
    }

    fn prune_retention(&self) -> anyhow::Result<OperatorRetentionPruneResult> {
        if let Some(OperatorStateBackendConfig::Postgres {
            url,
            table_name,
            snapshot_key,
        }) = self.operator_state_backend.as_ref()
        {
            return prune_postgres_operator_retention(
                url,
                table_name,
                snapshot_key,
                self.metrics_retention,
            );
        }
        let summary = self.retention_summary()?;
        Ok(OperatorRetentionPruneResult {
            backend: summary.backend,
            pruned_snapshot_count: 0,
            pruned_audit_record_count: 0,
            remaining_snapshot_count: summary.persisted_snapshot_count,
            remaining_audit_record_count: summary.persisted_audit_record_count,
            latest_snapshot_at: summary.latest_snapshot_at,
        })
    }

    fn head_eval_reports(&self, head_id: &HeadId) -> anyhow::Result<Vec<HeadEvalReport>> {
        Ok(self
            .all_head_eval_reports()?
            .into_iter()
            .filter(|report| &report.head_id == head_id)
            .collect())
    }

    #[cfg(feature = "artifact-publish")]
    fn eval_protocol_manifests(&self) -> anyhow::Result<Vec<StoredEvalProtocolManifestRecord>> {
        if let Some(preview) = self.remote_preview()? {
            return Ok(preview.eval_protocol_manifests);
        }

        self.storage_config()
            .map(|storage| history::load_all_eval_protocol_manifests(&storage))
            .transpose()
            .map(|loaded| loaded.unwrap_or_else(|| self.eval_protocol_preview.clone()))
    }

    fn leaderboard_snapshot(
        &self,
        network_id: &NetworkId,
        peer_principals: &BTreeMap<PeerId, PrincipalId>,
        captured_at: DateTime<Utc>,
    ) -> anyhow::Result<BrowserLeaderboardSnapshot> {
        Ok(crate::social_services::leaderboard_snapshot(
            network_id,
            &self.receipts(&ReceiptQuery::default())?,
            &self.merges()?,
            peer_principals,
            captured_at,
        ))
    }

    #[cfg(feature = "artifact-publish")]
    fn artifact_store(&self) -> anyhow::Result<Option<FsArtifactStore>> {
        self.artifact_store_root
            .as_ref()
            .map(|root| {
                let store = FsArtifactStore::new(root.clone());
                store.ensure_layout().map_err(anyhow::Error::from)?;
                Ok(store)
            })
            .transpose()
    }

    #[cfg(feature = "artifact-publish")]
    fn publication_store(&self) -> anyhow::Result<Option<PublicationStore>> {
        use burn_p2p_core::EvalProtocolManifest;

        self.publication_store_root
            .as_ref()
            .map(|root| {
                let mut store = PublicationStore::open(root)?;
                store.configure_targets(self.publication_targets.clone())?;
                let heads = self.heads(&HeadQuery::default())?;
                let head_eval_reports = self.all_head_eval_reports()?;
                let protocols = self
                    .eval_protocol_manifests()?
                    .into_iter()
                    .map(|record| record.manifest)
                    .collect::<Vec<EvalProtocolManifest>>();
                if let Some(artifact_store) = self.artifact_store()? {
                    store.sync_aliases_and_eager_publications(
                        &artifact_store,
                        &heads,
                        &head_eval_reports,
                        &protocols,
                    )?;
                } else {
                    store.sync_aliases(&heads, &head_eval_reports, &protocols)?;
                }
                Ok(store)
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests;
