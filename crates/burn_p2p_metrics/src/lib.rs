//! Optional metrics indexer and read-model helpers for `burn_p2p`.
//!
//! This crate is intentionally non-consensus-critical. It ingests peer-window,
//! reducer-cohort, and head-evaluation records and materializes derived views,
//! snapshot manifests, and append-only ledger segments for dashboards and peer
//! catchup flows.
#![forbid(unsafe_code)]

mod derive;
mod query;
mod retention;
mod store;

use burn_p2p_core::{
    CanaryEvalReport, CohortRobustnessReport, ContentId, DatasetViewId, ExperimentId,
    HeadEvalReport, HeadId, LeaseId, MetricScope, MetricTrustClass, MetricsLedgerSegment,
    MetricsSnapshotManifest, NetworkId, PeerId, PeerWindowMetrics, ReducerCohortMetrics,
    RevisionId, TrustScore, WindowId, WorkloadId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub use derive::MetricsIndexer;
pub use query::{
    PeerWindowDistributionDetail, PeerWindowDistributionSummary,
    derive_peer_window_distribution_detail, derive_peer_window_distribution_detail_with_limit,
    derive_peer_window_distribution_summaries,
};
pub use retention::MetricsIndexerConfig;
pub use store::{MetricsStore, MetricsStoreError, build_catchup_bundles, build_live_event};

use crate::retention::RetentionKind;

/// Appends one raw metric record to the metrics ledger.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MetricEnvelope {
    /// Peer-window metric material.
    PeerWindow(PeerWindowMetrics),
    /// Reducer cohort metric material.
    ReducerCohort(ReducerCohortMetrics),
    /// Canonical or candidate head evaluation material.
    HeadEval(HeadEvalReport),
}

impl MetricEnvelope {
    fn network_id(&self) -> &NetworkId {
        match self {
            Self::PeerWindow(metrics) => &metrics.network_id,
            Self::ReducerCohort(metrics) => &metrics.network_id,
            Self::HeadEval(report) => &report.network_id,
        }
    }

    fn experiment_id(&self) -> &ExperimentId {
        match self {
            Self::PeerWindow(metrics) => &metrics.experiment_id,
            Self::ReducerCohort(metrics) => &metrics.experiment_id,
            Self::HeadEval(report) => &report.experiment_id,
        }
    }

    fn revision_id(&self) -> &RevisionId {
        match self {
            Self::PeerWindow(metrics) => &metrics.revision_id,
            Self::ReducerCohort(metrics) => &metrics.revision_id,
            Self::HeadEval(report) => &report.revision_id,
        }
    }

    fn base_head_id(&self) -> Option<&HeadId> {
        match self {
            Self::PeerWindow(metrics) => Some(&metrics.base_head_id),
            Self::ReducerCohort(metrics) => Some(&metrics.base_head_id),
            Self::HeadEval(report) => report.base_head_id.as_ref(),
        }
    }

    fn merge_window_id(&self) -> Option<&ContentId> {
        match self {
            Self::ReducerCohort(metrics) => Some(&metrics.merge_window_id),
            Self::PeerWindow(_) | Self::HeadEval(_) => None,
        }
    }

    pub(crate) fn revision_key(&self) -> (ExperimentId, RevisionId) {
        (self.experiment_id().clone(), self.revision_id().clone())
    }

    pub(crate) fn retention_kind(&self) -> RetentionKind {
        match self {
            Self::PeerWindow(_) => RetentionKind::PeerWindow,
            Self::ReducerCohort(_) => RetentionKind::ReducerCohort,
            Self::HeadEval(_) => RetentionKind::HeadEval,
        }
    }

    pub(crate) fn captured_at(&self) -> DateTime<Utc> {
        match self {
            Self::PeerWindow(metrics) => metrics.window_finished_at,
            Self::ReducerCohort(metrics) => metrics.captured_at,
            Self::HeadEval(report) => report.finished_at,
        }
    }
}

/// Enumerates the derived metrics produced by the read-model service.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DerivedMetricKind {
    /// Accepted tokens or samples per second.
    AcceptedTokensPerSec,
    /// Accepted updates per hour.
    AcceptedUpdatesPerHour,
    /// Approximate epoch progress over accepted work.
    EffectiveEpochProgress,
    /// Approximate unique-sample coverage across accepted work.
    UniqueSampleCoverageEstimate,
    /// Time between canonical head evaluations.
    CanonicalHeadCadence,
    /// Fraction of accepted work that exceeded the stale-work threshold.
    StaleWorkFraction,
    /// Mean head lag across peer-window metrics.
    MeanHeadLag,
    /// Maximum head lag across peer-window metrics.
    MaxHeadLag,
    /// Mean agreement across replicated reducer cohorts.
    ReducerReplicaAgreement,
    /// Merge-window close skew.
    MergeWindowSkew,
    /// Number of distinct candidate branches alive for one base head.
    CandidateBranchFactor,
    /// P50 head adoption lag.
    HeadAdoptionLagP50,
    /// P90 head adoption lag.
    HeadAdoptionLagP90,
    /// Acceptance ratio across attempted and accepted work.
    AcceptanceRatio,
    /// Rejection ratio grouped by reason.
    RejectionRatioByReason,
    /// Bytes transferred per accepted token or sample.
    BytesPerAcceptedToken,
    /// Validation reports per hour.
    ValidationServiceRate,
}

/// One derived metric point materialized by the metrics indexer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DerivedMetricPoint {
    /// Derived metric family.
    pub metric: DerivedMetricKind,
    /// Scope used to interpret the metric.
    pub scope: MetricScope,
    /// Trust class attached to the metric.
    pub trust: MetricTrustClass,
    /// Network identifier for the metric point.
    pub network_id: NetworkId,
    /// Experiment identifier for the metric point.
    pub experiment_id: ExperimentId,
    /// Revision identifier for the metric point.
    pub revision_id: RevisionId,
    /// Workload identifier for the metric point.
    pub workload_id: WorkloadId,
    /// Dataset view identifier for the metric point.
    pub dataset_view_id: DatasetViewId,
    /// Canonical head identifier, when present.
    pub canonical_head_id: Option<HeadId>,
    /// Base head identifier, when present.
    pub base_head_id: Option<HeadId>,
    /// Candidate head identifier, when present.
    pub candidate_head_id: Option<HeadId>,
    /// Merge-window identifier, when present.
    pub merge_window_id: Option<ContentId>,
    /// Reducer-group identifier, when present.
    pub reducer_group_id: Option<ContentId>,
    /// Peer identifier, when present.
    pub peer_id: Option<PeerId>,
    /// Lease identifier, when present.
    pub lease_id: Option<LeaseId>,
    /// Optional label or dimension for the point.
    pub series_label: Option<String>,
    /// Metric value.
    pub value: f64,
    /// Timestamp attached to the materialized point.
    pub captured_at: DateTime<Utc>,
}

/// Materialized snapshot exported by the metrics indexer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Snapshot manifest for the export.
    pub manifest: MetricsSnapshotManifest,
    /// Canonical and candidate head reports included by the snapshot.
    pub head_eval_reports: Vec<HeadEvalReport>,
    /// Peer-window metrics included by the snapshot.
    pub peer_window_metrics: Vec<PeerWindowMetrics>,
    /// Reducer cohort metrics included by the snapshot.
    pub reducer_cohort_metrics: Vec<ReducerCohortMetrics>,
    /// Derived metric rollups included by the snapshot.
    pub derived_metrics: Vec<DerivedMetricPoint>,
}

/// Catchup bundle that seeds a peer or dashboard from one point-in-time metrics snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricsCatchupBundle {
    /// Network identifier covered by the bundle.
    pub network_id: NetworkId,
    /// Experiment identifier covered by the bundle.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the bundle.
    pub revision_id: RevisionId,
    /// Latest snapshot material for the revision.
    pub snapshot: MetricsSnapshot,
    /// Ledger segments after or alongside the snapshot.
    pub ledger_segments: Vec<MetricsLedgerSegment>,
    /// Time when the bundle was generated.
    pub generated_at: DateTime<Utc>,
}

/// Robustness rollup materialized from cohort screening and trust updates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RobustnessRollup {
    /// Network identifier covered by the rollup.
    pub network_id: NetworkId,
    /// Experiment identifier covered by the rollup.
    pub experiment_id: ExperimentId,
    /// Revision identifier covered by the rollup.
    pub revision_id: RevisionId,
    /// Merge window covered by the rollup.
    pub window_id: WindowId,
    /// Total updates screened in the cohort.
    pub total_updates: u32,
    /// Total rejected updates in the cohort.
    pub rejected_updates: u32,
    /// Fraction of rejected updates in the cohort.
    pub rejection_ratio: f64,
    /// Mean trust score across reported peers.
    pub mean_trust_score: f64,
    /// Number of quarantined peers in the trust snapshot.
    pub quarantined_peer_count: u32,
    /// Number of quarantined peers escalated to a ban recommendation.
    pub ban_recommended_peer_count: u32,
    /// Number of canary regressions in the supplied reports.
    pub canary_regression_count: u32,
    /// Number of active robustness alerts in the current cohort report.
    pub alert_count: u32,
    /// Rollup capture time.
    pub captured_at: DateTime<Utc>,
}

/// Derives one robustness rollup from screening, trust, and canary state.
pub fn derive_robustness_rollup(
    network_id: NetworkId,
    cohort: &CohortRobustnessReport,
    trust_scores: &[TrustScore],
    canary_reports: &[CanaryEvalReport],
    captured_at: DateTime<Utc>,
) -> RobustnessRollup {
    let mean_trust_score = if trust_scores.is_empty() {
        0.0
    } else {
        trust_scores.iter().map(|score| score.score).sum::<f64>() / trust_scores.len() as f64
    };
    let quarantined_peer_count = trust_scores
        .iter()
        .filter(|score| score.quarantined)
        .count() as u32;
    let ban_recommended_peer_count = trust_scores
        .iter()
        .filter(|score| score.quarantined && score.ban_recommended)
        .count() as u32;
    let canary_regression_count = canary_reports
        .iter()
        .filter(|report| !report.accepted || report.detected_backdoor_trigger)
        .count() as u32;
    let alert_count = cohort.alerts.len() as u32;

    RobustnessRollup {
        network_id,
        experiment_id: cohort.experiment_id.clone(),
        revision_id: cohort.revision_id.clone(),
        window_id: cohort.window_id,
        total_updates: cohort.total_updates,
        rejected_updates: cohort.rejected_updates,
        rejection_ratio: if cohort.total_updates == 0 {
            0.0
        } else {
            cohort.rejected_updates as f64 / cohort.total_updates as f64
        },
        mean_trust_score,
        quarantined_peer_count,
        ban_recommended_peer_count,
        canary_regression_count,
        alert_count,
        captured_at,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_core::{
        AggregationStrategy, BackendClass, CanaryEvalReport, CohortFilterStrategy,
        CohortRobustnessReport, ContentId, DatasetViewId, ExperimentId, HeadEvalReport, HeadId,
        LeaseId, MetricValue, MetricsLiveEventKind, NetworkId, PeerRole, PeerWindowMetrics,
        PeerWindowStatus, ReducerCohortMetrics, ReducerCohortStatus, RevisionId, RobustnessAlert,
        TrustScore, WindowId, WorkloadId,
    };
    use chrono::{Duration, Utc};
    use tempfile::tempdir;

    use super::{
        DerivedMetricKind, MetricEnvelope, MetricsIndexer, MetricsIndexerConfig, MetricsStore,
        derive_peer_window_distribution_detail, derive_peer_window_distribution_detail_with_limit,
        derive_peer_window_distribution_summaries, derive_robustness_rollup,
    };

    #[test]
    fn metrics_indexer_derives_rollups_and_exports_snapshot_and_segments() {
        let mut indexer = MetricsIndexer::new(MetricsIndexerConfig {
            stale_head_lag_threshold_steps: 2,
            ledger_segment_entry_limit: 2,
            ..MetricsIndexerConfig::default()
        });
        let started_at = Utc::now();
        indexer.ingest_peer_window_metrics(PeerWindowMetrics {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            dataset_view_id: DatasetViewId::new("view-a"),
            peer_id: burn_p2p_core::PeerId::new("peer-a"),
            principal_id: None,
            lease_id: LeaseId::new("lease-a"),
            base_head_id: HeadId::new("head-a"),
            window_started_at: started_at,
            window_finished_at: started_at + Duration::seconds(10),
            attempted_tokens_or_samples: 200,
            accepted_tokens_or_samples: Some(180),
            local_train_loss_mean: Some(0.4),
            local_train_loss_last: Some(0.3),
            grad_or_delta_norm: Some(1.1),
            optimizer_step_count: 10,
            compute_time_ms: 8_000,
            data_fetch_time_ms: 1_000,
            publish_latency_ms: 100,
            head_lag_at_start: 1,
            head_lag_at_finish: 1,
            backend_class: BackendClass::BrowserWgpu,
            role: PeerRole::BrowserTrainerWgpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        });
        indexer.ingest_peer_window_metrics(PeerWindowMetrics {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            dataset_view_id: DatasetViewId::new("view-a"),
            peer_id: burn_p2p_core::PeerId::new("peer-b"),
            principal_id: None,
            lease_id: LeaseId::new("lease-b"),
            base_head_id: HeadId::new("head-a"),
            window_started_at: started_at + Duration::seconds(1),
            window_finished_at: started_at + Duration::seconds(11),
            attempted_tokens_or_samples: 300,
            accepted_tokens_or_samples: Some(250),
            local_train_loss_mean: Some(0.5),
            local_train_loss_last: Some(0.45),
            grad_or_delta_norm: Some(1.4),
            optimizer_step_count: 12,
            compute_time_ms: 9_000,
            data_fetch_time_ms: 900,
            publish_latency_ms: 110,
            head_lag_at_start: 2,
            head_lag_at_finish: 4,
            backend_class: BackendClass::Cpu,
            role: PeerRole::TrainerCpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        });
        indexer.ingest_reducer_cohort_metrics(ReducerCohortMetrics {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            dataset_view_id: DatasetViewId::new("view-a"),
            merge_window_id: burn_p2p_core::ContentId::new("window-a"),
            reducer_group_id: burn_p2p_core::ContentId::new("reducers-a"),
            captured_at: started_at + Duration::seconds(12),
            base_head_id: HeadId::new("head-a"),
            candidate_head_id: Some(HeadId::new("head-b")),
            received_updates: 4,
            accepted_updates: 3,
            rejected_updates: 1,
            sum_weight: 2.5,
            accepted_tokens_or_samples: 430,
            staleness_mean: 1.5,
            staleness_max: 4.0,
            window_close_delay_ms: 250,
            cohort_duration_ms: 5_000,
            aggregate_norm: 1.2,
            reducer_load: 0.7,
            ingress_bytes: 3_072,
            egress_bytes: 1_024,
            replica_agreement: Some(0.95),
            late_arrival_count: Some(1),
            missing_peer_count: Some(0),
            rejection_reasons: BTreeMap::from([("stale_base_head".into(), 1)]),
            status: ReducerCohortStatus::Closed,
        });
        indexer.ingest_head_eval_report(burn_p2p_core::HeadEvalReport {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            head_id: HeadId::new("head-b"),
            base_head_id: Some(HeadId::new("head-a")),
            eval_protocol_id: burn_p2p_core::ContentId::new("eval-a"),
            evaluator_set_id: burn_p2p_core::ContentId::new("eval-set-a"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.25))]),
            sample_count: 512,
            dataset_view_id: DatasetViewId::new("view-a"),
            started_at: started_at + Duration::seconds(12),
            finished_at: started_at + Duration::seconds(14),
            trust_class: burn_p2p_core::MetricTrustClass::Canonical,
            status: burn_p2p_core::HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        });

        let derived =
            indexer.derive_metrics(&ExperimentId::new("exp-a"), &RevisionId::new("rev-a"));
        assert!(
            derived
                .iter()
                .any(|point| point.metric == DerivedMetricKind::AcceptedTokensPerSec)
        );
        assert!(
            derived
                .iter()
                .any(|point| point.metric == DerivedMetricKind::MeanHeadLag)
        );
        assert!(
            derived
                .iter()
                .any(|point| point.metric == DerivedMetricKind::ReducerReplicaAgreement)
        );
        assert!(
            derived
                .iter()
                .any(|point| point.metric == DerivedMetricKind::BytesPerAcceptedToken)
        );
        assert!(
            derived
                .iter()
                .any(|point| point.metric == DerivedMetricKind::RejectionRatioByReason)
        );

        let snapshot = indexer
            .export_snapshot(
                &ExperimentId::new("exp-a"),
                &RevisionId::new("rev-a"),
                3,
                None,
                Vec::new(),
            )
            .expect("snapshot");
        assert_eq!(snapshot.manifest.snapshot_seq, 3);
        assert_eq!(
            snapshot.manifest.covers_until_head_id,
            Some(HeadId::new("head-b"))
        );
        assert_eq!(snapshot.peer_window_metrics.len(), 2);

        let segments = indexer
            .export_ledger_segments(&ExperimentId::new("exp-a"), &RevisionId::new("rev-a"))
            .expect("segments");
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].segment_seq, 0);
        assert_eq!(segments[1].prev_hash, Some(segments[0].hash.clone()));
    }

    #[test]
    fn peer_window_distributions_group_quantiles_by_base_head() {
        let started_at = Utc::now();
        let summaries = derive_peer_window_distribution_summaries(&[
            PeerWindowMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                peer_id: burn_p2p_core::PeerId::new("peer-a"),
                principal_id: None,
                lease_id: LeaseId::new("lease-a"),
                base_head_id: HeadId::new("head-a"),
                window_started_at: started_at,
                window_finished_at: started_at + Duration::seconds(10),
                attempted_tokens_or_samples: 100,
                accepted_tokens_or_samples: Some(80),
                local_train_loss_mean: Some(0.6),
                local_train_loss_last: Some(0.55),
                grad_or_delta_norm: Some(1.0),
                optimizer_step_count: 10,
                compute_time_ms: 1_000,
                data_fetch_time_ms: 100,
                publish_latency_ms: 10,
                head_lag_at_start: 0,
                head_lag_at_finish: 1,
                backend_class: BackendClass::Cpu,
                role: PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            },
            PeerWindowMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                peer_id: burn_p2p_core::PeerId::new("peer-b"),
                principal_id: None,
                lease_id: LeaseId::new("lease-b"),
                base_head_id: HeadId::new("head-a"),
                window_started_at: started_at + Duration::seconds(1),
                window_finished_at: started_at + Duration::seconds(11),
                attempted_tokens_or_samples: 120,
                accepted_tokens_or_samples: Some(0),
                local_train_loss_mean: Some(0.9),
                local_train_loss_last: Some(0.8),
                grad_or_delta_norm: Some(1.2),
                optimizer_step_count: 11,
                compute_time_ms: 1_400,
                data_fetch_time_ms: 300,
                publish_latency_ms: 30,
                head_lag_at_start: 2,
                head_lag_at_finish: 5,
                backend_class: BackendClass::BrowserWgpu,
                role: PeerRole::BrowserVerifier,
                status: PeerWindowStatus::Rejected,
                status_reason: Some("lag".into()),
            },
            PeerWindowMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                peer_id: burn_p2p_core::PeerId::new("peer-c"),
                principal_id: None,
                lease_id: LeaseId::new("lease-c"),
                base_head_id: HeadId::new("head-b"),
                window_started_at: started_at,
                window_finished_at: started_at + Duration::seconds(12),
                attempted_tokens_or_samples: 90,
                accepted_tokens_or_samples: Some(70),
                local_train_loss_mean: Some(0.4),
                local_train_loss_last: Some(0.35),
                grad_or_delta_norm: Some(0.9),
                optimizer_step_count: 8,
                compute_time_ms: 900,
                data_fetch_time_ms: 90,
                publish_latency_ms: 9,
                head_lag_at_start: 0,
                head_lag_at_finish: 0,
                backend_class: BackendClass::Cpu,
                role: PeerRole::Evaluator,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            },
        ]);

        assert_eq!(summaries.len(), 2);
        let head_a = summaries
            .iter()
            .find(|summary| summary.base_head_id.as_str() == "head-a")
            .expect("head-a summary");
        assert_eq!(head_a.sample_count, 2);
        assert_eq!(head_a.completed_windows, 1);
        assert_eq!(head_a.rejected_windows, 1);
        assert_eq!(head_a.accepted_windows, 1);
        assert_eq!(head_a.attempted_tokens_or_samples, 220);
        assert_eq!(head_a.accepted_tokens_or_samples, 80);
        assert!(head_a.acceptance_ratio > 0.36 && head_a.acceptance_ratio < 0.37);
        assert_eq!(head_a.local_train_loss_mean_p10, Some(0.6));
        assert_eq!(head_a.local_train_loss_mean_p50, Some(0.6));
        assert_eq!(head_a.local_train_loss_mean_p90, Some(0.9));
        assert_eq!(head_a.compute_time_ms_p90, 1_400.0);
        assert_eq!(head_a.role_counts.get("trainercpu"), None);
        assert_eq!(head_a.role_counts.get("trainer_cpu"), Some(&1));
        assert_eq!(head_a.role_counts.get("browser_verifier"), Some(&1));
        assert_eq!(head_a.backend_counts.get("cpu"), Some(&1));
        assert_eq!(head_a.backend_counts.get("browserwgpu"), None);
        assert_eq!(head_a.backend_counts.get("browser_wgpu"), Some(&1));
    }

    #[test]
    fn peer_window_distribution_detail_filters_one_revision_and_base_head() {
        let started_at = Utc::now();
        let detail = derive_peer_window_distribution_detail(
            &[
                PeerWindowMetrics {
                    network_id: NetworkId::new("network-a"),
                    experiment_id: ExperimentId::new("exp-a"),
                    revision_id: RevisionId::new("rev-a"),
                    workload_id: WorkloadId::new("workload-a"),
                    dataset_view_id: DatasetViewId::new("view-a"),
                    peer_id: burn_p2p_core::PeerId::new("peer-a"),
                    principal_id: None,
                    lease_id: LeaseId::new("lease-a"),
                    base_head_id: HeadId::new("head-a"),
                    window_started_at: started_at,
                    window_finished_at: started_at + Duration::seconds(10),
                    attempted_tokens_or_samples: 100,
                    accepted_tokens_or_samples: Some(80),
                    local_train_loss_mean: Some(0.6),
                    local_train_loss_last: Some(0.55),
                    grad_or_delta_norm: Some(1.0),
                    optimizer_step_count: 10,
                    compute_time_ms: 1_000,
                    data_fetch_time_ms: 100,
                    publish_latency_ms: 10,
                    head_lag_at_start: 0,
                    head_lag_at_finish: 1,
                    backend_class: BackendClass::Cpu,
                    role: PeerRole::TrainerCpu,
                    status: PeerWindowStatus::Completed,
                    status_reason: None,
                },
                PeerWindowMetrics {
                    network_id: NetworkId::new("network-a"),
                    experiment_id: ExperimentId::new("exp-a"),
                    revision_id: RevisionId::new("rev-a"),
                    workload_id: WorkloadId::new("workload-a"),
                    dataset_view_id: DatasetViewId::new("view-a"),
                    peer_id: burn_p2p_core::PeerId::new("peer-b"),
                    principal_id: None,
                    lease_id: LeaseId::new("lease-b"),
                    base_head_id: HeadId::new("head-a"),
                    window_started_at: started_at + Duration::seconds(1),
                    window_finished_at: started_at + Duration::seconds(11),
                    attempted_tokens_or_samples: 120,
                    accepted_tokens_or_samples: Some(0),
                    local_train_loss_mean: Some(0.9),
                    local_train_loss_last: Some(0.8),
                    grad_or_delta_norm: Some(1.2),
                    optimizer_step_count: 11,
                    compute_time_ms: 1_400,
                    data_fetch_time_ms: 300,
                    publish_latency_ms: 30,
                    head_lag_at_start: 2,
                    head_lag_at_finish: 5,
                    backend_class: BackendClass::BrowserWgpu,
                    role: PeerRole::BrowserVerifier,
                    status: PeerWindowStatus::Rejected,
                    status_reason: Some("lag".into()),
                },
                PeerWindowMetrics {
                    network_id: NetworkId::new("network-a"),
                    experiment_id: ExperimentId::new("exp-a"),
                    revision_id: RevisionId::new("rev-b"),
                    workload_id: WorkloadId::new("workload-a"),
                    dataset_view_id: DatasetViewId::new("view-a"),
                    peer_id: burn_p2p_core::PeerId::new("peer-c"),
                    principal_id: None,
                    lease_id: LeaseId::new("lease-c"),
                    base_head_id: HeadId::new("head-a"),
                    window_started_at: started_at + Duration::seconds(2),
                    window_finished_at: started_at + Duration::seconds(12),
                    attempted_tokens_or_samples: 90,
                    accepted_tokens_or_samples: Some(70),
                    local_train_loss_mean: Some(0.4),
                    local_train_loss_last: Some(0.35),
                    grad_or_delta_norm: Some(0.9),
                    optimizer_step_count: 8,
                    compute_time_ms: 900,
                    data_fetch_time_ms: 90,
                    publish_latency_ms: 9,
                    head_lag_at_start: 0,
                    head_lag_at_finish: 0,
                    backend_class: BackendClass::Cpu,
                    role: PeerRole::Evaluator,
                    status: PeerWindowStatus::Completed,
                    status_reason: None,
                },
            ],
            &ExperimentId::new("exp-a"),
            &RevisionId::new("rev-a"),
            &HeadId::new("head-a"),
        )
        .expect("detail");

        assert_eq!(detail.summary.revision_id.as_str(), "rev-a");
        assert_eq!(detail.summary.base_head_id.as_str(), "head-a");
        assert_eq!(detail.summary.sample_count, 2);
        assert_eq!(detail.windows.len(), 2);
        assert_eq!(detail.windows[0].peer_id.as_str(), "peer-b");
        assert_eq!(detail.windows[1].peer_id.as_str(), "peer-a");
    }

    #[test]
    fn metrics_store_persists_and_recovers_materialized_views() {
        let mut indexer = MetricsIndexer::new(MetricsIndexerConfig {
            stale_head_lag_threshold_steps: 2,
            ledger_segment_entry_limit: 2,
            ..MetricsIndexerConfig::default()
        });
        let started_at = Utc::now();
        indexer.ingest_peer_window_metrics(PeerWindowMetrics {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            dataset_view_id: DatasetViewId::new("view-a"),
            peer_id: burn_p2p_core::PeerId::new("peer-a"),
            principal_id: None,
            lease_id: LeaseId::new("lease-a"),
            base_head_id: HeadId::new("head-a"),
            window_started_at: started_at,
            window_finished_at: started_at + Duration::seconds(10),
            attempted_tokens_or_samples: 200,
            accepted_tokens_or_samples: Some(180),
            local_train_loss_mean: Some(0.4),
            local_train_loss_last: Some(0.3),
            grad_or_delta_norm: Some(1.1),
            optimizer_step_count: 10,
            compute_time_ms: 8_000,
            data_fetch_time_ms: 1_000,
            publish_latency_ms: 100,
            head_lag_at_start: 1,
            head_lag_at_finish: 1,
            backend_class: BackendClass::BrowserWgpu,
            role: PeerRole::BrowserTrainerWgpu,
            status: PeerWindowStatus::Completed,
            status_reason: None,
        });
        indexer.ingest_head_eval_report(burn_p2p_core::HeadEvalReport {
            network_id: NetworkId::new("network-a"),
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            workload_id: WorkloadId::new("workload-a"),
            head_id: HeadId::new("head-b"),
            base_head_id: Some(HeadId::new("head-a")),
            eval_protocol_id: burn_p2p_core::ContentId::new("eval-a"),
            evaluator_set_id: burn_p2p_core::ContentId::new("eval-set-a"),
            metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.25))]),
            sample_count: 512,
            dataset_view_id: DatasetViewId::new("view-a"),
            started_at: started_at + Duration::seconds(12),
            finished_at: started_at + Duration::seconds(14),
            trust_class: burn_p2p_core::MetricTrustClass::Canonical,
            status: burn_p2p_core::HeadEvalStatus::Completed,
            signature_bundle: Vec::new(),
        });

        let temp = tempdir().expect("temp dir");
        let mut store = MetricsStore::open(temp.path(), MetricsIndexerConfig::default())
            .expect("open metrics store");
        store
            .replace_entries(indexer.entries.clone())
            .expect("persist entries");
        store.materialize_views().expect("materialize views");

        let reopened = MetricsStore::open(temp.path(), MetricsIndexerConfig::default())
            .expect("reopen metrics store");
        let snapshots = reopened.load_snapshots().expect("load snapshots");
        let segments = reopened
            .load_ledger_segments()
            .expect("load ledger segments");
        let reports = reopened
            .load_head_eval_reports(&HeadId::new("head-b"))
            .expect("load head eval reports");
        let catchup = reopened
            .load_catchup_bundles()
            .expect("load catchup bundles");
        let live_event = reopened
            .load_live_event(MetricsLiveEventKind::CatchupRefresh)
            .expect("load live event")
            .expect("live event");

        assert_eq!(snapshots.len(), 1);
        assert_eq!(
            snapshots[0].manifest.experiment_id,
            ExperimentId::new("exp-a")
        );
        assert!(!segments.is_empty());
        assert_eq!(reports.len(), 1);
        assert_eq!(catchup.len(), 1);
        assert_eq!(
            catchup[0].snapshot.manifest.revision_id,
            RevisionId::new("rev-a")
        );
        assert_eq!(live_event.cursors.len(), 1);
        assert_eq!(live_event.kind, MetricsLiveEventKind::CatchupRefresh);
    }

    #[test]
    fn metrics_indexer_prunes_raw_history_and_caps_detail_payloads() {
        let started_at = Utc::now();
        let mut indexer = MetricsIndexer::new(MetricsIndexerConfig {
            max_peer_window_entries_per_revision: 2,
            max_reducer_cohort_entries_per_revision: 1,
            max_head_eval_reports_per_revision: 1,
            max_revisions_per_experiment: 2,
            max_peer_window_detail_windows: 1,
            ..MetricsIndexerConfig::default()
        });

        for offset in 0..3 {
            indexer.ingest_peer_window_metrics(PeerWindowMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                peer_id: burn_p2p_core::PeerId::new(format!("peer-{offset}")),
                principal_id: None,
                lease_id: LeaseId::new(format!("lease-{offset}")),
                base_head_id: HeadId::new("head-a"),
                window_started_at: started_at + Duration::seconds(offset),
                window_finished_at: started_at + Duration::seconds(offset + 1),
                attempted_tokens_or_samples: 100,
                accepted_tokens_or_samples: Some(100),
                local_train_loss_mean: Some(0.5 + offset as f64),
                local_train_loss_last: Some(0.5 + offset as f64),
                grad_or_delta_norm: Some(1.0),
                optimizer_step_count: 1,
                compute_time_ms: 100,
                data_fetch_time_ms: 10,
                publish_latency_ms: 5,
                head_lag_at_start: 0,
                head_lag_at_finish: offset as u64,
                backend_class: BackendClass::Cpu,
                role: PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            });
        }
        for offset in 0..2 {
            indexer.ingest_reducer_cohort_metrics(ReducerCohortMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                merge_window_id: ContentId::new(format!("merge-{offset}")),
                reducer_group_id: ContentId::new("reducer-a"),
                captured_at: started_at + Duration::seconds(20 + offset),
                base_head_id: HeadId::new("head-a"),
                candidate_head_id: Some(HeadId::new(format!("candidate-{offset}"))),
                received_updates: 1,
                accepted_updates: 1,
                rejected_updates: 0,
                sum_weight: 1.0,
                accepted_tokens_or_samples: 100,
                staleness_mean: 0.0,
                staleness_max: 0.0,
                window_close_delay_ms: 1,
                cohort_duration_ms: 1,
                aggregate_norm: 1.0,
                reducer_load: 0.1,
                ingress_bytes: 10,
                egress_bytes: 10,
                replica_agreement: Some(1.0),
                late_arrival_count: Some(0),
                missing_peer_count: Some(0),
                rejection_reasons: BTreeMap::new(),
                status: burn_p2p_core::ReducerCohortStatus::Closed,
            });
        }
        for offset in 0..2 {
            indexer.ingest_head_eval_report(HeadEvalReport {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                workload_id: WorkloadId::new("workload-a"),
                head_id: HeadId::new(format!("head-{offset}")),
                base_head_id: Some(HeadId::new("head-a")),
                eval_protocol_id: ContentId::new("eval-a"),
                evaluator_set_id: ContentId::new("eval-set-a"),
                metric_values: BTreeMap::from([("loss".into(), MetricValue::Float(0.2))]),
                sample_count: 32,
                dataset_view_id: DatasetViewId::new("view-a"),
                started_at: started_at + Duration::seconds(30 + offset),
                finished_at: started_at + Duration::seconds(31 + offset),
                trust_class: burn_p2p_core::MetricTrustClass::Canonical,
                status: burn_p2p_core::HeadEvalStatus::Completed,
                signature_bundle: Vec::new(),
            });
        }

        let entries = indexer.into_entries();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, MetricEnvelope::PeerWindow(_)))
                .count(),
            2
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, MetricEnvelope::ReducerCohort(_)))
                .count(),
            1
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(entry, MetricEnvelope::HeadEval(_)))
                .count(),
            1
        );

        let peer_windows = entries
            .iter()
            .filter_map(|entry| match entry {
                MetricEnvelope::PeerWindow(metrics) => Some(metrics.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let detail = derive_peer_window_distribution_detail_with_limit(
            &peer_windows,
            &ExperimentId::new("exp-a"),
            &RevisionId::new("rev-a"),
            &HeadId::new("head-a"),
            1,
        )
        .expect("detail");
        assert_eq!(detail.summary.sample_count, 2);
        assert_eq!(detail.windows.len(), 1);
        assert_eq!(detail.windows[0].peer_id.as_str(), "peer-2");
    }

    #[test]
    fn metrics_indexer_prunes_older_revisions_per_experiment() {
        let started_at = Utc::now();
        let mut indexer = MetricsIndexer::new(MetricsIndexerConfig {
            max_peer_window_entries_per_revision: 2,
            max_revisions_per_experiment: 2,
            ..MetricsIndexerConfig::default()
        });

        for (offset, revision) in ["rev-a", "rev-b", "rev-c"].into_iter().enumerate() {
            indexer.ingest_peer_window_metrics(PeerWindowMetrics {
                network_id: NetworkId::new("network-a"),
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new(revision),
                workload_id: WorkloadId::new("workload-a"),
                dataset_view_id: DatasetViewId::new("view-a"),
                peer_id: burn_p2p_core::PeerId::new(format!("peer-{offset}")),
                principal_id: None,
                lease_id: LeaseId::new(format!("lease-{offset}")),
                base_head_id: HeadId::new(format!("head-{offset}")),
                window_started_at: started_at + Duration::seconds(offset as i64),
                window_finished_at: started_at + Duration::seconds(offset as i64 + 1),
                attempted_tokens_or_samples: 100,
                accepted_tokens_or_samples: Some(100),
                local_train_loss_mean: Some(0.5),
                local_train_loss_last: Some(0.5),
                grad_or_delta_norm: Some(1.0),
                optimizer_step_count: 1,
                compute_time_ms: 100,
                data_fetch_time_ms: 10,
                publish_latency_ms: 5,
                head_lag_at_start: 0,
                head_lag_at_finish: 0,
                backend_class: BackendClass::Cpu,
                role: PeerRole::TrainerCpu,
                status: PeerWindowStatus::Completed,
                status_reason: None,
            });
        }

        let retained_revisions = indexer
            .into_entries()
            .into_iter()
            .map(|entry| entry.revision_id().clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            retained_revisions,
            BTreeSet::from([RevisionId::new("rev-b"), RevisionId::new("rev-c")])
        );
    }

    #[test]
    fn derive_robustness_rollup_summarizes_rejections_and_quarantine() {
        let now = Utc::now();
        let cohort = CohortRobustnessReport {
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            window_id: WindowId(7),
            cohort_filter_strategy: CohortFilterStrategy::SimilarityAware,
            aggregation_strategy: AggregationStrategy::ClippedWeightedMean,
            total_updates: 5,
            accepted_updates: 3,
            rejected_updates: 2,
            downweighted_updates: 1,
            effective_weight_sum: 2.5,
            mean_screen_score: 1.4,
            rejection_reasons: std::collections::BTreeMap::new(),
            alerts: vec![RobustnessAlert {
                experiment_id: ExperimentId::new("exp-a"),
                revision_id: RevisionId::new("rev-a"),
                window_id: Some(WindowId(7)),
                peer_id: None,
                reason: burn_p2p_core::RejectionReason::Replay,
                severity: burn_p2p_core::RobustnessAlertSeverity::Warn,
                message: "replay".into(),
                emitted_at: now,
            }],
        };
        let trust_scores = vec![
            TrustScore {
                peer_id: burn_p2p_core::PeerId::new("peer-a"),
                score: 0.5,
                reducer_eligible: true,
                validator_eligible: true,
                quarantined: false,
                ban_recommended: false,
                updated_at: now,
            },
            TrustScore {
                peer_id: burn_p2p_core::PeerId::new("peer-b"),
                score: -1.5,
                reducer_eligible: false,
                validator_eligible: false,
                quarantined: true,
                ban_recommended: true,
                updated_at: now,
            },
        ];
        let canary_reports = vec![CanaryEvalReport {
            experiment_id: ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            eval_protocol_id: ContentId::new("canary"),
            candidate_head_id: HeadId::new("head-a"),
            base_head_id: Some(HeadId::new("head-base")),
            accepted: false,
            metric_deltas: std::collections::BTreeMap::new(),
            regression_margin: 0.12,
            detected_backdoor_trigger: false,
            evaluator_quorum: 2,
            evaluated_at: now,
        }];

        let rollup = derive_robustness_rollup(
            NetworkId::new("network-a"),
            &cohort,
            &trust_scores,
            &canary_reports,
            now,
        );

        assert_eq!(rollup.rejected_updates, 2);
        assert_eq!(rollup.quarantined_peer_count, 1);
        assert_eq!(rollup.ban_recommended_peer_count, 1);
        assert_eq!(rollup.canary_regression_count, 1);
        assert_eq!(rollup.alert_count, 1);
        assert!(rollup.rejection_ratio > 0.0);
    }
}
