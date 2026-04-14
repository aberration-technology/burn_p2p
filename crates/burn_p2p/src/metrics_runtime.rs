use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use super::*;
use burn_p2p_core::{BackendClass, MetricsLiveEvent, MetricsLiveEventKind, MetricsSyncCursor};
use burn_p2p_swarm::MetricsAnnouncement;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredEvalProtocolManifest {
    pub experiment_id: ExperimentId,
    pub revision_id: RevisionId,
    pub captured_at: DateTime<Utc>,
    pub manifest: EvalProtocolManifest,
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeMetricContext {
    pub workload_id: WorkloadId,
    pub dataset_view_id: DatasetViewId,
    pub principal_id: Option<PrincipalId>,
}

pub(crate) struct TrainingMetricBuildArgs<'a, T> {
    pub config: &'a NodeConfig,
    pub experiment: &'a ExperimentHandle,
    pub local_peer_id: &'a PeerId,
    pub limit_profile: &'a LimitProfile,
    pub lease: &'a AssignmentLease,
    pub base_head_id: &'a HeadId,
    pub report: &'a WindowReport<T>,
    pub contribution: &'a ContributionReceipt,
    pub window_started_at: DateTime<Utc>,
    pub data_fetch_time_ms: u64,
    pub publish_latency_ms: u64,
    pub head_lag_at_start: u64,
    pub head_lag_at_finish: u64,
}

pub(crate) struct ValidationMetricBuildArgs<'a> {
    pub config: &'a NodeConfig,
    pub experiment: &'a ExperimentHandle,
    pub local_peer_id: &'a PeerId,
    pub merge_window: &'a MergeWindowState,
    pub base_head_id: &'a HeadId,
    pub updates: &'a [UpdateAnnounce],
    pub evaluation: &'a MetricReport,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub publish_latency_ms: u64,
    pub head_lag_at_start: u64,
    pub head_lag_at_finish: u64,
}

pub(crate) fn resolve_runtime_metric_context(
    config: &NodeConfig,
    experiment: &ExperimentHandle,
    preferred_dataset_view_id: Option<DatasetViewId>,
) -> RuntimeMetricContext {
    let directory_entry = config
        .auth
        .as_ref()
        .and_then(|auth| {
            auth.experiment_directory.iter().find(|entry| {
                entry.network_id == experiment.network_id
                    && entry.study_id == experiment.study_id
                    && entry.experiment_id == experiment.experiment_id
                    && entry.current_revision_id == experiment.revision_id
            })
        })
        .cloned();
    let workload_id = config
        .selected_workload_id
        .clone()
        .or_else(|| {
            directory_entry
                .as_ref()
                .map(|entry| entry.workload_id.clone())
        })
        .unwrap_or_else(|| WorkloadId::new("runtime-default"));
    let dataset_view_id = preferred_dataset_view_id
        .or_else(|| {
            directory_entry
                .as_ref()
                .map(|entry| entry.dataset_view_id.clone())
        })
        .unwrap_or_else(|| DatasetViewId::new("runtime-default"));
    let principal_id = config.auth.as_ref().and_then(|auth| {
        auth.local_peer_auth
            .as_ref()
            .map(|envelope| envelope.certificate.claims().principal_id.clone())
    });

    RuntimeMetricContext {
        workload_id,
        dataset_view_id,
        principal_id,
    }
}

pub(crate) fn persist_peer_window_metrics(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    metrics: &PeerWindowMetrics,
    retention: MetricsRetentionBudget,
) -> anyhow::Result<()> {
    persist_json(
        storage.scoped_peer_window_metrics_path(experiment, &metrics.lease_id),
        metrics,
    )?;
    prune_metric_artifacts::<PeerWindowMetrics, _, _>(
        storage,
        &storage.scoped_peer_window_metrics_experiment_prefix(experiment),
        retention.max_peer_window_entries_per_revision,
        retention.max_metric_revisions_per_experiment,
        |metrics| metrics.window_finished_at,
        |metrics| &metrics.revision_id,
    )
}

pub(crate) fn persist_reducer_cohort_metrics(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    metrics: &ReducerCohortMetrics,
    retention: MetricsRetentionBudget,
) -> anyhow::Result<()> {
    persist_json(
        storage.scoped_reducer_cohort_metrics_path(
            experiment,
            &metrics.merge_window_id,
            &metrics.reducer_group_id,
        ),
        metrics,
    )?;
    prune_metric_artifacts::<ReducerCohortMetrics, _, _>(
        storage,
        &storage.scoped_reducer_cohort_metrics_experiment_prefix(experiment),
        retention.max_reducer_cohort_entries_per_revision,
        retention.max_metric_revisions_per_experiment,
        |metrics| metrics.captured_at,
        |metrics| &metrics.revision_id,
    )
}

pub(crate) fn persist_head_eval_report(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    report: &HeadEvalReport,
    retention: MetricsRetentionBudget,
) -> anyhow::Result<()> {
    persist_json(
        storage.scoped_head_eval_report_path(experiment, &report.head_id, &report.eval_protocol_id),
        report,
    )?;
    prune_metric_artifacts::<HeadEvalReport, _, _>(
        storage,
        &storage.scoped_head_eval_report_experiment_prefix(experiment),
        retention.max_head_eval_reports_per_revision,
        retention.max_metric_revisions_per_experiment,
        |report| report.finished_at,
        |report| &report.revision_id,
    )
}

pub(crate) fn persist_eval_protocol_manifest(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    manifest: &EvalProtocolManifest,
    retention: MetricsRetentionBudget,
) -> anyhow::Result<()> {
    let record = StoredEvalProtocolManifest {
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        captured_at: Utc::now(),
        manifest: manifest.clone(),
    };
    persist_json(
        storage.scoped_eval_protocol_manifest_path(experiment, &manifest.eval_protocol_id),
        &record,
    )?;
    prune_metric_artifacts::<StoredEvalProtocolManifest, _, _>(
        storage,
        &storage.scoped_eval_protocol_manifest_experiment_prefix(experiment),
        retention.max_head_eval_reports_per_revision,
        retention.max_metric_revisions_per_experiment,
        |record| record.captured_at,
        |record| &record.revision_id,
    )
}

fn prune_metric_artifacts<T, F, G>(
    storage: &StorageConfig,
    experiment_prefix: &str,
    limit_per_revision: usize,
    max_revisions_per_experiment: usize,
    captured_at: F,
    revision_id: G,
) -> anyhow::Result<()>
where
    T: serde::de::DeserializeOwned,
    F: Fn(&T) -> DateTime<Utc>,
    G: Fn(&T) -> &RevisionId,
{
    struct ArtifactMeta {
        path: PathBuf,
        revision_id: RevisionId,
        captured_at: DateTime<Utc>,
    }

    let artifacts = fs::read_dir(storage.metrics_dir())
        .map_err(|error| {
            anyhow::anyhow!(
                "failed to read {}: {error}",
                storage.metrics_dir().display()
            )
        })?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let file_name = path.file_name()?.to_str()?;
            if file_name.starts_with(experiment_prefix)
                && path.extension().and_then(|extension| extension.to_str()) == Some("json")
            {
                let file = fs::File::open(&path).ok()?;
                let record = serde_json::from_reader::<_, T>(std::io::BufReader::new(file)).ok()?;
                Some(ArtifactMeta {
                    path,
                    revision_id: revision_id(&record).clone(),
                    captured_at: captured_at(&record),
                })
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let limit_per_revision = limit_per_revision.max(1);
    let max_revisions_per_experiment = max_revisions_per_experiment.max(1);
    if artifacts.is_empty() {
        return Ok(());
    }

    let mut revision_latest = BTreeMap::<RevisionId, DateTime<Utc>>::new();
    for artifact in &artifacts {
        revision_latest
            .entry(artifact.revision_id.clone())
            .and_modify(|existing| {
                if artifact.captured_at > *existing {
                    *existing = artifact.captured_at;
                }
            })
            .or_insert(artifact.captured_at);
    }
    let mut retained_revisions = revision_latest.into_iter().collect::<Vec<_>>();
    retained_revisions
        .sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
    let retained_revisions = retained_revisions
        .into_iter()
        .take(max_revisions_per_experiment)
        .map(|(revision_id, _)| revision_id)
        .collect::<BTreeSet<_>>();

    let mut grouped = BTreeMap::<RevisionId, Vec<ArtifactMeta>>::new();
    for artifact in artifacts {
        grouped
            .entry(artifact.revision_id.clone())
            .or_default()
            .push(artifact);
    }

    for (revision_id, mut artifacts) in grouped {
        artifacts.sort_by(|left, right| {
            right
                .captured_at
                .cmp(&left.captured_at)
                .then_with(|| right.path.cmp(&left.path))
        });
        let keep_limit = if retained_revisions.contains(&revision_id) {
            limit_per_revision
        } else {
            0
        };
        for artifact in artifacts.into_iter().skip(keep_limit) {
            fs::remove_file(&artifact.path).map_err(|error| {
                anyhow::anyhow!("failed to remove {}: {error}", artifact.path.display())
            })?;
        }
    }

    Ok(())
}

pub(crate) fn build_metrics_announcement(
    experiment: &ExperimentHandle,
    overlay: OverlayTopic,
    kind: MetricsLiveEventKind,
    latest_head_id: Option<HeadId>,
    latest_merge_window_id: Option<ContentId>,
    peer_window_hints: Vec<PeerWindowPlacementHint>,
) -> MetricsAnnouncement {
    MetricsAnnouncement {
        overlay,
        event: MetricsLiveEvent {
            network_id: experiment.network_id.clone(),
            kind,
            cursors: vec![MetricsSyncCursor {
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                latest_snapshot_seq: None,
                latest_ledger_segment_seq: None,
                latest_head_id,
                latest_merge_window_id,
            }],
            generated_at: Utc::now(),
        },
        peer_window_hints,
        placement_snapshot: None,
    }
}

pub(crate) fn build_peer_window_placement_hint(
    metrics: &PeerWindowMetrics,
) -> PeerWindowPlacementHint {
    let accepted_tokens_or_samples =
        metrics
            .accepted_tokens_or_samples
            .unwrap_or(match metrics.status {
                PeerWindowStatus::Completed => metrics.attempted_tokens_or_samples,
                _ => 0,
            });
    let window_elapsed_ms = metrics
        .window_finished_at
        .signed_duration_since(metrics.window_started_at)
        .num_milliseconds()
        .max(1) as u64;

    PeerWindowPlacementHint {
        peer_id: metrics.peer_id.clone(),
        role: metrics.role.clone(),
        backend_class: metrics.backend_class.clone(),
        status: metrics.status.clone(),
        accepted_tokens_or_samples,
        window_elapsed_ms,
        head_lag_at_finish: metrics.head_lag_at_finish,
        window_finished_at: metrics.window_finished_at,
    }
}

pub(crate) fn build_training_peer_window_metrics<T>(
    args: TrainingMetricBuildArgs<'_, T>,
) -> PeerWindowMetrics {
    let context = resolve_runtime_metric_context(
        args.config,
        args.experiment,
        Some(args.lease.dataset_view_id.clone()),
    );
    let local_loss = numeric_metric(&args.contribution.metrics, "loss");

    PeerWindowMetrics {
        network_id: args.lease.network_id.clone(),
        experiment_id: args.experiment.experiment_id.clone(),
        revision_id: args.experiment.revision_id.clone(),
        workload_id: context.workload_id,
        dataset_view_id: context.dataset_view_id,
        peer_id: args.local_peer_id.clone(),
        principal_id: context.principal_id,
        lease_id: args.lease.lease_id.clone(),
        base_head_id: args.base_head_id.clone(),
        window_started_at: args.window_started_at,
        window_finished_at: args.report.completed_at,
        attempted_tokens_or_samples: args.lease.budget_work_units,
        accepted_tokens_or_samples: Some(args.lease.budget_work_units),
        local_train_loss_mean: local_loss,
        local_train_loss_last: local_loss,
        grad_or_delta_norm: Some(update_norm_stats(&args.contribution.metrics).l2_norm),
        optimizer_step_count: args.lease.microshards.len().max(1) as u64,
        compute_time_ms: millis_between(
            args.window_started_at + chrono::Duration::milliseconds(args.data_fetch_time_ms as i64),
            args.report.completed_at,
        ),
        data_fetch_time_ms: args.data_fetch_time_ms,
        publish_latency_ms: args.publish_latency_ms,
        head_lag_at_start: args.head_lag_at_start,
        head_lag_at_finish: args.head_lag_at_finish,
        backend_class: infer_backend_class(args.limit_profile, &PeerRole::TrainerCpu),
        role: infer_training_role(args.limit_profile),
        status: PeerWindowStatus::Completed,
        status_reason: None,
    }
}

pub(crate) fn build_validation_peer_window_metrics(
    args: ValidationMetricBuildArgs<'_>,
) -> PeerWindowMetrics {
    let context = resolve_runtime_metric_context(args.config, args.experiment, None);
    let attempted_tokens = sample_count_from_updates(args.updates);
    let local_loss = numeric_metric(&args.evaluation.metrics, "loss");

    PeerWindowMetrics {
        network_id: args.experiment.network_id.clone(),
        experiment_id: args.experiment.experiment_id.clone(),
        revision_id: args.experiment.revision_id.clone(),
        workload_id: context.workload_id,
        dataset_view_id: context.dataset_view_id,
        peer_id: args.local_peer_id.clone(),
        principal_id: context.principal_id,
        lease_id: LeaseId::new(format!(
            "validation-{}",
            args.merge_window.merge_window_id.as_str()
        )),
        base_head_id: args.base_head_id.clone(),
        window_started_at: args.started_at,
        window_finished_at: args.finished_at,
        attempted_tokens_or_samples: attempted_tokens,
        accepted_tokens_or_samples: Some(attempted_tokens),
        local_train_loss_mean: local_loss,
        local_train_loss_last: local_loss,
        grad_or_delta_norm: Some(update_norm_stats(&args.evaluation.metrics).l2_norm),
        optimizer_step_count: args.updates.len().max(1) as u64,
        compute_time_ms: millis_between(args.started_at, args.finished_at),
        data_fetch_time_ms: 0,
        publish_latency_ms: args.publish_latency_ms,
        head_lag_at_start: args.head_lag_at_start,
        head_lag_at_finish: args.head_lag_at_finish,
        backend_class: BackendClass::Viewer,
        role: PeerRole::Validator,
        status: PeerWindowStatus::Completed,
        status_reason: None,
    }
}

pub(crate) fn build_reducer_cohort_metrics(
    config: &NodeConfig,
    experiment: &ExperimentHandle,
    merge_window: &MergeWindowState,
    merged_head: &HeadDescriptor,
    aggregate: &AggregateEnvelope,
    reducer_load_report: &ReducerLoadReport,
    updates: &[UpdateAnnounce],
) -> anyhow::Result<ReducerCohortMetrics> {
    let context = resolve_runtime_metric_context(config, experiment, None);
    let captured_at = reducer_load_report.reported_at;
    let reducer_group_id = ContentId::derive(&merge_window.reducers)?;
    let rejected_updates = u64::from(
        aggregate.stats.duplicate_updates
            + aggregate.stats.dropped_updates
            + aggregate.stats.late_updates,
    );

    Ok(ReducerCohortMetrics {
        network_id: experiment.network_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        workload_id: context.workload_id,
        dataset_view_id: context.dataset_view_id,
        merge_window_id: merge_window.merge_window_id.clone(),
        reducer_group_id,
        captured_at,
        base_head_id: merge_window.base_head_id.clone(),
        candidate_head_id: Some(merged_head.head_id.clone()),
        received_updates: updates.len() as u64,
        accepted_updates: u64::from(aggregate.stats.accepted_updates),
        rejected_updates,
        sum_weight: aggregate.stats.sum_sample_weight,
        accepted_tokens_or_samples: sample_count_from_updates(updates),
        staleness_mean: 0.0,
        staleness_max: 0.0,
        window_close_delay_ms: millis_after_deadline(merge_window.closes_at, captured_at),
        cohort_duration_ms: millis_between(merge_window.opened_at, captured_at),
        aggregate_norm: aggregate.stats.sum_weighted_delta_norm,
        reducer_load: reducer_load_report.overload_ratio,
        ingress_bytes: reducer_load_report.ingress_bytes,
        egress_bytes: reducer_load_report.egress_bytes,
        replica_agreement: None,
        late_arrival_count: Some(u64::from(aggregate.stats.late_updates)),
        missing_peer_count: Some(0),
        rejection_reasons: BTreeMap::new(),
        status: ReducerCohortStatus::Closed,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_head_eval_report(
    config: &NodeConfig,
    experiment: &ExperimentHandle,
    merged_head: &HeadDescriptor,
    evaluation: &MetricReport,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    promoter_peer_id: &PeerId,
    promotion_mode: HeadPromotionMode,
) -> anyhow::Result<(HeadEvalReport, EvalProtocolManifest)> {
    let context = resolve_runtime_metric_context(config, experiment, None);
    let (eval_protocol, evaluator_set_id, metric_values, sample_count, status) =
        match promotion_mode {
            HeadPromotionMode::ValidatorQuorum => {
                let eval_protocol =
                    runtime_validation_eval_protocol(&context.dataset_view_id, evaluation)?;
                let evaluator_set_id = ContentId::derive(&[
                    "validator-set",
                    promoter_peer_id.as_str(),
                    experiment.experiment_id.as_str(),
                    experiment.revision_id.as_str(),
                ])?;
                (
                    eval_protocol,
                    evaluator_set_id,
                    evaluation.metrics.clone(),
                    evaluation_sample_count(evaluation),
                    HeadEvalStatus::Completed,
                )
            }
            HeadPromotionMode::ReducerAuthority => {
                let eval_protocol = EvalProtocolManifest::new(
                    "runtime-aggregation-only",
                    context.dataset_view_id.clone(),
                    "promotion",
                    Vec::new(),
                    EvalProtocolOptions::new(EvalAggregationRule::Mean, 0, 0, "v1"),
                )?;
                let evaluator_set_id = ContentId::derive(&[
                    "reducer-authority-promoter",
                    promoter_peer_id.as_str(),
                    experiment.experiment_id.as_str(),
                    experiment.revision_id.as_str(),
                ])?;
                (
                    eval_protocol,
                    evaluator_set_id,
                    BTreeMap::new(),
                    0,
                    HeadEvalStatus::Skipped,
                )
            }
            HeadPromotionMode::DiffusionSteadyState => {
                let eval_protocol = EvalProtocolManifest::new(
                    "runtime-diffusion-promotion",
                    context.dataset_view_id.clone(),
                    "promotion",
                    Vec::new(),
                    EvalProtocolOptions::new(EvalAggregationRule::Mean, 0, 0, "v1"),
                )?;
                let evaluator_set_id = ContentId::derive(&[
                    "diffusion-promoter",
                    promoter_peer_id.as_str(),
                    experiment.experiment_id.as_str(),
                    experiment.revision_id.as_str(),
                ])?;
                (
                    eval_protocol,
                    evaluator_set_id,
                    evaluation.metrics.clone(),
                    evaluation_sample_count(evaluation),
                    HeadEvalStatus::Completed,
                )
            }
        };
    let eval_protocol_id = eval_protocol.eval_protocol_id.clone();

    Ok((
        HeadEvalReport {
            network_id: experiment.network_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            workload_id: context.workload_id,
            head_id: merged_head.head_id.clone(),
            base_head_id: merged_head.parent_head_id.clone(),
            eval_protocol_id,
            evaluator_set_id,
            metric_values,
            sample_count,
            dataset_view_id: context.dataset_view_id,
            started_at,
            finished_at,
            trust_class: MetricTrustClass::Canonical,
            status,
            signature_bundle: Vec::new(),
        },
        eval_protocol,
    ))
}

fn runtime_validation_eval_protocol(
    dataset_view_id: &DatasetViewId,
    evaluation: &MetricReport,
) -> anyhow::Result<EvalProtocolManifest> {
    let metric_defs = evaluation
        .metrics
        .keys()
        .cloned()
        .map(|metric_key| EvalMetricDef {
            display_name: metric_key.clone(),
            higher_is_better: !metric_key.contains("loss"),
            metric_key,
            unit: None,
        })
        .collect::<Vec<_>>();

    EvalProtocolManifest::new(
        "runtime-validation-default",
        dataset_view_id.clone(),
        "validation",
        metric_defs,
        burn_p2p_core::EvalProtocolOptions::new(EvalAggregationRule::Mean, 1, 0, "v1"),
    )
    .map_err(anyhow::Error::from)
}

fn numeric_metric(metrics: &BTreeMap<String, MetricValue>, key: &str) -> Option<f64> {
    match metrics.get(key) {
        Some(MetricValue::Integer(value)) => Some(*value as f64),
        Some(MetricValue::Float(value)) => Some(*value),
        Some(MetricValue::Bool(_)) | Some(MetricValue::Text(_)) | None => None,
    }
}

fn evaluation_sample_count(evaluation: &MetricReport) -> u64 {
    for key in ["evaluation_items", "sample_count", "samples"] {
        match evaluation.metrics.get(key) {
            Some(MetricValue::Integer(value)) if *value > 0 => return *value as u64,
            Some(MetricValue::Float(value)) if *value > 0.0 => return value.round() as u64,
            _ => {}
        }
    }
    evaluation.metrics.len().max(1) as u64
}

fn sample_count_from_updates(updates: &[UpdateAnnounce]) -> u64 {
    let rounded = updates
        .iter()
        .map(|update| update.sample_weight.max(0.0).round() as u64)
        .sum::<u64>();
    rounded.max(updates.len() as u64)
}

fn millis_between(start: DateTime<Utc>, finish: DateTime<Utc>) -> u64 {
    (finish - start).num_milliseconds().max(0) as u64
}

fn millis_after_deadline(deadline: DateTime<Utc>, observed_at: DateTime<Utc>) -> u64 {
    (observed_at - deadline).num_milliseconds().max(0) as u64
}

fn infer_training_role(limit_profile: &LimitProfile) -> PeerRole {
    match infer_backend_class(limit_profile, &PeerRole::TrainerCpu) {
        BackendClass::BrowserWgpu => PeerRole::BrowserTrainerWgpu,
        BackendClass::BrowserFallback => PeerRole::BrowserFallback,
        BackendClass::Cpu | BackendClass::Ndarray | BackendClass::Viewer => PeerRole::TrainerCpu,
        BackendClass::Cuda
        | BackendClass::Wgpu
        | BackendClass::Rocm
        | BackendClass::Metal
        | BackendClass::Custom(_) => PeerRole::TrainerGpu,
    }
}

fn infer_backend_class(limit_profile: &LimitProfile, fallback_role: &PeerRole) -> BackendClass {
    match limit_profile
        .estimate
        .preferred_backends
        .first()
        .map(String::as_str)
    {
        Some("cuda") => BackendClass::Cuda,
        Some("wgpu") => BackendClass::Wgpu,
        Some("ndarray") => BackendClass::Ndarray,
        Some("cpu") => BackendClass::Cpu,
        Some("rocm") => BackendClass::Rocm,
        Some("metal") => BackendClass::Metal,
        Some(other) => BackendClass::Custom(other.to_owned()),
        None => match fallback_role {
            PeerRole::BrowserObserver => BackendClass::Viewer,
            PeerRole::BrowserVerifier | PeerRole::BrowserTrainerWgpu | PeerRole::BrowserTrainer => {
                BackendClass::BrowserWgpu
            }
            PeerRole::BrowserFallback => BackendClass::BrowserFallback,
            PeerRole::Viewer => BackendClass::Viewer,
            PeerRole::TrainerCpu => BackendClass::Cpu,
            PeerRole::TrainerGpu | PeerRole::Validator | PeerRole::Evaluator => BackendClass::Cpu,
            PeerRole::Bootstrap
            | PeerRole::Authority
            | PeerRole::Archive
            | PeerRole::Reducer
            | PeerRole::RelayHelper => BackendClass::Viewer,
        },
    }
}
