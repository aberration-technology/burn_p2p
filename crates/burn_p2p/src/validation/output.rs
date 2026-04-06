use super::*;

pub(super) struct ValidationExecution {
    pub source_peer_id: PeerId,
    pub merged_head: HeadDescriptor,
    pub accepted_updates: Vec<UpdateAnnounce>,
    pub merge_certificate: MergeCertificate,
    pub contribution: ContributionReceipt,
    pub evaluation: MetricReport,
    pub aggregate: AggregateEnvelope,
    pub aggregate_artifact: AggregateArtifactBytes,
    pub reduction_certificate: ReductionCertificate,
    pub reducer_load_report: ReducerLoadReport,
    pub robustness: ValidationRobustnessExecution,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

pub(super) fn aggregate_stats_from_updates(updates: &[UpdateAnnounce]) -> AggregateStats {
    AggregateStats {
        accepted_updates: updates.len() as u32,
        duplicate_updates: 0,
        dropped_updates: 0,
        late_updates: 0,
        sum_sample_weight: updates.iter().map(|update| update.sample_weight).sum(),
        sum_quality_weight: updates.iter().map(|update| update.quality_weight).sum(),
        sum_weighted_delta_norm: updates
            .iter()
            .map(|update| update.sample_weight * update.quality_weight * update.norm_stats.l2_norm)
            .sum(),
        max_update_norm: updates
            .iter()
            .map(|update| update.norm_stats.max_abs)
            .fold(0.0_f64, f64::max),
        accepted_sample_coverage: if updates.is_empty() { 0.0 } else { 1.0 },
    }
}

pub(super) fn build_aggregate_record(
    experiment: &ExperimentHandle,
    base_head_id: &HeadId,
    aggregate_id: &ContentId,
    merge_policy: MergePolicy,
    evaluation: &MetricReport,
    updates: &[UpdateAnnounce],
    aggregate_stats: &AggregateStats,
) -> AggregateArtifactRecord {
    AggregateArtifactRecord {
        aggregate_id: aggregate_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        base_head_id: base_head_id.clone(),
        policy: merge_policy,
        aggregate_stats: aggregate_stats.clone(),
        merge_plan: MergePlan {
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            base_head_id: base_head_id.clone(),
            policy: MergePolicy::QualityWeightedEma,
            contribution_receipt_ids: updates
                .iter()
                .flat_map(|update| update.receipt_ids.clone())
                .collect(),
            artifact_ids: updates
                .iter()
                .map(|update| update.delta_artifact_id.clone())
                .collect(),
            total_weight: updates
                .iter()
                .map(|update| update.sample_weight * update.quality_weight)
                .sum(),
            aggregated_numeric_metrics: evaluation
                .metrics
                .iter()
                .filter_map(|(name, value)| match value {
                    MetricValue::Integer(value) => Some((name.clone(), *value as f64)),
                    MetricValue::Float(value) => Some((name.clone(), *value)),
                    MetricValue::Bool(_) | MetricValue::Text(_) => None,
                })
                .collect(),
        },
        inputs: updates
            .iter()
            .map(|update| AggregateArtifactInput {
                peer_id: update.peer_id.clone(),
                artifact_id: update.delta_artifact_id.clone(),
                sample_weight: update.sample_weight,
                quality_weight: update.quality_weight,
                receipt_ids: update.receipt_ids.clone(),
            })
            .collect(),
        created_at: Utc::now(),
    }
}

pub(super) fn build_validation_aggregate(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    aggregate_id: &ContentId,
    aggregate_artifact: &AggregateArtifactBytes,
    aggregate_stats: &AggregateStats,
    updates: &[UpdateAnnounce],
) -> AggregateEnvelope {
    AggregateEnvelope {
        aggregate_id: aggregate_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: prepared.merge_window.window_id,
        base_head_id: prepared.base_head_id.clone(),
        aggregate_artifact_id: aggregate_artifact.descriptor.artifact_id.clone(),
        tier: AggregateTier::RootCandidate,
        reducer_peer_id: prepared.local_peer_id.clone(),
        contributor_peers: updates
            .iter()
            .map(|update| update.peer_id.clone())
            .collect(),
        child_aggregate_ids: Vec::new(),
        stats: aggregate_stats.clone(),
        providers: std::iter::once(prepared.local_peer_id.clone())
            .chain(updates.iter().flat_map(|update| update.providers.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        published_at: Utc::now(),
    }
}

pub(super) fn build_reduction_certificate(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    aggregate: &AggregateEnvelope,
    merge_window: &MergeWindowState,
) -> anyhow::Result<ReductionCertificate> {
    Ok(ReductionCertificate {
        reduction_id: ContentId::derive(&(
            aggregate.aggregate_id.as_str(),
            prepared.local_peer_id.as_str(),
            merge_window.window_id.0,
        ))?,
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: merge_window.window_id,
        base_head_id: prepared.base_head_id.clone(),
        aggregate_id: aggregate.aggregate_id.clone(),
        validator: prepared.local_peer_id.clone(),
        validator_quorum: merge_window.policy.promotion_policy.validator_quorum.max(1),
        cross_checked_reducers: merge_window
            .reducers
            .iter()
            .take(usize::from(merge_window.policy.reducer_replication.max(1)))
            .cloned()
            .collect(),
        issued_at: Utc::now(),
    })
}

pub(super) fn build_validation_contribution(
    experiment: &ExperimentHandle,
    source_peer_id: &PeerId,
    merged_head: &HeadDescriptor,
    evaluation: &MetricReport,
) -> ContributionReceipt {
    ContributionReceipt {
        receipt_id: ContributionReceiptId::new(format!(
            "{}-validated-{}-{}",
            experiment.experiment_id.as_str(),
            source_peer_id.as_str(),
            merged_head.global_step
        )),
        peer_id: source_peer_id.clone(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        base_head_id: merged_head
            .parent_head_id
            .clone()
            .unwrap_or_else(|| HeadId::new("genesis")),
        artifact_id: merged_head.artifact_id.clone(),
        accepted_at: Utc::now(),
        accepted_weight: 1.0,
        metrics: evaluation.metrics.clone(),
        merge_cert_id: None,
    }
}

pub(super) fn build_validation_merge_certificate(
    experiment: &ExperimentHandle,
    local_peer_id: &PeerId,
    base_head_id: &HeadId,
    merged_head: &HeadDescriptor,
    merge_policy: MergePolicy,
    contribution: &ContributionReceipt,
    updates: &[UpdateAnnounce],
) -> MergeCertificate {
    MergeCertificate {
        merge_cert_id: MergeCertId::new(format!(
            "{}-merge-{}",
            experiment.experiment_id.as_str(),
            merged_head.global_step
        )),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        base_head_id: base_head_id.clone(),
        merged_head_id: merged_head.head_id.clone(),
        merged_artifact_id: merged_head.artifact_id.clone(),
        policy: merge_policy,
        issued_at: Utc::now(),
        validator: local_peer_id.clone(),
        contribution_receipts: if updates.is_empty() {
            vec![contribution.receipt_id.clone()]
        } else {
            updates
                .iter()
                .flat_map(|update| update.receipt_ids.clone())
                .collect()
        },
    }
}

pub(super) fn build_validation_reducer_load(
    local_peer_id: &PeerId,
    aggregate: &AggregateEnvelope,
    updates: &[UpdateAnnounce],
) -> ReducerLoadReport {
    ReducerLoadReport {
        peer_id: local_peer_id.clone(),
        window_id: aggregate.window_id,
        assigned_leaf_updates: aggregate.stats.accepted_updates,
        assigned_aggregate_inputs: aggregate.stats.accepted_updates,
        ingress_bytes: updates.len() as u128 * 1024,
        egress_bytes: aggregate.providers.len() as u128 * 512,
        duplicate_transfer_ratio: 0.0,
        overload_ratio: 0.0,
        reported_at: Utc::now(),
    }
}
