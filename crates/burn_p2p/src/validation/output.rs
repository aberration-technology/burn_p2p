use super::*;

fn sort_and_dedup<T: Ord>(values: &mut Vec<T>) {
    values.sort();
    values.dedup();
}

fn canonicalize_f64(value: f64) -> f64 {
    if value.is_finite() {
        (value * 1_000_000_000_000.0).round() / 1_000_000_000_000.0
    } else {
        value
    }
}

#[derive(Clone)]
pub(super) struct ValidationExecution {
    pub source_peer_id: PeerId,
    pub merged_head: HeadDescriptor,
    pub accepted_updates: Vec<UpdateAnnounce>,
    pub merge_certificate: MergeCertificate,
    pub contribution: ContributionReceipt,
    pub evaluation: MetricReport,
    pub aggregate: AggregateEnvelope,
    pub local_aggregate_materialization: Option<LocalAggregateMaterialization>,
    pub reduction_certificate: ReductionCertificate,
    pub robustness: ValidationRobustnessExecution,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

#[derive(Clone)]
pub(super) struct LocalAggregateMaterialization {
    pub aggregate: AggregateEnvelope,
    pub aggregate_artifact: AggregateArtifactBytes,
    pub reducer_load_report: ReducerLoadReport,
}

pub(super) fn aggregate_stats_from_updates(updates: &[UpdateAnnounce]) -> AggregateStats {
    AggregateStats {
        accepted_updates: updates.len() as u32,
        duplicate_updates: 0,
        dropped_updates: 0,
        late_updates: 0,
        sum_sample_weight: canonicalize_f64(
            updates.iter().map(|update| update.sample_weight).sum(),
        ),
        sum_quality_weight: canonicalize_f64(
            updates.iter().map(|update| update.quality_weight).sum(),
        ),
        sum_weighted_delta_norm: canonicalize_f64(
            updates
                .iter()
                .map(|update| {
                    update.sample_weight * update.quality_weight * update.norm_stats.l2_norm
                })
                .sum(),
        ),
        max_update_norm: canonicalize_f64(
            updates
                .iter()
                .map(|update| update.norm_stats.max_abs)
                .fold(0.0_f64, f64::max),
        ),
        accepted_sample_coverage: if updates.is_empty() { 0.0 } else { 1.0 },
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_aggregate_record(
    experiment: &ExperimentHandle,
    base_head_id: &HeadId,
    aggregate_id: &ContentId,
    merge_policy: MergePolicy,
    evaluation: &MetricReport,
    updates: &[UpdateAnnounce],
    aggregate_stats: &AggregateStats,
    created_at: DateTime<Utc>,
) -> AggregateArtifactRecord {
    let mut contribution_receipt_ids = updates
        .iter()
        .flat_map(|update| update.receipt_ids.clone())
        .collect::<Vec<_>>();
    let mut artifact_ids = updates
        .iter()
        .map(|update| update.delta_artifact_id.clone())
        .collect::<Vec<_>>();
    let mut inputs = updates
        .iter()
        .map(|update| {
            let mut receipt_ids = update.receipt_ids.clone();
            sort_and_dedup(&mut receipt_ids);
            AggregateArtifactInput {
                peer_id: update.peer_id.clone(),
                artifact_id: update.delta_artifact_id.clone(),
                sample_weight: canonicalize_f64(update.sample_weight),
                quality_weight: canonicalize_f64(update.quality_weight),
                receipt_ids,
            }
        })
        .collect::<Vec<_>>();
    sort_and_dedup(&mut contribution_receipt_ids);
    sort_and_dedup(&mut artifact_ids);
    inputs.sort_by(|left, right| {
        left.peer_id
            .cmp(&right.peer_id)
            .then(left.artifact_id.cmp(&right.artifact_id))
    });
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
            contribution_receipt_ids,
            artifact_ids,
            total_weight: canonicalize_f64(
                updates
                    .iter()
                    .map(|update| update.sample_weight * update.quality_weight)
                    .sum(),
            ),
            aggregated_numeric_metrics: evaluation
                .metrics
                .iter()
                .filter_map(|(name, value)| match value {
                    MetricValue::Integer(value) => {
                        Some((name.clone(), canonicalize_f64(*value as f64)))
                    }
                    MetricValue::Float(value) => Some((name.clone(), canonicalize_f64(*value))),
                    MetricValue::Bool(_) | MetricValue::Text(_) => None,
                })
                .collect(),
        },
        inputs,
        created_at,
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
    let mut contributor_peers = updates
        .iter()
        .map(|update| update.peer_id.clone())
        .collect::<Vec<_>>();
    let mut providers = std::iter::once(prepared.local_peer_id.clone())
        .chain(updates.iter().flat_map(|update| update.providers.clone()))
        .collect::<Vec<_>>();
    sort_and_dedup(&mut contributor_peers);
    sort_and_dedup(&mut providers);
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
        contributor_peers,
        child_aggregate_ids: Vec::new(),
        stats: aggregate_stats.clone(),
        providers,
        published_at: Utc::now(),
    }
}

pub(super) fn build_reduction_certificate(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    aggregate: &AggregateEnvelope,
    merge_window: &MergeWindowState,
) -> anyhow::Result<ReductionCertificate> {
    let mut cross_checked_reducers = merge_window
        .reducers
        .iter()
        .take(usize::from(merge_window.policy.reducer_replication.max(1)))
        .cloned()
        .collect::<Vec<_>>();
    sort_and_dedup(&mut cross_checked_reducers);
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
        validator_quorum: super::effective_validator_quorum(merge_window) as u16,
        cross_checked_reducers,
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
    let mut contribution_receipts = if updates.is_empty() {
        vec![contribution.receipt_id.clone()]
    } else {
        updates
            .iter()
            .flat_map(|update| update.receipt_ids.clone())
            .collect::<Vec<_>>()
    };
    sort_and_dedup(&mut contribution_receipts);
    MergeCertificate {
        merge_cert_id: ContentId::derive(&(
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
            base_head_id.as_str(),
            merged_head.head_id.as_str(),
            local_peer_id.as_str(),
        ))
        .expect("merge certificate id derivation should succeed")
        .into(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        base_head_id: base_head_id.clone(),
        merged_head_id: merged_head.head_id.clone(),
        merged_artifact_id: merged_head.artifact_id.clone(),
        policy: merge_policy,
        issued_at: Utc::now(),
        validator: local_peer_id.clone(),
        contribution_receipts,
    }
}

pub(super) fn build_validation_quorum_certificate(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    aggregate: &AggregateEnvelope,
    merged_head: &HeadDescriptor,
    attesting_validators: &[PeerId],
    reduction_ids: &[ContentId],
) -> anyhow::Result<ValidationQuorumCertificate> {
    let mut attesting_validators = attesting_validators.to_vec();
    let mut reduction_ids = reduction_ids.to_vec();
    sort_and_dedup(&mut attesting_validators);
    sort_and_dedup(&mut reduction_ids);
    Ok(ValidationQuorumCertificate {
        quorum_cert_id: ContentId::derive(&(
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
            prepared.base_head_id.as_str(),
            aggregate.aggregate_id.as_str(),
            merged_head.head_id.as_str(),
        ))?,
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: prepared.merge_window.window_id,
        base_head_id: prepared.base_head_id.clone(),
        aggregate_id: aggregate.aggregate_id.clone(),
        aggregate_artifact_id: aggregate.aggregate_artifact_id.clone(),
        merged_head_id: merged_head.head_id.clone(),
        validator_quorum: super::effective_validator_quorum(&prepared.merge_window) as u16,
        coordinator: prepared.local_peer_id.clone(),
        attesting_validators,
        reduction_ids,
        issued_at: Utc::now(),
    })
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
