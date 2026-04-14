use super::*;
use crate::candidate::ValidationCandidateView;
use burn_p2p_core::QuarantinePolicy;

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PeerRobustnessState {
    pub trust_score: f64,
    pub consecutive_rejections: u16,
    pub quarantined: bool,
    pub last_rejection_reason: Option<RejectionReason>,
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_decision_window: Option<WindowId>,
    #[serde(default)]
    pub quarantine_started_window: Option<WindowId>,
    #[serde(default)]
    pub last_quarantine_window: Option<WindowId>,
    #[serde(default)]
    pub ban_recommended: bool,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PersistedRobustnessState {
    pub peers: BTreeMap<PeerId, PeerRobustnessState>,
}

pub(crate) struct CandidateRobustnessContext<'a> {
    pub robustness_policy: &'a RobustnessPolicy,
    pub robustness_state: &'a PersistedRobustnessState,
    pub snapshots: &'a [(PeerId, ControlPlaneSnapshot)],
    pub base_head_id: &'a HeadId,
    pub dataset_view_id: &'a DatasetViewId,
    pub merge_window: &'a MergeWindowState,
}

type AcceptedCandidateWeights = BTreeMap<(PeerId, ArtifactId), f64>;

pub(crate) struct CandidateRobustnessOutcome {
    pub decisions: Vec<RobustnessDecision>,
    pub trust_scores: Vec<TrustScore>,
    pub filtered_updates: Vec<UpdateAnnounce>,
    pub accepted_weights: AcceptedCandidateWeights,
}

struct AcceptedCohortCandidate {
    update_index: usize,
    sketch: UpdateFeatureSketch,
}

pub(crate) fn evaluate_candidate_robustness<M>(
    engine: &burn_p2p_security::RobustnessEngine,
    context: CandidateRobustnessContext<'_>,
    candidate_models: &[ValidationCandidateView<'_, M>],
    evaluated_at: DateTime<Utc>,
) -> CandidateRobustnessOutcome {
    let mut seen_receipt_roots = BTreeSet::new();
    let mut seen_receipt_ids = BTreeSet::new();
    let mut seen_artifacts = BTreeSet::new();
    let browser_peers = browser_peer_ids_from_snapshots(context.snapshots);
    let mut decisions = Vec::new();
    let mut accepted_candidates = Vec::new();
    let mut candidate_states = Vec::new();

    for (update_index, candidate) in candidate_models.iter().enumerate() {
        let update = candidate.update;
        let peer_state = context
            .robustness_state
            .peers
            .get(&update.peer_id)
            .map(|state| {
                resolved_peer_robustness_state(
                    &context.robustness_policy.quarantine_policy,
                    context.merge_window.window_id,
                    state,
                )
            })
            .unwrap_or_default();
        let duplicate = !seen_receipt_roots.insert(update.receipt_root.clone())
            || update
                .receipt_ids
                .iter()
                .any(|receipt_id| !seen_receipt_ids.insert(receipt_id.clone()))
            || !seen_artifacts.insert((update.peer_id.clone(), update.delta_artifact_id.clone()));
        let observation = burn_p2p_security::RobustUpdateObservation {
            peer_id: update.peer_id.clone(),
            base_head_id: update.base_head_id.clone(),
            expected_base_head_id: context.base_head_id.clone(),
            revision_id: update.revision_id.clone(),
            expected_revision_id: context.merge_window.revision_id.clone(),
            dataset_view_id: context.dataset_view_id.clone(),
            expected_dataset_view_id: context.dataset_view_id.clone(),
            lease_id: update.lease_id.clone(),
            expected_lease_id: update.lease_id.clone(),
            schema_matches: true,
            duplicate,
            quarantined: peer_state.quarantined,
            consecutive_rejections: peer_state.consecutive_rejections,
            current_trust_score: peer_state.trust_score,
            canary_report: candidate.canary_report.cloned(),
            feature_sketch: update
                .feature_sketch
                .clone()
                .unwrap_or_else(|| fallback_update_feature_sketch(update)),
        };
        let decision = engine.evaluate_update(&observation, evaluated_at);
        if decision.accepted {
            let sketch = update
                .feature_sketch
                .clone()
                .unwrap_or_else(|| fallback_update_feature_sketch(update));
            accepted_candidates.push(AcceptedCohortCandidate {
                update_index,
                sketch,
            });
        }
        candidate_states.push((peer_state, update.clone()));
        decisions.push(decision);
    }

    let accepted_sketches = accepted_candidates
        .iter()
        .map(|candidate| candidate.sketch.clone())
        .collect::<Vec<_>>();
    let accepted_base_weights = accepted_candidates
        .iter()
        .map(|candidate| decisions[candidate.update_index].effective_weight)
        .collect::<Vec<_>>();
    let cohort_decisions = burn_p2p_security::filter_update_sketches_with_policy(
        context.robustness_policy,
        &accepted_sketches,
        &accepted_base_weights,
    );
    for (accepted_candidate, cohort_decision) in accepted_candidates.iter().zip(cohort_decisions) {
        let decision = &mut decisions[accepted_candidate.update_index];
        if !cohort_decision.accepted {
            decision.accepted = false;
            decision.downweighted = false;
            decision.rejection_reason = Some(RejectionReason::SimilarityOutlier);
            decision.effective_weight = 0.0;
        } else {
            decision.effective_weight *= cohort_decision.weight_scale.max(0.01);
            if cohort_decision.weight_scale < 0.999 {
                decision.downweighted = true;
            }
        }
    }
    let maximum_cohort_size = usize::from(
        context
            .robustness_policy
            .aggregation_policy
            .maximum_cohort_size
            .max(1),
    );
    let mut ranked_accepted = decisions
        .iter()
        .enumerate()
        .filter(|(_, decision)| decision.accepted)
        .map(|(index, decision)| (index, decision.effective_weight, decision.screen_score))
        .collect::<Vec<_>>();
    if ranked_accepted.len() > maximum_cohort_size {
        ranked_accepted.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.2.total_cmp(&right.2))
                .then_with(|| left.0.cmp(&right.0))
        });
        let keep = ranked_accepted
            .iter()
            .take(maximum_cohort_size)
            .map(|(index, _, _)| *index)
            .collect::<BTreeSet<_>>();
        for (index, decision) in decisions.iter_mut().enumerate() {
            if decision.accepted && !keep.contains(&index) {
                decision.accepted = false;
                decision.downweighted = false;
                decision.rejection_reason = Some(RejectionReason::SimilarityOutlier);
                decision.effective_weight = 0.0;
            }
        }
    }

    let mut trust_scores = Vec::new();
    let mut filtered_updates = Vec::new();
    let mut accepted_weights = BTreeMap::new();
    for (index, decision) in decisions.iter_mut().enumerate() {
        let (peer_state, update) = &candidate_states[index];
        decision.quarantined = burn_p2p_security::should_quarantine(
            &context.robustness_policy.quarantine_policy,
            peer_state.consecutive_rejections,
            decision,
        );
        let trust_score =
            burn_p2p_security::updated_trust_score(burn_p2p_security::TrustUpdateInput {
                policy: &context.robustness_policy.reputation_policy,
                peer_id: decision.peer_id.clone(),
                current_score: peer_state.trust_score,
                accepted: decision.accepted,
                downweighted: decision.downweighted,
                screen_score: decision.screen_score,
                rejection_reason: decision.rejection_reason.as_ref(),
                quarantined: decision.quarantined,
                updated_at: evaluated_at,
            });
        decision.trust_score = Some(trust_score.clone());
        trust_scores.push(trust_score);

        if decision.accepted {
            let capped_weight = if browser_peers.contains(&update.peer_id) {
                decision
                    .effective_weight
                    .min(
                        context
                            .robustness_policy
                            .aggregation_policy
                            .browser_contribution_cap,
                    )
                    .max(0.01)
            } else {
                decision.effective_weight.max(0.01)
            };
            decision.effective_weight = capped_weight;
            accepted_weights.insert(
                (update.peer_id.clone(), update.delta_artifact_id.clone()),
                capped_weight,
            );
            let mut filtered = update.clone();
            filtered.sample_weight *= capped_weight;
            filtered_updates.push(filtered);
        }
    }

    CandidateRobustnessOutcome {
        decisions,
        trust_scores,
        filtered_updates,
        accepted_weights,
    }
}

pub(crate) fn build_validation_canary_report(
    experiment: &ExperimentHandle,
    current_head: &Option<(PeerId, HeadDescriptor)>,
    candidate_head: &HeadDescriptor,
    evaluation: &MetricReport,
    maximum_regression_delta: f64,
    evaluator_quorum: u16,
) -> anyhow::Result<CanaryEvalReport> {
    let base_quality = current_head
        .as_ref()
        .and_then(|(_, head)| (!head.metrics.is_empty()).then(|| metric_quality(&head.metrics)))
        .unwrap_or_else(|| metric_quality(&evaluation.metrics));
    let candidate_quality = metric_quality(&evaluation.metrics);
    let regression_margin = (candidate_quality - base_quality).max(0.0);
    let metric_deltas = evaluation
        .metrics
        .iter()
        .filter_map(|(key, value)| match value {
            MetricValue::Integer(value) => Some((key.clone(), *value as f64)),
            MetricValue::Float(value) => Some((key.clone(), *value)),
            MetricValue::Bool(_) | MetricValue::Text(_) => None,
        })
        .collect::<BTreeMap<_, _>>();

    Ok(CanaryEvalReport {
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        eval_protocol_id: ContentId::derive(&(
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
            "runtime-canary",
        ))?,
        candidate_head_id: candidate_head.head_id.clone(),
        base_head_id: current_head.as_ref().map(|(_, head)| head.head_id.clone()),
        accepted: regression_margin <= maximum_regression_delta,
        metric_deltas,
        regression_margin,
        detected_backdoor_trigger: false,
        evaluator_quorum,
        evaluated_at: Utc::now(),
    })
}

fn browser_peer_ids_from_snapshots(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
) -> BTreeSet<PeerId> {
    snapshots
        .iter()
        .filter_map(|(peer_id, snapshot)| {
            snapshot
                .auth_announcements
                .iter()
                .filter(|announcement| &announcement.peer_id == peer_id)
                .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
                .and_then(|announcement| {
                    announcement
                        .envelope
                        .certificate
                        .claims()
                        .granted_roles
                        .roles
                        .iter()
                        .any(|role| {
                            matches!(
                                role,
                                PeerRole::Viewer
                                    | PeerRole::BrowserObserver
                                    | PeerRole::BrowserVerifier
                                    | PeerRole::BrowserTrainerWgpu
                                    | PeerRole::BrowserFallback
                                    | PeerRole::BrowserTrainer
                            )
                        })
                        .then(|| peer_id.clone())
                })
        })
        .collect()
}

fn fallback_update_feature_sketch(update: &UpdateAnnounce) -> UpdateFeatureSketch {
    UpdateFeatureSketch {
        artifact_size_bytes: 0,
        global_norm: update.norm_stats.l2_norm,
        per_layer_norms: BTreeMap::from([(String::from("update"), update.norm_stats.l2_norm)]),
        random_projection: vec![update.norm_stats.l2_norm, update.norm_stats.max_abs],
        sign_projection: vec![
            if update.norm_stats.l2_norm > 0.0 {
                1
            } else {
                0
            },
            if update.norm_stats.max_abs > 0.0 {
                1
            } else {
                0
            },
        ],
        top_k_indices: Vec::new(),
        cosine_to_reference: None,
        sign_agreement_fraction: None,
        canary_loss_delta: None,
        historical_deviation_score: None,
        neighbor_distance: None,
        staleness_windows: 0,
        receive_delay_ms: 0,
        non_finite_tensor_count: update.norm_stats.non_finite_tensors,
    }
}

pub(crate) fn resolved_peer_robustness_state(
    policy: &QuarantinePolicy,
    current_window: WindowId,
    state: &PeerRobustnessState,
) -> PeerRobustnessState {
    let mut resolved = state.clone();
    if !policy.automatic_quarantine {
        resolved.quarantined = false;
        resolved.quarantine_started_window = None;
        resolved.last_quarantine_window = None;
        resolved.ban_recommended = false;
        return resolved;
    }
    if !resolved.quarantined {
        resolved.quarantine_started_window = None;
        resolved.last_quarantine_window = None;
        resolved.ban_recommended = false;
        return resolved;
    }

    let quarantine_started_window = resolved.quarantine_started_window.unwrap_or(current_window);
    let last_quarantine_window = resolved
        .last_quarantine_window
        .or(resolved.quarantine_started_window)
        .unwrap_or(current_window);
    if quarantine_expired(policy, current_window, last_quarantine_window) {
        resolved.consecutive_rejections = 0;
        resolved.quarantined = false;
        resolved.last_rejection_reason = None;
        resolved.quarantine_started_window = None;
        resolved.last_quarantine_window = None;
        resolved.ban_recommended = false;
        return resolved;
    }

    resolved.quarantine_started_window = Some(quarantine_started_window);
    resolved.last_quarantine_window = Some(last_quarantine_window);
    resolved.ban_recommended =
        quarantine_ban_due(policy, current_window, quarantine_started_window);
    resolved
}

pub(crate) fn quarantine_expired(
    policy: &QuarantinePolicy,
    current_window: WindowId,
    last_quarantine_window: WindowId,
) -> bool {
    current_window.0
        >= last_quarantine_window.0 + u64::from(policy.quarantine_duration_windows.max(1))
}

pub(crate) fn quarantine_ban_due(
    policy: &QuarantinePolicy,
    current_window: WindowId,
    quarantine_started_window: WindowId,
) -> bool {
    current_window.0
        >= quarantine_started_window.0 + u64::from(policy.ban_after_quarantine_windows.max(1))
}
