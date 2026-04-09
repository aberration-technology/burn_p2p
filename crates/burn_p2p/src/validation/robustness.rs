use super::*;

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct PeerRobustnessState {
    pub trust_score: f64,
    pub consecutive_rejections: u16,
    pub quarantined: bool,
    pub last_rejection_reason: Option<RejectionReason>,
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub quarantine_started_window: Option<WindowId>,
    #[serde(default)]
    pub last_quarantine_window: Option<WindowId>,
    #[serde(default)]
    pub ban_recommended: bool,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct PersistedRobustnessState {
    pub peers: BTreeMap<PeerId, PeerRobustnessState>,
}

#[derive(Clone, Debug)]
pub(super) struct ValidationRobustnessExecution {
    pub decisions: Vec<RobustnessDecision>,
    pub trust_scores: Vec<TrustScore>,
    pub cohort_report: CohortRobustnessReport,
    pub canary_report: Option<CanaryEvalReport>,
    pub replica_agreement: Option<f64>,
}

type AcceptedCandidateWeights = BTreeMap<(PeerId, ArtifactId), f64>;

pub(super) struct CandidateRobustnessOutcome {
    pub decisions: Vec<RobustnessDecision>,
    pub trust_scores: Vec<TrustScore>,
    pub filtered_updates: Vec<UpdateAnnounce>,
    pub accepted_weights: AcceptedCandidateWeights,
}

struct AcceptedCohortCandidate {
    update_index: usize,
    sketch: UpdateFeatureSketch,
}

pub(super) fn evaluate_candidate_robustness<M>(
    engine: &burn_p2p_security::RobustnessEngine,
    prepared: &ValidationPreparedState,
    candidate_models: &[ValidationCandidateView<'_, M>],
    evaluated_at: DateTime<Utc>,
) -> CandidateRobustnessOutcome {
    let mut seen_receipt_roots = BTreeSet::new();
    let mut seen_receipt_ids = BTreeSet::new();
    let mut seen_artifacts = BTreeSet::new();
    let browser_peers = browser_peer_ids_from_snapshots(&prepared.snapshots);
    let mut decisions = Vec::new();
    let mut accepted_candidates = Vec::new();
    let mut candidate_states = Vec::new();

    for (update_index, candidate) in candidate_models.iter().enumerate() {
        let update = candidate.update;
        let peer_state = prepared
            .robustness_state
            .peers
            .get(&update.peer_id)
            .map(|state| {
                resolved_peer_robustness_state(
                    &prepared.robustness_policy.quarantine_policy,
                    prepared.merge_window.window_id,
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
            expected_base_head_id: prepared.base_head_id.clone(),
            revision_id: update.revision_id.clone(),
            expected_revision_id: prepared.merge_window.revision_id.clone(),
            dataset_view_id: prepared.dataset_view_id.clone(),
            expected_dataset_view_id: prepared.dataset_view_id.clone(),
            lease_id: update.lease_id.clone(),
            expected_lease_id: update.lease_id.clone(),
            schema_matches: true,
            duplicate,
            quarantined: peer_state.quarantined,
            consecutive_rejections: peer_state.consecutive_rejections,
            current_trust_score: peer_state.trust_score,
            canary_report: Some(candidate.canary_report.clone()),
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
        &prepared.robustness_policy,
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
        prepared
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
            &prepared.robustness_policy.quarantine_policy,
            peer_state.consecutive_rejections,
            decision,
        );
        let trust_score =
            burn_p2p_security::updated_trust_score(burn_p2p_security::TrustUpdateInput {
                policy: &prepared.robustness_policy.reputation_policy,
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
                        prepared
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

pub(super) fn build_validation_canary_report(
    experiment: &ExperimentHandle,
    current_head: &Option<(PeerId, HeadDescriptor)>,
    candidate_head: &HeadDescriptor,
    evaluation: &MetricReport,
    maximum_regression_delta: f64,
    evaluator_quorum: u16,
) -> anyhow::Result<CanaryEvalReport> {
    let base_quality = current_head
        .as_ref()
        .map(|(_, head)| metric_quality(&head.metrics))
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

pub(super) fn append_quarantine_escalation_alerts(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    robustness: &mut ValidationRobustnessExecution,
    emitted_at: DateTime<Utc>,
) {
    let policy = &prepared.robustness_policy.escalation_policy;
    let resolved_previous = prepared
        .robustness_state
        .peers
        .iter()
        .map(|(peer_id, state)| {
            (
                peer_id.clone(),
                resolved_peer_robustness_state(
                    &prepared.robustness_policy.quarantine_policy,
                    prepared.merge_window.window_id,
                    state,
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let (next_state, _) = project_robustness_state(
        &prepared.robustness_state,
        &robustness.decisions,
        &prepared.robustness_policy.quarantine_policy,
        prepared.merge_window.window_id,
    );

    if policy.alert_on_quarantine {
        for (peer_id, _) in next_state
            .peers
            .iter()
            .filter(|(_, state)| state.quarantined)
        {
            let was_quarantined = resolved_previous
                .get(peer_id)
                .map(|state| state.quarantined)
                .unwrap_or(false);
            if was_quarantined {
                continue;
            }
            robustness.cohort_report.alerts.push(RobustnessAlert {
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: Some(prepared.merge_window.window_id),
                peer_id: Some(peer_id.clone()),
                reason: RejectionReason::QuarantinedPrincipal,
                severity: RobustnessAlertSeverity::Warn,
                message: "peer entered automatic quarantine after repeated suspicious updates"
                    .into(),
                emitted_at,
            });
        }
    }

    for (peer_id, _) in next_state
        .peers
        .iter()
        .filter(|(_, state)| state.quarantined && state.ban_recommended)
    {
        let previously_recommended = prepared
            .robustness_state
            .peers
            .get(peer_id)
            .map(|state| state.ban_recommended)
            .unwrap_or(false);
        if previously_recommended {
            continue;
        }
        robustness.cohort_report.alerts.push(RobustnessAlert {
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: Some(prepared.merge_window.window_id),
            peer_id: Some(peer_id.clone()),
            reason: RejectionReason::QuarantinedPrincipal,
            severity: RobustnessAlertSeverity::Critical,
            message:
                "peer remained under automatic quarantine long enough to recommend operator ban"
                    .into(),
            emitted_at,
        });
    }

    let effective_quarantine = next_state
        .peers
        .iter()
        .map(|(peer_id, state)| (peer_id.clone(), state.quarantined))
        .collect::<BTreeMap<_, _>>();
    let total_peers = effective_quarantine.len();
    if total_peers == 0 {
        return;
    }
    let quarantined_peers = effective_quarantine
        .values()
        .filter(|quarantined| **quarantined)
        .count();
    let quarantined_fraction = quarantined_peers as f64 / total_peers as f64;
    if quarantined_fraction > policy.maximum_quarantined_fraction {
        robustness.cohort_report.alerts.push(RobustnessAlert {
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: Some(prepared.merge_window.window_id),
            peer_id: None,
            reason: RejectionReason::QuarantinedPrincipal,
            severity: RobustnessAlertSeverity::Critical,
            message: format!(
                "quarantined peer fraction {:.2} exceeded configured maximum {:.2}",
                quarantined_fraction, policy.maximum_quarantined_fraction
            ),
            emitted_at,
        });
    }
}

pub(super) fn append_canary_escalation_alert(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    robustness: &mut ValidationRobustnessExecution,
    canary_report: &CanaryEvalReport,
    emitted_at: DateTime<Utc>,
) {
    if !prepared
        .robustness_policy
        .escalation_policy
        .pause_on_canary_regression
    {
        return;
    }
    let policy = &prepared.robustness_policy.validator_canary_policy;
    let blocked = !canary_report.accepted
        || canary_report.detected_backdoor_trigger
        || canary_report.regression_margin > policy.maximum_regression_delta
        || canary_report.evaluator_quorum
            < super::effective_canary_minimum_evaluator_quorum(prepared);
    if !blocked {
        return;
    }

    robustness.cohort_report.alerts.push(RobustnessAlert {
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: Some(prepared.merge_window.window_id),
        peer_id: None,
        reason: RejectionReason::CanaryRegression,
        severity: RobustnessAlertSeverity::Critical,
        message: format!(
            "candidate promotion paused by canary regression {:.4}",
            canary_report.regression_margin
        ),
        emitted_at,
    });
}

pub(super) fn append_replica_disagreement_alert(
    experiment: &ExperimentHandle,
    prepared: &ValidationPreparedState,
    robustness: &mut ValidationRobustnessExecution,
    emitted_at: DateTime<Utc>,
) {
    let Some(replica_agreement) = robustness.replica_agreement else {
        return;
    };
    if replica_agreement >= 0.999 {
        return;
    }

    robustness.cohort_report.alerts.push(RobustnessAlert {
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: Some(prepared.merge_window.window_id),
        peer_id: None,
        reason: RejectionReason::ReplicaDisagreement,
        severity: if prepared
            .robustness_policy
            .escalation_policy
            .pause_on_replica_disagreement
        {
            RobustnessAlertSeverity::Critical
        } else {
            RobustnessAlertSeverity::Warn
        },
        message: format!(
            "reducer replica agreement {:.2} fell below unanimity for the active cohort",
            replica_agreement
        ),
        emitted_at,
    });
}

pub(super) fn observed_replica_agreement(
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
    merge_window: &MergeWindowState,
    base_head_id: &HeadId,
) -> Option<f64> {
    let reducer_set = merge_window
        .reducers
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut latest = BTreeMap::<PeerId, AggregateEnvelope>::new();
    for (peer_id, snapshot) in snapshots {
        if !reducer_set.contains(peer_id) {
            continue;
        }
        let candidate = snapshot
            .aggregate_proposal_announcements
            .iter()
            .filter(|announcement| {
                let aggregate = &announcement.proposal;
                aggregate.study_id == experiment.study_id
                    && aggregate.experiment_id == experiment.experiment_id
                    && aggregate.revision_id == experiment.revision_id
                    && aggregate.window_id == merge_window.window_id
                    && &aggregate.base_head_id == base_head_id
                    && &aggregate.reducer_peer_id == peer_id
            })
            .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
            .map(|announcement| announcement.proposal.clone());
        if let Some(candidate) = candidate {
            latest.insert(peer_id.clone(), candidate);
        }
    }
    if latest.len() < 2 {
        return None;
    }

    let mut fingerprints = BTreeMap::<String, usize>::new();
    for aggregate in latest.values() {
        let mut contributors = aggregate
            .contributor_peers
            .iter()
            .map(|peer| peer.as_str().to_owned())
            .collect::<Vec<_>>();
        contributors.sort();
        let mut providers = aggregate
            .providers
            .iter()
            .map(|peer| peer.as_str().to_owned())
            .collect::<Vec<_>>();
        providers.sort();
        let fingerprint = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}",
            aggregate.aggregate_artifact_id.as_str(),
            aggregate.stats.accepted_updates,
            aggregate.stats.duplicate_updates,
            aggregate.stats.dropped_updates,
            aggregate.stats.late_updates,
            aggregate.stats.sum_sample_weight.to_bits(),
            contributors.join(","),
            providers.join(","),
        );
        *fingerprints.entry(fingerprint).or_default() += 1;
    }

    let agreeing = fingerprints.values().copied().max().unwrap_or(0);
    Some(agreeing as f64 / latest.len() as f64)
}

pub(super) fn persist_validation_robustness_state(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    previous: &PersistedRobustnessState,
    decisions: &[RobustnessDecision],
    policy: &QuarantinePolicy,
    current_window: WindowId,
) -> anyhow::Result<Vec<TrustScore>> {
    let (next, trust_scores) =
        project_robustness_state(previous, decisions, policy, current_window);
    persist_json(storage.scoped_robustness_state_path(experiment), &next)?;
    Ok(trust_scores)
}

pub(super) fn project_robustness_state(
    previous: &PersistedRobustnessState,
    decisions: &[RobustnessDecision],
    policy: &QuarantinePolicy,
    current_window: WindowId,
) -> (PersistedRobustnessState, Vec<TrustScore>) {
    let mut next = previous.clone();
    let mut trust_scores = Vec::new();
    for peer_state in next.peers.values_mut() {
        *peer_state = resolved_peer_robustness_state(policy, current_window, peer_state);
    }

    for decision in decisions {
        let peer_state = next.peers.entry(decision.peer_id.clone()).or_default();
        let was_quarantined = peer_state.quarantined;
        if decision.accepted {
            peer_state.consecutive_rejections = 0;
        } else {
            peer_state.consecutive_rejections = peer_state.consecutive_rejections.saturating_add(1);
        }
        if let Some(trust) = decision.trust_score.clone() {
            peer_state.trust_score = trust.score;
            peer_state.quarantined = trust.quarantined;
            peer_state.updated_at = Some(trust.updated_at);
            let mut trust = trust;
            if trust.quarantined {
                if !was_quarantined || peer_state.quarantine_started_window.is_none() {
                    peer_state.quarantine_started_window = Some(current_window);
                }
                peer_state.last_quarantine_window = Some(current_window);
                let quarantine_started_window = peer_state
                    .quarantine_started_window
                    .unwrap_or(current_window);
                peer_state.ban_recommended =
                    quarantine_ban_due(policy, current_window, quarantine_started_window);
            } else {
                peer_state.quarantine_started_window = None;
                peer_state.last_quarantine_window = None;
                peer_state.ban_recommended = false;
            }
            trust.ban_recommended = peer_state.ban_recommended;
            trust_scores.push(trust);
        }
        peer_state.last_rejection_reason = decision.rejection_reason.clone();
    }

    (next, trust_scores)
}

fn resolved_peer_robustness_state(
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

fn quarantine_expired(
    policy: &QuarantinePolicy,
    current_window: WindowId,
    last_quarantine_window: WindowId,
) -> bool {
    current_window.0
        >= last_quarantine_window.0 + u64::from(policy.quarantine_duration_windows.max(1))
}

fn quarantine_ban_due(
    policy: &QuarantinePolicy,
    current_window: WindowId,
    quarantine_started_window: WindowId,
) -> bool {
    current_window.0
        >= quarantine_started_window.0 + u64::from(policy.ban_after_quarantine_windows.max(1))
}
