use super::*;
use crate::candidate_screening::{
    PeerRobustnessState, PersistedRobustnessState, quarantine_ban_due,
    resolved_peer_robustness_state,
};

#[derive(Clone, Debug)]
pub(crate) struct ValidationRobustnessExecution {
    pub decisions: Vec<RobustnessDecision>,
    pub trust_scores: Vec<TrustScore>,
    pub cohort_report: CohortRobustnessReport,
    pub canary_report: Option<CanaryEvalReport>,
    pub replica_agreement: Option<f64>,
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
        &prepared.robustness_policy.reputation_policy,
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
    reputation_policy: &burn_p2p_core::ReputationPolicy,
    policy: &QuarantinePolicy,
    current_window: WindowId,
) -> anyhow::Result<Vec<TrustScore>> {
    let (next, trust_scores) = project_robustness_state(
        previous,
        decisions,
        reputation_policy,
        policy,
        current_window,
    );
    persist_json(storage.scoped_robustness_state_path(experiment), &next)?;
    Ok(trust_scores)
}

pub(super) fn project_robustness_state(
    previous: &PersistedRobustnessState,
    decisions: &[RobustnessDecision],
    reputation_policy: &burn_p2p_core::ReputationPolicy,
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
        if peer_state.last_decision_window == Some(current_window) {
            trust_scores.push(materialize_peer_trust_score(
                reputation_policy,
                &decision.peer_id,
                peer_state,
                decision
                    .trust_score
                    .as_ref()
                    .map(|trust| trust.updated_at)
                    .unwrap_or_else(Utc::now),
            ));
            continue;
        }
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
        peer_state.last_decision_window = Some(current_window);
        peer_state.last_rejection_reason = decision.rejection_reason.clone();
    }

    (next, trust_scores)
}

fn materialize_peer_trust_score(
    policy: &burn_p2p_core::ReputationPolicy,
    peer_id: &PeerId,
    state: &PeerRobustnessState,
    updated_at: DateTime<Utc>,
) -> TrustScore {
    TrustScore {
        peer_id: peer_id.clone(),
        score: state.trust_score,
        reducer_eligible: state.trust_score >= policy.reducer_demote_threshold,
        validator_eligible: state.trust_score >= policy.validator_demote_threshold,
        quarantined: state.quarantined,
        ban_recommended: state.ban_recommended,
        updated_at: state.updated_at.unwrap_or(updated_at),
    }
}
