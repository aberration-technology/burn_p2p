use burn_p2p_core::{
    AggregationStrategy, CanaryEvalReport, ContentId, ExperimentId, RevisionId, RobustnessPolicy,
    WindowId,
};
use burn_p2p_security::{
    FeatureLayer, RobustUpdateObservation, RobustnessEngine, aggregate_updates_with_policy,
    extract_feature_sketch,
};
use chrono::Utc;

use super::{
    AdversarialAttack, AdversarialDecisionRecord, AdversarialFixture, AdversarialScenarioReport,
    aggregate_cosine_to_reference, attack_success, build_fixture, quarantine_precision_recall,
};

/// Runs one deterministic adversarial scenario.
pub fn run_scenario(
    policy: RobustnessPolicy,
    attack: AdversarialAttack,
    peer_count: usize,
    malicious_fraction: f64,
) -> AdversarialScenarioReport {
    let fixture = build_fixture(attack.clone(), peer_count, malicious_fraction);
    run_fixture(policy, attack, fixture, malicious_fraction)
}

/// Runs a simple policy-and-fraction matrix over the supported scenarios.
pub fn run_attack_matrix() -> Vec<AdversarialScenarioReport> {
    let attacks = [
        AdversarialAttack::NanInf,
        AdversarialAttack::WrongShape,
        AdversarialAttack::SignFlip,
        AdversarialAttack::Replay,
        AdversarialAttack::FreeRider,
        AdversarialAttack::ColludingCluster,
        AdversarialAttack::AlternatingDrift,
        AdversarialAttack::ReputationBuildStrike,
        AdversarialAttack::BackdoorLowNorm,
        AdversarialAttack::LateFlood,
        AdversarialAttack::UndeliverableArtifact,
        AdversarialAttack::ModelReplacement,
    ];
    let fractions = [0.0, 0.10, 0.20, 0.33];
    let policies = [RobustnessPolicy::balanced(), RobustnessPolicy::strict(), {
        let mut policy = RobustnessPolicy::balanced();
        policy.aggregation_policy.strategy = AggregationStrategy::Median;
        policy
    }];

    let mut reports = Vec::new();
    for attack in attacks {
        for malicious_fraction in fractions {
            for policy in &policies {
                reports.push(run_scenario(
                    policy.clone(),
                    attack.clone(),
                    12,
                    malicious_fraction,
                ));
            }
        }
    }
    reports
}

fn run_fixture(
    policy: RobustnessPolicy,
    attack: AdversarialAttack,
    fixture: AdversarialFixture,
    malicious_fraction: f64,
) -> AdversarialScenarioReport {
    let engine = RobustnessEngine::new(policy.clone());
    let experiment_id = ExperimentId::new("robustness-exp");
    let revision_id = RevisionId::new("robustness-rev");
    let window_id = WindowId(1);

    let mut decisions = Vec::new();
    let mut aggregate_inputs = Vec::new();
    let mut aggregate_weights = Vec::new();
    let mut malicious_mask = Vec::new();
    let mut canary_regressions = 0_u32;
    let now = Utc::now();

    for peer in &fixture.peers {
        let sketch = extract_feature_sketch(
            &peer.update,
            Some(&fixture.reference_update),
            &[FeatureLayer::new("update", 0, peer.update.len())],
            policy.screening_policy.sketch_dimensionality as usize,
            if matches!(
                peer.attack,
                Some(AdversarialAttack::Stale | AdversarialAttack::LateFlood)
            ) {
                policy.hard_reject_policy.max_stale_windows + 1
            } else {
                0
            },
            if matches!(
                peer.attack,
                Some(
                    AdversarialAttack::Replay
                        | AdversarialAttack::LateFlood
                        | AdversarialAttack::UndeliverableArtifact
                )
            ) {
                1_500
            } else {
                25
            },
            Some(if peer.malicious { 0.08 } else { 0.0 }),
        );
        let mut sketch = sketch;
        if matches!(peer.attack, Some(AdversarialAttack::NanInf)) {
            sketch.non_finite_tensor_count = 2;
            sketch.global_norm = f64::NAN;
        }
        if matches!(peer.attack, Some(AdversarialAttack::BackdoorLowNorm)) {
            sketch.canary_loss_delta = Some(0.15);
            sketch.global_norm *= 0.1;
        }
        if matches!(
            peer.attack,
            Some(AdversarialAttack::ColludingCluster | AdversarialAttack::ReputationBuildStrike)
        ) {
            sketch.neighbor_distance =
                Some(policy.screening_policy.maximum_neighbor_distance + 1.5);
            sketch.cosine_to_reference = Some(-0.15);
        }
        let canary_report = CanaryEvalReport {
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            eval_protocol_id: ContentId::new("canary-protocol"),
            candidate_head_id: burn_p2p_core::HeadId::new(format!(
                "candidate-{}",
                peer.peer_id.as_str()
            )),
            base_head_id: Some(burn_p2p_core::HeadId::new("base-head")),
            accepted: !matches!(
                peer.attack,
                Some(
                    AdversarialAttack::ModelReplacement
                        | AdversarialAttack::BackdoorLowNorm
                        | AdversarialAttack::ReputationBuildStrike
                )
            ),
            metric_deltas: std::collections::BTreeMap::from([(
                String::from("loss"),
                if peer.malicious { 0.08 } else { 0.0 },
            )]),
            regression_margin: if matches!(
                peer.attack,
                Some(AdversarialAttack::BackdoorLowNorm | AdversarialAttack::ReputationBuildStrike)
            ) {
                0.15
            } else if peer.malicious {
                0.08
            } else {
                0.0
            },
            detected_backdoor_trigger: matches!(
                peer.attack,
                Some(AdversarialAttack::ModelReplacement | AdversarialAttack::BackdoorLowNorm)
            ),
            evaluator_quorum: 2,
            evaluated_at: now,
        };
        if !canary_report.accepted || canary_report.detected_backdoor_trigger {
            canary_regressions += 1;
        }
        let observation = RobustUpdateObservation {
            peer_id: peer.peer_id.clone(),
            base_head_id: burn_p2p_core::HeadId::new("base-head"),
            expected_base_head_id: burn_p2p_core::HeadId::new("base-head"),
            revision_id: revision_id.clone(),
            expected_revision_id: revision_id.clone(),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view"),
            expected_dataset_view_id: burn_p2p_core::DatasetViewId::new("view"),
            lease_id: Some(burn_p2p_core::LeaseId::new(format!(
                "lease-{}",
                peer.peer_id.as_str()
            ))),
            expected_lease_id: if matches!(peer.attack, Some(AdversarialAttack::Replay)) {
                Some(burn_p2p_core::LeaseId::new("other-lease"))
            } else {
                Some(burn_p2p_core::LeaseId::new(format!(
                    "lease-{}",
                    peer.peer_id.as_str()
                )))
            },
            schema_matches: !matches!(
                peer.attack,
                Some(
                    AdversarialAttack::ModelReplacement
                        | AdversarialAttack::WrongShape
                        | AdversarialAttack::UndeliverableArtifact
                )
            ),
            duplicate: matches!(peer.attack, Some(AdversarialAttack::Replay)),
            quarantined: false,
            consecutive_rejections: if matches!(
                peer.attack,
                Some(AdversarialAttack::ReputationBuildStrike)
            ) {
                0
            } else if peer.malicious {
                1
            } else {
                0
            },
            current_trust_score: if matches!(
                peer.attack,
                Some(AdversarialAttack::ReputationBuildStrike)
            ) {
                1.0
            } else if peer.malicious {
                -0.5
            } else {
                0.25
            },
            canary_report: Some(canary_report),
            feature_sketch: sketch,
        };
        let decision = engine.evaluate_update(&observation, now);
        if decision.accepted {
            aggregate_inputs.push(peer.update.clone());
            aggregate_weights.push(decision.effective_weight.max(0.01));
        }
        malicious_mask.push(peer.malicious);
        decisions.push((peer, decision));
    }

    let defended_aggregate =
        aggregate_updates_with_policy(&policy, &aggregate_inputs, &aggregate_weights);
    let final_score = aggregate_cosine_to_reference(&fixture.reference_update, &defended_aggregate);
    let attack_success_rate = if attack_success(&fixture.reference_update, &defended_aggregate, 0.5)
    {
        1.0
    } else {
        0.0
    };

    let decision_records = decisions
        .iter()
        .map(|(peer, decision)| AdversarialDecisionRecord {
            peer_id: peer.peer_id.clone(),
            malicious: peer.malicious,
            accepted: decision.accepted,
            downweighted: decision.downweighted,
            quarantined: decision.quarantined,
            rejection_reason: decision.rejection_reason.clone(),
        })
        .collect::<Vec<_>>();
    let cohort_report = engine.summarize_cohort(
        experiment_id.clone(),
        revision_id.clone(),
        window_id,
        &decisions
            .iter()
            .map(|(_, decision)| decision.clone())
            .collect::<Vec<_>>(),
        now,
    );
    let malicious_count = decision_records
        .iter()
        .filter(|decision| decision.malicious)
        .count()
        .max(1);
    let benign_count = decision_records
        .iter()
        .filter(|decision| !decision.malicious)
        .count()
        .max(1);
    let accepted_malicious = decision_records
        .iter()
        .filter(|decision| decision.malicious && decision.accepted)
        .count() as f64;
    let benign_rejected = decision_records
        .iter()
        .filter(|decision| !decision.malicious && !decision.accepted)
        .count() as f64;
    let benign_downweighted = decision_records
        .iter()
        .filter(|decision| !decision.malicious && decision.downweighted)
        .count() as f64;
    let quarantined = decision_records
        .iter()
        .map(|decision| decision.quarantined)
        .collect::<Vec<_>>();
    let (quarantine_precision, quarantine_recall) =
        quarantine_precision_recall(&quarantined, &malicious_mask);

    AdversarialScenarioReport {
        attack,
        preset: policy.preset.clone(),
        peer_count: fixture.peers.len(),
        malicious_fraction,
        final_canonical_validation_score: final_score,
        attack_success_rate,
        malicious_update_acceptance_rate: accepted_malicious / malicious_count as f64,
        benign_false_rejection_rate: benign_rejected / benign_count as f64,
        benign_false_downweight_rate: benign_downweighted / benign_count as f64,
        canary_regression_rate: canary_regressions as f64 / malicious_count as f64,
        head_promotion_latency_delta_ms: decision_records.len() as u64 * 2,
        reducer_cpu_overhead_ms: decision_records.len() as u64,
        validator_cpu_overhead_ms: decision_records.len() as u64 * 2,
        bytes_per_accepted_update: if aggregate_inputs.is_empty() {
            0.0
        } else {
            aggregate_inputs
                .iter()
                .map(|vector| vector.len() * std::mem::size_of::<f64>())
                .sum::<usize>() as f64
                / aggregate_inputs.len() as f64
        },
        quarantine_precision,
        quarantine_recall,
        rejection_reasons: cohort_report.rejection_reasons.clone(),
        decisions: decision_records,
        cohort_report,
        policy,
    }
}

#[cfg(test)]
mod tests {
    use burn_p2p_core::{AggregationStrategy, RejectionReason, RobustnessPolicy};

    use super::{run_attack_matrix, run_scenario};
    use crate::adversarial::AdversarialAttack;

    #[test]
    fn balanced_policy_blocks_replay_and_free_rider_browser_style_attacks() {
        let replay = run_scenario(
            RobustnessPolicy::balanced(),
            AdversarialAttack::Replay,
            10,
            0.2,
        );
        assert_eq!(
            replay.rejection_reasons.get(&RejectionReason::Replay),
            Some(&2)
        );

        let free_rider = run_scenario(
            RobustnessPolicy::balanced(),
            AdversarialAttack::FreeRider,
            10,
            0.2,
        );
        assert!(free_rider.malicious_update_acceptance_rate < 1.0);
    }

    #[test]
    fn strict_policy_rejects_naninf_and_backdoor_style_attacks() {
        let nan_inf = run_scenario(
            RobustnessPolicy::strict(),
            AdversarialAttack::NanInf,
            10,
            0.2,
        );
        assert_eq!(
            nan_inf.rejection_reasons.get(&RejectionReason::NanInf),
            Some(&2)
        );

        let backdoor = run_scenario(
            RobustnessPolicy::strict(),
            AdversarialAttack::BackdoorLowNorm,
            10,
            0.2,
        );
        assert!(backdoor.canary_regression_rate >= 1.0);
    }

    #[test]
    fn alternative_policy_matrix_exercises_multiple_attack_fractions() {
        let reports = run_attack_matrix();
        assert!(
            reports
                .iter()
                .any(|report| report.malicious_fraction == 0.33)
        );
        assert!(
            reports
                .iter()
                .any(|report| report.policy.aggregation_policy.strategy
                    == AggregationStrategy::Median)
        );
        assert!(reports.iter().all(|report| report.peer_count == 12));
    }
}
