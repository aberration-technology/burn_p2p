use std::collections::BTreeMap;

use burn_p2p_core::{
    CanaryEvalReport, CohortRobustnessReport, DatasetViewId, ExperimentId, HeadId, LeaseId, PeerId,
    RejectionReason, RevisionId, RobustnessAlert, RobustnessAlertSeverity, RobustnessDecision,
    RobustnessPolicy, UpdateFeatureSketch, WindowId,
};
use chrono::{DateTime, Utc};

use super::{
    quarantine::should_quarantine,
    reputation::{TrustUpdateInput, updated_trust_score},
    screen::{effective_norm, hard_rejection_reason, screening_score},
};

#[derive(Clone, Debug, PartialEq)]
/// One update observation evaluated by the robustness engine.
pub struct RobustUpdateObservation {
    /// Peer that produced the update.
    pub peer_id: PeerId,
    /// Observed base head identifier.
    pub base_head_id: HeadId,
    /// Expected base head identifier.
    pub expected_base_head_id: HeadId,
    /// Observed revision identifier.
    pub revision_id: RevisionId,
    /// Expected revision identifier.
    pub expected_revision_id: RevisionId,
    /// Observed dataset-view identifier.
    pub dataset_view_id: DatasetViewId,
    /// Expected dataset-view identifier.
    pub expected_dataset_view_id: DatasetViewId,
    /// Observed lease identifier.
    pub lease_id: Option<LeaseId>,
    /// Expected lease identifier.
    pub expected_lease_id: Option<LeaseId>,
    /// Whether schema and artifact compatibility checks passed.
    pub schema_matches: bool,
    /// Whether the update is a duplicate or replay.
    pub duplicate: bool,
    /// Whether the peer is already quarantined.
    pub quarantined: bool,
    /// Consecutive rejection streak entering this decision.
    pub consecutive_rejections: u16,
    /// Current trust score before this decision.
    pub current_trust_score: f64,
    /// Optional canary evaluation report attached to the candidate head.
    pub canary_report: Option<CanaryEvalReport>,
    /// Feature sketch used for screening.
    pub feature_sketch: UpdateFeatureSketch,
}

#[derive(Clone, Debug, PartialEq)]
/// Robustness engine bound to one policy.
pub struct RobustnessEngine {
    /// Active robustness policy.
    pub policy: RobustnessPolicy,
}

#[derive(Clone, Debug)]
struct DecisionSeed {
    accepted: bool,
    hard_rejected: bool,
    downweighted: bool,
    screen_score: f64,
    effective_norm: f64,
    rejection_reason: Option<RejectionReason>,
}

impl RobustnessEngine {
    /// Creates a robustness engine for one active policy.
    pub fn new(policy: RobustnessPolicy) -> Self {
        Self { policy }
    }

    /// Evaluates one update observation and returns a deterministic decision.
    pub fn evaluate_update(
        &self,
        observation: &RobustUpdateObservation,
        evaluated_at: DateTime<Utc>,
    ) -> RobustnessDecision {
        if let Some(reason) = hard_rejection_reason(&self.policy, observation) {
            return self.finalize_decision(
                observation,
                evaluated_at,
                DecisionSeed {
                    accepted: false,
                    hard_rejected: true,
                    downweighted: false,
                    screen_score: 0.0,
                    effective_norm: 0.0,
                    rejection_reason: Some(reason),
                },
            );
        }

        let norm = effective_norm(&self.policy.clipping_policy, observation);
        if norm < self.policy.clipping_policy.minimum_useful_norm {
            return self.finalize_decision(
                observation,
                evaluated_at,
                DecisionSeed {
                    accepted: false,
                    hard_rejected: true,
                    downweighted: false,
                    screen_score: 0.0,
                    effective_norm: norm,
                    rejection_reason: Some(RejectionReason::NormTooSmall),
                },
            );
        }

        let score = screening_score(&self.policy, observation);
        if score >= self.policy.screening_policy.hard_reject_score {
            return self.finalize_decision(
                observation,
                evaluated_at,
                DecisionSeed {
                    accepted: false,
                    hard_rejected: false,
                    downweighted: false,
                    screen_score: score,
                    effective_norm: norm,
                    rejection_reason: Some(RejectionReason::SimilarityOutlier),
                },
            );
        }

        let downweighted = score >= self.policy.screening_policy.downweight_score;
        let trust_budget = (observation.current_trust_score + 1.0).clamp(0.1, 1.5);
        let mut effective_weight = (norm / self.policy.clipping_policy.global_norm_cap.max(norm))
            .clamp(0.0, 1.0)
            * trust_budget;
        if downweighted {
            effective_weight *= 0.5;
        }
        effective_weight = effective_weight.min(self.policy.clipping_policy.max_effective_weight);
        effective_weight = effective_weight.min(self.policy.aggregation_policy.max_peer_weight);

        self.finalize_decision(
            observation,
            evaluated_at,
            DecisionSeed {
                accepted: true,
                hard_rejected: false,
                downweighted,
                screen_score: score,
                effective_norm: norm,
                rejection_reason: None,
            },
        )
        .with_effective_weight(effective_weight)
    }

    /// Evaluates whether a canary report blocks promotion.
    pub fn candidate_blocked_by_canary(&self, report: &CanaryEvalReport) -> bool {
        if !self.policy.validator_canary_policy.block_on_regression
            || !self.policy.escalation_policy.pause_on_canary_regression
        {
            return false;
        }
        !report.accepted
            || report.detected_backdoor_trigger
            || report.regression_margin
                > self.policy.validator_canary_policy.maximum_regression_delta
            || report.evaluator_quorum
                < self.policy.validator_canary_policy.minimum_evaluator_quorum
    }

    /// Builds a cohort-level report from per-update decisions.
    pub fn summarize_cohort(
        &self,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
        window_id: WindowId,
        decisions: &[RobustnessDecision],
        emitted_at: DateTime<Utc>,
    ) -> CohortRobustnessReport {
        let mut rejection_reasons = BTreeMap::new();
        let mut alerts = Vec::new();
        let mut accepted_updates = 0_u32;
        let mut rejected_updates = 0_u32;
        let mut downweighted_updates = 0_u32;
        let mut effective_weight_sum = 0.0_f64;
        let mut score_sum = 0.0_f64;

        for decision in decisions {
            score_sum += decision.screen_score;
            effective_weight_sum += decision.effective_weight;
            if decision.accepted {
                accepted_updates += 1;
                if decision.downweighted {
                    downweighted_updates += 1;
                }
            } else {
                rejected_updates += 1;
                if let Some(reason) = decision.rejection_reason.clone() {
                    *rejection_reasons.entry(reason.clone()).or_insert(0) += 1;
                    alerts.push(RobustnessAlert {
                        experiment_id: experiment_id.clone(),
                        revision_id: revision_id.clone(),
                        window_id: Some(window_id),
                        peer_id: Some(decision.peer_id.clone()),
                        reason,
                        severity: RobustnessAlertSeverity::Warn,
                        message: "update rejected by robustness policy".into(),
                        emitted_at,
                    });
                }
            }
        }

        CohortRobustnessReport {
            experiment_id,
            revision_id,
            window_id,
            cohort_filter_strategy: self.policy.cohort_filter_policy.strategy.clone(),
            aggregation_strategy: self.policy.aggregation_policy.strategy.clone(),
            total_updates: decisions.len() as u32,
            accepted_updates,
            rejected_updates,
            downweighted_updates,
            effective_weight_sum,
            mean_screen_score: if decisions.is_empty() {
                0.0
            } else {
                score_sum / decisions.len() as f64
            },
            rejection_reasons,
            alerts,
        }
    }

    fn finalize_decision(
        &self,
        observation: &RobustUpdateObservation,
        evaluated_at: DateTime<Utc>,
        seed: DecisionSeed,
    ) -> RobustnessDecision {
        let provisional = RobustnessDecision {
            peer_id: observation.peer_id.clone(),
            accepted: seed.accepted,
            hard_rejected: seed.hard_rejected,
            downweighted: seed.downweighted,
            quarantined: false,
            rejection_reason: seed.rejection_reason.clone(),
            screen_score: seed.screen_score,
            effective_weight: if seed.accepted { 1.0 } else { 0.0 },
            effective_norm: seed.effective_norm,
            trust_score: None,
        };
        let quarantined = should_quarantine(
            &self.policy.quarantine_policy,
            observation.consecutive_rejections,
            &provisional,
        );
        let trust_score = updated_trust_score(TrustUpdateInput {
            policy: &self.policy.reputation_policy,
            peer_id: observation.peer_id.clone(),
            current_score: observation.current_trust_score,
            accepted: seed.accepted,
            downweighted: seed.downweighted,
            screen_score: seed.screen_score,
            rejection_reason: seed.rejection_reason.as_ref(),
            quarantined,
            updated_at: evaluated_at,
        });

        RobustnessDecision {
            quarantined,
            trust_score: Some(trust_score),
            ..provisional
        }
    }
}

trait RobustnessDecisionExt {
    fn with_effective_weight(self, effective_weight: f64) -> Self;
}

impl RobustnessDecisionExt for RobustnessDecision {
    fn with_effective_weight(mut self, effective_weight: f64) -> Self {
        self.effective_weight = effective_weight;
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use burn_p2p_core::{CanaryEvalReport, ContentId, RejectionReason, RobustnessPolicy};
    use chrono::Utc;

    use crate::robust::{FeatureLayer, RobustnessEngine, extract_feature_sketch};

    use super::RobustUpdateObservation;

    fn baseline_observation() -> RobustUpdateObservation {
        RobustUpdateObservation {
            peer_id: burn_p2p_core::PeerId::new("peer-a"),
            base_head_id: burn_p2p_core::HeadId::new("head-1"),
            expected_base_head_id: burn_p2p_core::HeadId::new("head-1"),
            revision_id: burn_p2p_core::RevisionId::new("rev-1"),
            expected_revision_id: burn_p2p_core::RevisionId::new("rev-1"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view-1"),
            expected_dataset_view_id: burn_p2p_core::DatasetViewId::new("view-1"),
            lease_id: Some(burn_p2p_core::LeaseId::new("lease-1")),
            expected_lease_id: Some(burn_p2p_core::LeaseId::new("lease-1")),
            schema_matches: true,
            duplicate: false,
            quarantined: false,
            consecutive_rejections: 0,
            current_trust_score: 0.0,
            canary_report: None,
            feature_sketch: extract_feature_sketch(
                &[1.0, 0.9, -1.1, 0.4],
                Some(&[1.0, 1.0, -1.0, 0.5]),
                &[FeatureLayer::new("layer", 0, 4)],
                4,
                0,
                10,
                Some(0.0),
            ),
        }
    }

    #[test]
    fn engine_accepts_clean_update_and_rejects_replay() {
        let engine = RobustnessEngine::new(RobustnessPolicy::balanced());
        let accepted = engine.evaluate_update(&baseline_observation(), Utc::now());
        assert!(accepted.accepted);
        assert!(accepted.effective_weight > 0.0);

        let mut replay = baseline_observation();
        replay.duplicate = true;
        let rejected = engine.evaluate_update(&replay, Utc::now());
        assert_eq!(rejected.rejection_reason, Some(RejectionReason::Replay));
    }

    #[test]
    fn engine_rejects_similarity_outlier_under_strict_policy() {
        let engine = RobustnessEngine::new(RobustnessPolicy::strict());
        let mut suspicious = baseline_observation();
        suspicious.consecutive_rejections = 1;
        suspicious.feature_sketch.cosine_to_reference = Some(-0.5);
        suspicious.feature_sketch.sign_agreement_fraction = Some(0.0);
        suspicious.feature_sketch.neighbor_distance = Some(6.0);

        let decision = engine.evaluate_update(&suspicious, Utc::now());
        assert_eq!(
            decision.rejection_reason,
            Some(RejectionReason::SimilarityOutlier)
        );
        assert!(decision.quarantined);
    }

    #[test]
    fn canary_blocking_respects_escalation_pause_flag() {
        let report = CanaryEvalReport {
            experiment_id: burn_p2p_core::ExperimentId::new("exp-a"),
            revision_id: burn_p2p_core::RevisionId::new("rev-a"),
            eval_protocol_id: ContentId::new("eval-protocol"),
            candidate_head_id: burn_p2p_core::HeadId::new("head-candidate"),
            base_head_id: Some(burn_p2p_core::HeadId::new("head-base")),
            accepted: false,
            metric_deltas: BTreeMap::new(),
            regression_margin: 0.25,
            detected_backdoor_trigger: false,
            evaluator_quorum: 1,
            evaluated_at: Utc::now(),
        };

        let strict = RobustnessEngine::new(RobustnessPolicy::strict());
        assert!(strict.candidate_blocked_by_canary(&report));

        let mut relaxed = RobustnessPolicy::strict();
        relaxed.escalation_policy.pause_on_canary_regression = false;
        let relaxed = RobustnessEngine::new(relaxed);
        assert!(!relaxed.candidate_blocked_by_canary(&report));
    }
}
