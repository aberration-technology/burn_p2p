import BurnP2P.Protocol.Robustness

namespace BurnP2P.Proofs

open BurnP2P.Protocol

theorem rejected_canary_blocks
    (policy : CanaryPolicy)
    (report : CanaryReport)
    (hRejected : report.accepted = false) :
    canaryBlocksPromotion policy report = true := by
  simp [canaryBlocksPromotion, hRejected]

theorem backdoor_trigger_blocks
    (policy : CanaryPolicy)
    (report : CanaryReport)
    (hTrigger : report.detectedBackdoorTrigger = true) :
    canaryBlocksPromotion policy report = true := by
  simp [canaryBlocksPromotion, hTrigger]

theorem excess_regression_blocks
    (policy : CanaryPolicy)
    (report : CanaryReport)
    (hMargin : report.regressionMargin > policy.maximumRegressionDelta) :
    canaryBlocksPromotion policy report = true := by
  simp [canaryBlocksPromotion, hMargin]

theorem insufficient_evaluator_quorum_blocks
    (policy : CanaryPolicy)
    (report : CanaryReport)
    (hQuorum : report.evaluatorQuorum < policy.minimumEvaluatorQuorum) :
    canaryBlocksPromotion policy report = true := by
  simp [canaryBlocksPromotion, hQuorum]

theorem regression_margin_monotonic
    (policy : CanaryPolicy)
    (report : CanaryReport)
    (newMargin : Nat)
    (hThreshold : report.regressionMargin > policy.maximumRegressionDelta)
    (hMonotone : report.regressionMargin ≤ newMargin) :
    canaryBlocksPromotion policy { report with regressionMargin := newMargin } = true := by
  have hNew : newMargin > policy.maximumRegressionDelta := Nat.lt_of_lt_of_le hThreshold hMonotone
  simp [canaryBlocksPromotion, hNew]

theorem evaluator_quorum_monotonic
    (policy : CanaryPolicy)
    (report : CanaryReport)
    (newQuorum : Nat)
    (hLow : report.evaluatorQuorum < policy.minimumEvaluatorQuorum)
    (hMonotone : newQuorum ≤ report.evaluatorQuorum) :
    canaryBlocksPromotion policy { report with evaluatorQuorum := newQuorum } = true := by
  have hNew : newQuorum < policy.minimumEvaluatorQuorum := Nat.lt_of_le_of_lt hMonotone hLow
  simp [canaryBlocksPromotion, hNew]

end BurnP2P.Proofs
