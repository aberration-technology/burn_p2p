namespace BurnP2P.Protocol

structure CanaryPolicy where
  minimumEvaluatorQuorum : Nat
  maximumRegressionDelta : Nat
deriving Repr, DecidableEq

structure CanaryReport where
  accepted : Bool
  detectedBackdoorTrigger : Bool
  regressionMargin : Nat
  evaluatorQuorum : Nat
deriving Repr, DecidableEq

def canaryBlocksPromotion (policy : CanaryPolicy) (report : CanaryReport) : Bool :=
  (!report.accepted) ||
    report.detectedBackdoorTrigger ||
    report.regressionMargin > policy.maximumRegressionDelta ||
    report.evaluatorQuorum < policy.minimumEvaluatorQuorum

end BurnP2P.Protocol
