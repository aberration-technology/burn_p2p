import BurnP2P.Protocol.Invariants

namespace BurnP2P.ModelCheck

open BurnP2P.Protocol

def smokeState : State :=
  { scope := {
      networkId := "formal-net"
      studyId := "formal-study"
      experimentId := "mnist"
      revisionId := "pre.8"
    }
    authorityEpoch := 1
    validatorSet := ["validator-1", "validator-2"]
    validatorQuorumSize := 2
    revokedPeers := []
    candidateUpdates := []
    reducerProposals := []
    acceptedCandidates := []
    rejectedCandidates := []
    validatorAttestations := []
    promotedHead := none
  }

example : promotedHeadRequiresValidatorQuorum smokeState := by
  simp [smokeState, promotedHeadRequiresValidatorQuorum]

example : revokedPeersExcluded smokeState := by
  intro update hUpdate
  simp [smokeState] at hUpdate

end BurnP2P.ModelCheck
