import BurnP2P.Protocol.Semantics

namespace BurnP2P.Proofs

open BurnP2P.Protocol

theorem quorum_required_for_promotion
    (s : State)
    (hInvariant : promotedHeadRequiresValidatorQuorum s)
    (hPromoted : s.promotedHead ≠ none) :
    hasValidatorQuorum s := by
  unfold promotedHeadRequiresValidatorQuorum at hInvariant
  cases hHead : s.promotedHead with
  | none =>
      contradiction
  | some _ =>
      simp [hHead] at hInvariant
      exact hInvariant

theorem reducer_non_authority
    (s : State)
    (hInvariant : reducerNonAuthority s)
    (hPromoted : s.promotedHead.isSome) :
    hasValidatorQuorum s := by
  exact hInvariant hPromoted

theorem publish_reducer_proposal_does_not_promote_head
    (s : State)
    (peerId : PeerId)
    (windowId : WindowId)
    (baseHeadId : HeadId)
    (artifactId : ArtifactId) :
    (step s (.publishReducerProposal peerId windowId baseHeadId artifactId)).promotedHead = s.promotedHead := by
  simp [step]

theorem revoked_peer_update_is_ignored
    (s : State)
    (peerId : PeerId)
    (windowId : WindowId)
    (baseHeadId : HeadId)
    (artifactId : ArtifactId)
    (hRevoked : peerId ∈ s.revokedPeers) :
    step s (.publishUpdate peerId windowId baseHeadId artifactId) = s := by
  simp [step, hRevoked]

theorem promote_head_without_quorum_is_ignored
    (s : State)
    (windowId : WindowId)
    (baseHeadId : HeadId)
    (headId : HeadId)
    (artifactId : ArtifactId)
    (hNoQuorum : ¬ hasValidatorQuorumFor s windowId artifactId) :
    step s (.promoteHead windowId baseHeadId headId artifactId) = s := by
  unfold hasValidatorQuorumFor at hNoQuorum
  have hFalse : hasValidatorQuorumForB s windowId artifactId = false := by
    cases hBool : hasValidatorQuorumForB s windowId artifactId <;> simp [hBool] at hNoQuorum ⊢
  unfold step
  simp [hFalse]

theorem promote_head_with_quorum_sets_canonical_head
    (s : State)
    (windowId : WindowId)
    (baseHeadId : HeadId)
    (headId : HeadId)
    (artifactId : ArtifactId)
    (hQuorum : hasValidatorQuorumFor s windowId artifactId) :
    (step s (.promoteHead windowId baseHeadId headId artifactId)).promotedHead =
      some {
        windowId := windowId
        baseHeadId := baseHeadId
        headId := headId
        aggregateArtifactId := artifactId
        acceptedContributorPeerIds := acceptedContributorPeerIdsForWindow s windowId
      } := by
  unfold hasValidatorQuorumFor at hQuorum
  have hTrue : hasValidatorQuorumForB s windowId artifactId = true := hQuorum
  simp [step, hTrue]

end BurnP2P.Proofs
