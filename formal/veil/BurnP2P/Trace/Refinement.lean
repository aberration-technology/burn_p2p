import BurnP2P.Trace.EventSchema
import BurnP2P.Protocol.Semantics
import BurnP2P.Proofs.Safety

namespace BurnP2P.Trace

open BurnP2P.Protocol

def lowerEvent : ProtocolEvent → Option Action
  | .updatePublished _ peerId windowId baseHeadId artifactId =>
      some (.publishUpdate peerId windowId baseHeadId artifactId)
  | .reducerProposalObserved _ reducerPeerId windowId baseHeadId aggregateArtifactId _ =>
      some (.publishReducerProposal reducerPeerId windowId baseHeadId aggregateArtifactId)
  | .candidateAccepted _ validatorPeerId candidatePeerId _ artifactId =>
      some (.acceptCandidate validatorPeerId candidatePeerId artifactId)
  | .candidateRejected _ validatorPeerId candidatePeerId _ artifactId _ =>
      some (.rejectCandidate validatorPeerId candidatePeerId artifactId)
  | .validationAttested _ validatorPeerId windowId aggregateArtifactId =>
      some (.attestReduction validatorPeerId windowId aggregateArtifactId)
  | .quorumCertificateEmitted .. =>
      none
  | .canonicalHeadPromoted _ windowId baseHeadId headId aggregateArtifactId _ =>
      some (.promoteHead windowId baseHeadId headId aggregateArtifactId)
  | .peerRevoked _ peerId revocationEpoch =>
      some (.revokePeer peerId revocationEpoch)

def lowerTraceActions (trace : ProtocolTrace) : List Action :=
  trace.events.filterMap lowerEvent

def replayLoweredTrace (initial : State) (trace : ProtocolTrace) : State :=
  (lowerTraceActions trace).foldl step initial

theorem lower_reducer_event_matches_protocol_action
    (observedAt reducerPeerId : String)
    (windowId : Nat)
    (baseHeadId aggregateArtifactId : String)
    (contributors : List String) :
    lowerEvent (.reducerProposalObserved observedAt reducerPeerId windowId baseHeadId aggregateArtifactId contributors) =
      some (.publishReducerProposal reducerPeerId windowId baseHeadId aggregateArtifactId) := by
  rfl

theorem lower_attestation_event_matches_protocol_action
    (observedAt validatorPeerId : String)
    (windowId : Nat)
    (aggregateArtifactId : String) :
    lowerEvent (.validationAttested observedAt validatorPeerId windowId aggregateArtifactId) =
      some (.attestReduction validatorPeerId windowId aggregateArtifactId) := by
  rfl

theorem lowered_reducer_event_does_not_promote_head
    (s : State)
    (observedAt reducerPeerId : String)
    (windowId : Nat)
    (baseHeadId aggregateArtifactId : String)
    (contributors : List String) :
    (replayLoweredTrace s {
        schemaVersion := "burn_p2p.formal_trace.v1"
        scenario := "reducer-adversarial"
        events := [.reducerProposalObserved observedAt reducerPeerId windowId baseHeadId aggregateArtifactId contributors]
      }).promotedHead = s.promotedHead := by
  simp [replayLoweredTrace, lowerTraceActions, lowerEvent, Protocol.step]

theorem lowered_promotion_event_without_quorum_is_ignored
    (s : State)
    (observedAt : String)
    (windowId : Nat)
    (baseHeadId headId aggregateArtifactId : String)
    (acceptedContributorPeerIds : List String)
    (hNoQuorum : ¬ hasValidatorQuorumFor s windowId aggregateArtifactId) :
    replayLoweredTrace s {
      schemaVersion := "burn_p2p.formal_trace.v1"
      scenario := "mnist-smoke"
      events := [.canonicalHeadPromoted observedAt windowId baseHeadId headId aggregateArtifactId acceptedContributorPeerIds]
    } = s := by
  simp [replayLoweredTrace, lowerTraceActions, lowerEvent]
  exact BurnP2P.Proofs.promote_head_without_quorum_is_ignored s windowId baseHeadId headId aggregateArtifactId hNoQuorum

end BurnP2P.Trace
