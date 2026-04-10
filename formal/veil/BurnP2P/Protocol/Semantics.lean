import BurnP2P.Protocol.Actions
import BurnP2P.Protocol.Invariants

namespace BurnP2P.Protocol

def validatorAttestationCount (s : State) (windowId : WindowId) (artifactId : ArtifactId) : Nat :=
  (s.validatorAttestations.filter
    (fun attestation => attestation.windowId = windowId ∧ attestation.aggregateArtifactId = artifactId)).length

def hasValidatorQuorumForB (s : State) (windowId : WindowId) (artifactId : ArtifactId) : Bool :=
  s.validatorQuorumSize > 0 &&
    validatorAttestationCount s windowId artifactId >= s.validatorQuorumSize

def hasValidatorQuorumFor (s : State) (windowId : WindowId) (artifactId : ArtifactId) : Prop :=
  hasValidatorQuorumForB s windowId artifactId = true

def publishedCandidateExists (s : State) (candidatePeerId : PeerId) (windowId : WindowId) (artifactId : ArtifactId) : Bool :=
  (s.candidateUpdates.any
    (fun update => update.peerId = candidatePeerId ∧ update.windowId = windowId ∧ update.artifactId = artifactId))

def acceptedContributorPeerIdsForWindow (s : State) (windowId : WindowId) : List PeerId :=
  let acceptedForWindow :=
    s.acceptedCandidates.filter (fun (decision : CandidateDecision) => decision.windowId = windowId)
  let acceptedPeerIds := acceptedForWindow.map (·.candidatePeerId)
  (acceptedPeerIds.filter (fun peerId => peerId ∉ s.revokedPeers)).eraseDups

def proposalContributorPeerIdsForWindow (s : State) (windowId : WindowId) : List PeerId :=
  let updatesForWindow :=
    s.candidateUpdates.filter (fun (update : CandidateUpdate) => update.windowId = windowId ∧ update.peerId ∉ s.revokedPeers)
  (updatesForWindow.map (·.peerId)).eraseDups

def step (s : State) (action : Action) : State :=
  match action with
  | .publishUpdate peerId windowId baseHeadId artifactId =>
      if peerId ∈ s.revokedPeers then s
      else
        { s with
            candidateUpdates := s.candidateUpdates ++ [{
              peerId := peerId
              windowId := windowId
              baseHeadId := baseHeadId
              artifactId := artifactId
            }] }
  | .publishReducerProposal peerId windowId baseHeadId artifactId =>
      { s with
            reducerProposals := s.reducerProposals ++ [{
            reducerPeerId := peerId
            windowId := windowId
            baseHeadId := baseHeadId
            aggregateArtifactId := artifactId
            contributorPeerIds := proposalContributorPeerIdsForWindow s windowId
          }] }
  | .acceptCandidate validatorPeerId candidatePeerId artifactId =>
      match s.candidateUpdates.find? (fun update => update.peerId = candidatePeerId ∧ update.artifactId = artifactId) with
      | some update =>
          if candidatePeerId ∈ s.revokedPeers then s
          else
            { s with
                acceptedCandidates := s.acceptedCandidates ++ [{
                  validatorPeerId := validatorPeerId
                  candidatePeerId := candidatePeerId
                  windowId := update.windowId
                  artifactId := artifactId
                }] }
      | none => s
  | .rejectCandidate validatorPeerId candidatePeerId artifactId =>
      match s.candidateUpdates.find? (fun update => update.peerId = candidatePeerId ∧ update.artifactId = artifactId) with
      | some update =>
          { s with
              rejectedCandidates := s.rejectedCandidates ++ [{
                validatorPeerId := validatorPeerId
                candidatePeerId := candidatePeerId
                windowId := update.windowId
                artifactId := artifactId
              }] }
      | none => s
  | .attestReduction validatorPeerId windowId artifactId =>
      if validatorPeerId ∈ s.validatorSet then
        { s with
            validatorAttestations := s.validatorAttestations ++ [{
              validatorPeerId := validatorPeerId
              windowId := windowId
              aggregateArtifactId := artifactId
            }] }
      else s
  | .promoteHead windowId baseHeadId headId artifactId =>
      if hasValidatorQuorumForB s windowId artifactId then
        { s with
            promotedHead := some {
              windowId := windowId
              baseHeadId := baseHeadId
              headId := headId
              aggregateArtifactId := artifactId
              acceptedContributorPeerIds := acceptedContributorPeerIdsForWindow s windowId
            } }
      else s
  | .revokePeer peerId epoch =>
      { s with
          authorityEpoch := max s.authorityEpoch epoch
          revokedPeers := peerId :: s.revokedPeers.eraseDups
          candidateUpdates := s.candidateUpdates.filter (fun update => update.peerId ≠ peerId) }
  | .rotateValidatorSet epoch =>
      { s with authorityEpoch := max s.authorityEpoch epoch }

end BurnP2P.Protocol
