namespace BurnP2P.Trace

inductive ProtocolEvent where
  | updatePublished (observedAt : String) (peerId : String) (windowId : Nat) (baseHeadId : String) (artifactId : String)
  | reducerProposalObserved (observedAt : String) (reducerPeerId : String) (windowId : Nat) (baseHeadId : String) (aggregateArtifactId : String) (contributorPeerIds : List String)
  | candidateAccepted (observedAt : String) (validatorPeerId : String) (candidatePeerId : String) (windowId : Nat) (artifactId : String)
  | candidateRejected (observedAt : String) (validatorPeerId : String) (candidatePeerId : String) (windowId : Nat) (artifactId : String) (reason : String)
  | validationAttested (observedAt : String) (validatorPeerId : String) (windowId : Nat) (aggregateArtifactId : String)
  | quorumCertificateEmitted (observedAt : String) (windowId : Nat) (aggregateArtifactId : String) (validatorPeerIds : List String)
  | canonicalHeadPromoted (observedAt : String) (windowId : Nat) (baseHeadId : String) (headId : String) (aggregateArtifactId : String) (acceptedContributorPeerIds : List String)
  | peerRevoked (observedAt : String) (peerId : String) (revocationEpoch : Nat)
deriving Repr, DecidableEq

structure ProtocolTrace where
  schemaVersion : String
  scenario : String
  events : List ProtocolEvent
deriving Repr, DecidableEq

end BurnP2P.Trace
