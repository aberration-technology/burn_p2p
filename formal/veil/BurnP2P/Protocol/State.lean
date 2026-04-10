namespace BurnP2P.Protocol

abbrev PeerId := String
abbrev ArtifactId := String
abbrev HeadId := String
abbrev WindowId := Nat
abbrev AuthorityEpoch := Nat

structure Scope where
  networkId : String
  studyId : String
  experimentId : String
  revisionId : String
deriving Repr, DecidableEq

structure CandidateUpdate where
  peerId : PeerId
  windowId : WindowId
  baseHeadId : HeadId
  artifactId : ArtifactId
deriving Repr, DecidableEq

structure ReducerProposal where
  reducerPeerId : PeerId
  windowId : WindowId
  baseHeadId : HeadId
  aggregateArtifactId : ArtifactId
  contributorPeerIds : List PeerId
deriving Repr, DecidableEq

structure ValidatorAttestation where
  validatorPeerId : PeerId
  windowId : WindowId
  aggregateArtifactId : ArtifactId
deriving Repr, DecidableEq

structure CandidateDecision where
  validatorPeerId : PeerId
  candidatePeerId : PeerId
  windowId : WindowId
  artifactId : ArtifactId
deriving Repr, DecidableEq

structure CanonicalPromotion where
  windowId : WindowId
  baseHeadId : HeadId
  headId : HeadId
  aggregateArtifactId : ArtifactId
  acceptedContributorPeerIds : List PeerId
deriving Repr, DecidableEq

structure State where
  scope : Scope
  authorityEpoch : AuthorityEpoch
  validatorSet : List PeerId
  validatorQuorumSize : Nat
  revokedPeers : List PeerId
  candidateUpdates : List CandidateUpdate
  reducerProposals : List ReducerProposal
  acceptedCandidates : List CandidateDecision
  rejectedCandidates : List CandidateDecision
  validatorAttestations : List ValidatorAttestation
  promotedHead : Option CanonicalPromotion
deriving Repr, DecidableEq

end BurnP2P.Protocol
