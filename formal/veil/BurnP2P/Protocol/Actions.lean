import BurnP2P.Protocol.State

namespace BurnP2P.Protocol

inductive Action where
  | publishUpdate (peerId : PeerId) (windowId : WindowId) (baseHeadId : HeadId) (artifactId : ArtifactId)
  | publishReducerProposal (peerId : PeerId) (windowId : WindowId) (baseHeadId : HeadId) (artifactId : ArtifactId)
  | acceptCandidate (validatorPeerId : PeerId) (candidatePeerId : PeerId) (artifactId : ArtifactId)
  | rejectCandidate (validatorPeerId : PeerId) (candidatePeerId : PeerId) (artifactId : ArtifactId)
  | attestReduction (validatorPeerId : PeerId) (windowId : WindowId) (artifactId : ArtifactId)
  | promoteHead (windowId : WindowId) (baseHeadId : HeadId) (headId : HeadId) (artifactId : ArtifactId)
  | revokePeer (peerId : PeerId) (epoch : AuthorityEpoch)
  | rotateValidatorSet (epoch : AuthorityEpoch)
deriving Repr, DecidableEq

end BurnP2P.Protocol
