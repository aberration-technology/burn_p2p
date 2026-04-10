import BurnP2P.Protocol.State

namespace BurnP2P.Protocol

structure ValidatorSetManifest where
  epoch : AuthorityEpoch
  validatorPeerIds : List PeerId
  quorumSize : Nat
deriving Repr, DecidableEq

def validatorSetManifestWellFormed (manifest : ValidatorSetManifest) : Prop :=
  manifest.quorumSize > 0 ∧
    manifest.quorumSize <= manifest.validatorPeerIds.length

def authorityEpochMonotonic (before after : State) : Prop :=
  before.authorityEpoch <= after.authorityEpoch

end BurnP2P.Protocol
