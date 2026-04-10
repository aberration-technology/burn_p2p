import BurnP2P.Protocol.State

namespace BurnP2P.Protocol

def hasValidatorQuorum (s : State) : Prop :=
  s.validatorQuorumSize > 0 ∧
    s.validatorAttestations.length >= s.validatorQuorumSize

def promotedHeadRequiresValidatorQuorum (s : State) : Prop :=
  s.promotedHead.isNone ∨ hasValidatorQuorum s

def revokedPeersExcluded (s : State) : Prop :=
  ∀ update, update ∈ s.candidateUpdates → update.peerId ∉ s.revokedPeers

def acceptedInputsOnly (s : State) : Prop :=
  ∀ promotion,
    s.promotedHead = some promotion →
      ∀ peerId,
        peerId ∈ promotion.acceptedContributorPeerIds →
          ∃ update, update ∈ s.candidateUpdates ∧ update.peerId = peerId

def reducerNonAuthority (s : State) : Prop :=
  s.promotedHead.isSome → hasValidatorQuorum s

end BurnP2P.Protocol
