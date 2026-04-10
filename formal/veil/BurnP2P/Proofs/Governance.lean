import BurnP2P.Protocol.Governance

namespace BurnP2P.Proofs

open BurnP2P.Protocol

theorem validator_set_manifest_requires_nonzero_quorum
    (manifest : ValidatorSetManifest)
    (h : validatorSetManifestWellFormed manifest) :
    manifest.quorumSize > 0 := by
  exact h.left

theorem validator_set_manifest_quorum_is_bounded
    (manifest : ValidatorSetManifest)
    (h : validatorSetManifestWellFormed manifest) :
    manifest.quorumSize <= manifest.validatorPeerIds.length := by
  exact h.right

end BurnP2P.Proofs
