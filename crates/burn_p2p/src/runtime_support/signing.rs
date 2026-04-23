use super::*;
use burn_p2p_core::{SignatureAlgorithm, SignatureMetadata};

pub(crate) const DILOCO_STATE_SIGNATURE_KEY_ID: &str = "runtime-diloco-state";
pub(crate) const DILOCO_GRADIENT_SIGNATURE_KEY_ID: &str = "runtime-diloco-gradient";

pub(crate) fn sign_diloco_state_snapshot(
    keypair: &Keypair,
    snapshot: &DiLoCoStateSnapshot,
) -> anyhow::Result<SignatureMetadata> {
    let mut unsigned = snapshot.clone();
    unsigned.signature_bundle.clear();
    sign_runtime_payload(keypair, DILOCO_STATE_SIGNATURE_KEY_ID, &unsigned)
}

pub(crate) fn sign_diloco_gradient_manifest(
    keypair: &Keypair,
    manifest: &PseudoGradientManifest,
) -> anyhow::Result<SignatureMetadata> {
    let mut unsigned = manifest.clone();
    unsigned.signature_bundle.clear();
    sign_runtime_payload(keypair, DILOCO_GRADIENT_SIGNATURE_KEY_ID, &unsigned)
}

pub(crate) fn verify_diloco_state_snapshot_signature(
    control_plane: &ControlPlaneSnapshot,
    peer_id: &PeerId,
    snapshot: &DiLoCoStateSnapshot,
) -> bool {
    let mut unsigned = snapshot.clone();
    unsigned.signature_bundle.clear();
    verify_runtime_payload_signature(
        control_plane,
        peer_id,
        DILOCO_STATE_SIGNATURE_KEY_ID,
        &unsigned,
        &snapshot.signature_bundle,
    )
}

pub(crate) fn verify_diloco_gradient_manifest_signature(
    control_plane: &ControlPlaneSnapshot,
    peer_id: &PeerId,
    manifest: &PseudoGradientManifest,
) -> bool {
    let mut unsigned = manifest.clone();
    unsigned.signature_bundle.clear();
    verify_runtime_payload_signature(
        control_plane,
        peer_id,
        DILOCO_GRADIENT_SIGNATURE_KEY_ID,
        &unsigned,
        &manifest.signature_bundle,
    )
}

fn sign_runtime_payload<T>(
    keypair: &Keypair,
    key_id: &str,
    payload: &T,
) -> anyhow::Result<SignatureMetadata>
where
    T: serde::Serialize,
{
    let message = burn_p2p_core::deterministic_cbor(payload)?;
    let signature = keypair
        .sign(&message)
        .map_err(|error| anyhow::anyhow!("failed to sign {key_id} payload: {error}"))?;
    Ok(SignatureMetadata {
        signer: PeerId::new(
            libp2p_identity::PeerId::from_public_key(&keypair.public()).to_string(),
        ),
        key_id: key_id.into(),
        algorithm: SignatureAlgorithm::Ed25519,
        signed_at: Utc::now(),
        signature_hex: hex::encode(signature),
    })
}

fn verify_runtime_payload_signature<T>(
    control_plane: &ControlPlaneSnapshot,
    peer_id: &PeerId,
    key_id: &str,
    payload: &T,
    signatures: &[SignatureMetadata],
) -> bool
where
    T: serde::Serialize,
{
    let Some(public_key) = latest_peer_public_key(control_plane, peer_id) else {
        return false;
    };
    let Ok(message) = burn_p2p_core::deterministic_cbor(payload) else {
        return false;
    };
    signatures.iter().any(|signature| {
        signature.signer == *peer_id
            && signature.key_id == key_id
            && matches!(signature.algorithm, SignatureAlgorithm::Ed25519)
            && hex::decode(&signature.signature_hex)
                .map(|raw| public_key.verify(&message, &raw))
                .unwrap_or(false)
    })
}

fn latest_peer_public_key(
    control_plane: &ControlPlaneSnapshot,
    peer_id: &PeerId,
) -> Option<libp2p_identity::PublicKey> {
    let announcement = control_plane
        .auth_announcements
        .iter()
        .filter(|announcement| &announcement.peer_id == peer_id)
        .max_by(|left, right| left.announced_at.cmp(&right.announced_at))?;
    let claims = announcement.envelope.certificate.claims();
    if &claims.peer_id != peer_id {
        return None;
    }
    let public_key_bytes = hex::decode(&claims.peer_public_key_hex).ok()?;
    let public_key = libp2p_identity::PublicKey::try_decode_protobuf(&public_key_bytes).ok()?;
    (PeerId::new(libp2p_identity::PeerId::from_public_key(&public_key).to_string()) == *peer_id)
        .then_some(public_key)
}
