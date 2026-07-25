use std::collections::BTreeMap;

use burn_p2p_core::{
    CanonicalSchema, MODEL_GENESIS_SIGNATURE_KEY_ID, PeerId, REVISION_CONTRACT_SIGNATURE_KEY_ID,
    RevisionContractBundle, SchemaEnvelope, SignatureAlgorithm, SignatureMetadata, SignedPayload,
    TrustBundleExport,
};
use chrono::{DateTime, Utc};
use libp2p_identity::Keypair;
use serde::Serialize;

use crate::TrustedIssuer;

#[derive(Debug, thiserror::Error)]
/// Failures while signing or verifying canonical authority payloads.
pub enum SignedPayloadError {
    /// Canonical serialization failed.
    #[error("canonical schema error: {0}")]
    Schema(#[from] burn_p2p_core::SchemaError),
    /// Signing failed.
    #[error("failed to sign canonical payload: {0}")]
    Signing(String),
    /// The signature uses an unexpected domain-separated key identifier.
    #[error("signature key id {found:?} does not match expected {expected:?}")]
    UnexpectedKeyId { expected: String, found: String },
    /// Only Ed25519 authority signatures are currently supported.
    #[error("unsupported authority signature algorithm")]
    UnsupportedAlgorithm,
    /// The signer is absent from the current trust policy.
    #[error("authority signer {0} is not trusted")]
    UntrustedSigner(PeerId),
    /// The trusted issuer map is internally inconsistent.
    #[error("trusted issuer map key does not match issuer metadata")]
    TrustedIssuerMismatch,
    /// The trusted public key could not be decoded.
    #[error("failed to decode trusted issuer public key: {0}")]
    PublicKeyDecode(String),
    /// The trusted key does not derive the declared signer identity.
    #[error("trusted issuer public key does not match signer identity")]
    SignerKeyMismatch,
    /// The signature bytes could not be decoded.
    #[error("failed to decode authority signature: {0}")]
    SignatureDecode(String),
    /// Cryptographic signature verification failed.
    #[error("authority signature is invalid")]
    InvalidSignature,
    /// A signed payload's content identifier is stale or forged.
    #[error("signed payload id does not match canonical payload content")]
    PayloadIdMismatch,
    /// The revision bundle is structurally invalid.
    #[error("invalid revision contract bundle: {0}")]
    InvalidRevisionContract(#[from] burn_p2p_core::TrainingContractError),
}

/// Creates a domain-separated Ed25519 signature over a canonical payload.
pub fn sign_canonical_payload<T>(
    keypair: &Keypair,
    key_id: impl Into<String>,
    payload: &T,
    signed_at: DateTime<Utc>,
) -> Result<SignatureMetadata, SignedPayloadError>
where
    T: Serialize + ?Sized,
{
    let message = burn_p2p_core::deterministic_cbor(payload)?;
    let signature = keypair
        .sign(&message)
        .map_err(|error| SignedPayloadError::Signing(error.to_string()))?;
    Ok(SignatureMetadata {
        signer: PeerId::new(
            libp2p_identity::PeerId::from_public_key(&keypair.public()).to_string(),
        ),
        key_id: key_id.into(),
        algorithm: SignatureAlgorithm::Ed25519,
        signed_at,
        signature_hex: hex::encode(signature),
    })
}

/// Wraps and signs one canonical payload.
pub fn sign_payload<T>(
    keypair: &Keypair,
    key_id: impl Into<String>,
    payload: T,
    signed_at: DateTime<Utc>,
) -> Result<SignedPayload<T>, SignedPayloadError>
where
    T: Serialize,
{
    let signature = sign_canonical_payload(keypair, key_id, &payload, signed_at)?;
    Ok(SignedPayload::new(payload, signature)?)
}

/// Verifies a domain-separated signature against the current trusted issuer set.
pub fn verify_canonical_signature<T>(
    trusted_issuers: &BTreeMap<PeerId, TrustedIssuer>,
    expected_key_id: &str,
    payload: &T,
    signature: &SignatureMetadata,
) -> Result<(), SignedPayloadError>
where
    T: Serialize + ?Sized,
{
    if signature.key_id != expected_key_id {
        return Err(SignedPayloadError::UnexpectedKeyId {
            expected: expected_key_id.into(),
            found: signature.key_id.clone(),
        });
    }
    if !matches!(signature.algorithm, SignatureAlgorithm::Ed25519) {
        return Err(SignedPayloadError::UnsupportedAlgorithm);
    }
    let trusted = trusted_issuers
        .get(&signature.signer)
        .ok_or_else(|| SignedPayloadError::UntrustedSigner(signature.signer.clone()))?;
    if trusted.issuer_peer_id != signature.signer {
        return Err(SignedPayloadError::TrustedIssuerMismatch);
    }
    let public_key_bytes = hex::decode(&trusted.issuer_public_key_hex)
        .map_err(|error| SignedPayloadError::PublicKeyDecode(error.to_string()))?;
    let public_key = libp2p_identity::PublicKey::try_decode_protobuf(&public_key_bytes)
        .map_err(|error| SignedPayloadError::PublicKeyDecode(error.to_string()))?;
    let key_peer_id =
        PeerId::new(libp2p_identity::PeerId::from_public_key(&public_key).to_string());
    if key_peer_id != signature.signer {
        return Err(SignedPayloadError::SignerKeyMismatch);
    }
    let message = burn_p2p_core::deterministic_cbor(payload)?;
    let signature_bytes = hex::decode(&signature.signature_hex)
        .map_err(|error| SignedPayloadError::SignatureDecode(error.to_string()))?;
    if !public_key.verify(&message, &signature_bytes) {
        return Err(SignedPayloadError::InvalidSignature);
    }
    Ok(())
}

/// Verifies both content addressing and the signature of one signed payload.
pub fn verify_signed_payload<T>(
    trusted_issuers: &BTreeMap<PeerId, TrustedIssuer>,
    expected_key_id: &str,
    signed: &SignedPayload<T>,
) -> Result<(), SignedPayloadError>
where
    T: Serialize,
{
    if signed.payload.content_id()? != signed.payload_id {
        return Err(SignedPayloadError::PayloadIdMismatch);
    }
    verify_canonical_signature(
        trusted_issuers,
        expected_key_id,
        &signed.payload,
        &signed.signature,
    )
}

/// Signs both authority domains of a complete revision contract bundle.
pub fn sign_revision_contract_bundle(
    keypair: &Keypair,
    bundle: &mut RevisionContractBundle,
    signed_at: DateTime<Utc>,
) -> Result<(), SignedPayloadError> {
    bundle.genesis = sign_payload(
        keypair,
        MODEL_GENESIS_SIGNATURE_KEY_ID,
        bundle.genesis.payload.clone(),
        signed_at,
    )?;
    bundle.contract_signature = sign_canonical_payload(
        keypair,
        REVISION_CONTRACT_SIGNATURE_KEY_ID,
        &bundle.authority_payload(),
        signed_at,
    )?;
    bundle.validate()?;
    Ok(())
}

/// Verifies a complete revision contract and both of its authority signatures.
pub fn verify_revision_contract_bundle(
    trusted_issuers: &BTreeMap<PeerId, TrustedIssuer>,
    bundle: &RevisionContractBundle,
) -> Result<(), SignedPayloadError> {
    bundle.validate()?;
    verify_signed_payload(
        trusted_issuers,
        MODEL_GENESIS_SIGNATURE_KEY_ID,
        &bundle.genesis,
    )?;
    verify_canonical_signature(
        trusted_issuers,
        REVISION_CONTRACT_SIGNATURE_KEY_ID,
        &bundle.authority_payload(),
        &bundle.contract_signature,
    )
}

/// Verifies a revision contract against the accepted issuers in a browser trust bundle.
pub fn verify_revision_contract_with_trust_bundle(
    trust_bundle: &TrustBundleExport,
    bundle: &RevisionContractBundle,
) -> Result<(), SignedPayloadError> {
    let trusted_issuers = trust_bundle
        .issuers
        .iter()
        .filter(|issuer| issuer.accepted_for_admission)
        .map(|issuer| {
            (
                issuer.issuer_peer_id.clone(),
                TrustedIssuer {
                    issuer_peer_id: issuer.issuer_peer_id.clone(),
                    issuer_public_key_hex: issuer.issuer_public_key_hex.clone(),
                },
            )
        })
        .collect();
    verify_revision_contract_bundle(&trusted_issuers, bundle)
}

/// Signs a model genesis envelope with the canonical domain identifier.
pub fn sign_model_genesis<T>(
    keypair: &Keypair,
    payload: SchemaEnvelope<T>,
    signed_at: DateTime<Utc>,
) -> Result<SignedPayload<SchemaEnvelope<T>>, SignedPayloadError>
where
    T: Serialize,
{
    sign_payload(keypair, MODEL_GENESIS_SIGNATURE_KEY_ID, payload, signed_at)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority() -> (Keypair, BTreeMap<PeerId, TrustedIssuer>) {
        let keypair =
            Keypair::ed25519_from_bytes([17_u8; 32]).expect("deterministic authority keypair");
        let peer_id =
            PeerId::new(libp2p_identity::PeerId::from_public_key(&keypair.public()).to_string());
        let trusted = TrustedIssuer {
            issuer_peer_id: peer_id.clone(),
            issuer_public_key_hex: hex::encode(keypair.public().encode_protobuf()),
        };
        (keypair, BTreeMap::from([(peer_id, trusted)]))
    }

    #[test]
    fn canonical_signature_rejects_payload_mutation() {
        let (keypair, trusted) = authority();
        let payload = vec!["revision", "training", "genesis"];
        let signature =
            sign_canonical_payload(&keypair, "domain-v1", &payload, Utc::now()).expect("sign");

        verify_canonical_signature(&trusted, "domain-v1", &payload, &signature).expect("verify");
        let error = verify_canonical_signature(
            &trusted,
            "domain-v1",
            &vec!["revision", "mutated", "genesis"],
            &signature,
        )
        .expect_err("mutated payload must fail");
        assert!(matches!(error, SignedPayloadError::InvalidSignature));
    }

    #[test]
    fn canonical_signature_rejects_untrusted_or_wrong_domain() {
        let (keypair, trusted) = authority();
        let signature =
            sign_canonical_payload(&keypair, "domain-v1", &42_u64, Utc::now()).expect("sign");

        assert!(matches!(
            verify_canonical_signature(&trusted, "other-domain", &42_u64, &signature),
            Err(SignedPayloadError::UnexpectedKeyId { .. })
        ));
        assert!(matches!(
            verify_canonical_signature(&BTreeMap::new(), "domain-v1", &42_u64, &signature),
            Err(SignedPayloadError::UntrustedSigner(_))
        ));
    }
}
