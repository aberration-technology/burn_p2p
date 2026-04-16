use std::io::Cursor;

use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::id::ContentId;

pub const SHA2_256_MULTIHASH_CODE: u8 = 0x12;
pub const SHA2_256_DIGEST_LEN: usize = 32;
pub const SHA2_256_MULTIHASH_LEN: usize = 2 + SHA2_256_DIGEST_LEN;

#[derive(Debug, thiserror::Error)]
/// Errors returned while encoding or decoding canonical schema payloads.
pub enum SchemaError {
    /// The payload could not be encoded as canonical CBOR.
    #[error("failed to serialize schema payload as CBOR")]
    Encode(#[from] ciborium::ser::Error<std::io::Error>),
    /// The payload could not be decoded from canonical CBOR.
    #[error("failed to deserialize schema payload from CBOR")]
    Decode(#[from] ciborium::de::Error<std::io::Error>),
}

/// Serializes a value into canonical CBOR bytes.
pub fn deterministic_cbor<T>(value: &T) -> Result<Vec<u8>, SchemaError>
where
    T: Serialize + ?Sized,
{
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)?;
    Ok(bytes)
}

/// Deserializes a value from canonical CBOR bytes.
pub fn from_cbor_slice<T>(bytes: &[u8]) -> Result<T, SchemaError>
where
    T: DeserializeOwned,
{
    let mut cursor = Cursor::new(bytes);
    Ok(ciborium::de::from_reader(&mut cursor)?)
}

/// Encodes a raw SHA-256 digest as a multihash byte sequence.
pub fn multihash_from_sha256_digest(digest: impl AsRef<[u8]>) -> [u8; SHA2_256_MULTIHASH_LEN] {
    let digest = digest.as_ref();
    assert_eq!(
        digest.len(),
        SHA2_256_DIGEST_LEN,
        "sha256 digest should be 32 bytes"
    );

    let mut multihash = [0u8; SHA2_256_MULTIHASH_LEN];
    multihash[0] = SHA2_256_MULTIHASH_CODE;
    multihash[1] = SHA2_256_DIGEST_LEN as u8;
    multihash[2..].copy_from_slice(digest);
    multihash
}

/// Hashes a byte slice with SHA-256 and returns the multihash encoding.
pub fn multihash_sha256(bytes: &[u8]) -> [u8; SHA2_256_MULTIHASH_LEN] {
    multihash_from_sha256_digest(Sha256::digest(bytes))
}

/// Computes the canonical content identifier for a serializable value.
pub fn content_id_for<T>(value: &T) -> Result<ContentId, SchemaError>
where
    T: Serialize + ?Sized,
{
    let bytes = deterministic_cbor(value)?;
    Ok(ContentId::from_multihash(multihash_sha256(&bytes)))
}

/// Provides canonical encoding and content-addressing helpers for schema types.
pub trait CanonicalSchema: Serialize {
    /// Serializes the value into canonical CBOR bytes.
    fn to_cbor_vec(&self) -> Result<Vec<u8>, SchemaError> {
        deterministic_cbor(self)
    }

    /// Computes the canonical content identifier for the value.
    fn content_id(&self) -> Result<ContentId, SchemaError> {
        content_id_for(self)
    }
}

impl<T> CanonicalSchema for T where T: Serialize {}

#[cfg(test)]
mod tests {
    use super::{multihash_from_sha256_digest, multihash_sha256};
    use sha2::Digest;

    #[test]
    fn sha256_multihash_encoding_matches_multiformats_wire_format() {
        let digest = sha2::Sha256::digest(b"");
        let multihash = multihash_from_sha256_digest(digest);
        assert_eq!(
            hex::encode(multihash),
            "1220e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_multihash_hashes_payload_bytes() {
        assert_eq!(
            hex::encode(multihash_sha256(b"abc")),
            "1220ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
