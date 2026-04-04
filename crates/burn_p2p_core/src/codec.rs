use std::io::Cursor;

use multihash::Multihash;
use multihash_codetable::{Code, MultihashDigest};
use serde::{Serialize, de::DeserializeOwned};

use crate::id::ContentId;

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

/// Hashes a byte slice with SHA-256 and returns the multihash encoding.
pub fn multihash_sha256(bytes: &[u8]) -> Multihash<64> {
    Code::Sha2_256.digest(bytes)
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
