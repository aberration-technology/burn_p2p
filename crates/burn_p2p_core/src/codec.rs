use std::io::Cursor;

use multihash::Multihash;
use multihash_codetable::{Code, MultihashDigest};
use serde::{Serialize, de::DeserializeOwned};

use crate::id::ContentId;

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("failed to serialize schema payload as CBOR")]
    Encode(#[from] ciborium::ser::Error<std::io::Error>),
    #[error("failed to deserialize schema payload from CBOR")]
    Decode(#[from] ciborium::de::Error<std::io::Error>),
}

pub fn deterministic_cbor<T>(value: &T) -> Result<Vec<u8>, SchemaError>
where
    T: Serialize + ?Sized,
{
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)?;
    Ok(bytes)
}

pub fn from_cbor_slice<T>(bytes: &[u8]) -> Result<T, SchemaError>
where
    T: DeserializeOwned,
{
    let mut cursor = Cursor::new(bytes);
    Ok(ciborium::de::from_reader(&mut cursor)?)
}

pub fn multihash_sha256(bytes: &[u8]) -> Multihash<64> {
    Code::Sha2_256.digest(bytes)
}

pub fn content_id_for<T>(value: &T) -> Result<ContentId, SchemaError>
where
    T: Serialize + ?Sized,
{
    let bytes = deterministic_cbor(value)?;
    Ok(ContentId::from_multihash(multihash_sha256(&bytes)))
}

pub trait CanonicalSchema: Serialize {
    fn to_cbor_vec(&self) -> Result<Vec<u8>, SchemaError> {
        deterministic_cbor(self)
    }

    fn content_id(&self) -> Result<ContentId, SchemaError> {
        content_id_for(self)
    }
}

impl<T> CanonicalSchema for T where T: Serialize {}
