use std::io::{Cursor, Write};

use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::id::ContentId;

pub const SHA2_256_MULTIHASH_CODE: u8 = 0x12;
pub const SHA2_256_DIGEST_LEN: usize = 32;
pub const SHA2_256_MULTIHASH_LEN: usize = 2 + SHA2_256_DIGEST_LEN;

/// Compact byte serialization with backward-compatible sequence decoding.
///
/// New payloads use the serializer's byte-string representation. Decoders also
/// accept the integer sequence emitted by older `Vec<u8>` schemas so peers can
/// interoperate during rolling upgrades.
pub mod compact_bytes {
    use std::{cmp, fmt};

    use serde::{
        Deserializer, Serializer,
        de::{SeqAccess, Visitor},
    };

    /// Serializes bytes using the format's compact byte-string representation.
    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(bytes)
    }

    /// Deserializes either a compact byte string or a legacy integer sequence.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(CompatibleBytesVisitor)
    }

    struct CompatibleBytesVisitor;

    impl<'de> Visitor<'de> for CompatibleBytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a byte string or a sequence of bytes")
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E> {
            Ok(value.to_vec())
        }

        fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E> {
            Ok(value.to_vec())
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let capacity = cmp::min(sequence.size_hint().unwrap_or(0), 4096);
            let mut bytes = Vec::with_capacity(capacity);
            while let Some(byte) = sequence.next_element()? {
                bytes.push(byte);
            }
            Ok(bytes)
        }
    }
}

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

struct Sha256Writer(Sha256);

impl Write for Sha256Writer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Computes the canonical content identifier for a serializable value.
pub fn content_id_for<T>(value: &T) -> Result<ContentId, SchemaError>
where
    T: Serialize + ?Sized,
{
    let mut writer = Sha256Writer(Sha256::new());
    ciborium::ser::into_writer(value, &mut writer)?;
    Ok(ContentId::from_multihash(multihash_from_sha256_digest(
        writer.0.finalize(),
    )))
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
    use super::{
        content_id_for, deterministic_cbor, from_cbor_slice, multihash_from_sha256_digest,
        multihash_sha256,
    };
    use crate::ContentId;
    use serde::{Deserialize, Serialize};
    use sha2::Digest;

    #[derive(Debug, PartialEq, Serialize)]
    struct LegacyBytes {
        bytes: Vec<u8>,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct CompatibleBytes {
        #[serde(with = "crate::codec::compact_bytes")]
        bytes: Vec<u8>,
    }

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

    #[test]
    fn streaming_content_id_matches_buffered_canonical_cbor_hash() {
        let value = (
            "streaming-content-id",
            vec![1_u64, 2, 3, 5, 8],
            Some("stable"),
        );
        let buffered = deterministic_cbor(&value).expect("canonical CBOR");
        let expected = ContentId::from_multihash(multihash_sha256(&buffered));

        assert_eq!(
            content_id_for(&value).expect("streaming content identifier"),
            expected
        );
    }

    #[test]
    fn compact_bytes_decode_legacy_sequences_and_current_byte_strings() {
        let expected = vec![0, 1, 2, 127, 128, 254, 255];
        let legacy = deterministic_cbor(&LegacyBytes {
            bytes: expected.clone(),
        })
        .expect("encode legacy byte sequence");
        let compact = deterministic_cbor(&CompatibleBytes {
            bytes: expected.clone(),
        })
        .expect("encode compact byte string");

        let legacy_decoded: CompatibleBytes =
            from_cbor_slice(&legacy).expect("decode legacy byte sequence");
        let compact_decoded: CompatibleBytes =
            from_cbor_slice(&compact).expect("decode compact byte string");

        assert_eq!(legacy_decoded.bytes, expected);
        assert_eq!(compact_decoded.bytes, expected);
        assert!(compact.len() < legacy.len());
    }
}
