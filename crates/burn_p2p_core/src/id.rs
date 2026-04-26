use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::codec::{SchemaError, content_id_for};

/// Maximum byte length accepted for an ID when it is used directly as a path segment.
pub const ID_PATH_SEGMENT_MAX_BYTES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
/// Describes why an identifier cannot be used directly as one filesystem path segment.
pub enum IdPathSegmentError {
    /// The identifier was empty.
    #[error("identifier is empty")]
    Empty,
    /// The identifier is too long for the store layout.
    #[error("identifier is {actual} bytes, maximum is {max}")]
    TooLong {
        /// The actual byte length.
        actual: usize,
        /// The maximum byte length.
        max: usize,
    },
    /// The identifier contains bytes that are unsafe for a single path segment.
    #[error("identifier contains bytes outside [A-Za-z0-9._-] or is a reserved segment")]
    UnsafePathSegment,
}

/// Validates that an identifier is safe to use as one filesystem path segment.
pub fn validate_id_path_segment(value: &str) -> Result<(), IdPathSegmentError> {
    if value.is_empty() {
        return Err(IdPathSegmentError::Empty);
    }
    if value.len() > ID_PATH_SEGMENT_MAX_BYTES {
        return Err(IdPathSegmentError::TooLong {
            actual: value.len(),
            max: ID_PATH_SEGMENT_MAX_BYTES,
        });
    }
    if matches!(value, "." | "..") || !value.bytes().all(is_safe_id_path_segment_byte) {
        return Err(IdPathSegmentError::UnsafePathSegment);
    }
    Ok(())
}

/// Returns a deterministic filesystem-safe component for an identifier.
///
/// Safe identifiers are preserved for readable local stores. Unsafe identifiers
/// are mapped to a hash-backed component so remote metadata cannot escape the
/// intended store root through `/`, `..`, or platform-specific path bytes.
pub fn id_path_component(value: &str) -> String {
    if validate_id_path_segment(value).is_ok() {
        return value.to_owned();
    }
    let digest = Sha256::digest(value.as_bytes());
    format!("__burn_p2p_id_{}", hex::encode(digest))
}

fn is_safe_id_path_segment_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
/// Identifies canonical content in the filesystem-backed content-addressed store.
pub struct ContentId(String);

impl ContentId {
    /// Creates a content identifier from an existing string value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Creates a content identifier from a multihash byte sequence.
    pub fn from_multihash(multihash: impl AsRef<[u8]>) -> Self {
        Self(hex::encode(multihash.as_ref()))
    }

    /// Derives a canonical content identifier from a serializable value.
    pub fn derive<T>(value: &T) -> Result<Self, SchemaError>
    where
        T: serde::Serialize + ?Sized,
    {
        content_id_for(value)
    }

    /// Returns the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns a filesystem-safe path component for this identifier.
    pub fn path_component(&self) -> String {
        id_path_component(&self.0)
    }

    /// Consumes the identifier and returns the owned string value.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for ContentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ContentId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
    }
}

macro_rules! define_typed_id {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        #[doc = concat!("Identifies a ", stringify!($name), " value.")]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Creates a ", stringify!($name), " from an existing string value.")]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[doc = concat!(
                                "Derives a ",
                                stringify!($name),
                                " from the canonical content identifier of a serializable value."
                            )]
            pub fn derive<T>(value: &T) -> Result<Self, SchemaError>
            where
                T: serde::Serialize + ?Sized,
            {
                Ok(Self(ContentId::derive(value)?.into_inner()))
            }

            #[doc = concat!(
                                "Returns the ",
                                stringify!($name),
                                " as a string slice."
                            )]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[doc = concat!(
                "Returns a filesystem-safe path component for this ",
                stringify!($name),
                "."
            )]
            pub fn path_component(&self) -> String {
                id_path_component(&self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<ContentId> for $name {
            fn from(value: ContentId) -> Self {
                Self(value.into_inner())
            }
        }

        impl From<$name> for ContentId {
            fn from(value: $name) -> Self {
                ContentId::new(value.0)
            }
        }
    };
}

define_typed_id!(NetworkId);
define_typed_id!(ProjectFamilyId);
define_typed_id!(StudyId);
define_typed_id!(ExperimentId);
define_typed_id!(RevisionId);
define_typed_id!(RunId);
define_typed_id!(WorkloadId);
define_typed_id!(HeadId);
define_typed_id!(ArtifactId);
define_typed_id!(ChunkId);
define_typed_id!(DatasetId);
define_typed_id!(DatasetViewId);
define_typed_id!(MicroShardId);
define_typed_id!(LeaseId);
define_typed_id!(PeerId);
define_typed_id!(PrincipalId);
define_typed_id!(NodeCertId);
define_typed_id!(CapabilityCardId);
define_typed_id!(ContributionReceiptId);
define_typed_id!(ControlCertId);
define_typed_id!(MergeCertId);
define_typed_id!(ArtifactAliasId);
define_typed_id!(PublishedArtifactId);
define_typed_id!(ExportJobId);
define_typed_id!(DownloadTicketId);
define_typed_id!(PublicationTargetId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_component_preserves_safe_ids() {
        assert_eq!(id_path_component("head-001_a.b"), "head-001_a.b");
    }

    #[test]
    fn path_component_maps_unsafe_ids_without_path_separators() {
        let component = id_path_component("../nca/pretraining");
        assert!(component.starts_with("__burn_p2p_id_"));
        assert!(!component.contains('/'));
        assert_ne!(component, id_path_component("../other"));
    }

    #[test]
    fn validates_reserved_or_empty_segments() {
        assert_eq!(validate_id_path_segment(""), Err(IdPathSegmentError::Empty));
        assert_eq!(
            validate_id_path_segment(".."),
            Err(IdPathSegmentError::UnsafePathSegment)
        );
    }
}
