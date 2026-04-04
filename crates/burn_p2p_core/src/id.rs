use std::{fmt, str::FromStr};

use multihash::Multihash;
use serde::{Deserialize, Serialize};

use crate::codec::{SchemaError, content_id_for};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
/// Identifies canonical content in the filesystem-backed content-addressed store.
pub struct ContentId(String);

impl ContentId {
    /// Creates a content identifier from an existing string value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Creates a content identifier from a multihash digest.
    pub fn from_multihash(multihash: Multihash<64>) -> Self {
        Self(hex::encode(multihash.to_bytes()))
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
