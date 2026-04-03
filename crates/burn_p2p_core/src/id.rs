use std::{fmt, str::FromStr};

use multihash::Multihash;
use serde::{Deserialize, Serialize};

use crate::codec::{SchemaError, content_id_for};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentId(String);

impl ContentId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn from_multihash(multihash: Multihash<64>) -> Self {
        Self(hex::encode(multihash.to_bytes()))
    }

    pub fn derive<T>(value: &T) -> Result<Self, SchemaError>
    where
        T: serde::Serialize + ?Sized,
    {
        content_id_for(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

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
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn derive<T>(value: &T) -> Result<Self, SchemaError>
            where
                T: serde::Serialize + ?Sized,
            {
                Ok(Self(ContentId::derive(value)?.into_inner()))
            }

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
