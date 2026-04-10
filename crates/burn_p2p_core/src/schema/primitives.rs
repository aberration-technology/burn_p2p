use super::*;

/// The current schema version.
pub const SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
/// Identifies the window.
pub struct WindowId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a window activation.
pub struct WindowActivation {
    /// The activation window.
    pub activation_window: WindowId,
    /// The grace windows.
    pub grace_windows: u16,
}

impl WindowActivation {
    /// Returns whether the value becomes active at.
    pub fn becomes_active_at(&self, window: WindowId) -> bool {
        window >= self.activation_window
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Wraps the schema with transport metadata.
pub struct SchemaEnvelope<T> {
    /// The schema.
    pub schema: String,
    /// The schema version.
    pub schema_version: u16,
    /// The protocol version.
    pub protocol_version: Version,
    /// The payload.
    pub payload: T,
}

impl<T> SchemaEnvelope<T> {
    /// Creates a new value.
    pub fn new(schema: impl Into<String>, protocol_version: Version, payload: T) -> Self {
        Self {
            schema: schema.into(),
            schema_version: SCHEMA_VERSION,
            protocol_version,
            payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported signature algorithm values.
pub enum SignatureAlgorithm {
    /// Uses the ed25519 variant.
    Ed25519,
    /// Uses the secp256k1 variant.
    Secp256k1,
    /// Carries an unrecognized value.
    Unknown(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a signature metadata.
pub struct SignatureMetadata {
    /// The signer.
    pub signer: PeerId,
    /// The key ID.
    pub key_id: String,
    /// The algorithm.
    pub algorithm: SignatureAlgorithm,
    /// The signed at.
    pub signed_at: DateTime<Utc>,
    /// The signature hex.
    pub signature_hex: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a signed payload.
pub struct SignedPayload<T> {
    /// The payload ID.
    pub payload_id: ContentId,
    /// The payload.
    pub payload: T,
    /// The signature.
    pub signature: SignatureMetadata,
}

impl<T> SignedPayload<T>
where
    T: Serialize,
{
    /// Creates a new value.
    pub fn new(payload: T, signature: SignatureMetadata) -> Result<Self, SchemaError> {
        Ok(Self {
            payload_id: payload.content_id()?,
            payload,
            signature,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Describes one offset-based page request.
pub struct PageRequest {
    /// Number of matching items to skip.
    pub offset: usize,
    /// Maximum number of matching items to return.
    pub limit: usize,
}

impl PageRequest {
    /// Default number of items returned by one page.
    pub const DEFAULT_LIMIT: usize = 50;
    /// Hard upper bound accepted by helper pagination utilities.
    pub const MAX_LIMIT: usize = 1_000;

    /// Creates a new page request.
    pub const fn new(offset: usize, limit: usize) -> Self {
        Self { offset, limit }
    }

    /// Returns a normalized request with a non-zero bounded limit.
    pub const fn normalized(self) -> Self {
        let limit = if self.limit == 0 {
            Self::DEFAULT_LIMIT
        } else if self.limit > Self::MAX_LIMIT {
            Self::MAX_LIMIT
        } else {
            self.limit
        };
        Self {
            offset: self.offset,
            limit,
        }
    }
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: Self::DEFAULT_LIMIT,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// One offset-based page of items with total match count.
pub struct Page<T> {
    /// Returned items for this page.
    pub items: Vec<T>,
    /// Original request offset.
    pub offset: usize,
    /// Effective bounded limit used for this page.
    pub limit: usize,
    /// Total number of matching items across all pages.
    pub total: usize,
}

impl<T> Page<T> {
    /// Creates a new page result.
    pub fn new(items: Vec<T>, request: PageRequest, total: usize) -> Self {
        let request = request.normalized();
        Self {
            items,
            offset: request.offset,
            limit: request.limit,
            total,
        }
    }
}
