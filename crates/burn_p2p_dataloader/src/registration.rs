use burn_p2p_core::{DatasetManifest, DatasetView};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported upstream adapter values.
pub enum UpstreamAdapter {
    /// Uses the hf variant.
    Hf {
        /// The dataset.
        dataset: String,
        /// The config.
        config: Option<String>,
        /// The split.
        split: String,
        /// The streaming.
        streaming: bool,
        /// The num shards.
        num_shards: Option<u32>,
    },
    /// Uses the HTTP variant.
    Http {
        /// The base URL.
        base_url: String,
    },
    /// Uses the local variant.
    Local {
        /// The root.
        root: String,
    },
    /// Uses the s3 variant.
    S3 {
        /// The bucket.
        bucket: String,
        /// The prefix.
        prefix: String,
    },
    /// Exposes the private setting.
    Private {
        /// The scheme.
        scheme: String,
        /// The locator.
        locator: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a dataset registration.
pub struct DatasetRegistration {
    /// The manifest.
    pub manifest: DatasetManifest,
    /// The view.
    pub view: DatasetView,
    /// The upstream.
    pub upstream: UpstreamAdapter,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a dataset sizing.
pub struct DatasetSizing {
    /// The total examples.
    pub total_examples: u64,
    /// The total tokens.
    pub total_tokens: u64,
    /// The total bytes.
    pub total_bytes: u64,
}
