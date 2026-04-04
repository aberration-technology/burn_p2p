use std::path::PathBuf;

use burn_p2p_core::{ArtifactProfile, PublishedArtifactRecord};

/// Delivery body for a resolved artifact download.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DownloadArtifactBody {
    /// No body is needed because the caller should follow `redirect_url`.
    Empty,
    /// Small artifacts already materialized in memory.
    Bytes(Vec<u8>),
    /// Stream from one local filesystem mirror path.
    LocalFile {
        /// Local path to stream from.
        path: PathBuf,
        /// Declared content length of the file.
        content_length: u64,
    },
    /// Stream from one remote object-store URL through the portal.
    RemoteProxy {
        /// Remote URL to fetch from.
        url: String,
        /// Declared content length of the proxied object.
        content_length: u64,
    },
}

/// Streamable artifact payload resolved from a valid download ticket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DownloadArtifact {
    /// Metadata for the published artifact being served.
    pub published_artifact: PublishedArtifactRecord,
    /// Suggested download filename.
    pub file_name: String,
    /// Content type for the streamed response.
    pub content_type: String,
    /// Redirect URL for object-store delivery, when portal streaming is not used.
    pub redirect_url: Option<String>,
    /// Delivery body for portal-stream mode.
    pub body: DownloadArtifactBody,
}

pub(crate) fn content_type_for_profile(artifact_profile: &ArtifactProfile) -> &'static str {
    match artifact_profile {
        ArtifactProfile::ManifestOnly | ArtifactProfile::BrowserSnapshot => {
            "application/json; charset=utf-8"
        }
        ArtifactProfile::FullTrainingCheckpoint | ArtifactProfile::ServeCheckpoint => {
            "application/octet-stream"
        }
    }
}

pub(crate) fn download_file_name(record: &PublishedArtifactRecord) -> String {
    let suffix = match record.artifact_profile {
        ArtifactProfile::FullTrainingCheckpoint => "full.bin",
        ArtifactProfile::ServeCheckpoint => "serve.bin",
        ArtifactProfile::BrowserSnapshot => "browser.json",
        ArtifactProfile::ManifestOnly => "manifest.json",
    };
    format!("{}-{}", record.head_id.as_str(), suffix)
}
