mod hf_manifest;
mod http;
mod local;

use crate::{DataloaderError, UpstreamAdapter};

pub(crate) fn fetch_manifest_bytes(upstream: &UpstreamAdapter) -> Result<Vec<u8>, DataloaderError> {
    match upstream {
        UpstreamAdapter::Local { root } => local::fetch_manifest_bytes(root),
        UpstreamAdapter::Http { base_url } => http::fetch_manifest_bytes(base_url),
        UpstreamAdapter::Hf { .. } => hf_manifest::fetch_manifest_bytes(upstream),
        unsupported => Err(DataloaderError::UnsupportedAdapter(unsupported.clone())),
    }
}

pub(crate) fn fetch_entry_bytes(
    upstream: &UpstreamAdapter,
    locator: &str,
) -> Result<Vec<u8>, DataloaderError> {
    match upstream {
        UpstreamAdapter::Local { root } => local::fetch_entry_bytes(root, locator),
        UpstreamAdapter::Http { base_url } => http::fetch_entry_bytes(base_url, locator),
        UpstreamAdapter::Hf { .. } => hf_manifest::fetch_entry_bytes(upstream, locator),
        unsupported => Err(DataloaderError::UnsupportedAdapter(unsupported.clone())),
    }
}
