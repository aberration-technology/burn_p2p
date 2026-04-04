use crate::{DataloaderError, UpstreamAdapter};

pub(crate) fn fetch_manifest_bytes(upstream: &UpstreamAdapter) -> Result<Vec<u8>, DataloaderError> {
    Err(DataloaderError::UnsupportedAdapter(upstream.clone()))
}

pub(crate) fn fetch_entry_bytes(
    upstream: &UpstreamAdapter,
    _locator: &str,
) -> Result<Vec<u8>, DataloaderError> {
    Err(DataloaderError::UnsupportedAdapter(upstream.clone()))
}
