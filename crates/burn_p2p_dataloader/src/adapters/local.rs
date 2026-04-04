use std::{fs, path::Path};

use crate::DataloaderError;

pub(crate) fn fetch_manifest_bytes(root: &str) -> Result<Vec<u8>, DataloaderError> {
    fs::read(Path::new(root).join("fetch-manifest.json"))
        .map_err(|error| DataloaderError::Io(error.to_string()))
}

pub(crate) fn fetch_entry_bytes(root: &str, locator: &str) -> Result<Vec<u8>, DataloaderError> {
    fs::read(Path::new(root).join(locator)).map_err(|error| DataloaderError::Io(error.to_string()))
}
