use crate::DataloaderError;

pub(crate) fn fetch_manifest_bytes(base_url: &str) -> Result<Vec<u8>, DataloaderError> {
    http_get_bytes(format!(
        "{}/fetch-manifest.json",
        base_url.trim_end_matches('/')
    ))
}

pub(crate) fn fetch_entry_bytes(base_url: &str, locator: &str) -> Result<Vec<u8>, DataloaderError> {
    http_get_bytes(format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        locator.trim_start_matches('/')
    ))
}

#[cfg(not(target_arch = "wasm32"))]
fn http_get_bytes(url: String) -> Result<Vec<u8>, DataloaderError> {
    let response =
        reqwest::blocking::get(url).map_err(|error| DataloaderError::Http(error.to_string()))?;
    let bytes = response
        .error_for_status()
        .map_err(|error| DataloaderError::Http(error.to_string()))?
        .bytes()
        .map_err(|error| DataloaderError::Http(error.to_string()))?;
    Ok(bytes.to_vec())
}

#[cfg(target_arch = "wasm32")]
fn http_get_bytes(url: String) -> Result<Vec<u8>, DataloaderError> {
    futures::executor::block_on(async move {
        let response = reqwest::get(url)
            .await
            .map_err(|error| DataloaderError::Http(error.to_string()))?;
        let bytes = response
            .error_for_status()
            .map_err(|error| DataloaderError::Http(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| DataloaderError::Http(error.to_string()))?;
        Ok(bytes.to_vec())
    })
}
