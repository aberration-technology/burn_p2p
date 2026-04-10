use burn_p2p_core::NetworkId;

use crate::BrowserReceiptOutbox;
#[cfg(target_arch = "wasm32")]
use crate::BrowserReceiptOutboxBackend;

#[cfg(target_arch = "wasm32")]
const RECEIPT_OUTBOX_STORAGE_PREFIX: &str = "burn-p2p.browser.receipt-outbox.";

#[cfg(target_arch = "wasm32")]
fn receipt_outbox_storage_key(network_id: &NetworkId) -> String {
    format!("{RECEIPT_OUTBOX_STORAGE_PREFIX}{}", network_id.as_str())
}

#[cfg(target_arch = "wasm32")]
fn browser_local_storage() -> Option<web_sys::Storage> {
    web_sys::window()?.local_storage().ok().flatten()
}

#[cfg(target_arch = "wasm32")]
pub fn load_durable_receipt_outbox(network_id: &NetworkId) -> Result<BrowserReceiptOutbox, String> {
    let Some(storage) = browser_local_storage() else {
        return Ok(BrowserReceiptOutbox::default());
    };
    let Some(value) = storage
        .get_item(&receipt_outbox_storage_key(network_id))
        .map_err(|error| format!("failed to read browser receipt outbox: {error:?}"))?
    else {
        let mut outbox = BrowserReceiptOutbox::default();
        outbox.configure_backend(BrowserReceiptOutboxBackend::LocalStorage);
        return Ok(outbox);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        let mut outbox = BrowserReceiptOutbox::default();
        outbox.configure_backend(BrowserReceiptOutboxBackend::LocalStorage);
        return Ok(outbox);
    }
    let mut outbox: BrowserReceiptOutbox = serde_json::from_str(trimmed)
        .map_err(|error| format!("failed to decode browser receipt outbox: {error}"))?;
    outbox.configure_backend(BrowserReceiptOutboxBackend::LocalStorage);
    Ok(outbox)
}

#[cfg(target_arch = "wasm32")]
pub fn persist_durable_receipt_outbox(
    network_id: &NetworkId,
    outbox: &BrowserReceiptOutbox,
) -> Result<(), String> {
    let Some(storage) = browser_local_storage() else {
        return Ok(());
    };
    let mut durable = outbox.clone();
    durable.configure_backend(BrowserReceiptOutboxBackend::LocalStorage);
    let encoded = serde_json::to_string(&durable)
        .map_err(|error| format!("failed to encode browser receipt outbox: {error}"))?;
    storage
        .set_item(&receipt_outbox_storage_key(network_id), &encoded)
        .map_err(|error| format!("failed to persist browser receipt outbox: {error:?}"))
}

#[cfg(target_arch = "wasm32")]
pub fn clear_durable_receipt_outbox(network_id: &NetworkId) -> Result<(), String> {
    let Some(storage) = browser_local_storage() else {
        return Ok(());
    };
    storage
        .remove_item(&receipt_outbox_storage_key(network_id))
        .map_err(|error| format!("failed to clear browser receipt outbox: {error:?}"))
}

#[cfg(not(target_arch = "wasm32"))]
pub fn load_durable_receipt_outbox(
    _network_id: &NetworkId,
) -> Result<BrowserReceiptOutbox, String> {
    Ok(BrowserReceiptOutbox::default())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn persist_durable_receipt_outbox(
    _network_id: &NetworkId,
    _outbox: &BrowserReceiptOutbox,
) -> Result<(), String> {
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub fn clear_durable_receipt_outbox(_network_id: &NetworkId) -> Result<(), String> {
    Ok(())
}
