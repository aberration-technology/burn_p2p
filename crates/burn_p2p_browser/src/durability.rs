use burn_p2p::{ArtifactId, ChunkId};
use burn_p2p_core::NetworkId;

#[cfg(target_arch = "wasm32")]
use crate::BrowserReceiptOutboxBackend;
use crate::{BrowserArtifactReplayCheckpoint, BrowserReceiptOutbox, BrowserStorageSnapshot};

#[cfg(target_arch = "wasm32")]
use base64::{Engine as _, engine::general_purpose::STANDARD};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;

#[cfg(target_arch = "wasm32")]
const RECEIPT_OUTBOX_STORAGE_PREFIX: &str = "burn-p2p.browser.receipt-outbox.";
#[cfg(target_arch = "wasm32")]
const RECEIPT_OUTBOX_INDEXED_DB_PREFIX: &str = "burn-p2p.browser.receipt-outbox.";
#[cfg(target_arch = "wasm32")]
const RECEIPT_OUTBOX_INDEXED_DB_STORE: &str = "receipt_outboxes";
#[cfg(target_arch = "wasm32")]
const RECEIPT_OUTBOX_INDEXED_DB_VALUE_KEY: &str = "default";
#[cfg(target_arch = "wasm32")]
const STORAGE_SNAPSHOT_STORAGE_PREFIX: &str = "burn-p2p.browser.storage.";
#[cfg(target_arch = "wasm32")]
const STORAGE_SNAPSHOT_INDEXED_DB_PREFIX: &str = "burn-p2p.browser.storage.";
#[cfg(target_arch = "wasm32")]
const STORAGE_SNAPSHOT_INDEXED_DB_STORE: &str = "storage_snapshots";
#[cfg(target_arch = "wasm32")]
const STORAGE_SNAPSHOT_INDEXED_DB_VALUE_KEY: &str = "default";
#[cfg(target_arch = "wasm32")]
const ARTIFACT_REPLAY_INDEXED_DB_PREFIX: &str = "burn-p2p.browser.artifact-replay.";
#[cfg(target_arch = "wasm32")]
const ARTIFACT_REPLAY_INDEXED_DB_STORE: &str = "artifact_replay_chunks";
#[cfg(target_arch = "wasm32")]
const ARTIFACT_REPLAY_PREFIX_INDEXED_DB_STORE: &str = "artifact_replay_prefixes";

#[cfg(target_arch = "wasm32")]
fn receipt_outbox_storage_key(network_id: &NetworkId) -> String {
    format!("{RECEIPT_OUTBOX_STORAGE_PREFIX}{}", network_id.as_str())
}

#[cfg(target_arch = "wasm32")]
fn receipt_outbox_indexed_db_name(network_id: &NetworkId) -> String {
    format!("{RECEIPT_OUTBOX_INDEXED_DB_PREFIX}{}", network_id.as_str())
}

#[cfg(target_arch = "wasm32")]
fn storage_snapshot_storage_key(network_id: &NetworkId) -> String {
    format!("{STORAGE_SNAPSHOT_STORAGE_PREFIX}{}", network_id.as_str())
}

#[cfg(target_arch = "wasm32")]
fn storage_snapshot_indexed_db_name(network_id: &NetworkId) -> String {
    format!(
        "{STORAGE_SNAPSHOT_INDEXED_DB_PREFIX}{}",
        network_id.as_str()
    )
}

#[cfg(target_arch = "wasm32")]
fn artifact_replay_indexed_db_name(network_id: &NetworkId) -> String {
    format!("{ARTIFACT_REPLAY_INDEXED_DB_PREFIX}{}", network_id.as_str())
}

#[cfg(target_arch = "wasm32")]
fn artifact_replay_chunk_value_key(artifact_id: &ArtifactId, chunk_id: &ChunkId) -> String {
    format!("{}:{}", artifact_id.as_str(), chunk_id.as_str())
}

#[cfg(target_arch = "wasm32")]
fn artifact_replay_prefix_value_key(artifact_id: &ArtifactId) -> String {
    artifact_id.as_str().to_owned()
}

#[cfg(target_arch = "wasm32")]
fn artifact_replay_segment_value_key(artifact_id: &ArtifactId, start_offset: u64) -> String {
    format!("segment:{}:{start_offset:020}", artifact_id.as_str())
}

#[cfg(target_arch = "wasm32")]
fn browser_local_storage() -> Option<web_sys::Storage> {
    web_sys::window()?.local_storage().ok().flatten()
}

#[cfg(target_arch = "wasm32")]
fn browser_indexed_db_factory() -> Option<web_sys::IdbFactory> {
    web_sys::window()?.indexed_db().ok().flatten()
}

#[cfg(target_arch = "wasm32")]
fn js_value_error_message(value: &JsValue) -> String {
    value
        .as_string()
        .or_else(|| {
            value
                .dyn_ref::<web_sys::DomException>()
                .map(|error| error.message())
        })
        .unwrap_or_else(|| format!("{value:?}"))
}

#[cfg(target_arch = "wasm32")]
fn dom_exception_error_message(error: Option<web_sys::DomException>, fallback: &str) -> String {
    error
        .map(|error| error.message())
        .filter(|message| !message.trim().is_empty())
        .unwrap_or_else(|| fallback.to_owned())
}

#[cfg(target_arch = "wasm32")]
async fn await_idb_request(request: &web_sys::IdbRequest) -> Result<JsValue, String> {
    let request = request.clone();
    let promise = js_sys::Promise::new(&mut move |resolve, reject| {
        let resolve_success = resolve.clone();
        let request_success = request.clone();
        let reject_success = reject.clone();
        let success =
            Closure::<dyn FnMut(web_sys::Event)>::wrap(Box::new(
                move |_event| match request_success.result() {
                    Ok(value) => {
                        let _ = resolve_success.call1(&JsValue::UNDEFINED, &value);
                    }
                    Err(error) => {
                        let _ = reject_success.call1(
                            &JsValue::UNDEFINED,
                            &JsValue::from_str(&js_value_error_message(&error)),
                        );
                    }
                },
            ));

        let reject_error = reject.clone();
        let request_error = request.clone();
        let error = Closure::<dyn FnMut(web_sys::Event)>::wrap(Box::new(move |_event| {
            let _ = reject_error.call1(
                &JsValue::UNDEFINED,
                &JsValue::from_str(&dom_exception_error_message(
                    request_error.error().ok().flatten(),
                    "indexeddb request failed",
                )),
            );
        }));

        request.set_onsuccess(Some(success.as_ref().unchecked_ref()));
        request.set_onerror(Some(error.as_ref().unchecked_ref()));
        success.forget();
        error.forget();
    });

    JsFuture::from(promise)
        .await
        .map_err(|error| js_value_error_message(&error))
}

#[cfg(target_arch = "wasm32")]
async fn await_indexed_db_transaction(transaction: &web_sys::IdbTransaction) -> Result<(), String> {
    let transaction = transaction.clone();
    let promise = js_sys::Promise::new(&mut move |resolve, reject| {
        let resolve_complete = resolve.clone();
        let complete = Closure::<dyn FnMut(web_sys::Event)>::wrap(Box::new(move |_event| {
            let _ = resolve_complete.call0(&JsValue::UNDEFINED);
        }));

        let reject_abort = reject.clone();
        let transaction_abort = transaction.clone();
        let abort = Closure::<dyn FnMut(web_sys::Event)>::wrap(Box::new(move |_event| {
            let _ = reject_abort.call1(
                &JsValue::UNDEFINED,
                &JsValue::from_str(&dom_exception_error_message(
                    transaction_abort.error(),
                    "indexeddb transaction aborted",
                )),
            );
        }));

        let reject_error = reject.clone();
        let transaction_error = transaction.clone();
        let error = Closure::<dyn FnMut(web_sys::Event)>::wrap(Box::new(move |_event| {
            let _ = reject_error.call1(
                &JsValue::UNDEFINED,
                &JsValue::from_str(&dom_exception_error_message(
                    transaction_error.error(),
                    "indexeddb transaction failed",
                )),
            );
        }));

        transaction.set_oncomplete(Some(complete.as_ref().unchecked_ref()));
        transaction.set_onabort(Some(abort.as_ref().unchecked_ref()));
        transaction.set_onerror(Some(error.as_ref().unchecked_ref()));
        complete.forget();
        abort.forget();
        error.forget();
    });

    JsFuture::from(promise)
        .await
        .map_err(|error| js_value_error_message(&error))
        .map(|_| ())
}

#[cfg(target_arch = "wasm32")]
async fn open_receipt_outbox_indexed_db(
    network_id: &NetworkId,
) -> Result<Option<web_sys::IdbDatabase>, String> {
    let Some(factory) = browser_indexed_db_factory() else {
        return Ok(None);
    };
    let request = factory
        .open_with_u32(&receipt_outbox_indexed_db_name(network_id), 1)
        .map_err(|error| format!("failed to open browser indexeddb receipt outbox: {error:?}"))?;
    let upgrade_request = request.clone();
    let on_upgrade_needed = Closure::<dyn FnMut(web_sys::Event)>::wrap(Box::new(move |_event| {
        if let Ok(result) = upgrade_request.result()
            && let Ok(database) = result.dyn_into::<web_sys::IdbDatabase>()
        {
            let _ = database.create_object_store(RECEIPT_OUTBOX_INDEXED_DB_STORE);
        }
    }));
    request.set_onupgradeneeded(Some(on_upgrade_needed.as_ref().unchecked_ref()));
    on_upgrade_needed.forget();

    let value = await_idb_request(request.as_ref()).await?;
    value
        .dyn_into::<web_sys::IdbDatabase>()
        .map(Some)
        .map_err(|_| "indexeddb open request did not yield a database".into())
}

#[cfg(target_arch = "wasm32")]
async fn open_storage_snapshot_indexed_db(
    network_id: &NetworkId,
) -> Result<Option<web_sys::IdbDatabase>, String> {
    let Some(factory) = browser_indexed_db_factory() else {
        return Ok(None);
    };
    let request = factory
        .open_with_u32(&storage_snapshot_indexed_db_name(network_id), 1)
        .map_err(|error| format!("failed to open browser indexeddb storage snapshot: {error:?}"))?;
    let upgrade_request = request.clone();
    let on_upgrade_needed = Closure::<dyn FnMut(web_sys::Event)>::wrap(Box::new(move |_event| {
        if let Ok(result) = upgrade_request.result()
            && let Ok(database) = result.dyn_into::<web_sys::IdbDatabase>()
        {
            let _ = database.create_object_store(STORAGE_SNAPSHOT_INDEXED_DB_STORE);
        }
    }));
    request.set_onupgradeneeded(Some(on_upgrade_needed.as_ref().unchecked_ref()));
    on_upgrade_needed.forget();

    let value = await_idb_request(request.as_ref()).await?;
    value
        .dyn_into::<web_sys::IdbDatabase>()
        .map(Some)
        .map_err(|_| "indexeddb open request did not yield a database".into())
}

#[cfg(target_arch = "wasm32")]
async fn open_artifact_replay_indexed_db(
    network_id: &NetworkId,
) -> Result<Option<web_sys::IdbDatabase>, String> {
    let Some(factory) = browser_indexed_db_factory() else {
        return Ok(None);
    };
    let request = factory
        .open_with_u32(&artifact_replay_indexed_db_name(network_id), 2)
        .map_err(|error| format!("failed to open browser indexeddb artifact replay: {error:?}"))?;
    let upgrade_request = request.clone();
    let on_upgrade_needed = Closure::<dyn FnMut(web_sys::Event)>::wrap(Box::new(move |_event| {
        if let Ok(result) = upgrade_request.result()
            && let Ok(database) = result.dyn_into::<web_sys::IdbDatabase>()
        {
            let _ = database.create_object_store(ARTIFACT_REPLAY_INDEXED_DB_STORE);
            let _ = database.create_object_store(ARTIFACT_REPLAY_PREFIX_INDEXED_DB_STORE);
        }
    }));
    request.set_onupgradeneeded(Some(on_upgrade_needed.as_ref().unchecked_ref()));
    on_upgrade_needed.forget();

    let value = await_idb_request(request.as_ref()).await?;
    value
        .dyn_into::<web_sys::IdbDatabase>()
        .map(Some)
        .map_err(|_| "indexeddb open request did not yield a database".into())
}

#[cfg(target_arch = "wasm32")]
async fn load_indexed_db_receipt_outbox(
    network_id: &NetworkId,
) -> Result<Option<BrowserReceiptOutbox>, String> {
    let Some(database) = open_receipt_outbox_indexed_db(network_id).await? else {
        return Ok(None);
    };
    let transaction = database
        .transaction_with_str_and_mode(
            RECEIPT_OUTBOX_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readonly,
        )
        .map_err(|error| format!("failed to open indexeddb readonly transaction: {error:?}"))?;
    let store = transaction
        .object_store(RECEIPT_OUTBOX_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb receipt outbox store: {error:?}"))?;
    let request = store
        .get(&JsValue::from_str(RECEIPT_OUTBOX_INDEXED_DB_VALUE_KEY))
        .map_err(|error| format!("failed to read indexeddb receipt outbox: {error:?}"))?;
    let value = await_idb_request(&request).await?;
    await_indexed_db_transaction(&transaction).await?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    let encoded = value
        .as_string()
        .ok_or_else(|| "indexeddb receipt outbox payload was not a string".to_owned())?;
    let mut outbox: BrowserReceiptOutbox = serde_json::from_str(&encoded)
        .map_err(|error| format!("failed to decode indexeddb receipt outbox: {error}"))?;
    outbox.configure_backend(BrowserReceiptOutboxBackend::IndexedDb);
    Ok(Some(outbox))
}

#[cfg(target_arch = "wasm32")]
async fn load_indexed_db_storage_snapshot(
    network_id: &NetworkId,
) -> Result<Option<BrowserStorageSnapshot>, String> {
    let Some(database) = open_storage_snapshot_indexed_db(network_id).await? else {
        return Ok(None);
    };
    let transaction = database
        .transaction_with_str_and_mode(
            STORAGE_SNAPSHOT_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readonly,
        )
        .map_err(|error| format!("failed to open indexeddb readonly transaction: {error:?}"))?;
    let store = transaction
        .object_store(STORAGE_SNAPSHOT_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb storage snapshot store: {error:?}"))?;
    let request = store
        .get(&JsValue::from_str(STORAGE_SNAPSHOT_INDEXED_DB_VALUE_KEY))
        .map_err(|error| format!("failed to read indexeddb storage snapshot: {error:?}"))?;
    let value = await_idb_request(&request).await?;
    await_indexed_db_transaction(&transaction).await?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    let encoded = value
        .as_string()
        .ok_or_else(|| "indexeddb storage snapshot payload was not a string".to_owned())?;
    let snapshot: BrowserStorageSnapshot = serde_json::from_str(&encoded)
        .map_err(|error| format!("failed to decode indexeddb storage snapshot: {error}"))?;
    Ok(Some(snapshot))
}

#[cfg(target_arch = "wasm32")]
async fn persist_indexed_db_receipt_outbox(
    network_id: &NetworkId,
    outbox: &BrowserReceiptOutbox,
) -> Result<(), String> {
    let Some(database) = open_receipt_outbox_indexed_db(network_id).await? else {
        return Err("browser does not expose indexeddb".into());
    };
    let transaction = database
        .transaction_with_str_and_mode(
            RECEIPT_OUTBOX_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readwrite,
        )
        .map_err(|error| format!("failed to open indexeddb readwrite transaction: {error:?}"))?;
    let store = transaction
        .object_store(RECEIPT_OUTBOX_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb receipt outbox store: {error:?}"))?;
    let mut durable = outbox.clone();
    durable.configure_backend(BrowserReceiptOutboxBackend::IndexedDb);
    let encoded = serde_json::to_string(&durable)
        .map_err(|error| format!("failed to encode indexeddb receipt outbox: {error}"))?;
    let request = store
        .put_with_key(
            &JsValue::from_str(&encoded),
            &JsValue::from_str(RECEIPT_OUTBOX_INDEXED_DB_VALUE_KEY),
        )
        .map_err(|error| format!("failed to persist indexeddb receipt outbox: {error:?}"))?;
    let _ = await_idb_request(&request).await?;
    await_indexed_db_transaction(&transaction).await
}

#[cfg(target_arch = "wasm32")]
async fn persist_indexed_db_storage_snapshot(
    network_id: &NetworkId,
    snapshot: &BrowserStorageSnapshot,
) -> Result<(), String> {
    let Some(database) = open_storage_snapshot_indexed_db(network_id).await? else {
        return Err("browser does not expose indexeddb".into());
    };
    let transaction = database
        .transaction_with_str_and_mode(
            STORAGE_SNAPSHOT_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readwrite,
        )
        .map_err(|error| format!("failed to open indexeddb readwrite transaction: {error:?}"))?;
    let store = transaction
        .object_store(STORAGE_SNAPSHOT_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb storage snapshot store: {error:?}"))?;
    let encoded = serde_json::to_string(snapshot)
        .map_err(|error| format!("failed to encode indexeddb storage snapshot: {error}"))?;
    let request = store
        .put_with_key(
            &JsValue::from_str(&encoded),
            &JsValue::from_str(STORAGE_SNAPSHOT_INDEXED_DB_VALUE_KEY),
        )
        .map_err(|error| format!("failed to persist indexeddb storage snapshot: {error:?}"))?;
    let _ = await_idb_request(&request).await?;
    await_indexed_db_transaction(&transaction).await
}

#[cfg(target_arch = "wasm32")]
async fn load_indexed_db_artifact_replay_chunk(
    network_id: &NetworkId,
    artifact_id: &ArtifactId,
    chunk_id: &ChunkId,
) -> Result<Option<Vec<u8>>, String> {
    let Some(database) = open_artifact_replay_indexed_db(network_id).await? else {
        return Ok(None);
    };
    let transaction = database
        .transaction_with_str_and_mode(
            ARTIFACT_REPLAY_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readonly,
        )
        .map_err(|error| format!("failed to open indexeddb readonly transaction: {error:?}"))?;
    let store = transaction
        .object_store(ARTIFACT_REPLAY_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb artifact replay store: {error:?}"))?;
    let request = store
        .get(&JsValue::from_str(&artifact_replay_chunk_value_key(
            artifact_id,
            chunk_id,
        )))
        .map_err(|error| format!("failed to read indexeddb replay chunk: {error:?}"))?;
    let value = await_idb_request(&request).await?;
    await_indexed_db_transaction(&transaction).await?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    let encoded = value
        .as_string()
        .ok_or_else(|| "indexeddb replay chunk payload was not a string".to_owned())?;
    STANDARD
        .decode(encoded.as_bytes())
        .map(Some)
        .map_err(|error| format!("failed to decode indexeddb replay chunk payload: {error}"))
}

#[cfg(target_arch = "wasm32")]
async fn load_indexed_db_artifact_replay_prefix(
    network_id: &NetworkId,
    artifact_id: &ArtifactId,
) -> Result<Option<Vec<u8>>, String> {
    let Some(database) = open_artifact_replay_indexed_db(network_id).await? else {
        return Ok(None);
    };
    let transaction = database
        .transaction_with_str_and_mode(
            ARTIFACT_REPLAY_PREFIX_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readonly,
        )
        .map_err(|error| format!("failed to open indexeddb readonly transaction: {error:?}"))?;
    let store = transaction
        .object_store(ARTIFACT_REPLAY_PREFIX_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb replay prefix store: {error:?}"))?;
    let request = store
        .get(&JsValue::from_str(&artifact_replay_prefix_value_key(
            artifact_id,
        )))
        .map_err(|error| format!("failed to read indexeddb replay prefix: {error:?}"))?;
    let value = await_idb_request(&request).await?;
    await_indexed_db_transaction(&transaction).await?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    let encoded = value
        .as_string()
        .ok_or_else(|| "indexeddb replay prefix payload was not a string".to_owned())?;
    STANDARD
        .decode(encoded.as_bytes())
        .map(Some)
        .map_err(|error| format!("failed to decode indexeddb replay prefix payload: {error}"))
}

#[cfg(target_arch = "wasm32")]
async fn load_indexed_db_artifact_replay_segment(
    network_id: &NetworkId,
    artifact_id: &ArtifactId,
    start_offset: u64,
) -> Result<Option<Vec<u8>>, String> {
    let Some(database) = open_artifact_replay_indexed_db(network_id).await? else {
        return Ok(None);
    };
    let transaction = database
        .transaction_with_str_and_mode(
            ARTIFACT_REPLAY_PREFIX_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readonly,
        )
        .map_err(|error| format!("failed to open indexeddb readonly transaction: {error:?}"))?;
    let store = transaction
        .object_store(ARTIFACT_REPLAY_PREFIX_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb replay prefix store: {error:?}"))?;
    let request = store
        .get(&JsValue::from_str(&artifact_replay_segment_value_key(
            artifact_id,
            start_offset,
        )))
        .map_err(|error| format!("failed to read indexeddb replay segment: {error:?}"))?;
    let value = await_idb_request(&request).await?;
    await_indexed_db_transaction(&transaction).await?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    let encoded = value
        .as_string()
        .ok_or_else(|| "indexeddb replay segment payload was not a string".to_owned())?;
    STANDARD
        .decode(encoded.as_bytes())
        .map(Some)
        .map_err(|error| format!("failed to decode indexeddb replay segment payload: {error}"))
}

#[cfg(target_arch = "wasm32")]
async fn load_indexed_db_artifact_replay_prefix_from_segments(
    network_id: &NetworkId,
    checkpoint: &BrowserArtifactReplayCheckpoint,
) -> Result<Option<Vec<u8>>, String> {
    if checkpoint.edge_download_segments.is_empty() {
        return Ok(None);
    }

    let mut segments = checkpoint.edge_download_segments.clone();
    segments.sort_by_key(|segment| segment.start_offset);
    let mut prefix = Vec::new();
    let mut expected_offset = 0u64;

    for segment in segments {
        if segment.start_offset > expected_offset {
            break;
        }
        let mut bytes = if !segment.bytes.is_empty() {
            segment.bytes.clone()
        } else {
            load_indexed_db_artifact_replay_segment(
                network_id,
                &checkpoint.artifact_id,
                segment.start_offset,
            )
            .await?
            .unwrap_or_default()
        };
        if bytes.is_empty() {
            break;
        }
        let segment_len = segment.persisted_bytes.max(bytes.len() as u64);
        let overlap = expected_offset.saturating_sub(segment.start_offset) as usize;
        if overlap >= bytes.len() {
            expected_offset = segment.start_offset.saturating_add(segment_len);
            continue;
        }
        prefix.extend_from_slice(&bytes.split_off(overlap));
        expected_offset = segment.start_offset.saturating_add(segment_len);
    }

    Ok((!prefix.is_empty()).then_some(prefix))
}

#[cfg(target_arch = "wasm32")]
async fn persist_indexed_db_artifact_replay_chunks(
    network_id: &NetworkId,
    checkpoint: Option<&BrowserArtifactReplayCheckpoint>,
) -> Result<(), String> {
    let Some(database) = open_artifact_replay_indexed_db(network_id).await? else {
        return Err("browser does not expose indexeddb".into());
    };
    let transaction = database
        .transaction_with_str_and_mode(
            ARTIFACT_REPLAY_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readwrite,
        )
        .map_err(|error| format!("failed to open indexeddb readwrite transaction: {error:?}"))?;
    let store = transaction
        .object_store(ARTIFACT_REPLAY_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb artifact replay store: {error:?}"))?;
    let clear_request = store
        .clear()
        .map_err(|error| format!("failed to clear indexeddb artifact replay store: {error:?}"))?;
    let _ = await_idb_request(&clear_request).await?;
    if let Some(checkpoint) = checkpoint {
        for chunk in &checkpoint.completed_chunks {
            if chunk.chunk_bytes.is_empty() {
                continue;
            }
            let encoded = STANDARD.encode(&chunk.chunk_bytes);
            let request = store
                .put_with_key(
                    &JsValue::from_str(&encoded),
                    &JsValue::from_str(&artifact_replay_chunk_value_key(
                        &checkpoint.artifact_id,
                        &chunk.chunk_id,
                    )),
                )
                .map_err(|error| {
                    format!("failed to persist indexeddb artifact replay chunk: {error:?}")
                })?;
            let _ = await_idb_request(&request).await?;
        }
    }
    await_indexed_db_transaction(&transaction).await
}

#[cfg(target_arch = "wasm32")]
async fn persist_indexed_db_artifact_replay_prefix(
    network_id: &NetworkId,
    checkpoint: Option<&BrowserArtifactReplayCheckpoint>,
) -> Result<(), String> {
    let Some(database) = open_artifact_replay_indexed_db(network_id).await? else {
        return Err("browser does not expose indexeddb".into());
    };
    let transaction = database
        .transaction_with_str_and_mode(
            ARTIFACT_REPLAY_PREFIX_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readwrite,
        )
        .map_err(|error| format!("failed to open indexeddb readwrite transaction: {error:?}"))?;
    let store = transaction
        .object_store(ARTIFACT_REPLAY_PREFIX_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb replay prefix store: {error:?}"))?;
    let clear_request = store
        .clear()
        .map_err(|error| format!("failed to clear indexeddb replay prefix store: {error:?}"))?;
    let _ = await_idb_request(&clear_request).await?;
    if let Some(checkpoint) = checkpoint {
        let mut persisted_segment = false;
        for segment in &checkpoint.edge_download_segments {
            if segment.bytes.is_empty() {
                continue;
            }
            persisted_segment = true;
            let encoded = STANDARD.encode(&segment.bytes);
            let request = store
                .put_with_key(
                    &JsValue::from_str(&encoded),
                    &JsValue::from_str(&artifact_replay_segment_value_key(
                        &checkpoint.artifact_id,
                        segment.start_offset,
                    )),
                )
                .map_err(|error| {
                    format!("failed to persist indexeddb artifact replay segment: {error:?}")
                })?;
            let _ = await_idb_request(&request).await?;
        }
        if !persisted_segment
            && let Some(prefix) = checkpoint.edge_download_prefix.as_ref()
            && !prefix.bytes.is_empty()
        {
            let encoded = STANDARD.encode(&prefix.bytes);
            let request = store
                .put_with_key(
                    &JsValue::from_str(&encoded),
                    &JsValue::from_str(&artifact_replay_prefix_value_key(&checkpoint.artifact_id)),
                )
                .map_err(|error| {
                    format!("failed to persist indexeddb artifact replay prefix: {error:?}")
                })?;
            let _ = await_idb_request(&request).await?;
        }
    }
    await_indexed_db_transaction(&transaction).await
}

#[cfg(target_arch = "wasm32")]
async fn clear_indexed_db_receipt_outbox(network_id: &NetworkId) -> Result<(), String> {
    let Some(database) = open_receipt_outbox_indexed_db(network_id).await? else {
        return Ok(());
    };
    let transaction = database
        .transaction_with_str_and_mode(
            RECEIPT_OUTBOX_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readwrite,
        )
        .map_err(|error| format!("failed to open indexeddb readwrite transaction: {error:?}"))?;
    let store = transaction
        .object_store(RECEIPT_OUTBOX_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb receipt outbox store: {error:?}"))?;
    let request = store
        .delete(&JsValue::from_str(RECEIPT_OUTBOX_INDEXED_DB_VALUE_KEY))
        .map_err(|error| format!("failed to clear indexeddb receipt outbox: {error:?}"))?;
    let _ = await_idb_request(&request).await?;
    await_indexed_db_transaction(&transaction).await
}

#[cfg(target_arch = "wasm32")]
async fn clear_indexed_db_storage_snapshot(network_id: &NetworkId) -> Result<(), String> {
    let Some(database) = open_storage_snapshot_indexed_db(network_id).await? else {
        return Ok(());
    };
    let transaction = database
        .transaction_with_str_and_mode(
            STORAGE_SNAPSHOT_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readwrite,
        )
        .map_err(|error| format!("failed to open indexeddb readwrite transaction: {error:?}"))?;
    let store = transaction
        .object_store(STORAGE_SNAPSHOT_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb storage snapshot store: {error:?}"))?;
    let request = store
        .delete(&JsValue::from_str(STORAGE_SNAPSHOT_INDEXED_DB_VALUE_KEY))
        .map_err(|error| format!("failed to clear indexeddb storage snapshot: {error:?}"))?;
    let _ = await_idb_request(&request).await?;
    await_indexed_db_transaction(&transaction).await
}

#[cfg(target_arch = "wasm32")]
async fn clear_indexed_db_artifact_replay_chunks(network_id: &NetworkId) -> Result<(), String> {
    let Some(database) = open_artifact_replay_indexed_db(network_id).await? else {
        return Ok(());
    };
    let transaction = database
        .transaction_with_str_and_mode(
            ARTIFACT_REPLAY_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readwrite,
        )
        .map_err(|error| format!("failed to open indexeddb readwrite transaction: {error:?}"))?;
    let store = transaction
        .object_store(ARTIFACT_REPLAY_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb artifact replay store: {error:?}"))?;
    let request = store
        .clear()
        .map_err(|error| format!("failed to clear indexeddb artifact replay store: {error:?}"))?;
    let _ = await_idb_request(&request).await?;
    await_indexed_db_transaction(&transaction).await
}

#[cfg(target_arch = "wasm32")]
async fn clear_indexed_db_artifact_replay_prefix(network_id: &NetworkId) -> Result<(), String> {
    let Some(database) = open_artifact_replay_indexed_db(network_id).await? else {
        return Ok(());
    };
    let transaction = database
        .transaction_with_str_and_mode(
            ARTIFACT_REPLAY_PREFIX_INDEXED_DB_STORE,
            web_sys::IdbTransactionMode::Readwrite,
        )
        .map_err(|error| format!("failed to open indexeddb readwrite transaction: {error:?}"))?;
    let store = transaction
        .object_store(ARTIFACT_REPLAY_PREFIX_INDEXED_DB_STORE)
        .map_err(|error| format!("failed to open indexeddb replay prefix store: {error:?}"))?;
    let request = store
        .clear()
        .map_err(|error| format!("failed to clear indexeddb replay prefix store: {error:?}"))?;
    let _ = await_idb_request(&request).await?;
    await_indexed_db_transaction(&transaction).await
}

#[cfg(target_arch = "wasm32")]
fn load_local_storage_receipt_outbox(
    network_id: &NetworkId,
) -> Result<Option<BrowserReceiptOutbox>, String> {
    let Some(storage) = browser_local_storage() else {
        return Ok(None);
    };
    let Some(value) = storage
        .get_item(&receipt_outbox_storage_key(network_id))
        .map_err(|error| format!("failed to read browser receipt outbox: {error:?}"))?
    else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let mut outbox: BrowserReceiptOutbox = serde_json::from_str(trimmed)
        .map_err(|error| format!("failed to decode browser receipt outbox: {error}"))?;
    outbox.configure_backend(BrowserReceiptOutboxBackend::LocalStorage);
    Ok(Some(outbox))
}

#[cfg(target_arch = "wasm32")]
fn persist_local_storage_receipt_outbox(
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
fn clear_local_storage_receipt_outbox(network_id: &NetworkId) -> Result<(), String> {
    let Some(storage) = browser_local_storage() else {
        return Ok(());
    };
    storage
        .remove_item(&receipt_outbox_storage_key(network_id))
        .map_err(|error| format!("failed to clear browser receipt outbox: {error:?}"))
}

#[cfg(target_arch = "wasm32")]
fn load_local_storage_storage_snapshot(
    network_id: &NetworkId,
) -> Result<Option<BrowserStorageSnapshot>, String> {
    let Some(storage) = browser_local_storage() else {
        return Ok(None);
    };
    let Some(value) = storage
        .get_item(&storage_snapshot_storage_key(network_id))
        .map_err(|error| format!("failed to read browser storage snapshot: {error:?}"))?
    else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let snapshot: BrowserStorageSnapshot = serde_json::from_str(trimmed)
        .map_err(|error| format!("failed to decode browser storage snapshot: {error}"))?;
    Ok(Some(snapshot))
}

#[cfg(target_arch = "wasm32")]
fn persist_local_storage_storage_snapshot(
    network_id: &NetworkId,
    snapshot: &BrowserStorageSnapshot,
) -> Result<(), String> {
    let Some(storage) = browser_local_storage() else {
        return Ok(());
    };
    let encoded = serde_json::to_string(snapshot)
        .map_err(|error| format!("failed to encode browser storage snapshot: {error}"))?;
    storage
        .set_item(&storage_snapshot_storage_key(network_id), &encoded)
        .map_err(|error| format!("failed to persist browser storage snapshot: {error:?}"))
}

#[cfg(target_arch = "wasm32")]
fn clear_local_storage_storage_snapshot(network_id: &NetworkId) -> Result<(), String> {
    let Some(storage) = browser_local_storage() else {
        return Ok(());
    };
    storage
        .remove_item(&storage_snapshot_storage_key(network_id))
        .map_err(|error| format!("failed to clear browser storage snapshot: {error:?}"))
}

#[cfg(target_arch = "wasm32")]
pub async fn load_durable_receipt_outbox(
    network_id: &NetworkId,
) -> Result<BrowserReceiptOutbox, String> {
    if let Some(outbox) = load_indexed_db_receipt_outbox(network_id).await? {
        return Ok(outbox);
    }
    if let Some(outbox) = load_local_storage_receipt_outbox(network_id)? {
        if browser_indexed_db_factory().is_some()
            && persist_indexed_db_receipt_outbox(network_id, &outbox)
                .await
                .is_ok()
        {
            let mut migrated = outbox.clone();
            migrated.configure_backend(BrowserReceiptOutboxBackend::IndexedDb);
            return Ok(migrated);
        }
        return Ok(outbox);
    }

    let mut outbox = BrowserReceiptOutbox::default();
    outbox.configure_backend(if browser_indexed_db_factory().is_some() {
        BrowserReceiptOutboxBackend::IndexedDb
    } else {
        BrowserReceiptOutboxBackend::LocalStorage
    });
    Ok(outbox)
}

#[cfg(target_arch = "wasm32")]
pub async fn load_durable_browser_storage(
    network_id: &NetworkId,
) -> Result<BrowserStorageSnapshot, String> {
    if let Some(snapshot) = load_indexed_db_storage_snapshot(network_id).await? {
        return Ok(snapshot);
    }
    if let Some(snapshot) = load_local_storage_storage_snapshot(network_id)? {
        if browser_indexed_db_factory().is_some()
            && persist_indexed_db_artifact_replay_chunks(
                network_id,
                snapshot.artifact_replay_checkpoint.as_ref(),
            )
            .await
            .is_ok()
            && persist_indexed_db_artifact_replay_prefix(
                network_id,
                snapshot.artifact_replay_checkpoint.as_ref(),
            )
            .await
            .is_ok()
            && persist_indexed_db_storage_snapshot(network_id, &snapshot.durable_replay_snapshot())
                .await
                .is_ok()
        {
            return Ok(snapshot);
        }
        return Ok(snapshot);
    }
    Ok(BrowserStorageSnapshot::default())
}

#[cfg(target_arch = "wasm32")]
pub async fn persist_durable_receipt_outbox(
    network_id: &NetworkId,
    outbox: &BrowserReceiptOutbox,
) -> Result<(), String> {
    if browser_indexed_db_factory().is_some() {
        persist_indexed_db_receipt_outbox(network_id, outbox).await?;
        return clear_local_storage_receipt_outbox(network_id);
    }
    persist_local_storage_receipt_outbox(network_id, outbox)
}

#[cfg(target_arch = "wasm32")]
pub async fn persist_durable_browser_storage(
    network_id: &NetworkId,
    snapshot: &BrowserStorageSnapshot,
) -> Result<(), String> {
    if browser_indexed_db_factory().is_some() {
        persist_indexed_db_artifact_replay_chunks(
            network_id,
            snapshot.artifact_replay_checkpoint.as_ref(),
        )
        .await?;
        persist_indexed_db_artifact_replay_prefix(
            network_id,
            snapshot.artifact_replay_checkpoint.as_ref(),
        )
        .await?;
        persist_indexed_db_storage_snapshot(network_id, &snapshot.durable_replay_snapshot())
            .await?;
        return clear_local_storage_storage_snapshot(network_id);
    }
    persist_local_storage_storage_snapshot(network_id, snapshot)
}

#[cfg(target_arch = "wasm32")]
pub async fn load_durable_browser_artifact_replay_chunk(
    network_id: &NetworkId,
    artifact_id: &ArtifactId,
    chunk_id: &ChunkId,
) -> Result<Option<Vec<u8>>, String> {
    load_indexed_db_artifact_replay_chunk(network_id, artifact_id, chunk_id).await
}

#[cfg(target_arch = "wasm32")]
pub async fn load_durable_browser_artifact_replay_prefix(
    network_id: &NetworkId,
    checkpoint: &BrowserArtifactReplayCheckpoint,
) -> Result<Option<Vec<u8>>, String> {
    if let Some(prefix) =
        load_indexed_db_artifact_replay_prefix_from_segments(network_id, checkpoint)
            .await
            .unwrap_or(None)
    {
        return Ok(Some(prefix));
    }
    load_indexed_db_artifact_replay_prefix(network_id, &checkpoint.artifact_id).await
}

#[cfg(target_arch = "wasm32")]
pub async fn clear_durable_receipt_outbox(network_id: &NetworkId) -> Result<(), String> {
    let indexed_db_result = clear_indexed_db_receipt_outbox(network_id).await;
    let local_storage_result = clear_local_storage_receipt_outbox(network_id);
    indexed_db_result?;
    local_storage_result
}

#[cfg(target_arch = "wasm32")]
pub async fn clear_durable_browser_storage(network_id: &NetworkId) -> Result<(), String> {
    let indexed_db_result = clear_indexed_db_storage_snapshot(network_id).await;
    let replay_chunk_result = clear_indexed_db_artifact_replay_chunks(network_id).await;
    let replay_prefix_result = clear_indexed_db_artifact_replay_prefix(network_id).await;
    let local_storage_result = clear_local_storage_storage_snapshot(network_id);
    indexed_db_result?;
    replay_chunk_result?;
    replay_prefix_result?;
    local_storage_result
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn load_durable_receipt_outbox(
    _network_id: &NetworkId,
) -> Result<BrowserReceiptOutbox, String> {
    Ok(BrowserReceiptOutbox::default())
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn load_durable_browser_storage(
    _network_id: &NetworkId,
) -> Result<BrowserStorageSnapshot, String> {
    Ok(BrowserStorageSnapshot::default())
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn persist_durable_receipt_outbox(
    _network_id: &NetworkId,
    _outbox: &BrowserReceiptOutbox,
) -> Result<(), String> {
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn persist_durable_browser_storage(
    _network_id: &NetworkId,
    _snapshot: &BrowserStorageSnapshot,
) -> Result<(), String> {
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn load_durable_browser_artifact_replay_chunk(
    _network_id: &NetworkId,
    _artifact_id: &ArtifactId,
    _chunk_id: &ChunkId,
) -> Result<Option<Vec<u8>>, String> {
    Ok(None)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn load_durable_browser_artifact_replay_prefix(
    _network_id: &NetworkId,
    _checkpoint: &BrowserArtifactReplayCheckpoint,
) -> Result<Option<Vec<u8>>, String> {
    Ok(None)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn clear_durable_receipt_outbox(_network_id: &NetworkId) -> Result<(), String> {
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn clear_durable_browser_storage(_network_id: &NetworkId) -> Result<(), String> {
    Ok(())
}
