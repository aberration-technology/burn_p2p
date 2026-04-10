use burn_p2p_browser::{
    BrowserAppController, BrowserCapabilityReport, BrowserGpuSupport, BrowserRuntimeRole,
};
use burn_p2p_views::{BrowserAppClientView, BrowserAppStaticBootstrap};
use js_sys::Reflect;
use wasm_bindgen::JsValue;
use web_sys::window;

const SURFACE_STORAGE_KEY: &str = "burn-p2p.surface";
const SELECTION_STORAGE_KEY: &str = "burn-p2p.selection";

fn document() -> Result<web_sys::Document, String> {
    window()
        .and_then(|window| window.document())
        .ok_or_else(|| "window.document is not available".into())
}

pub(crate) fn load_bootstrap_from_document() -> Result<BrowserAppStaticBootstrap, String> {
    let document = document()?;
    let node = document
        .get_element_by_id("browser-app-static-bootstrap")
        .ok_or_else(|| "missing #browser-app-static-bootstrap".to_owned())?;
    let json = node
        .text_content()
        .ok_or_else(|| "static bootstrap node did not contain JSON".to_owned())?;
    serde_json::from_str(&json).map_err(|error| format!("invalid static bootstrap JSON: {error}"))
}

pub(crate) fn resolve_edge_base_url(bootstrap: &BrowserAppStaticBootstrap) -> String {
    resolve_edge_candidate(
        bootstrap.default_edge_url.as_deref().unwrap_or_default(),
        bootstrap,
    )
}

pub(crate) fn load_saved_surface() -> Option<burn_p2p_views::BrowserAppSurface> {
    let storage = storage()?;
    let value = storage.get_item(SURFACE_STORAGE_KEY).ok().flatten()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(burn_p2p_views::BrowserAppSurface::from_key(trimmed))
    }
}

pub(crate) fn save_surface(surface: burn_p2p_views::BrowserAppSurface) {
    if let Some(storage) = storage() {
        let _ = storage.set_item(SURFACE_STORAGE_KEY, surface.as_str());
    }
}

pub(crate) fn load_saved_selection() -> Option<(String, Option<String>)> {
    let storage = storage()?;
    let value = storage.get_item(SELECTION_STORAGE_KEY).ok().flatten()?;
    serde_json::from_str(&value).ok()
}

pub(crate) fn save_selection(selection: &Option<(String, Option<String>)>) {
    let Some(storage) = storage() else {
        return;
    };
    if let Some(selection) = selection {
        if let Ok(json) = serde_json::to_string(selection) {
            let _ = storage.set_item(SELECTION_STORAGE_KEY, &json);
        }
    } else {
        let _ = storage.remove_item(SELECTION_STORAGE_KEY);
    }
}

fn resolve_edge_candidate(value: &str, bootstrap: &BrowserAppStaticBootstrap) -> String {
    let location = window().map(|window| window.location());
    let origin = location
        .as_ref()
        .and_then(|location| location.origin().ok())
        .unwrap_or_default();
    let edge_base_url = if value.is_empty() {
        bootstrap.default_edge_url.as_deref().unwrap_or_default()
    } else {
        value
    };
    if edge_base_url.is_empty() {
        return origin;
    }
    if edge_base_url.starts_with("http://") || edge_base_url.starts_with("https://") {
        return edge_base_url.trim_end_matches('/').to_owned();
    }
    if edge_base_url.starts_with('/') {
        return format!("{origin}{}", edge_base_url.trim_end_matches('/'));
    }
    if origin.is_empty() {
        return edge_base_url.trim_end_matches('/').to_owned();
    }
    format!("{origin}/{}", edge_base_url.trim_matches('/'))
}

fn storage() -> Option<web_sys::Storage> {
    window()?.local_storage().ok().flatten()
}

pub(crate) async fn connect_controller(
    edge_base_url: &str,
    requested_role: BrowserRuntimeRole,
    selected_experiment: Option<(String, Option<String>)>,
) -> Result<BrowserAppController, String> {
    BrowserAppController::connect(
        edge_base_url,
        detect_capability(),
        requested_role,
        selected_experiment,
    )
    .await
    .map_err(|error| format!("failed to start local runtime: {error}"))
}

pub(crate) async fn refresh_controller(
    controller: &mut BrowserAppController,
) -> Result<BrowserAppClientView, String> {
    controller
        .refresh()
        .await
        .map_err(|error| format!("failed to sync local runtime: {error}"))
}

pub(crate) fn log_error(message: &str) {
    web_sys::console::error_1(&JsValue::from_str(message));
}

fn detect_capability() -> BrowserCapabilityReport {
    let mut report = BrowserCapabilityReport::default();
    let window_value = window().map(JsValue::from);
    let navigator = window_value
        .as_ref()
        .and_then(|window| Reflect::get(window, &JsValue::from_str("navigator")).ok());
    let has_webgpu = navigator
        .as_ref()
        .and_then(|navigator| Reflect::get(navigator, &JsValue::from_str("gpu")).ok())
        .is_some_and(|value| !value.is_null() && !value.is_undefined());
    let has_worker = window_value
        .as_ref()
        .and_then(|window| Reflect::get(window, &JsValue::from_str("Worker")).ok())
        .is_some_and(|value| !value.is_null() && !value.is_undefined());
    let has_storage_manager = navigator
        .as_ref()
        .and_then(|navigator| Reflect::get(navigator, &JsValue::from_str("storage")).ok())
        .is_some_and(|value| !value.is_null() && !value.is_undefined());
    let has_web_transport = window_value
        .as_ref()
        .and_then(|window| Reflect::get(window, &JsValue::from_str("WebTransport")).ok())
        .is_some_and(|value| !value.is_null() && !value.is_undefined());
    let has_webrtc = window_value
        .as_ref()
        .and_then(|window| Reflect::get(window, &JsValue::from_str("RTCPeerConnection")).ok())
        .is_some_and(|value| !value.is_null() && !value.is_undefined());

    report.dedicated_worker = if has_worker {
        burn_p2p_browser::BrowserWorkerSupport::DedicatedWorker
    } else {
        burn_p2p_browser::BrowserWorkerSupport::Unavailable("worker unavailable".into())
    };
    report.persistent_storage_exposed = has_storage_manager;
    report.web_transport_exposed = has_web_transport;
    report.web_rtc_exposed = has_webrtc;

    if has_webgpu {
        report.navigator_gpu_exposed = true;
        report.worker_gpu_exposed = has_worker;
        report.gpu_support = BrowserGpuSupport::Available;
        report.recommended_role = BrowserRuntimeRole::BrowserTrainerWgpu;
    } else if has_worker {
        report.gpu_support = BrowserGpuSupport::Unavailable("webgpu unavailable".into());
        report.recommended_role = BrowserRuntimeRole::BrowserVerifier;
    } else {
        report.gpu_support = BrowserGpuSupport::Unavailable("webgpu unavailable".into());
        report.recommended_role = BrowserRuntimeRole::BrowserFallback;
    }

    report
}
