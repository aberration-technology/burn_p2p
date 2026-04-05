mod bridge;
mod components;
mod sections;
mod state;

use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn start_browser_app() {
    console_error_panic_hook::set_once();
    dioxus::LaunchBuilder::web()
        .with_cfg(dioxus::web::Config::new().rootname("burn-p2p-browser-app"))
        .launch(components::BrowserAppRoot);
}
