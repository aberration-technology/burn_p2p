use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, ensure};
use wasm_bindgen_cli_support::Bindgen;

const BROWSER_APP_LOADER: &str = r#"import init from "./burn_p2p_app.js";

await init({ module_or_path: new URL("./burn_p2p_app_bg.wasm", import.meta.url) });
"#;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn cargo_bin() -> String {
    std::env::var("CARGO").unwrap_or_else(|_| "cargo".into())
}

fn run_step(command: &mut Command, label: &str) -> Result<()> {
    let output = command.output().with_context(|| format!("run {label}"))?;
    ensure!(
        output.status.success(),
        "{label} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}

/// Builds the static wasm browser-app bundle into the provided asset directory.
pub fn build_browser_app_web_assets(root: impl AsRef<Path>) -> Result<()> {
    let root = root.as_ref();
    fs::create_dir_all(root)?;

    let workspace_root = workspace_root();
    run_step(
        Command::new(cargo_bin())
            .current_dir(&workspace_root)
            .arg("build")
            .arg("--manifest-path")
            .arg(workspace_root.join("Cargo.toml"))
            .arg("-p")
            .arg("burn_p2p_app")
            .arg("--target")
            .arg("wasm32-unknown-unknown")
            .arg("--features")
            .arg("web-client"),
        "cargo build burn_p2p_app wasm client",
    )?;

    let wasm_input = workspace_root.join("target/wasm32-unknown-unknown/debug/burn_p2p_app.wasm");
    ensure!(
        wasm_input.exists(),
        "missing wasm output at {}",
        wasm_input.display()
    );

    let mut bindgen = Bindgen::new();
    bindgen.input_path(&wasm_input).out_name("burn_p2p_app");
    bindgen
        .web(true)
        .context("configure wasm-bindgen web target")?;
    bindgen
        .generate(root)
        .context("generate wasm-bindgen browser app bundle")?;

    fs::write(root.join("browser-app-loader.js"), BROWSER_APP_LOADER)?;
    fs::write(
        root.join("browser-app.css"),
        burn_p2p_app::browser_app_stylesheet(),
    )?;
    Ok(())
}
