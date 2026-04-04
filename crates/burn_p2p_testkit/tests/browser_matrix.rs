//! Test harnesses, fixtures, and mixed-fleet verification helpers for burn_p2p.
use std::{path::PathBuf, process::Command};

use tempfile::tempdir;

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

fn run_browser_matrix_step(args: &[&str]) -> anyhow::Result<()> {
    let target_dir = tempdir()?;
    let output = Command::new(cargo_bin())
        .current_dir(workspace_root())
        .env(
            "CARGO_TARGET_DIR",
            target_dir.path().join("browser-matrix-target"),
        )
        .args(args)
        .output()?;

    anyhow::ensure!(
        output.status.success(),
        "browser matrix step failed: {}\nstdout:\n{}\nstderr:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}

#[test]
fn browser_crate_compiles_for_wasm_target() -> anyhow::Result<()> {
    run_browser_matrix_step(&[
        "check",
        "--manifest-path",
        "Cargo.toml",
        "-p",
        "burn_p2p_browser",
        "--target",
        "wasm32-unknown-unknown",
    ])
}

#[test]
fn browser_supporting_crates_compile_for_wasm_target() -> anyhow::Result<()> {
    run_browser_matrix_step(&[
        "check",
        "--manifest-path",
        "Cargo.toml",
        "-p",
        "burn_p2p_swarm",
        "-p",
        "burn_p2p_dataloader",
        "--target",
        "wasm32-unknown-unknown",
    ])
}
