//! Example compilation checks for the documented downstream integration paths.

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

fn run_example_step(args: &[&str]) -> anyhow::Result<()> {
    let target_dir = tempdir()?;
    let output = Command::new(cargo_bin())
        .current_dir(workspace_root())
        .env(
            "CARGO_TARGET_DIR",
            target_dir.path().join("example-compile-target"),
        )
        .args(args)
        .output()?;

    anyhow::ensure!(
        output.status.success(),
        "example compile step failed: {}\nstdout:\n{}\nstderr:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}

fn run_example_and_capture(args: &[&str]) -> anyhow::Result<String> {
    let target_dir = tempdir()?;
    let output = Command::new(cargo_bin())
        .current_dir(workspace_root())
        .env(
            "CARGO_TARGET_DIR",
            target_dir.path().join("example-run-target"),
        )
        .args(args)
        .output()?;

    anyhow::ensure!(
        output.status.success(),
        "example run step failed: {}\nstdout:\n{}\nstderr:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[test]
fn burn_p2p_examples_compile() -> anyhow::Result<()> {
    run_example_step(&[
        "check",
        "--manifest-path",
        "Cargo.toml",
        "-p",
        "burn_p2p",
        "--example",
        "synthetic_trainer",
        "--example",
        "family_workload_minimal",
    ])
}

#[test]
fn burn_feature_examples_compile() -> anyhow::Result<()> {
    run_example_step(&[
        "check",
        "--manifest-path",
        "Cargo.toml",
        "-p",
        "burn_p2p",
        "--features",
        "burn",
        "--example",
        "burn_ndarray_runtime",
    ])
}

#[test]
fn bootstrap_examples_compile() -> anyhow::Result<()> {
    run_example_step(&[
        "check",
        "--manifest-path",
        "Cargo.toml",
        "-p",
        "burn_p2p_bootstrap",
        "--example",
        "embedded_runtime_daemon",
    ])
}

#[test]
fn minimal_family_workload_example_runs() -> anyhow::Result<()> {
    let stdout = run_example_and_capture(&[
        "run",
        "--manifest-path",
        "Cargo.toml",
        "-p",
        "burn_p2p",
        "--example",
        "family_workload_minimal",
    ])?;

    anyhow::ensure!(
        stdout.contains(
            "configured family=minimal-family workload=minimal-workload revision=revision-example"
        ),
        "minimal example did not print the expected configuration summary:\n{stdout}"
    );
    Ok(())
}
