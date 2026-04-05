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

fn run_profile_step(args: &[&str]) -> anyhow::Result<String> {
    let target_dir = tempdir()?;
    let output = Command::new(cargo_bin())
        .current_dir(workspace_root())
        .env(
            "CARGO_TARGET_DIR",
            target_dir.path().join("deployment-profile-target"),
        )
        .args(args)
        .output()?;

    anyhow::ensure!(
        output.status.success(),
        "profile step failed: {}\nstdout:\n{}\nstderr:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn check_bootstrap_profile(features: &str) -> anyhow::Result<()> {
    run_profile_step(&[
        "check",
        "--manifest-path",
        "Cargo.toml",
        "-p",
        "burn_p2p_bootstrap",
        "--no-default-features",
        "--features",
        features,
    ])?;
    Ok(())
}

fn bootstrap_tree(features: &str) -> anyhow::Result<String> {
    run_profile_step(&[
        "tree",
        "--manifest-path",
        "Cargo.toml",
        "-p",
        "burn_p2p_bootstrap",
        "--no-default-features",
        "--features",
        features,
        "-e",
        "normal",
    ])
}

#[test]
fn trusted_minimal_profile_builds_without_optional_stacks() -> anyhow::Result<()> {
    let features = "admin-http,metrics,auth-static";
    check_bootstrap_profile(features)?;
    let tree = bootstrap_tree(features)?;
    for forbidden in [
        "burn_p2p_auth_github",
        "burn_p2p_auth_oidc",
        "burn_p2p_auth_oauth",
        "burn_p2p_browser",
        "burn_p2p_portal",
        "burn_p2p_social",
    ] {
        anyhow::ensure!(
            !tree.contains(forbidden),
            "trusted-minimal should not depend on {forbidden}\n{tree}"
        );
    }
    Ok(())
}

#[test]
fn enterprise_sso_profile_builds_with_oidc_and_portal_only() -> anyhow::Result<()> {
    let features = "admin-http,metrics,portal,rbac,auth-oidc";
    check_bootstrap_profile(features)?;
    let tree = bootstrap_tree(features)?;
    for required in ["burn_p2p_auth_oidc", "burn_p2p_portal"] {
        anyhow::ensure!(
            tree.contains(required),
            "enterprise-sso should include {required}\n{tree}"
        );
    }
    for forbidden in ["burn_p2p_auth_github", "burn_p2p_social"] {
        anyhow::ensure!(
            !tree.contains(forbidden),
            "enterprise-sso should not depend on {forbidden}\n{tree}"
        );
    }
    Ok(())
}

#[test]
fn community_web_profile_builds_with_browser_and_social() -> anyhow::Result<()> {
    let features = "admin-http,metrics,portal,browser-edge,rbac,auth-github,social";
    check_bootstrap_profile(features)?;
    let tree = bootstrap_tree(features)?;
    for required in ["burn_p2p_auth_github", "burn_p2p_portal", "burn_p2p_social"] {
        anyhow::ensure!(
            tree.contains(required),
            "community-web should include {required}\n{tree}"
        );
    }
    Ok(())
}

#[test]
fn mixed_native_browser_profile_builds_without_social() -> anyhow::Result<()> {
    let features = "admin-http,metrics,portal,browser-edge,auth-static";
    check_bootstrap_profile(features)?;
    run_profile_step(&[
        "check",
        "--manifest-path",
        "Cargo.toml",
        "-p",
        "burn_p2p_browser",
    ])?;
    let tree = bootstrap_tree(features)?;
    anyhow::ensure!(
        tree.contains("burn_p2p_portal"),
        "mixed-native-browser should include burn_p2p_portal\n{tree}"
    );
    anyhow::ensure!(
        !tree.contains("burn_p2p_social"),
        "mixed-native-browser should not depend on burn_p2p_social\n{tree}"
    );
    Ok(())
}
