use std::{fs, path::Path, process::Command};

use burn_p2p_testkit::browser_app_assets::build_browser_app_web_assets;
use burn_p2p_testkit::portal_capture::write_portal_capture_bundle;
use tempfile::tempdir;

#[test]
fn portal_capture_bundle_renders_reference_scenarios() {
    let tempdir = tempdir().expect("tempdir");
    let manifest = write_portal_capture_bundle(tempdir.path()).expect("capture bundle");

    assert!(
        manifest.scenarios.len() >= 12,
        "expected a broad scenario matrix, got {}",
        manifest.scenarios.len()
    );
    assert!(
        manifest
            .scenarios
            .iter()
            .any(|scenario| scenario.slug == "publishing-download")
    );
    assert!(manifest.scenarios.iter().any(|scenario| {
        scenario.slug == "peers-1024"
            && scenario.estimated_network_size == 1024
            && scenario.peer_count < 64
    }));
    assert!(
        manifest
            .scenarios
            .iter()
            .any(|scenario| scenario.slug == "mobile-viewer" && scenario.viewport.is_some())
    );

    for scenario in &manifest.scenarios {
        assert!(
            tempdir.path().join(&scenario.html_path).exists(),
            "missing rendered html for {}",
            scenario.slug
        );
        assert!(
            tempdir.path().join(&scenario.snapshot_path).exists(),
            "missing snapshot json for {}",
            scenario.slug
        );
        assert!(
            tempdir.path().join(&scenario.scenario_path).exists(),
            "missing scenario metadata for {}",
            scenario.slug
        );
        assert!(
            tempdir
                .path()
                .join("scenarios")
                .join(&scenario.slug)
                .join("directory.json")
                .exists(),
            "missing directory json for {}",
            scenario.slug
        );
        assert!(
            tempdir
                .path()
                .join("scenarios")
                .join(&scenario.slug)
                .join("heads.json")
                .exists(),
            "missing heads json for {}",
            scenario.slug
        );
        assert!(
            tempdir
                .path()
                .join("scenarios")
                .join(&scenario.slug)
                .join("signed-directory.json")
                .exists(),
            "missing signed-directory json for {}",
            scenario.slug
        );
        assert!(
            tempdir
                .path()
                .join("scenarios")
                .join(&scenario.slug)
                .join("signed-leaderboard.json")
                .exists(),
            "missing signed-leaderboard json for {}",
            scenario.slug
        );
    }
}

#[test]
#[ignore = "requires system Chrome plus Playwright"]
fn portal_playwright_capture_writes_artifacts_summary() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let artifact_root = repo_root.join("artifacts/playwright");
    if artifact_root.exists() {
        fs::remove_dir_all(&artifact_root).expect("clear prior artifacts");
    }
    build_browser_app_web_assets(artifact_root.join("assets")).expect("browser app wasm assets");

    let manifest = write_portal_capture_bundle(&artifact_root).expect("capture bundle");
    let manifest_path = artifact_root.join("manifest.json");

    let output = Command::new("node")
        .arg("crates/burn_p2p_testkit/scripts/portal_playwright_capture.mjs")
        .arg(&manifest_path)
        .current_dir(&repo_root)
        .output()
        .expect("launch portal Playwright capture");

    assert!(
        output.status.success(),
        "capture script failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let summary_path = artifact_root.join("summary.json");
    assert!(
        summary_path.exists(),
        "expected root summary at {summary_path:?}"
    );
    for scenario in manifest.scenarios {
        let scenario_dir = artifact_root.join("scenarios").join(&scenario.slug);
        assert!(scenario_dir.join("screenshot.png").exists());
        assert!(scenario_dir.join("trace.zip").exists());
        assert!(scenario_dir.join("console.json").exists());
        assert!(scenario_dir.join("network.json").exists());
        assert!(scenario_dir.join("state.json").exists());
        assert!(scenario_dir.join("summary.json").exists());
    }
}
