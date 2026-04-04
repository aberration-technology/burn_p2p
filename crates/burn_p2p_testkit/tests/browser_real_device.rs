//! Real-browser probe wrapper for local Chrome/Firefox validation.

use std::process::Command;

#[test]
#[ignore = "requires system Chrome/Firefox plus Playwright"]
fn browser_real_device_probe_reports_budget_and_role_evidence() {
    let output = Command::new("node")
        .arg("crates/burn_p2p_testkit/scripts/browser_real_device_probe.mjs")
        .current_dir("/home/mosure/repos/burn_p2p")
        .output();

    let output = match output {
        Ok(output) if output.status.success() => output,
        _ => Command::new("npx")
            .arg("--yes")
            .arg("-p")
            .arg("playwright")
            .arg("node")
            .arg("crates/burn_p2p_testkit/scripts/browser_real_device_probe.mjs")
            .current_dir("/home/mosure/repos/burn_p2p")
            .output()
            .expect("real-browser probe should launch"),
    };

    assert!(
        output.status.success(),
        "probe failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let payload: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("probe should emit JSON");
    let results = payload["results"]
        .as_array()
        .expect("probe should return browser results");
    assert!(
        !results.is_empty(),
        "probe should return at least one browser result"
    );
    for result in results {
        assert_eq!(
            result["meets_browser_target_window"].as_bool(),
            Some(true),
            "browser result should satisfy current target window budget: {result:?}"
        );
    }
}
