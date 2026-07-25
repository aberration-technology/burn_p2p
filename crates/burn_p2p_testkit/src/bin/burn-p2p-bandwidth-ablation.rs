use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use burn_p2p_testkit::bandwidth_ablation::{BandwidthAblationConfig, run_bandwidth_ablation};

fn main() -> Result<()> {
    let output_dir = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/test-artifacts/p2p-bandwidth-ablation"));
    fs::create_dir_all(&output_dir).with_context(|| format!("create {}", output_dir.display()))?;
    let report = run_bandwidth_ablation(BandwidthAblationConfig::default())?;
    let json_path = output_dir.join("report.json");
    let markdown_path = output_dir.join("report.md");
    fs::write(&json_path, serde_json::to_vec_pretty(&report)?)
        .with_context(|| format!("write {}", json_path.display()))?;
    let markdown = report.to_markdown();
    fs::write(&markdown_path, &markdown)
        .with_context(|| format!("write {}", markdown_path.display()))?;
    print!("{markdown}");
    eprintln!(
        "wrote {} and {}",
        json_path.display(),
        markdown_path.display()
    );
    Ok(())
}
