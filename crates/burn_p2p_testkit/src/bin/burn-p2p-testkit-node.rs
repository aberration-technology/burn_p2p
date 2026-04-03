use std::{env, fs, path::PathBuf};

use burn_p2p_testkit::multiprocess::{
    SyntheticProcessConfig, SyntheticProcessReport, run_synthetic_process_node,
};

fn main() -> anyhow::Result<()> {
    let config_path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("expected a path to a JSON config"))?;
    let config: SyntheticProcessConfig =
        serde_json::from_slice(&fs::read(&config_path).map_err(|error| {
            anyhow::anyhow!("failed to read {}: {error}", config_path.display())
        })?)
        .map_err(|error| anyhow::anyhow!("failed to decode {}: {error}", config_path.display()))?;

    match run_synthetic_process_node(&config) {
        Ok(report) => {
            println!(
                "{} process completed with {} receipts and {} merges",
                match config.mode {
                    burn_p2p_testkit::multiprocess::SyntheticProcessMode::Validator => {
                        "validator"
                    }
                    burn_p2p_testkit::multiprocess::SyntheticProcessMode::Trainer => "trainer",
                },
                report.receipt_count,
                report.merge_count
            );
            Ok(())
        }
        Err(error) => {
            let failure = SyntheticProcessReport {
                status: "failed".into(),
                error: Some(error.to_string()),
                ..SyntheticProcessReport::default()
            };
            if let Some(parent) = config.report_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&config.report_path, serde_json::to_vec_pretty(&failure)?)?;
            Err(error)
        }
    }
}
