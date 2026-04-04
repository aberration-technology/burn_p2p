//! Test harnesses, fixtures, and mixed-fleet verification helpers for burn_p2p.
use std::{env, fs, path::PathBuf};

use burn_p2p_testkit::multiprocess::{SyntheticSoakConfig, run_synthetic_process_soak};

fn main() -> anyhow::Result<()> {
    let config_path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("expected a path to a JSON soak config"))?;
    let config: SyntheticSoakConfig =
        serde_json::from_slice(&fs::read(&config_path).map_err(|error| {
            anyhow::anyhow!("failed to read {}: {error}", config_path.display())
        })?)
        .map_err(|error| anyhow::anyhow!("failed to decode {}: {error}", config_path.display()))?;

    let current_exe = env::current_exe()?;
    let node_binary = current_exe
        .parent()
        .ok_or_else(|| anyhow::anyhow!("failed to locate current binary directory"))?
        .join(format!("burn-p2p-testkit-node{}", env::consts::EXE_SUFFIX));

    let summary = run_synthetic_process_soak(&config, &node_binary)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
