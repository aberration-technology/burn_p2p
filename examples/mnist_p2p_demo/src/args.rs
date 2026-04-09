use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(
    author,
    version,
    about = "single-machine mnist burn_p2p sanity workload"
)]
pub struct Args {
    /// Output root for storage, browser export, and summary files.
    #[arg(long)]
    pub output: PathBuf,
    /// Per-digit training examples used to build the shard-backed local dataset.
    #[arg(long, default_value_t = 64)]
    pub train_examples_per_digit: usize,
    /// Per-digit evaluation examples used for validator and viewer metrics.
    #[arg(long, default_value_t = 32)]
    pub eval_examples_per_digit: usize,
    /// Number of training microshards written to the local fetch manifest.
    #[arg(long, default_value_t = 8)]
    pub microshards: u32,
    /// Batch size used by the burn learner.
    #[arg(long, default_value_t = 32)]
    pub batch_size: usize,
    /// Baseline experiment rounds.
    #[arg(long, default_value_t = 3)]
    pub baseline_rounds: usize,
    /// Lower-learning-rate experiment rounds.
    #[arg(long, default_value_t = 3)]
    pub low_lr_rounds: usize,
    /// Prepares live browser probe handoff artifacts for a post-run browser probe.
    #[arg(long, hide = true, default_value_t = false)]
    pub live_browser_probe: bool,
}
