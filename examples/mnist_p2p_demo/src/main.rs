#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::fs;

    use anyhow::Context;
    use clap::Parser;
    use mnist_p2p_demo::{args::Args, scenario};

    pub fn run() -> anyhow::Result<()> {
        let args = Args::parse();
        fs::create_dir_all(&args.output)?;

        let export = scenario::run_demo(&args)?;
        fs::write(
            args.output.join("summary.json"),
            serde_json::to_vec_pretty(&export.summary)?,
        )
        .context("failed to write mnist summary")?;
        fs::write(
            args.output.join("correctness.json"),
            serde_json::to_vec_pretty(&export.correctness)?,
        )
        .context("failed to write mnist correctness summary")?;
        fs::write(
            args.output.join("browser-export.json"),
            serde_json::to_vec_pretty(&export)?,
        )
        .context("failed to write mnist browser export")?;

        println!(
            "mnist demo complete\nsummary: {}\ncorrectness: {}\nbrowser export: {}",
            args.output.join("summary.json").display(),
            args.output.join("correctness.json").display(),
            args.output.join("browser-export.json").display(),
        );

        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> anyhow::Result<()> {
    native::run()
}

#[cfg(target_arch = "wasm32")]
fn main() {}
