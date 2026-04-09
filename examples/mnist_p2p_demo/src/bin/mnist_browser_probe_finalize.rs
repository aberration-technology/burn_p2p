#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::{env, fs, path::PathBuf};

    use anyhow::{Context, Result};
    use mnist_p2p_demo::{
        correctness::{
            export::MnistRunExport,
            live_browser::LiveBrowserProbeManifest,
            report::apply_live_browser_probe_results,
        },
    };

    struct Args {
        export_path: PathBuf,
        correctness_path: PathBuf,
        manifest_path: PathBuf,
        probe_path: PathBuf,
    }

    fn parse_args() -> Result<Args> {
        let mut export_path = None;
        let mut correctness_path = None;
        let mut manifest_path = None;
        let mut probe_path = None;
        let mut args = env::args().skip(1);
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--export" => export_path = args.next().map(PathBuf::from),
                "--correctness" => correctness_path = args.next().map(PathBuf::from),
                "--manifest" => manifest_path = args.next().map(PathBuf::from),
                "--probe" => probe_path = args.next().map(PathBuf::from),
                _ => anyhow::bail!(
                    "usage: cargo run --bin mnist_browser_probe_finalize -- --export /abs/browser-export.json --correctness /abs/correctness.json --manifest /abs/browser-live.json --probe /abs/browser-probe-result.json"
                ),
            }
        }

        Ok(Args {
            export_path: export_path.context("missing --export")?,
            correctness_path: correctness_path.context("missing --correctness")?,
            manifest_path: manifest_path.context("missing --manifest")?,
            probe_path: probe_path.context("missing --probe")?,
        })
    }

    pub fn run() -> Result<()> {
        let args = parse_args()?;
        let mut export: MnistRunExport = serde_json::from_slice(
            &fs::read(&args.export_path)
                .with_context(|| format!("failed to read {}", args.export_path.display()))?,
        )
        .with_context(|| format!("failed to decode {}", args.export_path.display()))?;
        let manifest: LiveBrowserProbeManifest = serde_json::from_slice(
            &fs::read(&args.manifest_path)
                .with_context(|| format!("failed to read {}", args.manifest_path.display()))?,
        )
        .with_context(|| format!("failed to decode {}", args.manifest_path.display()))?;
        let probe_summary: serde_json::Value = serde_json::from_slice(
            &fs::read(&args.probe_path)
                .with_context(|| format!("failed to read {}", args.probe_path.display()))?,
        )
        .with_context(|| format!("failed to decode {}", args.probe_path.display()))?;
        apply_live_browser_probe_results(&mut export, &manifest, &probe_summary)?;
        fs::write(&args.export_path, serde_json::to_vec_pretty(&export)?)
            .with_context(|| format!("failed to write {}", args.export_path.display()))?;
        fs::write(
            &args.correctness_path,
            serde_json::to_vec_pretty(&export.correctness)?,
        )
        .with_context(|| format!("failed to write {}", args.correctness_path.display()))?;
        println!(
            "mnist browser probe finalized\nexport: {}\ncorrectness: {}",
            args.export_path.display(),
            args.correctness_path.display()
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
