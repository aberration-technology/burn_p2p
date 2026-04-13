#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::{env, fs, path::PathBuf, thread, time::Duration};

    use anyhow::{Context, Result};
    use mnist_p2p_demo::correctness::live_browser::{
        LiveBrowserEdgeServer, read_live_browser_edge_config, write_live_browser_manifest,
    };

    struct Args {
        config: PathBuf,
        output_root: PathBuf,
        stop_file: PathBuf,
    }

    fn parse_args() -> Result<Args> {
        let mut config = None;
        let mut output_root = None;
        let mut stop_file = None;
        let mut args = env::args().skip(1);
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--config" => config = args.next().map(PathBuf::from),
                "--output-root" => output_root = args.next().map(PathBuf::from),
                "--stop-file" => stop_file = args.next().map(PathBuf::from),
                _ => anyhow::bail!(
                    "usage: cargo run --bin mnist_browser_live_edge -- --config /abs/config.json --output-root /abs/output --stop-file /abs/stop"
                ),
            }
        }

        Ok(Args {
            config: config.context("missing --config")?,
            output_root: output_root.context("missing --output-root")?,
            stop_file: stop_file.context("missing --stop-file")?,
        })
    }

    pub fn run() -> Result<()> {
        let args = parse_args()?;
        if args.stop_file.exists() {
            fs::remove_file(&args.stop_file)
                .with_context(|| format!("failed to clear {}", args.stop_file.display()))?;
        }
        let config = read_live_browser_edge_config(&args.config)?;
        let (server, mut manifest) = LiveBrowserEdgeServer::spawn(config)?;
        let peer_dataset_root = args.output_root.join("browser-peer-dataset-root.txt");
        if peer_dataset_root.exists() {
            manifest.peer_dataset_root = Some(
                fs::read_to_string(&peer_dataset_root)
                    .with_context(|| format!("failed to read {}", peer_dataset_root.display()))?
                    .trim()
                    .to_owned(),
            );
        }
        let peer_head_artifact_root = args.output_root.join("browser-peer-head-artifact-root.txt");
        if peer_head_artifact_root.exists() {
            manifest.peer_head_artifact_root = Some(
                fs::read_to_string(&peer_head_artifact_root)
                    .with_context(|| {
                        format!("failed to read {}", peer_head_artifact_root.display())
                    })?
                    .trim()
                    .to_owned(),
            );
        }
        write_live_browser_manifest(&args.output_root, &manifest)?;
        println!(
            "mnist browser live edge ready\nmanifest: {}",
            args.output_root.join("browser-live.json").display()
        );

        while !args.stop_file.exists() {
            thread::sleep(Duration::from_millis(100));
        }
        drop(server);
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> anyhow::Result<()> {
    native::run()
}

#[cfg(target_arch = "wasm32")]
fn main() {}
