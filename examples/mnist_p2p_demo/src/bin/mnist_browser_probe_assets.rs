#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process::Command,
    };

    use anyhow::{Context, Result, ensure};
    use wasm_bindgen_cli_support::Bindgen;

    const MNIST_BROWSER_LOADER: &str = r#"import init, { run_browser_mnist_probe } from "./mnist_p2p_demo.js";

await init({ module_or_path: new URL("./mnist_p2p_demo_bg.wasm", import.meta.url) });

window.runBrowserMnistProbe = async (config) => run_browser_mnist_probe(config);
"#;

    const MNIST_BROWSER_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>mnist browser burn probe</title>
  </head>
  <body>
    <main>
      <h1>mnist browser burn probe</h1>
      <p>wasm burn mnist probe host</p>
    </main>
    <script type="module" src="./mnist-probe-loader.js"></script>
  </body>
</html>
"#;

    fn cargo_bin() -> String {
        env::var("CARGO").unwrap_or_else(|_| "cargo".into())
    }

    fn run_step(command: &mut Command, label: &str) -> Result<()> {
        let output = command.output().with_context(|| format!("run {label}"))?;
        ensure!(
            output.status.success(),
            "{label} failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        Ok(())
    }

    fn parse_output_dir() -> Result<PathBuf> {
        let mut args = env::args().skip(1);
        match (args.next().as_deref(), args.next()) {
            (Some("--output"), Some(path)) => Ok(PathBuf::from(path)),
            _ => anyhow::bail!(
                "usage: cargo run --bin mnist_browser_probe_assets -- --output /abs/path"
            ),
        }
    }

    fn crate_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    fn build_assets(root: impl AsRef<Path>) -> Result<()> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;

        let crate_root = crate_root();
        let manifest_path = crate_root.join("Cargo.toml");
        run_step(
            Command::new(cargo_bin())
                .current_dir(&crate_root)
                .arg("build")
                .arg("--manifest-path")
                .arg(&manifest_path)
                .arg("--lib")
                .arg("--target")
                .arg("wasm32-unknown-unknown"),
            "cargo build mnist_p2p_demo wasm probe",
        )?;

        let wasm_input = crate_root.join("target/wasm32-unknown-unknown/debug/mnist_p2p_demo.wasm");
        ensure!(
            wasm_input.exists(),
            "missing wasm output at {}",
            wasm_input.display()
        );

        let mut bindgen = Bindgen::new();
        bindgen.input_path(&wasm_input).out_name("mnist_p2p_demo");
        bindgen
            .web(true)
            .context("configure wasm-bindgen web target for mnist probe")?;
        bindgen
            .generate(root)
            .context("generate wasm-bindgen mnist probe bundle")?;

        fs::write(root.join("mnist-probe-loader.js"), MNIST_BROWSER_LOADER)?;
        fs::write(root.join("index.html"), MNIST_BROWSER_HTML)?;
        Ok(())
    }

    pub fn run() -> Result<()> {
        let output = parse_output_dir()?;
        build_assets(&output)?;
        println!("{}", output.display());
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> anyhow::Result<()> {
    native::run()
}

#[cfg(target_arch = "wasm32")]
fn main() {}
