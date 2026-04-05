use std::{
    collections::BTreeMap,
    env,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};

use anyhow::Context;
use serde::Serialize;

use crate::artifacts::ArtifactLayout;

#[derive(Debug, Clone, Serialize)]
pub struct StepRecord {
    pub label: String,
    pub command: String,
    pub exit_code: i32,
    pub duration_ms: u128,
    pub stdout_path: String,
    pub stderr_path: String,
}

#[derive(Debug, Clone)]
pub struct Workspace {
    pub root: PathBuf,
    cargo_bin: String,
    node_bin: String,
    npx_bin: String,
}

impl Workspace {
    pub fn discover() -> anyhow::Result<Self> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let root = manifest_dir
            .parent()
            .ok_or_else(|| anyhow::anyhow!("xtask should live under the workspace root"))?
            .to_path_buf();
        Ok(Self {
            root,
            cargo_bin: env::var("CARGO").unwrap_or_else(|_| "cargo".into()),
            node_bin: env::var("NODE").unwrap_or_else(|_| "node".into()),
            npx_bin: env::var("NPX").unwrap_or_else(|_| "npx".into()),
        })
    }

    pub fn cargo(&self) -> &str {
        &self.cargo_bin
    }

    pub fn node(&self) -> &str {
        &self.node_bin
    }

    pub fn npx(&self) -> &str {
        &self.npx_bin
    }

    pub fn run(
        &self,
        artifacts: &ArtifactLayout,
        label: &str,
        program: &str,
        args: &[String],
        envs: &BTreeMap<String, String>,
    ) -> anyhow::Result<StepRecord> {
        println!("==> {label}");
        let started = Instant::now();
        let output = Command::new(program)
            .current_dir(&self.root)
            .envs(envs)
            .args(args)
            .output()
            .with_context(|| format!("failed to start `{program}`"))?;
        let duration_ms = started.elapsed().as_millis();

        let stdout_path = artifacts.stdout.join(format!("{label}.log"));
        let stderr_path = artifacts.stderr.join(format!("{label}.log"));
        fs::write(&stdout_path, &output.stdout)?;
        fs::write(&stderr_path, &output.stderr)?;

        let record = StepRecord {
            label: label.into(),
            command: render_command(program, args),
            exit_code: output.status.code().unwrap_or(-1),
            duration_ms,
            stdout_path: rel_path(&artifacts.root, &stdout_path),
            stderr_path: rel_path(&artifacts.root, &stderr_path),
        };

        if output.status.success() {
            return Ok(record);
        }

        anyhow::bail!(
            "step `{label}` failed with status {}.\nstdout: {}\nstderr: {}",
            record.exit_code,
            stdout_path.display(),
            stderr_path.display()
        );
    }

    pub fn run_cargo<S: AsRef<str>>(
        &self,
        artifacts: &ArtifactLayout,
        label: &str,
        args: &[S],
        envs: &BTreeMap<String, String>,
    ) -> anyhow::Result<StepRecord> {
        self.run(
            artifacts,
            label,
            self.cargo(),
            &args
                .iter()
                .map(|value| value.as_ref().to_owned())
                .collect::<Vec<_>>(),
            envs,
        )
    }

    pub fn run_node<S: AsRef<str>>(
        &self,
        artifacts: &ArtifactLayout,
        label: &str,
        args: &[S],
        envs: &BTreeMap<String, String>,
    ) -> anyhow::Result<StepRecord> {
        self.run(
            artifacts,
            label,
            self.node(),
            &args
                .iter()
                .map(|value| value.as_ref().to_owned())
                .collect::<Vec<_>>(),
            envs,
        )
    }
}

pub fn rel_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

pub fn render_command(program: &str, args: &[String]) -> String {
    let mut rendered = vec![program.to_owned()];
    rendered.extend(args.iter().cloned());
    rendered.join(" ")
}

pub fn command_available(command: impl AsRef<OsStr>) -> bool {
    let candidate = command.as_ref();
    let candidate_path = Path::new(candidate);
    if candidate_path.is_absolute() {
        return candidate_path.exists();
    }

    env::var_os("PATH")
        .map(|paths| {
            env::split_paths(&paths).any(|dir| {
                let full = dir.join(candidate);
                if full.exists() {
                    return true;
                }
                #[cfg(windows)]
                {
                    let full_exe = dir.join(format!("{}.exe", candidate.to_string_lossy()));
                    if full_exe.exists() {
                        return true;
                    }
                }
                false
            })
        })
        .unwrap_or(false)
}
