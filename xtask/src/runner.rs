use std::{
    collections::BTreeMap,
    env,
    ffi::OsStr,
    fs,
    fs::File,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Instant,
};

use anyhow::Context;
use serde::Serialize;

use crate::artifacts::ArtifactLayout;

const FAILURE_LOG_PREVIEW_CHARS: usize = 16_000;

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

pub struct SpawnedStep {
    child: Child,
    label: String,
    command: String,
    started: Instant,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
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
        self.run_in_dir(artifacts, label, &self.root, program, args, envs)
    }

    pub fn run_in_dir(
        &self,
        artifacts: &ArtifactLayout,
        label: &str,
        current_dir: &Path,
        program: &str,
        args: &[String],
        envs: &BTreeMap<String, String>,
    ) -> anyhow::Result<StepRecord> {
        println!("==> {label}");
        let started = Instant::now();
        let output = Command::new(program)
            .current_dir(current_dir)
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

        let stdout_preview = failure_log_preview(&output.stdout);
        let stderr_preview = failure_log_preview(&output.stderr);
        anyhow::bail!(
            "step `{label}` failed with status {}.\ncommand: {}\nstdout: {}\nstderr: {}\n\n--- stdout (captured) ---\n{}\n\n--- stderr (captured) ---\n{}",
            record.exit_code,
            record.command,
            stdout_path.display(),
            stderr_path.display(),
            stdout_preview,
            stderr_preview,
        );
    }

    pub fn spawn(
        &self,
        artifacts: &ArtifactLayout,
        label: &str,
        program: &str,
        args: &[String],
        envs: &BTreeMap<String, String>,
    ) -> anyhow::Result<SpawnedStep> {
        println!("==> {label}");
        let stdout_path = artifacts.stdout.join(format!("{label}.log"));
        let stderr_path = artifacts.stderr.join(format!("{label}.log"));
        let stdout = File::create(&stdout_path)
            .with_context(|| format!("failed to create {}", stdout_path.display()))?;
        let stderr = File::create(&stderr_path)
            .with_context(|| format!("failed to create {}", stderr_path.display()))?;
        let child = Command::new(program)
            .current_dir(&self.root)
            .envs(envs)
            .args(args)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .with_context(|| format!("failed to start `{program}`"))?;

        Ok(SpawnedStep {
            child,
            label: label.into(),
            command: render_command(program, args),
            started: Instant::now(),
            stdout_path,
            stderr_path,
        })
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

impl SpawnedStep {
    pub fn kill(&mut self) -> anyhow::Result<()> {
        self.child
            .kill()
            .with_context(|| format!("failed to kill `{}`", self.label))
    }

    pub fn try_wait(&mut self, artifacts: &ArtifactLayout) -> anyhow::Result<Option<StepRecord>> {
        let Some(status) = self
            .child
            .try_wait()
            .with_context(|| format!("failed to poll `{}`", self.label))?
        else {
            return Ok(None);
        };
        let duration_ms = self.started.elapsed().as_millis();
        let record = StepRecord {
            label: self.label.clone(),
            command: self.command.clone(),
            exit_code: status.code().unwrap_or(-1),
            duration_ms,
            stdout_path: rel_path(&artifacts.root, &self.stdout_path),
            stderr_path: rel_path(&artifacts.root, &self.stderr_path),
        };

        if status.success() {
            return Ok(Some(record));
        }

        let stdout_preview = failure_log_preview(&fs::read(&self.stdout_path)?);
        let stderr_preview = failure_log_preview(&fs::read(&self.stderr_path)?);
        anyhow::bail!(
            "step `{}` failed with status {}.\ncommand: {}\nstdout: {}\nstderr: {}\n\n--- stdout (captured) ---\n{}\n\n--- stderr (captured) ---\n{}",
            record.label,
            record.exit_code,
            record.command,
            self.stdout_path.display(),
            self.stderr_path.display(),
            stdout_preview,
            stderr_preview,
        );
    }

    pub fn wait(mut self, artifacts: &ArtifactLayout) -> anyhow::Result<StepRecord> {
        let status = self
            .child
            .wait()
            .with_context(|| format!("failed to wait for `{}`", self.label))?;
        let duration_ms = self.started.elapsed().as_millis();
        let record = StepRecord {
            label: self.label,
            command: self.command,
            exit_code: status.code().unwrap_or(-1),
            duration_ms,
            stdout_path: rel_path(&artifacts.root, &self.stdout_path),
            stderr_path: rel_path(&artifacts.root, &self.stderr_path),
        };

        if status.success() {
            return Ok(record);
        }

        let stdout_preview = failure_log_preview(&fs::read(&self.stdout_path)?);
        let stderr_preview = failure_log_preview(&fs::read(&self.stderr_path)?);
        anyhow::bail!(
            "step `{}` failed with status {}.\ncommand: {}\nstdout: {}\nstderr: {}\n\n--- stdout (captured) ---\n{}\n\n--- stderr (captured) ---\n{}",
            record.label,
            record.exit_code,
            record.command,
            self.stdout_path.display(),
            self.stderr_path.display(),
            stdout_preview,
            stderr_preview,
        );
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

fn failure_log_preview(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "(empty)".into();
    }

    let text = String::from_utf8_lossy(bytes);
    let char_count = text.chars().count();
    if char_count <= FAILURE_LOG_PREVIEW_CHARS {
        return text.into_owned();
    }

    let start = text
        .char_indices()
        .nth(char_count - FAILURE_LOG_PREVIEW_CHARS)
        .map(|(index, _)| index)
        .unwrap_or(0);

    format!(
        "[truncated to last {FAILURE_LOG_PREVIEW_CHARS} chars of {char_count}]\n{}",
        &text[start..]
    )
}

#[cfg(test)]
mod tests {
    use super::failure_log_preview;

    #[test]
    fn failure_log_preview_marks_empty_output() {
        assert_eq!(failure_log_preview(&[]), "(empty)");
    }

    #[test]
    fn failure_log_preview_keeps_short_output() {
        assert_eq!(failure_log_preview(b"plain stderr"), "plain stderr");
    }

    #[test]
    fn failure_log_preview_truncates_to_tail() {
        let input = "a".repeat(17_000) + "tail-marker";
        let preview = failure_log_preview(input.as_bytes());
        assert!(preview.starts_with("[truncated to last "));
        assert!(preview.contains("tail-marker"));
        assert!(!preview.ends_with("(empty)"));
    }
}
