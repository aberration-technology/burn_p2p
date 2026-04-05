use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Context;
use chrono::Utc;
use serde::Serialize;

use crate::profile::Profile;

#[derive(Debug)]
pub struct ArtifactLayout {
    pub root: PathBuf,
    pub stdout: PathBuf,
    pub stderr: PathBuf,
    pub configs: PathBuf,
    pub screenshots: PathBuf,
    pub playwright_traces: PathBuf,
    pub metrics: PathBuf,
    pub publication: PathBuf,
    pub topology: PathBuf,
}

impl ArtifactLayout {
    pub fn create(workspace_root: &Path, suite: &str, profile: Profile) -> anyhow::Result<Self> {
        let run_id = Utc::now().format("%Y%m%d-%H%M%S-%3f").to_string();
        let root = workspace_root
            .join("target/test-artifacts")
            .join(suite)
            .join(run_id);
        let layout = Self {
            stdout: root.join("stdout"),
            stderr: root.join("stderr"),
            configs: root.join("configs"),
            screenshots: root.join("screenshots"),
            playwright_traces: root.join("playwright-traces"),
            metrics: root.join("metrics"),
            publication: root.join("publication"),
            topology: root.join("topology"),
            root,
        };

        for dir in [
            &layout.root,
            &layout.stdout,
            &layout.stderr,
            &layout.configs,
            &layout.screenshots,
            &layout.playwright_traces,
            &layout.metrics,
            &layout.publication,
            &layout.topology,
        ] {
            fs::create_dir_all(dir).with_context(|| {
                format!("failed to create artifact directory {}", dir.display())
            })?;
        }

        layout.write_json("ports.json", &serde_json::json!({ "reserved": [] }))?;
        layout.write_text("seed.txt", "")?;
        layout.write_json(
            "configs/profile.json",
            &serde_json::json!({ "profile": profile.label() }),
        )?;
        Ok(layout)
    }

    pub fn write_json<T: Serialize>(&self, rel: &str, value: &T) -> anyhow::Result<PathBuf> {
        let path = self.root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, format!("{}\n", serde_json::to_string_pretty(value)?))?;
        Ok(path)
    }

    pub fn write_text(&self, rel: &str, value: impl AsRef<str>) -> anyhow::Result<PathBuf> {
        let path = self.root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, value.as_ref())?;
        Ok(path)
    }
}

pub fn copy_dir_all(src: &Path, dst: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

pub fn copy_files_with_extension_tree(
    src: &Path,
    extension: &str,
    dst: &Path,
) -> anyhow::Result<()> {
    if !src.exists() {
        return Ok(());
    }
    fs::create_dir_all(dst)?;
    visit_files(src, &mut |path| {
        if path.extension().and_then(|value| value.to_str()) == Some(extension) {
            let relative = path.strip_prefix(src).with_context(|| {
                format!(
                    "failed to derive relative artifact path for {} from {}",
                    path.display(),
                    src.display()
                )
            })?;
            let target = dst.join(relative);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(path, target)?;
        }
        Ok(())
    })
}

fn visit_files(root: &Path, f: &mut dyn FnMut(&Path) -> anyhow::Result<()>) -> anyhow::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            visit_files(&path, f)?;
        } else {
            f(&path)?;
        }
    }
    Ok(())
}
