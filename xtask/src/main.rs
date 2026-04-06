mod artifacts;
mod cli;
mod profile;
mod runner;

use std::{
    collections::BTreeMap,
    env, fs,
    net::TcpListener,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, ensure};
use artifacts::{ArtifactLayout, copy_dir_all, copy_files_with_extension_tree};
use burn_p2p::{PeerId, WindowId};
use burn_p2p_core::{AggregationStrategy, RobustnessPolicy};
use burn_p2p_metrics::MetricsCatchupBundle;
use burn_p2p_security::{FeatureLayer, aggregate_updates_with_policy, extract_feature_sketch};
use burn_p2p_testkit::{
    ChaosEvent, FaultType, SimulationRunner, SimulationSpec,
    adversarial::{
        AdversarialAttack, AdversarialScenarioReport, build_fixture, run_attack_matrix,
        run_scenario,
    },
    browser_app_assets::build_browser_app_web_assets,
    multiprocess::SyntheticSoakConfig,
    multiprocess::run_synthetic_process_soak,
    portal_capture::{
        BrowserPortalCaptureSpec, PortalCaptureInteraction, PortalCaptureViewport,
        write_browser_portal_capture_bundle, write_portal_capture_bundle,
    },
};
use burn_p2p_views::BrowserAppSurface;
use clap::Parser;
use cli::{
    AdversarialCommand, BenchArgs, BenchCommand, BrowserArgs, BrowserCommand, ChaosArgs,
    CheckSubcommand, CiArgs, CiCommand, Cli, Command, CommonArgs, DeployAction, DeployCloudArgs,
    DeployCommand, DeployComposeArgs, E2eCommand, MultiprocessArgs, SetupCommand, StressCommand,
};
use profile::Profile;
use runner::{StepRecord, Workspace, command_available};
use serde::Deserialize;
use serde_json::json;

const PUBLISH_CRATES: &[&str] = &[
    "burn_p2p_core",
    "burn_p2p_experiment",
    "burn_p2p_checkpoint",
    "burn_p2p_limits",
    "burn_p2p_dataloader",
    "burn_p2p_security",
    "burn_p2p_swarm",
    "burn_p2p_engine",
    "burn_p2p_auth_external",
    "burn_p2p_auth_github",
    "burn_p2p_auth_oidc",
    "burn_p2p_auth_oauth",
    "burn_p2p_metrics",
    "burn_p2p_publish",
    "burn_p2p_social",
    "burn_p2p_views",
    "burn_p2p",
    "burn_p2p_browser",
    "burn_p2p_app",
    "burn_p2p_bootstrap",
];

#[derive(Debug, Deserialize)]
struct MnistRunExportFile {
    browser_scenarios: Vec<MnistBrowserScenario>,
}

#[derive(Debug, Deserialize)]
struct MnistBrowserScenario {
    slug: String,
    title: String,
    description: String,
    default_surface: BrowserAppSurface,
    snapshot: burn_p2p::BrowserEdgeSnapshot,
    metrics_catchup: Vec<MetricsCatchupBundle>,
    #[serde(default)]
    runtime_states: Vec<String>,
    #[serde(default)]
    interactions: Vec<PortalCaptureInteraction>,
    #[serde(default)]
    viewport: Option<PortalCaptureViewport>,
}

fn mnist_adversarial_correctness_summary() -> serde_json::Value {
    let policy = RobustnessPolicy::balanced();
    let attacks = [
        AdversarialAttack::Replay,
        AdversarialAttack::FreeRider,
        AdversarialAttack::NanInf,
        AdversarialAttack::LateFlood,
    ];
    let reports = attacks
        .into_iter()
        .map(|attack| {
            let report = run_scenario(policy.clone(), attack.clone(), 12, 0.20);
            json!({
                "attack": format!("{attack:?}"),
                "attack_success_rate": report.attack_success_rate,
                "malicious_update_acceptance_rate": report.malicious_update_acceptance_rate,
                "benign_false_rejection_rate": report.benign_false_rejection_rate,
                "quarantine_precision": report.quarantine_precision,
                "quarantine_recall": report.quarantine_recall,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "preset": "balanced",
        "reports": reports,
    })
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let workspace = Workspace::discover()?;

    match cli.command {
        Command::Doctor => doctor(&workspace),
        Command::Setup { command } => match command {
            SetupCommand::Browser => setup_browser(&workspace),
        },
        Command::Check(command) => match command.command {
            Some(CheckSubcommand::Publish(args)) => run_publish_checks(&workspace, args.common),
            None => run_fast_checks(&workspace, command.common),
        },
        Command::E2e { command } => match command {
            E2eCommand::Smoke(args) => run_e2e_smoke(&workspace, args.common),
            E2eCommand::Mixed(args) => run_e2e_mixed(&workspace, args.common),
            E2eCommand::Mnist(args) => run_e2e_mnist(&workspace, args.common),
            E2eCommand::Services(args) => run_e2e_services(&workspace, args.common),
        },
        Command::Browser { command } => match command {
            BrowserCommand::Smoke(args) => run_browser_smoke(&workspace, args),
            BrowserCommand::Trainer(args) => run_browser_trainer(&workspace, args),
            BrowserCommand::Real(args) => run_browser_real(&workspace, args),
        },
        Command::Adversarial { command } => match command {
            AdversarialCommand::Smoke(args) => run_adversarial_smoke(&workspace, args.common),
            AdversarialCommand::Matrix(args) => run_adversarial_matrix(&workspace, args.common),
            AdversarialCommand::Chaos(args) => run_adversarial_chaos(&workspace, args),
        },
        Command::Stress { command } => match command {
            StressCommand::Multiprocess(args) => run_stress_multiprocess(&workspace, args),
            StressCommand::Chaos(args) => run_stress_chaos(&workspace, args),
        },
        Command::Bench { command } => match command {
            BenchCommand::Core(args) => run_bench_core(&workspace, args),
            BenchCommand::Robust(args) => run_bench_robust(&workspace, args),
            BenchCommand::Nightly(args) => run_bench_nightly(&workspace, args),
        },
        Command::Deploy { command } => match command {
            DeployCommand::Compose(args) => run_deploy_compose(&workspace, args),
            DeployCommand::Aws(args) => run_deploy_cloud(&workspace, "aws", args),
            DeployCommand::Gcp(args) => run_deploy_cloud(&workspace, "gcp", args),
        },
        Command::Ci { command } => match command {
            CiCommand::PrFast(args) => run_ci_pr_fast(&workspace, args),
            CiCommand::Browser(args) => run_ci_browser(&workspace, args),
            CiCommand::Integration(args) => run_ci_integration(&workspace, args),
            CiCommand::Services(args) => run_ci_services(&workspace, args),
            CiCommand::Nightly(args) => run_ci_nightly(&workspace, args),
            CiCommand::Publish(args) => run_ci_publish(&workspace, args),
        },
    }
}

fn doctor(workspace: &Workspace) -> anyhow::Result<()> {
    let artifact_root = workspace.root.join("target/test-artifacts");
    fs::create_dir_all(&artifact_root)?;

    let rustc = probe_command("rustc", &["--version"]);
    let cargo = probe_command(workspace.cargo(), &["--version"]);
    let rustup = probe_command("rustup", &["target", "list", "--installed"]);
    let node = probe_command(workspace.node(), &["--version"]);
    let docker = probe_command("docker", &["--version"]);
    let compose = probe_command("docker", &["compose", "version"]);
    let terraform = probe_command("terraform", &["version"]);
    let chrome = resolve_chrome_path();
    let firefox = resolve_firefox_path();
    let playwright_cache = playwright_package_available();
    let wasm_target_installed = rustup
        .output
        .as_deref()
        .map(|output| {
            output
                .lines()
                .any(|line| line.trim() == "wasm32-unknown-unknown")
        })
        .unwrap_or(false);

    let ports = reserve_ports(3)?;
    let browser_real_ready = node.ok && playwright_cache && wasm_target_installed;

    println!("Local environment doctor");
    print_probe("Rust toolchain", &rustc, "rustup toolchain install stable");
    print_probe("Cargo", &cargo, "install Rust stable to get cargo");
    print_probe(
        "wasm32 target",
        &Probe {
            ok: wasm_target_installed,
            output: Some(if wasm_target_installed {
                "wasm32-unknown-unknown installed".into()
            } else {
                "missing wasm32-unknown-unknown".into()
            }),
        },
        "rustup target add wasm32-unknown-unknown",
    );
    print_probe("Node", &node, "install Node.js 20+");
    print_probe(
        "Playwright package cache",
        &Probe {
            ok: playwright_cache,
            output: Some(if playwright_cache {
                "playwright package cache found".into()
            } else {
                "playwright package cache not found".into()
            }),
        },
        "cargo xtask setup browser",
    );
    print_probe(
        "Chrome",
        &Probe {
            ok: chrome.is_some(),
            output: Some(
                chrome
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "not found".into()),
            ),
        },
        "install Chrome or set BURN_P2P_PLAYWRIGHT_CHROME",
    );
    print_probe(
        "Firefox",
        &Probe {
            ok: firefox.is_some(),
            output: Some(
                firefox
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "not found".into()),
            ),
        },
        "install Firefox or set BURN_P2P_FIREFOX_BIN",
    );
    print_optional("Docker", &docker);
    print_optional("Docker Compose", &compose);
    print_optional("Terraform", &terraform);
    println!("GREEN artifact directory: {}", artifact_root.display());
    println!(
        "GREEN free local ports: {}",
        ports
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "{} real-browser suites: {}",
        if browser_real_ready {
            "GREEN"
        } else {
            "YELLOW"
        },
        if browser_real_ready {
            "ready to run `cargo xtask browser real`"
        } else {
            "missing one or more browser prerequisites"
        }
    );
    Ok(())
}

fn setup_browser(workspace: &Workspace) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "setup-browser", Profile::Dev)?;
    let envs = BTreeMap::new();
    let mut steps = Vec::new();

    steps.push(workspace.run(
        &artifacts,
        "rustup-target-wasm32",
        "rustup",
        &[
            "target".into(),
            "add".into(),
            "wasm32-unknown-unknown".into(),
        ],
        &envs,
    )?);
    steps.push(workspace.run(
        &artifacts,
        "playwright-version",
        workspace.npx(),
        &["--yes".into(), "playwright".into(), "--version".into()],
        &envs,
    )?);
    steps.push(workspace.run(
        &artifacts,
        "playwright-install",
        workspace.npx(),
        &[
            "--yes".into(),
            "playwright".into(),
            "install".into(),
            "chromium".into(),
            "firefox".into(),
        ],
        &envs,
    )?);

    finalize_run(
        &artifacts,
        "setup-browser",
        Profile::Dev,
        &steps,
        json!({ "kind": "setup" }),
        true,
    )
}

fn run_deploy_compose(workspace: &Workspace, args: DeployComposeArgs) -> anyhow::Result<()> {
    ensure!(
        command_available("docker"),
        "docker is required for `cargo xtask deploy compose`"
    );
    let artifacts = ArtifactLayout::create(&workspace.root, "deploy-compose", args.common.profile)?;
    let compose_file = workspace
        .root
        .join("deploy/compose")
        .join(args.stack.file_name());
    ensure!(
        compose_file.exists(),
        "missing compose stack {}",
        compose_file.display()
    );
    let mut command = vec!["compose".into()];
    if let Some(env_file) = args.env_file.as_ref() {
        let env_file = workspace.root.join(env_file);
        command.push("--env-file".into());
        command.push(env_file.display().to_string());
    }
    command.push("-f".into());
    command.push(compose_file.display().to_string());
    for profile in &args.profile_name {
        command.push("--profile".into());
        command.push(profile.clone());
    }
    match args.action {
        DeployAction::Plan => {
            command.push("config".into());
        }
        DeployAction::Up => {
            command.push("up".into());
            command.push("--build".into());
            command.push("-d".into());
        }
        DeployAction::Down => {
            command.push("down".into());
            command.push("--remove-orphans".into());
        }
    }

    let envs = BTreeMap::new();
    let steps = vec![workspace.run(&artifacts, "deploy-compose", "docker", &command, &envs)?];
    finalize_run(
        &artifacts,
        "deploy-compose",
        args.common.profile,
        &steps,
        json!({
            "stack": args.stack.file_name(),
            "action": match args.action {
                DeployAction::Plan => "plan",
                DeployAction::Up => "up",
                DeployAction::Down => "down",
            },
            "env_file": args.env_file,
            "profiles": args.profile_name,
        }),
        args.common.keep_artifacts,
    )
}

fn run_deploy_cloud(
    workspace: &Workspace,
    provider: &str,
    args: DeployCloudArgs,
) -> anyhow::Result<()> {
    ensure!(
        command_available("terraform"),
        "terraform is required for `cargo xtask deploy {provider}`"
    );
    let artifacts = ArtifactLayout::create(
        &workspace.root,
        &format!("deploy-{provider}"),
        args.common.profile,
    )?;
    let stack_root = workspace.root.join("deploy/terraform").join(provider);
    ensure!(
        stack_root.exists(),
        "missing terraform stack {}",
        stack_root.display()
    );
    let mut envs = BTreeMap::new();
    if let Some(image) = args.bootstrap_image.as_ref() {
        envs.insert("TF_VAR_bootstrap_image".into(), image.clone());
    }
    if let Some(image) = args.validator_image.as_ref() {
        envs.insert("TF_VAR_validator_image".into(), image.clone());
    }
    if let Some(image) = args.trainer_image.as_ref() {
        envs.insert("TF_VAR_trainer_image".into(), image.clone());
    }
    let default_bootstrap_config = workspace.root.join("deploy/config/trusted-browser.json");
    let default_validator_config = workspace
        .root
        .join("deploy/config/reference-validator.json");
    let bootstrap_config_path = args
        .bootstrap_config
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or(default_bootstrap_config);
    let validator_config_path = args
        .validator_config
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or(default_validator_config);
    envs.insert(
        "TF_VAR_bootstrap_config_json".into(),
        fs::read_to_string(&bootstrap_config_path)
            .with_context(|| format!("failed to read {}", bootstrap_config_path.display()))?,
    );
    envs.insert(
        "TF_VAR_validator_config_json".into(),
        fs::read_to_string(&validator_config_path)
            .with_context(|| format!("failed to read {}", validator_config_path.display()))?,
    );

    let mut steps = Vec::new();
    steps.push(workspace.run(
        &artifacts,
        "terraform-init",
        "terraform",
        &[
            format!("-chdir={}", stack_root.display()),
            "init".into(),
            "-input=false".into(),
        ],
        &envs,
    )?);

    let mut terraform_args = vec![format!("-chdir={}", stack_root.display())];
    terraform_args.push(match args.action {
        DeployAction::Plan => "plan".into(),
        DeployAction::Up => "apply".into(),
        DeployAction::Down => "destroy".into(),
    });
    terraform_args.push("-input=false".into());
    if matches!(args.action, DeployAction::Up | DeployAction::Down) {
        terraform_args.push("-auto-approve".into());
    }
    let var_file = args.var_file.as_ref().map(|path| workspace.root.join(path));
    if let Some(var_file) = var_file.as_ref() {
        terraform_args.push(format!("-var-file={}", var_file.display()));
    }

    steps.push(workspace.run(
        &artifacts,
        &format!(
            "terraform-{}",
            match args.action {
                DeployAction::Plan => "plan",
                DeployAction::Up => "apply",
                DeployAction::Down => "destroy",
            }
        ),
        "terraform",
        &terraform_args,
        &envs,
    )?);

    finalize_run(
        &artifacts,
        &format!("deploy-{provider}"),
        args.common.profile,
        &steps,
        json!({
            "provider": provider,
            "action": match args.action {
                DeployAction::Plan => "plan",
                DeployAction::Up => "apply",
                DeployAction::Down => "destroy",
            },
            "var_file": var_file,
            "bootstrap_image": args.bootstrap_image,
            "validator_image": args.validator_image,
            "trainer_image": args.trainer_image,
            "bootstrap_config": bootstrap_config_path,
            "validator_config": validator_config_path,
        }),
        args.common.keep_artifacts,
    )
}

fn run_fast_checks(workspace: &Workspace, args: CommonArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "check", args.profile)?;
    let envs = BTreeMap::new();
    let mut steps = Vec::new();
    steps.push(workspace.run_cargo(&artifacts, "fmt", &["fmt", "--all", "--check"], &envs)?);
    steps.push(workspace.run_cargo(
        &artifacts,
        "clippy",
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
        &envs,
    )?);
    steps.push(workspace.run_cargo(
        &artifacts,
        "workspace-tests",
        &[
            "test",
            "--workspace",
            "--lib",
            "--bins",
            "--tests",
            "--exclude",
            "burn_p2p_testkit",
        ],
        &envs,
    )?);
    steps.push(workspace.run_cargo(
        &artifacts,
        "examples-compile",
        &[
            "test",
            "-p",
            "burn_p2p_testkit",
            "--test",
            "examples_compile",
        ],
        &envs,
    )?);
    steps.push(workspace.run_cargo(
        &artifacts,
        "deployment-profiles",
        &[
            "test",
            "-p",
            "burn_p2p_testkit",
            "--test",
            "deployment_profiles",
        ],
        &envs,
    )?);

    finalize_run(
        &artifacts,
        "check",
        args.profile,
        &steps,
        json!({ "kind": "fast-checks" }),
        args.keep_artifacts,
    )
}

fn run_publish_checks(workspace: &Workspace, args: CommonArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "check-publish", args.profile)?;
    let envs = BTreeMap::new();
    let mut steps = Vec::new();
    for (label, cargo_args) in [
        ("fmt", vec!["fmt", "--all", "--check"]),
        (
            "clippy-workspace",
            vec![
                "clippy",
                "--workspace",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
        ),
        (
            "clippy-burn",
            vec![
                "clippy",
                "-p",
                "burn_p2p",
                "--all-targets",
                "--features",
                "burn",
                "--",
                "-D",
                "warnings",
            ],
        ),
        (
            "clippy-swarm",
            vec![
                "clippy",
                "-p",
                "burn_p2p_swarm",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
        ),
        (
            "clippy-security",
            vec![
                "clippy",
                "-p",
                "burn_p2p_security",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
        ),
        (
            "clippy-publish",
            vec![
                "clippy",
                "-p",
                "burn_p2p_publish",
                "--all-targets",
                "--features",
                "fs,s3",
                "--",
                "-D",
                "warnings",
            ],
        ),
        (
            "clippy-bootstrap-default",
            vec![
                "clippy",
                "-p",
                "burn_p2p_bootstrap",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
        ),
        (
            "clippy-bootstrap-static",
            vec![
                "clippy",
                "-p",
                "burn_p2p_bootstrap",
                "--no-default-features",
                "--features",
                "admin-http,metrics,auth-static",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
        ),
        (
            "clippy-bootstrap-full",
            vec![
                "clippy",
                "-p",
                "burn_p2p_bootstrap",
                "--no-default-features",
                "--features",
                "admin-http,metrics,metrics-indexer,artifact-publish,artifact-download,artifact-fs,artifact-s3,browser-edge,browser-join,rbac,auth-static,auth-github,auth-oidc,auth-oauth,social",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
        ),
        (
            "test-workspace",
            vec!["test", "--workspace", "--exclude", "burn_p2p"],
        ),
        (
            "test-burn",
            vec!["test", "-p", "burn_p2p", "--features", "burn"],
        ),
        (
            "test-publish",
            vec!["test", "-p", "burn_p2p_publish", "--features", "fs,s3"],
        ),
        (
            "test-bootstrap-static",
            vec![
                "test",
                "-p",
                "burn_p2p_bootstrap",
                "--no-default-features",
                "--features",
                "admin-http,metrics,auth-static",
            ],
        ),
        (
            "test-bootstrap-full",
            vec![
                "test",
                "-p",
                "burn_p2p_bootstrap",
                "--no-default-features",
                "--features",
                "admin-http,metrics,metrics-indexer,artifact-publish,artifact-download,artifact-fs,artifact-s3,browser-edge,browser-join,rbac,auth-static,auth-github,auth-oidc,auth-oauth,social",
            ],
        ),
        ("test-docs", vec!["test", "--workspace", "--doc"]),
        ("doc", vec!["doc", "--workspace", "--no-deps"]),
    ] {
        let mut step_envs = envs.clone();
        if label == "test-docs" || label == "doc" {
            step_envs.insert("RUSTDOCFLAGS".into(), "-D warnings".into());
        }
        steps.push(workspace.run_cargo(&artifacts, label, &cargo_args, &step_envs)?);
    }

    let allow_dirty = allow_dirty_packages();
    let package_flags = if allow_dirty {
        vec!["--allow-dirty".to_owned()]
    } else {
        Vec::new()
    };
    for crate_name in PUBLISH_CRATES {
        let mut args_vec = vec![
            "package".to_owned(),
            "--list".to_owned(),
            "-p".to_owned(),
            (*crate_name).to_owned(),
        ];
        args_vec.extend(package_flags.clone());
        steps.push(workspace.run_cargo(
            &artifacts,
            &format!("package-list-{crate_name}"),
            &args_vec,
            &envs,
        )?);
    }

    finalize_run(
        &artifacts,
        "check-publish",
        args.profile,
        &steps,
        json!({ "kind": "publish-checks" }),
        args.keep_artifacts,
    )
}

fn run_e2e_smoke(workspace: &Workspace, args: CommonArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "e2e-smoke", args.profile)?;
    let envs = BTreeMap::new();
    let steps = vec![
        workspace.run_cargo(
            &artifacts,
            "multiprocess-smoke-cluster",
            &[
                "test",
                "-p",
                "burn_p2p_testkit",
                "--test",
                "multiprocess",
                "smoke_cluster_runs_across_processes",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
        workspace.run_cargo(
            &artifacts,
            "multiprocess-validator-restart",
            &[
                "test",
                "-p",
                "burn_p2p_testkit",
                "--test",
                "multiprocess",
                "validator_restart_restores_head_across_processes",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
        workspace.run_cargo(
            &artifacts,
            "metrics-catchup-store",
            &[
                "test",
                "-p",
                "burn_p2p_metrics",
                "metrics_store_persists_and_recovers_materialized_views",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
        workspace.run_cargo(
            &artifacts,
            "publication-download-ticket",
            &[
                "test",
                "-p",
                "burn_p2p_publish",
                "download_ticket_streams_published_artifact_bytes",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
    ];

    finalize_run(
        &artifacts,
        "e2e-smoke",
        args.profile,
        &steps,
        json!({ "kind": "e2e-smoke" }),
        args.keep_artifacts,
    )
}

fn run_e2e_mixed(workspace: &Workspace, args: CommonArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "e2e-mixed", args.profile)?;
    let envs = BTreeMap::new();
    let steps = vec![
        workspace.run_cargo(
            &artifacts,
            "mixed-browser-worker",
            &[
                "test",
                "-p",
                "burn_p2p_testkit",
                "--test",
                "mixed_browser_worker",
                "mixed_fleet_simulation_drives_browser_worker_training_and_validation",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
        workspace.run_cargo(
            &artifacts,
            "mixed-browser-worker-adversarial",
            &[
                "test",
                "-p",
                "burn_p2p_testkit",
                "--test",
                "mixed_browser_worker",
                "mixed_fleet_browser_adversarial_smoke_rejects_browser_style_replay_and_free_riding",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
        workspace.run_cargo(
            &artifacts,
            "browser-transport-memory",
            &[
                "test",
                "-p",
                "burn_p2p_testkit",
                "--test",
                "browser_swarm_transport",
                "browser_worker_promotes_to_trainer_only_after_live_memory_swarm_snapshot",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
        workspace.run_cargo(
            &artifacts,
            "browser-transport-tcp",
            &[
                "test",
                "-p",
                "burn_p2p_testkit",
                "--test",
                "browser_swarm_transport",
                "browser_worker_promotes_to_verifier_only_after_live_tcp_swarm_snapshot",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
    ];

    finalize_run(
        &artifacts,
        "e2e-mixed",
        args.profile,
        &steps,
        json!({ "kind": "e2e-mixed" }),
        args.keep_artifacts,
    )
}

fn wait_for_path(path: &std::path::Path, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    anyhow::bail!("timed out waiting for {}", path.display());
}

fn run_e2e_mnist(workspace: &Workspace, args: CommonArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "e2e-mnist", args.profile)?;
    let envs = BTreeMap::new();
    let demo_root = artifacts.root.join("mnist-demo");
    let mut steps = Vec::new();
    let browser_probe_root = artifacts.root.join("browser-wasm-probe");
    let browser_probe_assets = browser_probe_root.join("assets");
    steps.push(workspace.run_cargo(
        &artifacts,
        "mnist-browser-probe-assets",
        &[
            "run",
            "--manifest-path",
            "examples/mnist_p2p_demo/Cargo.toml",
            "--bin",
            "mnist_browser_probe_assets",
            "--",
            "--output",
            browser_probe_assets.display().to_string().as_str(),
        ],
        &envs,
    )?);
    let mut demo_process = workspace.spawn_cargo(
        &artifacts,
        "mnist-demo",
        &[
            "run",
            "--manifest-path",
            "examples/mnist_p2p_demo/Cargo.toml",
            "--bin",
            "mnist_p2p_demo",
            "--",
            "--output",
            demo_root.display().to_string().as_str(),
            "--await-live-browser-probe",
        ],
        &envs,
    )?;
    let browser_manifest_path = demo_root.join("browser-live.json");
    let browser_probe_summary = (|| -> anyhow::Result<serde_json::Value> {
        wait_for_path(&browser_manifest_path, Duration::from_secs(120))?;
        let browser_manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(&browser_manifest_path)
                .with_context(|| format!("failed to read {}", browser_manifest_path.display()))?,
        )
        .with_context(|| format!("failed to decode {}", browser_manifest_path.display()))?;
        let browser_probe_config = json!({
            "asset_root": browser_probe_assets.display().to_string(),
            "dataset_root": demo_root.join("dataset").display().to_string(),
            "artifact_root": browser_probe_root.display().to_string(),
            "network_id": browser_manifest
                .get("network_id")
                .and_then(serde_json::Value::as_str)
                .context("browser live manifest missing network_id")?,
            "experiment_id": browser_manifest
                .get("experiment_id")
                .and_then(serde_json::Value::as_str)
                .context("browser live manifest missing experiment_id")?,
            "revision_id": browser_manifest
                .get("revision_id")
                .and_then(serde_json::Value::as_str)
                .context("browser live manifest missing revision_id")?,
            "selected_head_id": browser_manifest
                .get("selected_head_id")
                .and_then(serde_json::Value::as_str)
                .context("browser live manifest missing selected_head_id")?,
            "lease_id": browser_manifest
                .get("lease_id")
                .and_then(serde_json::Value::as_str)
                .context("browser live manifest missing lease_id")?,
            "leased_microshards": browser_manifest
                .get("leased_microshards")
                .cloned()
                .context("browser live manifest missing leased_microshards")?,
            "edge_base_url": browser_manifest
                .get("edge_base_url")
                .and_then(serde_json::Value::as_str)
                .context("browser live manifest missing edge_base_url")?,
            "release_train_hash": browser_manifest
                .get("release_train_hash")
                .and_then(serde_json::Value::as_str)
                .context("browser live manifest missing release_train_hash")?,
            "target_artifact_id": browser_manifest
                .get("target_artifact_id")
                .and_then(serde_json::Value::as_str)
                .context("browser live manifest missing target_artifact_id")?,
            "target_artifact_hash": browser_manifest
                .get("target_artifact_hash")
                .and_then(serde_json::Value::as_str)
                .context("browser live manifest missing target_artifact_hash")?,
            "workload_id": browser_manifest
                .get("workload_id")
                .and_then(serde_json::Value::as_str)
                .context("browser live manifest missing workload_id")?,
            "principal_id": browser_manifest
                .get("principal_id")
                .and_then(serde_json::Value::as_str)
                .context("browser live manifest missing principal_id")?,
            "batch_size": 32,
            "learning_rate": 1.0e-3,
            "max_train_batches": 3,
            "profiles": [
                {
                    "slug": "fast",
                    "latency_ms": 0,
                    "bandwidth_bytes_per_sec": 0,
                },
                {
                    "slug": "slow",
                    "latency_ms": 150,
                    "bandwidth_bytes_per_sec": 262144,
                }
            ],
        });
        artifacts.write_json("configs/mnist-browser-probe.json", &browser_probe_config)?;
        let capture_envs = portal_capture_envs(args.profile, false);
        steps.push(
            workspace.run_node(
                &artifacts,
                "mnist-browser-wasm-probe",
                &[
                    "examples/mnist_p2p_demo/scripts/mnist_browser_wasm_probe.mjs",
                    artifacts
                        .root
                        .join("configs/mnist-browser-probe.json")
                        .display()
                        .to_string()
                        .as_str(),
                ],
                &capture_envs,
            )?,
        );
        let browser_probe_summary_path = browser_probe_root.join("browser-wasm/summary.json");
        let browser_probe_summary: serde_json::Value =
            serde_json::from_slice(&fs::read(&browser_probe_summary_path).with_context(|| {
                format!("failed to read {}", browser_probe_summary_path.display())
            })?)
            .with_context(|| {
                format!("failed to decode {}", browser_probe_summary_path.display())
            })?;
        fs::write(
            demo_root.join("browser-probe-result.json"),
            serde_json::to_vec_pretty(&browser_probe_summary)?,
        )
        .with_context(|| {
            format!(
                "failed to write {}",
                demo_root.join("browser-probe-result.json").display()
            )
        })?;
        Ok(browser_probe_summary)
    })();
    let browser_probe_summary = match browser_probe_summary {
        Ok(summary) => summary,
        Err(error) => {
            let _ = demo_process.kill();
            let _ = demo_process.wait(&artifacts);
            return Err(error);
        }
    };
    steps.push(demo_process.wait(&artifacts)?);

    let summary_path = demo_root.join("summary.json");
    let summary: serde_json::Value = serde_json::from_slice(
        &fs::read(&summary_path)
            .with_context(|| format!("failed to read {}", summary_path.display()))?,
    )
    .with_context(|| format!("failed to decode {}", summary_path.display()))?;
    let correctness_path = demo_root.join("correctness.json");
    let correctness: serde_json::Value = serde_json::from_slice(
        &fs::read(&correctness_path)
            .with_context(|| format!("failed to read {}", correctness_path.display()))?,
    )
    .with_context(|| format!("failed to decode {}", correctness_path.display()))?;
    let export_path = demo_root.join("browser-export.json");
    let export: MnistRunExportFile = serde_json::from_slice(
        &fs::read(&export_path)
            .with_context(|| format!("failed to read {}", export_path.display()))?,
    )
    .with_context(|| format!("failed to decode {}", export_path.display()))?;
    ensure!(
        correctness
            .get("baseline_outperformed_low_lr")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo did not show baseline experiment outperforming low-lr variant",
    );
    ensure!(
        correctness
            .get("late_joiner_synced_checkpoint")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo late joiner did not sync the accepted checkpoint",
    );
    ensure!(
        correctness
            .get("shard_assignments_are_distinct")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo did not assign distinct shards to concurrent baseline trainers",
    );
    ensure!(
        correctness
            .pointer("/browser_dataset_access/fetch_manifest_requested")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo browser dataset probe did not request fetch-manifest.json",
    );
    ensure!(
        correctness
            .pointer("/browser_dataset_access/fetched_only_leased_shards")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo browser dataset probe fetched shards outside the active lease",
    );
    ensure!(
        correctness
            .pointer("/browser_dataset_access/shards_distributed_over_p2p")
            .and_then(serde_json::Value::as_bool)
            == Some(false),
        "mnist demo browser dataset probe reported shard transport over the p2p overlay",
    );
    ensure!(
        correctness
            .pointer("/assessment/live_native_training")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo did not report live native training",
    );
    ensure!(
        correctness
            .pointer("/assessment/browser_runtime_roles_exercised")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo did not exercise browser runtime roles",
    );
    ensure!(
        correctness
            .pointer("/resilience/trainer_restart_reconnected")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo trainer restart drill did not reconnect",
    );
    ensure!(
        correctness
            .pointer("/resilience/trainer_restart_resumed_training")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo trainer restart drill did not resume training",
    );
    artifacts.write_json("metrics/mnist-summary.json", &summary)?;
    artifacts.write_json("metrics/mnist-correctness.json", &correctness)?;

    let browser_bundle = artifacts.root.join("playwright-bundle");
    fs::create_dir_all(&browser_bundle)?;
    build_browser_app_web_assets(browser_bundle.join("assets"))
        .context("failed to build browser app wasm assets")?;
    let capture_specs = export
        .browser_scenarios
        .into_iter()
        .map(|scenario| BrowserPortalCaptureSpec {
            slug: scenario.slug,
            title: scenario.title,
            description: scenario.description,
            default_surface: scenario.default_surface,
            snapshot: scenario.snapshot,
            metrics_catchup: scenario.metrics_catchup,
            runtime_states: scenario.runtime_states,
            interactions: scenario.interactions,
            viewport: scenario.viewport,
        })
        .collect::<Vec<_>>();
    let manifest = write_browser_portal_capture_bundle(&browser_bundle, &capture_specs)
        .context("failed to write mnist portal capture bundle")?;
    artifacts.write_json("configs/portal-manifest.json", &manifest)?;

    let capture_envs = portal_capture_envs(args.profile, false);
    steps.push(
        workspace.run_node(
            &artifacts,
            "portal-playwright-capture",
            &[
                "crates/burn_p2p_testkit/scripts/portal_playwright_capture.mjs",
                browser_bundle
                    .join("manifest.json")
                    .display()
                    .to_string()
                    .as_str(),
            ],
            &capture_envs,
        )?,
    );

    let bundle_artifacts = artifacts.root.join("playwright");
    copy_dir_all(&browser_bundle, &bundle_artifacts)?;
    copy_files_with_extension_tree(&browser_probe_root, "png", &artifacts.screenshots)?;
    copy_files_with_extension_tree(&browser_probe_root, "zip", &artifacts.playwright_traces)?;
    copy_files_with_extension_tree(&bundle_artifacts, "png", &artifacts.screenshots)?;
    copy_files_with_extension_tree(&bundle_artifacts, "zip", &artifacts.playwright_traces)?;
    let adversarial_correctness = mnist_adversarial_correctness_summary();
    ensure!(
        browser_probe_summary
            .pointer("/browser_execution/live_browser_training")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist browser wasm probe did not execute live burn training in the browser",
    );
    ensure!(
        browser_probe_summary
            .pointer("/browser_execution/browser_latency_emulated")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist browser wasm probe did not run with a latency-shaped dataset profile",
    );
    ensure!(
        browser_probe_summary
            .pointer("/browser_execution/slower_profile_increased_total_time")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist browser wasm probe did not show higher total time under the slower latency profile",
    );
    for profile in browser_probe_summary
        .get("profiles")
        .and_then(serde_json::Value::as_array)
        .context("mnist browser wasm probe summary missing profiles")?
    {
        ensure!(
            profile
                .get("fetch_manifest_requested")
                .and_then(serde_json::Value::as_bool)
                == Some(true),
            "mnist browser wasm probe profile never requested fetch-manifest.json",
        );
        ensure!(
            profile
                .get("fetched_only_leased_shards")
                .and_then(serde_json::Value::as_bool)
                == Some(true),
            "mnist browser wasm probe fetched shards outside the leased set",
        );
    }
    ensure!(
        correctness
            .pointer("/browser_execution/trainer_runtime_and_wasm_training_coherent")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist correctness summary did not prove a coherent live browser/native training run",
    );
    ensure!(
        correctness
            .pointer("/assessment/live_browser_training")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist correctness summary did not report live browser training",
    );
    for attack in adversarial_correctness
        .get("reports")
        .and_then(serde_json::Value::as_array)
        .context("mnist adversarial correctness summary missing reports")?
    {
        ensure!(
            attack
                .get("malicious_update_acceptance_rate")
                .and_then(serde_json::Value::as_f64)
                == Some(0.0),
            "mnist adversarial correctness annex accepted a malicious update",
        );
    }

    finalize_run(
        &artifacts,
        "e2e-mnist",
        args.profile,
        &steps,
        json!({
            "kind": "e2e-mnist",
            "demo_root": demo_root.display().to_string(),
            "bundle_root": bundle_artifacts.display().to_string(),
            "summary": summary,
            "correctness": correctness,
            "browser_wasm_probe": browser_probe_summary,
            "correctness_annex": {
                "adversarial": adversarial_correctness,
            },
        }),
        args.keep_artifacts,
    )
}

fn run_e2e_services(workspace: &Workspace, args: CommonArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "e2e-services", args.profile)?;
    let envs = BTreeMap::new();
    let steps = vec![
        workspace.run_cargo(
            &artifacts,
            "metrics-catchup-store",
            &[
                "test",
                "-p",
                "burn_p2p_metrics",
                "metrics_store_persists_and_recovers_materialized_views",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
        workspace.run_cargo(
            &artifacts,
            "publication-export-dedup",
            &[
                "test",
                "-p",
                "burn_p2p_publish",
                "request_export_deduplicates_ready_publications_and_survives_restart",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
        workspace.run_cargo(
            &artifacts,
            "publication-download-ticket",
            &[
                "test",
                "-p",
                "burn_p2p_publish",
                "download_ticket_streams_published_artifact_bytes",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
    ];

    finalize_run(
        &artifacts,
        "e2e-services",
        args.profile,
        &steps,
        json!({ "kind": "e2e-services" }),
        args.keep_artifacts,
    )
}

fn run_adversarial_smoke(workspace: &Workspace, args: CommonArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "adversarial-smoke", args.profile)?;
    let envs = BTreeMap::new();
    let steps = vec![workspace.run_cargo(
        &artifacts,
        "mixed-browser-worker-adversarial",
        &[
            "test",
            "-p",
            "burn_p2p_testkit",
            "--test",
            "mixed_browser_worker",
            "mixed_fleet_browser_adversarial_smoke_rejects_browser_style_replay_and_free_riding",
            "--",
            "--exact",
            "--nocapture",
        ],
        &envs,
    )?];
    let reports = vec![
        run_scenario(
            RobustnessPolicy::balanced(),
            AdversarialAttack::Replay,
            10,
            0.2,
        ),
        run_scenario(
            RobustnessPolicy::balanced(),
            AdversarialAttack::FreeRider,
            10,
            0.2,
        ),
        run_scenario(
            RobustnessPolicy::balanced(),
            AdversarialAttack::SignFlip,
            10,
            0.2,
        ),
        run_scenario(
            RobustnessPolicy::strict(),
            AdversarialAttack::BackdoorLowNorm,
            10,
            0.2,
        ),
    ];
    artifacts.write_json("metrics/scenario-reports.json", &reports)?;
    let summary = summarize_adversarial_reports(&reports);
    artifacts.write_json("metrics/summary.json", &summary)?;

    finalize_run(
        &artifacts,
        "adversarial-smoke",
        args.profile,
        &steps,
        json!({
            "kind": "adversarial-smoke",
            "report_count": reports.len(),
            "summary": summary,
        }),
        args.keep_artifacts,
    )
}

fn run_adversarial_matrix(workspace: &Workspace, args: CommonArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "adversarial-matrix", args.profile)?;
    let mut reports = run_attack_matrix();
    let fractions = [0.10, 0.20, 0.33];
    let extra_attacks = [
        AdversarialAttack::LittleEnough,
        AdversarialAttack::InnerProduct,
        AdversarialAttack::OppositeDirection,
        AdversarialAttack::Stale,
        AdversarialAttack::NanInf,
        AdversarialAttack::WrongShape,
        AdversarialAttack::ColludingCluster,
        AdversarialAttack::AlternatingDrift,
        AdversarialAttack::ReputationBuildStrike,
        AdversarialAttack::BackdoorLowNorm,
        AdversarialAttack::LateFlood,
        AdversarialAttack::UndeliverableArtifact,
    ];
    for attack in extra_attacks {
        for malicious_fraction in fractions {
            reports.push(run_scenario(
                RobustnessPolicy::strict(),
                attack.clone(),
                12,
                malicious_fraction,
            ));
        }
    }
    artifacts.write_json("metrics/scenario-reports.json", &reports)?;
    let summary = summarize_adversarial_reports(&reports);
    artifacts.write_json("metrics/summary.json", &summary)?;

    finalize_run(
        &artifacts,
        "adversarial-matrix",
        args.profile,
        &[],
        json!({
            "kind": "adversarial-matrix",
            "report_count": reports.len(),
            "summary": summary,
        }),
        args.keep_artifacts,
    )
}

fn run_adversarial_chaos(workspace: &Workspace, args: ChaosArgs) -> anyhow::Result<()> {
    let artifacts =
        ArtifactLayout::create(&workspace.root, "adversarial-chaos", args.common.profile)?;
    let seed = args.seed.unwrap_or_else(seed_now);
    let event_count = args
        .events
        .unwrap_or_else(|| args.common.profile.settings().chaos_events);
    let peer_count = args
        .peers
        .unwrap_or_else(|| args.common.profile.settings().multiprocess_peers.max(6));
    let attacks = [
        AdversarialAttack::Replay,
        AdversarialAttack::FreeRider,
        AdversarialAttack::SignFlip,
        AdversarialAttack::LittleEnough,
        AdversarialAttack::ModelReplacement,
        AdversarialAttack::InnerProduct,
        AdversarialAttack::Stale,
        AdversarialAttack::ColludingCluster,
        AdversarialAttack::BackdoorLowNorm,
        AdversarialAttack::LateFlood,
    ];
    let malicious_fractions = [0.10, 0.20, 0.33];
    let mut reports = Vec::with_capacity(event_count as usize);
    let mut attack_sequence = Vec::with_capacity(event_count as usize);

    for offset in 0..event_count as usize {
        let attack = attacks[((seed as usize).wrapping_add(offset * 17)) % attacks.len()].clone();
        let malicious_fraction = malicious_fractions
            [((seed as usize / 7).wrapping_add(offset)) % malicious_fractions.len()];
        let mut policy = match offset % 3 {
            0 => RobustnessPolicy::balanced(),
            1 => RobustnessPolicy::strict(),
            _ => {
                let mut policy = RobustnessPolicy::balanced();
                policy.aggregation_policy.strategy = AggregationStrategy::Median;
                policy
            }
        };
        if matches!(
            attack,
            AdversarialAttack::ModelReplacement
                | AdversarialAttack::Stale
                | AdversarialAttack::LateFlood
                | AdversarialAttack::ReputationBuildStrike
        ) {
            policy
                .quarantine_policy
                .quarantine_after_consecutive_rejections = 1;
        }
        reports.push(run_scenario(
            policy.clone(),
            attack.clone(),
            peer_count as usize,
            malicious_fraction,
        ));
        attack_sequence.push(json!({
            "index": offset,
            "attack": format!("{attack:?}"),
            "malicious_fraction": malicious_fraction,
            "preset": format!("{:?}", policy.preset),
        }));
    }

    artifacts.write_text("seed.txt", format!("{seed}\n"))?;
    artifacts.write_json("topology/attack-sequence.json", &attack_sequence)?;
    artifacts.write_json("metrics/scenario-reports.json", &reports)?;
    let summary = summarize_adversarial_reports(&reports);
    artifacts.write_json("metrics/summary.json", &summary)?;

    finalize_run(
        &artifacts,
        "adversarial-chaos",
        args.common.profile,
        &[],
        json!({
            "kind": "adversarial-chaos",
            "seed": seed,
            "event_count": event_count,
            "peer_count": peer_count,
            "summary": summary,
        }),
        args.common.keep_artifacts,
    )
}

fn portal_capture_envs(profile: Profile, headed: bool) -> BTreeMap<String, String> {
    let mut envs = BTreeMap::new();
    let headed = headed || !profile.settings().headless;
    envs.insert(
        "BURN_P2P_PLAYWRIGHT_HEADED".into(),
        if headed { "1" } else { "0" }.into(),
    );
    if let Some(chrome) = resolve_chrome_path() {
        envs.insert(
            "BURN_P2P_PLAYWRIGHT_CHROME".into(),
            chrome.display().to_string(),
        );
    }
    envs
}

fn run_browser_smoke(workspace: &Workspace, args: BrowserArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "browser-smoke", args.common.profile)?;
    let mut steps = Vec::new();
    let envs = BTreeMap::new();
    steps.push(workspace.run_cargo(
        &artifacts,
        "browser-matrix",
        &["test", "-p", "burn_p2p_testkit", "--test", "browser_matrix"],
        &envs,
    )?);
    steps.push(workspace.run_cargo(
        &artifacts,
        "portal-capture-bundle",
        &[
            "test",
            "-p",
            "burn_p2p_testkit",
            "--test",
            "portal_playwright",
            "portal_capture_bundle_renders_reference_scenarios",
            "--",
            "--exact",
            "--nocapture",
        ],
        &envs,
    )?);

    let browser_bundle = artifacts.root.join("playwright-bundle");
    fs::create_dir_all(&browser_bundle)?;
    build_browser_app_web_assets(browser_bundle.join("assets"))
        .context("failed to build browser app wasm assets")?;
    let manifest = write_portal_capture_bundle(&browser_bundle)
        .context("failed to write portal capture bundle")?;
    artifacts.write_json("configs/portal-manifest.json", &manifest)?;

    let envs = portal_capture_envs(args.common.profile, args.headed);
    steps.push(
        workspace.run_node(
            &artifacts,
            "portal-playwright-capture",
            &[
                "crates/burn_p2p_testkit/scripts/portal_playwright_capture.mjs",
                browser_bundle
                    .join("manifest.json")
                    .display()
                    .to_string()
                    .as_str(),
            ],
            &envs,
        )?,
    );

    let bundle_artifacts = artifacts.root.join("playwright");
    copy_dir_all(&browser_bundle, &bundle_artifacts)?;
    copy_files_with_extension_tree(&bundle_artifacts, "png", &artifacts.screenshots)?;
    copy_files_with_extension_tree(&bundle_artifacts, "zip", &artifacts.playwright_traces)?;

    finalize_run(
        &artifacts,
        "browser-smoke",
        args.common.profile,
        &steps,
        json!({
            "kind": "browser-smoke",
            "bundle_root": bundle_artifacts.display().to_string(),
        }),
        args.common.keep_artifacts,
    )
}

fn run_browser_trainer(workspace: &Workspace, args: BrowserArgs) -> anyhow::Result<()> {
    let artifacts =
        ArtifactLayout::create(&workspace.root, "browser-trainer", args.common.profile)?;
    let envs = BTreeMap::new();
    let mut steps = Vec::new();
    steps.push(workspace.run_cargo(
        &artifacts,
        "browser-matrix",
        &["test", "-p", "burn_p2p_testkit", "--test", "browser_matrix"],
        &envs,
    )?);
    steps.push(workspace.run_cargo(
        &artifacts,
        "mixed-browser-worker",
        &[
            "test",
            "-p",
            "burn_p2p_testkit",
            "--test",
            "mixed_browser_worker",
            "mixed_fleet_simulation_drives_browser_worker_training_and_validation",
            "--",
            "--exact",
            "--nocapture",
        ],
        &envs,
    )?);

    let browser_bundle = artifacts.root.join("playwright-bundle");
    fs::create_dir_all(&browser_bundle)?;
    build_browser_app_web_assets(browser_bundle.join("assets"))
        .context("failed to build browser app wasm assets")?;
    let manifest = write_portal_capture_bundle(&browser_bundle)
        .context("failed to write portal capture bundle")?;
    artifacts.write_json("configs/portal-manifest.json", &manifest)?;
    let capture_envs = portal_capture_envs(args.common.profile, args.headed);
    steps.push(
        workspace.run_node(
            &artifacts,
            "portal-playwright-capture",
            &[
                "crates/burn_p2p_testkit/scripts/portal_playwright_capture.mjs",
                browser_bundle
                    .join("manifest.json")
                    .display()
                    .to_string()
                    .as_str(),
            ],
            &capture_envs,
        )?,
    );
    let bundle_artifacts = artifacts.root.join("playwright");
    copy_dir_all(&browser_bundle, &bundle_artifacts)?;
    copy_files_with_extension_tree(&bundle_artifacts, "png", &artifacts.screenshots)?;
    copy_files_with_extension_tree(&bundle_artifacts, "zip", &artifacts.playwright_traces)?;

    finalize_run(
        &artifacts,
        "browser-trainer",
        args.common.profile,
        &steps,
        json!({
            "kind": "browser-trainer",
            "bundle_root": bundle_artifacts.display().to_string(),
        }),
        args.common.keep_artifacts,
    )
}

fn run_browser_real(workspace: &Workspace, args: BrowserArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "browser-real", args.common.profile)?;
    let mut envs = BTreeMap::new();
    let headed = args.headed || !args.common.profile.settings().headless;
    envs.insert(
        "BURN_P2P_PLAYWRIGHT_HEADED".into(),
        if headed { "1" } else { "0" }.into(),
    );
    envs.insert(
        "BURN_P2P_BROWSER_PROBE_ITERATIONS".into(),
        args.common
            .profile
            .settings()
            .browser_iterations
            .to_string(),
    );
    envs.insert(
        "BURN_P2P_BROWSER_PROBE_SHARD_COUNT".into(),
        args.common
            .profile
            .settings()
            .browser_shard_count
            .to_string(),
    );
    envs.insert(
        "BURN_P2P_BROWSER_PROBE_ARTIFACT_DIR".into(),
        artifacts.root.join("real-browser").display().to_string(),
    );
    if let Some(chrome) = resolve_chrome_path() {
        envs.insert("BURN_P2P_CHROME_BIN".into(), chrome.display().to_string());
    }
    if let Some(firefox) = resolve_firefox_path() {
        envs.insert("BURN_P2P_FIREFOX_BIN".into(), firefox.display().to_string());
    }
    let steps = vec![workspace.run_cargo(
        &artifacts,
        "browser-real-device-probe",
        &[
            "test",
            "-p",
            "burn_p2p_testkit",
            "--test",
            "browser_real_device",
            "browser_real_device_probe_reports_budget_and_role_evidence",
            "--",
            "--ignored",
            "--exact",
            "--nocapture",
        ],
        &envs,
    )?];
    copy_files_with_extension_tree(
        &artifacts.root.join("real-browser"),
        "png",
        &artifacts.screenshots,
    )?;
    copy_files_with_extension_tree(
        &artifacts.root.join("real-browser"),
        "zip",
        &artifacts.playwright_traces,
    )?;
    finalize_run(
        &artifacts,
        "browser-real",
        args.common.profile,
        &steps,
        json!({ "kind": "browser-real" }),
        args.common.keep_artifacts,
    )
}

fn run_stress_multiprocess(workspace: &Workspace, args: MultiprocessArgs) -> anyhow::Result<()> {
    let artifacts =
        ArtifactLayout::create(&workspace.root, "stress-multiprocess", args.common.profile)?;
    let envs = BTreeMap::new();
    let steps = vec![workspace.run_cargo(
        &artifacts,
        "build-testkit-node",
        &[
            "build",
            "-p",
            "burn_p2p_testkit",
            "--bin",
            "burn-p2p-testkit-node",
        ],
        &envs,
    )?];

    let peer_count = args
        .peers
        .unwrap_or_else(|| args.common.profile.settings().multiprocess_peers);
    let duration = args
        .duration
        .as_deref()
        .map(parse_duration_string)
        .transpose()?;
    let trainer_count = peer_count.saturating_sub(1).max(1);
    let duration_secs = duration
        .unwrap_or_else(|| Duration::from_secs(60))
        .as_secs();
    let window_count = args
        .common
        .profile
        .settings()
        .trainer_windows
        .max(((duration_secs / 30).max(1)) as u32);
    let config = SyntheticSoakConfig {
        root: artifacts.root.join("synthetic-soak"),
        trainer_count,
        trainer_window_count: window_count,
        startup_timeout_secs: 20,
        poll_interval_ms: 50,
        sync_timeout_secs: 20,
        merge_wait_timeout_secs: 20,
    };
    artifacts.write_json("configs/multiprocess-config.json", &config)?;

    let node_binary = workspace
        .root
        .join("target")
        .join("debug")
        .join(format!("burn-p2p-testkit-node{}", env::consts::EXE_SUFFIX));
    let summary = run_synthetic_process_soak(&config, &node_binary)
        .context("synthetic multiprocess soak failed")?;
    artifacts.write_json("topology/soak-summary.json", &summary)?;

    finalize_run(
        &artifacts,
        "stress-multiprocess",
        args.common.profile,
        &steps,
        json!({
            "kind": "stress-multiprocess",
            "peer_count": peer_count,
            "trainer_count": trainer_count,
            "trainer_window_count": window_count,
            "soak_root": summary.root.display().to_string(),
        }),
        args.common.keep_artifacts,
    )
}

fn run_stress_chaos(workspace: &Workspace, args: ChaosArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "stress-chaos", args.common.profile)?;
    let seed = args.seed.unwrap_or_else(seed_now);
    let peer_count = args.peers.unwrap_or(6);
    let event_count = args
        .events
        .unwrap_or_else(|| args.common.profile.settings().chaos_events);
    let events = generate_chaos_events(seed, event_count, peer_count.max(1));
    let mut spec = SimulationSpec {
        peer_count,
        browser_peer_count: peer_count.min(2),
        window_count: args.common.profile.settings().trainer_windows.max(2),
        chaos_events: events.clone(),
        ..SimulationSpec::default()
    };
    if spec.browser_peer_count >= spec.peer_count {
        spec.browser_peer_count = spec.peer_count.saturating_sub(1);
    }

    let started = Instant::now();
    let outcome = SimulationRunner::default()
        .run(spec.clone())
        .context("chaos simulation failed")?;
    let duration_ms = started.elapsed().as_millis();
    let final_head = outcome
        .windows
        .last()
        .and_then(|window| window.merge_certificate.as_ref())
        .map(|certificate| certificate.merged_head_id.as_str().to_owned());

    let rejected_by_peer = outcome
        .windows
        .iter()
        .flat_map(|window| window.rejected_updates.iter())
        .fold(BTreeMap::<String, usize>::new(), |mut acc, update| {
            *acc.entry(update.peer_id.to_string()).or_default() += 1;
            acc
        });

    artifacts.write_json("topology/chaos-events.json", &events)?;
    artifacts.write_json("topology/outcome.json", &outcome)?;
    artifacts.write_text("seed.txt", format!("{seed}\n"))?;

    let steps = Vec::<StepRecord>::new();
    finalize_run(
        &artifacts,
        "stress-chaos",
        args.common.profile,
        &steps,
        json!({
            "kind": "stress-chaos",
            "seed": seed,
            "peer_count": spec.peer_count,
            "browser_peer_count": spec.browser_peer_count,
            "event_count": event_count,
            "duration_ms": duration_ms,
            "final_head_id": final_head,
            "merge_count": outcome.windows.iter().filter(|window| window.merge_certificate.is_some()).count(),
            "rejected_updates_by_peer": rejected_by_peer,
        }),
        args.common.keep_artifacts,
    )
}

fn run_bench_core(workspace: &Workspace, args: BenchArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "bench-core", args.common.profile)?;
    let envs = BTreeMap::new();
    let steps = vec![workspace.run_cargo(
        &artifacts,
        "bench-core",
        &[
            "bench",
            "-p",
            "burn_p2p_testkit",
            "--bench",
            "merge_topology",
            "--bench",
            "browser_path",
            "--",
            "--noplot",
        ],
        &envs,
    )?];
    let criterion_dir = workspace.root.join("target/criterion");
    if criterion_dir.exists() {
        copy_dir_all(&criterion_dir, &artifacts.metrics.join("criterion"))?;
    }
    finalize_run(
        &artifacts,
        "bench-core",
        args.common.profile,
        &steps,
        json!({ "kind": "bench-core" }),
        args.common.keep_artifacts,
    )
}

fn run_bench_robust(workspace: &Workspace, args: BenchArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "bench-robust", args.common.profile)?;
    let cohort_sizes = [8usize, 16, 32, 64];
    let attack_samples = [
        AdversarialAttack::Replay,
        AdversarialAttack::FreeRider,
        AdversarialAttack::ModelReplacement,
        AdversarialAttack::BackdoorLowNorm,
    ];
    let mut feature_rows = Vec::new();
    let mut aggregation_rows = Vec::new();
    let mut end_to_end_rows = Vec::new();

    for cohort_size in cohort_sizes {
        let fixture = build_fixture(AdversarialAttack::SignFlip, cohort_size, 0.2);
        let layers = [FeatureLayer::new(
            "update",
            0,
            fixture.reference_update.len(),
        )];
        let feature_started = Instant::now();
        for peer in &fixture.peers {
            let _ = extract_feature_sketch(
                &peer.update,
                Some(&fixture.reference_update),
                &layers,
                RobustnessPolicy::balanced()
                    .screening_policy
                    .sketch_dimensionality as usize,
                0,
                25,
                Some(if peer.malicious { 0.05 } else { 0.0 }),
            );
        }
        let feature_elapsed = feature_started.elapsed();
        let feature_secs = feature_elapsed.as_secs_f64().max(1e-9);
        feature_rows.push(json!({
            "cohort_size": cohort_size,
            "duration_ms": feature_elapsed.as_secs_f64() * 1000.0,
            "updates_per_sec": cohort_size as f64 / feature_secs,
        }));

        let updates = fixture
            .peers
            .iter()
            .map(|peer| peer.update.clone())
            .collect::<Vec<_>>();
        let weights = vec![1.0; updates.len()];
        for (label, strategy) in [
            ("weighted-mean", AggregationStrategy::WeightedMean),
            (
                "clipped-weighted-mean",
                AggregationStrategy::ClippedWeightedMean,
            ),
            ("trimmed-mean", AggregationStrategy::TrimmedMean),
            ("median", AggregationStrategy::Median),
        ] {
            let mut policy = RobustnessPolicy::balanced();
            policy.aggregation_policy.strategy = strategy;
            let started = Instant::now();
            let _ = aggregate_updates_with_policy(&policy, &updates, &weights);
            let elapsed = started.elapsed();
            aggregation_rows.push(json!({
                "cohort_size": cohort_size,
                "strategy": label,
                "duration_ms": elapsed.as_secs_f64() * 1000.0,
                "updates_per_sec": cohort_size as f64 / elapsed.as_secs_f64().max(1e-9),
            }));
        }

        for attack in attack_samples.clone() {
            for (preset, policy) in [
                ("balanced", RobustnessPolicy::balanced()),
                ("strict", RobustnessPolicy::strict()),
            ] {
                let started = Instant::now();
                let report = run_scenario(policy, attack.clone(), cohort_size, 0.2);
                let elapsed = started.elapsed();
                end_to_end_rows.push(json!({
                    "cohort_size": cohort_size,
                    "attack": format!("{attack:?}"),
                    "preset": preset,
                    "duration_ms": elapsed.as_secs_f64() * 1000.0,
                    "attack_success_rate": report.attack_success_rate,
                    "malicious_update_acceptance_rate": report.malicious_update_acceptance_rate,
                    "benign_false_rejection_rate": report.benign_false_rejection_rate,
                    "validator_cpu_overhead_ms": report.validator_cpu_overhead_ms,
                    "reducer_cpu_overhead_ms": report.reducer_cpu_overhead_ms,
                }));
            }
        }
    }

    let summary = json!({
        "feature_rows": feature_rows.len(),
        "aggregation_rows": aggregation_rows.len(),
        "end_to_end_rows": end_to_end_rows.len(),
    });
    artifacts.write_json("metrics/feature-extraction.json", &feature_rows)?;
    artifacts.write_json("metrics/aggregation-cost.json", &aggregation_rows)?;
    artifacts.write_json("metrics/end-to-end.json", &end_to_end_rows)?;
    artifacts.write_json("metrics/summary.json", &summary)?;

    finalize_run(
        &artifacts,
        "bench-robust",
        args.common.profile,
        &[],
        json!({
            "kind": "bench-robust",
            "summary": summary,
        }),
        args.common.keep_artifacts,
    )
}

fn run_bench_nightly(workspace: &Workspace, args: BenchArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "bench-nightly", args.common.profile)?;
    let envs = BTreeMap::new();
    let steps = vec![workspace.run_cargo(
        &artifacts,
        "bench-nightly",
        &[
            "bench",
            "-p",
            "burn_p2p_testkit",
            "--bench",
            "merge_topology",
            "--bench",
            "browser_path",
        ],
        &envs,
    )?];
    let criterion_dir = workspace.root.join("target/criterion");
    if criterion_dir.exists() {
        copy_dir_all(&criterion_dir, &artifacts.metrics.join("criterion"))?;
    }
    finalize_run(
        &artifacts,
        "bench-nightly",
        args.common.profile,
        &steps,
        json!({ "kind": "bench-nightly" }),
        args.common.keep_artifacts,
    )
}

fn run_ci_pr_fast(workspace: &Workspace, args: CiArgs) -> anyhow::Result<()> {
    run_ci_lane(
        workspace,
        "ci-pr-fast",
        Profile::CiPr,
        args.keep_artifacts,
        &[
            (
                "check",
                vec!["check", "--profile", "ci-pr", "--keep-artifacts"],
            ),
            (
                "e2e-smoke",
                vec!["e2e", "smoke", "--profile", "ci-pr", "--keep-artifacts"],
            ),
            (
                "adversarial-smoke",
                vec![
                    "adversarial",
                    "smoke",
                    "--profile",
                    "ci-pr",
                    "--keep-artifacts",
                ],
            ),
        ],
    )
}

fn run_ci_browser(workspace: &Workspace, args: CiArgs) -> anyhow::Result<()> {
    run_ci_lane(
        workspace,
        "ci-browser",
        Profile::CiPr,
        args.keep_artifacts,
        &[
            ("setup-browser", vec!["setup", "browser"]),
            (
                "browser-smoke",
                vec!["browser", "smoke", "--profile", "ci-pr", "--keep-artifacts"],
            ),
        ],
    )
}

fn run_ci_integration(workspace: &Workspace, args: CiArgs) -> anyhow::Result<()> {
    run_ci_lane(
        workspace,
        "ci-integration",
        Profile::CiIntegration,
        args.keep_artifacts,
        &[
            ("setup-browser", vec!["setup", "browser"]),
            (
                "e2e-mixed",
                vec![
                    "e2e",
                    "mixed",
                    "--profile",
                    "ci-integration",
                    "--keep-artifacts",
                ],
            ),
            (
                "e2e-mnist",
                vec![
                    "e2e",
                    "mnist",
                    "--profile",
                    "ci-integration",
                    "--keep-artifacts",
                ],
            ),
            (
                "stress-multiprocess",
                vec![
                    "stress",
                    "multiprocess",
                    "--profile",
                    "ci-integration",
                    "--peers",
                    "8",
                    "--duration",
                    "90s",
                    "--keep-artifacts",
                ],
            ),
        ],
    )
}

fn run_ci_services(workspace: &Workspace, args: CiArgs) -> anyhow::Result<()> {
    run_ci_lane(
        workspace,
        "ci-services",
        Profile::CiPr,
        args.keep_artifacts,
        &[(
            "e2e-services",
            vec!["e2e", "services", "--profile", "ci-pr", "--keep-artifacts"],
        )],
    )
}

fn run_ci_nightly(workspace: &Workspace, args: CiArgs) -> anyhow::Result<()> {
    run_ci_lane(
        workspace,
        "ci-nightly",
        Profile::Nightly,
        args.keep_artifacts,
        &[
            ("setup-browser", vec!["setup", "browser"]),
            (
                "stress-chaos",
                vec![
                    "stress",
                    "chaos",
                    "--profile",
                    "nightly",
                    "--events",
                    "12",
                    "--peers",
                    "12",
                    "--keep-artifacts",
                ],
            ),
            (
                "adversarial-matrix",
                vec![
                    "adversarial",
                    "matrix",
                    "--profile",
                    "nightly",
                    "--keep-artifacts",
                ],
            ),
            (
                "adversarial-chaos",
                vec![
                    "adversarial",
                    "chaos",
                    "--profile",
                    "nightly",
                    "--events",
                    "12",
                    "--peers",
                    "12",
                    "--keep-artifacts",
                ],
            ),
            (
                "stress-multiprocess",
                vec![
                    "stress",
                    "multiprocess",
                    "--profile",
                    "nightly",
                    "--peers",
                    "16",
                    "--duration",
                    "5m",
                    "--keep-artifacts",
                ],
            ),
            (
                "browser-smoke",
                vec![
                    "browser",
                    "smoke",
                    "--profile",
                    "nightly",
                    "--keep-artifacts",
                ],
            ),
            (
                "bench-nightly",
                vec![
                    "bench",
                    "robust",
                    "--profile",
                    "nightly",
                    "--keep-artifacts",
                ],
            ),
            (
                "bench-nightly",
                vec![
                    "bench",
                    "nightly",
                    "--profile",
                    "nightly",
                    "--keep-artifacts",
                ],
            ),
        ],
    )
}

fn run_ci_publish(workspace: &Workspace, args: CiArgs) -> anyhow::Result<()> {
    run_ci_lane(
        workspace,
        "ci-publish",
        Profile::CiIntegration,
        args.keep_artifacts,
        &[(
            "check-publish",
            vec![
                "check",
                "publish",
                "--profile",
                "ci-integration",
                "--keep-artifacts",
            ],
        )],
    )
}

fn run_ci_lane(
    workspace: &Workspace,
    suite: &str,
    profile: Profile,
    keep_artifacts: bool,
    commands: &[(&str, Vec<&str>)],
) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, suite, profile)?;
    let envs = BTreeMap::new();
    let xtask_bin = env::current_exe().context("failed to locate xtask executable")?;
    let xtask_program = xtask_bin.to_string_lossy().into_owned();
    let mut steps = Vec::new();
    let mut nested_artifacts = Vec::new();

    for (label, args) in commands {
        let command_args = args
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        let step = workspace.run(&artifacts, label, &xtask_program, &command_args, &envs)?;
        if let Some(nested_artifact_dir) =
            extract_nested_artifact_dir(&artifacts, &step.stdout_path)?
        {
            nested_artifacts.push(json!({
                "label": label,
                "artifact_dir": nested_artifact_dir,
            }));
        }
        steps.push(step);
    }

    finalize_run(
        &artifacts,
        suite,
        profile,
        &steps,
        json!({
            "kind": "ci-lane",
            "commands": commands
                .iter()
                .map(|(label, args)| json!({ "label": label, "args": args }))
                .collect::<Vec<_>>(),
            "nested_artifacts": nested_artifacts,
        }),
        keep_artifacts,
    )
}

fn extract_nested_artifact_dir(
    artifacts: &ArtifactLayout,
    stdout_path: &str,
) -> anyhow::Result<Option<String>> {
    let stdout_log = artifacts.root.join(stdout_path);
    if !stdout_log.exists() {
        return Ok(None);
    }
    let stdout = fs::read_to_string(stdout_log)?;
    Ok(stdout
        .lines()
        .rev()
        .find_map(|line| line.strip_prefix("Artifacts: "))
        .map(str::to_owned))
}

fn finalize_run(
    artifacts: &ArtifactLayout,
    suite: &str,
    profile: Profile,
    steps: &[StepRecord],
    extra: serde_json::Value,
    keep_artifacts: bool,
) -> anyhow::Result<()> {
    let total_duration_ms = steps.iter().map(|step| step.duration_ms).sum::<u128>();
    let summary = json!({
        "suite": suite,
        "profile": profile.label(),
        "success": true,
        "artifact_dir": artifacts.root.display().to_string(),
        "duration_ms": total_duration_ms,
        "steps": steps,
        "extra": extra,
    });
    artifacts.write_json("summary.json", &summary)?;
    artifacts.write_text(
        "summary.md",
        format!(
            "# {suite}\n\n- profile: {}\n- success: true\n- artifact_dir: {}\n- duration_ms: {total_duration_ms}\n",
            profile.label(),
            artifacts.root.display()
        ),
    )?;
    println!("Artifacts: {}", artifacts.root.display());
    if !keep_artifacts && !profile.settings().keep_artifacts_on_success {
        println!("Kept local artifacts at {}", artifacts.root.display());
    }
    Ok(())
}

fn summarize_adversarial_reports(reports: &[AdversarialScenarioReport]) -> serde_json::Value {
    let report_count = reports.len().max(1) as f64;
    let attack_success_mean = reports
        .iter()
        .map(|report| report.attack_success_rate)
        .sum::<f64>()
        / report_count;
    let malicious_acceptance_mean = reports
        .iter()
        .map(|report| report.malicious_update_acceptance_rate)
        .sum::<f64>()
        / report_count;
    let benign_false_rejection_mean = reports
        .iter()
        .map(|report| report.benign_false_rejection_rate)
        .sum::<f64>()
        / report_count;
    let quarantine_precision_mean = reports
        .iter()
        .map(|report| report.quarantine_precision)
        .sum::<f64>()
        / report_count;
    let quarantine_recall_mean = reports
        .iter()
        .map(|report| report.quarantine_recall)
        .sum::<f64>()
        / report_count;
    let max_attack_success = reports
        .iter()
        .map(|report| report.attack_success_rate)
        .fold(0.0_f64, f64::max);
    let max_benign_false_rejection = reports
        .iter()
        .map(|report| report.benign_false_rejection_rate)
        .fold(0.0_f64, f64::max);

    json!({
        "report_count": reports.len(),
        "attack_success_mean": attack_success_mean,
        "malicious_acceptance_mean": malicious_acceptance_mean,
        "benign_false_rejection_mean": benign_false_rejection_mean,
        "quarantine_precision_mean": quarantine_precision_mean,
        "quarantine_recall_mean": quarantine_recall_mean,
        "max_attack_success": max_attack_success,
        "max_benign_false_rejection": max_benign_false_rejection,
    })
}

fn probe_command(program: &str, args: &[&str]) -> Probe {
    match std::process::Command::new(program).args(args).output() {
        Ok(output) if output.status.success() => Probe {
            ok: true,
            output: Some(String::from_utf8_lossy(&output.stdout).trim().to_owned()),
        },
        Ok(output) => Probe {
            ok: false,
            output: Some(String::from_utf8_lossy(&output.stderr).trim().to_owned()),
        },
        Err(error) => Probe {
            ok: false,
            output: Some(error.to_string()),
        },
    }
}

#[derive(Debug)]
struct Probe {
    ok: bool,
    output: Option<String>,
}

fn print_probe(label: &str, probe: &Probe, fix: &str) {
    println!(
        "{} {}: {}",
        if probe.ok { "GREEN" } else { "RED" },
        label,
        probe.output.as_deref().unwrap_or("no output")
    );
    if !probe.ok {
        println!("  next: {fix}");
    }
}

fn print_optional(label: &str, probe: &Probe) {
    println!(
        "{} {}: {}",
        if probe.ok { "GREEN" } else { "YELLOW" },
        label,
        probe.output.as_deref().unwrap_or("not available")
    );
}

fn reserve_ports(count: usize) -> anyhow::Result<Vec<u16>> {
    let mut ports = Vec::with_capacity(count);
    let mut listeners = Vec::with_capacity(count);
    for _ in 0..count {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        ports.push(listener.local_addr()?.port());
        listeners.push(listener);
    }
    drop(listeners);
    Ok(ports)
}

fn playwright_package_available() -> bool {
    if command_available("playwright") {
        return true;
    }
    let npx_root = env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(".npm/_npx"))
        .filter(|path| path.exists());
    npx_root
        .and_then(|root| fs::read_dir(root).ok())
        .map(|entries| {
            entries.filter_map(Result::ok).any(|entry| {
                entry
                    .path()
                    .join("node_modules/playwright/index.mjs")
                    .exists()
            })
        })
        .unwrap_or(false)
}

fn resolve_chrome_path() -> Option<PathBuf> {
    resolve_binary(
        "BURN_P2P_PLAYWRIGHT_CHROME",
        &[
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        ],
    )
}

fn resolve_firefox_path() -> Option<PathBuf> {
    resolve_binary(
        "BURN_P2P_FIREFOX_BIN",
        &[
            "/usr/bin/firefox",
            "/Applications/Firefox.app/Contents/MacOS/firefox",
        ],
    )
}

fn resolve_binary(env_var: &str, candidates: &[&str]) -> Option<PathBuf> {
    env::var_os(env_var)
        .map(PathBuf::from)
        .filter(|path| path.exists())
        .or_else(|| {
            candidates
                .iter()
                .map(PathBuf::from)
                .find(|path| path.exists())
        })
}

fn allow_dirty_packages() -> bool {
    env::var("ALLOW_DIRTY")
        .ok()
        .map(|value| value == "1")
        .unwrap_or_else(|| env::var_os("CI").is_none())
}

fn parse_duration_string(value: &str) -> anyhow::Result<Duration> {
    let trimmed = value.trim();
    if let Some(seconds) = trimmed.strip_suffix('s') {
        return Ok(Duration::from_secs(seconds.parse()?));
    }
    if let Some(minutes) = trimmed.strip_suffix('m') {
        return Ok(Duration::from_secs(minutes.parse::<u64>()? * 60));
    }
    if let Some(hours) = trimmed.strip_suffix('h') {
        return Ok(Duration::from_secs(hours.parse::<u64>()? * 60 * 60));
    }
    Ok(Duration::from_secs(trimmed.parse()?))
}

fn seed_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn generate_chaos_events(seed: u64, count: u32, peer_count: u32) -> Vec<ChaosEvent> {
    let faults = [
        FaultType::PeerChurn,
        FaultType::Partition,
        FaultType::SlowPeer,
        FaultType::RelayLoss,
        FaultType::StaleHead,
    ];
    let mut state = seed;
    (0..count)
        .map(|index| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let fault = faults[(state as usize) % faults.len()].clone();
            let peer_ix = (state.rotate_left(7) as u32) % peer_count.max(1);
            ChaosEvent {
                window_id: WindowId((index + 1).into()),
                fault,
                peer_id: Some(PeerId::new(format!("peer-{peer_ix}"))),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{generate_chaos_events, parse_duration_string};

    #[test]
    fn duration_parser_supports_suffixes() {
        assert_eq!(
            parse_duration_string("90s").expect("90s"),
            std::time::Duration::from_secs(90)
        );
        assert_eq!(
            parse_duration_string("2m").expect("2m"),
            std::time::Duration::from_secs(120)
        );
    }

    #[test]
    fn chaos_generation_is_replayable() {
        let first = generate_chaos_events(42, 4, 6);
        let second = generate_chaos_events(42, 4, 6);
        assert_eq!(first, second);
    }
}
