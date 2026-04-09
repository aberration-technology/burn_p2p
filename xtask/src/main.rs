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
use burn_p2p_bootstrap::BootstrapPreset;
use burn_p2p_core::{AggregationStrategy, RobustnessPolicy};
use burn_p2p_metrics::MetricsCatchupBundle;
use burn_p2p_security::{FeatureLayer, aggregate_updates_with_policy, extract_feature_sketch};
use burn_p2p_testkit::{
    ChaosEvent, FaultType, MaliciousBehavior, SimulationRunner, SimulationSpec,
    adversarial::{
        AdversarialAttack, AdversarialScenarioReport, build_fixture, run_attack_matrix,
        run_scenario,
    },
    browser_app_assets::build_browser_app_web_assets,
    multiprocess::{
        SyntheticNativeBackend, SyntheticSoakConfig, SyntheticWorkloadKind,
        derive_synthetic_network_dynamics_summary, derive_synthetic_soak_performance_summary,
        run_synthetic_process_soak,
    },
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
    DeployCommand, DeployComposeArgs, E2eCommand, MultiprocessArgs, RunArgs, SetupCommand,
    StressCommand,
};
use profile::Profile;
use runner::{SpawnedStep, StepRecord, Workspace, command_available};
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

#[derive(Debug, Deserialize, serde::Serialize)]
struct NativeAcceleratorProbeSummary {
    status: String,
    requested_backend: String,
    resolved_backend: Option<String>,
    matrix_size: usize,
    warmup_iterations: u32,
    measured_iterations: u32,
    elapsed_ms: u128,
    iterations_per_sec: Option<f64>,
    failure_reason: Option<String>,
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
            Some(CheckSubcommand::AuthOidcLive(args)) => {
                run_auth_oidc_live_check(&workspace, args.common)
            }
            Some(CheckSubcommand::AuthRedisLive(args)) => {
                run_auth_redis_live_check(&workspace, args.common)
            }
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
            StressCommand::Discovery(args) => run_stress_discovery(&workspace, args),
        },
        Command::Bench { command } => match command {
            BenchCommand::Core(args) => run_bench_core(&workspace, args),
            BenchCommand::Network(args) => run_bench_network(&workspace, args),
            BenchCommand::Accelerator(args) => run_bench_accelerator(&workspace, args),
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
    if let Some(image) = args
        .reducer_image
        .as_ref()
        .or(args.validator_image.as_ref())
    {
        envs.insert("TF_VAR_reducer_image".into(), image.clone());
    }
    if let Some(image) = args.trainer_image.as_ref() {
        envs.insert("TF_VAR_trainer_image".into(), image.clone());
    }
    let default_bootstrap_config = workspace.root.join("deploy/config/trusted-browser.json");
    let default_validator_config = workspace
        .root
        .join("deploy/config/reference-validator.json");
    let default_reducer_config = workspace.root.join("deploy/config/reference-reducer.json");
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
    let reducer_config_path = args
        .reducer_config
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or(default_reducer_config);
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
    envs.insert(
        "TF_VAR_reducer_config_json".into(),
        fs::read_to_string(&reducer_config_path)
            .with_context(|| format!("failed to read {}", reducer_config_path.display()))?,
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
            "reducer_image": args.reducer_image.as_ref().or(args.validator_image.as_ref()),
            "trainer_image": args.trainer_image,
            "bootstrap_config": bootstrap_config_path,
            "validator_config": validator_config_path,
            "reducer_config": reducer_config_path,
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

const REAL_OIDC_REQUIRED_ENV_VARS: &[&str] = &[
    "BURN_P2P_REAL_OIDC_ISSUER",
    "BURN_P2P_REAL_OIDC_CLIENT_ID",
    "BURN_P2P_REAL_OIDC_ID_TOKEN",
    "BURN_P2P_REAL_OIDC_EXPECTED_SUBJECT",
];

fn missing_real_oidc_env_vars() -> Vec<&'static str> {
    REAL_OIDC_REQUIRED_ENV_VARS
        .iter()
        .copied()
        .filter(|key| {
            env::var(key)
                .ok()
                .is_none_or(|value| value.trim().is_empty())
        })
        .collect()
}

fn collect_real_oidc_env() -> BTreeMap<String, String> {
    env::vars()
        .filter(|(key, _)| key.starts_with("BURN_P2P_REAL_OIDC_"))
        .collect()
}

fn run_auth_oidc_live_check(workspace: &Workspace, args: CommonArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "check-auth-oidc-live", args.profile)?;
    let missing_env = missing_real_oidc_env_vars();
    if !missing_env.is_empty() {
        return finalize_run(
            &artifacts,
            "check-auth-oidc-live",
            args.profile,
            &[],
            json!({
                "kind": "auth-oidc-live",
                "skipped": true,
                "reason": "missing_env",
                "missing_env": missing_env,
            }),
            args.keep_artifacts,
        );
    }

    let envs = collect_real_oidc_env();
    let steps = vec![workspace.run_cargo(
        &artifacts,
        "live-oidc-provider",
        &[
            "test",
            "-p",
            "burn_p2p_auth_external",
            "live_oidc_connector_validates_real_provider_identity_from_env",
            "--",
            "--ignored",
            "--exact",
            "--nocapture",
        ],
        &envs,
    )?];

    finalize_run(
        &artifacts,
        "check-auth-oidc-live",
        args.profile,
        &steps,
        json!({
            "kind": "auth-oidc-live",
            "skipped": false,
            "env_prefix": "BURN_P2P_REAL_OIDC_",
            "required_env": REAL_OIDC_REQUIRED_ENV_VARS,
            "passed_env": envs.keys().cloned().collect::<Vec<_>>(),
        }),
        args.keep_artifacts,
    )
}

fn run_auth_redis_live_check(workspace: &Workspace, args: CommonArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "check-auth-redis-live", args.profile)?;
    if !command_available("redis-server") {
        return finalize_run(
            &artifacts,
            "check-auth-redis-live",
            args.profile,
            &[],
            json!({
                "kind": "auth-redis-live",
                "skipped": true,
                "reason": "missing_redis_server",
            }),
            args.keep_artifacts,
        );
    }

    let steps = vec![workspace.run_cargo(
        &artifacts,
        "live-redis-auth",
        &[
            "test",
            "-p",
            "burn_p2p_bootstrap",
            "--features",
            "browser-edge,auth-github,auth-oidc,auth-oauth,auth-external",
            "auth_portal_redis_",
            "--",
            "--nocapture",
        ],
        &BTreeMap::new(),
    )?];

    finalize_run(
        &artifacts,
        "check-auth-redis-live",
        args.profile,
        &steps,
        json!({
            "kind": "auth-redis-live",
            "skipped": false,
            "required_binary": "redis-server",
            "test_filter": "auth_portal_redis_",
        }),
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

fn wait_for_path_from_step(
    path: &std::path::Path,
    timeout: Duration,
    step: &mut SpawnedStep,
    artifacts: &ArtifactLayout,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return Ok(());
        }
        if let Some(record) = step.try_wait(artifacts)? {
            anyhow::bail!(
                "step `{}` exited before producing {}",
                record.label,
                path.display(),
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    anyhow::bail!("timed out waiting for {}", path.display());
}

fn run_e2e_mnist(workspace: &Workspace, args: CommonArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "e2e-mnist", args.profile)?;
    let envs = BTreeMap::new();
    let demo_root = artifacts.root.join("mnist-demo");
    let trainer_windows = args.profile.settings().trainer_windows.max(1);
    let total_training_rounds = u64::from(trainer_windows)
        .saturating_mul(2)
        .saturating_add(1);
    // The live-browser handoff is written after the baseline, restart, and low-lr rounds.
    // Use a round-scaled timeout here so a slow but healthy demo does not get aborted
    // before it reaches the post-training browser handoff stage.
    let browser_manifest_timeout = Duration::from_secs(150 + total_training_rounds * 90);
    let mut steps = Vec::new();
    let browser_probe_root = artifacts.root.join("browser-wasm-probe");
    let browser_probe_assets = browser_probe_root.join("assets");
    let browser_manifest_path = demo_root.join("browser-live.json");
    let browser_edge_config_path = demo_root.join("browser-live-edge-config.json");
    let browser_edge_stop_path = demo_root.join("browser-live-edge.stop");
    steps.push(workspace.run_cargo(
        &artifacts,
        "mnist-topology-dedicated-reducer",
        &[
            "test",
            "-p",
            "burn_p2p",
            "--features",
            "burn",
            "dedicated_reducer_publishes_proposal_and_validators_only_attest_and_promote",
            "--",
            "--exact",
            "--nocapture",
        ],
        &envs,
    )?);
    steps.push(workspace.run_cargo(
        &artifacts,
        "mnist-topology-seed-fanout",
        &[
            "test",
            "-p",
            "burn_p2p",
            "--features",
            "burn",
            "peers_fan_out_beyond_bootstrap_seed_and_survive_seed_shutdown",
            "--",
            "--exact",
            "--nocapture",
        ],
        &envs,
    )?);
    steps.push(workspace.run_cargo(
        &artifacts,
        "mnist-topology-relay-reservation",
        &[
            "test",
            "-p",
            "burn_p2p_swarm",
            "native_control_plane_shell_reserves_and_dials_relay_paths",
            "--",
            "--exact",
            "--nocapture",
        ],
        &envs,
    )?);
    steps.push(workspace.run_cargo(
        &artifacts,
        "mnist-topology-rendezvous-discovery",
        &[
            "test",
            "-p",
            "burn_p2p_swarm",
            "native_control_plane_shell_discovers_peers_via_rendezvous_seed",
            "--",
            "--exact",
            "--nocapture",
        ],
        &envs,
    )?);
    steps.push(workspace.run_cargo(
        &artifacts,
        "mnist-topology-kademlia-discovery",
        &[
            "test",
            "-p",
            "burn_p2p_swarm",
            "native_control_plane_shell_discovers_peers_via_kademlia_seed",
            "--",
            "--exact",
            "--nocapture",
        ],
        &envs,
    )?);
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
    steps.push(workspace.run_cargo(
        &artifacts,
        "mnist-demo-build",
        &[
            "build",
            "--manifest-path",
            "examples/mnist_p2p_demo/Cargo.toml",
            "--bin",
            "mnist_p2p_demo",
            "--bin",
            "mnist_browser_live_edge",
            "--bin",
            "mnist_browser_probe_finalize",
        ],
        &envs,
    )?);
    let demo_binary = workspace
        .root
        .join("examples/mnist_p2p_demo/target/debug/mnist_p2p_demo");
    let browser_edge_binary = workspace
        .root
        .join("examples/mnist_p2p_demo/target/debug/mnist_browser_live_edge");
    let browser_probe_finalize_binary = workspace
        .root
        .join("examples/mnist_p2p_demo/target/debug/mnist_browser_probe_finalize");
    let demo_args = vec![
        "--output".to_owned(),
        demo_root.display().to_string(),
        "--baseline-rounds".to_owned(),
        trainer_windows.to_string(),
        "--low-lr-rounds".to_owned(),
        trainer_windows.to_string(),
        "--live-browser-probe".to_owned(),
    ];
    let mut demo_process = Some(
        workspace.spawn(
            &artifacts,
            "mnist-demo",
            demo_binary
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("mnist demo binary path was not valid utf-8"))?,
            &demo_args,
            &envs,
        )?,
    );
    let browser_probe_summary = (|| -> anyhow::Result<serde_json::Value> {
        let mut browser_edge_process: Option<SpawnedStep> = None;
        let result = (|| -> anyhow::Result<serde_json::Value> {
            wait_for_path_from_step(
                &browser_edge_config_path,
                browser_manifest_timeout,
                demo_process
                    .as_mut()
                    .expect("mnist demo process should remain active until awaited"),
                &artifacts,
            )?;
            steps.push(
                demo_process
                    .take()
                    .expect("mnist demo process should remain present before wait")
                    .wait(&artifacts)?,
            );
            browser_edge_process = Some(workspace.spawn(
                &artifacts,
                "mnist-browser-live-edge",
                browser_edge_binary.to_str().ok_or_else(|| {
                    anyhow::anyhow!("mnist browser live edge binary path was not valid utf-8")
                })?,
                &[
                    "--config".to_owned(),
                    browser_edge_config_path.display().to_string(),
                    "--output-root".to_owned(),
                    demo_root.display().to_string(),
                    "--stop-file".to_owned(),
                    browser_edge_stop_path.display().to_string(),
                ],
                &envs,
            )?);
            wait_for_path_from_step(
                &browser_manifest_path,
                browser_manifest_timeout,
                browser_edge_process
                    .as_mut()
                    .expect("browser edge process should be present after spawn"),
                &artifacts,
            )?;
            let browser_manifest: serde_json::Value =
                serde_json::from_slice(&fs::read(&browser_manifest_path).with_context(|| {
                    format!("failed to read {}", browser_manifest_path.display())
                })?)
                .with_context(|| format!("failed to decode {}", browser_manifest_path.display()))?;
            let default_dataset_root = demo_root.join("dataset").display().to_string();
            let peer_dataset_root = browser_manifest
                .get("peer_dataset_root")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let peer_head_artifact_root = browser_manifest
                .get("peer_head_artifact_root")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let browser_probe_config = json!({
                "asset_root": browser_probe_assets.display().to_string(),
                "dataset_root": peer_dataset_root
                    .as_str()
                    .unwrap_or(&default_dataset_root),
                "peer_dataset_root": peer_dataset_root,
                "peer_head_artifact_root": peer_head_artifact_root,
                "artifact_root": browser_probe_root.display().to_string(),
                "dataset_base_url": browser_manifest
                    .get("peer_dataset_root")
                    .and_then(serde_json::Value::as_str)
                    .map(|_| serde_json::Value::Null)
                    .unwrap_or_else(|| {
                        browser_manifest
                            .get("dataset_base_url")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null)
                    }),
                "browser_dataset_transport": if browser_manifest
                    .get("peer_dataset_root")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
                {
                    serde_json::Value::String("p2p-signed-peer-bundle".into())
                } else {
                    browser_manifest
                        .get("browser_dataset_transport")
                        .cloned()
                        .unwrap_or_else(|| serde_json::Value::String("http".into()))
                },
                "browser_head_artifact_transport": browser_manifest
                    .get("browser_head_artifact_transport")
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::String("unknown".into())),
                "shards_distributed_over_p2p": browser_manifest
                    .get("shards_distributed_over_p2p")
                    .and_then(serde_json::Value::as_bool)
                    .context("browser live manifest missing shards_distributed_over_p2p")?,
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
                serde_json::from_slice(&fs::read(&browser_probe_summary_path).with_context(
                    || format!("failed to read {}", browser_probe_summary_path.display()),
                )?)
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
            fs::write(&browser_edge_stop_path, b"stop\n")
                .with_context(|| format!("failed to write {}", browser_edge_stop_path.display()))?;
            steps.push(
                browser_edge_process
                    .take()
                    .expect("browser edge process should be present before wait")
                    .wait(&artifacts)?,
            );
            steps.push(
                workspace.run(
                    &artifacts,
                    "mnist-browser-probe-finalize",
                    browser_probe_finalize_binary.to_str().ok_or_else(|| {
                        anyhow::anyhow!(
                            "mnist browser probe finalize binary path was not valid utf-8"
                        )
                    })?,
                    &[
                        "--export".to_owned(),
                        demo_root.join("browser-export.json").display().to_string(),
                        "--correctness".to_owned(),
                        demo_root.join("correctness.json").display().to_string(),
                        "--manifest".to_owned(),
                        browser_manifest_path.display().to_string(),
                        "--probe".to_owned(),
                        demo_root
                            .join("browser-probe-result.json")
                            .display()
                            .to_string(),
                    ],
                    &envs,
                )?,
            );
            Ok(browser_probe_summary)
        })();
        if result.is_err()
            && let Some(mut browser_edge_process) = browser_edge_process
        {
            let _ = fs::write(&browser_edge_stop_path, b"stop\n");
            let _ = browser_edge_process.kill();
            let _ = browser_edge_process.wait(&artifacts);
        }
        result
    })();
    let browser_probe_summary = match browser_probe_summary {
        Ok(summary) => summary,
        Err(error) => {
            if let Some(mut demo_process) = demo_process.take() {
                let _ = demo_process.kill();
                let _ = demo_process.wait(&artifacts);
            }
            return Err(error);
        }
    };

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
            .pointer("/topology/all_non_seed_nodes_bootstrap_via_seed")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo did not route all non-seed nodes through the helper seed",
    );
    ensure!(
        correctness
            .pointer("/topology/mesh_fanout_beyond_seed_observed")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo did not show steady-state fanout beyond the helper seed",
    );
    ensure!(
        correctness
            .pointer("/topology/dedicated_reducer_participated")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo did not exercise a dedicated reducer node",
    );
    ensure!(
        correctness
            .pointer("/topology/aggregate_proposals_only_from_dedicated_reducer")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo aggregate proposals were not isolated to the dedicated reducer",
    );
    ensure!(
        correctness
            .pointer("/topology/reducer_load_only_from_dedicated_reducer")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo observed reducer-load telemetry from non-reducer peers",
    );
    ensure!(
        correctness
            .pointer("/topology/reduction_attestations_only_from_validators")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo observed reduction attestations from non-validator peers",
    );
    ensure!(
        correctness
            .pointer("/topology/merge_certificates_only_from_validators")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo observed merge certificates from non-validator peers",
    );
    ensure!(
        correctness
            .pointer("/topology/validators_observed_validation_quorum")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo validators did not converge on visible validation quorum records",
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
            == Some(true),
        "mnist demo browser dataset probe did not report shard transport over the p2p overlay",
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
            .pointer("/device_limits/native_limit_profiles_present")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo did not export native limit profiles for the active nodes",
    );
    ensure!(
        correctness
            .pointer("/device_limits/native_trainer_backend_visible")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo did not surface a native trainer backend preference",
    );
    ensure!(
        correctness
            .pointer("/device_limits/browser/webgpu_backend_confirmed")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo browser probe did not confirm live burn/webgpu execution with gpu support",
    );
    ensure!(
        correctness
            .pointer("/device_limits/browser/budget_within_capability_limits")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo browser training budget exceeded the exported browser capability limits",
    );
    ensure!(
        correctness
            .pointer("/assessment/split_topology_roles_exercised")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist demo did not prove the split seed/reducer/validator topology",
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
            .pointer("/browser_execution/transport")
            .and_then(serde_json::Value::as_str)
            == Some("webrtc-direct"),
        "mnist correctness summary did not keep the browser worker on the direct peer transport when it was advertised",
    );
    ensure!(
        correctness
            .pointer("/assessment/live_browser_training")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
        "mnist correctness summary did not report live browser training",
    );
    ensure!(
        correctness
            .pointer("/browser_dataset_access/upstream_mode")
            .and_then(serde_json::Value::as_str)
            == Some("p2p-signed-peer-bundle"),
        "mnist correctness summary did not switch browser dataset bytes onto the peer lease bundle path",
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
        workload_kind: SyntheticWorkloadKind::Scalar,
        trainer_backend: SyntheticNativeBackend::NdArray,
        validator_backend: SyntheticNativeBackend::NdArray,
        trainer_count,
        trainer_window_count: window_count,
        persistent_trainers: false,
        continuous_training: false,
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
    if let Some(performance_summary) = summary.performance_summary.as_ref() {
        artifacts.write_json("metrics/performance-summary.json", performance_summary)?;
    }
    if let Some(dynamics_summary) = summary.dynamics_summary.as_ref() {
        artifacts.write_json("metrics/network-dynamics-summary.json", dynamics_summary)?;
    }

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

fn summarize_discovery_dynamics(
    spec: &SimulationSpec,
    outcome: &burn_p2p_testkit::SimulationOutcome,
    duration_ms: u128,
) -> serde_json::Value {
    let merge_count = outcome
        .windows
        .iter()
        .filter(|window| window.merge_certificate.is_some())
        .count();
    let accepted_receipt_count = outcome
        .windows
        .iter()
        .map(|window| window.accepted_receipts.len())
        .sum::<usize>();
    let rejected_update_count = outcome
        .windows
        .iter()
        .map(|window| window.rejected_updates.len())
        .sum::<usize>();
    let browser_peer_ids = outcome
        .peer_fixtures
        .iter()
        .filter(|fixture| fixture.mode == burn_p2p_testkit::PeerFixtureMode::HonestBrowser)
        .map(|fixture| fixture.peer_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let browser_receipts_per_window = outcome
        .windows
        .iter()
        .map(|window| {
            window
                .accepted_receipts
                .iter()
                .filter(|receipt| browser_peer_ids.contains(&receipt.peer_id))
                .count()
        })
        .collect::<Vec<_>>();
    let accepted_browser_receipt_count = browser_receipts_per_window.iter().sum::<usize>();
    let windows_with_browser_receipts = browser_receipts_per_window
        .iter()
        .filter(|count| **count > 0)
        .count();
    let windows_with_rejected_updates = outcome
        .windows
        .iter()
        .filter(|window| !window.rejected_updates.is_empty())
        .count();
    let telemetry = outcome
        .windows
        .iter()
        .flat_map(|window| window.telemetry.iter())
        .collect::<Vec<_>>();
    let connected_peers = telemetry
        .iter()
        .map(|summary| summary.network.connected_peers)
        .collect::<Vec<_>>();
    let observed_peers = telemetry
        .iter()
        .map(|summary| summary.network.observed_peers)
        .collect::<Vec<_>>();
    let estimated_network_size = telemetry
        .iter()
        .map(|summary| summary.network.estimated_network_size)
        .collect::<Vec<_>>();
    let mean_connected_peers = if connected_peers.is_empty() {
        0.0
    } else {
        connected_peers
            .iter()
            .map(|value| u64::from(*value))
            .sum::<u64>() as f64
            / connected_peers.len() as f64
    };
    let mean_observed_peers = if observed_peers.is_empty() {
        0.0
    } else {
        observed_peers.iter().sum::<u64>() as f64 / observed_peers.len() as f64
    };
    let mean_estimated_network_size = if estimated_network_size.is_empty() {
        0.0
    } else {
        estimated_network_size.iter().sum::<f64>() / estimated_network_size.len() as f64
    };

    json!({
        "peer_count": spec.peer_count,
        "browser_peer_count": spec.browser_peer_count,
        "malicious_peer_count": spec.malicious_peers.len(),
        "window_count": spec.window_count,
        "telemetry_sample_count": telemetry.len(),
        "duration_ms": duration_ms,
        "merge_count": merge_count,
        "merge_rate_per_sec": if duration_ms == 0 { 0.0 } else { merge_count as f64 / (duration_ms as f64 / 1000.0) },
        "accepted_receipt_count": accepted_receipt_count,
        "accepted_browser_receipt_count": accepted_browser_receipt_count,
        "receipt_rate_per_sec": if duration_ms == 0 { 0.0 } else { accepted_receipt_count as f64 / (duration_ms as f64 / 1000.0) },
        "browser_receipt_window_coverage_ratio": if spec.window_count == 0 { 0.0 } else { windows_with_browser_receipts as f64 / spec.window_count as f64 },
        "rejected_update_count": rejected_update_count,
        "malicious_rejection_window_coverage_ratio": if spec.window_count == 0 { 0.0 } else { windows_with_rejected_updates as f64 / spec.window_count as f64 },
        "connected_peers": {
            "min": connected_peers.iter().copied().min().unwrap_or(0),
            "mean": mean_connected_peers,
            "max": connected_peers.iter().copied().max().unwrap_or(0),
        },
        "observed_peers": {
            "min": observed_peers.iter().copied().min().unwrap_or(0),
            "mean": mean_observed_peers,
            "max": observed_peers.iter().copied().max().unwrap_or(0),
        },
        "estimated_network_size": {
            "min": estimated_network_size.iter().copied().fold(f64::INFINITY, f64::min).is_finite().then_some(
                estimated_network_size.iter().copied().fold(f64::INFINITY, f64::min)
            ).unwrap_or(0.0),
            "mean": mean_estimated_network_size,
            "max": estimated_network_size.iter().copied().fold(0.0_f64, f64::max),
        },
    })
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
    spec.bootstrap_preset = BootstrapPreset::BootstrapOnly;
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
    let merge_count = outcome
        .windows
        .iter()
        .filter(|window| window.merge_certificate.is_some())
        .count();
    let accepted_receipt_count = outcome
        .windows
        .iter()
        .map(|window| window.accepted_receipts.len())
        .sum::<usize>();
    let browser_peer_ids = outcome
        .peer_fixtures
        .iter()
        .filter(|fixture| fixture.mode == burn_p2p_testkit::PeerFixtureMode::HonestBrowser)
        .map(|fixture| fixture.peer_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let accepted_browser_receipt_count = outcome
        .windows
        .iter()
        .flat_map(|window| window.accepted_receipts.iter())
        .filter(|receipt| browser_peer_ids.contains(&receipt.peer_id))
        .count();

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

    ensure!(
        merge_count > 0,
        "chaos simulation did not promote any merged heads"
    );
    ensure!(
        !outcome.heatmap.cells.is_empty(),
        "chaos simulation did not produce any shard-assignment heatmap cells",
    );
    if spec.browser_peer_count > 0 {
        ensure!(
            accepted_browser_receipt_count > 0,
            "chaos simulation did not preserve any browser contribution receipts",
        );
    }

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
            "bootstrap_preset": format!("{:?}", spec.bootstrap_preset),
            "merge_count": merge_count,
            "accepted_receipt_count": accepted_receipt_count,
            "accepted_browser_receipt_count": accepted_browser_receipt_count,
            "rejected_updates_by_peer": rejected_by_peer,
        }),
        args.common.keep_artifacts,
    )
}

fn run_stress_discovery(workspace: &Workspace, args: RunArgs) -> anyhow::Result<()> {
    let artifacts =
        ArtifactLayout::create(&workspace.root, "stress-discovery", args.common.profile)?;
    let envs = BTreeMap::new();
    let steps = vec![
        workspace.run_cargo(
            &artifacts,
            "discovery-relay",
            &[
                "test",
                "-p",
                "burn_p2p_swarm",
                "native_control_plane_shell_reserves_and_dials_relay_paths",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
        workspace.run_cargo(
            &artifacts,
            "discovery-rendezvous",
            &[
                "test",
                "-p",
                "burn_p2p_swarm",
                "native_control_plane_shell_discovers_peers_via_rendezvous_seed",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
        workspace.run_cargo(
            &artifacts,
            "discovery-kademlia",
            &[
                "test",
                "-p",
                "burn_p2p_swarm",
                "native_control_plane_shell_discovers_peers_via_kademlia_seed",
                "--",
                "--exact",
                "--nocapture",
            ],
            &envs,
        )?,
    ];

    let profile_settings = args.common.profile.settings();
    let (peer_count, browser_peer_count, window_count, chaos_event_count) =
        if args.common.profile == Profile::Nightly {
            let peer_count = (profile_settings.multiprocess_peers.max(16) * 5).max(64);
            let browser_peer_count = (peer_count / 4).clamp(4, 12);
            let window_count = profile_settings.trainer_windows.max(6) * 6;
            let chaos_event_count = profile_settings.chaos_events.max(16) * 3;
            (
                peer_count,
                browser_peer_count,
                window_count,
                chaos_event_count,
            )
        } else {
            let peer_count = (profile_settings.multiprocess_peers.max(6) * 2).max(12);
            let browser_peer_count = (peer_count / 4).clamp(1, 4);
            let window_count = profile_settings.trainer_windows.max(3) * 2;
            let chaos_event_count = profile_settings.chaos_events.max(4);
            (
                peer_count,
                browser_peer_count,
                window_count,
                chaos_event_count,
            )
        };
    let mut spec = SimulationSpec {
        peer_count,
        browser_peer_count,
        window_count,
        chaos_events: generate_chaos_events(seed_now(), chaos_event_count, peer_count),
        ..SimulationSpec::default()
    };
    spec.bootstrap_preset = BootstrapPreset::BootstrapOnly;
    spec.malicious_peers = discovery_malicious_peers(peer_count, browser_peer_count);

    let started = Instant::now();
    let outcome = SimulationRunner::default()
        .run(spec.clone())
        .context("discovery simulation failed")?;
    let duration_ms = started.elapsed().as_millis();
    let merge_count = outcome
        .windows
        .iter()
        .filter(|window| window.merge_certificate.is_some())
        .count();
    let rejected_update_count = outcome
        .windows
        .iter()
        .map(|window| window.rejected_updates.len())
        .sum::<usize>();
    let browser_peer_ids = outcome
        .peer_fixtures
        .iter()
        .filter(|fixture| fixture.mode == burn_p2p_testkit::PeerFixtureMode::HonestBrowser)
        .map(|fixture| fixture.peer_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let accepted_browser_receipt_count = outcome
        .windows
        .iter()
        .flat_map(|window| window.accepted_receipts.iter())
        .filter(|receipt| browser_peer_ids.contains(&receipt.peer_id))
        .count();
    let browser_receipts_per_window = outcome
        .windows
        .iter()
        .map(|window| {
            window
                .accepted_receipts
                .iter()
                .filter(|receipt| browser_peer_ids.contains(&receipt.peer_id))
                .count()
        })
        .collect::<Vec<_>>();
    let min_browser_receipts_per_window = browser_receipts_per_window
        .iter()
        .copied()
        .min()
        .unwrap_or(0);
    let max_browser_receipts_per_window = browser_receipts_per_window
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    let windows_with_browser_receipts = browser_receipts_per_window
        .iter()
        .filter(|count| **count > 0)
        .count();
    let windows_with_rejected_updates = outcome
        .windows
        .iter()
        .filter(|window| !window.rejected_updates.is_empty())
        .count();
    let quarantined_peer_count = outcome
        .diagnostics
        .robustness_rollup
        .as_ref()
        .map(|rollup| rollup.quarantined_peer_count)
        .or_else(|| {
            outcome
                .diagnostics
                .robustness_panel
                .as_ref()
                .map(|panel| panel.quarantined_peers.len() as u32)
        })
        .unwrap_or(0);
    let ban_recommended_peer_count = outcome
        .diagnostics
        .robustness_rollup
        .as_ref()
        .map(|rollup| rollup.ban_recommended_peer_count)
        .or_else(|| {
            outcome.diagnostics.robustness_panel.as_ref().map(|panel| {
                panel
                    .quarantined_peers
                    .iter()
                    .filter(|peer| peer.ban_recommended)
                    .count() as u32
            })
        })
        .unwrap_or(0);
    let telemetry = outcome
        .windows
        .iter()
        .flat_map(|window| window.telemetry.iter())
        .collect::<Vec<_>>();
    ensure!(
        !telemetry.is_empty(),
        "discovery soak did not capture any telemetry samples"
    );
    let min_connected_peers = telemetry
        .iter()
        .map(|summary| summary.network.connected_peers)
        .min()
        .unwrap_or(0);
    let min_observed_peer_count = telemetry
        .iter()
        .map(|summary| summary.network.observed_peers)
        .min()
        .unwrap_or(0);
    let min_estimated_network_size = telemetry
        .iter()
        .map(|summary| summary.network.estimated_network_size)
        .fold(f64::INFINITY, f64::min);
    let max_connected_peers = telemetry
        .iter()
        .map(|summary| summary.network.connected_peers)
        .max()
        .unwrap_or(0);
    let max_observed_peer_count = telemetry
        .iter()
        .map(|summary| summary.network.observed_peers)
        .max()
        .unwrap_or(0);
    let max_estimated_network_size = telemetry
        .iter()
        .map(|summary| summary.network.estimated_network_size)
        .fold(0.0_f64, f64::max);

    artifacts.write_json("topology/discovery-outcome.json", &outcome)?;
    artifacts.write_json(
        "metrics/network-dynamics-summary.json",
        &summarize_discovery_dynamics(&spec, &outcome, duration_ms),
    )?;
    artifacts.write_json(
        "topology/discovery-window-rollup.json",
        &json!({
            "windows": outcome
                .windows
                .iter()
                .zip(browser_receipts_per_window.iter())
                .map(|(window, browser_receipts)| json!({
                    "window_id": window.window_id.0,
                    "telemetry_samples": window.telemetry.len(),
                    "accepted_receipts": window.accepted_receipts.len(),
                    "browser_receipts": browser_receipts,
                    "rejected_updates": window.rejected_updates.len(),
                    "merged": window.merge_certificate.is_some(),
                }))
                .collect::<Vec<_>>(),
        }),
    )?;

    ensure!(
        outcome.diagnostics.swarm.connected_peers >= spec.peer_count,
        "discovery soak did not preserve a fully connected synthetic fleet"
    );
    ensure!(
        min_connected_peers >= spec.peer_count,
        "discovery soak dropped below a full mesh in telemetry samples"
    );
    ensure!(
        min_observed_peer_count >= spec.peer_count as u64,
        "discovery soak dropped below full peer visibility in telemetry samples"
    );
    ensure!(
        min_estimated_network_size >= spec.peer_count as f64,
        "discovery soak network estimate regressed below the synthetic fleet size"
    );
    ensure!(
        merge_count >= spec.window_count as usize,
        "discovery soak did not preserve merged heads across every simulated window"
    );
    if !spec.malicious_peers.is_empty() {
        ensure!(
            rejected_update_count > 0,
            "discovery soak did not reject any malicious updates"
        );
        ensure!(
            windows_with_rejected_updates == spec.window_count as usize,
            "discovery soak did not sustain malicious-update rejection across every simulated window"
        );
    }
    ensure!(
        accepted_browser_receipt_count > 0,
        "discovery soak did not preserve browser contributions"
    );
    if spec.browser_peer_count > 0 {
        ensure!(
            windows_with_browser_receipts == spec.window_count as usize,
            "discovery soak did not preserve browser contributions across every simulated window"
        );
    }

    finalize_run(
        &artifacts,
        "stress-discovery",
        args.common.profile,
        &steps,
        json!({
            "kind": "stress-discovery",
            "peer_count": spec.peer_count,
            "browser_peer_count": spec.browser_peer_count,
            "window_count": spec.window_count,
            "malicious_peer_count": spec.malicious_peers.len(),
            "bootstrap_preset": format!("{:?}", spec.bootstrap_preset),
            "duration_ms": duration_ms,
            "merge_count": merge_count,
            "accepted_browser_receipt_count": accepted_browser_receipt_count,
            "min_browser_receipts_per_window": min_browser_receipts_per_window,
            "max_browser_receipts_per_window": max_browser_receipts_per_window,
            "windows_with_browser_receipts": windows_with_browser_receipts,
            "windows_with_rejected_updates": windows_with_rejected_updates,
            "rejected_update_count": rejected_update_count,
            "quarantined_peer_count": quarantined_peer_count,
            "ban_recommended_peer_count": ban_recommended_peer_count,
            "connected_peers": outcome.diagnostics.swarm.connected_peers,
            "observed_peer_count": outcome.diagnostics.swarm.observed_peers.len(),
            "estimated_network_size": outcome.diagnostics.swarm.network_estimate.estimated_network_size,
            "min_connected_peers": min_connected_peers,
            "min_observed_peer_count": min_observed_peer_count,
            "min_estimated_network_size": min_estimated_network_size,
            "max_connected_peers": max_connected_peers,
            "max_observed_peer_count": max_observed_peer_count,
            "max_estimated_network_size": max_estimated_network_size,
            "heatmap_cells": outcome.heatmap.cells.len(),
        }),
        args.common.keep_artifacts,
    )
}

fn discovery_malicious_peers(
    peer_count: u32,
    browser_peer_count: u32,
) -> BTreeMap<PeerId, MaliciousBehavior> {
    let behaviors = [
        MaliciousBehavior::OutOfLeaseWork,
        MaliciousBehavior::WrongBaseHead,
        MaliciousBehavior::NonFiniteMetric,
        MaliciousBehavior::StaleBaseHead,
    ];
    let desired = match peer_count {
        0..=11 => 0,
        12..=23 => 2,
        24..=47 => 3,
        _ => 4,
    };
    (browser_peer_count..peer_count)
        .rev()
        .take(desired.min(behaviors.len()))
        .zip(behaviors)
        .map(|(index, behavior)| (PeerId::new(format!("peer-{index}")), behavior))
        .collect()
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

fn run_bench_network(workspace: &Workspace, args: BenchArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "bench-network", args.common.profile)?;
    let envs = BTreeMap::new();
    let mut steps = vec![workspace.run_cargo(
        &artifacts,
        "build-testkit-node",
        &[
            "build",
            "-p",
            "burn_p2p_testkit",
            "--features",
            "native-gpu-probe",
            "--bin",
            "burn-p2p-testkit-node",
            "--bin",
            "burn-p2p-testkit-native-accelerator-probe",
        ],
        &envs,
    )?];

    let requested_backend = native_accelerator_backend_request();
    let accelerator_summary = if requested_backend == "ndarray" {
        NativeAcceleratorProbeSummary {
            status: "skipped".into(),
            requested_backend: requested_backend.clone(),
            resolved_backend: Some("ndarray".into()),
            matrix_size: native_accelerator_matrix_size(args.common.profile),
            warmup_iterations: native_accelerator_warmup_iterations(args.common.profile),
            measured_iterations: native_accelerator_measured_iterations(args.common.profile),
            elapsed_ms: 0,
            iterations_per_sec: None,
            failure_reason: Some("forced ndarray backend for network bench".into()),
        }
    } else {
        let probe_binary = workspace.root.join("target").join("debug").join(format!(
            "burn-p2p-testkit-native-accelerator-probe{}",
            env::consts::EXE_SUFFIX
        ));
        let output_path = artifacts
            .metrics
            .join("network-native-accelerator-summary.json");
        steps.push(
            workspace.run(
                &artifacts,
                "probe-network-native-accelerator",
                probe_binary
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("non-utf8 accelerator probe path"))?,
                &[
                    "--output".into(),
                    output_path.display().to_string(),
                    "--backend".into(),
                    requested_backend.clone(),
                    "--matrix-size".into(),
                    native_accelerator_matrix_size(args.common.profile).to_string(),
                    "--warmup".into(),
                    native_accelerator_warmup_iterations(args.common.profile).to_string(),
                    "--iterations".into(),
                    native_accelerator_measured_iterations(args.common.profile).to_string(),
                ],
                &envs,
            )?,
        );
        serde_json::from_slice(
            &fs::read(&output_path)
                .with_context(|| format!("failed to read {}", output_path.display()))?,
        )
        .with_context(|| format!("failed to decode {}", output_path.display()))?
    };
    artifacts.write_json(
        "metrics/network-native-accelerator-summary.json",
        &accelerator_summary,
    )?;
    let trainer_backend = match accelerator_summary.resolved_backend.as_deref() {
        Some("cuda") => SyntheticNativeBackend::Cuda,
        Some("wgpu") => SyntheticNativeBackend::Wgpu,
        Some("ndarray") => SyntheticNativeBackend::NdArray,
        Some(other) => {
            return Err(anyhow::anyhow!(
                "unsupported accelerator backend `{other}` returned by probe"
            ));
        }
        None if requested_backend == "auto" => SyntheticNativeBackend::NdArray,
        None => {
            return Err(anyhow::anyhow!(
                "requested backend `{requested_backend}` was unavailable: {}",
                accelerator_summary
                    .failure_reason
                    .clone()
                    .unwrap_or_else(|| accelerator_summary.status.clone())
            ));
        }
    };

    let peer_count = args.common.profile.settings().multiprocess_peers.max(4);
    let trainer_count = peer_count.saturating_sub(1).max(1);
    let trainer_window_count = args.common.profile.settings().trainer_windows.max(4);
    let soak_config = SyntheticSoakConfig {
        root: artifacts.root.join("synthetic-soak"),
        workload_kind: SyntheticWorkloadKind::BurnLinear,
        trainer_backend: trainer_backend.clone(),
        validator_backend: trainer_backend.clone(),
        trainer_count,
        trainer_window_count,
        persistent_trainers: true,
        continuous_training: true,
        startup_timeout_secs: 30,
        poll_interval_ms: 50,
        sync_timeout_secs: 30,
        merge_wait_timeout_secs: 30,
    };
    artifacts.write_json("configs/network-soak-config.json", &soak_config)?;

    let node_binary = workspace
        .root
        .join("target")
        .join("debug")
        .join(format!("burn-p2p-testkit-node{}", env::consts::EXE_SUFFIX));
    let mut soak_summary = run_synthetic_process_soak(&soak_config, &node_binary)
        .context("network bench synthetic soak failed")?;
    if soak_summary.performance_summary.is_none() {
        soak_summary.performance_summary =
            derive_synthetic_soak_performance_summary(&soak_summary)?;
    }
    if soak_summary.dynamics_summary.is_none() {
        soak_summary.dynamics_summary = derive_synthetic_network_dynamics_summary(&soak_summary);
    }
    artifacts.write_json("metrics/network-soak-summary.json", &soak_summary)?;
    if let Some(performance_summary) = soak_summary.performance_summary.as_ref() {
        artifacts.write_json(
            "metrics/network-soak-performance-summary.json",
            performance_summary,
        )?;
    }
    if let Some(dynamics_summary) = soak_summary.dynamics_summary.as_ref() {
        artifacts.write_json(
            "metrics/network-soak-dynamics-summary.json",
            dynamics_summary,
        )?;
    }

    let browser_peer_count = peer_count.min(3).min(peer_count.saturating_sub(1));
    let mut discovery_spec = SimulationSpec {
        peer_count,
        browser_peer_count,
        window_count: trainer_window_count.max(3),
        chaos_events: generate_chaos_events(
            seed_now(),
            args.common.profile.settings().chaos_events.max(4),
            peer_count,
        ),
        ..SimulationSpec::default()
    };
    discovery_spec.bootstrap_preset = BootstrapPreset::BootstrapOnly;
    discovery_spec.malicious_peers = discovery_malicious_peers(peer_count, browser_peer_count);
    artifacts.write_json("configs/network-discovery-spec.json", &discovery_spec)?;

    let discovery_started = Instant::now();
    let discovery_outcome = SimulationRunner::default()
        .run(discovery_spec.clone())
        .context("network bench discovery simulation failed")?;
    let discovery_duration_ms = discovery_started.elapsed().as_millis();
    let discovery_summary =
        summarize_discovery_dynamics(&discovery_spec, &discovery_outcome, discovery_duration_ms);
    let discovery_merge_rate_per_sec = discovery_summary.get("merge_rate_per_sec").cloned();
    let discovery_receipt_rate_per_sec = discovery_summary.get("receipt_rate_per_sec").cloned();
    artifacts.write_json(
        "topology/network-discovery-outcome.json",
        &discovery_outcome,
    )?;
    artifacts.write_json("metrics/network-discovery-summary.json", &discovery_summary)?;

    let bench_summary = json!({
        "native_soak": {
            "workload_kind": soak_summary.workload_kind,
            "trainer_backend": soak_summary.trainer_backend,
            "validator_backend": soak_summary.validator_backend,
            "trainer_count": soak_summary.trainer_count,
            "trainer_window_count": soak_summary.trainer_window_count,
            "continuous_training": soak_summary.continuous_training,
            "elapsed_millis": soak_summary.elapsed_millis,
            "performance_summary": soak_summary.performance_summary,
            "dynamics_summary": soak_summary.dynamics_summary,
        },
        "native_accelerator": accelerator_summary,
        "discovery": discovery_summary,
    });
    artifacts.write_json("metrics/network-bench-summary.json", &bench_summary)?;

    finalize_run(
        &artifacts,
        "bench-network",
        args.common.profile,
        &steps,
        json!({
            "kind": "bench-network",
            "peer_count": peer_count,
            "trainer_count": trainer_count,
            "trainer_window_count": trainer_window_count,
            "persistent_trainers": true,
            "continuous_training": true,
            "workload_kind": "burn-linear",
            "trainer_backend": trainer_backend,
            "validator_backend": soak_summary.validator_backend,
            "soak_elapsed_millis": soak_summary.elapsed_millis,
            "soak_training_throughput_work_units_per_sec": soak_summary
                .performance_summary
                .as_ref()
                .map(|summary| summary.training.throughput_work_units_per_sec),
            "soak_validation_throughput_work_units_per_sec": soak_summary
                .performance_summary
                .as_ref()
                .map(|summary| summary.validation.throughput_work_units_per_sec),
            "soak_head_eval_throughput_samples_per_sec": soak_summary
                .performance_summary
                .as_ref()
                .map(|summary| summary.head_evaluation.throughput_samples_per_sec),
            "discovery_duration_ms": discovery_duration_ms,
            "discovery_merge_rate_per_sec": discovery_merge_rate_per_sec,
            "discovery_receipt_rate_per_sec": discovery_receipt_rate_per_sec,
        }),
        args.common.keep_artifacts,
    )
}

fn native_accelerator_backend_request() -> String {
    env::var("BURN_P2P_NATIVE_ACCELERATOR_BACKEND").unwrap_or_else(|_| "auto".into())
}

fn native_accelerator_matrix_size(profile: Profile) -> usize {
    env::var("BURN_P2P_NATIVE_ACCELERATOR_MATRIX_SIZE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(match profile {
            Profile::Dev => 384,
            Profile::Smoke | Profile::CiPr => 512,
            Profile::CiIntegration | Profile::RealBrowser => 768,
            Profile::Nightly => 1024,
        })
}

fn native_accelerator_warmup_iterations(profile: Profile) -> u32 {
    env::var("BURN_P2P_NATIVE_ACCELERATOR_WARMUP")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(match profile {
            Profile::Dev => 1,
            Profile::Smoke | Profile::CiPr => 2,
            Profile::CiIntegration | Profile::RealBrowser => 2,
            Profile::Nightly => 3,
        })
}

fn native_accelerator_measured_iterations(profile: Profile) -> u32 {
    env::var("BURN_P2P_NATIVE_ACCELERATOR_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(match profile {
            Profile::Dev => 3,
            Profile::Smoke | Profile::CiPr => 4,
            Profile::CiIntegration | Profile::RealBrowser => 6,
            Profile::Nightly => 10,
        })
}

fn run_bench_accelerator(workspace: &Workspace, args: BenchArgs) -> anyhow::Result<()> {
    let artifacts =
        ArtifactLayout::create(&workspace.root, "bench-accelerator", args.common.profile)?;
    let envs = BTreeMap::new();
    let mut steps = vec![workspace.run_cargo(
        &artifacts,
        "build-native-accelerator-probe",
        &[
            "build",
            "-p",
            "burn_p2p_testkit",
            "--features",
            "native-gpu-probe",
            "--bin",
            "burn-p2p-testkit-native-accelerator-probe",
        ],
        &envs,
    )?];

    let output_path = artifacts.metrics.join("native-accelerator-summary.json");
    let probe_binary = workspace.root.join("target").join("debug").join(format!(
        "burn-p2p-testkit-native-accelerator-probe{}",
        env::consts::EXE_SUFFIX
    ));
    let backend = native_accelerator_backend_request();
    let matrix_size = native_accelerator_matrix_size(args.common.profile);
    let warmup_iterations = native_accelerator_warmup_iterations(args.common.profile);
    let measured_iterations = native_accelerator_measured_iterations(args.common.profile);
    steps.push(
        workspace.run(
            &artifacts,
            "run-native-accelerator-probe",
            probe_binary
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("non-utf8 accelerator probe path"))?,
            &[
                "--output".into(),
                output_path.display().to_string(),
                "--backend".into(),
                backend.clone(),
                "--matrix-size".into(),
                matrix_size.to_string(),
                "--warmup".into(),
                warmup_iterations.to_string(),
                "--iterations".into(),
                measured_iterations.to_string(),
            ],
            &envs,
        )?,
    );

    let summary: NativeAcceleratorProbeSummary = serde_json::from_slice(
        &fs::read(&output_path)
            .with_context(|| format!("failed to read {}", output_path.display()))?,
    )
    .with_context(|| format!("failed to decode {}", output_path.display()))?;

    finalize_run(
        &artifacts,
        "bench-accelerator",
        args.common.profile,
        &steps,
        json!({
            "kind": "bench-accelerator",
            "status": summary.status,
            "requested_backend": summary.requested_backend,
            "resolved_backend": summary.resolved_backend,
            "matrix_size": summary.matrix_size,
            "warmup_iterations": summary.warmup_iterations,
            "measured_iterations": summary.measured_iterations,
            "elapsed_ms": summary.elapsed_ms,
            "iterations_per_sec": summary.iterations_per_sec,
            "failure_reason": summary.failure_reason,
        }),
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
                "auth-redis-live",
                vec![
                    "check",
                    "auth-redis-live",
                    "--profile",
                    "nightly",
                    "--keep-artifacts",
                ],
            ),
            (
                "auth-oidc-live",
                vec![
                    "check",
                    "auth-oidc-live",
                    "--profile",
                    "nightly",
                    "--keep-artifacts",
                ],
            ),
            (
                "bench-nightly",
                vec![
                    "bench",
                    "network",
                    "--profile",
                    "nightly",
                    "--keep-artifacts",
                ],
            ),
            (
                "bench-accelerator",
                vec![
                    "bench",
                    "accelerator",
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
