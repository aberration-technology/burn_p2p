use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};

use crate::profile::Profile;

#[derive(Debug, Parser)]
#[command(author, version, about = "Local-first task runner for burn_p2p")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Check local tooling and browser prerequisites.
    Doctor,
    /// Install or validate optional local tooling.
    Setup {
        #[command(subcommand)]
        command: SetupCommand,
    },
    /// Run fast local checks or publish-readiness checks.
    Check(CheckCommand),
    /// Publish workspace crates to crates.io in dependency order.
    Publish(PublishCommand),
    /// Run local-first end-to-end suites.
    E2e {
        #[command(subcommand)]
        command: E2eCommand,
    },
    /// Run browser-facing suites.
    Browser {
        #[command(subcommand)]
        command: BrowserCommand,
    },
    /// Run adversarial robustness suites.
    Adversarial {
        #[command(subcommand)]
        command: AdversarialCommand,
    },
    /// Run stress and chaos suites.
    Stress {
        #[command(subcommand)]
        command: StressCommand,
    },
    /// Run benchmark suites.
    Bench {
        #[command(subcommand)]
        command: BenchCommand,
    },
    /// Run deployment helpers for local compose or cloud terraform stacks.
    Deploy {
        #[command(subcommand)]
        command: DeployCommand,
    },
    /// Run optional formal verification helpers and trace exports.
    Formal {
        #[command(subcommand)]
        command: FormalCommand,
    },
    /// Run the same grouped lanes used by CI workflows.
    Ci {
        #[command(subcommand)]
        command: CiCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum SetupCommand {
    /// Install browser-facing dependencies such as Playwright and wasm target support.
    Browser,
}

#[derive(Debug, Args)]
pub struct CheckCommand {
    #[command(subcommand)]
    pub command: Option<CheckSubcommand>,
    #[command(flatten)]
    pub common: CommonArgs,
}

#[derive(Debug, Subcommand)]
pub enum CheckSubcommand {
    /// Run local publish-readiness checks and dry runs.
    Publish(CheckPublishArgs),
    /// Run the env-gated live OIDC integration check.
    AuthOidcLive(RunArgs),
    /// Run the Redis-backed browser-edge auth HA and failure-mode checks.
    AuthRedisLive(RunArgs),
}

#[derive(Debug, Args)]
pub struct CheckPublishArgs {
    #[command(flatten)]
    pub common: CommonArgs,
}

#[derive(Debug, Args, Clone)]
pub struct PublishCommand {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Skip publish-readiness checks before publishing.
    #[arg(long)]
    pub skip_checks: bool,
    /// Package and verify each crate without uploading to crates.io.
    #[arg(long)]
    pub dry_run: bool,
    /// Resume the publish sequence starting from this crate name.
    #[arg(long)]
    pub from: Option<String>,
    /// Allow publishing from a dirty worktree.
    #[arg(long)]
    pub allow_dirty: bool,
}

#[derive(Debug, Subcommand)]
pub enum E2eCommand {
    /// Run the default native smoke suite.
    Smoke(RunArgs),
    /// Run the mixed native/browser smoke suite.
    Mixed(RunArgs),
    /// Run the downstream MNIST single-machine sanity suite and browser captures.
    Mnist(RunArgs),
    /// Run publication and metrics persistence smoke.
    Services(RunArgs),
}

#[derive(Debug, Subcommand)]
pub enum BrowserCommand {
    /// Run headless Playwright capture smoke locally.
    Smoke(BrowserArgs),
    /// Run the browser training smoke path and captures.
    Trainer(BrowserArgs),
    /// Run the local real-browser probe path.
    Real(BrowserArgs),
    /// Build a static wasm browser-site bundle for a downstream browser shell.
    Site(Box<BrowserSiteArgs>),
}

#[derive(Debug, Subcommand)]
pub enum AdversarialCommand {
    /// Run the default adversarial smoke scenarios.
    Smoke(RunArgs),
    /// Run the broader adversarial matrix locally.
    Matrix(RunArgs),
    /// Run one seeded adversarial scenario for replay.
    Chaos(ChaosArgs),
}

#[derive(Debug, Subcommand)]
pub enum StressCommand {
    /// Run the synthetic multiprocess soak harness locally.
    Multiprocess(MultiprocessArgs),
    /// Run a seeded chaos simulation locally.
    Chaos(ChaosArgs),
    /// Run the larger-fleet discovery and routing soak lane.
    Discovery(RunArgs),
}

#[derive(Debug, Subcommand)]
pub enum BenchCommand {
    /// Run the faster core benches.
    Core(BenchArgs),
    /// Run deployment-shaped network dynamics benches.
    Network(BenchArgs),
    /// Run the native cuda/wgpu accelerator probe lane.
    Accelerator(BenchArgs),
    /// Run robustness-oriented synthetic benches.
    Robust(BenchArgs),
    /// Run the heavier nightly bench profile.
    Nightly(BenchArgs),
}

#[derive(Debug, Subcommand)]
pub enum DeployCommand {
    /// Run a local docker compose deployment stack.
    Compose(DeployComposeArgs),
    /// Run the AWS terraform deployment wrapper.
    Aws(DeployCloudArgs),
    /// Run the GCP terraform deployment wrapper.
    Gcp(DeployCloudArgs),
}

#[derive(Debug, Subcommand)]
pub enum FormalCommand {
    /// Build the standalone formal/veil project.
    Check(FormalArgs),
    /// Build the dedicated modelcheck target in formal/veil.
    Modelcheck(FormalArgs),
    /// Export a versioned protocol trace for refinement checks.
    ExportTrace(FormalExportArgs),
    /// Generate a lean fixture from a rust trace export and verify it under lake.
    VerifyTrace(FormalExportArgs),
}

#[derive(Debug, Subcommand)]
pub enum CiCommand {
    /// Run the local production-contract lane before deploy-facing changes.
    LocalContract(CiArgs),
    /// Run the lean PR fast lane locally.
    PrFast(CiArgs),
    /// Run the browser workflow lane locally.
    Browser(CiArgs),
    /// Run the broader integration lane locally.
    Integration(CiArgs),
    /// Run the publication and metrics smoke lane locally.
    Services(CiArgs),
    /// Run the nightly lane locally.
    Nightly(CiArgs),
    /// Run the release-readiness lane locally.
    Publish(CiArgs),
}

#[derive(Debug, Args, Clone)]
pub struct CommonArgs {
    /// Profile selector used by local and CI flows.
    #[arg(long, value_enum, default_value_t = Profile::Smoke)]
    pub profile: Profile,
    /// Preserve generated artifacts even for successful local runs.
    #[arg(long)]
    pub keep_artifacts: bool,
}

#[derive(Debug, Args, Clone)]
pub struct RunArgs {
    #[command(flatten)]
    pub common: CommonArgs,
}

#[derive(Debug, Args, Clone)]
pub struct BrowserArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Launch browsers headed instead of headless.
    #[arg(long)]
    pub headed: bool,
}

#[derive(Debug, Args, Clone)]
pub struct BrowserSiteArgs {
    /// Workspace package that owns the wasm browser binary.
    #[arg(long)]
    pub package: String,
    /// Browser binary target name built for wasm.
    #[arg(long)]
    pub bin: String,
    /// Output directory for the generated static site.
    #[arg(long, default_value = "target/xtask/browser-site")]
    pub out_dir: PathBuf,
    /// Optional edge base URL embedded into the site bootstrap.
    #[arg(long)]
    pub edge_url: Option<String>,
    /// Seed node URLs embedded into the site bootstrap.
    #[arg(long = "seed-node-url", value_delimiter = ',', action = ArgAction::Append)]
    pub seed_node_urls: Vec<String>,
    /// Optional selected experiment identifier.
    #[arg(long)]
    pub selected_experiment_id: Option<String>,
    /// Optional selected revision identifier.
    #[arg(long)]
    pub selected_revision_id: Option<String>,
    /// Whether the generated site expects authenticated edge access.
    #[arg(long, default_value_t = true)]
    pub require_edge_auth: bool,
    /// Human-readable site title.
    #[arg(long, default_value = "burn_p2p browser")]
    pub app_name: String,
    /// Default browser-app surface for the generated shell.
    #[arg(long, default_value = "viewer")]
    pub default_surface: String,
    /// Cargo profile used for the wasm build.
    #[arg(long, default_value = "wasm-release")]
    pub wasm_profile: String,
    /// Optional cargo feature list passed to the wasm build.
    #[arg(long)]
    pub features: Option<String>,
    /// Disable default cargo features for the wasm build.
    #[arg(long)]
    pub no_default_features: bool,
}

#[derive(Debug, Args, Clone)]
pub struct MultiprocessArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Total peer count, including the validator process.
    #[arg(long)]
    pub peers: Option<u32>,
    /// Approximate run duration, for example 90s or 2m.
    #[arg(long)]
    pub duration: Option<String>,
    /// Explicit trainer window count for deterministic CI soak sizing.
    #[arg(long)]
    pub windows: Option<u32>,
}

#[derive(Debug, Args, Clone)]
pub struct ChaosArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Explicit deterministic seed for replay.
    #[arg(long)]
    pub seed: Option<u64>,
    /// Number of chaos events to synthesize.
    #[arg(long)]
    pub events: Option<u32>,
    /// Peer count in the synthetic simulation.
    #[arg(long)]
    pub peers: Option<u32>,
}

#[derive(Debug, Args, Clone)]
pub struct BenchArgs {
    #[command(flatten)]
    pub common: CommonArgs,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DeployComposeStack {
    BootstrapEdge,
    SplitFleet,
    TrustedMinimal,
    TrustedBrowser,
    EnterpriseSso,
    CommunityWeb,
}

impl DeployComposeStack {
    pub fn file_name(&self) -> &'static str {
        match self {
            Self::BootstrapEdge => "bootstrap-edge.compose.yaml",
            Self::SplitFleet => "split-fleet.compose.yaml",
            Self::TrustedMinimal => "trusted-minimal.compose.yaml",
            Self::TrustedBrowser => "trusted-browser.compose.yaml",
            Self::EnterpriseSso => "enterprise-sso.compose.yaml",
            Self::CommunityWeb => "community-web.compose.yaml",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DeployAction {
    Plan,
    Up,
    Down,
}

#[derive(Debug, Args, Clone)]
pub struct DeployComposeArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Compose action to run.
    #[arg(long, value_enum, default_value_t = DeployAction::Up)]
    pub action: DeployAction,
    /// Compose stack under deploy/compose/.
    #[arg(long, value_enum, default_value_t = DeployComposeStack::BootstrapEdge)]
    pub stack: DeployComposeStack,
    /// Optional env file passed through to docker compose.
    #[arg(long)]
    pub env_file: Option<String>,
    /// Additional compose profile to enable.
    #[arg(long)]
    pub profile_name: Vec<String>,
}

#[derive(Debug, Args, Clone)]
pub struct DeployCloudArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Terraform action to run.
    #[arg(long, value_enum, default_value_t = DeployAction::Plan)]
    pub action: DeployAction,
    /// Optional terraform var-file.
    #[arg(long)]
    pub var_file: Option<String>,
    /// Bootstrap image URI.
    #[arg(long)]
    pub bootstrap_image: Option<String>,
    /// Validator image URI.
    #[arg(long)]
    pub validator_image: Option<String>,
    /// Reducer image URI.
    #[arg(long)]
    pub reducer_image: Option<String>,
    /// Trainer image URI.
    #[arg(long)]
    pub trainer_image: Option<String>,
    /// Optional bootstrap config path passed as TF_VAR_bootstrap_config_json.
    #[arg(long)]
    pub bootstrap_config: Option<String>,
    /// Optional validator config path passed as TF_VAR_validator_config_json.
    #[arg(long)]
    pub validator_config: Option<String>,
    /// Optional reducer config path passed as TF_VAR_reducer_config_json.
    #[arg(long)]
    pub reducer_config: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub struct CiArgs {
    /// Preserve generated artifacts even for successful local runs.
    #[arg(long)]
    pub keep_artifacts: bool,
}

#[derive(Debug, Args, Clone)]
pub struct FormalArgs {
    #[command(flatten)]
    pub common: CommonArgs,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum FormalTraceScenarioArg {
    MnistSmoke,
    ReducerAdversarial,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum FormalTraceFormatArg {
    Json,
    Cbor,
}

#[derive(Debug, Args, Clone)]
pub struct FormalExportArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Named formal trace scenario to export.
    #[arg(long, value_enum, default_value_t = FormalTraceScenarioArg::MnistSmoke)]
    pub scenario: FormalTraceScenarioArg,
    /// Output format for the exported trace.
    #[arg(long, value_enum, default_value_t = FormalTraceFormatArg::Json)]
    pub format: FormalTraceFormatArg,
}
