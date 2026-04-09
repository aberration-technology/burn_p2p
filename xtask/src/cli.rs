use clap::{Args, Parser, Subcommand, ValueEnum};

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
    Publish(PublishArgs),
    /// Run the env-gated live OIDC integration check.
    AuthOidcLive(RunArgs),
    /// Run the Redis-backed browser-edge auth HA and failure-mode checks.
    AuthRedisLive(RunArgs),
}

#[derive(Debug, Args)]
pub struct PublishArgs {
    #[command(flatten)]
    pub common: CommonArgs,
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
pub enum CiCommand {
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
pub struct MultiprocessArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Total peer count, including the validator process.
    #[arg(long)]
    pub peers: Option<u32>,
    /// Approximate run duration, for example 90s or 2m.
    #[arg(long)]
    pub duration: Option<String>,
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
