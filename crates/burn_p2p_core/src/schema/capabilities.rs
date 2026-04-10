use super::*;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported peer roles.
pub enum PeerRole {
    /// Uses the bootstrap variant.
    Bootstrap,
    /// Uses the authority variant.
    Authority,
    /// Uses the validator variant.
    Validator,
    /// Uses the archive variant.
    Archive,
    /// Uses the reducer variant.
    Reducer,
    /// Uses the trainer GPU variant.
    TrainerGpu,
    /// Uses the trainer CPU variant.
    TrainerCpu,
    /// Uses the evaluator variant.
    Evaluator,
    /// Uses the portal viewer variant.
    Viewer,
    /// Uses the browser observer variant.
    BrowserObserver,
    /// Uses the browser verifier variant.
    BrowserVerifier,
    /// Uses the browser trainer wgpu variant.
    BrowserTrainerWgpu,
    /// Uses the browser fallback variant.
    BrowserFallback,
    /// Uses the browser trainer variant.
    BrowserTrainer,
    /// Uses the relay helper variant.
    RelayHelper,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported experiment scopes.
pub enum ExperimentScope {
    /// Uses the connect variant.
    Connect,
    /// Uses the discover variant.
    Discover,
    /// Uses the train variant.
    Train {
        /// The experiment ID.
        experiment_id: ExperimentId,
    },
    /// Uses the validate variant.
    Validate {
        /// The experiment ID.
        experiment_id: ExperimentId,
    },
    /// Uses the archive variant.
    Archive {
        /// The experiment ID.
        experiment_id: ExperimentId,
    },
    /// Uses the admin variant.
    Admin {
        /// The study ID.
        study_id: StudyId,
    },
}

impl ExperimentScope {
    /// Returns whether the value applies to experiment.
    pub fn applies_to_experiment(&self, experiment_id: &ExperimentId) -> bool {
        matches!(
            self,
            Self::Train { experiment_id: scoped }
                | Self::Validate { experiment_id: scoped }
                | Self::Archive { experiment_id: scoped }
                if scoped == experiment_id
        )
    }

    /// Returns whether the value allows directory discovery.
    pub fn allows_directory_discovery(&self) -> bool {
        matches!(self, Self::Connect | Self::Discover | Self::Admin { .. })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a peer role set.
pub struct PeerRoleSet {
    /// The roles.
    pub roles: BTreeSet<PeerRole>,
}

impl PeerRoleSet {
    /// Creates a new value.
    pub fn new(roles: impl IntoIterator<Item = PeerRole>) -> Self {
        Self {
            roles: roles.into_iter().collect(),
        }
    }

    /// Performs the default trainer operation.
    pub fn default_trainer() -> Self {
        Self::new([PeerRole::TrainerGpu])
    }

    /// Performs the contains operation.
    pub fn contains(&self, role: &PeerRole) -> bool {
        self.roles.contains(role)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported persistence class values.
pub enum PersistenceClass {
    /// Uses the ephemeral variant.
    Ephemeral,
    /// Uses the session variant.
    Session,
    /// Uses the durable variant.
    Durable,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported client platform values.
pub enum ClientPlatform {
    /// Uses the native variant.
    Native,
    /// Uses the browser variant.
    Browser,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported artifact target kinds.
pub enum ArtifactTargetKind {
    /// Uses the native linux x86 64 kind.
    NativeLinuxX86_64,
    /// Uses the native macos aarch64 kind.
    NativeMacosAarch64,
    /// Uses the native windows x86 64 kind.
    NativeWindowsX86_64,
    /// Uses the browser WASM kind.
    BrowserWasm,
    /// Uses the other kind.
    Other(String),
}

impl ArtifactTargetKind {
    /// Returns the target artifact ID view.
    pub fn as_target_artifact_id(&self) -> &str {
        match self {
            Self::NativeLinuxX86_64 => "native-linux-x86_64",
            Self::NativeMacosAarch64 => "native-macos-aarch64",
            Self::NativeWindowsX86_64 => "native-windows-x86_64",
            Self::BrowserWasm => "browser-wasm",
            Self::Other(target) => target.as_str(),
        }
    }

    /// Performs the parse operation.
    pub fn parse(target: &str) -> Self {
        match target {
            "native-linux-x86_64" => Self::NativeLinuxX86_64,
            "native-macos-aarch64" => Self::NativeMacosAarch64,
            "native-windows-x86_64" => Self::NativeWindowsX86_64,
            "browser-wasm" => Self::BrowserWasm,
            other => Self::Other(other.into()),
        }
    }

    /// Performs the platform operation.
    pub fn platform(&self) -> ClientPlatform {
        match self {
            Self::BrowserWasm => ClientPlatform::Browser,
            Self::NativeLinuxX86_64
            | Self::NativeMacosAarch64
            | Self::NativeWindowsX86_64
            | Self::Other(_) => ClientPlatform::Native,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported browser roles.
pub enum BrowserRole {
    /// Uses the portal viewer variant.
    Viewer,
    /// Allows observer behavior.
    Observer,
    /// Allows verifier behavior.
    Verifier,
    /// Uses the trainer wgpu variant.
    TrainerWgpu,
    /// Uses the fallback variant.
    Fallback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported browser capability values.
pub enum BrowserCapability {
    /// Uses the web GPU variant.
    WebGpu,
    /// Uses the dedicated worker variant.
    DedicatedWorker,
    /// Uses the persistent storage variant.
    PersistentStorage,
    /// Uses the web rtc direct variant.
    WebRtcDirect,
    /// Uses the web transport variant.
    WebTransport,
    /// Uses the WSS fallback variant.
    WssFallback,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported attestation level values.
pub enum AttestationLevel {
    /// Uses the none variant.
    None,
    /// Uses the manifest variant.
    Manifest,
    /// Uses the challenge variant.
    Challenge,
    /// Uses the strong variant.
    Strong,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported capability class values.
pub enum CapabilityClass {
    /// Uses the trainer GPU variant.
    TrainerGpu,
    /// Uses the trainer CPU variant.
    TrainerCpu,
    /// Uses the evaluator variant.
    Evaluator,
    /// Uses the archive variant.
    Archive,
    /// Uses the relay helper variant.
    RelayHelper,
    /// Uses the browser observer variant.
    BrowserObserver,
    /// Uses the browser verifier variant.
    BrowserVerifier,
    /// Uses the browser trainer wgpu variant.
    BrowserTrainerWgpu,
    /// Uses the browser fallback variant.
    BrowserFallback,
    /// Uses the browser opportunistic variant.
    BrowserOpportunistic,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a capability card.
pub struct CapabilityCard {
    /// The card ID.
    pub card_id: CapabilityCardId,
    /// The peer ID.
    pub peer_id: PeerId,
    /// The platform.
    pub platform: ClientPlatform,
    /// The roles.
    pub roles: PeerRoleSet,
    /// The preferred backends.
    pub preferred_backends: Vec<String>,
    #[serde(default)]
    /// The browser capabilities detected when this card was measured.
    pub browser_capabilities: BTreeSet<BrowserCapability>,
    /// The recommended classes.
    pub recommended_classes: BTreeSet<CapabilityClass>,
    /// The device memory bytes.
    pub device_memory_bytes: Option<u64>,
    /// The system memory bytes.
    pub system_memory_bytes: u64,
    /// The disk bytes.
    pub disk_bytes: u64,
    /// The upload mbps.
    pub upload_mbps: f32,
    /// The download mbps.
    pub download_mbps: f32,
    /// The persistence.
    pub persistence: PersistenceClass,
    /// The work units per second.
    pub work_units_per_second: f64,
    /// The attestation level.
    pub attestation_level: AttestationLevel,
    /// The benchmark hash.
    pub benchmark_hash: Option<ContentId>,
    /// The reported at.
    pub reported_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a capability estimate.
pub struct CapabilityEstimate {
    /// The preferred backends.
    pub preferred_backends: Vec<String>,
    /// The work units per second.
    pub work_units_per_second: f64,
    /// The target window seconds.
    pub target_window_seconds: u64,
}
