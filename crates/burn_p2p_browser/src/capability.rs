use std::collections::BTreeSet;

use burn_p2p::{BrowserCapability, ClientPlatform};
use serde::{Deserialize, Serialize};

use crate::BrowserRuntimeRole;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported browser GPU support values.
pub enum BrowserGpuSupport {
    /// Uses the available variant.
    Available,
    /// Uses the unavailable variant.
    Unavailable(String),
    /// Carries an unrecognized value.
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Enumerates the supported browser worker support values.
pub enum BrowserWorkerSupport {
    /// Uses the dedicated worker variant.
    DedicatedWorker,
    /// Uses the unavailable variant.
    Unavailable(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Reports browser capability details.
pub struct BrowserCapabilityReport {
    /// The platform.
    pub platform: ClientPlatform,
    /// The navigator GPU exposed.
    pub navigator_gpu_exposed: bool,
    /// The worker GPU exposed.
    pub worker_gpu_exposed: bool,
    /// The GPU support.
    pub gpu_support: BrowserGpuSupport,
    /// The dedicated worker.
    pub dedicated_worker: BrowserWorkerSupport,
    /// The recommended role.
    pub recommended_role: BrowserRuntimeRole,
    /// The max training window secs.
    pub max_training_window_secs: u64,
    /// The max checkpoint bytes.
    pub max_checkpoint_bytes: u64,
    /// The max shard bytes.
    pub max_shard_bytes: u64,
}

impl Default for BrowserCapabilityReport {
    fn default() -> Self {
        Self {
            platform: ClientPlatform::Browser,
            navigator_gpu_exposed: false,
            worker_gpu_exposed: false,
            gpu_support: BrowserGpuSupport::Unknown,
            dedicated_worker: BrowserWorkerSupport::DedicatedWorker,
            recommended_role: BrowserRuntimeRole::BrowserObserver,
            max_training_window_secs: 30,
            max_checkpoint_bytes: 16 * 1024 * 1024,
            max_shard_bytes: 8 * 1024 * 1024,
        }
    }
}

impl BrowserCapabilityReport {
    /// Performs the capabilities operation.
    pub fn capabilities(&self) -> BTreeSet<BrowserCapability> {
        let mut capabilities = BTreeSet::from([BrowserCapability::WssFallback]);

        if matches!(self.dedicated_worker, BrowserWorkerSupport::DedicatedWorker) {
            capabilities.insert(BrowserCapability::DedicatedWorker);
        }

        if matches!(self.gpu_support, BrowserGpuSupport::Available) {
            capabilities.insert(BrowserCapability::WebGpu);
        }

        capabilities
    }
}
