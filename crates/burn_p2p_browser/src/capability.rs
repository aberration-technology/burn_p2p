use std::collections::BTreeSet;

use burn_p2p::{BrowserCapability, ClientPlatform};
use serde::{Deserialize, Serialize};

use crate::BrowserRuntimeRole;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrowserGpuSupport {
    Available,
    Unavailable(String),
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrowserWorkerSupport {
    DedicatedWorker,
    Unavailable(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserCapabilityReport {
    pub platform: ClientPlatform,
    pub navigator_gpu_exposed: bool,
    pub worker_gpu_exposed: bool,
    pub gpu_support: BrowserGpuSupport,
    pub dedicated_worker: BrowserWorkerSupport,
    pub recommended_role: BrowserRuntimeRole,
    pub max_training_window_secs: u64,
    pub max_checkpoint_bytes: u64,
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
