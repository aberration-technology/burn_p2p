use std::collections::BTreeSet;

use burn_p2p::{BrowserCapability, CapabilityProbe, PeerId, browser_probe_from_capabilities};
use burn_p2p_core::{ClientPlatform, ContentId, PersistenceClass};
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
    /// Whether persistent storage APIs are exposed.
    pub persistent_storage_exposed: bool,
    /// Whether WebTransport APIs are exposed.
    pub web_transport_exposed: bool,
    /// Whether WebRTC direct APIs are exposed.
    pub web_rtc_exposed: bool,
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
            persistent_storage_exposed: false,
            web_transport_exposed: false,
            web_rtc_exposed: false,
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
        if self.persistent_storage_exposed {
            capabilities.insert(BrowserCapability::PersistentStorage);
        }
        if self.web_transport_exposed {
            capabilities.insert(BrowserCapability::WebTransport);
        }
        if self.web_rtc_exposed {
            capabilities.insert(BrowserCapability::WebRtcDirect);
        }

        capabilities
    }

    /// Converts the browser capability report into the shared limits probe format.
    pub fn to_capability_probe(
        &self,
        peer_id: PeerId,
        work_units_per_second: f64,
        persistence: PersistenceClass,
        benchmark_hash: Option<ContentId>,
    ) -> CapabilityProbe {
        browser_probe_from_capabilities(
            peer_id,
            self.capabilities(),
            work_units_per_second,
            persistence,
            benchmark_hash,
        )
    }
}
