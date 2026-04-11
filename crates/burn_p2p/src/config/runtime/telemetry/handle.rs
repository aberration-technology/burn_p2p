use super::*;

#[derive(Clone)]
/// Represents a telemetry handle.
pub struct TelemetryHandle {
    pub(crate) state: Arc<Mutex<NodeTelemetrySnapshot>>,
}

impl fmt::Debug for TelemetryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TelemetryHandle").finish_non_exhaustive()
    }
}

impl TelemetryHandle {
    /// Performs the snapshot operation.
    pub fn snapshot(&self) -> NodeTelemetrySnapshot {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}
