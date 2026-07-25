use super::*;

/// Immutable description emitted when a native artifact window begins.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TrainingWindowStartedEvent {
    /// Study owning the window.
    pub study_id: StudyId,
    /// Experiment owning the window.
    pub experiment_id: ExperimentId,
    /// Revision executed by the window.
    pub revision_id: RevisionId,
    /// Monotonic window identifier.
    pub window_id: WindowId,
    /// Canonical or speculative base head loaded for the window.
    pub base_head_id: HeadId,
    /// Runtime timestamp captured before shard fetch and model execution.
    pub started_at: DateTime<Utc>,
}

/// Immutable description emitted after an artifact window is published.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TrainingWindowCompletedEvent {
    /// Study owning the window.
    pub study_id: StudyId,
    /// Experiment owning the window.
    pub experiment_id: ExperimentId,
    /// Revision executed by the window.
    pub revision_id: RevisionId,
    /// Monotonic window identifier.
    pub window_id: WindowId,
    /// Base head loaded for the window.
    pub base_head_id: HeadId,
    /// Published training head.
    pub head_id: HeadId,
    /// Published model or compact-update artifact.
    pub artifact_id: ArtifactId,
    /// Runtime timestamp captured before shard fetch and model execution.
    pub started_at: DateTime<Utc>,
    /// Backend completion timestamp.
    pub completed_at: DateTime<Utc>,
    /// Time spent fetching leased data.
    pub data_fetch_time_ms: u64,
    /// Time spent publishing the completed update.
    pub publish_latency_ms: u64,
    /// Workload-projected contribution metrics.
    pub metrics: BTreeMap<String, MetricValue>,
}

/// Non-blocking subscriber for native training-window lifecycle events.
///
/// Implementations run inline on the training caller. They should enqueue or
/// record events without blocking on I/O, network transport, or downstream
/// application updates.
pub trait TrainingWindowObserver: Send + Sync + 'static {
    /// Observes a window after planning and before data fetch begins.
    fn window_started(&self, _event: &TrainingWindowStartedEvent) {}

    /// Observes a window after its update has been published locally.
    fn window_completed(&self, _event: &TrainingWindowCompletedEvent) {}
}

#[derive(Clone, Default)]
pub(crate) struct TrainingWindowObservers {
    subscribers: Vec<Arc<dyn TrainingWindowObserver>>,
}

impl fmt::Debug for TrainingWindowObservers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrainingWindowObservers")
            .field("subscriber_count", &self.subscribers.len())
            .finish()
    }
}

impl TrainingWindowObservers {
    pub(crate) fn push(&mut self, observer: Arc<dyn TrainingWindowObserver>) {
        self.subscribers.push(observer);
    }

    pub(crate) fn window_started(&self, event: &TrainingWindowStartedEvent) {
        for observer in &self.subscribers {
            observer.window_started(event);
        }
    }

    pub(crate) fn window_completed(&self, event: &TrainingWindowCompletedEvent) {
        for observer in &self.subscribers {
            observer.window_completed(event);
        }
    }
}

impl<P> RunningNode<P> {
    pub(crate) fn notify_training_window_started(&self, event: &TrainingWindowStartedEvent) {
        self.node
            .as_ref()
            .expect("running node should retain prepared node")
            .training_window_observers
            .window_started(event);
    }

    pub(crate) fn notify_training_window_completed<T>(
        &self,
        experiment: &ExperimentHandle,
        outcome: &TrainingWindowOutcome<T>,
    ) {
        let event = TrainingWindowCompletedEvent {
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: outcome.lease.window_id,
            base_head_id: outcome.contribution.base_head_id.clone(),
            head_id: outcome.head.head_id.clone(),
            artifact_id: outcome.artifact.artifact_id.clone(),
            started_at: outcome.timing.window_started_at,
            completed_at: outcome.timing.completed_at,
            data_fetch_time_ms: outcome.timing.data_fetch_time_ms,
            publish_latency_ms: outcome.timing.publish_latency_ms,
            metrics: outcome.head.metrics.clone(),
        };
        self.node
            .as_ref()
            .expect("running node should retain prepared node")
            .training_window_observers
            .window_completed(&event);
    }
}
