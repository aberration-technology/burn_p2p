use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
    mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel},
};

use burn_ecs::bevy_ecs;
use burn_ecs::prelude::{
    App, IntoScheduleConfigs, MessageWriter, Plugin, Res, Resource, TrainingRunId, TrainingSet,
    Update,
};

use crate::{
    MetricValue, TrainingWindowCompletedEvent, TrainingWindowObserver, TrainingWindowStartedEvent,
};

use super::{
    P2pCanonicalReconcileEvent, P2pCapabilityAssessment, P2pContextUpdateObserved,
    P2pTrainingPlugin, P2pWindowFinished, P2pWindowStarted,
};

const DEFAULT_MAX_EVENTS_PER_UPDATE: usize = 256;

#[derive(Debug)]
enum P2pTrainingCommand {
    WindowStarted(P2pWindowStarted),
    WindowFinished(P2pWindowFinished),
    CanonicalReconcile(P2pCanonicalReconcileEvent),
    Capability(P2pCapabilityAssessment),
    ContextUpdate(P2pContextUpdateObserved),
}

#[derive(Clone, Debug)]
/// Bounded, non-blocking producer for a P2P training ECS app.
pub struct P2pTrainingEventBus {
    sender: SyncSender<P2pTrainingCommand>,
    capacity: usize,
    counters: Arc<P2pTrainingIngressCounters>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Point-in-time pressure and delivery counters for the P2P ECS ingress.
pub struct P2pTrainingEventBusStats {
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub queue_high_watermark: usize,
    pub send_attempts: u64,
    pub sends_accepted: u64,
    pub sends_full: u64,
    pub send_disconnects: u64,
}

impl P2pTrainingEventBusStats {
    pub fn utilization(&self) -> f64 {
        if self.queue_capacity == 0 {
            0.0
        } else {
            self.queue_depth as f64 / self.queue_capacity as f64
        }
    }
}

#[derive(Debug, Default)]
struct P2pTrainingIngressCounters {
    queue_depth: AtomicUsize,
    queue_high_watermark: AtomicUsize,
    send_attempts: AtomicU64,
    sends_accepted: AtomicU64,
    sends_full: AtomicU64,
    send_disconnects: AtomicU64,
}

impl P2pTrainingEventBus {
    fn send(&self, command: P2pTrainingCommand) -> anyhow::Result<()> {
        self.counters.send_attempts.fetch_add(1, Ordering::Relaxed);
        let depth = self
            .counters
            .queue_depth
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        match self.sender.try_send(command) {
            Ok(()) => {
                self.counters.sends_accepted.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .queue_high_watermark
                    .fetch_max(depth, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                self.counters.queue_depth.fetch_sub(1, Ordering::AcqRel);
                self.counters.sends_full.fetch_add(1, Ordering::Relaxed);
                Err(anyhow::anyhow!("P2P training ECS ingress queue is full"))
            }
            Err(TrySendError::Disconnected(_)) => {
                self.counters.queue_depth.fetch_sub(1, Ordering::AcqRel);
                self.counters
                    .send_disconnects
                    .fetch_add(1, Ordering::Relaxed);
                Err(anyhow::anyhow!("P2P training ECS ingress is disconnected"))
            }
        }
    }

    /// Returns bounded-ingress pressure and delivery counters.
    pub fn stats(&self) -> P2pTrainingEventBusStats {
        P2pTrainingEventBusStats {
            queue_capacity: self.capacity,
            queue_depth: self.counters.queue_depth.load(Ordering::Acquire),
            queue_high_watermark: self.counters.queue_high_watermark.load(Ordering::Relaxed),
            send_attempts: self.counters.send_attempts.load(Ordering::Relaxed),
            sends_accepted: self.counters.sends_accepted.load(Ordering::Relaxed),
            sends_full: self.counters.sends_full.load(Ordering::Relaxed),
            send_disconnects: self.counters.send_disconnects.load(Ordering::Relaxed),
        }
    }

    /// Reports a P2P training window start.
    pub fn send_window_started(&self, event: P2pWindowStarted) -> anyhow::Result<()> {
        self.send(P2pTrainingCommand::WindowStarted(event))
    }

    /// Reports a P2P training window completion.
    pub fn send_window_finished(&self, event: P2pWindowFinished) -> anyhow::Result<()> {
        self.send(P2pTrainingCommand::WindowFinished(event))
    }

    /// Reports canonical-head reconciliation.
    pub fn send_canonical_reconcile(
        &self,
        event: P2pCanonicalReconcileEvent,
    ) -> anyhow::Result<()> {
        self.send(P2pTrainingCommand::CanonicalReconcile(event))
    }

    /// Reports one effective capability assessment.
    pub fn send_capability(&self, event: P2pCapabilityAssessment) -> anyhow::Result<()> {
        self.send(P2pTrainingCommand::Capability(event))
    }

    /// Reports one accepted context-bound update.
    pub fn send_context_update(&self, event: P2pContextUpdateObserved) -> anyhow::Result<()> {
        self.send(P2pTrainingCommand::ContextUpdate(event))
    }
}

#[derive(Clone, Debug)]
/// Bridges native `burn_p2p` window callbacks into one run-scoped ECS ingress.
pub struct P2pTrainingEcsObserver {
    run_id: TrainingRunId,
    bus: P2pTrainingEventBus,
}

impl P2pTrainingEcsObserver {
    /// Binds a generic P2P runtime observer to one ECS training run.
    pub fn new(run_id: impl Into<TrainingRunId>, bus: P2pTrainingEventBus) -> Self {
        Self {
            run_id: run_id.into(),
            bus,
        }
    }
}

impl TrainingWindowObserver for P2pTrainingEcsObserver {
    fn window_started(&self, event: &TrainingWindowStartedEvent) {
        let _ = self.bus.send_window_started(P2pWindowStarted {
            run_id: self.run_id.clone(),
            experiment_id: event.experiment_id.as_str().to_owned(),
            revision_id: event.revision_id.as_str().to_owned(),
            window_id: event.window_id.0,
            base_head_id: Some(event.base_head_id.as_str().to_owned()),
            canonical_head_id: Some(event.base_head_id.as_str().to_owned()),
            training_head_id: None,
        });
    }

    fn window_completed(&self, event: &TrainingWindowCompletedEvent) {
        let metrics = event
            .metrics
            .iter()
            .filter_map(|(name, value)| match value {
                MetricValue::Float(value) if value.is_finite() => Some((name.clone(), *value)),
                MetricValue::Integer(value) => Some((name.clone(), *value as f64)),
                MetricValue::Bool(_) | MetricValue::Text(_) | MetricValue::Float(_) => None,
            })
            .collect();
        let _ = self.bus.send_window_finished(P2pWindowFinished {
            run_id: self.run_id.clone(),
            experiment_id: event.experiment_id.as_str().to_owned(),
            revision_id: event.revision_id.as_str().to_owned(),
            window_id: event.window_id.0,
            head_id: event.head_id.as_str().to_owned(),
            artifact_id: event.artifact_id.as_str().to_owned(),
            data_fetch_time_ms: event.data_fetch_time_ms,
            publish_latency_ms: event.publish_latency_ms,
            metrics,
        });
        if let Some(context) = event.routing_context.as_ref() {
            let _ = self.bus.send_context_update(P2pContextUpdateObserved {
                run_id: self.run_id.clone(),
                window_id: event.window_id.0,
                context: context.clone(),
            });
        }
    }
}

#[derive(Resource)]
struct P2pTrainingIngress {
    receiver: Arc<Mutex<Receiver<P2pTrainingCommand>>>,
    counters: Arc<P2pTrainingIngressCounters>,
    max_events_per_update: usize,
}

/// Adds [`P2pTrainingPlugin`] plus a bounded typed event ingress.
pub struct P2pTrainingIngressPlugin {
    ingress: P2pTrainingIngress,
}

impl P2pTrainingIngressPlugin {
    /// Creates a bounded ingress plugin and its producer handle.
    pub fn channel(capacity: usize) -> (Self, P2pTrainingEventBus) {
        let capacity = capacity.max(1);
        let (sender, receiver) = sync_channel(capacity);
        let counters = Arc::new(P2pTrainingIngressCounters::default());
        (
            Self {
                ingress: P2pTrainingIngress {
                    receiver: Arc::new(Mutex::new(receiver)),
                    counters: Arc::clone(&counters),
                    max_events_per_update: DEFAULT_MAX_EVENTS_PER_UPDATE,
                },
            },
            P2pTrainingEventBus {
                sender,
                capacity,
                counters,
            },
        )
    }

    /// Bounds how much ingress work one ECS update may consume.
    pub fn with_max_events_per_update(mut self, max_events: usize) -> Self {
        self.ingress.max_events_per_update = max_events.max(1);
        self
    }
}

impl Plugin for P2pTrainingIngressPlugin {
    fn build(&self, app: &mut App) {
        app.add_plugins(P2pTrainingPlugin)
            .insert_resource(P2pTrainingIngress {
                receiver: Arc::clone(&self.ingress.receiver),
                counters: Arc::clone(&self.ingress.counters),
                max_events_per_update: self.ingress.max_events_per_update,
            })
            .add_systems(
                Update,
                drain_p2p_training_ingress.in_set(TrainingSet::Input),
            );
    }
}

fn drain_p2p_training_ingress(
    ingress: Res<P2pTrainingIngress>,
    mut window_started: MessageWriter<P2pWindowStarted>,
    mut window_finished: MessageWriter<P2pWindowFinished>,
    mut reconcile: MessageWriter<P2pCanonicalReconcileEvent>,
    mut capability: MessageWriter<P2pCapabilityAssessment>,
    mut context_update: MessageWriter<P2pContextUpdateObserved>,
) {
    let Ok(receiver) = ingress.receiver.lock() else {
        return;
    };
    for _ in 0..ingress.max_events_per_update {
        match receiver.try_recv() {
            Ok(P2pTrainingCommand::WindowStarted(event)) => {
                ingress.counters.queue_depth.fetch_sub(1, Ordering::AcqRel);
                window_started.write(event);
            }
            Ok(P2pTrainingCommand::WindowFinished(event)) => {
                ingress.counters.queue_depth.fetch_sub(1, Ordering::AcqRel);
                window_finished.write(event);
            }
            Ok(P2pTrainingCommand::CanonicalReconcile(event)) => {
                ingress.counters.queue_depth.fetch_sub(1, Ordering::AcqRel);
                reconcile.write(event);
            }
            Ok(P2pTrainingCommand::Capability(event)) => {
                ingress.counters.queue_depth.fetch_sub(1, Ordering::AcqRel);
                capability.write(event);
            }
            Ok(P2pTrainingCommand::ContextUpdate(event)) => {
                ingress.counters.queue_depth.fetch_sub(1, Ordering::AcqRel);
                context_update.write(event);
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use burn_ecs::{TrainingAppExt, TrainingPlugins, TrainingRunConfig, TrainingRuntime};

    use super::*;

    fn runtime(
        plugin: P2pTrainingIngressPlugin,
    ) -> (tempfile::TempDir, TrainingRuntime, bevy_ecs::entity::Entity) {
        let dir = tempfile::tempdir().expect("dir");
        let mut app = App::new();
        app.add_plugins(TrainingPlugins).add_plugins(plugin);
        let run = app
            .try_add_training_run(TrainingRunConfig::new("p2p", "p2p", dir.path(), 1))
            .expect("run");
        (dir, TrainingRuntime::new(app), run)
    }

    #[test]
    fn bounded_ingress_drives_run_scoped_p2p_state() {
        let (plugin, bus) = P2pTrainingIngressPlugin::channel(8);
        let (_dir, mut runtime, run) = runtime(plugin);
        bus.send_capability(P2pCapabilityAssessment {
            run_id: "p2p".into(),
            participation: burn_ecs::PipelineParticipation::Trainer,
            compute: burn_ecs::PipelineComputeClass::Accelerator,
            supported_participation: std::collections::BTreeSet::from([
                burn_ecs::PipelineParticipation::Observer,
                burn_ecs::PipelineParticipation::Trainer,
            ]),
            reason: "probe succeeded".into(),
        })
        .expect("capability ingress");
        bus.send_window_finished(P2pWindowFinished {
            run_id: "p2p".into(),
            experiment_id: "experiment".into(),
            revision_id: "revision".into(),
            window_id: 3,
            head_id: "head".into(),
            artifact_id: "artifact".into(),
            data_fetch_time_ms: 2,
            publish_latency_ms: 4,
            metrics: vec![("loss".into(), 0.5)],
        })
        .expect("window ingress");

        for _ in 0..3 {
            runtime.update();
        }
        let entity = runtime.app().world().entity(run);
        let capability = entity
            .get::<burn_ecs::PipelineCapabilityState>()
            .expect("capability");
        assert_eq!(
            capability.participation,
            burn_ecs::PipelineParticipation::Trainer
        );
        let telemetry = entity
            .get::<super::super::P2pTrainingTelemetryState>()
            .expect("P2P telemetry");
        assert_eq!(telemetry.latest_window_id, Some(3));
    }

    #[test]
    fn native_window_observer_drives_the_same_typed_ingress() {
        let (plugin, bus) = P2pTrainingIngressPlugin::channel(8);
        let observer = P2pTrainingEcsObserver::new("p2p", bus.clone());
        let (_dir, mut runtime, run) = runtime(plugin);
        let now = chrono::Utc::now();
        observer.window_started(&TrainingWindowStartedEvent {
            study_id: crate::StudyId::new("study"),
            experiment_id: crate::ExperimentId::new("experiment"),
            revision_id: crate::RevisionId::new("revision"),
            window_id: crate::WindowId(4),
            base_head_id: crate::HeadId::new("base"),
            started_at: now,
        });
        observer.window_completed(&TrainingWindowCompletedEvent {
            study_id: crate::StudyId::new("study"),
            experiment_id: crate::ExperimentId::new("experiment"),
            revision_id: crate::RevisionId::new("revision"),
            window_id: crate::WindowId(4),
            base_head_id: crate::HeadId::new("base"),
            head_id: crate::HeadId::new("head"),
            artifact_id: crate::ArtifactId::new("artifact"),
            routing_context: Some(crate::UpdateRoutingContext {
                context_family_hash: crate::ContentId::new("family"),
                slot: 2,
                generation: 5,
                parameter_catalog_hash: crate::ContentId::new("catalog"),
            }),
            started_at: now,
            completed_at: now,
            data_fetch_time_ms: 3,
            publish_latency_ms: 5,
            metrics: std::collections::BTreeMap::from([(
                "loss".into(),
                crate::MetricValue::Float(0.25),
            )]),
        });

        for _ in 0..3 {
            runtime.update();
        }
        let entity = runtime.app().world().entity(run);
        let telemetry = entity
            .get::<super::super::P2pTrainingTelemetryState>()
            .expect("P2P telemetry");
        assert_eq!(telemetry.latest_window_id, Some(4));
        assert_eq!(telemetry.published_windows, 1);
        assert_eq!(bus.stats().sends_accepted, 3);
    }

    #[test]
    fn bounded_ingress_reports_saturation_and_drain() {
        let (plugin, bus) = P2pTrainingIngressPlugin::channel(1);
        let (_dir, mut runtime, _run) = runtime(plugin);
        let event = P2pCapabilityAssessment {
            run_id: "p2p".into(),
            participation: burn_ecs::PipelineParticipation::Observer,
            compute: burn_ecs::PipelineComputeClass::None,
            supported_participation: std::collections::BTreeSet::from([
                burn_ecs::PipelineParticipation::Observer,
            ]),
            reason: "bounded ingress test".into(),
        };

        bus.send_capability(event.clone()).expect("first event");
        let error = bus
            .send_capability(event)
            .expect_err("second event must observe full ingress");
        assert!(error.to_string().contains("queue is full"));
        assert_eq!(
            bus.stats(),
            P2pTrainingEventBusStats {
                queue_capacity: 1,
                queue_depth: 1,
                queue_high_watermark: 1,
                send_attempts: 2,
                sends_accepted: 1,
                sends_full: 1,
                send_disconnects: 0,
            }
        );
        assert_eq!(bus.stats().utilization(), 1.0);

        runtime.update();
        assert_eq!(bus.stats().queue_depth, 0);
        assert_eq!(bus.stats().utilization(), 0.0);
    }
}
