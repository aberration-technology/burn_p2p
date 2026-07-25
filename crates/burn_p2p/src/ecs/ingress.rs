use std::sync::{
    Arc, Mutex,
    mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel},
};

use burn_ecs::bevy_ecs;
use burn_ecs::prelude::{
    App, IntoScheduleConfigs, MessageWriter, Plugin, Res, Resource, TrainingSet, Update,
};

use crate::{
    MetricValue, TrainingWindowCompletedEvent, TrainingWindowObserver, TrainingWindowStartedEvent,
};

use super::{
    P2pCanonicalReconcileEvent, P2pCapabilityAssessment, P2pTrainingPlugin, P2pWindowFinished,
    P2pWindowStarted,
};

const DEFAULT_MAX_EVENTS_PER_UPDATE: usize = 256;

#[derive(Debug)]
enum P2pTrainingCommand {
    WindowStarted(P2pWindowStarted),
    WindowFinished(P2pWindowFinished),
    CanonicalReconcile(P2pCanonicalReconcileEvent),
    Capability(P2pCapabilityAssessment),
}

#[derive(Clone, Debug)]
/// Bounded, non-blocking producer for a P2P training ECS app.
pub struct P2pTrainingEventBus {
    sender: SyncSender<P2pTrainingCommand>,
}

impl P2pTrainingEventBus {
    fn send(&self, command: P2pTrainingCommand) -> anyhow::Result<()> {
        self.sender.try_send(command).map_err(|error| match error {
            TrySendError::Full(_) => {
                anyhow::anyhow!("P2P training ECS ingress queue is full")
            }
            TrySendError::Disconnected(_) => {
                anyhow::anyhow!("P2P training ECS ingress is disconnected")
            }
        })
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
}

#[derive(Clone, Debug)]
/// Bridges native `burn_p2p` window callbacks into one run-scoped ECS ingress.
pub struct P2pTrainingEcsObserver {
    run_id: String,
    bus: P2pTrainingEventBus,
}

impl P2pTrainingEcsObserver {
    /// Binds a generic P2P runtime observer to one ECS training run.
    pub fn new(run_id: impl Into<String>, bus: P2pTrainingEventBus) -> Self {
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
    }
}

#[derive(Resource)]
struct P2pTrainingIngress {
    receiver: Arc<Mutex<Receiver<P2pTrainingCommand>>>,
    max_events_per_update: usize,
}

/// Adds [`P2pTrainingPlugin`] plus a bounded typed event ingress.
pub struct P2pTrainingIngressPlugin {
    ingress: P2pTrainingIngress,
}

impl P2pTrainingIngressPlugin {
    /// Creates a bounded ingress plugin and its producer handle.
    pub fn channel(capacity: usize) -> (Self, P2pTrainingEventBus) {
        let (sender, receiver) = sync_channel(capacity.max(1));
        (
            Self {
                ingress: P2pTrainingIngress {
                    receiver: Arc::new(Mutex::new(receiver)),
                    max_events_per_update: DEFAULT_MAX_EVENTS_PER_UPDATE,
                },
            },
            P2pTrainingEventBus { sender },
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
) {
    let Ok(receiver) = ingress.receiver.lock() else {
        return;
    };
    for _ in 0..ingress.max_events_per_update {
        match receiver.try_recv() {
            Ok(P2pTrainingCommand::WindowStarted(event)) => {
                window_started.write(event);
            }
            Ok(P2pTrainingCommand::WindowFinished(event)) => {
                window_finished.write(event);
            }
            Ok(P2pTrainingCommand::CanonicalReconcile(event)) => {
                reconcile.write(event);
            }
            Ok(P2pTrainingCommand::Capability(event)) => {
                capability.write(event);
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use burn_ecs::{TrainingAppBuilder, TrainingAppConfig, TrainingRunConfig};

    use super::*;

    #[test]
    fn bounded_ingress_drives_run_scoped_p2p_state() {
        let dir = tempfile::tempdir().expect("dir");
        let (plugin, bus) = P2pTrainingIngressPlugin::channel(8);
        let mut runtime = TrainingAppBuilder::new(TrainingAppConfig {
            run: TrainingRunConfig::new("p2p", "p2p", dir.path(), 1),
            ..TrainingAppConfig::default()
        })
        .with_plugin(plugin)
        .build()
        .expect("runtime");
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
        let run = runtime
            .app()
            .world()
            .resource::<burn_ecs::TrainingRunEntity>()
            .0;
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
        let dir = tempfile::tempdir().expect("dir");
        let (plugin, bus) = P2pTrainingIngressPlugin::channel(8);
        let observer = P2pTrainingEcsObserver::new("p2p", bus);
        let mut runtime = TrainingAppBuilder::new(TrainingAppConfig {
            run: TrainingRunConfig::new("p2p", "p2p", dir.path(), 1),
            ..TrainingAppConfig::default()
        })
        .with_plugin(plugin)
        .build()
        .expect("runtime");
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
        let run = runtime
            .app()
            .world()
            .resource::<burn_ecs::TrainingRunEntity>()
            .0;
        let entity = runtime.app().world().entity(run);
        let telemetry = entity
            .get::<super::super::P2pTrainingTelemetryState>()
            .expect("P2P telemetry");
        assert_eq!(telemetry.latest_window_id, Some(4));
        assert_eq!(telemetry.published_windows, 1);
    }
}
