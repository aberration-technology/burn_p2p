use burn_ecs::bevy_ecs::prelude::{With, Without};
use burn_ecs::prelude::{
    App, Commands, Entity, IntoScheduleConfigs, MessageReader, MessageWriter, Plugin, Query,
    ResMut, TrainingRunId, TrainingSet, TrainingWindowIndex, Update,
};

use super::messages::{
    P2pCanonicalReconcileEvent, P2pCapabilityAssessment, P2pWindowFinished, P2pWindowStarted,
};
use super::resources::{P2pTrainingTelemetryState, P2pWindowMetadata, PendingP2pWindowMetadata};

pub struct P2pTrainingPlugin;

impl Plugin for P2pTrainingPlugin {
    fn build(&self, app: &mut App) {
        app.add_message::<P2pWindowStarted>()
            .add_message::<P2pWindowFinished>()
            .add_message::<P2pCanonicalReconcileEvent>()
            .add_message::<P2pCapabilityAssessment>()
            .init_resource::<PendingP2pWindowMetadata>()
            .add_systems(
                Update,
                (
                    attach_p2p_run_state,
                    mirror_p2p_window_started,
                    mirror_p2p_window_finished,
                    record_p2p_canonical_reconcile,
                    bridge_p2p_capability_assessments,
                )
                    .chain()
                    .in_set(TrainingSet::Window),
            )
            .add_systems(
                Update,
                attach_p2p_window_metadata.in_set(TrainingSet::Control),
            );
    }
}

fn bridge_p2p_capability_assessments(
    mut messages: MessageReader<P2pCapabilityAssessment>,
    mut transitions: MessageWriter<burn_ecs::PipelineCapabilityTransitionRequest>,
    mut runs: Query<(
        &TrainingRunId,
        &mut burn_ecs::PipelineCapabilityState,
        &mut P2pTrainingTelemetryState,
    )>,
) {
    for assessment in messages.read() {
        let Some((_, mut capability, mut telemetry)) = runs
            .iter_mut()
            .find(|(run_id, ..)| *run_id == &assessment.run_id)
        else {
            continue;
        };
        capability.supported_participation = assessment.supported_participation.clone();
        telemetry.participation = Some(assessment.participation);
        telemetry.compute = Some(assessment.compute);
        telemetry.capability_transitions = telemetry.capability_transitions.saturating_add(1);
        transitions.write(burn_ecs::PipelineCapabilityTransitionRequest {
            run_id: assessment.run_id.clone(),
            expected_revision: capability.revision,
            target: assessment.participation,
            compute: assessment.compute,
            reason: assessment.reason.clone(),
        });
    }
}

fn attach_p2p_run_state(
    mut commands: Commands,
    runs: Query<Entity, (With<TrainingRunId>, Without<P2pTrainingTelemetryState>)>,
) {
    for entity in &runs {
        commands
            .entity(entity)
            .insert(P2pTrainingTelemetryState::default());
    }
}

fn mirror_p2p_window_started(
    mut messages: MessageReader<P2pWindowStarted>,
    mut runs: Query<(&TrainingRunId, &mut P2pTrainingTelemetryState)>,
    mut pending: ResMut<PendingP2pWindowMetadata>,
    mut training_windows: MessageWriter<burn_ecs::TrainingWindowStarted>,
) {
    for event in messages.read() {
        let Some((_, mut telemetry)) = runs
            .iter_mut()
            .find(|(run_id, ..)| *run_id == &event.run_id)
        else {
            continue;
        };
        telemetry.run_id = Some(event.run_id.clone());
        telemetry.experiment_id = Some(event.experiment_id.clone());
        telemetry.revision_id = Some(event.revision_id.clone());
        telemetry.latest_window_id = Some(event.window_id);
        telemetry.canonical_head_id = event.canonical_head_id.clone();
        telemetry.training_head_id = event.training_head_id.clone();
        pending.by_run_window_id.insert(
            (event.run_id.clone(), event.window_id),
            P2pWindowMetadata {
                experiment_id: event.experiment_id.clone(),
                revision_id: event.revision_id.clone(),
                base_head_id: event.base_head_id.clone(),
                canonical_head_id: event.canonical_head_id.clone(),
                training_head_id: event.training_head_id.clone(),
            },
        );
        training_windows.write(burn_ecs::TrainingWindowStarted {
            run_id: event.run_id.clone(),
            window_id: event.window_id,
            mode: "p2p".to_string(),
        });
    }
}

fn attach_p2p_window_metadata(
    mut commands: Commands,
    windows: Query<(&TrainingRunId, &TrainingWindowIndex)>,
    mut pending: ResMut<PendingP2pWindowMetadata>,
) {
    let attached = pending
        .by_run_window_id
        .iter()
        .filter_map(|((run_id, window_id), metadata)| {
            let (_, windows) = windows
                .iter()
                .find(|(candidate_run_id, _)| *candidate_run_id == run_id)?;
            let entity = windows.get(*window_id)?;
            commands.entity(entity).insert(metadata.clone());
            Some((run_id.clone(), *window_id))
        })
        .collect::<Vec<_>>();
    for key in attached {
        pending.by_run_window_id.remove(&key);
    }
}

fn mirror_p2p_window_finished(
    mut messages: MessageReader<P2pWindowFinished>,
    mut runs: Query<(&TrainingRunId, &mut P2pTrainingTelemetryState)>,
    mut training_windows: MessageWriter<burn_ecs::TrainingWindowFinished>,
) {
    for event in messages.read() {
        let Some((_, mut telemetry)) = runs
            .iter_mut()
            .find(|(run_id, ..)| *run_id == &event.run_id)
        else {
            continue;
        };
        telemetry.run_id = Some(event.run_id.clone());
        telemetry.experiment_id = Some(event.experiment_id.clone());
        telemetry.revision_id = Some(event.revision_id.clone());
        telemetry.latest_window_id = Some(event.window_id);
        telemetry.latest_head_id = Some(event.head_id.clone());
        telemetry.latest_artifact_id = Some(event.artifact_id.clone());
        telemetry.training_head_id = Some(event.head_id.clone());
        telemetry.published_windows = telemetry.published_windows.saturating_add(1);
        training_windows.write(burn_ecs::TrainingWindowFinished {
            run_id: event.run_id.clone(),
            window_id: event.window_id,
            metrics: event.metrics.clone(),
        });
    }
}

fn record_p2p_canonical_reconcile(
    mut messages: MessageReader<P2pCanonicalReconcileEvent>,
    mut runs: Query<(&TrainingRunId, &mut P2pTrainingTelemetryState)>,
) {
    for event in messages.read() {
        let Some((_, mut telemetry)) = runs
            .iter_mut()
            .find(|(run_id, ..)| *run_id == &event.run_id)
        else {
            continue;
        };
        telemetry.run_id = Some(event.run_id.clone());
        telemetry.experiment_id = Some(event.experiment_id.clone());
        telemetry.revision_id = Some(event.revision_id.clone());
        telemetry.canonical_head_id = Some(event.canonical_head_id.clone());
        telemetry.training_head_id = Some(event.canonical_head_id.clone());
        telemetry.reconciliations = telemetry.reconciliations.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use burn_ecs::{TrainingAppExt, TrainingPlugins, TrainingRunConfig, TrainingRuntime};

    use super::*;

    fn runtime() -> (tempfile::TempDir, TrainingRuntime, Entity) {
        let dir = tempfile::tempdir().expect("dir");
        let mut app = App::new();
        app.add_plugins(TrainingPlugins)
            .add_plugins(P2pTrainingPlugin);
        let run = app
            .try_add_training_run(TrainingRunConfig::new("p2p", "p2p", dir.path(), 1))
            .expect("run");
        (dir, TrainingRuntime::new(app), run)
    }

    #[test]
    fn p2p_plugin_tracks_window_state_and_mirrors_common_events() {
        let (dir, mut runtime, run_entity) = runtime();
        runtime
            .app_mut()
            .world_mut()
            .write_message(P2pWindowStarted {
                run_id: "p2p".into(),
                experiment_id: "exp".into(),
                revision_id: "rev".into(),
                window_id: 7,
                base_head_id: Some("base".into()),
                canonical_head_id: Some("canon".into()),
                training_head_id: Some("train".into()),
            });
        runtime
            .app_mut()
            .world_mut()
            .write_message(P2pWindowFinished {
                run_id: "p2p".into(),
                experiment_id: "exp".into(),
                revision_id: "rev".into(),
                window_id: 7,
                head_id: "head".into(),
                artifact_id: "artifact".into(),
                data_fetch_time_ms: 11,
                publish_latency_ms: 13,
                metrics: vec![("loss".into(), 1.0)],
            });
        runtime.update();
        runtime.update();
        runtime.update();
        runtime.finish().expect("finish");
        let state = runtime
            .app()
            .world()
            .entity(run_entity)
            .get::<P2pTrainingTelemetryState>()
            .expect("run-scoped p2p telemetry");
        assert_eq!(state.latest_window_id, Some(7));
        assert_eq!(state.latest_head_id.as_deref(), Some("head"));
        assert_eq!(state.published_windows, 1);
        let window_entity = runtime
            .app()
            .world()
            .entity(run_entity)
            .get::<TrainingWindowIndex>()
            .expect("window index")
            .get(7)
            .expect("window entity");
        let metadata = runtime
            .app()
            .world()
            .entity(window_entity)
            .get::<P2pWindowMetadata>()
            .expect("p2p metadata");
        assert_eq!(metadata.experiment_id, "exp");
        let jsonl = std::fs::read_to_string(dir.path().join("events/training_events.jsonl"))
            .expect("training events jsonl");
        assert!(jsonl.contains("window_started"));
        assert!(jsonl.contains("window_finished"));
    }

    #[test]
    fn p2p_capability_assessment_supports_upgrade_and_read_only_downgrade() {
        let (_dir, mut runtime, run_entity) = runtime();
        runtime
            .app_mut()
            .world_mut()
            .write_message(P2pCapabilityAssessment {
                run_id: "p2p".into(),
                participation: burn_ecs::PipelineParticipation::Trainer,
                compute: burn_ecs::PipelineComputeClass::Accelerator,
                supported_participation: std::collections::BTreeSet::from([
                    burn_ecs::PipelineParticipation::Observer,
                    burn_ecs::PipelineParticipation::Trainer,
                ]),
                reason: "accelerator probe succeeded".into(),
            });
        runtime.update();
        runtime.update();
        let capability = runtime
            .app()
            .world()
            .entity(run_entity)
            .get::<burn_ecs::PipelineCapabilityState>()
            .expect("capability");
        assert_eq!(
            capability.participation,
            burn_ecs::PipelineParticipation::Trainer
        );

        runtime
            .app_mut()
            .world_mut()
            .write_message(P2pCapabilityAssessment {
                run_id: "p2p".into(),
                participation: burn_ecs::PipelineParticipation::Observer,
                compute: burn_ecs::PipelineComputeClass::None,
                supported_participation: std::collections::BTreeSet::from([
                    burn_ecs::PipelineParticipation::Observer,
                    burn_ecs::PipelineParticipation::Validator,
                ]),
                reason: "WebGPU unavailable".into(),
            });
        runtime.update();
        runtime.update();

        let capability = runtime
            .app()
            .world()
            .entity(run_entity)
            .get::<burn_ecs::PipelineCapabilityState>()
            .expect("capability");
        assert_eq!(
            capability.participation,
            burn_ecs::PipelineParticipation::Observer
        );
        assert!(capability.participation.is_read_only());
    }
}
