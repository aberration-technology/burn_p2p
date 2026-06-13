use burn_ecs::prelude::{
    App, IntoScheduleConfigs, MessageReader, MessageWriter, Plugin, ResMut, TrainingSet, Update,
};

use super::messages::{P2pCanonicalReconcileEvent, P2pWindowFinished, P2pWindowStarted};
use super::resources::P2pTrainingTelemetryState;

pub struct P2pTrainingPlugin;

impl Plugin for P2pTrainingPlugin {
    fn build(&self, app: &mut App) {
        app.add_message::<P2pWindowStarted>()
            .add_message::<P2pWindowFinished>()
            .add_message::<P2pCanonicalReconcileEvent>()
            .init_resource::<P2pTrainingTelemetryState>()
            .add_systems(
                Update,
                (
                    mirror_p2p_window_started,
                    mirror_p2p_window_finished,
                    record_p2p_canonical_reconcile,
                )
                    .in_set(TrainingSet::Window),
            );
    }
}

fn mirror_p2p_window_started(
    mut messages: MessageReader<P2pWindowStarted>,
    mut telemetry: ResMut<P2pTrainingTelemetryState>,
    mut training_windows: MessageWriter<burn_ecs::TrainingWindowStarted>,
) {
    for event in messages.read() {
        telemetry.run_id = Some(event.run_id.clone());
        telemetry.experiment_id = Some(event.experiment_id.clone());
        telemetry.revision_id = Some(event.revision_id.clone());
        telemetry.latest_window_id = Some(event.window_id);
        telemetry.canonical_head_id = event.canonical_head_id.clone();
        telemetry.training_head_id = event.training_head_id.clone();
        training_windows.write(burn_ecs::TrainingWindowStarted {
            run_id: event.run_id.clone(),
            window_id: event.window_id,
            mode: "p2p".to_string(),
        });
    }
}

fn mirror_p2p_window_finished(
    mut messages: MessageReader<P2pWindowFinished>,
    mut telemetry: ResMut<P2pTrainingTelemetryState>,
    mut training_windows: MessageWriter<burn_ecs::TrainingWindowFinished>,
) {
    for event in messages.read() {
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
    mut telemetry: ResMut<P2pTrainingTelemetryState>,
) {
    for event in messages.read() {
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
    use burn_ecs::{TrainingAppBuilder, TrainingAppConfig, TrainingRunConfig};

    use super::*;

    #[test]
    fn p2p_plugin_tracks_window_state_and_mirrors_common_events() {
        let dir = tempfile::tempdir().expect("dir");
        let mut runtime = TrainingAppBuilder::new(TrainingAppConfig {
            run: TrainingRunConfig::new("p2p", "p2p", dir.path(), 1),
            ..TrainingAppConfig::default()
        })
        .with_plugin(P2pTrainingPlugin)
        .build()
        .expect("runtime");
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
        runtime.finish();
        let state = runtime
            .app()
            .world()
            .resource::<P2pTrainingTelemetryState>();
        assert_eq!(state.latest_window_id, Some(7));
        assert_eq!(state.latest_head_id.as_deref(), Some("head"));
        assert_eq!(state.published_windows, 1);
        let jsonl =
            std::fs::read_to_string(dir.path().join("events/training_events.jsonl")).unwrap();
        assert!(jsonl.contains("window_started"));
        assert!(jsonl.contains("window_finished"));
    }
}
