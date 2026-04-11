use super::*;

impl<P> RunningNode<P> {
    pub(in crate::training) fn publish_training_execution<T, M>(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &TrainingPreparedState,
        execution: &TrainingExecution<T, M>,
    ) -> anyhow::Result<u64> {
        let publish_started_at = Utc::now();
        persist_limit_profile(&prepared.storage, experiment, &execution.limit_profile)?;
        set_effective_limit_profile(&self.telemetry, Some(execution.limit_profile.clone()));
        persist_json(
            prepared
                .storage
                .scoped_receipt_path(&execution.contribution.receipt_id),
            &execution.contribution,
        )?;
        persist_json(
            prepared.storage.scoped_head_path(&execution.head.head_id),
            &execution.head,
        )?;
        persist_window_id(&prepared.storage, experiment, execution.window_id)?;
        self.update_runtime_state(
            NodeRuntimeState::PublishingUpdate,
            Some(SlotRuntimeState::Publishing(prepared.assignment.clone())),
        );

        let overlays = experiment.overlay_set()?;
        self.control.publish_merge_window(MergeWindowAnnouncement {
            overlay: overlays.heads.clone(),
            merge_window: execution.merge_window.clone(),
            announced_at: Utc::now(),
        })?;
        self.control
            .publish_reducer_assignment(ReducerAssignmentAnnouncement {
                overlay: overlays.heads.clone(),
                assignment: execution.reducer_assignment.clone(),
                announced_at: Utc::now(),
            })?;
        self.publish_artifact_from_store(&execution.artifact.artifact_id)?;
        self.control.publish_update(UpdateEnvelopeAnnouncement {
            overlay: overlays.heads.clone(),
            update: UpdateAnnounce {
                peer_id: prepared.local_peer_id.clone(),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: execution.window_id,
                base_head_id: execution.base_head_id.clone(),
                lease_id: Some(execution.lease.lease_id.clone()),
                delta_artifact_id: execution.artifact.artifact_id.clone(),
                sample_weight: execution.contribution.accepted_weight,
                quality_weight: (1.0
                    / (1.0 + metric_quality(&execution.contribution.metrics).abs()))
                .max(0.01),
                norm_stats: update_norm_stats(&execution.contribution.metrics),
                feature_sketch: Some(update_feature_sketch_from_metrics(
                    &execution.contribution.metrics,
                    prepared
                        .current_head
                        .as_ref()
                        .map(|(_, head)| &head.metrics),
                    prepared
                        .robustness_policy
                        .screening_policy
                        .sketch_dimensionality as usize,
                    0,
                    0,
                    None,
                )),
                receipt_root: ContentId::derive(&[execution.contribution.receipt_id.as_str()])?,
                receipt_ids: vec![execution.contribution.receipt_id.clone()],
                providers: vec![prepared.local_peer_id.clone()],
                announced_at: Utc::now(),
            },
        })?;
        self.control.publish_head(HeadAnnouncement {
            overlay: overlays.heads,
            provider_peer_id: Some(prepared.local_peer_id.clone()),
            head: execution.head.clone(),
            announced_at: Utc::now(),
        })?;
        let publish_finished_at = Utc::now();
        let publish_latency_ms = (publish_finished_at - publish_started_at)
            .num_milliseconds()
            .max(0) as u64;
        let peer_window_metrics = build_training_peer_window_metrics(TrainingMetricBuildArgs {
            config: &prepared.node_config,
            experiment,
            local_peer_id: &prepared.local_peer_id,
            limit_profile: &execution.limit_profile,
            lease: &execution.lease,
            base_head_id: &execution.base_head_id,
            report: &execution.report,
            contribution: &execution.contribution,
            window_started_at: execution.window_started_at,
            data_fetch_time_ms: execution.data_fetch_time_ms,
            publish_latency_ms,
            head_lag_at_start: prepared.telemetry_snapshot.head_lag_steps,
            head_lag_at_finish: prepared.telemetry_snapshot.head_lag_steps,
        });
        persist_peer_window_metrics(
            &prepared.storage,
            experiment,
            &peer_window_metrics,
            prepared.metrics_retention,
        )?;
        let peer_window_hint = build_peer_window_placement_hint(&peer_window_metrics);
        self.control.publish_metrics(build_metrics_announcement(
            experiment,
            overlays.metrics,
            MetricsLiveEventKind::LedgerAppend,
            None,
            Some(execution.merge_window.merge_window_id.clone()),
            vec![peer_window_hint],
        ))?;
        self.update_runtime_state(
            NodeRuntimeState::WaitingMerge,
            Some(SlotRuntimeState::CoolingDown(prepared.assignment.clone())),
        );

        Ok(publish_latency_ms)
    }
}
