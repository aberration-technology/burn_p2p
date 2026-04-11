use super::*;

impl<P> RunningNode<P> {
    pub(super) fn reap_training_prefetch(&mut self) {
        let Some(prefetch) = self.training_prefetch.as_ref() else {
            return;
        };
        let Some(handle) = prefetch.handle.as_ref() else {
            return;
        };
        if !handle.is_finished() {
            return;
        }

        let prefetch = self
            .training_prefetch
            .as_mut()
            .expect("finished prefetch task should remain present");
        if let Some(handle) = prefetch.handle.take() {
            let _ = handle.join();
        }
    }

    pub(super) fn take_ready_training_prefetch(
        &mut self,
        experiment: &ExperimentHandle,
        local_peer_id: &PeerId,
        window_id: WindowId,
        budget_work_units: u64,
        available_microshards: &[MicroShard],
    ) -> Option<PlannedLease> {
        self.reap_training_prefetch();
        let prefetch = self.training_prefetch.as_ref()?;
        if !prefetched_lease_is_reusable(
            prefetch,
            experiment,
            local_peer_id,
            window_id,
            budget_work_units,
            available_microshards,
        ) {
            return None;
        }

        const PREFETCH_REUSE_GRACE: Duration = Duration::from_millis(250);
        if let Some(handle) = prefetch.handle.as_ref()
            && !wait_for_prefetch_completion(handle, PREFETCH_REUSE_GRACE)
        {
            return None;
        }
        self.reap_training_prefetch();

        let mut prefetch = self
            .training_prefetch
            .take()
            .expect("reusable prefetch task should remain present");
        if let Some(handle) = prefetch.handle.take() {
            let _ = handle.join();
        }
        Some(prefetch.planned_lease)
    }

    pub(super) fn maybe_start_next_window_prefetch(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &TrainingPreparedState,
        planned: &PlannedTrainingWindow,
        capability: &CapabilityEstimate,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<()>
    where
        P: P2pWorkload,
    {
        self.reap_training_prefetch();
        if self.training_prefetch.as_ref().is_some_and(|prefetch| {
            prefetch.planned_lease.lease.study_id == experiment.study_id
                && prefetch.planned_lease.lease.experiment_id == experiment.experiment_id
                && prefetch.planned_lease.lease.revision_id == experiment.revision_id
                && prefetch.planned_lease.lease.window_id > planned.window_id
                && prefetch.planned_lease.lease.expires_at > Utc::now()
        }) {
            return Ok(());
        }
        if let Some(mut stale_prefetch) = self.training_prefetch.take()
            && let Some(handle) = stale_prefetch.handle.take()
            && handle.is_finished()
        {
            let _ = handle.join();
        }

        let directory_entry = active_experiment_directory_entry(
            &prepared.node_config,
            &prepared.telemetry_snapshot,
            experiment,
        );
        let adaptation_factor =
            local_training_adaptation_factor(&prepared.telemetry_snapshot, &prepared.local_peer_id);
        let schedule_hint =
            local_training_schedule_hint(&prepared.telemetry_snapshot, &prepared.local_peer_id);
        let placement_budget_work_units = training_placement_budget_work_units(
            planned
                .limit_profile
                .recommended_budget
                .budget_work_units
                .max(1),
            capability,
            directory_entry.as_ref(),
            adaptation_factor,
            schedule_hint.budget_scale,
        )?;
        let assignment_peers = runtime_training_assignment_peers(
            &prepared.telemetry_snapshot,
            &prepared.mainnet_roles,
            &prepared.local_peer_id,
        );
        let prefetch_lease = plan_prefetch_lease_for_window(PrefetchLeasePlanArgs {
            network_id: &prepared.network_id,
            experiment,
            dataset_view: &planned.registration.view,
            local_peer_id: &prepared.local_peer_id,
            assignment_peers: &assignment_peers,
            microshards: &planned.microshard_plan.microshards,
            window_id: WindowId(planned.window_id.0 + 1),
            budget_work_units: placement_budget_work_units,
            adaptation_factor,
        })?;
        let cached_ids = cached_microshards
            .iter()
            .map(|microshard| microshard.microshard.microshard_id.clone())
            .collect::<BTreeSet<_>>();
        let should_prefetch_shards = supports_background_shard_prefetch(&planned.limit_profile)
            && !prefetch_lease
                .lease
                .microshards
                .iter()
                .all(|microshard_id| cached_ids.contains(microshard_id));
        let handle = if should_prefetch_shards {
            let cache_dir = prepared.storage.dataset_cache_dir();
            let registration = planned.registration.clone();
            let microshard_plan = planned.microshard_plan.clone();
            let lease = prefetch_lease.lease.clone();
            Some(
                std::thread::Builder::new()
                    .name("burn-p2p-train-prefetch".into())
                    .spawn(move || {
                        let _ = ShardCache::new(cache_dir).fetch_lease_microshards(
                            &registration,
                            &microshard_plan,
                            &lease,
                        );
                    })
                    .map_err(|error| {
                        anyhow::anyhow!("failed to spawn training prefetch task: {error}")
                    })?,
            )
        } else {
            None
        };
        self.training_prefetch = Some(TrainingPrefetchTask {
            planned_lease: prefetch_lease,
            handle,
        });
        Ok(())
    }
}
