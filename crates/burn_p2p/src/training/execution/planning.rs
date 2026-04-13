use super::*;

impl<P> RunningNode<P> {
    pub(super) fn plan_training_window(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &TrainingPreparedState,
        capability: &CapabilityEstimate,
    ) -> anyhow::Result<PlannedTrainingWindow>
    where
        P: P2pWorkload,
    {
        let calibrator = CapabilityCalibrator::new(runtime_limit_policy(capability))?;
        let limit_profile = effective_limit_profile(
            &prepared.storage,
            experiment,
            &prepared.local_peer_id,
            &prepared.node_config,
            capability,
            Utc::now(),
        )?;
        set_effective_limit_profile(&self.telemetry, Some(limit_profile.clone()));
        let directory_entry = active_experiment_directory_entry(
            &prepared.node_config,
            &prepared.telemetry_snapshot,
            experiment,
        );
        if let Some(entry) = directory_entry.as_ref() {
            ensure_training_placement_roles(&prepared.mainnet_roles, entry)?;
        }
        let adaptation_factor =
            local_training_adaptation_factor(&prepared.telemetry_snapshot, &prepared.local_peer_id);
        let window_id = inferred_next_window_id(
            &prepared.storage,
            experiment,
            prepared.current_head.as_ref().map(|(_, head)| head),
        )?;
        let schedule_hint = local_training_schedule_hint(
            &prepared.telemetry_snapshot,
            experiment,
            &prepared.local_peer_id,
            window_id,
        );
        let budget_work_units = training_placement_budget_work_units(
            limit_profile.recommended_budget.budget_work_units.max(1),
            capability,
            directory_entry.as_ref(),
            adaptation_factor,
            schedule_hint.budget_scale,
        )?;
        let registration = self
            .node
            .as_ref()
            .expect("running node should retain prepared node")
            .project
            .dataset_registration()?;
        let microshard_plan = self
            .node
            .as_ref()
            .expect("running node should retain prepared node")
            .project
            .microshard_plan(&registration)?;
        let overlays = experiment.overlay_set()?;
        let base_head_id = prepared
            .current_head
            .as_ref()
            .map(|(_, head)| head.head_id.clone())
            .unwrap_or_else(|| HeadId::new("genesis"));
        let topology_policy = runtime_merge_topology_policy(
            self.node
                .as_ref()
                .expect("running node should retain prepared node")
                .config(),
            &prepared.telemetry_snapshot,
            experiment,
            Some(&base_head_id),
        );
        let assignment_peers = runtime_training_assignment_peers(
            &prepared.telemetry_snapshot,
            &prepared.mainnet_roles,
            &prepared.local_peer_id,
        );
        let topology_peers = runtime_topology_peers(
            &prepared.telemetry_snapshot,
            &prepared.mainnet_roles,
            &prepared.local_peer_id,
        );
        let validator_peers = runtime_validator_peers(
            &prepared.telemetry_snapshot,
            &prepared.mainnet_roles,
            &prepared.local_peer_id,
        );
        let validators = if matches!(
            topology_policy.promotion_policy.mode,
            HeadPromotionMode::ReducerAuthority
        ) {
            Vec::new()
        } else {
            runtime_validators(
                &prepared.mainnet_roles,
                &prepared.local_peer_id,
                &validator_peers,
                topology_policy.promotion_policy.validator_quorum,
            )
        };
        let merge_window = latest_merge_window_from_snapshot(
            &prepared.telemetry_snapshot.control_plane,
            experiment,
            Some(&base_head_id),
        )
        .filter(|merge_window| merge_window.window_id == window_id)
        .unwrap_or(open_runtime_merge_window(
            experiment,
            window_id,
            base_head_id.clone(),
            topology_policy.clone(),
            topology_peers.clone(),
            validators,
        )?);
        let reducer_assignment = latest_reducer_assignment_from_snapshot(
            &prepared.telemetry_snapshot.control_plane,
            window_id,
            &prepared.local_peer_id,
        )
        .unwrap_or(runtime_assign_reducers(
            &merge_window,
            &prepared.local_peer_id,
            &topology_peers,
        )?);
        let lease_microshards = unleased_microshards_for_window(
            &prepared.telemetry_snapshot,
            experiment,
            window_id,
            &prepared.local_peer_id,
            &microshard_plan.microshards,
        );
        let lease_microshards = if lease_microshards.len() == microshard_plan.microshards.len() {
            preferred_microshards_for_peer(
                &assignment_peers,
                &prepared.local_peer_id,
                &lease_microshards,
            )
        } else {
            lease_microshards
        };
        let budget_work_units = fair_share_budget_work_units(
            assignment_peers.len(),
            lease_microshards.len(),
            budget_work_units,
        );
        let lease = if let Some(prefetched_lease) = self.take_ready_training_prefetch(
            experiment,
            &prepared.local_peer_id,
            window_id,
            budget_work_units,
            &lease_microshards,
        ) {
            prefetched_lease
        } else {
            let mut lease_planner = LeasePlanner::default();
            lease_planner.config.max_microshards_per_lease = lease_planner
                .config
                .max_microshards_per_lease
                .min(adaptive_microshard_cap(
                    scheduled_microshard_cap(
                        fair_share_microshard_cap(assignment_peers.len(), lease_microshards.len()),
                        schedule_hint.microshard_scale,
                    ),
                    adaptation_factor,
                ));
            lease_planner.plan_lease(
                prepared.network_id.clone(),
                experiment.study_id.clone(),
                experiment.experiment_id.clone(),
                experiment.revision_id.clone(),
                &registration.view,
                prepared.local_peer_id.clone(),
                window_id,
                Utc::now(),
                budget_work_units,
                &lease_microshards,
            )?
        };
        let lease_announcement = LeaseAnnouncement {
            overlay: overlays.leases.clone(),
            lease: lease.lease.clone(),
            announced_at: Utc::now(),
        };
        self.control.publish_lease(lease_announcement.clone())?;
        persist_json(
            prepared.storage.scoped_lease_path(
                &lease_announcement.lease.study_id,
                &lease_announcement.lease.experiment_id,
                &lease_announcement.lease.revision_id,
            ),
            &lease_announcement,
        )?;

        Ok(PlannedTrainingWindow {
            calibrator,
            limit_profile,
            registration,
            microshard_plan,
            lease,
            window_id,
            base_head_id,
            merge_window,
            reducer_assignment,
        })
    }
}
