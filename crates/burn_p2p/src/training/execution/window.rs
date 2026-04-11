use super::*;

impl<P> RunningNode<P> {
    /// Creates a stateful continuous trainer with the default policy.
    pub fn continuous_trainer<'a>(
        &'a mut self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<ContinuousTrainer<'a, P>>
    where
        P: P2pWorkload,
    {
        self.continuous_trainer_with_policy(experiment, ContinuousTrainerPolicy::default())
    }

    /// Creates a stateful continuous trainer with one explicit policy.
    pub fn continuous_trainer_with_policy<'a>(
        &'a mut self,
        experiment: &ExperimentHandle,
        policy: ContinuousTrainerPolicy,
    ) -> anyhow::Result<ContinuousTrainer<'a, P>>
    where
        P: P2pWorkload,
    {
        ContinuousTrainer::new(self, experiment, policy)
    }

    /// Performs the train window once operation.
    pub fn train_window_once(
        &mut self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<TrainingWindowOutcome<P::WindowStats>>
    where
        P: P2pWorkload,
    {
        self.train_window_once_with_pinned_head(experiment, None)
    }

    /// Performs the train window once operation against an explicitly pinned
    /// base head that has already been synchronized locally.
    pub fn train_window_once_with_pinned_head(
        &mut self,
        experiment: &ExperimentHandle,
        pinned_head: Option<&HeadDescriptor>,
    ) -> anyhow::Result<TrainingWindowOutcome<P::WindowStats>>
    where
        P: P2pWorkload,
    {
        let prepared = self.prepare_training_state(experiment, pinned_head)?;
        let execution = self.execute_training_window(experiment, &prepared)?;
        let publish_latency_ms =
            self.publish_training_execution(experiment, &prepared, &execution)?;

        Ok(TrainingWindowOutcome {
            lease: execution.lease,
            head: execution.head,
            artifact: execution.artifact,
            contribution: execution.contribution,
            timing: TrainingWindowTiming {
                window_started_at: execution.window_started_at,
                completed_at: execution.report.completed_at,
                data_fetch_time_ms: execution.data_fetch_time_ms,
                publish_latency_ms,
            },
            report: execution.report,
        })
    }

    pub(in crate::training) fn prepare_training_state(
        &mut self,
        experiment: &ExperimentHandle,
        pinned_head: Option<&HeadDescriptor>,
    ) -> anyhow::Result<TrainingPreparedState> {
        let assignment = SlotAssignmentState::from_experiment(experiment);
        self.persist_primary_assignment(&assignment)?;
        self.update_runtime_state(
            NodeRuntimeState::LeasePending,
            Some(SlotRuntimeState::Assigned(assignment.clone())),
        );
        self.ensure_experiment_topics(experiment)?;

        let storage = self
            .config()
            .storage
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("training requires configured storage"))?;
        let store = FsArtifactStore::new(storage.root.clone());
        store.ensure_layout()?;

        let snapshots = self.fetch_experiment_snapshots(experiment, Duration::from_secs(3))?;
        let telemetry_snapshot = self.telemetry().snapshot();
        let lag_assessment = self.assess_and_record_lag(&storage, experiment, &snapshots)?;
        if matches!(
            lag_assessment.state,
            LagState::LeaseBlocked | LagState::RebaseRequired
        ) {
            let reason = runtime_blocked_reason("training", &lag_assessment);
            self.update_runtime_state(
                NodeRuntimeState::HeadSync,
                Some(SlotRuntimeState::Blocked {
                    assignment: Some(assignment.clone()),
                    reason: reason.clone(),
                }),
            );
            return Err(anyhow::anyhow!(reason));
        }

        let local_peer_id = self
            .telemetry()
            .snapshot()
            .local_peer_id
            .ok_or_else(|| anyhow::anyhow!("runtime does not have a local peer id yet"))?;
        let current_head = if let Some(head) = pinned_head.cloned() {
            anyhow::ensure!(
                head.study_id == experiment.study_id
                    && head.experiment_id == experiment.experiment_id
                    && head.revision_id == experiment.revision_id,
                "pinned training head {} does not belong to {}:{}:{}",
                head.head_id.as_str(),
                experiment.study_id.as_str(),
                experiment.experiment_id.as_str(),
                experiment.revision_id.as_str(),
            );
            Some((local_peer_id.clone(), head))
        } else {
            let canonical_snapshots = snapshots_with_local_control_plane(
                &snapshots,
                Some(&local_peer_id),
                &telemetry_snapshot.control_plane,
            );
            resolve_canonical_head(&storage, experiment, &canonical_snapshots)?.or_else(|| {
                latest_head_from_snapshot(telemetry_snapshot.control_plane.clone(), experiment)
            })
        };
        let network_id = self.mainnet().network_id().clone();
        let mut telemetry_snapshot = telemetry_snapshot;
        merge_connected_lease_announcements(&mut telemetry_snapshot.control_plane, &snapshots);
        let mainnet_roles = self.mainnet().roles.clone();
        let node_config = self.config().clone();
        let metrics_retention = node_config
            .metrics_retention
            .resolve_for_roles(&mainnet_roles);
        let robustness_policy =
            runtime_robustness_policy(&node_config, &telemetry_snapshot, experiment);

        self.update_runtime_state(
            NodeRuntimeState::HeadSync,
            Some(SlotRuntimeState::MaterializingBase(assignment.clone())),
        );
        const BASE_HEAD_SYNC_TIMEOUT: Duration = Duration::from_secs(5);
        if let Some((source_peer_id, source_head)) = current_head.as_ref()
            && !store.has_manifest(&source_head.artifact_id)
        {
            if pinned_head.is_some() && source_head.global_step > 0 {
                anyhow::bail!(
                    "pinned base head {} artifact {} was not present locally",
                    source_head.head_id.as_str(),
                    source_head.artifact_id.as_str(),
                );
            }
            if let Err(error) = self.sync_artifact_from_peer_bounded(
                source_peer_id,
                source_head.artifact_id.clone(),
                BASE_HEAD_SYNC_TIMEOUT,
            ) && source_head.global_step > 0
            {
                return Err(error);
            }
        }

        Ok(TrainingPreparedState {
            assignment,
            storage,
            store,
            local_peer_id,
            current_head,
            network_id,
            telemetry_snapshot,
            mainnet_roles,
            metrics_retention,
            node_config,
            robustness_policy,
        })
    }

    fn execute_training_window(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &TrainingPreparedState,
    ) -> anyhow::Result<TrainingExecution<P::WindowStats, P::Model>>
    where
        P: P2pWorkload,
    {
        self.reap_training_prefetch();
        let (device, model, capability) = {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            let device = project.runtime_device();
            let model =
                load_runtime_model(project, &prepared.current_head, &prepared.store, &device)?;
            let capability = project.benchmark(&model, &device);
            (device, model, capability)
        };
        self.execute_training_window_with_model(experiment, prepared, device, model, capability)
    }

    pub(in crate::training) fn execute_training_window_with_model(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &TrainingPreparedState,
        device: P::Device,
        model: P::Model,
        capability: CapabilityEstimate,
    ) -> anyhow::Result<TrainingExecution<P::WindowStats, P::Model>>
    where
        P: P2pWorkload,
    {
        let mut planned = self.plan_training_window(experiment, prepared, &capability)?;
        let telemetry = self.telemetry.clone();

        {
            let mut snapshot = telemetry
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            snapshot.set_node_state(NodeRuntimeState::LeasePending);
            snapshot.set_primary_slot_state(SlotRuntimeState::FetchingShards(
                prepared.assignment.clone(),
            ));
        }

        let throughput_sample_started_at = Utc::now();
        let cache = ShardCache::new(prepared.storage.dataset_cache_dir());
        let cached_microshards = cache.fetch_lease_microshards(
            &planned.registration,
            &planned.microshard_plan,
            &planned.lease.lease,
        )?;
        self.maybe_start_next_window_prefetch(
            experiment,
            prepared,
            &planned,
            &capability,
            &cached_microshards,
        )?;
        let batches = {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            project.load_batches(&planned.lease.lease, &cached_microshards)?
        };
        let data_fetch_time_ms = (Utc::now() - throughput_sample_started_at)
            .num_milliseconds()
            .max(0) as u64;
        let mut ctx = WindowCtx {
            device,
            model,
            lease: planned.lease.lease.clone(),
            cached_microshards,
            batches,
        };

        {
            let mut snapshot = telemetry
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            snapshot.set_node_state(NodeRuntimeState::TrainingWindow);
            snapshot
                .set_primary_slot_state(SlotRuntimeState::Training(prepared.assignment.clone()));
        }

        let report = {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            project.train_window(&mut ctx)?
        };
        let head_id = HeadId::new(format!(
            "{}-{}-window-{}",
            experiment.experiment_id.as_str(),
            prepared.local_peer_id.as_str(),
            planned.window_id.0
        ));
        let artifact = {
            let project = &mut self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            project.materialize_model_artifact(
                &ctx.model,
                ArtifactKind::FullHead,
                head_id.clone(),
                prepared
                    .current_head
                    .as_ref()
                    .map(|(_, head)| head.head_id.clone()),
                &prepared.store,
            )?
        };
        let throughput_sample_finished_at = std::cmp::max(Utc::now(), report.completed_at);
        let observed_throughput = ObservedThroughputUpdate {
            measured_work_units: planned.lease.selection.estimated_work_units.max(1),
            elapsed_seconds: observed_elapsed_seconds(
                throughput_sample_started_at,
                throughput_sample_finished_at,
            ),
            completed_windows: planned.window_id.0.min(u64::from(u32::MAX)) as u32,
            sampled_at: throughput_sample_finished_at,
            coordination_penalty: Some(local_training_adaptation_factor(
                &prepared.telemetry_snapshot,
                &prepared.local_peer_id,
            )),
        };
        planned.limit_profile = planned
            .calibrator
            .rebudget(&planned.limit_profile, observed_throughput.clone())?;

        let head = HeadDescriptor {
            head_id: head_id.clone(),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            parent_head_id: prepared
                .current_head
                .as_ref()
                .map(|(_, head)| head.head_id.clone()),
            global_step: prepared
                .current_head
                .as_ref()
                .map(|(_, head)| head.global_step + 1)
                .unwrap_or(0),
            created_at: report.completed_at,
            metrics: self
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project
                .contribution_metrics(&report),
        };
        let contribution = report
            .contribution
            .clone()
            .unwrap_or_else(|| ContributionReceipt {
                receipt_id: ContributionReceiptId::new(format!(
                    "{}-{}-receipt-{}",
                    experiment.experiment_id.as_str(),
                    prepared.local_peer_id.as_str(),
                    planned.window_id.0
                )),
                peer_id: prepared.local_peer_id.clone(),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                base_head_id: prepared
                    .current_head
                    .as_ref()
                    .map(|(_, head)| head.head_id.clone())
                    .unwrap_or_else(|| HeadId::new("genesis")),
                artifact_id: artifact.artifact_id.clone(),
                accepted_at: report.completed_at,
                accepted_weight: self
                    .node
                    .as_mut()
                    .expect("running node should retain prepared node")
                    .project
                    .contribution_weight(&report),
                metrics: head.metrics.clone(),
                merge_cert_id: None,
            });

        Ok(TrainingExecution {
            lease: planned.lease.lease,
            window_id: planned.window_id,
            base_head_id: planned.base_head_id,
            merge_window: planned.merge_window,
            reducer_assignment: planned.reducer_assignment,
            limit_profile: planned.limit_profile,
            model: ctx.model,
            head,
            artifact,
            contribution,
            report,
            window_started_at: throughput_sample_started_at,
            data_fetch_time_ms,
        })
    }
}
