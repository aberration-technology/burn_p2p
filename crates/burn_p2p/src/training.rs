use super::*;
use crate::metrics_runtime::{
    TrainingMetricBuildArgs, build_metrics_announcement, build_training_peer_window_metrics,
    persist_peer_window_metrics,
};
use crate::runtime_support::LagAssessment;
use burn_p2p_core::MetricsLiveEventKind;

struct TrainingPreparedState {
    assignment: SlotAssignmentState,
    storage: StorageConfig,
    store: FsArtifactStore,
    local_peer_id: PeerId,
    current_head: Option<(PeerId, HeadDescriptor)>,
    network_id: NetworkId,
    telemetry_snapshot: NodeTelemetrySnapshot,
    mainnet_roles: PeerRoleSet,
    metrics_retention: MetricsRetentionBudget,
    node_config: NodeConfig,
    robustness_policy: RobustnessPolicy,
}

struct PlannedTrainingWindow {
    calibrator: CapabilityCalibrator,
    limit_profile: LimitProfile,
    registration: DatasetRegistration,
    microshard_plan: MicroShardPlan,
    lease: PlannedLease,
    window_id: WindowId,
    base_head_id: HeadId,
    merge_window: MergeWindowState,
    reducer_assignment: ReducerAssignment,
}

struct TrainingExecution<T> {
    lease: AssignmentLease,
    window_id: WindowId,
    base_head_id: HeadId,
    merge_window: MergeWindowState,
    reducer_assignment: ReducerAssignment,
    limit_profile: LimitProfile,
    head: HeadDescriptor,
    artifact: ArtifactDescriptor,
    contribution: ContributionReceipt,
    report: WindowReport<T>,
    window_started_at: DateTime<Utc>,
    data_fetch_time_ms: u64,
}

impl<P> RunningNode<P> {
    /// Performs the train window once operation.
    pub fn train_window_once(
        &mut self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<TrainingWindowOutcome<P::WindowStats>>
    where
        P: P2pWorkload,
    {
        let prepared = self.prepare_training_state(experiment)?;
        let execution = self.execute_training_window(experiment, &prepared)?;
        self.publish_training_execution(experiment, &prepared, &execution)?;

        Ok(TrainingWindowOutcome {
            lease: execution.lease,
            head: execution.head,
            artifact: execution.artifact,
            contribution: execution.contribution,
            report: execution.report,
        })
    }

    fn prepare_training_state(
        &mut self,
        experiment: &ExperimentHandle,
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

        let snapshots = self.fetch_connected_snapshots(Duration::from_secs(3))?;
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
        let current_head =
            resolve_canonical_head(&storage, experiment, &snapshots)?.or_else(|| {
                latest_head_from_snapshot(
                    self.telemetry().snapshot().control_plane.clone(),
                    experiment,
                )
            });
        let network_id = self.mainnet().network_id().clone();
        let telemetry_snapshot = self.telemetry().snapshot();
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
        if let Some((source_peer_id, source_head)) = current_head.as_ref()
            && !store.has_manifest(&source_head.artifact_id)
            && let Err(error) =
                self.sync_artifact_from_peer(source_peer_id, source_head.artifact_id.clone())
            && source_head.global_step > 0
        {
            return Err(error);
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
    ) -> anyhow::Result<TrainingExecution<P::WindowStats>>
    where
        P: P2pWorkload,
    {
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
        let mut planned = self.plan_training_window(experiment, prepared, &capability)?;
        let telemetry = self.telemetry.clone();
        let project = &mut self
            .node
            .as_mut()
            .expect("running node should retain prepared node")
            .project;

        {
            let mut snapshot = telemetry
                .state
                .lock()
                .expect("telemetry state lock should not be poisoned");
            snapshot.set_node_state(NodeRuntimeState::LeasePending);
            snapshot.set_primary_slot_state(SlotRuntimeState::FetchingShards(
                prepared.assignment.clone(),
            ));
        }

        let throughput_sample_started_at = Utc::now();
        let cached_microshards = ShardCache::new(prepared.storage.dataset_cache_dir())
            .fetch_lease_microshards(
                &planned.registration,
                &planned.microshard_plan,
                &planned.lease.lease,
            )?;
        let batches = project.load_batches(&planned.lease.lease, &cached_microshards)?;
        let data_fetch_time_ms = (Utc::now() - throughput_sample_started_at)
            .num_milliseconds()
            .max(0) as u64;
        let mut ctx = WindowCtx {
            device,
            model,
            lease: planned.lease.lease.clone(),
            batches,
        };

        {
            let mut snapshot = telemetry
                .state
                .lock()
                .expect("telemetry state lock should not be poisoned");
            snapshot.set_node_state(NodeRuntimeState::TrainingWindow);
            snapshot
                .set_primary_slot_state(SlotRuntimeState::Training(prepared.assignment.clone()));
        }

        let report = project.train_window(&mut ctx)?;
        let head_id = HeadId::new(format!(
            "{}-{}-window-{}",
            experiment.experiment_id.as_str(),
            prepared.local_peer_id.as_str(),
            planned.window_id.0
        ));
        let artifact = project.materialize_model_artifact(
            &ctx.model,
            ArtifactKind::FullHead,
            head_id.clone(),
            prepared
                .current_head
                .as_ref()
                .map(|(_, head)| head.head_id.clone()),
            &prepared.store,
        )?;
        let throughput_sample_finished_at = std::cmp::max(Utc::now(), report.completed_at);
        let observed_throughput = ObservedThroughputUpdate {
            measured_work_units: planned.lease.selection.estimated_work_units.max(1),
            elapsed_seconds: (throughput_sample_finished_at - throughput_sample_started_at)
                .num_seconds()
                .max(1) as u64,
            completed_windows: planned.window_id.0.min(u64::from(u32::MAX)) as u32,
            sampled_at: throughput_sample_finished_at,
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
            metrics: project.contribution_metrics(&report),
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
                accepted_weight: project.contribution_weight(&report),
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
            head,
            artifact,
            contribution,
            report,
            window_started_at: throughput_sample_started_at,
            data_fetch_time_ms,
        })
    }

    fn plan_training_window(
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
        let budget_work_units = limit_profile.recommended_budget.budget_work_units.max(1);
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
        let window_id = next_window_id(&prepared.storage, experiment)?;
        let overlays = experiment.overlay_set()?;
        let base_head_id = prepared
            .current_head
            .as_ref()
            .map(|(_, head)| head.head_id.clone())
            .unwrap_or_else(|| HeadId::new("genesis"));
        let topology_policy = runtime_merge_topology_policy(
            &prepared.telemetry_snapshot,
            experiment,
            Some(&base_head_id),
        );
        let topology_peers =
            runtime_topology_peers(&prepared.telemetry_snapshot, &prepared.local_peer_id);
        let validator_peers =
            runtime_validator_peers(&prepared.telemetry_snapshot, &prepared.local_peer_id);
        let validators = runtime_validators(
            &prepared.mainnet_roles,
            &prepared.local_peer_id,
            &validator_peers,
            topology_policy.promotion_policy.validator_quorum,
        );
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
        let lease = LeasePlanner::default().plan_lease(
            prepared.network_id.clone(),
            experiment.study_id.clone(),
            experiment.experiment_id.clone(),
            experiment.revision_id.clone(),
            &registration.view,
            prepared.local_peer_id.clone(),
            window_id,
            Utc::now(),
            budget_work_units,
            &microshard_plan.microshards,
        )?;
        self.control.publish_lease(LeaseAnnouncement {
            overlay: overlays.leases.clone(),
            lease: lease.lease.clone(),
            announced_at: Utc::now(),
        })?;

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

    fn publish_training_execution<T>(
        &mut self,
        experiment: &ExperimentHandle,
        prepared: &TrainingPreparedState,
        execution: &TrainingExecution<T>,
    ) -> anyhow::Result<()> {
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
            publish_latency_ms: (publish_finished_at - publish_started_at)
                .num_milliseconds()
                .max(0) as u64,
            head_lag_at_start: prepared.telemetry_snapshot.head_lag_steps,
            head_lag_at_finish: prepared.telemetry_snapshot.head_lag_steps,
        });
        persist_peer_window_metrics(
            &prepared.storage,
            experiment,
            &peer_window_metrics,
            prepared.metrics_retention,
        )?;
        self.control.publish_metrics(build_metrics_announcement(
            experiment,
            overlays.metrics,
            MetricsLiveEventKind::LedgerAppend,
            None,
            Some(execution.merge_window.merge_window_id.clone()),
        ))?;
        self.update_runtime_state(
            NodeRuntimeState::WaitingMerge,
            Some(SlotRuntimeState::CoolingDown(prepared.assignment.clone())),
        );

        Ok(())
    }
}

fn load_runtime_model<P>(
    project: &mut P,
    current_head: &Option<(PeerId, HeadDescriptor)>,
    store: &FsArtifactStore,
    device: &P::Device,
) -> anyhow::Result<P::Model>
where
    P: P2pWorkload,
{
    Ok(if let Some((_, base_head)) = current_head.as_ref() {
        match store.load_manifest(&base_head.artifact_id) {
            Ok(descriptor) => project.load_model_artifact(
                project.init_model(device),
                &descriptor,
                store,
                device,
            )?,
            Err(_) if base_head.global_step == 0 => project.init_model(device),
            Err(error) => return Err(error.into()),
        }
    } else {
        project.init_model(device)
    })
}

fn runtime_blocked_reason(prefix: &str, lag_assessment: &LagAssessment) -> String {
    match lag_assessment.state {
        LagState::LeaseBlocked => format!(
            "{prefix} blocked: local node is {} head steps behind canonical head",
            lag_assessment.head_lag_steps
        ),
        LagState::RebaseRequired => format!(
            "{prefix} blocked: full rebase required after falling {} head steps behind",
            lag_assessment.head_lag_steps
        ),
        _ => unreachable!("non-blocking lag state matched blocking branch"),
    }
}
