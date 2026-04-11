use super::*;
use crate::metrics_runtime::{
    TrainingMetricBuildArgs, build_metrics_announcement, build_peer_window_placement_hint,
    build_training_peer_window_metrics, persist_peer_window_metrics,
};
use crate::runtime_support::{
    LagAssessment, active_experiment_directory_entry, runtime_training_assignment_peers,
    snapshots_with_local_control_plane,
};
use burn_p2p_core::{MetricsLiveEventKind, MicroShard};
use std::collections::BTreeSet;

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

struct TrainingExecution<T, M> {
    lease: AssignmentLease,
    window_id: WindowId,
    base_head_id: HeadId,
    merge_window: MergeWindowState,
    reducer_assignment: ReducerAssignment,
    limit_profile: LimitProfile,
    model: M,
    head: HeadDescriptor,
    artifact: ArtifactDescriptor,
    contribution: ContributionReceipt,
    report: WindowReport<T>,
    window_started_at: DateTime<Utc>,
    data_fetch_time_ms: u64,
}

/// Stateful continuous-training helper that keeps a warm in-memory model and
/// reconciles newly visible canonical heads between windows.
///
/// This is the opinionated long-running trainer path: it lets a trainer keep
/// publishing local windows without synchronously waiting for validator
/// promotion after every window. While the trainer is ahead of the last visible
/// canonical head it advances on its own local speculative chain; once a newer
/// canonical head appears, the session rebases onto that canonical head using
/// the configured reconcile strategy before the next window starts.
pub struct ContinuousTrainer<'a, P>
where
    P: P2pWorkload,
{
    node: &'a mut RunningNode<P>,
    experiment: ExperimentHandle,
    policy: ContinuousTrainerPolicy,
    canonical_head: Option<HeadDescriptor>,
    training_head: Option<HeadDescriptor>,
    warm_model: Option<P::Model>,
}

struct PrefetchLeasePlanArgs<'a> {
    network_id: &'a NetworkId,
    experiment: &'a ExperimentHandle,
    dataset_view: &'a DatasetView,
    local_peer_id: &'a PeerId,
    assignment_peers: &'a [PeerId],
    microshards: &'a [MicroShard],
    window_id: WindowId,
    budget_work_units: u64,
}

fn observed_elapsed_seconds(started_at: DateTime<Utc>, finished_at: DateTime<Utc>) -> u64 {
    let elapsed_ms = finished_at
        .signed_duration_since(started_at)
        .num_milliseconds()
        .max(1) as u64;
    elapsed_ms.saturating_add(999) / 1000
}

fn plan_prefetch_lease_for_window(args: PrefetchLeasePlanArgs<'_>) -> anyhow::Result<PlannedLease> {
    let preferred_microshards =
        preferred_microshards_for_peer(args.assignment_peers, args.local_peer_id, args.microshards);
    let budget_work_units = fair_share_budget_work_units(
        args.assignment_peers.len(),
        preferred_microshards.len(),
        args.budget_work_units,
    );
    let mut lease_planner = LeasePlanner::default();
    lease_planner.config.max_microshards_per_lease = lease_planner
        .config
        .max_microshards_per_lease
        .min(fair_share_microshard_cap(
            args.assignment_peers.len(),
            preferred_microshards.len(),
        ));

    Ok(lease_planner.plan_lease(
        args.network_id.clone(),
        args.experiment.study_id.clone(),
        args.experiment.experiment_id.clone(),
        args.experiment.revision_id.clone(),
        args.dataset_view,
        args.local_peer_id.clone(),
        args.window_id,
        Utc::now(),
        budget_work_units,
        &preferred_microshards,
    )?)
}

fn supports_background_shard_prefetch(limit_profile: &LimitProfile) -> bool {
    matches!(
        limit_profile
            .estimate
            .preferred_backends
            .first()
            .map(String::as_str),
        Some("cuda" | "wgpu")
    )
}

fn prefetched_lease_is_reusable(
    prefetch: &TrainingPrefetchTask,
    experiment: &ExperimentHandle,
    local_peer_id: &PeerId,
    window_id: WindowId,
    budget_work_units: u64,
    available_microshards: &[MicroShard],
) -> bool {
    let lease = &prefetch.planned_lease.lease;
    let available_microshard_ids = available_microshards
        .iter()
        .map(|microshard| &microshard.microshard_id)
        .collect::<BTreeSet<_>>();
    lease.study_id == experiment.study_id
        && lease.experiment_id == experiment.experiment_id
        && lease.revision_id == experiment.revision_id
        && lease.peer_id == *local_peer_id
        && lease.window_id == window_id
        && lease.budget_work_units == budget_work_units
        && lease
            .microshards
            .iter()
            .all(|microshard_id| available_microshard_ids.contains(microshard_id))
        && lease.expires_at > Utc::now()
}

fn wait_for_prefetch_completion(handle: &std::thread::JoinHandle<()>, grace: Duration) -> bool {
    if handle.is_finished() {
        return true;
    }

    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if handle.is_finished() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    handle.is_finished()
}

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

    fn prepare_training_state(
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

    fn execute_training_window_with_model(
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

    fn reap_training_prefetch(&mut self) {
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

    fn take_ready_training_prefetch(
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

    fn maybe_start_next_window_prefetch(
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
        let placement_budget_work_units = training_placement_budget_work_units(
            planned
                .limit_profile
                .recommended_budget
                .budget_work_units
                .max(1),
            capability,
            directory_entry.as_ref(),
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
        let directory_entry = active_experiment_directory_entry(
            &prepared.node_config,
            &prepared.telemetry_snapshot,
            experiment,
        );
        if let Some(entry) = directory_entry.as_ref() {
            ensure_training_placement_roles(&prepared.mainnet_roles, entry)?;
        }
        let budget_work_units = training_placement_budget_work_units(
            limit_profile.recommended_budget.budget_work_units.max(1),
            capability,
            directory_entry.as_ref(),
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
        let window_id = inferred_next_window_id(
            &prepared.storage,
            experiment,
            prepared.current_head.as_ref().map(|(_, head)| head),
        )?;
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
                .min(fair_share_microshard_cap(
                    assignment_peers.len(),
                    lease_microshards.len(),
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

    fn publish_training_execution<T, M>(
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

fn ensure_training_placement_roles(
    local_roles: &PeerRoleSet,
    entry: &ExperimentDirectoryEntry,
) -> anyhow::Result<()> {
    let missing_roles = entry
        .resource_requirements
        .minimum_roles
        .iter()
        .filter(|role| !local_roles.contains(role))
        .cloned()
        .collect::<Vec<_>>();
    if missing_roles.is_empty() {
        return Ok(());
    }

    anyhow::bail!(
        "training placement rejected: local roles {:?} do not satisfy minimum experiment roles {:?}",
        local_roles.roles,
        missing_roles
    );
}

fn training_placement_budget_work_units(
    recommended_budget_work_units: u64,
    capability: &CapabilityEstimate,
    directory_entry: Option<&ExperimentDirectoryEntry>,
) -> anyhow::Result<u64> {
    let Some(directory_entry) = directory_entry else {
        return Ok(recommended_budget_work_units.max(1));
    };
    let target_window_seconds = directory_entry
        .resource_requirements
        .estimated_window_seconds
        .max(1);
    let placement_budget_work_units =
        (capability.work_units_per_second.max(0.0) * target_window_seconds as f64).floor() as u64;
    if placement_budget_work_units == 0 {
        anyhow::bail!(
            "training placement rejected: local capability {:.3} work units/s cannot satisfy estimated window {}s",
            capability.work_units_per_second,
            target_window_seconds
        );
    }

    Ok(recommended_budget_work_units
        .max(1)
        .min(placement_budget_work_units))
}

fn fair_share_microshard_cap(
    assignment_peer_count: usize,
    available_microshard_count: usize,
) -> usize {
    available_microshard_count
        .max(1)
        .div_ceil(assignment_peer_count.max(1))
}

fn fair_share_budget_work_units(
    assignment_peer_count: usize,
    available_microshard_count: usize,
    placement_budget_work_units: u64,
) -> u64 {
    let fair_share_microshards =
        fair_share_microshard_cap(assignment_peer_count, available_microshard_count) as u128;
    let available_microshards = available_microshard_count.max(1) as u128;
    let placement_budget_work_units = placement_budget_work_units.max(1) as u128;

    ((placement_budget_work_units * fair_share_microshards).div_ceil(available_microshards)).max(1)
        as u64
}

fn unleased_microshards_for_window(
    snapshot: &NodeTelemetrySnapshot,
    experiment: &ExperimentHandle,
    window_id: WindowId,
    local_peer_id: &PeerId,
    microshards: &[MicroShard],
) -> Vec<MicroShard> {
    let now = Utc::now();
    let leased_microshards = snapshot
        .control_plane
        .lease_announcements
        .iter()
        .filter(|announcement| {
            announcement.lease.study_id == experiment.study_id
                && announcement.lease.experiment_id == experiment.experiment_id
                && announcement.lease.revision_id == experiment.revision_id
                && announcement.lease.window_id == window_id
                && &announcement.lease.peer_id != local_peer_id
                && announcement.lease.expires_at > now
        })
        .flat_map(|announcement| announcement.lease.microshards.iter().cloned())
        .collect::<BTreeSet<_>>();
    let available = microshards
        .iter()
        .filter(|microshard| !leased_microshards.contains(&microshard.microshard_id))
        .cloned()
        .collect::<Vec<_>>();
    if available.is_empty() {
        microshards.to_vec()
    } else {
        available
    }
}

fn merge_connected_lease_announcements(
    local_snapshot: &mut ControlPlaneSnapshot,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
) {
    for (_, snapshot) in snapshots {
        for announcement in &snapshot.lease_announcements {
            if !local_snapshot.lease_announcements.contains(announcement) {
                local_snapshot
                    .lease_announcements
                    .push(announcement.clone());
            }
        }
    }
}

fn preferred_microshards_for_peer(
    topology_peers: &[PeerId],
    local_peer_id: &PeerId,
    microshards: &[MicroShard],
) -> Vec<MicroShard> {
    if microshards.len() <= 1 {
        return microshards.to_vec();
    }

    let peer_index = topology_peers
        .iter()
        .position(|peer_id| peer_id == local_peer_id)
        .unwrap_or_default();
    let slot_count = topology_peers.len().max(1);
    let preferred = microshards
        .iter()
        .enumerate()
        .filter(|(index, _)| index % slot_count == peer_index % slot_count)
        .map(|(_, microshard)| microshard.clone())
        .collect::<Vec<_>>();

    if preferred.is_empty() {
        vec![microshards[peer_index % microshards.len()].clone()]
    } else {
        preferred
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
        load_model_for_head(project, base_head, store, device)?
    } else {
        project.init_model(device)
    })
}

fn load_model_for_head<P>(
    project: &mut P,
    head: &HeadDescriptor,
    store: &FsArtifactStore,
    device: &P::Device,
) -> anyhow::Result<P::Model>
where
    P: P2pWorkload,
{
    match store.load_manifest(&head.artifact_id) {
        Ok(descriptor) => {
            project.load_model_artifact(project.init_model(device), &descriptor, store, device)
        }
        Err(_) if head.global_step == 0 => Ok(project.init_model(device)),
        Err(error) => Err(error.into()),
    }
}

impl<'a, P> ContinuousTrainer<'a, P>
where
    P: P2pWorkload,
{
    pub(crate) fn new(
        node: &'a mut RunningNode<P>,
        experiment: &ExperimentHandle,
        policy: ContinuousTrainerPolicy,
    ) -> anyhow::Result<Self> {
        let canonical_head = node.sync_experiment_head(experiment)?;
        let training_head = canonical_head.clone();
        let storage =
            node.config().storage.as_ref().cloned().ok_or_else(|| {
                anyhow::anyhow!("continuous training requires configured storage")
            })?;
        let store = FsArtifactStore::new(storage.root.clone());
        let warm_model = {
            let project = &mut node
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            let device = project.runtime_device();
            let current_head = canonical_head
                .clone()
                .map(|head| (PeerId::new("canonical"), head));
            load_runtime_model(project, &current_head, &store, &device)?
        };

        Ok(Self {
            node,
            experiment: experiment.clone(),
            policy,
            canonical_head,
            training_head,
            warm_model: Some(warm_model),
        })
    }

    /// Returns the experiment this session is driving.
    pub fn experiment(&self) -> &ExperimentHandle {
        &self.experiment
    }

    /// Returns the active policy.
    pub fn policy(&self) -> &ContinuousTrainerPolicy {
        &self.policy
    }

    /// Returns the last visible canonical head tracked by the session.
    pub fn canonical_head(&self) -> Option<&HeadDescriptor> {
        self.canonical_head.as_ref()
    }

    /// Returns the local training head that the warm model currently descends
    /// from. This may run ahead of the last visible canonical head while the
    /// trainer is optimistically continuing local work.
    pub fn training_head(&self) -> Option<&HeadDescriptor> {
        self.training_head.as_ref()
    }

    /// Polls the runtime for a newer canonical head and reconciles it into the
    /// warm local model when one appears.
    pub fn sync_canonical_head(&mut self) -> anyhow::Result<Option<HeadDescriptor>> {
        let latest = self.node.sync_experiment_head(&self.experiment)?;
        self.reconcile_visible_canonical_head(latest)
    }

    /// Waits until at least one canonical head is visible and reconciles it
    /// into the warm local model if it is newer than the current session view.
    pub fn wait_for_canonical_head(&mut self, timeout: Duration) -> anyhow::Result<HeadDescriptor> {
        let latest = self
            .node
            .wait_for_experiment_head(&self.experiment, timeout)?;
        self.reconcile_visible_canonical_head(Some(latest.clone()))?;
        Ok(latest)
    }

    /// Trains one new window immediately using the current warm model, then
    /// publishes the resulting candidate without waiting for a validator merge.
    pub fn train_next_window(&mut self) -> anyhow::Result<TrainingWindowOutcome<P::WindowStats>> {
        let _ = self.sync_canonical_head()?;
        self.wait_for_canonical_visibility_if_too_far_ahead()?;
        let prepared = self
            .node
            .prepare_training_state(&self.experiment, self.training_head.as_ref())?;
        let model = self
            .warm_model
            .take()
            .expect("continuous trainer should retain a warm model");
        let (device, capability) = {
            let project = &mut self
                .node
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            let device = project.runtime_device();
            let capability = project.benchmark(&model, &device);
            (device, capability)
        };
        let execution = self.node.execute_training_window_with_model(
            &self.experiment,
            &prepared,
            device,
            model,
            capability,
        )?;
        let publish_latency_ms =
            self.node
                .publish_training_execution(&self.experiment, &prepared, &execution)?;

        self.training_head = Some(execution.head.clone());
        self.warm_model = Some(execution.model);

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

    fn wait_for_canonical_visibility_if_too_far_ahead(&mut self) -> anyhow::Result<()> {
        let max_speculative_windows = self.policy.max_speculative_windows.max(1) as u64;
        if self.speculative_lead_steps() < max_speculative_windows {
            return Ok(());
        }

        let deadline =
            Instant::now() + Duration::from_millis(self.policy.canonical_visibility_wait_ms);
        while Instant::now() < deadline {
            let latest = self.node.sync_experiment_head(&self.experiment)?;
            let _ = self.reconcile_visible_canonical_head(latest)?;
            if self.speculative_lead_steps() < max_speculative_windows {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        Ok(())
    }

    fn speculative_lead_steps(&self) -> u64 {
        match (&self.training_head, &self.canonical_head) {
            (Some(training), Some(canonical)) => {
                training.global_step.saturating_sub(canonical.global_step)
            }
            _ => 0,
        }
    }

    fn reconcile_visible_canonical_head(
        &mut self,
        latest: Option<HeadDescriptor>,
    ) -> anyhow::Result<Option<HeadDescriptor>> {
        let Some(latest) = latest else {
            return Ok(self.canonical_head.clone());
        };
        if self
            .canonical_head
            .as_ref()
            .is_some_and(|current| current.head_id == latest.head_id)
        {
            return Ok(Some(latest));
        }

        let storage = self
            .node
            .config()
            .storage
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("continuous training requires configured storage"))?;
        let store = FsArtifactStore::new(storage.root.clone());
        let local_model = self
            .warm_model
            .take()
            .expect("continuous trainer should retain a warm model");
        let reconciled = {
            let project = &mut self
                .node
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            let device = project.runtime_device();
            let canonical_model = load_model_for_head(project, &latest, &store, &device)?;
            if self
                .training_head
                .as_ref()
                .is_some_and(|current| current.head_id == latest.head_id)
            {
                canonical_model
            } else {
                project.reconcile_canonical_model(
                    &local_model,
                    canonical_model,
                    self.policy.canonical_reconcile,
                )?
            }
        };

        self.warm_model = Some(reconciled);
        self.canonical_head = Some(latest.clone());
        self.training_head = Some(latest.clone());
        Ok(Some(latest))
    }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        PrefetchLeasePlanArgs, fair_share_budget_work_units, fair_share_microshard_cap,
        plan_prefetch_lease_for_window, prefetched_lease_is_reusable,
        supports_background_shard_prefetch, wait_for_prefetch_completion,
    };
    use crate::{
        ExperimentHandle, LimitProfile, PeerId, PeerRoleSet, TrainingPrefetchTask, WorkBudget,
    };
    use burn_p2p_core::{
        AttestationLevel, CapabilityCard, CapabilityCardId, CapabilityClass, CapabilityEstimate,
        ClientPlatform, ContentId, DatasetId, DatasetView, DatasetViewId, ExperimentId, MicroShard,
        NetworkId, PersistenceClass, RevisionId, StudyId, WindowId,
    };
    use chrono::Utc;

    #[test]
    fn fair_share_budget_tracks_microshard_share() {
        assert_eq!(fair_share_microshard_cap(3, 8), 3);
        assert_eq!(fair_share_budget_work_units(3, 8, 240), 90);
        assert_eq!(fair_share_budget_work_units(2, 5, 240), 144);
    }

    #[test]
    fn fair_share_budget_never_drops_below_one() {
        assert_eq!(fair_share_budget_work_units(8, 1, 1), 1);
        assert_eq!(fair_share_budget_work_units(16, 16, 1), 1);
    }

    #[test]
    fn prefetch_lease_targets_next_window_peer_slice() {
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
        };
        let dataset_view = DatasetView {
            dataset_view_id: DatasetViewId::new("view"),
            dataset_id: DatasetId::new("dataset"),
            preprocessing_hash: ContentId::new("prep"),
            tokenizer_hash: None,
            manifest_hash: ContentId::new("manifest"),
            metadata: Default::default(),
        };
        let assignment_peers = vec![PeerId::new("peer-a"), PeerId::new("peer-b")];
        let microshards = (0..6)
            .map(|ordinal| MicroShard {
                microshard_id: burn_p2p_core::MicroShardId::new(format!("micro-{ordinal}")),
                dataset_view_id: dataset_view.dataset_view_id.clone(),
                ordinal,
                estimated_examples: 16,
                estimated_tokens: 512,
                estimated_bytes: 1024,
            })
            .collect::<Vec<_>>();

        let planned = plan_prefetch_lease_for_window(PrefetchLeasePlanArgs {
            network_id: &experiment.network_id,
            experiment: &experiment,
            dataset_view: &dataset_view,
            local_peer_id: &assignment_peers[1],
            assignment_peers: &assignment_peers,
            microshards: &microshards,
            window_id: WindowId(9),
            budget_work_units: 128,
        })
        .expect("prefetch lease");

        assert_eq!(planned.lease.window_id, WindowId(9));
        assert!(!planned.lease.microshards.is_empty());
        assert!(
            planned
                .lease
                .microshards
                .iter()
                .all(|microshard_id| microshard_id.as_str().starts_with("micro-"))
        );
        assert!(planned.lease.microshards.len() <= fair_share_microshard_cap(2, 3));
    }

    #[test]
    fn background_prefetch_only_runs_for_compute_backends() {
        let gpu_profile = LimitProfile {
            card: CapabilityCard {
                card_id: CapabilityCardId::new("card-gpu"),
                peer_id: PeerId::new("peer-gpu"),
                platform: ClientPlatform::Native,
                roles: PeerRoleSet::default_trainer(),
                preferred_backends: vec!["cuda".into()],
                browser_capabilities: BTreeSet::new(),
                recommended_classes: BTreeSet::from([CapabilityClass::TrainerGpu]),
                device_memory_bytes: Some(1),
                system_memory_bytes: 1,
                disk_bytes: 1,
                upload_mbps: 1.0,
                download_mbps: 1.0,
                persistence: PersistenceClass::Ephemeral,
                work_units_per_second: 1.0,
                attestation_level: AttestationLevel::None,
                benchmark_hash: None,
                reported_at: Utc::now(),
            },
            estimate: CapabilityEstimate {
                preferred_backends: vec!["cuda".into()],
                work_units_per_second: 1.0,
                target_window_seconds: 1,
            },
            recommended_roles: PeerRoleSet::default_trainer(),
            recommended_budget: WorkBudget {
                target_window_seconds: 1,
                budget_work_units: 1,
            },
        };
        let cpu_profile = LimitProfile {
            card: CapabilityCard {
                preferred_backends: vec!["ndarray".into()],
                ..gpu_profile.card.clone()
            },
            estimate: CapabilityEstimate {
                preferred_backends: vec!["ndarray".into()],
                ..gpu_profile.estimate.clone()
            },
            recommended_roles: gpu_profile.recommended_roles.clone(),
            recommended_budget: gpu_profile.recommended_budget.clone(),
        };

        assert!(supports_background_shard_prefetch(&gpu_profile));
        assert!(!supports_background_shard_prefetch(&cpu_profile));
    }

    #[test]
    fn prefetched_lease_reuse_requires_matching_window_and_live_lease() {
        let experiment = ExperimentHandle {
            network_id: NetworkId::new("net"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("exp"),
            revision_id: RevisionId::new("rev"),
        };
        let dataset_view = DatasetView {
            dataset_view_id: DatasetViewId::new("view"),
            dataset_id: DatasetId::new("dataset"),
            preprocessing_hash: ContentId::new("prep"),
            tokenizer_hash: None,
            manifest_hash: ContentId::new("manifest"),
            metadata: Default::default(),
        };
        let local_peer_id = PeerId::new("peer-a");
        let assignment_peers = vec![local_peer_id.clone(), PeerId::new("peer-b")];
        let microshards = (0..4)
            .map(|ordinal| MicroShard {
                microshard_id: burn_p2p_core::MicroShardId::new(format!("micro-{ordinal}")),
                dataset_view_id: dataset_view.dataset_view_id.clone(),
                ordinal,
                estimated_examples: 16,
                estimated_tokens: 512,
                estimated_bytes: 1024,
            })
            .collect::<Vec<_>>();
        let planned_lease = plan_prefetch_lease_for_window(PrefetchLeasePlanArgs {
            network_id: &experiment.network_id,
            experiment: &experiment,
            dataset_view: &dataset_view,
            local_peer_id: &local_peer_id,
            assignment_peers: &assignment_peers,
            microshards: &microshards,
            window_id: WindowId(3),
            budget_work_units: 128,
        })
        .expect("prefetch lease");
        let reusable = TrainingPrefetchTask {
            planned_lease: planned_lease.clone(),
            handle: None,
        };

        assert!(prefetched_lease_is_reusable(
            &reusable,
            &experiment,
            &local_peer_id,
            WindowId(3),
            planned_lease.lease.budget_work_units,
            &microshards,
        ));
        assert!(!prefetched_lease_is_reusable(
            &reusable,
            &experiment,
            &local_peer_id,
            WindowId(4),
            planned_lease.lease.budget_work_units,
            &microshards,
        ));
        assert!(!prefetched_lease_is_reusable(
            &reusable,
            &experiment,
            &PeerId::new("peer-c"),
            WindowId(3),
            planned_lease.lease.budget_work_units,
            &microshards,
        ));
        assert!(!prefetched_lease_is_reusable(
            &reusable,
            &experiment,
            &local_peer_id,
            WindowId(3),
            planned_lease.lease.budget_work_units.saturating_add(1),
            &microshards,
        ));

        let mut expired = reusable;
        expired.planned_lease.lease.expires_at = Utc::now() - chrono::Duration::seconds(1);
        assert!(!prefetched_lease_is_reusable(
            &expired,
            &experiment,
            &local_peer_id,
            WindowId(3),
            planned_lease.lease.budget_work_units,
            &microshards,
        ));
    }

    #[test]
    fn prefetch_completion_wait_observes_finished_worker() {
        let handle = std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_millis(15));
        });
        assert!(wait_for_prefetch_completion(
            &handle,
            std::time::Duration::from_millis(100),
        ));
        let _ = handle.join();
    }

    #[test]
    fn prefetch_completion_wait_does_not_block_past_grace() {
        let handle = std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_millis(100));
        });
        assert!(!wait_for_prefetch_completion(
            &handle,
            std::time::Duration::from_millis(10),
        ));
        let _ = handle.join();
    }
}
