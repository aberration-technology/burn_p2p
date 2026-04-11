use super::*;

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
        if prepared.experiment != self.experiment {
            self.reset_session_to_prepared_experiment(&prepared)?;
        } else {
            let _ = self.reconcile_visible_canonical_head(
                prepared.current_head.as_ref().map(|(_, head)| head.clone()),
            )?;
        }
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
            &prepared.experiment,
            &prepared,
            device,
            model,
            capability,
        )?;
        let publish_latency_ms =
            self.node
                .publish_training_execution(&prepared.experiment, &prepared, &execution)?;

        self.experiment = prepared.experiment.clone();
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

    fn reset_session_to_prepared_experiment(
        &mut self,
        prepared: &TrainingPreparedState,
    ) -> anyhow::Result<()> {
        let model = {
            let project = &mut self
                .node
                .node
                .as_mut()
                .expect("running node should retain prepared node")
                .project;
            let device = project.runtime_device();
            load_runtime_model(project, &prepared.current_head, &prepared.store, &device)?
        };
        let current_head = prepared.current_head.as_ref().map(|(_, head)| head.clone());
        self.experiment = prepared.experiment.clone();
        self.canonical_head = current_head.clone();
        self.training_head = current_head;
        self.warm_model = Some(model);
        Ok(())
    }
}
