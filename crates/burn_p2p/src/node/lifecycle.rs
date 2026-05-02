use super::*;
use crate::runtime_support::load_slot_assignments;

/// Handle to a live native runtime.
///
/// This wraps the prepared node together with the telemetry/control handles and
/// the background runtime thread.
pub struct RunningNode<P> {
    pub(crate) node: Option<Node<P>>,
    pub(crate) telemetry: TelemetryHandle,
    pub(crate) control: ControlHandle,
    pub(crate) training_prefetch: Option<TrainingPrefetchTask>,
    pub(crate) diffusion_state: crate::promotion::diffusion::DiffusionStateCache,
    pub(crate) validation_cache: Option<Box<dyn Any + Send>>,
    pub(crate) runtime_thread: Option<JoinHandle<()>>,
}

pub(crate) struct TrainingPrefetchTask {
    pub(crate) planned_lease: PlannedLease,
    pub(crate) handle: Option<JoinHandle<()>>,
}

impl<P> RunningNode<P> {
    pub(crate) fn spawn(node: Node<P>) -> anyhow::Result<Self> {
        if let Some(storage) = node.config.storage.as_ref() {
            storage.ensure_layout()?;
            persist_auth_state(storage, node.config.auth.as_ref())?;
            persist_runtime_binding_state(storage, node.config())?;
        }

        let keypair = resolve_identity(&node.config.identity, node.config.storage.as_ref())?;

        let mut snapshot = NodeTelemetrySnapshot::starting(&node.mainnet, &node.config);
        let bootstrap_addresses = node.config.bootstrap_peers.clone();
        if let Some(storage) = node.config.storage.as_ref() {
            for address in load_known_peers(storage)? {
                snapshot.known_peer_addresses.insert(address);
            }
            let slot_assignments = load_slot_assignments(storage)?;
            if !slot_assignments.is_empty() {
                snapshot.set_slot_assignments(&slot_assignments);
            }
            restore_control_plane_state(storage, &mut snapshot)?;
            restore_runtime_security_state(storage, &mut snapshot)?;
            restore_in_flight_transfer_states(storage, &mut snapshot)?;
        }
        let boundary = RuntimeBoundary::for_platform_and_roles(
            &node.mainnet.genesis,
            burn_p2p_core::ClientPlatform::Native,
            &node.mainnet.roles,
            bootstrap_addresses,
            node.config.listen_addresses.clone(),
            node.config.external_addresses.clone(),
            node.config
                .storage
                .as_ref()
                .map(StorageConfig::webrtc_certificate_pem_path),
        )?;
        snapshot.runtime_boundary = Some(boundary.clone());
        snapshot.listen_addresses = Vec::new();

        let shared_state = Arc::new(Mutex::new(snapshot));
        if let Some(storage) = node.config.storage.as_ref() {
            let snapshot = shared_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            persist_runtime_security_state(storage, &snapshot)?;
        }
        let telemetry = TelemetryHandle {
            state: Arc::clone(&shared_state),
        };
        let (command_tx, command_rx) = mpsc::channel();
        let control = ControlHandle {
            tx: command_tx,
            telemetry: telemetry.clone(),
            runtime_boundary: boundary.clone(),
        };
        let state_for_thread = Arc::clone(&shared_state);
        let auth = node.config.auth.clone();
        let storage = node.config.storage.clone();

        let runtime_thread = thread::Builder::new()
            .name("burn-p2p-runtime".into())
            .spawn(move || {
                run_control_plane(
                    boundary,
                    keypair,
                    storage,
                    auth,
                    command_rx,
                    state_for_thread,
                )
            })
            .map_err(|error| anyhow::anyhow!("failed to spawn runtime thread: {error}"))?;

        Ok(Self {
            node: Some(node),
            telemetry,
            control,
            training_prefetch: None,
            diffusion_state: crate::promotion::diffusion::DiffusionStateCache::default(),
            validation_cache: None,
            runtime_thread: Some(runtime_thread),
        })
    }

    /// Performs the mainnet operation.
    pub fn mainnet(&self) -> &MainnetHandle {
        &self
            .node
            .as_ref()
            .expect("running node should retain prepared node")
            .mainnet
    }

    /// Performs the config operation.
    pub fn config(&self) -> &NodeConfig {
        &self
            .node
            .as_ref()
            .expect("running node should retain prepared node")
            .config
    }

    /// Performs the checkpoint sync operation.
    pub fn checkpoint_sync(&self, target_head_id: HeadId) -> CheckpointSyncHandle {
        self.mainnet().checkpoint_sync(target_head_id)
    }

    /// Performs the experiment operation.
    pub fn experiment(
        &self,
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
    ) -> ExperimentHandle {
        self.mainnet()
            .experiment(study_id, experiment_id, revision_id)
    }

    /// Performs the telemetry operation.
    pub fn telemetry(&self) -> TelemetryHandle {
        self.telemetry.clone()
    }

    pub(crate) fn update_runtime_state(
        &self,
        node_state: NodeRuntimeState,
        slot_state: Option<SlotRuntimeState>,
    ) {
        let mut snapshot = self
            .telemetry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let persist_security_state = node_state != NodeRuntimeState::ShuttingDown;
        snapshot.set_node_state(node_state);
        if let Some(slot_state) = slot_state {
            snapshot.set_primary_slot_state(slot_state);
        }
        if persist_security_state
            && let Some(storage) = self.config().storage.as_ref()
            && let Err(error) = persist_runtime_security_state(storage, &snapshot)
        {
            snapshot.last_error = Some(format!("failed to persist security state: {error}"));
        }
    }

    pub(crate) fn update_lag_status(
        &self,
        lag_state: LagState,
        head_lag_steps: u64,
        lag_policy: LagPolicy,
    ) {
        let mut snapshot = self
            .telemetry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.set_lag_status(lag_state, head_lag_steps, lag_policy);
        if let Some(storage) = self.config().storage.as_ref()
            && let Err(error) = persist_runtime_security_state(storage, &snapshot)
        {
            snapshot.last_error = Some(format!("failed to persist security state: {error}"));
        }
    }

    pub(crate) fn lag_policy(&self, experiment: &ExperimentHandle) -> LagPolicy {
        self.visible_experiment_entry(
            &experiment.study_id,
            &experiment.experiment_id,
            &experiment.revision_id,
        )
        .map(|entry| entry.lag_policy())
        .unwrap_or_default()
    }

    pub(crate) fn assess_and_record_lag(
        &self,
        storage: &StorageConfig,
        experiment: &ExperimentHandle,
        snapshots: &[(PeerId, ControlPlaneSnapshot)],
    ) -> anyhow::Result<LagAssessment> {
        let lag_policy = self.lag_policy(experiment);
        let assessment = assess_head_lag(storage, experiment, snapshots, &lag_policy)?;
        self.update_lag_status(assessment.state, assessment.head_lag_steps, lag_policy);
        Ok(assessment)
    }

    pub(crate) fn persist_transfer_snapshot(&self, snapshot: &mut NodeTelemetrySnapshot) {
        if let Some(storage) = self.config().storage.as_ref()
            && let Err(error) =
                persist_in_flight_transfer_states(storage, &snapshot.in_flight_transfers)
        {
            snapshot.last_error = Some(format!("failed to persist transfer state: {error}"));
        }
    }

    pub(crate) fn record_transfer_state(&self, transfer_state: ArtifactTransferState) {
        let mut snapshot = self
            .telemetry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.update_transfer_state(transfer_state);
        self.persist_transfer_snapshot(&mut snapshot);
    }

    pub(crate) fn clear_transfer_state(&self, artifact_id: &ArtifactId) {
        let mut snapshot = self
            .telemetry
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.clear_transfer_state(artifact_id);
        self.persist_transfer_snapshot(&mut snapshot);
        if let Some(storage) = self.config().storage.as_ref()
            && let Err(error) = remove_artifact_transfer_state(storage, artifact_id)
        {
            snapshot.last_error = Some(format!("failed to clear transfer state: {error}"));
        }
    }

    pub(crate) fn redial_known_candidates(&self, candidates: &[PeerId]) {
        if candidates.is_empty() {
            return;
        }
        let telemetry_snapshot = self.telemetry().snapshot();
        let connected = connected_peer_ids(&telemetry_snapshot);
        let dial_targets = telemetry_snapshot
            .control_plane
            .peer_directory_announcements
            .iter()
            .filter(|announcement| candidates.contains(&announcement.peer_id))
            .filter(|announcement| !connected.contains(&announcement.peer_id))
            .flat_map(|announcement| announcement.addresses.iter().cloned())
            .filter(|address| !telemetry_snapshot.listen_addresses.contains(address))
            .collect::<BTreeSet<_>>();
        for address in dial_targets {
            let _ = self.control.dial_address(address);
        }
    }

    pub(crate) fn set_experiment_idle_state(
        &self,
        experiment: &ExperimentHandle,
        node_state: NodeRuntimeState,
    ) {
        self.update_runtime_state(
            node_state,
            Some(SlotRuntimeState::Assigned(
                SlotAssignmentState::from_experiment(experiment),
            )),
        );
    }

    /// Performs the control handle operation.
    pub fn control_handle(&self) -> ControlHandle {
        self.control.clone()
    }

    /// Performs the shutdown operation.
    pub fn shutdown(&self) -> anyhow::Result<()> {
        self.update_runtime_state(NodeRuntimeState::ShuttingDown, None);
        self.control.shutdown()
    }

    /// Performs the await termination operation.
    pub fn await_termination(mut self) -> anyhow::Result<Node<P>> {
        if let Some(mut training_prefetch) = self.training_prefetch.take()
            && training_prefetch
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished)
            && let Some(handle) = training_prefetch.handle.take()
        {
            let _ = handle.join();
        }
        if let Some(runtime_thread) = self.runtime_thread.take() {
            runtime_thread
                .join()
                .map_err(|_| anyhow::anyhow!("runtime thread panicked"))?;
        }

        self.node
            .take()
            .ok_or_else(|| anyhow::anyhow!("running node already consumed"))
    }

    /// Performs the await termination operation with a bounded timeout.
    pub fn await_termination_timeout(mut self, timeout: Duration) -> anyhow::Result<Node<P>> {
        if let Some(mut training_prefetch) = self.training_prefetch.take()
            && training_prefetch
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished)
            && let Some(handle) = training_prefetch.handle.take()
        {
            let _ = handle.join();
        }
        if let Some(runtime_thread) = self.runtime_thread.take() {
            let deadline = Instant::now() + timeout;
            while !runtime_thread.is_finished() {
                if Instant::now() >= deadline {
                    anyhow::bail!("timed out waiting for runtime thread termination");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            runtime_thread
                .join()
                .map_err(|_| anyhow::anyhow!("runtime thread panicked"))?;
        }

        self.node
            .take()
            .ok_or_else(|| anyhow::anyhow!("running node already consumed"))
    }
}
