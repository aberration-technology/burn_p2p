use super::*;
use crate::runtime_support::{
    best_head_by_ids_from_snapshots, directory_current_head_ids_from_snapshots,
    head_provider_peers, latest_compatible_directory_entries, load_slot_assignments,
    persist_slot_assignments, resolve_canonical_head, runtime_window_reducers,
};
use anyhow::Context;

#[derive(Clone, Copy)]
enum HeadSyncTargetMode {
    DirectoryCurrent,
    LatestPromoted,
}

impl<P> RunningNode<P> {
    /// Performs the list experiments operation.
    pub fn list_experiments(&self) -> Vec<ExperimentDirectoryEntry> {
        let scopes = self
            .config()
            .auth
            .as_ref()
            .and_then(|auth| auth.local_peer_auth.as_ref())
            .map(|envelope| {
                envelope
                    .certificate
                    .claims()
                    .experiment_scopes
                    .iter()
                    .cloned()
                    .chain(envelope.requested_scopes.iter().cloned())
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();

        let telemetry = self.telemetry().snapshot();
        let trusted_baseline = self
            .config()
            .auth
            .as_ref()
            .map(|auth| auth.experiment_directory.as_slice())
            .unwrap_or_default();
        let directory = latest_compatible_directory_entries(
            &telemetry.control_plane.directory_announcements,
            &self.mainnet().genesis.network_id,
            trusted_baseline,
        );

        let directory = ExperimentDirectory {
            network_id: self.mainnet().genesis.network_id.clone(),
            generated_at: Utc::now(),
            entries: directory,
        };

        directory.visible_to(&scopes).into_iter().cloned().collect()
    }

    pub(crate) fn visible_experiment_entry(
        &self,
        study_id: &StudyId,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
    ) -> anyhow::Result<ExperimentDirectoryEntry> {
        self.list_experiments()
            .into_iter()
            .find(|entry| {
                entry.study_id == *study_id
                    && entry.experiment_id == *experiment_id
                    && entry.current_revision_id == *revision_id
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "experiment {} is not visible to the current node scope",
                    experiment_id.as_str()
                )
            })
    }

    pub(crate) fn persist_primary_assignment(
        &self,
        assignment: &SlotAssignmentState,
    ) -> anyhow::Result<()> {
        if let Some(storage) = self.config().storage.as_ref() {
            let mut assignments = load_slot_assignments(storage)?;
            if assignments.is_empty() {
                assignments.push(assignment.clone());
            } else {
                assignments[0] = assignment.clone();
            }
            persist_primary_slot_assignment(storage, assignment)?;
            persist_slot_assignments(storage, &assignments)?;
        }
        Ok(())
    }

    /// Performs the select experiment operation.
    pub fn select_experiment(
        &self,
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
    ) -> anyhow::Result<ExperimentHandle> {
        let entry = self.visible_experiment_entry(&study_id, &experiment_id, &revision_id)?;
        Ok(self.experiment(
            entry.study_id,
            entry.experiment_id,
            entry.current_revision_id,
        ))
    }

    pub(crate) fn ensure_experiment_topics(
        &self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<()> {
        let overlays = experiment.overlay_set()?;
        self.control.subscribe_topic(overlays.control.clone())?;
        for topic in overlays.experiment_topics() {
            self.control.subscribe_topic(topic)?;
        }
        Ok(())
    }

    pub(crate) fn fetch_experiment_snapshots(
        &self,
        experiment: &ExperimentHandle,
        timeout: Duration,
    ) -> anyhow::Result<Vec<(PeerId, ControlPlaneSnapshot)>> {
        let telemetry_snapshot = self.telemetry().snapshot();
        let peer_ids = prioritized_experiment_snapshot_peer_ids(&telemetry_snapshot, experiment);
        self.fetch_targeted_snapshots_with_cache(timeout, true, Some(peer_ids))
    }

    pub(crate) fn fetch_targeted_snapshots_with_cache(
        &self,
        timeout: Duration,
        include_cached: bool,
        target_peer_ids: Option<Vec<PeerId>>,
    ) -> anyhow::Result<Vec<(PeerId, ControlPlaneSnapshot)>> {
        const SNAPSHOT_FETCH_PARALLELISM: usize = 4;

        let admission_policy = self.effective_admission_policy();
        let telemetry_snapshot = self.telemetry().snapshot();
        let mut snapshots = if include_cached {
            cached_connected_snapshots(&telemetry_snapshot)
                .into_iter()
                .collect::<BTreeMap<_, _>>()
        } else {
            BTreeMap::new()
        };
        let mut connected_peers = connected_peer_ids(&telemetry_snapshot)
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(target_peer_ids) = target_peer_ids {
            connected_peers = dedupe_peer_ids(target_peer_ids.into_iter().chain(connected_peers));
        }
        if connected_peers.is_empty() {
            return Ok(snapshots.into_iter().collect());
        }

        let worker_count = connected_peers.len().clamp(1, SNAPSHOT_FETCH_PARALLELISM);
        let chunk_size = connected_peers.len().div_ceil(worker_count);
        let mut workers = Vec::with_capacity(worker_count);
        for chunk in connected_peers.chunks(chunk_size) {
            let control = self.control.clone();
            let admission_policy = admission_policy.clone();
            let peers = chunk.to_vec();
            workers.push(thread::spawn(
                move || -> BTreeMap<PeerId, ControlPlaneSnapshot> {
                    let mut fetched = BTreeMap::new();
                    for peer_id in peers {
                        let Ok(snapshot) = control.fetch_snapshot(peer_id.as_str(), timeout) else {
                            continue;
                        };
                        if let Some(policy) = admission_policy.as_ref() {
                            let Ok(report) = verify_snapshot_admission(policy, &peer_id, &snapshot)
                            else {
                                continue;
                            };
                            if !matches!(report.decision(), AdmissionDecision::Allow) {
                                continue;
                            }
                        }
                        fetched.insert(peer_id, snapshot);
                    }
                    fetched
                },
            ));
        }

        for worker in workers {
            let fetched = worker
                .join()
                .map_err(|_| anyhow::anyhow!("snapshot fetch worker panicked"))?;
            snapshots.extend(fetched);
        }

        Ok(snapshots.into_iter().collect())
    }

    fn fetch_bootstrap_snapshots(
        &self,
        timeout: Duration,
    ) -> anyhow::Result<Vec<(PeerId, ControlPlaneSnapshot)>> {
        let telemetry_snapshot = self.telemetry().snapshot();
        let addresses = self
            .control
            .runtime_boundary
            .bootstrap_addresses
            .iter()
            .filter(|address| !telemetry_snapshot.listen_addresses.contains(address))
            .cloned()
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Ok(Vec::new());
        }

        let admission_policy = self.effective_admission_policy();
        let deadline = Instant::now() + timeout;
        let mut snapshots = BTreeMap::new();
        let address_count = addresses.len();
        for (index, address) in addresses.into_iter().enumerate() {
            let remaining_candidates = address_count.saturating_sub(index).max(1);
            let Some(attempt_timeout) =
                fair_request_timeout(deadline, timeout, remaining_candidates)
            else {
                break;
            };
            match self
                .control
                .fetch_snapshot_from_address(address, attempt_timeout)
            {
                Ok((peer_id, snapshot)) => {
                    if let Some(policy) = admission_policy.as_ref() {
                        let report = verify_snapshot_admission(policy, &peer_id, &snapshot)?;
                        if !matches!(report.decision(), AdmissionDecision::Allow) {
                            continue;
                        }
                    }
                    self.ingest_control_plane_snapshot(&snapshot)?;
                    snapshots.insert(peer_id, snapshot);
                }
                Err(_) => continue,
            }
        }

        Ok(snapshots.into_iter().collect())
    }

    /// Performs the initialize local head operation.
    pub fn initialize_local_head(
        &mut self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<HeadDescriptor>
    where
        P: P2pWorkload,
    {
        let assignment = SlotAssignmentState::from_experiment(experiment);
        self.persist_primary_assignment(&assignment)?;
        self.update_runtime_state(
            NodeRuntimeState::HeadSync,
            Some(SlotRuntimeState::MaterializingBase(assignment.clone())),
        );
        self.ensure_experiment_topics(experiment)?;
        let storage =
            self.config().storage.as_ref().cloned().ok_or_else(|| {
                anyhow::anyhow!("initializing a head requires configured storage")
            })?;
        let store = FsArtifactStore::new(storage.root.clone());
        store.ensure_layout()?;

        let local_peer_id = self
            .telemetry()
            .snapshot()
            .local_peer_id
            .ok_or_else(|| anyhow::anyhow!("runtime does not have a local peer id yet"))?;
        let (revision_contract, require_signed_revision_contracts) = {
            let node = self
                .node
                .as_ref()
                .expect("running node should retain prepared node");
            (
                node.revision_contracts
                    .get(&experiment.revision_id)
                    .cloned(),
                node.require_signed_revision_contracts,
            )
        };
        if require_signed_revision_contracts && revision_contract.is_none() {
            anyhow::bail!(
                "revision {} has no authority-signed training contract",
                experiment.revision_id.as_str()
            );
        }
        if let Some(contract) = &revision_contract {
            contract.validate()?;
            anyhow::ensure!(
                contract.revision.experiment_id == experiment.experiment_id
                    && contract.revision.revision_id == experiment.revision_id,
                "revision contract does not belong to experiment {} revision {}",
                experiment.experiment_id.as_str(),
                experiment.revision_id.as_str(),
            );
        }

        let (expected_artifact, created_at) = revision_contract
            .as_ref()
            .map(|contract| {
                (
                    Some(contract.genesis.payload.payload.artifact.clone()),
                    contract.genesis.payload.payload.created_at,
                )
            })
            .unwrap_or((None, Utc::now()));
        let local_model_schema_hash = self.current_workload_model_schema_hash()?;
        let head_id = expected_artifact
            .as_ref()
            .and_then(|artifact| artifact.head_id.clone())
            .unwrap_or_else(|| unsigned_genesis_head_id(experiment, &local_model_schema_hash));
        let project = &mut self
            .node
            .as_mut()
            .expect("running node should retain prepared node")
            .project;
        let device = project.runtime_device();
        let initialized_model = project.init_model(&device);
        let (model, artifact) = match expected_artifact {
            Some(expected_artifact)
                if store.has_complete_artifact(&expected_artifact.artifact_id)? =>
            {
                let stored_artifact = store.load_manifest(&expected_artifact.artifact_id)?;
                anyhow::ensure!(
                    stored_artifact == expected_artifact,
                    "pre-provisioned genesis artifact {} does not match its authority-signed descriptor",
                    expected_artifact.artifact_id.as_str(),
                );
                let contract = revision_contract
                    .as_ref()
                    .expect("authority-signed genesis artifact requires its revision contract");
                let model = project.load_genesis_artifact(
                    initialized_model,
                    crate::GenesisArtifactLoadContext {
                        descriptor: &stored_artifact,
                        training_contract_id: &contract.training_contract_id,
                        contract: &contract.training,
                        materialization: &contract.genesis.payload.payload.materialization,
                        store: &store,
                        device: &device,
                    },
                )?;
                (model, stored_artifact)
            }
            expected_artifact => {
                let artifact = if let Some(contract) = revision_contract.as_ref() {
                    project.materialize_genesis_artifact(
                        crate::GenesisArtifactMaterializationContext {
                            model: &initialized_model,
                            head_id: head_id.clone(),
                            training_contract_id: &contract.training_contract_id,
                            contract: &contract.training,
                            materialization: &contract.genesis.payload.payload.materialization,
                            store: &store,
                        },
                    )?
                } else {
                    project.materialize_model_artifact(
                        &initialized_model,
                        ArtifactKind::FullHead,
                        head_id.clone(),
                        None,
                        &store,
                    )?
                };
                if let Some(expected_artifact) = expected_artifact {
                    anyhow::ensure!(
                        artifact == expected_artifact,
                        "locally materialized genesis artifact {} does not match authority-signed artifact {}",
                        artifact.artifact_id.as_str(),
                        expected_artifact.artifact_id.as_str(),
                    );
                }
                let model = if let Some(contract) = revision_contract.as_ref() {
                    project.load_genesis_artifact(
                        initialized_model,
                        crate::GenesisArtifactLoadContext {
                            descriptor: &artifact,
                            training_contract_id: &contract.training_contract_id,
                            contract: &contract.training,
                            materialization: &contract.genesis.payload.payload.materialization,
                            store: &store,
                            device: &device,
                        },
                    )?
                } else {
                    project.load_model_artifact(initialized_model, &artifact, &store, &device)?
                };
                (model, artifact)
            }
        };
        if let Some(contract) = &revision_contract {
            let actual_tensor_digest = project.model_tensor_digest(&model).with_context(|| {
                format!(
                    "compute decoded tensor digest for signed genesis {}",
                    artifact.artifact_id.as_str(),
                )
            })?;
            ensure_genesis_tensor_digest(
                &actual_tensor_digest,
                &contract.genesis.payload.payload.tensor_digest,
            )?;
        }
        let evaluation = project.evaluate(&model, EvalSplit::Validation);
        let head = HeadDescriptor {
            head_id: head_id.clone(),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            parent_head_id: None,
            global_step: 0,
            created_at,
            metrics: evaluation.metrics,
        };

        persist_head_state(&storage, experiment, &head)?;
        persist_json(storage.scoped_head_path(&head.head_id), &head)?;
        store.pin_head(&head.head_id)?;
        store.pin_artifact(&artifact.artifact_id)?;
        self.update_runtime_state(
            NodeRuntimeState::PublishingUpdate,
            Some(SlotRuntimeState::Publishing(assignment)),
        );
        self.publish_artifact_from_store(&artifact.artifact_id)?;
        self.control.publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set()?.heads,
            provider_peer_id: Some(local_peer_id),
            head: head.clone(),
            announced_at: Utc::now(),
        })?;
        self.set_experiment_idle_state(experiment, NodeRuntimeState::IdleReady);

        Ok(head)
    }

    /// Synchronizes the experiment head.
    pub fn sync_experiment_head(
        &self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<Option<HeadDescriptor>>
    where
        P: P2pWorkload,
    {
        self.sync_experiment_head_with_mode(experiment, HeadSyncTargetMode::DirectoryCurrent)
    }

    /// Synchronizes the latest promoted experiment head.
    ///
    /// This is intended for trusted head mirrors and other durability services
    /// that must promote a newly merged/diffusion-settled head even while the
    /// directory's `current_head_id` still points at the previous durable head.
    /// Training peers should normally use [`Self::sync_experiment_head`].
    pub fn sync_latest_promoted_experiment_head(
        &self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<Option<HeadDescriptor>>
    where
        P: P2pWorkload,
    {
        self.sync_experiment_head_with_mode(experiment, HeadSyncTargetMode::LatestPromoted)
    }

    fn sync_experiment_head_with_mode(
        &self,
        experiment: &ExperimentHandle,
        target_mode: HeadSyncTargetMode,
    ) -> anyhow::Result<Option<HeadDescriptor>>
    where
        P: P2pWorkload,
    {
        const HEAD_SYNC_SNAPSHOT_TIMEOUT: Duration = Duration::from_millis(750);
        // Head adoption may require pulling a newly promoted artifact from the
        // network. Give larger runtime payloads enough room to materialize
        // before the caller falls back to another outer retry loop.
        let head_sync_wait_timeout = head_sync_wait_timeout();
        const HEAD_SYNC_POLL_INTERVAL: Duration = Duration::from_millis(50);

        let assignment = SlotAssignmentState::from_experiment(experiment);
        self.persist_primary_assignment(&assignment)?;
        self.update_runtime_state(
            NodeRuntimeState::HeadSync,
            Some(SlotRuntimeState::MaterializingBase(assignment)),
        );
        self.ensure_experiment_topics(experiment)?;
        let storage = match self.config().storage.as_ref() {
            Some(storage) => storage.clone(),
            None => return Ok(None),
        };
        let telemetry_snapshot = self.telemetry().snapshot();
        let cached_snapshots = cached_connected_snapshots(&telemetry_snapshot);
        let cached_canonical_snapshots = snapshots_with_local_control_plane(
            &cached_snapshots,
            telemetry_snapshot.local_peer_id.as_ref(),
            &telemetry_snapshot.control_plane,
        );
        let expected_directory_entry = self
            .visible_experiment_entry(
                &experiment.study_id,
                &experiment.experiment_id,
                &experiment.revision_id,
            )
            .ok();
        let local_directory_current_head_ids = expected_directory_entry
            .as_ref()
            .and_then(|entry| entry.current_head_id.as_ref())
            .cloned()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut snapshots = cached_snapshots;
        let mut resolved_head = resolve_sync_target_head(
            &storage,
            experiment,
            &cached_canonical_snapshots,
            &local_directory_current_head_ids,
            expected_directory_entry.as_ref(),
            target_mode,
        )?;

        if resolved_head.is_none() {
            snapshots = self.fetch_experiment_snapshots(experiment, HEAD_SYNC_SNAPSHOT_TIMEOUT)?;
            let canonical_snapshots = snapshots_with_local_control_plane(
                &snapshots,
                telemetry_snapshot.local_peer_id.as_ref(),
                &telemetry_snapshot.control_plane,
            );
            resolved_head = resolve_sync_target_head(
                &storage,
                experiment,
                &canonical_snapshots,
                &local_directory_current_head_ids,
                expected_directory_entry.as_ref(),
                target_mode,
            )?;
        }
        if resolved_head.is_none() {
            let bootstrap_snapshots = self.fetch_bootstrap_snapshots(ci_scaled_timeout(
                Duration::from_secs(3),
                Duration::from_secs(10),
            ))?;
            if !bootstrap_snapshots.is_empty() {
                let mut snapshots_by_peer = snapshots.into_iter().collect::<BTreeMap<_, _>>();
                snapshots_by_peer.extend(bootstrap_snapshots);
                snapshots = snapshots_by_peer.into_iter().collect();
                let refreshed_telemetry_snapshot = self.telemetry().snapshot();
                let canonical_snapshots = snapshots_with_local_control_plane(
                    &snapshots,
                    refreshed_telemetry_snapshot.local_peer_id.as_ref(),
                    &refreshed_telemetry_snapshot.control_plane,
                );
                resolved_head = resolve_sync_target_head(
                    &storage,
                    experiment,
                    &canonical_snapshots,
                    &local_directory_current_head_ids,
                    expected_directory_entry.as_ref(),
                    target_mode,
                )?;
            }
        }
        let _ = self.assess_and_record_lag(&storage, experiment, &snapshots)?;
        let Some((mut source_peer_id, mut head)) = resolved_head else {
            let connected_peers = connected_peer_ids(&telemetry_snapshot);
            let bootstrap_addresses = self
                .control
                .runtime_boundary
                .bootstrap_addresses
                .iter()
                .filter(|address| !telemetry_snapshot.listen_addresses.contains(address))
                .cloned()
                .collect::<BTreeSet<_>>();
            let provider_addresses = telemetry_snapshot
                .control_plane
                .peer_directory_announcements
                .iter()
                .filter(|announcement| !connected_peers.contains(&announcement.peer_id))
                .flat_map(|announcement| announcement.addresses.iter().cloned())
                .filter(|address| !telemetry_snapshot.listen_addresses.contains(address))
                .collect::<BTreeSet<_>>();
            for address in bootstrap_addresses.into_iter().chain(provider_addresses) {
                let _ = self.control.dial_address(address);
            }
            return Ok(None);
        };
        let store = FsArtifactStore::new(storage.root.clone());
        if !store.has_complete_artifact(&head.artifact_id)? {
            let expected_model_schema_hash =
                self.expected_model_schema_hash_for_experiment(experiment)?;
            let bootstrap_snapshots = self.fetch_bootstrap_snapshots(ci_scaled_timeout(
                Duration::from_secs(3),
                Duration::from_secs(10),
            ))?;
            if !bootstrap_snapshots.is_empty() {
                let mut snapshots_by_peer = snapshots.into_iter().collect::<BTreeMap<_, _>>();
                snapshots_by_peer.extend(bootstrap_snapshots);
                snapshots = snapshots_by_peer.into_iter().collect();
                let refreshed_telemetry_snapshot = self.telemetry().snapshot();
                let canonical_snapshots = snapshots_with_local_control_plane(
                    &snapshots,
                    refreshed_telemetry_snapshot.local_peer_id.as_ref(),
                    &refreshed_telemetry_snapshot.control_plane,
                );
                if let Some((refreshed_source_peer_id, refreshed_head)) = resolve_sync_target_head(
                    &storage,
                    experiment,
                    &canonical_snapshots,
                    &local_directory_current_head_ids,
                    expected_directory_entry.as_ref(),
                    target_mode,
                )? {
                    source_peer_id = refreshed_source_peer_id;
                    head = refreshed_head;
                }
            }
            let provider_peer_ids = head_provider_peers(
                Some(&source_peer_id),
                &snapshots,
                &telemetry_snapshot.control_plane,
                telemetry_snapshot.local_peer_id.as_ref(),
                experiment,
                &head,
            );
            let connected_peer_ids = connected_peer_ids(&self.telemetry().snapshot())
                .into_iter()
                .collect::<BTreeSet<_>>();
            let provider_peer_ids =
                prioritize_connected_provider_peers(provider_peer_ids, &connected_peer_ids);
            let deadline = Instant::now() + head_sync_wait_timeout;
            loop {
                let result = if provider_peer_ids.is_empty() {
                    self.sync_artifact_from_peer_bounded_for_model_schema(
                        &source_peer_id,
                        head.artifact_id.clone(),
                        &expected_model_schema_hash,
                        head_sync_wait_timeout,
                    )
                    .map(|_| ())
                } else {
                    self.wait_for_artifact_from_peers_for_model_schema(
                        &provider_peer_ids,
                        &head.artifact_id,
                        &expected_model_schema_hash,
                        head_sync_wait_timeout,
                    )
                };
                match result {
                    Ok(()) => break,
                    Err(error)
                        if is_transient_artifact_sync_error(&error)
                            && Instant::now() < deadline => {}
                    Err(error) if is_transient_artifact_sync_error(&error) => return Ok(None),
                    Err(error) => return Err(error),
                }
                std::thread::sleep(HEAD_SYNC_POLL_INTERVAL);
            }
        }
        self.ensure_head_artifact_matches_workload(&store, experiment, &head)?;
        persist_head_state(&storage, experiment, &head)?;
        persist_json(storage.scoped_head_path(&head.head_id), &head)?;
        store.pin_head(&head.head_id)?;
        store.pin_artifact(&head.artifact_id)?;
        let telemetry_snapshot = self.telemetry().snapshot();
        if let Some(local_peer_id) = telemetry_snapshot.local_peer_id.clone() {
            let already_announced = telemetry_snapshot
                .control_plane
                .head_announcements
                .iter()
                .any(|announcement| {
                    announcement.provider_peer_id.as_ref() == Some(&local_peer_id)
                        && announcement.head.head_id == head.head_id
                        && announcement.head.artifact_id == head.artifact_id
                });
            if !already_announced {
                self.publish_artifact_from_store(&head.artifact_id)?;
                self.control.publish_head(HeadAnnouncement {
                    overlay: experiment.overlay_set()?.heads,
                    provider_peer_id: Some(local_peer_id),
                    head: head.clone(),
                    announced_at: Utc::now(),
                })?;
            }
        }
        self.update_lag_status(LagState::Current, 0, self.lag_policy(experiment));
        self.set_experiment_idle_state(experiment, NodeRuntimeState::IdleReady);
        Ok(Some(head))
    }

    /// Waits until the runtime can materialize a canonical experiment head.
    pub fn wait_for_experiment_head(
        &self,
        experiment: &ExperimentHandle,
        timeout: Duration,
    ) -> anyhow::Result<HeadDescriptor>
    where
        P: P2pWorkload,
    {
        const HEAD_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

        let deadline = Instant::now() + timeout;
        let mut last_error = None;
        while Instant::now() < deadline {
            match self.sync_experiment_head(experiment) {
                Ok(Some(head)) => return Ok(head),
                Ok(None) => {}
                Err(error) => last_error = Some(error.to_string()),
            }
            std::thread::sleep(HEAD_WAIT_POLL_INTERVAL);
        }

        if let Some(error) = last_error {
            anyhow::bail!("timed out waiting for experiment head sync: {error}");
        }
        anyhow::bail!("timed out waiting for experiment head sync")
    }

    /// Waits until the runtime has adopted one specific known head.
    pub fn wait_for_known_head(
        &self,
        experiment: &ExperimentHandle,
        expected_head: &HeadDescriptor,
        timeout: Duration,
    ) -> anyhow::Result<HeadDescriptor>
    where
        P: P2pWorkload,
    {
        const HEAD_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

        if self.adopt_known_head_if_present(experiment, expected_head)? {
            return Ok(expected_head.clone());
        }

        let deadline = Instant::now() + timeout;
        let mut last_error = None;
        while Instant::now() < deadline {
            match self.sync_experiment_head(experiment) {
                Ok(Some(head)) if head.head_id == expected_head.head_id => return Ok(head),
                Ok(Some(_)) | Ok(None) => {
                    if self.adopt_known_head_if_present(experiment, expected_head)? {
                        return Ok(expected_head.clone());
                    }
                }
                Err(error) => last_error = Some(error.to_string()),
            }
            std::thread::sleep(HEAD_WAIT_POLL_INTERVAL);
        }

        if self.adopt_known_head_if_present(experiment, expected_head)? {
            return Ok(expected_head.clone());
        }
        if let Some(error) = last_error {
            anyhow::bail!(
                "timed out waiting for known head {}: {error}",
                expected_head.head_id.as_str()
            );
        }
        anyhow::bail!(
            "timed out waiting for known head {}",
            expected_head.head_id.as_str()
        )
    }

    /// Prewarms one artifact from any currently known provider peer.
    pub fn wait_for_artifact_from_peers(
        &self,
        provider_peer_ids: &[PeerId],
        artifact_id: &ArtifactId,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        self.wait_for_artifact_from_peers_with_model_schema(
            provider_peer_ids,
            artifact_id,
            None,
            timeout,
        )
    }

    fn wait_for_artifact_from_peers_for_model_schema(
        &self,
        provider_peer_ids: &[PeerId],
        artifact_id: &ArtifactId,
        expected_model_schema_hash: &ContentId,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        self.wait_for_artifact_from_peers_with_model_schema(
            provider_peer_ids,
            artifact_id,
            Some(expected_model_schema_hash),
            timeout,
        )
    }

    fn wait_for_artifact_from_peers_with_model_schema(
        &self,
        provider_peer_ids: &[PeerId],
        artifact_id: &ArtifactId,
        expected_model_schema_hash: Option<&ContentId>,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        const ARTIFACT_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(100);

        let Some(store) = self.artifact_store() else {
            anyhow::bail!("artifact prewarm requires configured storage");
        };
        if store.has_complete_artifact(artifact_id)? {
            return Ok(());
        }
        anyhow::ensure!(
            !provider_peer_ids.is_empty(),
            "artifact {} does not have any provider peers to fetch from",
            artifact_id.as_str()
        );

        let deadline = Instant::now() + timeout;
        let mut last_error = None;
        while Instant::now() < deadline {
            if store.has_complete_artifact(artifact_id)? {
                return Ok(());
            }

            let telemetry_snapshot = self.telemetry().snapshot();
            let transfer_state = telemetry_snapshot.in_flight_transfers.get(artifact_id);
            let transfer_provider_peer_id =
                transfer_state.and_then(|state| state.provider_peer_id.as_ref());
            let initial_connected_peer_ids = connected_peer_ids(&telemetry_snapshot)
                .into_iter()
                .collect::<BTreeSet<_>>();
            let prioritized_provider_peer_ids = prioritized_artifact_provider_peers(
                provider_peer_ids,
                transfer_provider_peer_id,
                &initial_connected_peer_ids,
            );

            for (provider_index, provider_peer_id) in
                prioritized_provider_peer_ids.iter().enumerate()
            {
                let transfer_snapshot = self.telemetry().snapshot();
                let transfer_state = transfer_snapshot
                    .in_flight_transfers
                    .get(artifact_id)
                    .cloned();
                let transfer_connected_peer_ids = connected_peer_ids(&transfer_snapshot)
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                let transfer_in_progress = transfer_state.as_ref().is_some_and(|state| {
                    state.provider_peer_id.as_ref() == Some(provider_peer_id)
                        && !state.completed_chunks.is_empty()
                        && transfer_connected_peer_ids.contains(provider_peer_id)
                });
                let completed_chunks_before = transfer_state
                    .as_ref()
                    .map_or(0, |state| state.completed_chunks.len());
                let remaining_candidates = prioritized_provider_peer_ids
                    .len()
                    .saturating_sub(provider_index);
                let Some(sync_timeout) = artifact_sync_attempt_timeout(
                    deadline,
                    timeout,
                    remaining_candidates,
                    transfer_in_progress,
                ) else {
                    break;
                };
                let sync_result = match expected_model_schema_hash {
                    Some(expected_model_schema_hash) => self
                        .sync_artifact_from_peer_bounded_for_model_schema(
                            provider_peer_id,
                            artifact_id.clone(),
                            expected_model_schema_hash,
                            sync_timeout,
                        ),
                    None => self.sync_artifact_from_peer_bounded(
                        provider_peer_id,
                        artifact_id.clone(),
                        sync_timeout,
                    ),
                };
                match sync_result {
                    Ok(_) => return Ok(()),
                    Err(error) => {
                        if error
                            .downcast_ref::<super::artifacts::ArtifactModelSchemaMismatch>()
                            .is_some()
                        {
                            return Err(error);
                        }
                        last_error = Some(format!(
                            "could not fetch {} from {}: {error}",
                            artifact_id.as_str(),
                            provider_peer_id.as_str(),
                        ));
                        let transfer_state = self
                            .telemetry()
                            .snapshot()
                            .in_flight_transfers
                            .get(artifact_id)
                            .cloned();
                        let made_progress = transfer_state.as_ref().is_some_and(|state| {
                            state.completed_chunks.len() > completed_chunks_before
                        });
                        if made_progress {
                            break;
                        }
                    }
                }
            }

            std::thread::sleep(ARTIFACT_WAIT_POLL_INTERVAL);
        }

        if store.has_complete_artifact(artifact_id)? {
            return Ok(());
        }
        if let Some(error) = last_error {
            anyhow::bail!("{error}");
        }
        anyhow::bail!(
            "timed out waiting for artifact {} from providers",
            artifact_id.as_str()
        )
    }

    /// Adopts a known head descriptor once its artifact is available locally.
    pub fn adopt_known_head_if_present(
        &self,
        experiment: &ExperimentHandle,
        head: &HeadDescriptor,
    ) -> anyhow::Result<bool>
    where
        P: P2pWorkload,
    {
        let Some(storage) = self.config().storage.as_ref().cloned() else {
            return Ok(false);
        };
        self.ensure_experiment_topics(experiment)?;
        let store = FsArtifactStore::new(storage.root.clone());
        if !store.has_complete_artifact(&head.artifact_id)? {
            return Ok(false);
        }
        self.ensure_head_artifact_matches_workload(&store, experiment, head)?;

        persist_head_state(&storage, experiment, head)?;
        persist_json(storage.scoped_head_path(&head.head_id), head)?;
        store.pin_head(&head.head_id)?;
        store.pin_artifact(&head.artifact_id)?;
        self.update_lag_status(LagState::Current, 0, self.lag_policy(experiment));
        self.set_experiment_idle_state(experiment, NodeRuntimeState::IdleReady);
        Ok(true)
    }

    /// Publishes a locally materialized head as an available provider without
    /// updating the experiment's canonical current head.
    pub fn publish_head_provider(
        &self,
        experiment: &ExperimentHandle,
        head: &HeadDescriptor,
    ) -> anyhow::Result<()> {
        self.ensure_experiment_topics(experiment)?;
        self.publish_artifact_from_store(&head.artifact_id)?;
        let local_peer_id = self
            .telemetry()
            .snapshot()
            .local_peer_id
            .ok_or_else(|| anyhow::anyhow!("runtime does not have a local peer id yet"))?;
        self.control.publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set()?.heads,
            provider_peer_id: Some(local_peer_id),
            head: head.clone(),
            announced_at: Utc::now(),
        })?;
        Ok(())
    }

    /// Merges an already fetched control-plane snapshot into the local runtime
    /// view and republishes active records through the runtime command channel.
    pub fn ingest_control_plane_snapshot(
        &self,
        remote_snapshot: &ControlPlaneSnapshot,
    ) -> anyhow::Result<()> {
        for announcement in &remote_snapshot.control_announcements {
            let _ = self.control.publish_control(announcement.clone());
        }
        for announcement in &remote_snapshot.head_announcements {
            let _ = self.control.publish_head(announcement.clone());
        }
        for announcement in &remote_snapshot.lease_announcements {
            let _ = self.control.publish_lease(announcement.clone());
        }
        for announcement in &remote_snapshot.merge_announcements {
            let _ = self.control.publish_merge(announcement.clone());
        }
        for announcement in &remote_snapshot.merge_window_announcements {
            let _ = self.control.publish_merge_window(announcement.clone());
        }
        for announcement in &remote_snapshot.reducer_assignment_announcements {
            let _ = self
                .control
                .publish_reducer_assignment(announcement.clone());
        }
        for announcement in &remote_snapshot.update_announcements {
            let _ = self.control.publish_update(announcement.clone());
        }
        for announcement in &remote_snapshot.trainer_promotion_attestation_announcements {
            let _ = self
                .control
                .publish_trainer_promotion_attestation(announcement.clone());
        }
        for announcement in &remote_snapshot.diffusion_promotion_certificate_announcements {
            let _ = self
                .control
                .publish_diffusion_promotion_certificate(announcement.clone());
        }
        for announcement in &remote_snapshot.aggregate_proposal_announcements {
            let _ = self
                .control
                .publish_aggregate_proposal(announcement.clone());
        }
        for announcement in &remote_snapshot.reduction_certificate_announcements {
            let _ = self
                .control
                .publish_reduction_certificate(announcement.clone());
        }
        for announcement in &remote_snapshot.validation_quorum_announcements {
            let _ = self.control.publish_validation_quorum(announcement.clone());
        }
        for announcement in &remote_snapshot.reducer_load_announcements {
            let _ = self.control.publish_reducer_load(announcement.clone());
        }
        for announcement in &remote_snapshot.auth_announcements {
            let _ = self.control.publish_auth(announcement.clone());
        }
        for announcement in &remote_snapshot.directory_announcements {
            let _ = self.control.publish_directory(announcement.clone());
        }
        for announcement in &remote_snapshot.metrics_announcements {
            let _ = self.control.publish_metrics(announcement.clone());
        }

        let mut telemetry_snapshot = lock_telemetry_state(&self.telemetry.state);
        merge_control_plane_snapshot(&mut telemetry_snapshot.control_plane, remote_snapshot);
        if let Some(storage) = self.config().storage.as_ref() {
            persist_control_plane_state(storage, &telemetry_snapshot.control_plane)?;
        }
        telemetry_snapshot.updated_at = Utc::now();
        Ok(())
    }

    /// Fetches a peer snapshot on demand and merges it into the local control-plane
    /// view. This is useful when the caller already knows a peer should have
    /// relevant experiment state and wants to avoid waiting for passive gossip.
    pub fn ingest_peer_snapshot(
        &self,
        peer_id: &PeerId,
        timeout: Duration,
    ) -> anyhow::Result<ControlPlaneSnapshot> {
        const SNAPSHOT_INGEST_POLL_INTERVAL: Duration = Duration::from_millis(50);
        let deadline = Instant::now() + timeout;
        let mut last_error = None;

        loop {
            let attempt_timeout = deadline.saturating_duration_since(Instant::now());
            if attempt_timeout.is_zero() {
                break;
            }

            match self.control.fetch_snapshot(
                peer_id.as_str(),
                attempt_timeout.min(Duration::from_secs(1)),
            ) {
                Ok(remote_snapshot) => {
                    self.ingest_control_plane_snapshot(&remote_snapshot)?;
                    return Ok(remote_snapshot);
                }
                Err(error) => {
                    last_error = Some(error);
                    let telemetry_snapshot = self.telemetry().snapshot();
                    let known_addresses = telemetry_snapshot
                        .control_plane
                        .peer_directory_announcements
                        .iter()
                        .filter(|announcement| announcement.peer_id == *peer_id)
                        .flat_map(|announcement| announcement.addresses.iter().cloned())
                        .filter(|address| !telemetry_snapshot.listen_addresses.contains(address))
                        .collect::<BTreeSet<_>>();
                    for address in known_addresses {
                        let _ = self.control.dial_address(address);
                    }
                    let _ = self.control.request_snapshot(peer_id.as_str());
                }
            }

            std::thread::sleep(SNAPSHOT_INGEST_POLL_INTERVAL);
        }

        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!(
                "timed out ingesting snapshot from peer {}",
                peer_id.as_str()
            )
        }))
    }

    /// Republishes the locally visible training control-plane announcements for one
    /// completed training window. This is useful when a live peer has already
    /// materialized a window locally and needs to nudge merge-window/update/head
    /// propagation across the mesh without recomputing the window.
    pub fn republish_training_window_control_plane(
        &self,
        experiment: &ExperimentHandle,
        window_id: WindowId,
        base_head_id: &HeadId,
        artifact_id: &ArtifactId,
    ) -> anyhow::Result<()> {
        self.ensure_experiment_topics(experiment)?;
        let overlay = experiment.overlay_set()?.heads;
        let snapshot = self.telemetry().snapshot().control_plane;

        if let Some(announcement) = snapshot
            .merge_window_announcements
            .iter()
            .find(|announcement| {
                announcement.overlay == overlay
                    && announcement.merge_window.study_id == experiment.study_id
                    && announcement.merge_window.experiment_id == experiment.experiment_id
                    && announcement.merge_window.revision_id == experiment.revision_id
                    && announcement.merge_window.window_id == window_id
                    && announcement.merge_window.base_head_id == *base_head_id
            })
            .cloned()
        {
            self.control.publish_merge_window(announcement)?;
        }

        if let Some(announcement) = snapshot
            .update_announcements
            .iter()
            .find(|announcement| {
                announcement.overlay == overlay
                    && announcement.update.study_id == experiment.study_id
                    && announcement.update.experiment_id == experiment.experiment_id
                    && announcement.update.revision_id == experiment.revision_id
                    && announcement.update.window_id == window_id
                    && announcement.update.base_head_id == *base_head_id
                    && announcement.update.delta_artifact_id == *artifact_id
            })
            .cloned()
        {
            self.control.publish_update(announcement)?;
        }

        if let Some(announcement) = snapshot
            .head_announcements
            .iter()
            .find(|announcement| {
                announcement.overlay == overlay
                    && announcement.head.study_id == experiment.study_id
                    && announcement.head.experiment_id == experiment.experiment_id
                    && announcement.head.revision_id == experiment.revision_id
                    && announcement.head.artifact_id == *artifact_id
            })
            .cloned()
        {
            self.control.publish_head(announcement)?;
        }

        Ok(())
    }

    /// Seeds locally known control-plane state for a completed training window
    /// outcome that originated on another peer. This is useful for reducers or
    /// validators that already fetched the candidate artifact and want to
    /// prewarm the corresponding merge-window, update, and head records before a
    /// validation pass.
    pub fn seed_training_candidate<T>(
        &self,
        experiment: &ExperimentHandle,
        outcome: &TrainingWindowOutcome<T>,
    ) -> anyhow::Result<()> {
        self.ensure_experiment_topics(experiment)?;
        self.publish_artifact_from_store(&outcome.artifact.artifact_id)?;
        let telemetry_snapshot = self.telemetry().snapshot();
        let local_peer_id = telemetry_snapshot
            .local_peer_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("runtime does not have a local peer id yet"))?;
        let topology_policy = runtime_merge_topology_policy(
            self.config(),
            &telemetry_snapshot,
            experiment,
            Some(&outcome.contribution.base_head_id),
        );
        let robustness_policy =
            runtime_robustness_policy(self.config(), &telemetry_snapshot, experiment);
        let topology_peers =
            runtime_topology_peers(&telemetry_snapshot, &self.mainnet().roles, &local_peer_id);
        let reducer_peers = runtime_window_reducers(
            &outcome.contribution.base_head_id,
            outcome.lease.window_id,
            &topology_policy,
            &topology_peers,
        );
        let validator_peers =
            runtime_validator_peers(&telemetry_snapshot, &self.mainnet().roles, &local_peer_id);
        let validators = if matches!(
            topology_policy.promotion_policy.mode,
            HeadPromotionMode::ReducerAuthority | HeadPromotionMode::DiffusionSteadyState
        ) {
            Vec::new()
        } else {
            runtime_validators(
                &self.mainnet().roles,
                &local_peer_id,
                &validator_peers,
                topology_policy.promotion_policy.validator_quorum,
            )
        };
        let merge_window = latest_merge_window_from_snapshot(
            &telemetry_snapshot.control_plane,
            experiment,
            Some(&outcome.contribution.base_head_id),
        )
        .filter(|merge_window| merge_window.window_id == outcome.lease.window_id)
        .unwrap_or(open_runtime_merge_window(
            experiment,
            outcome.lease.window_id,
            outcome.contribution.base_head_id.clone(),
            topology_policy,
            reducer_peers,
            validators,
        )?);
        self.control.publish_merge_window(MergeWindowAnnouncement {
            overlay: experiment.overlay_set()?.heads.clone(),
            merge_window,
            announced_at: Utc::now(),
        })?;
        self.control.publish_update(UpdateEnvelopeAnnouncement {
            overlay: experiment.overlay_set()?.heads.clone(),
            update: UpdateAnnounce {
                peer_id: outcome.contribution.peer_id.clone(),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                window_id: outcome.lease.window_id,
                base_head_id: outcome.contribution.base_head_id.clone(),
                lease_id: Some(outcome.lease.lease_id.clone()),
                delta_artifact_id: outcome.artifact.artifact_id.clone(),
                sample_weight: outcome.contribution.accepted_weight,
                quality_weight: (1.0 / (1.0 + metric_quality(&outcome.contribution.metrics).abs()))
                    .max(0.01),
                norm_stats: update_norm_stats(&outcome.contribution.metrics),
                feature_sketch: Some(update_feature_sketch_from_metrics(
                    &outcome.contribution.metrics,
                    Some(&outcome.head.metrics),
                    robustness_policy.screening_policy.sketch_dimensionality as usize,
                    0,
                    0,
                    None,
                )),
                receipt_root: ContentId::derive(&[outcome.contribution.receipt_id.as_str()])?,
                receipt_ids: vec![outcome.contribution.receipt_id.clone()],
                providers: vec![outcome.contribution.peer_id.clone()],
                announced_at: Utc::now(),
            },
            workload_update: None,
        })?;
        self.control.publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set()?.heads,
            provider_peer_id: Some(outcome.contribution.peer_id.clone()),
            head: outcome.head.clone(),
            announced_at: Utc::now(),
        })?;
        Ok(())
    }

    /// Performs the restore experiment head operation.
    pub fn restore_experiment_head(
        &self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<Option<HeadDescriptor>>
    where
        P: P2pWorkload,
    {
        let assignment = SlotAssignmentState::from_experiment(experiment);
        self.persist_primary_assignment(&assignment)?;
        self.update_runtime_state(
            NodeRuntimeState::HeadSync,
            Some(SlotRuntimeState::MaterializingBase(assignment)),
        );
        self.ensure_experiment_topics(experiment)?;
        let Some(storage) = self.config().storage.as_ref().cloned() else {
            return Ok(None);
        };
        let Some(head) = load_head_state(&storage, experiment)? else {
            return Ok(None);
        };

        let store = FsArtifactStore::new(storage.root.clone());
        if self
            .head_artifact_schema_mismatch(&store, experiment, &head)?
            .is_some()
        {
            return Ok(None);
        }
        self.publish_artifact_from_store(&head.artifact_id)?;
        let local_peer_id = self
            .telemetry()
            .snapshot()
            .local_peer_id
            .ok_or_else(|| anyhow::anyhow!("runtime does not have a local peer id yet"))?;
        if let Some(merge_certificate) = load_latest_merge_certificate(&storage, experiment)?
            && merge_certificate.merged_head_id == head.head_id
            && merge_certificate.merged_artifact_id == head.artifact_id
        {
            self.control.publish_merge(MergeAnnouncement {
                overlay: experiment.overlay_set()?.heads,
                certificate: merge_certificate,
                announced_at: Utc::now(),
            })?;
        }
        self.control.publish_head(HeadAnnouncement {
            overlay: experiment.overlay_set()?.heads,
            provider_peer_id: Some(local_peer_id),
            head: head.clone(),
            announced_at: Utc::now(),
        })?;
        self.set_experiment_idle_state(experiment, NodeRuntimeState::IdleReady);
        Ok(Some(head))
    }

    fn ensure_head_artifact_matches_workload(
        &self,
        store: &FsArtifactStore,
        experiment: &ExperimentHandle,
        head: &HeadDescriptor,
    ) -> anyhow::Result<()>
    where
        P: P2pWorkload,
    {
        let descriptor = store.load_manifest(&head.artifact_id)?;
        anyhow::ensure!(
            descriptor.kind == ArtifactKind::FullHead,
            "head artifact {} for head {} must be a full training head, found {:?}",
            descriptor.artifact_id.as_str(),
            head.head_id.as_str(),
            descriptor.kind,
        );
        anyhow::ensure!(
            descriptor.head_id.as_ref() == Some(&head.head_id)
                && descriptor.artifact_id == head.artifact_id,
            "head artifact {} is not bound to announced head {}",
            descriptor.artifact_id.as_str(),
            head.head_id.as_str(),
        );
        let expected = self.expected_model_schema_hash_for_experiment(experiment)?;
        if descriptor.model_schema_hash != expected {
            anyhow::bail!(
                "head artifact {} for head {} has model schema {}, but workload expects {}; ignoring incompatible head",
                head.artifact_id.as_str(),
                head.head_id.as_str(),
                descriptor.model_schema_hash.as_str(),
                expected.as_str()
            );
        }
        let revision_contract = self
            .node
            .as_ref()
            .and_then(|node| node.revision_contracts.get(&experiment.revision_id));
        if let Some(contract) = revision_contract {
            let genesis = &contract.genesis.payload.payload;
            if genesis.artifact.head_id.as_ref() == Some(&head.head_id) {
                anyhow::ensure!(
                    descriptor == genesis.artifact,
                    "head {} claims the signed genesis identity but its artifact descriptor differs",
                    head.head_id.as_str(),
                );
            }
        }
        Ok(())
    }

    fn head_artifact_schema_mismatch(
        &self,
        store: &FsArtifactStore,
        experiment: &ExperimentHandle,
        head: &HeadDescriptor,
    ) -> anyhow::Result<Option<(ContentId, ContentId)>>
    where
        P: P2pWorkload,
    {
        let descriptor = store.load_manifest(&head.artifact_id)?;
        let expected = self.expected_model_schema_hash_for_experiment(experiment)?;
        if descriptor.model_schema_hash != expected {
            return Ok(Some((descriptor.model_schema_hash, expected)));
        }
        Ok(None)
    }

    fn expected_model_schema_hash_for_experiment(
        &self,
        experiment: &ExperimentHandle,
    ) -> anyhow::Result<ContentId>
    where
        P: P2pWorkload,
    {
        let current = self.current_workload_model_schema_hash()?;
        let selected_workload_id = self.config().selected_workload_id.as_ref();
        if let Some(entry) = self.list_experiments().into_iter().rev().find(|entry| {
            entry.study_id == experiment.study_id
                && entry.experiment_id == experiment.experiment_id
                && entry.current_revision_id == experiment.revision_id
        }) && selected_workload_id.is_some_and(|workload_id| workload_id != &entry.workload_id)
        {
            return Ok(entry.model_schema_hash);
        }
        Ok(current)
    }

    fn current_workload_model_schema_hash(&self) -> anyhow::Result<ContentId>
    where
        P: P2pWorkload,
    {
        Ok(self
            .node
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("running node is missing its prepared workload"))?
            .project
            .model_schema_hash())
    }
}

fn ensure_genesis_tensor_digest(actual: &ContentId, expected: &ContentId) -> anyhow::Result<()> {
    anyhow::ensure!(
        actual == expected,
        "decoded genesis tensor digest {} does not match authority-signed digest {}",
        actual.as_str(),
        expected.as_str(),
    );
    Ok(())
}

fn unsigned_genesis_head_id(
    experiment: &ExperimentHandle,
    model_schema_hash: &ContentId,
) -> HeadId {
    HeadId::new(format!(
        "{}-{}-{}-genesis",
        experiment.experiment_id.as_str(),
        experiment.revision_id.as_str(),
        model_schema_hash.as_str(),
    ))
}

fn resolve_sync_target_head(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    local_directory_current_head_ids: &BTreeSet<HeadId>,
    expected_directory_entry: Option<&ExperimentDirectoryEntry>,
    target_mode: HeadSyncTargetMode,
) -> anyhow::Result<Option<(PeerId, HeadDescriptor)>> {
    let mut directory_current_head_ids = local_directory_current_head_ids.clone();
    if directory_current_head_ids.is_empty() {
        directory_current_head_ids.extend(directory_current_head_ids_from_snapshots(
            snapshots,
            experiment,
            expected_directory_entry,
        ));
    }

    let directory_current = if directory_current_head_ids.is_empty() {
        None
    } else {
        resolve_directory_current_head(storage, experiment, snapshots, &directory_current_head_ids)?
    };

    match target_mode {
        HeadSyncTargetMode::DirectoryCurrent if !directory_current_head_ids.is_empty() => {
            Ok(directory_current)
        }
        HeadSyncTargetMode::DirectoryCurrent => {
            resolve_canonical_head(storage, experiment, snapshots)
        }
        HeadSyncTargetMode::LatestPromoted => {
            let canonical = resolve_canonical_head(storage, experiment, snapshots)?;
            Ok(newer_head_candidate(directory_current, canonical))
        }
    }
}

fn resolve_directory_current_head(
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    snapshots: &[(PeerId, ControlPlaneSnapshot)],
    directory_current_head_ids: &BTreeSet<HeadId>,
) -> anyhow::Result<Option<(PeerId, HeadDescriptor)>> {
    let best_remote =
        best_head_by_ids_from_snapshots(snapshots, experiment, directory_current_head_ids);
    if best_remote.is_some() {
        return Ok(best_remote);
    }
    if let Some(head) = load_head_state(storage, experiment)?
        && directory_current_head_ids.contains(&head.head_id)
    {
        return Ok(Some((PeerId::new("local"), head)));
    }
    Ok(None)
}

fn newer_head_candidate(
    left: Option<(PeerId, HeadDescriptor)>,
    right: Option<(PeerId, HeadDescriptor)>,
) -> Option<(PeerId, HeadDescriptor)> {
    match (left, right) {
        (Some(left), Some(right)) if head_is_newer(&right.1, &left.1) => Some(right),
        (Some(left), Some(_)) => Some(left),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

pub(crate) fn prioritize_connected_provider_peers(
    provider_peer_ids: Vec<PeerId>,
    connected_peer_ids: &BTreeSet<PeerId>,
) -> Vec<PeerId> {
    if connected_peer_ids.is_empty() {
        return provider_peer_ids;
    }

    let connected_first = provider_peer_ids
        .iter()
        .filter(|peer_id| connected_peer_ids.contains(*peer_id))
        .cloned()
        .collect::<Vec<_>>();
    dedupe_peer_ids(connected_first.into_iter().chain(provider_peer_ids))
}

fn head_is_newer(candidate: &HeadDescriptor, current: &HeadDescriptor) -> bool {
    candidate.global_step > current.global_step
        || (candidate.global_step == current.global_step
            && candidate.created_at > current.created_at)
}

fn head_sync_wait_timeout() -> Duration {
    ci_scaled_timeout(Duration::from_secs(300), Duration::from_secs(600))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn experiment() -> ExperimentHandle {
        ExperimentHandle {
            network_id: NetworkId::new("net-test"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
        }
    }

    fn directory_entry(
        experiment: &ExperimentHandle,
        current_head_id: HeadId,
    ) -> ExperimentDirectoryEntry {
        ExperimentDirectoryEntry {
            network_id: experiment.network_id.clone(),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            workload_id: WorkloadId::new("workload"),
            display_name: "runtime test".into(),
            model_schema_hash: ContentId::new("schema"),
            dataset_view_id: DatasetViewId::new("view"),
            resource_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::new(),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: None,
                estimated_download_bytes: 1024,
                estimated_window_seconds: 30,
            },
            visibility: ExperimentVisibility::Public,
            opt_in_policy: ExperimentOptInPolicy::Open,
            current_revision_id: experiment.revision_id.clone(),
            current_head_id: Some(current_head_id),
            allowed_roles: PeerRoleSet::default_trainer(),
            allowed_scopes: BTreeSet::new(),
            training_protocol: TrainingProtocol::default(),
            metadata: BTreeMap::new(),
        }
    }

    fn head(
        experiment: &ExperimentHandle,
        head_id: &str,
        artifact_id: &str,
        global_step: u64,
    ) -> HeadDescriptor {
        HeadDescriptor {
            head_id: HeadId::new(head_id),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: ArtifactId::new(artifact_id),
            parent_head_id: None,
            global_step,
            created_at: Utc::now() + chrono::Duration::milliseconds(global_step as i64),
            metrics: BTreeMap::new(),
        }
    }

    #[test]
    fn latest_promoted_sync_can_advance_past_stale_directory_current_head() {
        let experiment = experiment();
        let overlay = experiment.overlay_set().expect("overlay").heads;
        let stale = head(&experiment, "head-window-2", "artifact-window-2", 2);
        let promoted = head(&experiment, "head-window-3", "artifact-window-3", 3);
        let validator = PeerId::new("validator");
        let trainer = PeerId::new("trainer");
        let now = Utc::now();
        let mut snapshot = ControlPlaneSnapshot::default();

        snapshot
            .directory_announcements
            .push(ExperimentDirectoryAnnouncement {
                network_id: experiment.network_id.clone(),
                entries: vec![directory_entry(&experiment, stale.head_id.clone())],
                announced_at: now,
            });
        for (provider, announced_head) in [
            (PeerId::new("stale-provider"), stale.clone()),
            (trainer.clone(), promoted.clone()),
        ] {
            snapshot.head_announcements.push(HeadAnnouncement {
                overlay: overlay.clone(),
                provider_peer_id: Some(provider),
                head: announced_head,
                announced_at: now,
            });
        }
        snapshot.merge_announcements.push(MergeAnnouncement {
            overlay,
            certificate: MergeCertificate {
                merge_cert_id: MergeCertId::new("merge-window-3"),
                study_id: experiment.study_id.clone(),
                experiment_id: experiment.experiment_id.clone(),
                revision_id: experiment.revision_id.clone(),
                base_head_id: stale.head_id.clone(),
                merged_head_id: promoted.head_id.clone(),
                merged_artifact_id: promoted.artifact_id.clone(),
                policy: MergePolicy::WeightedMean,
                issued_at: now,
                promoter_peer_id: validator.clone(),
                promotion_mode: HeadPromotionMode::ValidatorQuorum,
                contribution_receipts: vec![ContributionReceiptId::new("receipt-window-3")],
            },
            announced_at: now,
        });

        let storage_root = tempdir().expect("storage tempdir");
        let snapshots = [(validator, snapshot)];
        let directory_current_head_ids = BTreeSet::new();
        let directory_current = resolve_sync_target_head(
            &StorageConfig::new(storage_root.path()),
            &experiment,
            &snapshots,
            &directory_current_head_ids,
            None,
            HeadSyncTargetMode::DirectoryCurrent,
        )
        .expect("resolve directory current")
        .expect("directory current head");
        let latest_promoted = resolve_sync_target_head(
            &StorageConfig::new(storage_root.path()),
            &experiment,
            &snapshots,
            &directory_current_head_ids,
            None,
            HeadSyncTargetMode::LatestPromoted,
        )
        .expect("resolve latest promoted")
        .expect("latest promoted head");

        assert_eq!(directory_current.1.head_id, stale.head_id);
        assert_eq!(latest_promoted.0, trainer);
        assert_eq!(latest_promoted.1.head_id, promoted.head_id);
    }

    #[test]
    fn head_sync_wait_timeout_covers_large_production_checkpoints() {
        let timeout = head_sync_wait_timeout();
        if std::env::var_os("CI").is_some() || std::env::var_os("GITHUB_ACTIONS").is_some() {
            assert_eq!(timeout, Duration::from_secs(600));
        } else {
            assert_eq!(timeout, Duration::from_secs(300));
        }
    }

    #[test]
    fn signed_genesis_tensor_digest_mismatch_fails_closed() {
        let error =
            ensure_genesis_tensor_digest(&ContentId::new("decoded"), &ContentId::new("authority"))
                .expect_err("mismatched tensors must fail");

        assert!(
            error
                .to_string()
                .contains("does not match authority-signed")
        );
    }

    #[test]
    fn unsigned_genesis_identity_binds_model_schema() {
        let experiment = experiment();
        let schema_a = unsigned_genesis_head_id(&experiment, &ContentId::new("schema-a"));
        let schema_a_again = unsigned_genesis_head_id(&experiment, &ContentId::new("schema-a"));
        let schema_b = unsigned_genesis_head_id(&experiment, &ContentId::new("schema-b"));

        assert_eq!(schema_a, schema_a_again);
        assert_ne!(schema_a, schema_b);
    }
}
