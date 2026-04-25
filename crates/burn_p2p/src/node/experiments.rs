use super::*;
use crate::runtime_support::{
    head_provider_peers, load_slot_assignments, persist_slot_assignments, runtime_window_reducers,
};

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

        let mut directory = self
            .telemetry()
            .snapshot()
            .control_plane
            .directory_announcements
            .iter()
            .filter(|announcement| announcement.network_id == self.mainnet().genesis.network_id)
            .flat_map(|announcement| announcement.entries.clone())
            .collect::<Vec<_>>();

        if directory.is_empty()
            && let Some(auth) = &self.config().auth
        {
            directory = auth.experiment_directory.clone();
        }

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
        let project = &mut self
            .node
            .as_mut()
            .expect("running node should retain prepared node")
            .project;
        let device = project.runtime_device();
        let model = project.init_model(&device);
        let head_id = HeadId::new(format!(
            "{}-{}-genesis",
            experiment.experiment_id.as_str(),
            local_peer_id.as_str()
        ));
        let artifact = project.materialize_model_artifact(
            &model,
            ArtifactKind::FullHead,
            head_id.clone(),
            None,
            &store,
        )?;
        let evaluation = project.evaluate(&model, EvalSplit::Validation);
        let head = HeadDescriptor {
            head_id: head_id.clone(),
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            parent_head_id: None,
            global_step: 0,
            created_at: Utc::now(),
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
    ) -> anyhow::Result<Option<HeadDescriptor>> {
        const HEAD_SYNC_SNAPSHOT_TIMEOUT: Duration = Duration::from_millis(750);
        // Head adoption may require pulling a newly promoted artifact from the
        // network. Give larger runtime payloads enough room to materialize
        // before the caller falls back to another outer retry loop.
        let head_sync_wait_timeout =
            ci_scaled_timeout(Duration::from_secs(10), Duration::from_secs(30));
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
        let mut snapshots = cached_snapshots;
        let mut resolved_head =
            resolve_canonical_head(&storage, experiment, &cached_canonical_snapshots)?;

        if resolved_head.is_none() {
            snapshots = self.fetch_experiment_snapshots(experiment, HEAD_SYNC_SNAPSHOT_TIMEOUT)?;
            let canonical_snapshots = snapshots_with_local_control_plane(
                &snapshots,
                telemetry_snapshot.local_peer_id.as_ref(),
                &telemetry_snapshot.control_plane,
            );
            resolved_head = resolve_canonical_head(&storage, experiment, &canonical_snapshots)?;
        }
        let _ = self.assess_and_record_lag(&storage, experiment, &snapshots)?;
        let Some((source_peer_id, head)) = resolved_head else {
            let connected_peers = connected_peer_ids(&telemetry_snapshot);
            let provider_addresses = telemetry_snapshot
                .control_plane
                .peer_directory_announcements
                .iter()
                .filter(|announcement| !connected_peers.contains(&announcement.peer_id))
                .flat_map(|announcement| announcement.addresses.iter().cloned())
                .filter(|address| !telemetry_snapshot.listen_addresses.contains(address))
                .collect::<BTreeSet<_>>();
            for address in provider_addresses {
                let _ = self.control.dial_address(address);
            }
            return Ok(None);
        };
        let store = FsArtifactStore::new(storage.root.clone());
        if !store.has_complete_artifact(&head.artifact_id)? && head.global_step > 0 {
            let provider_peer_ids = head_provider_peers(
                Some(&source_peer_id),
                &snapshots,
                &telemetry_snapshot.control_plane,
                telemetry_snapshot.local_peer_id.as_ref(),
                experiment,
                &head,
            );
            let deadline = Instant::now() + head_sync_wait_timeout;
            loop {
                let result = if provider_peer_ids.is_empty() {
                    self.sync_artifact_from_peer_bounded(
                        &source_peer_id,
                        head.artifact_id.clone(),
                        head_sync_wait_timeout,
                    )
                    .map(|_| ())
                } else {
                    self.wait_for_artifact_from_peers(
                        &provider_peer_ids,
                        &head.artifact_id,
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
        persist_head_state(&storage, experiment, &head)?;
        persist_json(storage.scoped_head_path(&head.head_id), &head)?;
        store.pin_head(&head.head_id)?;
        store.pin_artifact(&head.artifact_id)?;
        if head.global_step > 0 {
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
    ) -> anyhow::Result<HeadDescriptor> {
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
    ) -> anyhow::Result<HeadDescriptor> {
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
        const ARTIFACT_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(100);
        let artifact_request_timeout =
            ci_scaled_timeout(Duration::from_secs(3), Duration::from_secs(10));

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

            for provider_peer_id in provider_peer_ids {
                let Some(request_timeout) = fair_request_timeout(
                    deadline,
                    artifact_request_timeout,
                    provider_peer_ids.len(),
                ) else {
                    break;
                };
                match self.sync_artifact_from_peer_bounded(
                    provider_peer_id,
                    artifact_id.clone(),
                    request_timeout,
                ) {
                    Ok(_) => return Ok(()),
                    Err(error) => {
                        last_error = Some(format!(
                            "could not fetch {} from {}: {error}",
                            artifact_id.as_str(),
                            provider_peer_id.as_str(),
                        ));
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
    ) -> anyhow::Result<bool> {
        let Some(storage) = self.config().storage.as_ref().cloned() else {
            return Ok(false);
        };
        self.ensure_experiment_topics(experiment)?;
        let store = FsArtifactStore::new(storage.root.clone());
        if !store.has_complete_artifact(&head.artifact_id)? {
            return Ok(false);
        }

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
                    for announcement in &remote_snapshot.trainer_promotion_attestation_announcements
                    {
                        let _ = self
                            .control
                            .publish_trainer_promotion_attestation(announcement.clone());
                    }
                    for announcement in
                        &remote_snapshot.diffusion_promotion_certificate_announcements
                    {
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
                    merge_control_plane_snapshot(
                        &mut telemetry_snapshot.control_plane,
                        &remote_snapshot,
                    );
                    if let Some(storage) = self.config().storage.as_ref() {
                        persist_control_plane_state(storage, &telemetry_snapshot.control_plane)?;
                    }
                    telemetry_snapshot.updated_at = Utc::now();
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
    ) -> anyhow::Result<Option<HeadDescriptor>> {
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
}
