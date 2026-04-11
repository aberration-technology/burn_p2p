use super::*;

impl<P> RunningNode<SelectedWorkloadProject<P>>
where
    P: P2pProjectFamily,
{
    fn current_primary_assignment(&self) -> Option<SlotAssignmentState> {
        self.telemetry()
            .snapshot()
            .slot_states
            .first()
            .and_then(slot_assignment_from_state)
            .or_else(|| {
                self.config()
                    .storage
                    .as_ref()
                    .and_then(|storage| load_primary_slot_assignment(storage).ok().flatten())
            })
    }

    fn visible_current_experiment_entry(
        &self,
        study_id: &StudyId,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<ExperimentDirectoryEntry> {
        self.list_experiments()
            .into_iter()
            .rev()
            .find(|entry| entry.study_id == *study_id && entry.experiment_id == *experiment_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "experiment {} is not visible to the current node scope",
                    experiment_id.as_str()
                )
            })
    }

    fn switch_experiment_entry(
        &mut self,
        entry: ExperimentDirectoryEntry,
    ) -> anyhow::Result<ExperimentHandle> {
        let experiment = self.experiment(
            entry.study_id.clone(),
            entry.experiment_id.clone(),
            entry.current_revision_id.clone(),
        );
        let assignment = SlotAssignmentState::from_experiment(&experiment);
        let idle_state = default_node_runtime_state(&self.mainnet().roles);

        self.update_runtime_state(
            NodeRuntimeState::DirectorySync,
            Some(SlotRuntimeState::Migrating(assignment.clone())),
        );

        let result = (|| -> anyhow::Result<()> {
            {
                let node = self
                    .node
                    .as_mut()
                    .expect("running node should retain prepared node");
                node.project.switch_workload(entry.workload_id.clone())?;
                node.config.selected_workload_id = Some(entry.workload_id.clone());
                if let Some(storage) = node.config.storage.as_ref() {
                    persist_runtime_binding_state(storage, node.config())?;
                }
            }
            self.ensure_experiment_topics(&experiment)?;
            self.persist_primary_assignment(&assignment)?;
            Ok(())
        })();

        if let Err(error) = result {
            self.update_runtime_state(
                idle_state,
                Some(SlotRuntimeState::Blocked {
                    assignment: Some(assignment),
                    reason: error.to_string(),
                }),
            );
            return Err(error);
        }

        self.update_runtime_state(idle_state, Some(SlotRuntimeState::Assigned(assignment)));
        Ok(experiment)
    }

    fn reconcile_directory_assignment(&mut self) -> anyhow::Result<Option<ExperimentHandle>> {
        let Some(assignment) = self.current_primary_assignment() else {
            return Ok(None);
        };
        let entry =
            self.visible_current_experiment_entry(&assignment.study_id, &assignment.experiment_id)?;
        let selected_workload_id = self.config().selected_workload_id.clone();

        if entry.current_revision_id != assignment.revision_id
            || selected_workload_id.as_ref() != Some(&entry.workload_id)
        {
            return self.switch_experiment_entry(entry).map(Some);
        }

        Ok(Some(self.experiment(
            assignment.study_id,
            assignment.experiment_id,
            assignment.revision_id,
        )))
    }

    fn apply_control_announcements_for_window(
        &mut self,
        activation_window: WindowId,
    ) -> anyhow::Result<()> {
        let local_peer_id = self.telemetry().snapshot().local_peer_id;
        let mut announcements = self
            .telemetry()
            .snapshot()
            .control_plane
            .control_announcements
            .into_iter()
            .filter(|announcement| {
                announcement.certificate.network_id == self.mainnet().genesis.network_id
                    && announcement.certificate.activation.activation_window <= activation_window
            })
            .collect::<Vec<_>>();
        announcements.sort_by(|left, right| {
            left.certificate
                .activation
                .activation_window
                .cmp(&right.certificate.activation.activation_window)
                .then(left.announced_at.cmp(&right.announced_at))
        });

        for announcement in announcements {
            match &announcement.certificate.body.payload.payload.command {
                ExperimentControlCommand::PauseExperiment {
                    study_id,
                    experiment_id,
                    reason,
                    ..
                } => {
                    let Some(assignment) = self.current_primary_assignment() else {
                        continue;
                    };
                    if assignment.study_id == *study_id
                        && assignment.experiment_id == *experiment_id
                    {
                        self.update_runtime_state(
                            default_node_runtime_state(&self.mainnet().roles),
                            Some(SlotRuntimeState::Blocked {
                                assignment: Some(assignment),
                                reason: reason.clone().unwrap_or_else(|| {
                                    format!("experiment {} paused", experiment_id.as_str())
                                }),
                            }),
                        );
                    }
                }
                ExperimentControlCommand::ResumeExperiment {
                    study_id,
                    experiment_id,
                    ..
                } => {
                    let Some(assignment) = self.current_primary_assignment() else {
                        continue;
                    };
                    if assignment.study_id == *study_id
                        && assignment.experiment_id == *experiment_id
                    {
                        self.update_runtime_state(
                            default_node_runtime_state(&self.mainnet().roles),
                            Some(SlotRuntimeState::Assigned(assignment)),
                        );
                    }
                }
                ExperimentControlCommand::RevokePeer {
                    peer_id,
                    minimum_revocation_epoch,
                    reason,
                } if local_peer_id.as_ref() == Some(peer_id) => {
                    self.update_runtime_state(
                        NodeRuntimeState::Revoked,
                        Some(SlotRuntimeState::Blocked {
                            assignment: self.current_primary_assignment(),
                            reason: format!(
                                "{} (minimum revocation epoch {})",
                                reason, minimum_revocation_epoch.0
                            ),
                        }),
                    );
                }
                ExperimentControlCommand::QuarantinePeer {
                    peer_id, reason, ..
                } if local_peer_id.as_ref() == Some(peer_id) => {
                    self.update_runtime_state(
                        NodeRuntimeState::Quarantined,
                        Some(SlotRuntimeState::Blocked {
                            assignment: self.current_primary_assignment(),
                            reason: reason.clone(),
                        }),
                    );
                }
                ExperimentControlCommand::PardonPeer { peer_id, .. }
                    if local_peer_id.as_ref() == Some(peer_id) =>
                {
                    let slot_state = self
                        .current_primary_assignment()
                        .map(SlotRuntimeState::Assigned)
                        .unwrap_or(SlotRuntimeState::Unassigned);
                    self.update_runtime_state(
                        default_node_runtime_state(&self.mainnet().roles),
                        Some(slot_state),
                    );
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Performs the switch experiment operation.
    pub fn switch_experiment(
        &mut self,
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
    ) -> anyhow::Result<ExperimentHandle> {
        let entry = self.visible_experiment_entry(&study_id, &experiment_id, &revision_id)?;
        self.switch_experiment_entry(entry)
    }

    /// Performs the restore selected experiment operation.
    pub fn restore_selected_experiment(&mut self) -> anyhow::Result<Option<ExperimentHandle>> {
        let Some(storage) = self.config().storage.as_ref().cloned() else {
            return Ok(None);
        };
        let Some(assignment) = load_primary_slot_assignment(&storage)? else {
            return Ok(None);
        };

        self.switch_experiment(
            assignment.study_id,
            assignment.experiment_id,
            assignment.revision_id,
        )
        .map(Some)
    }

    /// Performs the active experiment operation.
    pub fn active_experiment(&self) -> Option<ExperimentHandle> {
        let assignment = self.current_primary_assignment()?;
        Some(self.experiment(
            assignment.study_id,
            assignment.experiment_id,
            assignment.revision_id,
        ))
    }

    /// Performs the follow control plane once operation.
    pub fn follow_control_plane_once(
        &mut self,
        activation_window: WindowId,
    ) -> anyhow::Result<Option<ExperimentHandle>> {
        let experiment = self.reconcile_directory_assignment()?;
        self.apply_control_announcements_for_window(activation_window)?;
        Ok(experiment.or_else(|| self.active_experiment()))
    }
}
