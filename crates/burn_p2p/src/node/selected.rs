use super::*;
use crate::runtime_support::{load_json, load_slot_assignments, persist_slot_assignments};
use burn_p2p_core::ControlCertId;

impl<P> RunningNode<SelectedWorkloadProject<P>>
where
    P: P2pProjectFamily,
{
    fn current_slot_assignments(&self) -> Vec<SlotAssignmentState> {
        let assignments = self
            .telemetry()
            .snapshot()
            .slot_states
            .iter()
            .filter_map(slot_assignment_from_state)
            .collect::<Vec<_>>();
        if !assignments.is_empty() {
            return assignments;
        }

        self.config()
            .storage
            .as_ref()
            .and_then(|storage| load_slot_assignments(storage).ok())
            .unwrap_or_default()
    }

    fn current_primary_assignment(&self) -> Option<SlotAssignmentState> {
        self.current_slot_assignments().into_iter().next()
    }

    fn has_applied_control_cert(&self, control_cert_id: &ControlCertId) -> bool {
        self.telemetry()
            .snapshot()
            .has_applied_control_cert(control_cert_id)
    }

    fn mark_control_cert_applied(&self, control_cert_id: ControlCertId) {
        let mut snapshot = lock_telemetry_state(&self.telemetry.state);
        snapshot.mark_control_cert_applied(control_cert_id);
    }

    fn effective_control_entry_for_experiment(
        &self,
        study_id: &StudyId,
        experiment_id: &ExperimentId,
        activation_window: WindowId,
    ) -> anyhow::Result<ExperimentDirectoryEntry> {
        let snapshot = self.telemetry().snapshot();
        if let Some(plan) = effective_experiment_lifecycle_plan(
            &snapshot.control_plane,
            self.mainnet().network_id(),
            study_id,
            experiment_id,
            activation_window,
        ) {
            return Ok(plan.target_entry);
        }

        self.visible_current_experiment_entry(study_id, experiment_id)
    }

    fn effective_control_entry_for_assignment(
        &self,
        assignment: &SlotAssignmentState,
        activation_window: WindowId,
    ) -> anyhow::Result<ExperimentDirectoryEntry> {
        let snapshot = self.telemetry().snapshot();
        if let Some(plan) = effective_experiment_lifecycle_plan(
            &snapshot.control_plane,
            self.mainnet().network_id(),
            &assignment.study_id,
            &assignment.experiment_id,
            activation_window,
        )
        .filter(|plan| plan.target_entry.current_revision_id == assignment.revision_id)
        {
            return Ok(plan.target_entry);
        }

        self.visible_experiment_entry(
            &assignment.study_id,
            &assignment.experiment_id,
            &assignment.revision_id,
        )
    }

    fn apply_patch_command(
        &mut self,
        study_id: &StudyId,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        patch: &RuntimePatch,
    ) -> anyhow::Result<bool> {
        let Some(assignment) = self.current_primary_assignment() else {
            return Ok(false);
        };
        if assignment.study_id != *study_id
            || assignment.experiment_id != *experiment_id
            || assignment.revision_id != *revision_id
        {
            return Ok(false);
        }

        let outcome = {
            let node = self
                .node
                .as_mut()
                .expect("running node should retain prepared node");
            node.project.apply_patch(patch)
        };

        match outcome {
            PatchOutcome::Applied => Ok(true),
            PatchOutcome::RequiresRestart => anyhow::bail!(
                "runtime patch for {} requires restart",
                experiment_id.as_str()
            ),
            PatchOutcome::Rejected(reason) => anyhow::bail!(
                "runtime patch for {} was rejected: {}",
                experiment_id.as_str(),
                reason
            ),
        }
    }

    fn resolve_control_target_head(
        &self,
        experiment: &ExperimentHandle,
        head_id: &HeadId,
    ) -> anyhow::Result<Option<(HeadDescriptor, Vec<PeerId>)>> {
        let snapshot = self.telemetry().snapshot();
        let matching_announcements = snapshot
            .control_plane
            .head_announcements
            .iter()
            .filter(|announcement| {
                announcement.head.head_id == *head_id
                    && matches_experiment_head(&announcement.head, experiment)
            })
            .collect::<Vec<_>>();

        let provider_peer_ids = dedupe_peer_ids(
            matching_announcements
                .iter()
                .filter_map(|announcement| announcement.provider_peer_id.clone()),
        );
        if let Some(announcement) = matching_announcements.into_iter().max_by(|left, right| {
            left.head
                .created_at
                .cmp(&right.head.created_at)
                .then(left.announced_at.cmp(&right.announced_at))
        }) {
            return Ok(Some((announcement.head.clone(), provider_peer_ids)));
        }

        let Some(storage) = self.config().storage.as_ref() else {
            return Ok(None);
        };
        Ok(
            load_json::<HeadDescriptor>(storage.scoped_head_path(head_id))?
                .map(|head| (head, provider_peer_ids)),
        )
    }

    fn apply_head_target_command(
        &self,
        study_id: &StudyId,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        head_id: &HeadId,
    ) -> anyhow::Result<bool> {
        let Some(assignment) = self.current_primary_assignment() else {
            return Ok(false);
        };
        if assignment.study_id != *study_id
            || assignment.experiment_id != *experiment_id
            || assignment.revision_id != *revision_id
        {
            return Ok(false);
        }

        let experiment = self.experiment(
            assignment.study_id,
            assignment.experiment_id,
            assignment.revision_id,
        );
        let Some(storage) = self.config().storage.as_ref().cloned() else {
            anyhow::bail!("head control command requires configured storage");
        };
        let Some((head, provider_peer_ids)) =
            self.resolve_control_target_head(&experiment, head_id)?
        else {
            anyhow::bail!(
                "head {} is not currently known for experiment {}",
                head_id.as_str(),
                experiment_id.as_str()
            );
        };

        let store = FsArtifactStore::new(storage.root.clone());
        if !store.has_manifest(&head.artifact_id) {
            anyhow::ensure!(
                !provider_peer_ids.is_empty(),
                "head {} is not materialized locally and has no known provider peers",
                head.head_id.as_str()
            );
            self.wait_for_artifact_from_peers(
                &provider_peer_ids,
                &head.artifact_id,
                ci_scaled_timeout(Duration::from_secs(5), Duration::from_secs(20)),
            )?;
        }
        anyhow::ensure!(
            self.adopt_known_head_if_present(&experiment, &head)?,
            "head {} could not be adopted after artifact sync",
            head.head_id.as_str()
        );
        self.publish_head_provider(&experiment, &head)?;
        Ok(true)
    }

    fn apply_reassign_cohort_command(
        &mut self,
        study_id: &StudyId,
        experiment_id: &ExperimentId,
        peer_ids: &[PeerId],
        activation_window: WindowId,
    ) -> anyhow::Result<bool> {
        let Some(local_peer_id) = self.telemetry().snapshot().local_peer_id else {
            return Ok(false);
        };
        if !peer_ids.iter().any(|peer_id| peer_id == &local_peer_id) {
            return Ok(false);
        }

        let entry = self.effective_control_entry_for_experiment(
            study_id,
            experiment_id,
            activation_window,
        )?;
        let target_assignment = SlotAssignmentState::new(
            entry.study_id.clone(),
            entry.experiment_id.clone(),
            entry.current_revision_id.clone(),
        );
        if self.current_primary_assignment().as_ref() == Some(&target_assignment)
            && self.config().selected_workload_id.as_ref() == Some(&entry.workload_id)
        {
            return Ok(true);
        }

        self.switch_experiment_entry(entry)?;
        Ok(true)
    }

    fn apply_schedule_epoch_for_window(
        &mut self,
        activation_window: WindowId,
    ) -> anyhow::Result<()> {
        let snapshot = self.telemetry().snapshot();
        let Some(local_peer_id) = snapshot.local_peer_id else {
            return Ok(());
        };
        let Some(epoch) = effective_fleet_schedule_epoch(
            &snapshot.control_plane,
            self.mainnet().network_id(),
            activation_window,
        ) else {
            return Ok(());
        };
        let assignments = epoch
            .assignments_for_peer(&local_peer_id)
            .into_iter()
            .map(|assignment| {
                SlotAssignmentState::new(
                    assignment.study_id.clone(),
                    assignment.experiment_id.clone(),
                    assignment.revision_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        if assignments.is_empty() {
            return Ok(());
        }

        if let Some(storage) = self.config().storage.as_ref() {
            persist_slot_assignments(storage, &assignments)?;
        }

        let primary_assignment = assignments
            .first()
            .cloned()
            .expect("non-empty schedule assignments should retain a primary slot");
        let selected_workload_id = self.config().selected_workload_id.clone();
        let entry =
            self.effective_control_entry_for_assignment(&primary_assignment, activation_window)?;
        let current_primary = self.current_primary_assignment();
        let switched = if current_primary.as_ref() != Some(&primary_assignment)
            || selected_workload_id.as_ref() != Some(&entry.workload_id)
        {
            self.switch_experiment_entry(entry)?;
            true
        } else {
            false
        };

        let mut snapshot = lock_telemetry_state(&self.telemetry.state);
        snapshot.set_slot_assignments(&assignments);
        if switched
            && let Some(storage) = self.config().storage.as_ref()
            && let Err(error) = persist_runtime_security_state(storage, &snapshot)
        {
            snapshot.last_error = Some(format!("failed to persist security state: {error}"));
        }
        Ok(())
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

    fn reconcile_directory_assignment(
        &mut self,
        activation_window: WindowId,
    ) -> anyhow::Result<Option<ExperimentHandle>> {
        let Some(assignment) = self.current_primary_assignment() else {
            return Ok(None);
        };
        let snapshot = self.telemetry().snapshot();
        let lifecycle_plan = effective_experiment_lifecycle_plan(
            &snapshot.control_plane,
            self.mainnet().network_id(),
            &assignment.study_id,
            &assignment.experiment_id,
            activation_window,
        )
        .filter(|plan| {
            plan.base_revision_id
                .as_ref()
                .map(|base_revision_id| base_revision_id == &assignment.revision_id)
                .unwrap_or(true)
                || plan.target_entry.current_revision_id == assignment.revision_id
        });
        let lifecycle_exists = experiment_has_lifecycle_plan(
            &snapshot.control_plane,
            self.mainnet().network_id(),
            &assignment.study_id,
            &assignment.experiment_id,
        );
        let selected_workload_id = self.config().selected_workload_id.clone();

        if let Some(plan) = lifecycle_plan {
            let entry = plan.target_entry;
            if entry.current_revision_id != assignment.revision_id
                || selected_workload_id.as_ref() != Some(&entry.workload_id)
            {
                return self.switch_experiment_entry(entry).map(Some);
            }
        } else if lifecycle_exists {
            return Ok(Some(self.experiment(
                assignment.study_id,
                assignment.experiment_id,
                assignment.revision_id,
            )));
        } else {
            let entry = self.visible_current_experiment_entry(
                &assignment.study_id,
                &assignment.experiment_id,
            )?;
            if entry.current_revision_id != assignment.revision_id
                || selected_workload_id.as_ref() != Some(&entry.workload_id)
            {
                return self.switch_experiment_entry(entry).map(Some);
            }
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
            let control_cert_id = announcement.certificate.control_cert_id.clone();
            if self.has_applied_control_cert(&control_cert_id) {
                continue;
            }

            let applied = match &announcement.certificate.body.payload.payload.command {
                ExperimentControlCommand::PatchExperiment {
                    study_id,
                    experiment_id,
                    revision_id,
                    patch,
                    ..
                } => self.apply_patch_command(study_id, experiment_id, revision_id, patch)?,
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
                        true
                    } else {
                        false
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
                        true
                    } else {
                        false
                    }
                }
                ExperimentControlCommand::PromoteHead {
                    study_id,
                    experiment_id,
                    revision_id,
                    head_id,
                    ..
                }
                | ExperimentControlCommand::RollbackHead {
                    study_id,
                    experiment_id,
                    revision_id,
                    head_id,
                    ..
                } => {
                    self.apply_head_target_command(study_id, experiment_id, revision_id, head_id)?
                }
                ExperimentControlCommand::ReassignCohort {
                    study_id,
                    experiment_id,
                    peer_ids,
                    ..
                } => self.apply_reassign_cohort_command(
                    study_id,
                    experiment_id,
                    peer_ids,
                    activation_window,
                )?,
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
                    true
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
                    true
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
                    true
                }
                _ => false,
            };

            if applied {
                self.mark_control_cert_applied(control_cert_id);
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
        let experiment = self.reconcile_directory_assignment(activation_window)?;
        self.apply_control_announcements_for_window(activation_window)?;
        self.apply_schedule_epoch_for_window(activation_window)?;
        Ok(self.active_experiment().or(experiment))
    }
}
