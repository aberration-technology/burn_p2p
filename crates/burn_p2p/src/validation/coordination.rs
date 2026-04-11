use super::*;

pub(super) fn validation_blocked_reason(lag_assessment: &LagAssessment) -> String {
    match lag_assessment.state {
        LagState::LeaseBlocked => format!(
            "validation blocked: local node is {} head steps behind canonical head",
            lag_assessment.head_lag_steps
        ),
        LagState::RebaseRequired => format!(
            "validation blocked: full rebase required after falling {} head steps behind",
            lag_assessment.head_lag_steps
        ),
        _ => unreachable!("non-blocking lag state matched blocking branch"),
    }
}

fn reduction_attestations_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    aggregate_id: &ContentId,
) -> BTreeMap<PeerId, ContentId> {
    snapshot
        .reduction_certificate_announcements
        .iter()
        .filter(|announcement| {
            announcement.overlay == *overlay
                && announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.aggregate_id == *aggregate_id
        })
        .map(|announcement| {
            (
                announcement.certificate.validator.clone(),
                announcement.certificate.reduction_id.clone(),
            )
        })
        .collect()
}

fn aggregate_proposal_announced_in_snapshot(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    aggregate_id: &ContentId,
) -> bool {
    snapshot
        .aggregate_proposal_announcements
        .iter()
        .any(|announcement| {
            announcement.overlay == *overlay
                && announcement.proposal.study_id == experiment.study_id
                && announcement.proposal.experiment_id == experiment.experiment_id
                && announcement.proposal.revision_id == experiment.revision_id
                && announcement.proposal.aggregate_id == *aggregate_id
        })
}

fn merge_announced_in_snapshot(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    merged_head_id: &HeadId,
) -> bool {
    snapshot.merge_announcements.iter().any(|announcement| {
        announcement.overlay == *overlay
            && announcement.certificate.study_id == experiment.study_id
            && announcement.certificate.experiment_id == experiment.experiment_id
            && announcement.certificate.revision_id == experiment.revision_id
            && announcement.certificate.merged_head_id == *merged_head_id
    })
}

fn validation_quorum_announced_in_snapshot(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    aggregate_id: &ContentId,
    merged_head_id: &HeadId,
) -> bool {
    snapshot
        .validation_quorum_announcements
        .iter()
        .any(|announcement| {
            announcement.overlay == *overlay
                && announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.aggregate_id == *aggregate_id
                && announcement.certificate.merged_head_id == *merged_head_id
        })
}

fn merge_certificate_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    merged_head_id: &HeadId,
) -> Option<MergeCertificate> {
    snapshot
        .merge_announcements
        .iter()
        .find_map(|announcement| {
            (announcement.overlay == *overlay
                && announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.merged_head_id == *merged_head_id)
                .then(|| announcement.certificate.clone())
        })
}

fn collect_validation_coordination_from_snapshots<'a>(
    snapshots: impl IntoIterator<Item = &'a ControlPlaneSnapshot>,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    aggregate_id: &ContentId,
    merged_head_id: &HeadId,
    local_attestation: Option<(PeerId, ContentId)>,
) -> ValidationCoordinationState {
    let mut attestations = BTreeMap::new();
    if let Some((peer_id, reduction_id)) = local_attestation {
        attestations.insert(peer_id, reduction_id);
    }
    let mut aggregate_proposal_announced = false;
    let mut quorum_announced = false;
    let mut merge_announced = false;
    let mut merge_certificate = None;
    for snapshot in snapshots {
        attestations.extend(reduction_attestations_from_snapshot(
            snapshot,
            overlay,
            experiment,
            aggregate_id,
        ));
        aggregate_proposal_announced |=
            aggregate_proposal_announced_in_snapshot(snapshot, overlay, experiment, aggregate_id);
        quorum_announced |= validation_quorum_announced_in_snapshot(
            snapshot,
            overlay,
            experiment,
            aggregate_id,
            merged_head_id,
        );
        merge_announced |=
            merge_announced_in_snapshot(snapshot, overlay, experiment, merged_head_id);
        if merge_certificate.is_none() {
            merge_certificate =
                merge_certificate_from_snapshot(snapshot, overlay, experiment, merged_head_id);
        }
    }
    ValidationCoordinationState {
        attesters: attestations.keys().cloned().collect(),
        reduction_ids: attestations.into_values().collect(),
        aggregate_proposal_announced,
        quorum_announced,
        merge_announced,
        merge_certificate,
    }
}

fn merge_promoted_validation_outcome(
    promoted: Option<ValidationOutcome>,
    outcome: Option<ValidationOutcome>,
) -> anyhow::Result<Option<ValidationOutcome>> {
    let Some(outcome) = outcome else {
        return Ok(promoted);
    };
    if let Some(existing) = &promoted {
        anyhow::ensure!(
            existing.merged_head.head_id == outcome.merged_head.head_id,
            "validators promoted different merged heads for the same aggregate proposal",
        );
        Ok(promoted)
    } else {
        Ok(Some(outcome))
    }
}

impl<P> RunningNode<P> {
    /// Observes validation coordination for one aggregate and merged head across the local and
    /// currently reachable experiment peers.
    pub fn observe_validation_coordination_for_head(
        &self,
        experiment: &ExperimentHandle,
        aggregate_id: &ContentId,
        merged_head_id: &HeadId,
    ) -> anyhow::Result<ValidationCoordinationState>
    where
        P: P2pWorkload,
    {
        let overlay = experiment.overlay_set()?.heads;
        let local_snapshot = self.telemetry().snapshot().control_plane;
        Ok(collect_validation_coordination_from_snapshots(
            std::iter::once(&local_snapshot),
            &overlay,
            experiment,
            aggregate_id,
            merged_head_id,
            None,
        ))
    }

    /// Drives validator execution until the local node has made visible progress or the
    /// aggregate settles.
    pub fn drive_validation_until_local_progress(
        &mut self,
        experiment: &ExperimentHandle,
        aggregate_id: &ContentId,
        merged_head_id: &HeadId,
        timeout: Duration,
    ) -> anyhow::Result<ValidationDriveOutcome>
    where
        P: P2pWorkload,
        P::Model: Send + 'static,
    {
        let local_peer_id = self
            .telemetry()
            .snapshot()
            .local_peer_id
            .ok_or_else(|| anyhow::anyhow!("validation driver missing local peer id"))?;
        let deadline = Instant::now() + timeout;
        let mut attempts = 0usize;
        let mut promoted = None;
        let mut coordination = self.observe_validation_coordination_for_head(
            experiment,
            aggregate_id,
            merged_head_id,
        )?;
        while Instant::now() < deadline {
            if promoted.is_some()
                || coordination.quorum_announced
                || coordination.merge_announced
                || coordination.attesters.contains(&local_peer_id)
            {
                break;
            }
            attempts += 1;
            let next = self.validate_candidates_once(experiment)?;
            promoted = merge_promoted_validation_outcome(promoted, next)?;
            coordination = self.observe_validation_coordination_for_head(
                experiment,
                aggregate_id,
                merged_head_id,
            )?;
            if promoted.is_none()
                && !coordination.quorum_announced
                && !coordination.merge_announced
                && !coordination.attesters.contains(&local_peer_id)
            {
                std::thread::sleep(VALIDATION_COORDINATION_POLL_INTERVAL);
            }
        }
        Ok(ValidationDriveOutcome {
            attempts,
            promoted,
            coordination,
        })
    }

    pub(super) fn observe_validation_coordination(
        &self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        execution: &ValidationExecution,
    ) -> anyhow::Result<ValidationCoordinationState> {
        let overlay = experiment.overlay_set()?.heads;
        let local_snapshot = self.telemetry().snapshot().control_plane;
        let remote_snapshots =
            self.fetch_experiment_snapshots(experiment, Duration::from_millis(250))?;
        Ok(collect_validation_coordination_from_snapshots(
            std::iter::once(&local_snapshot)
                .chain(remote_snapshots.iter().map(|(_, snapshot)| snapshot)),
            &overlay,
            experiment,
            &execution.aggregate.aggregate_id,
            &execution.merged_head.head_id,
            Some((
                prepared.local_peer_id.clone(),
                execution.reduction_certificate.reduction_id.clone(),
            )),
        ))
    }

    pub(super) fn wait_for_validation_coordination(
        &self,
        experiment: &ExperimentHandle,
        prepared: &ValidationPreparedState,
        execution: &ValidationExecution,
    ) -> anyhow::Result<ValidationCoordinationState> {
        let quorum = effective_validator_quorum(&prepared.merge_window);
        let deadline = Instant::now() + VALIDATION_QUORUM_WAIT;
        loop {
            let observed = self.observe_validation_coordination(experiment, prepared, execution)?;
            if observed.settled(quorum) || Instant::now() >= deadline {
                return Ok(observed);
            }
            std::thread::sleep(VALIDATION_COORDINATION_POLL_INTERVAL);
        }
    }
}
