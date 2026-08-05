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

fn promotion_peer_allowed(window: &MergeWindowState, peer_id: &PeerId) -> bool {
    match head_promotion_mode(window) {
        HeadPromotionMode::ValidatorQuorum => window.validators.contains(peer_id),
        HeadPromotionMode::ReducerAuthority => window.reducers.contains(peer_id),
        HeadPromotionMode::DiffusionSteadyState => false,
    }
}

#[derive(Clone)]
struct ReductionAttestationEvidence {
    reduction_id: ContentId,
    evaluation: HeadEvaluationBinding,
}

#[derive(Clone, Copy)]
pub(super) struct ValidationEvidenceScope<'a> {
    overlay: &'a OverlayTopic,
    experiment: &'a ExperimentHandle,
    aggregate_id: &'a ContentId,
    merged_head_id: &'a HeadId,
    merged_artifact_id: Option<&'a ArtifactId>,
    eval_protocol_id: Option<&'a ContentId>,
    merge_window: Option<&'a MergeWindowState>,
}

impl<'a> ValidationEvidenceScope<'a> {
    pub(super) fn new(
        overlay: &'a OverlayTopic,
        experiment: &'a ExperimentHandle,
        aggregate_id: &'a ContentId,
        merged_head_id: &'a HeadId,
    ) -> Self {
        Self {
            overlay,
            experiment,
            aggregate_id,
            merged_head_id,
            merged_artifact_id: None,
            eval_protocol_id: None,
            merge_window: None,
        }
    }

    pub(super) fn with_evaluation(
        mut self,
        merged_artifact_id: &'a ArtifactId,
        eval_protocol_id: &'a ContentId,
    ) -> Self {
        self.merged_artifact_id = Some(merged_artifact_id);
        self.eval_protocol_id = Some(eval_protocol_id);
        self
    }

    pub(super) fn with_merge_window(mut self, merge_window: &'a MergeWindowState) -> Self {
        self.merge_window = Some(merge_window);
        self
    }
}

fn reduction_evaluation_matches(
    certificate: &ReductionCertificate,
    scope: ValidationEvidenceScope<'_>,
) -> bool {
    certificate.validate_structure().is_ok()
        && certificate.evaluation.as_ref().is_some_and(|evaluation| {
            evaluation.head_id == *scope.merged_head_id
                && scope
                    .merged_artifact_id
                    .is_none_or(|artifact_id| evaluation.artifact_id == *artifact_id)
                && scope
                    .eval_protocol_id
                    .is_none_or(|protocol_id| evaluation.eval_protocol_id == *protocol_id)
        })
}

fn reduction_attestations_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    scope: ValidationEvidenceScope<'_>,
) -> BTreeMap<PeerId, ReductionAttestationEvidence> {
    snapshot
        .reduction_certificate_announcements
        .iter()
        .filter(|announcement| {
            announcement.overlay == *scope.overlay
                && announcement.certificate.study_id == scope.experiment.study_id
                && announcement.certificate.experiment_id == scope.experiment.experiment_id
                && announcement.certificate.revision_id == scope.experiment.revision_id
                && announcement.certificate.aggregate_id == *scope.aggregate_id
                && reduction_evaluation_matches(&announcement.certificate, scope)
                && scope.merge_window.is_none_or(|window| {
                    announcement.certificate.window_id == window.window_id
                        && announcement.certificate.base_head_id == window.base_head_id
                        && promotion_peer_allowed(
                            window,
                            &announcement.certificate.promoter_peer_id,
                        )
                })
        })
        .filter_map(|announcement| {
            announcement
                .certificate
                .evaluation
                .clone()
                .map(|evaluation| {
                    (
                        announcement.certificate.promoter_peer_id.clone(),
                        ReductionAttestationEvidence {
                            reduction_id: announcement.certificate.reduction_id.clone(),
                            evaluation,
                        },
                    )
                })
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
    merge_window: Option<&MergeWindowState>,
) -> bool {
    snapshot.merge_announcements.iter().any(|announcement| {
        announcement.overlay == *overlay
            && announcement.certificate.study_id == experiment.study_id
            && announcement.certificate.experiment_id == experiment.experiment_id
            && announcement.certificate.revision_id == experiment.revision_id
            && announcement.certificate.merged_head_id == *merged_head_id
            && merge_window.is_none_or(|window| {
                announcement.certificate.base_head_id == window.base_head_id
                    && announcement.certificate.promotion_mode == head_promotion_mode(window)
                    && promotion_peer_allowed(window, &announcement.certificate.promoter_peer_id)
            })
    })
}

fn quorum_has_matching_reduction_evidence(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    quorum: &ValidationQuorumCertificate,
) -> bool {
    let (Some(merged_artifact_id), Some(eval_protocol_id)) = (
        quorum.merged_artifact_id.as_ref(),
        quorum.eval_protocol_id.as_ref(),
    ) else {
        return false;
    };
    let required = usize::from(quorum.validator_quorum);
    let mut validators = BTreeSet::new();
    let mut reduction_ids = BTreeSet::new();
    let mut eval_report_ids = BTreeSet::new();
    for announcement in &snapshot.reduction_certificate_announcements {
        let certificate = &announcement.certificate;
        let Some(evaluation) = certificate.evaluation.as_ref() else {
            continue;
        };
        if announcement.overlay != *overlay
            || certificate.study_id != experiment.study_id
            || certificate.experiment_id != experiment.experiment_id
            || certificate.revision_id != experiment.revision_id
            || certificate.window_id != quorum.window_id
            || certificate.base_head_id != quorum.base_head_id
            || certificate.aggregate_id != quorum.aggregate_id
            || certificate.promotion_mode != HeadPromotionMode::ValidatorQuorum
            || certificate.validate_structure().is_err()
            || !quorum
                .attesting_validators
                .contains(&certificate.promoter_peer_id)
            || !quorum.reduction_ids.contains(&certificate.reduction_id)
            || evaluation.head_id != quorum.merged_head_id
            || evaluation.artifact_id != *merged_artifact_id
            || evaluation.eval_protocol_id != *eval_protocol_id
            || !quorum.eval_report_ids.contains(&evaluation.eval_report_id)
        {
            continue;
        }
        validators.insert(certificate.promoter_peer_id.clone());
        reduction_ids.insert(certificate.reduction_id.clone());
        eval_report_ids.insert(evaluation.eval_report_id.clone());
    }
    validators.len() >= required
        && reduction_ids.len() >= required
        && eval_report_ids.len() >= required
}

pub(super) fn validation_quorum_announced_in_snapshot(
    snapshot: &ControlPlaneSnapshot,
    scope: ValidationEvidenceScope<'_>,
) -> bool {
    snapshot
        .validation_quorum_announcements
        .iter()
        .any(|announcement| {
            announcement.overlay == *scope.overlay
                && announcement.certificate.study_id == scope.experiment.study_id
                && announcement.certificate.experiment_id == scope.experiment.experiment_id
                && announcement.certificate.revision_id == scope.experiment.revision_id
                && announcement.certificate.aggregate_id == *scope.aggregate_id
                && announcement.certificate.merged_head_id == *scope.merged_head_id
                && scope.merged_artifact_id.is_none_or(|artifact_id| {
                    announcement.certificate.merged_artifact_id.as_ref() == Some(artifact_id)
                })
                && scope.eval_protocol_id.is_none_or(|protocol_id| {
                    announcement.certificate.eval_protocol_id.as_ref() == Some(protocol_id)
                })
                && announcement.certificate.validate_structure().is_ok()
                && quorum_has_matching_reduction_evidence(
                    snapshot,
                    scope.overlay,
                    scope.experiment,
                    &announcement.certificate,
                )
                && scope.merge_window.is_none_or(|window| {
                    announcement.certificate.window_id == window.window_id
                        && announcement.certificate.base_head_id == window.base_head_id
                        && usize::from(announcement.certificate.validator_quorum)
                            == effective_validator_quorum(window)
                        && announcement
                            .certificate
                            .attesting_validators
                            .iter()
                            .all(|peer_id| window.validators.contains(peer_id))
                })
        })
}

pub(super) fn merge_certificate_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    experiment: &ExperimentHandle,
    merged_head_id: &HeadId,
    merge_window: Option<&MergeWindowState>,
) -> Option<MergeCertificate> {
    snapshot
        .merge_announcements
        .iter()
        .find_map(|announcement| {
            (announcement.overlay == *overlay
                && announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.merged_head_id == *merged_head_id
                && merge_window.is_none_or(|window| {
                    announcement.certificate.base_head_id == window.base_head_id
                        && announcement.certificate.promotion_mode == head_promotion_mode(window)
                        && promotion_peer_allowed(
                            window,
                            &announcement.certificate.promoter_peer_id,
                        )
                }))
            .then(|| announcement.certificate.clone())
        })
}

fn collect_validation_coordination_from_snapshots<'a>(
    snapshots: impl IntoIterator<Item = &'a ControlPlaneSnapshot>,
    scope: ValidationEvidenceScope<'_>,
    local_attestation: Option<&ReductionCertificate>,
) -> ValidationCoordinationState {
    let mut attestations = BTreeMap::new();
    if let Some(certificate) = local_attestation
        && scope
            .merge_window
            .is_none_or(|window| promotion_peer_allowed(window, &certificate.promoter_peer_id))
        && certificate.aggregate_id == *scope.aggregate_id
        && reduction_evaluation_matches(certificate, scope)
        && let Some(evaluation) = certificate.evaluation.clone()
    {
        attestations.insert(
            certificate.promoter_peer_id.clone(),
            ReductionAttestationEvidence {
                reduction_id: certificate.reduction_id.clone(),
                evaluation,
            },
        );
    }
    let mut aggregate_proposal_announced = false;
    let mut quorum_announced = false;
    let mut merge_announced = false;
    let mut merge_certificate = None;
    for snapshot in snapshots {
        attestations.extend(reduction_attestations_from_snapshot(snapshot, scope));
        aggregate_proposal_announced |= aggregate_proposal_announced_in_snapshot(
            snapshot,
            scope.overlay,
            scope.experiment,
            scope.aggregate_id,
        );
        quorum_announced |= validation_quorum_announced_in_snapshot(snapshot, scope);
        merge_announced |= merge_announced_in_snapshot(
            snapshot,
            scope.overlay,
            scope.experiment,
            scope.merged_head_id,
            scope.merge_window,
        );
        if merge_certificate.is_none() {
            merge_certificate = merge_certificate_from_snapshot(
                snapshot,
                scope.overlay,
                scope.experiment,
                scope.merged_head_id,
                scope.merge_window,
            );
        }
    }
    if scope
        .merge_window
        .is_some_and(|window| head_promotion_mode(window) == HeadPromotionMode::ValidatorQuorum)
        && !quorum_announced
    {
        merge_announced = false;
        merge_certificate = None;
    }
    let attesters = attestations.keys().cloned().collect();
    let reduction_ids = attestations
        .values()
        .map(|evidence| evidence.reduction_id.clone())
        .collect();
    let eval_report_ids = attestations
        .values()
        .map(|evidence| evidence.evaluation.eval_report_id.clone())
        .collect();
    let observed_eval_protocol_id = scope.eval_protocol_id.cloned().or_else(|| {
        attestations
            .values()
            .next()
            .map(|evidence| evidence.evaluation.eval_protocol_id.clone())
    });
    ValidationCoordinationState {
        attesters,
        reduction_ids,
        eval_protocol_id: observed_eval_protocol_id,
        eval_report_ids,
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
        let scope =
            ValidationEvidenceScope::new(&overlay, experiment, aggregate_id, merged_head_id);
        Ok(collect_validation_coordination_from_snapshots(
            std::iter::once(&local_snapshot),
            scope,
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
        let scope = ValidationEvidenceScope::new(
            &overlay,
            experiment,
            &execution.aggregate.aggregate_id,
            &execution.merged_head.head_id,
        )
        .with_evaluation(
            &execution.merged_head.artifact_id,
            &execution.head_eval_report.eval_protocol_id,
        )
        .with_merge_window(&prepared.merge_window);
        Ok(collect_validation_coordination_from_snapshots(
            std::iter::once(&local_snapshot)
                .chain(remote_snapshots.iter().map(|(_, snapshot)| snapshot)),
            scope,
            Some(&execution.reduction_certificate),
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
