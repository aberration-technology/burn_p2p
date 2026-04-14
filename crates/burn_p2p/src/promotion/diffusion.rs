use super::*;
use crate::candidate::{
    ValidationCandidate, ValidationCandidateHead, ValidationCandidateLoadArgs,
    ValidationCandidateView, collect_validation_candidate_heads, fallback_best_candidate_index,
    load_validation_candidate_model, select_validation_head,
};
use crate::candidate_screening::{
    CandidateRobustnessContext, PersistedRobustnessState, build_validation_canary_report,
    evaluate_candidate_robustness,
};
use crate::runtime_support::{active_experiment_directory_entry, load_json};
use crate::training::load_model_for_head;

const DIFFUSION_WINDOW_LOOKBACK: usize = 4;
const DIFFUSION_ARTIFACT_SYNC_TIMEOUT: Duration = Duration::from_secs(8);
const DIFFUSION_SNAPSHOT_REFRESH_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DiffusionObservationKey {
    study_id: StudyId,
    experiment_id: ExperimentId,
    revision_id: RevisionId,
    window_id: WindowId,
    base_head_id: HeadId,
}

#[derive(Clone, Debug, Default)]
struct DiffusionObservationState {
    last_support_fingerprint: Option<String>,
    stable_observations: u8,
    last_observed_at: Option<DateTime<Utc>>,
}

#[derive(Default)]
pub(crate) struct DiffusionStateCache {
    observations: BTreeMap<DiffusionObservationKey, DiffusionObservationState>,
}

#[derive(Clone)]
struct DiffusionCandidateSupport {
    merged_head_id: HeadId,
    merged_artifact_id: ArtifactId,
    attesting_trainers: Vec<PeerId>,
    attestation_ids: Vec<ContentId>,
    attester_count: u16,
    cumulative_sample_weight: f64,
    quality_score: Option<f64>,
}

#[derive(Clone)]
pub(crate) struct DiffusionLocalSupport {
    merged_head: HeadDescriptor,
    attestation: TrainerPromotionAttestationAnnouncement,
    needs_publication: bool,
}

pub(crate) struct DiffusionSettlementPublication {
    pub overlay: OverlayTopic,
    pub certificate: DiffusionPromotionCertificate,
    pub merge_certificate: MergeCertificate,
}

impl DiffusionStateCache {
    fn observe_support_map(
        &mut self,
        key: DiffusionObservationKey,
        fingerprint: String,
        observed_at: DateTime<Utc>,
        timeout: Duration,
    ) -> u8 {
        let timeout = chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::MAX);
        let state = self.observations.entry(key).or_default();
        if state
            .last_observed_at
            .is_some_and(|last_observed_at| observed_at - last_observed_at > timeout)
        {
            state.last_support_fingerprint = None;
            state.stable_observations = 0;
        }
        if state.last_support_fingerprint.as_deref() == Some(fingerprint.as_str()) {
            state.stable_observations = state.stable_observations.saturating_add(1);
        } else {
            state.last_support_fingerprint = Some(fingerprint);
            state.stable_observations = 1;
        }
        state.last_observed_at = Some(observed_at);
        state.stable_observations
    }
}

fn compare_diffusion_support(
    left: &DiffusionCandidateSupport,
    right: &DiffusionCandidateSupport,
) -> std::cmp::Ordering {
    left.attester_count
        .cmp(&right.attester_count)
        .then_with(|| {
            left.cumulative_sample_weight
                .total_cmp(&right.cumulative_sample_weight)
        })
        .then_with(|| match (left.quality_score, right.quality_score) {
            (Some(left), Some(right)) => right.total_cmp(&left),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        })
        .then_with(|| right.merged_head_id.cmp(&left.merged_head_id))
}

fn diffusion_merge_windows_from_snapshot(
    snapshot: &ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
    target_window_id: Option<WindowId>,
    target_base_head_id: Option<&HeadId>,
) -> Vec<MergeWindowState> {
    let mut windows = BTreeMap::<(WindowId, HeadId), MergeWindowState>::new();
    for announcement in &snapshot.merge_window_announcements {
        let merge_window = &announcement.merge_window;
        if merge_window.study_id != experiment.study_id
            || merge_window.experiment_id != experiment.experiment_id
            || merge_window.revision_id != experiment.revision_id
            || !matches!(
                merge_window.policy.promotion_policy.mode,
                HeadPromotionMode::DiffusionSteadyState
            )
            || !target_window_id
                .map(|window_id| merge_window.window_id == window_id)
                .unwrap_or(true)
            || !target_base_head_id
                .map(|base_head_id| &merge_window.base_head_id == base_head_id)
                .unwrap_or(true)
        {
            continue;
        }
        let key = (merge_window.window_id, merge_window.base_head_id.clone());
        match windows.get(&key) {
            Some(existing) if existing.opened_at >= merge_window.opened_at => {}
            _ => {
                windows.insert(key, merge_window.clone());
            }
        }
    }
    let mut windows = windows.into_values().collect::<Vec<_>>();
    windows.sort_by(|left, right| {
        right
            .window_id
            .cmp(&left.window_id)
            .then(right.opened_at.cmp(&left.opened_at))
    });
    if target_window_id.is_none() && target_base_head_id.is_none() {
        windows.truncate(DIFFUSION_WINDOW_LOOKBACK);
    }
    windows
}

fn resolve_known_head_descriptor(
    local_snapshot: &ControlPlaneSnapshot,
    cached_snapshots: &[(PeerId, ControlPlaneSnapshot)],
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    head_id: &HeadId,
) -> anyhow::Result<Option<(HeadDescriptor, Vec<PeerId>)>> {
    let matching_announcements = std::iter::once(local_snapshot)
        .chain(cached_snapshots.iter().map(|(_, snapshot)| snapshot))
        .flat_map(|snapshot| snapshot.head_announcements.iter())
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
    Ok(
        load_json::<HeadDescriptor>(storage.scoped_head_path(head_id))?
            .map(|head| (head, provider_peer_ids)),
    )
}

fn local_candidate_head_for_update(
    snapshot: &ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
    local_peer_id: &PeerId,
    base_head_id: &HeadId,
    update: &UpdateAnnounce,
) -> Option<ValidationCandidateHead> {
    snapshot
        .head_announcements
        .iter()
        .filter(|announcement| {
            matches_experiment_head(&announcement.head, experiment)
                && announcement.head.parent_head_id.as_ref() == Some(base_head_id)
                && announcement.head.artifact_id == update.delta_artifact_id
                && announcement.provider_peer_id.as_ref() == Some(local_peer_id)
        })
        .max_by(|left, right| {
            left.head
                .created_at
                .cmp(&right.head.created_at)
                .then(left.announced_at.cmp(&right.announced_at))
        })
        .map(|announcement| ValidationCandidateHead {
            origin_peer_id: local_peer_id.clone(),
            provider_peer_ids: vec![local_peer_id.clone()],
            head: announcement.head.clone(),
            update: update.clone(),
        })
}

fn diffusion_frontier_bounds(
    merge_window: &MergeWindowState,
    robustness_policy: &RobustnessPolicy,
) -> (usize, usize) {
    let max_loaded_candidate_models = usize::from(
        merge_window
            .policy
            .target_leaf_cohort
            .min(robustness_policy.aggregation_policy.maximum_cohort_size)
            .max(1),
    );
    let max_visible_candidate_heads = max_loaded_candidate_models.saturating_mul(2).max(1);
    (max_visible_candidate_heads, max_loaded_candidate_models)
}

fn bounded_candidate_updates(
    updates: &[UpdateAnnounce],
    local_peer_id: &PeerId,
    max_visible_candidate_heads: usize,
) -> Vec<UpdateAnnounce> {
    let mut updates = updates.to_vec();
    updates.sort_by(|left, right| {
        (right.sample_weight * right.quality_weight)
            .total_cmp(&(left.sample_weight * left.quality_weight))
            .then(right.announced_at.cmp(&left.announced_at))
            .then(left.peer_id.cmp(&right.peer_id))
            .then(left.delta_artifact_id.cmp(&right.delta_artifact_id))
            .then(left.receipt_ids.cmp(&right.receipt_ids))
    });
    if updates.len() <= max_visible_candidate_heads {
        return updates;
    }

    let local_update = updates
        .iter()
        .find(|update| &update.peer_id == local_peer_id)
        .cloned();
    updates.truncate(max_visible_candidate_heads);
    if let Some(local_update) = local_update
        && !updates
            .iter()
            .any(|update| update.peer_id == local_update.peer_id)
    {
        let _ = updates.pop();
        updates.push(local_update);
    }
    updates.sort_by(|left, right| {
        left.peer_id
            .cmp(&right.peer_id)
            .then(left.delta_artifact_id.cmp(&right.delta_artifact_id))
            .then(left.receipt_ids.cmp(&right.receipt_ids))
    });
    updates
}

fn local_diffusion_attestation_is_current(
    snapshot: &ControlPlaneSnapshot,
    overlay: &OverlayTopic,
    attestation: &TrainerPromotionAttestation,
) -> bool {
    snapshot
        .trainer_promotion_attestation_announcements
        .iter()
        .filter(|announcement| {
            announcement.overlay == *overlay
                && announcement.attestation.study_id == attestation.study_id
                && announcement.attestation.experiment_id == attestation.experiment_id
                && announcement.attestation.revision_id == attestation.revision_id
                && announcement.attestation.window_id == attestation.window_id
                && announcement.attestation.base_head_id == attestation.base_head_id
                && announcement.attestation.attester_peer_id == attestation.attester_peer_id
        })
        .max_by(|left, right| {
            left.attestation
                .issued_at
                .cmp(&right.attestation.issued_at)
                .then(left.announced_at.cmp(&right.announced_at))
        })
        .is_some_and(|announcement| announcement.attestation == *attestation)
}

fn latest_local_diffusion_attestation_for_window(
    local_snapshot: &ControlPlaneSnapshot,
    experiment: &ExperimentHandle,
    merge_window: &MergeWindowState,
    local_peer_id: &PeerId,
) -> Option<TrainerPromotionAttestationAnnouncement> {
    local_snapshot
        .trainer_promotion_attestation_announcements
        .iter()
        .filter(|announcement| {
            announcement.attestation.study_id == experiment.study_id
                && announcement.attestation.experiment_id == experiment.experiment_id
                && announcement.attestation.revision_id == experiment.revision_id
                && announcement.attestation.window_id == merge_window.window_id
                && announcement.attestation.base_head_id == merge_window.base_head_id
                && announcement.attestation.attester_peer_id == *local_peer_id
        })
        .max_by(|left, right| {
            left.attestation
                .issued_at
                .cmp(&right.attestation.issued_at)
                .then(left.announced_at.cmp(&right.announced_at))
        })
        .cloned()
}

pub(crate) fn latest_local_diffusion_support_for_window(
    local_snapshot: &ControlPlaneSnapshot,
    cached_snapshots: &[(PeerId, ControlPlaneSnapshot)],
    storage: &StorageConfig,
    experiment: &ExperimentHandle,
    merge_window: &MergeWindowState,
    local_peer_id: &PeerId,
) -> anyhow::Result<Option<DiffusionLocalSupport>> {
    let Some(attestation) = latest_local_diffusion_attestation_for_window(
        local_snapshot,
        experiment,
        merge_window,
        local_peer_id,
    ) else {
        return Ok(None);
    };
    let Some((merged_head, _)) = resolve_known_head_descriptor(
        local_snapshot,
        cached_snapshots,
        storage,
        experiment,
        &attestation.attestation.merged_head_id,
    )?
    .filter(|(head, _)| head.artifact_id == attestation.attestation.merged_artifact_id) else {
        return Ok(None);
    };
    Ok(Some(DiffusionLocalSupport {
        merged_head,
        attestation,
        needs_publication: false,
    }))
}

fn latest_attestations_for_window(
    local_snapshot: &ControlPlaneSnapshot,
    cached_snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
    merge_window: &MergeWindowState,
    local_override: Option<&TrainerPromotionAttestationAnnouncement>,
) -> Vec<TrainerPromotionAttestationAnnouncement> {
    let mut latest = BTreeMap::<PeerId, TrainerPromotionAttestationAnnouncement>::new();
    for snapshot in
        std::iter::once(local_snapshot).chain(cached_snapshots.iter().map(|(_, snapshot)| snapshot))
    {
        for announcement in &snapshot.trainer_promotion_attestation_announcements {
            if announcement.attestation.study_id != experiment.study_id
                || announcement.attestation.experiment_id != experiment.experiment_id
                || announcement.attestation.revision_id != experiment.revision_id
                || announcement.attestation.window_id != merge_window.window_id
                || announcement.attestation.base_head_id != merge_window.base_head_id
            {
                continue;
            }
            let key = announcement.attestation.attester_peer_id.clone();
            match latest.get(&key) {
                Some(existing)
                    if existing.attestation.issued_at > announcement.attestation.issued_at
                        || (existing.attestation.issued_at
                            == announcement.attestation.issued_at
                            && existing.announced_at >= announcement.announced_at) => {}
                _ => {
                    latest.insert(key, announcement.clone());
                }
            }
        }
    }
    if let Some(local_override) = local_override {
        latest.insert(
            local_override.attestation.attester_peer_id.clone(),
            local_override.clone(),
        );
    }
    latest.into_values().collect()
}

fn support_map_fingerprint(
    attestations: &[TrainerPromotionAttestationAnnouncement],
) -> anyhow::Result<String> {
    let mut entries = attestations
        .iter()
        .map(|announcement| {
            (
                announcement
                    .attestation
                    .attester_peer_id
                    .as_str()
                    .to_owned(),
                announcement.attestation.merged_head_id.as_str().to_owned(),
                announcement
                    .attestation
                    .merged_artifact_id
                    .as_str()
                    .to_owned(),
            )
        })
        .collect::<Vec<_>>();
    entries.sort();
    Ok(ContentId::derive(&entries)?.as_str().to_owned())
}

fn grouped_diffusion_support(
    attestations: &[TrainerPromotionAttestationAnnouncement],
) -> anyhow::Result<Vec<DiffusionCandidateSupport>> {
    let mut grouped =
        BTreeMap::<(HeadId, ArtifactId), Vec<&TrainerPromotionAttestationAnnouncement>>::new();
    for announcement in attestations {
        grouped
            .entry((
                announcement.attestation.merged_head_id.clone(),
                announcement.attestation.merged_artifact_id.clone(),
            ))
            .or_default()
            .push(announcement);
    }
    let mut supports = Vec::new();
    for ((merged_head_id, merged_artifact_id), announcements) in grouped {
        let mut attesting_trainers = announcements
            .iter()
            .map(|announcement| announcement.attestation.attester_peer_id.clone())
            .collect::<Vec<_>>();
        let mut attestation_ids = announcements
            .iter()
            .map(|announcement| ContentId::derive(&announcement.attestation))
            .collect::<Result<Vec<_>, _>>()?;
        attesting_trainers.sort();
        attesting_trainers.dedup();
        attestation_ids.sort();
        attestation_ids.dedup();
        let quality_score = announcements
            .iter()
            .filter_map(|announcement| announcement.attestation.quality_score)
            .min_by(|left, right| left.total_cmp(right));
        supports.push(DiffusionCandidateSupport {
            merged_head_id,
            merged_artifact_id,
            attesting_trainers: attesting_trainers.clone(),
            attestation_ids,
            attester_count: attesting_trainers.len().min(usize::from(u16::MAX)) as u16,
            cumulative_sample_weight: announcements
                .iter()
                .map(|announcement| announcement.attestation.accepted_sample_weight)
                .sum(),
            quality_score,
        });
    }
    supports.sort_by(|left, right| compare_diffusion_support(right, left));
    Ok(supports)
}

fn best_visible_diffusion_certificate(
    local_snapshot: &ControlPlaneSnapshot,
    cached_snapshots: &[(PeerId, ControlPlaneSnapshot)],
    experiment: &ExperimentHandle,
    merge_window: &MergeWindowState,
) -> Option<DiffusionPromotionCertificate> {
    std::iter::once(local_snapshot)
        .chain(cached_snapshots.iter().map(|(_, snapshot)| snapshot))
        .flat_map(|snapshot| {
            snapshot
                .diffusion_promotion_certificate_announcements
                .iter()
        })
        .filter(|announcement| {
            announcement.certificate.study_id == experiment.study_id
                && announcement.certificate.experiment_id == experiment.experiment_id
                && announcement.certificate.revision_id == experiment.revision_id
                && announcement.certificate.window_id == merge_window.window_id
                && announcement.certificate.base_head_id == merge_window.base_head_id
        })
        .map(|announcement| announcement.certificate.clone())
        .max_by(compare_visible_diffusion_certificate)
}

fn compare_visible_diffusion_certificate(
    left: &DiffusionPromotionCertificate,
    right: &DiffusionPromotionCertificate,
) -> std::cmp::Ordering {
    left.attester_count
        .cmp(&right.attester_count)
        .then_with(|| {
            left.cumulative_sample_weight
                .total_cmp(&right.cumulative_sample_weight)
        })
        .then(left.settled_at.cmp(&right.settled_at))
        .then_with(|| right.merged_head_id.cmp(&left.merged_head_id))
}

fn build_diffusion_merge_certificate(
    experiment: &ExperimentHandle,
    _merge_window: &MergeWindowState,
    local_peer_id: &PeerId,
    attestation: &TrainerPromotionAttestation,
) -> anyhow::Result<MergeCertificate> {
    Ok(MergeCertificate {
        merge_cert_id: ContentId::derive(&(
            experiment.study_id.as_str(),
            experiment.experiment_id.as_str(),
            experiment.revision_id.as_str(),
            attestation.base_head_id.as_str(),
            attestation.merged_head_id.as_str(),
            local_peer_id.as_str(),
        ))?
        .into(),
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        base_head_id: attestation.base_head_id.clone(),
        merged_head_id: attestation.merged_head_id.clone(),
        merged_artifact_id: attestation.merged_artifact_id.clone(),
        policy: MergePolicy::QualityWeightedEma,
        issued_at: Utc::now(),
        promoter_peer_id: local_peer_id.clone(),
        promotion_mode: HeadPromotionMode::DiffusionSteadyState,
        contribution_receipts: attestation.contribution_receipt_ids.clone(),
    })
}

fn build_diffusion_promotion_certificate(
    experiment: &ExperimentHandle,
    merge_window: &MergeWindowState,
    local_peer_id: &PeerId,
    support: &DiffusionCandidateSupport,
) -> DiffusionPromotionCertificate {
    DiffusionPromotionCertificate {
        study_id: experiment.study_id.clone(),
        experiment_id: experiment.experiment_id.clone(),
        revision_id: experiment.revision_id.clone(),
        window_id: merge_window.window_id,
        base_head_id: merge_window.base_head_id.clone(),
        merged_head_id: support.merged_head_id.clone(),
        merged_artifact_id: support.merged_artifact_id.clone(),
        promotion_mode: HeadPromotionMode::DiffusionSteadyState,
        attesting_trainers: support.attesting_trainers.clone(),
        attestation_ids: support.attestation_ids.clone(),
        attester_count: support.attester_count,
        cumulative_sample_weight: support.cumulative_sample_weight,
        settled_at: Utc::now(),
        promoter_peer_id: local_peer_id.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
fn settle_visible_diffusion_support(
    storage: &StorageConfig,
    diffusion_state: &mut DiffusionStateCache,
    experiment: &ExperimentHandle,
    merge_window: &MergeWindowState,
    local_peer_id: &PeerId,
    local_snapshot: &ControlPlaneSnapshot,
    cached_snapshots: &[(PeerId, ControlPlaneSnapshot)],
    local_support: &DiffusionLocalSupport,
) -> anyhow::Result<Option<DiffusionSettlementPublication>> {
    let diffusion_policy = merge_window
        .policy
        .promotion_policy
        .diffusion
        .clone()
        .unwrap_or_default();
    let latest_attestations = latest_attestations_for_window(
        local_snapshot,
        cached_snapshots,
        experiment,
        merge_window,
        Some(&local_support.attestation),
    );
    if latest_attestations.is_empty() {
        return Ok(None);
    }
    let support_fingerprint = support_map_fingerprint(&latest_attestations)?;
    let stable_observations = diffusion_state.observe_support_map(
        DiffusionObservationKey {
            study_id: experiment.study_id.clone(),
            experiment_id: experiment.experiment_id.clone(),
            revision_id: experiment.revision_id.clone(),
            window_id: merge_window.window_id,
            base_head_id: merge_window.base_head_id.clone(),
        },
        support_fingerprint,
        Utc::now(),
        Duration::from_secs(u64::from(diffusion_policy.settlement_timeout_secs.max(1))),
    );
    let supports = grouped_diffusion_support(&latest_attestations)?;
    let Some(leader) = supports.first() else {
        return Ok(None);
    };
    let runner_up = supports.get(1);
    if runner_up.is_some_and(|runner_up| compare_diffusion_support(leader, runner_up).is_eq()) {
        return Ok(None);
    }
    if runner_up.is_some_and(|runner_up| {
        leader.attester_count
            < runner_up
                .attester_count
                .saturating_add(diffusion_policy.support_margin.max(1))
    }) {
        return Ok(None);
    }
    if !diffusion_policy.allow_solo_promotion && leader.attester_count <= 1 {
        return Ok(None);
    }
    let required_stable_observations =
        if diffusion_policy.allow_solo_promotion && leader.attester_count == 1 {
            1
        } else {
            diffusion_policy.required_stable_observations.max(1)
        };
    if stable_observations < required_stable_observations {
        return Ok(None);
    }
    if leader.merged_head_id != local_support.merged_head.head_id
        || leader.merged_artifact_id != local_support.merged_head.artifact_id
    {
        return Ok(None);
    }
    let certificate =
        build_diffusion_promotion_certificate(experiment, merge_window, local_peer_id, leader);
    if let Some(existing_certificate) = best_visible_diffusion_certificate(
        local_snapshot,
        cached_snapshots,
        experiment,
        merge_window,
    ) && compare_visible_diffusion_certificate(&existing_certificate, &certificate).is_ge()
    {
        return Ok(None);
    }

    let merge_certificate = build_diffusion_merge_certificate(
        experiment,
        merge_window,
        local_peer_id,
        &local_support.attestation.attestation,
    )?;
    persist_head_state(storage, experiment, &local_support.merged_head)?;
    persist_json(
        storage.scoped_merge_cert_path(&merge_certificate.merge_cert_id),
        &merge_certificate,
    )?;
    Ok(Some(DiffusionSettlementPublication {
        overlay: local_support.attestation.overlay.clone(),
        certificate,
        merge_certificate,
    }))
}

pub(crate) fn observe_diffusion_steady_state_from_snapshot(
    storage: &StorageConfig,
    network_id: &NetworkId,
    local_snapshot: &ControlPlaneSnapshot,
    local_peer_id: &PeerId,
    diffusion_state: &mut DiffusionStateCache,
) -> anyhow::Result<Vec<DiffusionSettlementPublication>> {
    let latest_attestations = local_snapshot
        .trainer_promotion_attestation_announcements
        .iter()
        .filter(|announcement| {
            announcement.attestation.attester_peer_id == *local_peer_id
                && matches!(
                    announcement.attestation.promotion_mode,
                    HeadPromotionMode::DiffusionSteadyState
                )
        })
        .fold(
            BTreeMap::<
                (StudyId, ExperimentId, RevisionId, WindowId, HeadId),
                TrainerPromotionAttestationAnnouncement,
            >::new(),
            |mut latest, announcement| {
                let key = (
                    announcement.attestation.study_id.clone(),
                    announcement.attestation.experiment_id.clone(),
                    announcement.attestation.revision_id.clone(),
                    announcement.attestation.window_id,
                    announcement.attestation.base_head_id.clone(),
                );
                match latest.get(&key) {
                    Some(existing)
                        if existing.attestation.issued_at > announcement.attestation.issued_at
                            || (existing.attestation.issued_at
                                == announcement.attestation.issued_at
                                && existing.announced_at >= announcement.announced_at) => {}
                    _ => {
                        latest.insert(key, announcement.clone());
                    }
                }
                latest
            },
        );

    let mut publications = Vec::new();
    for announcement in latest_attestations.into_values() {
        let experiment = ExperimentHandle {
            network_id: network_id.clone(),
            study_id: announcement.attestation.study_id.clone(),
            experiment_id: announcement.attestation.experiment_id.clone(),
            revision_id: announcement.attestation.revision_id.clone(),
        };
        let Some(merge_window) = diffusion_merge_windows_from_snapshot(
            local_snapshot,
            &experiment,
            Some(announcement.attestation.window_id),
            Some(&announcement.attestation.base_head_id),
        )
        .into_iter()
        .next() else {
            continue;
        };
        let Some(local_support) = latest_local_diffusion_support_for_window(
            local_snapshot,
            &[],
            storage,
            &experiment,
            &merge_window,
            local_peer_id,
        )?
        else {
            continue;
        };
        if let Some(publication) = settle_visible_diffusion_support(
            storage,
            diffusion_state,
            &experiment,
            &merge_window,
            local_peer_id,
            local_snapshot,
            &[],
            &local_support,
        )? {
            publications.push(publication);
        }
    }

    Ok(publications)
}

impl<P> RunningNode<P>
where
    P: P2pWorkload,
{
    /// Runs one bounded local diffusion-promotion pass for one experiment.
    ///
    /// This is intentionally non-blocking: it refreshes visible trainer
    /// support, may publish one local attestation or diffusion certificate,
    /// and returns immediately. Callers that want eventual convergence should
    /// invoke it opportunistically from their own cadence rather than treating
    /// it as a global barrier.
    pub fn advance_diffusion_steady_state(
        &mut self,
        experiment: &ExperimentHandle,
        target_window_id: Option<WindowId>,
        target_base_head_id: Option<&HeadId>,
    ) -> anyhow::Result<()> {
        let Some(storage) = self.config().storage.as_ref().cloned() else {
            return Ok(());
        };
        let telemetry_snapshot = self.telemetry().snapshot();
        let Some(local_peer_id) = telemetry_snapshot.local_peer_id.clone() else {
            return Ok(());
        };
        let local_snapshot = telemetry_snapshot.control_plane.clone();
        let cached_snapshots = self
            .fetch_experiment_snapshots(experiment, DIFFUSION_SNAPSHOT_REFRESH_TIMEOUT)
            .unwrap_or_else(|_| cached_connected_snapshots(&telemetry_snapshot));
        let windows = diffusion_merge_windows_from_snapshot(
            &local_snapshot,
            experiment,
            target_window_id,
            target_base_head_id,
        );
        for merge_window in windows {
            self.advance_diffusion_window(
                experiment,
                &storage,
                &telemetry_snapshot,
                &local_snapshot,
                &cached_snapshots,
                &local_peer_id,
                &merge_window,
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn advance_diffusion_window(
        &mut self,
        experiment: &ExperimentHandle,
        storage: &StorageConfig,
        telemetry_snapshot: &NodeTelemetrySnapshot,
        local_snapshot: &ControlPlaneSnapshot,
        cached_snapshots: &[(PeerId, ControlPlaneSnapshot)],
        local_peer_id: &PeerId,
        merge_window: &MergeWindowState,
    ) -> anyhow::Result<()> {
        let robustness_policy =
            runtime_robustness_policy(self.config(), telemetry_snapshot, experiment);
        let dataset_view_id =
            active_experiment_directory_entry(self.config(), telemetry_snapshot, experiment)
                .map(|entry| entry.dataset_view_id)
                .unwrap_or_else(|| DatasetViewId::new("runtime-default"));
        let updates = update_announces_from_connected_snapshots(
            local_snapshot,
            cached_snapshots,
            experiment,
            merge_window.window_id,
            &merge_window.base_head_id,
        );
        let existing_local_support = latest_local_diffusion_support_for_window(
            local_snapshot,
            cached_snapshots,
            storage,
            experiment,
            merge_window,
            local_peer_id,
        )?;
        if updates.is_empty() && existing_local_support.is_none() {
            return Ok(());
        }

        let local_support = if updates.is_empty() {
            existing_local_support
        } else {
            self.build_local_diffusion_candidate(
                experiment,
                storage,
                local_snapshot,
                cached_snapshots,
                local_peer_id,
                merge_window,
                &dataset_view_id,
                &robustness_policy,
                &updates,
            )?
            .or(existing_local_support)
        };
        let Some(local_support) = local_support else {
            return Ok(());
        };

        let overlays = experiment.overlay_set()?;
        if local_support.needs_publication {
            persist_json(
                storage.scoped_head_path(&local_support.merged_head.head_id),
                &local_support.merged_head,
            )?;
            self.publish_artifact_from_store(&local_support.merged_head.artifact_id)?;
            self.control.publish_head(HeadAnnouncement {
                overlay: overlays.heads.clone(),
                provider_peer_id: Some(local_peer_id.clone()),
                head: local_support.merged_head.clone(),
                announced_at: Utc::now(),
            })?;
            if !local_diffusion_attestation_is_current(
                local_snapshot,
                &overlays.heads,
                &local_support.attestation.attestation,
            ) {
                self.control
                    .publish_trainer_promotion_attestation(local_support.attestation.clone())?;
            }
        }
        if let Some(publication) = settle_visible_diffusion_support(
            storage,
            &mut self.diffusion_state,
            experiment,
            merge_window,
            local_peer_id,
            local_snapshot,
            cached_snapshots,
            &local_support,
        )? {
            self.control.publish_diffusion_promotion_certificate(
                DiffusionPromotionCertificateAnnouncement {
                    overlay: publication.overlay.clone(),
                    certificate: publication.certificate,
                    announced_at: Utc::now(),
                },
            )?;
            self.control.publish_merge(MergeAnnouncement {
                overlay: publication.overlay,
                certificate: publication.merge_certificate,
                announced_at: Utc::now(),
            })?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_local_diffusion_candidate(
        &mut self,
        experiment: &ExperimentHandle,
        storage: &StorageConfig,
        local_snapshot: &ControlPlaneSnapshot,
        cached_snapshots: &[(PeerId, ControlPlaneSnapshot)],
        local_peer_id: &PeerId,
        merge_window: &MergeWindowState,
        dataset_view_id: &DatasetViewId,
        robustness_policy: &RobustnessPolicy,
        updates: &[UpdateAnnounce],
    ) -> anyhow::Result<Option<DiffusionLocalSupport>> {
        let (max_visible_candidate_heads, max_loaded_candidate_models) =
            diffusion_frontier_bounds(merge_window, robustness_policy);
        let bounded_updates =
            bounded_candidate_updates(updates, local_peer_id, max_visible_candidate_heads);
        let base_head = resolve_known_head_descriptor(
            local_snapshot,
            cached_snapshots,
            storage,
            experiment,
            &merge_window.base_head_id,
        )?;
        let store = FsArtifactStore::new(storage.root.clone());
        if let Some((head, provider_peer_ids)) = base_head.as_ref()
            && !store.has_manifest(&head.artifact_id)
            && head.global_step > 0
            && !provider_peer_ids.is_empty()
        {
            self.wait_for_artifact_from_peers(
                provider_peer_ids,
                &head.artifact_id,
                DIFFUSION_ARTIFACT_SYNC_TIMEOUT,
            )?;
        }
        let current_head = base_head
            .as_ref()
            .map(|(head, _)| (PeerId::new("diffusion-base"), head.clone()));
        let expected_global_step = current_head
            .as_ref()
            .map(|(_, head)| head.global_step + 1)
            .unwrap_or(0);
        let candidate_snapshots = std::iter::once((local_peer_id.clone(), local_snapshot.clone()))
            .chain(cached_snapshots.iter().cloned())
            .collect::<Vec<_>>();
        let candidate_heads = collect_validation_candidate_heads(
            experiment,
            &candidate_snapshots,
            local_peer_id,
            Some(&merge_window.base_head_id),
            expected_global_step,
            &bounded_updates,
        );
        let mut candidate_heads = candidate_heads;
        if let Some(local_update) = bounded_updates
            .iter()
            .find(|update| &update.peer_id == local_peer_id)
            && !candidate_heads
                .iter()
                .any(|candidate_head| candidate_head.origin_peer_id == *local_peer_id)
            && let Some(local_candidate_head) = local_candidate_head_for_update(
                local_snapshot,
                experiment,
                local_peer_id,
                &merge_window.base_head_id,
                local_update,
            )
        {
            candidate_heads.push(local_candidate_head);
        }
        if candidate_heads.is_empty() {
            return Ok(None);
        }
        candidate_heads.sort_by(|left, right| {
            (right.update.sample_weight * right.update.quality_weight)
                .total_cmp(&(left.update.sample_weight * left.update.quality_weight))
                .then(right.update.announced_at.cmp(&left.update.announced_at))
                .then(left.origin_peer_id.cmp(&right.origin_peer_id))
                .then(left.head.artifact_id.cmp(&right.head.artifact_id))
        });
        candidate_heads.truncate(max_loaded_candidate_models.max(1));

        let canary_threshold = robustness_policy
            .validator_canary_policy
            .maximum_regression_delta;
        let mut loaded_candidates = Vec::<ValidationCandidate<P::Model>>::new();
        let base_model = {
            let node = self
                .node
                .as_mut()
                .expect("running node should retain prepared node");
            let project = &mut node.project;
            let device = project.runtime_device();
            if let Some((head, _)) = base_head.as_ref() {
                load_model_for_head(project, head, &store, &device)?
            } else {
                project.init_model(&device)
            }
        };
        for candidate_head in candidate_heads {
            if !store.has_manifest(&candidate_head.head.artifact_id)
                && !candidate_head.provider_peer_ids.is_empty()
            {
                let _ = self.wait_for_artifact_from_peers(
                    &candidate_head.provider_peer_ids,
                    &candidate_head.head.artifact_id,
                    DIFFUSION_ARTIFACT_SYNC_TIMEOUT,
                );
            }
            if !store.has_manifest(&candidate_head.head.artifact_id) {
                continue;
            }
            let loaded = {
                let node = self
                    .node
                    .as_mut()
                    .expect("running node should retain prepared node");
                let project = &mut node.project;
                let device = project.runtime_device();
                load_validation_candidate_model(
                    project,
                    ValidationCandidateLoadArgs {
                        experiment,
                        store: &store,
                        device: &device,
                        current_head: &current_head,
                        canary_threshold,
                        evaluate_candidates: true,
                    },
                    candidate_head,
                )?
            };
            loaded_candidates.push(loaded);
        }
        if loaded_candidates.is_empty() {
            return Ok(None);
        }
        let candidate_views = loaded_candidates
            .iter()
            .map(ValidationCandidateView::from)
            .collect::<Vec<_>>();
        let robustness_outcome = evaluate_candidate_robustness(
            &burn_p2p_security::RobustnessEngine::new(robustness_policy.clone()),
            CandidateRobustnessContext {
                robustness_policy,
                robustness_state: &load_json::<PersistedRobustnessState>(
                    storage.scoped_robustness_state_path(experiment),
                )?
                .unwrap_or_default(),
                snapshots: cached_snapshots,
                base_head_id: &merge_window.base_head_id,
                dataset_view_id,
                merge_window,
            },
            &candidate_views,
            Utc::now(),
        );
        let mut filtered_updates = robustness_outcome.filtered_updates;
        let mut accepted_candidates = loaded_candidates
            .iter()
            .filter_map(|candidate| {
                robustness_outcome
                    .accepted_weights
                    .get(&(
                        candidate.peer_id.clone(),
                        candidate.head.artifact_id.clone(),
                    ))
                    .copied()
                    .map(|effective_weight| ValidationCandidateView {
                        peer_id: &candidate.peer_id,
                        head: &candidate.head,
                        update: &candidate.update,
                        evaluation: &candidate.evaluation,
                        canary_report: candidate.canary_report.as_ref(),
                        sample_weight: candidate.sample_weight * effective_weight.max(0.01),
                        quality_weight: candidate.quality_weight,
                        model: &candidate.model,
                    })
            })
            .collect::<Vec<_>>();
        if accepted_candidates.is_empty() {
            return Ok(None);
        }
        accepted_candidates.sort_by(|left, right| {
            left.peer_id
                .cmp(right.peer_id)
                .then(left.head.head_id.cmp(&right.head.head_id))
                .then(left.head.artifact_id.cmp(&right.head.artifact_id))
        });
        filtered_updates.sort_by(|left, right| {
            left.peer_id
                .cmp(&right.peer_id)
                .then(left.delta_artifact_id.cmp(&right.delta_artifact_id))
                .then(left.receipt_ids.cmp(&right.receipt_ids))
        });
        let Some(fallback_best_index) = fallback_best_candidate_index(&accepted_candidates) else {
            return Ok(None);
        };
        let (merged_head, evaluation) = {
            let node = self
                .node
                .as_mut()
                .expect("running node should retain prepared node");
            let project = &mut node.project;
            let (source_peer_id, merged_head, evaluation) = select_validation_head(
                project,
                experiment,
                &store,
                &current_head,
                &merge_window.base_head_id,
                merge_window.window_id,
                &base_model,
                &accepted_candidates,
                fallback_best_index,
                MergePolicy::QualityWeightedEma,
                local_peer_id,
            )?;
            let _ = source_peer_id;
            (merged_head, evaluation)
        };
        let canary_report = build_validation_canary_report(
            experiment,
            &current_head,
            &merged_head,
            &evaluation,
            robustness_policy
                .validator_canary_policy
                .maximum_regression_delta,
            1,
        )?;
        if !canary_report.accepted
            || canary_report.detected_backdoor_trigger
            || canary_report.regression_margin
                > robustness_policy
                    .validator_canary_policy
                    .maximum_regression_delta
        {
            return Ok(None);
        }
        let mut contribution_receipt_ids = filtered_updates
            .iter()
            .flat_map(|update| update.receipt_ids.clone())
            .collect::<Vec<_>>();
        contribution_receipt_ids.sort();
        contribution_receipt_ids.dedup();
        let contribution_receipt_root = ContentId::derive(&contribution_receipt_ids)?;
        Ok(Some(DiffusionLocalSupport {
            merged_head: merged_head.clone(),
            attestation: TrainerPromotionAttestationAnnouncement {
                overlay: experiment.overlay_set()?.heads,
                attestation: TrainerPromotionAttestation {
                    study_id: experiment.study_id.clone(),
                    experiment_id: experiment.experiment_id.clone(),
                    revision_id: experiment.revision_id.clone(),
                    window_id: merge_window.window_id,
                    base_head_id: merge_window.base_head_id.clone(),
                    merged_head_id: merged_head.head_id.clone(),
                    merged_artifact_id: merged_head.artifact_id.clone(),
                    contribution_receipt_ids,
                    contribution_receipt_root,
                    attester_peer_id: local_peer_id.clone(),
                    promotion_mode: HeadPromotionMode::DiffusionSteadyState,
                    accepted_update_count: filtered_updates.len().min(u32::MAX as usize) as u32,
                    accepted_sample_weight: filtered_updates
                        .iter()
                        .map(|update| update.sample_weight)
                        .sum(),
                    quality_score: (!evaluation.metrics.is_empty())
                        .then(|| metric_quality(&evaluation.metrics)),
                    issued_at: Utc::now(),
                },
                announced_at: Utc::now(),
            },
            needs_publication: true,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_update(peer: &str, sample_weight: f64, quality_weight: f64) -> UpdateAnnounce {
        UpdateAnnounce {
            peer_id: PeerId::new(peer),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            window_id: WindowId(7),
            base_head_id: HeadId::new("base"),
            lease_id: None,
            delta_artifact_id: ArtifactId::new(format!("artifact-{peer}")),
            sample_weight,
            quality_weight,
            norm_stats: UpdateNormStats {
                l2_norm: 1.0,
                max_abs: 1.0,
                clipped: false,
                non_finite_tensors: 0,
            },
            feature_sketch: None,
            receipt_root: ContentId::new(format!("root-{peer}")),
            receipt_ids: vec![ContributionReceiptId::new(format!("receipt-{peer}"))],
            providers: vec![PeerId::new(peer)],
            announced_at: Utc::now(),
        }
    }

    fn test_attestation(
        attester: &str,
        head: &str,
        artifact: &str,
        sample_weight: f64,
        quality_score: Option<f64>,
        issued_at: DateTime<Utc>,
    ) -> TrainerPromotionAttestationAnnouncement {
        TrainerPromotionAttestationAnnouncement {
            overlay: OverlayTopic::experiment(
                NetworkId::new("net"),
                StudyId::new("study"),
                ExperimentId::new("experiment"),
                OverlayChannel::Heads,
            )
            .expect("heads overlay"),
            attestation: TrainerPromotionAttestation {
                study_id: StudyId::new("study"),
                experiment_id: ExperimentId::new("experiment"),
                revision_id: RevisionId::new("revision"),
                window_id: WindowId(7),
                base_head_id: HeadId::new("base"),
                merged_head_id: HeadId::new(head),
                merged_artifact_id: ArtifactId::new(artifact),
                contribution_receipt_ids: vec![ContributionReceiptId::new(format!(
                    "receipt-{attester}"
                ))],
                contribution_receipt_root: ContentId::new(format!("root-{attester}")),
                attester_peer_id: PeerId::new(attester),
                promotion_mode: HeadPromotionMode::DiffusionSteadyState,
                accepted_update_count: 1,
                accepted_sample_weight: sample_weight,
                quality_score,
                issued_at,
            },
            announced_at: issued_at,
        }
    }

    #[test]
    fn bounded_candidate_updates_keeps_local_update_within_frontier_cap() {
        let updates = vec![
            test_update("peer-remote-a", 32.0, 1.0),
            test_update("peer-remote-b", 24.0, 1.0),
            test_update("peer-local", 1.0, 0.25),
        ];

        let bounded = bounded_candidate_updates(&updates, &PeerId::new("peer-local"), 2);

        assert_eq!(bounded.len(), 2);
        assert!(
            bounded
                .iter()
                .any(|update| update.peer_id == PeerId::new("peer-local"))
        );
    }

    #[test]
    fn grouped_diffusion_support_ranks_by_support_then_weight_then_quality() {
        let now = Utc::now();
        let attestations = vec![
            test_attestation("peer-a", "head-1", "artifact-1", 8.0, Some(0.4), now),
            test_attestation(
                "peer-b",
                "head-1",
                "artifact-1",
                4.0,
                Some(0.5),
                now + chrono::Duration::seconds(1),
            ),
            test_attestation(
                "peer-c",
                "head-2",
                "artifact-2",
                50.0,
                Some(0.2),
                now + chrono::Duration::seconds(2),
            ),
        ];

        let supports = grouped_diffusion_support(&attestations).expect("group diffusion support");

        assert_eq!(supports.len(), 2);
        assert_eq!(supports[0].merged_head_id, HeadId::new("head-1"));
        assert_eq!(supports[0].attester_count, 2);
        assert_eq!(supports[1].merged_head_id, HeadId::new("head-2"));
        assert_eq!(supports[1].attester_count, 1);
    }
}
