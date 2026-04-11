use super::*;

pub(super) fn plan_prefetch_lease_for_window(
    args: PrefetchLeasePlanArgs<'_>,
) -> anyhow::Result<PlannedLease> {
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
        .min(adaptive_microshard_cap(
            fair_share_microshard_cap(args.assignment_peers.len(), preferred_microshards.len()),
            args.adaptation_factor,
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

pub(super) fn supports_background_shard_prefetch(limit_profile: &LimitProfile) -> bool {
    matches!(
        limit_profile
            .estimate
            .preferred_backends
            .first()
            .map(String::as_str),
        Some("cuda" | "wgpu")
    )
}

pub(super) fn prefetched_lease_is_reusable(
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

pub(super) fn wait_for_prefetch_completion(
    handle: &std::thread::JoinHandle<()>,
    grace: Duration,
) -> bool {
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

pub(super) fn ensure_training_placement_roles(
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

pub(super) fn training_placement_budget_work_units(
    recommended_budget_work_units: u64,
    capability: &CapabilityEstimate,
    directory_entry: Option<&ExperimentDirectoryEntry>,
    adaptation_factor: f64,
    planner_budget_scale: f64,
) -> anyhow::Result<u64> {
    let Some(directory_entry) = directory_entry else {
        return Ok(adaptive_budget_work_units(
            scheduled_budget_work_units(recommended_budget_work_units.max(1), planner_budget_scale),
            adaptation_factor,
        ));
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

    Ok(adaptive_budget_work_units(
        scheduled_budget_work_units(
            recommended_budget_work_units
                .max(1)
                .min(placement_budget_work_units),
            planner_budget_scale,
        )
        .min(placement_budget_work_units.max(1)),
        adaptation_factor,
    ))
}

pub(super) fn fair_share_microshard_cap(
    assignment_peer_count: usize,
    available_microshard_count: usize,
) -> usize {
    available_microshard_count
        .max(1)
        .div_ceil(assignment_peer_count.max(1))
}

pub(super) fn fair_share_budget_work_units(
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

pub(super) fn adaptive_budget_work_units(
    base_budget_work_units: u64,
    adaptation_factor: f64,
) -> u64 {
    ((base_budget_work_units.max(1) as f64) * adaptation_factor.clamp(0.2, 1.0))
        .ceil()
        .max(1.0) as u64
}

pub(super) fn adaptive_microshard_cap(base_cap: usize, adaptation_factor: f64) -> usize {
    ((base_cap.max(1) as f64) * adaptation_factor.clamp(0.2, 1.0))
        .ceil()
        .max(1.0) as usize
}

pub(super) fn scheduled_budget_work_units(
    base_budget_work_units: u64,
    planner_budget_scale: f64,
) -> u64 {
    ((base_budget_work_units.max(1) as f64) * planner_budget_scale.clamp(0.35, 1.6))
        .ceil()
        .max(1.0) as u64
}

pub(super) fn scheduled_microshard_cap(base_cap: usize, planner_microshard_scale: f64) -> usize {
    ((base_cap.max(1) as f64) * planner_microshard_scale.clamp(0.35, 1.4))
        .ceil()
        .max(1.0) as usize
}

pub(super) fn unleased_microshards_for_window(
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

pub(super) fn merge_connected_lease_announcements(
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

pub(super) fn preferred_microshards_for_peer(
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

pub(super) fn load_runtime_model<P>(
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

pub(super) fn load_model_for_head<P>(
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

pub(super) fn runtime_blocked_reason(prefix: &str, lag_assessment: &LagAssessment) -> String {
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
