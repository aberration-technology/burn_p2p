use super::*;
use burn_p2p_core::{
    FleetPlacementPeer, FleetPlacementSnapshot, SignatureAlgorithm, SignatureMetadata,
};
use std::cmp::Ordering;

pub(crate) const TRAINING_PLACEMENT_HISTORY_WINDOWS: usize = 8;
const TRAINING_PLACEMENT_HINT_FRESHNESS_MINUTES: i64 = 30;
const TRAINING_PLACEMENT_HISTORY_DECAY_FACTOR: f64 = 0.65;
const TRAINING_PLACEMENT_FAILURE_STREAK_PRUNE: usize = 2;
const TRAINING_PLACEMENT_MIN_WINDOWS_FOR_HEALTH_PRUNE: usize = 3;
const TRAINING_PLACEMENT_MIN_WEIGHTED_HEALTH: f64 = 0.2;
pub(crate) const FLEET_PLACEMENT_SNAPSHOT_FRESHNESS_SECS: u32 = 15 * 60;
const FLEET_PLACEMENT_HISTORY_HORIZON_SECS: i64 = 6 * 60 * 60;
const FLEET_PLACEMENT_HISTORY_MAX_SNAPSHOTS: usize = 24;
const FLEET_PLACEMENT_HISTORY_DECAY_FACTOR: f64 = 0.82;
const FLEET_PLACEMENT_MIN_SNAPSHOTS_FOR_GLOBAL_BACKPRESSURE: usize = 4;
const FLEET_PLACEMENT_MIN_DISTINCT_PLANNERS_FOR_CONSENSUS: usize = 2;
const FLEET_PLACEMENT_MIN_SUPPORT_RATIO_FOR_ASSIGNMENT: f64 = 0.2;
const FLEET_PLACEMENT_MIN_CONSENSUS_RATIO_FOR_ASSIGNMENT: f64 = 0.35;
const FLEET_PLACEMENT_MIN_BUDGET_SCALE_FOR_ASSIGNMENT: f64 = 0.55;
const FLEET_PLACEMENT_MIN_MICROSHARD_SCALE_FOR_ASSIGNMENT: f64 = 0.55;
pub(crate) const FLEET_PLACEMENT_PLANNER_VERSION: &str = "burn_p2p.runtime.v1";

#[derive(Clone, Debug)]
struct TrainingPlacementHistory {
    recent_window_count: usize,
    completed_window_count: usize,
    recent_failure_streak: usize,
    latest_status: PeerWindowStatus,
    latest_finished_at: DateTime<Utc>,
    weighted_health: f64,
    weighted_head_lag: f64,
    weighted_throughput: f64,
}

#[derive(Clone, Debug)]
struct FleetPlacementPlannerHistory {
    snapshot_count: usize,
    distinct_planner_count: usize,
    selected_planner_count: usize,
    visible_weight: f64,
    selected_weight: f64,
    weighted_rank: f64,
    weighted_health: f64,
    weighted_head_lag: f64,
    weighted_throughput: f64,
    weighted_trust: f64,
    weighted_budget_scale: f64,
    weighted_microshard_scale: f64,
    oldest_generated_at: DateTime<Utc>,
    latest_generated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
struct FleetPlacementPlannerPeerContribution {
    snapshot_count: usize,
    visible_weight: f64,
    selected_weight: f64,
    weighted_rank: f64,
    weighted_health: f64,
    weighted_head_lag: f64,
    weighted_throughput: f64,
    weighted_trust: f64,
    weighted_budget_scale: f64,
    weighted_microshard_scale: f64,
    oldest_generated_at: Option<DateTime<Utc>>,
    latest_generated_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TrainingScheduleHint {
    pub budget_scale: f64,
    pub microshard_scale: f64,
}

impl Default for TrainingScheduleHint {
    fn default() -> Self {
        Self {
            budget_scale: 1.0,
            microshard_scale: 1.0,
        }
    }
}

fn training_status_health_score(status: &PeerWindowStatus) -> f64 {
    match status {
        PeerWindowStatus::Completed => 1.0,
        PeerWindowStatus::Aborted => 0.4,
        PeerWindowStatus::Rejected => 0.2,
        PeerWindowStatus::Failed => 0.0,
    }
}

fn recent_training_peer_window_histories(
    snapshot: &NodeTelemetrySnapshot,
    latest_hints: &[PeerWindowPlacementHint],
) -> BTreeMap<PeerId, Vec<PeerWindowPlacementHint>> {
    let freshness_cutoff =
        Utc::now() - chrono::Duration::minutes(TRAINING_PLACEMENT_HINT_FRESHNESS_MINUTES);
    let mut hints = BTreeMap::<PeerId, Vec<PeerWindowPlacementHint>>::new();
    for announcement in &snapshot.control_plane.metrics_announcements {
        for hint in &announcement.peer_window_hints {
            if hint.window_finished_at < freshness_cutoff {
                continue;
            }
            if !matches!(
                hint.role,
                PeerRole::TrainerGpu
                    | PeerRole::TrainerCpu
                    | PeerRole::BrowserTrainerWgpu
                    | PeerRole::BrowserTrainer
            ) {
                continue;
            }
            hints
                .entry(hint.peer_id.clone())
                .or_default()
                .push(hint.clone());
        }
    }
    for hint in latest_hints {
        if hint.window_finished_at < freshness_cutoff {
            continue;
        }
        if !matches!(
            hint.role,
            PeerRole::TrainerGpu
                | PeerRole::TrainerCpu
                | PeerRole::BrowserTrainerWgpu
                | PeerRole::BrowserTrainer
        ) {
            continue;
        }
        hints
            .entry(hint.peer_id.clone())
            .or_default()
            .push(hint.clone());
    }
    for values in hints.values_mut() {
        values.sort_by_key(|hint| std::cmp::Reverse(hint.window_finished_at));
        values.truncate(TRAINING_PLACEMENT_HISTORY_WINDOWS);
    }
    hints
}

fn summarize_training_peer_window_history(
    hints: &[PeerWindowPlacementHint],
) -> Option<TrainingPlacementHistory> {
    let latest = hints.first()?;
    let mut total_weight = 0.0;
    let mut weighted_health = 0.0;
    let mut weighted_head_lag = 0.0;
    let mut weighted_throughput = 0.0;
    let mut completed_window_count = 0usize;

    for (index, hint) in hints.iter().enumerate() {
        let weight = TRAINING_PLACEMENT_HISTORY_DECAY_FACTOR.powi(index as i32);
        total_weight += weight;
        weighted_health += weight * training_status_health_score(&hint.status);
        weighted_head_lag += weight * hint.head_lag_at_finish as f64;
        weighted_throughput += weight
            * ((hint.accepted_tokens_or_samples.max(1) as f64 * 1000.0)
                / hint.window_elapsed_ms.max(1) as f64);
        if hint.status == PeerWindowStatus::Completed {
            completed_window_count += 1;
        }
    }

    let recent_failure_streak = hints
        .iter()
        .take_while(|hint| hint.status != PeerWindowStatus::Completed)
        .count();

    Some(TrainingPlacementHistory {
        recent_window_count: hints.len(),
        completed_window_count,
        recent_failure_streak,
        latest_status: latest.status.clone(),
        latest_finished_at: latest.window_finished_at,
        weighted_health: weighted_health / total_weight.max(f64::EPSILON),
        weighted_head_lag: weighted_head_lag / total_weight.max(f64::EPSILON),
        weighted_throughput: weighted_throughput / total_weight.max(f64::EPSILON),
    })
}

fn peer_trust_score_value(snapshot: &NodeTelemetrySnapshot, peer_id: &PeerId) -> f64 {
    snapshot
        .trust_scores
        .iter()
        .find(|score| &score.peer_id == peer_id)
        .map(|score| score.score)
        .unwrap_or(0.0)
}

fn training_history_priority_rank(history: Option<&TrainingPlacementHistory>) -> u8 {
    match history {
        Some(history)
            if history.latest_status == PeerWindowStatus::Completed
                && history.recent_failure_streak == 0 =>
        {
            3
        }
        Some(history) if history.completed_window_count > 0 => 2,
        None => 1,
        Some(_) => 0,
    }
}

fn compare_training_assignment_peer(
    snapshot: &NodeTelemetrySnapshot,
    histories: &BTreeMap<PeerId, TrainingPlacementHistory>,
    planner_histories: &BTreeMap<PeerId, FleetPlacementPlannerHistory>,
    left: &PeerId,
    right: &PeerId,
) -> Ordering {
    let left_history = histories.get(left);
    let right_history = histories.get(right);
    let left_rank = training_history_priority_rank(left_history);
    let right_rank = training_history_priority_rank(right_history);
    if left_rank != right_rank {
        return right_rank.cmp(&left_rank);
    }

    if let (Some(left_history), Some(right_history)) = (left_history, right_history) {
        let left_planner = planner_histories.get(left);
        let right_planner = planner_histories.get(right);
        let consensus_cmp = planner_consensus_ratio(right_planner)
            .total_cmp(&planner_consensus_ratio(left_planner));
        if consensus_cmp != Ordering::Equal {
            return consensus_cmp;
        }
        let distinct_cmp =
            planner_distinct_count(right_planner).cmp(&planner_distinct_count(left_planner));
        if distinct_cmp != Ordering::Equal {
            return distinct_cmp;
        }
        let support_cmp =
            planner_support_ratio(right_planner).total_cmp(&planner_support_ratio(left_planner));
        if support_cmp != Ordering::Equal {
            return support_cmp;
        }
        let rank_cmp =
            planner_average_rank(left_planner).total_cmp(&planner_average_rank(right_planner));
        if rank_cmp != Ordering::Equal {
            return rank_cmp;
        }
        let planner_lag_cmp = planner_average_head_lag(left_planner)
            .total_cmp(&planner_average_head_lag(right_planner));
        if planner_lag_cmp != Ordering::Equal {
            return planner_lag_cmp;
        }
        let planner_health_cmp =
            planner_average_health(right_planner).total_cmp(&planner_average_health(left_planner));
        if planner_health_cmp != Ordering::Equal {
            return planner_health_cmp;
        }
        let planner_throughput_cmp = planner_average_throughput(right_planner)
            .total_cmp(&planner_average_throughput(left_planner));
        if planner_throughput_cmp != Ordering::Equal {
            return planner_throughput_cmp;
        }
        let lag_cmp = left_history
            .weighted_head_lag
            .total_cmp(&right_history.weighted_head_lag);
        if lag_cmp != Ordering::Equal {
            return lag_cmp;
        }
        let health_cmp = right_history
            .weighted_health
            .total_cmp(&left_history.weighted_health);
        if health_cmp != Ordering::Equal {
            return health_cmp;
        }
        let throughput_cmp = right_history
            .weighted_throughput
            .total_cmp(&left_history.weighted_throughput);
        if throughput_cmp != Ordering::Equal {
            return throughput_cmp;
        }
        let recency_cmp = left_history
            .latest_finished_at
            .cmp(&right_history.latest_finished_at);
        if recency_cmp != Ordering::Equal {
            return recency_cmp.reverse();
        }
    }

    let left_planner = planner_histories.get(left);
    let right_planner = planner_histories.get(right);
    let consensus_cmp =
        planner_consensus_ratio(right_planner).total_cmp(&planner_consensus_ratio(left_planner));
    if consensus_cmp != Ordering::Equal {
        return consensus_cmp;
    }
    let distinct_cmp =
        planner_distinct_count(right_planner).cmp(&planner_distinct_count(left_planner));
    if distinct_cmp != Ordering::Equal {
        return distinct_cmp;
    }
    let support_cmp =
        planner_support_ratio(right_planner).total_cmp(&planner_support_ratio(left_planner));
    if support_cmp != Ordering::Equal {
        return support_cmp;
    }
    let rank_cmp =
        planner_average_rank(left_planner).total_cmp(&planner_average_rank(right_planner));
    if rank_cmp != Ordering::Equal {
        return rank_cmp;
    }
    let planner_lag_cmp =
        planner_average_head_lag(left_planner).total_cmp(&planner_average_head_lag(right_planner));
    if planner_lag_cmp != Ordering::Equal {
        return planner_lag_cmp;
    }
    let planner_health_cmp =
        planner_average_health(right_planner).total_cmp(&planner_average_health(left_planner));
    if planner_health_cmp != Ordering::Equal {
        return planner_health_cmp;
    }
    let planner_throughput_cmp = planner_average_throughput(right_planner)
        .total_cmp(&planner_average_throughput(left_planner));
    if planner_throughput_cmp != Ordering::Equal {
        return planner_throughput_cmp;
    }
    let planner_trust_cmp =
        planner_average_trust(right_planner).total_cmp(&planner_average_trust(left_planner));
    if planner_trust_cmp != Ordering::Equal {
        return planner_trust_cmp;
    }

    let left_trust = peer_trust_score_value(snapshot, left);
    let right_trust = peer_trust_score_value(snapshot, right);
    let trust_cmp = right_trust.total_cmp(&left_trust);
    if trust_cmp != Ordering::Equal {
        return trust_cmp;
    }

    left.as_str().cmp(right.as_str())
}

fn training_assignment_histories(
    snapshot: &NodeTelemetrySnapshot,
    latest_hints: &[PeerWindowPlacementHint],
) -> BTreeMap<PeerId, TrainingPlacementHistory> {
    recent_training_peer_window_histories(snapshot, latest_hints)
        .into_iter()
        .filter_map(|(peer_id, hints)| {
            summarize_training_peer_window_history(&hints).map(|history| (peer_id, history))
        })
        .collect()
}

pub(crate) fn local_training_adaptation_factor(
    snapshot: &NodeTelemetrySnapshot,
    local_peer_id: &PeerId,
) -> f64 {
    let histories = training_assignment_histories(snapshot, &[]);
    let planner_histories = fleet_placement_planner_histories(snapshot);
    let lag_penalty = (1.0 / (1.0 + snapshot.head_lag_steps as f64 * 0.2)).clamp(0.2, 1.0);
    let local_history_penalty = histories
        .get(local_peer_id)
        .map(|history| {
            let failure_penalty =
                (1.0 / (1.0 + history.recent_failure_streak as f64 * 0.75)).clamp(0.35, 1.0);
            let completion_penalty = if history.latest_status == PeerWindowStatus::Completed {
                1.0
            } else {
                0.75
            };
            failure_penalty * history.weighted_health.clamp(0.35, 1.0) * completion_penalty
        })
        .unwrap_or(1.0);
    let planner_penalty = planner_histories
        .get(local_peer_id)
        .and_then(|history| {
            (history.snapshot_count >= 3
                && history.distinct_planner_count
                    >= FLEET_PLACEMENT_MIN_DISTINCT_PLANNERS_FOR_CONSENSUS
                && history.visible_weight > f64::EPSILON)
                .then_some(
                    (planner_support_ratio(Some(history)) * 0.65
                        + planner_consensus_ratio(Some(history)) * 0.35)
                        .clamp(0.35, 1.0),
                )
        })
        .unwrap_or(1.0);
    let planner_backpressure_penalty = (1.0
        - planner_global_backpressure(planner_histories.get(local_peer_id)) * 0.7)
        .clamp(0.25, 1.0);

    (lag_penalty * local_history_penalty * planner_penalty * planner_backpressure_penalty)
        .clamp(0.2, 1.0)
}

fn planner_average_budget_scale(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_budget_scale / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(1.0)
}

fn planner_average_microshard_scale(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_microshard_scale / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(1.0)
}

fn planner_global_backpressure(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    let Some(history) = history else {
        return 0.0;
    };
    if history.snapshot_count < FLEET_PLACEMENT_MIN_SNAPSHOTS_FOR_GLOBAL_BACKPRESSURE {
        return 0.0;
    }
    let support_gap = (1.0 - planner_support_ratio(Some(history))).clamp(0.0, 1.0);
    let consensus_gap = (1.0 - planner_consensus_ratio(Some(history))).clamp(0.0, 1.0);
    let budget_gap = (1.0 - planner_average_budget_scale(Some(history))).clamp(0.0, 1.0);
    let microshard_gap = (1.0 - planner_average_microshard_scale(Some(history))).clamp(0.0, 1.0);
    (support_gap * 0.3 + consensus_gap * 0.25 + budget_gap * 0.3 + microshard_gap * 0.15)
        .clamp(0.0, 1.0)
}

fn planner_prunes_training_assignment(history: Option<&FleetPlacementPlannerHistory>) -> bool {
    let Some(history) = history else {
        return false;
    };
    history.snapshot_count >= FLEET_PLACEMENT_MIN_SNAPSHOTS_FOR_GLOBAL_BACKPRESSURE
        && history.distinct_planner_count >= FLEET_PLACEMENT_MIN_DISTINCT_PLANNERS_FOR_CONSENSUS
        && planner_support_ratio(Some(history)) < FLEET_PLACEMENT_MIN_SUPPORT_RATIO_FOR_ASSIGNMENT
        && planner_consensus_ratio(Some(history))
            < FLEET_PLACEMENT_MIN_CONSENSUS_RATIO_FOR_ASSIGNMENT
        && planner_average_budget_scale(Some(history))
            < FLEET_PLACEMENT_MIN_BUDGET_SCALE_FOR_ASSIGNMENT
        && planner_average_microshard_scale(Some(history))
            < FLEET_PLACEMENT_MIN_MICROSHARD_SCALE_FOR_ASSIGNMENT
}

pub(crate) fn local_training_schedule_hint(
    snapshot: &NodeTelemetrySnapshot,
    local_peer_id: &PeerId,
) -> TrainingScheduleHint {
    let histories = training_assignment_histories(snapshot, &[]);
    let planner_histories = fleet_placement_planner_histories(snapshot);
    let planner_history = planner_histories.get(local_peer_id);
    let local_history = histories.get(local_peer_id);

    let planner_support = planner_support_ratio(planner_history);
    let planner_consensus = planner_consensus_ratio(planner_history);
    let planner_backpressure = planner_global_backpressure(planner_history);
    let lag_penalty = (1.0 / (1.0 + snapshot.head_lag_steps as f64 * 0.15)).clamp(0.45, 1.0);
    let failure_penalty = local_history
        .map(|history| (1.0 / (1.0 + history.recent_failure_streak as f64 * 0.5)).clamp(0.5, 1.0))
        .unwrap_or(1.0);
    let health_factor = local_history
        .map(|history| history.weighted_health.clamp(0.45, 1.2))
        .unwrap_or(1.0);
    let confidence = planner_scaling_confidence(planner_history);
    let planner_pressure_scale = (1.0 - planner_backpressure * 0.6).clamp(0.4, 1.0);
    let budget_scale = if confidence <= f64::EPSILON {
        1.0
    } else {
        let scaled = 1.0
            + (planner_average_budget_scale(planner_history) - 1.0)
                * confidence
                * (0.65 + planner_consensus * 0.35);
        (scaled
            * lag_penalty
            * failure_penalty
            * health_factor.clamp(0.8, 1.1)
            * planner_pressure_scale)
            .clamp(0.25, 1.6)
    };
    let microshard_scale = if confidence <= f64::EPSILON {
        1.0
    } else {
        let scaled = 1.0
            + (planner_average_microshard_scale(planner_history) - 1.0)
                * confidence
                * (0.65 + planner_support * 0.2 + planner_consensus * 0.15);
        (scaled
            * lag_penalty.sqrt()
            * failure_penalty
            * health_factor.clamp(0.85, 1.05)
            * planner_pressure_scale)
            .clamp(0.25, 1.4)
    };

    TrainingScheduleHint {
        budget_scale,
        microshard_scale,
    }
}

fn filter_training_assignment_peers(
    peers: &[PeerId],
    snapshot: &NodeTelemetrySnapshot,
    histories: &BTreeMap<PeerId, TrainingPlacementHistory>,
    planner_histories: &BTreeMap<PeerId, FleetPlacementPlannerHistory>,
    local_peer_id: &PeerId,
) -> Vec<PeerId> {
    peers
        .iter()
        .filter(|peer_id| {
            if *peer_id == local_peer_id {
                return true;
            }
            if planner_prunes_training_assignment(planner_histories.get(*peer_id)) {
                return false;
            }
            histories
                .get(*peer_id)
                .map(|history| {
                    history.weighted_head_lag
                        <= snapshot.lag_policy.max_head_lag_before_block as f64
                        && !(history.latest_status != PeerWindowStatus::Completed
                            && history.completed_window_count == 0)
                        && history.recent_failure_streak < TRAINING_PLACEMENT_FAILURE_STREAK_PRUNE
                        && !(history.recent_window_count
                            >= TRAINING_PLACEMENT_MIN_WINDOWS_FOR_HEALTH_PRUNE
                            && history.weighted_health < TRAINING_PLACEMENT_MIN_WEIGHTED_HEALTH)
                })
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}

fn recent_verified_fleet_placement_snapshots(
    snapshot: &NodeTelemetrySnapshot,
) -> Vec<&FleetPlacementSnapshot> {
    let now = Utc::now();
    let freshness_cutoff = now - chrono::Duration::seconds(FLEET_PLACEMENT_HISTORY_HORIZON_SECS);
    let mut placements = snapshot
        .control_plane
        .metrics_announcements
        .iter()
        .filter_map(|announcement| announcement.placement_snapshot.as_ref())
        .filter(|placement| {
            !placement.selected_peer_ids.is_empty()
                && !placement.signature_bundle.is_empty()
                && placement.generated_at >= freshness_cutoff
                && fleet_placement_snapshot_is_verified(snapshot, placement)
        })
        .collect::<Vec<_>>();
    placements.sort_by_key(|placement| std::cmp::Reverse(placement.generated_at));
    placements.truncate(FLEET_PLACEMENT_HISTORY_MAX_SNAPSHOTS);
    placements
}

fn latest_planner_public_key_hex<'a>(
    snapshot: &'a NodeTelemetrySnapshot,
    peer_id: &PeerId,
) -> Option<&'a str> {
    snapshot
        .control_plane
        .auth_announcements
        .iter()
        .filter(|announcement| &announcement.peer_id == peer_id)
        .max_by(|left, right| left.announced_at.cmp(&right.announced_at))
        .map(|announcement| {
            announcement
                .envelope
                .certificate
                .claims()
                .peer_public_key_hex
                .as_str()
        })
}

fn fleet_placement_snapshot_is_verified(
    snapshot: &NodeTelemetrySnapshot,
    placement: &FleetPlacementSnapshot,
) -> bool {
    let Some(public_key_hex) = latest_planner_public_key_hex(snapshot, &placement.planner_peer_id)
    else {
        return false;
    };
    let Ok(public_key_bytes) = hex::decode(public_key_hex) else {
        return false;
    };
    let Ok(public_key) = libp2p_identity::PublicKey::try_decode_protobuf(&public_key_bytes) else {
        return false;
    };
    if PeerId::new(libp2p_identity::PeerId::from_public_key(&public_key).to_string())
        != placement.planner_peer_id
    {
        return false;
    }
    let mut unsigned = placement.clone();
    unsigned.signature_bundle.clear();
    let Ok(message) = burn_p2p_core::deterministic_cbor(&unsigned) else {
        return false;
    };
    placement.signature_bundle.iter().any(|signature| {
        signature.signer == placement.planner_peer_id
            && signature.key_id == "runtime-fleet-placement"
            && matches!(signature.algorithm, SignatureAlgorithm::Ed25519)
            && hex::decode(&signature.signature_hex)
                .map(|raw| public_key.verify(&message, &raw))
                .unwrap_or(false)
    })
}

fn placement_snapshot_peer_rank(
    placement: &FleetPlacementSnapshot,
    peer_id: &PeerId,
) -> Option<usize> {
    placement
        .selected_peer_ids
        .iter()
        .position(|candidate| candidate == peer_id)
}

fn planner_history_visibility_rank(
    placement: &FleetPlacementSnapshot,
    peer_id: &PeerId,
) -> Option<usize> {
    if let Some(rank) = placement_snapshot_peer_rank(placement, peer_id) {
        return Some(rank);
    }
    placement
        .ranked_candidates
        .iter()
        .position(|candidate| &candidate.peer_id == peer_id)
        .map(|rank| placement.selected_peer_ids.len() + rank)
}

fn fleet_placement_planner_histories(
    snapshot: &NodeTelemetrySnapshot,
) -> BTreeMap<PeerId, FleetPlacementPlannerHistory> {
    let placements = recent_verified_fleet_placement_snapshots(snapshot);
    let mut planner_contributions =
        BTreeMap::<PeerId, BTreeMap<PeerId, FleetPlacementPlannerPeerContribution>>::new();

    for (index, placement) in placements.into_iter().enumerate() {
        let recency_weight = FLEET_PLACEMENT_HISTORY_DECAY_FACTOR.powi(index as i32);
        let planner_weight = recency_weight
            * (1.0 + peer_trust_score_value(snapshot, &placement.planner_peer_id).clamp(0.0, 1.0));

        for (rank, peer_id) in placement.selected_peer_ids.iter().enumerate() {
            let entry = planner_contributions
                .entry(peer_id.clone())
                .or_default()
                .entry(placement.planner_peer_id.clone())
                .or_default();
            entry.snapshot_count += 1;
            entry.visible_weight += planner_weight;
            entry.selected_weight += planner_weight;
            entry.weighted_rank += planner_weight * rank as f64;
            entry.oldest_generated_at = Some(
                entry
                    .oldest_generated_at
                    .map(|current| current.min(placement.generated_at))
                    .unwrap_or(placement.generated_at),
            );
            entry.latest_generated_at = Some(
                entry
                    .latest_generated_at
                    .map(|current| current.max(placement.generated_at))
                    .unwrap_or(placement.generated_at),
            );
        }

        for candidate in &placement.ranked_candidates {
            let rank = planner_history_visibility_rank(placement, &candidate.peer_id)
                .unwrap_or(placement.selected_peer_ids.len() + placement.ranked_candidates.len());
            let entry = planner_contributions
                .entry(candidate.peer_id.clone())
                .or_default()
                .entry(placement.planner_peer_id.clone())
                .or_default();
            if !placement
                .selected_peer_ids
                .iter()
                .any(|peer_id| peer_id == &candidate.peer_id)
            {
                entry.snapshot_count += 1;
                entry.visible_weight += planner_weight;
                entry.weighted_rank += planner_weight * rank as f64;
            }
            entry.weighted_health += planner_weight * candidate.weighted_health;
            entry.weighted_head_lag += planner_weight * candidate.weighted_head_lag;
            entry.weighted_throughput += planner_weight * candidate.weighted_throughput;
            entry.weighted_trust += planner_weight * candidate.trust_score;
            entry.weighted_budget_scale += planner_weight * candidate.recommended_budget_scale;
            entry.weighted_microshard_scale +=
                planner_weight * candidate.recommended_microshard_scale;
            entry.oldest_generated_at = Some(
                entry
                    .oldest_generated_at
                    .map(|current| current.min(placement.generated_at))
                    .unwrap_or(placement.generated_at),
            );
            entry.latest_generated_at = Some(
                entry
                    .latest_generated_at
                    .map(|current| current.max(placement.generated_at))
                    .unwrap_or(placement.generated_at),
            );
        }
    }

    planner_contributions
        .into_iter()
        .map(|(peer_id, planners)| {
            let snapshot_count = planners
                .values()
                .map(|contribution| contribution.snapshot_count)
                .sum();
            let distinct_planner_count = planners.len();
            let selected_planner_count = planners
                .values()
                .filter(|contribution| contribution.selected_weight > f64::EPSILON)
                .count();
            let visible_weight = planners
                .values()
                .map(|contribution| contribution.visible_weight)
                .sum();
            let selected_weight = planners
                .values()
                .map(|contribution| contribution.selected_weight)
                .sum();
            let weighted_rank = planners
                .values()
                .map(|contribution| contribution.weighted_rank)
                .sum();
            let weighted_health = planners
                .values()
                .map(|contribution| contribution.weighted_health)
                .sum();
            let weighted_head_lag = planners
                .values()
                .map(|contribution| contribution.weighted_head_lag)
                .sum();
            let weighted_throughput = planners
                .values()
                .map(|contribution| contribution.weighted_throughput)
                .sum();
            let weighted_trust = planners
                .values()
                .map(|contribution| contribution.weighted_trust)
                .sum();
            let weighted_budget_scale = planners
                .values()
                .map(|contribution| contribution.weighted_budget_scale)
                .sum();
            let weighted_microshard_scale = planners
                .values()
                .map(|contribution| contribution.weighted_microshard_scale)
                .sum();
            let oldest_generated_at = planners
                .values()
                .filter_map(|contribution| contribution.oldest_generated_at)
                .min()
                .unwrap_or_else(Utc::now);
            let latest_generated_at = planners
                .values()
                .filter_map(|contribution| contribution.latest_generated_at)
                .max()
                .unwrap_or_else(Utc::now);
            (
                peer_id,
                FleetPlacementPlannerHistory {
                    snapshot_count,
                    distinct_planner_count,
                    selected_planner_count,
                    visible_weight,
                    selected_weight,
                    weighted_rank,
                    weighted_health,
                    weighted_head_lag,
                    weighted_throughput,
                    weighted_trust,
                    weighted_budget_scale,
                    weighted_microshard_scale,
                    oldest_generated_at,
                    latest_generated_at,
                },
            )
        })
        .collect()
}

fn planner_support_ratio(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| {
            if history.visible_weight <= f64::EPSILON {
                0.0
            } else {
                history.selected_weight / history.visible_weight
            }
        })
        .unwrap_or(0.0)
}

fn planner_consensus_ratio(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| {
            if history.distinct_planner_count == 0 {
                0.0
            } else {
                history.selected_planner_count as f64 / history.distinct_planner_count as f64
            }
        })
        .unwrap_or(0.0)
}

fn planner_history_horizon_secs(history: Option<&FleetPlacementPlannerHistory>) -> i64 {
    history
        .map(|history| {
            history
                .latest_generated_at
                .signed_duration_since(history.oldest_generated_at)
                .num_seconds()
                .max(0)
        })
        .unwrap_or(0)
}

fn planner_scaling_confidence(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    let Some(history) = history else {
        return 0.0;
    };
    let support = planner_support_ratio(Some(history));
    let consensus = planner_consensus_ratio(Some(history));
    let horizon_factor = (planner_history_horizon_secs(Some(history)) as f64
        / FLEET_PLACEMENT_HISTORY_HORIZON_SECS as f64)
        .clamp(0.25, 1.0);

    if history.distinct_planner_count >= FLEET_PLACEMENT_MIN_DISTINCT_PLANNERS_FOR_CONSENSUS {
        (0.35 + support * 0.3 + consensus * 0.35)
            .mul_add(horizon_factor, 0.0)
            .clamp(0.45, 1.0)
    } else {
        (0.15 + support * 0.2)
            .mul_add(horizon_factor, 0.0)
            .clamp(0.1, 0.35)
    }
}

fn planner_distinct_count(history: Option<&FleetPlacementPlannerHistory>) -> usize {
    history
        .map(|history| history.distinct_planner_count)
        .unwrap_or(0)
}

fn planner_average_rank(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_rank / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(f64::MAX)
}

fn planner_average_health(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_health / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(0.0)
}

fn planner_average_head_lag(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_head_lag / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(f64::MAX)
}

fn planner_average_throughput(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_throughput / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(0.0)
}

fn planner_average_trust(history: Option<&FleetPlacementPlannerHistory>) -> f64 {
    history
        .map(|history| history.weighted_trust / history.visible_weight.max(f64::EPSILON))
        .unwrap_or(0.0)
}

fn fleet_placement_peer_summary(
    peer_id: &PeerId,
    histories: &BTreeMap<PeerId, TrainingPlacementHistory>,
    hints: &BTreeMap<PeerId, Vec<PeerWindowPlacementHint>>,
    trust_score: f64,
    max_weighted_throughput: f64,
) -> Option<FleetPlacementPeer> {
    let history = histories.get(peer_id)?;
    let latest_hint = hints.get(peer_id).and_then(|values| values.first())?;
    let normalized_throughput = if max_weighted_throughput > f64::EPSILON {
        (history.weighted_throughput / max_weighted_throughput).clamp(0.0, 1.5)
    } else {
        1.0
    };
    let lag_penalty = (1.0 / (1.0 + history.weighted_head_lag * 0.25)).clamp(0.4, 1.0);
    let failure_penalty =
        (1.0 / (1.0 + history.recent_failure_streak as f64 * 0.5)).clamp(0.45, 1.0);
    let completion_bias = if history.latest_status == PeerWindowStatus::Completed {
        1.0
    } else {
        0.75
    };
    let recommended_budget_scale = ((0.45
        + history.weighted_health.clamp(0.0, 1.1) * 0.55
        + normalized_throughput * 0.55
        + trust_score.clamp(0.0, 1.0) * 0.15)
        * lag_penalty
        * failure_penalty
        * completion_bias)
        .clamp(0.35, 1.75);
    let recommended_microshard_scale = ((0.45
        + history.weighted_health.clamp(0.0, 1.1) * 0.6
        + normalized_throughput * 0.35
        + trust_score.clamp(0.0, 1.0) * 0.1)
        * lag_penalty.sqrt()
        * failure_penalty
        * completion_bias)
        .clamp(0.35, 1.5);
    Some(FleetPlacementPeer {
        peer_id: peer_id.clone(),
        role: latest_hint.role.clone(),
        backend_class: latest_hint.backend_class.clone(),
        recent_window_count: history.recent_window_count,
        completed_window_count: history.completed_window_count,
        recent_failure_streak: history.recent_failure_streak,
        latest_status: history.latest_status.clone(),
        latest_finished_at: history.latest_finished_at,
        weighted_health: history.weighted_health,
        weighted_head_lag: history.weighted_head_lag,
        weighted_throughput: history.weighted_throughput,
        trust_score,
        recommended_budget_scale,
        recommended_microshard_scale,
    })
}

pub(crate) fn build_fleet_placement_snapshot(
    snapshot: &NodeTelemetrySnapshot,
    local_roles: &PeerRoleSet,
    local_peer_id: &PeerId,
    latest_hints: &[PeerWindowPlacementHint],
) -> Option<FleetPlacementSnapshot> {
    let network_id = snapshot.network_id.clone()?;
    let peers = runtime_training_peers(snapshot, local_roles, local_peer_id);
    if peers.is_empty() {
        return None;
    }

    let hint_history = recent_training_peer_window_histories(snapshot, latest_hints);
    let histories = hint_history
        .iter()
        .filter_map(|(peer_id, hints)| {
            summarize_training_peer_window_history(hints).map(|history| (peer_id.clone(), history))
        })
        .collect::<BTreeMap<_, _>>();
    let planner_histories = fleet_placement_planner_histories(snapshot);
    let max_weighted_throughput = histories
        .values()
        .map(|history| history.weighted_throughput)
        .fold(0.0, f64::max);

    let mut selected = filter_training_assignment_peers(
        &peers,
        snapshot,
        &histories,
        &planner_histories,
        local_peer_id,
    );
    if selected.is_empty() {
        selected = peers.clone();
    }
    selected.sort_by(|left, right| {
        compare_training_assignment_peer(snapshot, &histories, &planner_histories, left, right)
    });

    let mut ranked_peer_ids = peers;
    ranked_peer_ids.sort_by(|left, right| {
        compare_training_assignment_peer(snapshot, &histories, &planner_histories, left, right)
    });
    let ranked_candidates = ranked_peer_ids
        .into_iter()
        .filter_map(|peer_id| {
            fleet_placement_peer_summary(
                &peer_id,
                &histories,
                &hint_history,
                peer_trust_score_value(snapshot, &peer_id),
                max_weighted_throughput,
            )
        })
        .collect::<Vec<_>>();

    Some(FleetPlacementSnapshot {
        network_id,
        planner_peer_id: local_peer_id.clone(),
        generated_at: Utc::now(),
        freshness_secs: FLEET_PLACEMENT_SNAPSHOT_FRESHNESS_SECS,
        retained_hint_windows: TRAINING_PLACEMENT_HISTORY_WINDOWS,
        selected_peer_ids: selected,
        ranked_candidates,
        planner_version: FLEET_PLACEMENT_PLANNER_VERSION.into(),
        signature_bundle: Vec::new(),
    })
}

pub(crate) fn sign_fleet_placement_snapshot(
    keypair: &Keypair,
    snapshot: &FleetPlacementSnapshot,
) -> anyhow::Result<SignatureMetadata> {
    let mut unsigned = snapshot.clone();
    unsigned.signature_bundle.clear();
    let message = burn_p2p_core::deterministic_cbor(&unsigned)?;
    let signature = keypair
        .sign(&message)
        .map_err(|error| anyhow::anyhow!("failed to sign placement snapshot: {error}"))?;
    Ok(SignatureMetadata {
        signer: PeerId::new(
            libp2p_identity::PeerId::from_public_key(&keypair.public()).to_string(),
        ),
        key_id: "runtime-fleet-placement".into(),
        algorithm: SignatureAlgorithm::Ed25519,
        signed_at: Utc::now(),
        signature_hex: hex::encode(signature),
    })
}

pub(crate) fn runtime_training_assignment_peers(
    snapshot: &NodeTelemetrySnapshot,
    local_roles: &PeerRoleSet,
    local_peer_id: &PeerId,
) -> Vec<PeerId> {
    let peers = runtime_training_peers(snapshot, local_roles, local_peer_id);
    if peers.len() <= 1 {
        return peers;
    }

    let histories = training_assignment_histories(snapshot, &[]);
    let planner_histories = fleet_placement_planner_histories(snapshot);
    let mut selected = filter_training_assignment_peers(
        &peers,
        snapshot,
        &histories,
        &planner_histories,
        local_peer_id,
    );

    if selected.is_empty() {
        selected = peers;
    }

    selected.sort_by(|left, right| {
        compare_training_assignment_peer(snapshot, &histories, &planner_histories, left, right)
    });
    selected
}
