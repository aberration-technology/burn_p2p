use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    AggregateEnvelope, AggregateStats, AggregateTier, ArtifactId, ContentId, ContributionReceiptId,
    ExperimentId, HeadId, MergePolicy, MergeStrategy, MergeTopologyPolicy, MergeWindowState,
    NetworkId, PeerId, ReducerAssignment, ReducerLoadReport, ReductionCertificate, RevisionId,
    StudyId, UpdateAnnounce, UpdateNormStats, WindowId,
};
use burn_p2p_experiment::{
    ExperimentSpec, PatchSupport, RevisionCompatibility, RevisionSpec, assign_reducers,
    open_merge_window,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Enumerates the supported bandwidth class values.
pub enum BandwidthClass {
    /// Uses the slow variant.
    Slow,
    /// Uses the medium variant.
    Medium,
    /// Uses the fast variant.
    Fast,
}

impl BandwidthClass {
    fn mbps(self) -> f64 {
        match self {
            Self::Slow => 25.0,
            Self::Medium => 100.0,
            Self::Fast => 400.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configures merge topology sim.
pub struct MergeTopologySimConfig {
    /// The network ID.
    pub network_id: NetworkId,
    /// The study ID.
    pub study_id: StudyId,
    /// The experiment ID.
    pub experiment_id: ExperimentId,
    /// The revision ID.
    pub revision_id: RevisionId,
    /// The peer count.
    pub peer_count: u32,
    /// The reducer pool size.
    pub reducer_pool_size: u16,
    /// The validator count.
    pub validator_count: u16,
    /// The delta bytes.
    pub delta_bytes: u64,
    /// The chunk size bytes.
    pub chunk_size_bytes: u64,
    /// The simultaneous finish fraction.
    pub simultaneous_finish_fraction: f64,
    /// The malicious peer fraction.
    pub malicious_peer_fraction: f64,
    /// The churn rate.
    pub churn_rate: f64,
    /// The cache hit rate.
    pub cache_hit_rate: f64,
    /// The merge policy.
    pub merge_policy: MergePolicy,
    /// The topology policy.
    pub topology_policy: MergeTopologyPolicy,
}

impl Default for MergeTopologySimConfig {
    fn default() -> Self {
        Self {
            network_id: NetworkId::new("merge-sim"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new("experiment"),
            revision_id: RevisionId::new("revision"),
            peer_count: 64,
            reducer_pool_size: 8,
            validator_count: 2,
            delta_bytes: 256 * 1024 * 1024,
            chunk_size_bytes: 4 * 1024 * 1024,
            simultaneous_finish_fraction: 0.6,
            malicious_peer_fraction: 0.05,
            churn_rate: 0.05,
            cache_hit_rate: 0.2,
            merge_policy: MergePolicy::WeightedMean,
            topology_policy: MergeTopologyPolicy::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a merge topology metrics.
pub struct MergeTopologyMetrics {
    /// The total bytes sent.
    pub total_bytes_sent: u128,
    /// The busiest peer bytes.
    pub busiest_peer_bytes: u128,
    /// The p50 merge completion ms.
    pub p50_merge_completion_ms: u64,
    /// The p95 merge completion ms.
    pub p95_merge_completion_ms: u64,
    /// The p99 merge completion ms.
    pub p99_merge_completion_ms: u64,
    /// The certified head latency ms.
    pub certified_head_latency_ms: u64,
    /// The head diffusion p50 ms.
    pub head_diffusion_p50_ms: u64,
    /// The head diffusion p90 ms.
    pub head_diffusion_p90_ms: u64,
    /// The head diffusion p99 ms.
    pub head_diffusion_p99_ms: u64,
    /// The duplicate transfer ratio.
    pub duplicate_transfer_ratio: f64,
    /// The raw update drops.
    pub raw_update_drops: u32,
    /// The late arrivals.
    pub late_arrivals: u32,
    /// The reducer overload rate.
    pub reducer_overload_rate: f64,
    /// The validator backlog depth.
    pub validator_backlog_depth: u32,
    /// The accepted sample coverage.
    pub accepted_sample_coverage: f64,
    /// The update staleness ms.
    pub update_staleness_ms: u64,
    /// The merge skew.
    pub merge_skew: f64,
    /// The malicious acceptance rate.
    pub malicious_acceptance_rate: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Represents a merge topology simulation.
pub struct MergeTopologySimulation {
    /// The strategy.
    pub strategy: MergeStrategy,
    /// The config.
    pub config: MergeTopologySimConfig,
    /// The merge window.
    pub merge_window: MergeWindowState,
    /// The updates.
    pub updates: Vec<UpdateAnnounce>,
    /// The assignments.
    pub assignments: Vec<ReducerAssignment>,
    /// The aggregates.
    pub aggregates: Vec<AggregateEnvelope>,
    /// The reduction certificate.
    pub reduction_certificate: ReductionCertificate,
    /// The reducer load.
    pub reducer_load: Vec<ReducerLoadReport>,
    /// The metrics.
    pub metrics: MergeTopologyMetrics,
}

#[derive(Clone, Debug, PartialEq)]
struct SimulatedPeer {
    peer_id: PeerId,
    finish_ms: u64,
    bandwidth: BandwidthClass,
    sample_weight: f64,
    quality_weight: f64,
    delta_value: f64,
    malicious: bool,
    churned: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct AggregateAccumulator {
    weighted_delta_sum: f64,
    weight_sum: f64,
    quality_sum: f64,
    accepted_updates: u32,
    duplicate_updates: u32,
    late_updates: u32,
    max_norm: f64,
}

impl AggregateAccumulator {
    fn push(&mut self, delta_value: f64, sample_weight: f64, quality_weight: f64, norm: f64) {
        self.weighted_delta_sum += delta_value * sample_weight * quality_weight;
        self.weight_sum += sample_weight;
        self.quality_sum += quality_weight;
        self.accepted_updates += 1;
        self.max_norm = self.max_norm.max(norm);
    }

    fn combine(&mut self, other: &Self) {
        self.weighted_delta_sum += other.weighted_delta_sum;
        self.weight_sum += other.weight_sum;
        self.quality_sum += other.quality_sum;
        self.accepted_updates += other.accepted_updates;
        self.duplicate_updates += other.duplicate_updates;
        self.late_updates += other.late_updates;
        self.max_norm = self.max_norm.max(other.max_norm);
    }
}

fn hash_fraction(parts: impl Serialize) -> f64 {
    let hash = ContentId::derive(&parts).expect("hash");
    let hex = hash.as_str();
    let prefix_len = hex.len().min(16);
    let prefix = &hex[..prefix_len];
    let value = u64::from_str_radix(prefix, 16).unwrap_or(0);
    (value as f64) / (u64::MAX as f64)
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let clamped = q.clamp(0.0, 1.0);
    let index = ((sorted.len() - 1) as f64 * clamped).round() as usize;
    sorted[index]
}

fn transfer_ms(bytes: u64, bandwidth: BandwidthClass) -> u64 {
    let bits = (bytes as f64) * 8.0;
    let seconds = bits / (bandwidth.mbps() * 1_000_000.0);
    (seconds * 1000.0).ceil() as u64 + 5
}

fn validator_overhead_ms(update_count: usize) -> u64 {
    10 + (update_count as u64 / 2)
}

fn build_simulated_peers(config: &MergeTopologySimConfig) -> Vec<SimulatedPeer> {
    let simultaneous_count = ((config.peer_count as f64)
        * config.simultaneous_finish_fraction.clamp(0.0, 1.0))
    .round() as u32;
    let malicious_count = ((config.peer_count as f64)
        * config.malicious_peer_fraction.clamp(0.0, 1.0))
    .round() as u32;
    let churn_count =
        ((config.peer_count as f64) * config.churn_rate.clamp(0.0, 1.0)).round() as u32;

    (0..config.peer_count)
        .map(|index| {
            let peer_id = PeerId::new(format!("peer-{index:04}"));
            let finish_ms = if index < simultaneous_count {
                (config.topology_policy.window_duration_secs as u64 * 1000).saturating_sub(
                    (hash_fraction((index, "jitter"))
                        * f64::from(config.topology_policy.publish_jitter_ms))
                        as u64,
                )
            } else {
                (hash_fraction((index, "finish"))
                    * ((config.topology_policy.window_duration_secs as u64 * 1000) as f64))
                    as u64
            };
            let bandwidth = match index % 3 {
                0 => BandwidthClass::Slow,
                1 => BandwidthClass::Medium,
                _ => BandwidthClass::Fast,
            };
            let sample_weight = 32.0 + ((index % 11) as f64);
            let quality_weight = if index < malicious_count {
                0.2
            } else {
                0.8 + ((index % 5) as f64) / 10.0
            };
            let delta_value = if index + 1 == config.peer_count {
                10.0
            } else {
                ((index + 1) as f64) / ((config.peer_count + 1) as f64)
            };

            SimulatedPeer {
                peer_id,
                finish_ms,
                bandwidth,
                sample_weight,
                quality_weight,
                delta_value,
                malicious: index < malicious_count,
                churned: index >= malicious_count && index < malicious_count + churn_count,
            }
        })
        .collect()
}

fn build_update_announces(
    config: &MergeTopologySimConfig,
    peers: &[SimulatedPeer],
    merge_window: &MergeWindowState,
) -> Vec<UpdateAnnounce> {
    peers
        .iter()
        .filter(|peer| !peer.churned)
        .map(|peer| UpdateAnnounce {
            peer_id: peer.peer_id.clone(),
            study_id: config.study_id.clone(),
            experiment_id: config.experiment_id.clone(),
            revision_id: config.revision_id.clone(),
            window_id: merge_window.window_id,
            base_head_id: merge_window.base_head_id.clone(),
            delta_artifact_id: ArtifactId::new(format!(
                "delta-{}-{}",
                merge_window.window_id.0,
                peer.peer_id.as_str()
            )),
            sample_weight: peer.sample_weight,
            quality_weight: peer.quality_weight,
            norm_stats: UpdateNormStats {
                l2_norm: peer.delta_value.abs(),
                max_abs: peer.delta_value.abs(),
                clipped: false,
                non_finite_tensors: 0,
            },
            receipt_root: ContentId::derive(&(peer.peer_id.as_str(), "receipt-root"))
                .expect("receipt root"),
            receipt_ids: vec![ContributionReceiptId::new(format!(
                "receipt-{}",
                peer.peer_id.as_str()
            ))],
            providers: vec![peer.peer_id.clone()],
            announced_at: Utc::now(),
        })
        .collect()
}

fn record_transfer(io: &mut BTreeMap<PeerId, u128>, from: &PeerId, to: &PeerId, bytes: u64) {
    *io.entry(from.clone()).or_default() += u128::from(bytes);
    *io.entry(to.clone()).or_default() += u128::from(bytes);
}

fn reducer_pool(config: &MergeTopologySimConfig, peers: &[SimulatedPeer]) -> Vec<PeerId> {
    peers
        .iter()
        .take(config.reducer_pool_size.max(1) as usize)
        .map(|peer| peer.peer_id.clone())
        .collect()
}

fn validator_pool(config: &MergeTopologySimConfig, reducers: &[PeerId]) -> Vec<PeerId> {
    reducers
        .iter()
        .take(config.validator_count.max(1) as usize)
        .cloned()
        .collect()
}

fn make_experiment_and_revision(config: &MergeTopologySimConfig) -> (ExperimentSpec, RevisionSpec) {
    let mut experiment = ExperimentSpec::new(
        config.study_id.clone(),
        config.experiment_id.as_str(),
        burn_p2p_core::DatasetViewId::new("dataset-view"),
        ContentId::new("schema-hash"),
        config.merge_policy.clone(),
    )
    .expect("experiment")
    .with_merge_topology(config.topology_policy.clone());
    experiment.experiment_id = config.experiment_id.clone();
    experiment.study_id = config.study_id.clone();
    let revision = RevisionSpec::new(
        config.experiment_id.clone(),
        None,
        ContentId::new("project-hash"),
        ContentId::new("config-hash"),
        RevisionCompatibility {
            model_schema_hash: ContentId::new("schema-hash"),
            dataset_view_id: burn_p2p_core::DatasetViewId::new("dataset-view"),
            required_client_capabilities: BTreeSet::new(),
        },
        PatchSupport::default(),
        Utc::now(),
    )
    .expect("revision");

    (experiment, revision)
}

fn combine_balanced(mut accumulators: Vec<AggregateAccumulator>) -> AggregateAccumulator {
    while accumulators.len() > 1 {
        let mut next = Vec::new();
        for pair in accumulators.chunks(2) {
            let mut combined = pair[0].clone();
            if let Some(right) = pair.get(1) {
                combined.combine(right);
            }
            next.push(combined);
        }
        accumulators = next;
    }
    accumulators.pop().unwrap_or(AggregateAccumulator {
        weighted_delta_sum: 0.0,
        weight_sum: 0.0,
        quality_sum: 0.0,
        accepted_updates: 0,
        duplicate_updates: 0,
        late_updates: 0,
        max_norm: 0.0,
    })
}

struct MetricsInput<'a> {
    certified_head_latency_ms: u64,
    peer_completion_times: Vec<u64>,
    io: &'a BTreeMap<PeerId, u128>,
    total_bytes_sent: u128,
    duplicate_transfer_ratio: f64,
    raw_update_drops: u32,
    late_arrivals: u32,
    reducer_load: &'a [ReducerLoadReport],
    accepted_sample_coverage: f64,
    update_staleness_ms: u64,
    merge_skew: f64,
    malicious_acceptance_rate: f64,
    peer_count: usize,
}

fn make_metrics(input: MetricsInput<'_>) -> MergeTopologyMetrics {
    let mut completion_times = input.peer_completion_times;
    completion_times.sort_unstable();
    let busiest_peer_bytes = input.io.values().copied().max().unwrap_or(0);
    let overload_count = input
        .reducer_load
        .iter()
        .filter(|report| report.overload_ratio > 1.0)
        .count();
    let reducer_overload_rate = if input.reducer_load.is_empty() {
        0.0
    } else {
        overload_count as f64 / input.reducer_load.len() as f64
    };
    let diffusion_base = input.certified_head_latency_ms + 25;

    MergeTopologyMetrics {
        total_bytes_sent: input.total_bytes_sent,
        busiest_peer_bytes,
        p50_merge_completion_ms: percentile(&completion_times, 0.50),
        p95_merge_completion_ms: percentile(&completion_times, 0.95),
        p99_merge_completion_ms: percentile(&completion_times, 0.99),
        certified_head_latency_ms: input.certified_head_latency_ms,
        head_diffusion_p50_ms: diffusion_base + ((input.peer_count as f64).sqrt() * 10.0) as u64,
        head_diffusion_p90_ms: diffusion_base + ((input.peer_count as f64).ln_1p() * 80.0) as u64,
        head_diffusion_p99_ms: diffusion_base + ((input.peer_count as f64).ln_1p() * 140.0) as u64,
        duplicate_transfer_ratio: input.duplicate_transfer_ratio,
        raw_update_drops: input.raw_update_drops,
        late_arrivals: input.late_arrivals,
        reducer_overload_rate,
        validator_backlog_depth: input
            .reducer_load
            .iter()
            .map(|report| report.assigned_aggregate_inputs)
            .sum(),
        accepted_sample_coverage: input.accepted_sample_coverage,
        update_staleness_ms: input.update_staleness_ms,
        merge_skew: input.merge_skew,
        malicious_acceptance_rate: input.malicious_acceptance_rate,
    }
}

#[derive(Default)]
struct StrategySimulation {
    io: BTreeMap<PeerId, u128>,
    assignments: Vec<ReducerAssignment>,
    aggregates: Vec<AggregateEnvelope>,
    reducer_load: Vec<ReducerLoadReport>,
    certified_head_latency_ms: u64,
    peer_completion_times: Vec<u64>,
    duplicate_transfer_ratio: f64,
    merge_skew: f64,
}

fn empty_accumulator() -> AggregateAccumulator {
    AggregateAccumulator {
        weighted_delta_sum: 0.0,
        weight_sum: 0.0,
        quality_sum: 0.0,
        accepted_updates: 0,
        duplicate_updates: 0,
        late_updates: 0,
        max_norm: 0.0,
    }
}

fn simulate_global_broadcast(
    config: &MergeTopologySimConfig,
    validators: &[PeerId],
    accepted_peers: &[&SimulatedPeer],
    active_updates: usize,
) -> StrategySimulation {
    let validator = validators[0].clone();
    let mut outcome = StrategySimulation::default();

    for peer in accepted_peers {
        let arrival = peer.finish_ms + transfer_ms(config.delta_bytes, peer.bandwidth);
        outcome.certified_head_latency_ms = outcome.certified_head_latency_ms.max(arrival);
        outcome.peer_completion_times.push(arrival);
        for other in accepted_peers {
            if other.peer_id != peer.peer_id {
                record_transfer(
                    &mut outcome.io,
                    &peer.peer_id,
                    &other.peer_id,
                    config.delta_bytes,
                );
            }
        }
        record_transfer(
            &mut outcome.io,
            &peer.peer_id,
            &validator,
            config.delta_bytes,
        );
    }
    outcome.certified_head_latency_ms += validator_overhead_ms(active_updates);
    outcome.duplicate_transfer_ratio = ((active_updates * active_updates.saturating_sub(1)) as f64
        / active_updates as f64)
        .max(1.0);
    outcome
}

fn simulate_central_reducer(
    config: &MergeTopologySimConfig,
    merge_window: &MergeWindowState,
    reducers: &[PeerId],
    validators: &[PeerId],
    accepted_peers: &[&SimulatedPeer],
    active_updates: usize,
    raw_update_drops: u32,
) -> StrategySimulation {
    let reducer = reducers[0].clone();
    let mut outcome = StrategySimulation::default();
    let mut reducer_acc = empty_accumulator();

    for peer in accepted_peers {
        let arrival = peer.finish_ms + transfer_ms(config.delta_bytes, peer.bandwidth);
        outcome.certified_head_latency_ms = outcome.certified_head_latency_ms.max(arrival);
        outcome.peer_completion_times.push(arrival);
        record_transfer(&mut outcome.io, &peer.peer_id, &reducer, config.delta_bytes);
        reducer_acc.push(
            peer.delta_value,
            peer.sample_weight,
            peer.quality_weight,
            peer.delta_value.abs(),
        );
    }

    outcome.certified_head_latency_ms += validator_overhead_ms(active_updates);
    outcome.reducer_load.push(ReducerLoadReport {
        peer_id: reducer.clone(),
        window_id: merge_window.window_id,
        assigned_leaf_updates: active_updates as u32,
        assigned_aggregate_inputs: active_updates as u32,
        ingress_bytes: u128::from(config.delta_bytes) * active_updates as u128,
        egress_bytes: u128::from(config.delta_bytes),
        duplicate_transfer_ratio: 0.0,
        overload_ratio: active_updates as f64 / 8.0,
        reported_at: Utc::now(),
    });
    outcome.aggregates.push(AggregateEnvelope {
        aggregate_id: ContentId::derive(&("central", merge_window.window_id.0)).expect("aggregate"),
        study_id: config.study_id.clone(),
        experiment_id: config.experiment_id.clone(),
        revision_id: config.revision_id.clone(),
        window_id: merge_window.window_id,
        base_head_id: merge_window.base_head_id.clone(),
        aggregate_artifact_id: ArtifactId::new("aggregate-central"),
        tier: AggregateTier::RootCandidate,
        reducer_peer_id: reducer,
        contributor_peers: accepted_peers
            .iter()
            .map(|peer| peer.peer_id.clone())
            .collect(),
        child_aggregate_ids: Vec::new(),
        stats: AggregateStats {
            accepted_updates: reducer_acc.accepted_updates,
            duplicate_updates: 0,
            dropped_updates: raw_update_drops,
            late_updates: 0,
            sum_sample_weight: reducer_acc.weight_sum,
            sum_quality_weight: reducer_acc.quality_sum,
            sum_weighted_delta_norm: reducer_acc.weighted_delta_sum.abs(),
            max_update_norm: reducer_acc.max_norm,
            accepted_sample_coverage: 1.0,
        },
        providers: vec![validators[0].clone()],
        published_at: Utc::now(),
    });

    outcome
}

fn simulate_gossip(
    config: &MergeTopologySimConfig,
    validators: &[PeerId],
    accepted_peers: &[&SimulatedPeer],
    active_updates: usize,
    fanout: usize,
) -> StrategySimulation {
    let validator = validators[0].clone();
    let mut outcome = StrategySimulation::default();

    for peer in accepted_peers {
        let mut path_latency = peer.finish_ms;
        let recipients = accepted_peers
            .iter()
            .filter(|other| other.peer_id != peer.peer_id)
            .take(fanout)
            .map(|other| other.peer_id.clone())
            .collect::<Vec<_>>();
        for recipient in recipients {
            record_transfer(
                &mut outcome.io,
                &peer.peer_id,
                &recipient,
                config.delta_bytes,
            );
            path_latency += transfer_ms(config.delta_bytes, peer.bandwidth);
        }
        record_transfer(
            &mut outcome.io,
            &peer.peer_id,
            &validator,
            config.delta_bytes,
        );
        path_latency += transfer_ms(config.delta_bytes, peer.bandwidth);
        outcome.peer_completion_times.push(path_latency);
        outcome.certified_head_latency_ms = outcome.certified_head_latency_ms.max(path_latency);
    }

    outcome.certified_head_latency_ms += validator_overhead_ms(active_updates + fanout);
    outcome.duplicate_transfer_ratio = fanout as f64;
    outcome
}

fn simulate_tree(
    config: &MergeTopologySimConfig,
    merge_window: &MergeWindowState,
    validators: &[PeerId],
    accepted_peers: &[&SimulatedPeer],
    active_updates: usize,
    raw_update_drops: u32,
) -> StrategySimulation {
    let fanin = usize::from(config.topology_policy.upper_fanin.max(2));
    let mut outcome = StrategySimulation::default();
    let mut layer = accepted_peers
        .iter()
        .map(|peer| {
            outcome.peer_completion_times.push(peer.finish_ms);
            let mut acc = empty_accumulator();
            acc.push(
                peer.delta_value,
                peer.sample_weight,
                peer.quality_weight,
                peer.delta_value.abs(),
            );
            (
                peer.peer_id.clone(),
                peer.finish_ms,
                acc,
                vec![peer.peer_id.clone()],
            )
        })
        .collect::<Vec<_>>();

    let mut tier = 0;
    while layer.len() > 1 {
        let mut next = Vec::new();
        for chunk in layer.chunks(fanin) {
            let owner = chunk[0].0.clone();
            let mut ready_at = chunk[0].1;
            let mut combined = chunk[0].2.clone();
            let mut contributors = chunk[0].3.clone();
            for (peer_id, child_ready, acc, child_contributors) in &chunk[1..] {
                record_transfer(&mut outcome.io, peer_id, &owner, config.delta_bytes);
                ready_at = ready_at
                    .max(*child_ready + transfer_ms(config.delta_bytes, BandwidthClass::Medium));
                combined.combine(acc);
                contributors.extend(child_contributors.iter().cloned());
            }
            next.push((owner.clone(), ready_at + 10, combined, contributors.clone()));
            tier += 1;
        }
        layer = next;
    }

    let (reducer_peer_id, ready_at, acc, contributors) = layer.pop().expect("root layer");
    outcome.certified_head_latency_ms = ready_at + validator_overhead_ms(active_updates);
    outcome.aggregates.push(AggregateEnvelope {
        aggregate_id: ContentId::derive(&(reducer_peer_id.as_str(), "tree")).expect("agg"),
        study_id: config.study_id.clone(),
        experiment_id: config.experiment_id.clone(),
        revision_id: config.revision_id.clone(),
        window_id: merge_window.window_id,
        base_head_id: merge_window.base_head_id.clone(),
        aggregate_artifact_id: ArtifactId::new(format!("aggregate-tree-{tier}")),
        tier: AggregateTier::RootCandidate,
        reducer_peer_id: reducer_peer_id.clone(),
        contributor_peers: contributors,
        child_aggregate_ids: Vec::new(),
        stats: AggregateStats {
            accepted_updates: acc.accepted_updates,
            duplicate_updates: 0,
            dropped_updates: raw_update_drops,
            late_updates: 0,
            sum_sample_weight: acc.weight_sum,
            sum_quality_weight: acc.quality_sum,
            sum_weighted_delta_norm: acc.weighted_delta_sum.abs(),
            max_update_norm: acc.max_norm,
            accepted_sample_coverage: 1.0,
        },
        providers: vec![validators[0].clone()],
        published_at: Utc::now(),
    });
    outcome.merge_skew = tier as f64 / active_updates.max(1) as f64;
    outcome
}

fn simulate_replicated_topology(
    config: &MergeTopologySimConfig,
    merge_window: &MergeWindowState,
    reducers: &[PeerId],
    validators: &[PeerId],
    accepted_peers: &[&SimulatedPeer],
    active_updates: usize,
    raw_update_drops: u32,
) -> StrategySimulation {
    let mut outcome = StrategySimulation::default();
    let mut reducer_buckets = BTreeMap::<PeerId, Vec<&SimulatedPeer>>::new();

    for peer in accepted_peers {
        let assignment =
            assign_reducers(merge_window, &peer.peer_id, reducers).expect("reducer assignment");
        for reducer in &assignment.assigned_reducers {
            reducer_buckets
                .entry(reducer.clone())
                .or_default()
                .push(peer);
            record_transfer(&mut outcome.io, &peer.peer_id, reducer, config.delta_bytes);
        }
        outcome.assignments.push(assignment);
    }

    outcome.duplicate_transfer_ratio = config.topology_policy.reducer_replication as f64 - 1.0;
    let mut leaf_nodes = Vec::new();
    for (reducer, bucket) in &reducer_buckets {
        let mut ready_at = 0_u64;
        let mut accumulator = AggregateAccumulator {
            duplicate_updates: bucket.len().saturating_sub(
                bucket
                    .iter()
                    .map(|peer| peer.peer_id.clone())
                    .collect::<BTreeSet<_>>()
                    .len(),
            ) as u32,
            ..empty_accumulator()
        };
        for peer in bucket {
            let arrival = peer.finish_ms + transfer_ms(config.delta_bytes, peer.bandwidth);
            ready_at = ready_at.max(arrival);
            accumulator.push(
                peer.delta_value,
                peer.sample_weight,
                peer.quality_weight,
                peer.delta_value.abs(),
            );
        }
        let overload_ratio =
            bucket.len() as f64 / f64::from(config.topology_policy.target_leaf_cohort.max(1));
        outcome.reducer_load.push(ReducerLoadReport {
            peer_id: reducer.clone(),
            window_id: merge_window.window_id,
            assigned_leaf_updates: bucket.len() as u32,
            assigned_aggregate_inputs: 0,
            ingress_bytes: u128::from(config.delta_bytes) * bucket.len() as u128,
            egress_bytes: u128::from(config.delta_bytes),
            duplicate_transfer_ratio: outcome.duplicate_transfer_ratio,
            overload_ratio,
            reported_at: Utc::now(),
        });
        let aggregate_id = ContentId::derive(&(
            merge_window.merge_window_id.as_str(),
            reducer.as_str(),
            "leaf",
        ))
        .expect("aggregate id");
        outcome.aggregates.push(AggregateEnvelope {
            aggregate_id: aggregate_id.clone(),
            study_id: config.study_id.clone(),
            experiment_id: config.experiment_id.clone(),
            revision_id: config.revision_id.clone(),
            window_id: merge_window.window_id,
            base_head_id: merge_window.base_head_id.clone(),
            aggregate_artifact_id: ArtifactId::new(format!("aggregate-leaf-{}", reducer.as_str())),
            tier: AggregateTier::Leaf,
            reducer_peer_id: reducer.clone(),
            contributor_peers: bucket.iter().map(|peer| peer.peer_id.clone()).collect(),
            child_aggregate_ids: Vec::new(),
            stats: AggregateStats {
                accepted_updates: accumulator.accepted_updates,
                duplicate_updates: accumulator.duplicate_updates,
                dropped_updates: raw_update_drops,
                late_updates: 0,
                sum_sample_weight: accumulator.weight_sum,
                sum_quality_weight: accumulator.quality_sum,
                sum_weighted_delta_norm: accumulator.weighted_delta_sum.abs(),
                max_update_norm: accumulator.max_norm,
                accepted_sample_coverage: bucket.len() as f64 / active_updates.max(1) as f64,
            },
            providers: vec![reducer.clone()],
            published_at: Utc::now(),
        });
        leaf_nodes.push((reducer.clone(), ready_at + 10, accumulator, aggregate_id));
    }

    leaf_nodes.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
    let upper_fanin = usize::from(config.topology_policy.upper_fanin.max(1));
    let mut upper_nodes = Vec::new();
    for chunk in leaf_nodes.chunks(upper_fanin) {
        let owner = chunk[0].0.clone();
        let mut ready_at = chunk[0].1;
        let mut accumulator = chunk[0].2.clone();
        let mut child_ids = vec![chunk[0].3.clone()];
        for (reducer, child_ready, child_acc, aggregate_id) in &chunk[1..] {
            record_transfer(&mut outcome.io, reducer, &owner, config.delta_bytes);
            ready_at = ready_at
                .max(*child_ready + transfer_ms(config.delta_bytes, BandwidthClass::Medium));
            accumulator.combine(child_acc);
            child_ids.push(aggregate_id.clone());
        }
        upper_nodes.push((
            owner.clone(),
            ready_at + 10,
            accumulator.clone(),
            child_ids.clone(),
        ));
        outcome.aggregates.push(AggregateEnvelope {
            aggregate_id: ContentId::derive(&(owner.as_str(), "upper", &child_ids))
                .expect("upper id"),
            study_id: config.study_id.clone(),
            experiment_id: config.experiment_id.clone(),
            revision_id: config.revision_id.clone(),
            window_id: merge_window.window_id,
            base_head_id: merge_window.base_head_id.clone(),
            aggregate_artifact_id: ArtifactId::new(format!("aggregate-upper-{}", owner.as_str())),
            tier: if matches!(
                config.topology_policy.strategy,
                MergeStrategy::MicrocohortReducePlusValidatorPromotion
            ) {
                AggregateTier::RootCandidate
            } else {
                AggregateTier::Upper
            },
            reducer_peer_id: owner,
            contributor_peers: Vec::new(),
            child_aggregate_ids: child_ids,
            stats: AggregateStats {
                accepted_updates: accumulator.accepted_updates,
                duplicate_updates: accumulator.duplicate_updates,
                dropped_updates: raw_update_drops,
                late_updates: 0,
                sum_sample_weight: accumulator.weight_sum,
                sum_quality_weight: accumulator.quality_sum,
                sum_weighted_delta_norm: accumulator.weighted_delta_sum.abs(),
                max_update_norm: accumulator.max_norm,
                accepted_sample_coverage: accumulator.accepted_updates as f64
                    / active_updates.max(1) as f64,
            },
            providers: vec![validators[0].clone()],
            published_at: Utc::now(),
        });
    }

    let root_accumulator = combine_balanced(
        upper_nodes
            .iter()
            .map(|(_, _, acc, _)| acc.clone())
            .collect::<Vec<_>>(),
    );
    outcome.certified_head_latency_ms = upper_nodes
        .iter()
        .map(|(_, ready_at, _, _)| *ready_at)
        .max()
        .unwrap_or(0)
        + validator_overhead_ms(upper_nodes.len());
    outcome
        .peer_completion_times
        .extend(accepted_peers.iter().map(|peer| {
            outcome
                .certified_head_latency_ms
                .saturating_sub(peer.finish_ms)
        }));
    outcome.merge_skew = if reducer_buckets.is_empty() {
        0.0
    } else {
        let mean = active_updates as f64 / reducer_buckets.len() as f64;
        reducer_buckets
            .values()
            .map(|bucket| ((bucket.len() as f64) - mean).abs())
            .sum::<f64>()
            / reducer_buckets.len() as f64
    };
    let _ = root_accumulator;

    outcome
}

/// Performs the simulate merge topology operation.
pub fn simulate_merge_topology(
    config: MergeTopologySimConfig,
) -> Result<MergeTopologySimulation, burn_p2p_core::SchemaError> {
    let peers = build_simulated_peers(&config);
    let reducers = reducer_pool(&config, &peers);
    let validators = validator_pool(&config, &reducers);
    let (experiment, revision) = make_experiment_and_revision(&config);
    let merge_window = open_merge_window(
        &experiment,
        &revision,
        WindowId(1),
        HeadId::new("base-head"),
        reducers.clone(),
        validators.clone(),
    )?;
    let updates = build_update_announces(&config, &peers, &merge_window);

    let raw_update_drops = peers.iter().filter(|peer| peer.churned).count() as u32;
    let active_updates = updates.len();
    let accepted_peers = peers
        .iter()
        .filter(|peer| !peer.churned)
        .collect::<Vec<_>>();
    let malicious_accepted = accepted_peers.iter().filter(|peer| peer.malicious).count() as u32;
    let late_arrivals = 0_u32;
    let outcome = match config.topology_policy.strategy {
        MergeStrategy::GlobalBroadcastBaseline => {
            simulate_global_broadcast(&config, &validators, &accepted_peers, active_updates)
        }
        MergeStrategy::CentralReducerBaseline => simulate_central_reducer(
            &config,
            &merge_window,
            &reducers,
            &validators,
            &accepted_peers,
            active_updates,
            raw_update_drops,
        ),
        MergeStrategy::RandomPeerGossip => {
            simulate_gossip(&config, &validators, &accepted_peers, active_updates, 3)
        }
        MergeStrategy::KRegularGossip => {
            simulate_gossip(&config, &validators, &accepted_peers, active_updates, 4)
        }
        MergeStrategy::FixedTreeReduce | MergeStrategy::RotatingRendezvousTree => simulate_tree(
            &config,
            &merge_window,
            &validators,
            &accepted_peers,
            active_updates,
            raw_update_drops,
        ),
        MergeStrategy::ReplicatedRendezvousDag
        | MergeStrategy::LocalGossipPlusPeriodicGlobal
        | MergeStrategy::MicrocohortReducePlusValidatorPromotion => simulate_replicated_topology(
            &config,
            &merge_window,
            &reducers,
            &validators,
            &accepted_peers,
            active_updates,
            raw_update_drops,
        ),
    };

    let total_bytes_sent = outcome.io.values().copied().sum::<u128>() / 2;
    let accepted_sample_coverage = if config.peer_count == 0 {
        0.0
    } else {
        accepted_peers.len() as f64 / config.peer_count as f64
    };
    let update_staleness_ms = accepted_peers
        .iter()
        .map(|peer| {
            outcome
                .certified_head_latency_ms
                .saturating_sub(peer.finish_ms)
        })
        .max()
        .unwrap_or(0);
    let reduction_certificate = ReductionCertificate {
        reduction_id: ContentId::derive(&(
            merge_window.merge_window_id.as_str(),
            "reduction-certificate",
        ))?,
        study_id: config.study_id.clone(),
        experiment_id: config.experiment_id.clone(),
        revision_id: config.revision_id.clone(),
        window_id: merge_window.window_id,
        base_head_id: merge_window.base_head_id.clone(),
        aggregate_id: outcome
            .aggregates
            .last()
            .map(|aggregate| aggregate.aggregate_id.clone())
            .unwrap_or_else(|| ContentId::new("aggregate-root")),
        validator: validators[0].clone(),
        validator_quorum: config.topology_policy.promotion_policy.validator_quorum,
        cross_checked_reducers: reducers.iter().take(2).cloned().collect(),
        issued_at: Utc::now(),
    };
    let metrics = make_metrics(MetricsInput {
        certified_head_latency_ms: outcome.certified_head_latency_ms,
        peer_completion_times: if outcome.peer_completion_times.is_empty() {
            vec![0]
        } else {
            outcome.peer_completion_times.clone()
        },
        io: &outcome.io,
        total_bytes_sent,
        duplicate_transfer_ratio: outcome.duplicate_transfer_ratio,
        raw_update_drops,
        late_arrivals,
        reducer_load: &outcome.reducer_load,
        accepted_sample_coverage,
        update_staleness_ms,
        merge_skew: outcome.merge_skew,
        malicious_acceptance_rate: if accepted_peers.is_empty() {
            0.0
        } else {
            malicious_accepted as f64 / accepted_peers.len() as f64
        },
        peer_count: config.peer_count as usize,
    });

    Ok(MergeTopologySimulation {
        strategy: config.topology_policy.strategy.clone(),
        config,
        merge_window,
        updates,
        assignments: outcome.assignments,
        aggregates: outcome.aggregates,
        reduction_certificate,
        reducer_load: outcome.reducer_load,
        metrics,
    })
}

#[cfg(test)]
mod tests {
    use burn_p2p_core::{HeadPromotionPolicy, MergeStrategy, MergeTopologyPolicy};

    use super::{AggregateAccumulator, MergeTopologySimConfig, simulate_merge_topology};

    #[test]
    fn weighted_delta_accumulator_is_associative_across_tree_shapes() {
        let mut flat = AggregateAccumulator {
            weighted_delta_sum: 0.0,
            weight_sum: 0.0,
            quality_sum: 0.0,
            accepted_updates: 0,
            duplicate_updates: 0,
            late_updates: 0,
            max_norm: 0.0,
        };
        flat.push(1.0, 2.0, 1.0, 1.0);
        flat.push(3.0, 1.0, 1.0, 3.0);
        flat.push(-1.0, 4.0, 0.5, 1.0);

        let mut left = AggregateAccumulator {
            weighted_delta_sum: 0.0,
            weight_sum: 0.0,
            quality_sum: 0.0,
            accepted_updates: 0,
            duplicate_updates: 0,
            late_updates: 0,
            max_norm: 0.0,
        };
        left.push(1.0, 2.0, 1.0, 1.0);
        left.push(3.0, 1.0, 1.0, 3.0);

        let mut right = AggregateAccumulator {
            weighted_delta_sum: 0.0,
            weight_sum: 0.0,
            quality_sum: 0.0,
            accepted_updates: 0,
            duplicate_updates: 0,
            late_updates: 0,
            max_norm: 0.0,
        };
        right.push(-1.0, 4.0, 0.5, 1.0);
        left.combine(&right);

        assert_eq!(
            flat.weighted_delta_sum / flat.weight_sum,
            left.weighted_delta_sum / left.weight_sum
        );
        assert_eq!(flat.weight_sum, left.weight_sum);
    }

    #[test]
    fn nested_leaf_ema_differs_from_single_root_ema() {
        let mut first = AggregateAccumulator {
            weighted_delta_sum: 0.0,
            weight_sum: 0.0,
            quality_sum: 0.0,
            accepted_updates: 0,
            duplicate_updates: 0,
            late_updates: 0,
            max_norm: 0.0,
        };
        first.push(2.0, 1.0, 1.0, 2.0);
        let mut second = AggregateAccumulator {
            weighted_delta_sum: 0.0,
            weight_sum: 0.0,
            quality_sum: 0.0,
            accepted_updates: 0,
            duplicate_updates: 0,
            late_updates: 0,
            max_norm: 0.0,
        };
        second.push(6.0, 1.0, 1.0, 6.0);

        let mut root = first.clone();
        root.combine(&second);

        let decay = 0.9;
        let first_leaf_ema = (first.weighted_delta_sum / first.weight_sum) * (1.0 - decay);
        let second_leaf_ema = (second.weighted_delta_sum / second.weight_sum) * (1.0 - decay);
        let nested = (((first_leaf_ema * first.weight_sum)
            + (second_leaf_ema * second.weight_sum))
            / (first.weight_sum + second.weight_sum))
            * (1.0 - decay);
        let single_root = (root.weighted_delta_sum / root.weight_sum) * (1.0 - decay);

        assert_ne!(nested, single_root);
    }

    #[test]
    fn replicated_rendezvous_dag_beats_central_and_broadcast_baselines_on_load() {
        let central = simulate_merge_topology(MergeTopologySimConfig {
            topology_policy: MergeTopologyPolicy {
                strategy: MergeStrategy::CentralReducerBaseline,
                ..MergeTopologyPolicy::default()
            },
            ..MergeTopologySimConfig::default()
        })
        .expect("central");
        let broadcast = simulate_merge_topology(MergeTopologySimConfig {
            topology_policy: MergeTopologyPolicy {
                strategy: MergeStrategy::GlobalBroadcastBaseline,
                ..MergeTopologyPolicy::default()
            },
            ..MergeTopologySimConfig::default()
        })
        .expect("broadcast");
        let dag = simulate_merge_topology(MergeTopologySimConfig::default()).expect("dag");

        assert!(dag.metrics.busiest_peer_bytes < central.metrics.busiest_peer_bytes);
        assert!(dag.metrics.total_bytes_sent < broadcast.metrics.total_bytes_sent);
    }

    #[test]
    fn microcohort_validator_promotion_beats_random_gossip_on_certified_latency() {
        let gossip = simulate_merge_topology(MergeTopologySimConfig {
            topology_policy: MergeTopologyPolicy {
                strategy: MergeStrategy::RandomPeerGossip,
                ..MergeTopologyPolicy::default()
            },
            ..MergeTopologySimConfig::default()
        })
        .expect("gossip");
        let microcohort = simulate_merge_topology(MergeTopologySimConfig {
            topology_policy: MergeTopologyPolicy {
                strategy: MergeStrategy::MicrocohortReducePlusValidatorPromotion,
                promotion_policy: HeadPromotionPolicy {
                    apply_single_root_ema: true,
                    ..HeadPromotionPolicy::default()
                },
                ..MergeTopologyPolicy::default()
            },
            ..MergeTopologySimConfig::default()
        })
        .expect("microcohort");

        assert!(
            microcohort.metrics.certified_head_latency_ms
                <= gossip.metrics.certified_head_latency_ms
        );
    }

    #[test]
    fn degraded_network_matrix_increases_drop_and_overload_under_storm_conditions() {
        let steady = simulate_merge_topology(MergeTopologySimConfig {
            peer_count: 48,
            reducer_pool_size: 8,
            simultaneous_finish_fraction: 0.35,
            churn_rate: 0.02,
            cache_hit_rate: 0.35,
            ..MergeTopologySimConfig::default()
        })
        .expect("steady");
        let storm = simulate_merge_topology(MergeTopologySimConfig {
            peer_count: 48,
            reducer_pool_size: 4,
            simultaneous_finish_fraction: 0.9,
            churn_rate: 0.30,
            cache_hit_rate: 0.05,
            ..MergeTopologySimConfig::default()
        })
        .expect("storm");

        assert!(storm.metrics.raw_update_drops >= steady.metrics.raw_update_drops);
        assert!(storm.metrics.reducer_overload_rate >= steady.metrics.reducer_overload_rate);
        assert!(storm.metrics.accepted_sample_coverage <= steady.metrics.accepted_sample_coverage);
    }

    #[test]
    fn degraded_network_thresholds_remain_serviceable_under_heavy_faults() {
        let stressed = simulate_merge_topology(MergeTopologySimConfig {
            peer_count: 64,
            reducer_pool_size: 4,
            validator_count: 2,
            simultaneous_finish_fraction: 0.85,
            churn_rate: 0.25,
            cache_hit_rate: 0.1,
            malicious_peer_fraction: 0.0,
            ..MergeTopologySimConfig::default()
        })
        .expect("stressed");

        assert!(stressed.metrics.accepted_sample_coverage >= 0.55);
        assert!(stressed.metrics.malicious_acceptance_rate <= 0.05);
        assert!(stressed.metrics.reducer_overload_rate <= 1.0);
        assert!(
            stressed.metrics.certified_head_latency_ms
                <= (stressed.config.topology_policy.window_duration_secs as u64 * 1000) * 2
        );
    }
}
