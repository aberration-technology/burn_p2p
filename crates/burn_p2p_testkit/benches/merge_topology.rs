//! Test harnesses, fixtures, and mixed-fleet verification helpers for burn_p2p.
#![allow(missing_docs)]
use burn_p2p_core::{MergeStrategy, MergeTopologyPolicy};
use burn_p2p_testkit::merge_topology::{MergeTopologySimConfig, simulate_merge_topology};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

fn bench_merge_topology_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_topology_policy_compare");

    for (label, strategy) in [
        ("central", MergeStrategy::CentralReducerBaseline),
        ("dag", MergeStrategy::ReplicatedRendezvousDag),
        (
            "microcohort",
            MergeStrategy::MicrocohortReducePlusValidatorPromotion,
        ),
    ] {
        for peer_count in [64_u32, 256_u32] {
            let config = MergeTopologySimConfig {
                peer_count,
                topology_policy: MergeTopologyPolicy {
                    strategy: strategy.clone(),
                    ..MergeTopologyPolicy::default()
                },
                ..MergeTopologySimConfig::default()
            };
            group.throughput(Throughput::Elements(peer_count as u64));
            group.bench_with_input(BenchmarkId::new(label, peer_count), &config, |b, config| {
                b.iter(|| simulate_merge_topology(black_box(config.clone())).expect("sim"));
            });
        }
    }

    group.finish();
}

fn bench_same_time_finish_storm(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_topology_same_time_finish");

    for peer_count in [256_u32, 1024_u32] {
        let config = MergeTopologySimConfig {
            peer_count,
            simultaneous_finish_fraction: 1.0,
            topology_policy: MergeTopologyPolicy {
                strategy: MergeStrategy::ReplicatedRendezvousDag,
                reducer_replication: 2,
                target_leaf_cohort: 16,
                upper_fanin: 4,
                publish_jitter_ms: 2000,
                ..MergeTopologyPolicy::default()
            },
            ..MergeTopologySimConfig::default()
        };
        group.throughput(Throughput::Elements(peer_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{peer_count}_peers")),
            &config,
            |b, config| {
                b.iter(|| simulate_merge_topology(black_box(config.clone())).expect("storm sim"));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_merge_topology_strategies,
    bench_same_time_finish_storm
);
criterion_main!(benches);
