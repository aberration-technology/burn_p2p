use burn_p2p_core::{
    DatasetId, DatasetView, DatasetViewId, ExperimentId, NetworkId, PeerId, RevisionId, StudyId,
    WindowId,
};
use burn_p2p_dataloader::{
    DataReceiptBuilder, DatasetSizing, LeasePlanner, LeasePlannerConfig, MicroShardPlanner,
    MicroShardPlannerConfig,
};
use chrono::Utc;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

fn dataset_view() -> DatasetView {
    DatasetView {
        dataset_view_id: DatasetViewId::new("bench-dataset-view"),
        dataset_id: DatasetId::new("bench-dataset"),
        preprocessing_hash: burn_p2p_core::ContentId::new("bench-preprocess"),
        tokenizer_hash: Some(burn_p2p_core::ContentId::new("bench-tokenizer")),
        manifest_hash: burn_p2p_core::ContentId::new("bench-manifest"),
        metadata: std::collections::BTreeMap::new(),
    }
}

fn planner() -> MicroShardPlanner {
    MicroShardPlanner::new(MicroShardPlannerConfig {
        target_microshard_bytes: 4 * 1024 * 1024,
        min_microshards: 16,
        max_microshards: 32_768,
    })
    .expect("microshard planner")
}

fn lease_planner() -> LeasePlanner {
    LeasePlanner::new(LeasePlannerConfig {
        lease_duration_seconds: 300,
        max_microshards_per_lease: 128,
        cost_model: burn_p2p_dataloader::ShardCostModel {
            tokens_per_work_unit: 2048,
            examples_per_work_unit: 32,
            bytes_per_work_unit: 512 * 1024,
            minimum_work_units: 1,
        },
    })
    .expect("lease planner")
}

fn bench_microshard_planning(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataloader_microshard_planning");
    let dataset_view = dataset_view();
    let planner = planner();

    for total_bytes in [
        256_u64 * 1024 * 1024,
        1024_u64 * 1024 * 1024,
        4_u64 * 1024 * 1024 * 1024,
    ] {
        let sizing = DatasetSizing {
            total_examples: total_bytes / 2048,
            total_tokens: total_bytes / 4,
            total_bytes,
        };
        let planned = planner
            .plan(&dataset_view, sizing.clone())
            .expect("microshard planning throughput baseline");
        group.throughput(Throughput::Elements(planned.microshards.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}MiB", total_bytes / (1024 * 1024))),
            &sizing,
            |b, sizing| {
                b.iter(|| planner.plan(black_box(&dataset_view), black_box(sizing.clone())));
            },
        );
    }

    group.finish();
}

fn bench_lease_planning(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataloader_lease_planning");
    let dataset_view = dataset_view();
    let planner = planner();
    let lease_planner = lease_planner();
    let now = Utc::now();

    for total_bytes in [
        512_u64 * 1024 * 1024,
        2_u64 * 1024 * 1024 * 1024,
        8_u64 * 1024 * 1024 * 1024,
    ] {
        let sizing = DatasetSizing {
            total_examples: total_bytes / 2048,
            total_tokens: total_bytes / 4,
            total_bytes,
        };
        let plan = planner
            .plan(&dataset_view, sizing)
            .expect("microshard plan for lease bench");
        let peer_id = PeerId::new(format!("peer-{}mib", total_bytes / (1024 * 1024)));
        let microshard_count = plan.microshards.len() as u64;
        group.throughput(Throughput::Elements(microshard_count));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{microshard_count}_microshards")),
            &plan,
            |b, plan| {
                b.iter(|| {
                    lease_planner
                        .plan_lease(
                            NetworkId::new("bench-net"),
                            StudyId::new("bench-study"),
                            ExperimentId::new("bench-experiment"),
                            RevisionId::new("bench-revision"),
                            black_box(&dataset_view),
                            black_box(peer_id.clone()),
                            WindowId(7),
                            now,
                            512,
                            black_box(plan.microshards.as_slice()),
                        )
                        .expect("planned lease");
                });
            },
        );
    }

    group.finish();
}

fn bench_receipt_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("dataloader_receipt_generation");
    let dataset_view = dataset_view();
    let planner = planner();
    let lease_planner = lease_planner();
    let granted_at = Utc::now();

    for total_bytes in [256_u64 * 1024 * 1024, 1024_u64 * 1024 * 1024] {
        let sizing = DatasetSizing {
            total_examples: total_bytes / 2048,
            total_tokens: total_bytes / 4,
            total_bytes,
        };
        let plan = planner.plan(&dataset_view, sizing).expect("plan");
        let planned_lease = lease_planner
            .plan_lease(
                NetworkId::new("bench-net"),
                StudyId::new("bench-study"),
                ExperimentId::new("bench-experiment"),
                RevisionId::new("bench-revision"),
                &dataset_view,
                PeerId::new(format!("receipt-peer-{total_bytes}")),
                WindowId(11),
                granted_at,
                512,
                &plan.microshards,
            )
            .expect("lease");
        let builder = DataReceiptBuilder::accepted(
            planned_lease.selection.estimated_examples,
            planned_lease.selection.estimated_tokens,
        );
        let shard_count = planned_lease.lease.microshards.len() as u64;
        group.throughput(Throughput::Elements(shard_count));
        group.bench_with_input(
            BenchmarkId::new(
                format!("{shard_count}_lease_shards"),
                format!("{}MiB", total_bytes / (1024 * 1024)),
            ),
            &planned_lease,
            |b, planned_lease| {
                b.iter(|| {
                    builder
                        .build(black_box(&planned_lease.lease), Utc::now())
                        .expect("receipt");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_microshard_planning,
    bench_lease_planning,
    bench_receipt_generation
);
criterion_main!(benches);
