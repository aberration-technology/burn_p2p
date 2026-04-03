use std::{collections::BTreeMap, io::Cursor};

use burn_p2p_checkpoint::{
    AggregateArtifactInput, AggregateArtifactRecord, ArtifactBuildSpec, ChunkingScheme, MergePlan,
    build_artifact_descriptor_from_reader, materialize_aggregate_artifact_bytes,
};
use burn_p2p_core::{
    AggregateStats, ArtifactId, ArtifactKind, ContentId, ContributionReceiptId, ExperimentId,
    HeadId, MergePolicy, Precision, RevisionId, StudyId,
};
use chrono::Utc;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

fn bench_payload(bytes_len: usize) -> Vec<u8> {
    let seed = b"burn-p2p-artifact-bench";
    (0..bytes_len)
        .map(|index| seed[index % seed.len()] ^ ((index & 0xff) as u8))
        .collect()
}

fn artifact_spec(label: &str) -> ArtifactBuildSpec {
    ArtifactBuildSpec::new(
        ArtifactKind::DeltaPack,
        Precision::Fp32,
        ContentId::derive(&("artifact-bench", label)).expect("content id"),
        "burn-p2p:bench",
    )
    .with_head(HeadId::new(format!("{label}-head")))
    .with_base_head(HeadId::new(format!("{label}-base")))
}

fn aggregate_record(input_count: usize) -> AggregateArtifactRecord {
    let study_id = StudyId::new("bench-study");
    let experiment_id = ExperimentId::new("bench-experiment");
    let revision_id = RevisionId::new("bench-revision");
    let base_head_id = HeadId::new("bench-base-head");

    AggregateArtifactRecord {
        aggregate_id: ContentId::derive(&("aggregate-bench", input_count)).expect("aggregate id"),
        study_id: study_id.clone(),
        experiment_id: experiment_id.clone(),
        revision_id: revision_id.clone(),
        base_head_id: base_head_id.clone(),
        policy: MergePolicy::QualityWeightedEma,
        aggregate_stats: AggregateStats {
            accepted_updates: input_count as u32,
            duplicate_updates: (input_count / 8) as u32,
            dropped_updates: 0,
            late_updates: (input_count / 16) as u32,
            sum_sample_weight: input_count as f64 * 32.0,
            sum_quality_weight: input_count as f64 * 0.95,
            sum_weighted_delta_norm: input_count as f64 * 4.0,
            max_update_norm: 1.75,
            accepted_sample_coverage: 0.98,
        },
        merge_plan: MergePlan {
            study_id: study_id.clone(),
            experiment_id: experiment_id.clone(),
            revision_id: revision_id.clone(),
            base_head_id: base_head_id.clone(),
            policy: MergePolicy::QualityWeightedEma,
            contribution_receipt_ids: (0..input_count)
                .map(|index| ContributionReceiptId::new(format!("receipt-{index:04}")))
                .collect(),
            artifact_ids: (0..input_count)
                .map(|index| ArtifactId::new(format!("artifact-{index:04}")))
                .collect(),
            total_weight: input_count as f64 * 0.95,
            aggregated_numeric_metrics: BTreeMap::from([
                ("loss".to_owned(), 0.125),
                ("accuracy".to_owned(), 0.875),
                ("quality".to_owned(), 0.96),
            ]),
        },
        inputs: (0..input_count)
            .map(|index| AggregateArtifactInput {
                peer_id: burn_p2p_core::PeerId::new(format!("peer-{index:04}")),
                artifact_id: ArtifactId::new(format!("delta-{index:04}")),
                sample_weight: 32.0 + (index % 5) as f64,
                quality_weight: 0.8 + ((index % 4) as f64 * 0.05),
                receipt_ids: vec![ContributionReceiptId::new(format!("receipt-{index:04}"))],
            })
            .collect(),
        created_at: Utc::now(),
    }
}

fn bench_descriptor_streaming(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_descriptor_streaming");

    for bytes_len in [8 * 1024 * 1024, 32 * 1024 * 1024] {
        let payload = bench_payload(bytes_len);
        group.throughput(Throughput::Bytes(bytes_len as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}MiB", bytes_len / (1024 * 1024))),
            &bytes_len,
            |b, bytes_len| {
                let chunking = ChunkingScheme::new(4 * 1024 * 1024).expect("chunking");
                let spec = artifact_spec(&format!("stream-{}", bytes_len));
                b.iter(|| {
                    build_artifact_descriptor_from_reader(
                        black_box(&spec),
                        Cursor::new(black_box(payload.as_slice())),
                        black_box(chunking),
                    )
                    .expect("descriptor");
                });
            },
        );
    }

    group.finish();
}

fn bench_aggregate_materialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_aggregate_materialization");

    for input_count in [16_usize, 64, 256] {
        let record = aggregate_record(input_count);
        let expected_bytes = serde_json::to_vec_pretty(&record)
            .expect("aggregate json")
            .len() as u64;
        group.throughput(Throughput::Bytes(expected_bytes));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{input_count}_inputs")),
            &input_count,
            |b, _| {
                let chunking = ChunkingScheme::new(1024 * 1024).expect("chunking");
                b.iter(|| {
                    materialize_aggregate_artifact_bytes(black_box(&record), black_box(chunking))
                        .expect("aggregate bytes");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_descriptor_streaming,
    bench_aggregate_materialization
);
criterion_main!(benches);
