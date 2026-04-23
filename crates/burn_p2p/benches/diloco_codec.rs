use burn_p2p::{
    BaseCheckpointId, ContentId, EncodedPseudoGradient, ExperimentId, FlattenedTensorPack,
    GradientCodec, PeerId, RevisionId, RoundCursor, decode_pseudo_gradient, encode_pseudo_gradient,
};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

fn gradient_pack(len: usize) -> FlattenedTensorPack {
    FlattenedTensorPack::new(
        ContentId::new("bench-schema"),
        ContentId::new("bench-layout"),
        (0..len)
            .map(|index| {
                (((index as f32) * 0.0137).sin() * 3.5) + (((index as f32) * 0.0031).cos())
            })
            .collect(),
    )
}

fn codec_cases() -> [(&'static str, GradientCodec); 4] {
    [
        ("fp16", GradientCodec::Fp16),
        (
            "blockwise-int8",
            GradientCodec::BlockwiseInt8 { block_size: 256 },
        ),
        (
            "qsgd-8bit",
            GradientCodec::Qsgd {
                bits: 8,
                stochastic: false,
            },
        ),
        (
            "signsgd",
            GradientCodec::SignSgd {
                error_feedback: true,
            },
        ),
    ]
}

fn encode_bench_input(
    codec: GradientCodec,
    round_cursor: RoundCursor,
    pack: &FlattenedTensorPack,
) -> EncodedPseudoGradient {
    encode_pseudo_gradient(
        ExperimentId::new("bench-experiment"),
        RevisionId::new("bench-revision"),
        PeerId::new("bench-peer"),
        round_cursor,
        codec,
        pack,
        32 * 1024,
    )
    .expect("encode pseudo gradient")
}

fn bench_encode(c: &mut Criterion) {
    let pack = gradient_pack(65_536);
    let mut group = c.benchmark_group("diloco_encode");
    let round_cursor = RoundCursor::new(BaseCheckpointId::new("bench-base"), 8);

    for (label, codec) in codec_cases() {
        group.throughput(Throughput::Elements(pack.values.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), &codec, |b, codec| {
            b.iter(|| {
                let encoded =
                    encode_bench_input(codec.clone(), round_cursor.clone(), black_box(&pack));
                black_box(encoded.total_encoded_bytes())
            });
        });
    }

    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let pack = gradient_pack(65_536);
    let round_cursor = RoundCursor::new(BaseCheckpointId::new("bench-base"), 8);
    let encoded_inputs = codec_cases()
        .into_iter()
        .map(|(label, codec)| {
            (
                label,
                encode_bench_input(codec, round_cursor.clone(), &pack),
            )
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("diloco_decode");
    for (label, encoded) in &encoded_inputs {
        group.throughput(Throughput::Elements(pack.values.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), encoded, |b, encoded| {
            b.iter(|| {
                let decoded = decode_pseudo_gradient(
                    black_box(&encoded.manifest),
                    black_box(encoded.chunks.as_slice()),
                )
                .expect("decode pseudo gradient");
                black_box(decoded.values.len())
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
