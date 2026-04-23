use std::collections::BTreeMap;

use super::test_support::ScalarDiLoCoTestWorkload;
use crate::{
    BaseCheckpointId, DiLoCoAggregationPolicy, DiLoCoPolicy, DiLoCoReferenceCoordinator,
    DiLoCoReferencePeer, DiLoCoWorkload, ExperimentId, FlattenedTensorPack, GradientCodec,
    OuterOptimizerPolicy, PeerId, RevisionId, SignMajorityTieBreak,
};

fn coordinator(policy: DiLoCoPolicy) -> DiLoCoReferenceCoordinator {
    DiLoCoReferenceCoordinator::new(
        ExperimentId::new("exp-diloco"),
        RevisionId::new("rev-diloco"),
        policy,
    )
    .with_chunk_size_bytes(7)
}

#[test]
fn fp16_codec_roundtrip_preserves_shape_and_bound() {
    let pack = FlattenedTensorPack::new(
        crate::ContentId::new("schema"),
        crate::ContentId::new("layout"),
        (0..128).map(|idx| ((idx as f32) * 0.03125).sin()).collect(),
    );
    let encoded = crate::encode_pseudo_gradient(
        ExperimentId::new("exp"),
        RevisionId::new("rev"),
        PeerId::new("peer-a"),
        crate::RoundCursor::new(BaseCheckpointId::new("base"), 4),
        GradientCodec::Fp16,
        &pack,
        9,
    )
    .expect("encode fp16");
    let decoded =
        crate::decode_pseudo_gradient(&encoded.manifest, &encoded.chunks).expect("decode fp16");
    assert_eq!(decoded.model_schema_hash, pack.model_schema_hash);
    assert_eq!(decoded.layout_hash, pack.layout_hash);
    let max_error = decoded
        .values
        .iter()
        .zip(&pack.values)
        .map(|(left, right)| (left - right).abs())
        .fold(0.0_f32, f32::max);
    assert!(max_error < 0.001);
}

#[test]
fn blockwise_int8_codec_reassembles_chunks() {
    let pack = FlattenedTensorPack::new(
        crate::ContentId::new("schema"),
        crate::ContentId::new("layout"),
        (0..64).map(|idx| (idx as f32 / 10.0) - 3.0).collect(),
    );
    let encoded = crate::encode_pseudo_gradient(
        ExperimentId::new("exp"),
        RevisionId::new("rev"),
        PeerId::new("peer-b"),
        crate::RoundCursor::new(BaseCheckpointId::new("base"), 4),
        GradientCodec::BlockwiseInt8 { block_size: 8 },
        &pack,
        11,
    )
    .expect("encode int8");
    let decoded =
        crate::decode_pseudo_gradient(&encoded.manifest, &encoded.chunks).expect("decode int8");
    let max_error = decoded
        .values
        .iter()
        .zip(&pack.values)
        .map(|(left, right)| (left - right).abs())
        .fold(0.0_f32, f32::max);
    assert!(max_error < 0.05);
}

#[test]
fn sign_sgd_codec_uses_bit_packed_ternary_signs() {
    let pack = FlattenedTensorPack::new(
        crate::ContentId::new("schema"),
        crate::ContentId::new("layout"),
        vec![-2.0, 0.0, 3.0, -4.0, 5.0, -0.1, 0.0],
    );
    let encoded = crate::encode_pseudo_gradient(
        ExperimentId::new("exp"),
        RevisionId::new("rev"),
        PeerId::new("peer-sign"),
        crate::RoundCursor::new(BaseCheckpointId::new("base"), 4),
        GradientCodec::SignSgd {
            error_feedback: false,
        },
        &pack,
        64,
    )
    .expect("encode signSGD");

    let encoded_bytes = encoded
        .chunks
        .iter()
        .flat_map(|chunk| chunk.bytes.iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(encoded.total_encoded_bytes(), 2);
    assert_eq!(encoded_bytes, vec![146, 9]);

    let decoded =
        crate::decode_pseudo_gradient(&encoded.manifest, &encoded.chunks).expect("decode signSGD");
    assert_eq!(decoded.values, vec![-1.0, 0.0, 1.0, -1.0, 1.0, -1.0, 0.0]);
}

#[test]
fn qsgd_codec_roundtrips_with_deterministic_payloads() {
    let pack = FlattenedTensorPack::new(
        crate::ContentId::new("schema"),
        crate::ContentId::new("layout"),
        vec![0.0, 1.0, -2.0, 3.5, -4.25, 0.75],
    );
    let codec = GradientCodec::Qsgd {
        bits: 8,
        stochastic: true,
    };
    let left = crate::encode_pseudo_gradient(
        ExperimentId::new("exp"),
        RevisionId::new("rev"),
        PeerId::new("peer-qsgd"),
        crate::RoundCursor::new(BaseCheckpointId::new("base"), 4),
        codec.clone(),
        &pack,
        64,
    )
    .expect("encode qsgd left");
    let right = crate::encode_pseudo_gradient(
        ExperimentId::new("exp"),
        RevisionId::new("rev"),
        PeerId::new("peer-qsgd"),
        crate::RoundCursor::new(BaseCheckpointId::new("base"), 4),
        codec,
        &pack,
        64,
    )
    .expect("encode qsgd right");

    let left_bytes = left
        .chunks
        .iter()
        .flat_map(|chunk| chunk.bytes.iter().copied())
        .collect::<Vec<_>>();
    let right_bytes = right
        .chunks
        .iter()
        .flat_map(|chunk| chunk.bytes.iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(left_bytes, right_bytes);
    assert_eq!(left.total_encoded_bytes(), 4 + pack.parameter_count() * 3);

    let decoded = crate::decode_pseudo_gradient(&left.manifest, &left.chunks).expect("decode qsgd");
    assert_eq!(decoded.model_schema_hash, pack.model_schema_hash);
    assert_eq!(decoded.layout_hash, pack.layout_hash);
    assert_eq!(decoded.values.len(), pack.values.len());
    for (decoded, original) in decoded.values.iter().zip(&pack.values) {
        assert_eq!(decoded.signum(), original.signum());
    }
}

#[test]
fn pseudo_gradient_aggregation_is_order_invariant() {
    let first = FlattenedTensorPack::new(
        crate::ContentId::new("schema"),
        crate::ContentId::new("layout"),
        vec![1.0, 3.0],
    );
    let second = FlattenedTensorPack::new(
        crate::ContentId::new("schema"),
        crate::ContentId::new("layout"),
        vec![3.0, 1.0],
    );
    let left = crate::average_pseudo_gradients(&[first.clone(), second.clone()])
        .expect("average first ordering");
    let right = crate::average_pseudo_gradients(&[second, first]).expect("average second ordering");
    assert_eq!(left, right);
    assert_eq!(left.values, vec![2.0, 2.0]);
}

#[test]
fn coordinate_median_aggregation_rejects_outliers() {
    let gradients = [
        FlattenedTensorPack::new(
            crate::ContentId::new("schema"),
            crate::ContentId::new("layout"),
            vec![1.0, 10.0],
        ),
        FlattenedTensorPack::new(
            crate::ContentId::new("schema"),
            crate::ContentId::new("layout"),
            vec![2.0, 11.0],
        ),
        FlattenedTensorPack::new(
            crate::ContentId::new("schema"),
            crate::ContentId::new("layout"),
            vec![1000.0, -500.0],
        ),
    ];

    let aggregate =
        crate::aggregate_pseudo_gradients(&DiLoCoAggregationPolicy::CoordinateMedian, &gradients)
            .expect("aggregate coordinate median");

    assert_eq!(aggregate.values, vec![2.0, 10.0]);
}

#[test]
fn sign_majority_aggregation_obeys_threshold_and_tie_breaks() {
    let first = FlattenedTensorPack::new(
        crate::ContentId::new("schema"),
        crate::ContentId::new("layout"),
        vec![1.0, -1.0, 1.0, 1.0, 0.0],
    );
    let second = FlattenedTensorPack::new(
        crate::ContentId::new("schema"),
        crate::ContentId::new("layout"),
        vec![1.0, -1.0, -1.0, 0.0, 0.0],
    );
    let third = FlattenedTensorPack::new(
        crate::ContentId::new("schema"),
        crate::ContentId::new("layout"),
        vec![-1.0, -1.0, -1.0, 0.0, 1.0],
    );
    let gradients = vec![first, second, third];

    let aggregate = crate::aggregate_pseudo_gradients(
        &DiLoCoAggregationPolicy::SignMajority {
            minimum_agreement_micros: 500_000,
            tie_break: SignMajorityTieBreak::Zero,
        },
        &gradients,
    )
    .expect("aggregate sign majority");
    assert_eq!(aggregate.values, vec![1.0, -1.0, -1.0, 0.0, 0.0]);

    let strict = crate::aggregate_pseudo_gradients(
        &DiLoCoAggregationPolicy::SignMajority {
            minimum_agreement_micros: 700_000,
            tie_break: SignMajorityTieBreak::Zero,
        },
        &gradients,
    )
    .expect("aggregate strict sign majority");
    assert_eq!(strict.values, vec![0.0, -1.0, 0.0, 0.0, 0.0]);

    let tie_positive = crate::aggregate_pseudo_gradients(
        &DiLoCoAggregationPolicy::SignMajority {
            minimum_agreement_micros: 500_000,
            tie_break: SignMajorityTieBreak::Positive,
        },
        &[
            FlattenedTensorPack::new(
                crate::ContentId::new("schema"),
                crate::ContentId::new("layout"),
                vec![1.0],
            ),
            FlattenedTensorPack::new(
                crate::ContentId::new("schema"),
                crate::ContentId::new("layout"),
                vec![-1.0],
            ),
        ],
    )
    .expect("aggregate tie sign majority");
    assert_eq!(tie_positive.values, vec![1.0]);
}

#[test]
fn reference_round_advances_cursor_and_emits_checkpoint() {
    let workload = ScalarDiLoCoTestWorkload::reference(0.5);
    let policy = DiLoCoPolicy {
        num_inner_steps: 2,
        target_group_size: 2,
        minimum_group_size: 1,
        checkpoint_interval_rounds: 1,
        outer_optimizer_policy: OuterOptimizerPolicy::Sgd {
            learning_rate_micros: 1_000_000,
            momentum_micros: Some(500_000),
            nesterov: false,
            weight_decay_micros: None,
        },
        ..DiLoCoPolicy::default()
    };
    let mut peers = vec![
        DiLoCoReferencePeer::bootstrap(
            &workload,
            PeerId::new("peer-a"),
            &policy,
            BaseCheckpointId::new("genesis"),
        )
        .expect("bootstrap peer a"),
        DiLoCoReferencePeer::bootstrap(
            &workload,
            PeerId::new("peer-b"),
            &policy,
            BaseCheckpointId::new("genesis"),
        )
        .expect("bootstrap peer b"),
    ];
    let batches = BTreeMap::from([
        (PeerId::new("peer-a"), vec![1.0, 1.0]),
        (PeerId::new("peer-b"), vec![0.5, 0.5]),
    ]);

    let outcome = coordinator(policy.clone())
        .run_round(&workload, &mut peers, &batches)
        .expect("run round");

    assert_eq!(outcome.completed_round.round_id.as_u64(), 0);
    assert_eq!(outcome.next_round_cursor.round_id.as_u64(), 1);
    assert_eq!(outcome.contributions.len(), 2);
    assert!(outcome.published_checkpoint.is_some());
    assert!(peers.iter().all(|peer| peer.checkpoint_head_id.is_some()));
}

#[test]
fn reference_round_rejects_stale_peers() {
    let workload = ScalarDiLoCoTestWorkload::reference(0.5);
    let policy = DiLoCoPolicy::default();
    let mut peers = vec![
        DiLoCoReferencePeer::bootstrap(
            &workload,
            PeerId::new("peer-a"),
            &policy,
            BaseCheckpointId::new("genesis"),
        )
        .expect("bootstrap peer a"),
        DiLoCoReferencePeer::bootstrap(
            &workload,
            PeerId::new("peer-b"),
            &policy,
            BaseCheckpointId::new("genesis"),
        )
        .expect("bootstrap peer b"),
    ];
    peers[1].round_cursor = peers[1]
        .round_cursor
        .advance(BaseCheckpointId::new("stale-base"));

    let error = coordinator(policy)
        .run_round(&workload, &mut peers, &BTreeMap::new())
        .expect_err("stale peer should fail");
    assert!(
        error.to_string().contains("stale DiLoCo round")
            || error.to_string().contains("base checkpoint")
    );
}

#[test]
fn checkpoint_restores_peer_for_rejoin() {
    let workload = ScalarDiLoCoTestWorkload::reference(0.25);
    let policy = DiLoCoPolicy {
        checkpoint_interval_rounds: 1,
        ..DiLoCoPolicy::default()
    };
    let mut peers = vec![
        DiLoCoReferencePeer::bootstrap(
            &workload,
            PeerId::new("peer-a"),
            &policy,
            BaseCheckpointId::new("genesis"),
        )
        .expect("bootstrap peer a"),
    ];
    let batches = BTreeMap::from([(PeerId::new("peer-a"), vec![2.0, 2.0])]);

    let outcome = coordinator(policy.clone())
        .run_round(&workload, &mut peers, &batches)
        .expect("run round");
    let checkpoint = outcome.published_checkpoint.expect("checkpoint");
    let restored = DiLoCoReferencePeer::from_checkpoint(
        &workload,
        PeerId::new("peer-rejoin"),
        &policy,
        &checkpoint,
    )
    .expect("restore from checkpoint");

    let restored_pack = workload
        .export_parameter_pack(&restored.model)
        .expect("restored pack");
    assert_eq!(restored_pack, checkpoint.parameters);
    assert_eq!(restored.round_cursor, checkpoint.snapshot.round_cursor);
}
