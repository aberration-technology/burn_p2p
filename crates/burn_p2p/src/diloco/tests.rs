use std::collections::BTreeMap;

use burn_p2p_workload::P2pWorkload;
use serde::{Deserialize, Serialize};

use crate::{
    BaseCheckpointId, CapabilityEstimate, DiLoCoAggregationPolicy, DiLoCoPolicy,
    DiLoCoReferenceCoordinator, DiLoCoReferencePeer, DiLoCoWorkload, EvalSplit, ExperimentId,
    FlattenedTensorPack, GradientCodec, MetricReport, MetricValue, OuterOptimizerPolicy,
    PatchOutcome, PatchSupport, PeerId, RevisionId, SignMajorityTieBreak, StateBlob,
    SupportedWorkload, TrainError, UpstreamAdapter, WindowCtx, WindowReport,
};

#[derive(Clone, Debug)]
struct ScalarDiLoCoWorkload {
    inner_lr: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MomentumState {
    velocity: f32,
}

impl crate::P2pWorkload for ScalarDiLoCoWorkload {
    type Device = ();
    type Model = f32;
    type Batch = f32;
    type WindowStats = BTreeMap<String, MetricValue>;

    fn init_model(&self, _device: &Self::Device) -> Self::Model {
        0.0
    }

    fn benchmark(&self, _model: &Self::Model, _device: &Self::Device) -> CapabilityEstimate {
        CapabilityEstimate {
            preferred_backends: vec!["ndarray".into()],
            work_units_per_second: 16.0,
            target_window_seconds: 1,
        }
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<Self::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, TrainError> {
        let delta = ctx.batches.iter().copied().sum::<f32>() * self.inner_lr;
        ctx.model += delta;
        Ok(WindowReport {
            contribution: None,
            stats: BTreeMap::from([("delta".into(), MetricValue::Float(delta as f64))]),
            completed_at: chrono::Utc::now(),
        })
    }

    fn evaluate(&self, model: &Self::Model, _split: EvalSplit) -> MetricReport {
        MetricReport {
            metrics: BTreeMap::from([("model".into(), MetricValue::Float(*model as f64))]),
            captured_at: chrono::Utc::now(),
        }
    }

    fn apply_patch(&mut self, _patch: &crate::RuntimePatch) -> PatchOutcome {
        PatchOutcome::Rejected("unsupported".into())
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        PatchSupport::default()
    }

    fn runtime_device(&self) -> Self::Device {}

    fn dataset_registration(&self) -> anyhow::Result<crate::DatasetRegistration> {
        Ok(crate::DatasetRegistration {
            manifest: crate::DatasetManifest {
                dataset_id: crate::DatasetId::new("scalar-dataset"),
                source_uri: "memory://scalar-dataset".into(),
                format: "synthetic".into(),
                manifest_hash: crate::ContentId::new("scalar-manifest"),
                metadata: BTreeMap::new(),
            },
            view: crate::DatasetView {
                dataset_view_id: crate::DatasetViewId::new("scalar-view"),
                dataset_id: crate::DatasetId::new("scalar-dataset"),
                preprocessing_hash: crate::ContentId::new("scalar-preprocess"),
                tokenizer_hash: None,
                manifest_hash: crate::ContentId::new("scalar-manifest"),
                metadata: BTreeMap::new(),
            },
            upstream: UpstreamAdapter::Local {
                root: "/tmp".into(),
            },
        })
    }

    fn microshard_plan(
        &self,
        _registration: &crate::DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        unimplemented!("DiLoCo reference tests do not materialize microshards")
    }

    fn load_batches(
        &self,
        _lease: &crate::AssignmentLease,
        _cached_microshards: &[crate::CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        unimplemented!("DiLoCo reference tests inject batches directly")
    }

    fn load_model_artifact(
        &self,
        _model: Self::Model,
        _descriptor: &crate::ArtifactDescriptor,
        _store: &crate::FsArtifactStore,
        _device: &Self::Device,
    ) -> anyhow::Result<Self::Model> {
        unimplemented!("DiLoCo reference tests do not load artifacts")
    }

    fn materialize_model_artifact(
        &self,
        _model: &Self::Model,
        _artifact_kind: crate::ArtifactKind,
        _head_id: crate::HeadId,
        _base_head_id: Option<crate::HeadId>,
        _store: &crate::FsArtifactStore,
    ) -> anyhow::Result<crate::ArtifactDescriptor> {
        unimplemented!("DiLoCo reference tests do not store artifacts")
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> BTreeMap<String, MetricValue> {
        report.stats.clone()
    }

    fn supported_workload(&self) -> SupportedWorkload {
        SupportedWorkload {
            workload_id: crate::WorkloadId::new("scalar-diloco"),
            workload_name: "Scalar DiLoCo".into(),
            model_program_hash: crate::ContentId::new("scalar-program"),
            checkpoint_format_hash: crate::ContentId::new("scalar-format"),
            supported_revision_family: crate::ContentId::new("scalar-family"),
            resource_class: "cpu".into(),
        }
    }

    fn model_schema_hash(&self) -> crate::ContentId {
        crate::ContentId::new("scalar-schema")
    }
}

impl DiLoCoWorkload for ScalarDiLoCoWorkload {
    fn export_parameter_pack(&self, model: &Self::Model) -> anyhow::Result<FlattenedTensorPack> {
        Ok(FlattenedTensorPack::new(
            self.model_schema_hash(),
            crate::ContentId::new("scalar-layout"),
            vec![*model],
        ))
    }

    fn import_parameter_pack(
        &self,
        _device: &Self::Device,
        pack: &FlattenedTensorPack,
    ) -> anyhow::Result<Self::Model> {
        Ok(*pack.values.first().unwrap_or(&0.0))
    }

    fn run_inner_steps(
        &self,
        model: &Self::Model,
        batches: &[Self::Batch],
        num_inner_steps: u32,
        inner_optimizer_state: Option<&StateBlob>,
    ) -> Result<burn_p2p_workload::DiLoCoInnerLoopReport, TrainError> {
        let mut local = *model;
        for batch in batches
            .iter()
            .copied()
            .cycle()
            .take(num_inner_steps as usize)
        {
            local += batch * self.inner_lr;
        }
        Ok(burn_p2p_workload::DiLoCoInnerLoopReport {
            local_parameters: self
                .export_parameter_pack(&local)
                .map_err(|error| TrainError::new(error.to_string()))?,
            inner_optimizer_state: inner_optimizer_state.cloned(),
            steps_completed: num_inner_steps,
            metrics: BTreeMap::from([("local_model".into(), MetricValue::Float(local as f64))]),
        })
    }

    fn initialize_outer_optimizer_state(
        &self,
        _model: &Self::Model,
        _policy: &OuterOptimizerPolicy,
    ) -> anyhow::Result<StateBlob> {
        StateBlob::try_new(
            "application/json",
            serde_json::to_vec(&MomentumState { velocity: 0.0 })?,
        )
        .map_err(anyhow::Error::from)
    }

    fn apply_aggregated_outer_update(
        &self,
        base: &FlattenedTensorPack,
        aggregate: &FlattenedTensorPack,
        outer_optimizer_state: &StateBlob,
        policy: &OuterOptimizerPolicy,
    ) -> anyhow::Result<(FlattenedTensorPack, StateBlob)> {
        let mut state: MomentumState = serde_json::from_slice(&outer_optimizer_state.bytes)?;
        let momentum = policy.momentum().unwrap_or(0.0) as f32;
        let grad = aggregate.values[0];
        state.velocity = momentum * state.velocity + grad;
        let step = match policy {
            OuterOptimizerPolicy::Sgd { nesterov: true, .. } => momentum * state.velocity + grad,
            _ => state.velocity,
        };
        let next = base.values[0] - ((policy.learning_rate() as f32) * step);
        let state = StateBlob::try_new("application/json", serde_json::to_vec(&state)?)?;
        Ok((
            FlattenedTensorPack::new(
                base.model_schema_hash.clone(),
                base.layout_hash.clone(),
                vec![next],
            ),
            state,
        ))
    }
}

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
    let left = crate::average_pseudo_gradients(&[first.clone(), second.clone()]).unwrap();
    let right = crate::average_pseudo_gradients(&[second, first]).unwrap();
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
    let workload = ScalarDiLoCoWorkload { inner_lr: 0.5 };
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
        .unwrap(),
        DiLoCoReferencePeer::bootstrap(
            &workload,
            PeerId::new("peer-b"),
            &policy,
            BaseCheckpointId::new("genesis"),
        )
        .unwrap(),
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
    let workload = ScalarDiLoCoWorkload { inner_lr: 0.5 };
    let policy = DiLoCoPolicy::default();
    let mut peers = vec![
        DiLoCoReferencePeer::bootstrap(
            &workload,
            PeerId::new("peer-a"),
            &policy,
            BaseCheckpointId::new("genesis"),
        )
        .unwrap(),
        DiLoCoReferencePeer::bootstrap(
            &workload,
            PeerId::new("peer-b"),
            &policy,
            BaseCheckpointId::new("genesis"),
        )
        .unwrap(),
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
    let workload = ScalarDiLoCoWorkload { inner_lr: 0.25 };
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
        .unwrap(),
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
