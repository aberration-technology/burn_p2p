use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use burn_p2p_core::{
    COMPACT_UPDATE_PAYLOAD_VERSION, CompactScalarEncoding, CompactScalarVector, CompactUpdateBody,
    CompactUpdatePayload, ContentId, MergeStrategy, MergeTopologyPolicy, SeededFitnessGeneration,
    deterministic_cbor,
};
use serde::{Deserialize, Serialize};

use crate::merge_topology::{MergeTopologySimConfig, simulate_merge_topology};

/// Input for a deterministic compact-update and topology bandwidth ablation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BandwidthAblationConfig {
    /// Number of trainable model parameters represented by each update.
    pub parameter_count: u64,
    /// Number of simultaneously contributing peers.
    pub peer_count: u32,
    /// Number of exact record identities bound to each fitness generation.
    pub records_per_generation: u32,
    /// Number of generations bundled in one seeded-fitness update.
    pub fitness_generations: u32,
    /// Number of antithetic pairs independently replayed by a validator.
    pub replay_pairs_per_generation: u32,
}

impl Default for BandwidthAblationConfig {
    fn default() -> Self {
        Self {
            parameter_count: 100_000_000,
            peer_count: 64,
            records_per_generation: 32,
            fitness_generations: 1,
            replay_pairs_per_generation: 4,
        }
    }
}

/// One measured or explicitly estimated update representation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PayloadBandwidthRow {
    /// Stable representation label.
    pub representation: String,
    /// Whether the byte count came from a real encoder or a dense baseline estimate.
    pub byte_basis: String,
    /// Bytes uploaded by one peer per update.
    pub payload_bytes: u64,
    /// Dense FP32 bytes divided by payload bytes.
    pub compression_ratio_vs_dense_fp32: f64,
    /// Aggregate peer uploads before topology amplification.
    pub fleet_upload_bytes: u128,
    /// One-hop payload transfer latency at 25 Mbps.
    pub transfer_ms_25_mbps: u64,
    /// One-hop payload transfer latency at 100 Mbps.
    pub transfer_ms_100_mbps: u64,
    /// One-hop payload transfer latency at 400 Mbps.
    pub transfer_ms_400_mbps: u64,
    /// Domain in which compatible updates can be aggregated.
    pub aggregation_domain: String,
    /// Independent forward evaluations required by the configured replay policy.
    pub validator_forward_evaluations: u64,
}

/// Network amplification and completion metrics for one payload/topology pair.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TopologyBandwidthRow {
    /// Update representation.
    pub representation: String,
    /// Merge topology.
    pub topology: String,
    /// Total simulated bytes sent across all links.
    pub total_bytes_sent: u128,
    /// Bytes handled by the busiest peer.
    pub busiest_peer_bytes: u128,
    /// Simulated p95 completion latency.
    pub p95_merge_completion_ms: u64,
    /// Simulated certified-head latency.
    pub certified_head_latency_ms: u64,
    /// Ratio of duplicate transfers to useful update transfers.
    pub duplicate_transfer_ratio: f64,
    /// Fraction of sample weight retained after deterministic churn.
    pub accepted_sample_coverage: f64,
}

/// Complete deterministic ablation report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BandwidthAblationReport {
    /// Report schema.
    pub schema: String,
    /// Ablation input.
    pub config: BandwidthAblationConfig,
    /// Fixed heterogeneous-link mix used by the topology simulator.
    pub heterogeneous_peer_mix: BTreeMap<String, u32>,
    /// Payload measurements.
    pub payloads: Vec<PayloadBandwidthRow>,
    /// Topology measurements.
    pub topologies: Vec<TopologyBandwidthRow>,
    /// Scope boundary for interpreting the report.
    pub interpretation: Vec<String>,
}

impl BandwidthAblationReport {
    /// Renders a compact Markdown report suitable for CI artifacts.
    pub fn to_markdown(&self) -> String {
        let mut output = format!(
            "# P2P bandwidth ablation\n\n\
             Model parameters: `{}`  \n\
             Peers: `{}`  \n\
             Heterogeneous links: `{}` slow / `{}` medium / `{}` fast\n\n\
             ## Payloads\n\n\
             | representation | basis | bytes/peer | dense ratio | 25 Mbps | 100 Mbps | 400 Mbps | validator forwards |\n\
             |---|---:|---:|---:|---:|---:|---:|---:|\n",
            self.config.parameter_count,
            self.config.peer_count,
            self.heterogeneous_peer_mix
                .get("slow_25_mbps")
                .copied()
                .unwrap_or_default(),
            self.heterogeneous_peer_mix
                .get("medium_100_mbps")
                .copied()
                .unwrap_or_default(),
            self.heterogeneous_peer_mix
                .get("fast_400_mbps")
                .copied()
                .unwrap_or_default(),
        );
        for row in &self.payloads {
            output.push_str(&format!(
                "| {} | {} | {} | {:.1}x | {} ms | {} ms | {} ms | {} |\n",
                row.representation,
                row.byte_basis,
                row.payload_bytes,
                row.compression_ratio_vs_dense_fp32,
                row.transfer_ms_25_mbps,
                row.transfer_ms_100_mbps,
                row.transfer_ms_400_mbps,
                row.validator_forward_evaluations,
            ));
        }
        output.push_str(
            "\n## Topology\n\n\
             | representation | topology | total bytes | busiest peer | p95 merge | certified head | duplicate ratio |\n\
             |---|---|---:|---:|---:|---:|---:|\n",
        );
        for row in &self.topologies {
            output.push_str(&format!(
                "| {} | {} | {} | {} | {} ms | {} ms | {:.2} |\n",
                row.representation,
                row.topology,
                row.total_bytes_sent,
                row.busiest_peer_bytes,
                row.p95_merge_completion_ms,
                row.certified_head_latency_ms,
                row.duplicate_transfer_ratio,
            ));
        }
        output.push_str("\n## Interpretation\n\n");
        for note in &self.interpretation {
            output.push_str(&format!("- {note}\n"));
        }
        output
    }
}

/// Runs the deterministic bandwidth ablation with actual compact CBOR encoders.
pub fn run_bandwidth_ablation(config: BandwidthAblationConfig) -> Result<BandwidthAblationReport> {
    ensure!(
        config.parameter_count > 0 && config.peer_count > 0,
        "parameter_count and peer_count must be positive"
    );
    ensure!(
        config.records_per_generation > 0
            && config.fitness_generations > 0
            && config.replay_pairs_per_generation > 0,
        "fitness replay dimensions must be positive"
    );
    let dense_bytes = config
        .parameter_count
        .checked_mul(4)
        .context("dense FP32 payload byte count overflowed")?;
    let mut measured = vec![("dense_delta_fp32".to_owned(), dense_bytes, "dense_estimate")];

    for (dimensions, encoding) in [
        (1_280_u32, CompactScalarEncoding::Fp32),
        (1_280, CompactScalarEncoding::SymmetricInt8),
        (4_096, CompactScalarEncoding::SymmetricInt8),
    ] {
        let payload = subspace_payload(config.parameter_count, dimensions, encoding)?;
        measured.push((
            format!("subspace_{dimensions}_{}", scalar_encoding_label(encoding)),
            encoded_len(&payload)?,
            "canonical_cbor",
        ));
    }
    for population in [256_u32, 1_024, 4_096] {
        ensure!(
            config.replay_pairs_per_generation <= population / 2,
            "replay pair count exceeds seeded-fitness population"
        );
        let payload =
            seeded_fitness_payload(&config, population, CompactScalarEncoding::SymmetricInt8)?;
        measured.push((
            format!("seeded_fitness_pop{population}_int8"),
            encoded_len(&payload)?,
            "canonical_cbor",
        ));
    }

    let payloads = measured
        .iter()
        .map(|(representation, payload_bytes, basis)| {
            let validator_forward_evaluations = if representation.starts_with("seeded_fitness") {
                u64::from(config.fitness_generations)
                    * u64::from(config.replay_pairs_per_generation)
                    * 2
            } else {
                0
            };
            PayloadBandwidthRow {
                representation: representation.clone(),
                byte_basis: (*basis).to_owned(),
                payload_bytes: *payload_bytes,
                compression_ratio_vs_dense_fp32: dense_bytes as f64 / *payload_bytes as f64,
                fleet_upload_bytes: u128::from(*payload_bytes) * u128::from(config.peer_count),
                transfer_ms_25_mbps: transfer_ms(*payload_bytes, 25.0),
                transfer_ms_100_mbps: transfer_ms(*payload_bytes, 100.0),
                transfer_ms_400_mbps: transfer_ms(*payload_bytes, 400.0),
                aggregation_domain: if representation.starts_with("subspace") {
                    "coefficient_space_exact_affine".into()
                } else if representation.starts_with("seeded_fitness") {
                    "fitness_observation_space".into()
                } else {
                    "parameter_space".into()
                },
                validator_forward_evaluations,
            }
        })
        .collect::<Vec<_>>();

    let selected = [
        "dense_delta_fp32",
        "subspace_1280_int8",
        "seeded_fitness_pop4096_int8",
    ];
    let mut topologies = Vec::new();
    for representation in selected {
        let payload = payloads
            .iter()
            .find(|row| row.representation == representation)
            .context("selected bandwidth representation was not measured")?;
        for strategy in [
            MergeStrategy::GlobalBroadcastBaseline,
            MergeStrategy::CentralReducerBaseline,
            MergeStrategy::ReplicatedRendezvousDag,
            MergeStrategy::MicrocohortReducePlusValidatorPromotion,
        ] {
            let simulation = simulate_merge_topology(MergeTopologySimConfig {
                peer_count: config.peer_count,
                delta_bytes: payload.payload_bytes,
                topology_policy: MergeTopologyPolicy {
                    strategy: strategy.clone(),
                    ..MergeTopologyPolicy::default()
                },
                ..MergeTopologySimConfig::default()
            })
            .context("simulate compact-update merge topology")?;
            topologies.push(TopologyBandwidthRow {
                representation: representation.to_owned(),
                topology: merge_strategy_label(&strategy).into(),
                total_bytes_sent: simulation.metrics.total_bytes_sent,
                busiest_peer_bytes: simulation.metrics.busiest_peer_bytes,
                p95_merge_completion_ms: simulation.metrics.p95_merge_completion_ms,
                certified_head_latency_ms: simulation.metrics.certified_head_latency_ms,
                duplicate_transfer_ratio: simulation.metrics.duplicate_transfer_ratio,
                accepted_sample_coverage: simulation.metrics.accepted_sample_coverage,
            });
        }
    }

    let slow = config.peer_count.div_ceil(3);
    let medium = (config.peer_count + 1) / 3;
    let fast = config.peer_count / 3;
    Ok(BandwidthAblationReport {
        schema: "burn-p2p-bandwidth-ablation-v1".into(),
        config,
        heterogeneous_peer_mix: BTreeMap::from([
            ("slow_25_mbps".into(), slow),
            ("medium_100_mbps".into(), medium),
            ("fast_400_mbps".into(), fast),
        ]),
        payloads,
        topologies,
        interpretation: vec![
            "Canonical CBOR rows are measured from the production schema encoder; dense FP32 is parameter_count * 4 and excludes artifact-envelope overhead.".into(),
            "SubspaceLatent averaging is exactly linear in coefficient space, but the current seeded CountSketch-style map is not yet a learned FLITE UV^T z generator.".into(),
            "SeededFitness minimizes upload bytes but moves cost to independent validator forward replay; the table reports replay forward evaluations, not GPU time.".into(),
            "Topology results are deterministic simulations with equal repeating 25/100/400 Mbps link classes, 5% churn, and 5% malicious peers.".into(),
            "This report establishes communication and orchestration behavior only. It does not establish convergence, reasoning quality, or promotion readiness.".into(),
        ],
    })
}

fn subspace_payload(
    parameter_count: u64,
    dimensions: u32,
    encoding: CompactScalarEncoding,
) -> Result<CompactUpdatePayload> {
    let values = scalar_fixture(dimensions as usize);
    Ok(base_payload(
        parameter_count,
        CompactUpdateBody::SubspaceLatent {
            dimensions,
            seed: 0x5eed,
            coefficients: CompactScalarVector::encode(&values, encoding)
                .context("encode subspace coefficients")?,
        },
    ))
}

fn seeded_fitness_payload(
    config: &BandwidthAblationConfig,
    population: u32,
    encoding: CompactScalarEncoding,
) -> Result<CompactUpdatePayload> {
    let fitness = scalar_fixture(population as usize);
    let record_digests = (0..config.records_per_generation)
        .map(|record| ContentId::derive(&("bandwidth-record", record)))
        .collect::<Result<Vec<_>, _>>()
        .context("derive record digests")?;
    let generations = (0..config.fitness_generations)
        .map(|generation| {
            Ok(SeededFitnessGeneration {
                generation: u64::from(generation),
                batch_digest: ContentId::derive(&("bandwidth-batch", generation))?,
                record_digests: record_digests.clone(),
                reset_stream_state: generation == 0,
                fitness: CompactScalarVector::encode(&fitness, encoding)?,
            })
        })
        .collect::<Result<Vec<_>, burn_p2p_core::TrainingContractError>>()
        .context("build seeded-fitness generations")?;
    Ok(base_payload(
        config.parameter_count,
        CompactUpdateBody::SeededFitness {
            population,
            rank: 4,
            seed: 0x5eed,
            perturbation_generator_hash: ContentId::new("perturbation-generator-v1"),
            optimizer_update_hash: ContentId::new("optimizer-update-v1"),
            generations,
        },
    ))
}

fn base_payload(parameter_count: u64, body: CompactUpdateBody) -> CompactUpdatePayload {
    CompactUpdatePayload {
        version: COMPACT_UPDATE_PAYLOAD_VERSION,
        training_contract_id: ContentId::new("bandwidth-contract"),
        model_schema_hash: ContentId::new("bandwidth-model-schema"),
        parameter_catalog_hash: ContentId::new("bandwidth-parameter-catalog"),
        parameter_count,
        body,
    }
}

fn scalar_fixture(count: usize) -> Vec<f32> {
    (0..count)
        .map(|index| ((index % 31) as f32 - 15.0) / 16.0)
        .collect()
}

fn encoded_len(payload: &CompactUpdatePayload) -> Result<u64> {
    u64::try_from(
        deterministic_cbor(payload)
            .context("encode compact payload for bandwidth ablation")?
            .len(),
    )
    .context("compact payload length exceeded u64")
}

fn transfer_ms(bytes: u64, mbps: f64) -> u64 {
    (((bytes as f64 * 8.0) / (mbps * 1_000_000.0)) * 1_000.0).ceil() as u64
}

fn scalar_encoding_label(encoding: CompactScalarEncoding) -> &'static str {
    match encoding {
        CompactScalarEncoding::Fp32 => "fp32",
        CompactScalarEncoding::SymmetricInt8 => "int8",
        CompactScalarEncoding::SymmetricInt16 => "int16",
    }
}

fn merge_strategy_label(strategy: &MergeStrategy) -> &'static str {
    match strategy {
        MergeStrategy::GlobalBroadcastBaseline => "global_broadcast",
        MergeStrategy::CentralReducerBaseline => "central_reducer",
        MergeStrategy::RandomPeerGossip => "random_gossip",
        MergeStrategy::KRegularGossip => "k_regular_gossip",
        MergeStrategy::LocalGossipPlusPeriodicGlobal => "local_gossip_periodic_global",
        MergeStrategy::FixedTreeReduce => "fixed_tree",
        MergeStrategy::RotatingRendezvousTree => "rotating_tree",
        MergeStrategy::ReplicatedRendezvousDag => "replicated_dag",
        MergeStrategy::MicrocohortReducePlusValidatorPromotion => "microcohort_validator",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_payloads_reduce_bandwidth_and_topology_bytes() {
        let report =
            run_bandwidth_ablation(BandwidthAblationConfig::default()).expect("ablation report");
        let dense = report
            .payloads
            .iter()
            .find(|row| row.representation == "dense_delta_fp32")
            .expect("dense row");
        let subspace = report
            .payloads
            .iter()
            .find(|row| row.representation == "subspace_1280_int8")
            .expect("subspace row");
        let fitness_small = report
            .payloads
            .iter()
            .find(|row| row.representation == "seeded_fitness_pop256_int8")
            .expect("small fitness row");
        let fitness_large = report
            .payloads
            .iter()
            .find(|row| row.representation == "seeded_fitness_pop4096_int8")
            .expect("large fitness row");

        assert!(subspace.payload_bytes < dense.payload_bytes / 1_000);
        assert!(fitness_small.payload_bytes < fitness_large.payload_bytes);
        assert_eq!(fitness_large.validator_forward_evaluations, 8);

        let dense_dag = report
            .topologies
            .iter()
            .find(|row| {
                row.representation == "dense_delta_fp32" && row.topology == "replicated_dag"
            })
            .expect("dense dag");
        let subspace_dag = report
            .topologies
            .iter()
            .find(|row| {
                row.representation == "subspace_1280_int8" && row.topology == "replicated_dag"
            })
            .expect("subspace dag");
        assert!(subspace_dag.total_bytes_sent < dense_dag.total_bytes_sent / 1_000);
        assert!(report.to_markdown().contains("reasoning quality"));
    }
}
