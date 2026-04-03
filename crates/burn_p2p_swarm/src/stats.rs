use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    CapabilityCard, HeadId, MetricValue, NetworkEstimate, PeerId, TelemetrySummary,
    WindowActivation,
};
use burn_p2p_experiment::ActivationTarget;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ExperimentOverlaySet, OverlayTopic};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PeerObservation {
    pub peer_id: PeerId,
    pub connected: bool,
    pub addresses: BTreeSet<crate::SwarmAddress>,
    pub capability_card: Option<CapabilityCard>,
    pub telemetry: Option<TelemetrySummary>,
    pub estimated_flops: Option<f64>,
    pub observed_at: DateTime<Utc>,
    pub tags: BTreeSet<String>,
}

impl PeerObservation {
    pub fn new(peer_id: PeerId, observed_at: DateTime<Utc>) -> Self {
        Self {
            peer_id,
            connected: true,
            addresses: BTreeSet::new(),
            capability_card: None,
            telemetry: None,
            estimated_flops: None,
            observed_at,
            tags: BTreeSet::new(),
        }
    }

    pub fn with_capability_card(mut self, capability_card: CapabilityCard) -> Self {
        self.capability_card = Some(capability_card);
        self
    }

    pub fn with_telemetry(mut self, telemetry: TelemetrySummary) -> Self {
        self.estimated_flops = extract_flops_estimate(&telemetry.metrics);
        self.telemetry = Some(telemetry);
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PeerStore {
    peers: BTreeMap<PeerId, PeerObservation>,
}

impl PeerStore {
    pub fn upsert(&mut self, observation: PeerObservation) {
        self.peers.insert(observation.peer_id.clone(), observation);
    }

    pub fn mark_connection(
        &mut self,
        peer_id: PeerId,
        connected: bool,
        observed_at: DateTime<Utc>,
    ) {
        self.peers
            .entry(peer_id.clone())
            .and_modify(|entry| {
                entry.connected = connected;
                entry.observed_at = observed_at;
            })
            .or_insert_with(|| {
                let mut observation = PeerObservation::new(peer_id, observed_at);
                observation.connected = connected;
                observation
            });
    }

    pub fn get(&self, peer_id: &PeerId) -> Option<&PeerObservation> {
        self.peers.get(peer_id)
    }

    pub fn observations(&self) -> impl Iterator<Item = &PeerObservation> {
        self.peers.values()
    }

    pub fn observed_peer_ids(&self) -> Vec<PeerId> {
        self.peers.keys().cloned().collect()
    }

    pub fn connected_peer_ids(&self) -> Vec<PeerId> {
        self.peers
            .iter()
            .filter_map(|(peer_id, observation)| observation.connected.then_some(peer_id.clone()))
            .collect()
    }

    pub fn stats(&self, remaining_work_units: Option<u64>) -> SwarmStats {
        let connected_peer_ids = self.connected_peer_ids();
        let observed_peer_ids = self.observed_peer_ids();
        let connected_peers = connected_peer_ids.len() as u32;
        let observed_peers = observed_peer_ids.len() as u64;

        let max_active_peers = self
            .observations()
            .filter_map(|observation| observation.telemetry.as_ref())
            .map(|telemetry| telemetry.active_peers as u64)
            .max()
            .unwrap_or(connected_peers as u64);

        let estimated_total_vram_bytes = {
            let mut sum = 0_u128;
            let mut saw_value = false;

            for observation in self
                .observations()
                .filter(|observation| observation.connected)
            {
                if let Some(device_memory_bytes) = observation
                    .capability_card
                    .as_ref()
                    .and_then(|card| card.device_memory_bytes)
                {
                    sum += u128::from(device_memory_bytes);
                    saw_value = true;
                }
            }

            saw_value.then_some(sum)
        };

        let estimated_total_flops = {
            let mut sum = 0.0_f64;
            let mut saw_value = false;

            for observation in self
                .observations()
                .filter(|observation| observation.connected)
            {
                if let Some(flops) = observation.estimated_flops {
                    sum += flops;
                    saw_value = true;
                }
            }

            saw_value.then_some(sum)
        };

        let throughput = self
            .observations()
            .filter(|observation| observation.connected)
            .filter_map(|observation| {
                observation
                    .telemetry
                    .as_ref()
                    .map(|telemetry| telemetry.throughput_work_units_per_second)
                    .or_else(|| {
                        observation
                            .capability_card
                            .as_ref()
                            .map(|card| card.work_units_per_second)
                    })
            })
            .sum::<f64>();

        let eta_seconds = remaining_work_units.and_then(|remaining| {
            (throughput > 0.0).then_some((remaining as f64 / throughput).ceil().max(1.0) as u64)
        });

        let (eta_lower_seconds, eta_upper_seconds) = eta_seconds
            .map(|eta| {
                (
                    ((eta as f64) * 0.8).floor().max(1.0) as u64,
                    ((eta as f64) * 1.2).ceil() as u64,
                )
            })
            .unzip();

        SwarmStats {
            connected_peers,
            connected_peer_ids,
            observed_peers: observed_peer_ids,
            network_estimate: NetworkEstimate {
                connected_peers,
                observed_peers,
                estimated_network_size: observed_peers.max(max_active_peers) as f64,
                estimated_total_vram_bytes,
                estimated_total_flops,
                eta_lower_seconds,
                eta_upper_seconds,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SwarmStats {
    pub connected_peers: u32,
    pub connected_peer_ids: Vec<PeerId>,
    pub observed_peers: Vec<PeerId>,
    pub network_estimate: NetworkEstimate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub activation: WindowActivation,
    pub required_client_capabilities: BTreeSet<String>,
    pub leave_topics: Vec<OverlayTopic>,
    pub join_topics: Vec<OverlayTopic>,
    pub fetch_base_head_id: Option<HeadId>,
    pub drain_current_window: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationCoordinator;

impl MigrationCoordinator {
    pub fn should_activate(
        current_window: burn_p2p_core::WindowId,
        activation: &WindowActivation,
    ) -> bool {
        current_window >= activation.activation_window
    }

    pub fn plan_overlay_transition(
        current: &ExperimentOverlaySet,
        next: &ExperimentOverlaySet,
        target: &ActivationTarget,
        fetch_base_head_id: Option<HeadId>,
    ) -> MigrationPlan {
        let current_topics = BTreeSet::from(current.experiment_topics());
        let next_topics = BTreeSet::from(next.experiment_topics());

        let leave_topics = current_topics
            .difference(&next_topics)
            .cloned()
            .collect::<Vec<_>>();
        let join_topics = next_topics
            .difference(&current_topics)
            .cloned()
            .collect::<Vec<_>>();

        MigrationPlan {
            activation: target.activation.clone(),
            required_client_capabilities: target.required_client_capabilities.clone(),
            leave_topics,
            join_topics,
            fetch_base_head_id,
            drain_current_window: true,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SwarmError {
    #[error("invalid swarm address `{0}`")]
    InvalidAddress(String),
    #[error("invalid peer id `{0}`")]
    InvalidPeerId(String),
    #[error("invalid protocol id `{0}`")]
    InvalidProtocolId(String),
    #[error("runtime error: {0}")]
    Runtime(String),
    #[error("request failure: {0}")]
    Request(String),
    #[error("timed out waiting for {0}")]
    TimedOut(&'static str),
    #[error("{reason}")]
    InvalidOverlayChannel { reason: &'static str },
    #[error("listen error: {0}")]
    Listen(String),
    #[error("dial error: {0}")]
    Dial(String),
    #[error("pubsub error: {0}")]
    Pubsub(String),
}

fn extract_flops_estimate(metrics: &BTreeMap<String, MetricValue>) -> Option<f64> {
    const FLOP_KEYS: [&str; 3] = ["estimated_flops", "sustained_flops", "flops_per_second"];

    FLOP_KEYS
        .iter()
        .find_map(|key| metrics.get(*key))
        .and_then(metric_as_f64)
}

fn metric_as_f64(metric: &MetricValue) -> Option<f64> {
    match metric {
        MetricValue::Integer(value) => Some(*value as f64),
        MetricValue::Float(value) => Some(*value),
        MetricValue::Bool(_) | MetricValue::Text(_) => None,
    }
}
