//! Swarm transport, control-plane, and overlay helpers for burn_p2p.
#![forbid(unsafe_code)]

mod browser_edge;
mod browser_runtime;
mod control_shell;
#[cfg(not(target_arch = "wasm32"))]
mod diloco_store;
mod events;
mod memory_control;
mod memory_swarm;
mod native_control;
mod runtime_helpers;
mod stats;
mod types;

#[cfg(not(target_arch = "wasm32"))]
use std::{collections::VecDeque, pin::Pin};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    task::{Context, Poll},
};

use burn_p2p_core::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use burn_p2p_core::time::Instant;
use burn_p2p_core::{
    AggregateEnvelope, ArtifactDescriptor, ArtifactId, AssignmentLease, ChunkDescriptor, ChunkId,
    ContentId, ControlCertificate, DatasetViewId, DiLoCoRequest, DiLoCoResponse,
    DiLoCoStateSnapshot, DiffusionPromotionCertificate, ExperimentDirectoryEntry,
    FlattenedTensorPack, HeadDescriptor, HeadId, MergeCertificate, MergeWindowState,
    MetricsLiveEvent, MicroShardId, NetworkId, PeerAuthEnvelope, PeerId, PeerRoleSet,
    PeerWindowPlacementHint, PseudoGradientChunk, PseudoGradientManifest, ReducerAssignment,
    ReducerLoadReport, ReductionCertificate, StateBlob, TelemetrySummary,
    TrainerPromotionAttestation, UpdateAnnounce, ValidationQuorumCertificate,
};
#[cfg(not(target_arch = "wasm32"))]
use burn_p2p_core::{
    DiLoCoRoundFinalize, DiLoCoRoundHeartbeat, DiLoCoRoundOffer, ExperimentId, RevisionId,
};
use burn_p2p_experiment::{
    ExperimentControlEnvelope, ExperimentLifecycleEnvelope, FleetScheduleEpochEnvelope,
};
use chrono::{DateTime, Utc};
#[cfg(not(target_arch = "wasm32"))]
use futures::{Stream, StreamExt};
#[cfg(not(target_arch = "wasm32"))]
use libp2p::Multiaddr;
#[cfg(not(target_arch = "wasm32"))]
use libp2p_core::{Transport, transport::MemoryTransport, upgrade};
use libp2p_identity::{Keypair, PeerId as Libp2pPeerId};
#[cfg(not(target_arch = "wasm32"))]
use libp2p_request_response::ProtocolSupport;
use libp2p_request_response::{self as request_response};
use libp2p_swarm::SwarmEvent;
#[cfg(not(target_arch = "wasm32"))]
use libp2p_swarm::{Config as Libp2pSwarmConfig, NetworkBehaviour, Swarm, dummy};
#[cfg(not(target_arch = "wasm32"))]
use libp2p_yamux as yamux;
use serde::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
use libp2p::{SwarmBuilder, autonat, connection_limits, dcutr, gossipsub, identify, ping, relay};
#[cfg(not(target_arch = "wasm32"))]
use libp2p_kad as kad;
#[cfg(not(target_arch = "wasm32"))]
use libp2p_mdns as mdns;
#[cfg(not(target_arch = "wasm32"))]
use libp2p_rendezvous as rendezvous;
#[cfg(not(target_arch = "wasm32"))]
use tokio::{
    runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime},
    time::timeout,
};

pub use browser_edge::*;
pub use browser_runtime::*;
pub use control_shell::*;
#[cfg(test)]
pub(crate) use events::apply_pubsub_payload;
pub use events::*;
pub(crate) use events::{
    ControlPlaneHotIndex, insert_aggregate_proposal_announcement_with_index,
    insert_diffusion_promotion_certificate_announcement_with_index,
    insert_merge_announcement_with_index, insert_reduction_certificate_announcement_with_index,
    insert_trainer_promotion_attestation_announcement_with_index,
    insert_validation_quorum_announcement_with_index,
};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use events::{
    NativeControlPlaneBehaviour, NativeControlPlaneBehaviourEvent, pubsub_payload_kind,
};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use events::{apply_pubsub_payload_with_index, pubsub_semantic_message_id};
pub use memory_control::*;
pub use memory_swarm::*;
pub use native_control::*;
#[cfg(not(target_arch = "wasm32"))]
use runtime_helpers::stream_protocol;
#[cfg(not(target_arch = "wasm32"))]
use runtime_helpers::{
    kademlia_protocol_for_control_protocol, materialize_listen_addr, other_native_control_name,
    peer_directory_record_key_for_peer, protocol_supports_relay_hop, protocol_supports_rendezvous,
    relay_reservation_listen_addr, rendezvous_namespace_for_control_protocol, tls_config,
};
use runtime_helpers::{other_control_name, other_name};
pub use stats::{
    MigrationCoordinator, MigrationPlan, PeerObservation, PeerStore, SwarmError, SwarmStats,
};
pub use types::{
    ExperimentOverlaySet, OverlayChannel, OverlayTopic, ProtocolId, ProtocolSet, RuntimeBoundary,
    RuntimeEnvironment, RuntimeTransportPolicy, SwarmAddress, TransportKind,
};

/// Returns whether the native swarm runtime can honestly advertise browser WebRTC direct support.
pub fn native_browser_webrtc_direct_runtime_supported() -> bool {
    cfg!(not(target_arch = "wasm32"))
}

/// Returns whether the native swarm runtime can honestly advertise browser WebTransport support.
pub fn native_browser_webtransport_gateway_runtime_supported() -> bool {
    false
}

#[cfg(test)]
mod tests;
