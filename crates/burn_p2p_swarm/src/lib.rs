//! Swarm transport, control-plane, and overlay helpers for burn_p2p.
#![forbid(unsafe_code)]

mod browser_edge;
mod control_shell;
mod events;
mod memory_control;
mod memory_swarm;
mod native_control;
mod runtime_helpers;
mod stats;
mod types;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use burn_p2p_core::{
    AggregateEnvelope, ArtifactDescriptor, ArtifactId, AssignmentLease, ChunkDescriptor, ChunkId,
    ContentId, ControlCertificate, DatasetViewId, ExperimentDirectoryEntry, HeadDescriptor, HeadId,
    MergeCertificate, MergeWindowState, MetricsLiveEvent, MicroShardId, NetworkId,
    PeerAuthEnvelope, PeerId, ReducerAssignment, ReducerLoadReport, ReductionCertificate,
    TelemetrySummary, UpdateAnnounce,
};
use burn_p2p_experiment::ExperimentControlEnvelope;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use libp2p::Multiaddr;
use libp2p_core::{Transport, transport::MemoryTransport, upgrade};
use libp2p_identity::{Keypair, PeerId as Libp2pPeerId};
use libp2p_request_response::{self as request_response, ProtocolSupport};
use libp2p_swarm::{Config as Libp2pSwarmConfig, NetworkBehaviour, Swarm, SwarmEvent, dummy};
use libp2p_yamux as yamux;
use serde::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
use libp2p::{SwarmBuilder, connection_limits, gossipsub, identify};
#[cfg(not(target_arch = "wasm32"))]
use libp2p_mdns as mdns;
#[cfg(not(target_arch = "wasm32"))]
use tokio::{
    runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime},
    time::timeout,
};

pub use browser_edge::*;
pub use control_shell::*;
#[cfg(target_arch = "wasm32")]
pub(crate) use events::push_unique;
pub use events::*;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use events::{
    NativeControlPlaneBehaviour, NativeControlPlaneBehaviourEvent, apply_pubsub_payload,
    pubsub_payload_kind, push_unique,
};
pub use memory_control::*;
pub use memory_swarm::*;
pub use native_control::*;
#[cfg(not(target_arch = "wasm32"))]
use runtime_helpers::tls_config;
use runtime_helpers::{
    materialize_listen_addr, other_control_name, other_name, other_native_control_name,
    stream_protocol,
};
pub use stats::{
    MigrationCoordinator, MigrationPlan, PeerObservation, PeerStore, SwarmError, SwarmStats,
};
pub use types::{
    ExperimentOverlaySet, OverlayChannel, OverlayTopic, ProtocolId, ProtocolSet, RuntimeBoundary,
    RuntimeEnvironment, RuntimeTransportPolicy, SwarmAddress, TransportKind,
};

#[cfg(test)]
mod tests;
