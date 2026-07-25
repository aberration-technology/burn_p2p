//! `burn_ecs` integration for P2P training orchestration.
//!
//! The plugin in this module keeps P2P-specific state separate from model code
//! while publishing common training events that downstream applications can
//! monitor with the shared `burn_ecs` gates and sinks.

mod ingress;
mod messages;
mod plugin;
mod resources;

pub use ingress::{
    P2pTrainingEcsObserver, P2pTrainingEventBus, P2pTrainingEventBusStats, P2pTrainingIngressPlugin,
};
pub use messages::*;
pub use plugin::P2pTrainingPlugin;
pub use resources::*;
