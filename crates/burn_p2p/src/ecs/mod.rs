//! `burn_ecs` integration for P2P training orchestration.
//!
//! The plugin in this module keeps P2P-specific state separate from model code
//! while publishing common training events that downstream applications can
//! monitor with the shared `burn_ecs` gates and sinks.

mod messages;
mod plugin;
mod resources;

pub use messages::*;
pub use plugin::P2pTrainingPlugin;
pub use resources::*;
