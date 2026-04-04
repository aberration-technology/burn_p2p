//! Experiment manifests, directory policies, and topology helpers for burn_p2p.
#![forbid(unsafe_code)]

/// Public APIs for control.
pub mod control;
/// Public APIs for directory.
pub mod directory;
/// Experiment specification helpers.
pub mod spec;
/// Reducer and merge topology helpers.
pub mod topology;

pub use control::{
    ActivationTarget, ExperimentControlCommand, ExperimentControlEnvelope, ExperimentControlState,
    ExperimentSnapshot, PatchPlan, StageError,
};
pub use directory::{ExperimentDirectory, ExperimentDirectoryAccess, ExperimentDirectoryPolicyExt};
pub use spec::{
    ExperimentSpec, PatchClass, PatchSupport, PatchValue, RevisionCompatibility,
    RevisionMetricsPolicy, RevisionSpec, RuntimePatch, StudySpec,
};
pub use topology::{
    TopologyError, assign_reducers, open_merge_window, repair_reducer_candidates,
    validate_merge_topology,
};
