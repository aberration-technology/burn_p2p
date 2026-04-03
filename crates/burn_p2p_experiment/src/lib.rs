#![forbid(unsafe_code)]

pub mod control;
pub mod directory;
pub mod spec;
pub mod topology;

pub use control::{
    ActivationTarget, ExperimentControlCommand, ExperimentControlEnvelope, ExperimentControlState,
    ExperimentSnapshot, PatchPlan, StageError,
};
pub use directory::{ExperimentDirectory, ExperimentDirectoryAccess, ExperimentDirectoryPolicyExt};
pub use spec::{
    ExperimentSpec, PatchClass, PatchSupport, PatchValue, RevisionCompatibility, RevisionSpec,
    RuntimePatch, StudySpec,
};
pub use topology::{
    TopologyError, assign_reducers, open_merge_window, repair_reducer_candidates,
    validate_merge_topology,
};
