//! advanced burn integration hooks for custom dataset, shard, and local batch
//! plumbing.

use super::*;

#[derive(Clone, Debug)]
/// Configures the local dataset fallback used by
/// [`BurnLearnerProjectBuilderAdvancedExt::with_batches`],
/// [`BurnLearnerProjectBuilderAdvancedExt::with_assignment_batches`], and
/// [`BurnLearnerProjectBuilder::with_train_loader`].
pub struct BurnLocalDatasetConfig {
    /// Label used to derive stable dataset and view ids.
    pub dataset_name: String,
    /// Approximate dataset sizing used for planning.
    pub sizing: crate::DatasetSizing,
}

impl Default for BurnLocalDatasetConfig {
    fn default() -> Self {
        Self {
            dataset_name: "burn-local-dataset".into(),
            sizing: crate::DatasetSizing {
                total_examples: 1,
                total_tokens: 0,
                total_bytes: 1,
            },
        }
    }
}

/// Advanced burn builder hooks for projects that need custom local batch or
/// shard plumbing beyond the common loader-based path.
pub trait BurnLearnerProjectBuilderAdvancedExt<LC>: Sized
where
    LC: LearningComponentsTypes + 'static,
    BurnLearnerModel<LC>: BurnModuleTarget<BurnLearnerBackend<LC>>
        + TrainStep
        + AutodiffModule<BurnLearnerBackend<LC>, InnerModule = BurnLearnerEvalModel<LC>>
        + Clone
        + core::fmt::Display
        + 'static,
    BurnLearnerEvalModel<LC>: BurnModuleTarget<<BurnLearnerBackend<LC> as AutodiffBackend>::InnerBackend>
        + InferenceStep
        + Clone
        + 'static,
{
    /// Sets dataset registration.
    fn with_dataset_registration<F>(self, dataset_registration: F) -> Self
    where
        F: Fn() -> anyhow::Result<crate::DatasetRegistration> + Send + Sync + 'static;

    /// Sets a fixed dataset registration and microshard plan.
    fn with_dataset(
        self,
        registration: crate::DatasetRegistration,
        microshard_plan: crate::MicroShardPlan,
    ) -> Self;

    /// Overrides the local dataset metadata used by `with_batches(...)`,
    /// `with_assignment_batches(...)`, and `with_train_loader(...)`.
    fn with_local_dataset(self, config: BurnLocalDatasetConfig) -> Self;

    /// Sets microshard planning.
    fn with_microshard_plan<F>(self, microshard_plan: F) -> Self
    where
        F: Fn(&crate::DatasetRegistration) -> anyhow::Result<crate::MicroShardPlan>
            + Send
            + Sync
            + 'static;

    /// Sets batch loading from cached microshards.
    fn with_load_batches<F>(self, load_batches: F) -> Self
    where
        F: Fn(
                &AssignmentLease,
                &[CachedMicroShard],
                &BurnLearnerDevice<LC>,
            ) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
            + Send
            + Sync
            + 'static;

    /// Sets a simple batch source for local learner-owned datasets.
    fn with_batches<F>(self, batches: F) -> Self
    where
        F: Fn(&BurnLearnerDevice<LC>) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
            + Send
            + Sync
            + 'static;

    /// Sets a lease-aware batch source without explicit shard plumbing.
    fn with_assignment_batches<F>(self, batches: F) -> Self
    where
        F: Fn(
                &AssignmentLease,
                &BurnLearnerDevice<LC>,
            ) -> anyhow::Result<Vec<BurnLearnerBatch<LC>>>
            + Send
            + Sync
            + 'static;
}
