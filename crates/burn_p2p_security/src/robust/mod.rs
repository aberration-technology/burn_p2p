//! Robustness screening, feature extraction, and bounded-influence aggregation helpers.

mod aggregate;
mod features;
mod policy;
mod quarantine;
mod reputation;
mod screen;

pub use aggregate::{
    CohortWeightDecision, aggregate_updates_with_policy, clipped_weighted_mean, coordinate_median,
    filter_update_sketches_with_policy, multi_krum_indices, trimmed_mean,
};
pub use features::{FeatureLayer, cosine_similarity, extract_feature_sketch};
pub use policy::{RobustUpdateObservation, RobustnessEngine};
pub use quarantine::should_quarantine;
pub use reputation::{TrustUpdateInput, updated_trust_score};
