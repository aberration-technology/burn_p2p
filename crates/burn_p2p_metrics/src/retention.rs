use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};

use crate::MetricEnvelope;
use burn_p2p_core::{ExperimentId, RevisionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RetentionKind {
    PeerWindow,
    ReducerCohort,
    HeadEval,
}

/// Configures the in-memory metrics indexer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsIndexerConfig {
    /// Head-lag threshold in steps used to classify stale work.
    pub stale_head_lag_threshold_steps: u64,
    /// Maximum number of raw entries included in one ledger segment.
    pub ledger_segment_entry_limit: usize,
    /// Maximum retained peer-window envelopes per experiment revision.
    pub max_peer_window_entries_per_revision: usize,
    /// Maximum retained reducer cohort envelopes per experiment revision.
    pub max_reducer_cohort_entries_per_revision: usize,
    /// Maximum retained head-eval envelopes per experiment revision.
    pub max_head_eval_reports_per_revision: usize,
    /// Maximum recent revisions with raw metrics detail retained per experiment.
    pub max_revisions_per_experiment: usize,
    /// Maximum raw peer windows returned by one detail drilldown.
    pub max_peer_window_detail_windows: usize,
}

impl Default for MetricsIndexerConfig {
    fn default() -> Self {
        Self {
            stale_head_lag_threshold_steps: 4,
            ledger_segment_entry_limit: 128,
            max_peer_window_entries_per_revision: 256,
            max_reducer_cohort_entries_per_revision: 128,
            max_head_eval_reports_per_revision: 192,
            max_revisions_per_experiment: 8,
            max_peer_window_detail_windows: 64,
        }
    }
}

pub(crate) fn enforce_retention(entries: &mut Vec<MetricEnvelope>, config: &MetricsIndexerConfig) {
    if entries.is_empty() {
        return;
    }

    let mut grouped =
        BTreeMap::<(ExperimentId, RevisionId, RetentionKind), Vec<(usize, DateTime<Utc>)>>::new();
    for (index, entry) in entries.iter().enumerate() {
        let (experiment_id, revision_id) = entry.revision_key();
        grouped
            .entry((experiment_id, revision_id, entry.retention_kind()))
            .or_default()
            .push((index, entry.captured_at()));
    }

    let mut keep = vec![false; entries.len()];
    for ((_, _, kind), mut indices) in grouped {
        indices.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
        let limit = match kind {
            RetentionKind::PeerWindow => config.max_peer_window_entries_per_revision,
            RetentionKind::ReducerCohort => config.max_reducer_cohort_entries_per_revision,
            RetentionKind::HeadEval => config.max_head_eval_reports_per_revision,
        };
        for (index, _) in indices.into_iter().take(limit) {
            keep[index] = true;
        }
    }

    let retained_entries = entries
        .drain(..)
        .enumerate()
        .filter_map(|(index, entry)| keep[index].then_some(entry))
        .collect::<Vec<_>>();

    let mut revision_latest = BTreeMap::<(ExperimentId, RevisionId), DateTime<Utc>>::new();
    for entry in &retained_entries {
        revision_latest
            .entry(entry.revision_key())
            .and_modify(|existing| {
                if entry.captured_at() > *existing {
                    *existing = entry.captured_at();
                }
            })
            .or_insert(entry.captured_at());
    }

    let mut revisions_by_experiment =
        BTreeMap::<ExperimentId, Vec<(RevisionId, DateTime<Utc>)>>::new();
    for ((experiment_id, revision_id), captured_at) in revision_latest {
        revisions_by_experiment
            .entry(experiment_id)
            .or_default()
            .push((revision_id, captured_at));
    }

    let mut retained_revision_keys = BTreeSet::<(ExperimentId, RevisionId)>::new();
    for (experiment_id, mut revisions) in revisions_by_experiment {
        revisions.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
        for (revision_id, _) in revisions
            .into_iter()
            .take(config.max_revisions_per_experiment.max(1))
        {
            retained_revision_keys.insert((experiment_id.clone(), revision_id));
        }
    }

    *entries = retained_entries
        .into_iter()
        .filter(|entry| retained_revision_keys.contains(&entry.revision_key()))
        .collect();
}
