#[cfg(feature = "metrics-indexer")]
use std::collections::BTreeMap;

use crate::BootstrapAdminState;
#[cfg(feature = "metrics-indexer")]
use burn_p2p::MetricsRetentionBudget;
#[cfg(feature = "metrics-indexer")]
use burn_p2p_core::{
    ExperimentId, HeadId, MetricsLiveEvent, MetricsLiveEventKind, NetworkId, ReducerCohortMetrics,
    RevisionId,
};
#[cfg(feature = "metrics-indexer")]
use burn_p2p_metrics::{
    CanonicalHeadAdoptionCurve, MetricEnvelope, MetricsCatchupBundle, MetricsIndexer,
    MetricsIndexerConfig, MetricsSnapshot, MetricsStore, PeerWindowDistributionDetail,
    PeerWindowDistributionSummary, VisibleHeadPopulationHistogram, build_catchup_bundles,
    build_live_event, derive_canonical_head_adoption_curves,
    derive_latest_canonical_head_population_histograms,
    derive_peer_window_distribution_detail_with_limit, derive_peer_window_distribution_summaries,
};
#[cfg(feature = "metrics-indexer")]
use chrono::Utc;

#[cfg(feature = "metrics-indexer")]
fn reducer_cohort_is_candidate_view(metrics: &ReducerCohortMetrics) -> bool {
    metrics.candidate_head_id.is_some()
        || metrics.status != burn_p2p_core::ReducerCohortStatus::Closed
}

#[cfg(feature = "metrics-indexer")]
fn reducer_cohort_is_disagreement(metrics: &ReducerCohortMetrics) -> bool {
    metrics.status == burn_p2p_core::ReducerCohortStatus::Inconsistent
        || metrics
            .replica_agreement
            .map(|agreement| agreement < 0.999)
            .unwrap_or(false)
        || metrics.late_arrival_count.unwrap_or(0) > 0
        || metrics.missing_peer_count.unwrap_or(0) > 0
}

#[cfg(feature = "metrics-indexer")]
impl BootstrapAdminState {
    /// Exports grouped metrics snapshots across all loaded experiment revisions.
    pub fn export_metrics_snapshots(&self) -> anyhow::Result<Vec<MetricsSnapshot>> {
        let peer_window_metrics = self.resolved_peer_window_metrics();
        let reducer_cohort_metrics = self.resolved_reducer_cohort_metrics();
        let head_eval_reports = self.resolved_head_eval_reports();
        if let Some(store) = self.metrics_store()? {
            let snapshots = store.load_snapshots()?;
            if !snapshots.is_empty()
                || Self::metric_envelopes(
                    &peer_window_metrics,
                    &reducer_cohort_metrics,
                    &head_eval_reports,
                )
                .is_empty()
            {
                return Ok(snapshots);
            }
        }
        self.metrics_indexers(
            &peer_window_metrics,
            &reducer_cohort_metrics,
            &head_eval_reports,
        )?
        .into_iter()
        .map(|((experiment_id, revision_id), indexer)| {
            let snapshot_seq = peer_window_metrics
                .iter()
                .filter(|metrics| {
                    metrics.experiment_id == experiment_id && metrics.revision_id == revision_id
                })
                .count()
                + reducer_cohort_metrics
                    .iter()
                    .filter(|metrics| {
                        metrics.experiment_id == experiment_id && metrics.revision_id == revision_id
                    })
                    .count()
                + head_eval_reports
                    .iter()
                    .filter(|report| {
                        report.experiment_id == experiment_id && report.revision_id == revision_id
                    })
                    .count();
            indexer
                .export_snapshot(
                    &experiment_id,
                    &revision_id,
                    snapshot_seq as u64,
                    None,
                    Vec::new(),
                )
                .map_err(anyhow::Error::from)
        })
        .collect()
    }

    /// Exports grouped metrics snapshots for one experiment.
    pub fn export_metrics_snapshots_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<MetricsSnapshot>> {
        if let Some(store) = self.metrics_store()? {
            let snapshots = store.load_snapshots_for_experiment(experiment_id)?;
            if !snapshots.is_empty() || self.resolved_metric_envelopes().is_empty() {
                return Ok(snapshots);
            }
        }
        Ok(self
            .export_metrics_snapshots()?
            .into_iter()
            .filter(|snapshot| &snapshot.manifest.experiment_id == experiment_id)
            .collect())
    }

    /// Exports append-only metrics ledger segments across all loaded experiment revisions.
    pub fn export_metrics_ledger_segments(
        &self,
    ) -> anyhow::Result<Vec<burn_p2p_core::MetricsLedgerSegment>> {
        if let Some(store) = self.metrics_store()? {
            let segments = store.load_ledger_segments()?;
            if !segments.is_empty() || self.resolved_metric_envelopes().is_empty() {
                return Ok(segments);
            }
        }
        let mut segments = Vec::new();
        for ((experiment_id, revision_id), indexer) in self.resolved_metrics_indexers()? {
            segments.extend(indexer.export_ledger_segments(&experiment_id, &revision_id)?);
        }
        Ok(segments)
    }

    /// Exports current per-revision metrics catchup bundles.
    pub fn export_metrics_catchup_bundles(&self) -> anyhow::Result<Vec<MetricsCatchupBundle>> {
        if let Some(store) = self.metrics_store()? {
            let bundles = store.load_catchup_bundles()?;
            if !bundles.is_empty() || self.resolved_metric_envelopes().is_empty() {
                return Ok(bundles);
            }
        }
        Ok(build_catchup_bundles(
            self.export_metrics_snapshots()?,
            self.export_metrics_ledger_segments()?,
        ))
    }

    /// Exports current metrics catchup bundles for one experiment.
    pub fn export_metrics_catchup_bundles_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<MetricsCatchupBundle>> {
        if let Some(store) = self.metrics_store()? {
            let bundles = store.load_catchup_bundles_for_experiment(experiment_id)?;
            if !bundles.is_empty() || self.resolved_metric_envelopes().is_empty() {
                return Ok(bundles);
            }
        }
        Ok(self
            .export_metrics_catchup_bundles()?
            .into_iter()
            .filter(|bundle| &bundle.experiment_id == experiment_id)
            .collect())
    }

    /// Exports peer-window distribution summaries across all loaded experiment revisions.
    pub fn export_metrics_peer_window_distributions(
        &self,
    ) -> anyhow::Result<Vec<PeerWindowDistributionSummary>> {
        if let Some(store) = self.metrics_store()? {
            let summaries = store.load_peer_window_distributions()?;
            if !summaries.is_empty() || self.resolved_metric_envelopes().is_empty() {
                return Ok(summaries);
            }
        }
        Ok(derive_peer_window_distribution_summaries(
            &self
                .export_metrics_snapshots()?
                .into_iter()
                .flat_map(|snapshot| snapshot.peer_window_metrics.into_iter())
                .collect::<Vec<_>>(),
        ))
    }

    /// Exports peer-window distribution summaries for one experiment.
    pub fn export_metrics_peer_window_distributions_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<PeerWindowDistributionSummary>> {
        Ok(self
            .export_metrics_peer_window_distributions()?
            .into_iter()
            .filter(|summary| &summary.experiment_id == experiment_id)
            .collect())
    }

    /// Exports one peer-window distribution drilldown for an experiment revision and base head.
    pub fn export_metrics_peer_window_distribution_detail(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        base_head_id: &HeadId,
    ) -> anyhow::Result<Option<PeerWindowDistributionDetail>> {
        if let Some(store) = self.metrics_store()? {
            let detail = store.load_peer_window_distribution_detail(
                experiment_id,
                revision_id,
                base_head_id,
            )?;
            if detail.is_some() || self.resolved_metric_envelopes().is_empty() {
                return Ok(detail);
            }
        }
        Ok(derive_peer_window_distribution_detail_with_limit(
            &self.resolved_peer_window_metrics(),
            experiment_id,
            revision_id,
            base_head_id,
            self.metrics_retention.max_peer_window_detail_windows,
        ))
    }

    /// Exports canonical-head adoption curves across all loaded experiment revisions.
    pub fn export_metrics_head_adoption_curves(
        &self,
    ) -> anyhow::Result<Vec<CanonicalHeadAdoptionCurve>> {
        if let Some(store) = self.metrics_store()? {
            let curves = store.load_head_adoption_curves()?;
            if !curves.is_empty() || self.resolved_metric_envelopes().is_empty() {
                return Ok(curves);
            }
        }
        Ok(derive_canonical_head_adoption_curves(
            &self
                .export_metrics_snapshots()?
                .into_iter()
                .flat_map(|snapshot| snapshot.peer_window_metrics.into_iter())
                .collect::<Vec<_>>(),
            &self
                .export_metrics_snapshots()?
                .into_iter()
                .flat_map(|snapshot| snapshot.head_eval_reports.into_iter())
                .collect::<Vec<_>>(),
        ))
    }

    /// Exports canonical-head adoption curves for one experiment.
    pub fn export_metrics_head_adoption_curves_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<CanonicalHeadAdoptionCurve>> {
        Ok(self
            .export_metrics_head_adoption_curves()?
            .into_iter()
            .filter(|curve| &curve.experiment_id == experiment_id)
            .collect())
    }

    /// Exports latest-canonical visible-head population histograms across revisions.
    pub fn export_metrics_head_populations(
        &self,
    ) -> anyhow::Result<Vec<VisibleHeadPopulationHistogram>> {
        if let Some(store) = self.metrics_store()? {
            let histograms = store.load_visible_head_population_histograms()?;
            if !histograms.is_empty() || self.resolved_metric_envelopes().is_empty() {
                return Ok(histograms);
            }
        }
        Ok(derive_latest_canonical_head_population_histograms(
            &self
                .export_metrics_snapshots()?
                .into_iter()
                .flat_map(|snapshot| snapshot.peer_window_metrics.into_iter())
                .collect::<Vec<_>>(),
            &self
                .export_metrics_snapshots()?
                .into_iter()
                .flat_map(|snapshot| snapshot.head_eval_reports.into_iter())
                .collect::<Vec<_>>(),
        ))
    }

    /// Exports latest-canonical visible-head population histograms for one experiment.
    pub fn export_metrics_head_populations_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<VisibleHeadPopulationHistogram>> {
        Ok(self
            .export_metrics_head_populations()?
            .into_iter()
            .filter(|histogram| &histogram.experiment_id == experiment_id)
            .collect())
    }

    /// Exports candidate-focused reducer cohorts across all loaded experiment revisions.
    pub fn export_metrics_candidates(&self) -> anyhow::Result<Vec<ReducerCohortMetrics>> {
        let mut cohorts = self
            .export_metrics_snapshots()?
            .into_iter()
            .flat_map(|snapshot| snapshot.reducer_cohort_metrics.into_iter())
            .filter(reducer_cohort_is_candidate_view)
            .collect::<Vec<_>>();
        cohorts.sort_by_key(|metrics| std::cmp::Reverse(metrics.captured_at));
        Ok(cohorts)
    }

    /// Exports candidate-focused reducer cohorts for one experiment.
    pub fn export_metrics_candidates_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<ReducerCohortMetrics>> {
        Ok(self
            .export_metrics_candidates()?
            .into_iter()
            .filter(|metrics| &metrics.experiment_id == experiment_id)
            .collect())
    }

    /// Exports reducer cohorts that currently indicate disagreement or inconsistency.
    pub fn export_metrics_disagreements(&self) -> anyhow::Result<Vec<ReducerCohortMetrics>> {
        let mut cohorts = self
            .export_metrics_snapshots()?
            .into_iter()
            .flat_map(|snapshot| snapshot.reducer_cohort_metrics.into_iter())
            .filter(reducer_cohort_is_disagreement)
            .collect::<Vec<_>>();
        cohorts.sort_by_key(|metrics| std::cmp::Reverse(metrics.captured_at));
        Ok(cohorts)
    }

    /// Exports disagreement-focused reducer cohorts for one experiment.
    pub fn export_metrics_disagreements_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> anyhow::Result<Vec<ReducerCohortMetrics>> {
        Ok(self
            .export_metrics_disagreements()?
            .into_iter()
            .filter(|metrics| &metrics.experiment_id == experiment_id)
            .collect())
    }

    /// Exports the current live metrics refresh event.
    pub fn export_metrics_live_event(
        &self,
        network_id: &NetworkId,
    ) -> anyhow::Result<MetricsLiveEvent> {
        if let Some(event) = self.runtime_snapshot.as_ref().and_then(|snapshot| {
            snapshot
                .control_plane
                .metrics_announcements
                .iter()
                .max_by_key(|announcement| announcement.event.generated_at)
                .map(|announcement| announcement.event.clone())
        }) {
            return Ok(event);
        }

        if let Some(store) = self.metrics_store()?
            && let Some(event) = store.load_live_event(MetricsLiveEventKind::CatchupRefresh)?
        {
            return Ok(event);
        }

        Ok(build_live_event(
            MetricsLiveEventKind::CatchupRefresh,
            &self.export_metrics_snapshots()?,
            &self.export_metrics_ledger_segments()?,
        )
        .unwrap_or(MetricsLiveEvent {
            network_id: network_id.clone(),
            kind: MetricsLiveEventKind::CatchupRefresh,
            cursors: Vec::new(),
            generated_at: Utc::now(),
        }))
    }

    fn resolved_metrics_indexers(
        &self,
    ) -> anyhow::Result<BTreeMap<(ExperimentId, RevisionId), MetricsIndexer>> {
        let peer_window_metrics = self.resolved_peer_window_metrics();
        let reducer_cohort_metrics = self.resolved_reducer_cohort_metrics();
        let head_eval_reports = self.resolved_head_eval_reports();
        self.metrics_indexers(
            &peer_window_metrics,
            &reducer_cohort_metrics,
            &head_eval_reports,
        )
    }

    fn resolved_metric_envelopes(&self) -> Vec<MetricEnvelope> {
        let peer_window_metrics = self.resolved_peer_window_metrics();
        let reducer_cohort_metrics = self.resolved_reducer_cohort_metrics();
        let head_eval_reports = self.resolved_head_eval_reports();
        Self::metric_envelopes(
            &peer_window_metrics,
            &reducer_cohort_metrics,
            &head_eval_reports,
        )
    }

    fn metrics_indexers(
        &self,
        peer_window_metrics: &[burn_p2p_core::PeerWindowMetrics],
        reducer_cohort_metrics: &[ReducerCohortMetrics],
        head_eval_reports: &[burn_p2p_core::HeadEvalReport],
    ) -> anyhow::Result<BTreeMap<(ExperimentId, RevisionId), MetricsIndexer>> {
        let mut indexers = BTreeMap::<(ExperimentId, RevisionId), MetricsIndexer>::new();

        for metrics in peer_window_metrics {
            indexers
                .entry((metrics.experiment_id.clone(), metrics.revision_id.clone()))
                .or_insert_with(|| {
                    MetricsIndexer::new(metrics_indexer_config_for_budget(self.metrics_retention))
                })
                .ingest_peer_window_metrics(metrics.clone());
        }
        for metrics in reducer_cohort_metrics {
            indexers
                .entry((metrics.experiment_id.clone(), metrics.revision_id.clone()))
                .or_insert_with(|| {
                    MetricsIndexer::new(metrics_indexer_config_for_budget(self.metrics_retention))
                })
                .ingest_reducer_cohort_metrics(metrics.clone());
        }
        for report in head_eval_reports {
            indexers
                .entry((report.experiment_id.clone(), report.revision_id.clone()))
                .or_insert_with(|| {
                    MetricsIndexer::new(metrics_indexer_config_for_budget(self.metrics_retention))
                })
                .ingest_head_eval_report(report.clone());
        }

        Ok(indexers)
    }

    fn metric_envelopes(
        peer_window_metrics: &[burn_p2p_core::PeerWindowMetrics],
        reducer_cohort_metrics: &[ReducerCohortMetrics],
        head_eval_reports: &[burn_p2p_core::HeadEvalReport],
    ) -> Vec<MetricEnvelope> {
        peer_window_metrics
            .iter()
            .cloned()
            .map(MetricEnvelope::PeerWindow)
            .chain(
                reducer_cohort_metrics
                    .iter()
                    .cloned()
                    .map(MetricEnvelope::ReducerCohort),
            )
            .chain(
                head_eval_reports
                    .iter()
                    .cloned()
                    .map(MetricEnvelope::HeadEval),
            )
            .collect()
    }

    fn metrics_store(&self) -> anyhow::Result<Option<MetricsStore>> {
        self.metrics_store_root
            .as_ref()
            .map(|root| {
                MetricsStore::open(
                    root,
                    metrics_indexer_config_for_budget(self.metrics_retention),
                )
                .map_err(anyhow::Error::from)
            })
            .transpose()
    }

    pub(crate) fn sync_metrics_store(&self) -> anyhow::Result<()> {
        let Some(root) = self.metrics_store_root.as_ref() else {
            return Ok(());
        };
        let peer_window_metrics = self.resolved_peer_window_metrics();
        let reducer_cohort_metrics = self.resolved_reducer_cohort_metrics();
        let head_eval_reports = self.resolved_head_eval_reports();
        let mut store = MetricsStore::open(
            root,
            metrics_indexer_config_for_budget(self.metrics_retention),
        )?;
        store.replace_entries(Self::metric_envelopes(
            &peer_window_metrics,
            &reducer_cohort_metrics,
            &head_eval_reports,
        ))?;
        store.materialize_views()?;
        Ok(())
    }
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn metrics_indexer_config_for_budget(
    budget: MetricsRetentionBudget,
) -> MetricsIndexerConfig {
    MetricsIndexerConfig {
        max_peer_window_entries_per_revision: budget.max_peer_window_entries_per_revision,
        max_reducer_cohort_entries_per_revision: budget.max_reducer_cohort_entries_per_revision,
        max_head_eval_reports_per_revision: budget.max_head_eval_reports_per_revision,
        max_revisions_per_experiment: budget.max_metric_revisions_per_experiment,
        max_peer_window_detail_windows: budget.max_peer_window_detail_windows,
        ..MetricsIndexerConfig::default()
    }
}
