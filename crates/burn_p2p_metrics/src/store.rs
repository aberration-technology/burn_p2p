use std::{
    collections::BTreeSet,
    fs,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use burn_p2p_core::{
    ExperimentId, HeadEvalReport, HeadId, MetricsLedgerSegment, MetricsLiveEvent,
    MetricsLiveEventKind, PeerWindowMetrics, ReducerCohortMetrics, RevisionId,
};
use chrono::Utc;
use thiserror::Error;

use crate::{
    CanonicalHeadAdoptionCurve, MetricEnvelope, MetricsCatchupBundle, MetricsIndexer,
    MetricsIndexerConfig, MetricsSnapshot, PeerWindowDistributionDetail,
    PeerWindowDistributionSummary, VisibleHeadPopulationHistogram,
    derive_canonical_head_adoption_curves, derive_latest_canonical_head_population_histograms,
    derive_peer_window_distribution_detail_with_limit, derive_peer_window_distribution_summaries,
};

/// Errors raised by the durable metrics store.
#[derive(Debug, Error)]
pub enum MetricsStoreError {
    /// Filesystem access failed.
    #[error("metrics store I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization or deserialization failed.
    #[error("metrics store JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// Content-addressing or schema derivation failed.
    #[error("metrics store schema failed: {0}")]
    Schema(#[from] burn_p2p_core::SchemaError),
}

/// Durable file-backed metrics store used by read-model services.
#[derive(Clone, Debug)]
pub struct MetricsStore {
    root_dir: PathBuf,
    indexer: MetricsIndexer,
}

impl MetricsStore {
    /// Opens or creates a durable metrics store rooted at the provided path.
    pub fn open(
        root_dir: impl AsRef<Path>,
        config: MetricsIndexerConfig,
    ) -> Result<Self, MetricsStoreError> {
        let root_dir = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(root_dir.join("snapshots"))?;
        fs::create_dir_all(root_dir.join("ledger"))?;

        let mut indexer = MetricsIndexer::new(config);
        let entries_path = root_dir.join("entries.jsonl");
        let mut loaded_entries = 0usize;
        if entries_path.exists() {
            let reader = BufReader::new(fs::File::open(&entries_path)?);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                loaded_entries += 1;
                match serde_json::from_str::<MetricEnvelope>(&line)? {
                    MetricEnvelope::PeerWindow(metrics) => {
                        indexer.ingest_peer_window_metrics(metrics)
                    }
                    MetricEnvelope::ReducerCohort(metrics) => {
                        indexer.ingest_reducer_cohort_metrics(metrics)
                    }
                    MetricEnvelope::HeadEval(report) => indexer.ingest_head_eval_report(report),
                }
            }
        }

        let store = Self { root_dir, indexer };
        if loaded_entries != store.indexer.entries.len() {
            store.persist_entries_file()?;
            store.materialize_views()?;
        }

        Ok(store)
    }

    /// Replaces the raw metric envelopes stored on disk and in memory.
    pub fn replace_entries(
        &mut self,
        entries: impl IntoIterator<Item = MetricEnvelope>,
    ) -> Result<(), MetricsStoreError> {
        let entries = entries.into_iter().collect::<Vec<_>>();
        self.indexer = MetricsIndexer::new(self.indexer.config.clone());
        for entry in &entries {
            match entry {
                MetricEnvelope::PeerWindow(metrics) => {
                    self.indexer.ingest_peer_window_metrics(metrics.clone())
                }
                MetricEnvelope::ReducerCohort(metrics) => {
                    self.indexer.ingest_reducer_cohort_metrics(metrics.clone())
                }
                MetricEnvelope::HeadEval(report) => {
                    self.indexer.ingest_head_eval_report(report.clone())
                }
            }
        }

        self.persist_entries_file()?;
        Ok(())
    }

    /// Rebuilds the durable store from metric records held by the caller.
    pub fn sync_from_records(
        &mut self,
        peer_window_metrics: &[PeerWindowMetrics],
        reducer_cohort_metrics: &[ReducerCohortMetrics],
        head_eval_reports: &[HeadEvalReport],
    ) -> Result<(), MetricsStoreError> {
        let entries = peer_window_metrics
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
            .collect::<Vec<_>>();
        self.replace_entries(entries)?;
        self.materialize_views()?;
        Ok(())
    }

    /// Materializes per-revision snapshots and ledger caches to disk.
    pub fn materialize_views(&self) -> Result<(), MetricsStoreError> {
        let snapshot_dir = self.root_dir.join("snapshots");
        let ledger_dir = self.root_dir.join("ledger");
        fs::create_dir_all(&snapshot_dir)?;
        fs::create_dir_all(&ledger_dir)?;

        let revisions = self
            .indexer
            .entries
            .iter()
            .map(|entry| (entry.experiment_id().clone(), entry.revision_id().clone()))
            .collect::<BTreeSet<_>>();

        let mut live_snapshot_paths = BTreeSet::new();
        let mut live_ledger_paths = BTreeSet::new();

        for (experiment_id, revision_id) in revisions {
            let snapshot_seq = self
                .indexer
                .entries
                .iter()
                .filter(|entry| {
                    entry.experiment_id() == &experiment_id && entry.revision_id() == &revision_id
                })
                .count() as u64;
            let snapshot = self.indexer.export_snapshot(
                &experiment_id,
                &revision_id,
                snapshot_seq,
                None,
                Vec::new(),
            )?;
            let ledger = self
                .indexer
                .export_ledger_segments(&experiment_id, &revision_id)?;

            let snapshot_path = self.snapshot_path(&experiment_id, &revision_id);
            let ledger_path = self.ledger_path(&experiment_id, &revision_id);
            serde_json::to_writer_pretty(
                BufWriter::new(fs::File::create(&snapshot_path)?),
                &snapshot,
            )?;
            serde_json::to_writer_pretty(BufWriter::new(fs::File::create(&ledger_path)?), &ledger)?;
            live_snapshot_paths.insert(snapshot_path);
            live_ledger_paths.insert(ledger_path);
        }

        prune_unknown_files(&snapshot_dir, &live_snapshot_paths)?;
        prune_unknown_files(&ledger_dir, &live_ledger_paths)?;
        Ok(())
    }

    /// Loads materialized metrics snapshots from disk.
    pub fn load_snapshots(&self) -> Result<Vec<MetricsSnapshot>, MetricsStoreError> {
        let snapshot_dir = self.root_dir.join("snapshots");
        let mut paths = read_json_paths(&snapshot_dir)?;
        if paths.is_empty() && !self.indexer.entries.is_empty() {
            self.materialize_views()?;
            paths = read_json_paths(&snapshot_dir)?;
        }
        paths.sort();
        let mut snapshots = paths
            .into_iter()
            .map(|path| {
                serde_json::from_reader::<_, MetricsSnapshot>(BufReader::new(fs::File::open(path)?))
                    .map_err(MetricsStoreError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        snapshots.sort_by(|left, right| {
            left.manifest
                .experiment_id
                .cmp(&right.manifest.experiment_id)
                .then_with(|| left.manifest.revision_id.cmp(&right.manifest.revision_id))
                .then_with(|| left.manifest.snapshot_seq.cmp(&right.manifest.snapshot_seq))
        });
        Ok(snapshots)
    }

    /// Loads materialized metrics snapshots for one experiment.
    pub fn load_snapshots_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<MetricsSnapshot>, MetricsStoreError> {
        Ok(self
            .load_snapshots()?
            .into_iter()
            .filter(|snapshot| &snapshot.manifest.experiment_id == experiment_id)
            .collect())
    }

    /// Loads materialized metrics ledger segments from disk.
    pub fn load_ledger_segments(&self) -> Result<Vec<MetricsLedgerSegment>, MetricsStoreError> {
        let ledger_dir = self.root_dir.join("ledger");
        let mut paths = read_json_paths(&ledger_dir)?;
        if paths.is_empty() && !self.indexer.entries.is_empty() {
            self.materialize_views()?;
            paths = read_json_paths(&ledger_dir)?;
        }
        paths.sort();
        let mut segments = Vec::new();
        for path in paths {
            segments.extend(serde_json::from_reader::<_, Vec<MetricsLedgerSegment>>(
                BufReader::new(fs::File::open(path)?),
            )?);
        }
        segments.sort_by(|left, right| {
            left.experiment_id
                .cmp(&right.experiment_id)
                .then_with(|| left.revision_id.cmp(&right.revision_id))
                .then_with(|| left.segment_seq.cmp(&right.segment_seq))
        });
        Ok(segments)
    }

    /// Loads materialized catchup bundles across all persisted experiment revisions.
    pub fn load_catchup_bundles(&self) -> Result<Vec<MetricsCatchupBundle>, MetricsStoreError> {
        Ok(build_catchup_bundles(
            self.load_snapshots()?,
            self.load_ledger_segments()?,
        ))
    }

    /// Loads materialized catchup bundles for one experiment.
    pub fn load_catchup_bundles_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<MetricsCatchupBundle>, MetricsStoreError> {
        Ok(self
            .load_catchup_bundles()?
            .into_iter()
            .filter(|bundle| &bundle.experiment_id == experiment_id)
            .collect())
    }

    /// Loads the current live metrics refresh event derived from persisted views.
    pub fn load_live_event(
        &self,
        kind: MetricsLiveEventKind,
    ) -> Result<Option<MetricsLiveEvent>, MetricsStoreError> {
        Ok(build_live_event(
            kind,
            &self.load_snapshots()?,
            &self.load_ledger_segments()?,
        ))
    }

    /// Loads head evaluation reports for one head from the durable store.
    pub fn load_head_eval_reports(
        &self,
        head_id: &HeadId,
    ) -> Result<Vec<HeadEvalReport>, MetricsStoreError> {
        Ok(self
            .indexer
            .entries
            .iter()
            .filter_map(|entry| match entry {
                MetricEnvelope::HeadEval(report) if &report.head_id == head_id => {
                    Some(report.clone())
                }
                _ => None,
            })
            .collect())
    }

    /// Loads the raw metric envelopes held by the durable store.
    pub fn load_entries(&self) -> Vec<MetricEnvelope> {
        self.indexer.entries.clone()
    }

    /// Loads peer-window distribution summaries across all persisted experiment revisions.
    pub fn load_peer_window_distributions(
        &self,
    ) -> Result<Vec<PeerWindowDistributionSummary>, MetricsStoreError> {
        Ok(derive_peer_window_distribution_summaries(
            &self
                .indexer
                .entries
                .iter()
                .filter_map(|entry| match entry {
                    MetricEnvelope::PeerWindow(metrics) => Some(metrics.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))
    }

    /// Loads peer-window distribution summaries for one experiment.
    pub fn load_peer_window_distributions_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<PeerWindowDistributionSummary>, MetricsStoreError> {
        Ok(self
            .load_peer_window_distributions()?
            .into_iter()
            .filter(|summary| &summary.experiment_id == experiment_id)
            .collect())
    }

    /// Loads one peer-window distribution drilldown for an experiment revision and base head.
    pub fn load_peer_window_distribution_detail(
        &self,
        experiment_id: &ExperimentId,
        revision_id: &RevisionId,
        base_head_id: &HeadId,
    ) -> Result<Option<PeerWindowDistributionDetail>, MetricsStoreError> {
        Ok(derive_peer_window_distribution_detail_with_limit(
            &self
                .indexer
                .entries
                .iter()
                .filter_map(|entry| match entry {
                    MetricEnvelope::PeerWindow(metrics) => Some(metrics.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            experiment_id,
            revision_id,
            base_head_id,
            self.indexer.config.max_peer_window_detail_windows,
        ))
    }

    /// Loads canonical-head adoption curves across all persisted experiment revisions.
    pub fn load_head_adoption_curves(
        &self,
    ) -> Result<Vec<CanonicalHeadAdoptionCurve>, MetricsStoreError> {
        Ok(derive_canonical_head_adoption_curves(
            &self
                .indexer
                .entries
                .iter()
                .filter_map(|entry| match entry {
                    MetricEnvelope::PeerWindow(metrics) => Some(metrics.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            &self
                .indexer
                .entries
                .iter()
                .filter_map(|entry| match entry {
                    MetricEnvelope::HeadEval(report) => Some(report.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))
    }

    /// Loads canonical-head adoption curves for one experiment.
    pub fn load_head_adoption_curves_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<CanonicalHeadAdoptionCurve>, MetricsStoreError> {
        Ok(self
            .load_head_adoption_curves()?
            .into_iter()
            .filter(|curve| &curve.experiment_id == experiment_id)
            .collect())
    }

    /// Loads latest-canonical visible-head population histograms across persisted revisions.
    pub fn load_visible_head_population_histograms(
        &self,
    ) -> Result<Vec<VisibleHeadPopulationHistogram>, MetricsStoreError> {
        Ok(derive_latest_canonical_head_population_histograms(
            &self
                .indexer
                .entries
                .iter()
                .filter_map(|entry| match entry {
                    MetricEnvelope::PeerWindow(metrics) => Some(metrics.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            &self
                .indexer
                .entries
                .iter()
                .filter_map(|entry| match entry {
                    MetricEnvelope::HeadEval(report) => Some(report.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))
    }

    /// Loads latest-canonical visible-head population histograms for one experiment.
    pub fn load_visible_head_population_histograms_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Result<Vec<VisibleHeadPopulationHistogram>, MetricsStoreError> {
        Ok(self
            .load_visible_head_population_histograms()?
            .into_iter()
            .filter(|histogram| &histogram.experiment_id == experiment_id)
            .collect())
    }

    fn snapshot_path(&self, experiment_id: &ExperimentId, revision_id: &RevisionId) -> PathBuf {
        self.root_dir.join("snapshots").join(format!(
            "{}--{}.json",
            experiment_id.as_str(),
            revision_id.as_str()
        ))
    }

    fn persist_entries_file(&self) -> Result<(), MetricsStoreError> {
        let entries_path = self.root_dir.join("entries.jsonl");
        let mut writer = BufWriter::new(fs::File::create(entries_path)?);
        for entry in &self.indexer.entries {
            serde_json::to_writer(&mut writer, entry)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        Ok(())
    }

    fn ledger_path(&self, experiment_id: &ExperimentId, revision_id: &RevisionId) -> PathBuf {
        self.root_dir.join("ledger").join(format!(
            "{}--{}.json",
            experiment_id.as_str(),
            revision_id.as_str()
        ))
    }
}

fn prune_unknown_files(
    dir: &Path,
    live_paths: &BTreeSet<PathBuf>,
) -> Result<(), MetricsStoreError> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        if !live_paths.contains(&path) {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

fn read_json_paths(dir: &Path) -> Result<Vec<PathBuf>, MetricsStoreError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    Ok(paths)
}

/// Builds per-revision catchup bundles from materialized snapshots and ledger segments.
pub fn build_catchup_bundles(
    snapshots: Vec<MetricsSnapshot>,
    ledger_segments: Vec<MetricsLedgerSegment>,
) -> Vec<MetricsCatchupBundle> {
    snapshots
        .into_iter()
        .map(|snapshot| {
            let experiment_id = snapshot.manifest.experiment_id.clone();
            let revision_id = snapshot.manifest.revision_id.clone();
            let network_id = snapshot.manifest.network_id.clone();
            let segments = ledger_segments
                .iter()
                .filter(|segment| {
                    segment.experiment_id == experiment_id && segment.revision_id == revision_id
                })
                .cloned()
                .collect();
            MetricsCatchupBundle {
                network_id,
                experiment_id,
                revision_id,
                generated_at: snapshot.manifest.created_at,
                snapshot,
                ledger_segments: segments,
            }
        })
        .collect()
}

/// Builds one live metrics refresh event from the currently materialized views.
pub fn build_live_event(
    kind: MetricsLiveEventKind,
    snapshots: &[MetricsSnapshot],
    ledger_segments: &[MetricsLedgerSegment],
) -> Option<MetricsLiveEvent> {
    let network_id = snapshots
        .first()
        .map(|snapshot| snapshot.manifest.network_id.clone())
        .or_else(|| {
            ledger_segments
                .first()
                .map(|segment| segment.network_id.clone())
        })?;

    let mut cursors = snapshots
        .iter()
        .map(|snapshot| {
            let latest_ledger_segment_seq = ledger_segments
                .iter()
                .filter(|segment| {
                    segment.experiment_id == snapshot.manifest.experiment_id
                        && segment.revision_id == snapshot.manifest.revision_id
                })
                .map(|segment| segment.segment_seq)
                .max();
            burn_p2p_core::MetricsSyncCursor {
                experiment_id: snapshot.manifest.experiment_id.clone(),
                revision_id: snapshot.manifest.revision_id.clone(),
                latest_snapshot_seq: Some(snapshot.manifest.snapshot_seq),
                latest_ledger_segment_seq,
                latest_head_id: snapshot.manifest.covers_until_head_id.clone(),
                latest_merge_window_id: snapshot.manifest.covers_until_merge_window_id.clone(),
            }
        })
        .collect::<Vec<_>>();
    cursors.sort_by(|left, right| {
        left.experiment_id
            .cmp(&right.experiment_id)
            .then_with(|| left.revision_id.cmp(&right.revision_id))
    });

    Some(MetricsLiveEvent {
        network_id,
        kind,
        cursors,
        generated_at: Utc::now(),
    })
}
