//! Shard leasing, dataset fetching, and cache helpers for burn_p2p workloads.
#![forbid(unsafe_code)]

mod adapters;
mod cache;
mod fetch;
mod lease;
mod planner;
mod receipt;
mod registration;

#[derive(Debug, thiserror::Error)]
/// Enumerates the supported dataloader error values.
pub enum DataloaderError {
    #[error("schema error: {0}")]
    /// Uses the schema variant.
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("i/o error: {0}")]
    /// Uses the io variant.
    Io(String),
    #[error("http error: {0}")]
    /// Uses the HTTP variant.
    Http(String),
    #[error("target microshard bytes must be greater than zero")]
    /// Uses the invalid target microshard bytes variant.
    InvalidTargetMicroshardBytes,
    #[error("lease duration seconds must be greater than zero")]
    /// Uses the invalid lease duration variant.
    InvalidLeaseDuration,
    #[error("budget work units must be greater than zero")]
    /// Uses the invalid budget variant.
    InvalidBudget,
    #[error("no microshards are available for planning")]
    /// Uses the no microshards variant.
    NoMicroshards,
    #[error("fetch manifest is missing entry for microshard {0}")]
    /// Uses the missing fetch entry variant.
    MissingFetchEntry(MicroShardId),
    #[error("leased microshard {0} was not found in the microshard plan")]
    /// Uses the unknown leased micro shard variant.
    UnknownLeasedMicroShard(MicroShardId),
    #[error("cached microshard {microshard_id} failed hash verification")]
    /// Uses the hash mismatch variant.
    HashMismatch {
        /// The microshard ID.
        microshard_id: MicroShardId,
    },
    #[error("adapter {0:?} is not supported by the disk cache fetch path")]
    /// Uses the unsupported adapter variant.
    UnsupportedAdapter(UpstreamAdapter),
}

/// Defines behavior for burn data loader adapter.
pub trait BurnDataLoaderAdapter<B> {
    /// Defines the batch alias.
    type Batch;
    /// Defines the error alias.
    type Error;

    /// Performs the dataset view operation.
    fn dataset_view(&self) -> &DatasetView;
    /// Performs the load lease operation.
    fn load_lease(&mut self, lease: &AssignmentLease) -> Result<Vec<Self::Batch>, Self::Error>;
}

pub use cache::{CachedMicroShard, CachedMicroShardLoader, ShardCache};
pub use fetch::{ShardFetchEntry, ShardFetchManifest};
pub use lease::{
    LeaseCache, LeasePlanner, LeasePlannerConfig, LeaseSelection, PlannedLease, ShardAwareSampler,
    ShardCostModel,
};
pub use planner::{MicroShardPlan, MicroShardPlanner, MicroShardPlannerConfig};
pub use receipt::DataReceiptBuilder;
pub use registration::{DatasetRegistration, DatasetSizing, UpstreamAdapter};

use burn_p2p_core::{AssignmentLease, DatasetView, MicroShardId};

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        io::{BufRead, BufReader, Write},
        net::TcpListener,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
        time::Duration,
    };

    use burn_p2p_core::{DatasetId, WindowId};
    use chrono::Utc;
    use tempfile::tempdir;

    use crate::{
        CachedMicroShardLoader, DataReceiptBuilder, DatasetRegistration, DatasetSizing, LeaseCache,
        LeasePlanner, LeasePlannerConfig, MicroShardPlanner, MicroShardPlannerConfig,
        ShardAwareSampler, ShardCache, ShardFetchManifest, UpstreamAdapter,
    };

    fn dataset_view() -> burn_p2p_core::DatasetView {
        burn_p2p_core::DatasetView {
            dataset_view_id: burn_p2p_core::DatasetViewId::new("view-1"),
            dataset_id: DatasetId::new("dataset-1"),
            preprocessing_hash: burn_p2p_core::ContentId::new("prep-1"),
            tokenizer_hash: Some(burn_p2p_core::ContentId::new("tok-1")),
            manifest_hash: burn_p2p_core::ContentId::new("manifest-1"),
            metadata: BTreeMap::new(),
        }
    }

    fn microshard_plan() -> crate::MicroShardPlan {
        MicroShardPlanner::new(MicroShardPlannerConfig {
            target_microshard_bytes: 10,
            min_microshards: 2,
            max_microshards: 2,
        })
        .expect("planner")
        .plan(
            &dataset_view(),
            DatasetSizing {
                total_examples: 20,
                total_tokens: 20,
                total_bytes: 20,
            },
        )
        .expect("plan")
    }

    fn make_registration(upstream: UpstreamAdapter) -> DatasetRegistration {
        DatasetRegistration {
            manifest: burn_p2p_core::DatasetManifest {
                dataset_id: DatasetId::new("dataset-1"),
                source_uri: "test".into(),
                format: "microshards".into(),
                manifest_hash: burn_p2p_core::ContentId::new("manifest"),
                metadata: BTreeMap::new(),
            },
            view: dataset_view(),
            upstream,
        }
    }

    #[test]
    fn planner_splits_dataset_into_deterministic_microshards() {
        let planner = MicroShardPlanner::new(MicroShardPlannerConfig {
            target_microshard_bytes: 100,
            min_microshards: 1,
            max_microshards: 32,
        })
        .expect("planner");

        let view = dataset_view();
        let first = planner
            .plan(
                &view,
                DatasetSizing {
                    total_examples: 10,
                    total_tokens: 1000,
                    total_bytes: 250,
                },
            )
            .expect("plan");
        let second = planner
            .plan(
                &view,
                DatasetSizing {
                    total_examples: 10,
                    total_tokens: 1000,
                    total_bytes: 250,
                },
            )
            .expect("plan");

        assert_eq!(first, second);
        assert_eq!(first.microshards.len(), 3);
        assert_eq!(
            first
                .microshards
                .iter()
                .map(|microshard| microshard.estimated_bytes)
                .sum::<u64>(),
            250
        );
    }

    #[test]
    fn planner_clamps_skewed_large_dataset_to_maximum_microshards() {
        let planner = MicroShardPlanner::new(MicroShardPlannerConfig {
            target_microshard_bytes: 4 * 1024 * 1024,
            min_microshards: 8,
            max_microshards: 64,
        })
        .expect("planner");

        let plan = planner
            .plan(
                &dataset_view(),
                DatasetSizing {
                    total_examples: 512,
                    total_tokens: 128 * 1024 * 1024,
                    total_bytes: 8 * 1024 * 1024 * 1024,
                },
            )
            .expect("plan");

        assert_eq!(plan.microshards.len(), 64);
        assert_eq!(
            plan.microshards
                .iter()
                .map(|microshard| microshard.estimated_bytes)
                .sum::<u64>(),
            8 * 1024 * 1024 * 1024
        );
        assert!(plan.microshards.iter().all(|microshard| {
            microshard.estimated_examples > 0
                && microshard.estimated_tokens > 0
                && microshard.estimated_bytes > 0
        }));
    }

    #[test]
    fn lease_planner_uses_stable_selection_and_builds_assignment() {
        let microshards = MicroShardPlanner::default()
            .plan(
                &dataset_view(),
                DatasetSizing {
                    total_examples: 128,
                    total_tokens: 16_384,
                    total_bytes: 256 * 1024 * 1024,
                },
            )
            .expect("plan")
            .microshards;

        let planner = LeasePlanner::new(LeasePlannerConfig {
            lease_duration_seconds: 120,
            max_microshards_per_lease: 8,
            ..LeasePlannerConfig::default()
        })
        .expect("planner");

        let planned = planner
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &dataset_view(),
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(9),
                Utc::now(),
                64,
                &microshards,
            )
            .expect("lease");

        assert!(!planned.selection.microshards.is_empty());
        assert_eq!(
            planned.lease.microshards.len(),
            planned.selection.microshards.len()
        );
        assert_eq!(planned.lease.budget_work_units, 64);
        assert_eq!(planned.lease.window_id, WindowId(9));
    }

    #[test]
    fn lease_cache_indexes_by_window_and_peer() {
        let planner = LeasePlanner::default();
        let microshards = MicroShardPlanner::default()
            .plan(
                &dataset_view(),
                DatasetSizing {
                    total_examples: 32,
                    total_tokens: 4_096,
                    total_bytes: 64 * 1024 * 1024,
                },
            )
            .expect("plan")
            .microshards;
        let planned = planner
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &dataset_view(),
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(2),
                Utc::now(),
                32,
                &microshards,
            )
            .expect("lease");

        let mut cache = LeaseCache::default();
        cache.insert(planned.lease.clone());

        assert!(
            cache
                .get(WindowId(2), &burn_p2p_core::PeerId::new("peer-1"))
                .is_some()
        );
        assert_eq!(cache.evict_before(WindowId(2)), 0);
        assert_eq!(cache.evict_before(WindowId(3)), 1);
    }

    #[test]
    fn sampler_order_is_deterministic_for_a_lease() {
        let planner = LeasePlanner::default();
        let microshards = MicroShardPlanner::default()
            .plan(
                &dataset_view(),
                DatasetSizing {
                    total_examples: 64,
                    total_tokens: 8_192,
                    total_bytes: 96 * 1024 * 1024,
                },
            )
            .expect("plan")
            .microshards;
        let planned = planner
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &dataset_view(),
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(7),
                Utc::now(),
                32,
                &microshards,
            )
            .expect("lease");

        let first = ShardAwareSampler::from_lease(&planned.lease).expect("sampler");
        let second = ShardAwareSampler::from_lease(&planned.lease).expect("sampler");

        assert_eq!(first.ordered_microshards(), second.ordered_microshards());
    }

    #[test]
    fn data_receipt_builder_tracks_completed_microshards() {
        let _source = UpstreamAdapter::Local {
            root: "/tmp/dataset".into(),
        };
        let planner = LeasePlanner::default();
        let microshards = MicroShardPlanner::default()
            .plan(
                &dataset_view(),
                DatasetSizing {
                    total_examples: 16,
                    total_tokens: 2_048,
                    total_bytes: 32 * 1024 * 1024,
                },
            )
            .expect("plan")
            .microshards;
        let planned = planner
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &dataset_view(),
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(1),
                Utc::now(),
                16,
                &microshards,
            )
            .expect("lease");

        let receipt = DataReceiptBuilder::accepted(16, 2_048)
            .build(&planned.lease, Utc::now())
            .expect("receipt");

        assert_eq!(receipt.lease_id, planned.lease.lease_id);
        assert_eq!(receipt.microshards, planned.lease.microshards);
    }

    #[test]
    fn local_cache_fetches_only_leased_microshards() {
        let upstream_dir = tempdir().expect("upstream dir");
        let cache_dir = tempdir().expect("cache dir");
        let plan = microshard_plan();
        let bytes_for_ordinal = |ordinal| format!("local-shard-{ordinal}").into_bytes();
        let manifest = ShardFetchManifest::from_microshards(
            &plan.dataset_view,
            &plan.microshards,
            bytes_for_ordinal,
        );
        fs::write(
            upstream_dir.path().join("fetch-manifest.json"),
            serde_json::to_vec_pretty(&manifest).expect("manifest json"),
        )
        .expect("write manifest");
        for entry in &manifest.entries {
            fs::write(
                upstream_dir.path().join(&entry.locator),
                bytes_for_ordinal(entry.ordinal),
            )
            .expect("write shard");
        }

        let registration = make_registration(UpstreamAdapter::Local {
            root: upstream_dir.path().display().to_string(),
        });
        let lease = LeasePlanner::default()
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &plan.dataset_view,
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(1),
                Utc::now(),
                1,
                &plan.microshards[..1],
            )
            .expect("lease")
            .lease;

        let mut loader = CachedMicroShardLoader::new(
            registration,
            plan.clone(),
            ShardCache::new(cache_dir.path()),
        );
        let cached = <CachedMicroShardLoader as crate::BurnDataLoaderAdapter<()>>::load_lease(
            &mut loader,
            &lease,
        )
        .expect("load lease");

        assert_eq!(cached.len(), 1);
        assert!(cached[0].path.exists());
        assert!(
            !cache_dir
                .path()
                .join(plan.dataset_view.dataset_view_id.as_str())
                .join("00001.bin")
                .exists()
        );
    }

    #[test]
    fn http_cache_reuses_manifest_and_shard_without_refetching() {
        let upstream_dir = tempdir().expect("upstream dir");
        let cache_dir = tempdir().expect("cache dir");
        let plan = microshard_plan();
        let bytes_for_ordinal = |ordinal| format!("http-shard-{ordinal}").into_bytes();
        let manifest = ShardFetchManifest::from_microshards(
            &plan.dataset_view,
            &plan.microshards,
            bytes_for_ordinal,
        );
        fs::write(
            upstream_dir.path().join("fetch-manifest.json"),
            serde_json::to_vec_pretty(&manifest).expect("manifest json"),
        )
        .expect("write manifest");
        for entry in &manifest.entries {
            fs::write(
                upstream_dir.path().join(&entry.locator),
                bytes_for_ordinal(entry.ordinal),
            )
            .expect("write shard");
        }

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let addr = listener.local_addr().expect("addr");
        let hits = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let root = upstream_dir.path().to_path_buf();
        let hits_for_thread = Arc::clone(&hits);
        let stop_for_thread = Arc::clone(&stop);
        let server = thread::spawn(move || {
            while !stop_for_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        hits_for_thread.fetch_add(1, Ordering::Relaxed);
                        let mut reader = BufReader::new(stream.try_clone().expect("clone"));
                        let mut line = String::new();
                        reader.read_line(&mut line).expect("request line");
                        let path = line
                            .split_whitespace()
                            .nth(1)
                            .unwrap_or("/")
                            .trim_start_matches('/');
                        loop {
                            let mut header = String::new();
                            reader.read_line(&mut header).expect("header");
                            if header == "\r\n" || header.is_empty() {
                                break;
                            }
                        }
                        let body = fs::read(root.join(path)).expect("read served file");
                        stream
                            .write_all(
                                format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    body.len()
                                )
                                .as_bytes(),
                            )
                            .expect("write head");
                        stream.write_all(&body).expect("write body");
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("server error: {error}"),
                }
            }
        });

        let registration = make_registration(UpstreamAdapter::Http {
            base_url: format!("http://{addr}"),
        });
        let lease = LeasePlanner::default()
            .plan_lease(
                burn_p2p_core::NetworkId::new("net-1"),
                burn_p2p_core::StudyId::new("study-1"),
                burn_p2p_core::ExperimentId::new("exp-1"),
                burn_p2p_core::RevisionId::new("rev-1"),
                &plan.dataset_view,
                burn_p2p_core::PeerId::new("peer-1"),
                WindowId(1),
                Utc::now(),
                1,
                &plan.microshards[..1],
            )
            .expect("lease")
            .lease;
        let cache = ShardCache::new(cache_dir.path());

        cache
            .fetch_lease_microshards(&registration, &plan, &lease)
            .expect("first fetch");
        cache
            .fetch_lease_microshards(&registration, &plan, &lease)
            .expect("second fetch");

        stop.store(true, Ordering::Relaxed);
        let _ = server.join();

        assert_eq!(hits.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn cache_pressure_evict_except_preserves_only_active_microshards() {
        let upstream_dir = tempdir().expect("upstream dir");
        let cache_dir = tempdir().expect("cache dir");
        let plan = MicroShardPlanner::new(MicroShardPlannerConfig {
            target_microshard_bytes: 4,
            min_microshards: 16,
            max_microshards: 16,
        })
        .expect("planner")
        .plan(
            &dataset_view(),
            DatasetSizing {
                total_examples: 512,
                total_tokens: 512,
                total_bytes: 512,
            },
        )
        .expect("plan");
        let bytes_for_ordinal = |ordinal| format!("pressure-shard-{ordinal}").into_bytes();
        let manifest = ShardFetchManifest::from_microshards(
            &plan.dataset_view,
            &plan.microshards,
            bytes_for_ordinal,
        );
        fs::write(
            upstream_dir.path().join("fetch-manifest.json"),
            serde_json::to_vec_pretty(&manifest).expect("manifest json"),
        )
        .expect("write manifest");
        for entry in &manifest.entries {
            fs::write(
                upstream_dir.path().join(&entry.locator),
                bytes_for_ordinal(entry.ordinal),
            )
            .expect("write shard");
        }

        let registration = make_registration(UpstreamAdapter::Local {
            root: upstream_dir.path().display().to_string(),
        });
        let lease = LeasePlanner::new(LeasePlannerConfig {
            max_microshards_per_lease: 16,
            ..LeasePlannerConfig::default()
        })
        .expect("lease planner")
        .plan_lease(
            burn_p2p_core::NetworkId::new("net-1"),
            burn_p2p_core::StudyId::new("study-1"),
            burn_p2p_core::ExperimentId::new("exp-1"),
            burn_p2p_core::RevisionId::new("rev-1"),
            &plan.dataset_view,
            burn_p2p_core::PeerId::new("peer-1"),
            WindowId(3),
            Utc::now(),
            4096,
            &plan.microshards,
        )
        .expect("lease")
        .lease;
        let cache = ShardCache::new(cache_dir.path());

        let fetched = cache
            .fetch_lease_microshards(&registration, &plan, &lease)
            .expect("fetch lease");
        assert_eq!(fetched.len(), plan.microshards.len());

        let keep = lease
            .microshards
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>();
        let removed = cache
            .evict_except(&plan.dataset_view, &keep)
            .expect("evict except");

        assert_eq!(removed, plan.microshards.len() - keep.len());
        let dataset_dir = cache.dataset_dir(&plan.dataset_view);
        let remaining_bin_count = fs::read_dir(&dataset_dir)
            .expect("read dataset dir")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .and_then(|value| value.to_str())
                    .is_some_and(|ext| ext == "bin")
            })
            .count();
        assert_eq!(remaining_bin_count, keep.len());
    }
}
