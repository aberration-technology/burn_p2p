use std::{collections::BTreeMap, fs, io::Cursor};

use anyhow::{Context, ensure};
use burn_p2p::{
    ArtifactBuildSpec, ArtifactKind, AssignmentLease, ChunkingScheme, ContentId, MicroShardId,
    Precision, RunningNode,
};
pub use burn_p2p_e2e::{
    BrowserLiveParticipantConfig, BrowserLiveParticipantHandle, BrowserLiveParticipantResult,
    LiveBrowserDatasetBundle, LiveBrowserDatasetTransport, LiveBrowserEdgeConfig,
    LiveBrowserEdgeServer, LiveBrowserFixtureAsset, LiveBrowserHeadArtifactTransport,
    LiveBrowserProbeManifest, finish_live_browser_participant,
    materialize_live_browser_dataset_bundle, materialize_live_browser_head_artifact_bundle,
    prepare_live_browser_head_artifact_transport, read_live_browser_edge_config,
    start_live_browser_participant, wait_for_live_browser_probe, write_live_browser_edge_config,
    write_live_browser_manifest,
};
use burn_p2p_publish::HeadArtifactView;

use crate::data::{MnistRecord, PreparedMnistData};

pub fn prepare_live_browser_dataset_transport<P>(
    provider: &RunningNode<P>,
    prepared_data: &PreparedMnistData,
    lease: &AssignmentLease,
) -> anyhow::Result<LiveBrowserDatasetTransport> {
    let storage = provider
        .config()
        .storage
        .as_ref()
        .context("live browser dataset bundle requires provider storage")?;
    let cache_root = storage
        .dataset_cache_dir()
        .join(prepared_data.registration.view.dataset_view_id.as_str());
    let manifest_path = cache_root.join("fetch-manifest.json");
    let full_manifest: burn_p2p::ShardFetchManifest = serde_json::from_slice(
        &fs::read(&manifest_path)
            .with_context(|| format!("failed to read {}", manifest_path.display()))?,
    )
    .with_context(|| format!("failed to decode {}", manifest_path.display()))?;
    let lease_entries = full_manifest
        .entries
        .iter()
        .filter(|entry| lease.microshards.contains(&entry.microshard_id))
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        !lease_entries.is_empty(),
        "live browser dataset bundle requires at least one leased microshard"
    );
    let mut asset_records: BTreeMap<String, Vec<MnistRecord>> = BTreeMap::new();
    for entry in &lease_entries {
        let shard_path = cache_root.join(&entry.locator);
        let records: Vec<MnistRecord> = serde_json::from_slice(
            &fs::read(&shard_path)
                .with_context(|| format!("failed to read {}", shard_path.display()))?,
        )
        .with_context(|| format!("failed to decode {}", shard_path.display()))?;
        asset_records.insert(entry.locator.clone(), records);
    }

    let mut assets = BTreeMap::from([(
        "eval-records.json".into(),
        LiveBrowserFixtureAsset::json(&prepared_data.eval_records)
            .context("encode browser eval fixture asset")?,
    )]);
    for (locator, records) in &asset_records {
        assets.insert(
            locator.clone(),
            LiveBrowserFixtureAsset::json(records)
                .with_context(|| format!("encode browser shard fixture asset {locator}"))?,
        );
    }

    let bundle = LiveBrowserDatasetBundle {
        lease_id: lease.lease_id.clone(),
        leased_microshards: lease.microshards.clone(),
        fetch_manifest: LiveBrowserFixtureAsset::json(&burn_p2p::ShardFetchManifest {
            dataset_view_id: full_manifest.dataset_view_id,
            entries: lease_entries,
        })
        .context("encode browser direct fetch manifest")?,
        assets,
    };
    let bundle_bytes =
        serde_json::to_vec_pretty(&bundle).context("encode live browser dataset bundle")?;
    let model_schema_hash = ContentId::derive(&(
        "mnist-browser-data-bundle",
        prepared_data.registration.view.dataset_view_id.as_str(),
        lease.lease_id.as_str(),
        bundle
            .leased_microshards
            .iter()
            .map(MicroShardId::as_str)
            .collect::<Vec<_>>(),
    ))
    .context("derive live browser dataset bundle hash")?;
    let provider_store = provider
        .artifact_store()
        .context("live browser dataset bundle publish requires provider storage")?;
    provider_store.ensure_layout()?;
    let descriptor = provider_store.store_artifact_reader(
        &ArtifactBuildSpec::new(
            ArtifactKind::BrowserDataBundle,
            Precision::Fp32,
            model_schema_hash,
            "burn-p2p:browser-data-bundle+json",
        ),
        Cursor::new(&bundle_bytes),
        ChunkingScheme::new(64 * 1024).context("build browser dataset chunking scheme")?,
    )?;

    let provider_peer_id = provider
        .telemetry()
        .snapshot()
        .local_peer_id
        .context("provider missing local peer id for live browser dataset bundle")?;
    let stored_bytes = provider_store
        .materialize_artifact_bytes(&descriptor)
        .context("materialize published live browser dataset bundle bytes")?;
    let stored_bundle: LiveBrowserDatasetBundle = serde_json::from_slice(&stored_bytes)
        .context("decode published live browser dataset bundle")?;
    ensure!(
        stored_bundle == bundle,
        "published live browser dataset bundle did not match the source bundle"
    );

    Ok(LiveBrowserDatasetTransport {
        upstream_mode: "p2p-signed-peer-bundle".into(),
        provider_peer_id,
        artifact_id: descriptor.artifact_id,
        bundle: stored_bundle,
    })
}

pub fn decode_live_browser_head_artifact_view(bytes: &[u8]) -> anyhow::Result<HeadArtifactView> {
    serde_json::from_slice(bytes).context("decode head artifact view")
}
