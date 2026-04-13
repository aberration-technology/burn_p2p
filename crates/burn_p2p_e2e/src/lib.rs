//! Public e2e and browser-validation facade for `burn_p2p`.
#![forbid(unsafe_code)]

mod live_browser;

use anyhow::{Context, ensure};
use burn_p2p::{ExperimentDirectoryEntry, HeadDescriptor};
use burn_p2p_core::{DatasetViewId, ExperimentId, HeadId, RevisionId};

pub use burn_p2p_testkit::portal_capture::{
    BrowserPortalCaptureSpec, PortalCaptureInteraction as BrowserScenarioInteraction,
    PortalCaptureManifest as BrowserScenarioManifest,
    PortalCaptureScenario as BrowserScenarioExport,
    PortalCaptureViewport as BrowserScenarioViewport, write_browser_portal_capture_bundle,
    write_portal_capture_bundle,
};
pub use live_browser::{
    BrowserLiveParticipantConfig, BrowserLiveParticipantHandle, BrowserLiveParticipantResult,
    LiveBrowserDatasetBundle, LiveBrowserDatasetTransport, LiveBrowserEdgeConfig,
    LiveBrowserFixtureAsset, LiveBrowserHeadArtifactTransport, LiveBrowserProbeManifest,
    finish_live_browser_participant, start_live_browser_participant,
};
#[cfg(not(target_arch = "wasm32"))]
pub use live_browser::{
    LiveBrowserEdgeServer, materialize_live_browser_dataset_bundle,
    materialize_live_browser_head_artifact_bundle, prepare_live_browser_head_artifact_transport,
    read_live_browser_edge_config, wait_for_live_browser_probe, write_live_browser_edge_config,
    write_live_browser_manifest,
};

/// Ensures the directory still exposes the requested experiment revision.
pub fn assert_directory_contains_revision<'a>(
    entries: &'a [ExperimentDirectoryEntry],
    experiment_id: &ExperimentId,
    revision_id: &RevisionId,
) -> anyhow::Result<&'a ExperimentDirectoryEntry> {
    entries
        .iter()
        .find(|entry| {
            entry.experiment_id == *experiment_id && entry.current_revision_id == *revision_id
        })
        .with_context(|| {
            format!(
                "directory did not expose revision {} for experiment {}",
                revision_id.as_str(),
                experiment_id.as_str()
            )
        })
}

/// Ensures the directory entry still targets the expected dataset view.
pub fn assert_directory_entry_dataset_view(
    entry: &ExperimentDirectoryEntry,
    dataset_view_id: &DatasetViewId,
) -> anyhow::Result<()> {
    ensure!(
        entry.dataset_view_id == *dataset_view_id,
        "directory entry {} targeted dataset view {}, expected {}",
        entry.experiment_id.as_str(),
        entry.dataset_view_id.as_str(),
        dataset_view_id.as_str(),
    );
    Ok(())
}

/// Ensures one head descriptor is visible for the requested experiment revision.
pub fn assert_head_visible<'a>(
    heads: &'a [HeadDescriptor],
    experiment_id: &ExperimentId,
    revision_id: &RevisionId,
    head_id: &HeadId,
) -> anyhow::Result<&'a HeadDescriptor> {
    heads
        .iter()
        .find(|head| {
            head.experiment_id == *experiment_id
                && head.revision_id == *revision_id
                && head.head_id == *head_id
        })
        .with_context(|| {
            format!(
                "head {} for {}/{} was not visible",
                head_id.as_str(),
                experiment_id.as_str(),
                revision_id.as_str(),
            )
        })
}
