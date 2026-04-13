use burn_p2p_core::ExperimentDirectoryEntry;
use serde::{Serialize, de::DeserializeOwned};

/// Generic versioned JSON attachment helper for experiment-directory metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryMetadataAttachment {
    version_metadata_key: String,
    json_metadata_key: String,
    version: String,
}

impl DirectoryMetadataAttachment {
    /// Creates one attachment helper with caller-owned metadata keys.
    pub fn new(
        version_metadata_key: impl Into<String>,
        json_metadata_key: impl Into<String>,
        version: impl Into<String>,
    ) -> Self {
        Self {
            version_metadata_key: version_metadata_key.into(),
            json_metadata_key: json_metadata_key.into(),
            version: version.into(),
        }
    }

    /// Returns the version metadata key.
    pub fn version_metadata_key(&self) -> &str {
        &self.version_metadata_key
    }

    /// Returns the JSON metadata key.
    pub fn json_metadata_key(&self) -> &str {
        &self.json_metadata_key
    }

    /// Returns the expected profile version string.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Attaches the typed payload to one directory entry.
    pub fn attach<T: Serialize>(
        &self,
        entry: &mut ExperimentDirectoryEntry,
        payload: &T,
    ) -> anyhow::Result<()> {
        entry
            .metadata
            .insert(self.version_metadata_key.clone(), self.version.clone());
        entry.metadata.insert(
            self.json_metadata_key.clone(),
            serde_json::to_string(payload)?,
        );
        Ok(())
    }

    /// Returns a cloned entry with the payload attached.
    pub fn attached<T: Serialize>(
        &self,
        mut entry: ExperimentDirectoryEntry,
        payload: &T,
    ) -> anyhow::Result<ExperimentDirectoryEntry> {
        self.attach(&mut entry, payload)?;
        Ok(entry)
    }

    /// Decodes a typed payload from one directory entry.
    pub fn decode<T: DeserializeOwned>(
        &self,
        entry: &ExperimentDirectoryEntry,
    ) -> anyhow::Result<Option<T>> {
        let Some(encoded) = entry.metadata.get(&self.json_metadata_key) else {
            return Ok(None);
        };
        let Some(version) = entry.metadata.get(&self.version_metadata_key) else {
            anyhow::bail!(
                "missing version metadata key `{}` for attached directory payload",
                self.version_metadata_key
            );
        };
        if version != &self.version {
            anyhow::bail!(
                "unsupported directory metadata version `{version}` for key `{}` (expected `{}`)",
                self.json_metadata_key,
                self.version
            );
        }
        Ok(Some(serde_json::from_str(encoded)?))
    }

    /// Returns whether the entry contains any attached JSON payload for this helper.
    pub fn has_payload(&self, entry: &ExperimentDirectoryEntry) -> bool {
        entry.metadata.contains_key(&self.json_metadata_key)
    }
}

/// Finds one directory entry using experiment/revision identity first, then a
/// typed metadata predicate, then any decodable attached payload.
pub fn find_matching_directory_entry_with_predicate<'a, T, F>(
    entries: &'a [ExperimentDirectoryEntry],
    attachment: &DirectoryMetadataAttachment,
    selected_experiment_id: Option<&str>,
    selected_revision_id: Option<&str>,
    metadata_predicate: F,
) -> anyhow::Result<Option<&'a ExperimentDirectoryEntry>>
where
    T: DeserializeOwned,
    F: Fn(&T) -> bool,
{
    let matches_revision = |entry: &&ExperimentDirectoryEntry| {
        selected_revision_id
            .is_none_or(|revision_id| entry.current_revision_id.as_str() == revision_id)
    };

    if let Some(experiment_id) = selected_experiment_id
        && let Some(entry) = entries
            .iter()
            .filter(|entry| entry.experiment_id.as_str() == experiment_id)
            .find(matches_revision)
    {
        return Ok(Some(entry));
    }

    for entry in entries.iter().filter(matches_revision) {
        if let Some(payload) = attachment.decode::<T>(entry)?
            && metadata_predicate(&payload)
        {
            return Ok(Some(entry));
        }
    }

    for entry in entries.iter().filter(matches_revision) {
        if attachment.decode::<T>(entry)?.is_some() {
            return Ok(Some(entry));
        }
    }

    Ok(None)
}

/// Finds one matching directory entry without requiring a custom predicate.
pub fn find_matching_directory_entry<'a, T>(
    entries: &'a [ExperimentDirectoryEntry],
    attachment: &DirectoryMetadataAttachment,
    selected_experiment_id: Option<&str>,
    selected_revision_id: Option<&str>,
) -> anyhow::Result<Option<&'a ExperimentDirectoryEntry>>
where
    T: DeserializeOwned,
{
    find_matching_directory_entry_with_predicate::<T, _>(
        entries,
        attachment,
        selected_experiment_id,
        selected_revision_id,
        |_| true,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use burn_p2p_core::{
        ContentId, DatasetViewId, ExperimentId, ExperimentOptInPolicy,
        ExperimentResourceRequirements, ExperimentScope, ExperimentVisibility, NetworkId,
        PeerRoleSet, RevisionId, StudyId, WorkloadId,
    };
    use serde::{Deserialize, Serialize};

    use super::{
        DirectoryMetadataAttachment, find_matching_directory_entry,
        find_matching_directory_entry_with_predicate,
    };

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Profile {
        kind: String,
        browser_ready: bool,
    }

    fn sample_entry(
        experiment_id: &str,
        revision_id: &str,
    ) -> burn_p2p_core::ExperimentDirectoryEntry {
        burn_p2p_core::ExperimentDirectoryEntry {
            network_id: NetworkId::new("network"),
            study_id: StudyId::new("study"),
            experiment_id: ExperimentId::new(experiment_id),
            workload_id: WorkloadId::new("workload"),
            display_name: experiment_id.into(),
            model_schema_hash: ContentId::new("schema"),
            dataset_view_id: DatasetViewId::new("dataset-view"),
            resource_requirements: ExperimentResourceRequirements {
                minimum_roles: BTreeSet::new(),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: None,
                estimated_download_bytes: 0,
                estimated_window_seconds: 30,
            },
            visibility: ExperimentVisibility::Public,
            opt_in_policy: ExperimentOptInPolicy::Open,
            current_revision_id: RevisionId::new(revision_id),
            current_head_id: None,
            allowed_roles: PeerRoleSet::default_trainer(),
            allowed_scopes: BTreeSet::from([ExperimentScope::Discover]),
            metadata: Default::default(),
        }
    }

    #[test]
    fn attachment_round_trips_versioned_json() {
        let attachment = DirectoryMetadataAttachment::new("profile_version", "profile_json", "v1");
        let mut entry = sample_entry("exp-a", "rev-a");
        let profile = Profile {
            kind: "language".into(),
            browser_ready: true,
        };
        attachment
            .attach(&mut entry, &profile)
            .expect("attach profile");

        assert_eq!(
            attachment
                .decode::<Profile>(&entry)
                .expect("decode profile"),
            Some(profile)
        );
    }

    #[test]
    fn attachment_rejects_mismatched_version() {
        let attachment = DirectoryMetadataAttachment::new("profile_version", "profile_json", "v2");
        let mut entry = sample_entry("exp-a", "rev-a");
        entry.metadata.insert("profile_version".into(), "v1".into());
        entry.metadata.insert(
            "profile_json".into(),
            "{\"kind\":\"language\",\"browser_ready\":true}".into(),
        );
        let error = attachment
            .decode::<Profile>(&entry)
            .expect_err("version mismatch should fail");
        assert!(
            error
                .to_string()
                .contains("unsupported directory metadata version")
        );
    }

    #[test]
    fn matching_prefers_exact_experiment_and_revision() {
        let attachment = DirectoryMetadataAttachment::new("profile_version", "profile_json", "v1");
        let mut entry_a = sample_entry("exp-a", "rev-a");
        let mut entry_b = sample_entry("exp-b", "rev-b");
        attachment
            .attach(
                &mut entry_a,
                &Profile {
                    kind: "wrong".into(),
                    browser_ready: false,
                },
            )
            .expect("attach a");
        attachment
            .attach(
                &mut entry_b,
                &Profile {
                    kind: "right".into(),
                    browser_ready: true,
                },
            )
            .expect("attach b");
        let entries = vec![entry_a, entry_b];
        let matched = find_matching_directory_entry_with_predicate::<Profile, _>(
            &entries,
            &attachment,
            Some("exp-a"),
            Some("rev-a"),
            |profile| profile.browser_ready,
        )
        .expect("resolve")
        .expect("entry");
        assert_eq!(matched.experiment_id.as_str(), "exp-a");
    }

    #[test]
    fn matching_uses_metadata_predicate_before_fallback() {
        let attachment = DirectoryMetadataAttachment::new("profile_version", "profile_json", "v1");
        let mut entry_a = sample_entry("exp-a", "rev-a");
        let mut entry_b = sample_entry("exp-b", "rev-b");
        attachment
            .attach(
                &mut entry_a,
                &Profile {
                    kind: "language".into(),
                    browser_ready: false,
                },
            )
            .expect("attach a");
        attachment
            .attach(
                &mut entry_b,
                &Profile {
                    kind: "language".into(),
                    browser_ready: true,
                },
            )
            .expect("attach b");
        let entries = vec![entry_a, entry_b];
        let matched = find_matching_directory_entry_with_predicate::<Profile, _>(
            &entries,
            &attachment,
            None,
            None,
            |profile| profile.browser_ready,
        )
        .expect("resolve")
        .expect("entry");
        assert_eq!(matched.experiment_id.as_str(), "exp-b");
    }

    #[test]
    fn matching_falls_back_to_any_decodable_profile() {
        let attachment = DirectoryMetadataAttachment::new("profile_version", "profile_json", "v1");
        let mut entry = sample_entry("exp-a", "rev-a");
        attachment
            .attach(
                &mut entry,
                &Profile {
                    kind: "language".into(),
                    browser_ready: false,
                },
            )
            .expect("attach");
        let entries = vec![entry];
        let matched = find_matching_directory_entry::<Profile>(&entries, &attachment, None, None)
            .expect("resolve")
            .expect("entry");
        assert_eq!(matched.experiment_id.as_str(), "exp-a");
    }
}
