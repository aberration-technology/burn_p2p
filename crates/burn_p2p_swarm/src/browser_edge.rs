use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Represents a browser edge control plane paths.
pub struct BrowserEdgeControlPlanePaths {
    /// The directory path.
    pub directory_path: String,
    /// The heads path.
    pub heads_path: String,
    /// The latest live metrics event path.
    pub metrics_live_latest_path: String,
}

impl Default for BrowserEdgeControlPlanePaths {
    fn default() -> Self {
        Self {
            directory_path: "/directory".into(),
            heads_path: "/heads".into(),
            metrics_live_latest_path: "/metrics/live/latest".into(),
        }
    }
}

#[derive(Clone, Debug)]
/// Represents a browser edge control plane client.
pub struct BrowserEdgeControlPlaneClient {
    http: reqwest::Client,
    base_url: String,
    network_id: NetworkId,
    paths: BrowserEdgeControlPlanePaths,
}

impl BrowserEdgeControlPlaneClient {
    /// Creates a new value.
    pub fn new(base_url: impl Into<String>, network_id: NetworkId) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            network_id,
            paths: BrowserEdgeControlPlanePaths::default(),
        }
    }

    /// Returns a copy configured with the paths.
    pub fn with_paths(mut self, paths: BrowserEdgeControlPlanePaths) -> Self {
        self.paths = paths;
        self
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        session_id: Option<&ContentId>,
    ) -> Result<T, SwarmError> {
        let request = session_id.map_or_else(
            || self.http.get(format!("{}{}", self.base_url, path)),
            |session_id| {
                self.http
                    .get(format!("{}{}", self.base_url, path))
                    .header("x-session-id", session_id.as_str())
            },
        );
        request
            .send()
            .await
            .map_err(|error| SwarmError::Request(error.to_string()))?
            .error_for_status()
            .map_err(|error| SwarmError::Request(error.to_string()))?
            .json::<T>()
            .await
            .map_err(|error| SwarmError::Request(error.to_string()))
    }

    async fn get_optional_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        session_id: Option<&ContentId>,
    ) -> Result<Option<T>, SwarmError> {
        let request = session_id.map_or_else(
            || self.http.get(format!("{}{}", self.base_url, path)),
            |session_id| {
                self.http
                    .get(format!("{}{}", self.base_url, path))
                    .header("x-session-id", session_id.as_str())
            },
        );
        let response = request
            .send()
            .await
            .map_err(|error| SwarmError::Request(error.to_string()))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = response
            .error_for_status()
            .map_err(|error| SwarmError::Request(error.to_string()))?;
        response
            .json::<T>()
            .await
            .map(Some)
            .map_err(|error| SwarmError::Request(error.to_string()))
    }

    /// Fetches the directory.
    pub async fn fetch_directory(
        &self,
        session_id: Option<&ContentId>,
    ) -> Result<Vec<ExperimentDirectoryEntry>, SwarmError> {
        self.get_json(&self.paths.directory_path, session_id).await
    }

    /// Fetches the heads.
    pub async fn fetch_heads(&self) -> Result<Vec<HeadDescriptor>, SwarmError> {
        self.get_json::<Vec<HeadDescriptor>>(&self.paths.heads_path, None)
            .await
    }

    /// Fetches the latest live metrics event exposed by the browser edge, when present.
    pub async fn fetch_latest_metrics_event(&self) -> Result<Option<MetricsLiveEvent>, SwarmError> {
        self.get_optional_json(&self.paths.metrics_live_latest_path, None)
            .await
    }

    /// Polls the latest metrics-event surface multiple times and returns only
    /// newly observed event frames.
    pub async fn collect_metrics_events_by_polling(
        &self,
        rounds: usize,
        poll_interval_ms: u64,
    ) -> Result<Vec<MetricsLiveEvent>, SwarmError> {
        if rounds == 0 {
            return Ok(Vec::new());
        }

        let mut events = Vec::new();
        for round in 0..rounds {
            if let Some(event) = self.fetch_latest_metrics_event().await?
                && events.last() != Some(&event)
            {
                events.push(event);
            }
            if round + 1 < rounds && poll_interval_ms > 0 {
                #[cfg(not(target_arch = "wasm32"))]
                std::thread::sleep(std::time::Duration::from_millis(poll_interval_ms));
                #[cfg(target_arch = "wasm32")]
                let _ = poll_interval_ms;
            }
        }
        Ok(events)
    }

    /// Fetches the snapshot.
    pub async fn fetch_snapshot(
        &self,
        session_id: Option<&ContentId>,
    ) -> Result<ControlPlaneSnapshot, SwarmError> {
        let generated_at = Utc::now();
        let entries = self.fetch_directory(session_id).await?;
        let heads = self.fetch_heads().await?;
        let live_metrics_event = self.fetch_latest_metrics_event().await?;

        let mut snapshot = ControlPlaneSnapshot {
            directory_announcements: vec![ExperimentDirectoryAnnouncement {
                network_id: self.network_id.clone(),
                entries,
                announced_at: generated_at,
            }],
            ..Default::default()
        };

        for head in heads {
            let overlay = OverlayTopic::experiment(
                self.network_id.clone(),
                head.study_id.clone(),
                head.experiment_id.clone(),
                OverlayChannel::Heads,
            )?;
            snapshot.head_announcements.push(HeadAnnouncement {
                overlay,
                provider_peer_id: None,
                head,
                announced_at: generated_at,
            });
        }

        if let Some(event) = live_metrics_event {
            let study_by_experiment = snapshot
                .directory_announcements
                .iter()
                .flat_map(|announcement| announcement.entries.iter())
                .map(|entry| (entry.experiment_id.clone(), entry.study_id.clone()))
                .collect::<std::collections::BTreeMap<_, _>>();
            for cursor in &event.cursors {
                let Some(study_id) = study_by_experiment.get(&cursor.experiment_id).cloned() else {
                    continue;
                };
                let Ok(overlay) = OverlayTopic::experiment(
                    self.network_id.clone(),
                    study_id,
                    cursor.experiment_id.clone(),
                    OverlayChannel::Metrics,
                ) else {
                    continue;
                };
                push_metrics_announcement(
                    &mut snapshot.metrics_announcements,
                    MetricsAnnouncement {
                        overlay,
                        event: MetricsLiveEvent {
                            network_id: event.network_id.clone(),
                            kind: event.kind.clone(),
                            cursors: vec![cursor.clone()],
                            generated_at: event.generated_at,
                        },
                        peer_window_hints: Vec::new(),
                    },
                );
            }
        }

        Ok(snapshot)
    }
}
