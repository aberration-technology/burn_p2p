use burn_p2p_views::{NodeAppClientView, NodeAppStaticBootstrap, NodeAppSurface};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefreshTone {
    Ready,
    Refreshing,
    Healthy,
    #[cfg(all(target_arch = "wasm32", feature = "web-client"))]
    Error,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NodeAppUiState {
    pub bootstrap: NodeAppStaticBootstrap,
    pub edge_base_url: String,
    pub active_surface: NodeAppSurface,
    pub refresh_status: String,
    pub refresh_detail: String,
    pub refresh_tone: RefreshTone,
    pub view: Option<NodeAppClientView>,
}

impl NodeAppUiState {
    pub(crate) fn new(bootstrap: NodeAppStaticBootstrap, edge_base_url: String) -> Self {
        Self {
            active_surface: bootstrap.default_surface,
            refresh_status: "ready".into(),
            refresh_detail: "starting local runtime.".into(),
            refresh_tone: RefreshTone::Ready,
            bootstrap,
            edge_base_url,
            view: None,
        }
    }

    #[cfg(all(target_arch = "wasm32", feature = "web-client"))]
    pub(crate) fn boot_error(message: impl Into<String>) -> Self {
        Self {
            bootstrap: NodeAppStaticBootstrap {
                app_name: "burn_p2p".into(),
                asset_base_url: String::new(),
                module_entry_path: String::new(),
                stylesheet_path: None,
                default_edge_url: None,
                default_surface: NodeAppSurface::Viewer,
                refresh_interval_ms: 15_000,
            },
            edge_base_url: String::new(),
            active_surface: NodeAppSurface::Viewer,
            refresh_status: "degraded".into(),
            refresh_detail: message.into(),
            refresh_tone: RefreshTone::Error,
            view: None,
        }
    }

    pub(crate) fn start_refresh(&mut self) {
        self.refresh_status = "syncing".into();
        self.refresh_detail = if self.edge_base_url.is_empty() {
            "waiting for an edge.".into()
        } else {
            format!("syncing {}", self.edge_base_url)
        };
        self.refresh_tone = RefreshTone::Refreshing;
    }

    pub(crate) fn apply_view(&mut self, view: NodeAppClientView) {
        if self.view.is_none() && self.active_surface == self.bootstrap.default_surface {
            self.active_surface = view.default_surface;
        }
        self.refresh_status = "live".into();
        self.refresh_detail = view.runtime_detail.clone();
        self.refresh_tone = RefreshTone::Healthy;
        self.edge_base_url = view.network.edge_base_url.clone();
        self.view = Some(view);
    }

    #[cfg(all(target_arch = "wasm32", feature = "web-client"))]
    pub(crate) fn apply_error(&mut self, message: impl Into<String>) {
        self.refresh_status = "degraded".into();
        self.refresh_detail = message.into();
        self.refresh_tone = RefreshTone::Error;
    }

    pub(crate) fn activate_surface(&mut self, surface: NodeAppSurface) {
        self.active_surface = surface;
    }

    pub(crate) fn view_or_placeholder(&self) -> NodeAppClientView {
        self.view.clone().unwrap_or_else(|| NodeAppClientView {
            network_id: "loading".into(),
            default_surface: self.active_surface,
            runtime_label: "starting".into(),
            runtime_detail: "connecting to the local runtime.".into(),
            capability_summary: "checking".into(),
            session_label: "guest".into(),
            viewer: burn_p2p_views::BrowserAppViewerView {
                visible_experiments: 0,
                visible_heads: 0,
                leaderboard_entries: 0,
                signed_directory_ready: false,
                signed_leaderboard_ready: false,
                experiments_preview: Vec::new(),
                leaderboard_preview: Vec::new(),
            },
            validation: burn_p2p_views::BrowserAppValidationView {
                validate_available: false,
                can_validate: false,
                current_head_id: None,
                metrics_sync_at: None,
                pending_receipts: 0,
                validation_status: None,
                checked_chunks: None,
                emitted_receipt_id: None,
                evaluation_summary: None,
                metric_preview: Vec::new(),
            },
            training: burn_p2p_views::BrowserAppTrainingView {
                train_available: false,
                can_train: false,
                active_assignment: None,
                active_training_lease: None,
                slice_status: "waiting for work".into(),
                latest_head_id: None,
                cached_chunk_artifacts: 0,
                cached_microshards: 0,
                pending_receipts: 0,
                max_window_secs: None,
                last_window_secs: None,
                optimizer_steps: None,
                accepted_samples: None,
                slice_target_samples: None,
                slice_remaining_samples: None,
                last_loss: None,
                publish_latency_ms: None,
                throughput_summary: None,
                last_artifact_id: None,
                last_receipt_id: None,
            },
            network: burn_p2p_views::BrowserAppNetworkView {
                edge_base_url: self.edge_base_url.clone(),
                transport: "starting".into(),
                node_state: "starting".into(),
                direct_peers: 0,
                observed_peers: 0,
                estimated_network_size: 0,
                accepted_receipts: 0,
                certified_merges: 0,
                in_flight_transfers: 0,
                network_note: "waiting for the first sync.".into(),
                metrics_live_ready: false,
                last_directory_sync_at: None,
                last_error: None,
                performance: None,
                diffusion: None,
            },
            selected_experiment: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ui_state_promotes_surface_from_latest_view() {
        let bootstrap = NodeAppStaticBootstrap {
            app_name: "burn_p2p".into(),
            asset_base_url: String::new(),
            module_entry_path: "browser-app-loader.js".into(),
            stylesheet_path: None,
            default_edge_url: Some("https://edge.example".into()),
            default_surface: NodeAppSurface::Viewer,
            refresh_interval_ms: 15_000,
        };
        let mut state = NodeAppUiState::new(bootstrap, "https://edge.example".into());

        state.apply_view(NodeAppClientView {
            network_id: "net".into(),
            default_surface: NodeAppSurface::Train,
            runtime_label: "trainer".into(),
            runtime_detail: "active assignment".into(),
            capability_summary: "webgpu".into(),
            session_label: "viewer-only".into(),
            viewer: burn_p2p_views::BrowserAppViewerView {
                visible_experiments: 1,
                visible_heads: 1,
                leaderboard_entries: 1,
                signed_directory_ready: true,
                signed_leaderboard_ready: true,
                experiments_preview: Vec::new(),
                leaderboard_preview: Vec::new(),
            },
            validation: burn_p2p_views::BrowserAppValidationView {
                validate_available: true,
                can_validate: true,
                current_head_id: Some("head-1".into()),
                metrics_sync_at: None,
                pending_receipts: 0,
                validation_status: None,
                checked_chunks: None,
                emitted_receipt_id: None,
                evaluation_summary: None,
                metric_preview: Vec::new(),
            },
            training: burn_p2p_views::BrowserAppTrainingView {
                train_available: true,
                can_train: true,
                active_assignment: Some("exp/rev".into()),
                active_training_lease: None,
                slice_status: "2 microshards cached · 128 left in slice".into(),
                latest_head_id: Some("head-1".into()),
                cached_chunk_artifacts: 2,
                cached_microshards: 4,
                pending_receipts: 0,
                max_window_secs: Some(30),
                last_window_secs: Some(30),
                optimizer_steps: Some(12),
                accepted_samples: Some(512),
                slice_target_samples: Some(640),
                slice_remaining_samples: Some(128),
                last_loss: Some("0.2450".into()),
                publish_latency_ms: Some(320),
                throughput_summary: Some("52.1 sample/s".into()),
                last_artifact_id: Some("artifact-1".into()),
                last_receipt_id: Some("receipt-1".into()),
            },
            network: burn_p2p_views::BrowserAppNetworkView {
                edge_base_url: "https://edge.example".into(),
                transport: "webtransport".into(),
                node_state: "trainer".into(),
                direct_peers: 4,
                observed_peers: 12,
                estimated_network_size: 128,
                accepted_receipts: 11,
                certified_merges: 3,
                in_flight_transfers: 1,
                network_note: "4 direct peers and an estimated 128 peers across the wider network."
                    .into(),
                metrics_live_ready: true,
                last_directory_sync_at: Some("2026-04-04T18:00:00Z".into()),
                diffusion: None,
                last_error: None,
                performance: Some(burn_p2p_views::BrowserAppPerformanceView {
                    scope_summary: "4 peer(s) · 8 train window(s) · 3 eval report(s)".into(),
                    captured_at: "2026-04-04T18:00:00Z".into(),
                    training_throughput: "84.2 work/s".into(),
                    validation_throughput: "512 sample/s".into(),
                    wait_time: "820 ms".into(),
                    idle_time: "6.2s".into(),
                }),
            },
            selected_experiment: None,
        });

        assert_eq!(state.active_surface, NodeAppSurface::Train);
        assert_eq!(state.refresh_status, "live");
        assert_eq!(
            state.view_or_placeholder().network.edge_base_url,
            "https://edge.example"
        );
    }
}
