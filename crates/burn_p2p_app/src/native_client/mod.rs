use std::time::Duration;

use burn_p2p::{
    ExperimentDirectoryEntry, ExperimentId, NodeAppSelection, RevisionId, RunningNode,
    TelemetryHandle,
};
use burn_p2p_views::{NodeAppClientView, NodeAppStaticBootstrap, NodeAppSurface};
use dioxus::prelude::*;
use tokio::time::sleep;

use crate::app::NodeAppShell;
use crate::app::NodeAppUiState;

fn default_refresh_interval_ms() -> u64 {
    15_000
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Configures one native host for the shared node app surface.
pub struct NodeAppHostConfig {
    /// Human-readable app label shown in the shell bootstrap.
    pub app_name: String,
    /// Bootstrap edge or control-surface URL paired with the node.
    pub edge_base_url: String,
    /// Default visible workspace when the app first opens.
    pub default_surface: NodeAppSurface,
    /// Refresh cadence for polling native telemetry.
    pub refresh_interval_ms: u64,
    /// Optional initial experiment focus.
    pub selection: Option<NodeAppSelection>,
}

impl NodeAppHostConfig {
    /// Creates one host config for a native node app.
    pub fn new(edge_base_url: impl Into<String>) -> Self {
        Self {
            app_name: "burn_p2p".into(),
            edge_base_url: edge_base_url.into(),
            default_surface: NodeAppSurface::Viewer,
            refresh_interval_ms: default_refresh_interval_ms(),
            selection: None,
        }
    }

    /// Overrides the visible app label.
    pub fn with_app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = app_name.into();
        self
    }

    /// Overrides the first visible workspace.
    pub fn with_default_surface(mut self, surface: NodeAppSurface) -> Self {
        self.default_surface = surface;
        self
    }

    /// Overrides the telemetry refresh cadence.
    pub fn with_refresh_interval_ms(mut self, refresh_interval_ms: u64) -> Self {
        self.refresh_interval_ms = refresh_interval_ms.max(250);
        self
    }

    /// Pins the initial experiment focus.
    pub fn with_selection(mut self, selection: NodeAppSelection) -> Self {
        self.selection = Some(selection);
        self
    }
}

#[derive(Clone, Debug)]
/// Cloneable native source for the shared node app surface.
///
/// The source keeps only telemetry and directory state needed to rebuild
/// `NodeAppClientView`. It does not expose runtime internals to the dioxus tree.
pub struct NodeAppHostSource {
    telemetry: TelemetryHandle,
    directory: Vec<ExperimentDirectoryEntry>,
    config: NodeAppHostConfig,
}

impl NodeAppHostSource {
    /// Creates one host source from raw telemetry and directory state.
    pub fn new(
        telemetry: TelemetryHandle,
        directory: Vec<ExperimentDirectoryEntry>,
        config: NodeAppHostConfig,
    ) -> Self {
        Self {
            telemetry,
            directory,
            config,
        }
    }

    /// Derives one host source from a running native node.
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "desktop-client")] {
    /// use burn_p2p_app::{launch_node_app, NodeAppHostConfig, NodeAppHostSource};
    ///
    /// # let node: burn_p2p::RunningNode<()> = todo!();
    /// let source = NodeAppHostSource::from_running_node(
    ///     &node,
    ///     NodeAppHostConfig::new("http://127.0.0.1:8080"),
    /// );
    /// launch_node_app(source);
    /// # }
    /// ```
    pub fn from_running_node<P>(node: &RunningNode<P>, config: NodeAppHostConfig) -> Self {
        Self {
            telemetry: node.telemetry(),
            directory: node.list_experiments(),
            config,
        }
    }

    fn bootstrap(&self) -> NodeAppStaticBootstrap {
        NodeAppStaticBootstrap {
            app_name: self.config.app_name.clone(),
            asset_base_url: String::new(),
            module_entry_path: "native-node-app".into(),
            stylesheet_path: None,
            default_edge_url: Some(self.config.edge_base_url.clone()),
            default_surface: self.config.default_surface,
            refresh_interval_ms: self.config.refresh_interval_ms,
        }
    }

    fn initial_state(&self) -> NodeAppUiState {
        let mut state = NodeAppUiState::new(self.bootstrap(), self.config.edge_base_url.clone());
        state.apply_view(self.view(self.config.selection.as_ref()));
        state
    }

    fn view(&self, selection: Option<&NodeAppSelection>) -> NodeAppClientView {
        burn_p2p::build_node_app_view(
            &self.telemetry.snapshot(),
            self.config.edge_base_url.clone(),
            &self.directory,
            selection,
        )
    }
}

/// Launches the shared dioxus node app on the current native target.
pub fn launch_node_app(source: NodeAppHostSource) {
    dioxus::LaunchBuilder::desktop()
        .with_context(source)
        .launch(NativeNodeAppRoot);
}

#[component]
fn NativeNodeAppRoot() -> Element {
    let source = consume_context::<NodeAppHostSource>();
    let edge_base_url = source.config.edge_base_url.clone();
    let source_for_effect = source.clone();
    let mut ui_state = use_signal(|| source.initial_state());
    let mut selection = use_signal(|| source.config.selection.clone());
    let mut refresh_epoch = use_signal(|| 0_u64);

    use_effect(move || {
        let epoch = refresh_epoch();
        let current_selection = selection();
        let source = source_for_effect.clone();
        let mut ui_state = ui_state;
        let refresh_epoch = refresh_epoch;
        spawn(async move {
            loop {
                if *refresh_epoch.peek() != epoch {
                    break;
                }

                let mut next = ui_state.peek().clone();
                next.start_refresh();
                ui_state.set(next);

                let mut next = ui_state.peek().clone();
                next.apply_view(source.view(current_selection.as_ref()));
                ui_state.set(next);

                sleep(Duration::from_millis(source.config.refresh_interval_ms)).await;

                if *refresh_epoch.peek() != epoch {
                    break;
                }
            }
        });
    });

    let on_surface_requested = move |surface| {
        let mut next = ui_state.peek().clone();
        next.activate_surface(surface);
        ui_state.set(next);
    };

    let on_select_experiment =
        move |(experiment_id, revision_id, _, _): (String, String, bool, bool)| {
            selection.set(Some(NodeAppSelection::new(
                ExperimentId::new(experiment_id),
                RevisionId::new(revision_id),
            )));
            let next_epoch = *refresh_epoch.peek() + 1;
            refresh_epoch.set(next_epoch);
        };

    rsx! {
        NodeAppShell {
            state: ui_state(),
            on_surface_requested: on_surface_requested,
            on_select_experiment: on_select_experiment,
            on_enable_validate: Option::<EventHandler<MouseEvent>>::None,
            on_enable_train: Option::<EventHandler<MouseEvent>>::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_config_clamps_refresh_interval() {
        let config = NodeAppHostConfig::new("https://edge.example").with_refresh_interval_ms(10);
        assert_eq!(config.refresh_interval_ms, 250);
    }
}
