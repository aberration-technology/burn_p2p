use burn_p2p_core::{ExperimentId, RevisionId};
use serde::{Deserialize, Serialize};

/// Static runtime-facing browser-site configuration emitted by build tools.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserSiteBootstrapConfig {
    /// Edge base URL shown or used by the browser shell, when configured.
    #[serde(default)]
    pub edge_base_url: Option<String>,
    /// Seed-node URLs embedded into the generated browser site.
    #[serde(default)]
    pub seed_node_urls: Vec<String>,
    /// Selected experiment identifier, when the site should focus one experiment.
    #[serde(default)]
    pub selected_experiment_id: Option<ExperimentId>,
    /// Selected revision identifier, when the site should focus one revision.
    #[serde(default)]
    pub selected_revision_id: Option<RevisionId>,
    /// Whether the generated site expects edge-backed authentication.
    #[serde(default = "default_require_edge_auth")]
    pub require_edge_auth: bool,
}

fn default_require_edge_auth() -> bool {
    true
}

impl BrowserSiteBootstrapConfig {
    /// Creates one site config from an optional edge base URL.
    pub fn new(edge_base_url: Option<String>) -> Self {
        Self {
            edge_base_url,
            seed_node_urls: Vec::new(),
            selected_experiment_id: None,
            selected_revision_id: None,
            require_edge_auth: true,
        }
    }

    /// Adds seed URLs to the site config.
    pub fn with_seed_node_urls(mut self, seed_node_urls: Vec<String>) -> Self {
        self.seed_node_urls = seed_node_urls;
        self
    }

    /// Adds one selected experiment/revision pair.
    pub fn with_selection(
        mut self,
        experiment_id: ExperimentId,
        revision_id: Option<RevisionId>,
    ) -> Self {
        self.selected_experiment_id = Some(experiment_id);
        self.selected_revision_id = revision_id;
        self
    }

    /// Updates whether the site requires authenticated edge access.
    pub fn with_edge_auth_requirement(mut self, require_edge_auth: bool) -> Self {
        self.require_edge_auth = require_edge_auth;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::BrowserSiteBootstrapConfig;
    use burn_p2p_core::{ExperimentId, RevisionId};

    #[test]
    fn site_bootstrap_config_round_trips() {
        let config = BrowserSiteBootstrapConfig::new(Some("https://edge.example".into()))
            .with_seed_node_urls(vec!["/dns4/seed.example/tcp/4001/ws".into()])
            .with_selection(
                ExperimentId::new("experiment-a"),
                Some(RevisionId::new("revision-a")),
            )
            .with_edge_auth_requirement(false);
        let encoded = serde_json::to_value(&config).expect("serialize config");
        assert_eq!(encoded["edge_base_url"], "https://edge.example");
        assert_eq!(encoded["require_edge_auth"], false);
        let decoded: BrowserSiteBootstrapConfig =
            serde_json::from_value(encoded).expect("decode config");
        assert_eq!(decoded, config);
    }
}
