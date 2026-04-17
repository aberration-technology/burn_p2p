use super::*;

/// Staged builder for a native `burn_p2p` node.
///
/// The builder is the canonical entry point for downstream Burn applications:
/// attach the selected workload family, runtime identity, storage layout,
/// release metadata, and enrollment configuration, then call [`Self::prepare`]
/// or [`Self::spawn`].
#[derive(Clone, Debug)]
pub struct NodeBuilder<P> {
    pub(crate) project: P,
    pub(crate) genesis: Option<GenesisSpec>,
    pub(crate) roles: PeerRoleSet,
    pub(crate) config: NodeConfig,
}

impl<P> NodeBuilder<P> {
    /// Creates a builder around the selected project or workload family.
    pub fn new(project: P) -> Self {
        Self {
            project,
            genesis: None,
            roles: PeerRoleSet::default_trainer(),
            config: NodeConfig::default(),
        }
    }

    /// Pins the builder to a specific network genesis.
    pub fn with_mainnet(mut self, genesis: GenesisSpec) -> Self {
        self.genesis = Some(genesis);
        self
    }

    /// Sets the local peer roles advertised by the node.
    pub fn with_roles(mut self, roles: PeerRoleSet) -> Self {
        self.roles = roles;
        self
    }

    /// Sets the local identity source used for libp2p and certificate flow.
    pub fn with_identity(mut self, identity: IdentityConfig) -> Self {
        self.config.identity = identity;
        self
    }

    /// Sets the storage root and persistence layout for the node.
    pub fn with_storage(mut self, storage: impl Into<StorageConfig>) -> Self {
        self.config.storage = Some(storage.into());
        self
    }

    /// Sets the dataset registration used for shard planning and fetch.
    pub fn with_dataset(mut self, dataset: impl Into<DatasetConfig>) -> Self {
        self.config.dataset = Some(dataset.into());
        self
    }

    /// Sets the enrollment and session configuration for certificate admission.
    pub fn with_auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Sets the raw metrics retention policy used by this node.
    pub fn with_metrics_retention(mut self, metrics_retention: MetricsRetentionConfig) -> Self {
        self.config.metrics_retention = metrics_retention;
        self
    }

    /// Adds one bootstrap peer to the initial dial set.
    pub fn with_bootstrap_peer(mut self, peer: SwarmAddress) -> Self {
        self.config.bootstrap_peers.push(peer);
        self
    }

    /// Extends the initial bootstrap peer list.
    pub fn with_bootstrap_peers(mut self, peers: impl IntoIterator<Item = SwarmAddress>) -> Self {
        self.config.bootstrap_peers.extend(peers);
        self
    }

    /// Adds one local listen address for inbound swarm traffic.
    pub fn with_listen_address(mut self, address: SwarmAddress) -> Self {
        self.config.listen_addresses.push(address);
        self
    }

    /// Extends the local listen-address list.
    pub fn with_listen_addresses(
        mut self,
        addresses: impl IntoIterator<Item = SwarmAddress>,
    ) -> Self {
        self.config.listen_addresses.extend(addresses);
        self
    }

    /// Adds one explicit externally reachable address for swarm advertisement.
    pub fn with_external_address(mut self, address: SwarmAddress) -> Self {
        self.config.external_addresses.push(address);
        self
    }

    /// Extends the explicit externally reachable address list.
    pub fn with_external_addresses(
        mut self,
        addresses: impl IntoIterator<Item = SwarmAddress>,
    ) -> Self {
        self.config.external_addresses.extend(addresses);
        self
    }

    /// Returns the current accumulated node configuration.
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// Validates config, restores persisted state, and returns a prepared node.
    ///
    /// This is useful when callers want to inspect the final prepared config or
    /// delay runtime startup until later.
    pub fn prepare(self) -> anyhow::Result<Node<P>> {
        let mut config = self.config;
        if let Some(storage) = config.storage.clone() {
            restore_runtime_binding_config(&storage, &mut config)?;
            config.auth = restore_auth_config(&storage, config.auth)?;
        }

        let genesis = self.genesis.or_else(|| {
            config
                .network_manifest
                .as_ref()
                .map(|network_manifest| GenesisSpec {
                    network_id: network_manifest.network_id.clone(),
                    protocol_version: Version::new(
                        u64::from(network_manifest.protocol_major),
                        0,
                        0,
                    ),
                    display_name: network_manifest.description.clone(),
                    created_at: network_manifest.created_at,
                    metadata: Default::default(),
                })
        });
        let genesis = genesis.ok_or_else(|| anyhow::anyhow!("missing genesis"))?;

        if let Some(network_manifest) = &config.network_manifest {
            if genesis.network_id != network_manifest.network_id {
                anyhow::bail!(
                    "genesis network {} does not match network manifest {}",
                    genesis.network_id.as_str(),
                    network_manifest.network_id.as_str(),
                );
            }

            if genesis.protocol_version.major != u64::from(network_manifest.protocol_major) {
                anyhow::bail!(
                    "genesis protocol major {} does not match network manifest {}",
                    genesis.protocol_version.major,
                    network_manifest.protocol_major,
                );
            }
        }

        if let Some(release_manifest) = &config.client_release_manifest {
            if let Some(network_manifest) = &config.network_manifest {
                if release_manifest.project_family_id != network_manifest.project_family_id {
                    anyhow::bail!(
                        "client release family {} does not match network family {}",
                        release_manifest.project_family_id.as_str(),
                        network_manifest.project_family_id.as_str(),
                    );
                }

                if release_manifest.release_train_hash
                    != network_manifest.required_release_train_hash
                {
                    anyhow::bail!(
                        "release train hash {} does not match network requirement {}",
                        release_manifest.release_train_hash.as_str(),
                        network_manifest.required_release_train_hash.as_str(),
                    );
                }

                if !network_manifest.allowed_target_artifact_hashes.is_empty()
                    && !network_manifest
                        .allowed_target_artifact_hashes
                        .contains(&release_manifest.target_artifact_hash)
                {
                    anyhow::bail!(
                        "target artifact hash {} is not allowed by network {}",
                        release_manifest.target_artifact_hash.as_str(),
                        network_manifest.network_id.as_str(),
                    );
                }

                if release_manifest.protocol_major != network_manifest.protocol_major {
                    anyhow::bail!(
                        "client release protocol major {} does not match network protocol major {}",
                        release_manifest.protocol_major,
                        network_manifest.protocol_major,
                    );
                }
            }

            if let Some(workload_id) = &config.selected_workload_id
                && !release_manifest
                    .supported_workloads
                    .iter()
                    .any(|workload| workload.workload_id == *workload_id)
            {
                anyhow::bail!(
                    "selected workload {} is not compiled into client release {}",
                    workload_id.as_str(),
                    release_manifest.target_artifact_hash.as_str(),
                );
            }
        }

        Ok(Node {
            project: self.project,
            mainnet: MainnetHandle {
                genesis,
                roles: self.roles,
            },
            config,
        })
    }

    /// Validates the builder and starts the native runtime thread immediately.
    pub fn spawn(self) -> anyhow::Result<RunningNode<P>> {
        RunningNode::spawn(self.prepare()?)
    }
}

/// Prepared but not yet running node.
///
/// A `Node` owns the selected project and validated configuration, but the
/// control plane and swarm threads have not been started yet.
pub struct Node<P> {
    pub(crate) project: P,
    pub(crate) mainnet: MainnetHandle,
    pub(crate) config: NodeConfig,
}

impl<P> Node<P> {
    /// Performs the mainnet operation.
    pub fn mainnet(&self) -> &MainnetHandle {
        &self.mainnet
    }

    /// Performs the checkpoint sync operation.
    pub fn checkpoint_sync(&self, target_head_id: HeadId) -> CheckpointSyncHandle {
        self.mainnet.checkpoint_sync(target_head_id)
    }

    /// Performs the experiment operation.
    pub fn experiment(
        &self,
        study_id: StudyId,
        experiment_id: ExperimentId,
        revision_id: RevisionId,
    ) -> ExperimentHandle {
        self.mainnet
            .experiment(study_id, experiment_id, revision_id)
    }

    /// Consumes the value and returns the project.
    pub fn into_project(self) -> P {
        self.project
    }

    /// Performs the config operation.
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }
}
