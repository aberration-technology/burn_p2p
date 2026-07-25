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
    pub(crate) role_capabilities: Option<PeerRoleSet>,
    pub(crate) config: NodeConfig,
    pub(crate) revision_contracts: BTreeMap<RevisionId, RevisionContractBundle>,
    pub(crate) revision_contract_trusted_issuers: BTreeMap<PeerId, TrustedIssuer>,
    pub(crate) require_signed_revision_contracts: bool,
    pub(crate) training_window_observers: crate::training::TrainingWindowObservers,
}

impl<P> NodeBuilder<P> {
    /// Creates a builder around the selected project or workload family.
    pub fn new(project: P) -> Self {
        Self {
            project,
            genesis: None,
            roles: PeerRoleSet::default_trainer(),
            role_capabilities: None,
            config: NodeConfig::default(),
            revision_contracts: BTreeMap::new(),
            revision_contract_trusted_issuers: BTreeMap::new(),
            require_signed_revision_contracts: false,
            training_window_observers: crate::training::TrainingWindowObservers::default(),
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

    /// Sets the roles this process may activate without rebuilding its runtime.
    ///
    /// Active roles still come from [`Self::with_roles`]. This separate
    /// capability set lets a resource-constrained process start read-only and
    /// activate an already-authorized compute role after a successful local
    /// capability probe.
    pub fn with_role_capabilities(mut self, roles: PeerRoleSet) -> Self {
        self.role_capabilities = Some(roles);
        self
    }

    /// Registers the authority-signed semantic contract for one revision.
    pub fn with_revision_contract(
        mut self,
        contract: RevisionContractBundle,
    ) -> anyhow::Result<Self> {
        contract.validate()?;
        let revision_id = contract.revision.revision_id.clone();
        if let Some(existing) = self.revision_contracts.get(&revision_id)
            && existing != &contract
        {
            anyhow::bail!(
                "conflicting revision contracts registered for {}",
                revision_id.as_str()
            );
        }
        self.revision_contracts.insert(revision_id, contract);
        Ok(self)
    }

    /// Trusts one authority for revision-contract verification without enabling
    /// peer admission enforcement.
    pub fn with_revision_contract_trusted_issuer(
        mut self,
        issuer: TrustedIssuer,
    ) -> anyhow::Result<Self> {
        let issuer_peer_id = issuer.issuer_peer_id.clone();
        if let Some(existing) = self.revision_contract_trusted_issuers.get(&issuer_peer_id)
            && existing != &issuer
        {
            anyhow::bail!(
                "conflicting revision authority keys registered for {}",
                issuer_peer_id.as_str()
            );
        }
        self.revision_contract_trusted_issuers
            .insert(issuer_peer_id, issuer);
        Ok(self)
    }

    /// Requires every executed revision to carry an authority-signed contract.
    ///
    /// Local test and single-process development nodes may leave this disabled.
    /// Network deployments should enable it.
    pub fn require_signed_revision_contracts(mut self, required: bool) -> Self {
        self.require_signed_revision_contracts = required;
        self
    }

    /// Adds a non-blocking subscriber for native training-window lifecycle events.
    pub fn with_training_window_observer(mut self, observer: impl TrainingWindowObserver) -> Self {
        self.training_window_observers.push(Arc::new(observer));
        self
    }

    /// Adds a shared non-blocking training-window subscriber.
    pub fn with_shared_training_window_observer(
        mut self,
        observer: Arc<dyn TrainingWindowObserver>,
    ) -> Self {
        self.training_window_observers.push(observer);
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

    /// Sets an explicit transport policy for specialized runtimes.
    pub fn with_transport_policy(mut self, transport_policy: RuntimeTransportPolicy) -> Self {
        self.config.transport_policy = Some(transport_policy);
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
                release_manifest
                    .validate_for_network(network_manifest)
                    .map_err(anyhow::Error::from)?;
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

        for (revision_id, contract) in &self.revision_contracts {
            contract.validate()?;
            if revision_id != &contract.revision.revision_id {
                anyhow::bail!(
                    "revision contract map key {} does not match manifest revision {}",
                    revision_id.as_str(),
                    contract.revision.revision_id.as_str(),
                );
            }
            if let Some(selected_workload_id) = &config.selected_workload_id
                && selected_workload_id != &contract.revision.workload_id
            {
                anyhow::bail!(
                    "revision contract {} selects workload {}, but node selected {}",
                    revision_id.as_str(),
                    contract.revision.workload_id.as_str(),
                    selected_workload_id.as_str(),
                );
            }
            if self.require_signed_revision_contracts {
                let trusted_issuers = if self.revision_contract_trusted_issuers.is_empty() {
                    config
                        .auth
                        .as_ref()
                        .and_then(|auth| auth.admission_policy.as_ref())
                        .map(|policy| &policy.trusted_issuers)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "strict revision contract verification requires a revision authority or admission policy"
                            )
                        })?
                } else {
                    &self.revision_contract_trusted_issuers
                };
                verify_revision_contract_bundle(trusted_issuers, contract).map_err(|error| {
                    anyhow::anyhow!(
                        "revision contract {} failed authority verification: {error}",
                        revision_id.as_str()
                    )
                })?;
            }
        }

        let role_capabilities = self.role_capabilities.unwrap_or_else(|| self.roles.clone());
        let active_compute_roles_are_authorized = self.roles.roles.iter().all(|role| {
            role_capabilities.roles.contains(role)
                || matches!(
                    role,
                    PeerRole::Viewer | PeerRole::BrowserObserver | PeerRole::BrowserFallback
                )
        });
        anyhow::ensure!(
            active_compute_roles_are_authorized,
            "active runtime roles must be contained in the configured role capability set"
        );
        if let Some(local_auth) = config
            .auth
            .as_ref()
            .and_then(|auth| auth.local_peer_auth.as_ref())
        {
            let granted_roles = &local_auth.certificate.claims().granted_roles;
            anyhow::ensure!(
                role_capabilities.roles.iter().all(|role| {
                    granted_roles.roles.contains(role)
                        || matches!(
                            role,
                            PeerRole::Viewer
                                | PeerRole::BrowserObserver
                                | PeerRole::BrowserFallback
                        )
                }),
                "runtime role capabilities exceed the roles granted by the local certificate"
            );
        }

        Ok(Node {
            project: self.project,
            mainnet: MainnetHandle {
                genesis,
                roles: self.roles,
            },
            role_capabilities,
            config,
            revision_contracts: self.revision_contracts,
            require_signed_revision_contracts: self.require_signed_revision_contracts,
            training_window_observers: self.training_window_observers,
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
    pub(crate) role_capabilities: PeerRoleSet,
    pub(crate) config: NodeConfig,
    pub(crate) revision_contracts: BTreeMap<RevisionId, RevisionContractBundle>,
    pub(crate) require_signed_revision_contracts: bool,
    pub(crate) training_window_observers: crate::training::TrainingWindowObservers,
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

    /// Returns the registered semantic contract for a revision.
    pub fn revision_contract(&self, revision_id: &RevisionId) -> Option<&RevisionContractBundle> {
        self.revision_contracts.get(revision_id)
    }

    /// Returns whether missing revision contracts are fatal for this node.
    pub fn signed_revision_contracts_required(&self) -> bool {
        self.require_signed_revision_contracts
    }
}
