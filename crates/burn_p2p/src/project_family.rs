use semver::Version;

use crate::{
    ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard, ClientReleaseManifest,
    ContentId, EvalSplit, FsArtifactStore, GenesisSpec, MergeModelCandidate, MergePolicy,
    MetricReport, MetricValue, NetworkManifest, NodeBuilder, P2pWorkload, PatchOutcome,
    PatchSupport, ProjectFamilyId, RuntimePatch, SupportedWorkload,
    TrainerCanonicalReconcileStrategy, WindowCtx, WindowReport, WorkloadId,
};

/// Groups one or more compatible workloads under a single project family.
///
/// A family pins the public-facing compatibility identity for a downstream Burn
/// application. Every workload returned by the family must match the same
/// `project_family_id` and `client_release_manifest`, while experiment
/// directory entries can switch between workloads without forcing a full
/// reconnect.
pub trait P2pProjectFamily {
    /// Defines the workload alias.
    type Workload: P2pWorkload;

    /// Performs the project family ID operation.
    fn project_family_id(&self) -> &ProjectFamilyId;

    /// Performs the client release manifest operation.
    fn client_release_manifest(&self) -> &ClientReleaseManifest;

    /// Performs the supported workloads operation.
    fn supported_workloads(&self) -> &[SupportedWorkload] {
        &self.client_release_manifest().supported_workloads
    }

    /// Performs the workload operation.
    fn workload(&self, workload_id: &WorkloadId) -> anyhow::Result<Self::Workload>;
}

#[derive(Clone, Debug)]
/// Wraps a family together with one concrete selected workload.
///
/// This is the bridge type most downstream integrations actually run through:
/// pick a family, select the compiled workload, then hand the selected project
/// to [`crate::NodeBuilder`].
pub struct SelectedWorkloadProject<P>
where
    P: P2pProjectFamily,
{
    family: P,
    workload_id: WorkloadId,
    workload: P::Workload,
}

impl<P> SelectedWorkloadProject<P>
where
    P: P2pProjectFamily,
{
    /// Creates a new value.
    pub fn new(family: P, workload_id: WorkloadId) -> anyhow::Result<Self> {
        let workload = family.workload(&workload_id)?;
        Ok(Self {
            family,
            workload_id,
            workload,
        })
    }

    /// Performs the selected workload ID operation.
    pub fn selected_workload_id(&self) -> &WorkloadId {
        &self.workload_id
    }

    /// Performs the project family ID operation.
    pub fn project_family_id(&self) -> &ProjectFamilyId {
        self.family.project_family_id()
    }

    /// Performs the client release manifest operation.
    pub fn client_release_manifest(&self) -> &ClientReleaseManifest {
        self.family.client_release_manifest()
    }

    /// Performs the switch workload operation.
    pub fn switch_workload(&mut self, workload_id: impl Into<WorkloadId>) -> anyhow::Result<()> {
        let workload_id = workload_id.into();
        let workload = self.family.workload(&workload_id)?;
        self.workload_id = workload_id;
        self.workload = workload;
        Ok(())
    }
}

#[derive(Clone, Debug)]
/// Convenience family wrapper for deployments that compile exactly one workload.
///
/// This is the simplest way to migrate an existing Burn integration onto the
/// family/workload model without building a custom workload registry.
pub struct SingleWorkloadProjectFamily<W> {
    release_manifest: ClientReleaseManifest,
    workload: W,
}

impl<W> SingleWorkloadProjectFamily<W>
where
    W: P2pWorkload + Clone,
{
    /// Creates a new value.
    pub fn new(release_manifest: ClientReleaseManifest, workload: W) -> anyhow::Result<Self> {
        if release_manifest.supported_workloads.len() != 1 {
            anyhow::bail!(
                "single-workload family requires exactly one supported workload, found {}",
                release_manifest.supported_workloads.len()
            );
        }

        let supported = workload.supported_workload();
        if release_manifest.supported_workloads[0] != supported {
            anyhow::bail!(
                "single-workload family manifest does not match workload {}",
                supported.workload_id.as_str()
            );
        }

        Ok(Self {
            release_manifest,
            workload,
        })
    }

    /// Performs the workload ID operation.
    pub fn workload_id(&self) -> WorkloadId {
        self.workload.workload_id()
    }

    /// Consumes the value and returns the workload.
    pub fn into_workload(self) -> W {
        self.workload
    }
}

impl<W> P2pProjectFamily for SingleWorkloadProjectFamily<W>
where
    W: P2pWorkload + Clone,
{
    type Workload = W;

    fn project_family_id(&self) -> &ProjectFamilyId {
        &self.release_manifest.project_family_id
    }

    fn client_release_manifest(&self) -> &ClientReleaseManifest {
        &self.release_manifest
    }

    fn workload(&self, workload_id: &WorkloadId) -> anyhow::Result<Self::Workload> {
        if self.workload.workload_id() != *workload_id {
            anyhow::bail!(
                "workload {} is not compiled into project family {}",
                workload_id.as_str(),
                self.project_family_id().as_str(),
            );
        }

        Ok(self.workload.clone())
    }
}

impl<W> P2pWorkload for SingleWorkloadProjectFamily<W>
where
    W: P2pWorkload + Clone,
{
    type Device = W::Device;
    type Model = W::Model;
    type Batch = W::Batch;
    type WindowStats = W::WindowStats;

    fn init_model(&self, device: &Self::Device) -> Self::Model {
        self.workload.init_model(device)
    }

    fn benchmark(&self, model: &Self::Model, device: &Self::Device) -> crate::CapabilityEstimate {
        self.workload.benchmark(model, device)
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<Self::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, crate::TrainError> {
        self.workload.train_window(ctx)
    }

    fn evaluate(&self, model: &Self::Model, split: EvalSplit) -> MetricReport {
        self.workload.evaluate(model, split)
    }

    fn apply_patch(&mut self, patch: &RuntimePatch) -> PatchOutcome {
        self.workload.apply_patch(patch)
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        self.workload.supported_patch_classes()
    }

    fn runtime_device(&self) -> Self::Device {
        self.workload.runtime_device()
    }

    fn dataset_registration(&self) -> anyhow::Result<crate::DatasetRegistration> {
        self.workload.dataset_registration()
    }

    fn microshard_plan(
        &self,
        registration: &crate::DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        self.workload.microshard_plan(registration)
    }

    fn load_batches(
        &self,
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        self.workload.load_batches(lease, cached_microshards)
    }

    fn load_model_artifact(
        &self,
        model: Self::Model,
        descriptor: &ArtifactDescriptor,
        store: &FsArtifactStore,
        device: &Self::Device,
    ) -> anyhow::Result<Self::Model> {
        self.workload
            .load_model_artifact(model, descriptor, store, device)
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: ArtifactKind,
        head_id: crate::HeadId,
        base_head_id: Option<crate::HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor> {
        self.workload
            .materialize_model_artifact(model, artifact_kind, head_id, base_head_id, store)
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> std::collections::BTreeMap<String, MetricValue> {
        self.workload.contribution_metrics(report)
    }

    fn contribution_weight(&self, report: &WindowReport<Self::WindowStats>) -> f64 {
        self.workload.contribution_weight(report)
    }

    fn reconcile_canonical_model(
        &self,
        local_model: &Self::Model,
        canonical_model: Self::Model,
        strategy: TrainerCanonicalReconcileStrategy,
    ) -> anyhow::Result<Self::Model> {
        self.workload
            .reconcile_canonical_model(local_model, canonical_model, strategy)
    }

    fn merge_candidate_models(
        &self,
        base_model: &Self::Model,
        candidates: &[MergeModelCandidate<'_, Self::Model>],
        policy: MergePolicy,
    ) -> anyhow::Result<Option<Self::Model>> {
        self.workload
            .merge_candidate_models(base_model, candidates, policy)
    }

    fn apply_single_root_ema(
        &self,
        base_model: &Self::Model,
        merged_model: Self::Model,
        policy: MergePolicy,
    ) -> anyhow::Result<Self::Model> {
        self.workload
            .apply_single_root_ema(base_model, merged_model, policy)
    }

    fn supported_workload(&self) -> SupportedWorkload {
        self.workload.supported_workload()
    }

    fn model_schema_hash(&self) -> ContentId {
        self.workload.model_schema_hash()
    }
}

impl<P> P2pWorkload for SelectedWorkloadProject<P>
where
    P: P2pProjectFamily,
{
    type Device = <P::Workload as P2pWorkload>::Device;
    type Model = <P::Workload as P2pWorkload>::Model;
    type Batch = <P::Workload as P2pWorkload>::Batch;
    type WindowStats = <P::Workload as P2pWorkload>::WindowStats;

    fn init_model(&self, device: &Self::Device) -> Self::Model {
        self.workload.init_model(device)
    }

    fn benchmark(&self, model: &Self::Model, device: &Self::Device) -> crate::CapabilityEstimate {
        self.workload.benchmark(model, device)
    }

    fn train_window(
        &self,
        ctx: &mut WindowCtx<Self::Device, Self::Model, Self::Batch>,
    ) -> Result<WindowReport<Self::WindowStats>, crate::TrainError> {
        self.workload.train_window(ctx)
    }

    fn evaluate(&self, model: &Self::Model, split: EvalSplit) -> MetricReport {
        self.workload.evaluate(model, split)
    }

    fn apply_patch(&mut self, patch: &RuntimePatch) -> PatchOutcome {
        self.workload.apply_patch(patch)
    }

    fn supported_patch_classes(&self) -> PatchSupport {
        self.workload.supported_patch_classes()
    }

    fn runtime_device(&self) -> Self::Device {
        self.workload.runtime_device()
    }

    fn dataset_registration(&self) -> anyhow::Result<crate::DatasetRegistration> {
        self.workload.dataset_registration()
    }

    fn microshard_plan(
        &self,
        registration: &crate::DatasetRegistration,
    ) -> anyhow::Result<crate::MicroShardPlan> {
        self.workload.microshard_plan(registration)
    }

    fn load_batches(
        &self,
        lease: &AssignmentLease,
        cached_microshards: &[CachedMicroShard],
    ) -> anyhow::Result<Vec<Self::Batch>> {
        self.workload.load_batches(lease, cached_microshards)
    }

    fn load_model_artifact(
        &self,
        model: Self::Model,
        descriptor: &ArtifactDescriptor,
        store: &FsArtifactStore,
        device: &Self::Device,
    ) -> anyhow::Result<Self::Model> {
        self.workload
            .load_model_artifact(model, descriptor, store, device)
    }

    fn materialize_model_artifact(
        &self,
        model: &Self::Model,
        artifact_kind: ArtifactKind,
        head_id: crate::HeadId,
        base_head_id: Option<crate::HeadId>,
        store: &FsArtifactStore,
    ) -> anyhow::Result<ArtifactDescriptor> {
        self.workload
            .materialize_model_artifact(model, artifact_kind, head_id, base_head_id, store)
    }

    fn contribution_metrics(
        &self,
        report: &WindowReport<Self::WindowStats>,
    ) -> std::collections::BTreeMap<String, MetricValue> {
        self.workload.contribution_metrics(report)
    }

    fn contribution_weight(&self, report: &WindowReport<Self::WindowStats>) -> f64 {
        self.workload.contribution_weight(report)
    }

    fn reconcile_canonical_model(
        &self,
        local_model: &Self::Model,
        canonical_model: Self::Model,
        strategy: TrainerCanonicalReconcileStrategy,
    ) -> anyhow::Result<Self::Model> {
        self.workload
            .reconcile_canonical_model(local_model, canonical_model, strategy)
    }

    fn merge_candidate_models(
        &self,
        base_model: &Self::Model,
        candidates: &[MergeModelCandidate<'_, Self::Model>],
        policy: MergePolicy,
    ) -> anyhow::Result<Option<Self::Model>> {
        self.workload
            .merge_candidate_models(base_model, candidates, policy)
    }

    fn apply_single_root_ema(
        &self,
        base_model: &Self::Model,
        merged_model: Self::Model,
        policy: MergePolicy,
    ) -> anyhow::Result<Self::Model> {
        self.workload
            .apply_single_root_ema(base_model, merged_model, policy)
    }

    fn supported_workload(&self) -> SupportedWorkload {
        self.workload.supported_workload()
    }

    fn model_schema_hash(&self) -> ContentId {
        self.workload.model_schema_hash()
    }

    fn switch_runtime_workload(&mut self, workload_id: &WorkloadId) -> anyhow::Result<()> {
        self.switch_workload(workload_id.clone())
    }
}

fn validate_network_manifest(
    project_family_id: &ProjectFamilyId,
    release_manifest: &ClientReleaseManifest,
    network_manifest: &NetworkManifest,
) -> anyhow::Result<()> {
    if network_manifest.project_family_id != *project_family_id {
        anyhow::bail!(
            "network {} targets project family {}, but builder is using family {}",
            network_manifest.network_id.as_str(),
            network_manifest.project_family_id.as_str(),
            project_family_id.as_str(),
        );
    }

    release_manifest
        .validate_for_network(network_manifest)
        .map_err(anyhow::Error::from)
}

impl<P> NodeBuilder<P>
where
    P: P2pProjectFamily,
{
    /// Returns a copy configured with the network.
    pub fn with_network(
        self,
        network_manifest: NetworkManifest,
    ) -> anyhow::Result<NodeBuilder<SelectedWorkloadProject<P>>> {
        let release_manifest = self.project.client_release_manifest().clone();
        let project_family_id = self.project.project_family_id().clone();
        validate_network_manifest(&project_family_id, &release_manifest, &network_manifest)?;

        let selected_workload_id = if release_manifest.supported_workloads.len() == 1 {
            release_manifest.supported_workloads[0].workload_id.clone()
        } else {
            anyhow::bail!(
                "project family {} exposes {} workloads; select one with for_workload(...) before spawn",
                project_family_id.as_str(),
                release_manifest.supported_workloads.len()
            );
        };

        let selected = SelectedWorkloadProject::new(self.project, selected_workload_id.clone())?;
        let mut config = self.config;
        let genesis = GenesisSpec {
            network_id: network_manifest.network_id.clone(),
            protocol_version: Version::new(u64::from(network_manifest.protocol_major), 0, 0),
            display_name: network_manifest.description.clone(),
            created_at: network_manifest.created_at,
            metadata: Default::default(),
        };
        config.network_manifest = Some(network_manifest);
        config.client_release_manifest = Some(release_manifest);
        config.selected_workload_id = Some(selected_workload_id);

        Ok(NodeBuilder {
            project: selected,
            genesis: Some(genesis),
            roles: self.roles,
            config,
        })
    }

    /// Performs the for workload operation.
    pub fn for_workload(
        self,
        workload_id: impl Into<WorkloadId>,
    ) -> anyhow::Result<NodeBuilder<SelectedWorkloadProject<P>>> {
        let workload_id = workload_id.into();
        let Self {
            project,
            genesis,
            roles,
            mut config,
        } = self;
        let selected = SelectedWorkloadProject::new(project, workload_id.clone())?;
        config.selected_workload_id = Some(workload_id);
        config.client_release_manifest = Some(selected.client_release_manifest().clone());

        Ok(NodeBuilder {
            project: selected,
            genesis,
            roles,
            config,
        })
    }
}

impl<P> NodeBuilder<SelectedWorkloadProject<P>>
where
    P: P2pProjectFamily,
{
    /// Returns a copy configured with the network.
    pub fn with_network(mut self, network_manifest: NetworkManifest) -> anyhow::Result<Self> {
        let release_manifest = self.project.client_release_manifest().clone();
        let project_family_id = self.project.project_family_id().clone();
        validate_network_manifest(&project_family_id, &release_manifest, &network_manifest)?;

        self.genesis = Some(GenesisSpec {
            network_id: network_manifest.network_id.clone(),
            protocol_version: Version::new(u64::from(network_manifest.protocol_major), 0, 0),
            display_name: network_manifest.description.clone(),
            created_at: network_manifest.created_at,
            metadata: Default::default(),
        });
        self.config.network_manifest = Some(network_manifest);
        self.config.client_release_manifest = Some(release_manifest);
        self.config.selected_workload_id = Some(self.project.selected_workload_id().clone());

        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use chrono::Utc;

    use crate::{
        ArtifactDescriptor, ArtifactKind, AssignmentLease, CachedMicroShard, CapabilityEstimate,
        ClientPlatform, ClientReleaseManifest, ContentId, DatasetRegistration, EvalSplit,
        ExperimentResourceRequirements, FsArtifactStore, MetricReport, MetricValue, NetworkId,
        NetworkManifest, NodeBuilder, P2pProjectFamily, PatchOutcome, PatchSupport,
        ProjectFamilyId, RevisionId, RevisionManifest, RuntimePatch, SupportedWorkload,
        WindowActivation, WindowCtx, WindowReport, WorkloadId,
    };

    use super::{P2pWorkload, SingleWorkloadProjectFamily};

    #[derive(Clone, Debug)]
    struct TestWorkload {
        workload_id: WorkloadId,
        model_schema_hash: ContentId,
        checkpoint_format_hash: ContentId,
    }

    impl TestWorkload {
        fn workload_manifest(&self) -> SupportedWorkload {
            SupportedWorkload {
                workload_id: self.workload_id.clone(),
                workload_name: "Test".into(),
                model_program_hash: ContentId::new("program"),
                checkpoint_format_hash: self.checkpoint_format_hash.clone(),
                supported_revision_family: ContentId::new("revision-family"),
                resource_class: "cpu".into(),
            }
        }
    }

    impl P2pWorkload for TestWorkload {
        type Device = ();
        type Model = ();
        type Batch = ();
        type WindowStats = BTreeMap<String, MetricValue>;

        fn init_model(&self, _device: &()) -> Self::Model {}

        fn benchmark(&self, _model: &Self::Model, _device: &()) -> CapabilityEstimate {
            CapabilityEstimate {
                preferred_backends: vec!["ndarray".into()],
                work_units_per_second: 1.0,
                target_window_seconds: 1,
            }
        }

        fn train_window(
            &self,
            _ctx: &mut WindowCtx<(), Self::Model, Self::Batch>,
        ) -> Result<WindowReport<Self::WindowStats>, crate::TrainError> {
            Ok(WindowReport {
                contribution: None,
                stats: BTreeMap::new(),
                completed_at: Utc::now(),
            })
        }

        fn evaluate(&self, _model: &Self::Model, _split: EvalSplit) -> MetricReport {
            MetricReport {
                metrics: BTreeMap::new(),
                captured_at: Utc::now(),
            }
        }

        fn apply_patch(&mut self, _patch: &RuntimePatch) -> PatchOutcome {
            PatchOutcome::Applied
        }

        fn supported_patch_classes(&self) -> PatchSupport {
            PatchSupport {
                hot: true,
                warm: false,
                cold: false,
            }
        }

        fn runtime_device(&self) {}

        fn dataset_registration(&self) -> anyhow::Result<DatasetRegistration> {
            anyhow::bail!("not used")
        }

        fn microshard_plan(
            &self,
            _registration: &DatasetRegistration,
        ) -> anyhow::Result<crate::MicroShardPlan> {
            anyhow::bail!("not used")
        }

        fn load_batches(
            &self,
            _lease: &AssignmentLease,
            _cached_microshards: &[CachedMicroShard],
        ) -> anyhow::Result<Vec<Self::Batch>> {
            Ok(Vec::new())
        }

        fn load_model_artifact(
            &self,
            model: Self::Model,
            _descriptor: &ArtifactDescriptor,
            _store: &FsArtifactStore,
            _device: &(),
        ) -> anyhow::Result<Self::Model> {
            Ok(model)
        }

        fn materialize_model_artifact(
            &self,
            _model: &Self::Model,
            _artifact_kind: ArtifactKind,
            _head_id: crate::HeadId,
            _base_head_id: Option<crate::HeadId>,
            _store: &FsArtifactStore,
        ) -> anyhow::Result<ArtifactDescriptor> {
            anyhow::bail!("not used")
        }

        fn contribution_metrics(
            &self,
            _report: &WindowReport<Self::WindowStats>,
        ) -> BTreeMap<String, MetricValue> {
            BTreeMap::new()
        }

        fn supported_workload(&self) -> SupportedWorkload {
            self.workload_manifest()
        }

        fn model_schema_hash(&self) -> ContentId {
            self.model_schema_hash.clone()
        }
    }

    fn release_manifest(workload: SupportedWorkload) -> ClientReleaseManifest {
        ClientReleaseManifest {
            project_family_id: ProjectFamilyId::new("family-a"),
            release_train_hash: ContentId::new("train-a"),
            target_artifact_id: "native-linux-x86_64".into(),
            target_artifact_hash: ContentId::new("artifact-native-a"),
            target_platform: ClientPlatform::Native,
            app_semver: semver::Version::new(0, 2, 0),
            git_commit: "deadbeef".into(),
            cargo_lock_hash: ContentId::new("cargo-lock"),
            burn_version_string: "0.21.0-pre.3".into(),
            enabled_features_hash: ContentId::new("features"),
            protocol_major: 1,
            supported_workloads: vec![workload],
            built_at: Utc::now(),
        }
    }

    fn release_manifest_with_workloads(workloads: Vec<SupportedWorkload>) -> ClientReleaseManifest {
        ClientReleaseManifest {
            project_family_id: ProjectFamilyId::new("family-a"),
            release_train_hash: ContentId::new("train-a"),
            target_artifact_id: "native-linux-x86_64".into(),
            target_artifact_hash: ContentId::new("artifact-native-a"),
            target_platform: ClientPlatform::Native,
            app_semver: semver::Version::new(0, 2, 0),
            git_commit: "deadbeef".into(),
            cargo_lock_hash: ContentId::new("cargo-lock"),
            burn_version_string: "0.21.0-pre.3".into(),
            enabled_features_hash: ContentId::new("features"),
            protocol_major: 1,
            supported_workloads: workloads,
            built_at: Utc::now(),
        }
    }

    fn network_manifest() -> NetworkManifest {
        NetworkManifest {
            network_id: NetworkId::new("network-a"),
            project_family_id: ProjectFamilyId::new("family-a"),
            protocol_major: 1,
            minimum_client_version: semver::Version::new(0, 1, 0),
            required_release_train_hash: ContentId::new("train-a"),
            allowed_target_artifact_hashes: BTreeSet::from([ContentId::new("artifact-native-a")]),
            authority_public_keys: vec!["authority-key".into()],
            bootstrap_addrs: vec!["/ip4/127.0.0.1/tcp/4101".into()],
            auth_policy_hash: ContentId::new("auth-policy"),
            created_at: Utc::now(),
            description: "family-a network".into(),
        }
    }

    #[test]
    fn single_workload_family_rejects_manifest_mismatch() {
        let workload = TestWorkload {
            workload_id: WorkloadId::new("compiled"),
            model_schema_hash: ContentId::new("schema-a"),
            checkpoint_format_hash: ContentId::new("format-a"),
        };
        let manifest = release_manifest(SupportedWorkload {
            workload_id: WorkloadId::new("different"),
            workload_name: "Different".into(),
            model_program_hash: ContentId::new("program"),
            checkpoint_format_hash: ContentId::new("format-a"),
            supported_revision_family: ContentId::new("revision-family"),
            resource_class: "cpu".into(),
        });

        let error = SingleWorkloadProjectFamily::new(manifest, workload)
            .expect_err("manifest mismatch should be rejected");
        assert!(error.to_string().contains("manifest does not match"));
    }

    #[test]
    fn single_workload_family_selects_the_compiled_workload() {
        let workload = TestWorkload {
            workload_id: WorkloadId::new("compiled"),
            model_schema_hash: ContentId::new("schema-a"),
            checkpoint_format_hash: ContentId::new("format-a"),
        };
        let manifest = release_manifest(workload.supported_workload());
        let family = SingleWorkloadProjectFamily::new(manifest, workload).expect("family");

        let builder = NodeBuilder::new(family)
            .for_workload(WorkloadId::new("compiled"))
            .expect("workload builder");
        assert!(builder.config().storage.is_none());
    }

    #[test]
    fn with_network_rejects_family_or_release_mismatch() {
        let workload = TestWorkload {
            workload_id: WorkloadId::new("compiled"),
            model_schema_hash: ContentId::new("schema-a"),
            checkpoint_format_hash: ContentId::new("format-a"),
        };
        let manifest = release_manifest(workload.supported_workload());
        let family = SingleWorkloadProjectFamily::new(manifest, workload).expect("family");
        let mut wrong_network = network_manifest();
        wrong_network.project_family_id = ProjectFamilyId::new("family-b");

        let error = NodeBuilder::new(family)
            .with_network(wrong_network)
            .expect_err("family mismatch should be rejected");
        assert!(error.to_string().contains("project family"));
    }

    #[test]
    fn with_network_sets_genesis_and_release_context() {
        let workload = TestWorkload {
            workload_id: WorkloadId::new("compiled"),
            model_schema_hash: ContentId::new("schema-a"),
            checkpoint_format_hash: ContentId::new("format-a"),
        };
        let manifest = release_manifest(workload.supported_workload());
        let family = SingleWorkloadProjectFamily::new(manifest, workload).expect("family");
        let network = network_manifest();

        let builder = NodeBuilder::new(family)
            .with_network(network.clone())
            .expect("network builder");

        assert_eq!(builder.config().network_manifest, Some(network.clone()));
        assert_eq!(
            builder
                .config()
                .client_release_manifest
                .as_ref()
                .map(|manifest| manifest.release_train_hash.clone()),
            Some(ContentId::new("train-a"))
        );
        assert_eq!(
            builder.config().selected_workload_id,
            Some(WorkloadId::new("compiled"))
        );
    }

    #[test]
    fn multi_workload_family_requires_explicit_selection_before_network_binding() {
        #[derive(Clone, Debug)]
        struct MultiWorkloadFamily {
            release_manifest: ClientReleaseManifest,
            workloads: BTreeMap<WorkloadId, TestWorkload>,
        }

        impl P2pProjectFamily for MultiWorkloadFamily {
            type Workload = TestWorkload;

            fn project_family_id(&self) -> &ProjectFamilyId {
                &self.release_manifest.project_family_id
            }

            fn client_release_manifest(&self) -> &ClientReleaseManifest {
                &self.release_manifest
            }

            fn workload(&self, workload_id: &WorkloadId) -> anyhow::Result<Self::Workload> {
                self.workloads
                    .get(workload_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("missing workload {}", workload_id.as_str()))
            }
        }

        let compiled = TestWorkload {
            workload_id: WorkloadId::new("compiled"),
            model_schema_hash: ContentId::new("schema-a"),
            checkpoint_format_hash: ContentId::new("format-a"),
        };
        let alternate = TestWorkload {
            workload_id: WorkloadId::new("alternate"),
            model_schema_hash: ContentId::new("schema-b"),
            checkpoint_format_hash: ContentId::new("format-b"),
        };
        let family = MultiWorkloadFamily {
            release_manifest: release_manifest_with_workloads(vec![
                compiled.supported_workload(),
                alternate.supported_workload(),
            ]),
            workloads: BTreeMap::from([
                (compiled.workload_id.clone(), compiled),
                (alternate.workload_id.clone(), alternate),
            ]),
        };

        let error = NodeBuilder::new(family)
            .with_network(network_manifest())
            .expect_err("multi-workload family should require explicit selection");
        assert!(error.to_string().contains("select one with for_workload"));
    }

    #[test]
    fn single_workload_family_prepares_without_explicit_selection() {
        let workload = TestWorkload {
            workload_id: WorkloadId::new("compiled"),
            model_schema_hash: ContentId::new("schema-a"),
            checkpoint_format_hash: ContentId::new("format-a"),
        };
        let manifest = release_manifest(workload.supported_workload());
        let family = SingleWorkloadProjectFamily::new(manifest, workload).expect("family");

        let node = NodeBuilder::new(family)
            .with_network(network_manifest())
            .expect("network builder")
            .prepare()
            .expect("prepared node");
        assert_eq!(
            node.config().selected_workload_id,
            Some(WorkloadId::new("compiled"))
        );
    }

    #[test]
    fn workload_verifies_revision_against_exact_hashes() {
        let workload = TestWorkload {
            workload_id: WorkloadId::new("compiled"),
            model_schema_hash: ContentId::new("schema-a"),
            checkpoint_format_hash: ContentId::new("format-a"),
        };
        let revision = RevisionManifest {
            experiment_id: crate::ExperimentId::new("exp-a"),
            revision_id: RevisionId::new("rev-a"),
            training_protocol: crate::TrainingProtocol::default(),
            workload_id: WorkloadId::new("compiled"),
            required_release_train_hash: ContentId::new("train-a"),
            model_schema_hash: ContentId::new("schema-a"),
            checkpoint_format_hash: ContentId::new("format-a"),
            dataset_view_id: crate::DatasetViewId::new("view-a"),
            training_config_hash: ContentId::new("training-a"),
            merge_topology_policy_hash: ContentId::new("topology-a"),
            slot_requirements: ExperimentResourceRequirements {
                minimum_roles: Default::default(),
                minimum_device_memory_bytes: None,
                minimum_system_memory_bytes: None,
                estimated_download_bytes: 0,
                estimated_window_seconds: 1,
            },
            activation_window: WindowActivation {
                activation_window: crate::WindowId(1),
                grace_windows: 0,
            },
            lag_policy: crate::LagPolicy::default(),
            merge_window_miss_policy: crate::MergeWindowMissPolicy::default(),
            robustness_policy: None,
            browser_enabled: false,
            browser_role_policy: crate::BrowserRolePolicy::default(),
            max_browser_checkpoint_bytes: None,
            max_browser_window_secs: None,
            max_browser_shard_bytes: None,
            requires_webgpu: false,
            max_browser_batch_size: None,
            recommended_browser_precision: None,
            visibility_policy: crate::BrowserVisibilityPolicy::Hidden,
            description: "test revision".into(),
        };

        workload
            .verify_revision(&revision)
            .expect("matching revision");

        let mut wrong_revision = revision.clone();
        wrong_revision.model_schema_hash = ContentId::new("schema-b");
        let error = workload
            .verify_revision(&wrong_revision)
            .expect_err("schema mismatch should be rejected");
        assert!(error.to_string().contains("model schema"));
    }
}
