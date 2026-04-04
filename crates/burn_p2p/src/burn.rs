use burn::{module::Module, prelude::Backend};

use crate::{ArtifactDescriptor, ArtifactKind, ChunkingScheme, FsArtifactStore, HeadId, Precision};

pub use burn::module::Module as BurnModule;
pub use burn::prelude::Backend as BurnBackend;
pub use burn_p2p_engine::{
    BurnArtifactBytes, BurnArtifactFile, BurnCheckpointer, BurnEvaluator, BurnLearner,
    BurnLearningCheckpointer, BurnMergeCandidate, BurnModuleInventory, BurnModuleParameter,
    BurnModuleTarget, BurnRecordBytesFormat, BurnRecordFileFormat, BurnRecordPrecision,
    BurnStoreFormat, BurnTensorKind, EngineError, apply_root_ema_modules, encode_record_bytes,
    encode_store_bytes, inspect_module, load_record_bytes, load_record_file, load_store_bytes,
    load_store_file, materialize_record_bytes_artifact, materialize_record_file_artifact,
    materialize_store_bytes_artifact, materialize_store_file_artifact, merge_weighted_mean_modules,
    module_schema_hash, save_record_file, save_store_file,
};

#[derive(Clone, Debug)]
/// Represents a record bytes runtime artifact options.
pub struct RecordBytesRuntimeArtifactOptions {
    /// The artifact kind.
    pub artifact_kind: ArtifactKind,
    /// The head ID.
    pub head_id: HeadId,
    /// The base head ID.
    pub base_head_id: Option<HeadId>,
    /// The format.
    pub format: BurnRecordBytesFormat,
    /// The precision.
    pub precision: BurnRecordPrecision,
    /// The chunking.
    pub chunking: ChunkingScheme,
}

#[derive(Clone, Debug)]
/// Represents a store bytes runtime artifact options.
pub struct StoreBytesRuntimeArtifactOptions {
    /// The artifact kind.
    pub artifact_kind: ArtifactKind,
    /// The head ID.
    pub head_id: HeadId,
    /// The base head ID.
    pub base_head_id: Option<HeadId>,
    /// The format.
    pub format: BurnStoreFormat,
    /// The declared precision.
    pub declared_precision: Precision,
    /// The chunking.
    pub chunking: ChunkingScheme,
}

/// Performs the load record bytes runtime artifact operation.
pub fn load_record_bytes_runtime_artifact<B, M>(
    module: M,
    descriptor: &ArtifactDescriptor,
    store: &FsArtifactStore,
    format: BurnRecordBytesFormat,
    precision: BurnRecordPrecision,
    device: &B::Device,
) -> Result<M, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    let bytes = store.materialize_artifact_bytes(descriptor)?;
    load_record_bytes::<B, M>(module, bytes, format, precision, device)
}

/// Performs the materialize record bytes runtime artifact operation.
pub fn materialize_record_bytes_runtime_artifact<B, M>(
    module: M,
    store: &FsArtifactStore,
    options: RecordBytesRuntimeArtifactOptions,
) -> Result<ArtifactDescriptor, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    let artifact = materialize_record_bytes_artifact::<B, M>(
        module,
        options.format,
        options.precision,
        options.artifact_kind,
        Some(options.head_id),
        options.base_head_id,
        options.chunking,
    )?;
    store.store_prebuilt_artifact_bytes(&artifact.descriptor, &artifact.bytes)?;
    Ok(artifact.descriptor)
}

/// Performs the load store bytes runtime artifact operation.
pub fn load_store_bytes_runtime_artifact<B, M>(
    mut module: M,
    descriptor: &ArtifactDescriptor,
    store: &FsArtifactStore,
    format: BurnStoreFormat,
) -> Result<M, EngineError>
where
    B: Backend,
    M: BurnModuleTarget<B>,
{
    let bytes = store.materialize_artifact_bytes(descriptor)?;
    load_store_bytes::<B, M>(&mut module, bytes, format)?;
    Ok(module)
}

/// Performs the materialize store bytes runtime artifact operation.
pub fn materialize_store_bytes_runtime_artifact<B, M>(
    module: &M,
    store: &FsArtifactStore,
    options: StoreBytesRuntimeArtifactOptions,
) -> Result<ArtifactDescriptor, EngineError>
where
    B: Backend,
    M: BurnModuleTarget<B>,
{
    let artifact = materialize_store_bytes_artifact::<B, M>(
        module,
        options.format,
        options.declared_precision,
        options.artifact_kind,
        Some(options.head_id),
        options.base_head_id,
        options.chunking,
    )?;
    store.store_prebuilt_artifact_bytes(&artifact.descriptor, &artifact.bytes)?;
    Ok(artifact.descriptor)
}
