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
pub struct RecordBytesRuntimeArtifactOptions {
    pub artifact_kind: ArtifactKind,
    pub head_id: HeadId,
    pub base_head_id: Option<HeadId>,
    pub format: BurnRecordBytesFormat,
    pub precision: BurnRecordPrecision,
    pub chunking: ChunkingScheme,
}

#[derive(Clone, Debug)]
pub struct StoreBytesRuntimeArtifactOptions {
    pub artifact_kind: ArtifactKind,
    pub head_id: HeadId,
    pub base_head_id: Option<HeadId>,
    pub format: BurnStoreFormat,
    pub declared_precision: Precision,
    pub chunking: ChunkingScheme,
}

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
