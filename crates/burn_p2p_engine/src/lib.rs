#![forbid(unsafe_code)]

use std::{collections::BTreeMap, path::PathBuf};

use burn::{
    module::{Module, ModuleMapper, ModuleVisitor, Param, ParamId},
    prelude::Backend,
    record::{
        BinBytesRecorder, BinFileRecorder, BinGzFileRecorder, FileRecorder, FullPrecisionSettings,
        HalfPrecisionSettings, JsonGzFileRecorder, NamedMpkBytesRecorder, NamedMpkFileRecorder,
        NamedMpkGzFileRecorder, PrettyJsonFileRecorder, Recorder,
    },
    tensor::{Bool, Bytes, Int, Tensor},
};
use burn_p2p_checkpoint::{
    ArtifactBuildSpec, CheckpointError, ChunkingScheme, build_artifact_descriptor_from_bytes,
    build_artifact_descriptor_from_file,
};
use burn_p2p_core::{ArtifactDescriptor, ArtifactKind, ContentId, HeadId, Precision};
use burn_store::{BurnpackStore, Collector, ModuleSnapshot, SafetensorsStore, TensorSnapshot};
use serde::{Deserialize, Serialize};

pub use burn::train::checkpoint::Checkpointer as BurnCheckpointer;
pub use burn::train::{
    Evaluator as BurnEvaluator, Learner as BurnLearner,
    LearningCheckpointer as BurnLearningCheckpointer,
};

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("schema error: {0}")]
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("checkpoint error: {0}")]
    Checkpoint(#[from] CheckpointError),
    #[error("burn recorder error: {0}")]
    Recorder(String),
    #[error("burnpack store error: {0}")]
    Burnpack(String),
    #[error("safetensors store error: {0}")]
    Safetensors(String),
    #[error("tensor snapshot error: {0}")]
    TensorSnapshot(String),
    #[error("tensor data error: {0}")]
    TensorData(String),
    #[error("module merge error: {0}")]
    ModuleMerge(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BurnRecordPrecision {
    Full,
    Half,
}

impl BurnRecordPrecision {
    pub fn as_checkpoint_precision(self) -> Precision {
        match self {
            Self::Full => Precision::Fp32,
            Self::Half => Precision::Fp16,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BurnRecordFileFormat {
    Bin,
    BinGz,
    PrettyJson,
    JsonGz,
    NamedMpk,
    NamedMpkGz,
}

impl BurnRecordFileFormat {
    pub fn file_extension(self) -> &'static str {
        match self {
            Self::Bin => "bin",
            Self::BinGz => "bin.gz",
            Self::PrettyJson => "json",
            Self::JsonGz => "json.gz",
            Self::NamedMpk => "mpk",
            Self::NamedMpkGz => "mpk.gz",
        }
    }

    pub fn record_format_name(self) -> &'static str {
        match self {
            Self::Bin => "burn-record:bin",
            Self::BinGz => "burn-record:bin.gz",
            Self::PrettyJson => "burn-record:json",
            Self::JsonGz => "burn-record:json.gz",
            Self::NamedMpk => "burn-record:mpk",
            Self::NamedMpkGz => "burn-record:mpk.gz",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BurnRecordBytesFormat {
    Bin,
    NamedMpk,
}

impl BurnRecordBytesFormat {
    pub fn record_format_name(self) -> &'static str {
        match self {
            Self::Bin => "burn-record:bytes-bin",
            Self::NamedMpk => "burn-record:bytes-mpk",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BurnStoreFormat {
    Burnpack,
    Safetensors,
}

impl BurnStoreFormat {
    pub fn file_extension(self) -> &'static str {
        match self {
            Self::Burnpack => "bpk",
            Self::Safetensors => "safetensors",
        }
    }

    pub fn record_format_name(self) -> &'static str {
        match self {
            Self::Burnpack => "burn-store:burnpack",
            Self::Safetensors => "burn-store:safetensors",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BurnTensorKind {
    Float,
    Int,
    Bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BurnModuleParameter {
    pub path: String,
    pub param_id: String,
    pub kind: BurnTensorKind,
    pub shape: Vec<usize>,
    pub num_elements: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BurnModuleInventory {
    pub parameter_count: usize,
    pub total_scalar_parameters: usize,
    pub parameters: Vec<BurnModuleParameter>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BurnArtifactBytes {
    pub descriptor: ArtifactDescriptor,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BurnArtifactFile {
    pub descriptor: ArtifactDescriptor,
    pub path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct BurnArtifactOptions {
    pub artifact_kind: ArtifactKind,
    pub head_id: Option<HeadId>,
    pub base_head_id: Option<HeadId>,
    pub chunking: ChunkingScheme,
}

#[derive(Clone, Debug)]
pub struct RecordArtifactFileOptions {
    pub base_path: PathBuf,
    pub format: BurnRecordFileFormat,
    pub precision: BurnRecordPrecision,
    pub artifact: BurnArtifactOptions,
}

#[derive(Clone, Debug)]
pub struct StoreArtifactFileOptions {
    pub base_path: PathBuf,
    pub format: BurnStoreFormat,
    pub declared_precision: Precision,
    pub artifact: BurnArtifactOptions,
}

#[derive(Clone, Copy, Debug)]
pub struct BurnMergeCandidate<'a, M> {
    pub module: &'a M,
    pub weight: f64,
}

pub trait BurnModuleTarget<B: Backend>: Module<B> + ModuleSnapshot<B> {}

impl<B: Backend, M> BurnModuleTarget<B> for M where M: Module<B> + ModuleSnapshot<B> {}

struct FloatTensorReplaceMapper {
    path_stack: Vec<String>,
    replacements: BTreeMap<String, burn::tensor::TensorData>,
}

impl FloatTensorReplaceMapper {
    fn new(replacements: BTreeMap<String, burn::tensor::TensorData>) -> Self {
        Self {
            path_stack: Vec::new(),
            replacements,
        }
    }
}

impl<B: Backend> ModuleMapper<B> for FloatTensorReplaceMapper {
    fn enter_module(&mut self, name: &str, _container_type: &str) {
        self.path_stack.push(name.to_string());
    }

    fn exit_module(&mut self, _name: &str, _container_type: &str) {
        self.path_stack.pop();
    }

    fn map_float<const D: usize>(&mut self, param: Param<Tensor<B, D>>) -> Param<Tensor<B, D>> {
        let path = self.path_stack.join(".");
        let Some(data) = self.replacements.remove(&path) else {
            return param;
        };

        let (id, tensor, mapper) = param.consume();
        let device = tensor.device();
        let require_grad = tensor.is_require_grad();
        let mut replacement = Tensor::<B, D>::from_data(data.convert::<B::FloatElem>(), &device);
        if require_grad {
            replacement = replacement.require_grad();
        }

        Param::from_mapped_value(id, replacement, mapper)
    }
}

macro_rules! with_precision_settings {
    ($precision:expr, |$settings:ident| $body:expr) => {{
        match $precision {
            BurnRecordPrecision::Full => {
                type $settings = FullPrecisionSettings;
                $body
            }
            BurnRecordPrecision::Half => {
                type $settings = HalfPrecisionSettings;
                $body
            }
        }
    }};
}

pub fn inspect_module<B, M>(module: &M) -> BurnModuleInventory
where
    B: Backend,
    M: Module<B>,
{
    #[derive(Default)]
    struct InventoryVisitor {
        path_stack: Vec<String>,
        parameters: Vec<BurnModuleParameter>,
    }

    impl<B: Backend> ModuleVisitor<B> for InventoryVisitor {
        fn enter_module(&mut self, name: &str, _container_type: &str) {
            self.path_stack.push(name.to_string());
        }

        fn exit_module(&mut self, _name: &str, _container_type: &str) {
            self.path_stack.pop();
        }

        fn visit_float<const D: usize>(&mut self, param: &Param<Tensor<B, D>>) {
            let tensor = param.val();
            self.parameters.push(BurnModuleParameter {
                path: self.path_stack.join("."),
                param_id: param.id.to_string(),
                kind: BurnTensorKind::Float,
                shape: tensor.dims().into_iter().collect(),
                num_elements: tensor.shape().num_elements(),
            });
        }

        fn visit_int<const D: usize>(&mut self, param: &Param<Tensor<B, D, Int>>) {
            let tensor = param.val();
            self.parameters.push(BurnModuleParameter {
                path: self.path_stack.join("."),
                param_id: param.id.to_string(),
                kind: BurnTensorKind::Int,
                shape: tensor.dims().into_iter().collect(),
                num_elements: tensor.shape().num_elements(),
            });
        }

        fn visit_bool<const D: usize>(&mut self, param: &Param<Tensor<B, D, Bool>>) {
            let tensor = param.val();
            self.parameters.push(BurnModuleParameter {
                path: self.path_stack.join("."),
                param_id: param.id.to_string(),
                kind: BurnTensorKind::Bool,
                shape: tensor.dims().into_iter().collect(),
                num_elements: tensor.shape().num_elements(),
            });
        }

        fn visit_float_with_path<const D: usize>(
            &mut self,
            path: &[String],
            id: ParamId,
            tensor: &Tensor<B, D>,
        ) {
            self.parameters.push(BurnModuleParameter {
                path: path.join("."),
                param_id: id.to_string(),
                kind: BurnTensorKind::Float,
                shape: tensor.dims().into_iter().collect(),
                num_elements: tensor.shape().num_elements(),
            });
        }

        fn visit_int_with_path<const D: usize>(
            &mut self,
            path: &[String],
            id: ParamId,
            tensor: &Tensor<B, D, Int>,
        ) {
            self.parameters.push(BurnModuleParameter {
                path: path.join("."),
                param_id: id.to_string(),
                kind: BurnTensorKind::Int,
                shape: tensor.dims().into_iter().collect(),
                num_elements: tensor.shape().num_elements(),
            });
        }

        fn visit_bool_with_path<const D: usize>(
            &mut self,
            path: &[String],
            id: ParamId,
            tensor: &Tensor<B, D, Bool>,
        ) {
            self.parameters.push(BurnModuleParameter {
                path: path.join("."),
                param_id: id.to_string(),
                kind: BurnTensorKind::Bool,
                shape: tensor.dims().into_iter().collect(),
                num_elements: tensor.shape().num_elements(),
            });
        }
    }

    let mut visitor = InventoryVisitor::default();
    module.visit(&mut visitor);
    visitor.parameters.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then(left.param_id.cmp(&right.param_id))
    });

    BurnModuleInventory {
        parameter_count: visitor.parameters.len(),
        total_scalar_parameters: module.num_params(),
        parameters: visitor.parameters,
    }
}

pub fn module_schema_hash<B, M>(module: &M) -> Result<ContentId, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    let inventory = inspect_module::<B, M>(module);
    let shape_only = inventory
        .parameters
        .into_iter()
        .map(|parameter| {
            (
                parameter.path,
                parameter.kind,
                parameter.shape,
                parameter.num_elements,
            )
        })
        .collect::<Vec<_>>();

    Ok(ContentId::derive(&shape_only)?)
}

pub fn merge_weighted_mean_modules<B, M>(
    base_module: &M,
    candidates: &[BurnMergeCandidate<'_, M>],
) -> Result<Option<M>, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    if candidates.is_empty() {
        return Ok(None);
    }

    let base_snapshots = collect_float_snapshots::<B, M>(base_module)?;
    let mut candidate_snapshots = Vec::with_capacity(candidates.len());
    let mut total_weight = 0.0_f64;

    for candidate in candidates {
        if !candidate.weight.is_finite() || candidate.weight < 0.0 {
            return Err(EngineError::ModuleMerge(format!(
                "candidate weight must be finite and non-negative, got {}",
                candidate.weight
            )));
        }
        total_weight += candidate.weight;
        let snapshots = collect_float_snapshots::<B, M>(candidate.module)?;
        validate_snapshot_layout(&base_snapshots, &snapshots)?;
        candidate_snapshots.push((candidate.weight, snapshots));
    }

    if total_weight <= f64::EPSILON {
        return Ok(Some(base_module.clone()));
    }

    let mut replacements = BTreeMap::new();
    for (path, base_snapshot) in &base_snapshots {
        let mut weighted_inputs = Vec::with_capacity(candidate_snapshots.len());
        for (weight, snapshots) in &candidate_snapshots {
            let snapshot = snapshots.get(path).ok_or_else(|| {
                EngineError::ModuleMerge(format!("missing candidate tensor for path {path}"))
            })?;
            weighted_inputs.push((*weight, snapshot));
        }
        replacements.insert(
            path.clone(),
            weighted_mean_tensor_data(base_snapshot, &weighted_inputs)?,
        );
    }

    replace_float_tensors::<B, M>(base_module, replacements).map(Some)
}

pub fn apply_root_ema_modules<B, M>(
    base_module: &M,
    merged_module: &M,
    decay: f64,
) -> Result<M, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    if !decay.is_finite() || !(0.0..=1.0).contains(&decay) {
        return Err(EngineError::ModuleMerge(format!(
            "ema decay must be finite and within [0, 1], got {decay}"
        )));
    }

    if decay <= f64::EPSILON {
        return Ok(base_module.clone());
    }
    if (1.0 - decay).abs() <= f64::EPSILON {
        return Ok(merged_module.clone());
    }

    let base_snapshots = collect_float_snapshots::<B, M>(base_module)?;
    let merged_snapshots = collect_float_snapshots::<B, M>(merged_module)?;
    validate_snapshot_layout(&base_snapshots, &merged_snapshots)?;

    let mut replacements = BTreeMap::new();
    for (path, base_snapshot) in &base_snapshots {
        let merged_snapshot = merged_snapshots.get(path).ok_or_else(|| {
            EngineError::ModuleMerge(format!("missing merged tensor for path {path}"))
        })?;
        replacements.insert(
            path.clone(),
            ema_tensor_data(base_snapshot, merged_snapshot, decay)?,
        );
    }

    replace_float_tensors::<B, M>(base_module, replacements)
}

fn collect_float_snapshots<B, M>(
    module: &M,
) -> Result<BTreeMap<String, TensorSnapshot>, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    let mut collector = Collector::default();
    module.visit(&mut collector);

    let mut snapshots = BTreeMap::new();
    for snapshot in collector.into_tensors() {
        if snapshot.dtype.is_float() {
            snapshots.insert(snapshot.full_path(), snapshot);
        }
    }

    Ok(snapshots)
}

fn validate_snapshot_layout(
    expected: &BTreeMap<String, TensorSnapshot>,
    actual: &BTreeMap<String, TensorSnapshot>,
) -> Result<(), EngineError> {
    if expected.len() != actual.len() {
        return Err(EngineError::ModuleMerge(format!(
            "candidate float parameter count mismatch: expected {}, got {}",
            expected.len(),
            actual.len()
        )));
    }

    for (path, expected_snapshot) in expected {
        let actual_snapshot = actual.get(path).ok_or_else(|| {
            EngineError::ModuleMerge(format!("candidate is missing tensor at path {path}"))
        })?;
        if expected_snapshot.shape != actual_snapshot.shape {
            return Err(EngineError::ModuleMerge(format!(
                "candidate tensor shape mismatch at {path}: expected {:?}, got {:?}",
                expected_snapshot.shape, actual_snapshot.shape
            )));
        }
    }

    Ok(())
}

fn weighted_mean_tensor_data(
    reference: &TensorSnapshot,
    inputs: &[(f64, &TensorSnapshot)],
) -> Result<burn::tensor::TensorData, EngineError> {
    let shape = reference.shape.clone();
    let element_count: usize = shape.iter().product();
    let mut accum = vec![0.0_f64; element_count];
    let mut total_weight = 0.0_f64;

    for (weight, snapshot) in inputs {
        if *weight <= f64::EPSILON {
            continue;
        }
        let data = snapshot
            .to_data()
            .map_err(|error| EngineError::TensorSnapshot(error.to_string()))?;
        if data.shape != shape {
            return Err(EngineError::ModuleMerge(format!(
                "tensor shape mismatch for weighted mean at {}: expected {:?}, got {:?}",
                reference.full_path(),
                shape,
                data.shape
            )));
        }

        for (slot, value) in accum.iter_mut().zip(data.iter::<f64>()) {
            *slot += value * *weight;
        }
        total_weight += *weight;
    }

    if total_weight <= f64::EPSILON {
        return reference
            .to_data()
            .map_err(|error| EngineError::TensorSnapshot(error.to_string()));
    }

    for value in &mut accum {
        *value /= total_weight;
    }

    Ok(burn::tensor::TensorData::new(accum, shape))
}

fn ema_tensor_data(
    base: &TensorSnapshot,
    merged: &TensorSnapshot,
    decay: f64,
) -> Result<burn::tensor::TensorData, EngineError> {
    let base_data = base
        .to_data()
        .map_err(|error| EngineError::TensorSnapshot(error.to_string()))?;
    let merged_data = merged
        .to_data()
        .map_err(|error| EngineError::TensorSnapshot(error.to_string()))?;
    if base_data.shape != merged_data.shape {
        return Err(EngineError::ModuleMerge(format!(
            "tensor shape mismatch for root ema at {}: expected {:?}, got {:?}",
            base.full_path(),
            base_data.shape,
            merged_data.shape
        )));
    }

    let blended = base_data
        .iter::<f64>()
        .zip(merged_data.iter::<f64>())
        .map(|(base_value, merged_value)| (base_value * (1.0 - decay)) + (merged_value * decay))
        .collect::<Vec<_>>();

    Ok(burn::tensor::TensorData::new(blended, base_data.shape))
}

fn replace_float_tensors<B, M>(
    base_module: &M,
    replacements: BTreeMap<String, burn::tensor::TensorData>,
) -> Result<M, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    let mut mapper = FloatTensorReplaceMapper::new(replacements);
    let merged = base_module.clone().map(&mut mapper);
    if !mapper.replacements.is_empty() {
        return Err(EngineError::ModuleMerge(format!(
            "unused tensor replacements remained after module mapping: {:?}",
            mapper.replacements.keys().cloned().collect::<Vec<_>>()
        )));
    }
    Ok(merged)
}

pub fn save_record_file<B, M>(
    module: M,
    base_path: impl Into<PathBuf>,
    format: BurnRecordFileFormat,
    precision: BurnRecordPrecision,
) -> Result<PathBuf, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    let base_path = base_path.into();
    with_precision_settings!(precision, |Settings| {
        match format {
            BurnRecordFileFormat::Bin => {
                save_record_file_with_recorder::<B, M, BinFileRecorder<Settings>>(module, base_path)
            }
            BurnRecordFileFormat::BinGz => {
                save_record_file_with_recorder::<B, M, BinGzFileRecorder<Settings>>(
                    module, base_path,
                )
            }
            BurnRecordFileFormat::PrettyJson => {
                save_record_file_with_recorder::<B, M, PrettyJsonFileRecorder<Settings>>(
                    module, base_path,
                )
            }
            BurnRecordFileFormat::JsonGz => {
                save_record_file_with_recorder::<B, M, JsonGzFileRecorder<Settings>>(
                    module, base_path,
                )
            }
            BurnRecordFileFormat::NamedMpk => {
                save_record_file_with_recorder::<B, M, NamedMpkFileRecorder<Settings>>(
                    module, base_path,
                )
            }
            BurnRecordFileFormat::NamedMpkGz => {
                save_record_file_with_recorder::<B, M, NamedMpkGzFileRecorder<Settings>>(
                    module, base_path,
                )
            }
        }
    })
}

pub fn load_record_file<B, M>(
    module: M,
    base_path: impl Into<PathBuf>,
    format: BurnRecordFileFormat,
    precision: BurnRecordPrecision,
    device: &B::Device,
) -> Result<M, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    let base_path = base_path.into();
    with_precision_settings!(precision, |Settings| {
        match format {
            BurnRecordFileFormat::Bin => {
                load_record_file_with_recorder::<B, M, BinFileRecorder<Settings>>(
                    module, base_path, device,
                )
            }
            BurnRecordFileFormat::BinGz => {
                load_record_file_with_recorder::<B, M, BinGzFileRecorder<Settings>>(
                    module, base_path, device,
                )
            }
            BurnRecordFileFormat::PrettyJson => {
                load_record_file_with_recorder::<B, M, PrettyJsonFileRecorder<Settings>>(
                    module, base_path, device,
                )
            }
            BurnRecordFileFormat::JsonGz => {
                load_record_file_with_recorder::<B, M, JsonGzFileRecorder<Settings>>(
                    module, base_path, device,
                )
            }
            BurnRecordFileFormat::NamedMpk => {
                load_record_file_with_recorder::<B, M, NamedMpkFileRecorder<Settings>>(
                    module, base_path, device,
                )
            }
            BurnRecordFileFormat::NamedMpkGz => {
                load_record_file_with_recorder::<B, M, NamedMpkGzFileRecorder<Settings>>(
                    module, base_path, device,
                )
            }
        }
    })
}

pub fn encode_record_bytes<B, M>(
    module: M,
    format: BurnRecordBytesFormat,
    precision: BurnRecordPrecision,
) -> Result<Vec<u8>, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    with_precision_settings!(precision, |Settings| {
        match format {
            BurnRecordBytesFormat::Bin => {
                encode_record_bytes_with_recorder::<B, M, BinBytesRecorder<Settings>>(module)
            }
            BurnRecordBytesFormat::NamedMpk => {
                encode_record_bytes_with_recorder::<B, M, NamedMpkBytesRecorder<Settings>>(module)
            }
        }
    })
}

pub fn load_record_bytes<B, M>(
    module: M,
    bytes: Vec<u8>,
    format: BurnRecordBytesFormat,
    precision: BurnRecordPrecision,
    device: &B::Device,
) -> Result<M, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    with_precision_settings!(precision, |Settings| {
        match format {
            BurnRecordBytesFormat::Bin => {
                load_record_bytes_with_recorder::<B, M, BinBytesRecorder<Settings>>(
                    module, bytes, device,
                )
            }
            BurnRecordBytesFormat::NamedMpk => {
                load_record_bytes_with_recorder::<B, M, NamedMpkBytesRecorder<Settings>>(
                    module, bytes, device,
                )
            }
        }
    })
}

pub fn save_store_file<B, M>(
    module: &M,
    base_path: impl Into<PathBuf>,
    format: BurnStoreFormat,
) -> Result<PathBuf, EngineError>
where
    B: Backend,
    M: BurnModuleTarget<B>,
{
    let path = store_file_path(base_path.into(), format);

    match format {
        BurnStoreFormat::Burnpack => {
            let mut store = BurnpackStore::from_file(&path);
            module
                .save_into(&mut store)
                .map_err(|error| EngineError::Burnpack(error.to_string()))?;
        }
        BurnStoreFormat::Safetensors => {
            let mut store = SafetensorsStore::from_file(&path);
            module
                .save_into(&mut store)
                .map_err(|error| EngineError::Safetensors(error.to_string()))?;
        }
    }

    Ok(path)
}

pub fn load_store_file<B, M>(
    module: &mut M,
    path: impl Into<PathBuf>,
    format: BurnStoreFormat,
) -> Result<(), EngineError>
where
    B: Backend,
    M: BurnModuleTarget<B>,
{
    let path = store_file_path(path.into(), format);

    match format {
        BurnStoreFormat::Burnpack => {
            let mut store = BurnpackStore::from_file(path);
            module
                .load_from(&mut store)
                .map_err(|error| EngineError::Burnpack(error.to_string()))?;
        }
        BurnStoreFormat::Safetensors => {
            let mut store = SafetensorsStore::from_file(path);
            module
                .load_from(&mut store)
                .map_err(|error| EngineError::Safetensors(error.to_string()))?;
        }
    }

    Ok(())
}

pub fn encode_store_bytes<B, M>(module: &M, format: BurnStoreFormat) -> Result<Vec<u8>, EngineError>
where
    B: Backend,
    M: BurnModuleTarget<B>,
{
    match format {
        BurnStoreFormat::Burnpack => {
            let mut store = BurnpackStore::from_bytes(None);
            module
                .save_into(&mut store)
                .map_err(|error| EngineError::Burnpack(error.to_string()))?;
            let bytes = store
                .get_bytes()
                .map_err(|error| EngineError::Burnpack(error.to_string()))?;
            Ok(bytes.to_vec())
        }
        BurnStoreFormat::Safetensors => {
            let mut store = SafetensorsStore::from_bytes(None);
            module
                .save_into(&mut store)
                .map_err(|error| EngineError::Safetensors(error.to_string()))?;
            store
                .get_bytes()
                .map_err(|error| EngineError::Safetensors(error.to_string()))
        }
    }
}

pub fn load_store_bytes<B, M>(
    module: &mut M,
    bytes: Vec<u8>,
    format: BurnStoreFormat,
) -> Result<(), EngineError>
where
    B: Backend,
    M: BurnModuleTarget<B>,
{
    match format {
        BurnStoreFormat::Burnpack => {
            let mut store = BurnpackStore::from_bytes(Some(Bytes::from_bytes_vec(bytes)));
            module
                .load_from(&mut store)
                .map_err(|error| EngineError::Burnpack(error.to_string()))?;
        }
        BurnStoreFormat::Safetensors => {
            let mut store = SafetensorsStore::from_bytes(Some(bytes));
            module
                .load_from(&mut store)
                .map_err(|error| EngineError::Safetensors(error.to_string()))?;
        }
    }

    Ok(())
}

pub fn materialize_record_file_artifact<B, M>(
    module: M,
    options: RecordArtifactFileOptions,
) -> Result<BurnArtifactFile, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    let RecordArtifactFileOptions {
        base_path,
        format,
        precision,
        artifact,
    } = options;
    let schema_hash = module_schema_hash::<B, M>(&module)?;
    let path = save_record_file::<B, M>(module, base_path, format, precision)?;
    let descriptor = build_artifact_descriptor_from_file(
        &artifact_build_spec(
            artifact.artifact_kind,
            artifact.head_id,
            artifact.base_head_id,
            precision.as_checkpoint_precision(),
            schema_hash,
            format.record_format_name(),
        ),
        &path,
        artifact.chunking,
    )?;

    Ok(BurnArtifactFile { descriptor, path })
}

pub fn materialize_record_bytes_artifact<B, M>(
    module: M,
    format: BurnRecordBytesFormat,
    precision: BurnRecordPrecision,
    artifact_kind: ArtifactKind,
    head_id: Option<HeadId>,
    base_head_id: Option<HeadId>,
    chunking: ChunkingScheme,
) -> Result<BurnArtifactBytes, EngineError>
where
    B: Backend,
    M: Module<B>,
{
    let schema_hash = module_schema_hash::<B, M>(&module)?;
    let bytes = encode_record_bytes::<B, M>(module, format, precision)?;
    let descriptor = build_artifact_descriptor_from_bytes(
        &artifact_build_spec(
            artifact_kind,
            head_id,
            base_head_id,
            precision.as_checkpoint_precision(),
            schema_hash,
            format.record_format_name(),
        ),
        &bytes,
        chunking,
    )?;

    Ok(BurnArtifactBytes { descriptor, bytes })
}

pub fn materialize_store_file_artifact<B, M>(
    module: &M,
    options: StoreArtifactFileOptions,
) -> Result<BurnArtifactFile, EngineError>
where
    B: Backend,
    M: BurnModuleTarget<B>,
{
    let StoreArtifactFileOptions {
        base_path,
        format,
        declared_precision,
        artifact,
    } = options;
    let schema_hash = module_schema_hash::<B, M>(module)?;
    let path = save_store_file::<B, M>(module, base_path, format)?;
    let descriptor = build_artifact_descriptor_from_file(
        &artifact_build_spec(
            artifact.artifact_kind,
            artifact.head_id,
            artifact.base_head_id,
            declared_precision,
            schema_hash,
            format.record_format_name(),
        ),
        &path,
        artifact.chunking,
    )?;

    Ok(BurnArtifactFile { descriptor, path })
}

pub fn materialize_store_bytes_artifact<B, M>(
    module: &M,
    format: BurnStoreFormat,
    declared_precision: Precision,
    artifact_kind: ArtifactKind,
    head_id: Option<HeadId>,
    base_head_id: Option<HeadId>,
    chunking: ChunkingScheme,
) -> Result<BurnArtifactBytes, EngineError>
where
    B: Backend,
    M: BurnModuleTarget<B>,
{
    let schema_hash = module_schema_hash::<B, M>(module)?;
    let bytes = encode_store_bytes::<B, M>(module, format)?;
    let descriptor = build_artifact_descriptor_from_bytes(
        &artifact_build_spec(
            artifact_kind,
            head_id,
            base_head_id,
            declared_precision,
            schema_hash,
            format.record_format_name(),
        ),
        &bytes,
        chunking,
    )?;

    Ok(BurnArtifactBytes { descriptor, bytes })
}

fn artifact_build_spec(
    artifact_kind: ArtifactKind,
    head_id: Option<HeadId>,
    base_head_id: Option<HeadId>,
    precision: Precision,
    model_schema_hash: ContentId,
    record_format: &str,
) -> ArtifactBuildSpec {
    let mut spec =
        ArtifactBuildSpec::new(artifact_kind, precision, model_schema_hash, record_format);

    if let Some(head_id) = head_id {
        spec = spec.with_head(head_id);
    }

    if let Some(base_head_id) = base_head_id {
        spec = spec.with_base_head(base_head_id);
    }

    spec
}

fn record_file_path(mut base_path: PathBuf, extension: &str) -> PathBuf {
    base_path.set_extension(extension);
    base_path
}

fn store_file_path(mut base_path: PathBuf, format: BurnStoreFormat) -> PathBuf {
    if base_path.extension().is_none() {
        base_path.set_extension(format.file_extension());
    }

    base_path
}

fn save_record_file_with_recorder<B, M, R>(
    module: M,
    base_path: PathBuf,
) -> Result<PathBuf, EngineError>
where
    B: Backend,
    M: Module<B>,
    R: FileRecorder<B>,
{
    let final_path = record_file_path(base_path.clone(), R::file_extension());
    module
        .save_file(base_path, &R::default())
        .map_err(|error| EngineError::Recorder(error.to_string()))?;
    Ok(final_path)
}

fn load_record_file_with_recorder<B, M, R>(
    module: M,
    base_path: PathBuf,
    device: &B::Device,
) -> Result<M, EngineError>
where
    B: Backend,
    M: Module<B>,
    R: FileRecorder<B>,
{
    module
        .load_file(base_path, &R::default(), device)
        .map_err(|error| EngineError::Recorder(error.to_string()))
}

fn encode_record_bytes_with_recorder<B, M, R>(module: M) -> Result<Vec<u8>, EngineError>
where
    B: Backend,
    M: Module<B>,
    R: Recorder<B, RecordArgs = (), RecordOutput = Vec<u8>, LoadArgs = Vec<u8>>,
{
    R::default()
        .record(module.into_record(), ())
        .map_err(|error| EngineError::Recorder(error.to_string()))
}

fn load_record_bytes_with_recorder<B, M, R>(
    module: M,
    bytes: Vec<u8>,
    device: &B::Device,
) -> Result<M, EngineError>
where
    B: Backend,
    M: Module<B>,
    R: Recorder<B, RecordArgs = (), RecordOutput = Vec<u8>, LoadArgs = Vec<u8>>,
{
    let record = R::default()
        .load(bytes, device)
        .map_err(|error| EngineError::Recorder(error.to_string()))?;
    Ok(module.load_record(record))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use burn::{
        backend::NdArray,
        module::{Module, ModuleMapper, Param},
        nn::{Linear, LinearConfig},
        tensor::{Tensor, backend::Backend},
    };
    use burn_p2p_core::Precision;
    use tempfile::tempdir;

    use super::{
        BurnArtifactOptions, BurnMergeCandidate, BurnRecordBytesFormat, BurnRecordFileFormat,
        BurnRecordPrecision, BurnStoreFormat, RecordArtifactFileOptions, apply_root_ema_modules,
        encode_record_bytes, encode_store_bytes, inspect_module, load_record_bytes,
        load_store_bytes, materialize_record_file_artifact, materialize_store_bytes_artifact,
        merge_weighted_mean_modules, module_schema_hash,
    };
    use burn_p2p_checkpoint::ChunkingScheme;
    use burn_p2p_core::ArtifactKind;

    #[derive(Module, Debug)]
    struct TinyModel<B: Backend> {
        linear: Linear<B>,
    }

    impl<B: Backend> TinyModel<B> {
        fn new(device: &B::Device) -> Self {
            Self {
                linear: LinearConfig::new(4, 2).init(device),
            }
        }
    }

    type TestBackend = NdArray<f32>;

    #[derive(Debug)]
    struct FillMapper {
        value: f32,
    }

    impl<B: Backend> ModuleMapper<B> for FillMapper {
        fn map_float<const D: usize>(&mut self, param: Param<Tensor<B, D>>) -> Param<Tensor<B, D>> {
            param.map(|tensor| tensor.zeros_like() + self.value)
        }
    }

    fn fill_model(model: TinyModel<TestBackend>, value: f32) -> TinyModel<TestBackend> {
        let mut mapper = FillMapper { value };
        model.map(&mut mapper)
    }

    fn assert_all_close(values: &[f32], expected: f32) {
        assert!(values.iter().all(|value| (*value - expected).abs() < 1e-5));
    }

    #[test]
    fn module_inventory_reports_parameter_paths() {
        let device = <TestBackend as Backend>::Device::default();
        let model = TinyModel::<TestBackend>::new(&device);
        let inventory = inspect_module::<TestBackend, _>(&model);

        assert_eq!(inventory.parameter_count, 2);
        assert_eq!(inventory.total_scalar_parameters, 10);
        assert!(
            inventory
                .parameters
                .iter()
                .any(|parameter| parameter.path == "linear.weight")
        );
        assert!(
            inventory
                .parameters
                .iter()
                .any(|parameter| parameter.path == "linear.bias")
        );
    }

    #[test]
    fn record_bytes_round_trip_restores_the_same_payload() {
        let device = <TestBackend as Backend>::Device::default();
        let model = TinyModel::<TestBackend>::new(&device);
        let encoded = encode_record_bytes::<TestBackend, _>(
            model,
            BurnRecordBytesFormat::NamedMpk,
            BurnRecordPrecision::Full,
        )
        .expect("encode");

        let restored = load_record_bytes::<TestBackend, _>(
            TinyModel::<TestBackend>::new(&device),
            encoded.clone(),
            BurnRecordBytesFormat::NamedMpk,
            BurnRecordPrecision::Full,
            &device,
        )
        .expect("load");

        let reencoded = encode_record_bytes::<TestBackend, _>(
            restored,
            BurnRecordBytesFormat::NamedMpk,
            BurnRecordPrecision::Full,
        )
        .expect("reencode");

        assert_eq!(encoded, reencoded);
    }

    #[test]
    fn store_bytes_round_trip_restores_the_same_payload() {
        let device = <TestBackend as Backend>::Device::default();
        let model = TinyModel::<TestBackend>::new(&device);
        let encoded = encode_store_bytes::<TestBackend, _>(&model, BurnStoreFormat::Safetensors)
            .expect("encode");

        let mut restored = TinyModel::<TestBackend>::new(&device);
        load_store_bytes::<TestBackend, _>(
            &mut restored,
            encoded.clone(),
            BurnStoreFormat::Safetensors,
        )
        .expect("load");

        assert_eq!(
            model.linear.weight.to_data(),
            restored.linear.weight.to_data()
        );
        assert_eq!(
            model.linear.bias.as_ref().expect("bias").to_data(),
            restored.linear.bias.as_ref().expect("bias").to_data()
        );
    }

    #[test]
    fn schema_hash_is_stable_for_equivalent_models() {
        let device = <TestBackend as Backend>::Device::default();
        let first = TinyModel::<TestBackend>::new(&device);
        let second = TinyModel::<TestBackend>::new(&device);

        let first_hash = module_schema_hash::<TestBackend, _>(&first).expect("hash");
        let second_hash = module_schema_hash::<TestBackend, _>(&second).expect("hash");

        assert_eq!(first_hash, second_hash);
    }

    #[test]
    fn materialized_store_bytes_include_checkpoint_descriptor() {
        let device = <TestBackend as Backend>::Device::default();
        let model = TinyModel::<TestBackend>::new(&device);

        let artifact = materialize_store_bytes_artifact::<TestBackend, _>(
            &model,
            BurnStoreFormat::Burnpack,
            Precision::Fp32,
            ArtifactKind::FullHead,
            None,
            None,
            ChunkingScheme::new(64).expect("chunking"),
        )
        .expect("artifact");

        assert_eq!(artifact.descriptor.kind, ArtifactKind::FullHead);
        assert_eq!(artifact.descriptor.record_format, "burn-store:burnpack");
        assert!(!artifact.descriptor.chunks.is_empty());
        assert!(!artifact.bytes.is_empty());
    }

    #[test]
    fn materialized_record_file_uses_expected_extension() {
        let device = <TestBackend as Backend>::Device::default();
        let model = TinyModel::<TestBackend>::new(&device);
        let dir = tempdir().expect("tempdir");
        let base = dir.path().join("checkpoint");

        let artifact = materialize_record_file_artifact::<TestBackend, _>(
            model,
            RecordArtifactFileOptions {
                base_path: base,
                format: BurnRecordFileFormat::BinGz,
                precision: BurnRecordPrecision::Half,
                artifact: BurnArtifactOptions {
                    artifact_kind: ArtifactKind::ServeHead,
                    head_id: None,
                    base_head_id: None,
                    chunking: ChunkingScheme::new(64).expect("chunking"),
                },
            },
        )
        .expect("artifact");

        assert!(artifact.path.ends_with(Path::new("checkpoint.bin.gz")));
        assert_eq!(artifact.descriptor.precision, Precision::Fp16);
        assert_eq!(artifact.descriptor.record_format, "burn-record:bin.gz");
    }

    #[test]
    fn weighted_mean_merge_combines_float_parameters_parameterwise() {
        let device = <TestBackend as Backend>::Device::default();
        let base = fill_model(TinyModel::<TestBackend>::new(&device), 0.0);
        let left = fill_model(TinyModel::<TestBackend>::new(&device), 2.0);
        let right = fill_model(TinyModel::<TestBackend>::new(&device), 6.0);

        let merged = merge_weighted_mean_modules::<TestBackend, _>(
            &base,
            &[
                BurnMergeCandidate {
                    module: &left,
                    weight: 1.0,
                },
                BurnMergeCandidate {
                    module: &right,
                    weight: 3.0,
                },
            ],
        )
        .expect("merge")
        .expect("merged model");

        assert_all_close(
            &merged
                .linear
                .weight
                .to_data()
                .to_vec::<f32>()
                .expect("weight data"),
            5.0,
        );
        assert_all_close(
            &merged
                .linear
                .bias
                .as_ref()
                .expect("bias")
                .to_data()
                .to_vec::<f32>()
                .expect("bias data"),
            5.0,
        );
    }

    #[test]
    fn root_ema_is_applied_once_at_the_root_model() {
        let device = <TestBackend as Backend>::Device::default();
        let base = fill_model(TinyModel::<TestBackend>::new(&device), 2.0);
        let merged = fill_model(TinyModel::<TestBackend>::new(&device), 6.0);

        let ema =
            apply_root_ema_modules::<TestBackend, _>(&base, &merged, 0.25).expect("apply root ema");

        assert_all_close(
            &ema.linear
                .weight
                .to_data()
                .to_vec::<f32>()
                .expect("weight data"),
            3.0,
        );
        assert_all_close(
            &ema.linear
                .bias
                .as_ref()
                .expect("bias")
                .to_data()
                .to_vec::<f32>()
                .expect("bias data"),
            3.0,
        );
    }
}
