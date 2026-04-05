use burn::tensor::{Int, Tensor, TensorData, backend::Backend};
use serde::{Deserialize, Serialize};

use crate::model::{IMAGE_DIM, MnistBatch};

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::{PreparedMnistData, prepare_mnist_dataset};

pub const EVAL_RECORDS_FILE: &str = "eval-records.json";

#[derive(Clone, Debug)]
pub struct MnistDatasetConfig {
    pub train_examples_per_digit: usize,
    pub eval_examples_per_digit: usize,
    pub microshard_count: u32,
    pub batch_size: usize,
}

impl Default for MnistDatasetConfig {
    fn default() -> Self {
        Self {
            train_examples_per_digit: 64,
            eval_examples_per_digit: 32,
            microshard_count: 8,
            batch_size: 32,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MnistRecord {
    pub label: u8,
    pub pixels: Vec<f32>,
}

#[derive(Clone, Debug, Default)]
pub struct MnistBatcher;

impl MnistBatcher {
    pub fn batch_records<B: Backend>(
        &self,
        items: Vec<MnistRecord>,
        device: &B::Device,
    ) -> MnistBatch<B> {
        let mut pixels = Vec::with_capacity(items.len() * IMAGE_DIM);
        let mut labels = Vec::with_capacity(items.len());
        for item in items {
            pixels.extend(item.pixels);
            labels.push(item.label as i64);
        }
        let batch_size = labels.len();
        MnistBatch {
            images: Tensor::from_data(TensorData::new(pixels, [batch_size, IMAGE_DIM]), device),
            labels: Tensor::<B, 1, Int>::from_data(TensorData::new(labels, [batch_size]), device),
        }
    }
}

pub fn records_to_batches<B: Backend>(
    records: &[MnistRecord],
    batch_size: usize,
    device: &B::Device,
) -> Vec<MnistBatch<B>> {
    let chunk_size = batch_size.max(1);
    let batcher = MnistBatcher;
    let mut batches = Vec::new();
    for chunk in records.chunks(chunk_size) {
        batches.push(batcher.batch_records(chunk.to_vec(), device));
    }
    batches
}
