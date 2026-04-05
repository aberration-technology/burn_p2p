use burn::{
    module::Module,
    nn::{Linear, LinearConfig, loss::CrossEntropyLossConfig},
    tensor::{ElementConversion, Int, Tensor, activation, backend::Backend},
};
use serde::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
mod native;

pub const IMAGE_DIM: usize = 28 * 28;
pub const CLASS_COUNT: usize = 10;

#[derive(Module, Debug)]
pub struct MnistModel<B: Backend> {
    hidden: Linear<B>,
    output: Linear<B>,
}

impl<B: Backend> MnistModel<B> {
    pub fn new(device: &B::Device) -> Self {
        Self {
            hidden: LinearConfig::new(IMAGE_DIM, 128).init(device),
            output: LinearConfig::new(128, CLASS_COUNT).init(device),
        }
    }

    pub fn forward(&self, inputs: Tensor<B, 2>) -> Tensor<B, 2> {
        let hidden = activation::relu(self.hidden.forward(inputs));
        self.output.forward(hidden)
    }
}

#[derive(Clone, Debug)]
pub struct MnistBatch<B: Backend> {
    pub images: Tensor<B, 2>,
    pub labels: Tensor<B, 1, Int>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MnistMetricItem {
    pub loss: f64,
    pub accuracy: f64,
    pub digit_zero_accuracy: f64,
    pub digit_zero_examples: usize,
    pub sample_count: usize,
}

pub fn inference_metrics<B>(model: &MnistModel<B>, batch: MnistBatch<B>) -> MnistMetricItem
where
    B: Backend,
    B::Device: Default,
{
    let logits = model.forward(batch.images);
    let loss = CrossEntropyLossConfig::new()
        .init(&Default::default())
        .forward(logits.clone(), batch.labels.clone());
    metric_item_from_logits(logits, batch.labels, loss)
}

pub(crate) fn metric_item_from_logits<B: Backend>(
    logits: Tensor<B, 2>,
    labels: Tensor<B, 1, Int>,
    loss: Tensor<B, 1>,
) -> MnistMetricItem {
    let [sample_count] = labels.dims();
    let predictions = logits.argmax(1).reshape([sample_count]);
    let accuracy = if sample_count == 0 {
        0.0
    } else {
        predictions
            .clone()
            .equal(labels.clone())
            .int()
            .sum()
            .into_scalar()
            .elem::<f64>()
            / sample_count as f64
    };

    let zero_mask = labels.clone().equal_elem(0);
    let zero_examples = zero_mask
        .clone()
        .int()
        .sum()
        .into_scalar()
        .elem::<i64>()
        .max(0) as usize;
    let digit_zero_accuracy = if zero_examples == 0 {
        0.0
    } else {
        predictions
            .equal(labels)
            .int()
            .mask_fill(zero_mask.bool_not(), 0)
            .sum()
            .into_scalar()
            .elem::<f64>()
            / zero_examples as f64
    };

    MnistMetricItem {
        loss: loss.into_scalar().elem::<f64>(),
        accuracy,
        digit_zero_accuracy,
        digit_zero_examples: zero_examples,
        sample_count,
    }
}
