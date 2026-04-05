use burn::nn::loss::CrossEntropyLossConfig;
use burn::tensor::backend::{AutodiffBackend, Backend};
use burn::train::{InferenceStep, ItemLazy, TrainOutput, TrainStep};

use super::{MnistBatch, MnistMetricItem, MnistModel, metric_item_from_logits};

impl ItemLazy for MnistMetricItem {
    type ItemSync = Self;

    fn sync(self) -> Self::ItemSync {
        self
    }
}

impl<B> InferenceStep for MnistModel<B>
where
    B: Backend,
    B::Device: Default,
{
    type Input = MnistBatch<B>;
    type Output = MnistMetricItem;

    fn step(&self, batch: Self::Input) -> Self::Output {
        super::inference_metrics(self, batch)
    }
}

impl<B> TrainStep for MnistModel<B>
where
    B: AutodiffBackend,
    B::Device: Default,
{
    type Input = MnistBatch<B>;
    type Output = MnistMetricItem;

    fn step(&self, batch: Self::Input) -> TrainOutput<Self::Output> {
        let logits = self.forward(batch.images.clone());
        let loss = CrossEntropyLossConfig::new()
            .init(&Default::default())
            .forward(logits.clone(), batch.labels.clone());
        let metrics = metric_item_from_logits(logits, batch.labels, loss.clone());
        let grads = loss.backward();

        TrainOutput::new(self, grads, metrics)
    }
}
