use burn::{
    backend::{Autodiff, WebGpu, wgpu},
    module::AutodiffModule,
    optim::{AdamConfig, GradientsParams, Optimizer},
    tensor::{ElementConversion, Int, Tensor, backend::Backend},
};
use burn_p2p_e2e::{
    BrowserLiveParticipantConfig, BrowserLiveParticipantResult, finish_live_browser_participant,
    start_live_browser_participant,
};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
#[cfg(target_arch = "wasm32")]
use web_time::Instant;

use crate::{
    data::{MnistRecord, records_to_batches},
    model::{MnistBatch, MnistMetricItem, MnistModel},
};

type BrowserTrainBackend = Autodiff<WebGpu<f32>>;
type BrowserEvalBackend = WebGpu<f32>;

#[cfg(target_arch = "wasm32")]
static WEBGPU_RUNTIME_READY: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserMnistProbeConfig {
    pub train_records: Vec<MnistRecord>,
    pub eval_records: Vec<MnistRecord>,
    pub batch_size: usize,
    pub learning_rate: f64,
    #[serde(default)]
    pub max_train_batches: Option<usize>,
    #[serde(default)]
    pub live_participant: Option<BrowserLiveParticipantConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserMnistProbeResult {
    pub backend: String,
    pub train_batches: usize,
    pub train_examples: usize,
    pub eval_examples: usize,
    pub train_loss_mean: f64,
    pub train_accuracy_mean: f64,
    pub train_digit_zero_accuracy_mean: f64,
    pub eval_loss: f64,
    pub eval_accuracy: f64,
    pub eval_digit_zero_accuracy: f64,
    pub setup_time_ms: u64,
    pub training_time_ms: u64,
    pub eval_time_ms: u64,
    pub total_time_ms: u64,
    #[serde(default)]
    pub live_participant: Option<BrowserLiveParticipantResult>,
}

#[wasm_bindgen]
pub async fn run_browser_mnist_probe(config: JsValue) -> Result<JsValue, JsValue> {
    console_error_panic_hook::set_once();
    log_phase("mnist browser probe: start");
    let config: BrowserMnistProbeConfig =
        serde_wasm_bindgen::from_value(config).map_err(js_error)?;
    let total_started_at = Instant::now();
    let device = <BrowserTrainBackend as Backend>::Device::default();

    let setup_started_at = Instant::now();
    if !WEBGPU_RUNTIME_READY.swap(true, Ordering::SeqCst) {
        let _ = wgpu::init_setup_async::<wgpu::graphics::WebGpu>(&device, Default::default()).await;
    }
    let setup_time_ms = elapsed_ms(setup_started_at);
    log_phase("mnist browser probe: webgpu runtime ready");

    let mut live_participant_handle = if let Some(live_config) = config.live_participant.clone() {
        Some(
            start_live_browser_participant(live_config)
                .await
                .map_err(js_error)?,
        )
    } else {
        None
    };

    let training_started_at = Instant::now();
    let train_batches = records_to_batches::<BrowserTrainBackend>(
        &config.train_records,
        config.batch_size,
        &device,
    );
    log_phase("mnist browser probe: training batches ready");
    let mut model = MnistModel::<BrowserTrainBackend>::new(&device);
    let mut optimizer = AdamConfig::new().init();
    let mut train_loss_sum = 0.0;
    let mut train_accuracy_sum = 0.0;
    let mut train_zero_accuracy_sum = 0.0;
    let mut train_batch_count = 0usize;
    let train_examples = config.train_records.len();
    for (batch_index, batch) in train_batches.into_iter().enumerate() {
        if config
            .max_train_batches
            .map(|max_batches| batch_index >= max_batches)
            .unwrap_or(false)
        {
            break;
        }
        let logits = model.forward(batch.images.clone());
        let loss = burn::nn::loss::CrossEntropyLossConfig::new()
            .init(&Default::default())
            .forward(logits.clone(), batch.labels.clone());
        let metrics =
            metric_item_from_logits_async(logits, batch.labels.clone(), loss.clone()).await?;
        let grads = GradientsParams::from_grads(loss.backward(), &model);
        model = optimizer.step(config.learning_rate, model, grads);
        train_loss_sum += metrics.loss;
        train_accuracy_sum += metrics.accuracy;
        train_zero_accuracy_sum += metrics.digit_zero_accuracy;
        train_batch_count += 1;
    }
    let training_time_ms = elapsed_ms(training_started_at);
    log_phase("mnist browser probe: training complete");

    let eval_started_at = Instant::now();
    let eval_device = <BrowserEvalBackend as Backend>::Device::default();
    let eval_model = model.valid();
    let eval_batches = records_to_batches::<BrowserEvalBackend>(
        &config.eval_records,
        config.batch_size,
        &eval_device,
    );
    let mut eval_metrics = Vec::with_capacity(eval_batches.len());
    for batch in eval_batches {
        eval_metrics.push(inference_metrics_async(&eval_model, batch).await?);
    }
    let eval_time_ms = elapsed_ms(eval_started_at);
    log_phase("mnist browser probe: evaluation complete");

    let train_batch_count = train_batch_count.max(1);
    let eval_summary = summarize_eval(&eval_metrics);
    let live_participant = if let Some(handle) = live_participant_handle.as_mut() {
        Some(
            finish_live_browser_participant(handle)
                .await
                .map_err(js_error)?,
        )
    } else {
        None
    };
    let result = BrowserMnistProbeResult {
        backend: "burn-webgpu-wasm".into(),
        train_batches: train_batch_count,
        train_examples,
        eval_examples: config.eval_records.len(),
        train_loss_mean: train_loss_sum / train_batch_count as f64,
        train_accuracy_mean: train_accuracy_sum / train_batch_count as f64,
        train_digit_zero_accuracy_mean: train_zero_accuracy_sum / train_batch_count as f64,
        eval_loss: eval_summary.loss,
        eval_accuracy: eval_summary.accuracy,
        eval_digit_zero_accuracy: eval_summary.digit_zero_accuracy,
        setup_time_ms,
        training_time_ms,
        eval_time_ms,
        total_time_ms: elapsed_ms(total_started_at),
        live_participant,
    };
    log_phase("mnist browser probe: done");
    serde_wasm_bindgen::to_value(&result).map_err(js_error)
}

fn summarize_eval(metrics: &[MnistMetricItem]) -> MnistMetricItem {
    let mut loss_sum = 0.0;
    let mut accuracy_sum = 0.0;
    let mut sample_count = 0usize;
    let mut zero_accuracy_sum = 0.0;
    let mut zero_examples = 0usize;
    for metric in metrics {
        loss_sum += metric.loss * metric.sample_count as f64;
        accuracy_sum += metric.accuracy * metric.sample_count as f64;
        sample_count += metric.sample_count;
        zero_accuracy_sum += metric.digit_zero_accuracy * metric.digit_zero_examples as f64;
        zero_examples += metric.digit_zero_examples;
    }

    MnistMetricItem {
        loss: if sample_count == 0 {
            0.0
        } else {
            loss_sum / sample_count as f64
        },
        accuracy: if sample_count == 0 {
            0.0
        } else {
            accuracy_sum / sample_count as f64
        },
        digit_zero_accuracy: if zero_examples == 0 {
            0.0
        } else {
            zero_accuracy_sum / zero_examples as f64
        },
        digit_zero_examples: zero_examples,
        sample_count,
    }
}

async fn inference_metrics_async<B: Backend>(
    model: &MnistModel<B>,
    batch: MnistBatch<B>,
) -> Result<MnistMetricItem, JsValue> {
    let logits = model.forward(batch.images);
    let loss = burn::nn::loss::CrossEntropyLossConfig::new()
        .init(&Default::default())
        .forward(logits.clone(), batch.labels.clone());
    metric_item_from_logits_async(logits, batch.labels, loss).await
}

async fn metric_item_from_logits_async<B: Backend>(
    logits: Tensor<B, 2>,
    labels: Tensor<B, 1, Int>,
    loss: Tensor<B, 1>,
) -> Result<MnistMetricItem, JsValue> {
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
            .into_scalar_async()
            .await
            .map_err(js_error)?
            .elem::<f64>()
            / sample_count as f64
    };

    let zero_mask = labels.clone().equal_elem(0);
    let zero_examples = zero_mask
        .clone()
        .int()
        .sum()
        .into_scalar_async()
        .await
        .map_err(js_error)?
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
            .into_scalar_async()
            .await
            .map_err(js_error)?
            .elem::<f64>()
            / zero_examples as f64
    };

    Ok(MnistMetricItem {
        loss: loss
            .into_scalar_async()
            .await
            .map_err(js_error)?
            .elem::<f64>(),
        accuracy,
        digit_zero_accuracy,
        digit_zero_examples: zero_examples,
        sample_count,
    })
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis() as u64
}

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn log_phase(message: &str) {
    web_sys::console::log_1(&JsValue::from_str(message));
}
