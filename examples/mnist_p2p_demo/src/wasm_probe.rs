use std::collections::BTreeSet;

use burn::{
    backend::{Autodiff, WebGpu, wgpu},
    module::AutodiffModule,
    optim::{AdamConfig, GradientsParams, Optimizer},
    tensor::{ElementConversion, Int, Tensor, backend::Backend},
};
use burn_p2p::{
    ContentId, ExperimentId, ExperimentScope, PeerId, PeerRole, PeerRoleSet, PrincipalId,
    RevisionId, RuntimeTransportPolicy, StudyId, WorkloadId,
};
use burn_p2p_browser::{
    BrowserCapabilityReport, BrowserEdgeClient, BrowserEnrollmentConfig, BrowserGpuSupport,
    BrowserRuntimeConfig, BrowserRuntimeRole, BrowserSessionState, BrowserTrainingBudget,
    BrowserTrainingPlan, BrowserTransportPolicy, BrowserTransportStatus, BrowserUiBindings,
    BrowserWorkerCommand, BrowserWorkerEvent, BrowserWorkerIdentity, BrowserWorkerRuntime,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserLiveParticipantConfig {
    pub edge_base_url: String,
    pub network_id: String,
    pub experiment_id: String,
    pub revision_id: String,
    pub selected_head_id: String,
    pub lease_id: String,
    pub leased_microshards: Vec<String>,
    pub release_train_hash: String,
    pub target_artifact_id: String,
    pub target_artifact_hash: String,
    pub workload_id: String,
    pub principal_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserLiveParticipantResult {
    pub session_enrolled: bool,
    pub session_id: Option<String>,
    pub certificate_peer_id: Option<String>,
    pub runtime_state: Option<String>,
    pub transport: Option<String>,
    pub active_assignment: bool,
    pub emitted_receipt_id: Option<String>,
    pub accepted_receipt_ids: Vec<String>,
    pub receipt_submission_accepted: bool,
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
        Some(start_live_browser_participant(live_config).await?)
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
        Some(finish_live_browser_participant(handle).await?)
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

struct LiveBrowserParticipantHandle {
    client: BrowserEdgeClient,
    session: BrowserSessionState,
    runtime: BrowserWorkerRuntime,
    config: BrowserLiveParticipantConfig,
}

async fn start_live_browser_participant(
    config: BrowserLiveParticipantConfig,
) -> Result<LiveBrowserParticipantHandle, JsValue> {
    let snapshot = reqwest::get(format!("{}/portal/snapshot", config.edge_base_url))
        .await
        .map_err(js_error)?
        .error_for_status()
        .map_err(js_error)?
        .json::<burn_p2p::BrowserEdgeSnapshot>()
        .await
        .map_err(js_error)?;
    let requested_scopes = BTreeSet::from([
        ExperimentScope::Connect,
        ExperimentScope::Train {
            experiment_id: ExperimentId::new(config.experiment_id.clone()),
        },
    ]);
    let bindings = BrowserUiBindings::from_edge_snapshot(&config.edge_base_url, &snapshot);
    let enrollment = BrowserEnrollmentConfig::from_edge_snapshot(
        &snapshot,
        config.target_artifact_id.clone(),
        ContentId::new(config.target_artifact_hash.clone()),
        requested_scopes,
        900,
    )
    .map_err(js_error)?;
    let client = BrowserEdgeClient::new(bindings, enrollment);
    let identity = BrowserWorkerIdentity {
        peer_id: PeerId::new("mnist-browser-wasm-peer"),
        peer_public_key_hex: "mnist-browser-wasm-public".into(),
        serial: 1,
        client_policy_hash: Some(ContentId::new("mnist-browser-wasm-policy")),
    };
    let enrollment_result = client
        .enroll_static_principal(
            Some(config.principal_id.clone()),
            PrincipalId::new(config.principal_id.clone()),
            &identity,
        )
        .await
        .map_err(js_error)?;
    let mut session = BrowserSessionState::default();
    session.apply_enrollment(&enrollment_result);

    let mut runtime_config = BrowserRuntimeConfig::new(
        config.edge_base_url.clone(),
        burn_p2p::NetworkId::new(config.network_id.clone()),
        ContentId::new(config.release_train_hash.clone()),
        config.target_artifact_id.clone(),
        ContentId::new(config.target_artifact_hash.clone()),
    );
    runtime_config.role = BrowserRuntimeRole::BrowserTrainerWgpu;
    runtime_config.receipt_submit_path = snapshot.paths.receipt_submit_path.clone();
    runtime_config.transport = BrowserTransportPolicy::from(
        RuntimeTransportPolicy::browser_for_roles(&PeerRoleSet::new([PeerRole::BrowserTrainerWgpu])),
    );
    runtime_config.selected_experiment = Some(ExperimentId::new(config.experiment_id.clone()));
    runtime_config.selected_revision = Some(RevisionId::new(config.revision_id.clone()));

    let mut runtime = BrowserWorkerRuntime::start(
        runtime_config,
        BrowserCapabilityReport {
            navigator_gpu_exposed: true,
            worker_gpu_exposed: true,
            gpu_support: BrowserGpuSupport::Available,
            recommended_role: BrowserRuntimeRole::BrowserTrainerWgpu,
            ..BrowserCapabilityReport::default()
        },
        BrowserTransportStatus {
            active: None,
            webrtc_direct_enabled: snapshot.transports.webrtc_direct,
            webtransport_enabled: snapshot.transports.webtransport_gateway,
            wss_fallback_enabled: snapshot.transports.wss_fallback,
            last_error: None,
        },
    );
    runtime.remember_session(session.clone());
    let _ = client
        .sync_worker_runtime(&mut runtime, Some(&session), true)
        .await
        .map_err(js_error)?;

    Ok(LiveBrowserParticipantHandle {
        client,
        session,
        runtime,
        config,
    })
}

async fn finish_live_browser_participant(
    handle: &mut LiveBrowserParticipantHandle,
) -> Result<BrowserLiveParticipantResult, JsValue> {
    let train_events = handle.runtime.apply_command(
        BrowserWorkerCommand::Train(BrowserTrainingPlan {
            study_id: StudyId::new("mnist-study"),
            experiment_id: ExperimentId::new(handle.config.experiment_id.clone()),
            revision_id: RevisionId::new(handle.config.revision_id.clone()),
            workload_id: WorkloadId::new(handle.config.workload_id.clone()),
            budget: BrowserTrainingBudget {
                max_window_secs: 12,
                requires_webgpu: true,
                ..BrowserTrainingBudget::default()
            },
        }),
        None,
        None,
    );
    let emitted_receipt_id = train_events.iter().find_map(|event| match event {
        BrowserWorkerEvent::TrainingCompleted(result) => {
            result.receipt_id.as_ref().map(|receipt_id| receipt_id.as_str().to_owned())
        }
        _ => None,
    });

    let flush_events = handle
        .client
        .flush_worker_receipts(&mut handle.runtime)
        .await
        .map_err(js_error)?;
    let accepted_receipt_ids = flush_events
        .iter()
        .find_map(|event| match event {
            BrowserWorkerEvent::ReceiptsAcknowledged { receipt_ids, .. } => Some(
                receipt_ids
                    .iter()
                    .map(|receipt_id| receipt_id.as_str().to_owned())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .unwrap_or_default();

    Ok(BrowserLiveParticipantResult {
        session_enrolled: handle.session.session.is_some() && handle.session.certificate.is_some(),
        session_id: handle
            .session
            .session_id()
            .map(|session_id| session_id.as_str().to_owned()),
        certificate_peer_id: handle
            .session
            .peer_id()
            .map(|peer_id| peer_id.as_str().to_owned()),
        runtime_state: handle.runtime.state.as_ref().map(browser_runtime_state_label),
        transport: handle
            .runtime
            .transport
            .active
            .as_ref()
            .map(browser_transport_label),
        active_assignment: handle.runtime.storage.active_assignment.is_some(),
        emitted_receipt_id,
        receipt_submission_accepted: !accepted_receipt_ids.is_empty(),
        accepted_receipt_ids,
    })
}

fn browser_runtime_state_label(state: &burn_p2p_browser::BrowserRuntimeState) -> String {
    match state {
        burn_p2p_browser::BrowserRuntimeState::ViewerOnly => "viewer-only".into(),
        burn_p2p_browser::BrowserRuntimeState::Joining { stage, .. } => match stage {
            burn_p2p_browser::BrowserJoinStage::Authenticating => "joining-authenticating".into(),
            burn_p2p_browser::BrowserJoinStage::Enrolling => "joining-enrolling".into(),
            burn_p2p_browser::BrowserJoinStage::DirectorySync => "joining-directory-sync".into(),
            burn_p2p_browser::BrowserJoinStage::HeadSync => "joining-head-sync".into(),
            burn_p2p_browser::BrowserJoinStage::TransportConnect => {
                "joining-transport-connect".into()
            }
        },
        burn_p2p_browser::BrowserRuntimeState::Observer => "observer".into(),
        burn_p2p_browser::BrowserRuntimeState::Verifier => "verifier".into(),
        burn_p2p_browser::BrowserRuntimeState::Trainer => "trainer".into(),
        burn_p2p_browser::BrowserRuntimeState::BackgroundSuspended { .. } => {
            "background-suspended".into()
        }
        burn_p2p_browser::BrowserRuntimeState::Catchup { .. } => "catchup".into(),
        burn_p2p_browser::BrowserRuntimeState::Blocked { .. } => "blocked".into(),
    }
}

fn browser_transport_label(kind: &burn_p2p_browser::BrowserTransportKind) -> String {
    match kind {
        burn_p2p_browser::BrowserTransportKind::WebRtcDirect => "webrtc-direct".into(),
        burn_p2p_browser::BrowserTransportKind::WebTransport => "webtransport".into(),
        burn_p2p_browser::BrowserTransportKind::WssFallback => "wss-fallback".into(),
    }
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
