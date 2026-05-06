use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, bail};
use burn_p2p_core::{CapabilityEstimate, MergePolicy, MetricValue};
use burn_p2p_experiment::RuntimePatch;
use burn_p2p_workload::{EvalSplit, PatchOutcome, TrainerCanonicalReconcileStrategy};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{PythonBatchRef, PythonStateDictFilterConfig, PythonTorchRuntimeConfig};

#[derive(Clone, Debug)]
pub(crate) struct PythonWorkerClient {
    inner: Arc<Mutex<PythonWorkerTransport>>,
}

impl PythonWorkerClient {
    pub(crate) fn spawn(config: &PythonTorchRuntimeConfig) -> anyhow::Result<Self> {
        let runtime_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python");
        let mut pythonpath_entries = vec![runtime_root];
        pythonpath_entries.extend(config.module_search_roots.iter().cloned());
        let pythonpath = join_pythonpath(&pythonpath_entries)?;
        let config_json = serde_json::to_string(&config.workload_config)
            .context("serialize python workload config")?;
        let listener =
            TcpListener::bind(("127.0.0.1", 0)).context("bind local python worker listener")?;
        let listener_addr = listener
            .local_addr()
            .context("resolve local python worker listener address")?;

        let mut command = Command::new(&config.python_executable);
        command
            .arg("-m")
            .arg("burn_p2p_python_runtime.worker")
            .arg("--factory")
            .arg(&config.workload_factory)
            .arg("--config-json")
            .arg(config_json)
            .arg("--connect-host")
            .arg(listener_addr.ip().to_string())
            .arg("--connect-port")
            .arg(listener_addr.port().to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .env("PYTHONUNBUFFERED", "1");

        if let Some(path) = pythonpath {
            command.env("PYTHONPATH", path);
        }
        for (key, value) in &config.env {
            command.env(key, value);
        }

        let mut child = command
            .spawn()
            .with_context(|| format!("spawn python worker {:?}", config.python_executable))?;
        let stream = accept_worker_connection(&listener, &mut child, Duration::from_secs(10))?;
        Ok(Self {
            inner: Arc::new(Mutex::new(PythonWorkerTransport {
                child,
                stream,
                next_request_id: 1,
            })),
        })
    }

    pub(crate) fn hello(&self) -> anyhow::Result<HelloResponse> {
        self.call::<_, HelloResponse>("hello", Value::Null)
    }

    pub(crate) fn capability_probe(&self) -> anyhow::Result<CapabilityProbeResponse> {
        self.call::<_, CapabilityProbeResponse>("capability_probe", Value::Null)
    }

    pub(crate) fn init_model(&self, device: &str) -> anyhow::Result<String> {
        let response = self.call::<_, ModelHandleResponse>(
            "init_model",
            serde_json::json!({ "device": device }),
        )?;
        Ok(response.model_id)
    }

    pub(crate) fn train_window(
        &self,
        model_id: &str,
        batches: &[PythonBatchRef],
    ) -> anyhow::Result<BTreeMap<String, MetricValue>> {
        let response = self.call::<_, MetricsResponse>(
            "train_window",
            serde_json::json!({
                "model_id": model_id,
                "batches": batches,
            }),
        )?;
        Ok(response.metrics)
    }

    pub(crate) fn evaluate(
        &self,
        model_id: &str,
        split: EvalSplit,
    ) -> anyhow::Result<BTreeMap<String, MetricValue>> {
        let response = self.call::<_, MetricsResponse>(
            "evaluate",
            serde_json::json!({
                "model_id": model_id,
                "split": split,
            }),
        )?;
        Ok(response.metrics)
    }

    pub(crate) fn apply_patch(&self, patch: &RuntimePatch) -> anyhow::Result<PatchOutcome> {
        self.call("apply_patch", patch)
    }

    pub(crate) fn load_model_artifact_path(
        &self,
        model_id: &str,
        artifact_path: &std::path::Path,
    ) -> anyhow::Result<()> {
        let mut transport = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("python worker mutex poisoned"))?;
        let _: AckResponse = transport.call::<_, AckResponse>(
            "load_model_artifact",
            serde_json::json!({
                "model_id": model_id,
                "artifact_path": artifact_path.to_string_lossy(),
            }),
        )?;
        Ok(())
    }

    pub(crate) fn materialize_model_artifact_path(
        &self,
        model_id: &str,
        artifact_path: &std::path::Path,
    ) -> anyhow::Result<()> {
        let mut transport = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("python worker mutex poisoned"))?;
        let _: AckResponse = transport.call(
            "materialize_model_artifact",
            serde_json::json!({
                "model_id": model_id,
                "artifact_path": artifact_path.to_string_lossy(),
            }),
        )?;
        Ok(())
    }

    pub(crate) fn export_parameter_pack_path(
        &self,
        model_id: &str,
        parameter_pack_path: &std::path::Path,
        model_schema_hash: &burn_p2p_core::ContentId,
        state_dict_filter: &PythonStateDictFilterConfig,
    ) -> anyhow::Result<()> {
        let _: AckResponse = self.call(
            "export_parameter_pack",
            serde_json::json!({
                "model_id": model_id,
                "parameter_pack_path": parameter_pack_path.to_string_lossy(),
                "model_schema_hash": model_schema_hash.as_str(),
                "state_dict_filter": state_dict_filter,
            }),
        )?;
        Ok(())
    }

    pub(crate) fn import_parameter_pack_path(
        &self,
        device: &str,
        parameter_pack_path: &std::path::Path,
        state_dict_filter: &PythonStateDictFilterConfig,
    ) -> anyhow::Result<String> {
        let response = self.call::<_, ModelHandleResponse>(
            "import_parameter_pack",
            serde_json::json!({
                "device": device,
                "parameter_pack_path": parameter_pack_path.to_string_lossy(),
                "state_dict_filter": state_dict_filter,
            }),
        )?;
        Ok(response.model_id)
    }

    pub(crate) fn parameter_pack_plan(
        &self,
        device: &str,
        state_dict_filter: &PythonStateDictFilterConfig,
    ) -> anyhow::Result<PythonParameterPackPlanResponse> {
        self.call(
            "parameter_pack_plan",
            serde_json::json!({
                "device": device,
                "state_dict_filter": state_dict_filter,
            }),
        )
    }

    pub(crate) fn run_inner_loop_path(
        &self,
        request: PythonDiLoCoInnerLoopPathRequest<'_>,
    ) -> anyhow::Result<PythonDiLoCoInnerLoopResponse> {
        self.call(
            "run_inner_loop",
            serde_json::json!({
                "model_id": request.model_id,
                "batches": request.batches,
                "num_inner_steps": request.num_inner_steps,
                "inner_optimizer_state_path": request.inner_optimizer_state_path.map(|path| path.to_string_lossy().into_owned()),
                "output_parameter_pack_path": request.output_parameter_pack_path.to_string_lossy(),
                "model_schema_hash": request.model_schema_hash.as_str(),
                "require_exact_steps": request.require_exact_steps,
                "state_dict_filter": request.state_dict_filter,
            }),
        )
    }

    pub(crate) fn merge_candidate_models(
        &self,
        base_model_id: &str,
        candidates: &[PythonMergeCandidateRef],
        policy: MergePolicy,
    ) -> anyhow::Result<Option<String>> {
        let response = self.call::<_, OptionalModelHandleResponse>(
            "merge_candidate_models",
            serde_json::json!({
                "base_model_id": base_model_id,
                "candidates": candidates,
                "policy": policy,
            }),
        )?;
        Ok(response.model_id)
    }

    pub(crate) fn apply_single_root_ema(
        &self,
        base_model_id: &str,
        merged_model_id: &str,
        policy: MergePolicy,
    ) -> anyhow::Result<String> {
        let response = self.call::<_, ModelHandleResponse>(
            "apply_single_root_ema",
            serde_json::json!({
                "base_model_id": base_model_id,
                "merged_model_id": merged_model_id,
                "policy": policy,
            }),
        )?;
        Ok(response.model_id)
    }

    pub(crate) fn reconcile_canonical_model(
        &self,
        local_model_id: &str,
        canonical_model_id: &str,
        strategy: TrainerCanonicalReconcileStrategy,
    ) -> anyhow::Result<String> {
        let response = self.call::<_, ModelHandleResponse>(
            "reconcile_canonical_model",
            serde_json::json!({
                "local_model_id": local_model_id,
                "canonical_model_id": canonical_model_id,
                "strategy": strategy,
            }),
        )?;
        Ok(response.model_id)
    }

    pub(crate) fn release_model(&self, model_id: &str) {
        let _ = self
            .call::<_, AckResponse>("release_model", serde_json::json!({ "model_id": model_id }));
    }

    fn call<P, R>(&self, method: &str, params: P) -> anyhow::Result<R>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let mut transport = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("python worker mutex poisoned"))?;
        transport.call(method, params)
    }
}

#[derive(Debug)]
struct PythonWorkerTransport {
    child: Child,
    stream: TcpStream,
    next_request_id: u64,
}

impl PythonWorkerTransport {
    fn call<P, R>(&mut self, method: &str, params: P) -> anyhow::Result<R>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        let request = RpcRequest {
            id: request_id,
            method: method.to_owned(),
            params,
        };
        let bytes = serde_json::to_vec(&request).context("serialize python rpc request")?;
        write_frame(&mut self.stream, &bytes)
            .with_context(|| format!("send python rpc request {method}"))?;

        let response_bytes = match read_frame(&mut self.stream) {
            Ok(bytes) => bytes,
            Err(error) => {
                let status = self
                    .child
                    .try_wait()
                    .context("poll python worker status after transport failure")?;
                return Err(error).with_context(|| {
                    format!(
                        "read python rpc response {method} (worker status: {:?})",
                        status
                    )
                });
            }
        };
        if response_bytes.is_empty() {
            let status = self
                .child
                .try_wait()
                .context("poll python worker status after empty response")?;
            bail!(
                "python worker returned empty response for {method} (status: {:?})",
                status
            );
        }
        let response: RpcResponse =
            serde_json::from_slice(&response_bytes).context("decode python rpc response")?;
        if response.id != request_id {
            bail!(
                "python rpc response id mismatch for {method}: expected {}, got {}",
                request_id,
                response.id
            );
        }
        if !response.ok {
            bail!(
                "python rpc {method} failed: {}",
                response
                    .error
                    .unwrap_or_else(|| "unknown python worker error".to_owned())
            );
        }
        let result = response.result.unwrap_or(Value::Null);
        match serde_json::from_value(result.clone()) {
            Ok(value) => Ok(value),
            Err(error) => bail!("decode result for python rpc {method}: {error}; payload={result}"),
        }
    }
}

impl Drop for PythonWorkerTransport {
    fn drop(&mut self) {
        let _ = self.call::<_, AckResponse>("shutdown", Value::Null);
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn accept_worker_connection(
    listener: &TcpListener,
    child: &mut Child,
    timeout: Duration,
) -> anyhow::Result<TcpStream> {
    listener
        .set_nonblocking(true)
        .context("configure python worker listener as nonblocking")?;
    let deadline = Instant::now() + timeout;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                return configure_python_worker_stream(stream);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if let Some(status) = child
                    .try_wait()
                    .context("poll python worker while waiting for connection")?
                {
                    bail!("python worker exited before establishing rpc connection: {status}");
                }
                if Instant::now() >= deadline {
                    bail!("timed out waiting for python worker rpc connection");
                }
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => return Err(error).context("accept python worker rpc connection"),
        }
    }
}

fn configure_python_worker_stream(stream: TcpStream) -> anyhow::Result<TcpStream> {
    stream
        .set_nonblocking(false)
        .context("configure python worker tcp stream as blocking")?;
    stream
        .set_nodelay(true)
        .context("configure python worker tcp nodelay")?;
    Ok(stream)
}

fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> anyhow::Result<()> {
    let length = u64::try_from(payload.len()).context("frame payload exceeds u64")?;
    stream
        .write_all(&length.to_be_bytes())
        .and_then(|_| stream.write_all(payload))
        .and_then(|_| stream.flush())
        .context("write framed python rpc message")
}

fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let mut length_bytes = [0_u8; 8];
    stream
        .read_exact(&mut length_bytes)
        .context("read python rpc frame length")?;
    let length = u64::from_be_bytes(length_bytes);
    let mut payload = vec![0_u8; usize::try_from(length).context("python rpc frame too large")?];
    stream
        .read_exact(&mut payload)
        .context("read python rpc frame payload")?;
    Ok(payload)
}

fn join_pythonpath(entries: &[PathBuf]) -> anyhow::Result<Option<OsString>> {
    join_pythonpath_for_command(entries)
}

pub(crate) fn join_pythonpath_for_command(entries: &[PathBuf]) -> anyhow::Result<Option<OsString>> {
    let mut all_entries = entries.to_vec();
    if let Some(existing) = std::env::var_os("PYTHONPATH") {
        all_entries.extend(std::env::split_paths(&existing));
    }
    if all_entries.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        std::env::join_paths(all_entries).context("build python path for python worker")?,
    ))
}

#[derive(Clone, Debug, Serialize)]
struct RpcRequest<P> {
    id: u64,
    method: String,
    params: P,
}

#[derive(Clone, Debug, Deserialize)]
struct RpcResponse {
    id: u64,
    ok: bool,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct HelloResponse {
    pub protocol_version: u32,
    pub workload_name: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CapabilityProbeResponse {
    pub runtime_device: String,
    pub capability: CapabilityEstimate,
}

#[derive(Clone, Debug, Deserialize)]
struct ModelHandleResponse {
    model_id: String,
}

#[derive(Clone, Debug, Deserialize)]
struct OptionalModelHandleResponse {
    model_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct MetricsResponse {
    metrics: BTreeMap<String, MetricValue>,
}

#[derive(Clone, Debug, Deserialize)]
struct AckResponse {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PythonParameterPackPlanResponse {
    #[serde(default)]
    pub uses_custom_parameter_pack_hooks: bool,
    #[serde(default)]
    pub layout_hash: Option<String>,
    #[serde(default)]
    pub included_keys: Vec<String>,
    #[serde(default)]
    pub excluded_float_keys: Vec<String>,
    #[serde(default)]
    pub ignored_non_float_keys: Vec<String>,
    #[serde(default)]
    pub parameter_count: usize,
    #[serde(default)]
    pub generic_plan_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct PythonDiLoCoInnerLoopResponse {
    pub steps_completed: u32,
    #[serde(default)]
    pub metrics: BTreeMap<String, MetricValue>,
    #[serde(default)]
    pub inner_optimizer_state_path: Option<PathBuf>,
    #[serde(default)]
    pub inner_optimizer_state_encoding: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PythonDiLoCoInnerLoopPathRequest<'a> {
    pub model_id: &'a str,
    pub batches: &'a [PythonBatchRef],
    pub num_inner_steps: u32,
    pub inner_optimizer_state_path: Option<&'a Path>,
    pub output_parameter_pack_path: &'a Path,
    pub model_schema_hash: &'a burn_p2p_core::ContentId,
    pub require_exact_steps: bool,
    pub state_dict_filter: &'a PythonStateDictFilterConfig,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PythonMergeCandidateRef<'a> {
    pub peer_id: &'a str,
    pub head_id: &'a str,
    pub artifact_id: &'a str,
    pub model_id: &'a str,
    pub sample_weight: f64,
    pub quality_weight: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configure_python_worker_stream_restores_blocking_mode() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind listener");
        listener
            .set_nonblocking(true)
            .expect("set listener nonblocking");
        let listener_addr = listener.local_addr().expect("listener address");
        let writer = thread::spawn(move || {
            let mut stream = TcpStream::connect(listener_addr).expect("connect client stream");
            thread::sleep(Duration::from_millis(50));
            stream.write_all(b"ok").expect("write client payload");
        });

        let accepted = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept client stream: {error}"),
            }
        };

        accepted
            .set_nonblocking(true)
            .expect("force accepted stream nonblocking");
        let mut accepted =
            configure_python_worker_stream(accepted).expect("configure accepted stream");
        accepted
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set read timeout");

        let mut payload = [0_u8; 2];
        accepted
            .read_exact(&mut payload)
            .expect("blocking read should wait for payload");
        assert_eq!(&payload, b"ok");
        writer.join().expect("writer thread");
    }
}
