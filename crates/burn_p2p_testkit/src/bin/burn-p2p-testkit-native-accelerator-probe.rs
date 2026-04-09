use std::{
    env, fs,
    hint::black_box,
    panic::{self, AssertUnwindSafe},
    path::PathBuf,
    time::Instant,
};

use anyhow::Context;
use burn::{
    backend::{Autodiff, Cuda, Wgpu, cuda, wgpu},
    tensor::{Distribution, ElementConversion, Tensor, backend::AutodiffBackend},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
struct NativeAcceleratorProbeArgs {
    output: PathBuf,
    backend: String,
    matrix_size: usize,
    warmup_iterations: u32,
    measured_iterations: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NativeAcceleratorProbeSummary {
    status: String,
    requested_backend: String,
    resolved_backend: Option<String>,
    matrix_size: usize,
    warmup_iterations: u32,
    measured_iterations: u32,
    elapsed_ms: u128,
    iterations_per_sec: Option<f64>,
    failure_reason: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let args = parse_args()?;
    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let summary = run_requested_probe(&args);
    fs::write(&args.output, serde_json::to_vec_pretty(&summary)?)
        .with_context(|| format!("failed to write {}", args.output.display()))?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

fn parse_args() -> anyhow::Result<NativeAcceleratorProbeArgs> {
    let mut output = None;
    let mut backend = String::from("auto");
    let mut matrix_size = 512usize;
    let mut warmup_iterations = 2u32;
    let mut measured_iterations = 4u32;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" => {
                output =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        anyhow::anyhow!("missing value for --output")
                    })?));
            }
            "--backend" => {
                backend = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing value for --backend"))?;
            }
            "--matrix-size" => {
                matrix_size = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing value for --matrix-size"))?
                    .parse()
                    .context("invalid --matrix-size")?;
            }
            "--warmup" => {
                warmup_iterations = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing value for --warmup"))?
                    .parse()
                    .context("invalid --warmup")?;
            }
            "--iterations" => {
                measured_iterations = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing value for --iterations"))?
                    .parse()
                    .context("invalid --iterations")?;
            }
            other => anyhow::bail!("unknown argument `{other}`"),
        }
    }

    Ok(NativeAcceleratorProbeArgs {
        output: output.ok_or_else(|| anyhow::anyhow!("missing required --output"))?,
        backend,
        matrix_size,
        warmup_iterations,
        measured_iterations,
    })
}

fn run_requested_probe(args: &NativeAcceleratorProbeArgs) -> NativeAcceleratorProbeSummary {
    match args.backend.as_str() {
        "cuda" => attempt_cuda_probe(args),
        "wgpu" => attempt_wgpu_probe(args),
        "auto" => {
            let attempts = [attempt_cuda_probe(args), attempt_wgpu_probe(args)];
            if let Some(summary) = attempts.iter().find(|summary| summary.status == "ok") {
                let mut resolved = summary.clone();
                resolved.requested_backend = "auto".into();
                return resolved;
            }

            let status = if attempts.iter().any(|summary| summary.status == "error") {
                "error"
            } else {
                "skipped"
            };
            NativeAcceleratorProbeSummary {
                status: status.into(),
                requested_backend: "auto".into(),
                resolved_backend: None,
                matrix_size: args.matrix_size,
                warmup_iterations: args.warmup_iterations,
                measured_iterations: args.measured_iterations,
                elapsed_ms: attempts.iter().map(|summary| summary.elapsed_ms).sum(),
                iterations_per_sec: None,
                failure_reason: Some(
                    attempts
                        .iter()
                        .map(|summary| {
                            format!(
                                "{}: {}",
                                summary.requested_backend,
                                summary
                                    .failure_reason
                                    .clone()
                                    .unwrap_or_else(|| summary.status.clone())
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" | "),
                ),
            }
        }
        other => NativeAcceleratorProbeSummary {
            status: "error".into(),
            requested_backend: other.into(),
            resolved_backend: None,
            matrix_size: args.matrix_size,
            warmup_iterations: args.warmup_iterations,
            measured_iterations: args.measured_iterations,
            elapsed_ms: 0,
            iterations_per_sec: None,
            failure_reason: Some(format!("unsupported backend `{other}`")),
        },
    }
}

fn attempt_cuda_probe(args: &NativeAcceleratorProbeArgs) -> NativeAcceleratorProbeSummary {
    run_attempt(args, "cuda", || {
        type ProbeBackend = Autodiff<Cuda>;
        let device = cuda::CudaDevice::default();
        run_backend_probe::<ProbeBackend>("cuda", &device, args)
    })
}

fn attempt_wgpu_probe(args: &NativeAcceleratorProbeArgs) -> NativeAcceleratorProbeSummary {
    run_attempt(args, "wgpu", || {
        type ProbeBackend = Autodiff<Wgpu>;
        let device = wgpu::WgpuDevice::default();
        wgpu::init_setup::<wgpu::graphics::AutoGraphicsApi>(&device, Default::default());
        run_backend_probe::<ProbeBackend>("wgpu", &device, args)
    })
}

fn run_attempt(
    args: &NativeAcceleratorProbeArgs,
    backend: &str,
    attempt: impl FnOnce() -> anyhow::Result<NativeAcceleratorProbeSummary>,
) -> NativeAcceleratorProbeSummary {
    match panic::catch_unwind(AssertUnwindSafe(attempt)) {
        Ok(Ok(summary)) => summary,
        Ok(Err(error)) => classify_probe_failure(args, backend, &error.to_string()),
        Err(payload) => classify_probe_failure(args, backend, &panic_payload_to_string(payload)),
    }
}

fn classify_probe_failure(
    args: &NativeAcceleratorProbeArgs,
    backend: &str,
    reason: &str,
) -> NativeAcceleratorProbeSummary {
    let status = if is_unavailable_backend_reason(reason) {
        "skipped"
    } else {
        "error"
    };
    NativeAcceleratorProbeSummary {
        status: status.into(),
        requested_backend: backend.into(),
        resolved_backend: None,
        matrix_size: args.matrix_size,
        warmup_iterations: args.warmup_iterations,
        measured_iterations: args.measured_iterations,
        elapsed_ms: 0,
        iterations_per_sec: None,
        failure_reason: Some(reason.into()),
    }
}

fn run_backend_probe<B>(
    backend: &str,
    device: &B::Device,
    args: &NativeAcceleratorProbeArgs,
) -> anyhow::Result<NativeAcceleratorProbeSummary>
where
    B: AutodiffBackend,
{
    let shape = [args.matrix_size, args.matrix_size];
    let x: Tensor<B, 2> = Tensor::random(shape, Distribution::Default, device);
    let y: Tensor<B, 2> = Tensor::random(shape, Distribution::Default, device).require_grad();

    for _ in 0..args.warmup_iterations {
        black_box(run_training_iteration::<B>(&x, &y)?);
    }

    let started_at = Instant::now();
    let mut last_scalar = 0.0_f32;
    for _ in 0..args.measured_iterations {
        last_scalar = run_training_iteration::<B>(&x, &y)?;
    }
    let elapsed = started_at.elapsed();
    black_box(last_scalar);

    Ok(NativeAcceleratorProbeSummary {
        status: "ok".into(),
        requested_backend: backend.into(),
        resolved_backend: Some(backend.into()),
        matrix_size: args.matrix_size,
        warmup_iterations: args.warmup_iterations,
        measured_iterations: args.measured_iterations,
        elapsed_ms: elapsed.as_millis(),
        iterations_per_sec: Some(args.measured_iterations as f64 / elapsed.as_secs_f64().max(1e-9)),
        failure_reason: None,
    })
}

fn run_training_iteration<B>(x: &Tensor<B, 2>, y: &Tensor<B, 2>) -> anyhow::Result<f32>
where
    B: AutodiffBackend,
{
    let loss = x.clone().matmul(y.clone()).exp().sum();
    let grads = loss.backward();
    let y_grad = y
        .grad(&grads)
        .ok_or_else(|| anyhow::anyhow!("autodiff probe did not produce a gradient"))?;
    Ok(y_grad.sum().into_scalar().elem())
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).into(),
            Err(_) => "accelerator probe panicked without a string payload".into(),
        },
    }
}

fn is_unavailable_backend_reason(reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    [
        "no adapter",
        "no suitable adapter",
        "adapter not found",
        "backend is not available",
        "backend unavailable",
        "cuda driver",
        "driver version is insufficient",
        "failed to initialize cuda",
        "could not initialize cuda",
        "libcuda",
        "vulkan",
        "metal",
        "dx12",
        "webgpu",
        "not supported on this system",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}
