//! Capability and budget evaluation helpers for runtime placement decisions.
#![allow(missing_docs)]
use burn_p2p_core::{AttestationLevel, ClientPlatform, PeerId, PersistenceClass};
use burn_p2p_limits::{
    CapabilityCalibrator, CapabilityProbe, LimitPolicy, LocalBackend, ObservedThroughputUpdate,
    backend_preference_order,
};
use chrono::Utc;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

fn native_gpu_probe() -> CapabilityProbe {
    CapabilityProbe {
        peer_id: PeerId::new("bench-peer-gpu"),
        platform: ClientPlatform::Native,
        available_backends: vec![
            LocalBackend::Ndarray,
            LocalBackend::Cuda,
            LocalBackend::Wgpu,
        ],
        device_memory_bytes: Some(24 * 1024 * 1024 * 1024),
        system_memory_bytes: 64 * 1024 * 1024 * 1024,
        disk_bytes: 500 * 1024 * 1024 * 1024,
        upload_mbps: 100.0,
        download_mbps: 250.0,
        persistence: PersistenceClass::Durable,
        attestation_level: AttestationLevel::Strong,
        work_units_per_second: 4_200.0,
        benchmark_hash: None,
    }
}

fn browser_probe() -> CapabilityProbe {
    CapabilityProbe {
        peer_id: PeerId::new("bench-peer-browser"),
        platform: ClientPlatform::Browser,
        available_backends: vec![LocalBackend::Wgpu],
        device_memory_bytes: Some(2 * 1024 * 1024 * 1024),
        system_memory_bytes: 8 * 1024 * 1024 * 1024,
        disk_bytes: 8 * 1024 * 1024 * 1024,
        upload_mbps: 10.0,
        download_mbps: 50.0,
        persistence: PersistenceClass::Ephemeral,
        attestation_level: AttestationLevel::None,
        work_units_per_second: 120.0,
        benchmark_hash: None,
    }
}

fn cpu_probe() -> CapabilityProbe {
    CapabilityProbe {
        peer_id: PeerId::new("bench-peer-cpu"),
        platform: ClientPlatform::Native,
        available_backends: vec![LocalBackend::Cpu, LocalBackend::Ndarray],
        device_memory_bytes: None,
        system_memory_bytes: 16 * 1024 * 1024 * 1024,
        disk_bytes: 200 * 1024 * 1024 * 1024,
        upload_mbps: 40.0,
        download_mbps: 120.0,
        persistence: PersistenceClass::Session,
        attestation_level: AttestationLevel::Manifest,
        work_units_per_second: 850.0,
        benchmark_hash: None,
    }
}

fn bench_backend_ordering(c: &mut Criterion) {
    let mut group = c.benchmark_group("limits_backend_ordering");

    for (label, backends) in [
        (
            "gpu",
            vec![
                LocalBackend::Ndarray,
                LocalBackend::Cuda,
                LocalBackend::Wgpu,
            ],
        ),
        (
            "mixed",
            vec![
                LocalBackend::Custom("metal".into()),
                LocalBackend::Cpu,
                LocalBackend::Wgpu,
                LocalBackend::Ndarray,
            ],
        ),
    ] {
        group.throughput(Throughput::Elements(backends.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &backends,
            |b, backends| {
                b.iter(|| backend_preference_order(black_box(backends.iter())));
            },
        );
    }

    group.finish();
}

fn bench_initial_calibration(c: &mut Criterion) {
    let mut group = c.benchmark_group("limits_initial_calibration");
    let calibrator = CapabilityCalibrator::default();

    for (label, probe) in [
        ("gpu", native_gpu_probe()),
        ("browser", browser_probe()),
        ("cpu", cpu_probe()),
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(label), &probe, |b, probe| {
            b.iter(|| {
                calibrator
                    .calibrate(black_box(probe.clone()), black_box(Utc::now()))
                    .expect("calibrated profile");
            });
        });
    }

    group.finish();
}

fn bench_observed_rebudgeting(c: &mut Criterion) {
    let mut group = c.benchmark_group("limits_observed_rebudgeting");
    let calibrator = CapabilityCalibrator::new(LimitPolicy {
        observed_throughput_smoothing: 0.5,
        ..LimitPolicy::default()
    })
    .expect("calibrator");
    let profiles = vec![
        (
            "gpu_slowdown",
            calibrator
                .calibrate(native_gpu_probe(), Utc::now())
                .expect("gpu profile"),
            ObservedThroughputUpdate {
                measured_work_units: 180_000,
                elapsed_seconds: 100,
                completed_windows: 1,
                sampled_at: Utc::now(),
            },
        ),
        (
            "browser_rebound",
            calibrator
                .calibrate(browser_probe(), Utc::now())
                .expect("browser profile"),
            ObservedThroughputUpdate {
                measured_work_units: 9_000,
                elapsed_seconds: 45,
                completed_windows: 2,
                sampled_at: Utc::now(),
            },
        ),
        (
            "cpu_stable",
            calibrator
                .calibrate(cpu_probe(), Utc::now())
                .expect("cpu profile"),
            ObservedThroughputUpdate {
                measured_work_units: 51_000,
                elapsed_seconds: 60,
                completed_windows: 1,
                sampled_at: Utc::now(),
            },
        ),
    ];

    for (label, profile, update) in profiles {
        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &(profile, update),
            |b, input| {
                b.iter(|| {
                    calibrator
                        .rebudget(black_box(&input.0), black_box(input.1.clone()))
                        .expect("rebudgeted profile");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_backend_ordering,
    bench_initial_calibration,
    bench_observed_rebudgeting
);
criterion_main!(benches);
