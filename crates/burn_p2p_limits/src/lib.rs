#![forbid(unsafe_code)]

use std::collections::BTreeSet;

use burn_p2p_core::{
    AttestationLevel, CapabilityCard, CapabilityCardId, CapabilityClass, CapabilityEstimate,
    ClientPlatform, ContentId, PeerId, PeerRole, PeerRoleSet, PersistenceClass,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum LimitsError {
    #[error("schema error: {0}")]
    Schema(#[from] burn_p2p_core::SchemaError),
    #[error("target window seconds must be greater than zero")]
    InvalidTargetWindow,
    #[error("observed throughput window seconds must be greater than zero")]
    InvalidObservedWindow,
    #[error("work units per second must be non-negative")]
    InvalidWorkRate,
    #[error("observed throughput smoothing must be within 0.0..=1.0")]
    InvalidObservedSmoothing,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LocalBackend {
    Cuda,
    Wgpu,
    Ndarray,
    Cpu,
    Custom(String),
}

impl LocalBackend {
    pub fn as_name(&self) -> &str {
        match self {
            Self::Cuda => "cuda",
            Self::Wgpu => "wgpu",
            Self::Ndarray => "ndarray",
            Self::Cpu => "cpu",
            Self::Custom(name) => name.as_str(),
        }
    }

    fn priority(&self) -> u8 {
        match self {
            Self::Cuda => 0,
            Self::Wgpu => 1,
            Self::Ndarray => 2,
            Self::Cpu => 3,
            Self::Custom(_) => 4,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityProbe {
    pub peer_id: PeerId,
    pub platform: ClientPlatform,
    pub available_backends: Vec<LocalBackend>,
    pub device_memory_bytes: Option<u64>,
    pub system_memory_bytes: u64,
    pub disk_bytes: u64,
    pub upload_mbps: f32,
    pub download_mbps: f32,
    pub persistence: PersistenceClass,
    pub attestation_level: AttestationLevel,
    pub work_units_per_second: f64,
    pub benchmark_hash: Option<ContentId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LimitPolicy {
    pub target_window_seconds: u64,
    pub browser_target_window_seconds: u64,
    pub archive_min_disk_bytes: u64,
    pub relay_helper_min_upload_mbps: f32,
    pub reducer_min_work_units_per_second: f64,
    pub validator_min_attestation: AttestationLevel,
    pub observed_throughput_smoothing: f64,
}

impl Default for LimitPolicy {
    fn default() -> Self {
        Self {
            target_window_seconds: 300,
            browser_target_window_seconds: 45,
            archive_min_disk_bytes: 100 * 1024 * 1024 * 1024,
            relay_helper_min_upload_mbps: 25.0,
            reducer_min_work_units_per_second: 2_500.0,
            validator_min_attestation: AttestationLevel::Challenge,
            observed_throughput_smoothing: 0.25,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkBudget {
    pub target_window_seconds: u64,
    pub budget_work_units: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObservedThroughputUpdate {
    pub measured_work_units: u64,
    pub elapsed_seconds: u64,
    pub completed_windows: u32,
    pub sampled_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LimitProfile {
    pub card: CapabilityCard,
    pub estimate: CapabilityEstimate,
    pub recommended_roles: PeerRoleSet,
    pub recommended_budget: WorkBudget,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CapabilityCalibrator {
    pub policy: LimitPolicy,
}

impl CapabilityCalibrator {
    pub fn new(policy: LimitPolicy) -> Result<Self, LimitsError> {
        if policy.target_window_seconds == 0 || policy.browser_target_window_seconds == 0 {
            return Err(LimitsError::InvalidTargetWindow);
        }
        if !(0.0..=1.0).contains(&policy.observed_throughput_smoothing) {
            return Err(LimitsError::InvalidObservedSmoothing);
        }

        Ok(Self { policy })
    }

    pub fn calibrate(
        &self,
        probe: CapabilityProbe,
        reported_at: DateTime<Utc>,
    ) -> Result<LimitProfile, LimitsError> {
        if probe.work_units_per_second < 0.0 {
            return Err(LimitsError::InvalidWorkRate);
        }

        let preferred_backends = backend_preference_order(probe.available_backends.iter());
        let recommended_classes = recommended_capability_classes(&probe, &self.policy);
        let recommended_roles = recommended_roles(&probe, &recommended_classes, &self.policy);
        let recommended_budget = recommended_budget(&probe, &self.policy);

        let card_id = CapabilityCardId::derive(&(
            probe.peer_id.as_str(),
            &probe.platform,
            &preferred_backends,
            probe.device_memory_bytes,
            probe.system_memory_bytes,
            probe.disk_bytes,
            probe.upload_mbps.to_bits(),
            probe.download_mbps.to_bits(),
            &probe.persistence,
            probe.work_units_per_second.to_bits(),
            &probe.attestation_level,
            probe.benchmark_hash.as_ref().map(ContentId::as_str),
        ))?;

        let estimate = CapabilityEstimate {
            preferred_backends: preferred_backends.clone(),
            work_units_per_second: probe.work_units_per_second,
            target_window_seconds: recommended_budget.target_window_seconds,
        };

        let card = CapabilityCard {
            card_id,
            peer_id: probe.peer_id,
            platform: probe.platform,
            roles: recommended_roles.clone(),
            preferred_backends,
            recommended_classes,
            device_memory_bytes: probe.device_memory_bytes,
            system_memory_bytes: probe.system_memory_bytes,
            disk_bytes: probe.disk_bytes,
            upload_mbps: probe.upload_mbps,
            download_mbps: probe.download_mbps,
            persistence: probe.persistence,
            work_units_per_second: probe.work_units_per_second,
            attestation_level: probe.attestation_level,
            benchmark_hash: probe.benchmark_hash,
            reported_at,
        };

        Ok(LimitProfile {
            card,
            estimate,
            recommended_roles,
            recommended_budget,
        })
    }

    pub fn rebudget(
        &self,
        profile: &LimitProfile,
        observed: ObservedThroughputUpdate,
    ) -> Result<LimitProfile, LimitsError> {
        if observed.elapsed_seconds == 0 {
            return Err(LimitsError::InvalidObservedWindow);
        }

        let effective_work_units_per_second = corrected_work_units_per_second(
            profile.estimate.work_units_per_second,
            &observed,
            self.policy.observed_throughput_smoothing,
        )?;

        let probe = probe_from_card(&profile.card, effective_work_units_per_second);
        self.calibrate(probe, observed.sampled_at)
    }

    pub fn recalibrate_profile(
        &self,
        profile: &LimitProfile,
        reported_at: DateTime<Utc>,
    ) -> Result<LimitProfile, LimitsError> {
        let probe = probe_from_card(&profile.card, profile.card.work_units_per_second);
        self.calibrate(probe, reported_at)
    }
}

pub fn probe_from_profile(profile: &LimitProfile) -> CapabilityProbe {
    probe_from_card(&profile.card, profile.card.work_units_per_second)
}

fn probe_from_card(card: &CapabilityCard, work_units_per_second: f64) -> CapabilityProbe {
    CapabilityProbe {
        peer_id: card.peer_id.clone(),
        platform: card.platform.clone(),
        available_backends: card
            .preferred_backends
            .iter()
            .map(|backend| backend_from_name(backend))
            .collect(),
        device_memory_bytes: card.device_memory_bytes,
        system_memory_bytes: card.system_memory_bytes,
        disk_bytes: card.disk_bytes,
        upload_mbps: card.upload_mbps,
        download_mbps: card.download_mbps,
        persistence: card.persistence.clone(),
        attestation_level: card.attestation_level.clone(),
        work_units_per_second,
        benchmark_hash: card.benchmark_hash.clone(),
    }
}

fn backend_from_name(name: &str) -> LocalBackend {
    match name {
        "cuda" => LocalBackend::Cuda,
        "wgpu" => LocalBackend::Wgpu,
        "ndarray" => LocalBackend::Ndarray,
        "cpu" => LocalBackend::Cpu,
        other => LocalBackend::Custom(other.to_owned()),
    }
}

pub fn corrected_work_units_per_second(
    baseline_work_units_per_second: f64,
    observed: &ObservedThroughputUpdate,
    smoothing: f64,
) -> Result<f64, LimitsError> {
    if baseline_work_units_per_second < 0.0 {
        return Err(LimitsError::InvalidWorkRate);
    }
    if observed.elapsed_seconds == 0 {
        return Err(LimitsError::InvalidObservedWindow);
    }
    if !(0.0..=1.0).contains(&smoothing) {
        return Err(LimitsError::InvalidObservedSmoothing);
    }

    let observed_rate = observed.measured_work_units as f64 / observed.elapsed_seconds as f64;
    Ok(((1.0 - smoothing) * baseline_work_units_per_second) + (smoothing * observed_rate))
}

pub fn backend_preference_order<'a>(
    backends: impl IntoIterator<Item = &'a LocalBackend>,
) -> Vec<String> {
    let mut ordered = backends
        .into_iter()
        .map(LocalBackend::clone)
        .collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.priority()
            .cmp(&right.priority())
            .then(left.as_name().cmp(right.as_name()))
    });
    ordered.dedup_by(|left, right| left.as_name() == right.as_name());
    ordered
        .into_iter()
        .map(|backend| backend.as_name().to_owned())
        .collect()
}

pub fn recommended_capability_classes(
    probe: &CapabilityProbe,
    policy: &LimitPolicy,
) -> BTreeSet<CapabilityClass> {
    let mut classes = BTreeSet::new();
    let has_cuda = probe
        .available_backends
        .iter()
        .any(|backend| matches!(backend, LocalBackend::Cuda));
    let has_wgpu = probe
        .available_backends
        .iter()
        .any(|backend| matches!(backend, LocalBackend::Wgpu));
    let has_compute_backend = has_cuda || has_wgpu;

    if probe.platform == ClientPlatform::Browser {
        classes.insert(CapabilityClass::BrowserOpportunistic);
    } else if has_cuda || (has_wgpu && probe.device_memory_bytes.unwrap_or_default() > 0) {
        classes.insert(CapabilityClass::TrainerGpu);
    } else {
        classes.insert(CapabilityClass::TrainerCpu);
        classes.insert(CapabilityClass::Evaluator);
    }

    if probe.platform == ClientPlatform::Native && !has_compute_backend {
        classes.insert(CapabilityClass::Evaluator);
    }

    if probe.disk_bytes >= policy.archive_min_disk_bytes
        && probe.persistence == PersistenceClass::Durable
    {
        classes.insert(CapabilityClass::Archive);
    }

    if probe.upload_mbps >= policy.relay_helper_min_upload_mbps
        && probe.platform == ClientPlatform::Native
    {
        classes.insert(CapabilityClass::RelayHelper);
    }

    classes
}

pub fn recommended_roles(
    probe: &CapabilityProbe,
    classes: &BTreeSet<CapabilityClass>,
    policy: &LimitPolicy,
) -> PeerRoleSet {
    let mut roles = BTreeSet::new();

    if classes.contains(&CapabilityClass::TrainerGpu) {
        roles.insert(PeerRole::TrainerGpu);
    }
    if classes.contains(&CapabilityClass::TrainerCpu) {
        roles.insert(PeerRole::TrainerCpu);
    }
    if classes.contains(&CapabilityClass::Evaluator) {
        roles.insert(PeerRole::Evaluator);
    }
    if classes.contains(&CapabilityClass::Archive) {
        roles.insert(PeerRole::Archive);
    }
    if classes.contains(&CapabilityClass::RelayHelper) {
        roles.insert(PeerRole::RelayHelper);
    }
    if classes.contains(&CapabilityClass::BrowserOpportunistic) {
        roles.insert(PeerRole::BrowserTrainer);
        roles.insert(PeerRole::Evaluator);
    }

    if probe.platform == ClientPlatform::Native
        && probe.persistence == PersistenceClass::Durable
        && probe.attestation_level >= policy.validator_min_attestation
    {
        roles.insert(PeerRole::Validator);
    }

    if probe.platform == ClientPlatform::Native
        && probe.work_units_per_second >= policy.reducer_min_work_units_per_second
        && roles.contains(&PeerRole::TrainerGpu)
    {
        roles.insert(PeerRole::Reducer);
    }

    PeerRoleSet { roles }
}

pub fn recommended_budget(probe: &CapabilityProbe, policy: &LimitPolicy) -> WorkBudget {
    let target_window_seconds = match probe.platform {
        ClientPlatform::Browser => policy
            .browser_target_window_seconds
            .min(policy.target_window_seconds),
        ClientPlatform::Native => policy.target_window_seconds,
    };

    let adjusted_window = match probe.persistence {
        PersistenceClass::Ephemeral => target_window_seconds.min(120),
        PersistenceClass::Session => target_window_seconds,
        PersistenceClass::Durable => target_window_seconds,
    };

    let budget_work_units = (probe.work_units_per_second * adjusted_window as f64)
        .floor()
        .max(1.0) as u64;

    WorkBudget {
        target_window_seconds: adjusted_window,
        budget_work_units,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::{
        CapabilityCalibrator, CapabilityProbe, LimitPolicy, LocalBackend, ObservedThroughputUpdate,
        backend_preference_order,
    };

    fn native_gpu_probe() -> CapabilityProbe {
        CapabilityProbe {
            peer_id: burn_p2p_core::PeerId::new("peer-gpu"),
            platform: burn_p2p_core::ClientPlatform::Native,
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
            persistence: burn_p2p_core::PersistenceClass::Durable,
            attestation_level: burn_p2p_core::AttestationLevel::Strong,
            work_units_per_second: 4_200.0,
            benchmark_hash: None,
        }
    }

    #[test]
    fn backend_order_prefers_cuda_then_wgpu_then_ndarray() {
        let ordered = backend_preference_order(
            [
                LocalBackend::Ndarray,
                LocalBackend::Custom("metal".into()),
                LocalBackend::Cuda,
                LocalBackend::Wgpu,
            ]
            .iter(),
        );

        assert_eq!(ordered, vec!["cuda", "wgpu", "ndarray", "metal"]);
    }

    #[test]
    fn calibrator_promotes_native_gpu_node_into_training_roles() {
        let profile = CapabilityCalibrator::default()
            .calibrate(native_gpu_probe(), Utc::now())
            .expect("profile");

        assert!(
            profile
                .recommended_roles
                .contains(&burn_p2p_core::PeerRole::TrainerGpu)
        );
        assert!(
            profile
                .recommended_roles
                .contains(&burn_p2p_core::PeerRole::Archive)
        );
        assert!(
            profile
                .recommended_roles
                .contains(&burn_p2p_core::PeerRole::RelayHelper)
        );
        assert!(
            profile
                .recommended_roles
                .contains(&burn_p2p_core::PeerRole::Validator)
        );
        assert_eq!(profile.estimate.preferred_backends[0], "cuda");
        assert!(profile.recommended_budget.budget_work_units > 0);
    }

    #[test]
    fn browser_probe_prefers_short_opportunistic_windows() {
        let probe = CapabilityProbe {
            peer_id: burn_p2p_core::PeerId::new("peer-browser"),
            platform: burn_p2p_core::ClientPlatform::Browser,
            available_backends: vec![LocalBackend::Wgpu],
            device_memory_bytes: Some(2 * 1024 * 1024 * 1024),
            system_memory_bytes: 8 * 1024 * 1024 * 1024,
            disk_bytes: 8 * 1024 * 1024 * 1024,
            upload_mbps: 10.0,
            download_mbps: 50.0,
            persistence: burn_p2p_core::PersistenceClass::Ephemeral,
            attestation_level: burn_p2p_core::AttestationLevel::None,
            work_units_per_second: 120.0,
            benchmark_hash: None,
        };

        let profile = CapabilityCalibrator::new(LimitPolicy::default())
            .expect("calibrator")
            .calibrate(probe, Utc::now())
            .expect("profile");

        assert!(
            profile
                .recommended_roles
                .contains(&burn_p2p_core::PeerRole::BrowserTrainer)
        );
        assert!(
            profile
                .recommended_roles
                .contains(&burn_p2p_core::PeerRole::Evaluator)
        );
        assert_eq!(profile.recommended_budget.target_window_seconds, 45);
    }

    #[test]
    fn card_identity_is_deterministic_for_the_same_probe() {
        let calibrator = CapabilityCalibrator::default();
        let probe = native_gpu_probe();

        let first = calibrator
            .calibrate(probe.clone(), Utc::now())
            .expect("first")
            .card
            .card_id;
        let second = calibrator
            .calibrate(probe, Utc::now())
            .expect("second")
            .card
            .card_id;

        assert_eq!(first, second);
    }

    #[test]
    fn observed_throughput_rebudgets_native_profile_from_runtime_speed() {
        let calibrator = CapabilityCalibrator::default();
        let profile = calibrator
            .calibrate(native_gpu_probe(), Utc::now())
            .expect("profile");

        let rebudgeted = calibrator
            .rebudget(
                &profile,
                ObservedThroughputUpdate {
                    measured_work_units: 180_000,
                    elapsed_seconds: 100,
                    completed_windows: 1,
                    sampled_at: Utc::now(),
                },
            )
            .expect("rebudgeted");

        assert!(rebudgeted.estimate.work_units_per_second < profile.estimate.work_units_per_second);
        assert!(
            rebudgeted.recommended_budget.budget_work_units
                < profile.recommended_budget.budget_work_units
        );
        assert_ne!(rebudgeted.card.card_id, profile.card.card_id);
    }

    #[test]
    fn observed_throughput_can_demote_reducer_role_when_runtime_is_slower() {
        let calibrator = CapabilityCalibrator::new(LimitPolicy {
            observed_throughput_smoothing: 1.0,
            ..LimitPolicy::default()
        })
        .expect("calibrator");
        let profile = calibrator
            .calibrate(native_gpu_probe(), Utc::now())
            .expect("profile");

        let rebudgeted = calibrator
            .rebudget(
                &profile,
                ObservedThroughputUpdate {
                    measured_work_units: 30_000,
                    elapsed_seconds: 60,
                    completed_windows: 1,
                    sampled_at: Utc::now(),
                },
            )
            .expect("rebudgeted");

        assert!(
            !rebudgeted
                .recommended_roles
                .contains(&burn_p2p_core::PeerRole::Reducer)
        );
        assert!(
            rebudgeted
                .recommended_roles
                .contains(&burn_p2p_core::PeerRole::TrainerGpu)
        );
    }
}
