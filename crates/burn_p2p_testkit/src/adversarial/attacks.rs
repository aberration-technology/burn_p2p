use serde::{Deserialize, Serialize};

/// Attack-class labels used by the adversarial harness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttackClass {
    /// Accidental or malformed corruption.
    BaselineCorruption,
    /// Update-direction or norm manipulation.
    GradientAttack,
    /// Cohort or collusion attack.
    CohortAttack,
    /// Backdoor or trigger-specific attack.
    BackdoorAttack,
    /// Timing or replay attack.
    TimingAttack,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Supported adversarial attack patterns in the synthetic harness.
pub enum AdversarialAttack {
    /// Inject NaN or Inf values into the update sketch.
    NanInf,
    /// Simulate a malformed or wrong-shape update payload.
    WrongShape,
    /// Random sign flip against the trusted direction.
    SignFlip,
    /// Inflate the update norm while preserving direction.
    ScaleUp(f64),
    /// Shrink the update norm to simulate free-riding.
    ScaleDown(f64),
    /// Explicitly move opposite to the trusted direction.
    OppositeDirection,
    /// Small coordinated drift that stays close in norm.
    LittleEnough,
    /// Manipulate inner products while staying superficially plausible.
    InnerProduct,
    /// Attempt a large model-replacement style push.
    ModelReplacement,
    /// Reuse a stale but otherwise valid update.
    Replay,
    /// Submit an update outside the allowed lag window.
    Stale,
    /// Browser-oriented free-rider update.
    FreeRider,
    /// Coordinate a colluding cluster around one malicious centroid.
    ColludingCluster,
    /// Alternate attack direction across peers or windows.
    AlternatingDrift,
    /// Stay benign-looking before switching to a stronger attack.
    ReputationBuildStrike,
    /// Low-norm backdoor-style poisoning attempt.
    BackdoorLowNorm,
    /// Delay the update to flood a late window.
    LateFlood,
    /// Announce an update that cannot actually be delivered.
    UndeliverableArtifact,
}

impl AdversarialAttack {
    /// Returns the attack class.
    pub fn class(&self) -> AttackClass {
        match self {
            Self::NanInf | Self::WrongShape => AttackClass::BaselineCorruption,
            Self::Replay | Self::Stale | Self::LateFlood | Self::UndeliverableArtifact => {
                AttackClass::TimingAttack
            }
            Self::BackdoorLowNorm => AttackClass::BackdoorAttack,
            Self::ColludingCluster | Self::AlternatingDrift | Self::ReputationBuildStrike => {
                AttackClass::CohortAttack
            }
            Self::FreeRider => AttackClass::GradientAttack,
            Self::ModelReplacement => AttackClass::CohortAttack,
            Self::LittleEnough | Self::InnerProduct => AttackClass::GradientAttack,
            Self::SignFlip | Self::ScaleUp(_) | Self::ScaleDown(_) | Self::OppositeDirection => {
                AttackClass::GradientAttack
            }
        }
    }
}

/// Applies one attack transformation to an update vector.
pub fn apply_attack(
    reference: &[f64],
    honest_update: &[f64],
    attack: &AdversarialAttack,
) -> Vec<f64> {
    match attack {
        AdversarialAttack::NanInf => {
            let mut poisoned = honest_update.to_vec();
            if let Some(first) = poisoned.first_mut() {
                *first = f64::NAN;
            }
            if poisoned.len() > 1 {
                poisoned[1] = f64::INFINITY;
            }
            poisoned
        }
        AdversarialAttack::WrongShape => honest_update
            .iter()
            .take(honest_update.len() / 2)
            .copied()
            .collect(),
        AdversarialAttack::SignFlip | AdversarialAttack::OppositeDirection => {
            honest_update.iter().map(|value| -*value).collect()
        }
        AdversarialAttack::ScaleUp(scale) => {
            honest_update.iter().map(|value| value * scale).collect()
        }
        AdversarialAttack::ScaleDown(scale) => {
            honest_update.iter().map(|value| value * scale).collect()
        }
        AdversarialAttack::FreeRider => honest_update.iter().map(|value| value * 0.05).collect(),
        AdversarialAttack::LittleEnough => honest_update
            .iter()
            .zip(reference.iter().cycle())
            .enumerate()
            .map(|(index, (value, trusted))| {
                let drift = if index % 2 == 0 { 0.18 } else { -0.18 };
                value * 0.6 + trusted * 0.2 + drift
            })
            .collect(),
        AdversarialAttack::InnerProduct => honest_update
            .iter()
            .enumerate()
            .map(|(index, value)| if index % 2 == 0 { *value } else { -*value })
            .collect(),
        AdversarialAttack::ModelReplacement => reference.iter().map(|value| value * 6.0).collect(),
        AdversarialAttack::ColludingCluster => reference
            .iter()
            .enumerate()
            .map(|(index, value)| value * 1.4 + if index % 2 == 0 { 0.12 } else { -0.12 })
            .collect(),
        AdversarialAttack::AlternatingDrift => honest_update
            .iter()
            .enumerate()
            .map(|(index, value)| {
                if index % 2 == 0 {
                    value * 1.3
                } else {
                    -value * 0.7
                }
            })
            .collect(),
        AdversarialAttack::ReputationBuildStrike => reference
            .iter()
            .zip(honest_update.iter())
            .map(|(trusted, honest)| honest * 0.2 + trusted * 1.8)
            .collect(),
        AdversarialAttack::BackdoorLowNorm => honest_update
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let trigger = if index == 0 { 0.35 } else { 0.0 };
                value * 0.15 + trigger
            })
            .collect(),
        AdversarialAttack::Replay
        | AdversarialAttack::Stale
        | AdversarialAttack::LateFlood
        | AdversarialAttack::UndeliverableArtifact => honest_update.to_vec(),
    }
}
