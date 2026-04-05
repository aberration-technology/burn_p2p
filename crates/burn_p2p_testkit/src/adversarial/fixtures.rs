use burn_p2p_core::PeerId;

use super::{AdversarialAttack, apply_attack};

#[derive(Clone, Debug, PartialEq)]
/// One synthetic peer in an adversarial fixture.
pub struct FixturePeer {
    /// Peer identifier.
    pub peer_id: PeerId,
    /// Dense update vector.
    pub update: Vec<f64>,
    /// Whether the peer is malicious in the scenario oracle.
    pub malicious: bool,
    /// Attack applied to the peer, when malicious.
    pub attack: Option<AdversarialAttack>,
}

#[derive(Clone, Debug, PartialEq)]
/// Cohort fixture used by adversarial scenarios.
pub struct AdversarialFixture {
    /// Trusted or canary reference update direction.
    pub reference_update: Vec<f64>,
    /// Cohort peers.
    pub peers: Vec<FixturePeer>,
}

/// Builds a deterministic adversarial cohort fixture.
pub fn build_fixture(
    attack: AdversarialAttack,
    total_peers: usize,
    malicious_fraction: f64,
) -> AdversarialFixture {
    let total_peers = total_peers.max(3);
    let malicious_peers = ((total_peers as f64) * malicious_fraction)
        .round()
        .clamp(0.0, total_peers as f64) as usize;
    let reference_update = vec![0.9, 0.8, -0.7, 0.5, -0.3, 0.2, 0.1, -0.05];

    let peers = (0..total_peers)
        .map(|index| {
            let honest_update = reference_update
                .iter()
                .enumerate()
                .map(|(dimension, value)| {
                    let jitter = ((index + dimension) % 3) as f64 * 0.02 - 0.02;
                    value + jitter
                })
                .collect::<Vec<_>>();
            let malicious = index < malicious_peers;
            let update = if malicious {
                apply_attack(&reference_update, &honest_update, &attack)
            } else {
                honest_update
            };
            FixturePeer {
                peer_id: PeerId::new(format!("peer-{index}")),
                update,
                malicious,
                attack: malicious.then(|| attack.clone()),
            }
        })
        .collect();

    AdversarialFixture {
        reference_update,
        peers,
    }
}
