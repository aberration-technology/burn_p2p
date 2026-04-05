//! Adversarial fixtures and scenario runners for robustness testing.

mod attacks;
mod fixtures;
mod oracle;
mod report;
mod scenarios;

pub use attacks::{AdversarialAttack, AttackClass, apply_attack};
pub use fixtures::{AdversarialFixture, FixturePeer, build_fixture};
pub use oracle::{aggregate_cosine_to_reference, attack_success, quarantine_precision_recall};
pub use report::{AdversarialDecisionRecord, AdversarialScenarioReport};
pub use scenarios::{run_attack_matrix, run_scenario};
