//! Receipt-driven leaderboard, profile, and badge services for burn_p2p.
#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_core::{
    BadgeAward, BadgeKind, ContributionReceipt, ContributionRollup, IdentityVisibility,
    LeaderboardEntry, LeaderboardIdentity, LeaderboardSnapshot, MergeCertificate, MetricValue,
    NetworkId, PeerId, PrincipalId, SocialProfile,
};
use chrono::{DateTime, NaiveDate, Utc};

/// The leaderboard score version v1 constant.
pub const LEADERBOARD_SCORE_VERSION_V1: &str = "leaderboard_score_v1";

#[derive(Clone, Debug)]
/// Represents a leaderboard computation input.
pub struct LeaderboardComputationInput<'a> {
    /// The network ID.
    pub network_id: &'a NetworkId,
    /// The receipts.
    pub receipts: &'a [ContributionReceipt],
    /// The merge certificates.
    pub merge_certificates: &'a [MergeCertificate],
    /// The peer principals.
    pub peer_principals: &'a BTreeMap<PeerId, PrincipalId>,
    /// The captured at.
    pub captured_at: DateTime<Utc>,
}

/// Defines behavior for profile service.
pub trait ProfileService {
    /// Performs the social profile operation.
    fn social_profile(&self, principal_id: &PrincipalId) -> Option<SocialProfile>;
}

#[derive(Clone, Debug, Default)]
/// Provides no profile services.
pub struct NoProfileService;

impl ProfileService for NoProfileService {
    /// Performs the social profile operation.
    fn social_profile(&self, _principal_id: &PrincipalId) -> Option<SocialProfile> {
        None
    }
}

#[derive(Clone, Debug, Default)]
/// Provides static profile services.
pub struct StaticProfileService {
    profiles: BTreeMap<PrincipalId, SocialProfile>,
}

impl StaticProfileService {
    /// Creates a new value.
    pub fn new(profiles: BTreeMap<PrincipalId, SocialProfile>) -> Self {
        Self { profiles }
    }
}

impl ProfileService for StaticProfileService {
    fn social_profile(&self, principal_id: &PrincipalId) -> Option<SocialProfile> {
        self.profiles.get(principal_id).cloned()
    }
}

/// Defines behavior for badge service.
pub trait BadgeService {
    /// Performs the badges for row operation.
    fn badges_for_row(
        &self,
        first_receipt_at: Option<DateTime<Utc>>,
        helped_promote_canonical_head: bool,
        receipt_days: &BTreeSet<NaiveDate>,
    ) -> Vec<BadgeAward>;
}

#[derive(Clone, Debug, Default)]
/// Provides default badge services.
pub struct DefaultBadgeService;

impl BadgeService for DefaultBadgeService {
    fn badges_for_row(
        &self,
        first_receipt_at: Option<DateTime<Utc>>,
        helped_promote_canonical_head: bool,
        receipt_days: &BTreeSet<NaiveDate>,
    ) -> Vec<BadgeAward> {
        let mut badges = Vec::new();
        if let Some(awarded_at) = first_receipt_at {
            badges.push(BadgeAward {
                kind: BadgeKind::FirstAcceptedUpdate,
                label: "First Accepted Update".into(),
                awarded_at: Some(awarded_at),
                detail: Some("Accepted work contributed to the network.".into()),
            });
        }
        if helped_promote_canonical_head {
            badges.push(BadgeAward {
                kind: BadgeKind::HelpedPromoteCanonicalHead,
                label: "Helped Promote Canonical Head".into(),
                awarded_at: first_receipt_at,
                detail: Some("Accepted work was included in a promoted merge.".into()),
            });
        }
        if has_receipt_day_streak(receipt_days, 7) {
            badges.push(BadgeAward {
                kind: BadgeKind::SevenDayStreak,
                label: "7-Day Streak".into(),
                awarded_at: first_receipt_at,
                detail: Some("Accepted work landed on seven consecutive days.".into()),
            });
        }
        badges
    }
}

#[derive(Clone, Debug, Default)]
/// Provides no badge services.
pub struct NoBadgeService;

impl BadgeService for NoBadgeService {
    fn badges_for_row(
        &self,
        _first_receipt_at: Option<DateTime<Utc>>,
        _helped_promote_canonical_head: bool,
        _receipt_days: &BTreeSet<NaiveDate>,
    ) -> Vec<BadgeAward> {
        Vec::new()
    }
}

/// Defines behavior for contribution rollup service.
pub trait ContributionRollupService {
    /// Performs the entries operation.
    fn entries(&self, input: &LeaderboardComputationInput<'_>) -> Vec<LeaderboardEntry>;
}

/// Defines behavior for leaderboard service.
pub trait LeaderboardService {
    /// Performs the snapshot operation.
    fn snapshot(&self, input: &LeaderboardComputationInput<'_>) -> LeaderboardSnapshot;
}

#[derive(Clone, Debug, Default)]
/// Provides no leaderboard services.
pub struct NoLeaderboardService;

impl LeaderboardService for NoLeaderboardService {
    fn snapshot(&self, input: &LeaderboardComputationInput<'_>) -> LeaderboardSnapshot {
        LeaderboardSnapshot {
            network_id: input.network_id.clone(),
            score_version: LEADERBOARD_SCORE_VERSION_V1.into(),
            entries: Vec::new(),
            captured_at: input.captured_at,
        }
    }
}

#[derive(Clone, Debug)]
/// Provides receipt leaderboard services.
pub struct ReceiptLeaderboardService<P = NoProfileService, B = DefaultBadgeService> {
    profile_service: P,
    badge_service: B,
}

impl Default for ReceiptLeaderboardService<NoProfileService, DefaultBadgeService> {
    fn default() -> Self {
        Self {
            profile_service: NoProfileService,
            badge_service: DefaultBadgeService,
        }
    }
}

impl<P, B> ReceiptLeaderboardService<P, B> {
    /// Creates a new value.
    pub fn new(profile_service: P, badge_service: B) -> Self {
        Self {
            profile_service,
            badge_service,
        }
    }
}

impl<P, B> ContributionRollupService for ReceiptLeaderboardService<P, B>
where
    P: ProfileService,
    B: BadgeService,
{
    fn entries(&self, input: &LeaderboardComputationInput<'_>) -> Vec<LeaderboardEntry> {
        #[derive(Default)]
        struct AggregateRow {
            principal_id: Option<PrincipalId>,
            peer_ids: BTreeSet<PeerId>,
            label: String,
            social_profile: Option<SocialProfile>,
            base_head_receipt_counts: BTreeMap<burn_p2p_core::HeadId, u32>,
            accepted_work_score: f64,
            quality_weighted_impact_score: f64,
            validation_service_score: f64,
            artifact_serving_score: f64,
            accepted_receipt_count: u64,
            first_receipt_at: Option<DateTime<Utc>>,
            last_receipt_at: Option<DateTime<Utc>>,
            receipt_days: BTreeSet<NaiveDate>,
            helped_promote_canonical_head: bool,
        }

        let promoted_receipt_ids = input
            .merge_certificates
            .iter()
            .flat_map(|certificate| certificate.contribution_receipts.iter().cloned())
            .collect::<BTreeSet<_>>();
        let mut rows = BTreeMap::<String, AggregateRow>::new();

        for receipt in input.receipts {
            let principal_id = input.peer_principals.get(&receipt.peer_id).cloned();
            let key = principal_id
                .as_ref()
                .map(|principal_id| format!("principal:{}", principal_id.as_str()))
                .unwrap_or_else(|| format!("peer:{}", receipt.peer_id.as_str()));
            let fallback_label = principal_id
                .as_ref()
                .map(|principal_id| principal_id.as_str().to_owned())
                .unwrap_or_else(|| receipt.peer_id.as_str().to_owned());
            let social_profile = principal_id
                .as_ref()
                .and_then(|principal_id| self.profile_service.social_profile(principal_id));
            let sanitized_profile = social_profile.as_ref().and_then(sanitize_social_profile);
            let label = leaderboard_label(sanitized_profile.as_ref(), &fallback_label);

            let row = rows.entry(key).or_default();
            row.principal_id = principal_id;
            row.label = label;
            row.social_profile = sanitized_profile.or_else(|| row.social_profile.clone());
            row.peer_ids.insert(receipt.peer_id.clone());
            let accepted_work_score = receipt.accepted_weight.max(0.0);
            let repeated_receipt_count = row
                .base_head_receipt_counts
                .get(&receipt.base_head_id)
                .copied()
                .unwrap_or(0);
            let anti_gaming_multiplier = anti_gaming_multiplier(repeated_receipt_count);
            let weighted_score = accepted_work_score * anti_gaming_multiplier;
            row.base_head_receipt_counts
                .entry(receipt.base_head_id.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1);
            row.accepted_work_score += weighted_score;
            row.quality_weighted_impact_score +=
                weighted_score * quality_multiplier(&receipt.metrics);
            row.validation_service_score +=
                validation_service_points(&receipt.metrics) * anti_gaming_multiplier;
            row.artifact_serving_score +=
                serving_service_points(&receipt.metrics) * anti_gaming_multiplier;
            row.accepted_receipt_count += 1;
            row.first_receipt_at = Some(
                row.first_receipt_at
                    .map(|current| current.min(receipt.accepted_at))
                    .unwrap_or(receipt.accepted_at),
            );
            row.last_receipt_at = Some(
                row.last_receipt_at
                    .map(|current| current.max(receipt.accepted_at))
                    .unwrap_or(receipt.accepted_at),
            );
            row.receipt_days.insert(receipt.accepted_at.date_naive());
            row.helped_promote_canonical_head |= receipt.merge_cert_id.is_some();
            row.helped_promote_canonical_head |= promoted_receipt_ids.contains(&receipt.receipt_id);
        }

        let mut entries = rows
            .into_values()
            .map(|row| {
                let rollup = ContributionRollup {
                    accepted_work_score: row.accepted_work_score,
                    quality_weighted_impact_score: row.quality_weighted_impact_score,
                    validation_service_score: row.validation_service_score,
                    artifact_serving_score: row.artifact_serving_score,
                    leaderboard_score_v1: row.accepted_work_score
                        + (row.quality_weighted_impact_score * 0.5)
                        + row.validation_service_score
                        + row.artifact_serving_score,
                    accepted_receipt_count: row.accepted_receipt_count,
                    last_receipt_at: row.last_receipt_at,
                };
                LeaderboardEntry {
                    identity: LeaderboardIdentity {
                        principal_id: row.principal_id,
                        peer_ids: row.peer_ids,
                        label: row.label,
                        social_profile: row.social_profile,
                    },
                    accepted_work_score: rollup.accepted_work_score,
                    quality_weighted_impact_score: rollup.quality_weighted_impact_score,
                    validation_service_score: rollup.validation_service_score,
                    artifact_serving_score: rollup.artifact_serving_score,
                    leaderboard_score_v1: rollup.leaderboard_score_v1,
                    accepted_receipt_count: rollup.accepted_receipt_count,
                    last_receipt_at: rollup.last_receipt_at,
                    badges: self.badge_service.badges_for_row(
                        row.first_receipt_at,
                        row.helped_promote_canonical_head,
                        &row.receipt_days,
                    ),
                }
            })
            .collect::<Vec<_>>();

        entries.sort_by(|left, right| {
            right
                .leaderboard_score_v1
                .partial_cmp(&left.leaderboard_score_v1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(right.last_receipt_at.cmp(&left.last_receipt_at))
                .then(left.identity.label.cmp(&right.identity.label))
        });

        entries
    }
}

impl<P, B> LeaderboardService for ReceiptLeaderboardService<P, B>
where
    P: ProfileService,
    B: BadgeService,
{
    fn snapshot(&self, input: &LeaderboardComputationInput<'_>) -> LeaderboardSnapshot {
        LeaderboardSnapshot {
            network_id: input.network_id.clone(),
            score_version: LEADERBOARD_SCORE_VERSION_V1.into(),
            entries: self.entries(input),
            captured_at: input.captured_at,
        }
    }
}

fn leaderboard_label(profile: Option<&SocialProfile>, fallback_label: &str) -> String {
    match profile {
        Some(profile) => profile
            .display_name
            .as_ref()
            .filter(|display_name| !display_name.trim().is_empty())
            .cloned()
            .or_else(|| profile.org_slug.clone())
            .unwrap_or_else(|| fallback_label.to_owned()),
        None => fallback_label.to_owned(),
    }
}

fn sanitize_social_profile(profile: &SocialProfile) -> Option<SocialProfile> {
    match profile.visibility {
        IdentityVisibility::Anonymous | IdentityVisibility::AggregateOnly => None,
        IdentityVisibility::PublicDisplayName => Some(SocialProfile {
            principal_id: profile.principal_id.clone(),
            display_name: profile.display_name.clone(),
            avatar_url: None,
            profile_url: None,
            org_slug: None,
            team_slug: None,
            visibility: profile.visibility.clone(),
        }),
        IdentityVisibility::PublicOrg => Some(SocialProfile {
            principal_id: profile.principal_id.clone(),
            display_name: profile.display_name.clone(),
            avatar_url: None,
            profile_url: None,
            org_slug: profile.org_slug.clone(),
            team_slug: profile.team_slug.clone(),
            visibility: profile.visibility.clone(),
        }),
        IdentityVisibility::PublicProfile => Some(profile.clone()),
    }
}

fn quality_multiplier(metrics: &BTreeMap<String, MetricValue>) -> f64 {
    match metrics.get("accuracy").or_else(|| metrics.get("score")) {
        Some(MetricValue::Float(value)) => value.max(0.0),
        Some(MetricValue::Integer(value)) => (*value as f64).max(0.0),
        Some(MetricValue::Bool(_)) | Some(MetricValue::Text(_)) => 1.0,
        None => match metrics.get("loss") {
            Some(MetricValue::Float(value)) => 1.0 / (1.0 + value.max(0.0)),
            Some(MetricValue::Integer(value)) => 1.0 / (1.0 + (*value as f64).max(0.0)),
            Some(MetricValue::Bool(_)) | Some(MetricValue::Text(_)) | None => 1.0,
        },
    }
}

fn validation_service_points(metrics: &BTreeMap<String, MetricValue>) -> f64 {
    match metrics.get("validated_chunks") {
        Some(MetricValue::Float(value)) => value.max(0.0),
        Some(MetricValue::Integer(value)) => (*value as f64).max(0.0),
        Some(MetricValue::Bool(_)) | Some(MetricValue::Text(_)) | None => 0.0,
    }
}

fn serving_service_points(metrics: &BTreeMap<String, MetricValue>) -> f64 {
    for key in ["served_chunks", "served_bytes"] {
        match metrics.get(key) {
            Some(MetricValue::Float(value)) => return value.max(0.0),
            Some(MetricValue::Integer(value)) => return (*value as f64).max(0.0),
            Some(MetricValue::Bool(_)) | Some(MetricValue::Text(_)) => return 0.0,
            None => {}
        }
    }
    0.0
}

fn anti_gaming_multiplier(repeated_receipt_count: u32) -> f64 {
    1.0 / (1.0 + repeated_receipt_count as f64)
}

fn has_receipt_day_streak(receipt_days: &BTreeSet<NaiveDate>, required_days: usize) -> bool {
    if required_days == 0 {
        return true;
    }

    let mut streak = 0usize;
    let mut previous: Option<NaiveDate> = None;
    for day in receipt_days {
        streak = match previous {
            Some(previous_day) if *day == previous_day.succ_opt().unwrap_or(previous_day) => {
                streak + 1
            }
            _ => 1,
        };
        if streak >= required_days {
            return true;
        }
        previous = Some(*day);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{
        BadgeService, DefaultBadgeService, LeaderboardComputationInput, LeaderboardService,
        NoBadgeService, NoLeaderboardService, ReceiptLeaderboardService, StaticProfileService,
        sanitize_social_profile,
    };
    use std::collections::{BTreeMap, BTreeSet};

    use burn_p2p_core::{
        ArtifactId, ContributionReceipt, ContributionReceiptId, ExperimentId, HeadId,
        IdentityVisibility, MergeCertId, MergeCertificate, MetricValue, NetworkId, PeerId,
        PrincipalId, RevisionId, SocialProfile, StudyId,
    };
    use chrono::{Duration, Utc};

    #[test]
    fn no_leaderboard_service_returns_empty_snapshot() {
        let network_id = NetworkId::new("network-1");
        let input = LeaderboardComputationInput {
            network_id: &network_id,
            receipts: &[],
            merge_certificates: &[],
            peer_principals: &BTreeMap::new(),
            captured_at: Utc::now(),
        };
        let snapshot = NoLeaderboardService.snapshot(&input);
        assert!(snapshot.entries.is_empty());
        assert_eq!(snapshot.score_version, super::LEADERBOARD_SCORE_VERSION_V1);
    }

    #[test]
    fn receipt_leaderboard_service_aggregates_by_principal_and_surfaces_profile() {
        let peer_one = PeerId::new("peer-1");
        let peer_two = PeerId::new("peer-2");
        let principal_id = PrincipalId::new("alice");
        let now = Utc::now();
        let receipt =
            |peer_id: &PeerId, suffix: &str, accepted_weight: f64, loss: f64, accepted_at| {
                ContributionReceipt {
                    receipt_id: ContributionReceiptId::new(format!("receipt-{suffix}")),
                    peer_id: peer_id.clone(),
                    study_id: StudyId::new("study-1"),
                    experiment_id: ExperimentId::new("exp-1"),
                    revision_id: RevisionId::new("rev-1"),
                    base_head_id: HeadId::new("head-0"),
                    artifact_id: ArtifactId::new(format!("artifact-{suffix}")),
                    accepted_at,
                    accepted_weight,
                    metrics: BTreeMap::from([("loss".into(), MetricValue::Float(loss))]),
                    merge_cert_id: None,
                }
            };

        let first_receipt = receipt(&peer_one, "a", 3.0, 1.0, now - Duration::days(1));
        let second_receipt = receipt(&peer_two, "b", 2.0, 0.5, now);
        let merge_certificates = vec![MergeCertificate {
            merge_cert_id: MergeCertId::new("merge-1"),
            study_id: StudyId::new("study-1"),
            experiment_id: ExperimentId::new("exp-1"),
            revision_id: RevisionId::new("rev-1"),
            base_head_id: HeadId::new("head-0"),
            merged_head_id: HeadId::new("head-1"),
            merged_artifact_id: ArtifactId::new("artifact-merged"),
            policy: burn_p2p_core::MergePolicy::WeightedMean,
            issued_at: now,
            promoter_peer_id: PeerId::new("validator-1"),
            promotion_mode: burn_p2p_core::HeadPromotionMode::ValidatorQuorum,
            contribution_receipts: vec![first_receipt.receipt_id.clone()],
        }];
        let peer_principals = BTreeMap::from([
            (peer_one.clone(), principal_id.clone()),
            (peer_two.clone(), principal_id.clone()),
        ]);
        let profiles = StaticProfileService::new(BTreeMap::from([(
            principal_id.clone(),
            SocialProfile {
                principal_id: principal_id.clone(),
                display_name: Some("Alice".into()),
                avatar_url: Some("https://example.invalid/avatar.png".into()),
                profile_url: Some("https://example.invalid/alice".into()),
                org_slug: Some("org".into()),
                team_slug: None,
                visibility: IdentityVisibility::PublicProfile,
            },
        )]));
        let input = LeaderboardComputationInput {
            network_id: &NetworkId::new("network-1"),
            receipts: &[first_receipt.clone(), second_receipt],
            merge_certificates: &merge_certificates,
            peer_principals: &peer_principals,
            captured_at: now,
        };
        let service = ReceiptLeaderboardService::new(profiles, DefaultBadgeService);
        let snapshot = service.snapshot(&input);

        assert_eq!(snapshot.entries.len(), 1);
        let entry = &snapshot.entries[0];
        assert_eq!(entry.identity.principal_id, Some(principal_id));
        assert_eq!(
            entry.identity.peer_ids,
            BTreeSet::from([peer_one, peer_two])
        );
        assert_eq!(entry.identity.label, "Alice");
        assert!(entry.identity.social_profile.is_some());
        assert_eq!(entry.accepted_receipt_count, 2);
        assert!((entry.accepted_work_score - 4.0).abs() < f64::EPSILON);
        assert!(
            entry
                .badges
                .iter()
                .any(|badge| badge.kind == burn_p2p_core::BadgeKind::FirstAcceptedUpdate)
        );
        assert!(
            entry
                .badges
                .iter()
                .any(|badge| badge.kind == burn_p2p_core::BadgeKind::HelpedPromoteCanonicalHead)
        );
    }

    #[test]
    fn no_badge_service_can_disable_badge_computation() {
        let badge_days = BTreeSet::from([Utc::now().date_naive()]);
        let badges = NoBadgeService.badges_for_row(Some(Utc::now()), true, &badge_days);
        assert!(badges.is_empty());
    }

    #[test]
    fn private_profiles_are_redacted_from_public_leaderboard_entries() {
        let now = Utc::now();
        let principal_id = PrincipalId::new("private-user");
        let service = ReceiptLeaderboardService::new(
            StaticProfileService::new(BTreeMap::from([(
                principal_id.clone(),
                SocialProfile {
                    principal_id: principal_id.clone(),
                    display_name: Some("Private User".into()),
                    avatar_url: Some("https://example.invalid/private.png".into()),
                    profile_url: Some("https://example.invalid/private".into()),
                    org_slug: Some("secret-org".into()),
                    team_slug: Some("secret-team".into()),
                    visibility: IdentityVisibility::AggregateOnly,
                },
            )])),
            DefaultBadgeService,
        );
        let receipt = ContributionReceipt {
            receipt_id: ContributionReceiptId::new("receipt-private"),
            peer_id: PeerId::new("peer-private"),
            study_id: StudyId::new("study-1"),
            experiment_id: ExperimentId::new("exp-1"),
            revision_id: RevisionId::new("rev-1"),
            base_head_id: HeadId::new("head-0"),
            artifact_id: ArtifactId::new("artifact-private"),
            accepted_at: now,
            accepted_weight: 1.0,
            metrics: BTreeMap::new(),
            merge_cert_id: None,
        };
        let peer_principals = BTreeMap::from([(PeerId::new("peer-private"), principal_id)]);
        let input = LeaderboardComputationInput {
            network_id: &NetworkId::new("network-1"),
            receipts: &[receipt],
            merge_certificates: &[],
            peer_principals: &peer_principals,
            captured_at: now,
        };

        let snapshot = service.snapshot(&input);
        let entry = &snapshot.entries[0];
        assert_eq!(entry.identity.label, "private-user");
        assert!(entry.identity.social_profile.is_none());
    }

    #[test]
    fn leaderboard_rollup_tracks_validation_and_serving_scores() {
        let now = Utc::now();
        let peer_id = PeerId::new("peer-1");
        let principal_id = PrincipalId::new("alice");
        let receipts = vec![
            ContributionReceipt {
                receipt_id: ContributionReceiptId::new("receipt-validate"),
                peer_id: peer_id.clone(),
                study_id: StudyId::new("study-1"),
                experiment_id: ExperimentId::new("exp-1"),
                revision_id: RevisionId::new("rev-1"),
                base_head_id: HeadId::new("head-0"),
                artifact_id: ArtifactId::new("artifact-validate"),
                accepted_at: now,
                accepted_weight: 1.0,
                metrics: BTreeMap::from([("validated_chunks".into(), MetricValue::Integer(12))]),
                merge_cert_id: None,
            },
            ContributionReceipt {
                receipt_id: ContributionReceiptId::new("receipt-serve"),
                peer_id,
                study_id: StudyId::new("study-1"),
                experiment_id: ExperimentId::new("exp-1"),
                revision_id: RevisionId::new("rev-1"),
                base_head_id: HeadId::new("head-0"),
                artifact_id: ArtifactId::new("artifact-serve"),
                accepted_at: now,
                accepted_weight: 1.0,
                metrics: BTreeMap::from([("served_bytes".into(), MetricValue::Integer(4096))]),
                merge_cert_id: None,
            },
        ];
        let peer_principals = BTreeMap::from([(PeerId::new("peer-1"), principal_id)]);
        let input = LeaderboardComputationInput {
            network_id: &NetworkId::new("network-1"),
            receipts: &receipts,
            merge_certificates: &[],
            peer_principals: &peer_principals,
            captured_at: now,
        };

        let snapshot = ReceiptLeaderboardService::default().snapshot(&input);
        let entry = &snapshot.entries[0];
        assert_eq!(entry.validation_service_score, 12.0);
        assert_eq!(entry.artifact_serving_score, 2048.0);
        assert!(entry.leaderboard_score_v1 > entry.accepted_work_score);
    }

    #[test]
    fn public_org_visibility_keeps_org_metadata_but_redacts_full_profile_fields() {
        let profile = sanitize_social_profile(&SocialProfile {
            principal_id: PrincipalId::new("org-user"),
            display_name: Some("Org User".into()),
            avatar_url: Some("https://example.invalid/avatar.png".into()),
            profile_url: Some("https://example.invalid/profile".into()),
            org_slug: Some("burn-org".into()),
            team_slug: Some("research".into()),
            visibility: IdentityVisibility::PublicOrg,
        })
        .expect("public org profile");

        assert_eq!(profile.display_name.as_deref(), Some("Org User"));
        assert_eq!(profile.org_slug.as_deref(), Some("burn-org"));
        assert_eq!(profile.team_slug.as_deref(), Some("research"));
        assert!(profile.avatar_url.is_none());
        assert!(profile.profile_url.is_none());
    }

    #[test]
    fn default_badge_service_awards_seven_day_streaks() {
        let start = Utc::now().date_naive() - Duration::days(6);
        let receipt_days = BTreeSet::from_iter((0..7).map(|offset| start + Duration::days(offset)));
        let badges = DefaultBadgeService.badges_for_row(Some(Utc::now()), false, &receipt_days);

        assert!(
            badges
                .iter()
                .any(|badge| badge.kind == burn_p2p_core::BadgeKind::SevenDayStreak)
        );
    }

    #[test]
    fn repeated_receipts_on_the_same_base_head_get_diminishing_public_score_weight() {
        let now = Utc::now();
        let peer_principals = BTreeMap::from([(PeerId::new("peer-1"), PrincipalId::new("alice"))]);
        let receipts = vec![
            ContributionReceipt {
                receipt_id: ContributionReceiptId::new("receipt-1"),
                peer_id: PeerId::new("peer-1"),
                study_id: StudyId::new("study-1"),
                experiment_id: ExperimentId::new("exp-1"),
                revision_id: RevisionId::new("rev-1"),
                base_head_id: HeadId::new("head-0"),
                artifact_id: ArtifactId::new("artifact-1"),
                accepted_at: now,
                accepted_weight: 4.0,
                metrics: BTreeMap::new(),
                merge_cert_id: None,
            },
            ContributionReceipt {
                receipt_id: ContributionReceiptId::new("receipt-2"),
                peer_id: PeerId::new("peer-1"),
                study_id: StudyId::new("study-1"),
                experiment_id: ExperimentId::new("exp-1"),
                revision_id: RevisionId::new("rev-1"),
                base_head_id: HeadId::new("head-0"),
                artifact_id: ArtifactId::new("artifact-2"),
                accepted_at: now,
                accepted_weight: 4.0,
                metrics: BTreeMap::new(),
                merge_cert_id: None,
            },
        ];
        let input = LeaderboardComputationInput {
            network_id: &NetworkId::new("network-1"),
            receipts: &receipts,
            merge_certificates: &[],
            peer_principals: &peer_principals,
            captured_at: now,
        };

        let snapshot = ReceiptLeaderboardService::default().snapshot(&input);
        let entry = &snapshot.entries[0];
        assert_eq!(entry.accepted_receipt_count, 2);
        assert!((entry.accepted_work_score - 6.0).abs() < f64::EPSILON);
    }
}
