# burn_p2p Leaderboard Scoring

## Source of truth

Leaderboards only consume accepted receipts and related merge metadata.

Counted:

- accepted training receipts
- accepted validation receipts
- accepted serving/archive receipts
- accepted audit receipts

Not counted:

- self-reported FLOPs
- self-reported uptime
- self-reported bandwidth
- rejected or unaccepted work

## Identity model

Scoring rolls up to `PrincipalId`, not peer ID.

One principal may contribute through:

- many peer IDs
- many sessions
- native and browser peers

## Current score fields

The current rollup surface exposes:

- `accepted_work_score`
- `quality_weighted_impact_score`
- `validation_service_score`
- `artifact_serving_score`
- `leaderboard_score_v1`

## Current `leaderboard_score_v1`

The current implementation computes:

`accepted_work_score + (quality_weighted_impact_score * 0.5) + validation_service_score + artifact_serving_score`

This is intentionally explicit so the score is inspectable rather than a vanity number.

## Quality interpretation

`quality_weighted_impact_score` currently uses receipt metrics:

- `accuracy` or `score` if present
- otherwise `loss` as an inverse-quality proxy
- otherwise a neutral multiplier

## Badges

The current default badge service awards:

- first accepted update
- helped promote canonical head
- seven-day streak

These badges are product-layer signals only. They do not affect runtime validity.

## Anti-gaming posture

Current protections:

- accepted receipts only
- principal-level aggregation
- no self-reported vanity metrics

Further hardening still belongs on the roadmap:

- suspicious identity clustering
- org/team abuse review
- anomaly flags on public boards

The current implementation now also applies diminishing public-score weight to repeated accepted
receipts from the same principal on the same base head. This is a conservative anti-gaming
heuristic for the product layer only. It does not change receipt validity, merge validity, or peer
admission.

## UX guidance

When presenting leaderboard scores:

- show the component scores
- explain the score version
- avoid implying the score is a total measure of contribution quality
- keep privacy settings visible and understandable
