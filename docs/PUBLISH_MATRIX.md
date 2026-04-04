# Publish Matrix

Target prerelease line: `0.21.0-pre.2`

The publish set for `0.21.0-pre.2` is intentionally limited to the reusable
library crates with a clear external-consumer story and a clean registry-safe
dependency closure.

Service/application companions remain internal for this prerelease:

- `burn_p2p_bootstrap`
- `burn_p2p_browser`
- `burn_p2p_ui`
- `burn_p2p_metrics`
- `burn_p2p_publish`
- `burn_p2p_portal`
- `burn_p2p_social`
- `burn_p2p_auth_external`
- `burn_p2p_auth_github`
- `burn_p2p_auth_oidc`
- `burn_p2p_auth_oauth`
- `burn_p2p_testkit`

Reason:

- they are operational companions rather than the main crates.io integration
  surface for this prerelease
- keeping them unpublished avoids a bootstrap/browser/UI prerelease cycle in
  local and pre-publication verification
- the public facade and companion library surface remains registry-safe without
  them

| Crate | Publish? | Why | Depends on | Required publish order |
| --- | --- | --- | --- | --- |
| `burn_p2p_core` | yes | Core schema/ID crate used by every published library crate. | none | 1 |
| `burn_p2p_experiment` | yes | Public experiment/revision model used by facade and supporting libraries. | `burn_p2p_core` | 2 |
| `burn_p2p_checkpoint` | yes | Public checkpoint/artifact boundary used by facade and engine. | `burn_p2p_core`, `burn_p2p_experiment` | 3 |
| `burn_p2p_limits` | yes | Public capability and role-selection crate used by the facade. | `burn_p2p_core` | 4 |
| `burn_p2p_dataloader` | yes | Public dataset planning and lease crate used by the facade. | `burn_p2p_core`, `burn_p2p_experiment` | 5 |
| `burn_p2p_security` | yes | Public admission and validator-policy crate used across the published stack. | `burn_p2p_core`, `burn_p2p_experiment` | 6 |
| `burn_p2p_swarm` | yes | Public swarm/runtime boundary used by the facade. | `burn_p2p_core`, `burn_p2p_experiment` | 7 |
| `burn_p2p_engine` | yes | `burn_p2p` still depends on it as a normal optional dependency behind the published `burn` feature. It remains an implementation-detail crate, not the recommended entrypoint. | `burn_p2p_checkpoint`, `burn_p2p_core`, `burn_p2p_experiment` | 8 |
| `burn_p2p` | yes | Primary public facade crate and the README entrypoint. | `burn_p2p_checkpoint`, `burn_p2p_core`, `burn_p2p_dataloader`, `burn_p2p_engine` (optional), `burn_p2p_experiment`, `burn_p2p_limits`, `burn_p2p_security`, `burn_p2p_swarm` | 9 |
| `burn_p2p_metrics` | no | Optional metrics/indexer service crate. Useful in deployments, but not part of the intended crates.io library surface for this prerelease. | n/a | n/a |
| `burn_p2p_portal` | no | Portal rendering/product surface; operational companion, not a standalone library target for this release. | n/a | n/a |
| `burn_p2p_social` | no | Optional social/leaderboard service crate. Internal for `0.21.0-pre.2`. | n/a | n/a |
| `burn_p2p_auth_external` | no | Shared auth/provider integration layer used only by unpublished service companions in this release. | n/a | n/a |
| `burn_p2p_publish` | no | Optional checkpoint-publication service crate. Internal for this prerelease. | n/a | n/a |
| `burn_p2p_auth_github` | no | Provider-specific auth connector used only by unpublished service companions in this release. | n/a | n/a |
| `burn_p2p_auth_oidc` | no | Provider-specific auth connector used only by unpublished service companions in this release. | n/a | n/a |
| `burn_p2p_auth_oauth` | no | Provider-specific auth connector used only by unpublished service companions in this release. | n/a | n/a |
| `burn_p2p_bootstrap` | no | Bootstrap/operator application crate. Not published on this prerelease line. | n/a | n/a |
| `burn_p2p_ui` | no | Typed UI/telemetry contract crate tied to unpublished application companions for this prerelease. | n/a | n/a |
| `burn_p2p_browser` | no | Browser-edge/browser-worker companion crate. Internal until the companion application stack is published. | n/a | n/a |
| `burn_p2p_testkit` | no | Internal simulation/QA harness with no crates.io end-user story for this prerelease. | n/a | n/a |

Notes:

- `burn_p2p_engine` stays in the publish set because the public `burn_p2p`
  facade still exposes it through the published `burn` feature.
- The unpublished companion crates remain version-aligned at
  `0.21.0-pre.2` inside the workspace, but they are explicitly `publish =
  false` and do not block the registry release of the library set above.
