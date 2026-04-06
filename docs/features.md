# Feature Flags

`burn_p2p` is a workspace, not one monolithic crate.

feature flags are used where they meaningfully change compile graph or runtime
surface. most crates intentionally have no public feature flags.

## Short Answer

yes, `burn_p2p` can use feature flags to gate optional sub-crate integration.

that works well for:

- optional burn integration
- optional publication backends
- optional auth providers
- optional app hosts
- optional browser-edge capabilities

that works less well for:

- collapsing every workspace crate into one top-level dependency
- hiding major runtime boundaries between native training, browser runtime,
  bootstrap services, and optional ui hosts

reason:

- cargo features can gate optional dependencies and re-exports
- cargo features do not erase real runtime boundaries
- a browser runtime, a native trainer, and a bootstrap http service are not
  just "more code", they are different deployment targets

current design aims for:

- one small core downstream dependency for training: `burn_p2p`
- separate composition crates for browser, app, and bootstrap/coherence-seed deployment
- feature flags inside those composition crates where it helps

## Design Notes

- `burn_p2p` is the main downstream entrypoint
- browser runtime, app ui, bootstrap/browser-edge services, publication backends, and
  social surfaces stay in separate crates
- separation keeps downstream compile graphs smaller than a single mega-crate
- optional features are used mainly inside composition crates such as
  `burn_p2p_bootstrap`, `burn_p2p_app`, and `burn_p2p_publish`

## Facade Strategy

closest current match to a bevy-style setup:

- `burn_p2p` is the single cargo entry for native p2p training
- `burn_p2p = { features = ["burn"] }` is the ready-to-go burn path
- `burn_p2p_bootstrap` is the ready-to-go coherence-seed + browser-edge path
- `burn_p2p_app` is the ready-to-go reference ui path

current repo does **not** try to make one crate auto-import every other crate.

that is intentional:

- it would pull browser, portal, and bootstrap dependencies into projects that
  only want trainer/validator runtime code
- it would blur crate boundaries that are currently clean
- it would make wasm/native/server dependency graphs harder to reason about

clean future direction, if a broader single-entry facade is wanted:

- keep `burn_p2p` as the core runtime crate
- add a separate product/meta crate that re-exports browser, app, and
  bootstrap surfaces behind features
- avoid turning `burn_p2p` itself into a kitchen-sink crate

## `burn_p2p`

main downstream runtime crate.

| feature | default | enables | intended use |
| --- | --- | --- | --- |
| `burn` | no | burn learner integration helpers and `burn_p2p_engine` | downstream burn training projects |

notes:

- `burn_p2p` intentionally does not auto-pull the browser runtime, app ui,
  or bootstrap services
- downstream burn users usually want:

```toml
[dependencies]
burn_p2p = { version = "=0.21.0-pre.6", features = ["burn"] }
```

## `burn_p2p_browser`

browser runtime glue crate.

public features: none

notes:

- browser runtime is its own crate because wasm/browser transport, worker, and
  capability logic are a different runtime target from native training
- downstream browser apps add `burn_p2p_browser` explicitly when they need it

## `burn_p2p_app`

optional reference ui/product surface.

| feature | default | enables | intended use |
| --- | --- | --- | --- |
| `readonly` | yes | read-only app rendering | static or view-only app surfaces |
| `interactive` | no | interactive app actions on top of `readonly` | browser or operator flows with mutations |
| `social` | no | social widgets and related rendering seams | deployments using social surfaces |
| `browser-join` | no | browser join/onboarding ui on top of `interactive` | browser training or browser admission flows |
| `web-client` | no | dioxus web host | wasm/browser app host |
| `desktop-client` | no | dioxus desktop host, native node host bridge | native desktop app window |

notes:

- `burn_p2p_app` is the reference dioxus app, not the core protocol
- static assets can be hosted anywhere, including a cdn
- live state still comes from the bootstrap/browser-edge http surface, mainly
  endpoints such as `/portal/snapshot`

## `burn_p2p_bootstrap`

coherence-seed/admin/browser-edge composition crate.

| feature | default | enables | intended use |
| --- | --- | --- | --- |
| `admin-http` | no | admin/status http surface | operational bootstrap service |
| `metrics` | no | metrics routes and metrics state | operator metrics |
| `metrics-indexer` | no | metrics indexing with `burn_p2p_metrics` | indexed metrics views |
| `artifact-publish` | no | publication plumbing | artifact publication flows |
| `artifact-download` | no | download endpoints on top of publication | browser/native artifact downloads |
| `artifact-fs` | no | filesystem publication backend | local or single-node storage |
| `artifact-s3` | no | s3 publication backend | cloud/object-store publication |
| `browser-edge` | no | served browser/native ui snapshots, html, and edge http/json surface | coherence-seed / bootstrap edge deployments serving app state |
| `browser-join` | no | browser participant join/onboarding additions on top of `browser-edge` | browser trainer/verifier deployments |
| `rbac` | no | role-based access control hooks | guarded admin/public surfaces |
| `auth-static` | no | static auth provider | local/dev/simple deployments |
| `auth-github` | no | github auth provider | github login deployments |
| `auth-oidc` | no | oidc auth provider | enterprise sso |
| `auth-oauth` | no | generic oauth auth provider | hosted oauth providers |
| `auth-external` | no | external auth bridge | custom auth services |
| `social` | no | social/profile/leaderboard extras | community-facing deployments |

notes:

- defaults are intentionally empty
- operators opt into only the admin, browser-edge, publication, and auth
  surfaces they actually need

example:

```toml
[dependencies]
burn_p2p_bootstrap = { version = "=0.21.0-pre.6", default-features = false, features = [
  "admin-http",
  "metrics",
  "browser-edge",
  "browser-join",
  "auth-github",
] }
```

## `burn_p2p_publish`

artifact export/publish crate.

| feature | default | enables | intended use |
| --- | --- | --- | --- |
| `fs` | yes | filesystem exporter | local and simple deployments |
| `s3` | no | s3-compatible exporter | object storage publication |
| `lazy-export` | yes | export on demand | lower background work |
| `eager-export` | no | export immediately | lower first-download latency |

## `burn_p2p_social`

optional profile/leaderboard/badge crate.

| feature | default | enables | intended use |
| --- | --- | --- | --- |
| `profiles` | yes | profile models and rendering | participant identity/profile views |
| `leaderboards` | yes | leaderboard views on top of profiles | rank and score surfaces |
| `badges` | yes | badge/achievement surfaces | lightweight gamification |
| `team-rollups` | no | grouped/team score views | org or team deployments |

## Crates With No Public Features

current workspace crates with no public feature flags:

- `burn_p2p_core`
- `burn_p2p_checkpoint`
- `burn_p2p_limits`
- `burn_p2p_dataloader`
- `burn_p2p_experiment`
- `burn_p2p_security`
- `burn_p2p_swarm`
- `burn_p2p_engine`
- `burn_p2p_metrics`
- `burn_p2p_views`
- `burn_p2p_browser`

`burn_p2p_testkit` is intentionally not a downstream dependency surface. it is
an internal qa/simulation harness used by `xtask`, browser/playwright capture,
synthetic multiprocess runs, adversarial scenarios, and downstream example
verification.

## Practical Combinations

smallest burn training app:

```toml
[dependencies]
burn_p2p = { version = "=0.21.0-pre.6", features = ["burn"] }
```

browser runtime app:

```toml
[dependencies]
burn_p2p_browser = "=0.21.0-pre.6"
burn_p2p_views = "=0.21.0-pre.6"
```

browser or desktop reference ui:

```toml
[dependencies]
burn_p2p_app = { version = "=0.21.0-pre.6", default-features = false, features = [
  "interactive",
  "browser-join",
  "web-client",
] }
```

native desktop portal host:

```toml
[dependencies]
burn_p2p_app = { version = "=0.21.0-pre.6", default-features = false, features = [
  "interactive",
  "desktop-client",
] }
```

reference bootstrap/browser-edge deployment:

```toml
[dependencies]
burn_p2p_bootstrap = { version = "=0.21.0-pre.6", default-features = false, features = [
  "admin-http",
  "metrics",
  "browser-edge",
  "browser-join",
  "auth-oidc",
] }
```
