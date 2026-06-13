# AGENTS.md instructions for burn_p2p

## Role In The Stack

- `burn_p2p` extends Burn training with peer-to-peer coordination and continuous training flows.
- Treat `burn_ecs` as the ECS/app/plugin provider. P2P training integration should be a `burn_ecs` plugin, not a competing runtime abstraction.
- Downstream layering is `burn_ecs -> burn_p2p -> burn_dragon`; do not add dependencies on Dragon or Dragon-specific ruliad concepts.

## Local Tooling

- Use rustup toolchain binaries directly when running Rust commands. Prefer the installed stable toolchain path from `rustup which cargo` / `rustup which rustc`, or the matching files under `~/.rustup/toolchains/stable-*/bin/`.
- Set `RUSTC` to the matching rustup `rustc` when invoking Cargo. For wrappers such as `cargo fmt`, also set `CARGO` to the matching rustup `cargo` when needed.
- Avoid relying on `/snap/bin/cargo`; snap confinement can fail before repository logic runs.

## ECS Integration Principles

- Implement P2P training behavior as Bevy plugins, systems, messages, resources, and components layered over `burn_ecs`.
- P2P-specific metadata should attach to canonical `burn_ecs` entities where possible, especially training windows and run-scoped state.
- Key P2P metadata by `(run_id, window_id)` or stronger identities. Do not assume window IDs are globally unique across a multi-run `App`.
- Keep run-local state on entities and true process-wide handles as resources. Avoid adding global mutable state that would cause contention between multiple training pipelines.
- Use `TrainingSet` ordering intentionally. Message mirroring should run before systems that consume the mirrored common events or entity indexes.
- Derive Bevy `Component` and `Resource` where possible. Import the provider path through `burn_ecs` when derive macros need a visible `bevy_ecs` crate path.

## Scope Boundaries

- Browser, auth, swarm, checkpoint, and publishing code should remain modular by crate. Do not push P2P transport concerns into the ECS provider.
- Keep the public facade stable; add narrow compatibility aliases only when needed for downstream branches.
- Prefer deterministic smoke tests for ECS behavior over live network tests unless the user explicitly asks for deployment or end-to-end P2P validation.

## Testing And Validation

- For ECS/plugin changes, run:
  - `cargo fmt --all`
  - `cargo check -p burn_p2p --features ecs`
  - `cargo test -p burn_p2p --features ecs ecs::plugin::tests::p2p_plugin_tracks_window_state_and_mirrors_common_events`
- For transport or deployment changes, add the relevant package-specific checks rather than broad workspace rebuilds unless needed.
