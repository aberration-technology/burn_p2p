#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PUBLISH_CRATES=(
  burn_p2p_core
  burn_p2p_experiment
  burn_p2p_checkpoint
  burn_p2p_limits
  burn_p2p_dataloader
  burn_p2p_security
  burn_p2p_swarm
  burn_p2p_engine
  burn_p2p
)

declare -A PUBLISH_CRATE_PATHS=(
  [burn_p2p]="$ROOT_DIR/crates/burn_p2p"
  [burn_p2p_checkpoint]="$ROOT_DIR/crates/burn_p2p_checkpoint"
  [burn_p2p_core]="$ROOT_DIR/crates/burn_p2p_core"
  [burn_p2p_dataloader]="$ROOT_DIR/crates/burn_p2p_dataloader"
  [burn_p2p_engine]="$ROOT_DIR/crates/burn_p2p_engine"
  [burn_p2p_experiment]="$ROOT_DIR/crates/burn_p2p_experiment"
  [burn_p2p_limits]="$ROOT_DIR/crates/burn_p2p_limits"
  [burn_p2p_security]="$ROOT_DIR/crates/burn_p2p_security"
  [burn_p2p_swarm]="$ROOT_DIR/crates/burn_p2p_swarm"
)

PACKAGE_FLAGS=()
if [[ "${ALLOW_DIRTY:-0}" == "1" ]]; then
  PACKAGE_FLAGS+=(--allow-dirty)
fi

LOCAL_PATCH_DRY_RUN="${LOCAL_PATCH_DRY_RUN:-${ALLOW_DIRTY:-0}}"

run_publish_dry_run() {
  local crate="$1"

  if [[ "$LOCAL_PATCH_DRY_RUN" != "1" ]]; then
    cargo publish --dry-run -p "$crate" "${PACKAGE_FLAGS[@]}"
    return
  fi

  local temp_home
  temp_home="$(mktemp -d "${TMPDIR:-/tmp}/burn_p2p_publish_home.XXXXXX")"

  local wrote_patch=0
  : > "$temp_home/config.toml"
  for patched_crate in "${PUBLISH_CRATES[@]}"; do
    if [[ "$patched_crate" == "$crate" ]]; then
      break
    fi
    if [[ "$wrote_patch" == "0" ]]; then
      echo "[patch.crates-io]" >> "$temp_home/config.toml"
      wrote_patch=1
    fi
    printf '%s = { path = "%s" }\n' "$patched_crate" "${PUBLISH_CRATE_PATHS[$patched_crate]}" >> "$temp_home/config.toml"
  done

  CARGO_HOME="$temp_home" cargo publish --dry-run -p "$crate" "${PACKAGE_FLAGS[@]}"
  rm -rf "$temp_home"
}

cargo fmt --all --check

cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p burn_p2p --all-targets --features burn -- -D warnings
cargo clippy -p burn_p2p_swarm --all-targets -- -D warnings
cargo clippy -p burn_p2p_security --all-targets -- -D warnings
cargo clippy -p burn_p2p_publish --all-targets --features fs,s3 -- -D warnings
cargo clippy -p burn_p2p_bootstrap --all-targets -- -D warnings
cargo clippy -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,auth-static --all-targets -- -D warnings
cargo clippy -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,metrics-indexer,artifact-publish,artifact-download,artifact-fs,artifact-s3,portal,browser-edge,rbac,auth-static,auth-github,auth-oidc,auth-oauth,social --all-targets -- -D warnings

cargo test --workspace
cargo test -p burn_p2p --features burn
cargo test -p burn_p2p_publish --features fs,s3
cargo test -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,auth-static
cargo test -p burn_p2p_bootstrap --no-default-features --features admin-http,metrics,metrics-indexer,artifact-publish,artifact-download,artifact-fs,artifact-s3,portal,browser-edge,rbac,auth-static,auth-github,auth-oidc,auth-oauth,social

RUSTDOCFLAGS="-D warnings" cargo test --workspace --doc
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

for crate in "${PUBLISH_CRATES[@]}"; do
  cargo package --list -p "$crate" "${PACKAGE_FLAGS[@]}"
done

for crate in "${PUBLISH_CRATES[@]}"; do
  run_publish_dry_run "$crate"
done
