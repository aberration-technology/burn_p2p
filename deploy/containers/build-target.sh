#!/bin/sh
set -eu

manifest_path="$1"
target_kind="$2"
target_name="$3"
features="${4:-}"
no_default_features="${5:-false}"

build_args="--locked --release --manifest-path ${manifest_path}"

if [ "${no_default_features}" = "true" ]; then
  build_args="${build_args} --no-default-features"
fi

if [ -n "${features}" ]; then
  build_args="${build_args} --features ${features}"
fi

case "${target_kind}" in
  bin)
    cargo build ${build_args} --bin "${target_name}"
    mkdir -p /out
    cp "target/release/${target_name}" /out/app-target
    ;;
  example)
    cargo build ${build_args} --example "${target_name}"
    mkdir -p /out
    cp "target/release/examples/${target_name}" /out/app-target
    ;;
  *)
    echo "unsupported target kind: ${target_kind}" >&2
    exit 1
    ;;
esac
