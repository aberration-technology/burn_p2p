# syntax=docker/dockerfile:1.7
ARG RUST_VERSION=1.92
FROM rust:${RUST_VERSION}-bookworm AS builder

WORKDIR /workspace
COPY . .

ARG BURN_P2P_BOOTSTRAP_FEATURES=admin-http,metrics,auth-static
RUN cargo build \
    --locked \
    --release \
    --manifest-path crates/burn_p2p_bootstrap/Cargo.toml \
    --bin burn-p2p-bootstrap \
    --no-default-features \
    --features "${BURN_P2P_BOOTSTRAP_FEATURES}"

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /workspace/target/release/burn-p2p-bootstrap /app/bin/burn-p2p-bootstrap

ENTRYPOINT ["/app/bin/burn-p2p-bootstrap"]
