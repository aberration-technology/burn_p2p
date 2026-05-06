# syntax=docker/dockerfile:1.7
ARG RUST_VERSION=1.92
FROM rust:${RUST_VERSION}-bookworm AS builder

WORKDIR /workspace
COPY . .

ARG APP_MANIFEST_PATH
ARG APP_TARGET_KIND=bin
ARG APP_TARGET_NAME
ARG APP_FEATURES=
ARG APP_NO_DEFAULT_FEATURES=false
RUN cargo run --locked -p xtask -- container-build-target --from-env

FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility

WORKDIR /app
COPY --from=builder /out/app-target /app/bin/app-target

ENTRYPOINT ["/app/bin/app-target"]
