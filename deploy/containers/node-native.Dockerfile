# syntax=docker/dockerfile:1.7
ARG RUST_VERSION=1.85
FROM rust:${RUST_VERSION}-bookworm AS builder

WORKDIR /workspace
COPY . .

COPY deploy/containers/build-target.sh /usr/local/bin/build-target.sh
RUN chmod +x /usr/local/bin/build-target.sh

ARG APP_MANIFEST_PATH
ARG APP_TARGET_KIND=bin
ARG APP_TARGET_NAME
ARG APP_FEATURES=
ARG APP_NO_DEFAULT_FEATURES=false
RUN /usr/local/bin/build-target.sh \
    "${APP_MANIFEST_PATH}" \
    "${APP_TARGET_KIND}" \
    "${APP_TARGET_NAME}" \
    "${APP_FEATURES}" \
    "${APP_NO_DEFAULT_FEATURES}"

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /out/app-target /app/bin/app-target

ENTRYPOINT ["/app/bin/app-target"]
