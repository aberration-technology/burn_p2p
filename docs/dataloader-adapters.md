# burn_p2p Dataloader Adapter Guide

## Purpose

This guide shows how dataset registrations map onto the current `UpstreamAdapter` variants and
how those adapters interact with microshard planning, lease assignment, and cache fetches.

## Current adapter types

The dataloader currently models these upstream sources:

- `UpstreamAdapter::Local`
- `UpstreamAdapter::Http`
- `UpstreamAdapter::Hf`
- `UpstreamAdapter::S3`
- `UpstreamAdapter::Private`

The disk-cache fetch path is currently implemented for:

- `Local`
- `Http`

The remaining adapters are schema-level integration points and should be treated as composition
surfaces until a deployment-specific fetcher is wired in.

## Local adapter

Use `Local` when:

- the bootstrap or trainer node already has a materialized shard directory
- CI or local development should avoid remote fetches
- browser ingress is not needed for the dataset path

Expected layout:

- `fetch-manifest.json`
- one file per shard locator such as `00000.bin`

Example:

```rust
use burn_p2p::{DatasetRegistration, UpstreamAdapter};

let registration = DatasetRegistration {
    manifest,
    view,
    upstream: UpstreamAdapter::Local {
        root: "/srv/burn-p2p/datasets/wiki-v1".into(),
    },
};
```

## HTTP adapter

Use `Http` when:

- shards are served from a trusted object gateway or CDN
- native and browser clients should share a simple fetch manifest layout
- the deployment wants resumable fetches through a plain signed-edge path

Expected layout:

- `GET {base_url}/fetch-manifest.json`
- `GET {base_url}/{locator}`

Example:

```rust
use burn_p2p::{DatasetRegistration, UpstreamAdapter};

let registration = DatasetRegistration {
    manifest,
    view,
    upstream: UpstreamAdapter::Http {
        base_url: "https://datasets.example.invalid/wiki-v1".into(),
    },
};
```

## Hugging Face adapter

Use `Hf` when:

- the source of truth is a Hugging Face dataset
- a bootstrap or preprocessing job will materialize fetch manifests and shard locators from that
  dataset family

The current repository does not yet ship a production fetch path for `Hf` inside the local disk
cache loader. Treat it as a registration and planning descriptor, not a complete downloader.

## S3 adapter

Use `S3` when:

- the deployment controls an S3-compatible shard bucket
- fetch manifests are generated externally and the runtime only needs typed registration state

As with `Hf`, the current disk-cache fetch path does not directly materialize `S3` shards.

## Private adapter

Use `Private` when:

- a deployment-specific fetcher exists behind an internal scheme
- locators should remain opaque to the generic runtime crates

This adapter is the right place for controlled enterprise/private dataset backends, not for new
general-purpose public protocols.

## Recommended integration flow

1. generate a deterministic `DatasetManifest`
2. derive one or more `DatasetView` variants for tokenization or preprocessing changes
3. register the upstream adapter for that view
4. plan microshards from real sizing data
5. publish or cache a `ShardFetchManifest`
6. issue leases against the planned microshards
7. fetch only the leased shards into the local cache
8. evict old shards aggressively on browser or low-disk deployments

## Operational notes

- Browser-facing deployments should prefer smaller shard locators and explicit cache eviction
  policies.
- Large or skewed datasets should be planned with real sizing inputs rather than synthetic byte
  estimates.
- `max_microshards_per_lease` should stay conservative for browser workloads, especially when
  suspension or low-memory recovery is expected.
