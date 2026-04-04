# Multi-Workload Burn Integration Guide

This guide shows the recommended shape for a downstream Burn project that wants
to advertise more than one workload under the same `burn_p2p` project family.

## When To Use This Path

Use a multi-workload family when one deployment needs to expose multiple
workload manifests without reconnecting clients to a different network.

Typical cases:

- train and validate two model sizes in the same project family
- expose separate fine-tune and evaluation workloads behind one release train
- keep one trusted network while switching experiments and workloads by
  directory selection instead of by reconnecting

Do not use this path just to rename one workload. If there is only one compiled
workload, prefer
[SingleWorkloadProjectFamily](/home/mosure/repos/burn_p2p/crates/burn_p2p/src/project_family.rs).

## Recommended Model

The recommended moving pieces are:

1. define one runtime project type per workload
2. implement `P2pWorkload<B>` for each workload
3. expose all `SupportedWorkload` values from one `P2pProjectFamily`
4. select the active workload through `NodeBuilder::for_workload(...)`

Today the concrete workload types still implement the compatibility traits in
`burn_p2p::compat`, because the internal adapter layer still depends on them.
Treat those as implementation detail rather than the conceptual API.

## Minimal Family Skeleton

```rust
use burn_p2p::{
    ClientReleaseManifest, NodeBuilder, P2pProjectFamily, ProjectBackend,
    SelectedWorkloadProject, SupportedWorkload, WorkloadId,
};

#[derive(Clone)]
struct ExampleFamily<B> {
    release: ClientReleaseManifest,
    workloads: std::collections::BTreeMap<WorkloadId, ExampleWorkload<B>>,
}

impl<B: ProjectBackend> P2pProjectFamily for ExampleFamily<B> {
    type Backend = B;

    fn client_release_manifest(&self) -> &ClientReleaseManifest {
        &self.release
    }

    fn prepare_workload(
        &self,
        workload_id: &WorkloadId,
    ) -> anyhow::Result<SelectedWorkloadProject<Self>> {
        SelectedWorkloadProject::new(self.clone(), workload_id.clone())
    }
}
```

The important point is that the release manifest advertises every compiled
workload, while the runtime only activates one selected workload for a given
node.

## Recommended Builder Flow

The downstream selection path should look like:

1. build a `ClientReleaseManifest` that lists every supported workload
2. construct your family object
3. call `NodeBuilder::new(family)`
4. bind the network and storage
5. select the active workload with `for_workload(...)`
6. spawn the node

Example shape:

```rust
let builder = NodeBuilder::new(family)
    .with_mainnet(genesis)
    .with_storage(storage_config)
    .for_workload(WorkloadId::new("decoder-small"))?;
```

## Selection Rules

Keep these invariants:

- every advertised workload must appear in the release manifest
- the selected workload must be one of those advertised workloads
- all workloads still belong to the same project family and release train
- experiment switching still happens within that compatibility envelope

This keeps workload selection compatible with the project-family and
release-train model instead of creating hidden side channels.

## Operational Guidance

For a multi-workload family, publish clearly:

- which workload IDs are trainer-capable
- which workload IDs are browser-eligible
- which workloads are observer/verifier-only
- whether some workloads require different artifact targets or backends

Do not rely on display names alone. Operators and browser clients should reason
about stable workload IDs and release-manifest compatibility.

## Current Caveats

- The current examples in-tree still emphasize the single-workload path more
  than the multi-workload path.
- The family-level trait surface is still more generic than ideal.
- Browser and portal selection are workload-aware, but the primary tutorial
  path is still native-first.

## Suggested Next Steps

- add a full runnable multi-workload example under `crates/burn_p2p/examples`
- add a browser-visible multi-workload portal fixture
- keep the forward-facing docs focused on `P2pProjectFamily`,
  `SelectedWorkloadProject`, and `NodeBuilder::for_workload(...)`
