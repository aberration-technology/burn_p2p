# deployment

This directory holds the deployment source of truth:

- `containers/`: role-specific container build recipes
- `config/`: container-friendly bootstrap, reducer, and validator configs
- `compose/`: local and single-host reference stacks
- `terraform/`: cloud reference stacks for AWS and GCP

The deployment split is intentional:

- bootstrap/coherence seed nodes are cheap CPU/network boxes that provide relay, rendezvous, and Kademlia discovery for native peers
- reducer nodes are separate and CPU-oriented
- validator/authority nodes are separate and CPU-oriented
- trainer nodes are separate again and can use CPU or GPU-specific images

Two boundaries matter:

1. `burn-p2p-bootstrap` is a real publishable binary, so bootstrap, reducer, and validator
   containers in this repo are concrete and ready to build.
2. trainer binaries are downstream-application specific because the workload
   code lives in the downstream app. The trainer container recipes here are
   base images that compile and run a chosen downstream bin or example.

## local compose

Bootstrap-only edge:

```bash
docker compose -f deploy/compose/bootstrap-edge.compose.yaml up --build
```

Split reference fleet:

```bash
cp deploy/compose/split-fleet.env.example deploy/compose/split-fleet.env
docker compose \
  --env-file deploy/compose/split-fleet.env \
  -f deploy/compose/split-fleet.compose.yaml \
  up --build
```

The split fleet starts:

- one bootstrap/browser-edge node
- one separate reducer node
- one separate authority/validator node
- optional trainer containers under the `trainers` profile

The enterprise SSO compose stack now also starts a dedicated Redis service for
browser-edge auth session state. That is the expected HA shape for browser auth:

- single-edge or tightly controlled local setups may still use `session_state_path`
- multi-edge or load-balanced browser auth should point `session_state_backend`
  at Redis or a managed equivalent instead of a shared file
- the reference `enterprise-sso.compose.yaml` wires that Redis dependency in by
  default

## cloud

`terraform/` contains reference stacks for:

- `terraform/aws`: EC2-based split fleet
- `terraform/gcp`: Compute Engine-based split fleet

The intended operator flow is:

1. build and push the bootstrap image
2. build and push the reducer/validator images, or reuse the same bootstrap image for them
3. build and push the downstream trainer images
4. provide the container image URIs plus the config JSON through terraform vars
5. apply with one command through `cargo xtask deploy ...`

The cloud stacks deliberately keep bootstrap, reducer, validator, and trainer
hardware separate so bootstrap nodes can stay cheap and reducers can scale
independently from validators.

One important boundary remains: the reference terraform stacks do not provision
managed Redis for browser-edge auth state. If you deploy the enterprise/browser
auth surface in AWS or GCP, point `session_state_backend.url` at an external
Redis service such as ElastiCache or Memorystore.
