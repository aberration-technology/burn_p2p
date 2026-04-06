# deployment

This directory holds the deployment source of truth:

- `containers/`: role-specific container build recipes
- `config/`: container-friendly bootstrap and validator configs
- `compose/`: local and single-host reference stacks
- `terraform/`: cloud reference stacks for AWS and GCP

The deployment split is intentional:

- bootstrap/coherence seed nodes are cheap CPU/network boxes
- validator/authority nodes are separate and CPU-oriented
- trainer nodes are separate again and can use CPU or GPU-specific images

Two boundaries matter:

1. `burn-p2p-bootstrap` is a real publishable binary, so bootstrap and validator
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
- one separate authority/validator node
- optional trainer containers under the `trainers` profile

## cloud

`terraform/` contains reference stacks for:

- `terraform/aws`: EC2-based split fleet
- `terraform/gcp`: Compute Engine-based split fleet

The intended operator flow is:

1. build and push the bootstrap image
2. build and push the downstream validator/trainer images
3. provide the container image URIs plus the config JSON through terraform vars
4. apply with one command through `cargo xtask deploy ...`

The cloud stacks deliberately keep bootstrap and validator/trainer hardware
separate so bootstrap nodes can stay cheap.
