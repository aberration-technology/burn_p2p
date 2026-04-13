# bootstrap and cloud deployment pattern

this is the generic deployment starting point for downstream repos that want a
split `burn_p2p` fleet without copying dragon-specific infra.

## what is stable vs reference here

stable runtime/library surfaces:

- `burn_p2p_bootstrap`
- the config schema accepted by the daemon
- `cargo xtask deploy compose`
- `cargo xtask deploy aws`
- `cargo xtask deploy gcp`

reference assets you are expected to copy and adapt:

- `deploy/config/reference-bootstrap.json`
- `deploy/config/reference-validator.json`
- `deploy/config/reference-reducer.json`
- `deploy/compose/*.compose.yaml`
- `deploy/terraform/aws/`
- `deploy/terraform/gcp/`

those reference assets are intentionally generic, but they are still templates,
not a semver compatibility guarantee.

## recommended downstream starting point

1. keep the split-fleet topology
   - public bootstrap/coherence seeds
   - private reducer and validator hosts
   - private downstream trainer hosts
2. start from the reference config json files in `deploy/config/`
3. adapt only the downstream-owned parts
   - container images
   - auth provider values
   - domain names
   - object-store settings
   - trainer/runtime workload config

## local reference flow

for a local fleet:

```bash
cp deploy/compose/split-fleet.env.example deploy/compose/split-fleet.env
cargo xtask deploy compose --stack split-fleet --env-file deploy/compose/split-fleet.env --action up
```

for local browser-edge or operator-state support, reuse the packaged backing
services described in [operator-runbook.md](../operator-runbook.md).

## cloud reference flow

for aws:

```bash
cargo xtask deploy aws \
  --action plan \
  --var-file deploy/terraform/aws/reference.tfvars.example \
  --bootstrap-image ghcr.io/acme/burn-p2p-bootstrap:latest \
  --reducer-image ghcr.io/acme/burn-p2p-bootstrap:latest \
  --validator-image ghcr.io/acme/burn-p2p-bootstrap:latest \
  --trainer-image ghcr.io/acme/your-trainer:latest
```

for gcp:

```bash
cargo xtask deploy gcp \
  --action plan \
  --var-file deploy/terraform/gcp/reference.tfvars.example \
  --bootstrap-image ghcr.io/acme/burn-p2p-bootstrap:latest \
  --reducer-image ghcr.io/acme/burn-p2p-bootstrap:latest \
  --validator-image ghcr.io/acme/burn-p2p-bootstrap:latest \
  --trainer-image ghcr.io/acme/your-trainer:latest
```

## operator-state and publication defaults

the reference stacks assume:

- postgres can back shared operator replay state
- redis can back multi-edge browser auth session state
- artifact publication can use hot local storage plus warm s3-compatible object
  storage

the generic environment placeholders and reference values already live in:

- `deploy/compose/split-fleet.env.example`
- `deploy/terraform/README.md`
- [operator-runbook.md](../operator-runbook.md)

downstreams should keep their own cloud account wiring and secret management,
but they should not need to invent the fleet shape from scratch.
