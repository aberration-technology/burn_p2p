# terraform

These stacks are intentionally simple reference deployments:

- `aws/`: EC2 instances
- `gcp/`: Compute Engine instances

They provision a split fleet:

- bootstrap/coherence seed nodes
- reducer nodes
- validator/authority nodes
- optional trainer nodes, including a GPU-capable trainer pool

They do not provision managed Redis for browser-edge auth session state. If you
run the browser/enterprise auth surface in HA mode, point
`session_state_backend.url` at an external Redis service such as ElastiCache or
Memorystore instead of relying on the single-host file backend.

They do **not** build images in the cloud. The expected flow is:

1. build and push the bootstrap image
2. build and push the reducer/validator images, or reuse the same bootstrap image
3. build and push the downstream trainer images
4. pass those image URIs to terraform
5. let `cargo xtask deploy aws ...` or `cargo xtask deploy gcp ...` inject the
   bootstrap, reducer, and validator config JSON directly from `deploy/config/`

Example:

```bash
cargo xtask deploy aws \
  --action plan \
  --var-file deploy/terraform/aws/reference.tfvars.example \
  --bootstrap-image ghcr.io/acme/burn-p2p-bootstrap:0.21.0-pre.7 \
  --reducer-image ghcr.io/acme/burn-p2p-bootstrap:0.21.0-pre.7 \
  --validator-image ghcr.io/acme/burn-p2p-bootstrap:0.21.0-pre.7 \
  --trainer-image ghcr.io/acme/my-burn-node:latest
```
