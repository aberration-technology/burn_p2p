# terraform

These stacks are intentionally simple reference deployments:

- `aws/`: EC2 instances
- `gcp/`: Compute Engine instances

They provision a split fleet:

- bootstrap/coherence seed nodes
- validator/authority nodes
- optional trainer nodes, including a GPU-capable trainer pool

They do **not** build images in the cloud. The expected flow is:

1. build and push the bootstrap image
2. build and push the downstream validator/trainer images
3. pass those image URIs to terraform
4. let `cargo xtask deploy aws ...` or `cargo xtask deploy gcp ...` inject the
   bootstrap and validator config JSON directly from `deploy/config/`

Example:

```bash
cargo xtask deploy aws \
  --action plan \
  --var-file deploy/terraform/aws/reference.tfvars.example \
  --bootstrap-image ghcr.io/acme/burn-p2p-bootstrap:0.21.0-pre.6 \
  --validator-image ghcr.io/acme/burn-p2p-bootstrap:0.21.0-pre.6 \
  --trainer-image ghcr.io/acme/my-burn-node:latest
```
