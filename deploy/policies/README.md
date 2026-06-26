# Example verification policies

These manifests enforce — at runtime, cluster-side — the integrity
guarantees described in [`docs/verification.md`](../../docs/verification.md):
that the `milestone-operator` image and chart were built and signed by this
repository's release workflow.

> **They are examples, not part of the operator.** The Helm chart in
> `deploy/charts/milestone-operator` does **not** install them and does not
> depend on any admission stack. Apply them yourself, in clusters that run
> Flux and/or Kyverno, after adapting the namespace/targeting to your setup.

| File | Enforces | Requires |
|------|----------|----------|
| [`flux-ocirepository-verify.yaml`](./flux-ocirepository-verify.yaml) | Chart signature verified before reconcile | Flux (source-controller, helm-controller) |
| [`kyverno-verify-image.yaml`](./kyverno-verify-image.yaml) | Image signature + SLSA provenance verified at admission | Kyverno |

Both pin the same trust anchor — OIDC issuer
`https://token.actions.githubusercontent.com` and the publishing workflow
identity. If you fork or rename the repo, update those values or the
policies will (correctly) reject your artifacts.
