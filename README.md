# milestone-operator

A Kubernetes operator that aggregates the
[kstatus](https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus)
of arbitrary resources, identified by GVK and label selector, into a single
kstatus-compatible `Ready` condition exposed on its own CRD.

The operator drives a *deployment-wave* / *milestone gating* mechanism
alongside [FluxCD](https://fluxcd.io/): downstream consumers gate on a
`Milestone`'s `Ready` condition to decide when one stage has settled and
the next may proceed.

## CRDs

| Kind               | Scope        | Use                                                                |
|--------------------|--------------|--------------------------------------------------------------------|
| `Milestone`        | Namespaced   | Aggregate within the Milestone's own namespace                     |
| `ClusterMilestone` | Cluster-wide | Aggregate across namespaces (per-dependency `namespaces` selectors)|

Both CRDs live at `milestone.as-code.io/v1`. Each Milestone declares one
or more named *dependencies* under `spec.dependsOn`; the operator
aggregates the kstatus of every resource matching each dependency's target
GVK + selector into a per-dependency rollup, and combines the rollups into
the owner's `Ready` condition. `name` is the listmap key — a kebab-case
RFC-1123 label — and serves as the stable identifier surfaced in
`status.dependsOn[].name`, condition messages, log fields, and the
`dependency` metric label.

### Milestone (minimal)

```yaml
apiVersion: milestone.as-code.io/v1
kind: Milestone
metadata: { name: wave-0, namespace: flux-system }
spec:
  dependsOn:
    - name: kustomizations
      emptySetPolicy: NotReady
      target:
        group: kustomize.toolkit.fluxcd.io
        kind: Kustomization
        selector: { matchLabels: { wave: "0" } }
```

### ClusterMilestone (multi-dependency, multi-scope)

```yaml
apiVersion: milestone.as-code.io/v1
kind: ClusterMilestone
metadata: { name: platform-wave-0 }
spec:
  dependsOn:
    - name: flux-kustomizations
      emptySetPolicy: NotReady
      target:
        group: kustomize.toolkit.fluxcd.io
        kind: Kustomization
        namespaces: [flux-system]
        selector: { matchLabels: { wave: "0" } }
    - name: platform-helmreleases
      target:
        group: helm.toolkit.fluxcd.io
        kind: HelmRelease
        namespaceSelector: { matchLabels: { tier: platform } }
        selector: { matchLabels: { wave: "0" } }
```

### `emptySetPolicy`

Per-dependency. Controls how an empty resource set is reported:

| Value      | Meaning                                                                 |
|------------|-------------------------------------------------------------------------|
| `Unknown`  | (Default) Ready=Unknown, reason=EmptySet — safest for wave gates        |
| `Ready`    | Ready=True — vacuously advance when nothing matches                     |
| `NotReady` | Ready=False — emptiness is itself a misconfiguration                    |

### Conditions

`Milestone` and `ClusterMilestone` expose three conditions:

- `Ready` — kstatus-compatible aggregate over all dependencies
- `Reconciling` — True while the controller is wiring watchers / settling
- `Stalled` — True for non-transient structural problems
  (`GVKNotEstablished`, `NamespaceScopeMismatch`, `WatchSetupFailed`,
  `DiscoveryFailed`)

`Stalled` is independent of `Ready`. When `Stalled=True`, `Ready`
reflects what we *can* observe (typically `Unknown`) — never silently
`True`.

## Architecture

| Layer                    | Package                  | Responsibility                                                |
|--------------------------|--------------------------|---------------------------------------------------------------|
| Per-resource readiness   | `internal/status`        | Wraps `kstatus.Compute`, reduces resources → rollup           |
| Discovery TTL cache      | `internal/discovery`     | Resolves group+kind → GVK + scope                             |
| Dynamic watcher registry | `internal/watcher`       | Refcounted "one informer per GVK" pattern                     |
| Reconciler               | `internal/controller`    | Generic pipeline shared by Milestone and ClusterMilestone     |
| Metrics                  | `internal/metrics`       | Prometheus inventory + lister-backed state collector          |

The reconciler is single-pass and idempotent. Per-stage timing histograms
(`milestone_reconcile_stage_duration_seconds`) and an idempotency check
(`milestone_status_patch_total{result=unchanged}`) make slow or churning
deployments diagnosable in production.

## Observability

Metrics are registered against the controller-runtime metrics registry; the
manager's `/metrics` endpoint exposes both the standard
`controller_runtime_*` families and the operator-specific `milestone_*`
families documented in [`PLAN.md`](./PLAN.md#metric-inventory).

A starter `ServiceMonitor` and `PrometheusRule` ship in
[`config/prometheus/`](./config/prometheus/); a sample Grafana dashboard
JSON is in [`config/grafana/`](./config/grafana/).

### Metrics endpoint posture

The default install exposes `/metrics` over plain HTTP on `:8080`. The
endpoint is still gated by bearer-token authentication and authorization
via controller-runtime's `WithAuthenticationAndAuthorization` filter
whenever `--metrics-secure=true`; that gate is independent of TLS.

To enable TLS with cert-manager-managed certificates:

1. Install cert-manager in the target cluster.
2. In [`config/default/kustomization.yaml`](./config/default/kustomization.yaml),
   uncomment the `cert_metrics_manager_patch.yaml` and
   `metrics_service_tls_patch.yaml` patches.
3. In [`config/prometheus/kustomization.yaml`](./config/prometheus/kustomization.yaml),
   uncomment the `monitor_tls_patch.yaml` entry under `patches`.
4. Re-deploy. The manager will then listen on `:8443` and the
   `ServiceMonitor` will scrape HTTPS with mTLS.

## Supply chain

Tagged releases publish a keyless-signed (Sigstore) container image and OCI
Helm chart, each with SLSA build provenance; the image also carries an SBOM
attestation. The image index additionally embeds unsigned BuildKit
SBOM/provenance attestations that survive plain index copies (`skopeo`,
`crane`); the signed artifacts are OCI referrers and need referrers-aware
mirroring. [`docs/verification.md`](./docs/verification.md) documents how
to verify them — `gh attestation verify`, `cosign`, and `helm --verify` —
plus the artifact layout and mirroring guidance, and
[`deploy/policies/`](./deploy/policies/) ships ready-to-apply Flux and
Kyverno policies to enforce verification at runtime.

## Getting started

### Prerequisites

- Go 1.26+
- Docker 17.03+
- kubectl v1.11.3+
- A Kubernetes cluster (Kubernetes v1.27+ recommended)

### Run unit tests

```sh
go test ./internal/...
```

### Run envtest integration tests

```sh
make setup-envtest
KUBEBUILDER_ASSETS="$(./bin/setup-envtest use --bin-dir ./bin -p path)" \
  go test ./internal/controller/... -run TestEnvtest
```

### Build and deploy

```sh
make docker-build docker-push IMG=<registry>/milestone-operator:tag
make install                      # installs CRDs
make deploy IMG=<registry>/milestone-operator:tag
kubectl apply -k config/samples/  # sample Milestone and ClusterMilestone
```

### Watching custom resource kinds

The operator's dynamic informers `get,list,watch` whatever GVK a
`spec.dependsOn[].target` names. Because those kinds are not known until a
Milestone is applied, read access is granted through an **aggregated
ClusterRole**. The default install
([`config/rbac/dynamic_watch_role.yaml`](./config/rbac/dynamic_watch_role.yaml))
ships:

- `manager-dynamic-watch-role` — the aggregation umbrella, bound to the
  controller's ServiceAccount. Its rules are owned by the
  kube-controller-manager; **do not edit them directly**.
- `manager-dynamic-watch-flux` — a member granting the FluxCD resource groups,
  labelled so its rules union into the umbrella.

To let the operator watch **any other kind**, ship a ClusterRole that carries
the aggregation label — no binding of your own is needed, and you never touch
the umbrella:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: milestone-watch-configmaps
  labels:
    milestone.as-code.io/aggregate-to-dynamic-watch: "true"
rules:
  - apiGroups: [""]
    resources: [configmaps]
    verbs: [get, list, watch]
```

The kube-controller-manager unions every labelled role into
`manager-dynamic-watch-role`. If a target kind has no matching grant, the
informer's `list` is `forbidden`, the dependency stays `Unknown`, and the
operator logs the watch error (it does not crash). For a restricted cluster,
drop the `manager-dynamic-watch-flux` member and ship only the narrow
ClusterRoles for the kinds you actually reference.

### Resource baseline

The shipped Deployment requests `128Mi` memory and limits to `512Mi`. That
is a starting point for a fleet of roughly 500 Milestones; observe
`milestone_reconcile_stage_duration_seconds` and container working-set
size before tuning for larger fleets.

## Design

The full design — including the API surface, watcher registry semantics,
reconcile pipeline, reduction rules, metric inventory, cardinality budget,
and v1 compatibility discipline — is in [`PLAN.md`](./PLAN.md).

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
