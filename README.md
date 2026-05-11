# echelon-operator

A Kubernetes operator that aggregates the
[kstatus](https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus)
of arbitrary resources, identified by GVK and label selector, into a single
kstatus-compatible `Ready` condition exposed on its own CRD.

The operator drives a *deployment-wave* / *run-level* mechanism alongside
[FluxCD](https://fluxcd.io/): downstream consumers gate on an `Echelon`'s
`Ready` condition to decide when one wave has settled and the next may
proceed.

## CRDs

| Kind             | Scope        | Use                                                              |
|------------------|--------------|------------------------------------------------------------------|
| `Echelon`        | Namespaced   | Aggregate within the Echelon's own namespace                     |
| `ClusterEchelon` | Cluster-wide | Aggregate across namespaces (per-member `namespaces` selectors)  |

Both CRDs live at `as-code.io/v1`. Each Echelon declares one or more named
*members* under `spec.members`; the operator aggregates the kstatus of every
resource matching each member's GVK + selector into a per-member rollup, and
combines the rollups into the owner's `Ready` condition. Map keys are
user-chosen RFC-1123 labels and serve as stable identifiers for SSA
field-ownership and `status.members[name]` lookups.

### Echelon (minimal)

```yaml
apiVersion: as-code.io/v1
kind: Echelon
metadata: { name: wave-0, namespace: flux-system }
spec:
  members:
    kustomizations:
      group: kustomize.toolkit.fluxcd.io
      kind: Kustomization
      selector: { matchLabels: { wave: "0" } }
      emptySetPolicy: NotReady
```

### ClusterEchelon (multi-member, multi-scope)

```yaml
apiVersion: as-code.io/v1
kind: ClusterEchelon
metadata: { name: platform-wave-0 }
spec:
  members:
    flux-kustomizations:
      group: kustomize.toolkit.fluxcd.io
      kind: Kustomization
      namespaces: [flux-system]
      selector: { matchLabels: { wave: "0" } }
      emptySetPolicy: NotReady
    platform-helmreleases:
      group: helm.toolkit.fluxcd.io
      kind: HelmRelease
      namespaceSelector: { matchLabels: { tier: platform } }
      selector: { matchLabels: { wave: "0" } }
      emptySetPolicy: Unknown
```

### `emptySetPolicy`

Per-member. Controls how an empty resource set is reported:

| Value      | Meaning                                                                 |
|------------|-------------------------------------------------------------------------|
| `Unknown`  | (Default) Ready=Unknown, reason=EmptySet — safest for wave gates        |
| `Ready`    | Ready=True — vacuously advance when nothing matches                     |
| `NotReady` | Ready=False — emptiness is itself a misconfiguration                    |

### Conditions

`Echelon` and `ClusterEchelon` expose three conditions:

- `Ready` — kstatus-compatible aggregate over all members
- `Reconciling` — True while the controller is wiring watchers / settling
- `Stalled` — True for non-transient structural problems (`GVKNotEstablished`, `NamespaceScopeMismatch`, `WatchSetupFailed`, `DiscoveryFailed`)

`Stalled` is independent of `Ready`. When `Stalled=True`, `Ready` reflects what
we *can* observe (typically `Unknown`) — never silently `True`.

## Architecture

| Layer                    | Package                  | Responsibility                                          |
|--------------------------|--------------------------|---------------------------------------------------------|
| Per-resource readiness   | `internal/status`        | Wraps `kstatus.Compute`, reduces resources → rollup     |
| Discovery TTL cache      | `internal/discovery`     | Resolves group+kind → GVK + scope                       |
| Dynamic watcher registry | `internal/watcher`       | Refcounted "one informer per GVK" pattern               |
| Reconciler               | `internal/controller`    | Generic pipeline shared by Echelon and ClusterEchelon   |
| Metrics                  | `internal/metrics`       | Prometheus inventory + lister-backed state collector    |

The reconciler is single-pass and idempotent. Per-stage timing histograms
(`echelon_reconcile_stage_duration_seconds`) and an idempotency check
(`echelon_status_patch_total{result=unchanged}`) make slow or churning
deployments diagnosable in production.

## Observability

Metrics are registered against the controller-runtime metrics registry; the
manager's `/metrics` endpoint exposes both the standard `controller_runtime_*`
families and the operator-specific `echelon_*` families documented in
[`PLAN.md`](./PLAN.md#metric-inventory).

A starter `ServiceMonitor` and `PrometheusRule` ship in
[`config/prometheus/`](./config/prometheus/); a sample Grafana dashboard JSON
is in [`config/grafana/`](./config/grafana/).

## Getting started

### Prerequisites

- Go 1.26.3+
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
make docker-build docker-push IMG=<registry>/echelon-operator:tag
make install                      # installs CRDs
make deploy IMG=<registry>/echelon-operator:tag
kubectl apply -k config/samples/  # sample Echelon and ClusterEchelon
```

### Watching custom resource kinds

The default install grants the operator's dynamic informers `get,list,watch`
on the FluxCD resource groups. To watch additional kinds, edit
[`config/rbac/dynamic_watch_role.yaml`](./config/rbac/dynamic_watch_role.yaml)
and re-deploy.

For restricted clusters, replace that ClusterRoleBinding with a narrower role
covering only the kinds you intend to reference from `spec.members.<name>`.

## Design

The full design — including the API surface, watcher registry semantics,
reconcile pipeline, reduction rules, metric inventory, cardinality budget, and
v1 compatibility discipline — is in [`PLAN.md`](./PLAN.md).

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
