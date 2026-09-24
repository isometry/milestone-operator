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

### `suspendPolicy`

Per-dependency. Controls whether a matched resource's `spec.suspend: true`
affects dependency readiness:

| Value      | Meaning                                                                 |
|------------|-------------------------------------------------------------------------|
| `Ignore`   | (Default) Suspension has no effect — kstatus is reported as-is         |
| `NotReady` | Any matched resource with `spec.suspend: true` forces Ready=False, reason=ResourcesSuspended |

Detection is a boolean `spec.suspend: true` on the matched resource — every
reconciling Flux kind (Kustomization, HelmRelease, sources, …) exposes it.
Under `NotReady`, `status.notReadyResources` can list an otherwise-`Current`
resource with `reason: Suspended`.

### Conditions

`Milestone` and `ClusterMilestone` expose two kstatus-compatible
conditions (`Reconciling` is reserved for a future two-phase status
patch and is not emitted today):

- `Ready` — aggregate over all dependencies
- `Stalled` — True for non-transient structural problems
  (`GVKNotEstablished`, `NamespaceScopeMismatch`, `WatchSetupFailed`,
  `DiscoveryFailed`)

A freshly-created object is served with `status.observedGeneration: -1`
(CRD schema default), so kstatus-based health checks — e.g. a FluxCD
Kustomization with `wait: true` — report it `InProgress` until the
first reconcile, never prematurely healthy.

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

## CLI (milestonectl)

`milestonectl` inspects Milestones and ClusterMilestones from the command
line, in the style of the Flux CLI. The recorded `status` is deliberately
lossy (per-dependency counts, at most 50 not-ready resources, no healthy
members), so the CLI can also answer "what is actually in `wave-0`?" and
"which milestones gate on this HelmRelease?" by evaluating membership live.

Build and install:

```sh
make build-cli          # bin/milestonectl + kubectl plugin symlinks
install -m 0755 bin/milestonectl /usr/local/bin/kubectl-milestone
# kubectl >= 1.26 plugin tab-completion looks for this name on PATH:
ln -s kubectl-milestone /usr/local/bin/kubectl_complete-milestone
```

The same binary runs as `milestonectl ...` or `kubectl milestone ...` and
accepts the usual kubeconfig flags (`--context`, `-n`, `--as`, ...).
`milestonectl completion bash|zsh|fish|powershell` emits shell completions.

| Command                         | Shows                                                              |
|---------------------------------|--------------------------------------------------------------------|
| `get milestones\|clustermilestones\|all` | The status the operator last **recorded** (`-A`, `--status-selector`, `-w`, `-o table\|wide\|json\|yaml`) |
| `tree milestone\|clustermilestone NAME`   | Every member of every dependency, **evaluated live** (`--not-ready`, `-o tree\|json\|yaml`) |
| `trace TYPE/NAME`               | Every milestone dependency that includes the object, and its live state |
| `version`                       | Client version and the operator image(s) found in the cluster (`--client` skips the lookup) |

```sh
# Every Milestone and ClusterMilestone, cluster-wide
milestonectl get all -A

# Only what is not ready, updating as it changes
milestonectl get mile -A --status-selector ready=False --watch

# What is in wave-1, and what is holding it back?
milestonectl tree milestone wave-1 -n flux-system
milestonectl tree cmile platform --not-ready

# Which milestones gate on this object?
milestonectl trace ks/broken -n flux-system
milestonectl trace deploy/web -n apps

milestonectl version
```

`tree` output (colour is used only on a terminal; `--no-color` and
`NO_COLOR` disable it):

```
Milestone/flux-system/wave-1  Ready=False  DependenciesNotReady  (evaluated 42s ago, stale: generation 3 not yet observed)
└── ✗ kustomizations  Kustomization.kustomize.toolkit.fluxcd.io/v1  wave=1  1/2  ResourcesNotReady
    ├── ✔ flux-system/apps     Current
    └── ✗ flux-system/broken   Failed  BuildFailed: build failed
```

Glyphs: `✔` ready, `✗` not ready, `⏸` suspended, `◌` in progress, `?`
unknown. A dependency the operator could not evaluate (`GVKNotEstablished`,
`NamespaceScopeMismatch`, ...) is shown inline with its reason rather than
aborting the tree.

`trace` prints the object's live kstatus followed by one row per matching
`(owner, dependency)`:

```
✗ Kustomization.kustomize.toolkit.fluxcd.io/flux-system/broken  Failed  BuildFailed: build failed

OWNER                          DEPENDENCY                OWNER READY     OWNER REASON           BLOCKING
Milestone/flux-system/wave-1   kustomizations            False (stale)   DependenciesNotReady   yes
ClusterMilestone/platform      platform-kustomizations   True            AllDependenciesReady   yes
```

`get` never evaluates anything: it prints what is in `status`, and marks
READY with `(stale)` while `observedGeneration` lags `metadata.generation`.
`tree` and `trace` list the target resources with your credentials and run
the operator's own normalisation and kstatus reduction (the shared
`internal/membership` package), so they reflect what the operator would
record right now. When the operator has observed the current generation but
a dependency's live `ready`/`reason` differs from the recorded one, `tree`
adds a `⚠ live differs from recorded` line; while the generation is stale
the difference is expected and only the `(stale)` note is shown. Resource
counts are not compared, to avoid false alarms from informer lag.

RBAC required by the CLI user (the operator's service account is not used):

- `get`/`list`/`watch` on `milestones` and `clustermilestones`
  (`milestone.as-code.io`)
- `list` on each target kind referenced by the dependencies you `tree`
  (and `get` on the object you `trace`)
- `list` on `namespaces`, for `ClusterMilestone` `namespaceSelector`
  dependencies (`tree` and `trace`) and for `--namespace` completion
- `list` on `deployments` across namespaces for `version` (optional; a
  failure is reported on the `operator:` line and does not fail the command)

A 403 on a target list is reported as `cannot check: forbidden (your
credentials)` on that dependency, distinct from the operator's own
`ListFailed`, so a gap in your RBAC is not mistaken for an operator fault.

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
