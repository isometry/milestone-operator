# Echelon Operator — Design

## Context

Build a new Kubernetes operator (`echelon-operator`) that monitors and aggregates the
[kstatus](https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus) of arbitrary
resources identified by GVK and label selector, surfacing a kstatus-compatible `Ready`
condition on its own CRD. The operator exists to support a *deployment-wave* / *run-level*
mechanism running alongside FluxCD: downstream consumers gate on an `Echelon`'s `Ready`
condition to decide when a wave has settled and the next wave may proceed.

Two CRDs:

- `Echelon` (namespaced) — targets resources within its own namespace.
- `ClusterEchelon` (cluster-scoped) — targets resources across namespaces, with per-target
  namespace selection.

Built with operator-sdk **v1.42.2** (Kubebuilder v4) and Go **1.26.3**. New repository,
no existing code.

## Decisions (locked via brainstorming)

| Topic | Decision |
| --- | --- |
| Spec shape | `spec.targets[]` from day one, no per-target cap (multi-GVK supported) |
| Empty-set semantics | `emptySetPolicy: Unknown\|Ready\|NotReady` **per target** (default `Unknown`) |
| API ident | `as-code.io/v1` (commit to strong interfaces from day one; iterate locally pre-release) |
| `status.members` verbosity | Only non-Current members (capped, e.g. 50) + aggregate `summary` counters; `truncated` flag |
| Missing CRD handling | `Stalled=True, reason=GVKNotEstablished` + watch `apiextensions.k8s.io/v1.CustomResourceDefinition` to wake on `Established=True` |
| ClusterEchelon scoping | `namespaces` and `namespaceSelector` are **per-target** and mutually exclusive (CRD CEL validation) |
| Watcher architecture | Shared, refcounted registry; one cluster-scoped dynamic informer per GVK |
| Per-resource readiness | `sigs.k8s.io/cli-utils/pkg/kstatus/status.Compute` (strict — no condition-fallback) |
| Conditions | `Ready`, `Reconciling`, `Stalled` |
| Operator scope | Single cluster-scoped deployment serves both CRDs |
| Test strategy (MVP) | Unit + envtest (ginkgo/gomega). Defer kwok / real-cluster e2e |
| Observability | First-class Prometheus metrics from day one (custom collector + pipeline counters/histograms); ServiceMonitor + sample alert rules shipped in `config/` |

## API surface

### Shared types (`api/v1/shared_types.go`)

```go
// +kubebuilder:validation:Enum=Unknown;Ready;NotReady
type EmptySetPolicy string

type TargetSpec struct {
    Group          string                `json:"group,omitempty"`
    Version        string                `json:"version,omitempty"` // empty → resolved via discovery
    Kind           string                `json:"kind"`
    Selector       *metav1.LabelSelector `json:"selector,omitempty"` // standard Kubernetes idiom (cf. Deployment, NetworkPolicy)
    EmptySetPolicy EmptySetPolicy        `json:"emptySetPolicy,omitempty"` // default Unknown
    // Future-additive: a new optional `Filter`/`CEL` field can be added without breaking v1.
}

type ClusterTargetSpec struct {
    TargetSpec        `json:",inline"`
    Namespaces        []string              `json:"namespaces,omitempty"`
    NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`
}
// CRD-level CEL: !(has(self.namespaces) && has(self.namespaceSelector))

type Summary struct {
    Total       int `json:"total"`
    Current     int `json:"current"`
    InProgress  int `json:"inProgress"`
    Failed      int `json:"failed"`
    NotFound    int `json:"notFound"`
    Terminating int `json:"terminating"`
    Unknown     int `json:"unknown"`
}

type TargetRollup struct {
    Group   string `json:"group,omitempty"`
    Version string `json:"version,omitempty"`
    Kind    string `json:"kind"`
    Ready   metav1.ConditionStatus `json:"ready"`
    Reason  string  `json:"reason,omitempty"`
    Summary Summary `json:"summary"`
}

type MemberStatus struct {
    Group     string `json:"group,omitempty"`
    Version   string `json:"version"`
    Kind      string `json:"kind"`
    Namespace string `json:"namespace,omitempty"`
    Name      string `json:"name"`
    Status    string `json:"status"`            // kstatus enum: Current/InProgress/Failed/NotFound/Terminating/Unknown
    Reason    string `json:"reason,omitempty"`
    Message   string `json:"message,omitempty"`
}

type EchelonStatusBase struct {
    ObservedGeneration int64              `json:"observedGeneration,omitempty"`
    Conditions         []metav1.Condition `json:"conditions,omitempty"`
    Summary            Summary            `json:"summary,omitempty"`
    Targets            []TargetRollup     `json:"targets,omitempty"`
    NotReadyMembers    []MemberStatus     `json:"notReadyMembers,omitempty"`
    Truncated          bool               `json:"truncated,omitempty"`
    LastEvaluatedTime  metav1.Time        `json:"lastEvaluatedTime,omitempty"`
}
```

### `Echelon` (namespaced)

```yaml
apiVersion: as-code.io/v1
kind: Echelon
metadata: { name: wave-0, namespace: flux-system }
spec:
  targets:
    - kind: Kustomization
      group: kustomize.toolkit.fluxcd.io
      selector: { matchLabels: { wave: "0" } }
      emptySetPolicy: NotReady
```

### `ClusterEchelon` (cluster-scoped)

```yaml
apiVersion: as-code.io/v1
kind: ClusterEchelon
metadata: { name: platform-wave-0 }
spec:
  targets:
    - kind: Kustomization
      group: kustomize.toolkit.fluxcd.io
      namespaces: [flux-system]
      selector: { matchLabels: { wave: "0" } }
      emptySetPolicy: NotReady
    - kind: HelmRelease
      group: helm.toolkit.fluxcd.io
      namespaceSelector: { matchLabels: { tier: platform } }
      selector: { matchLabels: { wave: "0" } }
      emptySetPolicy: Unknown
```

### Reasons vocabulary (locked)

`AllMembersReady`, `AllTargetsReady`, `MembersNotReady`, `TargetsNotReady`,
`MembersInProgress`, `MembersUnknown`, `TargetsInProgress`, `EmptySet`,
`GVKNotEstablished`, `NamespaceScopeMismatch`, `DiscoveryFailed`, `WatchSetupFailed`,
`Reconciling`.

## Architecture

### Module layout

```
echelon-operator/
├── PROJECT, Makefile, Dockerfile, go.mod
├── cmd/main.go                          # manager bootstrap, registry init, both controllers + CRD watcher
├── api/v1/
│   ├── groupversion_info.go
│   ├── shared_types.go
│   ├── echelon_types.go
│   ├── clusterechelon_types.go
│   └── zz_generated.deepcopy.go
├── internal/
│   ├── controller/
│   │   ├── reconciler.go                # generic pipeline; owns workflow
│   │   ├── owner_adapter.go             # interface bridging Echelon / ClusterEchelon
│   │   ├── echelon_controller.go        # thin wiring (For + Watches)
│   │   ├── clusterechelon_controller.go # thin wiring
│   │   └── crd_watcher.go               # apiextensions watcher → enqueue stalled owners
│   ├── watcher/
│   │   ├── registry.go                  # WatcherRegistry impl, refcount, lifecycle
│   │   ├── subscriber_index.go          # GVK → []Subscriber match index
│   │   └── handler.go                   # dynamic informer EventHandler → enqueue
│   ├── discovery/resolver.go            # group+kind → version+scope, TTL cache
│   ├── status/
│   │   ├── kstatus.go                   # wrapper around cli-utils kstatus.Compute
│   │   └── reducer.go                   # per-target + Echelon-level reductions
│   └── metrics/
│       ├── metrics.go                   # collectors, registration on controller-runtime registry
│       ├── pipeline.go                  # stage timing + counters used by reconciler
│       └── collector.go                 # custom collector: lister-backed gauges for status/targets
├── config/                              # kubebuilder kustomize (CRDs incl. CEL, RBAC, manager, samples)
│   ├── prometheus/                      # ServiceMonitor + PrometheusRule (alerts)
│   └── grafana/                         # sample dashboard JSON
└── test/
    ├── envtest/                         # integration suites
    └── helpers/                         # CRD fixtures, member factories
```

### WatcherRegistry

- One `*dynamicinformer.DynamicSharedInformerFactory` cluster-scoped per *unique* GVK
  (covers both Echelon and ClusterEchelon needs in a single cache; per-namespace
  filtering at dispatch time).
- Refcount keyed by `OwnerKey{ Kind, Namespace, Name } → set[GVK]`. Idempotent
  re-Subscribe on every reconcile; Unsubscribe drops to zero → informer stop + cache
  release.
- One `EventHandler` per informer; on Add/Update/Delete it consults the
  `SubscriberIndex` to enqueue every Echelon whose label/namespace selectors match the
  changed object.
- Mutex protects subscription churn; event dispatch is read-heavy (RWMutex).

```go
type OwnerKey struct{ Kind, Namespace, Name string }

type Subscriber struct {
    Owner            OwnerKey
    Selector         labels.Selector
    NamespaceMatcher func(string) bool   // nil ⇒ all namespaces
}

type WatcherRegistry interface {
    Subscribe(owner OwnerKey, gvk schema.GroupVersionKind, sub Subscriber) error
    Unsubscribe(owner OwnerKey, gvk schema.GroupVersionKind)
    UnsubscribeAll(owner OwnerKey)
    List(gvk schema.GroupVersionKind) ([]*unstructured.Unstructured, error)
    Subscribers(gvk schema.GroupVersionKind, obj client.Object) []OwnerKey
}
```

### DiscoveryResolver

- Wraps `discovery.DiscoveryClient` with a TTL cache (e.g. 60s).
- Resolves `(group, kind, optional version)` → `(GroupVersionKind, scope)`.
- On miss, returns a typed `ErrGVKNotEstablished` so the reconciler can map to the
  `Stalled` condition without sniffing string errors.

### CRD watcher

A second tiny controller (`crd_watcher.go`) `For()`'s `CustomResourceDefinition`. On any
`Established=True` transition it queries the registry/index for owners stalled on that
group/kind and enqueues them. No polling.

### OwnerAdapter

```go
type NormalizedTarget struct {
    GVK              schema.GroupVersionKind
    Scope            apimeta.RESTScopeName
    Selector         labels.Selector
    NamespaceMatcher func(string) bool   // nil ⇒ all namespaces in scope
    EmptySetPolicy   v1.EmptySetPolicy
}

type OwnerAdapter interface {
    Object() client.Object
    OwnerKey() OwnerKey
    Targets(ctx context.Context, dr discovery.Resolver) ([]NormalizedTarget, []TargetError)
    PatchStatus(ctx context.Context, c client.Client, status v1.EchelonStatusBase) error
}
```

Two thin implementations (one per CRD) feed the same `Reconciler.Reconcile`.

## Metrics

All metrics are registered against the controller-runtime metrics registry
(`sigs.k8s.io/controller-runtime/pkg/metrics.Registry`) so they appear on the
manager's `/metrics` endpoint alongside the standard `controller_runtime_*`,
`workqueue_*`, `rest_client_*`, Go runtime, and process metrics. Two emission
patterns:

- **Custom collector (lister-backed)** for object-state metrics — collector
  walks the Echelon / ClusterEchelon caches at scrape time, emitting current
  truth. No staleness, no leaks on deletion, no per-reconcile gauge bookkeeping.
- **Direct counters / histograms / gauges** for pipeline events — updated
  inline by the reconciler and the `WatcherRegistry`.

### Metric inventory

All metrics use the `echelon_` prefix. Cardinality notes inline.

**Object-state (custom collector, scraped from cache):**

| Metric | Type | Labels | Notes |
| --- | --- | --- | --- |
| `echelon_status_condition` | gauge (1/0) | `owner_kind, namespace, name, type, status` | `type ∈ {Ready,Reconciling,Stalled}`, `status ∈ {True,False,Unknown}`. kube-state-metrics convention; one series active per (object, type). |
| `echelon_observed_generation` | gauge | `owner_kind, namespace, name` | Detect stuck reconciles vs `metadata.generation`. |
| `echelon_target_members` | gauge | `owner_kind, namespace, name, target_group, target_kind, status` | `status ∈ {current,inProgress,failed,notFound,terminating,unknown,total}`. Bounded by Echelons × targets × 7. |
| `echelon_target_ready` | gauge (1/0/-1) | `owner_kind, namespace, name, target_group, target_kind` | Per-target rollup readiness. `-1` = Unknown for True/False/Unknown encoding. |
| `echelon_last_evaluated_timestamp_seconds` | gauge | `owner_kind, namespace, name` | Detect frozen reconcile loops. |

**Watcher / registry (in-line updates):**

| Metric | Type | Labels | Notes |
| --- | --- | --- | --- |
| `echelon_informers` | gauge | `group, version, kind` | Active dynamic informers. Bounded by distinct GVKs. |
| `echelon_subscribers` | gauge | `group, version, kind` | Refcount per GVK. |
| `echelon_informer_events_total` | counter | `group, version, kind, event` | `event ∈ {add,update,delete}`. |
| `echelon_subscribe_total` | counter | `group, version, kind, result` | `result ∈ {ok,error}`. |
| `echelon_unsubscribe_total` | counter | `group, version, kind` | |
| `echelon_event_dispatch_duration_seconds` | histogram | `group, version, kind` | Time from event receipt to enqueue completion. |

**Discovery:**

| Metric | Type | Labels | Notes |
| --- | --- | --- | --- |
| `echelon_discovery_resolve_total` | counter | `result` | `result ∈ {hit,miss,not_established,error}`. |
| `echelon_discovery_cache_size` | gauge | — | |

**Reconcile pipeline (in addition to controller-runtime defaults):**

| Metric | Type | Labels | Notes |
| --- | --- | --- | --- |
| `echelon_reconcile_stage_duration_seconds` | histogram | `controller, stage` | `stage ∈ {discovery,subscriptions,list,compute,reduce,patch}`. Surfaces slow stages without per-object cardinality. |
| `echelon_status_patch_total` | counter | `controller, result` | `result ∈ {changed,unchanged,error}`. Verifies idempotency in production. |
| `echelon_target_resolve_errors_total` | counter | `controller, reason` | `reason ∈ {GVKNotEstablished,DiscoveryFailed,NamespaceScopeMismatch,WatchSetupFailed}`. |

**CRD watcher:**

| Metric | Type | Labels | Notes |
| --- | --- | --- | --- |
| `echelon_crd_established_events_total` | counter | `group, kind` | |
| `echelon_owners_woken_total` | counter | `reason` | `reason ∈ {crd_established,…}`. |

### Cardinality budget

Worst case for the collector: `Echelons × targets × 7 status buckets`. With
typical deployment-wave usage (≤20 Echelons × ≤10 targets × 7 = 1,400 series)
this is well within Prometheus comfort. No high-cardinality labels (no member
names, no resource UIDs, no namespaces from the watched resources themselves).

### Shipped artefacts

- `config/prometheus/service_monitor.yaml` — opinionated `ServiceMonitor` for
  the prometheus-operator stack; documented kustomize patch to disable.
- `config/prometheus/alerts.yaml` — `PrometheusRule` with starter alerts:
  `EchelonStuckReconciling` (Reconciling=True for >10m),
  `EchelonStalled` (Stalled=True for >5m),
  `EchelonRegistryLeak` (`echelon_subscribers - sum(echelon_informers)` skew),
  `EchelonReconcileErrors` (controller-runtime error rate >0 for 5m).
- `config/grafana/echelon-dashboard.json` — single dashboard panelling the
  collector metrics (overview, per-Echelon drill-down, registry health,
  pipeline latency).

### Test coverage

Unit tests for the custom collector (fake client + table-driven scrape
assertions). One envtest scenario verifies metrics surface on `/metrics`
end-to-end (create Echelon, mutate members, assert metric values via
`prometheus/client_golang/prometheus/testutil`).

## Reconcile pipeline (per object)

```
1. Fetch; if deletionTimestamp → run finalizer (UnsubscribeAll), remove finalizer, return.
2. Ensure finalizer present.
3. Begin reconcile (Reconciling=True; deferred Reconciling=False on success path).
4. Adapter.Targets() — discovery resolution; collect per-target errors as TargetError so the
   reconcile continues for the resolvable subset.
5. Reconcile subscriptions:
     desired       := { gvk : NormalizedTarget }
     toSubscribe   := desired \ current
     toUnsubscribe := current \ desired
6. For each resolvable target:
     a. registry.List(gvk)
     b. Filter: NamespaceMatcher → Selector
     c. status/kstatus.Compute per member (strict)
     d. status/reducer.Target(rollup, summary) applying emptySetPolicy
7. status/reducer.Echelon → conditions {Ready, Reconciling=false, Stalled}
8. Build EchelonStatusBase; cap NotReadyMembers at 50, set Truncated.
9. Adapter.PatchStatus — server-side apply with stable field manager; skip when hash
   unchanged to avoid resourceVersion churn (echelon_status_patch_total{result}).
10. Return; requeue only when Stalled with retryable reason and budget remaining.
```

Each numbered stage from steps 4–9 is wrapped by `metrics.ObserveStage(ctx, "stage")`
emitting `echelon_reconcile_stage_duration_seconds{stage}`. Per-target resolution
errors increment `echelon_target_resolve_errors_total{reason}`. The custom collector
walks the Echelon and ClusterEchelon caches at scrape time — independent of the
reconcile loop — so object-state metrics never go stale on operator restart.

### Reduction rules

Per-target:

```
total == 0                                  → apply emptySetPolicy (reason=EmptySet)
failed > 0  || notFound > 0                 → False, MembersNotReady
inProgress > 0 || terminating > 0 || unknown > 0
                                            → Unknown, MembersInProgress
current == total                            → True,  AllMembersReady
```

Echelon (rollup over per-target):

```
all True   → True,    AllTargetsReady
any False  → False,   TargetsNotReady (message lists offending kinds)
otherwise  → Unknown, TargetsInProgress
```

`Stalled` is independent of `Ready`. When set, `Ready` reflects what we *can* observe
(typically `Unknown`) — never silently `True`.

## Critical files to create

- `api/v1/{groupversion_info,shared_types,echelon_types,clusterechelon_types}.go`
- `internal/controller/{reconciler,owner_adapter,echelon_controller,clusterechelon_controller,crd_watcher}.go`
- `internal/watcher/{registry,subscriber_index,handler}.go`
- `internal/discovery/resolver.go`
- `internal/status/{kstatus,reducer}.go`
- `internal/metrics/{metrics,pipeline,collector}.go`
- `cmd/main.go`
- `config/**` (kubebuilder generates; we add CEL validators + RBAC samples)
- `config/prometheus/{service_monitor,alerts}.yaml`
- `config/grafana/echelon-dashboard.json`

## External libraries to reuse (not reinvent)

| Need | Use |
| --- | --- |
| Per-resource readiness | `sigs.k8s.io/cli-utils/pkg/kstatus/status` (`Compute`) |
| Manager / controller plumbing | `sigs.k8s.io/controller-runtime` (latest compatible w/ operator-sdk 1.42.2) |
| Dynamic informers | `k8s.io/client-go/dynamic/dynamicinformer` |
| Discovery | `k8s.io/client-go/discovery` (with cached round-tripper) |
| Conditions helpers | `k8s.io/apimachinery/pkg/api/meta` (`SetStatusCondition`) |
| Selectors | `k8s.io/apimachinery/pkg/labels` |
| Metrics | `github.com/prometheus/client_golang/prometheus` (registered against `sigs.k8s.io/controller-runtime/pkg/metrics.Registry`) |
| Metrics testing | `github.com/prometheus/client_golang/prometheus/testutil` |
| Test framework | `github.com/onsi/ginkgo/v2` + `github.com/onsi/gomega`, `sigs.k8s.io/controller-runtime/pkg/envtest` |

## Build sequence (TDD-friendly order)

1. `operator-sdk init` + `create api` for both kinds; commit empty scaffold.
2. `api/v1` types and CEL markers; codegen; CRD manifests.
3. `internal/status/{kstatus,reducer}` — pure-function package, full unit coverage of reduction matrix first (TDD).
4. `internal/discovery/resolver` — fake discovery client tests first.
5. `internal/watcher/{subscriber_index,registry,handler}` — unit tests with fake informers; emit subscribers/informers/event metrics from registry methods.
6. `internal/metrics/{metrics,pipeline}` — register collectors/counters/histograms; unit tests with `prometheus/client_golang/prometheus/testutil`.
7. `internal/controller/owner_adapter` + `reconciler` — pipeline unit tests with fakes; wrap stages in `metrics.ObserveStage`.
8. `internal/metrics/collector` — lister-backed custom collector; unit tests with fake client + table-driven scrape assertions.
9. `internal/controller/{echelon,clusterechelon,crd_watcher}_controller` — wiring.
10. `cmd/main.go` — manager bootstrap; register metrics on the controller-runtime registry.
11. `test/envtest/**` — integration suites covering scenarios listed below (incl. one that asserts on `/metrics`).
12. Sample manifests in `config/samples/`, `config/prometheus/{service_monitor,alerts}.yaml`, `config/grafana/echelon-dashboard.json`; basic README.

## Verification

**Build & generate**
```bash
make generate manifests          # codegen + CRDs
go build ./...
golangci-lint run ./...
```

**Unit tests**
```bash
go test ./internal/status/... ./internal/watcher/... ./internal/discovery/... ./internal/controller/... ./internal/metrics/... -race -count=1
```
Expect 100% coverage of `status/reducer.go`, `watcher/subscriber_index.go`, and
`metrics/collector.go` — these are the load-bearing pure functions.

**Envtest integration suites** (`test/envtest/`):
1. Empty selector + each `emptySetPolicy` value yields the expected `Ready` per target.
2. Members transition Current→InProgress→Failed; status converges within seconds (use `Eventually`).
3. ClusterEchelon with `namespaces:[a,b]` and another target using `namespaceSelector` — both scopes match correctly.
4. Echelon references not-yet-installed CRD: starts `Stalled=GVKNotEstablished`; install CRD; converges to Ready=Unknown→True without manual nudge.
5. Spec edit removes a target → registry Unsubscribes → informer torn down at refcount=0 (verify via registry introspection helper exposed for tests).
6. Echelon deletion runs finalizer → all subscriptions released; deleted object disappears.
7. Status patch idempotency: with no underlying change, no resourceVersion bump after second reconcile.
8. Two Echelons referencing the same GVK with different selectors share a single informer (verify via registry introspection).
9. `/metrics` endpoint exposes the documented metric families with expected values: create an Echelon with N members in known kstatus states, then assert `echelon_status_condition`, `echelon_target_members`, `echelon_informers`, `echelon_subscribers` via `prometheus/client_golang/prometheus/testutil.ToFloat64` / `CollectAndCompare`.
```bash
go test ./test/envtest/... -race -count=1 -timeout=10m
```

**Manual smoke (optional, post-MVP)**
- `kind create cluster && make install deploy`
- Apply FluxCD CRDs + a sample Echelon; observe `kubectl get echelon -o yaml`
  reflecting member transitions live.

## v1 compatibility discipline

Because we are shipping `v1` directly (no alpha/beta), every shape decision in this design
must be one we are willing to live with. Iteration before first release happens locally:
the schema and reduction rules are revised in-tree until we tag v1.0.0, after which:

- Field renames or removals require a new API version + conversion webhook.
- Field additions are safe iff optional (`omitempty`) and have a sensible zero-value default.
- New conditions / reasons can be added freely (consumers must tolerate unknowns).
- New `EmptySetPolicy` enum values are *not* backwards-compatible for strict clients;
  treat the existing three as final.
- New optional fields like a future `spec.targets[].filter` or `spec.targets[].cel` are
  additive and acceptable post-v1.

## Out of scope (MVP)

- CEL/expression filter implementation (additive post-v1)
- Conversion webhooks (only one served version)
- kwok-driven or real-cluster e2e suites
- Admission webhooks (CEL on the CRD covers cross-field validation)
- Bespoke per-kind status evaluators beyond what `kstatus.Compute` provides
- Cross-cluster targets
- Helm chart packaging
