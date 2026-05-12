# Milestone Operator — Design

## Context

A Kubernetes operator (`milestone-operator`) that monitors and aggregates the
[kstatus](https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus)
of arbitrary resources identified by GVK and label selector, surfacing a
kstatus-compatible `Ready` condition on its own CRD. The operator exists to
support a *deployment-wave* / *milestone gating* mechanism running alongside
FluxCD: downstream consumers gate on a `Milestone`'s `Ready` condition to
decide when a stage has settled and the next stage may proceed.

Two CRDs:

- `Milestone` (namespaced) — aggregates resources within its own namespace.
- `ClusterMilestone` (cluster-scoped) — aggregates resources across
  namespaces, with per-dependency namespace selection.

Built with operator-sdk **v1.42.2** (Kubebuilder v4) and Go **1.26+**.

## Decisions

| Topic | Decision |
| --- | --- |
| Spec shape | `spec.dependsOn` is a non-atomic list of `{name, emptySetPolicy, target}` entries; `+listType=map`, `+listMapKey=name`. `MinItems=1`. Names are RFC-1123 labels enforced by a field-level `Pattern` marker. |
| Empty-set semantics | `emptySetPolicy: Unknown\|Ready\|NotReady` **per dependency** (default `Unknown`). |
| API ident | `milestone.as-code.io/v1`. Pre-v1.0.0; we can break our own internal APIs freely until tagged. |
| `status.notReadyResources` verbosity | Only non-Current resources (capped at 50) + aggregate `summary` counters; `truncated` flag. |
| Missing CRD handling | `Stalled=True, reason=GVKNotEstablished` + watch `apiextensions.k8s.io/v1.CustomResourceDefinition` to wake on `Established=True`. |
| ClusterMilestone scoping | `target.namespaces` and `target.namespaceSelector` are **per-dependency** and mutually exclusive (CRD CEL validation). |
| Watcher architecture | Shared, refcounted registry; one cluster-scoped dynamic informer per GVK. |
| Per-resource readiness | `sigs.k8s.io/cli-utils/pkg/kstatus/status.Compute` (strict — no condition-fallback). |
| Conditions | `Ready`, `Reconciling`, `Stalled` (kstatus-compatible). |
| Operator scope | Single cluster-scoped deployment serves both CRDs. |
| Test strategy | Unit + envtest (Go test); e2e against kind via ginkgo deferred. |
| Observability | First-class Prometheus metrics from day one (custom collector + pipeline counters/histograms); ServiceMonitor + sample alert rules shipped in `config/`. |

## API surface

### Shared types (`api/v1/shared_types.go`)

```go
// +kubebuilder:validation:Enum=Unknown;Ready;NotReady
type EmptySetPolicy string

type TargetSpec struct {
    Group    string                `json:"group,omitempty"`
    Version  string                `json:"version,omitempty"` // empty → resolved via discovery
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:MinLength=1
    Kind     string                `json:"kind"`
    Selector *metav1.LabelSelector `json:"selector,omitempty"` // standard Kubernetes idiom
}

// CEL on ClusterTargetSpec:
//   !(has(self.namespaces) && has(self.namespaceSelector))
type ClusterTargetSpec struct {
    TargetSpec        `json:",inline"`
    Namespaces        []string              `json:"namespaces,omitempty"`
    NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`
}

type DependencyRef struct {
    // +kubebuilder:validation:Required
    // Pattern: ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ (RFC-1123 label), MinLength=1, MaxLength=63
    Name           string         `json:"name"`
    // +kubebuilder:default=Unknown
    EmptySetPolicy EmptySetPolicy `json:"emptySetPolicy,omitempty"`
    // +kubebuilder:validation:Required
    Target         TargetSpec     `json:"target"`
}

type ClusterDependencyRef struct {
    Name           string            `json:"name"`
    EmptySetPolicy EmptySetPolicy    `json:"emptySetPolicy,omitempty"`
    Target         ClusterTargetSpec `json:"target"`
}

type Summary struct {
    Total       int32 `json:"total"`
    Current     int32 `json:"current"`
    InProgress  int32 `json:"inProgress"`
    Failed      int32 `json:"failed"`
    NotFound    int32 `json:"notFound"`
    Terminating int32 `json:"terminating"`
    Unknown     int32 `json:"unknown"`
}

type DependencyStatus struct {
    Name    string                 `json:"name"`
    Group   string                 `json:"group,omitempty"`
    Version string                 `json:"version,omitempty"`
    Kind    string                 `json:"kind"`
    // +kubebuilder:validation:Enum=True;False;Unknown
    Ready   metav1.ConditionStatus `json:"ready"`
    Reason  string                 `json:"reason,omitempty"`
    Summary Summary                `json:"summary"`
}

type ResourceStatus struct {
    Group     string `json:"group,omitempty"`
    Version   string `json:"version"`
    Kind      string `json:"kind"`
    Namespace string `json:"namespace,omitempty"`
    Name      string `json:"name"`
    Status    string `json:"status"`            // kstatus enum
    Reason    string `json:"reason,omitempty"`
    Message   string `json:"message,omitempty"`
}

type MilestoneStatusBase struct {
    ObservedGeneration int64              `json:"observedGeneration,omitempty"`
    Conditions         []metav1.Condition `json:"conditions,omitempty"`
    Summary            Summary            `json:"summary,omitempty"`
    // +listType=map +listMapKey=name; reconciler sorts by Name before assigning.
    DependsOn          []DependencyStatus `json:"dependsOn,omitempty"`
    NotReadyResources  []ResourceStatus   `json:"notReadyResources,omitempty"`
    Truncated          bool               `json:"truncated,omitempty"`
    LastEvaluatedTime  metav1.Time        `json:"lastEvaluatedTime,omitempty"`
}
```

### `Milestone` / `ClusterMilestone`

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
    - name: helmreleases
      target:
        group: helm.toolkit.fluxcd.io
        kind: HelmRelease
        selector: { matchLabels: { wave: "0" } }
```

```yaml
apiVersion: milestone.as-code.io/v1
kind: ClusterMilestone
metadata: { name: platform-wave-0 }
spec:
  dependsOn:
    - name: flux-kustomizations
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

CRDs use the `milestone.as-code.io/v1` group; finalizer is
`milestone.as-code.io/finalizer`. No `shortNames` are set — easy to add
later if operators clamour for them and we've cleared collisions
(`ms`/`cms` collide with cluster-api `MachineSet`).

## Reduction rules

**Per dependency (resource → dependency)** in `internal/status/reducer.go`:

1. Compute kstatus for every matched resource (`status.Compute`).
2. Bucket counts populate `Summary`.
3. Empty set: apply `emptySetPolicy` → `Ready = Unknown | True | False`,
   `Reason = EmptySet`.
4. Non-empty:
   - any `Failed` or `NotFound` → `Ready=False`, `Reason=ResourcesNotReady`.
   - any `InProgress` or `Terminating` → `Ready=Unknown`,
     `Reason=ResourcesInProgress`.
   - any `Unknown` (no other transitions) → `Ready=Unknown`,
     `Reason=ResourcesUnknown`.
   - else `Ready=True`, `Reason=AllResourcesReady`.

**Owner-level (dependency → owner)** in `ReduceOwner`:

- 0 dependencies → `Ready=Unknown`, `Reason=EmptySet`,
  `message="no dependencies configured"`.
- any dependency `Ready=False` → `Ready=False`,
  `Reason=DependenciesNotReady`,
  `message="dependencies not ready: <sorted names>"`.
- all `Ready=True` → `Ready=True`, `Reason=AllDependenciesReady`.
- otherwise → `Ready=Unknown`, `Reason=DependenciesInProgress`.

`Stalled` is **independent** of `Ready` and is driven by structural
failures (discovery, watch setup, scope mismatch, list errors). When
stalled, `Ready` reflects what we *can* observe — never silently `True`.

The reason vocabulary uses a three-layer model — **resources → targets →
owner**. Resource-level reasons describe matched resources;
dependency-level / owner-level reasons describe dependency rollups.

## Watcher architecture

`internal/watcher/registry.go` holds one dynamic informer per unique GVK,
refcounted across Milestone and ClusterMilestone owners. The Subscriber
shape carries multiple `Matcher`s so two dependencies sharing a GVK with
different selectors collapse into one informer + two matchers. Per-GVK
informer events are translated to `event.GenericEvent` on per-controller
channels (`milestoneEvents`, `cmilestoneEvents`) so each controller pulls
its own queue.

A separate `CRDWatcher` controller (`internal/controller/crd_watcher.go`)
watches `CustomResourceDefinition` and, on every `Established=True`
transition, invalidates the discovery cache and enqueues every Milestone /
ClusterMilestone whose `spec.dependsOn[*].target.group + kind` matches the
newly-established CRD.

## Reconcile pipeline

`Reconciler[T client.Object]` in `internal/controller/reconciler.go` is
generic over the owner type; per-CRD wrappers
(`MilestoneReconciler`, `ClusterMilestoneReconciler`) wire it into
controller-runtime. The pipeline is:

1. Finalizer check / addition (skip when deleting → run unsubscribe).
2. `adapter.Dependencies(ctx, resolver)` → `[]NormalizedDependency` +
   `[]DependencyError`. Discovery / scope / selector errors surface as
   `DependencyError`s with structural reasons.
3. `reconcileSubscriptions(ownerKey, deps)`: group desired by GVK, diff
   against currently-subscribed GVKs, subscribe/unsubscribe accordingly.
   Subscribe failures fan out as `DependencyError`s with
   `WatchSetupFailed` for every dependency in the failing GVK group.
4. `evaluateDependencies(deps, failedReasons)`: list resources per GVK,
   admit by namespace+label, compute kstatus, reduce to a per-dependency
   `DependencyStatus`. Dependencies whose subscribe failed skip list +
   reduce — their rollup carries `Ready=Unknown, Reason=WatchSetupFailed`.
5. `applyStatus(status, generation, rollups, notReady, errs)`: sort
   `status.dependsOn` by `name`, set `Ready` from `ReduceOwner`, set
   `Reconciling=False, Reason=ReconcileComplete`, set
   `Stalled` from the accumulated errors.
6. Idempotency: `statusEqualIgnoringTimestamp` (deep-equal modulo
   `LastEvaluatedTime` and per-condition `LastTransitionTime` +
   `ObservedGeneration`) gates the patch. No churn on identical reconciles.
7. Patch via `Status().Update()` (full replacement). If `Stalled=True`,
   requeue after 30s as a safety net (the CRD watcher also wakes us).

## Flux integration

FluxCD's `kustomize-controller` and `helm-controller` only evaluate
`spec.healthChecks` on each Kustomization/HelmRelease's own reconcile
interval — they do not continuously watch managed CRs. For a Milestone
deployed via Flux, the parent's health therefore lags the child's real
state between intervals. To close that gap, after every successful
status patch the Reconciler checks whether the `Ready` condition's
status changed; if so it invokes a `FluxNotifier` that patches
`reconcile.fluxcd.io/requestedAt` on the parent Kustomization /
HelmRelease, prompting Flux to re-evaluate immediately.

- Parent identity is read from the labels the Flux controllers
  auto-stamp on every managed resource (`SetOwnerLabels` in
  `github.com/fluxcd/pkg/ssa/manager.go`):
  `kustomize.toolkit.fluxcd.io/{name,namespace}` and
  `helm.toolkit.fluxcd.io/{name,namespace}`. A child carrying both
  pairs (rare but legal: a Kustomization wrapping a HelmRelease that
  templates the Milestone) is poked on both parents.
- The notify is fire-and-forget: errors are classified
  (`success` / `not_found` / `no_match` / `forbidden` / `error`) on
  `milestone_flux_notify_total` and logged at V(1); they never propagate
  back into the reconcile result. No-match (Flux CRDs not installed) is
  treated identically to any other failure mode.
- The hook lives inside the patch-changed branch of the pipeline, so
  idempotent reconciles never poke. Transition is defined as
  `readyConditionStatus(prior) != readyConditionStatus(current)`; a
  fresh object with no prior `Ready` condition counts as a transition
  from `Unknown`.
- Controlled by `--flux-notify` (default `true`). When disabled, the
  `FluxNotifier` field on the generic Reconciler is left nil and the
  hook becomes a no-op.
- No per-Milestone opt-out and no in-process debounce: status-patch
  idempotency already gates the poke rate, and Flux dedups identical
  reconcile requests via `.status.lastHandledReconcileAt`.

## Metric inventory

All metrics namespaced `milestone_*`. Cardinality bounds in parentheses.

### Watcher / informers

- `milestone_informers{group,version,kind}` (gauge, bounded by distinct
  watched GVKs across all owners).
- `milestone_subscribers{group,version,kind}` (gauge, same bound).
- `milestone_informer_events_total{group,version,kind,event}` (counter,
  event ∈ {add, update, delete}; bounded by 3× distinct GVKs).
- `milestone_subscribe_total{group,version,kind,result}` (counter,
  result ∈ {ok, error}).
- `milestone_unsubscribe_total{group,version,kind}` (counter).
- `milestone_event_dispatch_duration_seconds{group,version,kind}`
  (histogram).

### Discovery

- `milestone_discovery_resolve_total{result}` (counter,
  result ∈ {hit, miss, not_established, error}).
- `milestone_discovery_cache_size` (gauge).

### Reconcile pipeline

- `milestone_reconcile_stage_duration_seconds{controller,stage}`
  (histogram; controllers ∈ {Milestone, ClusterMilestone};
  stages ∈ {discovery, subscriptions, list, compute, reduce, patch}).
- `milestone_status_patch_total{controller,result}` (counter,
  result ∈ {changed, unchanged, error}).
- `milestone_target_resolve_errors_total{controller,reason}` (counter,
  reason ∈ {GVKNotEstablished, WatchSetupFailed, DiscoveryFailed,
  NamespaceScopeMismatch}). No `dependency` label — discovery / scope
  errors fire before the dependency identity is fully resolved.

### CRD watcher

- `milestone_crd_established_events_total{group,kind}` (counter).
- `milestone_owners_woken_total{reason}` (counter,
  reason ∈ {crd_established}).

### Flux integration

- `milestone_flux_notify_total{controller,parent_kind,result}` (counter,
  controller ∈ {Milestone, ClusterMilestone}; parent_kind ∈
  {Kustomization, HelmRelease}; result ∈ {success, not_found, no_match,
  forbidden, error}). Bound: ≤20 series.

### Object-state (lister-backed, scrape-time)

- `milestone_status_condition{owner_kind,namespace,name,type,status}`
  (gauge).
- `milestone_observed_generation{owner_kind,namespace,name}` (gauge).
- `milestone_dependency_resources{owner_kind,namespace,name,dependency,target_group,target_kind,status}`
  (gauge; status is the kstatus bucket name including `total`). Cardinality:
  `8 × ⟨total owners⟩ × ⟨dependencies-per-owner⟩`.
- `milestone_dependency_ready{owner_kind,namespace,name,dependency,target_group,target_kind}`
  (gauge: 1=True, 0=False, -1=Unknown).
- `milestone_last_evaluated_timestamp_seconds{owner_kind,namespace,name}`
  (gauge).

## v1 compatibility discipline

`milestone.as-code.io/v1` will be stable once v1.0.0 is tagged. Until
then, we iterate freely. Once tagged:

- **Safe**: new optional `omitempty` fields; new condition types and
  reasons; new printcolumns; new metrics.
- **Unsafe**: field renames or removals; new enum values on
  `EmptySetPolicy`; reordering of `+listMapKey` semantics; changing the
  finalizer string. These require a new API version + a conversion webhook.

Pre-v1.0.0 internal APIs (function signatures, package layout, interface
shapes) are not stable and may change without notice.
