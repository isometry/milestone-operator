# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`milestone-operator` aggregates the kstatus of arbitrary Kubernetes
resources (identified by GVK + label selector) into a kstatus-compatible
`Ready` condition on its own CRDs (`Milestone`, `ClusterMilestone`, group
`milestone.as-code.io`, version **`v1`**). It exists to drive
deployment-wave / run-level gating alongside FluxCD.

The full design — API surface, reduction rules, watcher architecture,
metric inventory, build sequence, and v1 compatibility discipline — lives
in [`PLAN.md`](./PLAN.md). **Treat `PLAN.md` as the source of truth for
intent.** When code and `PLAN.md` disagree, fix one or the other
deliberately and update both.

## Architecture (big picture)

Operator-sdk v1.42.2 / Kubebuilder v4 layout. Both CRDs are served by a
**single generic reconcile pipeline** (`internal/controller/reconciler.go`)
behind an `OwnerAdapter` interface, with two thin per-CRD wrappers wiring
controller-runtime. Per reconcile: resolve targets via the cached discovery
resolver → diff subscriptions against the shared watcher registry →
list-and-compute kstatus per dependency → reduce per-target then
owner-level → patch status (skip when unchanged).

The watcher layer is the load-bearing piece: **one dynamic informer per
unique GVK** (`internal/watcher/registry.go`), refcounted across
Milestone/ClusterMilestone owners. A separate CRD-watch controller wakes
stalled owners on `Established=True` instead of polling. Dependency events
flow back to owners via per-controller `event.GenericEvent` channels fed
by the registry's `EnqueueFunc`.

Status surfaces three kstatus-compatible conditions: `Ready` (aggregate),
`Reconciling`, and `Stalled` (independent of Ready). Per-dependency
`emptySetPolicy` controls how an empty resource set is reported. Status
patching is idempotent: identical reconciles produce no resourceVersion
churn (verified in tests). `status.dependsOn` is a listmap keyed by `name`,
sorted by `name` before assignment so `reflect.DeepEqual` stays stable.

Spec shape: `spec.dependsOn[]` is a non-atomic list of
`{name, emptySetPolicy, target}` entries; each `target` carries the GVK +
label selector (plus `namespaces` / `namespaceSelector` on
ClusterMilestone). `name` is the listmap key — kebab-case, RFC-1123 label.

Metrics are first-class. `internal/metrics/metrics.go` defines a 13-family
inventory; `internal/metrics/collector.go` is a lister-backed collector
emitting per-object gauges at scrape time. All registered against the
controller-runtime registry — never a separate `/metrics` server.

The naming vocabulary is layered: **resources → targets → owner**.
Resource-level reasons (`ReasonAllResourcesReady` etc.) describe matched
resources; owner-level reasons (`ReasonAllDependenciesReady` etc.) describe
per-dependency rollups. Failure metrics with no dependency name available
(GVK resolution, list errors) use the `target_*` family; rollup gauges
labelled by name use `dependency_*`.

`DependencyStatus.Reason` is a closed CRD enum. The full set is:
`AllResourcesReady`, `ResourcesNotReady`, `ResourcesInProgress`,
`ResourcesUnknown`, `EmptySet`, `GVKNotEstablished`,
`NamespaceScopeMismatch`, `DiscoveryFailed`, `DiscoveryUnavailable`,
`WatchSetupFailed`, `ListFailed`, `ReconcileError`. Use `ListFailed` for
informer lister errors (the watch is up); use `DiscoveryUnavailable` for
apiserver discovery API failures distinct from "CRD not installed"
(`GVKNotEstablished`). Owner-level `Conditions[].Reason` is documented
but not enum-closed (it's `metav1.Condition.Reason`).

## Workflow

### Common commands

```sh
make help                        # list every target
make generate manifests          # regenerate deepcopy + CRDs after api/v1 changes
make build                       # full build (generate + fmt + vet + go build)
make lint                        # golangci-lint
make test                        # full unit + envtest run (downloads envtest binaries)
make run                         # run the manager against the current kubeconfig
make install / make uninstall    # apply/remove CRDs to the current cluster
make deploy IMG=<reg>/img:tag    # build, push, and apply manifests
```

### Running tests directly

```sh
# All unit tests, race-checked, no envtest binaries needed
go test ./internal/... -race -count=1

# Envtest scenarios — needs `make setup-envtest` once
KUBEBUILDER_ASSETS=$(./bin/setup-envtest use --bin-dir ./bin -p path) \
  go test ./internal/controller/... -run TestEnvtest -count=1

# Single test by name
go test ./internal/status -run TestReduceDependency_Resources -v
go test ./internal/status -run 'TestReduceDependency_Resources/all_current' -v

# Single envtest scenario
KUBEBUILDER_ASSETS=$(./bin/setup-envtest use --bin-dir ./bin -p path) \
  go test ./internal/controller -run TestEnvtest_LateCRD_StalledThenConverges -v
```

`test/e2e/` expects a real cluster (kind/k3d) and is **not** part of the
default unit/envtest run. Exclude it explicitly with
`go test $(go list ./... | grep -v /test/e2e)` when running broad sweeps.

### TDD is the working mode

The reducer, registry, discovery resolver, metrics, and adapters were all
built test-first. New behaviour follows the same cycle: failing test →
minimal implementation → green. Don't add a feature without a test that
would have caught its absence.

Pure-function packages (`internal/status`, `internal/discovery`,
`internal/metrics`, `internal/watcher/subscriber_index.go`) carry the highest
coverage — keep them that way. Integration concerns belong in
`internal/controller/envtest_test.go`.

### Commits are user-gated

This repo signs every commit with a yubikey. Before invoking `git commit`,
**stage the changes and use `AskUserQuestion` to pause**. A rhetorical
"ready to commit, please touch your yubikey" sentence is *not* a pause; the
shell call fires immediately and the touch times out. Batch related work
into a single commit when it's a logical unit, to reduce yubikey touches.

## Conventions

### API design (`milestone.as-code.io/v1`)

- v1 is **pre-release**. Until v1.0.0 is tagged we can break our own
  internal APIs freely; once tagged, field renames or removals require a
  new version + a conversion webhook. New optional `omitempty` fields and
  new owner-level condition types/reasons are safe within a stable v1.
  *Not* back-compat-safe post-tag: new `EmptySetPolicy` enum values; new
  `DependencyStatus.Reason` enum values (the field carries a closed CRD
  enum); lowered `MaxItems` on `dependsOn` or `notReadyResources`;
  changes to the `milestone.as-code.io/finalizer` string.
- Use canonical Kubernetes field names. `selector`, not `labelSelector`;
  `namespaceSelector`, not `nsLabelSelector`. The Go *type* name is rarely
  the right *field* name.
- For multi-target shapes, prefer `selector` (wrapping `metav1.LabelSelector`)
  over inlining `matchLabels`/`matchExpressions` on the parent struct.
  Idiomatic > fewer indents.
- Required fields get explicit `+kubebuilder:validation:Required` markers.
  Don't rely on absent `omitempty` to imply requiredness — controller-gen
  treats that as optional in OpenAPI unless you say otherwise.
- Defensive validation belongs in the CRD via `+kubebuilder:validation` and
  CEL `XValidation`, plus runtime checks in the adapter (some checks like
  scope mismatches need discovery and can't run at admission time). Watch
  the CEL cost budget — listmap uniqueness comes free server-side, so
  redundant CEL uniqueness rules will blow the budget.

### Code

- The generic `Reconciler` (`internal/controller/reconciler.go`) is shared by
  both CRDs via `OwnerAdapter`. Don't duplicate pipeline logic in the
  per-CRD wrappers.
- Errors flowing out of a reconcile stage should be retryable (`return err`)
  or surfaced via `DependencyError` on `Stalled`. Don't silently swallow.
- `Stalled` is independent of `Ready`. When stalled, `Ready` reflects what
  we *can* observe (typically `Unknown`) — never silently `True`.
- One informer per GVK, refcounted in `watcher.Registry`. Never construct a
  per-Milestone informer.

### Observability

- Metrics are **first-class**, not phase-2. Any new code path that's
  diagnosable should emit a counter or histogram from `internal/metrics`.
- New metric? Document it in `PLAN.md`'s "Metric inventory" with its
  cardinality bound.
- Object-state metrics belong in `metrics/collector.go` (lister-backed,
  scrape-time evaluation). Pipeline events belong in `metrics/metrics.go`
  (in-line counters).
- Register against `sigs.k8s.io/controller-runtime/pkg/metrics.Registry`
  (the manager's `/metrics`). Never start a separate `/metrics` server.
- Metric naming follows the three-layer model: `target_*` for failures
  where no dependency name is available, `dependency_*` where the name is
  a label dimension. Be consistent — if a metric is labelled `dependency`,
  name it `dependency_*`.

### Code style

- No comments restating the obvious. Save them for **why** something
  surprising is the way it is — not **what** the code does.
- Don't add backwards-compat shims for code that hasn't shipped publicly
  yet. We can break our own internal APIs freely until v1.0.0 is tagged.

## Where things live

| Concern                              | Path                                         |
|--------------------------------------|----------------------------------------------|
| Approved design (intent of record)   | `PLAN.md`                                    |
| API types                            | `api/v1/`                                    |
| Reducer / kstatus                    | `internal/status/`                           |
| Discovery + TTL cache                | `internal/discovery/`                        |
| Watcher registry + dynamic informers | `internal/watcher/`                          |
| Reconcile pipeline + adapters        | `internal/controller/`                       |
| Prometheus inventory + collector     | `internal/metrics/`                          |
| Manager bootstrap                    | `cmd/main.go`                                |
| Generated CRDs / RBAC                | `config/crd/bases/`, `config/rbac/`          |
| Envtest scenarios                    | `internal/controller/envtest_test.go`        |
