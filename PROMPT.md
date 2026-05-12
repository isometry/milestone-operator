We're developing a new Kubernetes controller, based on the latest version of the operator-sdk (v1.42.2) and written in modern, idiomatic Go (v1.26+).

The driving purpose of this operator is to monitor and accumulate the kstatus of other resources, identified by GVK and label selector, to support the implementation of a "milestone gating" / "deployment-wave" mechanism running alongside FluxCD. It owns a `Milestone` CRD of (something like) the following shape, and is expected to set up watchers for the resources specified by it, and report their aggregate status (kstatus-style) via the status sub-resource, via a Ready condition (kstatus compatible).

```yaml
# CRD: milestone.as-code.io/v1, kind: Milestone, scope: Namespaced
spec:
  dependsOn:                              # non-atomic list, +listMapKey=name
    - name: <kebab-case identifier>       # required; surfaces in status, logs, metrics
      emptySetPolicy: Unknown|Ready|NotReady  # default Unknown
      target:
        group:    <optional, default "">
        version:  <optional, resolved via discovery when empty>
        kind:     <required>
        selector: <metav1.LabelSelector>
status:
  observedGeneration: <int>
  summary: { total, current, inProgress, failed, notFound, terminating, unknown }
  conditions:                             # kstatus-compatible
    - type: Ready
      status: "True" | "False" | "Unknown"
      reason: AllDependenciesReady | DependenciesNotReady | DependenciesInProgress | EmptySet | ...
      lastTransitionTime: <RFC3339>
      message: "dependencies not ready: <sorted names>" | ...
    - type: Reconciling
      ...
    - type: Stalled
      ...
  dependsOn:                              # listmap by name, mirrors spec.dependsOn
    - name: <dependency name>
      group: ...
      version: ...
      kind: ...
      ready: "True" | "False" | "Unknown"
      reason: ...
      summary: { ... }
  notReadyResources: [ ... ]
```

A second, `ClusterMilestone` CRD supports the same resources but with cross-namespace support, with each dependency taking optional, mutually-exclusive `target.namespaces` and `target.namespaceSelector` fields. The schema leaves room for future addition of CEL filters for more fine-grained selection.

The controller builds a per-owner reconcile pipeline (generic across `Milestone` / `ClusterMilestone` via an `OwnerAdapter` interface), tracking the resources that match each dependency's target and setting up watchers (one informer per unique GVK, refcounted across owners) to keep status up-to-date. The status of each Milestone is automatically updated as watched resources change state.

Tests are key, and TDD is the working mode; envtest covers the integration story.
