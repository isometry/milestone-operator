We're going to develop a new kubernetes controller, based on the latest version of the operator-sdk (v1.42.2) and written in modern, idiomatic Go (v1.26.3).
The driving purpose of this operator is to monitor and accumulate the kstatus of other resources, identified by GVK and label selector, to support the implementation of a "run-level"/"deployment-wave" mechanism running alongside FluxCD. It will own an `Echelon` CRD of (something like) the following shape, and is expected to setup watchers for the resources specified by it, and report their aggregate status (kstatus-style) via the status sub-resource, via a Ready condition (kstatus compatible).

```
# CRD: as-code.io/v1, kind: Echelon, scope: Namespaced
spec:
   group: # optional, default ""
   version: # optional
   kind: # required
   labelSelector: <metav1.LabelSelector>
status:
   observedGeneration: <int>
   isReady: <bool>
   conditions:                            # kstatus-compatible
     - type: Ready
       status: "True" | "False" | "Unknown"
       reason: AllMembersReady | MembersNotReady | NoMembers | <other>
       lastTransitionTime: <RFC3339>
       message: "<n>/<total> members Ready" | "missing: gitops.X" | …
   members:                               # optional; informational
     - { name: gitops.cert-manager, namespace: flux-system, ready: true }
```

A second, ClusterEchelon CRD should support the same resources, but with cross-namespace support, taking additional, optional, `spec.namespace` and `spec.namespaceSelector` fields to target the resources it monitors. In addition, I'd like to leave room in the schema and underlying data-models for the future addition of CEL filters to allow more fine-grained filtering.

If you think that there are more idiomatic names for the proposed fields, then I'm open to suggestions.

I'm expecting the controller to build a state machine for each `Echelon`, tracking the resources that match its specification, and set up watchers for those resources to keep those state machines up-to-date. The status of each `Echelon` being automatically updated as watched resources change state.

Tests are key, and we should embrace TDD; envtest and/or kwok should be sufficient for integration/acceptance tests.

Please ultrathink on this design, grill me to clarify any uncertainties or inconsistencies, ultrathink more, and draw up an plan that supports a clean, idiomatic and maintainable implementation.
