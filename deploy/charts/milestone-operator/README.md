# milestone-operator Helm chart

Deploys [milestone-operator](https://github.com/isometry/milestone-operator) —
an operator that aggregates the kstatus of arbitrary Kubernetes resources
(identified by GVK + label selector) into a `Ready` condition on its own
`Milestone` / `ClusterMilestone` CRDs, for deployment-wave / run-level gating
alongside FluxCD.

## Installing the Chart

To install with the release name `milestone-operator`:

```sh
helm install milestone-operator \
  -n milestone-operator-system --create-namespace \
  oci://ghcr.io/isometry/charts/milestone-operator
```

## Verifying the chart

Releases are keyless-signed (Sigstore) and carry SLSA build provenance.
Verify before installing:

```sh
helm pull oci://ghcr.io/isometry/charts/milestone-operator --version <version> --verify
```

See [`docs/verification.md`](../../../docs/verification.md) for cosign
commands and ready-to-apply Flux/Kyverno enforcement policies.

## Uninstalling the Chart

```sh
helm uninstall milestone-operator -n milestone-operator-system
```

By default the CRDs are annotated `helm.sh/resource-policy: keep`, so a
`helm uninstall` leaves the CRDs — and any `Milestone` / `ClusterMilestone`
resources — in place. Set `crds.keep=false` to let Helm remove them.

## Configuration

| Parameter | Description | Default |
| --- | --- | --- |
| `crds.install` | Install the `Milestone` / `ClusterMilestone` CRDs | `true` |
| `crds.keep` | Annotate CRDs with `helm.sh/resource-policy: keep` | `true` |
| `leaderElection.enabled` | Run a single active manager via a Lease | `true` |
| `fluxNotify` | Poke the FluxCD parent on Ready transitions (toggles the `--flux-notify` arg **and** the helmrelease/kustomization `patch` grant) | `true` |
| `rbac.create` | Create the ServiceAccount, Roles and Bindings | `true` |
| `rbac.serviceAccount.name` | Override the ServiceAccount name | chart fullname |
| `rbac.serviceAccount.annotations` | Annotations on the ServiceAccount (e.g. IRSA) | `{}` |
| `rbac.extraClusterRoleBindings` | Bind pre-existing ClusterRoles to the operator SA | `[]` |
| `rbac.dynamicWatch.targets` | Named member ClusterRoles granting watch on target kinds (see below) | `flux` enabled |
| `metrics.enabled` | Serve and expose the `/metrics` endpoint | `true` |
| `metrics.listen.port` | Metrics port (`--metrics-bind-address`) | `8080` |
| `metrics.secure` | Serve metrics over HTTPS with bearer-token auth (`--metrics-secure`) | `false` |
| `metrics.service.type` | Metrics Service type | `ClusterIP` |
| `metrics.serviceMonitor.enabled` | Render a Prometheus `ServiceMonitor` (needs the Prometheus Operator CRDs) | `false` |
| `manager.repository` | Image repository | `ghcr.io/isometry/milestone-operator` |
| `manager.tag` | Image tag (a `sha256:…` value is treated as a digest) | chart `appVersion`, then `latest` |
| `manager.replicas` | Replica count | `1` |
| `manager.extraArgs` | Additional CLI flags as a `key: value` map (`--key=value`) | `{}` |
| `manager.env` | Additional container environment variables | `[]` |
| `manager.resources` | Manager container resource requests/limits | req `10m`/`128Mi`, lim `500m`/`512Mi` |

## Granting watch access to new target kinds

The operator watches the kinds referenced by `spec.dependsOn[].target` through
**dynamic informers**. Their permissions come from an aggregation umbrella
ClusterRole whose rules the kube-controller-manager unions from every ClusterRole
labelled `milestone.as-code.io/aggregate-to-dynamic-watch: "true"`.

Each entry under `rbac.dynamicWatch.targets` renders one such labelled member
ClusterRole. The `flux` target ships enabled (covering the FluxCD toolkit
groups). To watch additional groups, add entries — `verbs` defaults to
`[get, list, watch]` (all the operator ever needs), so you usually specify only
`apiGroups` and `resources`:

```yaml
rbac:
  dynamicWatch:
    targets:
      flux:
        enabled: true            # default; set false to drop, or override rules
      argocd:
        enabled: true
        rules:
          - apiGroups: [argoproj.io]
            resources: [applications, applicationsets]
      certManager:
        enabled: true
        rules:
          - apiGroups: [cert-manager.io]
            resources: [certificates, issuers, clusterissuers]
      core:
        enabled: true
        rules:
          - apiGroups: [""]
            resources: [configmaps, services]
```

Cluster admins can equivalently ship their own ClusterRole carrying the same
aggregation label out-of-band; the chart's `targets` are simply the ergonomic
path. To bind an existing, fully-formed ClusterRole to the operator's
ServiceAccount instead, list it under `rbac.extraClusterRoleBindings`.

> **GitOps note:** the umbrella ClusterRole `…-dynamic-watch-role` has its
> `.rules` populated by the kube-controller-manager at runtime; the chart renders
> it rule-less by design. Configure GitOps drift-detectors (Argo CD / Flux) to
> ignore differences on `/rules` of that ClusterRole.

## Observability

The operator exposes a controller-runtime Prometheus `/metrics` endpoint. With
`metrics.serviceMonitor.enabled=true` the chart renders a `ServiceMonitor` (the
Prometheus Operator CRDs must be installed). Set `metrics.secure=true` to serve
over HTTPS with a bearer-token AuthN/AuthZ filter.
