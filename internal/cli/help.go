/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli

import "strings"

// Help text lives here rather than beside the commands so the long lines of
// examples stay out of cmd/, where lll applies.

const RootLong = `Inspect Milestones and ClusterMilestones.

The same binary works as a kubectl plugin: install or symlink it as
kubectl-milestone and run "kubectl milestone ...".`

const GetLong = `Show the status the operator last recorded on Milestones and ClusterMilestones.

READY is marked "(stale)" while the operator has not yet observed the current
generation. CURRENT/TOTAL counts matched resources; DEPS counts ready
dependencies. Nothing is evaluated here: use "tree" for a live view.`

// GetExample is rendered with the command path in place of %[1]s.
const GetExample = `  # Milestones in the current namespace
  %[1]s get milestones

  # Every Milestone and ClusterMilestone, cluster-wide
  %[1]s get all -A

  # Only not-ready Milestones in all namespaces, updating as they change
  %[1]s get mile -A --status-selector ready=False --watch

  # A single ClusterMilestone as YAML
  %[1]s get cmile platform -o yaml`

const TreeLong = `Evaluate a Milestone or ClusterMilestone live and show every member resource.

Membership is resolved with the operator's own rules, so the tree shows what
the operator would record right now. A ⚠ note marks a dependency whose live
verdict differs from the recorded one. Dependencies the operator cannot
evaluate, and lists your credentials may not read, are reported inline.

Legend: ✔ ready  ✗ not ready  ⏸ suspended  ◌ in progress  ? unknown`

const TreeExample = `  # Everything in wave-0
  %[1]s tree milestone wave-0 -n flux-system

  # Only the members holding a ClusterMilestone back
  %[1]s tree cmile platform --not-ready

  # The full evaluation as JSON
  %[1]s tree mile wave-0 -o json`

const TraceLong = `Show every Milestone and ClusterMilestone dependency that includes an object.

TYPE is any resource type kubectl understands, including short names
(deploy, ks, hr) and fully-qualified names (helmreleases.helm.toolkit.fluxcd.io).`

const TraceExample = `  %[1]s trace deploy/web -n apps
  %[1]s trace hr podinfo -n flux-system
  %[1]s trace kustomizations.kustomize.toolkit.fluxcd.io/infra -n flux-system -o yaml`

const VersionLong = `Print the client version and, unless --client is given, the image of every
milestone-operator deployment visible to your credentials.`

// Examples substitutes the command path into an example template, so the
// help reads "kubectl milestone ..." when run as a kubectl plugin.
func Examples(template, commandPath string) string {
	return strings.ReplaceAll(template, "%[1]s", commandPath)
}
