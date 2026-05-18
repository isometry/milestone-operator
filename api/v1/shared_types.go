/*
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
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EmptySetPolicy controls how a dependency's Ready is reported when zero
// resources match its target selector.
//
// +kubebuilder:validation:Enum=Unknown;Ready;NotReady
type EmptySetPolicy string

const (
	// EmptySetUnknown reports Ready=Unknown for a dependency with zero
	// matches. Default; safe for wave gates that should not advance on
	// emptiness.
	EmptySetUnknown EmptySetPolicy = "Unknown"
	// EmptySetReady reports Ready=True for a dependency with zero matches.
	// Use when a wave should vacuously advance if nothing is selected.
	EmptySetReady EmptySetPolicy = "Ready"
	// EmptySetNotReady reports Ready=False for a dependency with zero
	// matches. Use when emptiness is itself a misconfiguration that should
	// block.
	EmptySetNotReady EmptySetPolicy = "NotReady"
)

// TargetSpec selects a set of resources by GVK and label selector.
type TargetSpec struct {
	// Group is the API group of the target kind. Empty string means the
	// core Kubernetes group.
	// +optional
	Group string `json:"group,omitempty"`

	// Version is the API version of the target kind. When empty, the
	// operator resolves the preferred version via discovery.
	// +optional
	Version string `json:"version,omitempty"`

	// Kind is the target kind (e.g. "Kustomization", "HelmRelease").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Kind string `json:"kind"`

	// Selector is a standard Kubernetes label selector. Nil selects every
	// resource of Kind in scope.
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`
}

// ClusterTargetSpec extends TargetSpec with namespace selection. Exactly
// one of Namespaces or NamespaceSelector may be set; both empty means
// "all namespaces".
//
// +kubebuilder:validation:XValidation:rule="!(size(self.namespaces) > 0 && has(self.namespaceSelector))",message="namespaces and namespaceSelector are mutually exclusive"
type ClusterTargetSpec struct {
	TargetSpec `json:",inline"`

	// Namespaces is an explicit allow-list of namespaces to search.
	// Mutually exclusive with NamespaceSelector.
	// +optional
	// +listType=set
	Namespaces []string `json:"namespaces,omitempty"`

	// NamespaceSelector matches namespaces by label. Mutually exclusive
	// with Namespaces.
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`
}

// DependencyRef is one entry in spec.dependsOn. Identified by Name; the
// Target carries the GVK + selector this dependency aggregates over.
type DependencyRef struct {
	// Name identifies this dependency. Used as the listmap key in
	// spec.dependsOn and surfaced in status, condition messages, logs, and
	// the dependency metric label.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// EmptySetPolicy controls Ready reporting when zero resources match.
	// Defaults to Unknown when omitted.
	// +optional
	// +kubebuilder:default=Unknown
	EmptySetPolicy EmptySetPolicy `json:"emptySetPolicy,omitempty"`

	// Target selects the set of resources this dependency aggregates.
	// +kubebuilder:validation:Required
	Target TargetSpec `json:"target"`
}

// ClusterDependencyRef is the cluster-scoped variant of DependencyRef.
type ClusterDependencyRef struct {
	// Name identifies this dependency. Used as the listmap key in
	// spec.dependsOn and surfaced in status, condition messages, logs, and
	// the dependency metric label.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// EmptySetPolicy controls Ready reporting when zero resources match.
	// Defaults to Unknown when omitted.
	// +optional
	// +kubebuilder:default=Unknown
	EmptySetPolicy EmptySetPolicy `json:"emptySetPolicy,omitempty"`

	// Target selects the set of resources this dependency aggregates,
	// including per-dependency namespace scoping.
	// +kubebuilder:validation:Required
	Target ClusterTargetSpec `json:"target"`
}

// Summary holds aggregate kstatus counters for a set of resources. All
// counters are always populated; a zero value means no resources fall in
// that bucket, not "not yet computed". The bucket names mirror
// sigs.k8s.io/cli-utils kstatus.
//
// +kubebuilder:validation:XValidation:rule="self.total == self.current + self.inProgress + self.failed + self.notFound + self.terminating + self.unknown",message="Summary.Total must equal the sum of all buckets"
type Summary struct {
	// Total is the count of resources currently matched by the dependency's
	// target selector. Total == sum of all the other buckets.
	Total int32 `json:"total"`
	// Current counts resources whose kstatus is Current (steady-state ready).
	Current int32 `json:"current"`
	// InProgress counts resources whose kstatus is InProgress (still converging).
	InProgress int32 `json:"inProgress"`
	// Failed counts resources whose kstatus is Failed.
	Failed int32 `json:"failed"`
	// NotFound counts resources whose kstatus is NotFound (selector matched a
	// name but the resource is absent).
	NotFound int32 `json:"notFound"`
	// Terminating counts resources whose kstatus is Terminating (deletion in
	// progress).
	Terminating int32 `json:"terminating"`
	// Unknown counts resources whose kstatus could not be determined.
	Unknown int32 `json:"unknown"`
}

// DependencyStatus is the per-dependency aggregated readiness reported on
// the owner. One entry is produced for every spec.dependsOn entry,
// regardless of whether discovery succeeded.
type DependencyStatus struct {
	// Name mirrors spec.dependsOn[].name and is the listmap key.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Group is the API group of the resolved target Kind. Empty for the core
	// Kubernetes group.
	// +optional
	Group string `json:"group,omitempty"`
	// Version is the API version resolved via discovery (the target's
	// version when set; otherwise the group's preferred version).
	// +optional
	Version string `json:"version,omitempty"`
	// Kind is the target Kind as declared in the spec.
	// +kubebuilder:validation:Required
	Kind string `json:"kind"`

	// Ready is True when every selected resource is Current; False when any
	// resource is Failed or NotFound; Unknown otherwise (or per
	// emptySetPolicy when no resources match).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=True;False;Unknown
	Ready metav1.ConditionStatus `json:"ready"`

	// Reason is a short machine-readable code summarising why Ready has its
	// current value. The closed enum covers every reason the operator emits
	// at the dependency level; see the Reason* constants in this package
	// for canonical values.
	// +optional
	// +kubebuilder:validation:Enum=AllResourcesReady;ResourcesNotReady;ResourcesInProgress;ResourcesUnknown;EmptySet;GVKNotEstablished;NamespaceScopeMismatch;DiscoveryFailed;DiscoveryUnavailable;WatchSetupFailed;ListFailed;ReconcileError
	Reason string `json:"reason,omitempty"`
	// Summary holds the per-kstatus-bucket counts that produced Ready.
	Summary Summary `json:"summary"`
}

// ResourceStatus identifies an individual matched resource and the kstatus
// computed for it. Only resources whose Status is not Current are surfaced
// on the owner (see MilestoneStatusBase.NotReadyResources).
type ResourceStatus struct {
	// Group is the API group of the resource. Empty for the core group.
	// +optional
	Group string `json:"group,omitempty"`
	// Version is the API version of the resource.
	Version string `json:"version"`
	// Kind is the API Kind of the resource.
	Kind string `json:"kind"`
	// Namespace is the namespace of the resource. Empty when the resource
	// is cluster-scoped.
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// Name is the name of the resource.
	Name string `json:"name"`

	// Status is the kstatus computed by sigs.k8s.io/cli-utils for this
	// resource.
	// +kubebuilder:validation:Enum=Current;InProgress;Failed;NotFound;Terminating;Unknown
	Status string `json:"status"`
	// Reason is the first non-empty reason from the conditions emitted by
	// sigs.k8s.io/cli-utils for this resource (typically the resource's own
	// Ready/Reconciling/Stalled condition reason — e.g. LessReplicas,
	// ProgressDeadlineExceeded).
	// +optional
	Reason string `json:"reason,omitempty"`
	// Message is the kstatus message for this resource when one was
	// provided by sigs.k8s.io/cli-utils.
	// +optional
	Message string `json:"message,omitempty"`
}

// MilestoneStatusBase is the status surface shared by Milestone and
// ClusterMilestone.
type MilestoneStatusBase struct {
	// ObservedGeneration mirrors metadata.generation at the time of the
	// last successful reconcile.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions exposes Ready and Stalled (kstatus-compatible). The
	// operator may add additional condition types in future releases as
	// additive (non-breaking) API changes.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// Summary aggregates resource counters across all dependencies.
	// +optional
	Summary Summary `json:"summary,omitempty"`

	// DependsOn carries per-dependency rollups, keyed by the same name as
	// spec.dependsOn.
	// +optional
	// +listType=map
	// +listMapKey=name
	DependsOn []DependencyStatus `json:"dependsOn,omitempty"`

	// NotReadyResources lists resources whose kstatus is not Current.
	// Capped to avoid object-size explosions; Truncated indicates the cap
	// was hit. The MaxItems cap mirrors the runtime cap applied by the
	// reconciler.
	// +optional
	// +kubebuilder:validation:MaxItems=50
	NotReadyResources []ResourceStatus `json:"notReadyResources,omitempty"`

	// Truncated is true when NotReadyResources was capped.
	// +optional
	Truncated bool `json:"truncated,omitempty"`

	// LastEvaluatedTime is the timestamp of the last status evaluation.
	// +optional
	LastEvaluatedTime metav1.Time `json:"lastEvaluatedTime,omitempty"`
}

// Condition types exposed on Milestone and ClusterMilestone. Reconciling
// is intentionally not exposed today: the pipeline never emits
// Reconciling=True, and shipping a permanently-False condition wastes API
// surface. A future two-phase patch may reintroduce it as an additive
// (non-breaking) change.
const (
	ConditionReady   = "Ready"
	ConditionStalled = "Stalled"
)

// Reason vocabulary for Ready / Reconciling / Stalled conditions and
// rollups.
//
// Two levels of aggregation share this vocabulary:
//   - Resource-level (per-dependency rollup): describes the population of
//     individual matched resources that contributed to the rollup.
//   - Owner-level: describes the population of per-dependency rollups
//     that contributed to the owner Ready.
const (
	// Resource-level reasons (used on DependencyStatus).
	ReasonAllResourcesReady   = "AllResourcesReady"
	ReasonResourcesNotReady   = "ResourcesNotReady"
	ReasonResourcesInProgress = "ResourcesInProgress"
	ReasonResourcesUnknown    = "ResourcesUnknown"

	// Owner-level reasons (used on the owner Ready condition).
	ReasonAllDependenciesReady   = "AllDependenciesReady"
	ReasonDependenciesNotReady   = "DependenciesNotReady"
	ReasonDependenciesInProgress = "DependenciesInProgress"

	// Shared / structural.
	ReasonEmptySet               = "EmptySet"
	ReasonGVKNotEstablished      = "GVKNotEstablished"
	ReasonNamespaceScopeMismatch = "NamespaceScopeMismatch"
	ReasonDiscoveryFailed        = "DiscoveryFailed"
	// ReasonDiscoveryUnavailable indicates the apiserver discovery API
	// was unavailable, distinct from "the requested CRD is not installed"
	// (ReasonGVKNotEstablished).
	ReasonDiscoveryUnavailable = "DiscoveryUnavailable"
	ReasonWatchSetupFailed     = "WatchSetupFailed"
	// ReasonListFailed indicates the dynamic informer's lister returned an
	// error during reconcile. The watch is established; the failure is
	// downstream of subscribe.
	ReasonListFailed = "ListFailed"
	// ReasonReconcileError is the catch-all reason for unexpected reconcile
	// failures that don't fit a more specific reason.
	ReasonReconcileError = "ReconcileError"

	// ReasonReconcileComplete marks a settled reconcile — the reason
	// carried on Stalled=False (no-error path).
	ReasonReconcileComplete = "ReconcileComplete"
)

// Finalizer applied to Milestone and ClusterMilestone objects so the
// operator can release informer subscriptions before the object is
// deleted.
const Finalizer = "milestone.as-code.io/finalizer"
