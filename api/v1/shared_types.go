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

// EmptySetPolicy controls how a member's Ready is reported when zero resources
// match its selector.
//
// +kubebuilder:validation:Enum=Unknown;Ready;NotReady
type EmptySetPolicy string

const (
	// EmptySetUnknown reports Ready=Unknown for a member with zero matches.
	// Default; safe for wave gates that should not advance on emptiness.
	EmptySetUnknown EmptySetPolicy = "Unknown"
	// EmptySetReady reports Ready=True for a member with zero matches.
	// Use when a wave should vacuously advance if nothing is selected.
	EmptySetReady EmptySetPolicy = "Ready"
	// EmptySetNotReady reports Ready=False for a member with zero matches.
	// Use when emptiness is itself a misconfiguration that should block.
	EmptySetNotReady EmptySetPolicy = "NotReady"
)

// MemberSpec selects a set of resources by GVK and label selector. The map key
// in spec.members carries the user-given name of this member.
type MemberSpec struct {
	// Group is the API group of the target kind. Empty string means the core
	// Kubernetes group.
	// +optional
	Group string `json:"group,omitempty"`

	// Version is the API version of the target kind. When empty, the operator
	// resolves the preferred version via discovery.
	// +optional
	Version string `json:"version,omitempty"`

	// Kind is the target kind (e.g. "Kustomization", "HelmRelease").
	// +kubebuilder:validation:MinLength=1
	Kind string `json:"kind"`

	// Selector is a standard Kubernetes label selector. Nil selects every
	// resource of Kind in scope.
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`

	// EmptySetPolicy controls Ready reporting when zero resources match.
	// Defaults to Unknown when omitted.
	// +optional
	// +kubebuilder:default=Unknown
	EmptySetPolicy EmptySetPolicy `json:"emptySetPolicy,omitempty"`
}

// ClusterMemberSpec is MemberSpec extended with namespace selection.
// Exactly one of Namespaces or NamespaceSelector may be set; both empty means
// "all namespaces".
//
// +kubebuilder:validation:XValidation:rule="!(has(self.namespaces) && has(self.namespaceSelector))",message="namespaces and namespaceSelector are mutually exclusive"
type ClusterMemberSpec struct {
	MemberSpec `json:",inline"`

	// Namespaces is an explicit allow-list of namespaces to search. Mutually
	// exclusive with NamespaceSelector.
	// +optional
	// +listType=set
	Namespaces []string `json:"namespaces,omitempty"`

	// NamespaceSelector matches namespaces by label. Mutually exclusive with
	// Namespaces.
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`
}

// Summary holds aggregate kstatus counters for a set of resources. All counters
// are always populated; a zero value means no resources fall in that bucket,
// not "not yet computed". The bucket names mirror sigs.k8s.io/cli-utils kstatus.
type Summary struct {
	// Total is the count of resources currently matched by the member's
	// selector. Total == sum of all the other buckets.
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

// MemberRollup is the per-member aggregated readiness reported on the owner.
// One MemberRollup is produced for every entry in spec.members, keyed by the
// same map key, regardless of whether discovery succeeded.
type MemberRollup struct {
	// Group is the API group of the resolved target Kind. Empty for the core
	// Kubernetes group.
	// +optional
	Group string `json:"group,omitempty"`
	// Version is the API version resolved via discovery (the spec's version
	// when set; otherwise the group's preferred version).
	// +optional
	Version string `json:"version,omitempty"`
	// Kind is the target Kind as declared in the spec.
	Kind string `json:"kind"`

	// Ready is True when every selected resource is Current; False when any
	// resource is Failed or NotFound; Unknown otherwise (or per emptySetPolicy
	// when no resources match).
	// +kubebuilder:validation:Enum=True;False;Unknown
	Ready metav1.ConditionStatus `json:"ready"`

	// Reason is a short machine-readable code summarising why Ready has its
	// current value (e.g. AllResourcesReady, ResourcesNotReady, EmptySet).
	// +optional
	Reason string `json:"reason,omitempty"`
	// Summary holds the per-kstatus-bucket counts that produced Ready.
	Summary Summary `json:"summary"`
}

// ResourceStatus identifies an individual matched resource and the kstatus
// computed for it. Only resources whose Status is not Current are surfaced on
// the owner (see EchelonStatusBase.NotReadyResources).
type ResourceStatus struct {
	// Group is the API group of the resource. Empty for the core group.
	// +optional
	Group string `json:"group,omitempty"`
	// Version is the API version of the resource.
	Version string `json:"version"`
	// Kind is the API Kind of the resource.
	Kind string `json:"kind"`
	// Namespace is the namespace of the resource. Empty when the resource is
	// cluster-scoped.
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// Name is the name of the resource.
	Name string `json:"name"`

	// Status is the kstatus computed by sigs.k8s.io/cli-utils for this
	// resource.
	// +kubebuilder:validation:Enum=Current;InProgress;Failed;NotFound;Terminating;Unknown
	Status string `json:"status"`
	// Reason is the kstatus reason for this resource when one was provided by
	// sigs.k8s.io/cli-utils.
	// +optional
	Reason string `json:"reason,omitempty"`
	// Message is the kstatus message for this resource when one was provided
	// by sigs.k8s.io/cli-utils.
	// +optional
	Message string `json:"message,omitempty"`
}

// EchelonStatusBase is the status surface shared by Echelon and ClusterEchelon.
type EchelonStatusBase struct {
	// ObservedGeneration mirrors metadata.generation at the time of the last
	// successful reconcile.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions exposes Ready, Reconciling, and Stalled (kstatus-compatible).
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// Summary aggregates resource counters across all members.
	// +optional
	Summary Summary `json:"summary,omitempty"`

	// Members carries per-member rollups, keyed identically to spec.members.
	// +optional
	Members map[string]MemberRollup `json:"members,omitempty"`

	// NotReadyResources lists resources whose kstatus is not Current. Capped to
	// avoid object-size explosions; Truncated indicates the cap was hit.
	// +optional
	NotReadyResources []ResourceStatus `json:"notReadyResources,omitempty"`

	// Truncated is true when NotReadyResources was capped.
	// +optional
	Truncated bool `json:"truncated,omitempty"`

	// LastEvaluatedTime is the timestamp of the last status evaluation.
	// +optional
	LastEvaluatedTime metav1.Time `json:"lastEvaluatedTime,omitempty"`
}

// Condition types exposed on Echelon and ClusterEchelon.
const (
	ConditionReady       = "Ready"
	ConditionReconciling = "Reconciling"
	ConditionStalled     = "Stalled"
)

// Reason vocabulary for Ready / Reconciling / Stalled conditions and rollups.
//
// Two levels of aggregation share this vocabulary:
//   - Resource-level (per-member rollup): describes the population of
//     individual matched resources that contributed to the rollup.
//   - Member-level (owner-wide): describes the population of per-member
//     rollups that contributed to the owner Ready.
const (
	// Resource-level reasons (used on MemberRollup).
	ReasonAllResourcesReady   = "AllResourcesReady"
	ReasonResourcesNotReady   = "ResourcesNotReady"
	ReasonResourcesInProgress = "ResourcesInProgress"
	ReasonResourcesUnknown    = "ResourcesUnknown"

	// Member-level reasons (used on the owner Ready condition).
	ReasonAllMembersReady   = "AllMembersReady"
	ReasonMembersNotReady   = "MembersNotReady"
	ReasonMembersInProgress = "MembersInProgress"

	// Shared / structural.
	ReasonEmptySet               = "EmptySet"
	ReasonGVKNotEstablished      = "GVKNotEstablished"
	ReasonNamespaceScopeMismatch = "NamespaceScopeMismatch"
	ReasonDiscoveryFailed        = "DiscoveryFailed"
	ReasonWatchSetupFailed       = "WatchSetupFailed"

	// ReasonReconciling marks an in-flight reconcile. Reserved for a future
	// transient Reconciling=True patch; the current pipeline never writes it.
	ReasonReconciling = "Reconciling"
	// ReasonReconcileComplete marks a settled reconcile — the reason carried
	// on Reconciling=False (always) and Stalled=False (no-error path).
	ReasonReconcileComplete = "ReconcileComplete"
)

// Finalizer applied to Echelon and ClusterEchelon objects so the operator can
// release informer subscriptions before the object is deleted.
const Finalizer = "as-code.io/echelon-finalizer"
