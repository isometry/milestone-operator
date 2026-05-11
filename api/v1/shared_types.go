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

// EmptySetPolicy controls how a target's Ready is reported when zero resources
// match its selector.
//
// +kubebuilder:validation:Enum=Unknown;Ready;NotReady
type EmptySetPolicy string

const (
	// EmptySetUnknown reports Ready=Unknown for a target with zero matches.
	// Default; safe for wave gates that should not advance on emptiness.
	EmptySetUnknown EmptySetPolicy = "Unknown"
	// EmptySetReady reports Ready=True for a target with zero matches.
	// Use when a wave should vacuously advance if nothing is selected.
	EmptySetReady EmptySetPolicy = "Ready"
	// EmptySetNotReady reports Ready=False for a target with zero matches.
	// Use when emptiness is itself a misconfiguration that should block.
	EmptySetNotReady EmptySetPolicy = "NotReady"
)

// TargetSpec selects a set of resources by GVK and label selector.
type TargetSpec struct {
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

// ClusterTargetSpec is TargetSpec extended with namespace selection.
// Exactly one of Namespaces or NamespaceSelector may be set; both empty means
// "all namespaces".
//
// +kubebuilder:validation:XValidation:rule="!(has(self.namespaces) && has(self.namespaceSelector))",message="namespaces and namespaceSelector are mutually exclusive"
type ClusterTargetSpec struct {
	TargetSpec `json:",inline"`

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
// are always populated; a zero value means no members fall in that bucket, not
// "not yet computed". The bucket names mirror sigs.k8s.io/cli-utils kstatus.
type Summary struct {
	// Total is the count of resources currently matched by the target's
	// selector. Total == sum of all the other buckets.
	Total int32 `json:"total"`
	// Current counts members whose kstatus is Current (steady-state ready).
	Current int32 `json:"current"`
	// InProgress counts members whose kstatus is InProgress (still converging).
	InProgress int32 `json:"inProgress"`
	// Failed counts members whose kstatus is Failed.
	Failed int32 `json:"failed"`
	// NotFound counts members whose kstatus is NotFound (selector matched a
	// name but the resource is absent).
	NotFound int32 `json:"notFound"`
	// Terminating counts members whose kstatus is Terminating (deletion in
	// progress).
	Terminating int32 `json:"terminating"`
	// Unknown counts members whose kstatus could not be determined.
	Unknown int32 `json:"unknown"`
}

// TargetRollup is the per-target aggregated readiness reported on the owner.
// One TargetRollup is produced for every entry in spec.targets, in the same
// order, regardless of whether discovery succeeded.
type TargetRollup struct {
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

	// Ready is True when every selected member is Current; False when any
	// member is Failed or NotFound; Unknown otherwise (or per emptySetPolicy
	// when no members match).
	// +kubebuilder:validation:Enum=True;False;Unknown
	Ready metav1.ConditionStatus `json:"ready"`

	// Reason is a short machine-readable code summarising why Ready has its
	// current value (e.g. AllMembersReady, MembersNotReady, EmptySet).
	// +optional
	Reason string `json:"reason,omitempty"`
	// Summary holds the per-kstatus-bucket counts that produced Ready.
	Summary Summary `json:"summary"`
}

// MemberStatus identifies an individual matched resource and the kstatus
// computed for it. Only members whose Status is not Current are surfaced on the
// owner (see EchelonStatusBase.NotReadyMembers).
type MemberStatus struct {
	// Group is the API group of the member resource. Empty for the core group.
	// +optional
	Group string `json:"group,omitempty"`
	// Version is the API version of the member resource.
	Version string `json:"version"`
	// Kind is the API Kind of the member resource.
	Kind string `json:"kind"`
	// Namespace is the namespace of the member resource. Empty when the
	// resource is cluster-scoped.
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// Name is the name of the member resource.
	Name string `json:"name"`

	// Status is the kstatus computed by sigs.k8s.io/cli-utils for this
	// member.
	// +kubebuilder:validation:Enum=Current;InProgress;Failed;NotFound;Terminating;Unknown
	Status string `json:"status"`
	// Reason is the kstatus reason for this member when one was provided by
	// sigs.k8s.io/cli-utils.
	// +optional
	Reason string `json:"reason,omitempty"`
	// Message is the kstatus message for this member when one was provided by
	// sigs.k8s.io/cli-utils.
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

	// Summary aggregates member counters across all targets.
	// +optional
	Summary Summary `json:"summary,omitempty"`

	// Targets carries per-target rollups in spec order.
	// +optional
	Targets []TargetRollup `json:"targets,omitempty"`

	// NotReadyMembers lists members whose kstatus is not Current. Capped to
	// avoid object-size explosions; Truncated indicates the cap was hit.
	// +optional
	NotReadyMembers []MemberStatus `json:"notReadyMembers,omitempty"`

	// Truncated is true when NotReadyMembers was capped.
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
const (
	ReasonAllMembersReady        = "AllMembersReady"
	ReasonAllTargetsReady        = "AllTargetsReady"
	ReasonMembersNotReady        = "MembersNotReady"
	ReasonTargetsNotReady        = "TargetsNotReady"
	ReasonMembersInProgress      = "MembersInProgress"
	ReasonMembersUnknown         = "MembersUnknown"
	ReasonTargetsInProgress      = "TargetsInProgress"
	ReasonEmptySet               = "EmptySet"
	ReasonGVKNotEstablished      = "GVKNotEstablished"
	ReasonNamespaceScopeMismatch = "NamespaceScopeMismatch"
	ReasonDiscoveryFailed        = "DiscoveryFailed"
	ReasonWatchSetupFailed       = "WatchSetupFailed"
	ReasonReconciling            = "Reconciling"
)

// Finalizer applied to Echelon and ClusterEchelon objects so the operator can
// release informer subscriptions before the object is deleted.
const Finalizer = "as-code.io/echelon-finalizer"
