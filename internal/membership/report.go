/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package membership

import (
	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Report is a live evaluation of one owner alongside what the operator last
// recorded for it.
type Report struct {
	Owner    OwnerRef       `json:"owner"`
	Recorded RecordedStatus `json:"recorded"`

	Ready   metav1.ConditionStatus `json:"ready"`
	Reason  string                 `json:"reason"`
	Message string                 `json:"message,omitempty"`
	Summary apiv1.Summary          `json:"summary"`

	// Dependencies follow spec order.
	Dependencies []DependencyReport `json:"dependencies"`
	EvaluatedAt  metav1.Time        `json:"evaluatedAt"`
}

// OwnerRef identifies the evaluated Milestone or ClusterMilestone.
type OwnerRef struct {
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace,omitempty"`
	Name       string `json:"name"`
	Generation int64  `json:"generation"`
}

// RecordedStatus is the owner status the operator last wrote.
type RecordedStatus struct {
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration"`
	LastEvaluatedTime  metav1.Time        `json:"lastEvaluatedTime,omitzero"`
	// Stale means the operator has not yet reconciled the current generation.
	Stale bool `json:"stale"`
}

// TargetRef describes a dependency's target as written in the spec; Version
// is the requested one and may be empty. Selectors are rendered as strings.
type TargetRef struct {
	Group             string   `json:"group,omitempty"`
	Version           string   `json:"version,omitempty"`
	Kind              string   `json:"kind"`
	Selector          string   `json:"selector,omitempty"`
	Namespaces        []string `json:"namespaces,omitempty"`
	NamespaceSelector string   `json:"namespaceSelector,omitempty"`
}

// DependencyReport is the live evaluation of one spec.dependsOn[] entry.
type DependencyReport struct {
	Name   string    `json:"name"`
	Target TargetRef `json:"target"`
	// Status is what the operator would record for this dependency now.
	Status   apiv1.DependencyStatus  `json:"status"`
	Recorded *apiv1.DependencyStatus `json:"recorded,omitempty"`
	// Members are sorted by namespace/name.
	Members []Member `json:"members,omitempty"`
	Error   string   `json:"error,omitempty"`
	// Forbidden marks a list the caller's credentials were denied. The
	// operator's own RBAC may still allow it, so Status reports ListFailed
	// rather than a verdict.
	Forbidden bool `json:"forbidden,omitempty"`
	// Drift means the recorded entry for the current generation disagrees
	// with the live Ready or Reason.
	Drift bool `json:"drift,omitempty"`
}

// Member is one resource admitted by a dependency.
type Member struct {
	status.Resource `json:",inline"`
	// Blocking means the operator would list this resource in
	// status.notReadyResources.
	Blocking bool `json:"blocking"`
}
