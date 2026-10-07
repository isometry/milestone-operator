/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package cli holds the presentation layer of milestonectl: kind
// descriptors, row extraction, status selectors and the table, tree and
// structured printers. It carries no cobra or client construction so every
// printer can be tested from plain values.
package cli

import (
	"context"
	"fmt"
	"strings"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/membership"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Kind describes one of the operator's owner kinds.
type Kind struct {
	Kind       string
	Singular   string
	Plural     string
	ShortName  string
	Namespaced bool
}

var (
	MilestoneKind = Kind{
		Kind:       "Milestone",
		Singular:   "milestone",
		Plural:     "milestones",
		ShortName:  "mile",
		Namespaced: true,
	}
	ClusterMilestoneKind = Kind{
		Kind:      "ClusterMilestone",
		Singular:  "clustermilestone",
		Plural:    "clustermilestones",
		ShortName: "cmile",
	}
)

// Kinds lists the owner kinds in display order.
func Kinds() []Kind { return []Kind{MilestoneKind, ClusterMilestoneKind} }

// LookupKind resolves a singular, plural or short name, case-insensitively.
func LookupKind(name string) (Kind, bool) {
	name = strings.ToLower(name)
	for _, k := range Kinds() {
		if name == k.Singular || name == k.Plural || name == k.ShortName {
			return k, true
		}
	}
	return Kind{}, false
}

// GVR is the resource the kind is served at.
func (k Kind) GVR() schema.GroupVersionResource {
	return apiv1.GroupVersion.WithResource(k.Plural)
}

// Owner is a decoded Milestone or ClusterMilestone. Object keeps the raw
// form so structured output shows exactly what the server returned.
type Owner struct {
	Kind   Kind
	Object *unstructured.Unstructured

	milestone        *apiv1.Milestone
	clusterMilestone *apiv1.ClusterMilestone
}

// Decode converts u into the typed API object for k.
func (k Kind) Decode(u *unstructured.Unstructured) (Owner, error) {
	o := Owner{Kind: k, Object: u}
	var target any
	if k.Namespaced {
		o.milestone = &apiv1.Milestone{}
		target = o.milestone
	} else {
		o.clusterMilestone = &apiv1.ClusterMilestone{}
		target = o.clusterMilestone
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, target); err != nil {
		return Owner{}, fmt.Errorf("decode %s %s: %w", k.Kind, u.GetName(), err)
	}
	return o, nil
}

// Meta is the owner's object metadata.
func (o Owner) Meta() *metav1.ObjectMeta {
	if o.milestone != nil {
		return &o.milestone.ObjectMeta
	}
	return &o.clusterMilestone.ObjectMeta
}

// Status is the owner's recorded status.
func (o Owner) Status() *apiv1.MilestoneStatusBase {
	if o.milestone != nil {
		return &o.milestone.Status.MilestoneStatusBase
	}
	return &o.clusterMilestone.Status.MilestoneStatusBase
}

// Ref names the owner as Kind/namespace/name, or Kind/name when cluster-scoped.
func (o Owner) Ref() string {
	m := o.Meta()
	if m.Namespace == "" {
		return o.Kind.Kind + "/" + m.Name
	}
	return o.Kind.Kind + "/" + m.Namespace + "/" + m.Name
}

// Evaluate reports the owner's membership live.
func (o Owner) Evaluate(ctx context.Context, e *membership.Evaluator) (membership.Report, error) {
	if o.milestone != nil {
		return e.Milestone(ctx, o.milestone)
	}
	return e.ClusterMilestone(ctx, o.clusterMilestone)
}

// Normalize resolves the owner's dependencies exactly as the operator does.
func (o Owner) Normalize(ctx context.Context, r discovery.Resolver, ns membership.NamespaceLister) ([]membership.Dependency, []membership.Error) {
	if o.milestone != nil {
		return membership.NormalizeMilestone(ctx, r, o.milestone)
	}
	return membership.NormalizeClusterMilestone(ctx, r, ns, o.clusterMilestone)
}
