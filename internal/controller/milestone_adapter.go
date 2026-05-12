/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"errors"
	"fmt"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/watcher"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MilestoneAdapter implements OwnerAdapter for the namespaced Milestone CRD.
type MilestoneAdapter struct {
	Milestone *apiv1.Milestone
}

// NewMilestoneAdapter constructs an adapter for m.
func NewMilestoneAdapter(m *apiv1.Milestone) OwnerAdapter {
	return &MilestoneAdapter{Milestone: m}
}

// OwnerKey returns the watcher.OwnerKey for this Milestone.
func (a *MilestoneAdapter) OwnerKey() watcher.OwnerKey {
	return watcher.OwnerKey{
		Kind:      "Milestone",
		Namespace: a.Milestone.Namespace,
		Name:      a.Milestone.Name,
	}
}

// Dependencies normalises spec.dependsOn; per-entry discovery failures are
// returned as DependencyError, not as a fatal error, so the reconciler can
// proceed with the resolvable subset. Entries are returned in spec order.
func (a *MilestoneAdapter) Dependencies(ctx context.Context, dr discovery.Resolver) ([]NormalizedDependency, []DependencyError) {
	if dr == nil {
		return nil, []DependencyError{{Reason: apiv1.ReasonDiscoveryFailed, Err: errors.New("nil discovery resolver")}}
	}
	ns := a.Milestone.Namespace
	matcher := func(target string) bool { return target == ns }

	deps := a.Milestone.Spec.DependsOn
	out := make([]NormalizedDependency, 0, len(deps))
	var errs []DependencyError
	for i := range deps {
		d := &deps[i]
		gvk, scope, err := dr.Resolve(ctx, d.Target.Group, d.Target.Kind, d.Target.Version)
		if err != nil {
			errs = append(errs, DependencyError{
				Name:   d.Name,
				Group:  d.Target.Group,
				Kind:   d.Target.Kind,
				Reason: apiv1.ReasonGVKNotEstablished,
				Err:    err,
			})
			continue
		}
		// Milestones are namespaced and only ever observe resources in
		// their own namespace. Targeting a cluster-scoped kind would
		// silently produce an empty set after the namespace matcher.
		if scope != apimeta.RESTScopeNameNamespace {
			errs = append(errs, DependencyError{
				Name:   d.Name,
				Group:  gvk.Group,
				Kind:   gvk.Kind,
				Reason: apiv1.ReasonNamespaceScopeMismatch,
				Err:    fmt.Errorf("kind %q is cluster-scoped; Milestone can only target namespaced resources (use ClusterMilestone)", gvk.Kind),
			})
			continue
		}
		sel, err := labelSelectorOrEverything(d.Target.Selector)
		if err != nil {
			errs = append(errs, DependencyError{
				Name:   d.Name,
				Group:  gvk.Group,
				Kind:   gvk.Kind,
				Reason: apiv1.ReasonDiscoveryFailed,
				Err:    err,
			})
			continue
		}
		out = append(out, NormalizedDependency{
			Name:             d.Name,
			GVK:              gvk,
			Scope:            scope,
			Selector:         sel,
			NamespaceMatcher: matcher,
			EmptySetPolicy:   d.EmptySetPolicy,
		})
	}
	return out, errs
}

// Status returns the embedded MilestoneStatusBase.
func (a *MilestoneAdapter) Status() *apiv1.MilestoneStatusBase {
	return &a.Milestone.Status.MilestoneStatusBase
}

// PatchStatus persists the in-memory status using the status subresource.
func (a *MilestoneAdapter) PatchStatus(ctx context.Context, c client.Client) error {
	return c.Status().Update(ctx, a.Milestone)
}

func labelSelectorOrEverything(ls *metav1.LabelSelector) (labels.Selector, error) {
	if ls == nil {
		return labels.Everything(), nil
	}
	return metav1.LabelSelectorAsSelector(ls)
}
