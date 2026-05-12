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
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterMilestoneAdapter implements OwnerAdapter for the cluster-scoped CRD.
// Each instance binds a client.Client so namespaceSelector dependencies can
// list Namespaces during normalisation.
type ClusterMilestoneAdapter struct {
	ClusterMilestone *apiv1.ClusterMilestone
	Client           client.Client
}

// NewClusterMilestoneAdapterFactory returns a NewAdapter function bound to c.
func NewClusterMilestoneAdapterFactory(c client.Client) func(*apiv1.ClusterMilestone) OwnerAdapter {
	return func(cm *apiv1.ClusterMilestone) OwnerAdapter {
		return &ClusterMilestoneAdapter{
			ClusterMilestone: cm,
			Client:           c,
		}
	}
}

// OwnerKey returns the watcher.OwnerKey for this ClusterMilestone.
func (a *ClusterMilestoneAdapter) OwnerKey() watcher.OwnerKey {
	return watcher.OwnerKey{Kind: "ClusterMilestone", Name: a.ClusterMilestone.Name}
}

// Dependencies normalises spec.dependsOn. Per-entry failures (discovery or
// scope/selector mismatches) become DependencyErrors that the reconciler
// maps to Stalled reasons; the resolvable subset still flows through the
// pipeline. Entries are returned in spec order.
func (a *ClusterMilestoneAdapter) Dependencies(ctx context.Context, dr discovery.Resolver) ([]NormalizedDependency, []DependencyError) {
	if dr == nil {
		return nil, []DependencyError{{Reason: apiv1.ReasonDiscoveryFailed, Err: errors.New("nil discovery resolver")}}
	}

	deps := a.ClusterMilestone.Spec.DependsOn
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

		hasNamespaceFilter := len(d.Target.Namespaces) > 0 || d.Target.NamespaceSelector != nil

		// Cluster-scoped resources cannot carry namespace filters.
		if scope == apimeta.RESTScopeNameRoot && hasNamespaceFilter {
			errs = append(errs, DependencyError{
				Name:   d.Name,
				Group:  gvk.Group,
				Kind:   gvk.Kind,
				Reason: apiv1.ReasonNamespaceScopeMismatch,
				Err:    fmt.Errorf("kind %q is cluster-scoped; namespaces and namespaceSelector are forbidden", gvk.Kind),
			})
			continue
		}

		// XOR is enforced by CRD CEL but we re-check defensively.
		if len(d.Target.Namespaces) > 0 && d.Target.NamespaceSelector != nil {
			errs = append(errs, DependencyError{
				Name:   d.Name,
				Group:  gvk.Group,
				Kind:   gvk.Kind,
				Reason: apiv1.ReasonNamespaceScopeMismatch,
				Err:    errors.New("namespaces and namespaceSelector are mutually exclusive"),
			})
			continue
		}

		matcher, merr := a.buildNamespaceMatcher(ctx, d.Target.Namespaces, d.Target.NamespaceSelector)
		if merr != nil {
			errs = append(errs, DependencyError{
				Name:   d.Name,
				Group:  gvk.Group,
				Kind:   gvk.Kind,
				Reason: apiv1.ReasonDiscoveryFailed,
				Err:    merr,
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

func (a *ClusterMilestoneAdapter) buildNamespaceMatcher(ctx context.Context, names []string, selector *metav1.LabelSelector) (func(string) bool, error) {
	if len(names) > 0 {
		set := make(map[string]struct{}, len(names))
		for _, n := range names {
			set[n] = struct{}{}
		}
		return func(ns string) bool { _, ok := set[ns]; return ok }, nil
	}
	if selector != nil {
		sel, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			return nil, fmt.Errorf("invalid namespaceSelector: %w", err)
		}
		nsList := &corev1.NamespaceList{}
		if err := a.Client.List(ctx, nsList, &client.ListOptions{LabelSelector: sel}); err != nil {
			return nil, fmt.Errorf("list namespaces: %w", err)
		}
		set := make(map[string]struct{}, len(nsList.Items))
		for _, ns := range nsList.Items {
			set[ns.Name] = struct{}{}
		}
		return func(ns string) bool { _, ok := set[ns]; return ok }, nil
	}
	return nil, nil
}

// Status returns the embedded MilestoneStatusBase.
func (a *ClusterMilestoneAdapter) Status() *apiv1.MilestoneStatusBase {
	return &a.ClusterMilestone.Status.MilestoneStatusBase
}

// PatchStatus persists the in-memory status using the status subresource.
func (a *ClusterMilestoneAdapter) PatchStatus(ctx context.Context, c client.Client) error {
	return c.Status().Update(ctx, a.ClusterMilestone)
}
