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

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	"github.com/isometry/echelon-operator/internal/discovery"
	"github.com/isometry/echelon-operator/internal/watcher"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterEchelonAdapter implements OwnerAdapter for the cluster-scoped CRD.
// Each NewClusterEchelonAdapter binds a client.Client so namespaceSelector
// members can list Namespaces during normalisation.
type ClusterEchelonAdapter struct {
	ClusterEchelon *apiv1.ClusterEchelon
	Client         client.Client
}

// NewClusterEchelonAdapterFactory returns a NewAdapter function bound to c.
func NewClusterEchelonAdapterFactory(c client.Client) func(*apiv1.ClusterEchelon) OwnerAdapter {
	return func(ce *apiv1.ClusterEchelon) OwnerAdapter {
		return &ClusterEchelonAdapter{
			ClusterEchelon: ce,
			Client:         c,
		}
	}
}

// OwnerKey returns the watcher.OwnerKey for this ClusterEchelon.
func (a *ClusterEchelonAdapter) OwnerKey() watcher.OwnerKey {
	return watcher.OwnerKey{Kind: "ClusterEchelon", Name: a.ClusterEchelon.Name}
}

// Members normalises spec.members. Per-member failures (discovery or
// scope/selector mismatches) become MemberErrors that the reconciler maps to
// Stalled reasons; the resolvable subset still flows through the pipeline.
// Members are returned sorted by name for deterministic downstream behaviour.
func (a *ClusterEchelonAdapter) Members(ctx context.Context, dr discovery.Resolver) ([]NormalizedMember, []MemberError) {
	if dr == nil {
		return nil, []MemberError{{Reason: apiv1.ReasonDiscoveryFailed, Err: errors.New("nil discovery resolver")}}
	}

	names := sortedMemberKeys(a.ClusterEchelon.Spec.Members)
	out := make([]NormalizedMember, 0, len(names))
	var errs []MemberError

	for _, name := range names {
		m := a.ClusterEchelon.Spec.Members[name]
		gvk, scope, err := dr.Resolve(ctx, m.Group, m.Kind, m.Version)
		if err != nil {
			errs = append(errs, MemberError{
				Name:   name,
				Group:  m.Group,
				Kind:   m.Kind,
				Reason: apiv1.ReasonGVKNotEstablished,
				Err:    err,
			})
			continue
		}

		hasNamespaceFilter := len(m.Namespaces) > 0 || m.NamespaceSelector != nil

		// Cluster-scoped resources cannot carry namespace filters.
		if scope == apimeta.RESTScopeNameRoot && hasNamespaceFilter {
			errs = append(errs, MemberError{
				Name:   name,
				Group:  gvk.Group,
				Kind:   gvk.Kind,
				Reason: apiv1.ReasonNamespaceScopeMismatch,
				Err:    fmt.Errorf("kind %q is cluster-scoped; namespaces and namespaceSelector are forbidden", gvk.Kind),
			})
			continue
		}

		// XOR is enforced by CRD CEL but we re-check defensively.
		if len(m.Namespaces) > 0 && m.NamespaceSelector != nil {
			errs = append(errs, MemberError{
				Name:   name,
				Group:  gvk.Group,
				Kind:   gvk.Kind,
				Reason: apiv1.ReasonNamespaceScopeMismatch,
				Err:    errors.New("namespaces and namespaceSelector are mutually exclusive"),
			})
			continue
		}

		matcher, merr := a.buildNamespaceMatcher(ctx, m.Namespaces, m.NamespaceSelector)
		if merr != nil {
			errs = append(errs, MemberError{
				Name:   name,
				Group:  gvk.Group,
				Kind:   gvk.Kind,
				Reason: apiv1.ReasonDiscoveryFailed,
				Err:    merr,
			})
			continue
		}

		sel, err := labelSelectorOrEverything(m.Selector)
		if err != nil {
			errs = append(errs, MemberError{
				Name:   name,
				Group:  gvk.Group,
				Kind:   gvk.Kind,
				Reason: apiv1.ReasonDiscoveryFailed,
				Err:    err,
			})
			continue
		}

		out = append(out, NormalizedMember{
			Name:             name,
			GVK:              gvk,
			Scope:            scope,
			Selector:         sel,
			NamespaceMatcher: matcher,
			EmptySetPolicy:   m.EmptySetPolicy,
		})
	}
	return out, errs
}

func (a *ClusterEchelonAdapter) buildNamespaceMatcher(ctx context.Context, names []string, selector *metav1.LabelSelector) (func(string) bool, error) {
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

// Status returns the embedded EchelonStatusBase.
func (a *ClusterEchelonAdapter) Status() *apiv1.EchelonStatusBase {
	return &a.ClusterEchelon.Status.EchelonStatusBase
}

// PatchStatus persists the in-memory status using the status subresource.
func (a *ClusterEchelonAdapter) PatchStatus(ctx context.Context, c client.Client) error {
	return c.Status().Update(ctx, a.ClusterEchelon)
}
