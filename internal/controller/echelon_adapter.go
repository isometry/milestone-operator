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
	"sort"

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	"github.com/isometry/echelon-operator/internal/discovery"
	"github.com/isometry/echelon-operator/internal/watcher"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EchelonAdapter implements OwnerAdapter for the namespaced Echelon CRD.
type EchelonAdapter struct {
	Echelon *apiv1.Echelon
}

// NewEchelonAdapter constructs an adapter for e.
func NewEchelonAdapter(e *apiv1.Echelon) OwnerAdapter {
	return &EchelonAdapter{Echelon: e}
}

// OwnerKey returns the watcher.OwnerKey for this Echelon.
func (a *EchelonAdapter) OwnerKey() watcher.OwnerKey {
	return watcher.OwnerKey{
		Kind:      "Echelon",
		Namespace: a.Echelon.Namespace,
		Name:      a.Echelon.Name,
	}
}

// Members normalises spec.members; per-member discovery failures are returned
// as MemberError, not as a fatal error, so the reconciler can proceed with the
// resolvable subset. Members are returned sorted by name for deterministic
// downstream behaviour.
func (a *EchelonAdapter) Members(ctx context.Context, dr discovery.Resolver) ([]NormalizedMember, []MemberError) {
	if dr == nil {
		return nil, []MemberError{{Reason: apiv1.ReasonDiscoveryFailed, Err: errors.New("nil discovery resolver")}}
	}
	ns := a.Echelon.Namespace
	matcher := func(target string) bool { return target == ns }

	names := sortedMemberKeys(a.Echelon.Spec.Members)
	out := make([]NormalizedMember, 0, len(names))
	var errs []MemberError
	for _, name := range names {
		m := a.Echelon.Spec.Members[name]
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
		// Echelons are namespaced and only ever observe resources in their
		// own namespace. Targeting a cluster-scoped kind would silently
		// produce an empty set after the namespace matcher; surface the
		// configuration error instead.
		if scope != apimeta.RESTScopeNameNamespace {
			errs = append(errs, MemberError{
				Name:   name,
				Group:  gvk.Group,
				Kind:   gvk.Kind,
				Reason: apiv1.ReasonNamespaceScopeMismatch,
				Err:    fmt.Errorf("kind %q is cluster-scoped; Echelon can only target namespaced resources (use ClusterEchelon)", gvk.Kind),
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

// Status returns the embedded EchelonStatusBase.
func (a *EchelonAdapter) Status() *apiv1.EchelonStatusBase {
	return &a.Echelon.Status.EchelonStatusBase
}

// PatchStatus persists the in-memory status using the status subresource.
func (a *EchelonAdapter) PatchStatus(ctx context.Context, c client.Client) error {
	return c.Status().Update(ctx, a.Echelon)
}

func labelSelectorOrEverything(ls *metav1.LabelSelector) (labels.Selector, error) {
	if ls == nil {
		return labels.Everything(), nil
	}
	return metav1.LabelSelectorAsSelector(ls)
}

func sortedMemberKeys[V any](m map[string]V) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
