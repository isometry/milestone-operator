/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package membership

import (
	"context"
	"errors"
	"fmt"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Error classification lives here — exactly once — so Milestone and
// ClusterMilestone can never drift apart in how they report the same failure.
// Per-entry checks run in a fixed order and the first failure wins.

// NormalizeMilestone normalises m.spec.dependsOn in spec order. Per-entry
// failures are returned as Errors, not a fatal error, so callers can proceed
// with the resolvable subset.
func NormalizeMilestone(ctx context.Context, dr discovery.Resolver, m *apiv1.Milestone) ([]Dependency, []Error) {
	if dr == nil {
		return nil, []Error{nilResolverError()}
	}
	ns := m.Namespace
	matcher := func(target string) bool { return target == ns }

	deps := m.Spec.DependsOn
	out := make([]Dependency, 0, len(deps))
	var errs []Error
	for i := range deps {
		d := &deps[i]
		gvk, scope, derr := resolveTarget(ctx, dr, d.Name, d.Target)
		if derr != nil {
			errs = append(errs, *derr)
			continue
		}
		// Milestones only ever observe their own namespace, so a
		// cluster-scoped kind would silently produce an empty set.
		if scope != apimeta.RESTScopeNameNamespace {
			errs = append(errs, NewError(d.Name, gvk, apiv1.ReasonNamespaceScopeMismatch,
				fmt.Errorf("kind %q is cluster-scoped; Milestone can only target namespaced resources (use ClusterMilestone)", gvk.Kind)))
			continue
		}
		sel, derr := parseSelector(d.Name, gvk, d.Target.Selector)
		if derr != nil {
			errs = append(errs, *derr)
			continue
		}
		out = append(out, Dependency{
			Name:             d.Name,
			GVK:              gvk,
			Scope:            scope,
			Selector:         sel,
			NamespaceMatcher: matcher,
			EmptySetPolicy:   d.EmptySetPolicy,
			SuspendPolicy:    d.SuspendPolicy,
		})
	}
	return out, errs
}

// NormalizeClusterMilestone normalises cm.spec.dependsOn in spec order.
// listNamespaces is consulted only for namespaceSelector targets, at most
// once per distinct selector.
func NormalizeClusterMilestone(ctx context.Context, dr discovery.Resolver, listNamespaces NamespaceLister, cm *apiv1.ClusterMilestone) ([]Dependency, []Error) {
	if dr == nil {
		return nil, []Error{nilResolverError()}
	}

	deps := cm.Spec.DependsOn
	out := make([]Dependency, 0, len(deps))
	var errs []Error
	// Several dependencies commonly share one namespaceSelector, and each
	// selector costs a Namespace list.
	matcherCache := make(map[string]func(string) bool)

	for i := range deps {
		d := &deps[i]
		gvk, scope, derr := resolveTarget(ctx, dr, d.Name, d.Target.TargetSpec)
		if derr != nil {
			errs = append(errs, *derr)
			continue
		}

		hasNamespaceFilter := len(d.Target.Namespaces) > 0 || d.Target.NamespaceSelector != nil
		if scope == apimeta.RESTScopeNameRoot && hasNamespaceFilter {
			errs = append(errs, NewError(d.Name, gvk, apiv1.ReasonNamespaceScopeMismatch,
				fmt.Errorf("kind %q is cluster-scoped; namespaces and namespaceSelector are forbidden", gvk.Kind)))
			continue
		}

		// XOR is enforced by CRD CEL but we re-check defensively.
		if len(d.Target.Namespaces) > 0 && d.Target.NamespaceSelector != nil {
			errs = append(errs, NewError(d.Name, gvk, apiv1.ReasonNamespaceScopeMismatch,
				errors.New("namespaces and namespaceSelector are mutually exclusive")))
			continue
		}

		matcher, merr := buildNamespaceMatcher(ctx, listNamespaces, d.Target.Namespaces, d.Target.NamespaceSelector, matcherCache)
		if merr != nil {
			errs = append(errs, NewError(d.Name, gvk, apiv1.ReasonDiscoveryFailed, merr))
			continue
		}

		sel, derr := parseSelector(d.Name, gvk, d.Target.Selector)
		if derr != nil {
			errs = append(errs, *derr)
			continue
		}

		out = append(out, Dependency{
			Name:             d.Name,
			GVK:              gvk,
			Scope:            scope,
			Selector:         sel,
			NamespaceMatcher: matcher,
			EmptySetPolicy:   d.EmptySetPolicy,
			SuspendPolicy:    d.SuspendPolicy,
		})
	}
	return out, errs
}

func nilResolverError() Error {
	return Error{Reason: apiv1.ReasonDiscoveryFailed, Err: errors.New("nil discovery resolver")}
}

// resolveTarget classifies resolution failures: a discovery outage is
// DiscoveryUnavailable, an answered "no such group/kind" is GVKNotEstablished.
func resolveTarget(ctx context.Context, dr discovery.Resolver, name string, t apiv1.TargetSpec) (schema.GroupVersionKind, apimeta.RESTScopeName, *Error) {
	gvk, scope, err := dr.Resolve(ctx, t.Group, t.Kind, t.Version)
	if err != nil {
		reason := apiv1.ReasonGVKNotEstablished
		if errors.Is(err, discovery.ErrDiscoveryUnavailable) {
			reason = apiv1.ReasonDiscoveryUnavailable
		}
		return schema.GroupVersionKind{}, "", &Error{
			Name:    name,
			Group:   t.Group,
			Version: t.Version, // resolution failed: best we have is the requested version
			Kind:    t.Kind,
			Reason:  reason,
			Err:     err,
		}
	}
	return gvk, scope, nil
}

func parseSelector(name string, gvk schema.GroupVersionKind, ls *metav1.LabelSelector) (labels.Selector, *Error) {
	sel, err := labelSelectorOrEverything(ls)
	if err != nil {
		e := NewError(name, gvk, apiv1.ReasonDiscoveryFailed, err)
		return nil, &e
	}
	return sel, nil
}

func labelSelectorOrEverything(ls *metav1.LabelSelector) (labels.Selector, error) {
	if ls == nil {
		return labels.Everything(), nil
	}
	return metav1.LabelSelectorAsSelector(ls)
}

// buildNamespaceMatcher returns nil (all namespaces) when neither filter is set.
func buildNamespaceMatcher(ctx context.Context, listNamespaces NamespaceLister, names []string, selector *metav1.LabelSelector, cache map[string]func(string) bool) (func(string) bool, error) {
	if len(names) > 0 {
		return setMatcher(names), nil
	}
	if selector == nil {
		return nil, nil
	}
	sel, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return nil, fmt.Errorf("invalid namespaceSelector: %w", err)
	}
	key := sel.String()
	if m, ok := cache[key]; ok {
		return m, nil
	}
	if listNamespaces == nil {
		return nil, errors.New("list namespaces: no namespace lister configured")
	}
	matched, err := listNamespaces(ctx, sel)
	if err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	m := setMatcher(matched)
	cache[key] = m
	return m, nil
}

func setMatcher(names []string) func(string) bool {
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[n] = struct{}{}
	}
	return func(ns string) bool { _, ok := set[ns]; return ok }
}
