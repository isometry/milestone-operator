/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package membership decides which resources belong to each dependency of a
// Milestone or ClusterMilestone, and how per-dependency failures feed the
// owner verdict. It is the single source of truth shared by the reconciler
// and milestonectl, so the CLI can never disagree with the operator about
// what a spec means. It deliberately avoids controller-runtime.
package membership

import (
	"context"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Dependency is the discovery- and selector-resolved view of a single
// spec.dependsOn[] entry.
type Dependency struct {
	Name     string
	GVK      schema.GroupVersionKind
	Scope    apimeta.RESTScopeName
	Selector labels.Selector
	// NamespaceMatcher is nil when every namespace is admitted.
	NamespaceMatcher func(namespace string) bool
	EmptySetPolicy   apiv1.EmptySetPolicy
	SuspendPolicy    apiv1.SuspendPolicy
}

// Admits reports whether an object in namespace carrying objLabels is a
// member of d.
func (d Dependency) Admits(namespace string, objLabels labels.Set) bool {
	if d.NamespaceMatcher != nil && !d.NamespaceMatcher(namespace) {
		return false
	}
	if d.Selector != nil && !d.Selector.Matches(objLabels) {
		return false
	}
	return true
}

// Matches returns the dependencies in deps that admit an object of kind gk in
// namespace carrying lbls. Versions are ignored: an object is the same member
// whichever served version it was read at.
func Matches(deps []Dependency, gk schema.GroupKind, namespace string, lbls labels.Set) []Dependency {
	var out []Dependency
	for _, d := range deps {
		if d.GVK.GroupKind() == gk && d.Admits(namespace, lbls) {
			out = append(out, d)
		}
	}
	return out
}

// Error is a structural failure tied to a single dependency. Reason is a
// DependencyStatus.Reason value; Name is empty only for owner-wide failures.
type Error struct {
	Name    string
	Group   string
	Version string
	Kind    string
	Reason  string
	Err     error
}

func (e Error) Error() string { return e.Err.Error() }
func (e Error) Unwrap() error { return e.Err }

// NewError builds an Error for a dependency whose GVK has already been
// resolved, carrying the resolved version so its FailedRollup does not write
// an empty version into status.dependsOn.
func NewError(name string, gvk schema.GroupVersionKind, reason string, err error) Error {
	return Error{
		Name:    name,
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind,
		Reason:  reason,
		Err:     err,
	}
}

// NamespaceLister returns the names of the Namespaces matching sel. It lets
// callers supply whichever Kubernetes client they hold.
type NamespaceLister func(ctx context.Context, sel labels.Selector) ([]string, error)

// FailedRollup forces Ready=Unknown so emptySetPolicy can't promote a missing
// informer / failed list to Ready=True.
func FailedRollup(name string, gvk schema.GroupVersionKind, reason string) apiv1.DependencyStatus {
	return apiv1.DependencyStatus{
		Name:    name,
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind,
		Ready:   metav1.ConditionUnknown,
		Reason:  reason,
	}
}

// Rollups returns perDep plus a Ready=Unknown stand-in for every named error
// whose dependency has no rollup yet, ready for status.ReduceOwner and
// status.SummarizeOwner. Normalisation failures never reach evaluation, so
// without the stand-in they would vanish from status.dependsOn and could let
// the owner report Ready=True. The first error for a name wins. perDep is not
// modified.
func Rollups(perDep map[string]apiv1.DependencyStatus, errs []Error) map[string]apiv1.DependencyStatus {
	out := make(map[string]apiv1.DependencyStatus, len(perDep)+len(errs))
	for k, v := range perDep {
		out[k] = v
	}
	for _, e := range errs {
		if e.Name == "" {
			continue
		}
		if _, ok := out[e.Name]; ok {
			continue
		}
		out[e.Name] = FailedRollup(e.Name, schema.GroupVersionKind{
			Group:   e.Group,
			Version: e.Version,
			Kind:    e.Kind,
		}, e.Reason)
	}
	return out
}
