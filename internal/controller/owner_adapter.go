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

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	"github.com/isometry/echelon-operator/internal/discovery"
	"github.com/isometry/echelon-operator/internal/watcher"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NormalizedTarget is the discovery- and selector-resolved view of a single
// spec.targets[] entry, ready to feed the registry and the lister.
type NormalizedTarget struct {
	Index            int
	GVK              schema.GroupVersionKind
	Scope            apimeta.RESTScopeName
	Selector         labels.Selector
	NamespaceMatcher func(namespace string) bool
	EmptySetPolicy   apiv1.EmptySetPolicy
}

// TargetError is a structural failure tied to a single target. The reconciler
// surfaces these as Stalled with the carried Reason; reconcile continues for
// the remaining targets.
type TargetError struct {
	Index   int
	Group   string
	Version string
	Kind    string
	Reason  string
	Err     error
}

func (e TargetError) Error() string { return e.Err.Error() }
func (e TargetError) Unwrap() error { return e.Err }

// OwnerAdapter abstracts the differences between Echelon and ClusterEchelon so
// the reconcile pipeline can serve both with one implementation.
type OwnerAdapter interface {
	// Object returns the wrapped resource (used for finalizer mutation and the
	// status patch). Returning a typed object is fine; callers treat it as
	// client.Object.
	Object() client.Object

	// OwnerKey identifies this object in the watcher registry.
	OwnerKey() watcher.OwnerKey

	// Targets normalizes spec.targets[] into NormalizedTarget plus per-target
	// errors that the reconciler should surface as Stalled reasons.
	Targets(ctx context.Context, dr discovery.Resolver) ([]NormalizedTarget, []TargetError)

	// Status returns a pointer to the embedded EchelonStatusBase so the
	// reconciler can mutate it in place before patching.
	Status() *apiv1.EchelonStatusBase

	// PatchStatus persists the in-memory status to the API server. The
	// reconciler is responsible for deciding whether the patch is a no-op.
	PatchStatus(ctx context.Context, c client.Client) error
}

// RegistryAPI is the subset of *watcher.Registry the reconciler depends on.
// Defining it as an interface keeps the reconciler unit-testable with a fake.
type RegistryAPI interface {
	Subscribe(gvk schema.GroupVersionKind, scope apimeta.RESTScopeName, sub watcher.Subscriber) error
	Unsubscribe(gvk schema.GroupVersionKind, owner watcher.OwnerKey)
	UnsubscribeAll(owner watcher.OwnerKey)
	List(gvk schema.GroupVersionKind) ([]*unstructured.Unstructured, error)
	GVKsByOwner(owner watcher.OwnerKey) []schema.GroupVersionKind
}
