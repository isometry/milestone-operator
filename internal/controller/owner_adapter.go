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

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/watcher"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NormalizedDependency is the discovery- and selector-resolved view of a
// single spec.dependsOn[] entry, ready to feed the registry and the lister.
type NormalizedDependency struct {
	Name             string
	GVK              schema.GroupVersionKind
	Scope            apimeta.RESTScopeName
	Selector         labels.Selector
	NamespaceMatcher func(namespace string) bool
	EmptySetPolicy   apiv1.EmptySetPolicy
}

// DependencyError is a structural failure tied to a single dependency. The
// reconciler surfaces these as Stalled with the carried Reason; reconcile
// continues for the remaining dependencies.
type DependencyError struct {
	Name    string
	Group   string
	Version string
	Kind    string
	Reason  string
	Err     error
}

func (e DependencyError) Error() string { return e.Err.Error() }
func (e DependencyError) Unwrap() error { return e.Err }

// OwnerAdapter abstracts the differences between Milestone and
// ClusterMilestone so the reconcile pipeline can serve both with one
// implementation. The interface is intentionally non-generic: the typed
// owner flows through Reconciler[T] directly, and the adapter only exposes
// owner-agnostic shapes (NormalizedDependency, DependencyError,
// *MilestoneStatusBase).
type OwnerAdapter interface {
	// OwnerKey identifies this object in the watcher registry.
	OwnerKey() watcher.OwnerKey

	// Dependencies normalises spec.dependsOn into NormalizedDependency plus
	// per-entry errors that the reconciler should surface as Stalled
	// reasons. Implementations must return dependencies in spec order so
	// downstream iteration is deterministic.
	Dependencies(ctx context.Context, dr discovery.Resolver) ([]NormalizedDependency, []DependencyError)

	// Status returns a pointer to the embedded MilestoneStatusBase so the
	// reconciler can mutate it in place before patching.
	Status() *apiv1.MilestoneStatusBase

	// PatchStatus persists the in-memory status to the API server. The
	// reconciler is responsible for deciding whether the patch is a no-op.
	PatchStatus(ctx context.Context, c client.Client) error
}

// RegistryAPI is the subset of *watcher.Registry the reconciler depends on.
// Defining it as an interface keeps the reconciler unit-testable with a fake.
type RegistryAPI interface {
	Subscribe(ctx context.Context, gvk schema.GroupVersionKind, scope apimeta.RESTScopeName, sub watcher.Subscriber) error
	Unsubscribe(gvk schema.GroupVersionKind, owner watcher.OwnerKey)
	UnsubscribeAll(owner watcher.OwnerKey)
	List(gvk schema.GroupVersionKind) ([]*unstructured.Unstructured, error)
	GVKsByOwner(owner watcher.OwnerKey) []schema.GroupVersionKind
}
