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

// NormalizedMember is the discovery- and selector-resolved view of a single
// spec.members[name] entry, ready to feed the registry and the lister.
type NormalizedMember struct {
	Name             string
	GVK              schema.GroupVersionKind
	Scope            apimeta.RESTScopeName
	Selector         labels.Selector
	NamespaceMatcher func(namespace string) bool
	EmptySetPolicy   apiv1.EmptySetPolicy
}

// MemberError is a structural failure tied to a single member. The reconciler
// surfaces these as Stalled with the carried Reason; reconcile continues for
// the remaining members.
type MemberError struct {
	Name    string
	Group   string
	Version string
	Kind    string
	Reason  string
	Err     error
}

func (e MemberError) Error() string { return e.Err.Error() }
func (e MemberError) Unwrap() error { return e.Err }

// OwnerAdapter abstracts the differences between Echelon and ClusterEchelon so
// the reconcile pipeline can serve both with one implementation. The interface
// is intentionally non-generic: the typed owner flows through Reconciler[T]
// directly, and the adapter only exposes owner-agnostic shapes
// (NormalizedMember, MemberError, *EchelonStatusBase).
type OwnerAdapter interface {
	// OwnerKey identifies this object in the watcher registry.
	OwnerKey() watcher.OwnerKey

	// Members normalises spec.members into NormalizedMember plus per-member
	// errors that the reconciler should surface as Stalled reasons.
	// Implementations must return members sorted by Name so downstream
	// iteration is deterministic.
	Members(ctx context.Context, dr discovery.Resolver) ([]NormalizedMember, []MemberError)

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
	Subscribe(ctx context.Context, gvk schema.GroupVersionKind, scope apimeta.RESTScopeName, sub watcher.Subscriber) error
	Unsubscribe(gvk schema.GroupVersionKind, owner watcher.OwnerKey)
	UnsubscribeAll(owner watcher.OwnerKey)
	List(gvk schema.GroupVersionKind) ([]*unstructured.Unstructured, error)
	GVKsByOwner(owner watcher.OwnerKey) []schema.GroupVersionKind
}
