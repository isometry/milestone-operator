/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package discovery_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/isometry/echelon-operator/internal/discovery"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fakeDiscoverer satisfies discovery.Discoverer with deterministic, in-memory
// data and counts calls so tests can assert caching behaviour.
type fakeDiscoverer struct {
	groups        *metav1.APIGroupList
	resources     map[string]*metav1.APIResourceList // keyed by groupVersion
	groupsCalls   atomic.Int32
	resourceCalls atomic.Int32
	failResources error
}

func (f *fakeDiscoverer) ServerGroups(_ context.Context) (*metav1.APIGroupList, error) {
	f.groupsCalls.Add(1)
	if f.groups == nil {
		return nil, errors.New("no groups")
	}
	return f.groups, nil
}

func (f *fakeDiscoverer) ServerResourcesForGroupVersion(_ context.Context, gv string) (*metav1.APIResourceList, error) {
	f.resourceCalls.Add(1)
	if f.failResources != nil {
		return nil, f.failResources
	}
	rl, ok := f.resources[gv]
	if !ok {
		return nil, errors.New("not found")
	}
	return rl, nil
}

func newFluxFake() *fakeDiscoverer {
	return &fakeDiscoverer{
		groups: &metav1.APIGroupList{Groups: []metav1.APIGroup{
			{
				Name: groupKustomize,
				Versions: []metav1.GroupVersionForDiscovery{
					{GroupVersion: gvKustomizeV1, Version: "v1"},
					{GroupVersion: gvKustomizeV1beta2, Version: "v1beta2"},
				},
				PreferredVersion: metav1.GroupVersionForDiscovery{
					GroupVersion: gvKustomizeV1, Version: "v1",
				},
			},
			{
				Name: groupRBAC,
				Versions: []metav1.GroupVersionForDiscovery{
					{GroupVersion: gvRBACv1, Version: "v1"},
				},
				PreferredVersion: metav1.GroupVersionForDiscovery{
					GroupVersion: gvRBACv1, Version: "v1",
				},
			},
		}},
		resources: map[string]*metav1.APIResourceList{
			gvKustomizeV1: {
				GroupVersion: gvKustomizeV1,
				APIResources: []metav1.APIResource{
					{Name: "kustomizations", Kind: kindKustomization, Namespaced: true},
				},
			},
			gvKustomizeV1beta2: {
				GroupVersion: gvKustomizeV1beta2,
				APIResources: []metav1.APIResource{
					{Name: "kustomizations", Kind: kindKustomization, Namespaced: true},
				},
			},
			gvRBACv1: {
				GroupVersion: gvRBACv1,
				APIResources: []metav1.APIResource{
					{Name: "clusterroles", Kind: "ClusterRole", Namespaced: false},
					{Name: "roles", Kind: "Role", Namespaced: true},
				},
			},
		},
	}
}

func newResolver(t *testing.T, fd *fakeDiscoverer, ttl time.Duration) discovery.Resolver {
	t.Helper()
	return discovery.NewResolver(fd, ttl)
}

func TestResolve_PreferredVersion(t *testing.T) {
	fd := newFluxFake()
	r := newResolver(t, fd, time.Hour)
	gvk, scope, err := r.Resolve(t.Context(), groupKustomize, kindKustomization, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantGVK := schema.GroupVersionKind{Group: groupKustomize, Version: "v1", Kind: kindKustomization}
	if gvk != wantGVK {
		t.Errorf("gvk = %v, want %v", gvk, wantGVK)
	}
	if scope != apimeta.RESTScopeNameNamespace {
		t.Errorf("scope = %v, want Namespaced", scope)
	}
}

func TestResolve_ExplicitVersion(t *testing.T) {
	fd := newFluxFake()
	r := newResolver(t, fd, time.Hour)
	gvk, _, err := r.Resolve(t.Context(), groupKustomize, kindKustomization, "v1beta2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gvk.Version != "v1beta2" {
		t.Errorf("version = %q, want v1beta2", gvk.Version)
	}
}

func TestResolve_ClusterScoped(t *testing.T) {
	fd := newFluxFake()
	r := newResolver(t, fd, time.Hour)
	_, scope, err := r.Resolve(t.Context(), "rbac.authorization.k8s.io", "ClusterRole", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if scope != apimeta.RESTScopeNameRoot {
		t.Errorf("scope = %v, want Root (cluster)", scope)
	}
}

func TestResolve_MissingGroup(t *testing.T) {
	fd := newFluxFake()
	r := newResolver(t, fd, time.Hour)
	_, _, err := r.Resolve(t.Context(), "does-not-exist.io", "Whatever", "")
	if !errors.Is(err, discovery.ErrGVKNotEstablished) {
		t.Errorf("err = %v, want ErrGVKNotEstablished", err)
	}
}

func TestResolve_MissingKind(t *testing.T) {
	fd := newFluxFake()
	r := newResolver(t, fd, time.Hour)
	_, _, err := r.Resolve(t.Context(), groupKustomize, "DoesNotExist", "")
	if !errors.Is(err, discovery.ErrGVKNotEstablished) {
		t.Errorf("err = %v, want ErrGVKNotEstablished", err)
	}
}

func TestResolve_CacheHit(t *testing.T) {
	fd := newFluxFake()
	r := newResolver(t, fd, time.Hour)
	for range 5 {
		if _, _, err := r.Resolve(t.Context(), groupKustomize, kindKustomization, ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if g := fd.groupsCalls.Load(); g != 1 {
		t.Errorf("ServerGroups calls = %d, want 1 (cached)", g)
	}
	if g := fd.resourceCalls.Load(); g != 1 {
		t.Errorf("ServerResourcesForGroupVersion calls = %d, want 1 (cached)", g)
	}
}

func TestResolve_CacheExpiry(t *testing.T) {
	fd := newFluxFake()
	r := newResolver(t, fd, 10*time.Millisecond)
	if _, _, err := r.Resolve(t.Context(), groupKustomize, kindKustomization, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, _, err := r.Resolve(t.Context(), groupKustomize, kindKustomization, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if g := fd.groupsCalls.Load(); g < 2 {
		t.Errorf("ServerGroups calls = %d, want >= 2 after TTL", g)
	}
}

func TestResolve_Invalidate(t *testing.T) {
	fd := newFluxFake()
	r := newResolver(t, fd, time.Hour)
	if _, _, err := r.Resolve(t.Context(), groupKustomize, kindKustomization, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r.Invalidate()
	if _, _, err := r.Resolve(t.Context(), groupKustomize, kindKustomization, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if g := fd.groupsCalls.Load(); g < 2 {
		t.Errorf("ServerGroups calls = %d, want >= 2 after Invalidate", g)
	}
}

// TestResolve_CoreGroupOmittedVersion: when group=="" and version=="", the
// resolver must short-circuit to v1 and validate via
// ServerResourcesForGroupVersion. ServerGroups doesn't list the core group,
// so calling it would return ErrGVKNotEstablished — this fake fails that
// call so the test will only pass if it's never invoked.
func TestResolve_CoreGroupOmittedVersion(t *testing.T) {
	fd := &fakeDiscoverer{
		// no groups configured → ServerGroups returns "no groups" error
		resources: map[string]*metav1.APIResourceList{
			"v1": {
				GroupVersion: "v1",
				APIResources: []metav1.APIResource{
					{Name: "configmaps", Kind: "ConfigMap", Namespaced: true},
					{Name: "pods", Kind: "Pod", Namespaced: true},
				},
			},
		},
	}
	r := newResolver(t, fd, time.Hour)

	gvk, scope, err := r.Resolve(t.Context(), "", "ConfigMap", "")
	if err != nil {
		t.Fatalf("ConfigMap: %v", err)
	}
	want := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
	if gvk != want {
		t.Errorf("gvk = %v, want %v", gvk, want)
	}
	if scope != apimeta.RESTScopeNameNamespace {
		t.Errorf("scope = %v, want Namespaced", scope)
	}
	if g := fd.groupsCalls.Load(); g != 0 {
		t.Errorf("ServerGroups invoked %d times for core-group short-circuit; want 0", g)
	}
}

func TestResolve_NegativeCacheRefreshes(t *testing.T) {
	// A miss should not be cached as long-lived; a subsequent CRD install
	// (simulated by adding a group between calls) should be observed quickly.
	fd := newFluxFake()
	r := newResolver(t, fd, time.Hour)
	if _, _, err := r.Resolve(t.Context(), groupLate, "Late", ""); !errors.Is(err, discovery.ErrGVKNotEstablished) {
		t.Fatalf("err = %v, want ErrGVKNotEstablished", err)
	}
	// Install the new group.
	fd.groups.Groups = append(fd.groups.Groups, metav1.APIGroup{
		Name:             groupLate,
		Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: gvLateV1, Version: "v1"}},
		PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: gvLateV1, Version: "v1"},
	})
	fd.resources[gvLateV1] = &metav1.APIResourceList{
		GroupVersion: gvLateV1,
		APIResources: []metav1.APIResource{{Name: "lates", Kind: "Late", Namespaced: true}},
	}
	if _, _, err := r.Resolve(t.Context(), groupLate, "Late", ""); err != nil {
		t.Fatalf("after install, err = %v, want nil", err)
	}
}
