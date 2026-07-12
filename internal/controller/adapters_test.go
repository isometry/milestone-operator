/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller_test

import (
	"context"
	"errors"
	"testing"
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/controller"
	"github.com/isometry/milestone-operator/internal/discovery"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type fakeDisc struct {
	groups    *metav1.APIGroupList
	resources map[string]*metav1.APIResourceList
}

func (f *fakeDisc) ServerGroups(_ context.Context) (*metav1.APIGroupList, error) {
	return f.groups, nil
}
func (f *fakeDisc) ServerResourcesForGroupVersion(_ context.Context, gv string) (*metav1.APIResourceList, error) {
	if rl, ok := f.resources[gv]; ok {
		return rl, nil
	}
	return nil, errors.New("not found")
}

func newDisc() discovery.Resolver {
	fd := &fakeDisc{
		groups: &metav1.APIGroupList{Groups: []metav1.APIGroup{
			{
				Name:             groupKustomize,
				Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: gvKustomizeV1, Version: "v1"}},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: gvKustomizeV1, Version: "v1"},
			},
			{
				Name:             groupRBAC,
				Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: gvRBACv1, Version: "v1"}},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: gvRBACv1, Version: "v1"},
			},
		}},
		resources: map[string]*metav1.APIResourceList{
			gvKustomizeV1: {APIResources: []metav1.APIResource{{Name: depKustomizations, Kind: kindKustomization, Namespaced: true}}},
			gvRBACv1:      {APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: kindClusterRole, Namespaced: false}}},
		},
	}
	return discovery.NewResolver(fd, time.Hour)
}

func TestMilestoneAdapter_Dependencies_NamespaceMatcherIsOwnNamespace(t *testing.T) {
	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "wave-0"},
		Spec: apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{
			{
				Name:           depKustomizations,
				EmptySetPolicy: apiv1.EmptySetUnknown,
				Target:         apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization},
			},
		}},
	}
	a := controller.NewMilestoneAdapter(m)
	deps, errs := a.Dependencies(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(deps) != 1 {
		t.Fatalf("deps len = %d, want 1", len(deps))
	}
	d := deps[0]
	if d.Name != depKustomizations {
		t.Errorf("Name = %q, want kustomizations", d.Name)
	}
	if d.GVK.Kind != kindKustomization || d.GVK.Version != "v1" {
		t.Errorf("GVK = %v", d.GVK)
	}
	if d.NamespaceMatcher == nil {
		t.Fatalf("NamespaceMatcher should be non-nil for Milestone")
	}
	if !d.NamespaceMatcher(nsFluxSystem) {
		t.Errorf("matcher should accept own namespace")
	}
	if d.NamespaceMatcher("other") {
		t.Errorf("matcher should reject foreign namespace")
	}
}

// TestMilestoneAdapter_Dependencies_ClusterScopedKind_IsScopeMismatch guards
// the asymmetric scope contract: a Milestone (namespaced) can only target
// namespaced resources. Pointing one at a cluster-scoped kind must surface
// as a DependencyError rather than silently starting an informer whose
// namespace matcher will filter everything out.
func TestMilestoneAdapter_Dependencies_ClusterScopedKind_IsScopeMismatch(t *testing.T) {
	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{
			{Name: depRoles, Target: apiv1.TargetSpec{Group: groupRBAC, Kind: kindClusterRole}},
		}},
	}
	deps, errs := controller.NewMilestoneAdapter(m).Dependencies(t.Context(), newDisc())
	if len(deps) != 0 {
		t.Errorf("scope mismatch should drop the dependency; got %d", len(deps))
	}
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonNamespaceScopeMismatch {
		t.Errorf("errs = %+v, want one ReasonNamespaceScopeMismatch", errs)
	}
	if errs[0].Name != depRoles {
		t.Errorf("err Name = %q, want %q", errs[0].Name, depRoles)
	}
}

func TestMilestoneAdapter_Dependencies_DiscoveryFailureReportsDependencyError(t *testing.T) {
	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{
			{Name: depLate, Target: apiv1.TargetSpec{Group: groupMissing, Kind: kindLate}},
		}},
	}
	a := controller.NewMilestoneAdapter(m)
	deps, errs := a.Dependencies(t.Context(), newDisc())
	if len(deps) != 0 {
		t.Errorf("deps should be empty when discovery fails")
	}
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonGVKNotEstablished {
		t.Errorf("errs = %+v, want one ReasonGVKNotEstablished", errs)
	}
	if errs[0].Name != depLate {
		t.Errorf("err Name = %q, want %q", errs[0].Name, depLate)
	}
}

func TestMilestoneAdapter_Dependencies_PreservesSpecOrder(t *testing.T) {
	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{
			{Name: "zeta", Target: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization}},
			{Name: "alpha", Target: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization}},
			{Name: "mid", Target: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization}},
		}},
	}
	deps, errs := controller.NewMilestoneAdapter(m).Dependencies(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	want := []string{"zeta", "alpha", "mid"}
	for i, d := range deps {
		if d.Name != want[i] {
			t.Errorf("deps[%d].Name = %q, want %q", i, d.Name, want[i])
		}
	}
}

func TestClusterMilestoneAdapter_Dependencies_NamespaceListMatcher(t *testing.T) {
	cm := &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: namePlatform},
		Spec: apiv1.ClusterMilestoneSpec{DependsOn: []apiv1.ClusterDependencyRef{
			{
				Name: depKustomizations,
				Target: apiv1.ClusterTargetSpec{
					TargetSpec: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization},
					Namespaces: []string{nsFluxSystem, nsTeamA},
				},
			},
		}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	factory := controller.NewClusterMilestoneAdapterFactory(cl)
	a := factory(cm)
	deps, errs := a.Dependencies(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(deps) != 1 {
		t.Fatalf("deps len = %d, want 1", len(deps))
	}
	matcher := deps[0].NamespaceMatcher
	if !matcher(nsFluxSystem) || !matcher(nsTeamA) {
		t.Errorf("matcher should accept listed namespaces")
	}
	if matcher("team-b") {
		t.Errorf("matcher should reject unlisted namespaces")
	}
}

func TestClusterMilestoneAdapter_Dependencies_NamespaceSelectorListsNamespaces(t *testing.T) {
	ns1 := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsTeamA, Labels: map[string]string{labelTier: namePlatform}}}
	ns2 := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-b", Labels: map[string]string{labelTier: "data"}}}
	cm := &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: namePlatform},
		Spec: apiv1.ClusterMilestoneSpec{DependsOn: []apiv1.ClusterDependencyRef{
			{
				Name: depKustomizations,
				Target: apiv1.ClusterTargetSpec{
					TargetSpec:        apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization},
					NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelTier: namePlatform}},
				},
			},
		}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(ns1, ns2).Build()
	factory := controller.NewClusterMilestoneAdapterFactory(cl)
	deps, errs := factory(cm).Dependencies(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	matcher := deps[0].NamespaceMatcher
	if !matcher(nsTeamA) {
		t.Errorf("matcher should accept team-a (label match)")
	}
	if matcher("team-b") {
		t.Errorf("matcher should reject team-b (label mismatch)")
	}
}

func TestClusterMilestoneAdapter_Dependencies_ClusterScopedKindWithNamespaces_IsScopeMismatch(t *testing.T) {
	cm := &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: "x"},
		Spec: apiv1.ClusterMilestoneSpec{DependsOn: []apiv1.ClusterDependencyRef{
			{
				Name: depRoles,
				Target: apiv1.ClusterTargetSpec{
					TargetSpec: apiv1.TargetSpec{Group: groupRBAC, Kind: kindClusterRole},
					Namespaces: []string{nsFluxSystem},
				},
			},
		}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	factory := controller.NewClusterMilestoneAdapterFactory(cl)
	deps, errs := factory(cm).Dependencies(t.Context(), newDisc())
	if len(deps) != 0 {
		t.Errorf("scope mismatch should drop the dependency; got %d", len(deps))
	}
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonNamespaceScopeMismatch {
		t.Errorf("errs = %+v, want one ReasonNamespaceScopeMismatch", errs)
	}
}

func TestClusterMilestoneAdapter_Dependencies_NoNamespaceFilter_AllNamespaces(t *testing.T) {
	cm := &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: "global"},
		Spec: apiv1.ClusterMilestoneSpec{DependsOn: []apiv1.ClusterDependencyRef{
			{
				Name:   depKustomizations,
				Target: apiv1.ClusterTargetSpec{TargetSpec: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization}},
			},
		}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	deps, errs := controller.NewClusterMilestoneAdapterFactory(cl)(cm).Dependencies(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	if deps[0].NamespaceMatcher != nil {
		t.Errorf("nil matcher expected when no namespace filter (means all namespaces)")
	}
}

func TestMilestoneAdapter_PassesScopeFromDiscovery(t *testing.T) {
	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{
			{Name: "k", Target: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization}},
		}},
	}
	deps, _ := controller.NewMilestoneAdapter(m).Dependencies(t.Context(), newDisc())
	if deps[0].Scope != apimeta.RESTScopeNameNamespace {
		t.Errorf("scope = %v, want Namespaced", deps[0].Scope)
	}
}

// failingDisc simulates a discovery transport outage: both discovery
// endpoints error without answering.
type failingDisc struct{}

func (failingDisc) ServerGroups(context.Context) (*metav1.APIGroupList, error) {
	return nil, errors.New("the server is currently unable to handle the request (503)")
}
func (failingDisc) ServerResourcesForGroupVersion(context.Context, string) (*metav1.APIResourceList, error) {
	return nil, errors.New("the server is currently unable to handle the request (503)")
}

// TestAdapters_Dependencies_DiscoveryUnavailableReason: a discovery outage
// must surface as ReasonDiscoveryUnavailable — not ReasonGVKNotEstablished,
// which tells users the CRD is not installed — and both adapters must
// classify identically.
func TestAdapters_Dependencies_DiscoveryUnavailableReason(t *testing.T) {
	unavailable := discovery.NewResolver(failingDisc{}, time.Hour)

	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{
			{Name: depKustomizations, Target: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization}},
		}},
	}
	_, merrs := controller.NewMilestoneAdapter(m).Dependencies(t.Context(), unavailable)
	if len(merrs) != 1 || merrs[0].Reason != apiv1.ReasonDiscoveryUnavailable {
		t.Errorf("Milestone errs = %+v, want one ReasonDiscoveryUnavailable", merrs)
	}

	cm := &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: "x"},
		Spec: apiv1.ClusterMilestoneSpec{DependsOn: []apiv1.ClusterDependencyRef{
			{Name: depKustomizations, Target: apiv1.ClusterTargetSpec{TargetSpec: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization}}},
		}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	_, cerrs := controller.NewClusterMilestoneAdapterFactory(cl)(cm).Dependencies(t.Context(), unavailable)
	if len(cerrs) != 1 || cerrs[0].Reason != apiv1.ReasonDiscoveryUnavailable {
		t.Errorf("ClusterMilestone errs = %+v, want one ReasonDiscoveryUnavailable", cerrs)
	}
	if len(merrs) == 1 && len(cerrs) == 1 && merrs[0].Reason != cerrs[0].Reason {
		t.Errorf("adapters disagree on classification: %q vs %q", merrs[0].Reason, cerrs[0].Reason)
	}
}

// TestAdapters_DependencyError_CarriesResolvedVersion: once discovery has
// resolved a GVK, post-resolve failures (scope mismatch etc.) must carry the
// resolved version so failedRollup doesn't write an empty version into
// status.dependsOn.
func TestAdapters_DependencyError_CarriesResolvedVersion(t *testing.T) {
	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{
			{Name: depRoles, Target: apiv1.TargetSpec{Group: groupRBAC, Kind: kindClusterRole}},
		}},
	}
	_, merrs := controller.NewMilestoneAdapter(m).Dependencies(t.Context(), newDisc())
	if len(merrs) != 1 || merrs[0].Version != "v1" {
		t.Errorf("Milestone scope-mismatch error = %+v, want Version=v1", merrs)
	}

	cm := &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: "x"},
		Spec: apiv1.ClusterMilestoneSpec{DependsOn: []apiv1.ClusterDependencyRef{
			{
				Name: depRoles,
				Target: apiv1.ClusterTargetSpec{
					TargetSpec: apiv1.TargetSpec{Group: groupRBAC, Kind: kindClusterRole},
					Namespaces: []string{nsFluxSystem},
				},
			},
		}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	_, cerrs := controller.NewClusterMilestoneAdapterFactory(cl)(cm).Dependencies(t.Context(), newDisc())
	if len(cerrs) != 1 || cerrs[0].Version != "v1" {
		t.Errorf("ClusterMilestone scope-mismatch error = %+v, want Version=v1", cerrs)
	}
}

// TestClusterMilestoneAdapter_Dependencies_MemoizesNamespaceSelectorLists:
// several dependencies sharing one namespaceSelector must not re-list
// Namespaces once per dependency within a single Dependencies call.
func TestClusterMilestoneAdapter_Dependencies_MemoizesNamespaceSelectorLists(t *testing.T) {
	sel := &metav1.LabelSelector{MatchLabels: map[string]string{labelTier: namePlatform}}
	cm := &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: namePlatform},
		Spec: apiv1.ClusterMilestoneSpec{DependsOn: []apiv1.ClusterDependencyRef{
			{
				Name: "dep-a",
				Target: apiv1.ClusterTargetSpec{
					TargetSpec:        apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization},
					NamespaceSelector: sel,
				},
			},
			{
				Name: "dep-b",
				Target: apiv1.ClusterTargetSpec{
					TargetSpec:        apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization},
					NamespaceSelector: sel,
				},
			},
		}},
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsTeamA, Labels: map[string]string{labelTier: namePlatform}}}
	var listCalls int
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(ns).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.NamespaceList); ok {
					listCalls++
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()

	deps, errs := controller.NewClusterMilestoneAdapterFactory(cl)(cm).Dependencies(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	if len(deps) != 2 {
		t.Fatalf("deps len = %d, want 2", len(deps))
	}
	for _, d := range deps {
		if d.NamespaceMatcher == nil || !d.NamespaceMatcher(nsTeamA) {
			t.Errorf("dependency %q matcher should accept team-a", d.Name)
		}
	}
	if listCalls != 1 {
		t.Errorf("namespace List calls = %d, want 1 (memoized per Dependencies call)", listCalls)
	}
}

// Sanity: the discovery resolver is required.
func TestMilestoneAdapter_NilResolverIsDependencyError(t *testing.T) {
	m := &apiv1.Milestone{ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "x"}}
	_, errs := controller.NewMilestoneAdapter(m).Dependencies(t.Context(), nil)
	if len(errs) == 0 || errs[0].Reason != apiv1.ReasonDiscoveryFailed {
		t.Errorf("expected DiscoveryFailed; got %+v", errs)
	}
}

// Sanity: Group/Kind round-trip on the GVK.
func TestMilestoneAdapter_GVK(t *testing.T) {
	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{
			{Name: "k", Target: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization}},
		}},
	}
	deps, _ := controller.NewMilestoneAdapter(m).Dependencies(t.Context(), newDisc())
	wantGVK := schema.GroupVersionKind{Group: groupKustomize, Version: "v1", Kind: kindKustomization}
	if deps[0].GVK != wantGVK {
		t.Errorf("GVK = %v, want %v", deps[0].GVK, wantGVK)
	}
}
