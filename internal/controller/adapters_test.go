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

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	"github.com/isometry/echelon-operator/internal/controller"
	"github.com/isometry/echelon-operator/internal/discovery"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
			gvKustomizeV1: {APIResources: []metav1.APIResource{{Name: memberKustomizations, Kind: kindKustomization, Namespaced: true}}},
			gvRBACv1:      {APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: kindClusterRole, Namespaced: false}}},
		},
	}
	return discovery.NewResolver(fd, time.Hour)
}

func TestEchelonAdapter_Members_NamespaceMatcherIsOwnNamespace(t *testing.T) {
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "wave-0"},
		Spec: apiv1.EchelonSpec{Members: map[string]apiv1.MemberSpec{
			memberKustomizations: {Group: groupKustomize, Kind: kindKustomization, EmptySetPolicy: apiv1.EmptySetUnknown},
		}},
	}
	a := controller.NewEchelonAdapter(ech)
	members, errs := a.Members(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(members) != 1 {
		t.Fatalf("members len = %d, want 1", len(members))
	}
	m := members[0]
	if m.Name != memberKustomizations {
		t.Errorf("Name = %q, want kustomizations", m.Name)
	}
	if m.GVK.Kind != kindKustomization || m.GVK.Version != "v1" {
		t.Errorf("GVK = %v", m.GVK)
	}
	if m.NamespaceMatcher == nil {
		t.Fatalf("NamespaceMatcher should be non-nil for Echelon")
	}
	if !m.NamespaceMatcher(nsFluxSystem) {
		t.Errorf("matcher should accept own namespace")
	}
	if m.NamespaceMatcher("other") {
		t.Errorf("matcher should reject foreign namespace")
	}
}

// TestEchelonAdapter_Members_ClusterScopedKind_IsScopeMismatch guards the
// asymmetric scope contract: an Echelon (namespaced) can only target
// namespaced resources. Pointing one at a cluster-scoped kind must be
// surfaced as a MemberError rather than silently starting an informer whose
// namespace matcher will filter everything out.
func TestEchelonAdapter_Members_ClusterScopedKind_IsScopeMismatch(t *testing.T) {
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.EchelonSpec{Members: map[string]apiv1.MemberSpec{
			memberRoles: {Group: groupRBAC, Kind: kindClusterRole},
		}},
	}
	members, errs := controller.NewEchelonAdapter(ech).Members(t.Context(), newDisc())
	if len(members) != 0 {
		t.Errorf("scope mismatch should drop the member; got %d", len(members))
	}
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonNamespaceScopeMismatch {
		t.Errorf("errs = %+v, want one ReasonNamespaceScopeMismatch", errs)
	}
	if errs[0].Name != memberRoles {
		t.Errorf("err Name = %q, want roles", errs[0].Name)
	}
}

func TestEchelonAdapter_Members_DiscoveryFailureReportsMemberError(t *testing.T) {
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.EchelonSpec{Members: map[string]apiv1.MemberSpec{
			memberLate: {Group: "missing.io", Kind: kindLate},
		}},
	}
	a := controller.NewEchelonAdapter(ech)
	members, errs := a.Members(t.Context(), newDisc())
	if len(members) != 0 {
		t.Errorf("members should be empty when discovery fails")
	}
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonGVKNotEstablished {
		t.Errorf("errs = %+v, want one ReasonGVKNotEstablished", errs)
	}
	if errs[0].Name != memberLate {
		t.Errorf("err Name = %q, want late", errs[0].Name)
	}
}

func TestEchelonAdapter_Members_SortedByKey(t *testing.T) {
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.EchelonSpec{Members: map[string]apiv1.MemberSpec{
			"zeta":  {Group: groupKustomize, Kind: kindKustomization},
			"alpha": {Group: groupKustomize, Kind: kindKustomization},
			"mid":   {Group: groupKustomize, Kind: kindKustomization},
		}},
	}
	members, errs := controller.NewEchelonAdapter(ech).Members(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	want := []string{"alpha", "mid", "zeta"}
	for i, m := range members {
		if m.Name != want[i] {
			t.Errorf("members[%d].Name = %q, want %q", i, m.Name, want[i])
		}
	}
}

func TestClusterEchelonAdapter_Members_NamespaceListMatcher(t *testing.T) {
	ce := &apiv1.ClusterEchelon{
		ObjectMeta: metav1.ObjectMeta{Name: namePlatform},
		Spec: apiv1.ClusterEchelonSpec{Members: map[string]apiv1.ClusterMemberSpec{
			memberKustomizations: {
				MemberSpec: apiv1.MemberSpec{Group: groupKustomize, Kind: kindKustomization},
				Namespaces: []string{nsFluxSystem, "team-a"},
			},
		}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	factory := controller.NewClusterEchelonAdapterFactory(cl)
	a := factory(ce)
	members, errs := a.Members(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(members) != 1 {
		t.Fatalf("members len = %d, want 1", len(members))
	}
	m := members[0].NamespaceMatcher
	if !m(nsFluxSystem) || !m("team-a") {
		t.Errorf("matcher should accept listed namespaces")
	}
	if m("team-b") {
		t.Errorf("matcher should reject unlisted namespaces")
	}
}

func TestClusterEchelonAdapter_Members_NamespaceSelectorListsNamespaces(t *testing.T) {
	ns1 := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a", Labels: map[string]string{labelTier: namePlatform}}}
	ns2 := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-b", Labels: map[string]string{labelTier: "data"}}}
	ce := &apiv1.ClusterEchelon{
		ObjectMeta: metav1.ObjectMeta{Name: namePlatform},
		Spec: apiv1.ClusterEchelonSpec{Members: map[string]apiv1.ClusterMemberSpec{
			memberKustomizations: {
				MemberSpec:        apiv1.MemberSpec{Group: groupKustomize, Kind: kindKustomization},
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelTier: namePlatform}},
			},
		}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(ns1, ns2).Build()
	factory := controller.NewClusterEchelonAdapterFactory(cl)
	members, errs := factory(ce).Members(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	m := members[0].NamespaceMatcher
	if !m("team-a") {
		t.Errorf("matcher should accept team-a (label match)")
	}
	if m("team-b") {
		t.Errorf("matcher should reject team-b (label mismatch)")
	}
}

func TestClusterEchelonAdapter_Members_ClusterScopedKindWithNamespaces_IsScopeMismatch(t *testing.T) {
	ce := &apiv1.ClusterEchelon{
		ObjectMeta: metav1.ObjectMeta{Name: "x"},
		Spec: apiv1.ClusterEchelonSpec{Members: map[string]apiv1.ClusterMemberSpec{
			memberRoles: {
				MemberSpec: apiv1.MemberSpec{Group: groupRBAC, Kind: kindClusterRole},
				Namespaces: []string{nsFluxSystem},
			},
		}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	factory := controller.NewClusterEchelonAdapterFactory(cl)
	members, errs := factory(ce).Members(t.Context(), newDisc())
	if len(members) != 0 {
		t.Errorf("scope mismatch should drop the member; got %d", len(members))
	}
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonNamespaceScopeMismatch {
		t.Errorf("errs = %+v, want one ReasonNamespaceScopeMismatch", errs)
	}
}

func TestClusterEchelonAdapter_Members_NoNamespaceFilter_AllNamespaces(t *testing.T) {
	ce := &apiv1.ClusterEchelon{
		ObjectMeta: metav1.ObjectMeta{Name: "global"},
		Spec: apiv1.ClusterEchelonSpec{Members: map[string]apiv1.ClusterMemberSpec{
			memberKustomizations: {
				MemberSpec: apiv1.MemberSpec{Group: groupKustomize, Kind: kindKustomization},
			},
		}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	members, errs := controller.NewClusterEchelonAdapterFactory(cl)(ce).Members(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	if members[0].NamespaceMatcher != nil {
		t.Errorf("nil matcher expected when no namespace filter (means all namespaces)")
	}
}

func TestEchelonAdapter_PassesScopeFromDiscovery(t *testing.T) {
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.EchelonSpec{Members: map[string]apiv1.MemberSpec{
			"k": {Group: groupKustomize, Kind: kindKustomization},
		}},
	}
	members, _ := controller.NewEchelonAdapter(ech).Members(t.Context(), newDisc())
	if members[0].Scope != apimeta.RESTScopeNameNamespace {
		t.Errorf("scope = %v, want Namespaced", members[0].Scope)
	}
}

// Sanity: the discovery resolver is required.
func TestEchelonAdapter_NilResolverIsMemberError(t *testing.T) {
	ech := &apiv1.Echelon{ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "x"}}
	_, errs := controller.NewEchelonAdapter(ech).Members(t.Context(), nil)
	if len(errs) == 0 || errs[0].Reason != apiv1.ReasonDiscoveryFailed {
		t.Errorf("expected DiscoveryFailed; got %+v", errs)
	}
}

// Sanity: Group/Kind round-trip on the GVK.
func TestEchelonAdapter_GVK(t *testing.T) {
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.EchelonSpec{Members: map[string]apiv1.MemberSpec{
			"k": {Group: groupKustomize, Kind: kindKustomization},
		}},
	}
	members, _ := controller.NewEchelonAdapter(ech).Members(t.Context(), newDisc())
	wantGVK := schema.GroupVersionKind{Group: groupKustomize, Version: "v1", Kind: kindKustomization}
	if members[0].GVK != wantGVK {
		t.Errorf("GVK = %v, want %v", members[0].GVK, wantGVK)
	}
}
