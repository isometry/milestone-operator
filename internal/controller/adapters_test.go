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
			gvKustomizeV1: {APIResources: []metav1.APIResource{{Name: "kustomizations", Kind: kindKustomization, Namespaced: true}}},
			gvRBACv1:      {APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: kindClusterRole, Namespaced: false}}},
		},
	}
	return discovery.NewResolver(fd, time.Hour)
}

func TestEchelonAdapter_Targets_NamespaceMatcherIsOwnNamespace(t *testing.T) {
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "wave-0"},
		Spec: apiv1.EchelonSpec{Targets: []apiv1.TargetSpec{
			{Group: groupKustomize, Kind: kindKustomization, EmptySetPolicy: apiv1.EmptySetUnknown},
		}},
	}
	a := controller.NewEchelonAdapter(ech)
	targets, errs := a.Targets(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(targets) != 1 {
		t.Fatalf("targets len = %d, want 1", len(targets))
	}
	tgt := targets[0]
	if tgt.GVK.Kind != kindKustomization || tgt.GVK.Version != "v1" {
		t.Errorf("GVK = %v", tgt.GVK)
	}
	if tgt.NamespaceMatcher == nil {
		t.Fatalf("NamespaceMatcher should be non-nil for Echelon")
	}
	if !tgt.NamespaceMatcher(nsFluxSystem) {
		t.Errorf("matcher should accept own namespace")
	}
	if tgt.NamespaceMatcher("other") {
		t.Errorf("matcher should reject foreign namespace")
	}
}

// TestEchelonAdapter_Targets_ClusterScopedKind_IsScopeMismatch guards the
// asymmetric scope contract: an Echelon (namespaced) can only target
// namespaced resources. Pointing one at a cluster-scoped kind must be
// surfaced as a TargetError rather than silently starting an informer whose
// namespace matcher will filter everything out.
func TestEchelonAdapter_Targets_ClusterScopedKind_IsScopeMismatch(t *testing.T) {
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.EchelonSpec{Targets: []apiv1.TargetSpec{
			{Group: groupRBAC, Kind: kindClusterRole},
		}},
	}
	targets, errs := controller.NewEchelonAdapter(ech).Targets(t.Context(), newDisc())
	if len(targets) != 0 {
		t.Errorf("scope mismatch should drop the target; got %d", len(targets))
	}
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonNamespaceScopeMismatch {
		t.Errorf("errs = %+v, want one ReasonNamespaceScopeMismatch", errs)
	}
}

func TestEchelonAdapter_Targets_DiscoveryFailureReportsTargetError(t *testing.T) {
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.EchelonSpec{Targets: []apiv1.TargetSpec{
			{Group: "missing.io", Kind: kindLate},
		}},
	}
	a := controller.NewEchelonAdapter(ech)
	targets, errs := a.Targets(t.Context(), newDisc())
	if len(targets) != 0 {
		t.Errorf("targets should be empty when discovery fails")
	}
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonGVKNotEstablished {
		t.Errorf("errs = %+v, want one ReasonGVKNotEstablished", errs)
	}
}

func TestClusterEchelonAdapter_Targets_NamespaceListMatcher(t *testing.T) {
	ce := &apiv1.ClusterEchelon{
		ObjectMeta: metav1.ObjectMeta{Name: namePlatform},
		Spec: apiv1.ClusterEchelonSpec{Targets: []apiv1.ClusterTargetSpec{{
			TargetSpec: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization},
			Namespaces: []string{nsFluxSystem, "team-a"},
		}}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	factory := controller.NewClusterEchelonAdapterFactory(cl)
	a := factory(ce)
	targets, errs := a.Targets(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(targets) != 1 {
		t.Fatalf("targets len = %d, want 1", len(targets))
	}
	m := targets[0].NamespaceMatcher
	if !m(nsFluxSystem) || !m("team-a") {
		t.Errorf("matcher should accept listed namespaces")
	}
	if m("team-b") {
		t.Errorf("matcher should reject unlisted namespaces")
	}
}

func TestClusterEchelonAdapter_Targets_NamespaceSelectorListsNamespaces(t *testing.T) {
	ns1 := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a", Labels: map[string]string{labelTier: namePlatform}}}
	ns2 := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-b", Labels: map[string]string{labelTier: "data"}}}
	ce := &apiv1.ClusterEchelon{
		ObjectMeta: metav1.ObjectMeta{Name: namePlatform},
		Spec: apiv1.ClusterEchelonSpec{Targets: []apiv1.ClusterTargetSpec{{
			TargetSpec:        apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization},
			NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelTier: namePlatform}},
		}}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(ns1, ns2).Build()
	factory := controller.NewClusterEchelonAdapterFactory(cl)
	targets, errs := factory(ce).Targets(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	m := targets[0].NamespaceMatcher
	if !m("team-a") {
		t.Errorf("matcher should accept team-a (label match)")
	}
	if m("team-b") {
		t.Errorf("matcher should reject team-b (label mismatch)")
	}
}

func TestClusterEchelonAdapter_Targets_ClusterScopedKindWithNamespaces_IsScopeMismatch(t *testing.T) {
	ce := &apiv1.ClusterEchelon{
		ObjectMeta: metav1.ObjectMeta{Name: "x"},
		Spec: apiv1.ClusterEchelonSpec{Targets: []apiv1.ClusterTargetSpec{{
			TargetSpec: apiv1.TargetSpec{Group: groupRBAC, Kind: kindClusterRole},
			Namespaces: []string{nsFluxSystem},
		}}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	factory := controller.NewClusterEchelonAdapterFactory(cl)
	targets, errs := factory(ce).Targets(t.Context(), newDisc())
	if len(targets) != 0 {
		t.Errorf("scope mismatch should drop the target; got %d", len(targets))
	}
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonNamespaceScopeMismatch {
		t.Errorf("errs = %+v, want one ReasonNamespaceScopeMismatch", errs)
	}
}

func TestClusterEchelonAdapter_Targets_NoNamespaceFilter_AllNamespaces(t *testing.T) {
	ce := &apiv1.ClusterEchelon{
		ObjectMeta: metav1.ObjectMeta{Name: "global"},
		Spec: apiv1.ClusterEchelonSpec{Targets: []apiv1.ClusterTargetSpec{{
			TargetSpec: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKustomization},
		}}},
	}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	targets, errs := controller.NewClusterEchelonAdapterFactory(cl)(ce).Targets(t.Context(), newDisc())
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	if targets[0].NamespaceMatcher != nil {
		t.Errorf("nil matcher expected when no namespace filter (means all namespaces)")
	}
}

func TestEchelonAdapter_PassesScopeFromDiscovery(t *testing.T) {
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.EchelonSpec{Targets: []apiv1.TargetSpec{
			{Group: groupKustomize, Kind: kindKustomization},
		}},
	}
	targets, _ := controller.NewEchelonAdapter(ech).Targets(t.Context(), newDisc())
	if targets[0].Scope != apimeta.RESTScopeNameNamespace {
		t.Errorf("scope = %v, want Namespaced", targets[0].Scope)
	}
}

// Sanity: the discovery resolver is required.
func TestEchelonAdapter_NilResolverIsTargetError(t *testing.T) {
	ech := &apiv1.Echelon{ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "x"}}
	_, errs := controller.NewEchelonAdapter(ech).Targets(t.Context(), nil)
	if len(errs) == 0 || errs[0].Reason != apiv1.ReasonDiscoveryFailed {
		t.Errorf("expected DiscoveryFailed; got %+v", errs)
	}
}

// Sanity: Group/Kind round-trip on the GVK.
func TestEchelonAdapter_GVK(t *testing.T) {
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: "x"},
		Spec: apiv1.EchelonSpec{Targets: []apiv1.TargetSpec{
			{Group: groupKustomize, Kind: kindKustomization},
		}},
	}
	targets, _ := controller.NewEchelonAdapter(ech).Targets(t.Context(), newDisc())
	wantGVK := schema.GroupVersionKind{Group: groupKustomize, Version: "v1", Kind: kindKustomization}
	if targets[0].GVK != wantGVK {
		t.Errorf("GVK = %v, want %v", targets[0].GVK, wantGVK)
	}
}
