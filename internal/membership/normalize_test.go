/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package membership_test

import (
	"context"
	"errors"
	"testing"
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/membership"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	groupKustomize = "kustomize.toolkit.fluxcd.io"
	groupRBAC      = "rbac.authorization.k8s.io"
	groupMissing   = "missing.example.com"
	kindKust       = "Kustomization"
	kindRole       = "ClusterRole"
	nsFlux         = "flux-system"
	nsTeamA        = "team-a"
	nsTeamB        = "team-b"
	labelTier      = "tier"
	tierPlatform   = "platform"
	kindLate       = "Late"
	depName        = "dep"
	depListed      = "listed"
	depOK          = "ok"
	depMissing     = "missing"
)

type fakeDisc struct {
	groups    *metav1.APIGroupList
	resources map[string]*metav1.APIResourceList
}

func (f *fakeDisc) ServerGroups(context.Context) (*metav1.APIGroupList, error) {
	return f.groups, nil
}

func (f *fakeDisc) ServerResourcesForGroupVersion(_ context.Context, gv string) (*metav1.APIResourceList, error) {
	if rl, ok := f.resources[gv]; ok {
		return rl, nil
	}
	return nil, errors.New("not found")
}

func newResolver() discovery.Resolver {
	gvK, gvR := groupKustomize+"/v1", groupRBAC+"/v1"
	return discovery.NewResolver(&fakeDisc{
		groups: &metav1.APIGroupList{Groups: []metav1.APIGroup{
			{
				Name:             groupKustomize,
				Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: gvK, Version: "v1"}},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: gvK, Version: "v1"},
			},
			{
				Name:             groupRBAC,
				Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: gvR, Version: "v1"}},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: gvR, Version: "v1"},
			},
		}},
		resources: map[string]*metav1.APIResourceList{
			gvK: {APIResources: []metav1.APIResource{{Name: "kustomizations", Kind: kindKust, Namespaced: true}}},
			gvR: {APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: kindRole, Namespaced: false}}},
		},
	}, time.Hour)
}

type failingDisc struct{}

func (failingDisc) ServerGroups(context.Context) (*metav1.APIGroupList, error) {
	return nil, errors.New("503")
}

func (failingDisc) ServerResourcesForGroupVersion(context.Context, string) (*metav1.APIResourceList, error) {
	return nil, errors.New("503")
}

// fakeNamespaces is a NamespaceLister over a static label table that counts
// calls, so memoisation is observable.
type fakeNamespaces struct {
	byName map[string]labels.Set
	err    error
	calls  int
}

func (f *fakeNamespaces) list(_ context.Context, sel labels.Selector) ([]string, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	var out []string
	for n, l := range f.byName {
		if sel.Matches(l) {
			out = append(out, n)
		}
	}
	return out, nil
}

func tierNamespaces() *fakeNamespaces {
	return &fakeNamespaces{byName: map[string]labels.Set{
		nsTeamA: {labelTier: tierPlatform},
		nsTeamB: {labelTier: "data"},
	}}
}

var (
	kustTarget = apiv1.TargetSpec{Group: groupKustomize, Kind: kindKust}
	roleTarget = apiv1.TargetSpec{Group: groupRBAC, Kind: kindRole}
	badLabels  = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
		{Key: "k", Operator: "Bogus"},
	}}
	platformSel = &metav1.LabelSelector{MatchLabels: map[string]string{labelTier: tierPlatform}}
)

func milestone(deps ...apiv1.DependencyRef) *apiv1.Milestone {
	return &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFlux, Name: "wave-0"},
		Spec:       apiv1.MilestoneSpec{DependsOn: deps},
	}
}

func clusterMilestone(deps ...apiv1.ClusterDependencyRef) *apiv1.ClusterMilestone {
	return &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: tierPlatform},
		Spec:       apiv1.ClusterMilestoneSpec{DependsOn: deps},
	}
}

func TestNormalizeMilestone_Reasons(t *testing.T) {
	cases := []struct {
		name       string
		resolver   discovery.Resolver
		target     apiv1.TargetSpec
		wantReason string
		wantVer    string
	}{
		{"discovery outage", discovery.NewResolver(failingDisc{}, time.Hour), kustTarget,
			apiv1.ReasonDiscoveryUnavailable, ""},
		{"unknown group", newResolver(), apiv1.TargetSpec{Group: groupMissing, Kind: kindLate},
			apiv1.ReasonGVKNotEstablished, ""},
		{"cluster-scoped kind", newResolver(), roleTarget, apiv1.ReasonNamespaceScopeMismatch, "v1"},
		// Scope mismatch is checked before the selector is parsed.
		{"cluster-scoped kind with invalid selector", newResolver(),
			apiv1.TargetSpec{Group: groupRBAC, Kind: kindRole, Selector: badLabels},
			apiv1.ReasonNamespaceScopeMismatch, "v1"},
		{"invalid selector", newResolver(),
			apiv1.TargetSpec{Group: groupKustomize, Kind: kindKust, Selector: badLabels},
			apiv1.ReasonDiscoveryFailed, "v1"},
		// Resolution is checked before the selector is parsed.
		{"unknown group with invalid selector", newResolver(),
			apiv1.TargetSpec{Group: groupMissing, Kind: kindLate, Selector: badLabels},
			apiv1.ReasonGVKNotEstablished, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps, errs := membership.NormalizeMilestone(t.Context(), tc.resolver,
				milestone(apiv1.DependencyRef{Name: depName, Target: tc.target}))
			if len(deps) != 0 {
				t.Errorf("deps = %+v, want none", deps)
			}
			if len(errs) != 1 {
				t.Fatalf("errs = %+v, want exactly one", errs)
			}
			e := errs[0]
			if e.Name != depName || e.Reason != tc.wantReason || e.Version != tc.wantVer {
				t.Errorf("err = {Name:%q Reason:%q Version:%q}, want {dep %q %q}",
					e.Name, e.Reason, e.Version, tc.wantReason, tc.wantVer)
			}
			if e.Group != tc.target.Group || e.Kind != tc.target.Kind {
				t.Errorf("err GK = %s/%s, want %s/%s", e.Group, e.Kind, tc.target.Group, tc.target.Kind)
			}
			if e.Err == nil || e.Error() == "" {
				t.Errorf("err must carry an underlying error")
			}
		})
	}
}

func TestNormalizeMilestone_Success(t *testing.T) {
	m := milestone(
		apiv1.DependencyRef{Name: "zeta", EmptySetPolicy: apiv1.EmptySetUnknown, Target: kustTarget},
		apiv1.DependencyRef{Name: "alpha", SuspendPolicy: apiv1.SuspendNotReady, Target: apiv1.TargetSpec{
			Group: groupKustomize, Kind: kindKust, Selector: platformSel,
		}},
		apiv1.DependencyRef{Name: "broken", Target: roleTarget},
		apiv1.DependencyRef{Name: "mid", Target: kustTarget},
	)
	deps, errs := membership.NormalizeMilestone(t.Context(), newResolver(), m)
	if len(errs) != 1 || errs[0].Name != "broken" {
		t.Fatalf("errs = %+v, want only broken", errs)
	}
	want := []string{"zeta", "alpha", "mid"}
	if len(deps) != len(want) {
		t.Fatalf("deps len = %d, want %d", len(deps), len(want))
	}
	for i, d := range deps {
		if d.Name != want[i] {
			t.Errorf("deps[%d].Name = %q, want %q (spec order)", i, d.Name, want[i])
		}
	}
	z, a := deps[0], deps[1]
	wantGVK := schema.GroupVersionKind{Group: groupKustomize, Version: "v1", Kind: kindKust}
	if z.GVK != wantGVK || z.Scope != apimeta.RESTScopeNameNamespace {
		t.Errorf("zeta GVK/scope = %v/%v", z.GVK, z.Scope)
	}
	if z.EmptySetPolicy != apiv1.EmptySetUnknown || a.SuspendPolicy != apiv1.SuspendNotReady {
		t.Errorf("policies not carried: %+v / %+v", z, a)
	}
	if z.NamespaceMatcher == nil || !z.NamespaceMatcher(nsFlux) || z.NamespaceMatcher("other") {
		t.Errorf("Milestone matcher must admit exactly its own namespace")
	}
	if !z.Selector.Empty() {
		t.Errorf("nil selector should normalise to Everything, got %q", z.Selector)
	}
	if a.Selector.String() != labelTier+"=platform" {
		t.Errorf("alpha selector = %q", a.Selector)
	}
}

func TestNormalizeMilestone_NilResolver(t *testing.T) {
	_, errs := membership.NormalizeMilestone(t.Context(), nil, milestone())
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonDiscoveryFailed || errs[0].Name != "" {
		t.Errorf("errs = %+v, want one unnamed DiscoveryFailed", errs)
	}
}

func TestNormalizeClusterMilestone_Reasons(t *testing.T) {
	cases := []struct {
		name       string
		resolver   discovery.Resolver
		nsErr      error
		target     apiv1.ClusterTargetSpec
		wantReason string
	}{
		{"discovery outage", discovery.NewResolver(failingDisc{}, time.Hour), nil,
			apiv1.ClusterTargetSpec{TargetSpec: kustTarget}, apiv1.ReasonDiscoveryUnavailable},
		{"unknown group", newResolver(), nil,
			apiv1.ClusterTargetSpec{TargetSpec: apiv1.TargetSpec{Group: groupMissing, Kind: kindLate}},
			apiv1.ReasonGVKNotEstablished},
		{"cluster-scoped kind with namespaces", newResolver(), nil,
			apiv1.ClusterTargetSpec{TargetSpec: roleTarget, Namespaces: []string{nsFlux}},
			apiv1.ReasonNamespaceScopeMismatch},
		{"cluster-scoped kind with namespaceSelector", newResolver(), nil,
			apiv1.ClusterTargetSpec{TargetSpec: roleTarget, NamespaceSelector: platformSel},
			apiv1.ReasonNamespaceScopeMismatch},
		{"both namespace filters", newResolver(), nil,
			apiv1.ClusterTargetSpec{TargetSpec: kustTarget, Namespaces: []string{nsFlux}, NamespaceSelector: platformSel},
			apiv1.ReasonNamespaceScopeMismatch},
		// Both-filters mismatch wins over an invalid namespaceSelector.
		{"both namespace filters with invalid namespaceSelector", newResolver(), nil,
			apiv1.ClusterTargetSpec{TargetSpec: kustTarget, Namespaces: []string{nsFlux}, NamespaceSelector: badLabels},
			apiv1.ReasonNamespaceScopeMismatch},
		{"invalid namespaceSelector", newResolver(), nil,
			apiv1.ClusterTargetSpec{TargetSpec: kustTarget, NamespaceSelector: badLabels},
			apiv1.ReasonDiscoveryFailed},
		{"namespace list fails", newResolver(), errors.New("forbidden"),
			apiv1.ClusterTargetSpec{TargetSpec: kustTarget, NamespaceSelector: platformSel},
			apiv1.ReasonDiscoveryFailed},
		{"invalid selector", newResolver(), nil,
			apiv1.ClusterTargetSpec{TargetSpec: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKust, Selector: badLabels}},
			apiv1.ReasonDiscoveryFailed},
		// Scope mismatch wins over an invalid label selector.
		{"cluster-scoped kind with namespaces and invalid selector", newResolver(), nil,
			apiv1.ClusterTargetSpec{
				TargetSpec: apiv1.TargetSpec{Group: groupRBAC, Kind: kindRole, Selector: badLabels},
				Namespaces: []string{nsFlux},
			},
			apiv1.ReasonNamespaceScopeMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ns := tierNamespaces()
			ns.err = tc.nsErr
			deps, errs := membership.NormalizeClusterMilestone(t.Context(), tc.resolver, ns.list,
				clusterMilestone(apiv1.ClusterDependencyRef{Name: depName, Target: tc.target}))
			if len(deps) != 0 {
				t.Errorf("deps = %+v, want none", deps)
			}
			if len(errs) != 1 || errs[0].Name != depName || errs[0].Reason != tc.wantReason {
				t.Fatalf("errs = %+v, want one %q for dep", errs, tc.wantReason)
			}
		})
	}
}

func TestNormalizeClusterMilestone_NamespaceMatchers(t *testing.T) {
	cases := []struct {
		name    string
		target  apiv1.ClusterTargetSpec
		nilWant bool
		admit   []string
		reject  []string
	}{
		{"no filter admits all", apiv1.ClusterTargetSpec{TargetSpec: kustTarget}, true, nil, nil},
		{"empty namespaces counts as unset", apiv1.ClusterTargetSpec{TargetSpec: kustTarget, Namespaces: []string{}},
			true, nil, nil},
		{"empty namespaces with namespaceSelector",
			apiv1.ClusterTargetSpec{TargetSpec: kustTarget, Namespaces: []string{}, NamespaceSelector: platformSel},
			false, []string{nsTeamA}, []string{nsTeamB}},
		{"namespaces allow-list",
			apiv1.ClusterTargetSpec{TargetSpec: kustTarget, Namespaces: []string{nsFlux, nsTeamA}},
			false, []string{nsFlux, nsTeamA}, []string{nsTeamB}},
		{"namespaceSelector", apiv1.ClusterTargetSpec{TargetSpec: kustTarget, NamespaceSelector: platformSel},
			false, []string{nsTeamA}, []string{nsTeamB, nsFlux}},
		{"cluster-scoped kind without filters", apiv1.ClusterTargetSpec{TargetSpec: roleTarget}, true, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps, errs := membership.NormalizeClusterMilestone(t.Context(), newResolver(), tierNamespaces().list,
				clusterMilestone(apiv1.ClusterDependencyRef{
					Name: depName, SuspendPolicy: apiv1.SuspendNotReady, Target: tc.target,
				}))
			if len(errs) != 0 || len(deps) != 1 {
				t.Fatalf("deps=%+v errs=%+v, want one dep and no errors", deps, errs)
			}
			d := deps[0]
			if d.SuspendPolicy != apiv1.SuspendNotReady {
				t.Errorf("SuspendPolicy = %q", d.SuspendPolicy)
			}
			if tc.nilWant != (d.NamespaceMatcher == nil) {
				t.Fatalf("NamespaceMatcher nil = %v, want %v", d.NamespaceMatcher == nil, tc.nilWant)
			}
			for _, n := range tc.admit {
				if !d.NamespaceMatcher(n) {
					t.Errorf("matcher should admit %q", n)
				}
			}
			for _, n := range tc.reject {
				if d.NamespaceMatcher(n) {
					t.Errorf("matcher should reject %q", n)
				}
			}
		})
	}
}

func TestNormalizeClusterMilestone_ClusterScopedKindScope(t *testing.T) {
	deps, errs := membership.NormalizeClusterMilestone(t.Context(), newResolver(), nil,
		clusterMilestone(apiv1.ClusterDependencyRef{
			Name: "roles", Target: apiv1.ClusterTargetSpec{TargetSpec: roleTarget},
		}))
	if len(errs) != 0 || len(deps) != 1 {
		t.Fatalf("deps=%+v errs=%+v", deps, errs)
	}
	if deps[0].Scope != apimeta.RESTScopeNameRoot {
		t.Errorf("Scope = %q, want Root", deps[0].Scope)
	}
}

func TestNormalizeClusterMilestone_MemoisesNamespaceSelectorLists(t *testing.T) {
	ns := tierNamespaces()
	dep := func(name string) apiv1.ClusterDependencyRef {
		return apiv1.ClusterDependencyRef{Name: name, Target: apiv1.ClusterTargetSpec{
			TargetSpec: kustTarget, NamespaceSelector: platformSel,
		}}
	}
	deps, errs := membership.NormalizeClusterMilestone(t.Context(), newResolver(), ns.list,
		clusterMilestone(dep("dep-a"), dep("dep-b")))
	if len(errs) != 0 || len(deps) != 2 {
		t.Fatalf("deps=%+v errs=%+v", deps, errs)
	}
	if ns.calls != 1 {
		t.Errorf("namespace lister calls = %d, want 1", ns.calls)
	}
}

func TestNormalizeClusterMilestone_NilResolver(t *testing.T) {
	_, errs := membership.NormalizeClusterMilestone(t.Context(), nil, nil, clusterMilestone())
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonDiscoveryFailed {
		t.Errorf("errs = %+v, want one DiscoveryFailed", errs)
	}
}

func TestNormalizeClusterMilestone_NilLister(t *testing.T) {
	_, errs := membership.NormalizeClusterMilestone(t.Context(), newResolver(), nil,
		clusterMilestone(apiv1.ClusterDependencyRef{Name: depName, Target: apiv1.ClusterTargetSpec{
			TargetSpec: kustTarget, NamespaceSelector: platformSel,
		}}))
	if len(errs) != 1 || errs[0].Reason != apiv1.ReasonDiscoveryFailed {
		t.Errorf("errs = %+v, want one DiscoveryFailed", errs)
	}
}
