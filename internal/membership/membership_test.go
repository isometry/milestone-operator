/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package membership_test

import (
	"errors"
	"reflect"
	"testing"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/membership"
	"github.com/isometry/milestone-operator/internal/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestDependency_Admits(t *testing.T) {
	onlyA := func(ns string) bool { return ns == nsTeamA }
	platform := labels.SelectorFromSet(labels.Set{labelTier: tierPlatform})
	platformLabels := labels.Set{labelTier: tierPlatform}
	cases := []struct {
		name  string
		dep   membership.Dependency
		ns    string
		lbls  labels.Set
		admit bool
	}{
		{"nil matcher and selector admit everything", membership.Dependency{}, nsTeamB, nil, true},
		{"nil matcher admits any namespace", membership.Dependency{Selector: platform}, nsTeamB, platformLabels, true},
		{"matcher rejects foreign namespace", membership.Dependency{NamespaceMatcher: onlyA}, nsTeamB, nil, false},
		{"matcher admits own namespace", membership.Dependency{NamespaceMatcher: onlyA}, nsTeamA, nil, true},
		{"selector rejects label mismatch", membership.Dependency{Selector: platform}, nsTeamA,
			labels.Set{labelTier: tierData}, false},
		{"selector rejects missing labels", membership.Dependency{Selector: platform}, nsTeamA, nil, false},
		{"both must pass", membership.Dependency{NamespaceMatcher: onlyA, Selector: platform}, nsTeamB,
			platformLabels, false},
		{"both pass", membership.Dependency{NamespaceMatcher: onlyA, Selector: platform}, nsTeamA,
			platformLabels, true},
		{"everything selector", membership.Dependency{Selector: labels.Everything()}, "", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.dep.Admits(tc.ns, tc.lbls); got != tc.admit {
				t.Errorf("Admits(%q, %v) = %v, want %v", tc.ns, tc.lbls, got, tc.admit)
			}
		})
	}
}

func TestError_WrapsUnderlying(t *testing.T) {
	sentinel := errors.New("boom")
	e := membership.NewError(depName, schema.GroupVersionKind{Group: "g", Version: "v2", Kind: "K"},
		apiv1.ReasonDiscoveryFailed, sentinel)
	want := membership.Error{Name: depName, Group: "g", Version: "v2", Kind: "K",
		Reason: apiv1.ReasonDiscoveryFailed, Err: sentinel}
	if !reflect.DeepEqual(e, want) {
		t.Errorf("NewError = %+v, want %+v", e, want)
	}
	if !errors.Is(e, sentinel) || e.Error() != "boom" {
		t.Errorf("Error must wrap and render the underlying error")
	}
}

func TestFailedRollup(t *testing.T) {
	got := membership.FailedRollup(depName, schema.GroupVersionKind{Group: "g", Version: "v1", Kind: "K"},
		apiv1.ReasonListFailed)
	want := apiv1.DependencyStatus{Name: depName, Group: "g", Version: "v1", Kind: "K",
		Ready: metav1.ConditionUnknown, Reason: apiv1.ReasonListFailed}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FailedRollup = %+v, want %+v", got, want)
	}
}

func readyRollup() apiv1.DependencyStatus {
	return apiv1.DependencyStatus{Name: depOK, Group: "g", Version: "v1", Kind: "K",
		Ready: metav1.ConditionTrue, Reason: apiv1.ReasonAllResourcesReady,
		Summary: apiv1.Summary{Total: 1, Current: 1}}
}

func TestRollups(t *testing.T) {
	listFailed := membership.FailedRollup(depListed, schema.GroupVersionKind{Group: "g", Version: "v1", Kind: "K"},
		apiv1.ReasonListFailed)
	perDep := map[string]apiv1.DependencyStatus{
		depOK:     readyRollup(),
		depListed: listFailed,
	}
	errs := []membership.Error{
		{Name: depMissing, Group: groupMissing, Kind: kindLate, Reason: apiv1.ReasonGVKNotEstablished,
			Err: errors.New("no")},
		// A dependency that already has a rollup keeps it.
		{Name: depListed, Group: "g", Version: "v1", Kind: "K", Reason: apiv1.ReasonWatchSetupFailed,
			Err: errors.New("no")},
		// First error for a name wins.
		{Name: depMissing, Group: "other", Kind: "Other", Reason: apiv1.ReasonDiscoveryFailed, Err: errors.New("no")},
		// Unnamed errors (e.g. nil resolver) have no dependency to stand in for.
		{Reason: apiv1.ReasonDiscoveryFailed, Err: errors.New("nil resolver")},
	}

	got := membership.Rollups(perDep, errs)

	want := map[string]apiv1.DependencyStatus{
		depOK:     readyRollup(),
		depListed: listFailed,
		depMissing: {Name: depMissing, Group: groupMissing, Kind: kindLate,
			Ready: metav1.ConditionUnknown, Reason: apiv1.ReasonGVKNotEstablished},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Rollups =\n%+v\nwant\n%+v", got, want)
	}
	if len(perDep) != 2 {
		t.Errorf("Rollups must not mutate its input; perDep now has %d entries", len(perDep))
	}

	ready, _, _ := status.ReduceOwner(got)
	if ready != metav1.ConditionUnknown {
		t.Errorf("owner Ready = %q, want Unknown with failed dependencies", ready)
	}
	// Stand-ins carry no resource counts.
	if s := status.SummarizeOwner(got); s != (apiv1.Summary{Total: 1, Current: 1}) {
		t.Errorf("summary = %+v, want total=1 current=1", s)
	}
}

// A failed dependency must never let an otherwise-ready owner report
// Ready=True: without the stand-in the failure would vanish from the reduction.
func TestRollups_FailureBlocksReadyOwner(t *testing.T) {
	perDep := map[string]apiv1.DependencyStatus{depOK: readyRollup()}
	if ready, _, _ := status.ReduceOwner(perDep); ready != metav1.ConditionTrue {
		t.Fatalf("precondition: owner Ready = %q, want True", ready)
	}
	got := membership.Rollups(perDep, []membership.Error{{
		Name: depRoles, Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRole",
		Reason: apiv1.ReasonNamespaceScopeMismatch, Err: errors.New("cluster-scoped"),
	}})
	if ready, _, _ := status.ReduceOwner(got); ready != metav1.ConditionUnknown {
		t.Errorf("owner Ready = %q, want Unknown", ready)
	}
}

func TestRollups_NoErrors(t *testing.T) {
	perDep := map[string]apiv1.DependencyStatus{depOK: readyRollup()}
	if got := membership.Rollups(perDep, nil); !reflect.DeepEqual(got, perDep) {
		t.Errorf("Rollups = %+v, want %+v", got, perDep)
	}
	if got := membership.Rollups(nil, nil); len(got) != 0 {
		t.Errorf("Rollups(nil, nil) = %+v, want empty", got)
	}
}

func TestMatches(t *testing.T) {
	kustGK := schema.GroupKind{Group: groupKustomize, Kind: kindKust}
	platform := labels.SelectorFromSet(labels.Set{labelTier: tierPlatform})
	onlyA := func(ns string) bool { return ns == nsTeamA }
	deps := []membership.Dependency{
		{Name: depV1Platform, GVK: kustGK.WithVersion("v1"), Selector: platform},
		{Name: "v1beta2-team-a", GVK: kustGK.WithVersion("v1beta2"), Selector: labels.Everything(), NamespaceMatcher: onlyA},
		{Name: "other-kind", GVK: schema.GroupVersionKind{Group: groupRBAC, Version: "v1", Kind: kindRole}},
		{Name: "other-group", GVK: schema.GroupVersionKind{Group: "other.example.com", Version: "v1", Kind: kindKust}},
	}
	names := func(ds []membership.Dependency) []string {
		out := make([]string, 0, len(ds))
		for _, d := range ds {
			out = append(out, d.Name)
		}
		return out
	}
	cases := []struct {
		name string
		ns   string
		lbls labels.Set
		want []string
	}{
		// The object's served version is irrelevant: the operator watches one
		// version, but the object belongs regardless of which one it was read at.
		{"both versions", nsTeamA, labels.Set{labelTier: tierPlatform}, []string{depV1Platform, "v1beta2-team-a"}},
		{"namespace filter", nsTeamB, labels.Set{labelTier: tierPlatform}, []string{depV1Platform}},
		{"selector filter", nsTeamB, nil, []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := names(membership.Matches(deps, kustGK, tc.ns, tc.lbls)); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Matches = %v, want %v", got, tc.want)
			}
		})
	}
}
