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
	"reflect"
	"testing"
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/membership"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakediscovery "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/restmapper"
	clienttesting "k8s.io/client-go/testing"
)

const (
	nsOwner       = nsFlux
	ownerName     = "wave-0"
	kindCM        = "ConfigMap"
	stateCurrent  = "current"
	stateFailed   = "failed"
	stateWorking  = "working"
	resKust       = "kustomizations"
	resCM         = "configmaps"
	resRoles      = "clusterroles"
	statCurrent   = "Current"
	statFailed    = "Failed"
	statInProg    = "InProgress"
	verbList      = "list"
	condReady     = "Ready"
	keyMessage    = "message"
	keyReason     = "reason"
	keyStatus     = "status"
	keyType       = "type"
	nameBroken    = "broken"
	tierData      = "data"
	depRoles      = "roles"
	depCMs        = "cms"
	depV1Platform = "v1-platform"
)

var (
	gvrKust  = schema.GroupVersionResource{Group: groupKustomize, Version: "v1", Resource: resKust}
	gvrCM    = schema.GroupVersionResource{Version: "v1", Resource: resCM}
	gvrRoles = schema.GroupVersionResource{Group: groupRBAC, Version: "v1", Resource: resRoles}
	platform = map[string]string{labelTier: tierPlatform}
	fixedNow = time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
)

func fakeDiscovery() *fakediscovery.FakeDiscovery {
	return &fakediscovery.FakeDiscovery{
		Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{
			{GroupVersion: "v1", APIResources: []metav1.APIResource{
				{Name: resCM, Kind: kindCM, Namespaced: true, Verbs: metav1.Verbs{verbList}},
			}},
			{GroupVersion: groupKustomize + "/v1", APIResources: []metav1.APIResource{
				{Name: resKust, Kind: kindKust, Namespaced: true, Verbs: metav1.Verbs{verbList}},
			}},
			{GroupVersion: groupRBAC + "/v1", APIResources: []metav1.APIResource{
				{Name: resRoles, Kind: kindRole, Namespaced: false, Verbs: metav1.Verbs{verbList}},
			}},
		}},
	}
}

type harness struct {
	eval *membership.Evaluator
	dyn  *dynamicfake.FakeDynamicClient
}

func newHarness(t *testing.T, objs ...runtime.Object) harness {
	t.Helper()
	fd := fakeDiscovery()
	agr, err := restmapper.GetAPIGroupResources(fd)
	if err != nil {
		t.Fatalf("GetAPIGroupResources: %v", err)
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		gvrKust:  "KustomizationList",
		gvrCM:    "ConfigMapList",
		gvrRoles: "ClusterRoleList",
	}, objs...)
	return harness{
		eval: &membership.Evaluator{
			Resolver:   discovery.NewResolver(discovery.WrapClient(fd), time.Minute),
			Mapper:     restmapper.NewDiscoveryRESTMapper(agr),
			Dynamic:    dyn,
			Namespaces: namespaceLister(map[string]map[string]string{nsTeamA: platform, nsTeamB: nil, nsFlux: platform}),
			Now:        func() time.Time { return fixedNow },
		},
		dyn: dyn,
	}
}

func namespaceLister(nsLabels map[string]map[string]string) membership.NamespaceLister {
	return func(_ context.Context, sel labels.Selector) ([]string, error) {
		var out []string
		for ns, l := range nsLabels {
			if sel.Matches(labels.Set(l)) {
				out = append(out, ns)
			}
		}
		return out, nil
	}
}

type kustOpt func(*unstructured.Unstructured)

func suspended(u *unstructured.Unstructured) {
	_ = unstructured.SetNestedField(u.Object, true, "spec", "suspend")
}

func kust(ns, name, state string, lbls map[string]string, opts ...kustOpt) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(groupKustomize + "/v1")
	u.SetKind(kindKust)
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetLabels(lbls)
	u.SetGeneration(2)
	_ = unstructured.SetNestedField(u.Object, int64(2), "status", "observedGeneration")
	var conds []any
	switch state {
	case stateCurrent:
		conds = []any{map[string]any{keyType: condReady, keyStatus: "True", keyReason: "ReconciliationSucceeded"}}
	case stateFailed:
		conds = []any{
			map[string]any{keyType: condReady, keyStatus: "False", keyReason: "BuildFailed"},
			map[string]any{keyType: "Stalled", keyStatus: "True", keyReason: "BuildFailed", keyMessage: "build failed"},
		}
	case stateWorking:
		conds = []any{map[string]any{keyType: condReady, keyStatus: "False", keyReason: "Progressing", keyMessage: "working"}}
	}
	_ = unstructured.SetNestedSlice(u.Object, conds, "status", "conditions")
	for _, o := range opts {
		o(u)
	}
	return u
}

func configMap(ns, name string, lbls map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("v1")
	u.SetKind(kindCM)
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetLabels(lbls)
	return u
}

func kustDep(name string, sel map[string]string) apiv1.DependencyRef {
	d := apiv1.DependencyRef{Name: name, Target: apiv1.TargetSpec{Group: groupKustomize, Kind: kindKust}}
	if sel != nil {
		d.Target.Selector = &metav1.LabelSelector{MatchLabels: sel}
	}
	return d
}

func ownedMilestone(deps ...apiv1.DependencyRef) *apiv1.Milestone {
	return &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsOwner, Name: ownerName, Generation: 3},
		Spec:       apiv1.MilestoneSpec{DependsOn: deps},
	}
}

func ownedClusterMilestone(deps ...apiv1.ClusterDependencyRef) *apiv1.ClusterMilestone {
	return &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: ownerName, Generation: 1},
		Spec:       apiv1.ClusterMilestoneSpec{DependsOn: deps},
	}
}

func evalMilestone(t *testing.T, h harness, m *apiv1.Milestone) membership.Report {
	t.Helper()
	r, err := h.eval.Milestone(context.Background(), m)
	if err != nil {
		t.Fatalf("Milestone: %v", err)
	}
	return r
}

func evalCluster(t *testing.T, h harness, cm *apiv1.ClusterMilestone) membership.Report {
	t.Helper()
	r, err := h.eval.ClusterMilestone(context.Background(), cm)
	if err != nil {
		t.Fatalf("ClusterMilestone: %v", err)
	}
	return r
}

func memberNames(ms []membership.Member) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.Namespace+"/"+m.Name)
	}
	return out
}

func assertDep(t *testing.T, d membership.DependencyReport, ready metav1.ConditionStatus, reason string) {
	t.Helper()
	if d.Status.Ready != ready || d.Status.Reason != reason {
		t.Errorf("dependency %q: Ready=%q Reason=%q, want %q %q (error %q)",
			d.Name, d.Status.Ready, d.Status.Reason, ready, reason, d.Error)
	}
}

func TestEvaluate_AllCurrent(t *testing.T) {
	h := newHarness(t,
		kust(nsOwner, "b", stateCurrent, platform),
		kust(nsOwner, "a", stateCurrent, platform),
		kust(nsTeamA, "foreign", stateCurrent, platform),
	)
	r := evalMilestone(t, h, ownedMilestone(kustDep(depName, platform)))

	if r.Ready != metav1.ConditionTrue || r.Reason != apiv1.ReasonAllDependenciesReady || r.Message != "" {
		t.Errorf("owner = %q %q %q, want True AllDependenciesReady", r.Ready, r.Reason, r.Message)
	}
	if r.Summary != (apiv1.Summary{Total: 2, Current: 2}) {
		t.Errorf("summary = %+v", r.Summary)
	}
	wantOwner := membership.OwnerRef{Kind: "Milestone", Namespace: nsOwner, Name: ownerName, Generation: 3}
	if r.Owner != wantOwner {
		t.Errorf("owner = %+v, want %+v", r.Owner, wantOwner)
	}
	if !r.EvaluatedAt.Time.Equal(fixedNow) {
		t.Errorf("EvaluatedAt = %v, want %v", r.EvaluatedAt, fixedNow)
	}
	if len(r.Dependencies) != 1 {
		t.Fatalf("dependencies = %d, want 1", len(r.Dependencies))
	}
	d := r.Dependencies[0]
	assertDep(t, d, metav1.ConditionTrue, apiv1.ReasonAllResourcesReady)
	if got := memberNames(d.Members); !reflect.DeepEqual(got, []string{nsOwner + "/a", nsOwner + "/b"}) {
		t.Errorf("members = %v, want own-namespace members sorted", got)
	}
	for _, m := range d.Members {
		if m.Blocking {
			t.Errorf("member %s blocking, want not", m.Name)
		}
	}
	wantTarget := membership.TargetRef{Group: groupKustomize, Kind: kindKust, Selector: labelTier + "=" + tierPlatform}
	if !reflect.DeepEqual(d.Target, wantTarget) {
		t.Errorf("target = %+v, want %+v", d.Target, wantTarget)
	}
	if d.Status.Version != "v1" {
		t.Errorf("live version = %q, want resolved v1", d.Status.Version)
	}
}

func TestEvaluate_MixedFailedAndInProgress(t *testing.T) {
	h := newHarness(t,
		kust(nsOwner, "ok", stateCurrent, platform),
		kust(nsOwner, nameBroken, stateFailed, platform),
		kust(nsOwner, "busy", stateWorking, platform),
	)
	r := evalMilestone(t, h, ownedMilestone(kustDep(depName, platform)))

	d := r.Dependencies[0]
	assertDep(t, d, metav1.ConditionFalse, apiv1.ReasonResourcesNotReady)
	if d.Status.Summary != (apiv1.Summary{Total: 3, Current: 1, Failed: 1, InProgress: 1}) {
		t.Errorf("summary = %+v", d.Status.Summary)
	}
	blocking := map[string]bool{}
	statuses := map[string]string{}
	for _, m := range d.Members {
		blocking[m.Name] = m.Blocking
		statuses[m.Name] = m.Status
	}
	if !reflect.DeepEqual(blocking, map[string]bool{"ok": false, nameBroken: true, "busy": true}) {
		t.Errorf("blocking = %v", blocking)
	}
	if statuses[nameBroken] != statFailed || statuses["busy"] != statInProg {
		t.Errorf("statuses = %v", statuses)
	}
	if r.Ready != metav1.ConditionFalse || r.Reason != apiv1.ReasonDependenciesNotReady ||
		r.Message != "dependencies not ready: "+depName {
		t.Errorf("owner = %q %q %q", r.Ready, r.Reason, r.Message)
	}
}

func TestEvaluate_Suspended(t *testing.T) {
	cases := []struct {
		policy   apiv1.SuspendPolicy
		ready    metav1.ConditionStatus
		reason   string
		blocking bool
	}{
		{apiv1.SuspendIgnore, metav1.ConditionTrue, apiv1.ReasonAllResourcesReady, false},
		{apiv1.SuspendNotReady, metav1.ConditionFalse, apiv1.ReasonResourcesSuspended, true},
	}
	for _, tc := range cases {
		t.Run(string(tc.policy), func(t *testing.T) {
			h := newHarness(t, kust(nsOwner, "paused", stateCurrent, platform, suspended))
			dep := kustDep(depName, platform)
			dep.SuspendPolicy = tc.policy
			r := evalMilestone(t, h, ownedMilestone(dep))

			d := r.Dependencies[0]
			assertDep(t, d, tc.ready, tc.reason)
			if d.Status.Summary.Suspended != 1 || r.Summary.Suspended != 1 {
				t.Errorf("suspended count = %d (owner %d), want 1", d.Status.Summary.Suspended, r.Summary.Suspended)
			}
			if m := d.Members[0]; m.Blocking != tc.blocking || !m.Suspended {
				t.Errorf("member blocking=%v suspended=%v, want blocking=%v suspended", m.Blocking, m.Suspended, tc.blocking)
			}
		})
	}
}

func TestEvaluate_EmptySet(t *testing.T) {
	cases := []struct {
		policy apiv1.EmptySetPolicy
		ready  metav1.ConditionStatus
	}{
		{"", metav1.ConditionUnknown},
		{apiv1.EmptySetUnknown, metav1.ConditionUnknown},
		{apiv1.EmptySetReady, metav1.ConditionTrue},
		{apiv1.EmptySetNotReady, metav1.ConditionFalse},
	}
	for _, tc := range cases {
		t.Run(string(tc.policy), func(t *testing.T) {
			h := newHarness(t, kust(nsOwner, "other", stateCurrent, map[string]string{labelTier: tierData}))
			dep := kustDep(depName, platform)
			dep.EmptySetPolicy = tc.policy
			r := evalMilestone(t, h, ownedMilestone(dep))

			d := r.Dependencies[0]
			assertDep(t, d, tc.ready, apiv1.ReasonEmptySet)
			if len(d.Members) != 0 {
				t.Errorf("members = %v, want none", memberNames(d.Members))
			}
			if r.Ready != tc.ready {
				t.Errorf("owner Ready = %q, want %q", r.Ready, tc.ready)
			}
		})
	}
}

func TestEvaluate_MissingCRD(t *testing.T) {
	h := newHarness(t, kust(nsOwner, "ok", stateCurrent, platform))
	missing := apiv1.DependencyRef{Name: depMissing, Target: apiv1.TargetSpec{Group: groupMissing, Version: "v1", Kind: kindLate}}
	r := evalMilestone(t, h, ownedMilestone(kustDep(depOK, platform), missing))

	if got := []string{r.Dependencies[0].Name, r.Dependencies[1].Name}; !reflect.DeepEqual(got, []string{depOK, depMissing}) {
		t.Errorf("dependency order = %v, want spec order", got)
	}
	assertDep(t, r.Dependencies[0], metav1.ConditionTrue, apiv1.ReasonAllResourcesReady)
	d := r.Dependencies[1]
	assertDep(t, d, metav1.ConditionUnknown, apiv1.ReasonGVKNotEstablished)
	if d.Error == "" {
		t.Errorf("missing CRD must carry an error message")
	}
	if d.Status.Group != groupMissing || d.Status.Kind != kindLate || d.Status.Version != "v1" {
		t.Errorf("stand-in identity = %+v", d.Status)
	}
	if r.Ready != metav1.ConditionUnknown || r.Reason != apiv1.ReasonDependenciesInProgress {
		t.Errorf("owner = %q %q, want Unknown DependenciesInProgress", r.Ready, r.Reason)
	}
}

func TestEvaluate_NamespaceScopeMismatch(t *testing.T) {
	h := newHarness(t)
	roles := apiv1.DependencyRef{Name: depRoles, Target: apiv1.TargetSpec{Group: groupRBAC, Kind: kindRole}}
	r := evalMilestone(t, h, ownedMilestone(roles))

	assertDep(t, r.Dependencies[0], metav1.ConditionUnknown, apiv1.ReasonNamespaceScopeMismatch)
	if len(h.dyn.Actions()) != 0 {
		t.Errorf("structural failure must not list; actions = %v", h.dyn.Actions())
	}
}

func TestEvaluate_ListErrors(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		forbidden bool
	}{
		{"forbidden", apierrors.NewForbidden(gvrKust.GroupResource(), "", errors.New("rbac")), true},
		{"generic", errors.New("connection refused"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, configMap(nsOwner, "cm", platform))
			h.dyn.PrependReactor(verbList, resKust, func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, tc.err
			})
			cms := apiv1.DependencyRef{Name: depCMs, Target: apiv1.TargetSpec{Kind: kindCM}}
			r := evalMilestone(t, h, ownedMilestone(kustDep(depName, nil), cms))

			d := r.Dependencies[0]
			assertDep(t, d, metav1.ConditionUnknown, apiv1.ReasonListFailed)
			if d.Forbidden != tc.forbidden {
				t.Errorf("Forbidden = %v, want %v", d.Forbidden, tc.forbidden)
			}
			if d.Error == "" {
				t.Errorf("list failure must carry an error message")
			}
			if d.Status.Version != "v1" {
				t.Errorf("list failure keeps resolved version; got %q", d.Status.Version)
			}
			// One failed dependency never stops the rest being evaluated.
			assertDep(t, r.Dependencies[1], metav1.ConditionTrue, apiv1.ReasonAllResourcesReady)
			if r.Ready != metav1.ConditionUnknown {
				t.Errorf("owner Ready = %q, want Unknown", r.Ready)
			}
		})
	}
}

func listNamespaces(dyn *dynamicfake.FakeDynamicClient, resource string) []string {
	var out []string
	for _, a := range dyn.Actions() {
		if a.GetVerb() == verbList && a.GetResource().Resource == resource {
			out = append(out, a.GetNamespace())
		}
	}
	return out
}

func TestEvaluate_ClusterMilestoneNamespaceFanOut(t *testing.T) {
	h := newHarness(t,
		configMap(nsTeamA, "a", platform),
		configMap(nsTeamB, "b", platform),
		configMap(nsFlux, "f", platform),
	)
	cm := ownedClusterMilestone(apiv1.ClusterDependencyRef{Name: depCMs, Target: apiv1.ClusterTargetSpec{
		TargetSpec: apiv1.TargetSpec{Kind: kindCM},
		Namespaces: []string{nsTeamB, nsTeamA},
	}})
	r := evalCluster(t, h, cm)

	if got := listNamespaces(h.dyn, resCM); !reflect.DeepEqual(got, []string{nsTeamB, nsTeamA}) {
		t.Errorf("list namespaces = %v, want one list per namespace", got)
	}
	d := r.Dependencies[0]
	if got := memberNames(d.Members); !reflect.DeepEqual(got, []string{nsTeamA + "/a", nsTeamB + "/b"}) {
		t.Errorf("members = %v", got)
	}
	if !reflect.DeepEqual(d.Target.Namespaces, []string{nsTeamB, nsTeamA}) {
		t.Errorf("target namespaces = %v", d.Target.Namespaces)
	}
	if r.Owner.Kind != "ClusterMilestone" || r.Owner.Namespace != "" {
		t.Errorf("owner = %+v", r.Owner)
	}
}

func TestEvaluate_ClusterMilestoneNamespaceSelector(t *testing.T) {
	h := newHarness(t,
		configMap(nsTeamA, "a", platform),
		configMap(nsTeamB, "b", platform),
		configMap(nsFlux, "f", platform),
	)
	cm := ownedClusterMilestone(apiv1.ClusterDependencyRef{Name: depCMs, Target: apiv1.ClusterTargetSpec{
		TargetSpec:        apiv1.TargetSpec{Kind: kindCM},
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: platform},
	}})
	r := evalCluster(t, h, cm)

	if got := listNamespaces(h.dyn, resCM); !reflect.DeepEqual(got, []string{""}) {
		t.Errorf("list namespaces = %v, want a single cluster-wide list", got)
	}
	d := r.Dependencies[0]
	if got := memberNames(d.Members); !reflect.DeepEqual(got, []string{nsFlux + "/f", nsTeamA + "/a"}) {
		t.Errorf("members = %v, want only selected namespaces", got)
	}
	if d.Target.NamespaceSelector != labelTier+"="+tierPlatform {
		t.Errorf("target namespaceSelector = %q", d.Target.NamespaceSelector)
	}
}

func TestEvaluate_ClusterScopedKind(t *testing.T) {
	role := &unstructured.Unstructured{}
	role.SetAPIVersion(groupRBAC + "/v1")
	role.SetKind(kindRole)
	role.SetName("admin")
	h := newHarness(t, role)
	cm := ownedClusterMilestone(apiv1.ClusterDependencyRef{Name: depRoles, Target: apiv1.ClusterTargetSpec{
		TargetSpec: apiv1.TargetSpec{Group: groupRBAC, Kind: kindRole},
	}})
	r := evalCluster(t, h, cm)

	assertDep(t, r.Dependencies[0], metav1.ConditionTrue, apiv1.ReasonAllResourcesReady)
	if got := memberNames(r.Dependencies[0].Members); !reflect.DeepEqual(got, []string{"/admin"}) {
		t.Errorf("members = %v", got)
	}
}

func TestEvaluate_LabelSelectorServerSide(t *testing.T) {
	h := newHarness(t,
		kust(nsOwner, "in", stateCurrent, platform),
		kust(nsOwner, "out", stateFailed, nil),
	)
	r := evalMilestone(t, h, ownedMilestone(kustDep(depName, platform)))

	var sel []string
	for _, a := range h.dyn.Actions() {
		if la, ok := a.(clienttesting.ListAction); ok {
			sel = append(sel, la.GetListRestrictions().Labels.String())
		}
	}
	if !reflect.DeepEqual(sel, []string{labelTier + "=" + tierPlatform}) {
		t.Errorf("list label selectors = %v, want selector pushed to the server", sel)
	}
	assertDep(t, r.Dependencies[0], metav1.ConditionTrue, apiv1.ReasonAllResourcesReady)
}

// The server-side selector is an optimisation: Admits still decides
// membership, so a server that ignores the selector cannot widen the set.
func TestEvaluate_AdmitsAppliedAfterServerList(t *testing.T) {
	h := newHarness(t)
	h.dyn.PrependReactor(verbList, resKust, func(clienttesting.Action) (bool, runtime.Object, error) {
		l := &unstructured.UnstructuredList{}
		l.SetAPIVersion(groupKustomize + "/v1")
		l.SetKind("KustomizationList")
		l.Items = []unstructured.Unstructured{
			*kust(nsOwner, "in", stateCurrent, platform),
			*kust(nsOwner, "out", stateFailed, nil),
		}
		return true, l, nil
	})
	r := evalMilestone(t, h, ownedMilestone(kustDep(depName, platform)))

	if got := memberNames(r.Dependencies[0].Members); !reflect.DeepEqual(got, []string{nsOwner + "/in"}) {
		t.Errorf("members = %v, want only admitted objects", got)
	}
}

func TestEvaluate_MultipleDependenciesVerdict(t *testing.T) {
	h := newHarness(t,
		kust(nsOwner, "ok", stateCurrent, platform),
		configMap(nsOwner, "cm", nil),
	)
	cms := apiv1.DependencyRef{Name: depCMs, Target: apiv1.TargetSpec{Kind: kindCM}}
	bad := apiv1.DependencyRef{Name: "bad", Target: apiv1.TargetSpec{Kind: kindCM,
		Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "x", Operator: "Bogus"}}}}}
	r := evalMilestone(t, h, ownedMilestone(kustDep(depName, platform), cms, bad))

	if len(r.Dependencies) != 3 {
		t.Fatalf("dependencies = %d, want 3", len(r.Dependencies))
	}
	assertDep(t, r.Dependencies[2], metav1.ConditionUnknown, apiv1.ReasonDiscoveryFailed)
	if r.Ready != metav1.ConditionUnknown || r.Reason != apiv1.ReasonDependenciesInProgress {
		t.Errorf("owner = %q %q, want Unknown DependenciesInProgress", r.Ready, r.Reason)
	}
	if r.Summary != (apiv1.Summary{Total: 2, Current: 2}) {
		t.Errorf("summary = %+v", r.Summary)
	}
}

func TestEvaluate_NoDependencies(t *testing.T) {
	r := evalMilestone(t, newHarness(t), ownedMilestone())
	if r.Ready != metav1.ConditionUnknown || r.Reason != apiv1.ReasonEmptySet || r.Dependencies == nil {
		t.Errorf("owner = %q %q deps=%v, want Unknown EmptySet with empty (non-nil) dependencies",
			r.Ready, r.Reason, r.Dependencies)
	}
}

func TestEvaluate_NilResolver(t *testing.T) {
	h := newHarness(t)
	h.eval.Resolver = nil
	if _, err := h.eval.Milestone(context.Background(), ownedMilestone(kustDep(depName, nil))); err == nil {
		t.Errorf("nil resolver must fail the evaluation")
	}
}

// resettingMapper reports NoKindMatch until Reset, mimicking a cached mapper
// that predates a freshly installed CRD.
type resettingMapper struct {
	apimeta.RESTMapper
	resets int
}

func (m *resettingMapper) Reset() { m.resets++ }

func (m *resettingMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*apimeta.RESTMapping, error) {
	if m.resets == 0 {
		return nil, &apimeta.NoKindMatchError{GroupKind: gk, SearchedVersions: versions}
	}
	return m.RESTMapper.RESTMapping(gk, versions...)
}

func TestEvaluate_MapperResetOnNoKindMatch(t *testing.T) {
	h := newHarness(t, kust(nsOwner, "ok", stateCurrent, nil))
	m := &resettingMapper{RESTMapper: h.eval.Mapper}
	h.eval.Mapper = m
	r := evalMilestone(t, h, ownedMilestone(kustDep(depName, nil)))

	if m.resets != 1 {
		t.Errorf("resets = %d, want 1", m.resets)
	}
	assertDep(t, r.Dependencies[0], metav1.ConditionTrue, apiv1.ReasonAllResourcesReady)
}

func TestEvaluate_UnmappableKind(t *testing.T) {
	h := newHarness(t)
	h.eval.Mapper = apimeta.NewDefaultRESTMapper(nil)
	r := evalMilestone(t, h, ownedMilestone(kustDep(depName, nil)))

	d := r.Dependencies[0]
	assertDep(t, d, metav1.ConditionUnknown, apiv1.ReasonWatchSetupFailed)
	if d.Error == "" {
		t.Errorf("mapping failure must carry an error message")
	}
}

// The mapper must be asked for the resolver's version, which is what the
// operator's informer watches, not whatever version the mapper prefers.
func TestEvaluate_UsesResolvedVersion(t *testing.T) {
	h := newHarness(t, kust(nsOwner, "ok", stateCurrent, nil))
	rec := &recordingMapper{RESTMapper: h.eval.Mapper}
	h.eval.Mapper = rec
	evalMilestone(t, h, ownedMilestone(kustDep(depName, nil)))

	if !reflect.DeepEqual(rec.versions, [][]string{{"v1"}}) {
		t.Errorf("RESTMapping versions = %v, want [[v1]]", rec.versions)
	}
}

type recordingMapper struct {
	apimeta.RESTMapper
	versions [][]string
}

func (m *recordingMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*apimeta.RESTMapping, error) {
	m.versions = append(m.versions, versions)
	return m.RESTMapper.RESTMapping(gk, versions...)
}
