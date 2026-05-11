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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	"github.com/isometry/echelon-operator/internal/controller"
	"github.com/isometry/echelon-operator/internal/discovery"
	"github.com/isometry/echelon-operator/internal/watcher"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// fakeRegistry tracks subscribe/unsubscribe lifecycle and serves fixed lists
// per GVK.
type fakeRegistry struct {
	mu            sync.Mutex
	subscribed    map[watcher.OwnerKey]map[schema.GroupVersionKind]watcher.Subscriber
	subscribeErr  map[schema.GroupVersionKind]error
	listResponses map[schema.GroupVersionKind][]*unstructured.Unstructured
	subscribeOps  []schema.GroupVersionKind
	unsubOps      []schema.GroupVersionKind
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{
		subscribed:    make(map[watcher.OwnerKey]map[schema.GroupVersionKind]watcher.Subscriber),
		subscribeErr:  make(map[schema.GroupVersionKind]error),
		listResponses: make(map[schema.GroupVersionKind][]*unstructured.Unstructured),
	}
}

func (r *fakeRegistry) Subscribe(gvk schema.GroupVersionKind, _ apimeta.RESTScopeName, sub watcher.Subscriber) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.subscribeErr[gvk]; err != nil {
		return err
	}
	if _, ok := r.subscribed[sub.Owner]; !ok {
		r.subscribed[sub.Owner] = make(map[schema.GroupVersionKind]watcher.Subscriber)
	}
	r.subscribed[sub.Owner][gvk] = sub
	r.subscribeOps = append(r.subscribeOps, gvk)
	return nil
}

func (r *fakeRegistry) Unsubscribe(gvk schema.GroupVersionKind, owner watcher.OwnerKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if subs, ok := r.subscribed[owner]; ok {
		delete(subs, gvk)
		if len(subs) == 0 {
			delete(r.subscribed, owner)
		}
	}
	r.unsubOps = append(r.unsubOps, gvk)
}

func (r *fakeRegistry) UnsubscribeAll(owner watcher.OwnerKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for gvk := range r.subscribed[owner] {
		r.unsubOps = append(r.unsubOps, gvk)
	}
	delete(r.subscribed, owner)
}

func (r *fakeRegistry) List(gvk schema.GroupVersionKind) ([]*unstructured.Unstructured, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.listResponses[gvk], nil
}

func (r *fakeRegistry) GVKsByOwner(owner watcher.OwnerKey) []schema.GroupVersionKind {
	r.mu.Lock()
	defer r.mu.Unlock()
	subs, ok := r.subscribed[owner]
	if !ok {
		return nil
	}
	out := make([]schema.GroupVersionKind, 0, len(subs))
	for g := range subs {
		out = append(out, g)
	}
	return out
}

// fakeAdapter wraps an apiv1.Echelon for testing purposes; fixed targets and a
// fixed set of pre-resolved errors. PatchStatus persists to the fake client.
type fakeAdapter struct {
	obj      *apiv1.Echelon
	targets  []controller.NormalizedTarget
	errs     []controller.TargetError
	patchErr error
	patches  int
}

func (a *fakeAdapter) Object() client.Object { return a.obj }
func (a *fakeAdapter) OwnerKey() watcher.OwnerKey {
	return watcher.OwnerKey{Kind: kindEchelon, Namespace: a.obj.GetNamespace(), Name: a.obj.GetName()}
}
func (a *fakeAdapter) Targets(_ context.Context, _ discovery.Resolver) ([]controller.NormalizedTarget, []controller.TargetError) {
	return a.targets, a.errs
}
func (a *fakeAdapter) Status() *apiv1.EchelonStatusBase { return &a.obj.Status.EchelonStatusBase }
func (a *fakeAdapter) PatchStatus(ctx context.Context, c client.Client) error {
	a.patches++
	if a.patchErr != nil {
		return a.patchErr
	}
	return c.Status().Update(ctx, a.obj)
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sc := runtime.NewScheme()
	if err := apiv1.AddToScheme(sc); err != nil {
		t.Fatalf("AddToScheme apiv1: %v", err)
	}
	if err := corev1.AddToScheme(sc); err != nil {
		t.Fatalf("AddToScheme corev1: %v", err)
	}
	return sc
}

func newEchelon(name string) *apiv1.Echelon {
	return &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: name, Generation: 1},
		Spec:       apiv1.EchelonSpec{Targets: []apiv1.TargetSpec{{Kind: kindKustomization, Group: groupKustomize}}},
	}
}

func mustSelector(t *testing.T) labels.Selector {
	t.Helper()
	return labels.Everything()
}

func currentMember(name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(gvKustomizeV1)
	u.SetKind(kindKustomization)
	u.SetNamespace(nsFluxSystem)
	u.SetName(name)
	u.SetGeneration(1)
	_ = unstructured.SetNestedField(u.Object, int64(1), schemaPropStatus, "observedGeneration")
	_ = unstructured.SetNestedSlice(u.Object, []any{
		map[string]any{keyType: apiv1.ConditionReady, schemaPropStatus: "True", keyReason: "ReconciliationSucceeded"},
	}, schemaPropStatus, "conditions")
	return u
}

var kustomizationGVK = schema.GroupVersionKind{Group: groupKustomize, Version: "v1", Kind: kindKustomization}

func newFixture(t *testing.T, ech *apiv1.Echelon, fa *fakeAdapter, freg *fakeRegistry) *controller.Reconciler {
	t.Helper()
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(ech).WithStatusSubresource(ech).Build()
	return &controller.Reconciler{
		Client:     cl,
		Registry:   freg,
		Resolver:   nil, // unused: fakeAdapter pre-resolves targets
		NewAdapter: func(_ client.Object) controller.OwnerAdapter { return fa },
		Controller: kindEchelon,
		Now:        func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	}
}

func TestReconcile_AddsFinalizer(t *testing.T) {
	ech := newEchelon("e1")
	fa := &fakeAdapter{obj: ech}
	r := newFixture(t, ech, fa, newFakeRegistry())

	if _, err := r.ReconcileObject(t.Context(), ech); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	found := false
	for _, f := range ech.GetFinalizers() {
		if f == apiv1.Finalizer {
			found = true
		}
	}
	if !found {
		t.Errorf("finalizer not added: %v", ech.GetFinalizers())
	}
}

func TestReconcile_HappyPath_AllCurrent(t *testing.T) {
	ech := newEchelon("e1")
	ech.Finalizers = []string{apiv1.Finalizer}
	freg := newFakeRegistry()
	freg.listResponses[kustomizationGVK] = []*unstructured.Unstructured{currentMember("a"), currentMember("b")}
	fa := &fakeAdapter{
		obj: ech,
		targets: []controller.NormalizedTarget{{
			Index: 0, GVK: kustomizationGVK, Scope: apimeta.RESTScopeNameNamespace,
			Selector: mustSelector(t), EmptySetPolicy: apiv1.EmptySetUnknown,
		}},
	}
	r := newFixture(t, ech, fa, freg)

	if _, err := r.ReconcileObject(t.Context(), ech); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := readyStatusOf(ech); got != metav1.ConditionTrue {
		t.Errorf("Ready = %s, want True", got)
	}
	if ech.Status.Summary.Total != 2 || ech.Status.Summary.Current != 2 {
		t.Errorf("summary = %+v, want total=2 current=2", ech.Status.Summary)
	}
	if fa.patches != 1 {
		t.Errorf("patches = %d, want 1", fa.patches)
	}
}

func TestReconcile_EmptySet_AppliesPolicy(t *testing.T) {
	ech := newEchelon("e1")
	ech.Finalizers = []string{apiv1.Finalizer}
	freg := newFakeRegistry()
	fa := &fakeAdapter{
		obj: ech,
		targets: []controller.NormalizedTarget{{
			Index: 0, GVK: kustomizationGVK, Scope: apimeta.RESTScopeNameNamespace,
			Selector: mustSelector(t), EmptySetPolicy: apiv1.EmptySetNotReady,
		}},
	}
	r := newFixture(t, ech, fa, freg)

	if _, err := r.ReconcileObject(t.Context(), ech); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := readyStatusOf(ech); got != metav1.ConditionFalse {
		t.Errorf("Ready = %s, want False (NotReady policy)", got)
	}
}

func TestReconcile_Deletion_RunsFinalizer(t *testing.T) {
	ech := newEchelon("e1")
	ech.Finalizers = []string{apiv1.Finalizer}
	now := metav1.Now()
	ech.DeletionTimestamp = &now
	freg := newFakeRegistry()
	freg.subscribed[watcher.OwnerKey{Kind: kindEchelon, Namespace: nsFluxSystem, Name: "e1"}] = map[schema.GroupVersionKind]watcher.Subscriber{
		kustomizationGVK: {Owner: watcher.OwnerKey{Kind: kindEchelon, Namespace: nsFluxSystem, Name: "e1"}},
	}
	fa := &fakeAdapter{obj: ech}
	r := newFixture(t, ech, fa, freg)

	if _, err := r.ReconcileObject(t.Context(), ech); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(freg.unsubOps) != 1 {
		t.Errorf("unsubscribe ops = %d, want 1", len(freg.unsubOps))
	}
	for _, f := range ech.GetFinalizers() {
		if f == apiv1.Finalizer {
			t.Errorf("finalizer still present after deletion")
		}
	}
}

func TestReconcile_SubscriptionDiff_RemovesStale(t *testing.T) {
	ech := newEchelon("e1")
	ech.Finalizers = []string{apiv1.Finalizer}
	freg := newFakeRegistry()
	owner := watcher.OwnerKey{Kind: kindEchelon, Namespace: nsFluxSystem, Name: "e1"}
	helmGVK := schema.GroupVersionKind{Group: "helm.toolkit.fluxcd.io", Version: "v2", Kind: "HelmRelease"}
	// Pre-subscribe owner to two GVKs; spec only carries Kustomization.
	freg.subscribed[owner] = map[schema.GroupVersionKind]watcher.Subscriber{
		kustomizationGVK: {Owner: owner},
		helmGVK:          {Owner: owner},
	}
	fa := &fakeAdapter{
		obj: ech,
		targets: []controller.NormalizedTarget{{
			Index: 0, GVK: kustomizationGVK, Scope: apimeta.RESTScopeNameNamespace,
			Selector: mustSelector(t),
		}},
	}
	r := newFixture(t, ech, fa, freg)

	if _, err := r.ReconcileObject(t.Context(), ech); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	found := false
	for _, g := range freg.unsubOps {
		if g == helmGVK {
			found = true
		}
	}
	if !found {
		t.Errorf("expected unsubscribe for stale GVK %v; ops=%v", helmGVK, freg.unsubOps)
	}
}

func TestReconcile_TargetResolveError_SetsStalled(t *testing.T) {
	ech := newEchelon("e1")
	ech.Finalizers = []string{apiv1.Finalizer}
	fa := &fakeAdapter{
		obj: ech,
		errs: []controller.TargetError{{
			Index: 0, Group: groupMissing, Version: "v1", Kind: kindLate,
			Reason: apiv1.ReasonGVKNotEstablished,
			Err:    errors.New("not established"),
		}},
	}
	r := newFixture(t, ech, fa, newFakeRegistry())

	res, err := r.ReconcileObject(t.Context(), ech)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("expected RequeueAfter when stalled")
	}
	if !hasCondition(ech, apiv1.ConditionStalled, metav1.ConditionTrue) {
		t.Errorf("expected Stalled=True; conditions=%+v", ech.Status.Conditions)
	}
}

// TestReconcile_StalledMessage_TruncatesManyErrors guards the API-server
// condition-message size limit: even a pathological owner with hundreds of
// misconfigured targets must produce a bounded Stalled message with an
// explicit overflow suffix.
func TestReconcile_StalledMessage_TruncatesManyErrors(t *testing.T) {
	const cap = 50
	const overflow = 10
	ech := newEchelon("e1")
	ech.Finalizers = []string{apiv1.Finalizer}

	errs := make([]controller.TargetError, 0, cap+overflow)
	for i := range cap + overflow {
		errs = append(errs, controller.TargetError{
			Index:   i,
			Group:   groupMissing,
			Version: "v1",
			Kind:    "Kind" + intToStr(i),
			Reason:  apiv1.ReasonGVKNotEstablished,
			Err:     fmt.Errorf("not established %d", i),
		})
	}
	fa := &fakeAdapter{obj: ech, errs: errs}
	r := newFixture(t, ech, fa, newFakeRegistry())

	if _, err := r.ReconcileObject(t.Context(), ech); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	msg := stalledMessageOf(ech)
	if !strings.Contains(msg, fmt.Sprintf("… %d more", overflow)) {
		t.Errorf("expected overflow suffix '… %d more' in message; got %q", overflow, msg)
	}
	// Exactly cap entries before the suffix: count separators in the prefix.
	prefix, _, found := strings.Cut(msg, "; … ")
	if !found {
		t.Fatalf("expected '; … ' separator in message; got %q", msg)
	}
	if parts := strings.Split(prefix, "; "); len(parts) != cap {
		t.Errorf("expected %d entries before suffix, got %d", cap, len(parts))
	}
	// Beyond-cap kinds must not appear; in-cap kinds must.
	if strings.Contains(msg, fmt.Sprintf("Kind%d ", cap+overflow-1)) {
		t.Errorf("beyond-cap kind leaked into message: %q", msg)
	}
	if !strings.Contains(msg, "Kind0 ") {
		t.Errorf("in-cap kind missing from message: %q", msg)
	}
}

func TestReconcile_StalledMessage_NoTruncationWhenWithinCap(t *testing.T) {
	ech := newEchelon("e1")
	ech.Finalizers = []string{apiv1.Finalizer}
	errs := []controller.TargetError{
		{Index: 0, Group: groupMissing, Version: "v1", Kind: "K0",
			Reason: apiv1.ReasonGVKNotEstablished, Err: errors.New("nope")},
		{Index: 1, Group: groupMissing, Version: "v1", Kind: "K1",
			Reason: apiv1.ReasonGVKNotEstablished, Err: errors.New("nope")},
	}
	fa := &fakeAdapter{obj: ech, errs: errs}
	r := newFixture(t, ech, fa, newFakeRegistry())

	if _, err := r.ReconcileObject(t.Context(), ech); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if msg := stalledMessageOf(ech); strings.Contains(msg, "more") {
		t.Errorf("did not expect truncation marker; got %q", msg)
	}
}

func TestReconcile_PatchIdempotency(t *testing.T) {
	ech := newEchelon("e1")
	ech.Finalizers = []string{apiv1.Finalizer}
	freg := newFakeRegistry()
	freg.listResponses[kustomizationGVK] = []*unstructured.Unstructured{currentMember("a")}
	fa := &fakeAdapter{
		obj: ech,
		targets: []controller.NormalizedTarget{{
			Index: 0, GVK: kustomizationGVK, Scope: apimeta.RESTScopeNameNamespace,
			Selector: mustSelector(t), EmptySetPolicy: apiv1.EmptySetUnknown,
		}},
	}
	r := newFixture(t, ech, fa, freg)

	if _, err := r.ReconcileObject(t.Context(), ech); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	patches1 := fa.patches
	if _, err := r.ReconcileObject(t.Context(), ech); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if fa.patches != patches1 {
		t.Errorf("idempotency violated: patches went from %d to %d", patches1, fa.patches)
	}
}

func TestReconcile_SubscribeFailure_SetsStalled(t *testing.T) {
	ech := newEchelon("e1")
	ech.Finalizers = []string{apiv1.Finalizer}
	freg := newFakeRegistry()
	freg.subscribeErr[kustomizationGVK] = errors.New("RBAC denied")
	fa := &fakeAdapter{
		obj: ech,
		targets: []controller.NormalizedTarget{{
			Index: 0, GVK: kustomizationGVK, Scope: apimeta.RESTScopeNameNamespace,
			Selector: mustSelector(t),
		}},
	}
	r := newFixture(t, ech, fa, freg)

	if _, err := r.ReconcileObject(t.Context(), ech); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !hasCondition(ech, apiv1.ConditionStalled, metav1.ConditionTrue) {
		t.Errorf("expected Stalled=True after subscribe failure")
	}
}

func TestReconcile_CapsNotReadyMembers(t *testing.T) {
	ech := newEchelon("e1")
	ech.Finalizers = []string{apiv1.Finalizer}
	freg := newFakeRegistry()
	// 60 explicitly not-ready members; cap is 50.
	members := make([]*unstructured.Unstructured, 0, 60)
	for i := range 60 {
		u := &unstructured.Unstructured{}
		u.SetAPIVersion(gvKustomizeV1)
		u.SetKind(kindKustomization)
		u.SetNamespace(nsFluxSystem)
		u.SetName(intToStr(i))
		u.SetGeneration(2)
		_ = unstructured.SetNestedField(u.Object, int64(2), schemaPropStatus, "observedGeneration")
		_ = unstructured.SetNestedSlice(u.Object, []any{
			map[string]any{keyType: apiv1.ConditionReady, schemaPropStatus: "False", keyReason: "Reconciling"},
		}, schemaPropStatus, "conditions")
		members = append(members, u)
	}
	freg.listResponses[kustomizationGVK] = members
	fa := &fakeAdapter{
		obj: ech,
		targets: []controller.NormalizedTarget{{
			Index: 0, GVK: kustomizationGVK, Scope: apimeta.RESTScopeNameNamespace,
			Selector: mustSelector(t),
		}},
	}
	r := newFixture(t, ech, fa, freg)

	if _, err := r.ReconcileObject(t.Context(), ech); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := len(ech.Status.NotReadyMembers); got != 50 {
		t.Errorf("NotReadyMembers len = %d, want 50 (capped)", got)
	}
	if !ech.Status.Truncated {
		t.Errorf("Truncated should be true when capped")
	}
}

func TestReconcile_NotFoundFromGet_NoOp(t *testing.T) {
	// AsReconcileFunc should swallow IsNotFound and return no error.
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	r := &controller.Reconciler{
		Client:     cl,
		Registry:   newFakeRegistry(),
		NewAdapter: func(_ client.Object) controller.OwnerAdapter { return nil },
		Controller: kindEchelon,
	}
	rf := r.AsReconcileFunc(func() client.Object { return &apiv1.Echelon{} })
	res, err := rf(t.Context(), reconcileRequest(nsFluxSystem, "missing"))
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if res != (reconcile.Result{}) {
		t.Errorf("expected zero result, got %+v", res)
	}
}

// TestReconcile_MultipleEchelons_IndependentStatus drives two owners through
// the pipeline against a shared registry and asserts each computes its own
// status. e1 watches Kustomizations (one Current member ⇒ Ready=True); e2
// watches HelmReleases with an empty member set under the NotReady policy
// (⇒ Ready=False). Independence means: shared registry, distinct outcomes.
func TestReconcile_MultipleEchelons_IndependentStatus(t *testing.T) {
	helmGVK := schema.GroupVersionKind{Group: "helm.toolkit.fluxcd.io", Version: "v2", Kind: "HelmRelease"}

	e1 := newEchelon("e1")
	e1.Finalizers = []string{apiv1.Finalizer}
	e2 := newEchelon("e2")
	e2.Finalizers = []string{apiv1.Finalizer}

	freg := newFakeRegistry()
	freg.listResponses[kustomizationGVK] = []*unstructured.Unstructured{currentMember("a")}
	// helmGVK has no listResponses entry → empty set.

	fa1 := &fakeAdapter{obj: e1, targets: []controller.NormalizedTarget{{
		Index: 0, GVK: kustomizationGVK, Scope: apimeta.RESTScopeNameNamespace,
		Selector: mustSelector(t), EmptySetPolicy: apiv1.EmptySetUnknown,
	}}}
	fa2 := &fakeAdapter{obj: e2, targets: []controller.NormalizedTarget{{
		Index: 0, GVK: helmGVK, Scope: apimeta.RESTScopeNameNamespace,
		Selector: mustSelector(t), EmptySetPolicy: apiv1.EmptySetNotReady,
	}}}

	cl := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(e1, e2).
		WithStatusSubresource(e1, e2).
		Build()
	r := &controller.Reconciler{
		Client:   cl,
		Registry: freg,
		Resolver: nil,
		NewAdapter: func(obj client.Object) controller.OwnerAdapter {
			if obj.GetName() == "e1" {
				return fa1
			}
			return fa2
		},
		Controller: kindEchelon,
		Now:        func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	}

	if _, err := r.ReconcileObject(t.Context(), e1); err != nil {
		t.Fatalf("e1 Reconcile: %v", err)
	}
	if _, err := r.ReconcileObject(t.Context(), e2); err != nil {
		t.Fatalf("e2 Reconcile: %v", err)
	}

	if got := readyStatusOf(e1); got != metav1.ConditionTrue {
		t.Errorf("e1 Ready=%s, want True; conditions=%+v", got, e1.Status.Conditions)
	}
	if got := readyStatusOf(e2); got != metav1.ConditionFalse {
		t.Errorf("e2 Ready=%s, want False; conditions=%+v", got, e2.Status.Conditions)
	}

	// Each owner should have subscribed to its own GVK only.
	owner1 := watcher.OwnerKey{Kind: kindEchelon, Namespace: nsFluxSystem, Name: "e1"}
	owner2 := watcher.OwnerKey{Kind: kindEchelon, Namespace: nsFluxSystem, Name: "e2"}
	if got := freg.GVKsByOwner(owner1); len(got) != 1 || got[0] != kustomizationGVK {
		t.Errorf("e1 subscriptions = %v, want [Kustomization]", got)
	}
	if got := freg.GVKsByOwner(owner2); len(got) != 1 || got[0] != helmGVK {
		t.Errorf("e2 subscriptions = %v, want [HelmRelease]", got)
	}
	if fa1.patches != 1 || fa2.patches != 1 {
		t.Errorf("patches: fa1=%d fa2=%d, want 1 each", fa1.patches, fa2.patches)
	}
}

// helpers

func readyStatusOf(ech *apiv1.Echelon) metav1.ConditionStatus {
	for _, c := range ech.Status.Conditions {
		if c.Type == apiv1.ConditionReady {
			return c.Status
		}
	}
	return ""
}

func hasCondition(ech *apiv1.Echelon, t string, st metav1.ConditionStatus) bool {
	for _, c := range ech.Status.Conditions {
		if c.Type == t && c.Status == st {
			return true
		}
	}
	return false
}

func stalledMessageOf(ech *apiv1.Echelon) string {
	for _, c := range ech.Status.Conditions {
		if c.Type == apiv1.ConditionStalled {
			return c.Message
		}
	}
	return ""
}

func intToStr(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var buf [16]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = digits[i%10]
		i /= 10
	}
	return string(buf[pos:])
}

func reconcileRequest(ns, name string) reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}
}
