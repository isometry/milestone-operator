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
	return watcher.OwnerKey{Kind: "Echelon", Namespace: a.obj.GetNamespace(), Name: a.obj.GetName()}
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
		ObjectMeta: metav1.ObjectMeta{Namespace: "flux-system", Name: name, Generation: 1},
		Spec:       apiv1.EchelonSpec{Targets: []apiv1.TargetSpec{{Kind: "Kustomization", Group: "kustomize.toolkit.fluxcd.io"}}},
	}
}

func mustSelector(t *testing.T) labels.Selector {
	t.Helper()
	return labels.Everything()
}

func currentMember(name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("kustomize.toolkit.fluxcd.io/v1")
	u.SetKind("Kustomization")
	u.SetNamespace("flux-system")
	u.SetName(name)
	u.SetGeneration(1)
	_ = unstructured.SetNestedField(u.Object, int64(1), "status", "observedGeneration")
	_ = unstructured.SetNestedSlice(u.Object, []any{
		map[string]any{"type": "Ready", "status": "True", "reason": "ReconciliationSucceeded"},
	}, "status", "conditions")
	return u
}

var kustomizationGVK = schema.GroupVersionKind{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Kind: "Kustomization"}

func newFixture(t *testing.T, ech *apiv1.Echelon, fa *fakeAdapter, freg *fakeRegistry) *controller.Reconciler {
	t.Helper()
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(ech).WithStatusSubresource(ech).Build()
	return &controller.Reconciler{
		Client:     cl,
		Registry:   freg,
		Resolver:   nil, // unused: fakeAdapter pre-resolves targets
		NewAdapter: func(_ client.Object) controller.OwnerAdapter { return fa },
		Controller: "Echelon",
		Now:        func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	}
}

func TestReconcile_AddsFinalizerAndRequeues(t *testing.T) {
	ech := newEchelon("e1")
	fa := &fakeAdapter{obj: ech}
	r := newFixture(t, ech, fa, newFakeRegistry())

	res, err := r.ReconcileObject(t.Context(), ech)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.Requeue {
		t.Errorf("expected requeue after adding finalizer")
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
	freg.subscribed[watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: "e1"}] = map[schema.GroupVersionKind]watcher.Subscriber{
		kustomizationGVK: {Owner: watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: "e1"}},
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
	owner := watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: "e1"}
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
			Index: 0, Group: "missing.io", Version: "v1", Kind: "Late",
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
	var members []*unstructured.Unstructured
	for i := range 60 {
		u := &unstructured.Unstructured{}
		u.SetAPIVersion("kustomize.toolkit.fluxcd.io/v1")
		u.SetKind("Kustomization")
		u.SetNamespace("flux-system")
		u.SetName(intToStr(i))
		u.SetGeneration(2)
		_ = unstructured.SetNestedField(u.Object, int64(2), "status", "observedGeneration")
		_ = unstructured.SetNestedSlice(u.Object, []any{
			map[string]any{"type": "Ready", "status": "False", "reason": "Reconciling"},
		}, "status", "conditions")
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
		Controller: "Echelon",
	}
	rf := r.AsReconcileFunc(func() client.Object { return &apiv1.Echelon{} })
	res, err := rf(t.Context(), reconcileRequest("flux-system", "missing"))
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected zero result, got %+v", res)
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
