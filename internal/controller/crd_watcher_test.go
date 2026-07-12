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

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/controller"
	ctrmetrics "github.com/isometry/milestone-operator/internal/metrics"
	"github.com/isometry/milestone-operator/internal/watcher"
	"github.com/prometheus/client_golang/prometheus/testutil"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type groupKindPair struct{ Group, Kind string }

// recordingResolver records InvalidateGroupKind calls.
type recordingResolver struct {
	mu      sync.Mutex
	invalid []groupKindPair
}

func (*recordingResolver) Resolve(context.Context, string, string, string) (schema.GroupVersionKind, apimeta.RESTScopeName, error) {
	return schema.GroupVersionKind{}, "", errors.New("recordingResolver.Resolve not used")
}
func (*recordingResolver) Invalidate() {}
func (r *recordingResolver) InvalidateGroupKind(group, kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.invalid = append(r.invalid, groupKindPair{group, kind})
}
func (r *recordingResolver) calls() []groupKindPair {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]groupKindPair, len(r.invalid))
	copy(out, r.invalid)
	return out
}

// recordingRegistry records InvalidateGroupKind calls.
type recordingRegistry struct {
	mu      sync.Mutex
	invalid []groupKindPair
}

func (r *recordingRegistry) InvalidateGroupKind(group, kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.invalid = append(r.invalid, groupKindPair{group, kind})
}
func (r *recordingRegistry) calls() []groupKindPair {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]groupKindPair, len(r.invalid))
	copy(out, r.invalid)
	return out
}

func drainQueue(q workqueue.TypedRateLimitingInterface[reconcile.Request]) []reconcile.Request {
	out := make([]reconcile.Request, 0, q.Len())
	for q.Len() > 0 {
		item, _ := q.Get()
		q.Done(item)
		out = append(out, item)
	}
	return out
}

func crdWatcherScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := apiv1.AddToScheme(s); err != nil {
		t.Fatalf("apiv1 scheme: %v", err)
	}
	if err := apiextv1.AddToScheme(s); err != nil {
		t.Fatalf("apiextv1 scheme: %v", err)
	}
	return s
}

func crdEstablishedObj(name, group, kind string) *apiextv1.CustomResourceDefinition {
	return &apiextv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: apiextv1.CustomResourceDefinitionSpec{
			Group: group,
			Names: apiextv1.CustomResourceDefinitionNames{Kind: kind},
		},
		Status: apiextv1.CustomResourceDefinitionStatus{Conditions: []apiextv1.CustomResourceDefinitionCondition{
			{Type: apiextv1.Established, Status: apiextv1.ConditionTrue},
		}},
	}
}

//nolint:unparam // signature mirrors crdEstablishedObj for fixture symmetry
func crdNotEstablishedObj(name, group, kind string) *apiextv1.CustomResourceDefinition {
	crd := crdEstablishedObj(name, group, kind)
	crd.Status.Conditions[0].Status = apiextv1.ConditionFalse
	return crd
}

// startSource captures a workqueue into an EnqueueSource. Cleanup runs at
// test end. Returns the queue so the test can inspect/drain it.
func startSource(t *testing.T, src *watcher.EnqueueSource) workqueue.TypedRateLimitingInterface[reconcile.Request] {
	t.Helper()
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	ctx, cancel := context.WithCancel(context.Background())
	if err := src.Start(ctx, q); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		q.ShutDown()
	})
	return q
}

func milestoneWithDep(name, group, kind string) *apiv1.Milestone {
	return &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFluxSystem, Name: name},
		Spec: apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{
			{Name: depKustomizations, Target: apiv1.TargetSpec{Group: group, Kind: kind}},
		}},
	}
}

func clusterMilestoneWithDep(name, group, kind string) *apiv1.ClusterMilestone {
	return &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: apiv1.ClusterMilestoneSpec{DependsOn: []apiv1.ClusterDependencyRef{
			{Name: depKustomizations, Target: apiv1.ClusterTargetSpec{TargetSpec: apiv1.TargetSpec{Group: group, Kind: kind}}},
		}},
	}
}

// setCRDEstablished flips the stored CRD's Established condition via the
// status subresource.
func setCRDEstablished(t *testing.T, c client.Client, name string, established bool) {
	t.Helper()
	stored := &apiextv1.CustomResourceDefinition{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, stored); err != nil {
		t.Fatalf("Get CRD: %v", err)
	}
	status := apiextv1.ConditionFalse
	if established {
		status = apiextv1.ConditionTrue
	}
	stored.Status.Conditions[0].Status = status
	if err := c.Status().Update(context.Background(), stored); err != nil {
		t.Fatalf("Status update: %v", err)
	}
}

// TestCRDWatcher_EstablishedTrue_WakesAndInvalidates pins the happy path: an
// observed not-established → Established transition invalidates the discovery
// cache and the registry for that (group, kind), and enqueues every
// matching Milestone / ClusterMilestone.
func TestCRDWatcher_EstablishedTrue_WakesAndInvalidates(t *testing.T) {
	s := crdWatcherScheme(t)
	matchM := milestoneWithDep(nameWave0, groupKustomize, kindKustomization)
	unrelatedM := milestoneWithDep(nameWave1, "other.group", "Other")
	matchCM := clusterMilestoneWithDep("platform-0", groupKustomize, kindKustomization)
	crd := crdNotEstablishedObj("kustomizations."+groupKustomize, groupKustomize, kindKustomization)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(matchM, unrelatedM, matchCM, crd).
		WithStatusSubresource(&apiextv1.CustomResourceDefinition{}).Build()

	res := &recordingResolver{}
	reg := &recordingRegistry{}
	mSrc := watcher.NewEnqueueSource()
	cSrc := watcher.NewEnqueueSource()
	mQ := startSource(t, mSrc)
	cQ := startSource(t, cSrc)

	w := &controller.CRDWatcher{Client: c, Resolver: res, Registry: reg, MilestoneEnqueue: mSrc, ClusterMilestoneEnqueue: cSrc}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: crd.Name}}
	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (seed): %v", err)
	}
	setCRDEstablished(t, c, crd.Name, true)
	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (establish): %v", err)
	}

	if got := res.calls(); len(got) != 1 || got[0] != (groupKindPair{groupKustomize, kindKustomization}) {
		t.Errorf("resolver invalidations = %v, want one [%s/%s]", got, groupKustomize, kindKustomization)
	}
	if got := reg.calls(); len(got) != 1 || got[0] != (groupKindPair{groupKustomize, kindKustomization}) {
		t.Errorf("registry invalidations = %v, want one [%s/%s]", got, groupKustomize, kindKustomization)
	}
	if mReqs := drainQueue(mQ); len(mReqs) != 1 || mReqs[0].Name != nameWave0 {
		t.Errorf("milestone wakes = %v, want exactly wave-0", mReqs)
	}
	if cReqs := drainQueue(cQ); len(cReqs) != 1 || cReqs[0].Name != "platform-0" {
		t.Errorf("clustermilestone wakes = %v, want exactly platform-0", cReqs)
	}
}

// TestCRDWatcher_StartupReplay_EstablishedCRD_SeedsSilently: the first
// observation of an already-Established CRD (informer replay at operator
// startup or leader change) must seed transition state without firing side
// effects — controller-runtime already reconciles every owner at startup, so
// invalidating and waking per pre-existing CRD is pure churn and inflates
// milestone_crd_established_events_total on every restart. The seeded state
// must still drive a later de-establish transition.
func TestCRDWatcher_StartupReplay_EstablishedCRD_SeedsSilently(t *testing.T) {
	s := crdWatcherScheme(t)
	m := milestoneWithDep(nameWave0, groupKustomize, kindKustomization)
	crd := crdEstablishedObj("kustomizations."+groupKustomize, groupKustomize, kindKustomization)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(m, crd).
		WithStatusSubresource(&apiextv1.CustomResourceDefinition{}).Build()

	res := &recordingResolver{}
	reg := &recordingRegistry{}
	mSrc, cSrc := watcher.NewEnqueueSource(), watcher.NewEnqueueSource()
	mQ := startSource(t, mSrc)
	cQ := startSource(t, cSrc)
	w := &controller.CRDWatcher{Client: c, Resolver: res, Registry: reg, MilestoneEnqueue: mSrc, ClusterMilestoneEnqueue: cSrc}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: crd.Name}}

	established := testutil.ToFloat64(ctrmetrics.CRDEstablishedEvents.WithLabelValues(groupKustomize, kindKustomization))
	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (startup replay): %v", err)
	}
	if got := len(res.calls()) + len(reg.calls()) + mQ.Len() + cQ.Len(); got != 0 {
		t.Errorf("startup replay produced side effects (counts sum = %d); want silent seed", got)
	}
	if got := testutil.ToFloat64(ctrmetrics.CRDEstablishedEvents.WithLabelValues(groupKustomize, kindKustomization)); got != established {
		t.Errorf("CRDEstablishedEvents delta = %v on startup replay, want 0", got-established)
	}

	// The silent seed must have recorded (group, kind): a subsequent
	// de-establish fires with it.
	setCRDEstablished(t, c, crd.Name, false)
	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (de-establish): %v", err)
	}
	if got := res.calls(); len(got) != 1 || got[0] != (groupKindPair{groupKustomize, kindKustomization}) {
		t.Errorf("post-de-establish resolver invalidations = %v, want one [%s/%s]", got, groupKustomize, kindKustomization)
	}
	if mReqs := drainQueue(mQ); len(mReqs) != 1 || mReqs[0].Name != nameWave0 {
		t.Errorf("de-establish milestone wakes = %v, want exactly wave-0", mReqs)
	}
}

// TestCRDWatcher_WakeError_RetryRefiresTransition pins the peek-then-commit
// contract: when the wake fails (transient list error) the Reconcile error
// must leave the transition uncommitted, so the controller-runtime retry
// re-fires the invalidate-and-wake instead of silently matching "no
// transition". Without this, a CRD deleted while an owner is Ready=True can
// leave that owner reporting Ready forever against a vanished API surface.
func TestCRDWatcher_WakeError_RetryRefiresTransition(t *testing.T) {
	directions := []struct {
		name       string
		transition func(t *testing.T, c client.Client, crdName string)
	}{
		{name: "establish", transition: func(t *testing.T, c client.Client, crdName string) {
			setCRDEstablished(t, c, crdName, true)
		}},
		{name: "delete", transition: func(t *testing.T, c client.Client, crdName string) {
			stored := &apiextv1.CustomResourceDefinition{}
			if err := c.Get(context.Background(), types.NamespacedName{Name: crdName}, stored); err != nil {
				t.Fatalf("Get CRD: %v", err)
			}
			if err := c.Delete(context.Background(), stored); err != nil {
				t.Fatalf("Delete CRD: %v", err)
			}
		}},
	}
	for _, dir := range directions {
		t.Run(dir.name, func(t *testing.T) {
			s := crdWatcherScheme(t)
			m := milestoneWithDep(nameWave0, groupKustomize, kindKustomization)
			// establish direction starts not-established; delete direction
			// starts established (seeded on first observation).
			var crd *apiextv1.CustomResourceDefinition
			if dir.name == "establish" {
				crd = crdNotEstablishedObj("kustomizations."+groupKustomize, groupKustomize, kindKustomization)
			} else {
				crd = crdEstablishedObj("kustomizations."+groupKustomize, groupKustomize, kindKustomization)
			}
			failOnce := true
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(m, crd).
				WithStatusSubresource(&apiextv1.CustomResourceDefinition{}).
				WithInterceptorFuncs(interceptor.Funcs{
					List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if _, ok := list.(*apiv1.MilestoneList); ok && failOnce {
							failOnce = false
							return errors.New("apiserver: transient list failure")
						}
						return cl.List(ctx, list, opts...)
					},
				}).Build()

			res := &recordingResolver{}
			reg := &recordingRegistry{}
			mSrc, cSrc := watcher.NewEnqueueSource(), watcher.NewEnqueueSource()
			mQ := startSource(t, mSrc)
			_ = startSource(t, cSrc)
			w := &controller.CRDWatcher{Client: c, Resolver: res, Registry: reg, MilestoneEnqueue: mSrc, ClusterMilestoneEnqueue: cSrc}
			req := ctrl.Request{NamespacedName: types.NamespacedName{Name: crd.Name}}

			// Seed the initial observation (list interceptor untouched: no wake).
			if _, err := w.Reconcile(context.Background(), req); err != nil {
				t.Fatalf("Reconcile (seed): %v", err)
			}
			dir.transition(t, c, crd.Name)

			// Transition reconcile: wake list fails once → error for retry.
			if _, err := w.Reconcile(context.Background(), req); err == nil {
				t.Fatalf("Reconcile (failed wake) returned nil; expected error for retry")
			}
			// Retry with no further CRD change must re-fire the wake.
			if _, err := w.Reconcile(context.Background(), req); err != nil {
				t.Fatalf("Reconcile (retry): %v", err)
			}
			if mReqs := drainQueue(mQ); len(mReqs) != 1 || mReqs[0].Name != nameWave0 {
				t.Errorf("wakes after retry = %v, want exactly wave-0 (transition must survive the failed attempt)", mReqs)
			}
		})
	}
}

// TestCRDWatcher_NotEstablished_NoOp covers the "CRD exists but never
// became Established" path: no invalidation, no wake.
func TestCRDWatcher_NotEstablished_NoOp(t *testing.T) {
	s := crdWatcherScheme(t)
	crd := crdNotEstablishedObj("kustomizations."+groupKustomize, groupKustomize, kindKustomization)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(crd).Build()

	res := &recordingResolver{}
	reg := &recordingRegistry{}
	mSrc, cSrc := watcher.NewEnqueueSource(), watcher.NewEnqueueSource()
	mQ := startSource(t, mSrc)
	cQ := startSource(t, cSrc)
	w := &controller.CRDWatcher{Client: c, Resolver: res, Registry: reg, MilestoneEnqueue: mSrc, ClusterMilestoneEnqueue: cSrc}

	if _, err := w.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: crd.Name}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := res.calls(); len(got) != 0 {
		t.Errorf("resolver invalidations = %v, want none", got)
	}
	if got := reg.calls(); len(got) != 0 {
		t.Errorf("registry invalidations = %v, want none", got)
	}
	if got := mQ.Len() + cQ.Len(); got != 0 {
		t.Errorf("wakes = %d, want 0", got)
	}
}

// TestCRDWatcher_RepeatEstablishedTrue_Idempotent ensures a periodic
// resync of an already-Established CRD does NOT re-invalidate the cache
// or re-fire side effects. Without the transition guard, every CRD
// resync at the controller-runtime default 10-minute interval would
// invalidate and wake every owner.
func TestCRDWatcher_RepeatEstablishedTrue_Idempotent(t *testing.T) {
	s := crdWatcherScheme(t)
	m := milestoneWithDep(nameWave0, groupKustomize, kindKustomization)
	crd := crdNotEstablishedObj("kustomizations."+groupKustomize, groupKustomize, kindKustomization)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(m, crd).
		WithStatusSubresource(&apiextv1.CustomResourceDefinition{}).Build()

	res := &recordingResolver{}
	reg := &recordingRegistry{}
	mSrc, cSrc := watcher.NewEnqueueSource(), watcher.NewEnqueueSource()
	mQ := startSource(t, mSrc)
	_ = startSource(t, cSrc)
	w := &controller.CRDWatcher{Client: c, Resolver: res, Registry: reg, MilestoneEnqueue: mSrc, ClusterMilestoneEnqueue: cSrc}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: crd.Name}}

	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (seed): %v", err)
	}
	setCRDEstablished(t, c, crd.Name, true)
	for i := range 3 {
		if _, err := w.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("Reconcile %d: %v", i, err)
		}
	}
	if got := len(res.calls()); got != 1 {
		t.Errorf("resolver invalidations after transition + 2 resyncs = %d, want 1", got)
	}
	if got := len(reg.calls()); got != 1 {
		t.Errorf("registry invalidations after transition + 2 resyncs = %d, want 1", got)
	}
	if mQ.Len() != 1 {
		t.Errorf("milestone queue length = %d, want 1 (single Enqueue, key-deduped)", mQ.Len())
	}
}

// TestCRDWatcher_CRDDeleted_PriorEstablished_InvalidatesAndWakes pins
// CR-2: a previously-Established CRD that is deleted must trigger
// invalidate + wake using the recorded (group, kind).
func TestCRDWatcher_CRDDeleted_PriorEstablished_InvalidatesAndWakes(t *testing.T) {
	s := crdWatcherScheme(t)
	m := milestoneWithDep(nameWave0, groupKustomize, kindKustomization)
	crd := crdEstablishedObj("kustomizations."+groupKustomize, groupKustomize, kindKustomization)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(m, crd).Build()

	res := &recordingResolver{}
	reg := &recordingRegistry{}
	mSrc, cSrc := watcher.NewEnqueueSource(), watcher.NewEnqueueSource()
	mQ := startSource(t, mSrc)
	_ = startSource(t, cSrc)
	w := &controller.CRDWatcher{Client: c, Resolver: res, Registry: reg, MilestoneEnqueue: mSrc, ClusterMilestoneEnqueue: cSrc}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: crd.Name}}

	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (establish): %v", err)
	}
	drainQueue(mQ)
	resBaseline := len(res.calls())
	regBaseline := len(reg.calls())

	if err := c.Delete(context.Background(), crd); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (delete): %v", err)
	}

	if got := res.calls(); len(got) != resBaseline+1 || got[len(got)-1] != (groupKindPair{groupKustomize, kindKustomization}) {
		t.Errorf("post-delete resolver invalidations = %v, want one new [%s/%s]", got, groupKustomize, kindKustomization)
	}
	if got := reg.calls(); len(got) != regBaseline+1 || got[len(got)-1] != (groupKindPair{groupKustomize, kindKustomization}) {
		t.Errorf("post-delete registry invalidations = %v, want one new [%s/%s]", got, groupKustomize, kindKustomization)
	}
	if mReqs := drainQueue(mQ); len(mReqs) != 1 || mReqs[0].Name != nameWave0 {
		t.Errorf("post-delete milestone wakes = %v, want exactly wave-0", mReqs)
	}
}

// TestCRDWatcher_CRDDeleted_NeverEstablished_NoOp ensures the watcher
// doesn't fire on the NotFound of a CRD it never saw Established.
func TestCRDWatcher_CRDDeleted_NeverEstablished_NoOp(t *testing.T) {
	s := crdWatcherScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()

	res := &recordingResolver{}
	reg := &recordingRegistry{}
	mSrc, cSrc := watcher.NewEnqueueSource(), watcher.NewEnqueueSource()
	mQ := startSource(t, mSrc)
	cQ := startSource(t, cSrc)
	w := &controller.CRDWatcher{Client: c, Resolver: res, Registry: reg, MilestoneEnqueue: mSrc, ClusterMilestoneEnqueue: cSrc}

	if _, err := w.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "kustomizations." + groupKustomize}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := len(res.calls()) + len(reg.calls()) + mQ.Len() + cQ.Len(); got != 0 {
		t.Errorf("never-seen NotFound reconcile produced side effects (counts sum = %d)", got)
	}
}

// TestCRDWatcher_TransitionEstablishedToFalse_InvalidatesAndWakes covers
// the "CRD lost Established without being deleted" branch.
func TestCRDWatcher_TransitionEstablishedToFalse_InvalidatesAndWakes(t *testing.T) {
	s := crdWatcherScheme(t)
	m := milestoneWithDep(nameWave0, groupKustomize, kindKustomization)
	crd := crdEstablishedObj("kustomizations."+groupKustomize, groupKustomize, kindKustomization)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(m, crd).WithStatusSubresource(&apiextv1.CustomResourceDefinition{}).Build()

	res := &recordingResolver{}
	reg := &recordingRegistry{}
	mSrc, cSrc := watcher.NewEnqueueSource(), watcher.NewEnqueueSource()
	mQ := startSource(t, mSrc)
	_ = startSource(t, cSrc)
	w := &controller.CRDWatcher{Client: c, Resolver: res, Registry: reg, MilestoneEnqueue: mSrc, ClusterMilestoneEnqueue: cSrc}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: crd.Name}}

	// First observation is a silent seed (startup replay semantics).
	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (seed): %v", err)
	}
	drainQueue(mQ)

	setCRDEstablished(t, c, crd.Name, false)
	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (de-establish): %v", err)
	}
	if got := len(res.calls()); got != 1 {
		t.Errorf("resolver invalidations after de-establish = %d, want 1", got)
	}
	if mReqs := drainQueue(mQ); len(mReqs) != 1 || mReqs[0].Name != nameWave0 {
		t.Errorf("de-establish milestone wakes = %v, want exactly wave-0", mReqs)
	}
}

// TestCRDWatcher_OwnsKind_DistinctGroups_NoCrossWake guards against
// (group, kind) collisions: a Kind name shared across two groups must
// not cross-wake owners that reference a different group.
func TestCRDWatcher_OwnsKind_DistinctGroups_NoCrossWake(t *testing.T) {
	s := crdWatcherScheme(t)
	matchM := milestoneWithDep(nameWave0, groupKustomize, kindKustomization)
	otherM := milestoneWithDep(nameWave1, "other.group", kindKustomization)
	crd := crdNotEstablishedObj("kustomizations."+groupKustomize, groupKustomize, kindKustomization)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(matchM, otherM, crd).
		WithStatusSubresource(&apiextv1.CustomResourceDefinition{}).Build()

	res := &recordingResolver{}
	reg := &recordingRegistry{}
	mSrc, cSrc := watcher.NewEnqueueSource(), watcher.NewEnqueueSource()
	mQ := startSource(t, mSrc)
	_ = startSource(t, cSrc)
	w := &controller.CRDWatcher{Client: c, Resolver: res, Registry: reg, MilestoneEnqueue: mSrc, ClusterMilestoneEnqueue: cSrc}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: crd.Name}}

	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (seed): %v", err)
	}
	setCRDEstablished(t, c, crd.Name, true)
	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (establish): %v", err)
	}
	mReqs := drainQueue(mQ)
	if len(mReqs) != 1 || mReqs[0].Name != nameWave0 {
		t.Errorf("wakes = %v, want only wave-0 (matching group)", mReqs)
	}
}

// TestCRDWatcher_WakeListError_ReturnedForRetry pins HI-5: a transient
// list failure at wake time must surface so controller-runtime retries.
func TestCRDWatcher_WakeListError_ReturnedForRetry(t *testing.T) {
	s := crdWatcherScheme(t)
	crd := crdNotEstablishedObj("kustomizations."+groupKustomize, groupKustomize, kindKustomization)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(crd).
		WithStatusSubresource(&apiextv1.CustomResourceDefinition{}).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*apiv1.MilestoneList); ok {
					return errors.New("apiserver: transient list failure")
				}
				return cl.List(ctx, list, opts...)
			},
		}).Build()

	res := &recordingResolver{}
	reg := &recordingRegistry{}
	mSrc, cSrc := watcher.NewEnqueueSource(), watcher.NewEnqueueSource()
	_ = startSource(t, mSrc)
	_ = startSource(t, cSrc)
	w := &controller.CRDWatcher{Client: c, Resolver: res, Registry: reg, MilestoneEnqueue: mSrc, ClusterMilestoneEnqueue: cSrc}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: crd.Name}}

	if _, err := w.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile (seed): %v", err)
	}
	setCRDEstablished(t, c, crd.Name, true)
	if _, err := w.Reconcile(context.Background(), req); err == nil {
		t.Fatalf("Reconcile returned nil; expected wake error to propagate so controller-runtime retries")
	}
}
