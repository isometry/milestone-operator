/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package watcher_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/isometry/milestone-operator/internal/watcher"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fakeFactory is a test-only InformerFactory that records lifecycle calls and
// lets tests fire fake events.
type fakeFactory struct {
	mu      sync.Mutex
	started []schema.GroupVersionKind
	// entered counts Start invocations per GVK, successful or not. Lets tests
	// assert that a cooldown-guarded Subscribe never reached the factory.
	entered map[schema.GroupVersionKind]int
	stopped map[schema.GroupVersionKind]int
	entries map[schema.GroupVersionKind]*fakeEntry
	failOn  map[schema.GroupVersionKind]error
	// panicOn, if set for a GVK, panics with the given message on Start.
	// Used to exercise the deferred cleanup path in Registry.Subscribe.
	panicOn map[schema.GroupVersionKind]string
	// startBlock, if set for a GVK, blocks Start until the channel is closed.
	// Tests use this to exercise Subscribe concurrency without holding the
	// registry lock across the slow Start call.
	startBlock map[schema.GroupVersionKind]chan struct{}
	// startedSignal, if set for a GVK, is closed exactly once when Start is
	// entered for that GVK. Lets tests synchronise on "A is inside Start"
	// without sleep-based barriers.
	startedSignal map[schema.GroupVersionKind]chan struct{}
}

type fakeEntry struct {
	parent  *fakeFactory
	gvk     schema.GroupVersionKind
	stops   int
	handler watcher.InformerEventHandler
	listFn  func() ([]*unstructured.Unstructured, error)
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{
		entered: make(map[schema.GroupVersionKind]int),
		stopped: make(map[schema.GroupVersionKind]int),
		entries: make(map[schema.GroupVersionKind]*fakeEntry),
		failOn:  make(map[schema.GroupVersionKind]error),
		panicOn: make(map[schema.GroupVersionKind]string),
	}
}

func (f *fakeFactory) Start(ctx context.Context, gvk schema.GroupVersionKind, _ apimeta.RESTScopeName, handler watcher.InformerEventHandler) (watcher.InformerEntry, error) {
	f.mu.Lock()
	f.entered[gvk]++
	block := f.startBlock[gvk]
	panicMsg, shouldPanic := f.panicOn[gvk]
	if shouldPanic {
		delete(f.panicOn, gvk) // single-shot
	}
	if ch, ok := f.startedSignal[gvk]; ok {
		delete(f.startedSignal, gvk) // signal at most once per GVK
		close(ch)
	}
	f.mu.Unlock()
	if shouldPanic {
		panic(panicMsg)
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failOn[gvk]; err != nil {
		delete(f.failOn, gvk) // single-shot, like the previous failNext semantics
		return nil, err
	}
	f.started = append(f.started, gvk)
	e := &fakeEntry{parent: f, gvk: gvk, handler: handler, listFn: func() ([]*unstructured.Unstructured, error) {
		return nil, nil
	}}
	f.entries[gvk] = e
	return e, nil
}

// startCount returns the number of completed Start calls.
func (f *fakeFactory) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.started)
}

// enteredCount returns the number of Start invocations for gvk, successful
// or not.
func (f *fakeFactory) enteredCount(gvk schema.GroupVersionKind) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.entered[gvk]
}

func (e *fakeEntry) Stop() {
	e.parent.mu.Lock()
	defer e.parent.mu.Unlock()
	e.stops++
	e.parent.stopped[e.gvk]++
}

func (e *fakeEntry) List() ([]*unstructured.Unstructured, error) {
	return e.listFn()
}

// sub builds a one-matcher Subscriber for the common test case.
func sub(owner watcher.OwnerKey, sel labels.Selector) watcher.Subscriber {
	return watcher.Subscriber{
		Owner:    owner,
		Matchers: []watcher.Matcher{{Selector: sel}},
	}
}

func TestRegistry_Subscribe_FirstStartsInformer(t *testing.T) {
	ff := newFakeFactory()
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: nsFluxSystem, Name: nameWave0}
	if err := r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything())); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if len(ff.started) != 1 {
		t.Errorf("started informers = %d, want 1", len(ff.started))
	}
}

func TestRegistry_Subscribe_SecondReusesInformer(t *testing.T) {
	ff := newFakeFactory()
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	a := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "a"}
	b := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "b"}
	_ = r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(a, labels.Everything()))
	_ = r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(b, labels.Everything()))
	if len(ff.started) != 1 {
		t.Errorf("started informers = %d, want 1 (shared)", len(ff.started))
	}
	if c := r.SubscriberCount(kustomizationGVK); c != 2 {
		t.Errorf("SubscriberCount = %d, want 2", c)
	}
}

func TestRegistry_UnsubscribeOfLastStopsInformer(t *testing.T) {
	ff := newFakeFactory()
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	a := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "a"}
	b := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "b"}
	_ = r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(a, labels.Everything()))
	_ = r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(b, labels.Everything()))

	r.Unsubscribe(kustomizationGVK, a)
	if ff.stopped[kustomizationGVK] != 0 {
		t.Errorf("stopped after first unsubscribe = %d, want 0", ff.stopped[kustomizationGVK])
	}

	r.Unsubscribe(kustomizationGVK, b)
	if ff.stopped[kustomizationGVK] != 1 {
		t.Errorf("stopped after last unsubscribe = %d, want 1", ff.stopped[kustomizationGVK])
	}
	if r.GVKCount() != 0 {
		t.Errorf("GVKCount after teardown = %d, want 0", r.GVKCount())
	}
}

func TestRegistry_UnsubscribeAll(t *testing.T) {
	ff := newFakeFactory()
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "multi"}
	helmGVK := schema.GroupVersionKind{Group: groupHelmToolkit, Version: "v2", Kind: kindHelmRelease}
	_ = r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything()))
	_ = r.Subscribe(context.Background(), helmGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything()))
	r.UnsubscribeAll(owner)
	if r.GVKCount() != 0 {
		t.Errorf("GVKCount after UnsubscribeAll = %d, want 0", r.GVKCount())
	}
}

func TestRegistry_Subscribe_FactoryError(t *testing.T) {
	ff := newFakeFactory()
	ff.failOn[kustomizationGVK] = errors.New("boom")
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "a"}
	err := r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything()))
	if err == nil {
		t.Fatalf("expected error from failed factory")
	}
	if r.SubscriberCount(kustomizationGVK) != 0 {
		t.Errorf("subscriber added despite factory failure")
	}
	if _, err := r.List(kustomizationGVK); err == nil {
		t.Errorf("expected List error post-failure, registry should not have an informer")
	}
}

// TestRegistry_Subscribe_PartialFailurePreservesPriorMatchers covers the
// transactional invariant: when the first Subscribe succeeds and a later
// Subscribe for the same owner/GVK fails (e.g. apiserver outage during a
// retry), the original matcher set must survive — the new attempt happens
// after factory.Start returns an error and before SubscriberIndex.Add runs.
func TestRegistry_Subscribe_PartialFailurePreservesPriorMatchers(t *testing.T) {
	ff := newFakeFactory()
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "a"}

	if err := r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace,
		sub(owner, mustSelector(t, map[string]string{labelWave: "0"}))); err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}

	// A second Subscribe for an *unrelated* GVK that fails must not corrupt
	// the matcher we already recorded for kustomizationGVK.
	ff.failOn[schema.GroupVersionKind{Group: groupHelmToolkit, Version: "v2", Kind: kindHelmRelease}] = errors.New("apiserver down")
	helmGVK := schema.GroupVersionKind{Group: groupHelmToolkit, Version: "v2", Kind: kindHelmRelease}
	if err := r.Subscribe(context.Background(), helmGVK, apimeta.RESTScopeNameNamespace,
		sub(owner, labels.Everything())); err == nil {
		t.Fatalf("expected second Subscribe to fail")
	}

	// Original matcher still admits the wave-0 object.
	if c := r.SubscriberCount(kustomizationGVK); c != 1 {
		t.Fatalf("kustomization SubscriberCount after failed unrelated Subscribe = %d, want 1", c)
	}
}

func TestRegistry_HandlerEnqueuesMatchedSubscribers(t *testing.T) {
	ff := newFakeFactory()
	var enqueued []watcher.OwnerKey
	var mu sync.Mutex
	r := watcher.NewRegistry(ff, func(o watcher.OwnerKey) {
		mu.Lock()
		defer mu.Unlock()
		enqueued = append(enqueued, o)
	})
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: nsFluxSystem, Name: nameWave0}
	_ = r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace,
		sub(owner, mustSelector(t, map[string]string{labelWave: "0"})))

	entry, ok := ff.entries[kustomizationGVK]
	if !ok || entry == nil {
		t.Fatalf("no entry created")
		return // unreachable: t.Fatalf calls Goexit; helps staticcheck reason about nilness below.
	}
	matching := makeObj(nsFluxSystem, "k1", map[string]string{labelWave: "0"})
	other := makeObj(nsFluxSystem, "k2", map[string]string{labelWave: "1"})

	entry.handler(watcher.EventAdd, matching)
	entry.handler(watcher.EventUpdate, other) // does not match
	entry.handler(watcher.EventDelete, matching)

	mu.Lock()
	defer mu.Unlock()
	if len(enqueued) != 2 {
		t.Errorf("enqueued = %v, want 2 (Add + Delete of matching)", enqueued)
	}
	for _, e := range enqueued {
		if e != owner {
			t.Errorf("unexpected enqueue %v", e)
		}
	}
}

func TestRegistry_List_DelegatesToInformer(t *testing.T) {
	ff := newFakeFactory()
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "a"}
	_ = r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything()))

	want := []*unstructured.Unstructured{makeObj("ns", "x", nil)}
	ff.entries[kustomizationGVK].listFn = func() ([]*unstructured.Unstructured, error) { return want, nil }

	got, err := r.List(kustomizationGVK)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].GetName() != "x" {
		t.Errorf("List = %v, want one obj named x", got)
	}
}

func TestRegistry_List_UnknownGVK(t *testing.T) {
	r := watcher.NewRegistry(newFakeFactory(), func(watcher.OwnerKey) {})
	if _, err := r.List(kustomizationGVK); err == nil {
		t.Errorf("expected error listing unknown GVK")
	}
}

// TestRegistry_Subscribe_SyncTimeout simulates the cache-sync timeout path:
// factory.Start blocks until ctx (with sync timeout) is cancelled, then
// returns the cancellation error. Registry must not register a subscriber
// and the next Subscribe past the failure cooldown must retry cleanly.
func TestRegistry_Subscribe_SyncTimeout(t *testing.T) {
	ff := newFakeFactory()
	never := make(chan struct{}) // never closed → Start blocks until ctx times out
	ff.startBlock = map[schema.GroupVersionKind]chan struct{}{kustomizationGVK: never}
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	r.SyncTimeout = 50 * time.Millisecond
	current := time.Unix(1000, 0)
	r.Now = func() time.Time { return current }

	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "a"}
	err := r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything()))
	if err == nil {
		t.Fatalf("expected sync timeout error")
	}
	if r.SubscriberCount(kustomizationGVK) != 0 {
		t.Errorf("subscriber registered after sync timeout")
	}
	if _, err := r.List(kustomizationGVK); err == nil {
		t.Errorf("expected List error post-timeout")
	}

	// Retry path: clear the block and step past the failure cooldown; the
	// next Subscribe should succeed.
	close(never)
	ff.startBlock = nil
	current = current.Add(r.SyncTimeout + time.Second)
	if err := r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything())); err != nil {
		t.Errorf("retry Subscribe after timeout: %v", err)
	}
}

// TestRegistry_Subscribe_FailureCooldown covers the negative cache on failed
// informer starts: a Subscribe for a GVK whose informer can never sync (RBAC
// forbidden, CRD deleted behind a discovery-cache hit) must not re-block the
// reconcile worker for the full sync timeout on every attempt — within the
// cooldown it fails fast without reaching the factory.
func TestRegistry_Subscribe_FailureCooldown(t *testing.T) {
	ff := newFakeFactory()
	ff.failOn[kustomizationGVK] = errors.New("forbidden: RBAC")
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	r.SyncTimeout = 50 * time.Millisecond

	current := time.Unix(1000, 0)
	r.Now = func() time.Time { return current }

	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "a"}
	subscribe := func() error {
		return r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything()))
	}

	// First attempt reaches the factory and fails.
	if err := subscribe(); err == nil {
		t.Fatalf("expected first Subscribe to fail")
	}
	if got := ff.enteredCount(kustomizationGVK); got != 1 {
		t.Fatalf("factory entered %d times, want 1", got)
	}

	// Second attempt inside the cooldown fails fast without touching the
	// factory (which would otherwise block the worker for the sync timeout).
	if err := subscribe(); err == nil {
		t.Fatalf("expected cooldown Subscribe to fail")
	}
	if got := ff.enteredCount(kustomizationGVK); got != 1 {
		t.Errorf("factory entered %d times, want 1 (cooldown must not retry)", got)
	}

	// Past the cooldown the factory is retried for real; ff.failOn is
	// single-shot so this attempt succeeds and clears the negative entry.
	current = current.Add(r.SyncTimeout + time.Second)
	if err := subscribe(); err != nil {
		t.Fatalf("post-cooldown Subscribe: %v", err)
	}
	if got := ff.enteredCount(kustomizationGVK); got != 2 {
		t.Errorf("factory entered %d times, want 2", got)
	}
	if c := r.SubscriberCount(kustomizationGVK); c != 1 {
		t.Errorf("SubscriberCount = %d, want 1", c)
	}
}

// TestRegistry_InvalidateGroupKind_ClearsFailureCooldown: CRD establishment
// invalidates (group, kind), which must also drop the negative entry so the
// very next Subscribe retries immediately instead of waiting out the cooldown.
func TestRegistry_InvalidateGroupKind_ClearsFailureCooldown(t *testing.T) {
	ff := newFakeFactory()
	ff.failOn[kustomizationGVK] = errors.New("informer sync: CRD absent")
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	r.SyncTimeout = 50 * time.Millisecond
	current := time.Unix(1000, 0)
	r.Now = func() time.Time { return current }

	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "a"}
	subscribe := func() error {
		return r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything()))
	}

	if err := subscribe(); err == nil {
		t.Fatalf("expected first Subscribe to fail")
	}

	// CRD becomes Established → the watcher invalidates the group/kind.
	r.InvalidateGroupKind(kustomizationGVK.Group, kustomizationGVK.Kind)

	// Clock unchanged: without the invalidate this would be inside the
	// cooldown; the invalidate must clear it so the retry is immediate.
	if err := subscribe(); err != nil {
		t.Fatalf("Subscribe after invalidate: %v", err)
	}
	if got := ff.enteredCount(kustomizationGVK); got != 2 {
		t.Errorf("factory entered %d times, want 2 (invalidate clears cooldown)", got)
	}
}

// TestRegistry_Subscribe_ConcurrentDistinctGVKs_NotSerialised guards against
// the registry holding its mutex across factory.Start: a slow Start for one
// GVK must not block a Subscribe for an unrelated GVK.
func TestRegistry_Subscribe_ConcurrentDistinctGVKs_NotSerialised(t *testing.T) {
	ff := newFakeFactory()
	blockA := make(chan struct{})
	enteredA := make(chan struct{})
	ff.startBlock = map[schema.GroupVersionKind]chan struct{}{kustomizationGVK: blockA}
	ff.startedSignal = map[schema.GroupVersionKind]chan struct{}{kustomizationGVK: enteredA}
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})

	aDone := make(chan error, 1)
	go func() {
		aDone <- r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace,
			sub(watcher.OwnerKey{Kind: kindMilestone, Namespace: "n", Name: "a"}, labels.Everything()))
	}()

	// Wait until A is provably inside factory.Start before issuing B; with
	// the old "lock held across Start" code, A holds r.mu here.
	select {
	case <-enteredA:
	case <-time.After(2 * time.Second):
		t.Fatalf("Subscribe(A) never reached factory.Start")
	}

	helmGVK := schema.GroupVersionKind{Group: groupHelmToolkit, Version: "v2", Kind: kindHelmRelease}
	bDone := make(chan error, 1)
	go func() {
		bDone <- r.Subscribe(context.Background(), helmGVK, apimeta.RESTScopeNameNamespace,
			sub(watcher.OwnerKey{Kind: kindMilestone, Namespace: "n", Name: "b"}, labels.Everything()))
	}()

	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("Subscribe(B): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Subscribe(B) blocked behind Subscribe(A)'s in-flight start")
	}

	close(blockA)
	select {
	case err := <-aDone:
		if err != nil {
			t.Fatalf("Subscribe(A): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Subscribe(A) never completed after unblocking")
	}
}

// TestRegistry_Subscribe_ConcurrentSameGVK_StartsOnce guards the
// compare-and-set: multiple concurrent Subscribes for the same GVK must
// trigger exactly one factory.Start, with all subscribers registered.
func TestRegistry_Subscribe_ConcurrentSameGVK_StartsOnce(t *testing.T) {
	ff := newFakeFactory()
	block := make(chan struct{})
	entered := make(chan struct{})
	ff.startBlock = map[schema.GroupVersionKind]chan struct{}{kustomizationGVK: block}
	ff.startedSignal = map[schema.GroupVersionKind]chan struct{}{kustomizationGVK: entered}
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})

	const N = 5
	var wg sync.WaitGroup
	errsCh := make(chan error, N)
	// Launch the first Subscribe and wait until it has entered factory.Start
	// so we know it owns the in-flight start before the remaining N-1 race in.
	wg.Go(func() {
		errsCh <- r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace,
			sub(watcher.OwnerKey{Kind: kindMilestone, Namespace: "n", Name: "o0"}, labels.Everything()))
	})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("first Subscribe never reached factory.Start")
	}
	for i := 1; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errsCh <- r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace,
				sub(watcher.OwnerKey{Kind: kindMilestone, Namespace: "n", Name: fmt.Sprintf("o%d", i)}, labels.Everything()))
		}(i)
	}

	// Give followers a moment to enqueue on the in-flight start, then release.
	time.Sleep(50 * time.Millisecond)
	close(block)
	wg.Wait()
	close(errsCh)
	for err := range errsCh {
		if err != nil {
			t.Errorf("Subscribe: %v", err)
		}
	}

	if c := ff.startCount(); c != 1 {
		t.Errorf("Start called %d times, want 1", c)
	}
	if c := r.SubscriberCount(kustomizationGVK); c != N {
		t.Errorf("SubscriberCount = %d, want %d", c, N)
	}
}

// TestRegistry_Subscribe_ConcurrentSameGVK_StartFailurePropagates: when the
// shared start fails, every concurrent waiter must observe the error rather
// than silently see "no informer" later.
func TestRegistry_Subscribe_ConcurrentSameGVK_StartFailurePropagates(t *testing.T) {
	ff := newFakeFactory()
	block := make(chan struct{})
	entered := make(chan struct{})
	ff.startBlock = map[schema.GroupVersionKind]chan struct{}{kustomizationGVK: block}
	ff.startedSignal = map[schema.GroupVersionKind]chan struct{}{kustomizationGVK: entered}
	ff.failOn[kustomizationGVK] = errors.New("apiserver down")
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})

	const N = 3
	var wg sync.WaitGroup
	errsCh := make(chan error, N)
	wg.Go(func() {
		errsCh <- r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace,
			sub(watcher.OwnerKey{Kind: kindMilestone, Namespace: "n", Name: "o0"}, labels.Everything()))
	})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("first Subscribe never reached factory.Start")
	}
	for i := 1; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errsCh <- r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace,
				sub(watcher.OwnerKey{Kind: kindMilestone, Namespace: "n", Name: fmt.Sprintf("o%d", i)}, labels.Everything()))
		}(i)
	}

	time.Sleep(50 * time.Millisecond)
	close(block)
	wg.Wait()
	close(errsCh)
	failures := 0
	for err := range errsCh {
		if err != nil {
			failures++
		}
	}
	if failures != N {
		t.Errorf("propagated failures = %d, want %d", failures, N)
	}
}

// TestRegistry_Subscribe_PanicInStart_DoesNotStrandFollowers pins the
// defer-cleanup contract on Subscribe's in-flight sentinel: if factory.Start
// panics before the success/failure paths run, the deferred cleanup must
// still remove the starting[gvk] entry and close inflight.done. Otherwise
// followers waiting on the inflight would block forever (closed-and-empty)
// or loop forever (informer never installed, but starting[gvk] orphaned).
func TestRegistry_Subscribe_PanicInStart_DoesNotStrandFollowers(t *testing.T) {
	ff := newFakeFactory()
	ff.panicOn[kustomizationGVK] = "boom from factory.Start"
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})

	// Leader Subscribe panics inside factory.Start. Recover so the test
	// process survives; we only care that registry state is consistent after.
	var leaderRecovered atomic.Value
	var wg sync.WaitGroup
	wg.Go(func() {
		defer func() {
			if v := recover(); v != nil {
				leaderRecovered.Store(v)
			}
		}()
		_ = r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace,
			sub(watcher.OwnerKey{Kind: kindMilestone, Namespace: "n", Name: "leader"}, labels.Everything()))
	})
	wg.Wait()

	if leaderRecovered.Load() == nil {
		t.Fatalf("leader did not panic; factory.panicOn configuration broken")
	}

	// Follower Subscribe arriving after the leader's panic must complete in
	// bounded time. With panicOn cleared (single-shot), the follower should
	// become the new owner of a fresh Start and succeed.
	done := make(chan error, 1)
	go func() {
		done <- r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace,
			sub(watcher.OwnerKey{Kind: kindMilestone, Namespace: "n", Name: "follower"}, labels.Everything()))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("follower Subscribe after leader panic = %v, want success", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("follower stranded: Subscribe did not return after leader panic — defer cleanup missing")
	}
}

// TestRegistry_InvalidateGroupKind_ConcurrentSubscribe_Consistent pins the
// linearizability of InvalidateGroupKind against a racing Subscribe: whatever
// the interleaving, the post-state must be one of the two serial outcomes —
// (informer running ∧ subscriber registered) or (no informer ∧ no
// subscriber). A ClearGVK that runs outside the registry lock lets a racing
// Subscribe register against the stale index (skipping the refcount
// increment) and then be wiped, orphaning a running informer whose events
// dispatch to nobody.
func TestRegistry_InvalidateGroupKind_ConcurrentSubscribe_Consistent(t *testing.T) {
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "a"}
	for i := range 5000 {
		ff := newFakeFactory()
		r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
		// Seed: informer running, index populated — the stale-index precondition.
		if err := r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything())); err != nil {
			t.Fatalf("seed Subscribe: %v", err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			r.InvalidateGroupKind(kustomizationGVK.Group, kustomizationGVK.Kind)
		}()
		var subErr error
		go func() {
			defer wg.Done()
			<-start
			subErr = r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything()))
		}()
		close(start)
		wg.Wait()
		if subErr != nil {
			t.Fatalf("iteration %d: racing Subscribe: %v", i, subErr)
		}

		_, listErr := r.List(kustomizationGVK)
		informerRunning := listErr == nil
		subs := r.SubscriberCount(kustomizationGVK)
		switch {
		case informerRunning && subs != 1:
			t.Fatalf("iteration %d: informer running with %d subscribers (orphaned informer, events dispatch to nobody)", i, subs)
		case !informerRunning && subs != 0:
			t.Fatalf("iteration %d: no informer but %d subscribers linger", i, subs)
		}
	}
}

// TestRegistry_UnsubscribeAll_StopsDispatchToFinalizedOwner covers the race
// where a member event arrives just as the owner is being torn down. After
// UnsubscribeAll returns, no further events for that owner's GVKs should
// produce an enqueue.
func TestRegistry_UnsubscribeAll_StopsDispatchToFinalizedOwner(t *testing.T) {
	ff := newFakeFactory()
	var enqueued atomic.Int64
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) { enqueued.Add(1) })
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: "ns", Name: "a"}
	_ = r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything()))
	entry := ff.entries[kustomizationGVK]
	if entry == nil {
		t.Fatalf("entry not created")
		return // unreachable: t.Fatalf calls Goexit; keeps staticcheck happy below.
	}

	// Fire one event before finalize.
	entry.handler(watcher.EventAdd, makeObj("ns", "k1", nil))
	if got := enqueued.Load(); got != 1 {
		t.Fatalf("pre-finalize enqueue = %d, want 1", got)
	}

	r.UnsubscribeAll(owner)
	// Subsequent events for that GVK must not reach the (now-unregistered)
	// owner. The informer itself is stopped because refcount went to zero,
	// but a racing dispatcher could still invoke handler with an in-flight
	// object — assert the dispatch path is filtering correctly.
	entry.handler(watcher.EventDelete, makeObj("ns", "k1", nil))
	if got := enqueued.Load(); got != 1 {
		t.Errorf("post-finalize enqueue = %d, want 1 (no further enqueues)", got)
	}
}
