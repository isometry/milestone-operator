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
	stopped map[schema.GroupVersionKind]int
	entries map[schema.GroupVersionKind]*fakeEntry
	failOn  map[schema.GroupVersionKind]error
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
		stopped: make(map[schema.GroupVersionKind]int),
		entries: make(map[schema.GroupVersionKind]*fakeEntry),
		failOn:  make(map[schema.GroupVersionKind]error),
	}
}

func (f *fakeFactory) Start(ctx context.Context, gvk schema.GroupVersionKind, _ apimeta.RESTScopeName, handler watcher.InformerEventHandler) (watcher.InformerEntry, error) {
	f.mu.Lock()
	block := f.startBlock[gvk]
	if ch, ok := f.startedSignal[gvk]; ok {
		delete(f.startedSignal, gvk) // signal at most once per GVK
		close(ch)
	}
	f.mu.Unlock()
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
// and the next Subscribe must be able to retry cleanly.
func TestRegistry_Subscribe_SyncTimeout(t *testing.T) {
	ff := newFakeFactory()
	never := make(chan struct{}) // never closed → Start blocks until ctx times out
	ff.startBlock = map[schema.GroupVersionKind]chan struct{}{kustomizationGVK: never}
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	r.SyncTimeout = 50 * time.Millisecond

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

	// Retry path: clear the block; the next Subscribe should succeed.
	close(never)
	ff.startBlock = nil
	if err := r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace, sub(owner, labels.Everything())); err != nil {
		t.Errorf("retry Subscribe after timeout: %v", err)
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		errsCh <- r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace,
			sub(watcher.OwnerKey{Kind: kindMilestone, Namespace: "n", Name: "o0"}, labels.Everything()))
	}()
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		errsCh <- r.Subscribe(context.Background(), kustomizationGVK, apimeta.RESTScopeNameNamespace,
			sub(watcher.OwnerKey{Kind: kindMilestone, Namespace: "n", Name: "o0"}, labels.Everything()))
	}()
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
