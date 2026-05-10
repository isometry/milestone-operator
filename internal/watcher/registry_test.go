/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package watcher_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/isometry/echelon-operator/internal/watcher"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fakeFactory is a test-only InformerFactory that records lifecycle calls and
// lets tests fire fake events.
type fakeFactory struct {
	mu       sync.Mutex
	started  []schema.GroupVersionKind
	stopped  map[schema.GroupVersionKind]int
	entries  map[schema.GroupVersionKind]*fakeEntry
	failNext error
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
	}
}

func (f *fakeFactory) Start(gvk schema.GroupVersionKind, _ apimeta.RESTScopeName, handler watcher.InformerEventHandler) (watcher.InformerEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return nil, err
	}
	f.started = append(f.started, gvk)
	e := &fakeEntry{parent: f, gvk: gvk, handler: handler, listFn: func() ([]*unstructured.Unstructured, error) {
		return nil, nil
	}}
	f.entries[gvk] = e
	return e, nil
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

func TestRegistry_Subscribe_FirstStartsInformer(t *testing.T) {
	ff := newFakeFactory()
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	owner := watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: "wave-0"}
	if err := r.Subscribe(kustomizationGVK, apimeta.RESTScopeNameNamespace, watcher.Subscriber{Owner: owner, Selector: labels.Everything()}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if len(ff.started) != 1 {
		t.Errorf("started informers = %d, want 1", len(ff.started))
	}
}

func TestRegistry_Subscribe_SecondReusesInformer(t *testing.T) {
	ff := newFakeFactory()
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	a := watcher.OwnerKey{Kind: "Echelon", Namespace: "ns", Name: "a"}
	b := watcher.OwnerKey{Kind: "Echelon", Namespace: "ns", Name: "b"}
	_ = r.Subscribe(kustomizationGVK, apimeta.RESTScopeNameNamespace, watcher.Subscriber{Owner: a, Selector: labels.Everything()})
	_ = r.Subscribe(kustomizationGVK, apimeta.RESTScopeNameNamespace, watcher.Subscriber{Owner: b, Selector: labels.Everything()})
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
	a := watcher.OwnerKey{Kind: "Echelon", Namespace: "ns", Name: "a"}
	b := watcher.OwnerKey{Kind: "Echelon", Namespace: "ns", Name: "b"}
	_ = r.Subscribe(kustomizationGVK, apimeta.RESTScopeNameNamespace, watcher.Subscriber{Owner: a, Selector: labels.Everything()})
	_ = r.Subscribe(kustomizationGVK, apimeta.RESTScopeNameNamespace, watcher.Subscriber{Owner: b, Selector: labels.Everything()})

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
	owner := watcher.OwnerKey{Kind: "Echelon", Namespace: "ns", Name: "multi"}
	helmGVK := schema.GroupVersionKind{Group: "helm.toolkit.fluxcd.io", Version: "v2", Kind: "HelmRelease"}
	_ = r.Subscribe(kustomizationGVK, apimeta.RESTScopeNameNamespace, watcher.Subscriber{Owner: owner, Selector: labels.Everything()})
	_ = r.Subscribe(helmGVK, apimeta.RESTScopeNameNamespace, watcher.Subscriber{Owner: owner, Selector: labels.Everything()})
	r.UnsubscribeAll(owner)
	if r.GVKCount() != 0 {
		t.Errorf("GVKCount after UnsubscribeAll = %d, want 0", r.GVKCount())
	}
}

func TestRegistry_Subscribe_FactoryError(t *testing.T) {
	ff := newFakeFactory()
	ff.failNext = errors.New("boom")
	r := watcher.NewRegistry(ff, func(watcher.OwnerKey) {})
	owner := watcher.OwnerKey{Kind: "Echelon", Namespace: "ns", Name: "a"}
	err := r.Subscribe(kustomizationGVK, apimeta.RESTScopeNameNamespace, watcher.Subscriber{Owner: owner, Selector: labels.Everything()})
	if err == nil {
		t.Fatalf("expected error from failed factory")
	}
	if r.SubscriberCount(kustomizationGVK) != 0 {
		t.Errorf("subscriber added despite factory failure")
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
	owner := watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: "wave-0"}
	_ = r.Subscribe(kustomizationGVK, apimeta.RESTScopeNameNamespace, watcher.Subscriber{
		Owner:    owner,
		Selector: mustSelector(t, map[string]string{"wave": "0"}),
	})

	entry := ff.entries[kustomizationGVK]
	if entry == nil {
		t.Fatalf("no entry created")
	}
	matching := makeObj("flux-system", "k1", map[string]string{"wave": "0"})
	other := makeObj("flux-system", "k2", map[string]string{"wave": "1"})

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
	owner := watcher.OwnerKey{Kind: "Echelon", Namespace: "ns", Name: "a"}
	_ = r.Subscribe(kustomizationGVK, apimeta.RESTScopeNameNamespace, watcher.Subscriber{Owner: owner, Selector: labels.Everything()})

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
