/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package watcher

import (
	"fmt"
	"slices"
	"sync"

	"github.com/isometry/echelon-operator/internal/metrics"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EventType identifies the kind of informer event being dispatched.
type EventType int

const (
	EventAdd EventType = iota
	EventUpdate
	EventDelete
)

func (e EventType) String() string {
	switch e {
	case EventAdd:
		return metrics.EventAdd
	case EventUpdate:
		return metrics.EventUpdate
	case EventDelete:
		return metrics.EventDelete
	default:
		return "unknown"
	}
}

// InformerEventHandler is invoked by the underlying informer for every
// observed Add/Update/Delete event.
type InformerEventHandler func(ev EventType, obj client.Object)

// InformerEntry is the lifecycle handle returned by InformerFactory.Start.
type InformerEntry interface {
	Stop()
	List() ([]*unstructured.Unstructured, error)
}

// InformerFactory creates per-GVK informers. Abstracted so tests can fake the
// underlying dynamic informer machinery.
type InformerFactory interface {
	Start(gvk schema.GroupVersionKind, scope apimeta.RESTScopeName, handler InformerEventHandler) (InformerEntry, error)
}

// EnqueueFunc enqueues a single owner for reconciliation.
type EnqueueFunc func(owner OwnerKey)

// startInFlight tracks an informer Start call in progress. The owning
// goroutine fills err (if any) and then closes done; waiters block on done and
// then read err under the registry lock.
type startInFlight struct {
	done chan struct{}
	err  error
}

// Registry coordinates the per-GVK informers and the SubscriberIndex,
// implementing the refcounted "shared informer per GVK" pattern.
type Registry struct {
	factory InformerFactory
	enqueue EnqueueFunc
	index   *SubscriberIndex

	mu        sync.Mutex
	informers map[schema.GroupVersionKind]InformerEntry
	refcount  map[schema.GroupVersionKind]int
	// starting tracks GVKs whose informer Start is in flight. New Subscribes
	// for the same GVK wait on the in-flight result instead of either
	// serialising on r.mu or starting a duplicate informer.
	starting map[schema.GroupVersionKind]*startInFlight
}

// NewRegistry returns a Registry wired to factory and enqueue.
func NewRegistry(factory InformerFactory, enqueue EnqueueFunc) *Registry {
	return &Registry{
		factory:   factory,
		enqueue:   enqueue,
		index:     NewSubscriberIndex(),
		informers: make(map[schema.GroupVersionKind]InformerEntry),
		refcount:  make(map[schema.GroupVersionKind]int),
		starting:  make(map[schema.GroupVersionKind]*startInFlight),
	}
}

// Subscribe registers sub for gvk, starting the per-GVK informer if this is
// the first subscriber. Idempotent: re-subscribing the same Owner replaces the
// previous Subscriber (selector update) without changing refcounts.
//
// The slow factory.Start call runs without holding r.mu — concurrent Subscribes
// for unrelated GVKs proceed in parallel, and concurrent Subscribes for the
// same GVK wait on a single shared start (one informer per GVK invariant
// preserved).
func (r *Registry) Subscribe(gvk schema.GroupVersionKind, scope apimeta.RESTScopeName, sub Subscriber) error {
	for {
		r.mu.Lock()
		if _, running := r.informers[gvk]; running {
			r.registerLocked(gvk, sub)
			r.mu.Unlock()
			return nil
		}
		if inflight, ok := r.starting[gvk]; ok {
			r.mu.Unlock()
			<-inflight.done
			if inflight.err != nil {
				metrics.SubscribeTotal.WithLabelValues(gvk.Group, gvk.Version, gvk.Kind, metrics.SubscribeError).Inc()
				return fmt.Errorf("watcher: start informer for %s: %w", gvk, inflight.err)
			}
			// Start succeeded: loop back and take the running path to
			// register this subscriber.
			continue
		}
		// No informer, no in-flight start: this Subscribe owns the start.
		inflight := &startInFlight{done: make(chan struct{})}
		r.starting[gvk] = inflight
		r.mu.Unlock()

		entry, err := r.factory.Start(gvk, scope, r.dispatch(gvk))

		r.mu.Lock()
		delete(r.starting, gvk)
		if err != nil {
			inflight.err = err
			close(inflight.done)
			metrics.SubscribeTotal.WithLabelValues(gvk.Group, gvk.Version, gvk.Kind, metrics.SubscribeError).Inc()
			r.mu.Unlock()
			return fmt.Errorf("watcher: start informer for %s: %w", gvk, err)
		}
		r.informers[gvk] = entry
		metrics.Informers.WithLabelValues(gvk.Group, gvk.Version, gvk.Kind).Set(1)
		r.registerLocked(gvk, sub)
		close(inflight.done)
		r.mu.Unlock()
		return nil
	}
}

// registerLocked records sub in the index and updates refcount/metrics. The
// caller must hold r.mu and have ensured the informer for gvk is running.
func (r *Registry) registerLocked(gvk schema.GroupVersionKind, sub Subscriber) {
	if !r.hasSubscriberLocked(gvk, sub.Owner) {
		r.refcount[gvk]++
		metrics.Subscribers.WithLabelValues(gvk.Group, gvk.Version, gvk.Kind).Set(float64(r.refcount[gvk]))
	}
	r.index.Add(gvk, sub)
	metrics.SubscribeTotal.WithLabelValues(gvk.Group, gvk.Version, gvk.Kind, metrics.SubscribeOK).Inc()
}

// Unsubscribe drops owner's subscription for gvk. Stops the informer when the
// last subscriber releases.
func (r *Registry) Unsubscribe(gvk schema.GroupVersionKind, owner OwnerKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unsubscribeLocked(gvk, owner)
}

// UnsubscribeAll releases every subscription owned by owner.
func (r *Registry) UnsubscribeAll(owner OwnerKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, gvk := range r.index.GVKsByOwner(owner) {
		r.unsubscribeLocked(gvk, owner)
	}
}

// List proxies to the per-GVK informer cache.
func (r *Registry) List(gvk schema.GroupVersionKind) ([]*unstructured.Unstructured, error) {
	r.mu.Lock()
	entry, ok := r.informers[gvk]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("watcher: no informer for %s", gvk)
	}
	return entry.List()
}

// SubscriberCount returns the number of distinct owners subscribed to gvk.
func (r *Registry) SubscriberCount(gvk schema.GroupVersionKind) int {
	return r.index.SubscriberCount(gvk)
}

// GVKCount returns the number of distinct GVKs with active informers.
func (r *Registry) GVKCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.informers)
}

// GVKsByOwner reports which GVKs owner currently subscribes to. Used by the
// reconciler to compute subscription diffs.
func (r *Registry) GVKsByOwner(owner OwnerKey) []schema.GroupVersionKind {
	return r.index.GVKsByOwner(owner)
}

func (r *Registry) hasSubscriberLocked(gvk schema.GroupVersionKind, owner OwnerKey) bool {
	return slices.Contains(r.index.GVKsByOwner(owner), gvk)
}

func (r *Registry) unsubscribeLocked(gvk schema.GroupVersionKind, owner OwnerKey) {
	if !r.hasSubscriberLocked(gvk, owner) {
		return
	}
	r.index.Remove(gvk, owner)
	r.refcount[gvk]--
	metrics.UnsubscribeTotal.WithLabelValues(gvk.Group, gvk.Version, gvk.Kind).Inc()
	if r.refcount[gvk] <= 0 {
		if entry, ok := r.informers[gvk]; ok {
			entry.Stop()
			delete(r.informers, gvk)
		}
		delete(r.refcount, gvk)
		metrics.Informers.DeleteLabelValues(gvk.Group, gvk.Version, gvk.Kind)
		metrics.Subscribers.DeleteLabelValues(gvk.Group, gvk.Version, gvk.Kind)
		return
	}
	metrics.Subscribers.WithLabelValues(gvk.Group, gvk.Version, gvk.Kind).Set(float64(r.refcount[gvk]))
}

func (r *Registry) dispatch(gvk schema.GroupVersionKind) InformerEventHandler {
	return func(ev EventType, obj client.Object) {
		defer metrics.ObserveDispatch(gvk.Group, gvk.Version, gvk.Kind)()
		metrics.InformerEvents.WithLabelValues(gvk.Group, gvk.Version, gvk.Kind, ev.String()).Inc()
		for _, owner := range r.index.Subscribers(gvk, obj) {
			r.enqueue(owner)
		}
	}
}
