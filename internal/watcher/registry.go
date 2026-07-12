/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package watcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/isometry/milestone-operator/internal/metrics"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DefaultSyncTimeout caps how long Subscribe will wait for the per-GVK
// informer cache to populate before returning a WatchSetupFailed error. The
// reconciler runs subscriptions sequentially per GVK, so this is also the
// effective per-GVK budget within a single reconcile.
const DefaultSyncTimeout = 30 * time.Second

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

// InformerFactory creates per-GVK informers. Start must block until the
// informer's cache reports HasSynced or ctx is cancelled; on failure it must
// stop any internal goroutines and return an error so the Registry does not
// register a half-initialised entry. The Registry derives ctx with a sync
// deadline before calling Start.
type InformerFactory interface {
	Start(ctx context.Context, gvk schema.GroupVersionKind, scope apimeta.RESTScopeName, handler InformerEventHandler) (InformerEntry, error)
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

	// SyncTimeout overrides DefaultSyncTimeout. Zero means use the default.
	// Envtest sets a smaller value to keep test runtimes tight.
	SyncTimeout time.Duration

	// FailureCooldown overrides how long a failed informer start is cached
	// before Subscribe retries the factory. Zero means use the effective
	// sync timeout. An informer that can never sync (RBAC-forbidden kind,
	// CRD deleted behind a discovery-cache hit) would otherwise block a
	// reconcile worker for the full sync timeout on every attempt, starving
	// every other owner served by that worker.
	FailureCooldown time.Duration

	// Now returns the current time; injectable for tests. Nil means time.Now.
	Now func() time.Time

	// mu protects informers, refcount, starting, and failed. List and
	// GVKCount take the read lock; Subscribe, Unsubscribe, and lifecycle
	// changes take the write lock. The subscriber index has its own RWMutex.
	mu        sync.RWMutex
	informers map[schema.GroupVersionKind]InformerEntry
	refcount  map[schema.GroupVersionKind]int
	// starting tracks GVKs whose informer Start is in flight. New Subscribes
	// for the same GVK wait on the in-flight result instead of either
	// serialising on r.mu or starting a duplicate informer.
	starting map[schema.GroupVersionKind]*startInFlight
	// failed is the negative cache of informer starts: within the cooldown,
	// Subscribe returns the recorded error without re-blocking on the
	// factory. Cleared on successful start and by InvalidateGroupKind (so a
	// CRD becoming Established retries immediately).
	failed map[schema.GroupVersionKind]failedStart
}

// failedStart records one failed informer start for the negative cache.
type failedStart struct {
	err error
	at  time.Time
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
		failed:    make(map[schema.GroupVersionKind]failedStart),
	}
}

func (r *Registry) now() time.Time {
	if r.Now == nil {
		return time.Now()
	}
	return r.Now()
}

// Subscribe registers sub for gvk, starting the per-GVK informer (and waiting
// for its cache to sync) on the first subscriber. Re-subscribing the same
// Owner replaces the previous matcher set without changing refcounts.
//
// factory.Start runs without holding r.mu — concurrent Subscribes for
// unrelated GVKs proceed in parallel; concurrent Subscribes for the same GVK
// share a single in-flight Start (one informer per GVK).
func (r *Registry) Subscribe(ctx context.Context, gvk schema.GroupVersionKind, scope apimeta.RESTScopeName, sub Subscriber) error {
	timeout := r.SyncTimeout
	if timeout <= 0 {
		timeout = DefaultSyncTimeout
	}
	cooldown := r.FailureCooldown
	if cooldown <= 0 {
		cooldown = timeout
	}
	for {
		r.mu.Lock()
		if _, running := r.informers[gvk]; running {
			r.registerLocked(gvk, sub)
			r.mu.Unlock()
			return nil
		}
		if f, ok := r.failed[gvk]; ok {
			if r.now().Before(f.at.Add(cooldown)) {
				r.mu.Unlock()
				metrics.SubscribeTotal.WithLabelValues(gvk.Group, gvk.Version, gvk.Kind, metrics.SubscribeError).Inc()
				return fmt.Errorf("watcher: start informer for %s (cooling down after failure): %w", gvk, f.err)
			}
			delete(r.failed, gvk)
		}
		if inflight, ok := r.starting[gvk]; ok {
			r.mu.Unlock()
			select {
			case <-inflight.done:
			case <-ctx.Done():
				return fmt.Errorf("watcher: subscribe %s cancelled: %w", gvk, ctx.Err())
			}
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
		// Always tear down the inflight sentinel and release followers, even
		// on panic; without this a panicking Start would strand every
		// follower on <-inflight.done forever and the orphaned starting[gvk]
		// entry would prevent any future Subscribe from starting a fresh
		// informer.
		defer func() {
			r.mu.Lock()
			delete(r.starting, gvk)
			r.mu.Unlock()
			close(inflight.done)
		}()

		startCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		entry, err := r.factory.Start(startCtx, gvk, scope, r.dispatch(gvk))

		r.mu.Lock()
		if err != nil {
			inflight.err = err
			r.failed[gvk] = failedStart{err: err, at: r.now()}
			metrics.SubscribeTotal.WithLabelValues(gvk.Group, gvk.Version, gvk.Kind, metrics.SubscribeError).Inc()
			r.mu.Unlock()
			return fmt.Errorf("watcher: start informer for %s: %w", gvk, err)
		}
		delete(r.failed, gvk)
		r.informers[gvk] = entry
		metrics.Informers.WithLabelValues(gvk.Group, gvk.Version, gvk.Kind).Set(1)
		r.registerLocked(gvk, sub)
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
	r.mu.RLock()
	entry, ok := r.informers[gvk]
	r.mu.RUnlock()
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
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.informers)
}

// GVKsByOwner reports which GVKs owner currently subscribes to. Used by the
// reconciler to compute subscription diffs.
func (r *Registry) GVKsByOwner(owner OwnerKey) []schema.GroupVersionKind {
	return r.index.GVKsByOwner(owner)
}

// InvalidateGroupKind drops every informer entry whose group and kind match
// and clears the subscriber index for each. Used by the CRD watcher when a
// CRD is removed or de-Established: a CRD can serve multiple versions, so
// the right unit is (group, kind), not a single GVK. A subsequent reconcile
// for any owner that still references this kind will re-Subscribe from a
// clean state.
//
// Safe to call when no matching informer exists (no-op).
func (r *Registry) InvalidateGroupKind(group, kind string) {
	r.mu.Lock()
	// Drop negative-cache entries too: the caller signals the (group, kind)
	// changed state (e.g. CRD now Established), so a Subscribe blocked only
	// by the failure cooldown must retry immediately.
	for gvk := range r.failed {
		if gvk.Group == group && gvk.Kind == kind {
			delete(r.failed, gvk)
		}
	}
	matched := make([]schema.GroupVersionKind, 0, 1)
	for gvk := range r.informers {
		if gvk.Group == group && gvk.Kind == kind {
			matched = append(matched, gvk)
		}
	}
	entries := make([]InformerEntry, 0, len(matched))
	for _, gvk := range matched {
		entries = append(entries, r.informers[gvk])
		delete(r.informers, gvk)
		delete(r.refcount, gvk)
		// ClearGVK must run inside the r.mu critical section: released
		// between the map deletes and the index clear, a racing Subscribe
		// can start a fresh informer and register against the stale index
		// (registerLocked sees index.Has and skips the refcount increment),
		// only for ClearGVK to wipe the new subscriber — an orphaned informer
		// whose events dispatch to nobody. Lock ordering r.mu → index.mu is
		// already established by registerLocked.
		r.index.ClearGVK(gvk)
	}
	r.mu.Unlock()

	for _, entry := range entries {
		entry.Stop()
	}
	for _, gvk := range matched {
		metrics.Informers.DeleteLabelValues(gvk.Group, gvk.Version, gvk.Kind)
		metrics.Subscribers.DeleteLabelValues(gvk.Group, gvk.Version, gvk.Kind)
	}
}

// Start implements manager.Runnable. The Registry participates in manager
// lifecycle so that all informers stop cleanly when the manager shuts down
// (otherwise informer dispatch goroutines outlive the controllers and would
// either deadlock on a closed workqueue or pile up event-handler closures
// against a stale subscriber index).
//
// Returns when ctx is cancelled. The default-leader-election semantics
// apply (i.e. the registry runs only on the leader manager), matching the
// controllers that consume it.
func (r *Registry) Start(ctx context.Context) error {
	<-ctx.Done()
	r.mu.Lock()
	defer r.mu.Unlock()
	for gvk, entry := range r.informers {
		entry.Stop()
		delete(r.informers, gvk)
		metrics.Informers.DeleteLabelValues(gvk.Group, gvk.Version, gvk.Kind)
		metrics.Subscribers.DeleteLabelValues(gvk.Group, gvk.Version, gvk.Kind)
	}
	r.refcount = make(map[schema.GroupVersionKind]int)
	return nil
}

func (r *Registry) hasSubscriberLocked(gvk schema.GroupVersionKind, owner OwnerKey) bool {
	return r.index.Has(gvk, owner)
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
