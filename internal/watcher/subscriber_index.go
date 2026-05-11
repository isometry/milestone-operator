/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package watcher manages dynamic informers and dispatches member events to
// the Echelons that subscribe to them.
package watcher

import (
	"sync"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// OwnerKey identifies an Echelon or ClusterEchelon object.
type OwnerKey struct {
	Kind      string
	Namespace string
	Name      string
}

// Matcher pairs a label selector with a namespace filter; an object is admitted
// when both predicates return true. A nil Selector admits any labels; a nil
// Namespaces admits any namespace.
type Matcher struct {
	Selector   labels.Selector
	Namespaces func(namespace string) bool
}

// Admit reports whether obj passes this Matcher.
func (m Matcher) Admit(obj client.Object) bool {
	if m.Namespaces != nil && !m.Namespaces(obj.GetNamespace()) {
		return false
	}
	if m.Selector != nil && !m.Selector.Matches(labels.Set(obj.GetLabels())) {
		return false
	}
	return true
}

// Subscriber is one owner's subscription to a single GVK. An owner may have
// multiple spec.members entries that share a GVK with disjoint selectors; all
// of them live on Matchers so a single informer event can wake the owner once
// regardless of which member admitted the object.
type Subscriber struct {
	Owner    OwnerKey
	Matchers []Matcher
}

// Match reports whether any of s.Matchers admits obj. A Subscriber with no
// matchers admits everything (preserves prior nil-selector semantics).
func (s Subscriber) Match(obj client.Object) bool {
	if len(s.Matchers) == 0 {
		return true
	}
	for _, m := range s.Matchers {
		if m.Admit(obj) {
			return true
		}
	}
	return false
}

// SubscriberIndex maps GVKs to their interested Echelons and supports the
// reverse lookup needed at unsubscribe time.
type SubscriberIndex struct {
	mu      sync.RWMutex
	byGVK   map[schema.GroupVersionKind]map[OwnerKey]Subscriber
	byOwner map[OwnerKey]map[schema.GroupVersionKind]struct{}
}

// NewSubscriberIndex returns an empty SubscriberIndex.
func NewSubscriberIndex() *SubscriberIndex {
	return &SubscriberIndex{
		byGVK:   make(map[schema.GroupVersionKind]map[OwnerKey]Subscriber),
		byOwner: make(map[OwnerKey]map[schema.GroupVersionKind]struct{}),
	}
}

// Add replaces sub.Owner's entire matcher set for gvk. The atomic unit is "all
// matchers an owner declares for this GVK in one reconcile"; callers must pass
// the full set every time, not deltas.
func (s *SubscriberIndex) Add(gvk schema.GroupVersionKind, sub Subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byGVK[gvk]; !ok {
		s.byGVK[gvk] = make(map[OwnerKey]Subscriber)
	}
	s.byGVK[gvk][sub.Owner] = sub
	if _, ok := s.byOwner[sub.Owner]; !ok {
		s.byOwner[sub.Owner] = make(map[schema.GroupVersionKind]struct{})
	}
	s.byOwner[sub.Owner][gvk] = struct{}{}
}

// Remove drops owner's subscription for gvk. No-op if absent.
func (s *SubscriberIndex) Remove(gvk schema.GroupVersionKind, owner OwnerKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if subs, ok := s.byGVK[gvk]; ok {
		delete(subs, owner)
		if len(subs) == 0 {
			delete(s.byGVK, gvk)
		}
	}
	if gvks, ok := s.byOwner[owner]; ok {
		delete(gvks, gvk)
		if len(gvks) == 0 {
			delete(s.byOwner, owner)
		}
	}
}

// Subscribers returns the distinct owners whose Subscriber admits obj. Each
// owner appears at most once even when multiple matchers within its Subscriber
// would all admit obj.
func (s *SubscriberIndex) Subscribers(gvk schema.GroupVersionKind, obj client.Object) []OwnerKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	subs, ok := s.byGVK[gvk]
	if !ok {
		return nil
	}
	out := make([]OwnerKey, 0, len(subs))
	for _, sub := range subs {
		if !sub.Match(obj) {
			continue
		}
		out = append(out, sub.Owner)
	}
	return out
}

// SubscriberCount returns the number of owners subscribed to gvk.
func (s *SubscriberIndex) SubscriberCount(gvk schema.GroupVersionKind) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byGVK[gvk])
}

// GVKsByOwner lists every GVK owner is subscribed to.
func (s *SubscriberIndex) GVKsByOwner(owner OwnerKey) []schema.GroupVersionKind {
	s.mu.RLock()
	defer s.mu.RUnlock()
	gvks, ok := s.byOwner[owner]
	if !ok {
		return nil
	}
	out := make([]schema.GroupVersionKind, 0, len(gvks))
	for g := range gvks {
		out = append(out, g)
	}
	return out
}
