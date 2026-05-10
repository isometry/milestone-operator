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

// Subscriber represents one Echelon's subscription to a single GVK.
type Subscriber struct {
	Owner            OwnerKey
	Selector         labels.Selector
	NamespaceMatcher func(namespace string) bool
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

// Add inserts or replaces sub for sub.Owner under gvk.
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

// Subscribers returns the owners whose Selector and NamespaceMatcher both
// admit obj.
func (s *SubscriberIndex) Subscribers(gvk schema.GroupVersionKind, obj client.Object) []OwnerKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	subs, ok := s.byGVK[gvk]
	if !ok {
		return nil
	}
	out := make([]OwnerKey, 0, len(subs))
	for _, sub := range subs {
		if !matches(sub, obj) {
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

func matches(sub Subscriber, obj client.Object) bool {
	if sub.NamespaceMatcher != nil && !sub.NamespaceMatcher(obj.GetNamespace()) {
		return false
	}
	if sub.Selector != nil && !sub.Selector.Matches(labels.Set(obj.GetLabels())) {
		return false
	}
	return true
}
