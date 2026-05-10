/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package discovery resolves (group, kind, optional version) to a concrete
// GroupVersionKind and REST scope, with a small in-memory TTL cache to avoid
// hammering the apiserver discovery endpoints in the reconcile hot path.
package discovery

import (
	"errors"
	"fmt"
	"sync"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ErrGVKNotEstablished signals the requested group/kind cannot be resolved by
// discovery, typically because the CRD is not installed yet.
var ErrGVKNotEstablished = errors.New("GVK not established")

// Discoverer is the minimal subset of k8s.io/client-go/discovery.DiscoveryInterface
// needed by the resolver. Defining it here lets tests fake it without dragging
// in the full clientgo testing kit.
type Discoverer interface {
	ServerGroups() (*metav1.APIGroupList, error)
	ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error)
}

// Resolver maps target identities to concrete GVK + scope.
type Resolver interface {
	Resolve(group, kind, version string) (schema.GroupVersionKind, apimeta.RESTScopeName, error)
	Invalidate()
}

type cacheKey struct{ group, kind, version string }

type cacheEntry struct {
	gvk    schema.GroupVersionKind
	scope  apimeta.RESTScopeName
	expiry time.Time
}

type resolver struct {
	d   Discoverer
	ttl time.Duration
	now func() time.Time

	mu    sync.RWMutex
	cache map[cacheKey]cacheEntry
}

// NewResolver returns a Resolver wrapping the given Discoverer with a TTL
// cache. Negative results (ErrGVKNotEstablished) are never cached so that
// late-installed CRDs are picked up promptly.
func NewResolver(d Discoverer, ttl time.Duration) Resolver {
	return &resolver{
		d:     d,
		ttl:   ttl,
		now:   time.Now,
		cache: make(map[cacheKey]cacheEntry),
	}
}

func (r *resolver) Resolve(group, kind, version string) (schema.GroupVersionKind, apimeta.RESTScopeName, error) {
	key := cacheKey{group, kind, version}
	if hit, ok := r.lookup(key); ok {
		return hit.gvk, hit.scope, nil
	}

	resolvedVersion := version
	if resolvedVersion == "" {
		v, err := r.preferredVersion(group)
		if err != nil {
			return schema.GroupVersionKind{}, "", err
		}
		resolvedVersion = v
	}

	gv := groupVersionString(group, resolvedVersion)
	rl, err := r.d.ServerResourcesForGroupVersion(gv)
	if err != nil {
		return schema.GroupVersionKind{}, "", fmt.Errorf("%w: %s: %v", ErrGVKNotEstablished, gv, err)
	}
	for _, res := range rl.APIResources {
		if res.Kind != kind {
			continue
		}
		gvk := schema.GroupVersionKind{Group: group, Version: resolvedVersion, Kind: kind}
		scope := apimeta.RESTScopeNameRoot
		if res.Namespaced {
			scope = apimeta.RESTScopeNameNamespace
		}
		r.store(key, cacheEntry{gvk: gvk, scope: scope, expiry: r.now().Add(r.ttl)})
		return gvk, scope, nil
	}
	return schema.GroupVersionKind{}, "", fmt.Errorf("%w: kind %q not found in %s", ErrGVKNotEstablished, kind, gv)
}

func (r *resolver) Invalidate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache = make(map[cacheKey]cacheEntry)
}

func (r *resolver) lookup(k cacheKey) (cacheEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.cache[k]
	if !ok || r.now().After(e.expiry) {
		return cacheEntry{}, false
	}
	return e, true
}

func (r *resolver) store(k cacheKey, e cacheEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[k] = e
}

func (r *resolver) preferredVersion(group string) (string, error) {
	groups, err := r.d.ServerGroups()
	if err != nil {
		return "", fmt.Errorf("%w: ServerGroups: %v", ErrGVKNotEstablished, err)
	}
	for _, g := range groups.Groups {
		if g.Name != group {
			continue
		}
		if g.PreferredVersion.Version != "" {
			return g.PreferredVersion.Version, nil
		}
		if len(g.Versions) > 0 {
			return g.Versions[0].Version, nil
		}
		return "", fmt.Errorf("%w: group %q has no versions", ErrGVKNotEstablished, group)
	}
	return "", fmt.Errorf("%w: group %q not found", ErrGVKNotEstablished, group)
}

func groupVersionString(group, version string) string {
	if group == "" {
		return version
	}
	return group + "/" + version
}
