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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientdiscovery "k8s.io/client-go/discovery"
)

// ErrGVKNotEstablished signals the requested group/kind cannot be resolved by
// discovery, typically because the CRD is not installed yet.
var ErrGVKNotEstablished = errors.New("GVK not established")

// Discoverer is the minimal subset of k8s.io/client-go/discovery.DiscoveryInterface
// needed by the resolver. The real client-go methods do not yet accept a
// context (k8s.io/client-go v0.33); WrapClient adapts a real client into this
// interface, and tests fake it directly.
type Discoverer interface {
	ServerGroups(ctx context.Context) (*metav1.APIGroupList, error)
	ServerResourcesForGroupVersion(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error)
}

// WrapClient adapts a client-go DiscoveryInterface to the context-aware
// Discoverer used by the resolver. The ctx is not yet honoured by client-go
// discovery; the underlying RESTClient timeout currently bounds these calls.
// When client-go ships ctx-aware discovery, this wrapper will pass ctx through
// and become a no-op adapter.
func WrapClient(d clientdiscovery.DiscoveryInterface) Discoverer {
	return &clientDiscoverer{d: d}
}

type clientDiscoverer struct {
	d clientdiscovery.DiscoveryInterface
}

func (c *clientDiscoverer) ServerGroups(_ context.Context) (*metav1.APIGroupList, error) {
	return c.d.ServerGroups()
}

func (c *clientDiscoverer) ServerResourcesForGroupVersion(_ context.Context, gv string) (*metav1.APIResourceList, error) {
	return c.d.ServerResourcesForGroupVersion(gv)
}

// Resolver maps target identities to concrete GVK + scope.
type Resolver interface {
	Resolve(ctx context.Context, group, kind, version string) (schema.GroupVersionKind, apimeta.RESTScopeName, error)
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

func (r *resolver) Resolve(ctx context.Context, group, kind, version string) (schema.GroupVersionKind, apimeta.RESTScopeName, error) {
	key := cacheKey{group, kind, version}
	if hit, ok := r.lookup(key); ok {
		return hit.gvk, hit.scope, nil
	}

	resolvedVersion := version
	if resolvedVersion == "" {
		// ServerGroups doesn't list the core API group; short-circuit so that
		// omitted-version core resources (ConfigMap, Pod, …) resolve.
		if group == "" {
			resolvedVersion = "v1"
		} else {
			v, err := r.preferredVersion(ctx, group)
			if err != nil {
				return schema.GroupVersionKind{}, "", err
			}
			resolvedVersion = v
		}
	}

	gv := groupVersionString(group, resolvedVersion)
	rl, err := r.d.ServerResourcesForGroupVersion(ctx, gv)
	if err != nil {
		return schema.GroupVersionKind{}, "", fmt.Errorf("%w: %s: %w", ErrGVKNotEstablished, gv, err)
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

func (r *resolver) preferredVersion(ctx context.Context, group string) (string, error) {
	groups, err := r.d.ServerGroups(ctx)
	if err != nil {
		return "", fmt.Errorf("%w: ServerGroups: %w", ErrGVKNotEstablished, err)
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
