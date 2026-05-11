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
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

// DynamicFactory implements InformerFactory using client-go's dynamic
// informer machinery. One DynamicSharedInformerFactory is created per GVK so
// each informer has independent lifecycle (start/stop without affecting peers).
type DynamicFactory struct {
	client       dynamic.Interface
	mapper       apimeta.RESTMapper
	resyncPeriod time.Duration
}

// NewDynamicFactory constructs a DynamicFactory.
func NewDynamicFactory(client dynamic.Interface, mapper apimeta.RESTMapper, resync time.Duration) *DynamicFactory {
	return &DynamicFactory{client: client, mapper: mapper, resyncPeriod: resync}
}

// Start creates and runs an informer for the given GVK, blocking until the
// cache has reported HasSynced or ctx is cancelled (the Registry derives ctx
// with a sync-timeout deadline). Returning before sync would let the caller
// observe an empty cache and report Ready=True for a member whose resources
// simply hadn't loaded yet.
func (f *DynamicFactory) Start(ctx context.Context, gvk schema.GroupVersionKind, _ apimeta.RESTScopeName, handler InformerEventHandler) (InformerEntry, error) {
	mapping, err := f.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, fmt.Errorf("REST mapping for %s: %w", gvk, err)
	}
	gvr := mapping.Resource

	infFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(f.client, f.resyncPeriod, "", nil)
	gen := infFactory.ForResource(gvr)
	if _, err := gen.Informer().AddEventHandler(newCacheHandler(handler)); err != nil {
		return nil, fmt.Errorf("AddEventHandler for %s: %w", gvk, err)
	}

	stopCh := make(chan struct{})
	infFactory.Start(stopCh)

	if !cache.WaitForCacheSync(ctx.Done(), gen.Informer().HasSynced) {
		close(stopCh)
		return nil, fmt.Errorf("informer sync for %s: %w", gvk, ctx.Err())
	}

	return &dynamicEntry{
		gvk:    gvk,
		gvr:    gvr,
		stop:   stopCh,
		lister: gen.Lister(),
	}, nil
}

type dynamicEntry struct {
	gvk    schema.GroupVersionKind
	gvr    schema.GroupVersionResource
	stop   chan struct{}
	lister cache.GenericLister
}

func (e *dynamicEntry) Stop() {
	select {
	case <-e.stop: // already closed
	default:
		close(e.stop)
	}
}

func (e *dynamicEntry) List() ([]*unstructured.Unstructured, error) {
	objs, err := e.lister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", e.gvk, err)
	}
	out := make([]*unstructured.Unstructured, 0, len(objs))
	for _, o := range objs {
		if u, ok := o.(*unstructured.Unstructured); ok {
			out = append(out, u)
		}
	}
	return out, nil
}

// newCacheHandler adapts our InformerEventHandler to client-go's
// cache.ResourceEventHandler. It tolerates DeletedFinalStateUnknown so that
// missed deletes during a resync still reach the dispatcher.
func newCacheHandler(handler InformerEventHandler) cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if u, ok := asUnstructured(obj); ok {
				handler(EventAdd, u)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if u, ok := asUnstructured(newObj); ok {
				handler(EventUpdate, u)
			}
		},
		DeleteFunc: func(obj any) {
			if u, ok := asUnstructured(obj); ok {
				handler(EventDelete, u)
			}
		},
	}
}

// asUnstructured extracts an *unstructured.Unstructured from the object the
// informer hands us, transparently unwrapping DeletedFinalStateUnknown.
func asUnstructured(obj any) (*unstructured.Unstructured, bool) {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u, true
	}
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if u, ok := d.Obj.(*unstructured.Unstructured); ok {
			return u, true
		}
	}
	return nil, false
}
