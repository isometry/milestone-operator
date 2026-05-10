/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package watcher

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestNewCacheHandler_DispatchesAllEventTypes(t *testing.T) {
	var got []EventType
	h := newCacheHandler(func(ev EventType, _ client.Object) {
		got = append(got, ev)
	}).(cache.ResourceEventHandlerFuncs)

	u := &unstructured.Unstructured{}
	u.SetName("x")
	h.AddFunc(u)
	h.UpdateFunc(u, u)
	h.DeleteFunc(u)

	want := []EventType{EventAdd, EventUpdate, EventDelete}
	if len(got) != len(want) {
		t.Fatalf("dispatched = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestNewCacheHandler_UnwrapsDeletedFinalStateUnknown(t *testing.T) {
	var seenName string
	h := newCacheHandler(func(_ EventType, obj client.Object) {
		seenName = obj.GetName()
	}).(cache.ResourceEventHandlerFuncs)

	u := &unstructured.Unstructured{}
	u.SetName("vanished")
	h.DeleteFunc(cache.DeletedFinalStateUnknown{Key: "ns/vanished", Obj: u})
	if seenName != "vanished" {
		t.Errorf("DeletedFinalStateUnknown not unwrapped; seen=%q", seenName)
	}
}

func TestNewCacheHandler_IgnoresUnknownObjectTypes(t *testing.T) {
	called := 0
	h := newCacheHandler(func(EventType, client.Object) { called++ }).(cache.ResourceEventHandlerFuncs)
	h.AddFunc("a string is not an Unstructured")
	if called != 0 {
		t.Errorf("handler invoked for unknown type, called=%d", called)
	}
}
