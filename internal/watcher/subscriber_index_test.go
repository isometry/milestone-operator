/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package watcher_test

import (
	"sort"
	"sync"
	"testing"

	"github.com/isometry/echelon-operator/internal/watcher"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var kustomizationGVK = schema.GroupVersionKind{
	Group:   "kustomize.toolkit.fluxcd.io",
	Version: "v1",
	Kind:    "Kustomization",
}

func makeObj(ns, name string, lbls map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("kustomize.toolkit.fluxcd.io/v1")
	u.SetKind("Kustomization")
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetLabels(lbls)
	return u
}

func ownerKeys(in []watcher.OwnerKey) []string {
	out := make([]string, len(in))
	for i, k := range in {
		out[i] = k.Kind + "/" + k.Namespace + "/" + k.Name
	}
	sort.Strings(out)
	return out
}

func TestSubscriberIndex_Subscribers_LabelSelector(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	wave0 := watcher.Subscriber{
		Owner:    watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: "wave-0"},
		Selector: mustSelector(t, map[string]string{"wave": "0"}),
	}
	wave1 := watcher.Subscriber{
		Owner:    watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: "wave-1"},
		Selector: mustSelector(t, map[string]string{"wave": "1"}),
	}
	idx.Add(kustomizationGVK, wave0)
	idx.Add(kustomizationGVK, wave1)

	got := idx.Subscribers(kustomizationGVK, makeObj("flux-system", "k1", map[string]string{"wave": "0"}))
	want := []string{"Echelon/flux-system/wave-0"}
	if !equal(ownerKeys(got), want) {
		t.Errorf("subscribers = %v, want %v", ownerKeys(got), want)
	}

	got = idx.Subscribers(kustomizationGVK, makeObj("flux-system", "k2", map[string]string{"wave": "1"}))
	want = []string{"Echelon/flux-system/wave-1"}
	if !equal(ownerKeys(got), want) {
		t.Errorf("subscribers = %v, want %v", ownerKeys(got), want)
	}

	got = idx.Subscribers(kustomizationGVK, makeObj("flux-system", "k3", map[string]string{"wave": "2"}))
	if len(got) != 0 {
		t.Errorf("subscribers = %v, want []", ownerKeys(got))
	}
}

func TestSubscriberIndex_Subscribers_NamespaceMatcher(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	teamA := watcher.Subscriber{
		Owner:            watcher.OwnerKey{Kind: "ClusterEchelon", Name: "platform-wave"},
		Selector:         labels.Everything(),
		NamespaceMatcher: func(ns string) bool { return ns == "team-a" },
	}
	all := watcher.Subscriber{
		Owner:    watcher.OwnerKey{Kind: "ClusterEchelon", Name: "global-wave"},
		Selector: labels.Everything(),
	}
	idx.Add(kustomizationGVK, teamA)
	idx.Add(kustomizationGVK, all)

	got := idx.Subscribers(kustomizationGVK, makeObj("team-a", "k1", nil))
	if !equal(ownerKeys(got), []string{"ClusterEchelon//global-wave", "ClusterEchelon//platform-wave"}) {
		t.Errorf("subscribers (team-a) = %v", ownerKeys(got))
	}

	got = idx.Subscribers(kustomizationGVK, makeObj("team-b", "k2", nil))
	if !equal(ownerKeys(got), []string{"ClusterEchelon//global-wave"}) {
		t.Errorf("subscribers (team-b) = %v", ownerKeys(got))
	}
}

func TestSubscriberIndex_NilSelectorMatchesAll(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	idx.Add(kustomizationGVK, watcher.Subscriber{
		Owner: watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: "all"},
		// Selector intentionally nil ⇒ should match everything.
	})
	got := idx.Subscribers(kustomizationGVK, makeObj("flux-system", "anything", map[string]string{"any": "label"}))
	if len(got) != 1 {
		t.Errorf("nil selector should match everything, got %v", ownerKeys(got))
	}
}

func TestSubscriberIndex_Remove(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	owner := watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: "wave-0"}
	idx.Add(kustomizationGVK, watcher.Subscriber{Owner: owner, Selector: labels.Everything()})
	if c := idx.SubscriberCount(kustomizationGVK); c != 1 {
		t.Errorf("count after Add = %d, want 1", c)
	}
	idx.Remove(kustomizationGVK, owner)
	if c := idx.SubscriberCount(kustomizationGVK); c != 0 {
		t.Errorf("count after Remove = %d, want 0", c)
	}
	got := idx.Subscribers(kustomizationGVK, makeObj("flux-system", "k1", nil))
	if len(got) != 0 {
		t.Errorf("subscribers after Remove = %v, want []", ownerKeys(got))
	}
}

func TestSubscriberIndex_AddReplacesExistingForSameOwner(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	owner := watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: "wave-0"}
	idx.Add(kustomizationGVK, watcher.Subscriber{Owner: owner, Selector: mustSelector(t, map[string]string{"wave": "0"})})
	idx.Add(kustomizationGVK, watcher.Subscriber{Owner: owner, Selector: mustSelector(t, map[string]string{"wave": "1"})})
	if c := idx.SubscriberCount(kustomizationGVK); c != 1 {
		t.Errorf("count after re-Add = %d, want 1", c)
	}
	got := idx.Subscribers(kustomizationGVK, makeObj("flux-system", "k1", map[string]string{"wave": "1"}))
	if len(got) != 1 {
		t.Errorf("re-Add should update selector, got %v", ownerKeys(got))
	}
	got = idx.Subscribers(kustomizationGVK, makeObj("flux-system", "k0", map[string]string{"wave": "0"}))
	if len(got) != 0 {
		t.Errorf("after re-Add wave=0 should not match, got %v", ownerKeys(got))
	}
}

func TestSubscriberIndex_GVKsByOwner(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	owner := watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: "multi"}
	helmGVK := schema.GroupVersionKind{Group: "helm.toolkit.fluxcd.io", Version: "v2", Kind: "HelmRelease"}
	idx.Add(kustomizationGVK, watcher.Subscriber{Owner: owner, Selector: labels.Everything()})
	idx.Add(helmGVK, watcher.Subscriber{Owner: owner, Selector: labels.Everything()})

	gvks := idx.GVKsByOwner(owner)
	if len(gvks) != 2 {
		t.Fatalf("GVKsByOwner = %v, want 2 entries", gvks)
	}
	want := map[schema.GroupVersionKind]bool{kustomizationGVK: true, helmGVK: true}
	for _, g := range gvks {
		if !want[g] {
			t.Errorf("unexpected gvk %v", g)
		}
	}
}

func TestSubscriberIndex_Concurrent(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			owner := watcher.OwnerKey{Kind: "Echelon", Namespace: "flux-system", Name: nameOf(i)}
			idx.Add(kustomizationGVK, watcher.Subscriber{Owner: owner, Selector: labels.Everything()})
			_ = idx.Subscribers(kustomizationGVK, makeObj("flux-system", "k", nil))
			idx.Remove(kustomizationGVK, owner)
		}(i)
	}
	wg.Wait()
	if c := idx.SubscriberCount(kustomizationGVK); c != 0 {
		t.Errorf("count after concurrent churn = %d, want 0", c)
	}
}

func mustSelector(t *testing.T, m map[string]string) labels.Selector {
	t.Helper()
	sel, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: m})
	if err != nil {
		t.Fatalf("LabelSelectorAsSelector: %v", err)
	}
	return sel
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func nameOf(i int) string {
	return "owner-" + intToStr(i)
}

func intToStr(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var buf [16]byte
	pos := len(buf)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = digits[i%10]
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
