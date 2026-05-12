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

	"github.com/isometry/milestone-operator/internal/watcher"
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

// subWith constructs a Subscriber with a single Matcher in its Matchers slice.
// Keeps test call-sites concise for the common single-matcher case.
func subWith(owner watcher.OwnerKey, sel labels.Selector, ns func(string) bool) watcher.Subscriber {
	return watcher.Subscriber{
		Owner:    owner,
		Matchers: []watcher.Matcher{{Selector: sel, Namespaces: ns}},
	}
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
	wave0 := subWith(
		watcher.OwnerKey{Kind: kindMilestone, Namespace: nsFluxSystem, Name: nameWave0},
		mustSelector(t, map[string]string{labelWave: "0"}), nil,
	)
	wave1 := subWith(
		watcher.OwnerKey{Kind: kindMilestone, Namespace: nsFluxSystem, Name: "wave-1"},
		mustSelector(t, map[string]string{labelWave: "1"}), nil,
	)
	idx.Add(kustomizationGVK, wave0)
	idx.Add(kustomizationGVK, wave1)

	got := idx.Subscribers(kustomizationGVK, makeObj(nsFluxSystem, "k1", map[string]string{labelWave: "0"}))
	want := []string{milestoneFluxSystemWave0Key}
	if !equal(ownerKeys(got), want) {
		t.Errorf("subscribers = %v, want %v", ownerKeys(got), want)
	}

	got = idx.Subscribers(kustomizationGVK, makeObj(nsFluxSystem, "k2", map[string]string{labelWave: "1"}))
	want = []string{"Milestone/flux-system/wave-1"}
	if !equal(ownerKeys(got), want) {
		t.Errorf("subscribers = %v, want %v", ownerKeys(got), want)
	}

	got = idx.Subscribers(kustomizationGVK, makeObj(nsFluxSystem, "k3", map[string]string{labelWave: "2"}))
	if len(got) != 0 {
		t.Errorf("subscribers = %v, want []", ownerKeys(got))
	}
}

func TestSubscriberIndex_Subscribers_NamespaceMatcher(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	teamA := subWith(
		watcher.OwnerKey{Kind: "ClusterMilestone", Name: "platform-wave"},
		labels.Everything(),
		func(ns string) bool { return ns == "team-a" },
	)
	all := subWith(
		watcher.OwnerKey{Kind: "ClusterMilestone", Name: "global-wave"},
		labels.Everything(), nil,
	)
	idx.Add(kustomizationGVK, teamA)
	idx.Add(kustomizationGVK, all)

	got := idx.Subscribers(kustomizationGVK, makeObj("team-a", "k1", nil))
	if !equal(ownerKeys(got), []string{"ClusterMilestone//global-wave", "ClusterMilestone//platform-wave"}) {
		t.Errorf("subscribers (team-a) = %v", ownerKeys(got))
	}

	got = idx.Subscribers(kustomizationGVK, makeObj("team-b", "k2", nil))
	if !equal(ownerKeys(got), []string{"ClusterMilestone//global-wave"}) {
		t.Errorf("subscribers (team-b) = %v", ownerKeys(got))
	}
}

func TestSubscriberIndex_NoMatchers_MatchesAll(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	idx.Add(kustomizationGVK, watcher.Subscriber{
		Owner: watcher.OwnerKey{Kind: kindMilestone, Namespace: nsFluxSystem, Name: "all"},
		// Matchers intentionally empty ⇒ should match everything (preserves
		// prior nil-selector semantics).
	})
	got := idx.Subscribers(kustomizationGVK, makeObj(nsFluxSystem, "anything", map[string]string{"any": "label"}))
	if len(got) != 1 {
		t.Errorf("empty matchers should match everything, got %v", ownerKeys(got))
	}
}

func TestSubscriberIndex_Remove(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: nsFluxSystem, Name: nameWave0}
	idx.Add(kustomizationGVK, subWith(owner, labels.Everything(), nil))
	if c := idx.SubscriberCount(kustomizationGVK); c != 1 {
		t.Errorf("count after Add = %d, want 1", c)
	}
	idx.Remove(kustomizationGVK, owner)
	if c := idx.SubscriberCount(kustomizationGVK); c != 0 {
		t.Errorf("count after Remove = %d, want 0", c)
	}
	got := idx.Subscribers(kustomizationGVK, makeObj(nsFluxSystem, "k1", nil))
	if len(got) != 0 {
		t.Errorf("subscribers after Remove = %v, want []", ownerKeys(got))
	}
}

// TestSubscriberIndex_AddReplacesMatcherSet pins the atomicity invariant:
// Add({A}) followed by Add({B}) yields exactly {B}, not A∪B. The reconciler
// passes the *full* matcher set on every Subscribe, so anything else would
// silently retain stale matchers from previous reconciles.
func TestSubscriberIndex_AddReplacesMatcherSet(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: nsFluxSystem, Name: nameWave0}
	idx.Add(kustomizationGVK, subWith(owner, mustSelector(t, map[string]string{labelWave: "0"}), nil))
	idx.Add(kustomizationGVK, subWith(owner, mustSelector(t, map[string]string{labelWave: "1"}), nil))
	if c := idx.SubscriberCount(kustomizationGVK); c != 1 {
		t.Errorf("count after re-Add = %d, want 1", c)
	}
	got := idx.Subscribers(kustomizationGVK, makeObj(nsFluxSystem, "k1", map[string]string{labelWave: "1"}))
	if len(got) != 1 {
		t.Errorf("re-Add should keep new matcher, got %v", ownerKeys(got))
	}
	got = idx.Subscribers(kustomizationGVK, makeObj(nsFluxSystem, "k0", map[string]string{labelWave: "0"}))
	if len(got) != 0 {
		t.Errorf("re-Add should discard old matcher, got %v", ownerKeys(got))
	}
}

// TestSubscriberIndex_MultipleMatchers_SameOwnerGVK covers the same-GVK
// members case: one owner declares two label selectors for the same GVK and
// objects admitted by either selector must enqueue the owner.
func TestSubscriberIndex_MultipleMatchers_SameOwnerGVK(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: nsFluxSystem, Name: nameWave0}
	idx.Add(kustomizationGVK, watcher.Subscriber{
		Owner: owner,
		Matchers: []watcher.Matcher{
			{Selector: mustSelector(t, map[string]string{labelWave: "0"})},
			{Selector: mustSelector(t, map[string]string{labelWave: "1"})},
		},
	})

	for _, wave := range []string{"0", "1"} {
		got := idx.Subscribers(kustomizationGVK, makeObj(nsFluxSystem, "k-"+wave, map[string]string{labelWave: wave}))
		if !equal(ownerKeys(got), []string{milestoneFluxSystemWave0Key}) {
			t.Errorf("wave=%s: subscribers = %v, want wave-0", wave, ownerKeys(got))
		}
	}

	got := idx.Subscribers(kustomizationGVK, makeObj(nsFluxSystem, "k-2", map[string]string{labelWave: "2"}))
	if len(got) != 0 {
		t.Errorf("wave=2 should not match, got %v", ownerKeys(got))
	}
}

// TestSubscriberIndex_MultipleMatchers_OwnerReturnedOnce covers the dedupe
// invariant: even when an object would be admitted by every Matcher on a
// Subscriber, the owner is enqueued exactly once.
func TestSubscriberIndex_MultipleMatchers_OwnerReturnedOnce(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: nsFluxSystem, Name: nameWave0}
	idx.Add(kustomizationGVK, watcher.Subscriber{
		Owner: owner,
		Matchers: []watcher.Matcher{
			{Selector: labels.Everything()},
			{Selector: labels.Everything()},
		},
	})
	got := idx.Subscribers(kustomizationGVK, makeObj(nsFluxSystem, "k1", nil))
	if !equal(ownerKeys(got), []string{milestoneFluxSystemWave0Key}) {
		t.Errorf("owner should appear once, got %v", ownerKeys(got))
	}
}

func TestSubscriberIndex_GVKsByOwner(t *testing.T) {
	idx := watcher.NewSubscriberIndex()
	owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: nsFluxSystem, Name: "multi"}
	helmGVK := schema.GroupVersionKind{Group: groupHelmToolkit, Version: "v2", Kind: kindHelmRelease}
	idx.Add(kustomizationGVK, subWith(owner, labels.Everything(), nil))
	idx.Add(helmGVK, subWith(owner, labels.Everything(), nil))

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
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			owner := watcher.OwnerKey{Kind: kindMilestone, Namespace: nsFluxSystem, Name: nameOf(i)}
			idx.Add(kustomizationGVK, subWith(owner, labels.Everything(), nil))
			_ = idx.Subscribers(kustomizationGVK, makeObj(nsFluxSystem, "k", nil))
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
