/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	"github.com/isometry/echelon-operator/internal/controller"
	"github.com/isometry/echelon-operator/internal/discovery"
	"github.com/isometry/echelon-operator/internal/watcher"
	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clientdiscovery "k8s.io/client-go/discovery"
	memorydiscovery "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	convergenceMaxDur = 15 * time.Second
	convergencePoll   = 100 * time.Millisecond
	informerWarmup    = 500 * time.Millisecond
)

// envFixture wires a real Reconciler against the envtest apiserver. Each
// fixture creates its own namespace so tests don't leak Widget objects
// across each other.
type envFixture struct {
	t          *testing.T
	registry   *watcher.Registry
	resolver   discovery.Resolver
	reconciler *controller.Reconciler[*apiv1.Echelon]
	mapper     *restmapper.DeferredDiscoveryRESTMapper
	namespace  string
}

func newEnvFixture(t *testing.T) *envFixture {
	t.Helper()
	requiresEnvtest(t)

	ns := strings.ReplaceAll(strings.ToLower(t.Name()), "_", "-")
	if len(ns) > 60 {
		ns = ns[:60]
	}
	createNamespace(t, ns)

	dc, err := clientdiscovery.NewDiscoveryClientForConfig(envtestCfg)
	if err != nil {
		t.Fatalf("discovery client: %v", err)
	}
	resolver := discovery.NewResolver(discovery.WrapClient(dc), time.Hour)

	dyn, err := dynamic.NewForConfig(envtestCfg)
	if err != nil {
		t.Fatalf("dynamic client: %v", err)
	}
	cachedDC := memorydiscovery.NewMemCacheClient(dc)
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(cachedDC)
	dynFactory := watcher.NewDynamicFactory(dyn, mapper, time.Minute)

	registry := watcher.NewRegistry(dynFactory, func(watcher.OwnerKey) {})

	rec := &controller.Reconciler[*apiv1.Echelon]{
		Client:     envtestClient,
		Registry:   registry,
		Resolver:   resolver,
		NewAdapter: controller.NewEchelonAdapter,
		Controller: kindEchelon,
	}
	return &envFixture{t: t, registry: registry, resolver: resolver, reconciler: rec, mapper: mapper, namespace: ns}
}

func createNamespace(t *testing.T, name string) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := envtestClient.Create(t.Context(), ns); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %s: %v", name, err)
	}
}

// createEchelon creates an Echelon in ns and returns the live object.
func createEchelon(t *testing.T, ns, name string, members map[string]apiv1.MemberSpec) *apiv1.Echelon {
	t.Helper()
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       apiv1.EchelonSpec{Members: members},
	}
	if err := envtestClient.Create(t.Context(), ech); err != nil {
		t.Fatalf("create echelon: %v", err)
	}
	return ech
}

// createWidget creates a Widget in ns. ready is one of statusTrue, "False", "" (no
// status — kstatus reports Current).
func createWidget(t *testing.T, ns, name, ready string) *unstructured.Unstructured {
	t.Helper()
	w := newWidget(ns, name, "")
	if err := envtestClient.Create(t.Context(), w); err != nil {
		t.Fatalf("create widget: %v", err)
	}
	if ready != "" {
		_ = unstructured.SetNestedField(w.Object, int64(1), schemaPropStatus, "observedGeneration")
		_ = unstructured.SetNestedSlice(w.Object, []any{
			map[string]any{keyType: apiv1.ConditionReady, schemaPropStatus: ready, keyReason: testReason},
		}, schemaPropStatus, "conditions")
		if err := envtestClient.Status().Update(t.Context(), w); err != nil {
			t.Fatalf("update widget status: %v", err)
		}
	}
	return w
}

// reconcileToConvergence reconciles repeatedly until the predicate returns
// nil, or the deadline expires.
func reconcileToConvergence(t *testing.T, fix *envFixture, key client.ObjectKey, predicate func(*apiv1.Echelon) error) {
	t.Helper()
	deadline := time.Now().Add(convergenceMaxDur)
	for time.Now().Before(deadline) {
		ech := &apiv1.Echelon{}
		if err := envtestClient.Get(t.Context(), key, ech); err != nil {
			t.Fatalf("get echelon: %v", err)
		}
		if _, err := fix.reconciler.ReconcileObject(t.Context(), ech); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		// Re-fetch to read current status.
		_ = envtestClient.Get(t.Context(), key, ech)
		if err := predicate(ech); err == nil {
			return
		}
		time.Sleep(convergencePoll)
	}
	t.Fatalf("did not converge within %s", convergenceMaxDur)
}

func ready(ech *apiv1.Echelon) metav1.ConditionStatus {
	for _, c := range ech.Status.Conditions {
		if c.Type == apiv1.ConditionReady {
			return c.Status
		}
	}
	return ""
}

func stalled(ech *apiv1.Echelon) metav1.ConditionStatus {
	for _, c := range ech.Status.Conditions {
		if c.Type == apiv1.ConditionStalled {
			return c.Status
		}
	}
	return ""
}

// 1. Happy path: an Echelon with one all-Current Widget member converges to Ready=True.
func TestEnvtest_HappyPath_AllCurrent(t *testing.T) {
	fix := newEnvFixture(t)
	createWidget(t, fix.namespace, "w1", statusTrue)

	ech := createEchelon(t, fix.namespace, "happy", map[string]apiv1.MemberSpec{
		widgetPlural: {Group: groupTestAsCode, Kind: kindWidget, EmptySetPolicy: apiv1.EmptySetUnknown},
	})

	// First reconcile adds the finalizer; second reconcile starts the informer.
	// Informers need a moment to sync; we then loop until Ready=True or timeout.
	for i := range 3 {
		_, err := fix.reconciler.ReconcileObject(t.Context(), refresh(t, ech))
		if err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	time.Sleep(informerWarmup)

	reconcileToConvergence(t, fix, client.ObjectKeyFromObject(ech), func(e *apiv1.Echelon) error {
		if ready(e) != metav1.ConditionTrue {
			return fmt.Errorf("Ready=%s", ready(e))
		}
		return nil
	})
}

// 2. Empty selector with NotReady policy yields Ready=False.
func TestEnvtest_EmptySet_NotReadyPolicy(t *testing.T) {
	fix := newEnvFixture(t)
	ech := createEchelon(t, fix.namespace, "empty", map[string]apiv1.MemberSpec{
		widgetPlural: {Group: groupTestAsCode, Kind: kindWidget, EmptySetPolicy: apiv1.EmptySetNotReady},
	})

	for range 3 {
		_, _ = fix.reconciler.ReconcileObject(t.Context(), refresh(t, ech))
	}
	time.Sleep(informerWarmup)

	reconcileToConvergence(t, fix, client.ObjectKeyFromObject(ech), func(e *apiv1.Echelon) error {
		if ready(e) != metav1.ConditionFalse {
			return fmt.Errorf("Ready=%s", ready(e))
		}
		return nil
	})
}

// 3. Late CRD: an Echelon referencing an unknown kind starts Stalled, then
// converges after the CRD is installed.
func TestEnvtest_LateCRD_StalledThenConverges(t *testing.T) {
	fix := newEnvFixture(t)

	ech := createEchelon(t, fix.namespace, memberLate, map[string]apiv1.MemberSpec{
		"lates": {Group: "late.test.as-code.io", Kind: kindLate, EmptySetPolicy: apiv1.EmptySetUnknown},
	})

	// Initial reconcile: should set Stalled=True.
	_, _ = fix.reconciler.ReconcileObject(t.Context(), refresh(t, ech))
	_, _ = fix.reconciler.ReconcileObject(t.Context(), refresh(t, ech)) // post-finalizer

	got := refresh(t, ech)
	if stalled(got) != metav1.ConditionTrue {
		t.Fatalf("Stalled=%s, want True before CRD install; conds=%+v", stalled(got), got.Status.Conditions)
	}

	// Install the late CRD.
	lateCRD := &apiextv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "lates.late.test.as-code.io"},
		Spec: apiextv1.CustomResourceDefinitionSpec{
			Group: "late.test.as-code.io",
			Names: apiextv1.CustomResourceDefinitionNames{
				Plural: "lates", Singular: memberLate, Kind: kindLate, ListKind: "LateList",
			},
			Scope: apiextv1.NamespaceScoped,
			Versions: []apiextv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Schema: &apiextv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextv1.JSONSchemaProps{Type: schemaTypeObject, XPreserveUnknownFields: ptrBool(true)},
				},
			}},
		},
	}
	if err := envtestClient.Create(t.Context(), lateCRD); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create late CRD: %v", err)
	}
	if err := waitForCRDEstablished(t.Context(), envtestClient, "lates.late.test.as-code.io", 10*time.Second); err != nil {
		t.Fatalf("wait CRD established: %v", err)
	}

	// Invalidate both caches (production CRD watcher does the resolver invalidate;
	// the manager's RESTMapper invalidates via its own watcher).
	fix.resolver.Invalidate()
	fix.mapper.Reset()

	reconcileToConvergence(t, fix, client.ObjectKeyFromObject(ech), func(e *apiv1.Echelon) error {
		if stalled(e) == metav1.ConditionTrue {
			return errors.New("still stalled")
		}
		return nil
	})
}

// 4. Subscription diff: removing a member from spec.members tears the
// informer down (registry GVKCount drops back).
func TestEnvtest_SubscriptionDiff_RemovesInformer(t *testing.T) {
	fix := newEnvFixture(t)

	ech := createEchelon(t, fix.namespace, "diff", map[string]apiv1.MemberSpec{
		widgetPlural: {Group: groupTestAsCode, Kind: kindWidget, EmptySetPolicy: apiv1.EmptySetUnknown},
	})
	for range 3 {
		_, _ = fix.reconciler.ReconcileObject(t.Context(), refresh(t, ech))
	}
	if got := fix.registry.GVKCount(); got != 1 {
		t.Fatalf("after subscribe GVKCount=%d, want 1", got)
	}

	// Removing all members requires MinProperties=1 in CRD validation, so we
	// instead delete the Echelon, which triggers the finalizer cleanup.
	if err := envtestClient.Delete(t.Context(), refresh(t, ech)); err != nil {
		t.Fatalf("delete echelon: %v", err)
	}
	for range 3 {
		curr := &apiv1.Echelon{}
		if err := envtestClient.Get(t.Context(), client.ObjectKeyFromObject(ech), curr); err != nil {
			break // gone
		}
		if _, err := fix.reconciler.ReconcileObject(t.Context(), curr); err != nil {
			t.Fatalf("post-delete reconcile: %v", err)
		}
	}
	if got := fix.registry.GVKCount(); got != 0 {
		t.Fatalf("after delete GVKCount=%d, want 0", got)
	}
}

// 5. CRD-level validation: an empty members map must be rejected on Create.
func TestEnvtest_EmptyMembersMap_RejectedByCEL(t *testing.T) {
	fix := newEnvFixture(t)

	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: fix.namespace, Name: "empty-map"},
		Spec:       apiv1.EchelonSpec{Members: map[string]apiv1.MemberSpec{}},
	}
	err := envtestClient.Create(t.Context(), ech)
	if err == nil {
		t.Fatalf("expected validation error for empty members map; got nil")
	}
	if !apierrors.IsInvalid(err) && !strings.Contains(err.Error(), "minProperties") && !strings.Contains(err.Error(), "Invalid") {
		t.Errorf("expected Invalid/minProperties error, got %v", err)
	}
}

// 6. CRD-level validation: a member key that violates the RFC-1123 label
// regex must be rejected by the spec-level CEL XValidation.
func TestEnvtest_InvalidMemberKey_RejectedByCEL(t *testing.T) {
	fix := newEnvFixture(t)

	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: fix.namespace, Name: "bad-key"},
		Spec: apiv1.EchelonSpec{Members: map[string]apiv1.MemberSpec{
			"BadKey_WithCaps": {Group: groupTestAsCode, Kind: kindWidget},
		}},
	}
	err := envtestClient.Create(t.Context(), ech)
	if err == nil {
		t.Fatalf("expected CEL validation error for invalid key; got nil")
	}
	if !apierrors.IsInvalid(err) {
		t.Errorf("expected Invalid error, got %v", err)
	}
	if !strings.Contains(err.Error(), "member keys") && !strings.Contains(err.Error(), "RFC-1123") {
		t.Errorf("expected CEL message mentioning member-key rule, got %v", err)
	}
}

// 7. Map-shape motivating case: two members with the *same* GVK but distinct
// selectors. The registry must hold a single informer (one GVK), and each
// member should converge to its own rollup with the expected resource count.
func TestEnvtest_TwoMembersSameGVK_DistinctSelectors(t *testing.T) {
	fix := newEnvFixture(t)

	// Two widgets, distinct labels.
	wA := newWidget(fix.namespace, "alpha", statusTrue)
	wA.SetLabels(map[string]string{labelWave: "a"})
	if err := envtestClient.Create(t.Context(), wA); err != nil {
		t.Fatalf("create widget alpha: %v", err)
	}
	_ = unstructured.SetNestedField(wA.Object, int64(1), schemaPropStatus, "observedGeneration")
	_ = unstructured.SetNestedSlice(wA.Object, []any{
		map[string]any{keyType: apiv1.ConditionReady, schemaPropStatus: statusTrue, keyReason: testReason},
	}, schemaPropStatus, "conditions")
	if err := envtestClient.Status().Update(t.Context(), wA); err != nil {
		t.Fatalf("status alpha: %v", err)
	}

	wB := newWidget(fix.namespace, "beta", statusTrue)
	wB.SetLabels(map[string]string{labelWave: "b"})
	if err := envtestClient.Create(t.Context(), wB); err != nil {
		t.Fatalf("create widget beta: %v", err)
	}
	_ = unstructured.SetNestedField(wB.Object, int64(1), schemaPropStatus, "observedGeneration")
	_ = unstructured.SetNestedSlice(wB.Object, []any{
		map[string]any{keyType: apiv1.ConditionReady, schemaPropStatus: statusTrue, keyReason: testReason},
	}, schemaPropStatus, "conditions")
	if err := envtestClient.Status().Update(t.Context(), wB); err != nil {
		t.Fatalf("status beta: %v", err)
	}

	ech := createEchelon(t, fix.namespace, "shared-gvk", map[string]apiv1.MemberSpec{
		memberWaveA: {Group: groupTestAsCode, Kind: kindWidget,
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{labelWave: "a"}},
			EmptySetPolicy: apiv1.EmptySetUnknown},
		memberWaveB: {Group: groupTestAsCode, Kind: kindWidget,
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{labelWave: "b"}},
			EmptySetPolicy: apiv1.EmptySetUnknown},
	})

	for range 3 {
		_, _ = fix.reconciler.ReconcileObject(t.Context(), refresh(t, ech))
	}
	time.Sleep(informerWarmup)

	reconcileToConvergence(t, fix, client.ObjectKeyFromObject(ech), func(e *apiv1.Echelon) error {
		if ready(e) != metav1.ConditionTrue {
			return fmt.Errorf("Ready=%s", ready(e))
		}
		if len(e.Status.Members) != 2 {
			return fmt.Errorf("Members len=%d", len(e.Status.Members))
		}
		for _, name := range []string{"wave-a", "wave-b"} {
			rollup, ok := e.Status.Members[name]
			if !ok {
				return fmt.Errorf("Members[%q] missing", name)
			}
			if rollup.Summary.Total != 1 || rollup.Summary.Current != 1 {
				return fmt.Errorf("Members[%q].Summary = %+v", name, rollup.Summary)
			}
		}
		return nil
	})

	// Exactly one informer for the Widget GVK, shared across the two members.
	if got := fix.registry.GVKCount(); got != 1 {
		t.Errorf("GVKCount = %d, want 1 (one informer per GVK regardless of member count)", got)
	}
}

// refresh re-fetches the live Echelon by name.
func refresh(t *testing.T, ech *apiv1.Echelon) *apiv1.Echelon {
	t.Helper()
	out := &apiv1.Echelon{}
	if err := envtestClient.Get(t.Context(), client.ObjectKeyFromObject(ech), out); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	return out
}
