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

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/controller"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/watcher"
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
)

// envFixture wires a real Reconciler against the envtest apiserver. Each
// fixture creates its own namespace so tests don't leak Widget objects
// across each other.
type envFixture struct {
	t          *testing.T
	registry   *watcher.Registry
	resolver   discovery.Resolver
	reconciler *controller.Reconciler[*apiv1.Milestone]
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

	rec := &controller.Reconciler[*apiv1.Milestone]{
		Client:     envtestClient,
		Registry:   registry,
		Resolver:   resolver,
		NewAdapter: controller.NewMilestoneAdapter,
		Controller: kindMilestone,
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

// createMilestone creates a Milestone in ns and returns the live object.
func createMilestone(t *testing.T, ns, name string, deps []apiv1.DependencyRef) *apiv1.Milestone {
	t.Helper()
	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       apiv1.MilestoneSpec{DependsOn: deps},
	}
	if err := envtestClient.Create(t.Context(), m); err != nil {
		t.Fatalf("create milestone: %v", err)
	}
	return m
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
func reconcileToConvergence(t *testing.T, fix *envFixture, key client.ObjectKey, predicate func(*apiv1.Milestone) error) {
	t.Helper()
	deadline := time.Now().Add(convergenceMaxDur)
	for time.Now().Before(deadline) {
		m := &apiv1.Milestone{}
		if err := envtestClient.Get(t.Context(), key, m); err != nil {
			t.Fatalf("get milestone: %v", err)
		}
		if _, err := fix.reconciler.ReconcileObject(t.Context(), m); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		// Re-fetch to read current status.
		_ = envtestClient.Get(t.Context(), key, m)
		if err := predicate(m); err == nil {
			return
		}
		time.Sleep(convergencePoll)
	}
	t.Fatalf("did not converge within %s", convergenceMaxDur)
}

func ready(m *apiv1.Milestone) metav1.ConditionStatus {
	for _, c := range m.Status.Conditions {
		if c.Type == apiv1.ConditionReady {
			return c.Status
		}
	}
	return ""
}

func stalled(m *apiv1.Milestone) metav1.ConditionStatus {
	for _, c := range m.Status.Conditions {
		if c.Type == apiv1.ConditionStalled {
			return c.Status
		}
	}
	return ""
}

func depStatusByName(m *apiv1.Milestone, name string) (apiv1.DependencyStatus, bool) {
	for i := range m.Status.DependsOn {
		if m.Status.DependsOn[i].Name == name {
			return m.Status.DependsOn[i], true
		}
	}
	return apiv1.DependencyStatus{}, false
}

// 1. Happy path: a Milestone with one all-Current Widget dependency
// converges to Ready=True.
func TestEnvtest_HappyPath_AllCurrent(t *testing.T) {
	fix := newEnvFixture(t)
	createWidget(t, fix.namespace, "w1", statusTrue)

	m := createMilestone(t, fix.namespace, "happy", []apiv1.DependencyRef{{
		Name:           widgetPlural,
		EmptySetPolicy: apiv1.EmptySetUnknown,
		Target:         apiv1.TargetSpec{Group: groupTestAsCode, Kind: kindWidget},
	}})

	// First reconcile adds the finalizer; second reconcile starts the informer.
	// Informers need a moment to sync; we then loop until Ready=True or timeout.
	for i := range 3 {
		_, err := fix.reconciler.ReconcileObject(t.Context(), refresh(t, m))
		if err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	reconcileToConvergence(t, fix, client.ObjectKeyFromObject(m), func(e *apiv1.Milestone) error {
		if ready(e) != metav1.ConditionTrue {
			return fmt.Errorf("Ready=%s", ready(e))
		}
		return nil
	})
}

// 2. Empty selector with NotReady policy yields Ready=False.
func TestEnvtest_EmptySet_NotReadyPolicy(t *testing.T) {
	fix := newEnvFixture(t)
	m := createMilestone(t, fix.namespace, "empty", []apiv1.DependencyRef{{
		Name:           widgetPlural,
		EmptySetPolicy: apiv1.EmptySetNotReady,
		Target:         apiv1.TargetSpec{Group: groupTestAsCode, Kind: kindWidget},
	}})

	for range 3 {
		_, _ = fix.reconciler.ReconcileObject(t.Context(), refresh(t, m))
	}
	reconcileToConvergence(t, fix, client.ObjectKeyFromObject(m), func(e *apiv1.Milestone) error {
		if ready(e) != metav1.ConditionFalse {
			return fmt.Errorf("Ready=%s", ready(e))
		}
		return nil
	})
}

// 3. Late CRD: a Milestone referencing an unknown kind starts Stalled, then
// converges after the CRD is installed.
func TestEnvtest_LateCRD_StalledThenConverges(t *testing.T) {
	fix := newEnvFixture(t)

	m := createMilestone(t, fix.namespace, depLate, []apiv1.DependencyRef{{
		Name:           "lates",
		EmptySetPolicy: apiv1.EmptySetUnknown,
		Target:         apiv1.TargetSpec{Group: "late.test.milestone.as-code.io", Kind: kindLate},
	}})

	// Initial reconcile: should set Stalled=True.
	_, _ = fix.reconciler.ReconcileObject(t.Context(), refresh(t, m))
	_, _ = fix.reconciler.ReconcileObject(t.Context(), refresh(t, m)) // post-finalizer

	got := refresh(t, m)
	if stalled(got) != metav1.ConditionTrue {
		t.Fatalf("Stalled=%s, want True before CRD install; conds=%+v", stalled(got), got.Status.Conditions)
	}

	// Install the late CRD.
	lateCRD := &apiextv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "lates.late.test.milestone.as-code.io"},
		Spec: apiextv1.CustomResourceDefinitionSpec{
			Group: "late.test.milestone.as-code.io",
			Names: apiextv1.CustomResourceDefinitionNames{
				Plural: "lates", Singular: depLate, Kind: kindLate, ListKind: "LateList",
			},
			Scope: apiextv1.NamespaceScoped,
			Versions: []apiextv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Schema: &apiextv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextv1.JSONSchemaProps{Type: schemaTypeObject, XPreserveUnknownFields: new(true)},
				},
			}},
		},
	}
	if err := envtestClient.Create(t.Context(), lateCRD); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create late CRD: %v", err)
	}
	if err := waitForCRDEstablished(t.Context(), envtestClient, "lates.late.test.milestone.as-code.io", 10*time.Second); err != nil {
		t.Fatalf("wait CRD established: %v", err)
	}

	// Invalidate both caches (production CRD watcher does the resolver invalidate;
	// the manager's RESTMapper invalidates via its own watcher).
	fix.resolver.Invalidate()
	fix.mapper.Reset()

	reconcileToConvergence(t, fix, client.ObjectKeyFromObject(m), func(e *apiv1.Milestone) error {
		if stalled(e) == metav1.ConditionTrue {
			return errors.New("still stalled")
		}
		return nil
	})
}

// 4. Subscription diff: deleting a Milestone tears the informer down
// (registry GVKCount drops back to zero via the finalizer cleanup).
func TestEnvtest_SubscriptionDiff_RemovesInformer(t *testing.T) {
	fix := newEnvFixture(t)

	m := createMilestone(t, fix.namespace, "diff", []apiv1.DependencyRef{{
		Name:           widgetPlural,
		EmptySetPolicy: apiv1.EmptySetUnknown,
		Target:         apiv1.TargetSpec{Group: groupTestAsCode, Kind: kindWidget},
	}})
	for range 3 {
		_, _ = fix.reconciler.ReconcileObject(t.Context(), refresh(t, m))
	}
	if got := fix.registry.GVKCount(); got != 1 {
		t.Fatalf("after subscribe GVKCount=%d, want 1", got)
	}

	// Removing all dependencies requires MinItems=1 in CRD validation, so we
	// instead delete the Milestone, which triggers the finalizer cleanup.
	if err := envtestClient.Delete(t.Context(), refresh(t, m)); err != nil {
		t.Fatalf("delete milestone: %v", err)
	}
	for range 3 {
		curr := &apiv1.Milestone{}
		if err := envtestClient.Get(t.Context(), client.ObjectKeyFromObject(m), curr); err != nil {
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

// 5. CRD-level validation: an empty dependsOn list must be rejected on Create.
func TestEnvtest_EmptyDependsOn_RejectedByCRD(t *testing.T) {
	fix := newEnvFixture(t)

	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: fix.namespace, Name: "empty-list"},
		Spec:       apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{}},
	}
	err := envtestClient.Create(t.Context(), m)
	if err == nil {
		t.Fatalf("expected validation error for empty dependsOn list; got nil")
	}
	if !apierrors.IsInvalid(err) && !strings.Contains(err.Error(), "minItems") && !strings.Contains(err.Error(), "Invalid") {
		t.Errorf("expected Invalid/minItems error, got %v", err)
	}
}

// 6. CRD-level validation: a dependency name that violates the RFC-1123
// label regex must be rejected by the field-level pattern marker.
func TestEnvtest_InvalidDependencyName_RejectedByCRD(t *testing.T) {
	fix := newEnvFixture(t)

	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: fix.namespace, Name: "bad-name"},
		Spec: apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{{
			Name:   "BadName_WithCaps",
			Target: apiv1.TargetSpec{Group: groupTestAsCode, Kind: kindWidget},
		}}},
	}
	err := envtestClient.Create(t.Context(), m)
	if err == nil {
		t.Fatalf("expected validation error for invalid name; got nil")
	}
	if !apierrors.IsInvalid(err) {
		t.Errorf("expected Invalid error, got %v", err)
	}
}

// 7. Two dependencies with the *same* GVK but distinct selectors. The
// registry must hold a single informer (one GVK), and each dependency
// should converge to its own rollup with the expected resource count.
func TestEnvtest_TwoDependenciesSameGVK_DistinctSelectors(t *testing.T) {
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

	m := createMilestone(t, fix.namespace, "shared-gvk", []apiv1.DependencyRef{
		{
			Name:           depWaveA,
			EmptySetPolicy: apiv1.EmptySetUnknown,
			Target: apiv1.TargetSpec{
				Group: groupTestAsCode, Kind: kindWidget,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{labelWave: "a"}},
			},
		},
		{
			Name:           depWaveB,
			EmptySetPolicy: apiv1.EmptySetUnknown,
			Target: apiv1.TargetSpec{
				Group: groupTestAsCode, Kind: kindWidget,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{labelWave: "b"}},
			},
		},
	})

	for range 3 {
		_, _ = fix.reconciler.ReconcileObject(t.Context(), refresh(t, m))
	}
	reconcileToConvergence(t, fix, client.ObjectKeyFromObject(m), func(e *apiv1.Milestone) error {
		if ready(e) != metav1.ConditionTrue {
			return fmt.Errorf("Ready=%s", ready(e))
		}
		if len(e.Status.DependsOn) != 2 {
			return fmt.Errorf("DependsOn len=%d", len(e.Status.DependsOn))
		}
		for _, name := range []string{depWaveA, depWaveB} {
			rollup, ok := depStatusByName(e, name)
			if !ok {
				return fmt.Errorf("DependsOn[%q] missing", name)
			}
			if rollup.Summary.Total != 1 || rollup.Summary.Current != 1 {
				return fmt.Errorf("DependsOn[%q].Summary = %+v", name, rollup.Summary)
			}
		}
		return nil
	})

	// Exactly one informer for the Widget GVK, shared across the two dependencies.
	if got := fix.registry.GVKCount(); got != 1 {
		t.Errorf("GVKCount = %d, want 1 (one informer per GVK regardless of dependency count)", got)
	}
}

// refresh re-fetches the live Milestone by name.
func refresh(t *testing.T, m *apiv1.Milestone) *apiv1.Milestone {
	t.Helper()
	out := &apiv1.Milestone{}
	if err := envtestClient.Get(t.Context(), client.ObjectKeyFromObject(m), out); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	return out
}
