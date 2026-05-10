/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller_test

import (
	"context"
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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
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
	reconciler *controller.Reconciler
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
	resolver := discovery.NewResolver(dc, time.Hour)

	dyn, err := dynamic.NewForConfig(envtestCfg)
	if err != nil {
		t.Fatalf("dynamic client: %v", err)
	}
	cachedDC := memorydiscovery.NewMemCacheClient(dc)
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(cachedDC)
	dynFactory := watcher.NewDynamicFactory(dyn, mapper, time.Minute)

	registry := watcher.NewRegistry(dynFactory, func(watcher.OwnerKey) {})

	rec := &controller.Reconciler{
		Client:     envtestClient,
		Registry:   registry,
		Resolver:   resolver,
		NewAdapter: controller.NewEchelonAdapter,
		Controller: "Echelon",
	}
	return &envFixture{t: t, registry: registry, resolver: resolver, reconciler: rec, mapper: mapper, namespace: ns}
}

func createNamespace(t *testing.T, name string) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := envtestClient.Create(context.Background(), ns); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %s: %v", name, err)
	}
}

// createEchelon creates an Echelon in ns and returns the live object.
func createEchelon(t *testing.T, ns, name string, targets []apiv1.TargetSpec) *apiv1.Echelon {
	t.Helper()
	ech := &apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       apiv1.EchelonSpec{Targets: targets},
	}
	if err := envtestClient.Create(context.Background(), ech); err != nil {
		t.Fatalf("create echelon: %v", err)
	}
	return ech
}

// createWidget creates a Widget in ns. ready is one of "True", "False", "" (no
// status — kstatus reports Current).
func createWidget(t *testing.T, ns, name, ready string) *unstructured.Unstructured {
	t.Helper()
	w := newWidget(ns, name, "")
	if err := envtestClient.Create(context.Background(), w); err != nil {
		t.Fatalf("create widget: %v", err)
	}
	if ready != "" {
		_ = unstructured.SetNestedField(w.Object, int64(1), "status", "observedGeneration")
		_ = unstructured.SetNestedSlice(w.Object, []any{
			map[string]any{"type": "Ready", "status": ready, "reason": "Test"},
		}, "status", "conditions")
		if err := envtestClient.Status().Update(context.Background(), w); err != nil {
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
		if err := envtestClient.Get(context.Background(), key, ech); err != nil {
			t.Fatalf("get echelon: %v", err)
		}
		if _, err := fix.reconciler.ReconcileObject(context.Background(), ech); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		// Re-fetch to read current status.
		_ = envtestClient.Get(context.Background(), key, ech)
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
	createWidget(t, fix.namespace, "w1", "True")

	ech := createEchelon(t, fix.namespace, "happy", []apiv1.TargetSpec{
		{Group: "test.as-code.io", Kind: "Widget", EmptySetPolicy: apiv1.EmptySetUnknown},
	})

	// First reconcile adds the finalizer; second reconcile starts the informer.
	// Informers need a moment to sync; we then loop until Ready=True or timeout.
	for i := 0; i < 3; i++ {
		_, err := fix.reconciler.ReconcileObject(context.Background(), refresh(t, ech))
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
	ech := createEchelon(t, fix.namespace, "empty", []apiv1.TargetSpec{
		{Group: "test.as-code.io", Kind: "Widget", EmptySetPolicy: apiv1.EmptySetNotReady},
	})

	for i := 0; i < 3; i++ {
		_, _ = fix.reconciler.ReconcileObject(context.Background(), refresh(t, ech))
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

	ech := createEchelon(t, fix.namespace, "late", []apiv1.TargetSpec{
		{Group: "late.test.as-code.io", Kind: "Late", EmptySetPolicy: apiv1.EmptySetUnknown},
	})

	// Initial reconcile: should set Stalled=True.
	_, _ = fix.reconciler.ReconcileObject(context.Background(), refresh(t, ech))
	_, _ = fix.reconciler.ReconcileObject(context.Background(), refresh(t, ech)) // post-finalizer

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
				Plural: "lates", Singular: "late", Kind: "Late", ListKind: "LateList",
			},
			Scope: apiextv1.NamespaceScoped,
			Versions: []apiextv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Schema: &apiextv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptrBool(true)},
				},
			}},
		},
	}
	if err := envtestClient.Create(context.Background(), lateCRD); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create late CRD: %v", err)
	}
	if err := waitForCRDEstablished(envtestClient, "lates.late.test.as-code.io", 10*time.Second); err != nil {
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

// 4. Subscription diff: removing a target from spec.targets[] tears the
// informer down (registry GVKCount drops back).
func TestEnvtest_SubscriptionDiff_RemovesInformer(t *testing.T) {
	fix := newEnvFixture(t)

	ech := createEchelon(t, fix.namespace, "diff", []apiv1.TargetSpec{
		{Group: "test.as-code.io", Kind: "Widget", EmptySetPolicy: apiv1.EmptySetUnknown},
	})
	for i := 0; i < 3; i++ {
		_, _ = fix.reconciler.ReconcileObject(context.Background(), refresh(t, ech))
	}
	if got := fix.registry.GVKCount(); got != 1 {
		t.Fatalf("after subscribe GVKCount=%d, want 1", got)
	}

	// Edit spec to remove all targets requires MinItems=1 in CRD validation,
	// so we instead delete the Echelon, which triggers the finalizer cleanup.
	if err := envtestClient.Delete(context.Background(), refresh(t, ech)); err != nil {
		t.Fatalf("delete echelon: %v", err)
	}
	for i := 0; i < 3; i++ {
		curr := &apiv1.Echelon{}
		if err := envtestClient.Get(context.Background(), client.ObjectKeyFromObject(ech), curr); err != nil {
			break // gone
		}
		if _, err := fix.reconciler.ReconcileObject(context.Background(), curr); err != nil {
			t.Fatalf("post-delete reconcile: %v", err)
		}
	}
	if got := fix.registry.GVKCount(); got != 0 {
		t.Fatalf("after delete GVKCount=%d, want 0", got)
	}
}

// refresh re-fetches the live Echelon by name.
func refresh(t *testing.T, ech *apiv1.Echelon) *apiv1.Echelon {
	t.Helper()
	out := &apiv1.Echelon{}
	if err := envtestClient.Get(context.Background(), client.ObjectKeyFromObject(ech), out); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	return out
}
