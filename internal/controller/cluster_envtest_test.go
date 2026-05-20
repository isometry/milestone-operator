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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/controller"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/watcher"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clientdiscovery "k8s.io/client-go/discovery"
	memorydiscovery "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// clusterEnvFixture wires a real Reconciler against the envtest apiserver for
// the cluster-scoped CRD. Namespace and ClusterMilestone lifetimes are caller
// owned: each test creates namespaces with t.Name-derived prefixes and registers
// t.Cleanup to delete the ClusterMilestone (which drives finalizer release).
type clusterEnvFixture struct {
	t          *testing.T
	registry   *watcher.Registry
	resolver   discovery.Resolver
	reconciler *controller.Reconciler[*apiv1.ClusterMilestone]
	mapper     *restmapper.DeferredDiscoveryRESTMapper
}

func newClusterEnvFixture(t *testing.T) *clusterEnvFixture {
	t.Helper()
	requiresEnvtest(t)

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

	rec := &controller.Reconciler[*apiv1.ClusterMilestone]{
		Client:     envtestClient,
		Registry:   registry,
		Resolver:   resolver,
		NewAdapter: controller.NewClusterMilestoneAdapterFactory(envtestClient),
		Controller: kindClusterMilestone,
	}
	return &clusterEnvFixture{t: t, registry: registry, resolver: resolver, reconciler: rec, mapper: mapper}
}

// nsName derives a DNS-1123-safe namespace name from t.Name() and a suffix.
// The full name must be <= 63 chars and may not start or end with '-'; the
// prefix is hashed to a short stable suffix when the raw name would exceed
// that cap.
func nsName(t *testing.T, suffix string) string {
	t.Helper()
	base := strings.ReplaceAll(strings.ToLower(t.Name()), "_", "-")
	// Reserve room for "-<suffix>" plus a 9-char hash hop ("-xxxxxxxx").
	const tagLen = 9
	maxBase := 63 - len(suffix) - 1 - tagLen
	if maxBase < 1 {
		maxBase = 1
	}
	if len(base) > maxBase {
		sum := sha256.Sum256([]byte(base))
		base = base[:maxBase] + "-" + hex.EncodeToString(sum[:4])
	}
	full := base + "-" + suffix
	return strings.Trim(full, "-")
}

// createLabelledNamespace creates ns with labels.
func createLabelledNamespace(t *testing.T, name string, labels map[string]string) {
	t.Helper()
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
	}
	if err := envtestClient.Create(context.Background(), ns); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %s: %v", name, err)
	}
}

// labelNamespace adds/overwrites labels on an existing namespace.
func labelNamespace(t *testing.T, name string, labels map[string]string) {
	t.Helper()
	ns := &corev1.Namespace{}
	if err := envtestClient.Get(context.Background(), client.ObjectKey{Name: name}, ns); err != nil {
		t.Fatalf("get namespace %s: %v", name, err)
	}
	if ns.Labels == nil {
		ns.Labels = map[string]string{}
	}
	for k, v := range labels {
		ns.Labels[k] = v
	}
	if err := envtestClient.Update(context.Background(), ns); err != nil {
		t.Fatalf("label namespace %s: %v", name, err)
	}
}

// createClusterMilestone creates the cluster-scoped owner and registers a
// Cleanup that drives the finalizer to completion so the next test's registry
// doesn't see leaked subscriptions.
func createClusterMilestone(t *testing.T, fix *clusterEnvFixture, name string, deps []apiv1.ClusterDependencyRef) {
	t.Helper()
	cm := &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       apiv1.ClusterMilestoneSpec{DependsOn: deps},
	}
	if err := envtestClient.Create(context.Background(), cm); err != nil {
		t.Fatalf("create clustermilestone: %v", err)
	}
	t.Cleanup(func() {
		curr := &apiv1.ClusterMilestone{}
		if err := envtestClient.Get(context.Background(), client.ObjectKey{Name: name}, curr); err != nil {
			return
		}
		_ = envtestClient.Delete(context.Background(), curr)
		// Drive the finalizer.
		for range 5 {
			c := &apiv1.ClusterMilestone{}
			if err := envtestClient.Get(context.Background(), client.ObjectKey{Name: name}, c); err != nil {
				return
			}
			_, _ = fix.reconciler.ReconcileObject(context.Background(), c)
		}
	})
}

func refreshClusterMilestone(t *testing.T, name string) *apiv1.ClusterMilestone {
	t.Helper()
	out := &apiv1.ClusterMilestone{}
	if err := envtestClient.Get(context.Background(), client.ObjectKey{Name: name}, out); err != nil {
		t.Fatalf("refresh clustermilestone: %v", err)
	}
	return out
}

func clusterReady(cm *apiv1.ClusterMilestone) metav1.ConditionStatus {
	for _, c := range cm.Status.Conditions {
		if c.Type == apiv1.ConditionReady {
			return c.Status
		}
	}
	return ""
}

// reconcileClusterToConvergence reconciles repeatedly until predicate returns
// nil or the deadline expires.
func reconcileClusterToConvergence(t *testing.T, fix *clusterEnvFixture, name string, predicate func(*apiv1.ClusterMilestone) error) {
	t.Helper()
	deadline := time.Now().Add(convergenceMaxDur)
	for time.Now().Before(deadline) {
		cm := refreshClusterMilestone(t, name)
		if _, err := fix.reconciler.ReconcileObject(context.Background(), cm); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		cm = refreshClusterMilestone(t, name)
		if err := predicate(cm); err == nil {
			return
		}
		time.Sleep(convergencePoll)
	}
	t.Fatalf("did not converge within %s", convergenceMaxDur)
}

// testScopeLabel keys the per-test widget filter; envtest has no namespace
// controller, so widgets from prior tests linger and would inflate cross-test
// rollups without this. Tests label their widgets with this key and a unique
// value derived from t.Name(); the ClusterMilestone target.selector picks the
// same value so only this test's widgets are admitted.
const testScopeLabel = "milestone-test/scope"

func testScope(t *testing.T) string {
	t.Helper()
	sum := sha256.Sum256([]byte(t.Name()))
	return hex.EncodeToString(sum[:6])
}

// createWidgetInNS creates a Ready=True Widget in ns and labels it with the
// per-test scope so cross-test pollution doesn't inflate other rollups.
func createWidgetInNS(t *testing.T, ns, name string) {
	t.Helper()
	w := newWidget(ns, name, "")
	w.SetLabels(map[string]string{testScopeLabel: testScope(t)})
	if err := envtestClient.Create(context.Background(), w); err != nil {
		t.Fatalf("create widget %s/%s: %v", ns, name, err)
	}
	_ = unstructured.SetNestedField(w.Object, int64(1), schemaPropStatus, "observedGeneration")
	_ = unstructured.SetNestedSlice(w.Object, []any{
		map[string]any{keyType: apiv1.ConditionReady, schemaPropStatus: statusTrue, keyReason: testReason},
	}, schemaPropStatus, "conditions")
	if err := envtestClient.Status().Update(context.Background(), w); err != nil {
		t.Fatalf("status widget %s/%s: %v", ns, name, err)
	}
}

// scopedSelector returns a LabelSelector that admits only this test's widgets.
func scopedSelector(t *testing.T) *metav1.LabelSelector {
	t.Helper()
	return &metav1.LabelSelector{MatchLabels: map[string]string{testScopeLabel: testScope(t)}}
}

// TestEnvtest_ClusterMilestone_NamespaceSelector_Converges asserts that a
// ClusterMilestone scoped by namespaceSelector counts only widgets in
// matching namespaces and reaches Ready=True.
func TestEnvtest_ClusterMilestone_NamespaceSelector_Converges(t *testing.T) {
	fix := newClusterEnvFixture(t)

	matchA := nsName(t, "match-a")
	matchB := nsName(t, "match-b")
	skipA := nsName(t, "skip-a")
	skipB := nsName(t, "skip-b")
	createLabelledNamespace(t, matchA, map[string]string{labelTier: namePlatform})
	createLabelledNamespace(t, matchB, map[string]string{labelTier: namePlatform})
	createLabelledNamespace(t, skipA, nil)
	createLabelledNamespace(t, skipB, nil)

	createWidgetInNS(t, matchA, "w1")
	createWidgetInNS(t, matchB, "w2")
	createWidgetInNS(t, skipA, "w3")
	createWidgetInNS(t, skipB, "w4")

	cmName := nsName(t, "owner")
	createClusterMilestone(t, fix, cmName, []apiv1.ClusterDependencyRef{{
		Name:           widgetPlural,
		EmptySetPolicy: apiv1.EmptySetUnknown,
		Target: apiv1.ClusterTargetSpec{
			TargetSpec: apiv1.TargetSpec{
				Group:    groupTestAsCode,
				Kind:     kindWidget,
				Selector: scopedSelector(t),
			},
			NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelTier: namePlatform}},
		},
	}})

	// Prime: first reconcile adds the finalizer, second starts the informer.
	for i := range 3 {
		_, err := fix.reconciler.ReconcileObject(context.Background(), refreshClusterMilestone(t, cmName))
		if err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}

	reconcileClusterToConvergence(t, fix, cmName, func(cm *apiv1.ClusterMilestone) error {
		if clusterReady(cm) != metav1.ConditionTrue {
			return fmt.Errorf("Ready=%s", clusterReady(cm))
		}
		if cm.Status.Summary.Total != 2 {
			return fmt.Errorf("Summary.Total=%d, want 2", cm.Status.Summary.Total)
		}
		if cm.Status.Summary.Current != 2 {
			return fmt.Errorf("Summary.Current=%d, want 2", cm.Status.Summary.Current)
		}
		return nil
	})
}

// TestEnvtest_ClusterMilestone_NamespaceAdded_WakesOwner asserts that
// labelling a previously-unlabelled namespace into the selector grows
// Summary.Total on the next reconcile.
func TestEnvtest_ClusterMilestone_NamespaceAdded_WakesOwner(t *testing.T) {
	fix := newClusterEnvFixture(t)

	nsA := nsName(t, "a")
	nsB := nsName(t, "b")
	createLabelledNamespace(t, nsA, map[string]string{labelTier: namePlatform})
	createLabelledNamespace(t, nsB, nil)

	createWidgetInNS(t, nsA, "w1")
	createWidgetInNS(t, nsB, "w2")

	cmName := nsName(t, "owner")
	createClusterMilestone(t, fix, cmName, []apiv1.ClusterDependencyRef{{
		Name:           widgetPlural,
		EmptySetPolicy: apiv1.EmptySetUnknown,
		Target: apiv1.ClusterTargetSpec{
			TargetSpec: apiv1.TargetSpec{
				Group:    groupTestAsCode,
				Kind:     kindWidget,
				Selector: scopedSelector(t),
			},
			NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelTier: namePlatform}},
		},
	}})

	for i := range 3 {
		_, err := fix.reconciler.ReconcileObject(context.Background(), refreshClusterMilestone(t, cmName))
		if err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}

	// Pre-condition: only nsA's widget is counted.
	reconcileClusterToConvergence(t, fix, cmName, func(cm *apiv1.ClusterMilestone) error {
		if clusterReady(cm) != metav1.ConditionTrue {
			return fmt.Errorf("pre Ready=%s", clusterReady(cm))
		}
		if cm.Status.Summary.Total != 1 {
			return fmt.Errorf("pre Summary.Total=%d, want 1", cm.Status.Summary.Total)
		}
		return nil
	})
	// Label nsB into the selector.
	labelNamespace(t, nsB, map[string]string{labelTier: namePlatform})

	// Re-converge: the next reconcile must re-list namespaces and pick up
	// the newly-matching one. (Production wiring wakes the owner via the
	// namespaceToClusterMilestones mapper on the Namespace watch; this
	// envtest exercises the reconciler's namespace re-evaluation, which is
	// the post-wake code path.)
	reconcileClusterToConvergence(t, fix, cmName, func(cm *apiv1.ClusterMilestone) error {
		if cm.Status.Summary.Total != 2 {
			return fmt.Errorf("Summary.Total=%d, want 2", cm.Status.Summary.Total)
		}
		return nil
	})
}

// TestEnvtest_ClusterMilestone_NamespacesAndSelector_RejectedByCEL asserts the
// CRD-level CEL XOR rule on ClusterTargetSpec rejects a dependency that sets
// both Namespaces and NamespaceSelector.
func TestEnvtest_ClusterMilestone_NamespacesAndSelector_RejectedByCEL(t *testing.T) {
	requiresEnvtest(t)

	cm := &apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: nsName(t, "bad")},
		Spec: apiv1.ClusterMilestoneSpec{
			DependsOn: []apiv1.ClusterDependencyRef{{
				Name: widgetPlural,
				Target: apiv1.ClusterTargetSpec{
					TargetSpec:        apiv1.TargetSpec{Group: groupTestAsCode, Kind: kindWidget},
					Namespaces:        []string{"a"},
					NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{labelTier: namePlatform}},
				},
			}},
		},
	}
	err := envtestClient.Create(context.Background(), cm)
	if err == nil {
		_ = envtestClient.Delete(context.Background(), cm)
		t.Fatalf("expected admission rejection on namespaces+namespaceSelector; got nil")
	}
	if !apierrors.IsInvalid(err) {
		t.Errorf("expected Invalid error, got %v", err)
	}
	if !strings.Contains(err.Error(), "namespaceSelector are mutually exclusive") {
		t.Errorf("expected CEL message about mutual exclusion; got %v", err)
	}
}
