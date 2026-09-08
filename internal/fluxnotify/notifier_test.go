/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package fluxnotify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/isometry/milestone-operator/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	fixedTimestamp  = "2026-05-12T10:00:00Z"
	fluxSystemNS    = "flux-system"
	appsNS          = "apps"
	paymentsName    = "payments"
	monitoringName  = "monitoring"
	observabilityNS = "observability"

	kustomizationsResource = "kustomizations"
)

func fixedNow() time.Time {
	t, _ := time.Parse(time.RFC3339, fixedTimestamp)
	return t
}

// recordedPatch captures the parameters of a Patch call so assertions can
// be made on the GVK, namespace/name, and JSON body sent.
type recordedPatch struct {
	gvk       schema.GroupVersionKind
	namespace string
	name      string
	body      []byte
}

// parentState describes what the intercepted GET returns for a parent of
// the given kind: an error, or an object whose spec.suspend is true, false
// or (suspend == nil) absent.
type parentState struct {
	err     error
	suspend *bool
}

func boolPtr(b bool) *bool { return &b }

// recordingClient wraps a fake controller-runtime client and records every
// Patch call. An optional injected error is returned in place of the
// underlying patch result. The patch is never forwarded to the wrapped
// store: we only care about what the notifier sent. The pre-patch GET
// resolves to an unsuspended parent.
func recordingClient(t *testing.T, injectErr error) (client.Client, *[]recordedPatch) {
	t.Helper()
	c, calls, _ := recordingClientWithParents(t, injectErr, nil)
	return c, calls
}

// recordingClientWithParents additionally stubs the pre-patch GET per
// parent kind and records the keys it was called with as
// "Kind/namespace/name".
func recordingClientWithParents(t *testing.T, injectErr error, parents map[string]parentState) (client.Client, *[]recordedPatch, *[]string) {
	t.Helper()
	var calls []recordedPatch
	var gets []string
	c := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
			kind := obj.GetObjectKind().GroupVersionKind().Kind
			gets = append(gets, fmt.Sprintf("%s/%s/%s", kind, key.Namespace, key.Name))
			state := parents[kind]
			if state.err != nil {
				return state.err
			}
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("GET target = %T, want *unstructured.Unstructured", obj)
			}
			u.SetNamespace(key.Namespace)
			u.SetName(key.Name)
			if state.suspend != nil {
				if err := unstructured.SetNestedField(u.Object, *state.suspend, "spec", "suspend"); err != nil {
					t.Fatalf("SetNestedField: %v", err)
				}
			}
			return nil
		},
		Patch: func(_ context.Context, _ client.WithWatch, obj client.Object, patch client.Patch, _ ...client.PatchOption) error {
			body, _ := patch.Data(obj)
			calls = append(calls, recordedPatch{
				gvk:       obj.GetObjectKind().GroupVersionKind(),
				namespace: obj.GetNamespace(),
				name:      obj.GetName(),
				body:      body,
			})
			return injectErr
		},
	}).Build()
	return c, &calls, &gets
}

func newNotifier(t *testing.T, c client.Client, controllerLabel string) *Notifier {
	t.Helper()
	return &Notifier{
		Client:     c,
		Controller: controllerLabel,
		Now:        fixedNow,
		Log:        logr.Discard(),
	}
}

func milestoneWithLabels(labels map[string]string) client.Object {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "milestone.as-code.io", Version: "v1", Kind: "Milestone"})
	obj.SetNamespace(appsNS)
	obj.SetName("payments-wave")
	obj.SetLabels(labels)
	return obj
}

func resetMetrics() {
	metrics.FluxNotifyTotal.Reset()
}

// assertCounter asserts a one-increment on the given result label. Every
// test path here exercises a single notify call, so "want=1" is implicit.
func assertCounter(t *testing.T, controllerLabel, parentKind, result string) {
	t.Helper()
	got := testutil.ToFloat64(metrics.FluxNotifyTotal.WithLabelValues(controllerLabel, parentKind, result))
	if got != 1 {
		t.Errorf("flux_notify_total{controller=%q,parent_kind=%q,result=%q} = %v, want 1",
			controllerLabel, parentKind, result, got)
	}
}

func TestNotifier_NoLabels_NoPatch(t *testing.T) {
	resetMetrics()
	c, calls := recordingClient(t, nil)
	n := newNotifier(t, c, "Milestone")

	n.NotifyTransition(context.Background(), milestoneWithLabels(nil))

	if len(*calls) != 0 {
		t.Errorf("expected 0 patches, got %d", len(*calls))
	}
}

func TestNotifier_NilObject_NoPanic(t *testing.T) {
	resetMetrics()
	c, calls := recordingClient(t, nil)
	n := newNotifier(t, c, "Milestone")

	n.NotifyTransition(context.Background(), nil)

	if len(*calls) != 0 {
		t.Errorf("expected 0 patches, got %d", len(*calls))
	}
}

func TestNotifier_KustomizationOnly(t *testing.T) {
	resetMetrics()
	c, calls := recordingClient(t, nil)
	n := newNotifier(t, c, "Milestone")

	n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
		labelKustomizeName:      paymentsName,
		labelKustomizeNamespace: fluxSystemNS,
	}))

	if len(*calls) != 1 {
		t.Fatalf("expected 1 patch, got %d", len(*calls))
	}
	got := (*calls)[0]
	if got.gvk != kustomizationGVK {
		t.Errorf("gvk = %v, want %v", got.gvk, kustomizationGVK)
	}
	if got.namespace != fluxSystemNS || got.name != paymentsName {
		t.Errorf("target = %s/%s, want flux-system/payments", got.namespace, got.name)
	}
	wantBody := fmt.Sprintf(`{"metadata":{"annotations":{"reconcile.fluxcd.io/requestedAt":%q}}}`, fixedTimestamp)
	if string(got.body) != wantBody {
		t.Errorf("body = %s, want %s", got.body, wantBody)
	}
	assertCounter(t, "Milestone", "Kustomization", metrics.FluxNotifySuccess)
}

func TestNotifier_HelmReleaseOnly(t *testing.T) {
	resetMetrics()
	c, calls := recordingClient(t, nil)
	n := newNotifier(t, c, "ClusterMilestone")

	n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
		labelHelmName:      monitoringName,
		labelHelmNamespace: observabilityNS,
	}))

	if len(*calls) != 1 {
		t.Fatalf("expected 1 patch, got %d", len(*calls))
	}
	got := (*calls)[0]
	if got.gvk != helmReleaseGVK {
		t.Errorf("gvk = %v, want %v", got.gvk, helmReleaseGVK)
	}
	if got.namespace != observabilityNS || got.name != monitoringName {
		t.Errorf("target = %s/%s, want observability/monitoring", got.namespace, got.name)
	}
	assertCounter(t, "ClusterMilestone", "HelmRelease", metrics.FluxNotifySuccess)
}

func TestNotifier_BothLabelPairs_PokesBoth(t *testing.T) {
	resetMetrics()
	c, calls := recordingClient(t, nil)
	n := newNotifier(t, c, "Milestone")

	n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
		labelKustomizeName:      "outer",
		labelKustomizeNamespace: fluxSystemNS,
		labelHelmName:           "inner",
		labelHelmNamespace:      appsNS,
	}))

	if len(*calls) != 2 {
		t.Fatalf("expected 2 patches, got %d", len(*calls))
	}
	kinds := map[string]bool{}
	for _, c := range *calls {
		kinds[c.gvk.Kind] = true
	}
	if !kinds["Kustomization"] || !kinds["HelmRelease"] {
		t.Errorf("patched kinds = %v, want both Kustomization and HelmRelease", kinds)
	}
	assertCounter(t, "Milestone", "Kustomization", metrics.FluxNotifySuccess)
	assertCounter(t, "Milestone", "HelmRelease", metrics.FluxNotifySuccess)
}

func TestNotifier_SuspendedParent_NoPatch(t *testing.T) {
	cases := []struct {
		name       string
		labels     map[string]string
		parentKind string
		wantGet    string
	}{
		{
			name: "kustomization",
			labels: map[string]string{
				labelKustomizeName:      paymentsName,
				labelKustomizeNamespace: fluxSystemNS,
			},
			parentKind: kindKustomization,
			wantGet:    "Kustomization/flux-system/payments",
		},
		{
			name: "helmrelease",
			labels: map[string]string{
				labelHelmName:      monitoringName,
				labelHelmNamespace: observabilityNS,
			},
			parentKind: kindHelmRelease,
			wantGet:    "HelmRelease/observability/monitoring",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMetrics()
			c, calls, gets := recordingClientWithParents(t, nil, map[string]parentState{
				tc.parentKind: {suspend: boolPtr(true)},
			})
			n := newNotifier(t, c, "Milestone")

			n.NotifyTransition(context.Background(), milestoneWithLabels(tc.labels))

			if len(*calls) != 0 {
				t.Errorf("suspended parent triggered %d patches, want 0", len(*calls))
			}
			if len(*gets) != 1 || (*gets)[0] != tc.wantGet {
				t.Errorf("gets = %v, want [%s]", *gets, tc.wantGet)
			}
			assertCounter(t, "Milestone", tc.parentKind, metrics.FluxNotifySuspended)
			if got := testutil.CollectAndCount(metrics.FluxNotifyTotal); got != 1 {
				t.Errorf("emitted %d metric series, want only the suspended one", got)
			}
		})
	}
}

func TestNotifier_UnsuspendedParent_Patches(t *testing.T) {
	cases := []struct {
		name  string
		state parentState
	}{
		{name: "suspend_false", state: parentState{suspend: boolPtr(false)}},
		{name: "suspend_absent", state: parentState{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMetrics()
			c, calls, gets := recordingClientWithParents(t, nil, map[string]parentState{
				kindKustomization: tc.state,
			})
			n := newNotifier(t, c, "Milestone")

			n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
				labelKustomizeName:      paymentsName,
				labelKustomizeNamespace: fluxSystemNS,
			}))

			if len(*gets) != 1 {
				t.Fatalf("expected 1 GET, got %d", len(*gets))
			}
			if len(*calls) != 1 {
				t.Fatalf("expected 1 patch, got %d", len(*calls))
			}
			got := (*calls)[0]
			if got.gvk != kustomizationGVK || got.namespace != fluxSystemNS || got.name != paymentsName {
				t.Errorf("patch target = %v %s/%s, want %v flux-system/payments", got.gvk, got.namespace, got.name, kustomizationGVK)
			}
			wantBody := fmt.Sprintf(`{"metadata":{"annotations":{"reconcile.fluxcd.io/requestedAt":%q}}}`, fixedTimestamp)
			if string(got.body) != wantBody {
				t.Errorf("body = %s, want %s", got.body, wantBody)
			}
			assertCounter(t, "Milestone", kindKustomization, metrics.FluxNotifySuccess)
		})
	}
}

func TestNotifier_GetFailure_NoPatch(t *testing.T) {
	gr := schema.GroupResource{Group: groupKustomize, Resource: kustomizationsResource}
	cases := []struct {
		name       string
		injected   error
		wantResult string
	}{
		{
			name:       "not_found",
			injected:   apierrors.NewNotFound(gr, paymentsName),
			wantResult: metrics.FluxNotifyNotFound,
		},
		{
			name:       "forbidden",
			injected:   apierrors.NewForbidden(gr, paymentsName, errors.New("nope")),
			wantResult: metrics.FluxNotifyForbidden,
		},
		{
			name: "no_match",
			injected: &apimeta.NoKindMatchError{
				GroupKind:        schema.GroupKind{Group: gr.Group, Kind: kindKustomization},
				SearchedVersions: []string{"v1"},
			},
			wantResult: metrics.FluxNotifyNoMatch,
		},
		{
			name:       "error",
			injected:   errors.New("generic boom"),
			wantResult: metrics.FluxNotifyError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMetrics()
			c, calls, gets := recordingClientWithParents(t, nil, map[string]parentState{
				kindKustomization: {err: tc.injected},
			})
			n := newNotifier(t, c, "Milestone")

			n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
				labelKustomizeName:      paymentsName,
				labelKustomizeNamespace: fluxSystemNS,
			}))

			if len(*gets) != 1 {
				t.Fatalf("expected 1 GET, got %d", len(*gets))
			}
			if len(*calls) != 0 {
				t.Errorf("failed GET triggered %d patches, want 0", len(*calls))
			}
			assertCounter(t, "Milestone", kindKustomization, tc.wantResult)
		})
	}
}

func TestNotifier_BothLabelPairs_OneSuspended(t *testing.T) {
	resetMetrics()
	c, calls, gets := recordingClientWithParents(t, nil, map[string]parentState{
		kindKustomization: {suspend: boolPtr(true)},
		kindHelmRelease:   {suspend: boolPtr(false)},
	})
	n := newNotifier(t, c, "Milestone")

	n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
		labelKustomizeName:      "outer",
		labelKustomizeNamespace: fluxSystemNS,
		labelHelmName:           "inner",
		labelHelmNamespace:      appsNS,
	}))

	if len(*gets) != 2 {
		t.Fatalf("expected 2 GETs, got %v", *gets)
	}
	if len(*calls) != 1 {
		t.Fatalf("expected 1 patch, got %d", len(*calls))
	}
	if (*calls)[0].gvk != helmReleaseGVK {
		t.Errorf("patched %v, want only the unsuspended %v", (*calls)[0].gvk, helmReleaseGVK)
	}
	assertCounter(t, "Milestone", kindKustomization, metrics.FluxNotifySuspended)
	assertCounter(t, "Milestone", kindHelmRelease, metrics.FluxNotifySuccess)
}

func TestNotifier_OnlyNameLabel_SkipsWhenNamespaceMissing(t *testing.T) {
	// Defensive: Flux always stamps both labels together, but if only the
	// name is present we still attempt the poke with an empty namespace and
	// must not panic. The fake client accepts that; a real apiserver would
	// 404 the pre-check GET and the poke would be skipped as not_found.
	resetMetrics()
	c, calls := recordingClient(t, nil)
	n := newNotifier(t, c, "Milestone")

	n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
		labelKustomizeName: "lone",
	}))

	if len(*calls) != 1 {
		t.Fatalf("expected 1 patch attempt, got %d", len(*calls))
	}
	if (*calls)[0].namespace != "" {
		t.Errorf("namespace = %q, want empty", (*calls)[0].namespace)
	}
}

func TestNotifier_ClassifyErrors(t *testing.T) {
	gr := schema.GroupResource{Group: groupKustomize, Resource: kustomizationsResource}
	cases := []struct {
		name       string
		injected   error
		wantResult string
	}{
		{
			name:       "not_found",
			injected:   apierrors.NewNotFound(gr, "x"),
			wantResult: metrics.FluxNotifyNotFound,
		},
		{
			name:       "forbidden",
			injected:   apierrors.NewForbidden(gr, "x", errors.New("nope")),
			wantResult: metrics.FluxNotifyForbidden,
		},
		{
			name: "no_match",
			injected: &apimeta.NoKindMatchError{
				GroupKind:        schema.GroupKind{Group: gr.Group, Kind: kindKustomization},
				SearchedVersions: []string{"v1"},
			},
			wantResult: metrics.FluxNotifyNoMatch,
		},
		{
			name:       "error",
			injected:   errors.New("generic boom"),
			wantResult: metrics.FluxNotifyError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMetrics()
			c, calls := recordingClient(t, tc.injected)
			n := newNotifier(t, c, "Milestone")

			n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
				labelKustomizeName:      "k",
				labelKustomizeNamespace: fluxSystemNS,
			}))

			if len(*calls) != 1 {
				t.Fatalf("expected 1 patch attempt, got %d", len(*calls))
			}
			assertCounter(t, "Milestone", "Kustomization", tc.wantResult)
		})
	}
}

func TestNotifier_PatchPayloadUsesMergePatch(t *testing.T) {
	resetMetrics()
	var capturedType types.PatchType
	c := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
			return nil
		},
		Patch: func(_ context.Context, _ client.WithWatch, obj client.Object, patch client.Patch, _ ...client.PatchOption) error {
			capturedType = patch.Type()
			return nil
		},
	}).Build()
	n := newNotifier(t, c, "Milestone")

	n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
		labelKustomizeName:      "k",
		labelKustomizeNamespace: fluxSystemNS,
	}))

	if capturedType != types.MergePatchType {
		t.Errorf("patch type = %q, want %q", capturedType, types.MergePatchType)
	}
}

// TestNotifier_NilReceiver_NoPanic guards the `n == nil` short-circuit in
// NotifyTransition. Production code never constructs a nil *Notifier, but
// the guard exists and must hold — a future refactor that removes it would
// surface immediately.
func TestNotifier_NilReceiver_NoPanic(t *testing.T) {
	resetMetrics()
	var n *Notifier
	// Must not panic; must not increment metrics.
	n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
		labelKustomizeName:      "k",
		labelKustomizeNamespace: fluxSystemNS,
	}))
	got := testutil.CollectAndCount(metrics.FluxNotifyTotal)
	if got != 0 {
		t.Errorf("nil receiver emitted %d metric series, want 0", got)
	}
}

// TestNotifier_NonFluxLabelsOnly_NoPatch covers the case where the object
// carries labels but none of them are Flux owner labels. The existing
// `NoLabels` test only exercises the `len(lbls) == 0` early return; this
// drives the per-label-pair `if name != ""` arms instead.
func TestNotifier_NonFluxLabelsOnly_NoPatch(t *testing.T) {
	resetMetrics()
	c, calls := recordingClient(t, nil)
	n := newNotifier(t, c, "Milestone")

	n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
		"app":                          paymentsName,
		"app.kubernetes.io/managed-by": "kubectl",
	}))

	if len(*calls) != 0 {
		t.Errorf("non-Flux labels triggered %d patches, want 0", len(*calls))
	}
	if got := testutil.CollectAndCount(metrics.FluxNotifyTotal); got != 0 {
		t.Errorf("non-Flux labels emitted %d metric series, want 0", got)
	}
}

// TestNotifier_EmptyNameLabel_NoPatch covers the empty-string-name guard.
// Flux always stamps non-empty values, but a hand-edited label set could
// produce this; the notifier must not issue a patch with an empty name.
func TestNotifier_EmptyNameLabel_NoPatch(t *testing.T) {
	resetMetrics()
	c, calls := recordingClient(t, nil)
	n := newNotifier(t, c, "Milestone")

	n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
		labelKustomizeName:      "",
		labelKustomizeNamespace: fluxSystemNS,
		labelHelmName:           "",
		labelHelmNamespace:      appsNS,
	}))

	if len(*calls) != 0 {
		t.Errorf("empty-name labels triggered %d patches, want 0", len(*calls))
	}
}

// TestNotifier_WrappedErrors_Classified ensures the classifier unwraps via
// errors.As-style helpers. controller-runtime occasionally wraps apiserver
// errors as it passes them through interceptor chains; without unwrap, a
// real `NotFound` could end up classified as the catch-all `error`.
func TestNotifier_WrappedErrors_Classified(t *testing.T) {
	gr := schema.GroupResource{Group: groupKustomize, Resource: kustomizationsResource}
	cases := []struct {
		name       string
		injected   error
		wantResult string
	}{
		{
			name:       "wrapped_not_found",
			injected:   fmt.Errorf("rpc layer: %w", apierrors.NewNotFound(gr, "x")),
			wantResult: metrics.FluxNotifyNotFound,
		},
		{
			name:       "wrapped_forbidden",
			injected:   fmt.Errorf("rbac middleware: %w", apierrors.NewForbidden(gr, "x", errors.New("nope"))),
			wantResult: metrics.FluxNotifyForbidden,
		},
		{
			name: "wrapped_no_match",
			injected: fmt.Errorf("discovery: %w", &apimeta.NoKindMatchError{
				GroupKind:        schema.GroupKind{Group: gr.Group, Kind: kindKustomization},
				SearchedVersions: []string{"v1"},
			}),
			wantResult: metrics.FluxNotifyNoMatch,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMetrics()
			c, calls := recordingClient(t, tc.injected)
			n := newNotifier(t, c, "Milestone")

			n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
				labelKustomizeName:      "k",
				labelKustomizeNamespace: fluxSystemNS,
			}))

			if len(*calls) != 1 {
				t.Fatalf("expected 1 patch attempt, got %d", len(*calls))
			}
			assertCounter(t, "Milestone", "Kustomization", tc.wantResult)
		})
	}
}

// TestNotifier_ConcurrentCalls_NoRace documents the implicit thread-safety
// contract: a single *Notifier is shared across reconcile workers when
// MaxConcurrentReconciles > 1. The struct fields are immutable post-
// construction and the dependencies (client.Client, prometheus counter,
// logr.Logger) are documented thread-safe, so this test is mainly a
// regression net for future field additions. Run with `go test -race`.
func TestNotifier_ConcurrentCalls_NoRace(t *testing.T) {
	resetMetrics()
	c := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
			return nil
		},
		Patch: func(_ context.Context, _ client.WithWatch, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
			return nil
		},
	}).Build()
	n := newNotifier(t, c, "Milestone")

	const workers = 16
	const itersPerWorker = 50
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		go func(workerID int) {
			defer wg.Done()
			labels := map[string]string{
				labelKustomizeName:      fmt.Sprintf("k-%d", workerID),
				labelKustomizeNamespace: fluxSystemNS,
			}
			if workerID%2 == 0 {
				labels[labelHelmName] = fmt.Sprintf("h-%d", workerID)
				labels[labelHelmNamespace] = appsNS
			}
			for range itersPerWorker {
				n.NotifyTransition(context.Background(), milestoneWithLabels(labels))
			}
		}(w)
	}
	wg.Wait()

	// Sanity: total success increments should equal patches issued. With
	// half the workers carrying both label pairs:
	//   half_workers = workers/2
	//   kustomize_calls = workers * iters
	//   helm_calls      = half_workers * iters
	kustomize := testutil.ToFloat64(metrics.FluxNotifyTotal.WithLabelValues("Milestone", "Kustomization", metrics.FluxNotifySuccess))
	helm := testutil.ToFloat64(metrics.FluxNotifyTotal.WithLabelValues("Milestone", "HelmRelease", metrics.FluxNotifySuccess))
	wantKustomize := float64(workers * itersPerWorker)
	wantHelm := float64((workers / 2) * itersPerWorker)
	if kustomize != wantKustomize {
		t.Errorf("Kustomization successes = %v, want %v", kustomize, wantKustomize)
	}
	if helm != wantHelm {
		t.Errorf("HelmRelease successes = %v, want %v", helm, wantHelm)
	}
}

// TestNotifier_BodyJSON_WellFormed parses the merge-patch body as JSON
// and asserts the structural contract. Catches future regressions like
// stray top-level keys, mangled escaping, or annotation-key drift that a
// substring match would miss.
func TestNotifier_BodyJSON_WellFormed(t *testing.T) {
	resetMetrics()
	c, calls := recordingClient(t, nil)
	n := newNotifier(t, c, "Milestone")

	n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
		labelKustomizeName:      "k",
		labelKustomizeNamespace: fluxSystemNS,
	}))

	if len(*calls) != 1 {
		t.Fatalf("expected 1 patch, got %d", len(*calls))
	}

	var got map[string]any
	if err := json.Unmarshal((*calls)[0].body, &got); err != nil {
		t.Fatalf("body not valid JSON: %v (raw=%s)", err, (*calls)[0].body)
	}

	// Top level must have exactly one key: "metadata".
	if len(got) != 1 {
		t.Errorf("top-level keys = %v, want only [metadata]", keysOf(got))
	}
	meta, ok := got["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata not a JSON object: %T", got["metadata"])
	}
	if len(meta) != 1 {
		t.Errorf("metadata keys = %v, want only [annotations]", keysOf(meta))
	}
	anns, ok := meta["annotations"].(map[string]any)
	if !ok {
		t.Fatalf("annotations not a JSON object: %T", meta["annotations"])
	}
	if len(anns) != 1 {
		t.Errorf("annotations keys = %v, want only [reconcile.fluxcd.io/requestedAt]", keysOf(anns))
	}
	rawTS, ok := anns["reconcile.fluxcd.io/requestedAt"].(string)
	if !ok {
		t.Fatalf("requestedAt not a string: %T", anns["reconcile.fluxcd.io/requestedAt"])
	}
	if _, err := time.Parse(time.RFC3339Nano, rawTS); err != nil {
		t.Errorf("requestedAt %q not RFC3339Nano: %v", rawTS, err)
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestNotifier_DefaultNow_UsedWhenNil(t *testing.T) {
	// When Now is nil, the notifier should fall back to time.Now. We don't
	// assert the exact value; only that the patch body parses as RFC3339Nano
	// within a reasonable bound of the wall clock.
	resetMetrics()
	c, calls := recordingClient(t, nil)
	n := &Notifier{Client: c, Controller: "Milestone", Log: logr.Discard()}

	before := time.Now()
	n.NotifyTransition(context.Background(), milestoneWithLabels(map[string]string{
		labelKustomizeName:      "k",
		labelKustomizeNamespace: fluxSystemNS,
	}))
	after := time.Now()

	if len(*calls) != 1 {
		t.Fatalf("expected 1 patch, got %d", len(*calls))
	}
	body := string((*calls)[0].body)
	// Extract the timestamp from `…"reconcile.fluxcd.io/requestedAt":"<ts>"…`
	const marker = `"reconcile.fluxcd.io/requestedAt":"`
	_, after0, ok := strings.Cut(body, marker)
	if !ok {
		t.Fatalf("body missing requestedAt key: %s", body)
	}
	rest := after0
	end := strings.Index(rest, `"`)
	if end < 0 {
		t.Fatalf("body missing closing quote: %s", body)
	}
	ts, err := time.Parse(time.RFC3339Nano, rest[:end])
	if err != nil {
		t.Fatalf("timestamp %q not RFC3339Nano: %v", rest[:end], err)
	}
	if ts.Before(before.Add(-time.Second)) || ts.After(after.Add(time.Second)) {
		t.Errorf("timestamp %v outside [%v, %v]", ts, before, after)
	}
}
