/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/membership"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

func TestTree_Milestone(t *testing.T) {
	h := newHarness(t, standardObjects(t), standardNamespaces()...)
	got := h.mustRun(t, cmdTree, "milestone", nameWave1)
	want := strings.Join([]string{
		"Milestone/flux-system/wave-1  Ready=False  DependenciesNotReady  " +
			"(evaluated 42s ago, stale: generation 3 not yet observed)",
		"└── ✗ kustomizations  Kustomization.kustomize.toolkit.fluxcd.io/v1  wave=1  1/2  ResourcesNotReady",
		"    ├── ✔ flux-system/apps     Current",
		"    └── ✗ flux-system/broken   Failed  BuildFailed: build failed",
		"",
	}, "\n")
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestTree_ClusterMilestoneNotReadyAndDrift(t *testing.T) {
	h := newHarness(t, standardObjects(t), standardNamespaces()...)
	got := h.mustRun(t, cmdTree, argCmile, namePlat, "--not-ready")
	for _, w := range []string{
		"ClusterMilestone/platform  Ready=False",
		"namespaceSelector=tier=platform",
		"⚠ live differs from recorded (recorded: Ready=True AllResourcesReady)",
		"flux-system/broken",
	} {
		if !strings.Contains(got, w) {
			t.Errorf("missing %q in:\n%s", w, got)
		}
	}
	for _, healthy := range []string{"flux-system/infra", "flux-system/apps "} {
		if strings.Contains(got, healthy) {
			t.Errorf("--not-ready kept %q:\n%s", healthy, got)
		}
	}
}

func TestTree_JSON(t *testing.T) {
	h := newHarness(t, standardObjects(t), standardNamespaces()...)
	var r membership.Report
	if err := json.Unmarshal([]byte(h.mustRun(t, cmdTree, argMile, nameWave0, "-o", "json")), &r); err != nil {
		t.Fatal(err)
	}
	if r.Owner.Name != nameWave0 || r.Ready != metav1.ConditionTrue || len(r.Dependencies) != 1 {
		t.Fatalf("report = %+v", r)
	}
	if m := r.Dependencies[0].Members; len(m) != 1 || m[0].Name != nameInfra || m[0].Blocking {
		t.Errorf("members = %+v", m)
	}
}

func TestTree_ForbiddenIsReportedInline(t *testing.T) {
	h := newHarness(t, standardObjects(t), standardNamespaces()...)
	h.clients.dyn.PrependReactor(verbList, resKust, func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(gvrKust.GroupResource(), "", nil)
	})
	got := h.mustRun(t, cmdTree, argMile, nameWave0)
	if !strings.Contains(got, "? kustomizations") || !strings.Contains(got, "cannot check: forbidden (your credentials)") {
		t.Errorf("got:\n%s", got)
	}
}

func TestTree_StructuralErrorIsReportedInline(t *testing.T) {
	objs := []runtime.Object{milestone(t, nsFlux, "gadgets", []apiv1.DependencyRef{{
		Name: "gadgets", Target: apiv1.TargetSpec{Group: "example.com", Kind: "Gadget"},
	}})}
	h := newHarness(t, objs, standardNamespaces()...)
	got := h.mustRun(t, cmdTree, argMile, "gadgets")
	if !strings.Contains(got, "? gadgets  Gadget.example.com  <all>  GVKNotEstablished: ") {
		t.Errorf("got:\n%s", got)
	}
}

func TestTree_NotFound(t *testing.T) {
	h := newHarness(t, nil)
	if code := h.run(context.Background(), cmdTree, argMile, "nope"); code == 0 {
		t.Fatal("expected a non-zero exit")
	}
	if !strings.Contains(h.errOut.String(), `Milestone "nope" not found in "flux-system" namespace`) {
		t.Errorf("stderr = %q", h.errOut)
	}
	if code := h.run(context.Background(), cmdTree, argMile); code == 0 {
		t.Error("NAME is required")
	}
}
