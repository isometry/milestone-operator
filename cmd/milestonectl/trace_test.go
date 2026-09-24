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
	"errors"
	"strings"
	"testing"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/cli"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clienttesting "k8s.io/client-go/testing"
)

func configMap(ns, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("v1")
	u.SetKind("ConfigMap")
	u.SetNamespace(ns)
	u.SetName(name)
	return u
}

func TestTrace_ShortNameHit(t *testing.T) {
	h := newHarness(t, standardObjects(t), standardNamespaces()...)
	got := h.mustRun(t, cmdTrace, "ks/broken")
	want := "" +
		"✗ Kustomization.kustomize.toolkit.fluxcd.io/flux-system/broken  Failed  BuildFailed: build failed\n" +
		"\n" +
		"OWNER                          DEPENDENCY                OWNER READY     OWNER REASON           BLOCKING\n" +
		"Milestone/flux-system/wave-1   kustomizations            False (stale)   DependenciesNotReady   yes\n" +
		"ClusterMilestone/platform      platform-kustomizations   True            AllDependenciesReady   yes\n"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestTrace_TypeNameJSON(t *testing.T) {
	h := newHarness(t, standardObjects(t), standardNamespaces()...)
	var r cli.TraceReport
	out := h.mustRun(t, cmdTrace, "kustomizations.kustomize.toolkit.fluxcd.io", nameInfra, "-n", nsFlux, "-o", "json")
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		t.Fatal(err)
	}
	if r.Object.Name != nameInfra || r.Object.Status != "Current" || len(r.Memberships) != 2 {
		t.Fatalf("report = %+v", r)
	}
	if m := r.Memberships[0]; m.Owner.Name != nameWave0 || m.Dependency != resKust || m.Blocking {
		t.Errorf("first membership = %+v", m)
	}
}

func TestTrace_NamespaceScoping(t *testing.T) {
	objs := append(standardObjects(t), kust(nsApps, "other", wave0, true))
	h := newHarness(t, objs, standardNamespaces()...)
	got := h.mustRun(t, cmdTrace, "ks", "other", "-n", nsApps)
	if !strings.Contains(got, "Milestone/apps/apps-ready") || strings.Contains(got, namePlat) {
		t.Errorf("got:\n%s", got)
	}
}

func TestTrace_Miss(t *testing.T) {
	objs := append(standardObjects(t), configMap(nsFlux, "settings"))
	h := newHarness(t, objs, standardNamespaces()...)
	got := h.mustRun(t, cmdTrace, "cm/settings")
	if got != "✔ ConfigMap/flux-system/settings  Current\nnot a member of any milestone\n" {
		t.Errorf("got %q", got)
	}
}

func TestTrace_WarnsAboutUnevaluatedDependencies(t *testing.T) {
	bad := apiv1.DependencyRef{Name: "bad", Target: apiv1.TargetSpec{
		Group: groupKust, Kind: kindKust,
		Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: labelWave, Operator: "Bogus"},
		}},
	}}
	objs := []runtime.Object{
		milestone(t, nsFlux, "broken-spec", []apiv1.DependencyRef{bad}),
		kust(nsFlux, nameInfra, wave0, true),
	}
	h := newHarness(t, objs, standardNamespaces()...)
	if got := h.mustRun(t, cmdTrace, "ks/infra"); !strings.HasSuffix(got, "not a member of any milestone\n") {
		t.Errorf("stdout = %q", got)
	}
	want := `Milestone/flux-system/broken-spec dependency "bad" could not be evaluated (DiscoveryFailed)`
	if !strings.Contains(h.errOut.String(), want) {
		t.Errorf("stderr = %q", h.errOut)
	}
}

func TestTrace_Errors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"not found", []string{cmdTrace, "deploy/web"}, `Deployment "web" not found in "flux-system" namespace`},
		{"unknown type", []string{cmdTrace, "widgets/x"}, `resource type "widgets"`},
		{"malformed", []string{cmdTrace, "web"}, "expected TYPE/NAME or TYPE NAME"},
		{"too many", []string{cmdTrace, "a", "b", "c"}, "accepts between 1 and 2 arg(s)"},
	}
	h := newHarness(t, standardObjects(t), standardNamespaces()...)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if code := h.run(context.Background(), tt.args...); code == 0 {
				t.Fatalf("expected failure:\n%s", h.out)
			}
			if !strings.Contains(h.errOut.String(), tt.want) {
				t.Errorf("stderr %q does not contain %q", h.errOut, tt.want)
			}
		})
	}
}

func failList(h *harness, resource string, err error) {
	h.clients.dyn.PrependReactor(verbList, resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, err
	})
}

func forbidden(resource string) error {
	return apierrors.NewForbidden(schema.GroupResource{Group: "milestone.as-code.io", Resource: resource}, "",
		errors.New("denied"))
}

func TestTrace_SkipsUnlistableOwnerKind(t *testing.T) {
	const skipped = "membership via ClusterMilestones not checked"
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"forbidden owner kind", forbidden("clustermilestones"),
			"cannot list ClusterMilestones (" + whyForbidden + "): " + skipped},
		{"missing owner kind", apierrors.NewNotFound(schema.GroupResource{Resource: "clustermilestones"}, ""),
			"cannot list ClusterMilestones (" + whyNotFound + "): " + skipped},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, standardObjects(t), standardNamespaces()...)
			failList(h, "clustermilestones", tt.err)
			got := h.mustRun(t, cmdTrace, "ks/broken")
			if !strings.Contains(got, "Milestone/flux-system/wave-1") || strings.Contains(got, namePlat) {
				t.Errorf("stdout:\n%s", got)
			}
			if n := strings.Count(h.errOut.String(), "cannot list"); n != 1 || !strings.Contains(h.errOut.String(), tt.want) {
				t.Errorf("stderr = %q", h.errOut)
			}
		})
	}
}

func TestTrace_SkipsUnlistableOwnerKind_NoOverclaim(t *testing.T) {
	objs := append(standardObjects(t), configMap(nsFlux, "settings"))
	h := newHarness(t, objs, standardNamespaces()...)
	failList(h, "clustermilestones", forbidden("clustermilestones"))
	got := h.mustRun(t, cmdTrace, "cm/settings")
	if !strings.HasSuffix(got, "not a member of any milestone (some owner kinds could not be checked)\n") {
		t.Errorf("stdout = %q", got)
	}
	var r cli.TraceReport
	if err := json.Unmarshal([]byte(h.mustRun(t, cmdTrace, "cm/settings", "-o", "json")), &r); err != nil {
		t.Fatal(err)
	}
	if len(r.Unchecked) != 1 || r.Unchecked[0] != "ClusterMilestone" {
		t.Errorf("unchecked = %v", r.Unchecked)
	}
}

func TestTrace_UnlistableOwnerKindsAreHardErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*harness)
		want  string
	}{
		{"both forbidden", func(h *harness) {
			failList(h, "milestones", forbidden("milestones"))
			failList(h, "clustermilestones", forbidden("clustermilestones"))
		}, "forbidden"},
		{"unexpected error", func(h *harness) {
			failList(h, "clustermilestones", apierrors.NewInternalError(errors.New("boom")))
		}, "boom"},
		{"unexpected error on the other kind", func(h *harness) {
			failList(h, "milestones", apierrors.NewInternalError(errors.New("boom")))
			failList(h, "clustermilestones", forbidden("clustermilestones"))
		}, "boom"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, standardObjects(t), standardNamespaces()...)
			tt.setup(h)
			if code := h.run(context.Background(), cmdTrace, "ks/broken"); code == 0 {
				t.Fatalf("expected failure:\n%s", h.out)
			}
			if !strings.Contains(h.errOut.String(), tt.want) {
				t.Errorf("stderr %q does not contain %q", h.errOut, tt.want)
			}
		})
	}
}
