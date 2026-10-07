/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli_test

import (
	"context"
	"testing"
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/cli"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestLookupKind(t *testing.T) {
	for name, want := range map[string]string{
		"milestone": kindMilestone, "Milestones": kindMilestone, "mile": kindMilestone,
		"clustermilestone": kindClusterMilestone, "clustermilestones": kindClusterMilestone, "CMILE": kindClusterMilestone,
	} {
		k, ok := cli.LookupKind(name)
		if !ok || k.Kind != want {
			t.Errorf("LookupKind(%q) = %v, %v; want %s", name, k.Kind, ok, want)
		}
	}
	if _, ok := cli.LookupKind("kustomization"); ok {
		t.Error("unexpected match")
	}
	if gvr := cli.ClusterMilestoneKind.GVR(); gvr.String() != "milestone.as-code.io/v1, Resource=clustermilestones" {
		t.Errorf("GVR = %s", gvr)
	}
}

func toUnstructured(t *testing.T, obj runtime.Object) *unstructured.Unstructured {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	return &unstructured.Unstructured{Object: m}
}

func TestDecodeAndRow(t *testing.T) {
	created := metav1.NewTime(now.Add(-48 * time.Hour))
	m := &apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: nsFlux, Name: nameWave0, Generation: 2, CreationTimestamp: created},
		Spec: apiv1.MilestoneSpec{DependsOn: []apiv1.DependencyRef{
			{Name: "kustomizations", Target: apiv1.TargetSpec{Kind: kindKust}},
		}},
		Status: apiv1.MilestoneStatus{MilestoneStatusBase: apiv1.MilestoneStatusBase{
			ObservedGeneration: 1,
			Conditions: []metav1.Condition{
				{Type: apiv1.ConditionReady, Status: metav1.ConditionFalse, Reason: apiv1.ReasonDependenciesNotReady, Message: "m"},
				{Type: apiv1.ConditionStalled, Status: metav1.ConditionFalse, Reason: apiv1.ReasonReconcileComplete},
			},
			Summary: apiv1.Summary{Total: 2, Current: 1},
			DependsOn: []apiv1.DependencyStatus{
				{Name: "a", Ready: metav1.ConditionTrue},
				{Name: "b", Ready: metav1.ConditionFalse},
			},
		}},
	}
	o, err := cli.MilestoneKind.Decode(toUnstructured(t, m))
	if err != nil {
		t.Fatal(err)
	}
	if o.Ref() != "Milestone/flux-system/wave-0" || o.Meta().Name != nameWave0 || o.Status().ObservedGeneration != 1 {
		t.Errorf("decoded %s %+v", o.Ref(), o.Status())
	}
	got := o.Row()
	want := cli.Row{
		Namespace: nsFlux, Name: nameWave0,
		Ready: metav1.ConditionFalse, Reason: apiv1.ReasonDependenciesNotReady, Message: "m",
		Stalled: metav1.ConditionFalse, Stale: true,
		Summary: apiv1.Summary{Total: 2, Current: 1}, DepsReady: 1, DepsTotal: 2,
		Created: created.Time,
	}
	if !got.Created.Equal(want.Created) {
		t.Errorf("Created = %v, want %v", got.Created, want.Created)
	}
	got.Created = want.Created
	if got != want {
		t.Errorf("Row =\n%+v\nwant\n%+v", got, want)
	}

	// With no resolver every dependency fails normalisation, which is
	// enough to prove the dispatch reaches the Milestone path.
	if _, errs := o.Normalize(context.Background(), nil, nil); len(errs) == 0 {
		t.Error("expected a normalisation error without a resolver")
	}
}

func TestDecodeClusterMilestone(t *testing.T) {
	cm := &apiv1.ClusterMilestone{ObjectMeta: metav1.ObjectMeta{Name: namePlatform}}
	o, err := cli.ClusterMilestoneKind.Decode(toUnstructured(t, cm))
	if err != nil {
		t.Fatal(err)
	}
	if o.Ref() != "ClusterMilestone/platform" {
		t.Errorf("Ref = %s", o.Ref())
	}
	r := o.Row()
	if r.Ready != metav1.ConditionUnknown || r.Stalled != metav1.ConditionUnknown {
		t.Errorf("absent conditions should read Unknown: %+v", r)
	}

	bad := &unstructured.Unstructured{Object: map[string]any{"spec": "not-an-object"}}
	if _, err := cli.ClusterMilestoneKind.Decode(bad); err == nil {
		t.Error("expected decode error")
	}
}
