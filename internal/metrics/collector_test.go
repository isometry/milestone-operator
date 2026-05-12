/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package metrics_test

import (
	"context"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// valueAt collects from col and returns the sample value for metricName whose
// labels are a superset of labelMatch. Fatals if not found.
func valueAt(t *testing.T, col prometheus.Collector, metricName string, labelMatch map[string]string) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	if err := reg.Register(col); err != nil {
		t.Fatalf("register: %v", err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsContain(m.GetLabel(), labelMatch) {
				return sampleValue(m)
			}
		}
	}
	t.Fatalf("metric %q with labels %v not found", metricName, labelMatch)
	return 0
}

func labelsContain(have []*dto.LabelPair, want map[string]string) bool {
	got := make(map[string]string, len(have))
	for _, lp := range have {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func sampleValue(m *dto.Metric) float64 {
	if m.Gauge != nil {
		return m.Gauge.GetValue()
	}
	if m.Counter != nil {
		return m.Counter.GetValue()
	}
	if m.Untyped != nil {
		return m.Untyped.GetValue()
	}
	return 0
}

type fakeLister struct {
	milestones        []apiv1.Milestone
	clusterMilestones []apiv1.ClusterMilestone
}

func (f *fakeLister) ListMilestones(_ context.Context) []apiv1.Milestone { return f.milestones }
func (f *fakeLister) ListClusterMilestones(_ context.Context) []apiv1.ClusterMilestone {
	return f.clusterMilestones
}

const depName = "kustomizations"

func newReadyMilestone(ns, name string) apiv1.Milestone {
	now := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	return apiv1.Milestone{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Generation: 3},
		Status: apiv1.MilestoneStatus{
			MilestoneStatusBase: apiv1.MilestoneStatusBase{
				ObservedGeneration: 3,
				LastEvaluatedTime:  now,
				Summary:            apiv1.Summary{Total: 5, Current: 5},
				Conditions: []metav1.Condition{
					{Type: apiv1.ConditionReady, Status: metav1.ConditionTrue, Reason: apiv1.ReasonAllDependenciesReady},
					{Type: apiv1.ConditionReconciling, Status: metav1.ConditionFalse},
					{Type: apiv1.ConditionStalled, Status: metav1.ConditionFalse},
				},
				DependsOn: []apiv1.DependencyStatus{
					{Name: depName, Group: groupKustomize, Version: "v1", Kind: kindKustomization,
						Ready: metav1.ConditionTrue, Reason: apiv1.ReasonAllResourcesReady,
						Summary: apiv1.Summary{Total: 5, Current: 5}},
				},
			},
		},
	}
}

func TestStateCollector_EmitsConditionAndGenerationGauges(t *testing.T) {
	lister := &fakeLister{milestones: []apiv1.Milestone{newReadyMilestone(nsFluxSystem, "wave-0")}}
	reg := prometheus.NewRegistry()
	col := metrics.NewStateCollector(t.Context(), lister)
	if err := reg.Register(col); err != nil {
		t.Fatalf("register: %v", err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := map[string]bool{
		"milestone_status_condition":                 true,
		"milestone_observed_generation":              true,
		"milestone_dependency_resources":             true,
		"milestone_dependency_ready":                 true,
		"milestone_last_evaluated_timestamp_seconds": true,
	}
	have := map[string]bool{}
	for _, mf := range mfs {
		have[mf.GetName()] = true
	}
	for n := range want {
		if !have[n] {
			t.Errorf("missing metric family %q (have: %s)", n, joinStrings(have))
		}
	}
}

func TestStateCollector_StatusConditionGauge(t *testing.T) {
	lister := &fakeLister{milestones: []apiv1.Milestone{newReadyMilestone(nsFluxSystem, "wave-0")}}
	col := metrics.NewStateCollector(t.Context(), lister)

	v := valueAt(t, col, "milestone_status_condition", map[string]string{
		keyOwnerKind: kindMilestone, keyNamespace: nsFluxSystem, keyName: nameWave0,
		keyType: apiv1.ConditionReady, keyStatus: "True",
	})
	if v != 1 {
		t.Errorf("Ready=True gauge = %v, want 1", v)
	}
	// The 0-status sibling should be 0.
	v = valueAt(t, col, "milestone_status_condition", map[string]string{
		keyOwnerKind: kindMilestone, keyNamespace: nsFluxSystem, keyName: nameWave0,
		keyType: apiv1.ConditionReady, keyStatus: "False",
	})
	if v != 0 {
		t.Errorf("Ready=False gauge = %v, want 0", v)
	}
}

func TestStateCollector_DependencyReadyEncodes(t *testing.T) {
	cases := []struct {
		name   string
		ready  metav1.ConditionStatus
		expect float64
	}{
		{"true is 1", metav1.ConditionTrue, 1},
		{"false is 0", metav1.ConditionFalse, 0},
		{"unknown is -1", metav1.ConditionUnknown, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newReadyMilestone(nsFluxSystem, "x")
			m.Status.DependsOn[0].Ready = tc.ready
			lister := &fakeLister{milestones: []apiv1.Milestone{m}}
			col := metrics.NewStateCollector(t.Context(), lister)
			v := valueAt(t, col, "milestone_dependency_ready", map[string]string{
				keyOwnerKind: kindMilestone, keyNamespace: nsFluxSystem, keyName: "x",
				keyDependency: depName, keyTargetGroup: groupKustomize, keyTargetKind: kindKustomization,
			})
			if v != tc.expect {
				t.Errorf("encoded = %v, want %v", v, tc.expect)
			}
		})
	}
}

func TestStateCollector_ClusterMilestoneHasEmptyNamespaceLabel(t *testing.T) {
	cm := apiv1.ClusterMilestone{
		ObjectMeta: metav1.ObjectMeta{Name: "platform-wave", Generation: 1},
		Status: apiv1.ClusterMilestoneStatus{
			MilestoneStatusBase: apiv1.MilestoneStatusBase{
				ObservedGeneration: 1,
				Conditions: []metav1.Condition{
					{Type: apiv1.ConditionReady, Status: metav1.ConditionUnknown},
				},
			},
		},
	}
	col := metrics.NewStateCollector(t.Context(), &fakeLister{clusterMilestones: []apiv1.ClusterMilestone{cm}})
	v := valueAt(t, col, "milestone_status_condition", map[string]string{
		keyOwnerKind: "ClusterMilestone", keyNamespace: "", keyName: "platform-wave",
		keyType: apiv1.ConditionReady, keyStatus: "Unknown",
	})
	if v != 1 {
		t.Errorf("Ready=Unknown gauge = %v, want 1", v)
	}
}

func TestStateCollector_DependencyResourcesPerStatusBucket(t *testing.T) {
	m := newReadyMilestone(nsFluxSystem, "wave-0")
	m.Status.DependsOn[0].Summary = apiv1.Summary{Total: 4, Current: 1, InProgress: 2, Failed: 1}
	col := metrics.NewStateCollector(t.Context(), &fakeLister{milestones: []apiv1.Milestone{m}})

	cases := map[string]float64{
		"current":    1,
		"inProgress": 2,
		"failed":     1,
		"total":      4,
	}
	for status, want := range cases {
		got := valueAt(t, col, "milestone_dependency_resources", map[string]string{
			keyOwnerKind: kindMilestone, keyNamespace: nsFluxSystem, keyName: nameWave0,
			keyDependency: depName, keyTargetGroup: groupKustomize, keyTargetKind: kindKustomization,
			keyStatus: status,
		})
		if got != want {
			t.Errorf("status=%q gauge = %v, want %v", status, got, want)
		}
	}
}

// TestStateCollector_MultipleMilestones_SeparateSeries checks that two
// Milestones sharing a name across different namespaces produce two distinct
// metric series — i.e. the namespace label disambiguates them. Without
// per-namespace emission a deleted-then-recreated owner would clobber the
// live one's gauge.
func TestStateCollector_MultipleMilestones_SeparateSeries(t *testing.T) {
	lister := &fakeLister{milestones: []apiv1.Milestone{
		newReadyMilestone(nsFluxSystem, "wave-0"),
		newReadyMilestone("team-a", "wave-0"),
	}}
	col := metrics.NewStateCollector(t.Context(), lister)

	for _, ns := range []string{"flux-system", "team-a"} {
		v := valueAt(t, col, "milestone_status_condition", map[string]string{
			"owner_kind": kindMilestone, "namespace": ns, keyName: "wave-0",
			keyType: apiv1.ConditionReady, keyStatus: "True",
		})
		if v != 1 {
			t.Errorf("namespace=%q Ready=True gauge = %v, want 1", ns, v)
		}
	}
}

func joinStrings(m map[string]bool) string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return strings.Join(out, ", ")
}
