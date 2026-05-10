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

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	"github.com/isometry/echelon-operator/internal/metrics"
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
	echelons        []apiv1.Echelon
	clusterEchelons []apiv1.ClusterEchelon
}

func (f *fakeLister) ListEchelons(_ context.Context) []apiv1.Echelon { return f.echelons }
func (f *fakeLister) ListClusterEchelons(_ context.Context) []apiv1.ClusterEchelon {
	return f.clusterEchelons
}

func newReadyEchelon(ns, name string) apiv1.Echelon {
	now := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	return apiv1.Echelon{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Generation: 3},
		Status: apiv1.EchelonStatus{
			EchelonStatusBase: apiv1.EchelonStatusBase{
				ObservedGeneration: 3,
				LastEvaluatedTime:  now,
				Summary:            apiv1.Summary{Total: 5, Current: 5},
				Conditions: []metav1.Condition{
					{Type: apiv1.ConditionReady, Status: metav1.ConditionTrue, Reason: apiv1.ReasonAllTargetsReady},
					{Type: apiv1.ConditionReconciling, Status: metav1.ConditionFalse},
					{Type: apiv1.ConditionStalled, Status: metav1.ConditionFalse},
				},
				Targets: []apiv1.TargetRollup{
					{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Kind: "Kustomization",
						Ready: metav1.ConditionTrue, Reason: apiv1.ReasonAllMembersReady,
						Summary: apiv1.Summary{Total: 5, Current: 5}},
				},
			},
		},
	}
}

func TestStateCollector_EmitsConditionAndGenerationGauges(t *testing.T) {
	lister := &fakeLister{echelons: []apiv1.Echelon{newReadyEchelon("flux-system", "wave-0")}}
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
		"echelon_status_condition":                 true,
		"echelon_observed_generation":              true,
		"echelon_target_members":                   true,
		"echelon_target_ready":                     true,
		"echelon_last_evaluated_timestamp_seconds": true,
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
	lister := &fakeLister{echelons: []apiv1.Echelon{newReadyEchelon("flux-system", "wave-0")}}
	col := metrics.NewStateCollector(t.Context(), lister)

	v := valueAt(t, col, "echelon_status_condition", map[string]string{
		"owner_kind": "Echelon", "namespace": "flux-system", "name": "wave-0",
		"type": "Ready", "status": "True",
	})
	if v != 1 {
		t.Errorf("Ready=True gauge = %v, want 1", v)
	}
	// The 0-status sibling should be 0.
	v = valueAt(t, col, "echelon_status_condition", map[string]string{
		"owner_kind": "Echelon", "namespace": "flux-system", "name": "wave-0",
		"type": "Ready", "status": "False",
	})
	if v != 0 {
		t.Errorf("Ready=False gauge = %v, want 0", v)
	}
}

func TestStateCollector_TargetReadyEncodes(t *testing.T) {
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
			ech := newReadyEchelon("flux-system", "x")
			ech.Status.Targets[0].Ready = tc.ready
			lister := &fakeLister{echelons: []apiv1.Echelon{ech}}
			col := metrics.NewStateCollector(t.Context(), lister)
			v := valueAt(t, col, "echelon_target_ready", map[string]string{
				"owner_kind": "Echelon", "namespace": "flux-system", "name": "x",
				"target_group": "kustomize.toolkit.fluxcd.io", "target_kind": "Kustomization",
			})
			if v != tc.expect {
				t.Errorf("encoded = %v, want %v", v, tc.expect)
			}
		})
	}
}

func TestStateCollector_ClusterEchelonHasEmptyNamespaceLabel(t *testing.T) {
	ce := apiv1.ClusterEchelon{
		ObjectMeta: metav1.ObjectMeta{Name: "platform-wave", Generation: 1},
		Status: apiv1.ClusterEchelonStatus{
			EchelonStatusBase: apiv1.EchelonStatusBase{
				ObservedGeneration: 1,
				Conditions: []metav1.Condition{
					{Type: apiv1.ConditionReady, Status: metav1.ConditionUnknown},
				},
			},
		},
	}
	col := metrics.NewStateCollector(t.Context(), &fakeLister{clusterEchelons: []apiv1.ClusterEchelon{ce}})
	v := valueAt(t, col, "echelon_status_condition", map[string]string{
		"owner_kind": "ClusterEchelon", "namespace": "", "name": "platform-wave",
		"type": "Ready", "status": "Unknown",
	})
	if v != 1 {
		t.Errorf("Ready=Unknown gauge = %v, want 1", v)
	}
}

func TestStateCollector_TargetMembersPerStatusBucket(t *testing.T) {
	ech := newReadyEchelon("flux-system", "wave-0")
	ech.Status.Targets[0].Summary = apiv1.Summary{Total: 4, Current: 1, InProgress: 2, Failed: 1}
	col := metrics.NewStateCollector(t.Context(), &fakeLister{echelons: []apiv1.Echelon{ech}})

	cases := map[string]float64{
		"current":    1,
		"inProgress": 2,
		"failed":     1,
		"total":      4,
	}
	for status, want := range cases {
		got := valueAt(t, col, "echelon_target_members", map[string]string{
			"owner_kind": "Echelon", "namespace": "flux-system", "name": "wave-0",
			"target_group": "kustomize.toolkit.fluxcd.io", "target_kind": "Kustomization",
			"status": status,
		})
		if got != want {
			t.Errorf("status=%q gauge = %v, want %v", status, got, want)
		}
	}
}

// TestStateCollector_MultipleEchelons_SeparateSeries checks that two Echelons
// sharing a name across different namespaces produce two distinct metric
// series — i.e. the namespace label disambiguates them. Without per-namespace
// emission a deleted-then-recreated owner would clobber the live one's gauge.
func TestStateCollector_MultipleEchelons_SeparateSeries(t *testing.T) {
	lister := &fakeLister{echelons: []apiv1.Echelon{
		newReadyEchelon("flux-system", "wave-0"),
		newReadyEchelon("team-a", "wave-0"),
	}}
	col := metrics.NewStateCollector(t.Context(), lister)

	for _, ns := range []string{"flux-system", "team-a"} {
		v := valueAt(t, col, "echelon_status_condition", map[string]string{
			"owner_kind": "Echelon", "namespace": ns, "name": "wave-0",
			"type": "Ready", "status": "True",
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
