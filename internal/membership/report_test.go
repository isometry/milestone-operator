/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package membership_test

import (
	"encoding/json"
	"reflect"
	"testing"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func recordedMilestone(observed int64, deps ...apiv1.DependencyStatus) *apiv1.Milestone {
	m := ownedMilestone(kustDep(depName, platform))
	m.Status.ObservedGeneration = observed
	m.Status.DependsOn = deps
	m.Status.Conditions = []metav1.Condition{{Type: apiv1.ConditionReady, Status: metav1.ConditionTrue,
		Reason: apiv1.ReasonAllDependenciesReady}}
	m.Status.LastEvaluatedTime = metav1.NewTime(fixedNow)
	return m
}

func recordedDep(ready metav1.ConditionStatus, reason string, total int32) apiv1.DependencyStatus {
	return apiv1.DependencyStatus{Name: depName, Group: groupKustomize, Version: "v1", Kind: kindKust,
		Ready: ready, Reason: reason, Summary: apiv1.Summary{Total: total, Current: total}}
}

func TestReport_RecordedStaleAndDrift(t *testing.T) {
	cases := []struct {
		name     string
		observed int64
		recorded []apiv1.DependencyStatus
		stale    bool
		drift    bool
	}{
		{"agrees", 3, []apiv1.DependencyStatus{recordedDep(metav1.ConditionTrue, apiv1.ReasonAllResourcesReady, 1)},
			false, false},
		// Counts move with every scale event; only the verdict counts as drift.
		{"counts differ only", 3, []apiv1.DependencyStatus{recordedDep(metav1.ConditionTrue, apiv1.ReasonAllResourcesReady, 7)},
			false, false},
		{"ready differs", 3, []apiv1.DependencyStatus{recordedDep(metav1.ConditionFalse, apiv1.ReasonAllResourcesReady, 1)},
			false, true},
		{"reason differs", 3, []apiv1.DependencyStatus{recordedDep(metav1.ConditionTrue, apiv1.ReasonEmptySet, 1)},
			false, true},
		// The operator has not seen this generation yet, so disagreement is expected.
		{"stale suppresses drift", 2, []apiv1.DependencyStatus{recordedDep(metav1.ConditionFalse, apiv1.ReasonResourcesNotReady, 1)},
			true, false},
		{"never reconciled", 0, nil, true, false},
		{"no recorded entry", 3, nil, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, kust(nsOwner, "ok", stateCurrent, platform))
			m := recordedMilestone(tc.observed, tc.recorded...)
			r := evalMilestone(t, h, m)

			if r.Recorded.Stale != tc.stale {
				t.Errorf("Stale = %v, want %v", r.Recorded.Stale, tc.stale)
			}
			if r.Recorded.ObservedGeneration != tc.observed {
				t.Errorf("ObservedGeneration = %d, want %d", r.Recorded.ObservedGeneration, tc.observed)
			}
			if !reflect.DeepEqual(r.Recorded.Conditions, m.Status.Conditions) {
				t.Errorf("Conditions = %+v", r.Recorded.Conditions)
			}
			if !r.Recorded.LastEvaluatedTime.Equal(&m.Status.LastEvaluatedTime) {
				t.Errorf("LastEvaluatedTime = %v", r.Recorded.LastEvaluatedTime)
			}
			d := r.Dependencies[0]
			if d.Drift != tc.drift {
				t.Errorf("Drift = %v, want %v", d.Drift, tc.drift)
			}
			switch {
			case len(tc.recorded) == 0 && d.Recorded != nil:
				t.Errorf("Recorded = %+v, want nil", d.Recorded)
			case len(tc.recorded) > 0 && (d.Recorded == nil || !reflect.DeepEqual(*d.Recorded, tc.recorded[0])):
				t.Errorf("Recorded = %+v, want %+v", d.Recorded, tc.recorded[0])
			}
		})
	}
}

func TestReport_JSONInlinesMember(t *testing.T) {
	h := newHarness(t, kust(nsOwner, "paused", stateCurrent, platform, suspended))
	dep := kustDep(depName, platform)
	dep.SuspendPolicy = apiv1.SuspendNotReady
	r := evalMilestone(t, h, ownedMilestone(dep))

	raw, err := json.Marshal(r.Dependencies[0].Members[0])
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	want := map[string]any{
		"group": groupKustomize, "version": "v1", "kind": kindKust, "namespace": nsOwner, "name": "paused",
		keyStatus: statCurrent, keyMessage: "Resource is Ready", "suspended": true, "blocking": true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("member JSON = %s", raw)
	}
}

func TestReport_JSONShape(t *testing.T) {
	h := newHarness(t, kust(nsOwner, "ok", stateCurrent, platform))
	r := evalMilestone(t, h, recordedMilestone(3))

	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for _, key := range []string{"owner", "recorded", "ready", keyReason, "summary", "dependencies", "evaluatedAt"} {
		if _, ok := got[key]; !ok {
			t.Errorf("report JSON lacks %q: %s", key, raw)
		}
	}
	dep := got["dependencies"].([]any)[0].(map[string]any)
	for _, key := range []string{"name", "target", "status", "members"} {
		if _, ok := dep[key]; !ok {
			t.Errorf("dependency JSON lacks %q: %v", key, dep)
		}
	}
	owner := got["owner"].(map[string]any)
	if owner["kind"] != "Milestone" || owner["namespace"] != nsOwner || owner["name"] != ownerName {
		t.Errorf("owner JSON = %v", owner)
	}
}
