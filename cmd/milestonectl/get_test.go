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
	"slices"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/cli"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	clienttesting "k8s.io/client-go/testing"
)

func TestGet_Milestones(t *testing.T) {
	h := newHarness(t, standardObjects(t))
	got := h.mustRun(t, cmdGet, "milestones")
	want := "" +
		"NAME     READY           REASON                 CURRENT/TOTAL   DEPS   STALLED   AGE   MESSAGE\n" +
		"wave-0   True            AllDependenciesReady   1/1             1/1    False     2d    AllDependenciesReady\n" +
		"wave-1   False (stale)   DependenciesNotReady   1/2             0/1    False     2d    DependenciesNotReady\n"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestGet_Variants(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    []string
		notWant []string
	}{
		{
			name:    "alias and explicit namespace",
			args:    []string{cmdGet, argMile, "-n", nsApps},
			want:    []string{nameAppsReady},
			notWant: []string{nameWave0, colNamespace},
		},
		{
			name: "all namespaces adds the namespace column",
			args: []string{cmdGet, "milestones", "-A"},
			want: []string{colNamespace, "apps   ", nameAppsReady, "flux-system   wave-0"},
		},
		{
			name:    "status selector",
			args:    []string{cmdGet, argMile, "-A", flagStatusSelector, "ready=False"},
			want:    []string{nameWave1},
			notWant: []string{nameWave0, nameAppsReady},
		},
		{
			name:    "repeated status selectors AND",
			args:    []string{cmdGet, argMile, flagStatusSelector, "ready=True", flagStatusSelector, "stalled=False"},
			want:    []string{nameWave0},
			notWant: []string{nameWave1},
		},
		{
			name: cli.OutputWide,
			args: []string{cmdGet, argMile, "-o", cli.OutputWide},
			want: []string{"INPROGRESS", "LAST EVALUATED", "42s"},
		},
		{
			name:    "cluster milestones ignore the namespace",
			args:    []string{cmdGet, argCmile, "-n", nsApps},
			want:    []string{"NAME ", namePlat},
			notWant: []string{colNamespace},
		},
		{
			name: cmdAll,
			args: []string{cmdGet, cmdAll, "-A"},
			want: []string{"milestone/wave-0", "milestone/apps-ready", "clustermilestone/platform"},
		},
		{
			name:    "single name",
			args:    []string{cmdGet, "milestone", nameWave1},
			want:    []string{nameWave1},
			notWant: []string{nameWave0},
		},
	}
	h := newHarness(t, standardObjects(t))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := h.mustRun(t, tt.args...)
			for _, w := range tt.want {
				if !strings.Contains(got, w) {
					t.Errorf("missing %q in:\n%s", w, got)
				}
			}
			for _, w := range tt.notWant {
				if strings.Contains(got, w) {
					t.Errorf("unexpected %q in:\n%s", w, got)
				}
			}
		})
	}
}

func TestGet_Errors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"missing object", []string{cmdGet, argMile, "nope"}, `Milestone "nope" not found in "flux-system" namespace`},
		{"missing cluster object", []string{cmdGet, argCmile, "nope"}, `ClusterMilestone "nope" not found`},
		{"name with -A", []string{cmdGet, argMile, nameWave0, "-A"}, "cannot be retrieved by name across all namespaces"},
		{"bad selector", []string{cmdGet, argMile, flagStatusSelector, "ready"}, "invalid status selector"},
		{"bad output", []string{cmdGet, argMile, "-o", cli.OutputTree}, "must be one of: table|wide|json|yaml"},
		{"watch all", []string{cmdGet, cmdAll, "-w"}, "--watch is not supported"},
		{"unknown kind", []string{cmdGet, "widgets"}, `unknown command "widgets" for "milestonectl get"`},
	}
	h := newHarness(t, standardObjects(t))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code := h.run(context.Background(), tt.args...)
			if code == 0 {
				t.Fatalf("expected failure, got output:\n%s", h.out)
			}
			if !strings.Contains(h.errOut.String(), tt.want) {
				t.Errorf("stderr %q does not contain %q", h.errOut, tt.want)
			}
			if strings.Contains(h.errOut.String(), "Usage:") {
				t.Errorf("errors must not print usage:\n%s", h.errOut)
			}
		})
	}
}

func TestGet_Empty(t *testing.T) {
	h := newHarness(t, nil)
	if out := h.mustRun(t, cmdGet, argMile); out != "" {
		t.Errorf("stdout = %q", out)
	}
	if want := "no Milestone objects found in \"flux-system\" namespace\n"; h.errOut.String() != want {
		t.Errorf("stderr = %q", h.errOut)
	}
	h.mustRun(t, cmdGet, cmdAll)
	if want := "no Milestone or ClusterMilestone objects found\n"; h.errOut.String() != want {
		t.Errorf("stderr = %q", h.errOut)
	}
}

func TestGet_Structured(t *testing.T) {
	h := newHarness(t, standardObjects(t))

	var list struct {
		Kind  string           `json:"kind"`
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal([]byte(h.mustRun(t, cmdGet, argMile, "-o", "json")), &list); err != nil {
		t.Fatal(err)
	}
	if list.Kind != "List" || len(list.Items) != 2 {
		t.Errorf("list = %+v", list)
	}

	var obj apiv1.Milestone
	if err := json.Unmarshal([]byte(h.mustRun(t, cmdGet, argMile, nameWave0, "-o", "json")), &obj); err != nil {
		t.Fatal(err)
	}
	if obj.Kind != "Milestone" || obj.Name != nameWave0 {
		t.Errorf("object = %s %s", obj.Kind, obj.Name)
	}

	if out := h.mustRun(t, cmdGet, cmdAll, "-A", "-o", "yaml"); !strings.Contains(out, "kind: ClusterMilestone") ||
		!strings.Contains(out, "kind: Milestone") {
		t.Errorf("yaml:\n%s", out)
	}
}

func TestGet_Watch(t *testing.T) {
	h := newHarness(t, standardObjects(t))
	fw := watch.NewFake()
	h.clients.dyn.PrependWatchReactor("milestones", clienttesting.DefaultWatchReactor(fw, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int)
	go func() { done <- h.run(ctx, cmdGet, argMile, "-w") }()

	current := milestone(t, nsFlux, nameWave0, []apiv1.DependencyRef{kustDep(wave0)},
		recorded(metav1.ConditionTrue, apiv1.ReasonAllDependenciesReady, apiv1.DependencyStatus{
			Name: resKust, Ready: metav1.ConditionTrue, Summary: apiv1.Summary{Total: 1, Current: 1},
		}))
	// Unchanged apart from lastEvaluatedTime: must not be reprinted.
	bumped := current.DeepCopy()
	_ = unstructured.SetNestedField(bumped.Object, fixedNow.Format(time.RFC3339), "status", "lastEvaluatedTime")
	fw.Modify(bumped)
	// Flipped to not ready: must be reprinted.
	flipped := milestone(t, nsFlux, nameWave0, []apiv1.DependencyRef{kustDep(wave0)},
		recorded(metav1.ConditionFalse, apiv1.ReasonDependenciesNotReady))
	fw.Modify(flipped)
	// The fake watcher is unbuffered: once this send returns, every earlier
	// event has been fully handled.
	fw.Delete(flipped)
	cancel()

	if code := <-done; code != 0 {
		t.Fatalf("exit %d: %s", code, h.errOut)
	}
	lines := strings.Split(strings.TrimSpace(h.out.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("want header, 2 rows and 1 update, got:\n%s", h.out)
	}
	if !strings.HasPrefix(lines[3], "wave-0   False") || !strings.Contains(lines[3], apiv1.ReasonDependenciesNotReady) {
		t.Errorf("update row = %q", lines[3])
	}
}

func TestGet_WatchExpiredResumes(t *testing.T) {
	h := newHarness(t, nil)
	first, second := watch.NewFake(), watch.NewFake()
	watchers := []*watch.FakeWatcher{first, second}
	var rvs []string
	h.clients.dyn.PrependWatchReactor("milestones", func(action clienttesting.Action) (bool, watch.Interface, error) {
		rvs = append(rvs, action.(clienttesting.WatchActionImpl).WatchRestrictions.ResourceVersion)
		w := watchers[0]
		watchers = watchers[1:]
		return true, w, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int)
	go func() { done <- h.run(ctx, cmdGet, argMile, "-w", "-o", "yaml") }()

	first.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired})
	obj := milestone(t, nsFlux, nameWave0, nil)
	obj.SetResourceVersion("7")
	second.Add(obj)
	second.Delete(obj)
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("exit %d: %s", code, h.errOut)
	}
	if len(rvs) != 2 || rvs[1] != "" {
		t.Errorf("watch resourceVersions = %q; want a fresh watch after expiry", rvs)
	}
	if !strings.Contains(h.out.String(), "---\n") || !strings.Contains(h.out.String(), "name: wave-0") {
		t.Errorf("stdout:\n%s", h.out)
	}
}

func TestGet_WatchBacksOffWhenClosedWithoutEvents(t *testing.T) {
	var slept []time.Duration
	orig := sleepCtx
	sleepCtx = func(_ context.Context, d time.Duration) { slept = append(slept, d) }
	t.Cleanup(func() { sleepCtx = orig })

	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	opens := 0
	h.clients.dyn.PrependWatchReactor("milestones", func(clienttesting.Action) (bool, watch.Interface, error) {
		opens++
		w := watch.NewFake()
		switch opens {
		case 1, 2:
			w.Stop()
		case 3:
			// An event resets the backoff: the close that follows must not sleep.
			go func() {
				w.Add(milestone(t, nsFlux, nameWave0, nil))
				w.Stop()
			}()
		default:
			cancel()
		}
		return true, w, nil
	})
	if code := h.run(ctx, cmdGet, argMile, "-w"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.errOut)
	}
	if want := []time.Duration{watchBackoff, watchBackoff}; !slices.Equal(slept, want) {
		t.Errorf("sleeps = %v, want %v", slept, want)
	}
}
