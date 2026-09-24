/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli_test

import (
	"bytes"
	"testing"
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/cli"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var now = time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

func tableRows() []cli.Row {
	return []cli.Row{
		{
			Namespace: "flux-system", Name: nameWave0,
			Ready: metav1.ConditionTrue, Reason: "AllDependenciesReady", Message: "all 2 dependencies ready",
			Stalled: metav1.ConditionFalse,
			Summary: apiv1.Summary{Total: 5, Current: 5, Suspended: 1}, DepsReady: 2, DepsTotal: 2,
			Created: now.Add(-48 * time.Hour), LastEvaluated: now.Add(-42 * time.Second),
		},
		{
			Namespace: "flux-system", Name: "wave-1",
			Ready: metav1.ConditionFalse, Reason: "DependenciesNotReady", Message: "not ready:\nhelmreleases",
			Stalled: metav1.ConditionFalse, Stale: true,
			Summary:   apiv1.Summary{Total: 3, Current: 1, Failed: 1, InProgress: 1},
			DepsReady: 0, DepsTotal: 1,
			Created: now.Add(-5 * time.Minute),
		},
	}
}

func TestPrintTable(t *testing.T) {
	tests := []struct {
		name string
		opts cli.TableOptions
		want string
	}{
		{
			name: "namespaced",
			opts: cli.TableOptions{Namespace: true, Now: now},
			want: "" +
				"NAMESPACE     NAME     READY           REASON                 CURRENT/TOTAL   DEPS   STALLED   AGE   MESSAGE\n" +
				"flux-system   wave-0   True            AllDependenciesReady   5/5             2/2    False     2d    all 2 dependencies ready\n" +
				"flux-system   wave-1   False (stale)   DependenciesNotReady   1/3             0/1    False     5m    not ready: helmreleases\n",
		},
		{
			name: "cluster-scoped omits namespace",
			opts: cli.TableOptions{Now: now, NoHeaders: true},
			want: "" +
				"wave-0   True            AllDependenciesReady   5/5   2/2   False   2d   all 2 dependencies ready\n" +
				"wave-1   False (stale)   DependenciesNotReady   1/3   0/1   False   5m   not ready: helmreleases\n",
		},
		{
			name: "wide",
			opts: cli.TableOptions{Wide: true, Now: now},
			want: "" +
				"NAME     READY           REASON                 CURRENT/TOTAL   INPROGRESS   FAILED   NOTFOUND   TERMINATING   UNKNOWN   SUSPENDED   DEPS   STALLED   AGE   LAST EVALUATED   MESSAGE\n" +
				"wave-0   True            AllDependenciesReady   5/5             0            0        0          0             0         1           2/2    False     2d    42s              all 2 dependencies ready\n" +
				"wave-1   False (stale)   DependenciesNotReady   1/3             1            1        0          0             0         0           0/1    False     5m    <never>          not ready: helmreleases\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := cli.PrintTable(&buf, tableRows(), tt.opts); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}

func TestAge(t *testing.T) {
	tests := []struct {
		ago  time.Duration
		want string
	}{
		{42 * time.Second, "42s"},
		{5 * time.Minute, "5m"},
		{3 * time.Hour, "3h"},
		{49 * time.Hour, "2d1h"},
		{-time.Minute, "0s"},
	}
	for _, tt := range tests {
		if got := cli.Age(now.Add(-tt.ago), now); got != tt.want {
			t.Errorf("Age(%v) = %q, want %q", tt.ago, got, tt.want)
		}
	}
	if got := cli.Age(time.Time{}, now); got != "<unknown>" {
		t.Errorf("Age(zero) = %q", got)
	}
}
