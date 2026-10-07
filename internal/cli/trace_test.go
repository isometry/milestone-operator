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

	"github.com/isometry/milestone-operator/internal/cli"
	"github.com/isometry/milestone-operator/internal/membership"
	"github.com/isometry/milestone-operator/internal/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPrintTrace(t *testing.T) {
	obj := status.Resource{
		Group: groupHelm, Version: "v2", Kind: kindHR, Namespace: nsFlux, Name: "redis",
		Status: "Failed", Reason: "InstallFailed", Message: "timed out",
	}
	tests := []struct {
		name string
		r    cli.TraceReport
		want string
	}{
		{
			name: "miss",
			r:    cli.TraceReport{Object: status.Resource{Kind: kindConfigMap, Namespace: "default", Name: "x", Status: statCurrent}},
			want: "✔ ConfigMap/default/x  Current\nnot a member of any milestone\n",
		},
		{
			name: "miss with unchecked kinds",
			r: cli.TraceReport{
				Object:    status.Resource{Kind: kindConfigMap, Namespace: "default", Name: "x", Status: statCurrent},
				Unchecked: []string{kindClusterMilestone},
			},
			want: "✔ ConfigMap/default/x  Current\nnot a member of any milestone (some owner kinds could not be checked)\n",
		},
		{
			name: "hits",
			r: cli.TraceReport{Object: obj, Memberships: []cli.Membership{
				{
					Owner:      membership.OwnerRef{Kind: kindMilestone, Namespace: nsFlux, Name: nameWave0},
					Dependency: depHelm, OwnerReady: metav1.ConditionFalse, OwnerReason: "DependenciesNotReady",
					Blocking: true,
				},
				{
					Owner:      membership.OwnerRef{Kind: kindClusterMilestone, Name: namePlatform},
					Dependency: "releases", OwnerReady: metav1.ConditionTrue, OwnerReason: "AllDependenciesReady",
					OwnerStale: true,
				},
			}},
			want: "" +
				"✗ HelmRelease.helm.toolkit.fluxcd.io/flux-system/redis  Failed  InstallFailed: timed out\n" +
				"\n" +
				"OWNER                          DEPENDENCY     OWNER READY    OWNER REASON           BLOCKING\n" +
				"Milestone/flux-system/wave-0   helmreleases   False          DependenciesNotReady   yes\n" +
				"ClusterMilestone/platform      releases       True (stale)   AllDependenciesReady   no\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := cli.PrintTrace(&buf, tt.r, cli.Style{}); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}
