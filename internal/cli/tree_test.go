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
	"strings"
	"testing"
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/cli"
	"github.com/isometry/milestone-operator/internal/membership"
	"github.com/isometry/milestone-operator/internal/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	nsFlux      = "flux-system"
	kindKust    = "Kustomization"
	kindHR      = "HelmRelease"
	groupKust   = "kustomize.toolkit.fluxcd.io"
	groupHelm   = "helm.toolkit.fluxcd.io"
	selWave0    = "wave=0"
	statCurrent = "Current"
)

func member(kind, name, st string, blocking bool, mod ...func(*membership.Member)) membership.Member {
	m := membership.Member{
		Resource: status.Resource{Kind: kind, Namespace: nsFlux, Name: name, Status: st},
		Blocking: blocking,
	}
	for _, f := range mod {
		f(&m)
	}
	return m
}

func suspendedMember(m *membership.Member) { m.Suspended = true }

func depStatus(name, kind, group, version string, ready metav1.ConditionStatus, reason string, current, total int32) apiv1.DependencyStatus {
	return apiv1.DependencyStatus{
		Name: name, Kind: kind, Group: group, Version: version, Ready: ready, Reason: reason,
		Summary: apiv1.Summary{Current: current, Total: total},
	}
}

func sampleReport() membership.Report {
	return membership.Report{
		Owner:    membership.OwnerRef{Kind: kindMilestone, Namespace: nsFlux, Name: nameWave0, Generation: 3},
		Recorded: membership.RecordedStatus{ObservedGeneration: 3, LastEvaluatedTime: metav1.NewTime(now.Add(-42 * time.Second))},
		Ready:    metav1.ConditionFalse,
		Reason:   apiv1.ReasonDependenciesNotReady,
		Dependencies: []membership.DependencyReport{
			{
				Name:   "kustomizations",
				Target: membership.TargetRef{Group: groupKust, Kind: kindKust, Selector: selWave0},
				Status: depStatus("kustomizations", kindKust, groupKust, "v1", metav1.ConditionTrue, apiv1.ReasonAllResourcesReady, 2, 2),
				Members: []membership.Member{
					member(kindKust, "infra", statCurrent, false),
					member(kindKust, "monitoring", statCurrent, false, suspendedMember),
				},
			},
			{
				Name:   depHelm,
				Target: membership.TargetRef{Group: groupHelm, Version: "v2", Kind: kindHR, Selector: selWave0},
				Status: depStatus(depHelm, kindHR, groupHelm, "v2", metav1.ConditionFalse, apiv1.ReasonResourcesNotReady, 1, 2),
				Recorded: &apiv1.DependencyStatus{
					Name: depHelm, Ready: metav1.ConditionTrue, Reason: apiv1.ReasonAllResourcesReady,
				},
				Drift: true,
				Members: []membership.Member{
					member(kindHR, "podinfo", statCurrent, false),
					member(kindHR, "redis", "Failed", true, func(m *membership.Member) {
						m.Reason, m.Message = "InstallFailed", "timed out waiting for condition"
					}),
				},
			},
			{
				Name:   "gated",
				Target: membership.TargetRef{Group: groupKust, Kind: kindKust, Selector: "gate=true"},
				Status: depStatus("gated", kindKust, groupKust, "v1", metav1.ConditionFalse, apiv1.ReasonResourcesSuspended, 0, 1),
				Members: []membership.Member{
					member(kindKust, "paused", statCurrent, true, suspendedMember),
				},
			},
			{
				Name:   "optional",
				Target: membership.TargetRef{Kind: kindConfigMap, Namespaces: []string{"a", "b"}},
				Status: depStatus("optional", "ConfigMap", "", "v1", metav1.ConditionTrue, apiv1.ReasonEmptySet, 0, 0),
			},
			{
				Name:   "widgets",
				Target: membership.TargetRef{Group: "example.com", Kind: "Widget", NamespaceSelector: "env=prod"},
				Status: depStatus("widgets", "Widget", "example.com", "", metav1.ConditionUnknown, apiv1.ReasonGVKNotEstablished, 0, 0),
				Error:  "Widget.example.com is not served",
			},
			{
				Name:      "secrets",
				Target:    membership.TargetRef{Kind: "Secret", Selector: selWave0},
				Status:    depStatus("secrets", "Secret", "", "v1", metav1.ConditionUnknown, apiv1.ReasonListFailed, 0, 0),
				Error:     `secrets is forbidden: User "me" cannot list resource "secrets"`,
				Forbidden: true,
			},
		},
		EvaluatedAt: metav1.NewTime(now),
	}
}

func TestRenderTree(t *testing.T) {
	want := strings.Join([]string{
		"Milestone/flux-system/wave-0  Ready=False  DependenciesNotReady  (evaluated 42s ago)",
		"├── ✔ kustomizations  Kustomization.kustomize.toolkit.fluxcd.io/v1  wave=0  2/2  AllResourcesReady",
		"│   ├── ✔ flux-system/infra        Current",
		"│   └── ⏸ flux-system/monitoring   Current (suspended)",
		"├── ✗ helmreleases  HelmRelease.helm.toolkit.fluxcd.io/v2  wave=0  1/2  ResourcesNotReady",
		"│   ├── ⚠ live differs from recorded (recorded: Ready=True AllResourcesReady)",
		"│   ├── ✔ flux-system/podinfo      Current",
		"│   └── ✗ flux-system/redis        Failed  InstallFailed: timed out waiting for condition",
		"├── ⏸ gated  Kustomization.kustomize.toolkit.fluxcd.io/v1  gate=true  0/1  ResourcesSuspended",
		"│   └── ⏸ flux-system/paused       Current (suspended)  Suspended: spec.suspend is true",
		"├── ✔ optional  ConfigMap/v1  <all>  namespaces=a,b  0/0  EmptySet",
		"│   └── no matching resources (emptySetPolicy: Ready)",
		"├── ? widgets  Widget.example.com  <all>  namespaceSelector=env=prod  GVKNotEstablished: Widget.example.com is not served",
		"└── ? secrets  Secret/v1  wave=0  cannot check: forbidden (your credentials)",
		"",
	}, "\n")
	r := sampleReport()

	var buf bytes.Buffer
	if err := cli.RenderTree(&buf, r, cli.TreeOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderTree_NotReadyPrunesHealthyMembers(t *testing.T) {
	r := sampleReport()
	r.Dependencies = r.Dependencies[:2]
	var buf bytes.Buffer
	if err := cli.RenderTree(&buf, r, cli.TreeOptions{NotReady: true}); err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		"Milestone/flux-system/wave-0  Ready=False  DependenciesNotReady  (evaluated 42s ago)",
		"├── ✔ kustomizations  Kustomization.kustomize.toolkit.fluxcd.io/v1  wave=0  2/2  AllResourcesReady",
		"└── ✗ helmreleases  HelmRelease.helm.toolkit.fluxcd.io/v2  wave=0  1/2  ResourcesNotReady",
		"    ├── ⚠ live differs from recorded (recorded: Ready=True AllResourcesReady)",
		"    └── ✗ flux-system/redis   Failed  InstallFailed: timed out waiting for condition",
		"",
	}, "\n")
	if got := buf.String(); got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderTree_OwnerLine(t *testing.T) {
	tests := []struct {
		name string
		mod  func(*membership.Report)
		want string
	}{
		{
			name: "cluster-scoped, never evaluated",
			mod: func(r *membership.Report) {
				r.Owner = membership.OwnerRef{Kind: kindClusterMilestone, Name: namePlatform, Generation: 1}
				r.Recorded = membership.RecordedStatus{}
				r.Ready, r.Reason = metav1.ConditionUnknown, apiv1.ReasonEmptySet
			},
			want: "ClusterMilestone/platform  Ready=Unknown  EmptySet  (never evaluated by the operator)\n",
		},
		{
			name: "stale",
			mod: func(r *membership.Report) {
				r.Owner.Generation = 4
				r.Recorded.Stale = true
			},
			want: "Milestone/flux-system/wave-0  Ready=False  DependenciesNotReady  (evaluated 42s ago, stale: generation 4 not yet observed)\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := sampleReport()
			r.Dependencies = nil
			tt.mod(&r)
			var buf bytes.Buffer
			if err := cli.RenderTree(&buf, r, cli.TreeOptions{}); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestRenderTree_Colour(t *testing.T) {
	r := sampleReport()
	r.Dependencies = r.Dependencies[:1]
	var buf bytes.Buffer
	if err := cli.RenderTree(&buf, r, cli.TreeOptions{Style: cli.Style{Color: true}}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "\x1b[32m✔\x1b[0m") {
		t.Errorf("expected a green check, got %q", buf.String())
	}
}
