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
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/isometry/milestone-operator/internal/cli"
	"github.com/isometry/milestone-operator/internal/version"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

func TestInvocation(t *testing.T) {
	tests := []struct {
		argv     []string
		wantUse  string
		wantArgs []string
	}{
		{nil, defaultName, nil},
		{[]string{"milestonectl", cmdGet}, defaultName, []string{cmdGet}},
		{[]string{"/usr/local/bin/kubectl-milestone", cmdTree}, pluginUse, []string{cmdTree}},
		{[]string{"/opt/bin/kubectl-milestone.exe"}, pluginUse, nil},
		{[]string{"kubectl_complete-milestone", cmdGet, ""}, pluginUse, []string{"__complete", cmdGet, ""}},
		{[]string{"renamed-binary"}, defaultName, nil},
	}
	for _, tt := range tests {
		use, args := invocation(tt.argv)
		if use != tt.wantUse || !reflect.DeepEqual(args, tt.wantArgs) {
			t.Errorf("invocation(%q) = %q %q; want %q %q", tt.argv, use, args, tt.wantUse, tt.wantArgs)
		}
	}
}

func runArgv(h *harness, argv ...string) int {
	h.out.Reset()
	h.errOut.Reset()
	return run(context.Background(), argv, h.app())
}

func TestRun_PluginDisplayName(t *testing.T) {
	h := newHarness(t, nil)
	if code := runArgv(h, "kubectl-milestone", cmdGet, "--help"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.errOut)
	}
	out := h.out.String()
	for _, w := range []string{"kubectl milestone get [command]", "kubectl milestone get all -A"} {
		if !strings.Contains(out, w) {
			t.Errorf("missing %q in:\n%s", w, out)
		}
	}
	if strings.Contains(out, "kubectl-milestone") || strings.Contains(out, defaultName) {
		t.Errorf("plugin help leaks the binary name:\n%s", out)
	}
}

func TestRun_PluginCompletion(t *testing.T) {
	h := newHarness(t, standardObjects(t), standardNamespaces()...)
	tests := []struct {
		args []string
		want []string
	}{
		{[]string{"tr"}, []string{cmdTrace, cmdTree}},
		{[]string{cmdGet, argMile, ""}, []string{nameWave0, nameWave1}},
		{[]string{cmdTree, argCmile, ""}, []string{namePlat}},
		{[]string{cmdTrace, "ks/"}, []string{"ks/apps", "ks/broken", "ks/infra"}},
		{[]string{cmdTrace, "ks", ""}, []string{"apps", "broken", nameInfra}},
		{[]string{cmdGet, argMile, "-n", ""}, []string{nsApps, nsFlux}},
		{[]string{cmdGet, argMile, "-o", ""}, []string{"table", cli.OutputWide, "json", "yaml"}},
	}
	for _, tt := range tests {
		argv := append([]string{"kubectl_complete-milestone"}, tt.args...)
		if code := runArgv(h, argv...); code != 0 {
			t.Fatalf("%q: exit %d: %s", tt.args, code, h.errOut)
		}
		lines := strings.Split(strings.TrimSpace(h.out.String()), "\n")
		got := lines[:len(lines)-1] // the last line is cobra's directive
		for i, l := range got {
			got[i], _, _ = strings.Cut(l, "\t")
		}
		for _, w := range tt.want {
			if !slices.Contains(got, w) {
				t.Errorf("%q: completions %q lack %q", tt.args, got, w)
			}
		}
	}
}

func TestRun_ErrorsAreConcise(t *testing.T) {
	h := newHarness(t, nil)
	if code := runArgv(h, defaultName, "bogus"); code != 1 {
		t.Errorf("exit = %d", code)
	}
	got := h.errOut.String()
	if !strings.HasPrefix(got, `✗ unknown command "bogus"`) || strings.Contains(got, "Usage:") {
		t.Errorf("stderr = %q", got)
	}
}

func withBuildInfo(t *testing.T, v, c, d string) {
	t.Helper()
	oldV, oldC, oldD := version.Version, version.Commit, version.Date
	version.Version, version.Commit, version.Date = v, c, d
	t.Cleanup(func() { version.Version, version.Commit, version.Date = oldV, oldC, oldD })
}

func TestVersion(t *testing.T) {
	withBuildInfo(t, "v1.2.3", "abc1234", "2026-09-23T12:00:00Z")
	operator := deployment("milestone-system", "milestone-operator-controller-manager",
		"ghcr.io/isometry/milestone-operator:v1.2.3", map[string]string{"app.kubernetes.io/name": "milestone-operator"})
	unrelated := deployment(nsApps, "web", "nginx", map[string]string{"app.kubernetes.io/name": "web"})

	h := newHarness(t, nil, operator, unrelated)
	got := h.mustRun(t, "version", "--client")
	wantClient := "client: v1.2.3 (commit abc1234, built 2026-09-23T12:00:00Z, go"
	if !strings.HasPrefix(got, wantClient) || strings.Contains(got, "operator") {
		t.Errorf("--client = %q", got)
	}

	got = h.mustRun(t, "version")
	want := "operator: milestone-system/milestone-operator-controller-manager ghcr.io/isometry/milestone-operator:v1.2.3\n"
	if !strings.HasSuffix(got, want) || strings.Contains(got, "nginx") {
		t.Errorf("version = %q", got)
	}

	var info versionInfo
	if err := json.Unmarshal([]byte(h.mustRun(t, "version", "-o", "json")), &info); err != nil {
		t.Fatal(err)
	}
	if info.Client.Version != "v1.2.3" || len(info.Operators) != 1 || info.OperatorError != "" {
		t.Errorf("json = %+v", info)
	}
}

func TestVersion_OperatorUnavailableIsNotAnError(t *testing.T) {
	withBuildInfo(t, "", "", "")
	h := newHarness(t, nil)
	got := h.mustRun(t, "version")
	want := "operator: not found (no deployment labelled app.kubernetes.io/name=milestone-operator)\n"
	if !strings.HasPrefix(got, "client: ") || !strings.HasSuffix(got, want) {
		t.Errorf("version = %q", got)
	}

	h.clients.kube.PrependReactor(verbList, "deployments", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(appsv1.Resource("deployments"), "", nil)
	})
	if got := h.mustRun(t, "version"); !strings.Contains(got, "operator: unavailable (forbidden") {
		t.Errorf("version = %q", got)
	}
}
