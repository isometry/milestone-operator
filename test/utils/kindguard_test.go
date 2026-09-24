/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"k8s.io/client-go/tools/clientcmd"
)

const testCluster = "milestone-operator-test-e2e"

func writeKubeconfig(t *testing.T, currentContext, contextCluster string, clusters map[string]string) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("apiVersion: v1\nkind: Config\nclusters:\n")
	for name, server := range clusters {
		b.WriteString("- name: " + name + "\n  cluster:\n    server: " + server + "\n")
	}
	b.WriteString("contexts:\n- name: " + currentContext + "\n  context:\n    cluster: " + contextCluster +
		"\n    user: u\nusers:\n- name: u\n  user: {}\n")
	if currentContext != "" {
		b.WriteString("current-context: " + currentContext + "\n")
	}
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func kindKubeconfig(t *testing.T, cluster, server string) string {
	t.Helper()
	name := "kind-" + cluster
	return writeKubeconfig(t, name, name, map[string]string{name: server})
}

func TestAssertKindContext(t *testing.T) {
	sep := string(os.PathListSeparator)
	kind := func(server string) string { return kindKubeconfig(t, testCluster, server) }
	good := kind("https://127.0.0.1:45678")
	wantKind := "expected kind-" + testCluster
	eks := writeKubeconfig(t, "arn:aws:eks:us-east-1:123456789012:cluster/sandbox", "eks",
		map[string]string{"eks": "https://ABCDEF.gr7.us-east-1.eks.amazonaws.com"})
	noCurrent := writeKubeconfig(t, "", "x", map[string]string{"x": "https://127.0.0.1:1"})
	danglingCtx := writeKubeconfig(t, "kind-"+testCluster, "gone",
		map[string]string{"kind-" + testCluster: "https://127.0.0.1:1"})

	tests := []struct {
		name    string
		path    string
		cluster string
		wantErr string
	}{
		{name: "kind on 127.0.0.1", path: good, cluster: testCluster},
		{name: "kind on localhost", path: kind("https://localhost:6443"), cluster: testCluster},
		{name: "kind on ::1", path: kind("https://[::1]:6443"), cluster: testCluster},
		{name: "empty path", path: "", cluster: testCluster, wantErr: "KUBECONFIG is not set"},
		{name: "empty cluster", path: good, cluster: "", wantErr: "cluster name"},
		{name: "missing file", path: filepath.Join(t.TempDir(), "absent"), cluster: testCluster, wantErr: "does not exist"},
		{name: "multiple files", path: good + sep + good, cluster: testCluster, wantErr: "exactly one file"},
		{name: "default kubeconfig", path: clientcmd.RecommendedHomeFile, cluster: testCluster,
			wantErr: "default kubeconfig"},
		{name: "EKS context", path: eks, cluster: testCluster, wantErr: wantKind},
		{name: "right name, non-loopback", path: kind("https://10.0.0.1:6443"), cluster: testCluster,
			wantErr: "not loopback"},
		{name: "right name, plain http", path: kind("http://127.0.0.1:6443"), cluster: testCluster, wantErr: "https"},
		{name: "wrong cluster suffix", path: kindKubeconfig(t, testCluster+"-other", "https://127.0.0.1:6443"),
			cluster: testCluster, wantErr: wantKind},
		{name: "other kind cluster", path: kindKubeconfig(t, "kind", "https://127.0.0.1:6443"),
			cluster: testCluster, wantErr: wantKind},
		{name: "no current context", path: noCurrent, cluster: testCluster, wantErr: wantKind},
		{name: "context names a missing cluster", path: danglingCtx, cluster: testCluster, wantErr: "no cluster"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AssertKindContext(tt.path, tt.cluster)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestAssertKindContext_DefaultKubeconfigViaLink(t *testing.T) {
	home := t.TempDir()
	defaultPath := filepath.Join(home, ".kube", "config")
	if err := os.MkdirAll(filepath.Dir(defaultPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(kindKubeconfig(t, testCluster, "https://127.0.0.1:1"), defaultPath); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "e2e.kubeconfig")
	if err := os.Symlink(defaultPath, link); err != nil {
		t.Fatal(err)
	}
	err := assertKindContext(link, testCluster, defaultPath)
	if err == nil || !strings.Contains(err.Error(), "default kubeconfig") {
		t.Fatalf("symlink to the default kubeconfig: err = %v", err)
	}
}

func TestKindClusterName(t *testing.T) {
	t.Setenv("KIND_CLUSTER", "")
	if got := KindClusterName(); got != DefaultKindCluster {
		t.Fatalf("empty KIND_CLUSTER: got %q, want %q", got, DefaultKindCluster)
	}
	t.Setenv("KIND_CLUSTER", "custom")
	if got := KindClusterName(); got != "custom" {
		t.Fatalf("got %q, want custom", got)
	}
}

func TestSanitizedEnv(t *testing.T) {
	in := []string{
		"PATH=/bin",
		"KUBECONFIG=/home/me/.kube/config",
		"KUBERNETES_MASTER=https://prod:6443",
		"KUBERNETES_SERVICE_HOST=10.0.0.1",
		"KUBERNETES_SERVICE_PORT=443",
		"HOME=/home/me",
	}
	got := sanitizedEnv(in, "/tmp/e2e.kubeconfig")
	want := []string{"PATH=/bin", "HOME=/home/me", "GO111MODULE=on", "KUBECONFIG=/tmp/e2e.kubeconfig"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRunRefusesWhenUnpinned(t *testing.T) {
	pinnedKubeconfig = ""
	if _, err := Run(nil); err == nil || !strings.Contains(err.Error(), "PinKubeconfig") {
		t.Fatalf("Run without a pinned kubeconfig: err = %v", err)
	}
}

func TestPinKubeconfig(t *testing.T) {
	t.Cleanup(func() { pinnedKubeconfig = "" })

	t.Setenv("KIND_CLUSTER", testCluster)
	t.Setenv("KUBECONFIG", "")
	if err := PinKubeconfig(); err == nil {
		t.Fatal("expected refusal with KUBECONFIG unset")
	}
	if pinnedKubeconfig != "" {
		t.Fatalf("refused pin still set pinnedKubeconfig=%q", pinnedKubeconfig)
	}

	path := kindKubeconfig(t, testCluster, "https://127.0.0.1:45678")
	t.Setenv("KUBECONFIG", path)
	if err := PinKubeconfig(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pinnedKubeconfig != path {
		t.Fatalf("pinnedKubeconfig = %q, want %q", pinnedKubeconfig, path)
	}
}
