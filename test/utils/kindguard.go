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
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"k8s.io/client-go/tools/clientcmd"
)

// DefaultKindCluster matches the Makefile's KIND_CLUSTER default.
const DefaultKindCluster = "milestone-operator-test-e2e"

// pinnedKubeconfig is the only kubeconfig child processes may see; Run
// refuses to start anything until PinKubeconfig has vetted it.
var pinnedKubeconfig string

// clusterRedirectEnv are variables that could steer kubectl or client-go
// somewhere other than the pinned kubeconfig.
var clusterRedirectEnv = []string{
	"KUBECONFIG",
	"KUBERNETES_MASTER",
	"KUBERNETES_SERVICE_HOST",
	"KUBERNETES_SERVICE_PORT",
}

// KindClusterName is the kind cluster the suite may touch: KIND_CLUSTER as
// `make test-e2e` sets it, else the Makefile default.
func KindClusterName() string {
	if v := os.Getenv("KIND_CLUSTER"); v != "" {
		return v
	}
	return DefaultKindCluster
}

// AssertKindContext returns nil only when kubeconfigPath is a single,
// dedicated kubeconfig file whose current context is kind-<cluster> and whose
// API server is https on loopback. It inspects the file only; it never
// contacts a cluster.
func AssertKindContext(kubeconfigPath, cluster string) error {
	return assertKindContext(kubeconfigPath, cluster, clientcmd.RecommendedHomeFile)
}

func assertKindContext(kubeconfigPath, cluster, defaultKubeconfig string) error {
	if kubeconfigPath == "" {
		return errors.New("KUBECONFIG is not set: the e2e suite only runs against the dedicated kubeconfig " +
			"`make test-e2e` writes (bin/e2e.kubeconfig)")
	}
	if cluster == "" {
		return errors.New("kind cluster name is empty")
	}
	// A path list would let client-go merge in, and take current-context
	// from, a file other than the one vetted here.
	if strings.ContainsRune(kubeconfigPath, os.PathListSeparator) {
		return fmt.Errorf("KUBECONFIG %q must name exactly one file", kubeconfigPath)
	}
	abs, err := filepath.Abs(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("resolving KUBECONFIG %q: %w", kubeconfigPath, err)
	}
	defaultRefusal := fmt.Errorf("KUBECONFIG %q is the default kubeconfig; e2e needs a dedicated one", abs)
	if abs == filepath.Clean(defaultKubeconfig) {
		return defaultRefusal
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return fmt.Errorf("KUBECONFIG %q does not exist or is unreadable: %w", abs, err)
	}
	if dfi, err := os.Stat(defaultKubeconfig); err == nil && os.SameFile(fi, dfi) {
		return defaultRefusal
	}

	cfg, err := clientcmd.LoadFromFile(abs)
	if err != nil {
		return fmt.Errorf("loading kubeconfig %q: %w", abs, err)
	}

	want := "kind-" + cluster
	if cfg.CurrentContext != want {
		return fmt.Errorf("refusing e2e against context %q in %q: expected %s", cfg.CurrentContext, abs, want)
	}
	kctx, ok := cfg.Contexts[cfg.CurrentContext]
	if !ok {
		return fmt.Errorf("context %q is not defined in %q", cfg.CurrentContext, abs)
	}
	kcluster, ok := cfg.Clusters[kctx.Cluster]
	if !ok {
		return fmt.Errorf("context %q references no cluster %q in %q", cfg.CurrentContext, kctx.Cluster, abs)
	}
	server, err := url.Parse(kcluster.Server)
	if err != nil {
		return fmt.Errorf("unparseable API server %q: %w", kcluster.Server, err)
	}
	if server.Scheme != "https" {
		return fmt.Errorf("refusing e2e against API server %q: expected https", kcluster.Server)
	}
	switch server.Hostname() {
	case "127.0.0.1", "localhost", "::1":
	default:
		return fmt.Errorf("refusing e2e against API server %q: host is not loopback", kcluster.Server)
	}
	return nil
}

// PinKubeconfig vets $KUBECONFIG with AssertKindContext for KindClusterName
// and, on success, makes it the kubeconfig every Run child process gets.
func PinKubeconfig() error {
	path := os.Getenv("KUBECONFIG")
	if err := AssertKindContext(path, KindClusterName()); err != nil {
		return err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	pinnedKubeconfig = abs
	return nil
}

// sanitizedEnv drops anything that could redirect kubectl/client-go and
// appends the pinned kubeconfig, so the child never falls back to ambient
// configuration.
func sanitizedEnv(environ []string, kubeconfig string) []string {
	env := make([]string, 0, len(environ)+2)
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if slices.Contains(clusterRedirectEnv, name) {
			continue
		}
		env = append(env, kv)
	}
	return append(env, "GO111MODULE=on", "KUBECONFIG="+kubeconfig)
}
