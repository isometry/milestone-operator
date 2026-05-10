/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	envtestCfg     *rest.Config
	envtestClient  client.Client
	envtestEnv     *envtest.Environment
	envtestScheme  = runtime.NewScheme()
	envtestStopper context.CancelFunc

	widgetCRD = &apiextv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "widgets.test.as-code.io"},
		Spec: apiextv1.CustomResourceDefinitionSpec{
			Group: groupTestAsCode,
			Names: apiextv1.CustomResourceDefinitionNames{
				Plural:   "widgets",
				Singular: "widget",
				Kind:     kindWidget,
				ListKind: "WidgetList",
			},
			Scope: apiextv1.NamespaceScoped,
			Versions: []apiextv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Subresources: &apiextv1.CustomResourceSubresources{Status: &apiextv1.CustomResourceSubresourceStatus{}},
				Schema: &apiextv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextv1.JSONSchemaProps{
						Type: schemaTypeObject,
						Properties: map[string]apiextv1.JSONSchemaProps{
							"spec":           {Type: schemaTypeObject, XPreserveUnknownFields: ptrBool(true)},
							schemaPropStatus: {Type: schemaTypeObject, XPreserveUnknownFields: ptrBool(true)},
						},
					},
				},
			}},
		},
	}

	widgetGVK = schema.GroupVersionKind{Group: groupTestAsCode, Version: "v1", Kind: kindWidget}
)

func ptrBool(b bool) *bool { return &b }

func TestMain(m *testing.M) {
	if !envtestAvailable() {
		// Allow the package's pure unit tests to run without envtest binaries
		// installed. The Test* envtest scenarios will be skipped individually.
		os.Exit(m.Run())
	}
	if err := setupEnvtest(); err != nil {
		_, _ = os.Stderr.WriteString("envtest setup failed: " + err.Error() + "\n")
		os.Exit(1)
	}
	code := m.Run()
	teardownEnvtest()
	os.Exit(code)
}

func envtestAvailable() bool {
	if os.Getenv("KUBEBUILDER_ASSETS") != "" {
		return true
	}
	for _, e := range readDirOrEmpty(filepath.Join("..", "..", "bin", "k8s")) {
		if e.IsDir() {
			return true
		}
	}
	return false
}

func readDirOrEmpty(p string) []os.DirEntry {
	entries, err := os.ReadDir(p)
	if err != nil {
		return nil
	}
	return entries
}

func envtestBinaryDir() string {
	if d := os.Getenv("KUBEBUILDER_ASSETS"); d != "" {
		return d
	}
	base := filepath.Join("..", "..", "bin", "k8s")
	for _, e := range readDirOrEmpty(base) {
		if e.IsDir() {
			return filepath.Join(base, e.Name())
		}
	}
	return ""
}

func setupEnvtest() error {
	logf.SetLogger(zap.New(zap.UseDevMode(true)))

	utilruntime.Must(clientgoscheme.AddToScheme(envtestScheme))
	utilruntime.Must(apiv1.AddToScheme(envtestScheme))
	utilruntime.Must(apiextv1.AddToScheme(envtestScheme))

	envtestEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: envtestBinaryDir(),
	}

	cfg, err := envtestEnv.Start()
	if err != nil {
		return err
	}
	envtestCfg = cfg

	cl, err := client.New(cfg, client.Options{Scheme: envtestScheme})
	if err != nil {
		return err
	}
	envtestClient = cl

	// Install the test Widget CRD used by integration scenarios.
	if err := cl.Create(context.Background(), widgetCRD.DeepCopy()); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	// Wait briefly for the Widget CRD to become Established.
	if err := waitForCRDEstablished(context.Background(), cl, "widgets.test.as-code.io", 30*time.Second); err != nil {
		return err
	}
	return nil
}

func teardownEnvtest() {
	if envtestStopper != nil {
		envtestStopper()
	}
	if envtestEnv != nil {
		_ = envtestEnv.Stop()
	}
}

func waitForCRDEstablished(ctx context.Context, c client.Client, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	crd := &apiextv1.CustomResourceDefinition{}
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKey{Name: name}, crd); err == nil {
			for _, cond := range crd.Status.Conditions {
				if cond.Type == apiextv1.Established && cond.Status == apiextv1.ConditionTrue {
					return nil
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return apierrors.NewTimeoutError("CRD never reached Established=True", 0)
}

// newWidget returns an unstructured Widget object for testing. Pass status
// conditions inline; pass nil for "no status" (kstatus reports Current).
func newWidget(ns, name string, ready string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(widgetGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetGeneration(1)
	if ready != "" {
		_ = unstructured.SetNestedField(u.Object, int64(1), schemaPropStatus, "observedGeneration")
		_ = unstructured.SetNestedSlice(u.Object, []any{
			map[string]any{keyType: apiv1.ConditionReady, schemaPropStatus: ready, keyReason: "Test"},
		}, schemaPropStatus, "conditions")
	}
	return u
}

// requiresEnvtest skips a test when the envtest binaries aren't available.
func requiresEnvtest(t *testing.T) {
	t.Helper()
	if envtestCfg == nil {
		t.Skip("envtest not initialized; run `make setup-envtest` and re-test")
	}
}
