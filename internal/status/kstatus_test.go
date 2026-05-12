/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package status_test

import (
	"testing"

	"github.com/isometry/milestone-operator/internal/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func newDeploymentReady(replicas, ready int64) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("apps/v1")
	u.SetKind("Deployment")
	u.SetNamespace("default")
	u.SetName("d1")
	u.SetGeneration(1)
	_ = unstructured.SetNestedField(u.Object, replicas, "spec", "replicas")
	_ = unstructured.SetNestedField(u.Object, int64(1), keyStatus, "observedGeneration")
	_ = unstructured.SetNestedField(u.Object, ready, keyStatus, "updatedReplicas")
	_ = unstructured.SetNestedField(u.Object, ready, keyStatus, "readyReplicas")
	_ = unstructured.SetNestedField(u.Object, ready, keyStatus, "availableReplicas")
	_ = unstructured.SetNestedField(u.Object, ready, keyStatus, "replicas")
	_ = unstructured.SetNestedSlice(u.Object, []any{
		map[string]any{keyType: "Available", keyStatus: statusTrue},
		map[string]any{keyType: "Progressing", keyStatus: statusTrue, keyReason: "NewReplicaSetAvailable"},
	}, keyStatus, "conditions")
	return u
}

func TestCompute_DeploymentCurrent(t *testing.T) {
	u := newDeploymentReady(3, 3)
	got := status.Compute(u)
	if got.Status != "Current" {
		t.Errorf("Status = %q, want Current; reason=%q msg=%q", got.Status, got.Reason, got.Message)
	}
	if got.Group != "apps" || got.Version != "v1" || got.Kind != "Deployment" {
		t.Errorf("identity = (%q,%q,%q), want (apps,v1,Deployment)", got.Group, got.Version, got.Kind)
	}
	if got.Namespace != "default" || got.Name != "d1" {
		t.Errorf("namespace/name = (%q,%q), want (default,d1)", got.Namespace, got.Name)
	}
}

func TestCompute_DeploymentInProgress(t *testing.T) {
	u := newDeploymentReady(3, 1) // not all replicas ready
	got := status.Compute(u)
	if got.Status != "InProgress" {
		t.Errorf("Status = %q, want InProgress", got.Status)
	}
}

func TestCompute_Terminating(t *testing.T) {
	u := newDeploymentReady(3, 3)
	now := metav1.Now()
	u.SetDeletionTimestamp(&now)
	got := status.Compute(u)
	if got.Status != "Terminating" {
		t.Errorf("Status = %q, want Terminating", got.Status)
	}
}

func TestCompute_CustomResourceReadyTrue(t *testing.T) {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("kustomize.toolkit.fluxcd.io/v1")
	u.SetKind("Kustomization")
	u.SetNamespace("flux-system")
	u.SetName("k1")
	u.SetGeneration(2)
	_ = unstructured.SetNestedField(u.Object, int64(2), keyStatus, "observedGeneration")
	_ = unstructured.SetNestedSlice(u.Object, []any{
		map[string]any{keyType: "Ready", keyStatus: statusTrue, keyReason: "ReconciliationSucceeded"},
	}, keyStatus, "conditions")
	got := status.Compute(u)
	if got.Status != "Current" {
		t.Errorf("Status = %q, want Current", got.Status)
	}
}

func TestCompute_CustomResourceReadyFalse(t *testing.T) {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("kustomize.toolkit.fluxcd.io/v1")
	u.SetKind("Kustomization")
	u.SetNamespace("flux-system")
	u.SetName("k1")
	u.SetGeneration(2)
	_ = unstructured.SetNestedField(u.Object, int64(2), keyStatus, "observedGeneration")
	_ = unstructured.SetNestedSlice(u.Object, []any{
		map[string]any{keyType: "Ready", keyStatus: "False", keyReason: "BuildFailed", "message": "kustomize build failed"},
	}, keyStatus, "conditions")
	got := status.Compute(u)
	// kstatus reports a False Ready as InProgress (still working towards readiness).
	if got.Status != "InProgress" {
		t.Errorf("Status = %q, want InProgress (kstatus convention for Ready=False); reason=%q msg=%q", got.Status, got.Reason, got.Message)
	}
	if got.Message == "" {
		t.Errorf("expected non-empty Message when Ready=False has a message")
	}
}
