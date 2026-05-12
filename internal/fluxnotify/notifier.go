/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package fluxnotify pokes FluxCD parent objects (Kustomization /
// HelmRelease) when a child Milestone or ClusterMilestone's Ready
// condition transitions, so the parent's health checks re-evaluate
// immediately instead of waiting for the next scheduled reconcile.
//
// Parent identity is read from the labels the Flux controllers
// auto-stamp on every managed resource via SetOwnerLabels in
// github.com/fluxcd/pkg/ssa/manager.go.
package fluxnotify

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/isometry/milestone-operator/internal/metrics"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// Labels auto-stamped by kustomize-controller / helm-controller on every
	// resource they apply. See github.com/fluxcd/pkg/ssa/manager.go
	// SetOwnerLabels.
	labelKustomizeName      = "kustomize.toolkit.fluxcd.io/name"
	labelKustomizeNamespace = "kustomize.toolkit.fluxcd.io/namespace"
	labelHelmName           = "helm.toolkit.fluxcd.io/name"
	labelHelmNamespace      = "helm.toolkit.fluxcd.io/namespace"

	// reconcileAnnotation is the opaque-value annotation honoured by both
	// Flux controllers to trigger an on-demand reconcile. Flux dedups via
	// .status.lastHandledReconcileAt, so issuing the same value twice is a
	// harmless no-op on Flux's side.
	reconcileAnnotation = "reconcile.fluxcd.io/requestedAt"

	groupKustomize    = "kustomize.toolkit.fluxcd.io"
	kindKustomization = "Kustomization"
	groupHelm         = "helm.toolkit.fluxcd.io"
	kindHelmRelease   = "HelmRelease"
)

var (
	kustomizationGVK = schema.GroupVersionKind{
		Group:   groupKustomize,
		Version: "v1",
		Kind:    kindKustomization,
	}
	helmReleaseGVK = schema.GroupVersionKind{
		Group:   groupHelm,
		Version: "v2",
		Kind:    kindHelmRelease,
	}
)

// Notifier issues fire-and-forget reconcile-request annotations on the
// FluxCD parent of a Milestone or ClusterMilestone. Constructed once per
// controller (Milestone / ClusterMilestone) so that emitted metrics carry
// the correct controller label.
type Notifier struct {
	Client     client.Client
	Controller string
	Now        func() time.Time
	Log        logr.Logger
}

// NotifyTransition reads obj's labels and pokes each Flux parent found.
// Errors are classified into a metric label and logged at V(1); they are
// never propagated to the caller — a failed Flux poke must not interfere
// with the reconcile pipeline.
//
// A child may carry both label-pairs simultaneously (e.g. a Kustomization
// that wraps a HelmRelease whose template includes a Milestone); in that
// case both parents are poked.
func (n *Notifier) NotifyTransition(ctx context.Context, obj client.Object) {
	if n == nil || obj == nil {
		return
	}
	lbls := obj.GetLabels()
	if len(lbls) == 0 {
		return
	}
	timestamp := n.now().UTC().Format(time.RFC3339Nano)

	if name := lbls[labelKustomizeName]; name != "" {
		n.poke(ctx, kustomizationGVK, lbls[labelKustomizeNamespace], name, timestamp)
	}
	if name := lbls[labelHelmName]; name != "" {
		n.poke(ctx, helmReleaseGVK, lbls[labelHelmNamespace], name, timestamp)
	}
}

func (n *Notifier) poke(ctx context.Context, gvk schema.GroupVersionKind, namespace, name, timestamp string) {
	parent := &unstructured.Unstructured{}
	parent.SetGroupVersionKind(gvk)
	parent.SetNamespace(namespace)
	parent.SetName(name)

	payload := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, reconcileAnnotation, timestamp)
	err := n.Client.Patch(ctx, parent, client.RawPatch(types.MergePatchType, []byte(payload)))
	result := classify(err)
	metrics.FluxNotifyTotal.WithLabelValues(n.Controller, gvk.Kind, result).Inc()
	if err != nil {
		n.Log.V(1).Info("flux notify failed",
			"controller", n.Controller,
			"parentKind", gvk.Kind,
			"parentNamespace", namespace,
			"parentName", name,
			"result", result,
			"err", err)
		return
	}
	n.Log.V(1).Info("flux notify",
		"controller", n.Controller,
		"parentKind", gvk.Kind,
		"parentNamespace", namespace,
		"parentName", name,
		"requestedAt", timestamp)
}

func (n *Notifier) now() time.Time {
	if n.Now == nil {
		return time.Now()
	}
	return n.Now()
}

func classify(err error) string {
	switch {
	case err == nil:
		return metrics.FluxNotifySuccess
	case apierrors.IsNotFound(err):
		return metrics.FluxNotifyNotFound
	case apimeta.IsNoMatchError(err):
		return metrics.FluxNotifyNoMatch
	case apierrors.IsForbidden(err):
		return metrics.FluxNotifyForbidden
	default:
		return metrics.FluxNotifyError
	}
}
