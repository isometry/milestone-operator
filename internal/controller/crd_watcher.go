/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	"github.com/isometry/echelon-operator/internal/discovery"
	"github.com/isometry/echelon-operator/internal/metrics"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// CRDWatcher watches CustomResourceDefinitions and, on every Established=True
// transition, invalidates the discovery cache and enqueues every Echelon and
// ClusterEchelon that references the now-Established (group, kind). Spec
// references are resolved by listing both CRDs and matching by group+kind.
//
// This complements the per-reconcile retry-with-backoff that would otherwise
// be needed: instead of polling, we wake stalled owners precisely when the
// CRD shows up.
type CRDWatcher struct {
	client.Client
	Resolver       discovery.Resolver
	EchelonEvents  chan<- event.GenericEvent
	CEchelonEvents chan<- event.GenericEvent
}

// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

// Reconcile is invoked for every CRD change. We only react to Established=True
// CRDs; absent or NotEstablished CRDs are ignored.
func (w *CRDWatcher) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	crd := &apiextv1.CustomResourceDefinition{}
	if err := w.Get(ctx, req.NamespacedName, crd); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	if !crdEstablished(crd) {
		return ctrl.Result{}, nil
	}

	group := crd.Spec.Group
	kind := crd.Spec.Names.Kind
	metrics.CRDEstablishedEvents.WithLabelValues(group, kind).Inc()

	// New CRD: clear the discovery cache so the next reconcile sees it.
	if w.Resolver != nil {
		w.Resolver.Invalidate()
	}

	// Wake every Echelon and ClusterEchelon that references this kind.
	if err := w.wakeEchelons(ctx, group, kind); err != nil {
		log.Error(err, "wake echelons", "group", group, "kind", kind)
	}
	if err := w.wakeClusterEchelons(ctx, group, kind); err != nil {
		log.Error(err, "wake clusterechelons", "group", group, "kind", kind)
	}
	return ctrl.Result{}, nil
}

func (w *CRDWatcher) wakeEchelons(ctx context.Context, group, kind string) error {
	list := &apiv1.EchelonList{}
	if err := w.List(ctx, list); err != nil {
		return err
	}
	for i := range list.Items {
		e := &list.Items[i]
		if !ownsKind(e.Spec.Members, group, kind) {
			continue
		}
		w.EchelonEvents <- event.GenericEvent{Object: e}
		metrics.OwnersWoken.WithLabelValues("crd_established").Inc()
	}
	return nil
}

func (w *CRDWatcher) wakeClusterEchelons(ctx context.Context, group, kind string) error {
	list := &apiv1.ClusterEchelonList{}
	if err := w.List(ctx, list); err != nil {
		return err
	}
	for i := range list.Items {
		ce := &list.Items[i]
		if !ownsClusterKind(ce.Spec.Members, group, kind) {
			continue
		}
		w.CEchelonEvents <- event.GenericEvent{Object: ce}
		metrics.OwnersWoken.WithLabelValues("crd_established").Inc()
	}
	return nil
}

// SetupWithManager wires the CRD watcher.
func (w *CRDWatcher) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&apiextv1.CustomResourceDefinition{}).
		Named("crd-watcher").
		Complete(w)
}

func crdEstablished(crd *apiextv1.CustomResourceDefinition) bool {
	for _, c := range crd.Status.Conditions {
		if c.Type == apiextv1.Established && c.Status == apiextv1.ConditionTrue {
			return true
		}
	}
	return false
}

func ownsKind(members map[string]apiv1.MemberSpec, group, kind string) bool {
	for _, m := range members {
		if m.Group == group && m.Kind == kind {
			return true
		}
	}
	return false
}

func ownsClusterKind(members map[string]apiv1.ClusterMemberSpec, group, kind string) bool {
	for _, m := range members {
		if m.Group == group && m.Kind == kind {
			return true
		}
	}
	return false
}

// Avoid an unused-import nag if the struct grows; metav1 is referenced by RBAC
// markers that controller-gen rewrites during code generation.
var _ = metav1.Now
