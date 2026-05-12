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

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/metrics"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// CRDWatcher watches CustomResourceDefinitions and, on every Established=True
// transition, invalidates the discovery cache and enqueues every Milestone
// and ClusterMilestone that references the now-Established (group, kind).
// Spec references are resolved by listing both CRDs and matching by group+kind.
//
// This complements the per-reconcile retry-with-backoff that would otherwise
// be needed: instead of polling, we wake stalled owners precisely when the
// CRD shows up.
type CRDWatcher struct {
	client.Client
	Resolver         discovery.Resolver
	MilestoneEvents  chan<- event.GenericEvent
	CMilestoneEvents chan<- event.GenericEvent
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

	// Wake every Milestone and ClusterMilestone that references this kind.
	if err := w.wakeMilestones(ctx, group, kind); err != nil {
		log.Error(err, "wake milestones", "group", group, "kind", kind)
	}
	if err := w.wakeClusterMilestones(ctx, group, kind); err != nil {
		log.Error(err, "wake clustermilestones", "group", group, "kind", kind)
	}
	return ctrl.Result{}, nil
}

func (w *CRDWatcher) wakeMilestones(ctx context.Context, group, kind string) error {
	list := &apiv1.MilestoneList{}
	if err := w.List(ctx, list); err != nil {
		return err
	}
	for i := range list.Items {
		m := &list.Items[i]
		if !ownsKind(m.Spec.DependsOn, group, kind) {
			continue
		}
		w.MilestoneEvents <- event.GenericEvent{Object: m}
		metrics.OwnersWoken.WithLabelValues("crd_established").Inc()
	}
	return nil
}

func (w *CRDWatcher) wakeClusterMilestones(ctx context.Context, group, kind string) error {
	list := &apiv1.ClusterMilestoneList{}
	if err := w.List(ctx, list); err != nil {
		return err
	}
	for i := range list.Items {
		cm := &list.Items[i]
		if !ownsClusterKind(cm.Spec.DependsOn, group, kind) {
			continue
		}
		w.CMilestoneEvents <- event.GenericEvent{Object: cm}
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

func ownsKind(deps []apiv1.DependencyRef, group, kind string) bool {
	for i := range deps {
		if deps[i].Target.Group == group && deps[i].Target.Kind == kind {
			return true
		}
	}
	return false
}

func ownsClusterKind(deps []apiv1.ClusterDependencyRef, group, kind string) bool {
	for i := range deps {
		if deps[i].Target.Group == group && deps[i].Target.Kind == kind {
			return true
		}
	}
	return false
}

// Avoid an unused-import nag if the struct grows; metav1 is referenced by RBAC
// markers that controller-gen rewrites during code generation.
var _ = metav1.Now
