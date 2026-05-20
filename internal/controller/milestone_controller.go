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
	"github.com/isometry/milestone-operator/internal/watcher"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MilestoneReconciler is the thin per-CRD wrapper around the generic
// Reconciler for namespaced Milestone objects.
type MilestoneReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Reconciler *Reconciler[*apiv1.Milestone]
	// EventSource bridges informer and CRD-watcher wakes directly into this
	// controller's workqueue. controller-runtime calls EventSource.Start at
	// controller construction to capture the queue.
	EventSource *watcher.EnqueueSource
}

// +kubebuilder:rbac:groups=milestone.as-code.io,resources=milestones,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=milestone.as-code.io,resources=milestones/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=milestone.as-code.io,resources=milestones/finalizers,verbs=update

// Reconcile implements the controller-runtime Reconciler interface by
// delegating to the generic pipeline.
func (r *MilestoneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return r.Reconciler.AsReconcileFunc(func() *apiv1.Milestone { return &apiv1.Milestone{} })(ctx, req)
}

// SetupWithManager wires the controller into the manager. EventSource (fed
// by the watcher.Registry and CRD watcher) triggers reconciles for
// dependency events.
func (r *MilestoneReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&apiv1.Milestone{}).
		Named("milestone")
	if r.EventSource != nil {
		b = b.WatchesRawSource(r.EventSource)
	}
	return b.Complete(r)
}
