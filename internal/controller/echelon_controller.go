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
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// EchelonReconciler is the thin per-CRD wrapper around the generic Reconciler
// for namespaced Echelon objects.
type EchelonReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Reconciler    *Reconciler[*apiv1.Echelon]
	EnqueueEvents <-chan event.GenericEvent
}

// +kubebuilder:rbac:groups=as-code.io,resources=echelons,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=as-code.io,resources=echelons/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=as-code.io,resources=echelons/finalizers,verbs=update

// Reconcile implements the controller-runtime Reconciler interface by
// delegating to the generic pipeline.
func (r *EchelonReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return r.Reconciler.AsReconcileFunc(func() *apiv1.Echelon { return &apiv1.Echelon{} })(ctx, req)
}

// SetupWithManager wires the controller into the manager. EnqueueEvents (fed
// by the watcher.Registry) triggers reconciles for member events.
func (r *EchelonReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&apiv1.Echelon{}).
		Named("echelon")
	if r.EnqueueEvents != nil {
		b = b.WatchesRawSource(source.Channel(r.EnqueueEvents, &handler.EnqueueRequestForObject{}))
	}
	return b.Complete(r)
}
