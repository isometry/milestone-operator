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

// ClusterEchelonReconciler is the thin per-CRD wrapper around the generic
// Reconciler for the cluster-scoped CRD.
type ClusterEchelonReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Reconciler    *Reconciler[*apiv1.ClusterEchelon]
	EnqueueEvents <-chan event.GenericEvent
}

// +kubebuilder:rbac:groups=as-code.io,resources=clusterechelons,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=as-code.io,resources=clusterechelons/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=as-code.io,resources=clusterechelons/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

// Reconcile implements the controller-runtime Reconciler interface.
func (r *ClusterEchelonReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return r.Reconciler.AsReconcileFunc(func() *apiv1.ClusterEchelon { return &apiv1.ClusterEchelon{} })(ctx, req)
}

// SetupWithManager wires the controller into the manager.
func (r *ClusterEchelonReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&apiv1.ClusterEchelon{}).
		Named("clusterechelon")
	if r.EnqueueEvents != nil {
		b = b.WatchesRawSource(source.Channel(r.EnqueueEvents, &handler.EnqueueRequestForObject{}))
	}
	return b.Complete(r)
}
