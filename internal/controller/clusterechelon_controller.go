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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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
		Named("clusterechelon").
		Watches(
			&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.namespaceToClusterEchelons),
			// Label-changed admits the only events that can affect
			// namespaceSelector membership; deletion still has to wake
			// previously-matching owners so they drop the namespace from
			// their materialised set.
			builder.WithPredicates(predicate.Or(predicate.LabelChangedPredicate{}, deleteEventPredicate{})),
		)
	if r.EnqueueEvents != nil {
		b = b.WatchesRawSource(source.Channel(r.EnqueueEvents, &handler.EnqueueRequestForObject{}))
	}
	return b.Complete(r)
}

// namespaceToClusterEchelons fans a Namespace event out to every
// ClusterEchelon that uses *any* spec.members[*].namespaceSelector. No
// label-side filtering: a label removal or namespace deletion must still
// reach previously-matching owners so they can recompute their member set.
func (r *ClusterEchelonReconciler) namespaceToClusterEchelons(ctx context.Context, _ client.Object) []reconcile.Request {
	var list apiv1.ClusterEchelonList
	if err := r.List(ctx, &list); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		if !usesNamespaceSelector(&list.Items[i]) {
			continue
		}
		out = append(out, reconcile.Request{
			NamespacedName: client.ObjectKey{Name: list.Items[i].Name},
		})
	}
	return out
}

func usesNamespaceSelector(ce *apiv1.ClusterEchelon) bool {
	for _, m := range ce.Spec.Members {
		if m.NamespaceSelector != nil {
			return true
		}
	}
	return false
}

// deleteEventPredicate admits Delete events only; the LabelChangedPredicate
// in the Or() chain ignores deletions, so without this side namespace removes
// would never reach the map func.
type deleteEventPredicate struct{ predicate.Funcs }

func (deleteEventPredicate) Create(event.CreateEvent) bool   { return false }
func (deleteEventPredicate) Update(event.UpdateEvent) bool   { return false }
func (deleteEventPredicate) Delete(event.DeleteEvent) bool   { return true }
func (deleteEventPredicate) Generic(event.GenericEvent) bool { return false }
