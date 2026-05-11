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
	"maps"

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
			builder.WithPredicates(namespaceMembershipPredicate{}),
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

// namespaceMembershipPredicate admits the Namespace events that can affect a
// ClusterEchelon's namespaceSelector membership: creation (new namespace may
// match), label-only updates (matching may flip), and deletion (previously
// matching namespace drops). Heartbeat updates with unchanged labels are
// dropped so unrelated noise doesn't churn reconciles.
//
// Spelled out explicitly rather than relying on
// predicate.LabelChangedPredicate{} composed via predicate.Or — that
// composition admits Creates today only because Funcs.Create defaults to
// true, which is fragile across controller-runtime upgrades.
type namespaceMembershipPredicate struct{ predicate.Funcs }

func (namespaceMembershipPredicate) Create(event.CreateEvent) bool { return true }
func (namespaceMembershipPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil || e.ObjectNew == nil {
		return false
	}
	return !maps.Equal(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels())
}
func (namespaceMembershipPredicate) Delete(event.DeleteEvent) bool   { return true }
func (namespaceMembershipPredicate) Generic(event.GenericEvent) bool { return false }
