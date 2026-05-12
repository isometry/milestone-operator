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

	apiv1 "github.com/isometry/milestone-operator/api/v1"
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

// ClusterMilestoneReconciler is the thin per-CRD wrapper around the generic
// Reconciler for the cluster-scoped CRD.
type ClusterMilestoneReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Reconciler    *Reconciler[*apiv1.ClusterMilestone]
	EnqueueEvents <-chan event.GenericEvent
}

// +kubebuilder:rbac:groups=milestone.as-code.io,resources=clustermilestones,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=milestone.as-code.io,resources=clustermilestones/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=milestone.as-code.io,resources=clustermilestones/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

// Reconcile implements the controller-runtime Reconciler interface.
func (r *ClusterMilestoneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return r.Reconciler.AsReconcileFunc(func() *apiv1.ClusterMilestone { return &apiv1.ClusterMilestone{} })(ctx, req)
}

// SetupWithManager wires the controller into the manager.
func (r *ClusterMilestoneReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&apiv1.ClusterMilestone{}).
		Named("clustermilestone").
		Watches(
			&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.namespaceToClusterMilestones),
			builder.WithPredicates(namespaceMembershipPredicate{}),
		)
	if r.EnqueueEvents != nil {
		b = b.WatchesRawSource(source.Channel(r.EnqueueEvents, &handler.EnqueueRequestForObject{}))
	}
	return b.Complete(r)
}

// namespaceToClusterMilestones fans a Namespace event out to every
// ClusterMilestone that uses *any* spec.dependsOn[*].target.namespaceSelector.
// No label-side filtering: a label removal or namespace deletion must still
// reach previously-matching owners so they can recompute their dependency
// set.
func (r *ClusterMilestoneReconciler) namespaceToClusterMilestones(ctx context.Context, _ client.Object) []reconcile.Request {
	var list apiv1.ClusterMilestoneList
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

func usesNamespaceSelector(cm *apiv1.ClusterMilestone) bool {
	for i := range cm.Spec.DependsOn {
		if cm.Spec.DependsOn[i].Target.NamespaceSelector != nil {
			return true
		}
	}
	return false
}

// namespaceMembershipPredicate admits the Namespace events that can affect a
// ClusterMilestone's namespaceSelector membership: creation (new namespace
// may match), label-only updates (matching may flip), and deletion
// (previously matching namespace drops). Heartbeat updates with unchanged
// labels are dropped so unrelated noise doesn't churn reconciles.
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
