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
	"fmt"
	"reflect"
	"strings"
	"time"

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	"github.com/isometry/echelon-operator/internal/discovery"
	"github.com/isometry/echelon-operator/internal/metrics"
	"github.com/isometry/echelon-operator/internal/status"
	"github.com/isometry/echelon-operator/internal/watcher"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// stalledRequeue is the requeue delay applied when an object ends a reconcile
// in Stalled=True (e.g. waiting on a CRD to be installed). The CRD watcher
// also wakes us on Established=True, so this is just a safety net.
const stalledRequeue = 30 * time.Second

// defaultMemberCap caps NotReadyMembers to keep the status object below the
// 1MiB etcd limit even on huge selectors.
const defaultMemberCap = 50

// defaultStalledErrorCap caps the per-target errors enumerated in the Stalled
// condition message. Beyond this we append a "... N more" suffix. Mirrors
// defaultMemberCap's role: bound the worst-case condition size.
const defaultStalledErrorCap = 50

// Reconciler is the GVK-agnostic reconcile pipeline shared by Echelon and
// ClusterEchelon controllers. Behavioural variation is encapsulated in
// OwnerAdapter implementations passed by the per-controller wiring.
//
// The owner type T (e.g. *apiv1.Echelon) is a type parameter so that
// NewAdapter is statically typed: mis-pairing a Reconciler[*apiv1.X] with a
// NewAdapter func(*apiv1.Y) is a compile error at the assignment site.
type Reconciler[T client.Object] struct {
	Client     client.Client
	Registry   RegistryAPI
	Resolver   discovery.Resolver
	NewAdapter func(T) OwnerAdapter
	Controller string

	// Now, MemberCap and StalledErrorCap have sensible defaults; injectable
	// for tests.
	Now             func() time.Time
	MemberCap       int
	StalledErrorCap int
}

// ReconcileObject runs the full pipeline for the given owner object. Per-stage
// metrics are emitted via metrics.ObserveStage. Stage-boundary diagnostics
// are emitted at V(1) so operators can grep an otherwise-silent pipeline.
func (r *Reconciler[T]) ReconcileObject(ctx context.Context, obj T) (ctrl.Result, error) {
	adapter := r.NewAdapter(obj)
	ownerKey := adapter.OwnerKey()
	log := logf.FromContext(ctx).WithValues("controller", r.Controller, "owner", ownerKey)

	if !obj.GetDeletionTimestamp().IsZero() {
		return r.finalize(ctx, obj, ownerKey)
	}

	if !controllerutil.ContainsFinalizer(obj, apiv1.Finalizer) {
		controllerutil.AddFinalizer(obj, apiv1.Finalizer)
		if err := r.Client.Update(ctx, obj); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		// The Update emits a Watch event on the owner; controller-runtime
		// requeues from that automatically. No explicit Requeue needed.
		return ctrl.Result{}, nil
	}

	log.V(1).Info("reconciling", "generation", obj.GetGeneration())
	prior := *adapter.Status().DeepCopy()

	targets, targetErrs := func() ([]NormalizedTarget, []TargetError) {
		defer r.observe(metrics.StageDiscovery)()
		return adapter.Targets(ctx, r.Resolver)
	}()
	for _, te := range targetErrs {
		metrics.TargetResolveErrors.WithLabelValues(r.Controller, te.Reason).Inc()
		log.V(1).Info("target discovery failed",
			"index", te.Index, "group", te.Group, "kind", te.Kind,
			"reason", te.Reason, "err", te.Err)
	}
	log.V(1).Info("discovery resolved", "targets", len(targets), "errors", len(targetErrs))

	subscribeErrs := r.reconcileSubscriptions(ownerKey, targets)
	for _, se := range subscribeErrs {
		log.V(1).Info("subscription failed",
			"index", se.Index, "group", se.Group, "version", se.Version, "kind", se.Kind,
			"reason", se.Reason, "err", se.Err)
	}
	targetErrs = append(targetErrs, subscribeErrs...)

	rollups, notReady := r.evaluateTargets(targets)
	log.V(1).Info("evaluated", "rollups", len(rollups), "notReady", len(notReady))

	r.applyStatus(adapter.Status(), obj.GetGeneration(), rollups, notReady, targetErrs)

	if !statusEqualIgnoringTimestamp(prior, *adapter.Status()) {
		adapter.Status().LastEvaluatedTime = metav1.NewTime(r.now())
		if err := r.patchStatus(ctx, adapter); err != nil {
			metrics.StatusPatchTotal.WithLabelValues(r.Controller, metrics.PatchError).Inc()
			return ctrl.Result{}, err
		}
		metrics.StatusPatchTotal.WithLabelValues(r.Controller, metrics.PatchChanged).Inc()
		log.V(1).Info("status patched", "ready", readyConditionStatus(adapter.Status()))
	} else {
		// Preserve prior LastEvaluatedTime when nothing substantive changed.
		adapter.Status().LastEvaluatedTime = prior.LastEvaluatedTime
		metrics.StatusPatchTotal.WithLabelValues(r.Controller, metrics.PatchUnchanged).Inc()
		log.V(1).Info("status unchanged", "ready", readyConditionStatus(adapter.Status()))
	}

	if r.isStalled(adapter.Status()) {
		log.V(1).Info("stalled; requeuing", "after", stalledRequeue)
		return ctrl.Result{RequeueAfter: stalledRequeue}, nil
	}
	return ctrl.Result{}, nil
}

// AsReconcileFunc adapts ReconcileObject to a controller-runtime
// reconcile.Func that fetches obj using newObj() before delegating.
func (r *Reconciler[T]) AsReconcileFunc(newObj func() T) reconcile.Func {
	return func(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
		obj := newObj()
		if err := r.Client.Get(ctx, req.NamespacedName, obj); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}
		return r.ReconcileObject(ctx, obj)
	}
}

func (r *Reconciler[T]) finalize(ctx context.Context, obj T, ownerKey watcher.OwnerKey) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(obj, apiv1.Finalizer) {
		return ctrl.Result{}, nil
	}
	r.Registry.UnsubscribeAll(ownerKey)
	controllerutil.RemoveFinalizer(obj, apiv1.Finalizer)
	if err := r.Client.Update(ctx, obj); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler[T]) reconcileSubscriptions(owner watcher.OwnerKey, desired []NormalizedTarget) []TargetError {
	defer r.observe(metrics.StageSubscriptions)()

	desiredSet := make(map[schema.GroupVersionKind]NormalizedTarget, len(desired))
	for _, t := range desired {
		desiredSet[t.GVK] = t
	}
	for _, gvk := range r.Registry.GVKsByOwner(owner) {
		if _, ok := desiredSet[gvk]; !ok {
			r.Registry.Unsubscribe(gvk, owner)
		}
	}

	var errs []TargetError
	for _, t := range desired {
		sub := watcher.Subscriber{
			Owner:            owner,
			Selector:         t.Selector,
			NamespaceMatcher: t.NamespaceMatcher,
		}
		if err := r.Registry.Subscribe(t.GVK, t.Scope, sub); err != nil {
			errs = append(errs, TargetError{
				Index:   t.Index,
				Group:   t.GVK.Group,
				Version: t.GVK.Version,
				Kind:    t.GVK.Kind,
				Reason:  apiv1.ReasonWatchSetupFailed,
				Err:     err,
			})
		}
	}
	return errs
}

func (r *Reconciler[T]) evaluateTargets(targets []NormalizedTarget) ([]apiv1.TargetRollup, []apiv1.MemberStatus) {
	rollups := make([]apiv1.TargetRollup, 0, len(targets))
	// Stay nil until we actually have not-ready members: an empty-but-allocated
	// slice would round-trip through status DeepCopy as != nil and trigger
	// spurious patches against equal prior state.
	var notReady []apiv1.MemberStatus //nolint:prealloc
	for _, t := range targets {
		members := r.listAndCompute(t)
		var rollup apiv1.TargetRollup
		func() {
			defer r.observe(metrics.StageReduce)()
			rollup = status.ReduceTarget(t.GVK.Group, t.GVK.Version, t.GVK.Kind, members, t.EmptySetPolicy)
		}()
		rollups = append(rollups, rollup)
		notReady = append(notReady, notReadyMembersOf(members)...)
	}
	return rollups, notReady
}

func (r *Reconciler[T]) listAndCompute(t NormalizedTarget) []status.Member {
	objs, err := func() ([]*unstructured.Unstructured, error) {
		defer r.observe(metrics.StageList)()
		o, err := r.Registry.List(t.GVK)
		if err != nil {
			return nil, err
		}
		out := make([]*unstructured.Unstructured, 0, len(o))
		for _, u := range o {
			if !targetAdmits(t, u.GetNamespace(), u.GetLabels()) {
				continue
			}
			out = append(out, u)
		}
		return out, nil
	}()
	if err != nil {
		return nil
	}
	defer r.observe(metrics.StageCompute)()
	members := make([]status.Member, 0, len(objs))
	for _, u := range objs {
		members = append(members, status.Compute(u))
	}
	return members
}

func (r *Reconciler[T]) applyStatus(sb *apiv1.EchelonStatusBase, generation int64, rollups []apiv1.TargetRollup, notReady []apiv1.MemberStatus, errs []TargetError) {
	sb.ObservedGeneration = generation
	sb.Targets = rollups
	sb.Summary = status.SummarizeOwner(rollups)
	sb.NotReadyMembers, sb.Truncated = capMembers(notReady, r.memberCap())

	readyStatus, readyReason, readyMessage := status.ReduceOwner(rollups)
	setCondition(sb, apiv1.ConditionReady, readyStatus, readyReason, readyMessage)
	setCondition(sb, apiv1.ConditionReconciling, metav1.ConditionFalse, apiv1.ReasonReconciling, "")
	r.applyStalledFromErrors(sb, errs)
}

func (r *Reconciler[T]) patchStatus(ctx context.Context, adapter OwnerAdapter) error {
	defer r.observe(metrics.StagePatch)()
	return adapter.PatchStatus(ctx, r.Client)
}

func (r *Reconciler[T]) observe(stage string) func() {
	return metrics.ObserveStage(r.Controller, stage)
}

func (r *Reconciler[T]) now() time.Time {
	if r.Now == nil {
		return time.Now()
	}
	return r.Now()
}

func (r *Reconciler[T]) memberCap() int {
	if r.MemberCap <= 0 {
		return defaultMemberCap
	}
	return r.MemberCap
}

func (r *Reconciler[T]) stalledErrorCap() int {
	if r.StalledErrorCap <= 0 {
		return defaultStalledErrorCap
	}
	return r.StalledErrorCap
}

func (r *Reconciler[T]) isStalled(sb *apiv1.EchelonStatusBase) bool {
	for _, c := range sb.Conditions {
		if c.Type == apiv1.ConditionStalled && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// --- helpers (pure) ---

func targetAdmits(t NormalizedTarget, namespace string, lbls map[string]string) bool {
	if t.NamespaceMatcher != nil && !t.NamespaceMatcher(namespace) {
		return false
	}
	if t.Selector != nil && !t.Selector.Matches(labels.Set(lbls)) {
		return false
	}
	return true
}

func notReadyMembersOf(members []status.Member) []apiv1.MemberStatus {
	out := make([]apiv1.MemberStatus, 0)
	for _, m := range members {
		if m.Status == "Current" {
			continue
		}
		out = append(out, apiv1.MemberStatus{
			Group:     m.Group,
			Version:   m.Version,
			Kind:      m.Kind,
			Namespace: m.Namespace,
			Name:      m.Name,
			Status:    m.Status,
			Reason:    m.Reason,
			Message:   m.Message,
		})
	}
	return out
}

func capMembers(in []apiv1.MemberStatus, cap int) ([]apiv1.MemberStatus, bool) {
	if len(in) <= cap {
		return in, false
	}
	return in[:cap], true
}

func setCondition(sb *apiv1.EchelonStatusBase, condType string, st metav1.ConditionStatus, reason, message string) {
	c := metav1.Condition{Type: condType, Status: st, Reason: reason, Message: message}
	apimeta.SetStatusCondition(&sb.Conditions, c)
}

func (r *Reconciler[T]) applyStalledFromErrors(sb *apiv1.EchelonStatusBase, errs []TargetError) {
	if len(errs) == 0 {
		apimeta.SetStatusCondition(&sb.Conditions, metav1.Condition{
			Type:    apiv1.ConditionStalled,
			Status:  metav1.ConditionFalse,
			Reason:  apiv1.ReasonReconciling,
			Message: "",
		})
		return
	}
	// Use the first reason; message lists offenders for visibility. Cap the
	// enumeration so a pathological owner can't exceed condition-message
	// limits; the overflow count is surfaced explicitly.
	primary := errs[0].Reason
	cap := r.stalledErrorCap()
	visible := errs
	overflow := 0
	if len(errs) > cap {
		visible = errs[:cap]
		overflow = len(errs) - cap
	}
	parts := make([]string, 0, len(visible))
	for _, e := range visible {
		parts = append(parts, fmt.Sprintf("%s.%s/%s [%s]: %v", e.Group, e.Version, e.Kind, e.Reason, e.Err))
	}
	message := strings.Join(parts, "; ")
	if overflow > 0 {
		message = fmt.Sprintf("%s; … %d more", message, overflow)
	}
	apimeta.SetStatusCondition(&sb.Conditions, metav1.Condition{
		Type:    apiv1.ConditionStalled,
		Status:  metav1.ConditionTrue,
		Reason:  primary,
		Message: message,
	})
}

func readyConditionStatus(sb *apiv1.EchelonStatusBase) metav1.ConditionStatus {
	for _, c := range sb.Conditions {
		if c.Type == apiv1.ConditionReady {
			return c.Status
		}
	}
	return metav1.ConditionUnknown
}

// statusEqualIgnoringTimestamp compares two EchelonStatusBase values ignoring
// LastEvaluatedTime and condition lastTransitionTime (which only changes when
// status itself changes, courtesy of meta.SetStatusCondition).
func statusEqualIgnoringTimestamp(a, b apiv1.EchelonStatusBase) bool {
	a.LastEvaluatedTime = metav1.Time{}
	b.LastEvaluatedTime = metav1.Time{}
	a.Conditions = stripTransitionTimes(a.Conditions)
	b.Conditions = stripTransitionTimes(b.Conditions)
	return reflect.DeepEqual(a, b)
}

func stripTransitionTimes(in []metav1.Condition) []metav1.Condition {
	out := make([]metav1.Condition, len(in))
	copy(out, in)
	for i := range out {
		out[i].LastTransitionTime = metav1.Time{}
		out[i].ObservedGeneration = 0
	}
	return out
}
