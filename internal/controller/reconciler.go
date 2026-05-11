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
	"sort"
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

// defaultResourceCap caps NotReadyResources to keep the status object below the
// 1MiB etcd limit even on huge selectors.
const defaultResourceCap = 50

// defaultStalledErrorCap caps the per-member errors enumerated in the Stalled
// condition message. Beyond this we append a "... N more" suffix.
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

	// Now, ResourceCap and StalledErrorCap have sensible defaults; injectable
	// for tests.
	Now             func() time.Time
	ResourceCap     int
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

	members, memberErrs := func() ([]NormalizedMember, []MemberError) {
		defer r.observe(metrics.StageDiscovery)()
		return adapter.Members(ctx, r.Resolver)
	}()
	for _, me := range memberErrs {
		metrics.MemberResolveErrors.WithLabelValues(r.Controller, me.Reason).Inc()
		log.V(1).Info("member discovery failed",
			"name", me.Name, "group", me.Group, "kind", me.Kind,
			"reason", me.Reason, "err", me.Err)
	}
	log.V(1).Info("discovery resolved", "members", len(members), "errors", len(memberErrs))

	subscribeErrs := r.reconcileSubscriptions(ctx, ownerKey, members)
	for _, se := range subscribeErrs {
		log.V(1).Info("subscription failed",
			"name", se.Name, "group", se.Group, "version", se.Version, "kind", se.Kind,
			"reason", se.Reason, "err", se.Err)
	}
	memberErrs = append(memberErrs, subscribeErrs...)

	rollups, notReady := r.evaluateMembers(members)
	log.V(1).Info("evaluated", "rollups", len(rollups), "notReady", len(notReady))

	r.applyStatus(adapter.Status(), obj.GetGeneration(), rollups, notReady, memberErrs)

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

func (r *Reconciler[T]) reconcileSubscriptions(ctx context.Context, owner watcher.OwnerKey, desired []NormalizedMember) []MemberError {
	defer r.observe(metrics.StageSubscriptions)()

	// Group desired members by GVK so each owner/GVK pair becomes one
	// Subscribe call carrying every matcher the owner declared for that GVK.
	// Without this, two members on the same GVK with different selectors
	// would overwrite each other's matcher in the registry and the loser
	// would stop receiving events.
	type group struct {
		scope    apimeta.RESTScopeName
		matchers []watcher.Matcher
		members  []NormalizedMember
	}
	gvkOrder := make([]schema.GroupVersionKind, 0, len(desired))
	groups := make(map[schema.GroupVersionKind]*group, len(desired))
	for _, m := range desired {
		g, ok := groups[m.GVK]
		if !ok {
			g = &group{scope: m.Scope}
			groups[m.GVK] = g
			gvkOrder = append(gvkOrder, m.GVK)
		}
		g.matchers = append(g.matchers, watcher.Matcher{
			Selector:   m.Selector,
			Namespaces: m.NamespaceMatcher,
		})
		g.members = append(g.members, m)
	}

	// One informer per unique GVK across members; refcounted by ownerKey.
	for _, gvk := range r.Registry.GVKsByOwner(owner) {
		if _, ok := groups[gvk]; !ok {
			r.Registry.Unsubscribe(gvk, owner)
		}
	}

	var errs []MemberError
	for _, gvk := range gvkOrder {
		g := groups[gvk]
		sub := watcher.Subscriber{Owner: owner, Matchers: g.matchers}
		if err := r.Registry.Subscribe(ctx, gvk, g.scope, sub); err != nil {
			// One informer failure prevents every member in this GVK group
			// from receiving events. Surface a per-member error so each
			// member's rollup carries WatchSetupFailed, not a misleading
			// empty-set rollup.
			for _, m := range g.members {
				errs = append(errs, MemberError{
					Name:    m.Name,
					Group:   gvk.Group,
					Version: gvk.Version,
					Kind:    gvk.Kind,
					Reason:  apiv1.ReasonWatchSetupFailed,
					Err:     err,
				})
			}
		}
	}
	return errs
}

func (r *Reconciler[T]) evaluateMembers(members []NormalizedMember) (map[string]apiv1.MemberRollup, []apiv1.ResourceStatus) {
	rollups := make(map[string]apiv1.MemberRollup, len(members))
	// Stay nil until we actually have not-ready resources: an empty-but-allocated
	// slice would round-trip through status DeepCopy as != nil and trigger
	// spurious patches against equal prior state.
	var notReady []apiv1.ResourceStatus //nolint:prealloc
	for _, m := range members {
		resources := r.listAndCompute(m)
		var rollup apiv1.MemberRollup
		func() {
			defer r.observe(metrics.StageReduce)()
			rollup = status.ReduceMember(m.GVK.Group, m.GVK.Version, m.GVK.Kind, resources, m.EmptySetPolicy)
		}()
		rollups[m.Name] = rollup
		notReady = append(notReady, notReadyResourcesOf(resources)...)
	}
	return rollups, notReady
}

func (r *Reconciler[T]) listAndCompute(m NormalizedMember) []status.Resource {
	objs, err := func() ([]*unstructured.Unstructured, error) {
		defer r.observe(metrics.StageList)()
		o, err := r.Registry.List(m.GVK)
		if err != nil {
			return nil, err
		}
		out := make([]*unstructured.Unstructured, 0, len(o))
		for _, u := range o {
			if !memberAdmits(m, u.GetNamespace(), u.GetLabels()) {
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
	resources := make([]status.Resource, 0, len(objs))
	for _, u := range objs {
		resources = append(resources, status.Compute(u))
	}
	return resources
}

func (r *Reconciler[T]) applyStatus(sb *apiv1.EchelonStatusBase, generation int64, rollups map[string]apiv1.MemberRollup, notReady []apiv1.ResourceStatus, errs []MemberError) {
	sb.ObservedGeneration = generation
	sb.Members = rollups
	sb.Summary = status.SummarizeOwner(rollups)
	sb.NotReadyResources, sb.Truncated = capResources(notReady, r.resourceCap())

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

func (r *Reconciler[T]) resourceCap() int {
	if r.ResourceCap <= 0 {
		return defaultResourceCap
	}
	return r.ResourceCap
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

func memberAdmits(m NormalizedMember, namespace string, lbls map[string]string) bool {
	if m.NamespaceMatcher != nil && !m.NamespaceMatcher(namespace) {
		return false
	}
	if m.Selector != nil && !m.Selector.Matches(labels.Set(lbls)) {
		return false
	}
	return true
}

func notReadyResourcesOf(resources []status.Resource) []apiv1.ResourceStatus {
	out := make([]apiv1.ResourceStatus, 0)
	for _, m := range resources {
		if m.Status == "Current" {
			continue
		}
		out = append(out, apiv1.ResourceStatus{
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

func capResources(in []apiv1.ResourceStatus, cap int) ([]apiv1.ResourceStatus, bool) {
	if len(in) <= cap {
		return in, false
	}
	return in[:cap], true
}

func setCondition(sb *apiv1.EchelonStatusBase, condType string, st metav1.ConditionStatus, reason, message string) {
	c := metav1.Condition{Type: condType, Status: st, Reason: reason, Message: message}
	apimeta.SetStatusCondition(&sb.Conditions, c)
}

func (r *Reconciler[T]) applyStalledFromErrors(sb *apiv1.EchelonStatusBase, errs []MemberError) {
	if len(errs) == 0 {
		apimeta.SetStatusCondition(&sb.Conditions, metav1.Condition{
			Type:    apiv1.ConditionStalled,
			Status:  metav1.ConditionFalse,
			Reason:  apiv1.ReasonReconciling,
			Message: "",
		})
		return
	}
	// Stable ordering: errors come from the adapter sorted by member name, so
	// the primary reason and the enumerated message survive reconciles
	// unchanged when the same errors recur. Sort defensively.
	sorted := make([]MemberError, len(errs))
	copy(sorted, errs)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	primary := sorted[0].Reason
	cap := r.stalledErrorCap()
	visible := sorted
	overflow := 0
	if len(sorted) > cap {
		visible = sorted[:cap]
		overflow = len(sorted) - cap
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
