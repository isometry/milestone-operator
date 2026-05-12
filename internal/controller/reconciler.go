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

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/metrics"
	"github.com/isometry/milestone-operator/internal/status"
	"github.com/isometry/milestone-operator/internal/watcher"
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

// defaultStalledErrorCap caps the per-dependency errors enumerated in the
// Stalled condition message. Beyond this we append a "... N more" suffix.
const defaultStalledErrorCap = 50

// Reconciler is the GVK-agnostic reconcile pipeline shared by Milestone and
// ClusterMilestone controllers. Behavioural variation is encapsulated in
// OwnerAdapter implementations passed by the per-controller wiring.
//
// The owner type T (e.g. *apiv1.Milestone) is a type parameter so that
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

	deps, depErrs := func() ([]NormalizedDependency, []DependencyError) {
		defer r.observe(metrics.StageDiscovery)()
		return adapter.Dependencies(ctx, r.Resolver)
	}()
	for _, de := range depErrs {
		metrics.TargetResolveErrors.WithLabelValues(r.Controller, de.Reason).Inc()
		log.V(1).Info("dependency discovery failed",
			"dependency", de.Name, "group", de.Group, "kind", de.Kind,
			"reason", de.Reason, "err", de.Err)
	}
	log.V(1).Info("discovery resolved", "dependencies", len(deps), "errors", len(depErrs))

	subscribeErrs := r.reconcileSubscriptions(ctx, ownerKey, deps)
	for _, se := range subscribeErrs {
		metrics.TargetResolveErrors.WithLabelValues(r.Controller, se.Reason).Inc()
		log.V(1).Info("subscription failed",
			"dependency", se.Name, "group", se.Group, "version", se.Version, "kind", se.Kind,
			"reason", se.Reason, "err", se.Err)
	}
	depErrs = append(depErrs, subscribeErrs...)

	// Carry subscribe failures into evaluateDependencies so the lister-less
	// GVKs surface as Ready=Unknown instead of empty-set rollups (which
	// emptySetPolicy: Ready would promote to Ready=True).
	failedReasons := failedDependencyReasons(subscribeErrs)

	rollups, notReady, listErrs := r.evaluateDependencies(deps, failedReasons)
	for _, le := range listErrs {
		log.V(1).Info("list failed",
			"dependency", le.Name, "group", le.Group, "version", le.Version, "kind", le.Kind,
			"reason", le.Reason, "err", le.Err)
		metrics.TargetResolveErrors.WithLabelValues(r.Controller, le.Reason).Inc()
	}
	depErrs = append(depErrs, listErrs...)
	log.V(1).Info("evaluated", "rollups", len(rollups), "notReady", len(notReady))

	r.applyStatus(adapter.Status(), obj.GetGeneration(), rollups, notReady, depErrs)

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

func (r *Reconciler[T]) reconcileSubscriptions(ctx context.Context, owner watcher.OwnerKey, desired []NormalizedDependency) []DependencyError {
	defer r.observe(metrics.StageSubscriptions)()

	// Group desired dependencies by GVK so each owner/GVK pair becomes one
	// Subscribe call carrying every matcher the owner declared for that GVK.
	// Without this, two dependencies on the same GVK with different
	// selectors would overwrite each other's matcher in the registry and
	// the loser would stop receiving events.
	type group struct {
		scope    apimeta.RESTScopeName
		matchers []watcher.Matcher
		deps     []NormalizedDependency
	}
	gvkOrder := make([]schema.GroupVersionKind, 0, len(desired))
	groups := make(map[schema.GroupVersionKind]*group, len(desired))
	for _, d := range desired {
		g, ok := groups[d.GVK]
		if !ok {
			g = &group{scope: d.Scope}
			groups[d.GVK] = g
			gvkOrder = append(gvkOrder, d.GVK)
		}
		g.matchers = append(g.matchers, watcher.Matcher{
			Selector:   d.Selector,
			Namespaces: d.NamespaceMatcher,
		})
		g.deps = append(g.deps, d)
	}

	// One informer per unique GVK across dependencies; refcounted by ownerKey.
	for _, gvk := range r.Registry.GVKsByOwner(owner) {
		if _, ok := groups[gvk]; !ok {
			r.Registry.Unsubscribe(gvk, owner)
		}
	}

	var errs []DependencyError
	for _, gvk := range gvkOrder {
		g := groups[gvk]
		sub := watcher.Subscriber{Owner: owner, Matchers: g.matchers}
		if err := r.Registry.Subscribe(ctx, gvk, g.scope, sub); err != nil {
			// One informer failure prevents every dependency in this GVK
			// group from receiving events. Surface a per-dependency error
			// so each rollup carries WatchSetupFailed, not a misleading
			// empty-set rollup.
			for _, d := range g.deps {
				errs = append(errs, DependencyError{
					Name:    d.Name,
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

// evaluateDependencies produces per-dependency rollups, owner-level
// not-ready resources, and DependencyErrors for list failures.
// Dependencies named in failedReasons skip list+reduce — their lister is
// missing or the upstream subscribe already failed, and emptySetPolicy
// must not see an empty result. Returned rollup keys are dependency names.
func (r *Reconciler[T]) evaluateDependencies(deps []NormalizedDependency, failedReasons map[string]string) (map[string]apiv1.DependencyStatus, []apiv1.ResourceStatus, []DependencyError) {
	rollups := make(map[string]apiv1.DependencyStatus, len(deps))
	// Stay nil until we actually have not-ready resources: an empty-but-allocated
	// slice would round-trip through status DeepCopy as != nil and trigger
	// spurious patches against equal prior state.
	var notReady []apiv1.ResourceStatus //nolint:prealloc
	var listErrs []DependencyError
	for _, d := range deps {
		if reason, skip := failedReasons[d.Name]; skip {
			rollups[d.Name] = failedRollup(d.Name, d.GVK, reason)
			continue
		}
		resources, err := r.listAndCompute(d)
		if err != nil {
			rollups[d.Name] = failedRollup(d.Name, d.GVK, apiv1.ReasonWatchSetupFailed)
			listErrs = append(listErrs, DependencyError{
				Name:    d.Name,
				Group:   d.GVK.Group,
				Version: d.GVK.Version,
				Kind:    d.GVK.Kind,
				Reason:  apiv1.ReasonWatchSetupFailed,
				Err:     err,
			})
			continue
		}
		var rollup apiv1.DependencyStatus
		func() {
			defer r.observe(metrics.StageReduce)()
			rollup = status.ReduceDependency(d.Name, d.GVK.Group, d.GVK.Version, d.GVK.Kind, resources, d.EmptySetPolicy)
		}()
		rollups[d.Name] = rollup
		notReady = append(notReady, notReadyResourcesOf(resources)...)
	}
	return rollups, notReady, listErrs
}

func (r *Reconciler[T]) listAndCompute(d NormalizedDependency) ([]status.Resource, error) {
	objs, err := func() ([]*unstructured.Unstructured, error) {
		defer r.observe(metrics.StageList)()
		o, err := r.Registry.List(d.GVK)
		if err != nil {
			return nil, err
		}
		out := make([]*unstructured.Unstructured, 0, len(o))
		for _, u := range o {
			if !dependencyAdmits(d, u.GetNamespace(), u.GetLabels()) {
				continue
			}
			out = append(out, u)
		}
		return out, nil
	}()
	if err != nil {
		return nil, err
	}
	defer r.observe(metrics.StageCompute)()
	resources := make([]status.Resource, 0, len(objs))
	for _, u := range objs {
		resources = append(resources, status.Compute(u))
	}
	return resources, nil
}

// failedRollup forces Ready=Unknown so emptySetPolicy can't promote a missing
// informer / failed list to Ready=True.
func failedRollup(name string, gvk schema.GroupVersionKind, reason string) apiv1.DependencyStatus {
	return apiv1.DependencyStatus{
		Name:    name,
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind,
		Ready:   metav1.ConditionUnknown,
		Reason:  reason,
	}
}

func failedDependencyReasons(errs []DependencyError) map[string]string {
	if len(errs) == 0 {
		return nil
	}
	out := make(map[string]string, len(errs))
	for _, e := range errs {
		if e.Name == "" {
			continue
		}
		// First reason wins so the user-facing reason is the earliest failure
		// in the upstream pipeline (subscribe before list).
		if _, ok := out[e.Name]; !ok {
			out[e.Name] = e.Reason
		}
	}
	return out
}

func (r *Reconciler[T]) applyStatus(sb *apiv1.MilestoneStatusBase, generation int64, rollups map[string]apiv1.DependencyStatus, notReady []apiv1.ResourceStatus, errs []DependencyError) {
	sb.ObservedGeneration = generation
	sb.DependsOn = sortedDependencyStatuses(rollups)
	sb.Summary = status.SummarizeOwner(rollups)
	sb.NotReadyResources, sb.Truncated = capResources(notReady, r.resourceCap())

	readyStatus, readyReason, readyMessage := status.ReduceOwner(rollups)
	setCondition(sb, apiv1.ConditionReady, readyStatus, readyReason, readyMessage)
	setCondition(sb, apiv1.ConditionReconciling, metav1.ConditionFalse, apiv1.ReasonReconcileComplete, "")
	r.applyStalledFromErrors(sb, errs)
}

// sortedDependencyStatuses materialises the per-dependency rollup map into
// a slice sorted by Name, keeping the listmap+DeepEqual idempotency guarantee.
func sortedDependencyStatuses(rollups map[string]apiv1.DependencyStatus) []apiv1.DependencyStatus {
	if len(rollups) == 0 {
		return nil
	}
	out := make([]apiv1.DependencyStatus, 0, len(rollups))
	for _, v := range rollups {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
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

func (r *Reconciler[T]) isStalled(sb *apiv1.MilestoneStatusBase) bool {
	for _, c := range sb.Conditions {
		if c.Type == apiv1.ConditionStalled && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// --- helpers (pure) ---

func dependencyAdmits(d NormalizedDependency, namespace string, lbls map[string]string) bool {
	if d.NamespaceMatcher != nil && !d.NamespaceMatcher(namespace) {
		return false
	}
	if d.Selector != nil && !d.Selector.Matches(labels.Set(lbls)) {
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

func setCondition(sb *apiv1.MilestoneStatusBase, condType string, st metav1.ConditionStatus, reason, message string) {
	c := metav1.Condition{
		Type:               condType,
		Status:             st,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: sb.ObservedGeneration,
	}
	apimeta.SetStatusCondition(&sb.Conditions, c)
}

func (r *Reconciler[T]) applyStalledFromErrors(sb *apiv1.MilestoneStatusBase, errs []DependencyError) {
	if len(errs) == 0 {
		apimeta.SetStatusCondition(&sb.Conditions, metav1.Condition{
			Type:               apiv1.ConditionStalled,
			Status:             metav1.ConditionFalse,
			Reason:             apiv1.ReasonReconcileComplete,
			Message:            "",
			ObservedGeneration: sb.ObservedGeneration,
		})
		return
	}
	// Stable ordering: errors come from the adapter sorted by dependency
	// name, so the primary reason and the enumerated message survive
	// reconciles unchanged when the same errors recur. Sort defensively.
	sorted := make([]DependencyError, len(errs))
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
		Type:               apiv1.ConditionStalled,
		Status:             metav1.ConditionTrue,
		Reason:             primary,
		Message:            message,
		ObservedGeneration: sb.ObservedGeneration,
	})
}

func readyConditionStatus(sb *apiv1.MilestoneStatusBase) metav1.ConditionStatus {
	for _, c := range sb.Conditions {
		if c.Type == apiv1.ConditionReady {
			return c.Status
		}
	}
	return metav1.ConditionUnknown
}

// statusEqualIgnoringTimestamp compares two MilestoneStatusBase values ignoring
// LastEvaluatedTime and condition lastTransitionTime (which only changes when
// status itself changes, courtesy of meta.SetStatusCondition).
func statusEqualIgnoringTimestamp(a, b apiv1.MilestoneStatusBase) bool {
	a.LastEvaluatedTime = metav1.Time{}
	b.LastEvaluatedTime = metav1.Time{}
	a.Conditions = stripTransitionTimes(a.Conditions)
	b.Conditions = stripTransitionTimes(b.Conditions)
	return reflect.DeepEqual(a, b)
}

// stripTransitionTimes zeroes ObservedGeneration *deliberately*: SetStatus
// Condition rewrites the per-condition obsGen on every call, so leaving it in
// the comparison would force a patch on every reconcile (even an identical
// one) once the generation has been written once. status.observedGeneration
// (the top-level field) carries the authoritative freshness signal and is
// part of the deep compare, so genuine generation bumps still patch exactly
// once.
func stripTransitionTimes(in []metav1.Condition) []metav1.Condition {
	out := make([]metav1.Condition, len(in))
	copy(out, in)
	for i := range out {
		out[i].LastTransitionTime = metav1.Time{}
		out[i].ObservedGeneration = 0
	}
	return out
}
