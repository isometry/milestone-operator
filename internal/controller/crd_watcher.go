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
	"errors"
	"sync"

	"github.com/go-logr/logr"
	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/metrics"
	"github.com/isometry/milestone-operator/internal/watcher"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// RegistryInvalidator is the subset of *watcher.Registry the CRD watcher
// depends on. Defined as an interface so tests can substitute a fake.
type RegistryInvalidator interface {
	InvalidateGroupKind(group, kind string)
}

// CRDWatcher watches CustomResourceDefinitions and reconciles the operator's
// discovery cache and informer registry against the lifecycle of every CRD
// that any Milestone or ClusterMilestone references.
//
// Two transition kinds matter:
//   - observed not-Established → Established (CRD has just become
//     resolvable): clear the cached discovery entry for (group, kind), drop
//     any stale informer for the same, and wake every owner that references
//     the kind so they re-resolve and re-subscribe.
//   - Established=True → anything else (CRD removed, NamesAccepted lost,
//     etc.): same actions, using the previously-recorded (group, kind).
//     Without this the informer keeps a now-empty cache and combined with
//     emptySetPolicy=Ready would flip the dependency to True against a
//     vanished API surface.
//
// Transition tracking is per CRD name (== reconcile.Request name) and only
// fires on observed state *changes*. The first observation of a CRD —
// notably the informer replay of every pre-existing CRD at operator startup
// or leader change — seeds the state silently: controller-runtime already
// reconciles every owner at startup, so firing per-CRD invalidations and
// full owner list scans there is pure churn (and would inflate
// milestone_crd_established_events_total on every restart). The narrow race
// this concedes — a CRD reaching Established before its very first observed
// event — converges via the owners' 30s stalled requeue.
//
// State is committed only after the transition side effects succeed
// (peek-then-commit), so a failed wake surfaces as a Reconcile error and the
// controller-runtime retry re-fires the same transition instead of matching
// "no change". Reconciles for one key are never concurrent, so peeking then
// committing is race-free.
type CRDWatcher struct {
	client.Client
	Resolver discovery.Resolver
	// Registry drops informer entries for (group, kind) on CRD removal /
	// de-Establish. Optional in tests.
	Registry RegistryInvalidator
	// MilestoneEnqueue and ClusterMilestoneEnqueue forward wake events
	// directly into each controller's workqueue. The workqueue dedupes by
	// key so an event storm collapses to a single reconcile.
	MilestoneEnqueue        *watcher.EnqueueSource
	ClusterMilestoneEnqueue *watcher.EnqueueSource

	mu           sync.Mutex
	lastObserved map[string]crdState // keyed by CRD metadata.name
}

type groupKind struct{ group, kind string }

// crdState is the last-observed identity and Established condition of one CRD.
type crdState struct {
	gk          groupKind
	established bool
}

// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

// Reconcile is invoked for every CRD change. It computes the (prior, current)
// state pair for this CRD and runs the cache-invalidate + owner-wake side
// effects only on observed transitions, committing the new state only once
// those side effects succeed.
func (w *CRDWatcher) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	crd := &apiextv1.CustomResourceDefinition{}
	getErr := w.Get(ctx, req.NamespacedName, crd)
	if getErr != nil && client.IgnoreNotFound(getErr) != nil {
		return ctrl.Result{}, getErr
	}
	missing := getErr != nil // NotFound: CRD has been deleted

	var current crdState
	if !missing {
		current = crdState{
			gk:          groupKind{group: crd.Spec.Group, kind: crd.Spec.Names.Kind},
			established: crdEstablished(crd),
		}
	}

	prior, seen := w.peekObserved(req.Name)

	var fire *groupKind
	establishing := false
	switch {
	case !seen:
		// First observation (startup / leader-change informer replay, or a
		// brand-new CRD's create event): seed silently below.
	case current.established && (!prior.established || prior.gk != current.gk):
		// Transition to Established (or the (group, kind) changed under us —
		// unusual but possible with apiserver edits).
		fire = &current.gk
		establishing = true
	case prior.established && !current.established:
		// Transition away from Established (CRD gone, or status changed). Use
		// the previously-recorded (group, kind) since `current` is empty when
		// the CRD is missing.
		fire = &prior.gk
	}

	if fire != nil {
		if err := w.applyTransition(ctx, log, *fire); err != nil {
			// State deliberately not committed: the retry recomputes the same
			// transition and re-fires the invalidate-and-wake.
			return ctrl.Result{}, err
		}
		if establishing {
			metrics.CRDEstablishedEvents.WithLabelValues(fire.group, fire.kind).Inc()
		}
	}

	w.commitObserved(req.Name, current, missing)
	return ctrl.Result{}, nil
}

// applyTransition runs the invalidate-and-wake side effects for a (group,
// kind). Wake errors are joined and returned so controller-runtime backs
// off and retries; otherwise a transient apiserver hiccup would leave
// stalled owners waiting on the 30s stalled-requeue safety net.
func (w *CRDWatcher) applyTransition(ctx context.Context, log logr.Logger, gk groupKind) error {
	if w.Resolver != nil {
		w.Resolver.InvalidateGroupKind(gk.group, gk.kind)
	}
	if w.Registry != nil {
		w.Registry.InvalidateGroupKind(gk.group, gk.kind)
	}
	mErr := w.wakeMilestones(ctx, gk.group, gk.kind)
	if mErr != nil {
		log.Error(mErr, "wake milestones", "group", gk.group, "kind", gk.kind)
	}
	cErr := w.wakeClusterMilestones(ctx, gk.group, gk.kind)
	if cErr != nil {
		log.Error(cErr, "wake clustermilestones", "group", gk.group, "kind", gk.kind)
	}
	return errors.Join(mErr, cErr)
}

// peekObserved returns the last committed observation for the CRD without
// mutating it.
func (w *CRDWatcher) peekObserved(name string) (crdState, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	prior, seen := w.lastObserved[name]
	return prior, seen
}

// commitObserved records the latest observation for the CRD; a missing
// (deleted) CRD clears the entry.
func (w *CRDWatcher) commitObserved(name string, current crdState, missing bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if missing {
		delete(w.lastObserved, name)
		return
	}
	if w.lastObserved == nil {
		w.lastObserved = make(map[string]crdState)
	}
	w.lastObserved[name] = current
}

func (w *CRDWatcher) wakeMilestones(ctx context.Context, group, kind string) error {
	if w.MilestoneEnqueue == nil {
		return nil
	}
	list := &apiv1.MilestoneList{}
	if err := w.List(ctx, list); err != nil {
		return err
	}
	gk := schema.GroupKind{Group: group, Kind: kind}
	for i := range list.Items {
		m := &list.Items[i]
		if !anyTargetMatches(m.Spec.DependsOn, gk) {
			continue
		}
		w.MilestoneEnqueue.Enqueue(reconcile.Request{NamespacedName: client.ObjectKeyFromObject(m)})
		metrics.OwnersWoken.WithLabelValues("crd_established").Inc()
	}
	return nil
}

func (w *CRDWatcher) wakeClusterMilestones(ctx context.Context, group, kind string) error {
	if w.ClusterMilestoneEnqueue == nil {
		return nil
	}
	list := &apiv1.ClusterMilestoneList{}
	if err := w.List(ctx, list); err != nil {
		return err
	}
	gk := schema.GroupKind{Group: group, Kind: kind}
	for i := range list.Items {
		cm := &list.Items[i]
		if !anyClusterTargetMatches(cm.Spec.DependsOn, gk) {
			continue
		}
		w.ClusterMilestoneEnqueue.Enqueue(reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cm)})
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

func anyTargetMatches(deps []apiv1.DependencyRef, gk schema.GroupKind) bool {
	for i := range deps {
		if deps[i].Target.GroupKind() == gk {
			return true
		}
	}
	return false
}

func anyClusterTargetMatches(deps []apiv1.ClusterDependencyRef, gk schema.GroupKind) bool {
	for i := range deps {
		if deps[i].Target.GroupKind() == gk {
			return true
		}
	}
	return false
}
