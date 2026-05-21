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
//   - Established=True (CRD has just become resolvable): clear the cached
//     discovery entry for (group, kind), drop any stale informer for the
//     same, and wake every owner that references the kind so they
//     re-resolve and re-subscribe.
//   - Established=True → anything else (CRD removed, NamesAccepted lost,
//     etc.): same actions, using the previously-recorded (group, kind).
//     Without this the informer keeps a now-empty cache and combined with
//     emptySetPolicy=Ready would flip the dependency to True against a
//     vanished API surface.
//
// Transition tracking is per CRD name (== reconcile.Request name). Repeated
// observations of the same Established=True state are idempotent — the cache
// invalidate and wake only fire when state actually changes.
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

	mu              sync.Mutex
	lastEstablished map[string]groupKind // keyed by CRD metadata.name
}

type groupKind struct{ group, kind string }

// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

// Reconcile is invoked for every CRD change. It computes the (prior, current)
// state pair for this CRD and runs the cache-invalidate + owner-wake side
// effects only on transitions.
func (w *CRDWatcher) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	crd := &apiextv1.CustomResourceDefinition{}
	getErr := w.Get(ctx, req.NamespacedName, crd)
	if getErr != nil && client.IgnoreNotFound(getErr) != nil {
		return ctrl.Result{}, getErr
	}
	missing := getErr != nil // NotFound: CRD has been deleted

	var current groupKind
	currentEstablished := false
	if !missing {
		current = groupKind{group: crd.Spec.Group, kind: crd.Spec.Names.Kind}
		currentEstablished = crdEstablished(crd)
	}

	prior, hadPrior := w.swapLastEstablished(req.Name, current, currentEstablished, missing)

	switch {
	case currentEstablished && (!hadPrior || prior != current):
		// Transition to Established (or the (group, kind) changed under us —
		// unusual but possible with apiserver edits).
		metrics.CRDEstablishedEvents.WithLabelValues(current.group, current.kind).Inc()
		if err := w.applyTransition(ctx, log, current); err != nil {
			return ctrl.Result{}, err
		}
	case hadPrior && !currentEstablished:
		// Transition away from Established (CRD gone, or status changed). Use
		// the previously-recorded (group, kind) since `current` is empty when
		// the CRD is missing.
		if err := w.applyTransition(ctx, log, prior); err != nil {
			return ctrl.Result{}, err
		}
	}

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

// swapLastEstablished records the latest observed (group, kind) for the CRD
// and returns the prior state so the caller can decide whether a transition
// fired. When the CRD is missing or not Established, the entry is cleared.
func (w *CRDWatcher) swapLastEstablished(name string, current groupKind, currentEstablished, missing bool) (groupKind, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lastEstablished == nil {
		w.lastEstablished = make(map[string]groupKind)
	}
	prior, had := w.lastEstablished[name]
	if currentEstablished {
		w.lastEstablished[name] = current
	} else if missing || !currentEstablished {
		delete(w.lastEstablished, name)
	}
	return prior, had
}

func (w *CRDWatcher) wakeMilestones(ctx context.Context, group, kind string) error {
	if w.MilestoneEnqueue == nil {
		return nil
	}
	list := &apiv1.MilestoneList{}
	if err := w.List(ctx, list); err != nil {
		return err
	}
	for i := range list.Items {
		m := &list.Items[i]
		if !ownsGroupKind(m.Spec.DependsOn, group, kind) {
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
	for i := range list.Items {
		cm := &list.Items[i]
		if !ownsGroupKind(cm.Spec.DependsOn, group, kind) {
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

// targetIdentifier is the minimal shape ownsGroupKind needs: a
// dependency entry that can report its target's (group, kind). Implemented
// by both apiv1.DependencyRef and apiv1.ClusterDependencyRef.
type targetIdentifier interface {
	TargetGroupKind() (string, string)
}

func ownsGroupKind[T targetIdentifier](deps []T, group, kind string) bool {
	for i := range deps {
		g, k := deps[i].TargetGroupKind()
		if g == group && k == kind {
			return true
		}
	}
	return false
}
