/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package metrics declares the Prometheus metrics emitted by the
// echelon-operator. Metrics are registered against the controller-runtime
// metrics registry from cmd/main.go via Register; tests register against a
// fresh prometheus.Registry to avoid global pollution.
package metrics

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

const ns = "echelon"

// Label names. Centralised so both metrics.go and collector.go reference the
// same string and so callers can build label maps without typos.
const (
	labelGroup       = "group"
	labelVersion     = "version"
	labelKind        = "kind"
	labelEvent       = "event"
	labelResult      = "result"
	labelController  = "controller"
	labelStage       = "stage"
	labelReason      = "reason"
	labelOwnerKind   = "owner_kind"
	labelNamespace   = "namespace"
	labelName        = "name"
	labelType        = "type"
	labelStatus      = "status"
	labelTargetGroup = "target_group"
	labelTargetKind  = "target_kind"
	labelMember      = "member"
)

var (
	// Informers gauges the number of active dynamic informers per GVK.
	Informers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "informers",
		Help:      "Active dynamic informers, one per unique watched GVK.",
	}, []string{labelGroup, labelVersion, labelKind})

	// Subscribers gauges the refcount of Echelon/ClusterEchelon subscribers
	// per GVK.
	Subscribers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "subscribers",
		Help:      "Subscribers per GVK (refcount across Echelons and ClusterEchelons).",
	}, []string{labelGroup, labelVersion, labelKind})

	// InformerEvents counts dispatched informer events.
	InformerEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "informer_events_total",
		Help:      "Informer events dispatched per GVK.",
	}, []string{labelGroup, labelVersion, labelKind, labelEvent})

	// SubscribeTotal counts subscription attempts and their result.
	SubscribeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "subscribe_total",
		Help:      "Subscribe operations issued by reconcilers.",
	}, []string{labelGroup, labelVersion, labelKind, labelResult})

	// UnsubscribeTotal counts unsubscribe operations.
	UnsubscribeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "unsubscribe_total",
		Help:      "Unsubscribe operations issued by reconcilers.",
	}, []string{labelGroup, labelVersion, labelKind})

	// EventDispatchDuration measures how long each informer event takes from
	// receipt to enqueue completion.
	EventDispatchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns,
		Name:      "event_dispatch_duration_seconds",
		Help:      "Time from informer event receipt to subscriber enqueue completion.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2.5, 10),
	}, []string{labelGroup, labelVersion, labelKind})

	// DiscoveryResolveTotal counts discovery resolve outcomes.
	DiscoveryResolveTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "discovery_resolve_total",
		Help:      "Discovery resolve outcomes (hit, miss, not_established, error).",
	}, []string{labelResult})

	// DiscoveryCacheSize gauges the current discovery cache size.
	DiscoveryCacheSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "discovery_cache_size",
		Help:      "Number of entries in the discovery resolver cache.",
	})

	// ReconcileStageDuration measures per-stage reconcile latency.
	ReconcileStageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns,
		Name:      "reconcile_stage_duration_seconds",
		Help:      "Reconcile stage latency, in addition to controller-runtime defaults.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2.5, 12),
	}, []string{labelController, labelStage})

	// StatusPatchTotal counts status patch outcomes.
	StatusPatchTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "status_patch_total",
		Help:      "Status patch outcomes (changed, unchanged, error).",
	}, []string{labelController, labelResult})

	// MemberResolveErrors counts per-member resolution errors by reason.
	MemberResolveErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "member_resolve_errors_total",
		Help:      "Per-member resolution errors, labelled by reason.",
	}, []string{labelController, labelReason})

	// CRDEstablishedEvents counts CRD Established=True transitions observed by
	// the CRD watcher controller.
	CRDEstablishedEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "crd_established_events_total",
		Help:      "CRD Established=True transitions observed by the CRD watcher.",
	}, []string{labelGroup, labelKind})

	// OwnersWoken counts owner re-enqueues triggered by external events.
	OwnersWoken = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "owners_woken_total",
		Help:      "Owners re-enqueued in response to external events.",
	}, []string{labelReason})
)

// All returns every collector defined by this package.
func All() []prometheus.Collector {
	return []prometheus.Collector{
		Informers,
		Subscribers,
		InformerEvents,
		SubscribeTotal,
		UnsubscribeTotal,
		EventDispatchDuration,
		DiscoveryResolveTotal,
		DiscoveryCacheSize,
		ReconcileStageDuration,
		StatusPatchTotal,
		MemberResolveErrors,
		CRDEstablishedEvents,
		OwnersWoken,
	}
}

// Register adds all package-level metrics to the given registerer. Already-
// registered collectors are tolerated, so repeat calls and double-registration
// against the same registry are no-ops.
func Register(r prometheus.Registerer) error {
	if r == nil {
		return errors.New("metrics: nil registerer")
	}
	for _, c := range All() {
		if err := r.Register(c); err != nil {
			var are prometheus.AlreadyRegisteredError
			if errors.As(err, &are) {
				continue
			}
			return err
		}
	}
	return nil
}
