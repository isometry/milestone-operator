/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package metrics

import (
	"context"

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StateLister is the abstraction the collector uses to walk Echelon and
// ClusterEchelon objects at scrape time. In production this is implemented by
// a controller-runtime cache-backed lister; tests inject a fake. The ctx is
// the manager's signal context — when shutdown begins, in-flight scrapes fail
// soft (return empty slices) rather than blocking.
type StateLister interface {
	ListEchelons(ctx context.Context) []apiv1.Echelon
	ListClusterEchelons(ctx context.Context) []apiv1.ClusterEchelon
}

// StateCollector emits per-object gauges for every Echelon/ClusterEchelon at
// scrape time. Lister-backed so a deleted object's series disappears
// immediately on the next scrape, with no per-reconcile bookkeeping.
//
// The Prometheus Collector interface predates context.Context, so the base
// context is captured at construction; it is the manager's signal context and
// becomes Done on shutdown.
type StateCollector struct {
	base   context.Context
	lister StateLister

	statusCondition        *prometheus.Desc
	observedGeneration     *prometheus.Desc
	targetMembers          *prometheus.Desc
	targetReady            *prometheus.Desc
	lastEvaluatedTimestamp *prometheus.Desc
}

// NewStateCollector returns a StateCollector backed by lister. ctx is used for
// every List call made during a scrape; it should be the manager's signal
// context so scrapes-in-flight unblock during shutdown.
func NewStateCollector(ctx context.Context, lister StateLister) *StateCollector {
	return &StateCollector{
		base:   ctx,
		lister: lister,
		statusCondition: prometheus.NewDesc(
			"echelon_status_condition",
			"1 when the named condition has the named status, 0 otherwise.",
			[]string{"owner_kind", "namespace", "name", "type", "status"}, nil,
		),
		observedGeneration: prometheus.NewDesc(
			"echelon_observed_generation",
			"metadata.generation observed at the last successful reconcile.",
			[]string{"owner_kind", "namespace", "name"}, nil,
		),
		targetMembers: prometheus.NewDesc(
			"echelon_target_members",
			"Per-target member counts by kstatus bucket; status label includes 'total'.",
			[]string{"owner_kind", "namespace", "name", "target_group", "target_kind", "status"}, nil,
		),
		targetReady: prometheus.NewDesc(
			"echelon_target_ready",
			"Per-target Ready encoded as 1=True, 0=False, -1=Unknown.",
			[]string{"owner_kind", "namespace", "name", "target_group", "target_kind"}, nil,
		),
		lastEvaluatedTimestamp: prometheus.NewDesc(
			"echelon_last_evaluated_timestamp_seconds",
			"Unix timestamp of the last status evaluation.",
			[]string{"owner_kind", "namespace", "name"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *StateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.statusCondition
	ch <- c.observedGeneration
	ch <- c.targetMembers
	ch <- c.targetReady
	ch <- c.lastEvaluatedTimestamp
}

// Collect implements prometheus.Collector.
func (c *StateCollector) Collect(ch chan<- prometheus.Metric) {
	for _, e := range c.lister.ListEchelons(c.base) {
		c.emit(ch, "Echelon", e.GetNamespace(), e.GetName(), &e.Status.EchelonStatusBase)
	}
	for _, e := range c.lister.ListClusterEchelons(c.base) {
		c.emit(ch, "ClusterEchelon", "", e.GetName(), &e.Status.EchelonStatusBase)
	}
}

func (c *StateCollector) emit(ch chan<- prometheus.Metric, kind, namespace, name string, sb *apiv1.EchelonStatusBase) {
	ch <- prometheus.MustNewConstMetric(c.observedGeneration, prometheus.GaugeValue, float64(sb.ObservedGeneration), kind, namespace, name)

	if !sb.LastEvaluatedTime.IsZero() {
		ch <- prometheus.MustNewConstMetric(c.lastEvaluatedTimestamp, prometheus.GaugeValue, float64(sb.LastEvaluatedTime.Unix()), kind, namespace, name)
	}

	for _, cond := range sb.Conditions {
		for _, s := range []metav1.ConditionStatus{metav1.ConditionTrue, metav1.ConditionFalse, metav1.ConditionUnknown} {
			v := 0.0
			if cond.Status == s {
				v = 1
			}
			ch <- prometheus.MustNewConstMetric(c.statusCondition, prometheus.GaugeValue, v, kind, namespace, name, cond.Type, string(s))
		}
	}

	for _, t := range sb.Targets {
		ch <- prometheus.MustNewConstMetric(c.targetReady, prometheus.GaugeValue, encodeReady(t.Ready), kind, namespace, name, t.Group, t.Kind)
		emit := func(bucket string, v int32) {
			ch <- prometheus.MustNewConstMetric(c.targetMembers, prometheus.GaugeValue, float64(v), kind, namespace, name, t.Group, t.Kind, bucket)
		}
		emit("total", t.Summary.Total)
		emit("current", t.Summary.Current)
		emit("inProgress", t.Summary.InProgress)
		emit("failed", t.Summary.Failed)
		emit("notFound", t.Summary.NotFound)
		emit("terminating", t.Summary.Terminating)
		emit("unknown", t.Summary.Unknown)
	}
}

func encodeReady(s metav1.ConditionStatus) float64 {
	switch s {
	case metav1.ConditionTrue:
		return 1
	case metav1.ConditionFalse:
		return 0
	default:
		return -1
	}
}
