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

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StateLister is the abstraction the collector uses to walk Milestone and
// ClusterMilestone objects at scrape time. In production this is
// implemented by a controller-runtime cache-backed lister; tests inject a
// fake. The ctx is the manager's signal context — when shutdown begins,
// in-flight scrapes fail soft (return empty slices) rather than blocking.
type StateLister interface {
	ListMilestones(ctx context.Context) []apiv1.Milestone
	ListClusterMilestones(ctx context.Context) []apiv1.ClusterMilestone
}

// StateCollector emits per-object gauges for every Milestone/ClusterMilestone
// at scrape time. Lister-backed so a deleted object's series disappears
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
	dependencyResources    *prometheus.Desc
	dependencyReady        *prometheus.Desc
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
			"milestone_status_condition",
			"1 when the named condition has the named status, 0 otherwise.",
			[]string{labelOwnerKind, labelNamespace, labelName, labelType, labelStatus}, nil,
		),
		observedGeneration: prometheus.NewDesc(
			"milestone_observed_generation",
			"metadata.generation observed at the last successful reconcile.",
			[]string{labelOwnerKind, labelNamespace, labelName}, nil,
		),
		dependencyResources: prometheus.NewDesc(
			"milestone_dependency_resources",
			"Per-dependency resource counts by kstatus bucket; status label includes 'total'.",
			[]string{labelOwnerKind, labelNamespace, labelName, labelDependency, labelTargetGroup, labelTargetKind, labelStatus}, nil,
		),
		dependencyReady: prometheus.NewDesc(
			"milestone_dependency_ready",
			"Per-dependency Ready encoded as 1=True, 0=False, -1=Unknown.",
			[]string{labelOwnerKind, labelNamespace, labelName, labelDependency, labelTargetGroup, labelTargetKind}, nil,
		),
		lastEvaluatedTimestamp: prometheus.NewDesc(
			"milestone_last_evaluated_timestamp_seconds",
			"Unix timestamp of the last status evaluation.",
			[]string{labelOwnerKind, labelNamespace, labelName}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *StateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.statusCondition
	ch <- c.observedGeneration
	ch <- c.dependencyResources
	ch <- c.dependencyReady
	ch <- c.lastEvaluatedTimestamp
}

// Collect implements prometheus.Collector.
func (c *StateCollector) Collect(ch chan<- prometheus.Metric) {
	for _, m := range c.lister.ListMilestones(c.base) {
		c.emit(ch, "Milestone", m.GetNamespace(), m.GetName(), &m.Status.MilestoneStatusBase)
	}
	for _, m := range c.lister.ListClusterMilestones(c.base) {
		c.emit(ch, "ClusterMilestone", "", m.GetName(), &m.Status.MilestoneStatusBase)
	}
}

func (c *StateCollector) emit(ch chan<- prometheus.Metric, kind, namespace, name string, sb *apiv1.MilestoneStatusBase) {
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

	// status.DependsOn is already sorted by Name (reconciler invariant).
	for _, d := range sb.DependsOn {
		ch <- prometheus.MustNewConstMetric(c.dependencyReady, prometheus.GaugeValue, encodeReady(d.Ready), kind, namespace, name, d.Name, d.Group, d.Kind)
		emit := func(bucket string, v int32) {
			ch <- prometheus.MustNewConstMetric(c.dependencyResources, prometheus.GaugeValue, float64(v), kind, namespace, name, d.Name, d.Group, d.Kind, bucket)
		}
		emit("total", d.Summary.Total)
		emit("current", d.Summary.Current)
		emit("inProgress", d.Summary.InProgress)
		emit("failed", d.Summary.Failed)
		emit("notFound", d.Summary.NotFound)
		emit("terminating", d.Summary.Terminating)
		emit("unknown", d.Summary.Unknown)
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
