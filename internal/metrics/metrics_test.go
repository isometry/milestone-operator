/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package metrics_test

import (
	"strings"
	"testing"
	"time"

	"github.com/isometry/echelon-operator/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRegister_Idempotent(t *testing.T) {
	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := metrics.Register(reg); err != nil {
		t.Fatalf("Register (second call): %v", err)
	}
}

func TestInformers_Gauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	metrics.Informers.WithLabelValues(groupKustomize, "v1", kindKustomization).Set(2)
	got := testutil.ToFloat64(metrics.Informers.WithLabelValues(groupKustomize, "v1", kindKustomization))
	if got != 2 {
		t.Errorf("Informers gauge = %v, want 2", got)
	}
}

func TestInformerEvents_Counter(t *testing.T) {
	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	metrics.InformerEvents.WithLabelValues(groupKustomize, "v1", kindKustomization, "add").Inc()
	metrics.InformerEvents.WithLabelValues(groupKustomize, "v1", kindKustomization, "add").Inc()
	got := testutil.ToFloat64(metrics.InformerEvents.WithLabelValues(groupKustomize, "v1", kindKustomization, "add"))
	if got != 2 {
		t.Errorf("InformerEvents counter = %v, want 2", got)
	}
}

func TestObserveStage_RecordsDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	done := metrics.ObserveStage(kindEchelon, "discovery")
	time.Sleep(2 * time.Millisecond)
	done()
	// We can't easily assert exact duration, but we can confirm the histogram
	// recorded at least one observation (sample count >= 1).
	count := testutil.CollectAndCount(metrics.ReconcileStageDuration, "echelon_reconcile_stage_duration_seconds")
	if count < 1 {
		t.Errorf("histogram series count = %d, want >= 1", count)
	}
}

func TestRegister_AllMetricFamiliesPresent(t *testing.T) {
	reg := prometheus.NewRegistry()
	if err := metrics.Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// Touch every metric so it appears in the registry output (some collectors
	// only expose families with at least one observed series).
	metrics.Informers.WithLabelValues("g", "v", "K").Set(0)
	metrics.Subscribers.WithLabelValues("g", "v", "K").Set(0)
	metrics.InformerEvents.WithLabelValues("g", "v", "K", "add").Inc()
	metrics.SubscribeTotal.WithLabelValues("g", "v", "K", "ok").Inc()
	metrics.UnsubscribeTotal.WithLabelValues("g", "v", "K").Inc()
	metrics.EventDispatchDuration.WithLabelValues("g", "v", "K").Observe(0)
	metrics.DiscoveryResolveTotal.WithLabelValues("hit").Inc()
	metrics.DiscoveryCacheSize.Set(0)
	metrics.ReconcileStageDuration.WithLabelValues(kindEchelon, "discovery").Observe(0)
	metrics.StatusPatchTotal.WithLabelValues(kindEchelon, "changed").Inc()
	metrics.TargetResolveErrors.WithLabelValues(kindEchelon, "GVKNotEstablished").Inc()
	metrics.CRDEstablishedEvents.WithLabelValues(groupKustomize, kindKustomization).Inc()
	metrics.OwnersWoken.WithLabelValues("crd_established").Inc()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	wantNames := []string{
		"echelon_informers",
		"echelon_subscribers",
		"echelon_informer_events_total",
		"echelon_subscribe_total",
		"echelon_unsubscribe_total",
		"echelon_event_dispatch_duration_seconds",
		"echelon_discovery_resolve_total",
		"echelon_discovery_cache_size",
		"echelon_reconcile_stage_duration_seconds",
		"echelon_status_patch_total",
		"echelon_target_resolve_errors_total",
		"echelon_crd_established_events_total",
		"echelon_owners_woken_total",
	}
	have := make(map[string]bool, len(families))
	for _, mf := range families {
		have[mf.GetName()] = true
	}
	for _, n := range wantNames {
		if !have[n] {
			t.Errorf("registry missing metric family %q (have: %s)", n, joinKeys(have))
		}
	}
}

func joinKeys(m map[string]bool) string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return strings.Join(out, ", ")
}
