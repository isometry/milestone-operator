/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package metrics

import "github.com/prometheus/client_golang/prometheus"

// Stage labels used in echelon_reconcile_stage_duration_seconds.
const (
	StageDiscovery     = "discovery"
	StageSubscriptions = "subscriptions"
	StageList          = "list"
	StageCompute       = "compute"
	StageReduce        = "reduce"
	StagePatch         = "patch"
)

// Patch result labels used in echelon_status_patch_total.
const (
	PatchChanged   = "changed"
	PatchUnchanged = "unchanged"
	PatchError     = "error"
)

// Subscribe result labels used in echelon_subscribe_total.
const (
	SubscribeOK    = "ok"
	SubscribeError = "error"
)

// Discovery result labels used in echelon_discovery_resolve_total.
const (
	DiscoveryHit            = "hit"
	DiscoveryMiss           = "miss"
	DiscoveryNotEstablished = "not_established"
	DiscoveryError          = "error"
)

// Informer event labels used in echelon_informer_events_total.
const (
	EventAdd    = "add"
	EventUpdate = "update"
	EventDelete = "delete"
)

// ObserveStage starts a timer for a named reconcile stage. Defer the returned
// closure to record the elapsed time:
//
//	defer metrics.ObserveStage("Echelon", metrics.StageDiscovery)()
func ObserveStage(controller, stage string) func() {
	t := prometheus.NewTimer(ReconcileStageDuration.WithLabelValues(controller, stage))
	return func() { t.ObserveDuration() }
}

// ObserveDispatch starts a timer for an informer event dispatch. Defer the
// returned closure to record the elapsed time.
func ObserveDispatch(group, version, kind string) func() {
	t := prometheus.NewTimer(EventDispatchDuration.WithLabelValues(group, version, kind))
	return func() { t.ObserveDuration() }
}
