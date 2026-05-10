/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package status reduces per-resource kstatus into per-target and owner-level
// readiness rollups.
package status

import (
	"strings"

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Member identifies a single resource and its computed kstatus.
type Member struct {
	Group, Version, Kind  string
	Namespace, Name       string
	Status, Reason        string
	Message               string
}

// ReduceTarget reduces members of one target into a TargetRollup. When the
// member set is empty, the target's EmptySetPolicy controls Ready reporting.
func ReduceTarget(group, version, kind string, members []Member, policy apiv1.EmptySetPolicy) apiv1.TargetRollup {
	rollup := apiv1.TargetRollup{Group: group, Version: version, Kind: kind}

	for _, m := range members {
		rollup.Summary.Total++
		switch m.Status {
		case "Current":
			rollup.Summary.Current++
		case "InProgress":
			rollup.Summary.InProgress++
		case "Failed":
			rollup.Summary.Failed++
		case "NotFound":
			rollup.Summary.NotFound++
		case "Terminating":
			rollup.Summary.Terminating++
		default:
			rollup.Summary.Unknown++
		}
	}

	if rollup.Summary.Total == 0 {
		rollup.Reason = apiv1.ReasonEmptySet
		switch policy {
		case apiv1.EmptySetReady:
			rollup.Ready = metav1.ConditionTrue
		case apiv1.EmptySetNotReady:
			rollup.Ready = metav1.ConditionFalse
		default:
			rollup.Ready = metav1.ConditionUnknown
		}
		return rollup
	}

	switch {
	case rollup.Summary.Failed > 0 || rollup.Summary.NotFound > 0:
		rollup.Ready = metav1.ConditionFalse
		rollup.Reason = apiv1.ReasonMembersNotReady
	case rollup.Summary.InProgress > 0 || rollup.Summary.Terminating > 0:
		rollup.Ready = metav1.ConditionUnknown
		rollup.Reason = apiv1.ReasonMembersInProgress
	case rollup.Summary.Unknown > 0:
		rollup.Ready = metav1.ConditionUnknown
		rollup.Reason = apiv1.ReasonMembersUnknown
	default:
		rollup.Ready = metav1.ConditionTrue
		rollup.Reason = apiv1.ReasonAllMembersReady
	}
	return rollup
}

// ReduceOwner reduces per-target rollups into the owner-level Ready status,
// reason, and human-readable message.
func ReduceOwner(rollups []apiv1.TargetRollup) (metav1.ConditionStatus, string, string) {
	if len(rollups) == 0 {
		return metav1.ConditionUnknown, apiv1.ReasonEmptySet, "no targets configured"
	}
	var failed []string
	allTrue := true
	for _, r := range rollups {
		switch r.Ready {
		case metav1.ConditionFalse:
			failed = append(failed, r.Kind)
			allTrue = false
		case metav1.ConditionTrue:
		default:
			allTrue = false
		}
	}
	switch {
	case len(failed) > 0:
		return metav1.ConditionFalse, apiv1.ReasonTargetsNotReady, "kinds not ready: " + strings.Join(failed, ", ")
	case allTrue:
		return metav1.ConditionTrue, apiv1.ReasonAllTargetsReady, ""
	default:
		return metav1.ConditionUnknown, apiv1.ReasonTargetsInProgress, ""
	}
}

// SummarizeOwner aggregates per-target Summaries into an owner-level Summary.
func SummarizeOwner(rollups []apiv1.TargetRollup) apiv1.Summary {
	var total apiv1.Summary
	for _, r := range rollups {
		total.Total += r.Summary.Total
		total.Current += r.Summary.Current
		total.InProgress += r.Summary.InProgress
		total.Failed += r.Summary.Failed
		total.NotFound += r.Summary.NotFound
		total.Terminating += r.Summary.Terminating
		total.Unknown += r.Summary.Unknown
	}
	return total
}
