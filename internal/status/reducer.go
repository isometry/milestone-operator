/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package status reduces per-resource kstatus into per-member and owner-level
// readiness rollups.
package status

import (
	"sort"
	"strings"

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Resource identifies a single matched resource and its computed kstatus.
type Resource struct {
	Group, Version, Kind string
	Namespace, Name      string
	Status, Reason       string
	Message              string
}

// ReduceMember reduces resources of one member into a MemberRollup. When the
// resource set is empty, the member's EmptySetPolicy controls Ready reporting.
func ReduceMember(group, version, kind string, resources []Resource, policy apiv1.EmptySetPolicy) apiv1.MemberRollup {
	rollup := apiv1.MemberRollup{Group: group, Version: version, Kind: kind}

	for _, r := range resources {
		rollup.Summary.Total++
		switch r.Status {
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
		rollup.Reason = apiv1.ReasonResourcesNotReady
	case rollup.Summary.InProgress > 0 || rollup.Summary.Terminating > 0:
		rollup.Ready = metav1.ConditionUnknown
		rollup.Reason = apiv1.ReasonResourcesInProgress
	case rollup.Summary.Unknown > 0:
		rollup.Ready = metav1.ConditionUnknown
		rollup.Reason = apiv1.ReasonResourcesUnknown
	default:
		rollup.Ready = metav1.ConditionTrue
		rollup.Reason = apiv1.ReasonAllResourcesReady
	}
	return rollup
}

// ReduceOwner reduces per-member rollups into the owner-level Ready status,
// reason, and human-readable message. The not-ready member names are sorted
// so the message is stable across reconciles (Go map iteration order is not).
func ReduceOwner(rollups map[string]apiv1.MemberRollup) (metav1.ConditionStatus, string, string) {
	if len(rollups) == 0 {
		return metav1.ConditionUnknown, apiv1.ReasonEmptySet, "no members configured"
	}
	var failed []string
	allTrue := true
	for name, r := range rollups {
		switch r.Ready {
		case metav1.ConditionFalse:
			failed = append(failed, name)
			allTrue = false
		case metav1.ConditionTrue:
		default:
			allTrue = false
		}
	}
	switch {
	case len(failed) > 0:
		sort.Strings(failed)
		return metav1.ConditionFalse, apiv1.ReasonMembersNotReady, "members not ready: " + strings.Join(failed, ", ")
	case allTrue:
		return metav1.ConditionTrue, apiv1.ReasonAllMembersReady, ""
	default:
		return metav1.ConditionUnknown, apiv1.ReasonMembersInProgress, ""
	}
}

// SummarizeOwner aggregates per-member Summaries into an owner-level Summary.
// Order-independent: addition is commutative, map iteration order does not
// affect the result.
func SummarizeOwner(rollups map[string]apiv1.MemberRollup) apiv1.Summary {
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
