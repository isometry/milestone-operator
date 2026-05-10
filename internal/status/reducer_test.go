/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package status_test

import (
	"strings"
	"testing"

	apiv1 "github.com/isometry/echelon-operator/api/v1"
	"github.com/isometry/echelon-operator/internal/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	statCurrent     = "Current"
	statInProgress  = "InProgress"
	statFailed      = "Failed"
	statNotFound    = "NotFound"
	statTerminating = "Terminating"
	statUnknown     = "Unknown"
)

func member(s string) status.Member {
	return status.Member{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Kind: "Kustomization", Namespace: "flux-system", Name: "x", Status: s}
}

func TestReduceTarget_EmptySet(t *testing.T) {
	cases := []struct {
		name       string
		policy     apiv1.EmptySetPolicy
		wantReady  metav1.ConditionStatus
		wantReason string
	}{
		{"unknown policy", apiv1.EmptySetUnknown, metav1.ConditionUnknown, apiv1.ReasonEmptySet},
		{"ready policy", apiv1.EmptySetReady, metav1.ConditionTrue, apiv1.ReasonEmptySet},
		{"notready policy", apiv1.EmptySetNotReady, metav1.ConditionFalse, apiv1.ReasonEmptySet},
		{"empty policy defaults to unknown", apiv1.EmptySetPolicy(""), metav1.ConditionUnknown, apiv1.ReasonEmptySet},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := status.ReduceTarget("kustomize.toolkit.fluxcd.io", "v1", "Kustomization", nil, tc.policy)
			if got.Ready != tc.wantReady {
				t.Errorf("Ready = %s, want %s", got.Ready, tc.wantReady)
			}
			if got.Reason != tc.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, tc.wantReason)
			}
			if got.Summary.Total != 0 {
				t.Errorf("Summary.Total = %d, want 0", got.Summary.Total)
			}
			if got.Group != "kustomize.toolkit.fluxcd.io" || got.Version != "v1" || got.Kind != "Kustomization" {
				t.Errorf("identity = (%q,%q,%q), want (kustomize.toolkit.fluxcd.io,v1,Kustomization)", got.Group, got.Version, got.Kind)
			}
		})
	}
}

func TestReduceTarget_Members(t *testing.T) {
	cases := []struct {
		name       string
		statuses   []string
		wantReady  metav1.ConditionStatus
		wantReason string
		wantSum    apiv1.Summary
	}{
		{
			name:       "all current",
			statuses:   []string{statCurrent, statCurrent, statCurrent},
			wantReady:  metav1.ConditionTrue,
			wantReason: apiv1.ReasonAllMembersReady,
			wantSum:    apiv1.Summary{Total: 3, Current: 3},
		},
		{
			name:       "one failed wins over current",
			statuses:   []string{statCurrent, statFailed, statCurrent},
			wantReady:  metav1.ConditionFalse,
			wantReason: apiv1.ReasonMembersNotReady,
			wantSum:    apiv1.Summary{Total: 3, Current: 2, Failed: 1},
		},
		{
			name:       "notfound counts as not ready",
			statuses:   []string{statNotFound, statCurrent},
			wantReady:  metav1.ConditionFalse,
			wantReason: apiv1.ReasonMembersNotReady,
			wantSum:    apiv1.Summary{Total: 2, Current: 1, NotFound: 1},
		},
		{
			name:       "failed wins over inprogress",
			statuses:   []string{statFailed, statInProgress},
			wantReady:  metav1.ConditionFalse,
			wantReason: apiv1.ReasonMembersNotReady,
			wantSum:    apiv1.Summary{Total: 2, Failed: 1, InProgress: 1},
		},
		{
			name:       "inprogress yields unknown",
			statuses:   []string{statCurrent, statInProgress},
			wantReady:  metav1.ConditionUnknown,
			wantReason: apiv1.ReasonMembersInProgress,
			wantSum:    apiv1.Summary{Total: 2, Current: 1, InProgress: 1},
		},
		{
			name:       "terminating yields inprogress reason",
			statuses:   []string{statCurrent, statTerminating},
			wantReady:  metav1.ConditionUnknown,
			wantReason: apiv1.ReasonMembersInProgress,
			wantSum:    apiv1.Summary{Total: 2, Current: 1, Terminating: 1},
		},
		{
			name:       "only-unknown yields MembersUnknown reason",
			statuses:   []string{statUnknown, statUnknown},
			wantReady:  metav1.ConditionUnknown,
			wantReason: apiv1.ReasonMembersUnknown,
			wantSum:    apiv1.Summary{Total: 2, Unknown: 2},
		},
		{
			name:       "inprogress wins over unknown for reason",
			statuses:   []string{statInProgress, statUnknown},
			wantReady:  metav1.ConditionUnknown,
			wantReason: apiv1.ReasonMembersInProgress,
			wantSum:    apiv1.Summary{Total: 2, InProgress: 1, Unknown: 1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms := make([]status.Member, len(tc.statuses))
			for i, s := range tc.statuses {
				ms[i] = member(s)
			}
			got := status.ReduceTarget("kustomize.toolkit.fluxcd.io", "v1", "Kustomization", ms, apiv1.EmptySetUnknown)
			if got.Ready != tc.wantReady {
				t.Errorf("Ready = %s, want %s", got.Ready, tc.wantReady)
			}
			if got.Reason != tc.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, tc.wantReason)
			}
			if got.Summary != tc.wantSum {
				t.Errorf("Summary = %+v, want %+v", got.Summary, tc.wantSum)
			}
		})
	}
}

func TestReduceOwner(t *testing.T) {
	rollup := func(kind string, ready metav1.ConditionStatus) apiv1.TargetRollup {
		return apiv1.TargetRollup{Kind: kind, Ready: ready}
	}
	cases := []struct {
		name        string
		rollups     []apiv1.TargetRollup
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantInMsg   []string // substrings expected in the message
	}{
		{
			name:       "no targets defensive",
			rollups:    nil,
			wantStatus: metav1.ConditionUnknown,
			wantReason: apiv1.ReasonEmptySet,
		},
		{
			name:       "all ready",
			rollups:    []apiv1.TargetRollup{rollup("Kustomization", metav1.ConditionTrue), rollup("HelmRelease", metav1.ConditionTrue)},
			wantStatus: metav1.ConditionTrue,
			wantReason: apiv1.ReasonAllTargetsReady,
		},
		{
			name:       "any false wins",
			rollups:    []apiv1.TargetRollup{rollup("Kustomization", metav1.ConditionTrue), rollup("HelmRelease", metav1.ConditionFalse), rollup("ConfigMap", metav1.ConditionUnknown)},
			wantStatus: metav1.ConditionFalse,
			wantReason: apiv1.ReasonTargetsNotReady,
			wantInMsg:  []string{"HelmRelease"},
		},
		{
			name:       "unknown when mixed without false",
			rollups:    []apiv1.TargetRollup{rollup("Kustomization", metav1.ConditionTrue), rollup("HelmRelease", metav1.ConditionUnknown)},
			wantStatus: metav1.ConditionUnknown,
			wantReason: apiv1.ReasonTargetsInProgress,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotStatus, gotReason, gotMsg := status.ReduceOwner(tc.rollups)
			if gotStatus != tc.wantStatus {
				t.Errorf("status = %s, want %s", gotStatus, tc.wantStatus)
			}
			if gotReason != tc.wantReason {
				t.Errorf("reason = %q, want %q", gotReason, tc.wantReason)
			}
			for _, s := range tc.wantInMsg {
				if !strings.Contains(gotMsg, s) {
					t.Errorf("message %q does not contain %q", gotMsg, s)
				}
			}
		})
	}
}

func TestSummarizeOwner(t *testing.T) {
	rollups := []apiv1.TargetRollup{
		{Kind: "Kustomization", Summary: apiv1.Summary{Total: 3, Current: 2, InProgress: 1}},
		{Kind: "HelmRelease", Summary: apiv1.Summary{Total: 2, Current: 1, Failed: 1}},
		{Kind: "ConfigMap", Summary: apiv1.Summary{Total: 1, NotFound: 1}},
	}
	want := apiv1.Summary{Total: 6, Current: 3, InProgress: 1, Failed: 1, NotFound: 1}
	got := status.SummarizeOwner(rollups)
	if got != want {
		t.Errorf("SummarizeOwner = %+v, want %+v", got, want)
	}
}

func TestSummarizeOwner_Empty(t *testing.T) {
	got := status.SummarizeOwner(nil)
	if got != (apiv1.Summary{}) {
		t.Errorf("SummarizeOwner(nil) = %+v, want zero Summary", got)
	}
}
