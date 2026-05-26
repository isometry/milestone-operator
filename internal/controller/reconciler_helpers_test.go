/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller_test

import (
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/controller"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSanitiseErrText(t *testing.T) {
	t.Run("nil error returns empty", func(t *testing.T) {
		if got := controller.SanitiseErrText(nil); got != "" {
			t.Fatalf("SanitiseErrText(nil) = %q, want \"\"", got)
		}
	})

	t.Run("plain text passes through", func(t *testing.T) {
		got := controller.SanitiseErrText(errors.New("kaboom"))
		if got != "kaboom" {
			t.Fatalf("SanitiseErrText(\"kaboom\") = %q, want \"kaboom\"", got)
		}
	})

	t.Run("collapses mixed newlines to spaces", func(t *testing.T) {
		// Mix of \r\n, \n, and \r — the implementation rewrites in that
		// order, so we must end up with no line breaks of any flavour.
		got := controller.SanitiseErrText(errors.New("a\r\nb\nc\rd"))
		if got != "a b c d" {
			t.Fatalf("SanitiseErrText with mixed newlines = %q, want %q", got, "a b c d")
		}
		if strings.ContainsAny(got, "\r\n") {
			t.Errorf("result still contains line breaks: %q", got)
		}
	})

	t.Run("truncates oversized errors with ellipsis", func(t *testing.T) {
		input := strings.Repeat("x", controller.MaxStalledErrChars+50)
		got := controller.SanitiseErrText(errors.New(input))
		// Rune count must be exactly MaxStalledErrChars: (MaxStalledErrChars-1)
		// retained runes + 1 ellipsis rune.
		if n := utf8.RuneCountInString(got); n != controller.MaxStalledErrChars {
			t.Errorf("rune count = %d, want %d", n, controller.MaxStalledErrChars)
		}
		if !strings.HasSuffix(got, "…") {
			t.Errorf("missing ellipsis suffix: %q", got[len(got)-10:])
		}
	})
}

// TestStripTransitionTimes_DeliberatelyZeroesObservedGeneration locks in the
// subtlest invariant in the reconciler's idempotency story: per-condition
// ObservedGeneration MUST be zeroed before the DeepEqual comparison.
//
// Why: reconciler.go:617–623 zeroes ObservedGeneration *deliberately* because
// meta.SetStatusCondition rewrites it on every call. If a future "tidy-up"
// removes that line, identical reconciles will start producing patches on
// every pass — status object churns, Flux notify fires repeatedly, and the
// status-patch-idempotency contract documented in PLAN.md silently breaks.
//
// Any change that makes this test fail should re-read reconciler.go:617–623
// before being merged.
func TestStripTransitionTimes_DeliberatelyZeroesObservedGeneration(t *testing.T) {
	earlier := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	later := metav1.NewTime(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	a := []metav1.Condition{{
		Type: apiv1.ConditionReady, Status: metav1.ConditionTrue,
		Reason: apiv1.ReasonAllDependenciesReady, Message: "ok",
		LastTransitionTime: earlier, ObservedGeneration: 1,
	}}
	b := []metav1.Condition{{
		Type: apiv1.ConditionReady, Status: metav1.ConditionTrue,
		Reason: apiv1.ReasonAllDependenciesReady, Message: "ok",
		LastTransitionTime: later, ObservedGeneration: 99, // both differ
	}}
	gotA := controller.StripTransitionTimes(a)
	gotB := controller.StripTransitionTimes(b)
	if len(gotA) != 1 || len(gotB) != 1 {
		t.Fatalf("unexpected output lengths: %d / %d", len(gotA), len(gotB))
	}
	if !gotA[0].LastTransitionTime.IsZero() || !gotB[0].LastTransitionTime.IsZero() {
		t.Errorf("LastTransitionTime not zeroed: %+v / %+v", gotA[0].LastTransitionTime, gotB[0].LastTransitionTime)
	}
	if gotA[0].ObservedGeneration != 0 || gotB[0].ObservedGeneration != 0 {
		t.Errorf("ObservedGeneration not zeroed: %d / %d (the load-bearing invariant — see reconciler.go:617–623)",
			gotA[0].ObservedGeneration, gotB[0].ObservedGeneration)
	}
	// Substantive fields must survive.
	if gotA[0].Type != apiv1.ConditionReady || gotA[0].Status != metav1.ConditionTrue ||
		gotA[0].Reason != apiv1.ReasonAllDependenciesReady || gotA[0].Message != "ok" {
		t.Errorf("substantive fields not preserved: %+v", gotA[0])
	}
}

func TestStripTransitionTimes_ReturnsCopy(t *testing.T) {
	in := []metav1.Condition{{Type: "T", LastTransitionTime: metav1.NewTime(time.Now())}}
	out := controller.StripTransitionTimes(in)
	if in[0].LastTransitionTime.IsZero() {
		t.Fatalf("input was mutated: stripTransitionTimes must return a copy")
	}
	if !out[0].LastTransitionTime.IsZero() {
		t.Fatalf("output LastTransitionTime not zeroed")
	}
}

func TestStatusEqualIgnoringTimestamp(t *testing.T) {
	cond := func(obsGen int64, when metav1.Time) metav1.Condition {
		return metav1.Condition{
			Type: apiv1.ConditionReady, Status: metav1.ConditionTrue,
			Reason: apiv1.ReasonAllDependenciesReady, Message: "ok",
			LastTransitionTime: when, ObservedGeneration: obsGen,
		}
	}
	now := metav1.NewTime(time.Now())
	later := metav1.NewTime(time.Now().Add(time.Hour))

	t.Run("equal modulo LastEvaluatedTime", func(t *testing.T) {
		a := apiv1.MilestoneStatusBase{
			Conditions:        []metav1.Condition{cond(1, now)},
			LastEvaluatedTime: now,
		}
		b := apiv1.MilestoneStatusBase{
			Conditions:        []metav1.Condition{cond(1, now)},
			LastEvaluatedTime: later, // only differs here
		}
		if !controller.StatusEqualIgnoringTimestamp(a, b) {
			t.Fatalf("expected equal when only LastEvaluatedTime differs")
		}
	})

	t.Run("equal modulo per-condition ObservedGeneration", func(t *testing.T) {
		// The load-bearing case: meta.SetStatusCondition stamps per-condition
		// ObservedGeneration on every call. Comparison MUST ignore it to keep
		// reconciles idempotent.
		a := apiv1.MilestoneStatusBase{Conditions: []metav1.Condition{cond(1, now)}}
		b := apiv1.MilestoneStatusBase{Conditions: []metav1.Condition{cond(99, later)}}
		if !controller.StatusEqualIgnoringTimestamp(a, b) {
			t.Fatalf("expected equal when only per-condition ObservedGeneration + LastTransitionTime differ")
		}
	})

	t.Run("not equal on substantive condition change", func(t *testing.T) {
		a := apiv1.MilestoneStatusBase{Conditions: []metav1.Condition{cond(1, now)}}
		other := cond(1, now)
		other.Reason = apiv1.ReasonDependenciesNotReady
		b := apiv1.MilestoneStatusBase{Conditions: []metav1.Condition{other}}
		if controller.StatusEqualIgnoringTimestamp(a, b) {
			t.Fatalf("expected NOT equal when Ready reason differs")
		}
	})

	t.Run("not equal on top-level ObservedGeneration change", func(t *testing.T) {
		// Top-level status.observedGeneration is authoritative freshness and
		// MUST participate in the comparison. (Distinct from per-condition obsGen.)
		a := apiv1.MilestoneStatusBase{
			Conditions:         []metav1.Condition{cond(1, now)},
			ObservedGeneration: 5,
		}
		b := apiv1.MilestoneStatusBase{
			Conditions:         []metav1.Condition{cond(1, now)},
			ObservedGeneration: 6,
		}
		if controller.StatusEqualIgnoringTimestamp(a, b) {
			t.Fatalf("expected NOT equal when top-level ObservedGeneration differs")
		}
	})
}
