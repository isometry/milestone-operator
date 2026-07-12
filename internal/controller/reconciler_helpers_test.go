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
	"reflect"
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
		// The cap is a byte budget (status size is what matters to etcd) and
		// includes the ellipsis: ASCII input fills it exactly.
		if len(got) != controller.MaxStalledErrChars {
			t.Errorf("byte length = %d, want %d", len(got), controller.MaxStalledErrChars)
		}
		if !strings.HasSuffix(got, "…") {
			t.Errorf("missing ellipsis suffix: %q", got[len(got)-10:])
		}
	})

	t.Run("multibyte input never yields invalid UTF-8", func(t *testing.T) {
		// 2-byte runes guarantee some cut index would split a rune; the
		// apiserver would coerce that to U+FFFD on write, so the stored
		// message would never DeepEqual the recomputed one — a permanent
		// status-churn loop.
		input := strings.Repeat("é", controller.MaxStalledErrChars)
		got := controller.SanitiseErrText(errors.New(input))
		if !utf8.ValidString(got) {
			t.Errorf("result is invalid UTF-8: %q", got)
		}
		if len(got) > controller.MaxStalledErrChars {
			t.Errorf("byte length = %d, want <= %d", len(got), controller.MaxStalledErrChars)
		}
		if !strings.HasSuffix(got, "…") {
			t.Errorf("missing ellipsis suffix")
		}
	})
}

func TestDedupeAndSortResources(t *testing.T) {
	rs := func(group, kind, ns, name string) apiv1.ResourceStatus {
		return apiv1.ResourceStatus{Group: group, Version: "v1", Kind: kind, Namespace: ns, Name: name, Status: "InProgress"}
	}

	t.Run("nil in, nil out", func(t *testing.T) {
		if got := controller.DedupeAndSortResources(nil); got != nil {
			t.Errorf("want nil for empty input, got %+v", got)
		}
	})

	t.Run("sorts by group, kind, namespace, name", func(t *testing.T) {
		in := []apiv1.ResourceStatus{
			rs("b.example", "Widget", "ns1", "x"),
			rs("a.example", "Widget", "ns2", "y"),
			rs("a.example", "Widget", "ns1", "z"),
			rs("a.example", "Gadget", "ns9", "a"),
		}
		got := controller.DedupeAndSortResources(in)
		want := []apiv1.ResourceStatus{
			rs("a.example", "Gadget", "ns9", "a"),
			rs("a.example", "Widget", "ns1", "z"),
			rs("a.example", "Widget", "ns2", "y"),
			rs("b.example", "Widget", "ns1", "x"),
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("sort order wrong:\n got %+v\nwant %+v", got, want)
		}
	})

	t.Run("dedupes by full resource identity", func(t *testing.T) {
		in := []apiv1.ResourceStatus{
			rs("a.example", "Widget", "ns1", "x"),
			rs("a.example", "Widget", "ns1", "x"), // same object via a second overlapping dependency
			rs("a.example", "Widget", "ns1", "y"),
		}
		got := controller.DedupeAndSortResources(in)
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2: %+v", len(got), got)
		}
		if got[0].Name != "x" || got[1].Name != "y" {
			t.Errorf("unexpected contents: %+v", got)
		}
	})

	t.Run("stable across permutations", func(t *testing.T) {
		in := []apiv1.ResourceStatus{
			rs("g", "K", "n1", "b"),
			rs("g", "K", "n1", "a"),
			rs("g", "K", "n2", "a"),
		}
		perm := []apiv1.ResourceStatus{in[2], in[0], in[1]}
		if !reflect.DeepEqual(controller.DedupeAndSortResources(in), controller.DedupeAndSortResources(perm)) {
			t.Errorf("output differs across input permutations")
		}
	})
}

func TestTruncateWithEllipsis(t *testing.T) {
	const max = 16

	t.Run("short strings pass through", func(t *testing.T) {
		for _, s := range []string{"", "abc", strings.Repeat("x", max)} {
			if got := controller.TruncateWithEllipsis(s, max); got != s {
				t.Errorf("TruncateWithEllipsis(%q) = %q, want unchanged", s, got)
			}
		}
	})

	t.Run("ASCII truncation is byte-exact including ellipsis", func(t *testing.T) {
		got := controller.TruncateWithEllipsis(strings.Repeat("x", 100), max)
		if len(got) != max {
			t.Errorf("byte length = %d, want %d", len(got), max)
		}
		if !strings.HasSuffix(got, "…") {
			t.Errorf("missing ellipsis suffix: %q", got)
		}
	})

	t.Run("never splits a multi-byte rune", func(t *testing.T) {
		// Try every prefix length of a 2-byte-rune string so at least one
		// naive byte cut would land mid-rune.
		s := strings.Repeat("é", 32)
		for budget := 3; budget <= len(s); budget++ {
			got := controller.TruncateWithEllipsis(s, budget)
			if !utf8.ValidString(got) {
				t.Fatalf("budget %d: invalid UTF-8: %q", budget, got)
			}
			if len(got) > budget {
				t.Fatalf("budget %d: byte length %d exceeds budget", budget, len(got))
			}
		}
	})

	t.Run("idempotent", func(t *testing.T) {
		once := controller.TruncateWithEllipsis(strings.Repeat("héllo…", 40), max)
		twice := controller.TruncateWithEllipsis(once, max)
		if once != twice {
			t.Errorf("not idempotent: %q vs %q", once, twice)
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
