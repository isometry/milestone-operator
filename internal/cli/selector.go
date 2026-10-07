/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StatusSelector filters rows by recorded condition status. Every term must
// match.
type StatusSelector struct {
	terms []statusTerm
}

type statusTerm struct {
	condition string
	want      metav1.ConditionStatus
}

var conditionStatuses = []metav1.ConditionStatus{
	metav1.ConditionTrue, metav1.ConditionFalse, metav1.ConditionUnknown,
}

// ParseStatusSelector parses terms of the form ready=True|False|Unknown or
// stalled=True|False|Unknown. Keys and values are case-insensitive.
func ParseStatusSelector(exprs []string) (StatusSelector, error) {
	var sel StatusSelector
	for _, expr := range exprs {
		key, value, ok := strings.Cut(expr, "=")
		if !ok {
			return StatusSelector{}, fmt.Errorf("invalid status selector %q: want ready=<status> or stalled=<status>", expr)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		if key != "ready" && key != "stalled" {
			return StatusSelector{}, fmt.Errorf("invalid status selector %q: key must be ready or stalled", expr)
		}
		want, err := parseConditionStatus(strings.TrimSpace(value))
		if err != nil {
			return StatusSelector{}, fmt.Errorf("invalid status selector %q: %w", expr, err)
		}
		sel.terms = append(sel.terms, statusTerm{condition: key, want: want})
	}
	return sel, nil
}

func parseConditionStatus(s string) (metav1.ConditionStatus, error) {
	for _, cs := range conditionStatuses {
		if strings.EqualFold(s, string(cs)) {
			return cs, nil
		}
	}
	return "", fmt.Errorf("status must be True, False or Unknown, not %q", s)
}

// Matches reports whether r satisfies every term.
func (s StatusSelector) Matches(r Row) bool {
	for _, t := range s.terms {
		got := r.Ready
		if t.condition == "stalled" {
			got = r.Stalled
		}
		if got != t.want {
			return false
		}
	}
	return true
}
