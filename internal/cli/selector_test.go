/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli_test

import (
	"testing"

	"github.com/isometry/milestone-operator/internal/cli"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	rowFalse   = "ready-false"
	rowTrue    = "ready-true"
	rowStalled = "stalled"
)

func TestParseStatusSelector(t *testing.T) {
	readyFalse := cli.Row{Ready: metav1.ConditionFalse, Stalled: metav1.ConditionFalse}
	readyTrue := cli.Row{Ready: metav1.ConditionTrue, Stalled: metav1.ConditionFalse}
	stalled := cli.Row{Ready: metav1.ConditionUnknown, Stalled: metav1.ConditionTrue}

	tests := []struct {
		name    string
		exprs   []string
		wantErr bool
		matches map[string]bool
	}{
		{name: "empty matches everything", matches: map[string]bool{rowFalse: true, rowTrue: true, rowStalled: true}},
		{name: "ready=False", exprs: []string{"ready=False"}, matches: map[string]bool{rowFalse: true, rowTrue: false, rowStalled: false}},
		{name: "case-insensitive", exprs: []string{"Ready=true"}, matches: map[string]bool{rowFalse: false, rowTrue: true, rowStalled: false}},
		{name: "unknown", exprs: []string{"ready=unknown"}, matches: map[string]bool{rowFalse: false, rowTrue: false, rowStalled: true}},
		{name: "stalled condition", exprs: []string{"stalled=True"}, matches: map[string]bool{rowFalse: false, rowTrue: false, rowStalled: true}},
		{name: "AND", exprs: []string{"ready=Unknown", "stalled=False"}, matches: map[string]bool{rowFalse: false, rowTrue: false, rowStalled: false}},
		{name: "spaces tolerated", exprs: []string{" ready = False "}, matches: map[string]bool{rowFalse: true, rowTrue: false, rowStalled: false}},
		{name: "missing =", exprs: []string{"ready"}, wantErr: true},
		{name: "unknown key", exprs: []string{"reason=Foo"}, wantErr: true},
		{name: "bad value", exprs: []string{"ready=maybe"}, wantErr: true},
		{name: "stalled=Unknown rejected", exprs: []string{"stalled=bogus"}, wantErr: true},
	}
	rows := map[string]cli.Row{rowFalse: readyFalse, rowTrue: readyTrue, rowStalled: stalled}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel, err := cli.ParseStatusSelector(tt.exprs)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			for key, want := range tt.matches {
				if got := sel.Matches(rows[key]); got != want {
					t.Errorf("Matches(%s) = %v, want %v", key, got, want)
				}
			}
		})
	}
}
