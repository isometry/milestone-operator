/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli_test

import (
	"bytes"
	"os"
	"testing"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/cli"
	"github.com/isometry/milestone-operator/internal/membership"
	"github.com/isometry/milestone-operator/internal/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConditionGlyph(t *testing.T) {
	tests := []struct {
		ready  metav1.ConditionStatus
		reason string
		want   cli.Glyph
	}{
		{metav1.ConditionTrue, apiv1.ReasonAllResourcesReady, cli.GlyphReady},
		{metav1.ConditionFalse, apiv1.ReasonResourcesNotReady, cli.GlyphNotReady},
		{metav1.ConditionFalse, apiv1.ReasonResourcesSuspended, cli.GlyphSuspended},
		{metav1.ConditionUnknown, apiv1.ReasonResourcesInProgress, cli.GlyphInProgress},
		{metav1.ConditionUnknown, apiv1.ReasonDependenciesInProgress, cli.GlyphInProgress},
		{metav1.ConditionUnknown, apiv1.ReasonListFailed, cli.GlyphUnknown},
	}
	for _, tt := range tests {
		if got := cli.ConditionGlyph(tt.ready, tt.reason); got != tt.want {
			t.Errorf("ConditionGlyph(%s, %s) = %s, want %s", tt.ready, tt.reason, got, tt.want)
		}
	}
}

func TestMemberGlyph(t *testing.T) {
	tests := []struct {
		status    string
		suspended bool
		want      cli.Glyph
	}{
		{"Current", false, cli.GlyphReady},
		{"Current", true, cli.GlyphSuspended},
		{"InProgress", false, cli.GlyphInProgress},
		{"Terminating", false, cli.GlyphInProgress},
		{"Failed", false, cli.GlyphNotReady},
		{"NotFound", false, cli.GlyphNotReady},
		{"Unknown", false, cli.GlyphUnknown},
	}
	for _, tt := range tests {
		m := membership.Member{Resource: status.Resource{Status: tt.status, Suspended: tt.suspended}}
		if got := cli.MemberGlyph(m); got != tt.want {
			t.Errorf("MemberGlyph(%s, %v) = %s, want %s", tt.status, tt.suspended, got, tt.want)
		}
	}
}

func TestStyle(t *testing.T) {
	plain := cli.Style{}
	if plain.Glyph(cli.GlyphNotReady) != "✗" || plain.Bold("x") != "x" || plain.Faint("x") != "x" {
		t.Error("plain style must not emit escapes")
	}
	c := cli.Style{Color: true}
	if got := c.Glyph(cli.GlyphNotReady); got != "\x1b[31m✗\x1b[0m" {
		t.Errorf("red cross = %q", got)
	}
	if c.Bold("") != "" {
		t.Error("empty text must stay empty")
	}
}

func TestNewStyle(t *testing.T) {
	if cli.NewStyle(&bytes.Buffer{}, false).Color {
		t.Error("a buffer is not a terminal")
	}
	if cli.NewStyle(os.Stdout, true).Color {
		t.Error("--no-color must win")
	}
	t.Setenv("NO_COLOR", "1")
	if cli.NewStyle(os.Stdout, false).Color {
		t.Error("NO_COLOR must win")
	}
}
