/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli

import (
	"io"
	"os"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/membership"
	"golang.org/x/term"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kstatus "sigs.k8s.io/cli-utils/pkg/kstatus/status"
)

// Glyph is a one-character verdict marker.
type Glyph string

const (
	GlyphReady      Glyph = "✔"
	GlyphNotReady   Glyph = "✗"
	GlyphSuspended  Glyph = "⏸"
	GlyphInProgress Glyph = "◌"
	GlyphUnknown    Glyph = "?"
	GlyphWarning    Glyph = "⚠"
)

const (
	ansiReset  = "\x1b[0m"
	ansiRed    = "\x1b[31m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiCyan   = "\x1b[36m"
	ansiGrey   = "\x1b[90m"
	ansiBold   = "\x1b[1m"
)

var glyphColour = map[Glyph]string{
	GlyphReady:      ansiGreen,
	GlyphNotReady:   ansiRed,
	GlyphSuspended:  ansiYellow,
	GlyphInProgress: ansiCyan,
	GlyphUnknown:    ansiGrey,
	GlyphWarning:    ansiYellow,
}

// Style renders glyphs and emphasis, in colour or plain.
type Style struct {
	Color bool
}

// NewStyle enables colour only when w is a terminal, NO_COLOR is unset
// (https://no-color.org) and noColor is false.
func NewStyle(w io.Writer, noColor bool) Style {
	if noColor || os.Getenv("NO_COLOR") != "" {
		return Style{}
	}
	f, ok := w.(*os.File)
	return Style{Color: ok && term.IsTerminal(int(f.Fd()))}
}

// Glyph renders g, coloured by its meaning.
func (s Style) Glyph(g Glyph) string {
	return s.paint(glyphColour[g], string(g))
}

// Bold emphasises text.
func (s Style) Bold(text string) string {
	return s.paint(ansiBold, text)
}

// Faint de-emphasises text.
func (s Style) Faint(text string) string {
	return s.paint(ansiGrey, text)
}

func (s Style) paint(code, text string) string {
	if !s.Color || code == "" || text == "" {
		return text
	}
	return code + text + ansiReset
}

// ConditionGlyph marks an owner or dependency verdict. Unknown splits on the
// reason so that "still converging" reads differently from "cannot tell".
func ConditionGlyph(ready metav1.ConditionStatus, reason string) Glyph {
	switch ready {
	case metav1.ConditionTrue:
		return GlyphReady
	case metav1.ConditionFalse:
		if reason == apiv1.ReasonResourcesSuspended {
			return GlyphSuspended
		}
		return GlyphNotReady
	}
	switch reason {
	case apiv1.ReasonResourcesInProgress, apiv1.ReasonDependenciesInProgress:
		return GlyphInProgress
	}
	return GlyphUnknown
}

// MemberGlyph marks one resource by its kstatus. A suspended but otherwise
// Current resource is ⏸ whether or not its policy makes it block; the
// member line's reason says which.
func MemberGlyph(m membership.Member) Glyph {
	switch m.Status {
	case kstatus.CurrentStatus.String():
		if m.Suspended {
			return GlyphSuspended
		}
		return GlyphReady
	case kstatus.InProgressStatus.String(), kstatus.TerminatingStatus.String():
		return GlyphInProgress
	case kstatus.FailedStatus.String(), kstatus.NotFoundStatus.String():
		return GlyphNotReady
	}
	return GlyphUnknown
}
