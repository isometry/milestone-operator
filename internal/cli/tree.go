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
	"io"
	"strings"
	"unicode/utf8"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/membership"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TreeOptions shapes RenderTree output.
type TreeOptions struct {
	// NotReady prunes members that do not hold their dependency back.
	NotReady bool
	Style    Style
}

const (
	branchMid  = "├── "
	branchLast = "└── "
	indentMid  = "│   "
	indentLast = "    "
	colGap     = "  "
	// memberGap is wider than colGap so the status column stands apart from
	// long namespace/name pairs.
	memberGap = "   "
)

// RenderTree writes r as an owner line followed by one branch per
// dependency and one leaf per member.
func RenderTree(w io.Writer, r membership.Report, opts TreeOptions) error {
	var b strings.Builder
	b.WriteString(ownerLine(r, opts.Style))
	b.WriteByte('\n')

	width := memberWidth(r, opts.NotReady)
	for i, d := range r.Dependencies {
		branch, indent := branchMid, indentMid
		if i == len(r.Dependencies)-1 {
			branch, indent = branchLast, indentLast
		}
		b.WriteString(branch + dependencyLine(d, opts.Style) + "\n")
		children := dependencyChildren(d, opts, width)
		for j, c := range children {
			if j == len(children)-1 {
				b.WriteString(indent + branchLast + c + "\n")
			} else {
				b.WriteString(indent + branchMid + c + "\n")
			}
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func ownerLine(r membership.Report, s Style) string {
	ref := r.Owner.Kind + "/" + r.Owner.Name
	if r.Owner.Namespace != "" {
		ref = r.Owner.Kind + "/" + r.Owner.Namespace + "/" + r.Owner.Name
	}
	when := "never evaluated by the operator"
	if !r.Recorded.LastEvaluatedTime.IsZero() {
		when = "evaluated " + Age(r.Recorded.LastEvaluatedTime.Time, r.EvaluatedAt.Time) + " ago"
	}
	if r.Recorded.Stale {
		when += fmt.Sprintf(", stale: generation %d not yet observed", r.Owner.Generation)
	}
	return strings.Join([]string{
		s.Bold(ref), "Ready=" + string(r.Ready), r.Reason, s.Faint("(" + when + ")"),
	}, colGap)
}

func dependencyLine(d membership.DependencyReport, s Style) string {
	parts := []string{
		s.Glyph(ConditionGlyph(d.Status.Ready, d.Status.Reason)) + " " + d.Name,
		kindRef(d),
		orAll(d.Target.Selector),
	}
	switch {
	case len(d.Target.Namespaces) > 0:
		parts = append(parts, "namespaces="+strings.Join(d.Target.Namespaces, ","))
	case d.Target.NamespaceSelector != "":
		parts = append(parts, "namespaceSelector="+d.Target.NamespaceSelector)
	}
	switch {
	case d.Forbidden:
		parts = append(parts, "cannot check: forbidden (your credentials)")
	case d.Error != "":
		parts = append(parts, d.Status.Reason+": "+d.Error)
	default:
		parts = append(parts, fmt.Sprintf("%d/%d", d.Status.Summary.Current, d.Status.Summary.Total), d.Status.Reason)
	}
	return strings.Join(parts, colGap)
}

// kindRef prefers the resolved version, which is the one actually read.
func kindRef(d membership.DependencyReport) string {
	ref := d.Target.Kind
	if d.Target.Group != "" {
		ref += "." + d.Target.Group
	}
	version := d.Status.Version
	if version == "" {
		version = d.Target.Version
	}
	if version != "" {
		ref += "/" + version
	}
	return ref
}

func orAll(selector string) string {
	if selector == "" {
		return "<all>"
	}
	return selector
}

func dependencyChildren(d membership.DependencyReport, opts TreeOptions, width int) []string {
	var out []string
	if d.Drift && d.Recorded != nil {
		out = append(out, fmt.Sprintf("%s live differs from recorded (recorded: Ready=%s %s)",
			opts.Style.Glyph(GlyphWarning), d.Recorded.Ready, d.Recorded.Reason))
	}
	if d.Error == "" && d.Status.Summary.Total == 0 {
		out = append(out, opts.Style.Faint("no matching resources (emptySetPolicy: "+emptySetOutcome(d.Status.Ready)+")"))
	}
	for _, m := range d.Members {
		if opts.NotReady && !m.Blocking {
			continue
		}
		out = append(out, memberLine(m, opts.Style, width))
	}
	return out
}

// emptySetOutcome recovers the policy from the verdict it produced, since
// ReduceDependency maps each emptySetPolicy to a distinct Ready value.
func emptySetOutcome(ready metav1.ConditionStatus) string {
	switch ready {
	case metav1.ConditionTrue:
		return string(apiv1.EmptySetReady)
	case metav1.ConditionFalse:
		return string(apiv1.EmptySetNotReady)
	}
	return string(apiv1.EmptySetUnknown)
}

func memberLine(m membership.Member, s Style, width int) string {
	name := memberName(m)
	pad := strings.Repeat(" ", width-utf8.RuneCountInString(name))
	line := s.Glyph(MemberGlyph(m)) + " " + name + pad + memberGap + memberState(m)
	if detail := memberDetail(m); detail != "" {
		line += colGap + detail
	}
	return line
}

// memberDetail explains only blocking members. A Current one is blocked by
// suspension alone, mirroring status.notReadyResources.
func memberDetail(m membership.Member) string {
	if !m.Blocking {
		return ""
	}
	if m.IsCurrent() {
		return apiv1.ReasonSuspended + ": spec.suspend is true"
	}
	return resourceDetail(m.Resource)
}

func memberState(m membership.Member) string {
	if m.Suspended {
		return m.Status + " (suspended)"
	}
	return m.Status
}

func memberName(m membership.Member) string {
	if m.Namespace == "" {
		return m.Name
	}
	return m.Namespace + "/" + m.Name
}

// memberWidth aligns the status column across the whole tree, counting only
// the members that will be shown.
func memberWidth(r membership.Report, notReady bool) int {
	w := 0
	for _, d := range r.Dependencies {
		for _, m := range d.Members {
			if notReady && !m.Blocking {
				continue
			}
			w = max(w, utf8.RuneCountInString(memberName(m)))
		}
	}
	return w
}
