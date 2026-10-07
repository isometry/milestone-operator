/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"k8s.io/apimachinery/pkg/util/duration"
)

// TableOptions shapes PrintTable output.
type TableOptions struct {
	// Namespace adds the NAMESPACE column.
	Namespace bool
	// Wide adds the per-bucket resource counts and LAST EVALUATED.
	Wide      bool
	NoHeaders bool
	Now       time.Time
}

// Headers returns the column headers for opts.
func (opts TableOptions) Headers() []string {
	var h []string
	if opts.Namespace {
		h = append(h, "NAMESPACE")
	}
	h = append(h, "NAME", "READY", "REASON", "CURRENT/TOTAL")
	if opts.Wide {
		h = append(h, "INPROGRESS", "FAILED", "NOTFOUND", "TERMINATING", "UNKNOWN", "SUSPENDED")
	}
	h = append(h, "DEPS", "STALLED", "AGE")
	if opts.Wide {
		h = append(h, "LAST EVALUATED")
	}
	return append(h, "MESSAGE")
}

// Cells renders r as one cell per header.
func (opts TableOptions) Cells(r Row) []string {
	var c []string
	if opts.Namespace {
		c = append(c, r.Namespace)
	}
	ready := string(r.Ready)
	if r.Stale {
		ready += " (stale)"
	}
	s := r.Summary
	c = append(c, r.Name, ready, r.Reason, fmt.Sprintf("%d/%d", s.Current, s.Total))
	if opts.Wide {
		c = append(c, itoa(s.InProgress), itoa(s.Failed), itoa(s.NotFound),
			itoa(s.Terminating), itoa(s.Unknown), itoa(s.Suspended))
	}
	c = append(c, fmt.Sprintf("%d/%d", r.DepsReady, r.DepsTotal), string(r.Stalled), Age(r.Created, opts.Now))
	if opts.Wide {
		last := "<never>"
		if !r.LastEvaluated.IsZero() {
			last = Age(r.LastEvaluated, opts.Now)
		}
		c = append(c, last)
	}
	return append(c, oneLine(r.Message))
}

// PrintTable writes rows as an aligned table.
func PrintTable(w io.Writer, rows []Row, opts TableOptions) error {
	var header []string
	if !opts.NoHeaders {
		header = opts.Headers()
	}
	cells := make([][]string, 0, len(rows))
	for _, r := range rows {
		cells = append(cells, opts.Cells(r))
	}
	return WriteColumns(w, header, cells)
}

// WriteColumns aligns cells under header (omitted when nil) and strips the
// trailing padding tabwriter leaves after an empty last column.
func WriteColumns(w io.Writer, header []string, rows [][]string) error {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 0, 8, 3, ' ', 0)
	if header != nil {
		_, _ = fmt.Fprintln(tw, strings.Join(header, "\t"))
	}
	for _, r := range rows {
		_, _ = fmt.Fprintln(tw, strings.Join(r, "\t"))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	var out strings.Builder
	for line := range strings.Lines(buf.String()) {
		out.WriteString(strings.TrimRight(line, " \n"))
		out.WriteByte('\n')
	}
	_, err := io.WriteString(w, out.String())
	return err
}

// Age renders the time since t the way kubectl's AGE column does.
func Age(t, now time.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	return duration.HumanDuration(max(now.Sub(t), 0))
}

func itoa(n int32) string { return fmt.Sprint(n) }

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
