/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"

	"github.com/isometry/milestone-operator/internal/cli"
)

// watchBackoff spaces out reopening a watch that closed without delivering
// anything, so a server that keeps closing it cannot cause a hot loop.
const watchBackoff = time.Second

// sleepCtx is swapped out by tests.
var sleepCtx = func(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

type getOptions struct {
	allNamespaces  bool
	statusSelector []string
	watch          bool
	output         *cli.OutputFlag
}

func newGetCommand(a *app) *cobra.Command {
	o := &getOptions{output: cli.NewOutputFlag(cli.OutputTable, cli.OutputWide, cli.OutputJSON, cli.OutputYAML)}
	cmd := &cobra.Command{
		Use:     cmdGet,
		Short:   "Show the recorded status of Milestones and ClusterMilestones",
		Long:    cli.GetLong,
		Example: cli.Examples(cli.GetExample, a.name),
		Args:    cobra.NoArgs,
		RunE:    helpForKind,
	}
	pf := cmd.PersistentFlags()
	pf.BoolVarP(&o.allNamespaces, "all-namespaces", "A", false, "List across all namespaces")
	pf.StringArrayVar(&o.statusSelector, "status-selector", nil,
		"Filter by recorded status: ready=True|False|Unknown or stalled=True|False|Unknown (repeatable, ANDed)")
	pf.BoolVarP(&o.watch, "watch", "w", false, "After listing, print each object again whenever its status changes")
	pf.VarP(o.output, "output", "o", "Output format: table|wide|json|yaml")
	registerOutputCompletion(cmd, o.output)
	_ = cmd.RegisterFlagCompletionFunc("status-selector", cobra.FixedCompletions([]string{
		"ready=True", "ready=False", "ready=Unknown", "stalled=True", "stalled=False",
	}, cobra.ShellCompDirectiveNoFileComp))

	for _, k := range cli.Kinds() {
		cmd.AddCommand(&cobra.Command{
			Use:               k.Plural + " [NAME]",
			Aliases:           []string{k.Singular, k.ShortName},
			Short:             "Show the recorded status of " + k.Kind + "s",
			Args:              cobra.MaximumNArgs(1),
			ValidArgsFunction: a.completeOwnerNames(k),
			RunE: func(cmd *cobra.Command, args []string) error {
				var name string
				if len(args) == 1 {
					name = args[0]
				}
				return o.runKind(cmd.Context(), a, k, name)
			},
		})
	}
	cmd.AddCommand(&cobra.Command{
		Use:   cmdAll,
		Short: "Show the recorded status of every Milestone and ClusterMilestone",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.runAll(cmd.Context(), a)
		},
	})
	return cmd
}

// listing is one kind's filtered objects.
type listing struct {
	kind   cli.Kind
	owners []cli.Owner
	// namespace is "" when listing across namespaces or cluster-scoped.
	namespace       string
	resourceVersion string
}

func (o *getOptions) runKind(ctx context.Context, a *app, k cli.Kind, name string) error {
	sel, err := cli.ParseStatusSelector(o.statusSelector)
	if err != nil {
		return err
	}
	if name != "" && o.allNamespaces && k.Namespaced {
		return errors.New("a resource cannot be retrieved by name across all namespaces")
	}
	ri, ns, err := o.resource(a, k)
	if err != nil {
		return err
	}
	l, err := fetch(ctx, ri, k, ns, name, sel)
	if err != nil {
		return err
	}

	switch {
	case o.output.Structured() && name != "" && len(l.owners) == 1:
		err = cli.PrintStructured(a.out, o.output.String(), l.owners[0].Object)
	case o.output.Structured():
		err = cli.PrintStructured(a.out, o.output.String(), cli.NewList(objects(l.owners)))
	case len(l.owners) == 0 && !o.watch:
		_, err = fmt.Fprintln(a.errOut, notFoundMessage(k, ns))
	default:
		err = cli.PrintTable(a.out, rows(l.owners), o.tableOptions(a, k, false))
	}
	if err != nil || !o.watch {
		return err
	}
	return o.watchKind(ctx, a, ri, l, name, sel)
}

func (o *getOptions) runAll(ctx context.Context, a *app) error {
	if o.watch {
		return errors.New("--watch is not supported with 'get all'; watch one kind at a time")
	}
	sel, err := cli.ParseStatusSelector(o.statusSelector)
	if err != nil {
		return err
	}
	var all []*unstructured.Unstructured
	printed := 0
	for _, k := range cli.Kinds() {
		ri, ns, err := o.resource(a, k)
		if err != nil {
			return err
		}
		l, err := fetch(ctx, ri, k, ns, "", sel)
		if err != nil {
			return err
		}
		if o.output.Structured() {
			all = append(all, objects(l.owners)...)
			continue
		}
		if len(l.owners) == 0 {
			continue
		}
		if printed > 0 {
			_, _ = fmt.Fprintln(a.out)
		}
		printed++
		rs := rows(l.owners)
		for i := range rs {
			rs[i].Name = k.Singular + "/" + rs[i].Name
		}
		if err := cli.PrintTable(a.out, rs, o.tableOptions(a, k, false)); err != nil {
			return err
		}
	}
	if o.output.Structured() {
		return cli.PrintStructured(a.out, o.output.String(), cli.NewList(all))
	}
	if printed == 0 {
		_, err = fmt.Fprintln(a.errOut, "no Milestone or ClusterMilestone objects found")
	}
	return err
}

// resource scopes k's client to the requested namespace. The returned
// namespace is "" for cluster-scoped kinds and with --all-namespaces.
func (o *getOptions) resource(a *app, k cli.Kind) (dynamic.ResourceInterface, string, error) {
	return a.scoped(k.GVR(), k.Namespaced && !o.allNamespaces)
}

func (o *getOptions) tableOptions(a *app, k cli.Kind, noHeaders bool) cli.TableOptions {
	return cli.TableOptions{
		Namespace: k.Namespaced && o.allNamespaces,
		Wide:      o.output.String() == cli.OutputWide,
		NoHeaders: noHeaders,
		Now:       a.now(),
	}
}

// fetch gets one object by name, or lists them all, keeping those sel
// matches.
func fetch(ctx context.Context, ri dynamic.ResourceInterface, k cli.Kind, ns, name string,
	sel cli.StatusSelector) (listing, error) {
	l := listing{kind: k, namespace: ns}
	var items []unstructured.Unstructured
	if name != "" {
		u, err := ri.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return l, notFoundError(k, ns, name)
		}
		if err != nil {
			return l, err
		}
		items, l.resourceVersion = []unstructured.Unstructured{*u}, u.GetResourceVersion()
	} else {
		list, err := ri.List(ctx, metav1.ListOptions{})
		if err != nil {
			return l, fmt.Errorf("list %s: %w", k.Plural, err)
		}
		items, l.resourceVersion = list.Items, list.GetResourceVersion()
	}
	for i := range items {
		owner, err := k.Decode(&items[i])
		if err != nil {
			return l, err
		}
		if sel.Matches(owner.Row()) {
			l.owners = append(l.owners, owner)
		}
	}
	return l, nil
}

// watchKind follows changes after the initial listing and prints an object
// only when what would be printed for it has changed. It returns nil when
// ctx is cancelled (Ctrl-C).
func (o *getOptions) watchKind(ctx context.Context, a *app, ri dynamic.ResourceInterface, l listing, name string,
	sel cli.StatusSelector) error {
	seen := make(map[string]any, len(l.owners))
	for _, owner := range l.owners {
		seen[owner.Object.GetNamespace()+"/"+owner.Object.GetName()] = o.watchKey(owner)
	}
	opts := metav1.ListOptions{ResourceVersion: l.resourceVersion}
	if name != "" {
		opts.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
	}
	for {
		w, err := ri.Watch(ctx, opts)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("watch %s: %w", l.kind.Plural, err)
		}
		var delivered bool
		opts.ResourceVersion, delivered, err = o.drain(ctx, a, w, l.kind, sel, seen, opts.ResourceVersion)
		w.Stop()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		if !delivered {
			sleepCtx(ctx, watchBackoff)
			if ctx.Err() != nil {
				return nil
			}
		}
	}
}

// drain consumes w until it closes, returning the resourceVersion to resume
// from and whether w delivered any event at all. An expired resourceVersion
// resumes from "now", whose initial ADDED events the seen map turns into
// no-ops.
func (o *getOptions) drain(ctx context.Context, a *app, w watch.Interface, k cli.Kind, sel cli.StatusSelector,
	seen map[string]any, rv string) (next string, delivered bool, err error) {
	for {
		var ev watch.Event
		var ok bool
		select {
		case <-ctx.Done():
			return rv, delivered, nil
		case ev, ok = <-w.ResultChan():
		}
		if !ok {
			return rv, delivered, nil
		}
		delivered = true
		if ev.Type == watch.Error {
			err := apierrors.FromObject(ev.Object)
			if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
				return "", delivered, nil
			}
			return rv, delivered, fmt.Errorf("watch %s: %w", k.Plural, err)
		}
		u, isUnstructured := ev.Object.(*unstructured.Unstructured)
		if !isUnstructured {
			continue
		}
		rv = u.GetResourceVersion()
		key := u.GetNamespace() + "/" + u.GetName()
		if ev.Type == watch.Deleted {
			delete(seen, key)
			continue
		}
		owner, err := k.Decode(u)
		if err != nil {
			return rv, delivered, err
		}
		wk := o.watchKey(owner)
		if !sel.Matches(owner.Row()) || seen[key] == wk {
			continue
		}
		seen[key] = wk
		if err := o.printWatched(a, owner); err != nil {
			return rv, delivered, err
		}
	}
}

// watchKey captures what the chosen output shows, so a reconcile that only
// bumps lastEvaluatedTime does not reprint an unchanged table row.
func (o *getOptions) watchKey(owner cli.Owner) any {
	if o.output.Structured() {
		return owner.Object.GetResourceVersion()
	}
	r := owner.Row()
	if o.output.String() != cli.OutputWide {
		r.LastEvaluated = time.Time{}
	}
	return r
}

func (o *getOptions) printWatched(a *app, owner cli.Owner) error {
	switch o.output.String() {
	case cli.OutputYAML:
		if _, err := io.WriteString(a.out, "---\n"); err != nil {
			return err
		}
		fallthrough
	case cli.OutputJSON:
		return cli.PrintStructured(a.out, o.output.String(), owner.Object)
	}
	return cli.PrintTable(a.out, []cli.Row{owner.Row()}, o.tableOptions(a, owner.Kind, true))
}

func rows(owners []cli.Owner) []cli.Row {
	out := make([]cli.Row, 0, len(owners))
	for _, owner := range owners {
		out = append(out, owner.Row())
	}
	return out
}

func objects(owners []cli.Owner) []*unstructured.Unstructured {
	out := make([]*unstructured.Unstructured, 0, len(owners))
	for _, owner := range owners {
		out = append(out, owner.Object)
	}
	return out
}

func notFoundMessage(k cli.Kind, ns string) string {
	if ns == "" {
		return fmt.Sprintf("no %s objects found", k.Kind)
	}
	return fmt.Sprintf("no %s objects found in %q namespace", k.Kind, ns)
}

func notFoundError(k cli.Kind, ns, name string) error {
	if ns == "" {
		return fmt.Errorf("%s %q not found", k.Kind, name)
	}
	return fmt.Errorf("%s %q not found in %q namespace", k.Kind, name, ns)
}
