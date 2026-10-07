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
	"strings"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/isometry/milestone-operator/internal/cli"
	"github.com/isometry/milestone-operator/internal/membership"
	"github.com/isometry/milestone-operator/internal/status"
)

type traceOptions struct {
	output *cli.OutputFlag
}

func newTraceCommand(a *app) *cobra.Command {
	o := &traceOptions{output: cli.NewOutputFlag(cli.OutputTable, cli.OutputJSON, cli.OutputYAML)}
	cmd := &cobra.Command{
		Use:               cmdTrace + " (TYPE/NAME | TYPE NAME)",
		Short:             "Show which Milestones and ClusterMilestones include an object",
		Long:              cli.TraceLong,
		Example:           cli.Examples(cli.TraceExample, a.name),
		Args:              cobra.RangeArgs(1, 2),
		ValidArgsFunction: a.completeTraceArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			typ, name, err := parseTraceArgs(args)
			if err != nil {
				return err
			}
			return o.run(cmd.Context(), a, typ, name)
		},
	}
	cmd.Flags().VarP(o.output, "output", "o", "Output format: table|json|yaml")
	registerOutputCompletion(cmd, o.output)
	return cmd
}

func parseTraceArgs(args []string) (string, string, error) {
	if len(args) == 2 {
		return args[0], args[1], nil
	}
	typ, name, ok := strings.Cut(args[0], "/")
	if !ok || typ == "" || name == "" {
		return "", "", fmt.Errorf("expected TYPE/NAME or TYPE NAME, got %q", args[0])
	}
	return typ, name, nil
}

// traceTarget is the traced object's resolved type.
type traceTarget struct {
	gvr        schema.GroupVersionResource
	gvk        schema.GroupVersionKind
	namespaced bool
}

// resolveType accepts anything kubectl does: plural, singular, short name,
// or a group-qualified resource such as deployments.apps.
func resolveType(mapper apimeta.RESTMapper, typ string) (traceTarget, error) {
	fully, gr := schema.ParseResourceArg(strings.ToLower(typ))
	var gvr schema.GroupVersionResource
	var err error
	if fully != nil {
		gvr, err = mapper.ResourceFor(*fully)
	}
	if fully == nil || err != nil {
		gvr, err = mapper.ResourceFor(gr.WithVersion(""))
	}
	if err != nil {
		return traceTarget{}, fmt.Errorf("resource type %q: %w", typ, err)
	}
	gvk, err := mapper.KindFor(gvr)
	if err != nil {
		return traceTarget{}, err
	}
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return traceTarget{}, err
	}
	return traceTarget{gvr: gvr, gvk: gvk, namespaced: mapping.Scope.Name() == apimeta.RESTScopeNameNamespace}, nil
}

func (o *traceOptions) run(ctx context.Context, a *app, typ, name string) error {
	mapper, err := a.clients.RESTMapper()
	if err != nil {
		return err
	}
	target, err := resolveType(mapper, typ)
	if err != nil {
		return err
	}
	ri, ns, err := a.scoped(target.gvr, target.namespaced)
	if err != nil {
		return err
	}
	u, err := ri.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return notFoundError(cli.Kind{Kind: target.gvk.Kind}, ns, name)
	}
	if err != nil {
		return err
	}
	if u.GetKind() == "" {
		u.SetGroupVersionKind(target.gvk)
	}

	owners, unchecked, err := o.candidateOwners(ctx, a, target, ns)
	if err != nil {
		return err
	}
	report, err := o.trace(ctx, a, owners, status.Compute(u), labels.Set(u.GetLabels()))
	if err != nil {
		return err
	}
	report.Unchecked = unchecked
	if o.output.Structured() {
		return cli.PrintStructured(a.out, o.output.String(), report)
	}
	return cli.PrintTrace(a.out, report, a.style())
}

// candidateOwners lists the Milestones in the object's namespace and every
// ClusterMilestone. A Milestone can never admit a cluster-scoped object.
//
// A namespaced user routinely cannot list ClusterMilestones, and a cluster may
// have only one CRD kind installed, so a kind that is forbidden or absent is
// skipped with a warning and returned in unchecked. Any other error, or every
// kind being skipped, fails the trace: nothing would have been checked.
func (o *traceOptions) candidateOwners(ctx context.Context, a *app, target traceTarget,
	ns string) (owners []cli.Owner, unchecked []string, err error) {
	dyn, err := a.clients.Dynamic()
	if err != nil {
		return nil, nil, err
	}
	var skipErrs []error
	attempted := 0
	for _, k := range cli.Kinds() {
		var ri dynamic.ResourceInterface = dyn.Resource(k.GVR())
		if k.Namespaced {
			if !target.namespaced {
				continue
			}
			ri = dyn.Resource(k.GVR()).Namespace(ns)
		}
		attempted++
		l, err := fetch(ctx, ri, k, ns, "", cli.StatusSelector{})
		if why := skippable(err); why != "" {
			_, _ = fmt.Fprintf(a.errOut, "%s cannot list %ss (%s): membership via %ss not checked\n",
				cli.NewStyle(a.errOut, a.noColor).Glyph(cli.GlyphWarning), k.Kind, why, k.Kind)
			skipErrs = append(skipErrs, err)
			unchecked = append(unchecked, k.Kind)
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		owners = append(owners, l.owners...)
	}
	if attempted > 0 && len(skipErrs) == attempted {
		return nil, nil, errors.Join(skipErrs...)
	}
	return owners, unchecked, nil
}

const (
	whyForbidden = "forbidden"
	whyNotFound  = "not found"
)

// skippable reports why a failed owner listing may be tolerated, or "" if err
// must abort the trace.
func skippable(err error) string {
	switch {
	case err == nil:
		return ""
	case apierrors.IsForbidden(err):
		return whyForbidden
	case apierrors.IsNotFound(err), apimeta.IsNoMatchError(err):
		return whyNotFound
	}
	return ""
}

func (o *traceOptions) trace(ctx context.Context, a *app, owners []cli.Owner, obj status.Resource,
	objLabels labels.Set) (cli.TraceReport, error) {
	resolver, err := a.resolver()
	if err != nil {
		return cli.TraceReport{}, err
	}
	nsLister, err := a.namespaceLister()
	if err != nil {
		return cli.TraceReport{}, err
	}
	gk := schema.GroupKind{Group: obj.Group, Kind: obj.Kind}
	report := cli.TraceReport{Object: obj, Memberships: []cli.Membership{}}
	for _, owner := range owners {
		deps, errs := owner.Normalize(ctx, resolver, nsLister)
		o.warnUnevaluated(a, owner, gk, errs)
		row := owner.Row()
		for _, d := range membership.Matches(deps, gk, obj.Namespace, objLabels) {
			report.Memberships = append(report.Memberships, cli.Membership{
				Owner: membership.OwnerRef{
					Kind:       owner.Kind.Kind,
					Namespace:  row.Namespace,
					Name:       row.Name,
					Generation: owner.Meta().Generation,
				},
				Dependency:  d.Name,
				OwnerReady:  row.Ready,
				OwnerReason: row.Reason,
				OwnerStale:  row.Stale,
				Blocking:    obj.Blocks(d.SuspendPolicy),
			})
		}
	}
	return report, nil
}

// warnUnevaluated flags dependencies that target the traced kind but could
// not be normalised, since they might have admitted the object.
func (o *traceOptions) warnUnevaluated(a *app, owner cli.Owner, gk schema.GroupKind, errs []membership.Error) {
	for _, e := range errs {
		if e.Name != "" && (e.Group != gk.Group || e.Kind != gk.Kind) {
			continue
		}
		var what string
		if e.Name != "" {
			what = fmt.Sprintf(" dependency %q", e.Name)
		}
		_, _ = fmt.Fprintf(a.errOut, "%s %s%s could not be evaluated (%s): %v\n",
			cli.NewStyle(a.errOut, a.noColor).Glyph(cli.GlyphWarning), owner.Ref(), what, e.Reason, e)
	}
}
