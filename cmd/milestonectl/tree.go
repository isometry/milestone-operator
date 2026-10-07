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

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/isometry/milestone-operator/internal/cli"
)

type treeOptions struct {
	notReady bool
	output   *cli.OutputFlag
}

func newTreeCommand(a *app) *cobra.Command {
	o := &treeOptions{output: cli.NewOutputFlag(cli.OutputTree, cli.OutputJSON, cli.OutputYAML)}
	cmd := &cobra.Command{
		Use:     cmdTree,
		Short:   "Evaluate a Milestone or ClusterMilestone live and show its members",
		Long:    cli.TreeLong,
		Example: cli.Examples(cli.TreeExample, a.name),
		Args:    cobra.NoArgs,
		RunE:    helpForKind,
	}
	pf := cmd.PersistentFlags()
	pf.BoolVar(&o.notReady, "not-ready", false, "Show only the members holding their dependency back")
	pf.VarP(o.output, "output", "o", "Output format: tree|json|yaml")
	registerOutputCompletion(cmd, o.output)

	for _, k := range cli.Kinds() {
		cmd.AddCommand(&cobra.Command{
			Use:               k.Singular + " NAME",
			Aliases:           []string{k.Plural, k.ShortName},
			Short:             "Evaluate a " + k.Kind + " live and show its members",
			Args:              cobra.ExactArgs(1),
			ValidArgsFunction: a.completeOwnerNames(k),
			RunE: func(cmd *cobra.Command, args []string) error {
				return o.run(cmd.Context(), a, k, args[0])
			},
		})
	}
	return cmd
}

// run exits zero whatever the verdict: tree reports, it does not gate.
func (o *treeOptions) run(ctx context.Context, a *app, k cli.Kind, name string) error {
	ri, ns, err := a.scoped(k.GVR(), k.Namespaced)
	if err != nil {
		return err
	}
	u, err := ri.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return notFoundError(k, ns, name)
	}
	if err != nil {
		return err
	}
	owner, err := k.Decode(u)
	if err != nil {
		return err
	}
	eval, err := a.evaluator()
	if err != nil {
		return err
	}
	report, err := owner.Evaluate(ctx, eval)
	if err != nil {
		return err
	}
	if o.output.Structured() {
		return cli.PrintStructured(a.out, o.output.String(), report)
	}
	return cli.RenderTree(a.out, report, cli.TreeOptions{NotReady: o.notReady, Style: a.style()})
}
