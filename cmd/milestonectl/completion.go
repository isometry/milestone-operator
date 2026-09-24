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
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/isometry/milestone-operator/internal/cli"
)

// Completion is best-effort: any failure simply offers nothing.

const noFiles = cobra.ShellCompDirectiveNoFileComp

func registerOutputCompletion(cmd *cobra.Command, output *cli.OutputFlag) {
	_ = cmd.RegisterFlagCompletionFunc("output", cobra.FixedCompletions(output.Allowed(), noFiles))
}

func commandContext(cmd *cobra.Command) context.Context {
	if ctx := cmd.Context(); ctx != nil {
		return ctx
	}
	return context.Background()
}

func (a *app) completeOwnerNames(k cli.Kind) cobra.CompletionFunc {
	return func(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return nil, noFiles
		}
		return a.names(commandContext(cmd), k.GVR(), k.Namespaced, ""), noFiles
	}
}

// completeTraceArgs completes NAME once TYPE is known, as either TYPE/NAME
// or a second argument.
func (a *app) completeTraceArgs(cmd *cobra.Command, args []string,
	toComplete string) ([]string, cobra.ShellCompDirective) {
	var typ, prefix string
	switch {
	case len(args) == 0 && strings.Contains(toComplete, "/"):
		typ, _, _ = strings.Cut(toComplete, "/")
		prefix = typ + "/"
	case len(args) == 1 && !strings.Contains(args[0], "/"):
		typ = args[0]
	default:
		return nil, noFiles
	}
	mapper, err := a.clients.RESTMapper()
	if err != nil {
		return nil, noFiles
	}
	target, err := resolveType(mapper, typ)
	if err != nil {
		return nil, noFiles
	}
	return a.names(commandContext(cmd), target.gvr, target.namespaced, prefix), noFiles
}

func (a *app) completeNamespaces(cmd *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	list, err := a.namespaceLister()
	if err != nil {
		return nil, noFiles
	}
	names, _ := list(commandContext(cmd), labels.Everything())
	return names, noFiles
}

func (a *app) names(ctx context.Context, gvr schema.GroupVersionResource, namespaced bool, prefix string) []string {
	ri, _, err := a.scoped(gvr, namespaced)
	if err != nil {
		return nil
	}
	list, err := ri.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(list.Items))
	for _, u := range list.Items {
		out = append(out, prefix+u.GetName())
	}
	return out
}
