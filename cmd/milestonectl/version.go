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
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/isometry/milestone-operator/internal/cli"
)

// operatorSelector matches the operator Deployment in both the kustomize
// and Helm manifests.
const operatorSelector = "app.kubernetes.io/name=milestone-operator"

type versionInfo struct {
	Client    buildInfo      `json:"client"`
	Operators []operatorInfo `json:"operators,omitempty"`
	// OperatorError explains why no operator is reported.
	OperatorError string `json:"operatorError,omitempty"`
}

type buildInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit,omitempty"`
	Date      string `json:"date,omitempty"`
	GoVersion string `json:"goVersion"`
	Platform  string `json:"platform"`
}

type operatorInfo struct {
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Images    []string `json:"images"`
}

func newVersionCommand(a *app) *cobra.Command {
	var clientOnly bool
	output := cli.NewOutputFlag(cli.OutputTable, cli.OutputJSON, cli.OutputYAML)
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the client and operator versions",
		Long:  cli.VersionLong,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			info := versionInfo{Client: clientVersion()}
			if !clientOnly {
				info.Operators, info.OperatorError = operatorVersions(cmd.Context(), a)
			}
			if output.Structured() {
				return cli.PrintStructured(a.out, output.String(), info)
			}
			return printVersion(a, info, clientOnly)
		},
	}
	cmd.Flags().BoolVar(&clientOnly, "client", false, "Print the client version only, without contacting the cluster")
	cmd.Flags().VarP(output, "output", "o", "Output format: table|json|yaml")
	registerOutputCompletion(cmd, output)
	return cmd
}

func printVersion(a *app, info versionInfo, clientOnly bool) error {
	c := info.Client
	line := "client: " + c.Version
	var extra []string
	if c.Commit != "" {
		extra = append(extra, "commit "+c.Commit)
	}
	if c.Date != "" {
		extra = append(extra, "built "+c.Date)
	}
	extra = append(extra, c.GoVersion, c.Platform)
	line += " (" + strings.Join(extra, ", ") + ")"
	if _, err := fmt.Fprintln(a.out, line); err != nil || clientOnly {
		return err
	}
	if info.OperatorError != "" {
		_, err := fmt.Fprintf(a.out, "operator: %s\n", info.OperatorError)
		return err
	}
	for _, op := range info.Operators {
		images := strings.Join(op.Images, ",")
		if _, err := fmt.Fprintf(a.out, "operator: %s/%s %s\n", op.Namespace, op.Name, images); err != nil {
			return err
		}
	}
	return nil
}

// clientVersion prefers the linker-injected values and falls back to what
// the Go toolchain recorded, so "go install" builds still identify
// themselves.
func clientVersion() buildInfo {
	b := buildInfo{
		Version:   version,
		Commit:    commit,
		Date:      date,
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		if b.Version == "" && bi.Main.Version != "(devel)" {
			b.Version = bi.Main.Version
		}
		for _, s := range bi.Settings {
			switch {
			case s.Key == "vcs.revision" && b.Commit == "":
				b.Commit = s.Value
			case s.Key == "vcs.time" && b.Date == "":
				b.Date = s.Value
			}
		}
	}
	if b.Version == "" {
		b.Version = "dev"
	}
	return b
}

// operatorVersions never fails the command: the operator may live where the
// caller cannot look, and the client version is still worth printing.
func operatorVersions(ctx context.Context, a *app) ([]operatorInfo, string) {
	kube, err := a.clients.Kube()
	if err != nil {
		return nil, "unavailable (" + err.Error() + ")"
	}
	list, err := kube.AppsV1().Deployments(metav1.NamespaceAll).List(ctx,
		metav1.ListOptions{LabelSelector: operatorSelector})
	switch {
	case apierrors.IsForbidden(err):
		return nil, "unavailable (forbidden: your credentials cannot list deployments across namespaces)"
	case err != nil:
		return nil, "unavailable (" + err.Error() + ")"
	case len(list.Items) == 0:
		return nil, "not found (no deployment labelled " + operatorSelector + ")"
	}
	ops := make([]operatorInfo, 0, len(list.Items))
	for _, d := range list.Items {
		op := operatorInfo{Namespace: d.Namespace, Name: d.Name}
		for _, c := range d.Spec.Template.Spec.Containers {
			op.Images = append(op.Images, c.Image)
		}
		ops = append(ops, op)
	}
	return ops, ""
}
