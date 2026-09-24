/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Command milestonectl inspects Milestones and ClusterMilestones. Installed
// as kubectl-milestone it doubles as the "kubectl milestone" plugin.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/isometry/milestone-operator/internal/cli"
)

// Set with -ldflags "-X main.version=... -X main.commit=... -X main.date=...".
var (
	version string
	commit  string
	date    string
)

const (
	defaultName = "milestonectl"
	pluginUse   = "kubectl-milestone"
	pluginName  = "kubectl milestone"
	// kubectl (>= 1.26) completes plugin arguments by running
	// kubectl_complete-<plugin> with the words typed so far.
	pluginCompleter = "kubectl_complete-milestone"
)

const (
	cmdGet   = "get"
	cmdTree  = "tree"
	cmdTrace = "trace"
	cmdAll   = "all"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args, newApp(os.Stdout, os.Stderr))
	stop()
	os.Exit(code)
}

// run executes the command line argv (argv[0] included) and returns the
// process exit code.
func run(ctx context.Context, argv []string, a *app) int {
	use, args := invocation(argv)
	if use == pluginUse {
		a.name = pluginName
	}
	root := newRootCommand(a, use)
	root.SetArgs(args)
	if err := root.ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintf(a.errOut, "%s %v\n", cli.NewStyle(a.errOut, a.noColor).Glyph(cli.GlyphNotReady), err)
		return 1
	}
	return 0
}

// invocation maps the executable name to the root command's Use and the
// arguments cobra should see.
func invocation(argv []string) (string, []string) {
	var args []string
	if len(argv) > 1 {
		args = argv[1:]
	}
	if len(argv) == 0 {
		return defaultName, args
	}
	switch strings.TrimSuffix(filepath.Base(argv[0]), ".exe") {
	case pluginUse:
		return pluginUse, args
	case pluginCompleter:
		return pluginUse, append([]string{cobra.ShellCompRequestCmd}, args...)
	}
	return defaultName, args
}
