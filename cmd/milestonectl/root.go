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
	"io"
	"time"

	"github.com/spf13/cobra"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	k8sdiscovery "k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/isometry/milestone-operator/internal/cli"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/membership"
)

// resolverTTL only needs to outlive one command; it exists because the
// resolver is shared with the operator, which wants a cache.
const resolverTTL = time.Minute

// clientFactory hands commands their Kubernetes clients, so tests can swap
// in fakes.
type clientFactory interface {
	Dynamic() (dynamic.Interface, error)
	// Discovery must not be cached: a stale cache would report a new CRD as
	// missing, or a removed one as established.
	Discovery() (k8sdiscovery.DiscoveryInterface, error)
	// RESTMapper expands short names (deploy, ks, hr) and plurals.
	RESTMapper() (apimeta.RESTMapper, error)
	Kube() (kubernetes.Interface, error)
	// DefaultNamespace is the kubeconfig context's namespace.
	DefaultNamespace() (string, error)
}

// app is the state shared by every command.
type app struct {
	// name is how the user invoked us: "milestonectl" or "kubectl milestone".
	name    string
	flags   *genericclioptions.ConfigFlags
	clients clientFactory
	out     io.Writer
	errOut  io.Writer
	now     func() time.Time
	noColor bool
}

func newApp(out, errOut io.Writer) *app {
	flags := genericclioptions.NewConfigFlags(true)
	return &app{
		name:    defaultName,
		flags:   flags,
		clients: &configClients{flags: flags},
		out:     out,
		errOut:  errOut,
		now:     time.Now,
	}
}

func (a *app) style() cli.Style { return cli.NewStyle(a.out, a.noColor) }

// namespace is --namespace if given, else the kubeconfig context's.
func (a *app) namespace() (string, error) {
	if ns := *a.flags.Namespace; ns != "" {
		return ns, nil
	}
	return a.clients.DefaultNamespace()
}

// scoped returns a client for gvr in the target namespace when namespaced,
// or across all namespaces (with namespace "") when not.
func (a *app) scoped(gvr schema.GroupVersionResource, namespaced bool) (dynamic.ResourceInterface, string, error) {
	dyn, err := a.clients.Dynamic()
	if err != nil {
		return nil, "", err
	}
	if !namespaced {
		return dyn.Resource(gvr), "", nil
	}
	ns, err := a.namespace()
	if err != nil {
		return nil, "", err
	}
	return dyn.Resource(gvr).Namespace(ns), ns, nil
}

func (a *app) namespaceLister() (membership.NamespaceLister, error) {
	kube, err := a.clients.Kube()
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, sel labels.Selector) ([]string, error) {
		list, err := kube.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: sel.String()})
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(list.Items))
		for _, ns := range list.Items {
			names = append(names, ns.Name)
		}
		return names, nil
	}, nil
}

func (a *app) resolver() (discovery.Resolver, error) {
	dc, err := a.clients.Discovery()
	if err != nil {
		return nil, err
	}
	return discovery.NewResolver(discovery.WrapClient(dc), resolverTTL), nil
}

func (a *app) evaluator() (*membership.Evaluator, error) {
	resolver, err := a.resolver()
	if err != nil {
		return nil, err
	}
	mapper, err := a.clients.RESTMapper()
	if err != nil {
		return nil, err
	}
	dyn, err := a.clients.Dynamic()
	if err != nil {
		return nil, err
	}
	nsLister, err := a.namespaceLister()
	if err != nil {
		return nil, err
	}
	return &membership.Evaluator{Resolver: resolver, Mapper: mapper, Dynamic: dyn, Namespaces: nsLister, Now: a.now}, nil
}

func newRootCommand(a *app, use string) *cobra.Command {
	root := &cobra.Command{
		Use:           use,
		Short:         "Inspect milestone-operator Milestones and ClusterMilestones",
		Long:          cli.RootLong,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	if a.name != use {
		root.Annotations = map[string]string{cobra.CommandDisplayNameAnnotation: a.name}
	}
	root.SetOut(a.out)
	root.SetErr(a.errOut)

	a.flags.AddFlags(root.PersistentFlags())
	root.PersistentFlags().BoolVar(&a.noColor, "no-color", false, "Disable colour output (also honours NO_COLOR)")
	_ = root.RegisterFlagCompletionFunc("namespace", a.completeNamespaces)

	root.AddCommand(
		newGetCommand(a),
		newTreeCommand(a),
		newTraceCommand(a),
		newVersionCommand(a),
	)
	return root
}

// helpForKind backs verbs whose kind is a subcommand, so that an unknown
// kind is an error rather than a silent help page.
func helpForKind(cmd *cobra.Command, _ []string) error {
	return cmd.Help()
}

// configClients builds clients from the kubeconfig flags on first use.
type configClients struct {
	flags  *genericclioptions.ConfigFlags
	config *rest.Config
}

func (c *configClients) restConfig() (*rest.Config, error) {
	if c.config != nil {
		return c.config, nil
	}
	cfg, err := c.flags.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	c.config = rest.AddUserAgent(cfg, "milestonectl/"+clientVersion().Version)
	return c.config, nil
}

func (c *configClients) Dynamic() (dynamic.Interface, error) {
	cfg, err := c.restConfig()
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(cfg)
}

func (c *configClients) Discovery() (k8sdiscovery.DiscoveryInterface, error) {
	cfg, err := c.restConfig()
	if err != nil {
		return nil, err
	}
	return k8sdiscovery.NewDiscoveryClientForConfig(cfg)
}

func (c *configClients) RESTMapper() (apimeta.RESTMapper, error) {
	return c.flags.ToRESTMapper()
}

func (c *configClients) Kube() (kubernetes.Interface, error) {
	cfg, err := c.restConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

func (c *configClients) DefaultNamespace() (string, error) {
	ns, _, err := c.flags.ToRawKubeConfigLoader().Namespace()
	return ns, err
}
