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
	"crypto/tls"
	"flag"
	"os"
	"path/filepath"
	"time"

	// Import all Kubernetes client auth plugins (Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/controller"
	resolverpkg "github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/fluxnotify"
	"github.com/isometry/milestone-operator/internal/metrics"
	"github.com/isometry/milestone-operator/internal/watcher"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(apiv1.AddToScheme(scheme))
	utilruntime.Must(apiextv1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

const (
	informerResync = 10 * time.Minute
	discoveryTTL   = 60 * time.Second

	controllerMilestone        = "Milestone"
	controllerClusterMilestone = "ClusterMilestone"
)

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", false,
		"If set, the metrics endpoint is served via HTTPS with a bearer-token AuthN/AuthZ filter. "+
			"Default is plain HTTP with no authentication; restrict access by other means (e.g. NetworkPolicy).")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics server")
	var enableFluxNotify bool
	flag.BoolVar(&enableFluxNotify, "flux-notify", true,
		"Poke FluxCD parent (Kustomization/HelmRelease) on Milestone Ready transitions.")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	var metricsCertWatcher *certwatcher.CertWatcher

	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}
	if secureMetrics {
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher", "path", metricsCertPath)
		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(metricsCertPath, metricsCertName),
			filepath.Join(metricsCertPath, metricsCertKey),
		)
		if err != nil {
			setupLog.Error(err, "init metrics cert watcher")
			os.Exit(1)
		}
		metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(c *tls.Config) {
			c.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	cfg := ctrl.GetConfigOrDie()

	// Manager signal context — cancelled on SIGINT/SIGTERM. Captured here so it
	// can be threaded into the metrics state collector before mgr.Start runs.
	ctx := ctrl.SetupSignalHandler()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "milestone-operator.as-code.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Discovery resolver (TTL cache). client-go's DiscoveryInterface is not yet
	// context-aware (k8s.io/client-go v0.33); resolverpkg.WrapClient adapts it
	// to the resolver's ctx-aware Discoverer interface.
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		setupLog.Error(err, "unable to construct discovery client")
		os.Exit(1)
	}
	resolver := resolverpkg.NewResolver(resolverpkg.WrapClient(discoveryClient), discoveryTTL)

	// Dynamic informer factory
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		setupLog.Error(err, "unable to construct dynamic client")
		os.Exit(1)
	}
	dynFactory := watcher.NewDynamicFactory(dynClient, mgr.GetRESTMapper(), informerResync)

	// Per-controller workqueue-backed sources. Each source captures the
	// controller's RateLimitingInterface at Start time; the registry and the
	// CRD watcher push reconcile.Requests directly without an intermediate
	// channel. The workqueue dedupes by key, so an event storm for the same
	// owner collapses to a single reconcile.
	milestoneSource := watcher.NewEnqueueSource()
	cmilestoneSource := watcher.NewEnqueueSource()
	enqueue := func(o watcher.OwnerKey) {
		switch o.Kind {
		case controllerMilestone:
			milestoneSource.Enqueue(reconcile.Request{NamespacedName: client.ObjectKey{
				Namespace: o.Namespace, Name: o.Name,
			}})
		case controllerClusterMilestone:
			cmilestoneSource.Enqueue(reconcile.Request{NamespacedName: client.ObjectKey{
				Name: o.Name,
			}})
		default:
			setupLog.Error(nil, "dropping enqueue for unknown owner kind",
				"kind", o.Kind, "namespace", o.Namespace, "name", o.Name)
		}
	}
	registry := watcher.NewRegistry(dynFactory, enqueue)
	if err := mgr.Add(registry); err != nil {
		setupLog.Error(err, "register watcher.Registry as manager.Runnable")
		os.Exit(1)
	}

	// FluxCD notify side-effect: one Notifier per controller so that emitted
	// metrics carry the correct controller label. Both share the manager's
	// client. Disabled when --flux-notify=false (field stays nil → no-op).
	var milestoneFluxNotifier, clusterMilestoneFluxNotifier controller.FluxNotifier
	if enableFluxNotify {
		milestoneFluxNotifier = &fluxnotify.Notifier{
			Client:     mgr.GetClient(),
			Controller: controllerMilestone,
			Now:        time.Now,
			Log:        ctrl.Log.WithName("flux-notify").WithValues("controller", controllerMilestone),
		}
		clusterMilestoneFluxNotifier = &fluxnotify.Notifier{
			Client:     mgr.GetClient(),
			Controller: controllerClusterMilestone,
			Now:        time.Now,
			Log:        ctrl.Log.WithName("flux-notify").WithValues("controller", controllerClusterMilestone),
		}
	}

	// Generic reconcilers (one per CRD).
	milestoneReconciler := &controller.Reconciler[*apiv1.Milestone]{
		Client:       mgr.GetClient(),
		Registry:     registry,
		Resolver:     resolver,
		NewAdapter:   controller.NewMilestoneAdapter,
		Controller:   controllerMilestone,
		FluxNotifier: milestoneFluxNotifier,
	}
	clusterMilestoneReconciler := &controller.Reconciler[*apiv1.ClusterMilestone]{
		Client:       mgr.GetClient(),
		Registry:     registry,
		Resolver:     resolver,
		NewAdapter:   controller.NewClusterMilestoneAdapterFactory(mgr.GetClient()),
		Controller:   controllerClusterMilestone,
		FluxNotifier: clusterMilestoneFluxNotifier,
	}

	if err := (&controller.MilestoneReconciler{
		Client:      mgr.GetClient(),
		Scheme:      mgr.GetScheme(),
		Reconciler:  milestoneReconciler,
		EventSource: milestoneSource,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", controllerMilestone)
		os.Exit(1)
	}
	if err := (&controller.ClusterMilestoneReconciler{
		Client:      mgr.GetClient(),
		Scheme:      mgr.GetScheme(),
		Reconciler:  clusterMilestoneReconciler,
		EventSource: cmilestoneSource,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", controllerClusterMilestone)
		os.Exit(1)
	}

	// CRD watcher: wakes stalled owners on Established=True transitions and
	// invalidates discovery + registry state on CRD removal / de-Establish.
	if err := (&controller.CRDWatcher{
		Client:                  mgr.GetClient(),
		Resolver:                resolver,
		Registry:                registry,
		MilestoneEnqueue:        milestoneSource,
		ClusterMilestoneEnqueue: cmilestoneSource,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "CRDWatcher")
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder

	// Register all package metrics + lister-backed state collector against the
	// controller-runtime metrics registry.
	if err := metrics.Register(ctrlmetrics.Registry); err != nil {
		setupLog.Error(err, "register metrics")
		os.Exit(1)
	}
	stateCollector := metrics.NewStateCollector(ctx, &managerStateLister{client: mgr.GetClient()})
	if err := ctrlmetrics.Registry.Register(stateCollector); err != nil {
		setupLog.Error(err, "register state collector")
		os.Exit(1)
	}

	if metricsCertWatcher != nil {
		if err := mgr.Add(metricsCertWatcher); err != nil {
			setupLog.Error(err, "add metrics cert watcher")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "set up healthz")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "set up readyz")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// managerStateLister implements metrics.StateLister using the manager's cache
// via client.Client. List errors yield empty slices so the scrape never fails,
// but the failure is logged and surfaced on the StateCollectorErrors counter
// so alerts that watch for the absence of Ready owners can distinguish an
// empty cluster from a broken read path.
type managerStateLister struct {
	client client.Client
}

func (l *managerStateLister) ListMilestones(ctx context.Context) []apiv1.Milestone {
	list := &apiv1.MilestoneList{}
	if err := l.client.List(ctx, list); err != nil {
		ctrl.Log.WithName("state-collector").Error(err, "list milestones for scrape")
		metrics.StateCollectorErrors.WithLabelValues(controllerMilestone).Inc()
		return nil
	}
	return list.Items
}

func (l *managerStateLister) ListClusterMilestones(ctx context.Context) []apiv1.ClusterMilestone {
	list := &apiv1.ClusterMilestoneList{}
	if err := l.client.List(ctx, list); err != nil {
		ctrl.Log.WithName("state-collector").Error(err, "list clustermilestones for scrape")
		metrics.StateCollectorErrors.WithLabelValues(controllerClusterMilestone).Inc()
		return nil
	}
	return list.Items
}
