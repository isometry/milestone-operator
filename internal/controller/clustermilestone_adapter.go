/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	"github.com/isometry/milestone-operator/internal/membership"
	"github.com/isometry/milestone-operator/internal/watcher"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterMilestoneAdapter implements OwnerAdapter for the cluster-scoped CRD.
// Each instance binds a client.Client so namespaceSelector dependencies can
// list Namespaces during normalisation.
type ClusterMilestoneAdapter struct {
	ClusterMilestone *apiv1.ClusterMilestone
	Client           client.Client
}

// NewClusterMilestoneAdapterFactory returns a NewAdapter function bound to c.
func NewClusterMilestoneAdapterFactory(c client.Client) func(*apiv1.ClusterMilestone) OwnerAdapter {
	return func(cm *apiv1.ClusterMilestone) OwnerAdapter {
		return &ClusterMilestoneAdapter{
			ClusterMilestone: cm,
			Client:           c,
		}
	}
}

// OwnerKey returns the watcher.OwnerKey for this ClusterMilestone.
func (a *ClusterMilestoneAdapter) OwnerKey() watcher.OwnerKey {
	return watcher.OwnerKey{Kind: "ClusterMilestone", Name: a.ClusterMilestone.Name}
}

// Dependencies normalises spec.dependsOn via
// membership.NormalizeClusterMilestone, listing Namespaces through a.Client.
func (a *ClusterMilestoneAdapter) Dependencies(ctx context.Context, dr discovery.Resolver) ([]NormalizedDependency, []DependencyError) {
	return membership.NormalizeClusterMilestone(ctx, dr, a.listNamespaces, a.ClusterMilestone)
}

func (a *ClusterMilestoneAdapter) listNamespaces(ctx context.Context, sel labels.Selector) ([]string, error) {
	nsList := &corev1.NamespaceList{}
	if err := a.Client.List(ctx, nsList, &client.ListOptions{LabelSelector: sel}); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(nsList.Items))
	for _, ns := range nsList.Items {
		names = append(names, ns.Name)
	}
	return names, nil
}

// Status returns the embedded MilestoneStatusBase.
func (a *ClusterMilestoneAdapter) Status() *apiv1.MilestoneStatusBase {
	return &a.ClusterMilestone.Status.MilestoneStatusBase
}

// PatchStatus persists the in-memory status using the status subresource.
// See MilestoneAdapter.PatchStatus for why this is a full-status merge
// patch rather than an Update.
func (a *ClusterMilestoneAdapter) PatchStatus(ctx context.Context, c client.Client) error {
	patch, err := statusMergePatch(&a.ClusterMilestone.Status.MilestoneStatusBase)
	if err != nil {
		return err
	}
	return c.Status().Patch(ctx, a.ClusterMilestone, patch)
}
