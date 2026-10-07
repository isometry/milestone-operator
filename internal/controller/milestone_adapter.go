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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MilestoneAdapter implements OwnerAdapter for the namespaced Milestone CRD.
type MilestoneAdapter struct {
	Milestone *apiv1.Milestone
}

// NewMilestoneAdapter constructs an adapter for m.
func NewMilestoneAdapter(m *apiv1.Milestone) OwnerAdapter {
	return &MilestoneAdapter{Milestone: m}
}

// OwnerKey returns the watcher.OwnerKey for this Milestone.
func (a *MilestoneAdapter) OwnerKey() watcher.OwnerKey {
	return watcher.OwnerKey{
		Kind:      "Milestone",
		Namespace: a.Milestone.Namespace,
		Name:      a.Milestone.Name,
	}
}

// Dependencies normalises spec.dependsOn via membership.NormalizeMilestone.
func (a *MilestoneAdapter) Dependencies(ctx context.Context, dr discovery.Resolver) ([]NormalizedDependency, []DependencyError) {
	return membership.NormalizeMilestone(ctx, dr, a.Milestone)
}

// Status returns the embedded MilestoneStatusBase.
func (a *MilestoneAdapter) Status() *apiv1.MilestoneStatusBase {
	return &a.Milestone.Status.MilestoneStatusBase
}

// PatchStatus persists the in-memory status using the status subresource.
// A full-status merge patch rather than an Update: the object was read from
// a cache that lags our own writes, so an optimistic lock would 409 on
// every transition without protecting anything — nothing else writes this
// status.
func (a *MilestoneAdapter) PatchStatus(ctx context.Context, c client.Client) error {
	patch, err := statusMergePatch(&a.Milestone.Status.MilestoneStatusBase)
	if err != nil {
		return err
	}
	return c.Status().Patch(ctx, a.Milestone, patch)
}
