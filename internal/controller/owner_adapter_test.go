/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller_test

import (
	"testing"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/controller"
)

// TestAdapter_OwnerKey_MatchesControllerName pins the implicit contract that
// each adapter's OwnerKey().Kind equals the controller-label literal the
// wire-site in cmd/main.go uses for metrics and logging. A rename on one side
// without the other would silently break Prometheus queries that join on the
// `controller` label; this test makes the drift a compile/test failure.
func TestAdapter_OwnerKey_MatchesControllerName(t *testing.T) {
	t.Run("Milestone", func(t *testing.T) {
		got := controller.NewMilestoneAdapter(&apiv1.Milestone{}).OwnerKey().Kind
		// Literal mirrors cmd/main.go's controllerMilestone constant.
		if want := "Milestone"; got != want {
			t.Fatalf("MilestoneAdapter.OwnerKey().Kind = %q, want %q (must match cmd/main.go's controllerMilestone)", got, want)
		}
	})
	t.Run("ClusterMilestone", func(t *testing.T) {
		got := controller.NewClusterMilestoneAdapterFactory(nil)(&apiv1.ClusterMilestone{}).OwnerKey().Kind
		// Literal mirrors cmd/main.go's controllerClusterMilestone constant.
		if want := "ClusterMilestone"; got != want {
			t.Fatalf("ClusterMilestoneAdapter.OwnerKey().Kind = %q, want %q (must match cmd/main.go's controllerClusterMilestone)", got, want)
		}
	})
}
