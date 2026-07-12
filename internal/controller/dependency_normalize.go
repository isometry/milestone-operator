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
	"errors"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/discovery"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Shared dependency-normalisation plumbing for both adapters. Error
// classification lives here — exactly once — so Milestone and
// ClusterMilestone can never drift apart in how they report the same failure.

// resolveDependencyTarget resolves one dependency's target identity and
// classifies failures into the closed DependencyStatus.Reason enum: a
// discovery outage is DiscoveryUnavailable, an answered "no such group/kind"
// is GVKNotEstablished.
func resolveDependencyTarget(ctx context.Context, dr discovery.Resolver, name string, t apiv1.TargetSpec) (schema.GroupVersionKind, apimeta.RESTScopeName, *DependencyError) {
	gvk, scope, err := dr.Resolve(ctx, t.Group, t.Kind, t.Version)
	if err != nil {
		reason := apiv1.ReasonGVKNotEstablished
		if errors.Is(err, discovery.ErrDiscoveryUnavailable) {
			reason = apiv1.ReasonDiscoveryUnavailable
		}
		return schema.GroupVersionKind{}, "", &DependencyError{
			Name:    name,
			Group:   t.Group,
			Version: t.Version, // resolution failed: best we have is the requested version
			Kind:    t.Kind,
			Reason:  reason,
			Err:     err,
		}
	}
	return gvk, scope, nil
}

// dependencyError builds a DependencyError for a dependency whose GVK has
// already been resolved, carrying the resolved version so failedRollup does
// not write an empty version into status.dependsOn.
func dependencyError(name string, gvk schema.GroupVersionKind, reason string, err error) DependencyError {
	return DependencyError{
		Name:    name,
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind,
		Reason:  reason,
		Err:     err,
	}
}

// parseDependencySelector normalises the optional label selector; a nil
// selector matches everything.
func parseDependencySelector(name string, gvk schema.GroupVersionKind, ls *metav1.LabelSelector) (labels.Selector, *DependencyError) {
	sel, err := labelSelectorOrEverything(ls)
	if err != nil {
		e := dependencyError(name, gvk, apiv1.ReasonDiscoveryFailed, err)
		return nil, &e
	}
	return sel, nil
}
