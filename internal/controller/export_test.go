/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"reflect"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Narrow white-box exports for unit tests in controller_test that need to
// exercise package-internal helpers carrying load-bearing invariants.

// SanitiseErrText exposes sanitiseErrText for test access.
func SanitiseErrText(err error) string { return sanitiseErrText(err) }

// StripTransitionTimes exposes stripTransitionTimes for test access.
func StripTransitionTimes(in []metav1.Condition) []metav1.Condition {
	return stripTransitionTimes(in)
}

// StatusEqualIgnoringTimestamp exposes statusEqualIgnoringTimestamp for test access.
func StatusEqualIgnoringTimestamp(a, b apiv1.MilestoneStatusBase) bool {
	return statusEqualIgnoringTimestamp(a, b)
}

// DedupeAndSortResources exposes dedupeAndSortResources for test access.
func DedupeAndSortResources(in []apiv1.ResourceStatus) []apiv1.ResourceStatus {
	return dedupeAndSortResources(in)
}

// NotReadyResourcesOf exposes notReadyResourcesOf for test access.
func NotReadyResourcesOf(resources []status.Resource, policy apiv1.SuspendPolicy) []apiv1.ResourceStatus {
	return notReadyResourcesOf(resources, policy)
}

// TruncateWithEllipsis exposes truncateWithEllipsis for test access.
func TruncateWithEllipsis(s string, maxBytes int) string {
	return truncateWithEllipsis(s, maxBytes)
}

// MaxStalledErrChars exposes the truncation constant so tests can assert
// against the contract rather than hardcoding the number.
const MaxStalledErrChars = maxStalledErrChars

// StatusMergePatch exposes statusMergePatch for test access.
func StatusMergePatch(sb *apiv1.MilestoneStatusBase) (client.Patch, error) {
	return statusMergePatch(sb)
}

// StalledRequeue exposes the stalled requeue interval so tests assert
// against the contract rather than a hardcoded duration.
const StalledRequeue = stalledRequeue

// JSONKeysOf exposes jsonKeysOf for test access, so the inline-embed
// flattening can be exercised against fixture types the real status
// doesn't (yet) contain.
func JSONKeysOf(t reflect.Type) []string { return jsonKeysOf(t) }
