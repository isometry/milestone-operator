/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	apiv1 "github.com/isometry/milestone-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// MaxStalledErrChars exposes the truncation constant so tests can assert
// against the contract rather than hardcoding the number.
const MaxStalledErrChars = maxStalledErrChars
