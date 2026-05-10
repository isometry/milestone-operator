/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package status

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kstatus "sigs.k8s.io/cli-utils/pkg/kstatus/status"
)

// Compute computes the kstatus of an unstructured resource and lifts it into
// a Member ready for reduction. Errors and nil results are reported as Unknown.
func Compute(u *unstructured.Unstructured) Member {
	gvk := u.GroupVersionKind()
	m := Member{
		Group:     gvk.Group,
		Version:   gvk.Version,
		Kind:      gvk.Kind,
		Namespace: u.GetNamespace(),
		Name:      u.GetName(),
	}
	res, err := kstatus.Compute(u)
	if err != nil {
		m.Status = "Unknown"
		m.Message = err.Error()
		return m
	}
	if res == nil {
		m.Status = "Unknown"
		return m
	}
	m.Status = res.Status.String()
	m.Message = res.Message
	return m
}
