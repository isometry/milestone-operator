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
// a Resource ready for reduction. Errors and nil results are reported as Unknown.
func Compute(u *unstructured.Unstructured) Resource {
	gvk := u.GroupVersionKind()
	r := Resource{
		Group:     gvk.Group,
		Version:   gvk.Version,
		Kind:      gvk.Kind,
		Namespace: u.GetNamespace(),
		Name:      u.GetName(),
	}
	res, err := kstatus.Compute(u)
	if err != nil {
		r.Status = "Unknown"
		r.Message = err.Error()
		return r
	}
	if res == nil {
		r.Status = "Unknown"
		return r
	}
	r.Status = res.Status.String()
	r.Message = res.Message
	return r
}
