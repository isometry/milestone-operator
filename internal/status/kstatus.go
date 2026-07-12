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

// IsCurrent reports whether the resource's computed kstatus is Current
// (steady-state ready).
func (r Resource) IsCurrent() bool {
	return r.Status == kstatus.CurrentStatus.String()
}

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
		r.Status = kstatus.UnknownStatus.String()
		r.Message = err.Error()
		return r
	}
	if res == nil {
		r.Status = kstatus.UnknownStatus.String()
		return r
	}
	r.Status = res.Status.String()
	r.Message = res.Message
	// Pull a Reason from the kstatus-extracted resource conditions. kstatus
	// surfaces the resource's own Ready/Reconciling/Stalled conditions; the
	// first non-empty Reason is the most informative thing we can attribute
	// (e.g. LessReplicas, ProgressDeadlineExceeded, ReconciliationFailed).
	for _, c := range res.Conditions {
		if c.Reason != "" {
			r.Reason = c.Reason
			break
		}
	}
	return r
}
