/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli

import (
	"time"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Row is the recorded status of one owner, flattened for tabular output.
type Row struct {
	Namespace string
	Name      string

	Ready   metav1.ConditionStatus
	Reason  string
	Message string
	Stalled metav1.ConditionStatus
	// Stale means the operator has not yet reconciled the current generation.
	Stale bool

	Summary   apiv1.Summary
	DepsReady int
	DepsTotal int

	Created       time.Time
	LastEvaluated time.Time
}

// Row flattens the owner's recorded status. An absent condition reads as
// Unknown: nothing has been observed yet.
func (o Owner) Row() Row {
	meta, st := o.Meta(), o.Status()
	r := Row{
		Namespace:     meta.Namespace,
		Name:          meta.Name,
		Ready:         metav1.ConditionUnknown,
		Stalled:       metav1.ConditionUnknown,
		Stale:         st.ObservedGeneration < meta.Generation,
		Summary:       st.Summary,
		DepsTotal:     len(st.DependsOn),
		Created:       meta.CreationTimestamp.Time,
		LastEvaluated: st.LastEvaluatedTime.Time,
	}
	if c := apimeta.FindStatusCondition(st.Conditions, apiv1.ConditionReady); c != nil {
		r.Ready, r.Reason, r.Message = c.Status, c.Reason, c.Message
	}
	if c := apimeta.FindStatusCondition(st.Conditions, apiv1.ConditionStalled); c != nil {
		r.Stalled = c.Status
	}
	for _, d := range st.DependsOn {
		if d.Ready == metav1.ConditionTrue {
			r.DepsReady++
		}
	}
	return r
}
