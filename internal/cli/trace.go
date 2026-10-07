/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli

import (
	"fmt"
	"io"

	"github.com/isometry/milestone-operator/internal/membership"
	"github.com/isometry/milestone-operator/internal/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TraceReport lists every dependency that admits one object.
type TraceReport struct {
	Object      status.Resource `json:"object"`
	Memberships []Membership    `json:"memberships"`
	// Unchecked names the owner kinds that could not be listed, so a miss is
	// not proof of non-membership.
	Unchecked []string `json:"uncheckedKinds,omitempty"`
}

// Membership is one (owner, dependency) pair admitting the traced object.
type Membership struct {
	Owner      membership.OwnerRef `json:"owner"`
	Dependency string              `json:"dependency"`
	// OwnerReady and OwnerReason are as last recorded by the operator.
	OwnerReady  metav1.ConditionStatus `json:"ownerReady"`
	OwnerReason string                 `json:"ownerReason,omitempty"`
	OwnerStale  bool                   `json:"ownerStale,omitempty"`
	// Blocking means the object holds this dependency back under its
	// suspendPolicy.
	Blocking bool `json:"blocking"`
}

// PrintTrace writes the object's live state followed by its memberships.
func PrintTrace(w io.Writer, r TraceReport, s Style) error {
	obj := membership.Member{Resource: r.Object}
	head := fmt.Sprintf("%s %s  %s", s.Glyph(MemberGlyph(obj)), s.Bold(ResourceRef(r.Object)), memberState(obj))
	// kstatus explains Current objects with boilerplate ("Resource is
	// always ready"), so only an unhealthy object gets its reason.
	if detail := resourceDetail(r.Object); detail != "" && !r.Object.IsCurrent() {
		head += colGap + detail
	}
	if len(r.Memberships) == 0 {
		miss := "not a member of any milestone"
		if len(r.Unchecked) > 0 {
			miss += " (some owner kinds could not be checked)"
		}
		_, err := fmt.Fprintf(w, "%s\n%s\n", head, miss)
		return err
	}
	if _, err := fmt.Fprintf(w, "%s\n\n", head); err != nil {
		return err
	}
	rows := make([][]string, 0, len(r.Memberships))
	for _, m := range r.Memberships {
		ready := string(m.OwnerReady)
		if m.OwnerStale {
			ready += " (stale)"
		}
		ref := m.Owner.Kind + "/" + m.Owner.Name
		if m.Owner.Namespace != "" {
			ref = m.Owner.Kind + "/" + m.Owner.Namespace + "/" + m.Owner.Name
		}
		blocking := "no"
		if m.Blocking {
			blocking = "yes"
		}
		rows = append(rows, []string{ref, m.Dependency, ready, m.OwnerReason, blocking})
	}
	return WriteColumns(w, []string{"OWNER", "DEPENDENCY", "OWNER READY", "OWNER REASON", "BLOCKING"}, rows)
}

// ResourceRef names a resource as Kind.group/namespace/name.
func ResourceRef(r status.Resource) string {
	ref := r.Kind
	if r.Group != "" {
		ref += "." + r.Group
	}
	if r.Namespace != "" {
		ref += "/" + r.Namespace
	}
	return ref + "/" + r.Name
}

func resourceDetail(r status.Resource) string {
	switch {
	case r.Reason != "" && r.Message != "":
		return r.Reason + ": " + oneLine(r.Message)
	case r.Reason != "":
		return r.Reason
	}
	return oneLine(r.Message)
}
