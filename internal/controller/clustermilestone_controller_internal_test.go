/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestNamespaceMembershipPredicate(t *testing.T) {
	p := namespaceMembershipPredicate{}

	nsA := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"tier": "prod"}}}
	nsAChanged := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"tier": "dev"}}}

	if !p.Create(event.CreateEvent{Object: nsA}) {
		t.Errorf("Create must wake the controller — new namespaces can match a namespaceSelector")
	}
	if p.Update(event.UpdateEvent{ObjectOld: nsA, ObjectNew: nsA}) {
		t.Errorf("Update with unchanged labels must not wake the controller (heartbeat noise)")
	}
	if !p.Update(event.UpdateEvent{ObjectOld: nsA, ObjectNew: nsAChanged}) {
		t.Errorf("Update with changed labels must wake the controller")
	}
	if p.Update(event.UpdateEvent{ObjectOld: nil, ObjectNew: nsA}) {
		t.Errorf("Update with nil ObjectOld must not panic and must return false")
	}
	if p.Update(event.UpdateEvent{ObjectOld: nsA, ObjectNew: nil}) {
		t.Errorf("Update with nil ObjectNew must not panic and must return false")
	}
	if !p.Delete(event.DeleteEvent{Object: nsA}) {
		t.Errorf("Delete must wake the controller — previously-matching namespaces must drop")
	}
	if p.Generic(event.GenericEvent{Object: nsA}) {
		t.Errorf("Generic events have no membership semantics; must not wake the controller")
	}
}
