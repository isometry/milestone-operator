/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"github.com/isometry/milestone-operator/internal/controller"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// statusBaseJSONKeys mirrors the reflection the helper does, independently,
// so the test fails if the helper stops covering a field rather than
// agreeing with it by construction. Inline anonymous embeds are flattened:
// their keys land in the enclosing object, so the helper must null-fill
// them too.
func statusBaseJSONKeys(t *testing.T) []string {
	t.Helper()
	return jsonKeysOfForTest(t, reflect.TypeFor[apiv1.MilestoneStatusBase]())
}

func jsonKeysOfForTest(t *testing.T, typ reflect.Type) []string {
	t.Helper()
	out := make([]string, 0, typ.NumField())
	for i := range typ.NumField() {
		f := typ.Field(i)
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if f.Anonymous {
			if !f.IsExported() && ft.Kind() != reflect.Struct {
				continue
			}
		} else if !f.IsExported() {
			continue
		}
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if name == "" {
			if f.Anonymous && ft.Kind() == reflect.Struct {
				out = append(out, jsonKeysOfForTest(t, ft)...)
				continue
			}
			t.Fatalf("%s.%s has no json name", typ, f.Name)
		}
		out = append(out, name)
	}
	return out
}

// TestStatusMergePatch_CarriesEveryStatusKey pins the replacement semantics
// the patch depends on: a key omitempty dropped must go on the wire as an
// explicit null, or a field cleared in this reconcile would survive in etcd
// forever. It also pins what the patch must NOT carry — any metadata key
// (notably resourceVersion) would reintroduce the optimistic lock this
// change exists to remove.
func TestStatusMergePatch_CarriesEveryStatusKey(t *testing.T) {
	sb := &apiv1.MilestoneStatusBase{
		ObservedGeneration: 3,
		Conditions: []metav1.Condition{{
			Type: apiv1.ConditionReady, Status: metav1.ConditionTrue, Reason: testReason,
		}},
		// NotReadyResources nil and Truncated false: the omitempty cases.
	}

	patch, err := controller.StatusMergePatch(sb)
	if err != nil {
		t.Fatalf("StatusMergePatch: %v", err)
	}
	data, err := patch.Data(newMilestone("e1"))
	if err != nil {
		t.Fatalf("patch.Data: %v", err)
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("decode patch body: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("patch body keys = %v, want exactly [status]", keysOf(body))
	}
	if _, ok := body[schemaPropStatus]; !ok {
		t.Fatalf("patch body keys = %v, want [status]", keysOf(body))
	}
	for _, forbidden := range []string{"metadata", "spec", "resourceVersion", "apiVersion", "kind"} {
		if _, ok := body[forbidden]; ok {
			t.Errorf("patch body carries %q; it must scope to status only", forbidden)
		}
	}

	var st map[string]json.RawMessage
	if err := json.Unmarshal(body[schemaPropStatus], &st); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	for _, k := range statusBaseJSONKeys(t) {
		if _, ok := st[k]; !ok {
			t.Errorf("status key %q missing from patch; omitempty fields must be sent as null", k)
		}
	}
	for _, k := range []string{"notReadyResources", "truncated"} {
		if got := string(st[k]); got != "null" {
			t.Errorf("status[%q] = %s, want null", k, got)
		}
	}

	var summary map[string]json.RawMessage
	if err := json.Unmarshal(st["summary"], &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	// All eight counters are `required` in the CRD with a CEL identity
	// between total and the buckets, so a partial summary is a 422.
	for _, k := range []string{"total", "current", "inProgress", "failed", "notFound", "terminating", "unknown", "suspended"} {
		if _, ok := summary[k]; !ok {
			t.Errorf("summary key %q missing; the CRD marks every counter required", k)
		}
	}
	if len(summary) != 8 {
		t.Errorf("summary keys = %v, want 8", keysOf(summary))
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// embeddedStatus stands in for a future status field group factored into an
// inline embed — the shape MilestoneStatus and ClusterMilestoneStatus
// already use to carry MilestoneStatusBase.
type embeddedStatus struct {
	Nested string `json:"nested,omitempty"`
	Kept   int32  `json:"kept"`
}

type inlineFixture struct {
	embeddedStatus `json:",inline"`
	Top            string `json:"top,omitempty"`
	Hidden         string `json:"-"`
	unexported     string //nolint:unused // presence is the point: reflection must skip it
}

type untaggedFixture struct {
	Oops string
}

// TestJSONKeysOf_FlattensInlineEmbeds guards the null-fill blind spot: an
// inline anonymous embed contributes its keys to the enclosing object, so
// its omitempty fields must appear in the key set. Skipping the embed would
// leave `nested` un-nulled and a cleared value would survive in etcd.
func TestJSONKeysOf_FlattensInlineEmbeds(t *testing.T) {
	got := controller.JSONKeysOf(reflect.TypeFor[inlineFixture]())
	want := []string{"nested", "kept", "top"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("JSONKeysOf = %v, want %v", got, want)
	}

	// Cross-check against encoding/json itself rather than against our
	// reading of its promotion rules: every key a fully-populated value
	// marshals to must be in the set, or the patch would fail to null it.
	raw, err := json.Marshal(inlineFixture{
		embeddedStatus: embeddedStatus{Nested: "n", Kept: 1},
		Top:            "t",
		Hidden:         "h",
	})
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	var marshalled map[string]json.RawMessage
	if err := json.Unmarshal(raw, &marshalled); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	for k := range marshalled {
		if !slices.Contains(got, k) {
			t.Errorf("encoding/json emits %q but JSONKeysOf does not cover it", k)
		}
	}
}

// TestJSONKeysOf_PanicsOnUntaggedExportedField: an exported field with no
// json tag marshals under its Go name. Guessing that shape would silently
// drop the key from the null fill, so drift in the API types must be loud.
func TestJSONKeysOf_PanicsOnUntaggedExportedField(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("expected a panic for an exported field with no json name")
		}
	}()
	_ = controller.JSONKeysOf(reflect.TypeFor[untaggedFixture]())
}
