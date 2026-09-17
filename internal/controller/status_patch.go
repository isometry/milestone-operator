/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	apiv1 "github.com/isometry/milestone-operator/api/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// statusJSONKeys is every json key MilestoneStatusBase can emit, derived by
// reflection so a new status field is covered without touching this file.
// Computed eagerly at package init: the derivation panics on a status field
// it cannot derive a wire key for, and that drift should stop the process
// at start-up (and fail the test binary at load) rather than inside a
// reconcile.
var statusJSONKeys = jsonKeysOf(reflect.TypeFor[apiv1.MilestoneStatusBase]())

// jsonKeysOf returns the json object keys t marshals to. An inline
// anonymous embed contributes its own keys to the enclosing object rather
// than a key of its own, so it is flattened: skipping it would leave every
// omitempty field it carries un-nulled, and a value cleared in one
// reconcile would survive in etcd forever.
//
// An exported non-anonymous field with no json name marshals under its Go
// field name, which is a shape this patch has no business guessing at.
// That is a drift bug in the API types, so it panics rather than silently
// dropping the key from the null fill.
func jsonKeysOf(t reflect.Type) []string {
	keys := make([]string, 0, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		// encoding/json promotes the exported fields of an embedded struct
		// even when the embedded type itself is unexported, so visibility
		// is only decisive for non-embedded fields.
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
				keys = append(keys, jsonKeysOf(ft)...)
				continue
			}
			panic(fmt.Sprintf("controller: %s.%s has no json name; status patch cannot derive its wire key", t, f.Name))
		}
		keys = append(keys, name)
	}
	return keys
}

// statusMergePatch returns a JSON merge patch carrying the complete status.
// Keys that omitempty dropped are sent as explicit null so the patch has
// replacement semantics (RFC 7386) without an optimistic lock: the owner
// status is a pure function of observed member state, the informer cache
// lags our own writes, and a competing write always re-enqueues us.
//
// A diffing patch (client.MergeFrom) cannot be used here: the CRD marks
// every status.summary counter required with a CEL identity between total
// and the buckets, so a patch carrying only the changed counters is
// rejected with 422.
func statusMergePatch(sb *apiv1.MilestoneStatusBase) (client.Patch, error) {
	raw, err := json.Marshal(sb)
	if err != nil {
		return nil, fmt.Errorf("marshal status: %w", err)
	}
	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("decode status: %w", err)
	}
	for _, k := range statusJSONKeys {
		if _, ok := fields[k]; !ok {
			fields[k] = json.RawMessage("null")
		}
	}
	body, err := json.Marshal(map[string]any{"status": fields})
	if err != nil {
		return nil, fmt.Errorf("marshal status patch: %w", err)
	}
	return client.RawPatch(types.MergePatchType, body), nil
}
