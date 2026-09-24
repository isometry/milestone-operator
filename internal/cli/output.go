/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

// Output format names accepted by -o.
const (
	OutputTable = "table"
	OutputWide  = "wide"
	OutputTree  = "tree"
	OutputJSON  = "json"
	OutputYAML  = "yaml"
)

// OutputFlag is a -o value restricted to a command's formats. It satisfies
// pflag.Value.
type OutputFlag struct {
	value   string
	allowed []string
}

// NewOutputFlag returns a flag defaulting to the first allowed format.
func NewOutputFlag(allowed ...string) *OutputFlag {
	return &OutputFlag{value: allowed[0], allowed: allowed}
}

func (o *OutputFlag) String() string { return o.value }
func (o *OutputFlag) Type() string   { return "format" }

// Set accepts only the allowed formats.
func (o *OutputFlag) Set(s string) error {
	if !slices.Contains(o.allowed, s) {
		return fmt.Errorf("must be one of: %s", strings.Join(o.allowed, "|"))
	}
	o.value = s
	return nil
}

// Allowed lists the accepted formats, for shell completion and help.
func (o *OutputFlag) Allowed() []string { return slices.Clone(o.allowed) }

// Structured reports whether the format is json or yaml.
func (o *OutputFlag) Structured() bool {
	return o.value == OutputJSON || o.value == OutputYAML
}

// PrintStructured writes v as indented JSON or as YAML.
func PrintStructured(w io.Writer, format string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if format == OutputYAML {
		if data, err = yaml.JSONToYAML(data); err != nil {
			return err
		}
	} else {
		data = append(data, '\n')
	}
	_, err = w.Write(data)
	return err
}

// NewList wraps objects in a kubectl-style v1 List.
func NewList(items []*unstructured.Unstructured) map[string]any {
	objs := make([]any, 0, len(items))
	for _, u := range items {
		objs = append(objs, u.Object)
	}
	return map[string]any{"apiVersion": "v1", "kind": "List", "items": objs}
}
