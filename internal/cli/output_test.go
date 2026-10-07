/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package cli_test

import (
	"bytes"
	"testing"

	"github.com/isometry/milestone-operator/internal/cli"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestOutputFlag(t *testing.T) {
	o := cli.NewOutputFlag(cli.OutputTable, cli.OutputJSON)
	if o.String() != cli.OutputTable || o.Structured() {
		t.Fatalf("default = %q structured=%v", o.String(), o.Structured())
	}
	if err := o.Set(cli.OutputYAML); err == nil {
		t.Error("yaml should be rejected")
	}
	if err := o.Set(cli.OutputJSON); err != nil || !o.Structured() {
		t.Errorf("Set(json) = %v, structured=%v", err, o.Structured())
	}
	if o.Type() != "format" || len(o.Allowed()) != 2 {
		t.Errorf("Type=%q Allowed=%v", o.Type(), o.Allowed())
	}
}

func TestPrintStructured(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{"kind": kindMilestone, "metadata": map[string]any{"name": "a"}}}
	list := cli.NewList([]*unstructured.Unstructured{u})

	var js bytes.Buffer
	if err := cli.PrintStructured(&js, cli.OutputJSON, list); err != nil {
		t.Fatal(err)
	}
	wantJSON := `{
  "apiVersion": "v1",
  "items": [
    {
      "kind": "Milestone",
      "metadata": {
        "name": "a"
      }
    }
  ],
  "kind": "List"
}
`
	if js.String() != wantJSON {
		t.Errorf("json:\n%s", js.String())
	}

	var ys bytes.Buffer
	if err := cli.PrintStructured(&ys, cli.OutputYAML, list); err != nil {
		t.Fatal(err)
	}
	wantYAML := "apiVersion: v1\nitems:\n- kind: Milestone\n  metadata:\n    name: a\nkind: List\n"
	if ys.String() != wantYAML {
		t.Errorf("yaml:\n%s", ys.String())
	}

	if err := cli.PrintStructured(&ys, cli.OutputJSON, func() {}); err == nil {
		t.Error("expected marshal error")
	}
}
