/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller_test

// Test fixture constants. Exported strings under milestone.as-code.io live
// in api/v1; these are the third-party identifiers (Flux, RBAC, CRD
// machinery) and the fixture namespace/name/label values that the
// controller test fakes reuse.
const (
	nsFluxSystem      = "flux-system"
	kindMilestone     = "Milestone"
	kindKustomization = "Kustomization"
	groupKustomize    = "kustomize.toolkit.fluxcd.io"
	gvKustomizeV1     = groupKustomize + "/v1"
	groupRBAC         = "rbac.authorization.k8s.io"
	gvRBACv1          = groupRBAC + "/v1"
	kindLate          = "Late"
	groupMissing      = "missing.io"
	kindClusterRole   = "ClusterRole"
	kindWidget        = "Widget"
	groupTestAsCode   = "test.milestone.as-code.io"
	schemaTypeObject  = "object"
	schemaPropStatus  = "status"
	keyReason         = "reason"
	keyType           = "type"
	namePlatform      = "platform"
	labelTier         = "tier"

	// Test fixture identifiers (dependency names, status values, labels)
	// repeated across multiple test files. Consolidated to satisfy goconst.
	depKustomizations = "kustomizations"
	depLate           = "late"
	statusTrue        = "True"
	testReason        = "Test"
	labelWave         = "wave"
	depWaveA          = "wave-a"
	depWaveB          = "wave-b"
	depHelmreleases   = "helmreleases"
	widgetPlural      = "widgets"
	depRoles          = "roles"
)
