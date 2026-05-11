/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller_test

// Test fixture constants. Exported strings under as-code.io live in api/v1;
// these are the third-party identifiers (Flux, RBAC, CRD machinery) and the
// fixture namespace/name/label values that the controller test fakes reuse.
const (
	nsFluxSystem      = "flux-system"
	kindEchelon       = "Echelon"
	kindKustomization = "Kustomization"
	groupKustomize    = "kustomize.toolkit.fluxcd.io"
	gvKustomizeV1     = groupKustomize + "/v1"
	groupRBAC         = "rbac.authorization.k8s.io"
	gvRBACv1          = groupRBAC + "/v1"
	kindLate          = "Late"
	groupMissing      = "missing.io"
	kindClusterRole   = "ClusterRole"
	kindWidget        = "Widget"
	groupTestAsCode   = "test.as-code.io"
	schemaTypeObject  = "object"
	schemaPropStatus  = "status"
	keyReason         = "reason"
	keyType           = "type"
	namePlatform      = "platform"
	labelTier         = "tier"

	// Test fixture identifiers (member keys, status values, labels) repeated
	// across multiple test files. Consolidated to satisfy goconst.
	memberKustomizations = "kustomizations"
	memberLate           = "late"
	statusTrue           = "True"
	testReason           = "Test"
	labelWave            = "wave"
	memberWaveA          = "wave-a"
	memberWaveB          = "wave-b"
	memberHelmreleases   = "helmreleases"
	widgetPlural         = "widgets"
	memberRoles          = "roles"
)
