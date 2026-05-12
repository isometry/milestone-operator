/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package metrics_test

const (
	kindMilestone     = "Milestone"
	kindKustomization = "Kustomization"
	groupKustomize    = "kustomize.toolkit.fluxcd.io"
	nsFluxSystem      = "flux-system"
	nameWave0         = "wave-0"

	// Label keys mirror the production metrics package (which keeps them
	// unexported); naming matches so that a divergence is obvious.
	keyOwnerKind   = "owner_kind"
	keyNamespace   = "namespace"
	keyName        = "name"
	keyType        = "type"
	keyStatus      = "status"
	keyTargetGroup = "target_group"
	keyTargetKind  = "target_kind"
	keyDependency  = "dependency"
)
