/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package watcher_test

const (
	kindMilestone    = "Milestone"
	labelWave        = "wave"
	nsFluxSystem     = "flux-system"
	nameWave0        = "wave-0"
	groupHelmToolkit = "helm.toolkit.fluxcd.io"
	kindHelmRelease  = "HelmRelease"

	// milestoneFluxSystemWave0Key is the canonical owner-key string for
	// nsFluxSystem/nameWave0 produced by ownerKeys().
	milestoneFluxSystemWave0Key = "Milestone/flux-system/wave-0"
)
