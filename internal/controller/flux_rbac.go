/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

// RBAC for the FluxCD notify side-effect. Both controllers patch the same
// parent kinds (Kustomization / HelmRelease), so the markers live in one
// file to keep them in sync.
//
// patch is the minimum verb needed for a blind merge-patch via RawPatch.
// The Flux CRDs may not be installed on the deploying cluster; that's
// surfaced at runtime as apimeta.NoMatchError and classified as the
// `no_match` result on milestone_flux_notify_total.
//
// +kubebuilder:rbac:groups=kustomize.toolkit.fluxcd.io,resources=kustomizations,verbs=patch
// +kubebuilder:rbac:groups=helm.toolkit.fluxcd.io,resources=helmreleases,verbs=patch
