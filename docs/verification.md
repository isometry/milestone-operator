# Verifying release artifacts

Every tagged release (`v*.*.*`) of `milestone-operator` publishes two
artifacts to GitHub Container Registry, both **keyless-signed with Sigstore**
(GitHub OIDC, no long-lived keys) and accompanied by **SLSA build
provenance**; the image additionally carries an **SBOM attestation**:

| Artifact | Reference |
|----------|-----------|
| Container image | `ghcr.io/isometry/milestone-operator` |
| Helm chart (OCI) | `ghcr.io/isometry/charts/milestone-operator` |

This lets you prove an artifact was built by this repository's release
workflow — not substituted or tampered with — before you run or deploy it.

## Trust anchor

Keyless signatures bind to the **workflow identity**, not a key. Pin both of
these everywhere you verify (CLI, Flux, Kyverno). They are the load-bearing
trust anchor — get them right or verification proves nothing:

- **OIDC issuer:** `https://token.actions.githubusercontent.com`
- **Identity (SAN) regexp:**
  `^https://github\.com/isometry/milestone-operator/\.github/workflows/publish\.yaml@refs/tags/v.+$`

The identity is the publishing workflow (`.github/workflows/publish.yaml`)
running on a version tag. Narrow the trailing `v.+` to an exact version
(e.g. `v1\.2\.3`) when you want to pin a specific release.

## Verify the image

### With the GitHub CLI

```sh
gh attestation verify \
  oci://ghcr.io/isometry/milestone-operator:<tag> \
  --repo isometry/milestone-operator
```

### With cosign

Signature:

```sh
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/isometry/milestone-operator/\.github/workflows/publish\.yaml@refs/tags/v.+$' \
  ghcr.io/isometry/milestone-operator:<tag>
```

SLSA provenance attestation:

```sh
cosign verify-attestation --type slsaprovenance \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/isometry/milestone-operator/\.github/workflows/publish\.yaml@refs/tags/v.+$' \
  ghcr.io/isometry/milestone-operator:<tag>
```

SBOM attestation:

```sh
cosign verify-attestation --type spdxjson \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/isometry/milestone-operator/\.github/workflows/publish\.yaml@refs/tags/v.+$' \
  ghcr.io/isometry/milestone-operator:<tag>
```

## Verify the chart

```sh
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/isometry/milestone-operator/\.github/workflows/publish\.yaml@refs/tags/v.+$' \
  ghcr.io/isometry/charts/milestone-operator:<version>
```

Helm can verify on pull once the chart is signed:

```sh
helm pull oci://ghcr.io/isometry/charts/milestone-operator --version <version> --verify
```

## Runtime enforcement

For continuous, cluster-side enforcement (rather than ad-hoc CLI checks),
see the ready-to-apply example policies in
[`deploy/policies/`](../deploy/policies/):

- **Flux** — `OCIRepository`/`HelmRepository` `.spec.verify` rejects an
  unsigned or foreign chart at reconcile time.
- **Kyverno** — a `verifyImages` `ClusterPolicy` rejects an unsigned image,
  or one missing valid SLSA provenance, at admission.

## Negative test

An integrity guarantee is only real if the wrong thing is rejected. Any of
the commands above should **fail** when run against an unsigned image, a
foreign image, or with a mismatched `--certificate-identity-regexp`.
Confirm that before trusting the green path.
