# Verifying release artifacts

Every tagged release (`v*.*.*`) of `milestone-operator` publishes the
artifacts below. All of them carry **SLSA build provenance** as a GitHub
attestation — except the per-archive SBOMs, which are informational (see
below). The manager image, the Helm chart and the `milestonectl` checksum
manifest are additionally **keyless-signed with Sigstore** (GitHub OIDC, no
long-lived keys), and the manager image also carries an **SBOM
attestation**:

| Artifact | Reference |
|----------|-----------|
| Manager image | `ghcr.io/isometry/milestone-operator` |
| Helm chart (OCI) | `oci://ghcr.io/isometry/charts/milestone-operator` |
| Kustomize install bundle | `install.yaml` GitHub Release asset (the release's versioned image baked in) |
| `milestonectl` GitHub Release archives | `milestonectl_<version>_<os>_<arch>.{tar.gz,zip}`, `milestonectl_<version>_SHA256SUMS`, `milestonectl_<version>_SHA256SUMS.sigstore.json` |
| `milestonectl` per-archive SBOMs (SPDX, from syft) | `milestonectl_<version>_<os>_<arch>.{tar.gz,zip}.sbom.json` — informational only, see below |
| `milestonectl` Homebrew bottles | `ghcr.io/isometry/tap/milestonectl` (via `brew trust isometry/tap && brew install isometry/tap/milestonectl`) |

This lets you prove an artifact was built by this repository's release
workflow — not substituted or tampered with — before you run or deploy it.

Prerelease tags (`vX.Y.Z-rc.N`) publish the image, the chart, the install
bundle and the `milestonectl` archives exactly as above, but they build **no
Homebrew bottles** and never move the `latest` image tag — so `brew install`
and `:latest` always resolve to the newest stable release.

Only *git* tags carry the `v` prefix. Published image and chart tags are the
bare semver: the release tagged `v0.3.0` pushes
`ghcr.io/isometry/milestone-operator:0.3.0` and
`oci://ghcr.io/isometry/charts/milestone-operator:0.3.0`. Wherever `<tag>`
or `<version>` appears below, use the un-prefixed form (the
`milestonectl_<version>_…` filenames are un-prefixed too). Binaries still
report the v-prefixed git tag: `milestonectl version` and
`manager --version` print `v0.3.0`.

## Trust anchor

Keyless signatures bind to the **workflow identity**, not a key. Pin both of
these everywhere you verify (CLI, Flux, Kyverno). They are the load-bearing
trust anchor — get them right or verification proves nothing:

- **OIDC issuer:** `https://token.actions.githubusercontent.com`
- **Identity (SAN) regexp:**
  `^https://github\.com/isometry/milestone-operator/\.github/workflows/publish\.yaml@refs/tags/v.+$`

The identity is the publishing workflow (`.github/workflows/publish.yaml`)
running on a version tag. It signs every artifact above, including the
`milestonectl` checksum manifest. Narrow the trailing `v.+` to an exact
version (e.g. `v1\.2\.3`) when you want to pin a specific release.

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

### SLSA provenance and SBOM attestations

`cosign verify-attestation` looks up `sha256-<digest>.att` tags, which
`actions/attest-build-provenance`/`actions/attest` do not write (they
publish to GitHub's attestation store and, with `push-to-registry: true`,
as OCI 1.1 referrers — see [Artifact layout & mirroring](#artifact-layout--mirroring)
below). `gh attestation verify` is the supported path for both:

```sh
# SLSA provenance (this is gh's default predicate type, shown explicitly here)
gh attestation verify \
  oci://ghcr.io/isometry/milestone-operator:<tag> \
  --repo isometry/milestone-operator \
  --predicate-type https://slsa.dev/provenance/v1

# SPDX SBOM attestation
gh attestation verify \
  oci://ghcr.io/isometry/milestone-operator:<tag> \
  --repo isometry/milestone-operator \
  --predicate-type https://spdx.dev/Document/v2.3
```

## Verify the chart

```sh
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/isometry/milestone-operator/\.github/workflows/publish\.yaml@refs/tags/v.+$' \
  ghcr.io/isometry/charts/milestone-operator:<version>

gh attestation verify \
  oci://ghcr.io/isometry/charts/milestone-operator:<version> \
  --repo isometry/milestone-operator
```

Helm's own `--verify` flag checks a PGP `.prov` provenance file; this
pipeline signs the chart with cosign (keyless, Sigstore) instead and
publishes no `.prov`, so `helm pull --verify` **does not work** against this
chart — use the `cosign`/`gh attestation` commands above.

## Verify the install bundle

`install.yaml` is a GitHub Release asset, not a registry artifact; it carries
a GitHub build-provenance attestation and nothing else (no cosign signature,
no SBOM). Download it, then:

```sh
gh attestation verify install.yaml --repo isometry/milestone-operator
```

## Verify `milestonectl`

### GitHub Release archives

Each release ships a `SHA256SUMS` file and a matching Sigstore bundle
(`.sigstore.json`) signing that checksum manifest, plus per-archive GitHub
attestations. Verify the checksum manifest's signature, then the archive
against the manifest:

```sh
cosign verify-blob \
  --bundle milestonectl_<version>_SHA256SUMS.sigstore.json \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/isometry/milestone-operator/\.github/workflows/publish\.yaml@refs/tags/v.+$' \
  milestonectl_<version>_SHA256SUMS

sha256sum -c --ignore-missing milestonectl_<version>_SHA256SUMS
```

(`--ignore-missing` because the manifest covers every platform's archive and
you have downloaded one; without it `sha256sum` reports the absent files as
failures.)

Or, per-archive, with GitHub attestations:

```sh
gh attestation verify milestonectl_<version>_<os>_<arch>.tar.gz \
  --repo isometry/milestone-operator
```

Each release also ships a per-archive SPDX SBOM,
`milestonectl_<version>_<os>_<arch>.{tar.gz,zip}.sbom.json` (generated by
syft via goreleaser's `sboms:` config — see `.goreleaser.yaml`). It is
**not** listed in `SHA256SUMS` (the checksum manifest only covers archives)
and is not itself signed or attested — it's an informational companion
file, not a verifiable artifact in its own right. Verify the archive it
describes instead, with the commands above.

### Homebrew bottles

`brew install isometry/tap/milestonectl` pours a bottle from
`ghcr.io/isometry/tap/milestonectl`. Bottles are **attested**, not
cosign-signed: both the bottle tarball and the binary inside it carry a
GitHub build-provenance attestation, so you can verify the installed file
directly:

```sh
gh attestation verify "$(brew --prefix)/bin/milestonectl" \
  --repo isometry/milestone-operator
```

## Artifact layout & mirroring

The manager image is built with `ko`, not BuildKit: the image index holds
only the per-platform images, with no embedded SBOM or provenance
manifests. Every supply-chain artifact for it lives outside the index — the
difference matters when you mirror:

- **As OCI 1.1 referrers**: the GitHub-signed SLSA provenance and SPDX SBOM
  attestations (`actions/attest-build-provenance`, `actions/attest`, both
  `push-to-registry: true`) are attached to the image digest as referrers —
  the chart carries its provenance attestation the same way. Referrers point
  *at* the subject via the OCI 1.1 referrers API, so a plain `skopeo copy
  --all` or a tag-only copy does **not** carry them (skopeo has no
  referrers support: [containers/skopeo#2061]), and a referrers-aware
  consumer looking for them against such a mirror finds nothing.
- **The cosign signature: either, depending on cosign's version** — the
  classic layout attaches it as a `sha256-<digest>.sig` tag, while cosign's
  newer bundle format attaches it as an OCI referrer instead. Don't assume
  which one a given release has; copy both layouts and you are correct
  either way, which is exactly what `regctl image copy --referrers
  --digest-tags` (below) does. A mirror missing the signature makes
  `cosign verify` — and the Flux/Kyverno policies — fail against it.

To mirror with the signature and signed attestations intact, use a copy
tool that handles both digest tags and referrers:

```sh
regctl image copy --referrers --digest-tags ghcr.io/isometry/milestone-operator:<tag> <mirror>/milestone-operator:<tag>
# or: oras cp -r … / cosign copy … (cosign copy carries a digest-tagged
# signature; pair it with a referrers-aware tool for the attestations —
# and for a bundle-format signature)
# or repo-level: skopeo sync (copies the sha256-<digest> tags as ordinary
# tags; the destination's referrers API re-indexes any copied referrers)
```

Independently of registry contents, `gh attestation verify` works against
**any** mirror: attestations are also stored in GitHub's attestation store,
keyed by the image digest, which copying preserves. The `milestonectl`
archives' Sigstore bundle and GitHub attestations likewise travel with the
files themselves, so mirroring the GitHub Release (or the Homebrew bottle)
is enough — no referrers-aware tooling needed for those.

[containers/skopeo#2061]: https://github.com/containers/skopeo/issues/2061

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
the commands above should **fail** when run against an unsigned artifact, a
foreign artifact, or with a mismatched `--certificate-identity-regexp`.
Confirm that before trusting the green path.
