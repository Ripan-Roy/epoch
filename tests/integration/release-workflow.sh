#!/usr/bin/env bash
# shellcheck disable=SC2016 # GitHub expressions and workflow shell are literals here.
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workflow="${repository_root}/.github/workflows/release-tag.yml"
ci_workflow="${repository_root}/.github/workflows/ci.yml"
release_images="${repository_root}/scripts/release-images.txt"

fail() {
  printf 'release workflow contract failed: %s\n' "$1" >&2
  exit 1
}

require_literal() {
  local literal="$1"
  grep -Fq -- "$literal" "$workflow" || fail "missing ${literal}"
}

reject_literal() {
  local literal="$1"
  if grep -Fq -- "$literal" "$workflow"; then
    fail "forbidden ${literal}"
  fi
}

# Publication must stay tag-only and must not depend on cross-architecture
# emulation. Each target architecture is compiled on a matching native runner.
require_literal "tags:"
require_literal "- 'v*'"
reject_literal "workflow_dispatch:"
reject_literal "docker/setup-qemu-action@"
require_literal "build-platform-images:"
require_literal 'runs-on: ${{ matrix.platform.runner }}'
require_literal "name: linux/amd64"
require_literal "runner: ubuntu-latest"
require_literal "machine: x86_64"
require_literal "name: linux/arm64"
require_literal "runner: ubuntu-24.04-arm"
require_literal "machine: aarch64"
require_literal 'test "$(uname -m)" = "${{ matrix.platform.machine }}"'
require_literal 'platforms: ${{ matrix.platform.name }}'

# Native jobs push untagged, immutable platform results. The trusted finalize
# job alone creates the public exact-version manifest after both builds pass.
require_literal 'outputs: type=image,name=ghcr.io/ripan-roy/epoch-${{ matrix.component.name }},push-by-digest=true,name-canonical=true,push=true'
require_literal 'cache-from: type=gha,scope=release-${{ matrix.component.name }}-${{ matrix.platform.arch }}'
require_literal 'cache-to: type=gha,scope=release-${{ matrix.component.name }}-${{ matrix.platform.arch }},mode=max'
require_literal 'name: epoch-${{ matrix.component.name }}-digest-${{ matrix.platform.arch }}'
require_literal 'pattern: epoch-${{ matrix.component.name }}-digest-*'
require_literal "./scripts/create-release-manifest.sh"
require_literal '/tmp/digests'
require_literal 'DIGEST: ${{ steps.manifest.outputs.amd64 }}'

# The build matrix, finalize matrix, and manifest helper must share the exact
# official image set. This rejects adding an image to only part of the release
# pipeline, which would otherwise fail after expensive native builds finish.
official_components="$(
  sed 's#^ghcr\.io/ripan-roy/epoch-##' "$release_images" | sort
)"
[[ "$(printf '%s\n' "$official_components" | sed '/^$/d' | wc -l | tr -d ' ')" -eq 5 ]] || \
  fail "official release image set must contain exactly five components"
[[ "$(printf '%s\n' "$official_components" | sort -u)" == "$official_components" ]] || \
  fail "official release image set contains duplicates"

matrix_components() {
  local job="$1"
  awk -v job="  ${job}:" '
    $0 == job { in_job = 1; next }
    in_job && /^  [a-z0-9-]+:/ { exit }
    in_job && /^          - name: [a-z][a-z0-9-]*$/ {
      sub(/^          - name: /, "")
      print
    }
  ' "$workflow" | sort
}

for matrix_job in build-platform-images publish-images; do
  [[ "$(matrix_components "$matrix_job")" == "$official_components" ]] || \
    fail "$matrix_job does not match scripts/release-images.txt"
done

# The faster topology must retain the release trust boundary and evidence:
# exact-main provenance, one platform SBOM per architecture and component,
# manifest provenance, keyless signing, attestation verification, and curated
# prerelease publication. No mutable latest tag may enter the workflow.
require_literal "Require the current main commit"
require_literal "Require successful exact-main CI and Pages"
require_literal "Generate amd64 SPDX SBOM from published digest"
require_literal "Generate arm64 SPDX SBOM from published digest"
require_literal 'subject-digest: ${{ steps.manifest.outputs.digest }}'
require_literal 'cosign sign --yes "${IMAGE}@${DIGEST}"'
require_literal 'gh attestation verify "oci://${IMAGE}@${DIGEST}"'
require_literal 'path: epoch-${{ matrix.component.name }}-${{ github.ref_name }}-linux-*.spdx.json'
require_literal "Publish release notes and SBOM assets"
reject_literal ":latest"

grep -Fq -- "make test-release-manifest test-release-workflow" "$ci_workflow" || \
  fail "CI does not execute the release workflow contracts"

printf 'release workflow contract passed\n'
