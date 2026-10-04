#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fixture_root="$(mktemp -d "${TMPDIR:-/tmp}/epoch-release-version.XXXXXX")"
trap 'rm -rf -- "$fixture_root"' EXIT INT TERM
release_version="$(tr -d '\r\n' < "${repository_root}/VERSION")"

fixture_files=(
  VERSION Cargo.toml Cargo.lock package.json console/package.json
  console/src/App.tsx console/src/docs/content.ts console/src/docs/pages.tsx
  sdk/java/pom.xml sdk/java/src/main/java/io/epoch/sdk/HttpTransport.java
  sdk/python/pyproject.toml sdk/python/src/epoch_sdk/transport.py
  sdk/go/epoch/transport.go sdk/go/epoch/transport_test.go
  deploy/kubernetes/operator/deployment.yaml
  deploy/kubernetes/operator/sample-cluster.yaml
  docs/RELEASE_ARTIFACTS.md docs/KUBERNETES_OPERATOR.md
  "docs/releases/v${release_version}.md" scripts/check-release-version.sh
)
for file in "${fixture_files[@]}"; do
  mkdir -p "${fixture_root}/$(dirname "$file")"
  cp "${repository_root}/${file}" "${fixture_root}/${file}"
done

check_fixture() {
  sh "${fixture_root}/scripts/check-release-version.sh" "$@"
}

expect_failure() {
  local label="$1"
  shift
  if check_fixture "$@" >/dev/null 2>&1; then
    printf 'release version contract failed: accepted %s\n' "$label" >&2
    exit 1
  fi
}

check_fixture "v${release_version}"
expect_failure 'a mismatched tag' v99.0.0-beta.1

# Package versions alone are insufficient: the published docs and Kubernetes
# examples must advertise the same release instead of a stale or future image.
# shellcheck disable=SC2016 # Match the literal TypeScript template variable.
public_references=(
  "console/src/docs/content.ts|export const releaseVersion = \"${release_version}\";"
  "console/src/docs/content.ts|export EPOCH_RELEASE_TAG=v${release_version}"
  "console/src/docs/content.ts|nodeImage: ghcr.io/ripan-roy/epoch-node:v${release_version}"
  "console/src/docs/pages.tsx|title=\"v${release_version} release notes\""
  'console/src/docs/pages.tsx|href={`${repositoryDocsUrl}/releases/v'"${release_version}"'.md`}'
  "deploy/kubernetes/operator/deployment.yaml|image: ghcr.io/ripan-roy/epoch-operator:v${release_version}"
  "deploy/kubernetes/operator/sample-cluster.yaml|nodeImage: ghcr.io/ripan-roy/epoch-node:v${release_version}"
  "deploy/kubernetes/operator/sample-cluster.yaml|controlImage: ghcr.io/ripan-roy/epoch-control:v${release_version}"
  "docs/RELEASE_ARTIFACTS.md|export EPOCH_RELEASE_TAG=v${release_version}"
  "docs/KUBERNETES_OPERATOR.md|nodeImage: registry.example/epoch-node:v${release_version}"
  "docs/releases/v${release_version}.md|# Epoch v${release_version}"
)
for reference in "${public_references[@]}"; do
  file="${reference%%|*}"
  literal="${reference#*|}"
  EPOCH_TEST_OLD="$literal" \
    EPOCH_TEST_NEW="${literal/$release_version/99.0.0-beta.1}" \
    perl -0pi -e 's/\Q$ENV{EPOCH_TEST_OLD}\E/$ENV{EPOCH_TEST_NEW}/g' \
    "${fixture_root}/${file}"
  expect_failure "stale release reference: ${reference}"
  cp "${repository_root}/${file}" "${fixture_root}/${file}"
done

mv "${fixture_root}/docs/releases/v${release_version}.md" \
  "${fixture_root}/docs/releases/parked.md"
expect_failure 'missing version-controlled release notes'
mv "${fixture_root}/docs/releases/parked.md" \
  "${fixture_root}/docs/releases/v${release_version}.md"
check_fixture "v${release_version}"
printf 'release version and public-reference contracts passed\n'
