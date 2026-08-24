#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fixture="${repository_root}/tests/fixtures/oci-inspection.Dockerfile"
tag_prefix="epoch/oci-inspection-test:$$"
good_image="${tag_prefix}-good"
root_image="${tag_prefix}-root"
wrong_revision_image="${tag_prefix}-wrong-revision"

cleanup() {
  docker image rm --force \
    "$good_image" "$root_image" "$wrong_revision_image" >/dev/null 2>&1 || true
}
trap cleanup EXIT

build_fixture() {
  image="$1"
  shift
  docker build \
    --file "$fixture" \
    --tag "$image" \
    "$@" \
    "$repository_root" >/dev/null
}

expect_rejection() {
  image="$1"
  expected_revision="$2"
  if "${repository_root}/scripts/inspect-oci-image.sh" \
    "$image" 0.1.0-test "$expected_revision" /usr/local/bin/epoch "Epoch CLI"; then
    echo "OCI inspection unexpectedly accepted ${image}" >&2
    exit 1
  fi
}

build_fixture "$good_image"
"${repository_root}/scripts/inspect-oci-image.sh" \
  "$good_image" 0.1.0-test fixture-revision /usr/local/bin/epoch "Epoch CLI"

build_fixture "$root_image" --build-arg RUNTIME_USER=0:0
expect_rejection "$root_image" fixture-revision

build_fixture "$wrong_revision_image" --build-arg VCS_REF=other-revision
expect_rejection "$wrong_revision_image" fixture-revision

printf 'OCI inspection negative contract passed\n'
