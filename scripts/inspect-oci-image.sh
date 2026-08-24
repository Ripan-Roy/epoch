#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 IMAGE EXPECTED_VERSION EXPECTED_REVISION EXPECTED_ENTRYPOINT EXPECTED_TITLE" >&2
  exit 64
fi

image="$1"
expected_version="$2"
expected_revision="$3"
expected_entrypoint="$4"
expected_title="$5"
expected_source="https://github.com/Ripan-Roy/epoch"

fail() {
  echo "OCI inspection failed for ${image}: $*" >&2
  exit 1
}

inspect() {
  docker image inspect --format "$1" "$image"
}

label() {
  inspect "{{ index .Config.Labels \"$1\" }}"
}

docker image inspect "$image" >/dev/null 2>&1 || fail "image is not present"

test "$(inspect '{{.Os}}')" = "linux" || fail "image OS must be linux"
case "$(inspect '{{.Architecture}}')" in
amd64 | arm64) ;;
*) fail "image architecture must be amd64 or arm64" ;;
esac

runtime_user="$(inspect '{{.Config.User}}')"
case "$runtime_user" in
"" | root | 0 | 0:0 | 0:root | root:root)
  fail "runtime user must be explicit and non-root"
  ;;
esac

entrypoint="$(inspect '{{json .Config.Entrypoint}}')"
jq -e --arg expected "$expected_entrypoint" \
  'type == "array" and index($expected) != null' <<<"$entrypoint" >/dev/null ||
  fail "entrypoint ${entrypoint} does not include ${expected_entrypoint}"

test "$(label org.opencontainers.image.title)" = "$expected_title" ||
  fail "unexpected OCI title"
test "$(label org.opencontainers.image.version)" = "$expected_version" ||
  fail "unexpected OCI version"
test "$(label org.opencontainers.image.revision)" = "$expected_revision" ||
  fail "unexpected OCI revision"
test "$(label org.opencontainers.image.source)" = "$expected_source" ||
  fail "unexpected OCI source"
test "$(label org.opencontainers.image.url)" = "$expected_source" ||
  fail "unexpected OCI URL"
test "$(label org.opencontainers.image.licenses)" = "MIT" ||
  fail "unexpected OCI license"
test "$(label org.opencontainers.image.vendor)" = "Epoch" ||
  fail "unexpected OCI vendor"
test -n "$(label org.opencontainers.image.description)" ||
  fail "OCI description is required"
test -n "$(label org.opencontainers.image.documentation)" ||
  fail "OCI documentation URL is required"

if inspect '{{json .Config.Env}}' | grep -Eiq '(^|[,\[])"[^"=]*(TOKEN|PASSWORD|SECRET|PRIVATE_KEY)='; then
  fail "runtime environment embeds a credential-shaped value"
fi

printf 'verified %s (%s, %s, user %s)\n' \
  "$image" "$expected_version" "$(inspect '{{.Architecture}}')" "$runtime_user"
