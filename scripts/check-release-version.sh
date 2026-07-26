#!/bin/sh

set -eu

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"

release_version=$(tr -d '\r\n' < VERSION)
python_version=$(printf '%s' "$release_version" | sed 's/-alpha\./a/')

case "$release_version" in
  [0-9]*.[0-9]*.[0-9]*-alpha.[0-9]*) ;;
  *)
    printf 'VERSION must contain an alpha SemVer, got %s\n' "$release_version" >&2
    exit 1
    ;;
esac

expect_line() {
  file=$1
  expected=$2
  if ! grep -Fqx "$expected" "$file"; then
    printf '%s does not contain the expected release metadata: %s\n' \
      "$file" "$expected" >&2
    exit 1
  fi
}

expect_line Cargo.toml "version = \"$release_version\""
expect_line package.json "  \"version\": \"$release_version\","
expect_line console/package.json "  \"version\": \"$release_version\","
expect_line console/src/App.tsx "const releaseVersion = \"$release_version\";"
expect_line sdk/java/pom.xml "  <version>$release_version</version>"
expect_line sdk/java/src/main/java/io/epoch/sdk/HttpTransport.java \
  "  private static final String USER_AGENT = \"epoch-java/$release_version\";"
expect_line sdk/python/pyproject.toml "version = \"$python_version\""
expect_line sdk/python/src/epoch_sdk/transport.py \
  "        headers = {\"accept\": \"application/json\", \"user-agent\": \"epoch-python/$python_version\"}"
expect_line sdk/go/epoch/transport.go \
  "	userAgent        = \"epoch-go/$release_version\""
expect_line sdk/go/epoch/transport_test.go \
  "		if request.Header.Get(\"User-Agent\") != \"epoch-go/$release_version\" {"

bad_lock_versions=$(
  awk -v expected="version = \"$release_version\"" '
    $0 == "[[package]]" {
      epoch_package = 0
      next
    }
    $0 ~ /^name = "epoch-/ {
      epoch_package = 1
      next
    }
    epoch_package && $0 ~ /^version = / {
      if ($0 != expected) {
        print $0
      }
      epoch_package = 0
    }
  ' Cargo.lock
)
if [ -n "$bad_lock_versions" ]; then
  printf 'Cargo.lock contains stale Epoch package versions:\n%s\n' \
    "$bad_lock_versions" >&2
  exit 1
fi

if [ "$#" -gt 1 ]; then
  printf 'usage: %s [v<version>]\n' "$0" >&2
  exit 1
fi
if [ "$#" -eq 1 ] && [ "$1" != "v$release_version" ]; then
  printf 'tag %s does not match VERSION v%s\n' "$1" "$release_version" >&2
  exit 1
fi

printf 'release metadata is synchronized at %s\n' "$release_version"
