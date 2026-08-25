#!/usr/bin/env bash
set -Eeuo pipefail

fail() {
  printf 'release manifest assembly failed: %s\n' "$1" >&2
  exit 1
}

if [[ "$#" -ne 4 ]]; then
  printf 'usage: %s <image> <v-tag> <digest-directory> <github-output>\n' "$0" >&2
  exit 64
fi

image="$1"
tag="$2"
digest_directory="$3"
github_output="$4"
script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
official_images="${script_directory}/release-images.txt"

[[ -f "$official_images" ]] || fail "official release image set is missing"
if [[ ! "$image" =~ ^ghcr\.io/ripan-roy/epoch-[a-z0-9-]+$ ]] || \
  ! grep -Fqx -- "$image" "$official_images"; then
  fail "image is outside the official Epoch release set: $image"
fi
[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-(alpha|beta)\.[0-9]+$ ]] || \
  fail "tag is not a supported Epoch prerelease: $tag"
[[ -d "$digest_directory" ]] || fail "digest directory does not exist"
[[ -d "$(dirname "$github_output")" ]] || fail "GitHub output directory does not exist"
command -v docker >/dev/null || fail "docker is required"
command -v jq >/dev/null || fail "jq is required"

shopt -s nullglob
digest_files=("${digest_directory}"/*)
[[ "${#digest_files[@]}" -eq 2 ]] || \
  fail "expected exactly two platform digest artifacts, found ${#digest_files[@]}"

sources=()
amd64_sources=0
arm64_sources=0

for digest_file in "${digest_files[@]}"; do
  [[ -f "$digest_file" ]] || fail "digest artifact is not a regular file: $digest_file"
  [[ ! -s "$digest_file" ]] || fail "digest artifact must be an empty immutable marker: $digest_file"

  digest="$(basename "$digest_file")"
  [[ "$digest" =~ ^[0-9a-f]{64}$ ]] || fail "invalid platform digest artifact: $digest"
  source="${image}@sha256:${digest}"
  source_manifest="$(docker buildx imagetools inspect "$source" --raw)"

  source_target_count="$(jq '[.manifests[]? | select(.platform.os == "linux" and (.platform.architecture == "amd64" or .platform.architecture == "arm64"))] | length' <<<"$source_manifest")"
  source_unexpected_count="$(jq '[.manifests[]? | select((((.platform.os == "linux") and (.platform.architecture == "amd64" or .platform.architecture == "arm64")) or ((.platform.os == "unknown") and (.platform.architecture == "unknown"))) | not)] | length' <<<"$source_manifest")"
  [[ "$source_target_count" -eq 1 ]] || fail "$source must contain exactly one target runtime manifest"
  [[ "$source_unexpected_count" -eq 0 ]] || fail "$source contains an unexpected platform manifest"

  source_arch="$(jq -er '.manifests[] | select(.platform.os == "linux" and (.platform.architecture == "amd64" or .platform.architecture == "arm64")) | .platform.architecture' <<<"$source_manifest")"
  case "$source_arch" in
    amd64)
      amd64_sources=$((amd64_sources + 1))
      ;;
    arm64)
      arm64_sources=$((arm64_sources + 1))
      ;;
    *)
      fail "$source resolved an unsupported architecture: $source_arch"
      ;;
  esac
  sources+=("$source")
done

[[ "$amd64_sources" -eq 1 ]] || fail "expected exactly one amd64 source"
[[ "$arm64_sources" -eq 1 ]] || fail "expected exactly one arm64 source"

docker buildx imagetools create --tag "${image}:${tag}" "${sources[@]}"
manifest_metadata="$(docker buildx imagetools inspect "${image}:${tag}" --format '{{json .Manifest}}')"
manifest_digest="$(jq -er '.digest | select(test("^sha256:[0-9a-f]{64}$"))' <<<"$manifest_metadata")"
manifest="$(docker buildx imagetools inspect "${image}@${manifest_digest}" --raw)"

amd64_count="$(jq '[.manifests[]? | select(.platform.os == "linux" and .platform.architecture == "amd64")] | length' <<<"$manifest")"
arm64_count="$(jq '[.manifests[]? | select(.platform.os == "linux" and .platform.architecture == "arm64")] | length' <<<"$manifest")"
unexpected_count="$(jq '[.manifests[]? | select((((.platform.os == "linux") and (.platform.architecture == "amd64" or .platform.architecture == "arm64")) or ((.platform.os == "unknown") and (.platform.architecture == "unknown"))) | not)] | length' <<<"$manifest")"
[[ "$amd64_count" -eq 1 ]] || fail "assembled manifest must contain exactly one linux/amd64 runtime"
[[ "$arm64_count" -eq 1 ]] || fail "assembled manifest must contain exactly one linux/arm64 runtime"
[[ "$unexpected_count" -eq 0 ]] || fail "assembled manifest contains an unexpected platform"

amd64_digest="$(jq -er '.manifests[] | select(.platform.os == "linux" and .platform.architecture == "amd64") | .digest | select(test("^sha256:[0-9a-f]{64}$"))' <<<"$manifest")"
arm64_digest="$(jq -er '.manifests[] | select(.platform.os == "linux" and .platform.architecture == "arm64") | .digest | select(test("^sha256:[0-9a-f]{64}$"))' <<<"$manifest")"

{
  printf 'digest=%s\n' "$manifest_digest"
  printf 'amd64=%s\n' "$amd64_digest"
  printf 'arm64=%s\n' "$arm64_digest"
} >>"$github_output"

printf 'assembled %s@%s with linux/amd64 %s and linux/arm64 %s\n' \
  "$image" "$manifest_digest" "$amd64_digest" "$arm64_digest"
