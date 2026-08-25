#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/epoch-release-manifest.XXXXXX")"
trap 'rm -rf -- "$test_root"' EXIT INT TERM

fake_bin="${test_root}/bin"
digest_dir="${test_root}/digests"
output_file="${test_root}/github-output"
docker_log="${test_root}/docker.log"
mkdir -p "$fake_bin" "$digest_dir"

cat >"${fake_bin}/docker" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail

printf '%q ' "$@" >>"$EPOCH_FAKE_DOCKER_LOG"
printf '\n' >>"$EPOCH_FAKE_DOCKER_LOG"

test "${1-}" = buildx
test "${2-}" = imagetools

case "${3-}" in
  create)
    test "${4-}" = --tag
    exit 0
    ;;
  inspect)
    if [[ " $* " == *" --format "* ]]; then
      printf '{"digest":"sha256:%064d","mediaType":"application/vnd.oci.image.index.v1+json"}\n' 0
      exit 0
    fi
    test "${5-}" = --raw
    case "${4-}" in
      *@sha256:1111111111111111111111111111111111111111111111111111111111111111)
        cat <<'JSON'
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[
  {"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","platform":{"os":"linux","architecture":"amd64"}},
  {"digest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","platform":{"os":"unknown","architecture":"unknown"}}
]}
JSON
        exit 0
        ;;
      *@sha256:2222222222222222222222222222222222222222222222222222222222222222)
        cat <<'JSON'
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[
  {"digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","platform":{"os":"linux","architecture":"arm64"}},
  {"digest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","platform":{"os":"unknown","architecture":"unknown"}}
]}
JSON
        exit 0
        ;;
    esac
    case "${EPOCH_FAKE_MANIFEST_MODE:-valid}" in
      valid)
        cat <<'JSON'
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[
  {"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","platform":{"os":"linux","architecture":"amd64"}},
  {"digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","platform":{"os":"linux","architecture":"arm64"}},
  {"digest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","platform":{"os":"unknown","architecture":"unknown"}}
]}
JSON
        ;;
      duplicate-amd64)
        cat <<'JSON'
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[
  {"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","platform":{"os":"linux","architecture":"amd64"}},
  {"digest":"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","platform":{"os":"linux","architecture":"amd64"}},
  {"digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","platform":{"os":"linux","architecture":"arm64"}}
]}
JSON
        ;;
      unexpected-platform)
        cat <<'JSON'
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[
  {"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","platform":{"os":"linux","architecture":"amd64"}},
  {"digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","platform":{"os":"linux","architecture":"arm64"}},
  {"digest":"sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","platform":{"os":"windows","architecture":"amd64"}}
]}
JSON
        ;;
      *)
        exit 65
        ;;
    esac
    ;;
  *)
    exit 64
    ;;
esac
EOF
chmod +x "${fake_bin}/docker"

source_amd64="1111111111111111111111111111111111111111111111111111111111111111"
source_arm64="2222222222222222222222222222222222222222222222222222222222222222"
touch "${digest_dir}/${source_amd64}" "${digest_dir}/${source_arm64}"

while IFS= read -r official_image; do
  : >"$output_file"
  PATH="${fake_bin}:$PATH" \
    EPOCH_FAKE_DOCKER_LOG="$docker_log" \
    "${repository_root}/scripts/create-release-manifest.sh" \
    "$official_image" \
    v0.2.0-beta.6 \
    "$digest_dir" \
    "$output_file"

  grep -Fq "${official_image}@sha256:${source_amd64}" "$docker_log"
  grep -Fq "${official_image}@sha256:${source_arm64}" "$docker_log"
done <"${repository_root}/scripts/release-images.txt"

grep -Fqx 'digest=sha256:0000000000000000000000000000000000000000000000000000000000000000' "$output_file"
grep -Fqx 'amd64=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' "$output_file"
grep -Fqx 'arm64=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' "$output_file"

expect_rejection() {
  local reason="$1"
  shift
  if PATH="${fake_bin}:$PATH" EPOCH_FAKE_DOCKER_LOG="$docker_log" "$@" 2>/dev/null; then
    printf 'release manifest helper accepted %s\n' "$reason" >&2
    exit 1
  fi
}

expect_rejection "an invalid tag" \
  "${repository_root}/scripts/create-release-manifest.sh" \
  ghcr.io/ripan-roy/epoch-node latest "$digest_dir" "$output_file"

expect_rejection "an unofficial image" \
  "${repository_root}/scripts/create-release-manifest.sh" \
  ghcr.io/ripan-roy/epoch-unknown v0.2.0-beta.6 "$digest_dir" "$output_file"

rm -f "${digest_dir}/${source_arm64}"
expect_rejection "one platform digest" \
  "${repository_root}/scripts/create-release-manifest.sh" \
  ghcr.io/ripan-roy/epoch-node v0.2.0-beta.3 "$digest_dir" "$output_file"
touch "${digest_dir}/${source_arm64}"

printf 'tampered\n' >"${digest_dir}/${source_arm64}"
expect_rejection "a nonempty digest artifact" \
  "${repository_root}/scripts/create-release-manifest.sh" \
  ghcr.io/ripan-roy/epoch-node v0.2.0-beta.3 "$digest_dir" "$output_file"
: >"${digest_dir}/${source_arm64}"

EPOCH_FAKE_MANIFEST_MODE=duplicate-amd64 expect_rejection "duplicate amd64 manifests" \
  "${repository_root}/scripts/create-release-manifest.sh" \
  ghcr.io/ripan-roy/epoch-node v0.2.0-beta.3 "$digest_dir" "$output_file"

EPOCH_FAKE_MANIFEST_MODE=unexpected-platform expect_rejection "an unexpected runtime platform" \
  "${repository_root}/scripts/create-release-manifest.sh" \
  ghcr.io/ripan-roy/epoch-node v0.2.0-beta.3 "$digest_dir" "$output_file"

printf 'release manifest helper tests passed\n'
