#!/bin/sh
set -eu

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
policy_path=${EPOCH_PACKAGE_POLICY:-"$repository_root/release/package-readiness.json"}
consumer_root=$(mktemp -d "${TMPDIR:-/tmp}/epoch-go-consumer.XXXXXX")
trap 'rm -rf -- "$consumer_root"' EXIT INT TERM

cd "$repository_root"
python3 scripts/release/check-package-policy.py \
  --repo "$repository_root" \
  --policy "$policy_path" \
  --scope go

go mod tidy -diff
module_path=$(go list -m -f '{{.Path}}')

cd "$consumer_root"
GOWORK=off go mod init example.invalid/epoch-package-shape >/dev/null
GOWORK=off go mod edit -require "$module_path@v0.0.0"
GOWORK=off go mod edit -replace "$module_path=$repository_root"
printf '%s\n' \
  'package main' \
  '' \
  'import (' \
  '    "time"' \
  "    epoch \"$module_path/sdk/go/epoch\"" \
  ')' \
  '' \
  'func main() {' \
  '    _, _ = epoch.NewClient("", time.Second)' \
  '}' >main.go
GOWORK=off go mod tidy
GOWORK=off go test ./...

echo "Go package consumer: valid for $module_path"
