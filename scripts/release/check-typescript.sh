#!/bin/sh
set -eu

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
policy_path=${EPOCH_PACKAGE_POLICY:-"$repository_root/release/package-readiness.json"}
package_root="$repository_root/sdk/typescript"

cd "$repository_root"
python3 scripts/release/check-package-policy.py \
  --repo "$repository_root" \
  --policy "$policy_path" \
  --scope typescript

if [ ! -f "$package_root/package.json" ]; then
  echo "TypeScript package dry-run: skipped; SDK status is explicitly planned"
  exit 0
fi

pnpm --dir "$package_root" run lint
pnpm --dir "$package_root" run typecheck
pnpm --dir "$package_root" run test
pnpm --dir "$package_root" run build

pack_report=$(mktemp "${TMPDIR:-/tmp}/epoch-typescript-pack.XXXXXX")
trap 'rm -f -- "$pack_report"' EXIT INT TERM
(
  cd "$package_root"
  npm pack --dry-run --json >"$pack_report"
)
python3 - "$pack_report" <<'PYTHON'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as report_file:
    report = json.load(report_file)
if not isinstance(report, list) or len(report) != 1:
    raise SystemExit("npm pack dry-run must describe exactly one package")
files = report[0].get("files", [])
paths = {entry.get("path") for entry in files if isinstance(entry, dict)}
for required_path in ("package.json",):
    if required_path not in paths:
        raise SystemExit(f"npm pack dry-run omitted {required_path}")
for path in paths:
    if isinstance(path, str) and (path.startswith(".env") or "/.env" in path):
        raise SystemExit(f"npm pack dry-run included secret-prone path: {path}")
PYTHON

echo "TypeScript package dry-run: valid"
