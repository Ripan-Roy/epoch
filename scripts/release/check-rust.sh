#!/bin/sh
set -eu

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
policy_path=${EPOCH_PACKAGE_POLICY:-"$repository_root/release/package-readiness.json"}
metadata_path=$(mktemp "${TMPDIR:-/tmp}/epoch-cargo-metadata.XXXXXX")
trap 'rm -f -- "$metadata_path"' EXIT INT TERM

cd "$repository_root"
cargo metadata --locked --no-deps --format-version 1 >"$metadata_path"
python3 scripts/release/check-package-policy.py \
  --repo "$repository_root" \
  --policy "$policy_path" \
  --scope rust \
  --cargo-metadata "$metadata_path"

public_crates=$(
  python3 - "$policy_path" <<'PYTHON'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as policy_file:
    policy = json.load(policy_file)
print(" ".join(policy["packages"]["rust"]["publicCrates"]))
PYTHON
)

if [ -z "$public_crates" ]; then
  echo "Rust package dry-run: skipped; every workspace crate is explicitly publish=false"
  exit 0
fi

# This branch is unreachable for the source-only alpha.2 policy. When a reviewed
# public allowlist is added, Cargo packages exactly that allowlist locally. It
# never invokes a registry publication command.
set -- cargo package --locked
for crate in $public_crates; do
  set -- "$@" -p "$crate"
done
"$@"

for crate in $public_crates; do
  cargo package --locked --list -p "$crate" >/dev/null
done
