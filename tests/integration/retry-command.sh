#!/usr/bin/env bash

set -Eeuo pipefail

epoch_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
epoch_retry_tmp="$(mktemp -d "${TMPDIR:-/tmp}/epoch-retry-command.XXXXXX")"
trap 'rm -rf -- "$epoch_retry_tmp"' EXIT INT TERM

epoch_flaky_command="$epoch_retry_tmp/flaky-command.sh"
epoch_attempt_file="$epoch_retry_tmp/attempts"

cat >"$epoch_flaky_command" <<'EOF'
#!/bin/sh
set -eu
attempts=0
if [ -f "$EPOCH_RETRY_ATTEMPT_FILE" ]; then
  attempts=$(cat "$EPOCH_RETRY_ATTEMPT_FILE")
fi
attempts=$((attempts + 1))
printf '%s\n' "$attempts" >"$EPOCH_RETRY_ATTEMPT_FILE"
if [ "$attempts" -lt "$EPOCH_RETRY_SUCCESS_ON" ]; then
  exit "$EPOCH_RETRY_FAILURE_STATUS"
fi
EOF
chmod +x "$epoch_flaky_command"

EPOCH_RETRY_ATTEMPT_FILE="$epoch_attempt_file" \
  EPOCH_RETRY_SUCCESS_ON=3 \
  EPOCH_RETRY_FAILURE_STATUS=42 \
  "$epoch_repo_root/scripts/retry-command.sh" 3 0 "$epoch_flaky_command"
test "$(cat "$epoch_attempt_file")" = 3

rm -f -- "$epoch_attempt_file"
set +e
EPOCH_RETRY_ATTEMPT_FILE="$epoch_attempt_file" \
  EPOCH_RETRY_SUCCESS_ON=3 \
  EPOCH_RETRY_FAILURE_STATUS=42 \
  "$epoch_repo_root/scripts/retry-command.sh" 2 0 "$epoch_flaky_command"
epoch_status=$?
set -e
test "$epoch_status" = 42
test "$(cat "$epoch_attempt_file")" = 2

if "$epoch_repo_root/scripts/retry-command.sh" 0 0 true 2>/dev/null; then
  printf 'retry helper accepted zero attempts\n' >&2
  exit 1
fi
if "$epoch_repo_root/scripts/retry-command.sh" 1 61 true 2>/dev/null; then
  printf 'retry helper accepted an excessive delay\n' >&2
  exit 1
fi

printf 'Bounded command retry tests passed.\n'
