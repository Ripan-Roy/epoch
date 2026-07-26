#!/bin/sh

set -eu

if [ "$#" -lt 3 ]; then
  printf 'usage: %s <max-attempts> <base-delay-seconds> <command> [args...]\n' \
    "$0" >&2
  exit 64
fi

max_attempts=$1
base_delay_seconds=$2
shift 2

case "$max_attempts" in
  '' | *[!0-9]*)
    printf 'max-attempts must be an integer from 1 through 10\n' >&2
    exit 64
    ;;
esac
case "$base_delay_seconds" in
  '' | *[!0-9]*)
    printf 'base-delay-seconds must be an integer from 0 through 60\n' >&2
    exit 64
    ;;
esac
if [ "$max_attempts" -lt 1 ] || [ "$max_attempts" -gt 10 ]; then
  printf 'max-attempts must be an integer from 1 through 10\n' >&2
  exit 64
fi
if [ "$base_delay_seconds" -gt 60 ]; then
  printf 'base-delay-seconds must be an integer from 0 through 60\n' >&2
  exit 64
fi

attempt=1
while :; do
  if "$@"; then
    exit 0
  else
    command_status=$?
  fi

  if [ "$attempt" -ge "$max_attempts" ]; then
    exit "$command_status"
  fi

  retry_delay=$((base_delay_seconds * attempt))
  printf 'command failed with status %s; retrying attempt %s of %s in %ss\n' \
    "$command_status" "$((attempt + 1))" "$max_attempts" "$retry_delay" >&2
  sleep "$retry_delay"
  attempt=$((attempt + 1))
done
