#!/usr/bin/env bash

set -Eeuo pipefail

epoch_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
epoch_test_dir="$(mktemp -d "${TMPDIR:-/tmp}/epoch-compose-crash-restart.XXXXXX")"
trap 'rm -rf -- "$epoch_test_dir"' EXIT INT TERM

epoch_mock_bin="$epoch_test_dir/bin"
epoch_mock_log="$epoch_test_dir/docker.log"
epoch_mock_state="$epoch_test_dir/state"
mkdir -p "$epoch_mock_bin" "$epoch_mock_state"

cat >"$epoch_mock_bin/docker" <<'MOCK'
#!/usr/bin/env bash
set -Eeuo pipefail

printf '%s\n' "$*" >>"$EPOCH_MOCK_DOCKER_LOG"

if [[ "$1" == inspect ]]; then
  case "${EPOCH_MOCK_MODE:-delayed}" in
    invalid-state) printf 'unknown\n'; exit 0 ;;
    inspect-error) exit 42 ;;
    never-stops) printf 'true\n'; exit 0 ;;
  esac
  container_id=${4}
  state_file="$EPOCH_MOCK_DOCKER_STATE/$container_id"
  state="$(<"$state_file")"
  printf '%s\n' "$state"
  if [[ "$state" == true ]]; then
    printf 'false\n' >"$state_file"
  fi
  exit 0
fi

shift
[[ "$1" == --project-name ]]
shift 2
[[ "$1" == --file ]]
shift 2
command=$1
shift

case "$command" in
  kill)
    if [[ "${EPOCH_MOCK_MODE:-}" == kill-error ]]; then exit 43; fi
    [[ "$1" == --signal && "$2" == SIGKILL ]]
    shift 2
    for service in "$@"; do
      printf 'true\n' >"$EPOCH_MOCK_DOCKER_STATE/container-$service"
    done
    ;;
  ps)
    if [[ "$#" == 1 && "$1" == --all ]]; then exit 0; fi
    [[ "$1" == --all && "$2" == --quiet ]]
    if [[ "${EPOCH_MOCK_MODE:-}" == missing-container ]]; then exit 0; fi
    printf 'container-%s\n' "$3"
    ;;
  start)
    if [[ "${EPOCH_MOCK_MODE:-}" == start-error ]]; then exit 44; fi
    for service in "$@"; do
      [[ "$(<"$EPOCH_MOCK_DOCKER_STATE/container-$service")" == false ]]
    done
    ;;
  *)
    printf 'unexpected mocked Docker command: %s\n' "$command" >&2
    exit 1
    ;;
esac
MOCK
chmod +x "$epoch_mock_bin/docker"

PATH="$epoch_mock_bin:$PATH" \
EPOCH_MOCK_DOCKER_LOG="$epoch_mock_log" \
EPOCH_MOCK_DOCKER_STATE="$epoch_mock_state" \
  "$epoch_repo_root/scripts/crash-restart-compose-services.sh" \
    epoch-test \
    deploy/compose/docker-compose.consensus-probe.yml \
    epoch-probe-1 epoch-probe-2 epoch-probe-3

grep -q \
  'kill --signal SIGKILL epoch-probe-1 epoch-probe-2 epoch-probe-3' \
  "$epoch_mock_log"
grep -q \
  'start epoch-probe-1 epoch-probe-2 epoch-probe-3' \
  "$epoch_mock_log"
for epoch_service in epoch-probe-1 epoch-probe-2 epoch-probe-3; do
  epoch_inspections="$(grep -c "inspect --format {{.State.Running}} container-$epoch_service" \
    "$epoch_mock_log")"
  if ((epoch_inspections < 2)); then
    printf 'service %s was not observed running and then stopped\n' "$epoch_service" >&2
    exit 1
  fi
done

for epoch_mode in invalid-state inspect-error never-stops missing-container kill-error start-error; do
  # Skip the polling delay only in the fake-Docker test; keep the real bound.
  printf '#!/usr/bin/env bash\nexit 0\n' >"$epoch_mock_bin/sleep"
  chmod +x "$epoch_mock_bin/sleep"
  : >"$epoch_mock_log"
  epoch_status=0
  PATH="$epoch_mock_bin:$PATH" \
  EPOCH_MOCK_DOCKER_LOG="$epoch_mock_log" \
  EPOCH_MOCK_DOCKER_STATE="$epoch_mock_state" \
  EPOCH_MOCK_MODE="$epoch_mode" \
    "$epoch_repo_root/scripts/crash-restart-compose-services.sh" \
      epoch-test deploy/compose/docker-compose.consensus-probe.yml \
      epoch-probe-1 epoch-probe-2 epoch-probe-3 \
      >"$epoch_test_dir/failure.log" 2>&1 || epoch_status=$?
  case "$epoch_mode" in
    inspect-error) epoch_expected=42 ;;
    kill-error) epoch_expected=43 ;;
    start-error) epoch_expected=44 ;;
    *) epoch_expected=1 ;;
  esac
  if ((epoch_status != epoch_expected)); then
    printf '%s returned %s instead of %s\n' "$epoch_mode" "$epoch_status" "$epoch_expected" >&2
    exit 1
  fi
  if [[ "$epoch_mode" != start-error ]] && grep -q ' start ' "$epoch_mock_log"; then
    printf '%s incorrectly attempted to restart containers\n' "$epoch_mode" >&2
    exit 1
  fi
done

printf 'Compose crash/restart lifecycle tests passed.\n'
