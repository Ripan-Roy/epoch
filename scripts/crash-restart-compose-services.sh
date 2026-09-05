#!/usr/bin/env bash

set -Eeuo pipefail

if (( $# < 3 )); then
  printf 'usage: %s PROJECT_NAME COMPOSE_FILE SERVICE [SERVICE ...]\n' "$0" >&2
  exit 64
fi

epoch_project_name=$1
epoch_compose_file=$2
shift 2
epoch_services=("$@")
epoch_compose=(
  docker compose
  --project-name "$epoch_project_name"
  --file "$epoch_compose_file"
)

wait_for_services_stopped() {
  local container_id
  local running
  local service
  local state

  for _ in {1..100}; do
    running=0
    for service in "${epoch_services[@]}"; do
      container_id="$("${epoch_compose[@]}" ps --all --quiet "$service")"
      if [[ -z "$container_id" ]]; then
        printf 'Compose service %s has no container in project %s\n' \
          "$service" "$epoch_project_name" >&2
        return 1
      fi
      state="$(docker inspect --format '{{.State.Running}}' "$container_id")"
      case "$state" in
        true) running=$((running + 1)) ;;
        false) ;;
        *)
          printf 'Unexpected running state for Compose service %s: %s\n' \
            "$service" "$state" >&2
          return 1
          ;;
      esac
    done
    if ((running == 0)); then
      return 0
    fi
    sleep 0.1
  done

  printf 'Compose services did not stop after SIGKILL in project %s: %s\n' \
    "$epoch_project_name" "${epoch_services[*]}" >&2
  "${epoch_compose[@]}" ps --all >&2 || true
  return 1
}

"${epoch_compose[@]}" kill --signal SIGKILL "${epoch_services[@]}" >/dev/null
# The kill request can return before every container finishes exiting. Starting
# immediately can skip a still-running container which then exits and stays down.
wait_for_services_stopped
"${epoch_compose[@]}" start "${epoch_services[@]}" >/dev/null
