#!/usr/bin/env bash

set -Eeuo pipefail

epoch_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
epoch_compose_file="$epoch_repo_root/deploy/compose/docker-compose.consensus-probe.yml"
epoch_project_name="${EPOCH_PROBE_PROJECT_NAME:-epoch-probe-smoke-$$}"
epoch_artifact_dir="${EPOCH_PROBE_ARTIFACT_DIR:-}"
epoch_use_existing_image="${EPOCH_PROBE_USE_EXISTING_IMAGE:-0}"
epoch_internal_peer_message_path=/internal/v1/consensus/messages
epoch_experimental_status_path=/experimental/v1/consensus/status

epoch_ports=()
while IFS= read -r epoch_port; do
  epoch_ports+=("$epoch_port")
done < <(python3 - <<'PYTHON'
import socket

sockets = []
try:
    for _ in range(6):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
    for sock in sockets:
        print(sock.getsockname()[1])
finally:
    for sock in sockets:
        sock.close()
PYTHON
)

if [[ ${#epoch_ports[@]} -ne 6 ]]; then
  printf 'failed to allocate six loopback ports\n' >&2
  exit 1
fi

export EPOCH_PROBE_HTTP_PORT_1="${epoch_ports[0]}"
export EPOCH_PROBE_HTTP_PORT_2="${epoch_ports[1]}"
export EPOCH_PROBE_HTTP_PORT_3="${epoch_ports[2]}"
export EPOCH_PROBE_PEER_PORT_1="${epoch_ports[3]}"
export EPOCH_PROBE_PEER_PORT_2="${epoch_ports[4]}"
export EPOCH_PROBE_PEER_PORT_3="${epoch_ports[5]}"

epoch_http_ports=(
  "$EPOCH_PROBE_HTTP_PORT_1"
  "$EPOCH_PROBE_HTTP_PORT_2"
  "$EPOCH_PROBE_HTTP_PORT_3"
)
epoch_peer_ports=(
  "$EPOCH_PROBE_PEER_PORT_1"
  "$EPOCH_PROBE_PEER_PORT_2"
  "$EPOCH_PROBE_PEER_PORT_3"
)
epoch_services=(epoch-probe-1 epoch-probe-2 epoch-probe-3)
epoch_compose=(
  docker compose
  --project-name "$epoch_project_name"
  --file "$epoch_compose_file"
)

cleanup() {
  epoch_status=$?
  trap - EXIT INT TERM
  if (( epoch_status != 0 )); then
    if [[ -n "$epoch_artifact_dir" ]]; then
      mkdir -p "$epoch_artifact_dir"
      "${epoch_compose[@]}" logs --no-color >"$epoch_artifact_dir/containers.log" 2>&1 || true
      "${epoch_compose[@]}" ps --all >"$epoch_artifact_dir/containers.txt" 2>&1 || true
      env | grep '^EPOCH_PROBE_.*PORT_' | sort >"$epoch_artifact_dir/ports.txt" || true
      for epoch_service in "${epoch_services[@]}"; do
        mkdir -p "$epoch_artifact_dir/state/$epoch_service"
        "${epoch_compose[@]}" cp \
          "$epoch_service:/var/lib/epoch/consensus/." \
          "$epoch_artifact_dir/state/$epoch_service" >/dev/null 2>&1 || true
      done
    else
      "${epoch_compose[@]}" logs --no-color --tail 200 >&2 || true
    fi
  fi
  "${epoch_compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$epoch_status"
}
trap cleanup EXIT INT TERM

json_field() {
  local document=$1
  local field=$2
  python3 -c 'import json,sys; print(json.loads(sys.argv[1])[sys.argv[2]])' \
    "$document" "$field"
}

assert_committed_lookup() {
  local document=$1
  local expected_proposal_id=$2
  local expected_payload=$3
  python3 -c '
import json
import sys

document = json.loads(sys.argv[1])
expected_proposal_id = int(sys.argv[2])
expected_payload = json.loads(sys.argv[3])
assert document["proposal_id"] == expected_proposal_id, document
assert document["state"] == "committed", document
assert document["observation_scope"] == "local", document
commit = document["commit"]
assert isinstance(commit["term"], int) and commit["term"] > 0, document
assert isinstance(commit["log_index"], int) and commit["log_index"] > 0, document
assert commit["payload"] == expected_payload, document
' "$document" "$expected_proposal_id" "$expected_payload"
}

assert_identical_commit() {
  local proposal_id=$1
  local expected_payload=$2
  shift 2
  local expected_nodes=("$@")
  local node_id
  local lookup
  local reference=
  for node_id in "${expected_nodes[@]}"; do
    lookup="$(lookup_for_node "$node_id" "$proposal_id")"
    assert_committed_lookup "$lookup" "$proposal_id" "$expected_payload"
    if [[ -z "$reference" ]]; then
      reference=$lookup
    else
      [[ "$lookup" == "$reference" ]]
    fi
  done
}

status_for_node() {
  local node_id=$1
  local port="${epoch_peer_ports[node_id - 1]}"
  curl --fail --silent --show-error \
    "http://127.0.0.1:${port}/experimental/v1/consensus/status"
}

lookup_for_node() {
  local node_id=$1
  local proposal_id=$2
  local port="${epoch_peer_ports[node_id - 1]}"
  curl --fail --silent --show-error \
    "http://127.0.0.1:${port}/experimental/v1/consensus/proposals/${proposal_id}"
}

wait_for_nodes() {
  local node_id
  local ready
  for _ in {1..200}; do
    ready=0
    for node_id in 1 2 3; do
      if curl --fail --silent \
        "http://127.0.0.1:${epoch_http_ports[node_id - 1]}/healthz" >/dev/null 2>&1 \
        && status_for_node "$node_id" >/dev/null 2>&1; then
        ready=$((ready + 1))
      fi
    done
    if (( ready == 3 )); then
      return 0
    fi
    sleep 0.1
  done
  printf 'consensus probe nodes did not become ready\n' >&2
  return 1
}

assert_public_listener_isolated() {
  local node_id=$1
  local port="${epoch_http_ports[node_id - 1]}"
  local path
  local status_code
  for path in "$epoch_internal_peer_message_path" "$epoch_experimental_status_path"; do
    status_code="$(curl --silent --output /dev/null --write-out '%{http_code}' \
      "http://127.0.0.1:${port}${path}")"
    if [[ "$status_code" != 404 ]]; then
      printf 'internal path %s was exposed on node %s public port with status %s\n' \
        "$path" "$node_id" "$status_code" >&2
      return 1
    fi
  done
}

wait_for_leader() {
  local excluded_node=${1:-0}
  local node_id
  local status
  for _ in {1..200}; do
    for node_id in 1 2 3; do
      if (( node_id == excluded_node )); then
        continue
      fi
      status="$(status_for_node "$node_id" 2>/dev/null || true)"
      if [[ -n "$status" ]] && [[ "$(json_field "$status" role)" == leader ]]; then
        printf '%s %s\n' "$node_id" "$(json_field "$status" term)"
        return 0
      fi
    done
    sleep 0.1
  done
  printf 'consensus probe did not elect a leader\n' >&2
  return 1
}

propose() {
  local node_id=$1
  local term=$2
  local proposal_id=$3
  local payload=$4
  local port="${epoch_peer_ports[node_id - 1]}"
  curl --fail --silent --show-error \
    --header 'content-type: application/json' \
    --data "{\"proposal_id\":${proposal_id},\"expected_term\":${term},\"payload\":${payload}}" \
    "http://127.0.0.1:${port}/experimental/v1/consensus/proposals" >/dev/null
}

wait_for_commit() {
  local proposal_id=$1
  shift
  local expected_nodes=("$@")
  local committed
  local lookup
  local node_id
  for _ in {1..200}; do
    committed=0
    for node_id in "${expected_nodes[@]}"; do
      lookup="$(lookup_for_node "$node_id" "$proposal_id" 2>/dev/null || true)"
      if [[ -n "$lookup" ]] && [[ "$(json_field "$lookup" state)" == committed ]]; then
        committed=$((committed + 1))
      fi
    done
    if (( committed == ${#expected_nodes[@]} )); then
      return 0
    fi
    sleep 0.1
  done
  printf 'proposal %s did not commit on nodes %s\n' \
    "$proposal_id" "${expected_nodes[*]}" >&2
  return 1
}

cd "$epoch_repo_root"
if [[ "$epoch_use_existing_image" == 1 ]]; then
  "${epoch_compose[@]}" up --no-build --detach
else
  "${epoch_compose[@]}" up --build --detach
fi
wait_for_nodes

for epoch_node_id in 1 2 3; do
  assert_public_listener_isolated "$epoch_node_id"
  epoch_status="$(status_for_node "$epoch_node_id")"
  [[ "$(json_field "$epoch_status" stability)" == experimental ]]
  [[ "$(json_field "$epoch_status" production_readiness)" == not_production_ready ]]
  [[ "$(json_field "$epoch_status" profile_replication)" == False ]]
  [[ "$(json_field "$epoch_status" profile_guarantee_ceiling)" == local_durable ]]
  [[ "$(json_field "$epoch_status" peer_authentication)" == none ]]
done

read -r epoch_leader epoch_term < <(wait_for_leader)
propose "$epoch_leader" "$epoch_term" 101 '[101,112,111,99,104]'
wait_for_commit 101 1 2 3
assert_identical_commit 101 '[101,112,111,99,104]' 1 2 3

epoch_leader_service="${epoch_services[epoch_leader - 1]}"
"${epoch_compose[@]}" stop "$epoch_leader_service" >/dev/null
read -r epoch_next_leader epoch_next_term < <(wait_for_leader "$epoch_leader")
if (( epoch_next_term <= epoch_term )); then
  printf 'leader term did not advance: %s -> %s\n' "$epoch_term" "$epoch_next_term" >&2
  exit 1
fi
propose "$epoch_next_leader" "$epoch_next_term" 102 '[102,97,105,108,111,118,101,114]'
epoch_survivors=()
for epoch_node_id in 1 2 3; do
  if (( epoch_node_id != epoch_leader )); then
    epoch_survivors+=("$epoch_node_id")
  fi
done
wait_for_commit 102 "${epoch_survivors[@]}"

"${epoch_compose[@]}" start "$epoch_leader_service" >/dev/null
wait_for_commit 102 1 2 3
assert_identical_commit 102 '[102,97,105,108,111,118,101,114]' 1 2 3

printf 'Epoch three-container consensus probe failover smoke passed.\n'
