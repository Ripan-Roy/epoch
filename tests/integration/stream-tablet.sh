#!/usr/bin/env bash

set -Eeuo pipefail

epoch_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
epoch_compose_file="$epoch_repo_root/deploy/compose/docker-compose.consensus-probe.yml"
epoch_project_name="${EPOCH_TABLET_PROJECT_NAME:-epoch-tablet-smoke-$$}"
epoch_artifact_dir="${EPOCH_TABLET_ARTIFACT_DIR:-}"
epoch_use_existing_image="${EPOCH_TABLET_USE_EXISTING_IMAGE:-0}"
epoch_status_path=/experimental/v1/tablets/stream/status
epoch_records_path=/experimental/v1/tablets/stream/records
epoch_opaque_status_path=/experimental/v1/consensus/status
epoch_internal_peer_message_path=/internal/v1/consensus/messages
epoch_response_file=

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
export EPOCH_EXPERIMENTAL_STREAM_TABLET_ENABLED=true
export EPOCH_EXPERIMENTAL_STREAM_TABLET_NAME=orders

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
  if ((epoch_status != 0)); then
    if [[ -n "$epoch_artifact_dir" ]]; then
      mkdir -p "$epoch_artifact_dir"
      "${epoch_compose[@]}" logs --no-color >"$epoch_artifact_dir/containers.log" 2>&1 || true
      "${epoch_compose[@]}" ps --all >"$epoch_artifact_dir/containers.txt" 2>&1 || true
      env | awk '/^EPOCH_(PROBE|TABLET)_.*PORT_/' | sort >"$epoch_artifact_dir/ports.txt" || true
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
  if [[ -n "$epoch_response_file" ]]; then
    rm -f -- "$epoch_response_file"
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

tablet_status() {
  local node_id=$1
  curl --fail --silent --show-error \
    "http://127.0.0.1:${epoch_peer_ports[node_id - 1]}${epoch_status_path}"
}

tablet_records() {
  local node_id=$1
  curl --fail --silent --show-error \
    "http://127.0.0.1:${epoch_peer_ports[node_id - 1]}${epoch_records_path}?offset=0&limit=100"
}

assert_listener_boundaries() {
  local node_id=$1
  local public_port="${epoch_http_ports[node_id - 1]}"
  local peer_port="${epoch_peer_ports[node_id - 1]}"
  local health
  local path
  local status_code

  health="$(curl --fail --silent --show-error "http://127.0.0.1:${public_port}/healthz")"
  [[ "$(json_field "$health" guarantee_ceiling)" == local_durable ]]
  for path in "$epoch_internal_peer_message_path" "$epoch_status_path" "$epoch_records_path"; do
    status_code="$(curl --silent --output /dev/null --write-out '%{http_code}' \
      "http://127.0.0.1:${public_port}${path}")"
    if [[ "$status_code" != 404 ]]; then
      printf 'experimental path %s was exposed on node %s public port with status %s\n' \
        "$path" "$node_id" "$status_code" >&2
      return 1
    fi
  done

  status_code="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    "http://127.0.0.1:${peer_port}${epoch_opaque_status_path}")"
  if [[ "$status_code" != 404 ]]; then
    printf 'opaque proposal mode remained mounted on typed node %s with status %s\n' \
      "$node_id" "$status_code" >&2
    return 1
  fi
}

wait_for_nodes() {
  local ready
  local node_id
  for _ in {1..300}; do
    ready=0
    for node_id in 1 2 3; do
      if curl --fail --silent \
        "http://127.0.0.1:${epoch_http_ports[node_id - 1]}/healthz" >/dev/null 2>&1 \
        && tablet_status "$node_id" >/dev/null 2>&1; then
        ready=$((ready + 1))
      fi
    done
    if ((ready == 3)); then
      return 0
    fi
    sleep 0.1
  done
  printf 'typed Stream tablet nodes did not become ready\n' >&2
  return 1
}

wait_for_leader() {
  local excluded_node=${1:-0}
  local node_id
  local status
  for _ in {1..300}; do
    for node_id in 1 2 3; do
      if ((node_id == excluded_node)); then
        continue
      fi
      status="$(tablet_status "$node_id" 2>/dev/null || true)"
      if [[ -n "$status" ]] && [[ "$(json_field "$status" role)" == leader ]]; then
        printf '%s %s\n' "$node_id" "$(json_field "$status" term)"
        return 0
      fi
    done
    sleep 0.1
  done
  printf 'typed Stream tablet did not elect a leader\n' >&2
  return 1
}

append_record() {
  local node_id=$1
  local expected_term=$2
  local idempotency_key=$3
  local event_id=$4
  local payload_id=$5
  local output_file=$6
  local port="${epoch_peer_ports[node_id - 1]}"
  curl --silent --show-error \
    --connect-timeout 2 \
    --max-time 8 \
    --output "$output_file" \
    --write-out '%{http_code}' \
    --header 'content-type: application/json' \
    --data "{\"idempotency_key\":\"${idempotency_key}\",\"expected_term\":\"${expected_term}\",\"partition\":0,\"envelope\":{\"id\":\"${event_id}\",\"source\":\"integration\",\"type\":\"order.created\",\"time_ms\":\"1000\",\"payload\":{\"id\":${payload_id}}}}" \
    "http://127.0.0.1:${port}${epoch_records_path}"
}

assert_unresolved() {
  local document=$1
  python3 -c '
import json
import sys

document = json.loads(sys.argv[1])
assert document["state"] in {"unknown", "pending"}, document
assert document["outcome_certainty"] == "unknown", document
assert document["observation_scope"] == "local", document
assert document.get("receipt") is None, document
' "$document"
}

assert_committed_receipt() {
  local document=$1
  local expected_offset=$2
  local expected_disposition=$3
  python3 -c '
import json
import sys

document = json.loads(sys.argv[1])
receipt = document["receipt"]
assert document["state"] == "committed", document
assert document["outcome_certainty"] == "committed", document
assert receipt["offset"] == sys.argv[2], document
assert receipt["partition"] == 0, document
assert receipt["write_evidence"] == "fixed_voter_majority_persisted", document
assert receipt["durable_voter_acks"] == 2, document
assert isinstance(document["proposal_id"], str), document
assert receipt["proposal_id"] == document["proposal_id"], document
for field in ("tablet_id", "tablet_epoch", "term", "commit_index", "offset", "applied_at_ms"):
    assert isinstance(receipt[field], str), (field, document)
assert receipt["disposition"] == sys.argv[3], document
' "$document" "$expected_offset" "$expected_disposition"
}

assert_error() {
  local document=$1
  local expected_code=$2
  local expected_certainty=$3
  python3 -c '
import json
import sys

document = json.loads(sys.argv[1])
error = document["error"]
assert error["code"] == sys.argv[2], document
assert error["outcome_certainty"] == sys.argv[3], document
' "$document" "$expected_code" "$expected_certainty"
}

assert_json_equal() {
  local observed=$1
  local expected=$2
  python3 -c '
import json
import sys
assert json.loads(sys.argv[1]) == json.loads(sys.argv[2]), (sys.argv[1], sys.argv[2])
' "$observed" "$expected"
}

wait_for_record_count() {
  local expected=$1
  shift
  local nodes=("$@")
  local node_id
  local records
  local observed
  for _ in {1..300}; do
    observed=0
    for node_id in "${nodes[@]}"; do
      records="$(tablet_records "$node_id" 2>/dev/null || true)"
      if [[ -n "$records" ]] && python3 -c '
import json
import sys
raise SystemExit(0 if len(json.loads(sys.argv[1])["records"]) == int(sys.argv[2]) else 1)
' "$records" "$expected"; then
        observed=$((observed + 1))
      fi
    done
    if ((observed == ${#nodes[@]})); then
      return 0
    fi
    sleep 0.1
  done
  printf 'expected %s records on nodes %s\n' "$expected" "${nodes[*]}" >&2
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
  assert_listener_boundaries "$epoch_node_id"
  epoch_status="$(tablet_status "$epoch_node_id")"
  [[ "$(json_field "$epoch_status" stability)" == experimental ]]
  [[ "$(json_field "$epoch_status" production_readiness)" == not_production_ready ]]
  [[ "$(json_field "$epoch_status" write_guarantee)" == \
    fixed_three_voter_majority_persisted_then_local_profile_applied ]]
  [[ "$(json_field "$epoch_status" read_consistency)" == \
    local_profile_applied_stale_capable ]]
  [[ "$(json_field "$epoch_status" linearizable_read_barrier)" == False ]]
  python3 -c '
import json
import sys

status = json.loads(sys.argv[1])
for field in (
    "tablet_id", "tablet_epoch", "node_id", "term", "consensus_commit_index",
    "consensus_applied_index", "last_profile_mutation_index",
):
    assert isinstance(status[field], str), (field, status)
assert status["leader_id"] is None or isinstance(status["leader_id"], str), status
' "$epoch_status"
done

epoch_response_file="$(mktemp "${TMPDIR:-/tmp}/epoch-tablet-response.XXXXXX")"

read -r epoch_leader epoch_term < <(wait_for_leader)
epoch_follower=$((epoch_leader % 3 + 1))
epoch_status_code="$(append_record \
  "$epoch_follower" "$epoch_term" request-follower order-follower 0 "$epoch_response_file")"
[[ "$epoch_status_code" == 503 ]]
assert_error "$(<"$epoch_response_file")" not_leader unknown
wait_for_record_count 0 1 2 3

epoch_status_code="$(append_record "$epoch_leader" "$epoch_term" request-1 order-1 1 "$epoch_response_file")"
[[ "$epoch_status_code" == 201 ]]
epoch_response="$(<"$epoch_response_file")"
assert_committed_receipt "$epoch_response" 0 new
wait_for_record_count 1 1 2 3

epoch_status_code="$(append_record "$epoch_leader" "$epoch_term" request-1 order-1 1 "$epoch_response_file")"
[[ "$epoch_status_code" == 200 ]]
assert_committed_receipt "$(<"$epoch_response_file")" 0 replayed

epoch_status_code="$(append_record "$epoch_leader" "$epoch_term" request-1 order-1 99 "$epoch_response_file")"
[[ "$epoch_status_code" == 409 ]]

# Stop both followers before submitting a new command to the old leader. The
# request is bounded above by append_record's client timeout and must never
# claim a committed outcome while only one voter is running. Stop the old
# leader before restarting the majority so this minority-only entry cannot be
# committed by healing the original term.
epoch_survivors=()
epoch_survivor_services=()
for epoch_node_id in 1 2 3; do
  if ((epoch_node_id != epoch_leader)); then
    epoch_survivors+=("$epoch_node_id")
    epoch_survivor_services+=("${epoch_services[epoch_node_id - 1]}")
  fi
done
"${epoch_compose[@]}" stop --timeout 0 "${epoch_survivor_services[@]}" >/dev/null

epoch_status_code="$(append_record \
  "$epoch_leader" "$epoch_term" request-minority order-minority 2 "$epoch_response_file")"
case "$epoch_status_code" in
  202)
    assert_unresolved "$(<"$epoch_response_file")"
    ;;
  503)
    assert_error "$(<"$epoch_response_file")" not_leader unknown
    ;;
  *)
    printf 'isolated old leader returned unexpected status %s: %s\n' \
      "$epoch_status_code" "$(<"$epoch_response_file")" >&2
    exit 1
    ;;
esac
wait_for_record_count 1 "$epoch_leader"

epoch_old_leader_service="${epoch_services[epoch_leader - 1]}"
"${epoch_compose[@]}" stop --timeout 0 "$epoch_old_leader_service" >/dev/null
"${epoch_compose[@]}" start "${epoch_survivor_services[@]}" >/dev/null
read -r epoch_next_leader epoch_next_term < <(wait_for_leader "$epoch_leader")
if ((epoch_next_term <= epoch_term)); then
  printf 'leader term did not advance: %s -> %s\n' "$epoch_term" "$epoch_next_term" >&2
  exit 1
fi
# Rebind the same deterministic proposal ID to different semantic input on the
# replacement majority. The old leader's minority-only bytes must be
# overwritten safely, and a retry of the original input must conflict rather
# than being acknowledged with this different receipt.
epoch_status_code="$(append_record \
  "$epoch_next_leader" "$epoch_next_term" request-minority order-rebound 3 "$epoch_response_file")"
[[ "$epoch_status_code" == 201 ]]
assert_committed_receipt "$(<"$epoch_response_file")" 1 new
epoch_status_code="$(append_record \
  "$epoch_next_leader" "$epoch_next_term" request-minority order-minority 2 "$epoch_response_file")"
[[ "$epoch_status_code" == 409 ]]
assert_error "$(<"$epoch_response_file")" idempotency_conflict unknown

wait_for_record_count 2 "${epoch_survivors[@]}"

"${epoch_compose[@]}" start "$epoch_old_leader_service" >/dev/null
wait_for_nodes
wait_for_record_count 2 1 2 3

# Capture the complete typed state before the crash so deterministic but wrong
# replay on every voter cannot satisfy only a post-restart cross-node check.
epoch_pre_kill_records="$(tablet_records 1)"
epoch_pre_kill_digest="$(json_field "$(tablet_status 1)" state_digest)"
for epoch_node_id in 1 2 3; do
  assert_json_equal "$(tablet_records "$epoch_node_id")" "$epoch_pre_kill_records"
  [[ "$(json_field "$(tablet_status "$epoch_node_id")" state_digest)" == \
    "$epoch_pre_kill_digest" ]]
done

# Force every process down without graceful application shutdown. EPRS remains
# the only clustered source of truth; startup must rebuild the tablet before
# its typed status endpoint becomes ready.
"${epoch_compose[@]}" kill --signal SIGKILL >/dev/null
"${epoch_compose[@]}" start >/dev/null
wait_for_nodes
wait_for_record_count 2 1 2 3

epoch_reference_digest=
for epoch_node_id in 1 2 3; do
  epoch_status="$(tablet_status "$epoch_node_id")"
  [[ "$(json_field "$epoch_status" applied_command_count)" == 2 ]]
  epoch_digest="$(json_field "$epoch_status" state_digest)"
  [[ "$epoch_digest" == "$epoch_pre_kill_digest" ]]
  assert_json_equal "$(tablet_records "$epoch_node_id")" "$epoch_pre_kill_records"
  if [[ -z "$epoch_reference_digest" ]]; then
    epoch_reference_digest=$epoch_digest
  else
    [[ "$epoch_digest" == "$epoch_reference_digest" ]]
  fi
done

read -r epoch_reopened_leader epoch_reopened_term < <(wait_for_leader)
epoch_status_code="$(append_record "$epoch_reopened_leader" "$epoch_reopened_term" request-1 order-1 1 "$epoch_response_file")"
[[ "$epoch_status_code" == 200 ]]
assert_committed_receipt "$(<"$epoch_response_file")" 0 replayed
wait_for_record_count 2 1 2 3

rm -f -- "$epoch_response_file"
epoch_response_file=
printf 'Epoch typed Stream fixed-voter/minority/failover/SIGKILL replay smoke passed.\n'
