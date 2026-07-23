#!/usr/bin/env bash

set -Eeuo pipefail

epoch_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
epoch_compose_file="$epoch_repo_root/deploy/compose/docker-compose.consensus-probe.yml"
epoch_project_name="${EPOCH_QUEUE_TABLET_PROJECT_NAME:-epoch-queue-tablet-smoke-$$}"
epoch_artifact_dir="${EPOCH_QUEUE_TABLET_ARTIFACT_DIR:-}"
epoch_use_existing_image="${EPOCH_QUEUE_TABLET_USE_EXISTING_IMAGE:-0}"
epoch_status_path=/experimental/v1/tablets/queue/status
epoch_mutations_path=/experimental/v1/tablets/queue/mutations
epoch_counts_path=/experimental/v1/tablets/queue/counts
epoch_dead_letters_path=/experimental/v1/tablets/queue/dead-letters
epoch_redrives_path=/experimental/v1/tablets/queue/redrives
epoch_stream_status_path=/experimental/v1/tablets/stream/status
epoch_opaque_status_path=/experimental/v1/consensus/status
epoch_internal_peer_message_path=/internal/v1/consensus/messages
epoch_request_file=
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
export EPOCH_EXPERIMENTAL_STREAM_TABLET_ENABLED=false
export EPOCH_EXPERIMENTAL_QUEUE_TABLET_ENABLED=true
export EPOCH_EXPERIMENTAL_QUEUE_TABLET_NAME=jobs

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
      env | awk '/^EPOCH_(PROBE|QUEUE_TABLET)_.*PORT_/' | sort \
        >"$epoch_artifact_dir/ports.txt" || true
      for epoch_service in "${epoch_services[@]}"; do
        mkdir -p "$epoch_artifact_dir/state/$epoch_service"
        "${epoch_compose[@]}" cp \
          "$epoch_service:/var/lib/epoch/consensus/." \
          "$epoch_artifact_dir/state/$epoch_service" >/dev/null 2>&1 || true
      done
    else
      "${epoch_compose[@]}" logs --no-color --tail 250 >&2 || true
    fi
  fi
  if [[ -n "$epoch_request_file" ]]; then
    rm -f -- "$epoch_request_file"
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

tablet_get() {
  local node_id=$1
  local path=$2
  curl --fail --silent --show-error \
    "http://127.0.0.1:${epoch_peer_ports[node_id - 1]}${path}"
}

tablet_status() {
  tablet_get "$1" "$epoch_status_path"
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
  printf 'typed Queue tablet nodes did not become ready\n' >&2
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
  printf 'typed Queue tablet did not elect a leader\n' >&2
  return 1
}

mutate() {
  local node_id=$1
  local expected_term=$2
  local idempotency_key=$3
  local operation=$4
  python3 -c '
import json
import sys
json.dump({
    "idempotency_key": sys.argv[1],
    "expected_term": sys.argv[2],
    "operation": json.loads(sys.argv[3]),
}, sys.stdout, separators=(",", ":"))
' "$idempotency_key" "$expected_term" "$operation" >"$epoch_request_file"
  curl --silent --show-error \
    --connect-timeout 2 \
    --max-time 8 \
    --output "$epoch_response_file" \
    --write-out '%{http_code}' \
    --header 'content-type: application/json' \
    --data-binary "@$epoch_request_file" \
    "http://127.0.0.1:${epoch_peer_ports[node_id - 1]}${epoch_mutations_path}"
}

retryable_response() {
  local status_code=$1
  local document=$2
  python3 -c '
import json
import sys

status = int(sys.argv[1])
document = json.loads(sys.argv[2])
retryable = (
    (status == 503 and document.get("error", {}).get("code") == "not_leader")
    or (status == 409 and document.get("error", {}).get("code") == "stale_term")
    or (status == 202 and document.get("outcome_certainty") == "unknown")
)
raise SystemExit(0 if retryable else 1)
' "$status_code" "$document"
}

mutate_current_leader() {
  local idempotency_key=$1
  local operation=$2
  local leader
  local term
  local status_code
  for _ in {1..100}; do
    read -r leader term < <(wait_for_leader)
    status_code="$(mutate "$leader" "$term" "$idempotency_key" "$operation")"
    if retryable_response "$status_code" "$(<"$epoch_response_file")"; then
      sleep 0.1
      continue
    fi
    printf '%s\n' "$status_code"
    return 0
  done
  printf 'Queue mutation %s did not resolve\n' "$idempotency_key" >&2
  return 1
}

assert_error() {
  local document=$1
  local expected_code=$2
  local expected_certainty=$3
  python3 -c '
import json
import sys
document = json.loads(sys.argv[1])
assert document["error"]["code"] == sys.argv[2], document
assert document["error"]["outcome_certainty"] == sys.argv[3], document
' "$document" "$expected_code" "$expected_certainty"
}

assert_applied() {
  local document=$1
  local expected_kind=$2
  python3 -c '
import json
import sys
document = json.loads(sys.argv[1])
assert document["state"] == "committed", document
assert document["outcome_certainty"] == "committed", document
assert document["receipt"]["outcome"]["status"] == "applied", document
assert document["receipt"]["outcome"]["result"]["kind"] == sys.argv[2], document
assert document["receipt"]["write_evidence"] == "fixed_voter_majority_persisted", document
assert document["receipt"]["durable_voter_acks"] == 2, document
' "$document" "$expected_kind"
}

operation_value() {
  local document=$1
  local field=$2
  python3 -c '
import json
import sys
print(json.loads(sys.argv[1])["receipt"]["outcome"]["result"][sys.argv[2]])
' "$document" "$field"
}

delivery_value() {
  local document=$1
  local field=$2
  python3 -c '
import json
import sys
print(json.loads(sys.argv[1])["receipt"]["outcome"]["result"]["deliveries"][0][sys.argv[2]])
' "$document" "$field"
}

assert_listener_boundaries() {
  local node_id=$1
  local public_port="${epoch_http_ports[node_id - 1]}"
  local peer_port="${epoch_peer_ports[node_id - 1]}"
  local path
  local status_code
  curl --fail --silent --show-error "http://127.0.0.1:${public_port}/healthz" >/dev/null
  for path in "$epoch_internal_peer_message_path" "$epoch_status_path" "$epoch_mutations_path"; do
    status_code="$(curl --silent --output /dev/null --write-out '%{http_code}' \
      "http://127.0.0.1:${public_port}${path}")"
    [[ "$status_code" == 404 ]]
  done
  for path in "$epoch_opaque_status_path" "$epoch_stream_status_path"; do
    status_code="$(curl --silent --output /dev/null --write-out '%{http_code}' \
      "http://127.0.0.1:${peer_port}${path}")"
    [[ "$status_code" == 404 ]]
  done
}

wait_for_applied_count() {
  local expected=$1
  local observed
  local node_id
  local status
  for _ in {1..300}; do
    observed=0
    for node_id in 1 2 3; do
      status="$(tablet_status "$node_id" 2>/dev/null || true)"
      if [[ -n "$status" ]] && [[ "$(json_field "$status" applied_command_count)" == "$expected" ]]; then
        observed=$((observed + 1))
      fi
    done
    if ((observed == 3)); then
      return 0
    fi
    sleep 0.1
  done
  printf 'expected %s applied Queue commands on every voter\n' "$expected" >&2
  return 1
}

assert_cluster_state() {
  local expected_digest=$1
  local node_id
  local status
  local counts
  local dead_letters
  local redrives
  for node_id in 1 2 3; do
    status="$(tablet_status "$node_id")"
    [[ "$(json_field "$status" state_digest)" == "$expected_digest" ]]
    [[ "$(json_field "$status" applied_command_count)" == 14 ]]
    counts="$(tablet_get "$node_id" "$epoch_counts_path")"
    dead_letters="$(tablet_get "$node_id" "$epoch_dead_letters_path")"
    redrives="$(tablet_get "$node_id" "$epoch_redrives_path")"
    python3 -c '
import json
import sys
counts, dead_letters, redrives = map(json.loads, sys.argv[1:])
assert counts["counts"]["acknowledged"] == "2", counts
assert counts["counts"]["dead_lettered"] == "0", counts
assert len(dead_letters["records"]) == 1, dead_letters
assert dead_letters["records"][0]["dead_letter"]["message_id"] == "job-dlq", dead_letters
assert len(redrives["records"]) == 1, redrives
assert redrives["records"][0]["message_id"] == "job-dlq", redrives
' "$counts" "$dead_letters" "$redrives"
  done
}

cd "$epoch_repo_root"
epoch_request_file="$(mktemp "${TMPDIR:-/tmp}/epoch-queue-request.XXXXXX")"
epoch_response_file="$(mktemp "${TMPDIR:-/tmp}/epoch-queue-response.XXXXXX")"
if [[ "$epoch_use_existing_image" == 1 ]]; then
  "${epoch_compose[@]}" up --no-build --detach
else
  "${epoch_compose[@]}" up --build --detach
fi
wait_for_nodes

for epoch_node_id in 1 2 3; do
  assert_listener_boundaries "$epoch_node_id"
  epoch_status="$(tablet_status "$epoch_node_id")"
  [[ "$(json_field "$epoch_status" capability)" == single_partition_queue_tablet ]]
  [[ "$(json_field "$epoch_status" stability)" == experimental ]]
  [[ "$(json_field "$epoch_status" production_readiness)" == not_production_ready ]]
done

read -r epoch_leader epoch_term < <(wait_for_leader)
epoch_follower=$((epoch_leader % 3 + 1))
epoch_code="$(mutate "$epoch_follower" "$epoch_term" follower-1 '{"kind":"maintain"}')"
[[ "$epoch_code" == 503 ]]
assert_error "$(<"$epoch_response_file")" not_leader unknown

epoch_code="$(mutate "$epoch_leader" "$epoch_term" invalid-1 \
  '{"kind":"enqueue","envelope":{"id":"invalid","source":"integration","type":"job.created","time_ms":"1","payload":{},"paylod":"typo"}}')"
[[ "$epoch_code" == 422 ]]
assert_error "$(<"$epoch_response_file")" invalid_request definite_not_committed

epoch_now_ms="$(python3 -c 'import time; print(time.time_ns() // 1_000_000)')"
epoch_eligible_ms=$((epoch_now_ms + 1500))
epoch_schedule_operation="$(printf \
  '{"kind":"enqueue","envelope":{"id":"job-scheduled","source":"integration","type":"job.created","time_ms":"%s","deliver_at_ms":"%s","payload":{"id":1}}}' \
  "$epoch_now_ms" "$epoch_eligible_ms")"
epoch_code="$(mutate_current_leader enqueue-scheduled "$epoch_schedule_operation")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
assert_applied "$(<"$epoch_response_file")" enqueued

epoch_empty_acquire='{"kind":"acquire","consumer":"worker-a","consumer_epoch":"1","max_messages":1,"visibility_timeout_ms":"1200"}'
epoch_code="$(mutate_current_leader acquire-empty "$epoch_empty_acquire")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
assert_applied "$(<"$epoch_response_file")" acquired
[[ "$(operation_value "$(<"$epoch_response_file")" deliveries)" == '[]' ]]

python3 -c '
import sys
import time
deadline = int(sys.argv[1])
delay = max(0.0, (deadline - time.time_ns() // 1_000_000 + 150) / 1000)
time.sleep(delay)
' "$epoch_eligible_ms"
epoch_code="$(mutate_current_leader acquire-scheduled "$epoch_empty_acquire")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
assert_applied "$(<"$epoch_response_file")" acquired
epoch_old_token="$(delivery_value "$(<"$epoch_response_file")" lease_token)"
epoch_old_deadline="$(delivery_value "$(<"$epoch_response_file")" lease_deadline_ms)"

read -r epoch_old_leader epoch_old_term < <(wait_for_leader)
epoch_old_service="${epoch_services[epoch_old_leader - 1]}"
"${epoch_compose[@]}" kill --signal SIGKILL "$epoch_old_service" >/dev/null
read -r epoch_new_leader epoch_new_term < <(wait_for_leader "$epoch_old_leader")
if ((epoch_new_leader == epoch_old_leader)); then
  printf 'Queue leader replacement did not move to a surviving voter\n' >&2
  exit 1
fi
if ((epoch_new_term <= epoch_old_term)); then
  printf 'Queue leader term did not advance: %s -> %s\n' "$epoch_old_term" "$epoch_new_term" >&2
  exit 1
fi

epoch_old_ack="$(printf \
  '{"kind":"acknowledge","consumer":"worker-a","consumer_epoch":"1","lease_token":"%s"}' \
  "$epoch_old_token")"
epoch_code="$(mutate_current_leader ack-old-term "$epoch_old_ack")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
python3 -c '
import json
import sys
document = json.loads(sys.argv[1])
assert document["outcome_certainty"] == "committed", document
assert document["receipt"]["outcome"]["status"] == "rejected", document
assert document["receipt"]["outcome"]["code"] == "fenced", document
' "$(<"$epoch_response_file")"

python3 -c '
import sys
import time
deadline = int(sys.argv[1])
delay = max(0.0, (deadline - time.time_ns() // 1_000_000 + 150) / 1000)
time.sleep(delay)
' "$epoch_old_deadline"
epoch_code="$(mutate_current_leader maintain-expired '{"kind":"maintain"}')"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
assert_applied "$(<"$epoch_response_file")" maintained
sleep 1.3
epoch_code="$(mutate_current_leader acquire-redelivery "$epoch_empty_acquire")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
epoch_redelivery_token="$(delivery_value "$(<"$epoch_response_file")" lease_token)"
epoch_ack_redelivery="$(printf \
  '{"kind":"acknowledge","consumer":"worker-a","consumer_epoch":"1","lease_token":"%s"}' \
  "$epoch_redelivery_token")"
epoch_code="$(mutate_current_leader ack-redelivery "$epoch_ack_redelivery")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
assert_applied "$(<"$epoch_response_file")" acknowledged

"${epoch_compose[@]}" start "$epoch_old_service" >/dev/null
wait_for_nodes

epoch_dlq_enqueue='{"kind":"enqueue","envelope":{"id":"job-dlq","source":"integration","type":"job.created","time_ms":"1","payload":{"id":2}}}'
epoch_code="$(mutate_current_leader enqueue-dlq "$epoch_dlq_enqueue")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
epoch_dlq_acquire='{"kind":"acquire","consumer":"worker-b","consumer_epoch":"1","max_messages":1,"visibility_timeout_ms":"3000"}'
epoch_code="$(mutate_current_leader acquire-dlq "$epoch_dlq_acquire")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
epoch_dlq_token="$(delivery_value "$(<"$epoch_response_file")" lease_token)"
epoch_extend_operation="$(printf \
  '{"kind":"extend_lease","consumer":"worker-b","consumer_epoch":"1","lease_token":"%s","extension_ms":"5000"}' \
  "$epoch_dlq_token")"
epoch_code="$(mutate_current_leader extend-dlq "$epoch_extend_operation")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
assert_applied "$(<"$epoch_response_file")" lease_extended
epoch_renewed_token="$(operation_value "$(<"$epoch_response_file")" lease_token)"
epoch_first_renewal="$(<"$epoch_response_file")"
epoch_code="$(mutate_current_leader extend-dlq "$epoch_extend_operation")"
[[ "$epoch_code" == 200 ]]
[[ "$(operation_value "$(<"$epoch_response_file")" lease_token)" == "$epoch_renewed_token" ]]
[[ "$(json_field "$(<"$epoch_response_file")" proposal_id)" == \
  "$(json_field "$epoch_first_renewal" proposal_id)" ]]

epoch_reject_operation="$(printf \
  '{"kind":"reject","consumer":"worker-b","consumer_epoch":"1","lease_token":"%s","reason":"poison"}' \
  "$epoch_renewed_token")"
epoch_code="$(mutate_current_leader reject-dlq "$epoch_reject_operation")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
assert_applied "$(<"$epoch_response_file")" dead_lettered
epoch_history_id="$(operation_value "$(<"$epoch_response_file")" dead_letter_history_id)"
epoch_redrive_operation="$(printf \
  '{"kind":"redrive","message_id":"job-dlq","dead_letter_history_id":"%s"}' \
  "$epoch_history_id")"
epoch_code="$(mutate_current_leader redrive-dlq "$epoch_redrive_operation")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
assert_applied "$(<"$epoch_response_file")" redriven
epoch_code="$(mutate_current_leader acquire-redriven "$epoch_dlq_acquire")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
epoch_redriven_token="$(delivery_value "$(<"$epoch_response_file")" lease_token)"
epoch_ack_dlq="$(printf \
  '{"kind":"acknowledge","consumer":"worker-b","consumer_epoch":"1","lease_token":"%s"}' \
  "$epoch_redriven_token")"
epoch_code="$(mutate_current_leader ack-redriven "$epoch_ack_dlq")"
[[ "$epoch_code" == 201 || "$epoch_code" == 200 ]]
assert_applied "$(<"$epoch_response_file")" acknowledged

wait_for_applied_count 14
epoch_pre_kill_digest="$(json_field "$(tablet_status 1)" state_digest)"
assert_cluster_state "$epoch_pre_kill_digest"

"${epoch_compose[@]}" kill --signal SIGKILL >/dev/null
"${epoch_compose[@]}" start >/dev/null
wait_for_nodes
wait_for_applied_count 14
assert_cluster_state "$epoch_pre_kill_digest"

printf 'Epoch typed Queue lease/failover/DLQ/redrive/SIGKILL replay smoke passed.\n'
