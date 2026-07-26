#!/usr/bin/env bash

set -Eeuo pipefail

epoch_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
epoch_compose_file="$epoch_repo_root/deploy/compose/docker-compose.consensus-probe.yml"
epoch_project_name="${EPOCH_BUS_TABLET_PROJECT_NAME:-epoch-bus-tablet-smoke-$$}"
epoch_artifact_dir="${EPOCH_BUS_TABLET_ARTIFACT_DIR:-}"
epoch_use_existing_image="${EPOCH_BUS_TABLET_USE_EXISTING_IMAGE:-0}"
epoch_status_path=/experimental/v1/tablets/bus/status
epoch_mutations_path=/experimental/v1/tablets/bus/mutations
epoch_replay_path=/experimental/v1/tablets/bus/archive/replay
epoch_delivery_query_path=/experimental/v1/tablets/bus/deliveries/query
epoch_opaque_status_path=/experimental/v1/consensus/status
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
export EPOCH_EXPERIMENTAL_QUEUE_TABLET_ENABLED=false
export EPOCH_EXPERIMENTAL_CACHE_TABLET_ENABLED=false
export EPOCH_EXPERIMENTAL_BUS_TABLET_ENABLED=true
export EPOCH_EXPERIMENTAL_BUS_TABLET_NAME=events

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
      env | awk '/^EPOCH_(PROBE|BUS_TABLET)_.*PORT_/' | sort \
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

archive_replay() {
  local node_id=$1
  curl --fail --silent --show-error \
    --header 'content-type: application/json' \
    --data '{"from_ms":"0","to_ms":"18446744073709551615","filter":{"event_type_patterns":["order.*"]},"limit":10}' \
    "http://127.0.0.1:${epoch_peer_ports[node_id - 1]}${epoch_replay_path}"
}

delivery_query() {
  local node_id=$1
  curl --fail --silent --show-error \
    --header 'content-type: application/json' \
    --data '{"limit":100}' \
    "http://127.0.0.1:${epoch_peer_ports[node_id - 1]}${epoch_delivery_query_path}"
}

submit_mutation() {
  local node_id=$1
  local body=$2
  local output_file=$3
  curl --silent --show-error \
    --connect-timeout 2 \
    --max-time 8 \
    --output "$output_file" \
    --write-out '%{http_code}' \
    --header 'content-type: application/json' \
    --data "$body" \
    "http://127.0.0.1:${epoch_peer_ports[node_id - 1]}${epoch_mutations_path}"
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
  printf 'typed Event Bus tablet nodes did not become ready\n' >&2
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
  printf 'typed Event Bus tablet did not elect a leader\n' >&2
  return 1
}

assert_error() {
  local document=$1
  local expected_code=$2
  python3 -c '
import json
import sys

document = json.loads(sys.argv[1])
assert document["error"]["code"] == sys.argv[2], document
assert document["error"]["outcome_certainty"] == "unknown", document
' "$document" "$expected_code"
}

assert_committed() {
  local document=$1
  local expected_kind=$2
  local expected_disposition=$3
  python3 -c '
import json
import sys

document = json.loads(sys.argv[1])
receipt = document["receipt"]
assert document["state"] == "committed", document
assert document["outcome_certainty"] == "committed", document
assert receipt["outcome"]["status"] == "applied", document
assert receipt["outcome"]["result"]["kind"] == sys.argv[2], document
assert receipt["disposition"] == sys.argv[3], document
assert receipt["write_evidence"] == "fixed_voter_majority_persisted", document
assert receipt["durable_voter_acks"] == 2, document
for field in (
    "proposal_id", "tablet_id", "tablet_epoch", "term", "commit_index",
    "applied_at_ms",
):
    assert isinstance(receipt[field], str), (field, document)
' "$document" "$expected_kind" "$expected_disposition"
}

wait_for_state() {
  local expected_events=$1
  local expected_acknowledged=$2
  local statuses=()
  local node_id
  local status
  for _ in {1..300}; do
    statuses=()
    for node_id in 1 2 3; do
      status="$(tablet_status "$node_id" 2>/dev/null || true)"
      if [[ -z "$status" ]]; then
        statuses=()
        break
      fi
      statuses+=("$status")
    done
    if [[ ${#statuses[@]} -eq 3 ]] && python3 - \
      "$expected_events" "$expected_acknowledged" "${statuses[@]}" <<'PYTHON'
import json
import sys

expected = int(sys.argv[1])
acknowledged = int(sys.argv[2])
statuses = [json.loads(value) for value in sys.argv[3:]]
digests = {status["state_digest"] for status in statuses}
valid = len(digests) == 1 and all(
    status["route_plan_version"] == "2"
    and status["subscription_count"] == "1"
    and status["commit_position"] == str(expected)
    and status["archived_event_count"] == str(expected)
    and status["target_dispatch"] == "external_executor_not_implemented"
    and status["durable_target_outbox"] is True
    and status["pending_delivery_count"] == str(expected - acknowledged)
    and status["in_flight_delivery_count"] == "0"
    and status["acknowledged_delivery_count"] == str(acknowledged)
    and status["dead_lettered_delivery_count"] == "0"
    and int(status["consensus_applied_index"]) >= int(status["last_profile_mutation_index"])
    for status in statuses
)
raise SystemExit(0 if valid else 1)
PYTHON
    then
      return 0
    fi
    sleep 0.1
  done
  printf 'Event Bus voters did not converge on %s archived events\n' "$expected_events" >&2
  return 1
}

assert_delivery_counts() {
  local expected_pending=$1
  local expected_acknowledged=$2
  local node_id
  local deliveries
  for node_id in 1 2 3; do
    deliveries="$(delivery_query "$node_id")"
    python3 -c '
import json
import sys

document = json.loads(sys.argv[1])
pending = int(sys.argv[2])
acknowledged = int(sys.argv[3])
assert document["observation_scope"] == "local", document
assert document["read_consistency"] == "local_profile_applied_stale_capable", document
records = document["records"]
states = [record["state"]["kind"] for record in records]
assert states.count("pending") == pending, document
assert states.count("acknowledged") == acknowledged, document
for record in records:
    assert isinstance(record["publish_position"], str), record
    assert isinstance(record["route_plan_version"], str), record
    assert isinstance(record["created_at_ms"], str), record
    assert isinstance(record["envelope"]["time_ms"], str), record
    for attempt in record["attempts"]:
        assert isinstance(attempt["dispatcher_epoch"], str), attempt
        assert isinstance(attempt["leader_term"], str), attempt
        assert isinstance(attempt["started_at_ms"], str), attempt
        assert isinstance(attempt["lease_deadline_ms"], str), attempt
' "$deliveries" "$expected_pending" "$expected_acknowledged"
  done
}

assert_archive_count() {
  local expected=$1
  local node_id
  local replay
  for node_id in 1 2 3; do
    replay="$(archive_replay "$node_id")"
    python3 -c '
import json
import sys

document = json.loads(sys.argv[1])
expected = int(sys.argv[2])
assert document["observation_scope"] == "local", document
assert document["read_consistency"] == "local_profile_applied_stale_capable", document
assert len(document["records"]) == expected, document
for index, record in enumerate(document["records"], start=1):
    assert record["position"] == str(index), record
    assert isinstance(record["received_at_ms"], str), record
    assert isinstance(record["route_plan_version"], str), record
    assert isinstance(record["envelope"]["time_ms"], str), record
' "$replay" "$expected"
  done
}

cd "$epoch_repo_root"
if [[ "$epoch_use_existing_image" == 1 ]]; then
  "${epoch_compose[@]}" up --no-build --detach
else
  "${epoch_compose[@]}" up --build --detach
fi
wait_for_nodes

for epoch_node_id in 1 2 3; do
  epoch_public_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    "http://127.0.0.1:${epoch_http_ports[epoch_node_id - 1]}${epoch_status_path}")"
  [[ "$epoch_public_status" == 404 ]]
  epoch_opaque_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    "http://127.0.0.1:${epoch_peer_ports[epoch_node_id - 1]}${epoch_opaque_status_path}")"
  [[ "$epoch_opaque_status" == 404 ]]
done

epoch_response_file="$(mktemp "${TMPDIR:-/tmp}/epoch-bus-tablet-response.XXXXXX")"
read -r epoch_leader epoch_term < <(wait_for_leader)
epoch_follower=$((epoch_leader % 3 + 1))

epoch_subscription_body="{\"idempotency_key\":\"subscription-1\",\"expected_term\":\"${epoch_term}\",\"operation\":{\"kind\":\"upsert_subscription\",\"subscription\":{\"name\":\"orders\",\"filter\":{\"event_type_patterns\":[\"order.*\"]},\"target\":{\"kind\":\"pull\"},\"transform\":{\"add_headers\":{\"x-epoch-route\":\"orders\"}}}}}"
epoch_status_code="$(submit_mutation \
  "$epoch_follower" "$epoch_subscription_body" "$epoch_response_file")"
[[ "$epoch_status_code" == 503 ]]
assert_error "$(<"$epoch_response_file")" not_leader

epoch_status_code="$(submit_mutation \
  "$epoch_leader" "$epoch_subscription_body" "$epoch_response_file")"
[[ "$epoch_status_code" == 201 ]]
assert_committed "$(<"$epoch_response_file")" subscription_upserted new

epoch_publish_one="{\"idempotency_key\":\"publish-1\",\"expected_term\":\"${epoch_term}\",\"operation\":{\"kind\":\"publish\",\"envelope\":{\"id\":\"event-1\",\"source\":\"integration\",\"type\":\"order.created\",\"time_ms\":\"9007199254740993\",\"payload\":{\"id\":1}}}}"
epoch_status_code="$(submit_mutation \
  "$epoch_leader" "$epoch_publish_one" "$epoch_response_file")"
[[ "$epoch_status_code" == 201 ]]
assert_committed "$(<"$epoch_response_file")" published new

epoch_status_code="$(submit_mutation \
  "$epoch_leader" "$epoch_publish_one" "$epoch_response_file")"
[[ "$epoch_status_code" == 200 ]]
assert_committed "$(<"$epoch_response_file")" published replayed

epoch_publish_conflict="${epoch_publish_one/\"id\":1/\"id\":99}"
epoch_status_code="$(submit_mutation \
  "$epoch_leader" "$epoch_publish_conflict" "$epoch_response_file")"
[[ "$epoch_status_code" == 409 ]]
assert_error "$(<"$epoch_response_file")" idempotency_conflict

epoch_acquire_one="{\"idempotency_key\":\"acquire-1\",\"expected_term\":\"${epoch_term}\",\"operation\":{\"kind\":\"acquire_deliveries\",\"subscription\":\"orders\",\"dispatcher\":\"sender\",\"dispatcher_epoch\":\"1\",\"max_deliveries\":1}}"
epoch_status_code="$(submit_mutation \
  "$epoch_leader" "$epoch_acquire_one" "$epoch_response_file")"
[[ "$epoch_status_code" == 201 ]]
assert_committed "$(<"$epoch_response_file")" deliveries_acquired new
epoch_delivery_id="$(python3 -c \
  'import json,sys; print(json.load(open(sys.argv[1]))["receipt"]["outcome"]["result"]["deliveries"][0]["delivery_id"])' \
  "$epoch_response_file")"
epoch_lease_token="$(python3 -c \
  'import json,sys; print(json.load(open(sys.argv[1]))["receipt"]["outcome"]["result"]["deliveries"][0]["lease_token"])' \
  "$epoch_response_file")"
[[ "$epoch_delivery_id" == epoch.bus.delivery.v1.1.orders ]]
epoch_ack_one="{\"idempotency_key\":\"ack-1\",\"expected_term\":\"${epoch_term}\",\"operation\":{\"kind\":\"acknowledge_delivery\",\"delivery_id\":\"${epoch_delivery_id}\",\"dispatcher\":\"sender\",\"dispatcher_epoch\":\"1\",\"lease_token\":\"${epoch_lease_token}\"}}"
epoch_status_code="$(submit_mutation \
  "$epoch_leader" "$epoch_ack_one" "$epoch_response_file")"
[[ "$epoch_status_code" == 201 ]]
assert_committed "$(<"$epoch_response_file")" delivery_acknowledged new

wait_for_state 1 1
assert_archive_count 1
assert_delivery_counts 0 1

"${epoch_compose[@]}" kill "${epoch_services[epoch_leader - 1]}" >/dev/null
read -r epoch_new_leader epoch_new_term < <(wait_for_leader "$epoch_leader")
epoch_publish_two="{\"idempotency_key\":\"publish-2\",\"expected_term\":\"${epoch_new_term}\",\"operation\":{\"kind\":\"publish\",\"envelope\":{\"id\":\"event-2\",\"source\":\"integration\",\"type\":\"order.updated\",\"time_ms\":\"9007199254740994\",\"payload\":{\"id\":2}}}}"
epoch_status_code="$(submit_mutation \
  "$epoch_new_leader" "$epoch_publish_two" "$epoch_response_file")"
[[ "$epoch_status_code" == 201 ]]
assert_committed "$(<"$epoch_response_file")" published new

"${epoch_compose[@]}" up --no-build --detach "${epoch_services[epoch_leader - 1]}" >/dev/null
wait_for_nodes
wait_for_state 2 1
assert_archive_count 2
assert_delivery_counts 1 1

"${epoch_compose[@]}" kill >/dev/null
"${epoch_compose[@]}" up --no-build --detach >/dev/null
wait_for_nodes
wait_for_state 2 1
assert_archive_count 2
assert_delivery_counts 1 1

printf 'Event Bus tablet integration passed (leader %s, failover leader %s)\n' \
  "$epoch_leader" "$epoch_new_leader"
