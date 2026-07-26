#!/usr/bin/env bash

set -Eeuo pipefail

epoch_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
epoch_compose_file="$epoch_repo_root/deploy/compose/docker-compose.consensus-probe.yml"
epoch_project_name="${EPOCH_CACHE_TABLET_PROJECT_NAME:-epoch-cache-tablet-smoke-$$}"
epoch_artifact_dir="${EPOCH_CACHE_TABLET_ARTIFACT_DIR:-}"
epoch_use_existing_image="${EPOCH_CACHE_TABLET_USE_EXISTING_IMAGE:-0}"
epoch_status_path=/experimental/v1/tablets/cache/status
epoch_mutations_path=/experimental/v1/tablets/cache/mutations
epoch_observations_path=/experimental/v1/tablets/cache/observations
epoch_stream_status_path=/experimental/v1/tablets/stream/status
epoch_queue_status_path=/experimental/v1/tablets/queue/status
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
export EPOCH_EXPERIMENTAL_QUEUE_TABLET_ENABLED=false
export EPOCH_EXPERIMENTAL_CACHE_TABLET_ENABLED=true
export EPOCH_EXPERIMENTAL_CACHE_TABLET_NAME=sessions

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
      env | awk '/^EPOCH_(PROBE|CACHE_TABLET)_.*PORT_/' | sort \
        >"$epoch_artifact_dir/ports.txt" || true
      if [[ -n "$epoch_request_file" && -f "$epoch_request_file" ]]; then
        cp "$epoch_request_file" "$epoch_artifact_dir/last-request.json" || true
      fi
      if [[ -n "$epoch_response_file" && -f "$epoch_response_file" ]]; then
        cp "$epoch_response_file" "$epoch_artifact_dir/last-response.json" || true
      fi
      for epoch_service in "${epoch_services[@]}"; do
        mkdir -p "$epoch_artifact_dir/state/$epoch_service"
        "${epoch_compose[@]}" cp \
          "$epoch_service:/var/lib/epoch/consensus/." \
          "$epoch_artifact_dir/state/$epoch_service" >/dev/null 2>&1 || true
      done
    else
      "${epoch_compose[@]}" logs --no-color --tail 300 >&2 || true
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

tablet_observation() {
  local node_id=$1
  local key=$2
  curl --fail --silent --show-error \
    --get \
    --data-urlencode "key=$key" \
    "http://127.0.0.1:${epoch_peer_ports[node_id - 1]}${epoch_observations_path}"
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
  printf 'typed Cache tablet nodes did not become ready\n' >&2
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
  printf 'typed Cache tablet did not elect a leader\n' >&2
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
  printf 'Cache mutation %s did not resolve\n' "$idempotency_key" >&2
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
receipt = document["receipt"]
assert document["state"] == "committed", document
assert document["outcome_certainty"] == "committed", document
assert document["observation_scope"] == "local", document
assert receipt["outcome"]["status"] == "applied", document
assert receipt["outcome"]["result"]["kind"] == sys.argv[2], document
assert receipt["write_evidence"] == "fixed_voter_majority_persisted", document
assert receipt["durable_voter_acks"] == 2, document
for field in (
    "proposal_id", "tablet_id", "tablet_epoch", "term", "commit_index",
    "applied_at_ms",
):
    assert isinstance(receipt[field], str), (field, document)
' "$document" "$expected_kind"
}

assert_rejected() {
  local document=$1
  local expected_code=$2
  python3 -c '
import json
import sys
document = json.loads(sys.argv[1])
receipt = document["receipt"]
assert document["state"] == "committed", document
assert document["outcome_certainty"] == "committed", document
assert receipt["outcome"]["status"] == "rejected", document
assert receipt["outcome"]["code"] == sys.argv[2], document
assert receipt["write_evidence"] == "fixed_voter_majority_persisted", document
assert receipt["durable_voter_acks"] == 2, document
' "$document" "$expected_code"
}

receipt_result_field() {
  local document=$1
  local field=$2
  python3 -c '
import json
import sys
print(json.loads(sys.argv[1])["receipt"]["outcome"]["result"][sys.argv[2]])
' "$document" "$field"
}

receipt_item_field() {
  local document=$1
  local field=$2
  python3 -c '
import json
import sys
print(json.loads(sys.argv[1])["receipt"]["outcome"]["result"]["item"][sys.argv[2]])
' "$document" "$field"
}

observation_revision() {
  local document=$1
  python3 -c 'import json,sys; print(json.loads(sys.argv[1])["observation"]["shard_revision"])' \
    "$document"
}

assert_observation_contract() {
  local document=$1
  python3 -c '
import json
import sys
document = json.loads(sys.argv[1])
assert document["observation_scope"] == "local", document
assert document["read_consistency"] == "local_profile_applied_stale_capable", document
assert document["linearizable_read_barrier"] is False, document
observation = document["observation"]
assert isinstance(observation["shard_revision"], str), document
assert isinstance(observation["observed_at_ms"], str), document
' "$document"
}

assert_item() {
  local document=$1
  local expected_kind=$2
  local expected_value=$3
  local expected_version=$4
  assert_observation_contract "$document"
  python3 -c '
import json
import sys
document = json.loads(sys.argv[1])
item = document["observation"]["item"]
assert item is not None, document
assert item["value"]["kind"] == sys.argv[2], document
assert item["value"]["value"] == sys.argv[3], document
assert item["version"] == sys.argv[4], document
assert item["expires_at_ms"] is None or isinstance(item["expires_at_ms"], str), document
' "$document" "$expected_kind" "$expected_value" "$expected_version"
}

assert_absent() {
  local document=$1
  assert_observation_contract "$document"
  python3 -c '
import json
import sys
document = json.loads(sys.argv[1])
assert document["observation"]["item"] is None, document
' "$document"
}

assert_status_contract() {
  local document=$1
  python3 -c '
import json
import sys
document = json.loads(sys.argv[1])
assert document["capability"] == "single_shard_cache_tablet", document
assert document["stability"] == "experimental", document
assert document["production_readiness"] == "not_production_ready", document
assert document["write_guarantee"] == "fixed_three_voter_majority_persisted_then_local_profile_applied", document
assert document["read_consistency"] == "local_profile_applied_stale_capable", document
assert document["linearizable_read_barrier"] is False, document
for field in (
    "tablet_id", "tablet_epoch", "node_id", "term", "consensus_commit_index",
    "consensus_applied_index", "last_profile_mutation_index",
    "last_applied_time_ms", "applied_command_count", "cache_revision",
    "retained_entry_count", "active_lock_count",
):
    assert isinstance(document[field], str), (field, document)
for field in ("cache_recovery_state_digest", "state_digest"):
    assert len(document[field]) == 64, (field, document)
    int(document[field], 16)
' "$document"
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
  for path in \
    "$epoch_internal_peer_message_path" \
    "$epoch_status_path" \
    "$epoch_mutations_path" \
    "$epoch_observations_path"; do
    status_code="$(curl --silent --output /dev/null --write-out '%{http_code}' \
      "http://127.0.0.1:${public_port}${path}")"
    [[ "$status_code" == 404 ]]
  done
  for path in "$epoch_opaque_status_path" "$epoch_stream_status_path" "$epoch_queue_status_path"; do
    status_code="$(curl --silent --output /dev/null --write-out '%{http_code}' \
      "http://127.0.0.1:${peer_port}${path}")"
    [[ "$status_code" == 404 ]]
  done
}

wait_for_convergence() {
  local expected_count=$1
  shift
  local nodes=("$@")
  local documents=()
  local node_id
  local status
  for _ in {1..300}; do
    documents=()
    for node_id in "${nodes[@]}"; do
      status="$(tablet_status "$node_id" 2>/dev/null || true)"
      if [[ -z "$status" ]]; then
        documents=()
        break
      fi
      documents+=("$status")
    done
    if ((${#documents[@]} == ${#nodes[@]})) \
      && python3 - "$expected_count" "${documents[@]}" <<'PYTHON'
import json
import sys

expected = sys.argv[1]
documents = [json.loads(value) for value in sys.argv[2:]]
assert documents
fields = (
    "last_profile_mutation_index",
    "last_applied_time_ms",
    "applied_command_count",
    "cache_revision",
    "retained_entry_count",
    "active_lock_count",
    "cache_recovery_state_digest",
    "state_digest",
)
reference = tuple(documents[0][field] for field in fields)
assert documents[0]["applied_command_count"] == expected
assert all(tuple(document[field] for field in fields) == reference for document in documents)
PYTHON
    then
      printf '%s\n' "$(json_field "${documents[0]}" state_digest)"
      return 0
    fi
    sleep 0.1
  done
  printf 'Cache tablet voters %s did not converge at applied count %s\n' \
    "${nodes[*]}" "$expected_count" >&2
  return 1
}

assert_same_observation() {
  local key=$1
  local expected=$2
  shift 2
  local node_id
  local observed
  for node_id in "$@"; do
    observed="$(tablet_observation "$node_id" "$key")"
    python3 -c '
import json
import sys
assert json.loads(sys.argv[1]) == json.loads(sys.argv[2]), (sys.argv[1], sys.argv[2])
' "$observed" "$expected"
  done
}

wait_until_deadline() {
  local deadline_ms=$1
  local maximum_wait_seconds=$2
  python3 - "$deadline_ms" "$maximum_wait_seconds" <<'PYTHON'
import sys
import time

deadline = int(sys.argv[1])
maximum = float(sys.argv[2])
delay = max(0.0, (deadline - time.time_ns() // 1_000_000 + 150) / 1000)
if delay > maximum:
    raise SystemExit(f"deadline wait {delay:.3f}s exceeds bound {maximum:.3f}s")
time.sleep(delay)
PYTHON
}

lock_operation() {
  local kind=$1
  local owner=$2
  local token=${3:-}
  local duration=${4:-}
  python3 - "$kind" "$owner" "$token" "$duration" <<'PYTHON'
import json
import sys

kind, owner, token, duration = sys.argv[1:]
operation = {
    "kind": kind,
    "lock_key": "critical",
    "owner": owner,
    "owner_epoch": "1",
}
if kind == "acquire_lock":
    operation["lease_ms"] = duration
elif kind == "renew_lock":
    operation["lease_token"] = token
    operation["extension_ms"] = duration
elif kind == "release_lock":
    operation["lease_token"] = token
else:
    raise SystemExit(f"unsupported lock operation: {kind}")
print(json.dumps(operation, separators=(",", ":")))
PYTHON
}

guarded_set_operation() {
  local value=$1
  local owner=$2
  local token=$3
  python3 - "$value" "$owner" "$token" <<'PYTHON'
import json
import sys

value, owner, token = sys.argv[1:]
print(json.dumps({
    "kind": "set",
    "key": "protected",
    "value": {"kind": "string", "value": value},
    "lock_guard": {
        "lock_key": "critical",
        "owner": owner,
        "owner_epoch": "1",
        "lease_token": token,
    },
}, separators=(",", ":")))
PYTHON
}

cd "$epoch_repo_root"
epoch_request_file="$(mktemp "${TMPDIR:-/tmp}/epoch-cache-request.XXXXXX")"
epoch_response_file="$(mktemp "${TMPDIR:-/tmp}/epoch-cache-response.XXXXXX")"
if [[ "$epoch_use_existing_image" == 1 ]]; then
  "${epoch_compose[@]}" up --no-build --detach
else
  "${epoch_compose[@]}" up --build --detach
fi
wait_for_nodes

for epoch_node_id in 1 2 3; do
  assert_listener_boundaries "$epoch_node_id"
  assert_status_contract "$(tablet_status "$epoch_node_id")"
  assert_observation_contract "$(tablet_observation "$epoch_node_id" not-present)"
done

read -r epoch_leader epoch_term < <(wait_for_leader)
epoch_follower=$((epoch_leader % 3 + 1))
epoch_code="$(mutate "$epoch_follower" "$epoch_term" follower-admission \
  '{"kind":"maintain","max_expirations":1}')"
[[ "$epoch_code" == 503 ]]
assert_error "$(<"$epoch_response_file")" not_leader unknown

epoch_stale_term=$((epoch_term - 1))
epoch_code="$(mutate "$epoch_leader" "$epoch_stale_term" stale-term-admission \
  '{"kind":"maintain","max_expirations":1}')"
[[ "$epoch_code" == 409 ]]
assert_error "$(<"$epoch_response_file")" stale_term unknown

epoch_code="$(mutate "$epoch_leader" "$epoch_term" invalid-schema \
  '{"kind":"set","key":"invalid","value":{"kind":"string","value":"bad"},"typo":true}')"
[[ "$epoch_code" == 422 ]]
assert_error "$(<"$epoch_response_file")" invalid_request definite_not_committed

epoch_set_operation='{"kind":"set","key":"profile","value":{"kind":"string","value":"v1"}}'
epoch_code="$(mutate_current_leader set-profile "$epoch_set_operation")"
[[ "$epoch_code" == 201 ]]
epoch_first_set_response="$(<"$epoch_response_file")"
assert_applied "$epoch_first_set_response" set
epoch_profile_version="$(receipt_item_field "$epoch_first_set_response" version)"
[[ "$epoch_profile_version" == 1 ]]
epoch_first_proposal_id="$(json_field "$epoch_first_set_response" proposal_id)"
epoch_first_lookup="$(tablet_get "$epoch_leader" \
  "$epoch_mutations_path/$epoch_first_proposal_id")"
python3 -c '
import json
import sys
original, lookup = map(json.loads, sys.argv[1:])
assert lookup["state"] == "committed", lookup
assert lookup["outcome_certainty"] == "committed", lookup
assert lookup["observation_scope"] == "local", lookup
assert lookup["proposal_id"] == original["proposal_id"], (original, lookup)
assert lookup["receipt"] == original["receipt"], (original, lookup)
' "$epoch_first_set_response" "$epoch_first_lookup"

epoch_code="$(mutate_current_leader set-profile "$epoch_set_operation")"
[[ "$epoch_code" == 200 ]]
epoch_set_replay="$(<"$epoch_response_file")"
assert_applied "$epoch_set_replay" set
python3 -c '
import json
import sys
original, replay = map(json.loads, sys.argv[1:])
assert replay["receipt"]["disposition"] == "replayed", replay
assert replay["receipt"]["proposal_id"] == original["receipt"]["proposal_id"]
assert replay["receipt"]["commit_index"] == original["receipt"]["commit_index"]
assert replay["receipt"]["outcome"] == original["receipt"]["outcome"]
' "$epoch_first_set_response" "$epoch_set_replay"

epoch_code="$(mutate_current_leader set-profile \
  '{"kind":"set","key":"profile","value":{"kind":"string","value":"changed"}}')"
[[ "$epoch_code" == 409 ]]
assert_error "$(<"$epoch_response_file")" idempotency_conflict unknown

epoch_cas_operation="$(printf \
  '{"kind":"compare_and_set","key":"profile","expected":{"kind":"version","version":"%s"},"value":{"kind":"string","value":"v2"}}' \
  "$epoch_profile_version")"
epoch_code="$(mutate_current_leader cas-profile "$epoch_cas_operation")"
[[ "$epoch_code" == 201 ]]
assert_applied "$(<"$epoch_response_file")" compared_and_set
epoch_profile_version="$(receipt_item_field "$(<"$epoch_response_file")" version)"
[[ "$epoch_profile_version" == 2 ]]

epoch_stale_cas='{"kind":"compare_and_set","key":"profile","expected":{"kind":"version","version":"1"},"value":{"kind":"string","value":"stale"}}'
epoch_code="$(mutate_current_leader cas-profile-stale "$epoch_stale_cas")"
[[ "$epoch_code" == 201 ]]
assert_rejected "$(<"$epoch_response_file")" conflict

read -r epoch_leader _ < <(wait_for_leader)
epoch_missing_observation="$(tablet_observation "$epoch_leader" aba)"
assert_absent "$epoch_missing_observation"
epoch_missing_revision="$(observation_revision "$epoch_missing_observation")"
[[ "$epoch_missing_revision" == 2 ]]
epoch_missing_cas="$(printf \
  '{"kind":"compare_and_set","key":"aba","expected":{"kind":"missing","shard_revision":"%s"},"value":{"kind":"string","value":"created"}}' \
  "$epoch_missing_revision")"
epoch_code="$(mutate_current_leader cas-missing-create "$epoch_missing_cas")"
[[ "$epoch_code" == 201 ]]
assert_applied "$(<"$epoch_response_file")" compared_and_set
epoch_aba_version="$(receipt_item_field "$(<"$epoch_response_file")" version)"
[[ "$epoch_aba_version" == 3 ]]

epoch_delete_aba="$(printf \
  '{"kind":"delete","key":"aba","expected_version":"%s"}' "$epoch_aba_version")"
epoch_code="$(mutate_current_leader delete-aba "$epoch_delete_aba")"
[[ "$epoch_code" == 201 ]]
assert_applied "$(<"$epoch_response_file")" deleted

epoch_code="$(mutate_current_leader cas-missing-stale "$epoch_missing_cas")"
[[ "$epoch_code" == 201 ]]
assert_rejected "$(<"$epoch_response_file")" conflict

epoch_transaction_ok='{"kind":"transaction","expected_revision":"4","mutations":[{"kind":"set","key":"name","value":{"kind":"string","value":"alice"}},{"kind":"increment","key":"visits","delta":"2","expected_version":"0"}]}'
epoch_code="$(mutate_current_leader transaction-ok "$epoch_transaction_ok")"
[[ "$epoch_code" == 201 ]]
epoch_transaction_response="$(<"$epoch_response_file")"
assert_applied "$epoch_transaction_response" transaction_committed
python3 -c '
import json
import sys
result = json.loads(sys.argv[1])["receipt"]["outcome"]["result"]
assert result["revision"] == "5", result
assert [entry["kind"] for entry in result["results"]] == ["set", "incremented"], result
assert result["results"][0]["item"]["version"] == "5", result
assert result["results"][1]["version"] == "5", result
assert result["results"][1]["value"] == "2", result
' "$epoch_transaction_response"

epoch_transaction_rollback='{"kind":"transaction","expected_revision":"5","mutations":[{"kind":"set","key":"must-not-exist","value":{"kind":"string","value":"bad"}},{"kind":"increment","key":"name","delta":"1","expected_version":"5"}]}'
epoch_code="$(mutate_current_leader transaction-rollback "$epoch_transaction_rollback")"
[[ "$epoch_code" == 201 ]]
assert_rejected "$(<"$epoch_response_file")" conflict
read -r epoch_leader _ < <(wait_for_leader)
assert_absent "$(tablet_observation "$epoch_leader" must-not-exist)"
assert_item "$(tablet_observation "$epoch_leader" name)" string alice 5

epoch_code="$(mutate_current_leader increment-visits \
  '{"kind":"increment","key":"visits","delta":"3","expected_version":"5"}')"
[[ "$epoch_code" == 201 ]]
assert_applied "$(<"$epoch_response_file")" incremented
[[ "$(receipt_result_field "$(<"$epoch_response_file")" value)" == 5 ]]
epoch_visits_version="$(receipt_result_field "$(<"$epoch_response_file")" version)"
[[ "$epoch_visits_version" == 6 ]]

epoch_code="$(mutate_current_leader set-ephemeral \
  '{"kind":"set","key":"ephemeral","value":{"kind":"string","value":"short"},"ttl_ms":"1000"}')"
[[ "$epoch_code" == 201 ]]
assert_applied "$(<"$epoch_response_file")" set
epoch_expiry_deadline="$(receipt_item_field "$(<"$epoch_response_file")" expires_at_ms)"
wait_until_deadline "$epoch_expiry_deadline" 3

read -r epoch_leader _ < <(wait_for_leader)
epoch_digest_before_observe="$(json_field "$(tablet_status "$epoch_leader")" state_digest)"
assert_item "$(tablet_observation "$epoch_leader" ephemeral)" string short 7
assert_item "$(tablet_observation "$epoch_leader" ephemeral)" string short 7
[[ "$(json_field "$(tablet_status "$epoch_leader")" state_digest)" == \
  "$epoch_digest_before_observe" ]]

epoch_code="$(mutate_current_leader maintain-ttl \
  '{"kind":"maintain","max_expirations":10}')"
[[ "$epoch_code" == 201 ]]
epoch_maintained_ttl="$(<"$epoch_response_file")"
assert_applied "$epoch_maintained_ttl" maintained
python3 -c '
import json
import sys
result = json.loads(sys.argv[1])["receipt"]["outcome"]["result"]
assert result["expired_keys"] == ["ephemeral"], result
assert result["expired_locks"] == [], result
assert result["cache_revision"] == "8", result
' "$epoch_maintained_ttl"
read -r epoch_leader _ < <(wait_for_leader)
assert_absent "$(tablet_observation "$epoch_leader" ephemeral)"

epoch_advance_operation="$(printf \
  '{"kind":"increment","key":"visits","delta":"1","expected_version":"%s"}' \
  "$epoch_visits_version")"
epoch_code="$(mutate_current_leader increment-visits-after-maintenance "$epoch_advance_operation")"
[[ "$epoch_code" == 201 ]]
assert_applied "$(<"$epoch_response_file")" incremented
[[ "$(receipt_result_field "$(<"$epoch_response_file")" value)" == 6 ]]

epoch_acquire_operation="$(lock_operation acquire_lock owner-a "" 4000)"
epoch_code="$(mutate_current_leader acquire-critical "$epoch_acquire_operation")"
[[ "$epoch_code" == 201 ]]
epoch_acquired="$(<"$epoch_response_file")"
assert_applied "$epoch_acquired" lock_acquired
epoch_old_token="$(receipt_result_field "$epoch_acquired" lease_token)"

epoch_renew_operation="$(lock_operation renew_lock owner-a "$epoch_old_token" 8000)"
epoch_code="$(mutate_current_leader renew-critical "$epoch_renew_operation")"
[[ "$epoch_code" == 201 ]]
epoch_renewed="$(<"$epoch_response_file")"
assert_applied "$epoch_renewed" lock_renewed
epoch_renewed_token="$(receipt_result_field "$epoch_renewed" lease_token)"
epoch_renewed_deadline="$(receipt_result_field "$epoch_renewed" lease_deadline_ms)"
[[ "$epoch_renewed_token" != "$epoch_old_token" ]]
python3 -c '
import json
import sys
acquired, renewed = map(json.loads, sys.argv[1:])
first = acquired["receipt"]["outcome"]["result"]
second = renewed["receipt"]["outcome"]["result"]
assert second["fencing_token"] == first["fencing_token"]
assert second["lease_generation"] == "2"
' "$epoch_acquired" "$epoch_renewed"

epoch_code="$(mutate_current_leader renew-critical "$epoch_renew_operation")"
[[ "$epoch_code" == 200 ]]
epoch_renewed_replay="$(<"$epoch_response_file")"
assert_applied "$epoch_renewed_replay" lock_renewed
[[ "$(receipt_result_field "$epoch_renewed_replay" lease_token)" == "$epoch_renewed_token" ]]
python3 -c '
import json,sys
document=json.loads(sys.argv[1])
assert document["receipt"]["disposition"] == "replayed", document
' "$epoch_renewed_replay"

epoch_guard_old="$(guarded_set_operation old-token owner-a "$epoch_old_token")"
epoch_code="$(mutate_current_leader guard-old-token "$epoch_guard_old")"
[[ "$epoch_code" == 201 ]]
assert_rejected "$(<"$epoch_response_file")" fenced

epoch_guard_current="$(guarded_set_operation before-failover owner-a "$epoch_renewed_token")"
epoch_code="$(mutate_current_leader guard-current-token "$epoch_guard_current")"
[[ "$epoch_code" == 201 ]]
assert_applied "$(<"$epoch_response_file")" set

read -r epoch_old_leader epoch_old_term < <(wait_for_leader)
epoch_old_service="${epoch_services[epoch_old_leader - 1]}"
"${epoch_compose[@]}" kill --signal SIGKILL "$epoch_old_service" >/dev/null
read -r _ epoch_new_term < <(wait_for_leader "$epoch_old_leader")
if ((epoch_new_term <= epoch_old_term)); then
  printf 'Cache leader term did not advance: %s -> %s\n' \
    "$epoch_old_term" "$epoch_new_term" >&2
  exit 1
fi

epoch_guard_old_term="$(guarded_set_operation old-term owner-a "$epoch_renewed_token")"
epoch_code="$(mutate_current_leader guard-old-term "$epoch_guard_old_term")"
[[ "$epoch_code" == 201 ]]
assert_rejected "$(<"$epoch_response_file")" fenced

epoch_contend_operation="$(lock_operation acquire_lock owner-b "" 4000)"
epoch_code="$(mutate_current_leader contend-before-deadline "$epoch_contend_operation")"
[[ "$epoch_code" == 201 ]]
assert_rejected "$(<"$epoch_response_file")" conflict

wait_until_deadline "$epoch_renewed_deadline" 10
epoch_code="$(mutate_current_leader maintain-expired-lock \
  '{"kind":"maintain","max_expirations":10}')"
[[ "$epoch_code" == 201 ]]
epoch_lock_maintenance="$(<"$epoch_response_file")"
assert_applied "$epoch_lock_maintenance" maintained
python3 -c '
import json
import sys
result = json.loads(sys.argv[1])["receipt"]["outcome"]["result"]
assert result["expired_keys"] == [], result
assert result["expired_locks"] == ["critical"], result
' "$epoch_lock_maintenance"

epoch_acquire_new_operation="$(lock_operation acquire_lock owner-b "" 4000)"
epoch_code="$(mutate_current_leader acquire-critical-new-owner "$epoch_acquire_new_operation")"
[[ "$epoch_code" == 201 ]]
epoch_acquired_new="$(<"$epoch_response_file")"
assert_applied "$epoch_acquired_new" lock_acquired
epoch_new_token="$(receipt_result_field "$epoch_acquired_new" lease_token)"
python3 -c '
import json
import sys
old, new = map(json.loads, sys.argv[1:])
old_fence = old["receipt"]["outcome"]["result"]["fencing_token"]
new_fence = new["receipt"]["outcome"]["result"]["fencing_token"]
assert (int(new_fence["tablet_epoch"]), int(new_fence["acquisition_index"])) > (
    int(old_fence["tablet_epoch"]), int(old_fence["acquisition_index"])
), (old_fence, new_fence)
' "$epoch_acquired" "$epoch_acquired_new"

epoch_guard_new="$(guarded_set_operation after-failover owner-b "$epoch_new_token")"
epoch_code="$(mutate_current_leader guard-new-owner "$epoch_guard_new")"
[[ "$epoch_code" == 201 ]]
epoch_guarded_after_failover="$(<"$epoch_response_file")"
assert_applied "$epoch_guarded_after_failover" set
epoch_protected_version="$(receipt_item_field "$epoch_guarded_after_failover" version)"
[[ "$epoch_protected_version" == 11 ]]

epoch_release_new="$(lock_operation release_lock owner-b "$epoch_new_token")"
epoch_code="$(mutate_current_leader release-critical-new-owner "$epoch_release_new")"
[[ "$epoch_code" == 201 ]]
assert_applied "$(<"$epoch_response_file")" lock_released

"${epoch_compose[@]}" start "$epoch_old_service" >/dev/null
wait_for_nodes
epoch_pre_kill_digest="$(wait_for_convergence 22 1 2 3)"
for epoch_node_id in 1 2 3; do
  assert_status_contract "$(tablet_status "$epoch_node_id")"
done

epoch_profile_observation="$(tablet_observation 1 profile)"
epoch_name_observation="$(tablet_observation 1 name)"
epoch_visits_observation="$(tablet_observation 1 visits)"
epoch_protected_observation="$(tablet_observation 1 protected)"
epoch_aba_observation="$(tablet_observation 1 aba)"
epoch_ephemeral_observation="$(tablet_observation 1 ephemeral)"
assert_item "$epoch_profile_observation" string v2 2
assert_item "$epoch_name_observation" string alice 5
assert_item "$epoch_visits_observation" counter 6 9
assert_item "$epoch_protected_observation" string after-failover 11
assert_absent "$epoch_aba_observation"
assert_absent "$epoch_ephemeral_observation"
for epoch_key in profile name visits protected aba ephemeral; do
  case "$epoch_key" in
    profile) epoch_reference="$epoch_profile_observation" ;;
    name) epoch_reference="$epoch_name_observation" ;;
    visits) epoch_reference="$epoch_visits_observation" ;;
    protected) epoch_reference="$epoch_protected_observation" ;;
    aba) epoch_reference="$epoch_aba_observation" ;;
    ephemeral) epoch_reference="$epoch_ephemeral_observation" ;;
  esac
  assert_same_observation "$epoch_key" "$epoch_reference" 1 2 3
done

epoch_pre_kill_status="$(tablet_status 1)"
[[ "$(json_field "$epoch_pre_kill_status" cache_revision)" == 11 ]]
[[ "$(json_field "$epoch_pre_kill_status" retained_entry_count)" == 4 ]]
[[ "$(json_field "$epoch_pre_kill_status" active_lock_count)" == 0 ]]

"${epoch_compose[@]}" kill --signal SIGKILL >/dev/null
"${epoch_compose[@]}" start >/dev/null
wait_for_nodes
epoch_recovered_digest="$(wait_for_convergence 22 1 2 3)"
[[ "$epoch_recovered_digest" == "$epoch_pre_kill_digest" ]]
for epoch_node_id in 1 2 3; do
  epoch_recovered_status="$(tablet_status "$epoch_node_id")"
  assert_status_contract "$epoch_recovered_status"
  [[ "$(json_field "$epoch_recovered_status" cache_revision)" == 11 ]]
  [[ "$(json_field "$epoch_recovered_status" retained_entry_count)" == 4 ]]
  [[ "$(json_field "$epoch_recovered_status" active_lock_count)" == 0 ]]
done
assert_same_observation profile "$epoch_profile_observation" 1 2 3
assert_same_observation name "$epoch_name_observation" 1 2 3
assert_same_observation visits "$epoch_visits_observation" 1 2 3
assert_same_observation protected "$epoch_protected_observation" 1 2 3
assert_same_observation aba "$epoch_aba_observation" 1 2 3
assert_same_observation ephemeral "$epoch_ephemeral_observation" 1 2 3

epoch_code="$(mutate_current_leader set-profile "$epoch_set_operation")"
[[ "$epoch_code" == 200 ]]
epoch_recovered_replay="$(<"$epoch_response_file")"
assert_applied "$epoch_recovered_replay" set
python3 -c '
import json
import sys
original, replay = map(json.loads, sys.argv[1:])
assert replay["receipt"]["disposition"] == "replayed", replay
for field in ("proposal_id", "term", "commit_index", "applied_at_ms", "outcome"):
    assert replay["receipt"][field] == original["receipt"][field], (field, original, replay)
' "$epoch_first_set_response" "$epoch_recovered_replay"

epoch_recovery_cas="$(printf \
  '{"kind":"compare_and_set","key":"protected","expected":{"kind":"version","version":"%s"},"value":{"kind":"string","value":"recovered"}}' \
  "$epoch_protected_version")"
epoch_code="$(mutate_current_leader cas-after-recovery "$epoch_recovery_cas")"
[[ "$epoch_code" == 201 ]]
assert_applied "$(<"$epoch_response_file")" compared_and_set

epoch_acquire_recovered="$(lock_operation acquire_lock owner-c "" 4000)"
epoch_code="$(mutate_current_leader acquire-after-recovery "$epoch_acquire_recovered")"
[[ "$epoch_code" == 201 ]]
epoch_recovered_lock="$(<"$epoch_response_file")"
assert_applied "$epoch_recovered_lock" lock_acquired
epoch_recovered_token="$(receipt_result_field "$epoch_recovered_lock" lease_token)"
python3 -c '
import json
import sys
previous, recovered = map(json.loads, sys.argv[1:])
old_fence = previous["receipt"]["outcome"]["result"]["fencing_token"]
new_fence = recovered["receipt"]["outcome"]["result"]["fencing_token"]
assert (int(new_fence["tablet_epoch"]), int(new_fence["acquisition_index"])) > (
    int(old_fence["tablet_epoch"]), int(old_fence["acquisition_index"])
), (old_fence, new_fence)
' "$epoch_acquired_new" "$epoch_recovered_lock"

epoch_release_recovered="$(lock_operation release_lock owner-c "$epoch_recovered_token")"
epoch_code="$(mutate_current_leader release-after-recovery "$epoch_release_recovered")"
[[ "$epoch_code" == 201 ]]
assert_applied "$(<"$epoch_response_file")" lock_released

wait_for_convergence 25 1 2 3 >/dev/null
for epoch_node_id in 1 2 3; do
  epoch_final_status="$(tablet_status "$epoch_node_id")"
  assert_status_contract "$epoch_final_status"
  [[ "$(json_field "$epoch_final_status" cache_revision)" == 12 ]]
  [[ "$(json_field "$epoch_final_status" retained_entry_count)" == 4 ]]
  [[ "$(json_field "$epoch_final_status" active_lock_count)" == 0 ]]
done
read -r epoch_leader _ < <(wait_for_leader)
epoch_final_protected="$(tablet_observation "$epoch_leader" protected)"
assert_item "$epoch_final_protected" string recovered 12
assert_same_observation protected "$epoch_final_protected" 1 2 3

rm -f -- "$epoch_request_file" "$epoch_response_file"
epoch_request_file=
epoch_response_file=
printf 'Epoch typed Cache CAS/transaction/TTL/fencing/failover/SIGKILL replay smoke passed.\n'
