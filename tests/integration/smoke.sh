#!/usr/bin/env bash

set -Eeuo pipefail

epoch_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
epoch_smoke_tmp="$(mktemp -d "${TMPDIR:-/tmp}/epoch-smoke.XXXXXX")"
epoch_node_addr="${EPOCH_SMOKE_NODE_ADDR:-127.0.0.1:17651}"
epoch_control_addr="${EPOCH_SMOKE_CONTROL_ADDR:-127.0.0.1:18081}"
epoch_target_dir="${EPOCH_SMOKE_TARGET_DIR:-${epoch_repo_root}/target}"
epoch_node_pid=""
epoch_control_pid=""

cleanup() {
  epoch_status=$?
  trap - EXIT INT TERM
  if [[ -n "$epoch_node_pid" ]]; then
    kill "$epoch_node_pid" 2>/dev/null || true
    wait "$epoch_node_pid" 2>/dev/null || true
  fi
  if [[ -n "$epoch_control_pid" ]]; then
    kill "$epoch_control_pid" 2>/dev/null || true
    wait "$epoch_control_pid" 2>/dev/null || true
  fi
  if (( epoch_status != 0 )); then
    printf 'Epoch node log:\n' >&2
    tail -n 100 "$epoch_smoke_tmp/node.log" 2>/dev/null >&2 || true
    printf 'Epoch control-plane log:\n' >&2
    tail -n 100 "$epoch_smoke_tmp/control.log" 2>/dev/null >&2 || true
  fi
  rm -rf -- "$epoch_smoke_tmp"
  exit "$epoch_status"
}
trap cleanup EXIT INT TERM

wait_for_health() {
  local service_name=$1
  local health_url=$2
  for _ in {1..100}; do
    if curl --fail --silent --show-error "$health_url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  printf '%s did not become healthy at %s\n' "$service_name" "$health_url" >&2
  return 1
}

cd "$epoch_repo_root"
CARGO_TARGET_DIR="$epoch_target_dir" cargo build --locked -p epoch-node -p epoch-cli
go build -o "$epoch_smoke_tmp/epoch-control" ./control/cmd/epoch-control

"$epoch_target_dir/debug/epoch-node" \
  --http-listen "$epoch_node_addr" \
  --log warn >"$epoch_smoke_tmp/node.log" 2>&1 &
epoch_node_pid=$!

EPOCH_CONTROL_ADDR="$epoch_control_addr" \
  "$epoch_smoke_tmp/epoch-control" >"$epoch_smoke_tmp/control.log" 2>&1 &
epoch_control_pid=$!

wait_for_health "Epoch node" "http://${epoch_node_addr}/healthz"
wait_for_health "Epoch control plane" "http://${epoch_control_addr}/healthz"

"$epoch_target_dir/debug/epoch" \
  --url "http://${epoch_node_addr}" health >/dev/null

EPOCH_SMOKE_NODE_URL="http://${epoch_node_addr}" \
EPOCH_SMOKE_CONTROL_URL="http://${epoch_control_addr}" \
PYTHONPATH="$epoch_repo_root/sdk/python/src" \
python3 <<'PYTHON'
import json
import os
from urllib.request import Request, urlopen

from epoch_sdk import EpochClient, EventEnvelope, EventFilter, Subscription, SubscriptionTarget


client = EpochClient(os.environ["EPOCH_SMOKE_NODE_URL"])
health = client.health()
assert health["status"] == "ok"
assert health["guarantee_ceiling"] == "volatile"

client.create_cache("sessions", max_entries=100)
client.cache_set("sessions", "user-42", "active", only_if_absent=True)
assert client.cache_get("sessions", "user-42")["value"]["value"] == "active"

client.create_stream("orders", partitions=1)
stream_receipt = client.append_stream(
    "orders",
    EventEnvelope(
        id="order-1",
        source="checkout",
        event_type="order.created",
        payload={"order_id": "1"},
        key="customer-42",
    ),
)
assert stream_receipt["acknowledgement"]["durability"] == "volatile"
assert client.fetch_stream("orders")[0]["envelope"]["id"] == "order-1"
client.commit_stream_offset("orders", "billing", partition=0, next_offset=1)
assert client.stream_lag("orders", "billing")["lag"] == 0

client.create_queue("jobs", max_messages=100)
client.send(
    "jobs",
    EventEnvelope(
        id="job-1",
        source="api",
        event_type="job.requested",
        payload={"job_id": "1"},
    ),
)
delivery = client.receive("jobs", consumer="worker-1")[0]
client.acknowledge("jobs", delivery["lease_token"])
assert client.queue_counts("jobs")["acknowledged"] == 1

client.create_bus("events")
client.upsert_subscription(
    "events",
    Subscription(
        name="orders-to-jobs",
        target=SubscriptionTarget.queue("jobs"),
        filter=EventFilter(event_type_patterns=["order.*"]),
    ),
)
publish = client.publish(
    "events",
    EventEnvelope(
        id="order-2",
        source="checkout",
        event_type="order.created",
        payload={"order_id": "2"},
    ),
)
assert publish["routes"][0]["status"] == "delivered"
assert client.replay_bus("events", event_type="order.*")[0]["envelope"]["id"] == "order-2"
assert len(client.resources()) == 4

control_url = os.environ["EPOCH_SMOKE_CONTROL_URL"] + "/v1/resources"
body = json.dumps(
    {
        "request_token": "smoke-apply-1",
        "expected_generation": 0,
        "resource": {
            "namespace": "smoke",
            "kind": "stream",
            "name": "orders",
            "labels": {"environment": "test"},
            "spec": {"partitions": 1, "durability": "volatile"},
        },
    }
).encode()


def apply_control_resource() -> tuple[int, dict]:
    request = Request(
        control_url,
        data=body,
        method="PUT",
        headers={"content-type": "application/json"},
    )
    with urlopen(request, timeout=5) as response:
        return response.status, json.load(response)


status, created = apply_control_resource()
assert status == 201
assert created["created"] is True
status, replayed = apply_control_resource()
assert status == 200
assert replayed["replayed"] is True
assert replayed["resource"]["generation"] == 1
PYTHON

printf 'Epoch standalone cross-language smoke passed.\n'
