#!/usr/bin/env bash

set -Eeuo pipefail

epoch_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
epoch_compat_tmp="$(mktemp -d "${TMPDIR:-/tmp}/epoch-protocol-compat.XXXXXX")"
epoch_target_dir="${EPOCH_COMPAT_TARGET_DIR:-${epoch_repo_root}/target}"
epoch_fixture_log="${EPOCH_COMPAT_LOG:-${epoch_compat_tmp}/fixture.log}"
epoch_fixture_pid=""
epoch_redis_image="redis@sha256:2b42a93631132be6df7a31f843b91ea8a907011e955b03395b7edbb13a20a99d"
epoch_docker_host="${EPOCH_COMPAT_DOCKER_HOST:-host.docker.internal}"
epoch_docker_network_args=(--add-host host.docker.internal:host-gateway)

# A Linux container's host-gateway address cannot reach a process bound only to
# host loopback. Keep the fixture private and put only the disposable Redis CLI
# container in the host network. Docker Desktop continues to use its supported
# host.docker.internal mapping.
if [[ "$(uname -s)" == Linux ]]; then
  epoch_docker_host="${EPOCH_COMPAT_DOCKER_HOST:-127.0.0.1}"
  epoch_docker_network_args=(--network host)
fi

cleanup() {
  epoch_status=$?
  trap - EXIT INT TERM
  if [[ -n "$epoch_fixture_pid" ]]; then
    kill "$epoch_fixture_pid" 2>/dev/null || true
    wait "$epoch_fixture_pid" 2>/dev/null || true
  fi
  if (( epoch_status != 0 )) && [[ -f "$epoch_fixture_log" ]]; then
    printf 'Protocol compatibility fixture logs:\n' >&2
    tail -n 200 "$epoch_fixture_log" >&2 || true
  fi
  rm -rf -- "$epoch_compat_tmp"
  exit "$epoch_status"
}
trap cleanup EXIT INT TERM

reserve_ports() {
  python3 - <<'PY'
import socket

sockets = []
try:
    for _ in range(3):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        sockets.append(listener)
    print(*(listener.getsockname()[1] for listener in sockets))
finally:
    for listener in sockets:
        listener.close()
PY
}

wait_for_port() {
  local port=$1
  for _ in {1..200}; do
    if python3 - "$port" <<'PY'
import socket
import sys

with socket.socket() as connection:
    connection.settimeout(0.1)
    raise SystemExit(connection.connect_ex(("127.0.0.1", int(sys.argv[1]))) != 0)
PY
    then
      return 0
    fi
    sleep 0.1
  done
  printf 'Compatibility listener did not open port %s\n' "$port" >&2
  return 1
}

redis_cli() {
  docker run --rm \
    "${epoch_docker_network_args[@]}" \
    "$epoch_redis_image" \
    redis-cli --raw --no-auth-warning \
    -h "$epoch_docker_host" \
    -p "$epoch_redis_port" \
    -a compat-secret \
    "$@"
}

assert_equal() {
  local expected=$1
  local actual=$2
  local evidence=$3
  if [[ "$actual" != "$expected" ]]; then
    printf '%s: expected <%s>, got <%s>\n' "$evidence" "$expected" "$actual" >&2
    return 1
  fi
}

read -r epoch_redis_port epoch_kafka_port epoch_amqp_port <<<"$(reserve_ports)"

cd "$epoch_repo_root"
CARGO_TARGET_DIR="$epoch_target_dir" \
  cargo build --locked -p epoch-compat --features test-support --bin epoch-compat-fixture

"$epoch_target_dir/debug/epoch-compat-fixture" \
  --redis-listen "127.0.0.1:${epoch_redis_port}" \
  --kafka-listen "127.0.0.1:${epoch_kafka_port}" \
  --amqp-listen "127.0.0.1:${epoch_amqp_port}" \
  --kafka-advertised-host 127.0.0.1 >"$epoch_fixture_log" 2>&1 &
epoch_fixture_pid=$!

wait_for_port "$epoch_redis_port"
wait_for_port "$epoch_kafka_port"
wait_for_port "$epoch_amqp_port"

assert_equal "redis-cli 8.8.2" "$(docker run --rm "$epoch_redis_image" redis-cli --version)" \
  "Redis client version"
assert_equal PONG "$(redis_cli PING)" "Redis PING"
assert_equal OK "$(redis_cli SET text-key original PX 60000)" "Redis SET with TTL"
assert_equal original "$(redis_cli GET text-key)" "Redis GET"
assert_equal "" "$(redis_cli SET text-key replacement NX)" "Redis SET NX"
assert_equal 1 "$(redis_cli INCR counter)" "Redis atomic counter"
assert_equal $'one\ntwo' "$(redis_cli MSET first one second two >/dev/null && redis_cli MGET first second)" \
  "Redis multi-key round trip"

epoch_ttl="$(redis_cli PTTL text-key)"
if (( epoch_ttl <= 0 || epoch_ttl > 60000 )); then
  printf 'Redis PTTL returned an invalid value: %s\n' "$epoch_ttl" >&2
  exit 1
fi

printf 'binary\0value' | docker run --rm --interactive \
  "${epoch_docker_network_args[@]}" \
  "$epoch_redis_image" \
  redis-cli --raw --no-auth-warning \
  -h "$epoch_docker_host" \
  -p "$epoch_redis_port" \
  -a compat-secret \
  -x SET binary-key >"$epoch_compat_tmp/redis-binary-set.out"
assert_equal OK "$(tr -d '\n' <"$epoch_compat_tmp/redis-binary-set.out")" "Redis binary SET"
redis_cli GET binary-key >"$epoch_compat_tmp/redis-binary-get.out"
python3 - "$epoch_compat_tmp/redis-binary-get.out" <<'PY'
from pathlib import Path
import sys

actual = Path(sys.argv[1]).read_bytes()
if actual != b"binary\x00value\n":
    raise SystemExit(f"Redis binary GET mismatch: {actual!r}")
PY

assert_equal PONG "$(docker run --rm \
  "${epoch_docker_network_args[@]}" \
  "$epoch_redis_image" \
  redis-cli -3 --raw --no-auth-warning \
  -h "$epoch_docker_host" \
  -p "$epoch_redis_port" \
  -a compat-secret PING)" "Redis RESP3 negotiation"

./scripts/retry-command.sh 3 2 \
  ./sdk/java/mvnw --file tests/compatibility/java/pom.xml \
  --batch-mode --no-transfer-progress dependency:build-classpath \
  -Dmdep.outputFile=target/runtime-classpath.txt
epoch_java_classpath="$(cat tests/compatibility/java/target/runtime-classpath.txt)"
epoch_java_classes="$epoch_compat_tmp/java-classes"
mkdir -p "$epoch_java_classes"
javac --release 17 -Xlint:all -Werror \
  -cp "$epoch_java_classpath" \
  -d "$epoch_java_classes" \
  tests/compatibility/java/ProtocolConformance.java
java -cp "$epoch_java_classes:$epoch_java_classpath" \
  ProtocolConformance 127.0.0.1 "$epoch_kafka_port" "$epoch_amqp_port"

printf 'Redis CLI 8.8.2, Kafka Java 4.3.1, and RabbitMQ Java 5.34.0 conformance passed.\n'
