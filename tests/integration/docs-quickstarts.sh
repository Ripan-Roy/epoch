#!/usr/bin/env bash

set -Eeuo pipefail

epoch_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
epoch_docs_tmp="$(mktemp -d "${TMPDIR:-/tmp}/epoch-docs-quickstarts.XXXXXX")"
epoch_node_url="${EPOCH_DOCS_NODE_URL:-http://127.0.0.1:7601}"
epoch_node_addr="${epoch_node_url#http://}"
epoch_target_dir="${EPOCH_DOCS_TARGET_DIR:-${epoch_repo_root}/target}"
epoch_node_pid=""
epoch_node_log=""

cleanup() {
  epoch_status=$?
  trap - EXIT INT TERM
  if [[ -n "$epoch_node_pid" ]]; then
    kill "$epoch_node_pid" 2>/dev/null || true
    wait "$epoch_node_pid" 2>/dev/null || true
  fi
  if (( epoch_status != 0 )); then
    printf 'Displayed SDK quickstart failed. Node logs:\n' >&2
    for epoch_log in "$epoch_docs_tmp"/node-*.log; do
      [[ -f "$epoch_log" ]] || continue
      printf '\n%s:\n' "$(basename "$epoch_log")" >&2
      tail -n 100 "$epoch_log" >&2 || true
    done
  fi
  rm -rf -- "$epoch_docs_tmp"
  exit "$epoch_status"
}
trap cleanup EXIT INT TERM

wait_for_health() {
  for _ in {1..150}; do
    if curl --fail --silent --show-error "$epoch_node_url/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  printf 'Epoch node did not become healthy at %s\n' "$epoch_node_url" >&2
  return 1
}

start_node() {
  local language=$1
  local phase=$2
  local data_dir="$epoch_docs_tmp/data-$language"
  epoch_node_log="$epoch_docs_tmp/node-$language-$phase.log"
  "$epoch_target_dir/debug/epoch-node" \
    --http-listen "$epoch_node_addr" \
    --data-dir "$data_dir" \
    --wal-segment-bytes 512 \
    --log warn >"$epoch_node_log" 2>&1 &
  epoch_node_pid=$!
  wait_for_health
}

crash_node() {
  kill -KILL "$epoch_node_pid"
  wait "$epoch_node_pid" 2>/dev/null || true
  epoch_node_pid=""
}

stop_node() {
  kill "$epoch_node_pid"
  wait "$epoch_node_pid" 2>/dev/null || true
  epoch_node_pid=""
}

run_example() {
  local language=$1
  local mode=$2
  case "$language" in
    go)
      EPOCH_URL="$epoch_node_url" \
        go run ./console/src/quickstarts/quickstart.go "$mode"
      ;;
    java)
      EPOCH_URL="$epoch_node_url" \
        java -cp "$epoch_java_classes:sdk/java/target/classes:$epoch_java_runtime_classpath" \
        Quickstart "$mode"
      ;;
    python)
      EPOCH_URL="$epoch_node_url" \
        "$epoch_docs_tmp/python/bin/python" console/src/quickstarts/quickstart.py "$mode"
      ;;
    *)
      printf 'Unknown quickstart language: %s\n' "$language" >&2
      return 1
      ;;
  esac
}

if curl --silent --show-error "$epoch_node_url/healthz" >/dev/null 2>&1; then
  printf 'Refusing to reuse an existing service at %s\n' "$epoch_node_url" >&2
  exit 1
fi

cd "$epoch_repo_root"
CARGO_TARGET_DIR="$epoch_target_dir" cargo build --locked -p epoch-node

python3 -m venv "$epoch_docs_tmp/python"
"$epoch_docs_tmp/python/bin/python" -m pip install \
  --disable-pip-version-check --no-deps --editable ./sdk/python

./sdk/java/mvnw --file sdk/java/pom.xml --batch-mode --no-transfer-progress \
  -DskipTests package dependency:build-classpath \
  -Dmdep.outputFile=target/docs-runtime-classpath.txt
epoch_java_runtime_classpath="$(cat sdk/java/target/docs-runtime-classpath.txt)"
epoch_java_classes="$epoch_docs_tmp/java-classes"
mkdir -p "$epoch_java_classes"
javac --release 25 -Xlint:all -Werror \
  -cp "sdk/java/target/classes:$epoch_java_runtime_classpath" \
  -d "$epoch_java_classes" \
  console/src/quickstarts/Quickstart.java

for epoch_language in go java python; do
  printf 'Executing displayed %s SDK lifecycle\n' "$epoch_language"
  start_node "$epoch_language" seed
  run_example "$epoch_language" seed
  crash_node
  start_node "$epoch_language" verify
  run_example "$epoch_language" verify
  stop_node
done

printf 'All displayed SDK quickstarts survived forced restart.\n'
