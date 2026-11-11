#!/usr/bin/env bash

set -Eeuo pipefail

epoch_container_id=

cleanup_owned_container() {
  local epoch_status=$?
  trap - EXIT
  if [[ -n "$epoch_container_id" ]]; then
    docker stop --timeout 10 "$epoch_container_id" >/dev/null 2>&1 || true
    docker rm "$epoch_container_id" >/dev/null 2>&1 || true
  fi
  exit "$epoch_status"
}
trap cleanup_owned_container EXIT

start_container() {
  local epoch_started_id
  # --rm deletion may outlive `docker stop`. Retain this exact container until
  # explicit, synchronous removal has released its name; keep the volume intact.
  epoch_started_id="$(docker run --detach --name epoch-node-ci-smoke \
    --publish 127.0.0.1:17661:7601 \
    --volume epoch-node-ci-data:/var/lib/epoch \
    --env EPOCH_WAL_SEGMENT_BYTES=512 \
    epoch/node:ci)"
  if [[ ! "$epoch_started_id" =~ ^[0-9a-f]{64}$ ]]; then
    printf 'Docker did not return an exact owned smoke container ID\n' >&2
    return 1
  fi
  epoch_container_id=$epoch_started_id
}

stop_and_remove_container() {
  docker stop --timeout 10 "$epoch_container_id" >/dev/null
  docker rm "$epoch_container_id" >/dev/null
  epoch_container_id=
}

wait_for_health() {
  for _ in {1..100}; do
    if curl --fail --silent --show-error http://127.0.0.1:17661/healthz >/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  docker logs "$epoch_container_id"
  return 1
}

wal_segment_count() {
  docker exec "$epoch_container_id" sh -c \
    'set -- /var/lib/epoch/engine-wal/segment-*.wal; if [ ! -e "$1" ]; then echo 0; else echo "$#"; fi'
}

assert_wal_rotated() {
  local minimum_count=$1
  local segment_count
  segment_count="$(wal_segment_count)" || return "$?"
  if [[ "$segment_count" -lt "$minimum_count" ]]; then
    printf 'expected at least %s WAL segments, found %s\n' "$minimum_count" "$segment_count" >&2
    docker exec "$epoch_container_id" \
      find /var/lib/epoch/engine-wal -maxdepth 1 -type f -print >&2 || true
    return 1
  fi
  printf '%s\n' "$segment_count"
}

docker volume create epoch-node-ci-data >/dev/null
start_container
wait_for_health
curl --fail --silent --show-error --output /dev/null \
  --header 'content-type: application/json' \
  --data '{"partitions":1,"durability":"local_durable","max_records_per_partition":null}' \
  http://127.0.0.1:17661/v1/streams/audit
curl --fail --silent --show-error --output /dev/null \
  --header 'content-type: application/json' \
  --data '{"envelope":{"id":"container-1","source":"ci","type":"audit.created","time_ms":1,"payload":{"ok":true}}}' \
  http://127.0.0.1:17661/v1/streams/audit/records
curl --fail --silent --show-error --output /dev/null \
  --header 'content-type: application/json' \
  --data '{"durability":"local_durable","visibility_timeout_ms":30000,"max_messages":100,"retry":{"strategy":"fixed","initial_delay_ms":1000,"max_delay_ms":1000,"jitter_percent":0,"max_attempts":3,"max_age_ms":null},"dedupe_window_ms":null}' \
  http://127.0.0.1:17661/v1/queues/jobs
curl --fail --silent --show-error --output /dev/null \
  --header 'content-type: application/json' \
  --data '{"id":"container-job-1","source":"ci","type":"job.created","time_ms":1,"payload":{"ok":true}}' \
  http://127.0.0.1:17661/v1/queues/jobs/messages
segments_before_restart="$(assert_wal_rotated 2)"
stop_and_remove_container

start_container
wait_for_health
records="$(curl --fail --silent --show-error \
  'http://127.0.0.1:17661/v1/streams/audit/records?partition=0&offset=0&limit=10')"
grep -q '"id":"container-1"' <<<"$records"
queue_counts="$(curl --fail --silent --show-error \
  'http://127.0.0.1:17661/v1/queues/jobs/counts')"
grep -q '"ready":1' <<<"$queue_counts"
segments_after_restart="$(assert_wal_rotated 2)"
test "$segments_after_restart" -ge "$segments_before_restart"
stop_and_remove_container
