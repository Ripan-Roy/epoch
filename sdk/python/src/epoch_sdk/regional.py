"""Authenticated leader- and fence-aware regional Stream client."""

from __future__ import annotations

import base64
import gzip
import json
from dataclasses import dataclass
from typing import Any, Literal

from ._regional import (
    RegionalClient,
    RegionalScope,
    Route,
    _non_negative,
    _positive,
    _required,
    _segment,
)
from .models import EventEnvelope

_MAX_FETCH_RECORDS = 1_000
_MAX_RETENTION_RECORDS = 100_000
_MAX_RETENTION_BYTES = 3 * 1024 * 1024
_MAX_RETENTION_AGE_MS = 10 * 365 * 24 * 60 * 60 * 1_000
_MIN_SESSION_TIMEOUT_MS = 1_000
_MAX_SESSION_TIMEOUT_MS = 300_000
_MAX_CLAIM_TRANSITIONS = 4_096
_STREAM_PARTITIONER = "fnv1a64_utf8_mod_n_v1"
_MAX_BATCH_RECORDS = 1_000
_MAX_BATCH_COMPRESSED_BYTES = 360 * 1024
_MAX_BATCH_UNCOMPRESSED_BYTES = 4 * 1024 * 1024
_MIN_CAPTURE_INTERVAL_MS = 1_000
_MAX_CAPTURE_INTERVAL_MS = 31 * 24 * 60 * 60 * 1_000

StreamCompression = Literal["none", "gzip", "lz4", "snappy", "zstd"]
_STREAM_COMPRESSIONS = frozenset({"none", "gzip", "lz4", "snappy", "zstd"})
StreamReadIsolation = Literal["read_committed", "read_uncommitted"]
StreamConsumerMode = Literal["push", "dedicated"]
StreamCaptureFormat = Literal["json_lines", "json_array"]


@dataclass(frozen=True, slots=True)
class StreamBatchRecord:
    """One batch record correlated by an unsigned 32-bit client sequence."""

    client_sequence: int
    envelope: EventEnvelope

    def __post_init__(self) -> None:
        if (
            isinstance(self.client_sequence, bool)
            or not isinstance(self.client_sequence, int)
            or not 0 <= self.client_sequence <= 0xFFFF_FFFF
        ):
            raise ValueError("Stream batch client sequence must fit an unsigned 32-bit integer")
        if not isinstance(self.envelope, EventEnvelope):
            raise TypeError("Stream batch envelope must be an EventEnvelope")


@dataclass(frozen=True, slots=True)
class StreamBatchFrame:
    """One bounded client-framed atomic Stream batch."""

    compression: StreamCompression
    record_count: int
    uncompressed_bytes: int
    compressed: bytes

    def __post_init__(self) -> None:
        if self.compression not in _STREAM_COMPRESSIONS:
            raise ValueError(f"unsupported Stream batch compression: {self.compression}")
        if isinstance(self.record_count, bool) or not 1 <= self.record_count <= _MAX_BATCH_RECORDS:
            raise ValueError(
                f"Stream batch record count must be between 1 and {_MAX_BATCH_RECORDS}"
            )
        if (
            isinstance(self.uncompressed_bytes, bool)
            or not 1 <= self.uncompressed_bytes <= _MAX_BATCH_UNCOMPRESSED_BYTES
        ):
            raise ValueError(
                "Stream batch uncompressed bytes must be between 1 and "
                f"{_MAX_BATCH_UNCOMPRESSED_BYTES}"
            )
        if not isinstance(self.compressed, bytes):
            raise TypeError("Stream batch compressed payload must be bytes")
        if not 1 <= len(self.compressed) <= _MAX_BATCH_COMPRESSED_BYTES:
            raise ValueError(
                f"Stream batch compressed bytes must be between 1 and {_MAX_BATCH_COMPRESSED_BYTES}"
            )
        if self.compression == "none" and len(self.compressed) != self.uncompressed_bytes:
            raise ValueError("uncompressed Stream batch frame sizes must match")

    @classmethod
    def from_compressed(
        cls,
        compression: StreamCompression,
        record_count: int,
        uncompressed_bytes: int,
        compressed: bytes,
    ) -> StreamBatchFrame:
        """Wrap exact standard LZ4, Snappy, Zstd, gzip, or uncompressed bytes."""

        return cls(compression, record_count, uncompressed_bytes, compressed)

    @classmethod
    def encode(
        cls, records: list[StreamBatchRecord], compression: StreamCompression = "none"
    ) -> StreamBatchFrame:
        """Encode canonical record JSON using the built-in none or gzip path."""

        if not isinstance(records, list) or not 1 <= len(records) <= _MAX_BATCH_RECORDS:
            raise ValueError(
                f"Stream batch must contain between 1 and {_MAX_BATCH_RECORDS} records"
            )
        sequences: set[int] = set()
        canonical: list[dict[str, Any]] = []
        for record in records:
            if not isinstance(record, StreamBatchRecord):
                raise TypeError("Stream batch records must be StreamBatchRecord values")
            if record.client_sequence in sequences:
                raise ValueError(f"duplicate Stream batch client sequence {record.client_sequence}")
            sequences.add(record.client_sequence)
            canonical.append(
                {
                    "client_sequence": record.client_sequence,
                    "envelope": _canonical_stream_envelope(record.envelope),
                }
            )
        plain = json.dumps(
            canonical,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=False,
            allow_nan=False,
        ).encode()
        if len(plain) > _MAX_BATCH_UNCOMPRESSED_BYTES:
            raise ValueError(
                f"Stream batch uncompressed bytes must not exceed {_MAX_BATCH_UNCOMPRESSED_BYTES}"
            )
        if compression == "none":
            compressed = plain
        elif compression == "gzip":
            compressed = gzip.compress(plain, mtime=0)
        elif compression in {"lz4", "snappy", "zstd"}:
            raise ValueError(
                f"{compression} Stream batches require a caller-supplied standard frame"
            )
        else:
            raise ValueError(f"unsupported Stream batch compression: {compression}")
        return cls(compression, len(records), len(plain), compressed)

    def to_wire(self) -> dict[str, Any]:
        return {
            "compression": self.compression,
            "record_count": self.record_count,
            "uncompressed_bytes": self.uncompressed_bytes,
            "compressed_bytes": len(self.compressed),
            "payload_base64": base64.b64encode(self.compressed).decode("ascii"),
        }


def _canonical_stream_envelope(event: EventEnvelope) -> dict[str, Any]:
    value: dict[str, Any] = {
        "id": event.id,
        "source": event.source,
        "type": event.event_type,
    }
    if event.subject is not None:
        value["subject"] = event.subject
    value["time_ms"] = event.time_ms
    if event.key is not None:
        value["key"] = event.key
    value["headers"] = _sort_json_objects(event.headers)
    value["content_type"] = event.content_type
    if event.schema_ref is not None:
        value["schema_ref"] = event.schema_ref
    if event.traceparent is not None:
        value["traceparent"] = event.traceparent
    value["payload"] = _sort_json_objects(event.payload)
    if event.deliver_at_ms is not None:
        value["deliver_at_ms"] = event.deliver_at_ms
    if event.ttl_ms is not None:
        value["ttl_ms"] = event.ttl_ms
    value["priority"] = event.priority
    if event.dedupe_id is not None:
        value["dedupe_id"] = event.dedupe_id
    if event.transaction_id is not None:
        value["transaction_id"] = event.transaction_id
    value["extensions"] = _sort_json_objects(event.extensions)
    return value


def _sort_json_objects(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sort_json_objects(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_sort_json_objects(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class StreamRetentionPolicy:
    """Independent per-partition record, canonical-byte, and age bounds."""

    max_records_per_partition: int | None = None
    max_bytes_per_partition: int | None = None
    max_age_ms: int | None = None

    def __post_init__(self) -> None:
        _optional_bound(
            self.max_records_per_partition,
            "Stream retention max records",
            _MAX_RETENTION_RECORDS,
        )
        _optional_bound(
            self.max_bytes_per_partition,
            "Stream retention max bytes",
            _MAX_RETENTION_BYTES,
        )
        _optional_bound(
            self.max_age_ms,
            "Stream retention max age",
            _MAX_RETENTION_AGE_MS,
        )

    def to_wire(self) -> dict[str, Any]:
        document: dict[str, Any] = {}
        if self.max_records_per_partition is not None:
            document["max_records_per_partition"] = self.max_records_per_partition
        if self.max_bytes_per_partition is not None:
            document["max_bytes_per_partition"] = str(self.max_bytes_per_partition)
        if self.max_age_ms is not None:
            document["max_age_ms"] = str(self.max_age_ms)
        return document


@dataclass(frozen=True, slots=True)
class StreamOffsetCommit:
    """One consumer offset committed atomically with a Stream transaction."""

    group: str
    partition: int
    next_offset: int

    def __post_init__(self) -> None:
        _required(self.group, "consumer group")
        _non_negative(self.partition, "partition")
        _non_negative(self.next_offset, "next offset")


@dataclass(frozen=True, slots=True)
class StreamReplicationRecord:
    """One source-positioned record and its loop-prevention path."""

    source_offset: int
    envelope: EventEnvelope
    traversed_clusters: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _non_negative(self.source_offset, "source offset")
        if not isinstance(self.envelope, EventEnvelope):
            raise TypeError("replication envelope must be an EventEnvelope")
        for cluster in self.traversed_clusters:
            _required(cluster, "traversed cluster")


@dataclass(frozen=True, slots=True)
class StreamReplicationBatch:
    """One bounded contiguous cross-cluster replication batch."""

    source_cluster: str
    source_stream: str
    source_partition: int
    first_source_offset: int
    records: tuple[StreamReplicationRecord, ...]

    def __post_init__(self) -> None:
        _required(self.source_cluster, "source cluster")
        _required(self.source_stream, "source stream")
        _non_negative(self.source_partition, "source partition")
        _non_negative(self.first_source_offset, "first source offset")
        if not 1 <= len(self.records) <= 128:
            raise ValueError("replication batch must contain between 1 and 128 records")
        if any(not isinstance(record, StreamReplicationRecord) for record in self.records):
            raise TypeError("replication records must be StreamReplicationRecord values")


@dataclass(frozen=True, slots=True)
class StreamSuperstreamMember:
    """One named physical Stream shard in a logical superstream merge."""

    name: str
    stream: str
    shard: int
    offset: int = 0

    def __post_init__(self) -> None:
        _required(self.name, "superstream member name")
        _required(self.stream, "superstream member Stream")
        _non_negative(self.shard, "superstream member shard")
        _non_negative(self.offset, "superstream member offset")


class RegionalStreamClient(RegionalClient):
    """Synchronous regional Stream client with explicit mutation identity."""

    def append(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        event: EventEnvelope,
    ) -> dict[str, Any]:
        _required(idempotency_key, "idempotency key")
        return self._stream_call(
            stream,
            shard,
            lambda route: (
                "POST",
                "/records",
                {
                    "idempotency_key": idempotency_key,
                    "expected_term": route.term,
                    "partition": 0,
                    "envelope": event.to_dict(),
                },
                None,
                {},
            ),
        )

    def append_batch(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        frame: StreamBatchFrame,
    ) -> dict[str, Any]:
        """Atomically append one caller-framed batch to a single Stream shard."""

        _required(idempotency_key, "idempotency key")
        if not isinstance(frame, StreamBatchFrame):
            raise TypeError("frame must be a StreamBatchFrame")
        return self._stream_call(
            stream,
            shard,
            lambda route: (
                "POST",
                "/records/batches",
                {
                    "idempotency_key": idempotency_key,
                    "expected_term": route.term,
                    "partition": 0,
                    **frame.to_wire(),
                },
                None,
                {},
            ),
        )

    def append_keyed(
        self,
        stream: str,
        idempotency_key: str,
        event: EventEnvelope,
    ) -> dict[str, Any]:
        """Discover routing and append by event key, falling back to event ID."""
        _required(idempotency_key, "idempotency key")
        if not isinstance(event, EventEnvelope):
            raise TypeError("event must be an EventEnvelope")
        routing = self.discover_route("streams", "Stream", stream, 0)
        partitioning = routing.stream_partitioning
        if (
            partitioning is None
            or partitioning.algorithm != _STREAM_PARTITIONER
            or partitioning.key_encoding != "utf8"
            or partitioning.missing_key_fallback != "event_id"
            or partitioning.shard_count <= 0
        ):
            raise ValueError("regional Stream partitioning metadata is unsupported or incomplete")
        shard = stream_shard_for(event.key or event.id, partitioning.shard_count)
        return self.call_at_generation(
            "streams",
            "Stream",
            stream,
            shard,
            routing.resource_generation,
            lambda route: (
                "POST",
                "/records",
                {
                    "idempotency_key": idempotency_key,
                    "expected_term": route.term,
                    "partition": 0,
                    "envelope": event.to_dict(),
                },
                None,
                {},
            ),
        )

    def fetch(
        self,
        stream: str,
        shard: int,
        offset: int,
        *,
        limit: int = 100,
        isolation: StreamReadIsolation = "read_committed",
    ) -> dict[str, Any]:
        _non_negative(offset, "offset")
        _fetch_limit(limit)
        if isolation not in {"read_committed", "read_uncommitted"}:
            raise ValueError(f"unsupported Stream read isolation: {isolation}")
        return self._stream_call(
            stream,
            shard,
            lambda _route: (
                "GET",
                "/records",
                None,
                {"offset": offset, "limit": limit, "isolation": isolation},
                {"x-epoch-read-consistency": "linearizable"},
            ),
        )

    def fetch_superstream(
        self,
        members: list[StreamSuperstreamMember] | tuple[StreamSuperstreamMember, ...],
        *,
        limit: int = 100,
        isolation: StreamReadIsolation = "read_committed",
    ) -> dict[str, Any]:
        """Merge named shards deterministically without claiming an atomic cross-shard snapshot."""
        if not 1 <= len(members) <= 128:
            raise ValueError("superstream must contain between 1 and 128 members")
        _fetch_limit(limit)
        names: set[str] = set()
        for member in members:
            if not isinstance(member, StreamSuperstreamMember):
                raise TypeError("superstream members must be StreamSuperstreamMember values")
            if member.name in names:
                raise ValueError(f"duplicate superstream member: {member.name}")
            names.add(member.name)
        merged: list[tuple[int, str, int, int, dict[str, Any]]] = []
        for member in members:
            response = self.fetch(
                member.stream,
                member.shard,
                member.offset,
                limit=limit,
                isolation=isolation,
            )
            records = response.get("records") if isinstance(response, dict) else None
            if not isinstance(records, list):
                raise ValueError(f"superstream member {member.name} response omitted records")
            for record in records:
                if not isinstance(record, dict):
                    raise ValueError(f"superstream member {member.name} returned an invalid record")
                appended_at = _stream_response_u64(record.get("appended_at_ms"), "appended_at_ms")
                partition = _stream_response_u64(record.get("partition"), "partition")
                offset = _stream_response_u64(record.get("offset"), "offset")
                decorated = dict(record)
                decorated["member"] = member.name
                merged.append((appended_at, member.name, partition, offset, decorated))
        merged.sort(key=lambda item: item[:4])
        return {
            "records": [item[4] for item in merged[:limit]],
            "member_count": len(members),
            "ordering": "appended_at_member_partition_offset",
            "snapshot_scope": "independently_linearizable_members",
        }

    def append_idempotent(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        producer_id: str,
        producer_epoch: int,
        sequence: int,
        event: EventEnvelope,
    ) -> dict[str, Any]:
        """Append one producer-epoch/sequence-fenced record."""
        _stream_producer(producer_id, producer_epoch)
        _non_negative(sequence, "producer sequence")
        if not isinstance(event, EventEnvelope):
            raise TypeError("event must be an EventEnvelope")
        return self._mutate_state(
            stream,
            shard,
            idempotency_key,
            {
                "action": "append_idempotent",
                "producer_id": producer_id,
                "producer_epoch": str(producer_epoch),
                "sequence": str(sequence),
                "partition": 0,
                "envelope": event.to_dict(),
            },
        )

    def consume_long_poll(
        self,
        stream: str,
        shard: int,
        offset: int,
        *,
        limit: int = 100,
        isolation: StreamReadIsolation = "read_committed",
        mode: StreamConsumerMode = "push",
        consumer_id: str | None = None,
        wait_ms: int = 30_000,
    ) -> dict[str, Any]:
        """Wait for visible records in a shared push or dedicated consumer lane."""
        _non_negative(offset, "offset")
        _fetch_limit(limit)
        if isolation not in {"read_committed", "read_uncommitted"}:
            raise ValueError(f"unsupported Stream read isolation: {isolation}")
        if isinstance(wait_ms, bool) or not 1 <= wait_ms <= 30_000:
            raise ValueError("consumer wait must be between 1 and 30000 milliseconds")
        if mode == "dedicated":
            _required(consumer_id or "", "dedicated consumer ID")
        elif mode != "push" or consumer_id is not None:
            raise ValueError("push mode does not accept a consumer ID")
        query: dict[str, Any] = {
            "offset": offset,
            "limit": limit,
            "isolation": isolation,
            "mode": mode,
            "wait_ms": wait_ms,
        }
        if consumer_id is not None:
            query["consumer_id"] = consumer_id
        return self._stream_call(
            stream,
            shard,
            lambda _route: (
                "GET",
                "/records/consume",
                None,
                query,
                {"x-epoch-read-consistency": "linearizable"},
            ),
        )

    def begin_transaction(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        transaction_id: str,
        producer_id: str,
        producer_epoch: int,
    ) -> dict[str, Any]:
        """Open or exactly replay a producer-fenced transaction."""
        _required(transaction_id, "transaction ID")
        _stream_producer(producer_id, producer_epoch)
        return self._mutate_state(
            stream,
            shard,
            idempotency_key,
            {
                "action": "begin_transaction",
                "transaction_id": transaction_id,
                "producer_id": producer_id,
                "producer_epoch": str(producer_epoch),
            },
        )

    def append_transaction(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        transaction_id: str,
        producer_id: str,
        producer_epoch: int,
        sequence: int,
        events: list[EventEnvelope],
    ) -> dict[str, Any]:
        """Atomically append a bounded sequence inside an open transaction."""
        _required(transaction_id, "transaction ID")
        _stream_producer(producer_id, producer_epoch)
        _non_negative(sequence, "producer sequence")
        if not isinstance(events, list) or not 1 <= len(events) <= 128:
            raise ValueError("transaction append must contain between 1 and 128 records")
        if any(not isinstance(event, EventEnvelope) for event in events):
            raise TypeError("transaction events must be EventEnvelope values")
        return self._mutate_state(
            stream,
            shard,
            idempotency_key,
            {
                "action": "append_transaction",
                "transaction_id": transaction_id,
                "producer_id": producer_id,
                "producer_epoch": str(producer_epoch),
                "sequence": str(sequence),
                "partition": 0,
                "envelopes": [event.to_dict() for event in events],
            },
        )

    def commit_transaction(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        transaction_id: str,
        *,
        offset_commit: StreamOffsetCommit | None = None,
    ) -> dict[str, Any]:
        """Make transaction records visible and optionally commit an offset atomically."""
        _required(transaction_id, "transaction ID")
        operation: dict[str, Any] = {
            "action": "commit_transaction",
            "transaction_id": transaction_id,
        }
        if offset_commit is not None:
            if not isinstance(offset_commit, StreamOffsetCommit):
                raise TypeError("offset_commit must be a StreamOffsetCommit")
            if offset_commit.partition != shard:
                raise ValueError("transaction offset commit must target the transaction shard")
            operation["offset_commit"] = {
                "group": offset_commit.group,
                "partition": 0,
                "next_offset": str(offset_commit.next_offset),
            }
        return self._mutate_state(stream, shard, idempotency_key, operation)

    def abort_transaction(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        transaction_id: str,
    ) -> dict[str, Any]:
        """Permanently hide transaction records from read-committed consumers."""
        _required(transaction_id, "transaction ID")
        return self._mutate_state(
            stream,
            shard,
            idempotency_key,
            {"action": "abort_transaction", "transaction_id": transaction_id},
        )

    def compact(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        tombstone_retention_ms: int,
    ) -> dict[str, Any]:
        """Retain the latest committed record per key and expire old tombstones."""
        _positive(tombstone_retention_ms, "tombstone retention")
        return self._mutate_state(
            stream,
            shard,
            idempotency_key,
            {
                "action": "compact",
                "partition": 0,
                "tombstone_retention_ms": str(tombstone_retention_ms),
            },
        )

    def tier_prefix(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        before_offset: int,
        *,
        max_records: int = 1_024,
    ) -> dict[str, Any]:
        """Move a committed hot prefix into an immutable checksum-verified object."""
        _non_negative(before_offset, "tier before offset")
        if isinstance(max_records, bool) or not 1 <= max_records <= 1_024:
            raise ValueError("tier max records must be between 1 and 1024")
        return self._mutate_state(
            stream,
            shard,
            idempotency_key,
            {
                "action": "tier_prefix",
                "partition": 0,
                "before_offset": str(before_offset),
                "max_records": max_records,
            },
        )

    def capture(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        capture_id: str,
        first_offset: int,
        end_offset: int,
        *,
        format: StreamCaptureFormat = "json_lines",
    ) -> dict[str, Any]:
        """Capture one committed offset range in a portable open format."""
        _required(capture_id, "capture ID")
        _non_negative(first_offset, "capture first offset")
        _non_negative(end_offset, "capture end offset")
        if first_offset > end_offset:
            raise ValueError("capture offset range must be ordered")
        if format not in {"json_lines", "json_array"}:
            raise ValueError(f"unsupported Stream capture format: {format}")
        return self._mutate_state(
            stream,
            shard,
            idempotency_key,
            {
                "action": "capture",
                "capture_id": capture_id,
                "partition": 0,
                "first_offset": str(first_offset),
                "end_offset": str(end_offset),
                "format": format,
            },
        )

    def configure_capture_schedule(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        schedule_id: str,
        interval_ms: int,
        *,
        format: StreamCaptureFormat = "json_lines",
    ) -> dict[str, Any]:
        """Enable leader-driven periodic capture with a replicated offset checkpoint."""
        _required(schedule_id, "capture schedule ID")
        if (
            isinstance(interval_ms, bool)
            or not isinstance(interval_ms, int)
            or not _MIN_CAPTURE_INTERVAL_MS <= interval_ms <= _MAX_CAPTURE_INTERVAL_MS
        ):
            raise ValueError(
                "capture interval must be between "
                f"{_MIN_CAPTURE_INTERVAL_MS} and {_MAX_CAPTURE_INTERVAL_MS} milliseconds"
            )
        if format not in {"json_lines", "json_array"}:
            raise ValueError(f"unsupported Stream capture format: {format}")
        return self._mutate_state(
            stream,
            shard,
            idempotency_key,
            {
                "action": "configure_capture_schedule",
                "schedule_id": schedule_id,
                "partition": 0,
                "interval_ms": str(interval_ms),
                "format": format,
            },
        )

    def replicate(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        batch: StreamReplicationBatch,
    ) -> dict[str, Any]:
        """Apply one contiguous source batch with checkpoint and loop fencing."""
        if not isinstance(batch, StreamReplicationBatch):
            raise TypeError("batch must be a StreamReplicationBatch")
        records = [
            {
                "source_offset": str(record.source_offset),
                "envelope": record.envelope.to_dict(),
                "traversed_clusters": list(record.traversed_clusters),
            }
            for record in batch.records
        ]
        return self._mutate_state(
            stream,
            shard,
            idempotency_key,
            {
                "action": "replicate",
                "local_partition": 0,
                "batch": {
                    "source_cluster": batch.source_cluster,
                    "source_stream": batch.source_stream,
                    "source_partition": batch.source_partition,
                    "first_source_offset": str(batch.first_source_offset),
                    "records": records,
                },
            },
        )

    def transaction(self, stream: str, shard: int, transaction_id: str) -> dict[str, Any]:
        """Return one linearizable transaction observation."""
        transaction = _segment(transaction_id, "transaction")
        return self._stream_call(
            stream,
            shard,
            lambda _route: (
                "GET",
                f"/transactions/{transaction}",
                None,
                None,
                {"x-epoch-read-consistency": "linearizable"},
            ),
        )

    def tier_objects(self, stream: str, shard: int) -> dict[str, Any]:
        """List immutable tier manifests for one shard."""
        return self._linearizable_stream_read(stream, shard, "/tier/objects")

    def capture_artifact(self, stream: str, shard: int, capture_id: str) -> dict[str, Any]:
        """Return one retained capture artifact and checksum."""
        capture = _segment(capture_id, "capture")
        return self._linearizable_stream_read(stream, shard, f"/captures/{capture}")

    def capture_schedule(self, stream: str, shard: int, schedule_id: str) -> dict[str, Any]:
        """Return a replicated automatic-capture checkpoint and next deadline."""
        schedule = _segment(schedule_id, "capture schedule")
        return self._linearizable_stream_read(stream, shard, f"/capture-schedules/{schedule}")

    def partition_advice(
        self,
        stream: str,
        target_records_per_partition: int,
        target_bytes_per_partition: int,
    ) -> dict[str, Any]:
        """Estimate an online expand-only partition target."""
        _positive(target_records_per_partition, "target records per partition")
        _positive(target_bytes_per_partition, "target bytes per partition")
        return self._stream_call(
            stream,
            0,
            lambda _route: (
                "GET",
                "/partitions/advice",
                None,
                {
                    "target_records_per_partition": target_records_per_partition,
                    "target_bytes_per_partition": target_bytes_per_partition,
                },
                {"x-epoch-read-consistency": "linearizable"},
            ),
        )

    def commit_offset(
        self,
        stream: str,
        shard: int,
        group: str,
        member_id: str,
        generation: int,
        next_offset: int,
        *,
        reset: bool = False,
        idempotency_key: str,
    ) -> dict[str, Any]:
        group_segment = _segment(group, "consumer group")
        _required(member_id, "consumer member")
        _positive(generation, "consumer group generation")
        _non_negative(next_offset, "next offset")
        _required(idempotency_key, "idempotency key")
        return self._stream_call(
            stream,
            shard,
            lambda route: (
                "PUT",
                f"/groups/{group_segment}/offsets",
                {
                    "idempotency_key": idempotency_key,
                    "expected_term": route.term,
                    "member_id": member_id,
                    "group_generation": str(generation),
                    "partition": 0,
                    "next_offset": str(next_offset),
                    "mode": "reset" if reset else "commit",
                },
                None,
                {},
            ),
        )

    def lag(self, stream: str, shard: int, group: str) -> dict[str, Any]:
        return self._lag_at_generation(stream, shard, group, None)

    def _lag_at_generation(
        self,
        stream: str,
        shard: int,
        group: str,
        resource_generation: str | None,
    ) -> dict[str, Any]:
        group_segment = _segment(group, "consumer group")
        return self.call_at_generation(
            "streams",
            "Stream",
            stream,
            shard,
            resource_generation,
            lambda _route: (
                "GET",
                f"/groups/{group_segment}/lag",
                None,
                None,
                {"x-epoch-read-consistency": "linearizable"},
            ),
        )

    def fetch_group(
        self, stream: str, shard: int, group: str, *, limit: int = 100
    ) -> dict[str, Any]:
        _fetch_limit(limit)
        group_segment = _segment(group, "consumer group")
        return self._stream_call(
            stream,
            shard,
            lambda _route: (
                "GET",
                f"/groups/{group_segment}/records",
                None,
                {"limit": limit},
                {"x-epoch-read-consistency": "linearizable"},
            ),
        )

    def claim_group(
        self,
        stream: str,
        shard: int,
        group: str,
        member_id: str,
        generation: int,
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Install one coordinated-session generation as a shard checkpoint fence."""
        return self._claim_group_at_generation(
            stream,
            shard,
            group,
            member_id,
            generation,
            idempotency_key,
            None,
        )

    def _claim_group_at_generation(
        self,
        stream: str,
        shard: int,
        group: str,
        member_id: str,
        generation: int,
        idempotency_key: str,
        resource_generation: str | None,
    ) -> dict[str, Any]:
        group_segment = _segment(group, "consumer group")
        _required(member_id, "consumer member")
        _positive(generation, "consumer group generation")
        _required(idempotency_key, "idempotency key")
        return self.call_at_generation(
            "streams",
            "Stream",
            stream,
            shard,
            resource_generation,
            lambda route: (
                "PUT",
                f"/groups/{group_segment}/claim",
                {
                    "idempotency_key": idempotency_key,
                    "expected_term": route.term,
                    "member_id": member_id,
                    "group_generation": str(generation),
                    "partition": 0,
                },
                None,
                {},
            ),
        )

    def fetch_claimed_group(
        self,
        stream: str,
        shard: int,
        group: str,
        member_id: str,
        generation: int,
        *,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Fetch only while the exact member and session generation own the shard."""
        _fetch_limit(limit)
        group_segment = _segment(group, "consumer group")
        _required(member_id, "consumer member")
        _positive(generation, "consumer group generation")
        return self._stream_call(
            stream,
            shard,
            lambda _route: (
                "GET",
                f"/groups/{group_segment}/claimed-records",
                None,
                {
                    "member_id": member_id,
                    "group_generation": str(generation),
                    "limit": limit,
                },
                {"x-epoch-read-consistency": "linearizable"},
            ),
        )

    def claim_consumer_session(
        self,
        stream: str,
        group: str,
        member_id: str,
        generation: int,
        *,
        idempotency_key_prefix: str,
    ) -> tuple[int, ...]:
        """Claim all assigned shards and reject a concurrent coordinator rebalance."""
        _segment(group, "consumer group")
        _required(member_id, "consumer member")
        _positive(generation, "consumer group generation")
        _required(idempotency_key_prefix, "idempotency key prefix")
        coordinator = self.discover_route("streams", "Stream", stream, 0)
        resource_generation = coordinator.resource_generation
        assigned = _coordinated_assignment(
            self._consumer_session_at_generation(stream, group, resource_generation),
            group,
            member_id,
            generation,
        )
        claims: list[tuple[int, int, str]] = []
        for shard in assigned:
            lag = self._lag_at_generation(stream, shard, group, resource_generation)
            for claim_generation in _claim_generations(lag, generation):
                key = f"{idempotency_key_prefix}-shard-{shard}-generation-{claim_generation}"
                if len(key.encode()) > 128:
                    raise ValueError("derived consumer claim idempotency key exceeds 128 bytes")
                claims.append((shard, claim_generation, key))
        for shard, claim_generation, key in claims:
            result = self._claim_group_at_generation(
                stream,
                shard,
                group,
                member_id,
                claim_generation,
                key,
                resource_generation,
            )
            receipt = result.get("receipt") if isinstance(result, dict) else None
            if (
                not isinstance(receipt, dict)
                or receipt.get("outcome") != "applied"
                or receipt.get("session_fenced") is not True
            ):
                raise ValueError(f"shard {shard} rejected the coordinated consumer claim")
        revalidated = _coordinated_assignment(
            self._consumer_session_at_generation(stream, group, resource_generation),
            group,
            member_id,
            generation,
        )
        if revalidated != assigned:
            raise ValueError("consumer session rebalanced while shard claims were being installed")
        return assigned

    def join_consumer_session(
        self,
        stream: str,
        group: str,
        member_id: str,
        session_timeout_ms: int,
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Join or renew a member and return its generation-fenced shard assignment."""
        group_segment = _segment(group, "consumer group")
        _required(member_id, "consumer member")
        _bounded_session_timeout(session_timeout_ms)
        _required(idempotency_key, "idempotency key")
        return self._stream_call(
            stream,
            0,
            lambda route: (
                "POST",
                f"/groups/{group_segment}/sessions",
                {
                    "idempotency_key": idempotency_key,
                    "expected_term": route.term,
                    "member_id": member_id,
                    "session_timeout_ms": str(session_timeout_ms),
                },
                None,
                {},
            ),
        )

    def heartbeat_consumer_session(
        self,
        stream: str,
        group: str,
        member_id: str,
        generation: int,
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Renew one member with the current group-generation fence."""
        return self._mutate_consumer_session(
            "PUT",
            stream,
            group,
            member_id,
            generation,
            idempotency_key,
            "/heartbeat",
        )

    def leave_consumer_session(
        self,
        stream: str,
        group: str,
        member_id: str,
        generation: int,
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Leave a group and deterministically reassign this member's shards."""
        return self._mutate_consumer_session(
            "DELETE", stream, group, member_id, generation, idempotency_key, ""
        )

    def maintain_consumer_session(
        self, stream: str, group: str, *, idempotency_key: str
    ) -> dict[str, Any]:
        """Commit an inclusive-deadline member-expiry sweep on shard zero."""
        group_segment = _segment(group, "consumer group")
        _required(idempotency_key, "idempotency key")
        return self._stream_call(
            stream,
            0,
            lambda route: (
                "POST",
                f"/groups/{group_segment}/sessions/maintenance",
                {"idempotency_key": idempotency_key, "expected_term": route.term},
                None,
                {},
            ),
        )

    def consumer_session(self, stream: str, group: str) -> dict[str, Any]:
        """Return linearizable membership, generation, deadlines, and assignments."""
        return self._consumer_session_at_generation(stream, group, None)

    def _consumer_session_at_generation(
        self, stream: str, group: str, resource_generation: str | None
    ) -> dict[str, Any]:
        group_segment = _segment(group, "consumer group")
        return self.call_at_generation(
            "streams",
            "Stream",
            stream,
            0,
            resource_generation,
            lambda _route: (
                "GET",
                f"/groups/{group_segment}/sessions",
                None,
                None,
                {"x-epoch-read-consistency": "linearizable"},
            ),
        )

    def _mutate_consumer_session(
        self,
        method: str,
        stream: str,
        group: str,
        member_id: str,
        generation: int,
        idempotency_key: str,
        suffix: str,
    ) -> dict[str, Any]:
        group_segment = _segment(group, "consumer group")
        member_segment = _segment(member_id, "consumer member")
        _positive(generation, "consumer group generation")
        _required(idempotency_key, "idempotency key")
        return self._stream_call(
            stream,
            0,
            lambda route: (
                method,
                f"/groups/{group_segment}/sessions/{member_segment}{suffix}",
                {
                    "idempotency_key": idempotency_key,
                    "expected_term": route.term,
                    "group_generation": str(generation),
                },
                None,
                {},
            ),
        )

    def configure_retention(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        policy: StreamRetentionPolicy,
    ) -> dict[str, Any]:
        """Commit a replacement retention policy and apply it immediately."""
        _required(idempotency_key, "idempotency key")
        if not isinstance(policy, StreamRetentionPolicy):
            raise TypeError("policy must be a StreamRetentionPolicy")
        return self._stream_call(
            stream,
            shard,
            lambda route: (
                "PUT",
                "/retention",
                {
                    "idempotency_key": idempotency_key,
                    "expected_term": route.term,
                    **policy.to_wire(),
                },
                None,
                {},
            ),
        )

    def maintain_retention(self, stream: str, shard: int, idempotency_key: str) -> dict[str, Any]:
        """Commit an idle-stream age sweep using the current leader time."""
        _required(idempotency_key, "idempotency key")
        return self._stream_call(
            stream,
            shard,
            lambda route: (
                "POST",
                "/retention/maintenance",
                {"idempotency_key": idempotency_key, "expected_term": route.term},
                None,
                {},
            ),
        )

    def retention(self, stream: str, shard: int) -> dict[str, Any]:
        """Return a linearizable retention policy and retained boundary."""
        return self._stream_call(
            stream,
            shard,
            lambda _route: (
                "GET",
                "/retention",
                None,
                None,
                {"x-epoch-read-consistency": "linearizable"},
            ),
        )

    def _stream_call(self, stream: str, shard: int, request_for: Any) -> Any:
        return self.call("streams", "Stream", stream, shard, request_for)

    def _mutate_state(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        operation: dict[str, Any],
    ) -> dict[str, Any]:
        _required(idempotency_key, "idempotency key")
        return self._stream_call(
            stream,
            shard,
            lambda route: (
                "POST",
                "/state",
                {
                    "idempotency_key": idempotency_key,
                    "expected_term": route.term,
                    "operation": operation,
                },
                None,
                {},
            ),
        )

    def _linearizable_stream_read(self, stream: str, shard: int, path: str) -> dict[str, Any]:
        return self._stream_call(
            stream,
            shard,
            lambda _route: (
                "GET",
                path,
                None,
                None,
                {"x-epoch-read-consistency": "linearizable"},
            ),
        )


def _fetch_limit(limit: int) -> None:
    if isinstance(limit, bool) or not 1 <= limit <= _MAX_FETCH_RECORDS:
        raise ValueError(f"fetch limit must be between 1 and {_MAX_FETCH_RECORDS}")


def _stream_producer(producer_id: str, producer_epoch: int) -> None:
    _required(producer_id, "producer ID")
    _positive(producer_epoch, "producer epoch")


def _stream_response_u64(value: Any, field: str) -> int:
    maximum = (1 << 64) - 1
    if isinstance(value, str):
        if not value.isascii() or not value.isdigit() or str(int(value)) != value:
            raise ValueError(f"Stream response {field} is invalid")
        parsed = int(value)
    elif isinstance(value, int) and not isinstance(value, bool):
        parsed = value
    else:
        raise ValueError(f"Stream response omitted {field}")
    if not 0 <= parsed <= maximum:
        raise ValueError(f"Stream response {field} is invalid")
    return parsed


def _bounded_session_timeout(session_timeout_ms: int) -> None:
    if (
        isinstance(session_timeout_ms, bool)
        or not isinstance(session_timeout_ms, int)
        or not _MIN_SESSION_TIMEOUT_MS <= session_timeout_ms <= _MAX_SESSION_TIMEOUT_MS
    ):
        raise ValueError(
            "consumer session timeout must be between "
            f"{_MIN_SESSION_TIMEOUT_MS} and {_MAX_SESSION_TIMEOUT_MS} milliseconds"
        )


def _coordinated_assignment(
    document: dict[str, Any], group: str, member_id: str, generation: int
) -> tuple[int, ...]:
    session = document.get("session") if isinstance(document, dict) else None
    if not isinstance(session, dict):
        raise ValueError("consumer session response omitted session state")
    if (
        session.get("exists") is not True
        or session.get("group") != group
        or session.get("group_generation") != str(generation)
    ):
        raise ValueError("consumer session generation is absent or fenced")
    members = session.get("members")
    if not isinstance(members, list):
        raise ValueError("consumer session response omitted members")
    for member in members:
        if not isinstance(member, dict) or member.get("member_id") != member_id:
            continue
        shards = member.get("assigned_shards")
        if not isinstance(shards, list) or not shards:
            raise ValueError("consumer member has no assigned shards")
        if any(
            isinstance(shard, bool) or not isinstance(shard, int) or not 0 <= shard <= 0xFFFF_FFFF
            for shard in shards
        ):
            raise ValueError("consumer session returned an invalid shard assignment")
        if shards != sorted(set(shards)):
            raise ValueError("consumer session returned an invalid shard assignment")
        return tuple(shards)
    raise ValueError("consumer member is not active in the requested session generation")


def _claim_generations(document: dict[str, Any], target: int) -> tuple[int, ...]:
    checkpoint = document.get("checkpoint") if isinstance(document, dict) else None
    if not isinstance(checkpoint, dict):
        raise ValueError("checkpoint observation is missing")
    current = 0
    if checkpoint.get("exists") is True:
        raw = checkpoint.get("group_generation")
        if (
            not isinstance(raw, str)
            or not raw.isascii()
            or not raw.isdigit()
            or not 0 < int(raw) <= (1 << 64) - 1
            or str(int(raw)) != raw
        ):
            raise ValueError("checkpoint generation is invalid")
        current = int(raw)
    if current > target:
        raise ValueError(f"checkpoint generation {current} is ahead of session generation {target}")
    start = current if current == target else current + 1
    count = target - start + 1
    if count > _MAX_CLAIM_TRANSITIONS:
        raise ValueError(f"claim requires {count} transitions; maximum is {_MAX_CLAIM_TRANSITIONS}")
    return tuple(range(start, target + 1))


def stream_shard_for(partition_value: str, shard_count: int) -> int:
    """Map UTF-8 bytes with the versioned unsigned FNV-1a Stream partitioner."""
    if not isinstance(partition_value, str):
        raise TypeError("partition value must be a string")
    if isinstance(shard_count, bool) or not isinstance(shard_count, int) or shard_count <= 0:
        raise ValueError("Stream shard count must be greater than zero")
    hash_value = 0xCBF29CE484222325
    for value in partition_value.encode("utf-8"):
        hash_value = ((hash_value ^ value) * 0x100000001B3) & ((1 << 64) - 1)
    return hash_value % shard_count


def _optional_bound(value: int | None, name: str, maximum: int) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum} when set")


__all__ = [
    "RegionalScope",
    "RegionalStreamClient",
    "Route",
    "StreamCaptureFormat",
    "StreamConsumerMode",
    "StreamOffsetCommit",
    "StreamReadIsolation",
    "StreamReplicationBatch",
    "StreamReplicationRecord",
    "StreamRetentionPolicy",
    "StreamSuperstreamMember",
    "stream_shard_for",
]
