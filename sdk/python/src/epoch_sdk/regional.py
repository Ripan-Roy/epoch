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
_STREAM_PARTITIONER = "fnv1a64_utf8_mod_n_v1"
_MAX_BATCH_RECORDS = 1_000
_MAX_BATCH_COMPRESSED_BYTES = 360 * 1024
_MAX_BATCH_UNCOMPRESSED_BYTES = 4 * 1024 * 1024

StreamCompression = Literal["none", "gzip", "lz4", "snappy", "zstd"]
_STREAM_COMPRESSIONS = frozenset({"none", "gzip", "lz4", "snappy", "zstd"})


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

    def fetch(self, stream: str, shard: int, offset: int, *, limit: int = 100) -> dict[str, Any]:
        _non_negative(offset, "offset")
        _fetch_limit(limit)
        return self._stream_call(
            stream,
            shard,
            lambda _route: (
                "GET",
                "/records",
                None,
                {"offset": offset, "limit": limit},
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
        group_segment = _segment(group, "consumer group")
        return self._stream_call(
            stream,
            shard,
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
        group_segment = _segment(group, "consumer group")
        return self._stream_call(
            stream,
            0,
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


def _fetch_limit(limit: int) -> None:
    if isinstance(limit, bool) or not 1 <= limit <= _MAX_FETCH_RECORDS:
        raise ValueError(f"fetch limit must be between 1 and {_MAX_FETCH_RECORDS}")


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
    "StreamRetentionPolicy",
    "stream_shard_for",
]
