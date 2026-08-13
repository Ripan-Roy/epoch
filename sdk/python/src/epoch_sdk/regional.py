"""Authenticated leader- and fence-aware regional Stream client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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
