"""Authenticated leader- and fence-aware regional Stream client."""

from __future__ import annotations

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

    def _stream_call(self, stream: str, shard: int, request_for: Any) -> Any:
        return self.call("streams", "Stream", stream, shard, request_for)


def _fetch_limit(limit: int) -> None:
    if isinstance(limit, bool) or not 1 <= limit <= _MAX_FETCH_RECORDS:
        raise ValueError(f"fetch limit must be between 1 and {_MAX_FETCH_RECORDS}")


__all__ = ["RegionalScope", "RegionalStreamClient", "Route"]
