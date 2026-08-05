"""Authenticated leader- and fence-aware regional Stream client."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar
from urllib.parse import quote

from .errors import EpochAPIError
from .models import EventEnvelope
from .transport import Transport, UrllibTransport

_T = TypeVar("_T")
_MAX_FETCH_RECORDS = 1_000


@dataclass(frozen=True, slots=True)
class RegionalScope:
    """Fully-qualified organization/project/environment/namespace scope."""

    organization: str
    project: str
    environment: str
    namespace: str

    def __post_init__(self) -> None:
        for label, value in (
            ("organization", self.organization),
            ("project", self.project),
            ("environment", self.environment),
            ("namespace", self.namespace),
        ):
            if not value.strip():
                raise ValueError(f"{label} is required")

    def path(self) -> str:
        return (
            f"/v1/organizations/{_segment(self.organization, 'organization')}"
            f"/projects/{_segment(self.project, 'project')}"
            f"/environments/{_segment(self.environment, 'environment')}"
            f"/namespaces/{_segment(self.namespace, 'namespace')}"
        )


@dataclass(frozen=True, slots=True)
class _Route:
    resource_generation: str
    tablet_epoch: str
    term: str


class RegionalStreamClient:
    """Synchronous regional Stream client with explicit mutation identity.

    The client discovers the current leader before every operation. A retry
    caused by transport ambiguity, leader replacement, or fencing reuses the
    caller's original idempotency key.
    """

    def __init__(
        self,
        endpoints: list[str] | tuple[str, ...],
        *,
        token: str,
        scope: RegionalScope,
        timeout: float = 10.0,
    ) -> None:
        if not endpoints:
            raise ValueError("at least one regional endpoint is required")
        self._initialize(
            [UrllibTransport(endpoint, timeout=timeout) for endpoint in endpoints], token, scope
        )

    @classmethod
    def with_transports(
        cls,
        transports: list[Transport] | tuple[Transport, ...],
        *,
        token: str,
        scope: RegionalScope,
    ) -> RegionalStreamClient:
        """Construct with injected endpoint transports for tests or custom networking."""

        client = cls.__new__(cls)
        client._initialize(list(transports), token, scope)
        return client

    def _initialize(self, transports: list[Transport], token: str, scope: RegionalScope) -> None:
        if not transports or any(transport is None for transport in transports):
            raise ValueError("regional transports must contain at least one non-null transport")
        normalized_token = token.strip()
        if not normalized_token or "\r" in normalized_token or "\n" in normalized_token:
            raise ValueError("bearer token is required and must fit one HTTP header")
        self._transports = tuple(transports)
        self._token = normalized_token
        self._scope_path = scope.path()

    def append(
        self,
        stream: str,
        shard: int,
        idempotency_key: str,
        event: EventEnvelope,
    ) -> dict[str, Any]:
        _required(idempotency_key, "idempotency key")
        return self._call(
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
        return self._call(
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
        return self._call(
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
        return self._call(
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
        return self._call(
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

    def _call(
        self,
        stream: str,
        shard: int,
        request_for: Callable[
            [_Route], tuple[str, str, Any, dict[str, Any] | None, dict[str, str]]
        ],
    ) -> Any:
        base_path = self._stream_path(stream, shard)
        last_error: Exception | None = None
        for _attempt in range(2):
            try:
                transport, route = self._discover_leader(base_path)
                method, suffix, body, query, headers = request_for(route)
                return transport.request(
                    method,
                    f"{base_path}{suffix}",
                    body=body,
                    query=query,
                    headers=self._headers(route, headers),
                )
            except EpochAPIError as error:
                last_error = error
                if not _rediscover(error):
                    raise
        raise EpochAPIError(
            0,
            "unavailable",
            f"regional Stream operation could not reach a current leader: {last_error}",
        ) from last_error

    def _discover_leader(self, path: str) -> tuple[Transport, _Route]:
        last_error: Exception | None = None
        for transport in self._transports:
            try:
                document = transport.request(
                    "GET",
                    path,
                    headers={"authorization": f"Bearer {self._token}"},
                )
                route = _parse_route(document)
                if document.get("accepts_writes") is True:
                    return transport, route
            except EpochAPIError as error:
                if not _rediscover(error):
                    raise
                last_error = error
            except (TypeError, ValueError) as error:
                last_error = error
        detail = "no configured endpoint reported the current leader"
        if last_error is not None:
            detail = f"{detail}: {last_error}"
        raise EpochAPIError(0, "unavailable", detail) from last_error

    def _headers(self, route: _Route, extra: dict[str, str]) -> dict[str, str]:
        return {
            **extra,
            "authorization": f"Bearer {self._token}",
            "x-epoch-resource-generation": route.resource_generation,
            "x-epoch-tablet-epoch": route.tablet_epoch,
        }

    def _stream_path(self, stream: str, shard: int) -> str:
        _non_negative(shard, "shard")
        return f"{self._scope_path}/streams/{_segment(stream, 'stream')}/shards/{shard}"


def _parse_route(document: Any) -> _Route:
    if not isinstance(document, dict):
        raise ValueError("regional route response must be an object")
    values = []
    for field in ("resource_generation", "tablet_epoch", "term"):
        value = document.get(field)
        if not isinstance(value, str) or not value.isdecimal() or int(value) == 0:
            raise ValueError(f"regional route {field} must be a non-zero decimal string")
        values.append(value)
    return _Route(*values)


def _rediscover(error: EpochAPIError) -> bool:
    return error.retryable or error.code in {
        "not_leader",
        "fenced",
        "route_not_found",
        "route_unavailable",
        "read_barrier_timeout",
    }


def _segment(value: str, label: str) -> str:
    _required(value, label)
    return quote(value, safe="")


def _required(value: str, label: str) -> None:
    if not value.strip():
        raise ValueError(f"{label} is required")


def _positive(value: int, label: str) -> None:
    if isinstance(value, bool) or value <= 0:
        raise ValueError(f"{label} must be positive")


def _non_negative(value: int, label: str) -> None:
    if isinstance(value, bool) or value < 0:
        raise ValueError(f"{label} must be non-negative")


def _fetch_limit(limit: int) -> None:
    if isinstance(limit, bool) or not 1 <= limit <= _MAX_FETCH_RECORDS:
        raise ValueError(f"fetch limit must be between 1 and {_MAX_FETCH_RECORDS}")
