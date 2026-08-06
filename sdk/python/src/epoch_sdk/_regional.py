"""Shared authenticated leader discovery for regional SDK clients."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Self, TypeVar
from urllib.parse import quote

from .errors import EpochAPIError
from .transport import Transport, UrllibTransport

_T = TypeVar("_T")
_MAX_U64 = (1 << 64) - 1


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
            _required(value, label)

    def path(self) -> str:
        return (
            f"/v1/organizations/{_segment(self.organization, 'organization')}"
            f"/projects/{_segment(self.project, 'project')}"
            f"/environments/{_segment(self.environment, 'environment')}"
            f"/namespaces/{_segment(self.namespace, 'namespace')}"
        )


@dataclass(frozen=True, slots=True)
class Route:
    resource_generation: str
    tablet_epoch: str
    term: str


RequestFactory = Callable[[Route], tuple[str, str, Any, dict[str, Any] | None, dict[str, str]]]


class RegionalClient:
    """Shared synchronous leader-discovery, fencing, and rediscovery engine."""

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
    ) -> Self:
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

    def call(
        self,
        collection: str,
        resource_label: str,
        resource: str,
        shard: int,
        request_for: RequestFactory,
    ) -> Any:
        base_path = self._resource_path(collection, resource_label, resource, shard)
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
            f"regional {resource_label} operation could not reach a current leader: {last_error}",
        ) from last_error

    def _discover_leader(self, path: str) -> tuple[Transport, Route]:
        last_error: Exception | None = None
        for transport in self._transports:
            try:
                document = transport.request(
                    "GET", path, headers={"authorization": f"Bearer {self._token}"}
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

    def _headers(self, route: Route, extra: dict[str, str]) -> dict[str, str]:
        return {
            **extra,
            "authorization": f"Bearer {self._token}",
            "x-epoch-resource-generation": route.resource_generation,
            "x-epoch-tablet-epoch": route.tablet_epoch,
        }

    def _resource_path(
        self, collection: str, resource_label: str, resource: str, shard: int
    ) -> str:
        _non_negative(shard, "shard")
        return (
            f"{self._scope_path}/{collection}/{_segment(resource, resource_label)}/shards/{shard}"
        )


def _parse_route(document: Any) -> Route:
    if not isinstance(document, dict):
        raise ValueError("regional route response must be an object")
    values = []
    for field in ("resource_generation", "tablet_epoch", "term"):
        value = document.get(field)
        if (
            not isinstance(value, str)
            or not value.isascii()
            or not value.isdigit()
            or not 0 < int(value) <= _MAX_U64
            or str(int(value)) != value
        ):
            raise ValueError(f"regional route {field} must be a non-zero decimal string")
        values.append(value)
    return Route(*values)


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
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= _MAX_U64:
        raise ValueError(f"{label} must be a positive unsigned 64-bit integer")


def _non_negative(value: int, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _MAX_U64:
        raise ValueError(f"{label} must be a non-negative unsigned 64-bit integer")
