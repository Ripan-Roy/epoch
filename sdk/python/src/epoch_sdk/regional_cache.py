"""Authenticated leader- and fence-aware regional Cache client."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, ClassVar

from ._regional import RegionalClient, Route, _non_negative, _positive, _required

_MAX_I64 = (1 << 63) - 1
_MIN_I64 = -(1 << 63)
_MAX_TRANSACTION_MUTATIONS = 128
_MAX_MAINTENANCE_EXPIRATIONS = 1_000
_LINEARIZABLE = {"x-epoch-read-consistency": "linearizable"}


@dataclass(frozen=True, slots=True)
class RegionalCacheValue:
    """One strict Cache value, created with a type-specific factory."""

    kind: str
    value: Any

    _KINDS: ClassVar[frozenset[str]] = frozenset(
        {"string", "blob", "counter", "hash", "list", "set", "sorted_set"}
    )

    def __post_init__(self) -> None:
        if self.kind not in self._KINDS:
            raise ValueError(f"unsupported Cache value kind: {self.kind}")
        object.__setattr__(self, "value", _normalize_value(self.kind, self.value))

    @classmethod
    def string(cls, value: str) -> RegionalCacheValue:
        return cls("string", value)

    @classmethod
    def blob(cls, value: bytes | bytearray) -> RegionalCacheValue:
        return cls("blob", value)

    @classmethod
    def counter(cls, value: int) -> RegionalCacheValue:
        return cls("counter", value)

    @classmethod
    def hash(cls, value: dict[str, str]) -> RegionalCacheValue:
        return cls("hash", value)

    @classmethod
    def list(cls, value: list[str] | tuple[str, ...]) -> RegionalCacheValue:
        return cls("list", value)

    @classmethod
    def set(
        cls, value: list[str] | tuple[str, ...] | set[str] | frozenset[str]
    ) -> RegionalCacheValue:
        return cls("set", value)

    @classmethod
    def sorted_set(cls, value: dict[str, float]) -> RegionalCacheValue:
        return cls("sorted_set", value)

    def to_wire(self) -> dict[str, Any]:
        value = self.value
        if self.kind == "counter":
            value = str(value)
        elif self.kind in {"blob", "list", "set"}:
            value = list(value)
        elif self.kind in {"hash", "sorted_set"}:
            value = dict(value)
        return {"kind": self.kind, "value": value}


@dataclass(frozen=True, slots=True)
class RegionalCacheExpectation:
    """CAS expectation that distinguishes missing-at-revision from a version."""

    kind: str
    value: int

    def __post_init__(self) -> None:
        if self.kind not in {"missing", "version"}:
            raise ValueError(f"unsupported Cache expectation kind: {self.kind}")
        _non_negative(self.value, f"Cache {self.kind} expectation")

    @classmethod
    def missing(cls, shard_revision: int) -> RegionalCacheExpectation:
        return cls("missing", shard_revision)

    @classmethod
    def version(cls, version: int) -> RegionalCacheExpectation:
        return cls("version", version)

    def to_wire(self) -> dict[str, str]:
        field = "shard_revision" if self.kind == "missing" else "version"
        return {"kind": self.kind, field: str(self.value)}


@dataclass(frozen=True, slots=True)
class RegionalCacheLockGuard:
    """Opaque lease proof required by a guarded Cache mutation."""

    lock_key: str
    owner: str
    owner_epoch: int
    lease_token: str

    def __post_init__(self) -> None:
        _required(self.lock_key, "Cache lock key")
        _required(self.owner, "Cache lock owner")
        _positive(self.owner_epoch, "Cache lock owner epoch")
        _required(self.lease_token, "Cache lease token")

    def to_wire(self) -> dict[str, str]:
        return {
            "lock_key": self.lock_key,
            "owner": self.owner,
            "owner_epoch": str(self.owner_epoch),
            "lease_token": self.lease_token,
        }


@dataclass(frozen=True, slots=True)
class RegionalCacheMutation:
    """One mutation permitted inside an atomic Cache transaction."""

    key: str
    operation: dict[str, Any]

    def __post_init__(self) -> None:
        _required(self.key, "Cache key")
        object.__setattr__(self, "operation", dict(self.operation))

    @classmethod
    def set(
        cls, key: str, value: RegionalCacheValue, *, ttl_ms: int | None = None
    ) -> RegionalCacheMutation:
        body = _set_operation(key, value, ttl_ms, None)
        body.pop("shard")
        return cls(key, body)

    @classmethod
    def delete(cls, key: str, *, expected_version: int | None = None) -> RegionalCacheMutation:
        body = _delete_operation(key, expected_version, None)
        body.pop("shard")
        return cls(key, body)

    @classmethod
    def compare_and_set(
        cls,
        key: str,
        expected: RegionalCacheExpectation,
        value: RegionalCacheValue,
        *,
        ttl_ms: int | None = None,
    ) -> RegionalCacheMutation:
        body = _cas_operation(key, expected, value, ttl_ms, None)
        body.pop("shard")
        return cls(key, body)

    @classmethod
    def increment(
        cls,
        key: str,
        delta: int,
        *,
        expected_version: int | None = None,
        ttl_ms: int | None = None,
    ) -> RegionalCacheMutation:
        body = _increment_operation(key, delta, expected_version, ttl_ms, None)
        body.pop("shard")
        return cls(key, body)

    def to_wire(self) -> dict[str, Any]:
        return dict(self.operation)


class RegionalCacheClient(RegionalClient):
    """Complete single-shard Cache lifecycle client with explicit mutation identities."""

    def set(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        key: str,
        value: RegionalCacheValue,
        *,
        ttl_ms: int | None = None,
        lock_guard: RegionalCacheLockGuard | None = None,
    ) -> dict[str, Any]:
        return self._mutate(
            cache, shard, idempotency_key, _set_operation(key, value, ttl_ms, lock_guard)
        )

    def get(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        key: str,
    ) -> dict[str, Any]:
        """Return one value and commit its access for deterministic LRU/LFU admission."""
        _required(key, "Cache key")
        return self._mutate(
            cache,
            shard,
            idempotency_key,
            {"kind": "get", "shard": 0, "key": key},
        )

    def delete(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        key: str,
        *,
        expected_version: int | None = None,
        lock_guard: RegionalCacheLockGuard | None = None,
    ) -> dict[str, Any]:
        return self._mutate(
            cache,
            shard,
            idempotency_key,
            _delete_operation(key, expected_version, lock_guard),
        )

    def compare_and_set(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        key: str,
        expected: RegionalCacheExpectation,
        value: RegionalCacheValue,
        *,
        ttl_ms: int | None = None,
        lock_guard: RegionalCacheLockGuard | None = None,
    ) -> dict[str, Any]:
        return self._mutate(
            cache,
            shard,
            idempotency_key,
            _cas_operation(key, expected, value, ttl_ms, lock_guard),
        )

    def increment(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        key: str,
        delta: int,
        *,
        expected_version: int | None = None,
        ttl_ms: int | None = None,
        lock_guard: RegionalCacheLockGuard | None = None,
    ) -> dict[str, Any]:
        return self._mutate(
            cache,
            shard,
            idempotency_key,
            _increment_operation(key, delta, expected_version, ttl_ms, lock_guard),
        )

    def transaction(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        expected_revision: int,
        mutations: list[RegionalCacheMutation] | tuple[RegionalCacheMutation, ...],
        *,
        lock_guards: list[RegionalCacheLockGuard] | tuple[RegionalCacheLockGuard, ...] = (),
    ) -> dict[str, Any]:
        _non_negative(expected_revision, "Cache expected revision")
        if not 1 <= len(mutations) <= _MAX_TRANSACTION_MUTATIONS:
            raise ValueError("Cache transaction mutations must be between 1 and 128")
        if any(not isinstance(mutation, RegionalCacheMutation) for mutation in mutations):
            raise TypeError("Cache transaction mutations must be RegionalCacheMutation values")
        keys = [mutation.key for mutation in mutations]
        if len(keys) != len(set(keys)):
            raise ValueError("Cache transaction keys must be distinct")
        guards = [_guard(guard).to_wire() for guard in lock_guards]
        return self._mutate(
            cache,
            shard,
            idempotency_key,
            {
                "kind": "transaction",
                "shard": 0,
                "expected_revision": str(expected_revision),
                "mutations": [mutation.to_wire() for mutation in mutations],
                "lock_guards": guards,
            },
        )

    def atomic_batch(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        expected_revision: int,
        mutations: list[RegionalCacheMutation] | tuple[RegionalCacheMutation, ...],
        *,
        lock_guards: list[RegionalCacheLockGuard] | tuple[RegionalCacheLockGuard, ...] = (),
    ) -> dict[str, Any]:
        """Send one ordered atomic batch as one HTTP request and consensus proposal."""
        return self.transaction(
            cache,
            shard,
            idempotency_key,
            expected_revision,
            mutations,
            lock_guards=lock_guards,
        )

    def acquire_lock(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        lock_key: str,
        owner: str,
        owner_epoch: int,
        lease_ms: int,
    ) -> dict[str, Any]:
        _lock_identity(lock_key, owner, owner_epoch)
        _positive(lease_ms, "Cache lock lease")
        return self._mutate(
            cache,
            shard,
            idempotency_key,
            {
                "kind": "acquire_lock",
                "shard": 0,
                "lock_key": lock_key,
                "owner": owner,
                "owner_epoch": str(owner_epoch),
                "lease_ms": str(lease_ms),
            },
        )

    def renew_lock(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        lock_key: str,
        owner: str,
        owner_epoch: int,
        lease_token: str,
        extension_ms: int,
    ) -> dict[str, Any]:
        operation = _lock_operation("renew_lock", lock_key, owner, owner_epoch, lease_token)
        _positive(extension_ms, "Cache lock extension")
        operation["extension_ms"] = str(extension_ms)
        return self._mutate(cache, shard, idempotency_key, operation)

    def release_lock(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        lock_key: str,
        owner: str,
        owner_epoch: int,
        lease_token: str,
    ) -> dict[str, Any]:
        return self._mutate(
            cache,
            shard,
            idempotency_key,
            _lock_operation("release_lock", lock_key, owner, owner_epoch, lease_token),
        )

    def maintain(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        *,
        max_expirations: int = 1_000,
    ) -> dict[str, Any]:
        if (
            isinstance(max_expirations, bool)
            or not isinstance(max_expirations, int)
            or not 1 <= max_expirations <= _MAX_MAINTENANCE_EXPIRATIONS
        ):
            raise ValueError("Cache max expirations must be between 1 and 1000")
        return self._mutate(
            cache,
            shard,
            idempotency_key,
            {"kind": "maintain", "shard": 0, "max_expirations": max_expirations},
        )

    def mutation(self, cache: str, shard: int, proposal_id: int) -> dict[str, Any]:
        _positive(proposal_id, "Cache proposal ID")
        return self._read(cache, shard, f"/mutations/{proposal_id}")

    def observe(self, cache: str, shard: int, key: str) -> dict[str, Any]:
        _required(key, "Cache key")
        return self._read(cache, shard, "/observations", {"key": key})

    def status(self, cache: str, shard: int) -> dict[str, Any]:
        return self._read(cache, shard, "/status")

    def _read(
        self, cache: str, shard: int, path: str, query: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return self.call(
            "caches",
            "Cache",
            cache,
            shard,
            lambda _route: ("GET", path, None, query, _LINEARIZABLE),
        )

    def _mutate(
        self,
        cache: str,
        shard: int,
        idempotency_key: str,
        operation: dict[str, Any],
    ) -> dict[str, Any]:
        _required(idempotency_key, "idempotency key")
        return self.call(
            "caches",
            "Cache",
            cache,
            shard,
            lambda route: _mutation_request(route, idempotency_key, operation),
        )


def _mutation_request(
    route: Route, idempotency_key: str, operation: dict[str, Any]
) -> tuple[str, str, dict[str, Any], None, dict[str, str]]:
    return (
        "POST",
        "/mutations",
        {
            "idempotency_key": idempotency_key,
            "expected_term": route.term,
            "operation": operation,
        },
        None,
        {},
    )


def _set_operation(
    key: str,
    value: RegionalCacheValue,
    ttl_ms: int | None,
    lock_guard: RegionalCacheLockGuard | None,
) -> dict[str, Any]:
    _required(key, "Cache key")
    operation: dict[str, Any] = {
        "kind": "set",
        "shard": 0,
        "key": key,
        "value": _value(value).to_wire(),
    }
    _optional_u64(operation, "ttl_ms", ttl_ms, positive=True)
    _optional_guard(operation, lock_guard)
    return operation


def _delete_operation(
    key: str, expected_version: int | None, lock_guard: RegionalCacheLockGuard | None
) -> dict[str, Any]:
    _required(key, "Cache key")
    operation: dict[str, Any] = {"kind": "delete", "shard": 0, "key": key}
    _optional_u64(operation, "expected_version", expected_version)
    _optional_guard(operation, lock_guard)
    return operation


def _cas_operation(
    key: str,
    expected: RegionalCacheExpectation,
    value: RegionalCacheValue,
    ttl_ms: int | None,
    lock_guard: RegionalCacheLockGuard | None,
) -> dict[str, Any]:
    _required(key, "Cache key")
    if not isinstance(expected, RegionalCacheExpectation):
        raise TypeError("Cache expectation must be RegionalCacheExpectation")
    operation: dict[str, Any] = {
        "kind": "compare_and_set",
        "shard": 0,
        "key": key,
        "expected": expected.to_wire(),
        "value": _value(value).to_wire(),
    }
    _optional_u64(operation, "ttl_ms", ttl_ms, positive=True)
    _optional_guard(operation, lock_guard)
    return operation


def _increment_operation(
    key: str,
    delta: int,
    expected_version: int | None,
    ttl_ms: int | None,
    lock_guard: RegionalCacheLockGuard | None,
) -> dict[str, Any]:
    _required(key, "Cache key")
    _signed_i64(delta, "Cache increment delta")
    operation: dict[str, Any] = {
        "kind": "increment",
        "shard": 0,
        "key": key,
        "delta": str(delta),
    }
    _optional_u64(operation, "expected_version", expected_version)
    _optional_u64(operation, "ttl_ms", ttl_ms, positive=True)
    _optional_guard(operation, lock_guard)
    return operation


def _optional_u64(
    operation: dict[str, Any], field: str, value: int | None, *, positive: bool = False
) -> None:
    if value is None:
        return
    (_positive if positive else _non_negative)(value, f"Cache {field.replace('_', ' ')}")
    operation[field] = str(value)


def _optional_guard(operation: dict[str, Any], lock_guard: RegionalCacheLockGuard | None) -> None:
    if lock_guard is not None:
        operation["lock_guard"] = _guard(lock_guard).to_wire()


def _value(value: RegionalCacheValue) -> RegionalCacheValue:
    if not isinstance(value, RegionalCacheValue):
        raise TypeError("Cache value must be RegionalCacheValue")
    return value


def _guard(guard: RegionalCacheLockGuard) -> RegionalCacheLockGuard:
    if not isinstance(guard, RegionalCacheLockGuard):
        raise TypeError("Cache lock guard must be RegionalCacheLockGuard")
    return guard


def _lock_identity(lock_key: str, owner: str, owner_epoch: int) -> None:
    _required(lock_key, "Cache lock key")
    _required(owner, "Cache lock owner")
    _positive(owner_epoch, "Cache lock owner epoch")


def _lock_operation(
    kind: str, lock_key: str, owner: str, owner_epoch: int, lease_token: str
) -> dict[str, Any]:
    _lock_identity(lock_key, owner, owner_epoch)
    _required(lease_token, "Cache lease token")
    return {
        "kind": kind,
        "shard": 0,
        "lock_key": lock_key,
        "owner": owner,
        "owner_epoch": str(owner_epoch),
        "lease_token": lease_token,
    }


def _normalize_value(kind: str, value: Any) -> Any:
    if kind == "string":
        if not isinstance(value, str):
            raise TypeError("Cache string value must be str")
        return value
    if kind == "blob":
        if not isinstance(value, (bytes, bytearray)):
            raise TypeError("Cache blob value must be bytes or bytearray")
        return tuple(bytes(value))
    if kind == "counter":
        _signed_i64(value, "Cache counter")
        return value
    if kind == "hash":
        if not isinstance(value, dict) or any(
            not isinstance(key, str) or not isinstance(item, str) for key, item in value.items()
        ):
            raise TypeError("Cache hash value must map str to str")
        return dict(value)
    if kind in {"list", "set"}:
        if isinstance(value, (str, bytes, bytearray)):
            raise TypeError(f"Cache {kind} value must contain strings")
        try:
            items = tuple(value)
        except TypeError as error:
            raise TypeError(f"Cache {kind} value must contain strings") from error
        if any(not isinstance(item, str) for item in items):
            raise TypeError(f"Cache {kind} value must contain strings")
        if kind == "set" and len(items) != len(set(items)):
            raise ValueError("Cache set value contains duplicate members")
        return items
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise TypeError("Cache sorted set value must map str to finite numbers")
    normalized: dict[str, float] = {}
    for key, score in value.items():
        if (
            isinstance(score, bool)
            or not isinstance(score, (int, float))
            or not math.isfinite(score)
        ):
            raise ValueError("Cache sorted set scores must be finite numbers")
        normalized[key] = float(score)
    return normalized


def _signed_i64(value: int, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not _MIN_I64 <= value <= _MAX_I64:
        raise ValueError(f"{label} must be a signed 64-bit integer")


__all__ = [
    "RegionalCacheClient",
    "RegionalCacheExpectation",
    "RegionalCacheLockGuard",
    "RegionalCacheMutation",
    "RegionalCacheValue",
]
