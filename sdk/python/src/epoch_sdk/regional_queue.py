"""Authenticated leader- and fence-aware regional Queue client."""

from __future__ import annotations

from typing import Any

from ._regional import RegionalClient, Route, _positive, _required, _segment
from .models import EventEnvelope

_MAX_ACQUIRE_BATCH = 100
_MAX_IN_FLIGHT = 10_000
_MAX_HISTORY = 1_000
_LINEARIZABLE = {"x-epoch-read-consistency": "linearizable"}


class RegionalQueueClient(RegionalClient):
    """Complete Queue lifecycle client with explicit mutation identities."""

    def enqueue(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        event: EventEnvelope,
        *,
        session_id: str | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
    ) -> dict[str, Any]:
        operation: dict[str, Any] = {
            "kind": "enqueue",
            "partition": 0,
            "envelope": event.to_dict(),
        }
        for field, value in (
            ("session_id", session_id),
            ("correlation_id", correlation_id),
            ("reply_to", reply_to),
        ):
            if value is not None:
                _required(value, f"Queue {field}")
                operation[field] = value
        return self._mutate(
            queue,
            shard,
            idempotency_key,
            operation,
        )

    def acquire(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        *,
        consumer: str,
        consumer_epoch: int,
        max_messages: int,
        max_in_flight: int | None = None,
        visibility_timeout_ms: int | None = None,
        session_id: str | None = None,
        session_lock_token: str | None = None,
    ) -> dict[str, Any]:
        _consumer(consumer, consumer_epoch)
        _bounded(max_messages, 1, _MAX_ACQUIRE_BATCH, "Queue max messages")
        if max_in_flight is not None:
            _bounded(max_in_flight, 1, _MAX_IN_FLIGHT, "Queue max in flight")
        if visibility_timeout_ms is not None:
            _positive(visibility_timeout_ms, "Queue visibility timeout")
        if session_id is not None:
            _required(session_id, "Queue session ID")
            if max_in_flight is None:
                raise ValueError("Queue session acquire requires max_in_flight")
        if session_lock_token is not None:
            _required(session_lock_token, "Queue session lock token")
            if session_id is None:
                raise ValueError("Queue session lock token requires session_id")
        operation: dict[str, Any] = {
            "kind": "acquire",
            "partition": 0,
            "consumer": consumer,
            "consumer_epoch": str(consumer_epoch),
            "max_messages": max_messages,
        }
        if max_in_flight is not None:
            operation["max_in_flight"] = max_in_flight
        if visibility_timeout_ms is not None:
            operation["visibility_timeout_ms"] = str(visibility_timeout_ms)
        if session_id is not None:
            operation["session_id"] = session_id
        if session_lock_token is not None:
            operation["session_lock_token"] = session_lock_token
        return self._mutate(queue, shard, idempotency_key, operation)

    def renew_session_lock(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        consumer: str,
        consumer_epoch: int,
        session_lock_token: str,
        extension_ms: int,
    ) -> dict[str, Any]:
        _consumer(consumer, consumer_epoch)
        _required(session_lock_token, "Queue session lock token")
        _positive(extension_ms, "Queue session lock extension")
        return self._mutate(
            queue,
            shard,
            idempotency_key,
            {
                "kind": "renew_session_lock",
                "partition": 0,
                "consumer": consumer,
                "consumer_epoch": str(consumer_epoch),
                "session_lock_token": session_lock_token,
                "extension_ms": str(extension_ms),
            },
        )

    def release_session_lock(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        consumer: str,
        consumer_epoch: int,
        session_lock_token: str,
    ) -> dict[str, Any]:
        _consumer(consumer, consumer_epoch)
        _required(session_lock_token, "Queue session lock token")
        return self._mutate(
            queue,
            shard,
            idempotency_key,
            {
                "kind": "release_session_lock",
                "partition": 0,
                "consumer": consumer,
                "consumer_epoch": str(consumer_epoch),
                "session_lock_token": session_lock_token,
            },
        )

    def defer(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        consumer: str,
        consumer_epoch: int,
        lease_token: str,
        reason: str,
    ) -> dict[str, Any]:
        operation = _settlement("defer", consumer, consumer_epoch, lease_token)
        _required(reason, "Queue defer reason")
        operation["reason"] = reason
        return self._mutate(queue, shard, idempotency_key, operation)

    def receive_deferred(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        message_id: str,
        consumer: str,
        consumer_epoch: int,
        *,
        visibility_timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        _required(message_id, "Queue message ID")
        _consumer(consumer, consumer_epoch)
        operation: dict[str, Any] = {
            "kind": "receive_deferred",
            "partition": 0,
            "message_id": message_id,
            "consumer": consumer,
            "consumer_epoch": str(consumer_epoch),
        }
        if visibility_timeout_ms is not None:
            _positive(visibility_timeout_ms, "Queue visibility timeout")
            operation["visibility_timeout_ms"] = str(visibility_timeout_ms)
        return self._mutate(queue, shard, idempotency_key, operation)

    def acknowledge(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        consumer: str,
        consumer_epoch: int,
        lease_token: str,
    ) -> dict[str, Any]:
        return self._mutate(
            queue,
            shard,
            idempotency_key,
            _settlement("acknowledge", consumer, consumer_epoch, lease_token),
        )

    def extend_lease(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        consumer: str,
        consumer_epoch: int,
        lease_token: str,
        extension_ms: int,
    ) -> dict[str, Any]:
        _positive(extension_ms, "Queue lease extension")
        operation = _settlement("extend_lease", consumer, consumer_epoch, lease_token)
        operation["extension_ms"] = str(extension_ms)
        return self._mutate(queue, shard, idempotency_key, operation)

    def release(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        consumer: str,
        consumer_epoch: int,
        lease_token: str,
        delay_ms: int,
        reason: str = "",
    ) -> dict[str, Any]:
        if isinstance(delay_ms, bool) or delay_ms < 0:
            raise ValueError("Queue release delay must be non-negative")
        operation = _settlement("release", consumer, consumer_epoch, lease_token)
        operation["delay_ms"] = str(delay_ms)
        if reason.strip():
            operation["reason"] = reason
        return self._mutate(queue, shard, idempotency_key, operation)

    def nack(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        consumer: str,
        consumer_epoch: int,
        lease_token: str,
        reason: str,
    ) -> dict[str, Any]:
        return self._disposition(
            queue, shard, idempotency_key, "nack", consumer, consumer_epoch, lease_token, reason
        )

    def reject(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        consumer: str,
        consumer_epoch: int,
        lease_token: str,
        reason: str,
    ) -> dict[str, Any]:
        return self._disposition(
            queue, shard, idempotency_key, "reject", consumer, consumer_epoch, lease_token, reason
        )

    def _disposition(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        kind: str,
        consumer: str,
        consumer_epoch: int,
        lease_token: str,
        reason: str,
    ) -> dict[str, Any]:
        _required(reason, "Queue disposition reason")
        operation = _settlement(kind, consumer, consumer_epoch, lease_token)
        operation["reason"] = reason
        return self._mutate(queue, shard, idempotency_key, operation)

    def redrive(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        message_id: str,
        dead_letter_history_id: int,
    ) -> dict[str, Any]:
        _required(message_id, "Queue message ID")
        _positive(dead_letter_history_id, "Queue dead-letter history ID")
        return self._mutate(
            queue,
            shard,
            idempotency_key,
            {
                "kind": "redrive",
                "partition": 0,
                "message_id": message_id,
                "dead_letter_history_id": str(dead_letter_history_id),
            },
        )

    def maintain(self, queue: str, shard: int, idempotency_key: str) -> dict[str, Any]:
        return self._mutate(queue, shard, idempotency_key, {"kind": "maintain", "partition": 0})

    def mutation(self, queue: str, shard: int, proposal_id: int) -> dict[str, Any]:
        _positive(proposal_id, "Queue proposal ID")
        return self._read(queue, shard, f"/mutations/{proposal_id}")

    def counts(self, queue: str, shard: int) -> dict[str, Any]:
        return self._read(queue, shard, "/counts")

    def dead_letters(self, queue: str, shard: int, *, limit: int = 100) -> dict[str, Any]:
        return self._history(queue, shard, "/dead-letters", limit)

    def redrives(self, queue: str, shard: int, *, limit: int = 100) -> dict[str, Any]:
        return self._history(queue, shard, "/redrives", limit)

    def consumer_flow(self, queue: str, shard: int, consumer: str) -> dict[str, Any]:
        return self._read(queue, shard, f"/consumers/{_segment(consumer, 'Queue consumer')}/flow")

    def advanced_status(self, queue: str, shard: int) -> dict[str, Any]:
        return self._read(queue, shard, "/advanced")

    def correlation(self, queue: str, shard: int, correlation_id: str) -> dict[str, Any]:
        return self._read(
            queue,
            shard,
            f"/correlations/{_segment(correlation_id, 'Queue correlation ID')}",
        )

    def dead_letter_forwards(self, queue: str, shard: int, *, limit: int = 100) -> dict[str, Any]:
        return self._history(queue, shard, "/dead-letter-forwards", limit)

    def status(self, queue: str, shard: int) -> dict[str, Any]:
        return self._read(queue, shard, "/status")

    def _history(self, queue: str, shard: int, path: str, limit: int) -> dict[str, Any]:
        _bounded(limit, 1, _MAX_HISTORY, "Queue history limit")
        return self._read(queue, shard, path, {"limit": limit})

    def _read(
        self, queue: str, shard: int, path: str, query: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return self.call(
            "queues",
            "Queue",
            queue,
            shard,
            lambda _route: ("GET", path, None, query, _LINEARIZABLE),
        )

    def _mutate(
        self,
        queue: str,
        shard: int,
        idempotency_key: str,
        operation: dict[str, Any],
    ) -> dict[str, Any]:
        _required(idempotency_key, "idempotency key")
        return self.call(
            "queues",
            "Queue",
            queue,
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


def _settlement(kind: str, consumer: str, consumer_epoch: int, lease_token: str) -> dict[str, Any]:
    _consumer(consumer, consumer_epoch)
    _required(lease_token, "Queue lease token")
    return {
        "kind": kind,
        "partition": 0,
        "consumer": consumer,
        "consumer_epoch": str(consumer_epoch),
        "lease_token": lease_token,
    }


def _consumer(consumer: str, consumer_epoch: int) -> None:
    _required(consumer, "Queue consumer")
    _positive(consumer_epoch, "Queue consumer epoch")


def _bounded(value: int, minimum: int, maximum: int, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{label} must be between {minimum} and {maximum}")


__all__ = ["RegionalQueueClient"]
