"""Guarantee-aware high-level client for Epoch's native HTTP surface."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from .models import DurabilityProfile, EventEnvelope, Subscription
from .transport import Transport, UrllibTransport


class EpochClient:
    """Synchronous client for one Epoch node.

    The transport is injected so domain behavior remains testable without a
    network and future gRPC transports can satisfy the same narrow interface.
    """

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:7601",
        *,
        timeout: float = 10.0,
        transport: Transport | None = None,
    ) -> None:
        self._transport = transport or UrllibTransport(base_url, timeout=timeout)

    def health(self) -> dict[str, Any]:
        return self._transport.request("GET", "/healthz")

    def resources(self) -> list[dict[str, Any]]:
        return self._transport.request("GET", "/v1/resources")

    def create_cache(
        self,
        name: str,
        *,
        max_entries: int = 10_000,
        default_ttl_ms: int | None = None,
        eviction: str = "no_eviction",
    ) -> dict[str, Any]:
        return self._create(
            "caches",
            name,
            {
                "max_entries": max_entries,
                "default_ttl_ms": default_ttl_ms,
                "eviction": eviction,
                "durability": "volatile",
            },
        )

    def create_stream(
        self,
        name: str,
        *,
        partitions: int = 1,
        durability: DurabilityProfile = "volatile",
        max_records_per_partition: int | None = None,
    ) -> dict[str, Any]:
        return self._create(
            "streams",
            name,
            {
                "partitions": partitions,
                "durability": durability,
                "max_records_per_partition": max_records_per_partition,
            },
        )

    def create_queue(
        self,
        name: str,
        *,
        durability: DurabilityProfile = "volatile",
        visibility_timeout_ms: int = 30_000,
        max_messages: int = 100_000,
        max_attempts: int = 8,
    ) -> dict[str, Any]:
        return self._create(
            "queues",
            name,
            {
                "durability": durability,
                "visibility_timeout_ms": visibility_timeout_ms,
                "max_messages": max_messages,
                "retry": {
                    "strategy": "exponential",
                    "initial_delay_ms": 1_000,
                    "max_delay_ms": 60_000,
                    "jitter_percent": 10,
                    "max_attempts": max_attempts,
                    "max_age_ms": None,
                },
                "dedupe_window_ms": None,
            },
        )

    def create_bus(self, name: str, *, archive: bool = True) -> dict[str, Any]:
        return self._create("buses", name, {"durability": "volatile", "archive": archive})

    def cache_set(
        self,
        cache: str,
        key: str,
        value: str,
        *,
        ttl_ms: int | None = None,
        expected_version: int | None = None,
        only_if_absent: bool = False,
        only_if_present: bool = False,
    ) -> dict[str, Any]:
        return self._transport.request(
            "PUT",
            f"/v1/caches/{_segment(cache)}/keys/{_segment(key)}",
            body={
                "value": {"kind": "string", "value": value},
                "ttl_ms": ttl_ms,
                "expected_version": expected_version,
                "only_if_absent": only_if_absent,
                "only_if_present": only_if_present,
            },
        )

    def cache_get(self, cache: str, key: str) -> dict[str, Any]:
        return self._transport.request("GET", f"/v1/caches/{_segment(cache)}/keys/{_segment(key)}")

    def cache_delete(self, cache: str, key: str) -> None:
        return self._transport.request(
            "DELETE", f"/v1/caches/{_segment(cache)}/keys/{_segment(key)}"
        )

    def cache_increment(self, cache: str, key: str, *, delta: int = 1) -> dict[str, int]:
        return self._transport.request(
            "POST",
            f"/v1/caches/{_segment(cache)}/keys/{_segment(key)}/increment",
            body={"delta": delta},
        )

    def append_stream(
        self,
        stream: str,
        event: EventEnvelope,
        *,
        partition: int | None = None,
    ) -> dict[str, Any]:
        return self._transport.request(
            "POST",
            f"/v1/streams/{_segment(stream)}/records",
            body={"envelope": event.to_dict(), "partition": partition},
        )

    def fetch_stream(
        self,
        stream: str,
        *,
        partition: int = 0,
        offset: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return self._transport.request(
            "GET",
            f"/v1/streams/{_segment(stream)}/records",
            query={"partition": partition, "offset": offset, "limit": limit},
        )

    def commit_stream_offset(
        self,
        stream: str,
        group: str,
        *,
        partition: int,
        next_offset: int,
        reset: bool = False,
    ) -> None:
        return self._transport.request(
            "PUT",
            f"/v1/streams/{_segment(stream)}/groups/{_segment(group)}/offsets",
            body={"partition": partition, "next_offset": next_offset, "reset": reset},
        )

    def stream_lag(self, stream: str, group: str, *, partition: int = 0) -> dict[str, Any]:
        return self._transport.request(
            "GET",
            f"/v1/streams/{_segment(stream)}/groups/{_segment(group)}/lag",
            query={"partition": partition},
        )

    def send(self, queue: str, event: EventEnvelope) -> dict[str, Any]:
        return self._transport.request(
            "POST", f"/v1/queues/{_segment(queue)}/messages", body=event.to_dict()
        )

    def receive(
        self,
        queue: str,
        *,
        consumer: str,
        max_messages: int = 1,
        visibility_timeout_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._transport.request(
            "POST",
            f"/v1/queues/{_segment(queue)}/acquire",
            body={
                "consumer": consumer,
                "max_messages": max_messages,
                "visibility_timeout_ms": visibility_timeout_ms,
            },
        )

    def acknowledge(self, queue: str, lease_token: str) -> dict[str, Any]:
        return self._settle(queue, {"action": "ack", "token": lease_token})

    def release(
        self,
        queue: str,
        lease_token: str,
        *,
        delay_ms: int = 0,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return self._settle(
            queue,
            {
                "action": "release",
                "token": lease_token,
                "delay_ms": delay_ms,
                "reason": reason,
            },
        )

    def reject(self, queue: str, lease_token: str, *, reason: str) -> dict[str, bool]:
        return self._settle(
            queue,
            {"action": "reject", "token": lease_token, "reason": reason},
        )

    def extend_lease(self, queue: str, lease_token: str, *, extension_ms: int) -> dict[str, int]:
        return self._settle(
            queue,
            {
                "action": "extend",
                "token": lease_token,
                "extension_ms": extension_ms,
            },
        )

    def queue_counts(self, queue: str) -> dict[str, int]:
        return self._transport.request("GET", f"/v1/queues/{_segment(queue)}/counts")

    def redrive(self, queue: str, message_id: str) -> None:
        return self._transport.request(
            "POST",
            f"/v1/queues/{_segment(queue)}/dead-letters/{_segment(message_id)}/redrive",
        )

    def publish(self, bus: str, event: EventEnvelope) -> dict[str, Any]:
        return self._transport.request(
            "POST", f"/v1/buses/{_segment(bus)}/events", body=event.to_dict()
        )

    def upsert_subscription(self, bus: str, subscription: Subscription) -> dict[str, int]:
        return self._transport.request(
            "PUT",
            f"/v1/buses/{_segment(bus)}/subscriptions/{_segment(subscription.name)}",
            body=subscription.to_dict(),
        )

    def remove_subscription(self, bus: str, subscription: str) -> None:
        return self._transport.request(
            "DELETE",
            f"/v1/buses/{_segment(bus)}/subscriptions/{_segment(subscription)}",
        )

    def replay_bus(
        self,
        bus: str,
        *,
        from_ms: int = 0,
        to_ms: int = (1 << 64) - 1,
        limit: int = 100,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._transport.request(
            "GET",
            f"/v1/buses/{_segment(bus)}/replay",
            query={
                "from_ms": from_ms,
                "to_ms": to_ms,
                "limit": limit,
                "event_type": event_type,
            },
        )

    def _create(self, collection: str, name: str, config: dict[str, Any]) -> dict[str, Any]:
        return self._transport.request("POST", f"/v1/{collection}/{_segment(name)}", body=config)

    def _settle(self, queue: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._transport.request("POST", f"/v1/queues/{_segment(queue)}/settle", body=body)


def _segment(value: str) -> str:
    if not value:
        raise ValueError("resource name or key cannot be empty")
    return quote(value, safe="")
