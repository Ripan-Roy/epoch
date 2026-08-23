"""Authenticated leader- and fence-aware regional Event Bus client."""

from __future__ import annotations

from typing import Any, Literal

from ._regional import RegionalClient, Route, _non_negative, _positive, _required
from .models import EventEnvelope, EventFilter, Subscription

DeliveryState = Literal["pending", "in_flight", "acknowledged", "dead_lettered"]
_DELIVERY_STATES = frozenset({"pending", "in_flight", "acknowledged", "dead_lettered"})
_MAX_DELIVERY_BATCH = 100
_MAX_READ_RESULTS = 10_000
_MAX_LONG_POLL_MS = 30_000
_LINEARIZABLE = {"x-epoch-read-consistency": "linearizable"}


class RegionalBusClient(RegionalClient):
    """Complete replicated Event Bus route, archive, and delivery-ledger client."""

    def upsert_subscription(
        self,
        bus: str,
        shard: int,
        idempotency_key: str,
        subscription: Subscription,
    ) -> dict[str, Any]:
        if not isinstance(subscription, Subscription):
            raise TypeError("subscription must be Subscription")
        return self._mutate(
            bus,
            shard,
            idempotency_key,
            {
                "kind": "upsert_subscription",
                "subscription": subscription.to_dict(),
            },
        )

    def remove_subscription(
        self, bus: str, shard: int, idempotency_key: str, name: str
    ) -> dict[str, Any]:
        _required(name, "subscription name")
        return self._mutate(
            bus,
            shard,
            idempotency_key,
            {"kind": "remove_subscription", "name": name},
        )

    def publish(
        self,
        bus: str,
        shard: int,
        idempotency_key: str,
        event: EventEnvelope,
    ) -> dict[str, Any]:
        if not isinstance(event, EventEnvelope):
            raise TypeError("event must be EventEnvelope")
        return self._mutate(
            bus,
            shard,
            idempotency_key,
            {"kind": "publish", "envelope": event.to_dict()},
        )

    def acquire_deliveries(
        self,
        bus: str,
        shard: int,
        idempotency_key: str,
        *,
        subscription: str,
        dispatcher: str,
        dispatcher_epoch: int,
        max_deliveries: int,
        wait_ms: int = 0,
    ) -> dict[str, Any]:
        _required(subscription, "subscription name")
        _required(dispatcher, "dispatcher")
        _positive(dispatcher_epoch, "dispatcher epoch")
        _delivery_batch(max_deliveries)
        _non_negative(wait_ms, "delivery wait")
        if wait_ms > _MAX_LONG_POLL_MS:
            raise ValueError(f"delivery wait must not exceed {_MAX_LONG_POLL_MS} milliseconds")
        return self._mutate(
            bus,
            shard,
            idempotency_key,
            {
                "kind": "acquire_deliveries",
                "subscription": subscription,
                "dispatcher": dispatcher,
                "dispatcher_epoch": str(dispatcher_epoch),
                "max_deliveries": max_deliveries,
                "wait_ms": wait_ms,
            },
        )

    def acknowledge_delivery(
        self,
        bus: str,
        shard: int,
        idempotency_key: str,
        delivery_id: str,
        dispatcher: str,
        dispatcher_epoch: int,
        lease_token: str,
    ) -> dict[str, Any]:
        return self._mutate(
            bus,
            shard,
            idempotency_key,
            _settlement(
                "acknowledge_delivery",
                delivery_id,
                dispatcher,
                dispatcher_epoch,
                lease_token,
            ),
        )

    def fail_delivery(
        self,
        bus: str,
        shard: int,
        idempotency_key: str,
        delivery_id: str,
        dispatcher: str,
        dispatcher_epoch: int,
        lease_token: str,
        reason: str,
    ) -> dict[str, Any]:
        _required(reason, "delivery failure reason")
        operation = _settlement(
            "fail_delivery", delivery_id, dispatcher, dispatcher_epoch, lease_token
        )
        operation["reason"] = reason
        return self._mutate(bus, shard, idempotency_key, operation)

    def reject_delivery(
        self,
        bus: str,
        shard: int,
        idempotency_key: str,
        delivery_id: str,
        dispatcher: str,
        dispatcher_epoch: int,
        lease_token: str,
        reason: str,
    ) -> dict[str, Any]:
        _required(reason, "delivery rejection reason")
        operation = _settlement(
            "reject_delivery", delivery_id, dispatcher, dispatcher_epoch, lease_token
        )
        operation["reason"] = reason
        return self._mutate(bus, shard, idempotency_key, operation)

    def redrive_delivery(
        self,
        bus: str,
        shard: int,
        idempotency_key: str,
        delivery_id: str,
    ) -> dict[str, Any]:
        """Return one dead-lettered delivery to pending with preserved history."""
        _required(delivery_id, "delivery ID")
        return self._mutate(
            bus,
            shard,
            idempotency_key,
            {"kind": "redrive_delivery", "delivery_id": delivery_id},
        )

    def maintain_deliveries(
        self,
        bus: str,
        shard: int,
        idempotency_key: str,
        *,
        max_deliveries: int = _MAX_DELIVERY_BATCH,
    ) -> dict[str, Any]:
        _delivery_batch(max_deliveries)
        return self._mutate(
            bus,
            shard,
            idempotency_key,
            {"kind": "maintain_deliveries", "max_deliveries": max_deliveries},
        )

    def maintain_archive(
        self,
        bus: str,
        shard: int,
        idempotency_key: str,
        *,
        max_events: int = _MAX_READ_RESULTS,
    ) -> dict[str, Any]:
        """Apply bounded replicated archive age/count retention immediately."""
        _read_limit(max_events)
        return self._mutate(
            bus,
            shard,
            idempotency_key,
            {"kind": "maintain_archive", "max_events": max_events},
        )

    def apply_integration(
        self,
        bus: str,
        shard: int,
        idempotency_key: str,
        operation: dict[str, Any],
    ) -> dict[str, Any]:
        """Commit one schema, connector, MQTT, catalog, enrichment, or endpoint operation."""
        if not isinstance(operation, dict) or not isinstance(operation.get("kind"), str):
            raise ValueError("integration operation kind is required")
        _required(operation["kind"], "integration operation kind")
        return self._mutate(
            bus,
            shard,
            idempotency_key,
            {"kind": "apply_integration", "operation": dict(operation)},
        )

    def mutation(self, bus: str, shard: int, proposal_id: int) -> dict[str, Any]:
        _positive(proposal_id, "Event Bus proposal ID")
        return self._read(bus, shard, "GET", f"/mutations/{proposal_id}")

    def replay_archive(
        self,
        bus: str,
        shard: int,
        *,
        from_ms: int,
        to_ms: int,
        limit: int = 100,
        filter: EventFilter | None = None,
    ) -> dict[str, Any]:
        _non_negative(from_ms, "Event Bus replay from time")
        _non_negative(to_ms, "Event Bus replay to time")
        if from_ms > to_ms:
            raise ValueError("Event Bus replay from time must not exceed to time")
        _read_limit(limit)
        body: dict[str, Any] = {
            "from_ms": str(from_ms),
            "to_ms": str(to_ms),
            "limit": limit,
        }
        if filter is not None:
            if not isinstance(filter, EventFilter):
                raise TypeError("Event Bus replay filter must be EventFilter")
            body["filter"] = filter.to_dict()
        return self._read(bus, shard, "POST", "/archive/replay", body)

    def query_deliveries(
        self,
        bus: str,
        shard: int,
        *,
        subscription: str | None = None,
        state: DeliveryState | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        _read_limit(limit)
        body: dict[str, Any] = {"limit": limit}
        if subscription is not None:
            _required(subscription, "subscription name")
            body["subscription"] = subscription
        if state is not None:
            if state not in _DELIVERY_STATES:
                raise ValueError(f"unsupported Event Bus delivery state: {state}")
            body["state"] = state
        return self._read(bus, shard, "POST", "/deliveries/query", body)

    def status(self, bus: str, shard: int) -> dict[str, Any]:
        return self._read(bus, shard, "GET", "/status")

    def integration_state(self, bus: str, shard: int) -> dict[str, Any]:
        """Return the complete linearizable Event Bus integration state."""
        return self._read(bus, shard, "GET", "/integration/state")

    def _mutate(
        self, bus: str, shard: int, idempotency_key: str, operation: dict[str, Any]
    ) -> dict[str, Any]:
        _required(idempotency_key, "idempotency key")
        return self.call(
            "buses",
            "Event Bus",
            bus,
            shard,
            lambda route: _mutation_request(route, idempotency_key, operation),
        )

    def _read(
        self,
        bus: str,
        shard: int,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.call(
            "buses",
            "Event Bus",
            bus,
            shard,
            lambda _route: (method, path, body, None, _LINEARIZABLE),
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


def _settlement(
    kind: str,
    delivery_id: str,
    dispatcher: str,
    dispatcher_epoch: int,
    lease_token: str,
) -> dict[str, Any]:
    _required(delivery_id, "delivery ID")
    _required(dispatcher, "dispatcher")
    _positive(dispatcher_epoch, "dispatcher epoch")
    _required(lease_token, "delivery lease token")
    return {
        "kind": kind,
        "delivery_id": delivery_id,
        "dispatcher": dispatcher,
        "dispatcher_epoch": str(dispatcher_epoch),
        "lease_token": lease_token,
    }


def _delivery_batch(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 100:
        raise ValueError("Event Bus max deliveries must be between 1 and 100")


def _read_limit(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 10_000:
        raise ValueError("Event Bus read limit must be between 1 and 10000")


__all__ = ["DeliveryState", "RegionalBusClient"]
