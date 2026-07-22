"""Stable client-side models independent of the HTTP implementation."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(slots=True)
class EventEnvelope:
    """Common record envelope accepted by all four workload profiles."""

    source: str
    event_type: str
    payload: Any
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    time_ms: int = field(default_factory=lambda: time.time_ns() // 1_000_000)
    subject: str | None = None
    key: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    content_type: str = "application/json"
    schema_ref: str | None = None
    traceparent: str | None = None
    deliver_at_ms: int | None = None
    ttl_ms: int | None = None
    priority: int = 0
    dedupe_id: str | None = None
    transaction_id: str | None = None
    extensions: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.source.strip():
            raise ValueError("event source is required")
        if not self.event_type.strip():
            raise ValueError("event type is required")
        if not self.id.strip():
            raise ValueError("event id is required")
        if not 0 <= self.priority <= 9:
            raise ValueError("priority must be between 0 and 9")

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON-compatible native API representation."""

        value: dict[str, Any] = {
            "id": self.id,
            "source": self.source,
            "type": self.event_type,
            "time_ms": self.time_ms,
            "headers": dict(self.headers),
            "content_type": self.content_type,
            "payload": self.payload,
            "priority": self.priority,
            "extensions": dict(self.extensions),
        }
        optional = {
            "subject": self.subject,
            "key": self.key,
            "schema_ref": self.schema_ref,
            "traceparent": self.traceparent,
            "deliver_at_ms": self.deliver_at_ms,
            "ttl_ms": self.ttl_ms,
            "dedupe_id": self.dedupe_id,
            "transaction_id": self.transaction_id,
        }
        value.update({key: item for key, item in optional.items() if item is not None})
        return value


@dataclass(slots=True)
class EventFilter:
    """Event Bus subscription filter using the native matching vocabulary."""

    event_type_patterns: list[str] = field(default_factory=list)
    source_patterns: list[str] = field(default_factory=list)
    subject_patterns: list[str] = field(default_factory=list)
    headers: dict[str, str] = field(default_factory=dict)
    json_equals: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type_patterns": list(self.event_type_patterns),
            "source_patterns": list(self.source_patterns),
            "subject_patterns": list(self.subject_patterns),
            "headers": dict(self.headers),
            "json_equals": dict(self.json_equals),
        }


TargetKind = Literal["pull", "queue", "stream", "webhook", "http"]


@dataclass(frozen=True, slots=True)
class SubscriptionTarget:
    """Typed Event Bus delivery target."""

    kind: TargetKind
    resource: str | None = None
    url: str | None = None

    def __post_init__(self) -> None:
        if self.kind in {"queue", "stream"}:
            if not self.resource or self.url is not None:
                raise ValueError(f"{self.kind} targets require only a resource")
        elif self.kind in {"webhook", "http"}:
            if not self.url or self.resource is not None:
                raise ValueError(f"{self.kind} targets require only a URL")
        elif self.kind == "pull":
            if self.resource is not None or self.url is not None:
                raise ValueError("pull targets do not accept a resource or URL")
        else:
            raise ValueError(f"unsupported subscription target: {self.kind}")

    @classmethod
    def pull(cls) -> SubscriptionTarget:
        return cls("pull")

    @classmethod
    def queue(cls, resource: str) -> SubscriptionTarget:
        return cls("queue", resource=resource)

    @classmethod
    def stream(cls, resource: str) -> SubscriptionTarget:
        return cls("stream", resource=resource)

    @classmethod
    def webhook(cls, url: str) -> SubscriptionTarget:
        return cls("webhook", url=url)

    @classmethod
    def http(cls, url: str) -> SubscriptionTarget:
        return cls("http", url=url)

    def to_dict(self) -> dict[str, str]:
        value = {"kind": self.kind}
        if self.resource is not None:
            value["resource"] = self.resource
        if self.url is not None:
            value["url"] = self.url
        return value


@dataclass(slots=True)
class EventTransform:
    """Deterministic Event Bus header and payload projection transform."""

    add_headers: dict[str, str] = field(default_factory=dict)
    payload_projection: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "add_headers": dict(self.add_headers),
            "payload_projection": dict(self.payload_projection),
        }


@dataclass(slots=True)
class Subscription:
    """Typed Event Bus subscription definition."""

    name: str
    target: SubscriptionTarget
    filter: EventFilter = field(default_factory=EventFilter)
    transform: EventTransform = field(default_factory=EventTransform)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("subscription name is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "filter": self.filter.to_dict(),
            "target": self.target.to_dict(),
            "transform": self.transform.to_dict(),
        }
