"""Stable client-side models independent of the HTTP implementation."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import urlsplit


def _valid_resource_name(value: str | None) -> bool:
    return bool(
        value
        and 1 <= len(value.encode()) <= 128
        and value.isascii()
        and all(character.isalnum() or character in "-_." for character in value)
    )


def _valid_http_url(value: str | None) -> bool:
    if not value:
        return False
    try:
        parsed = urlsplit(value)
        return (
            parsed.scheme in {"http", "https"}
            and parsed.hostname is not None
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
        )
    except ValueError:
        return False


DurabilityProfile = Literal[
    "volatile",
    "replicated_memory",
    "local_durable",
    "quorum_durable",
    "geo_async",
    "geo_sync",
]


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

    topic_patterns: list[str] = field(default_factory=list)
    event_type_patterns: list[str] = field(default_factory=list)
    source_patterns: list[str] = field(default_factory=list)
    subject_patterns: list[str] = field(default_factory=list)
    headers: dict[str, str] = field(default_factory=dict)
    json_equals: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic_patterns": list(self.topic_patterns),
            "event_type_patterns": list(self.event_type_patterns),
            "source_patterns": list(self.source_patterns),
            "subject_patterns": list(self.subject_patterns),
            "headers": dict(self.headers),
            "json_equals": dict(self.json_equals),
        }


TargetKind = Literal[
    "pull",
    "queue",
    "stream",
    "webhook",
    "http",
    "api_destination",
    "endpoint_pool",
    "function",
    "connector",
]
DeliveryBackoffStrategy = Literal["exponential", "fixed"]


@dataclass(frozen=True, slots=True)
class DeliveryRetryPolicy:
    """Bounded deterministic Event Bus delivery retry policy."""

    strategy: DeliveryBackoffStrategy = "exponential"
    initial_delay_ms: int = 1_000
    max_delay_ms: int = 60_000
    jitter_percent: int = 10
    max_attempts: int = 8
    max_age_ms: int | None = None

    def __post_init__(self) -> None:
        if self.strategy not in {"exponential", "fixed"}:
            raise ValueError(f"unsupported delivery backoff strategy: {self.strategy}")
        for label, value in (
            ("initial delay", self.initial_delay_ms),
            ("max delay", self.max_delay_ms),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 0 <= value <= 604_800_000
            ):
                raise ValueError(f"delivery retry {label} must be between 0 and 604800000")
        if self.initial_delay_ms > self.max_delay_ms:
            raise ValueError("delivery retry initial delay must not exceed max delay")
        if isinstance(self.jitter_percent, bool) or not 0 <= self.jitter_percent <= 100:
            raise ValueError("delivery retry jitter percent must be between 0 and 100")
        if isinstance(self.max_attempts, bool) or not 1 <= self.max_attempts <= 100:
            raise ValueError("delivery retry max attempts must be between 1 and 100")
        if self.max_age_ms is not None and (
            isinstance(self.max_age_ms, bool)
            or not isinstance(self.max_age_ms, int)
            or self.max_age_ms <= 0
        ):
            raise ValueError("delivery retry max age must be positive when provided")

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy,
            "initial_delay_ms": self.initial_delay_ms,
            "max_delay_ms": self.max_delay_ms,
            "jitter_percent": self.jitter_percent,
            "max_attempts": self.max_attempts,
            "max_age_ms": self.max_age_ms,
        }


@dataclass(frozen=True, slots=True)
class DeliveryRateLimit:
    """Per-subscription committed delivery rate and burst."""

    deliveries_per_second: int
    burst: int

    def __post_init__(self) -> None:
        for label, value in (
            ("deliveries per second", self.deliveries_per_second),
            ("burst", self.burst),
        ):
            if isinstance(value, bool) or not 1 <= value <= 1_000_000:
                raise ValueError(f"delivery {label} must be between 1 and 1000000")

    def to_dict(self) -> dict[str, int]:
        return {
            "deliveries_per_second": self.deliveries_per_second,
            "burst": self.burst,
        }


@dataclass(frozen=True, slots=True)
class DeliveryPolicy:
    """One subscription's timeout, concurrency, and retry bounds."""

    timeout_ms: int = 30_000
    max_in_flight: int = 16
    retry: DeliveryRetryPolicy = field(default_factory=DeliveryRetryPolicy)
    rate_limit: DeliveryRateLimit | None = None
    dead_letter_retention_ms: int | None = None

    def __post_init__(self) -> None:
        if isinstance(self.timeout_ms, bool) or not 1 <= self.timeout_ms <= 604_800_000:
            raise ValueError("delivery timeout must be between 1 and 604800000")
        if isinstance(self.max_in_flight, bool) or not 1 <= self.max_in_flight <= 1_000:
            raise ValueError("delivery max in flight must be between 1 and 1000")
        if not isinstance(self.retry, DeliveryRetryPolicy):
            raise TypeError("delivery retry must be DeliveryRetryPolicy")
        if self.rate_limit is not None and not isinstance(self.rate_limit, DeliveryRateLimit):
            raise TypeError("delivery rate limit must be DeliveryRateLimit")
        if self.dead_letter_retention_ms is not None and (
            isinstance(self.dead_letter_retention_ms, bool)
            or not 1 <= self.dead_letter_retention_ms <= 31_536_000_000
        ):
            raise ValueError("dead-letter retention must be between 1 and 31536000000 milliseconds")

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "timeout_ms": self.timeout_ms,
            "max_in_flight": self.max_in_flight,
            "retry": self.retry.to_dict(),
        }
        if self.rate_limit is not None:
            value["rate_limit"] = self.rate_limit.to_dict()
        if self.dead_letter_retention_ms is not None:
            value["dead_letter_retention_ms"] = self.dead_letter_retention_ms
        return value


@dataclass(frozen=True, slots=True)
class DestinationAuth:
    """Rotatable API destination credential reference; never a secret value."""

    kind: Literal["none", "api_key", "oauth2"]
    secret_ref: str | None = None
    header: str | None = None
    token_url: str | None = None
    scopes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.kind == "none":
            if (
                any(value is not None for value in (self.secret_ref, self.header, self.token_url))
                or self.scopes
            ):
                raise ValueError("none destination auth cannot carry credential fields")
        elif self.kind == "api_key":
            if (
                not _valid_resource_name(self.secret_ref)
                or not self.header
                or len(self.header.encode()) > 256
                or self.token_url is not None
                or self.scopes
            ):
                raise ValueError("API-key auth requires a secret reference and header")
        elif self.kind == "oauth2":
            if (
                not _valid_resource_name(self.secret_ref)
                or not _valid_http_url(self.token_url)
                or self.header is not None
                or len(self.scopes) > 64
                or any(not scope or len(scope.encode()) > 4 * 1024 for scope in self.scopes)
            ):
                raise ValueError(
                    "OAuth2 auth requires a secret reference, HTTP(S) token URL, and bounded scopes"
                )
        else:
            raise ValueError(f"unsupported destination auth: {self.kind}")

    @classmethod
    def none(cls) -> DestinationAuth:
        return cls("none")

    @classmethod
    def api_key(cls, secret_ref: str, header: str) -> DestinationAuth:
        return cls("api_key", secret_ref=secret_ref, header=header)

    @classmethod
    def oauth2(
        cls, secret_ref: str, token_url: str, scopes: tuple[str, ...] = ()
    ) -> DestinationAuth:
        return cls("oauth2", secret_ref=secret_ref, token_url=token_url, scopes=scopes)

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {"kind": self.kind}
        for key, item in (
            ("secret_ref", self.secret_ref),
            ("header", self.header),
            ("token_url", self.token_url),
        ):
            if item is not None:
                value[key] = item
        if self.scopes:
            value["scopes"] = list(self.scopes)
        return value


@dataclass(frozen=True, slots=True)
class SubscriptionTarget:
    """Typed Event Bus delivery target."""

    kind: TargetKind
    resource: str | None = None
    url: str | None = None
    signing_key_id: str | None = None
    pool: str | None = None
    auth: DestinationAuth | None = None
    cloud_events_mode: Literal["binary", "structured"] | None = None

    def __post_init__(self) -> None:
        if self.kind in {"queue", "stream", "function", "connector"}:
            if (
                not self.resource
                or self.url is not None
                or self.signing_key_id is not None
                or self.pool is not None
                or self.auth is not None
            ):
                raise ValueError(f"{self.kind} targets require only a resource")
        elif self.kind in {"webhook", "http"}:
            if (
                not self.url
                or self.resource is not None
                or self.pool is not None
                or self.auth is not None
            ):
                raise ValueError(f"{self.kind} targets require only a URL")
            if self.signing_key_id is not None and (
                not 1 <= len(self.signing_key_id) <= 128
                or not self.signing_key_id.isascii()
                or any(
                    not (character.isalnum() or character in "-_.")
                    for character in self.signing_key_id
                )
            ):
                raise ValueError("signing key ID must be a 1-128 byte resource name")
        elif self.kind == "pull":
            if any(
                item is not None
                for item in (self.resource, self.url, self.signing_key_id, self.pool, self.auth)
            ):
                raise ValueError("pull targets do not accept a resource or URL")
        elif self.kind == "api_destination":
            if (
                not self.url
                or self.auth is None
                or any(item is not None for item in (self.resource, self.pool, self.signing_key_id))
            ):
                raise ValueError("API destination targets require a URL and auth reference")
            if not _valid_http_url(self.url):
                raise ValueError("API destination requires an absolute HTTP(S) URL")
            if self.cloud_events_mode not in {None, "binary", "structured"}:
                raise ValueError("CloudEvents mode must be binary or structured")
        elif self.kind == "endpoint_pool":
            if (
                not self.pool
                or self.auth is None
                or any(item is not None for item in (self.resource, self.url, self.signing_key_id))
            ):
                raise ValueError("endpoint pool targets require a pool and auth reference")
            if self.cloud_events_mode not in {None, "binary", "structured"}:
                raise ValueError("CloudEvents mode must be binary or structured")
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
    def signed_webhook(cls, url: str, signing_key_id: str) -> SubscriptionTarget:
        return cls("webhook", url=url, signing_key_id=signing_key_id)

    @classmethod
    def http(cls, url: str) -> SubscriptionTarget:
        return cls("http", url=url)

    @classmethod
    def signed_http(cls, url: str, signing_key_id: str) -> SubscriptionTarget:
        return cls("http", url=url, signing_key_id=signing_key_id)

    @classmethod
    def api_destination(
        cls,
        url: str,
        auth: DestinationAuth,
        cloud_events_mode: Literal["binary", "structured"] = "binary",
    ) -> SubscriptionTarget:
        return cls("api_destination", url=url, auth=auth, cloud_events_mode=cloud_events_mode)

    @classmethod
    def endpoint_pool(
        cls,
        pool: str,
        auth: DestinationAuth,
        cloud_events_mode: Literal["binary", "structured"] = "binary",
    ) -> SubscriptionTarget:
        return cls("endpoint_pool", pool=pool, auth=auth, cloud_events_mode=cloud_events_mode)

    @classmethod
    def function(cls, resource: str) -> SubscriptionTarget:
        return cls("function", resource=resource)

    @classmethod
    def connector(cls, resource: str) -> SubscriptionTarget:
        return cls("connector", resource=resource)

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {"kind": self.kind}
        if self.resource is not None:
            value["resource"] = self.resource
        if self.url is not None:
            value["url"] = self.url
        if self.signing_key_id is not None:
            value["signing_key_id"] = self.signing_key_id
        if self.pool is not None:
            value["pool"] = self.pool
        if self.auth is not None:
            value["auth"] = self.auth.to_dict()
        if self.cloud_events_mode is not None:
            value["cloud_events_mode"] = self.cloud_events_mode
        return value


@dataclass(frozen=True, slots=True)
class TransformLimits:
    """Deterministic transform CPU, memory, time, and network limits."""

    max_operations: int = 64
    max_output_bytes: int = 256 * 1024
    max_value_bytes: int = 64 * 1024
    timeout_ms: int = 100
    network_access: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.max_operations, bool) or not 1 <= self.max_operations <= 256:
            raise ValueError("transform max operations must be between 1 and 256")
        if isinstance(self.max_output_bytes, bool) or not 1 <= self.max_output_bytes <= 1024 * 1024:
            raise ValueError("transform max output bytes must be between 1 and 1048576")
        if (
            isinstance(self.max_value_bytes, bool)
            or not 1 <= self.max_value_bytes <= 256 * 1024
            or self.max_value_bytes > self.max_output_bytes
        ):
            raise ValueError(
                "transform max value bytes must be positive, bounded, and not exceed output"
            )
        if isinstance(self.timeout_ms, bool) or not 1 <= self.timeout_ms <= 1_000:
            raise ValueError("transform timeout must be between 1 and 1000 milliseconds")
        if self.network_access:
            raise ValueError("deterministic transforms cannot enable network access")

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_operations": self.max_operations,
            "max_output_bytes": self.max_output_bytes,
            "max_value_bytes": self.max_value_bytes,
            "timeout_ms": self.timeout_ms,
            "network_access": self.network_access,
        }


@dataclass(slots=True)
class EventTransform:
    """Deterministic Event Bus header and payload projection transform."""

    add_headers: dict[str, str] = field(default_factory=dict)
    payload_projection: dict[str, str] = field(default_factory=dict)
    rename_fields: dict[str, str] = field(default_factory=dict)
    constants: dict[str, Any] = field(default_factory=dict)
    templates: dict[str, str] = field(default_factory=dict)
    limits: TransformLimits | None = None
    enrichment_ref: str | None = None

    def __post_init__(self) -> None:
        mappings = (
            self.add_headers,
            self.payload_projection,
            self.rename_fields,
            self.constants,
            self.templates,
        )
        if any(len(mapping) > 64 for mapping in mappings):
            raise ValueError("transform mappings cannot exceed 64 entries each")
        limits = self.limits or TransformLimits()
        if sum(len(mapping) for mapping in mappings) > limits.max_operations:
            raise ValueError("transform operations exceed the configured limit")
        if self.enrichment_ref is not None and not _valid_resource_name(self.enrichment_ref):
            raise ValueError("enrichment reference must be a resource name")

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "add_headers": dict(self.add_headers),
            "payload_projection": dict(self.payload_projection),
            "rename_fields": dict(self.rename_fields),
            "constants": dict(self.constants),
            "templates": dict(self.templates),
        }
        if self.limits is not None:
            value["limits"] = self.limits.to_dict()
        if self.enrichment_ref is not None:
            value["enrichment_ref"] = self.enrichment_ref
        return value


@dataclass(slots=True)
class Subscription:
    """Typed Event Bus subscription definition."""

    name: str
    target: SubscriptionTarget
    filter: EventFilter = field(default_factory=EventFilter)
    transform: EventTransform = field(default_factory=EventTransform)
    delivery_policy: DeliveryPolicy = field(default_factory=DeliveryPolicy)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("subscription name is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "filter": self.filter.to_dict(),
            "target": self.target.to_dict(),
            "transform": self.transform.to_dict(),
            "delivery_policy": self.delivery_policy.to_dict(),
        }
