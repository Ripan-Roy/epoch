"""Official Python client for the Epoch real-time data platform."""

from .client import EpochClient
from .errors import EpochAPIError
from .models import (
    DeliveryPolicy,
    DeliveryRetryPolicy,
    DurabilityProfile,
    EventEnvelope,
    EventFilter,
    EventTransform,
    Subscription,
    SubscriptionTarget,
)
from .regional import (
    RegionalScope,
    RegionalStreamClient,
    StreamBatchFrame,
    StreamBatchRecord,
    StreamCompression,
    StreamRetentionPolicy,
    stream_shard_for,
)
from .regional_bus import RegionalBusClient
from .regional_cache import (
    RegionalCacheClient,
    RegionalCacheExpectation,
    RegionalCacheLockGuard,
    RegionalCacheMutation,
    RegionalCacheValue,
)
from .regional_queue import RegionalQueueClient
from .transport import Transport, UrllibTransport
from .webhook import WebhookVerification, verify_webhook_signature

__all__ = [
    "DeliveryPolicy",
    "DeliveryRetryPolicy",
    "DurabilityProfile",
    "EpochAPIError",
    "EpochClient",
    "EventEnvelope",
    "EventFilter",
    "EventTransform",
    "RegionalBusClient",
    "RegionalCacheClient",
    "RegionalCacheExpectation",
    "RegionalCacheLockGuard",
    "RegionalCacheMutation",
    "RegionalCacheValue",
    "RegionalQueueClient",
    "RegionalScope",
    "RegionalStreamClient",
    "StreamBatchFrame",
    "StreamBatchRecord",
    "StreamCompression",
    "StreamRetentionPolicy",
    "Subscription",
    "SubscriptionTarget",
    "Transport",
    "UrllibTransport",
    "WebhookVerification",
    "stream_shard_for",
    "verify_webhook_signature",
]
