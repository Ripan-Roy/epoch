"""Official Python client for the Epoch real-time data platform."""

from .client import EpochClient
from .errors import EpochAPIError
from .models import (
    DeliveryPolicy,
    DeliveryRateLimit,
    DeliveryRetryPolicy,
    DestinationAuth,
    DurabilityProfile,
    EventEnvelope,
    EventFilter,
    EventTransform,
    Subscription,
    SubscriptionTarget,
    TransformLimits,
)
from .regional import (
    RegionalScope,
    RegionalStreamClient,
    StreamBatchFrame,
    StreamBatchRecord,
    StreamCaptureFormat,
    StreamCompression,
    StreamConsumerMode,
    StreamOffsetCommit,
    StreamReadIsolation,
    StreamReplicationBatch,
    StreamReplicationRecord,
    StreamRetentionPolicy,
    StreamSuperstreamMember,
    stream_shard_for,
)
from .regional_bus import RegionalBusClient
from .regional_cache import (
    RegionalCacheClient,
    RegionalCacheExpectation,
    RegionalCacheLockGuard,
    RegionalCacheMultiplexMutation,
    RegionalCacheMutation,
    RegionalCacheTransform,
    RegionalCacheValue,
)
from .regional_queue import RegionalQueueClient
from .transport import Transport, UrllibTransport
from .webhook import WebhookVerification, verify_webhook_signature

__all__ = [
    "DeliveryPolicy",
    "DeliveryRateLimit",
    "DeliveryRetryPolicy",
    "DestinationAuth",
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
    "RegionalCacheMultiplexMutation",
    "RegionalCacheMutation",
    "RegionalCacheTransform",
    "RegionalCacheValue",
    "RegionalQueueClient",
    "RegionalScope",
    "RegionalStreamClient",
    "StreamBatchFrame",
    "StreamBatchRecord",
    "StreamCaptureFormat",
    "StreamCompression",
    "StreamConsumerMode",
    "StreamOffsetCommit",
    "StreamReadIsolation",
    "StreamReplicationBatch",
    "StreamReplicationRecord",
    "StreamRetentionPolicy",
    "StreamSuperstreamMember",
    "Subscription",
    "SubscriptionTarget",
    "TransformLimits",
    "Transport",
    "UrllibTransport",
    "WebhookVerification",
    "stream_shard_for",
    "verify_webhook_signature",
]
