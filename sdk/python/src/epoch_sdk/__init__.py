"""Official Python client for the Epoch real-time data platform."""

from .client import EpochClient
from .errors import EpochAPIError
from .models import (
    DurabilityProfile,
    EventEnvelope,
    EventFilter,
    EventTransform,
    Subscription,
    SubscriptionTarget,
)
from .regional import RegionalScope, RegionalStreamClient
from .regional_queue import RegionalQueueClient
from .transport import Transport, UrllibTransport

__all__ = [
    "DurabilityProfile",
    "EpochAPIError",
    "EpochClient",
    "EventEnvelope",
    "EventFilter",
    "EventTransform",
    "RegionalQueueClient",
    "RegionalScope",
    "RegionalStreamClient",
    "Subscription",
    "SubscriptionTarget",
    "Transport",
    "UrllibTransport",
]
