"""Verification helpers for Epoch signed webhook requests."""

from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class WebhookVerification:
    """Authenticated identity to persist before applying webhook side effects."""

    delivery_id: str
    attempt: int
    signed_at_seconds: int


def verify_webhook_signature(
    secret: bytes,
    body: bytes,
    delivery_id: str,
    attempt_header: str,
    timestamp_header: str,
    signature_header: str,
    *,
    now_seconds: int | None = None,
    tolerance_seconds: int = 300,
) -> WebhookVerification:
    """Verify Epoch's exact-body v1 signature and bounded replay timestamp."""

    if not isinstance(secret, bytes) or not secret:
        raise ValueError("webhook secret must be non-empty bytes")
    if not isinstance(body, bytes):
        raise TypeError("webhook body must be bytes")
    if not delivery_id.strip():
        raise ValueError("webhook delivery ID is required")
    attempt = _canonical_non_negative_integer(attempt_header, "attempt")
    if attempt == 0 or attempt > 4_294_967_295:
        raise ValueError("webhook attempt must be between 1 and 4294967295")
    timestamp = _canonical_non_negative_integer(timestamp_header, "timestamp")
    if isinstance(tolerance_seconds, bool) or tolerance_seconds <= 0:
        raise ValueError("webhook timestamp tolerance must be positive")
    if now_seconds is None:
        now_seconds = int(time.time())
    if isinstance(now_seconds, bool) or not isinstance(now_seconds, int):
        raise TypeError("webhook current time must be integer seconds")
    if abs(now_seconds - timestamp) > tolerance_seconds:
        raise ValueError("webhook timestamp is outside the allowed tolerance")
    if (
        len(signature_header) != 67
        or not signature_header.startswith("v1=")
        or signature_header[3:] != signature_header[3:].lower()
    ):
        raise ValueError("webhook signature must use v1 lowercase hexadecimal")
    try:
        provided = bytes.fromhex(signature_header[3:])
    except ValueError as error:
        raise ValueError("webhook signature must use v1 lowercase hexadecimal") from error
    body_digest = hashlib.sha256(body).hexdigest()
    canonical = f"v1\n{timestamp}\n{delivery_id}\n{attempt}\n{body_digest}".encode()
    expected = hmac.new(secret, canonical, hashlib.sha256).digest()
    if not hmac.compare_digest(provided, expected):
        raise ValueError("webhook signature is invalid")
    return WebhookVerification(delivery_id, attempt, timestamp)


def _canonical_non_negative_integer(value: str, name: str) -> int:
    if not value or not value.isascii() or not value.isdecimal():
        raise ValueError(f"webhook {name} must be a non-negative integer")
    parsed = int(value)
    if str(parsed) != value:
        raise ValueError(f"webhook {name} must use canonical decimal encoding")
    return parsed
