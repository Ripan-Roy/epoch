"""Typed errors exposed by the Epoch Python SDK."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class EpochProtocolError(ValueError):
    """An invalid server response; mutation outcomes must not be inferred.

    This is not an automatic retry signal. A caller must retain the original
    idempotency token and resolve an uncertain mutation before retrying it.
    """


@dataclass(slots=True)
class EpochAPIError(Exception):
    """An HTTP or typed Epoch API failure."""

    status: int
    code: str
    detail: str
    body: Any = None

    def __str__(self) -> str:
        return f"Epoch API error {self.status} ({self.code}): {self.detail}"

    @property
    def retryable(self) -> bool:
        """Whether a generic transport retry can be considered.

        Callers must still use an idempotency key or mutation lookup before
        retrying writes whose outcome is unknown.
        """

        return (
            self.status == 0
            or self.status in {408, 425, 429}
            or self.status >= 500
            or self.code
            in {
                "deadline_exceeded",
                "overloaded",
                "rate_limited",
                "transport_error",
                "unavailable",
            }
        )
