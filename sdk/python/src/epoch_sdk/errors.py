"""Typed errors exposed by the Epoch Python SDK."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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

        return self.status in {408, 425, 429, 502, 503, 504}
