"""Immutable call results and explicit cancellation for management RPCs."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock
from time import monotonic
from typing import Generic, TypeVar

import grpc
from google.protobuf.message import Message

from .transport import TLSConfig

_Response = TypeVar("_Response", bound=Message)


@dataclass(frozen=True, slots=True)
class ManagementConfig:
    """Explicit controller authorities and trust; timeout covers all unary attempts."""

    endpoints: tuple[str, ...]
    bearer_token: str = field(repr=False)
    timeout: float = 10.0
    tls: TLSConfig | None = None
    allow_insecure_loopback: bool = False


@dataclass(frozen=True, slots=True)
class ManagementCallInfo:
    """SDK invocations, not physical frames; cancellation never means rollback."""

    attempts: int = 0
    budget: int = 0
    outcome_may_be_unknown: bool = False


@dataclass(frozen=True, slots=True)
class ManagementResult(Generic[_Response]):
    """Generated presence-aware receipt and bounded application-call evidence."""

    response: _Response
    info: ManagementCallInfo


class ManagementRPCError(grpc.RpcError):
    """Printable errors are redacted; ``cause`` deliberately exposes original status/details."""

    def __init__(
        self,
        code: grpc.StatusCode,
        info: ManagementCallInfo | None = None,
        *,
        cause: BaseException | None = None,
    ) -> None:
        info = info or ManagementCallInfo()
        super().__init__(
            f"epoch: management RPC failed after {info.attempts} attempts ({code.name})"
        )
        self.info = info
        self.cause = cause
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code


def failure_code(error: BaseException) -> grpc.StatusCode:
    if isinstance(error, ManagementRPCError):
        return error.code()
    if isinstance(error, grpc.Call):
        return error.code()
    if isinstance(error, grpc.FutureCancelledError):
        return grpc.StatusCode.CANCELLED
    return grpc.StatusCode.UNKNOWN


class ManagementContext:
    """Shared caller cancellation and optional monotonic deadline.

    A context may bound multiple independent calls. Closing a watch does not
    cancel its shared context. A zero timeout is an already-expired deadline.
    """

    def __init__(self, *, timeout: float | None = None) -> None:
        if timeout is not None and (
            isinstance(timeout, bool) or not math.isfinite(timeout) or timeout < 0
        ):
            raise ValueError("management context timeout must be finite and nonnegative")
        self._deadline = None if timeout is None else monotonic() + timeout
        self._lock = Lock()
        self._cancelled = False
        self._callbacks: dict[int, Callable[[], object]] = {}
        self._next_callback = 0

    @property
    def cancelled(self) -> bool:
        with self._lock:
            return self._cancelled

    def cancel(self) -> None:
        with self._lock:
            if self._cancelled:
                return
            self._cancelled = True
            callbacks = tuple(self._callbacks.values())
            self._callbacks.clear()
        for callback in callbacks:
            callback()

    def remaining(self) -> float | None:
        return None if self._deadline is None else max(0.0, self._deadline - monotonic())

    def _unary_deadline(self, timeout: float) -> float:
        cap = monotonic() + timeout
        return cap if self._deadline is None else min(cap, self._deadline)

    def _status(self) -> grpc.StatusCode | None:
        if self.cancelled:
            return grpc.StatusCode.CANCELLED
        remaining = self.remaining()
        return grpc.StatusCode.DEADLINE_EXCEEDED if remaining == 0 else None

    def _register(self, callback: Callable[[], object]) -> Callable[[], None]:
        with self._lock:
            identity = self._next_callback
            self._next_callback += 1
            cancelled = self._cancelled
            if not cancelled:
                self._callbacks[identity] = callback
        if cancelled:
            callback()

        def unregister() -> None:
            with self._lock:
                self._callbacks.pop(identity, None)

        return unregister
