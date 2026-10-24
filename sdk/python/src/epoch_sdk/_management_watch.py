"""Single-consumer watches with durable application-owned scanned checkpoints."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from threading import Event, Lock
from typing import Protocol, runtime_checkable

import grpc

from . import _management_contracts as contracts
from ._generated.epoch.v1 import regional_admin_pb2 as messages
from ._generated.epoch.v1 import regional_admin_pb2_grpc as service
from ._management_transport import Metadata
from ._management_types import (
    ManagementCallInfo,
    ManagementContext,
    ManagementRPCError,
    failure_code,
)


@runtime_checkable
class _CancellableStream(Protocol):
    def __iter__(self) -> Iterator[messages.WatchResourceChangesResponse]: ...
    def cancel(self) -> bool: ...


class ManagementWatch:
    """One pending page; ACK only after processing and durably saving NextCursor.

    Recv/acknowledge/checkpoint have one consumer. Close may run concurrently.
    Reconnect tries each allowlisted controller at most once per handle; it
    never resets a stale checkpoint or claims exactly-once external effects.
    """

    def __init__(
        self,
        endpoints: tuple[service.RegionalAdminServiceStub, ...],
        usable: Callable[[], None],
        request: messages.WatchResourceChangesRequest,
        context: ManagementContext,
        metadata: Metadata,
    ) -> None:
        self._endpoints = endpoints
        self._usable = usable
        self._request = contracts.clone(request)
        self._context = context
        self._metadata = metadata
        self._checkpoint = request.after_cursor
        self._pending: int | None = None
        self._attempts = 0
        self._closed = Event()
        self._lock = Lock()
        self._stream: _CancellableStream | None = None
        self._iterator: Iterator[messages.WatchResourceChangesResponse] | None = None
        self._unregister: Callable[[], None] = lambda: None
        self._unregister = context._register(self.close)

    def __enter__(self) -> ManagementWatch:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @property
    def checkpoint(self) -> int:
        return self._checkpoint

    @property
    def info(self) -> ManagementCallInfo:
        return ManagementCallInfo(attempts=self._attempts, budget=len(self._endpoints))

    def close(self) -> None:
        self._closed.set()
        self._drop_stream()
        self._unregister()

    def _drop_stream(self) -> None:
        with self._lock:
            stream, self._stream = self._stream, None
        if stream is not None:
            stream.cancel()

    def acknowledge(self, cursor: int) -> None:
        if self._closed.is_set():
            raise ManagementRPCError(grpc.StatusCode.CANCELLED, self.info)
        if self._pending is None:
            raise ManagementRPCError(grpc.StatusCode.FAILED_PRECONDITION, self.info)
        if isinstance(cursor, bool) or not isinstance(cursor, int) or cursor != self._pending:
            raise ManagementRPCError(grpc.StatusCode.INVALID_ARGUMENT, self.info)
        self._checkpoint, self._pending = cursor, None

    def recv(self) -> messages.WatchResourceChangesResponse:
        if self._closed.is_set():
            raise ManagementRPCError(grpc.StatusCode.CANCELLED, self.info)
        if self._pending is not None:
            raise ManagementRPCError(grpc.StatusCode.FAILED_PRECONDITION, self.info)
        failure: BaseException | None = None
        while True:
            status = self._context._status()
            if self._closed.is_set() or status is not None:
                self._fail(status or grpc.StatusCode.CANCELLED)
            try:
                self._usable()
                if self._iterator is None:
                    if self._attempts == len(self._endpoints):
                        self._fail(grpc.StatusCode.UNAVAILABLE, failure)
                    endpoint = self._endpoints[self._attempts]
                    self._attempts += 1
                    request = contracts.clone(self._request)
                    request.after_cursor = self._checkpoint
                    stream = endpoint.WatchResourceChanges(
                        request,
                        timeout=self._context.remaining(),
                        metadata=self._metadata,
                        wait_for_ready=False,
                    )
                    if not isinstance(stream, _CancellableStream):
                        contracts.protocol_error()
                    cancellable: _CancellableStream = stream
                    with self._lock:
                        closed = self._closed.is_set()
                        if not closed:
                            self._stream = cancellable
                    if closed:
                        cancellable.cancel()
                        self._fail(grpc.StatusCode.CANCELLED)
                    self._iterator = iter(cancellable)
                page = next(self._iterator)
                contracts.watch_page(self._request, self._checkpoint, page)
                self._pending = page.next_cursor
                return page
            except StopIteration as error:
                self._iterator, failure = None, error
                self._drop_stream()
            except (grpc.RpcError, grpc.FutureCancelledError) as error:
                code = failure_code(error)
                if code == grpc.StatusCode.UNAVAILABLE and not self._closed.is_set():
                    self._iterator, failure = None, error
                    self._drop_stream()
                    continue
                self._fail(code, error)
            except ValueError as error:
                self._fail(
                    grpc.StatusCode.CANCELLED
                    if self._closed.is_set()
                    else grpc.StatusCode.UNAVAILABLE,
                    error,
                )

    def _fail(self, code: grpc.StatusCode, cause: BaseException | None = None) -> None:
        self.close()
        raise ManagementRPCError(code, self.info, cause=cause) from None
