"""Typed seven-method Go-controller management API (optional gRPC dependencies).

Import explicitly from ``epoch_sdk.management``. Ordinary ``epoch_sdk`` HTTP
imports do not load grpcio/protobuf. This is not the native data gRPC surface.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from threading import Lock
from time import monotonic
from typing import TypeVar

import grpc
from google.protobuf.message import Message

from . import _management_contracts as contracts
from ._generated.epoch.v1 import common_pb2 as common
from ._generated.epoch.v1 import regional_admin_pb2 as messages
from ._generated.epoch.v1 import regional_admin_pb2_grpc as service
from ._management_transport import authenticated_metadata, connections
from ._management_types import (
    ManagementCallInfo,
    ManagementConfig,
    ManagementContext,
    ManagementResult,
    ManagementRPCError,
    failure_code,
)
from ._management_watch import ManagementWatch

__all__ = [
    "ManagementCallInfo",
    "ManagementClient",
    "ManagementConfig",
    "ManagementContext",
    "ManagementRPCError",
    "ManagementResult",
    "ManagementWatch",
    "common",
    "messages",
]

_Request = TypeVar("_Request", bound=Message)
_Response = TypeVar("_Response", bound=Message)


class ManagementClient:
    """Thread-safe lazy connections and unaries; close owned channels explicitly.

    Only UNAVAILABLE advances to another allowlisted endpoint within one
    original deadline. No hedging, semantic retries, or replacement token.
    Successful desired acceptance does not imply physical resource readiness.
    """

    def __init__(self, config: ManagementConfig) -> None:
        self._channels = connections(config)
        self._endpoints = tuple(
            service.RegionalAdminServiceStub(channel) for channel in self._channels
        )
        self._token = config.bearer_token
        self._timeout = config.timeout
        self._lock = Lock()
        self._closed = False

    def __enter__(self) -> ManagementClient:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        for channel in self._channels:
            channel.close()

    def _usable(self) -> None:
        with self._lock:
            if self._closed:
                raise ManagementRPCError(grpc.StatusCode.UNAVAILABLE)

    def _call(
        self,
        request: _Request,
        select: Callable[
            [service.RegionalAdminServiceStub], grpc.UnaryUnaryMultiCallable[_Request, _Response]
        ],
        validate: Callable[[_Request, _Response], None],
        *,
        mutation: bool,
        context: ManagementContext | None,
        metadata: Sequence[tuple[str, str | bytes]],
    ) -> ManagementResult[_Response]:
        self._usable()
        caller = context or ManagementContext()
        deadline = caller._unary_deadline(self._timeout)
        outgoing = authenticated_metadata(self._token, metadata)
        frozen = contracts.clone(request)
        info = ManagementCallInfo(budget=len(self._endpoints))
        failure: BaseException | None = None
        code = grpc.StatusCode.UNAVAILABLE
        for endpoint in self._endpoints:
            code = caller._status() or (
                grpc.StatusCode.DEADLINE_EXCEEDED if monotonic() >= deadline else grpc.StatusCode.OK
            )
            if code != grpc.StatusCode.OK:
                break
            try:
                self._usable()
            except ManagementRPCError as error:
                code, failure = error.code(), error
                break
            info = ManagementCallInfo(info.attempts + 1, info.budget, mutation)
            try:
                future: grpc.Future[_Response] = select(endpoint).future(
                    contracts.clone(frozen),
                    timeout=max(0.0, deadline - monotonic()),
                    metadata=outgoing,
                    wait_for_ready=False,
                )
                unregister = caller._register(future.cancel)
                try:
                    response = future.result()
                finally:
                    unregister()
                validate(frozen, response)
                return ManagementResult(response, ManagementCallInfo(info.attempts, info.budget))
            except (grpc.RpcError, grpc.FutureCancelledError) as error:
                code, failure = failure_code(error), error
            except ValueError as error:
                code, failure = grpc.StatusCode.UNAVAILABLE, error
            if code != grpc.StatusCode.UNAVAILABLE:
                break
        raise ManagementRPCError(code, info, cause=failure) from None

    def apply_resource(
        self,
        request: messages.ApplyResourceRequest,
        *,
        context: ManagementContext | None = None,
        metadata: Sequence[tuple[str, str | bytes]] = (),
    ) -> ManagementResult[messages.ApplyResourceResponse]:
        contracts.apply_request(request)
        return self._call(
            request,
            lambda endpoint: endpoint.ApplyResource,
            contracts.apply_response,
            mutation=True,
            context=context,
            metadata=metadata,
        )

    def get_resource(
        self,
        request: messages.GetResourceRequest,
        *,
        context: ManagementContext | None = None,
        metadata: Sequence[tuple[str, str | bytes]] = (),
    ) -> ManagementResult[messages.GetResourceResponse]:
        contracts.name(request.name)
        return self._call(
            request,
            lambda endpoint: endpoint.GetResource,
            contracts.get_response,
            mutation=False,
            context=context,
            metadata=metadata,
        )

    def list_resources(
        self,
        request: messages.ListResourcesRequest,
        *,
        context: ManagementContext | None = None,
        metadata: Sequence[tuple[str, str | bytes]] = (),
    ) -> ManagementResult[messages.ListResourcesResponse]:
        """One bounded page; the current controller rejects nonempty page tokens."""
        contracts.list_request(request)
        return self._call(
            request,
            lambda endpoint: endpoint.ListResources,
            contracts.list_response,
            mutation=False,
            context=context,
            metadata=metadata,
        )

    def delete_resource(
        self,
        request: messages.DeleteResourceRequest,
        *,
        context: ManagementContext | None = None,
        metadata: Sequence[tuple[str, str | bytes]] = (),
    ) -> ManagementResult[messages.DeleteResourceResponse]:
        contracts.token(request.request_token)
        contracts.name(request.name)
        return self._call(
            request,
            lambda endpoint: endpoint.DeleteResource,
            contracts.delete_response,
            mutation=True,
            context=context,
            metadata=metadata,
        )

    def batch_apply_resources(
        self,
        request: messages.BatchApplyResourcesRequest,
        *,
        context: ManagementContext | None = None,
        metadata: Sequence[tuple[str, str | bytes]] = (),
    ) -> ManagementResult[messages.BatchApplyResourcesResponse]:
        contracts.batch_request(request)
        return self._call(
            request,
            lambda endpoint: endpoint.BatchApplyResources,
            contracts.batch_response,
            mutation=True,
            context=context,
            metadata=metadata,
        )

    def get_operation(
        self,
        request: messages.GetOperationRequest,
        *,
        context: ManagementContext | None = None,
        metadata: Sequence[tuple[str, str | bytes]] = (),
    ) -> ManagementResult[messages.GetOperationResponse]:
        contracts.token(request.request_token)
        contracts.names(request.affected_resources)
        return self._call(
            request,
            lambda endpoint: endpoint.GetOperation,
            contracts.operation_response,
            mutation=False,
            context=context,
            metadata=metadata,
        )

    def watch_resource_changes(
        self,
        request: messages.WatchResourceChangesRequest,
        *,
        context: ManagementContext | None = None,
        metadata: Sequence[tuple[str, str | bytes]] = (),
    ) -> ManagementWatch:
        self._usable()
        contracts.watch_request(request)
        return ManagementWatch(
            self._endpoints,
            self._usable,
            request,
            context or ManagementContext(),
            authenticated_metadata(self._token, metadata),
        )
