"""Generated-message identity, outcome, governance, and scanned-cursor contracts."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TypeVar

import grpc
from google.protobuf.message import Message

from ._generated.epoch.v1 import common_pb2 as common
from ._generated.epoch.v1 import regional_admin_pb2 as messages
from ._management_types import ManagementRPCError

_Message = TypeVar("_Message", bound=Message)
_KINDS = frozenset(common.ResourceKind.values())
_CLASSIFICATIONS = frozenset(common.DataClassification.values())


def clone(value: _Message) -> _Message:
    copied = type(value)()
    copied.CopyFrom(value)
    return copied


def invalid() -> None:
    raise ManagementRPCError(grpc.StatusCode.INVALID_ARGUMENT)


def protocol_error() -> None:
    raise ManagementRPCError(grpc.StatusCode.DATA_LOSS)


def token(value: str) -> None:
    if not value or value.strip() != value or len(value.encode("utf-8")) > 256:
        invalid()


def name(value: common.ResourceName) -> None:
    if value.kind == 0 or value.kind not in _KINDS:
        invalid()
    for segment in (
        value.organization,
        value.project,
        value.environment,
        value.namespace,
        value.name,
    ):
        if not segment or segment.strip() != segment or any(char in segment for char in "/\0\r\n"):
            invalid()


def name_key(value: common.ResourceName) -> tuple[str, str, str, str, int, str]:
    return (
        value.organization,
        value.project,
        value.environment,
        value.namespace,
        value.kind,
        value.name,
    )


def names(values: Iterable[common.ResourceName]) -> None:
    seen: set[tuple[str, str, str, str, int, str]] = set()
    for value in values:
        name(value)
        key = name_key(value)
        if key in seen:
            invalid()
        seen.add(key)
    if not 1 <= len(seen) <= 128:
        invalid()


def apply_request(request: messages.ApplyResourceRequest) -> None:
    token(request.request_token)
    name(request.name)
    if not request.HasField("spec"):
        invalid()


def list_request(request: messages.ListResourcesRequest) -> None:
    if not 0 <= request.page_size <= 100 or (
        request.kind not in _KINDS or request.classification not in _CLASSIFICATIONS
    ):
        invalid()


def batch_request(request: messages.BatchApplyResourcesRequest) -> None:
    token(request.request_token)
    names(item.name for item in request.resources)
    if any(not item.HasField("spec") for item in request.resources):
        invalid()


def watch_request(request: messages.WatchResourceChangesRequest) -> None:
    if request.batch_size > 1000 or request.kind not in _KINDS:
        invalid()


def resource(value: common.Resource) -> None:
    try:
        name(value.name)
    except ManagementRPCError:
        protocol_error()
    if value.generation == 0 or not value.HasField("spec"):
        protocol_error()


def matches_scope(
    request: messages.ListResourcesRequest | messages.WatchResourceChangesRequest,
    identity: common.ResourceName,
) -> bool:
    return all(
        not expected or expected == actual
        for expected, actual in (
            (request.organization, identity.organization),
            (request.project, identity.project),
            (request.environment, identity.environment),
            (request.namespace, identity.namespace),
            (request.kind, identity.kind),
        )
    )


def apply_response(
    request: messages.ApplyResourceRequest, response: messages.ApplyResourceResponse
) -> None:
    resource(response.resource)
    if response.resource.name != request.name:
        protocol_error()


def get_response(
    request: messages.GetResourceRequest, response: messages.GetResourceResponse
) -> None:
    resource(response.resource)
    if response.resource.name != request.name:
        protocol_error()


def list_response(
    request: messages.ListResourcesRequest, response: messages.ListResourcesResponse
) -> None:
    if len(response.resources) > (request.page_size or 50):
        protocol_error()
    seen: set[tuple[str, str, str, str, int, str]] = set()
    for value in response.resources:
        resource(value)
        key = name_key(value.name)
        governance = value.spec.governance
        if (
            key in seen
            or not matches_scope(request, value.name)
            or (request.owner and request.owner != governance.owner)
            or (request.cost_center and request.cost_center != governance.cost_center)
            or (request.classification and request.classification != governance.classification)
            or any(
                key not in governance.tags or governance.tags[key] != expected
                for key, expected in request.tags.items()
            )
        ):
            protocol_error()
        seen.add(key)


def delete_response(
    request: messages.DeleteResourceRequest, response: messages.DeleteResourceResponse
) -> None:
    if response.name != request.name or (response.deleted and response.generation == 0):
        protocol_error()


def batch_response(
    request: messages.BatchApplyResourcesRequest, response: messages.BatchApplyResourcesResponse
) -> None:
    remaining = [item.name for item in request.resources]
    if len(remaining) != len(response.results):
        protocol_error()
    for result in response.results:
        resource(result.resource)
        if result.replayed != response.replayed or result.resource.name not in remaining:
            protocol_error()
        remaining.remove(result.resource.name)


def operation_response(
    request: messages.GetOperationRequest, response: messages.GetOperationResponse
) -> None:
    if (
        response.request_token != request.request_token
        or response.proposal_id == 0
        or not response.command_kind
        or response.state
        not in (
            messages.OPERATION_STATE_PENDING,
            messages.OPERATION_STATE_SUCCEEDED,
            messages.OPERATION_STATE_FAILED,
        )
        or len(response.affected_resources) != len(request.affected_resources)
        or response.last_change_cursor < response.first_change_cursor
        or (response.first_change_cursor == 0 and response.last_change_cursor != 0)
    ):
        protocol_error()
    try:
        names(response.affected_resources)
    except ManagementRPCError:
        protocol_error()
    if any(value not in response.affected_resources for value in request.affected_resources):
        protocol_error()
    if response.state == messages.OPERATION_STATE_FAILED:
        if not response.failure_code:
            protocol_error()
    elif response.failure_code or response.failure_message:
        protocol_error()


def watch_page(
    request: messages.WatchResourceChangesRequest,
    after: int,
    page: messages.WatchResourceChangesResponse,
) -> None:
    if (
        page.earliest_cursor == 0
        or page.earliest_cursor - 1 > after
        or (
            page.earliest_cursor > page.latest_cursor
            and not (page.earliest_cursor == 1 and page.latest_cursor == 0)
        )
        or not after <= page.next_cursor <= page.latest_cursor
        or len(page.changes) > (request.batch_size or 100)
    ):
        protocol_error()
    previous = after
    for change in page.changes:
        try:
            name(change.name)
        except ManagementRPCError:
            protocol_error()
        if (
            not previous < change.cursor <= page.next_cursor
            or change.generation == 0
            or change.kind
            not in (
                messages.RESOURCE_CHANGE_KIND_DESIRED_APPLIED,
                messages.RESOURCE_CHANGE_KIND_DESIRED_DELETED,
                messages.RESOURCE_CHANGE_KIND_STATUS_UPDATED,
            )
            or not matches_scope(request, change.name)
        ):
            protocol_error()
        previous = change.cursor
