from epoch_sdk._generated.epoch.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OperationState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OPERATION_STATE_UNSPECIFIED: _ClassVar[OperationState]
    OPERATION_STATE_PENDING: _ClassVar[OperationState]
    OPERATION_STATE_SUCCEEDED: _ClassVar[OperationState]
    OPERATION_STATE_FAILED: _ClassVar[OperationState]

class ResourceChangeKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESOURCE_CHANGE_KIND_UNSPECIFIED: _ClassVar[ResourceChangeKind]
    RESOURCE_CHANGE_KIND_DESIRED_APPLIED: _ClassVar[ResourceChangeKind]
    RESOURCE_CHANGE_KIND_DESIRED_DELETED: _ClassVar[ResourceChangeKind]
    RESOURCE_CHANGE_KIND_STATUS_UPDATED: _ClassVar[ResourceChangeKind]
OPERATION_STATE_UNSPECIFIED: OperationState
OPERATION_STATE_PENDING: OperationState
OPERATION_STATE_SUCCEEDED: OperationState
OPERATION_STATE_FAILED: OperationState
RESOURCE_CHANGE_KIND_UNSPECIFIED: ResourceChangeKind
RESOURCE_CHANGE_KIND_DESIRED_APPLIED: ResourceChangeKind
RESOURCE_CHANGE_KIND_DESIRED_DELETED: ResourceChangeKind
RESOURCE_CHANGE_KIND_STATUS_UPDATED: ResourceChangeKind

class ApplyResourceRequest(_message.Message):
    __slots__ = ("request_token", "name", "spec", "expected_generation")
    REQUEST_TOKEN_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_GENERATION_FIELD_NUMBER: _ClassVar[int]
    request_token: str
    name: _common_pb2.ResourceName
    spec: _common_pb2.ResourceSpec
    expected_generation: int
    def __init__(self, request_token: _Optional[str] = ..., name: _Optional[_Union[_common_pb2.ResourceName, _Mapping[str, object]]] = ..., spec: _Optional[_Union[_common_pb2.ResourceSpec, _Mapping[str, object]]] = ..., expected_generation: _Optional[int] = ...) -> None: ...

class ApplyResourceResponse(_message.Message):
    __slots__ = ("resource", "created", "changed", "replayed")
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    CHANGED_FIELD_NUMBER: _ClassVar[int]
    REPLAYED_FIELD_NUMBER: _ClassVar[int]
    resource: _common_pb2.Resource
    created: bool
    changed: bool
    replayed: bool
    def __init__(self, resource: _Optional[_Union[_common_pb2.Resource, _Mapping[str, object]]] = ..., created: _Optional[bool] = ..., changed: _Optional[bool] = ..., replayed: _Optional[bool] = ...) -> None: ...

class GetResourceRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: _common_pb2.ResourceName
    def __init__(self, name: _Optional[_Union[_common_pb2.ResourceName, _Mapping[str, object]]] = ...) -> None: ...

class GetResourceResponse(_message.Message):
    __slots__ = ("resource",)
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    resource: _common_pb2.Resource
    def __init__(self, resource: _Optional[_Union[_common_pb2.Resource, _Mapping[str, object]]] = ...) -> None: ...

class ListResourcesRequest(_message.Message):
    __slots__ = ("organization", "project", "environment", "namespace", "kind", "page_size", "page_token", "owner", "cost_center", "classification", "tags")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ORGANIZATION_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    COST_CENTER_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    organization: str
    project: str
    environment: str
    namespace: str
    kind: _common_pb2.ResourceKind
    page_size: int
    page_token: str
    owner: str
    cost_center: str
    classification: _common_pb2.DataClassification
    tags: _containers.ScalarMap[str, str]
    def __init__(self, organization: _Optional[str] = ..., project: _Optional[str] = ..., environment: _Optional[str] = ..., namespace: _Optional[str] = ..., kind: _Optional[_Union[_common_pb2.ResourceKind, str]] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ..., owner: _Optional[str] = ..., cost_center: _Optional[str] = ..., classification: _Optional[_Union[_common_pb2.DataClassification, str]] = ..., tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ListResourcesResponse(_message.Message):
    __slots__ = ("resources", "next_page_token")
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    resources: _containers.RepeatedCompositeFieldContainer[_common_pb2.Resource]
    next_page_token: str
    def __init__(self, resources: _Optional[_Iterable[_Union[_common_pb2.Resource, _Mapping[str, object]]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class DeleteResourceRequest(_message.Message):
    __slots__ = ("request_token", "name", "expected_generation")
    REQUEST_TOKEN_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_GENERATION_FIELD_NUMBER: _ClassVar[int]
    request_token: str
    name: _common_pb2.ResourceName
    expected_generation: int
    def __init__(self, request_token: _Optional[str] = ..., name: _Optional[_Union[_common_pb2.ResourceName, _Mapping[str, object]]] = ..., expected_generation: _Optional[int] = ...) -> None: ...

class DeleteResourceResponse(_message.Message):
    __slots__ = ("name", "generation", "deleted", "replayed")
    NAME_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    DELETED_FIELD_NUMBER: _ClassVar[int]
    REPLAYED_FIELD_NUMBER: _ClassVar[int]
    name: _common_pb2.ResourceName
    generation: int
    deleted: bool
    replayed: bool
    def __init__(self, name: _Optional[_Union[_common_pb2.ResourceName, _Mapping[str, object]]] = ..., generation: _Optional[int] = ..., deleted: _Optional[bool] = ..., replayed: _Optional[bool] = ...) -> None: ...

class BatchApplyResource(_message.Message):
    __slots__ = ("name", "spec", "expected_generation")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_GENERATION_FIELD_NUMBER: _ClassVar[int]
    name: _common_pb2.ResourceName
    spec: _common_pb2.ResourceSpec
    expected_generation: int
    def __init__(self, name: _Optional[_Union[_common_pb2.ResourceName, _Mapping[str, object]]] = ..., spec: _Optional[_Union[_common_pb2.ResourceSpec, _Mapping[str, object]]] = ..., expected_generation: _Optional[int] = ...) -> None: ...

class BatchApplyResourcesRequest(_message.Message):
    __slots__ = ("request_token", "resources")
    REQUEST_TOKEN_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    request_token: str
    resources: _containers.RepeatedCompositeFieldContainer[BatchApplyResource]
    def __init__(self, request_token: _Optional[str] = ..., resources: _Optional[_Iterable[_Union[BatchApplyResource, _Mapping[str, object]]]] = ...) -> None: ...

class BatchApplyResourcesResponse(_message.Message):
    __slots__ = ("results", "replayed")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    REPLAYED_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[ApplyResourceResponse]
    replayed: bool
    def __init__(self, results: _Optional[_Iterable[_Union[ApplyResourceResponse, _Mapping[str, object]]]] = ..., replayed: _Optional[bool] = ...) -> None: ...

class GetOperationRequest(_message.Message):
    __slots__ = ("request_token", "affected_resources")
    REQUEST_TOKEN_FIELD_NUMBER: _ClassVar[int]
    AFFECTED_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    request_token: str
    affected_resources: _containers.RepeatedCompositeFieldContainer[_common_pb2.ResourceName]
    def __init__(self, request_token: _Optional[str] = ..., affected_resources: _Optional[_Iterable[_Union[_common_pb2.ResourceName, _Mapping[str, object]]]] = ...) -> None: ...

class GetOperationResponse(_message.Message):
    __slots__ = ("request_token", "proposal_id", "state", "affected_resources", "failure_code", "failure_message", "first_change_cursor", "last_change_cursor", "command_kind", "expected_generation")
    REQUEST_TOKEN_FIELD_NUMBER: _ClassVar[int]
    PROPOSAL_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    AFFECTED_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    FAILURE_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    FIRST_CHANGE_CURSOR_FIELD_NUMBER: _ClassVar[int]
    LAST_CHANGE_CURSOR_FIELD_NUMBER: _ClassVar[int]
    COMMAND_KIND_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_GENERATION_FIELD_NUMBER: _ClassVar[int]
    request_token: str
    proposal_id: int
    state: OperationState
    affected_resources: _containers.RepeatedCompositeFieldContainer[_common_pb2.ResourceName]
    failure_code: str
    failure_message: str
    first_change_cursor: int
    last_change_cursor: int
    command_kind: str
    expected_generation: int
    def __init__(self, request_token: _Optional[str] = ..., proposal_id: _Optional[int] = ..., state: _Optional[_Union[OperationState, str]] = ..., affected_resources: _Optional[_Iterable[_Union[_common_pb2.ResourceName, _Mapping[str, object]]]] = ..., failure_code: _Optional[str] = ..., failure_message: _Optional[str] = ..., first_change_cursor: _Optional[int] = ..., last_change_cursor: _Optional[int] = ..., command_kind: _Optional[str] = ..., expected_generation: _Optional[int] = ...) -> None: ...

class ResourceChange(_message.Message):
    __slots__ = ("cursor", "kind", "name", "generation")
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    cursor: int
    kind: ResourceChangeKind
    name: _common_pb2.ResourceName
    generation: int
    def __init__(self, cursor: _Optional[int] = ..., kind: _Optional[_Union[ResourceChangeKind, str]] = ..., name: _Optional[_Union[_common_pb2.ResourceName, _Mapping[str, object]]] = ..., generation: _Optional[int] = ...) -> None: ...

class WatchResourceChangesRequest(_message.Message):
    __slots__ = ("after_cursor", "batch_size", "organization", "project", "environment", "namespace", "kind")
    AFTER_CURSOR_FIELD_NUMBER: _ClassVar[int]
    BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    after_cursor: int
    batch_size: int
    organization: str
    project: str
    environment: str
    namespace: str
    kind: _common_pb2.ResourceKind
    def __init__(self, after_cursor: _Optional[int] = ..., batch_size: _Optional[int] = ..., organization: _Optional[str] = ..., project: _Optional[str] = ..., environment: _Optional[str] = ..., namespace: _Optional[str] = ..., kind: _Optional[_Union[_common_pb2.ResourceKind, str]] = ...) -> None: ...

class WatchResourceChangesResponse(_message.Message):
    __slots__ = ("earliest_cursor", "latest_cursor", "changes", "next_cursor")
    EARLIEST_CURSOR_FIELD_NUMBER: _ClassVar[int]
    LATEST_CURSOR_FIELD_NUMBER: _ClassVar[int]
    CHANGES_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    earliest_cursor: int
    latest_cursor: int
    changes: _containers.RepeatedCompositeFieldContainer[ResourceChange]
    next_cursor: int
    def __init__(self, earliest_cursor: _Optional[int] = ..., latest_cursor: _Optional[int] = ..., changes: _Optional[_Iterable[_Union[ResourceChange, _Mapping[str, object]]]] = ..., next_cursor: _Optional[int] = ...) -> None: ...
