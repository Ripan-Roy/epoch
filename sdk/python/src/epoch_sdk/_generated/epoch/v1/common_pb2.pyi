import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WorkloadProfile(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WORKLOAD_PROFILE_UNSPECIFIED: _ClassVar[WorkloadProfile]
    WORKLOAD_PROFILE_CACHE: _ClassVar[WorkloadProfile]
    WORKLOAD_PROFILE_STATE_TABLE: _ClassVar[WorkloadProfile]
    WORKLOAD_PROFILE_STREAM_LOG: _ClassVar[WorkloadProfile]
    WORKLOAD_PROFILE_WORK_QUEUE: _ClassVar[WorkloadProfile]
    WORKLOAD_PROFILE_EVENT_BUS: _ClassVar[WorkloadProfile]

class ResourceKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESOURCE_KIND_UNSPECIFIED: _ClassVar[ResourceKind]
    RESOURCE_KIND_CACHE: _ClassVar[ResourceKind]
    RESOURCE_KIND_TABLE: _ClassVar[ResourceKind]
    RESOURCE_KIND_STREAM: _ClassVar[ResourceKind]
    RESOURCE_KIND_QUEUE: _ClassVar[ResourceKind]
    RESOURCE_KIND_EVENT_BUS: _ClassVar[ResourceKind]
    RESOURCE_KIND_SUBSCRIPTION: _ClassVar[ResourceKind]
    RESOURCE_KIND_SCHEMA: _ClassVar[ResourceKind]
    RESOURCE_KIND_PIPE: _ClassVar[ResourceKind]
    RESOURCE_KIND_CONNECTOR: _ClassVar[ResourceKind]
    RESOURCE_KIND_POLICY: _ClassVar[ResourceKind]

class DurabilityProfile(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DURABILITY_PROFILE_UNSPECIFIED: _ClassVar[DurabilityProfile]
    DURABILITY_PROFILE_VOLATILE: _ClassVar[DurabilityProfile]
    DURABILITY_PROFILE_REPLICATED_MEMORY: _ClassVar[DurabilityProfile]
    DURABILITY_PROFILE_LOCAL_DURABLE: _ClassVar[DurabilityProfile]
    DURABILITY_PROFILE_QUORUM_DURABLE: _ClassVar[DurabilityProfile]
    DURABILITY_PROFILE_GEO_ASYNC: _ClassVar[DurabilityProfile]
    DURABILITY_PROFILE_GEO_SYNC: _ClassVar[DurabilityProfile]

class DeliverySemantics(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DELIVERY_SEMANTICS_UNSPECIFIED: _ClassVar[DeliverySemantics]
    DELIVERY_SEMANTICS_AT_MOST_ONCE: _ClassVar[DeliverySemantics]
    DELIVERY_SEMANTICS_AT_LEAST_ONCE: _ClassVar[DeliverySemantics]
    DELIVERY_SEMANTICS_EFFECTIVELY_ONCE: _ClassVar[DeliverySemantics]
    DELIVERY_SEMANTICS_TRANSACTIONAL_EXACTLY_ONCE: _ClassVar[DeliverySemantics]

class OrderingScope(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ORDERING_SCOPE_UNSPECIFIED: _ClassVar[OrderingScope]
    ORDERING_SCOPE_NONE: _ClassVar[OrderingScope]
    ORDERING_SCOPE_KEY: _ClassVar[OrderingScope]
    ORDERING_SCOPE_SESSION: _ClassVar[OrderingScope]
    ORDERING_SCOPE_PARTITION: _ClassVar[OrderingScope]
    ORDERING_SCOPE_RESOURCE: _ClassVar[OrderingScope]

class DeploymentMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DEPLOYMENT_MODE_UNSPECIFIED: _ClassVar[DeploymentMode]
    DEPLOYMENT_MODE_EMBEDDED: _ClassVar[DeploymentMode]
    DEPLOYMENT_MODE_STANDALONE: _ClassVar[DeploymentMode]
    DEPLOYMENT_MODE_CLUSTER: _ClassVar[DeploymentMode]
    DEPLOYMENT_MODE_MANAGED: _ClassVar[DeploymentMode]

class ResourcePhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESOURCE_PHASE_UNSPECIFIED: _ClassVar[ResourcePhase]
    RESOURCE_PHASE_PENDING: _ClassVar[ResourcePhase]
    RESOURCE_PHASE_READY: _ClassVar[ResourcePhase]
    RESOURCE_PHASE_DEGRADED: _ClassVar[ResourcePhase]
    RESOURCE_PHASE_FAILED: _ClassVar[ResourcePhase]
    RESOURCE_PHASE_DELETING: _ClassVar[ResourcePhase]

class DataClassification(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DATA_CLASSIFICATION_UNSPECIFIED: _ClassVar[DataClassification]
    DATA_CLASSIFICATION_PUBLIC: _ClassVar[DataClassification]
    DATA_CLASSIFICATION_INTERNAL: _ClassVar[DataClassification]
    DATA_CLASSIFICATION_CONFIDENTIAL: _ClassVar[DataClassification]
    DATA_CLASSIFICATION_RESTRICTED: _ClassVar[DataClassification]

class ConditionState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONDITION_STATE_UNSPECIFIED: _ClassVar[ConditionState]
    CONDITION_STATE_TRUE: _ClassVar[ConditionState]
    CONDITION_STATE_FALSE: _ClassVar[ConditionState]
    CONDITION_STATE_UNKNOWN: _ClassVar[ConditionState]

class TabletPhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TABLET_PHASE_UNSPECIFIED: _ClassVar[TabletPhase]
    TABLET_PHASE_PENDING: _ClassVar[TabletPhase]
    TABLET_PHASE_SERVING: _ClassVar[TabletPhase]
    TABLET_PHASE_DRAINING: _ClassVar[TabletPhase]
    TABLET_PHASE_FAILED: _ClassVar[TabletPhase]
WORKLOAD_PROFILE_UNSPECIFIED: WorkloadProfile
WORKLOAD_PROFILE_CACHE: WorkloadProfile
WORKLOAD_PROFILE_STATE_TABLE: WorkloadProfile
WORKLOAD_PROFILE_STREAM_LOG: WorkloadProfile
WORKLOAD_PROFILE_WORK_QUEUE: WorkloadProfile
WORKLOAD_PROFILE_EVENT_BUS: WorkloadProfile
RESOURCE_KIND_UNSPECIFIED: ResourceKind
RESOURCE_KIND_CACHE: ResourceKind
RESOURCE_KIND_TABLE: ResourceKind
RESOURCE_KIND_STREAM: ResourceKind
RESOURCE_KIND_QUEUE: ResourceKind
RESOURCE_KIND_EVENT_BUS: ResourceKind
RESOURCE_KIND_SUBSCRIPTION: ResourceKind
RESOURCE_KIND_SCHEMA: ResourceKind
RESOURCE_KIND_PIPE: ResourceKind
RESOURCE_KIND_CONNECTOR: ResourceKind
RESOURCE_KIND_POLICY: ResourceKind
DURABILITY_PROFILE_UNSPECIFIED: DurabilityProfile
DURABILITY_PROFILE_VOLATILE: DurabilityProfile
DURABILITY_PROFILE_REPLICATED_MEMORY: DurabilityProfile
DURABILITY_PROFILE_LOCAL_DURABLE: DurabilityProfile
DURABILITY_PROFILE_QUORUM_DURABLE: DurabilityProfile
DURABILITY_PROFILE_GEO_ASYNC: DurabilityProfile
DURABILITY_PROFILE_GEO_SYNC: DurabilityProfile
DELIVERY_SEMANTICS_UNSPECIFIED: DeliverySemantics
DELIVERY_SEMANTICS_AT_MOST_ONCE: DeliverySemantics
DELIVERY_SEMANTICS_AT_LEAST_ONCE: DeliverySemantics
DELIVERY_SEMANTICS_EFFECTIVELY_ONCE: DeliverySemantics
DELIVERY_SEMANTICS_TRANSACTIONAL_EXACTLY_ONCE: DeliverySemantics
ORDERING_SCOPE_UNSPECIFIED: OrderingScope
ORDERING_SCOPE_NONE: OrderingScope
ORDERING_SCOPE_KEY: OrderingScope
ORDERING_SCOPE_SESSION: OrderingScope
ORDERING_SCOPE_PARTITION: OrderingScope
ORDERING_SCOPE_RESOURCE: OrderingScope
DEPLOYMENT_MODE_UNSPECIFIED: DeploymentMode
DEPLOYMENT_MODE_EMBEDDED: DeploymentMode
DEPLOYMENT_MODE_STANDALONE: DeploymentMode
DEPLOYMENT_MODE_CLUSTER: DeploymentMode
DEPLOYMENT_MODE_MANAGED: DeploymentMode
RESOURCE_PHASE_UNSPECIFIED: ResourcePhase
RESOURCE_PHASE_PENDING: ResourcePhase
RESOURCE_PHASE_READY: ResourcePhase
RESOURCE_PHASE_DEGRADED: ResourcePhase
RESOURCE_PHASE_FAILED: ResourcePhase
RESOURCE_PHASE_DELETING: ResourcePhase
DATA_CLASSIFICATION_UNSPECIFIED: DataClassification
DATA_CLASSIFICATION_PUBLIC: DataClassification
DATA_CLASSIFICATION_INTERNAL: DataClassification
DATA_CLASSIFICATION_CONFIDENTIAL: DataClassification
DATA_CLASSIFICATION_RESTRICTED: DataClassification
CONDITION_STATE_UNSPECIFIED: ConditionState
CONDITION_STATE_TRUE: ConditionState
CONDITION_STATE_FALSE: ConditionState
CONDITION_STATE_UNKNOWN: ConditionState
TABLET_PHASE_UNSPECIFIED: TabletPhase
TABLET_PHASE_PENDING: TabletPhase
TABLET_PHASE_SERVING: TabletPhase
TABLET_PHASE_DRAINING: TabletPhase
TABLET_PHASE_FAILED: TabletPhase

class Envelope(_message.Message):
    __slots__ = ("id", "source", "type", "subject", "time", "key", "headers", "content_type", "schema_ref", "traceparent", "payload", "deliver_at", "ttl", "priority", "dedupe_id", "transaction_id", "extensions")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class ExtensionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_REF_FIELD_NUMBER: _ClassVar[int]
    TRACEPARENT_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    DELIVER_AT_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    DEDUPE_ID_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    EXTENSIONS_FIELD_NUMBER: _ClassVar[int]
    id: str
    source: str
    type: str
    subject: str
    time: _timestamp_pb2.Timestamp
    key: str
    headers: _containers.ScalarMap[str, str]
    content_type: str
    schema_ref: str
    traceparent: str
    payload: bytes
    deliver_at: _timestamp_pb2.Timestamp
    ttl: _duration_pb2.Duration
    priority: int
    dedupe_id: str
    transaction_id: str
    extensions: _containers.ScalarMap[str, bytes]
    def __init__(self, id: _Optional[str] = ..., source: _Optional[str] = ..., type: _Optional[str] = ..., subject: _Optional[str] = ..., time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping[str, object]]] = ..., key: _Optional[str] = ..., headers: _Optional[_Mapping[str, str]] = ..., content_type: _Optional[str] = ..., schema_ref: _Optional[str] = ..., traceparent: _Optional[str] = ..., payload: _Optional[bytes] = ..., deliver_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping[str, object]]] = ..., ttl: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping[str, object]]] = ..., priority: _Optional[int] = ..., dedupe_id: _Optional[str] = ..., transaction_id: _Optional[str] = ..., extensions: _Optional[_Mapping[str, bytes]] = ...) -> None: ...

class WriteReceipt(_message.Message):
    __slots__ = ("achieved_durability", "resource_epoch", "commit_position", "replica_acks", "duplicate", "request_id")
    ACHIEVED_DURABILITY_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    COMMIT_POSITION_FIELD_NUMBER: _ClassVar[int]
    REPLICA_ACKS_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    achieved_durability: DurabilityProfile
    resource_epoch: int
    commit_position: int
    replica_acks: int
    duplicate: bool
    request_id: str
    def __init__(self, achieved_durability: _Optional[_Union[DurabilityProfile, str]] = ..., resource_epoch: _Optional[int] = ..., commit_position: _Optional[int] = ..., replica_acks: _Optional[int] = ..., duplicate: _Optional[bool] = ..., request_id: _Optional[str] = ...) -> None: ...

class ResourceName(_message.Message):
    __slots__ = ("organization", "project", "environment", "namespace", "kind", "name")
    ORGANIZATION_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    organization: str
    project: str
    environment: str
    namespace: str
    kind: ResourceKind
    name: str
    def __init__(self, organization: _Optional[str] = ..., project: _Optional[str] = ..., environment: _Optional[str] = ..., namespace: _Optional[str] = ..., kind: _Optional[_Union[ResourceKind, str]] = ..., name: _Optional[str] = ...) -> None: ...

class PlacementPolicy(_message.Message):
    __slots__ = ("allowed_regions", "minimum_zones", "required_node_class", "minimum_racks", "excluded_node_ids")
    ALLOWED_REGIONS_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_ZONES_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_NODE_CLASS_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_RACKS_FIELD_NUMBER: _ClassVar[int]
    EXCLUDED_NODE_IDS_FIELD_NUMBER: _ClassVar[int]
    allowed_regions: _containers.RepeatedScalarFieldContainer[str]
    minimum_zones: int
    required_node_class: str
    minimum_racks: int
    excluded_node_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, allowed_regions: _Optional[_Iterable[str]] = ..., minimum_zones: _Optional[int] = ..., required_node_class: _Optional[str] = ..., minimum_racks: _Optional[int] = ..., excluded_node_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class ResourceGovernance(_message.Message):
    __slots__ = ("owner", "cost_center", "classification", "tags")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    OWNER_FIELD_NUMBER: _ClassVar[int]
    COST_CENTER_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    owner: str
    cost_center: str
    classification: DataClassification
    tags: _containers.ScalarMap[str, str]
    def __init__(self, owner: _Optional[str] = ..., cost_center: _Optional[str] = ..., classification: _Optional[_Union[DataClassification, str]] = ..., tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ResourceSpec(_message.Message):
    __slots__ = ("workload_profile", "durability", "delivery", "ordering", "replicas", "labels", "configuration", "placement", "governance")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    WORKLOAD_PROFILE_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_FIELD_NUMBER: _ClassVar[int]
    ORDERING_FIELD_NUMBER: _ClassVar[int]
    REPLICAS_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    GOVERNANCE_FIELD_NUMBER: _ClassVar[int]
    workload_profile: WorkloadProfile
    durability: DurabilityProfile
    delivery: DeliverySemantics
    ordering: OrderingScope
    replicas: int
    labels: _containers.ScalarMap[str, str]
    configuration: _struct_pb2.Struct
    placement: PlacementPolicy
    governance: ResourceGovernance
    def __init__(self, workload_profile: _Optional[_Union[WorkloadProfile, str]] = ..., durability: _Optional[_Union[DurabilityProfile, str]] = ..., delivery: _Optional[_Union[DeliverySemantics, str]] = ..., ordering: _Optional[_Union[OrderingScope, str]] = ..., replicas: _Optional[int] = ..., labels: _Optional[_Mapping[str, str]] = ..., configuration: _Optional[_Union[_struct_pb2.Struct, _Mapping[str, object]]] = ..., placement: _Optional[_Union[PlacementPolicy, _Mapping[str, object]]] = ..., governance: _Optional[_Union[ResourceGovernance, _Mapping[str, object]]] = ...) -> None: ...

class Condition(_message.Message):
    __slots__ = ("type", "state", "reason", "message", "observed_generation", "last_transition_time")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_GENERATION_FIELD_NUMBER: _ClassVar[int]
    LAST_TRANSITION_TIME_FIELD_NUMBER: _ClassVar[int]
    type: str
    state: ConditionState
    reason: str
    message: str
    observed_generation: int
    last_transition_time: _timestamp_pb2.Timestamp
    def __init__(self, type: _Optional[str] = ..., state: _Optional[_Union[ConditionState, str]] = ..., reason: _Optional[str] = ..., message: _Optional[str] = ..., observed_generation: _Optional[int] = ..., last_transition_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping[str, object]]] = ...) -> None: ...

class TabletDescriptor(_message.Message):
    __slots__ = ("tablet_id", "consensus_group_id", "shard_index", "workload_profile", "tablet_epoch", "resource_generation", "desired_replicas", "voter_node_ids", "leader_node_id", "phase", "assigned_node_ids", "reachable_voter_node_ids", "bootstrap_voter_node_ids", "target_voter_node_ids")
    TABLET_ID_FIELD_NUMBER: _ClassVar[int]
    CONSENSUS_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    SHARD_INDEX_FIELD_NUMBER: _ClassVar[int]
    WORKLOAD_PROFILE_FIELD_NUMBER: _ClassVar[int]
    TABLET_EPOCH_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_GENERATION_FIELD_NUMBER: _ClassVar[int]
    DESIRED_REPLICAS_FIELD_NUMBER: _ClassVar[int]
    VOTER_NODE_IDS_FIELD_NUMBER: _ClassVar[int]
    LEADER_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    ASSIGNED_NODE_IDS_FIELD_NUMBER: _ClassVar[int]
    REACHABLE_VOTER_NODE_IDS_FIELD_NUMBER: _ClassVar[int]
    BOOTSTRAP_VOTER_NODE_IDS_FIELD_NUMBER: _ClassVar[int]
    TARGET_VOTER_NODE_IDS_FIELD_NUMBER: _ClassVar[int]
    tablet_id: int
    consensus_group_id: int
    shard_index: int
    workload_profile: WorkloadProfile
    tablet_epoch: int
    resource_generation: int
    desired_replicas: int
    voter_node_ids: _containers.RepeatedScalarFieldContainer[int]
    leader_node_id: int
    phase: TabletPhase
    assigned_node_ids: _containers.RepeatedScalarFieldContainer[int]
    reachable_voter_node_ids: _containers.RepeatedScalarFieldContainer[int]
    bootstrap_voter_node_ids: _containers.RepeatedScalarFieldContainer[int]
    target_voter_node_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, tablet_id: _Optional[int] = ..., consensus_group_id: _Optional[int] = ..., shard_index: _Optional[int] = ..., workload_profile: _Optional[_Union[WorkloadProfile, str]] = ..., tablet_epoch: _Optional[int] = ..., resource_generation: _Optional[int] = ..., desired_replicas: _Optional[int] = ..., voter_node_ids: _Optional[_Iterable[int]] = ..., leader_node_id: _Optional[int] = ..., phase: _Optional[_Union[TabletPhase, str]] = ..., assigned_node_ids: _Optional[_Iterable[int]] = ..., reachable_voter_node_ids: _Optional[_Iterable[int]] = ..., bootstrap_voter_node_ids: _Optional[_Iterable[int]] = ..., target_voter_node_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RegionalNodeObservation(_message.Message):
    __slots__ = ("node_id", "region", "zone", "node_class", "consensus_voter_node_ids", "max_consensus_groups", "used_consensus_groups", "available_consensus_groups", "rack")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    ZONE_FIELD_NUMBER: _ClassVar[int]
    NODE_CLASS_FIELD_NUMBER: _ClassVar[int]
    CONSENSUS_VOTER_NODE_IDS_FIELD_NUMBER: _ClassVar[int]
    MAX_CONSENSUS_GROUPS_FIELD_NUMBER: _ClassVar[int]
    USED_CONSENSUS_GROUPS_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_CONSENSUS_GROUPS_FIELD_NUMBER: _ClassVar[int]
    RACK_FIELD_NUMBER: _ClassVar[int]
    node_id: int
    region: str
    zone: str
    node_class: str
    consensus_voter_node_ids: _containers.RepeatedScalarFieldContainer[int]
    max_consensus_groups: int
    used_consensus_groups: int
    available_consensus_groups: int
    rack: str
    def __init__(self, node_id: _Optional[int] = ..., region: _Optional[str] = ..., zone: _Optional[str] = ..., node_class: _Optional[str] = ..., consensus_voter_node_ids: _Optional[_Iterable[int]] = ..., max_consensus_groups: _Optional[int] = ..., used_consensus_groups: _Optional[int] = ..., available_consensus_groups: _Optional[int] = ..., rack: _Optional[str] = ...) -> None: ...

class PlacementStatus(_message.Message):
    __slots__ = ("allowed_regions", "minimum_zones", "required_node_class", "achieved_zones", "nodes", "minimum_racks", "excluded_node_ids", "achieved_racks")
    ALLOWED_REGIONS_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_ZONES_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_NODE_CLASS_FIELD_NUMBER: _ClassVar[int]
    ACHIEVED_ZONES_FIELD_NUMBER: _ClassVar[int]
    NODES_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_RACKS_FIELD_NUMBER: _ClassVar[int]
    EXCLUDED_NODE_IDS_FIELD_NUMBER: _ClassVar[int]
    ACHIEVED_RACKS_FIELD_NUMBER: _ClassVar[int]
    allowed_regions: _containers.RepeatedScalarFieldContainer[str]
    minimum_zones: int
    required_node_class: str
    achieved_zones: int
    nodes: _containers.RepeatedCompositeFieldContainer[RegionalNodeObservation]
    minimum_racks: int
    excluded_node_ids: _containers.RepeatedScalarFieldContainer[int]
    achieved_racks: int
    def __init__(self, allowed_regions: _Optional[_Iterable[str]] = ..., minimum_zones: _Optional[int] = ..., required_node_class: _Optional[str] = ..., achieved_zones: _Optional[int] = ..., nodes: _Optional[_Iterable[_Union[RegionalNodeObservation, _Mapping[str, object]]]] = ..., minimum_racks: _Optional[int] = ..., excluded_node_ids: _Optional[_Iterable[int]] = ..., achieved_racks: _Optional[int] = ...) -> None: ...

class ResourceStatus(_message.Message):
    __slots__ = ("phase", "observed_generation", "deployment_mode", "achieved_durability", "resource_epoch", "conditions", "tablets", "placement", "catalog_generation")
    PHASE_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_GENERATION_FIELD_NUMBER: _ClassVar[int]
    DEPLOYMENT_MODE_FIELD_NUMBER: _ClassVar[int]
    ACHIEVED_DURABILITY_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    CONDITIONS_FIELD_NUMBER: _ClassVar[int]
    TABLETS_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    CATALOG_GENERATION_FIELD_NUMBER: _ClassVar[int]
    phase: ResourcePhase
    observed_generation: int
    deployment_mode: DeploymentMode
    achieved_durability: DurabilityProfile
    resource_epoch: int
    conditions: _containers.RepeatedCompositeFieldContainer[Condition]
    tablets: _containers.RepeatedCompositeFieldContainer[TabletDescriptor]
    placement: PlacementStatus
    catalog_generation: int
    def __init__(self, phase: _Optional[_Union[ResourcePhase, str]] = ..., observed_generation: _Optional[int] = ..., deployment_mode: _Optional[_Union[DeploymentMode, str]] = ..., achieved_durability: _Optional[_Union[DurabilityProfile, str]] = ..., resource_epoch: _Optional[int] = ..., conditions: _Optional[_Iterable[_Union[Condition, _Mapping[str, object]]]] = ..., tablets: _Optional[_Iterable[_Union[TabletDescriptor, _Mapping[str, object]]]] = ..., placement: _Optional[_Union[PlacementStatus, _Mapping[str, object]]] = ..., catalog_generation: _Optional[int] = ...) -> None: ...

class Resource(_message.Message):
    __slots__ = ("name", "generation", "spec", "status", "create_time", "update_time")
    NAME_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATE_TIME_FIELD_NUMBER: _ClassVar[int]
    UPDATE_TIME_FIELD_NUMBER: _ClassVar[int]
    name: ResourceName
    generation: int
    spec: ResourceSpec
    status: ResourceStatus
    create_time: _timestamp_pb2.Timestamp
    update_time: _timestamp_pb2.Timestamp
    def __init__(self, name: _Optional[_Union[ResourceName, _Mapping[str, object]]] = ..., generation: _Optional[int] = ..., spec: _Optional[_Union[ResourceSpec, _Mapping[str, object]]] = ..., status: _Optional[_Union[ResourceStatus, _Mapping[str, object]]] = ..., create_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping[str, object]]] = ..., update_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping[str, object]]] = ...) -> None: ...
