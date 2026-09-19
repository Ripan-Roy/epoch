//! Regional catalog administration API backed by the catalog consensus group.

use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use epoch_bus::{BusConfig, EventBus};
use epoch_cache::{CacheConfig, EvictionPolicy};
use epoch_catalog::{
    AcquireControlLease, ApplyDesiredResources, ApplyResource, CatalogChange, CatalogCommand,
    CatalogError, CatalogMutation, ControlLease, ControlLeaseGuard, DeleteDesiredResource,
    DeleteManagedResource, DeleteResource, DesiredResourceWrite, FinalizeTabletMembership,
    ImportManagedResources, ManagedResourceApplyResult, ManagedResourcePlacement,
    ManagedResourceRecord, NodeCapacityObservation, PlanManagedTabletMembership,
    PlanTabletMembership, ReconcileManagedResources, ResourceGeneration, ResourceGovernance,
    ResourceName, ResourceRecord, ResourceSpec, TabletDescriptor, TabletPlacement,
    UpdateManagedResourceStatus, catalog_proposal_id_for,
};
use epoch_consensus::{CommittedProposal, ConsensusError, ProposalLookup};
use epoch_core::{DurabilityProfile, ResourceKind, WorkloadProfile};
use epoch_queue::{Queue, QueueConfig};
use epoch_tablet::{MAX_CACHE_TABLET_ENTRIES, MAX_CACHE_TABLET_TIER_BYTES, MAX_CACHE_TTL_MS};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, broadcast};

use crate::{
    catalog_tablet::{
        CatalogTabletQueryError, CatalogTabletReceipt, CatalogTabletService, CatalogTabletSnapshot,
    },
    consensus::{ConsensusProbeApiError, ConsensusProbeError, ConsensusProbeHandle},
    tablet_http::{
        deserialize_optional_u64_from_number_or_decimal, deserialize_u64_from_number_or_decimal,
        deserialize_vec_u64_from_number_or_decimal, serialize_u64_as_decimal,
    },
    tablet_materializer::{
        RegionalTabletMaterializer, TabletMaterializerError, TabletReconcileOutcome,
    },
};

pub const REGIONAL_CATALOG_PATH: &str = "/experimental/v1/regional/catalog";
pub const REGIONAL_CATALOG_RESOURCE_PATH: &str = "/experimental/v1/regional/catalog/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}";
pub const REGIONAL_CATALOG_TABLET_MEMBERSHIP_PATH: &str =
    "/experimental/v1/regional/catalog/tablets/{tablet_id}/membership";
pub const REGIONAL_CONTROL_RESOURCES_PATH: &str = "/experimental/v1/regional/control/resources";
pub const REGIONAL_CONTROL_IMPORT_PATH: &str = "/experimental/v1/regional/control/import";
pub const REGIONAL_CONTROL_RESOURCE_PATH: &str = "/experimental/v1/regional/control/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}";
pub const REGIONAL_CONTROL_STATUS_PATH: &str = "/experimental/v1/regional/control/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}/status";
pub const REGIONAL_CONTROL_MATERIALIZATION_PATH: &str = "/experimental/v1/regional/control/materializations/{organization}/{project}/{environment}/{namespace}/{kind}/{name}";
pub const REGIONAL_CONTROL_LEASE_PATH: &str = "/experimental/v1/regional/control/lease";
pub const REGIONAL_CONTROL_RECONCILE_PATH: &str = "/experimental/v1/regional/control/reconcile";
pub const REGIONAL_CONTROL_TABLET_MEMBERSHIP_PATH: &str =
    "/experimental/v1/regional/control/tablets/{tablet_id}/membership";
pub const REGIONAL_CONTROL_OPERATION_PATH: &str =
    "/experimental/v1/regional/control/operations/{request_token}";
pub const REGIONAL_CONTROL_CHANGES_PATH: &str = "/experimental/v1/regional/control/changes";
pub const REGIONAL_CONTROL_ALLOCATIONS_PATH: &str = "/experimental/v1/regional/control/allocations";
const CATALOG_REQUEST_BODY_BYTES: usize = 512 * 1024;
const CONTROL_RESOURCE_PAGE_BYTES: usize = 768 * 1024;
const MAX_CONTROL_RESOURCE_PAGE_SIZE: usize = 128;
pub const DEFAULT_CATALOG_COMMIT_WAIT: Duration = Duration::from_secs(5);

pub type SharedRegionalTabletMaterializer = Arc<Mutex<RegionalTabletMaterializer>>;

#[derive(Debug, Clone)]
pub struct RegionalCatalogState {
    catalog: Arc<CatalogTabletService>,
    consensus: ConsensusProbeHandle,
    materializer: SharedRegionalTabletMaterializer,
    commit_wait: Duration,
    write_serial: Arc<Mutex<()>>,
}

impl RegionalCatalogState {
    pub fn new(
        catalog: Arc<CatalogTabletService>,
        consensus: ConsensusProbeHandle,
        materializer: SharedRegionalTabletMaterializer,
        commit_wait: Duration,
    ) -> Result<Self, String> {
        if catalog.scope().group_id() != consensus.group_id().get()
            || catalog.scope().group_epoch() != consensus.group_epoch().get()
        {
            return Err("catalog service scope does not match its consensus handle".into());
        }
        if commit_wait.is_zero() {
            return Err("catalog commit wait must be non-zero".into());
        }
        Ok(Self {
            catalog,
            consensus,
            materializer,
            commit_wait,
            write_serial: Arc::new(Mutex::new(())),
        })
    }

    pub fn subscribe_commits(&self) -> broadcast::Receiver<CommittedProposal> {
        self.consensus.subscribe_commits()
    }

    pub(crate) fn consensus_handle(&self) -> ConsensusProbeHandle {
        self.consensus.clone()
    }

    pub(crate) fn catalog_snapshot(&self) -> Result<CatalogTabletSnapshot, String> {
        self.catalog.snapshot()
    }

    pub(crate) async fn finalize_membership(
        &self,
        request: FinalizeTabletMembership,
    ) -> Result<CatalogTabletReceipt, RegionalCatalogApiError> {
        let (receipt, _) = commit_command_with_mode(
            self,
            CatalogCommand::FinalizeMembership(request),
            CatalogSubmissionMode::Forwarded,
        )
        .await?;
        self.reconcile_latest().await?;
        Ok(receipt)
    }

    pub async fn reconcile_latest(
        &self,
    ) -> Result<TabletReconcileOutcome, RegionalCatalogApiError> {
        let snapshot = self
            .catalog
            .snapshot()
            .map_err(RegionalCatalogApiError::CatalogState)?;
        self.materializer
            .lock()
            .await
            .reconcile(&snapshot.resources)
            .await
            .map_err(RegionalCatalogApiError::Materializer)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct CatalogResourcePath {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    kind: String,
    name: String,
}

impl CatalogResourcePath {
    fn resource_name(&self) -> Result<ResourceName, RegionalCatalogApiError> {
        ResourceName::new(
            &self.organization,
            &self.project,
            &self.environment,
            &self.namespace,
            parse_resource_kind(&self.kind)?,
            &self.name,
        )
        .map_err(RegionalCatalogApiError::Catalog)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyResourceRequest {
    request_token: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
    )]
    expected_generation: Option<u64>,
    shard_count: u32,
    replica_count: u16,
    #[serde(default)]
    tablet_placements: Vec<TabletPlacement>,
    #[serde(default)]
    configuration: Option<serde_json::Value>,
    #[serde(default)]
    governance: Option<ResourceGovernance>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheCatalogConfiguration {
    #[serde(default)]
    shard_count: Option<u32>,
    #[serde(default = "default_cache_max_entries")]
    max_entries: usize,
    #[serde(default)]
    max_memory_bytes: Option<usize>,
    #[serde(default)]
    max_cold_bytes: Option<usize>,
    #[serde(default)]
    default_ttl_ms: Option<u64>,
    #[serde(default)]
    eviction: EvictionPolicy,
    #[serde(default = "default_regional_cache_durability")]
    durability: DurabilityProfile,
}

const fn default_cache_max_entries() -> usize {
    10_000
}

const fn default_regional_cache_durability() -> DurabilityProfile {
    DurabilityProfile::QuorumDurable
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteResourceRequest {
    request_token: String,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
    )]
    expected_generation: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanTabletMembershipRequest {
    request_token: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_tablet_epoch: u64,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_resource_generation: u64,
    #[serde(deserialize_with = "deserialize_vec_u64_from_number_or_decimal")]
    target_voter_node_ids: Vec<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplyDesiredResourcesRequest {
    request_token: String,
    resources: Vec<DesiredResourceWriteRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DesiredResourceWriteRequest {
    name: ResourceName,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u64_from_number_or_decimal"
    )]
    expected_generation: Option<u64>,
    desired: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportManagedResourcesRequest {
    request_token: String,
    resources: Vec<ImportedManagedResourceRequest>,
    generations: Vec<ImportedManagedGenerationRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportedManagedResourceRequest {
    name: ResourceName,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    generation: u64,
    desired: serde_json::Value,
    status: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportedManagedGenerationRequest {
    name: ResourceName,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    generation: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcquireControlLeaseRequest {
    request_token: String,
    owner_id: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    now_ms: u64,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    ttl_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlLeaseGuardRequest {
    owner_id: String,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    fence: u64,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    now_ms: u64,
}

impl From<ControlLeaseGuardRequest> for ControlLeaseGuard {
    fn from(request: ControlLeaseGuardRequest) -> Self {
        Self {
            owner_id: request.owner_id,
            fence: request.fence,
            now_ms: request.now_ms,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateManagedStatusRequest {
    request_token: String,
    lease: ControlLeaseGuardRequest,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_generation: u64,
    status: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteManagedResourceRequest {
    request_token: String,
    lease: ControlLeaseGuardRequest,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_desired_generation: u64,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_catalog_generation: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReconcileManagedResourcesRequest {
    request_token: String,
    lease: ControlLeaseGuardRequest,
    capacity: Vec<NodeCapacityObservationRequest>,
    resources: Vec<ManagedResourcePlacementRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanManagedTabletMembershipRequest {
    request_token: String,
    lease: ControlLeaseGuardRequest,
    capacity: Vec<NodeCapacityObservationRequest>,
    name: ResourceName,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_desired_generation: u64,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_tablet_epoch: u64,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_resource_generation: u64,
    #[serde(deserialize_with = "deserialize_vec_u64_from_number_or_decimal")]
    target_voter_node_ids: Vec<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeCapacityObservationRequest {
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    node_id: u64,
    max_consensus_groups: u32,
    used_consensus_groups: u32,
    catalog_groups: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedResourcePlacementRequest {
    name: ResourceName,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_desired_generation: u64,
    #[serde(deserialize_with = "deserialize_u64_from_number_or_decimal")]
    expected_catalog_generation: u64,
    spec: ResourceSpec,
    tablet_placements: Vec<TabletPlacement>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlChangesQuery {
    #[serde(default, deserialize_with = "deserialize_u64_from_number_or_decimal")]
    after: u64,
    #[serde(default = "default_control_change_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlResourcesQuery {
    #[serde(default)]
    after: Option<String>,
    #[serde(default = "default_control_resource_limit")]
    limit: usize,
}

const fn default_control_resource_limit() -> usize {
    MAX_CONTROL_RESOURCE_PAGE_SIZE
}

const fn default_control_change_limit() -> usize {
    100
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogTabletResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub tablet_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub consensus_group_id: u64,
    pub shard_index: u32,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub tablet_epoch: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub resource_generation: u64,
    pub workload_profile: WorkloadProfile,
    pub replica_count: u16,
    pub voter_node_ids: Vec<String>,
    pub bootstrap_voter_node_ids: Vec<String>,
    pub target_voter_node_ids: Vec<String>,
}

impl From<&TabletDescriptor> for CatalogTabletResponse {
    fn from(descriptor: &TabletDescriptor) -> Self {
        Self {
            tablet_id: descriptor.tablet_id,
            consensus_group_id: descriptor.consensus_group_id,
            shard_index: descriptor.shard_index,
            tablet_epoch: descriptor.tablet_epoch,
            resource_generation: descriptor.resource_generation,
            workload_profile: descriptor.workload_profile,
            replica_count: descriptor.replica_count,
            voter_node_ids: descriptor
                .voter_node_ids
                .iter()
                .map(u64::to_string)
                .collect(),
            bootstrap_voter_node_ids: descriptor
                .bootstrap_voter_node_ids
                .iter()
                .map(u64::to_string)
                .collect(),
            target_voter_node_ids: descriptor
                .target_voter_node_ids
                .iter()
                .map(u64::to_string)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogResourceResponse {
    pub name: ResourceName,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub generation: u64,
    pub workload_profile: WorkloadProfile,
    pub shard_count: u32,
    pub replica_count: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub governance: Option<ResourceGovernance>,
    pub tablets: Vec<CatalogTabletResponse>,
}

impl From<&ResourceRecord> for CatalogResourceResponse {
    fn from(resource: &ResourceRecord) -> Self {
        Self {
            name: resource.name.clone(),
            generation: resource.generation,
            workload_profile: resource.spec.workload_profile,
            shard_count: resource.spec.shard_count,
            replica_count: resource.spec.replica_count,
            configuration: resource.spec.configuration.clone(),
            governance: resource.spec.governance.clone(),
            tablets: resource.tablets.iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ManagedResourceResponse {
    name: ResourceName,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    generation: u64,
    desired: serde_json::Value,
    status: serde_json::Value,
    deletion_requested: bool,
}

impl From<&ManagedResourceRecord> for ManagedResourceResponse {
    fn from(resource: &ManagedResourceRecord) -> Self {
        Self {
            name: resource.name.clone(),
            generation: resource.generation,
            desired: resource.desired.clone(),
            status: resource.status.clone(),
            deletion_requested: resource.deletion_requested,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ManagedResourceApplyResultResponse {
    resource: ManagedResourceResponse,
    created: bool,
    changed: bool,
}

impl From<&ManagedResourceApplyResult> for ManagedResourceApplyResultResponse {
    fn from(result: &ManagedResourceApplyResult) -> Self {
        Self {
            resource: (&result.resource).into(),
            created: result.created,
            changed: result.changed,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ControlLeaseResponse {
    owner_id: String,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    fence: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    valid_until_ms: u64,
}

impl From<&ControlLease> for ControlLeaseResponse {
    fn from(lease: &ControlLease) -> Self {
        Self {
            owner_id: lease.owner_id.clone(),
            fence: lease.fence,
            valid_until_ms: lease.valid_until_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct CatalogChangeResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    cursor: u64,
    kind: epoch_catalog::CatalogChangeKind,
    name: ResourceName,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    generation: u64,
}

impl From<&CatalogChange> for CatalogChangeResponse {
    fn from(change: &CatalogChange) -> Self {
        Self {
            cursor: change.cursor,
            kind: change.kind,
            name: change.name.clone(),
            generation: change.generation,
        }
    }
}

#[derive(Debug, Serialize)]
struct CatalogSnapshotResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    group_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    group_epoch: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    last_applied_index: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    applied_command_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    resource_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    tablet_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    managed_resource_count: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    latest_change_cursor: u64,
    state_digest: String,
    resources: Vec<CatalogResourceResponse>,
    managed_resources: Vec<ManagedResourceResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    control_lease: Option<ControlLeaseResponse>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CatalogMutationResponse {
    Applied {
        resource: CatalogResourceResponse,
        created: bool,
        changed: bool,
        replayed: bool,
    },
    Deleted {
        name: ResourceName,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        generation: u64,
        deleted: bool,
        replayed: bool,
    },
    DesiredApplied {
        resources: Vec<ManagedResourceApplyResultResponse>,
        changed: bool,
        replayed: bool,
    },
    DesiredDeleted {
        name: ResourceName,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        generation: u64,
        deleted: bool,
        replayed: bool,
    },
    ManagedDeleted {
        name: ResourceName,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        desired_generation: u64,
        #[serde(serialize_with = "serialize_u64_as_decimal")]
        catalog_generation: u64,
        deleted: bool,
        replayed: bool,
    },
    ControlLeaseAcquired {
        lease: ControlLeaseResponse,
        replayed: bool,
    },
    ManagedStatusUpdated {
        resource: ManagedResourceResponse,
        changed: bool,
        replayed: bool,
    },
    ManagedReconciled {
        resources: Vec<CatalogResourceResponse>,
        changed: bool,
        replayed: bool,
    },
    Rejected {
        code: epoch_catalog::CatalogRejectionCode,
        message: String,
        replayed: bool,
    },
}

impl From<&CatalogMutation> for CatalogMutationResponse {
    fn from(mutation: &CatalogMutation) -> Self {
        match mutation {
            CatalogMutation::Applied {
                resource,
                created,
                changed,
                replayed,
            } => Self::Applied {
                resource: resource.into(),
                created: *created,
                changed: *changed,
                replayed: *replayed,
            },
            CatalogMutation::Deleted {
                name,
                generation,
                deleted,
                replayed,
            } => Self::Deleted {
                name: name.clone(),
                generation: *generation,
                deleted: *deleted,
                replayed: *replayed,
            },
            CatalogMutation::DesiredApplied {
                resources,
                changed,
                replayed,
            } => Self::DesiredApplied {
                resources: resources.iter().map(Into::into).collect(),
                changed: *changed,
                replayed: *replayed,
            },
            CatalogMutation::DesiredDeleted {
                name,
                generation,
                deleted,
                replayed,
            } => Self::DesiredDeleted {
                name: name.clone(),
                generation: *generation,
                deleted: *deleted,
                replayed: *replayed,
            },
            CatalogMutation::ManagedDeleted {
                name,
                desired_generation,
                catalog_generation,
                deleted,
                replayed,
            } => Self::ManagedDeleted {
                name: name.clone(),
                desired_generation: *desired_generation,
                catalog_generation: *catalog_generation,
                deleted: *deleted,
                replayed: *replayed,
            },
            CatalogMutation::ControlLeaseAcquired { lease, replayed } => {
                Self::ControlLeaseAcquired {
                    lease: lease.into(),
                    replayed: *replayed,
                }
            }
            CatalogMutation::ManagedStatusUpdated {
                resource,
                changed,
                replayed,
            } => Self::ManagedStatusUpdated {
                resource: resource.into(),
                changed: *changed,
                replayed: *replayed,
            },
            CatalogMutation::ManagedReconciled {
                resources,
                changed,
                replayed,
            } => Self::ManagedReconciled {
                resources: resources.iter().map(Into::into).collect(),
                changed: *changed,
                replayed: *replayed,
            },
            CatalogMutation::Rejected {
                code,
                message,
                replayed,
            } => Self::Rejected {
                code: *code,
                message: message.clone(),
                replayed: *replayed,
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct CatalogMutationReceiptResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    proposal_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    term: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    commit_index: u64,
    request_replayed: bool,
    mutation: CatalogMutationResponse,
    state_digest: String,
    materialization: TabletReconcileOutcome,
}

#[derive(Debug, Serialize)]
struct ControlMutationReceiptResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    proposal_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    term: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    commit_index: u64,
    request_replayed: bool,
    mutation: CatalogMutationResponse,
    state_digest: String,
}

#[derive(Debug, Serialize)]
struct ControlResourceListResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    latest_change_cursor: u64,
    resources: Vec<ManagedResourceResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_after: Option<ResourceName>,
}

#[derive(Debug, Serialize)]
struct ControlChangesResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    earliest_cursor: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    latest_cursor: u64,
    changes: Vec<CatalogChangeResponse>,
}

#[derive(Debug, Serialize)]
struct ControlNodeAllocationResponse {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    node_id: u64,
    catalog_groups: u32,
}

#[derive(Debug, Serialize)]
struct ControlAllocationsResponse {
    allocations: Vec<ControlNodeAllocationResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ControlOperationState {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Debug, Serialize)]
struct ControlOperationResponse {
    request_token: String,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    proposal_id: u64,
    state: ControlOperationState,
    resource_names: Vec<ResourceName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mutation: Option<CatalogMutationResponse>,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    first_change_cursor: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    last_change_cursor: u64,
}

pub fn regional_catalog_router(state: RegionalCatalogState) -> Router {
    Router::new()
        .route(REGIONAL_CATALOG_PATH, get(catalog_snapshot))
        .route(
            REGIONAL_CATALOG_RESOURCE_PATH,
            get(catalog_resource)
                .put(apply_resource)
                .delete(delete_resource),
        )
        .route(
            REGIONAL_CATALOG_TABLET_MEMBERSHIP_PATH,
            axum::routing::post(plan_tablet_membership),
        )
        .route(
            REGIONAL_CONTROL_RESOURCES_PATH,
            get(managed_resources).put(apply_desired_resources),
        )
        .route(
            REGIONAL_CONTROL_IMPORT_PATH,
            axum::routing::post(import_managed_resources),
        )
        .route(
            REGIONAL_CONTROL_RESOURCE_PATH,
            get(managed_resource).delete(delete_desired_resource),
        )
        .route(
            REGIONAL_CONTROL_STATUS_PATH,
            axum::routing::put(update_managed_status),
        )
        .route(
            REGIONAL_CONTROL_MATERIALIZATION_PATH,
            axum::routing::delete(delete_managed_resource),
        )
        .route(
            REGIONAL_CONTROL_LEASE_PATH,
            get(control_lease).post(acquire_control_lease),
        )
        .route(
            REGIONAL_CONTROL_RECONCILE_PATH,
            axum::routing::post(reconcile_managed_resources),
        )
        .route(
            REGIONAL_CONTROL_TABLET_MEMBERSHIP_PATH,
            axum::routing::post(plan_managed_tablet_membership),
        )
        .route(REGIONAL_CONTROL_OPERATION_PATH, get(control_operation))
        .route(REGIONAL_CONTROL_CHANGES_PATH, get(control_changes))
        .route(REGIONAL_CONTROL_ALLOCATIONS_PATH, get(control_allocations))
        .layer(DefaultBodyLimit::max(CATALOG_REQUEST_BODY_BYTES))
        .with_state(state)
}

async fn catalog_snapshot(
    State(state): State<RegionalCatalogState>,
) -> Result<Json<CatalogSnapshotResponse>, RegionalCatalogApiError> {
    let snapshot = state
        .catalog
        .snapshot()
        .map_err(RegionalCatalogApiError::CatalogState)?;
    Ok(Json(CatalogSnapshotResponse {
        group_id: snapshot.group_id,
        group_epoch: snapshot.group_epoch,
        last_applied_index: snapshot.last_applied_index,
        applied_command_count: snapshot.applied_command_count,
        resource_count: snapshot.resource_count,
        tablet_count: snapshot.tablet_count,
        managed_resource_count: snapshot.managed_resource_count,
        latest_change_cursor: snapshot.latest_change_cursor,
        state_digest: snapshot.state_digest,
        resources: snapshot.resources.iter().map(Into::into).collect(),
        managed_resources: snapshot.managed_resources.iter().map(Into::into).collect(),
        control_lease: snapshot.control_lease.as_ref().map(Into::into),
    }))
}

async fn catalog_resource(
    State(state): State<RegionalCatalogState>,
    Path(path): Path<CatalogResourcePath>,
) -> Result<Json<CatalogResourceResponse>, RegionalCatalogApiError> {
    let name = path.resource_name()?;
    let snapshot = state
        .catalog
        .snapshot()
        .map_err(RegionalCatalogApiError::CatalogState)?;
    snapshot
        .resources
        .iter()
        .find(|resource| resource.name == name)
        .map(CatalogResourceResponse::from)
        .map(Json)
        .ok_or_else(|| {
            RegionalCatalogApiError::Catalog(CatalogError::NotFound(name.canonical_name()))
        })
}

async fn managed_resources(
    State(state): State<RegionalCatalogState>,
    Query(query): Query<ControlResourcesQuery>,
) -> Result<Json<ControlResourceListResponse>, RegionalCatalogApiError> {
    if query.limit == 0 || query.limit > MAX_CONTROL_RESOURCE_PAGE_SIZE {
        return Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(
            format!(
                "control resource page size must be between 1 and {MAX_CONTROL_RESOURCE_PAGE_SIZE}"
            ),
        )));
    }
    let after = query
        .after
        .as_deref()
        .map(serde_json::from_str::<ResourceName>)
        .transpose()
        .map_err(|error| {
            RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(format!(
                "control resource page cursor is invalid: {error}"
            )))
        })?;
    if let Some(after) = &after {
        after.validate().map_err(RegionalCatalogApiError::Catalog)?;
    }
    control_read_barrier(&state).await?;
    let snapshot = state
        .catalog
        .snapshot()
        .map_err(RegionalCatalogApiError::CatalogState)?;
    let candidates = snapshot
        .managed_resources
        .iter()
        .filter(|resource| after.as_ref().is_none_or(|after| resource.name > *after))
        .take(query.limit)
        .map(ManagedResourceResponse::from)
        .collect::<Vec<_>>();
    let mut resources = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        resources.push(candidate);
        let probe = ControlResourceListResponse {
            latest_change_cursor: snapshot.latest_change_cursor,
            resources: resources.clone(),
            next_page_after: None,
        };
        let encoded = serde_json::to_vec(&probe).map_err(|error| {
            RegionalCatalogApiError::CatalogState(format!("encode control resource page: {error}"))
        })?;
        if encoded.len() > CONTROL_RESOURCE_PAGE_BYTES && resources.len() > 1 {
            resources.pop();
            break;
        }
    }
    let last_name = resources.last().map(|resource| resource.name.clone());
    let has_more = last_name.as_ref().is_some_and(|last| {
        snapshot
            .managed_resources
            .iter()
            .any(|resource| resource.name > *last)
    });
    Ok(Json(ControlResourceListResponse {
        latest_change_cursor: snapshot.latest_change_cursor,
        resources,
        next_page_after: has_more.then_some(last_name).flatten(),
    }))
}

async fn managed_resource(
    State(state): State<RegionalCatalogState>,
    Path(path): Path<CatalogResourcePath>,
) -> Result<Json<ManagedResourceResponse>, RegionalCatalogApiError> {
    control_read_barrier(&state).await?;
    let name = path.resource_name()?;
    let snapshot = state
        .catalog
        .snapshot()
        .map_err(RegionalCatalogApiError::CatalogState)?;
    snapshot
        .managed_resources
        .iter()
        .find(|resource| resource.name == name)
        .map(ManagedResourceResponse::from)
        .map(Json)
        .ok_or_else(|| {
            RegionalCatalogApiError::Catalog(CatalogError::NotFound(name.canonical_name()))
        })
}

async fn delete_desired_resource(
    State(state): State<RegionalCatalogState>,
    Path(path): Path<CatalogResourcePath>,
    request: Result<Json<DeleteResourceRequest>, JsonRejection>,
) -> Result<Json<ControlMutationReceiptResponse>, RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let command = CatalogCommand::DeleteDesired(DeleteDesiredResource {
        request_token: request.request_token,
        expected_generation: request.expected_generation,
        name: path.resource_name()?,
    });
    let (receipt, request_replayed) = commit_command(&state, command).await?;
    ensure_control_mutation_accepted(&receipt)?;
    Ok(Json(control_mutation_response(receipt, request_replayed)))
}

async fn delete_managed_resource(
    State(state): State<RegionalCatalogState>,
    Path(path): Path<CatalogResourcePath>,
    request: Result<Json<DeleteManagedResourceRequest>, JsonRejection>,
) -> Result<Json<CatalogMutationReceiptResponse>, RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let command = CatalogCommand::DeleteManaged(DeleteManagedResource {
        request_token: request.request_token,
        lease: request.lease.into(),
        name: path.resource_name()?,
        expected_desired_generation: request.expected_desired_generation,
        expected_catalog_generation: request.expected_catalog_generation,
    });
    let (receipt, request_replayed) = commit_command(&state, command).await?;
    ensure_control_mutation_accepted(&receipt)?;
    let materialization = state.reconcile_latest().await?;
    Ok(Json(mutation_response(
        receipt,
        request_replayed,
        materialization,
    )))
}

async fn apply_desired_resources(
    State(state): State<RegionalCatalogState>,
    request: Result<Json<ApplyDesiredResourcesRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ControlMutationReceiptResponse>), RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let command = CatalogCommand::ApplyDesired(ApplyDesiredResources {
        request_token: request.request_token,
        resources: request
            .resources
            .into_iter()
            .map(|resource| DesiredResourceWrite {
                name: resource.name,
                expected_generation: resource.expected_generation,
                desired: resource.desired,
            })
            .collect(),
    });
    let (receipt, request_replayed) = commit_command(&state, command).await?;
    ensure_control_mutation_accepted(&receipt)?;
    Ok((
        StatusCode::OK,
        Json(control_mutation_response(receipt, request_replayed)),
    ))
}

async fn import_managed_resources(
    State(state): State<RegionalCatalogState>,
    request: Result<Json<ImportManagedResourcesRequest>, JsonRejection>,
) -> Result<Json<ControlMutationReceiptResponse>, RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let command = CatalogCommand::ImportManaged(ImportManagedResources {
        request_token: request.request_token,
        resources: request
            .resources
            .into_iter()
            .map(|resource| ManagedResourceRecord {
                name: resource.name,
                generation: resource.generation,
                desired: resource.desired,
                status: resource.status,
                deletion_requested: false,
            })
            .collect(),
        generations: request
            .generations
            .into_iter()
            .map(|generation| ResourceGeneration {
                name: generation.name,
                generation: generation.generation,
            })
            .collect(),
    });
    let (receipt, request_replayed) = commit_command(&state, command).await?;
    ensure_control_mutation_accepted(&receipt)?;
    Ok(Json(control_mutation_response(receipt, request_replayed)))
}

async fn acquire_control_lease(
    State(state): State<RegionalCatalogState>,
    request: Result<Json<AcquireControlLeaseRequest>, JsonRejection>,
) -> Result<Json<ControlMutationReceiptResponse>, RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let command = CatalogCommand::AcquireControlLease(AcquireControlLease {
        request_token: request.request_token,
        owner_id: request.owner_id,
        now_ms: request.now_ms,
        ttl_ms: request.ttl_ms,
    });
    let (receipt, request_replayed) = commit_command(&state, command).await?;
    ensure_control_mutation_accepted(&receipt)?;
    Ok(Json(control_mutation_response(receipt, request_replayed)))
}

async fn control_lease(
    State(state): State<RegionalCatalogState>,
) -> Result<Json<ControlLeaseResponse>, RegionalCatalogApiError> {
    control_read_barrier(&state).await?;
    let snapshot = state
        .catalog
        .snapshot()
        .map_err(RegionalCatalogApiError::CatalogState)?;
    snapshot
        .control_lease
        .as_ref()
        .map(ControlLeaseResponse::from)
        .map(Json)
        .ok_or_else(|| {
            RegionalCatalogApiError::Catalog(CatalogError::NotFound("control lease".into()))
        })
}

async fn update_managed_status(
    State(state): State<RegionalCatalogState>,
    Path(path): Path<CatalogResourcePath>,
    request: Result<Json<UpdateManagedStatusRequest>, JsonRejection>,
) -> Result<Json<ControlMutationReceiptResponse>, RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let command = CatalogCommand::UpdateManagedStatus(UpdateManagedResourceStatus {
        request_token: request.request_token,
        lease: request.lease.into(),
        name: path.resource_name()?,
        expected_generation: request.expected_generation,
        status: request.status,
    });
    let (receipt, request_replayed) = commit_command(&state, command).await?;
    ensure_control_mutation_accepted(&receipt)?;
    Ok(Json(control_mutation_response(receipt, request_replayed)))
}

async fn reconcile_managed_resources(
    State(state): State<RegionalCatalogState>,
    request: Result<Json<ReconcileManagedResourcesRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<CatalogMutationReceiptResponse>), RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let mut resources = Vec::with_capacity(request.resources.len());
    for placement in request.resources {
        let mut spec = placement.spec;
        spec.configuration =
            normalize_profile_configuration(&placement.name, spec.shard_count, spec.configuration)?;
        resources.push(ManagedResourcePlacement {
            name: placement.name,
            expected_desired_generation: placement.expected_desired_generation,
            expected_catalog_generation: placement.expected_catalog_generation,
            spec,
            tablet_placements: placement.tablet_placements,
        });
    }
    let command = CatalogCommand::ReconcileManaged(ReconcileManagedResources {
        request_token: request.request_token,
        lease: request.lease.into(),
        capacity: request
            .capacity
            .into_iter()
            .map(|observation| NodeCapacityObservation {
                node_id: observation.node_id,
                max_consensus_groups: observation.max_consensus_groups,
                used_consensus_groups: observation.used_consensus_groups,
                catalog_groups: observation.catalog_groups,
            })
            .collect(),
        resources,
    });
    let (receipt, request_replayed) = commit_command(&state, command).await?;
    ensure_control_mutation_accepted(&receipt)?;
    let materialization = state.reconcile_latest().await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(mutation_response(
            receipt,
            request_replayed,
            materialization,
        )),
    ))
}

async fn plan_managed_tablet_membership(
    State(state): State<RegionalCatalogState>,
    Path(tablet_id): Path<u64>,
    request: Result<Json<PlanManagedTabletMembershipRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<CatalogMutationReceiptResponse>), RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let command = CatalogCommand::PlanManagedMembership(PlanManagedTabletMembership {
        request_token: request.request_token,
        lease: request.lease.into(),
        capacity: request
            .capacity
            .into_iter()
            .map(|observation| NodeCapacityObservation {
                node_id: observation.node_id,
                max_consensus_groups: observation.max_consensus_groups,
                used_consensus_groups: observation.used_consensus_groups,
                catalog_groups: observation.catalog_groups,
            })
            .collect(),
        name: request.name,
        expected_desired_generation: request.expected_desired_generation,
        tablet_id,
        expected_tablet_epoch: request.expected_tablet_epoch,
        expected_resource_generation: request.expected_resource_generation,
        target_voter_node_ids: request.target_voter_node_ids,
    });
    let (receipt, request_replayed) = commit_command(&state, command).await?;
    ensure_control_mutation_accepted(&receipt)?;
    let materialization = state.reconcile_latest().await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(mutation_response(
            receipt,
            request_replayed,
            materialization,
        )),
    ))
}

async fn control_operation(
    State(state): State<RegionalCatalogState>,
    Path(request_token): Path<String>,
) -> Result<Json<ControlOperationResponse>, RegionalCatalogApiError> {
    control_read_barrier(&state).await?;
    let proposal_id = catalog_proposal_id_for(
        state.catalog.scope().group_id(),
        state.catalog.scope().group_epoch(),
        &request_token,
    )?;
    if let Some(operation) = state
        .catalog
        .operation(&request_token)
        .map_err(RegionalCatalogApiError::CatalogState)?
    {
        let operation_state = if matches!(&operation.mutation, CatalogMutation::Rejected { .. }) {
            ControlOperationState::Failed
        } else {
            ControlOperationState::Succeeded
        };
        return Ok(Json(ControlOperationResponse {
            request_token,
            proposal_id,
            state: operation_state,
            resource_names: operation.resource_names,
            mutation: Some((&operation.mutation).into()),
            first_change_cursor: operation.first_change_cursor,
            last_change_cursor: operation.last_change_cursor,
        }));
    }
    match state.consensus.lookup(proposal_id).await? {
        ProposalLookup::Pending { .. } | ProposalLookup::Committed(_) => {
            Ok(Json(ControlOperationResponse {
                request_token,
                proposal_id,
                state: ControlOperationState::Pending,
                resource_names: Vec::new(),
                mutation: None,
                first_change_cursor: 0,
                last_change_cursor: 0,
            }))
        }
        ProposalLookup::Unknown => Err(RegionalCatalogApiError::Catalog(CatalogError::NotFound(
            format!("control operation {request_token}"),
        ))),
    }
}

async fn control_changes(
    State(state): State<RegionalCatalogState>,
    Query(query): Query<ControlChangesQuery>,
) -> Result<Json<ControlChangesResponse>, RegionalCatalogApiError> {
    control_read_barrier(&state).await?;
    let page = state
        .catalog
        .changes_after(query.after, query.limit)
        .map_err(|error| match error {
            CatalogTabletQueryError::Catalog(error) => RegionalCatalogApiError::Catalog(error),
            CatalogTabletQueryError::Unavailable(error) => {
                RegionalCatalogApiError::CatalogState(error)
            }
        })?;
    Ok(Json(ControlChangesResponse {
        earliest_cursor: page.earliest_cursor,
        latest_cursor: page.latest_cursor,
        changes: page.changes.iter().map(Into::into).collect(),
    }))
}

async fn control_allocations(
    State(state): State<RegionalCatalogState>,
) -> Result<Json<ControlAllocationsResponse>, RegionalCatalogApiError> {
    control_read_barrier(&state).await?;
    let snapshot = state
        .catalog
        .snapshot()
        .map_err(RegionalCatalogApiError::CatalogState)?;
    Ok(Json(ControlAllocationsResponse {
        allocations: snapshot
            .node_allocations
            .into_iter()
            .map(|(node_id, catalog_groups)| ControlNodeAllocationResponse {
                node_id,
                catalog_groups,
            })
            .collect(),
    }))
}

async fn control_read_barrier(state: &RegionalCatalogState) -> Result<(), RegionalCatalogApiError> {
    let status = state.consensus.status().await?;
    state
        .consensus
        .read_barrier(status.term.get(), state.commit_wait)
        .await?;
    Ok(())
}

async fn apply_resource(
    State(state): State<RegionalCatalogState>,
    Path(path): Path<CatalogResourcePath>,
    request: Result<Json<ApplyResourceRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<CatalogMutationReceiptResponse>), RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let name = path.resource_name()?;
    let workload_profile = profile_for_kind(name.kind)?;
    let configuration =
        normalize_profile_configuration(&name, request.shard_count, request.configuration)?;
    let command = CatalogCommand::Apply(ApplyResource {
        request_token: request.request_token,
        expected_generation: request.expected_generation,
        name,
        spec: ResourceSpec {
            workload_profile,
            shard_count: request.shard_count,
            replica_count: request.replica_count,
            configuration,
            governance: request.governance,
        },
        tablet_placements: request.tablet_placements,
    });
    let (receipt, request_replayed) = commit_command(&state, command).await?;
    let materialization = state.reconcile_latest().await?;
    let status = match &receipt.mutation {
        CatalogMutation::Applied { created: true, .. } => StatusCode::CREATED,
        _ => StatusCode::OK,
    };
    Ok((
        status,
        Json(mutation_response(
            receipt,
            request_replayed,
            materialization,
        )),
    ))
}

fn normalize_profile_configuration(
    resource: &ResourceName,
    shard_count: u32,
    configuration: Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, RegionalCatalogApiError> {
    let kind = resource.kind;
    if kind == ResourceKind::Queue {
        return normalize_queue_configuration(resource, configuration);
    }
    if kind == ResourceKind::EventBus {
        return normalize_bus_configuration(configuration);
    }
    if !matches!(kind, ResourceKind::Cache | ResourceKind::Table) {
        return Ok(None);
    }
    let Some(raw) = configuration else {
        return Ok(None);
    };
    let configuration: CacheCatalogConfiguration =
        serde_json::from_value(raw).map_err(|error| {
            RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(format!(
                "invalid Cache configuration: {error}"
            )))
        })?;
    if configuration
        .shard_count
        .is_some_and(|configured| configured != shard_count)
    {
        return Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(
            "Cache configuration.shard_count must match shard_count".into(),
        )));
    }
    if configuration.max_entries == 0 || configuration.max_entries > MAX_CACHE_TABLET_ENTRIES {
        return Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(
            format!("Cache max_entries must be between 1 and {MAX_CACHE_TABLET_ENTRIES}"),
        )));
    }
    if configuration
        .default_ttl_ms
        .is_some_and(|ttl| ttl == 0 || ttl > MAX_CACHE_TTL_MS)
    {
        return Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(
            format!("Cache default_ttl_ms must be between 1 and {MAX_CACHE_TTL_MS}"),
        )));
    }
    if !matches!(
        configuration.durability,
        DurabilityProfile::ReplicatedMemory | DurabilityProfile::QuorumDurable
    ) {
        return Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(
            "regional Cache durability must be replicated_memory or quorum_durable".into(),
        )));
    }
    for (name, capacity) in [
        ("max_memory_bytes", configuration.max_memory_bytes),
        ("max_cold_bytes", configuration.max_cold_bytes),
    ] {
        if capacity.is_some_and(|capacity| capacity == 0 || capacity > MAX_CACHE_TABLET_TIER_BYTES)
        {
            return Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(
                format!("Cache {name} must be between 1 and {MAX_CACHE_TABLET_TIER_BYTES}"),
            )));
        }
    }
    serde_json::to_value(CacheConfig {
        max_entries: configuration.max_entries,
        max_memory_bytes: configuration.max_memory_bytes,
        max_cold_bytes: configuration.max_cold_bytes,
        default_ttl_ms: configuration.default_ttl_ms,
        eviction: configuration.eviction,
        durability: configuration.durability,
    })
    .map(Some)
    .map_err(|error| RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(error.to_string())))
}

fn normalize_bus_configuration(
    configuration: Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, RegionalCatalogApiError> {
    let Some(raw) = configuration else {
        return Ok(None);
    };
    let mut configuration: BusConfig = serde_json::from_value(raw).map_err(|error| {
        RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(format!(
            "invalid Event Bus configuration: {error}"
        )))
    })?;
    if configuration.durability != DurabilityProfile::QuorumDurable {
        return Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(
            "regional Event Bus durability must be quorum_durable".into(),
        )));
    }
    configuration.delivery_outbox = true;
    EventBus::new(configuration.clone()).map_err(|error| {
        RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(format!(
            "invalid Event Bus configuration: {error}"
        )))
    })?;
    serde_json::to_value(configuration)
        .map(Some)
        .map_err(|error| {
            RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(error.to_string()))
        })
}

fn normalize_queue_configuration(
    resource: &ResourceName,
    configuration: Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, RegionalCatalogApiError> {
    let Some(raw) = configuration else {
        return Ok(None);
    };
    let configuration: QueueConfig = serde_json::from_value(raw).map_err(|error| {
        RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(format!(
            "invalid Queue configuration: {error}"
        )))
    })?;
    if !matches!(
        configuration.durability,
        DurabilityProfile::ReplicatedMemory | DurabilityProfile::QuorumDurable
    ) {
        return Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(
            "regional Queue durability must be replicated_memory or quorum_durable".into(),
        )));
    }
    let dead_letter_target = configuration
        .advanced
        .as_ref()
        .and_then(|advanced| advanced.dead_letter_target.as_deref());
    if let Some(target) = dead_letter_target {
        if configuration.durability != DurabilityProfile::QuorumDurable {
            return Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(
                "Queue dead-letter forwarding requires quorum_durable durability".into(),
            )));
        }
        if target == resource.name {
            return Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(
                "Queue dead-letter target must differ from the source Queue".into(),
            )));
        }
    }
    Queue::new(configuration.clone()).map_err(|error| {
        RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(error.to_string()))
    })?;
    serde_json::to_value(configuration)
        .map(Some)
        .map_err(|error| {
            RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(error.to_string()))
        })
}

async fn delete_resource(
    State(state): State<RegionalCatalogState>,
    Path(path): Path<CatalogResourcePath>,
    request: Result<Json<DeleteResourceRequest>, JsonRejection>,
) -> Result<Json<CatalogMutationReceiptResponse>, RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let command = CatalogCommand::Delete(DeleteResource {
        request_token: request.request_token,
        expected_generation: request.expected_generation,
        name: path.resource_name()?,
    });
    let (receipt, request_replayed) = commit_command(&state, command).await?;
    let materialization = state.reconcile_latest().await?;
    Ok(Json(mutation_response(
        receipt,
        request_replayed,
        materialization,
    )))
}

async fn plan_tablet_membership(
    State(state): State<RegionalCatalogState>,
    Path(tablet_id): Path<u64>,
    request: Result<Json<PlanTabletMembershipRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<CatalogMutationReceiptResponse>), RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let command = CatalogCommand::PlanMembership(PlanTabletMembership {
        request_token: request.request_token,
        tablet_id,
        expected_tablet_epoch: request.expected_tablet_epoch,
        expected_resource_generation: request.expected_resource_generation,
        target_voter_node_ids: request.target_voter_node_ids,
    });
    let (receipt, request_replayed) = commit_command(&state, command).await?;
    let materialization = state.reconcile_latest().await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(mutation_response(
            receipt,
            request_replayed,
            materialization,
        )),
    ))
}

fn mutation_response(
    receipt: CatalogTabletReceipt,
    request_replayed: bool,
    materialization: TabletReconcileOutcome,
) -> CatalogMutationReceiptResponse {
    CatalogMutationReceiptResponse {
        proposal_id: receipt.proposal_id,
        term: receipt.term,
        commit_index: receipt.commit_index,
        request_replayed,
        mutation: (&receipt.mutation).into(),
        state_digest: receipt.state_digest,
        materialization,
    }
}

fn control_mutation_response(
    receipt: CatalogTabletReceipt,
    request_replayed: bool,
) -> ControlMutationReceiptResponse {
    ControlMutationReceiptResponse {
        proposal_id: receipt.proposal_id,
        term: receipt.term,
        commit_index: receipt.commit_index,
        request_replayed,
        mutation: (&receipt.mutation).into(),
        state_digest: receipt.state_digest,
    }
}

fn ensure_control_mutation_accepted(
    receipt: &CatalogTabletReceipt,
) -> Result<(), RegionalCatalogApiError> {
    if let CatalogMutation::Rejected { code, message, .. } = &receipt.mutation {
        return Err(RegionalCatalogApiError::CommittedRejection {
            code: *code,
            message: message.clone(),
        });
    }
    Ok(())
}

async fn commit_command(
    state: &RegionalCatalogState,
    command: CatalogCommand,
) -> Result<(CatalogTabletReceipt, bool), RegionalCatalogApiError> {
    commit_command_with_mode(state, command, CatalogSubmissionMode::LeaderOnly).await
}

#[derive(Debug, Clone, Copy)]
enum CatalogSubmissionMode {
    LeaderOnly,
    Forwarded,
}

async fn commit_command_with_mode(
    state: &RegionalCatalogState,
    command: CatalogCommand,
    mode: CatalogSubmissionMode,
) -> Result<(CatalogTabletReceipt, bool), RegionalCatalogApiError> {
    state
        .catalog
        .ensure_healthy()
        .map_err(RegionalCatalogApiError::CatalogState)?;
    let request_token = command.request_token();
    let proposal_id = catalog_proposal_id_for(
        state.catalog.scope().group_id(),
        state.catalog.scope().group_epoch(),
        request_token,
    )?;
    let payload = command.encode()?;
    let _write_guard = state.write_serial.lock().await;
    let initial = state.consensus.lookup(proposal_id).await?;
    let request_replayed = !matches!(initial, ProposalLookup::Unknown);
    match initial {
        ProposalLookup::Unknown => {
            let status = state.consensus.status().await?;
            match mode {
                CatalogSubmissionMode::LeaderOnly => {
                    state
                        .consensus
                        .propose(proposal_id, status.term.get(), payload.clone())
                        .await?;
                }
                CatalogSubmissionMode::Forwarded => {
                    state
                        .consensus
                        .forward_propose(proposal_id, status.term.get(), payload.clone())
                        .await?;
                }
            }
        }
        ProposalLookup::Pending {
            payload: ref tracked,
        }
        | ProposalLookup::Committed(CommittedProposal {
            payload: ref tracked,
            ..
        }) if *tracked != payload => {
            return Err(RegionalCatalogApiError::Consensus(
                ConsensusError::ConflictingProposal(
                    epoch_consensus::ProposalId::new(proposal_id)
                        .expect("derived catalog proposal ID is nonzero"),
                )
                .into(),
            ));
        }
        ProposalLookup::Pending { .. } | ProposalLookup::Committed(_) => {}
    }

    let receipt = tokio::time::timeout(state.commit_wait, async {
        loop {
            if let Some(receipt) = state
                .catalog
                .receipt(proposal_id)
                .map_err(RegionalCatalogApiError::CatalogState)?
            {
                return Ok(receipt);
            }
            match state.consensus.lookup(proposal_id).await? {
                ProposalLookup::Committed(committed) if committed.payload != payload => {
                    return Err(RegionalCatalogApiError::Consensus(
                        ConsensusError::ConflictingProposal(
                            epoch_consensus::ProposalId::new(proposal_id)
                                .expect("derived catalog proposal ID is nonzero"),
                        )
                        .into(),
                    ));
                }
                ProposalLookup::Committed(committed) => {
                    if let Some(receipt) = state
                        .catalog
                        .durable_replay_receipt(&committed)
                        .map_err(RegionalCatalogApiError::CatalogState)?
                    {
                        return Ok(receipt);
                    }
                }
                ProposalLookup::Unknown => {
                    return Err(RegionalCatalogApiError::Inconsistent(
                        "catalog proposal disappeared after submission".into(),
                    ));
                }
                ProposalLookup::Pending { .. } => {}
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| RegionalCatalogApiError::CommitTimeout {
        proposal_id,
        wait: state.commit_wait,
    })??;
    Ok((receipt, request_replayed))
}

#[derive(Debug, Error)]
pub enum RegionalCatalogApiError {
    #[error(transparent)]
    Consensus(#[from] ConsensusProbeError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error("catalog state is unavailable: {0}")]
    CatalogState(String),
    #[error(transparent)]
    Materializer(#[from] TabletMaterializerError),
    #[error("invalid request body: {message}")]
    RequestBody { status: StatusCode, message: String },
    #[error("catalog proposal {proposal_id} did not commit within {wait:?}")]
    CommitTimeout { proposal_id: u64, wait: Duration },
    #[error("catalog runtime is inconsistent: {0}")]
    Inconsistent(String),
    #[error("catalog operation was rejected ({code:?}): {message}")]
    CommittedRejection {
        code: epoch_catalog::CatalogRejectionCode,
        message: String,
    },
}

#[derive(Debug, Serialize)]
struct CatalogErrorBody {
    code: &'static str,
    message: String,
    retryable: bool,
}

impl IntoResponse for RegionalCatalogApiError {
    fn into_response(self) -> Response {
        if let Self::Consensus(error) = self {
            return ConsensusProbeApiError::from(error).into_response();
        }
        let (status, code, retryable) = match &self {
            Self::Catalog(CatalogError::NotFound(_)) => {
                (StatusCode::NOT_FOUND, "catalog_not_found", false)
            }
            Self::Catalog(
                CatalogError::GenerationConflict { .. }
                | CatalogError::ProfileMismatch { .. }
                | CatalogError::ShardCountDecrease { .. }
                | CatalogError::IdempotencyConflict
                | CatalogError::ControlLeaseHeld { .. }
                | CatalogError::ControlLeaseFenced { .. }
                | CatalogError::ControlLeaseExpired { .. }
                | CatalogError::CapacityObservationConflict { .. }
                | CatalogError::CapacityExceeded { .. }
                | CatalogError::StaleChangeCursor { .. },
            ) => (StatusCode::CONFLICT, "catalog_conflict", false),
            Self::Catalog(_) | Self::RequestBody { .. } => {
                let status = match self {
                    Self::RequestBody { status, .. } => status,
                    _ => StatusCode::BAD_REQUEST,
                };
                (status, "invalid_catalog_request", false)
            }
            Self::CatalogState(_) | Self::Materializer(_) | Self::Inconsistent(_) => {
                (StatusCode::SERVICE_UNAVAILABLE, "catalog_unavailable", true)
            }
            Self::CommitTimeout { .. } => {
                (StatusCode::GATEWAY_TIMEOUT, "catalog_commit_timeout", true)
            }
            Self::CommittedRejection {
                code: epoch_catalog::CatalogRejectionCode::InvalidArgument,
                ..
            } => (StatusCode::BAD_REQUEST, "invalid_catalog_request", false),
            Self::CommittedRejection { .. } => (StatusCode::CONFLICT, "catalog_conflict", false),
            Self::Consensus(_) => unreachable!("consensus errors returned above"),
        };
        (
            status,
            Json(CatalogErrorBody {
                code,
                message: self.to_string(),
                retryable,
            }),
        )
            .into_response()
    }
}

fn request_body_error(rejection: &JsonRejection) -> RegionalCatalogApiError {
    RegionalCatalogApiError::RequestBody {
        status: rejection.status(),
        message: rejection.body_text(),
    }
}

fn parse_resource_kind(value: &str) -> Result<ResourceKind, RegionalCatalogApiError> {
    match value {
        "cache" => Ok(ResourceKind::Cache),
        "table" => Ok(ResourceKind::Table),
        "stream" => Ok(ResourceKind::Stream),
        "queue" => Ok(ResourceKind::Queue),
        "event-bus" => Ok(ResourceKind::EventBus),
        _ => Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidName(
            "kind must be cache, table, stream, queue, or event-bus".into(),
        ))),
    }
}

fn profile_for_kind(kind: ResourceKind) -> Result<WorkloadProfile, RegionalCatalogApiError> {
    match kind {
        ResourceKind::Cache | ResourceKind::Table => Ok(WorkloadProfile::CacheAndState),
        ResourceKind::Stream => Ok(WorkloadProfile::StreamLog),
        ResourceKind::Queue => Ok(WorkloadProfile::WorkQueue),
        ResourceKind::EventBus => Ok(WorkloadProfile::EventBus),
        ResourceKind::Subscription
        | ResourceKind::Schema
        | ResourceKind::Pipe
        | ResourceKind::Connector
        | ResourceKind::Policy => Err(RegionalCatalogApiError::Catalog(CatalogError::InvalidSpec(
            "resource kind is not data-bearing".into(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use axum::{body::Body, http::Request};
    use epoch_consensus::ConsensusRole;
    use epoch_core::ManualClock;
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::{net::TcpListener, task::JoinHandle};
    use tower::ServiceExt;
    use url::Url;

    use super::*;
    use crate::{
        catalog_tablet::CatalogTabletScope,
        consensus::{CommittedProposalApplier, ConsensusProbeConfig, TEST_CONSENSUS_TICK_INTERVAL},
        consensus_groups::{ConsensusGroupSupervisor, shared_internal_peer_router},
        regional_router::{
            READ_CONSISTENCY_HEADER, RESOURCE_GENERATION_HEADER, TABLET_EPOCH_HEADER,
            regional_tablet_router,
        },
        tablet_materializer::TabletDirectory,
    };

    struct RegionalTestNode {
        state: RegionalCatalogState,
        materializer: SharedRegionalTabletMaterializer,
        directory: TabletDirectory,
        app: Router,
        peer_server: JoinHandle<()>,
    }

    async fn start_cluster(root: &TempDir) -> Vec<RegionalTestNode> {
        let listeners = bind_three_listeners().await;
        let peers = listeners
            .iter()
            .enumerate()
            .map(|(index, listener)| {
                let node_id = u64::try_from(index + 1).expect("node ID fits");
                let address = listener.local_addr().expect("listener has address");
                (
                    node_id,
                    Url::parse(&format!("http://{address}/")).expect("peer URL should parse"),
                )
            })
            .collect::<Vec<_>>();
        let mut nodes = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let node_id = u64::try_from(index + 1).expect("node ID fits");
            let config = ConsensusProbeConfig::new(
                node_id,
                1,
                1,
                peers.clone(),
                TEST_CONSENSUS_TICK_INTERVAL,
            )
            .expect("catalog config should be valid");
            let catalog = CatalogTabletService::new(CatalogTabletScope::new(1, 1).unwrap());
            let mut supervisor =
                ConsensusGroupSupervisor::new(node_id, 16).expect("supervisor should be valid");
            let stable_path = node_path(root, node_id, 1);
            std::fs::create_dir_all(stable_path.parent().unwrap()).unwrap();
            let applier: Arc<dyn CommittedProposalApplier> = catalog.clone();
            let consensus = supervisor
                .start_group(config.clone(), &stable_path, Some(applier))
                .await
                .expect("catalog group should start");
            let materializer = RegionalTabletMaterializer::new(
                supervisor,
                config,
                root.path().join(format!("node-{node_id}")),
                Arc::new(ManualClock::new(1_000)),
                Duration::from_secs(2),
            )
            .expect("materializer should start");
            let directory = materializer.directory();
            let peer_router = shared_internal_peer_router(materializer.peer_registry());
            let materializer = Arc::new(Mutex::new(materializer));
            let state = RegionalCatalogState::new(
                catalog,
                consensus,
                Arc::clone(&materializer),
                Duration::from_secs(3),
            )
            .expect("catalog state should be valid");
            let app = regional_catalog_router(state.clone())
                .merge(regional_tablet_router(directory.clone()));
            let peer_server = tokio::spawn(async move {
                axum::serve(listener, peer_router)
                    .await
                    .expect("peer server should run");
            });
            nodes.push(RegionalTestNode {
                state,
                materializer,
                directory,
                app,
                peer_server,
            });
        }
        nodes
    }

    async fn bind_three_listeners() -> Vec<TcpListener> {
        let mut listeners = Vec::new();
        for _ in 0..3 {
            listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        listeners
    }

    fn node_path(root: &TempDir, node_id: u64, group_id: u64) -> PathBuf {
        root.path()
            .join(format!("node-{node_id}"))
            .join("consensus")
            .join(format!("group-{group_id}"))
            .join(format!("node-{node_id}.wal"))
    }

    async fn leader_index(nodes: &[RegionalTestNode]) -> usize {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut leaders = Vec::new();
                for (index, node) in nodes.iter().enumerate() {
                    let status = node.state.consensus.status().await.unwrap();
                    if status.role == ConsensusRole::Leader {
                        leaders.push(index);
                    }
                }
                if let [leader] = leaders.as_slice() {
                    return *leader;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("one catalog leader should be elected")
    }

    async fn response_json(response: Response) -> Value {
        serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("body should collect")
                .to_bytes(),
        )
        .expect("response should contain JSON")
    }

    fn catalog_resource_path() -> &'static str {
        "/experimental/v1/regional/catalog/resources/acme/shop/dev/core/stream/orders"
    }

    fn regional_data_path(operation: &str) -> String {
        format!(
            "/experimental/v1/regional/resources/acme/shop/dev/core/stream/orders/shards/0/data/{operation}"
        )
    }

    fn cache_catalog_resource_path() -> &'static str {
        "/experimental/v1/regional/catalog/resources/acme/shop/dev/core/cache/sessions"
    }

    fn cache_data_path(operation: &str) -> String {
        format!(
            "/experimental/v1/regional/resources/acme/shop/dev/core/cache/sessions/shards/0/data/{operation}"
        )
    }

    async fn create_stream_resource(nodes: &[RegionalTestNode]) -> Value {
        let catalog_leader = leader_index(nodes).await;
        let response = nodes[catalog_leader]
            .app
            .clone()
            .oneshot(
                Request::put(catalog_resource_path())
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "request_token": "create-orders-v1",
                            "expected_generation": "0",
                            "shard_count": 1,
                            "replica_count": 3
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        response_json(response).await
    }

    async fn wait_for_catalog_resource_count(nodes: &[RegionalTestNode], expected: u64) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if nodes.iter().all(|node| {
                    node.state
                        .catalog
                        .snapshot()
                        .is_ok_and(|snapshot| snapshot.resource_count == expected)
                }) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("catalog resource count should converge on every voter");
    }

    async fn reconcile_all(nodes: &[RegionalTestNode]) {
        for node in nodes {
            node.state
                .reconcile_latest()
                .await
                .expect("each node should reconcile the committed catalog");
        }
    }

    fn stream_resource_name() -> ResourceName {
        ResourceName::new(
            "acme",
            "shop",
            "dev",
            "core",
            ResourceKind::Stream,
            "orders",
        )
        .unwrap()
    }

    fn cache_resource_name() -> ResourceName {
        ResourceName::new(
            "acme",
            "shop",
            "dev",
            "core",
            ResourceKind::Cache,
            "sessions",
        )
        .unwrap()
    }

    fn queue_resource_name() -> ResourceName {
        ResourceName::new("acme", "shop", "dev", "core", ResourceKind::Queue, "jobs").unwrap()
    }

    fn bus_resource_name() -> ResourceName {
        ResourceName::new(
            "acme",
            "shop",
            "dev",
            "core",
            ResourceKind::EventBus,
            "events",
        )
        .unwrap()
    }

    // One shared real three-node lifecycle is intentional: splitting the
    // phases would weaken the lease/failover/operation continuity proof.
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn replicated_control_api_fences_ownership_and_exposes_operations_and_changes() {
        let root = TempDir::new().unwrap();
        let nodes = start_cluster(&root).await;
        let leader = leader_index(&nodes).await;
        let desired = json!({
            "workload_profile": "WORKLOAD_PROFILE_STREAM_LOG",
            "replicas": 3,
            "configuration": {"shard_count": 1},
            "governance": {
                "owner": "team-payments",
                "cost_center": "payments",
                "classification": "DATA_CLASSIFICATION_INTERNAL"
            }
        });
        let response = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::put(REGIONAL_CONTROL_RESOURCES_PATH)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "request_token": "managed/orders%v1 canary",
                            "resources": [{
                                "name": stream_resource_name(),
                                "expected_generation": "0",
                                "desired": desired
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let applied = response_json(response).await;
        assert_eq!(applied["mutation"]["kind"], "desired_applied");
        assert_eq!(
            applied["mutation"]["resources"][0]["resource"]["generation"],
            "1"
        );
        assert_eq!(applied["mutation"]["resources"][0]["created"], true);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if nodes.iter().all(|node| {
                    node.state.catalog.snapshot().is_ok_and(|snapshot| {
                        snapshot.managed_resource_count == 1 && snapshot.latest_change_cursor == 1
                    })
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("managed desired state should replicate");

        let operation = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::get(
                    "/experimental/v1/regional/control/operations/managed%2Forders%25v1%20canary",
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(operation.status(), StatusCode::OK);
        let operation = response_json(operation).await;
        assert_eq!(operation["state"], "succeeded");
        assert_eq!(operation["resource_names"][0]["name"], "orders");
        assert_eq!(operation["first_change_cursor"], "1");

        let lease = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::post(REGIONAL_CONTROL_LEASE_PATH)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "request_token": "control-a-lease-1",
                            "owner_id": "control-a",
                            "now_ms": "1000",
                            "ttl_ms": "10000"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(lease.status(), StatusCode::OK);
        let lease = response_json(lease).await;
        assert_eq!(lease["mutation"]["lease"]["fence"], "1");

        let current_lease = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::get(REGIONAL_CONTROL_LEASE_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(current_lease.status(), StatusCode::OK);
        assert_eq!(response_json(current_lease).await["owner_id"], "control-a");

        let rejected_lease = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::post(REGIONAL_CONTROL_LEASE_PATH)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "request_token": "control-b-lease-too-early",
                            "owner_id": "control-b",
                            "now_ms": "1002",
                            "ttl_ms": "10000"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected_lease.status(), StatusCode::CONFLICT);
        let rejected_operation = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::get(
                    "/experimental/v1/regional/control/operations/control-b-lease-too-early",
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected_operation.status(), StatusCode::OK);
        let rejected_operation = response_json(rejected_operation).await;
        assert_eq!(rejected_operation["state"], "failed");
        assert_eq!(rejected_operation["mutation"]["kind"], "rejected");

        let reconcile = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::post(REGIONAL_CONTROL_RECONCILE_PATH)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "request_token": "managed-orders-reconcile-v1",
                            "lease": {"owner_id": "control-a", "fence": "1", "now_ms": "1001"},
                            "capacity": [
                                {"node_id": "1", "max_consensus_groups": 8, "used_consensus_groups": 1, "catalog_groups": 0},
                                {"node_id": "2", "max_consensus_groups": 8, "used_consensus_groups": 1, "catalog_groups": 0},
                                {"node_id": "3", "max_consensus_groups": 8, "used_consensus_groups": 1, "catalog_groups": 0}
                            ],
                            "resources": [{
                                "name": stream_resource_name(),
                                "expected_desired_generation": "1",
                                "expected_catalog_generation": "0",
                                "spec": {
                                    "workload_profile": "stream_log",
                                    "shard_count": 1,
                                    "replica_count": 3
                                },
                                "tablet_placements": [{"shard_index": 0, "voter_node_ids": [1, 2, 3]}]
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reconcile.status(), StatusCode::ACCEPTED);
        let reconcile = response_json(reconcile).await;
        assert_eq!(reconcile["mutation"]["kind"], "managed_reconciled");

        let tablet_id = reconcile["mutation"]["resources"][0]["tablets"][0]["tablet_id"]
            .as_str()
            .unwrap();
        let membership = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::post(format!(
                    "/experimental/v1/regional/control/tablets/{tablet_id}/membership"
                ))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "request_token": "managed-orders-membership-v1",
                        "lease": {"owner_id": "control-a", "fence": "1", "now_ms": "1002"},
                        "capacity": [
                            {"node_id": "1", "max_consensus_groups": 8, "used_consensus_groups": 2, "catalog_groups": 1},
                            {"node_id": "2", "max_consensus_groups": 8, "used_consensus_groups": 2, "catalog_groups": 1},
                            {"node_id": "3", "max_consensus_groups": 8, "used_consensus_groups": 2, "catalog_groups": 1}
                        ],
                        "name": stream_resource_name(),
                        "expected_desired_generation": "1",
                        "expected_tablet_epoch": "1",
                        "expected_resource_generation": "1",
                        "target_voter_node_ids": [1, 2, 3]
                    })
                    .to_string(),
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(membership.status(), StatusCode::ACCEPTED);
        let membership = response_json(membership).await;
        assert_eq!(membership["mutation"]["kind"], "applied");
        assert_eq!(membership["mutation"]["changed"], false);

        let status_path =
            "/experimental/v1/regional/control/resources/acme/shop/dev/core/stream/orders/status";
        let updated = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::put(status_path)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "request_token": "managed-orders-status-v1",
                            "lease": {"owner_id": "control-a", "fence": "1", "now_ms": "1003"},
                            "expected_generation": "1",
                            "status": {"phase": "ready", "observed_generation": "1"}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(updated.status(), StatusCode::OK);

        let changes = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::get("/experimental/v1/regional/control/changes?after=0&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(changes.status(), StatusCode::OK);
        let changes = response_json(changes).await;
        assert_eq!(changes["latest_cursor"], "2");
        assert_eq!(changes["changes"].as_array().unwrap().len(), 2);

        let materialization_path =
            "/experimental/v1/regional/control/materializations/acme/shop/dev/core/stream/orders";
        let deleted = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::delete(materialization_path)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "request_token": "managed-orders-delete-v1",
                            "lease": {"owner_id": "control-a", "fence": "1", "now_ms": "1004"},
                            "expected_desired_generation": "1",
                            "expected_catalog_generation": "1"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::OK);
        let deleted = response_json(deleted).await;
        assert_eq!(deleted["mutation"]["kind"], "managed_deleted");
        assert_eq!(deleted["mutation"]["deleted"], true);
        assert_eq!(deleted["mutation"]["desired_generation"], "2");
        assert_eq!(deleted["mutation"]["catalog_generation"], "2");

        let changes = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::get("/experimental/v1/regional/control/changes?after=2&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(changes.status(), StatusCode::OK);
        let changes = response_json(changes).await;
        assert_eq!(changes["latest_cursor"], "3");
        assert_eq!(changes["changes"].as_array().unwrap().len(), 1);
        assert_eq!(changes["changes"][0]["kind"], "desired_deleted");

        for node in nodes {
            node.peer_server.abort();
        }
    }

    #[tokio::test]
    async fn replicated_control_import_preserves_legacy_generation_high_water_marks() {
        let root = TempDir::new().unwrap();
        let nodes = start_cluster(&root).await;
        let leader = leader_index(&nodes).await;
        let desired = json!({
            "organization": "acme",
            "project": "shop",
            "environment": "dev",
            "namespace": "core",
            "kind": "stream",
            "name": "orders",
            "governance": {
                "owner": "team:platform",
                "cost_center": "cc-1042",
                "classification": "internal"
            },
            "spec": {"shard_count": 1, "replica_count": 3}
        });
        let imported = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::post(REGIONAL_CONTROL_IMPORT_PATH)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "request_token": "legacy-import-v1",
                            "resources": [{
                                "name": stream_resource_name(),
                                "generation": "7",
                                "desired": desired,
                                "status": {"phase": "ready", "observed_generation": 7, "catalog_generation": 7}
                            }],
                            "generations": [
                                {"name": ResourceName::new("acme", "shop", "dev", "core", ResourceKind::Stream, "audit").unwrap(), "generation": "4"},
                                {"name": stream_resource_name(), "generation": "7"}
                            ]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(imported.status(), StatusCode::OK);
        let imported = response_json(imported).await;
        assert_eq!(imported["mutation"]["kind"], "desired_applied");
        assert_eq!(
            imported["mutation"]["resources"][0]["resource"]["generation"],
            "7"
        );

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if nodes.iter().all(|node| {
                    node.state.catalog.snapshot().is_ok_and(|snapshot| {
                        snapshot.managed_resources.first().is_some_and(|resource| {
                            resource.name == stream_resource_name() && resource.generation == 7
                        })
                    })
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("legacy import should replicate");

        let missing_audit =
            "/experimental/v1/regional/control/resources/acme/shop/dev/core/stream/audit";
        let deleted = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::delete(missing_audit)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "request_token": "delete-missing-audit",
                            "expected_generation": "0"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::OK);
        let deleted = response_json(deleted).await;
        assert_eq!(deleted["mutation"]["generation"], "4");
        assert_eq!(deleted["mutation"]["deleted"], false);

        for node in nodes {
            node.peer_server.abort();
        }
    }

    #[tokio::test]
    async fn replicated_control_inventory_is_keyset_paginated() {
        let root = TempDir::new().unwrap();
        let nodes = start_cluster(&root).await;
        let leader = leader_index(&nodes).await;
        let names = ["audit", "orders", "payments"].map(|value| {
            ResourceName::new("acme", "shop", "dev", "core", ResourceKind::Stream, value).unwrap()
        });
        let desired = json!({
            "workload_profile": "WORKLOAD_PROFILE_STREAM_LOG",
            "replicas": 3,
            "configuration": {"shard_count": 1},
            "governance": {
                "owner": "team-payments",
                "cost_center": "payments",
                "classification": "DATA_CLASSIFICATION_INTERNAL"
            }
        });
        let resources = names
            .iter()
            .map(|name| {
                json!({
                    "name": name,
                    "expected_generation": "0",
                    "desired": desired
                })
            })
            .collect::<Vec<_>>();
        let response = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::put(REGIONAL_CONTROL_RESOURCES_PATH)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"request_token": "managed-page-v1", "resources": resources})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let first = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::get(format!("{REGIONAL_CONTROL_RESOURCES_PATH}?limit=2"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let first = response_json(first).await;
        let first_resources = first["resources"].as_array().unwrap();
        assert_eq!(first_resources.len(), 2);
        assert_eq!(first_resources[0]["name"]["name"], "audit");
        assert_eq!(first_resources[1]["name"]["name"], "orders");
        assert_eq!(first["next_page_after"]["name"], "orders");

        let cursor = serde_json::to_string(&first["next_page_after"]).unwrap();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("limit", "2")
            .append_pair("after", &cursor)
            .finish();
        let second = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::get(format!("{REGIONAL_CONTROL_RESOURCES_PATH}?{query}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        let second = response_json(second).await;
        assert_eq!(second["resources"].as_array().unwrap().len(), 1);
        assert_eq!(second["resources"][0]["name"]["name"], "payments");
        assert!(second.get("next_page_after").is_none());

        for node in nodes {
            node.peer_server.abort();
        }
    }

    #[test]
    fn omitted_cache_configuration_preserves_the_legacy_catalog_contract() {
        assert_eq!(
            normalize_profile_configuration(&cache_resource_name(), 1, None).unwrap(),
            None
        );
    }

    #[test]
    fn queue_configuration_is_validated_and_persisted_canonically() {
        let raw = json!({
            "durability": "quorum_durable",
            "visibility_timeout_ms": 5000,
            "max_messages": 100,
            "retry": {
                "strategy": "fixed",
                "initial_delay_ms": 10,
                "max_delay_ms": 10,
                "jitter_percent": 0,
                "max_attempts": 3,
                "max_age_ms": null
            },
            "dedupe_window_ms": 60000,
            "advanced": {
                "max_active_bytes": 1_048_576,
                "overflow": "dead_letter_oldest",
                "idle_expiry_ms": 600_000,
                "priority_aging_interval_ms": 10,
                "dispatch": {
                    "messages_per_second": 1000,
                    "burst": 100,
                    "max_in_flight": 100,
                    "failure_threshold": 2,
                    "open_interval_ms": 50
                },
                "dead_letter_target": "failed-jobs"
            }
        });
        let normalized = normalize_profile_configuration(&queue_resource_name(), 1, Some(raw))
            .expect("advanced Queue configuration should be accepted")
            .expect("configured Queue should retain its configuration");
        let config: QueueConfig = serde_json::from_value(normalized.clone()).unwrap();
        assert_eq!(config.durability, DurabilityProfile::QuorumDurable);
        let advanced = config
            .advanced
            .as_ref()
            .expect("advanced policy should survive");
        assert_eq!(advanced.max_active_bytes, Some(1_048_576));
        assert_eq!(advanced.dead_letter_target.as_deref(), Some("failed-jobs"));
        assert_eq!(
            normalized["advanced"]["overflow"],
            json!("dead_letter_oldest")
        );

        let mut volatile = normalized;
        volatile["durability"] = json!("volatile");
        assert!(
            normalize_profile_configuration(&queue_resource_name(), 1, Some(volatile)).is_err()
        );

        let mut memory_only = serde_json::to_value(&config).unwrap();
        memory_only["durability"] = json!("replicated_memory");
        assert!(
            normalize_profile_configuration(&queue_resource_name(), 1, Some(memory_only)).is_err()
        );

        let mut self_target = serde_json::to_value(&config).unwrap();
        self_target["advanced"]["dead_letter_target"] = json!("jobs");
        assert!(
            normalize_profile_configuration(&queue_resource_name(), 1, Some(self_target)).is_err()
        );
    }

    #[test]
    fn event_bus_retention_configuration_is_validated_and_persisted_canonically() {
        let raw = json!({
            "durability": "quorum_durable",
            "archive": true,
            "max_subscriptions": 1000,
            "max_archive_events": 10000,
            "archive_retention": {
                "max_events": 5000,
                "max_age_ms": 86_400_000
            },
            "max_outbox_deliveries": 20000
        });
        let normalized = normalize_profile_configuration(&bus_resource_name(), 1, Some(raw))
            .expect("Event Bus retention configuration should be accepted")
            .expect("configured Event Bus should retain its configuration");
        let config: BusConfig = serde_json::from_value(normalized.clone()).unwrap();
        assert_eq!(config.durability, DurabilityProfile::QuorumDurable);
        assert!(config.delivery_outbox);
        assert_eq!(config.archive_retention.max_events, Some(5_000));
        assert_eq!(config.archive_retention.max_age_ms, Some(86_400_000));

        let mut volatile = normalized.clone();
        volatile["durability"] = json!("volatile");
        assert!(normalize_profile_configuration(&bus_resource_name(), 1, Some(volatile)).is_err());
        let mut invalid_retention = normalized;
        invalid_retention["archive_retention"]["max_events"] = json!(20_000);
        assert!(
            normalize_profile_configuration(&bus_resource_name(), 1, Some(invalid_retention))
                .is_err()
        );
    }

    async fn data_leader(nodes: &[RegionalTestNode], resource: &ResourceName) -> (usize, u64) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut leaders = Vec::new();
                for (index, node) in nodes.iter().enumerate() {
                    let route = node
                        .directory
                        .resource_route(resource, 0)
                        .unwrap()
                        .expect("tablet should be materialized");
                    let status = route.consensus().status().await.unwrap();
                    if status.role == ConsensusRole::Leader {
                        leaders.push((index, status.term.get()));
                    }
                }
                if let [leader] = leaders.as_slice() {
                    return *leader;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("data tablet should elect one leader")
    }

    async fn append_stream_record(nodes: &[RegionalTestNode], leader: usize, term: u64) {
        let response = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::post(regional_data_path("records"))
                    .header("content-type", "application/json")
                    .header(RESOURCE_GENERATION_HEADER, "1")
                    .header(TABLET_EPOCH_HEADER, "1")
                    .body(Body::from(
                        json!({
                            "idempotency_key": "order-1",
                            "expected_term": term.to_string(),
                            "partition": 0,
                            "envelope": {
                                "id": "order-1",
                                "source": "catalog-api-test",
                                "type": "order.created",
                                "time_ms": "1000",
                                "payload": {"id": 1}
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "leader-routed append failed: {}",
            response.status()
        );
    }

    async fn cache_mutation(
        nodes: &[RegionalTestNode],
        leader: usize,
        term: u64,
        idempotency_key: &str,
        operation: Value,
    ) -> Value {
        let response = nodes[leader]
            .app
            .clone()
            .oneshot(
                Request::post(cache_data_path("mutations"))
                    .header("content-type", "application/json")
                    .header(RESOURCE_GENERATION_HEADER, "1")
                    .header(TABLET_EPOCH_HEADER, "1")
                    .body(Body::from(
                        json!({
                            "idempotency_key": idempotency_key,
                            "expected_term": term.to_string(),
                            "operation": operation
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "leader-routed Cache mutation failed: {}",
            response.status()
        );
        response_json(response).await
    }

    async fn assert_cache_values(nodes: &[RegionalTestNode]) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut converged = true;
                for node in nodes {
                    for (key, expected) in
                        [("alpha", Some("1")), ("beta", None), ("gamma", Some("3"))]
                    {
                        let response = node
                            .app
                            .clone()
                            .oneshot(
                                Request::get(format!(
                                    "{}?key={key}",
                                    cache_data_path("observations")
                                ))
                                .header(RESOURCE_GENERATION_HEADER, "1")
                                .header(TABLET_EPOCH_HEADER, "1")
                                .header(READ_CONSISTENCY_HEADER, "local_stale")
                                .body(Body::empty())
                                .unwrap(),
                            )
                            .await
                            .unwrap();
                        if response.status() != StatusCode::OK {
                            converged = false;
                            continue;
                        }
                        let document = response_json(response).await;
                        let observed = document["observation"]["item"]["value"]["value"].as_str();
                        if observed != expected {
                            converged = false;
                        }
                    }
                }
                if converged {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("evicted Cache state should converge on all voters");
    }

    async fn wait_for_stream_convergence(nodes: &[RegionalTestNode]) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut converged = true;
                for node in nodes {
                    let response = node
                        .app
                        .clone()
                        .oneshot(
                            Request::get(format!(
                                "{}?offset=0&limit=10",
                                regional_data_path("records")
                            ))
                            .header(RESOURCE_GENERATION_HEADER, "1")
                            .header(TABLET_EPOCH_HEADER, "1")
                            .header(READ_CONSISTENCY_HEADER, "local_stale")
                            .body(Body::empty())
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    if response.status() != StatusCode::OK
                        || response_json(response).await["records"]
                            .as_array()
                            .is_none_or(Vec::is_empty)
                    {
                        converged = false;
                    }
                }
                if converged {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("committed Stream record should converge on all voters");
    }

    async fn delete_stream_resource(nodes: &[RegionalTestNode]) {
        let catalog_leader = leader_index(nodes).await;
        let response = nodes[catalog_leader]
            .app
            .clone()
            .oneshot(
                Request::delete(catalog_resource_path())
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "request_token": "delete-orders-v2",
                            "expected_generation": "1"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn shutdown_cluster(nodes: &mut [RegionalTestNode]) {
        for node in nodes.iter() {
            node.materializer
                .lock()
                .await
                .shutdown()
                .await
                .expect("regional runtime should stop");
        }
        for node in nodes {
            node.peer_server.abort();
            let _ = (&mut node.peer_server).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn catalog_api_creates_routes_replicates_data_and_deletes_over_real_groups() {
        let root = TempDir::new().expect("temp directory should be created");
        let mut nodes = start_cluster(&root).await;
        let created = create_stream_resource(&nodes).await;
        assert_eq!(
            created["mutation"]["resource"]["tablets"][0]["consensus_group_id"], "2",
            "catalog group 1 must remain reserved"
        );
        assert_eq!(created["materialization"]["started"], 1);
        wait_for_catalog_resource_count(&nodes, 1).await;
        reconcile_all(&nodes).await;

        let resource = stream_resource_name();
        let (data_leader, data_term) = data_leader(&nodes, &resource).await;
        append_stream_record(&nodes, data_leader, data_term).await;
        wait_for_stream_convergence(&nodes).await;

        delete_stream_resource(&nodes).await;
        wait_for_catalog_resource_count(&nodes, 0).await;
        reconcile_all(&nodes).await;
        for node in &nodes {
            assert!(
                node.directory
                    .resource_route(&resource, 0)
                    .unwrap()
                    .is_none()
            );
        }
        shutdown_cluster(&mut nodes).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn configured_cache_evicts_after_committed_get_and_reopens_on_every_voter() {
        let root = TempDir::new().expect("temp directory should be created");
        let mut nodes = start_cluster(&root).await;
        let catalog_leader = leader_index(&nodes).await;
        let response = nodes[catalog_leader]
            .app
            .clone()
            .oneshot(
                Request::put(cache_catalog_resource_path())
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "request_token": "create-sessions-v1",
                            "expected_generation": "0",
                            "shard_count": 1,
                            "replica_count": 3,
                            "configuration": {
                                "shard_count": 1,
                                "max_entries": 2,
                                "default_ttl_ms": null,
                                "eviction": "all_keys_lru"
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let created = response_json(response).await;
        assert_eq!(
            created["mutation"]["resource"]["configuration"]["eviction"],
            "all_keys_lru"
        );
        wait_for_catalog_resource_count(&nodes, 1).await;
        reconcile_all(&nodes).await;

        let resource = cache_resource_name();
        let (leader, term) = data_leader(&nodes, &resource).await;
        cache_mutation(
            &nodes,
            leader,
            term,
            "set-alpha",
            json!({"kind": "set", "key": "alpha", "value": {"kind": "counter", "value": "1"}}),
        )
        .await;
        cache_mutation(
            &nodes,
            leader,
            term,
            "set-beta",
            json!({"kind": "set", "key": "beta", "value": {"kind": "counter", "value": "2"}}),
        )
        .await;
        let accessed = cache_mutation(
            &nodes,
            leader,
            term,
            "get-alpha",
            json!({"kind": "get", "key": "alpha"}),
        )
        .await;
        assert_eq!(accessed["receipt"]["outcome"]["result"]["kind"], "accessed");
        let admitted = cache_mutation(
            &nodes,
            leader,
            term,
            "set-gamma",
            json!({"kind": "set", "key": "gamma", "value": {"kind": "counter", "value": "3"}}),
        )
        .await;
        assert_eq!(
            admitted["receipt"]["outcome"]["result"]["evicted_keys"],
            json!(["beta"])
        );
        assert_cache_values(&nodes).await;

        shutdown_cluster(&mut nodes).await;
        let mut reopened = start_cluster(&root).await;
        wait_for_catalog_resource_count(&reopened, 1).await;
        reconcile_all(&reopened).await;
        assert_cache_values(&reopened).await;
        shutdown_cluster(&mut reopened).await;
    }
}
