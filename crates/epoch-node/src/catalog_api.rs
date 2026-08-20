//! Regional catalog administration API backed by the catalog consensus group.

use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use epoch_cache::{CacheConfig, EvictionPolicy};
use epoch_catalog::{
    ApplyResource, CatalogCommand, CatalogError, CatalogMutation, DeleteResource,
    ResourceGovernance, ResourceName, ResourceRecord, ResourceSpec, TabletDescriptor,
    catalog_proposal_id_for,
};
use epoch_consensus::{CommittedProposal, ConsensusError, ProposalLookup};
use epoch_core::{DurabilityProfile, ResourceKind, WorkloadProfile};
use epoch_tablet::{MAX_CACHE_TABLET_ENTRIES, MAX_CACHE_TABLET_TIER_BYTES, MAX_CACHE_TTL_MS};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, broadcast};

use crate::{
    catalog_tablet::{CatalogTabletReceipt, CatalogTabletService},
    consensus::{ConsensusProbeApiError, ConsensusProbeError, ConsensusProbeHandle},
    tablet_http::{deserialize_optional_u64_from_number_or_decimal, serialize_u64_as_decimal},
    tablet_materializer::{
        RegionalTabletMaterializer, TabletMaterializerError, TabletReconcileOutcome,
    },
};

pub const REGIONAL_CATALOG_PATH: &str = "/experimental/v1/regional/catalog";
pub const REGIONAL_CATALOG_RESOURCE_PATH: &str = "/experimental/v1/regional/catalog/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}";
const CATALOG_REQUEST_BODY_BYTES: usize = 64 * 1024;
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
    state_digest: String,
    resources: Vec<CatalogResourceResponse>,
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

pub fn regional_catalog_router(state: RegionalCatalogState) -> Router {
    Router::new()
        .route(REGIONAL_CATALOG_PATH, get(catalog_snapshot))
        .route(
            REGIONAL_CATALOG_RESOURCE_PATH,
            get(catalog_resource)
                .put(apply_resource)
                .delete(delete_resource),
        )
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
        state_digest: snapshot.state_digest,
        resources: snapshot.resources.iter().map(Into::into).collect(),
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

async fn apply_resource(
    State(state): State<RegionalCatalogState>,
    Path(path): Path<CatalogResourcePath>,
    request: Result<Json<ApplyResourceRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<CatalogMutationReceiptResponse>), RegionalCatalogApiError> {
    let Json(request) = request.map_err(|rejection| request_body_error(&rejection))?;
    let name = path.resource_name()?;
    let workload_profile = profile_for_kind(name.kind)?;
    let configuration =
        normalize_profile_configuration(name.kind, request.shard_count, request.configuration)?;
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
    kind: ResourceKind,
    shard_count: u32,
    configuration: Option<serde_json::Value>,
) -> Result<Option<serde_json::Value>, RegionalCatalogApiError> {
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

async fn commit_command(
    state: &RegionalCatalogState,
    command: CatalogCommand,
) -> Result<(CatalogTabletReceipt, bool), RegionalCatalogApiError> {
    state
        .catalog
        .ensure_healthy()
        .map_err(RegionalCatalogApiError::CatalogState)?;
    let request_token = match &command {
        CatalogCommand::Apply(request) => &request.request_token,
        CatalogCommand::Delete(request) => &request.request_token,
    };
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
            state
                .consensus
                .propose(proposal_id, status.term.get(), payload.clone())
                .await?;
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
                ProposalLookup::Unknown => {
                    return Err(RegionalCatalogApiError::Inconsistent(
                        "catalog proposal disappeared after submission".into(),
                    ));
                }
                ProposalLookup::Pending { .. } | ProposalLookup::Committed(_) => {}
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
                | CatalogError::IdempotencyConflict,
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

    #[test]
    fn omitted_cache_configuration_preserves_the_legacy_catalog_contract() {
        assert_eq!(
            normalize_profile_configuration(ResourceKind::Cache, 1, None).unwrap(),
            None
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
