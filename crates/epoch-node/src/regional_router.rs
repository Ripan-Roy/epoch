//! Resource-aware routing and fencing for materialized regional tablets.

use std::{convert::Infallible, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use epoch_catalog::ResourceName;
use epoch_consensus::{ConsensusError, ConsensusRole, ConsensusStatus};
use epoch_core::{ResourceKind, WorkloadProfile};
use epoch_stream::STREAM_PARTITIONER;
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

use crate::{
    consensus::{ConsensusProbeError, ConsensusProbeRole},
    tablet_http::{
        TabletReadMetadata, serialize_optional_u64_as_decimal, serialize_u64_as_decimal,
    },
    tablet_materializer::{MaterializedTabletRoute, TabletDirectory, TabletDirectoryError},
};

pub const REGIONAL_RESOURCE_ROUTE_PATH: &str = "/experimental/v1/regional/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}/shards/{shard}";
pub const REGIONAL_RESOURCE_DATA_PATH: &str = "/experimental/v1/regional/resources/{organization}/{project}/{environment}/{namespace}/{kind}/{name}/shards/{shard}/data/{*operation}";
pub const REGIONAL_STREAM_ROUTE_PATH: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}";
pub const REGIONAL_STREAM_DATA_PATH: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}/{*operation}";
pub const REGIONAL_QUEUE_ROUTE_PATH: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}";
pub const REGIONAL_QUEUE_DATA_PATH: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}/{*operation}";
pub const REGIONAL_CACHE_ROUTE_PATH: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}";
pub const REGIONAL_CACHE_DATA_PATH: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}/{*operation}";
pub const REGIONAL_BUS_ROUTE_PATH: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/buses/{name}/shards/{shard}";
pub const REGIONAL_BUS_DATA_PATH: &str = "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/buses/{name}/shards/{shard}/{*operation}";
pub const RESOURCE_GENERATION_HEADER: &str = "x-epoch-resource-generation";
pub const TABLET_EPOCH_HEADER: &str = "x-epoch-tablet-epoch";
pub const READ_CONSISTENCY_HEADER: &str = "x-epoch-read-consistency";
pub const READ_INDEX_HEADER: &str = "x-epoch-read-index";
pub const DEFAULT_REGIONAL_READ_BARRIER_TIMEOUT: Duration = Duration::from_secs(2);
pub const MAX_REGIONAL_READ_BARRIER_TIMEOUT: Duration = Duration::from_mins(1);

#[derive(Debug, Clone)]
struct RegionalRouterState {
    directory: TabletDirectory,
    read_barrier_timeout: Duration,
}

#[derive(Debug, Clone, Deserialize)]
struct RegionalResourcePath {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    kind: String,
    name: String,
    shard: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegionalDataPath {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    kind: String,
    name: String,
    shard: String,
    operation: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegionalStreamPath {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    name: String,
    shard: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegionalStreamDataPath {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    name: String,
    shard: String,
    operation: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegionalQueuePath {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    name: String,
    shard: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegionalQueueDataPath {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    name: String,
    shard: String,
    operation: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegionalCachePath {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    name: String,
    shard: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegionalCacheDataPath {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    name: String,
    shard: String,
    operation: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegionalBusPath {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    name: String,
    shard: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegionalBusDataPath {
    organization: String,
    project: String,
    environment: String,
    namespace: String,
    name: String,
    shard: String,
    operation: String,
}

impl RegionalStreamPath {
    fn regional_path(&self) -> RegionalResourcePath {
        RegionalResourcePath {
            organization: self.organization.clone(),
            project: self.project.clone(),
            environment: self.environment.clone(),
            namespace: self.namespace.clone(),
            kind: "stream".into(),
            name: self.name.clone(),
            shard: self.shard.clone(),
        }
    }
}

impl RegionalStreamDataPath {
    fn regional_path(&self) -> RegionalDataPath {
        RegionalDataPath {
            organization: self.organization.clone(),
            project: self.project.clone(),
            environment: self.environment.clone(),
            namespace: self.namespace.clone(),
            kind: "stream".into(),
            name: self.name.clone(),
            shard: self.shard.clone(),
            operation: self.operation.clone(),
        }
    }
}

impl RegionalQueuePath {
    fn regional_path(&self) -> RegionalResourcePath {
        RegionalResourcePath {
            organization: self.organization.clone(),
            project: self.project.clone(),
            environment: self.environment.clone(),
            namespace: self.namespace.clone(),
            kind: "queue".into(),
            name: self.name.clone(),
            shard: self.shard.clone(),
        }
    }
}

impl RegionalQueueDataPath {
    fn regional_path(&self) -> RegionalDataPath {
        RegionalDataPath {
            organization: self.organization.clone(),
            project: self.project.clone(),
            environment: self.environment.clone(),
            namespace: self.namespace.clone(),
            kind: "queue".into(),
            name: self.name.clone(),
            shard: self.shard.clone(),
            operation: self.operation.clone(),
        }
    }
}

impl RegionalCachePath {
    fn regional_path(&self) -> RegionalResourcePath {
        RegionalResourcePath {
            organization: self.organization.clone(),
            project: self.project.clone(),
            environment: self.environment.clone(),
            namespace: self.namespace.clone(),
            kind: "cache".into(),
            name: self.name.clone(),
            shard: self.shard.clone(),
        }
    }
}

impl RegionalCacheDataPath {
    fn regional_path(&self) -> RegionalDataPath {
        RegionalDataPath {
            organization: self.organization.clone(),
            project: self.project.clone(),
            environment: self.environment.clone(),
            namespace: self.namespace.clone(),
            kind: "cache".into(),
            name: self.name.clone(),
            shard: self.shard.clone(),
            operation: self.operation.clone(),
        }
    }
}

impl RegionalBusPath {
    fn regional_path(&self) -> RegionalResourcePath {
        RegionalResourcePath {
            organization: self.organization.clone(),
            project: self.project.clone(),
            environment: self.environment.clone(),
            namespace: self.namespace.clone(),
            kind: "event-bus".into(),
            name: self.name.clone(),
            shard: self.shard.clone(),
        }
    }
}

impl RegionalBusDataPath {
    fn regional_path(&self) -> RegionalDataPath {
        RegionalDataPath {
            organization: self.organization.clone(),
            project: self.project.clone(),
            environment: self.environment.clone(),
            namespace: self.namespace.clone(),
            kind: "event-bus".into(),
            name: self.name.clone(),
            shard: self.shard.clone(),
            operation: self.operation.clone(),
        }
    }
}

impl RegionalDataPath {
    fn resource_path(&self) -> RegionalResourcePath {
        RegionalResourcePath {
            organization: self.organization.clone(),
            project: self.project.clone(),
            environment: self.environment.clone(),
            namespace: self.namespace.clone(),
            kind: self.kind.clone(),
            name: self.name.clone(),
            shard: self.shard.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RegionalRouteResponse {
    pub resource: ResourceName,
    pub shard_index: u32,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub resource_generation: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub tablet_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub consensus_group_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub tablet_epoch: u64,
    pub workload_profile: WorkloadProfile,
    pub replica_count: u16,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub local_node_id: u64,
    pub local_role: ConsensusProbeRole,
    #[serde(serialize_with = "serialize_optional_u64_as_decimal")]
    pub leader_node_id: Option<u64>,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    pub term: u64,
    pub accepts_writes: bool,
    pub retry_hint: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_partitioning: Option<StreamPartitioningResponse>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct StreamPartitioningResponse {
    pub algorithm: &'static str,
    pub key_encoding: &'static str,
    pub missing_key_fallback: &'static str,
    pub shard_count: u32,
}

impl RegionalRouteResponse {
    fn new(route: &MaterializedTabletRoute, consensus: &ConsensusStatus) -> Self {
        let descriptor = &route.metadata().descriptor;
        Self {
            resource: route.metadata().resource.clone(),
            shard_index: descriptor.shard_index,
            resource_generation: descriptor.resource_generation,
            tablet_id: descriptor.tablet_id,
            consensus_group_id: descriptor.consensus_group_id,
            tablet_epoch: descriptor.tablet_epoch,
            workload_profile: descriptor.workload_profile,
            replica_count: descriptor.replica_count,
            local_node_id: consensus.node_id.get(),
            local_role: consensus.role.into(),
            leader_node_id: consensus.leader_id.map(epoch_consensus::NodeId::get),
            term: consensus.term.get(),
            accepts_writes: consensus.role == ConsensusRole::Leader && !consensus.fail_stopped,
            retry_hint: if consensus.role == ConsensusRole::Leader && !consensus.fail_stopped {
                "send fenced requests to this node"
            } else {
                "refresh routing and retry the current leader with the same idempotency key"
            },
            stream_partitioning: (descriptor.workload_profile == WorkloadProfile::StreamLog)
                .then_some(StreamPartitioningResponse {
                    algorithm: STREAM_PARTITIONER,
                    key_encoding: "utf8",
                    missing_key_fallback: "event_id",
                    shard_count: route.metadata().shard_count,
                }),
        }
    }
}

#[derive(Debug, Serialize)]
struct RegionalRouteErrorBody {
    code: &'static str,
    message: String,
    retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    current: Option<RegionalRouteFence>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_as_decimal"
    )]
    leader_node_id: Option<u64>,
}

#[derive(Debug, Serialize)]
struct RegionalRouteFence {
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    resource_generation: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    tablet_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    consensus_group_id: u64,
    #[serde(serialize_with = "serialize_u64_as_decimal")]
    tablet_epoch: u64,
}

#[derive(Debug)]
struct RegionalRouterError {
    status: StatusCode,
    body: RegionalRouteErrorBody,
}

impl RegionalRouterError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: RegionalRouteErrorBody {
                code: "invalid_route",
                message: message.into(),
                retryable: false,
                current: None,
                leader_node_id: None,
            },
        }
    }

    fn not_found(resource: &ResourceName, shard: u32) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            body: RegionalRouteErrorBody {
                code: "route_not_found",
                message: format!(
                    "{} shard {shard} is not materialized on this node",
                    resource.canonical_name()
                ),
                retryable: true,
                current: None,
                leader_node_id: None,
            },
        }
    }

    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: RegionalRouteErrorBody {
                code: "route_unavailable",
                message: message.into(),
                retryable: true,
                current: None,
                leader_node_id: None,
            },
        }
    }

    fn fenced(route: &MaterializedTabletRoute, message: impl Into<String>) -> Self {
        let descriptor = &route.metadata().descriptor;
        Self {
            status: StatusCode::CONFLICT,
            body: RegionalRouteErrorBody {
                code: "fenced",
                message: message.into(),
                retryable: true,
                current: Some(RegionalRouteFence {
                    resource_generation: descriptor.resource_generation,
                    tablet_id: descriptor.tablet_id,
                    consensus_group_id: descriptor.consensus_group_id,
                    tablet_epoch: descriptor.tablet_epoch,
                }),
                leader_node_id: None,
            },
        }
    }

    fn not_leader(route: &MaterializedTabletRoute, consensus: &ConsensusStatus) -> Self {
        let descriptor = &route.metadata().descriptor;
        Self {
            status: StatusCode::CONFLICT,
            body: RegionalRouteErrorBody {
                code: "not_leader",
                message: format!(
                    "local node {} is not leader for consensus group {}",
                    consensus.node_id, descriptor.consensus_group_id
                ),
                retryable: true,
                current: Some(RegionalRouteFence {
                    resource_generation: descriptor.resource_generation,
                    tablet_id: descriptor.tablet_id,
                    consensus_group_id: descriptor.consensus_group_id,
                    tablet_epoch: descriptor.tablet_epoch,
                }),
                leader_node_id: consensus.leader_id.map(epoch_consensus::NodeId::get),
            },
        }
    }

    fn read_barrier_timeout(route: &MaterializedTabletRoute, message: impl Into<String>) -> Self {
        let descriptor = &route.metadata().descriptor;
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: RegionalRouteErrorBody {
                code: "read_barrier_timeout",
                message: message.into(),
                retryable: true,
                current: Some(RegionalRouteFence {
                    resource_generation: descriptor.resource_generation,
                    tablet_id: descriptor.tablet_id,
                    consensus_group_id: descriptor.consensus_group_id,
                    tablet_epoch: descriptor.tablet_epoch,
                }),
                leader_node_id: None,
            },
        }
    }
}

impl IntoResponse for RegionalRouterError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

pub fn regional_tablet_router(directory: TabletDirectory) -> Router {
    regional_tablet_router_with_read_timeout(directory, DEFAULT_REGIONAL_READ_BARRIER_TIMEOUT)
}

pub fn regional_tablet_router_with_read_timeout(
    directory: TabletDirectory,
    read_barrier_timeout: Duration,
) -> Router {
    assert!(
        !read_barrier_timeout.is_zero()
            && read_barrier_timeout <= MAX_REGIONAL_READ_BARRIER_TIMEOUT,
        "regional read barrier timeout must be between 1 ms and 60 seconds"
    );
    Router::new()
        .route(REGIONAL_RESOURCE_ROUTE_PATH, get(resolve_route))
        .route(REGIONAL_RESOURCE_DATA_PATH, any(dispatch_data))
        .route(REGIONAL_STREAM_ROUTE_PATH, get(resolve_stream_route))
        .route(REGIONAL_STREAM_DATA_PATH, any(dispatch_stream_data))
        .route(REGIONAL_QUEUE_ROUTE_PATH, get(resolve_queue_route))
        .route(REGIONAL_QUEUE_DATA_PATH, any(dispatch_queue_data))
        .route(REGIONAL_CACHE_ROUTE_PATH, get(resolve_cache_route))
        .route(REGIONAL_CACHE_DATA_PATH, any(dispatch_cache_data))
        .route(REGIONAL_BUS_ROUTE_PATH, get(resolve_bus_route))
        .route(REGIONAL_BUS_DATA_PATH, any(dispatch_bus_data))
        .with_state(RegionalRouterState {
            directory,
            read_barrier_timeout,
        })
}

async fn resolve_route(
    State(state): State<RegionalRouterState>,
    Path(path): Path<RegionalResourcePath>,
) -> Result<Json<RegionalRouteResponse>, RegionalRouterError> {
    let (route, _) = resolve_local_route(&state.directory, &path)?;
    let consensus = route
        .consensus()
        .status()
        .await
        .map_err(|error| RegionalRouterError::unavailable(error.to_string()))?;
    Ok(Json(RegionalRouteResponse::new(&route, &consensus)))
}

async fn resolve_stream_route(
    State(state): State<RegionalRouterState>,
    Path(path): Path<RegionalStreamPath>,
) -> Result<Json<RegionalRouteResponse>, RegionalRouterError> {
    resolve_route(State(state), Path(path.regional_path())).await
}

async fn resolve_queue_route(
    State(state): State<RegionalRouterState>,
    Path(path): Path<RegionalQueuePath>,
) -> Result<Json<RegionalRouteResponse>, RegionalRouterError> {
    resolve_route(State(state), Path(path.regional_path())).await
}

async fn resolve_cache_route(
    State(state): State<RegionalRouterState>,
    Path(path): Path<RegionalCachePath>,
) -> Result<Json<RegionalRouteResponse>, RegionalRouterError> {
    resolve_route(State(state), Path(path.regional_path())).await
}

async fn resolve_bus_route(
    State(state): State<RegionalRouterState>,
    Path(path): Path<RegionalBusPath>,
) -> Result<Json<RegionalRouteResponse>, RegionalRouterError> {
    resolve_route(State(state), Path(path.regional_path())).await
}

async fn dispatch_data(
    State(state): State<RegionalRouterState>,
    Path(path): Path<RegionalDataPath>,
    request: Request<Body>,
) -> Result<Response, RegionalRouterError> {
    dispatch_data_request(&state, &path, request).await
}

async fn dispatch_stream_data(
    State(state): State<RegionalRouterState>,
    Path(path): Path<RegionalStreamDataPath>,
    request: Request<Body>,
) -> Result<Response, RegionalRouterError> {
    dispatch_data_request(&state, &path.regional_path(), request).await
}

async fn dispatch_queue_data(
    State(state): State<RegionalRouterState>,
    Path(path): Path<RegionalQueueDataPath>,
    request: Request<Body>,
) -> Result<Response, RegionalRouterError> {
    dispatch_data_request(&state, &path.regional_path(), request).await
}

async fn dispatch_cache_data(
    State(state): State<RegionalRouterState>,
    Path(path): Path<RegionalCacheDataPath>,
    request: Request<Body>,
) -> Result<Response, RegionalRouterError> {
    dispatch_data_request(&state, &path.regional_path(), request).await
}

async fn dispatch_bus_data(
    State(state): State<RegionalRouterState>,
    Path(path): Path<RegionalBusDataPath>,
    request: Request<Body>,
) -> Result<Response, RegionalRouterError> {
    dispatch_data_request(&state, &path.regional_path(), request).await
}

async fn dispatch_data_request(
    state: &RegionalRouterState,
    path: &RegionalDataPath,
    mut request: Request<Body>,
) -> Result<Response, RegionalRouterError> {
    let (route, _) = resolve_local_route(&state.directory, &path.resource_path())?;
    validate_fences(&route, request.headers())?;
    if path.operation.trim_matches('/').is_empty() {
        return Err(RegionalRouterError::invalid(
            "a profile operation path is required",
        ));
    }

    let is_read = is_read_operation(
        request.method(),
        route.metadata().descriptor.workload_profile,
        &path.operation,
    );
    let requested_consistency = requested_read_consistency(request.headers(), is_read)?;
    let read_metadata = if requested_consistency == Some(RequestedReadConsistency::Linearizable) {
        let consensus = route
            .consensus()
            .status()
            .await
            .map_err(|error| RegionalRouterError::unavailable(error.to_string()))?;
        if consensus.fail_stopped {
            return Err(RegionalRouterError::unavailable(format!(
                "consensus group {} is fail-stopped",
                route.metadata().descriptor.consensus_group_id
            )));
        }
        if consensus.role != ConsensusRole::Leader {
            return Err(RegionalRouterError::not_leader(&route, &consensus));
        }
        let completed = route
            .consensus()
            .read_barrier(consensus.term.get(), state.read_barrier_timeout)
            .await
            .map_err(|error| read_barrier_error(&route, &consensus, &error))?;
        let metadata = TabletReadMetadata::linearizable(completed);
        request.extensions_mut().insert(metadata);
        Some(metadata)
    } else {
        if !is_read {
            let consensus = route
                .consensus()
                .status()
                .await
                .map_err(|error| RegionalRouterError::unavailable(error.to_string()))?;
            if consensus.fail_stopped {
                return Err(RegionalRouterError::unavailable(format!(
                    "consensus group {} is fail-stopped",
                    route.metadata().descriptor.consensus_group_id
                )));
            }
            if consensus.role != ConsensusRole::Leader {
                return Err(RegionalRouterError::not_leader(&route, &consensus));
            }
        }
        None
    };

    let inner_uri = profile_uri(
        route.metadata().descriptor.workload_profile,
        &path.operation,
        request.uri().query(),
    )?;
    *request.uri_mut() = inner_uri;

    // The outer router caches its decoded path parameters in request extensions.
    // Forwarding those parameters into the profile router makes an inner extractor
    // such as `Path<String>` observe both the regional and profile parameters. Axum
    // rejects that incompatible shape with a 500 before the profile handler runs.
    // Treat the profile dispatch as a request boundary and carry across only the
    // extension that is part of the tablet API contract.
    request.extensions_mut().clear();
    if let Some(metadata) = read_metadata {
        request.extensions_mut().insert(metadata);
    }
    let result: Result<Response, Infallible> = route.router().oneshot(request).await;
    match result {
        Ok(mut response) => {
            if let Some(metadata) = read_metadata {
                response.headers_mut().insert(
                    READ_CONSISTENCY_HEADER,
                    HeaderValue::from_static("linearizable"),
                );
                if let Some(read_index) = metadata.barrier_index() {
                    response.headers_mut().insert(
                        READ_INDEX_HEADER,
                        HeaderValue::from_str(&read_index.to_string()).map_err(|error| {
                            RegionalRouterError::unavailable(format!(
                                "read index response header could not be encoded: {error}"
                            ))
                        })?,
                    );
                }
            }
            Ok(response)
        }
        Err(never) => match never {},
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestedReadConsistency {
    Linearizable,
    LocalStale,
}

fn requested_read_consistency(
    headers: &HeaderMap,
    is_read: bool,
) -> Result<Option<RequestedReadConsistency>, RegionalRouterError> {
    let Some(raw) = headers.get(READ_CONSISTENCY_HEADER) else {
        return Ok(is_read.then_some(RequestedReadConsistency::Linearizable));
    };
    if !is_read {
        return Err(RegionalRouterError::invalid(format!(
            "header {READ_CONSISTENCY_HEADER} is valid only for read operations"
        )));
    }
    match raw.to_str().map_err(|_| {
        RegionalRouterError::invalid(format!(
            "header {READ_CONSISTENCY_HEADER} is not valid ASCII"
        ))
    })? {
        "linearizable" => Ok(Some(RequestedReadConsistency::Linearizable)),
        "local_stale" => Ok(Some(RequestedReadConsistency::LocalStale)),
        _ => Err(RegionalRouterError::invalid(format!(
            "header {READ_CONSISTENCY_HEADER} must be linearizable or local_stale"
        ))),
    }
}

fn is_read_operation(method: &Method, profile: WorkloadProfile, operation: &str) -> bool {
    if method == Method::GET {
        return true;
    }
    method == Method::POST
        && profile == WorkloadProfile::EventBus
        && matches!(
            operation.trim_matches('/'),
            "archive/replay" | "deliveries/query"
        )
}

fn read_barrier_error(
    route: &MaterializedTabletRoute,
    consensus: &ConsensusStatus,
    error: &ConsensusProbeError,
) -> RegionalRouterError {
    match error {
        ConsensusProbeError::ReadBarrierTimeout { .. }
        | ConsensusProbeError::Consensus(ConsensusError::TooManyReadBarriers) => {
            RegionalRouterError::read_barrier_timeout(route, error.to_string())
        }
        ConsensusProbeError::Consensus(
            ConsensusError::NotLeader { .. } | ConsensusError::StaleTerm { .. },
        ) => RegionalRouterError::not_leader(route, consensus),
        _ => RegionalRouterError::unavailable(error.to_string()),
    }
}

fn resolve_local_route(
    directory: &TabletDirectory,
    path: &RegionalResourcePath,
) -> Result<(MaterializedTabletRoute, u32), RegionalRouterError> {
    let kind = parse_resource_kind(&path.kind)?;
    let shard = path
        .shard
        .parse::<u32>()
        .map_err(|_| RegionalRouterError::invalid("shard must be an unsigned 32-bit integer"))?;
    let resource = ResourceName::new(
        &path.organization,
        &path.project,
        &path.environment,
        &path.namespace,
        kind,
        &path.name,
    )
    .map_err(|error| RegionalRouterError::invalid(error.to_string()))?;
    let route = directory
        .resource_route(&resource, shard)
        .map_err(|error| directory_error(&error))?
        .ok_or_else(|| RegionalRouterError::not_found(&resource, shard))?;
    Ok((route, shard))
}

fn validate_fences(
    route: &MaterializedTabletRoute,
    headers: &HeaderMap,
) -> Result<(), RegionalRouterError> {
    let generation = required_fence(headers, RESOURCE_GENERATION_HEADER)?;
    let tablet_epoch = required_fence(headers, TABLET_EPOCH_HEADER)?;
    let descriptor = &route.metadata().descriptor;
    if generation != descriptor.resource_generation {
        return Err(RegionalRouterError::fenced(
            route,
            format!(
                "resource generation {generation} is fenced by generation {}",
                descriptor.resource_generation
            ),
        ));
    }
    if tablet_epoch != descriptor.tablet_epoch {
        return Err(RegionalRouterError::fenced(
            route,
            format!(
                "tablet epoch {tablet_epoch} is fenced by epoch {}",
                descriptor.tablet_epoch
            ),
        ));
    }
    Ok(())
}

fn required_fence(headers: &HeaderMap, name: &'static str) -> Result<u64, RegionalRouterError> {
    let raw = headers
        .get(name)
        .ok_or_else(|| RegionalRouterError::invalid(format!("required header {name} is missing")))?
        .to_str()
        .map_err(|_| RegionalRouterError::invalid(format!("header {name} is not valid ASCII")))?;
    let value = raw.parse::<u64>().map_err(|_| {
        RegionalRouterError::invalid(format!("header {name} must be a decimal u64"))
    })?;
    if value == 0 {
        return Err(RegionalRouterError::invalid(format!(
            "header {name} must be non-zero"
        )));
    }
    Ok(value)
}

fn profile_uri(
    profile: WorkloadProfile,
    operation: &str,
    query: Option<&str>,
) -> Result<Uri, RegionalRouterError> {
    let profile = match profile {
        WorkloadProfile::CacheAndState => "cache",
        WorkloadProfile::StreamLog => "stream",
        WorkloadProfile::WorkQueue => "queue",
        WorkloadProfile::EventBus => "bus",
    };
    let operation = operation.trim_start_matches('/');
    let path = if let Some(query) = query {
        format!("/experimental/v1/tablets/{profile}/{operation}?{query}")
    } else {
        format!("/experimental/v1/tablets/{profile}/{operation}")
    };
    path.parse()
        .map_err(|error| RegionalRouterError::invalid(format!("invalid operation URI: {error}")))
}

fn parse_resource_kind(value: &str) -> Result<ResourceKind, RegionalRouterError> {
    match value {
        "cache" => Ok(ResourceKind::Cache),
        "table" => Ok(ResourceKind::Table),
        "stream" => Ok(ResourceKind::Stream),
        "queue" => Ok(ResourceKind::Queue),
        "event-bus" => Ok(ResourceKind::EventBus),
        _ => Err(RegionalRouterError::invalid(
            "kind must be cache, table, stream, queue, or event-bus",
        )),
    }
}

fn directory_error(error: &TabletDirectoryError) -> RegionalRouterError {
    RegionalRouterError::unavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use epoch_catalog::{ResourceRecord, ResourceSpec, TabletDescriptor};
    use epoch_core::ManualClock;
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tempfile::TempDir;
    use url::Url;

    use super::*;
    use crate::{
        consensus::ConsensusProbeConfig, consensus_groups::ConsensusGroupSupervisor,
        tablet_materializer::RegionalTabletMaterializer,
    };

    fn peer_url(port: u16) -> Url {
        Url::parse(&format!("http://127.0.0.1:{port}/")).expect("test peer URL should parse")
    }

    async fn routed_resource(
        directory: &TempDir,
        kind: ResourceKind,
        name: &str,
        workload_profile: WorkloadProfile,
    ) -> (RegionalTabletMaterializer, Router, ResourceRecord) {
        routed_resource_with_shards(directory, kind, name, workload_profile, 1).await
    }

    async fn routed_resource_with_shards(
        directory: &TempDir,
        kind: ResourceKind,
        name: &str,
        workload_profile: WorkloadProfile,
        shard_count: u32,
    ) -> (RegionalTabletMaterializer, Router, ResourceRecord) {
        let resource = ResourceRecord {
            name: ResourceName::new("acme", "shop", "dev", "core", kind, name).unwrap(),
            generation: 5,
            spec: ResourceSpec {
                workload_profile,
                shard_count,
                replica_count: 3,
            },
            tablets: (0..shard_count)
                .map(|shard_index| TabletDescriptor {
                    tablet_id: 7 + u64::from(shard_index),
                    consensus_group_id: 17 + u64::from(shard_index),
                    shard_index,
                    tablet_epoch: 3,
                    resource_generation: 5,
                    workload_profile,
                    replica_count: 3,
                })
                .collect(),
        };
        let template = ConsensusProbeConfig::new(
            2,
            900,
            1,
            [
                (1, peer_url(41_001)),
                (2, peer_url(41_002)),
                (3, peer_url(41_003)),
            ],
            Duration::from_mins(1),
        )
        .unwrap();
        let mut materializer = RegionalTabletMaterializer::new(
            ConsensusGroupSupervisor::new(2, 8).unwrap(),
            template,
            directory.path(),
            Arc::new(ManualClock::new(1_000)),
            Duration::from_secs(1),
        )
        .unwrap();
        materializer
            .reconcile(std::slice::from_ref(&resource))
            .await
            .unwrap();
        let router = regional_tablet_router(materializer.directory());
        (materializer, router, resource)
    }

    async fn routed_stream(
        directory: &TempDir,
    ) -> (RegionalTabletMaterializer, Router, ResourceRecord) {
        routed_resource(
            directory,
            ResourceKind::Stream,
            "orders",
            WorkloadProfile::StreamLog,
        )
        .await
    }

    async fn routed_stream_with_shards(
        directory: &TempDir,
        shard_count: u32,
    ) -> (RegionalTabletMaterializer, Router, ResourceRecord) {
        routed_resource_with_shards(
            directory,
            ResourceKind::Stream,
            "orders",
            WorkloadProfile::StreamLog,
            shard_count,
        )
        .await
    }

    async fn routed_queue(
        directory: &TempDir,
    ) -> (RegionalTabletMaterializer, Router, ResourceRecord) {
        routed_resource(
            directory,
            ResourceKind::Queue,
            "jobs",
            WorkloadProfile::WorkQueue,
        )
        .await
    }

    async fn routed_cache(
        directory: &TempDir,
    ) -> (RegionalTabletMaterializer, Router, ResourceRecord) {
        routed_resource(
            directory,
            ResourceKind::Cache,
            "sessions",
            WorkloadProfile::CacheAndState,
        )
        .await
    }

    async fn routed_bus(
        directory: &TempDir,
    ) -> (RegionalTabletMaterializer, Router, ResourceRecord) {
        routed_resource(
            directory,
            ResourceKind::EventBus,
            "events",
            WorkloadProfile::EventBus,
        )
        .await
    }

    fn route_path() -> &'static str {
        "/experimental/v1/regional/resources/acme/shop/dev/core/stream/orders/shards/0"
    }

    fn data_path(operation: &str) -> String {
        format!("{}/data/{operation}", route_path())
    }

    fn native_stream_route_path() -> &'static str {
        "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/streams/orders/shards/0"
    }

    fn native_stream_data_path(operation: &str) -> String {
        format!("{}/{operation}", native_stream_route_path())
    }

    fn native_queue_route_path() -> &'static str {
        "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/queues/jobs/shards/0"
    }

    fn native_queue_data_path(operation: &str) -> String {
        format!("{}/{operation}", native_queue_route_path())
    }

    fn native_cache_route_path() -> &'static str {
        "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/caches/sessions/shards/0"
    }

    fn native_cache_data_path(operation: &str) -> String {
        format!("{}/{operation}", native_cache_route_path())
    }

    fn native_bus_route_path() -> &'static str {
        "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/buses/events/shards/0"
    }

    fn native_bus_data_path(operation: &str) -> String {
        format!("{}/{operation}", native_bus_route_path())
    }

    async fn json(response: Response) -> Value {
        serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("response body should collect")
                .to_bytes(),
        )
        .expect("response should be JSON")
    }

    #[test]
    fn read_consistency_is_semantic_and_never_downgrades_implicitly() {
        assert!(is_read_operation(
            &Method::GET,
            WorkloadProfile::StreamLog,
            "records"
        ));
        assert!(is_read_operation(
            &Method::GET,
            WorkloadProfile::WorkQueue,
            "consumers/worker-a/flow"
        ));
        assert_eq!(
            profile_uri(WorkloadProfile::WorkQueue, "consumers/worker-a/flow", None)
                .unwrap()
                .path(),
            "/experimental/v1/tablets/queue/consumers/worker-a/flow"
        );
        assert_eq!(
            profile_uri(WorkloadProfile::StreamLog, "records/batches", None)
                .unwrap()
                .path(),
            "/experimental/v1/tablets/stream/records/batches"
        );
        assert_eq!(
            profile_uri(WorkloadProfile::StreamLog, "groups/billing/lag", None)
                .unwrap()
                .path(),
            "/experimental/v1/tablets/stream/groups/billing/lag"
        );
        assert!(is_read_operation(
            &Method::GET,
            WorkloadProfile::StreamLog,
            "groups/billing/records"
        ));
        assert!(!is_read_operation(
            &Method::PUT,
            WorkloadProfile::StreamLog,
            "groups/billing/offsets"
        ));
        assert!(!is_read_operation(
            &Method::POST,
            WorkloadProfile::StreamLog,
            "records/batches"
        ));
        assert!(is_read_operation(
            &Method::POST,
            WorkloadProfile::EventBus,
            "archive/replay"
        ));
        assert!(is_read_operation(
            &Method::POST,
            WorkloadProfile::EventBus,
            "deliveries/query"
        ));
        assert!(!is_read_operation(
            &Method::POST,
            WorkloadProfile::EventBus,
            "mutations"
        ));
        assert_eq!(
            requested_read_consistency(&HeaderMap::new(), true).unwrap(),
            Some(RequestedReadConsistency::Linearizable)
        );

        let mut stale_headers = HeaderMap::new();
        stale_headers.insert(
            READ_CONSISTENCY_HEADER,
            HeaderValue::from_static("local_stale"),
        );
        assert_eq!(
            requested_read_consistency(&stale_headers, true).unwrap(),
            Some(RequestedReadConsistency::LocalStale)
        );
        assert!(requested_read_consistency(&stale_headers, false).is_err());

        let mut invalid_headers = HeaderMap::new();
        invalid_headers.insert(
            READ_CONSISTENCY_HEADER,
            HeaderValue::from_static("eventual"),
        );
        assert!(requested_read_consistency(&invalid_headers, true).is_err());
    }

    #[tokio::test]
    async fn discovery_and_data_routing_are_generation_epoch_and_leader_fenced() {
        let data_directory = TempDir::new().expect("temp directory should be created");
        let (mut materializer, router, _resource) = routed_stream(&data_directory).await;

        let discovery = router
            .clone()
            .oneshot(Request::get(route_path()).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(discovery.status(), StatusCode::OK);
        let discovery = json(discovery).await;
        assert_eq!(discovery["resource_generation"], "5");
        assert_eq!(discovery["tablet_id"], "7");
        assert_eq!(discovery["consensus_group_id"], "17");
        assert_eq!(discovery["tablet_epoch"], "3");
        assert_eq!(discovery["accepts_writes"], false);

        let missing_fence = router
            .clone()
            .oneshot(
                Request::get(data_path("status"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_fence.status(), StatusCode::BAD_REQUEST);

        let stale = router
            .clone()
            .oneshot(
                Request::get(data_path("status"))
                    .header(RESOURCE_GENERATION_HEADER, "4")
                    .header(TABLET_EPOCH_HEADER, "3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        let stale = json(stale).await;
        assert_eq!(stale["code"], "fenced");
        assert_eq!(stale["current"]["resource_generation"], "5");

        let quorum_read_without_leader = router
            .clone()
            .oneshot(
                Request::get(data_path("status"))
                    .header(RESOURCE_GENERATION_HEADER, "5")
                    .header(TABLET_EPOCH_HEADER, "3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(quorum_read_without_leader.status(), StatusCode::CONFLICT);
        assert_eq!(json(quorum_read_without_leader).await["code"], "not_leader");

        let local_read = router
            .clone()
            .oneshot(
                Request::get(data_path("status"))
                    .header(RESOURCE_GENERATION_HEADER, "5")
                    .header(TABLET_EPOCH_HEADER, "3")
                    .header(READ_CONSISTENCY_HEADER, "local_stale")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(local_read.status(), StatusCode::OK);
        let local_read = json(local_read).await;
        assert_eq!(local_read["tablet_id"], "7");
        assert_eq!(local_read["tablet_epoch"], "3");
        assert_eq!(
            local_read["read_consistency"],
            "local_profile_applied_stale_capable"
        );
        assert_eq!(local_read["linearizable_read_barrier"], false);
        let follower_write = router
            .clone()
            .oneshot(
                Request::post(data_path("records"))
                    .header(RESOURCE_GENERATION_HEADER, "5")
                    .header(TABLET_EPOCH_HEADER, "3")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(follower_write.status(), StatusCode::CONFLICT);
        assert_eq!(json(follower_write).await["code"], "not_leader");

        let unknown = router
            .oneshot(
                Request::get(
                    "/experimental/v1/regional/resources/acme/shop/dev/core/stream/missing/shards/0",
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
        materializer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn native_stream_v1_is_an_exact_fenced_adapter_over_the_regional_tablet() {
        assert_eq!(
            REGIONAL_STREAM_ROUTE_PATH,
            "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}"
        );
        assert_eq!(
            REGIONAL_STREAM_DATA_PATH,
            "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/streams/{name}/shards/{shard}/{*operation}"
        );

        let data_directory = TempDir::new().expect("temp directory should be created");
        let (mut materializer, router, _resource) = routed_stream(&data_directory).await;

        let discovery = router
            .clone()
            .oneshot(
                Request::get(native_stream_route_path())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(discovery.status(), StatusCode::OK);
        let discovery = json(discovery).await;
        assert_eq!(discovery["resource_generation"], "5");
        assert_eq!(discovery["tablet_epoch"], "3");
        assert_eq!(discovery["workload_profile"], "stream_log");
        assert_eq!(discovery["stream_partitioning"]["shard_count"], 1);
        assert_eq!(
            discovery["stream_partitioning"]["algorithm"],
            "fnv1a64_utf8_mod_n_v1"
        );
        assert_eq!(discovery["stream_partitioning"]["key_encoding"], "utf8");
        assert_eq!(
            discovery["stream_partitioning"]["missing_key_fallback"],
            "event_id"
        );

        let missing_fence = router
            .clone()
            .oneshot(
                Request::get(native_stream_data_path("records"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_fence.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json(missing_fence).await["code"], "invalid_route");

        let local_read = router
            .clone()
            .oneshot(
                Request::get(native_stream_data_path("records?offset=0&limit=1"))
                    .header(RESOURCE_GENERATION_HEADER, "5")
                    .header(TABLET_EPOCH_HEADER, "3")
                    .header(READ_CONSISTENCY_HEADER, "local_stale")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(local_read.status(), StatusCode::OK);
        let local_read = json(local_read).await;
        assert_eq!(
            local_read["read_consistency"],
            "local_profile_applied_stale_capable"
        );
        assert_eq!(local_read["records"], serde_json::json!([]));

        let group_read = router
            .clone()
            .oneshot(
                Request::get(native_stream_data_path("groups/billing/lag?partition=0"))
                    .header(RESOURCE_GENERATION_HEADER, "5")
                    .header(TABLET_EPOCH_HEADER, "3")
                    .header(READ_CONSISTENCY_HEADER, "local_stale")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(group_read.status(), StatusCode::OK);
        let group_read = json(group_read).await;
        assert_eq!(group_read["checkpoint"]["group"], "billing");
        assert_eq!(group_read["checkpoint"]["exists"], false);

        let wrong_profile = router
            .oneshot(
                Request::get(
                    "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/streams/missing/shards/0",
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_profile.status(), StatusCode::NOT_FOUND);

        materializer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn native_stream_routes_every_materialized_shard_with_logical_partition_metadata() {
        let data_directory = TempDir::new().expect("temp directory should be created");
        let (mut materializer, router, _resource) =
            routed_stream_with_shards(&data_directory, 3).await;

        for shard in 0..3 {
            let base = format!(
                "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/streams/orders/shards/{shard}"
            );
            let discovery = router
                .clone()
                .oneshot(Request::get(&base).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(discovery.status(), StatusCode::OK);
            let discovery = json(discovery).await;
            assert_eq!(discovery["shard_index"], shard);
            assert_eq!(discovery["stream_partitioning"]["shard_count"], 3);

            let fetch = router
                .clone()
                .oneshot(
                    Request::get(format!("{base}/records?offset=0&limit=1"))
                        .header(RESOURCE_GENERATION_HEADER, "5")
                        .header(TABLET_EPOCH_HEADER, "3")
                        .header(READ_CONSISTENCY_HEADER, "local_stale")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(fetch.status(), StatusCode::OK);
            let fetch = json(fetch).await;
            assert_eq!(fetch["shard_index"], shard);
            assert_eq!(fetch["records"], serde_json::json!([]));
        }

        materializer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn native_queue_v1_routes_to_the_existing_queue_tablet() {
        assert_eq!(
            REGIONAL_QUEUE_ROUTE_PATH,
            "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}"
        );
        assert_eq!(
            REGIONAL_QUEUE_DATA_PATH,
            "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/queues/{name}/shards/{shard}/{*operation}"
        );

        let data_directory = TempDir::new().expect("temp directory should be created");
        let (mut materializer, router, _resource) = routed_queue(&data_directory).await;

        let discovery = router
            .clone()
            .oneshot(
                Request::get(native_queue_route_path())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(discovery.status(), StatusCode::OK);
        let discovery = json(discovery).await;
        assert_eq!(discovery["workload_profile"], "work_queue");

        let counts = router
            .oneshot(
                Request::get(native_queue_data_path("counts"))
                    .header(RESOURCE_GENERATION_HEADER, "5")
                    .header(TABLET_EPOCH_HEADER, "3")
                    .header(READ_CONSISTENCY_HEADER, "local_stale")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(counts.status(), StatusCode::OK);
        assert_eq!(json(counts).await["counts"]["ready"], "0");

        materializer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn native_cache_v1_routes_to_the_existing_cache_tablet() {
        assert_eq!(
            REGIONAL_CACHE_ROUTE_PATH,
            "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}"
        );
        assert_eq!(
            REGIONAL_CACHE_DATA_PATH,
            "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/caches/{name}/shards/{shard}/{*operation}"
        );

        let data_directory = TempDir::new().expect("temp directory should be created");
        let (mut materializer, router, _resource) = routed_cache(&data_directory).await;

        let discovery = router
            .clone()
            .oneshot(
                Request::get(native_cache_route_path())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(discovery.status(), StatusCode::OK);
        assert_eq!(json(discovery).await["workload_profile"], "cache_and_state");

        let observation = router
            .oneshot(
                Request::get(native_cache_data_path("observations?key=missing"))
                    .header(RESOURCE_GENERATION_HEADER, "5")
                    .header(TABLET_EPOCH_HEADER, "3")
                    .header(READ_CONSISTENCY_HEADER, "local_stale")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(observation.status(), StatusCode::OK);
        assert!(json(observation).await["observation"]["item"].is_null());

        materializer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn native_bus_v1_routes_to_the_existing_event_bus_tablet() {
        assert_eq!(
            REGIONAL_BUS_ROUTE_PATH,
            "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/buses/{name}/shards/{shard}"
        );
        assert_eq!(
            REGIONAL_BUS_DATA_PATH,
            "/v1/organizations/{organization}/projects/{project}/environments/{environment}/namespaces/{namespace}/buses/{name}/shards/{shard}/{*operation}"
        );

        let data_directory = TempDir::new().expect("temp directory should be created");
        let (mut materializer, router, _resource) = routed_bus(&data_directory).await;

        let discovery = router
            .clone()
            .oneshot(
                Request::get(native_bus_route_path())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(discovery.status(), StatusCode::OK);
        assert_eq!(json(discovery).await["workload_profile"], "event_bus");

        let status = router
            .oneshot(
                Request::get(native_bus_data_path("status"))
                    .header(RESOURCE_GENERATION_HEADER, "5")
                    .header(TABLET_EPOCH_HEADER, "3")
                    .header(READ_CONSISTENCY_HEADER, "local_stale")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        assert_eq!(
            json(status).await["capability"],
            "single_partition_event_bus_ingress_outbox_tablet"
        );

        materializer.shutdown().await.unwrap();
    }
}
