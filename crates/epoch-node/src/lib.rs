//! Native HTTP surface for the standalone Epoch node.

use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use epoch_bus::{ArchivedEvent, BusConfig, EventFilter, Subscription};
use epoch_cache::{CacheConfig, CacheItem, CacheValue, SetOptions};
use epoch_core::{EpochError, EventEnvelope};
use epoch_engine::{BusPublishOutcome, EngineHealth, EpochEngine, ResourceSummary};
use epoch_queue::{Delivery, EnqueueReceipt, QueueConfig, QueueCounts};
use epoch_stream::{AppendReceipt, ConsumerLag, StreamConfig, StreamRecord};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tower_http::{cors::CorsLayer, trace::TraceLayer};

#[derive(Debug, Clone)]
pub struct AppState {
    pub engine: Arc<EpochEngine>,
}

pub fn router(engine: Arc<EpochEngine>) -> Router {
    let state = AppState { engine };
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .route("/v1/resources", get(list_resources))
        .route("/v1/caches/{name}", post(create_cache))
        .route(
            "/v1/caches/{name}/keys/{key}",
            get(cache_get).put(cache_put).delete(cache_delete),
        )
        .route(
            "/v1/caches/{name}/keys/{key}/increment",
            post(cache_increment),
        )
        .route("/v1/streams/{name}", post(create_stream))
        .route(
            "/v1/streams/{name}/records",
            post(stream_append).get(stream_fetch),
        )
        .route(
            "/v1/streams/{name}/groups/{group}/offsets",
            put(stream_commit),
        )
        .route("/v1/streams/{name}/groups/{group}/lag", get(stream_lag))
        .route("/v1/queues/{name}", post(create_queue))
        .route("/v1/queues/{name}/messages", post(queue_enqueue))
        .route("/v1/queues/{name}/acquire", post(queue_acquire))
        .route("/v1/queues/{name}/settle", post(queue_settle))
        .route("/v1/queues/{name}/counts", get(queue_counts))
        .route(
            "/v1/queues/{name}/dead-letters/{message_id}/redrive",
            post(queue_redrive),
        )
        .route("/v1/buses/{name}", post(create_bus))
        .route("/v1/buses/{name}/events", post(bus_publish))
        .route("/v1/buses/{name}/replay", get(bus_replay))
        .route(
            "/v1/buses/{name}/subscriptions/{subscription}",
            put(bus_upsert_subscription).delete(bus_remove_subscription),
        )
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub fn spawn_maintenance(engine: Arc<EpochEngine>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            if let Err(error) = engine.maintain(1_000) {
                tracing::error!(%error, "background maintenance could not be persisted");
            }
        }
    })
}

async fn health(State(state): State<AppState>) -> Json<EngineHealth> {
    Json(state.engine.health())
}

async fn list_resources(State(state): State<AppState>) -> Json<Vec<ResourceSummary>> {
    Json(state.engine.resources())
}

async fn create_cache(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(config): Json<CacheConfig>,
) -> ApiResult<(StatusCode, Json<ResourceCreated>)> {
    state.engine.create_cache(&name, config)?;
    Ok((StatusCode::CREATED, Json(ResourceCreated::new(name))))
}

async fn create_stream(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(config): Json<StreamConfig>,
) -> ApiResult<(StatusCode, Json<ResourceCreated>)> {
    state.engine.create_stream(&name, config)?;
    Ok((StatusCode::CREATED, Json(ResourceCreated::new(name))))
}

async fn create_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(config): Json<QueueConfig>,
) -> ApiResult<(StatusCode, Json<ResourceCreated>)> {
    state.engine.create_queue(&name, config)?;
    Ok((StatusCode::CREATED, Json(ResourceCreated::new(name))))
}

async fn create_bus(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(config): Json<BusConfig>,
) -> ApiResult<(StatusCode, Json<ResourceCreated>)> {
    state.engine.create_bus(&name, config)?;
    Ok((StatusCode::CREATED, Json(ResourceCreated::new(name))))
}

#[derive(Debug, Serialize)]
struct ResourceCreated {
    name: String,
    resource_epoch: u64,
}

impl ResourceCreated {
    fn new(name: String) -> Self {
        Self {
            name,
            resource_epoch: 1,
        }
    }
}

#[derive(Debug, Deserialize)]
struct CachePutRequest {
    value: CacheValue,
    #[serde(default)]
    ttl_ms: Option<u64>,
    #[serde(default)]
    expected_version: Option<u64>,
    #[serde(default)]
    only_if_absent: bool,
    #[serde(default)]
    only_if_present: bool,
}

async fn cache_put(
    State(state): State<AppState>,
    Path((name, key)): Path<(String, String)>,
    Json(request): Json<CachePutRequest>,
) -> ApiResult<Json<CacheItem>> {
    let now = state.engine.now_ms();
    let cache = state.engine.cache(&name)?;
    let item = cache.lock().set(
        key,
        request.value,
        SetOptions {
            ttl_ms: request.ttl_ms,
            expected_version: request.expected_version,
            only_if_absent: request.only_if_absent,
            only_if_present: request.only_if_present,
        },
        now,
    )?;
    Ok(Json(item))
}

async fn cache_get(
    State(state): State<AppState>,
    Path((name, key)): Path<(String, String)>,
) -> ApiResult<Json<CacheItem>> {
    let now = state.engine.now_ms();
    state
        .engine
        .cache(&name)?
        .lock()
        .get(&key, now)
        .map(Json)
        .ok_or_else(|| EpochError::NotFound(format!("cache key:{key}")).into())
}

async fn cache_delete(
    State(state): State<AppState>,
    Path((name, key)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let now = state.engine.now_ms();
    let deleted = state.engine.cache(&name)?.lock().delete(&key, now);
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(EpochError::NotFound(format!("cache key:{key}")).into())
    }
}

#[derive(Debug, Deserialize)]
struct IncrementRequest {
    delta: i64,
}

async fn cache_increment(
    State(state): State<AppState>,
    Path((name, key)): Path<(String, String)>,
    Json(request): Json<IncrementRequest>,
) -> ApiResult<Json<Value>> {
    let now = state.engine.now_ms();
    let value = state
        .engine
        .cache(&name)?
        .lock()
        .increment(&key, request.delta, now)?;
    Ok(Json(json!({"value": value})))
}

#[derive(Debug, Deserialize)]
struct StreamAppendRequest {
    envelope: EventEnvelope,
    #[serde(default)]
    partition: Option<u32>,
}

async fn stream_append(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<StreamAppendRequest>,
) -> ApiResult<(StatusCode, Json<AppendReceipt>)> {
    let receipt = state
        .engine
        .append_stream(&name, request.envelope, request.partition)?;
    Ok((StatusCode::CREATED, Json(receipt)))
}

#[derive(Debug, Deserialize)]
struct FetchQuery {
    #[serde(default)]
    partition: u32,
    #[serde(default)]
    offset: u64,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    100
}

async fn stream_fetch(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<FetchQuery>,
) -> ApiResult<Json<Vec<StreamRecord>>> {
    let records = state.engine.stream(&name)?.lock().fetch(
        query.partition,
        query.offset,
        query.limit.min(10_000),
    )?;
    Ok(Json(records))
}

#[derive(Debug, Deserialize)]
struct OffsetRequest {
    partition: u32,
    next_offset: u64,
    #[serde(default)]
    reset: bool,
}

async fn stream_commit(
    State(state): State<AppState>,
    Path((name, group)): Path<(String, String)>,
    Json(request): Json<OffsetRequest>,
) -> ApiResult<StatusCode> {
    if request.reset {
        state
            .engine
            .reset_stream_offset(&name, &group, request.partition, request.next_offset)?;
    } else {
        state
            .engine
            .commit_stream_offset(&name, &group, request.partition, request.next_offset)?;
    }
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct PartitionQuery {
    #[serde(default)]
    partition: u32,
}

async fn stream_lag(
    State(state): State<AppState>,
    Path((name, group)): Path<(String, String)>,
    Query(query): Query<PartitionQuery>,
) -> ApiResult<Json<ConsumerLag>> {
    let lag = state
        .engine
        .stream(&name)?
        .lock()
        .lag(&group, query.partition)?;
    Ok(Json(lag))
}

async fn queue_enqueue(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(envelope): Json<EventEnvelope>,
) -> ApiResult<(StatusCode, Json<EnqueueReceipt>)> {
    let receipt = state.engine.enqueue(&name, envelope)?;
    Ok((StatusCode::CREATED, Json(receipt)))
}

#[derive(Debug, Deserialize)]
struct AcquireRequest {
    consumer: String,
    #[serde(default = "default_one")]
    max_messages: usize,
    #[serde(default)]
    visibility_timeout_ms: Option<u64>,
}

fn default_one() -> usize {
    1
}

async fn queue_acquire(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<AcquireRequest>,
) -> ApiResult<Json<Vec<Delivery>>> {
    let messages = state.engine.acquire_queue(
        &name,
        &request.consumer,
        request.max_messages.min(1_000),
        request.visibility_timeout_ms,
    )?;
    Ok(Json(messages))
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum SettleRequest {
    Ack {
        token: String,
    },
    Release {
        token: String,
        #[serde(default)]
        delay_ms: u64,
        #[serde(default)]
        reason: Option<String>,
    },
    Reject {
        token: String,
        reason: String,
    },
    Extend {
        token: String,
        extension_ms: u64,
    },
}

async fn queue_settle(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<SettleRequest>,
) -> ApiResult<Json<Value>> {
    match request {
        SettleRequest::Ack { token } => {
            let acknowledgement = state.engine.acknowledge_queue(&name, &token)?;
            Ok(Json(serde_json::to_value(acknowledgement).map_err(
                |error| EpochError::Internal(error.to_string()),
            )?))
        }
        SettleRequest::Release {
            token,
            delay_ms,
            reason,
        } => {
            state
                .engine
                .release_queue(&name, &token, delay_ms, reason)?;
            Ok(Json(json!({"released": true})))
        }
        SettleRequest::Reject { token, reason } => {
            state.engine.reject_queue(&name, &token, reason)?;
            Ok(Json(json!({"dead_lettered": true})))
        }
        SettleRequest::Extend {
            token,
            extension_ms,
        } => {
            let deadline_ms = state
                .engine
                .extend_queue_lease(&name, &token, extension_ms)?;
            Ok(Json(json!({"lease_deadline_ms": deadline_ms})))
        }
    }
}

async fn queue_counts(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<QueueCounts>> {
    Ok(Json(state.engine.queue(&name)?.lock().counts()))
}

async fn queue_redrive(
    State(state): State<AppState>,
    Path((name, message_id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    state.engine.redrive_queue(&name, &message_id)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn bus_upsert_subscription(
    State(state): State<AppState>,
    Path((name, subscription_name)): Path<(String, String)>,
    Json(mut subscription): Json<Subscription>,
) -> ApiResult<Json<Value>> {
    subscription.name = subscription_name;
    let route_plan_version = state.engine.upsert_subscription(&name, subscription)?;
    Ok(Json(json!({"route_plan_version": route_plan_version})))
}

async fn bus_remove_subscription(
    State(state): State<AppState>,
    Path((name, subscription_name)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    if state
        .engine
        .bus(&name)?
        .lock()
        .remove_subscription(&subscription_name)
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(EpochError::NotFound(subscription_name).into())
    }
}

async fn bus_publish(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(envelope): Json<EventEnvelope>,
) -> ApiResult<(StatusCode, Json<BusPublishOutcome>)> {
    let outcome = state.engine.publish_bus(&name, envelope)?;
    Ok((StatusCode::ACCEPTED, Json(outcome)))
}

#[derive(Debug, Deserialize)]
struct ReplayQuery {
    #[serde(default)]
    from_ms: u64,
    #[serde(default = "max_u64")]
    to_ms: u64,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    event_type: Option<String>,
}

const fn max_u64() -> u64 {
    u64::MAX
}

async fn bus_replay(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<ReplayQuery>,
) -> ApiResult<Json<Vec<ArchivedEvent>>> {
    let filter = query.event_type.map(|event_type| EventFilter {
        event_type_patterns: vec![event_type],
        ..EventFilter::default()
    });
    let events = state.engine.bus(&name)?.lock().replay(
        query.from_ms,
        query.to_ms,
        filter.as_ref(),
        query.limit.min(10_000),
    )?;
    Ok(Json(events))
}

type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug)]
struct ApiError(EpochError);

impl From<EpochError> for ApiError {
    fn from(value: EpochError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            EpochError::AlreadyExists(_) | EpochError::Conflict(_) | EpochError::Fenced => {
                StatusCode::CONFLICT
            }
            EpochError::NotFound(_) => StatusCode::NOT_FOUND,
            EpochError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            EpochError::Capacity(_) => StatusCode::INSUFFICIENT_STORAGE,
            EpochError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            EpochError::Storage(_) | EpochError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({"error": self.0}))).into_response()
    }
}
