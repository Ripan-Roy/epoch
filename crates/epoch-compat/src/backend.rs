use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::Write as _,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use epoch_core::EventEnvelope;
use flate2::{Compression as GzipCompression, GzBuilder};
use futures_util::StreamExt as _;
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

const MAX_HTTP_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_NATIVE_STREAM_BATCH_RECORDS: u16 = 1_000;
const MAX_NATIVE_STREAM_BATCH_COMPRESSED_BYTES: usize = 360 * 1024;
const MAX_CACHE_SET_ATTEMPTS: usize = 4;
const MAX_CACHE_COLLECTION_ITEMS: usize = 1_024;

/// One value stored through the Cache compatibility surface.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CacheValue {
    String(String),
    Blob(Vec<u8>),
    Counter(i64),
    Hash(BTreeMap<String, String>),
    List(Vec<String>),
    Set(Vec<String>),
    SortedSet(BTreeMap<String, f64>),
}

/// Native storage class retained by compatibility collection replacements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheStorageClass {
    #[default]
    Memory,
    Cold,
}

/// One observed Cache value and its absolute expiry.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheEntry {
    pub value: CacheValue,
    pub version: u64,
    pub expires_at_ms: Option<u64>,
    pub storage_class: CacheStorageClass,
}

/// Condition applied atomically with one compatibility Cache set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheSetCondition {
    #[default]
    Always,
    Missing,
    Present,
}

/// Redis-facing Cache set policy evaluated by the backend at one linearization point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheSetOptions {
    pub ttl_ms: Option<u64>,
    pub condition: CacheSetCondition,
    pub return_previous: bool,
}

/// Result of an atomic compatibility Cache set.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheSetOutcome {
    pub applied: bool,
    pub previous: Option<CacheEntry>,
}

/// One Redis collection mutation evaluated atomically by the backend.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheCollectionMutation {
    HashSet { entries: BTreeMap<String, String> },
    HashDelete { fields: Vec<String> },
    ListPush { values: Vec<String>, front: bool },
    ListPop { count: u32, front: bool },
    SetAdd { members: Vec<String> },
    SetRemove { members: Vec<String> },
    SortedSetAdd { entries: BTreeMap<String, f64> },
    SortedSetRemove { members: Vec<String> },
}

/// Protocol-relevant result of one atomic collection mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheCollectionResult {
    HashSet { added: u64 },
    HashDelete { removed: u64 },
    ListPush { length: u64 },
    ListPop { values: Vec<String> },
    SetAdd { added: u64 },
    SetRemove { removed: u64 },
    SortedSetAdd { added: u64 },
    SortedSetRemove { removed: u64 },
}

/// One record returned through the Stream compatibility surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRecord {
    pub offset: u64,
    pub timestamp_ms: u64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

/// One member of a replicated native Stream consumer-group session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGroupMember {
    pub member_id: String,
    pub assigned_partitions: Vec<u32>,
}

/// State returned after a native Stream consumer-group session mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGroupSession {
    pub generation: u64,
    pub members: Vec<StreamGroupMember>,
    pub assigned_partitions: Vec<u32>,
}

/// Durable rejection returned by the native Stream session coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamGroupRejection {
    UnknownGroup,
    UnknownMember,
    StaleGeneration,
    CapacityReached,
    Invalid,
}

/// Result of a replicated Stream consumer-group session mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGroupSessionResult {
    pub session: StreamGroupSession,
    pub rejection: Option<StreamGroupRejection>,
}

/// Consumer identity used to fence a Stream group offset commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGroupIdentity {
    pub member_id: String,
    pub generation: u64,
}

/// One message submitted through the Queue compatibility surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMessage {
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub exchange: Option<String>,
    pub routing_key: Option<String>,
    pub expiration: Option<String>,
    pub ttl_ms: Option<u64>,
}

/// One leased Queue delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDelivery {
    pub message_id: String,
    pub lease_token: String,
    pub redelivered: bool,
    pub message: QueueMessage,
}

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("resource was not found")]
    NotFound,
    #[error("operation conflicted with current state")]
    Conflict,
    #[error("operation is not valid for this value type")]
    WrongType,
    #[error("operation was rejected: {0}")]
    Invalid(String),
    #[error("backend is unavailable: {0}")]
    Unavailable(String),
}

/// Narrow semantic port consumed by all compatibility protocol sessions.
/// Implementations must preserve operation identity across uncertain retries.
#[async_trait]
pub trait CompatibilityBackend: Send + Sync + 'static {
    async fn cache_get(&self, cache: &str, key: &str) -> Result<Option<CacheEntry>, BackendError>;
    async fn cache_set(
        &self,
        cache: &str,
        key: &str,
        value: CacheValue,
        options: CacheSetOptions,
    ) -> Result<CacheSetOutcome, BackendError>;
    async fn cache_delete(&self, cache: &str, keys: &[String]) -> Result<u64, BackendError>;
    async fn cache_increment(
        &self,
        cache: &str,
        key: &str,
        delta: i64,
    ) -> Result<i64, BackendError>;
    async fn cache_expire(
        &self,
        cache: &str,
        key: &str,
        ttl_ms: Option<u64>,
    ) -> Result<bool, BackendError>;
    async fn cache_collection_mutate(
        &self,
        cache: &str,
        key: &str,
        mutation: CacheCollectionMutation,
    ) -> Result<CacheCollectionResult, BackendError>;

    async fn stream_partition_count(&self, stream: &str) -> Result<u32, BackendError>;
    async fn stream_append(
        &self,
        stream: &str,
        partition: u32,
        records: Vec<StreamRecord>,
    ) -> Result<u64, BackendError>;
    async fn stream_fetch(
        &self,
        stream: &str,
        partition: u32,
        offset: u64,
        limit: u32,
    ) -> Result<Vec<StreamRecord>, BackendError>;
    async fn stream_end_offset(&self, stream: &str, partition: u32) -> Result<u64, BackendError>;
    async fn stream_commit_offset(
        &self,
        group: &str,
        stream: &str,
        partition: u32,
        next_offset: u64,
        identity: Option<&StreamGroupIdentity>,
    ) -> Result<(), BackendError>;
    async fn stream_committed_offset(
        &self,
        group: &str,
        stream: &str,
        partition: u32,
    ) -> Result<Option<u64>, BackendError>;
    async fn stream_group_join(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        session_timeout_ms: u64,
    ) -> Result<StreamGroupSessionResult, BackendError>;
    async fn stream_group_observe(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
    ) -> Result<Option<StreamGroupSession>, BackendError>;
    async fn stream_group_heartbeat(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        generation: u64,
    ) -> Result<StreamGroupSessionResult, BackendError>;
    async fn stream_group_leave(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        generation: u64,
    ) -> Result<StreamGroupSessionResult, BackendError>;
    async fn stream_group_claim(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        generation: u64,
        partitions: &[u32],
    ) -> Result<(), BackendError>;

    async fn queue_exists(&self, queue: &str) -> Result<bool, BackendError>;
    async fn queue_publish(&self, queue: &str, message: QueueMessage) -> Result<(), BackendError>;
    async fn queue_acquire(
        &self,
        queue: &str,
        consumer: &str,
        max_messages: u16,
        visibility_timeout_ms: u64,
    ) -> Result<Vec<QueueDelivery>, BackendError>;
    async fn queue_ack(
        &self,
        queue: &str,
        consumer: &str,
        lease_token: &str,
    ) -> Result<(), BackendError>;
    async fn queue_reject(
        &self,
        queue: &str,
        consumer: &str,
        lease_token: &str,
        requeue: bool,
    ) -> Result<(), BackendError>;
}

/// Configuration for the authenticated native regional HTTP adapter.
#[derive(Clone)]
pub struct NativeHttpConfig {
    pub endpoints: Vec<Url>,
    pub token: String,
    pub organization: String,
    pub project: String,
    pub environment: String,
    pub namespace: String,
    pub timeout: Duration,
}

impl fmt::Debug for NativeHttpConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeHttpConfig")
            .field("endpoint_count", &self.endpoints.len())
            .field("token", &"<redacted>")
            .field("organization", &self.organization)
            .field("project", &self.project)
            .field("environment", &self.environment)
            .field("namespace", &self.namespace)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// Authenticated backend adapter that routes through Epoch's regional API.
#[derive(Debug, Clone)]
pub struct NativeHttpBackend {
    client: Client,
    config: Arc<NativeHttpConfig>,
    scope_path: String,
}

#[derive(Debug, Deserialize)]
struct RegionalRoute {
    resource_generation: String,
    tablet_epoch: String,
    term: String,
    accepts_writes: bool,
    #[serde(default)]
    stream_partitioning: Option<StreamPartitioning>,
}

#[derive(Debug, Deserialize)]
struct StreamPartitioning {
    shard_count: u32,
}

#[derive(Debug)]
struct NativeCacheObservation {
    shard_revision: u64,
    entry: Option<CacheEntry>,
}

impl NativeHttpBackend {
    pub fn new(config: NativeHttpConfig) -> Result<Self, BackendError> {
        if config.endpoints.is_empty() || config.token.trim().is_empty() || config.timeout.is_zero()
        {
            return Err(BackendError::Invalid(
                "endpoint, token, and positive timeout are required".into(),
            ));
        }
        if config.endpoints.iter().any(|endpoint| {
            !matches!(endpoint.scheme(), "http" | "https")
                || endpoint.host_str().is_none()
                || !endpoint.username().is_empty()
                || endpoint.password().is_some()
                || endpoint.query().is_some()
                || endpoint.fragment().is_some()
                || endpoint.path() != "/"
        }) {
            return Err(BackendError::Invalid(
                "endpoints must be credential-free HTTP(S) origins".into(),
            ));
        }
        let segments = [
            &config.organization,
            &config.project,
            &config.environment,
            &config.namespace,
        ];
        if segments
            .iter()
            .any(|segment| !valid_resource_segment(segment))
        {
            return Err(BackendError::Invalid(
                "scope segments must be non-empty URL-safe names".into(),
            ));
        }
        let scope_path = format!(
            "/v1/organizations/{}/projects/{}/environments/{}/namespaces/{}",
            config.organization, config.project, config.environment, config.namespace
        );
        let client = Client::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| BackendError::Invalid(error.to_string()))?;
        Ok(Self {
            client,
            config: Arc::new(config),
            scope_path,
        })
    }

    async fn discover(
        &self,
        collection: &str,
        resource: &str,
        shard: u32,
    ) -> Result<(Url, RegionalRoute), BackendError> {
        if !valid_resource_segment(resource) {
            return Err(BackendError::Invalid("invalid resource name".into()));
        }
        let path = format!(
            "{}/{}/{}/shards/{shard}",
            self.scope_path, collection, resource
        );
        let mut last_error = None;
        for endpoint in &self.config.endpoints {
            let url = join_url(endpoint, &path)?;
            match self
                .send_json::<RegionalRoute>(Method::GET, url.clone(), None, &[])
                .await
            {
                Ok(route)
                    if valid_decimal(&route.resource_generation)
                        && valid_decimal(&route.tablet_epoch)
                        && valid_decimal(&route.term) =>
                {
                    if route.accepts_writes {
                        return Ok((url, route));
                    }
                    last_error = Some(BackendError::Unavailable(
                        "endpoint is not the current leader".into(),
                    ));
                }
                Ok(_) => {
                    last_error = Some(BackendError::Unavailable(
                        "route response was incomplete".into(),
                    ));
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| BackendError::Unavailable("no endpoint".into())))
    }

    async fn read<T: DeserializeOwned>(
        &self,
        collection: &str,
        resource: &str,
        shard: u32,
        suffix: &str,
    ) -> Result<T, BackendError> {
        let (base, route) = self.discover(collection, resource, shard).await?;
        let url = suffix_url(&base, suffix);
        self.send_json(
            Method::GET,
            url,
            None,
            &[
                ("x-epoch-read-consistency", "linearizable"),
                ("x-epoch-resource-generation", &route.resource_generation),
                ("x-epoch-tablet-epoch", &route.tablet_epoch),
            ],
        )
        .await
    }

    async fn read_query<T: DeserializeOwned>(
        &self,
        collection: &str,
        resource: &str,
        shard: u32,
        suffix: &str,
        query: &[(&str, String)],
    ) -> Result<T, BackendError> {
        let (base, route) = self.discover(collection, resource, shard).await?;
        let mut url = suffix_url(&base, suffix);
        url.query_pairs_mut()
            .extend_pairs(query.iter().map(|(name, value)| (*name, value.as_str())));
        self.send_json(
            Method::GET,
            url,
            None,
            &[
                ("x-epoch-read-consistency", "linearizable"),
                ("x-epoch-resource-generation", &route.resource_generation),
                ("x-epoch-tablet-epoch", &route.tablet_epoch),
            ],
        )
        .await
    }

    async fn observe_cache(
        &self,
        cache: &str,
        key: &str,
    ) -> Result<NativeCacheObservation, BackendError> {
        let response: Value = self
            .read_query(
                "caches",
                cache,
                0,
                "/observations",
                &[("key", key.to_owned())],
            )
            .await?;
        let observation = response
            .get("observation")
            .ok_or_else(|| invalid_response("Cache observation is missing"))?;
        let shard_revision = decimal_field(observation, "shard_revision")?;
        let entry = observation
            .get("item")
            .filter(|value| !value.is_null())
            .map(|item| {
                Ok(CacheEntry {
                    value: decode_cache_value(
                        item.get("value")
                            .ok_or_else(|| invalid_response("Cache value is missing"))?,
                    )?,
                    version: decimal_field(item, "version")?,
                    expires_at_ms: optional_decimal_field(item, "expires_at_ms")?,
                    storage_class: decode_storage_class(item.get("storage_class"))?,
                })
            })
            .transpose()?;
        Ok(NativeCacheObservation {
            shard_revision,
            entry,
        })
    }

    async fn mutate(
        &self,
        collection: &str,
        resource: &str,
        shard: u32,
        operation: Value,
    ) -> Result<Value, BackendError> {
        let (base, route) = self.discover(collection, resource, shard).await?;
        let url = suffix_url(&base, "/mutations");
        let body = json!({
            "idempotency_key": Uuid::now_v7().to_string(),
            "expected_term": route.term,
            "operation": operation,
        });
        let response = self
            .send_json(
                Method::POST,
                url,
                Some(body),
                &[
                    ("x-epoch-resource-generation", &route.resource_generation),
                    ("x-epoch-tablet-epoch", &route.tablet_epoch),
                ],
            )
            .await?;
        require_applied_mutation(&response)?;
        Ok(response)
    }

    async fn stream_session_mutation(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        method: Method,
        suffix: String,
        fields: Value,
    ) -> Result<StreamGroupSessionResult, BackendError> {
        if !valid_resource_segment(group) || !valid_resource_segment(member_id) {
            return Err(BackendError::Invalid(
                "invalid Kafka group or member ID".into(),
            ));
        }
        let (base, route) = self.discover("streams", stream, 0).await?;
        let mut body = fields
            .as_object()
            .cloned()
            .ok_or_else(|| BackendError::Invalid("session fields must be an object".into()))?;
        body.insert(
            "idempotency_key".into(),
            Value::String(Uuid::now_v7().to_string()),
        );
        body.insert("expected_term".into(), Value::String(route.term.clone()));
        let response: Value = self
            .send_json(
                method,
                suffix_url(&base, &suffix),
                Some(Value::Object(body)),
                &[
                    ("x-epoch-resource-generation", &route.resource_generation),
                    ("x-epoch-tablet-epoch", &route.tablet_epoch),
                ],
            )
            .await?;
        decode_stream_group_session(&response)
    }

    async fn send_json<T: DeserializeOwned>(
        &self,
        method: Method,
        url: Url,
        body: Option<Value>,
        headers: &[(&str, &str)],
    ) -> Result<T, BackendError> {
        let mut request = self
            .client
            .request(method, url)
            .bearer_auth(&self.config.token)
            .header("accept", "application/json")
            .header(
                "user-agent",
                concat!("epoch-compat/", env!("CARGO_PKG_VERSION")),
            );
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| BackendError::Unavailable(error.to_string()))?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|length| length > MAX_HTTP_RESPONSE_BYTES as u64)
        {
            return Err(BackendError::Unavailable("response exceeds limit".into()));
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| BackendError::Unavailable(error.to_string()))?;
            if bytes.len().saturating_add(chunk.len()) > MAX_HTTP_RESPONSE_BYTES {
                return Err(BackendError::Unavailable("response exceeds limit".into()));
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            return Err(http_error(status, &bytes));
        }
        serde_json::from_slice(&bytes)
            .map_err(|error| BackendError::Unavailable(format!("invalid response: {error}")))
    }
}

#[async_trait]
impl CompatibilityBackend for NativeHttpBackend {
    async fn cache_get(&self, cache: &str, key: &str) -> Result<Option<CacheEntry>, BackendError> {
        Ok(self.observe_cache(cache, key).await?.entry)
    }

    async fn cache_set(
        &self,
        cache: &str,
        key: &str,
        value: CacheValue,
        options: CacheSetOptions,
    ) -> Result<CacheSetOutcome, BackendError> {
        for attempt in 0..MAX_CACHE_SET_ATTEMPTS {
            let observation = self.observe_cache(cache, key).await?;
            if options.return_previous
                && observation
                    .entry
                    .as_ref()
                    .is_some_and(|entry| !is_redis_string_value(&entry.value))
            {
                return Err(BackendError::WrongType);
            }
            let condition_matches = match options.condition {
                CacheSetCondition::Always => true,
                CacheSetCondition::Missing => observation.entry.is_none(),
                CacheSetCondition::Present => observation.entry.is_some(),
            };
            if !condition_matches {
                return Ok(CacheSetOutcome {
                    applied: false,
                    previous: observation.entry,
                });
            }
            let expected = observation.entry.as_ref().map_or_else(
                || json!({"kind":"missing", "shard_revision":observation.shard_revision.to_string()}),
                |entry| json!({"kind":"version", "version":entry.version.to_string()}),
            );
            let mut operation = Map::new();
            operation.insert("kind".into(), Value::String("compare_and_set".into()));
            operation.insert("shard".into(), Value::from(0));
            operation.insert("key".into(), Value::String(key.to_owned()));
            operation.insert("value".into(), encode_cache_value(value.clone()));
            operation.insert("expected".into(), expected);
            if let Some(ttl_ms) = options.ttl_ms {
                operation.insert("ttl_ms".into(), Value::String(ttl_ms.to_string()));
            }
            match self
                .mutate("caches", cache, 0, Value::Object(operation))
                .await
            {
                Ok(_) => {
                    return Ok(CacheSetOutcome {
                        applied: true,
                        previous: observation.entry,
                    });
                }
                Err(BackendError::Conflict) if attempt + 1 < MAX_CACHE_SET_ATTEMPTS => {}
                Err(error) => return Err(error),
            }
        }
        Err(BackendError::Conflict)
    }

    async fn cache_delete(&self, cache: &str, keys: &[String]) -> Result<u64, BackendError> {
        let mut deleted = 0_u64;
        for key in keys {
            let response: Value = self
                .mutate(
                    "caches",
                    cache,
                    0,
                    json!({"kind":"delete", "shard":0, "key":key}),
                )
                .await?;
            if response
                .pointer("/receipt/outcome/result/deleted")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    async fn cache_increment(
        &self,
        cache: &str,
        key: &str,
        delta: i64,
    ) -> Result<i64, BackendError> {
        let response: Value = self
            .mutate(
                "caches",
                cache,
                0,
                json!({"kind":"increment", "shard":0, "key":key, "delta":delta.to_string()}),
            )
            .await?;
        response
            .pointer("/receipt/outcome/result/value")
            .and_then(decimal_i64)
            .ok_or_else(|| invalid_response("Cache increment outcome is missing"))
    }

    async fn cache_expire(
        &self,
        cache: &str,
        key: &str,
        ttl_ms: Option<u64>,
    ) -> Result<bool, BackendError> {
        let Some(entry) = self.cache_get(cache, key).await? else {
            return Ok(false);
        };
        let operation = json!({
            "kind":"compare_and_set", "shard":0, "key":key,
            "expected":{"kind":"version", "version":entry.version.to_string()},
            "value":encode_cache_value(entry.value),
            "ttl_ms":ttl_ms.map(|value| value.to_string()),
        });
        let _: Value = self.mutate("caches", cache, 0, operation).await?;
        Ok(true)
    }

    async fn cache_collection_mutate(
        &self,
        cache: &str,
        key: &str,
        mutation: CacheCollectionMutation,
    ) -> Result<CacheCollectionResult, BackendError> {
        for attempt in 0..MAX_CACHE_SET_ATTEMPTS {
            let observation = self.observe_cache(cache, key).await?;
            let plan = plan_collection_mutation(
                observation.entry.as_ref().map(|entry| &entry.value),
                &mutation,
            )?;
            if !plan.changed {
                return Ok(plan.result);
            }
            let operation = match (&observation.entry, &plan.value) {
                (None, Some(value)) => json!({
                    "kind":"compare_and_set", "shard":0, "key":key,
                    "expected":{"kind":"missing", "shard_revision":observation.shard_revision.to_string()},
                    "value":encode_cache_value(value.clone()),
                }),
                (Some(entry), Some(value)) => json!({
                    "kind":"transform", "shard":0, "key":key,
                    "transform":{
                        "kind":"replace", "value":encode_cache_value(value.clone()),
                        "storage_class":encode_storage_class(entry.storage_class),
                    },
                    "expected_version":entry.version.to_string(),
                }),
                (Some(entry), None) => json!({
                    "kind":"delete", "shard":0, "key":key,
                    "expected_version":entry.version.to_string(),
                }),
                (None, None) => return Ok(plan.result),
            };
            match self.mutate("caches", cache, 0, operation).await {
                Ok(_) => return Ok(plan.result),
                Err(BackendError::Conflict) if attempt + 1 < MAX_CACHE_SET_ATTEMPTS => {}
                Err(error) => return Err(error),
            }
        }
        Err(BackendError::Conflict)
    }

    async fn stream_partition_count(&self, stream: &str) -> Result<u32, BackendError> {
        let (_, route) = self.discover("streams", stream, 0).await?;
        route
            .stream_partitioning
            .map(|partitioning| partitioning.shard_count)
            .filter(|count| *count > 0)
            .ok_or_else(|| invalid_response("Stream partition metadata is missing"))
    }

    async fn stream_append(
        &self,
        stream: &str,
        partition: u32,
        records: Vec<StreamRecord>,
    ) -> Result<u64, BackendError> {
        let record_count = u16::try_from(records.len())
            .ok()
            .filter(|count| (1..=MAX_NATIVE_STREAM_BATCH_RECORDS).contains(count))
            .ok_or_else(|| {
                BackendError::Invalid(format!(
                    "Kafka batch count exceeds limit of {MAX_NATIVE_STREAM_BATCH_RECORDS}"
                ))
            })?;
        let records = records
            .into_iter()
            .enumerate()
            .map(|(client_sequence, record)| NativeStreamBatchRecord {
                client_sequence: u32::try_from(client_sequence).unwrap_or(u32::MAX),
                envelope: stream_envelope(record),
            })
            .collect::<Vec<_>>();
        let uncompressed = serde_json::to_vec(&records)
            .map_err(|error| BackendError::Unavailable(error.to_string()))?;
        if uncompressed.len() > crate::MAX_MESSAGE_BYTES {
            return Err(BackendError::Invalid(
                "translated Kafka batch exceeds limit".into(),
            ));
        }
        let mut encoder = GzBuilder::new()
            .mtime(0)
            .write(Vec::new(), GzipCompression::default());
        encoder
            .write_all(&uncompressed)
            .map_err(|error| BackendError::Unavailable(error.to_string()))?;
        let compressed = encoder
            .finish()
            .map_err(|error| BackendError::Unavailable(error.to_string()))?;
        if compressed.len() > MAX_NATIVE_STREAM_BATCH_COMPRESSED_BYTES {
            return Err(BackendError::Invalid(format!(
                "translated Kafka batch exceeds limit of {MAX_NATIVE_STREAM_BATCH_COMPRESSED_BYTES} compressed bytes"
            )));
        }

        let (base, route) = self.discover("streams", stream, partition).await?;
        let url = suffix_url(&base, "/records/batches");
        let response: Value = self
            .send_json(
                Method::POST,
                url,
                Some(json!({
                    "idempotency_key":Uuid::now_v7().to_string(),
                    "expected_term":route.term,
                    "partition":0,
                    "compression":"gzip",
                    "record_count":record_count,
                    "uncompressed_bytes":uncompressed.len(),
                    "compressed_bytes":compressed.len(),
                    "payload_base64":STANDARD.encode(compressed),
                })),
                &[
                    ("x-epoch-resource-generation", &route.resource_generation),
                    ("x-epoch-tablet-epoch", &route.tablet_epoch),
                ],
            )
            .await?;
        response
            .pointer("/receipt/offset")
            .and_then(decimal_u64)
            .ok_or_else(|| invalid_response("Stream append offset is missing"))
    }

    async fn stream_fetch(
        &self,
        stream: &str,
        partition: u32,
        offset: u64,
        limit: u32,
    ) -> Result<Vec<StreamRecord>, BackendError> {
        let response: Value = self
            .read_query(
                "streams",
                stream,
                partition,
                "/records",
                &[
                    ("offset", offset.to_string()),
                    ("limit", limit.to_string()),
                    ("isolation", "read_committed".into()),
                ],
            )
            .await?;
        response
            .get("records")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid_response("Stream records are missing"))?
            .iter()
            .map(decode_stream_record)
            .collect()
    }

    async fn stream_end_offset(&self, stream: &str, partition: u32) -> Result<u64, BackendError> {
        let response: Value = self
            .read("streams", stream, partition, "/retention")
            .await?;
        response
            .pointer("/retention/end_offset")
            .and_then(decimal_u64)
            .ok_or_else(|| invalid_response("Stream end offset is missing"))
    }

    async fn stream_commit_offset(
        &self,
        group: &str,
        stream: &str,
        partition: u32,
        next_offset: u64,
        identity: Option<&StreamGroupIdentity>,
    ) -> Result<(), BackendError> {
        if !valid_resource_segment(group) {
            return Err(BackendError::Invalid("invalid Kafka group ID".into()));
        }
        let (base, route) = self.discover("streams", stream, partition).await?;
        let url = suffix_url(&base, &format!("/groups/{group}/offsets"));
        let (member_id, group_generation) = identity.map_or_else(
            || ("epoch-kafka-compat", 1),
            |identity| (identity.member_id.as_str(), identity.generation),
        );
        let response: Value = self
            .send_json(
                Method::PUT,
                url,
                Some(json!({
                    "idempotency_key":Uuid::now_v7().to_string(),
                    "expected_term":route.term,
                    "member_id":member_id,
                    "group_generation":group_generation.to_string(),
                    "partition":0,
                    "next_offset":next_offset.to_string(),
                    "mode":"commit",
                })),
                &[
                    ("x-epoch-resource-generation", &route.resource_generation),
                    ("x-epoch-tablet-epoch", &route.tablet_epoch),
                ],
            )
            .await?;
        match response.pointer("/receipt/outcome").and_then(Value::as_str) {
            Some("applied") => Ok(()),
            Some("rejected") => Err(BackendError::Conflict),
            _ => Err(invalid_response("Stream checkpoint outcome is missing")),
        }
    }

    async fn stream_committed_offset(
        &self,
        group: &str,
        stream: &str,
        partition: u32,
    ) -> Result<Option<u64>, BackendError> {
        if !valid_resource_segment(group) {
            return Err(BackendError::Invalid("invalid Kafka group ID".into()));
        }
        let response: Value = self
            .read_query(
                "streams",
                stream,
                partition,
                &format!("/groups/{group}/lag"),
                &[("partition", "0".into())],
            )
            .await?;
        let checkpoint = response
            .get("checkpoint")
            .ok_or_else(|| invalid_response("Stream checkpoint is missing"))?;
        if !checkpoint
            .get("exists")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Ok(None);
        }
        decimal_field(checkpoint, "committed_offset").map(Some)
    }

    async fn stream_group_join(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        session_timeout_ms: u64,
    ) -> Result<StreamGroupSessionResult, BackendError> {
        self.stream_session_mutation(
            stream,
            group,
            member_id,
            Method::POST,
            format!("/groups/{group}/sessions"),
            json!({
                "member_id":member_id,
                "session_timeout_ms":session_timeout_ms.to_string(),
            }),
        )
        .await
    }

    async fn stream_group_observe(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
    ) -> Result<Option<StreamGroupSession>, BackendError> {
        if !valid_resource_segment(group) || !valid_resource_segment(member_id) {
            return Err(BackendError::Invalid(
                "invalid Kafka group or member ID".into(),
            ));
        }
        let response: Value = self
            .read("streams", stream, 0, &format!("/groups/{group}/sessions"))
            .await?;
        decode_stream_group_observation(&response, member_id)
    }

    async fn stream_group_heartbeat(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        generation: u64,
    ) -> Result<StreamGroupSessionResult, BackendError> {
        self.stream_session_mutation(
            stream,
            group,
            member_id,
            Method::PUT,
            format!("/groups/{group}/sessions/{member_id}/heartbeat"),
            json!({"group_generation":generation.to_string()}),
        )
        .await
    }

    async fn stream_group_leave(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        generation: u64,
    ) -> Result<StreamGroupSessionResult, BackendError> {
        self.stream_session_mutation(
            stream,
            group,
            member_id,
            Method::DELETE,
            format!("/groups/{group}/sessions/{member_id}"),
            json!({"group_generation":generation.to_string()}),
        )
        .await
    }

    async fn stream_group_claim(
        &self,
        stream: &str,
        group: &str,
        member_id: &str,
        generation: u64,
        partitions: &[u32],
    ) -> Result<(), BackendError> {
        if !valid_resource_segment(group) || !valid_resource_segment(member_id) {
            return Err(BackendError::Invalid(
                "invalid Kafka group or member ID".into(),
            ));
        }
        for partition in partitions {
            let (base, route) = self.discover("streams", stream, *partition).await?;
            let url = suffix_url(&base, &format!("/groups/{group}/claim"));
            let response: Value = self
                .send_json(
                    Method::PUT,
                    url,
                    Some(json!({
                        "idempotency_key":Uuid::now_v7().to_string(),
                        "expected_term":route.term,
                        "member_id":member_id,
                        "group_generation":generation.to_string(),
                        "partition":0,
                    })),
                    &[
                        ("x-epoch-resource-generation", &route.resource_generation),
                        ("x-epoch-tablet-epoch", &route.tablet_epoch),
                    ],
                )
                .await?;
            match response.pointer("/receipt/outcome").and_then(Value::as_str) {
                Some("applied") => {}
                Some("rejected") => return Err(BackendError::Conflict),
                _ => return Err(invalid_response("Stream group claim outcome is missing")),
            }
        }
        Ok(())
    }

    async fn queue_exists(&self, queue: &str) -> Result<bool, BackendError> {
        match self.discover("queues", queue, 0).await {
            Ok(_) => Ok(true),
            Err(BackendError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn queue_publish(&self, queue: &str, message: QueueMessage) -> Result<(), BackendError> {
        let operation = json!({
            "kind":"enqueue", "partition":0,
            "envelope":queue_envelope(&message),
            "correlation_id":message.correlation_id,
            "reply_to":message.reply_to,
        });
        let _: Value = self.mutate("queues", queue, 0, operation).await?;
        Ok(())
    }

    async fn queue_acquire(
        &self,
        queue: &str,
        consumer: &str,
        max_messages: u16,
        visibility_timeout_ms: u64,
    ) -> Result<Vec<QueueDelivery>, BackendError> {
        let response: Value = self
            .mutate(
                "queues",
                queue,
                0,
                json!({
                    "kind":"acquire", "partition":0, "consumer":consumer,
                    "consumer_epoch":"1", "max_messages":max_messages,
                    "max_in_flight":max_messages,
                    "visibility_timeout_ms":visibility_timeout_ms.to_string(),
                }),
            )
            .await?;
        let deliveries = response
            .pointer("/receipt/outcome/result/deliveries")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid_response("Queue deliveries are missing"))?;
        deliveries.iter().map(decode_queue_delivery).collect()
    }

    async fn queue_ack(
        &self,
        queue: &str,
        consumer: &str,
        lease_token: &str,
    ) -> Result<(), BackendError> {
        let _: Value = self
            .mutate(
                "queues",
                queue,
                0,
                json!({
                    "kind":"acknowledge", "partition":0, "consumer":consumer,
                    "consumer_epoch":"1", "lease_token":lease_token,
                }),
            )
            .await?;
        Ok(())
    }

    async fn queue_reject(
        &self,
        queue: &str,
        consumer: &str,
        lease_token: &str,
        requeue: bool,
    ) -> Result<(), BackendError> {
        let operation = if requeue {
            json!({
                "kind":"release", "partition":0, "consumer":consumer,
                "consumer_epoch":"1", "lease_token":lease_token,
                "delay_ms":"0", "reason":"amqp.basic.reject",
            })
        } else {
            json!({
                "kind":"reject", "partition":0, "consumer":consumer,
                "consumer_epoch":"1", "lease_token":lease_token,
                "reason":"amqp.basic.reject",
            })
        };
        let _: Value = self.mutate("queues", queue, 0, operation).await?;
        Ok(())
    }
}

fn require_applied_mutation(response: &Value) -> Result<(), BackendError> {
    // A committed rejection is itself durable and may legitimately use HTTP
    // 201. Transport success is not proof that the profile mutation applied.
    match response
        .pointer("/receipt/outcome/status")
        .and_then(Value::as_str)
    {
        Some("applied") => Ok(()),
        Some("rejected") => match response
            .pointer("/receipt/outcome/code")
            .and_then(Value::as_str)
        {
            Some("conflict" | "fenced" | "already_exists") => Err(BackendError::Conflict),
            Some("not_found") => Err(BackendError::NotFound),
            Some("invalid_argument") => Err(BackendError::Invalid(
                "native operation was rejected".into(),
            )),
            Some("capacity" | "unavailable") => Err(BackendError::Unavailable(
                "native capacity or availability rejection".into(),
            )),
            _ => Err(invalid_response("unknown native rejection code")),
        },
        _ => Err(invalid_response("native mutation outcome is missing")),
    }
}

#[derive(Debug)]
pub(crate) struct CacheCollectionPlan {
    pub(crate) value: Option<CacheValue>,
    pub(crate) result: CacheCollectionResult,
    pub(crate) changed: bool,
}

#[allow(
    clippy::too_many_lines,
    reason = "one exhaustive planner keeps every collection mutation on the same atomic type boundary"
)]
pub(crate) fn plan_collection_mutation(
    current: Option<&CacheValue>,
    mutation: &CacheCollectionMutation,
) -> Result<CacheCollectionPlan, BackendError> {
    match mutation {
        CacheCollectionMutation::HashSet { entries } => {
            bounded_collection_items(entries.len())?;
            let mut hash = collection_value(current, "hash", |value| match value {
                CacheValue::Hash(hash) => Some(hash.clone()),
                _ => None,
            })?
            .unwrap_or_default();
            let added = entries
                .keys()
                .filter(|field| !hash.contains_key(*field))
                .count();
            let changed = entries
                .iter()
                .any(|(field, value)| hash.get(field) != Some(value));
            hash.extend(entries.clone());
            Ok(CacheCollectionPlan {
                value: Some(CacheValue::Hash(hash)),
                result: CacheCollectionResult::HashSet {
                    added: count_u64(added),
                },
                changed,
            })
        }
        CacheCollectionMutation::HashDelete { fields } => {
            bounded_collection_items(fields.len())?;
            let Some(mut hash) = collection_value(current, "hash", |value| match value {
                CacheValue::Hash(hash) => Some(hash.clone()),
                _ => None,
            })?
            else {
                return Ok(unchanged(CacheCollectionResult::HashDelete { removed: 0 }));
            };
            let removed = fields
                .iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .filter(|field| hash.remove(*field).is_some())
                .count();
            Ok(CacheCollectionPlan {
                value: (!hash.is_empty()).then_some(CacheValue::Hash(hash)),
                result: CacheCollectionResult::HashDelete {
                    removed: count_u64(removed),
                },
                changed: removed != 0,
            })
        }
        CacheCollectionMutation::ListPush { values, front } => {
            bounded_collection_items(values.len())?;
            let mut list = collection_value(current, "list", |value| match value {
                CacheValue::List(list) => Some(list.clone()),
                _ => None,
            })?
            .unwrap_or_default();
            if *front {
                let mut pushed = values.iter().rev().cloned().collect::<Vec<_>>();
                pushed.extend(list);
                list = pushed;
            } else {
                list.extend(values.clone());
            }
            let length = count_u64(list.len());
            Ok(CacheCollectionPlan {
                value: Some(CacheValue::List(list)),
                result: CacheCollectionResult::ListPush { length },
                changed: true,
            })
        }
        CacheCollectionMutation::ListPop { count, front } => {
            let count = usize::try_from(*count)
                .ok()
                .filter(|count| (1..=MAX_CACHE_COLLECTION_ITEMS).contains(count))
                .ok_or_else(|| BackendError::Invalid("collection count is out of range".into()))?;
            let Some(mut list) = collection_value(current, "list", |value| match value {
                CacheValue::List(list) => Some(list.clone()),
                _ => None,
            })?
            else {
                return Ok(unchanged(CacheCollectionResult::ListPop {
                    values: Vec::new(),
                }));
            };
            let removed = count.min(list.len());
            let values = if *front {
                list.drain(..removed).collect()
            } else {
                let mut values = list.split_off(list.len().saturating_sub(removed));
                values.reverse();
                values
            };
            Ok(CacheCollectionPlan {
                value: (!list.is_empty()).then_some(CacheValue::List(list)),
                changed: !values.is_empty(),
                result: CacheCollectionResult::ListPop { values },
            })
        }
        CacheCollectionMutation::SetAdd { members } => {
            bounded_collection_items(members.len())?;
            let mut set = collection_value(current, "set", |value| match value {
                CacheValue::Set(set) => Some(set.iter().cloned().collect::<BTreeSet<_>>()),
                _ => None,
            })?
            .unwrap_or_default();
            let added = members
                .iter()
                .filter(|member| set.insert((*member).clone()))
                .count();
            Ok(CacheCollectionPlan {
                value: Some(CacheValue::Set(set.into_iter().collect())),
                result: CacheCollectionResult::SetAdd {
                    added: count_u64(added),
                },
                changed: added != 0,
            })
        }
        CacheCollectionMutation::SetRemove { members } => {
            bounded_collection_items(members.len())?;
            let Some(mut set) = collection_value(current, "set", |value| match value {
                CacheValue::Set(set) => Some(set.iter().cloned().collect::<BTreeSet<_>>()),
                _ => None,
            })?
            else {
                return Ok(unchanged(CacheCollectionResult::SetRemove { removed: 0 }));
            };
            let removed = members
                .iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .filter(|member| set.remove(*member))
                .count();
            Ok(CacheCollectionPlan {
                value: (!set.is_empty()).then(|| CacheValue::Set(set.into_iter().collect())),
                result: CacheCollectionResult::SetRemove {
                    removed: count_u64(removed),
                },
                changed: removed != 0,
            })
        }
        CacheCollectionMutation::SortedSetAdd { entries } => {
            bounded_collection_items(entries.len())?;
            if entries.values().any(|score| !score.is_finite()) {
                return Err(BackendError::Invalid(
                    "sorted-set score must be finite".into(),
                ));
            }
            let mut set = collection_value(current, "sorted set", |value| match value {
                CacheValue::SortedSet(set) => Some(set.clone()),
                _ => None,
            })?
            .unwrap_or_default();
            let added = entries
                .keys()
                .filter(|member| !set.contains_key(*member))
                .count();
            let changed = entries
                .iter()
                .any(|(member, score)| set.get(member) != Some(score));
            set.extend(entries.clone());
            Ok(CacheCollectionPlan {
                value: Some(CacheValue::SortedSet(set)),
                result: CacheCollectionResult::SortedSetAdd {
                    added: count_u64(added),
                },
                changed,
            })
        }
        CacheCollectionMutation::SortedSetRemove { members } => {
            bounded_collection_items(members.len())?;
            let Some(mut set) = collection_value(current, "sorted set", |value| match value {
                CacheValue::SortedSet(set) => Some(set.clone()),
                _ => None,
            })?
            else {
                return Ok(unchanged(CacheCollectionResult::SortedSetRemove {
                    removed: 0,
                }));
            };
            let removed = members
                .iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .filter(|member| set.remove(*member).is_some())
                .count();
            Ok(CacheCollectionPlan {
                value: (!set.is_empty()).then_some(CacheValue::SortedSet(set)),
                result: CacheCollectionResult::SortedSetRemove {
                    removed: count_u64(removed),
                },
                changed: removed != 0,
            })
        }
    }
}

fn collection_value<T>(
    current: Option<&CacheValue>,
    _expected: &str,
    decode: impl FnOnce(&CacheValue) -> Option<T>,
) -> Result<Option<T>, BackendError> {
    match current {
        None => Ok(None),
        Some(value) => decode(value).map(Some).ok_or(BackendError::WrongType),
    }
}

fn bounded_collection_items(count: usize) -> Result<(), BackendError> {
    if (1..=MAX_CACHE_COLLECTION_ITEMS).contains(&count) {
        Ok(())
    } else {
        Err(BackendError::Invalid(
            "collection mutation item count is out of range".into(),
        ))
    }
}

const fn unchanged(result: CacheCollectionResult) -> CacheCollectionPlan {
    CacheCollectionPlan {
        value: None,
        result,
        changed: false,
    }
}

fn count_u64(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

fn is_redis_string_value(value: &CacheValue) -> bool {
    matches!(
        value,
        CacheValue::String(_) | CacheValue::Blob(_) | CacheValue::Counter(_)
    )
}

fn valid_resource_segment(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_decimal(value: &str) -> bool {
    value
        .parse::<u64>()
        .is_ok_and(|parsed| parsed > 0 && parsed.to_string() == value)
}

fn join_url(endpoint: &Url, path: &str) -> Result<Url, BackendError> {
    if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
        return Err(BackendError::Invalid("endpoint must be HTTP(S)".into()));
    }
    endpoint
        .join(path)
        .map_err(|error| BackendError::Invalid(error.to_string()))
}

fn suffix_url(base: &Url, suffix: &str) -> Url {
    let mut url = base.clone();
    url.set_path(&format!("{}{}", base.path().trim_end_matches('/'), suffix));
    url
}

fn http_error(status: StatusCode, bytes: &[u8]) -> BackendError {
    match status {
        StatusCode::NOT_FOUND => BackendError::NotFound,
        StatusCode::CONFLICT => BackendError::Conflict,
        status if status.is_client_error() => BackendError::Invalid(masked_error(bytes)),
        _ => BackendError::Unavailable(masked_error(bytes)),
    }
}

fn masked_error(bytes: &[u8]) -> String {
    let parsed: Value = serde_json::from_slice(bytes).unwrap_or(Value::Null);
    parsed
        .pointer("/error/code")
        .or_else(|| parsed.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("backend_error")
        .chars()
        .take(128)
        .collect()
}

fn invalid_response(detail: &str) -> BackendError {
    BackendError::Unavailable(detail.into())
}

fn decimal_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn decimal_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn decimal_field(value: &Value, field: &str) -> Result<u64, BackendError> {
    value
        .get(field)
        .and_then(decimal_u64)
        .ok_or_else(|| invalid_response("decimal response field is missing"))
}

fn optional_decimal_field(value: &Value, field: &str) -> Result<Option<u64>, BackendError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => decimal_u64(value)
            .map(Some)
            .ok_or_else(|| invalid_response("optional decimal response field is invalid")),
    }
}

fn encode_storage_class(storage_class: CacheStorageClass) -> &'static str {
    match storage_class {
        CacheStorageClass::Memory => "memory",
        CacheStorageClass::Cold => "cold",
    }
}

fn decode_stream_group_session(response: &Value) -> Result<StreamGroupSessionResult, BackendError> {
    let receipt = response
        .get("receipt")
        .ok_or_else(|| invalid_response("Stream session receipt is missing"))?;
    let generation = decimal_field(receipt, "group_generation")?;
    let members = receipt
        .get("members")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_response("Stream session members are missing"))?
        .iter()
        .map(|member| {
            let member_id = member
                .get("member_id")
                .and_then(Value::as_str)
                .filter(|value| valid_resource_segment(value))
                .ok_or_else(|| invalid_response("Stream session member ID is invalid"))?;
            Ok(StreamGroupMember {
                member_id: member_id.to_owned(),
                assigned_partitions: decode_partition_list(member, "assigned_shards")?,
            })
        })
        .collect::<Result<Vec<_>, BackendError>>()?;
    let rejection = match receipt.get("rejection").and_then(Value::as_str) {
        None => None,
        Some("unknown_group") => Some(StreamGroupRejection::UnknownGroup),
        Some("unknown_member") => Some(StreamGroupRejection::UnknownMember),
        Some("stale_generation") => Some(StreamGroupRejection::StaleGeneration),
        Some("group_capacity_reached" | "member_capacity_reached") => {
            Some(StreamGroupRejection::CapacityReached)
        }
        Some("shard_count_mismatch" | "deadline_overflow") => Some(StreamGroupRejection::Invalid),
        Some(_) => return Err(invalid_response("Stream session rejection is unknown")),
    };
    let outcome = receipt
        .get("outcome")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_response("Stream session outcome is missing"))?;
    if (outcome == "applied") != rejection.is_none() {
        return Err(invalid_response("Stream session outcome is inconsistent"));
    }
    if !matches!(outcome, "applied" | "rejected") {
        return Err(invalid_response("Stream session outcome is unknown"));
    }
    Ok(StreamGroupSessionResult {
        session: StreamGroupSession {
            generation,
            members,
            assigned_partitions: decode_partition_list(receipt, "assigned_shards")?,
        },
        rejection,
    })
}

fn decode_stream_group_observation(
    response: &Value,
    member_id: &str,
) -> Result<Option<StreamGroupSession>, BackendError> {
    let session = response
        .get("session")
        .ok_or_else(|| invalid_response("Stream session observation is missing"))?;
    if !session
        .get("exists")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let members = session
        .get("members")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_response("Stream session members are missing"))?
        .iter()
        .map(|member| {
            let observed_member_id = member
                .get("member_id")
                .and_then(Value::as_str)
                .filter(|value| valid_resource_segment(value))
                .ok_or_else(|| invalid_response("Stream session member ID is invalid"))?;
            Ok(StreamGroupMember {
                member_id: observed_member_id.to_owned(),
                assigned_partitions: decode_partition_list(member, "assigned_shards")?,
            })
        })
        .collect::<Result<Vec<_>, BackendError>>()?;
    let assigned_partitions = members
        .iter()
        .find(|member| member.member_id == member_id)
        .map_or_else(Vec::new, |member| member.assigned_partitions.clone());
    Ok(Some(StreamGroupSession {
        generation: decimal_field(session, "group_generation")?,
        members,
        assigned_partitions,
    }))
}

fn decode_partition_list(value: &Value, field: &str) -> Result<Vec<u32>, BackendError> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_response("Stream session assignment is missing"))?
        .iter()
        .map(|partition| {
            partition
                .as_u64()
                .and_then(|partition| u32::try_from(partition).ok())
                .ok_or_else(|| invalid_response("Stream session partition is invalid"))
        })
        .collect()
}

fn decode_storage_class(value: Option<&Value>) -> Result<CacheStorageClass, BackendError> {
    match value.and_then(Value::as_str) {
        None | Some("memory") => Ok(CacheStorageClass::Memory),
        Some("cold") => Ok(CacheStorageClass::Cold),
        Some(_) => Err(invalid_response("Cache storage class is invalid")),
    }
}

fn encode_cache_value(value: CacheValue) -> Value {
    match value {
        CacheValue::String(value) => json!({"kind":"string", "value":value}),
        CacheValue::Blob(value) => json!({"kind":"blob", "value":value}),
        CacheValue::Counter(value) => json!({"kind":"counter", "value":value}),
        CacheValue::Hash(value) => json!({"kind":"hash", "value":value}),
        CacheValue::List(value) => json!({"kind":"list", "value":value}),
        CacheValue::Set(value) => json!({"kind":"set", "value":value}),
        CacheValue::SortedSet(value) => json!({"kind":"sorted_set", "value":value}),
    }
}

fn decode_cache_value(value: &Value) -> Result<CacheValue, BackendError> {
    let kind = value
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_response("Cache value kind is missing"))?;
    let content = value
        .get("value")
        .ok_or_else(|| invalid_response("Cache value content is missing"))?;
    match kind {
        "string" => content
            .as_str()
            .map(|value| CacheValue::String(value.to_owned())),
        "blob" => serde_json::from_value(content.clone())
            .ok()
            .map(CacheValue::Blob),
        "counter" => decimal_i64(content).map(CacheValue::Counter),
        "hash" => serde_json::from_value(content.clone())
            .ok()
            .map(CacheValue::Hash),
        "list" => serde_json::from_value(content.clone())
            .ok()
            .map(CacheValue::List),
        "set" => serde_json::from_value(content.clone())
            .ok()
            .map(CacheValue::Set),
        "sorted_set" => serde_json::from_value(content.clone())
            .ok()
            .map(CacheValue::SortedSet),
        _ => None,
    }
    .ok_or_else(|| invalid_response("Cache value is invalid or unsupported"))
}

#[derive(Debug, Serialize)]
struct NativeStreamBatchRecord {
    client_sequence: u32,
    envelope: EventEnvelope,
}

fn stream_envelope(record: StreamRecord) -> EventEnvelope {
    let headers = record
        .headers
        .into_iter()
        .map(|(name, value)| (name, value.map(|value| STANDARD_NO_PAD.encode(value))))
        .collect::<Vec<_>>();
    EventEnvelope {
        id: Uuid::now_v7().to_string(),
        source: "epoch://compat/kafka".into(),
        event_type: "org.apache.kafka.record".into(),
        subject: None,
        time_ms: record.timestamp_ms,
        key: record
            .key
            .as_ref()
            .map(|value| STANDARD_NO_PAD.encode(value)),
        headers: BTreeMap::from([("epoch-compat-protocol".into(), "kafka".into())]),
        content_type: "application/vnd.apache.kafka.record+json".into(),
        schema_ref: None,
        traceparent: None,
        payload: json!({
            "format_version":2,
            "value_base64":record.value.map(|value| STANDARD_NO_PAD.encode(value)),
            "headers":headers,
        }),
        deliver_at_ms: None,
        ttl_ms: None,
        priority: 0,
        dedupe_id: None,
        transaction_id: None,
        extensions: BTreeMap::new(),
    }
}

fn decode_stream_record(value: &Value) -> Result<StreamRecord, BackendError> {
    let envelope = value
        .get("envelope")
        .ok_or_else(|| invalid_response("Stream envelope is missing"))?;
    let payload = envelope
        .get("payload")
        .ok_or_else(|| invalid_response("Kafka payload is missing"))?;
    let key = optional_base64(envelope.get("key"))?;
    let record_value = optional_base64(payload.get("value_base64"))?;
    let mut headers = Vec::new();
    match payload.get("format_version") {
        None => {
            let values = payload
                .get("headers")
                .and_then(Value::as_object)
                .ok_or_else(|| invalid_response("legacy Kafka headers are missing"))?;
            for (name, value) in values {
                headers.push((name.clone(), optional_base64(Some(value))?));
            }
        }
        Some(version) if version.as_u64() == Some(2) => {
            let values = payload
                .get("headers")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid_response("ordered Kafka headers are missing"))?;
            for pair in values {
                let pair = pair
                    .as_array()
                    .filter(|pair| pair.len() == 2)
                    .ok_or_else(|| invalid_response("invalid ordered Kafka header"))?;
                let name = pair[0]
                    .as_str()
                    .ok_or_else(|| invalid_response("invalid Kafka header name"))?;
                headers.push((name.to_owned(), optional_base64(Some(&pair[1]))?));
            }
        }
        Some(_) => return Err(invalid_response("unsupported Kafka envelope version")),
    }
    Ok(StreamRecord {
        offset: decimal_field(value, "offset")?,
        timestamp_ms: decimal_field(envelope, "time_ms")?,
        key,
        value: record_value,
        headers,
    })
}

fn optional_base64(value: Option<&Value>) -> Result<Option<Vec<u8>>, BackendError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => STANDARD_NO_PAD
            .decode(value)
            .map(Some)
            .map_err(|_| invalid_response("compatibility payload base64 is invalid")),
        Some(_) => Err(invalid_response("compatibility payload is invalid")),
    }
}

fn queue_envelope(message: &QueueMessage) -> Value {
    json!({
        "id":Uuid::now_v7().to_string(),
        "source":"epoch://compat/amqp",
        "type":"org.amqp.message",
        "time_ms":"0",
        "headers":message.headers,
        "content_type":"application/vnd.amqp.body+json",
        "payload":{
            "body_base64":STANDARD_NO_PAD.encode(&message.body),
            "content_type":message.content_type,
            "exchange":message.exchange,
            "routing_key":message.routing_key,
            "expiration":message.expiration,
        },
        "ttl_ms":message.ttl_ms,
        "priority":0,
        "extensions":{},
    })
}

fn decode_queue_delivery(value: &Value) -> Result<QueueDelivery, BackendError> {
    let envelope = value
        .get("envelope")
        .ok_or_else(|| invalid_response("Queue envelope is missing"))?;
    let payload = envelope
        .get("payload")
        .ok_or_else(|| invalid_response("AMQP payload is missing"))?;
    let body = payload
        .get("body_base64")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_response("AMQP body is missing"))?;
    let headers = envelope
        .get("headers")
        .and_then(Value::as_object)
        .map(|values| {
            values
                .iter()
                .filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.into())))
                .collect()
        })
        .unwrap_or_default();
    Ok(QueueDelivery {
        message_id: value
            .get("message_id")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_response("Queue message ID is missing"))?
            .into(),
        lease_token: value
            .get("lease_token")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_response("Queue lease token is missing"))?
            .into(),
        redelivered: value
            .get("attempt")
            .and_then(decimal_u64)
            .is_some_and(|attempt| attempt > 1),
        message: QueueMessage {
            body: STANDARD_NO_PAD
                .decode(body)
                .map_err(|_| invalid_response("AMQP body base64 is invalid"))?,
            content_type: payload
                .get("content_type")
                .and_then(Value::as_str)
                .map(str::to_owned),
            correlation_id: value
                .pointer("/metadata/correlation_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            reply_to: value
                .pointer("/metadata/reply_to")
                .and_then(Value::as_str)
                .map(str::to_owned),
            headers,
            exchange: payload
                .get("exchange")
                .and_then(Value::as_str)
                .map(str::to_owned),
            routing_key: payload
                .get("routing_key")
                .and_then(Value::as_str)
                .map(str::to_owned),
            expiration: payload
                .get("expiration")
                .and_then(Value::as_str)
                .map(str::to_owned),
            ttl_ms: optional_decimal_field(envelope, "ttl_ms")?,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_envelope_preserves_producer_time_and_ordered_duplicate_headers() {
        let original = StreamRecord {
            offset: 0,
            timestamp_ms: 1_700_000_000_000,
            key: Some(vec![0, 255]),
            value: None,
            headers: vec![
                ("z".into(), Some(vec![1])),
                ("a".into(), None),
                ("z".into(), Some(vec![2])),
                (String::new(), Some(vec![])),
            ],
        };
        let envelope = stream_envelope(original.clone());
        let recovered = decode_stream_record(&json!({
            "offset":"7", "appended_at_ms":"1800000000000", "envelope":envelope,
        }))
        .unwrap();
        assert_eq!(recovered.timestamp_ms, original.timestamp_ms);
        assert_eq!(recovered.headers, original.headers);
        assert_eq!(recovered.key, original.key);
        assert_eq!(recovered.value, original.value);
        assert_eq!(recovered.offset, 7);
    }

    #[test]
    fn kafka_legacy_header_map_remains_readable_without_rewriting_history() {
        let recovered = decode_stream_record(&json!({
            "offset":"7", "appended_at_ms":"1800000000000",
            "envelope":{"time_ms":"1700000000000", "key":null,
                "payload":{"value_base64":null, "headers":{"trace": "AA", "nullable":null}}},
        }))
        .unwrap();
        assert_eq!(recovered.timestamp_ms, 1_700_000_000_000);
        assert_eq!(
            recovered.headers,
            vec![("nullable".into(), None), ("trace".into(), Some(vec![0])),]
        );
    }
}
