use std::{collections::BTreeMap, fmt, io::Write as _, sync::Arc, time::Duration};

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

/// One observed Cache value and its absolute expiry.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheEntry {
    pub value: CacheValue,
    pub version: u64,
    pub expires_at_ms: Option<u64>,
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

/// One message submitted through the Queue compatibility surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueMessage {
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub headers: BTreeMap<String, String>,
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
        ttl_ms: Option<u64>,
        only_if_absent: bool,
        only_if_present: bool,
    ) -> Result<Option<CacheEntry>, BackendError>;
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
    ) -> Result<(), BackendError>;
    async fn stream_committed_offset(
        &self,
        group: &str,
        stream: &str,
        partition: u32,
    ) -> Result<Option<u64>, BackendError>;

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

    async fn mutate<T: DeserializeOwned>(
        &self,
        collection: &str,
        resource: &str,
        shard: u32,
        operation: Value,
    ) -> Result<T, BackendError> {
        let (base, route) = self.discover(collection, resource, shard).await?;
        let url = suffix_url(&base, "/mutations");
        let body = json!({
            "idempotency_key": Uuid::now_v7().to_string(),
            "expected_term": route.term,
            "operation": operation,
        });
        self.send_json(
            Method::POST,
            url,
            Some(body),
            &[
                ("x-epoch-resource-generation", &route.resource_generation),
                ("x-epoch-tablet-epoch", &route.tablet_epoch),
            ],
        )
        .await
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
        let Some(item) = observation.get("item").filter(|value| !value.is_null()) else {
            return Ok(None);
        };
        Ok(Some(CacheEntry {
            value: decode_cache_value(
                item.get("value")
                    .ok_or_else(|| invalid_response("Cache value is missing"))?,
            )?,
            version: decimal_field(item, "version")?,
            expires_at_ms: optional_decimal_field(item, "expires_at_ms")?,
        }))
    }

    async fn cache_set(
        &self,
        cache: &str,
        key: &str,
        value: CacheValue,
        ttl_ms: Option<u64>,
        only_if_absent: bool,
        only_if_present: bool,
    ) -> Result<Option<CacheEntry>, BackendError> {
        let observed: Value = self
            .read_query(
                "caches",
                cache,
                0,
                "/observations",
                &[("key", key.to_owned())],
            )
            .await?;
        let observation = observed
            .get("observation")
            .ok_or_else(|| invalid_response("Cache observation is missing"))?;
        let item = observation.get("item").filter(|item| !item.is_null());
        if (only_if_absent && item.is_some()) || (only_if_present && item.is_none()) {
            return Ok(None);
        }
        let mut operation = Map::new();
        operation.insert("shard".into(), Value::from(0));
        operation.insert("key".into(), Value::String(key.to_owned()));
        operation.insert("value".into(), encode_cache_value(value));
        if let Some(ttl_ms) = ttl_ms {
            operation.insert("ttl_ms".into(), Value::String(ttl_ms.to_string()));
        }
        if only_if_absent || only_if_present {
            operation.insert("kind".into(), Value::String("compare_and_set".into()));
            operation.insert(
                "expected".into(),
                if let Some(item) = item {
                    json!({"kind":"version", "version": decimal_field(item, "version")?.to_string()})
                } else {
                    json!({"kind":"missing", "shard_revision": decimal_field(observation, "revision")?.to_string()})
                },
            );
        } else {
            operation.insert("kind".into(), Value::String("set".into()));
        }
        let _: Value = self
            .mutate("caches", cache, 0, Value::Object(operation))
            .await?;
        self.cache_get(cache, key).await
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
    ) -> Result<(), BackendError> {
        if !valid_resource_segment(group) {
            return Err(BackendError::Invalid("invalid Kafka group ID".into()));
        }
        let (base, route) = self.discover("streams", stream, partition).await?;
        let url = suffix_url(&base, &format!("/groups/{group}/offsets"));
        let response: Value = self
            .send_json(
                Method::PUT,
                url,
                Some(json!({
                    "idempotency_key":Uuid::now_v7().to_string(),
                    "expected_term":route.term,
                    "member_id":"epoch-kafka-compat",
                    "group_generation":"1",
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
    let mut headers = Map::new();
    for (name, value) in record.headers {
        headers.insert(
            name,
            value.map_or(Value::Null, |value| {
                Value::String(STANDARD_NO_PAD.encode(value))
            }),
        );
    }
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
    if let Some(values) = payload.get("headers").and_then(Value::as_object) {
        for (name, value) in values {
            headers.push((name.clone(), optional_base64(Some(value))?));
        }
    }
    Ok(StreamRecord {
        offset: decimal_field(value, "offset")?,
        timestamp_ms: decimal_field(value, "appended_at_ms")?,
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
        },
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
        },
    })
}
