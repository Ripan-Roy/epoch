//! Bounded, immutable-object ingestion for S3-compatible, Azure, and GCS stores.

use std::{collections::BTreeMap, sync::Arc};

use epoch_bus::{ConnectorKind, ConnectorResource};
use epoch_core::EventEnvelope;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{
    GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, aws::AmazonS3Builder,
    azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder, path::Path,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use crate::{
    managed_target_delivery::{ManagedSecretStore, ManagedTargetDeliveryConfig, enforce_allowlist},
    source_adapters::{SourceBatch, SourceRecord},
    webhook_delivery::safe_http_target,
};

const DEFAULT_MAX_BATCH_OBJECTS: usize = 8;
const MAX_BATCH_OBJECTS: usize = 64;
const DEFAULT_MAX_OBJECT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_OBJECT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_BATCH_BYTES: u64 = 16 * 1024 * 1024;
const MAX_BATCH_RECORDS: usize = 1_000;
const MAX_LIST_SCAN: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectFormat {
    Json,
    JsonArray,
    JsonLines,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ObjectProvider {
    S3 {
        bucket: String,
        region: String,
        endpoint: Option<String>,
        virtual_hosted_style: bool,
    },
    Azure {
        account: String,
        container: String,
        endpoint: Option<String>,
    },
    Gcs {
        bucket: String,
        endpoint: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectSource {
    provider: ObjectProvider,
    prefix: Option<Path>,
    format: ObjectFormat,
    max_batch_objects: usize,
    max_object_bytes: u64,
    secret_reference: Option<String>,
    anonymous: bool,
    source_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectCursor {
    version: u8,
    source: String,
    key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object_version: Option<String>,
    size: u64,
    last_modified_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ObjectSourceAdapter {
    secrets: Arc<ManagedSecretStore>,
    allow_http_loopback: bool,
}

impl ObjectSourceAdapter {
    pub(crate) fn new(config: &ManagedTargetDeliveryConfig) -> Self {
        Self {
            secrets: Arc::clone(&config.secrets),
            allow_http_loopback: config.allow_http_loopback,
        }
    }

    pub(crate) fn resolve(
        &self,
        name: &str,
        resource: &ConnectorResource,
    ) -> Result<Option<ObjectSource>, String> {
        if !matches!(
            resource.spec.kind,
            ConnectorKind::S3Compatible
                | ConnectorKind::AzureBlob
                | ConnectorKind::AzureDataLake
                | ConnectorKind::Gcs
        ) {
            return Ok(None);
        }
        let secret_reference = match resource.spec.secret_refs.len() {
            0 => None,
            1 => resource.spec.secret_refs.iter().next().cloned(),
            _ => return Err(format!("connector {name} object credentials are ambiguous")),
        };
        let anonymous = optional_bool(&resource.spec.config, "anonymous", false, name)?;
        if anonymous && secret_reference.is_some() {
            return Err(format!(
                "connector {name} cannot combine anonymous object access with credentials"
            ));
        }
        let endpoint = resource.spec.config.get("endpoint").cloned();
        if let Some(endpoint) = endpoint.as_deref() {
            validate_endpoint(
                name,
                endpoint,
                self.allow_http_loopback,
                &resource.spec.outbound_allowlist,
            )?;
        }
        let provider = resolve_provider(name, resource, endpoint)?;
        validate_default_endpoint(name, &provider, &resource.spec.outbound_allowlist)?;
        let prefix = resource
            .spec
            .config
            .get("prefix")
            .filter(|prefix| !prefix.is_empty())
            .map(|prefix| Path::parse(prefix).map_err(|error| error.to_string()))
            .transpose()
            .map_err(|error| format!("connector {name} prefix is invalid: {error}"))?;
        let format = match resource.spec.config.get("format").map(String::as_str) {
            None | Some("cloudevents_jsonl") => ObjectFormat::JsonLines,
            Some("cloudevents_json") => ObjectFormat::Json,
            Some("cloudevents_json_array") => ObjectFormat::JsonArray,
            Some(other) => {
                return Err(format!(
                    "connector {name} object format {other} is unsupported"
                ));
            }
        };
        let max_batch_objects = optional_usize(
            &resource.spec.config,
            "max_batch_objects",
            DEFAULT_MAX_BATCH_OBJECTS,
            1,
            MAX_BATCH_OBJECTS,
            name,
        )?;
        let max_object_bytes = u64::try_from(optional_usize(
            &resource.spec.config,
            "max_object_bytes",
            usize::try_from(DEFAULT_MAX_OBJECT_BYTES).expect("default fits usize"),
            1,
            usize::try_from(MAX_OBJECT_BYTES).expect("limit fits usize"),
            name,
        )?)
        .expect("validated object limit fits u64");
        let source_identity = source_identity(&provider, prefix.as_ref());
        Ok(Some(ObjectSource {
            provider,
            prefix,
            format,
            max_batch_objects,
            max_object_bytes,
            secret_reference,
            anonymous,
            source_identity,
        }))
    }

    pub(crate) async fn fetch(
        &self,
        source: &ObjectSource,
        source_position: &str,
    ) -> Result<Option<SourceBatch>, String> {
        let store = self.build_store(source)?;
        fetch_from_store(source, source_position, store).await
    }

    fn build_store(&self, source: &ObjectSource) -> Result<Arc<dyn ObjectStore>, String> {
        let credentials = source
            .secret_reference
            .as_deref()
            .map(|reference| {
                self.secrets
                    .connector_credentials(reference)
                    .map_err(|error| error.to_string())
            })
            .transpose()?;
        match &source.provider {
            ObjectProvider::S3 {
                bucket,
                region,
                endpoint,
                virtual_hosted_style,
            } => {
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_virtual_hosted_style_request(*virtual_hosted_style)
                    .with_skip_signature(source.anonymous);
                if let Some(endpoint) = endpoint {
                    builder = builder
                        .with_endpoint(endpoint)
                        .with_allow_http(endpoint.starts_with("http://"));
                }
                if let Some(credentials) = credentials {
                    builder = builder
                        .with_access_key_id(credential(credentials, "access_key_id")?)
                        .with_secret_access_key(credential(credentials, "secret_access_key")?);
                    if let Some(token) = credentials.get("session_token") {
                        builder = builder.with_token(token);
                    }
                }
                builder
                    .build()
                    .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                    .map_err(|error| format!("S3-compatible source configuration failed: {error}"))
            }
            ObjectProvider::Azure {
                account,
                container,
                endpoint,
            } => {
                let mut builder = MicrosoftAzureBuilder::from_env()
                    .with_account(account)
                    .with_container_name(container)
                    .with_skip_signature(source.anonymous);
                if let Some(endpoint) = endpoint {
                    builder = builder
                        .with_endpoint(endpoint.clone())
                        .with_allow_http(endpoint.starts_with("http://"));
                }
                if let Some(credentials) = credentials {
                    builder = if let Some(access_key) = credentials.get("access_key") {
                        builder.with_access_key(access_key)
                    } else if let Some(bearer) = credentials.get("bearer_token") {
                        builder.with_bearer_token_authorization(bearer)
                    } else {
                        builder.with_client_secret_authorization(
                            credential(credentials, "client_id")?,
                            credential(credentials, "client_secret")?,
                            credential(credentials, "tenant_id")?,
                        )
                    };
                }
                builder
                    .build()
                    .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                    .map_err(|error| format!("Azure object source configuration failed: {error}"))
            }
            ObjectProvider::Gcs { bucket, endpoint } => {
                let mut builder = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .with_skip_signature(source.anonymous);
                if let Some(endpoint) = endpoint {
                    builder = builder.with_base_url(endpoint);
                }
                if let Some(credentials) = credentials {
                    builder = if let Some(service_account) = credentials.get("service_account_json")
                    {
                        builder.with_service_account_key(service_account)
                    } else {
                        builder.with_bearer_token(credential(credentials, "bearer_token")?)
                    };
                }
                builder
                    .build()
                    .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                    .map_err(|error| format!("GCS source configuration failed: {error}"))
            }
        }
    }
}

fn resolve_provider(
    name: &str,
    resource: &ConnectorResource,
    endpoint: Option<String>,
) -> Result<ObjectProvider, String> {
    match resource.spec.kind {
        ConnectorKind::S3Compatible => Ok(ObjectProvider::S3 {
            bucket: required(&resource.spec.config, "bucket", name)?,
            region: resource
                .spec
                .config
                .get("region")
                .cloned()
                .unwrap_or_else(|| "us-east-1".into()),
            endpoint,
            virtual_hosted_style: optional_bool(
                &resource.spec.config,
                "virtual_hosted_style",
                false,
                name,
            )?,
        }),
        ConnectorKind::AzureBlob | ConnectorKind::AzureDataLake => Ok(ObjectProvider::Azure {
            account: required(&resource.spec.config, "account", name)?,
            container: required(&resource.spec.config, "container", name)?,
            endpoint,
        }),
        ConnectorKind::Gcs => Ok(ObjectProvider::Gcs {
            bucket: required(&resource.spec.config, "bucket", name)?,
            endpoint,
        }),
        _ => Err(format!("connector {name} is not an object source")),
    }
}

async fn fetch_from_store(
    source: &ObjectSource,
    source_position: &str,
    store: Arc<dyn ObjectStore>,
) -> Result<Option<SourceBatch>, String> {
    let cursor = parse_cursor(source_position, &source.source_identity)?;
    verify_checkpoint_object(store.as_ref(), &cursor).await?;
    let offset = Path::parse(&cursor.key).map_err(|error| error.to_string())?;
    let mut objects = store
        .list_with_offset(source.prefix.as_ref(), &offset)
        .take(MAX_LIST_SCAN + 1)
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| format!("object listing failed: {error}"))?;
    if objects.len() > MAX_LIST_SCAN {
        return Err(format!(
            "object listing exceeds the bounded {MAX_LIST_SCAN}-object scan; narrow the connector prefix"
        ));
    }
    objects.sort_by(|left, right| left.location.cmp(&right.location));
    objects.truncate(source.max_batch_objects);
    if objects.is_empty() {
        return Ok(None);
    }

    let mut records = Vec::new();
    let mut total_bytes = 0_u64;
    let mut last = None;
    for object in objects {
        if object.size > source.max_object_bytes {
            records.push(object_error(&object, "object_too_large", 0));
            last = Some(object);
            continue;
        }
        if total_bytes.saturating_add(object.size) > MAX_BATCH_BYTES {
            if records.is_empty() {
                records.push(object_error(&object, "batch_byte_limit", 0));
                last = Some(object);
            }
            break;
        }
        let bytes = get_immutable(store.as_ref(), &object).await?;
        let parsed = parse_object(source.format, &object, &bytes);
        if records.len().saturating_add(parsed.len()) > MAX_BATCH_RECORDS {
            if records.is_empty() {
                records.push(object_error(&object, "object_record_limit", 0));
                last = Some(object);
            }
            break;
        }
        total_bytes = total_bytes.saturating_add(object.size);
        records.extend(parsed);
        last = Some(object);
    }
    let Some(last) = last else {
        return Ok(None);
    };
    deduplicate_record_ids(&mut records, &last);
    let source_to = encode_cursor(&source.source_identity, &last)?;
    Ok(Some(SourceBatch {
        batch_id: batch_id(source_position, &source_to),
        source_from: source_position.to_owned(),
        source_to,
        records,
    }))
}

fn required(config: &BTreeMap<String, String>, key: &str, name: &str) -> Result<String, String> {
    config
        .get(key)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| format!("connector {name} requires {key}"))
}

fn optional_bool(
    config: &BTreeMap<String, String>,
    key: &str,
    default: bool,
    name: &str,
) -> Result<bool, String> {
    config
        .get(key)
        .map_or(Ok(default), |value| match value.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(format!("connector {name} {key} must be true or false")),
        })
}

fn optional_usize(
    config: &BTreeMap<String, String>,
    key: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
    name: &str,
) -> Result<usize, String> {
    config.get(key).map_or(Ok(default), |value| {
        value
            .parse::<usize>()
            .ok()
            .filter(|value| (*value >= minimum) && (*value <= maximum))
            .ok_or_else(|| {
                format!("connector {name} {key} must be between {minimum} and {maximum}")
            })
    })
}

fn validate_endpoint(
    name: &str,
    raw: &str,
    allow_http_loopback: bool,
    allowlist: &std::collections::BTreeSet<String>,
) -> Result<(), String> {
    let url = safe_http_target(raw, allow_http_loopback)
        .map_err(|_| format!("connector {name} endpoint is unsafe"))?;
    if !url.path().is_empty() && url.path() != "/" || url.query().is_some() {
        return Err(format!(
            "connector {name} endpoint must not contain a path or query"
        ));
    }
    enforce_allowlist(&url, allowlist, "connector")
        .map_err(|_| format!("connector {name} endpoint is not allowlisted"))
}

fn validate_default_endpoint(
    name: &str,
    provider: &ObjectProvider,
    allowlist: &std::collections::BTreeSet<String>,
) -> Result<(), String> {
    let raw = match provider {
        ObjectProvider::S3 {
            endpoint: Some(_), ..
        }
        | ObjectProvider::Azure {
            endpoint: Some(_), ..
        }
        | ObjectProvider::Gcs {
            endpoint: Some(_), ..
        } => return Ok(()),
        ObjectProvider::S3 {
            bucket,
            region,
            virtual_hosted_style,
            ..
        } => {
            if *virtual_hosted_style {
                format!("https://{bucket}.s3.{region}.amazonaws.com")
            } else {
                format!("https://s3.{region}.amazonaws.com")
            }
        }
        ObjectProvider::Azure { account, .. } => {
            format!("https://{account}.blob.core.windows.net")
        }
        ObjectProvider::Gcs { .. } => "https://storage.googleapis.com".into(),
    };
    let url = Url::parse(&raw).expect("derived provider endpoint is a valid URL");
    enforce_allowlist(&url, allowlist, "connector")
        .map_err(|_| format!("connector {name} provider endpoint is not allowlisted"))
}

fn source_identity(provider: &ObjectProvider, prefix: Option<&Path>) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epoch/object-source/v1\0");
    digest.update(format!("{provider:?}").as_bytes());
    digest.update([0]);
    if let Some(prefix) = prefix {
        digest.update(prefix.as_ref().as_bytes());
    }
    lower_hex(&digest.finalize())
}

fn parse_cursor(raw: &str, expected_source: &str) -> Result<ObjectCursor, String> {
    if raw == "0" {
        return Ok(ObjectCursor {
            version: 1,
            source: expected_source.into(),
            key: String::new(),
            etag: None,
            object_version: None,
            size: 0,
            last_modified_ms: 0,
        });
    }
    let cursor: ObjectCursor =
        serde_json::from_str(raw).map_err(|error| format!("object cursor is invalid: {error}"))?;
    if cursor.version != 1 || cursor.source != expected_source {
        return Err("object cursor is fenced by a different connector source".into());
    }
    Path::parse(&cursor.key).map_err(|error| format!("object cursor key is invalid: {error}"))?;
    Ok(cursor)
}

async fn verify_checkpoint_object(
    store: &dyn ObjectStore,
    cursor: &ObjectCursor,
) -> Result<(), String> {
    if cursor.key.is_empty() {
        return Ok(());
    }
    let path = Path::parse(&cursor.key).map_err(|error| error.to_string())?;
    match store.head(&path).await {
        Ok(meta) if same_checkpoint_object(&meta, cursor) => Ok(()),
        Ok(_) => Err(format!(
            "checkpointed object {} was overwritten; immutable source keys are required",
            cursor.key
        )),
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(format!("checkpoint object verification failed: {error}")),
    }
}

fn same_checkpoint_object(meta: &ObjectMeta, cursor: &ObjectCursor) -> bool {
    if meta.size != cursor.size {
        return false;
    }
    if let Some(version) = &cursor.object_version {
        return meta.version.as_ref() == Some(version);
    }
    if let Some(etag) = &cursor.etag {
        return meta.e_tag.as_ref() == Some(etag);
    }
    meta.last_modified.timestamp_millis() == cursor.last_modified_ms
}

async fn get_immutable(store: &dyn ObjectStore, object: &ObjectMeta) -> Result<Vec<u8>, String> {
    let options = GetOptions {
        if_match: object.e_tag.clone(),
        version: object.version.clone(),
        ..GetOptions::default()
    };
    let result = store
        .get_opts(&object.location, options)
        .await
        .map_err(|error| format!("object {} read failed: {error}", object.location))?;
    if result.meta.location != object.location
        || result.meta.e_tag != object.e_tag
        || result.meta.version != object.version
        || result.meta.size != object.size
    {
        return Err(format!(
            "object {} changed between list and read",
            object.location
        ));
    }
    result
        .bytes()
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(|error| format!("object {} body read failed: {error}", object.location))
}

fn parse_object(format: ObjectFormat, object: &ObjectMeta, bytes: &[u8]) -> Vec<SourceRecord> {
    match format {
        ObjectFormat::Json => vec![parse_event(object, 0, bytes)],
        ObjectFormat::JsonArray => {
            let values = serde_json::from_slice::<Vec<serde_json::Value>>(bytes);
            match values {
                Ok(values) if !values.is_empty() && values.len() <= MAX_BATCH_RECORDS => values
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| match serde_json::from_value(value) {
                        Ok(event) => SourceRecord::Event(Box::new(event)),
                        Err(_) => object_error(object, "invalid_cloudevent", index),
                    })
                    .collect(),
                Ok(_) => vec![object_error(object, "object_record_limit", 0)],
                Err(_) => vec![object_error(object, "invalid_json_array", 0)],
            }
        }
        ObjectFormat::JsonLines => {
            let lines = bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
                .collect::<Vec<_>>();
            if lines.is_empty() || lines.len() > MAX_BATCH_RECORDS {
                return vec![object_error(object, "object_record_limit", 0)];
            }
            lines
                .into_iter()
                .enumerate()
                .map(|(index, line)| parse_event(object, index, line))
                .collect()
        }
    }
}

fn parse_event(object: &ObjectMeta, index: usize, bytes: &[u8]) -> SourceRecord {
    match serde_json::from_slice::<EventEnvelope>(bytes) {
        Ok(event) if event.validate().is_ok() => SourceRecord::Event(Box::new(event)),
        Ok(_) | Err(_) => object_error(object, "invalid_cloudevent", index),
    }
}

fn object_error(object: &ObjectMeta, reason: &str, index: usize) -> SourceRecord {
    SourceRecord::Error {
        record_id: object_record_id(object, index),
        reason: reason.into(),
    }
}

fn object_record_id(object: &ObjectMeta, index: usize) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epoch/object-source-record/v1\0");
    digest.update(object.location.as_ref().as_bytes());
    digest.update([0]);
    if let Some(etag) = &object.e_tag {
        digest.update(etag.as_bytes());
    }
    digest.update([0]);
    digest.update(index.to_be_bytes());
    format!("object-{}", lower_hex(&digest.finalize()))
}

fn deduplicate_record_ids(records: &mut [SourceRecord], last: &ObjectMeta) {
    let mut seen = std::collections::BTreeSet::new();
    for (index, record) in records.iter_mut().enumerate() {
        if !seen.insert(record.record_id().to_owned()) {
            *record = object_error(last, "duplicate_event_id", index);
        }
    }
}

fn encode_cursor(source: &str, object: &ObjectMeta) -> Result<String, String> {
    serde_json::to_string(&ObjectCursor {
        version: 1,
        source: source.into(),
        key: object.location.to_string(),
        etag: object.e_tag.clone(),
        object_version: object.version.clone(),
        size: object.size,
        last_modified_ms: object.last_modified.timestamp_millis(),
    })
    .map_err(|error| format!("object cursor encoding failed: {error}"))
}

fn batch_id(source_from: &str, source_to: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epoch/object-source-batch/v1\0");
    digest.update(source_from.as_bytes());
    digest.update([0]);
    digest.update(source_to.as_bytes());
    format!("object-batch-{}", lower_hex(&digest.finalize()))
}

fn credential<'a>(credentials: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str, String> {
    credentials
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| format!("connector credentials require {key}"))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use epoch_bus::{ConnectorDirection, ConnectorRegistry, ConnectorSpec};
    use object_store::{PutPayload, memory::InMemory};
    use serde_json::json;

    use super::*;

    fn resource() -> ConnectorResource {
        let mut registry = ConnectorRegistry::default();
        registry
            .upsert(
                ConnectorSpec {
                    name: "archive-source".into(),
                    kind: ConnectorKind::S3Compatible,
                    direction: ConnectorDirection::Source,
                    secret_refs: BTreeSet::new(),
                    outbound_allowlist: BTreeSet::from(["127.0.0.1".into()]),
                    identity: "archive-reader".into(),
                    config: BTreeMap::from([
                        ("bucket".into(), "events".into()),
                        ("region".into(), "us-east-1".into()),
                        ("endpoint".into(), "http://127.0.0.1:9000".into()),
                        ("anonymous".into(), "true".into()),
                        ("prefix".into(), "orders".into()),
                    ]),
                },
                1,
            )
            .unwrap();
        registry.connector("archive-source").unwrap().clone()
    }

    fn event(id: &str) -> EventEnvelope {
        let mut event = EventEnvelope::new("urn:object", "order.created", json!({"id": id}), 1);
        event.id = id.into();
        event
    }

    #[test]
    fn resolves_bounded_s3_compatible_configuration_and_fences_cursor_identity() {
        let config = ManagedTargetDeliveryConfig {
            allow_http_loopback: true,
            ..ManagedTargetDeliveryConfig::default()
        };
        let adapter = ObjectSourceAdapter::new(&config);
        let source = adapter
            .resolve("archive-source", &resource())
            .unwrap()
            .unwrap();
        assert_eq!(source.max_batch_objects, DEFAULT_MAX_BATCH_OBJECTS);
        assert!(parse_cursor("0", &source.source_identity).is_ok());
        let foreign = serde_json::to_string(&ObjectCursor {
            version: 1,
            source: "foreign".into(),
            key: "orders/1.jsonl".into(),
            etag: None,
            object_version: None,
            size: 1,
            last_modified_ms: 1,
        })
        .unwrap();
        assert!(parse_cursor(&foreign, &source.source_identity).is_err());
    }

    #[tokio::test]
    async fn memory_store_reads_sorted_objects_routes_bad_records_and_resumes_exactly() {
        let store = InMemory::new();
        let first = Path::from("orders/0001.jsonl");
        let second = Path::from("orders/0002.jsonl");
        let first_body = format!(
            "{}\nnot-json\n",
            serde_json::to_string(&event("event-1")).unwrap()
        );
        store
            .put(&first, PutPayload::from(first_body))
            .await
            .unwrap();
        store
            .put(
                &second,
                PutPayload::from(serde_json::to_vec(&event("event-2")).unwrap()),
            )
            .await
            .unwrap();
        let source = ObjectSource {
            provider: ObjectProvider::S3 {
                bucket: "events".into(),
                region: "test".into(),
                endpoint: None,
                virtual_hosted_style: false,
            },
            prefix: Some(Path::from("orders")),
            format: ObjectFormat::JsonLines,
            max_batch_objects: 1,
            max_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            secret_reference: None,
            anonymous: true,
            source_identity: "memory-source".into(),
        };

        let batch = fetch_from_store(&source, "0", Arc::new(store.clone()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.records.len(), 2);
        assert!(matches!(batch.records[0], SourceRecord::Event(_)));
        assert!(matches!(batch.records[1], SourceRecord::Error { .. }));
        let resumed = fetch_from_store(&source, &batch.source_to, Arc::new(store))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed.records.len(), 1);
        assert_eq!(resumed.records[0].record_id(), "event-2");
    }

    #[tokio::test]
    async fn immutable_cursor_rejects_a_checkpointed_key_overwrite() {
        let store = InMemory::new();
        let path = Path::from("orders/0001.jsonl");
        store
            .put(
                &path,
                PutPayload::from(serde_json::to_vec(&event("event-1")).unwrap()),
            )
            .await
            .unwrap();
        let source = ObjectSource {
            provider: ObjectProvider::S3 {
                bucket: "events".into(),
                region: "test".into(),
                endpoint: None,
                virtual_hosted_style: false,
            },
            prefix: Some(Path::from("orders")),
            format: ObjectFormat::JsonLines,
            max_batch_objects: 1,
            max_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            secret_reference: None,
            anonymous: true,
            source_identity: "memory-overwrite-source".into(),
        };
        let first = fetch_from_store(&source, "0", Arc::new(store.clone()))
            .await
            .unwrap()
            .unwrap();
        store
            .put(
                &path,
                PutPayload::from(serde_json::to_vec(&event("event-2")).unwrap()),
            )
            .await
            .unwrap();

        assert!(
            fetch_from_store(&source, &first.source_to, Arc::new(store))
                .await
                .unwrap_err()
                .contains("was overwritten")
        );
    }

    #[tokio::test]
    #[ignore = "requires deploy/compose/docker-compose.connectors.yml"]
    async fn minio_conformance_reads_and_resumes_immutable_objects() {
        let config = crate::source_adapters::test_delivery_config(&json!([{
            "kind": "connector_credentials",
            "reference": "minio-creds",
            "values": {
                "access_key_id": "epoch-access",
                "secret_access_key": "epoch-secret-key"
            }
        }]));
        let adapter = ObjectSourceAdapter::new(&config);
        let mut registry = ConnectorRegistry::default();
        registry
            .upsert(
                ConnectorSpec {
                    name: "minio-source".into(),
                    kind: ConnectorKind::S3Compatible,
                    direction: ConnectorDirection::Source,
                    secret_refs: BTreeSet::from(["minio-creds".into()]),
                    outbound_allowlist: BTreeSet::from(["127.0.0.1".into()]),
                    identity: "minio-reader".into(),
                    config: BTreeMap::from([
                        ("bucket".into(), "events".into()),
                        ("region".into(), "us-east-1".into()),
                        ("endpoint".into(), "http://127.0.0.1:19000".into()),
                        ("prefix".into(), "conformance".into()),
                        ("max_batch_objects".into(), "1".into()),
                    ]),
                },
                1,
            )
            .unwrap();
        let source = adapter
            .resolve("minio-source", registry.connector("minio-source").unwrap())
            .unwrap()
            .unwrap();
        let store = adapter.build_store(&source).unwrap();
        store
            .put(
                &Path::from("conformance/0001.jsonl"),
                PutPayload::from(serde_json::to_vec(&event("minio-event-1")).unwrap()),
            )
            .await
            .unwrap();

        let batch = adapter.fetch(&source, "0").await.unwrap().unwrap();
        assert_eq!(batch.records[0].record_id(), "minio-event-1");
        assert!(
            adapter
                .fetch(&source, &batch.source_to)
                .await
                .unwrap()
                .is_none()
        );
    }
}
