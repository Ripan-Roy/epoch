//! Kafka ingestion with Epoch-authoritative partition offsets.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use epoch_bus::{ConnectorKind, ConnectorResource};
use epoch_core::EventEnvelope;
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    client::ClientContext,
    consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    message::Headers,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{sync::Mutex, time::timeout};
use url::Url;

use crate::{
    managed_target_delivery::{ManagedSecretStore, ManagedTargetDeliveryConfig, enforce_allowlist},
    source_adapters::{SourceBatch, SourceRecord},
};

const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_POLL_TIMEOUT: Duration = Duration::from_secs(5);
const NEXT_MESSAGE_TIMEOUT: Duration = Duration::from_millis(2);
const DEFAULT_MAX_BATCH_MESSAGES: usize = 256;
const MAX_BATCH_MESSAGES: usize = 1_000;
const DEFAULT_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const SEEK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KafkaFormat {
    Raw,
    CloudEventsJson,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KafkaSource {
    brokers: String,
    group_id: String,
    topics: Vec<String>,
    auto_offset_reset: String,
    security_protocol: String,
    sasl_mechanism: Option<String>,
    ssl_ca_location: Option<String>,
    ssl_certificate_location: Option<String>,
    ssl_key_location: Option<String>,
    poll_timeout: Duration,
    max_batch_messages: usize,
    max_batch_bytes: usize,
    format: KafkaFormat,
    secret_reference: Option<String>,
    source_identity: String,
}

#[derive(Debug)]
struct KafkaSecurity {
    protocol: String,
    sasl_mechanism: Option<String>,
    secret_reference: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaCursor {
    version: u8,
    source: String,
    #[serde(default)]
    offsets: BTreeMap<String, BTreeMap<i32, i64>>,
}

#[derive(Debug, Clone)]
struct PendingBatch {
    batch: SourceBatch,
    offsets: BTreeMap<String, BTreeMap<i32, i64>>,
}

#[derive(Debug, Clone, Default)]
struct KafkaContext {
    generation: Arc<AtomicU64>,
}

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, _rebalance: &Rebalance<'_>) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }
}

struct KafkaSession {
    consumer: StreamConsumer<KafkaContext>,
    checkpoint: String,
    cursor: KafkaCursor,
    applied_generation: u64,
    pending: Option<PendingBatch>,
}

impl std::fmt::Debug for KafkaSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KafkaSession")
            .field("checkpoint", &self.checkpoint)
            .field("cursor", &self.cursor)
            .field("applied_generation", &self.applied_generation)
            .field("pending", &self.pending)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct KafkaSourceAdapter {
    secrets: Arc<ManagedSecretStore>,
    allow_http_loopback: bool,
    sessions: Mutex<BTreeMap<String, KafkaSession>>,
}

impl KafkaSourceAdapter {
    pub(crate) fn new(config: &ManagedTargetDeliveryConfig) -> Self {
        Self {
            secrets: Arc::clone(&config.secrets),
            allow_http_loopback: config.allow_http_loopback,
            sessions: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn resolve(
        &self,
        name: &str,
        resource: &ConnectorResource,
    ) -> Result<Option<KafkaSource>, String> {
        if resource.spec.kind != ConnectorKind::Kafka {
            return Ok(None);
        }
        let brokers = required(&resource.spec.config, "brokers", name)?;
        let broker_hosts = validate_brokers(name, &brokers, &resource.spec.outbound_allowlist)?;
        let group_id = required(&resource.spec.config, "group_id", name)?;
        let raw_topics = required(&resource.spec.config, "topics", name)?;
        let topics = parse_topics(&raw_topics, name)?;
        let auto_offset_reset = resolve_auto_offset_reset(name, &resource.spec.config)?;
        let security = resolve_security(name, resource, &broker_hosts, self.allow_http_loopback)?;
        let poll_timeout = optional_duration(
            &resource.spec.config,
            "poll_timeout_ms",
            DEFAULT_POLL_TIMEOUT,
            MAX_POLL_TIMEOUT,
            name,
        )?;
        let max_batch_messages = optional_usize(
            &resource.spec.config,
            "max_batch_messages",
            DEFAULT_MAX_BATCH_MESSAGES,
            1,
            MAX_BATCH_MESSAGES,
            name,
        )?;
        let max_batch_bytes = optional_usize(
            &resource.spec.config,
            "max_batch_bytes",
            DEFAULT_MAX_BATCH_BYTES,
            1,
            MAX_BATCH_BYTES,
            name,
        )?;
        let format = resolve_format(name, &resource.spec.config)?;
        let source_identity = stable_hash(&[
            "kafka",
            &brokers,
            &group_id,
            &topics.join(","),
            &security.protocol,
        ]);
        Ok(Some(KafkaSource {
            brokers,
            group_id,
            topics,
            auto_offset_reset,
            security_protocol: security.protocol,
            sasl_mechanism: security.sasl_mechanism,
            ssl_ca_location: resource.spec.config.get("ssl_ca_location").cloned(),
            ssl_certificate_location: resource
                .spec
                .config
                .get("ssl_certificate_location")
                .cloned(),
            ssl_key_location: resource.spec.config.get("ssl_key_location").cloned(),
            poll_timeout,
            max_batch_messages,
            max_batch_bytes,
            format,
            secret_reference: security.secret_reference,
            source_identity,
        }))
    }

    pub(crate) async fn fetch(
        &self,
        runtime_key: &str,
        source: &KafkaSource,
        source_position: &str,
    ) -> Result<Option<SourceBatch>, String> {
        let mut sessions = self.sessions.lock().await;
        let session = self.resolve_session(&mut sessions, runtime_key, source, source_position)?;
        if let Some(pending) = &session.pending {
            return Ok(Some(pending.batch.clone()));
        }

        let mut records = Vec::new();
        let mut total_bytes = 0_usize;
        let mut next_offsets = session.cursor.offsets.clone();
        while records.len() < source.max_batch_messages {
            let wait = if records.is_empty() {
                source.poll_timeout
            } else {
                NEXT_MESSAGE_TIMEOUT
            };
            let message = match timeout(wait, session.consumer.recv()).await {
                Err(_) => break,
                Ok(Err(error)) => return Err(format!("Kafka source receive failed: {error}")),
                Ok(Ok(message)) => message,
            };
            let generation = session
                .consumer
                .context()
                .generation
                .load(Ordering::Acquire);
            let observed = ObservedKafkaMessage::from_message(&message);
            drop(message);
            if generation != session.applied_generation {
                if !session.cursor.offsets.is_empty() {
                    seek_to_epoch_cursor(&session.consumer, &session.cursor)?;
                    session.applied_generation = generation;
                    continue;
                }
                session.applied_generation = generation;
            }
            let expected = next_offsets
                .get(&observed.topic)
                .and_then(|partitions| partitions.get(&observed.partition))
                .copied();
            if expected.is_some_and(|expected| observed.offset < expected) {
                continue;
            }
            if expected.is_some_and(|expected| observed.offset > expected) {
                return Err(format!(
                    "Kafka source offset gap for {}[{}]: expected {}, received {}",
                    observed.topic,
                    observed.partition,
                    expected.expect("checked"),
                    observed.offset
                ));
            }
            let record_bytes = observed
                .payload
                .len()
                .saturating_add(observed.key.as_ref().map_or(0, Vec::len));
            if total_bytes.saturating_add(record_bytes) > source.max_batch_bytes {
                if records.is_empty() {
                    records.push(SourceRecord::Error {
                        record_id: observed.record_id(&source.source_identity),
                        reason: "kafka_record_too_large".into(),
                    });
                    set_next_offset(&mut next_offsets, &observed);
                }
                break;
            }
            total_bytes = total_bytes.saturating_add(record_bytes);
            set_next_offset(&mut next_offsets, &observed);
            records.push(observed.into_record(source));
        }
        if records.is_empty() {
            return Ok(None);
        }
        let source_to = encode_cursor(&source.source_identity, &next_offsets)?;
        let batch = SourceBatch {
            batch_id: format!(
                "kafka-{}",
                stable_hash(&[&source.source_identity, source_position, &source_to])
            ),
            source_from: source_position.to_owned(),
            source_to,
            records,
        };
        session.pending = Some(PendingBatch {
            batch: batch.clone(),
            offsets: next_offsets,
        });
        Ok(Some(batch))
    }

    pub(crate) async fn acknowledge(
        &self,
        runtime_key: &str,
        source_to: &str,
    ) -> Result<(), String> {
        let mut sessions = self.sessions.lock().await;
        let session = sessions
            .get_mut(runtime_key)
            .ok_or_else(|| "Kafka session disappeared before acknowledgement".to_owned())?;
        let pending = session
            .pending
            .as_ref()
            .ok_or_else(|| "Kafka session has no pending batch".to_owned())?;
        if pending.batch.source_to != source_to {
            return Err("Kafka acknowledgement does not match the pending offsets".into());
        }
        commit_offsets(&session.consumer, &pending.offsets)?;
        source_to.clone_into(&mut session.checkpoint);
        session.cursor = KafkaCursor {
            version: 1,
            source: session.cursor.source.clone(),
            offsets: pending.offsets.clone(),
        };
        session.pending = None;
        Ok(())
    }

    pub(crate) async fn retain_active(&self, active: &BTreeSet<String>) {
        self.sessions
            .lock()
            .await
            .retain(|key, _| active.contains(key));
    }

    fn create_session(
        &self,
        source: &KafkaSource,
        source_position: &str,
        cursor: KafkaCursor,
    ) -> Result<KafkaSession, String> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &source.brokers)
            .set("group.id", &source.group_id)
            .set("client.id", format!("epoch-{}", source.source_identity))
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("enable.partition.eof", "false")
            .set("isolation.level", "read_committed")
            .set("auto.offset.reset", &source.auto_offset_reset)
            .set("partition.assignment.strategy", "cooperative-sticky")
            .set("security.protocol", &source.security_protocol);
        if let Some(ca) = &source.ssl_ca_location {
            config.set("ssl.ca.location", ca);
        }
        if let Some(cert) = &source.ssl_certificate_location {
            config.set("ssl.certificate.location", cert);
        }
        if let Some(key) = &source.ssl_key_location {
            config.set("ssl.key.location", key);
        }
        if let Some(mechanism) = &source.sasl_mechanism {
            let reference = source
                .secret_reference
                .as_deref()
                .expect("SASL resolution requires credentials");
            let credentials = self
                .secrets
                .connector_credentials(reference)
                .map_err(|error| error.to_string())?;
            let username = credentials.get("username").ok_or_else(|| {
                "Kafka SASL connector credentials require a username property".to_owned()
            })?;
            let password = credentials.get("password").ok_or_else(|| {
                "Kafka SASL connector credentials require a password property".to_owned()
            })?;
            config
                .set("sasl.mechanism", mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password);
        }
        let context = KafkaContext::default();
        let consumer: StreamConsumer<KafkaContext> = config
            .create_with_context(context)
            .map_err(|error| format!("Kafka source configuration failed: {error}"))?;
        consumer
            .subscribe(&source.topics.iter().map(String::as_str).collect::<Vec<_>>())
            .map_err(|error| format!("Kafka source subscription failed: {error}"))?;
        Ok(KafkaSession {
            consumer,
            checkpoint: source_position.to_owned(),
            cursor,
            applied_generation: 0,
            pending: None,
        })
    }

    fn resolve_session<'a>(
        &self,
        sessions: &'a mut BTreeMap<String, KafkaSession>,
        runtime_key: &str,
        source: &KafkaSource,
        source_position: &str,
    ) -> Result<&'a mut KafkaSession, String> {
        let cursor = match parse_cursor(source_position, &source.source_identity) {
            Ok(cursor) => cursor,
            Err(error) => {
                sessions.remove(runtime_key);
                return Err(error);
            }
        };
        if let Some(session) = sessions.get_mut(runtime_key) {
            reconcile_pending(session, source_position)?;
        }
        let recreate = sessions
            .get(runtime_key)
            .is_none_or(|session| session.checkpoint != source_position);
        if recreate {
            sessions.remove(runtime_key);
            let session = self.create_session(source, source_position, cursor)?;
            sessions.insert(runtime_key.to_owned(), session);
        }
        Ok(sessions
            .get_mut(runtime_key)
            .expect("Kafka session was inserted above"))
    }
}

fn reconcile_pending(session: &mut KafkaSession, source_position: &str) -> Result<(), String> {
    let Some(pending) = &session.pending else {
        return Ok(());
    };
    if pending.batch.source_from == source_position {
        return Ok(());
    }
    if pending.batch.source_to == source_position {
        commit_offsets(&session.consumer, &pending.offsets)?;
        source_position.clone_into(&mut session.checkpoint);
        session.cursor = KafkaCursor {
            version: 1,
            source: session.cursor.source.clone(),
            offsets: pending.offsets.clone(),
        };
        session.pending = None;
        return Ok(());
    }
    Err("Kafka checkpoint diverged from the pending batch".into())
}

fn seek_to_epoch_cursor(
    consumer: &StreamConsumer<KafkaContext>,
    cursor: &KafkaCursor,
) -> Result<(), String> {
    let assignment = consumer
        .assignment()
        .map_err(|error| format!("Kafka assignment lookup failed: {error}"))?;
    for element in assignment.elements() {
        if let Some(offset) = cursor
            .offsets
            .get(element.topic())
            .and_then(|partitions| partitions.get(&element.partition()))
        {
            consumer
                .seek(
                    element.topic(),
                    element.partition(),
                    Offset::Offset(*offset),
                    SEEK_TIMEOUT,
                )
                .map_err(|error| format!("Kafka Epoch-offset seek failed: {error}"))?;
        }
    }
    Ok(())
}

fn commit_offsets(
    consumer: &StreamConsumer<KafkaContext>,
    offsets: &BTreeMap<String, BTreeMap<i32, i64>>,
) -> Result<(), String> {
    let mut partitions = TopicPartitionList::new();
    for (topic, topic_offsets) in offsets {
        for (partition, offset) in topic_offsets {
            partitions
                .add_partition_offset(topic, *partition, Offset::Offset(*offset))
                .map_err(|error| format!("Kafka offset list is invalid: {error}"))?;
        }
    }
    consumer
        .commit(&partitions, CommitMode::Sync)
        .map_err(|error| format!("Kafka durable offset commit failed: {error}"))
}

#[derive(Debug, Clone)]
struct ObservedKafkaMessage {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_ms: u64,
    key: Option<Vec<u8>>,
    payload: Vec<u8>,
    headers: BTreeMap<String, String>,
}

impl ObservedKafkaMessage {
    fn from_message(message: &rdkafka::message::BorrowedMessage<'_>) -> Self {
        let headers = message.headers().map_or_else(BTreeMap::new, |headers| {
            headers
                .iter()
                .map(|header| {
                    let value = header.value.map_or_else(String::new, |value| {
                        std::str::from_utf8(value).map_or_else(
                            |_| format!("base64:{}", BASE64.encode(value)),
                            str::to_owned,
                        )
                    });
                    (header.key.to_owned(), value)
                })
                .collect()
        });
        Self {
            topic: message.topic().to_owned(),
            partition: message.partition(),
            offset: message.offset(),
            timestamp_ms: message
                .timestamp()
                .to_millis()
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or(0),
            key: message.key().map(ToOwned::to_owned),
            payload: message.payload().unwrap_or_default().to_vec(),
            headers,
        }
    }

    fn record_id(&self, source_identity: &str) -> String {
        format!(
            "kafka-{}",
            stable_hash(&[
                source_identity,
                &self.topic,
                &self.partition.to_string(),
                &self.offset.to_string(),
            ])
        )
    }

    fn into_record(self, source: &KafkaSource) -> SourceRecord {
        let id = self.record_id(&source.source_identity);
        let mut event = match source.format {
            KafkaFormat::CloudEventsJson => {
                match serde_json::from_slice::<EventEnvelope>(&self.payload) {
                    Ok(mut event) => {
                        event
                            .extensions
                            .insert("kafka_original_id".into(), Value::String(event.id.clone()));
                        event
                    }
                    Err(_) => {
                        return SourceRecord::Error {
                            record_id: id,
                            reason: "kafka_cloudevent_invalid".into(),
                        };
                    }
                }
            }
            KafkaFormat::Raw => EventEnvelope::new(
                format!("urn:epoch:kafka:{}:{}", source.source_identity, self.topic),
                "io.epoch.kafka.record.v1",
                json!({
                    "encoding": "base64",
                    "data": BASE64.encode(&self.payload)
                }),
                self.timestamp_ms,
            ),
        };
        event.id = id;
        event.key = self.key.as_ref().map(|key| {
            std::str::from_utf8(key)
                .map_or_else(|_| format!("base64:{}", BASE64.encode(key)), str::to_owned)
        });
        event.headers.extend(self.headers);
        event
            .extensions
            .insert("kafka_topic".into(), Value::String(self.topic));
        event.extensions.insert(
            "kafka_partition".into(),
            Value::from(i64::from(self.partition)),
        );
        event
            .extensions
            .insert("kafka_offset".into(), Value::from(self.offset));
        SourceRecord::Event(Box::new(event))
    }
}

fn set_next_offset(
    offsets: &mut BTreeMap<String, BTreeMap<i32, i64>>,
    message: &ObservedKafkaMessage,
) {
    offsets
        .entry(message.topic.clone())
        .or_default()
        .insert(message.partition, message.offset.saturating_add(1));
}

fn parse_cursor(position: &str, expected_source: &str) -> Result<KafkaCursor, String> {
    if position == "0" {
        return Ok(KafkaCursor {
            version: 1,
            source: expected_source.to_owned(),
            offsets: BTreeMap::new(),
        });
    }
    let cursor: KafkaCursor = serde_json::from_str(position)
        .map_err(|error| format!("Kafka checkpoint is invalid: {error}"))?;
    if cursor.version != 1 || cursor.source != expected_source {
        return Err("Kafka checkpoint belongs to a different source configuration".into());
    }
    validate_offsets(&cursor.offsets)?;
    Ok(cursor)
}

fn encode_cursor(
    source: &str,
    offsets: &BTreeMap<String, BTreeMap<i32, i64>>,
) -> Result<String, String> {
    validate_offsets(offsets)?;
    serde_json::to_string(&KafkaCursor {
        version: 1,
        source: source.to_owned(),
        offsets: offsets.clone(),
    })
    .map_err(|error| error.to_string())
}

fn validate_offsets(offsets: &BTreeMap<String, BTreeMap<i32, i64>>) -> Result<(), String> {
    if offsets.len() > 1_024 {
        return Err("Kafka checkpoint contains too many topics".into());
    }
    for (topic, partitions) in offsets {
        validate_topic(topic)?;
        if partitions.len() > 100_000
            || partitions
                .iter()
                .any(|(partition, offset)| *partition < 0 || *offset < 0)
        {
            return Err("Kafka checkpoint contains invalid partition offsets".into());
        }
    }
    Ok(())
}

fn resolve_auto_offset_reset(
    name: &str,
    config: &BTreeMap<String, String>,
) -> Result<String, String> {
    match config.get("auto_offset_reset").map(String::as_str) {
        None | Some("earliest") => Ok("earliest".into()),
        Some("latest") => Ok("latest".into()),
        Some(other) => Err(format!(
            "connector {name} Kafka auto_offset_reset {other} is unsupported"
        )),
    }
}

fn resolve_security(
    name: &str,
    resource: &ConnectorResource,
    broker_hosts: &[String],
    allow_http_loopback: bool,
) -> Result<KafkaSecurity, String> {
    let protocol = resource
        .spec
        .config
        .get("security_protocol")
        .map_or("ssl", String::as_str)
        .to_ascii_lowercase();
    if !matches!(protocol.as_str(), "ssl" | "sasl_ssl" | "plaintext") {
        return Err(format!(
            "connector {name} Kafka security_protocol is unsupported"
        ));
    }
    if protocol == "plaintext"
        && (!allow_http_loopback || !broker_hosts.iter().all(|host| is_loopback(host)))
    {
        return Err(format!(
            "connector {name} may use Kafka plaintext only for explicitly enabled loopback brokers"
        ));
    }
    let secret_reference = at_most_one_secret(name, &resource.spec.secret_refs)?;
    if protocol == "sasl_ssl" && secret_reference.is_none() {
        return Err(format!(
            "connector {name} Kafka SASL requires one credential reference"
        ));
    }
    let sasl_mechanism = (protocol == "sasl_ssl")
        .then(|| {
            resource
                .spec
                .config
                .get("sasl_mechanism")
                .map_or("SCRAM-SHA-512", String::as_str)
                .to_ascii_uppercase()
        })
        .filter(|mechanism| {
            matches!(
                mechanism.as_str(),
                "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"
            )
        });
    if protocol == "sasl_ssl" && sasl_mechanism.is_none() {
        return Err(format!(
            "connector {name} Kafka SASL mechanism is unsupported"
        ));
    }
    Ok(KafkaSecurity {
        protocol,
        sasl_mechanism,
        secret_reference,
    })
}

fn resolve_format(name: &str, config: &BTreeMap<String, String>) -> Result<KafkaFormat, String> {
    match config.get("format").map(String::as_str) {
        None | Some("raw") => Ok(KafkaFormat::Raw),
        Some("cloudevents_json") => Ok(KafkaFormat::CloudEventsJson),
        Some(other) => Err(format!(
            "connector {name} Kafka record format {other} is unsupported"
        )),
    }
}

fn validate_brokers(
    name: &str,
    brokers: &str,
    allowlist: &BTreeSet<String>,
) -> Result<Vec<String>, String> {
    let mut hosts = Vec::new();
    for broker in brokers.split(',') {
        let broker = broker.trim();
        if broker.is_empty() || broker.contains(['/', '@', '\\']) {
            return Err(format!("connector {name} Kafka broker is invalid"));
        }
        let (host, port) = broker
            .rsplit_once(':')
            .ok_or_else(|| format!("connector {name} Kafka broker requires host:port"))?;
        if host.is_empty()
            || port
                .parse::<u16>()
                .ok()
                .as_ref()
                .is_none_or(|port| *port == 0)
        {
            return Err(format!("connector {name} Kafka broker is invalid"));
        }
        let url = Url::parse(&format!("https://{host}/"))
            .map_err(|_| format!("connector {name} Kafka broker host is invalid"))?;
        enforce_allowlist(&url, allowlist, "connector").map_err(|error| format!("{error:?}"))?;
        hosts.push(host.to_owned());
    }
    if hosts.is_empty() || hosts.len() > 64 {
        return Err(format!("connector {name} requires 1-64 Kafka brokers"));
    }
    Ok(hosts)
}

fn parse_topics(raw: &str, name: &str) -> Result<Vec<String>, String> {
    let topics = raw
        .split(',')
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if topics.is_empty() || topics.len() > 256 {
        return Err(format!("connector {name} requires 1-256 Kafka topics"));
    }
    let mut unique = BTreeSet::new();
    for topic in &topics {
        validate_topic(topic)?;
        if !unique.insert(topic) {
            return Err(format!("connector {name} repeats Kafka topic {topic}"));
        }
    }
    Ok(topics)
}

fn validate_topic(topic: &str) -> Result<(), String> {
    if topic.is_empty()
        || topic.len() > 249
        || !topic
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("Kafka topic name is invalid".into());
    }
    Ok(())
}

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn at_most_one_secret(name: &str, refs: &BTreeSet<String>) -> Result<Option<String>, String> {
    if refs.len() > 1 {
        return Err(format!("connector {name} Kafka credentials are ambiguous"));
    }
    Ok(refs.iter().next().cloned())
}

fn required(config: &BTreeMap<String, String>, key: &str, name: &str) -> Result<String, String> {
    let value = config
        .get(key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("connector {name} requires Kafka configuration property {key}"))?;
    if value.len() > 8 * 1_024
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
    {
        return Err(format!("connector {name} Kafka property {key} is invalid"));
    }
    Ok(value.clone())
}

fn optional_duration(
    config: &BTreeMap<String, String>,
    key: &str,
    default: Duration,
    maximum: Duration,
    name: &str,
) -> Result<Duration, String> {
    config.get(key).map_or(Ok(default), |raw| {
        raw.parse::<u64>()
            .ok()
            .map(Duration::from_millis)
            .filter(|value| !value.is_zero() && *value <= maximum)
            .ok_or_else(|| format!("connector {name} Kafka property {key} is invalid"))
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
    config.get(key).map_or(Ok(default), |raw| {
        raw.parse::<usize>()
            .ok()
            .filter(|value| *value >= minimum && *value <= maximum)
            .ok_or_else(|| format!("connector {name} Kafka property {key} is invalid"))
    })
}

fn stable_hash(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/kafka-source/v1\0");
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    lower_hex(&hasher.finalize())
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
    use epoch_bus::{ConnectorDirection, ConnectorRegistry, ConnectorSpec};
    use rdkafka::producer::{FutureProducer, FutureRecord};

    use super::*;

    fn source(format: KafkaFormat) -> KafkaSource {
        KafkaSource {
            brokers: "localhost:9092".into(),
            group_id: "epoch-orders".into(),
            topics: vec!["orders".into()],
            auto_offset_reset: "earliest".into(),
            security_protocol: "plaintext".into(),
            sasl_mechanism: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            poll_timeout: Duration::from_millis(1),
            max_batch_messages: 10,
            max_batch_bytes: 1_024,
            format,
            secret_reference: None,
            source_identity: "source-a".into(),
        }
    }

    fn message(offset: i64, payload: &[u8]) -> ObservedKafkaMessage {
        ObservedKafkaMessage {
            topic: "orders".into(),
            partition: 2,
            offset,
            timestamp_ms: 100,
            key: Some(b"customer-1".to_vec()),
            payload: payload.to_vec(),
            headers: BTreeMap::from([("traceparent".into(), "trace".into())]),
        }
    }

    #[test]
    fn raw_message_identity_and_partition_cursor_are_stable() {
        let source = source(KafkaFormat::Raw);
        let observed = message(41, b"order-created");
        let first_id = observed.record_id(&source.source_identity);
        let second_id = observed.record_id(&source.source_identity);
        let record = observed.clone().into_record(&source);
        let mut offsets = BTreeMap::new();
        set_next_offset(&mut offsets, &observed);
        let encoded = encode_cursor(&source.source_identity, &offsets).unwrap();

        assert_eq!(first_id, second_id);
        assert_eq!(
            parse_cursor(&encoded, &source.source_identity)
                .unwrap()
                .offsets["orders"][&2],
            42
        );
        let SourceRecord::Event(event) = record else {
            panic!("expected a Kafka event");
        };
        assert_eq!(event.extensions["kafka_offset"], 41);
        assert_eq!(event.key.as_deref(), Some("customer-1"));
    }

    #[test]
    fn malformed_cloudevent_routes_stable_error_at_its_offset() {
        let source = source(KafkaFormat::CloudEventsJson);
        let observed = message(9, b"not-json");
        let expected_id = observed.record_id(&source.source_identity);
        assert!(matches!(
            observed.into_record(&source),
            SourceRecord::Error { record_id, reason }
                if record_id == expected_id && reason == "kafka_cloudevent_invalid"
        ));
    }

    #[test]
    fn resolution_enforces_allowlist_transport_and_cursor_identity() {
        let mut registry = ConnectorRegistry::default();
        registry
            .upsert(
                ConnectorSpec {
                    name: "orders-kafka".into(),
                    kind: ConnectorKind::Kafka,
                    direction: ConnectorDirection::Source,
                    secret_refs: BTreeSet::new(),
                    outbound_allowlist: BTreeSet::from(["localhost".into()]),
                    identity: "orders-reader".into(),
                    config: BTreeMap::from([
                        ("brokers".into(), "localhost:9092".into()),
                        ("group_id".into(), "epoch-orders".into()),
                        ("topics".into(), "orders,customers".into()),
                        ("security_protocol".into(), "plaintext".into()),
                    ]),
                },
                1,
            )
            .unwrap();
        let adapter = KafkaSourceAdapter {
            secrets: Arc::new(ManagedSecretStore::default()),
            allow_http_loopback: true,
            sessions: Mutex::new(BTreeMap::new()),
        };
        let resolved = adapter
            .resolve("orders-kafka", registry.connector("orders-kafka").unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(resolved.topics, vec!["orders", "customers"]);
        let cursor = encode_cursor(
            &resolved.source_identity,
            &BTreeMap::from([("orders".into(), BTreeMap::from([(0, 7)]))]),
        )
        .unwrap();
        assert!(parse_cursor(&cursor, &resolved.source_identity).is_ok());
        assert!(parse_cursor(&cursor, "another-source").is_err());
    }

    #[tokio::test]
    #[ignore = "requires deploy/compose/docker-compose.connectors.yml"]
    async fn live_group_consumer_checkpoints_then_commits_next_partition_offset() {
        let topic = "epoch-connector-conformance";
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:19092")
            .create()
            .unwrap();
        producer
            .send(
                FutureRecord::to(topic)
                    .key("order-1")
                    .payload("live-kafka-record"),
                Duration::from_secs(10),
            )
            .await
            .unwrap();

        let config = ManagedTargetDeliveryConfig {
            allow_http_loopback: true,
            ..ManagedTargetDeliveryConfig::default()
        };
        let adapter = KafkaSourceAdapter::new(&config);
        let mut registry = ConnectorRegistry::default();
        registry
            .upsert(
                ConnectorSpec {
                    name: "kafka-live".into(),
                    kind: ConnectorKind::Kafka,
                    direction: ConnectorDirection::Source,
                    secret_refs: BTreeSet::new(),
                    outbound_allowlist: BTreeSet::from(["127.0.0.1".into()]),
                    identity: "kafka-live-reader".into(),
                    config: BTreeMap::from([
                        ("brokers".into(), "127.0.0.1:19092".into()),
                        ("group_id".into(), "epoch-connector-conformance".into()),
                        ("topics".into(), topic.into()),
                        ("security_protocol".into(), "plaintext".into()),
                        ("auto_offset_reset".into(), "earliest".into()),
                        ("poll_timeout_ms".into(), "5000".into()),
                    ]),
                },
                1,
            )
            .unwrap();
        let source = adapter
            .resolve("kafka-live", registry.connector("kafka-live").unwrap())
            .unwrap()
            .unwrap();

        let mut batch = None;
        for _ in 0..5 {
            batch = adapter.fetch("live:kafka", &source, "0").await.unwrap();
            if batch.is_some() {
                break;
            }
        }
        let batch = batch.expect("Kafka group should receive the produced record");
        assert!(batch.records.iter().any(|record| {
            matches!(
                record,
                SourceRecord::Event(event)
                    if event.payload["data"] == BASE64.encode(b"live-kafka-record")
            )
        }));
        let cursor = parse_cursor(&batch.source_to, &source.source_identity).unwrap();
        assert!(
            cursor
                .offsets
                .get(topic)
                .is_some_and(|partitions| partitions.values().any(|offset| *offset > 0))
        );
        adapter
            .acknowledge("live:kafka", &batch.source_to)
            .await
            .unwrap();
        adapter.retain_active(&BTreeSet::new()).await;
        assert!(adapter.sessions.lock().await.is_empty());
    }
}
