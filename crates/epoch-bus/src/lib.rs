//! Event routing, filtering, transformation, archive replay, and delivery state.

use std::collections::BTreeMap;

mod catalog;
mod connector;
mod delivery;
mod mqtt;
mod platform;
mod schema;

use epoch_core::{
    AckMetadata, DurabilityProfile, EpochError, EpochResult, EventEnvelope, validate_resource_name,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;

pub use catalog::{
    EndpointObservation, EndpointRegistry, EndpointRoute, EventCatalog, EventCatalogEntry,
};
pub use connector::{
    ConnectorBatchCommit, ConnectorBatchReceipt, ConnectorCheckpoint, ConnectorDirection,
    ConnectorKind, ConnectorRecordError, ConnectorRecordResult, ConnectorRegistry,
    ConnectorReplayRequest, ConnectorResource, ConnectorSecretVersion, ConnectorSpec,
    ConnectorStatus,
};
pub use delivery::{
    DEFAULT_MAX_OUTBOX_DELIVERIES, DeliveryAttempt, DeliveryAttemptOutcome,
    DeliveryBackoffStrategy, DeliveryCounts, DeliveryFence, DeliveryLease,
    DeliveryMaintenanceResult, DeliveryPolicy, DeliveryRateLimit, DeliveryRecord, DeliveryRedrive,
    DeliveryRetryPolicy, DeliveryState, DeliveryStateKind, EpochTargetDeliveryCandidate,
    EpochTargetDestination, EpochTargetKind, MAX_BUS_OUTBOX_DELIVERIES, MAX_DELIVERY_ACQUIRE_BATCH,
    MAX_DELIVERY_ATTEMPTS, MAX_DELIVERY_IN_FLIGHT, MAX_DELIVERY_LONG_POLL_MS,
    MAX_DELIVERY_QUERY_RESULTS, MAX_DELIVERY_REASON_BYTES, MAX_DELIVERY_TIMEOUT_MS,
    ManagedTargetDeliveryCandidate, SignedWebhookDeliveryCandidate,
};
use delivery::{DeliveryLedger, delivery_id};
pub use mqtt::{
    MqttBrokerState, MqttConnect, MqttDelivery, MqttPublish, MqttPublishPlan, MqttQos,
    MqttRetainedMessage, MqttSession, MqttSubscription,
};
pub use platform::{
    EnrichmentDefinition, EnrichmentLimits, EventIntegrationState, FunctionDefinition,
    FunctionResource, FunctionStatus, IntegrationOperation, IntegrationOutcome,
    MAX_EVENT_INTEGRATION_STATE_BYTES, SchemaValidationMode, SchemaValidationPolicy,
};
pub use schema::{
    SchemaCompatibility, SchemaField, SchemaFormat, SchemaRegistration, SchemaRegistry,
    SchemaRevision, SchemaValueType,
};

pub const DEFAULT_MAX_SUBSCRIPTIONS: usize = 1_024;
pub const DEFAULT_MAX_ARCHIVE_EVENTS: usize = 100_000;
pub const MAX_BUS_SUBSCRIPTIONS: usize = 100_000;
pub const MAX_BUS_ARCHIVE_EVENTS: usize = 10_000_000;
pub const MAX_ARCHIVE_RETENTION_MS: u64 = 10 * 365 * 24 * 60 * 60 * 1_000;
pub const MAX_REPLAY_EVENTS: usize = 10_000;
pub const MAX_FILTER_PATTERNS: usize = 64;
pub const MAX_FILTER_ENTRIES: usize = 64;
pub const MAX_TRANSFORM_ENTRIES: usize = 64;
pub const MAX_TRANSFORM_OPERATIONS: u16 = 256;
pub const MAX_TRANSFORM_OUTPUT_BYTES: usize = 1024 * 1024;
pub const MAX_TRANSFORM_VALUE_BYTES: usize = 256 * 1024;
pub const MAX_TRANSFORM_TIMEOUT_MS: u64 = 1_000;
pub const MAX_PATTERN_BYTES: usize = 512;
pub const MAX_HEADER_KEY_BYTES: usize = 256;
pub const MAX_HEADER_VALUE_BYTES: usize = 4 * 1024;
pub const MAX_JSON_PATH_BYTES: usize = 1_024;
pub const MAX_PROJECTED_FIELD_BYTES: usize = 256;
pub const MAX_FILTER_VALUE_BYTES: usize = 64 * 1024;
pub const MAX_TARGET_URL_BYTES: usize = 8 * 1024;
pub const EVENT_BUS_SNAPSHOT_FORMAT_VERSION: u16 = 5;
const INTEGRATION_EVENT_BUS_SNAPSHOT_FORMAT_VERSION: u16 = 4;
const EPOCH_TARGET_EVENT_BUS_SNAPSHOT_FORMAT_VERSION: u16 = 3;
const SIGNED_WEBHOOK_EVENT_BUS_SNAPSHOT_FORMAT_VERSION: u16 = 2;
const LEGACY_EVENT_BUS_SNAPSHOT_FORMAT_VERSION: u16 = 1;
pub const MAX_EVENT_BUS_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BusConfig {
    pub durability: DurabilityProfile,
    pub archive: bool,
    #[serde(default)]
    pub delivery_outbox: bool,
    #[serde(default = "default_max_subscriptions")]
    pub max_subscriptions: usize,
    #[serde(default = "default_max_archive_events")]
    pub max_archive_events: usize,
    #[serde(default, skip_serializing_if = "ArchiveRetentionPolicy::is_default")]
    pub archive_retention: ArchiveRetentionPolicy,
    #[serde(default = "default_max_outbox_deliveries")]
    pub max_outbox_deliveries: usize,
}

impl Default for BusConfig {
    fn default() -> Self {
        Self {
            durability: DurabilityProfile::Volatile,
            archive: true,
            delivery_outbox: false,
            max_subscriptions: DEFAULT_MAX_SUBSCRIPTIONS,
            max_archive_events: DEFAULT_MAX_ARCHIVE_EVENTS,
            archive_retention: ArchiveRetentionPolicy::default(),
            max_outbox_deliveries: DEFAULT_MAX_OUTBOX_DELIVERIES,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRetentionPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_events: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_ms: Option<u64>,
}

impl ArchiveRetentionPolicy {
    const fn is_default(&self) -> bool {
        self.max_events.is_none() && self.max_age_ms.is_none()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveMaintenanceResult {
    pub as_of_ms: u64,
    pub cutoff_ms: Option<u64>,
    pub purged: usize,
    pub archived_event_count: usize,
}

const fn default_max_subscriptions() -> usize {
    DEFAULT_MAX_SUBSCRIPTIONS
}

const fn default_max_archive_events() -> usize {
    DEFAULT_MAX_ARCHIVE_EVENTS
}

const fn default_max_outbox_deliveries() -> usize {
    DEFAULT_MAX_OUTBOX_DELIVERIES
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct EventFilter {
    #[serde(default)]
    pub topic_patterns: Vec<String>,
    #[serde(default)]
    pub event_type_patterns: Vec<String>,
    #[serde(default)]
    pub source_patterns: Vec<String>,
    #[serde(default)]
    pub subject_patterns: Vec<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub json_equals: BTreeMap<String, Value>,
}

impl EventFilter {
    pub fn matches(&self, event: &EventEnvelope) -> bool {
        matches_patterns(&self.topic_patterns, Some(event_topic(event)))
            && matches_patterns(&self.event_type_patterns, Some(&event.event_type))
            && matches_patterns(&self.source_patterns, Some(&event.source))
            && matches_patterns(&self.subject_patterns, event.subject.as_deref())
            && self
                .headers
                .iter()
                .all(|(key, expected)| event.headers.get(key) == Some(expected))
            && self.json_equals.iter().all(|(path, expected)| {
                json_path(&event.payload, path).is_some_and(|actual| actual == expected)
            })
    }

    fn validate(&self) -> EpochResult<()> {
        validate_patterns("topic_patterns", &self.topic_patterns)?;
        validate_patterns("event_type_patterns", &self.event_type_patterns)?;
        validate_patterns("source_patterns", &self.source_patterns)?;
        validate_patterns("subject_patterns", &self.subject_patterns)?;
        validate_map_capacity("headers", self.headers.len(), MAX_FILTER_ENTRIES)?;
        for (key, value) in &self.headers {
            validate_text("filter header name", key, MAX_HEADER_KEY_BYTES)?;
            validate_text("filter header value", value, MAX_HEADER_VALUE_BYTES)?;
        }
        validate_map_capacity("json_equals", self.json_equals.len(), MAX_FILTER_ENTRIES)?;
        for (path, expected) in &self.json_equals {
            validate_json_path(path)?;
            let encoded = serde_json::to_vec(expected)
                .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
            if encoded.len() > MAX_FILTER_VALUE_BYTES {
                return Err(EpochError::InvalidArgument(format!(
                    "JSON filter value is {} bytes; maximum is {MAX_FILTER_VALUE_BYTES}",
                    encoded.len()
                )));
            }
        }
        Ok(())
    }
}

fn matches_patterns(patterns: &[String], value: Option<&str>) -> bool {
    patterns.is_empty()
        || value.is_some_and(|value| patterns.iter().any(|pattern| glob_matches(pattern, value)))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SubscriptionTarget {
    Pull,
    Queue {
        resource: String,
    },
    Stream {
        resource: String,
    },
    Webhook {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signing_key_id: Option<String>,
    },
    Http {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signing_key_id: Option<String>,
    },
    ApiDestination {
        url: String,
        auth: DestinationAuth,
        #[serde(default)]
        cloud_events_mode: CloudEventsMode,
    },
    EndpointPool {
        pool: String,
        auth: DestinationAuth,
        #[serde(default)]
        cloud_events_mode: CloudEventsMode,
    },
    Function {
        resource: String,
    },
    Connector {
        resource: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DestinationAuth {
    None,
    ApiKey {
        secret_ref: String,
        header: String,
    },
    OAuth2 {
        secret_ref: String,
        token_url: String,
        #[serde(default)]
        scopes: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudEventsMode {
    #[default]
    Binary,
    Structured,
}

impl SubscriptionTarget {
    pub fn signing_key_id(&self) -> Option<&str> {
        match self {
            Self::Webhook { signing_key_id, .. } | Self::Http { signing_key_id, .. } => {
                signing_key_id.as_deref()
            }
            Self::Pull
            | Self::Queue { .. }
            | Self::Stream { .. }
            | Self::ApiDestination { .. }
            | Self::EndpointPool { .. }
            | Self::Function { .. }
            | Self::Connector { .. } => None,
        }
    }
}

impl<'de> Deserialize<'de> for SubscriptionTarget {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        StrictSubscriptionTarget::deserialize(deserializer).map(Into::into)
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StrictSubscriptionTarget {
    Pull(StrictPullTarget),
    Queue(StrictResourceTarget<QueueTargetKind>),
    Stream(StrictResourceTarget<StreamTargetKind>),
    Webhook(StrictSignedUrlTarget<WebhookTargetKind>),
    Http(StrictSignedUrlTarget<HttpTargetKind>),
    ApiDestination(StrictApiDestinationTarget),
    EndpointPool(StrictEndpointPoolTarget),
    Function(StrictResourceTarget<FunctionTargetKind>),
    Connector(StrictResourceTarget<ConnectorTargetKind>),
}

impl From<StrictSubscriptionTarget> for SubscriptionTarget {
    fn from(target: StrictSubscriptionTarget) -> Self {
        match target {
            StrictSubscriptionTarget::Pull(_) => Self::Pull,
            StrictSubscriptionTarget::Queue(target) => Self::Queue {
                resource: target.resource,
            },
            StrictSubscriptionTarget::Stream(target) => Self::Stream {
                resource: target.resource,
            },
            StrictSubscriptionTarget::Webhook(target) => Self::Webhook {
                url: target.url,
                signing_key_id: target.signing_key_id,
            },
            StrictSubscriptionTarget::Http(target) => Self::Http {
                url: target.url,
                signing_key_id: target.signing_key_id,
            },
            StrictSubscriptionTarget::ApiDestination(target) => Self::ApiDestination {
                url: target.url,
                auth: target.auth,
                cloud_events_mode: target.cloud_events_mode,
            },
            StrictSubscriptionTarget::EndpointPool(target) => Self::EndpointPool {
                pool: target.pool,
                auth: target.auth,
                cloud_events_mode: target.cloud_events_mode,
            },
            StrictSubscriptionTarget::Function(target) => Self::Function {
                resource: target.resource,
            },
            StrictSubscriptionTarget::Connector(target) => Self::Connector {
                resource: target.resource,
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictPullTarget {
    #[serde(rename = "kind")]
    _kind: PullTargetKind,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictResourceTarget<Kind> {
    #[serde(rename = "kind")]
    _kind: Kind,
    resource: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictSignedUrlTarget<Kind> {
    #[serde(rename = "kind")]
    _kind: Kind,
    url: String,
    #[serde(default)]
    signing_key_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictApiDestinationTarget {
    #[serde(rename = "kind")]
    _kind: ApiDestinationTargetKind,
    url: String,
    auth: DestinationAuth,
    #[serde(default)]
    cloud_events_mode: CloudEventsMode,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictEndpointPoolTarget {
    #[serde(rename = "kind")]
    _kind: EndpointPoolTargetKind,
    pool: String,
    auth: DestinationAuth,
    #[serde(default)]
    cloud_events_mode: CloudEventsMode,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum PullTargetKind {
    Pull,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum QueueTargetKind {
    Queue,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum StreamTargetKind {
    Stream,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WebhookTargetKind {
    Webhook,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum HttpTargetKind {
    Http,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ApiDestinationTargetKind {
    ApiDestination,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum EndpointPoolTargetKind {
    EndpointPool,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FunctionTargetKind {
    Function,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ConnectorTargetKind {
    Connector,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformLimits {
    #[serde(default = "default_transform_operations")]
    pub max_operations: u16,
    #[serde(default = "default_transform_output_bytes")]
    pub max_output_bytes: usize,
    #[serde(default = "default_transform_value_bytes")]
    pub max_value_bytes: usize,
    #[serde(default = "default_transform_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub network_access: bool,
}

impl Default for TransformLimits {
    fn default() -> Self {
        Self {
            max_operations: default_transform_operations(),
            max_output_bytes: default_transform_output_bytes(),
            max_value_bytes: default_transform_value_bytes(),
            timeout_ms: default_transform_timeout_ms(),
            network_access: false,
        }
    }
}

impl TransformLimits {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

const fn default_transform_operations() -> u16 {
    64
}

const fn default_transform_output_bytes() -> usize {
    256 * 1024
}

const fn default_transform_value_bytes() -> usize {
    64 * 1024
}

const fn default_transform_timeout_ms() -> u64 {
    100
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct EventTransform {
    #[serde(default)]
    pub add_headers: BTreeMap<String, String>,
    #[serde(default)]
    pub payload_projection: BTreeMap<String, String>,
    #[serde(default)]
    pub rename_fields: BTreeMap<String, String>,
    #[serde(default)]
    pub constants: BTreeMap<String, Value>,
    #[serde(default)]
    pub templates: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "TransformLimits::is_default")]
    pub limits: TransformLimits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enrichment_ref: Option<String>,
}

impl EventTransform {
    pub fn apply(&self, event: &EventEnvelope) -> EpochResult<EventEnvelope> {
        self.validate()?;
        let mut output = event.clone();
        output.headers.extend(self.add_headers.clone());
        if !self.payload_projection.is_empty()
            || !self.rename_fields.is_empty()
            || !self.constants.is_empty()
            || !self.templates.is_empty()
        {
            let mut projected = serde_json::Map::new();
            for (output_field, source_path) in &self.payload_projection {
                if let Some(value) = json_path(&event.payload, source_path) {
                    projected.insert(output_field.clone(), value.clone());
                }
            }
            for (source_path, output_field) in &self.rename_fields {
                if let Some(value) = json_path(&event.payload, source_path) {
                    projected.insert(output_field.clone(), value.clone());
                }
            }
            projected.extend(self.constants.clone());
            for (output_field, template) in &self.templates {
                projected.insert(
                    output_field.clone(),
                    Value::String(render_template(template, &event.payload)?),
                );
            }
            output.payload = Value::Object(projected);
        }
        let encoded = serde_json::to_vec(&output.payload)
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
        if encoded.len() > self.limits.max_output_bytes {
            return Err(EpochError::Capacity(format!(
                "transform output is {} bytes; configured maximum is {}",
                encoded.len(),
                self.limits.max_output_bytes
            )));
        }
        Ok(output)
    }

    fn validate(&self) -> EpochResult<()> {
        validate_map_capacity(
            "transform add_headers",
            self.add_headers.len(),
            MAX_TRANSFORM_ENTRIES,
        )?;
        for (key, value) in &self.add_headers {
            validate_text("transform header name", key, MAX_HEADER_KEY_BYTES)?;
            validate_text("transform header value", value, MAX_HEADER_VALUE_BYTES)?;
        }
        validate_map_capacity(
            "payload_projection",
            self.payload_projection.len(),
            MAX_TRANSFORM_ENTRIES,
        )?;
        for (output_field, source_path) in &self.payload_projection {
            validate_output_field(output_field)?;
            validate_json_path(source_path)?;
        }
        validate_map_capacity(
            "rename_fields",
            self.rename_fields.len(),
            MAX_TRANSFORM_ENTRIES,
        )?;
        for (source_path, output_field) in &self.rename_fields {
            validate_json_path(source_path)?;
            validate_output_field(output_field)?;
        }
        validate_map_capacity("constants", self.constants.len(), MAX_TRANSFORM_ENTRIES)?;
        for (output_field, value) in &self.constants {
            validate_output_field(output_field)?;
            validate_transform_value(value, self.limits.max_value_bytes)?;
        }
        validate_map_capacity("templates", self.templates.len(), MAX_TRANSFORM_ENTRIES)?;
        for (output_field, template) in &self.templates {
            validate_output_field(output_field)?;
            validate_text("transform template", template, MAX_HEADER_VALUE_BYTES)?;
            validate_template(template)?;
        }
        let operation_count = self
            .add_headers
            .len()
            .checked_add(self.payload_projection.len())
            .and_then(|count| count.checked_add(self.rename_fields.len()))
            .and_then(|count| count.checked_add(self.constants.len()))
            .and_then(|count| count.checked_add(self.templates.len()))
            .ok_or_else(|| EpochError::Capacity("transform operation count overflow".into()))?;
        if self.limits.max_operations == 0
            || self.limits.max_operations > MAX_TRANSFORM_OPERATIONS
            || operation_count > usize::from(self.limits.max_operations)
        {
            return Err(EpochError::InvalidArgument(format!(
                "transform operations must fit configured max_operations between 1 and {MAX_TRANSFORM_OPERATIONS}"
            )));
        }
        if self.limits.max_output_bytes == 0
            || self.limits.max_output_bytes > MAX_TRANSFORM_OUTPUT_BYTES
            || self.limits.max_value_bytes == 0
            || self.limits.max_value_bytes > MAX_TRANSFORM_VALUE_BYTES
            || self.limits.max_value_bytes > self.limits.max_output_bytes
        {
            return Err(EpochError::InvalidArgument(
                "transform memory limits are invalid".into(),
            ));
        }
        if self.limits.timeout_ms == 0 || self.limits.timeout_ms > MAX_TRANSFORM_TIMEOUT_MS {
            return Err(EpochError::InvalidArgument(format!(
                "transform timeout_ms must be between 1 and {MAX_TRANSFORM_TIMEOUT_MS}"
            )));
        }
        if self.limits.network_access {
            return Err(EpochError::InvalidArgument(
                "deterministic transforms cannot enable network access".into(),
            ));
        }
        if let Some(enrichment_ref) = &self.enrichment_ref {
            validate_resource_name(enrichment_ref)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub name: String,
    pub filter: EventFilter,
    pub target: SubscriptionTarget,
    #[serde(default)]
    pub transform: EventTransform,
    #[serde(default, skip_serializing_if = "DeliveryPolicy::is_default")]
    pub delivery_policy: DeliveryPolicy,
}

impl Subscription {
    pub fn validate(&self) -> EpochResult<()> {
        validate_resource_name(&self.name)?;
        self.filter.validate()?;
        self.transform.validate()?;
        self.delivery_policy.validate()?;
        match &self.target {
            SubscriptionTarget::Pull => {}
            SubscriptionTarget::Queue { resource }
            | SubscriptionTarget::Stream { resource }
            | SubscriptionTarget::Function { resource }
            | SubscriptionTarget::Connector { resource } => {
                validate_resource_name(resource)?;
            }
            SubscriptionTarget::Webhook {
                url,
                signing_key_id,
            }
            | SubscriptionTarget::Http {
                url,
                signing_key_id,
            } => {
                validate_http_target(url)?;
                if let Some(signing_key_id) = signing_key_id {
                    validate_resource_name(signing_key_id)?;
                }
            }
            SubscriptionTarget::ApiDestination { url, auth, .. } => {
                validate_http_target(url)?;
                validate_destination_auth(auth)?;
            }
            SubscriptionTarget::EndpointPool { pool, auth, .. } => {
                validate_resource_name(pool)?;
                validate_destination_auth(auth)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutedDelivery {
    pub delivery_id: String,
    pub subscription: String,
    pub target: SubscriptionTarget,
    pub envelope: EventEnvelope,
    pub route_plan_version: u64,
    pub delivery_policy: DeliveryPolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishResult {
    pub acknowledgement: AckMetadata,
    pub deliveries: Vec<RoutedDelivery>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArchivedEvent {
    pub position: u64,
    pub received_at_ms: u64,
    pub route_plan_version: u64,
    pub envelope: EventEnvelope,
}

#[derive(Debug, Clone)]
pub struct EventBus {
    config: BusConfig,
    subscriptions: BTreeMap<String, Subscription>,
    route_plan_version: u64,
    commit_position: u64,
    archive: Vec<ArchivedEvent>,
    archive_retention_watermark_ms: Option<u64>,
    delivery_ledger: DeliveryLedger,
    integration: EventIntegrationState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionedEventBusSnapshot {
    format_version: u16,
    config: BusConfig,
    subscriptions: BTreeMap<String, Subscription>,
    route_plan_version: u64,
    commit_position: u64,
    archive: Vec<ArchivedEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    archive_retention_watermark_ms: Option<u64>,
    delivery_ledger: DeliveryLedger,
    #[serde(default, skip_serializing_if = "EventIntegrationState::is_empty")]
    integration: EventIntegrationState,
    state_digest: [u8; 32],
}

impl EventBus {
    pub fn new(config: BusConfig) -> EpochResult<Self> {
        validate_config(&config)?;
        let delivery_ledger =
            DeliveryLedger::new(config.delivery_outbox, config.max_outbox_deliveries);
        Ok(Self {
            config,
            subscriptions: BTreeMap::new(),
            route_plan_version: 1,
            commit_position: 0,
            archive: Vec::new(),
            archive_retention_watermark_ms: None,
            delivery_ledger,
            integration: EventIntegrationState::default(),
        })
    }

    pub fn config(&self) -> &BusConfig {
        &self.config
    }

    pub fn upsert_subscription(&mut self, subscription: Subscription) -> EpochResult<u64> {
        subscription.validate()?;
        if let Some(enrichment_ref) = &subscription.transform.enrichment_ref
            && self.integration.enrichment(enrichment_ref).is_none()
        {
            return Err(EpochError::NotFound(format!(
                "enrichment {enrichment_ref} is not registered"
            )));
        }
        if !self.subscriptions.contains_key(&subscription.name)
            && self.subscriptions.len() >= self.config.max_subscriptions
        {
            return Err(EpochError::Capacity(format!(
                "event bus reached its {} subscription limit",
                self.config.max_subscriptions
            )));
        }
        let next_version = self
            .route_plan_version
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("route plan version overflow".into()))?;
        self.subscriptions
            .insert(subscription.name.clone(), subscription);
        self.route_plan_version = next_version;
        Ok(self.route_plan_version)
    }

    pub fn remove_subscription(&mut self, name: &str) -> EpochResult<bool> {
        validate_resource_name(name)?;
        if !self.subscriptions.contains_key(name) {
            return Ok(false);
        }
        let next_version = self
            .route_plan_version
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("route plan version overflow".into()))?;
        self.subscriptions.remove(name);
        self.route_plan_version = next_version;
        Ok(true)
    }

    pub fn publish(&mut self, event: EventEnvelope, now_ms: u64) -> EpochResult<PublishResult> {
        event.validate()?;
        self.integration.validate_for_broker(&event)?;
        let position = self
            .commit_position
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("event bus commit position overflow".into()))?;
        let route_plan_version = self.route_plan_version;
        let deliveries = self
            .subscriptions
            .values()
            .filter(|subscription| subscription.filter.matches(&event))
            .map(|subscription| {
                let enriched = if let Some(enrichment_ref) = &subscription.transform.enrichment_ref
                {
                    self.integration.enrich(enrichment_ref, &event)?
                } else {
                    event.clone()
                };
                Ok(RoutedDelivery {
                    delivery_id: delivery_id(position, &subscription.name),
                    subscription: subscription.name.clone(),
                    target: subscription.target.clone(),
                    envelope: subscription.transform.apply(&enriched)?,
                    route_plan_version,
                    delivery_policy: subscription.delivery_policy.clone(),
                })
            })
            .collect::<EpochResult<Vec<_>>>()?;
        let mut next_delivery_ledger = self.delivery_ledger.clone();
        next_delivery_ledger.append_publish(position, now_ms, &deliveries)?;
        let mut next_archive = self.archive.clone();
        let mut next_retention_watermark_ms = self.archive_retention_watermark_ms;
        if self.config.archive {
            let received_at_ms = effective_archive_time(
                self.config.archive_retention,
                next_retention_watermark_ms,
                now_ms,
            );
            next_archive.push(ArchivedEvent {
                position,
                received_at_ms,
                route_plan_version,
                envelope: event,
            });
            maintain_archive_records(
                &mut next_archive,
                self.config.archive_retention,
                &mut next_retention_watermark_ms,
                received_at_ms,
                usize::MAX,
            );
            if next_archive.len() > self.config.max_archive_events {
                return Err(EpochError::Capacity(format!(
                    "event archive reached its {} event limit",
                    self.config.max_archive_events
                )));
            }
        }
        self.commit_position = position;
        self.archive = next_archive;
        self.archive_retention_watermark_ms = next_retention_watermark_ms;
        self.delivery_ledger = next_delivery_ledger;
        Ok(PublishResult {
            acknowledgement: AckMetadata::standalone(position, self.config.durability),
            deliveries,
        })
    }

    pub fn replay(
        &self,
        from_ms: u64,
        to_ms: u64,
        filter: Option<&EventFilter>,
        limit: usize,
    ) -> EpochResult<Vec<ArchivedEvent>> {
        if from_ms > to_ms {
            return Err(EpochError::InvalidArgument(
                "replay start must not be after end".into(),
            ));
        }
        if limit > MAX_REPLAY_EVENTS {
            return Err(EpochError::InvalidArgument(format!(
                "replay limit {limit} exceeds maximum {MAX_REPLAY_EVENTS}"
            )));
        }
        if let Some(filter) = filter {
            filter.validate()?;
        }
        Ok(self
            .archive
            .iter()
            .filter(|record| {
                record.received_at_ms >= from_ms
                    && record.received_at_ms <= to_ms
                    && filter.is_none_or(|filter| filter.matches(&record.envelope))
            })
            .take(limit)
            .cloned()
            .collect())
    }

    pub fn route_plan_version(&self) -> u64 {
        self.route_plan_version
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub const fn commit_position(&self) -> u64 {
        self.commit_position
    }

    pub fn archived_event_count(&self) -> usize {
        self.archive.len()
    }

    pub const fn archive_retention_watermark_ms(&self) -> Option<u64> {
        self.archive_retention_watermark_ms
    }

    pub fn maintain_archive(
        &mut self,
        now_ms: u64,
        max_events: usize,
    ) -> EpochResult<ArchiveMaintenanceResult> {
        if max_events == 0 || max_events > MAX_REPLAY_EVENTS {
            return Err(EpochError::InvalidArgument(format!(
                "archive maintenance max_events must be between 1 and {MAX_REPLAY_EVENTS}"
            )));
        }
        if !self.config.archive {
            return Err(EpochError::Conflict(
                "archive maintenance requires an enabled Event Bus archive".into(),
            ));
        }
        let effective_now_ms = effective_archive_time(
            self.config.archive_retention,
            self.archive_retention_watermark_ms,
            now_ms,
        );
        Ok(maintain_archive_records(
            &mut self.archive,
            self.config.archive_retention,
            &mut self.archive_retention_watermark_ms,
            effective_now_ms,
            max_events,
        ))
    }

    pub fn next_archive_retention_deadline_ms(&self) -> Option<u64> {
        let max_age_ms = self.config.archive_retention.max_age_ms?;
        self.archive
            .first()
            .and_then(|record| record.received_at_ms.checked_add(max_age_ms))
    }

    pub fn acquire_deliveries(
        &mut self,
        subscription: &str,
        dispatcher: &str,
        max_deliveries: usize,
        now_ms: u64,
        fence: DeliveryFence,
    ) -> EpochResult<Vec<DeliveryLease>> {
        let mut candidate = self.delivery_ledger.clone();
        let deliveries =
            candidate.acquire(subscription, dispatcher, max_deliveries, now_ms, fence)?;
        self.delivery_ledger = candidate;
        Ok(deliveries)
    }

    pub fn has_acquirable_pull_delivery(
        &self,
        subscription: &str,
        now_ms: u64,
    ) -> EpochResult<bool> {
        let mut candidate = self.delivery_ledger.clone();
        candidate.has_acquirable_pull(subscription, now_ms)
    }

    pub fn acquire_specific_delivery(
        &mut self,
        subscription: &str,
        delivery_id: &str,
        dispatcher: &str,
        now_ms: u64,
        fence: DeliveryFence,
    ) -> EpochResult<Option<DeliveryLease>> {
        let mut candidate = self.delivery_ledger.clone();
        let delivery = candidate.acquire_specific(
            subscription,
            delivery_id,
            dispatcher,
            now_ms,
            fence,
            None,
        )?;
        self.delivery_ledger = candidate;
        Ok(delivery)
    }

    pub fn acquire_specific_epoch_target_delivery(
        &mut self,
        subscription: &str,
        delivery_id: &str,
        dispatcher: &str,
        now_ms: u64,
        fence: DeliveryFence,
        destination: EpochTargetDestination,
    ) -> EpochResult<Option<DeliveryLease>> {
        let mut candidate = self.delivery_ledger.clone();
        let delivery = candidate.acquire_specific(
            subscription,
            delivery_id,
            dispatcher,
            now_ms,
            fence,
            Some(destination),
        )?;
        self.delivery_ledger = candidate;
        Ok(delivery)
    }

    pub fn acknowledge_delivery(
        &mut self,
        delivery_id: &str,
        dispatcher: &str,
        lease_token: &str,
        fence: DeliveryFence,
        now_ms: u64,
    ) -> EpochResult<DeliveryRecord> {
        let mut candidate = self.delivery_ledger.clone();
        let record = candidate.acknowledge(delivery_id, dispatcher, lease_token, fence, now_ms)?;
        self.delivery_ledger = candidate;
        Ok(record)
    }

    pub fn fail_delivery(
        &mut self,
        delivery_id: &str,
        dispatcher: &str,
        lease_token: &str,
        fence: DeliveryFence,
        reason: &str,
        now_ms: u64,
    ) -> EpochResult<DeliveryRecord> {
        let mut candidate = self.delivery_ledger.clone();
        let record = candidate.fail(delivery_id, dispatcher, lease_token, fence, reason, now_ms)?;
        self.delivery_ledger = candidate;
        Ok(record)
    }

    pub fn reject_delivery(
        &mut self,
        delivery_id: &str,
        dispatcher: &str,
        lease_token: &str,
        fence: DeliveryFence,
        reason: &str,
        now_ms: u64,
    ) -> EpochResult<DeliveryRecord> {
        let mut candidate = self.delivery_ledger.clone();
        let record =
            candidate.reject(delivery_id, dispatcher, lease_token, fence, reason, now_ms)?;
        self.delivery_ledger = candidate;
        Ok(record)
    }

    pub fn redrive_delivery(
        &mut self,
        delivery_id: &str,
        now_ms: u64,
    ) -> EpochResult<DeliveryRecord> {
        let mut candidate = self.delivery_ledger.clone();
        let record = candidate.redrive(delivery_id, now_ms)?;
        self.delivery_ledger = candidate;
        Ok(record)
    }

    pub fn maintain_deliveries(
        &mut self,
        now_ms: u64,
        max_deliveries: usize,
    ) -> EpochResult<DeliveryMaintenanceResult> {
        let mut candidate = self.delivery_ledger.clone();
        let result = candidate.maintain(now_ms, max_deliveries)?;
        self.delivery_ledger = candidate;
        Ok(result)
    }

    /// Returns the earliest in-flight delivery lease that requires a
    /// committed maintenance transition.
    pub fn next_delivery_maintenance_deadline_ms(&self) -> Option<u64> {
        self.delivery_ledger.next_maintenance_deadline_ms()
    }

    pub fn signed_webhook_delivery_candidates(
        &self,
        now_ms: u64,
    ) -> EpochResult<Vec<SignedWebhookDeliveryCandidate>> {
        self.delivery_ledger.signed_webhook_candidates(now_ms)
    }

    pub fn epoch_target_delivery_candidates(
        &self,
        now_ms: u64,
    ) -> EpochResult<Vec<EpochTargetDeliveryCandidate>> {
        self.delivery_ledger.epoch_target_candidates(now_ms)
    }

    pub fn managed_target_delivery_candidates(
        &self,
        now_ms: u64,
    ) -> EpochResult<Vec<ManagedTargetDeliveryCandidate>> {
        self.delivery_ledger.managed_target_candidates(now_ms)
    }

    pub fn delivery(&self, delivery_id: &str) -> Option<DeliveryRecord> {
        self.delivery_ledger.get(delivery_id)
    }

    pub fn deliveries(
        &self,
        subscription: Option<&str>,
        state: Option<DeliveryStateKind>,
        limit: usize,
    ) -> EpochResult<Vec<DeliveryRecord>> {
        self.delivery_ledger.query(subscription, state, limit)
    }

    pub fn delivery_counts(&self) -> DeliveryCounts {
        self.delivery_ledger.counts()
    }

    pub fn integration(&self) -> &EventIntegrationState {
        &self.integration
    }

    pub fn integration_mut(&mut self) -> &mut EventIntegrationState {
        &mut self.integration
    }

    pub fn apply_integration(
        &mut self,
        operation: IntegrationOperation,
        applied_at_ms: u64,
    ) -> EpochResult<IntegrationOutcome> {
        let next_version = self
            .route_plan_version
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("route plan version overflow".into()))?;
        let mut candidate = self.integration.clone();
        let outcome = candidate.apply(operation, applied_at_ms)?;
        self.integration = candidate;
        self.route_plan_version = next_version;
        Ok(outcome)
    }

    /// Deterministic digest of all state required to rebuild route and replay behavior.
    pub fn recovery_state_digest(&self) -> EpochResult<[u8; 32]> {
        let mut hasher = Sha256::new();
        let encoded = if self.archive_retention_is_active() {
            hasher.update(b"epoch/event-bus/recovery-state/v4\0");
            serde_json::to_vec(&(
                &self.config,
                &self.subscriptions,
                self.route_plan_version,
                self.commit_position,
                &self.archive,
                self.archive_retention_watermark_ms,
                &self.delivery_ledger,
                &self.integration,
            ))
        } else if self.integration.is_empty() {
            hasher.update(b"epoch/event-bus/recovery-state/v2\0");
            serde_json::to_vec(&(
                &self.config,
                &self.subscriptions,
                self.route_plan_version,
                self.commit_position,
                &self.archive,
                &self.delivery_ledger,
            ))
        } else {
            hasher.update(b"epoch/event-bus/recovery-state/v3\0");
            serde_json::to_vec(&(
                &self.config,
                &self.subscriptions,
                self.route_plan_version,
                self.commit_position,
                &self.archive,
                &self.delivery_ledger,
                &self.integration,
            ))
        }
        .map_err(|error| EpochError::Internal(error.to_string()))?;
        hasher.update(encoded);
        Ok(hasher.finalize().into())
    }

    /// Encodes the complete routing, archive, and delivery-ledger state as a
    /// canonical versioned application snapshot.
    pub fn encode_snapshot(&self) -> EpochResult<Vec<u8>> {
        let format_version = if self.archive_retention_is_active() {
            EVENT_BUS_SNAPSHOT_FORMAT_VERSION
        } else if !self.integration.is_empty() {
            INTEGRATION_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
        } else if self.uses_epoch_target_format() {
            EPOCH_TARGET_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
        } else if self.uses_signed_webhook_format() {
            SIGNED_WEBHOOK_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
        } else {
            LEGACY_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
        };
        let encoded = serde_json::to_vec(&VersionedEventBusSnapshot {
            format_version,
            config: self.config.clone(),
            subscriptions: self.subscriptions.clone(),
            route_plan_version: self.route_plan_version,
            commit_position: self.commit_position,
            archive: self.archive.clone(),
            archive_retention_watermark_ms: self.archive_retention_watermark_ms,
            delivery_ledger: self.delivery_ledger.clone(),
            integration: self.integration.clone(),
            state_digest: self.recovery_state_digest()?,
        })
        .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
        if encoded.len() > MAX_EVENT_BUS_SNAPSHOT_BYTES {
            return Err(EpochError::Capacity(format!(
                "Event Bus snapshot is {} bytes; maximum is {MAX_EVENT_BUS_SNAPSHOT_BYTES}",
                encoded.len()
            )));
        }
        Ok(encoded)
    }

    /// Decodes and validates a canonical Event Bus snapshot without mutating
    /// an existing bus.
    pub fn decode_snapshot(encoded: &[u8]) -> EpochResult<Self> {
        if encoded.len() > MAX_EVENT_BUS_SNAPSHOT_BYTES {
            return Err(EpochError::Capacity(format!(
                "Event Bus snapshot is {} bytes; maximum is {MAX_EVENT_BUS_SNAPSHOT_BYTES}",
                encoded.len()
            )));
        }
        let snapshot: VersionedEventBusSnapshot = serde_json::from_slice(encoded)
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
        validate_snapshot_document(&snapshot, encoded)?;
        let bus = Self {
            config: snapshot.config,
            subscriptions: snapshot.subscriptions,
            route_plan_version: snapshot.route_plan_version,
            commit_position: snapshot.commit_position,
            archive: snapshot.archive,
            archive_retention_watermark_ms: snapshot.archive_retention_watermark_ms,
            delivery_ledger: snapshot.delivery_ledger,
            integration: snapshot.integration,
        };
        if bus.recovery_state_digest()? != snapshot.state_digest {
            return Err(EpochError::InvalidArgument(
                "Event Bus snapshot state digest is invalid".into(),
            ));
        }
        Ok(bus)
    }

    pub fn has_subscription(&self, name: &str) -> bool {
        self.subscriptions.contains_key(name)
    }

    fn uses_signed_webhook_format(&self) -> bool {
        self.subscriptions
            .values()
            .any(|subscription| subscription.target.signing_key_id().is_some())
            || self.delivery_ledger.has_signed_webhook_targets()
    }

    fn uses_epoch_target_format(&self) -> bool {
        self.delivery_ledger.has_epoch_target_bindings()
    }

    fn archive_retention_is_active(&self) -> bool {
        !self.config.archive_retention.is_default() || self.archive_retention_watermark_ms.is_some()
    }
}

fn validate_snapshot_document(
    snapshot: &VersionedEventBusSnapshot,
    encoded: &[u8],
) -> EpochResult<()> {
    if !matches!(
        snapshot.format_version,
        LEGACY_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
            | SIGNED_WEBHOOK_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
            | EPOCH_TARGET_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
            | INTEGRATION_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
            | EVENT_BUS_SNAPSHOT_FORMAT_VERSION
    ) {
        return Err(EpochError::InvalidArgument(format!(
            "unsupported Event Bus snapshot version {}",
            snapshot.format_version
        )));
    }
    if serde_json::to_vec(snapshot)
        .map_err(|error| EpochError::InvalidArgument(error.to_string()))?
        != encoded
    {
        return Err(EpochError::InvalidArgument(
            "Event Bus snapshot is not canonical".into(),
        ));
    }
    validate_config(&snapshot.config)?;
    if snapshot.route_plan_version == 0
        || snapshot.subscriptions.len() > snapshot.config.max_subscriptions
    {
        return Err(EpochError::InvalidArgument(
            "Event Bus snapshot route plan is invalid".into(),
        ));
    }
    for (name, subscription) in &snapshot.subscriptions {
        if name != &subscription.name {
            return Err(EpochError::InvalidArgument(
                "Event Bus snapshot subscription registry is invalid".into(),
            ));
        }
        subscription.validate()?;
    }
    let retention_active = !snapshot.config.archive_retention.is_default()
        || snapshot.archive_retention_watermark_ms.is_some();
    if (snapshot.config.archive
        && !retention_active
        && u64::try_from(snapshot.archive.len()).ok() != Some(snapshot.commit_position))
        || (!snapshot.config.archive
            && (!snapshot.archive.is_empty() || snapshot.archive_retention_watermark_ms.is_some()))
        || snapshot.archive.len() > snapshot.config.max_archive_events
        || snapshot.archive.len() > usize::try_from(snapshot.commit_position).unwrap_or(usize::MAX)
        || (snapshot.config.archive_retention.max_age_ms.is_none()
            && snapshot.archive_retention_watermark_ms.is_some())
    {
        return Err(EpochError::InvalidArgument(
            "Event Bus snapshot archive configuration is invalid".into(),
        ));
    }
    let first_retained_position = snapshot
        .commit_position
        .checked_sub(u64::try_from(snapshot.archive.len()).map_err(|_| {
            EpochError::InvalidArgument("Event Bus snapshot archive length is invalid".into())
        })?)
        .and_then(|position| position.checked_add(1));
    for (index, event) in snapshot.archive.iter().enumerate() {
        let expected_position = first_retained_position
            .and_then(|position| position.checked_add(u64::try_from(index).ok()?));
        if expected_position != Some(event.position)
            || event.route_plan_version == 0
            || event.route_plan_version > snapshot.route_plan_version
            || snapshot
                .archive_retention_watermark_ms
                .is_some_and(|watermark| event.received_at_ms > watermark)
        {
            return Err(EpochError::InvalidArgument(
                "Event Bus snapshot archive position is invalid".into(),
            ));
        }
        event.envelope.validate()?;
    }
    snapshot.delivery_ledger.validate_snapshot(
        snapshot.config.delivery_outbox,
        snapshot.config.max_outbox_deliveries,
        snapshot.commit_position,
        snapshot.route_plan_version,
    )?;
    snapshot.integration.validate_snapshot()?;
    validate_snapshot_format_for_targets(snapshot)
}

fn validate_snapshot_format_for_targets(snapshot: &VersionedEventBusSnapshot) -> EpochResult<()> {
    let uses_signed_webhook_format = snapshot
        .subscriptions
        .values()
        .any(|subscription| subscription.target.signing_key_id().is_some())
        || snapshot.delivery_ledger.has_signed_webhook_targets();
    let expected_format_version = if !snapshot.config.archive_retention.is_default()
        || snapshot.archive_retention_watermark_ms.is_some()
    {
        EVENT_BUS_SNAPSHOT_FORMAT_VERSION
    } else if !snapshot.integration.is_empty() {
        INTEGRATION_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
    } else if snapshot.delivery_ledger.has_epoch_target_bindings() {
        EPOCH_TARGET_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
    } else if uses_signed_webhook_format {
        SIGNED_WEBHOOK_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
    } else {
        LEGACY_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
    };
    if snapshot.format_version != expected_format_version {
        return Err(EpochError::InvalidArgument(format!(
            "Event Bus snapshot version {} does not match its target metadata",
            snapshot.format_version
        )));
    }
    Ok(())
}

fn validate_config(config: &BusConfig) -> EpochResult<()> {
    validate_capacity(
        "max_subscriptions",
        config.max_subscriptions,
        MAX_BUS_SUBSCRIPTIONS,
    )?;
    validate_capacity(
        "max_archive_events",
        config.max_archive_events,
        MAX_BUS_ARCHIVE_EVENTS,
    )?;
    validate_capacity(
        "max_outbox_deliveries",
        config.max_outbox_deliveries,
        MAX_BUS_OUTBOX_DELIVERIES,
    )?;
    if config
        .archive_retention
        .max_events
        .is_some_and(|limit| limit == 0 || limit > config.max_archive_events)
    {
        return Err(EpochError::InvalidArgument(format!(
            "archive retention max_events must be between 1 and {}",
            config.max_archive_events
        )));
    }
    if config
        .archive_retention
        .max_age_ms
        .is_some_and(|limit| limit == 0 || limit > MAX_ARCHIVE_RETENTION_MS)
    {
        return Err(EpochError::InvalidArgument(format!(
            "archive retention max_age_ms must be between 1 and {MAX_ARCHIVE_RETENTION_MS}"
        )));
    }
    if !config.archive && !config.archive_retention.is_default() {
        return Err(EpochError::InvalidArgument(
            "archive retention requires archive=true".into(),
        ));
    }
    Ok(())
}

fn effective_archive_time(
    policy: ArchiveRetentionPolicy,
    watermark_ms: Option<u64>,
    now_ms: u64,
) -> u64 {
    if policy.max_age_ms.is_some() {
        watermark_ms.unwrap_or(now_ms).max(now_ms)
    } else {
        now_ms
    }
}

fn maintain_archive_records(
    archive: &mut Vec<ArchivedEvent>,
    policy: ArchiveRetentionPolicy,
    watermark_ms: &mut Option<u64>,
    now_ms: u64,
    max_removals: usize,
) -> ArchiveMaintenanceResult {
    let cutoff_ms = policy
        .max_age_ms
        .and_then(|max_age_ms| now_ms.checked_sub(max_age_ms));
    if policy.max_age_ms.is_some() {
        *watermark_ms = Some(watermark_ms.unwrap_or(now_ms).max(now_ms));
    }
    let age_removals = cutoff_ms.map_or(0, |cutoff_ms| {
        archive
            .iter()
            .take_while(|record| record.received_at_ms <= cutoff_ms)
            .count()
    });
    let count_removals = policy
        .max_events
        .map_or(0, |limit| archive.len().saturating_sub(limit));
    let purged = age_removals.max(count_removals).min(max_removals);
    if purged > 0 {
        archive.drain(..purged);
    }
    ArchiveMaintenanceResult {
        as_of_ms: now_ms,
        cutoff_ms,
        purged,
        archived_event_count: archive.len(),
    }
}

fn validate_capacity(field: &str, value: usize, maximum: usize) -> EpochResult<()> {
    if value == 0 || value > maximum {
        return Err(EpochError::InvalidArgument(format!(
            "{field} must be between 1 and {maximum}"
        )));
    }
    Ok(())
}

fn validate_patterns(field: &str, patterns: &[String]) -> EpochResult<()> {
    if patterns.len() > MAX_FILTER_PATTERNS {
        return Err(EpochError::InvalidArgument(format!(
            "{field} has {} entries; maximum is {MAX_FILTER_PATTERNS}",
            patterns.len()
        )));
    }
    for pattern in patterns {
        validate_text("filter pattern", pattern, MAX_PATTERN_BYTES)?;
    }
    Ok(())
}

fn validate_map_capacity(field: &str, length: usize, maximum: usize) -> EpochResult<()> {
    if length > maximum {
        return Err(EpochError::InvalidArgument(format!(
            "{field} has {length} entries; maximum is {maximum}"
        )));
    }
    Ok(())
}

fn validate_text(field: &str, value: &str, maximum: usize) -> EpochResult<()> {
    if value.is_empty() {
        return Err(EpochError::InvalidArgument(format!("{field} is required")));
    }
    if value.len() > maximum {
        return Err(EpochError::InvalidArgument(format!(
            "{field} is {} bytes; maximum is {maximum}",
            value.len()
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(EpochError::InvalidArgument(format!(
            "{field} cannot contain control characters"
        )));
    }
    Ok(())
}

fn validate_json_path(path: &str) -> EpochResult<()> {
    validate_text("JSON path", path, MAX_JSON_PATH_BYTES)?;
    let normalized = path.strip_prefix("$.").unwrap_or(path);
    if normalized == "$" {
        return Ok(());
    }
    if normalized.is_empty() || normalized.split('.').any(str::is_empty) {
        return Err(EpochError::InvalidArgument(format!(
            "invalid JSON path: {path}"
        )));
    }
    Ok(())
}

fn validate_output_field(field: &str) -> EpochResult<()> {
    validate_text("transform output field", field, MAX_PROJECTED_FIELD_BYTES)?;
    if field.contains('.') {
        return Err(EpochError::InvalidArgument(
            "transform output fields cannot contain dots".into(),
        ));
    }
    Ok(())
}

fn validate_transform_value(value: &Value, maximum: usize) -> EpochResult<()> {
    let encoded = serde_json::to_vec(value)
        .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
    if encoded.len() > maximum {
        return Err(EpochError::InvalidArgument(format!(
            "transform value is {} bytes; configured maximum is {maximum}",
            encoded.len()
        )));
    }
    Ok(())
}

fn validate_template(template: &str) -> EpochResult<()> {
    let mut remainder = template;
    let mut placeholders = 0_u16;
    while let Some(start) = remainder.find("{{") {
        let after_start = &remainder[start + 2..];
        let Some(end) = after_start.find("}}") else {
            return Err(EpochError::InvalidArgument(
                "transform template has an unclosed placeholder".into(),
            ));
        };
        let path = &after_start[..end];
        validate_json_path(path)?;
        placeholders = placeholders
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("transform placeholder overflow".into()))?;
        if placeholders > MAX_TRANSFORM_OPERATIONS {
            return Err(EpochError::InvalidArgument(
                "transform template has too many placeholders".into(),
            ));
        }
        remainder = &after_start[end + 2..];
    }
    if remainder.contains("}}") {
        return Err(EpochError::InvalidArgument(
            "transform template has an unmatched closing delimiter".into(),
        ));
    }
    Ok(())
}

fn render_template(template: &str, payload: &Value) -> EpochResult<String> {
    let mut output = String::new();
    let mut remainder = template;
    while let Some(start) = remainder.find("{{") {
        output.push_str(&remainder[..start]);
        let after_start = &remainder[start + 2..];
        let end = after_start.find("}}").ok_or_else(|| {
            EpochError::InvalidArgument("transform template has an unclosed placeholder".into())
        })?;
        let path = &after_start[..end];
        let value = json_path(payload, path).ok_or_else(|| {
            EpochError::InvalidArgument(format!("transform template path {path} does not exist"))
        })?;
        match value {
            Value::String(value) => output.push_str(value),
            Value::Null | Value::Bool(_) | Value::Number(_) => output.push_str(&value.to_string()),
            Value::Array(_) | Value::Object(_) => {
                return Err(EpochError::InvalidArgument(format!(
                    "transform template path {path} must resolve to a scalar"
                )));
            }
        }
        remainder = &after_start[end + 2..];
    }
    output.push_str(remainder);
    Ok(output)
}

fn validate_http_target(value: &str) -> EpochResult<()> {
    validate_text("HTTP target URL", value, MAX_TARGET_URL_BYTES)?;
    let parsed = Url::parse(value).map_err(|error| {
        EpochError::InvalidArgument(format!("invalid HTTP target URL: {error}"))
    })?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(EpochError::InvalidArgument(
            "HTTP targets require an absolute http or https URL".into(),
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(EpochError::InvalidArgument(
            "HTTP target URLs cannot contain credentials".into(),
        ));
    }
    if parsed.fragment().is_some() {
        return Err(EpochError::InvalidArgument(
            "HTTP target URLs cannot contain fragments".into(),
        ));
    }
    Ok(())
}

fn validate_destination_auth(auth: &DestinationAuth) -> EpochResult<()> {
    match auth {
        DestinationAuth::None => Ok(()),
        DestinationAuth::ApiKey { secret_ref, header } => {
            validate_resource_name(secret_ref)?;
            validate_text("API key header", header, MAX_HEADER_KEY_BYTES)
        }
        DestinationAuth::OAuth2 {
            secret_ref,
            token_url,
            scopes,
        } => {
            validate_resource_name(secret_ref)?;
            validate_http_target(token_url)?;
            if scopes.len() > MAX_FILTER_ENTRIES {
                return Err(EpochError::InvalidArgument(format!(
                    "OAuth scopes have {} entries; maximum is {MAX_FILTER_ENTRIES}",
                    scopes.len()
                )));
            }
            for scope in scopes {
                validate_text("OAuth scope", scope, MAX_HEADER_VALUE_BYTES)?;
            }
            Ok(())
        }
    }
}

fn json_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let path = path.strip_prefix("$.").unwrap_or(path);
    if path.is_empty() || path == "$" {
        return Some(value);
    }
    path.split('.').try_fold(value, |current, segment| {
        current.as_object().and_then(|object| object.get(segment))
    })
}

fn event_topic(event: &EventEnvelope) -> &str {
    event
        .extensions
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or(&event.event_type)
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let value: Vec<char> = value.chars().collect();
    let (mut pattern_index, mut value_index) = (0, 0);
    let (mut star, mut checkpoint) = (None, 0);
    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == '?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == '*' {
            star = Some(pattern_index);
            pattern_index += 1;
            checkpoint = value_index;
        } else if let Some(star_index) = star {
            pattern_index = star_index + 1;
            checkpoint += 1;
            value_index = checkpoint;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == '*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn event(event_type: &str) -> EventEnvelope {
        let mut event = EventEnvelope::new(
            "checkout",
            event_type,
            json!({"order": {"total": 42}, "private": "remove"}),
            0,
        );
        event.headers.insert("tenant".into(), "acme".into());
        event
    }

    #[test]
    fn filters_and_fanout_are_deterministic() {
        let mut bus = EventBus::new(BusConfig::default()).unwrap();
        for name in ["worker", "audit"] {
            bus.upsert_subscription(Subscription {
                name: name.into(),
                filter: EventFilter {
                    event_type_patterns: vec!["order.*".into()],
                    headers: BTreeMap::from([("tenant".into(), "acme".into())]),
                    json_equals: BTreeMap::from([("order.total".into(), json!(42))]),
                    ..EventFilter::default()
                },
                target: SubscriptionTarget::Pull,
                transform: EventTransform::default(),
                delivery_policy: DeliveryPolicy::default(),
            })
            .unwrap();
        }
        let result = bus.publish(event("order.created"), 10).unwrap();
        assert_eq!(result.deliveries.len(), 2);
        assert_eq!(result.deliveries[0].subscription, "audit");
        assert_eq!(result.deliveries[1].subscription, "worker");
    }

    #[test]
    fn transformation_projects_payload_and_adds_headers() {
        let mut bus = EventBus::new(BusConfig::default()).unwrap();
        bus.upsert_subscription(Subscription {
            name: "worker".into(),
            filter: EventFilter::default(),
            target: SubscriptionTarget::Queue {
                resource: "orders".into(),
            },
            transform: EventTransform {
                add_headers: BTreeMap::from([("routed-by".into(), "epoch".into())]),
                payload_projection: BTreeMap::from([("total".into(), "order.total".into())]),
                ..EventTransform::default()
            },
            delivery_policy: DeliveryPolicy::default(),
        })
        .unwrap();
        let routed = bus
            .publish(event("order.created"), 1)
            .unwrap()
            .deliveries
            .remove(0);
        assert_eq!(routed.envelope.payload, json!({"total": 42}));
        assert_eq!(routed.envelope.headers["routed-by"], "epoch");
    }

    #[test]
    fn archive_replay_applies_time_and_filter() {
        let mut bus = EventBus::new(BusConfig::default()).unwrap();
        bus.publish(event("order.created"), 10).unwrap();
        bus.publish(event("order.cancelled"), 20).unwrap();
        let filter = EventFilter {
            event_type_patterns: vec!["*.cancelled".into()],
            ..EventFilter::default()
        };
        let replay = bus.replay(0, 100, Some(&filter), 10).unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].envelope.event_type, "order.cancelled");
    }

    #[test]
    fn archive_retention_is_bounded_monotonic_and_snapshot_recoverable() {
        let mut bus = EventBus::new(BusConfig {
            max_archive_events: 3,
            archive_retention: ArchiveRetentionPolicy {
                max_events: Some(2),
                max_age_ms: Some(10),
            },
            ..BusConfig::default()
        })
        .unwrap();
        bus.publish(event("order.one"), 100).unwrap();
        bus.publish(event("order.two"), 105).unwrap();
        bus.publish(event("order.three"), 109).unwrap();

        let retained = bus.replay(0, u64::MAX, None, 10).unwrap();
        assert_eq!(
            retained
                .iter()
                .map(|record| record.position)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(bus.next_archive_retention_deadline_ms(), Some(115));

        let early = bus.maintain_archive(114, 10).unwrap();
        assert_eq!(early.purged, 0);
        let due = bus.maintain_archive(115, 1).unwrap();
        assert_eq!(due.purged, 1);
        assert_eq!(due.cutoff_ms, Some(105));
        assert_eq!(bus.archive_retention_watermark_ms(), Some(115));

        bus.publish(event("order.four"), 110).unwrap();
        let retained = bus.replay(0, u64::MAX, None, 10).unwrap();
        assert_eq!(
            retained
                .iter()
                .map(|record| record.position)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert_eq!(retained[1].received_at_ms, 115);

        let snapshot = bus.encode_snapshot().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&snapshot).unwrap()["format_version"],
            EVENT_BUS_SNAPSHOT_FORMAT_VERSION
        );
        let restored = EventBus::decode_snapshot(&snapshot).unwrap();
        assert_eq!(
            restored.recovery_state_digest().unwrap(),
            bus.recovery_state_digest().unwrap()
        );
        assert_eq!(restored.archive_retention_watermark_ms(), Some(115));
    }

    #[test]
    fn glob_matching_handles_prefix_suffix_and_question_mark() {
        assert!(glob_matches("order.*", "order.created"));
        assert!(glob_matches("*.created", "order.created"));
        assert!(glob_matches("order.???????", "order.created"));
        assert!(glob_matches("order.?", "order.✓"));
        assert!(!glob_matches("payment.*", "order.created"));
    }

    #[test]
    fn legacy_configuration_defaults_bounds_and_state_digest_is_insertion_independent() {
        let config: BusConfig =
            serde_json::from_str(r#"{"durability":"volatile","archive":true}"#).unwrap();
        assert_eq!(config.max_subscriptions, DEFAULT_MAX_SUBSCRIPTIONS);
        assert_eq!(config.max_archive_events, DEFAULT_MAX_ARCHIVE_EVENTS);
        assert!(!config.delivery_outbox);
        assert_eq!(config.max_outbox_deliveries, DEFAULT_MAX_OUTBOX_DELIVERIES);

        let legacy_subscription: Subscription = serde_json::from_value(json!({
            "name": "audit",
            "filter": {},
            "target": {"kind": "pull"},
            "transform": {}
        }))
        .unwrap();
        assert_eq!(
            legacy_subscription.delivery_policy,
            DeliveryPolicy::default()
        );
        assert!(
            serde_json::to_value(&legacy_subscription)
                .unwrap()
                .get("delivery_policy")
                .is_none()
        );

        let mut first = EventBus::new(config.clone()).unwrap();
        let mut second = EventBus::new(config).unwrap();
        for name in ["worker", "audit"] {
            first
                .upsert_subscription(subscription(name, SubscriptionTarget::Pull))
                .unwrap();
        }
        for name in ["audit", "worker"] {
            second
                .upsert_subscription(subscription(name, SubscriptionTarget::Pull))
                .unwrap();
        }
        assert_eq!(
            first.recovery_state_digest().unwrap(),
            second.recovery_state_digest().unwrap()
        );
    }

    #[test]
    fn route_truth_table_requires_every_filter_dimension() {
        let filter = EventFilter {
            topic_patterns: Vec::new(),
            event_type_patterns: vec!["order.*".into(), "refund.*".into()],
            source_patterns: vec!["check?ut".into()],
            subject_patterns: vec!["tenant-*".into()],
            headers: BTreeMap::from([("tenant".into(), "acme".into())]),
            json_equals: BTreeMap::from([("$.order.total".into(), json!(42))]),
        };
        let mut candidate = event("order.created");
        candidate.subject = Some("tenant-primary".into());

        assert!(filter.matches(&candidate));

        let mut cases = Vec::new();
        let mut wrong_type = candidate.clone();
        wrong_type.event_type = "payment.created".into();
        cases.push(wrong_type);
        let mut wrong_source = candidate.clone();
        wrong_source.source = "warehouse".into();
        cases.push(wrong_source);
        let mut missing_subject = candidate.clone();
        missing_subject.subject = None;
        cases.push(missing_subject);
        let mut wrong_header = candidate.clone();
        wrong_header.headers.insert("tenant".into(), "other".into());
        cases.push(wrong_header);
        let mut wrong_payload = candidate;
        wrong_payload.payload = json!({"order": {"total": 43}});
        cases.push(wrong_payload);

        assert!(cases.iter().all(|candidate| !filter.matches(candidate)));
    }

    #[test]
    fn configuration_and_subscription_capacities_fail_without_mutation() {
        let invalid = BusConfig {
            max_subscriptions: 0,
            ..BusConfig::default()
        };
        assert!(matches!(
            EventBus::new(invalid),
            Err(EpochError::InvalidArgument(_))
        ));

        let mut bus = EventBus::new(BusConfig {
            max_subscriptions: 1,
            ..BusConfig::default()
        })
        .unwrap();
        let version = bus
            .upsert_subscription(subscription("one", SubscriptionTarget::Pull))
            .unwrap();
        assert_eq!(version, 2);
        assert!(matches!(
            bus.upsert_subscription(subscription("two", SubscriptionTarget::Pull)),
            Err(EpochError::Capacity(_))
        ));
        assert_eq!(bus.subscription_count(), 1);
        assert_eq!(bus.route_plan_version(), 2);
    }

    #[test]
    fn replacement_is_allowed_at_capacity_but_version_overflow_is_atomic() {
        let mut bus = EventBus::new(BusConfig {
            max_subscriptions: 1,
            ..BusConfig::default()
        })
        .unwrap();
        bus.upsert_subscription(subscription("worker", SubscriptionTarget::Pull))
            .unwrap();
        bus.upsert_subscription(subscription(
            "worker",
            SubscriptionTarget::Queue {
                resource: "orders".into(),
            },
        ))
        .unwrap();
        assert_eq!(bus.subscription_count(), 1);

        bus.route_plan_version = u64::MAX;
        let before = bus.subscriptions.clone();
        assert!(matches!(
            bus.upsert_subscription(subscription("worker", SubscriptionTarget::Pull)),
            Err(EpochError::Capacity(_))
        ));
        assert_eq!(bus.subscriptions, before);
        assert!(matches!(
            bus.remove_subscription("worker"),
            Err(EpochError::Capacity(_))
        ));
        assert_eq!(bus.subscriptions, before);
    }

    #[test]
    fn archive_capacity_and_position_overflow_are_atomic() {
        let mut bus = EventBus::new(BusConfig {
            max_archive_events: 1,
            ..BusConfig::default()
        })
        .unwrap();
        bus.publish(event("order.created"), 10).unwrap();
        assert!(matches!(
            bus.publish(event("order.cancelled"), 20),
            Err(EpochError::Capacity(_))
        ));
        assert_eq!(bus.commit_position(), 1);
        assert_eq!(bus.archived_event_count(), 1);

        let mut unarchived = EventBus::new(BusConfig {
            archive: false,
            ..BusConfig::default()
        })
        .unwrap();
        unarchived.commit_position = u64::MAX;
        assert!(matches!(
            unarchived.publish(event("order.created"), 30),
            Err(EpochError::Capacity(_))
        ));
        assert_eq!(unarchived.commit_position(), u64::MAX);
    }

    #[test]
    fn target_and_filter_validation_rejects_ambiguous_routes() {
        let mut bus = EventBus::new(BusConfig::default()).unwrap();
        for target in [
            SubscriptionTarget::Queue {
                resource: "bad/name".into(),
            },
            SubscriptionTarget::Webhook {
                url: "https://user:secret@example.com/hook".into(),
                signing_key_id: None,
            },
            SubscriptionTarget::Http {
                url: "file:///tmp/hook".into(),
                signing_key_id: None,
            },
        ] {
            assert!(matches!(
                bus.upsert_subscription(subscription("worker", target)),
                Err(EpochError::InvalidArgument(_))
            ));
        }

        let mut invalid_pattern = subscription("worker", SubscriptionTarget::Pull);
        invalid_pattern.filter.event_type_patterns = vec![String::new()];
        assert!(matches!(
            bus.upsert_subscription(invalid_pattern),
            Err(EpochError::InvalidArgument(_))
        ));
        assert_eq!(bus.subscription_count(), 0);
        assert_eq!(bus.route_plan_version(), 1);
    }

    #[test]
    fn target_json_rejects_unknown_fields_for_every_variant() {
        assert!(
            serde_json::from_value::<SubscriptionTarget>(json!({
                "kind": "pull",
                "url": "https://example.com"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<SubscriptionTarget>(json!({
                "kind": "queue",
                "resource": "orders",
                "unexpected": true
            }))
            .is_err()
        );
        for target in [
            SubscriptionTarget::Pull,
            SubscriptionTarget::Queue {
                resource: "orders".into(),
            },
            SubscriptionTarget::Stream {
                resource: "audit".into(),
            },
            SubscriptionTarget::Webhook {
                url: "https://example.com/hook".into(),
                signing_key_id: None,
            },
            SubscriptionTarget::Http {
                url: "https://example.com/events".into(),
                signing_key_id: None,
            },
        ] {
            let encoded = serde_json::to_value(&target).unwrap();
            assert_eq!(
                serde_json::from_value::<SubscriptionTarget>(encoded).unwrap(),
                target
            );
        }
    }

    #[test]
    fn replay_limit_is_bounded() {
        let bus = EventBus::new(BusConfig::default()).unwrap();
        assert!(matches!(
            bus.replay(0, 1, None, MAX_REPLAY_EVENTS + 1),
            Err(EpochError::InvalidArgument(_))
        ));
    }

    #[test]
    fn native_snapshot_round_trips_routes_archive_and_delivery_ledger_then_continues() {
        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        bus.upsert_subscription(subscription("audit", SubscriptionTarget::Pull))
            .unwrap();
        bus.publish(event("order.created"), 100).unwrap();
        let fence = DeliveryFence::new(7, 3, 2, 1).unwrap();
        let lease = bus
            .acquire_deliveries("audit", "dispatcher", 1, 100, fence)
            .unwrap()
            .remove(0);
        let expected_digest = bus.recovery_state_digest().unwrap();

        let encoded = bus.encode_snapshot().unwrap();
        let mut restored = EventBus::decode_snapshot(&encoded).unwrap();

        assert_eq!(restored.encode_snapshot().unwrap(), encoded);
        assert_eq!(restored.recovery_state_digest().unwrap(), expected_digest);
        assert_eq!(restored.subscription_count(), 1);
        assert_eq!(restored.commit_position(), 1);
        assert_eq!(restored.archived_event_count(), 1);
        assert_eq!(restored.delivery_counts().in_flight, 1);
        restored
            .acknowledge_delivery(
                &lease.delivery_id,
                "dispatcher",
                &lease.lease_token,
                fence,
                101,
            )
            .unwrap();
        assert_eq!(restored.delivery_counts().acknowledged, 1);
    }

    #[test]
    fn snapshot_version_tracks_signed_target_metadata_without_rewriting_legacy_images() {
        let legacy = EventBus::new(BusConfig::default()).unwrap();
        let legacy_snapshot: VersionedEventBusSnapshot =
            serde_json::from_slice(&legacy.encode_snapshot().unwrap()).unwrap();
        assert_eq!(legacy_snapshot.format_version, 1);

        let mut signed = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        signed
            .upsert_subscription(subscription(
                "orders",
                SubscriptionTarget::Webhook {
                    url: "https://example.com/orders".into(),
                    signing_key_id: Some("primary".into()),
                },
            ))
            .unwrap();
        signed.publish(event("order.created"), 100).unwrap();
        let encoded = signed.encode_snapshot().unwrap();
        let snapshot: VersionedEventBusSnapshot = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            snapshot.format_version,
            SIGNED_WEBHOOK_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
        );
        let restored = EventBus::decode_snapshot(&encoded).unwrap();
        assert_eq!(restored.encode_snapshot().unwrap(), encoded);
        assert_eq!(
            restored.recovery_state_digest().unwrap(),
            signed.recovery_state_digest().unwrap()
        );

        let mut mislabeled = snapshot;
        mislabeled.format_version = LEGACY_EVENT_BUS_SNAPSHOT_FORMAT_VERSION;
        assert!(EventBus::decode_snapshot(&serde_json::to_vec(&mislabeled).unwrap()).is_err());
    }

    #[test]
    fn native_snapshot_rejects_noncanonical_unknown_or_corrupt_images() {
        let mut bus = EventBus::new(BusConfig::default()).unwrap();
        bus.publish(event("order.created"), 100).unwrap();
        let encoded = bus.encode_snapshot().unwrap();
        let snapshot: VersionedEventBusSnapshot = serde_json::from_slice(&encoded).unwrap();
        assert!(EventBus::decode_snapshot(&serde_json::to_vec_pretty(&snapshot).unwrap()).is_err());

        let mut unknown = snapshot.clone();
        unknown.format_version = 99;
        assert!(EventBus::decode_snapshot(&serde_json::to_vec(&unknown).unwrap()).is_err());

        let mut corrupt = snapshot;
        corrupt.state_digest[0] ^= 1;
        assert!(EventBus::decode_snapshot(&serde_json::to_vec(&corrupt).unwrap()).is_err());
    }

    #[test]
    fn native_snapshot_revalidates_integration_semantics_after_digest_verification() {
        let invalid_integration: EventIntegrationState = serde_json::from_value(json!({
            "functions": {
                "wrong-key": {
                    "definition": {
                        "name": "actual-name",
                        "endpoint": "https://example.com/invoke",
                        "identity": "runner",
                        "timeout_ms": 100,
                        "max_input_bytes": 1024,
                        "outbound_allowlist": ["example.com"]
                    },
                    "revision": 1,
                    "status": "active",
                    "updated_at_ms": 1
                }
            }
        }))
        .unwrap();
        let mut bus = EventBus::new(BusConfig::default()).unwrap();
        bus.integration = invalid_integration;
        let snapshot = VersionedEventBusSnapshot {
            format_version: INTEGRATION_EVENT_BUS_SNAPSHOT_FORMAT_VERSION,
            config: bus.config.clone(),
            subscriptions: bus.subscriptions.clone(),
            route_plan_version: bus.route_plan_version,
            commit_position: bus.commit_position,
            archive: bus.archive.clone(),
            archive_retention_watermark_ms: bus.archive_retention_watermark_ms,
            delivery_ledger: bus.delivery_ledger.clone(),
            integration: bus.integration.clone(),
            state_digest: bus.recovery_state_digest().unwrap(),
        };

        assert!(EventBus::decode_snapshot(&serde_json::to_vec(&snapshot).unwrap()).is_err());
    }

    #[test]
    fn publish_persists_a_bounded_lexical_delivery_outbox_atomically() {
        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            max_outbox_deliveries: 2,
            ..BusConfig::default()
        })
        .unwrap();
        for name in ["worker", "audit"] {
            bus.upsert_subscription(subscription(name, SubscriptionTarget::Pull))
                .unwrap();
        }

        let published = bus.publish(event("order.created"), 100).unwrap();
        assert_eq!(
            published
                .deliveries
                .iter()
                .map(|delivery| delivery.delivery_id.as_str())
                .collect::<Vec<_>>(),
            [
                "epoch.bus.delivery.v1.1.audit",
                "epoch.bus.delivery.v1.1.worker"
            ]
        );
        let records = bus.deliveries(None, None, 10).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].subscription, "audit");
        assert!(matches!(
            records[0].state,
            DeliveryState::Pending {
                eligible_at_ms: 100
            }
        ));

        let digest = bus.recovery_state_digest().unwrap();
        assert!(matches!(
            bus.publish(event("order.cancelled"), 101),
            Err(EpochError::Capacity(_))
        ));
        assert_eq!(bus.commit_position(), 1);
        assert_eq!(bus.archived_event_count(), 1);
        assert_eq!(bus.delivery_counts().pending, 2);
        assert_eq!(bus.recovery_state_digest().unwrap(), digest);
    }

    #[test]
    fn failed_delivery_retries_at_its_boundary_without_affecting_other_targets() {
        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        for name in ["beta", "alpha"] {
            let mut route = subscription(name, SubscriptionTarget::Pull);
            route.delivery_policy = DeliveryPolicy {
                timeout_ms: 10,
                max_in_flight: 1,
                retry: DeliveryRetryPolicy {
                    strategy: DeliveryBackoffStrategy::Fixed,
                    initial_delay_ms: 10,
                    max_delay_ms: 10,
                    jitter_percent: 0,
                    max_attempts: 2,
                    max_age_ms: None,
                },
                rate_limit: None,
                dead_letter_retention_ms: None,
            };
            bus.upsert_subscription(route).unwrap();
        }
        bus.publish(event("order.created"), 100).unwrap();
        let fence = DeliveryFence::new(7, 3, 2, 1).unwrap();

        let first = bus
            .acquire_deliveries("alpha", "dispatcher", 10, 100, fence)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].attempt, 1);
        let delivery_id = first[0].delivery_id.clone();
        let token = first[0].lease_token.clone();
        let failed = bus
            .fail_delivery(
                &delivery_id,
                "dispatcher",
                &token,
                fence,
                "target unavailable",
                101,
            )
            .unwrap();
        assert!(matches!(
            failed.state,
            DeliveryState::Pending {
                eligible_at_ms: 111
            }
        ));
        assert!(matches!(
            bus.delivery("epoch.bus.delivery.v1.1.beta").unwrap().state,
            DeliveryState::Pending {
                eligible_at_ms: 100
            }
        ));
        assert!(
            bus.acquire_deliveries("alpha", "dispatcher", 10, 110, fence)
                .unwrap()
                .is_empty()
        );

        let second = bus
            .acquire_deliveries("alpha", "dispatcher", 10, 111, fence)
            .unwrap();
        assert_eq!(second[0].attempt, 2);
        let dead_lettered = bus
            .fail_delivery(
                &delivery_id,
                "dispatcher",
                &second[0].lease_token,
                fence,
                "still unavailable",
                112,
            )
            .unwrap();
        assert!(matches!(
            dead_lettered.state,
            DeliveryState::DeadLettered {
                dead_lettered_at_ms: 112,
                ..
            }
        ));
        assert_eq!(dead_lettered.attempts.len(), 2);
        assert_eq!(bus.delivery_counts().dead_lettered, 1);
        assert_eq!(bus.delivery_counts().pending, 1);
    }

    #[test]
    fn signed_webhook_candidates_follow_exact_acquire_order_and_attempt() {
        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        bus.upsert_subscription(subscription("orders", SubscriptionTarget::Pull))
            .unwrap();
        bus.publish(event("order.created"), 100).unwrap();
        bus.upsert_subscription(subscription(
            "orders",
            SubscriptionTarget::Webhook {
                url: "https://example.com/orders".into(),
                signing_key_id: Some("primary".into()),
            },
        ))
        .unwrap();
        bus.publish(event("order.updated"), 101).unwrap();
        bus.upsert_subscription(subscription(
            "unsigned",
            SubscriptionTarget::Webhook {
                url: "https://example.com/unsigned".into(),
                signing_key_id: None,
            },
        ))
        .unwrap();
        bus.publish(event("order.updated"), 102).unwrap();

        // The built-in signed dispatcher must not skip the older pull record
        // and then acquire a target it did not inspect.
        assert!(
            bus.signed_webhook_delivery_candidates(102)
                .unwrap()
                .is_empty()
        );

        let fence = DeliveryFence::new(7, 3, 2, 1).unwrap();
        assert!(
            bus.acquire_specific_delivery(
                "orders",
                "epoch.bus.delivery.v1.2.orders",
                "dispatcher",
                102,
                fence,
            )
            .unwrap()
            .is_none()
        );
        let pull = bus
            .acquire_deliveries("orders", "dispatcher", 1, 102, fence)
            .unwrap()
            .remove(0);
        bus.acknowledge_delivery(
            &pull.delivery_id,
            "dispatcher",
            &pull.lease_token,
            fence,
            103,
        )
        .unwrap();

        let candidate = bus
            .signed_webhook_delivery_candidates(103)
            .unwrap()
            .remove(0);
        assert_eq!(candidate.delivery_id, "epoch.bus.delivery.v1.2.orders");
        assert_eq!(candidate.subscription, "orders");
        assert_eq!(candidate.next_attempt, 1);
        assert_eq!(candidate.signing_key_id, "primary");

        let lease = bus
            .acquire_specific_delivery("orders", &candidate.delivery_id, "dispatcher", 103, fence)
            .unwrap()
            .unwrap();
        bus.fail_delivery(
            &lease.delivery_id,
            "dispatcher",
            &lease.lease_token,
            fence,
            "retry",
            104,
        )
        .unwrap();
        let retried = bus
            .signed_webhook_delivery_candidates(u64::MAX)
            .unwrap()
            .remove(0);
        assert_eq!(retried.delivery_id, candidate.delivery_id);
        assert_eq!(retried.next_attempt, 2);
    }

    #[test]
    fn epoch_target_candidates_bind_one_exact_destination_across_retries() {
        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        bus.upsert_subscription(subscription(
            "orders",
            SubscriptionTarget::Queue {
                resource: "jobs".into(),
            },
        ))
        .unwrap();
        let mut published = event("order.created");
        published.id = "event-1".into();
        published.key = Some("customer-42".into());
        bus.publish(published, 100).unwrap();

        let candidate = bus.epoch_target_delivery_candidates(100).unwrap().remove(0);
        assert_eq!(candidate.delivery_id, "epoch.bus.delivery.v1.1.orders");
        assert_eq!(candidate.subscription, "orders");
        assert_eq!(candidate.next_attempt, 1);
        assert_eq!(candidate.partition_key, "customer-42");
        assert_eq!(
            candidate.target,
            SubscriptionTarget::Queue {
                resource: "jobs".into()
            }
        );
        assert_eq!(candidate.destination, None);

        let destination =
            EpochTargetDestination::new(EpochTargetKind::Queue, "jobs", 4, 0, 41, 3).unwrap();
        let fence = DeliveryFence::new(7, 2, 5, 1).unwrap();
        assert!(
            bus.acquire_deliveries("orders", "external-dispatcher", 1, 100, fence)
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            bus.acquire_specific_delivery(
                "orders",
                &candidate.delivery_id,
                "external-dispatcher",
                100,
                fence,
            ),
            Err(EpochError::Conflict(_))
        ));
        let lease = bus
            .acquire_specific_epoch_target_delivery(
                "orders",
                &candidate.delivery_id,
                "epoch-target-v1",
                100,
                fence,
                destination.clone(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(lease.destination.as_ref(), Some(&destination));

        bus.fail_delivery(
            &lease.delivery_id,
            "epoch-target-v1",
            &lease.lease_token,
            fence,
            "destination_unavailable",
            101,
        )
        .unwrap();
        let retried = bus
            .epoch_target_delivery_candidates(u64::MAX)
            .unwrap()
            .remove(0);
        assert_eq!(retried.next_attempt, 2);
        assert_eq!(retried.destination.as_ref(), Some(&destination));

        let different_generation =
            EpochTargetDestination::new(EpochTargetKind::Queue, "jobs", 5, 0, 51, 1).unwrap();
        assert!(matches!(
            bus.acquire_specific_epoch_target_delivery(
                "orders",
                &candidate.delivery_id,
                "epoch-target-v1",
                u64::MAX,
                fence,
                different_generation,
            ),
            Err(EpochError::Conflict(_))
        ));
        assert_eq!(
            bus.delivery(&candidate.delivery_id)
                .unwrap()
                .destination
                .as_ref(),
            Some(&destination)
        );
    }

    #[test]
    fn epoch_target_binding_requires_snapshot_v3_and_preserves_legacy_versions() {
        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        bus.upsert_subscription(subscription(
            "audit",
            SubscriptionTarget::Stream {
                resource: "events".into(),
            },
        ))
        .unwrap();
        bus.publish(event("order.created"), 100).unwrap();
        let delivery_id = "epoch.bus.delivery.v1.1.audit";
        let destination =
            EpochTargetDestination::new(EpochTargetKind::Stream, "events", 8, 3, 83, 2).unwrap();
        bus.acquire_specific_epoch_target_delivery(
            "audit",
            delivery_id,
            "epoch-target-v1",
            100,
            DeliveryFence::new(7, 2, 5, 1).unwrap(),
            destination.clone(),
        )
        .unwrap()
        .unwrap();

        let encoded = bus.encode_snapshot().unwrap();
        let snapshot: VersionedEventBusSnapshot = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            snapshot.format_version,
            EPOCH_TARGET_EVENT_BUS_SNAPSHOT_FORMAT_VERSION
        );
        assert_eq!(EPOCH_TARGET_EVENT_BUS_SNAPSHOT_FORMAT_VERSION, 3);
        let restored = EventBus::decode_snapshot(&encoded).unwrap();
        assert_eq!(restored.encode_snapshot().unwrap(), encoded);
        assert_eq!(
            restored.delivery(delivery_id).unwrap().destination,
            Some(destination)
        );

        let mut mislabeled = snapshot;
        mislabeled.format_version = SIGNED_WEBHOOK_EVENT_BUS_SNAPSHOT_FORMAT_VERSION;
        assert!(EventBus::decode_snapshot(&serde_json::to_vec(&mislabeled).unwrap()).is_err());
    }

    #[test]
    fn lease_deadline_is_exclusive_and_maintenance_is_bounded() {
        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        let mut route = subscription("audit", SubscriptionTarget::Pull);
        route.delivery_policy = DeliveryPolicy {
            timeout_ms: 10,
            max_in_flight: 1,
            retry: DeliveryRetryPolicy {
                strategy: DeliveryBackoffStrategy::Fixed,
                initial_delay_ms: 5,
                max_delay_ms: 5,
                jitter_percent: 0,
                max_attempts: 2,
                max_age_ms: None,
            },
            rate_limit: None,
            dead_letter_retention_ms: None,
        };
        bus.upsert_subscription(route).unwrap();
        bus.publish(event("order.created"), 100).unwrap();
        let fence = DeliveryFence::new(7, 3, 2, 1).unwrap();
        bus.acquire_deliveries("audit", "dispatcher", 1, 100, fence)
            .unwrap();

        assert_eq!(bus.next_delivery_maintenance_deadline_ms(), Some(110));
        assert_eq!(bus.maintain_deliveries(109, 1).unwrap().processed, 0);
        let maintained = bus.maintain_deliveries(110, 1).unwrap();
        assert_eq!(maintained.processed, 1);
        assert_eq!(maintained.retried, 1);
        assert!(matches!(
            bus.delivery("epoch.bus.delivery.v1.1.audit").unwrap().state,
            DeliveryState::Pending {
                eligible_at_ms: 115
            }
        ));
        assert_eq!(bus.next_delivery_maintenance_deadline_ms(), None);
    }

    #[test]
    fn per_subscription_rate_limit_redrive_and_dead_letter_retention_are_replicated() {
        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        let mut route = subscription("audit", SubscriptionTarget::Pull);
        route.delivery_policy = DeliveryPolicy {
            rate_limit: Some(DeliveryRateLimit {
                deliveries_per_second: 1,
                burst: 1,
            }),
            dead_letter_retention_ms: Some(10),
            retry: DeliveryRetryPolicy {
                max_attempts: 1,
                ..DeliveryRetryPolicy::default()
            },
            ..DeliveryPolicy::default()
        };
        bus.upsert_subscription(route).unwrap();
        bus.publish(event("order.one"), 0).unwrap();
        bus.publish(event("order.two"), 0).unwrap();
        assert!(bus.has_acquirable_pull_delivery("audit", 0).unwrap());
        let fence = DeliveryFence::new(7, 3, 2, 1).unwrap();
        let first = bus
            .acquire_deliveries("audit", "worker", 2, 0, fence)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert!(!bus.has_acquirable_pull_delivery("audit", 0).unwrap());
        bus.reject_delivery(
            &first[0].delivery_id,
            "worker",
            &first[0].lease_token,
            fence,
            "invalid",
            1,
        )
        .unwrap();
        bus.redrive_delivery(&first[0].delivery_id, 2).unwrap();
        assert!(
            bus.acquire_deliveries("audit", "worker", 2, 999, fence)
                .unwrap()
                .is_empty()
        );
        assert!(!bus.has_acquirable_pull_delivery("audit", 999).unwrap());
        assert!(bus.has_acquirable_pull_delivery("audit", 1_000).unwrap());
        let redriven = bus
            .acquire_deliveries("audit", "worker", 2, 1_000, fence)
            .unwrap();
        assert_eq!(redriven.len(), 1);
        assert_eq!(redriven[0].delivery_id, first[0].delivery_id);
        bus.reject_delivery(
            &redriven[0].delivery_id,
            "worker",
            &redriven[0].lease_token,
            fence,
            "still invalid",
            1_001,
        )
        .unwrap();
        let maintenance = bus.maintain_deliveries(1_011, 10).unwrap();
        assert_eq!(maintenance.purged, 1);
        assert!(bus.delivery(&redriven[0].delivery_id).is_none());
    }

    #[test]
    fn pull_acquisition_never_steals_push_or_managed_target_work() {
        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        for (name, target) in [
            (
                "webhook",
                SubscriptionTarget::Webhook {
                    url: "https://example.com/events".into(),
                    signing_key_id: Some("primary".into()),
                },
            ),
            (
                "api",
                SubscriptionTarget::ApiDestination {
                    url: "https://example.com/api".into(),
                    auth: DestinationAuth::None,
                    cloud_events_mode: CloudEventsMode::Binary,
                },
            ),
        ] {
            bus.upsert_subscription(subscription(name, target)).unwrap();
        }
        bus.publish(event("order.created"), 10).unwrap();
        let fence = DeliveryFence::new(7, 3, 2, 1).unwrap();
        for subscription in ["webhook", "api"] {
            assert!(!bus.has_acquirable_pull_delivery(subscription, 10).unwrap());
            assert!(
                bus.acquire_deliveries(subscription, "pull-worker", 10, 10, fence)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn stale_dispatcher_epoch_and_changed_leader_term_are_fenced() {
        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        bus.upsert_subscription(subscription("audit", SubscriptionTarget::Pull))
            .unwrap();
        bus.publish(event("order.created"), 100).unwrap();
        let original_fence = DeliveryFence::new(7, 3, 2, 1).unwrap();
        let delivery = bus
            .acquire_deliveries("audit", "dispatcher", 1, 100, original_fence)
            .unwrap()
            .remove(0);

        assert!(matches!(
            bus.acknowledge_delivery(
                &delivery.delivery_id,
                "dispatcher",
                &delivery.lease_token,
                DeliveryFence::new(7, 3, 3, 1).unwrap(),
                101,
            ),
            Err(EpochError::Fenced)
        ));
        assert!(matches!(
            bus.acquire_deliveries(
                "audit",
                "dispatcher",
                1,
                101,
                DeliveryFence::new(7, 3, 2, 2).unwrap(),
            ),
            Ok(ref deliveries) if deliveries.is_empty()
        ));
        assert!(matches!(
            bus.acknowledge_delivery(
                &delivery.delivery_id,
                "dispatcher",
                &delivery.lease_token,
                original_fence,
                102,
            ),
            Err(EpochError::Fenced)
        ));
    }

    #[test]
    fn delivery_policy_is_strict_bounded_and_defaults_without_wire_drift() {
        let unknown_retry = json!({
            "name": "audit",
            "filter": {},
            "target": {"kind": "pull"},
            "delivery_policy": {
                "timeout_ms": 100,
                "max_in_flight": 1,
                "retry": {
                    "strategy": "fixed",
                    "initial_delay_ms": 1,
                    "max_delay_ms": 1,
                    "jitter_percent": 0,
                    "max_attempts": 1,
                    "max_age_ms": null,
                    "unexpected": true
                }
            }
        });
        assert!(serde_json::from_value::<Subscription>(unknown_retry).is_err());

        let mut bus = EventBus::new(BusConfig::default()).unwrap();
        let mut invalid = subscription("audit", SubscriptionTarget::Pull);
        invalid.delivery_policy.max_in_flight = 0;
        assert!(matches!(
            bus.upsert_subscription(invalid),
            Err(EpochError::InvalidArgument(_))
        ));
        assert_eq!(bus.subscription_count(), 0);
    }

    #[test]
    fn delivery_deadline_overflow_rejects_without_partial_state_or_epoch_fencing() {
        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        let mut expiring = subscription("audit", SubscriptionTarget::Pull);
        expiring.delivery_policy.retry.max_age_ms = Some(10);
        bus.upsert_subscription(expiring).unwrap();
        assert!(matches!(
            bus.publish(event("order.created"), u64::MAX - 5),
            Err(EpochError::Capacity(_))
        ));
        assert_eq!(bus.commit_position(), 0);
        assert_eq!(bus.archived_event_count(), 0);
        assert_eq!(bus.delivery_counts(), DeliveryCounts::default());

        let mut bus = EventBus::new(BusConfig {
            delivery_outbox: true,
            ..BusConfig::default()
        })
        .unwrap();
        bus.upsert_subscription(subscription("audit", SubscriptionTarget::Pull))
            .unwrap();
        bus.publish(event("order.created"), 0).unwrap();
        assert!(matches!(
            bus.acquire_deliveries(
                "audit",
                "dispatcher",
                1,
                u64::MAX,
                DeliveryFence::new(7, 3, 2, 2).unwrap(),
            ),
            Err(EpochError::Capacity(_))
        ));
        let acquired = bus
            .acquire_deliveries(
                "audit",
                "dispatcher",
                1,
                0,
                DeliveryFence::new(7, 3, 2, 1).unwrap(),
            )
            .unwrap();
        assert_eq!(acquired.len(), 1);
    }

    fn subscription(name: &str, target: SubscriptionTarget) -> Subscription {
        Subscription {
            name: name.into(),
            filter: EventFilter::default(),
            target,
            transform: EventTransform::default(),
            delivery_policy: DeliveryPolicy::default(),
        }
    }
}
