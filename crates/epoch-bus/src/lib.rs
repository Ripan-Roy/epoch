//! Event routing, filtering, transformation, archive replay, and delivery state.

use std::collections::BTreeMap;

mod delivery;

use epoch_core::{
    AckMetadata, DurabilityProfile, EpochError, EpochResult, EventEnvelope, validate_resource_name,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;

pub use delivery::{
    DEFAULT_MAX_OUTBOX_DELIVERIES, DeliveryAttempt, DeliveryAttemptOutcome,
    DeliveryBackoffStrategy, DeliveryCounts, DeliveryFence, DeliveryLease,
    DeliveryMaintenanceResult, DeliveryPolicy, DeliveryRecord, DeliveryRetryPolicy, DeliveryState,
    DeliveryStateKind, MAX_BUS_OUTBOX_DELIVERIES, MAX_DELIVERY_ACQUIRE_BATCH,
    MAX_DELIVERY_ATTEMPTS, MAX_DELIVERY_IN_FLIGHT, MAX_DELIVERY_QUERY_RESULTS,
    MAX_DELIVERY_REASON_BYTES, MAX_DELIVERY_TIMEOUT_MS,
};
use delivery::{DeliveryLedger, delivery_id};

pub const DEFAULT_MAX_SUBSCRIPTIONS: usize = 1_024;
pub const DEFAULT_MAX_ARCHIVE_EVENTS: usize = 100_000;
pub const MAX_BUS_SUBSCRIPTIONS: usize = 100_000;
pub const MAX_BUS_ARCHIVE_EVENTS: usize = 10_000_000;
pub const MAX_REPLAY_EVENTS: usize = 10_000;
pub const MAX_FILTER_PATTERNS: usize = 64;
pub const MAX_FILTER_ENTRIES: usize = 64;
pub const MAX_TRANSFORM_ENTRIES: usize = 64;
pub const MAX_PATTERN_BYTES: usize = 512;
pub const MAX_HEADER_KEY_BYTES: usize = 256;
pub const MAX_HEADER_VALUE_BYTES: usize = 4 * 1024;
pub const MAX_JSON_PATH_BYTES: usize = 1_024;
pub const MAX_PROJECTED_FIELD_BYTES: usize = 256;
pub const MAX_FILTER_VALUE_BYTES: usize = 64 * 1024;
pub const MAX_TARGET_URL_BYTES: usize = 8 * 1024;
pub const EVENT_BUS_SNAPSHOT_FORMAT_VERSION: u16 = 1;
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
            max_outbox_deliveries: DEFAULT_MAX_OUTBOX_DELIVERIES,
        }
    }
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
        matches_patterns(&self.event_type_patterns, Some(&event.event_type))
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
    Queue { resource: String },
    Stream { resource: String },
    Webhook { url: String },
    Http { url: String },
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
    Webhook(StrictUrlTarget<WebhookTargetKind>),
    Http(StrictUrlTarget<HttpTargetKind>),
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
            StrictSubscriptionTarget::Webhook(target) => Self::Webhook { url: target.url },
            StrictSubscriptionTarget::Http(target) => Self::Http { url: target.url },
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
struct StrictUrlTarget<Kind> {
    #[serde(rename = "kind")]
    _kind: Kind,
    url: String,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct EventTransform {
    #[serde(default)]
    pub add_headers: BTreeMap<String, String>,
    #[serde(default)]
    pub payload_projection: BTreeMap<String, String>,
}

impl EventTransform {
    fn apply(&self, event: &EventEnvelope) -> EventEnvelope {
        let mut output = event.clone();
        output.headers.extend(self.add_headers.clone());
        if !self.payload_projection.is_empty() {
            let mut projected = serde_json::Map::new();
            for (output_field, source_path) in &self.payload_projection {
                if let Some(value) = json_path(&event.payload, source_path) {
                    projected.insert(output_field.clone(), value.clone());
                }
            }
            output.payload = Value::Object(projected);
        }
        output
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
            validate_text(
                "projected output field",
                output_field,
                MAX_PROJECTED_FIELD_BYTES,
            )?;
            if output_field.contains('.') {
                return Err(EpochError::InvalidArgument(
                    "projected output fields cannot contain dots".into(),
                ));
            }
            validate_json_path(source_path)?;
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
            SubscriptionTarget::Queue { resource } | SubscriptionTarget::Stream { resource } => {
                validate_resource_name(resource)?;
            }
            SubscriptionTarget::Webhook { url } | SubscriptionTarget::Http { url } => {
                validate_http_target(url)?;
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
    delivery_ledger: DeliveryLedger,
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
    delivery_ledger: DeliveryLedger,
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
            delivery_ledger,
        })
    }

    pub fn config(&self) -> &BusConfig {
        &self.config
    }

    pub fn upsert_subscription(&mut self, subscription: Subscription) -> EpochResult<u64> {
        subscription.validate()?;
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
        let position = self
            .commit_position
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("event bus commit position overflow".into()))?;
        if self.config.archive && self.archive.len() >= self.config.max_archive_events {
            return Err(EpochError::Capacity(format!(
                "event archive reached its {} event limit",
                self.config.max_archive_events
            )));
        }
        let route_plan_version = self.route_plan_version;
        let deliveries = self
            .subscriptions
            .values()
            .filter(|subscription| subscription.filter.matches(&event))
            .map(|subscription| RoutedDelivery {
                delivery_id: delivery_id(position, &subscription.name),
                subscription: subscription.name.clone(),
                target: subscription.target.clone(),
                envelope: subscription.transform.apply(&event),
                route_plan_version,
                delivery_policy: subscription.delivery_policy.clone(),
            })
            .collect::<Vec<_>>();
        let mut next_delivery_ledger = self.delivery_ledger.clone();
        next_delivery_ledger.append_publish(position, now_ms, &deliveries)?;
        self.commit_position = position;
        if self.config.archive {
            self.archive.push(ArchivedEvent {
                position,
                received_at_ms: now_ms,
                route_plan_version,
                envelope: event,
            });
        }
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

    /// Deterministic digest of all state required to rebuild route and replay behavior.
    pub fn recovery_state_digest(&self) -> EpochResult<[u8; 32]> {
        let encoded = serde_json::to_vec(&(
            &self.config,
            &self.subscriptions,
            self.route_plan_version,
            self.commit_position,
            &self.archive,
            &self.delivery_ledger,
        ))
        .map_err(|error| EpochError::Internal(error.to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(b"epoch/event-bus/recovery-state/v2\0");
        hasher.update(encoded);
        Ok(hasher.finalize().into())
    }

    /// Encodes the complete routing, archive, and delivery-ledger state as a
    /// canonical versioned application snapshot.
    pub fn encode_snapshot(&self) -> EpochResult<Vec<u8>> {
        let encoded = serde_json::to_vec(&VersionedEventBusSnapshot {
            format_version: EVENT_BUS_SNAPSHOT_FORMAT_VERSION,
            config: self.config.clone(),
            subscriptions: self.subscriptions.clone(),
            route_plan_version: self.route_plan_version,
            commit_position: self.commit_position,
            archive: self.archive.clone(),
            delivery_ledger: self.delivery_ledger.clone(),
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
        if snapshot.format_version != EVENT_BUS_SNAPSHOT_FORMAT_VERSION {
            return Err(EpochError::InvalidArgument(format!(
                "unsupported Event Bus snapshot version {}",
                snapshot.format_version
            )));
        }
        if serde_json::to_vec(&snapshot)
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
        if (snapshot.config.archive
            && u64::try_from(snapshot.archive.len()).ok() != Some(snapshot.commit_position))
            || (!snapshot.config.archive && !snapshot.archive.is_empty())
            || snapshot.archive.len() > snapshot.config.max_archive_events
        {
            return Err(EpochError::InvalidArgument(
                "Event Bus snapshot archive configuration is invalid".into(),
            ));
        }
        for (position, event) in snapshot.archive.iter().enumerate() {
            if u64::try_from(position + 1).ok() != Some(event.position)
                || event.route_plan_version == 0
                || event.route_plan_version > snapshot.route_plan_version
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
        let bus = Self {
            config: snapshot.config,
            subscriptions: snapshot.subscriptions,
            route_plan_version: snapshot.route_plan_version,
            commit_position: snapshot.commit_position,
            archive: snapshot.archive,
            delivery_ledger: snapshot.delivery_ledger,
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
    )
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

fn json_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let path = path.strip_prefix("$.").unwrap_or(path);
    if path.is_empty() || path == "$" {
        return Some(value);
    }
    path.split('.').try_fold(value, |current, segment| {
        current.as_object().and_then(|object| object.get(segment))
    })
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
            },
            SubscriptionTarget::Http {
                url: "file:///tmp/hook".into(),
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
            },
            SubscriptionTarget::Http {
                url: "https://example.com/events".into(),
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
