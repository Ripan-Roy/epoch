//! Event routing, subscription filtering, transformation, and archive replay.

use std::collections::BTreeMap;

use epoch_core::{
    AckMetadata, DurabilityProfile, EpochError, EpochResult, EventEnvelope, validate_resource_name,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BusConfig {
    pub durability: DurabilityProfile,
    pub archive: bool,
    #[serde(default = "default_max_subscriptions")]
    pub max_subscriptions: usize,
    #[serde(default = "default_max_archive_events")]
    pub max_archive_events: usize,
}

impl Default for BusConfig {
    fn default() -> Self {
        Self {
            durability: DurabilityProfile::Volatile,
            archive: true,
            max_subscriptions: DEFAULT_MAX_SUBSCRIPTIONS,
            max_archive_events: DEFAULT_MAX_ARCHIVE_EVENTS,
        }
    }
}

const fn default_max_subscriptions() -> usize {
    DEFAULT_MAX_SUBSCRIPTIONS
}

const fn default_max_archive_events() -> usize {
    DEFAULT_MAX_ARCHIVE_EVENTS
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubscriptionTarget {
    Pull,
    Queue { resource: String },
    Stream { resource: String },
    Webhook { url: String },
    Http { url: String },
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
}

impl Subscription {
    pub fn validate(&self) -> EpochResult<()> {
        validate_resource_name(&self.name)?;
        self.filter.validate()?;
        self.transform.validate()?;
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
    pub subscription: String,
    pub target: SubscriptionTarget,
    pub envelope: EventEnvelope,
    pub route_plan_version: u64,
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
}

impl EventBus {
    pub fn new(config: BusConfig) -> EpochResult<Self> {
        validate_config(&config)?;
        Ok(Self {
            config,
            subscriptions: BTreeMap::new(),
            route_plan_version: 1,
            commit_position: 0,
            archive: Vec::new(),
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
                subscription: subscription.name.clone(),
                target: subscription.target.clone(),
                envelope: subscription.transform.apply(&event),
                route_plan_version,
            })
            .collect();
        self.commit_position = position;
        if self.config.archive {
            self.archive.push(ArchivedEvent {
                position,
                received_at_ms: now_ms,
                route_plan_version,
                envelope: event,
            });
        }
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

    /// Deterministic digest of all state required to rebuild route and replay behavior.
    pub fn recovery_state_digest(&self) -> EpochResult<[u8; 32]> {
        let encoded = serde_json::to_vec(&(
            &self.config,
            &self.subscriptions,
            self.route_plan_version,
            self.commit_position,
            &self.archive,
        ))
        .map_err(|error| EpochError::Internal(error.to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(b"epoch/event-bus/recovery-state/v1\0");
        hasher.update(encoded);
        Ok(hasher.finalize().into())
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
    fn replay_limit_is_bounded() {
        let bus = EventBus::new(BusConfig::default()).unwrap();
        assert!(matches!(
            bus.replay(0, 1, None, MAX_REPLAY_EVENTS + 1),
            Err(EpochError::InvalidArgument(_))
        ));
    }

    fn subscription(name: &str, target: SubscriptionTarget) -> Subscription {
        Subscription {
            name: name.into(),
            filter: EventFilter::default(),
            target,
            transform: EventTransform::default(),
        }
    }
}
