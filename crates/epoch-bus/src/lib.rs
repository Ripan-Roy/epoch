//! Event routing, subscription filtering, transformation, and archive replay.

use std::collections::{BTreeMap, HashMap};

use epoch_core::{AckMetadata, DurabilityProfile, EpochError, EpochResult, EventEnvelope};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusConfig {
    pub durability: DurabilityProfile,
    pub archive: bool,
}

impl Default for BusConfig {
    fn default() -> Self {
        Self {
            durability: DurabilityProfile::Volatile,
            archive: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
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
}

fn matches_patterns(patterns: &[String], value: Option<&str>) -> bool {
    patterns.is_empty()
        || value.is_some_and(|value| patterns.iter().any(|pattern| glob_matches(pattern, value)))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SubscriptionTarget {
    Pull,
    Queue { resource: String },
    Stream { resource: String },
    Webhook { url: String },
    Http { url: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subscription {
    pub name: String,
    pub filter: EventFilter,
    pub target: SubscriptionTarget,
    #[serde(default)]
    pub transform: EventTransform,
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

#[derive(Debug)]
pub struct EventBus {
    config: BusConfig,
    subscriptions: HashMap<String, Subscription>,
    route_plan_version: u64,
    commit_position: u64,
    archive: Vec<ArchivedEvent>,
}

impl EventBus {
    pub fn new(config: BusConfig) -> Self {
        Self {
            config,
            subscriptions: HashMap::new(),
            route_plan_version: 1,
            commit_position: 0,
            archive: Vec::new(),
        }
    }

    pub fn config(&self) -> &BusConfig {
        &self.config
    }

    pub fn upsert_subscription(&mut self, subscription: Subscription) -> EpochResult<u64> {
        if subscription.name.trim().is_empty() {
            return Err(EpochError::InvalidArgument(
                "subscription name is required".into(),
            ));
        }
        if let SubscriptionTarget::Webhook { url } | SubscriptionTarget::Http { url } =
            &subscription.target
            && !(url.starts_with("https://") || url.starts_with("http://"))
        {
            return Err(EpochError::InvalidArgument(
                "HTTP targets require an http or https URL".into(),
            ));
        }
        self.subscriptions
            .insert(subscription.name.clone(), subscription);
        self.route_plan_version = self.route_plan_version.saturating_add(1);
        Ok(self.route_plan_version)
    }

    pub fn remove_subscription(&mut self, name: &str) -> bool {
        let removed = self.subscriptions.remove(name).is_some();
        if removed {
            self.route_plan_version = self.route_plan_version.saturating_add(1);
        }
        removed
    }

    pub fn publish(&mut self, event: EventEnvelope, now_ms: u64) -> EpochResult<PublishResult> {
        event.validate()?;
        self.commit_position = self.commit_position.saturating_add(1);
        let position = self.commit_position;
        let route_plan_version = self.route_plan_version;
        let mut subscriptions: Vec<&Subscription> = self.subscriptions.values().collect();
        subscriptions.sort_by(|left, right| left.name.cmp(&right.name));
        let deliveries = subscriptions
            .into_iter()
            .filter(|subscription| subscription.filter.matches(&event))
            .map(|subscription| RoutedDelivery {
                subscription: subscription.name.clone(),
                target: subscription.target.clone(),
                envelope: subscription.transform.apply(&event),
                route_plan_version,
            })
            .collect();
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
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut pattern_index, mut value_index) = (0, 0);
    let (mut star, mut checkpoint) = (None, 0);
    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
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
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
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
        let mut bus = EventBus::new(BusConfig::default());
        for name in ["audit", "worker"] {
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
    }

    #[test]
    fn transformation_projects_payload_and_adds_headers() {
        let mut bus = EventBus::new(BusConfig::default());
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
        let mut bus = EventBus::new(BusConfig::default());
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
        assert!(!glob_matches("payment.*", "order.created"));
    }
}
