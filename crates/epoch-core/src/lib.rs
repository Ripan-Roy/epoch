//! Shared domain types for all Epoch workload profiles.
//!
//! This crate intentionally has no networking, async-runtime, or storage
//! dependencies. Protocol gateways and engines use these types as their
//! semantic boundary.

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

pub type EpochResult<T> = Result<T, EpochError>;

#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", content = "detail", rename_all = "snake_case")]
pub enum EpochError {
    #[error("resource already exists: {0}")]
    AlreadyExists(String),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("request was fenced by a newer epoch")]
    Fenced,
    #[error("resource capacity was exhausted: {0}")]
    Capacity(String),
    #[error("operation is unavailable: {0}")]
    Unavailable(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityProfile {
    #[default]
    Volatile,
    ReplicatedMemory,
    LocalDurable,
    QuorumDurable,
    GeoAsync,
    GeoSync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliverySemantics {
    AtMostOnce,
    AtLeastOnce,
    EffectivelyOnce,
    TransactionalExactlyOnce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingScope {
    None,
    Key,
    Session,
    Partition,
    Resource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentMode {
    Embedded,
    #[default]
    Standalone,
    Cluster,
    Managed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Cache,
    Stream,
    Queue,
    EventBus,
    Subscription,
    Schema,
    Pipe,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckMetadata {
    pub durability: DurabilityProfile,
    pub resource_epoch: u64,
    pub commit_position: u64,
    pub replica_acks: u16,
    pub duplicate: bool,
}

impl AckMetadata {
    pub fn standalone(position: u64, durability: DurabilityProfile) -> Self {
        Self {
            durability,
            resource_epoch: 1,
            commit_position: position,
            replica_acks: 1,
            duplicate: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: String,
    pub source: String,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    pub time_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default = "default_content_type")]
    pub content_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    #[serde(default)]
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deliver_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(default)]
    pub priority: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedupe_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
    #[serde(default)]
    pub extensions: BTreeMap<String, Value>,
}

fn default_content_type() -> String {
    "application/json".to_owned()
}

impl EventEnvelope {
    pub fn new(
        source: impl Into<String>,
        event_type: impl Into<String>,
        payload: Value,
        now_ms: u64,
    ) -> Self {
        Self {
            id: Uuid::now_v7().to_string(),
            source: source.into(),
            event_type: event_type.into(),
            subject: None,
            time_ms: now_ms,
            key: None,
            headers: BTreeMap::new(),
            content_type: default_content_type(),
            schema_ref: None,
            traceparent: None,
            payload,
            deliver_at_ms: None,
            ttl_ms: None,
            priority: 0,
            dedupe_id: None,
            transaction_id: None,
            extensions: BTreeMap::new(),
        }
    }

    pub fn validate(&self) -> EpochResult<()> {
        if self.id.trim().is_empty() {
            return Err(EpochError::InvalidArgument("event id is required".into()));
        }
        if self.source.trim().is_empty() {
            return Err(EpochError::InvalidArgument(
                "event source is required".into(),
            ));
        }
        if self.event_type.trim().is_empty() {
            return Err(EpochError::InvalidArgument("event type is required".into()));
        }
        if self.priority > 9 {
            return Err(EpochError::InvalidArgument(
                "priority must be between 0 and 9".into(),
            ));
        }
        Ok(())
    }
}

pub trait Clock: Send + Sync + std::fmt::Debug {
    fn now_ms(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        u64::try_from(millis).unwrap_or(u64::MAX)
    }
}

#[derive(Debug)]
pub struct ManualClock {
    now_ms: AtomicU64,
}

impl ManualClock {
    pub const fn new(now_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(now_ms),
        }
    }

    pub fn set(&self, now_ms: u64) {
        self.now_ms.store(now_ms, Ordering::SeqCst);
    }

    pub fn advance(&self, delta_ms: u64) -> u64 {
        self.now_ms.fetch_add(delta_ms, Ordering::SeqCst) + delta_ms
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

pub fn validate_resource_name(name: &str) -> EpochResult<()> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(EpochError::InvalidArgument(format!(
            "invalid resource name: {name}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_is_deterministic() {
        let clock = ManualClock::new(100);
        assert_eq!(clock.now_ms(), 100);
        assert_eq!(clock.advance(25), 125);
        clock.set(7);
        assert_eq!(clock.now_ms(), 7);
    }

    #[test]
    fn envelope_validation_names_bad_fields() {
        let mut event = EventEnvelope::new("", "order.created", Value::Null, 0);
        assert!(matches!(
            event.validate(),
            Err(EpochError::InvalidArgument(_))
        ));
        event.source = "checkout".into();
        event.priority = 10;
        assert!(matches!(
            event.validate(),
            Err(EpochError::InvalidArgument(_))
        ));
    }

    #[test]
    fn resource_names_are_restricted() {
        assert!(validate_resource_name("orders.v1_eu-west").is_ok());
        assert!(validate_resource_name("orders/v1").is_err());
    }
}
