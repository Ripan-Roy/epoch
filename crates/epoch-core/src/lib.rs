//! Shared domain types for all Epoch workload profiles.
//!
//! This crate intentionally has no networking, async-runtime, or storage
//! dependencies. Protocol gateways and engines use these types as their
//! semantic boundary.

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
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

/// Supplies both user-visible wall time and process-local elapsed time.
///
/// Engines use the wall clock for scheduled instants and timestamps.
/// Process-local waiting and timeout implementations should use the monotonic
/// clock. Persisted state-machine deadlines must separately apply their
/// documented logical-time and fencing rules across restart or leader change.
/// Implementations must never move their monotonic value backwards.
pub trait Clock: Send + Sync + std::fmt::Debug {
    fn wall_time_ms(&self) -> u64;

    fn monotonic_time_ms(&self) -> u64;

    /// Compatibility alias for call sites that explicitly require wall time.
    fn now_ms(&self) -> u64 {
        self.wall_time_ms()
    }
}

#[derive(Debug)]
pub struct SystemClock {
    started_at: Instant,
}

impl Default for SystemClock {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl Clock for SystemClock {
    fn wall_time_ms(&self) -> u64 {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        u64::try_from(millis).unwrap_or(u64::MAX)
    }

    fn monotonic_time_ms(&self) -> u64 {
        u64::try_from(self.started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

#[derive(Debug)]
pub struct ManualClock {
    wall_time_ms: AtomicU64,
    monotonic_time_ms: AtomicU64,
}

impl ManualClock {
    pub const fn new(wall_time_ms: u64) -> Self {
        Self::with_times(wall_time_ms, 0)
    }

    pub const fn with_times(wall_time_ms: u64, monotonic_time_ms: u64) -> Self {
        Self {
            wall_time_ms: AtomicU64::new(wall_time_ms),
            monotonic_time_ms: AtomicU64::new(monotonic_time_ms),
        }
    }

    /// Moves wall time to an explicit instant without changing elapsed time.
    pub fn set_wall_time_ms(&self, wall_time_ms: u64) {
        self.wall_time_ms.store(wall_time_ms, Ordering::SeqCst);
    }

    /// Compatibility alias for existing deterministic tests.
    pub fn set(&self, wall_time_ms: u64) {
        self.set_wall_time_ms(wall_time_ms);
    }

    /// Advances normal elapsed time, moving wall and monotonic time together.
    pub fn advance_elapsed(&self, delta_ms: u64) -> u64 {
        atomic_saturating_add(&self.monotonic_time_ms, delta_ms);
        atomic_saturating_add(&self.wall_time_ms, delta_ms)
    }

    /// Compatibility alias for existing deterministic tests.
    pub fn advance(&self, delta_ms: u64) -> u64 {
        self.advance_elapsed(delta_ms)
    }
}

impl Clock for ManualClock {
    fn wall_time_ms(&self) -> u64 {
        self.wall_time_ms.load(Ordering::SeqCst)
    }

    fn monotonic_time_ms(&self) -> u64 {
        self.monotonic_time_ms.load(Ordering::SeqCst)
    }
}

fn atomic_saturating_add(value: &AtomicU64, delta: u64) -> u64 {
    let mut current = value.load(Ordering::SeqCst);
    loop {
        let next = current.saturating_add(delta);
        match value.compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return next,
            Err(actual) => current = actual,
        }
    }
}

/// A persisted hybrid-logical-clock observation, ordered lexicographically.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HybridTimestamp {
    pub physical_ms: u64,
    pub logical: u32,
}

impl HybridTimestamp {
    pub const fn new(physical_ms: u64, logical: u32) -> Self {
        Self {
            physical_ms,
            logical,
        }
    }
}

/// Deterministically advances persisted logical time across clock jumps and
/// observations received from another node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HybridLogicalClock {
    last: HybridTimestamp,
}

impl HybridLogicalClock {
    pub const fn from_persisted(last: HybridTimestamp) -> Self {
        Self { last }
    }

    pub const fn last(&self) -> HybridTimestamp {
        self.last
    }

    pub fn tick(&mut self, wall_time_ms: u64) -> EpochResult<HybridTimestamp> {
        let next = if wall_time_ms > self.last.physical_ms {
            HybridTimestamp::new(wall_time_ms, 0)
        } else {
            increment_hybrid_logical(self.last)?
        };
        self.last = next;
        Ok(next)
    }

    pub fn observe(
        &mut self,
        wall_time_ms: u64,
        remote: HybridTimestamp,
    ) -> EpochResult<HybridTimestamp> {
        let physical_ms = wall_time_ms
            .max(self.last.physical_ms)
            .max(remote.physical_ms);
        let logical =
            if physical_ms == self.last.physical_ms && physical_ms == remote.physical_ms {
                self.last.logical.max(remote.logical).checked_add(1)
            } else if physical_ms == self.last.physical_ms {
                self.last.logical.checked_add(1)
            } else if physical_ms == remote.physical_ms {
                remote.logical.checked_add(1)
            } else {
                Some(0)
            }
            .ok_or_else(|| EpochError::Capacity("hybrid logical clock overflow".into()))?;
        let next = HybridTimestamp::new(physical_ms, logical);
        self.last = next;
        Ok(next)
    }
}

fn increment_hybrid_logical(timestamp: HybridTimestamp) -> EpochResult<HybridTimestamp> {
    let logical = timestamp
        .logical
        .checked_add(1)
        .ok_or_else(|| EpochError::Capacity("hybrid logical clock overflow".into()))?;
    Ok(HybridTimestamp::new(timestamp.physical_ms, logical))
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
    fn wall_clock_jumps_do_not_move_manual_monotonic_time_backwards() {
        let clock = ManualClock::with_times(1_000, 50);

        clock.set_wall_time_ms(10);
        assert_eq!(clock.wall_time_ms(), 10);
        assert_eq!(clock.monotonic_time_ms(), 50);

        assert_eq!(clock.advance_elapsed(25), 35);
        assert_eq!(clock.wall_time_ms(), 35);
        assert_eq!(clock.monotonic_time_ms(), 75);
    }

    #[test]
    fn manual_elapsed_time_saturates_instead_of_wrapping() {
        let clock = ManualClock::with_times(u64::MAX - 1, u64::MAX - 2);

        assert_eq!(clock.advance_elapsed(10), u64::MAX);
        assert_eq!(clock.wall_time_ms(), u64::MAX);
        assert_eq!(clock.monotonic_time_ms(), u64::MAX);
    }

    #[test]
    fn hybrid_logical_time_never_moves_back_with_wall_time() {
        let mut clock = HybridLogicalClock::from_persisted(HybridTimestamp::new(100, 0));

        let first = clock.tick(90).unwrap();
        let second = clock.tick(80).unwrap();

        assert_eq!(first, HybridTimestamp::new(100, 1));
        assert_eq!(second, HybridTimestamp::new(100, 2));
        assert!(second > first);
    }

    #[test]
    fn hybrid_logical_time_orders_remote_observations() {
        let mut clock = HybridLogicalClock::from_persisted(HybridTimestamp::new(100, 4));

        let observed = clock.observe(90, HybridTimestamp::new(100, 7)).unwrap();
        let advanced = clock.observe(110, HybridTimestamp::new(105, 9)).unwrap();

        assert_eq!(observed, HybridTimestamp::new(100, 8));
        assert_eq!(advanced, HybridTimestamp::new(110, 0));
        assert!(advanced > observed);
    }

    #[test]
    fn hybrid_logical_overflow_fails_without_reusing_a_timestamp() {
        let persisted = HybridTimestamp::new(100, u32::MAX);
        let mut clock = HybridLogicalClock::from_persisted(persisted);

        assert!(matches!(clock.tick(100), Err(EpochError::Capacity(_))));
        assert_eq!(clock.last(), persisted);
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
