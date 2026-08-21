//! Deterministic Queue state services for capacity, sessions, fair dispatch,
//! deferred retrieval, and request/reply metadata.

use std::collections::{BTreeMap, BTreeSet};

use crc32fast::Hasher;
use epoch_core::{EpochError, EpochResult, EventEnvelope};
use serde::{Deserialize, Serialize};

use crate::{Delivery, EnqueueReceipt, LeaseFence, Queue, QueueState};

pub const MAX_QUEUE_ACTIVE_BYTES: usize = 3 * 1024 * 1024;
pub const MAX_QUEUE_ADVANCED_IDENTIFIER_BYTES: usize = 256;
pub const MAX_QUEUE_DEFER_REASON_BYTES: usize = 4 * 1024;
pub const MAX_QUEUE_SESSION_LOCKS: usize = 4_096;
const SESSION_TOKEN_PREFIX: &str = "epoch.queue.session.";
const SESSION_TOKEN_DOMAIN: &[u8] = b"epoch.queue.session-lock.v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueueOverflowPolicy {
    #[default]
    RejectNew,
    DropOldest,
    DeadLetterOldest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueDispatchPolicy {
    pub messages_per_second: u32,
    pub burst: u32,
    pub max_in_flight: u32,
    pub failure_threshold: u32,
    pub open_interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct QueueAdvancedConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_active_bytes: Option<usize>,
    #[serde(default)]
    pub overflow: QueueOverflowPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_expiry_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority_aging_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch: Option<QueueDispatchPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_letter_target: Option<String>,
}

impl QueueAdvancedConfig {
    pub(crate) fn validate(&self) -> EpochResult<()> {
        if self
            .max_active_bytes
            .is_some_and(|limit| !(1..=MAX_QUEUE_ACTIVE_BYTES).contains(&limit))
        {
            return Err(EpochError::InvalidArgument(format!(
                "queue max_active_bytes must be between 1 and {MAX_QUEUE_ACTIVE_BYTES}"
            )));
        }
        if self.idle_expiry_ms == Some(0) {
            return Err(EpochError::InvalidArgument(
                "queue idle_expiry_ms must be greater than zero".into(),
            ));
        }
        if self.priority_aging_interval_ms == Some(0) {
            return Err(EpochError::InvalidArgument(
                "queue priority_aging_interval_ms must be greater than zero".into(),
            ));
        }
        if let Some(policy) = &self.dispatch
            && (policy.messages_per_second == 0
                || policy.burst == 0
                || policy.max_in_flight == 0
                || policy.failure_threshold == 0
                || policy.open_interval_ms == 0)
        {
            return Err(EpochError::InvalidArgument(
                "all queue dispatch policy values must be greater than zero".into(),
            ));
        }
        if let Some(target) = &self.dead_letter_target {
            validate_identifier("dead-letter target", target)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueIngress {
    pub envelope: EventEnvelope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
}

impl QueueIngress {
    pub fn new(envelope: EventEnvelope) -> Self {
        Self {
            envelope,
            session_id: None,
            correlation_id: None,
            reply_to: None,
        }
    }

    #[must_use]
    pub fn with_session(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    #[must_use]
    pub fn with_correlation(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    #[must_use]
    pub fn with_reply_to(mut self, reply_to: impl Into<String>) -> Self {
        self.reply_to = Some(reply_to.into());
        self
    }

    pub fn charged_bytes(&self) -> EpochResult<usize> {
        serde_json::to_vec(self)
            .map(|encoded| encoded.len())
            .map_err(|error| EpochError::InvalidArgument(error.to_string()))
    }

    fn validate(&self) -> EpochResult<()> {
        self.envelope.validate()?;
        for (name, value) in [
            ("session ID", self.session_id.as_deref()),
            ("correlation ID", self.correlation_id.as_deref()),
            ("reply destination", self.reply_to.as_deref()),
        ] {
            if let Some(value) = value {
                validate_identifier(name, value)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueMessageMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    pub charged_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueDeferredMessage {
    pub message_id: String,
    pub reason: String,
    pub deferred_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCorrelatedMessage {
    pub message: crate::QueueMessage,
    pub metadata: QueueMessageMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueueCircuitState {
    #[default]
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueAdvancedObservation {
    pub expired: bool,
    pub active_bytes: usize,
    pub deferred_messages: usize,
    pub session_locks: usize,
    pub circuit_state: QueueCircuitState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub circuit_open_until_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueSessionAcquisition {
    pub session_id: String,
    pub lock_token: String,
    pub lock_deadline_ms: u64,
    pub deliveries: Vec<Delivery>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueSessionRenewal {
    pub session_id: String,
    pub lock_token: String,
    pub lock_deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionLock {
    session_id: String,
    consumer: String,
    fence: LeaseFence,
    generation: u64,
    token: String,
    deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DispatchState {
    milli_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_refill_ms: Option<u64>,
    consecutive_failures: u32,
    circuit_state: QueueCircuitState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    open_until_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    half_open_probe_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueueAdvancedState {
    metadata: BTreeMap<String, QueueMessageMetadata>,
    deferred: BTreeMap<String, QueueDeferredMessage>,
    session_locks: BTreeMap<String, SessionLock>,
    session_lock_generation: u64,
    dispatch: DispatchState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    first_used_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_activity_at_ms: Option<u64>,
    expired: bool,
}

impl QueueAdvancedState {
    pub(crate) fn validate(&self, queue: &Queue) -> EpochResult<()> {
        for (id, metadata) in &self.metadata {
            if !queue.messages.contains_key(id) || metadata.charged_bytes == 0 {
                return Err(invalid_snapshot());
            }
            for (name, value) in [
                ("session ID", metadata.session_id.as_deref()),
                ("correlation ID", metadata.correlation_id.as_deref()),
                ("reply destination", metadata.reply_to.as_deref()),
            ] {
                if let Some(value) = value {
                    validate_identifier(name, value)?;
                }
            }
        }
        if self.deferred.iter().any(|(id, deferred)| {
            id != &deferred.message_id
                || deferred.reason.trim().is_empty()
                || deferred.reason.len() > MAX_QUEUE_DEFER_REASON_BYTES
                || !queue
                    .messages
                    .get(id)
                    .is_some_and(|message| message.state == QueueState::Ready)
        }) {
            return Err(invalid_snapshot());
        }
        if self.session_locks.len() > MAX_QUEUE_SESSION_LOCKS {
            return Err(invalid_snapshot());
        }
        for (session_id, lock) in &self.session_locks {
            if session_id != &lock.session_id
                || lock.generation == 0
                || lock.deadline_ms == 0
                || lock.fence.validate().is_err()
            {
                return Err(invalid_snapshot());
            }
            validate_identifier("session ID", session_id)?;
            validate_identifier("session consumer", &lock.consumer)?;
            if session_lock_token(lock)? != lock.token {
                return Err(invalid_snapshot());
            }
        }
        if self.expired
            && (queue.active_len() != 0
                || !self.session_locks.is_empty()
                || self.first_used_at_ms.is_none())
        {
            return Err(invalid_snapshot());
        }
        if self.first_used_at_ms.is_some() != self.last_activity_at_ms.is_some()
            || self
                .first_used_at_ms
                .zip(self.last_activity_at_ms)
                .is_some_and(|(first, last)| first > last)
        {
            return Err(invalid_snapshot());
        }
        if let Some(token) = &self.dispatch.half_open_probe_token
            && (self.dispatch.circuit_state != QueueCircuitState::HalfOpen
                || token.len() > crate::MAX_FENCED_LEASE_TOKEN_BYTES
                || !queue.messages.values().any(|message| {
                    matches!(
                        &message.state,
                        QueueState::Leased {
                            token: live_token,
                            ..
                        } if live_token == token
                    )
                }))
        {
            return Err(invalid_snapshot());
        }
        let active_bytes = queue.active_bytes();
        if queue
            .config
            .advanced
            .as_ref()
            .and_then(|config| config.max_active_bytes)
            .is_some_and(|limit| active_bytes > limit)
        {
            return Err(invalid_snapshot());
        }
        Ok(())
    }

    pub(crate) fn note_activity(&mut self, now_ms: u64) {
        self.first_used_at_ms.get_or_insert(now_ms);
        self.last_activity_at_ms = Some(now_ms.max(self.last_activity_at_ms.unwrap_or(0)));
    }
}

impl Queue {
    pub fn enqueue_advanced(
        &mut self,
        ingress: QueueIngress,
        now_ms: u64,
    ) -> EpochResult<EnqueueReceipt> {
        self.enqueue_advanced_with_pending_forwards(ingress, now_ms, 0)
    }

    pub fn enqueue_advanced_with_pending_forwards(
        &mut self,
        ingress: QueueIngress,
        now_ms: u64,
        pending_dead_letter_forwards: usize,
    ) -> EpochResult<EnqueueReceipt> {
        let mut candidate = self.clone();
        let result =
            candidate.enqueue_advanced_inner(ingress, now_ms, pending_dead_letter_forwards)?;
        *self = candidate;
        Ok(result)
    }

    fn enqueue_advanced_inner(
        &mut self,
        ingress: QueueIngress,
        now_ms: u64,
        pending_dead_letter_forwards: usize,
    ) -> EpochResult<EnqueueReceipt> {
        ingress.validate()?;
        self.maintain_advanced_inner(now_ms, pending_dead_letter_forwards)?;
        self.ensure_not_expired()?;
        self.cleanup_dedupe(now_ms);
        if let Some(receipt) = self.duplicate_receipt(&ingress.envelope) {
            return Ok(receipt);
        }
        if self.messages.contains_key(&ingress.envelope.id) {
            return Err(EpochError::AlreadyExists(ingress.envelope.id));
        }
        let charged_bytes = ingress.charged_bytes()?;
        if charged_bytes > MAX_QUEUE_ACTIVE_BYTES {
            return Err(EpochError::Capacity(format!(
                "message charge is {charged_bytes} bytes; maximum is {MAX_QUEUE_ACTIVE_BYTES}"
            )));
        }
        self.make_admission_capacity(charged_bytes, now_ms)?;
        let metadata = QueueMessageMetadata {
            session_id: ingress.session_id,
            correlation_id: ingress.correlation_id,
            reply_to: ingress.reply_to,
            charged_bytes,
        };
        let receipt = self.enqueue_unchecked(ingress.envelope, now_ms)?;
        self.advanced
            .metadata
            .insert(receipt.message_id.clone(), metadata);
        self.advanced.note_activity(now_ms);
        Ok(receipt)
    }

    pub fn acquire_advanced(
        &mut self,
        consumer: &str,
        max_messages: usize,
        visibility_timeout_ms: Option<u64>,
        now_ms: u64,
        fence: LeaseFence,
    ) -> EpochResult<Vec<Delivery>> {
        self.acquire_advanced_with_pending_forwards(
            consumer,
            max_messages,
            visibility_timeout_ms,
            now_ms,
            fence,
            0,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "dispatch requires the lease fence plus the tablet outbox expiry barrier"
    )]
    pub fn acquire_advanced_with_pending_forwards(
        &mut self,
        consumer: &str,
        max_messages: usize,
        visibility_timeout_ms: Option<u64>,
        now_ms: u64,
        fence: LeaseFence,
        pending_dead_letter_forwards: usize,
    ) -> EpochResult<Vec<Delivery>> {
        let mut candidate = self.clone();
        candidate.maintain_advanced_inner(now_ms, pending_dead_letter_forwards)?;
        candidate.ensure_not_expired()?;
        let requested = candidate.dispatch_allowance(max_messages, now_ms)?;
        let ids = candidate.ordinary_candidates(now_ms, requested);
        let deliveries = candidate.acquire_selected_fenced(
            consumer,
            &ids,
            visibility_timeout_ms,
            now_ms,
            fence,
        )?;
        candidate.consume_dispatch(&deliveries)?;
        if !deliveries.is_empty() && candidate.advanced_enabled() {
            candidate.advanced.note_activity(now_ms);
        }
        *self = candidate;
        Ok(deliveries)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn acquire_session_fenced(
        &mut self,
        session_id: &str,
        consumer: &str,
        max_messages: usize,
        visibility_timeout_ms: Option<u64>,
        lock_token: Option<&str>,
        now_ms: u64,
        fence: LeaseFence,
    ) -> EpochResult<QueueSessionAcquisition> {
        self.acquire_session_fenced_with_pending_forwards(
            session_id,
            consumer,
            max_messages,
            visibility_timeout_ms,
            lock_token,
            now_ms,
            fence,
            0,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "session acquisition binds delivery, lock, fence, and tablet outbox expiry state"
    )]
    pub fn acquire_session_fenced_with_pending_forwards(
        &mut self,
        session_id: &str,
        consumer: &str,
        max_messages: usize,
        visibility_timeout_ms: Option<u64>,
        lock_token: Option<&str>,
        now_ms: u64,
        fence: LeaseFence,
        pending_dead_letter_forwards: usize,
    ) -> EpochResult<QueueSessionAcquisition> {
        validate_identifier("session ID", session_id)?;
        if consumer.trim().is_empty() {
            return Err(EpochError::InvalidArgument("consumer is required".into()));
        }
        let mut candidate = self.clone();
        candidate.maintain_advanced_inner(now_ms, pending_dead_letter_forwards)?;
        candidate.ensure_not_expired()?;
        let visibility = visibility_timeout_ms.unwrap_or(candidate.config.visibility_timeout_ms);
        if visibility == 0 {
            return Err(EpochError::InvalidArgument(
                "visibility timeout must be greater than zero".into(),
            ));
        }
        let lock = candidate.acquire_or_validate_session_lock(
            session_id, consumer, lock_token, now_ms, visibility, fence,
        )?;
        let requested = candidate.dispatch_allowance(max_messages, now_ms)?;
        let ids = candidate.session_candidates(session_id, requested);
        let deliveries = candidate.acquire_selected_fenced(
            consumer,
            &ids,
            visibility_timeout_ms,
            now_ms,
            fence,
        )?;
        candidate.consume_dispatch(&deliveries)?;
        candidate.advanced.note_activity(now_ms);
        let result = QueueSessionAcquisition {
            session_id: session_id.to_owned(),
            lock_token: lock.token,
            lock_deadline_ms: lock.deadline_ms,
            deliveries,
        };
        *self = candidate;
        Ok(result)
    }

    pub fn renew_session_lock_fenced(
        &mut self,
        lock_token: &str,
        extension_ms: u64,
        now_ms: u64,
        fence: LeaseFence,
    ) -> EpochResult<QueueSessionRenewal> {
        self.renew_session_lock_fenced_with_pending_forwards(
            lock_token,
            extension_ms,
            now_ms,
            fence,
            0,
        )
    }

    pub fn renew_session_lock_fenced_with_pending_forwards(
        &mut self,
        lock_token: &str,
        extension_ms: u64,
        now_ms: u64,
        fence: LeaseFence,
        pending_dead_letter_forwards: usize,
    ) -> EpochResult<QueueSessionRenewal> {
        if extension_ms == 0 {
            return Err(EpochError::InvalidArgument(
                "session lock extension must be greater than zero".into(),
            ));
        }
        let mut candidate = self.clone();
        candidate.maintain_advanced_inner(now_ms, pending_dead_letter_forwards)?;
        candidate.ensure_not_expired()?;
        let session_id = candidate
            .advanced
            .session_locks
            .iter()
            .find_map(|(id, lock)| (lock.token == lock_token).then_some(id.clone()))
            .ok_or(EpochError::Fenced)?;
        candidate.advanced.session_lock_generation = candidate
            .advanced
            .session_lock_generation
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("session lock generation is exhausted".into()))?;
        let generation = candidate.advanced.session_lock_generation;
        let lock = candidate
            .advanced
            .session_locks
            .get_mut(&session_id)
            .expect("session lock exists");
        if lock.fence != fence || lock.deadline_ms <= now_ms {
            return Err(EpochError::Fenced);
        }
        lock.deadline_ms = lock.deadline_ms.saturating_add(extension_ms);
        lock.generation = generation;
        lock.token = session_lock_token(lock)?;
        let result = QueueSessionRenewal {
            session_id,
            lock_token: lock.token.clone(),
            lock_deadline_ms: lock.deadline_ms,
        };
        candidate.advanced.note_activity(now_ms);
        *self = candidate;
        Ok(result)
    }

    pub fn release_session_lock_fenced(
        &mut self,
        lock_token: &str,
        fence: LeaseFence,
        now_ms: u64,
    ) -> EpochResult<()> {
        self.release_session_lock_fenced_with_pending_forwards(lock_token, fence, now_ms, 0)
    }

    pub fn release_session_lock_fenced_with_pending_forwards(
        &mut self,
        lock_token: &str,
        fence: LeaseFence,
        now_ms: u64,
        pending_dead_letter_forwards: usize,
    ) -> EpochResult<()> {
        let mut candidate = self.clone();
        candidate.maintain_advanced_inner(now_ms, pending_dead_letter_forwards)?;
        let session_id = candidate
            .advanced
            .session_locks
            .iter()
            .find_map(|(id, lock)| {
                (lock.token == lock_token && lock.fence == fence).then_some(id.clone())
            })
            .ok_or(EpochError::Fenced)?;
        candidate.advanced.session_locks.remove(&session_id);
        candidate.advanced.note_activity(now_ms);
        *self = candidate;
        Ok(())
    }

    pub fn defer_fenced(
        &mut self,
        lease_token: &str,
        fence: LeaseFence,
        reason: impl Into<String>,
        now_ms: u64,
    ) -> EpochResult<String> {
        self.defer_fenced_with_pending_forwards(lease_token, fence, reason, now_ms, 0)
    }

    pub fn defer_fenced_with_pending_forwards(
        &mut self,
        lease_token: &str,
        fence: LeaseFence,
        reason: impl Into<String>,
        now_ms: u64,
        pending_dead_letter_forwards: usize,
    ) -> EpochResult<String> {
        let reason = reason.into();
        validate_bounded_reason(&reason)?;
        let mut candidate = self.clone();
        candidate.maintain_advanced_inner(now_ms, pending_dead_letter_forwards)?;
        candidate.ensure_not_expired()?;
        let message_id = candidate.defer_live_fenced(lease_token, fence, &reason, now_ms)?;
        candidate.advanced.deferred.insert(
            message_id.clone(),
            QueueDeferredMessage {
                message_id: message_id.clone(),
                reason,
                deferred_at_ms: now_ms,
            },
        );
        candidate.advanced.note_activity(now_ms);
        *self = candidate;
        Ok(message_id)
    }

    pub fn receive_deferred_fenced(
        &mut self,
        message_id: &str,
        consumer: &str,
        visibility_timeout_ms: Option<u64>,
        now_ms: u64,
        fence: LeaseFence,
    ) -> EpochResult<Delivery> {
        self.receive_deferred_fenced_with_pending_forwards(
            message_id,
            consumer,
            visibility_timeout_ms,
            now_ms,
            fence,
            0,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact deferred receive requires the lease fence plus tablet outbox expiry state"
    )]
    pub fn receive_deferred_fenced_with_pending_forwards(
        &mut self,
        message_id: &str,
        consumer: &str,
        visibility_timeout_ms: Option<u64>,
        now_ms: u64,
        fence: LeaseFence,
        pending_dead_letter_forwards: usize,
    ) -> EpochResult<Delivery> {
        validate_identifier("message ID", message_id)?;
        let mut candidate = self.clone();
        candidate.maintain_advanced_inner(now_ms, pending_dead_letter_forwards)?;
        candidate.ensure_not_expired()?;
        if candidate
            .advanced
            .metadata
            .get(message_id)
            .and_then(|metadata| metadata.session_id.as_ref())
            .is_some()
        {
            return Err(EpochError::Conflict(
                "session messages require a session acquisition".into(),
            ));
        }
        if !candidate.advanced.deferred.contains_key(message_id) {
            return Err(EpochError::NotFound(message_id.to_owned()));
        }
        let allowance = candidate.dispatch_allowance(1, now_ms)?;
        if allowance == 0 {
            return Err(EpochError::Unavailable(
                "queue dispatch is currently gated".into(),
            ));
        }
        let mut deliveries = candidate.acquire_selected_fenced(
            consumer,
            &[message_id.to_owned()],
            visibility_timeout_ms,
            now_ms,
            fence,
        )?;
        let delivery = deliveries
            .pop()
            .ok_or_else(|| EpochError::Unavailable("deferred message is not ready".into()))?;
        candidate.advanced.deferred.remove(message_id);
        candidate.consume_dispatch(std::slice::from_ref(&delivery))?;
        candidate.advanced.note_activity(now_ms);
        *self = candidate;
        Ok(delivery)
    }

    pub fn lookup_correlation(&self, correlation_id: &str) -> Vec<QueueCorrelatedMessage> {
        let mut messages = self
            .advanced
            .metadata
            .iter()
            .filter_map(|(message_id, metadata)| {
                if metadata.correlation_id.as_deref() != Some(correlation_id) {
                    return None;
                }
                let message = self.messages.get(message_id)?;
                (!is_terminal(&message.state)).then(|| QueueCorrelatedMessage {
                    message: message.clone(),
                    metadata: metadata.clone(),
                })
            })
            .collect::<Vec<_>>();
        messages.sort_by(|left, right| {
            left.message
                .commit_position
                .cmp(&right.message.commit_position)
                .then_with(|| left.message.id.cmp(&right.message.id))
        });
        messages
    }

    pub fn message_metadata(&self, message_id: &str) -> Option<QueueMessageMetadata> {
        self.advanced.metadata.get(message_id).cloned()
    }

    pub fn advanced_observation(&self) -> QueueAdvancedObservation {
        QueueAdvancedObservation {
            expired: self.advanced.expired,
            active_bytes: self.active_bytes(),
            deferred_messages: self.advanced.deferred.len(),
            session_locks: self.advanced.session_locks.len(),
            circuit_state: self.advanced.dispatch.circuit_state,
            circuit_open_until_ms: self.advanced.dispatch.open_until_ms,
        }
    }

    pub fn maintain_advanced(
        &mut self,
        now_ms: u64,
        pending_dead_letter_forwards: usize,
    ) -> EpochResult<()> {
        let mut candidate = self.clone();
        candidate.maintain_advanced_inner(now_ms, pending_dead_letter_forwards)?;
        *self = candidate;
        Ok(())
    }

    pub fn record_dispatch_success(&mut self, now_ms: u64) -> EpochResult<()> {
        let Some(_policy) = self.advanced_config().dispatch.as_ref() else {
            return Ok(());
        };
        self.advanced.dispatch.consecutive_failures = 0;
        self.advanced.dispatch.circuit_state = QueueCircuitState::Closed;
        self.advanced.dispatch.open_until_ms = None;
        self.advanced.dispatch.half_open_probe_token = None;
        self.advanced.note_activity(now_ms);
        Ok(())
    }

    pub fn record_dispatch_failure(&mut self, now_ms: u64) -> EpochResult<()> {
        let Some(policy) = self.advanced_config().dispatch.clone() else {
            return Ok(());
        };
        self.advanced.dispatch.consecutive_failures = self
            .advanced
            .dispatch
            .consecutive_failures
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("dispatch failure counter is exhausted".into()))?;
        if self.advanced.dispatch.circuit_state == QueueCircuitState::HalfOpen
            || self.advanced.dispatch.consecutive_failures >= policy.failure_threshold
        {
            self.advanced.dispatch.circuit_state = QueueCircuitState::Open;
            self.advanced.dispatch.open_until_ms =
                Some(now_ms.saturating_add(policy.open_interval_ms));
            self.advanced.dispatch.half_open_probe_token = None;
        }
        self.advanced.note_activity(now_ms);
        Ok(())
    }

    pub(crate) fn advanced_state(&self) -> &QueueAdvancedState {
        &self.advanced
    }

    pub(crate) fn restore_advanced_state(
        &mut self,
        advanced: QueueAdvancedState,
    ) -> EpochResult<()> {
        advanced.validate(self)?;
        self.advanced = advanced;
        Ok(())
    }

    pub(crate) fn note_advanced_activity(&mut self, now_ms: u64) {
        if self.advanced_enabled() {
            self.advanced.note_activity(now_ms);
        }
        self.reconcile_advanced();
    }

    fn advanced_enabled(&self) -> bool {
        self.config.advanced.is_some()
            || !self.advanced.metadata.is_empty()
            || !self.advanced.deferred.is_empty()
            || !self.advanced.session_locks.is_empty()
    }

    pub(crate) fn reconcile_advanced(&mut self) {
        self.advanced.deferred.retain(|message_id, _| {
            self.messages
                .get(message_id)
                .is_some_and(|message| message.state == QueueState::Ready)
        });
        let live_sessions = self
            .advanced
            .metadata
            .values()
            .filter_map(|metadata| metadata.session_id.clone())
            .collect::<BTreeSet<_>>();
        self.advanced
            .session_locks
            .retain(|session_id, _| live_sessions.contains(session_id));
    }

    fn maintain_advanced_inner(
        &mut self,
        now_ms: u64,
        pending_dead_letter_forwards: usize,
    ) -> EpochResult<()> {
        let active_before = self.active_len();
        let session_locks_before = self.advanced.session_locks.len();
        self.maintain_fenced(now_ms)?;
        self.advanced
            .session_locks
            .retain(|_, lock| lock.deadline_ms > now_ms);
        self.reconcile_advanced();
        if self.advanced_enabled()
            && ((active_before > 0 && self.active_len() == 0)
                || (session_locks_before > 0 && self.advanced.session_locks.is_empty()))
        {
            self.advanced.note_activity(now_ms);
        }
        if let (Some(idle_expiry_ms), Some(last_activity_at_ms)) = (
            self.advanced_config().idle_expiry_ms,
            self.advanced.last_activity_at_ms,
        ) && self.active_len() == 0
            && self.advanced.session_locks.is_empty()
            && pending_dead_letter_forwards == 0
            && last_activity_at_ms.saturating_add(idle_expiry_ms) <= now_ms
        {
            self.advanced.expired = true;
        }
        Ok(())
    }

    fn make_admission_capacity(&mut self, charged_bytes: usize, now_ms: u64) -> EpochResult<()> {
        let advanced = self.advanced_config().clone();
        while self.active_len() >= self.config.max_messages
            || advanced
                .max_active_bytes
                .is_some_and(|limit| self.active_bytes().saturating_add(charged_bytes) > limit)
        {
            match advanced.overflow {
                QueueOverflowPolicy::RejectNew => {
                    return Err(EpochError::Capacity("queue admission limit reached".into()));
                }
                QueueOverflowPolicy::DropOldest | QueueOverflowPolicy::DeadLetterOldest => {}
            }
            let victim = self
                .order
                .iter()
                .filter_map(|id| self.messages.get(id))
                .filter(|message| {
                    !is_terminal(&message.state)
                        && !matches!(message.state, QueueState::Leased { .. })
                })
                .min_by(|left, right| {
                    left.commit_position
                        .cmp(&right.commit_position)
                        .then_with(|| left.id.cmp(&right.id))
                })
                .map(|message| message.id.clone())
                .ok_or_else(|| {
                    EpochError::Capacity("queue admission limit has no evictable message".into())
                })?;
            match advanced.overflow {
                QueueOverflowPolicy::DropOldest => {
                    let commit_position = self.next_fenced_commit_position()?;
                    self.messages
                        .get_mut(&victim)
                        .expect("overflow victim exists")
                        .state = QueueState::Expired;
                    self.commit_position = commit_position;
                }
                QueueOverflowPolicy::DeadLetterOldest => {
                    self.move_to_dead_letter_fenced(&victim, "overflow".into(), now_ms)?;
                }
                QueueOverflowPolicy::RejectNew => unreachable!("handled above"),
            }
            self.reconcile_advanced();
        }
        Ok(())
    }

    fn ordinary_candidates(&self, now_ms: u64, limit: usize) -> Vec<String> {
        let aging_interval = self.advanced_config().priority_aging_interval_ms;
        let mut candidates = self
            .order
            .iter()
            .filter_map(|id| {
                let message = self.messages.get(id)?;
                if message.state != QueueState::Ready
                    || self.advanced.deferred.contains_key(id)
                    || self
                        .advanced
                        .metadata
                        .get(id)
                        .and_then(|metadata| metadata.session_id.as_ref())
                        .is_some()
                {
                    return None;
                }
                let aged = aging_interval.map_or(0, |interval| {
                    now_ms.saturating_sub(message.enqueued_at_ms) / interval
                });
                let effective = u64::from(message.envelope.priority)
                    .saturating_add(aged)
                    .min(u64::from(u8::MAX));
                Some((id.clone(), effective, message.commit_position))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| left.2.cmp(&right.2))
                .then_with(|| left.0.cmp(&right.0))
        });
        candidates
            .into_iter()
            .take(limit)
            .map(|(id, _, _)| id)
            .collect()
    }

    fn session_candidates(&self, session_id: &str, limit: usize) -> Vec<String> {
        let mut candidates = self
            .order
            .iter()
            .filter_map(|id| {
                let message = self.messages.get(id)?;
                (message.state == QueueState::Ready
                    && !self.advanced.deferred.contains_key(id)
                    && self
                        .advanced
                        .metadata
                        .get(id)
                        .and_then(|metadata| metadata.session_id.as_deref())
                        == Some(session_id))
                .then_some((id.clone(), message.commit_position))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
        candidates
            .into_iter()
            .take(limit)
            .map(|(id, _)| id)
            .collect()
    }

    fn acquire_or_validate_session_lock(
        &mut self,
        session_id: &str,
        consumer: &str,
        supplied_token: Option<&str>,
        now_ms: u64,
        visibility_ms: u64,
        fence: LeaseFence,
    ) -> EpochResult<SessionLock> {
        fence.validate()?;
        if let Some(lock) = self.advanced.session_locks.get(session_id) {
            if supplied_token == Some(lock.token.as_str())
                && lock.consumer == consumer
                && lock.fence == fence
                && lock.deadline_ms > now_ms
            {
                return Ok(lock.clone());
            }
            return Err(EpochError::Conflict(format!(
                "session {session_id} is exclusively locked"
            )));
        }
        if supplied_token.is_some() {
            return Err(EpochError::Fenced);
        }
        if self.advanced.session_locks.len() >= MAX_QUEUE_SESSION_LOCKS {
            return Err(EpochError::Capacity(
                "queue session lock capacity reached".into(),
            ));
        }
        self.advanced.session_lock_generation = self
            .advanced
            .session_lock_generation
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("session lock generation is exhausted".into()))?;
        let mut lock = SessionLock {
            session_id: session_id.to_owned(),
            consumer: consumer.to_owned(),
            fence,
            generation: self.advanced.session_lock_generation,
            token: String::new(),
            deadline_ms: now_ms.saturating_add(visibility_ms),
        };
        lock.token = session_lock_token(&lock)?;
        self.advanced
            .session_locks
            .insert(session_id.to_owned(), lock.clone());
        Ok(lock)
    }

    fn dispatch_allowance(&mut self, requested: usize, now_ms: u64) -> EpochResult<usize> {
        let Some(policy) = self.advanced_config().dispatch.clone() else {
            return Ok(requested);
        };
        self.reconcile_half_open_probe();
        let state = &mut self.advanced.dispatch;
        if state.circuit_state == QueueCircuitState::Open {
            let open_until_ms = state.open_until_ms.unwrap_or(u64::MAX);
            if now_ms < open_until_ms {
                return Ok(0);
            }
            state.circuit_state = QueueCircuitState::HalfOpen;
            state.milli_tokens = state.milli_tokens.max(1_000);
            state.half_open_probe_token = None;
        }
        let capacity = u64::from(policy.burst).saturating_mul(1_000);
        match state.last_refill_ms {
            None => {
                state.milli_tokens = capacity;
                state.last_refill_ms = Some(now_ms);
            }
            Some(previous) => {
                let elapsed_ms = now_ms.saturating_sub(previous);
                let refill = elapsed_ms.saturating_mul(u64::from(policy.messages_per_second));
                state.milli_tokens = state.milli_tokens.saturating_add(refill).min(capacity);
                state.last_refill_ms = Some(now_ms.max(previous));
            }
        }
        let tokens = usize::try_from(state.milli_tokens / 1_000)
            .map_err(|_| EpochError::Capacity("dispatch token count exceeds usize".into()))?;
        let in_flight = self
            .messages
            .values()
            .filter(|message| matches!(message.state, QueueState::Leased { .. }))
            .count();
        let concurrency = usize::try_from(policy.max_in_flight)
            .unwrap_or(usize::MAX)
            .saturating_sub(in_flight);
        let circuit_limit = if state.circuit_state == QueueCircuitState::HalfOpen {
            usize::from(state.half_open_probe_token.is_none())
        } else {
            usize::MAX
        };
        Ok(requested.min(tokens).min(concurrency).min(circuit_limit))
    }

    fn consume_dispatch(&mut self, deliveries: &[Delivery]) -> EpochResult<()> {
        if self.advanced_config().dispatch.is_none() {
            return Ok(());
        }
        if self.advanced.dispatch.circuit_state == QueueCircuitState::HalfOpen
            && deliveries.len() > 1
        {
            return Err(EpochError::Internal(
                "half-open circuit admitted more than one probe".into(),
            ));
        }
        let count = u64::try_from(deliveries.len())
            .map_err(|_| EpochError::Capacity("dispatch batch is too large".into()))?;
        let charge = count
            .checked_mul(1_000)
            .ok_or_else(|| EpochError::Capacity("dispatch token charge overflowed".into()))?;
        self.advanced.dispatch.milli_tokens = self
            .advanced
            .dispatch
            .milli_tokens
            .checked_sub(charge)
            .ok_or_else(|| EpochError::Internal("dispatch token underflow".into()))?;
        if self.advanced.dispatch.circuit_state == QueueCircuitState::HalfOpen
            && let Some(delivery) = deliveries.first()
        {
            self.advanced.dispatch.half_open_probe_token = Some(delivery.lease_token.clone());
        }
        Ok(())
    }

    fn reconcile_half_open_probe(&mut self) {
        if self.advanced.dispatch.circuit_state != QueueCircuitState::HalfOpen {
            self.advanced.dispatch.half_open_probe_token = None;
            return;
        }
        let Some(token) = self.advanced.dispatch.half_open_probe_token.as_deref() else {
            return;
        };
        let probe_is_live = self.messages.values().any(|message| {
            matches!(
                &message.state,
                QueueState::Leased {
                    token: live_token,
                    ..
                } if live_token == token
            )
        });
        if !probe_is_live {
            self.advanced.dispatch.half_open_probe_token = None;
        }
    }

    fn ensure_not_expired(&self) -> EpochResult<()> {
        if self.advanced.expired {
            Err(EpochError::Unavailable("queue has expired".into()))
        } else {
            Ok(())
        }
    }

    fn advanced_config(&self) -> &QueueAdvancedConfig {
        self.config
            .advanced
            .as_ref()
            .unwrap_or(&DEFAULT_ADVANCED_CONFIG)
    }

    pub(crate) fn active_bytes(&self) -> usize {
        self.advanced
            .metadata
            .iter()
            .filter_map(|(id, metadata)| {
                self.messages
                    .get(id)
                    .filter(|message| !is_terminal(&message.state))
                    .map(|_| metadata.charged_bytes)
            })
            .sum()
    }

    pub(crate) fn next_advanced_maintenance_deadline_ms(
        &self,
        pending_dead_letter_forwards: usize,
    ) -> Option<u64> {
        let session_deadline = self
            .advanced
            .session_locks
            .values()
            .map(|lock| lock.deadline_ms)
            .min();
        let idle_deadline = if !self.advanced.expired
            && self.active_len() == 0
            && pending_dead_letter_forwards == 0
        {
            self.advanced_config()
                .idle_expiry_ms
                .zip(self.advanced.last_activity_at_ms)
                .map(|(idle_ms, last)| last.saturating_add(idle_ms))
        } else {
            None
        };
        session_deadline.into_iter().chain(idle_deadline).min()
    }
}

static DEFAULT_ADVANCED_CONFIG: QueueAdvancedConfig = QueueAdvancedConfig {
    max_active_bytes: None,
    overflow: QueueOverflowPolicy::RejectNew,
    idle_expiry_ms: None,
    priority_aging_interval_ms: None,
    dispatch: None,
    dead_letter_target: None,
};

fn session_lock_token(lock: &SessionLock) -> EpochResult<String> {
    let payload = serde_json::to_vec(&(
        &lock.session_id,
        &lock.consumer,
        lock.fence,
        lock.generation,
        lock.deadline_ms,
    ))
    .map_err(|error| EpochError::InvalidArgument(error.to_string()))?;
    let mut hasher = Hasher::new();
    hasher.update(SESSION_TOKEN_DOMAIN);
    hasher.update(&payload);
    let checksum = hasher.finalize();
    Ok(format!(
        "{SESSION_TOKEN_PREFIX}{}.{checksum:08x}",
        encode_hex(&payload)
    ))
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn validate_identifier(name: &str, value: &str) -> EpochResult<()> {
    if value.trim().is_empty()
        || value.len() > MAX_QUEUE_ADVANCED_IDENTIFIER_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(EpochError::InvalidArgument(format!(
            "{name} must contain 1-{MAX_QUEUE_ADVANCED_IDENTIFIER_BYTES} non-control UTF-8 bytes"
        )));
    }
    Ok(())
}

fn validate_bounded_reason(reason: &str) -> EpochResult<()> {
    if reason.trim().is_empty() || reason.len() > MAX_QUEUE_DEFER_REASON_BYTES {
        return Err(EpochError::InvalidArgument(format!(
            "defer reason must contain 1-{MAX_QUEUE_DEFER_REASON_BYTES} bytes"
        )));
    }
    Ok(())
}

fn is_terminal(state: &QueueState) -> bool {
    matches!(
        state,
        QueueState::Acknowledged | QueueState::Expired | QueueState::DeadLettered { .. }
    )
}

fn invalid_snapshot() -> EpochError {
    EpochError::InvalidArgument("Queue advanced snapshot state is invalid".into())
}
