//! Lease-based work queue state machine.
//!
//! Acquiring, acknowledging, extending, and retrying are explicit state
//! transitions. Lease tokens carry a generation so an expired or replaced
//! owner cannot acknowledge work after being fenced.

use std::collections::{HashMap, VecDeque};

use crc32fast::Hasher;
use epoch_core::{AckMetadata, DurabilityProfile, EpochError, EpochResult, EventEnvelope};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod lease;

#[cfg(test)]
use lease::FENCED_LEASE_TOKEN_PREFIX;
pub use lease::{
    FencedLeaseTokenMetadata, LEASE_FENCE_FORMAT_VERSION, LeaseFence, MAX_FENCED_LEASE_TOKEN_BYTES,
};

const RECOVERY_STATE_CHECKSUM_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BackoffStrategy {
    #[default]
    Exponential,
    Fixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub strategy: BackoffStrategy,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter_percent: u8,
    pub max_attempts: u32,
    pub max_age_ms: Option<u64>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            strategy: BackoffStrategy::Exponential,
            initial_delay_ms: 1_000,
            max_delay_ms: 60_000,
            jitter_percent: 10,
            max_attempts: 8,
            max_age_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueConfig {
    pub durability: DurabilityProfile,
    pub visibility_timeout_ms: u64,
    pub max_messages: usize,
    pub retry: RetryPolicy,
    pub dedupe_window_ms: Option<u64>,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            durability: DurabilityProfile::Volatile,
            visibility_timeout_ms: 30_000,
            max_messages: 100_000,
            retry: RetryPolicy::default(),
            dedupe_window_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueState {
    Ready,
    Scheduled {
        eligible_at_ms: u64,
    },
    Leased {
        consumer: String,
        token: String,
        deadline_ms: u64,
        generation: u64,
    },
    Acknowledged,
    Expired,
    DeadLettered {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueMessage {
    pub id: String,
    pub envelope: EventEnvelope,
    pub state: QueueState,
    pub enqueued_at_ms: u64,
    pub attempt: u32,
    pub last_error: Option<String>,
    pub commit_position: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnqueueReceipt {
    pub message_id: String,
    pub acknowledgement: AckMetadata,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delivery {
    pub message: QueueMessage,
    pub lease_token: String,
    pub lease_deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaseRenewal {
    pub lease_token: String,
    pub lease_deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub message_id: String,
    pub envelope: EventEnvelope,
    pub reason: String,
    pub original_enqueued_at_ms: u64,
    pub dead_lettered_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct DedupeEntry {
    message_id: String,
    expires_at_ms: u64,
    receipt: AckMetadata,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Queue {
    config: QueueConfig,
    messages: HashMap<String, QueueMessage>,
    order: VecDeque<String>,
    dead_letters: VecDeque<DeadLetter>,
    dedupe: HashMap<String, DedupeEntry>,
    commit_position: u64,
    lease_generation: u64,
}

impl Queue {
    pub fn new(config: QueueConfig) -> EpochResult<Self> {
        if config.visibility_timeout_ms == 0 {
            return Err(EpochError::InvalidArgument(
                "visibility timeout must be greater than zero".into(),
            ));
        }
        if config.max_messages == 0 {
            return Err(EpochError::InvalidArgument(
                "queue max_messages must be greater than zero".into(),
            ));
        }
        if config.retry.max_attempts == 0 {
            return Err(EpochError::InvalidArgument(
                "retry max_attempts must be greater than zero".into(),
            ));
        }
        if config.retry.jitter_percent > 100 {
            return Err(EpochError::InvalidArgument(
                "retry jitter_percent cannot exceed 100".into(),
            ));
        }
        Ok(Self {
            config,
            messages: HashMap::new(),
            order: VecDeque::new(),
            dead_letters: VecDeque::new(),
            dedupe: HashMap::new(),
            commit_position: 0,
            lease_generation: 0,
        })
    }

    pub fn config(&self) -> &QueueConfig {
        &self.config
    }

    /// Returns the deterministic checksum persisted after durable queue commands.
    ///
    /// The checksum is a replay-drift guard, not a cryptographic integrity or
    /// authentication primitive. Its canonical encoding is part of the engine
    /// journal compatibility contract.
    pub fn recovery_state_checksum(&self) -> u32 {
        let mut encoder = CanonicalRecoveryState::new();
        encoder.queue(self);
        encoder.finish()
    }

    pub fn enqueue(&mut self, envelope: EventEnvelope, now_ms: u64) -> EpochResult<EnqueueReceipt> {
        envelope.validate()?;
        self.cleanup_dedupe(now_ms);
        if let Some(dedupe_id) = &envelope.dedupe_id
            && let Some(original) = self.dedupe.get(dedupe_id)
        {
            let mut acknowledgement = original.receipt.clone();
            acknowledgement.duplicate = true;
            return Ok(EnqueueReceipt {
                message_id: original.message_id.clone(),
                acknowledgement,
            });
        }
        if self.active_len() >= self.config.max_messages {
            return Err(EpochError::Capacity("queue is full".into()));
        }
        if self.messages.contains_key(&envelope.id) {
            return Err(EpochError::AlreadyExists(envelope.id));
        }
        let state = envelope
            .deliver_at_ms
            .map_or(QueueState::Ready, |eligible_at_ms| {
                if eligible_at_ms <= now_ms {
                    QueueState::Ready
                } else {
                    QueueState::Scheduled { eligible_at_ms }
                }
            });
        self.commit_position = self.commit_position.saturating_add(1);
        let acknowledgement = AckMetadata::standalone(self.commit_position, self.config.durability);
        let message_id = envelope.id.clone();
        let dedupe_id = envelope.dedupe_id.clone();
        self.messages.insert(
            message_id.clone(),
            QueueMessage {
                id: message_id.clone(),
                envelope,
                state,
                enqueued_at_ms: now_ms,
                attempt: 0,
                last_error: None,
                commit_position: self.commit_position,
            },
        );
        self.order.push_back(message_id.clone());
        if let (Some(dedupe_id), Some(window)) = (dedupe_id, self.config.dedupe_window_ms) {
            self.dedupe.insert(
                dedupe_id,
                DedupeEntry {
                    message_id: message_id.clone(),
                    expires_at_ms: now_ms.saturating_add(window),
                    receipt: acknowledgement.clone(),
                },
            );
        }
        Ok(EnqueueReceipt {
            message_id,
            acknowledgement,
        })
    }

    pub fn acquire(
        &mut self,
        consumer: &str,
        max_messages: usize,
        visibility_timeout_ms: Option<u64>,
        now_ms: u64,
    ) -> EpochResult<Vec<Delivery>> {
        if consumer.is_empty() {
            return Err(EpochError::InvalidArgument("consumer is required".into()));
        }
        self.maintain(now_ms);
        let visibility = visibility_timeout_ms.unwrap_or(self.config.visibility_timeout_ms);
        if visibility == 0 {
            return Err(EpochError::InvalidArgument(
                "visibility timeout must be greater than zero".into(),
            ));
        }
        let candidates = self.lease_candidates();
        let mut deliveries = Vec::new();
        for (id, _, _) in candidates.into_iter().take(max_messages) {
            self.lease_generation = self.lease_generation.saturating_add(1);
            self.commit_position = self.commit_position.saturating_add(1);
            let generation = self.lease_generation;
            let token = format!("{id}.{generation}.{}", self.commit_position);
            let deadline_ms = now_ms.saturating_add(visibility);
            let message = self.messages.get_mut(&id).expect("candidate exists");
            message.attempt = message.attempt.saturating_add(1);
            message.state = QueueState::Leased {
                consumer: consumer.to_owned(),
                token: token.clone(),
                deadline_ms,
                generation,
            };
            deliveries.push(Delivery {
                message: message.clone(),
                lease_token: token,
                lease_deadline_ms: deadline_ms,
            });
        }
        Ok(deliveries)
    }

    /// Acquires Queue work using a token fenced by replicated ownership epochs.
    ///
    /// The returned token remains opaque to clients. Unlike `acquire`, its
    /// deterministic representation binds tablet id, tablet epoch, partition,
    /// leader term, consumer identity/epoch, message id, lease generation, and
    /// deadline.
    pub fn acquire_fenced(
        &mut self,
        consumer: &str,
        max_messages: usize,
        visibility_timeout_ms: Option<u64>,
        now_ms: u64,
        fence: LeaseFence,
    ) -> EpochResult<Vec<Delivery>> {
        fence.validate()?;
        if consumer.is_empty() {
            return Err(EpochError::InvalidArgument("consumer is required".into()));
        }
        self.maintain_fenced(now_ms)?;
        let visibility = visibility_timeout_ms.unwrap_or(self.config.visibility_timeout_ms);
        if visibility == 0 {
            return Err(EpochError::InvalidArgument(
                "visibility timeout must be greater than zero".into(),
            ));
        }
        let candidates: Vec<_> = self
            .lease_candidates()
            .into_iter()
            .take(max_messages)
            .collect();
        let acquisition_count = u64::try_from(candidates.len())
            .map_err(|_| EpochError::Capacity("fenced acquisition batch is too large".into()))?;
        self.lease_generation
            .checked_add(acquisition_count)
            .ok_or_else(|| EpochError::Capacity("queue lease generation is exhausted".into()))?;
        self.commit_position
            .checked_add(acquisition_count)
            .ok_or_else(|| EpochError::Capacity("queue commit position is exhausted".into()))?;

        let mut generation = self.lease_generation;
        let mut commit_position = self.commit_position;
        let mut planned = Vec::with_capacity(candidates.len());
        for (id, _, _) in candidates {
            generation = generation.checked_add(1).ok_or_else(|| {
                EpochError::Capacity("queue lease generation is exhausted".into())
            })?;
            commit_position = commit_position
                .checked_add(1)
                .ok_or_else(|| EpochError::Capacity("queue commit position is exhausted".into()))?;
            let requested_deadline_ms = now_ms.saturating_add(visibility);
            let deadline_ms = self
                .messages
                .get(&id)
                .and_then(|message| self.terminal_deadline_ms(message))
                .map_or(requested_deadline_ms, |bound| {
                    requested_deadline_ms.min(bound)
                });
            if deadline_ms <= now_ms {
                return Err(EpochError::Unavailable(
                    "message reached its terminal deadline before fenced acquisition".into(),
                ));
            }
            let token = FencedLeaseTokenMetadata::new(
                fence,
                consumer.to_owned(),
                id.clone(),
                generation,
                deadline_ms,
            )?
            .encode()?;
            planned.push((id, generation, commit_position, deadline_ms, token));
        }

        let mut deliveries = Vec::with_capacity(planned.len());
        for (id, generation, commit_position, deadline_ms, token) in planned {
            self.lease_generation = generation;
            self.commit_position = commit_position;
            let message = self.messages.get_mut(&id).expect("candidate exists");
            message.attempt = message.attempt.saturating_add(1);
            message.state = QueueState::Leased {
                consumer: consumer.to_owned(),
                token: token.clone(),
                deadline_ms,
                generation,
            };
            deliveries.push(Delivery {
                message: message.clone(),
                lease_token: token,
                lease_deadline_ms: deadline_ms,
            });
        }
        Ok(deliveries)
    }

    fn lease_candidates(&self) -> Vec<(String, u8, u64)> {
        let mut candidates: Vec<_> = self
            .order
            .iter()
            .filter_map(|id| {
                self.messages.get(id).and_then(|message| {
                    (message.state == QueueState::Ready).then_some((
                        id.clone(),
                        message.envelope.priority,
                        message.commit_position,
                    ))
                })
            })
            .collect();
        candidates.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.2.cmp(&right.2)));
        candidates
    }

    /// Parses a fenced token and proves that it names the Queue's current lease.
    pub fn fenced_lease_metadata(&self, token: &str) -> EpochResult<FencedLeaseTokenMetadata> {
        let metadata = FencedLeaseTokenMetadata::parse(token)?;
        let message = self
            .messages
            .get(metadata.message_id())
            .ok_or(EpochError::Fenced)?;
        match &message.state {
            QueueState::Leased {
                consumer,
                token: live_token,
                deadline_ms,
                generation,
                ..
            } if live_token == token
                && consumer == metadata.consumer()
                && *deadline_ms == metadata.lease_deadline_ms
                && *generation == metadata.lease_generation =>
            {
                Ok(metadata)
            }
            _ => Err(EpochError::Fenced),
        }
    }

    pub fn acknowledge(&mut self, token: &str, now_ms: u64) -> EpochResult<AckMetadata> {
        self.reject_live_fenced_token_on_legacy_path(token)?;
        self.maintain(now_ms);
        let id = self.message_id_for_live_legacy_token(token)?;
        self.commit_position = self.commit_position.saturating_add(1);
        self.messages.get_mut(&id).expect("token resolved").state = QueueState::Acknowledged;
        Ok(AckMetadata::standalone(
            self.commit_position,
            self.config.durability,
        ))
    }

    pub fn acknowledge_fenced(
        &mut self,
        token: &str,
        expected_fence: LeaseFence,
        now_ms: u64,
    ) -> EpochResult<AckMetadata> {
        expected_fence.validate()?;
        Self::validate_expected_fence_metadata(token, expected_fence)?;
        self.maintain_fenced(now_ms)?;
        let (id, _) = self.message_id_for_expected_fence(token, expected_fence)?;
        let commit_position = self.next_fenced_commit_position()?;
        self.messages.get_mut(&id).expect("token resolved").state = QueueState::Acknowledged;
        self.commit_position = commit_position;
        Ok(AckMetadata {
            durability: self.config.durability,
            resource_epoch: expected_fence.tablet_epoch(),
            commit_position,
            replica_acks: 1,
            duplicate: false,
        })
    }

    pub fn extend_lease(
        &mut self,
        token: &str,
        extension_ms: u64,
        now_ms: u64,
    ) -> EpochResult<u64> {
        if extension_ms == 0 {
            return Err(EpochError::InvalidArgument(
                "lease extension must be greater than zero".into(),
            ));
        }
        self.reject_live_fenced_token_on_legacy_path(token)?;
        self.maintain(now_ms);
        let id = self.message_id_for_live_legacy_token(token)?;
        let message = self.messages.get_mut(&id).expect("token resolved");
        let QueueState::Leased { deadline_ms, .. } = &mut message.state else {
            return Err(EpochError::Fenced);
        };
        *deadline_ms = now_ms.saturating_add(extension_ms);
        self.commit_position = self.commit_position.saturating_add(1);
        Ok(*deadline_ms)
    }

    /// Renews a fenced lease without crossing its TTL or retry max-age boundary.
    ///
    /// The renewed token replaces the old token so its embedded deadline stays
    /// truthful. Retrying after a lost response is provided by the containing
    /// Queue-tablet command's idempotency key, not by token rotation itself.
    pub fn extend_lease_fenced_bounded(
        &mut self,
        token: &str,
        expected_fence: LeaseFence,
        extension_ms: u64,
        now_ms: u64,
    ) -> EpochResult<LeaseRenewal> {
        if extension_ms == 0 {
            return Err(EpochError::InvalidArgument(
                "lease extension must be greater than zero".into(),
            ));
        }
        expected_fence.validate()?;
        Self::validate_expected_fence_metadata(token, expected_fence)?;
        self.maintain_fenced(now_ms)?;
        let (id, metadata) = self.message_id_for_expected_fence(token, expected_fence)?;
        let terminal_deadline_ms = self
            .messages
            .get(&id)
            .and_then(|message| self.terminal_deadline_ms(message));
        let requested_deadline_ms = now_ms.saturating_add(extension_ms);
        let deadline_ms = terminal_deadline_ms.map_or(requested_deadline_ms, |bound| {
            requested_deadline_ms.min(bound)
        });
        if deadline_ms <= metadata.lease_deadline_ms {
            return Err(EpochError::Conflict(
                "bounded lease extension must produce a later deadline".into(),
            ));
        }
        let renewed_token = FencedLeaseTokenMetadata::new(
            metadata.fence,
            metadata.consumer,
            id.clone(),
            metadata.lease_generation,
            deadline_ms,
        )?
        .encode()?;
        let commit_position = self.next_fenced_commit_position()?;
        let message = self.messages.get_mut(&id).expect("token resolved");
        let QueueState::Leased {
            token: live_token,
            deadline_ms: live_deadline_ms,
            ..
        } = &mut message.state
        else {
            return Err(EpochError::Fenced);
        };
        live_token.clone_from(&renewed_token);
        *live_deadline_ms = deadline_ms;
        self.commit_position = commit_position;
        Ok(LeaseRenewal {
            lease_token: renewed_token,
            lease_deadline_ms: deadline_ms,
        })
    }

    pub fn release(
        &mut self,
        token: &str,
        delay_ms: u64,
        reason: Option<String>,
        now_ms: u64,
    ) -> EpochResult<()> {
        self.reject_live_fenced_token_on_legacy_path(token)?;
        self.maintain(now_ms);
        let id = self.message_id_for_live_legacy_token(token)?;
        self.retry_or_dead_letter(&id, delay_ms, reason, now_ms);
        Ok(())
    }

    pub fn release_fenced(
        &mut self,
        token: &str,
        expected_fence: LeaseFence,
        delay_ms: u64,
        reason: Option<String>,
        now_ms: u64,
    ) -> EpochResult<()> {
        expected_fence.validate()?;
        Self::validate_expected_fence_metadata(token, expected_fence)?;
        self.maintain_fenced(now_ms)?;
        let (id, _) = self.message_id_for_expected_fence(token, expected_fence)?;
        self.retry_or_dead_letter_fenced(&id, delay_ms, reason, now_ms)
    }

    /// Negatively acknowledges a delivery and applies configured retry policy.
    pub fn nack(&mut self, token: &str, reason: impl Into<String>, now_ms: u64) -> EpochResult<()> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(EpochError::InvalidArgument(
                "nack reason is required".into(),
            ));
        }
        self.reject_live_fenced_token_on_legacy_path(token)?;
        self.maintain(now_ms);
        let id = self.message_id_for_live_legacy_token(token)?;
        let delay_ms = self.retry_delay_for(&id);
        self.retry_or_dead_letter(&id, delay_ms, Some(reason), now_ms);
        Ok(())
    }

    pub fn nack_fenced(
        &mut self,
        token: &str,
        expected_fence: LeaseFence,
        reason: impl Into<String>,
        now_ms: u64,
    ) -> EpochResult<()> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(EpochError::InvalidArgument(
                "nack reason is required".into(),
            ));
        }
        expected_fence.validate()?;
        Self::validate_expected_fence_metadata(token, expected_fence)?;
        self.maintain_fenced(now_ms)?;
        let (id, _) = self.message_id_for_expected_fence(token, expected_fence)?;
        let delay_ms = self.retry_delay_for(&id);
        self.retry_or_dead_letter_fenced(&id, delay_ms, Some(reason), now_ms)
    }

    pub fn reject(
        &mut self,
        token: &str,
        reason: impl Into<String>,
        now_ms: u64,
    ) -> EpochResult<()> {
        self.reject_live_fenced_token_on_legacy_path(token)?;
        self.maintain(now_ms);
        let id = self.message_id_for_live_legacy_token(token)?;
        self.move_to_dead_letter(&id, reason.into(), now_ms);
        Ok(())
    }

    pub fn reject_fenced(
        &mut self,
        token: &str,
        expected_fence: LeaseFence,
        reason: impl Into<String>,
        now_ms: u64,
    ) -> EpochResult<()> {
        expected_fence.validate()?;
        Self::validate_expected_fence_metadata(token, expected_fence)?;
        self.maintain_fenced(now_ms)?;
        let (id, _) = self.message_id_for_expected_fence(token, expected_fence)?;
        self.move_to_dead_letter_fenced(&id, reason.into(), now_ms)
    }

    pub fn get(&self, message_id: &str) -> Option<QueueMessage> {
        self.messages.get(message_id).cloned()
    }

    pub fn dead_letters(&self, limit: usize) -> Vec<DeadLetter> {
        self.dead_letters.iter().take(limit).cloned().collect()
    }

    /// Returns the active dead-letter evidence for one message, if any.
    pub fn dead_letter(&self, message_id: &str) -> Option<DeadLetter> {
        if !self
            .messages
            .get(message_id)
            .is_some_and(|message| matches!(message.state, QueueState::DeadLettered { .. }))
        {
            return None;
        }
        self.dead_letters
            .iter()
            .rev()
            .find(|letter| letter.message_id == message_id)
            .cloned()
    }

    pub fn redrive(&mut self, message_id: &str, now_ms: u64) -> EpochResult<()> {
        let index = self
            .dead_letters
            .iter()
            .position(|letter| letter.message_id == message_id)
            .ok_or_else(|| EpochError::NotFound(message_id.to_owned()))?;
        self.dead_letters.remove(index);
        let message = self
            .messages
            .get_mut(message_id)
            .ok_or_else(|| EpochError::NotFound(message_id.to_owned()))?;
        message.attempt = 0;
        message.last_error = None;
        message.state = QueueState::Ready;
        self.commit_position = self.commit_position.saturating_add(1);
        message.commit_position = self.commit_position;
        self.order.retain(|candidate| candidate != message_id);
        self.order.push_back(message_id.to_owned());
        let _ = now_ms;
        Ok(())
    }

    pub fn maintain(&mut self, now_ms: u64) {
        self.cleanup_dedupe(now_ms);
        let ids: Vec<String> = self.order.iter().cloned().collect();
        for id in ids {
            let action = self.messages.get(&id).and_then(|message| {
                let ttl_expired = message
                    .envelope
                    .ttl_ms
                    .is_some_and(|ttl| message.enqueued_at_ms.saturating_add(ttl) <= now_ms);
                if ttl_expired {
                    return Some(MaintenanceAction::Expire);
                }
                match message.state {
                    QueueState::Scheduled { eligible_at_ms } if eligible_at_ms <= now_ms => {
                        Some(MaintenanceAction::Ready)
                    }
                    QueueState::Leased { deadline_ms, .. } if deadline_ms <= now_ms => {
                        Some(MaintenanceAction::LeaseExpired)
                    }
                    _ => None,
                }
            });
            match action {
                Some(MaintenanceAction::Expire) => {
                    if let Some(message) = self.messages.get_mut(&id) {
                        message.state = QueueState::Expired;
                    }
                    self.commit_position = self.commit_position.saturating_add(1);
                }
                Some(MaintenanceAction::Ready) => {
                    if let Some(message) = self.messages.get_mut(&id) {
                        message.state = QueueState::Ready;
                    }
                }
                Some(MaintenanceAction::LeaseExpired) => {
                    let delay = self.retry_delay_for(&id);
                    self.retry_or_dead_letter(
                        &id,
                        delay,
                        Some("visibility_timeout".into()),
                        now_ms,
                    );
                }
                None => {}
            }
        }
    }

    /// Applies maintenance for replicated Queue commands, including max age.
    ///
    /// Standalone v1 replay continues to use `maintain`, whose historical
    /// transition behavior is intentionally unchanged.
    pub fn maintain_fenced(&mut self, now_ms: u64) -> EpochResult<()> {
        let actions: Vec<_> = self
            .order
            .iter()
            .filter_map(|id| {
                self.messages.get(id).and_then(|message| {
                    if matches!(
                        message.state,
                        QueueState::Acknowledged
                            | QueueState::Expired
                            | QueueState::DeadLettered { .. }
                    ) {
                        return None;
                    }
                    let ttl_expired = message
                        .envelope
                        .ttl_ms
                        .is_some_and(|ttl| message.enqueued_at_ms.saturating_add(ttl) <= now_ms);
                    if ttl_expired {
                        return Some((id.clone(), MaintenanceAction::Expire));
                    }
                    let max_age_expired = self.config.retry.max_age_ms.is_some_and(|max_age| {
                        message.enqueued_at_ms.saturating_add(max_age) <= now_ms
                    });
                    if max_age_expired {
                        return Some((id.clone(), MaintenanceAction::Expire));
                    }
                    match message.state {
                        QueueState::Scheduled { eligible_at_ms } if eligible_at_ms <= now_ms => {
                            Some((id.clone(), MaintenanceAction::Ready))
                        }
                        QueueState::Leased { deadline_ms, .. } if deadline_ms <= now_ms => {
                            Some((id.clone(), MaintenanceAction::LeaseExpired))
                        }
                        _ => None,
                    }
                })
            })
            .collect();
        let committed_action_count = actions
            .iter()
            .filter(|(_, action)| *action != MaintenanceAction::Ready)
            .count();
        let committed_action_count = u64::try_from(committed_action_count)
            .map_err(|_| EpochError::Capacity("fenced maintenance batch is too large".into()))?;
        self.commit_position
            .checked_add(committed_action_count)
            .ok_or_else(|| EpochError::Capacity("queue commit position is exhausted".into()))?;

        self.cleanup_dedupe(now_ms);
        for (id, action) in actions {
            match action {
                MaintenanceAction::Expire => {
                    let commit_position = self.next_fenced_commit_position()?;
                    if let Some(message) = self.messages.get_mut(&id) {
                        message.state = QueueState::Expired;
                    }
                    self.commit_position = commit_position;
                }
                MaintenanceAction::Ready => {
                    if let Some(message) = self.messages.get_mut(&id) {
                        message.state = QueueState::Ready;
                    }
                }
                MaintenanceAction::LeaseExpired => {
                    let delay = self.retry_delay_for(&id);
                    self.retry_or_dead_letter_fenced(
                        &id,
                        delay,
                        Some("visibility_timeout".into()),
                        now_ms,
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn counts(&self) -> QueueCounts {
        let mut counts = QueueCounts::default();
        for message in self.messages.values() {
            match message.state {
                QueueState::Ready => counts.ready += 1,
                QueueState::Scheduled { .. } => counts.scheduled += 1,
                QueueState::Leased { .. } => counts.in_flight += 1,
                QueueState::Acknowledged => counts.acknowledged += 1,
                QueueState::Expired => counts.expired += 1,
                QueueState::DeadLettered { .. } => counts.dead_lettered += 1,
            }
        }
        counts
    }

    fn active_len(&self) -> usize {
        self.messages
            .values()
            .filter(|message| {
                !matches!(
                    message.state,
                    QueueState::Acknowledged
                        | QueueState::Expired
                        | QueueState::DeadLettered { .. }
                )
            })
            .count()
    }

    fn message_id_for_live_token(&self, token: &str) -> EpochResult<String> {
        self.messages
            .iter()
            .find_map(|(id, message)| match &message.state {
                QueueState::Leased {
                    token: live_token, ..
                } if live_token == token => Some(id.clone()),
                _ => None,
            })
            .ok_or(EpochError::Fenced)
    }

    fn reject_live_fenced_token_on_legacy_path(&self, token: &str) -> EpochResult<()> {
        if self.message_id_for_live_token(token).is_ok()
            && FencedLeaseTokenMetadata::parse(token).is_ok()
        {
            Err(EpochError::Fenced)
        } else {
            Ok(())
        }
    }

    fn message_id_for_live_legacy_token(&self, token: &str) -> EpochResult<String> {
        let id = self.message_id_for_live_token(token)?;
        if FencedLeaseTokenMetadata::parse(token).is_ok() {
            Err(EpochError::Fenced)
        } else {
            Ok(id)
        }
    }

    fn message_id_for_expected_fence(
        &self,
        token: &str,
        expected_fence: LeaseFence,
    ) -> EpochResult<(String, FencedLeaseTokenMetadata)> {
        let metadata = self
            .fenced_lease_metadata(token)
            .map_err(|_| EpochError::Fenced)?;
        if metadata.fence != expected_fence {
            return Err(EpochError::Fenced);
        }
        Ok((metadata.message_id.clone(), metadata))
    }

    fn validate_expected_fence_metadata(
        token: &str,
        expected_fence: LeaseFence,
    ) -> EpochResult<()> {
        let metadata = FencedLeaseTokenMetadata::parse(token).map_err(|_| EpochError::Fenced)?;
        if metadata.fence == expected_fence {
            Ok(())
        } else {
            Err(EpochError::Fenced)
        }
    }

    fn next_fenced_commit_position(&self) -> EpochResult<u64> {
        self.commit_position
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("queue commit position is exhausted".into()))
    }

    fn retry_or_dead_letter(
        &mut self,
        id: &str,
        delay_ms: u64,
        reason: Option<String>,
        now_ms: u64,
    ) {
        let should_dead_letter =
            self.messages.get(id).is_some_and(|message| {
                message.attempt >= self.config.retry.max_attempts
                    || self.config.retry.max_age_ms.is_some_and(|max_age| {
                        now_ms.saturating_sub(message.enqueued_at_ms) >= max_age
                    })
            });
        if should_dead_letter {
            self.move_to_dead_letter(
                id,
                reason.unwrap_or_else(|| "retry_exhausted".into()),
                now_ms,
            );
            return;
        }
        if let Some(message) = self.messages.get_mut(id) {
            message.last_error = reason;
            message.state = if delay_ms == 0 {
                QueueState::Ready
            } else {
                QueueState::Scheduled {
                    eligible_at_ms: now_ms.saturating_add(delay_ms),
                }
            };
        }
        self.commit_position = self.commit_position.saturating_add(1);
    }

    fn retry_or_dead_letter_fenced(
        &mut self,
        id: &str,
        delay_ms: u64,
        reason: Option<String>,
        now_ms: u64,
    ) -> EpochResult<()> {
        let should_dead_letter =
            self.messages.get(id).is_some_and(|message| {
                message.attempt >= self.config.retry.max_attempts
                    || self.config.retry.max_age_ms.is_some_and(|max_age| {
                        now_ms.saturating_sub(message.enqueued_at_ms) >= max_age
                    })
            });
        if should_dead_letter {
            return self.move_to_dead_letter_fenced(
                id,
                reason.unwrap_or_else(|| "retry_exhausted".into()),
                now_ms,
            );
        }
        let commit_position = self.next_fenced_commit_position()?;
        let message = self
            .messages
            .get_mut(id)
            .ok_or_else(|| EpochError::NotFound(id.to_owned()))?;
        message.last_error = reason;
        message.state = if delay_ms == 0 {
            QueueState::Ready
        } else {
            QueueState::Scheduled {
                eligible_at_ms: now_ms.saturating_add(delay_ms),
            }
        };
        self.commit_position = commit_position;
        Ok(())
    }

    fn move_to_dead_letter(&mut self, id: &str, reason: String, now_ms: u64) {
        if let Some(message) = self.messages.get_mut(id) {
            message.state = QueueState::DeadLettered {
                reason: reason.clone(),
            };
            message.last_error = Some(reason.clone());
            self.dead_letters.push_back(DeadLetter {
                message_id: id.to_owned(),
                envelope: message.envelope.clone(),
                reason,
                original_enqueued_at_ms: message.enqueued_at_ms,
                dead_lettered_at_ms: now_ms,
                attempts: message.attempt,
                last_error: message.last_error.clone(),
            });
            self.commit_position = self.commit_position.saturating_add(1);
        }
    }

    fn move_to_dead_letter_fenced(
        &mut self,
        id: &str,
        reason: String,
        now_ms: u64,
    ) -> EpochResult<()> {
        let commit_position = self.next_fenced_commit_position()?;
        let message = self
            .messages
            .get_mut(id)
            .ok_or_else(|| EpochError::NotFound(id.to_owned()))?;
        message.state = QueueState::DeadLettered {
            reason: reason.clone(),
        };
        message.last_error = Some(reason.clone());
        self.dead_letters.push_back(DeadLetter {
            message_id: id.to_owned(),
            envelope: message.envelope.clone(),
            reason,
            original_enqueued_at_ms: message.enqueued_at_ms,
            dead_lettered_at_ms: now_ms,
            attempts: message.attempt,
            last_error: message.last_error.clone(),
        });
        self.commit_position = commit_position;
        Ok(())
    }

    fn retry_delay_for(&self, id: &str) -> u64 {
        let attempt = self
            .messages
            .get(id)
            .map_or(1, |message| message.attempt.max(1));
        let base = match self.config.retry.strategy {
            BackoffStrategy::Fixed => self.config.retry.initial_delay_ms,
            BackoffStrategy::Exponential => self
                .config
                .retry
                .initial_delay_ms
                .saturating_mul(2_u64.saturating_pow(attempt.saturating_sub(1).min(63))),
        }
        .min(self.config.retry.max_delay_ms);
        if self.config.retry.jitter_percent == 0 {
            return base;
        }
        let hash = id.bytes().fold(0_u64, |value, byte| {
            value.wrapping_mul(31).wrapping_add(u64::from(byte))
        });
        let span = base.saturating_mul(u64::from(self.config.retry.jitter_percent)) / 100;
        if span == 0 {
            base
        } else {
            base.saturating_sub(span)
                .saturating_add(hash % span.saturating_mul(2).saturating_add(1))
        }
    }

    fn cleanup_dedupe(&mut self, now_ms: u64) {
        self.dedupe.retain(|_, entry| entry.expires_at_ms > now_ms);
    }

    fn terminal_deadline_ms(&self, message: &QueueMessage) -> Option<u64> {
        let ttl_deadline_ms = message
            .envelope
            .ttl_ms
            .map(|ttl_ms| message.enqueued_at_ms.saturating_add(ttl_ms));
        let max_age_deadline_ms = self
            .config
            .retry
            .max_age_ms
            .map(|max_age_ms| message.enqueued_at_ms.saturating_add(max_age_ms));
        match (ttl_deadline_ms, max_age_deadline_ms) {
            (Some(ttl), Some(max_age)) => Some(ttl.min(max_age)),
            (Some(ttl), None) => Some(ttl),
            (None, Some(max_age)) => Some(max_age),
            (None, None) => None,
        }
    }
}

struct CanonicalRecoveryState {
    hasher: Hasher,
}

impl std::fmt::Debug for CanonicalRecoveryState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CanonicalRecoveryState")
            .finish_non_exhaustive()
    }
}

impl CanonicalRecoveryState {
    fn new() -> Self {
        let mut encoder = Self {
            hasher: Hasher::new(),
        };
        encoder.string("epoch.queue.recovery-state");
        encoder.u16(RECOVERY_STATE_CHECKSUM_VERSION);
        encoder
    }

    fn finish(self) -> u32 {
        self.hasher.finalize()
    }

    fn byte(&mut self, value: u8) {
        self.hasher.update(&[value]);
    }

    fn boolean(&mut self, value: bool) {
        self.byte(u8::from(value));
    }

    fn u16(&mut self, value: u16) {
        self.hasher.update(&value.to_le_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.hasher.update(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.hasher.update(&value.to_le_bytes());
    }

    fn usize(&mut self, value: usize) {
        let value = u64::try_from(value).expect("supported queue sizes fit in u64");
        self.u64(value);
    }

    fn string(&mut self, value: &str) {
        self.usize(value.len());
        self.hasher.update(value.as_bytes());
    }

    fn optional_string(&mut self, value: Option<&str>) {
        match value {
            Some(value) => {
                self.byte(1);
                self.string(value);
            }
            None => self.byte(0),
        }
    }

    fn optional_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.byte(1);
                self.u64(value);
            }
            None => self.byte(0),
        }
    }

    fn durability(&mut self, durability: DurabilityProfile) {
        self.byte(match durability {
            DurabilityProfile::Volatile => 0,
            DurabilityProfile::ReplicatedMemory => 1,
            DurabilityProfile::LocalDurable => 2,
            DurabilityProfile::QuorumDurable => 3,
            DurabilityProfile::GeoAsync => 4,
            DurabilityProfile::GeoSync => 5,
        });
    }

    fn retry_strategy(&mut self, strategy: BackoffStrategy) {
        self.byte(match strategy {
            BackoffStrategy::Exponential => 0,
            BackoffStrategy::Fixed => 1,
        });
    }

    fn config(&mut self, config: &QueueConfig) {
        self.durability(config.durability);
        self.u64(config.visibility_timeout_ms);
        self.usize(config.max_messages);
        self.retry_strategy(config.retry.strategy);
        self.u64(config.retry.initial_delay_ms);
        self.u64(config.retry.max_delay_ms);
        self.byte(config.retry.jitter_percent);
        self.u32(config.retry.max_attempts);
        self.optional_u64(config.retry.max_age_ms);
        self.optional_u64(config.dedupe_window_ms);
    }

    fn queue(&mut self, queue: &Queue) {
        self.config(&queue.config);

        let mut messages: Vec<_> = queue.messages.iter().collect();
        messages.sort_unstable_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
        self.usize(messages.len());
        for (key, message) in messages {
            self.string(key);
            self.message(message);
        }

        self.usize(queue.order.len());
        for message_id in &queue.order {
            self.string(message_id);
        }

        self.usize(queue.dead_letters.len());
        for dead_letter in &queue.dead_letters {
            self.dead_letter(dead_letter);
        }

        let mut dedupe: Vec<_> = queue.dedupe.iter().collect();
        dedupe.sort_unstable_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
        self.usize(dedupe.len());
        for (key, entry) in dedupe {
            self.string(key);
            self.dedupe_entry(entry);
        }

        self.u64(queue.commit_position);
        self.u64(queue.lease_generation);
    }

    fn message(&mut self, message: &QueueMessage) {
        self.string(&message.id);
        self.envelope(&message.envelope);
        self.queue_state(&message.state);
        self.u64(message.enqueued_at_ms);
        self.u32(message.attempt);
        self.optional_string(message.last_error.as_deref());
        self.u64(message.commit_position);
    }

    fn queue_state(&mut self, state: &QueueState) {
        match state {
            QueueState::Ready => self.byte(0),
            QueueState::Scheduled { eligible_at_ms } => {
                self.byte(1);
                self.u64(*eligible_at_ms);
            }
            QueueState::Leased {
                consumer,
                token,
                deadline_ms,
                generation,
            } => {
                self.byte(2);
                self.string(consumer);
                self.string(token);
                self.u64(*deadline_ms);
                self.u64(*generation);
            }
            QueueState::Acknowledged => self.byte(3),
            QueueState::Expired => self.byte(4),
            QueueState::DeadLettered { reason } => {
                self.byte(5);
                self.string(reason);
            }
        }
    }

    fn envelope(&mut self, envelope: &EventEnvelope) {
        self.string(&envelope.id);
        self.string(&envelope.source);
        self.string(&envelope.event_type);
        self.optional_string(envelope.subject.as_deref());
        self.u64(envelope.time_ms);
        self.optional_string(envelope.key.as_deref());

        self.usize(envelope.headers.len());
        for (key, value) in &envelope.headers {
            self.string(key);
            self.string(value);
        }

        self.string(&envelope.content_type);
        self.optional_string(envelope.schema_ref.as_deref());
        self.optional_string(envelope.traceparent.as_deref());
        self.json(&envelope.payload);
        self.optional_u64(envelope.deliver_at_ms);
        self.optional_u64(envelope.ttl_ms);
        self.byte(envelope.priority);
        self.optional_string(envelope.dedupe_id.as_deref());
        self.optional_string(envelope.transaction_id.as_deref());

        self.usize(envelope.extensions.len());
        for (key, value) in &envelope.extensions {
            self.string(key);
            self.json(value);
        }
    }

    fn json(&mut self, value: &Value) {
        match value {
            Value::Null => self.byte(0),
            Value::Bool(value) => {
                self.byte(1);
                self.boolean(*value);
            }
            Value::Number(value) => {
                self.byte(2);
                self.string(&value.to_string());
            }
            Value::String(value) => {
                self.byte(3);
                self.string(value);
            }
            Value::Array(values) => {
                self.byte(4);
                self.usize(values.len());
                for value in values {
                    self.json(value);
                }
            }
            Value::Object(values) => {
                self.byte(5);
                let mut entries: Vec<_> = values.iter().collect();
                entries.sort_unstable_by(|(left, _), (right, _)| {
                    left.as_bytes().cmp(right.as_bytes())
                });
                self.usize(entries.len());
                for (key, value) in entries {
                    self.string(key);
                    self.json(value);
                }
            }
        }
    }

    fn dead_letter(&mut self, dead_letter: &DeadLetter) {
        self.string(&dead_letter.message_id);
        self.envelope(&dead_letter.envelope);
        self.string(&dead_letter.reason);
        self.u64(dead_letter.original_enqueued_at_ms);
        self.u64(dead_letter.dead_lettered_at_ms);
        self.u32(dead_letter.attempts);
        self.optional_string(dead_letter.last_error.as_deref());
    }

    fn dedupe_entry(&mut self, entry: &DedupeEntry) {
        self.string(&entry.message_id);
        self.u64(entry.expires_at_ms);
        self.acknowledgement(&entry.receipt);
    }

    fn acknowledgement(&mut self, acknowledgement: &AckMetadata) {
        self.durability(acknowledgement.durability);
        self.u64(acknowledgement.resource_epoch);
        self.u64(acknowledgement.commit_position);
        self.u16(acknowledgement.replica_acks);
        self.boolean(acknowledgement.duplicate);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueCounts {
    pub ready: usize,
    pub scheduled: usize,
    pub in_flight: usize,
    pub acknowledged: usize,
    pub expired: usize,
    pub dead_lettered: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaintenanceAction {
    Expire,
    Ready,
    LeaseExpired,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn event(id: &str) -> EventEnvelope {
        let mut event = EventEnvelope::new("tests", "work.requested", json!({"id": id}), 0);
        event.id = id.into();
        event
    }

    fn lease_fence() -> LeaseFence {
        LeaseFence::new(7, 11, 3, 13, 17).unwrap()
    }

    fn checksum_fixture_queue() -> Queue {
        let mut queue = Queue::new(QueueConfig {
            durability: DurabilityProfile::LocalDurable,
            visibility_timeout_ms: 50,
            max_messages: 20,
            retry: RetryPolicy {
                strategy: BackoffStrategy::Fixed,
                initial_delay_ms: 7,
                max_delay_ms: 70,
                jitter_percent: 3,
                max_attempts: 4,
                max_age_ms: Some(5_000),
            },
            dedupe_window_ms: Some(1_000),
        })
        .unwrap();

        let mut first = event("one");
        first.subject = Some("tenant-a".into());
        first.key = Some("account-7".into());
        first.headers.insert("z-last".into(), "z".into());
        first.headers.insert("a-first".into(), "a".into());
        first.schema_ref = Some("schema:work:1".into());
        first.traceparent = Some("00-trace-parent-01".into());
        first.payload = json!({"z": [true, null, 1.5], "a": {"value": 7}});
        first.ttl_ms = Some(10_000);
        first.priority = 9;
        first.dedupe_id = Some("request-one".into());
        first.transaction_id = Some("tx-one".into());
        first.extensions.insert("x-z".into(), json!({"n": 2}));
        first.extensions.insert("x-a".into(), json!([1, 2, 3]));
        queue.enqueue(first, 10).unwrap();

        let mut second = event("two");
        second.dedupe_id = Some("request-two".into());
        queue.enqueue(second, 11).unwrap();

        let rejected = queue
            .acquire("worker-a", 1, Some(40), 12)
            .unwrap()
            .remove(0);
        queue
            .reject(&rejected.lease_token, "invalid payload", 13)
            .unwrap();
        queue.acquire("worker-b", 1, None, 14).unwrap();
        queue
    }

    #[test]
    fn fenced_acquire_is_deterministic_and_server_metadata_requires_a_live_token() {
        let fence = lease_fence();
        let mut first_queue = Queue::new(QueueConfig::default()).unwrap();
        first_queue.enqueue(event("message.with.é"), 10).unwrap();
        let mut replayed_queue = first_queue.clone();

        let first = first_queue
            .acquire_fenced("worker", 1, Some(50), 20, fence)
            .unwrap()
            .remove(0);
        let replayed = replayed_queue
            .acquire_fenced("worker", 1, Some(50), 20, fence)
            .unwrap()
            .remove(0);
        assert_eq!(first.lease_token, replayed.lease_token);
        assert_eq!(
            first_queue.recovery_state_checksum(),
            replayed_queue.recovery_state_checksum()
        );
        assert!(first.lease_token.starts_with(FENCED_LEASE_TOKEN_PREFIX));

        let metadata = FencedLeaseTokenMetadata::parse(&first.lease_token).unwrap();
        assert_eq!(metadata.fence(), fence);
        assert_eq!(metadata.consumer(), "worker");
        assert_eq!(metadata.message_id(), "message.with.é");
        assert_eq!(metadata.lease_generation(), 1);
        assert_eq!(metadata.lease_deadline_ms(), 70);
        assert_eq!(
            first_queue
                .fenced_lease_metadata(&first.lease_token)
                .unwrap(),
            metadata
        );

        let mut corrupted = first.lease_token.clone();
        let replacement = if corrupted.ends_with('0') { '1' } else { '0' };
        corrupted.pop();
        corrupted.push(replacement);
        assert!(matches!(
            FencedLeaseTokenMetadata::parse(&corrupted),
            Err(EpochError::InvalidArgument(_))
        ));

        assert_eq!(
            first_queue.acknowledge(&first.lease_token, 21),
            Err(EpochError::Fenced)
        );
        let stale_fence = LeaseFence::new(7, 11, 3, 14, 17).unwrap();
        assert_eq!(
            first_queue.acknowledge_fenced(&first.lease_token, stale_fence, 21),
            Err(EpochError::Fenced)
        );
        first_queue
            .acknowledge_fenced(&first.lease_token, fence, 21)
            .unwrap();
        assert_eq!(
            first_queue.fenced_lease_metadata(&first.lease_token),
            Err(EpochError::Fenced)
        );
    }

    #[test]
    fn fenced_acquisition_rejects_an_oversized_token_before_mutation() {
        let oversized_id = "m".repeat(MAX_FENCED_LEASE_TOKEN_BYTES);
        let mut oversized_queue = Queue::new(QueueConfig::default()).unwrap();
        oversized_queue.enqueue(event(&oversized_id), 0).unwrap();
        let commit_position = oversized_queue.commit_position;
        assert!(matches!(
            oversized_queue.acquire_fenced("worker", 1, None, 1, lease_fence()),
            Err(EpochError::InvalidArgument(_))
        ));
        assert_eq!(oversized_queue.commit_position, commit_position);
        assert_eq!(oversized_queue.counts().ready, 1);
    }

    #[test]
    fn standalone_acquire_retains_the_v1_token_shape() {
        let mut queue = Queue::new(QueueConfig::default()).unwrap();
        queue.enqueue(event("one"), 0).unwrap();

        let delivery = queue.acquire("worker", 1, None, 0).unwrap().remove(0);

        assert_eq!(delivery.lease_token, "one.1.2");
    }

    #[test]
    fn legacy_token_with_fenced_prefix_remains_a_valid_v1_token() {
        let mut queue = Queue::new(QueueConfig::default()).unwrap();
        queue.enqueue(event("epoch.queue.lease.foo"), 0).unwrap();
        let delivery = queue.acquire("worker", 1, Some(10), 0).unwrap().remove(0);
        assert!(delivery.lease_token.starts_with(FENCED_LEASE_TOKEN_PREFIX));

        assert_eq!(
            queue.extend_lease(&delivery.lease_token, 20, 1).unwrap(),
            21
        );
        queue.acknowledge(&delivery.lease_token, 2).unwrap();
        assert_eq!(queue.counts().acknowledged, 1);
    }

    #[test]
    fn fenced_tokens_require_expected_fence_settlement_entry_points() {
        let fence = lease_fence();
        let mut base = Queue::new(QueueConfig {
            retry: RetryPolicy {
                initial_delay_ms: 0,
                max_delay_ms: 0,
                jitter_percent: 0,
                ..RetryPolicy::default()
            },
            ..QueueConfig::default()
        })
        .unwrap();
        base.enqueue(event("settle"), 0).unwrap();
        let delivery = base
            .acquire_fenced("worker", 1, Some(10), 0, fence)
            .unwrap()
            .remove(0);

        let mut queue = base.clone();
        assert_eq!(
            queue.acknowledge(&delivery.lease_token, 1),
            Err(EpochError::Fenced)
        );
        let mut queue = base.clone();
        assert_eq!(
            queue.nack(&delivery.lease_token, "failed", 1),
            Err(EpochError::Fenced)
        );
        let mut queue = base.clone();
        assert_eq!(
            queue.release(&delivery.lease_token, 0, None, 1),
            Err(EpochError::Fenced)
        );
        let mut queue = base.clone();
        assert_eq!(
            queue.reject(&delivery.lease_token, "failed", 1),
            Err(EpochError::Fenced)
        );
        let mut queue = base.clone();
        assert_eq!(
            queue.extend_lease(&delivery.lease_token, 20, 1),
            Err(EpochError::Fenced)
        );

        let mut queue = base.clone();
        queue
            .acknowledge_fenced(&delivery.lease_token, fence, 1)
            .unwrap();
        assert_eq!(queue.counts().acknowledged, 1);

        let mut queue = base.clone();
        queue
            .nack_fenced(&delivery.lease_token, fence, "retry", 1)
            .unwrap();
        assert_eq!(queue.counts().ready, 1);

        let mut queue = base.clone();
        queue
            .release_fenced(&delivery.lease_token, fence, 0, Some("release".into()), 1)
            .unwrap();
        assert_eq!(queue.counts().ready, 1);

        let mut queue = base;
        queue
            .reject_fenced(&delivery.lease_token, fence, "reject", 1)
            .unwrap();
        assert_eq!(queue.dead_letter("settle").unwrap().reason, "reject");
    }

    #[test]
    fn nack_uses_deterministic_configured_backoff_and_terminal_policy() {
        let mut queue = Queue::new(QueueConfig {
            retry: RetryPolicy {
                strategy: BackoffStrategy::Exponential,
                initial_delay_ms: 10,
                max_delay_ms: 100,
                jitter_percent: 0,
                max_attempts: 3,
                max_age_ms: None,
            },
            ..QueueConfig::default()
        })
        .unwrap();
        queue.enqueue(event("poison"), 0).unwrap();

        let first = queue.acquire("worker", 1, None, 0).unwrap().remove(0);
        queue.nack(&first.lease_token, "first failure", 1).unwrap();
        assert_eq!(
            queue.get("poison").unwrap().state,
            QueueState::Scheduled { eligible_at_ms: 11 }
        );
        assert!(queue.acquire("worker", 1, None, 10).unwrap().is_empty());

        let second = queue.acquire("worker", 1, None, 11).unwrap().remove(0);
        queue
            .nack(&second.lease_token, "second failure", 12)
            .unwrap();
        assert_eq!(
            queue.get("poison").unwrap().state,
            QueueState::Scheduled { eligible_at_ms: 32 }
        );

        let third = queue.acquire("worker", 1, None, 32).unwrap().remove(0);
        queue.nack(&third.lease_token, "final failure", 33).unwrap();
        let evidence = queue.dead_letter("poison").unwrap();
        assert_eq!(evidence.reason, "final failure");
        assert_eq!(evidence.attempts, 3);
        assert_eq!(evidence.last_error.as_deref(), Some("final failure"));
        assert!(queue.dead_letter("missing").is_none());

        queue.redrive("poison", 34).unwrap();
        assert!(queue.dead_letter("poison").is_none());
    }

    #[test]
    fn bounded_renewal_reissues_fenced_token_at_earliest_terminal_deadline() {
        let config = QueueConfig {
            retry: RetryPolicy {
                max_age_ms: Some(80),
                ..RetryPolicy::default()
            },
            ..QueueConfig::default()
        };
        let fence = lease_fence();

        let mut clamp_queue = Queue::new(config.clone()).unwrap();
        let mut clamp_event = event("initial-clamp");
        clamp_event.ttl_ms = Some(40);
        clamp_queue.enqueue(clamp_event, 10).unwrap();
        let clamped = clamp_queue
            .acquire_fenced("worker", 1, Some(1_000), 20, fence)
            .unwrap()
            .remove(0);
        assert_eq!(clamped.lease_deadline_ms, 50);
        assert_eq!(
            FencedLeaseTokenMetadata::parse(&clamped.lease_token)
                .unwrap()
                .lease_deadline_ms(),
            50
        );

        let mut ttl_queue = Queue::new(config.clone()).unwrap();
        let mut ttl_event = event("ttl-bound");
        ttl_event.ttl_ms = Some(40);
        ttl_queue.enqueue(ttl_event, 10).unwrap();
        let ttl_delivery = ttl_queue
            .acquire_fenced("worker", 1, Some(10), 20, fence)
            .unwrap()
            .remove(0);
        let commit_position = ttl_queue.commit_position;
        assert!(matches!(
            ttl_queue.extend_lease_fenced_bounded(&ttl_delivery.lease_token, fence, 1, 21),
            Err(EpochError::Conflict(_))
        ));
        assert_eq!(ttl_queue.commit_position, commit_position);
        assert_eq!(
            ttl_queue
                .fenced_lease_metadata(&ttl_delivery.lease_token)
                .unwrap()
                .lease_deadline_ms(),
            30
        );
        let ttl_renewal = ttl_queue
            .extend_lease_fenced_bounded(&ttl_delivery.lease_token, fence, 1_000, 25)
            .unwrap();
        assert_eq!(ttl_renewal.lease_deadline_ms, 50);
        assert_ne!(ttl_renewal.lease_token, ttl_delivery.lease_token);
        assert_eq!(
            ttl_queue.fenced_lease_metadata(&ttl_delivery.lease_token),
            Err(EpochError::Fenced)
        );
        assert_eq!(
            ttl_queue
                .fenced_lease_metadata(&ttl_renewal.lease_token)
                .unwrap()
                .lease_deadline_ms(),
            50
        );
        assert_eq!(
            ttl_queue.extend_lease_fenced_bounded(&ttl_delivery.lease_token, fence, 1_000, 26),
            Err(EpochError::Fenced)
        );

        let mut max_age_queue = Queue::new(config).unwrap();
        let mut max_age_event = event("max-age-bound");
        max_age_event.ttl_ms = Some(100);
        max_age_queue.enqueue(max_age_event, 10).unwrap();
        let max_age_delivery = max_age_queue
            .acquire_fenced("worker", 1, Some(10), 20, fence)
            .unwrap()
            .remove(0);
        let max_age_renewal = max_age_queue
            .extend_lease_fenced_bounded(&max_age_delivery.lease_token, fence, 1_000, 25)
            .unwrap();
        assert_eq!(max_age_renewal.lease_deadline_ms, 90);
        assert_eq!(
            FencedLeaseTokenMetadata::parse(&max_age_renewal.lease_token)
                .unwrap()
                .lease_deadline_ms(),
            90
        );
    }

    #[test]
    fn fenced_maintenance_enforces_max_age_without_changing_legacy_replay() {
        let config = QueueConfig {
            retry: RetryPolicy {
                max_age_ms: Some(10),
                ..RetryPolicy::default()
            },
            ..QueueConfig::default()
        };
        let mut legacy = Queue::new(config.clone()).unwrap();
        legacy.enqueue(event("legacy"), 0).unwrap();
        assert_eq!(legacy.acquire("worker", 1, None, 10).unwrap().len(), 1);

        let mut fenced = Queue::new(config).unwrap();
        fenced.enqueue(event("fenced"), 0).unwrap();
        assert!(
            fenced
                .acquire_fenced("worker", 1, None, 10, lease_fence())
                .unwrap()
                .is_empty()
        );
        assert_eq!(fenced.counts().expired, 1);
        assert!(fenced.dead_letter("fenced").is_none());
        let commit_position = fenced.commit_position;
        fenced.maintain_fenced(11).unwrap();
        assert_eq!(fenced.commit_position, commit_position);
    }

    #[test]
    fn fenced_mutations_fail_closed_when_monotonic_counters_are_exhausted() {
        let fence = lease_fence();
        let mut generation_exhausted = Queue::new(QueueConfig::default()).unwrap();
        generation_exhausted
            .enqueue(event("generation"), 0)
            .unwrap();
        generation_exhausted.lease_generation = u64::MAX;
        let commit_position = generation_exhausted.commit_position;
        assert!(matches!(
            generation_exhausted.acquire_fenced("worker", 1, None, 0, fence),
            Err(EpochError::Capacity(_))
        ));
        assert_eq!(generation_exhausted.commit_position, commit_position);
        assert_eq!(generation_exhausted.counts().ready, 1);

        let mut commit_exhausted = Queue::new(QueueConfig::default()).unwrap();
        commit_exhausted.enqueue(event("commit"), 0).unwrap();
        commit_exhausted.commit_position = u64::MAX;
        assert!(matches!(
            commit_exhausted.acquire_fenced("worker", 1, None, 0, fence),
            Err(EpochError::Capacity(_))
        ));
        assert_eq!(commit_exhausted.counts().ready, 1);

        let mut settlement_exhausted = Queue::new(QueueConfig::default()).unwrap();
        settlement_exhausted
            .enqueue(event("settlement"), 0)
            .unwrap();
        let delivery = settlement_exhausted
            .acquire_fenced("worker", 1, None, 0, fence)
            .unwrap()
            .remove(0);
        settlement_exhausted.commit_position = u64::MAX;
        assert!(matches!(
            settlement_exhausted.acknowledge_fenced(&delivery.lease_token, fence, 1),
            Err(EpochError::Capacity(_))
        ));
        assert!(matches!(
            settlement_exhausted.get("settlement").unwrap().state,
            QueueState::Leased { .. }
        ));

        let mut legacy = Queue::new(QueueConfig::default()).unwrap();
        legacy.enqueue(event("legacy-saturation"), 0).unwrap();
        legacy.lease_generation = u64::MAX;
        legacy.commit_position = u64::MAX;
        let delivery = legacy.acquire("worker", 1, None, 0).unwrap().remove(0);
        assert_eq!(
            delivery.lease_token,
            format!("legacy-saturation.{}.{}", u64::MAX, u64::MAX)
        );
    }

    #[test]
    fn acknowledge_is_fenced_by_lease_token() {
        let mut queue = Queue::new(QueueConfig {
            retry: RetryPolicy {
                initial_delay_ms: 0,
                max_delay_ms: 0,
                jitter_percent: 0,
                ..RetryPolicy::default()
            },
            ..QueueConfig::default()
        })
        .unwrap();
        queue.enqueue(event("one"), 0).unwrap();
        let first = queue.acquire("worker-a", 1, Some(10), 0).unwrap().remove(0);
        queue.maintain(10);
        let second = queue
            .acquire("worker-b", 1, Some(10), 10)
            .unwrap()
            .remove(0);
        assert!(matches!(
            queue.acknowledge(&first.lease_token, 11),
            Err(EpochError::Fenced)
        ));
        queue.acknowledge(&second.lease_token, 11).unwrap();
        assert_eq!(queue.counts().acknowledged, 1);
    }

    #[test]
    fn scheduled_message_is_not_visible_early() {
        let mut queue = Queue::new(QueueConfig::default()).unwrap();
        let mut scheduled = event("later");
        scheduled.deliver_at_ms = Some(100);
        queue.enqueue(scheduled, 0).unwrap();
        assert!(queue.acquire("worker", 1, None, 99).unwrap().is_empty());
        assert_eq!(queue.acquire("worker", 1, None, 100).unwrap().len(), 1);
    }

    #[test]
    fn retry_exhaustion_preserves_dead_letter_history() {
        let mut queue = Queue::new(QueueConfig {
            retry: RetryPolicy {
                max_attempts: 2,
                initial_delay_ms: 0,
                max_delay_ms: 0,
                jitter_percent: 0,
                ..RetryPolicy::default()
            },
            ..QueueConfig::default()
        })
        .unwrap();
        queue.enqueue(event("poison"), 0).unwrap();
        let first = queue.acquire("worker", 1, None, 0).unwrap().remove(0);
        queue
            .release(&first.lease_token, 0, Some("failed".into()), 1)
            .unwrap();
        let second = queue.acquire("worker", 1, None, 1).unwrap().remove(0);
        queue
            .release(&second.lease_token, 0, Some("failed again".into()), 2)
            .unwrap();
        assert_eq!(queue.counts().dead_lettered, 1);
        let dead = queue.dead_letters(10).remove(0);
        assert_eq!(dead.attempts, 2);
        assert_eq!(dead.reason, "failed again");
        queue.redrive("poison", 3).unwrap();
        assert_eq!(queue.counts().ready, 1);
    }

    #[test]
    fn redrive_cannot_lease_one_message_twice_in_a_batch() {
        let mut queue = Queue::new(QueueConfig::default()).unwrap();
        queue.enqueue(event("poison"), 0).unwrap();
        let delivery = queue.acquire("worker", 1, None, 0).unwrap().remove(0);
        queue.reject(&delivery.lease_token, "poison", 1).unwrap();
        queue.redrive("poison", 2).unwrap();

        let deliveries = queue.acquire("worker", 2, None, 2).unwrap();

        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].message.id, "poison");
    }

    #[test]
    fn priority_does_not_reorder_already_leased_messages() {
        let mut queue = Queue::new(QueueConfig::default()).unwrap();
        let mut low = event("low");
        low.priority = 1;
        queue.enqueue(low, 0).unwrap();
        let first = queue.acquire("worker", 1, None, 0).unwrap().remove(0);
        let mut high = event("high");
        high.priority = 9;
        queue.enqueue(high, 1).unwrap();
        let next = queue.acquire("worker", 1, None, 1).unwrap().remove(0);
        assert_eq!(first.message.id, "low");
        assert_eq!(next.message.id, "high");
    }

    #[test]
    fn dedupe_window_returns_original_receipt() {
        let mut queue = Queue::new(QueueConfig {
            dedupe_window_ms: Some(100),
            ..QueueConfig::default()
        })
        .unwrap();
        let mut value = event("one");
        value.dedupe_id = Some("request-1".into());
        let first = queue.enqueue(value.clone(), 0).unwrap();
        value.id = "two".into();
        let second = queue.enqueue(value, 1).unwrap();
        assert_eq!(first.message_id, second.message_id);
        assert!(second.acknowledgement.duplicate);
    }

    #[test]
    fn recovery_state_checksum_is_independent_of_hash_map_iteration_order() {
        let mut queue = checksum_fixture_queue();
        let expected = queue.recovery_state_checksum();

        let mut messages: Vec<_> = queue.messages.drain().collect();
        messages.sort_unstable_by(|(left, _), (right, _)| right.cmp(left));
        queue.messages.extend(messages);

        let mut dedupe: Vec<_> = queue.dedupe.drain().collect();
        dedupe.sort_unstable_by(|(left, _), (right, _)| right.cmp(left));
        queue.dedupe.extend(dedupe);

        assert_eq!(queue.recovery_state_checksum(), expected);
    }

    #[test]
    fn recovery_state_checksum_covers_every_queue_state_component() {
        let queue = checksum_fixture_queue();
        let expected = queue.recovery_state_checksum();

        let mut changed = queue.clone();
        changed.config.max_messages += 1;
        assert_ne!(changed.recovery_state_checksum(), expected);

        let mut changed = queue.clone();
        changed.messages.get_mut("two").unwrap().attempt += 1;
        assert_ne!(changed.recovery_state_checksum(), expected);

        let mut changed = queue.clone();
        changed.order.swap(0, 1);
        assert_ne!(changed.recovery_state_checksum(), expected);

        let mut changed = queue.clone();
        changed.dead_letters[0].reason.push_str(" changed");
        assert_ne!(changed.recovery_state_checksum(), expected);

        let mut changed = queue.clone();
        changed.dedupe.get_mut("request-one").unwrap().expires_at_ms += 1;
        assert_ne!(changed.recovery_state_checksum(), expected);

        let mut changed = queue.clone();
        changed.commit_position += 1;
        assert_ne!(changed.recovery_state_checksum(), expected);

        let mut changed = queue;
        changed.lease_generation += 1;
        assert_ne!(changed.recovery_state_checksum(), expected);
    }

    #[test]
    fn recovery_state_checksum_matches_the_v1_golden_value() {
        assert_eq!(
            checksum_fixture_queue().recovery_state_checksum(),
            3_359_853_911
        );
    }
}
