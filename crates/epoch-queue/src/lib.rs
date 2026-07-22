//! Lease-based work queue state machine.
//!
//! Acquiring, acknowledging, extending, and retrying are explicit state
//! transitions. Lease tokens carry a generation so an expired or replaced
//! owner cannot acknowledge work after being fenced.

use std::collections::{HashMap, VecDeque};

use epoch_core::{AckMetadata, DurabilityProfile, EpochError, EpochResult, EventEnvelope};
use serde::{Deserialize, Serialize};

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
            durability: DurabilityProfile::LocalDurable,
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
pub struct DeadLetter {
    pub message_id: String,
    pub envelope: EventEnvelope,
    pub reason: String,
    pub original_enqueued_at_ms: u64,
    pub dead_lettered_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
struct DedupeEntry {
    message_id: String,
    expires_at_ms: u64,
    receipt: AckMetadata,
}

#[derive(Debug)]
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
        let mut candidates: Vec<(String, u8, u64)> = self
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

    pub fn acknowledge(&mut self, token: &str, now_ms: u64) -> EpochResult<AckMetadata> {
        self.maintain(now_ms);
        let id = self.message_id_for_live_token(token)?;
        self.commit_position = self.commit_position.saturating_add(1);
        self.messages.get_mut(&id).expect("token resolved").state = QueueState::Acknowledged;
        Ok(AckMetadata::standalone(
            self.commit_position,
            self.config.durability,
        ))
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
        self.maintain(now_ms);
        let id = self.message_id_for_live_token(token)?;
        let message = self.messages.get_mut(&id).expect("token resolved");
        let QueueState::Leased { deadline_ms, .. } = &mut message.state else {
            return Err(EpochError::Fenced);
        };
        *deadline_ms = now_ms.saturating_add(extension_ms);
        self.commit_position = self.commit_position.saturating_add(1);
        Ok(*deadline_ms)
    }

    pub fn release(
        &mut self,
        token: &str,
        delay_ms: u64,
        reason: Option<String>,
        now_ms: u64,
    ) -> EpochResult<()> {
        self.maintain(now_ms);
        let id = self.message_id_for_live_token(token)?;
        self.retry_or_dead_letter(&id, delay_ms, reason, now_ms);
        Ok(())
    }

    pub fn reject(
        &mut self,
        token: &str,
        reason: impl Into<String>,
        now_ms: u64,
    ) -> EpochResult<()> {
        self.maintain(now_ms);
        let id = self.message_id_for_live_token(token)?;
        self.dead_letter(&id, reason.into(), now_ms);
        Ok(())
    }

    pub fn get(&self, message_id: &str) -> Option<QueueMessage> {
        self.messages.get(message_id).cloned()
    }

    pub fn dead_letters(&self, limit: usize) -> Vec<DeadLetter> {
        self.dead_letters.iter().take(limit).cloned().collect()
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
            self.dead_letter(
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

    fn dead_letter(&mut self, id: &str, reason: String, now_ms: u64) {
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

#[derive(Debug, Clone, Copy)]
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
}
