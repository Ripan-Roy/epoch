//! Durable, target-isolated Event Bus delivery ledger.
//!
//! This module owns only replicated delivery intent and settlement state. It
//! deliberately performs no network I/O: dispatch workers lease records,
//! execute target-specific work outside the state machine, then commit an
//! acknowledgement or failure.

use std::collections::BTreeMap;

use epoch_core::{EpochError, EpochResult, EventEnvelope, validate_resource_name};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{EventFilter, EventTransform, RoutedDelivery, Subscription, SubscriptionTarget};

pub const DEFAULT_MAX_OUTBOX_DELIVERIES: usize = 100_000;
pub const MAX_BUS_OUTBOX_DELIVERIES: usize = 10_000_000;
pub const MAX_DELIVERY_QUERY_RESULTS: usize = 10_000;
pub const MAX_DELIVERY_ACQUIRE_BATCH: usize = 100;
pub const MAX_DELIVERY_ATTEMPTS: u32 = 100;
pub const MAX_DELIVERY_IN_FLIGHT: u16 = 1_000;
pub const MAX_DELIVERY_TIMEOUT_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
pub const MAX_DELIVERY_REASON_BYTES: usize = 4 * 1_024;
pub const MAX_DISPATCHER_BYTES: usize = 128;

const DELIVERY_ID_PREFIX: &str = "epoch.bus.delivery.v1";
const DELIVERY_LEASE_PREFIX: &str = "epoch.bus.delivery.lease.v1.";

/// The Epoch-owned profile selected by a Queue or Stream subscription target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpochTargetKind {
    Queue,
    Stream,
}

/// The exact destination incarnation durably bound to an internal delivery.
///
/// The simple resource name is intentionally repeated from the target so a
/// snapshot or command cannot bind Queue intent to an unrelated tablet kind or
/// name. Tenant scope is supplied by the source Bus route and verified by the
/// regional worker before this binding is proposed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EpochTargetDestination {
    pub kind: EpochTargetKind,
    pub resource: String,
    pub resource_generation: u64,
    pub shard_index: u32,
    pub tablet_id: u64,
    pub tablet_epoch: u64,
}

impl EpochTargetDestination {
    pub fn new(
        kind: EpochTargetKind,
        resource: impl Into<String>,
        resource_generation: u64,
        shard_index: u32,
        tablet_id: u64,
        tablet_epoch: u64,
    ) -> EpochResult<Self> {
        let destination = Self {
            kind,
            resource: resource.into(),
            resource_generation,
            shard_index,
            tablet_id,
            tablet_epoch,
        };
        destination.validate()?;
        Ok(destination)
    }

    pub fn validate(&self) -> EpochResult<()> {
        validate_resource_name(&self.resource)?;
        if self.resource_generation == 0 || self.tablet_id == 0 || self.tablet_epoch == 0 {
            return Err(EpochError::InvalidArgument(
                "Epoch target generation, tablet ID, and tablet epoch must be non-zero".into(),
            ));
        }
        if self.kind == EpochTargetKind::Queue && self.shard_index != 0 {
            return Err(EpochError::InvalidArgument(
                "Queue targets must bind logical shard zero".into(),
            ));
        }
        Ok(())
    }

    pub fn validate_for_target(&self, target: &SubscriptionTarget) -> EpochResult<()> {
        self.validate()?;
        let matches = match target {
            SubscriptionTarget::Queue { resource } => {
                self.kind == EpochTargetKind::Queue && self.resource == *resource
            }
            SubscriptionTarget::Stream { resource } => {
                self.kind == EpochTargetKind::Stream && self.resource == *resource
            }
            SubscriptionTarget::Pull
            | SubscriptionTarget::Webhook { .. }
            | SubscriptionTarget::Http { .. } => false,
        };
        if matches {
            Ok(())
        } else {
            Err(EpochError::Conflict(
                "Epoch destination does not match the subscription target".into(),
            ))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryBackoffStrategy {
    #[default]
    Exponential,
    Fixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryRetryPolicy {
    pub strategy: DeliveryBackoffStrategy,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter_percent: u8,
    pub max_attempts: u32,
    pub max_age_ms: Option<u64>,
}

impl Default for DeliveryRetryPolicy {
    fn default() -> Self {
        Self {
            strategy: DeliveryBackoffStrategy::Exponential,
            initial_delay_ms: 1_000,
            max_delay_ms: 60_000,
            jitter_percent: 10,
            max_attempts: 8,
            max_age_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPolicy {
    pub timeout_ms: u64,
    pub max_in_flight: u16,
    #[serde(default)]
    pub retry: DeliveryRetryPolicy,
}

impl Default for DeliveryPolicy {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            max_in_flight: 16,
            retry: DeliveryRetryPolicy::default(),
        }
    }
}

impl DeliveryPolicy {
    pub(crate) fn is_default(&self) -> bool {
        self == &Self::default()
    }

    pub(crate) fn validate(&self) -> EpochResult<()> {
        if self.timeout_ms == 0 || self.timeout_ms > MAX_DELIVERY_TIMEOUT_MS {
            return Err(EpochError::InvalidArgument(format!(
                "delivery timeout_ms must be between 1 and {MAX_DELIVERY_TIMEOUT_MS}"
            )));
        }
        if self.max_in_flight == 0 || self.max_in_flight > MAX_DELIVERY_IN_FLIGHT {
            return Err(EpochError::InvalidArgument(format!(
                "delivery max_in_flight must be between 1 and {MAX_DELIVERY_IN_FLIGHT}"
            )));
        }
        if self.retry.max_attempts == 0 || self.retry.max_attempts > MAX_DELIVERY_ATTEMPTS {
            return Err(EpochError::InvalidArgument(format!(
                "delivery retry max_attempts must be between 1 and {MAX_DELIVERY_ATTEMPTS}"
            )));
        }
        if self.retry.initial_delay_ms > self.retry.max_delay_ms {
            return Err(EpochError::InvalidArgument(
                "delivery retry initial_delay_ms must not exceed max_delay_ms".into(),
            ));
        }
        if self.retry.max_delay_ms > MAX_DELIVERY_TIMEOUT_MS {
            return Err(EpochError::InvalidArgument(format!(
                "delivery retry max_delay_ms must not exceed {MAX_DELIVERY_TIMEOUT_MS}"
            )));
        }
        if self.retry.jitter_percent > 100 {
            return Err(EpochError::InvalidArgument(
                "delivery retry jitter_percent cannot exceed 100".into(),
            ));
        }
        if self.retry.max_age_ms == Some(0) {
            return Err(EpochError::InvalidArgument(
                "delivery retry max_age_ms must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

/// Replicated ownership coordinates for one dispatch attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryFence {
    tablet_id: u64,
    tablet_epoch: u64,
    leader_term: u64,
    dispatcher_epoch: u64,
}

impl DeliveryFence {
    pub fn new(
        tablet_id: u64,
        tablet_epoch: u64,
        leader_term: u64,
        dispatcher_epoch: u64,
    ) -> EpochResult<Self> {
        let fence = Self {
            tablet_id,
            tablet_epoch,
            leader_term,
            dispatcher_epoch,
        };
        fence.validate()?;
        Ok(fence)
    }

    fn validate(self) -> EpochResult<()> {
        if self.tablet_id == 0
            || self.tablet_epoch == 0
            || self.leader_term == 0
            || self.dispatcher_epoch == 0
        {
            return Err(EpochError::InvalidArgument(
                "delivery fence coordinates must be non-zero".into(),
            ));
        }
        Ok(())
    }

    pub const fn dispatcher_epoch(self) -> u64 {
        self.dispatcher_epoch
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStateKind {
    Pending,
    InFlight,
    Acknowledged,
    DeadLettered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeliveryState {
    Pending {
        eligible_at_ms: u64,
    },
    InFlight {
        dispatcher: String,
        dispatcher_epoch: u64,
        attempt: u32,
        lease_token: String,
        lease_deadline_ms: u64,
        fence: DeliveryFence,
    },
    Acknowledged {
        acknowledged_at_ms: u64,
    },
    DeadLettered {
        dead_lettered_at_ms: u64,
        reason: String,
    },
}

impl DeliveryState {
    pub const fn kind(&self) -> DeliveryStateKind {
        match self {
            Self::Pending { .. } => DeliveryStateKind::Pending,
            Self::InFlight { .. } => DeliveryStateKind::InFlight,
            Self::Acknowledged { .. } => DeliveryStateKind::Acknowledged,
            Self::DeadLettered { .. } => DeliveryStateKind::DeadLettered,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeliveryAttemptOutcome {
    InFlight,
    Acknowledged {
        completed_at_ms: u64,
    },
    Failed {
        failed_at_ms: u64,
        reason: String,
        retry_at_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryAttempt {
    pub attempt: u32,
    pub dispatcher: String,
    pub dispatcher_epoch: u64,
    pub leader_term: u64,
    pub started_at_ms: u64,
    pub lease_deadline_ms: u64,
    pub outcome: DeliveryAttemptOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryRecord {
    pub delivery_id: String,
    pub publish_position: u64,
    pub subscription: String,
    pub target: SubscriptionTarget,
    pub envelope: EventEnvelope,
    pub route_plan_version: u64,
    pub created_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub policy: DeliveryPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<EpochTargetDestination>,
    pub state: DeliveryState,
    pub attempts: Vec<DeliveryAttempt>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryLease {
    pub delivery_id: String,
    pub publish_position: u64,
    pub subscription: String,
    pub target: SubscriptionTarget,
    pub envelope: EventEnvelope,
    pub route_plan_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<EpochTargetDestination>,
    pub attempt: u32,
    pub lease_token: String,
    pub lease_deadline_ms: u64,
}

/// The next signed HTTP delivery that the built-in dispatcher may lease for a
/// subscription.
///
/// This is a read-only scheduling hint. The replicated acquire command remains
/// authoritative and rechecks eligibility when it is applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedWebhookDeliveryCandidate {
    pub delivery_id: String,
    pub subscription: String,
    pub next_attempt: u32,
    pub signing_key_id: String,
}

/// The oldest due Queue or Stream delivery that the built-in regional worker
/// may resolve and acquire for one subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochTargetDeliveryCandidate {
    pub delivery_id: String,
    pub subscription: String,
    pub next_attempt: u32,
    pub target: SubscriptionTarget,
    pub partition_key: String,
    pub destination: Option<EpochTargetDestination>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryCounts {
    pub pending: usize,
    pub in_flight: usize,
    pub acknowledged: usize,
    pub dead_lettered: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryMaintenanceResult {
    pub processed: usize,
    pub retried: usize,
    pub dead_lettered: usize,
    pub counts: DeliveryCounts,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeliveryLedger {
    enabled: bool,
    max_deliveries: usize,
    records: BTreeMap<String, DeliveryRecord>,
    dispatcher_epochs: BTreeMap<String, u64>,
}

impl DeliveryLedger {
    pub(crate) fn new(enabled: bool, max_deliveries: usize) -> Self {
        Self {
            enabled,
            max_deliveries,
            records: BTreeMap::new(),
            dispatcher_epochs: BTreeMap::new(),
        }
    }

    pub(crate) fn append_publish(
        &mut self,
        publish_position: u64,
        created_at_ms: u64,
        deliveries: &[RoutedDelivery],
    ) -> EpochResult<()> {
        if !self.enabled {
            return Ok(());
        }
        let next_len = self
            .records
            .len()
            .checked_add(deliveries.len())
            .ok_or_else(|| EpochError::Capacity("delivery outbox size overflow".into()))?;
        if next_len > self.max_deliveries {
            return Err(EpochError::Capacity(format!(
                "event bus delivery outbox reached its {} record limit",
                self.max_deliveries
            )));
        }

        let mut additions = Vec::with_capacity(deliveries.len());
        for delivery in deliveries {
            delivery.delivery_policy.validate()?;
            let expires_at_ms = delivery
                .delivery_policy
                .retry
                .max_age_ms
                .map(|max_age_ms| {
                    created_at_ms.checked_add(max_age_ms).ok_or_else(|| {
                        EpochError::Capacity("delivery max-age deadline overflow".into())
                    })
                })
                .transpose()?;
            if self.records.contains_key(&delivery.delivery_id)
                || additions
                    .iter()
                    .any(|(delivery_id, _)| delivery_id == &delivery.delivery_id)
            {
                return Err(EpochError::Internal(format!(
                    "duplicate deterministic delivery id {}",
                    delivery.delivery_id
                )));
            }
            additions.push((
                delivery.delivery_id.clone(),
                DeliveryRecord {
                    delivery_id: delivery.delivery_id.clone(),
                    publish_position,
                    subscription: delivery.subscription.clone(),
                    target: delivery.target.clone(),
                    envelope: delivery.envelope.clone(),
                    route_plan_version: delivery.route_plan_version,
                    created_at_ms,
                    expires_at_ms,
                    policy: delivery.delivery_policy.clone(),
                    destination: None,
                    state: DeliveryState::Pending {
                        eligible_at_ms: created_at_ms,
                    },
                    attempts: Vec::new(),
                },
            ));
        }
        self.records.extend(additions);
        Ok(())
    }

    pub(crate) fn acquire(
        &mut self,
        subscription: &str,
        dispatcher: &str,
        max_deliveries: usize,
        now_ms: u64,
        fence: DeliveryFence,
    ) -> EpochResult<Vec<DeliveryLease>> {
        ensure_enabled(self.enabled)?;
        validate_resource_name(subscription)?;
        validate_dispatcher(dispatcher)?;
        validate_batch_limit(max_deliveries, MAX_DELIVERY_ACQUIRE_BATCH)?;
        fence.validate()?;
        self.accept_dispatcher_epoch(dispatcher, fence.dispatcher_epoch())?;

        let initial_in_flight = self
            .records
            .values()
            .filter(|record| {
                record.subscription == subscription
                    && record.state.kind() == DeliveryStateKind::InFlight
            })
            .count();
        let mut candidates = self
            .records
            .values()
            .filter_map(|record| match record.state {
                DeliveryState::Pending { eligible_at_ms }
                    if record.subscription == subscription && eligible_at_ms <= now_ms =>
                {
                    Some((
                        record.publish_position,
                        record.subscription.clone(),
                        record.delivery_id.clone(),
                    ))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        candidates.sort();

        let mut leases = Vec::new();
        for (_, _, delivery_id) in candidates {
            if leases.len() >= max_deliveries {
                break;
            }
            let record = self
                .records
                .get_mut(&delivery_id)
                .ok_or_else(|| EpochError::Internal("delivery candidate disappeared".into()))?;
            if matches!(
                record.target,
                SubscriptionTarget::Queue { .. } | SubscriptionTarget::Stream { .. }
            ) {
                // Built-in Epoch targets require an exact v3 acquisition that
                // durably binds their destination. Do not skip them or expose
                // them to the legacy pull dispatcher.
                break;
            }
            let effective_in_flight = initial_in_flight
                .checked_add(leases.len())
                .ok_or_else(|| EpochError::Capacity("delivery in-flight count overflow".into()))?;
            if effective_in_flight >= usize::from(record.policy.max_in_flight) {
                break;
            }
            leases.push(begin_delivery_attempt(record, dispatcher, now_ms, fence)?);
        }
        Ok(leases)
    }

    pub(crate) fn acquire_specific(
        &mut self,
        subscription: &str,
        delivery_id: &str,
        dispatcher: &str,
        now_ms: u64,
        fence: DeliveryFence,
        destination: Option<EpochTargetDestination>,
    ) -> EpochResult<Option<DeliveryLease>> {
        ensure_enabled(self.enabled)?;
        validate_resource_name(subscription)?;
        validate_dispatcher(dispatcher)?;
        fence.validate()?;
        self.accept_dispatcher_epoch(dispatcher, fence.dispatcher_epoch())?;
        let record = self
            .records
            .get(delivery_id)
            .ok_or_else(|| EpochError::NotFound(delivery_id.to_owned()))?;
        if record.subscription != subscription {
            return Err(EpochError::Conflict(
                "expected delivery does not belong to the requested subscription".into(),
            ));
        }
        match (&record.target, destination.as_ref()) {
            (
                SubscriptionTarget::Queue { .. } | SubscriptionTarget::Stream { .. },
                Some(destination),
            ) => destination.validate_for_target(&record.target)?,
            (SubscriptionTarget::Queue { .. } | SubscriptionTarget::Stream { .. }, None) => {
                return Err(EpochError::Conflict(
                    "Epoch Queue and Stream targets require a bound destination".into(),
                ));
            }
            (
                SubscriptionTarget::Pull
                | SubscriptionTarget::Webhook { .. }
                | SubscriptionTarget::Http { .. },
                Some(_),
            ) => {
                return Err(EpochError::Conflict(
                    "only Epoch Queue and Stream targets accept a destination binding".into(),
                ));
            }
            (
                SubscriptionTarget::Pull
                | SubscriptionTarget::Webhook { .. }
                | SubscriptionTarget::Http { .. },
                None,
            ) => {}
        }
        if let (Some(bound), Some(requested)) = (&record.destination, destination.as_ref())
            && bound != requested
        {
            return Err(EpochError::Conflict(
                "delivery is already bound to a different Epoch destination".into(),
            ));
        }
        if !matches!(
            record.state,
            DeliveryState::Pending { eligible_at_ms } if eligible_at_ms <= now_ms
        ) {
            return Ok(None);
        }
        let oldest_eligible = self
            .records
            .values()
            .filter(|candidate| {
                candidate.subscription == subscription
                    && matches!(
                        candidate.state,
                        DeliveryState::Pending { eligible_at_ms } if eligible_at_ms <= now_ms
                    )
            })
            .min_by_key(|candidate| (candidate.publish_position, &candidate.delivery_id))
            .map(|candidate| candidate.delivery_id.as_str());
        if oldest_eligible != Some(delivery_id) {
            return Ok(None);
        }
        let in_flight = self
            .records
            .values()
            .filter(|candidate| {
                candidate.subscription == record.subscription
                    && candidate.state.kind() == DeliveryStateKind::InFlight
            })
            .count();
        if in_flight >= usize::from(record.policy.max_in_flight) {
            return Ok(None);
        }
        let record = self
            .records
            .get_mut(delivery_id)
            .ok_or_else(|| EpochError::Internal("delivery candidate disappeared".into()))?;
        if record.destination.is_none() {
            record.destination = destination;
        }
        begin_delivery_attempt(record, dispatcher, now_ms, fence).map(Some)
    }

    pub(crate) fn acknowledge(
        &mut self,
        delivery_id: &str,
        dispatcher: &str,
        lease_token: &str,
        fence: DeliveryFence,
        now_ms: u64,
    ) -> EpochResult<DeliveryRecord> {
        ensure_enabled(self.enabled)?;
        validate_dispatcher(dispatcher)?;
        fence.validate()?;
        self.authorize(delivery_id, dispatcher, lease_token, fence)?;
        let record = self
            .records
            .get_mut(delivery_id)
            .ok_or_else(|| EpochError::NotFound(delivery_id.to_owned()))?;
        complete_current_attempt(
            record,
            DeliveryAttemptOutcome::Acknowledged {
                completed_at_ms: now_ms,
            },
        )?;
        record.state = DeliveryState::Acknowledged {
            acknowledged_at_ms: now_ms,
        };
        Ok(record.clone())
    }

    pub(crate) fn fail(
        &mut self,
        delivery_id: &str,
        dispatcher: &str,
        lease_token: &str,
        fence: DeliveryFence,
        reason: &str,
        now_ms: u64,
    ) -> EpochResult<DeliveryRecord> {
        ensure_enabled(self.enabled)?;
        validate_dispatcher(dispatcher)?;
        validate_reason(reason)?;
        fence.validate()?;
        self.authorize(delivery_id, dispatcher, lease_token, fence)?;
        self.settle_failure(delivery_id, reason, now_ms)
    }

    pub(crate) fn reject(
        &mut self,
        delivery_id: &str,
        dispatcher: &str,
        lease_token: &str,
        fence: DeliveryFence,
        reason: &str,
        now_ms: u64,
    ) -> EpochResult<DeliveryRecord> {
        ensure_enabled(self.enabled)?;
        validate_dispatcher(dispatcher)?;
        validate_reason(reason)?;
        fence.validate()?;
        self.authorize(delivery_id, dispatcher, lease_token, fence)?;
        let record = self
            .records
            .get_mut(delivery_id)
            .ok_or_else(|| EpochError::NotFound(delivery_id.to_owned()))?;
        complete_current_attempt(
            record,
            DeliveryAttemptOutcome::Failed {
                failed_at_ms: now_ms,
                reason: reason.to_owned(),
                retry_at_ms: None,
            },
        )?;
        record.state = DeliveryState::DeadLettered {
            dead_lettered_at_ms: now_ms,
            reason: reason.to_owned(),
        };
        Ok(record.clone())
    }

    pub(crate) fn maintain(
        &mut self,
        now_ms: u64,
        max_deliveries: usize,
    ) -> EpochResult<DeliveryMaintenanceResult> {
        ensure_enabled(self.enabled)?;
        validate_batch_limit(max_deliveries, MAX_DELIVERY_ACQUIRE_BATCH)?;
        let mut expired = self
            .records
            .values()
            .filter_map(|record| match record.state {
                DeliveryState::InFlight {
                    lease_deadline_ms, ..
                } if lease_deadline_ms <= now_ms => {
                    Some((lease_deadline_ms, record.delivery_id.clone()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        expired.sort();
        expired.truncate(max_deliveries);

        let mut result = DeliveryMaintenanceResult::default();
        for (_, delivery_id) in expired {
            let settled = self.settle_failure(&delivery_id, "delivery_lease_timeout", now_ms)?;
            result.processed += 1;
            match settled.state {
                DeliveryState::Pending { .. } => result.retried += 1,
                DeliveryState::DeadLettered { .. } => result.dead_lettered += 1,
                _ => {
                    return Err(EpochError::Internal(
                        "lease maintenance produced a non-settled state".into(),
                    ));
                }
            }
        }
        result.counts = self.counts();
        Ok(result)
    }

    pub(crate) fn next_maintenance_deadline_ms(&self) -> Option<u64> {
        self.records
            .values()
            .filter_map(|record| match record.state {
                DeliveryState::InFlight {
                    lease_deadline_ms, ..
                } => Some(lease_deadline_ms),
                _ => None,
            })
            .min()
    }

    pub(crate) fn signed_webhook_candidates(
        &self,
        now_ms: u64,
    ) -> EpochResult<Vec<SignedWebhookDeliveryCandidate>> {
        ensure_enabled(self.enabled)?;
        let mut in_flight = BTreeMap::<&str, usize>::new();
        for record in self.records.values() {
            if record.state.kind() == DeliveryStateKind::InFlight {
                *in_flight.entry(&record.subscription).or_default() += 1;
            }
        }

        let mut pending = self
            .records
            .values()
            .filter(|record| match record.state {
                DeliveryState::Pending { eligible_at_ms } => eligible_at_ms <= now_ms,
                DeliveryState::InFlight { .. }
                | DeliveryState::Acknowledged { .. }
                | DeliveryState::DeadLettered { .. } => false,
            })
            .collect::<Vec<_>>();
        pending.sort_by(|left, right| {
            (&left.subscription, left.publish_position, &left.delivery_id).cmp(&(
                &right.subscription,
                right.publish_position,
                &right.delivery_id,
            ))
        });

        let mut candidates = Vec::new();
        let mut previous_subscription = None::<&str>;
        for record in pending {
            if previous_subscription == Some(record.subscription.as_str()) {
                continue;
            }
            previous_subscription = Some(&record.subscription);

            // AcquireDeliveries leases the oldest pending record for a
            // subscription. Do not skip an older pull/queue/unsigned target
            // and accidentally lease it as a signed webhook.
            let signing_key_id = match &record.target {
                SubscriptionTarget::Webhook {
                    signing_key_id: Some(signing_key_id),
                    ..
                }
                | SubscriptionTarget::Http {
                    signing_key_id: Some(signing_key_id),
                    ..
                } => signing_key_id,
                SubscriptionTarget::Pull
                | SubscriptionTarget::Queue { .. }
                | SubscriptionTarget::Stream { .. }
                | SubscriptionTarget::Webhook {
                    signing_key_id: None,
                    ..
                }
                | SubscriptionTarget::Http {
                    signing_key_id: None,
                    ..
                } => continue,
            };
            if in_flight
                .get(record.subscription.as_str())
                .copied()
                .unwrap_or_default()
                >= usize::from(record.policy.max_in_flight)
            {
                continue;
            }
            let next_attempt = u32::try_from(record.attempts.len())
                .map_err(|_| EpochError::Capacity("delivery attempt count overflow".into()))?
                .checked_add(1)
                .ok_or_else(|| EpochError::Capacity("delivery attempt count overflow".into()))?;
            candidates.push(SignedWebhookDeliveryCandidate {
                delivery_id: record.delivery_id.clone(),
                subscription: record.subscription.clone(),
                next_attempt,
                signing_key_id: signing_key_id.clone(),
            });
        }
        Ok(candidates)
    }

    pub(crate) fn epoch_target_candidates(
        &self,
        now_ms: u64,
    ) -> EpochResult<Vec<EpochTargetDeliveryCandidate>> {
        ensure_enabled(self.enabled)?;
        let mut in_flight = BTreeMap::<&str, usize>::new();
        for record in self.records.values() {
            if record.state.kind() == DeliveryStateKind::InFlight {
                *in_flight.entry(&record.subscription).or_default() += 1;
            }
        }

        let mut pending = self
            .records
            .values()
            .filter(|record| match record.state {
                DeliveryState::Pending { eligible_at_ms } => eligible_at_ms <= now_ms,
                DeliveryState::InFlight { .. }
                | DeliveryState::Acknowledged { .. }
                | DeliveryState::DeadLettered { .. } => false,
            })
            .collect::<Vec<_>>();
        pending.sort_by(|left, right| {
            (&left.subscription, left.publish_position, &left.delivery_id).cmp(&(
                &right.subscription,
                right.publish_position,
                &right.delivery_id,
            ))
        });

        let mut candidates = Vec::new();
        let mut previous_subscription = None::<&str>;
        for record in pending {
            if previous_subscription == Some(record.subscription.as_str()) {
                continue;
            }
            previous_subscription = Some(&record.subscription);
            if !matches!(
                record.target,
                SubscriptionTarget::Queue { .. } | SubscriptionTarget::Stream { .. }
            ) {
                continue;
            }
            if in_flight
                .get(record.subscription.as_str())
                .copied()
                .unwrap_or_default()
                >= usize::from(record.policy.max_in_flight)
            {
                continue;
            }
            let next_attempt = u32::try_from(record.attempts.len())
                .map_err(|_| EpochError::Capacity("delivery attempt count overflow".into()))?
                .checked_add(1)
                .ok_or_else(|| EpochError::Capacity("delivery attempt count overflow".into()))?;
            candidates.push(EpochTargetDeliveryCandidate {
                delivery_id: record.delivery_id.clone(),
                subscription: record.subscription.clone(),
                next_attempt,
                target: record.target.clone(),
                partition_key: record
                    .envelope
                    .key
                    .clone()
                    .unwrap_or_else(|| record.envelope.id.clone()),
                destination: record.destination.clone(),
            });
        }
        Ok(candidates)
    }

    pub(crate) fn has_signed_webhook_targets(&self) -> bool {
        self.records
            .values()
            .any(|record| record.target.signing_key_id().is_some())
    }

    pub(crate) fn has_epoch_target_bindings(&self) -> bool {
        self.records
            .values()
            .any(|record| record.destination.is_some())
    }

    pub(crate) fn get(&self, delivery_id: &str) -> Option<DeliveryRecord> {
        self.records.get(delivery_id).cloned()
    }

    pub(crate) fn query(
        &self,
        subscription: Option<&str>,
        state: Option<DeliveryStateKind>,
        limit: usize,
    ) -> EpochResult<Vec<DeliveryRecord>> {
        ensure_enabled(self.enabled)?;
        if let Some(subscription) = subscription {
            validate_resource_name(subscription)?;
        }
        validate_batch_limit(limit, MAX_DELIVERY_QUERY_RESULTS)?;
        let mut records = self
            .records
            .values()
            .filter(|record| {
                subscription.is_none_or(|name| record.subscription == name)
                    && state.is_none_or(|kind| record.state.kind() == kind)
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            (left.publish_position, &left.subscription)
                .cmp(&(right.publish_position, &right.subscription))
        });
        records.truncate(limit);
        Ok(records)
    }

    pub(crate) fn counts(&self) -> DeliveryCounts {
        let mut counts = DeliveryCounts::default();
        for record in self.records.values() {
            match record.state.kind() {
                DeliveryStateKind::Pending => counts.pending += 1,
                DeliveryStateKind::InFlight => counts.in_flight += 1,
                DeliveryStateKind::Acknowledged => counts.acknowledged += 1,
                DeliveryStateKind::DeadLettered => counts.dead_lettered += 1,
            }
        }
        counts
    }

    pub(crate) fn validate_snapshot(
        &self,
        enabled: bool,
        max_deliveries: usize,
        commit_position: u64,
        route_plan_version: u64,
    ) -> EpochResult<()> {
        if self.enabled != enabled
            || self.max_deliveries != max_deliveries
            || self.records.len() > self.max_deliveries
            || (!self.enabled && (!self.records.is_empty() || !self.dispatcher_epochs.is_empty()))
        {
            return Err(EpochError::InvalidArgument(
                "event bus snapshot delivery-ledger configuration is invalid".into(),
            ));
        }
        for (dispatcher, epoch) in &self.dispatcher_epochs {
            validate_dispatcher(dispatcher)?;
            if *epoch == 0 {
                return Err(EpochError::InvalidArgument(
                    "event bus snapshot has a zero dispatcher epoch".into(),
                ));
            }
        }
        for (delivery_id_key, record) in &self.records {
            if delivery_id_key != &record.delivery_id
                || record.delivery_id != delivery_id(record.publish_position, &record.subscription)
                || record.publish_position == 0
                || record.publish_position > commit_position
                || record.route_plan_version == 0
                || record.route_plan_version > route_plan_version
                || record.attempts.len()
                    > usize::try_from(record.policy.retry.max_attempts).unwrap_or(usize::MAX)
            {
                return Err(EpochError::InvalidArgument(
                    "event bus snapshot delivery record identity is invalid".into(),
                ));
            }
            record.envelope.validate()?;
            Subscription {
                name: record.subscription.clone(),
                filter: EventFilter::default(),
                target: record.target.clone(),
                transform: EventTransform::default(),
                delivery_policy: record.policy.clone(),
            }
            .validate()?;
            if let Some(destination) = &record.destination {
                destination.validate_for_target(&record.target)?;
            }
            let expected_expiry = record
                .policy
                .retry
                .max_age_ms
                .map(|max_age_ms| {
                    record.created_at_ms.checked_add(max_age_ms).ok_or_else(|| {
                        EpochError::InvalidArgument(
                            "event bus snapshot delivery expiry overflowed".into(),
                        )
                    })
                })
                .transpose()?;
            if record.expires_at_ms != expected_expiry {
                return Err(EpochError::InvalidArgument(
                    "event bus snapshot delivery expiry is invalid".into(),
                ));
            }
            validate_snapshot_attempts(record, &self.dispatcher_epochs)?;
            validate_snapshot_delivery_state(record)?;
        }
        Ok(())
    }

    fn accept_dispatcher_epoch(
        &mut self,
        dispatcher: &str,
        requested_epoch: u64,
    ) -> EpochResult<()> {
        match self.dispatcher_epochs.get(dispatcher).copied() {
            Some(current) if requested_epoch < current => Err(EpochError::Fenced),
            Some(current) if requested_epoch == current => Ok(()),
            _ => {
                self.dispatcher_epochs
                    .insert(dispatcher.to_owned(), requested_epoch);
                Ok(())
            }
        }
    }

    fn authorize(
        &self,
        delivery_id: &str,
        dispatcher: &str,
        lease_token: &str,
        fence: DeliveryFence,
    ) -> EpochResult<()> {
        if self.dispatcher_epochs.get(dispatcher).copied() != Some(fence.dispatcher_epoch()) {
            return Err(EpochError::Fenced);
        }
        let record = self
            .records
            .get(delivery_id)
            .ok_or_else(|| EpochError::NotFound(delivery_id.to_owned()))?;
        match &record.state {
            DeliveryState::InFlight {
                dispatcher: owner,
                lease_token: active_token,
                fence: active_fence,
                ..
            } if owner == dispatcher && active_token == lease_token && *active_fence == fence => {
                Ok(())
            }
            _ => Err(EpochError::Fenced),
        }
    }

    fn settle_failure(
        &mut self,
        delivery_id: &str,
        reason: &str,
        now_ms: u64,
    ) -> EpochResult<DeliveryRecord> {
        let record = self
            .records
            .get_mut(delivery_id)
            .ok_or_else(|| EpochError::NotFound(delivery_id.to_owned()))?;
        let attempt = record
            .attempts
            .last()
            .ok_or_else(|| EpochError::Internal("delivery has no active attempt".into()))?
            .attempt;
        let mut terminal = attempt >= record.policy.retry.max_attempts
            || record
                .expires_at_ms
                .is_some_and(|deadline| deadline <= now_ms);
        let retry_at_ms = if terminal {
            None
        } else {
            let delay = retry_delay(&record.delivery_id, attempt, &record.policy.retry);
            let retry_at_ms = now_ms
                .checked_add(delay)
                .ok_or_else(|| EpochError::Capacity("delivery retry deadline overflow".into()))?;
            if record
                .expires_at_ms
                .is_some_and(|deadline| retry_at_ms >= deadline)
            {
                terminal = true;
                None
            } else {
                Some(retry_at_ms)
            }
        };
        complete_current_attempt(
            record,
            DeliveryAttemptOutcome::Failed {
                failed_at_ms: now_ms,
                reason: reason.to_owned(),
                retry_at_ms,
            },
        )?;
        record.state = if terminal {
            DeliveryState::DeadLettered {
                dead_lettered_at_ms: now_ms,
                reason: reason.to_owned(),
            }
        } else {
            DeliveryState::Pending {
                eligible_at_ms: retry_at_ms
                    .ok_or_else(|| EpochError::Internal("retry deadline is missing".into()))?,
            }
        };
        Ok(record.clone())
    }
}

fn begin_delivery_attempt(
    record: &mut DeliveryRecord,
    dispatcher: &str,
    now_ms: u64,
    fence: DeliveryFence,
) -> EpochResult<DeliveryLease> {
    let attempt = u32::try_from(record.attempts.len())
        .map_err(|_| EpochError::Capacity("delivery attempt count overflow".into()))?
        .checked_add(1)
        .ok_or_else(|| EpochError::Capacity("delivery attempt count overflow".into()))?;
    if attempt > record.policy.retry.max_attempts {
        return Err(EpochError::Internal(format!(
            "pending delivery {} exceeded its retry policy",
            record.delivery_id
        )));
    }
    let lease_deadline_ms = now_ms
        .checked_add(record.policy.timeout_ms)
        .ok_or_else(|| EpochError::Capacity("delivery lease deadline overflow".into()))?;
    let lease_token = lease_token(
        &record.delivery_id,
        dispatcher,
        attempt,
        lease_deadline_ms,
        fence,
    );
    record.attempts.push(DeliveryAttempt {
        attempt,
        dispatcher: dispatcher.to_owned(),
        dispatcher_epoch: fence.dispatcher_epoch(),
        leader_term: fence.leader_term,
        started_at_ms: now_ms,
        lease_deadline_ms,
        outcome: DeliveryAttemptOutcome::InFlight,
    });
    record.state = DeliveryState::InFlight {
        dispatcher: dispatcher.to_owned(),
        dispatcher_epoch: fence.dispatcher_epoch(),
        attempt,
        lease_token: lease_token.clone(),
        lease_deadline_ms,
        fence,
    };
    Ok(DeliveryLease {
        delivery_id: record.delivery_id.clone(),
        publish_position: record.publish_position,
        subscription: record.subscription.clone(),
        target: record.target.clone(),
        envelope: record.envelope.clone(),
        route_plan_version: record.route_plan_version,
        destination: record.destination.clone(),
        attempt,
        lease_token,
        lease_deadline_ms,
    })
}

fn validate_snapshot_attempts(
    record: &DeliveryRecord,
    dispatcher_epochs: &BTreeMap<String, u64>,
) -> EpochResult<()> {
    for (position, attempt) in record.attempts.iter().enumerate() {
        validate_dispatcher(&attempt.dispatcher)?;
        if u32::try_from(position + 1).ok() != Some(attempt.attempt)
            || attempt.dispatcher_epoch == 0
            || attempt.leader_term == 0
            || attempt.lease_deadline_ms == 0
            || dispatcher_epochs
                .get(&attempt.dispatcher)
                .is_none_or(|epoch| attempt.dispatcher_epoch > *epoch)
        {
            return Err(EpochError::InvalidArgument(
                "event bus snapshot delivery attempt registry is invalid".into(),
            ));
        }
        if let DeliveryAttemptOutcome::Failed { reason, .. } = &attempt.outcome {
            validate_reason(reason)?;
        }
    }
    Ok(())
}

fn validate_snapshot_delivery_state(record: &DeliveryRecord) -> EpochResult<()> {
    let valid = match &record.state {
        DeliveryState::Pending { eligible_at_ms } => match record.attempts.last() {
            None => *eligible_at_ms == record.created_at_ms,
            Some(DeliveryAttempt {
                outcome: DeliveryAttemptOutcome::Failed { retry_at_ms, .. },
                ..
            }) => *retry_at_ms == Some(*eligible_at_ms),
            Some(_) => false,
        },
        DeliveryState::InFlight {
            dispatcher,
            dispatcher_epoch,
            attempt,
            lease_token: active_token,
            lease_deadline_ms,
            fence,
        } => {
            fence.validate()?;
            matches!(
                record.attempts.last(),
                Some(current)
                    if current.attempt == *attempt
                        && current.dispatcher == *dispatcher
                        && current.dispatcher_epoch == *dispatcher_epoch
                        && current.leader_term == fence.leader_term
                        && current.lease_deadline_ms == *lease_deadline_ms
                        && current.outcome == DeliveryAttemptOutcome::InFlight
                        && *active_token == lease_token(
                            &record.delivery_id,
                            dispatcher,
                            *attempt,
                            *lease_deadline_ms,
                            *fence,
                        )
            )
        }
        DeliveryState::Acknowledged { acknowledged_at_ms } => matches!(
            record.attempts.last(),
            Some(DeliveryAttempt {
                outcome: DeliveryAttemptOutcome::Acknowledged { completed_at_ms },
                ..
            }) if completed_at_ms == acknowledged_at_ms
        ),
        DeliveryState::DeadLettered {
            dead_lettered_at_ms,
            reason,
        } => {
            validate_reason(reason)?;
            matches!(
                record.attempts.last(),
                Some(DeliveryAttempt {
                    outcome: DeliveryAttemptOutcome::Failed {
                        failed_at_ms,
                        reason: failed_reason,
                        retry_at_ms: None,
                    },
                    ..
                }) if failed_at_ms == dead_lettered_at_ms && failed_reason == reason
            )
        }
    };
    if valid {
        Ok(())
    } else {
        Err(EpochError::InvalidArgument(
            "event bus snapshot delivery state is inconsistent with its attempts".into(),
        ))
    }
}

pub(crate) fn delivery_id(publish_position: u64, subscription: &str) -> String {
    format!("{DELIVERY_ID_PREFIX}.{publish_position}.{subscription}")
}

fn complete_current_attempt(
    record: &mut DeliveryRecord,
    outcome: DeliveryAttemptOutcome,
) -> EpochResult<()> {
    let attempt = record
        .attempts
        .last_mut()
        .ok_or_else(|| EpochError::Internal("delivery has no current attempt".into()))?;
    if attempt.outcome != DeliveryAttemptOutcome::InFlight {
        return Err(EpochError::Internal(
            "delivery current attempt was already settled".into(),
        ));
    }
    attempt.outcome = outcome;
    Ok(())
}

fn retry_delay(delivery_id: &str, attempt: u32, policy: &DeliveryRetryPolicy) -> u64 {
    let base = match policy.strategy {
        DeliveryBackoffStrategy::Fixed => policy.initial_delay_ms,
        DeliveryBackoffStrategy::Exponential => policy
            .initial_delay_ms
            .saturating_mul(2_u64.saturating_pow(attempt.saturating_sub(1).min(63))),
    }
    .min(policy.max_delay_ms);
    if policy.jitter_percent == 0 || base == 0 {
        return base;
    }
    let span = base.saturating_mul(u64::from(policy.jitter_percent)) / 100;
    if span == 0 {
        return base;
    }
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/event-bus/delivery-jitter/v1\0");
    hash_length_prefixed(&mut hasher, delivery_id.as_bytes());
    hasher.update(attempt.to_be_bytes());
    let digest = hasher.finalize();
    let sample = u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 prefix is eight bytes"),
    );
    base.saturating_sub(span)
        .saturating_add(sample % span.saturating_mul(2).saturating_add(1))
}

fn lease_token(
    delivery_id: &str,
    dispatcher: &str,
    attempt: u32,
    lease_deadline_ms: u64,
    fence: DeliveryFence,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epoch/event-bus/delivery-lease/v1\0");
    hash_length_prefixed(&mut hasher, delivery_id.as_bytes());
    hash_length_prefixed(&mut hasher, dispatcher.as_bytes());
    hasher.update(attempt.to_be_bytes());
    hasher.update(lease_deadline_ms.to_be_bytes());
    hasher.update(fence.tablet_id.to_be_bytes());
    hasher.update(fence.tablet_epoch.to_be_bytes());
    hasher.update(fence.leader_term.to_be_bytes());
    hasher.update(fence.dispatcher_epoch.to_be_bytes());
    format!("{DELIVERY_LEASE_PREFIX}{}", lower_hex(&hasher.finalize()))
}

fn hash_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
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

fn ensure_enabled(enabled: bool) -> EpochResult<()> {
    if enabled {
        Ok(())
    } else {
        Err(EpochError::Unavailable(
            "durable delivery outbox is disabled".into(),
        ))
    }
}

fn validate_batch_limit(value: usize, maximum: usize) -> EpochResult<()> {
    if value == 0 || value > maximum {
        return Err(EpochError::InvalidArgument(format!(
            "delivery batch limit must be between 1 and {maximum}"
        )));
    }
    Ok(())
}

fn validate_dispatcher(dispatcher: &str) -> EpochResult<()> {
    if dispatcher.len() > MAX_DISPATCHER_BYTES {
        return Err(EpochError::InvalidArgument(format!(
            "dispatcher is {} bytes; maximum is {MAX_DISPATCHER_BYTES}",
            dispatcher.len()
        )));
    }
    validate_resource_name(dispatcher)
}

fn validate_reason(reason: &str) -> EpochResult<()> {
    if reason.is_empty() || reason.len() > MAX_DELIVERY_REASON_BYTES {
        return Err(EpochError::InvalidArgument(format!(
            "delivery failure reason must be between 1 and {MAX_DELIVERY_REASON_BYTES} bytes"
        )));
    }
    if reason.chars().any(char::is_control) {
        return Err(EpochError::InvalidArgument(
            "delivery failure reason cannot contain control characters".into(),
        ));
    }
    Ok(())
}
