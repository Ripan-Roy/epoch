//! Canonical replicated Event Bus ingress and delivery-ledger state machine.

mod command;
mod digest;
mod model;

use std::collections::BTreeMap;

use epoch_bus::{
    ArchivedEvent, BusConfig, DeliveryCounts, DeliveryFence, DeliveryRecord, DeliveryState,
    DeliveryStateKind, EventBus, EventFilter,
};
use epoch_core::{DurabilityProfile, EpochError, EpochResult};

use crate::common::{AppliedCommand, validate_committed_command_scope};
use crate::{
    AppliedCommandMetadata, CommittedCommand, TabletError, TabletResult, TabletWriteEvidence,
};

pub use command::*;
use digest::{delivery_plan_digest, initial_state_digest, transition_digest};
pub use model::*;

#[derive(Debug)]
pub struct BusTablet {
    scope: BusTabletScope,
    bus: EventBus,
    applied: BTreeMap<u64, AppliedCommand<BusTabletReceipt>>,
    last_applied_command_index: u64,
    last_applied_time_ms: u64,
    business_state_digest: [u8; 32],
    state_digest: [u8; 32],
}

impl BusTablet {
    #[allow(
        clippy::needless_pass_by_value,
        reason = "tablet constructors consistently own their validated profile configuration"
    )]
    pub fn new(scope: BusTabletScope, mut config: BusConfig) -> TabletResult<Self> {
        scope.validate()?;
        // Consensus supplies durability evidence; the standalone engine label
        // cannot truthfully describe a committed tablet mutation.
        config.durability = DurabilityProfile::Volatile;
        config.delivery_outbox = true;
        let bus = EventBus::new(config)?;
        let business_state_digest = bus.recovery_state_digest()?;
        let state_digest = initial_state_digest(&scope, business_state_digest);
        Ok(Self {
            scope,
            bus,
            applied: BTreeMap::new(),
            last_applied_command_index: 0,
            last_applied_time_ms: 0,
            business_state_digest,
            state_digest,
        })
    }

    pub fn with_default_config(scope: BusTabletScope) -> TabletResult<Self> {
        Self::new(scope, BusConfig::default())
    }

    pub fn scope(&self) -> &BusTabletScope {
        &self.scope
    }

    pub fn apply(&mut self, committed: CommittedCommand<'_>) -> TabletResult<BusTabletReceipt> {
        validate_committed_command_scope(&self.scope, committed)?;
        let metadata = AppliedCommandMetadata::from_committed(committed);
        if let Some(mut receipt) = self.receipt_for_committed(committed)? {
            receipt.disposition = BusTabletDisposition::Replayed;
            return Ok(receipt);
        }
        if committed.log_index <= self.last_applied_command_index {
            return Err(TabletError::CommitOrder {
                previous: self.last_applied_command_index,
                observed: committed.log_index,
            });
        }

        let command = BusTabletCommand::decode(committed.payload, &self.scope)?;
        let expected_proposal_id = command.proposal_id(&self.scope)?;
        if committed.proposal_id != expected_proposal_id {
            return Err(TabletError::InvalidCommand(format!(
                "proposal_id {} does not match idempotency_key hash {expected_proposal_id}",
                committed.proposal_id
            )));
        }
        let applied_at_ms = command.applied_at_ms.max(self.last_applied_time_ms);
        let mut candidate = self.bus.clone();
        let execution = execute(
            &mut candidate,
            &self.scope,
            committed,
            command.operation,
            applied_at_ms,
        );
        let (outcome, next_bus) = match execution {
            Ok(result) => (BusTabletOutcome::Applied { result }, Some(candidate)),
            Err(error) => (recordable_rejected_outcome(error)?, None),
        };
        let receipt = BusTabletReceipt {
            proposal_id: committed.proposal_id,
            tablet_id: self.scope.tablet_id,
            tablet_epoch: self.scope.tablet_epoch,
            term: committed.term,
            commit_index: committed.log_index,
            applied_at_ms,
            write_evidence: TabletWriteEvidence::FixedVoterMajorityPersisted,
            durable_voter_acks: 2,
            disposition: BusTabletDisposition::New,
            outcome,
        };

        let effective_bus = next_bus.as_ref().unwrap_or(&self.bus);
        let business_state_digest = effective_bus.recovery_state_digest()?;
        let next_digest = transition_digest(
            self.state_digest,
            committed,
            metadata.payload_digest,
            business_state_digest,
            applied_at_ms,
            &receipt.outcome,
        )?;
        if let Some(next_bus) = next_bus {
            self.bus = next_bus;
        }
        self.business_state_digest = business_state_digest;
        self.state_digest = next_digest;
        self.last_applied_command_index = committed.log_index;
        self.last_applied_time_ms = applied_at_ms;
        self.applied.insert(
            committed.proposal_id,
            AppliedCommand {
                metadata,
                receipt: receipt.clone(),
            },
        );
        Ok(receipt)
    }

    pub fn lookup(&self, proposal_id: u64) -> Option<BusTabletReceipt> {
        self.applied
            .get(&proposal_id)
            .map(|applied| applied.receipt.clone())
    }

    pub fn receipt_for_committed(
        &self,
        committed: CommittedCommand<'_>,
    ) -> TabletResult<Option<BusTabletReceipt>> {
        validate_committed_command_scope(&self.scope, committed)?;
        let Some(previous) = self.applied.get(&committed.proposal_id) else {
            return Ok(None);
        };
        previous.metadata.validate_exact(committed)?;
        Ok(Some(previous.receipt.clone()))
    }

    pub fn replay(
        &self,
        from_ms: u64,
        to_ms: u64,
        filter: Option<&EventFilter>,
        limit: usize,
    ) -> TabletResult<Vec<ArchivedEvent>> {
        Ok(self.bus.replay(from_ms, to_ms, filter, limit)?)
    }

    pub const fn last_applied_command_index(&self) -> u64 {
        self.last_applied_command_index
    }

    pub const fn last_applied_time_ms(&self) -> u64 {
        self.last_applied_time_ms
    }

    pub fn applied_command_count(&self) -> usize {
        self.applied.len()
    }

    pub fn route_plan_version(&self) -> u64 {
        self.bus.route_plan_version()
    }

    pub fn subscription_count(&self) -> usize {
        self.bus.subscription_count()
    }

    pub const fn commit_position(&self) -> u64 {
        self.bus.commit_position()
    }

    pub fn archived_event_count(&self) -> usize {
        self.bus.archived_event_count()
    }

    pub fn delivery(&self, delivery_id: &str) -> Option<DeliveryRecord> {
        self.bus.delivery(delivery_id)
    }

    pub fn deliveries(
        &self,
        subscription: Option<&str>,
        state: Option<DeliveryStateKind>,
        limit: usize,
    ) -> TabletResult<Vec<DeliveryRecord>> {
        Ok(self.bus.deliveries(subscription, state, limit)?)
    }

    pub fn delivery_counts(&self) -> DeliveryCounts {
        self.bus.delivery_counts()
    }

    pub const fn business_state_digest(&self) -> [u8; 32] {
        self.business_state_digest
    }

    pub const fn state_digest(&self) -> [u8; 32] {
        self.state_digest
    }
}

fn execute(
    bus: &mut EventBus,
    scope: &BusTabletScope,
    committed: CommittedCommand<'_>,
    operation: BusTabletOperation,
    applied_at_ms: u64,
) -> EpochResult<BusTabletOperationResult> {
    match operation {
        BusTabletOperation::UpsertSubscription { subscription } => {
            let name = subscription.name.clone();
            let replaced = bus.has_subscription(&name);
            let route_plan_version = bus.upsert_subscription(subscription)?;
            Ok(BusTabletOperationResult::SubscriptionUpserted {
                name,
                replaced,
                route_plan_version,
            })
        }
        BusTabletOperation::RemoveSubscription { name } => {
            let removed = bus.remove_subscription(&name)?;
            Ok(BusTabletOperationResult::SubscriptionRemoved {
                name,
                removed,
                route_plan_version: bus.route_plan_version(),
            })
        }
        BusTabletOperation::Publish { envelope } => {
            let result = bus.publish(envelope, applied_at_ms)?;
            Ok(BusTabletOperationResult::Published {
                position: result.acknowledgement.commit_position,
                route_plan_version: bus.route_plan_version(),
                delivery_count: result.deliveries.len(),
                delivery_plan_digest: delivery_plan_digest(&result.deliveries)?,
            })
        }
        BusTabletOperation::AcquireDeliveries {
            subscription,
            dispatcher,
            dispatcher_epoch,
            max_deliveries,
        } => execute_acquire(
            bus,
            DeliveryExecution::new(scope, committed, applied_at_ms),
            &subscription,
            &dispatcher,
            dispatcher_epoch,
            max_deliveries,
        ),
        BusTabletOperation::AcknowledgeDelivery {
            delivery_id,
            dispatcher,
            dispatcher_epoch,
            lease_token,
        } => execute_acknowledge(
            bus,
            DeliveryExecution::new(scope, committed, applied_at_ms),
            delivery_id,
            &dispatcher,
            dispatcher_epoch,
            &lease_token,
        ),
        BusTabletOperation::FailDelivery {
            delivery_id,
            dispatcher,
            dispatcher_epoch,
            lease_token,
            reason,
        } => execute_failure(
            bus,
            DeliveryExecution::new(scope, committed, applied_at_ms),
            delivery_id,
            &dispatcher,
            dispatcher_epoch,
            &lease_token,
            &reason,
        ),
        BusTabletOperation::MaintainDeliveries { max_deliveries } => {
            execute_maintenance(bus, max_deliveries, applied_at_ms)
        }
    }
}

#[derive(Clone, Copy)]
struct DeliveryExecution<'a> {
    scope: &'a BusTabletScope,
    committed: CommittedCommand<'a>,
    applied_at_ms: u64,
}

impl<'a> DeliveryExecution<'a> {
    const fn new(
        scope: &'a BusTabletScope,
        committed: CommittedCommand<'a>,
        applied_at_ms: u64,
    ) -> Self {
        Self {
            scope,
            committed,
            applied_at_ms,
        }
    }

    fn fence(self, dispatcher_epoch: u64) -> EpochResult<DeliveryFence> {
        DeliveryFence::new(
            self.scope.tablet_id,
            self.scope.tablet_epoch,
            self.committed.term,
            dispatcher_epoch,
        )
    }
}

fn execute_acquire(
    bus: &mut EventBus,
    execution: DeliveryExecution<'_>,
    subscription: &str,
    dispatcher: &str,
    dispatcher_epoch: u64,
    max_deliveries: u16,
) -> EpochResult<BusTabletOperationResult> {
    let fence = execution.fence(dispatcher_epoch)?;
    let deliveries = bus
        .acquire_deliveries(
            subscription,
            dispatcher,
            usize::from(max_deliveries),
            execution.applied_at_ms,
            fence,
        )?
        .into_iter()
        .map(BusTabletDelivery::from)
        .collect();
    Ok(BusTabletOperationResult::DeliveriesAcquired { deliveries })
}

fn execute_acknowledge(
    bus: &mut EventBus,
    execution: DeliveryExecution<'_>,
    delivery_id: String,
    dispatcher: &str,
    dispatcher_epoch: u64,
    lease_token: &str,
) -> EpochResult<BusTabletOperationResult> {
    let fence = execution.fence(dispatcher_epoch)?;
    bus.acknowledge_delivery(
        &delivery_id,
        dispatcher,
        lease_token,
        fence,
        execution.applied_at_ms,
    )?;
    Ok(BusTabletOperationResult::DeliveryAcknowledged { delivery_id })
}

fn execute_failure(
    bus: &mut EventBus,
    execution: DeliveryExecution<'_>,
    delivery_id: String,
    dispatcher: &str,
    dispatcher_epoch: u64,
    lease_token: &str,
    reason: &str,
) -> EpochResult<BusTabletOperationResult> {
    let fence = execution.fence(dispatcher_epoch)?;
    let record = bus.fail_delivery(
        &delivery_id,
        dispatcher,
        lease_token,
        fence,
        reason,
        execution.applied_at_ms,
    )?;
    let (state, next_eligible_at_ms) = match record.state {
        DeliveryState::Pending { eligible_at_ms } => {
            (DeliveryStateKind::Pending, Some(eligible_at_ms))
        }
        DeliveryState::DeadLettered { .. } => (DeliveryStateKind::DeadLettered, None),
        _ => {
            return Err(EpochError::Internal(
                "failed delivery did not settle to pending or dead-lettered".into(),
            ));
        }
    };
    Ok(BusTabletOperationResult::DeliveryFailed {
        delivery_id,
        state,
        next_eligible_at_ms,
    })
}

fn execute_maintenance(
    bus: &mut EventBus,
    max_deliveries: u16,
    applied_at_ms: u64,
) -> EpochResult<BusTabletOperationResult> {
    let result = bus.maintain_deliveries(applied_at_ms, usize::from(max_deliveries))?;
    Ok(BusTabletOperationResult::DeliveriesMaintained {
        processed: count_as_u16(result.processed, "delivery maintenance")?,
        retried: count_as_u16(result.retried, "delivery retry")?,
        dead_lettered: count_as_u16(result.dead_lettered, "delivery dead-letter")?,
        counts: result.counts.try_into()?,
    })
}

fn count_as_u16(value: usize, field: &str) -> EpochResult<u16> {
    u16::try_from(value).map_err(|_| EpochError::Internal(format!("{field} count exceeds u16")))
}

fn recordable_rejected_outcome(error: EpochError) -> TabletResult<BusTabletOutcome> {
    let code = match &error {
        EpochError::AlreadyExists(_) => BusTabletRejectionCode::AlreadyExists,
        EpochError::NotFound(_) => BusTabletRejectionCode::NotFound,
        EpochError::InvalidArgument(_) => BusTabletRejectionCode::InvalidArgument,
        EpochError::Conflict(_) => BusTabletRejectionCode::Conflict,
        EpochError::Fenced => BusTabletRejectionCode::Fenced,
        EpochError::Capacity(_) => BusTabletRejectionCode::Capacity,
        EpochError::Unavailable(_) => BusTabletRejectionCode::Unavailable,
        EpochError::Storage(_) | EpochError::Internal(_) => {
            return Err(TabletError::Profile(error));
        }
    };
    Ok(BusTabletOutcome::Rejected {
        code,
        detail: error.to_string(),
    })
}

#[cfg(test)]
mod tests;
